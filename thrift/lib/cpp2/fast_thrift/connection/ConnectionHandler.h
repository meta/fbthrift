/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>

#include <folly/Executor.h>
#include <folly/container/F14Map.h>
#include <folly/io/async/AsyncTransport.h>
#include <folly/io/async/DelayedDestruction.h>
#include <folly/io/async/EventBase.h>
#include <folly/logging/xlog.h>
#include <folly/observer/Observer.h>
#include <folly/synchronization/Baton.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/BufferAllocator.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/HandlerTag.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineRef.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/StaticPipelineBuilder.h>
#include <thrift/lib/cpp2/fast_thrift/connection/ConnectionFactory.h>
#include <thrift/lib/cpp2/fast_thrift/connection/SocketOptions.h>
#include <thrift/lib/cpp2/fast_thrift/connection/common/ConnectionStats.h>
#include <thrift/lib/cpp2/fast_thrift/connection/endpoint/ConnectionInstaller.h>
#include <thrift/lib/cpp2/fast_thrift/connection/endpoint/ConnectionListener.h>
#include <thrift/lib/cpp2/fast_thrift/connection/handler/ConnectionAcceptCallbackHandler.h>
#include <thrift/lib/cpp2/fast_thrift/connection/handler/ConnectionBuilderHandler.h>
#include <thrift/lib/cpp2/fast_thrift/connection/handler/ConnectionLimitHandler.h>
#include <thrift/lib/cpp2/fast_thrift/connection/handler/ConnectionMetricsHandler.h>
#include <thrift/lib/cpp2/fast_thrift/connection/handler/ConnectionTLSHandler.h>
#include <thrift/lib/cpp2/fast_thrift/connection/security/common/TLSStats.h>
#include <thrift/lib/cpp2/fast_thrift/security/FizzServerContextBuilder.h>
#include <thrift/lib/cpp2/fast_thrift/security/SSLPolicy.h>

namespace apache::thrift::fast_thrift::connection {

namespace fast_security = ::apache::thrift::fast_thrift::security;

// Pipeline handler tags. Must be at namespace scope (HANDLER_TAG expands to
// an inline constexpr variable, which can't live in function bodies).
HANDLER_TAG(connection_tls);
HANDLER_TAG(connection_builder_handler);
HANDLER_TAG(connection_accept_callback_handler);

/**
 * ConnectionHandler — per-data-EventBase unit. Its ConnectionListener receives
 * accepted fds as the pipeline head, then this handler owns the resulting
 * connection through teardown.
 *
 * Pipeline shape (constructed lazily by setConnectionFactory):
 *
 *   ConnectionListener (head)
 *     → [ConnectionTLSHandler]
 *     → [ConnectionLimitHandler]                     // only if a per-IO-thread
 *                                                    //   connection cap is set
 *     → ConnectionBuilderHandler<F>
 *     → [ConnectionAcceptCallbackHandler<Conn>]      // only if onAccept is set
 *     → [ConnectionMetricsHandler]                   // only if stats are set
 *     → ConnectionInstaller<Conn>  (tail)
 *
 * Shutdown is single-call: stop() rejects new installs, triggers close on
 * every live connection, and waits for each one's
 * close callback to remove its entry. Each connection is responsible for
 * bounding its own termination — ConnectionHandler does not impose an
 * outer deadline. Must NOT be called from the owning EVB thread; the
 * wait would block the same loop that fires the close callbacks.
 *
 */
class ConnectionHandler {
 public:
  using Ptr = std::unique_ptr<ConnectionHandler>;

  // `stats` is borrowed and may be null. Must be constructed on `evb`: the
  // shard is resolved here and belongs to the constructing thread.
  ConnectionHandler(
      folly::EventBase& evb,
      fast_security::SSLPolicy sslPolicy,
      folly::observer::Observer<std::shared_ptr<const fast_security::TLSParams>>
          tlsParamsObserver,
      SocketOptions socketOptions,
      std::optional<folly::observer::Observer<uint32_t>>
          maxConnectionsPerIOThread,
      ConnectionStats* stats = nullptr,
      security::TLSStats* tlsStats = nullptr);

  ~ConnectionHandler();

  ConnectionHandler(const ConnectionHandler&) = delete;
  ConnectionHandler& operator=(const ConnectionHandler&) = delete;
  ConnectionHandler(ConnectionHandler&&) = delete;
  ConnectionHandler& operator=(ConnectionHandler&&) = delete;

  /**
   * Build and start the established-connection pipeline using `factory`.
   * Optional
   * `onAccept` callback fires once per accepted connection with typed
   * access to the freshly-built Connection, between creation and storage.
   *
   * `factory` is borrowed — caller must keep it alive until this
   * handler's pipeline is torn down (stop() / dtor). Typically owned by
   * ConnectionManager and outlives every per-EVB handler.
   *
   * Must be called exactly once on the owning EVB before any connection can
   * be installed.
   */
  template <ConnectionFactory F>
  void setConnectionFactory(
      F& factory, std::function<void(FactoryConnectionType<F>&)> onAccept = {});

  ConnectionListener& listener() noexcept { return listener_; }

  /**
   * Graceful shutdown. Stops installation, triggers close on every live
   * connection, and waits for them all to fully tear down (their close
   * callbacks remove each entry). Each connection bounds its own
   * termination — there is no outer deadline here. Must be called
   * off-EVB.
   */
  void stop();

  // Thread-safe: backed by an atomic kept in sync with connections_ on the
  // EVB. Plain connections_.size() would race with off-EVB readers
  // (test threads, observability).
  size_t connectionCount() const noexcept {
    return connectionCount_.load(std::memory_order_relaxed);
  }

 private:
  // Type-erased per-connection storage. The function-pointer-erasure
  // pattern keeps the connection layer free of vtables on our types.
  struct AnyConnection {
    std::unique_ptr<void, void (*)(void*) noexcept> conn;
    void (*startFn)(void*) noexcept;
    void (*closeFn)(void*, bool) noexcept;

    void start() noexcept { startFn(conn.get()); }
    void close(bool notifyOwner) noexcept { closeFn(conn.get(), notifyOwner); }
  };

  template <typename C>
  static AnyConnection wrap(C c) {
    return AnyConnection{
        .conn = std::unique_ptr<void, void (*)(void*) noexcept>(
            new C(std::move(c)),
            [](void* p) noexcept { delete static_cast<C*>(p); }),
        .startFn = [](void* p) noexcept { static_cast<C*>(p)->start(); },
        .closeFn =
            [](void* p, bool notifyOwner) noexcept {
              auto& connection = *static_cast<C*>(p);
              if (!notifyOwner) {
                connection.setCloseCallback({});
              }
              connection.close();
            },
    };
  }

  // Established-connection pipeline teardown — invoked from stop().
  void stopInstallingOnEvb();
  // Trigger close on every live connection — invoked from stop() on EVB.
  void closeAllOnEvb();
  // Synchronous force-destroy escape hatch — invoked only from the dtor's
  // on-EVB branch where we cannot wait for graceful teardown.
  void closeAllConnectionsOnEvb();

  // Close-callback installed on every accepted connection. Erases the
  // entry from connections_ and posts drainedBaton_ when the last one
  // drops during drain. Runs on this handler's EVB.
  void onConnectionClosed(uint64_t connId) noexcept;

  // Post drainedBaton_ at most once. The drain-empty path in
  // closeAllOnEvb and the last-close path in onConnectionClosed both
  // race to post; folly::Baton requires a single post per cycle.
  void postDrainedOnce() noexcept;

  folly::Executor::KeepAlive<folly::EventBase> evb_;
  fast_security::SSLPolicy sslPolicy_;
  folly::observer::Observer<std::shared_ptr<const fast_security::TLSParams>>
      tlsParamsObserver_;
  SocketOptions socketOptions_;
  std::optional<folly::observer::Observer<uint32_t>> maxConnectionsPerIOThread_;

  // Established-connection pipeline pieces. The listener callback is entered
  // only on this handler's EventBase. Declaration order ensures the pipeline
  // tears down first.
  channel_pipeline::SimpleBufferAllocator allocator_;
  ConnectionListener listener_;
  channel_pipeline::PipelineOwner pipeline_;

  // This EventBase's counter shard, resolved in the ctor, or null when the
  // server was given no stats. Written only from this EventBase.
  ConnectionStatsShard* statsShard_{nullptr};
  security::TLSStatsShard* tlsShard_{nullptr};

  folly::F14NodeMap<uint64_t, AnyConnection> connections_;
  // Mirrors connections_.size(). Mutated on the EVB alongside the map so
  // observability calls can read it lock-free from any thread.
  std::atomic<size_t> connectionCount_{0};
  uint64_t nextConnId_{0};

  // Posted from onConnectionClosed when the last connection drops while
  // draining_ is set. stop() waits on this with a timeout. drainedPosted_
  // guards against the multiple racers (empty-at-start path + last-close
  // path) all trying to post the baton.
  std::atomic<bool> draining_{false};
  std::atomic<bool> drainedPosted_{false};
  folly::Baton<> drainedBaton_;
};

// Template definition — must live in the header.
template <ConnectionFactory F>
void ConnectionHandler::setConnectionFactory(
    F& factory, std::function<void(FactoryConnectionType<F>&)> onAccept) {
  using Conn = FactoryConnectionType<F>;
  using Builder = ConnectionBuilderHandler<F>;
  using AcceptCallback = ConnectionAcceptCallbackHandler<Conn>;
  using Installer = ConnectionInstaller<Conn>;

  CHECK(!pipeline_) << "setConnectionFactory called twice";

  // Tail. registerFn is the only place the handler's concrete Conn type
  // appears outside the pipeline. It wraps each connection into an
  // AnyConnection and installs a close callback so the connection self-
  // removes from connections_ when it tears down.
  //
  // Ordering note: start() must run AFTER the connection is registered in
  // connections_, because start() can synchronously dispatch the first
  // request (e.g. when the post-StopTLS handoff has bytes already buffered
  // at the transport), and any synchronous close fired off that dispatch
  // would invoke onConnectionClosed against an entry that doesn't yet
  // exist. The local `c` is moved into the map, so start() is invoked via
  // the stored AnyConnection.
  auto builder =
      channel_pipeline::StaticPipelineBuilder<ConnectionListener, Installer>()
          .setEventBase(evb_.get())
          .setHead(&listener_)
          .setAllocator(&allocator_)
          .emplaceTail(typename Installer::RegisterFn{[this](Conn c) noexcept {
            auto connId = nextConnId_++;
            c.setCloseCallback(
                [this, connId]() noexcept { onConnectionClosed(connId); });
            auto it =
                connections_.emplace(connId, wrap<Conn>(std::move(c))).first;
            connectionCount_.fetch_add(1, std::memory_order_relaxed);
            it->second.start();
          }});

  const auto finish = [this](auto current) {
    // Only successfully built connections reach the metrics stage.
    if (statsShard_ != nullptr) {
      pipeline_ =
          std::move(current)
              .template addNextInbound<handler::ConnectionMetricsHandler>(
                  handler::connection_metrics_handler_tag, statsShard_)
              .build();
    } else {
      pipeline_ = std::move(current).build();
    }
  };
  const auto addBuilder = [&](auto current) {
    auto withBuilder = std::move(current).template addNextInbound<Builder>(
        connection_builder_handler_tag, std::ref(factory));
    if (onAccept) {
      finish(
          std::move(withBuilder)
              .template addNextInbound<AcceptCallback>(
                  connection_accept_callback_handler_tag, std::move(onAccept)));
    } else {
      finish(std::move(withBuilder));
    }
  };
  const auto addLimit = [&](auto current) {
    if (maxConnectionsPerIOThread_.has_value()) {
      // Only connections that passed TLS count against the established cap.
      addBuilder(
          std::move(current)
              .template addNextInbound<handler::ConnectionLimitHandler>(
                  handler::connection_limit_handler_tag,
                  *maxConnectionsPerIOThread_,
                  &connectionCount_,
                  statsShard_));
    } else {
      addBuilder(std::move(current));
    }
  };
  if (sslPolicy_ == fast_security::SSLPolicy::DISABLED) {
    addLimit(std::move(builder));
  } else {
    addLimit(
        std::move(builder)
            .template addNextDuplexTemplate<handler::ConnectionTLSHandler>(
                connection_tls_tag,
                std::ref(*evb_),
                sslPolicy_,
                tlsParamsObserver_,
                socketOptions_.maxPendingConnectionsPerWorker,
                tlsShard_));
  }
  listener_.setPipeline(pipeline_.get());
  pipeline_->activate();
}

} // namespace apache::thrift::fast_thrift::connection
