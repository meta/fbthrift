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

#include <cstdint>
#include <utility>

#include <folly/ExceptionWrapper.h>
#include <folly/io/async/EventBase.h>
#include <folly/observer/Observer.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/BufferAllocator.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/HandlerTag.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineRef.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/StaticPipelineBuilder.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/connection/common/Messages.h>
#include <thrift/lib/cpp2/fast_thrift/connection/security/common/Messages.h>
#include <thrift/lib/cpp2/fast_thrift/connection/security/common/PendingConnection.h>
#include <thrift/lib/cpp2/fast_thrift/connection/security/common/Util.h>
#include <thrift/lib/cpp2/fast_thrift/connection/security/handler/FizzHandshakeHandler.h>
#include <thrift/lib/cpp2/fast_thrift/connection/security/handler/PendingConnectionLimitHandler.h>
#include <thrift/lib/cpp2/fast_thrift/connection/security/handler/StopTLSV1Handler.h>
#include <thrift/lib/cpp2/fast_thrift/connection/security/handler/TLSClassifier.h>
#include <thrift/lib/cpp2/fast_thrift/connection/security/handler/TLSConfigHandler.h>
#include <thrift/lib/cpp2/fast_thrift/connection/security/handler/TLSConnectionAdapter.h>
#include <thrift/lib/cpp2/fast_thrift/connection/security/handler/TLSFinalizer.h>
#include <thrift/lib/cpp2/fast_thrift/connection/security/handler/TLSMetricsHandler.h>
#include <thrift/lib/cpp2/fast_thrift/security/FizzServerContextBuilder.h>
#include <thrift/lib/cpp2/fast_thrift/security/SSLPolicy.h>

namespace apache::thrift::fast_thrift::connection::handler {

namespace fast_security = ::apache::thrift::fast_thrift::security;
namespace tls_pipeline = ::apache::thrift::fast_thrift::connection::security;
namespace tls_handler =
    ::apache::thrift::fast_thrift::connection::security::handler;

// Inner-pipeline handler tags. Local to ConnectionTLSHandler; not exposed
// to the outer pipeline.
HANDLER_TAG(tls_inner_config);
HANDLER_TAG(tls_inner_metrics);
HANDLER_TAG(tls_inner_classifier);
HANDLER_TAG(tls_inner_fizz_handshake);
HANDLER_TAG(tls_inner_stoptls_v1);
HANDLER_TAG(tls_inner_pending_connection_limit);

inline channel_pipeline::PipelineOwner buildTLSConnectionPipeline(
    folly::EventBase& evb,
    fast_security::SSLPolicy sslPolicy,
    folly::observer::Observer<std::shared_ptr<const fast_security::TLSParams>>
        tlsParamsObserver,
    uint32_t maxPendingConnectionsPerWorker,
    tls_pipeline::TLSStatsShard* stats,
    void* owner,
    tls_handler::TLSConnectionAdapter::ResolvedFn onResolved,
    tls_handler::TLSConnectionAdapter::ExceptionFn onException) {
  using channel_pipeline::PipelineOwner;
  using tls_handler::TLSConnectionAdapter;
  auto builder = channel_pipeline::StaticPipelineBuilder<
                     tls_handler::TLSFinalizer,
                     TLSConnectionAdapter>()
                     .setEventBase(&evb)
                     .emplaceHead()
                     .emplaceTail(owner, onResolved, onException)
                     .emplaceAllocator()
                     .addState<tls_pipeline::PendingConnectionState>(
                         maxPendingConnectionsPerWorker)
                     .addNextDuplexTemplate<tls_handler::StopTLSV1Handler>(
                         tls_inner_stoptls_v1_tag)
                     .addNextDuplexTemplate<tls_handler::FizzHandshakeHandler>(
                         tls_inner_fizz_handshake_tag);

  const auto finish = [](auto current) {
    return PipelineOwner(
        std::move(current)
            .template addNextDuplex<tls_handler::PendingConnectionLimitHandler>(
                tls_inner_pending_connection_limit_tag)
            .build());
  };
  const auto addConfig = [&](auto current) {
    auto withConfig =
        std::move(current)
            .template addNextDuplex<tls_handler::TLSConfigHandler>(
                tls_inner_config_tag, std::move(tlsParamsObserver));
    if (stats != nullptr) {
      return finish(
          std::move(withConfig)
              .template addNextDuplex<tls_handler::TLSMetricsHandler>(
                  tls_inner_metrics_tag, stats));
    }
    return finish(std::move(withConfig));
  };
  if (sslPolicy == fast_security::SSLPolicy::PERMITTED) {
    return addConfig(
        std::move(builder).addNextDuplexTemplate<tls_handler::TLSClassifier>(
            tls_inner_classifier_tag));
  }
  return addConfig(std::move(builder));
}

/**
 * Outer-pipeline (connection acceptance) middle handler that hides the entire
 * TLS lifecycle behind a single boundary. It owns the TLS pipeline and drives
 * it, but is not itself a node in it.
 *
 * The TLS pipeline does its work on the write path: an accepted transport is
 * submitted to the tail adapter as a write that flows through the stages and
 * converges at the head, which returns the resolved transport on the read path
 * back to the adapter. The pipeline shape is fixed at construction by
 * sslPolicy (build order is head→tail, i.e. reverse of the work order):
 *
 *   DISABLED:  not installed in the outer pipeline (caller's choice).
 *   REQUIRED:  TLSFinalizer → StopTLSV1Handler → FizzHandshakeHandler
 *                           → TLSConfigHandler → [TLSMetricsHandler]
 *                           → PendingConnectionLimitHandler
 *                           → TLSConnectionAdapter
 *   PERMITTED: TLSFinalizer → StopTLSV1Handler → FizzHandshakeHandler
 *                           → TLSClassifier → TLSConfigHandler
 *                           → [TLSMetricsHandler]
 *                           → PendingConnectionLimitHandler
 *                           → TLSConnectionAdapter
 *
 * StopTLSV1Handler is always present. It's a passthrough for connections that
 * didn't negotiate StopTLS V1, so the extra stage is cheap; this also keeps
 * the pipeline shape stable across hot-reload changes to whether StopTLS is
 * enabled.
 *
 * On an accepted ConnectionMessage, onRead converts to a TLSRequestMessage
 * and submits it to the tail adapter. When the adapter receives the resolved
 * transport back, it invokes the handoff registered here, which fires it onto
 * the outer pipeline as a ConnectionMessage. Exceptions raised inside the TLS
 * pipeline are routed back here the same way and re-fired onto the outer
 * pipeline so its teardown still observes inner failures.
 *
 * Hot-reload of TLS state is pull-based: the Observer passed at construction
 * is threaded into the pipeline and snapshotted there per accepted connection.
 *
 * The pending-connection limit is enforced once around the entire inner
 * pipeline, so every accepted connection consumes one slot while it moves
 * among the classifier, handshake, and StopTLS stages.
 *
 * Lifecycle:
 *   - The pipeline and its endpoints are built once in the constructor.
 *   - onPipelineActive / onPipelineInactive cascade activate() / deactivate()
 *     into the pipeline so handlers can drain in-flight peek/handshake/StopTLS
 *     helpers on shutdown.
 */
template <typename StaticContext>
class ConnectionTLSHandler {
 public:
  // sslPolicy must not be DISABLED — caller (ConnectionHandler) doesn't
  // install this handler at all in that case. tlsParamsObserver supplies
  // fizz context + thrift extension params + handshake timeout per accept.
  ConnectionTLSHandler(
      folly::EventBase& evb,
      fast_security::SSLPolicy sslPolicy,
      folly::observer::Observer<std::shared_ptr<const fast_security::TLSParams>>
          tlsParamsObserver,
      uint32_t maxPendingConnectionsPerWorker,
      tls_pipeline::TLSStatsShard* stats = nullptr)
      : innerPipeline_(buildTLSConnectionPipeline(
            evb,
            sslPolicy,
            std::move(tlsParamsObserver),
            maxPendingConnectionsPerWorker,
            stats,
            this,
            [](void* self,
               folly::AsyncTransport::UniquePtr transport,
               folly::SocketAddress clientAddr,
               std::shared_ptr<const PeerSecurityInfo> peerSecurity) noexcept {
              return static_cast<ConnectionTLSHandler*>(self)->onResolved(
                  std::move(transport),
                  std::move(clientAddr),
                  std::move(peerSecurity));
            },
            [](void* self, folly::exception_wrapper&& e) noexcept {
              static_cast<ConnectionTLSHandler*>(self)->onInnerException(
                  std::move(e));
            })) {
    DCHECK(sslPolicy != fast_security::SSLPolicy::DISABLED)
        << "ConnectionTLSHandler should not be installed for DISABLED policy";
  }

  ~ConnectionTLSHandler() = default;
  ConnectionTLSHandler(const ConnectionTLSHandler&) = delete;
  ConnectionTLSHandler& operator=(const ConnectionTLSHandler&) = delete;
  ConnectionTLSHandler(ConnectionTLSHandler&&) = delete;
  ConnectionTLSHandler& operator=(ConnectionTLSHandler&&) = delete;

  template <typename Context>
  void handlerAdded(Context& ctx) noexcept {
    outerCtx_ = &ctx;
  }

  template <typename Context>
  void handlerRemoved(Context& /*ctx*/) noexcept {
    outerCtx_ = nullptr;
  }

  template <typename Context>
  channel_pipeline::Result onRead(
      Context& /*ctx*/, channel_pipeline::TypeErasedBox&& msg) noexcept {
    auto incoming = msg.take<ConnectionMessage>();
    tls_pipeline::TLSRequestMessage request{
        .transport = std::move(incoming.transport),
        .clientAddr = std::move(incoming.clientAddr),
        .tlsParams = nullptr,
        .extension = nullptr,
        .peerSecurity = nullptr,
        .pendingConnectionPermit = {},
    };
    return innerPipeline_->fireWrite(
        channel_pipeline::erase_and_box(std::move(request)));
  }

  template <typename Context>
  channel_pipeline::Result onWrite(
      Context& ctx, channel_pipeline::TypeErasedBox&& msg) noexcept {
    return ctx.fireWrite(std::move(msg));
  }

  template <typename Context>
  void onException(Context& ctx, folly::exception_wrapper&& e) noexcept {
    ctx.fireException(std::move(e));
  }

  template <typename Context>
  void onPipelineActive(Context& /*ctx*/) noexcept {
    innerPipeline_->activate();
  }

  template <typename Context>
  void onPipelineInactive(Context& /*ctx*/) noexcept {
    innerPipeline_->deactivate();
  }

  template <typename Context>
  void onReadReady(Context& /*ctx*/) noexcept {}

  template <typename Context>
  void onWriteReady(Context& /*ctx*/) noexcept {}

 private:
  // Handoff registered on the tail adapter: the resolved transport returns
  // here and is fired onto the outer pipeline as a ConnectionMessage.
  channel_pipeline::Result onResolved(
      folly::AsyncTransport::UniquePtr transport,
      folly::SocketAddress clientAddr,
      std::shared_ptr<const PeerSecurityInfo> peerSecurity) noexcept {
    if (!outerCtx_) [[unlikely]] {
      return channel_pipeline::Result::Success;
    }
    auto guard = tls_pipeline::guardPipeline(outerCtx_);
    ConnectionMessage out{
        .transport = std::move(transport),
        .clientAddr = std::move(clientAddr),
        .peerSecurity = std::move(peerSecurity),
    };
    return outerCtx_->fireRead(channel_pipeline::erase_and_box(std::move(out)));
  }

  // Routed here by the tail adapter when the TLS pipeline raises an exception.
  // Re-fire onto the outer pipeline so its teardown observes inner failures.
  void onInnerException(folly::exception_wrapper&& e) noexcept {
    if (outerCtx_) {
      auto guard = tls_pipeline::guardPipeline(outerCtx_);
      outerCtx_->fireException(std::move(e));
    }
  }

  StaticContext* outerCtx_{nullptr};
  channel_pipeline::PipelineOwner innerPipeline_;
};

} // namespace apache::thrift::fast_thrift::connection::handler
