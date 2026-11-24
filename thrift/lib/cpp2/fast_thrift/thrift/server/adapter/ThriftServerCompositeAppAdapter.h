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

#include <concepts>
#include <functional>
#include <memory>
#include <string_view>
#include <vector>

#include <folly/ExceptionWrapper.h>
#include <folly/Portability.h>
#include <folly/Synchronized.h>
#include <folly/io/async/DelayedDestruction.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineRef.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/adapter/ThriftServerAppAdapter.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/adapter/util/ThriftServerCompositeRoutingTable.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/Event.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/ServerAppAdapter.h>

namespace apache::thrift::fast_thrift::thrift {

/**
 * ThriftServerCompositeAppAdapter — non-owning pipeline tail that routes
 * inbound requests across an ordered list of child adapters, dispatched by
 * method name.
 *
 * Children are borrowed. The caller owns each child through its concrete
 * `T::Ptr` and is responsible for keeping children alive at least until
 * the composite is destroyed. The composite stores each child as its common
 * ThriftServerAppAdapter base plus type-erased lifecycle hooks.
 *
 * Routing is method-name only. The factory supplies an immutable flat routing
 * table shared by every connection. Each entry identifies the child and an
 * unbound dispatch thunk, so dispatch needs one lookup and one indirect call.
 * Unknown methods are answered with a
 * ResponseRpcErrorCode::UNKNOWN_METHOD framework error
 * fired through the composite's own pipeline reference.
 *
 * Satisfies TailEndpointHandler. The pipeline itself must be templated on
 * ThriftServerCompositeAppAdapter so the tail concept resolves.
 */
class ThriftServerCompositeAppAdapter final : public folly::DelayedDestruction {
 public:
  using Ptr = std::unique_ptr<
      ThriftServerCompositeAppAdapter,
      folly::DelayedDestruction::Destructor>;

  explicit ThriftServerCompositeAppAdapter(
      std::shared_ptr<const ThriftServerCompositeRoutingTable> routingTable =
          {})
      : routingTable_(std::move(routingTable)) {}

  ThriftServerCompositeAppAdapter(const ThriftServerCompositeAppAdapter&) =
      delete;
  ThriftServerCompositeAppAdapter& operator=(
      const ThriftServerCompositeAppAdapter&) = delete;
  ThriftServerCompositeAppAdapter(ThriftServerCompositeAppAdapter&&) = delete;
  ThriftServerCompositeAppAdapter& operator=(
      ThriftServerCompositeAppAdapter&&) = delete;

  // Install a callback invoked once when the pipeline goes inactive (or, as
  // a fallback, when this adapter is destroyed). Used by the owning
  // ConnectionHandler to learn that the per-connection state is done so it
  // can erase its map entry.
  void setCloseCallback(std::function<void()> cb);

  // Register a child. Caller retains ownership.
  //
  // Request dispatch uses the shared routing table when available. The
  // compatibility path for hand-written factories scans children and calls
  // child->onRead(). Lifecycle hooks are forwarded through kLifecycleVTable.
  template <typename T>
    requires ServerInboundAppAdapter<T> && ServerComposableAppAdapter<T> &&
      std::derived_from<T, ThriftServerAppAdapter>
  void addChild(T* child) {
    DCHECK(child != nullptr);
    children_.push_back(ChildHook{child, child, &kLifecycleVTable<T>});
  }

  // === TailEndpointHandler ===
  template <typename Context>
  channel_pipeline::Result onRead(
      Context&, channel_pipeline::TypeErasedBox&& msg) noexcept {
    return onReadImpl(std::move(msg));
  }
  void onException(folly::exception_wrapper&& e) noexcept;
  template <channel_pipeline::PipelineRefTarget P>
  void setPipeline(P* pipeline) noexcept {
    DCHECK(pipeline != nullptr);
    setPipeline(channel_pipeline::PipelineRef(*pipeline));
  }
  void setPipeline(channel_pipeline::PipelineRef pipeline) noexcept;
  // Drops the pipeline reference and releases the DestructorGuard taken in
  // setPipeline. Must be called before the adapter is destroyed.
  void resetPipeline() noexcept;
  channel_pipeline::PipelineRef pipeline() const noexcept { return pipeline_; }
  void handlerAdded() noexcept;
  void handlerRemoved() noexcept;
  void onPipelineActive() noexcept;
  void onPipelineInactive() noexcept;
  using PublishedEvents =
      channel_pipeline::Events<ThriftServerCloseConnectionEvent>;
  using SubscribedEvents =
      channel_pipeline::Events<ThriftServerConnectionClosedEvent>;

  template <channel_pipeline::PipelineEvent E>
    requires std::same_as<E, ThriftServerConnectionClosedEvent>
  void on() noexcept {
    onConnectionClosed();
  }
  void onWriteReady() noexcept;

  // Initiate connection close. Internally fires a
  // ThriftServerCloseConnectionEvent pipeline event; the
  // pipeline-resident ThriftServerConnectionCloseHandler picks it up and runs
  // the terminal state machine. The user closeCallback fires when the
  // connection has fully settled. No-op if the pipeline is not wired.
  void close() noexcept;

 protected:
  ~ThriftServerCompositeAppAdapter() override;

 private:
  void onConnectionClosed() noexcept;
  channel_pipeline::Result onReadImpl(
      channel_pipeline::TypeErasedBox&& msg) noexcept;

  struct LifecycleVTable {
    void (*setPipeline)(void*, channel_pipeline::PipelineRef) noexcept;
    void (*resetPipeline)(void*) noexcept;
    void (*onException)(void*, folly::exception_wrapper&&) noexcept;
    void (*handlerAdded)(void*) noexcept;
    void (*handlerRemoved)(void*) noexcept;
    void (*onPipelineActive)(void*) noexcept;
    void (*onPipelineInactive)(void*) noexcept;
    void (*onWriteReady)(void*) noexcept;
  };

  template <typename T>
  static constexpr LifecycleVTable kLifecycleVTable{
      +[](void* p, channel_pipeline::PipelineRef pipe) noexcept {
        static_cast<T*>(p)->setPipeline(pipe);
      },
      +[](void* p) noexcept { static_cast<T*>(p)->resetPipeline(); },
      +[](void* p, folly::exception_wrapper&& e) noexcept {
        static_cast<T*>(p)->onException(std::move(e));
      },
      +[](void* p) noexcept { static_cast<T*>(p)->handlerAdded(); },
      +[](void* p) noexcept { static_cast<T*>(p)->handlerRemoved(); },
      +[](void* p) noexcept { static_cast<T*>(p)->onPipelineActive(); },
      +[](void* p) noexcept { static_cast<T*>(p)->onPipelineInactive(); },
      +[](void* p) noexcept { static_cast<T*>(p)->onWriteReady(); },
  };

  FOLLY_NOINLINE channel_pipeline::Result writeUnknownMethodError(
      uint32_t streamId,
      std::string_view methodName,
      ThriftRequestContextPtr requestContext) noexcept;
  FOLLY_NOINLINE channel_pipeline::Result writeWrongRpcKindError(
      uint32_t streamId,
      apache::thrift::RpcKind kind,
      ThriftRequestContextPtr requestContext) noexcept;
  channel_pipeline::Result writeFrameworkError(
      ThriftServerResponseMessage&& message,
      ThriftRequestContextPtr requestContext) noexcept;

  struct ChildHook {
    ThriftServerAppAdapter* adapter;
    void* owner;
    const LifecycleVTable* vtable;
  };

  std::vector<ChildHook> children_;
  std::shared_ptr<const ThriftServerCompositeRoutingTable> routingTable_;
  channel_pipeline::PipelineRef pipeline_;
  // Keeps pipeline_ alive for the composite's lifetime so late writes
  // (writeUnknownMethodError, startDrain) and onPipelineInactive's EVB
  // hop cannot dereference a freed pipeline. Released by resetPipeline().
  channel_pipeline::PipelineGuard pipelineGuard_;
  folly::Synchronized<std::function<void()>> closeCallback_;
};

} // namespace apache::thrift::fast_thrift::thrift
