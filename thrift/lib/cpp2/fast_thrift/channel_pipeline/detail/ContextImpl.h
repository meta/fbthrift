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

#include <cstddef>
#include <cstdint>

#include <folly/CPortability.h>
#include <folly/io/async/EventBase.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/BufferAllocator.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Event.h>

namespace apache::thrift::fast_thrift::channel_pipeline {

// Forward declarations
class PipelineImpl;

namespace detail {

/**
 * ContextImpl is the concrete implementation of the Context API.
 *
 * Each handler in the pipeline gets its own ContextImpl instance.
 * The context provides:
 * - Fire methods to propagate messages to next handlers
 * - Access to the owning pipeline
 * - Buffer allocation
 * - EventBase access
 *
 * ContextImpl satisfies the ContextApi concept defined in context.h.
 *
 * Contexts are strictly owned by PipelineImpl and share its lifetime.
 * They do not need independent delayed destruction - the pipeline's
 * DestructorGuard protects all contexts during callback execution.
 */
class alignas(64) ContextImpl {
 public:
  /**
   * Constructs a context for a handler at the given index.
   *
   * @param pipeline The owning pipeline
   * @param event_base The EventBase this pipeline runs on
   * @param allocator The buffer allocator to use
   * @param handler_index The index of this handler in the pipeline
   * @param handler_id The unique ID for this handler
   */
  ContextImpl(
      PipelineImpl* pipeline,
      folly::EventBase* eventBase,
      void* allocator,
      size_t handlerIndex,
      HandlerId handlerId) noexcept;

  // Non-copyable, move-only (required for vector<ContextImpl> storage)
  ContextImpl(const ContextImpl&) = delete;
  ContextImpl& operator=(const ContextImpl&) = delete;
  ~ContextImpl() = default;
  ContextImpl(ContextImpl&&) noexcept = default;
  ContextImpl& operator=(ContextImpl&&) noexcept = default;

  // === Identity ===

  /**
   * Returns the handler ID for this context.
   */
  HandlerId handlerId() const noexcept { return handlerId_; }

  // === Fire methods (propagate to next handler) ===

  /**
   * Does nothing. PipelineImpl::activate() notifies every handler from the
   * head to the tail, so a handler does not pass activation on.
   */
  void activate() noexcept;

  /**
   * Fire a read event to the next inbound handler.
   * Direction: head → tail (increasing index)
   *
   * Uses cached next-handler pointer — no PipelineImpl round-trip.
   *
   * @param msg The message to propagate
   * @return Result from the next handler, or from the tail endpoint for the
   *         last handler
   */
  FOLLY_ALWAYS_INLINE Result fireRead(TypeErasedBox&& msg) noexcept {
    return nextReadFn_(nextHandler_, *nextCtx_, std::move(msg));
  }

  /**
   * Fire a write event to the next outbound handler.
   * Direction: tail → head (decreasing index)
   *
   * Uses cached prev-handler pointer — no PipelineImpl round-trip.
   *
   * @param msg The message to propagate
   * @return Result from the next handler, or from the head endpoint for
   *         handler 0
   */
  FOLLY_ALWAYS_INLINE Result fireWrite(TypeErasedBox&& msg) noexcept {
    return prevWriteFn_(prevHandler_, *prevCtx_, std::move(msg));
  }

  /**
   * Fire an exception event to the next inbound handler.
   * Direction: head → tail (increasing index)
   *
   * Uses cached next-handler pointer — no PipelineImpl round-trip.
   *
   * @param e The exception to propagate
   */
  FOLLY_ALWAYS_INLINE void fireException(
      folly::exception_wrapper&& e) noexcept {
    nextExceptionFn_(nextHandler_, *nextCtx_, std::move(e));
  }

  template <PipelineEvent E>
    requires std::is_void_v<typename E::Payload>
  void fireEvent() noexcept {
    fireTypeEvent(eventKey<E>(), nullptr);
  }

  template <PipelineEvent E>
    requires(!std::is_void_v<typename E::Payload>)
  void fireEvent(const typename E::Payload& payload) noexcept {
    fireTypeEvent(eventKey<E>(), &payload);
  }

  template <PipelineEvent E, std::size_t RouteIndex>
    requires std::is_void_v<typename E::Payload>
  void firePublishedEvent() noexcept {
    fireTypeEventFromRoute(RouteIndex, eventKey<E>(), nullptr);
  }

  template <PipelineEvent E, std::size_t RouteIndex>
    requires(!std::is_void_v<typename E::Payload>)
  void firePublishedEvent(const typename E::Payload& payload) noexcept {
    fireTypeEventFromRoute(RouteIndex, eventKey<E>(), &payload);
  }

  /**
   * Deactivates this handler and then each handler nearer the head
   * (decreasing index). From an endpoint's context it deactivates the whole
   * pipeline.
   */
  void deactivate() noexcept;

  // === Pipeline access ===

  /**
   * Returns a pointer to the owning pipeline.
   * Guaranteed non-null between handler_added() and handler_removed().
   */
  PipelineImpl* pipeline() const noexcept { return pipeline_; }

  // === Buffer allocation ===

  /**
   * Allocate a buffer using the pipeline's allocator.
   *
   * @param size The size of the buffer to allocate
   * @return A unique pointer to the allocated IOBuf
   */
  BytesPtr allocate(size_t size) noexcept;

  /**
   * Copy data into a new buffer using the pipeline's allocator.
   */
  BytesPtr copyBuffer(const void* data, size_t size) noexcept;

  /**
   * Wrap an externally owned buffer using the pipeline's allocator.
   * Ownership is consumed even when allocation fails.
   */
  BytesPtr takeOwnership(OwnedBuffer buffer) noexcept;

  // === Event loop access ===

  /**
   * Returns the EventBase this pipeline runs on.
   */
  folly::EventBase* eventBase() const noexcept { return eventBase_; }

  // === Lifecycle ===

  /**
   * Close the pipeline.
   * Delegates to pipeline->close().
   */
  void close() noexcept;

  // === Backpressure / Ready Registration ===

  /**
   * Register this handler to receive onWriteReady() callback.
   *
   * Call this after a fireWrite() that returned Result::Backpressure, or when
   * the handler waits for a state of its own, such as a full send window.
   *
   * The transport calls onWriteReady() only after it returned Backpressure to
   * a write. So a handler that waits for its own state must start the pass
   * itself with ctx.pipeline()->onWriteReady() when that state clears, or it
   * may never get onWriteReady().
   *
   * The handler must have a writeReadyHook_ member (detected at compile time).
   * No-op if the handler doesn't have a writeReadyHook_ or is already
   * registered.
   */
  void awaitWriteReady() noexcept;

  /**
   * Cancel pending await for write ready.
   *
   * Call this after successfully processing buffered messages to stop waiting
   * for onWriteReady().
   *
   * No-op if not currently awaiting.
   */
  void cancelAwaitWriteReady() noexcept;

  /**
   * Check if this handler is currently awaiting write ready.
   */
  bool isAwaitingWriteReady() const noexcept;

  /**
   * Register this handler to receive onReadReady() callback.
   *
   * Use this when an inbound handler needs to be notified to resume processing
   * after it returned Result::Backpressure from onRead().
   */
  void awaitReadReady() noexcept;

  /**
   * Cancel pending await for read ready.
   */
  void cancelAwaitReadReady() noexcept;

  /**
   * Check if this handler is currently awaiting read ready.
   */
  bool isAwaitingReadReady() const noexcept;

  // === Internal (used by PipelineImpl) ===

  /**
   * Returns the handler index for this context.
   */
  size_t handlerIndex() const noexcept { return handlerIndex_; }

  /**
   * Raw storage for this context's persistent TypedContext view (see
   * TypedContext.h). A TypedContext is a reference wrapper, so its size and
   * alignment are independent of the registered state types — a pointer-sized,
   * pointer-aligned slot holds any of them. Populated once by the builder for
   * state-using pipelines (after contexts are finalized), left untouched
   * otherwise. Used only by the TypedContext helpers.
   */
  void* typedViewStorage() noexcept { return &typedViewStorage_; }

 private:
  void fireTypeEvent(EventKey key, const void* payload) noexcept;
  void fireTypeEventFromRoute(
      std::size_t routeIndex, EventKey key, const void* payload) noexcept;

  // The first cache line contains the pipeline/EventBase pointers and both
  // read/write dispatch triples used by continuation completion.
  PipelineImpl* pipeline_;
  folly::EventBase* eventBase_;

  Result (*nextReadFn_)(void*, ContextImpl&, TypeErasedBox&&) noexcept {
      nullptr};
  void* nextHandler_{nullptr};
  ContextImpl* nextCtx_{nullptr};

  Result (*prevWriteFn_)(void*, ContextImpl&, TypeErasedBox&&) noexcept {
      nullptr};
  void* prevHandler_{nullptr};
  ContextImpl* prevCtx_{nullptr};

  void* allocator_; // Type-erased BufferAllocator
  void* pipelineState_{nullptr}; // Type-erased pipeline-level state
  // Storage for the per-context TypedContext view (constructed by the builder
  // for state-using pipelines). Sized/aligned for a reference wrapper.
  alignas(void*) unsigned char typedViewStorage_[sizeof(void*)];
  size_t handlerIndex_;
  HandlerId handlerId_;

  void (*nextExceptionFn_)(
      void*, ContextImpl&, folly::exception_wrapper&&) noexcept {nullptr};

  // Offset of this publisher's compile-time-ordered routes. On 64-bit targets
  // this occupies former tail padding and keeps ContextImpl at two cache lines;
  // on 32-bit targets the context fits in one cache line.
  std::uint32_t typeEventRouteOffset_{0};

  friend class ::apache::thrift::fast_thrift::channel_pipeline::PipelineImpl;

  // TypedContext reads pipelineState_ to expose the compile-time-typed
  // state<T>() accessor.
  template <typename StateTuple>
  friend class TypedContext;
};

static_assert(sizeof(ContextImpl) <= 2 * alignof(ContextImpl));
static_assert(alignof(ContextImpl) == 64);

} // namespace detail
} // namespace apache::thrift::fast_thrift::channel_pipeline
