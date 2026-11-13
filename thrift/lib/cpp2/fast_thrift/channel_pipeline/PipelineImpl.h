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

#include <folly/CppAttributes.h>
#include <folly/io/async/DelayedDestruction.h>
#include <folly/io/async/DelayedDestructionBase.h>
#include <folly/io/async/EventBase.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Backpressure.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/BufferAllocator.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/EndpointAdapter.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Event.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineRef.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineStorage.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/ContextImpl.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/HandlerNode.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/ReadyWaiters.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <span>
#include <stdexcept>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace apache::thrift::fast_thrift::channel_pipeline {

template <typename EventSet>
class EventPublisherHandle;

namespace detail {
class HeapPipelineImpl;
template <std::size_t MaxHandlers>
class InlinePipelineImpl;

/**
 * Describes how to construct and destroy a pipeline-owned endpoint (head/tail)
 * or allocator whose construction is deferred to build(). Mirrors
 * HandlerConstructionOps: the object is either placement-constructed into the
 * pipeline allocation (inline storage flavors) or heap-allocated (Large), and
 * destroyed with the matching operation.
 */
struct EndpointConstructionOps {
  std::size_t size;
  std::size_t alignment;
  void* (*constructInline)(void* storage, void* args);
  void* (*constructHeap)(void* args);
  void (*destroyInline)(void*) noexcept;
  void (*destroyHeap)(void*) noexcept;
};

/**
 * Source for a pipeline endpoint/allocator. Exactly one mode is active:
 *   - external (ops == nullptr): `external` is a non-owning object pointer set
 *     by setHead/setTail/setAllocator.
 *   - owned (ops != nullptr): the object is constructed at build() via `ops`,
 *     forwarding the decay-copied constructor arguments held in `args`.
 */
struct EndpointSource {
  void* external{nullptr};
  const EndpointConstructionOps* ops{nullptr};
  std::unique_ptr<void, void (*)(void*)> args{nullptr, nullptr};

  bool present() const noexcept {
    return ops != nullptr || external != nullptr;
  }
};

// Point an endpoint source at an externally-owned object. Overrides any prior
// owning configuration and drops its deferred arguments.
template <typename T>
void setExternalEndpoint(EndpointSource& src, T* obj) noexcept {
  src.external = static_cast<void*>(obj);
  src.ops = nullptr;
  src.args.reset();
}

// Configure an endpoint source to construct and own a T at build(). The
// constructor arguments are decay-copied into src.args now; the shared
// EndpointConstructionOps knows how to construct T inline or on the heap and
// how to destroy it. Used by both the dynamic and static pipeline builders.
template <typename T, typename... Args>
void prepareOwnedEndpoint(EndpointSource& src, Args&&... args) {
  static_assert(
      std::is_constructible_v<T, Args...>,
      "emplace*(): the endpoint/allocator type is not constructible from the "
      "given arguments");
  using ArgsTuple = std::tuple<StoredHandlerArg<Args>...>;

  src.external = nullptr;
  if constexpr (sizeof...(Args) != 0) {
    auto storedArgs = std::make_unique<ArgsTuple>(
        storeHandlerArg<Args>(std::forward<Args>(args))...);
    src.args = std::unique_ptr<void, void (*)(void*)>(
        storedArgs.release(),
        +[](void* p) { delete static_cast<ArgsTuple*>(p); });
  } else {
    src.args.reset();
  }

  static constexpr EndpointConstructionOps kOps{
      sizeof(T),
      alignof(T),
      [](void* storage, void* argsPtr) -> void* {
        if constexpr (sizeof...(Args) == 0) {
          (void)argsPtr;
          return std::construct_at(static_cast<T*>(storage));
        } else {
          auto& stored = *static_cast<ArgsTuple*>(argsPtr);
          return std::apply(
              [storage](auto&... a) {
                return std::construct_at(
                    static_cast<T*>(storage),
                    forwardStoredHandlerArg<Args>(a)...);
              },
              stored);
        }
      },
      [](void* argsPtr) -> void* {
        if constexpr (sizeof...(Args) == 0) {
          (void)argsPtr;
          return new T();
        } else {
          auto& stored = *static_cast<ArgsTuple*>(argsPtr);
          return std::apply(
              [](auto&... a) {
                return new T(forwardStoredHandlerArg<Args>(a)...);
              },
              stored);
        }
      },
      [](void* p) noexcept { std::destroy_at(static_cast<T*>(p)); },
      [](void* p) noexcept { delete static_cast<T*>(p); }};
  src.ops = &kOps;
}
} // namespace detail

/**
 * PipelineImpl is the concrete implementation of the Pipeline concept.
 *
 * The pipeline owns all handlers and their contexts. It provides:
 * - Fire from head/tail: fireRead/fireWrite to start the chain
 * - Fire to specific handler: sendRead/sendWrite by handler ID
 * - Context lookup: context(id) to get a handler's context
 * - Head/Tail integration: Routes to head and tail endpoint adapters
 * - Backpressure tracking: Tracks which handlers have pending ready callbacks
 *
 * Handlers are stored in a contiguous array for cache friendliness.
 * Contexts are stored in parallel (one per handler).
 *
 * Inherits from DelayedDestruction for safe async cleanup. This ensures the
 * pipeline is not destroyed while handler callbacks are executing. All methods
 * that invoke callbacks use DestructorGuard to defer destruction.
 */
class alignas(detail::ContextImpl) PipelineImpl
    : public folly::DelayedDestruction {
 public:
  using Ptr = folly::DelayedDestructionUniquePtr<PipelineImpl>;
  using Guard = folly::DelayedDestruction::DestructorGuard;

  /**
   * Pipeline lifecycle state.
   *
   *   Inactive  initial state; also re-entered after deactivate()
   *   Active    after activate() ran the onPipelineActive cascade
   *   Closed    handlers removed; head/tail pointers may dangle; terminal
   *
   * Transitions:
   *   Inactive  --activate()-->   Active
   *   Active    --deactivate()--> Inactive
   *   any       --close()-->      Closed
   *
   * activate()/deactivate() are idempotent: a second activate() in Active
   * or deactivate() in Inactive is a no-op (no re-cascade). close() is
   * also idempotent and is terminal — no transitions out.
   */
  enum class State : uint8_t {
    Inactive,
    Active,
    Closed,
  };

  template <typename Storage, typename HandlerContainer>
  static Ptr create(
      folly::EventBase* eventBase,
      HandlerContainer&& handlers,
      detail::EndpointSource headHandler,
      detail::EndpointSource tailHandler,
      detail::EndpointSource allocator);

  // Non-copyable
  PipelineImpl(const PipelineImpl&) = delete;
  PipelineImpl& operator=(const PipelineImpl&) = delete;
  PipelineImpl(PipelineImpl&&) = delete;
  PipelineImpl& operator=(PipelineImpl&&) = delete;

  // === Fire from head/tail handler ===

  /**
   * Activate the pipeline, notifying all handlers.
   *
   * Called by the endpoint adapter after the pipeline is fully wired up
   * and the connection is established.
   */
  void activate() noexcept;

  /**
   * Deactivate the pipeline, notifying all handlers.
   *
   * Called by the endpoint when the connection is lost.
   * Handlers should reset connection-specific state.
   */
  void deactivate() noexcept;

  /**
   * Fire a read event starting from the read-entry handler.
   */
  Result fireRead(TypeErasedBox&& msg) noexcept;

  /**
   * Fire a write event starting from the write-entry handler.
   */
  Result fireWrite(TypeErasedBox&& msg) noexcept;

  /**
   * Fire an exception event starting from the read-entry handler.
   */
  void fireException(folly::exception_wrapper&& e) noexcept;

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

  template <typename EventSet>
    requires kIsEventSet<EventSet>
  EventPublisherHandle<EventSet> bindEvents() noexcept;
  BoundEventRoute bindEvent(EventKey key) noexcept;
  void fireBoundEvent(BoundEventRoute route, const void* payload) noexcept;

  // === Fire to specific handler ===

  /**
   * Fire a read event to a specific handler by ID.
   */
  Result sendRead(HandlerId handlerId, TypeErasedBox&& msg) noexcept;

  /**
   * Fire a write event to a specific handler by ID.
   */
  Result sendWrite(HandlerId handlerId, TypeErasedBox&& msg) noexcept;

  /**
   * Fire an exception to a specific handler by ID.
   */
  void sendException(
      HandlerId handlerId, folly::exception_wrapper&& e) noexcept;

  // === Context lookup ===

  /**
   * Get a context by handler ID.
   * Returns nullptr if handler not found.
   */
  detail::ContextImpl* context(HandlerId handlerId) noexcept;

  // === Lifecycle ===

  /**
   * Close the pipeline.
   * Calls handlerRemoved in reverse order.
   */
  void close() noexcept;

  // === Pipeline composition (binding) ===

  /**
   * Chain `peer` onto this pipeline's tail. Binding changes nothing about how
   * this pipeline's handlers or its tail endpoint run — they execute
   * identically whether or not a peer is bound. The only effect is where the
   * tail endpoint's own `ctx.fireRead(...)` goes (propagation past the tail):
   * bound, the message is run through `TAdapter::adaptRead` (the default
   * ForwardingAdapter returns it unchanged) and fired into `peer` via fireRead,
   * and `peer`'s Result propagates back; unbound, it returns Result::Error
   * (nothing downstream). `adaptHead` is the outbound mirror — it sets where
   * the head endpoint's `ctx.fireWrite(...)` (past the head) goes.
   *
   * `TAdapter` is a stateless seam conversion selected by type; neither
   * pipeline's type knows the other. Bind records only a function pointer to
   * its static `adaptRead`/`adaptWrite`, so it allocates nothing and holds no
   * adapter object.
   *
   * The PipelineRef overload is non-owning: the caller keeps `peer` alive for
   * as long as this pipeline may fire into it. The PipelineOwner overload
   * transfers ownership to this pipeline, so `peer` is torn down together with
   * it — this is how a composed chain roots the inner pipeline's lifetime in
   * the outer one.
   *
   * `peer` must share this pipeline's EventBase (crossings are direct, same-
   * thread calls); binding across EventBases is rejected.
   */
  template <SeamReadAdapter TAdapter = ForwardingAdapter>
  void adaptTail(PipelineRef peer) noexcept {
    checkBindPeer(peer);
    boundTailPeer_ = peer;
    tailConvert_ = &TAdapter::adaptRead;
  }

  template <SeamReadAdapter TAdapter = ForwardingAdapter>
  void adaptTail(PipelineOwner peer) noexcept {
    ownedTailPeer_ = std::move(peer);
    adaptTail<TAdapter>(ownedTailPeer_.ref());
  }

  template <SeamWriteAdapter TAdapter = ForwardingAdapter>
  void adaptHead(PipelineRef peer) noexcept {
    checkBindPeer(peer);
    boundHeadPeer_ = peer;
    headConvert_ = &TAdapter::adaptWrite;
  }

  template <SeamWriteAdapter TAdapter = ForwardingAdapter>
  void adaptHead(PipelineOwner peer) noexcept {
    ownedHeadPeer_ = std::move(peer);
    adaptHead<TAdapter>(ownedHeadPeer_.ref());
  }

  // Terminal dispatch for propagation past the tail/head endpoint — where the
  // tail endpoint's ctx.fireRead and the head endpoint's ctx.fireWrite land.
  // Routes to the bound peer, or returns Error when nothing is bound.
  Result forwardReadPastTail(TypeErasedBox&& msg) noexcept;
  Result forwardWritePastHead(TypeErasedBox&& msg) noexcept;

  // === Internal methods (used by ContextImpl) ===

  /**
   * Fire read starting from a specific index.
   * Called by ContextImpl::fireRead().
   */
  Result fireReadFromIndex(size_t index, TypeErasedBox&& msg) noexcept;

  /**
   * Fire write starting from a specific index.
   * Called by ContextImpl::fireWrite().
   */
  Result fireWriteFromIndex(size_t index, TypeErasedBox&& msg) noexcept;

  /**
   * Fire exception starting from a specific index.
   * Called by ContextImpl::fireException().
   */
  void fireExceptionFromIndex(
      size_t index, folly::exception_wrapper&& e) noexcept;

  /**
   * Deactivate handlers starting from a specific index.
   * Called by ContextImpl::deactivate().
   */
  void deactivateFromIndex(size_t index) noexcept;

  /**
   * Fire read to the tail handler.
   * Called when fireRead passes the last internal handler.
   */
  Result fireReadToTailHandler(TypeErasedBox&& msg) noexcept;

  /**
   * Fire write to the head handler.
   * Called when fireWrite passes the first internal handler.
   */
  Result fireWriteToHeadHandler(TypeErasedBox&& msg) noexcept;

  /**
   * Fire exception to the tail handler.
   * Called when fireException passes the last internal handler.
   */
  void fireExceptionToTailHandler(folly::exception_wrapper&& e) noexcept;

  /**
   * Allocate a buffer using the pipeline's allocator.
   */
  BytesPtr allocate(size_t size) noexcept;

  /**
   * Copy data into a buffer obtained from the pipeline's allocator.
   */
  BytesPtr copyBuffer(const void* data, size_t size) noexcept;

  /**
   * Wrap an externally owned buffer using the pipeline's allocator.
   * Ownership is consumed even when allocation fails.
   */
  BytesPtr takeOwnership(OwnedBuffer buffer) noexcept;

  /**
   * Get the EventBase this pipeline runs on.
   */
  folly::EventBase* eventBase() const noexcept { return eventBase_; }

  /** Return whether the pipeline has completed handler removal. */
  bool isClosed() const noexcept { return state_ == State::Closed; }

  /**
   * Get the number of handlers in the pipeline.
   */
  size_t handlerCount() const noexcept { return handlers_.size(); }

  // === Backpressure / Ready Notifications ===

  /**
   * Wakes the head endpoint, then each waiting handler from the head toward
   * the tail, then the tail. The pass ends early, without the tail, when the
   * head endpoint waits again or the pipeline closes.
   *
   * Each handler wakes at most once per pass. A handler that arms behind the
   * walk, also in its own callback, waits for the next pass. So a handler that
   * waits for a state of its own must start a pass itself when that state
   * clears.
   *
   * A call made during a pass runs as one more pass after it, unless the pass
   * ended early.
   */
  void onWriteReady() noexcept;

  /**
   * Check if any handlers have pending write backpressure.
   */
  bool hasPendingWriteReady() const noexcept {
    return !writeReadyWaiters_.empty();
  }

  /**
   * Check whether the head endpoint currently owns write backpressure.
   */
  bool isHeadWriteBackpressured() const noexcept {
    return writeReadyWaiters_.isArmed(handlers_.size());
  }

  /**
   * Called by head when it can accept more reads.
   * Propagates onReadReady() to all handlers with pending read
   * backpressure.
   */
  void onReadReady() noexcept;

  /**
   * Check if any handlers have pending read ready notifications.
   */
  bool hasPendingReadReady() const noexcept { return !readReadyList_.empty(); }

  // === Internal: ready registration for ContextImpl ===

  /**
   * Whether the handler at this index has a write ready hook. Index
   * handlerCount() is the head endpoint, which counts as having one if it
   * takes onWriteReady().
   */
  bool hasWriteReadyHook(size_t index) const noexcept {
    if (index < handlers_.size()) {
      return handlers_[index].writeReadyHook_ != nullptr;
    }
    return index == handlers_.size() && headOnWriteReadyFn_ != nullptr;
  }

  /**
   * Get the read ready hook for a handler by index.
   * Returns nullptr if handler doesn't have a hook.
   */
  ReadReadyHook* handlerReadReadyHook(size_t index) noexcept {
    return index < handlers_.size() ? handlers_[index].readReadyHook_ : nullptr;
  }

  /**
   * Get the read ready list for registration.
   */
  ReadReadyList& readReadyList() noexcept { return readReadyList_; }

 protected:
  PipelineImpl(
      folly::EventBase* eventBase,
      std::size_t handlerCount,
      void* headHandler,
      void* tailHandler,
      void* allocator) noexcept;

  // Protected destructor - must use destroy() or Ptr for cleanup.
  // Called via DelayedDestruction when guardCount reaches zero.
  ~PipelineImpl() override;

  // Override to ensure handlers are cleaned up before destruction.
  // Called by DelayedDestruction::destroy() when safe to destruct.
  // Note: We do NOT call callHandlerRemoved() here because it uses
  // DestructorGuard which would trigger infinite recursion.
  // Instead, close() is responsible for calling handlerRemoved.
  void onDelayedDestroy(bool delayed) override {
    if (delayed && !getDestroyPending()) {
      return;
    }
    if (state_ != State::Closed) {
      state_ = State::Closed;
      writeReadyWaiters_.clear();
      readReadyList_.clear();
      callHandlerRemovedImpl();
    }
    delete this;
  }

 private:
  detail::ReadyWaiters& writeReadyWaiters() noexcept {
    return writeReadyWaiters_;
  }

  struct TypeEventSlot {
    EventKey key{nullptr};
    TypeEventDispatch* subscribers{nullptr};
    std::size_t subscriberCount{0};
  };

  struct TypeEventRoute {
    EventKey key{nullptr};
    const TypeEventDispatch* subscribers{nullptr};
    std::size_t subscriberCount{0};
  };

  TypeEventSlot* findTypeEventSlot(EventKey key) noexcept;
  void fireTypeEvent(EventKey key, const void* payload) noexcept;
  const void* FOLLY_NULLABLE bindTypeEvent(EventKey key) noexcept;
  void fireBoundTypeEvent(const void* route, const void* payload) noexcept;
  void fireTypeEventFromRoute(
      std::uint32_t routeOffset,
      std::size_t routeIndex,
      EventKey key,
      const void* payload) noexcept;

  using HandlerIndexEntry = std::pair<HandlerId, size_t>;

  size_t lookupHandler(HandlerId handlerId) const noexcept;

  void initializeStorageViews(
      std::span<detail::HandlerNode> handlers,
      std::span<detail::ContextImpl> contexts,
      std::span<HandlerIndexEntry> handlerMap) noexcept;
  void linkTypeEventLists() noexcept;
  // Call handlerAdded for all handlers
  void callHandlerAdded() noexcept;
  // Call handlerRemoved for all handlers (reverse order)
  void callHandlerRemoved() noexcept;
  // Helper to call handlerRemoved without DestructorGuard.
  // Used by both callHandlerRemoved() and onDelayedDestroy().
  void callHandlerRemovedImpl() noexcept;
  void propagatePipelineState() noexcept;

  // Validate a bind peer: non-empty and sharing this pipeline's EventBase.
  void checkBindPeer(PipelineRef peer) const noexcept;

  folly::EventBase* eventBase_;
  std::span<detail::HandlerNode> handlers_;
  std::span<detail::ContextImpl> contexts_;
  std::span<HandlerIndexEntry> handlerMap_;
  detail::ContextImpl headCtx_;
  detail::ContextImpl tailCtx_;

  // Type-erased endpoint adapters
  void* headHandler_; // HeadHandler*
  void* tailHandler_; // TailHandler*
  void* allocator_; // BufferAllocator*

  // Tail handler callbacks
  Result (*tailOnReadFn_)(
      void*, detail::ContextImpl&, TypeErasedBox&&) noexcept {nullptr};
  void (*tailOnExceptionFn_)(void*, folly::exception_wrapper&&) noexcept {
      nullptr};
  void (*tailOnWriteReadyFn_)(void*) noexcept {nullptr};
  const EventKey* tailPublishedEvents_{nullptr};
  std::size_t tailPublishedEventCount_{0};
  const TypeEventSubscription* tailTypeSubscriptions_{nullptr};
  std::size_t tailTypeSubscriptionCount_{0};
  // Tail handler lifecycle callbacks
  void (*tailOnPipelineActiveFn_)(void*) noexcept {nullptr};
  void (*tailOnPipelineInactiveFn_)(void*) noexcept {nullptr};
  void (*tailHandlerAddedFn_)(void*) noexcept {nullptr};
  void (*tailHandlerRemovedFn_)(void*) noexcept {nullptr};

  // Head handler callbacks
  Result (*headOnWriteFn_)(
      void*, detail::ContextImpl&, TypeErasedBox&&) noexcept {nullptr};
  void (*headOnWriteReadyFn_)(void*, detail::ContextImpl&) noexcept {nullptr};
  void (*headOnReadReadyFn_)(void*) noexcept {nullptr};
  const EventKey* headPublishedEvents_{nullptr};
  std::size_t headPublishedEventCount_{0};
  const TypeEventSubscription* headTypeSubscriptions_{nullptr};
  std::size_t headTypeSubscriptionCount_{0};
  // Head handler lifecycle callbacks
  void (*headOnPipelineActiveFn_)(void*) noexcept {nullptr};
  void (*headOnPipelineInactiveFn_)(void*) noexcept {nullptr};
  void (*headHandlerAddedFn_)(void*) noexcept {nullptr};
  void (*headHandlerRemovedFn_)(void*) noexcept {nullptr};

  // Allocator callbacks
  BytesPtr (*allocateFn_)(void*, size_t) noexcept {nullptr};
  BytesPtr (*copyBufferFn_)(void*, const void*, size_t) noexcept {nullptr};
  BytesPtr (*takeOwnershipFn_)(void*, OwnedBuffer) noexcept {nullptr};

  // Cached entry-point dispatch for fireRead/fireWrite hot paths.
  // Points directly to the read-entry/write-entry handler's function pointer,
  // eliminating the vector index on the entry path.
  detail::HandlerNode::OnReadFn firstReadFn_{nullptr};
  detail::HandlerNode::OnExceptionFn firstExceptionFn_{nullptr};
  void* firstHandler_{nullptr};
  detail::ContextImpl* firstCtx_{nullptr};

  detail::HandlerNode::OnWriteFn lastWriteFn_{nullptr};
  void* lastHandler_{nullptr};
  detail::ContextImpl* lastCtx_{nullptr};

  State state_{State::Inactive};

  // Handlers awaiting write ready notifications, by handler index, and an
  // intrusive list of handlers awaiting read ready notifications.
  // Handlers self-register via ctx.awaitWriteReady()/ctx.awaitReadReady().
  // The write set has one more index, handlerCount(), for the head endpoint.
  detail::ReadyWaiters writeReadyWaiters_;
  ReadReadyList readReadyList_;
  std::size_t readReadyGeneration_{0};
  bool writeReadyDispatching_{false};
  bool writeReadyDispatchPending_{false};

  // Immutable open-address table built once from process-local type tokens.
  // Subscriber dispatch entries are grouped into contiguous per-event spans.
  std::unique_ptr<TypeEventSlot[]> typeEventTable_;
  std::size_t typeEventTableSize_{0};
  std::unique_ptr<TypeEventDispatch[]> typeEventDispatches_;
  std::size_t typeEventDispatchCount_{0};
  std::unique_ptr<TypeEventRoute[]> typeEventRoutes_;
  std::size_t typeEventRouteCount_{0};

  // Pipeline-level state — type-erased, owned by the pipeline.
  void* pipelineState_{nullptr};
  void (*pipelineStateDeleter_)(void*) noexcept {nullptr};

  // Optionally pipeline-owned endpoints/allocator (set via emplaceHead/
  // emplaceTail/emplaceAllocator). Null deleter means the component is external
  // and not owned here. Destroyed in ~PipelineImpl after all handlers and
  // contexts, allocator last, preserving the allocator-outlives-everything
  // invariant. For inline storage the object lives in the pipeline allocation
  // and the deleter only runs its destructor; for Large storage it is a heap
  // allocation the deleter frees.
  void* ownedHead_{nullptr};
  void (*ownedHeadDeleter_)(void*) noexcept {nullptr};
  void* ownedTail_{nullptr};
  void (*ownedTailDeleter_)(void*) noexcept {nullptr};
  void* ownedAllocator_{nullptr};
  void (*ownedAllocatorDeleter_)(void*) noexcept {nullptr};

  // Bound peer pipelines (set via adaptTail/adaptHead). boundTailPeer_/
  // boundHeadPeer_ are the targets that
  // forwardReadPastTail/forwardWritePastHead route to; empty until a bind. The
  // owned*Peer_ members are engaged only by the owning bind overload and keep
  // an owned peer alive for this pipeline's lifetime (destroyed with it,
  // rooting a composed chain's inner pipeline in the outer one).
  // *Convert_ is the seam adapter's static message conversion, set alongside
  // the peer at bind (non-null whenever the peer is bound); the pipeline
  // applies it and performs the injection itself. The identity default returns
  // unchanged.
  PipelineRef boundTailPeer_;
  PipelineRef boundHeadPeer_;
  PipelineOwner ownedTailPeer_;
  PipelineOwner ownedHeadPeer_;
  TypeErasedBox (*tailConvert_)(TypeErasedBox&&) noexcept {nullptr};
  TypeErasedBox (*headConvert_)(TypeErasedBox&&) noexcept {nullptr};

  // PipelineBuilder needs access to set up the pipeline
  template <
      typename HeadHandler,
      typename TailHandler,
      typename Allocator,
      typename Storage,
      typename StateTuple>
  friend class PipelineBuilder;
  friend class detail::HeapPipelineImpl;
  template <std::size_t>
  friend class detail::InlinePipelineImpl;

  // ContextImpl needs access to ready waiters for registration
  friend class detail::ContextImpl;
  template <typename EventSet>
  friend class EventPublisherHandle;
};

namespace detail {

class HeapPipelineImpl final : public PipelineImpl {
 public:
  HeapPipelineImpl(
      folly::EventBase* eventBase,
      std::vector<HandlerNode> handlers,
      EndpointSource headHandler,
      EndpointSource tailHandler,
      EndpointSource allocator)
      : PipelineImpl(
            eventBase,
            handlers.size(),
            resolveHeapEndpoint(headHandler),
            resolveHeapEndpoint(tailHandler),
            resolveHeapEndpoint(allocator)),
        handlerStorage_(std::move(handlers)) {
    recordOwnedHeapEndpoint(
        ownedHead_, ownedHeadDeleter_, headHandler, headHandler_);
    recordOwnedHeapEndpoint(
        ownedTail_, ownedTailDeleter_, tailHandler, tailHandler_);
    recordOwnedHeapEndpoint(
        ownedAllocator_, ownedAllocatorDeleter_, allocator, allocator_);
    for (auto& handler : handlerStorage_) {
      if (const auto* constructionOps = handler.pendingConstructionOps()) {
        constructionOps->constructHeap(handler);
      }
    }
    handlerStorage_.shrink_to_fit();
    contextStorage_.reserve(handlerStorage_.size());
    handlerIndexStorage_.reserve(handlerStorage_.size());
    for (std::size_t i = 0; i < handlerStorage_.size(); ++i) {
      contextStorage_.emplace_back(
          this, eventBase, allocator_, i, handlerStorage_[i].handlerId);
      handlerIndexStorage_.emplace_back(handlerStorage_[i].handlerId, i);
    }
    contextStorage_.shrink_to_fit();
    handlerIndexStorage_.shrink_to_fit();
    std::sort(handlerIndexStorage_.begin(), handlerIndexStorage_.end());
    initializeStorageViews(
        handlerStorage_, contextStorage_, handlerIndexStorage_);
  }

 private:
  // Resolve an endpoint source to an object pointer: the external pointer, or a
  // freshly heap-constructed owned object.
  static void* resolveHeapEndpoint(EndpointSource& src) {
    return src.ops ? src.ops->constructHeap(src.args.get()) : src.external;
  }

  // Record pipeline ownership of a heap-constructed endpoint so ~PipelineImpl
  // frees it. No-op for external components.
  static void recordOwnedHeapEndpoint(
      void*& ownedSlot,
      void (*&deleter)(void*) noexcept,
      const EndpointSource& src,
      void* objPtr) noexcept {
    if (src.ops) {
      ownedSlot = objPtr;
      deleter = src.ops->destroyHeap;
    }
  }

  std::vector<HandlerNode> handlerStorage_;
  std::vector<ContextImpl> contextStorage_;
  std::vector<HandlerIndexEntry> handlerIndexStorage_;
};

template <std::size_t MaxHandlers>
class alignas(ContextImpl) InlinePipelineImpl final : public PipelineImpl {
  struct Allocation {
    std::size_t extraBytes;
  };

  struct Layout {
    std::size_t handlersOffset;
    std::size_t contextsOffset;
    std::size_t handlerIndexOffset;
    std::size_t inlineHandlersOffset;
    // Offsets of pipeline-owned endpoints co-located in this allocation; only
    // meaningful when the corresponding source is owned.
    std::size_t headOffset;
    std::size_t tailOffset;
    std::size_t allocatorOffset;
    std::size_t allocationSize;
  };

 public:
  template <typename HandlerContainer>
  static InlinePipelineImpl* create(
      folly::EventBase* eventBase,
      HandlerContainer&& handlers,
      EndpointSource headHandler,
      EndpointSource tailHandler,
      EndpointSource allocator) {
    if (handlers.size() > MaxHandlers) {
      throw std::length_error("inline pipeline handler capacity exceeded");
    }
    const auto layout = makeLayout(
        std::forward<HandlerContainer>(handlers),
        headHandler,
        tailHandler,
        allocator);
    return new (Allocation{layout.allocationSize - sizeof(InlinePipelineImpl)})
        InlinePipelineImpl(
            eventBase,
            std::forward<HandlerContainer>(handlers),
            std::move(headHandler),
            std::move(tailHandler),
            std::move(allocator),
            layout);
  }

  ~InlinePipelineImpl() override {
    std::destroy(handlerMap_.begin(), handlerMap_.end());
    std::destroy(contexts_.begin(), contexts_.end());
    std::destroy(handlers_.begin(), handlers_.end());
  }

  InlinePipelineImpl(const InlinePipelineImpl&) = delete;
  InlinePipelineImpl& operator=(const InlinePipelineImpl&) = delete;
  InlinePipelineImpl(InlinePipelineImpl&&) = delete;
  InlinePipelineImpl& operator=(InlinePipelineImpl&&) = delete;

  static void* operator new(std::size_t size, Allocation allocation) {
    return ::operator new(
        size + allocation.extraBytes,
        std::align_val_t{alignof(InlinePipelineImpl)});
  }

  static void operator delete(void* ptr, Allocation) noexcept {
    ::operator delete(ptr, std::align_val_t{alignof(InlinePipelineImpl)});
  }

  static void operator delete(void* ptr) noexcept {
    ::operator delete(ptr, std::align_val_t{alignof(InlinePipelineImpl)});
  }

 private:
  template <typename HandlerContainer>
  InlinePipelineImpl(
      folly::EventBase* eventBase,
      HandlerContainer&& sourceHandlers,
      EndpointSource headHandler,
      EndpointSource tailHandler,
      EndpointSource allocator,
      Layout layout)
      : PipelineImpl(
            eventBase,
            sourceHandlers.size(),
            endpointBasePtr(this, headHandler, layout.headOffset),
            endpointBasePtr(this, tailHandler, layout.tailOffset),
            endpointBasePtr(this, allocator, layout.allocatorOffset)) {
    auto* bytes = reinterpret_cast<std::byte*>(this);
    // Owned endpoints are placement-constructed into the reserved tail storage.
    // On success ~PipelineImpl destroys them (allocator last); if a later step
    // throws, the fully-constructed base subobject's destructor runs them via
    // the slots set here, so the catch below only unwinds handlers/contexts.
    constructInlineEndpoint(
        headHandler, bytes + layout.headOffset, ownedHead_, ownedHeadDeleter_);
    constructInlineEndpoint(
        tailHandler, bytes + layout.tailOffset, ownedTail_, ownedTailDeleter_);
    constructInlineEndpoint(
        allocator,
        bytes + layout.allocatorOffset,
        ownedAllocator_,
        ownedAllocatorDeleter_);
    auto* handlerData =
        reinterpret_cast<HandlerNode*>(bytes + layout.handlersOffset);
    auto* contextData =
        reinterpret_cast<ContextImpl*>(bytes + layout.contextsOffset);
    auto* handlerIndexData =
        reinterpret_cast<HandlerIndexEntry*>(bytes + layout.handlerIndexOffset);
    const auto count = sourceHandlers.size();
    auto inlineHandlerOffset = layout.inlineHandlersOffset;
    std::size_t handlersConstructed = 0;
    std::size_t contextsConstructed = 0;
    std::size_t handlerIndexesConstructed = 0;

    try {
      for (std::size_t i = 0; i < count; ++i) {
        std::construct_at(
            handlerData + i,
            std::move(std::forward<HandlerContainer>(sourceHandlers)[i]));
        ++handlersConstructed;
        if (const auto* constructionOps =
                handlerData[i].pendingConstructionOps()) {
          inlineHandlerOffset =
              alignUp(inlineHandlerOffset, constructionOps->alignment);
          constructionOps->constructInline(
              handlerData[i], bytes + inlineHandlerOffset);
          inlineHandlerOffset += constructionOps->size;
        }
        std::construct_at(
            contextData + i,
            this,
            eventBase,
            allocator_,
            i,
            handlerData[i].handlerId);
        ++contextsConstructed;
        std::construct_at(handlerIndexData + i, handlerData[i].handlerId, i);
        ++handlerIndexesConstructed;
      }
      if (count != 0) {
        std::sort(handlerIndexData, handlerIndexData + count);
      }
    } catch (...) {
      std::destroy_n(handlerIndexData, handlerIndexesConstructed);
      std::destroy_n(contextData, contextsConstructed);
      std::destroy_n(handlerData, handlersConstructed);
      throw;
    }
    initializeStorageViews(
        std::span<HandlerNode>{handlerData, count},
        std::span<ContextImpl>{contextData, count},
        std::span<HandlerIndexEntry>{handlerIndexData, count});
  }

  static constexpr std::size_t alignUp(
      std::size_t size, std::size_t alignment) noexcept {
    return (size + alignment - 1) & ~(alignment - 1);
  }

  // Base pointer for an endpoint: its reserved slot inside this allocation when
  // owned, otherwise the external pointer. Only does pointer arithmetic, so it
  // is safe to call on `this` from the base-class member initializer.
  static void* endpointBasePtr(
      InlinePipelineImpl* self,
      const EndpointSource& src,
      std::size_t offset) noexcept {
    return src.ops ? reinterpret_cast<std::byte*>(self) + offset : src.external;
  }

  // Placement-construct an owned endpoint into its reserved slot and record the
  // destructor so ~PipelineImpl tears it down. No-op for external components.
  static void constructInlineEndpoint(
      EndpointSource& src,
      void* storage,
      void*& ownedSlot,
      void (*&deleter)(void*) noexcept) {
    if (src.ops) {
      ownedSlot = src.ops->constructInline(storage, src.args.get());
      deleter = src.ops->destroyInline;
    }
  }

  template <typename HandlerContainer>
  static Layout makeLayout(
      const HandlerContainer& handlers,
      const EndpointSource& headHandler,
      const EndpointSource& tailHandler,
      const EndpointSource& allocator) noexcept {
    const auto handlerCount = handlers.size();
    const auto handlersOffset =
        alignUp(sizeof(InlinePipelineImpl), alignof(HandlerNode));
    const auto contextsOffset = alignUp(
        handlersOffset + handlerCount * sizeof(HandlerNode),
        alignof(ContextImpl));
    const auto handlerIndexOffset = alignUp(
        contextsOffset + handlerCount * sizeof(ContextImpl),
        alignof(HandlerIndexEntry));
    const auto inlineHandlersOffset =
        handlerIndexOffset + handlerCount * sizeof(HandlerIndexEntry);
    auto allocationSize = inlineHandlersOffset;
    for (const auto& handler : handlers) {
      if (const auto* storageOps = handler.pendingConstructionOps()) {
        allocationSize = alignUp(allocationSize, storageOps->alignment);
        allocationSize += storageOps->size;
      }
    }
    const auto reserve = [&allocationSize](const EndpointSource& src) {
      std::size_t offset = 0;
      if (src.ops) {
        allocationSize = alignUp(allocationSize, src.ops->alignment);
        offset = allocationSize;
        allocationSize += src.ops->size;
      }
      return offset;
    };
    const auto headOffset = reserve(headHandler);
    const auto tailOffset = reserve(tailHandler);
    const auto allocatorOffset = reserve(allocator);
    return {
        handlersOffset,
        contextsOffset,
        handlerIndexOffset,
        inlineHandlersOffset,
        headOffset,
        tailOffset,
        allocatorOffset,
        allocationSize};
  }
};

} // namespace detail

template <typename Storage, typename HandlerContainer>
PipelineImpl::Ptr PipelineImpl::create(
    folly::EventBase* eventBase,
    HandlerContainer&& handlers,
    detail::EndpointSource headHandler,
    detail::EndpointSource tailHandler,
    detail::EndpointSource allocator) {
  static_assert(
      pipeline_storage::Valid<Storage>,
      "Storage must be a pipeline_storage flavor");
  if constexpr (std::same_as<Storage, pipeline_storage::Automatic>) {
    if (handlers.size() <= pipeline_storage::Small::maxHandlers) {
      return create<pipeline_storage::Small>(
          eventBase,
          std::forward<HandlerContainer>(handlers),
          std::move(headHandler),
          std::move(tailHandler),
          std::move(allocator));
    }
    if (handlers.size() <= pipeline_storage::Medium::maxHandlers) {
      return create<pipeline_storage::Medium>(
          eventBase,
          std::forward<HandlerContainer>(handlers),
          std::move(headHandler),
          std::move(tailHandler),
          std::move(allocator));
    }
    std::vector<detail::HandlerNode> heapHandlers;
    heapHandlers.reserve(handlers.size());
    for (auto& handler : handlers) {
      heapHandlers.push_back(std::move(handler));
    }
    return create<pipeline_storage::Large>(
        eventBase,
        std::move(heapHandlers),
        std::move(headHandler),
        std::move(tailHandler),
        std::move(allocator));
  } else if constexpr (std::same_as<Storage, pipeline_storage::Large>) {
    return Ptr(new detail::HeapPipelineImpl(
        eventBase,
        std::forward<HandlerContainer>(handlers),
        std::move(headHandler),
        std::move(tailHandler),
        std::move(allocator)));
  } else {
    return Ptr(
        detail::InlinePipelineImpl<Storage::maxHandlers>::create(
            eventBase,
            std::forward<HandlerContainer>(handlers),
            std::move(headHandler),
            std::move(tailHandler),
            std::move(allocator)));
  }
}

template <PipelineEvent... Evs>
class EventPublisherHandle<Events<Evs...>> {
 public:
  EventPublisherHandle() noexcept = default;
  template <PipelineEvent E>
    requires(
        (std::same_as<E, Evs> || ...) && std::is_void_v<typename E::Payload>)
  void fire() const noexcept {
    pipeline_->fireBoundEvent(
        routes_[Events<Evs...>::template index<E>], nullptr);
  }

  template <PipelineEvent E>
    requires(
        (std::same_as<E, Evs> || ...) && (!std::is_void_v<typename E::Payload>))
  void fire(const typename E::Payload& payload) const noexcept {
    pipeline_->fireBoundEvent(
        routes_[Events<Evs...>::template index<E>], &payload);
  }

 private:
  explicit EventPublisherHandle(PipelineImpl& pipeline) noexcept
      : pipeline_(&pipeline), routes_{pipeline.bindEvent(eventKey<Evs>())...} {}

  PipelineImpl* pipeline_{nullptr};
  std::array<BoundEventRoute, sizeof...(Evs)> routes_{};

  friend class PipelineImpl;
};

template <typename EventSet>
  requires kIsEventSet<EventSet>
EventPublisherHandle<EventSet> PipelineImpl::bindEvents() noexcept {
  return EventPublisherHandle<EventSet>{*this};
}

} // namespace apache::thrift::fast_thrift::channel_pipeline
