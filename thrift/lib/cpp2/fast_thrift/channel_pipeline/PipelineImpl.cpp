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

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineImpl.h>

#include <algorithm>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <optional>

#include <folly/CppAttributes.h>

#if defined(__GNUC__) || defined(__clang__)
#define PIPELINE_HOT_PATH __attribute__((flatten))
#else
#define PIPELINE_HOT_PATH
#endif

// After close(), handlers have been removed and the head/tail raw pointers
// may dangle (their owning structs can be destroyed without nulling our
// raw pointers). Refuse all dispatches.
#define RETURN_IF_CLOSED(...)                      \
  do {                                             \
    if (FOLLY_UNLIKELY(state_ == State::Closed)) { \
      return __VA_ARGS__;                          \
    }                                              \
  } while (0)

namespace {

using apache::thrift::fast_thrift::channel_pipeline::EventKey;
using apache::thrift::fast_thrift::channel_pipeline::PipelineImpl;
using apache::thrift::fast_thrift::channel_pipeline::Result;
using apache::thrift::fast_thrift::channel_pipeline::TypeErasedBox;
using apache::thrift::fast_thrift::channel_pipeline::detail::ContextImpl;

Result tailHandlerReadFn(
    void* pipeline, ContextImpl& /*ctx*/, TypeErasedBox&& msg) noexcept {
  return static_cast<PipelineImpl*>(pipeline)->fireReadToTailHandler(
      std::move(msg));
}

Result headHandlerWriteFn(
    void* pipeline, ContextImpl& /*ctx*/, TypeErasedBox&& msg) noexcept {
  return static_cast<PipelineImpl*>(pipeline)->fireWriteToHeadHandler(
      std::move(msg));
}

void tailHandlerExceptionFn(
    void* pipeline,
    ContextImpl& /*ctx*/,
    folly::exception_wrapper&& e) noexcept {
  static_cast<PipelineImpl*>(pipeline)->fireExceptionToTailHandler(
      std::move(e));
}

// Terminal for a read propagating past the tail endpoint (the tail endpoint's
// own ctx.fireRead). Routes to a bound peer, else reports no downstream.
Result tailForwardReadFn(
    void* pipeline, ContextImpl& /*ctx*/, TypeErasedBox&& msg) noexcept {
  return static_cast<PipelineImpl*>(pipeline)->forwardReadPastTail(
      std::move(msg));
}

// Terminal for a write propagating past the head endpoint (the head endpoint's
// own ctx.fireWrite). Routes to a bound peer, else reports no downstream.
Result headForwardWriteFn(
    void* pipeline, ContextImpl& /*ctx*/, TypeErasedBox&& msg) noexcept {
  return static_cast<PipelineImpl*>(pipeline)->forwardWritePastHead(
      std::move(msg));
}

std::size_t typeEventStartIndex(EventKey key, std::size_t tableSize) noexcept {
  auto value = reinterpret_cast<std::uintptr_t>(key) >> 3;
  value ^= value >> 16;
  return value & (tableSize - 1);
}

} // namespace

namespace apache::thrift::fast_thrift::channel_pipeline {

PipelineImpl::~PipelineImpl() {
  if (pipelineStateDeleter_) {
    pipelineStateDeleter_(pipelineState_);
  }
  // Runs after all handlers and contexts have been destroyed (derived storage
  // is torn down before this base destructor), so an owned allocator outlives
  // every handler that may hold buffers allocated from it. Allocator is
  // destroyed last.
  if (ownedHeadDeleter_) {
    ownedHeadDeleter_(ownedHead_);
  }
  if (ownedTailDeleter_) {
    ownedTailDeleter_(ownedTail_);
  }
  if (ownedAllocatorDeleter_) {
    ownedAllocatorDeleter_(ownedAllocator_);
  }
}

PipelineImpl::PipelineImpl(
    folly::EventBase* eventBase,
    std::size_t handlerCount,
    void* headHandler,
    void* tailHandler,
    void* allocator) noexcept
    : eventBase_(eventBase),
      headCtx_(this, eventBase, allocator, handlerCount, 0),
      tailCtx_(this, eventBase, allocator, handlerCount, 0),
      headHandler_(headHandler),
      tailHandler_(tailHandler),
      allocator_(allocator) {}

void PipelineImpl::initializeStorageViews(
    std::span<detail::HandlerNode> handlers,
    std::span<detail::ContextImpl> contexts,
    std::span<HandlerIndexEntry> handlerMap) noexcept {
  handlers_ = handlers;
  contexts_ = contexts;
  handlerMap_ = handlerMap;

  const auto N = handlers_.size();

  // Wire up cached dispatch pointers.
  // Each context caches direct function pointers to the next/prev handler,
  // eliminating the per-hop round-trip through PipelineImpl on the hot path.
  //
  // Fixed direction: reads flow 0→N→terminal, writes flow N→0→terminal
  for (size_t i = 0; i < N; ++i) {
    auto& ctx = contexts_[i];

    // Read / exception: forward direction (i+1)
    if (i + 1 < N) {
      ctx.nextReadFn_ = handlers_[i + 1].onReadFn;
      ctx.nextExceptionFn_ = handlers_[i + 1].onExceptionFn;
      ctx.nextHandler_ = handlers_[i + 1].handlerPtr;
      ctx.nextCtx_ = &contexts_[i + 1];
    } else {
      ctx.nextReadFn_ = &tailHandlerReadFn;
      ctx.nextExceptionFn_ = &tailHandlerExceptionFn;
      ctx.nextHandler_ = this;
      ctx.nextCtx_ = &ctx; // unused by terminal functions
    }

    // Write: reverse direction (i-1)
    if (i > 0) {
      ctx.prevWriteFn_ = handlers_[i - 1].onWriteFn;
      ctx.prevHandler_ = handlers_[i - 1].handlerPtr;
      ctx.prevCtx_ = &contexts_[i - 1];
    } else {
      ctx.prevWriteFn_ = &headHandlerWriteFn;
      ctx.prevHandler_ = this;
      ctx.prevCtx_ = &ctx; // unused by terminal function
    }
  }

  // Cache entry-point dispatch pointers for fireRead/fireWrite. Eliminates
  // vector indexing on the hot path entry.
  if (!handlers_.empty()) {
    firstReadFn_ = handlers_[0].onReadFn;
    firstExceptionFn_ = handlers_[0].onExceptionFn;
    firstHandler_ = handlers_[0].handlerPtr;
    firstCtx_ = &contexts_[0];

    lastWriteFn_ = handlers_[N - 1].onWriteFn;
    lastHandler_ = handlers_[N - 1].handlerPtr;
    lastCtx_ = &contexts_[N - 1];
  }

  headCtx_.nextReadFn_ = firstReadFn_ ? firstReadFn_ : &tailHandlerReadFn;
  headCtx_.nextExceptionFn_ =
      firstExceptionFn_ ? firstExceptionFn_ : &tailHandlerExceptionFn;
  headCtx_.nextHandler_ = firstHandler_ ? firstHandler_ : this;
  headCtx_.nextCtx_ = firstCtx_ ? firstCtx_ : &tailCtx_;

  tailCtx_.prevWriteFn_ = lastWriteFn_ ? lastWriteFn_ : &headHandlerWriteFn;
  tailCtx_.prevHandler_ = lastHandler_ ? lastHandler_ : this;
  tailCtx_.prevCtx_ = lastCtx_ ? lastCtx_ : &headCtx_;

  // Propagation past the endpoints: the tail endpoint's ctx.fireRead and the
  // head endpoint's ctx.fireWrite route to a bound peer (or report no
  // downstream). This is the only behavior binding changes; the endpoints
  // themselves run unchanged.
  tailCtx_.nextReadFn_ = &tailForwardReadFn;
  tailCtx_.nextHandler_ = this;
  tailCtx_.nextCtx_ = &tailCtx_; // unused by terminal function
  headCtx_.prevWriteFn_ = &headForwardWriteFn;
  headCtx_.prevHandler_ = this;
  headCtx_.prevCtx_ = &headCtx_; // unused by terminal function
}

void PipelineImpl::linkTypeEventLists() noexcept {
  std::size_t declarationCount = tailPublishedEventCount_ +
      tailTypeSubscriptionCount_ + headPublishedEventCount_ +
      headTypeSubscriptionCount_;
  for (const auto& node : handlers_) {
    declarationCount += node.publishedEventCount + node.typeSubscriptionCount;
  }

  std::vector<EventKey> keys;
  keys.reserve(declarationCount);
  auto registerEvents = [&keys](const EventKey* events, std::size_t count) {
    for (std::size_t i = 0; i < count; ++i) {
      keys.push_back(events[i]);
    }
  };
  auto registerSubscriptions = [&registerEvents](
                                   const TypeEventSubscription* subscriptions,
                                   std::size_t count) {
    for (std::size_t i = 0; i < count; ++i) {
      registerEvents(&subscriptions[i].key, 1);
    }
  };

  registerEvents(tailPublishedEvents_, tailPublishedEventCount_);
  registerSubscriptions(tailTypeSubscriptions_, tailTypeSubscriptionCount_);
  for (const auto& node : handlers_) {
    registerEvents(node.publishedEvents, node.publishedEventCount);
    registerSubscriptions(node.typeSubscriptions, node.typeSubscriptionCount);
  }
  registerEvents(headPublishedEvents_, headPublishedEventCount_);
  registerSubscriptions(headTypeSubscriptions_, headTypeSubscriptionCount_);

  std::sort(keys.begin(), keys.end(), std::less<EventKey>{});
  keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
  if (keys.empty()) {
    return;
  }

  typeEventTableSize_ = 2;
  while (typeEventTableSize_ < keys.size() * 2) {
    typeEventTableSize_ *= 2;
  }
  typeEventTable_ = std::make_unique<TypeEventSlot[]>(typeEventTableSize_);
  for (const auto key : keys) {
    auto index = typeEventStartIndex(key, typeEventTableSize_);
    while (typeEventTable_[index].key != nullptr) {
      index = (index + 1) & (typeEventTableSize_ - 1);
    }
    typeEventTable_[index].key = key;
  }

  typeEventDispatchCount_ =
      tailTypeSubscriptionCount_ + headTypeSubscriptionCount_;
  for (const auto& node : handlers_) {
    typeEventDispatchCount_ += node.typeSubscriptionCount;
  }
  if (typeEventDispatchCount_ != 0) {
    typeEventDispatches_ =
        std::make_unique<TypeEventDispatch[]>(typeEventDispatchCount_);
  }

  auto countSubscribers =
      [this](const TypeEventSubscription* subscriptions, std::size_t count) {
        for (std::size_t i = 0; i < count; ++i) {
          auto* slot = findTypeEventSlot(subscriptions[i].key);
          DCHECK(slot);
          ++slot->subscriberCount;
        }
      };

  countSubscribers(tailTypeSubscriptions_, tailTypeSubscriptionCount_);
  for (const auto& node : handlers_) {
    countSubscribers(node.typeSubscriptions, node.typeSubscriptionCount);
  }
  countSubscribers(headTypeSubscriptions_, headTypeSubscriptionCount_);

  std::size_t subscriberOffset = 0;
  for (std::size_t i = 0; i < typeEventTableSize_; ++i) {
    auto& slot = typeEventTable_[i];
    if (slot.key == nullptr) {
      continue;
    }
    slot.subscribers = slot.subscriberCount == 0
        ? nullptr
        : typeEventDispatches_.get() + subscriberOffset;
    subscriberOffset += slot.subscriberCount;
  }
  DCHECK_EQ(subscriberOffset, typeEventDispatchCount_);

  // Advance each span pointer as its entries are filled, then restore every
  // pointer to the start of its immutable span below.
  auto link = [this](
                  void* target,
                  detail::ContextImpl* ctx,
                  const TypeEventSubscription* subscriptions,
                  std::size_t count) {
    for (std::size_t i = 0; i < count; ++i) {
      const auto& subscription = subscriptions[i];
      auto* slot = findTypeEventSlot(subscription.key);
      DCHECK(slot);
      auto& dispatch = *slot->subscribers++;
      dispatch.fn = subscription.thunk;
      dispatch.target = target;
      dispatch.ctx = ctx;
    }
  };

  link(
      tailHandler_,
      nullptr,
      tailTypeSubscriptions_,
      tailTypeSubscriptionCount_);
  for (std::size_t i = handlers_.size(); i > 0; --i) {
    const auto index = i - 1;
    const auto& node = handlers_[index];
    link(
        node.handlerPtr,
        &contexts_[index],
        node.typeSubscriptions,
        node.typeSubscriptionCount);
  }
  link(
      headHandler_,
      nullptr,
      headTypeSubscriptions_,
      headTypeSubscriptionCount_);
  subscriberOffset = 0;
  for (std::size_t i = 0; i < typeEventTableSize_; ++i) {
    auto& slot = typeEventTable_[i];
    if (slot.key == nullptr) {
      continue;
    }
    if (slot.subscriberCount != 0) {
      DCHECK_EQ(
          slot.subscribers,
          typeEventDispatches_.get() + subscriberOffset + slot.subscriberCount);
      slot.subscribers = typeEventDispatches_.get() + subscriberOffset;
    }
    subscriberOffset += slot.subscriberCount;
  }

  typeEventRouteCount_ = tailPublishedEventCount_ + headPublishedEventCount_;
  for (const auto& node : handlers_) {
    typeEventRouteCount_ += node.publishedEventCount;
  }
  if (typeEventRouteCount_ == 0) {
    return;
  }

  typeEventRoutes_ = std::make_unique<TypeEventRoute[]>(typeEventRouteCount_);
  std::size_t routeIndex = 0;
  auto linkPublisher =
      [this, &routeIndex](
          detail::ContextImpl& ctx, const EventKey* events, std::size_t count) {
        if (count == 0) {
          return;
        }
        DCHECK_LE(routeIndex, std::numeric_limits<std::uint32_t>::max());
        ctx.typeEventRouteOffset_ = static_cast<std::uint32_t>(routeIndex);
        for (std::size_t i = 0; i < count; ++i) {
          auto* slot = findTypeEventSlot(events[i]);
          DCHECK(slot);
          typeEventRoutes_[routeIndex++] = TypeEventRoute{
              events[i], slot->subscribers, slot->subscriberCount};
        }
      };

  linkPublisher(tailCtx_, tailPublishedEvents_, tailPublishedEventCount_);
  for (std::size_t i = 0; i < handlers_.size(); ++i) {
    linkPublisher(
        contexts_[i],
        handlers_[i].publishedEvents,
        handlers_[i].publishedEventCount);
  }
  linkPublisher(headCtx_, headPublishedEvents_, headPublishedEventCount_);
  DCHECK_EQ(routeIndex, typeEventRouteCount_);
}

PipelineImpl::TypeEventSlot* FOLLY_NULLABLE
PipelineImpl::findTypeEventSlot(EventKey key) noexcept {
  if (!typeEventTable_) {
    return nullptr;
  }

  auto index = typeEventStartIndex(key, typeEventTableSize_);
  while (true) {
    auto& slot = typeEventTable_[index];
    if (slot.key == key) {
      return &slot;
    }
    if (slot.key == nullptr) {
      return nullptr;
    }
    index = (index + 1) & (typeEventTableSize_ - 1);
  }
}

void PipelineImpl::callHandlerAdded() noexcept {
  DestructorGuard dg(this);
  headHandlerAddedFn_(headHandler_);
  for (size_t i = 0; i < handlers_.size(); ++i) {
    DCHECK(handlers_[i].handlerAddedFn);
    handlers_[i].handlerAddedFn(handlers_[i].handlerPtr, contexts_[i]);
  }
  tailHandlerAddedFn_(tailHandler_);
}

void PipelineImpl::propagatePipelineState() noexcept {
  headCtx_.pipelineState_ = pipelineState_;
  for (auto& ctx : contexts_) {
    ctx.pipelineState_ = pipelineState_;
  }
  tailCtx_.pipelineState_ = pipelineState_;
}

void PipelineImpl::callHandlerRemovedImpl() noexcept {
  // Call in reverse order (LIFO)
  tailHandlerRemovedFn_(tailHandler_);
  for (size_t i = handlers_.size(); i > 0; --i) {
    size_t idx = i - 1;
    DCHECK(handlers_[idx].handlerRemovedFn);
    handlers_[idx].handlerRemovedFn(handlers_[idx].handlerPtr, contexts_[idx]);
  }
  headHandlerRemovedFn_(headHandler_);
}

void PipelineImpl::callHandlerRemoved() noexcept {
  DestructorGuard dg(this);
  callHandlerRemovedImpl();
}

void PipelineImpl::activate() noexcept {
  DestructorGuard dg(this);
  if (state_ != State::Inactive) {
    return;
  }
  state_ = State::Active;

  headOnPipelineActiveFn_(headHandler_);
  for (size_t i = 0; i < handlers_.size(); ++i) {
    auto& handler = handlers_[i];
    DCHECK(handler.onPipelineActiveFn);
    handler.onPipelineActiveFn(handler.handlerPtr, contexts_[i]);
  }
  tailOnPipelineActiveFn_(tailHandler_);
}

void PipelineImpl::deactivate() noexcept {
  DestructorGuard dg(this);
  if (state_ != State::Active) {
    return;
  }
  state_ = State::Inactive;

  // Call in reverse order (LIFO)
  tailOnPipelineInactiveFn_(tailHandler_);
  if (!handlers_.empty()) {
    deactivateFromIndex(handlers_.size() - 1);
  }
  headOnPipelineInactiveFn_(headHandler_);
}

void PipelineImpl::deactivateFromIndex(size_t index) noexcept {
  for (size_t i = index + 1; i > 0; --i) {
    size_t idx = i - 1;
    auto& handler = handlers_[idx];
    DCHECK(handler.onPipelineInactiveFn);
    handler.onPipelineInactiveFn(handler.handlerPtr, contexts_[idx]);
  }
}

PIPELINE_HOT_PATH Result PipelineImpl::fireRead(TypeErasedBox&& msg) noexcept {
  RETURN_IF_CLOSED(Result::Error);
  if (!firstReadFn_) [[unlikely]] {
    return fireReadToTailHandler(std::move(msg));
  }
  return firstReadFn_(firstHandler_, *firstCtx_, std::move(msg));
}

PIPELINE_HOT_PATH Result PipelineImpl::fireWrite(TypeErasedBox&& msg) noexcept {
  RETURN_IF_CLOSED(Result::Error);
  if (!lastWriteFn_) [[unlikely]] {
    return fireWriteToHeadHandler(std::move(msg));
  }
  return lastWriteFn_(lastHandler_, *lastCtx_, std::move(msg));
}

PIPELINE_HOT_PATH void PipelineImpl::fireException(
    folly::exception_wrapper&& e) noexcept {
  RETURN_IF_CLOSED();
  if (!firstExceptionFn_) [[unlikely]] {
    fireExceptionToTailHandler(std::move(e));
    return;
  }
  firstExceptionFn_(firstHandler_, *firstCtx_, std::move(e));
}

void PipelineImpl::fireTypeEvent(EventKey key, const void* payload) noexcept {
  RETURN_IF_CLOSED();
  if (!typeEventTable_) [[unlikely]] {
    return;
  }

  auto index = typeEventStartIndex(key, typeEventTableSize_);
  while (true) {
    auto& slot = typeEventTable_[index];
    if (slot.key == key) [[likely]] {
      if (slot.subscriberCount == 0) [[unlikely]] {
        return;
      }
      DestructorGuard dg(this);
      for (std::size_t i = 0; i < slot.subscriberCount; ++i) {
        const auto& dispatch = slot.subscribers[i];
        dispatch.fn(dispatch.target, dispatch.ctx, payload);
      }
      return;
    }
    if (slot.key == nullptr) [[unlikely]] {
      return;
    }
    index = (index + 1) & (typeEventTableSize_ - 1);
  }
}

const void* FOLLY_NULLABLE PipelineImpl::bindTypeEvent(EventKey key) noexcept {
  return findTypeEventSlot(key);
}
BoundEventRoute PipelineImpl::bindEvent(EventKey key) noexcept {
  return BoundEventRoute{bindTypeEvent(key)};
}

void PipelineImpl::fireBoundEvent(
    BoundEventRoute route, const void* payload) noexcept {
  fireBoundTypeEvent(route.value, payload);
}

void PipelineImpl::fireBoundTypeEvent(
    const void* route, const void* payload) noexcept {
  const auto* slot = static_cast<const TypeEventSlot*>(route);
  RETURN_IF_CLOSED();
  if (slot == nullptr || slot->subscriberCount == 0) [[unlikely]] {
    return;
  }

  DestructorGuard dg(this);
  for (std::size_t i = 0; i < slot->subscriberCount; ++i) {
    const auto& dispatch = slot->subscribers[i];
    dispatch.fn(dispatch.target, dispatch.ctx, payload);
  }
}

void PipelineImpl::fireTypeEventFromRoute(
    std::uint32_t routeOffset,
    std::size_t routeIndex,
    EventKey key,
    const void* payload) noexcept {
  RETURN_IF_CLOSED();
  const auto absoluteIndex = static_cast<std::size_t>(routeOffset) + routeIndex;
  DCHECK_LT(absoluteIndex, typeEventRouteCount_);
  DCHECK_EQ(typeEventRoutes_[absoluteIndex].key, key);

  const auto& route = typeEventRoutes_[absoluteIndex];
  if (route.subscriberCount == 0) [[unlikely]] {
    return;
  }
  DestructorGuard dg(this);
  for (std::size_t i = 0; i < route.subscriberCount; ++i) {
    const auto& dispatch = route.subscribers[i];
    dispatch.fn(dispatch.target, dispatch.ctx, payload);
  }
}

Result PipelineImpl::sendRead(
    HandlerId handlerId, TypeErasedBox&& msg) noexcept {
  DestructorGuard dg(this);
  size_t index = lookupHandler(handlerId);
  if (index >= handlers_.size()) {
    return Result::Error;
  }
  return fireReadFromIndex(index, std::move(msg));
}

Result PipelineImpl::sendWrite(
    HandlerId handlerId, TypeErasedBox&& msg) noexcept {
  DestructorGuard dg(this);
  size_t index = lookupHandler(handlerId);
  if (index >= handlers_.size()) {
    return Result::Error;
  }
  return fireWriteFromIndex(index, std::move(msg));
}

void PipelineImpl::sendException(
    HandlerId handlerId, folly::exception_wrapper&& e) noexcept {
  DestructorGuard dg(this);
  size_t index = lookupHandler(handlerId);
  DCHECK(index < handlers_.size());
  if (index < handlers_.size()) {
    fireExceptionFromIndex(index, std::move(e));
  }
}

detail::ContextImpl* FOLLY_NULLABLE
PipelineImpl::context(HandlerId handlerId) noexcept {
  size_t index = lookupHandler(handlerId);
  if (index >= handlers_.size()) {
    return nullptr;
  }
  return &contexts_[index];
}

void PipelineImpl::close() noexcept {
  DestructorGuard dg(this);
  if (state_ == State::Closed) {
    return;
  }
  // Honor the deactivate-before-close contract even when close() is invoked
  // directly on an active pipeline (e.g. a setup-time rejection). deactivate()
  // is a no-op once the pipeline is already Inactive, so the normal
  // transport-close path (which deactivates first) is unaffected.
  deactivate();
  state_ = State::Closed;

  // Clear ready waiters - handlers should not receive callbacks after close
  writeReadyWaiters_.clear();
  readReadyWaiters_.clear();

  callHandlerRemoved();
}

void PipelineImpl::checkBindPeer(PipelineRef peer) const noexcept {
  CHECK(peer) << "bind: peer pipeline is empty";
  // Same-EventBase is a simplification: a crossing is a direct, same-thread
  // call. Executor hops across the seam will be supported later; until then we
  // enforce a shared EventBase.
  CHECK_EQ(peer.eventBase(), eventBase_)
      << "bind requires the peer to share this pipeline's EventBase";
}

Result PipelineImpl::forwardReadPastTail(TypeErasedBox&& msg) noexcept {
  if (boundTailPeer_) {
    return boundTailPeer_.fireRead(tailConvert_(std::move(msg)));
  }
  return Result::Error;
}

Result PipelineImpl::forwardWritePastHead(TypeErasedBox&& msg) noexcept {
  if (boundHeadPeer_) {
    return boundHeadPeer_.fireWrite(headConvert_(std::move(msg)));
  }
  return Result::Error;
}

PIPELINE_HOT_PATH Result
PipelineImpl::fireReadFromIndex(size_t index, TypeErasedBox&& msg) noexcept {
  if (index >= handlers_.size()) {
    return fireReadToTailHandler(std::move(msg));
  }
  auto& handler = handlers_[index];
  DCHECK(handler.onReadFn);
  return handler.onReadFn(handler.handlerPtr, contexts_[index], std::move(msg));
}

PIPELINE_HOT_PATH Result
PipelineImpl::fireWriteFromIndex(size_t index, TypeErasedBox&& msg) noexcept {
  auto& handler = handlers_[index];
  DCHECK(handler.onWriteFn);
  return handler.onWriteFn(
      handler.handlerPtr, contexts_[index], std::move(msg));
}

PIPELINE_HOT_PATH void PipelineImpl::fireExceptionFromIndex(
    size_t index, folly::exception_wrapper&& e) noexcept {
  if (index >= handlers_.size()) {
    fireExceptionToTailHandler(std::move(e));
    return;
  }
  auto& handler = handlers_[index];
  DCHECK(handler.onExceptionFn);
  handler.onExceptionFn(handler.handlerPtr, contexts_[index], std::move(e));
}

Result PipelineImpl::fireReadToTailHandler(TypeErasedBox&& msg) noexcept {
  if (!tailOnReadFn_ || !tailHandler_) {
    return Result::Error;
  }
  return tailOnReadFn_(tailHandler_, tailCtx_, std::move(msg));
}

Result PipelineImpl::fireWriteToHeadHandler(TypeErasedBox&& msg) noexcept {
  if (!headOnWriteFn_ || !headHandler_) {
    return Result::Error;
  }
  return headOnWriteFn_(headHandler_, headCtx_, std::move(msg));
}

void PipelineImpl::fireExceptionToTailHandler(
    folly::exception_wrapper&& e) noexcept {
  if (!tailOnExceptionFn_ || !tailHandler_) {
    return;
  }
  tailOnExceptionFn_(tailHandler_, std::move(e));
}

BytesPtr PipelineImpl::allocate(size_t size) noexcept {
  if (!allocateFn_ || !allocator_) {
    // Fallback to simple allocation
    return folly::IOBuf::create(size);
  }
  return allocateFn_(allocator_, size);
}

BytesPtr PipelineImpl::copyBuffer(const void* data, size_t size) noexcept {
  if (!copyBufferFn_ || !allocator_) {
    // Fallback to simple allocation
    return folly::IOBuf::copyBuffer(data, size);
  }
  return copyBufferFn_(allocator_, data, size);
}

BytesPtr PipelineImpl::takeOwnership(OwnedBuffer buffer) noexcept {
  if (!takeOwnershipFn_ || !allocator_) {
    return takeBufferOwnership(buffer);
  }
  return takeOwnershipFn_(allocator_, buffer);
}

size_t PipelineImpl::lookupHandler(HandlerId handlerId) const noexcept {
  const auto it = std::upper_bound(
      handlerMap_.begin(),
      handlerMap_.end(),
      handlerId,
      [](HandlerId id, const HandlerIndexEntry& entry) {
        return id < entry.first;
      });
  if (it == handlerMap_.begin() || std::prev(it)->first != handlerId) {
    return handlers_.size();
  }
  return std::prev(it)->second;
}

PIPELINE_HOT_PATH void PipelineImpl::onWriteReady() noexcept {
  DestructorGuard dg(this);
  if (state_ == State::Closed) {
    return;
  }
  if (writeReadyDispatching_) {
    writeReadyDispatchPending_ = true;
    return;
  }
  writeReadyDispatching_ = true;

  const size_t headIndex = handlers_.size();
  bool propagationBlocked = false;
  do {
    writeReadyDispatchPending_ = false;

    // Write capacity originates at the head endpoint. Let it consume the edge
    // before upstream handlers retry so they observe the updated transport
    // state.
    if (writeReadyWaiters_.isArmed(headIndex)) {
      DCHECK(headOnWriteReadyFn_);
      headOnWriteReadyFn_(headHandler_, headCtx_);
      if (state_ == State::Closed || writeReadyWaiters_.isArmed(headIndex)) {
        propagationBlocked = true;
        break;
      }
    }

    // A callback may arm or cancel any handler, so the walk looks up the next
    // waiting handler after each one. It never finds the head endpoint, because
    // the pass stops as soon as the head endpoint waits.
    size_t begin = 0;
    while (true) {
      const std::optional<size_t> next =
          writeReadyWaiters_.lowestArmedFrom(begin);
      if (!next) {
        break;
      }
      const size_t i = *next;
      begin = i + 1;
      DCHECK_LT(i, contexts_.size());
      DCHECK(handlers_[i].onWriteReadyFn);
      handlers_[i].onWriteReadyFn(handlers_[i].handlerPtr, contexts_[i]);
      if (state_ == State::Closed || writeReadyWaiters_.isArmed(headIndex)) {
        propagationBlocked = true;
        break;
      }
    }

    if (propagationBlocked) {
      break;
    }

    // Notify the tail endpoint that writes can resume.
    DCHECK(tailOnWriteReadyFn_);
    tailOnWriteReadyFn_(tailHandler_);
  } while (writeReadyDispatchPending_ && state_ != State::Closed);

  writeReadyDispatching_ = false;
}

void PipelineImpl::onReadReady() noexcept {
  DestructorGuard dg(this);
  if (state_ == State::Closed) {
    return;
  }
  if (readReadyDispatching_) {
    readReadyDispatchPending_ = true;
    return;
  }
  readReadyDispatching_ = true;

  do {
    readReadyDispatchPending_ = false;

    // A callback may arm or cancel any handler, so the walk looks up the next
    // waiting handler after each one.
    size_t end = handlers_.size();
    while (true) {
      const std::optional<size_t> next =
          readReadyWaiters_.highestArmedBelow(end);
      if (!next) {
        break;
      }
      const size_t i = *next;
      end = i;
      DCHECK(handlers_[i].onReadReadyFn);
      DCHECK_LT(i, contexts_.size());
      handlers_[i].onReadReadyFn(handlers_[i].handlerPtr, contexts_[i]);
      if (state_ == State::Closed) {
        break;
      }
    }

    if (state_ != State::Closed) {
      DCHECK(headOnReadReadyFn_);
      headOnReadReadyFn_(headHandler_);
    }
  } while (readReadyDispatchPending_ && state_ != State::Closed);

  readReadyDispatching_ = false;
}

} // namespace apache::thrift::fast_thrift::channel_pipeline
