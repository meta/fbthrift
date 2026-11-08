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

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/EndpointAdapter.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Handler.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/HandlerTag.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineImpl.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineRef.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/ErasedStaticHandler.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/ErasedStaticSegment.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/StaticHandler.h>

#include <glog/logging.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace apache::thrift::fast_thrift::channel_pipeline::detail {

enum class StaticHandlerDirection { Inbound, Outbound, Duplex };

template <StaticHandlerDirection Direction, typename H, typename Context>
concept StaticHandlerForDirection =
    (Direction == StaticHandlerDirection::Inbound &&
     InboundHandler<H, Context>) ||
    (Direction == StaticHandlerDirection::Outbound &&
     OutboundHandler<H, Context>) ||
    (Direction == StaticHandlerDirection::Duplex && DuplexHandler<H, Context>);

struct NoErasedStaticHandlers {
  static constexpr bool enabled = false;
  static constexpr bool segmented = false;
  static constexpr std::size_t index = 0;
};

template <std::size_t Index>
struct ErasedStaticHandlersAt {
  static constexpr bool enabled = true;
  static constexpr bool segmented = false;
  static constexpr std::size_t index = Index;
};

template <std::size_t Index>
struct ErasedStaticSegmentsAt {
  static constexpr bool enabled = true;
  static constexpr bool segmented = true;
  static constexpr std::size_t index = Index;
};

template <bool Enabled>
class ErasedStaticHandlerStorage;

template <>
class ErasedStaticHandlerStorage<true> {
 public:
  explicit ErasedStaticHandlerStorage(
      std::vector<ErasedStaticHandler>&& handlers) noexcept
      : handlers_(std::move(handlers)) {}
  std::size_t size() const noexcept { return handlers_.size(); }
  ErasedStaticHandler* begin() noexcept { return handlers_.data(); }
  ErasedStaticHandler* end() noexcept {
    return handlers_.data() + handlers_.size();
  }
  ErasedStaticHandler& operator[](std::size_t index) noexcept {
    return handlers_[index];
  }
  const ErasedStaticHandler& operator[](std::size_t index) const noexcept {
    return handlers_[index];
  }

 private:
  std::vector<ErasedStaticHandler> handlers_;
};

template <>
class ErasedStaticHandlerStorage<false> {
 public:
  explicit ErasedStaticHandlerStorage(
      std::vector<ErasedStaticHandler>&&) noexcept {}
  constexpr std::size_t size() const noexcept { return 0; }
  ErasedStaticHandler* begin() noexcept { return nullptr; }
  ErasedStaticHandler* end() noexcept { return nullptr; }
  ErasedStaticHandler& operator[](std::size_t) noexcept { std::terminate(); }
  const ErasedStaticHandler& operator[](std::size_t) const noexcept {
    std::terminate();
  }
};

template <bool Enabled>
class ErasedStaticSegmentStorage;

template <>
class ErasedStaticSegmentStorage<true> {
 public:
  explicit ErasedStaticSegmentStorage(
      std::vector<ErasedStaticSegment>&& segments) noexcept
      : segments_(std::move(segments)) {
    for (const auto& segment : segments_) {
      size_ += segment.handlerCount();
    }
  }
  std::size_t size() const noexcept { return size_; }
  std::vector<ErasedStaticSegment>& segments() noexcept { return segments_; }

 private:
  std::vector<ErasedStaticSegment> segments_;
  std::size_t size_{0};
};

template <>
class ErasedStaticSegmentStorage<false> {
 public:
  explicit ErasedStaticSegmentStorage(
      std::vector<ErasedStaticSegment>&&) noexcept {}
  constexpr std::size_t size() const noexcept { return 0; }
};

template <typename H, HandlerId Id, StaticHandlerDirection Direction>
struct StaticHandlerSpec {
  template <typename>
  using Handler = H;
  static constexpr HandlerId id = Id;
  static constexpr StaticHandlerDirection direction = Direction;
};

template <
    template <typename> typename H,
    HandlerId Id,
    StaticHandlerDirection Direction>
struct StaticHandlerTemplateSpec {
  template <typename Context>
  using Handler = H<Context>;
  static constexpr HandlerId id = Id;
  static constexpr StaticHandlerDirection direction = Direction;
};

template <
    typename H,
    HandlerId Id,
    StaticHandlerDirection Direction,
    typename... Args>
class StaticHandlerFactory {
 public:
  using Spec = StaticHandlerSpec<H, Id, Direction>;

  template <typename... Input>
    requires(sizeof...(Input) == sizeof...(Args))
  explicit StaticHandlerFactory(Input&&... args)
      : args_(std::forward<Input>(args)...) {}

  ~StaticHandlerFactory() = default;
  StaticHandlerFactory(StaticHandlerFactory&&) noexcept = default;
  StaticHandlerFactory& operator=(StaticHandlerFactory&&) noexcept = default;
  StaticHandlerFactory(const StaticHandlerFactory&) = delete;
  StaticHandlerFactory& operator=(const StaticHandlerFactory&) = delete;

  operator H() && {
    return std::apply(
        []<typename... StoredArgs>(StoredArgs&&... args) -> H {
          return H(std::forward<StoredArgs>(args)...);
        },
        std::move(args_));
  }

 private:
  std::tuple<Args...> args_;
};

template <
    template <typename> typename H,
    HandlerId Id,
    StaticHandlerDirection Direction,
    typename... Args>
class StaticHandlerTemplateFactory {
 public:
  using Spec = StaticHandlerTemplateSpec<H, Id, Direction>;

  template <typename... Input>
    requires(sizeof...(Input) == sizeof...(Args))
  explicit StaticHandlerTemplateFactory(Input&&... args)
      : args_(std::forward<Input>(args)...) {}

  ~StaticHandlerTemplateFactory() = default;
  StaticHandlerTemplateFactory(StaticHandlerTemplateFactory&&) noexcept =
      default;
  StaticHandlerTemplateFactory& operator=(
      StaticHandlerTemplateFactory&&) noexcept = default;
  StaticHandlerTemplateFactory(const StaticHandlerTemplateFactory&) = delete;
  StaticHandlerTemplateFactory& operator=(const StaticHandlerTemplateFactory&) =
      delete;

  template <typename Context>
  operator H<Context>() && {
    return std::apply(
        []<typename... StoredArgs>(StoredArgs&&... args) -> H<Context> {
          return H<Context>(std::forward<StoredArgs>(args)...);
        },
        std::move(args_));
  }

 private:
  std::tuple<Args...> args_;
};

template <
    typename Pipeline,
    std::size_t Index,
    typename StateTuple,
    typename... Specs>
class StaticHandlerStorage;

template <typename Pipeline, std::size_t Index, typename StateTuple>
class StaticHandlerStorage<Pipeline, Index, StateTuple> {
 public:
  template <typename FactoryTuple>
  StaticHandlerStorage(Pipeline*, FactoryTuple&) noexcept {}
};

template <typename H, typename Context>
concept StaticHandlerLifecycle = HandlerLifecycle<H, Context>;

template <
    typename Pipeline,
    std::size_t Index,
    typename StateTuple,
    typename Spec>
using StaticHandlerSlot = StaticHandler<
    typename Spec::template Handler<
        StaticContext<Pipeline, Index, Spec::id, StateTuple>>,
    Spec::id,
    Pipeline,
    Index,
    StateTuple>;

template <
    typename Pipeline,
    std::size_t Index,
    typename StateTuple,
    typename Spec,
    typename... Rest>
class StaticHandlerStorage<Pipeline, Index, StateTuple, Spec, Rest...>
    : private StaticHandlerSlot<Pipeline, Index, StateTuple, Spec>,
      private StaticHandlerStorage<Pipeline, Index + 1, StateTuple, Rest...> {
  using Slot = StaticHandlerSlot<Pipeline, Index, StateTuple, Spec>;
  using H = typename Slot::Handler;
  using Context = typename Slot::Context;
  using Tail = StaticHandlerStorage<Pipeline, Index + 1, StateTuple, Rest...>;

  static_assert(StaticHandlerLifecycle<H, Context>);
  static_assert(
      StaticHandlerForDirection<Spec::direction, H, Context>,
      "static pipeline handler does not satisfy its declared direction");

 public:
  template <typename FactoryTuple>
  StaticHandlerStorage(Pipeline* pipeline, FactoryTuple& factories)
      : Slot(pipeline, std::move(std::get<Index>(factories))),
        Tail(pipeline, factories) {}

  template <std::size_t Target>
  decltype(auto) get() noexcept {
    if constexpr (Target == Index) {
      return static_cast<Slot&>(*this);
    } else {
      return Tail::template get<Target>();
    }
  }

  template <std::size_t Target>
  decltype(auto) get() const noexcept {
    if constexpr (Target == Index) {
      return static_cast<const Slot&>(*this);
    } else {
      return Tail::template get<Target>();
    }
  }
};

template <typename EventSet, typename E>
inline constexpr bool kStaticEventSetContains = false;

template <PipelineEvent E, PipelineEvent... Evs>
inline constexpr bool kStaticEventSetContains<Events<Evs...>, E> =
    (std::same_as<E, Evs> || ...);

template <typename Pipeline, typename EventSet>
class StaticEventPublisherHandle;

template <typename Pipeline, PipelineEvent... Evs>
class StaticEventPublisherHandle<Pipeline, Events<Evs...>> {
 public:
  explicit StaticEventPublisherHandle(Pipeline& pipeline) noexcept
      : pipeline_(&pipeline) {}

  template <PipelineEvent E>
    requires(
        (std::same_as<E, Evs> || ...) && std::is_void_v<typename E::Payload>)
  void fire() const noexcept {
    pipeline_->template fireEvent<E>();
  }

  template <PipelineEvent E>
    requires(
        (std::same_as<E, Evs> || ...) && (!std::is_void_v<typename E::Payload>))
  void fire(const typename E::Payload& payload) const noexcept {
    pipeline_->template fireEvent<E>(payload);
  }

 private:
  Pipeline* pipeline_;
};

/**
 * RAII storage for a pipeline-owned endpoint/allocator, co-located in the
 * StaticPipelineImpl allocation. Constructed in place from an EndpointSource at
 * build() and destroyed in place when the slot member is destroyed. Slots are
 * declared before the handler storage so they are destroyed after all handlers;
 * the allocator slot is declared first so it is destroyed last, preserving the
 * allocator-outlives-everything invariant.
 */
template <typename T>
class OwnedEndpointSlot {
 public:
  OwnedEndpointSlot() noexcept = default;
  OwnedEndpointSlot(const OwnedEndpointSlot&) = delete;
  OwnedEndpointSlot& operator=(const OwnedEndpointSlot&) = delete;
  OwnedEndpointSlot(OwnedEndpointSlot&&) = delete;
  OwnedEndpointSlot& operator=(OwnedEndpointSlot&&) = delete;
  ~OwnedEndpointSlot() {
    if (destroy_ != nullptr) {
      destroy_(&storage_);
    }
  }

  // Construct the owned object in place from an owning source (src.ops != null)
  // and return its pointer. The destructor is taken from the type-erased
  // EndpointConstructionOps rather than calling std::destroy_at<T> here, so
  // this slot never odr-uses ~T for an endpoint that is only ever set
  // externally — some external endpoints (e.g. SRAppAdapter) have a private
  // destructor.
  T* construct(const EndpointSource& src) {
    T* obj =
        static_cast<T*>(src.ops->constructInline(&storage_, src.args.get()));
    destroy_ = src.ops->destroyInline;
    return obj;
  }

 private:
  alignas(T) std::byte storage_[sizeof(T)]{};
  void (*destroy_)(void*) noexcept {nullptr};
};

template <
    typename HeadHandler,
    typename TailHandler,
    typename Allocator,
    typename StateTuple,
    typename StaticHandlerConfig,
    typename... Specs>
class StaticPipelineImpl final {
 public:
  using Self = StaticPipelineImpl;
  static constexpr std::size_t kHandlerCount = sizeof...(Specs);
  static constexpr bool kHasStaticHandlers = StaticHandlerConfig::enabled;
  static constexpr bool kHasStaticSegments =
      kHasStaticHandlers && StaticHandlerConfig::segmented;
  static constexpr bool kHasErasedHandlers =
      kHasStaticHandlers && !kHasStaticSegments;
  static constexpr std::size_t kStaticHandlerIndex = StaticHandlerConfig::index;
  static_assert(!kHasStaticHandlers || kStaticHandlerIndex <= kHandlerCount);
  using EndpointContext =
      StaticContext<Self, kHandlerCount, 0, StateTuple, /*IsEndpoint=*/true>;

  class Guard {
   public:
    explicit Guard(Self* pipeline) noexcept : pipeline_(pipeline) {
      if (pipeline_ != nullptr) {
        ++pipeline_->guardCount_;
      }
    }
    Guard(const Guard&) = delete;
    Guard& operator=(const Guard&) = delete;
    Guard(Guard&& other) noexcept
        : pipeline_(std::exchange(other.pipeline_, nullptr)) {}
    Guard& operator=(Guard&&) = delete;
    ~Guard() {
      if (pipeline_ != nullptr && --pipeline_->guardCount_ == 0 &&
          pipeline_->destroyPending_) {
        pipeline_->destroyNow();
      }
    }

   private:
    Self* pipeline_;
  };

  struct Deleter {
    void operator()(Self* pipeline) const noexcept {
      if (pipeline != nullptr) {
        pipeline->destroy();
      }
    }
  };
  using Ptr = std::unique_ptr<Self, Deleter>;

  template <typename FactoryTuple>
  StaticPipelineImpl(
      folly::EventBase* eventBase,
      EndpointSource headSource,
      EndpointSource tailSource,
      EndpointSource allocatorSource,
      StateTuple&& state,
      FactoryTuple& factories,
      std::vector<ErasedStaticHandler>&& staticHandlers,
      std::vector<ErasedStaticSegment>&& staticSegments)
      : eventBase_(eventBase),
        headHandler_(static_cast<HeadHandler*>(headSource.external)),
        tailHandler_(static_cast<TailHandler*>(tailSource.external)),
        allocator_(static_cast<Allocator*>(allocatorSource.external)),
        stateStorage_(std::move(state)),
        handlers_(this, factories),
        staticHandlers_(std::move(staticHandlers)),
        staticSegments_(std::move(staticSegments)),
        endpointContext_(this) {
    static_assert(ValidEndpointPair<HeadHandler, TailHandler, EndpointContext>);
    // Construct any pipeline-owned endpoints in place; external components keep
    // the raw pointer set above.
    if (headSource.ops) {
      headHandler_ = ownedHead_.construct(headSource);
    }
    if (tailSource.ops) {
      tailHandler_ = ownedTail_.construct(tailSource);
    }
    if (allocatorSource.ops) {
      allocator_ = ownedAllocator_.construct(allocatorSource);
    }
    bindStaticHandlerContexts();
    initializeHooks(std::make_index_sequence<kHandlerCount>{});
  }

  StaticPipelineImpl(const StaticPipelineImpl&) = delete;
  StaticPipelineImpl& operator=(const StaticPipelineImpl&) = delete;
  StaticPipelineImpl(StaticPipelineImpl&&) = delete;
  StaticPipelineImpl& operator=(StaticPipelineImpl&&) = delete;

  void finishBuild() noexcept { callHandlerAdded(); }

  void activate() noexcept {
    Guard guard(this);
    if (state_ != State::Inactive) {
      return;
    }
    state_ = State::Active;
    headHandler_->onPipelineActive();
    activateHandlers(std::make_index_sequence<kHandlerCount>{});
    tailHandler_->onPipelineActive();
  }

  void deactivate() noexcept {
    Guard guard(this);
    deactivateImpl();
  }

  Result fireRead(TypeErasedBox&& msg) noexcept {
    if (isClosed()) [[unlikely]] {
      return Result::Error;
    }
    Guard guard(this);
    return fireReadFrom<0>(std::move(msg));
  }

  Result fireWrite(TypeErasedBox&& msg) noexcept {
    if (isClosed()) [[unlikely]] {
      return Result::Error;
    }
    Guard guard(this);
    return fireWriteFrom<kHandlerCount>(std::move(msg));
  }

  void fireException(folly::exception_wrapper&& e) noexcept {
    if (isClosed()) [[unlikely]] {
      return;
    }
    Guard guard(this);
    fireExceptionFrom<0>(std::move(e));
  }

  template <std::size_t Index>
  Result fireReadFrom(TypeErasedBox&& msg) noexcept {
    if constexpr (kHasStaticHandlers && Index == kStaticHandlerIndex) {
      if constexpr (kHasStaticSegments) {
        return staticSegments_.segments().front().onReadFirst(std::move(msg));
      } else {
        return fireReadFromStaticHandler(0, std::move(msg));
      }
    } else {
      return fireReadTypedAt<Index>(std::move(msg));
    }
  }

  template <std::size_t Index>
  Result fireReadTypedAt(TypeErasedBox&& msg) noexcept {
    if constexpr (Index == kHandlerCount) {
      return tailHandler_->onRead(endpointContext_, std::move(msg));
    } else {
      auto& slot = handlerAt<Index>();
      using H = typename std::remove_reference_t<decltype(slot)>::Handler;
      using Context = typename std::remove_reference_t<decltype(slot)>::Context;
      if constexpr (InboundHandler<H, Context>) {
        return slot.handler().onRead(slot.context(), std::move(msg));
      } else {
        return fireReadFrom<Index + 1>(std::move(msg));
      }
    }
  }

  template <std::size_t Count>
  Result fireWriteFrom(TypeErasedBox&& msg) noexcept {
    if constexpr (kHasStaticHandlers && Count == kStaticHandlerIndex) {
      if constexpr (kHasStaticSegments) {
        return staticSegments_.segments().back().onWriteLast(std::move(msg));
      } else {
        return fireWriteFromStaticHandler(staticHandlerCount(), std::move(msg));
      }
    } else {
      return fireWriteTypedAt<Count>(std::move(msg));
    }
  }

  template <std::size_t Count>
  Result fireWriteTypedAt(TypeErasedBox&& msg) noexcept {
    if constexpr (Count == 0) {
      return headHandler_->onWrite(endpointContext_, std::move(msg));
    } else {
      auto& slot = handlerAt<Count - 1>();
      using H = typename std::remove_reference_t<decltype(slot)>::Handler;
      using Context = typename std::remove_reference_t<decltype(slot)>::Context;
      if constexpr (OutboundHandler<H, Context>) {
        return slot.handler().onWrite(slot.context(), std::move(msg));
      } else {
        return fireWriteFrom<Count - 1>(std::move(msg));
      }
    }
  }

  template <std::size_t Index>
  void fireExceptionFrom(folly::exception_wrapper&& e) noexcept {
    if constexpr (kHasStaticHandlers && Index == kStaticHandlerIndex) {
      if constexpr (kHasStaticSegments) {
        staticSegments_.segments().front().onExceptionFirst(std::move(e));
      } else {
        fireExceptionFromStaticHandler(0, std::move(e));
      }
    } else {
      fireExceptionTypedAt<Index>(std::move(e));
    }
  }

  template <std::size_t Index>
  void fireExceptionTypedAt(folly::exception_wrapper&& e) noexcept {
    if constexpr (Index == kHandlerCount) {
      tailHandler_->onException(std::move(e));
    } else {
      auto& slot = handlerAt<Index>();
      using H = typename std::remove_reference_t<decltype(slot)>::Handler;
      using Context = typename std::remove_reference_t<decltype(slot)>::Context;
      if constexpr (InboundHandler<H, Context>) {
        slot.handler().onException(slot.context(), std::move(e));
      } else {
        fireExceptionFrom<Index + 1>(std::move(e));
      }
    }
  }

  template <std::size_t Index>
  std::size_t contextHandlerIndex() const noexcept {
    return logicalHandlerIndex<Index>();
  }

  template <std::size_t Index>
  void deactivateFromContext() noexcept {
    if constexpr (kHasStaticHandlers && Index < kStaticHandlerIndex) {
      deactivateTypedHandlers<Index + 1>();
    } else {
      deactivateFrom<Index + 1>();
    }
  }

  template <std::size_t Count>
  void deactivateFrom() noexcept {
    if constexpr (kHasStaticHandlers && Count == kStaticHandlerIndex) {
      deactivateStaticHandlersFrom(staticHandlerCount());
      deactivateTypedHandlers<Count>();
    } else if constexpr (Count > 0) {
      auto& slot = handlerAt<Count - 1>();
      using H = typename std::remove_reference_t<decltype(slot)>::Handler;
      using Context = typename std::remove_reference_t<decltype(slot)>::Context;
      if constexpr (requires(H& handler, Context& context) {
                      {
                        handler.onPipelineInactive(context)
                      } noexcept -> std::same_as<void>;
                    }) {
        slot.handler().onPipelineInactive(slot.context());
      }
      deactivateFrom<Count - 1>();
    }
  }

  Result sendRead(HandlerId id, TypeErasedBox&& msg) noexcept {
    Guard guard(this);
    if (auto index = findStaticHandler(id)) {
      return staticOnRead(*index, std::move(msg));
    }
    return sendReadAt<0>(id, std::move(msg));
  }
  Result sendWrite(HandlerId id, TypeErasedBox&& msg) noexcept {
    Guard guard(this);
    if (auto index = findStaticHandler(id)) {
      return staticOnWrite(*index, std::move(msg));
    }
    return sendWriteAt<0>(id, std::move(msg));
  }
  void sendException(HandlerId id, folly::exception_wrapper&& e) noexcept {
    Guard guard(this);
    if (auto index = findStaticHandler(id)) {
      staticOnException(*index, std::move(e));
      return;
    }
    sendExceptionAt<0>(id, std::move(e));
  }

  void close() noexcept {
    Guard guard(this);
    if (isClosed()) {
      return;
    }
    const bool wasActive = state_ == State::Active;
    state_ = State::Closing;
    if (wasActive) {
      tailHandler_->onPipelineInactive();
      deactivateFrom<kHandlerCount>();
      headHandler_->onPipelineInactive();
    }
    state_ = State::Closed;
    writeReadyList_.clear();
    readReadyList_.clear();
    callHandlerRemovedImpl();
  }

  bool isClosed() const noexcept {
    return state_ == State::Closing || state_ == State::Closed;
  }
  std::size_t handlerCount() const noexcept {
    return kHandlerCount + staticHandlerCount();
  }
  bool hasPendingWriteReady() const noexcept {
    return !writeReadyList_.empty();
  }
  bool hasPendingReadReady() const noexcept { return !readReadyList_.empty(); }

  folly::EventBase* eventBase() const noexcept { return eventBase_; }
  BytesPtr allocate(std::size_t size) noexcept {
    return allocator_->allocate(size);
  }
  BytesPtr copyBuffer(const void* data, std::size_t size) noexcept {
    return allocator_->copyBuffer(data, size);
  }

  template <typename T>
    requires(StaticTupleTypeCount<StateTuple, T>::value == 1)
  T& state() noexcept {
    return std::get<T>(stateStorage_);
  }
  template <typename T>
    requires(StaticTupleTypeCount<StateTuple, T>::value == 1)
  const T& state() const noexcept {
    return std::get<T>(stateStorage_);
  }

  template <HandlerId Id>
  auto* context(HandlerTag<Id>) noexcept {
    constexpr auto index = handlerIndex<Id, 0>();
    static_assert(index < kHandlerCount, "handler ID is not in this pipeline");
    return &handlerAt<index>().context();
  }

  template <HandlerId Id>
  decltype(auto) handler(HandlerTag<Id>) noexcept {
    constexpr auto index = handlerIndex<Id, 0>();
    static_assert(index < kHandlerCount, "handler ID is not in this pipeline");
    return handlerAt<index>().handler();
  }

  template <typename EventSet>
    requires kIsEventSet<EventSet>
  auto bindEvents() noexcept {
    return StaticEventPublisherHandle<Self, EventSet>{*this};
  }

  BoundEventRoute bindEvent(EventKey key) noexcept {
    return BoundEventRoute{key};
  }

  void fireBoundEvent(BoundEventRoute route, const void* payload) noexcept {
    if (isClosed()) {
      return;
    }
    Guard guard(this);
    const auto key = static_cast<EventKey>(route.value);
    dispatchEndpointEventByKey(*tailHandler_, key, payload);
    dispatchHandlerEventsReverseByKey(
        key, payload, std::make_index_sequence<kHandlerCount>{});
    dispatchEndpointEventByKey(*headHandler_, key, payload);
  }
  template <PipelineEvent E>
    requires std::is_void_v<typename E::Payload>
  void fireEvent() noexcept {
    fireEventImpl<E>(nullptr);
  }
  template <PipelineEvent E>
    requires(!std::is_void_v<typename E::Payload>)
  void fireEvent(const typename E::Payload& payload) noexcept {
    fireEventImpl<E>(&payload);
  }

  template <
      std::size_t PublisherIndex,
      PipelineEvent E,
      std::size_t RouteIndex,
      typename... Args>
  void firePublishedEvent(Args&&... args) noexcept {
    using Slot = std::remove_reference_t<decltype(handlerAt<PublisherIndex>())>;
    using PublishedEvents = typename Slot::Handler::PublishedEvents;
    static_assert(kStaticEventSetContains<PublishedEvents, E>);
    static_assert(RouteIndex == PublishedEvents::template index<E>);
    fireEvent<E>(std::forward<Args>(args)...);
  }

  template <std::size_t Index>
  void awaitWriteReady() noexcept {
    if (isClosed()) {
      return;
    }
    auto* hook = writeReadyHook<Index>();
    if (hook != nullptr && !hook->hook.is_linked()) {
      writeReadyList_.push_back(*hook);
    }
  }
  template <std::size_t Index>
  void cancelAwaitWriteReady() noexcept {
    auto* hook = writeReadyHook<Index>();
    if (hook != nullptr && hook->hook.is_linked()) {
      hook->hook.unlink();
    }
  }
  template <std::size_t Index>
  bool isAwaitingWriteReady() const noexcept {
    auto* hook = const_cast<Self*>(this)->template writeReadyHook<Index>();
    return hook != nullptr && hook->hook.is_linked();
  }
  template <std::size_t Index>
  void awaitReadReady() noexcept {
    if (isClosed()) {
      return;
    }
    auto* hook = readReadyHook<Index>();
    if (hook != nullptr && !hook->hook.is_linked()) {
      readReadyList_.push_back(*hook);
    }
  }
  template <std::size_t Index>
  void cancelAwaitReadReady() noexcept {
    auto* hook = readReadyHook<Index>();
    if (hook != nullptr && hook->hook.is_linked()) {
      hook->hook.unlink();
    }
  }
  template <std::size_t Index>
  bool isAwaitingReadReady() const noexcept {
    auto* hook = const_cast<Self*>(this)->template readReadyHook<Index>();
    return hook != nullptr && hook->hook.is_linked();
  }

  void onWriteReady() noexcept {
    Guard guard(this);
    if (isClosed()) {
      return;
    }
    if (writeReadyDispatching_) {
      writeReadyDispatchPending_ = true;
      return;
    }
    writeReadyDispatching_ = true;
    do {
      bool blocked = false;
      writeReadyDispatchPending_ = false;
      const auto generation = ++writeReadyGeneration_;
      // Write capacity originates at the head endpoint. Let it consume the
      // edge before upstream handlers retry so they observe the updated
      // transport state.
      if (headWriteReadyHook_.hook.is_linked()) {
        headWriteReadyHook_.lastNotifiedGeneration = generation;
        dispatchHeadWriteReady();
        blocked = isClosed() || headWriteReadyHook_.hook.is_linked();
      }
      while (!blocked) {
        WriteReadyHook* next = nullptr;
        for (auto& hook : writeReadyList_) {
          if (hook.lastNotifiedGeneration != generation) {
            next = &hook;
            break;
          }
        }
        if (next == nullptr) {
          break;
        }
        next->lastNotifiedGeneration = generation;
        if (next->handlerIndex == handlerCount()) {
          dispatchHeadWriteReady();
        } else {
          dispatchWriteReadyTarget(next->handlerIndex);
        }
        if (isClosed() || headWriteReadyHook_.hook.is_linked()) {
          blocked = true;
          break;
        }
      }
      if (!blocked) {
        tailHandler_->onWriteReady();
      }
    } while (writeReadyDispatchPending_ && !isClosed());
    writeReadyDispatching_ = false;
  }

  void onReadReady() noexcept {
    Guard guard(this);
    if (isClosed()) {
      return;
    }
    if (readReadyDispatching_) {
      readReadyDispatchPending_ = true;
      return;
    }
    readReadyDispatching_ = true;
    do {
      readReadyDispatchPending_ = false;
      const auto generation = ++readReadyGeneration_;
      while (true) {
        ReadReadyHook* next = nullptr;
        for (auto& hook : readReadyList_) {
          if (hook.lastNotifiedGeneration != generation) {
            next = &hook;
            break;
          }
        }
        if (next == nullptr) {
          break;
        }
        next->lastNotifiedGeneration = generation;
        dispatchReadReadyTarget(next->handlerIndex);
        if (isClosed()) {
          break;
        }
      }
      if (!isClosed()) {
        headHandler_->onReadReady();
      }
    } while (readReadyDispatchPending_ && !isClosed());
    readReadyDispatching_ = false;
  }

  // === Pipeline composition (binding) ===

  /**
   * Chain `peer` onto this pipeline's tail (`adaptTail`) or head (`adaptHead`),
   * the compile-time mirror of PipelineImpl::adaptTail/adaptHead. Binding
   * changes nothing about how this pipeline's handlers or endpoints run — only
   * where the tail endpoint's `ctx.fireRead` (past the tail) and the head
   * endpoint's `ctx.fireWrite` (past the head) go. Bound, the message is run
   * through `TAdapter`'s static conversion (the default ForwardingAdapter
   * returns it unchanged), fired into `peer`, and `peer`'s Result propagates
   * back; unbound, it returns Result::Error.
   *
   * The PipelineRef overload is non-owning; the PipelineOwner overload
   * transfers ownership so `peer` is torn down together with this pipeline.
   * `peer` must share this pipeline's EventBase — crossings are direct,
   * same-thread calls.
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
  // endpoint context's fireRead/fireWrite land. Routes to the bound peer, or
  // returns Error when nothing is bound.
  Result forwardReadPastTail(TypeErasedBox&& msg) noexcept {
    if (boundTailPeer_) {
      return boundTailPeer_.fireRead(tailConvert_(std::move(msg)));
    }
    return Result::Error;
  }
  Result forwardWritePastHead(TypeErasedBox&& msg) noexcept {
    if (boundHeadPeer_) {
      return boundHeadPeer_.fireWrite(headConvert_(std::move(msg)));
    }
    return Result::Error;
  }

 private:
  enum class State : std::uint8_t { Inactive, Active, Closing, Closed };

  // Validate a bind peer: non-empty and sharing this pipeline's EventBase.
  void checkBindPeer(PipelineRef peer) const noexcept {
    CHECK(peer) << "bind: peer pipeline is empty";
    CHECK_EQ(peer.eventBase(), eventBase_)
        << "bind requires the peer to share this pipeline's EventBase";
  }

  ~StaticPipelineImpl() {
    if (state_ != State::Closed) {
      deactivateImpl();
      state_ = State::Closed;
      writeReadyList_.clear();
      readReadyList_.clear();
      callHandlerRemovedImpl();
    }
  }

  void deactivateImpl() noexcept {
    if (state_ != State::Active) {
      return;
    }
    state_ = State::Inactive;
    tailHandler_->onPipelineInactive();
    deactivateFrom<kHandlerCount>();
    headHandler_->onPipelineInactive();
  }

  void destroy() noexcept {
    if (guardCount_ != 0) {
      destroyPending_ = true;
      return;
    }
    destroyNow();
  }
  void destroyNow() noexcept { delete this; }

  template <std::size_t Index>
  decltype(auto) handlerAt() noexcept {
    return handlers_.template get<Index>();
  }
  template <std::size_t Index>
  decltype(auto) handlerAt() const noexcept {
    return handlers_.template get<Index>();
  }

  template <std::size_t Index>
  Result sendReadAt(HandlerId id, TypeErasedBox&& msg) noexcept {
    if constexpr (Index == kHandlerCount) {
      return Result::Error;
    } else {
      using Spec = std::tuple_element_t<Index, std::tuple<Specs...>>;
      if (id == Spec::id) {
        return fireReadTypedAt<Index>(std::move(msg));
      }
      return sendReadAt<Index + 1>(id, std::move(msg));
    }
  }
  template <std::size_t Index>
  Result sendWriteAt(HandlerId id, TypeErasedBox&& msg) noexcept {
    if constexpr (Index == kHandlerCount) {
      return Result::Error;
    } else {
      using Spec = std::tuple_element_t<Index, std::tuple<Specs...>>;
      if (id == Spec::id) {
        return fireWriteTypedAt<Index + 1>(std::move(msg));
      }
      return sendWriteAt<Index + 1>(id, std::move(msg));
    }
  }
  template <std::size_t Index>
  void sendExceptionAt(HandlerId id, folly::exception_wrapper&& e) noexcept {
    if constexpr (Index < kHandlerCount) {
      using Spec = std::tuple_element_t<Index, std::tuple<Specs...>>;
      if (id == Spec::id) {
        fireExceptionTypedAt<Index>(std::move(e));
        return;
      }
      sendExceptionAt<Index + 1>(id, std::move(e));
    }
  }

  template <std::size_t... Index>
  void activateHandlers(std::index_sequence<Index...>) noexcept {
    (activateHandler<Index>(), ...);
    if constexpr (kHasStaticHandlers && kStaticHandlerIndex == kHandlerCount) {
      activateStaticHandlers();
    }
  }
  template <std::size_t Index>
  void activateHandler() noexcept {
    if constexpr (kHasStaticHandlers && Index == kStaticHandlerIndex) {
      activateStaticHandlers();
    }
    auto& slot = handlerAt<Index>();
    using H = typename std::remove_reference_t<decltype(slot)>::Handler;
    using Context = typename std::remove_reference_t<decltype(slot)>::Context;
    if constexpr (requires(H& handler, Context& context) {
                    {
                      handler.onPipelineActive(context)
                    } noexcept -> std::same_as<void>;
                  }) {
      slot.handler().onPipelineActive(slot.context());
    }
  }

  template <std::size_t Count>
  void deactivateTypedHandlers() noexcept {
    if constexpr (Count > 0) {
      auto& slot = handlerAt<Count - 1>();
      using H = typename std::remove_reference_t<decltype(slot)>::Handler;
      using Context = typename std::remove_reference_t<decltype(slot)>::Context;
      if constexpr (requires(H& handler, Context& context) {
                      {
                        handler.onPipelineInactive(context)
                      } noexcept -> std::same_as<void>;
                    }) {
        slot.handler().onPipelineInactive(slot.context());
      }
      deactivateTypedHandlers<Count - 1>();
    }
  }

  void callHandlerAdded() noexcept {
    Guard guard(this);
    headHandler_->handlerAdded();
    callHandlerAddedImpl(std::make_index_sequence<kHandlerCount>{});
    tailHandler_->handlerAdded();
  }
  template <std::size_t... Index>
  void callHandlerAddedImpl(std::index_sequence<Index...>) noexcept {
    (callHandlerAddedAt<Index>(), ...);
    if constexpr (kHasStaticHandlers && kStaticHandlerIndex == kHandlerCount) {
      callStaticHandlersAdded();
    }
  }

  template <std::size_t Index>
  void callHandlerAddedAt() noexcept {
    if constexpr (kHasStaticHandlers && Index == kStaticHandlerIndex) {
      callStaticHandlersAdded();
    }
    handlerAt<Index>().handler().handlerAdded(handlerAt<Index>().context());
  }
  void callHandlerRemovedImpl() noexcept {
    tailHandler_->handlerRemoved();
    removeHandlers<kHandlerCount>();
    headHandler_->handlerRemoved();
  }
  template <std::size_t Count>
  void removeHandlers() noexcept {
    if constexpr (kHasStaticHandlers && Count == kStaticHandlerIndex) {
      callStaticHandlersRemoved();
      removeTypedHandlers<Count>();
    } else if constexpr (Count > 0) {
      auto& slot = handlerAt<Count - 1>();
      slot.handler().handlerRemoved(slot.context());
      removeHandlers<Count - 1>();
    }
  }

  template <std::size_t Count>
  void removeTypedHandlers() noexcept {
    if constexpr (Count > 0) {
      auto& slot = handlerAt<Count - 1>();
      slot.handler().handlerRemoved(slot.context());
      removeTypedHandlers<Count - 1>();
    }
  }

  template <std::size_t... Index>
  void initializeHooks(std::index_sequence<Index...>) noexcept {
    headWriteReadyHook_.handlerIndex = handlerCount();
    (initializeHook<Index>(), ...);
    for (std::size_t i = 0; i < staticHandlerCount(); ++i) {
      const auto logicalIndex = kStaticHandlerIndex + i;
      if (auto* hook = staticWriteReadyHook(i)) {
        hook->handlerIndex = logicalIndex;
      }
      if (auto* hook = staticReadReadyHook(i)) {
        hook->handlerIndex = logicalIndex;
      }
    }
  }
  template <std::size_t Index>
  void initializeHook() noexcept {
    auto& slot = handlerAt<Index>();
    if (auto* hook = slot.writeReadyHook()) {
      hook->handlerIndex = logicalHandlerIndex<Index>();
    }
    if (auto* hook = slot.readReadyHook()) {
      hook->handlerIndex = logicalHandlerIndex<Index>();
    }
  }
  template <std::size_t Index>
  WriteReadyHook* writeReadyHook() noexcept {
    if constexpr (Index == kHandlerCount) {
      return &headWriteReadyHook_;
    } else {
      return handlerAt<Index>().writeReadyHook();
    }
  }
  template <std::size_t Index>
  ReadReadyHook* readReadyHook() noexcept {
    if constexpr (Index == kHandlerCount) {
      return nullptr;
    } else {
      return handlerAt<Index>().readReadyHook();
    }
  }

  void dispatchHeadWriteReady() noexcept {
    if constexpr (requires(HeadHandler& h, EndpointContext& ctx) {
                    h.onWriteReady(ctx);
                  }) {
      headHandler_->onWriteReady(endpointContext_);
    }
  }

  void dispatchWriteReadyTarget(std::size_t target) noexcept {
    if (isStaticHandlerIndex(target)) {
      staticOnWriteReady(target - kStaticHandlerIndex);
      return;
    }
    dispatchWriteReady<0>(target);
  }

  template <std::size_t Index>
  void dispatchWriteReady(std::size_t target) noexcept {
    if constexpr (Index < kHandlerCount) {
      if (target == logicalHandlerIndex<Index>()) {
        auto& slot = handlerAt<Index>();
        using H = typename std::remove_reference_t<decltype(slot)>::Handler;
        using Context =
            typename std::remove_reference_t<decltype(slot)>::Context;
        if constexpr (OutboundHandler<H, Context>) {
          slot.handler().onWriteReady(slot.context());
        }
        return;
      }
      dispatchWriteReady<Index + 1>(target);
    }
  }
  void dispatchReadReadyTarget(std::size_t target) noexcept {
    if (isStaticHandlerIndex(target)) {
      staticOnReadReady(target - kStaticHandlerIndex);
      return;
    }
    dispatchReadReady<0>(target);
  }

  template <std::size_t Index>
  void dispatchReadReady(std::size_t target) noexcept {
    if constexpr (Index < kHandlerCount) {
      if (target == logicalHandlerIndex<Index>()) {
        auto& slot = handlerAt<Index>();
        using H = typename std::remove_reference_t<decltype(slot)>::Handler;
        using Context =
            typename std::remove_reference_t<decltype(slot)>::Context;
        if constexpr (InboundHandler<H, Context>) {
          slot.handler().onReadReady(slot.context());
        }
        return;
      }
      dispatchReadReady<Index + 1>(target);
    }
  }

  template <PipelineEvent E>
  void fireEventImpl(const void* payload) noexcept {
    if (isClosed()) {
      return;
    }
    Guard guard(this);
    dispatchEndpointEvent<E>(*tailHandler_, payload);
    dispatchHandlerEventsReverse<E>(
        payload, std::make_index_sequence<kHandlerCount>{});
    dispatchEndpointEvent<E>(*headHandler_, payload);
  }
  template <PipelineEvent E, typename H>
  static void dispatchEndpointEvent(H& handler, const void* payload) noexcept {
    if constexpr (requires { typename H::SubscribedEvents; }) {
      if constexpr (kStaticEventSetContains<typename H::SubscribedEvents, E>) {
        static_assert(EndpointTypeEventSubscriber<H>);
        if constexpr (std::is_void_v<typename E::Payload>) {
          handler.template on<E>();
        } else {
          handler.template on<E>(
              *static_cast<const typename E::Payload*>(payload));
        }
      }
    }
  }
  template <PipelineEvent E, std::size_t Index>
  void dispatchHandlerEvent(const void* payload) noexcept {
    if constexpr (kHasStaticHandlers && Index + 1 == kStaticHandlerIndex) {
      dispatchStaticHandlerEvents(eventKey<E>(), payload);
    }
    dispatchTypedHandlerEvent<E, Index>(payload);
  }

  template <PipelineEvent E, std::size_t Index>
  void dispatchTypedHandlerEvent(const void* payload) noexcept {
    auto& slot = handlerAt<Index>();
    using Slot = std::remove_reference_t<decltype(slot)>;
    using H = typename Slot::Handler;
    using Context = typename Slot::Context;
    if constexpr (requires { typename H::SubscribedEvents; }) {
      if constexpr (kStaticEventSetContains<typename H::SubscribedEvents, E>) {
        static_assert(TypeEventSubscriber<H, Context>);
        if constexpr (std::is_void_v<typename E::Payload>) {
          slot.handler().template on<E>(slot.context());
        } else {
          slot.handler().template on<E>(
              slot.context(),
              *static_cast<const typename E::Payload*>(payload));
        }
      }
    }
  }
  template <PipelineEvent E, std::size_t... Index>
  void dispatchHandlerEventsReverse(
      const void* payload, std::index_sequence<Index...>) noexcept {
    (dispatchHandlerEvent<E, kHandlerCount - 1 - Index>(payload), ...);
    if constexpr (kHasStaticHandlers && kStaticHandlerIndex == 0) {
      dispatchStaticHandlerEvents(eventKey<E>(), payload);
    }
  }
  template <typename H, PipelineEvent... Evs>
  static void dispatchEndpointEventSetByKey(
      H& handler, EventKey key, const void* payload, Events<Evs...>) noexcept {
    static_cast<void>(
        ((key == eventKey<Evs>()
              ? (dispatchEndpointEvent<Evs>(handler, payload), true)
              : false) ||
         ...));
  }

  template <typename H>
  static void dispatchEndpointEventByKey(
      H& handler, EventKey key, const void* payload) noexcept {
    if constexpr (requires { typename H::SubscribedEvents; }) {
      dispatchEndpointEventSetByKey(
          handler, key, payload, typename H::SubscribedEvents{});
    }
  }

  template <std::size_t Index, PipelineEvent... Evs>
  void dispatchHandlerEventSetByKey(
      EventKey key, const void* payload, Events<Evs...>) noexcept {
    static_cast<void>(
        ((key == eventKey<Evs>()
              ? (dispatchTypedHandlerEvent<Evs, Index>(payload), true)
              : false) ||
         ...));
  }

  template <std::size_t Index>
  void dispatchHandlerEventByKey(EventKey key, const void* payload) noexcept {
    if constexpr (kHasStaticHandlers && Index + 1 == kStaticHandlerIndex) {
      dispatchStaticHandlerEvents(key, payload);
    }
    auto& slot = handlerAt<Index>();
    using H = typename std::remove_reference_t<decltype(slot)>::Handler;
    if constexpr (requires { typename H::SubscribedEvents; }) {
      dispatchHandlerEventSetByKey<Index>(
          key, payload, typename H::SubscribedEvents{});
    }
  }

  template <std::size_t... Index>
  void dispatchHandlerEventsReverseByKey(
      EventKey key,
      const void* payload,
      std::index_sequence<Index...>) noexcept {
    (dispatchHandlerEventByKey<kHandlerCount - 1 - Index>(key, payload), ...);
    if constexpr (kHasStaticHandlers && kStaticHandlerIndex == 0) {
      dispatchStaticHandlerEvents(key, payload);
    }
  }

  template <std::size_t TypedIndex>
  std::size_t logicalHandlerIndex() const noexcept {
    if constexpr (kHasStaticHandlers && TypedIndex >= kStaticHandlerIndex) {
      return TypedIndex + staticHandlerCount();
    }
    return TypedIndex;
  }

  bool isStaticHandlerIndex(std::size_t index) const noexcept {
    return kHasStaticHandlers && index >= kStaticHandlerIndex &&
        index < kStaticHandlerIndex + staticHandlerCount();
  }

  std::size_t staticHandlerCount() const noexcept {
    if constexpr (kHasStaticSegments) {
      return staticSegments_.size();
    } else {
      return staticHandlers_.size();
    }
  }

  struct SegmentPosition {
    ErasedStaticSegment* segment;
    std::size_t localIndex;
  };

  SegmentPosition staticSegmentPosition(std::size_t index) noexcept {
    for (auto& segment : staticSegments_.segments()) {
      if (index < segment.handlerCount()) {
        return {&segment, index};
      }
      index -= segment.handlerCount();
    }
    std::terminate();
  }

  HandlerId staticHandlerId(std::size_t index) noexcept {
    if constexpr (kHasStaticSegments) {
      auto position = staticSegmentPosition(index);
      return position.segment->handlerId(position.localIndex);
    } else {
      return staticHandlers_[index].handlerId();
    }
  }

  Result staticOnRead(std::size_t index, TypeErasedBox&& msg) noexcept {
    if constexpr (kHasStaticSegments) {
      auto position = staticSegmentPosition(index);
      if (position.localIndex == 0) {
        return position.segment->onReadFirst(std::move(msg));
      }
      return position.segment->onReadAt(position.localIndex, std::move(msg));
    } else {
      return staticHandlers_[index].onRead(std::move(msg));
    }
  }
  Result staticOnWrite(std::size_t index, TypeErasedBox&& msg) noexcept {
    if constexpr (kHasStaticSegments) {
      auto position = staticSegmentPosition(index);
      if (position.localIndex + 1 == position.segment->handlerCount()) {
        return position.segment->onWriteLast(std::move(msg));
      }
      return position.segment->onWriteAt(position.localIndex, std::move(msg));
    } else {
      return staticHandlers_[index].onWrite(std::move(msg));
    }
  }
  void staticOnException(
      std::size_t index, folly::exception_wrapper&& e) noexcept {
    if constexpr (kHasStaticSegments) {
      auto position = staticSegmentPosition(index);
      if (position.localIndex == 0) {
        position.segment->onExceptionFirst(std::move(e));
      } else {
        position.segment->onExceptionAt(position.localIndex, std::move(e));
      }
    } else {
      staticHandlers_[index].onException(std::move(e));
    }
  }

#define THRIFT_STATIC_SEGMENT_VOID_DISPATCH(                \
    Name, SegmentMethod, HandlerMethod)                     \
  void Name(std::size_t index) noexcept {                   \
    if constexpr (kHasStaticSegments) {                     \
      auto position = staticSegmentPosition(index);         \
      position.segment->SegmentMethod(position.localIndex); \
    } else {                                                \
      staticHandlers_[index].HandlerMethod();               \
    }                                                       \
  }

  THRIFT_STATIC_SEGMENT_VOID_DISPATCH(
      staticOnWriteReady, onWriteReady, onWriteReady)
  THRIFT_STATIC_SEGMENT_VOID_DISPATCH(
      staticOnReadReady, onReadReady, onReadReady)
  THRIFT_STATIC_SEGMENT_VOID_DISPATCH(
      staticOnPipelineActive, onPipelineActive, onPipelineActive)
  THRIFT_STATIC_SEGMENT_VOID_DISPATCH(
      staticOnPipelineInactive, onPipelineInactive, onPipelineInactive)
  THRIFT_STATIC_SEGMENT_VOID_DISPATCH(
      staticHandlerAdded, handlerAdded, handlerAdded)
  THRIFT_STATIC_SEGMENT_VOID_DISPATCH(
      staticHandlerRemoved, handlerRemoved, handlerRemoved)
#undef THRIFT_STATIC_SEGMENT_VOID_DISPATCH

  void staticFireEvent(
      std::size_t index, EventKey key, const void* payload) noexcept {
    if constexpr (kHasStaticSegments) {
      auto position = staticSegmentPosition(index);
      position.segment->fireEvent(position.localIndex, key, payload);
    } else {
      staticHandlers_[index].fireEvent(key, payload);
    }
  }
  WriteReadyHook* staticWriteReadyHook(std::size_t index) noexcept {
    if constexpr (kHasStaticSegments) {
      auto position = staticSegmentPosition(index);
      return position.segment->writeReadyHook(position.localIndex);
    } else {
      return staticHandlers_[index].writeReadyHook();
    }
  }
  ReadReadyHook* staticReadReadyHook(std::size_t index) noexcept {
    if constexpr (kHasStaticSegments) {
      auto position = staticSegmentPosition(index);
      return position.segment->readReadyHook(position.localIndex);
    } else {
      return staticHandlers_[index].readReadyHook();
    }
  }

  std::optional<std::size_t> findStaticHandler(HandlerId id) noexcept {
    for (std::size_t i = 0; i < staticHandlerCount(); ++i) {
      if (staticHandlerId(i) == id) {
        return i;
      }
    }
    return std::nullopt;
  }

  Result fireReadFromStaticHandler(
      std::size_t index, TypeErasedBox&& msg) noexcept {
    if (index == staticHandlerCount()) {
      return fireReadTypedAt<kStaticHandlerIndex>(std::move(msg));
    }
    return staticOnRead(index, std::move(msg));
  }

  Result fireWriteFromStaticHandler(
      std::size_t count, TypeErasedBox&& msg) noexcept {
    if (count == 0) {
      return fireWriteTypedAt<kStaticHandlerIndex>(std::move(msg));
    }
    return staticOnWrite(count - 1, std::move(msg));
  }

  void fireExceptionFromStaticHandler(
      std::size_t index, folly::exception_wrapper&& e) noexcept {
    if (index == staticHandlerCount()) {
      fireExceptionTypedAt<kStaticHandlerIndex>(std::move(e));
      return;
    }
    staticOnException(index, std::move(e));
  }

  Result fireReadAfterSegment(
      std::size_t segmentIndex, TypeErasedBox&& msg) noexcept {
    if constexpr (!kHasStaticSegments) {
      return fireReadTypedAt<kStaticHandlerIndex>(std::move(msg));
    } else {
      auto& segments = staticSegments_.segments();
      if (++segmentIndex == segments.size()) {
        return fireReadTypedAt<kStaticHandlerIndex>(std::move(msg));
      }
      return segments[segmentIndex].onReadFirst(std::move(msg));
    }
  }

  Result fireWriteBeforeSegment(
      std::size_t segmentIndex, TypeErasedBox&& msg) noexcept {
    if constexpr (!kHasStaticSegments) {
      return fireWriteTypedAt<kStaticHandlerIndex>(std::move(msg));
    } else {
      if (segmentIndex == 0) {
        return fireWriteTypedAt<kStaticHandlerIndex>(std::move(msg));
      }
      return staticSegments_.segments()[segmentIndex - 1].onWriteLast(
          std::move(msg));
    }
  }

  void fireExceptionAfterSegment(
      std::size_t segmentIndex, folly::exception_wrapper&& e) noexcept {
    if constexpr (!kHasStaticSegments) {
      fireExceptionTypedAt<kStaticHandlerIndex>(std::move(e));
    } else {
      auto& segments = staticSegments_.segments();
      if (++segmentIndex == segments.size()) {
        fireExceptionTypedAt<kStaticHandlerIndex>(std::move(e));
        return;
      }
      segments[segmentIndex].onExceptionFirst(std::move(e));
    }
  }

  void activateStaticHandlers() noexcept {
    for (std::size_t i = 0; i < staticHandlerCount(); ++i) {
      staticOnPipelineActive(i);
    }
  }

  void deactivateStaticHandlersFrom(std::size_t count) noexcept {
    while (count != 0) {
      staticOnPipelineInactive(--count);
    }
  }

  void callStaticHandlersAdded() noexcept {
    for (std::size_t i = 0; i < staticHandlerCount(); ++i) {
      staticHandlerAdded(i);
    }
  }

  void callStaticHandlersRemoved() noexcept {
    for (std::size_t i = staticHandlerCount(); i != 0; --i) {
      staticHandlerRemoved(i - 1);
    }
  }

  void dispatchStaticHandlerEvents(EventKey key, const void* payload) noexcept {
    for (std::size_t i = staticHandlerCount(); i != 0; --i) {
      staticFireEvent(i - 1, key, payload);
    }
  }

  void bindStaticHandlerContexts() noexcept {
    if constexpr (!kHasStaticHandlers) {
      return;
    }
    const auto& ops = staticHandlerContextOps();
    if constexpr (kHasStaticSegments) {
      std::size_t base = 0;
      std::size_t segmentIndex = 0;
      for (auto& segment : staticSegments_.segments()) {
        segment.bindContext(this, ops, base, segmentIndex++);
        base += segment.handlerCount();
      }
    } else {
      for (std::size_t i = 0; i < staticHandlers_.size(); ++i) {
        staticHandlers_[i].bindContext(this, ops, i);
      }
    }
  }

  static const StaticHandlerContextOps& staticHandlerContextOps() noexcept {
    static const StaticHandlerContextOps ops{
        .handlerId =
            +[](const void* p, std::size_t index) noexcept {
              return const_cast<Self*>(static_cast<const Self*>(p))
                  ->staticHandlerId(index);
            },
        .handlerIndex =
            +[](const void*, std::size_t index) noexcept {
              return kStaticHandlerIndex + index;
            },
        .fireRead =
            +[](void* p, std::size_t index, TypeErasedBox&& msg) noexcept {
              return static_cast<Self*>(p)->fireReadFromStaticHandler(
                  index + 1, std::move(msg));
            },
        .fireWrite =
            +[](void* p, std::size_t index, TypeErasedBox&& msg) noexcept {
              if (index == 0) {
                return static_cast<Self*>(p)
                    ->template fireWriteTypedAt<kStaticHandlerIndex>(
                        std::move(msg));
              }
              return static_cast<Self*>(p)->fireWriteFromStaticHandler(
                  index, std::move(msg));
            },
        .fireException =
            +[](void* p,
                std::size_t index,
                folly::exception_wrapper&& e) noexcept {
              static_cast<Self*>(p)->fireExceptionFromStaticHandler(
                  index + 1, std::move(e));
            },
        .fireReadAfterSegment =
            +[](void* p,
                std::size_t segmentIndex,
                TypeErasedBox&& msg) noexcept {
              return static_cast<Self*>(p)->fireReadAfterSegment(
                  segmentIndex, std::move(msg));
            },
        .fireWriteBeforeSegment =
            +[](void* p,
                std::size_t segmentIndex,
                TypeErasedBox&& msg) noexcept {
              return static_cast<Self*>(p)->fireWriteBeforeSegment(
                  segmentIndex, std::move(msg));
            },
        .fireExceptionAfterSegment =
            +[](void* p,
                std::size_t segmentIndex,
                folly::exception_wrapper&& e) noexcept {
              static_cast<Self*>(p)->fireExceptionAfterSegment(
                  segmentIndex, std::move(e));
            },
        .deactivate =
            +[](void* p, std::size_t index) noexcept {
              auto* self = static_cast<Self*>(p);
              self->deactivateStaticHandlersFrom(index + 1);
              self->template deactivateTypedHandlers<kStaticHandlerIndex>();
            },
        .fireEvent =
            +[](void* p, EventKey key, const void* payload) noexcept {
              static_cast<Self*>(p)->fireBoundEvent(
                  BoundEventRoute{key}, payload);
            },
        .allocate =
            +[](void* p, std::size_t size) noexcept {
              return static_cast<Self*>(p)->allocate(size);
            },
        .copyBuffer =
            +[](void* p, const void* data, std::size_t size) noexcept {
              return static_cast<Self*>(p)->copyBuffer(data, size);
            },
        .eventBase =
            +[](void* p) noexcept {
              return static_cast<Self*>(p)->eventBase();
            },
        .pipeline =
            +[](void* p) noexcept {
              return PipelineRef(*static_cast<Self*>(p));
            },
        .close = +[](void* p) noexcept { static_cast<Self*>(p)->close(); },
        .awaitWriteReady =
            +[](void* p, std::size_t index) noexcept {
              static_cast<Self*>(p)->awaitStaticWriteReady(index);
            },
        .cancelAwaitWriteReady =
            +[](void* p, std::size_t index) noexcept {
              static_cast<Self*>(p)->cancelStaticWriteReady(index);
            },
        .isAwaitingWriteReady =
            +[](const void* p, std::size_t index) noexcept {
              return static_cast<const Self*>(p)->isAwaitingStaticWriteReady(
                  index);
            },
        .awaitReadReady =
            +[](void* p, std::size_t index) noexcept {
              static_cast<Self*>(p)->awaitStaticReadReady(index);
            },
        .cancelAwaitReadReady =
            +[](void* p, std::size_t index) noexcept {
              static_cast<Self*>(p)->cancelStaticReadReady(index);
            },
        .isAwaitingReadReady =
            +[](const void* p, std::size_t index) noexcept {
              return static_cast<const Self*>(p)->isAwaitingStaticReadReady(
                  index);
            },
    };
    return ops;
  }

  void awaitStaticWriteReady(std::size_t index) noexcept {
    if (!isClosed()) {
      if (auto* hook = staticWriteReadyHook(index);
          hook != nullptr && !hook->hook.is_linked()) {
        writeReadyList_.push_back(*hook);
      }
    }
  }
  void cancelStaticWriteReady(std::size_t index) noexcept {
    if (auto* hook = staticWriteReadyHook(index);
        hook != nullptr && hook->hook.is_linked()) {
      hook->hook.unlink();
    }
  }
  bool isAwaitingStaticWriteReady(std::size_t index) const noexcept {
    auto* hook = const_cast<Self*>(this)->staticWriteReadyHook(index);
    return hook != nullptr && hook->hook.is_linked();
  }
  void awaitStaticReadReady(std::size_t index) noexcept {
    if (!isClosed()) {
      if (auto* hook = staticReadReadyHook(index);
          hook != nullptr && !hook->hook.is_linked()) {
        readReadyList_.push_back(*hook);
      }
    }
  }
  void cancelStaticReadReady(std::size_t index) noexcept {
    if (auto* hook = staticReadReadyHook(index);
        hook != nullptr && hook->hook.is_linked()) {
      hook->hook.unlink();
    }
  }
  bool isAwaitingStaticReadReady(std::size_t index) const noexcept {
    auto* hook = const_cast<Self*>(this)->staticReadReadyHook(index);
    return hook != nullptr && hook->hook.is_linked();
  }

  template <HandlerId Id, std::size_t Index>
  static consteval std::size_t handlerIndex() {
    if constexpr (Index == kHandlerCount) {
      return kHandlerCount;
    } else {
      using Spec = std::tuple_element_t<Index, std::tuple<Specs...>>;
      if constexpr (Spec::id == Id) {
        return Index;
      } else {
        return handlerIndex<Id, Index + 1>();
      }
    }
  }

  // Optionally pipeline-owned endpoints/allocator, co-located in this
  // allocation. Declared before handlers_ so they are destroyed after all
  // handlers; the allocator slot is declared first so it is destroyed last.
  OwnedEndpointSlot<Allocator> ownedAllocator_;
  OwnedEndpointSlot<TailHandler> ownedTail_;
  OwnedEndpointSlot<HeadHandler> ownedHead_;
  folly::EventBase* eventBase_;
  HeadHandler* headHandler_;
  TailHandler* tailHandler_;
  Allocator* allocator_;
  StateTuple stateStorage_;
  StaticHandlerStorage<Self, 0, StateTuple, Specs...> handlers_;
  [[no_unique_address]] ErasedStaticHandlerStorage<kHasErasedHandlers>
      staticHandlers_;
  [[no_unique_address]] ErasedStaticSegmentStorage<kHasStaticSegments>
      staticSegments_;
  EndpointContext endpointContext_;
  State state_{State::Inactive};
  WriteReadyList writeReadyList_;
  ReadReadyList readReadyList_;
  WriteReadyHook headWriteReadyHook_;
  std::size_t writeReadyGeneration_{0};
  std::size_t readReadyGeneration_{0};
  std::size_t guardCount_{0};
  bool writeReadyDispatching_{false};
  bool writeReadyDispatchPending_{false};
  bool readReadyDispatching_{false};
  bool readReadyDispatchPending_{false};
  bool destroyPending_{false};

  // Bound peer pipelines (set via adaptTail/adaptHead); see the binding model
  // on PipelineImpl. boundTailPeer_/boundHeadPeer_ are the targets
  // forwardReadPastTail/forwardWritePastHead route to, empty until a bind. The
  // owned*Peer_ members are engaged only by the owning overload and keep an
  // owned peer alive for this pipeline's lifetime (destroyed with it).
  // *Convert_ is the seam adapter's static conversion, set alongside the peer
  // at bind (non-null whenever the peer is bound).
  PipelineRef boundTailPeer_;
  PipelineRef boundHeadPeer_;
  PipelineOwner ownedTailPeer_;
  PipelineOwner ownedHeadPeer_;
  TypeErasedBox (*tailConvert_)(TypeErasedBox&&) noexcept {nullptr};
  TypeErasedBox (*headConvert_)(TypeErasedBox&&) noexcept {nullptr};
};

} // namespace apache::thrift::fast_thrift::channel_pipeline::detail
