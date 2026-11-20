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
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/ExecutionDomain.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Handler.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/HandlerTag.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineAdapter.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineImpl.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineRef.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/ErasedStaticHandler.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/ErasedStaticSegment.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/ReadyWaiters.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/StaticHandler.h>

#include <glog/logging.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace apache::thrift::fast_thrift::channel_pipeline::detail {

enum class StaticHandlerDirection { Inbound, Outbound, Duplex, Automatic };

template <StaticHandlerDirection Direction, typename H, typename Context>
concept StaticHandlerForDirection =
    (Direction == StaticHandlerDirection::Inbound &&
     InboundHandler<H, Context>) ||
    (Direction == StaticHandlerDirection::Outbound &&
     OutboundHandler<H, Context>) ||
    (Direction == StaticHandlerDirection::Duplex &&
     DuplexHandler<H, Context>) ||
    (Direction == StaticHandlerDirection::Automatic &&
     (InboundHandler<H, Context> || OutboundHandler<H, Context>));

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
        Tail(pipeline, factories) {
    checkReadyCallbacks<H, Context>();
  }

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

template <
    bool OwnHead = false,
    bool OwnTail = false,
    bool OwnAllocator = false,
    bool EnableBinding = false>
struct StaticPipelinePolicy {
  static constexpr bool uniformHandlers = false;
  using Resources = void;
  static constexpr bool ownHead = OwnHead;
  static constexpr bool ownTail = OwnTail;
  static constexpr bool ownAllocator = OwnAllocator;
  static constexpr bool enableBinding = EnableBinding;
};

enum class StaticEndpointRole { Head, Tail, Allocator };

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

template <typename T, bool Enabled, StaticEndpointRole Role>
struct OptionalOwnedEndpointSlot {};

template <typename T, StaticEndpointRole Role>
struct OptionalOwnedEndpointSlot<T, true, Role> : OwnedEndpointSlot<T> {};

template <
    typename HeadHandler,
    typename TailHandler,
    typename Allocator,
    typename StateTuple,
    typename Policy>
struct StaticPipelineEndpointResources {
  StaticPipelineEndpointResources(
      folly::EventBase* eventBase,
      const EndpointSource& headSource,
      const EndpointSource& tailSource,
      const EndpointSource& allocatorSource,
      StateTuple&& state)
      : eventBase_(eventBase),
        headHandler_(static_cast<HeadHandler*>(headSource.external)),
        tailHandler_(static_cast<TailHandler*>(tailSource.external)),
        allocator_(static_cast<Allocator*>(allocatorSource.external)),
        stateStorage_(std::move(state)) {}

  // Optionally pipeline-owned endpoints/allocator, co-located in this
  // allocation. Declared before handlers_ so they are destroyed after all
  // handlers; the allocator slot is declared first so it is destroyed last.
  [[no_unique_address]] OptionalOwnedEndpointSlot<
      Allocator,
      Policy::ownAllocator,
      StaticEndpointRole::Allocator> ownedAllocator_;
  [[no_unique_address]] OptionalOwnedEndpointSlot<
      TailHandler,
      Policy::ownTail,
      StaticEndpointRole::Tail> ownedTail_;
  [[no_unique_address]] OptionalOwnedEndpointSlot<
      HeadHandler,
      Policy::ownHead,
      StaticEndpointRole::Head> ownedHead_;
  folly::EventBase* eventBase_;
  HeadHandler* headHandler_;
  TailHandler* tailHandler_;
  Allocator* allocator_;
  StateTuple stateStorage_;
};

struct NoSpecAllocator {};

/** Resources outlive adapters and handlers; the allocator is destroyed last. */
template <
    typename TaskState,
    ExecutionDomain Domain,
    typename Allocator = NoSpecAllocator>
struct SpecPipelineResources {
  SpecPipelineResources(
      TaskState state, Domain domain, Allocator allocator = {})
      : allocator_(std::move(allocator)),
        executionDomain_(std::move(domain)),
        taskState_(std::move(state)) {}

  [[no_unique_address]] Allocator allocator_;
  [[no_unique_address]] Domain executionDomain_;
  [[no_unique_address]] TaskState taskState_;
  AdapterStorage readAdapter_;
  AdapterStorage writeAdapter_;
};

template <
    typename TaskState,
    ExecutionDomain Domain,
    typename Allocator = NoSpecAllocator>
struct SpecPipelinePolicy : StaticPipelinePolicy<> {
  static constexpr bool uniformHandlers = true;
  using Resources = SpecPipelineResources<TaskState, Domain, Allocator>;
};

struct NoStaticPipelineEndpoint {};

struct NoStaticPipelineEndpointContext {
  template <typename Pipeline>
  explicit NoStaticPipelineEndpointContext(Pipeline*) noexcept {}
};

template <bool Enabled>
struct StaticPipelineBindingStorage {};

template <>
struct StaticPipelineBindingStorage<true> {
  PipelineRef boundTailPeer_;
  PipelineRef boundHeadPeer_;
  PipelineOwner ownedTailPeer_;
  PipelineOwner ownedHeadPeer_;
  TypeErasedBox (*tailConvert_)(TypeErasedBox&&) noexcept {nullptr};
  TypeErasedBox (*headConvert_)(TypeErasedBox&&) noexcept {nullptr};
};

template <
    typename HeadHandler,
    typename TailHandler,
    typename Allocator,
    typename StateTuple,
    typename StaticHandlerConfig,
    typename Policy = StaticPipelinePolicy<>,
    typename... Specs>
class StaticPipelineImpl final {
 public:
  using Self = StaticPipelineImpl;
  static constexpr bool kUniformHandlers = Policy::uniformHandlers;
  using Resources = std::conditional_t<
      kUniformHandlers,
      typename Policy::Resources,
      StaticPipelineEndpointResources<
          HeadHandler,
          TailHandler,
          Allocator,
          StateTuple,
          Policy>>;
  using Readiness =
      std::conditional_t<kUniformHandlers, IntrusiveReadyWaiters, ReadyWaiters>;
  static constexpr std::size_t kHandlerCount = sizeof...(Specs);
  static constexpr bool kHasStaticHandlers = StaticHandlerConfig::enabled;
  static_assert(
      !kUniformHandlers || (kHandlerCount > 0 && !kHasStaticHandlers));
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
    requires(!kUniformHandlers)
      : resources_(
            eventBase,
            headSource,
            tailSource,
            allocatorSource,
            std::move(state)),
        handlers_(this, factories),
        staticHandlers_(std::move(staticHandlers)),
        staticSegments_(std::move(staticSegments)),
        endpointContext_(this) {
    static_assert(ValidEndpointPair<HeadHandler, TailHandler, EndpointContext>);
    // Construct any pipeline-owned endpoints in place; external components keep
    // the raw pointer set above.
    if constexpr (Policy::ownHead) {
      if (headSource.ops) {
        resources_.headHandler_ = resources_.ownedHead_.construct(headSource);
      }
    } else {
      CHECK(headSource.ops == nullptr)
          << "static pipeline head ownership is disabled";
    }
    if constexpr (Policy::ownTail) {
      if (tailSource.ops) {
        resources_.tailHandler_ = resources_.ownedTail_.construct(tailSource);
      }
    } else {
      CHECK(tailSource.ops == nullptr)
          << "static pipeline tail ownership is disabled";
    }
    if constexpr (Policy::ownAllocator) {
      if (allocatorSource.ops) {
        resources_.allocator_ =
            resources_.ownedAllocator_.construct(allocatorSource);
      }
    } else {
      CHECK(allocatorSource.ops == nullptr)
          << "static pipeline allocator ownership is disabled";
    }
    bindStaticHandlerContexts();
  }

  template <typename FactoryTuple>
  StaticPipelineImpl(Resources resources, FactoryTuple& factories)
    requires(kUniformHandlers)
      : resources_(std::move(resources)),
        handlers_(this, factories),
        staticHandlers_({}),
        staticSegments_({}),
        endpointContext_(this) {}

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
    if constexpr (!kUniformHandlers) {
      resources_.headHandler_->onPipelineActive();
    }
    activateHandlers(std::make_index_sequence<kHandlerCount>{});
    if constexpr (!kUniformHandlers) {
      resources_.tailHandler_->onPipelineActive();
    }
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
      if constexpr (kUniformHandlers) {
        return invokeAdapter<true>(std::move(msg));
      } else {
        return resources_.tailHandler_->onRead(
            endpointContext_, std::move(msg));
      }
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
      if constexpr (kUniformHandlers) {
        return invokeAdapter<false>(std::move(msg));
      } else {
        return resources_.headHandler_->onWrite(
            endpointContext_, std::move(msg));
      }
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
      if constexpr (!kUniformHandlers) {
        resources_.tailHandler_->onException(std::move(e));
      }
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
      if constexpr (
          (kUniformHandlers && requires(H& handler, Context& context) {
            handler.onPipelineInactive(context);
          }) || requires(H& handler, Context& context) {
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
    if (wasActive || kUniformHandlers) {
      if constexpr (!kUniformHandlers) {
        resources_.tailHandler_->onPipelineInactive();
      }
      deactivateFrom<kHandlerCount>();
      if constexpr (!kUniformHandlers) {
        resources_.headHandler_->onPipelineInactive();
      }
    }
    state_ = State::Closed;
    writeReadyWaiters_.clear();
    readReadyWaiters_.clear();
    callHandlerRemovedImpl();
  }

  bool isClosed() const noexcept {
    return state_ == State::Closing || state_ == State::Closed;
  }
  std::size_t handlerCount() const noexcept {
    return kHandlerCount + staticHandlerCount();
  }
  bool hasPendingWriteReady() const noexcept {
    return !writeReadyWaiters_.empty();
  }
  bool hasPendingReadReady() const noexcept {
    return !readReadyWaiters_.empty();
  }

  folly::EventBase* eventBase() const noexcept
    requires(!kUniformHandlers)
  {
    return resources_.eventBase_;
  }
  BytesPtr allocate(std::size_t size) noexcept
    requires(
        !kUniformHandlers ||
        requires(Resources& r) { r.allocator_.allocate(size); })
  {
    if constexpr (kUniformHandlers) {
      return resources_.allocator_.allocate(size);
    } else {
      return resources_.allocator_->allocate(size);
    }
  }
  BytesPtr copyBuffer(const void* data, std::size_t size) noexcept
    requires(
        !kUniformHandlers ||
        requires(Resources& r) { r.allocator_.copyBuffer(data, size); })
  {
    if constexpr (kUniformHandlers) {
      return resources_.allocator_.copyBuffer(data, size);
    } else {
      return resources_.allocator_->copyBuffer(data, size);
    }
  }

  template <typename T>
    requires(StaticTupleTypeCount<StateTuple, T>::value == 1)
  T& state() noexcept {
    return std::get<T>(resources_.stateStorage_);
  }
  template <typename T>
    requires(StaticTupleTypeCount<StateTuple, T>::value == 1)
  const T& state() const noexcept {
    return std::get<T>(resources_.stateStorage_);
  }

  decltype(auto) taskState() noexcept
    requires(kUniformHandlers)
  {
    return (resources_.taskState_);
  }
  decltype(auto) taskState() const noexcept
    requires(kUniformHandlers)
  {
    return (resources_.taskState_);
  }
  decltype(auto) executionDomain() noexcept
    requires(kUniformHandlers)
  {
    return (resources_.executionDomain_);
  }
  decltype(auto) executionDomain() const noexcept
    requires(kUniformHandlers)
  {
    return (resources_.executionDomain_);
  }

  bool adaptRead(AdapterStorage adapter) noexcept
    requires(kUniformHandlers)
  {
    if (state_ != State::Inactive || guardCount_ != 0) {
      return false;
    }
    Guard guard(this);
    resources_.readAdapter_ = std::move(adapter);
    return true;
  }
  bool adaptWrite(AdapterStorage adapter) noexcept
    requires(kUniformHandlers)
  {
    if (state_ != State::Inactive || guardCount_ != 0) {
      return false;
    }
    Guard guard(this);
    resources_.writeAdapter_ = std::move(adapter);
    return true;
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
    if constexpr (!kUniformHandlers) {
      dispatchEndpointEventByKey(*resources_.tailHandler_, key, payload);
    }
    dispatchHandlerEventsReverseByKey(
        key, payload, std::make_index_sequence<kHandlerCount>{});
    if constexpr (!kUniformHandlers) {
      dispatchEndpointEventByKey(*resources_.headHandler_, key, payload);
    }
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
    if constexpr (kUniformHandlers) {
      if (auto* hook = handlerAt<Index>().writeReadyHook()) {
        writeReadyWaiters_.arm(*hook);
      }
    } else {
      if (handlesWriteReady<Index>()) {
        writeReadyWaiters_.arm(logicalHandlerIndex<Index>());
      }
    }
  }
  template <std::size_t Index>
  void cancelAwaitWriteReady() noexcept {
    if constexpr (kUniformHandlers) {
      if (auto* hook = handlerAt<Index>().writeReadyHook()) {
        writeReadyWaiters_.cancel(*hook);
      }
    } else {
      if (handlesWriteReady<Index>()) {
        writeReadyWaiters_.cancel(logicalHandlerIndex<Index>());
      }
    }
  }
  template <std::size_t Index>
  bool isAwaitingWriteReady() const noexcept {
    if constexpr (kUniformHandlers) {
      auto* hook =
          const_cast<Self*>(this)->template handlerAt<Index>().writeReadyHook();
      return hook != nullptr && writeReadyWaiters_.isArmed(*hook);
    } else {
      return handlesWriteReady<Index>() &&
          writeReadyWaiters_.isArmed(logicalHandlerIndex<Index>());
    }
  }
  template <std::size_t Index>
  void awaitReadReady() noexcept {
    if (isClosed()) {
      return;
    }
    if constexpr (kUniformHandlers) {
      if (auto* hook = handlerAt<Index>().readReadyHook()) {
        readReadyWaiters_.arm(*hook);
      }
    } else {
      if (handlesReadReady<Index>()) {
        readReadyWaiters_.arm(logicalHandlerIndex<Index>());
      }
    }
  }
  template <std::size_t Index>
  void cancelAwaitReadReady() noexcept {
    if constexpr (kUniformHandlers) {
      if (auto* hook = handlerAt<Index>().readReadyHook()) {
        readReadyWaiters_.cancel(*hook);
      }
    } else {
      if (handlesReadReady<Index>()) {
        readReadyWaiters_.cancel(logicalHandlerIndex<Index>());
      }
    }
  }
  template <std::size_t Index>
  bool isAwaitingReadReady() const noexcept {
    if constexpr (kUniformHandlers) {
      auto* hook =
          const_cast<Self*>(this)->template handlerAt<Index>().readReadyHook();
      return hook != nullptr && readReadyWaiters_.isArmed(*hook);
    } else {
      return handlesReadReady<Index>() &&
          readReadyWaiters_.isArmed(logicalHandlerIndex<Index>());
    }
  }

  // Whether the head endpoint currently owns write backpressure. Same contract
  // as PipelineImpl::isHeadWriteBackpressured().
  bool isHeadWriteBackpressured() const noexcept {
    return writeReadyWaiters_.isArmed(handlerCount());
  }

  // Wakes the head endpoint, then each waiting handler from the head toward
  // the tail, then the tail. The pass ends early, without the tail, when the
  // head endpoint waits again or the pipeline closes.
  //
  // Each handler wakes at most once per pass. A handler that arms behind the
  // walk, also in its own callback, waits for the next pass. So a handler that
  // waits for a state of its own must start a pass itself when that state
  // clears.
  //
  // While the pipeline is open, a call made during a pass runs as one more pass
  // after it.
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
      writeReadyDispatchPending_ = false;
      if constexpr (kUniformHandlers) {
        writeReadyWaiters_.notify(
            [this](std::size_t index) { dispatchWriteReadyTarget(index); },
            [this] { return isClosed(); });
      } else {
        const std::size_t headIndex = handlerCount();
        bool blocked = false;

        // Write capacity originates at the head endpoint. Let it consume the
        // edge before upstream handlers retry so they observe the updated
        // transport state.
        if (writeReadyWaiters_.isArmed(headIndex)) {
          dispatchHeadWriteReady();
          blocked = isClosed() || writeReadyWaiters_.isArmed(headIndex);
        }

        // A callback may arm or cancel any handler, so the walk looks up the
        // next waiting handler after each one. It never finds the head
        // endpoint, because the pass stops as soon as the head endpoint waits.
        std::size_t begin = 0;
        while (!blocked) {
          const std::optional<std::size_t> next =
              writeReadyWaiters_.lowestArmedFrom(begin);
          if (!next) {
            break;
          }
          begin = *next + 1;
          dispatchWriteReadyTarget(*next);
          if (isClosed() || writeReadyWaiters_.isArmed(headIndex)) {
            blocked = true;
            break;
          }
        }

        if (!blocked) {
          resources_.tailHandler_->onWriteReady();
        }
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
      if constexpr (kUniformHandlers) {
        readReadyWaiters_.notify(
            [this](std::size_t index) { dispatchReadReadyTarget(index); },
            [this] { return isClosed(); });
      } else {
        // A callback may arm or cancel any handler, so the walk looks up the
        // next waiting handler after each one.
        std::size_t end = handlerCount();
        while (true) {
          const std::optional<std::size_t> next =
              readReadyWaiters_.highestArmedBelow(end);
          if (!next) {
            break;
          }
          end = *next;
          dispatchReadReadyTarget(*next);
          if (isClosed()) {
            break;
          }
        }

        if (!isClosed()) {
          resources_.headHandler_->onReadReady();
        }
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
   * Select this capability with StaticPipelineBuilder::withBinding().
   */
  template <SeamReadAdapter TAdapter = ForwardingAdapter>
    requires(Policy::enableBinding)
  void adaptTail(PipelineRef peer) noexcept {
    checkBindPeer(peer);
    bindings_.boundTailPeer_ = peer;
    bindings_.tailConvert_ = &TAdapter::adaptRead;
  }

  template <SeamReadAdapter TAdapter = ForwardingAdapter>
    requires(Policy::enableBinding)
  void adaptTail(PipelineOwner peer) noexcept {
    bindings_.ownedTailPeer_ = std::move(peer);
    adaptTail<TAdapter>(bindings_.ownedTailPeer_.ref());
  }

  template <SeamWriteAdapter TAdapter = ForwardingAdapter>
    requires(Policy::enableBinding)
  void adaptHead(PipelineRef peer) noexcept {
    checkBindPeer(peer);
    bindings_.boundHeadPeer_ = peer;
    bindings_.headConvert_ = &TAdapter::adaptWrite;
  }

  template <SeamWriteAdapter TAdapter = ForwardingAdapter>
    requires(Policy::enableBinding)
  void adaptHead(PipelineOwner peer) noexcept {
    bindings_.ownedHeadPeer_ = std::move(peer);
    adaptHead<TAdapter>(bindings_.ownedHeadPeer_.ref());
  }

  // Terminal dispatch for propagation past the tail/head endpoint — where the
  // endpoint context's fireRead/fireWrite land. Routes to the bound peer, or
  // returns Error when nothing is bound.
  Result forwardReadPastTail(TypeErasedBox&& msg) noexcept {
    if constexpr (Policy::enableBinding) {
      if (bindings_.boundTailPeer_) {
        return bindings_.boundTailPeer_.fireRead(
            bindings_.tailConvert_(std::move(msg)));
      }
    }
    return Result::Error;
  }
  Result forwardWritePastHead(TypeErasedBox&& msg) noexcept {
    if constexpr (Policy::enableBinding) {
      if (bindings_.boundHeadPeer_) {
        return bindings_.boundHeadPeer_.fireWrite(
            bindings_.headConvert_(std::move(msg)));
      }
    }
    return Result::Error;
  }

 private:
  enum class State : std::uint8_t { Inactive, Active, Closing, Closed };

  // Validate a bind peer: non-empty and sharing this pipeline's EventBase.
  void checkBindPeer(PipelineRef peer) const noexcept {
    CHECK(peer) << "bind: peer pipeline is empty";
    CHECK_EQ(peer.eventBase(), resources_.eventBase_)
        << "bind requires the peer to share this pipeline's EventBase";
  }

  ~StaticPipelineImpl() {
    if (state_ != State::Closed) {
      deactivateImpl();
      state_ = State::Closed;
      writeReadyWaiters_.clear();
      readReadyWaiters_.clear();
      callHandlerRemovedImpl();
    }
  }

  void deactivateImpl() noexcept {
    if (state_ != State::Active) {
      return;
    }
    state_ = State::Inactive;
    if constexpr (!kUniformHandlers) {
      resources_.tailHandler_->onPipelineInactive();
    }
    deactivateFrom<kHandlerCount>();
    if constexpr (!kUniformHandlers) {
      resources_.headHandler_->onPipelineInactive();
    }
  }

  template <bool Read>
  Result invokeAdapter(TypeErasedBox&& msg) noexcept
    requires(kUniformHandlers)
  {
    if (isClosed()) {
      return Result::Error;
    }
    Guard guard(this);
    if constexpr (Read) {
      return resources_.readAdapter_.invoke(std::move(msg));
    } else {
      return resources_.writeAdapter_.invoke(std::move(msg));
    }
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
    if constexpr ((kUniformHandlers && requires(H& handler, Context& context) {
                    handler.onPipelineActive(context);
                  }) || requires(H& handler, Context& context) {
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
      if constexpr (
          (kUniformHandlers && requires(H& handler, Context& context) {
            handler.onPipelineInactive(context);
          }) || requires(H& handler, Context& context) {
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
    if constexpr (!kUniformHandlers) {
      resources_.headHandler_->handlerAdded();
    }
    callHandlerAddedImpl(std::make_index_sequence<kHandlerCount>{});
    if constexpr (!kUniformHandlers) {
      resources_.tailHandler_->handlerAdded();
    }
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
    if constexpr (!kUniformHandlers) {
      resources_.tailHandler_->handlerRemoved();
    }
    removeHandlers<kHandlerCount>();
    if constexpr (!kUniformHandlers) {
      resources_.headHandler_->handlerRemoved();
    }
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

  // Whether the typed handler at Index has onWriteReady() or onReadReady(),
  // and so may wait for write ready or read ready, whatever its direction.
  // Index kHandlerCount is the head endpoint, which may wait for write ready
  // only if it has onWriteReady(), and never for read ready.
  template <std::size_t Index>
  static constexpr bool handlesWriteReady() noexcept {
    if constexpr (kUniformHandlers) {
      if constexpr (Index < kHandlerCount) {
        using Slot = std::remove_reference_t<
            decltype(std::declval<Self&>().template handlerAt<Index>())>;
        return requires(typename Slot::Handler& h) { h.writeReadyHook_; };
      } else {
        return false;
      }
    } else {
      if constexpr (Index == kHandlerCount) {
        return requires(HeadHandler& h, EndpointContext& ctx) {
          h.onWriteReady(ctx);
        };
      } else {
        using Slot = std::remove_reference_t<
            decltype(std::declval<Self&>().template handlerAt<Index>())>;
        return WriteReadyHandler<
            typename Slot::Handler,
            typename Slot::Context>;
      }
    }
  }
  template <std::size_t Index>
  static constexpr bool handlesReadReady() noexcept {
    if constexpr (kUniformHandlers) {
      if constexpr (Index < kHandlerCount) {
        using Slot = std::remove_reference_t<
            decltype(std::declval<Self&>().template handlerAt<Index>())>;
        return requires(typename Slot::Handler& h) { h.readReadyHook_; };
      } else {
        return false;
      }
    } else {
      if constexpr (Index == kHandlerCount) {
        return false;
      } else {
        using Slot = std::remove_reference_t<
            decltype(std::declval<Self&>().template handlerAt<Index>())>;
        return ReadReadyHandler<typename Slot::Handler, typename Slot::Context>;
      }
    }
  }

  void dispatchHeadWriteReady() noexcept {
    if constexpr (handlesWriteReady<kHandlerCount>()) {
      resources_.headHandler_->onWriteReady(endpointContext_);
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
        if constexpr (
            (kUniformHandlers && requires(H& h, Context& ctx) {
              h.onWriteReady(ctx);
            }) || (!kUniformHandlers && WriteReadyHandler<H, Context>)) {
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
        if constexpr (
            (kUniformHandlers && requires(H& h, Context& ctx) {
              h.onReadReady(ctx);
            }) || (!kUniformHandlers && ReadReadyHandler<H, Context>)) {
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
    if constexpr (!kUniformHandlers) {
      dispatchEndpointEvent<E>(*resources_.tailHandler_, payload);
    }
    dispatchHandlerEventsReverse<E>(
        payload, std::make_index_sequence<kHandlerCount>{});
    if constexpr (!kUniformHandlers) {
      dispatchEndpointEvent<E>(*resources_.headHandler_, payload);
    }
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
  bool staticHandlesWriteReady(std::size_t index) noexcept {
    if constexpr (kHasStaticSegments) {
      auto position = staticSegmentPosition(index);
      return position.segment->handlesWriteReady(position.localIndex);
    } else {
      return staticHandlers_[index].handlesWriteReady();
    }
  }
  bool staticHandlesReadReady(std::size_t index) noexcept {
    if constexpr (kHasStaticSegments) {
      auto position = staticSegmentPosition(index);
      return position.segment->handlesReadReady(position.localIndex);
    } else {
      return staticHandlers_[index].handlesReadReady();
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
    if (!isClosed() && staticHandlesWriteReady(index)) {
      writeReadyWaiters_.arm(kStaticHandlerIndex + index);
    }
  }
  void cancelStaticWriteReady(std::size_t index) noexcept {
    if (staticHandlesWriteReady(index)) {
      writeReadyWaiters_.cancel(kStaticHandlerIndex + index);
    }
  }
  bool isAwaitingStaticWriteReady(std::size_t index) const noexcept {
    // Safe: only reads, but the segment lookup returns a mutable segment.
    return const_cast<Self*>(this)->staticHandlesWriteReady(index) &&
        writeReadyWaiters_.isArmed(kStaticHandlerIndex + index);
  }
  void awaitStaticReadReady(std::size_t index) noexcept {
    if (!isClosed() && staticHandlesReadReady(index)) {
      readReadyWaiters_.arm(kStaticHandlerIndex + index);
    }
  }
  void cancelStaticReadReady(std::size_t index) noexcept {
    if (staticHandlesReadReady(index)) {
      readReadyWaiters_.cancel(kStaticHandlerIndex + index);
    }
  }
  bool isAwaitingStaticReadReady(std::size_t index) const noexcept {
    // Safe: only reads, but the segment lookup returns a mutable segment.
    return const_cast<Self*>(this)->staticHandlesReadReady(index) &&
        readReadyWaiters_.isArmed(kStaticHandlerIndex + index);
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

  [[no_unique_address]] Resources resources_;
  StaticHandlerStorage<Self, 0, StateTuple, Specs...> handlers_;
  [[no_unique_address]] ErasedStaticHandlerStorage<kHasErasedHandlers>
      staticHandlers_;
  [[no_unique_address]] ErasedStaticSegmentStorage<kHasStaticSegments>
      staticSegments_;
  [[no_unique_address]] std::conditional_t<
      kUniformHandlers,
      NoStaticPipelineEndpointContext,
      EndpointContext> endpointContext_;
  State state_{State::Inactive};
  bool writeReadyDispatching_{false};
  bool writeReadyDispatchPending_{false};
  bool readReadyDispatching_{false};
  bool readReadyDispatchPending_{false};
  bool destroyPending_{false};
  std::size_t guardCount_{0};
  // The write set has one more index, handlerCount(), for the head endpoint.
  Readiness writeReadyWaiters_;
  Readiness readReadyWaiters_;

  // Declared last so owned peers are destroyed before the local storage.
  [[no_unique_address]] StaticPipelineBindingStorage<Policy::enableBinding>
      bindings_;
};

} // namespace apache::thrift::fast_thrift::channel_pipeline::detail
