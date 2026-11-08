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

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/BufferAllocator.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/EndpointAdapter.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Handler.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/HandlerTag.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineImpl.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/StaticPipeline.h>

#include <cstddef>
#include <stdexcept>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace apache::thrift::fast_thrift::channel_pipeline {

/** Replaces no handler. See StaticPipelineBuilder::withHandlerOverrides(). */
struct NoHandlerOverrides {
  using Tags = std::tuple<>;
};

namespace detail {
struct StaticPipelineBuilderRebindTag {};

template <HandlerId Id, typename... Entry>
inline constexpr bool kStaticHandlerIdUnique = ((Entry::Spec::id != Id) && ...);

template <typename Tags, HandlerId Id>
inline constexpr bool kOverridesHandler = false;

template <typename... Tag, HandlerId Id>
inline constexpr bool kOverridesHandler<std::tuple<Tag...>, Id> =
    ((Tag::id == Id) || ...);

template <typename Tags, typename... Entry>
inline constexpr bool kOverriddenHandlersBuilt = false;

template <typename... Tag, typename... Entry>
inline constexpr bool kOverriddenHandlersBuilt<std::tuple<Tag...>, Entry...> =
    (!kStaticHandlerIdUnique<Tag::id, Entry...> && ...);
} // namespace detail

/** Builder for a pipeline whose handler types, IDs, and order are fixed. */
template <
    typename HeadHandler,
    typename TailHandler,
    typename Allocator = SimpleBufferAllocator,
    typename StateTuple = std::tuple<>,
    typename StaticHandlerConfig = detail::NoErasedStaticHandlers,
    typename Overrides = NoHandlerOverrides,
    typename... Entry>
class StaticPipelineBuilder {
  static_assert(BufferAllocator<Allocator>);

  template <
      typename,
      typename,
      typename,
      typename,
      typename,
      typename,
      typename...>
  friend class StaticPipelineBuilder;

 public:
  StaticPipelineBuilder() = default;
  ~StaticPipelineBuilder() = default;
  StaticPipelineBuilder(const StaticPipelineBuilder&) = delete;
  StaticPipelineBuilder& operator=(const StaticPipelineBuilder&) = delete;
  StaticPipelineBuilder(StaticPipelineBuilder&&) noexcept = default;
  StaticPipelineBuilder& operator=(StaticPipelineBuilder&&) noexcept = default;

  StaticPipelineBuilder& setEventBase(folly::EventBase* eventBase) & noexcept {
    eventBase_ = eventBase;
    return *this;
  }
  StaticPipelineBuilder&& setEventBase(
      folly::EventBase* eventBase) && noexcept {
    eventBase_ = eventBase;
    return std::move(*this);
  }

  StaticPipelineBuilder& setHead(HeadHandler* head) & noexcept {
    detail::setExternalEndpoint(headSource_, head);
    return *this;
  }
  StaticPipelineBuilder&& setHead(HeadHandler* head) && noexcept {
    detail::setExternalEndpoint(headSource_, head);
    return std::move(*this);
  }

  StaticPipelineBuilder& setTail(TailHandler* tail) & noexcept {
    detail::setExternalEndpoint(tailSource_, tail);
    return *this;
  }
  StaticPipelineBuilder&& setTail(TailHandler* tail) && noexcept {
    detail::setExternalEndpoint(tailSource_, tail);
    return std::move(*this);
  }

  StaticPipelineBuilder& setAllocator(Allocator* allocator) & noexcept {
    detail::setExternalEndpoint(allocatorSource_, allocator);
    return *this;
  }
  StaticPipelineBuilder&& setAllocator(Allocator* allocator) && noexcept {
    detail::setExternalEndpoint(allocatorSource_, allocator);
    return std::move(*this);
  }

  /**
   * Construct and own the head endpoint in place from `args`. The pipeline owns
   * the object, co-located in the single pipeline allocation and destroyed with
   * the pipeline. Overrides any prior setHead/emplaceHead. Arguments are
   * decay-copied and the object is constructed at build().
   */
  template <typename... Args>
  StaticPipelineBuilder& emplaceHead(Args&&... args) & {
    detail::prepareOwnedEndpoint<HeadHandler>(
        headSource_, std::forward<Args>(args)...);
    return *this;
  }
  template <typename... Args>
  StaticPipelineBuilder&& emplaceHead(Args&&... args) && {
    detail::prepareOwnedEndpoint<HeadHandler>(
        headSource_, std::forward<Args>(args)...);
    return std::move(*this);
  }

  /** Construct and own the tail endpoint in place. See emplaceHead. */
  template <typename... Args>
  StaticPipelineBuilder& emplaceTail(Args&&... args) & {
    detail::prepareOwnedEndpoint<TailHandler>(
        tailSource_, std::forward<Args>(args)...);
    return *this;
  }
  template <typename... Args>
  StaticPipelineBuilder&& emplaceTail(Args&&... args) && {
    detail::prepareOwnedEndpoint<TailHandler>(
        tailSource_, std::forward<Args>(args)...);
    return std::move(*this);
  }

  /** Construct and own the allocator in place. See emplaceHead. */
  template <typename... Args>
  StaticPipelineBuilder& emplaceAllocator(Args&&... args) & {
    detail::prepareOwnedEndpoint<Allocator>(
        allocatorSource_, std::forward<Args>(args)...);
    return *this;
  }
  template <typename... Args>
  StaticPipelineBuilder&& emplaceAllocator(Args&&... args) && {
    detail::prepareOwnedEndpoint<Allocator>(
        allocatorSource_, std::forward<Args>(args)...);
    return std::move(*this);
  }

  template <typename T, typename... Args>
    requires(sizeof...(Entry) == 0)
  auto addState(Args&&... args) && {
    static_assert(
        std::is_move_constructible_v<T>,
        "StaticPipelineBuilder::addState<T> requires move-constructible T");
    static_assert(
        detail::StaticTupleTypeCount<StateTuple, T>::value == 0,
        "each static pipeline state type may be registered once");
    using NewStateTuple = decltype(std::tuple_cat(
        std::declval<StateTuple&&>(), std::declval<std::tuple<T>>()));
    return StaticPipelineBuilder<
        HeadHandler,
        TailHandler,
        Allocator,
        NewStateTuple,
        StaticHandlerConfig,
        Overrides>(
        detail::StaticPipelineBuilderRebindTag{},
        eventBase_,
        std::move(headSource_),
        std::move(tailSource_),
        std::move(allocatorSource_),
        std::tuple<>{},
        std::tuple_cat(
            std::move(stateTuple_),
            std::tuple<T>(T(std::forward<Args>(args)...))),
        std::move(staticHandlers_),
        std::move(staticSegments_));
  }

  /**
   * Lets `NewOverrides` replace handlers that later addNext*() calls add, so
   * that a production pipeline can run with other handlers. It must come
   * before the first addNext*() call, and does not compile after one. A later
   * call replaces `NewOverrides` instead of adding to it.
   *
   * `NewOverrides::Tags` is a `std::tuple` of `HandlerTag` types. For a handler
   * with one of those tags, the builder calls
   * `NewOverrides::add(builder, tag, args...)` with the arguments of that
   * addNext*() call.
   *
   * `add()` must add a handler with `tag`, or build() does not compile. It can
   * add more handlers around that handler, and the builder does not replace
   * them again. The handlers that `add()` adds are part of the pipeline type,
   * and `NewOverrides` itself is not.
   *
   * build() also does not compile if a tag in `Tags` appears in no addNext*()
   * call.
   */
  template <typename NewOverrides>
    requires(sizeof...(Entry) == 0)
  auto withHandlerOverrides() && {
    return std::move(*this).template rebindOverrides<NewOverrides>();
  }

  template <typename H, HandlerId Id, typename... Args>
  auto addNextInbound(HandlerTag<Id>, Args&&... args) && {
    return std::move(*this)
        .template addHandler<detail::StaticHandlerDirection::Inbound, H, Id>(
            std::forward<Args>(args)...);
  }

  template <typename H, HandlerId Id, typename... Args>
  auto addNextOutbound(HandlerTag<Id>, Args&&... args) && {
    return std::move(*this)
        .template addHandler<detail::StaticHandlerDirection::Outbound, H, Id>(
            std::forward<Args>(args)...);
  }

  template <typename H, HandlerId Id, typename... Args>
  auto addNextDuplex(HandlerTag<Id>, Args&&... args) && {
    return std::move(*this)
        .template addHandler<detail::StaticHandlerDirection::Duplex, H, Id>(
            std::forward<Args>(args)...);
  }

  template <template <typename> typename H, HandlerId Id, typename... Args>
  auto addNextInboundTemplate(HandlerTag<Id>, Args&&... args) && {
    return std::move(*this)
        .template addHandlerTemplate<
            detail::StaticHandlerDirection::Inbound,
            H,
            Id>(std::forward<Args>(args)...);
  }

  template <template <typename> typename H, HandlerId Id, typename... Args>
  auto addNextOutboundTemplate(HandlerTag<Id>, Args&&... args) && {
    return std::move(*this)
        .template addHandlerTemplate<
            detail::StaticHandlerDirection::Outbound,
            H,
            Id>(std::forward<Args>(args)...);
  }

  template <template <typename> typename H, HandlerId Id, typename... Args>
  auto addNextDuplexTemplate(HandlerTag<Id>, Args&&... args) && {
    return std::move(*this)
        .template addHandlerTemplate<
            detail::StaticHandlerDirection::Duplex,
            H,
            Id>(std::forward<Args>(args)...);
  }

  auto addStaticHandlers(std::vector<detail::ErasedStaticHandler> handlers) &&
    requires(!StaticHandlerConfig::enabled)
  {
    if (handlers.empty()) {
      throw std::invalid_argument(
          "StaticPipelineBuilder: static handler list must not be empty");
    }
    using Config = detail::ErasedStaticHandlersAt<sizeof...(Entry)>;
    return StaticPipelineBuilder<
        HeadHandler,
        TailHandler,
        Allocator,
        StateTuple,
        Config,
        Overrides,
        Entry...>(
        detail::StaticPipelineBuilderRebindTag{},
        eventBase_,
        std::move(headSource_),
        std::move(tailSource_),
        std::move(allocatorSource_),
        std::move(factories_),
        std::move(stateTuple_),
        std::move(handlers),
        {});
  }

  auto addStaticSegments(std::vector<detail::ErasedStaticSegment> segments) &&
    requires(!StaticHandlerConfig::enabled)
  {
    if (segments.empty()) {
      throw std::invalid_argument(
          "StaticPipelineBuilder: static segment list must not be empty");
    }
    using Config = detail::ErasedStaticSegmentsAt<sizeof...(Entry)>;
    return StaticPipelineBuilder<
        HeadHandler,
        TailHandler,
        Allocator,
        StateTuple,
        Config,
        Overrides,
        Entry...>(
        detail::StaticPipelineBuilderRebindTag{},
        eventBase_,
        std::move(headSource_),
        std::move(tailSource_),
        std::move(allocatorSource_),
        std::move(factories_),
        std::move(stateTuple_),
        {},
        std::move(segments));
  }

  auto build() && {
    static_assert(
        detail::kOverriddenHandlersBuilt<typename Overrides::Tags, Entry...>,
        "each tag in Overrides::Tags must name a handler of the pipeline");
    validateRequired();
    validateHandlerIds();
    using Impl = detail::StaticPipelineImpl<
        HeadHandler,
        TailHandler,
        Allocator,
        StateTuple,
        StaticHandlerConfig,
        typename Entry::Spec...>;
    typename Impl::Ptr pipeline(new Impl(
        eventBase_,
        std::move(headSource_),
        std::move(tailSource_),
        std::move(allocatorSource_),
        std::move(stateTuple_),
        factories_,
        std::move(staticHandlers_),
        std::move(staticSegments_)));
    pipeline->finishBuild();
    return pipeline;
  }

 private:
  template <
      detail::StaticHandlerDirection Direction,
      typename H,
      HandlerId Id,
      typename... Args>
  auto addHandler(Args&&... args) && {
    if constexpr (detail::kOverridesHandler<typename Overrides::Tags, Id>) {
      return std::move(*this).template addOverride<Id>(
          std::forward<Args>(args)...);
    } else {
      static_assert(
          detail::kStaticHandlerIdUnique<Id, Entry...>,
          "static pipeline handler IDs must be unique");
      using NewEntry =
          detail::StaticHandlerFactory<H, Id, Direction, std::decay_t<Args>...>;
      return StaticPipelineBuilder<
          HeadHandler,
          TailHandler,
          Allocator,
          StateTuple,
          StaticHandlerConfig,
          Overrides,
          Entry...,
          NewEntry>(
          detail::StaticPipelineBuilderRebindTag{},
          eventBase_,
          std::move(headSource_),
          std::move(tailSource_),
          std::move(allocatorSource_),
          std::tuple_cat(
              std::move(factories_),
              std::tuple<NewEntry>(NewEntry(std::forward<Args>(args)...))),
          std::move(stateTuple_),
          std::move(staticHandlers_),
          std::move(staticSegments_));
    }
  }

  template <
      detail::StaticHandlerDirection Direction,
      template <typename> typename H,
      HandlerId Id,
      typename... Args>
  auto addHandlerTemplate(Args&&... args) && {
    if constexpr (detail::kOverridesHandler<typename Overrides::Tags, Id>) {
      return std::move(*this).template addOverride<Id>(
          std::forward<Args>(args)...);
    } else {
      static_assert(
          detail::kStaticHandlerIdUnique<Id, Entry...>,
          "static pipeline handler IDs must be unique");
      using NewEntry = detail::
          StaticHandlerTemplateFactory<H, Id, Direction, std::decay_t<Args>...>;
      return StaticPipelineBuilder<
          HeadHandler,
          TailHandler,
          Allocator,
          StateTuple,
          StaticHandlerConfig,
          Overrides,
          Entry...,
          NewEntry>(
          detail::StaticPipelineBuilderRebindTag{},
          eventBase_,
          std::move(headSource_),
          std::move(tailSource_),
          std::move(allocatorSource_),
          std::tuple_cat(
              std::move(factories_),
              std::tuple<NewEntry>(NewEntry(std::forward<Args>(args)...))),
          std::move(stateTuple_),
          std::move(staticHandlers_),
          std::move(staticSegments_));
    }
  }

  template <typename NewOverrides>
  auto rebindOverrides() && {
    return StaticPipelineBuilder<
        HeadHandler,
        TailHandler,
        Allocator,
        StateTuple,
        StaticHandlerConfig,
        NewOverrides,
        Entry...>(
        detail::StaticPipelineBuilderRebindTag{},
        eventBase_,
        std::move(headSource_),
        std::move(tailSource_),
        std::move(allocatorSource_),
        std::move(factories_),
        std::move(stateTuple_),
        std::move(staticHandlers_),
        std::move(staticSegments_));
  }

  // The override adds its handlers through a builder without overrides, so its
  // own addNext*() calls go through unchanged.
  template <HandlerId Id, typename... Args>
  auto addOverride(Args&&... args) && {
    return Overrides::add(
               std::move(*this).template rebindOverrides<NoHandlerOverrides>(),
               HandlerTag<Id>{},
               std::forward<Args>(args)...)
        .template rebindOverrides<Overrides>();
  }

  void validateRequired() const {
    if (eventBase_ == nullptr) {
      throw std::runtime_error("StaticPipelineBuilder: EventBase is required");
    }
    if (!headSource_.present()) {
      throw std::runtime_error("StaticPipelineBuilder: head is required");
    }
    if (!tailSource_.present()) {
      throw std::runtime_error("StaticPipelineBuilder: tail is required");
    }
    if (!allocatorSource_.present()) {
      throw std::runtime_error("StaticPipelineBuilder: allocator is required");
    }
  }

  void validateHandlerIds() const {
    std::vector<HandlerId> erasedIds;
    for (const auto& segment : staticSegments_) {
      for (std::size_t i = 0; i < segment.handlerCount(); ++i) {
        erasedIds.push_back(segment.handlerId(i));
      }
    }
    for (std::size_t i = 0; i < staticHandlers_.size(); ++i) {
      erasedIds.push_back(staticHandlers_[i].handlerId());
    }
    for (std::size_t i = 0; i < erasedIds.size(); ++i) {
      const auto id = erasedIds[i];
      if (((Entry::Spec::id == id) || ...)) {
        throw std::invalid_argument(
            "StaticPipelineBuilder: handler IDs must be unique");
      }
      for (std::size_t j = 0; j < i; ++j) {
        if (erasedIds[j] == id) {
          throw std::invalid_argument(
              "StaticPipelineBuilder: handler IDs must be unique");
        }
      }
    }
  }

  StaticPipelineBuilder(
      detail::StaticPipelineBuilderRebindTag,
      folly::EventBase* eventBase,
      detail::EndpointSource&& headSource,
      detail::EndpointSource&& tailSource,
      detail::EndpointSource&& allocatorSource,
      std::tuple<Entry...>&& factories,
      StateTuple&& stateTuple,
      std::vector<detail::ErasedStaticHandler>&& staticHandlers,
      std::vector<detail::ErasedStaticSegment>&& staticSegments) noexcept
      : eventBase_(eventBase),
        headSource_(std::move(headSource)),
        tailSource_(std::move(tailSource)),
        allocatorSource_(std::move(allocatorSource)),
        factories_(std::move(factories)),
        stateTuple_(std::move(stateTuple)),
        staticHandlers_(std::move(staticHandlers)),
        staticSegments_(std::move(staticSegments)) {}

  folly::EventBase* eventBase_{nullptr};
  detail::EndpointSource headSource_;
  detail::EndpointSource tailSource_;
  detail::EndpointSource allocatorSource_;
  std::tuple<Entry...> factories_;
  StateTuple stateTuple_;
  std::vector<detail::ErasedStaticHandler> staticHandlers_;
  std::vector<detail::ErasedStaticSegment> staticSegments_;
};

} // namespace apache::thrift::fast_thrift::channel_pipeline
