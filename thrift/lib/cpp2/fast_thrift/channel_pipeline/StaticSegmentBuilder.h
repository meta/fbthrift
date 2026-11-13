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

#include <folly/CPortability.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Handler.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/ErasedStaticSegment.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>

namespace apache::thrift::fast_thrift::channel_pipeline {

namespace detail {

template <template <typename> typename H, typename... Args>
struct StaticSegmentEntry {
  HandlerId id;
  std::tuple<Args...> args;
};

template <typename Segment, std::size_t Index>
class StaticSegmentContext {
 public:
  explicit StaticSegmentContext(Segment* segment) noexcept
      : segment_(segment) {}

  HandlerId handlerId() const noexcept { return segment_->handlerId(Index); }
  std::size_t handlerIndex() const noexcept {
    return segment_->handlerIndex(Index);
  }
  void activate() noexcept {}

  FOLLY_ALWAYS_INLINE Result fireRead(TypeErasedBox&& msg) noexcept {
    return segment_->template fireReadFrom<Index + 1>(std::move(msg));
  }
  FOLLY_ALWAYS_INLINE Result fireWrite(TypeErasedBox&& msg) noexcept {
    return segment_->template fireWriteFrom<Index>(std::move(msg));
  }
  FOLLY_ALWAYS_INLINE void fireException(
      folly::exception_wrapper&& e) noexcept {
    segment_->template fireExceptionFrom<Index + 1>(std::move(e));
  }

  template <PipelineEvent E>
    requires std::is_void_v<typename E::Payload>
  void fireEvent() noexcept {
    segment_->template fireEvent<E>();
  }
  template <PipelineEvent E>
    requires(!std::is_void_v<typename E::Payload>)
  void fireEvent(const typename E::Payload& payload) noexcept {
    segment_->template fireEvent<E>(payload);
  }
  template <PipelineEvent E, std::size_t RouteIndex, typename... Args>
  void firePublishedEvent(Args&&... args) noexcept {
    static_cast<void>(RouteIndex);
    fireEvent<E>(std::forward<Args>(args)...);
  }

  void deactivate() noexcept { segment_->deactivate(Index); }
  PipelineRef pipeline() const noexcept { return segment_->pipeline(); }
  BytesPtr allocate(std::size_t size) noexcept {
    return segment_->allocate(size);
  }
  BytesPtr copyBuffer(const void* data, std::size_t size) noexcept {
    return segment_->copyBuffer(data, size);
  }
  folly::EventBase* eventBase() const noexcept { return segment_->eventBase(); }
  void close() noexcept { segment_->close(); }

  void awaitWriteReady() noexcept { segment_->awaitWriteReady(Index); }
  void cancelAwaitWriteReady() noexcept {
    segment_->cancelAwaitWriteReady(Index);
  }
  bool isAwaitingWriteReady() const noexcept {
    return segment_->isAwaitingWriteReady(Index);
  }
  void awaitReadReady() noexcept { segment_->awaitReadReady(Index); }
  void cancelAwaitReadReady() noexcept {
    segment_->cancelAwaitReadReady(Index);
  }
  bool isAwaitingReadReady() const noexcept {
    return segment_->isAwaitingReadReady(Index);
  }

 private:
  Segment* segment_;
};

template <typename Segment, std::size_t Index, typename Entry>
class StaticSegmentSlot;

template <
    typename Segment,
    std::size_t Index,
    template <typename> typename H,
    typename... Args>
class StaticSegmentSlot<Segment, Index, StaticSegmentEntry<H, Args...>> {
 public:
  using Context = StaticSegmentContext<Segment, Index>;
  using Handler = H<Context>;
  using Entry = StaticSegmentEntry<H, Args...>;

  explicit StaticSegmentSlot(Segment* segment, const Entry& entry)
      : StaticSegmentSlot(segment, entry, std::index_sequence_for<Args...>{}) {}

  Handler& handler() noexcept { return handler_; }
  Context& context() noexcept { return context_; }

 private:
  template <std::size_t... ArgIndex>
  StaticSegmentSlot(
      Segment* segment, const Entry& entry, std::index_sequence<ArgIndex...>)
      : handler_(std::get<ArgIndex>(entry.args)...), context_(segment) {}

  [[no_unique_address]] Handler handler_;
  Context context_;
};

template <typename IndexSequence, typename IdStorage, typename... Entry>
class TypedStaticSegmentImpl;

template <std::size_t... Index, typename IdStorage, typename... Entry>
class TypedStaticSegmentImpl<std::index_sequence<Index...>, IdStorage, Entry...>
    : private StaticSegmentSlot<
          TypedStaticSegmentImpl<
              std::index_sequence<Index...>,
              IdStorage,
              Entry...>,
          Index,
          Entry>... {
  using Self = TypedStaticSegmentImpl<
      std::index_sequence<Index...>,
      IdStorage,
      Entry...>;
  static constexpr std::size_t kCount = sizeof...(Entry);

  template <std::size_t I>
  using EntryAt = std::tuple_element_t<I, std::tuple<Entry...>>;
  template <std::size_t I>
  using Slot = StaticSegmentSlot<Self, I, EntryAt<I>>;

 public:
  explicit TypedStaticSegmentImpl(
      const std::tuple<Entry...>& entries, IdStorage ids)
      : Slot<Index>(this, std::get<Index>(entries))..., ids_(std::move(ids)) {}

  static constexpr std::size_t handlerCount() noexcept { return kCount; }
  HandlerId handlerId(std::size_t index) const noexcept {
    if constexpr (
        std::is_pointer_v<IdStorage> ||
        requires(const IdStorage& ids) { *ids; }) {
      return (*ids_)[index];
    } else {
      return ids_[index];
    }
  }
  std::size_t handlerIndex(std::size_t index) const noexcept {
    return baseIndex_ + index;
  }

  void bindContext(
      void* pipeline,
      const StaticHandlerContextOps& ops,
      std::size_t baseIndex,
      std::size_t segmentIndex) noexcept {
    if (baseIndex > std::numeric_limits<std::uint32_t>::max() ||
        segmentIndex > std::numeric_limits<std::uint32_t>::max()) {
      std::terminate();
    }
    pipeline_ = pipeline;
    outerOps_ = &ops;
    baseIndex_ = static_cast<std::uint32_t>(baseIndex);
    segmentIndex_ = static_cast<std::uint32_t>(segmentIndex);
  }

  Result onReadFirst(TypeErasedBox&& msg) noexcept {
    return fireReadFrom<0>(std::move(msg));
  }
  FOLLY_NOINLINE Result
  onReadAt(std::size_t index, TypeErasedBox&& msg) noexcept {
    return dispatchReadAt<0>(index, std::move(msg));
  }
  Result onWriteLast(TypeErasedBox&& msg) noexcept {
    return fireWriteFrom<kCount>(std::move(msg));
  }
  FOLLY_NOINLINE Result
  onWriteAt(std::size_t index, TypeErasedBox&& msg) noexcept {
    return dispatchWriteAt<0>(index, std::move(msg));
  }
  void onExceptionFirst(folly::exception_wrapper&& e) noexcept {
    fireExceptionFrom<0>(std::move(e));
  }
  FOLLY_NOINLINE void onExceptionAt(
      std::size_t index, folly::exception_wrapper&& e) noexcept {
    dispatchExceptionAt<0>(index, std::move(e));
  }

  template <std::size_t I>
  FOLLY_ALWAYS_INLINE Result fireReadFrom(TypeErasedBox&& msg) noexcept {
    if constexpr (I == kCount) {
      return outerOps_->fireReadAfterSegment(
          pipeline_, segmentIndex_, std::move(msg));
    } else {
      auto& slot = get<I>();
      using H = typename Slot<I>::Handler;
      using Context = typename Slot<I>::Context;
      if constexpr (InboundHandler<H, Context>) {
        return slot.handler().onRead(slot.context(), std::move(msg));
      } else {
        return fireReadFrom<I + 1>(std::move(msg));
      }
    }
  }

  template <std::size_t Count>
  FOLLY_ALWAYS_INLINE Result fireWriteFrom(TypeErasedBox&& msg) noexcept {
    if constexpr (Count == 0) {
      return outerOps_->fireWriteBeforeSegment(
          pipeline_, segmentIndex_, std::move(msg));
    } else {
      auto& slot = get<Count - 1>();
      using H = typename Slot<Count - 1>::Handler;
      using Context = typename Slot<Count - 1>::Context;
      if constexpr (OutboundHandler<H, Context>) {
        return slot.handler().onWrite(slot.context(), std::move(msg));
      } else {
        return fireWriteFrom<Count - 1>(std::move(msg));
      }
    }
  }

  template <std::size_t I>
  FOLLY_ALWAYS_INLINE void fireExceptionFrom(
      folly::exception_wrapper&& e) noexcept {
    if constexpr (I == kCount) {
      outerOps_->fireExceptionAfterSegment(
          pipeline_, segmentIndex_, std::move(e));
    } else {
      auto& slot = get<I>();
      using H = typename Slot<I>::Handler;
      using Context = typename Slot<I>::Context;
      if constexpr (InboundHandler<H, Context>) {
        slot.handler().onException(slot.context(), std::move(e));
      } else {
        fireExceptionFrom<I + 1>(std::move(e));
      }
    }
  }

  template <PipelineEvent E>
  void fireEvent() noexcept {
    outerOps_->fireEvent(pipeline_, eventKey<E>(), nullptr);
  }
  template <PipelineEvent E>
  void fireEvent(const typename E::Payload& payload) noexcept {
    outerOps_->fireEvent(pipeline_, eventKey<E>(), &payload);
  }

  void deactivate(std::size_t index) noexcept {
    outerOps_->deactivate(pipeline_, baseIndex_ + index);
  }
  PipelineRef pipeline() const noexcept {
    return outerOps_->pipeline(pipeline_);
  }
  BytesPtr allocate(std::size_t size) noexcept {
    return outerOps_->allocate(pipeline_, size);
  }
  BytesPtr copyBuffer(const void* data, std::size_t size) noexcept {
    return outerOps_->copyBuffer(pipeline_, data, size);
  }
  folly::EventBase* eventBase() const noexcept {
    return outerOps_->eventBase(pipeline_);
  }
  void close() noexcept { outerOps_->close(pipeline_); }
  void awaitWriteReady(std::size_t index) noexcept {
    outerOps_->awaitWriteReady(pipeline_, baseIndex_ + index);
  }
  void cancelAwaitWriteReady(std::size_t index) noexcept {
    outerOps_->cancelAwaitWriteReady(pipeline_, baseIndex_ + index);
  }
  bool isAwaitingWriteReady(std::size_t index) const noexcept {
    return outerOps_->isAwaitingWriteReady(pipeline_, baseIndex_ + index);
  }
  void awaitReadReady(std::size_t index) noexcept {
    outerOps_->awaitReadReady(pipeline_, baseIndex_ + index);
  }
  void cancelAwaitReadReady(std::size_t index) noexcept {
    outerOps_->cancelAwaitReadReady(pipeline_, baseIndex_ + index);
  }
  bool isAwaitingReadReady(std::size_t index) const noexcept {
    return outerOps_->isAwaitingReadReady(pipeline_, baseIndex_ + index);
  }

  void onWriteReady(std::size_t index) noexcept {
    dispatchVoidAt<0>(index, []<typename S>(S& slot) {
      using H = typename S::Handler;
      using Context = typename S::Context;
      if constexpr (OutboundHandler<H, Context>) {
        slot.handler().onWriteReady(slot.context());
      }
    });
  }
  void onReadReady(std::size_t index) noexcept {
    dispatchVoidAt<0>(index, []<typename S>(S& slot) {
      using H = typename S::Handler;
      using Context = typename S::Context;
      if constexpr (InboundHandler<H, Context>) {
        slot.handler().onReadReady(slot.context());
      }
    });
  }
  void onPipelineActive(std::size_t index) noexcept {
    dispatchVoidAt<0>(index, [](auto& slot) {
      slot.handler().onPipelineActive(slot.context());
    });
  }
  void onPipelineInactive(std::size_t index) noexcept {
    dispatchVoidAt<0>(index, [](auto& slot) {
      slot.handler().onPipelineInactive(slot.context());
    });
  }
  void handlerAdded(std::size_t index) noexcept {
    dispatchVoidAt<0>(
        index, [](auto& slot) { slot.handler().handlerAdded(slot.context()); });
  }
  void handlerRemoved(std::size_t index) noexcept {
    dispatchVoidAt<0>(index, [](auto& slot) {
      slot.handler().handlerRemoved(slot.context());
    });
  }
  void fireEvent(
      std::size_t index, EventKey key, const void* payload) noexcept {
    dispatchVoidAt<0>(index, [key, payload]<typename S>(S& slot) {
      using H = typename S::Handler;
      if constexpr (requires { typename H::SubscribedEvents; }) {
        dispatchEvents(slot, key, payload, typename H::SubscribedEvents{});
      }
    });
  }
  bool handlesWriteReady(std::size_t index) noexcept {
    return handlesReadyAt<true, 0>(index);
  }
  bool handlesReadReady(std::size_t index) noexcept {
    return handlesReadyAt<false, 0>(index);
  }

 private:
  template <std::size_t I>
  Slot<I>& get() noexcept {
    return static_cast<Slot<I>&>(*this);
  }

  template <std::size_t I>
  FOLLY_NOINLINE Result
  dispatchReadAt(std::size_t index, TypeErasedBox&& msg) noexcept {
    if constexpr (I == kCount) {
      return Result::Error;
    } else if (index == I) {
      return fireReadFrom<I>(std::move(msg));
    } else {
      return dispatchReadAt<I + 1>(index, std::move(msg));
    }
  }
  template <std::size_t I>
  FOLLY_NOINLINE Result
  dispatchWriteAt(std::size_t index, TypeErasedBox&& msg) noexcept {
    if constexpr (I == kCount) {
      return Result::Error;
    } else if (index == I) {
      return fireWriteFrom<I + 1>(std::move(msg));
    } else {
      return dispatchWriteAt<I + 1>(index, std::move(msg));
    }
  }
  template <std::size_t I>
  FOLLY_NOINLINE void dispatchExceptionAt(
      std::size_t index, folly::exception_wrapper&& e) noexcept {
    if constexpr (I < kCount) {
      if (index == I) {
        fireExceptionFrom<I>(std::move(e));
      } else {
        dispatchExceptionAt<I + 1>(index, std::move(e));
      }
    }
  }

  template <std::size_t I, typename F>
  FOLLY_NOINLINE void dispatchVoidAt(std::size_t index, F&& fn) noexcept {
    if constexpr (I < kCount) {
      if (index == I) {
        fn(get<I>());
      } else {
        dispatchVoidAt<I + 1>(index, std::forward<F>(fn));
      }
    }
  }

  template <bool Write, std::size_t I>
  FOLLY_NOINLINE bool handlesReadyAt(std::size_t index) noexcept {
    if constexpr (I == kCount) {
      return false;
    } else if (index == I) {
      using H = typename Slot<I>::Handler;
      using Context = typename Slot<I>::Context;
      if constexpr (Write) {
        return OutboundHandler<H, Context>;
      } else {
        return InboundHandler<H, Context>;
      }
    } else {
      return handlesReadyAt<Write, I + 1>(index);
    }
  }

  template <typename S, PipelineEvent... E>
  static void dispatchEvents(
      S& slot, EventKey key, const void* payload, Events<E...>) noexcept {
    (dispatchEvent<E>(slot, key, payload), ...);
  }
  template <PipelineEvent E, typename S>
  static void dispatchEvent(
      S& slot, EventKey key, const void* payload) noexcept {
    if (key != eventKey<E>()) {
      return;
    }
    if constexpr (std::is_void_v<typename E::Payload>) {
      slot.handler().template on<E>(slot.context());
    } else {
      slot.handler().template on<E>(
          slot.context(), *static_cast<const typename E::Payload*>(payload));
    }
  }

  IdStorage ids_;
  void* pipeline_{nullptr};
  const StaticHandlerContextOps* outerOps_{nullptr};
  std::uint32_t baseIndex_{0};
  std::uint32_t segmentIndex_{0};
};

template <typename... Entry>
using TypedStaticSegment = TypedStaticSegmentImpl<
    std::index_sequence_for<Entry...>,
    std::shared_ptr<const std::array<HandlerId, sizeof...(Entry)>>,
    Entry...>;

template <typename... Entry>
using InlineTypedStaticSegment = TypedStaticSegmentImpl<
    std::index_sequence_for<Entry...>,
    std::array<HandlerId, sizeof...(Entry)>,
    Entry...>;

} // namespace detail

template <typename... Entry>
class StaticSegmentFactory {
 public:
  explicit StaticSegmentFactory(std::tuple<Entry...> entries)
      : entries_(std::move(entries)), ids_(makeIds()) {}

  detail::ErasedStaticSegment build() const {
    return detail::ErasedStaticSegment::make<
        detail::TypedStaticSegment<Entry...>>(entries_, ids_);
  }

 private:
  std::shared_ptr<const std::array<HandlerId, sizeof...(Entry)>> makeIds() {
    return std::apply(
        [](const auto&... entry) {
          return std::make_shared<
              const std::array<HandlerId, sizeof...(Entry)>>(
              std::array<HandlerId, sizeof...(Entry)>{entry.id...});
        },
        entries_);
  }

  std::tuple<Entry...> entries_;
  std::shared_ptr<const std::array<HandlerId, sizeof...(Entry)>> ids_;
};

template <typename... Entry>
class StaticSegmentBuilder {
 public:
  StaticSegmentBuilder() = default;

  template <template <typename> typename H, typename... Args>
  auto addHandler(HandlerId id, Args&&... args) && {
    using NewEntry = detail::StaticSegmentEntry<H, std::decay_t<Args>...>;
    return StaticSegmentBuilder<Entry..., NewEntry>(std::tuple_cat(
        std::move(entries_),
        std::tuple<NewEntry>(NewEntry{
            id,
            std::tuple<std::decay_t<Args>...>(std::forward<Args>(args)...)})));
  }

  StaticSegmentFactory<Entry...> seal() && {
    static_assert(sizeof...(Entry) != 0);
    return StaticSegmentFactory<Entry...>(std::move(entries_));
  }

  detail::ErasedStaticSegment build() && {
    return std::move(*this).seal().build();
  }

 private:
  template <typename...>
  friend class StaticSegmentBuilder;
  explicit StaticSegmentBuilder(std::tuple<Entry...>&& entries)
      : entries_(std::move(entries)) {}

  std::tuple<Entry...> entries_;
};

} // namespace apache::thrift::fast_thrift::channel_pipeline
