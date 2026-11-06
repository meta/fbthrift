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
#include <folly/ExceptionWrapper.h>
#include <folly/io/async/EventBase.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Event.h>

#include <cstddef>
#include <tuple>
#include <type_traits>
#include <utility>

namespace apache::thrift::fast_thrift::channel_pipeline::detail {

template <typename Tuple, typename T>
struct StaticTupleTypeCount;

template <typename T, typename... Ts>
struct StaticTupleTypeCount<std::tuple<Ts...>, T>
    : std::integral_constant<
          std::size_t,
          (std::size_t{0} + ... + std::same_as<T, Ts>)> {};

/**
 * Per-handler context whose pipeline and position are compile-time types.
 *
 * `IsEndpoint` marks the single context shared by the head and tail endpoints.
 * For an endpoint, `fireRead`/`fireWrite` mean "propagate past me": the tail's
 * `fireRead` routes to `forwardReadPastTail` and the head's `fireWrite` to
 * `forwardWritePastHead` (a bound peer, else `Result::Error`). An endpoint only
 * ever propagates in its own direction — the head never calls `fireRead`, the
 * tail never `fireWrite` — so one shared context serves both. An internal
 * handler (`IsEndpoint == false`) forwards to its neighbor as usual.
 */
template <
    typename Pipeline,
    std::size_t Index,
    HandlerId Id,
    typename StateTuple = std::tuple<>,
    bool IsEndpoint = false>
class StaticContext {
 public:
  explicit StaticContext(Pipeline* pipeline) noexcept : pipeline_(pipeline) {}

  static constexpr HandlerId handlerId() noexcept { return Id; }
  std::size_t handlerIndex() const noexcept {
    return pipeline_->template contextHandlerIndex<Index>();
  }

  void activate() noexcept {}

  FOLLY_ALWAYS_INLINE Result fireRead(TypeErasedBox&& msg) noexcept {
    if constexpr (IsEndpoint) {
      return pipeline_->forwardReadPastTail(std::move(msg));
    } else {
      return pipeline_->template fireReadFrom<Index + 1>(std::move(msg));
    }
  }

  FOLLY_ALWAYS_INLINE Result fireWrite(TypeErasedBox&& msg) noexcept {
    if constexpr (IsEndpoint) {
      return pipeline_->forwardWritePastHead(std::move(msg));
    } else {
      return pipeline_->template fireWriteFrom<Index>(std::move(msg));
    }
  }

  FOLLY_ALWAYS_INLINE void fireException(
      folly::exception_wrapper&& e) noexcept {
    pipeline_->template fireExceptionFrom<Index + 1>(std::move(e));
  }

  template <PipelineEvent E>
    requires std::is_void_v<typename E::Payload>
  void fireEvent() noexcept {
    pipeline_->template fireEvent<E>();
  }

  template <PipelineEvent E>
    requires(!std::is_void_v<typename E::Payload>)
  void fireEvent(const typename E::Payload& payload) noexcept {
    pipeline_->template fireEvent<E>(payload);
  }

  template <PipelineEvent E, std::size_t RouteIndex, typename... Args>
  void firePublishedEvent(Args&&... args) noexcept {
    pipeline_->template firePublishedEvent<Index, E, RouteIndex>(
        std::forward<Args>(args)...);
  }

  void deactivate() noexcept {
    pipeline_->template deactivateFromContext<Index>();
  }

  Pipeline* pipeline() const noexcept { return pipeline_; }

  BytesPtr allocate(std::size_t size) noexcept {
    return pipeline_->allocate(size);
  }

  BytesPtr copyBuffer(const void* data, std::size_t size) noexcept {
    return pipeline_->copyBuffer(data, size);
  }

  folly::EventBase* eventBase() const noexcept {
    return pipeline_->eventBase();
  }

  void close() noexcept { pipeline_->close(); }

  void awaitWriteReady() noexcept {
    pipeline_->template awaitWriteReady<Index>();
  }
  void cancelAwaitWriteReady() noexcept {
    pipeline_->template cancelAwaitWriteReady<Index>();
  }
  bool isAwaitingWriteReady() const noexcept {
    return pipeline_->template isAwaitingWriteReady<Index>();
  }

  void awaitReadReady() noexcept {
    pipeline_->template awaitReadReady<Index>();
  }
  void cancelAwaitReadReady() noexcept {
    pipeline_->template cancelAwaitReadReady<Index>();
  }
  bool isAwaitingReadReady() const noexcept {
    return pipeline_->template isAwaitingReadReady<Index>();
  }

  template <typename T>
    requires(StaticTupleTypeCount<StateTuple, T>::value == 1)
  T& state() noexcept {
    return pipeline_->template state<T>();
  }

  template <typename T>
    requires(StaticTupleTypeCount<StateTuple, T>::value == 1)
  const T& state() const noexcept {
    return std::as_const(*pipeline_).template state<T>();
  }

 private:
  Pipeline* pipeline_;
};

} // namespace apache::thrift::fast_thrift::channel_pipeline::detail
