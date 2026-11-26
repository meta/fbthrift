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
#include <cstddef>
#include <new>
#include <tuple>
#include <type_traits>
#include <utility>

#include <folly/CPortability.h>
#include <folly/lang/Assume.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/ContextImpl.h>

namespace apache::thrift::fast_thrift::channel_pipeline::detail {

/**
 * True iff type T appears in the std::tuple type `Tuple`.
 */
template <typename Tuple, typename T>
struct TupleHasType;

template <typename T, typename... Ts>
struct TupleHasType<std::tuple<Ts...>, T>
    : std::disjunction<std::is_same<T, Ts>...> {};

/** Adds compile-time-checked state and a native domain to a stable view. */
template <typename StateTuple, ExecutionDomain D = EvbExecutionDomain>
class TypedContext {
 public:
  using ExecutionDomainType = D;

  explicit TypedContext(ContextImpl& ctx) noexcept : ctx_(&ctx) {}

  operator ContextImpl&() const noexcept { return *ctx_; }

  ContextImpl& impl() const noexcept { return *ctx_; }

  FOLLY_ALWAYS_INLINE HandlerId handlerId() const noexcept {
    return ctx_->handlerId();
  }

  FOLLY_ALWAYS_INLINE void activate() const noexcept {
    return ctx_->activate();
  }

  FOLLY_ALWAYS_INLINE Result fireRead(TypeErasedBox&& msg) const noexcept {
    return ctx_->fireRead(std::move(msg));
  }

  FOLLY_ALWAYS_INLINE Result fireWrite(TypeErasedBox&& msg) const noexcept {
    return ctx_->fireWrite(std::move(msg));
  }

  FOLLY_ALWAYS_INLINE void fireException(
      folly::exception_wrapper&& e) const noexcept {
    return ctx_->fireException(std::move(e));
  }

  FOLLY_ALWAYS_INLINE void deactivate() const noexcept {
    return ctx_->deactivate();
  }

  FOLLY_ALWAYS_INLINE PipelineImpl* pipeline() const noexcept {
    return ctx_->pipeline();
  }

  FOLLY_ALWAYS_INLINE D& executionDomain() const noexcept {
    return ctx_->template executionDomain<D>();
  }

  FOLLY_ALWAYS_INLINE BytesPtr allocate(size_t size) const noexcept {
    return ctx_->allocate(size);
  }

  FOLLY_ALWAYS_INLINE BytesPtr
  copyBuffer(const void* data, size_t size) const noexcept {
    return ctx_->copyBuffer(data, size);
  }

  FOLLY_ALWAYS_INLINE BytesPtr
  takeOwnership(const OwnedBuffer& buffer) const noexcept {
    return ctx_->takeOwnership(buffer);
  }

  FOLLY_ALWAYS_INLINE folly::EventBase* eventBase() const noexcept
    requires std::same_as<D, EvbExecutionDomain>
  {
    return pipeline() ? executionDomain().getExecutor() : nullptr;
  }

  FOLLY_ALWAYS_INLINE void close() const noexcept { return ctx_->close(); }

  FOLLY_ALWAYS_INLINE void awaitWriteReady() const noexcept {
    return ctx_->awaitWriteReady();
  }

  FOLLY_ALWAYS_INLINE void cancelAwaitWriteReady() const noexcept {
    return ctx_->cancelAwaitWriteReady();
  }

  FOLLY_ALWAYS_INLINE bool isAwaitingWriteReady() const noexcept {
    return ctx_->isAwaitingWriteReady();
  }

  FOLLY_ALWAYS_INLINE void awaitReadReady() const noexcept {
    return ctx_->awaitReadReady();
  }

  FOLLY_ALWAYS_INLINE void cancelAwaitReadReady() const noexcept {
    return ctx_->cancelAwaitReadReady();
  }

  FOLLY_ALWAYS_INLINE bool isAwaitingReadReady() const noexcept {
    return ctx_->isAwaitingReadReady();
  }

  FOLLY_ALWAYS_INLINE size_t handlerIndex() const noexcept {
    return ctx_->handlerIndex();
  }

  template <PipelineEvent E, typename... Args>
    requires requires(ContextImpl& ctx, Args&&... args) {
      ctx.template fireEvent<E>(std::forward<Args>(args)...);
    }
  FOLLY_ALWAYS_INLINE void fireEvent(Args&&... args) const noexcept {
    ctx_->template fireEvent<E>(std::forward<Args>(args)...);
  }

  template <PipelineEvent E, std::size_t RouteIndex, typename... Args>
    requires requires(ContextImpl& ctx, Args&&... args) {
      ctx.template firePublishedEvent<E, RouteIndex>(
          std::forward<Args>(args)...);
    }
  FOLLY_ALWAYS_INLINE void firePublishedEvent(Args&&... args) const noexcept {
    ctx_->template firePublishedEvent<E, RouteIndex>(
        std::forward<Args>(args)...);
  }

  template <typename T>
    requires TupleHasType<StateTuple, T>::value
  FOLLY_ALWAYS_INLINE T& state() const noexcept {
    return std::get<T>(*static_cast<StateTuple*>(this->ctx_->pipelineState_));
  }

 private:
  ContextImpl* const ctx_;
};

template <typename StateTuple, ExecutionDomain D = EvbExecutionDomain>
using ContextFor = std::conditional_t<
    std::tuple_size_v<StateTuple> == 0 && std::same_as<D, EvbExecutionDomain>,
    ContextImpl,
    TypedContext<StateTuple, D>>;

template <typename StateTuple, ExecutionDomain D = EvbExecutionDomain>
FOLLY_ALWAYS_INLINE void initTypedView(ContextImpl& ctx) noexcept {
  using View = ContextFor<StateTuple, D>;
  if constexpr (!std::same_as<View, ContextImpl>) {
    static_assert(sizeof(View) == sizeof(void*));
    static_assert(alignof(View) <= alignof(void*));
    static_assert(std::is_standard_layout_v<View>);
    static_assert(std::is_trivially_destructible_v<View>);
    ::new (ctx.typedViewStorage()) View{ctx};
  }
}

template <typename StateTuple, ExecutionDomain D = EvbExecutionDomain>
FOLLY_ALWAYS_INLINE ContextFor<StateTuple, D>& contextFor(
    ContextImpl& ctx) noexcept {
  using View = ContextFor<StateTuple, D>;
  if constexpr (std::same_as<View, ContextImpl>) {
    return ctx;
  } else {
    auto& view = *std::launder(reinterpret_cast<View*>(ctx.typedViewStorage()));
    folly::assume(&view.impl() == &ctx);
    return view;
  }
}

} // namespace apache::thrift::fast_thrift::channel_pipeline::detail
