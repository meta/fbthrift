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

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/StaticContext.h>

#include <folly/CppAttributes.h>
#include <folly/IntrusiveList.h>

#include <concepts>
#include <cstddef>
#include <tuple>
#include <type_traits>
#include <utility>

namespace apache::thrift::fast_thrift::channel_pipeline::detail {

struct StaticReadyHook {
  folly::IntrusiveListHook hook;
  std::size_t handlerIndex{0};
  std::size_t lastNotifiedGeneration{0};
};

/**
 * Waiters retain registration order. Each hook is notified at most once per
 * pass, including when a callback cancels and re-arms it. Hooks live in the
 * handler slots and must be unlinked before the slots are destroyed.
 */
class IntrusiveReadyWaiters {
 public:
  void arm(StaticReadyHook& hook) noexcept {
    if (!isArmed(hook)) {
      waiters_.push_back(hook);
    }
  }
  void cancel(StaticReadyHook& hook) noexcept {
    if (isArmed(hook)) {
      hook.hook.unlink();
    }
  }
  bool isArmed(const StaticReadyHook& hook) const noexcept {
    return hook.hook.is_linked();
  }
  bool empty() const noexcept { return waiters_.empty(); }
  void clear() noexcept { waiters_.clear(); }

  template <typename Notify, typename IsClosed>
  void notify(Notify&& notify, IsClosed&& isClosed) noexcept {
    const auto generation = ++generation_;
    while (!isClosed()) {
      StaticReadyHook* next = nullptr;
      for (auto& hook : waiters_) {
        if (hook.lastNotifiedGeneration != generation) {
          next = &hook;
          break;
        }
      }
      if (next == nullptr) {
        break;
      }
      next->lastNotifiedGeneration = generation;
      notify(next->handlerIndex);
    }
  }

 private:
  folly::IntrusiveList<StaticReadyHook, &StaticReadyHook::hook> waiters_;
  std::size_t generation_{0};
};

struct NoStaticReadyHook {};

template <typename Handler, bool Enabled>
class StaticHandlerReadyHooks {
 public:
  explicit StaticHandlerReadyHooks(std::size_t) noexcept {}
  StaticReadyHook* writeReadyHook() noexcept { return nullptr; }
  StaticReadyHook* readReadyHook() noexcept { return nullptr; }
};

template <typename Handler>
class StaticHandlerReadyHooks<Handler, true> {
 public:
  explicit StaticHandlerReadyHooks(std::size_t index) noexcept {
    if (auto* hook = writeReadyHook()) {
      hook->handlerIndex = index;
    }
    if (auto* hook = readReadyHook()) {
      hook->handlerIndex = index;
    }
  }
  StaticReadyHook* writeReadyHook() noexcept {
    if constexpr (requires(Handler& h) { h.writeReadyHook_; }) {
      return &writeReadyHook_;
    }
    return nullptr;
  }
  StaticReadyHook* readReadyHook() noexcept {
    if constexpr (requires(Handler& h) { h.readReadyHook_; }) {
      return &readReadyHook_;
    }
    return nullptr;
  }

 private:
  [[FOLLY_ATTR_NO_UNIQUE_ADDRESS]] std::conditional_t<requires(Handler& h) {
    h.writeReadyHook_;
  }, StaticReadyHook, NoStaticReadyHook> writeReadyHook_;
  [[FOLLY_ATTR_NO_UNIQUE_ADDRESS]] std::conditional_t<requires(Handler& h) {
    h.readReadyHook_;
  }, StaticReadyHook, NoStaticReadyHook> readReadyHook_;
};

/** Inline handler and context storage for a compile-time-shaped pipeline. */
template <
    typename H,
    HandlerId Id,
    typename Pipeline,
    std::size_t Index,
    typename StateTuple = std::tuple<>>
class StaticHandler {
  static constexpr bool kUniformHandlers = [] {
    if constexpr (requires { Pipeline::kUniformHandlers; }) {
      return Pipeline::kUniformHandlers;
    } else {
      return false;
    }
  }();

 public:
  using Handler = H;
  using Context = StaticContext<Pipeline, Index, Id, StateTuple>;
  static constexpr HandlerId id = Id;

  template <typename... Args>
    requires std::constructible_from<H, Args...>
  explicit StaticHandler(Pipeline* pipeline, Args&&... args)
      : handler_(std::forward<Args>(args)...),
        context_(pipeline),
        readyHooks_(Index) {}

  H& handler() noexcept { return handler_; }
  const H& handler() const noexcept { return handler_; }
  Context& context() noexcept { return context_; }
  const Context& context() const noexcept { return context_; }

  StaticReadyHook* writeReadyHook() noexcept {
    return readyHooks_.writeReadyHook();
  }
  StaticReadyHook* readReadyHook() noexcept {
    return readyHooks_.readReadyHook();
  }

 private:
  [[FOLLY_ATTR_NO_UNIQUE_ADDRESS]] H handler_;
  Context context_;
  [[FOLLY_ATTR_NO_UNIQUE_ADDRESS]] StaticHandlerReadyHooks<
      H,
      kUniformHandlers && (requires(H& h) { h.writeReadyHook_; } ||
                           requires(H& h) { h.readReadyHook_; })> readyHooks_;
};

} // namespace apache::thrift::fast_thrift::channel_pipeline::detail
