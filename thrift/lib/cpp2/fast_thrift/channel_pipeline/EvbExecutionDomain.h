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
#include <type_traits>
#include <utility>

#include <folly/Executor.h>
#include <folly/io/async/EventBase.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/FollyExecutorDomain.h>

namespace apache::thrift::fast_thrift::channel_pipeline {

// Runs pipeline work on one EventBase.
class EvbExecutionDomain final {
 public:
  // Retains the loop and destruction drain. Release all bindings during
  // shutdown; the caller controls when the loop starts and stops.
  explicit EvbExecutionDomain(folly::EventBase& eventBase) noexcept
      : eventBase_(folly::getKeepAliveToken(eventBase)) {}

  // True only when this EventBase is running on the calling thread.
  bool inRunningDomainThread() const noexcept {
    return eventBase_->inRunningEventBaseThread();
  }

  // Always queues work, including on the EventBase thread and while idle.
  // Invokes callbacks in happens-before submission order; concurrent
  // submissions without that relationship have unspecified order.
  template <typename Function>
    requires(
        std::invocable<std::decay_t<Function>&> &&
        std::constructible_from<std::decay_t<Function>, Function &&> &&
        std::constructible_from<folly::Func, Function &&>)
  void post(Function&& function) noexcept(
      std::is_nothrow_constructible_v<folly::Func, Function&&>) {
    eventBase_->runInEventBaseThreadAlwaysEnqueue(
        folly::Func(std::forward<Function>(function)));
  }

  folly::EventBase* getExecutor() const noexcept { return eventBase_.get(); }

  // Domains on the same EventBase share its serialization.
  friend bool operator==(
      const EvbExecutionDomain& lhs, const EvbExecutionDomain& rhs) noexcept {
    return lhs.eventBase_.get() == rhs.eventBase_.get();
  }

 private:
  folly::Executor::KeepAlive<folly::EventBase> eventBase_;
};

static_assert(FollyExecutorDomain<EvbExecutionDomain>);

} // namespace apache::thrift::fast_thrift::channel_pipeline
