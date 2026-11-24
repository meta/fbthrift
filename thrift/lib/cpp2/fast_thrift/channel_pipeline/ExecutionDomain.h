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
#include <utility>
#include <vector>

#include <folly/Function.h>

namespace apache::thrift::fast_thrift::channel_pipeline {

/**
 * A serialized execution context for a pipeline: at most one thread runs the
 * domain's work at a time, though that thread may change over time. Equal
 * domains share the same serialization. A thread-confined domain such as
 * InlineExecution serializes only within the calling thread and leaves
 * exclusion across threads to its caller.
 */
template <typename Domain>
concept ExecutionDomain =
    std::move_constructible<Domain> && std::equality_comparable<Domain> &&
    requires(Domain& domain, const Domain& constDomain, void (*task)()) {
      // True only while this domain is running work on the calling thread.
      // Idle domains return false.
      { constDomain.inRunningDomainThread() } noexcept -> std::same_as<bool>;

      // Accepts work from any thread and invokes callbacks in happens-before
      // submission order. Concurrent submissions without a happens-before
      // relationship have unspecified order. A callback never runs nested
      // inside another callback of the same domain; when the caller is not
      // running the domain's work, the domain may run it before post()
      // returns.
      { domain.post(std::move(task)) } -> std::same_as<void>;
    };

namespace detail {

using InlineExecutionQueue = std::vector<folly::Function<void()>>;

// Queue owned by the outermost InlineExecution callback running on this
// thread; null while no callback runs.
inline thread_local InlineExecutionQueue* activeInlineExecutionQueue = nullptr;

} // namespace detail

/**
 * Runs work on the caller's thread and serializes it only within that thread:
 * callers must ensure exclusive pipeline access across threads. A callback
 * posted while another runs on this thread is queued and runs after it
 * returns. Instances share the calling thread's queue, so they compare equal.
 * Callbacks must not throw.
 */
struct InlineExecution {
  bool inRunningDomainThread() const noexcept {
    return detail::activeInlineExecutionQueue != nullptr;
  }

  template <typename F>
  void post(F&& task) noexcept {
    auto*& active = detail::activeInlineExecutionQueue;
    if (active != nullptr) {
      active->emplace_back(
          [task = std::forward<F>(task)]() mutable { std::move(task)(); });
      return;
    }
    detail::InlineExecutionQueue queue;
    active = &queue;
    std::forward<F>(task)();
    for (std::size_t i = 0; i < queue.size(); ++i) {
      auto next = std::move(queue[i]);
      next();
    }
    active = nullptr;
  }

  friend bool operator==(const InlineExecution&, const InlineExecution&) =
      default;
};

static_assert(ExecutionDomain<InlineExecution>);

} // namespace apache::thrift::fast_thrift::channel_pipeline
