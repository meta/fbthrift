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

#include <algorithm>
#include <cstdint>
#include <functional>
#include <list>

#include <folly/io/async/EventBase.h>
#include <folly/io/async/EventBaseLocal.h>

namespace apache::thrift::fast_thrift::thrift {

/** Shared admission accounting and deferred capacity notifications on one
 * EventBase.
 */
class ConcurrencyControlEventBaseState final
    : private folly::EventBase::LoopCallback {
 public:
  using Subscribers = std::list<std::function<void()>>;

  explicit ConcurrencyControlEventBaseState(folly::EventBase& evb) noexcept
      : eventBase_(evb) {}
  ~ConcurrencyControlEventBaseState() override { cancelLoopCallback(); }

  ConcurrencyControlEventBaseState(const ConcurrencyControlEventBaseState&) =
      delete;
  ConcurrencyControlEventBaseState& operator=(
      const ConcurrencyControlEventBaseState&) = delete;
  ConcurrencyControlEventBaseState(ConcurrencyControlEventBaseState&&) = delete;
  ConcurrencyControlEventBaseState& operator=(
      ConcurrencyControlEventBaseState&&) = delete;

  uint64_t admitted{0};
  uint64_t released{0};
  uint64_t dispatched{0};
  uint64_t finished{0};

  uint64_t outstanding() const noexcept { return admitted - released; }
  uint64_t active() const noexcept { return dispatched - finished; }

  uint64_t maxActiveSeen{0};
  uint64_t maxOutstandingSeen{0};
  // Nodes are allocated once when a pipeline subscribes, then spliced between
  // these lists so changing waiting state does not allocate.
  Subscribers idle;
  Subscribers waiters;

  void notify() noexcept {
    if (!waiters.empty() && !isLoopCallbackScheduled()) {
      eventBase_.runInLoop(this);
    }
  }

 private:
  void runLoopCallback() noexcept override {
    const auto freeActive =
        maxActiveSeen > active() ? maxActiveSeen - active() : 0;
    const auto freeOutstanding = maxOutstandingSeen > outstanding()
        ? maxOutstandingSeen - outstanding()
        : 0;
    auto budget = std::min<uint64_t>(
        waiters.size(), std::max(freeActive, freeOutstanding));
    while (budget-- > 0 && !waiters.empty()) {
      // Dispatch may move or erase the subscriber's node.
      auto notifyHandler = waiters.front();
      notifyHandler();
    }
  }

  folly::EventBase& eventBase_;
};

/** Lives with the server/factory, longer than its connection pipelines. */
class ConcurrencyControlStateHolder {
 public:
  ConcurrencyControlEventBaseState& stateFor(folly::EventBase& evb) {
    return local_.try_emplace(evb, evb);
  }

 private:
  folly::EventBaseLocal<ConcurrencyControlEventBaseState> local_;
};

} // namespace apache::thrift::fast_thrift::thrift
