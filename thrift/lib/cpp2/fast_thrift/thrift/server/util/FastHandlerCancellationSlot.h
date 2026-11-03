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

#include <atomic>
#include <cstdint>
#include <optional>

namespace apache::thrift::fast_thrift::thrift::detail {

enum class HandlerState : uint8_t {
  AwaitingDispatch,
  Running,
  Completed,
};

class LocalCancellationSlot {
 public:
  explicit LocalCancellationSlot(HandlerState state) noexcept : state_(state) {}

  bool tryComplete() noexcept {
    if (state_ == HandlerState::Completed) {
      return false;
    }
    state_ = HandlerState::Completed;
    return true;
  }

  void markHandlerStarted() noexcept {
    if (state_ == HandlerState::AwaitingDispatch) {
      state_ = HandlerState::Running;
    }
  }

  HandlerState state() const noexcept { return state_; }

 private:
  HandlerState state_;
};

class AtomicCancellationSlot {
 public:
  explicit AtomicCancellationSlot(HandlerState state) noexcept
      : state_(state) {}

  bool tryComplete() noexcept {
    auto state = state_.load(std::memory_order_acquire);
    while (state != HandlerState::Completed) {
      if (state_.compare_exchange_weak(
              state,
              HandlerState::Completed,
              std::memory_order_acq_rel,
              std::memory_order_acquire)) {
        return true;
      }
    }
    return false;
  }

  void markHandlerStarted() noexcept {
    auto expected = HandlerState::AwaitingDispatch;
    state_.compare_exchange_strong(
        expected,
        HandlerState::Running,
        std::memory_order_release,
        std::memory_order_relaxed);
  }

  HandlerState state() const noexcept {
    return state_.load(std::memory_order_acquire);
  }

 private:
  std::atomic<HandlerState> state_;
};

// The atomic slot is paid only when cancellation can race completion;
// otherwise sole callback ownership serializes state access.
class CancellationSlot {
 public:
  CancellationSlot(bool useAtomicSlot, HandlerState state) noexcept
      : local_(state) {
    if (useAtomicSlot) {
      atomic_.emplace(state);
    }
  }

  bool tryComplete() noexcept {
    return atomic_ ? atomic_->tryComplete() : local_.tryComplete();
  }

  void markHandlerStarted() noexcept {
    if (atomic_) {
      atomic_->markHandlerStarted();
    } else {
      local_.markHandlerStarted();
    }
  }

  HandlerState state() const noexcept {
    return atomic_ ? atomic_->state() : local_.state();
  }

 private:
  LocalCancellationSlot local_;
  std::optional<AtomicCancellationSlot> atomic_;
};

} // namespace apache::thrift::fast_thrift::thrift::detail
