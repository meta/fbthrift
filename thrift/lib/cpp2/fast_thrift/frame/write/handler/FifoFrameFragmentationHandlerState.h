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

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

#include <folly/Utility.h>

#include <thrift/lib/cpp2/fast_thrift/frame/write/ComposedFrame.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/FifoStreamQueue.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/QuantumFragmentationHandlerConfig.h>

namespace apache::thrift::fast_thrift::frame::write::handler {

/**
 * Keeps fragmentable frames for FIFO turns in later EventBase callbacks. Each
 * turn returns one fragment. A callback starts no new turn after its budget
 * reaches zero.
 */
class FifoFrameFragmentationHandlerState final
    : private folly::NonCopyableNonMovable {
 public:
  using ScheduledFragment = FifoStreamQueue::ScheduledFragment;

  static constexpr std::string_view kHandlerName =
      "FifoFrameFragmentationHandler";

  explicit FifoFrameFragmentationHandlerState(
      QuantumFragmentationHandlerConfig config = {}) noexcept;

  void clearPending() noexcept;
  /** After this call, no turn emits anything that was queued for the stream. */
  void removeStream(uint32_t streamId) noexcept;
  /**
   * Returns false and drops the frame if the config holds a bad value, if the
   * frame's metadata alone is above `kMaxStreamQuantum`, or if
   * FifoStreamQueue::enqueue refuses the frame. For a bad config or metadata
   * above the limit, logs the metadata size, the limit and whether the config
   * is valid, at most once a minute.
   */
  [[nodiscard]] bool enqueue(ComposedFrame&& frame);

  /** Refills the budget and gives the pass one turn per live stream. */
  void startPass() noexcept;
  /**
   * While the pass has turns and budget left, takes one fragment and subtracts
   * its payload from the budget. Returns nullopt when the pass is over or the
   * queue is empty.
   */
  [[nodiscard]] std::optional<ScheduledFragment> nextFragment() noexcept;
  /** Does nothing, because startPass() sets the turns and the budget. */
  void finishPass() noexcept {}

  [[nodiscard]] size_t streamQuantum() const noexcept { return streamQuantum_; }
  [[nodiscard]] bool configValid() const noexcept { return configValid_; }
  [[nodiscard]] size_t pendingStreamCount() const noexcept;
  [[nodiscard]] size_t pendingQueueEntryCount() const noexcept;
  [[nodiscard]] bool hasPendingStream(uint32_t streamId) const noexcept;
  [[nodiscard]] bool hasPendingWrites() const noexcept {
    return !streams_.empty();
  }
  /** Returns true while queued frames wait for a callback. */
  [[nodiscard]] bool needsFlushCallback() const noexcept {
    return hasPendingWrites();
  }

 private:
  const size_t streamQuantum_;
  const size_t callbackBudget_;
  const bool configValid_;
  size_t remainingCallbackBudget_{0};
  size_t remainingTurns_{0};
  FifoStreamQueue streams_;
};

} // namespace apache::thrift::fast_thrift::frame::write::handler
