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
#include <deque>
#include <optional>
#include <string_view>

#include <folly/Utility.h>

#include <thrift/lib/cpp2/fast_thrift/frame/write/ComposedFrame.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/PerStreamState.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/QuantumFragmentationHandlerConfig.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/SrptHeap.h>

namespace apache::thrift::fast_thrift::frame::write::handler {

/** Keeps yielding SRPT work between EventBase callbacks. */
class YieldingSrptFrameFragmentationHandlerState final
    : private folly::NonCopyableNonMovable {
 public:
  struct ScheduledFragment {
    uint32_t streamId{0};
    PerStreamState::Fragment fragment;
  };

  static constexpr std::string_view kHandlerName =
      "YieldingSrptFrameFragmentationHandler";

  explicit YieldingSrptFrameFragmentationHandlerState(
      QuantumFragmentationHandlerConfig config = {}) noexcept;

  void clearPending() noexcept;
  /**
   * Drops the deferred frames of the stream and the rest of its pinned frame.
   */
  void removeStream(uint32_t streamId) noexcept;
  /**
   * Returns false and drops the frame when the config holds a bad value or
   * when its metadata alone is above `kMaxStreamQuantum`. Logs the metadata
   * size, the limit and whether the config is valid, at most once a minute.
   */
  [[nodiscard]] bool enqueue(ComposedFrame&& frame);

  /** Starts one pass and snapshots its number of turns. */
  void startPass() noexcept;
  /**
   * While the pass has turns and budget left, returns the next fragment of the
   * pinned stream. After that, it takes the frames that arrived during the
   * pass in arrival order. It returns each one that fits in `streamQuantum`
   * while its stream has nothing queued, and queues the rest for the next
   * pass. Returns nullopt when the pass is over. A write that fails inside the
   * pass can close the pipeline, and `clearPending()` then ends the pass early.
   */
  [[nodiscard]] std::optional<ScheduledFragment> nextFragment() noexcept;
  /** Ends the pass. A frame that arrives after this call is queued at once. */
  void finishPass() noexcept;

  [[nodiscard]] size_t streamQuantum() const noexcept { return streamQuantum_; }
  [[nodiscard]] bool configValid() const noexcept { return configValid_; }
  [[nodiscard]] bool hasPendingStream(uint32_t streamId) const noexcept;
  [[nodiscard]] bool hasPendingWrites() const noexcept;
  /**
   * Returns true while deferred work waits for a callback. A frame that
   * arrives during a pass waits for the end of that pass instead, so this
   * returns false during a pass.
   */
  [[nodiscard]] bool needsFlushCallback() const noexcept {
    return !passActive_ && hasPendingWrites();
  }

 private:
  // A stream in the heap has sent no fragment of its front frame, because a
  // pin lasts until the last fragment of that frame. So the front frame's
  // `dataSize` is all its unsent data.
  struct PriorityFn {
    size_t operator()(const PerStreamState& stream) const noexcept {
      return stream.frames.front().dataSize;
    }
  };

  [[nodiscard]] bool hasQueuedStream(uint32_t streamId) const noexcept;
  [[nodiscard]] std::optional<ScheduledFragment> takeDeferredArrival();
  void enqueueNow(ComposedFrame&& frame);
  [[nodiscard]] ScheduledFragment takePinnedFragment() noexcept;
  [[nodiscard]] PerStreamState::Fragment takeFragment(
      PerStreamState& stream) noexcept;
  void spendCallbackBudget(size_t dataBytes) noexcept;

  const size_t streamQuantum_;
  const size_t callbackBudget_;
  const bool configValid_;
  bool passActive_{false};
  size_t remainingCallbackBudget_{0};
  size_t remainingTurns_{0};
  SrptHeap<PerStreamState, PriorityFn> streams_;
  std::optional<PerStreamState> pinned_;
  std::deque<ComposedFrame> deferredArrivals_;
};

} // namespace apache::thrift::fast_thrift::frame::write::handler
