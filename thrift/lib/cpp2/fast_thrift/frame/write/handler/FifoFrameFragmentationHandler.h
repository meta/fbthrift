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

#include <thrift/lib/cpp2/fast_thrift/frame/write/QuantumFragmentationHandlerConfig.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/handler/FifoFrameFragmentationHandlerState.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/handler/FragmentCompletionTracker.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/handler/QuantumFragmentationHandlerBase.h>

namespace apache::thrift::fast_thrift::frame::write::handler {

/**
 * Sends deferred data frames in FIFO order. A deferred frame joins the queue
 * behind the earlier frames of its stream. Queued frames finish one at a time,
 * so the handler has at most one partly sent frame.
 *
 * A callback takes as many turns as there are live streams when it starts, and
 * each turn sends one fragment. The callback starts no new turn after its
 * budget reaches zero. The turn that crosses the budget still sends its whole
 * fragment.
 *
 * `CANCEL` and `ERROR` first drop the queued frames of their stream and the
 * rest of a partly sent frame. If a downstream write fails during a callback,
 * the handler drops every queued frame and the rest of a partly sent frame.
 * Then it calls `fireException()`.
 *
 * QuantumFragmentationHandlerBase decides which frames go downstream at once,
 * and handles write-ready, write errors and the completion tracker.
 */
template <FragmentCompletionTracker Tracker = NoOpFragmentCompletionTracker>
class FifoFrameFragmentationHandlerT : public QuantumFragmentationHandlerBase<
                                           FifoFrameFragmentationHandlerState,
                                           Tracker> {
 public:
  explicit FifoFrameFragmentationHandlerT(
      QuantumFragmentationHandlerConfig config = {}) noexcept
      : QuantumFragmentationHandlerBase<
            FifoFrameFragmentationHandlerState,
            Tracker>(config) {}

  [[nodiscard]] size_t pendingStreamCount() const noexcept {
    return this->state().pendingStreamCount();
  }

  /**
   * Counts queue elements, one per queued stream whatever its frame count. A
   * stream that `CANCEL` or `ERROR` removed keeps its element until a later
   * callback turn reaches it. So the count can stay above zero while
   * `hasPendingWrites()` is false.
   */
  [[nodiscard]] size_t pendingQueueEntryCount() const noexcept {
    return this->state().pendingQueueEntryCount();
  }
};

using FifoFrameFragmentationHandler =
    FifoFrameFragmentationHandlerT<NoOpFragmentCompletionTracker>;

} // namespace apache::thrift::fast_thrift::frame::write::handler
