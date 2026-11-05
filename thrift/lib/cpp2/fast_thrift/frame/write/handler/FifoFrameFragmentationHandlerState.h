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

#include <folly/Utility.h>
#include <folly/io/async/EventBase.h>

#include <thrift/lib/cpp2/fast_thrift/frame/write/ComposedFrame.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/FifoStreamQueue.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/QuantumFragmentationHandlerConfig.h>

namespace apache::thrift::fast_thrift::frame::write::handler {

/**
 * Keeps fragmentable frames for FIFO turns in later EventBase callbacks. Each
 * turn returns one fragment.
 */
// folly::NonCopyableNonMovable deletes copy and move.
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
class FifoFrameFragmentationHandlerState final
    : private folly::EventBase::LoopCallback,
      private folly::NonCopyableNonMovable {
 public:
  enum class WriteDisposition { PassThrough, Defer };

  using FlushTrampoline = void (*)(void*, void*) noexcept;

  explicit FifoFrameFragmentationHandlerState(
      QuantumFragmentationHandlerConfig config = {}) noexcept;
  ~FifoFrameFragmentationHandlerState() override;

  void attach(
      folly::EventBase* eventBase,
      void* handler,
      void* context,
      FlushTrampoline flushTrampoline) noexcept;
  void detach() noexcept;
  void clearPending() noexcept;

  /**
   * For `CANCEL` and `ERROR`, removes the queued frames of the stream first.
   * Returns `PassThrough` for a frame that cannot be fragmented. It also
   * returns `PassThrough` for a data frame whose metadata plus data fits in
   * `streamQuantum` while its stream has nothing queued. Returns `Defer` for
   * every other data frame, and for every data frame if the config holds a
   * bad value.
   */
  [[nodiscard]] WriteDisposition prepareWrite(
      const ComposedFrame& frame) noexcept;
  /**
   * Returns false and drops the frame if the config holds a bad value, if the
   * frame's metadata alone is above `kMaxStreamQuantum`, or if
   * FifoStreamQueue::enqueue refuses the frame. For a bad config or metadata
   * above the limit, logs the metadata size, the limit and whether the config
   * is valid, at most once a minute.
   */
  [[nodiscard]] bool enqueue(ComposedFrame&& frame);

  /** Returns the number of live streams at the start of this pass. */
  [[nodiscard]] size_t startPass() const noexcept;
  [[nodiscard]] std::optional<FifoStreamQueue::ScheduledFragment>
  nextFragment() noexcept;
  /**
   * Returns true if no live stream remains. Otherwise makes sure that another
   * callback is scheduled and returns false.
   */
  [[nodiscard]] bool finishPass() noexcept;
  void stopAfterError() noexcept;

  [[nodiscard]] size_t pendingStreamCount() const noexcept;
  [[nodiscard]] size_t pendingQueueEntryCount() const noexcept;
  [[nodiscard]] bool hasPendingStream(uint32_t streamId) const noexcept;
  [[nodiscard]] bool hasPendingWrites() const noexcept;
  [[nodiscard]] bool isScheduled() const noexcept;

 private:
  void runLoopCallback() noexcept override;
  void scheduleFlushIfNeeded() noexcept;
  void cancelLoopCallbackIfScheduled() noexcept;

  const size_t streamQuantum_;
  const bool configValid_;
  folly::EventBase* eventBase_{nullptr};
  void* handler_{nullptr};
  void* context_{nullptr};
  FlushTrampoline flushTrampoline_{nullptr};
  bool isScheduled_{false};
  FifoStreamQueue streams_;
};

} // namespace apache::thrift::fast_thrift::frame::write::handler
