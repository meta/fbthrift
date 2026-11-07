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
#include <cstdint>
#include <deque>
#include <utility>

#include <glog/logging.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Event.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/transport/WriteCompletion.h>

namespace apache::thrift::fast_thrift::frame::write::handler {

/**
 * Defines completion hooks for fragmentation handlers.
 *
 * `onFragment()` supports two call orders. A handler that records after the
 * downstream write calls it only for a non-Error result. A handler that
 * records before the write allows an inline `BatchWriteComplete` to find the
 * record. If the write returns Error, it must call `onFragmentAborted()`
 * before another `onFragment()` call. `isLastFragment` is true when the
 * fragment completes its original frame.
 *
 * A handler calls `onFlush()` after a drain ends with no queued fragments.
 */
template <typename T>
concept FragmentCompletionTracker =
    channel_pipeline::kIsEventSet<typename T::PublishedEvents> &&
    channel_pipeline::kIsEventSet<typename T::SubscribedEvents> &&
    requires(T tracker, uint32_t streamId, bool isLast) {
      { tracker.onFragment(streamId, isLast) } noexcept;
      { tracker.onFragmentAborted() } noexcept;
      { tracker.onFlush() } noexcept;
    };

struct NoOpFragmentCompletionTracker {
  using PublishedEvents = channel_pipeline::Events<>;
  using SubscribedEvents = channel_pipeline::Events<>;

  void onFragment(uint32_t, bool) noexcept {}
  void onFragmentAborted() noexcept {}
  void onFlush() noexcept {}
};

static_assert(
    FragmentCompletionTracker<NoOpFragmentCompletionTracker>,
    "NoOpFragmentCompletionTracker must satisfy FragmentCompletionTracker concept");

template <typename T>
concept FrameWriteCompleteEventFactory =
    channel_pipeline::PipelineEvent<typename T::BatchWriteCompleteEventType> &&
    channel_pipeline::PipelineEvent<typename T::FrameWriteCompleteEventType> &&
    requires(
        typename T::BatchWriteCompleteEventType batchEvent,
        uint32_t streamId,
        bool quiesced) {
      {
        T::makeFrameWriteComplete(batchEvent.status, streamId, quiesced)
      } noexcept -> std::same_as<typename T::FrameWriteCompleteEventType>;
    };

/**
 * Maps downstream batch completions to original frames.
 *
 * `onFragment()` appends {streamId, isLastFragment} to a FIFO.
 * `onFragmentAborted()` removes the newest record, and does nothing when the
 * FIFO is empty.
 *
 * Each `BatchWriteComplete` pops `frameCount` records from the front and fires
 * FrameWriteComplete{streamId} for each record whose `isLastFragment` is true.
 *
 * Also narrows the quiescence the batch event carries. The batcher can only
 * speak for itself, and this handler sits above it holding frames the batcher
 * never saw, so egress is idle only if both are empty. Since quiescence
 * describes the connection rather than any one frame, it rides on the last
 * completion of the batch alone — otherwise a batch of N frames would report
 * the same edge N times.
 *
 * Templated on the pipeline's event factory (see
 * FrameWriteCompleteEventFactory). The factory must expose:
 *   - `using EventId = ...;` with `BatchWriteComplete` and
 *     `FrameWriteComplete` values.
 *   - `using BatchWriteCompleteEventType = ...;` — the message carried by
 *     the BatchWriteComplete event, with `status`, `frameCount`, `bytes`, and
 *     `quiesced`.
 *   - `static makeFrameWriteComplete(status, streamId, quiesced) noexcept;`
 *
 * EB-thread only — no synchronization.
 */
template <FrameWriteCompleteEventFactory EventFactory>
class FragmentCompletionTrackerT {
 public:
  using BatchEvent = typename EventFactory::BatchWriteCompleteEventType;
  using FrameEvent = typename EventFactory::FrameWriteCompleteEventType;
  using PublishedEvents = channel_pipeline::Events<FrameEvent>;
  using SubscribedEvents = channel_pipeline::Events<BatchEvent>;

  void onFragment(uint32_t streamId, bool isLastFragment) noexcept {
    fragments_.push_back({streamId, isLastFragment});
  }

  void onFragmentAborted() noexcept {
    if (!fragments_.empty()) {
      fragments_.pop_back();
    }
  }

  void onFlush() noexcept {}

  template <channel_pipeline::PipelineEvent E, typename Context>
    requires std::same_as<E, BatchEvent>
  void on(
      Context& ctx,
      const BatchEvent& evt,
      bool handlerHasPendingWrites) noexcept {
    size_t remaining = evt.frameCount;
    // Held back one completion so the last one — and only it — can carry the
    // quiescence edge; which frame is last is not known until the pop ends.
    bool deferred = false;
    uint32_t deferredStreamId = 0;
    while (remaining > 0 && !fragments_.empty()) {
      auto rec = fragments_.front();
      fragments_.pop_front();
      --remaining;
      if (!rec.isLastFragment) {
        continue;
      }
      if (deferred) {
        fire(ctx, evt.status, deferredStreamId, /*quiesced=*/false);
      }
      deferredStreamId = rec.streamId;
      deferred = true;
    }
    if (deferred) {
      // A leftover `remaining` means the FIFO drifted from the batch, so the
      // batch's quiescence verdict no longer describes what actually drained;
      // the edge is dropped rather than reported out of that state.
      fire(
          ctx,
          evt.status,
          deferredStreamId,
          evt.quiesced && !handlerHasPendingWrites && remaining == 0);
    }
    // frameCount must be matched by exactly that many tracked fragments. A
    // leftover count means the FIFO held fewer records than the batch reported,
    // so the tracker ran out mid-pop and silently dropped the completions for
    // this batch's remaining frames (FIFO/batch drift).
    DCHECK_EQ(remaining, 0u)
        << "FragmentCompletionTracker: BatchWriteComplete frameCount exceeds "
           "tracked fragments";
  }

 private:
  template <typename Context>
  void fire(
      Context& ctx,
      apache::thrift::fast_thrift::transport::WriteCompletionStatus status,
      uint32_t streamId,
      bool quiesced) noexcept {
    PublishedEvents::template fire<FrameEvent>(
        ctx, EventFactory::makeFrameWriteComplete(status, streamId, quiesced));
  }

  struct FragmentRecord {
    uint32_t streamId;
    bool isLastFragment;
  };
  std::deque<FragmentRecord> fragments_;
};

} // namespace apache::thrift::fast_thrift::frame::write::handler
