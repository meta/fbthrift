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
#include <stdexcept>
#include <utility>

#include <folly/ExceptionWrapper.h>
#include <folly/Utility.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Backpressure.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Event.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/ComposedFrame.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/QuantumFragmentationHandlerConfig.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/handler/FifoFrameFragmentationHandlerState.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/handler/FragmentCompletionTracker.h>

namespace apache::thrift::fast_thrift::frame::write::handler {

/**
 * Sends a fragmentable data frame downstream at once when its metadata plus
 * data fits in `streamQuantum` and its stream has nothing queued. Such a frame
 * does not wait for the fragments of other streams.
 *
 * Every other fragmentable data frame joins the queue behind the earlier
 * frames of its stream. Queued frames finish one at a time. The handler has at
 * most one partly sent frame.
 *
 * Queued frames wait for an `EventBase` callback. A callback takes as many
 * turns as there are live streams when it starts, and each turn sends one
 * fragment. The callback starts no new turn after its budget reaches zero. The
 * turn that crosses the budget still sends its whole fragment.
 * Backpressure does not stop a callback or delay the next one. After
 * downstream returns Backpressure, `onWrite()` returns Backpressure for each
 * accepted frame until write-ready. If downstream took one of those frames
 * without Backpressure, or the handler queued it, the handler asks for another
 * write-ready round. A writer above that already ran in the round can then
 * write.
 *
 * A frame that cannot be fragmented goes downstream at once. It can reach the
 * peer before a queued data frame of its stream. `CANCEL` and `ERROR` first
 * drop the queued frames of their stream and the rest of a partly sent frame.
 * If a downstream write fails during a callback, the handler drops every queued
 * frame and the rest of a partly sent frame. Then it calls `fireException()`.
 */
template <FragmentCompletionTracker Tracker = NoOpFragmentCompletionTracker>
class FifoFrameFragmentationHandlerT : private folly::NonCopyableNonMovable {
 public:
  channel_pipeline::WriteReadyHook writeReadyHook_;

  explicit FifoFrameFragmentationHandlerT(
      QuantumFragmentationHandlerConfig config = {}) noexcept
      : state_(config) {}

  template <typename Context>
  void handlerAdded(Context& ctx) noexcept {
    state_.attach(
        ctx.eventBase(),
        this,
        &ctx,
        +[](void* handler, void* context) noexcept {
          static_cast<FifoFrameFragmentationHandlerT*>(handler)
              ->flushAndPropagateErrors(*static_cast<Context*>(context));
        });
  }

  template <typename Context>
  void handlerRemoved(Context& ctx) noexcept {
    cancelAwaitWriteReady(ctx);
    state_.detach();
  }

  /**
   * Returns Error and drops a data frame if the config holds a bad value, if
   * the frame's metadata alone is above `kMaxStreamQuantum`, or if
   * FifoStreamQueue::enqueue refuses the frame. Logs the cause of the first
   * two, at most once a minute.
   */
  template <typename Context>
  [[nodiscard]] channel_pipeline::Result onWrite(
      Context& ctx, channel_pipeline::TypeErasedBox&& msg) noexcept {
    const ComposedFrame& frame = msg.get<ComposedFrame>();
    if (state_.prepareWrite(frame) ==
        FifoFrameFragmentationHandlerState::WriteDisposition::PassThrough) {
      const uint32_t streamId = frame.streamId;
      return passThrough(ctx, std::move(msg), streamId);
    }
    if (!state_.enqueue(msg.take<ComposedFrame>())) {
      return channel_pipeline::Result::Error;
    }
    return acceptedResult();
  }

  template <typename Context>
  void onWriteReady(Context& ctx) noexcept {
    const bool blockedWriterAbove = blockedWriterAbove_;
    cancelAwaitWriteReady(ctx);
    // A round wakes each handler once. A writer above may have run earlier in
    // this round and got Backpressure only from this handler. That writer needs
    // another round now. A writer that got Backpressure from downstream waits
    // for the next write-ready of downstream. If it got a round here too, a
    // writer that writes in every round would keep getting rounds while the
    // transport stays full.
    if (blockedWriterAbove) {
      ctx.pipeline()->onWriteReady();
    }
  }

  template <typename Context>
  void onPipelineInactive(Context& ctx) noexcept {
    cancelAwaitWriteReady(ctx);
    state_.clearPending();
  }

  [[nodiscard]] size_t pendingStreamCount() const noexcept {
    return state_.pendingStreamCount();
  }

  /**
   * Counts queue elements, one per queued stream whatever its frame count. A
   * stream that `CANCEL` or `ERROR` removed keeps its element until a later
   * callback turn reaches it. So the count can stay above zero while
   * `hasPendingWrites()` is false.
   */
  [[nodiscard]] size_t pendingQueueEntryCount() const noexcept {
    return state_.pendingQueueEntryCount();
  }

  [[nodiscard]] bool hasPendingStream(uint32_t streamId) const noexcept {
    return state_.hasPendingStream(streamId);
  }

  [[nodiscard]] bool hasPendingWrites() const noexcept {
    return state_.hasPendingWrites();
  }

  [[nodiscard]] bool isScheduled() const noexcept {
    return state_.isScheduled();
  }

  [[nodiscard]] bool isBackpressured() const noexcept { return backpressured_; }

  Tracker& tracker() noexcept { return tracker_; }

  using PublishedEvents = typename Tracker::PublishedEvents;
  using SubscribedEvents = typename Tracker::SubscribedEvents;

  template <channel_pipeline::PipelineEvent E, typename Context>
  void on(Context& ctx, const typename E::Payload& event) noexcept {
    tracker_.template on<E>(ctx, event, hasPendingWrites());
  }

 private:
  template <typename Context>
  void awaitWriteReady(Context& ctx) noexcept {
    backpressured_ = true;
    ctx.awaitWriteReady();
  }

  template <typename Context>
  void cancelAwaitWriteReady(Context& ctx) noexcept {
    if (backpressured_) {
      ctx.cancelAwaitWriteReady();
      backpressured_ = false;
    }
    blockedWriterAbove_ = false;
  }

  channel_pipeline::Result acceptedResult() noexcept {
    if (!backpressured_) {
      return channel_pipeline::Result::Success;
    }
    blockedWriterAbove_ = true;
    return channel_pipeline::Result::Backpressure;
  }

  template <typename Context>
  [[nodiscard]] channel_pipeline::Result passThrough(
      Context& ctx,
      channel_pipeline::TypeErasedBox&& msg,
      uint32_t streamId) noexcept {
    tracker_.onFragment(streamId, true);
    const channel_pipeline::Result result = ctx.fireWrite(std::move(msg));
    if (result == channel_pipeline::Result::Error) {
      tracker_.onFragmentAborted();
      return result;
    }
    if (result == channel_pipeline::Result::Backpressure) {
      awaitWriteReady(ctx);
      return result;
    }
    return acceptedResult();
  }

  template <typename Context>
  void flushAndPropagateErrors(Context& ctx) noexcept {
    if (flushOnePass(ctx) == channel_pipeline::Result::Error) {
      state_.stopAfterError();
      cancelAwaitWriteReady(ctx);
      ctx.fireException(
          folly::make_exception_wrapper<std::runtime_error>(
              "FifoFrameFragmentationHandler: downstream write failed"));
    } else if (state_.finishPass()) {
      tracker_.onFlush();
    }
  }

  template <typename Context>
  [[nodiscard]] channel_pipeline::Result flushOnePass(Context& ctx) noexcept {
    const size_t startingStreams = state_.startPass();
    for (size_t turn = 0; turn < startingStreams && state_.hasCallbackBudget();
         ++turn) {
      if (std::optional<FifoStreamQueue::ScheduledFragment> scheduled =
              state_.nextFragment();
          scheduled.has_value()) {
        tracker_.onFragment(
            scheduled->streamId, scheduled->fragment.currentFrameDone);
        const channel_pipeline::Result result = ctx.fireWrite(
            channel_pipeline::erase_and_box(
                std::move(scheduled->fragment.outFrame)));
        if (result == channel_pipeline::Result::Error) {
          tracker_.onFragmentAborted();
          return result;
        }
        if (result == channel_pipeline::Result::Backpressure) {
          awaitWriteReady(ctx);
        }
      }
    }
    return channel_pipeline::Result::Success;
  }

  bool backpressured_{false};
  // Set when onWrite() answers Backpressure for a frame that downstream took or
  // the handler queued. The writer above then waits for this handler alone.
  bool blockedWriterAbove_{false};
  [[no_unique_address]] Tracker tracker_{};
  FifoFrameFragmentationHandlerState state_;
};

using FifoFrameFragmentationHandler =
    FifoFrameFragmentationHandlerT<NoOpFragmentCompletionTracker>;

} // namespace apache::thrift::fast_thrift::frame::write::handler
