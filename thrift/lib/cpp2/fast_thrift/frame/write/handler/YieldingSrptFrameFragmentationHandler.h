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

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Event.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/ComposedFrame.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/QuantumFragmentationHandlerConfig.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/handler/FragmentCompletionTracker.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/handler/YieldingSrptFrameFragmentationHandlerState.h>

namespace apache::thrift::fast_thrift::frame::write::handler {

/**
 * Sends a fragmentable data frame downstream at once when its metadata plus
 * data fits in `streamQuantum` and its stream has nothing deferred. Defers
 * other fragmentable data and sends it in bounded yielding SRPT passes.
 *
 * A pass gives at most K turns, where K is the number of streams with deferred
 * frames at pass start. Each turn sends one fragment of the pinned stream.
 * When no stream is pinned, the stream whose front frame has the fewest data
 * bytes becomes pinned. It stays pinned until the last fragment of that frame,
 * and then competes again with its next frame. Frames behind the front frame
 * do not count. So a new frame does not change when its stream is pinned, and
 * it does not extend a pin. A callback starts no new turn after its data
 * budget reaches zero, but the turn that crosses the budget completes.
 *
 * Backpressure does not stop a pass or delay the next one. After downstream
 * returns Backpressure, `onWrite()` returns Backpressure for each accepted
 * frame until write-ready.
 *
 * A frame deferred during a pass waits until the pass ends. It then goes
 * downstream at once if it fits in `streamQuantum` and its stream has nothing
 * deferred, and otherwise waits for the next pass. Frames that cannot fragment
 * pass through inline. `CANCEL` and `ERROR` first drop the deferred frames of
 * their stream.
 */
template <FragmentCompletionTracker Tracker = NoOpFragmentCompletionTracker>
class YieldingSrptFrameFragmentationHandlerT
    : private folly::NonCopyableNonMovable {
 public:
  explicit YieldingSrptFrameFragmentationHandlerT(
      QuantumFragmentationHandlerConfig config = {}) noexcept
      : state_(config) {}

  template <typename Context>
  void handlerAdded(Context& ctx) noexcept {
    state_.attach(
        ctx.eventBase(),
        this,
        &ctx,
        +[](void* handler, void* context) noexcept {
          static_cast<YieldingSrptFrameFragmentationHandlerT*>(handler)
              ->flushAndPropagateErrors(*static_cast<Context*>(context));
        });
  }

  template <typename Context>
  void handlerRemoved(Context& ctx) noexcept {
    cancelAwaitWriteReady(ctx);
    state_.detach();
  }

  /**
   * Returns Error and drops a data frame when the config is invalid or when
   * the frame's metadata alone is above `kMaxStreamQuantum`. Logs the cause,
   * at most once a minute.
   */
  template <typename Context>
  [[nodiscard]] channel_pipeline::Result onWrite(
      Context& ctx, channel_pipeline::TypeErasedBox&& msg) noexcept {
    const ComposedFrame& frame = msg.get<ComposedFrame>();
    if (state_.prepareWrite(frame) ==
        YieldingSrptFrameFragmentationHandlerState::WriteDisposition::
            PassThrough) {
      const uint32_t streamId = frame.streamId;
      return passThrough(ctx, std::move(msg), streamId);
    }
    if (!state_.enqueue(msg.take<ComposedFrame>())) {
      return channel_pipeline::Result::Error;
    }
    return this->backpressured_ ? channel_pipeline::Result::Backpressure
                                : channel_pipeline::Result::Success;
  }

  template <typename Context>
  void onWriteReady(Context& ctx) noexcept {
    cancelAwaitWriteReady(ctx);
  }

  template <typename Context>
  void onPipelineInactive(Context& ctx) noexcept {
    cancelAwaitWriteReady(ctx);
    state_.clearPending();
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

  [[nodiscard]] bool isBackpressured() const noexcept {
    return this->backpressured_;
  }

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
    this->backpressured_ = true;
    ctx.awaitWriteReady();
  }

  template <typename Context>
  void cancelAwaitWriteReady(Context& ctx) noexcept {
    if (this->backpressured_) {
      ctx.cancelAwaitWriteReady();
      this->backpressured_ = false;
    }
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
    return this->backpressured_ ? channel_pipeline::Result::Backpressure
                                : channel_pipeline::Result::Success;
  }

  template <typename Context>
  void flushAndPropagateErrors(Context& ctx) noexcept {
    if (flushOnePass(ctx) == channel_pipeline::Result::Error) {
      state_.stopAfterError();
      cancelAwaitWriteReady(ctx);
      ctx.fireException(
          folly::make_exception_wrapper<std::runtime_error>(
              "YieldingSrptFrameFragmentationHandler: downstream write "
              "failed"));
    } else if (state_.finishPass()) {
      tracker_.onFlush();
    }
  }

  template <typename Context>
  [[nodiscard]] channel_pipeline::Result flushOnePass(Context& ctx) noexcept {
    state_.startPass();
    while (std::optional<
               YieldingSrptFrameFragmentationHandlerState::ScheduledFragment>
               scheduled = state_.nextFragment()) {
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
    return channel_pipeline::Result::Success;
  }

  bool backpressured_{false};
  [[no_unique_address]] Tracker tracker_{};
  YieldingSrptFrameFragmentationHandlerState state_;
};

using YieldingSrptFrameFragmentationHandler =
    YieldingSrptFrameFragmentationHandlerT<NoOpFragmentCompletionTracker>;

} // namespace apache::thrift::fast_thrift::frame::write::handler
