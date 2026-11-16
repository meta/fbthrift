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

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>

#include <folly/ExceptionWrapper.h>
#include <folly/Utility.h>
#include <folly/io/async/EventBase.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Event.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/frame/FrameType.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/ComposedFrame.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/QuantumFragmentationHandlerConfig.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/handler/FragmentCompletionTracker.h>

namespace apache::thrift::fast_thrift::frame::write::handler {

/**
 * The write path of a fragmenter that sends large frames in pieces from later
 * EventBase callbacks.
 *
 * A data frame goes downstream at once if the config is valid, the frame fits
 * in `streamQuantum` and its stream has nothing deferred. `State` keeps every
 * other data frame. Frames that cannot be fragmented go downstream at once,
 * also ahead of deferred data of their stream.
 *
 * Each callback runs one pass of `State`, and Backpressure does not stop it:
 * - `startPass()`, then `nextFragment()` until it returns nullopt, then
 *   `finishPass()`. Then `needsFlushCallback()` must be true exactly when
 *   deferred work remains.
 * - A write error ends the pass at once. `clearPending()` runs instead of
 *   `finishPass()`, so it must also end the pass.
 * - Downstream code can call back into the handler during a pass, so `State`
 *   can change between two `nextFragment()` calls.
 */
template <typename State, FragmentCompletionTracker Tracker>
// folly::NonCopyableNonMovable deletes copy and move.
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
class QuantumFragmentationHandlerBase : private folly::EventBase::LoopCallback,
                                        private folly::NonCopyableNonMovable {
 public:
  template <typename Context>
  void handlerAdded(Context& ctx) noexcept {
    eventBase_ = ctx.eventBase();
    context_ = &ctx;
    flushTrampoline_ =
        +[](QuantumFragmentationHandlerBase& handler, void* context) noexcept {
          handler.flushAndPropagateErrors(*static_cast<Context*>(context));
        };
  }

  template <typename Context>
  void handlerRemoved(Context& ctx) noexcept {
    cancelAwaitWriteReady(ctx);
    clearPending();
    eventBase_ = nullptr;
    context_ = nullptr;
    flushTrampoline_ = nullptr;
  }

  /**
   * Returns Error and drops a data frame that `State::enqueue()` refuses.
   */
  template <typename Context>
  [[nodiscard]] channel_pipeline::Result onWrite(
      Context& ctx, channel_pipeline::TypeErasedBox&& msg) noexcept {
    const ComposedFrame& frame = msg.get<ComposedFrame>();
    if (prepareWrite(frame) == WriteDisposition::PassThrough) {
      const uint32_t streamId = frame.streamId;
      return passThrough(ctx, std::move(msg), streamId);
    }
    if (!state_.enqueue(msg.take<ComposedFrame>())) {
      return channel_pipeline::Result::Error;
    }
    scheduleFlushIfNeeded();
    return backpressured_ ? channel_pipeline::Result::Backpressure
                          : channel_pipeline::Result::Success;
  }

  template <typename Context>
  void onWriteReady(Context& ctx) noexcept {
    cancelAwaitWriteReady(ctx);
  }

  template <typename Context>
  void onPipelineInactive(Context& ctx) noexcept {
    cancelAwaitWriteReady(ctx);
    clearPending();
  }

  [[nodiscard]] bool hasPendingStream(uint32_t streamId) const noexcept {
    return state_.hasPendingStream(streamId);
  }

  [[nodiscard]] bool hasPendingWrites() const noexcept {
    return state_.hasPendingWrites();
  }

  [[nodiscard]] bool isScheduled() const noexcept { return isScheduled_; }

  [[nodiscard]] bool isBackpressured() const noexcept { return backpressured_; }

  Tracker& tracker() noexcept { return tracker_; }

  using PublishedEvents = typename Tracker::PublishedEvents;
  using SubscribedEvents = typename Tracker::SubscribedEvents;

  template <channel_pipeline::PipelineEvent E, typename Context>
  void on(Context& ctx, const typename E::Payload& event) noexcept {
    tracker_.template on<E>(ctx, event, hasPendingWrites());
  }

 protected:
  explicit QuantumFragmentationHandlerBase(
      QuantumFragmentationHandlerConfig config) noexcept
      : state_(config) {}

  ~QuantumFragmentationHandlerBase() override {
    cancelLoopCallbackIfScheduled();
  }

  [[nodiscard]] const State& state() const noexcept { return state_; }

 private:
  enum class WriteDisposition { PassThrough, Defer };

  using FlushTrampoline =
      void (*)(QuantumFragmentationHandlerBase&, void*) noexcept;

  [[nodiscard]] WriteDisposition prepareWrite(
      const ComposedFrame& frame) noexcept {
    if (isTerminalFrameType(frame.frameType)) {
      state_.removeStream(frame.streamId);
      return WriteDisposition::PassThrough;
    }
    if (!frame.canFragment()) {
      return WriteDisposition::PassThrough;
    }
    // With a bad config, enqueue() refuses every data frame.
    if (state_.configValid() && !state_.hasPendingStream(frame.streamId) &&
        fitsInStreamQuantum(frame, state_.streamQuantum())) {
      return WriteDisposition::PassThrough;
    }
    return WriteDisposition::Defer;
  }

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
    return backpressured_ ? channel_pipeline::Result::Backpressure
                          : channel_pipeline::Result::Success;
  }

  template <typename Context>
  void flushAndPropagateErrors(Context& ctx) noexcept {
    if (flushOnePass(ctx) == channel_pipeline::Result::Error) {
      clearPending();
      cancelAwaitWriteReady(ctx);
      ctx.fireException(
          folly::make_exception_wrapper<std::runtime_error>(
              std::string(State::kHandlerName) + ": downstream write failed"));
      return;
    }
    state_.finishPass();
    if (state_.needsFlushCallback()) {
      scheduleFlushIfNeeded();
    } else {
      tracker_.onFlush();
    }
  }

  template <typename Context>
  [[nodiscard]] channel_pipeline::Result flushOnePass(Context& ctx) noexcept {
    state_.startPass();
    while (std::optional<typename State::ScheduledFragment> scheduled =
               state_.nextFragment()) {
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

  void clearPending() noexcept {
    cancelLoopCallbackIfScheduled();
    state_.clearPending();
  }

  void runLoopCallback() noexcept override {
    isScheduled_ = false;
    if (flushTrampoline_ != nullptr) {
      flushTrampoline_(*this, context_);
    }
  }

  void scheduleFlushIfNeeded() noexcept {
    if (!isScheduled_ && eventBase_ != nullptr && state_.needsFlushCallback()) {
      eventBase_->runInLoop(this);
      isScheduled_ = true;
    }
  }

  void cancelLoopCallbackIfScheduled() noexcept {
    if (isScheduled_) {
      cancelLoopCallback();
      isScheduled_ = false;
    }
  }

  bool backpressured_{false};
  [[no_unique_address]] Tracker tracker_{};
  State state_;
  folly::EventBase* eventBase_{nullptr};
  void* context_{nullptr};
  FlushTrampoline flushTrampoline_{nullptr};
  bool isScheduled_{false};
};

} // namespace apache::thrift::fast_thrift::frame::write::handler
