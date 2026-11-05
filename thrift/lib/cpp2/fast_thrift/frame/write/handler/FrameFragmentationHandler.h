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

/**
 * FrameFragmentationHandler - Outbound handler for HOL blocking mitigation.
 *
 * Uses SRPT (Shortest Remaining Processing Time) scheduling to minimize
 * mean stream completion latency. Streams with the fewest remaining bytes
 * are flushed first, so small responses (the common case — single-fragment
 * request-response) complete immediately rather than being blocked behind
 * bulk transfers.
 *
 * Operates on the flat `ComposedFrame`. Non-fragmentable frame types
 * (ERROR, CANCEL, REQUEST_N, KEEPALIVE, SETUP, METADATA_PUSH, EXT) bypass
 * the handler. Fragmentable types (PAYLOAD + REQUEST_*) are split
 * data-tail-only — all metadata stays with the first fragment.
 *
 * Each fragment carries at most `maxFragmentSize` data bytes, and no fragment
 * goes above `kMaxStreamQuantum` bytes. This limit keeps the frame length
 * within the three bytes that RSocket gives it. The first fragment counts its
 * metadata toward the limit. So a frame whose metadata plus data are above the
 * limit is split, even if its data alone fits in `maxFragmentSize`.
 * `onWrite()` returns Error and drops a data frame whose metadata alone is
 * above `kMaxStreamQuantum`. It logs the metadata size and the limit, at most
 * once a minute.
 *
 * See: FrameFragmentationHandler.md for design documentation.
 */

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Backpressure.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/frame/FrameType.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/ComposedFrame.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/FragmentationHandlerConfig.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/PerStreamState.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/QuantumFragmentationHandlerConfig.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/SrptHeap.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/handler/FragmentCompletionTracker.h>

#include <folly/ExceptionWrapper.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/EventBase.h>
#include <folly/logging/xlog.h>

#include <glog/logging.h>

#include <algorithm>
#include <deque>
#include <stdexcept>

namespace apache::thrift::fast_thrift::frame::write::handler {

/// KeyFn for SrptHeap: extracts remaining bytes from PerStreamState.
struct RemainingBytesFn {
  size_t operator()(const PerStreamState& s) const noexcept {
    return s.remaining();
  }
};

/**
 * Composable outbound handler for HOL mitigation. Consumes and emits
 * `ComposedFrame`.
 *
 * Templated on a `FragmentCompletionTracker`-satisfying type (see
 * FragmentCompletionTracker.h). The default `NoOpFragmentCompletionTracker`
 * makes the hook sites fully no-op and the compiler elides them. Pipelines
 * that need per-frame write completion fire through a concrete tracker that
 * maps fragments back to original frames.
 */
template <FragmentCompletionTracker Tracker = NoOpFragmentCompletionTracker>
class FrameFragmentationHandlerT : public folly::EventBase::LoopCallback {
 public:
  channel_pipeline::WriteReadyHook writeReadyHook_;

  explicit FrameFragmentationHandlerT(
      FragmentationHandlerConfig config = {}) noexcept
      : config_(config) {}

  ~FrameFragmentationHandlerT() override { cancelLoopCallbackIfScheduled(); }

  FrameFragmentationHandlerT(const FrameFragmentationHandlerT&) = delete;
  FrameFragmentationHandlerT& operator=(const FrameFragmentationHandlerT&) =
      delete;
  FrameFragmentationHandlerT(FrameFragmentationHandlerT&&) = delete;
  FrameFragmentationHandlerT& operator=(FrameFragmentationHandlerT&&) = delete;

  template <typename Context>
  void handlerAdded(Context& ctx) noexcept {
    eventBase_ = ctx.eventBase();
    ctxPtr_ = &ctx;
    // Type-erase the templated doFlush via a context-typed trampoline.
    // No std::function allocation; pure function pointer indirection.
    flushTrampoline_ = +[](FrameFragmentationHandlerT* self, void* c) noexcept {
      self->flushAndPropagateErrors(*static_cast<Context*>(c));
    };
  }

  template <typename Context>
  void handlerRemoved(Context& /*ctx*/) noexcept {
    cancelLoopCallbackIfScheduled();
    immediateQueue_.clear();
    streams_.clear();
    resetPending();
    blockedWriterAbove_ = false;
    eventBase_ = nullptr;
    ctxPtr_ = nullptr;
    flushTrampoline_ = nullptr;
  }

  template <typename Context>
  [[nodiscard]] apache::thrift::fast_thrift::channel_pipeline::Result onWrite(
      Context& ctx,
      apache::thrift::fast_thrift::channel_pipeline::TypeErasedBox&&
          msg) noexcept {
    auto& frame = msg.get<ComposedFrame>();
    const FrameType frameType = frame.frameType;
    const uint32_t streamId = frame.streamId;

    // Terminal frames: drop any queued fragments / immediate-queue entries
    // for this stream, then forward immediately.
    if (isTerminalFrameType(frameType)) {
      cancelStream(streamId);
      return passThrough(ctx, std::move(msg), streamId);
    }

    // Non-fragmentable types (KEEPALIVE, SETUP, ...) bypass the handler.
    if (!frame.canFragment()) {
      return passThrough(ctx, std::move(msg), streamId);
    }

    if (!metadataFitsInMaxStreamQuantum(frame)) {
      XLOGF_EVERY_MS(
          ERR,
          60'000,
          "FrameFragmentationHandler refused a data frame with {} metadata "
          "bytes. The limit is {} bytes.",
          frame.metadata->computeChainDataLength(),
          kMaxStreamQuantum);
      return channel_pipeline::Result::Error;
    }

    const size_t dataSize =
        frame.data ? frame.data->computeChainDataLength() : 0;
    const bool streamHasPending = streams_.contains(streamId);
    const bool needsFragmentation = dataSize > firstFragmentDataLimit(frame);

    // Slow path: needs fragmentation, OR same-stream frames are already
    // pending (ordering must be preserved on the wire for that stream).
    if (needsFragmentation || streamHasPending) {
      addToStreamQueue(streamId, std::move(msg));
    } else if (streams_.empty() || dataSize <= config_.minSizeToFragment) {
      // Fast path: no per-stream ordering risk and either nothing else is
      // pending or this frame is tiny enough to bypass.
      return passThrough(ctx, std::move(msg), streamId);
    } else {
      // Other streams have pending fragments; preserve cross-stream batching
      // semantics by queueing behind them.
      immediateQueue_.push_back(std::move(msg));
    }
    if (backpressured_) {
      blockedWriterAbove_ = true;
    }

    incrPending(dataSize, 1);

    if (pendingBytes_ > config_.maxPendingBytes ||
        pendingFrames_ > config_.maxPendingFrames) {
      return flushNow(ctx);
    }

    scheduleFlushIfNeeded();
    if (backpressured_) {
      return channel_pipeline::Result::Backpressure;
    }
    return channel_pipeline::Result::Success;
  }

  template <typename Context>
  void onPipelineInactive(Context& ctx) noexcept {
    cancelLoopCallbackIfScheduled();
    immediateQueue_.clear();
    streams_.clear();
    resetPending();
    cancelAwaitWriteReady(ctx);
  }

  template <typename Context>
  void onWriteReady(Context& ctx) noexcept {
    ctx.cancelAwaitWriteReady();
    backpressured_ = false;
    flushAndPropagateErrors(ctx);
    // A round wakes each handler once. A writer above may have run earlier in
    // this round and got Backpressure only from this handler. That writer needs
    // another round now. A writer that got Backpressure from downstream waits
    // for the next write-ready of downstream. If it got a round here too, a
    // writer that writes in every round would keep getting rounds while the
    // transport stays full. If the flush blocked this handler again, the round
    // that clears that block asks instead.
    if (!backpressured_ && blockedWriterAbove_) {
      blockedWriterAbove_ = false;
      ctx.pipeline()->onWriteReady();
    }
  }

  void runLoopCallback() noexcept override {
    isScheduled_ = false;
    if (flushTrampoline_) {
      flushTrampoline_(this, ctxPtr_);
    }
  }

  size_t pendingStreamCount() const noexcept { return streams_.size(); }
  size_t immediateQueueSize() const noexcept { return immediateQueue_.size(); }
  size_t pendingBytes() const noexcept { return pendingBytes_; }
  size_t pendingFrames() const noexcept { return pendingFrames_; }
  bool isScheduled() const noexcept { return isScheduled_; }

  bool hasPendingStream(uint32_t streamId) const noexcept {
    return streams_.contains(streamId);
  }

  // Whether this handler is still holding frames it has not handed downstream.
  // The layer below cannot see these, so anything it concludes about the
  // connection being caught up has to be qualified by this.
  bool hasPendingWrites() const noexcept {
    return !streams_.empty() || !immediateQueue_.empty();
  }

  Tracker& tracker() noexcept { return tracker_; }

  using PublishedEvents = typename Tracker::PublishedEvents;
  using SubscribedEvents = typename Tracker::SubscribedEvents;

  template <channel_pipeline::PipelineEvent E, typename Context>
  void on(Context& ctx, const typename E::Payload& event) noexcept {
    tracker_.template on<E>(ctx, event, hasPendingWrites());
  }

 private:
  /**
   * Forward a frame that bypasses fragmentation, recording it as a completed
   * single-fragment frame.
   *
   * The tracker's FIFO is popped against the downstream batch frame count, and
   * the batcher counts every frame it receives — bypassed ones included. A
   * bypassed frame that went unrecorded would leave the FIFO permanently short
   * of the counts, misattributing every later completion. The record comes
   * before fireWrite(), as in doFlush, because the batch can complete inside
   * fireWrite() and that completion must find it. A frame that downstream
   * refuses with Error never reaches the batcher, so its record is removed.
   */
  template <typename Context>
  [[nodiscard]] apache::thrift::fast_thrift::channel_pipeline::Result
  passThrough(
      Context& ctx,
      apache::thrift::fast_thrift::channel_pipeline::TypeErasedBox&& msg,
      uint32_t streamId) noexcept {
    tracker_.onFragment(streamId, /*isLastFragment=*/true);
    auto result = ctx.fireWrite(std::move(msg));
    if (result ==
        apache::thrift::fast_thrift::channel_pipeline::Result::Error) {
      tracker_.onFragmentAborted();
    }
    if (result == channel_pipeline::Result::Backpressure) {
      backpressured_ = true;
      ctx.awaitWriteReady();
    } else if (result == channel_pipeline::Result::Success && backpressured_) {
      blockedWriterAbove_ = true;
    }
    if (result != channel_pipeline::Result::Error && backpressured_) {
      return channel_pipeline::Result::Backpressure;
    }
    return result;
  }

  void addToStreamQueue(
      uint32_t streamId,
      apache::thrift::fast_thrift::channel_pipeline::TypeErasedBox&&
          msg) noexcept {
    auto frame = msg.take<ComposedFrame>();
    auto data = std::move(frame.data);
    const bool originalComplete = frame.isComplete();

    if (auto* existing = streams_.find(streamId)) {
      existing->enqueue(std::move(frame), std::move(data), originalComplete);
      streams_.update(streamId);
    } else {
      PerStreamState state;
      state.streamId = streamId;
      state.enqueue(std::move(frame), std::move(data), originalComplete);
      streams_.insert(streamId, std::move(state));
    }
  }

  // A fragment's metadata plus data stay within kMaxStreamQuantum. Only the
  // first fragment of a frame carries metadata. onWrite() refuses metadata
  // above kMaxStreamQuantum, so the subtraction does not wrap.
  size_t fragmentDataLimit(size_t metadataSize) const noexcept {
    return std::min(config_.maxFragmentSize, kMaxStreamQuantum - metadataSize);
  }

  size_t firstFragmentDataLimit(const ComposedFrame& frame) const noexcept {
    return fragmentDataLimit(
        frame.metadata ? frame.metadata->computeChainDataLength() : 0);
  }

  void scheduleFlushIfNeeded() noexcept {
    if (!isScheduled_ && eventBase_) {
      eventBase_->runInLoop(this);
      isScheduled_ = true;
    }
  }

  void cancelLoopCallbackIfScheduled() noexcept {
    if (isScheduled_) {
      this->cancelLoopCallback();
      isScheduled_ = false;
    }
  }

  /**
   * Cancel a stream: drop all queued fragments AND any immediate-queue
   * entries for that stream, so a CANCEL/ERROR going out is not followed
   * by stale data frames for the same stream.
   */
  void cancelStream(uint32_t streamId) noexcept {
    if (auto* state = streams_.find(streamId)) {
      decrPending(state->totalRemaining, state->frames.size());
      streams_.erase(streamId);
    }
    for (auto it = immediateQueue_.begin(); it != immediateQueue_.end();) {
      auto& f = it->get<ComposedFrame>();
      if (f.streamId == streamId) {
        const size_t sz = f.data ? f.data->computeChainDataLength() : 0;
        decrPending(sz, 1);
        it = immediateQueue_.erase(it);
      } else {
        ++it;
      }
    }
  }

  // Pending-counter mutators. All updates to pendingBytes_ / pendingFrames_
  // funnel through these so the bookkeeping has a single audit point.
  // DCHECKs trip on underflow / lost-tracking bugs in debug builds.
  void incrPending(size_t bytes, size_t frames) noexcept {
    pendingBytes_ += bytes;
    pendingFrames_ += frames;
  }

  void decrPending(size_t bytes, size_t frames) noexcept {
    DCHECK_GE(pendingBytes_, bytes);
    DCHECK_GE(pendingFrames_, frames);
    pendingBytes_ -= bytes;
    pendingFrames_ -= frames;
  }

  void resetPending() noexcept {
    // Reset is only valid once the in-flight queues have been emptied;
    // otherwise we'd be silently discarding tracked work.
    DCHECK(immediateQueue_.empty());
    DCHECK(streams_.empty());
    pendingBytes_ = 0;
    pendingFrames_ = 0;
  }

  template <typename Context>
  apache::thrift::fast_thrift::channel_pipeline::Result flushNow(
      Context& ctx) noexcept {
    cancelLoopCallbackIfScheduled();
    return doFlush(ctx);
  }

  template <typename Context>
  void cancelAwaitWriteReady(Context& ctx) noexcept {
    if (backpressured_) {
      ctx.cancelAwaitWriteReady();
      backpressured_ = false;
    }
    blockedWriterAbove_ = false;
  }

  template <typename Context>
  void flushAndPropagateErrors(Context& ctx) noexcept {
    if (doFlush(ctx) ==
        apache::thrift::fast_thrift::channel_pipeline::Result::Error) {
      cancelAwaitWriteReady(ctx);
      ctx.fireException(
          folly::make_exception_wrapper<std::runtime_error>(
              "FrameFragmentationHandler: downstream write failed"));
    }
  }

  template <typename Context>
  [[nodiscard]] apache::thrift::fast_thrift::channel_pipeline::Result doFlush(
      Context& ctx) noexcept {
    // Step 1: Drain immediate queue.
    while (!immediateQueue_.empty()) {
      auto box = std::move(immediateQueue_.front());
      immediateQueue_.pop_front();
      auto& f = box.get<ComposedFrame>();
      const size_t sz = f.data ? f.data->computeChainDataLength() : 0;
      const uint32_t streamId = f.streamId;
      // Settle the pending accounting before handing the frame off — once
      // fireWrite() runs the frame is no longer ours to track, and a
      // re-entrant flush must see counters that already exclude it.
      decrPending(sz, 1);
      tracker_.onFragment(streamId, true);
      auto result = ctx.fireWrite(std::move(box));
      if (result ==
          apache::thrift::fast_thrift::channel_pipeline::Result::Error) {
        tracker_.onFragmentAborted();
        return result;
      }
      if (result ==
          apache::thrift::fast_thrift::channel_pipeline::Result::Backpressure) {
        backpressured_ = true;
        ctx.awaitWriteReady();
      }
    }

    // Step 2: SRPT — always flush the stream with least remaining bytes
    // first. Minimizes mean flow completion time.
    while (!streams_.empty()) {
      auto& state = streams_.peekMin();
      if (!state.hasMore()) {
        streams_.extractMin();
        continue;
      }

      const uint32_t curStreamId = state.streamId;
      const PendingFrame& front = state.frames.front();
      auto frag = state.nextFragment(
          front.firstEmitted ? fragmentDataLimit(0)
                             : firstFragmentDataLimit(front.originalFrame));
      const bool frameDone = frag.currentFrameDone;

      // Refresh heap priority after mutating remaining bytes.
      streams_.update(curStreamId);

      // Settle pending accounting before fireWrite() so a re-entrant flush
      // observes counters that already exclude this fragment.
      decrPending(frag.payloadBytes, frameDone ? 1 : 0);

      tracker_.onFragment(curStreamId, frameDone);
      auto result = ctx.fireWrite(
          apache::thrift::fast_thrift::channel_pipeline::erase_and_box(
              std::move(frag.outFrame)));

      if (result ==
          apache::thrift::fast_thrift::channel_pipeline::Result::Error) {
        tracker_.onFragmentAborted();
        return result;
      }
      if (result ==
          apache::thrift::fast_thrift::channel_pipeline::Result::Backpressure) {
        backpressured_ = true;
        ctx.awaitWriteReady();
      }

      if (!state.hasMore()) {
        streams_.erase(curStreamId);
      }
    }

    tracker_.onFlush();
    if (backpressured_) {
      return channel_pipeline::Result::Backpressure;
    }
    return channel_pipeline::Result::Success;
  }

  FragmentationHandlerConfig config_;
  folly::EventBase* eventBase_{nullptr};

  // Type-erased context dispatch for runLoopCallback. The trampoline
  // restores the original Context type captured in handlerAdded.
  void* ctxPtr_{nullptr};
  void (*flushTrampoline_)(FrameFragmentationHandlerT*, void*) noexcept {
      nullptr};

  bool isScheduled_{false};

  size_t pendingBytes_{0};
  size_t pendingFrames_{0};

  std::deque<apache::thrift::fast_thrift::channel_pipeline::TypeErasedBox>
      immediateQueue_;
  // SRPT min-heap: always flush the stream with least remaining bytes first.
  SrptHeap<PerStreamState, RemainingBytesFn> streams_;

  bool backpressured_{false};
  // Set when onWrite() answers Backpressure for a frame that downstream or a
  // queue took. The writer above then waits for this handler alone.
  bool blockedWriterAbove_{false};
  [[no_unique_address]] Tracker tracker_{};
};

using FrameFragmentationHandler =
    FrameFragmentationHandlerT<NoOpFragmentCompletionTracker>;

} // namespace apache::thrift::fast_thrift::frame::write::handler
