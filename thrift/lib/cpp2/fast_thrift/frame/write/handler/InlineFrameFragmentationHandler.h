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
#include <memory>
#include <utility>

#include <folly/Utility.h>
#include <folly/io/IOBuf.h>
#include <folly/logging/xlog.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Event.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/ComposedFrame.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/PerStreamState.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/QuantumFragmentationHandlerConfig.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/handler/FragmentCompletionTracker.h>

namespace apache::thrift::fast_thrift::frame::write::handler {

/**
 * Sends every frame downstream inside `onWrite()`:
 * - A data frame whose metadata plus data fit in `kMaxStreamQuantum`,
 *   16,776,703 bytes, goes whole.
 * - A larger data frame goes as fragments of at most that size, one after
 *   another. The first fragment carries all metadata.
 * - A frame that cannot be fragmented goes unchanged.
 *
 * The handler holds no frame between calls. So Backpressure does not stop a
 * split, and the handler never waits for write-ready.
 *
 * Each frame that downstream accepts gets one completion from `Tracker`, when
 * the batch with its last fragment completes. A frame gets none if downstream
 * refuses it, or one of its fragments, with Error.
 *
 * Downstream may complete a batch inside `fireWrite()`, but a handler above
 * must not write again there, for example when it reacts to that completion.
 * If it does:
 * - During a split, the new frame goes out between the fragments of the
 *   current frame. A frame on the same stream breaks the split frame.
 * - If the new frame is split too, a batch that completes before the next
 *   fragment of the current frame can report egress as idle (`quiesced`),
 *   while the current frame still has fragments to send.
 * - If downstream then refuses the current write with Error, the new frame can
 *   lose its completion, and the current frame can get one.
 */
template <FragmentCompletionTracker Tracker = NoOpFragmentCompletionTracker>
class InlineFrameFragmentationHandlerT : private folly::NonCopyableNonMovable {
 public:
  template <typename Context>
  void handlerAdded(Context&) noexcept {}

  template <typename Context>
  void handlerRemoved(Context&) noexcept {}

  template <typename Context>
  void onWriteReady(Context&) noexcept {}

  template <typename Context>
  void onPipelineInactive(Context&) noexcept {}

  /**
   * Returns Error and drops a data frame whose metadata alone is above
   * `kMaxStreamQuantum`, and logs the metadata size at most once a minute.
   * After a downstream Error, sends no more fragments of the frame and returns
   * Error. Otherwise returns the result of the last downstream write.
   */
  template <typename Context>
  [[nodiscard]] channel_pipeline::Result onWrite(
      Context& ctx, channel_pipeline::TypeErasedBox&& msg) noexcept {
    const ComposedFrame& frame = msg.get<ComposedFrame>();
    if (!frame.canFragment() || fitsInStreamQuantum(frame, kMaxStreamQuantum)) {
      const uint32_t streamId = frame.streamId;
      return write(ctx, streamId, /*isLastFragment=*/true, std::move(msg));
    }
    if (!metadataFitsInMaxStreamQuantum(frame)) {
      XLOGF_EVERY_MS(
          ERR,
          60'000,
          "InlineFrameFragmentationHandler refused a data frame with {} "
          "metadata bytes. The limit is {} bytes.",
          frame.metadata->computeChainDataLength(),
          kMaxStreamQuantum);
      return channel_pipeline::Result::Error;
    }
    return writeFragments(ctx, msg.take<ComposedFrame>());
  }

  using PublishedEvents = typename Tracker::PublishedEvents;
  using SubscribedEvents = typename Tracker::SubscribedEvents;

  template <channel_pipeline::PipelineEvent E, typename Context>
  void on(Context& ctx, const typename E::Payload& event) noexcept {
    tracker_.template on<E>(ctx, event, splitting_);
  }

 private:
  template <typename Context>
  [[nodiscard]] channel_pipeline::Result writeFragments(
      Context& ctx, ComposedFrame&& frame) noexcept {
    PerStreamState state;
    state.streamId = frame.streamId;
    std::unique_ptr<folly::IOBuf> data = std::move(frame.data);
    const bool originalComplete = frame.isComplete();
    state.enqueue(std::move(frame), std::move(data), originalComplete);
    while (true) {
      PerStreamState::Fragment fragment = state.nextFragment(
          nextFragmentDataLimit(state.frames.front(), kMaxStreamQuantum));
      splitting_ = state.hasMore();
      const channel_pipeline::Result result = write(
          ctx,
          state.streamId,
          fragment.currentFrameDone,
          channel_pipeline::erase_and_box(std::move(fragment.outFrame)));
      if (result == channel_pipeline::Result::Error || !state.hasMore()) {
        splitting_ = false;
        return result;
      }
    }
  }

  // The record comes before fireWrite(), because a batch can complete inside
  // fireWrite() and that completion must find it.
  template <typename Context>
  [[nodiscard]] channel_pipeline::Result write(
      Context& ctx,
      uint32_t streamId,
      bool isLastFragment,
      channel_pipeline::TypeErasedBox&& msg) noexcept {
    tracker_.onFragment(streamId, isLastFragment);
    const channel_pipeline::Result result = ctx.fireWrite(std::move(msg));
    if (result == channel_pipeline::Result::Error) {
      tracker_.onFragmentAborted();
    }
    return result;
  }

  // True while fragments of the frame that writeFragments() splits wait for
  // their write. The tracker then reports no quiescence.
  bool splitting_{false};
  [[no_unique_address]] Tracker tracker_{};
};

using InlineFrameFragmentationHandler = InlineFrameFragmentationHandlerT<>;

} // namespace apache::thrift::fast_thrift::frame::write::handler
