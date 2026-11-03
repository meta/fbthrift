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
#include <utility>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Event.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/ComposedFrame.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/handler/FragmentCompletionTracker.h>

namespace apache::thrift::fast_thrift::frame::write::handler {

/**
 * Callers get the downstream result for the same `ComposedFrame` they wrote.
 * This version does not queue or fragment frames. The tracker records one
 * final fragment for each write that downstream accepts.
 */
template <FragmentCompletionTracker Tracker = NoOpFragmentCompletionTracker>
class FifoFrameFragmentationHandlerT {
 public:
  template <typename Context>
  void handlerAdded(Context& /*ctx*/) noexcept {}

  template <typename Context>
  void handlerRemoved(Context& /*ctx*/) noexcept {}

  template <typename Context>
  [[nodiscard]] channel_pipeline::Result onWrite(
      Context& ctx, channel_pipeline::TypeErasedBox&& msg) noexcept {
    const uint32_t streamId = msg.get<ComposedFrame>().streamId;
    const channel_pipeline::Result result = ctx.fireWrite(std::move(msg));
    if (result != channel_pipeline::Result::Error) {
      tracker_.onFragment(streamId, /*isLastFragment=*/true);
    }
    return result;
  }

  template <typename Context>
  void onWriteReady(Context& /*ctx*/) noexcept {}

  template <typename Context>
  void onPipelineInactive(Context& /*ctx*/) noexcept {}

  Tracker& tracker() noexcept { return tracker_; }

  // The pipeline reads these aliases to register the events that the tracker
  // publishes and receives.
  using PublishedEvents = typename Tracker::PublishedEvents;
  using SubscribedEvents = typename Tracker::SubscribedEvents;

  // The pipeline calls this for each event in SubscribedEvents. This version
  // has no pending writes when the tracker handles the event.
  template <channel_pipeline::PipelineEvent E, typename Context>
  void on(Context& ctx, const typename E::Payload& event) noexcept {
    tracker_.template on<E>(ctx, event, /*handlerHasPendingWrites=*/false);
  }

 private:
  [[no_unique_address]] Tracker tracker_{};
};

// Use this concrete type when a pipeline does not need frame completion events.
using FifoFrameFragmentationHandler =
    FifoFrameFragmentationHandlerT<NoOpFragmentCompletionTracker>;

} // namespace apache::thrift::fast_thrift::frame::write::handler
