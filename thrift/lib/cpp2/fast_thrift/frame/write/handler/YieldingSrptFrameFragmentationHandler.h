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

#include <thrift/lib/cpp2/fast_thrift/frame/write/QuantumFragmentationHandlerConfig.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/handler/FragmentCompletionTracker.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/handler/QuantumFragmentationHandlerBase.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/handler/YieldingSrptFrameFragmentationHandlerState.h>

namespace apache::thrift::fast_thrift::frame::write::handler {

/**
 * Sends deferred data frames in bounded yielding SRPT passes.
 *
 * A pass gives at most K turns, where K is the number of streams with deferred
 * frames at pass start. Each turn sends one fragment of the pinned stream. A
 * callback starts no new turn after its data budget reaches zero, but the turn
 * that crosses the budget completes.
 *
 * When no stream is pinned, the stream whose front frame has the fewest data
 * bytes becomes pinned. It stays pinned until the last fragment of that frame,
 * and then competes again with its next frame. Frames behind the front frame
 * do not count.
 *
 * A frame deferred during a pass waits until the pass ends. It then goes
 * downstream at once if it fits in `streamQuantum` and its stream has nothing
 * deferred, and otherwise waits for the next pass.
 *
 * `CANCEL` and `ERROR` first drop the deferred frames of their stream, and the
 * rest of its frame if the stream is pinned. If a downstream write fails
 * during a callback, the handler drops every deferred frame and the rest of
 * the pinned frame. Then it calls `fireException()`.
 *
 * QuantumFragmentationHandlerBase decides which frames go downstream at once,
 * and handles write-ready, write errors and the completion tracker.
 */
template <FragmentCompletionTracker Tracker = NoOpFragmentCompletionTracker>
class YieldingSrptFrameFragmentationHandlerT
    : public QuantumFragmentationHandlerBase<
          YieldingSrptFrameFragmentationHandlerState,
          Tracker> {
 public:
  explicit YieldingSrptFrameFragmentationHandlerT(
      QuantumFragmentationHandlerConfig config = {}) noexcept
      : QuantumFragmentationHandlerBase<
            YieldingSrptFrameFragmentationHandlerState,
            Tracker>(config) {}
};

using YieldingSrptFrameFragmentationHandler =
    YieldingSrptFrameFragmentationHandlerT<NoOpFragmentCompletionTracker>;

} // namespace apache::thrift::fast_thrift::frame::write::handler
