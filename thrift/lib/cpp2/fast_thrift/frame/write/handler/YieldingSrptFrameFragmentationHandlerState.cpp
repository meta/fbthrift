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

#include <thrift/lib/cpp2/fast_thrift/frame/write/handler/YieldingSrptFrameFragmentationHandlerState.h>

#include <algorithm>
#include <memory>
#include <utility>

#include <glog/logging.h>
#include <folly/logging/xlog.h>

#include <thrift/lib/cpp2/fast_thrift/frame/FrameType.h>

namespace apache::thrift::fast_thrift::frame::write::handler {

namespace {

// A frame sends all its metadata with its first fragment. enqueue() refuses
// metadata above kMaxStreamQuantum, so that fragment fits in one frame.
size_t unsentMetadataSize(const PendingFrame& frame) noexcept {
  return !frame.firstEmitted && frame.originalFrame.metadata != nullptr
      ? frame.originalFrame.metadata->computeChainDataLength()
      : 0;
}

// Only fragmentable frames are deferred, so a RESERVED one was removed.
bool isRemovedArrival(const ComposedFrame& frame) noexcept {
  return frame.frameType == FrameType::RESERVED;
}

} // namespace

YieldingSrptFrameFragmentationHandlerState::
    YieldingSrptFrameFragmentationHandlerState(
        QuantumFragmentationHandlerConfig config) noexcept
    : streamQuantum_(config.streamQuantum),
      callbackBudget_(config.callbackBudget),
      configValid_(config.isValid()) {}

YieldingSrptFrameFragmentationHandlerState::
    ~YieldingSrptFrameFragmentationHandlerState() {
  cancelLoopCallbackIfScheduled();
}

void YieldingSrptFrameFragmentationHandlerState::attach(
    folly::EventBase* eventBase,
    void* handler,
    void* context,
    FlushTrampoline flushTrampoline) noexcept {
  eventBase_ = eventBase;
  handler_ = handler;
  context_ = context;
  flushTrampoline_ = flushTrampoline;
}

void YieldingSrptFrameFragmentationHandlerState::detach() noexcept {
  clearPending();
  eventBase_ = nullptr;
  handler_ = nullptr;
  context_ = nullptr;
  flushTrampoline_ = nullptr;
}

void YieldingSrptFrameFragmentationHandlerState::clearPending() noexcept {
  cancelLoopCallbackIfScheduled();
  streams_.clear();
  pinned_.reset();
  deferredArrivals_.clear();
  passActive_ = false;
  remainingCallbackBudget_ = 0;
  remainingTurns_ = 0;
}

YieldingSrptFrameFragmentationHandlerState::WriteDisposition
YieldingSrptFrameFragmentationHandlerState::prepareWrite(
    const ComposedFrame& frame) noexcept {
  if (isTerminalFrameType(frame.frameType)) {
    removeStream(frame.streamId);
    return WriteDisposition::PassThrough;
  }
  if (!frame.canFragment()) {
    return WriteDisposition::PassThrough;
  }
  // With a bad config, enqueue() refuses every data frame.
  if (configValid_ && !hasPendingStream(frame.streamId) &&
      fitsInStreamQuantum(frame, streamQuantum_)) {
    return WriteDisposition::PassThrough;
  }
  return WriteDisposition::Defer;
}

bool YieldingSrptFrameFragmentationHandlerState::enqueue(
    ComposedFrame&& frame) {
  if (!configValid_ || !metadataFitsInMaxStreamQuantum(frame)) {
    XLOGF_EVERY_MS(
        ERR,
        60'000,
        "YieldingSrptFrameFragmentationHandler refused a data frame with {} "
        "metadata bytes. The limit is {} bytes, and the config is {}.",
        frame.metadata ? frame.metadata->computeChainDataLength() : 0,
        kMaxStreamQuantum,
        configValid_ ? "valid" : "invalid");
    return false;
  }
  if (passActive_) {
    deferredArrivals_.push_back(std::move(frame));
  } else {
    enqueueNow(std::move(frame));
    scheduleFlushIfNeeded();
  }
  return true;
}

void YieldingSrptFrameFragmentationHandlerState::startPass() noexcept {
  DCHECK(!passActive_);
  passActive_ = true;
  remainingCallbackBudget_ = callbackBudget_;
  remainingTurns_ = streams_.size() + (pinned_.has_value() ? 1 : 0);
}

std::optional<YieldingSrptFrameFragmentationHandlerState::ScheduledFragment>
YieldingSrptFrameFragmentationHandlerState::nextFragment() noexcept {
  if (remainingCallbackBudget_ != 0 && remainingTurns_ != 0 &&
      (pinned_.has_value() || !streams_.empty())) {
    --remainingTurns_;
    ScheduledFragment scheduled = takePinnedFragment();
    spendCallbackBudget(scheduled.fragment.payloadBytes);
    return scheduled;
  }
  // A frame that takeDeferredArrival() queues waits for the next pass.
  remainingTurns_ = 0;
  return takeDeferredArrival();
}

bool YieldingSrptFrameFragmentationHandlerState::finishPass() noexcept {
  passActive_ = false;
  if (!hasPendingWrites()) {
    return true;
  }
  scheduleFlushIfNeeded();
  return false;
}

void YieldingSrptFrameFragmentationHandlerState::stopAfterError() noexcept {
  clearPending();
}

bool YieldingSrptFrameFragmentationHandlerState::hasPendingStream(
    uint32_t streamId) const noexcept {
  if (hasQueuedStream(streamId)) {
    return true;
  }
  for (const ComposedFrame& frame : deferredArrivals_) {
    if (!isRemovedArrival(frame) && frame.streamId == streamId) {
      return true;
    }
  }
  return false;
}

bool YieldingSrptFrameFragmentationHandlerState::hasPendingWrites()
    const noexcept {
  return !streams_.empty() || pinned_.has_value() ||
      !std::all_of(
          deferredArrivals_.begin(), deferredArrivals_.end(), isRemovedArrival);
}

bool YieldingSrptFrameFragmentationHandlerState::isScheduled() const noexcept {
  return isScheduled_;
}

void YieldingSrptFrameFragmentationHandlerState::runLoopCallback() noexcept {
  isScheduled_ = false;
  if (flushTrampoline_ != nullptr) {
    flushTrampoline_(handler_, context_);
  }
}

void YieldingSrptFrameFragmentationHandlerState::
    scheduleFlushIfNeeded() noexcept {
  if (!isScheduled_ && eventBase_ != nullptr && hasPendingWrites()) {
    eventBase_->runInLoop(this);
    isScheduled_ = true;
  }
}

void YieldingSrptFrameFragmentationHandlerState::
    cancelLoopCallbackIfScheduled() noexcept {
  if (isScheduled_) {
    cancelLoopCallback();
    isScheduled_ = false;
  }
}

void YieldingSrptFrameFragmentationHandlerState::removeStream(
    uint32_t streamId) noexcept {
  streams_.erase(streamId);
  if (pinned_.has_value() && pinned_->streamId == streamId) {
    pinned_.reset();
  }
  for (ComposedFrame& frame : deferredArrivals_) {
    if (frame.streamId == streamId) {
      frame = ComposedFrame{};
    }
  }
}

bool YieldingSrptFrameFragmentationHandlerState::hasQueuedStream(
    uint32_t streamId) const noexcept {
  return streams_.contains(streamId) ||
      (pinned_.has_value() && pinned_->streamId == streamId);
}

std::optional<YieldingSrptFrameFragmentationHandlerState::ScheduledFragment>
YieldingSrptFrameFragmentationHandlerState::takeDeferredArrival() {
  while (!deferredArrivals_.empty()) {
    ComposedFrame frame = std::move(deferredArrivals_.front());
    deferredArrivals_.pop_front();
    if (isRemovedArrival(frame)) {
      continue;
    }
    if (!hasQueuedStream(frame.streamId) &&
        fitsInStreamQuantum(frame, streamQuantum_)) {
      const uint32_t streamId = frame.streamId;
      const size_t payloadBytes =
          frame.data ? frame.data->computeChainDataLength() : 0;
      return ScheduledFragment{
          streamId,
          PerStreamState::Fragment{
              .outFrame = std::move(frame),
              .payloadBytes = payloadBytes,
              .currentFrameDone = true,
          }};
    }
    enqueueNow(std::move(frame));
  }
  return std::nullopt;
}

void YieldingSrptFrameFragmentationHandlerState::enqueueNow(
    ComposedFrame&& frame) {
  const uint32_t streamId = frame.streamId;
  std::unique_ptr<folly::IOBuf> data = std::move(frame.data);
  const bool originalComplete = frame.isComplete();

  if (pinned_.has_value() && pinned_->streamId == streamId) {
    pinned_->enqueue(std::move(frame), std::move(data), originalComplete);
    return;
  }

  // The frame queues behind the front frame, so the heap key does not change.
  if (PerStreamState* stream = streams_.find(streamId)) {
    stream->enqueue(std::move(frame), std::move(data), originalComplete);
    return;
  }

  PerStreamState stream;
  stream.streamId = streamId;
  stream.enqueue(std::move(frame), std::move(data), originalComplete);
  streams_.insert(streamId, std::move(stream));
}

YieldingSrptFrameFragmentationHandlerState::ScheduledFragment
YieldingSrptFrameFragmentationHandlerState::takePinnedFragment() noexcept {
  if (!pinned_.has_value()) {
    pinned_.emplace(streams_.extractMin());
  }
  const uint32_t streamId = pinned_->streamId;
  PerStreamState::Fragment fragment = takeFragment(*pinned_);
  if (fragment.currentFrameDone) {
    if (pinned_->hasMore()) {
      streams_.insert(streamId, std::move(*pinned_));
    }
    pinned_.reset();
  }
  return ScheduledFragment{streamId, std::move(fragment)};
}

PerStreamState::Fragment
YieldingSrptFrameFragmentationHandlerState::takeFragment(
    PerStreamState& stream) noexcept {
  CHECK(!stream.frames.empty());
  const size_t metadataSize = unsentMetadataSize(stream.frames.front());
  const size_t dataLimit =
      metadataSize >= streamQuantum_ ? 0 : streamQuantum_ - metadataSize;
  return stream.nextFragment(dataLimit);
}

void YieldingSrptFrameFragmentationHandlerState::spendCallbackBudget(
    size_t dataBytes) noexcept {
  if (dataBytes >= remainingCallbackBudget_) {
    remainingCallbackBudget_ = 0;
  } else {
    remainingCallbackBudget_ -= dataBytes;
  }
}

} // namespace apache::thrift::fast_thrift::frame::write::handler
