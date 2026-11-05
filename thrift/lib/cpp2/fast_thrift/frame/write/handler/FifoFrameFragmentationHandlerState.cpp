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

#include <thrift/lib/cpp2/fast_thrift/frame/write/handler/FifoFrameFragmentationHandlerState.h>

#include <optional>
#include <utility>

#include <folly/logging/xlog.h>

#include <thrift/lib/cpp2/fast_thrift/frame/FrameType.h>

namespace apache::thrift::fast_thrift::frame::write::handler {

FifoFrameFragmentationHandlerState::FifoFrameFragmentationHandlerState(
    QuantumFragmentationHandlerConfig config) noexcept
    : streamQuantum_(config.streamQuantum),
      configValid_(config.isValid()),
      streams_(config.streamQuantum) {}

FifoFrameFragmentationHandlerState::~FifoFrameFragmentationHandlerState() {
  cancelLoopCallbackIfScheduled();
}

void FifoFrameFragmentationHandlerState::attach(
    folly::EventBase* eventBase,
    void* handler,
    void* context,
    FlushTrampoline flushTrampoline) noexcept {
  eventBase_ = eventBase;
  handler_ = handler;
  context_ = context;
  flushTrampoline_ = flushTrampoline;
}

void FifoFrameFragmentationHandlerState::detach() noexcept {
  cancelLoopCallbackIfScheduled();
  streams_.clear();
  eventBase_ = nullptr;
  handler_ = nullptr;
  context_ = nullptr;
  flushTrampoline_ = nullptr;
}

void FifoFrameFragmentationHandlerState::clearPending() noexcept {
  cancelLoopCallbackIfScheduled();
  streams_.clear();
}

FifoFrameFragmentationHandlerState::WriteDisposition
FifoFrameFragmentationHandlerState::prepareWrite(
    const ComposedFrame& frame) noexcept {
  if (isTerminalFrameType(frame.frameType)) {
    streams_.removeStream(frame.streamId);
    return WriteDisposition::PassThrough;
  }
  if (!frame.canFragment()) {
    return WriteDisposition::PassThrough;
  }
  // With a bad config, enqueue() refuses every data frame.
  if (configValid_ && !streams_.contains(frame.streamId) &&
      fitsInStreamQuantum(frame, streamQuantum_)) {
    return WriteDisposition::PassThrough;
  }
  return WriteDisposition::Defer;
}

bool FifoFrameFragmentationHandlerState::enqueue(ComposedFrame&& frame) {
  if (!configValid_ || !metadataFitsInMaxStreamQuantum(frame)) {
    XLOGF_EVERY_MS(
        ERR,
        60'000,
        "FifoFrameFragmentationHandler refused a data frame with {} metadata "
        "bytes. The limit is {} bytes, and the config is {}.",
        frame.metadata ? frame.metadata->computeChainDataLength() : 0,
        kMaxStreamQuantum,
        configValid_ ? "valid" : "invalid");
    return false;
  }
  if (streams_.enqueue(std::move(frame))) {
    scheduleFlushIfNeeded();
    return true;
  }
  return false;
}

size_t FifoFrameFragmentationHandlerState::startPass() const noexcept {
  return streams_.size();
}

std::optional<FifoStreamQueue::ScheduledFragment>
FifoFrameFragmentationHandlerState::nextFragment() noexcept {
  return streams_.nextFragment();
}

bool FifoFrameFragmentationHandlerState::finishPass() noexcept {
  if (streams_.empty()) {
    return true;
  }
  scheduleFlushIfNeeded();
  return false;
}

void FifoFrameFragmentationHandlerState::stopAfterError() noexcept {
  clearPending();
}

size_t FifoFrameFragmentationHandlerState::pendingStreamCount() const noexcept {
  return streams_.size();
}

size_t FifoFrameFragmentationHandlerState::pendingQueueEntryCount()
    const noexcept {
  return streams_.entryCount();
}

bool FifoFrameFragmentationHandlerState::hasPendingStream(
    uint32_t streamId) const noexcept {
  return streams_.contains(streamId);
}

bool FifoFrameFragmentationHandlerState::hasPendingWrites() const noexcept {
  return !streams_.empty();
}

bool FifoFrameFragmentationHandlerState::isScheduled() const noexcept {
  return isScheduled_;
}

void FifoFrameFragmentationHandlerState::runLoopCallback() noexcept {
  isScheduled_ = false;
  if (flushTrampoline_ != nullptr) {
    flushTrampoline_(handler_, context_);
  }
}

void FifoFrameFragmentationHandlerState::scheduleFlushIfNeeded() noexcept {
  if (!isScheduled_ && eventBase_ != nullptr && !streams_.empty()) {
    eventBase_->runInLoop(this);
    isScheduled_ = true;
  }
}

void FifoFrameFragmentationHandlerState::
    cancelLoopCallbackIfScheduled() noexcept {
  if (isScheduled_) {
    cancelLoopCallback();
    isScheduled_ = false;
  }
}

} // namespace apache::thrift::fast_thrift::frame::write::handler
