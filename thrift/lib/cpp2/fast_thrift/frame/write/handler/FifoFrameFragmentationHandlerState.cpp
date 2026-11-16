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

namespace apache::thrift::fast_thrift::frame::write::handler {

FifoFrameFragmentationHandlerState::FifoFrameFragmentationHandlerState(
    QuantumFragmentationHandlerConfig config) noexcept
    : streamQuantum_(config.streamQuantum),
      callbackBudget_(config.callbackBudget),
      configValid_(config.isValid()),
      streams_(config.streamQuantum) {}

void FifoFrameFragmentationHandlerState::clearPending() noexcept {
  streams_.clear();
}

void FifoFrameFragmentationHandlerState::removeStream(
    uint32_t streamId) noexcept {
  streams_.removeStream(streamId);
}

bool FifoFrameFragmentationHandlerState::enqueue(ComposedFrame&& frame) {
  if (!configValid_ || !metadataFitsInMaxStreamQuantum(frame)) {
    XLOGF_EVERY_MS(
        ERR,
        60'000,
        "{} refused a data frame with {} metadata bytes. The limit is {} "
        "bytes, and the config is {}.",
        kHandlerName,
        frame.metadata ? frame.metadata->computeChainDataLength() : 0,
        kMaxStreamQuantum,
        configValid_ ? "valid" : "invalid");
    return false;
  }
  return streams_.enqueue(std::move(frame));
}

void FifoFrameFragmentationHandlerState::startPass() noexcept {
  remainingCallbackBudget_ = callbackBudget_;
  remainingTurns_ = streams_.size();
}

std::optional<FifoFrameFragmentationHandlerState::ScheduledFragment>
FifoFrameFragmentationHandlerState::nextFragment() noexcept {
  if (remainingTurns_ == 0 || remainingCallbackBudget_ == 0) {
    return std::nullopt;
  }
  --remainingTurns_;
  std::optional<ScheduledFragment> scheduled = streams_.nextFragment();
  if (scheduled.has_value()) {
    if (scheduled->fragment.payloadBytes >= remainingCallbackBudget_) {
      remainingCallbackBudget_ = 0;
    } else {
      remainingCallbackBudget_ -= scheduled->fragment.payloadBytes;
    }
  }
  return scheduled;
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

} // namespace apache::thrift::fast_thrift::frame::write::handler
