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

#include <thrift/lib/cpp2/fast_thrift/frame/write/FifoStreamQueue.h>

#include <limits>
#include <memory>
#include <utility>

#include <glog/logging.h>

#include <thrift/lib/cpp2/fast_thrift/frame/FrameType.h>

namespace apache::thrift::fast_thrift::frame::write {

namespace {

uint32_t positionFor(uint32_t frontPosition, size_t queueSize) noexcept {
  DCHECK_LE(queueSize, std::numeric_limits<uint32_t>::max());
  return frontPosition + static_cast<uint32_t>(queueSize);
}

size_t indexFor(uint32_t position, uint32_t frontPosition) noexcept {
  return static_cast<size_t>(position - frontPosition);
}

size_t nextFragmentDataLimit(
    const PendingFrame& frame, size_t quantum) noexcept {
  if (frame.firstEmitted || frame.originalFrame.metadata == nullptr) {
    return quantum;
  }
  const size_t metadataSize =
      frame.originalFrame.metadata->computeChainDataLength();
  return metadataSize >= quantum ? 0 : quantum - metadataSize;
}

} // namespace

FifoStreamQueue::FifoStreamQueue(size_t quantum) : quantum_(quantum) {}

bool FifoStreamQueue::enqueue(ComposedFrame&& frame) {
  const uint32_t streamId = frame.streamId;
  std::unique_ptr<folly::IOBuf> data = std::move(frame.data);
  const bool originalComplete = frame.isComplete();
  const uint32_t position = positionFor(frontPosition_, states_.size());
  const auto [slot, inserted] = streamPositions_.emplace(streamId, position);
  if (!inserted) {
    const size_t index = indexFor(slot->second, frontPosition_);
    DCHECK_LT(index, states_.size());
    DCHECK(states_[index].has_value());
    PerStreamState& state = *states_[index];
    DCHECK_EQ(state.streamId, streamId);
    DCHECK(state.hasMore());
    if (state.oneFrameStream) {
      return false;
    }
    state.enqueue(std::move(frame), std::move(data), originalComplete);
    return true;
  }

  states_.emplace_back(std::in_place);
  PerStreamState& state = *states_.back();
  state.streamId = streamId;
  // A REQUEST_RESPONSE is the only data frame its sender sends, but
  // isComplete() is false for it.
  state.oneFrameStream =
      originalComplete || frame.frameType == FrameType::REQUEST_RESPONSE;
  state.enqueue(std::move(frame), std::move(data), originalComplete);
  return true;
}

std::optional<FifoStreamQueue::ScheduledFragment>
FifoStreamQueue::nextFragment() noexcept {
  while (!states_.empty()) {
    if (!states_.front().has_value()) {
      popFront();
      continue;
    }

    PerStreamState& state = *states_.front();
    const uint32_t streamId = state.streamId;
    DCHECK(state.hasMore());
    PerStreamState::Fragment fragment = state.nextFragment(
        nextFragmentDataLimit(state.frames.front(), quantum_));
    if (fragment.currentFrameDone && state.hasMore()) {
      // If this stream stayed at the front, it would send all its frames before
      // the other streams get a turn. If new frames keep arriving, the other
      // streams never get one.
      const uint32_t position = positionFor(frontPosition_, states_.size());
      states_.emplace_back(std::move(*states_.front()));
      popFront();
      const auto existing = streamPositions_.find(streamId);
      DCHECK(existing != streamPositions_.end());
      existing->second = position;
    } else if (fragment.currentFrameDone) {
      popFront();
      streamPositions_.erase(streamId);
    }
    return ScheduledFragment{streamId, std::move(fragment)};
  }

  DCHECK(streamPositions_.empty());
  return std::nullopt;
}

bool FifoStreamQueue::removeStream(uint32_t streamId) {
  const auto existing = streamPositions_.find(streamId);
  if (existing == streamPositions_.end()) {
    return false;
  }

  const size_t index = indexFor(existing->second, frontPosition_);
  DCHECK_LT(index, states_.size());
  DCHECK(states_[index].has_value());
  states_[index].reset();
  streamPositions_.erase(existing);
  return true;
}

void FifoStreamQueue::clear() {
  states_.clear();
  streamPositions_.clear();
}

bool FifoStreamQueue::empty() const noexcept {
  return streamPositions_.empty();
}

size_t FifoStreamQueue::size() const noexcept {
  return streamPositions_.size();
}

size_t FifoStreamQueue::entryCount() const noexcept {
  return states_.size();
}

bool FifoStreamQueue::contains(uint32_t streamId) const noexcept {
  return streamPositions_.contains(streamId);
}

bool FifoStreamQueue::setNextPositionForTest(uint32_t position) noexcept {
  if (!states_.empty()) {
    return false;
  }
  frontPosition_ = position;
  return true;
}

void FifoStreamQueue::popFront() noexcept {
  states_.pop_front();
  ++frontPosition_;
}

} // namespace apache::thrift::fast_thrift::frame::write
