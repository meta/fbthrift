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

#include <thrift/lib/cpp2/fast_thrift/frame/FrameType.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/ComposedFrame.h>

#include <folly/io/IOBuf.h>
#include <folly/io/IOBufQueue.h>

#include <glog/logging.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <utility>

namespace apache::thrift::fast_thrift::frame::write {

/**
 * One logical frame awaiting fragmentation. Its data stays in
 * `originalFrame.data` until `nextFragment()` emits this frame's first
 * fragment. `dataSize` is the size of that data at `enqueue()`, and emitted
 * fragments do not change it.
 *
 * `originalComplete` records whether the original frame carried COMPLETE.
 * Only the original frame's last emitted fragment carries that flag. If the
 * frame is not split, that fragment is the original frame. If it is split,
 * that fragment is the final PAYLOAD continuation. Earlier fragments clear
 * COMPLETE.
 */
struct PendingFrame {
  ComposedFrame originalFrame{};
  size_t dataSize{0};
  bool firstEmitted{false};
  bool originalComplete{false};

  PendingFrame() = default;
  ~PendingFrame() = default;
  PendingFrame(PendingFrame&&) noexcept = default;
  PendingFrame& operator=(PendingFrame&&) noexcept = default;
  PendingFrame(const PendingFrame&) = delete;
  PendingFrame& operator=(const PendingFrame&) = delete;
};

inline size_t nextFragmentDataLimit(
    const PendingFrame& frame, size_t quantum) noexcept {
  if (frame.firstEmitted || frame.originalFrame.metadata == nullptr) {
    return quantum;
  }
  const size_t metadataSize =
      frame.originalFrame.metadata->computeChainDataLength();
  return metadataSize >= quantum ? 0 : quantum - metadataSize;
}

/**
 * Per-stream fragmentation state. Owns a FIFO of `PendingFrame`s;
 * SRPT scheduling reads `remaining()` (sum across the FIFO, cached).
 *
 * `nextFragment()` extracts and materializes the next on-wire frame for
 * this stream, dispatching first-vs-continuation typing in one place so
 * the handler stays focused on scheduling.
 */
struct PerStreamState {
  uint32_t streamId{0};
  // Marks a stream that no later frame may join, because its first frame is
  // complete or a REQUEST_RESPONSE. Only an owner that applies this rule sets
  // the flag. This struct neither sets nor checks it.
  bool oneFrameStream{false};
  std::deque<PendingFrame> frames;
  // Holds the unsent data from the front frame after its first fragment. The
  // front frame leaves `frames` only after this queue is empty.
  folly::IOBufQueue dataQueue{folly::IOBufQueue::cacheChainLength()};
  size_t totalRemaining{0};

  PerStreamState() = default;
  ~PerStreamState() = default;
  PerStreamState(PerStreamState&&) noexcept = default;
  PerStreamState& operator=(PerStreamState&&) noexcept = default;
  PerStreamState(const PerStreamState&) = delete;
  PerStreamState& operator=(const PerStreamState&) = delete;

  void enqueue(
      ComposedFrame&& originalFrame,
      std::unique_ptr<folly::IOBuf> data,
      bool originalComplete) noexcept {
    const size_t size = data ? data->computeChainDataLength() : 0;
    PendingFrame pf;
    pf.originalFrame = std::move(originalFrame);
    pf.originalComplete = originalComplete;
    pf.originalFrame.data = std::move(data);
    pf.dataSize = size;
    totalRemaining += size;
    frames.push_back(std::move(pf));
  }

  [[nodiscard]] bool hasMore() const noexcept { return !frames.empty(); }
  [[nodiscard]] size_t remaining() const noexcept { return totalRemaining; }

  /**
   * Materialized fragment ready to emit. The handler fires `outFrame`,
   * decrements `payloadBytes` from its pending counter, and decrements
   * the frame counter iff `currentFrameDone`.
   */
  struct Fragment {
    ComposedFrame outFrame{};
    size_t payloadBytes{0};
    bool currentFrameDone{false};
  };

  /**
   * Returns the original frame first, then PAYLOAD continuations. Each frame
   * contains at most `maxSize` data bytes.
   *
   * The method removes the head frame after its final fragment.
   * Precondition: `hasMore()`.
   */
  Fragment nextFragment(size_t maxSize) noexcept {
    PendingFrame& frame = frames.front();
    if (!frame.firstEmitted) {
      DCHECK(dataQueue.empty());
      if (frame.originalFrame.data != nullptr) {
        dataQueue.append(std::move(frame.originalFrame.data));
      }
    }
    const size_t chunkSize = std::min(maxSize, dataQueue.chainLength());
    std::unique_ptr<folly::IOBuf> chunk = dataQueue.split(chunkSize);
    const bool follows = !dataQueue.empty();

    totalRemaining -= chunkSize;

    Fragment frag;
    frag.payloadBytes = chunkSize;
    frag.currentFrameDone = !follows;
    if (!frame.firstEmitted) {
      frame.firstEmitted = true;
      frag.outFrame = std::move(frame.originalFrame);
      frag.outFrame.data = std::move(chunk);
      frag.outFrame.follows = follows;
      frag.outFrame.complete = !follows && frame.originalComplete;
    } else {
      frag.outFrame.frameType = FrameType::PAYLOAD;
      frag.outFrame.streamId = streamId;
      frag.outFrame.data = std::move(chunk);
      frag.outFrame.follows = follows;
      frag.outFrame.complete = !follows && frame.originalComplete;
      frag.outFrame.next = true;
    }

    if (frag.currentFrameDone) {
      frames.pop_front();
    }
    return frag;
  }
};

} // namespace apache::thrift::fast_thrift::frame::write
