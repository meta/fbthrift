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

#include <cstddef>
#include <cstdint>
#include <deque>
#include <optional>

#include <folly/Utility.h>

#include <thrift/lib/cpp2/fast_thrift/frame/read/DirectStreamMap.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/ComposedFrame.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/PerStreamState.h>

namespace apache::thrift::fast_thrift::frame::write {

/**
 * Queues fragmentable frames per stream in arrival order. Each turn returns one
 * fragment of the front stream, which keeps the turns until its current frame
 * ends. A stream with more frames then moves to the tail. Removing a stream
 * leaves its element until a later turn reaches it.
 */
class FifoStreamQueue : private folly::NonCopyableNonMovable {
 public:
  struct ScheduledFragment {
    uint32_t streamId{0};
    PerStreamState::Fragment fragment;
  };

  /**
   * `quantum` limits the metadata plus data of each fragment. The first
   * fragment always carries all metadata, so it exceeds `quantum` when the
   * metadata alone does. The queue does not check `quantum`. With 0, a frame
   * that has data never finishes, because each turn returns an empty fragment.
   */
  explicit FifoStreamQueue(size_t quantum);

  /**
   * Queues the frame behind earlier frames of its stream. Returns false and
   * drops the frame if the stream entered the queue with a complete frame or a
   * REQUEST_RESPONSE that is still queued. No data may follow such a frame.
   */
  [[nodiscard]] bool enqueue(ComposedFrame&& frame);
  /**
   * Takes one turn and returns one fragment. It skips removed elements and
   * returns nothing only when the queue is empty.
   */
  [[nodiscard]] std::optional<ScheduledFragment> nextFragment() noexcept;
  /**
   * After this call, no turn emits anything that was queued for the stream.
   * Returns whether the stream had queued frames. The empty element stays in
   * the queue until a later turn reaches it.
   */
  bool removeStream(uint32_t streamId);
  void clear();

  [[nodiscard]] bool empty() const noexcept;
  [[nodiscard]] size_t size() const noexcept;
  /**
   * Returns the number of queue elements. Each stream has one element,
   * regardless of its frame count. A removed stream stays in this count until
   * a later turn reaches its element. `size()` counts only live streams.
   */
  [[nodiscard]] size_t entryCount() const noexcept;
  [[nodiscard]] bool contains(uint32_t streamId) const noexcept;

  /**
   * Sets the position of the next queued element. Returns false and changes
   * nothing if the queue holds an element.
   */
  [[nodiscard]] bool setNextPositionForTest(uint32_t position) noexcept;

 private:
  using StreamPositionMap =
      apache::thrift::fast_thrift::frame::read::DirectStreamMap<uint32_t>;

  void popFront() noexcept;

  const size_t quantum_;
  std::deque<std::optional<PerStreamState>> states_;
  StreamPositionMap streamPositions_;
  uint32_t frontPosition_{0};
};

} // namespace apache::thrift::fast_thrift::frame::write
