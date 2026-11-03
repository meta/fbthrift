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

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <ostream>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
#include <folly/io/IOBuf.h>
#include <folly/portability/GMock.h>

#include <thrift/lib/cpp2/fast_thrift/frame/write/FifoStreamQueue.h>

namespace apache::thrift::fast_thrift::frame::write {
namespace {

using namespace testing;

ComposedFrame makeFrame(
    uint32_t streamId, std::string_view data, bool complete = false) {
  return ComposedFrame{
      .frameType = FrameType::PAYLOAD,
      .streamId = streamId,
      .data = folly::IOBuf::copyBuffer(data),
      .complete = complete,
      .next = true,
  };
}

ComposedFrame makeRequestResponse(
    uint32_t streamId, std::string_view data, std::string_view metadata = {}) {
  return ComposedFrame{
      .frameType = FrameType::REQUEST_RESPONSE,
      .streamId = streamId,
      .metadata =
          metadata.empty() ? nullptr : folly::IOBuf::copyBuffer(metadata),
      .data = folly::IOBuf::copyBuffer(data),
  };
}

struct Visit {
  uint32_t streamId;
  size_t payloadBytes;

  bool operator==(const Visit&) const = default;
};

void PrintTo(const Visit& visit, std::ostream* os) {
  *os << "{" << visit.streamId << ", " << visit.payloadBytes << "}";
}

std::optional<Visit> visitNext(FifoStreamQueue& queue) {
  if (const std::optional<FifoStreamQueue::ScheduledFragment> scheduled =
          queue.nextFragment();
      scheduled.has_value()) {
    return Visit{scheduled->streamId, scheduled->fragment.payloadBytes};
  }
  return std::nullopt;
}

std::tuple<bool, size_t, size_t, bool> fragmentShape(
    const FifoStreamQueue::ScheduledFragment& scheduled) {
  const std::unique_ptr<folly::IOBuf>& metadata =
      scheduled.fragment.outFrame.metadata;
  return {
      metadata != nullptr,
      metadata ? metadata->computeChainDataLength() : 0,
      scheduled.fragment.payloadBytes,
      scheduled.fragment.outFrame.follows,
  };
}

std::vector<Visit> visitUntilEmpty(FifoStreamQueue& queue) {
  std::vector<Visit> visits;
  while (!queue.empty()) {
    const std::optional<Visit> visit = visitNext(queue);
    if (!visit.has_value()) {
      break;
    }
    visits.push_back(*visit);
  }
  return visits;
}

TEST(FifoStreamQueueTest, RequestResponseMetadataCountsTowardQuantum) {
  FifoStreamQueue queue(4);
  ASSERT_THAT(queue.enqueue(makeRequestResponse(1, "abc", "de")), IsTrue());

  const std::optional<FifoStreamQueue::ScheduledFragment> first =
      queue.nextFragment();
  const std::optional<FifoStreamQueue::ScheduledFragment> second =
      queue.nextFragment();

  ASSERT_THAT(first.has_value(), IsTrue());
  ASSERT_THAT(second.has_value(), IsTrue());
  EXPECT_THAT(
      fragmentShape(*first), Eq(std::tuple(true, size_t{2}, size_t{2}, true)));
  EXPECT_THAT(
      fragmentShape(*second),
      Eq(std::tuple(false, size_t{0}, size_t{1}, false)));
  EXPECT_THAT(queue.empty(), IsTrue());
}

TEST(FifoStreamQueueTest, MetadataLargerThanQuantumGetsADataFreeFirstFragment) {
  FifoStreamQueue queue(2);
  ASSERT_THAT(queue.enqueue(makeRequestResponse(1, "abc", "def")), IsTrue());

  const std::optional<FifoStreamQueue::ScheduledFragment> first =
      queue.nextFragment();

  ASSERT_THAT(first.has_value(), IsTrue());
  EXPECT_THAT(
      fragmentShape(*first), Eq(std::tuple(true, size_t{3}, size_t{0}, true)));
}

TEST(FifoStreamQueueTest, ContainsAQueuedStream) {
  FifoStreamQueue queue(1024);
  ASSERT_THAT(queue.enqueue(makeFrame(1, "a")), IsTrue());

  EXPECT_THAT(queue.contains(1), IsTrue());
  EXPECT_THAT(queue.size(), Eq(1));
  EXPECT_THAT(queue.entryCount(), Eq(1));
}

TEST(FifoStreamQueueTest, KeepsOneElementForAMultiFrameStream) {
  FifoStreamQueue queue(1024);
  ASSERT_THAT(queue.enqueue(makeFrame(7, "a")), IsTrue());
  ASSERT_THAT(queue.enqueue(makeFrame(7, "b")), IsTrue());
  ASSERT_THAT(queue.enqueue(makeFrame(7, "c")), IsTrue());

  EXPECT_THAT(queue.size(), Eq(1));
  EXPECT_THAT(queue.entryCount(), Eq(1));
}

TEST(FifoStreamQueueTest, CompleteFrameJoinsAQueuedStream) {
  FifoStreamQueue queue(1024);
  ASSERT_THAT(queue.enqueue(makeFrame(9, "a")), IsTrue());
  ASSERT_THAT(queue.enqueue(makeFrame(9, "bb", true)), IsTrue());
  ASSERT_THAT(queue.enqueue(makeFrame(9, "ccc")), IsTrue());

  EXPECT_THAT(queue.size(), Eq(1));
  EXPECT_THAT(queue.entryCount(), Eq(1));

  const std::optional<Visit> first = visitNext(queue);
  const std::optional<Visit> second = visitNext(queue);
  const std::optional<Visit> third = visitNext(queue);

  ASSERT_THAT(first.has_value(), IsTrue());
  ASSERT_THAT(second.has_value(), IsTrue());
  ASSERT_THAT(third.has_value(), IsTrue());
  EXPECT_THAT(first->streamId, Eq(9));
  EXPECT_THAT(second->streamId, Eq(9));
  EXPECT_THAT(third->streamId, Eq(9));
  const std::vector<size_t> payloadSizes{
      first->payloadBytes, second->payloadBytes, third->payloadBytes};
  EXPECT_THAT(payloadSizes, ElementsAre(1, 2, 3));
  EXPECT_THAT(queue.empty(), IsTrue());
}

TEST(FifoStreamQueueTest, RemovesEitherKindOfStream) {
  FifoStreamQueue queue(1);
  ASSERT_THAT(queue.enqueue(makeFrame(1, "abc", true)), IsTrue());
  ASSERT_THAT(queue.enqueue(makeFrame(3, "de")), IsTrue());
  ASSERT_THAT(visitNext(queue).has_value(), IsTrue());

  EXPECT_THAT(queue.removeStream(1), IsTrue());
  EXPECT_THAT(queue.removeStream(3), IsTrue());

  EXPECT_THAT(queue.contains(1), IsFalse());
  EXPECT_THAT(queue.contains(3), IsFalse());
  EXPECT_THAT(
      std::tuple(queue.size(), queue.entryCount()),
      Eq(std::tuple(size_t{0}, size_t{2})));

  EXPECT_THAT(visitNext(queue).has_value(), IsFalse());
  EXPECT_THAT(queue.removeStream(999), IsFalse());
}

TEST(FifoStreamQueueTest, RefusesASecondFrameOnAOneFrameStream) {
  FifoStreamQueue queue(2);
  ASSERT_THAT(queue.enqueue(makeRequestResponse(1, "abc")), IsTrue());
  ASSERT_THAT(queue.enqueue(makeFrame(3, "def", true)), IsTrue());
  const std::optional<FifoStreamQueue::ScheduledFragment> first =
      queue.nextFragment();
  ASSERT_THAT(first.has_value(), IsTrue());
  EXPECT_THAT(
      std::tuple(
          first->fragment.payloadBytes, first->fragment.outFrame.follows),
      Eq(std::tuple(size_t{2}, true)));

  EXPECT_THAT(queue.enqueue(makeFrame(1, "de")), IsFalse());
  EXPECT_THAT(queue.enqueue(makeFrame(3, "gh")), IsFalse());

  EXPECT_THAT(queue.size(), Eq(2));
  EXPECT_THAT(queue.entryCount(), Eq(2));
  const std::vector<Visit> expected{{1, 1}, {3, 2}, {3, 1}};
  EXPECT_THAT(visitUntilEmpty(queue), Eq(expected));
}

TEST(FifoStreamQueueTest, ClearDropsEveryStream) {
  FifoStreamQueue queue(1024);
  for (uint32_t streamId = 1; streamId <= 33; streamId += 2) {
    ASSERT_THAT(queue.enqueue(makeFrame(streamId, "a")), IsTrue());
  }

  queue.clear();

  EXPECT_THAT(queue.empty(), IsTrue());
  EXPECT_THAT(queue.size(), Eq(0));
  EXPECT_THAT(queue.entryCount(), Eq(0));
}

TEST(FifoStreamQueueTest, SendsLargeStreamsOneAfterAnother) {
  FifoStreamQueue queue(2);
  ASSERT_THAT(queue.enqueue(makeFrame(1, "abcdef")), IsTrue());
  ASSERT_THAT(queue.enqueue(makeFrame(3, "ghij")), IsTrue());

  const std::vector<Visit> expected{{1, 2}, {1, 2}, {1, 2}, {3, 2}, {3, 2}};
  EXPECT_THAT(visitUntilEmpty(queue), Eq(expected));
}

TEST(FifoStreamQueueTest, MovesAMultiFrameStreamAfterEachFrame) {
  FifoStreamQueue queue(4);
  ASSERT_THAT(queue.enqueue(makeFrame(1, "ab")), IsTrue());
  ASSERT_THAT(queue.enqueue(makeFrame(1, "cd", true)), IsTrue());
  ASSERT_THAT(queue.enqueue(makeFrame(3, "ef")), IsTrue());

  const std::vector<Visit> expected{{1, 2}, {3, 2}, {1, 2}};
  EXPECT_THAT(visitUntilEmpty(queue), Eq(expected));
}

TEST(FifoStreamQueueTest, RemovedElementsLoseOnlyTheirOwnTurns) {
  FifoStreamQueue queue(4);
  ASSERT_THAT(queue.enqueue(makeFrame(1, "abcdefghij")), IsTrue());
  ASSERT_THAT(queue.enqueue(makeRequestResponse(3, "k")), IsTrue());
  ASSERT_THAT(queue.enqueue(makeRequestResponse(5, "l")), IsTrue());
  ASSERT_THAT(queue.enqueue(makeFrame(7, "mnopqr")), IsTrue());
  ASSERT_THAT(queue.enqueue(makeRequestResponse(9, "s")), IsTrue());

  std::vector<Visit> actual;
  const std::optional<Visit> first = visitNext(queue);
  ASSERT_THAT(first.has_value(), IsTrue());
  actual.push_back(*first);
  ASSERT_THAT(queue.removeStream(3), IsTrue());
  ASSERT_THAT(queue.removeStream(5), IsTrue());
  ASSERT_THAT(queue.removeStream(1), IsTrue());
  const std::vector<Visit> remaining = visitUntilEmpty(queue);
  actual.insert(actual.end(), remaining.begin(), remaining.end());

  const std::vector<Visit> expected{{1, 4}, {7, 4}, {7, 2}, {9, 1}};
  EXPECT_THAT(actual, Eq(expected));
}

TEST(FifoStreamQueueTest, PositionsWrapPastThe32BitLimit) {
  FifoStreamQueue queue(4);
  ASSERT_THAT(
      queue.setNextPositionForTest(std::numeric_limits<uint32_t>::max() - 2),
      IsTrue());

  ASSERT_THAT(queue.enqueue(makeRequestResponse(1, "a")), IsTrue());
  ASSERT_THAT(queue.enqueue(makeFrame(3, "bcde")), IsTrue());
  ASSERT_THAT(queue.enqueue(makeRequestResponse(5, "f")), IsTrue());
  ASSERT_THAT(queue.enqueue(makeFrame(7, "ghij")), IsTrue());
  ASSERT_THAT(queue.enqueue(makeRequestResponse(9, "k")), IsTrue());
  ASSERT_THAT(queue.enqueue(makeFrame(11, "lmno")), IsTrue());
  ASSERT_THAT(queue.enqueue(makeRequestResponse(13, "p")), IsTrue());
  ASSERT_THAT(queue.enqueue(makeFrame(15, "qrst")), IsTrue());
  EXPECT_THAT(queue.setNextPositionForTest(0), IsFalse());

  ASSERT_THAT(queue.removeStream(9), IsTrue());
  ASSERT_THAT(queue.removeStream(15), IsTrue());

  const std::vector<Visit> expected{
      {1, 1}, {3, 4}, {5, 1}, {7, 4}, {11, 4}, {13, 1}};
  EXPECT_THAT(visitUntilEmpty(queue), Eq(expected));
}

} // namespace
} // namespace apache::thrift::fast_thrift::frame::write
