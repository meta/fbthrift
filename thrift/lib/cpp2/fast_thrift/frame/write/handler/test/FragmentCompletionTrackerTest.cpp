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

#include <thrift/lib/cpp2/fast_thrift/frame/write/handler/FragmentCompletionTracker.h>

#include <thrift/lib/cpp2/fast_thrift/transport/WriteCompletion.h>

#include <gtest/gtest.h>
#include <folly/portability/GMock.h>

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

namespace apache::thrift::fast_thrift::frame::write::handler {
namespace {

using namespace testing;

struct TestBatchWriteCompleteEvent
    : channel_pipeline::EventTag<TestBatchWriteCompleteEvent> {
  transport::WriteCompletionStatus status;
  size_t frameCount;
  size_t bytes;
  bool quiesced;
};

struct TestFrameWriteCompleteEvent
    : channel_pipeline::EventTag<TestFrameWriteCompleteEvent> {
  uint32_t streamId;
  transport::WriteCompletionStatus status;
  bool quiesced;

  bool operator==(const TestFrameWriteCompleteEvent& other) const noexcept {
    return streamId == other.streamId && status == other.status &&
        quiesced == other.quiesced;
  }
};

struct TestEventFactory {
  using BatchWriteCompleteEventType = TestBatchWriteCompleteEvent;
  using FrameWriteCompleteEventType = TestFrameWriteCompleteEvent;

  static TestFrameWriteCompleteEvent makeFrameWriteComplete(
      transport::WriteCompletionStatus status,
      uint32_t streamId,
      bool quiesced) noexcept {
    return {
        .streamId = streamId,
        .status = status,
        .quiesced = quiesced,
    };
  }
};

static_assert(
    FrameWriteCompleteEventFactory<TestEventFactory>,
    "TestEventFactory must satisfy FrameWriteCompleteEventFactory concept");

class CapturingContext {
 public:
  template <channel_pipeline::PipelineEvent E>
  void fireEvent(const typename E::Payload& event) noexcept {
    static_assert(std::same_as<E, TestFrameWriteCompleteEvent>);
    events_.push_back(event);
  }

  const std::vector<TestFrameWriteCompleteEvent>& events() const noexcept {
    return events_;
  }

 private:
  std::vector<TestFrameWriteCompleteEvent> events_;
};

// Helper: build a BatchWriteComplete box (what the downstream batch tracker
// would fire).
TestBatchWriteCompleteEvent batchWriteComplete(
    transport::WriteCompletionStatus status,
    size_t frameCount,
    size_t bytes,
    bool quiesced = false) noexcept {
  return {
      .status = status,
      .frameCount = frameCount,
      .bytes = bytes,
      .quiesced = quiesced,
  };
}

} // namespace

TEST(FragmentCompletionTrackerTest, FragmentedFrameFiresOnceOnLastFragment) {
  FragmentCompletionTrackerT<TestEventFactory> tracker;
  CapturingContext ctx;

  // One frame split into three fragments: only the last completes the frame.
  tracker.onFragment(7, /*isLastFragment=*/false);
  tracker.onFragment(7, /*isLastFragment=*/false);
  tracker.onFragment(7, /*isLastFragment=*/true);
  tracker.on<TestBatchWriteCompleteEvent>(
      ctx,
      batchWriteComplete(transport::WriteCompletionStatus::Success, 3, 0),
      /*handlerHasPendingWrites=*/false);

  ASSERT_EQ(ctx.events().size(), 1u);
  EXPECT_EQ(ctx.events()[0].streamId, 7u);
  EXPECT_EQ(ctx.events()[0].status, transport::WriteCompletionStatus::Success);
}

TEST(FragmentCompletionTrackerTest, UnfragmentedFramesEachFireOnce) {
  FragmentCompletionTrackerT<TestEventFactory> tracker;
  CapturingContext ctx;

  // Two unfragmented frames batched together: each is its own completed frame.
  tracker.onFragment(1, /*isLastFragment=*/true);
  tracker.onFragment(2, /*isLastFragment=*/true);
  tracker.on<TestBatchWriteCompleteEvent>(
      ctx,
      batchWriteComplete(transport::WriteCompletionStatus::Success, 2, 0),
      /*handlerHasPendingWrites=*/false);

  ASSERT_EQ(ctx.events().size(), 2u);
  EXPECT_EQ(ctx.events()[0].streamId, 1u);
  EXPECT_EQ(ctx.events()[1].streamId, 2u);
}

TEST(FragmentCompletionTrackerTest, MixedFragmentsResolveToOriginalFrames) {
  FragmentCompletionTrackerT<TestEventFactory> tracker;
  CapturingContext ctx;

  // Batch of 4 fragments = 2 original frames: stream 1 (split in two) and
  // stream 2 (split in two). Two FrameWriteCompletes, in stream order.
  tracker.onFragment(1, /*isLastFragment=*/false);
  tracker.onFragment(1, /*isLastFragment=*/true);
  tracker.onFragment(2, /*isLastFragment=*/false);
  tracker.onFragment(2, /*isLastFragment=*/true);
  tracker.on<TestBatchWriteCompleteEvent>(
      ctx,
      batchWriteComplete(transport::WriteCompletionStatus::Success, 4, 0),
      /*handlerHasPendingWrites=*/false);

  ASSERT_EQ(ctx.events().size(), 2u);
  EXPECT_EQ(ctx.events()[0].streamId, 1u);
  EXPECT_EQ(ctx.events()[1].streamId, 2u);
}

TEST(FragmentCompletionTrackerTest, BatchBoundaryPopsExactlyFrameCount) {
  FragmentCompletionTrackerT<TestEventFactory> tracker;
  CapturingContext ctx;

  // Two in-flight batches; each BatchWriteComplete must pop only its own
  // frameCount fragments, so the second frame's completion waits for the
  // second event.
  tracker.onFragment(1, /*isLastFragment=*/true); // batch 1: 1 fragment
  tracker.onFragment(2, /*isLastFragment=*/false); // batch 2: 2 fragments
  tracker.onFragment(2, /*isLastFragment=*/true);

  tracker.on<TestBatchWriteCompleteEvent>(
      ctx,
      batchWriteComplete(transport::WriteCompletionStatus::Success, 1, 0),
      /*handlerHasPendingWrites=*/false);
  ASSERT_EQ(ctx.events().size(), 1u);
  EXPECT_EQ(ctx.events()[0].streamId, 1u);

  tracker.on<TestBatchWriteCompleteEvent>(
      ctx,
      batchWriteComplete(transport::WriteCompletionStatus::Success, 2, 0),
      /*handlerHasPendingWrites=*/false);
  ASSERT_EQ(ctx.events().size(), 2u);
  EXPECT_EQ(ctx.events()[1].streamId, 2u);
}

TEST(FragmentCompletionTrackerTest, ErrorStatusPropagatesToFrameEvent) {
  FragmentCompletionTrackerT<TestEventFactory> tracker;
  CapturingContext ctx;

  tracker.onFragment(9, /*isLastFragment=*/true);
  tracker.on<TestBatchWriteCompleteEvent>(
      ctx,
      batchWriteComplete(transport::WriteCompletionStatus::Error, 1, 42),
      /*handlerHasPendingWrites=*/false);

  ASSERT_EQ(ctx.events().size(), 1u);
  EXPECT_EQ(ctx.events()[0].streamId, 9u);
  EXPECT_EQ(ctx.events()[0].status, transport::WriteCompletionStatus::Error);
}

TEST(FragmentCompletionTrackerTest, InterleavedFragmentsCompleteInFifoOrder) {
  FragmentCompletionTrackerT<TestEventFactory> tracker;
  CapturingContext ctx;

  // SRPT can alternate streams, so fragments of two frames arrive interleaved.
  // Completion keys off each record's own isLastFragment in FIFO order: here
  // stream 1's last fragment precedes stream 2's, so stream 1 completes first.
  tracker.onFragment(1, /*isLastFragment=*/false);
  tracker.onFragment(2, /*isLastFragment=*/false);
  tracker.onFragment(1, /*isLastFragment=*/true);
  tracker.onFragment(2, /*isLastFragment=*/true);
  tracker.on<TestBatchWriteCompleteEvent>(
      ctx,
      batchWriteComplete(transport::WriteCompletionStatus::Success, 4, 0),
      /*handlerHasPendingWrites=*/false);

  ASSERT_EQ(ctx.events().size(), 2u);
  EXPECT_EQ(ctx.events()[0].streamId, 1u);
  EXPECT_EQ(ctx.events()[1].streamId, 2u);
}

TEST(FragmentCompletionTrackerTest, InterleavedFragmentsCompleteInWireOrder) {
  FragmentCompletionTrackerT<TestEventFactory> tracker;
  CapturingContext ctx;

  // Same interleaving, but stream 2's last fragment lands before stream 1's:
  // completions follow wire order, so stream 2 fires first.
  tracker.onFragment(1, /*isLastFragment=*/false);
  tracker.onFragment(2, /*isLastFragment=*/false);
  tracker.onFragment(2, /*isLastFragment=*/true);
  tracker.onFragment(1, /*isLastFragment=*/true);
  tracker.on<TestBatchWriteCompleteEvent>(
      ctx,
      batchWriteComplete(transport::WriteCompletionStatus::Success, 4, 0),
      /*handlerHasPendingWrites=*/false);

  ASSERT_EQ(ctx.events().size(), 2u);
  EXPECT_EQ(ctx.events()[0].streamId, 2u);
  EXPECT_EQ(ctx.events()[1].streamId, 1u);
}

TEST(FragmentCompletionTrackerTest, ErrorBeforeLastFragmentFiresNoCompletion) {
  FragmentCompletionTrackerT<TestEventFactory> tracker;
  CapturingContext ctx;

  // A frame that errored mid-fragmentation: only its non-last fragments were
  // ever recorded (the handler skips onFragment for the fragment that hit
  // Result::Error). The Error BatchWriteComplete pops those records but fires
  // nothing, since none is marked isLastFragment — the partially-written frame
  // receives NO FrameWriteComplete. Documents current behavior.
  tracker.onFragment(5, /*isLastFragment=*/false);
  tracker.onFragment(5, /*isLastFragment=*/false);
  tracker.on<TestBatchWriteCompleteEvent>(
      ctx,
      batchWriteComplete(transport::WriteCompletionStatus::Error, 2, 0),
      /*handlerHasPendingWrites=*/false);

  EXPECT_TRUE(ctx.events().empty());
}

TEST(FragmentCompletionTrackerTest, AbortedFragmentRemovesItsOwnRecord) {
  FragmentCompletionTrackerT<TestEventFactory> tracker;
  CapturingContext ctx;

  tracker.onFragment(1, /*isLastFragment=*/true);
  tracker.onFragment(3, /*isLastFragment=*/true);
  tracker.onFragmentAborted();
  tracker.onFragment(5, /*isLastFragment=*/true);
  tracker.on<TestBatchWriteCompleteEvent>(
      ctx,
      batchWriteComplete(transport::WriteCompletionStatus::Success, 2, 0),
      /*handlerHasPendingWrites=*/false);

  const std::vector<TestFrameWriteCompleteEvent> expected{
      {
          .streamId = 1,
          .status = transport::WriteCompletionStatus::Success,
          .quiesced = false,
      },
      {
          .streamId = 5,
          .status = transport::WriteCompletionStatus::Success,
          .quiesced = false,
      }};
  EXPECT_THAT(ctx.events(), Eq(expected));
}

TEST(FragmentCompletionTrackerTest, AbortAfterCompletionLeavesTrackerUsable) {
  FragmentCompletionTrackerT<TestEventFactory> tracker;
  CapturingContext ctx;

  tracker.onFragment(1, /*isLastFragment=*/true);
  tracker.on<TestBatchWriteCompleteEvent>(
      ctx,
      batchWriteComplete(transport::WriteCompletionStatus::Success, 1, 0),
      /*handlerHasPendingWrites=*/false);
  tracker.onFragmentAborted();
  tracker.onFragment(3, /*isLastFragment=*/true);
  tracker.on<TestBatchWriteCompleteEvent>(
      ctx,
      batchWriteComplete(transport::WriteCompletionStatus::Success, 1, 0),
      /*handlerHasPendingWrites=*/false);

  const std::vector<TestFrameWriteCompleteEvent> expected{
      {
          .streamId = 1,
          .status = transport::WriteCompletionStatus::Success,
          .quiesced = false,
      },
      {
          .streamId = 3,
          .status = transport::WriteCompletionStatus::Success,
          .quiesced = false,
      }};
  EXPECT_THAT(ctx.events(), Eq(expected));
}

// The batcher can only speak for itself. When this handler is still holding
// frames the batcher never saw, the connection is not caught up and the
// batch's verdict has to be overridden.
TEST(FragmentCompletionTrackerTest, PendingWritesInHandlerSuppressQuiescence) {
  FragmentCompletionTrackerT<TestEventFactory> tracker;
  CapturingContext ctx;

  tracker.onFragment(1, /*isLastFragment=*/true);
  tracker.on<TestBatchWriteCompleteEvent>(
      ctx,
      batchWriteComplete(
          transport::WriteCompletionStatus::Success, 1, 0, /*quiesced=*/true),
      /*handlerHasPendingWrites=*/true);

  ASSERT_EQ(ctx.events().size(), 1u);
  EXPECT_FALSE(ctx.events()[0].quiesced);
}

TEST(FragmentCompletionTrackerTest, QuiescenceReportedWhenBothAreDrained) {
  FragmentCompletionTrackerT<TestEventFactory> tracker;
  CapturingContext ctx;

  tracker.onFragment(1, /*isLastFragment=*/true);
  tracker.on<TestBatchWriteCompleteEvent>(
      ctx,
      batchWriteComplete(
          transport::WriteCompletionStatus::Success, 1, 0, /*quiesced=*/true),
      /*handlerHasPendingWrites=*/false);

  ASSERT_EQ(ctx.events().size(), 1u);
  EXPECT_TRUE(ctx.events()[0].quiesced);
}

// Quiescence describes the connection, not a frame, so a batch carrying
// several frames must not repeat the edge once per frame.
TEST(FragmentCompletionTrackerTest, QuiescenceRidesOnlyOnTheLastFrameOfABatch) {
  FragmentCompletionTrackerT<TestEventFactory> tracker;
  CapturingContext ctx;

  tracker.onFragment(1, /*isLastFragment=*/true);
  tracker.onFragment(3, /*isLastFragment=*/true);
  tracker.onFragment(5, /*isLastFragment=*/true);
  tracker.on<TestBatchWriteCompleteEvent>(
      ctx,
      batchWriteComplete(
          transport::WriteCompletionStatus::Success, 3, 0, /*quiesced=*/true),
      /*handlerHasPendingWrites=*/false);

  ASSERT_EQ(ctx.events().size(), 3u);
  EXPECT_FALSE(ctx.events()[0].quiesced);
  EXPECT_FALSE(ctx.events()[1].quiesced);
  EXPECT_TRUE(ctx.events()[2].quiesced);
  // Order is still write order — deferring the last fire must not reorder.
  EXPECT_EQ(ctx.events()[0].streamId, 1u);
  EXPECT_EQ(ctx.events()[1].streamId, 3u);
  EXPECT_EQ(ctx.events()[2].streamId, 5u);
}

// A batch whose fragments all belong to frames still mid-fragmentation
// completes no frame, so there is no event to carry the edge on. That is
// consistent: unfinished fragments mean the handler is holding writes.
TEST(FragmentCompletionTrackerTest, BatchCompletingNoFrameFiresNothing) {
  FragmentCompletionTrackerT<TestEventFactory> tracker;
  CapturingContext ctx;

  tracker.onFragment(1, /*isLastFragment=*/false);
  tracker.on<TestBatchWriteCompleteEvent>(
      ctx,
      batchWriteComplete(
          transport::WriteCompletionStatus::Success, 1, 0, /*quiesced=*/true),
      /*handlerHasPendingWrites=*/true);

  EXPECT_TRUE(ctx.events().empty());
}

} // namespace apache::thrift::fast_thrift::frame::write::handler
