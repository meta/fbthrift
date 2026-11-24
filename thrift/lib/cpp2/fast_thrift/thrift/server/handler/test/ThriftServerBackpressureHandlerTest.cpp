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

#include <thrift/lib/cpp2/fast_thrift/thrift/server/handler/ThriftServerBackpressureHandler.h>

#include <vector>

#include <gtest/gtest.h>

#include <folly/io/async/EventBase.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/HandlerTag.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/StaticPipelineBuilder.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/test/MockAdapters.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/Messages.h>

namespace apache::thrift::fast_thrift::thrift {
namespace {

namespace cp = channel_pipeline;
using cp::erase_and_box;
using cp::Result;
using cp::TypeErasedBox;

struct FakePipeline {
  size_t onReadReadyCalls{0};
  void onReadReady() noexcept { ++onReadReadyCalls; }
};

struct FakeContext {
  template <typename T>
  T& state() noexcept {
    static_assert(std::same_as<T, ReadBackpressureArbiter>);
    return arbiter;
  }

  Result fireRead(TypeErasedBox&&) noexcept {
    ++readCalls;
    return nextReadResult;
  }

  Result fireWrite(TypeErasedBox&& msg) noexcept {
    writtenStreamIds.push_back(msg.get<ThriftServerResponseMessage>()
                                   .payload.get<ThriftInitialResponsePayload>()
                                   .streamId);
    return nextWriteResult;
  }

  void awaitWriteReady() noexcept { ++awaitWriteReadyCalls; }
  void cancelAwaitWriteReady() noexcept { ++cancelAwaitWriteReadyCalls; }
  FakePipeline* pipeline() noexcept { return &fakePipeline; }

  Result nextWriteResult{Result::Success};
  Result nextReadResult{Result::Success};
  size_t readCalls{0};
  size_t awaitWriteReadyCalls{0};
  size_t cancelAwaitWriteReadyCalls{0};
  std::vector<uint32_t> writtenStreamIds;
  FakePipeline fakePipeline;
  ReadBackpressureArbiter arbiter;
};

ThriftServerResponseMessage makeResponse(uint32_t streamId) {
  return ThriftServerResponseMessage{
      .payload = ThriftInitialResponsePayload{.streamId = streamId}};
}

TEST(
    ThriftServerBackpressureHandlerTest, ResponsesContinueWhileReadsArePaused) {
  ThriftServerBackpressureHandler<FakeContext> handler;
  FakeContext ctx;
  ctx.nextWriteResult = Result::Backpressure;

  for (uint32_t streamId : {1u, 2u, 3u}) {
    ctx.nextWriteResult =
        streamId == 2 ? Result::Success : Result::Backpressure;
    EXPECT_EQ(
        handler.onWrite(ctx, erase_and_box(makeResponse(streamId))),
        Result::Success);
  }

  EXPECT_EQ(ctx.writtenStreamIds, (std::vector<uint32_t>{1, 2, 3}));
  EXPECT_TRUE(handler.isBackpressured());
  EXPECT_EQ(ctx.arbiter.holderCount(), 1);
  EXPECT_EQ(ctx.awaitWriteReadyCalls, 1);
  EXPECT_EQ(ctx.fakePipeline.onReadReadyCalls, 0);
  EXPECT_EQ(
      handler.onRead(ctx, erase_and_box(ThriftServerRequestMessage{})),
      Result::Backpressure);
  EXPECT_EQ(ctx.readCalls, 1);

  handler.onWriteReady(ctx);
  handler.onWriteReady(ctx);

  EXPECT_FALSE(handler.isBackpressured());
  EXPECT_EQ(ctx.arbiter.holderCount(), 0);
  EXPECT_EQ(ctx.fakePipeline.onReadReadyCalls, 1);
  EXPECT_EQ(ctx.writtenStreamIds, (std::vector<uint32_t>{1, 2, 3}));
  EXPECT_EQ(
      handler.onRead(ctx, erase_and_box(ThriftServerRequestMessage{})),
      Result::Success);
}

TEST(ThriftServerBackpressureHandlerTest, ReadinessWaitsForOtherPauseOwners) {
  ThriftServerBackpressureHandler<FakeContext> handler;
  FakeContext ctx;
  ctx.arbiter.acquire();
  ctx.nextWriteResult = Result::Backpressure;
  (void)handler.onWrite(ctx, erase_and_box(makeResponse(1)));

  handler.onWriteReady(ctx);

  EXPECT_EQ(ctx.arbiter.holderCount(), 1);
  EXPECT_EQ(ctx.fakePipeline.onReadReadyCalls, 0);
}

TEST(
    ThriftServerBackpressureHandlerTest, InactiveReleasesPauseWithoutResuming) {
  ThriftServerBackpressureHandler<FakeContext> handler;
  FakeContext ctx;
  ctx.nextWriteResult = Result::Backpressure;
  (void)handler.onWrite(ctx, erase_and_box(makeResponse(1)));

  handler.onPipelineInactive(ctx);
  handler.onWriteReady(ctx);

  EXPECT_FALSE(handler.isBackpressured());
  EXPECT_EQ(ctx.arbiter.holderCount(), 0);
  EXPECT_EQ(ctx.fakePipeline.onReadReadyCalls, 0);
  EXPECT_EQ(ctx.cancelAwaitWriteReadyCalls, 1);
}

TEST(ThriftServerBackpressureHandlerTest, ErrorsRemainTerminalUnderPressure) {
  ThriftServerBackpressureHandler<FakeContext> handler;
  FakeContext ctx;
  ctx.nextWriteResult = Result::Backpressure;
  (void)handler.onWrite(ctx, erase_and_box(makeResponse(1)));

  ctx.nextReadResult = Result::Error;
  EXPECT_EQ(
      handler.onRead(ctx, erase_and_box(ThriftServerRequestMessage{})),
      Result::Error);
  ctx.nextWriteResult = Result::Error;
  EXPECT_EQ(
      handler.onWrite(ctx, erase_and_box(makeResponse(2))), Result::Error);
  EXPECT_EQ(ctx.arbiter.holderCount(), 1);
}

#if defined(FAST_THRIFT_SERVER_BACKPRESSURE_PIPELINE_TESTS)
HANDLER_TAG(read_backpressure);

TEST(
    ThriftServerBackpressureHandlerPipelineTest,
    WritesFlowWhileReadsArePaused) {
  folly::EventBase evb;
  cp::test::StaticMockHeadHandler head;
  cp::test::StaticMockTailHandler tail;
  cp::test::TestAllocator allocator;
  ReadBackpressureArbiter arbiter;
  auto pipeline = cp::StaticPipelineBuilder<
                      cp::test::StaticMockHeadHandler,
                      cp::test::StaticMockTailHandler,
                      cp::test::TestAllocator>()
                      .setEventBase(&evb)
                      .setHead(&head)
                      .setTail(&tail)
                      .setAllocator(&allocator)
                      .addNextDuplexTemplate<ThriftServerBackpressureHandler>(
                          read_backpressure_tag, &arbiter)
                      .build();
  head.setOnWriteCallback([](TypeErasedBox&&) { return Result::Backpressure; });

  for (uint32_t streamId : {1u, 2u, 3u}) {
    EXPECT_EQ(
        pipeline->fireWrite(erase_and_box(makeResponse(streamId))),
        Result::Success);
  }
  EXPECT_EQ(head.writeCount(), 3);
  EXPECT_EQ(
      pipeline->fireRead(erase_and_box(ThriftServerRequestMessage{})),
      Result::Backpressure);
  EXPECT_TRUE(pipeline->hasPendingWriteReady());
  EXPECT_EQ(head.onReadReadyCount(), 0);

  pipeline->onWriteReady();

  EXPECT_EQ(head.writeCount(), 3);
  EXPECT_EQ(head.onReadReadyCount(), 1);
  EXPECT_FALSE(pipeline->hasPendingWriteReady());
  EXPECT_EQ(
      pipeline->fireRead(erase_and_box(ThriftServerRequestMessage{})),
      Result::Success);
}
#endif

} // namespace
} // namespace apache::thrift::fast_thrift::thrift
