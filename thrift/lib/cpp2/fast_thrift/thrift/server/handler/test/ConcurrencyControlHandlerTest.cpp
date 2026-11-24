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

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/HandlerTag.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/StaticPipelineBuilder.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/test/MockAdapters.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/handler/ConcurrencyControlHandler.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/util/ConcurrencyControlState.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>

#include <folly/io/async/EventBase.h>
#include <folly/portability/GTest.h>

namespace apache::thrift::fast_thrift::thrift::test {
namespace {

using channel_pipeline::erase_and_box;
using channel_pipeline::EventTag;
using channel_pipeline::Result;
using channel_pipeline::StaticPipelineBuilder;
using channel_pipeline::TypeErasedBox;
using channel_pipeline::test::StaticMockHeadHandler;
using channel_pipeline::test::StaticMockTailHandler;
using channel_pipeline::test::TestAllocator;

HANDLER_TAG(concurrency);

struct TestRequestCompletedEvent : EventTag<> {};

struct TestPipelineResources {
  folly::EventBase& eventBase;
  StaticMockHeadHandler& head;
  StaticMockTailHandler& tail;
  TestAllocator& allocator;
  ConcurrencyControlStateHolder& holder;
};

template <typename CompletionEvent>
struct TestHandlerTemplate {
  template <typename Context>
  using Handler = ConcurrencyControlHandler<Context, CompletionEvent>;
};

template <typename CompletionEvent = TestRequestCompletedEvent>
auto buildTestPipeline(
    TestPipelineResources resources,
    uint64_t maxActive,
    uint64_t maxOutstanding) {
  return StaticPipelineBuilder<
             StaticMockHeadHandler,
             StaticMockTailHandler,
             TestAllocator>()
      .setEventBase(&resources.eventBase)
      .setHead(&resources.head)
      .setTail(&resources.tail)
      .setAllocator(&resources.allocator)
      .template addNextDuplexTemplate<
          TestHandlerTemplate<CompletionEvent>::template Handler>(
          concurrency_tag,
          maxActive,
          maxOutstanding,
          std::ref(resources.holder))
      .build();
}

struct TrackedMessage {
  explicit TrackedMessage(std::shared_ptr<size_t> destroyed)
      : destroyed(std::move(destroyed)) {}

  TrackedMessage(TrackedMessage&&) noexcept = default;
  TrackedMessage& operator=(TrackedMessage&&) noexcept = delete;
  TrackedMessage(const TrackedMessage&) = delete;
  TrackedMessage& operator=(const TrackedMessage&) = delete;

  ~TrackedMessage() {
    if (destroyed) {
      ++*destroyed;
    }
  }

  std::shared_ptr<size_t> destroyed;
};

class ConcurrencyControlHandlerTest : public ::testing::Test {
 protected:
  using PipelinePtr =
      decltype(buildTestPipeline(std::declval<TestPipelineResources>(), 0, 0));
  using Handler =
      std::remove_reference_t<decltype(std::declval<PipelinePtr&>()->handler(
          concurrency_tag))>;

  PipelinePtr build(uint64_t maxActive, uint64_t maxOutstanding) {
    return build(maxActive, maxOutstanding, holder_);
  }

  PipelinePtr build(
      uint64_t maxActive,
      uint64_t maxOutstanding,
      ConcurrencyControlStateHolder& holder) {
    return build(maxActive, maxOutstanding, holder, eventBase_);
  }

  PipelinePtr build(
      uint64_t maxActive,
      uint64_t maxOutstanding,
      ConcurrencyControlStateHolder& holder,
      folly::EventBase& eventBase) {
    tail_.setOnReadCallback([this](TypeErasedBox&& msg) {
      reads_.push_back(msg.take<uint32_t>());
      return readResult_;
    });
    head_.setOnWriteCallback([](TypeErasedBox&&) { return Result::Success; });
    auto pipeline = buildTestPipeline(
        {eventBase, head_, tail_, allocator_, holder},
        maxActive,
        maxOutstanding);
    handler_ = &pipeline->handler(concurrency_tag);
    return pipeline;
  }

  static TypeErasedBox message(uint32_t value) { return erase_and_box(value); }

  folly::EventBase eventBase_;
  ConcurrencyControlStateHolder holder_;
  StaticMockHeadHandler head_;
  StaticMockTailHandler tail_;
  TestAllocator allocator_;
  Handler* handler_{nullptr};
  Result readResult_{Result::Success};
  std::vector<uint32_t> reads_;
};

TEST_F(ConcurrencyControlHandlerTest, FastPathAndCoalescedFifoDrain) {
  auto pipeline = build(2, 4);

  EXPECT_EQ(pipeline->fireRead(message(1)), Result::Success);
  EXPECT_EQ(pipeline->fireRead(message(2)), Result::Success);
  EXPECT_EQ(pipeline->fireRead(message(3)), Result::Success);
  EXPECT_EQ(pipeline->fireRead(message(4)), Result::Backpressure);
  EXPECT_EQ(reads_, (std::vector<uint32_t>{1, 2}));
  EXPECT_EQ(handler_->admitted(), 4);
  EXPECT_EQ(handler_->released(), 0);
  EXPECT_EQ(handler_->outstanding(), 4);
  EXPECT_EQ(handler_->active(), 2);
  EXPECT_EQ(handler_->pending(), 2);

  EXPECT_EQ(pipeline->fireWrite(message(101)), Result::Success);
  EXPECT_EQ(pipeline->fireWrite(message(102)), Result::Success);
  EXPECT_EQ(reads_, (std::vector<uint32_t>{1, 2}));

  eventBase_.loopOnce(EVLOOP_NONBLOCK);

  EXPECT_EQ(reads_, (std::vector<uint32_t>{1, 2, 3, 4}));
  EXPECT_EQ(head_.onReadReadyCount(), 1);
  EXPECT_EQ(handler_->released(), 2);
  EXPECT_EQ(handler_->outstanding(), 2);
  EXPECT_EQ(handler_->active(), 2);
  EXPECT_EQ(handler_->pending(), 0);

  EXPECT_EQ(pipeline->fireWrite(message(103)), Result::Success);
  EXPECT_EQ(pipeline->fireWrite(message(104)), Result::Success);
  EXPECT_EQ(handler_->admitted(), 4);
  EXPECT_EQ(handler_->released(), 4);
  EXPECT_EQ(handler_->outstanding(), 0);
  EXPECT_EQ(handler_->active(), 0);
  EXPECT_EQ(handler_->pending(), 0);
}

TEST_F(ConcurrencyControlHandlerTest, ExactActiveLimitBackpressures) {
  auto pipeline = build(2, 2);

  EXPECT_EQ(pipeline->fireRead(message(1)), Result::Success);
  EXPECT_EQ(pipeline->fireRead(message(2)), Result::Backpressure);
  EXPECT_EQ(handler_->active(), 2);
  EXPECT_EQ(handler_->pending(), 0);

  EXPECT_EQ(pipeline->fireWrite(message(101)), Result::Success);
  EXPECT_EQ(head_.onReadReadyCount(), 0);
  eventBase_.loopOnce(EVLOOP_NONBLOCK);
  EXPECT_EQ(head_.onReadReadyCount(), 1);
}

TEST_F(
    ConcurrencyControlHandlerTest, DownstreamBackpressureResumesFromReadReady) {
  auto pipeline = build(2, 4);
  readResult_ = Result::Backpressure;

  EXPECT_EQ(pipeline->fireRead(message(1)), Result::Backpressure);
  EXPECT_TRUE(pipeline->hasPendingReadReady());

  readResult_ = Result::Success;
  EXPECT_EQ(pipeline->fireRead(message(2)), Result::Backpressure);
  EXPECT_EQ(reads_, (std::vector<uint32_t>{1}));
  EXPECT_EQ(handler_->pending(), 1);

  pipeline->onReadReady();

  EXPECT_FALSE(pipeline->hasPendingReadReady());
  EXPECT_EQ(reads_, (std::vector<uint32_t>{1, 2}));
  EXPECT_EQ(handler_->pending(), 0);
}

TEST_F(
    ConcurrencyControlHandlerTest,
    DrainBackpressureStopsAndReadReadyResumesInFifoOrder) {
  auto pipeline = build(2, 5);

  EXPECT_EQ(pipeline->fireRead(message(1)), Result::Success);
  EXPECT_EQ(pipeline->fireRead(message(2)), Result::Success);
  EXPECT_EQ(pipeline->fireRead(message(3)), Result::Success);
  EXPECT_EQ(pipeline->fireRead(message(4)), Result::Success);
  EXPECT_EQ(pipeline->fireRead(message(5)), Result::Backpressure);

  tail_.setOnReadCallback([this](TypeErasedBox&& msg) {
    const auto value = msg.take<uint32_t>();
    reads_.push_back(value);
    return value == 4 ? Result::Backpressure : Result::Success;
  });

  EXPECT_EQ(pipeline->fireWrite(message(101)), Result::Success);
  EXPECT_EQ(pipeline->fireWrite(message(102)), Result::Success);
  eventBase_.loopOnce(EVLOOP_NONBLOCK);

  EXPECT_EQ(reads_, (std::vector<uint32_t>{1, 2, 3, 4}));
  EXPECT_TRUE(pipeline->hasPendingReadReady());
  EXPECT_EQ(handler_->pending(), 1);
  EXPECT_EQ(head_.onReadReadyCount(), 0);

  EXPECT_EQ(pipeline->fireWrite(message(103)), Result::Success);
  eventBase_.loopOnce(EVLOOP_NONBLOCK);
  EXPECT_EQ(reads_, (std::vector<uint32_t>{1, 2, 3, 4}));
  EXPECT_EQ(handler_->pending(), 1);

  tail_.setOnReadCallback([this](TypeErasedBox&& msg) {
    reads_.push_back(msg.take<uint32_t>());
    return Result::Success;
  });
  pipeline->onReadReady();

  EXPECT_FALSE(pipeline->hasPendingReadReady());
  EXPECT_EQ(reads_, (std::vector<uint32_t>{1, 2, 3, 4, 5}));
  EXPECT_EQ(handler_->pending(), 0);
}

TEST_F(ConcurrencyControlHandlerTest, FullQueueFiresException) {
  auto pipeline = build(1, 2);

  EXPECT_EQ(pipeline->fireRead(message(1)), Result::Success);
  EXPECT_EQ(pipeline->fireRead(message(2)), Result::Backpressure);
  EXPECT_EQ(pipeline->fireRead(message(3)), Result::Error);

  EXPECT_EQ(tail_.exceptionCount(), 1);
  EXPECT_EQ(handler_->admitted(), 2);
  EXPECT_EQ(handler_->released(), 2);
  EXPECT_EQ(handler_->outstanding(), 0);
  EXPECT_EQ(handler_->active(), 0);
  EXPECT_EQ(handler_->pending(), 0);
}

TEST_F(
    ConcurrencyControlHandlerTest,
    DrainErrorReleasesCapacityAndQueuedMessages) {
  auto pipeline = build(2, 4);

  EXPECT_EQ(pipeline->fireRead(message(1)), Result::Success);
  EXPECT_EQ(pipeline->fireRead(message(2)), Result::Success);
  EXPECT_EQ(pipeline->fireRead(message(3)), Result::Success);
  EXPECT_EQ(pipeline->fireRead(message(4)), Result::Backpressure);

  tail_.setOnReadCallback([this](TypeErasedBox&& msg) {
    reads_.push_back(msg.take<uint32_t>());
    return Result::Error;
  });

  EXPECT_EQ(pipeline->fireWrite(message(101)), Result::Success);
  EXPECT_EQ(pipeline->fireWrite(message(102)), Result::Success);
  eventBase_.loopOnce(EVLOOP_NONBLOCK);

  EXPECT_EQ(reads_, (std::vector<uint32_t>{1, 2, 3}));
  EXPECT_FALSE(pipeline->hasPendingReadReady());
  EXPECT_EQ(handler_->pending(), 0);
  EXPECT_EQ(handler_->outstanding(), 0);
  EXPECT_EQ(handler_->active(), 0);
  EXPECT_EQ(head_.onReadReadyCount(), 0);

  pipeline->fireException(
      folly::make_exception_wrapper<std::runtime_error>("read failed"));
  EXPECT_EQ(tail_.exceptionCount(), 1);
  EXPECT_EQ(handler_->pending(), 0);
}

TEST_F(ConcurrencyControlHandlerTest, ReentrantWriteMaintainsCounters) {
  auto pipeline = build(2, 4);
  auto* pipelinePtr = pipeline.get();
  tail_.setOnReadCallback([this, pipelinePtr](TypeErasedBox&& msg) {
    const auto value = msg.take<uint32_t>();
    reads_.push_back(value);
    EXPECT_EQ(pipelinePtr->fireWrite(message(100 + value)), Result::Success);
    return Result::Success;
  });

  EXPECT_EQ(pipeline->fireRead(message(1)), Result::Success);
  EXPECT_EQ(pipeline->fireRead(message(2)), Result::Success);

  EXPECT_EQ(reads_, (std::vector<uint32_t>{1, 2}));
  EXPECT_EQ(handler_->admitted(), 2);
  EXPECT_EQ(handler_->released(), 2);
  EXPECT_EQ(handler_->outstanding(), 0);
  EXPECT_EQ(handler_->active(), 0);
  EXPECT_EQ(handler_->pending(), 0);
  EXPECT_EQ(head_.onReadReadyCount(), 0);
}

TEST_F(
    ConcurrencyControlHandlerTest,
    InactiveCancelsScheduledDrainAndDestroysQueuedMessage) {
  auto pipeline = build(1, 3);
  pipeline->activate();
  auto destroyed = std::make_shared<size_t>(0);
  readResult_ = Result::Backpressure;

  EXPECT_EQ(pipeline->fireRead(message(1)), Result::Backpressure);
  EXPECT_EQ(
      pipeline->fireRead(erase_and_box(TrackedMessage{destroyed})),
      Result::Backpressure);
  ASSERT_TRUE(pipeline->hasPendingReadReady());
  ASSERT_EQ(handler_->pending(), 1);
  ASSERT_EQ(*destroyed, 0);

  EXPECT_EQ(pipeline->fireWrite(message(101)), Result::Success);
  pipeline->deactivate();
  eventBase_.loopOnce(EVLOOP_NONBLOCK);

  EXPECT_EQ(reads_, (std::vector<uint32_t>{1}));
  EXPECT_FALSE(pipeline->hasPendingReadReady());
  EXPECT_EQ(handler_->pending(), 0);
  EXPECT_EQ(*destroyed, 1);
  EXPECT_EQ(head_.onReadReadyCount(), 0);
}

TEST_F(
    ConcurrencyControlHandlerTest,
    ExceptionCancelsScheduledDrainAndDestroysQueuedMessage) {
  auto pipeline = build(1, 3);
  auto destroyed = std::make_shared<size_t>(0);

  EXPECT_EQ(pipeline->fireRead(message(1)), Result::Success);
  EXPECT_EQ(
      pipeline->fireRead(erase_and_box(TrackedMessage{destroyed})),
      Result::Success);
  EXPECT_EQ(pipeline->fireWrite(message(101)), Result::Success);

  pipeline->fireException(
      folly::make_exception_wrapper<std::runtime_error>("closed"));
  eventBase_.loopOnce(EVLOOP_NONBLOCK);

  EXPECT_EQ(reads_, (std::vector<uint32_t>{1}));
  EXPECT_EQ(handler_->pending(), 0);
  EXPECT_EQ(*destroyed, 1);
  EXPECT_EQ(tail_.exceptionCount(), 1);
  EXPECT_EQ(head_.onReadReadyCount(), 0);
}

TEST_F(
    ConcurrencyControlHandlerTest,
    RemovalCancelsScheduledDrainAndDestroysQueuedMessage) {
  auto destroyed = std::make_shared<size_t>(0);
  {
    auto pipeline = build(1, 3);

    EXPECT_EQ(pipeline->fireRead(message(1)), Result::Success);
    EXPECT_EQ(
        pipeline->fireRead(erase_and_box(TrackedMessage{destroyed})),
        Result::Success);
    EXPECT_EQ(pipeline->fireWrite(message(101)), Result::Success);

    pipeline->close();
    EXPECT_EQ(reads_, (std::vector<uint32_t>{1}));
    EXPECT_EQ(*destroyed, 1);
    EXPECT_EQ(head_.onReadReadyCount(), 0);
  }
  eventBase_.loopOnce(EVLOOP_NONBLOCK);
  EXPECT_EQ(reads_, (std::vector<uint32_t>{1}));
  EXPECT_EQ(head_.onReadReadyCount(), 0);
  EXPECT_EQ(*destroyed, 1);
}

TEST_F(
    ConcurrencyControlHandlerTest, CloseDuringScheduledDrainReleasesCapacity) {
  auto pipeline = build(1, 3);
  auto& state = holder_.stateFor(eventBase_);
  EXPECT_EQ(pipeline->fireRead(message(1)), Result::Success);
  EXPECT_EQ(pipeline->fireRead(message(2)), Result::Success);

  tail_.setOnReadCallback([&](TypeErasedBox&& msg) {
    reads_.push_back(msg.take<uint32_t>());
    pipeline->close();
    return Result::Success;
  });
  EXPECT_EQ(pipeline->fireWrite(message(101)), Result::Success);
  eventBase_.loopOnce(EVLOOP_NONBLOCK);

  EXPECT_EQ(reads_, (std::vector<uint32_t>{1, 2}));
  EXPECT_TRUE(pipeline->isClosed());
  EXPECT_EQ(state.active(), 0);
  EXPECT_EQ(state.outstanding(), 0);
  EXPECT_TRUE(state.waiters.empty());
  EXPECT_EQ(head_.onReadReadyCount(), 0);
}

TEST_F(ConcurrencyControlHandlerTest, SharedStateThrottlesAcrossPipelines) {
  ConcurrencyControlStateHolder holder;

  StaticMockHeadHandler head2;
  StaticMockTailHandler tail2;
  TestAllocator allocator2;
  std::vector<uint32_t> reads2;
  tail2.setOnReadCallback([&](TypeErasedBox&& msg) {
    reads2.push_back(msg.take<uint32_t>());
    return Result::Success;
  });
  head2.setOnWriteCallback([](TypeErasedBox&&) { return Result::Success; });
  auto pipeline2 =
      buildTestPipeline({eventBase_, head2, tail2, allocator2, holder}, 1, 3);
  auto* handler2Ptr = &pipeline2->handler(concurrency_tag);

  auto pipeline1 = build(1, 3, holder);

  EXPECT_EQ(pipeline1->fireRead(message(1)), Result::Success);
  EXPECT_EQ(pipeline2->fireRead(message(2)), Result::Success);
  EXPECT_EQ(reads_, (std::vector<uint32_t>{1}));
  EXPECT_TRUE(reads2.empty());
  EXPECT_EQ(handler2Ptr->pending(), 1);
  EXPECT_EQ(handler_->active(), 1);

  EXPECT_EQ(pipeline1->fireWrite(message(101)), Result::Success);
  eventBase_.loopOnce(EVLOOP_NONBLOCK);
  EXPECT_EQ(reads2, (std::vector<uint32_t>{2}));
  EXPECT_EQ(handler2Ptr->pending(), 0);
  EXPECT_EQ(handler_->active(), 1);
}

TEST_F(ConcurrencyControlHandlerTest, CloseReleasesSharedCapacity) {
  ConcurrencyControlStateHolder holder;
  auto& state = holder.stateFor(eventBase_);

  {
    auto pipeline = build(1, 3, holder);
    EXPECT_EQ(pipeline->fireRead(message(1)), Result::Success);
    EXPECT_EQ(pipeline->fireRead(message(2)), Result::Success);
    EXPECT_EQ(state.admitted, 2);
    EXPECT_EQ(state.outstanding(), 2);
    EXPECT_EQ(state.active(), 1);
    pipeline->close();
  }

  EXPECT_EQ(state.admitted, 2);
  EXPECT_EQ(state.released, 2);
  EXPECT_EQ(state.dispatched, 1);
  EXPECT_EQ(state.finished, 1);
  EXPECT_EQ(state.outstanding(), 0);
  EXPECT_EQ(state.active(), 0);

  reads_.clear();
  auto pipeline = build(1, 3, holder);
  EXPECT_EQ(pipeline->fireRead(message(3)), Result::Success);
  EXPECT_EQ(reads_, (std::vector<uint32_t>{3}));
}

TEST_F(ConcurrencyControlHandlerTest, QueueRemainsSubscribedWhileActiveIsFull) {
  auto first = build(2, 5);
  auto second = build(2, 5);

  EXPECT_EQ(first->fireRead(message(1)), Result::Success);
  EXPECT_EQ(first->fireRead(message(2)), Result::Success);
  EXPECT_EQ(second->fireRead(message(3)), Result::Success);
  EXPECT_EQ(second->fireRead(message(4)), Result::Success);
  EXPECT_EQ(second->fireRead(message(5)), Result::Backpressure);

  EXPECT_EQ(first->fireWrite(message(101)), Result::Success);
  eventBase_.loopOnce(EVLOOP_NONBLOCK);
  EXPECT_EQ(reads_, (std::vector<uint32_t>{1, 2, 3}));
  EXPECT_EQ(handler_->pending(), 2);

  EXPECT_EQ(first->fireWrite(message(102)), Result::Success);
  eventBase_.loopOnce(EVLOOP_NONBLOCK);
  EXPECT_EQ(reads_, (std::vector<uint32_t>{1, 2, 3, 4}));
  EXPECT_EQ(handler_->pending(), 1);
}

TEST_F(ConcurrencyControlHandlerTest, OutstandingLimitAppliesToFastPath) {
  auto first = build(2, 2);
  readResult_ = Result::Backpressure;
  EXPECT_EQ(first->fireRead(message(1)), Result::Backpressure);
  EXPECT_EQ(first->fireRead(message(2)), Result::Backpressure);

  auto second = build(2, 2);
  readResult_ = Result::Success;
  EXPECT_EQ(second->fireRead(message(3)), Result::Error);
  EXPECT_EQ(reads_, (std::vector<uint32_t>{1}));
  EXPECT_EQ(handler_->outstanding(), 2);
  EXPECT_EQ(handler_->active(), 1);
}

TEST_F(
    ConcurrencyControlHandlerTest, UnmatchedCompletionsPreserveSharedCapacity) {
  auto first = build(1, 3);
  auto second = build(1, 3);

  EXPECT_EQ(second->fireWrite(message(100)), Result::Success);
  EXPECT_EQ(handler_->outstanding(), 0);
  EXPECT_EQ(first->fireRead(message(1)), Result::Success);
  EXPECT_EQ(second->fireRead(message(2)), Result::Success);

  EXPECT_EQ(second->fireWrite(message(102)), Result::Success);
  second->fireEvent<TestRequestCompletedEvent>();
  EXPECT_EQ(handler_->outstanding(), 2);
  EXPECT_EQ(handler_->active(), 1);
  EXPECT_EQ(handler_->released(), 0);
  EXPECT_EQ(handler_->pending(), 1);

  first->fireEvent<TestRequestCompletedEvent>();
  eventBase_.loopOnce(EVLOOP_NONBLOCK);
  EXPECT_EQ(reads_, (std::vector<uint32_t>{1, 2}));
  EXPECT_EQ(handler_->outstanding(), 1);
  EXPECT_EQ(handler_->active(), 1);
  EXPECT_EQ(handler_->released(), 1);
}

TEST_F(
    ConcurrencyControlHandlerTest,
    FailedDrainReleasesCapacityForOtherConnection) {
  auto first = build(1, 3);
  auto second = build(1, 3);
  EXPECT_EQ(first->fireRead(message(1)), Result::Success);
  EXPECT_EQ(second->fireRead(message(2)), Result::Success);

  tail_.setOnReadCallback([this](TypeErasedBox&& msg) {
    const auto value = msg.take<uint32_t>();
    reads_.push_back(value);
    return value == 2 ? Result::Error : Result::Success;
  });
  EXPECT_EQ(first->fireWrite(message(101)), Result::Success);
  eventBase_.loopOnce(EVLOOP_NONBLOCK);
  EXPECT_EQ(handler_->outstanding(), 0);
  EXPECT_EQ(handler_->active(), 0);
  EXPECT_EQ(handler_->released(), 2);

  EXPECT_EQ(first->fireRead(message(3)), Result::Success);
  EXPECT_EQ(reads_, (std::vector<uint32_t>{1, 2, 3}));
  EXPECT_EQ(handler_->active(), 1);
}

TEST_F(ConcurrencyControlHandlerTest, CloseWakesAnotherConnection) {
  auto first = build(1, 3);
  auto second = build(1, 3);
  EXPECT_EQ(first->fireRead(message(1)), Result::Success);
  EXPECT_EQ(second->fireRead(message(2)), Result::Success);

  first->close();
  eventBase_.loopOnce(EVLOOP_NONBLOCK);
  EXPECT_EQ(reads_, (std::vector<uint32_t>{1, 2}));
  EXPECT_EQ(handler_->outstanding(), 1);
  EXPECT_EQ(handler_->active(), 1);
  EXPECT_EQ(handler_->released(), 1);
}

TEST_F(
    ConcurrencyControlHandlerTest, ReadErrorPreservesOtherConnectionCapacity) {
  auto first = build(2, 4);
  auto second = build(2, 4);
  EXPECT_EQ(first->fireRead(message(1)), Result::Success);
  readResult_ = Result::Error;
  EXPECT_EQ(second->fireRead(message(2)), Result::Error);
  EXPECT_EQ(handler_->outstanding(), 1);
  EXPECT_EQ(handler_->active(), 1);
  EXPECT_EQ(handler_->released(), 1);

  EXPECT_EQ(first->fireWrite(message(101)), Result::Success);
  EXPECT_EQ(handler_->outstanding(), 0);
  EXPECT_EQ(handler_->active(), 0);
}

TEST_F(ConcurrencyControlHandlerTest, EventBasesHaveIndependentCapacity) {
  auto first = build(1, 2);
  auto* firstHandler = handler_;
  folly::EventBase otherEventBase;
  auto second = build(1, 2, holder_, otherEventBase);

  EXPECT_EQ(first->fireRead(message(1)), Result::Success);
  EXPECT_EQ(second->fireRead(message(2)), Result::Success);
  EXPECT_EQ(firstHandler->active(), 1);
  EXPECT_EQ(handler_->active(), 1);
  EXPECT_EQ(reads_, (std::vector<uint32_t>{1, 2}));
}

struct RequestCompletedEvent : EventTag<RequestCompletedEvent> {
  uint32_t streamId;
};

TEST_F(
    ConcurrencyControlHandlerTest, CompletionEventWithPayloadReleasesCapacity) {
  tail_.setOnReadCallback([](TypeErasedBox&&) { return Result::Success; });
  auto pipeline = buildTestPipeline<RequestCompletedEvent>(
      {eventBase_, head_, tail_, allocator_, holder_}, 1, 2);
  auto* handlerPtr = &pipeline->handler(concurrency_tag);

  EXPECT_EQ(pipeline->fireRead(message(1)), Result::Success);
  pipeline->fireEvent<RequestCompletedEvent>(
      RequestCompletedEvent{.streamId = 1});
  EXPECT_EQ(handlerPtr->outstanding(), 0);
  EXPECT_EQ(handlerPtr->active(), 0);
  EXPECT_EQ(handlerPtr->released(), 1);
}

} // namespace
} // namespace apache::thrift::fast_thrift::thrift::test
