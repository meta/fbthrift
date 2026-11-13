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

#include <array>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <functional>
#include <memory>
#include <set>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
#include <folly/ExceptionWrapper.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/EventBase.h>
#include <folly/logging/LogCategory.h>
#include <folly/logging/LoggerDB.h>
#include <folly/logging/test/TestLogHandler.h>
#include <folly/portability/GMock.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Event.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Handler.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/HandlerTag.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineBuilder.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/StaticPipelineBuilder.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/test/MockAdapters.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/handler/YieldingSrptFrameFragmentationHandler.h>
#include <thrift/lib/cpp2/fast_thrift/transport/WriteCompletion.h>

namespace apache::thrift::fast_thrift::frame::write::handler {
namespace {

using namespace testing;

namespace cp = channel_pipeline;
using cp::test::TestAllocator;

HANDLER_TAG(yielding_srpt_test);
HANDLER_TAG(queueing_writer_test);

class PipelineHeadHandler {
 public:
  template <typename Context>
  cp::Result onWrite(Context&, cp::TypeErasedBox&&) noexcept {
    ++writeCount_;
    if (!nextWriteResults_.empty()) {
      const cp::Result result = nextWriteResults_.front();
      nextWriteResults_.pop_front();
      return result;
    }
    return writeResult_;
  }

  void onException(folly::exception_wrapper&&) noexcept {}
  void handlerAdded() noexcept {}
  void handlerRemoved() noexcept {}
  void onPipelineActive() noexcept {}
  void onPipelineInactive() noexcept {}
  void onReadReady() noexcept {}

  void setWriteResult(cp::Result result) noexcept { writeResult_ = result; }
  // The next writes return these results in order, and later writes return
  // the result of setWriteResult().
  void setNextWriteResults(std::deque<cp::Result> results) {
    nextWriteResults_ = std::move(results);
  }
  [[nodiscard]] size_t writeCount() const noexcept { return writeCount_; }

 private:
  cp::Result writeResult_{cp::Result::Success};
  std::deque<cp::Result> nextWriteResults_;
  size_t writeCount_{0};
};

class PipelineTailHandler {
 public:
  template <typename Context>
  cp::Result onRead(Context&, cp::TypeErasedBox&&) noexcept {
    return cp::Result::Success;
  }

  void onException(folly::exception_wrapper&&) noexcept {}
  void handlerAdded() noexcept {}
  void handlerRemoved() noexcept {}
  void onPipelineActive() noexcept {}
  void onPipelineInactive() noexcept {}
  void onWriteReady() noexcept {
    ++writeReadyCount_;
    if (onWriteReady_) {
      onWriteReady_();
    }
  }

  void setOnWriteReady(std::function<void()> onWriteReady) {
    onWriteReady_ = std::move(onWriteReady);
  }
  [[nodiscard]] size_t writeReadyCount() const noexcept {
    return writeReadyCount_;
  }

 private:
  std::function<void()> onWriteReady_;
  size_t writeReadyCount_{0};
};

// Handles Backpressure as Backpressure.h asks: it queues the frames that
// arrive after Backpressure and sends them from onWriteReady(), and it stays
// registered for write-ready until it has sent them all.
class QueueingWriter {
 public:
  cp::WriteReadyHook writeReadyHook_;

  template <typename Context>
  void handlerAdded(Context&) noexcept {}
  template <typename Context>
  void handlerRemoved(Context&) noexcept {}
  template <typename Context>
  void onPipelineInactive(Context&) noexcept {}

  template <typename Context>
  cp::Result onWrite(Context& ctx, cp::TypeErasedBox&& msg) noexcept {
    if (ctx.isAwaitingWriteReady()) {
      queued_.push_back(std::move(msg));
      return cp::Result::Backpressure;
    }
    const cp::Result result = ctx.fireWrite(std::move(msg));
    if (result == cp::Result::Backpressure) {
      ctx.awaitWriteReady();
    }
    return result;
  }

  template <typename Context>
  void onWriteReady(Context& ctx) noexcept {
    while (!queued_.empty()) {
      cp::TypeErasedBox msg = std::move(queued_.front());
      queued_.pop_front();
      if (ctx.fireWrite(std::move(msg)) == cp::Result::Backpressure) {
        return;
      }
    }
    ctx.cancelAwaitWriteReady();
  }

 private:
  std::deque<cp::TypeErasedBox> queued_;
};

constexpr size_t kQuantum = 100;
constexpr size_t kBudget = 1000;
constexpr size_t kCloseResponseSize = 8 * 1024 * 1024;
constexpr std::array<size_t, 2> kCloseFragmentSizes{512 * 1024, 1024 * 1024};

struct RecordingTracker {
  using PublishedEvents = channel_pipeline::Events<>;
  using SubscribedEvents = channel_pipeline::Events<>;

  struct FragmentCall {
    uint32_t streamId;
    bool isLastFragment;

    bool operator==(const FragmentCall&) const = default;
  };

  void onFragment(uint32_t streamId, bool isLastFragment) noexcept {
    fragments.push_back({streamId, isLastFragment});
  }
  void onFragmentAborted() noexcept {
    ++abortCount;
    fragments.pop_back();
  }
  void onFlush() noexcept { ++flushCount; }

  std::vector<FragmentCall> fragments;
  size_t flushCount{0};
  size_t abortCount{0};
};

static_assert(FragmentCompletionTracker<RecordingTracker>);

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

struct CapturedFrame {
  FrameType frameType;
  uint32_t streamId;
  size_t metadataSize;
  size_t dataSize;
  bool follows;
};

class TestContext {
 public:
  explicit TestContext(folly::EventBase* eventBase) : eventBase_(eventBase) {}

  folly::EventBase* eventBase() const noexcept { return eventBase_; }

  channel_pipeline::Result fireWrite(
      channel_pipeline::TypeErasedBox&& msg) noexcept {
    const size_t attempt = writeAttemptCount_++;
    const channel_pipeline::Result result = attempt < results_.size()
        ? results_[attempt]
        : channel_pipeline::Result::Success;
    if (writeAttemptObserver_) {
      writeAttemptObserver_();
    }
    if (result == channel_pipeline::Result::Error) {
      return result;
    }

    const ComposedFrame& frame = msg.get<ComposedFrame>();
    written_.push_back({
        .frameType = frame.frameType,
        .streamId = frame.streamId,
        .metadataSize =
            frame.metadata ? frame.metadata->computeChainDataLength() : 0,
        .dataSize = frame.data ? frame.data->computeChainDataLength() : 0,
        .follows = frame.follows,
    });
    if (std::function<void()> write = std::exchange(reentrantWrite_, nullptr);
        write) {
      write();
    }
    return result;
  }

  void fireException(folly::exception_wrapper&& exception) noexcept {
    ++fireExceptionCount_;
    lastException_ = std::move(exception);
  }

  template <channel_pipeline::PipelineEvent E>
  void fireEvent(const typename E::Payload& event) noexcept {
    static_assert(std::same_as<E, TestFrameWriteCompleteEvent>);
    completed_.push_back(event);
  }

  void awaitWriteReady() noexcept { awaitingWriteReady_ = true; }
  void cancelAwaitWriteReady() noexcept { awaitingWriteReady_ = false; }

  // Takes the handler's request for another write-ready round. These tests run
  // no rounds.
  struct NoPipeline {
    void onWriteReady() noexcept {}
  };
  NoPipeline* pipeline() noexcept { return &pipeline_; }

  void setResults(std::vector<channel_pipeline::Result> results) {
    results_ = std::move(results);
  }

  void setReentrantWrite(std::function<void()> write) {
    reentrantWrite_ = std::move(write);
  }

  void setWriteAttemptObserver(std::function<void()> observer) {
    writeAttemptObserver_ = std::move(observer);
  }

  [[nodiscard]] const std::vector<CapturedFrame>& written() const noexcept {
    return written_;
  }
  [[nodiscard]] const std::vector<TestFrameWriteCompleteEvent>& completed()
      const noexcept {
    return completed_;
  }
  [[nodiscard]] size_t writeAttemptCount() const noexcept {
    return writeAttemptCount_;
  }
  [[nodiscard]] bool awaitingWriteReady() const noexcept {
    return awaitingWriteReady_;
  }
  [[nodiscard]] size_t fireExceptionCount() const noexcept {
    return fireExceptionCount_;
  }
  [[nodiscard]] const folly::exception_wrapper& lastException() const noexcept {
    return lastException_;
  }

 private:
  folly::EventBase* const eventBase_;
  std::vector<channel_pipeline::Result> results_;
  std::function<void()> reentrantWrite_;
  std::function<void()> writeAttemptObserver_;
  std::vector<CapturedFrame> written_;
  std::vector<TestFrameWriteCompleteEvent> completed_;
  size_t writeAttemptCount_{0};
  bool awaitingWriteReady_{false};
  NoPipeline pipeline_;
  size_t fireExceptionCount_{0};
  folly::exception_wrapper lastException_;
};

using Handler = YieldingSrptFrameFragmentationHandlerT<RecordingTracker>;
using CompletionHandler = YieldingSrptFrameFragmentationHandlerT<
    FragmentCompletionTrackerT<TestEventFactory>>;

static_assert(channel_pipeline::OutboundHandler<Handler, TestContext>);

channel_pipeline::TypeErasedBox makeFrame(
    FrameType frameType,
    uint32_t streamId,
    size_t dataSize = 0,
    bool complete = false,
    size_t metadataSize = 0) {
  ComposedFrame frame{
      .frameType = frameType,
      .streamId = streamId,
      .complete = complete,
  };
  if (metadataSize > 0) {
    frame.metadata = folly::IOBuf::create(metadataSize);
    frame.metadata->append(metadataSize);
    std::memset(frame.metadata->writableData(), 'm', metadataSize);
  }
  if (dataSize > 0) {
    frame.data = folly::IOBuf::create(dataSize);
    frame.data->append(dataSize);
    std::memset(frame.data->writableData(), 'd', dataSize);
  }
  if (frameType == FrameType::PAYLOAD) {
    frame.next = true;
  }
  return channel_pipeline::erase_and_box(std::move(frame));
}

std::vector<uint32_t> streamIds(const std::vector<CapturedFrame>& frames) {
  std::vector<uint32_t> result;
  result.reserve(frames.size());
  for (const CapturedFrame& frame : frames) {
    result.push_back(frame.streamId);
  }
  return result;
}

std::vector<size_t> dataSizes(const std::vector<CapturedFrame>& frames) {
  std::vector<size_t> result;
  result.reserve(frames.size());
  for (const CapturedFrame& frame : frames) {
    result.push_back(frame.dataSize);
  }
  return result;
}

std::set<uint32_t> unfinishedStreams(const std::vector<CapturedFrame>& frames) {
  std::set<uint32_t> result;
  for (const CapturedFrame& frame : frames) {
    if (frame.follows) {
      result.insert(frame.streamId);
    } else {
      result.erase(frame.streamId);
    }
  }
  return result;
}

class YieldingSrptFrameFragmentationHandlerTest : public Test {
 protected:
  void runLoop() { eventBase_.loopOnce(EVLOOP_NONBLOCK); }

  folly::EventBase eventBase_;
  TestContext context_{&eventBase_};
};

TEST_F(
    YieldingSrptFrameFragmentationHandlerTest,
    InvalidConfigRejectsDataWithoutQueuingIt) {
  const auto expectRejected = [this](QuantumFragmentationHandlerConfig config) {
    Handler handler(config);
    handler.handlerAdded(context_);

    EXPECT_THAT(
        handler.onWrite(context_, makeFrame(FrameType::REQUEST_RESPONSE, 1, 1)),
        Eq(channel_pipeline::Result::Error));
    EXPECT_THAT(context_.written(), IsEmpty());
    EXPECT_THAT(
        std::tuple(handler.hasPendingWrites(), handler.isScheduled()),
        Eq(std::tuple(false, false)));
  };

  expectRejected({.streamQuantum = 0});
  expectRejected({.streamQuantum = kMaxStreamQuantum + 1});
  expectRejected({.callbackBudget = 0});
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerTest,
    RefusesMetadataAboveTheLargestStreamQuantum) {
  Handler handler({.streamQuantum = kQuantum, .callbackBudget = kBudget});
  handler.handlerAdded(context_);

  const channel_pipeline::Result tooLarge = handler.onWrite(
      context_,
      makeFrame(
          FrameType::PAYLOAD, 1, 0, /*complete=*/false, kMaxStreamQuantum + 1));
  const channel_pipeline::Result largest = handler.onWrite(
      context_,
      makeFrame(
          FrameType::PAYLOAD, 3, 0, /*complete=*/false, kMaxStreamQuantum));
  runLoop();

  EXPECT_THAT(
      std::tuple(tooLarge, largest, streamIds(context_.written())),
      Eq(std::tuple(
          channel_pipeline::Result::Error,
          channel_pipeline::Result::Success,
          std::vector<uint32_t>{3})));
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerTest,
    RefusalLogsTheMetadataSizeAndTheLimit) {
  // The refusal logs at most once a minute in a process. A threadsafe death
  // test runs the refusal in a new process, so refusals of earlier tests in
  // this binary cannot hide the log line.
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  EXPECT_EXIT(
      {
        const auto logs = std::make_shared<folly::TestLogHandler>();
        folly::LoggerDB::get().getCategory("")->addHandler(logs);
        Handler handler({.streamQuantum = kQuantum, .callbackBudget = kBudget});
        handler.handlerAdded(context_);
        const channel_pipeline::Result result = handler.onWrite(
            context_,
            makeFrame(
                FrameType::PAYLOAD,
                1,
                0,
                /*complete=*/false,
                kMaxStreamQuantum + 1));
        for (const std::string& message : logs->getMessageValues()) {
          std::fputs(message.c_str(), stderr);
        }
        std::_Exit(result == channel_pipeline::Result::Error ? 0 : 1);
      },
      ExitedWithCode(0),
      HasSubstr(
          "YieldingSrptFrameFragmentationHandler refused a data frame with "
          "16776704 metadata bytes. The limit is 16776703 bytes, and the "
          "config is valid."));
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerTest,
    SendsAFrameThatFitsInOneQuantumAtOnce) {
  Handler handler({.streamQuantum = kQuantum, .callbackBudget = kBudget});
  handler.handlerAdded(context_);
  constexpr std::array<FrameType, 5> kDataFrameTypes{
      FrameType::REQUEST_RESPONSE,
      FrameType::REQUEST_FNF,
      FrameType::REQUEST_STREAM,
      FrameType::REQUEST_CHANNEL,
      FrameType::PAYLOAD,
  };

  uint32_t streamId = 1;
  for (const FrameType frameType : kDataFrameTypes) {
    EXPECT_THAT(
        handler.onWrite(context_, makeFrame(frameType, streamId)),
        Eq(channel_pipeline::Result::Success));
    streamId += 2;
  }
  EXPECT_THAT(
      handler.onWrite(
          context_,
          makeFrame(
              FrameType::PAYLOAD,
              streamId,
              kQuantum / 2,
              /*complete=*/false,
              kQuantum / 2)),
      Eq(channel_pipeline::Result::Success));

  ASSERT_THAT(context_.written(), SizeIs(kDataFrameTypes.size() + 1));
  for (size_t index = 0; index < kDataFrameTypes.size(); ++index) {
    EXPECT_THAT(
        context_.written()[index].frameType, Eq(kDataFrameTypes[index]));
  }
  EXPECT_THAT(
      std::tuple(
          context_.written().back().metadataSize,
          context_.written().back().dataSize,
          context_.written().back().follows),
      Eq(std::tuple(kQuantum / 2, kQuantum / 2, false)));
  const std::vector<RecordingTracker::FragmentCall> expectedFragments{
      {.streamId = 1, .isLastFragment = true},
      {.streamId = 3, .isLastFragment = true},
      {.streamId = 5, .isLastFragment = true},
      {.streamId = 7, .isLastFragment = true},
      {.streamId = 9, .isLastFragment = true},
      {.streamId = 11, .isLastFragment = true}};
  EXPECT_THAT(handler.tracker().fragments, Eq(expectedFragments));
  EXPECT_THAT(
      std::tuple(handler.hasPendingWrites(), handler.isScheduled()),
      Eq(std::tuple(false, false)));
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerTest,
    AFrameThatFitsWaitsBehindTheDeferredFramesOfItsStream) {
  Handler handler({.streamQuantum = kQuantum, .callbackBudget = kBudget});
  handler.handlerAdded(context_);
  (void)handler.onWrite(context_, makeFrame(FrameType::PAYLOAD, 1, 150));
  (void)handler.onWrite(context_, makeFrame(FrameType::PAYLOAD, 1, 10));
  (void)handler.onWrite(context_, makeFrame(FrameType::PAYLOAD, 3, 30));
  // Runs during the first pass, while stream 1 is pinned and stream 5 has
  // only a deferred arrival.
  context_.setReentrantWrite([&] {
    (void)handler.onWrite(context_, makeFrame(FrameType::PAYLOAD, 1, 20));
    (void)handler.onWrite(context_, makeFrame(FrameType::PAYLOAD, 5, 150));
    (void)handler.onWrite(context_, makeFrame(FrameType::PAYLOAD, 5, 40));
  });

  EXPECT_THAT(streamIds(context_.written()), ElementsAre(3));

  for (int pass = 0; pass < 5; ++pass) {
    runLoop();
  }

  EXPECT_THAT(
      streamIds(context_.written()), ElementsAre(3, 1, 1, 1, 1, 5, 5, 5));
  EXPECT_THAT(
      dataSizes(context_.written()),
      ElementsAre(30, 100, 50, 10, 20, 100, 50, 40));
  EXPECT_THAT(handler.hasPendingWrites(), IsFalse());
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerTest,
    LoneLargeRequestSendsOneQuantumAndYields) {
  Handler handler({.streamQuantum = kQuantum, .callbackBudget = kBudget});
  handler.handlerAdded(context_);
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 1, 250));

  runLoop();

  EXPECT_THAT(streamIds(context_.written()), ElementsAre(1));
  EXPECT_THAT(dataSizes(context_.written()), ElementsAre(kQuantum));
  EXPECT_THAT(context_.written()[0].follows, IsTrue());
  EXPECT_THAT(handler.hasPendingStream(1), IsTrue());
  EXPECT_THAT(handler.isScheduled(), IsTrue());
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerTest,
    KLargeRequestsGiveThePinnedShortestRequestKTurns) {
  Handler handler({.streamQuantum = kQuantum, .callbackBudget = kBudget});
  handler.handlerAdded(context_);
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 1, 500));
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 3, 600));
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 5, 700));

  runLoop();

  EXPECT_THAT(streamIds(context_.written()), ElementsAre(1, 1, 1));
  EXPECT_THAT(dataSizes(context_.written()), ElementsAre(100, 100, 100));
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerTest,
    RemainingLargeTurnsMoveToTheNextShortestRequest) {
  Handler handler({.streamQuantum = kQuantum, .callbackBudget = kBudget});
  handler.handlerAdded(context_);
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 1, 150));
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 3, 300));
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 5, 400));

  runLoop();

  EXPECT_THAT(streamIds(context_.written()), ElementsAre(1, 1, 3));
  EXPECT_THAT(dataSizes(context_.written()), ElementsAre(100, 50, 100));
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerTest,
    TheSmallestFrontFrameGoesFirstWhateverQueuesBehindIt) {
  Handler handler({.streamQuantum = kQuantum, .callbackBudget = kBudget});
  handler.handlerAdded(context_);
  (void)handler.onWrite(context_, makeFrame(FrameType::PAYLOAD, 1, 150));
  (void)handler.onWrite(context_, makeFrame(FrameType::PAYLOAD, 1, 500));
  (void)handler.onWrite(context_, makeFrame(FrameType::PAYLOAD, 1, 500));
  (void)handler.onWrite(context_, makeFrame(FrameType::PAYLOAD, 3, 300));

  runLoop();

  EXPECT_THAT(
      std::tuple(streamIds(context_.written()), dataSizes(context_.written())),
      Eq(std::tuple(
          std::vector<uint32_t>{1, 1}, std::vector<size_t>{100, 50})));
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerTest,
    AnotherStreamGoesOutWhenThePinnedStreamFinishesAFrame) {
  constexpr size_t kStreamQuantum = 64 * 1024;
  Handler handler({.streamQuantum = kStreamQuantum});
  handler.handlerAdded(context_);
  (void)handler.onWrite(
      context_, makeFrame(FrameType::PAYLOAD, 1, 16 * kStreamQuantum));
  (void)handler.onWrite(
      context_, makeFrame(FrameType::PAYLOAD, 1, 16 * kStreamQuantum));
  runLoop();
  // Stream 1 is pinned in its first frame. Stream 3 needs 5 fragments.
  (void)handler.onWrite(context_, makeFrame(FrameType::PAYLOAD, 3, 300 * 1024));

  while (handler.isScheduled()) {
    runLoop();
  }

  std::vector<uint32_t> expected(16, 1);
  expected.insert(expected.end(), 5, 3);
  expected.insert(expected.end(), 16, 1);
  EXPECT_THAT(streamIds(context_.written()), Eq(expected));
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerTest,
    AFrameForThePinnedStreamJoinsItsQueueButNotThePin) {
  Handler handler({.streamQuantum = kQuantum, .callbackBudget = kBudget});
  handler.handlerAdded(context_);
  (void)handler.onWrite(context_, makeFrame(FrameType::PAYLOAD, 1, 250));
  (void)handler.onWrite(context_, makeFrame(FrameType::PAYLOAD, 3, 400));
  runLoop();
  // Arrives while stream 1 is pinned. The next pass still has one turn for
  // each of the two streams.
  (void)handler.onWrite(context_, makeFrame(FrameType::PAYLOAD, 1, 500));

  runLoop();

  EXPECT_THAT(
      std::tuple(streamIds(context_.written()), dataSizes(context_.written())),
      Eq(std::tuple(
          std::vector<uint32_t>{1, 1, 1, 3},
          std::vector<size_t>{100, 100, 50, 100})));
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerTest,
    DeferredFramesThatFitLeaveAtPassEndInArrivalOrder) {
  Handler handler({.streamQuantum = kQuantum, .callbackBudget = kBudget});
  handler.handlerAdded(context_);
  (void)handler.onWrite(context_, makeFrame(FrameType::PAYLOAD, 1, 101));
  (void)handler.onWrite(context_, makeFrame(FrameType::PAYLOAD, 3, 102));
  (void)handler.onWrite(context_, makeFrame(FrameType::PAYLOAD, 5, 300));
  (void)handler.onWrite(context_, makeFrame(FrameType::PAYLOAD, 7, 400));
  // Streams 1 and 3 still have deferred data here, so these frames wait. Both
  // streams finish in this pass, so the frames go out when the pass ends.
  context_.setReentrantWrite([&] {
    (void)handler.onWrite(context_, makeFrame(FrameType::PAYLOAD, 1, 70, true));
    (void)handler.onWrite(context_, makeFrame(FrameType::PAYLOAD, 3, 20, true));
  });

  runLoop();

  EXPECT_THAT(streamIds(context_.written()), ElementsAre(1, 1, 3, 3, 1, 3));

  runLoop();

  EXPECT_THAT(
      streamIds(context_.written()), ElementsAre(1, 1, 3, 3, 1, 3, 5, 5));
  EXPECT_THAT(
      dataSizes(context_.written()),
      ElementsAre(100, 1, 100, 2, 70, 20, 100, 100));
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerTest,
    AFrameQueuedAtPassEndWaitsForTheNextPass) {
  Handler handler({.streamQuantum = kQuantum, .callbackBudget = kBudget});
  handler.handlerAdded(context_);
  (void)handler.onWrite(context_, makeFrame(FrameType::PAYLOAD, 1, 150));
  (void)handler.onWrite(context_, makeFrame(FrameType::PAYLOAD, 3, 160));
  (void)handler.onWrite(context_, makeFrame(FrameType::PAYLOAD, 7, 170));
  // The cancels leave this pass with a spare turn. Stream 5 must not take it
  // after the frame of stream 1 goes out at the end of the pass.
  context_.setReentrantWrite([&] {
    (void)handler.onWrite(context_, makeFrame(FrameType::CANCEL, 3));
    (void)handler.onWrite(context_, makeFrame(FrameType::CANCEL, 7));
    (void)handler.onWrite(context_, makeFrame(FrameType::PAYLOAD, 5, 150));
    (void)handler.onWrite(context_, makeFrame(FrameType::PAYLOAD, 1, 10));
  });

  runLoop();

  EXPECT_THAT(streamIds(context_.written()), ElementsAre(1, 3, 7, 1, 1));
  EXPECT_THAT(dataSizes(context_.written()), ElementsAre(100, 0, 0, 50, 10));
  EXPECT_THAT(
      std::tuple(handler.hasPendingStream(5), handler.isScheduled()),
      Eq(std::tuple(true, true)));
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerTest,
    SmallRequestAdmittedBetweenPassesPreemptsPinnedLargeRequest) {
  Handler handler({.streamQuantum = kQuantum, .callbackBudget = kBudget});
  handler.handlerAdded(context_);
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 1, 400));
  runLoop();
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 3, 50));

  runLoop();

  EXPECT_THAT(streamIds(context_.written()), ElementsAre(1, 3, 1));
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerTest,
    RequestAdmittedDuringAPassWaitsForTheNextPass) {
  Handler handler({.streamQuantum = kQuantum, .callbackBudget = kBudget});
  handler.handlerAdded(context_);
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 1, 200));
  context_.setReentrantWrite([&] {
    EXPECT_THAT(
        handler.onWrite(
            context_, makeFrame(FrameType::REQUEST_RESPONSE, 3, 150)),
        Eq(channel_pipeline::Result::Success));
  });

  runLoop();

  EXPECT_THAT(streamIds(context_.written()), ElementsAre(1));
  runLoop();
  EXPECT_THAT(streamIds(context_.written()), ElementsAre(1, 1, 3));
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerTest,
    CancelDuringAPassDropsOnlyThatStreamsDeferredFrames) {
  Handler handler({.streamQuantum = kQuantum, .callbackBudget = kBudget});
  handler.handlerAdded(context_);
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 1, 200));
  context_.setReentrantWrite([&] {
    (void)handler.onWrite(
        context_, makeFrame(FrameType::REQUEST_RESPONSE, 3, 150));
    (void)handler.onWrite(
        context_, makeFrame(FrameType::REQUEST_RESPONSE, 5, 150));
    (void)handler.onWrite(context_, makeFrame(FrameType::CANCEL, 3));
  });

  runLoop();
  runLoop();
  runLoop();

  EXPECT_THAT(streamIds(context_.written()), ElementsAre(1, 3, 1, 5, 5));
  EXPECT_THAT(dataSizes(context_.written()), ElementsAre(100, 0, 100, 100, 50));
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerTest,
    NewLargeRequestCannotPreemptPinnedLargeRequest) {
  Handler handler({.streamQuantum = kQuantum, .callbackBudget = kBudget});
  handler.handlerAdded(context_);
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 1, 500));
  runLoop();
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 3, 150));

  runLoop();

  EXPECT_THAT(streamIds(context_.written()), ElementsAre(1, 1, 1));
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerTest,
    AtMostOneLargeRequestIsPartialAtPassEnd) {
  Handler handler({.streamQuantum = kQuantum, .callbackBudget = kBudget});
  handler.handlerAdded(context_);
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 1, 150));
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 3, 250));
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 5, 400));

  runLoop();

  EXPECT_THAT(unfinishedStreams(context_.written()), ElementsAre(3));
  EXPECT_THAT(handler.hasPendingStream(5), IsTrue());
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerTest,
    CallbackBudgetStopsNewTurnsWithoutCuttingAStartedTurn) {
  Handler handler({.streamQuantum = kQuantum, .callbackBudget = 150});
  handler.handlerAdded(context_);
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 1, 250));
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 3, 260));
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 5, 300));

  runLoop();

  EXPECT_THAT(streamIds(context_.written()), ElementsAre(1, 1));
  EXPECT_THAT(dataSizes(context_.written()), ElementsAre(100, 100));
  EXPECT_THAT(handler.hasPendingStream(3), IsTrue());
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerTest,
    MetadataCountsTowardQuantumButCallbackBudgetCountsData) {
  Handler handler({.streamQuantum = 100, .callbackBudget = 40});
  handler.handlerAdded(context_);
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 1, 60, false, 60));

  runLoop();

  EXPECT_THAT(context_.written(), SizeIs(1));
  EXPECT_THAT(
      std::tuple(
          context_.written()[0].metadataSize,
          context_.written()[0].dataSize,
          context_.written()[0].follows),
      Eq(std::tuple(size_t{60}, size_t{40}, true)));

  runLoop();

  EXPECT_THAT(context_.written(), SizeIs(2));
  EXPECT_THAT(
      std::tuple(
          context_.written()[1].metadataSize,
          context_.written()[1].dataSize,
          context_.written()[1].follows),
      Eq(std::tuple(size_t{0}, size_t{20}, false)));
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerTest, BackpressureDoesNotStopThePass) {
  Handler handler({.streamQuantum = kQuantum, .callbackBudget = kBudget});
  handler.handlerAdded(context_);
  context_.setResults({channel_pipeline::Result::Backpressure});
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 1, 150));
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 3, 300));

  runLoop();

  EXPECT_THAT(streamIds(context_.written()), ElementsAre(1, 1));
  EXPECT_THAT(
      std::tuple(
          context_.awaitingWriteReady(),
          handler.isBackpressured(),
          handler.isScheduled()),
      Eq(std::tuple(true, true, true)));
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerTest,
    DataArrivingUnderBackpressureQueuesAndReportsBackpressure) {
  Handler handler({.streamQuantum = kQuantum, .callbackBudget = kBudget});
  handler.handlerAdded(context_);
  context_.setResults({channel_pipeline::Result::Backpressure});
  EXPECT_THAT(
      handler.onWrite(context_, makeFrame(FrameType::REQUEST_RESPONSE, 1, 200)),
      Eq(channel_pipeline::Result::Success));
  runLoop();

  EXPECT_THAT(
      handler.onWrite(context_, makeFrame(FrameType::REQUEST_RESPONSE, 3, 150)),
      Eq(channel_pipeline::Result::Backpressure));
  EXPECT_THAT(context_.written(), SizeIs(1));

  runLoop();
  EXPECT_THAT(streamIds(context_.written()), ElementsAre(1, 1, 3));
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerTest,
    PassThroughBackpressureReportsUntilWriteReady) {
  Handler handler({.streamQuantum = kQuantum, .callbackBudget = kBudget});
  handler.handlerAdded(context_);
  context_.setResults({channel_pipeline::Result::Backpressure});

  EXPECT_THAT(
      handler.onWrite(context_, makeFrame(FrameType::REQUEST_N, 1)),
      Eq(channel_pipeline::Result::Backpressure));
  EXPECT_THAT(
      std::tuple(handler.isBackpressured(), context_.awaitingWriteReady()),
      Eq(std::tuple(true, true)));
  EXPECT_THAT(
      handler.onWrite(context_, makeFrame(FrameType::REQUEST_N, 3)),
      Eq(channel_pipeline::Result::Backpressure));
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerTest,
    WriteReadyClearsBackpressureWithoutSending) {
  Handler handler({.streamQuantum = kQuantum, .callbackBudget = kBudget});
  handler.handlerAdded(context_);
  context_.setResults({channel_pipeline::Result::Backpressure});
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 1, 250));
  runLoop();

  handler.onWriteReady(context_);

  EXPECT_THAT(
      std::tuple(
          context_.awaitingWriteReady(),
          handler.isBackpressured(),
          context_.written().size()),
      Eq(std::tuple(false, false, size_t{1})));
  EXPECT_THAT(
      handler.onWrite(context_, makeFrame(FrameType::REQUEST_RESPONSE, 3, 50)),
      Eq(channel_pipeline::Result::Success));
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerTest,
    PreservesSameStreamFrameOrderAcrossPasses) {
  Handler handler({.streamQuantum = kQuantum, .callbackBudget = kBudget});
  handler.handlerAdded(context_);
  (void)handler.onWrite(context_, makeFrame(FrameType::PAYLOAD, 1, 150));
  (void)handler.onWrite(context_, makeFrame(FrameType::PAYLOAD, 1, 50));

  runLoop();
  runLoop();
  runLoop();

  EXPECT_THAT(streamIds(context_.written()), ElementsAre(1, 1, 1));
  EXPECT_THAT(dataSizes(context_.written()), ElementsAre(100, 50, 50));
  const std::array<bool, 3> follows{
      context_.written()[0].follows,
      context_.written()[1].follows,
      context_.written()[2].follows};
  EXPECT_THAT(follows, ElementsAre(true, false, false));
}

TEST(
    YieldingSrptFrameFragmentationHandlerTerminalTest,
    CancelAndErrorRemovePinnedStreamData) {
  for (const FrameType terminal : {FrameType::CANCEL, FrameType::ERROR}) {
    folly::EventBase eventBase;
    TestContext context(&eventBase);
    Handler handler({.streamQuantum = kQuantum, .callbackBudget = kBudget});
    handler.handlerAdded(context);
    (void)handler.onWrite(
        context, makeFrame(FrameType::REQUEST_RESPONSE, 1, 250));
    eventBase.loopOnce(EVLOOP_NONBLOCK);

    EXPECT_THAT(
        handler.onWrite(context, makeFrame(terminal, 1)),
        Eq(channel_pipeline::Result::Success));

    EXPECT_THAT(handler.hasPendingWrites(), IsFalse());
    EXPECT_THAT(context.written(), SizeIs(2));
    EXPECT_THAT(context.written()[1].frameType, Eq(terminal));
    eventBase.loopOnce(EVLOOP_NONBLOCK);
    EXPECT_THAT(context.written(), SizeIs(2));
  }
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerTest,
    ControlFramesKeepTheInlinePassThroughContract) {
  Handler handler({.streamQuantum = kQuantum, .callbackBudget = kBudget});
  handler.handlerAdded(context_);
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 1, 250));

  EXPECT_THAT(
      handler.onWrite(context_, makeFrame(FrameType::REQUEST_N, 1)),
      Eq(channel_pipeline::Result::Success));

  EXPECT_THAT(context_.written(), SizeIs(1));
  EXPECT_THAT(context_.written()[0].frameType, Eq(FrameType::REQUEST_N));
  EXPECT_THAT(handler.hasPendingStream(1), IsTrue());
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerTest,
    DownstreamErrorClearsWorkAndPropagatesAnException) {
  Handler handler({.streamQuantum = kQuantum, .callbackBudget = kBudget});
  handler.handlerAdded(context_);
  context_.setResults({channel_pipeline::Result::Error});
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 1, 250));
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 3, 150));

  runLoop();

  EXPECT_THAT(context_.writeAttemptCount(), Eq(1));
  EXPECT_THAT(context_.written(), IsEmpty());
  EXPECT_THAT(handler.tracker().fragments, IsEmpty());
  EXPECT_THAT(handler.hasPendingWrites(), IsFalse());
  EXPECT_THAT(handler.isScheduled(), IsFalse());
  EXPECT_THAT(context_.fireExceptionCount(), Eq(1));
  EXPECT_THAT(
      std::string(context_.lastException().what()),
      HasSubstr(
          "YieldingSrptFrameFragmentationHandler: downstream write "
          "failed"));
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerTest,
    CompletionFiresOnceAfterTheLastFragmentAcrossPasses) {
  CompletionHandler handler(
      {.streamQuantum = kQuantum, .callbackBudget = kBudget});
  handler.handlerAdded(context_);
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 1, 150));

  runLoop();
  handler.on<TestBatchWriteCompleteEvent>(
      context_,
      TestBatchWriteCompleteEvent{
          .status = transport::WriteCompletionStatus::Success,
          .frameCount = 1,
          .bytes = 100,
          .quiesced = true,
      });
  EXPECT_THAT(context_.completed(), IsEmpty());

  runLoop();
  handler.on<TestBatchWriteCompleteEvent>(
      context_,
      TestBatchWriteCompleteEvent{
          .status = transport::WriteCompletionStatus::Success,
          .frameCount = 1,
          .bytes = 50,
          .quiesced = true,
      });

  EXPECT_THAT(context_.completed(), SizeIs(1));
  EXPECT_THAT(
      std::tuple(
          context_.completed()[0].streamId,
          context_.completed()[0].status,
          context_.completed()[0].quiesced),
      Eq(std::tuple(
          uint32_t{1}, transport::WriteCompletionStatus::Success, true)));
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerTest,
    QueuedInlineCompletionSeesTheFragmentRecord) {
  CompletionHandler handler(
      {.streamQuantum = kQuantum, .callbackBudget = kBudget});
  handler.handlerAdded(context_);
  context_.setWriteAttemptObserver([&] {
    handler.on<TestBatchWriteCompleteEvent>(
        context_,
        TestBatchWriteCompleteEvent{
            .status = transport::WriteCompletionStatus::Success,
            .frameCount = 1,
            .bytes = 100,
            .quiesced = true,
        });
  });
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 1, 150));

  runLoop();
  runLoop();

  const std::vector<TestFrameWriteCompleteEvent> expected{{
      .streamId = 1,
      .status = transport::WriteCompletionStatus::Success,
      .quiesced = true,
  }};
  EXPECT_THAT(context_.completed(), Eq(expected));
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerTest,
    PassThroughInlineCompletionSeesTheFragmentRecord) {
  CompletionHandler handler(
      {.streamQuantum = kQuantum, .callbackBudget = kBudget});
  handler.handlerAdded(context_);
  context_.setWriteAttemptObserver([&] {
    handler.on<TestBatchWriteCompleteEvent>(
        context_,
        TestBatchWriteCompleteEvent{
            .status = transport::WriteCompletionStatus::Success,
            .frameCount = 1,
            .bytes = 0,
            .quiesced = true,
        });
  });

  EXPECT_THAT(
      handler.onWrite(context_, makeFrame(FrameType::KEEPALIVE, 0)),
      Eq(channel_pipeline::Result::Success));

  const std::vector<TestFrameWriteCompleteEvent> expected{{
      .streamId = 0,
      .status = transport::WriteCompletionStatus::Success,
      .quiesced = true,
  }};
  EXPECT_THAT(context_.completed(), Eq(expected));
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerTest,
    RemovedDeferredFramesDoNotBlockQuiescence) {
  CompletionHandler handler(
      {.streamQuantum = kQuantum, .callbackBudget = kBudget});
  handler.handlerAdded(context_);
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 1, 150));
  runLoop();
  handler.on<TestBatchWriteCompleteEvent>(
      context_,
      TestBatchWriteCompleteEvent{
          .status = transport::WriteCompletionStatus::Success,
          .frameCount = 1,
          .bytes = 100,
          .quiesced = true,
      });
  context_.setReentrantWrite([&] {
    (void)handler.onWrite(
        context_, makeFrame(FrameType::REQUEST_RESPONSE, 3, 150));
    (void)handler.onWrite(context_, makeFrame(FrameType::CANCEL, 3));
    handler.on<TestBatchWriteCompleteEvent>(
        context_,
        TestBatchWriteCompleteEvent{
            .status = transport::WriteCompletionStatus::Success,
            .frameCount = 2,
            .bytes = 50,
            .quiesced = true,
        });
  });

  runLoop();

  const std::vector<TestFrameWriteCompleteEvent> expected{
      {
          .streamId = 1,
          .status = transport::WriteCompletionStatus::Success,
          .quiesced = false,
      },
      {
          .streamId = 3,
          .status = transport::WriteCompletionStatus::Success,
          .quiesced = true,
      },
  };
  EXPECT_THAT(context_.completed(), Eq(expected));
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerTest,
    QueuedWriteErrorAbortsTheRecordedFragment) {
  Handler handler({.streamQuantum = kQuantum, .callbackBudget = kBudget});
  handler.handlerAdded(context_);
  size_t recordsDuringWrite = 0;
  context_.setWriteAttemptObserver(
      [&] { recordsDuringWrite = handler.tracker().fragments.size(); });
  context_.setResults({channel_pipeline::Result::Error});
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 1, 150));

  runLoop();

  EXPECT_THAT(recordsDuringWrite, Eq(1));
  EXPECT_THAT(handler.tracker().fragments, IsEmpty());
  EXPECT_THAT(handler.tracker().abortCount, Eq(1));
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerTest,
    PassThroughWriteErrorAbortsTheRecordedFragment) {
  Handler handler({.streamQuantum = kQuantum, .callbackBudget = kBudget});
  handler.handlerAdded(context_);
  size_t recordsDuringWrite = 0;
  context_.setWriteAttemptObserver(
      [&] { recordsDuringWrite = handler.tracker().fragments.size(); });
  context_.setResults({channel_pipeline::Result::Error});

  EXPECT_THAT(
      handler.onWrite(context_, makeFrame(FrameType::KEEPALIVE, 0)),
      Eq(channel_pipeline::Result::Error));

  EXPECT_THAT(recordsDuringWrite, Eq(1));
  EXPECT_THAT(handler.tracker().fragments, IsEmpty());
  EXPECT_THAT(handler.tracker().abortCount, Eq(1));
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerTest,
    HandlerRemovalCancelsCallbackAndClearsWork) {
  Handler handler({.streamQuantum = kQuantum, .callbackBudget = kBudget});
  handler.handlerAdded(context_);
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 1, 250));

  handler.handlerRemoved(context_);

  EXPECT_THAT(handler.isScheduled(), IsFalse());
  EXPECT_THAT(handler.hasPendingWrites(), IsFalse());
  runLoop();
  EXPECT_THAT(context_.written(), IsEmpty());
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerTest,
    HandlerRemovalCancelsWriteReadyHook) {
  Handler handler({.streamQuantum = kQuantum, .callbackBudget = kBudget});
  handler.handlerAdded(context_);
  context_.setResults({channel_pipeline::Result::Backpressure});
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 1, 250));
  runLoop();
  EXPECT_THAT(context_.awaitingWriteReady(), IsTrue());

  handler.handlerRemoved(context_);

  EXPECT_THAT(context_.awaitingWriteReady(), IsFalse());
  EXPECT_THAT(handler.hasPendingWrites(), IsFalse());
  EXPECT_THAT(handler.isScheduled(), IsFalse());
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerTest,
    PipelineInactiveCancelsCallbackAndClearsWork) {
  Handler handler({.streamQuantum = kQuantum, .callbackBudget = kBudget});
  handler.handlerAdded(context_);
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 1, 250));

  handler.onPipelineInactive(context_);

  EXPECT_THAT(handler.isScheduled(), IsFalse());
  EXPECT_THAT(handler.hasPendingWrites(), IsFalse());
  runLoop();
  EXPECT_THAT(context_.written(), IsEmpty());
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerTest,
    PipelineInactiveCancelsWriteReadyHook) {
  Handler handler({.streamQuantum = kQuantum, .callbackBudget = kBudget});
  handler.handlerAdded(context_);
  context_.setResults({channel_pipeline::Result::Backpressure});
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 1, 250));
  runLoop();
  EXPECT_THAT(context_.awaitingWriteReady(), IsTrue());

  handler.onPipelineInactive(context_);

  EXPECT_THAT(context_.awaitingWriteReady(), IsFalse());
  EXPECT_THAT(handler.hasPendingWrites(), IsFalse());
  EXPECT_THAT(handler.isScheduled(), IsFalse());
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerTest,
    DownstreamErrorCancelsWriteReadyHook) {
  Handler handler({.streamQuantum = kQuantum, .callbackBudget = kBudget});
  handler.handlerAdded(context_);
  context_.setResults(
      {channel_pipeline::Result::Backpressure,
       channel_pipeline::Result::Error});
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 1, 250));
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 3, 150));

  runLoop();

  EXPECT_THAT(
      std::tuple(
          context_.fireExceptionCount(),
          context_.awaitingWriteReady(),
          handler.isBackpressured()),
      Eq(std::tuple(size_t{1}, false, false)));
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerTest,
    PipelineInactiveDuringAPassEndsThePass) {
  Handler handler({.streamQuantum = kQuantum, .callbackBudget = kBudget});
  handler.handlerAdded(context_);
  // A socket write that fails inside the pass closes the pipeline before the
  // write returns.
  context_.setWriteAttemptObserver(
      [&] { handler.onPipelineInactive(context_); });
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 1, 150));
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 3, 160));

  runLoop();

  EXPECT_THAT(
      std::tuple(
          context_.writeAttemptCount(),
          handler.hasPendingWrites(),
          handler.isScheduled(),
          context_.fireExceptionCount()),
      Eq(std::tuple(size_t{1}, false, false, size_t{0})));
}

TEST(
    YieldingSrptFrameFragmentationHandlerCloseTest,
    PipelineInactiveAfterSuccessDropsRemainder) {
  for (const size_t fragmentSize : kCloseFragmentSizes) {
    folly::EventBase eventBase;
    TestContext context(&eventBase);
    Handler handler({.streamQuantum = fragmentSize});
    handler.handlerAdded(context);
    EXPECT_THAT(
        handler.onWrite(
            context,
            makeFrame(FrameType::PAYLOAD, 1, kCloseResponseSize, true)),
        Eq(channel_pipeline::Result::Success));

    eventBase.loopOnce(EVLOOP_NONBLOCK);

    EXPECT_THAT(context.written(), SizeIs(1));
    EXPECT_THAT(context.written()[0].dataSize, Eq(fragmentSize));
    EXPECT_THAT(handler.tracker().fragments, SizeIs(1));
    EXPECT_THAT(handler.hasPendingWrites(), IsTrue());

    handler.onPipelineInactive(context);
    eventBase.loopOnce(EVLOOP_NONBLOCK);

    EXPECT_THAT(context.written(), SizeIs(1));
    EXPECT_THAT(handler.hasPendingWrites(), IsFalse());
  }
}

TEST(
    YieldingSrptFrameFragmentationHandlerCloseTest,
    ErrorBeforeCloseAbortsRecordAndClearsRemainder) {
  for (const size_t fragmentSize : kCloseFragmentSizes) {
    folly::EventBase eventBase;
    TestContext context(&eventBase);
    context.setResults(
        {channel_pipeline::Result::Success, channel_pipeline::Result::Error});
    Handler handler({.streamQuantum = fragmentSize});
    handler.handlerAdded(context);
    (void)handler.onWrite(
        context, makeFrame(FrameType::PAYLOAD, 1, kCloseResponseSize, true));

    eventBase.loopOnce(EVLOOP_NONBLOCK);
    eventBase.loopOnce(EVLOOP_NONBLOCK);

    EXPECT_THAT(context.writeAttemptCount(), Eq(2));
    EXPECT_THAT(context.written(), SizeIs(1));
    EXPECT_THAT(handler.tracker().fragments, SizeIs(1));
    EXPECT_THAT(handler.tracker().abortCount, Eq(1));
    EXPECT_THAT(handler.hasPendingWrites(), IsFalse());
    EXPECT_THAT(context.fireExceptionCount(), Eq(1));
  }
}

TEST(
    YieldingSrptFrameFragmentationHandlerCloseTest,
    PipelineInactiveAfterBackpressureCancelsAwaitAndDropsRemainder) {
  for (const size_t fragmentSize : kCloseFragmentSizes) {
    folly::EventBase eventBase;
    TestContext context(&eventBase);
    context.setResults(
        {channel_pipeline::Result::Success,
         channel_pipeline::Result::Backpressure});
    Handler handler({.streamQuantum = fragmentSize});
    handler.handlerAdded(context);
    (void)handler.onWrite(
        context, makeFrame(FrameType::PAYLOAD, 1, kCloseResponseSize, true));

    eventBase.loopOnce(EVLOOP_NONBLOCK);
    eventBase.loopOnce(EVLOOP_NONBLOCK);

    EXPECT_THAT(
        std::tuple(
            context.written().size(),
            handler.tracker().fragments.size(),
            handler.hasPendingWrites(),
            context.awaitingWriteReady(),
            handler.isScheduled()),
        Eq(std::tuple(size_t{2}, size_t{2}, true, true, true)));

    handler.onPipelineInactive(context);
    eventBase.loopOnce(EVLOOP_NONBLOCK);

    EXPECT_THAT(
        std::tuple(
            context.written().size(),
            handler.hasPendingWrites(),
            context.awaitingWriteReady(),
            handler.isScheduled()),
        Eq(std::tuple(size_t{2}, false, false, false)));
  }
}

class YieldingSrptFrameFragmentationHandlerPipelineTest : public Test {
 protected:
  cp::PipelineImpl::Ptr buildDynamicPipeline() {
    return cp::PipelineBuilder<
               PipelineHeadHandler,
               PipelineTailHandler,
               TestAllocator>()
        .setEventBase(&eventBase_)
        .setHead(&transport_)
        .setTail(&app_)
        .setAllocator(&allocator_)
        .addNextOutbound<Handler>(
            yielding_srpt_test_tag,
            std::make_unique<Handler>(QuantumFragmentationHandlerConfig{
                .streamQuantum = kQuantum,
                .callbackBudget = kBudget,
            }))
        .build();
  }

  auto buildStaticPipeline() {
    return cp::StaticPipelineBuilder<
               PipelineHeadHandler,
               PipelineTailHandler,
               TestAllocator>()
        .setEventBase(&eventBase_)
        .setHead(&transport_)
        .setTail(&app_)
        .setAllocator(&allocator_)
        .addNextOutbound<Handler>(
            yielding_srpt_test_tag,
            QuantumFragmentationHandlerConfig{
                .streamQuantum = kQuantum,
                .callbackBudget = kBudget,
            })
        .build();
  }

  void verifyHookWiring(auto pipeline) {
    transport_.setWriteResult(cp::Result::Backpressure);
    EXPECT_THAT(
        pipeline->fireWrite(makeFrame(FrameType::REQUEST_RESPONSE, 1, 250)),
        Eq(cp::Result::Success));
    eventBase_.loopOnce(EVLOOP_NONBLOCK);
    EXPECT_THAT(transport_.writeCount(), Eq(1));
    EXPECT_THAT(pipeline->hasPendingWriteReady(), IsTrue());
    EXPECT_THAT(
        pipeline->fireWrite(makeFrame(FrameType::REQUEST_RESPONSE, 3, 150)),
        Eq(cp::Result::Backpressure));
    EXPECT_THAT(transport_.writeCount(), Eq(1));

    transport_.setWriteResult(cp::Result::Success);
    pipeline->onWriteReady();
    EXPECT_THAT(pipeline->hasPendingWriteReady(), IsFalse());
    EXPECT_THAT(transport_.writeCount(), Eq(1));
    eventBase_.loopOnce(EVLOOP_NONBLOCK);
    EXPECT_THAT(transport_.writeCount(), Eq(3));
  }

  cp::PipelineImpl::Ptr buildDynamicPipelineWithWriterAbove() {
    return cp::PipelineBuilder<
               PipelineHeadHandler,
               PipelineTailHandler,
               TestAllocator>()
        .setEventBase(&eventBase_)
        .setHead(&transport_)
        .setTail(&app_)
        .setAllocator(&allocator_)
        .addNextOutbound<Handler>(
            yielding_srpt_test_tag,
            std::make_unique<Handler>(QuantumFragmentationHandlerConfig{
                .streamQuantum = kQuantum,
                .callbackBudget = kBudget,
            }))
        .addNextOutbound<QueueingWriter>(
            queueing_writer_test_tag, std::make_unique<QueueingWriter>())
        .build();
  }

  auto buildStaticPipelineWithWriterAbove() {
    return cp::StaticPipelineBuilder<
               PipelineHeadHandler,
               PipelineTailHandler,
               TestAllocator>()
        .setEventBase(&eventBase_)
        .setHead(&transport_)
        .setTail(&app_)
        .setAllocator(&allocator_)
        .addNextOutbound<Handler>(
            yielding_srpt_test_tag,
            QuantumFragmentationHandlerConfig{
                .streamQuantum = kQuantum,
                .callbackBudget = kBudget,
            })
        .addNextOutbound<QueueingWriter>(queueing_writer_test_tag)
        .build();
  }

  // The writer above queues the frames that arrive while the transport is
  // full. A write-ready pass wakes the fragmenter before the writer, so once
  // the fragmenter clears its block, the writer must get to send its queue.
  void verifyWriterAboveSendsItsQueueAfterTheBlockClears(auto pipeline) {
    transport_.setWriteResult(cp::Result::Backpressure);
    for (const uint32_t streamId : {1, 3, 5, 7}) {
      EXPECT_THAT(
          pipeline->fireWrite(
              makeFrame(FrameType::REQUEST_RESPONSE, streamId, 50)),
          Eq(cp::Result::Backpressure));
    }
    EXPECT_THAT(transport_.writeCount(), Eq(1));

    // The first write of the next pass fills the transport again, so the
    // writer sends the rest of its queue in the pass after it.
    transport_.setWriteResult(cp::Result::Success);
    transport_.setNextWriteResults({cp::Result::Backpressure});
    pipeline->onWriteReady();
    pipeline->onWriteReady();

    EXPECT_THAT(
        std::tuple(transport_.writeCount(), pipeline->hasPendingWriteReady()),
        Eq(std::tuple(size_t{4}, false)));
  }

  folly::EventBase eventBase_;
  PipelineHeadHandler transport_;
  PipelineTailHandler app_;
  TestAllocator allocator_;
};

TEST_F(
    YieldingSrptFrameFragmentationHandlerPipelineTest,
    DynamicPipelineWiresWriteReadyHook) {
  verifyHookWiring(buildDynamicPipeline());
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerPipelineTest,
    StaticPipelineWiresWriteReadyHook) {
  verifyHookWiring(buildStaticPipeline());
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerPipelineTest,
    DynamicPipelineLetsTheWriterAboveSendAfterTheBlockClears) {
  verifyWriterAboveSendsItsQueueAfterTheBlockClears(
      buildDynamicPipelineWithWriterAbove());
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerPipelineTest,
    StaticPipelineLetsTheWriterAboveSendAfterTheBlockClears) {
  verifyWriterAboveSendsItsQueueAfterTheBlockClears(
      buildStaticPipelineWithWriterAbove());
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerPipelineTest,
    TheWriterAboveSendsAFrameLongerThanAQuantumAfterTheBlockClears) {
  auto pipeline = buildDynamicPipelineWithWriterAbove();
  transport_.setWriteResult(cp::Result::Backpressure);
  (void)pipeline->fireWrite(makeFrame(FrameType::REQUEST_RESPONSE, 1, 50));
  for (const uint32_t streamId : {3, 5, 7}) {
    (void)pipeline->fireWrite(makeFrame(
        FrameType::REQUEST_RESPONSE, streamId, streamId == 5 ? 250 : 50));
  }

  // Stream 3 fills the transport again. In the next pass the writer above
  // sends stream 5, which the fragmenter queues because it does not fit in
  // one quantum.
  transport_.setWriteResult(cp::Result::Success);
  transport_.setNextWriteResults({cp::Result::Backpressure});
  pipeline->onWriteReady();
  pipeline->onWriteReady();

  EXPECT_THAT(
      std::tuple(transport_.writeCount(), pipeline->hasPendingWriteReady()),
      Eq(std::tuple(size_t{3}, false)));
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerPipelineTest,
    AFullTransportGetsOneWriteReadyRound) {
  auto pipeline = buildDynamicPipeline();
  transport_.setWriteResult(cp::Result::Backpressure);
  EXPECT_THAT(
      pipeline->fireWrite(makeFrame(FrameType::REQUEST_RESPONSE, 1, 50)),
      Eq(cp::Result::Backpressure));
  // The application writes one more reply each time it hears write-ready, and
  // the transport stays full. The cap only keeps a failing run finite.
  uint32_t nextStreamId = 3;
  app_.setOnWriteReady([&] {
    if (nextStreamId <= 21) {
      (void)pipeline->fireWrite(
          makeFrame(FrameType::REQUEST_RESPONSE, nextStreamId, 50));
      nextStreamId += 2;
    }
  });

  pipeline->onWriteReady();

  EXPECT_THAT(
      std::tuple(app_.writeReadyCount(), transport_.writeCount()),
      Eq(std::tuple(size_t{1}, size_t{2})));
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerPipelineTest,
    TwoWritesPerWakeGetOneRoundPerWriteReady) {
  auto pipeline = buildDynamicPipeline();
  transport_.setWriteResult(cp::Result::Backpressure);
  EXPECT_THAT(
      pipeline->fireWrite(makeFrame(FrameType::REQUEST_RESPONSE, 1, 50)),
      Eq(cp::Result::Backpressure));
  // The application writes two more replies each time it hears write-ready,
  // and the transport stays full. So the second reply reaches the handler
  // after the first one blocked it again. The cap only keeps a failing run
  // finite.
  uint32_t nextStreamId = 3;
  const auto writeReply = [&] {
    if (nextStreamId <= 41) {
      (void)pipeline->fireWrite(
          makeFrame(FrameType::REQUEST_RESPONSE, nextStreamId, 50));
      nextStreamId += 2;
    }
  };
  app_.setOnWriteReady([&] {
    writeReply();
    writeReply();
  });

  pipeline->onWriteReady();
  pipeline->onWriteReady();

  EXPECT_THAT(
      std::tuple(app_.writeReadyCount(), transport_.writeCount()),
      Eq(std::tuple(size_t{2}, size_t{5})));
}

TEST_F(
    YieldingSrptFrameFragmentationHandlerTest,
    SustainedSingleQuantumTrafficLeavesTheBudgetToThePinnedLargeRequest) {
  Handler handler({.streamQuantum = kQuantum, .callbackBudget = kQuantum});
  handler.handlerAdded(context_);
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 1, 300));

  for (const uint32_t smallStreamId : {3u, 5u, 7u}) {
    (void)handler.onWrite(
        context_,
        makeFrame(FrameType::REQUEST_RESPONSE, smallStreamId, kQuantum));
    runLoop();
  }

  EXPECT_THAT(streamIds(context_.written()), ElementsAre(3, 1, 5, 1, 7, 1));
  EXPECT_THAT(handler.hasPendingStream(1), IsFalse());
}

} // namespace
} // namespace apache::thrift::fast_thrift::frame::write::handler
