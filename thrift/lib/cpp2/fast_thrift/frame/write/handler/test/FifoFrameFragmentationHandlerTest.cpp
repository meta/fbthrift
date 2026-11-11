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
#include <optional>
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
#include <thrift/lib/cpp2/fast_thrift/frame/write/handler/FifoFrameFragmentationHandler.h>
#include <thrift/lib/cpp2/fast_thrift/transport/WriteCompletion.h>

namespace apache::thrift::fast_thrift::frame::write::handler {
namespace {

using namespace testing;

namespace cp = channel_pipeline;
using cp::test::TestAllocator;

HANDLER_TAG(fifo_fragmentation_test);
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

constexpr size_t kStreamQuantum = 64 * 1024;
constexpr size_t kCallbackBudget = 16 * 1024 * 1024;
constexpr size_t kFullTurnsPerCallback = kCallbackBudget / kStreamQuantum;
constexpr size_t kCloseResponseSize = 8 * 1024 * 1024;
constexpr std::array<size_t, 2> kCloseFragmentSizes{512 * 1024, 1024 * 1024};

struct TestPublishedEvent : channel_pipeline::EventTag<int> {};
struct TestSubscribedEvent : channel_pipeline::EventTag<int> {};

struct RecordingTracker {
  using PublishedEvents = channel_pipeline::Events<TestPublishedEvent>;
  using SubscribedEvents = channel_pipeline::Events<TestSubscribedEvent>;

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

  [[maybe_unused]] void onFlush() noexcept { ++flushCount; }

  template <channel_pipeline::PipelineEvent E, typename Context>
    requires std::same_as<E, TestSubscribedEvent>
  void on(
      Context& /*ctx*/,
      const int& event,
      bool handlerHasPendingWrites) noexcept {
    subscribedEvents.push_back(event);
    pendingStates.push_back(handlerHasPendingWrites);
  }

  std::vector<FragmentCall> fragments;
  size_t flushCount{0};
  size_t abortCount{0};
  std::vector<int> subscribedEvents;
  std::vector<bool> pendingStates;
};

static_assert(FragmentCompletionTracker<RecordingTracker>);

using Handler = FifoFrameFragmentationHandlerT<RecordingTracker>;

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

using CompletionHandler = FifoFrameFragmentationHandlerT<
    FragmentCompletionTrackerT<TestEventFactory>>;

struct CapturedFrame {
  FrameType frameType;
  uint32_t streamId;
  size_t dataSize;
  bool follows;
  bool complete;
  bool next;
};

class TestContext {
 public:
  explicit TestContext(folly::EventBase* eventBase) : eventBase_(eventBase) {}

  folly::EventBase* eventBase() const noexcept { return eventBase_; }

  channel_pipeline::Result fireWrite(
      channel_pipeline::TypeErasedBox&& msg) noexcept {
    const size_t attempt = ++writeAttemptCount_;
    const ComposedFrame& frame = msg.get<ComposedFrame>();
    const CapturedFrame captured{
        .frameType = frame.frameType,
        .streamId = frame.streamId,
        .dataSize = frame.data ? frame.data->computeChainDataLength() : 0,
        .follows = frame.follows,
        .complete = frame.complete,
        .next = frame.next,
    };
    if (writeAttemptObserver_) {
      writeAttemptObserver_();
    }
    if (errorAttempt_.has_value() && attempt == *errorAttempt_) {
      return channel_pipeline::Result::Error;
    }
    written_.push_back(captured);
    if (writeObserver_) {
      writeObserver_(captured);
    }
    if (const std::function<void()> reentrantWrite =
            std::exchange(reentrantWrite_, nullptr);
        reentrantWrite) {
      reentrantWrite();
    }
    return attempt <= results_.size() ? results_[attempt - 1] : result_;
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

  void setResult(channel_pipeline::Result result) noexcept { result_ = result; }

  void setResults(std::vector<channel_pipeline::Result> results) {
    results_ = std::move(results);
  }

  void setErrorOnAttempt(size_t attempt) noexcept { errorAttempt_ = attempt; }

  void setReentrantWrite(std::function<void()> write) {
    reentrantWrite_ = std::move(write);
  }

  void setWriteObserver(std::function<void(const CapturedFrame&)> observer) {
    writeObserver_ = std::move(observer);
  }

  void setWriteAttemptObserver(std::function<void()> observer) {
    writeAttemptObserver_ = std::move(observer);
  }

  [[nodiscard]] const std::vector<CapturedFrame>& written() const noexcept {
    return written_;
  }

  [[nodiscard]] size_t writeAttemptCount() const noexcept {
    return writeAttemptCount_;
  }

  [[nodiscard]] size_t fireExceptionCount() const noexcept {
    return fireExceptionCount_;
  }

  [[nodiscard]] bool awaitingWriteReady() const noexcept {
    return awaitingWriteReady_;
  }

  [[nodiscard]] const std::vector<TestFrameWriteCompleteEvent>& completed()
      const noexcept {
    return completed_;
  }

  [[nodiscard]] const folly::exception_wrapper& lastException() const noexcept {
    return lastException_;
  }

 private:
  folly::EventBase* const eventBase_;
  channel_pipeline::Result result_{channel_pipeline::Result::Success};
  std::vector<channel_pipeline::Result> results_;
  std::optional<size_t> errorAttempt_;
  std::function<void()> reentrantWrite_;
  std::function<void(const CapturedFrame&)> writeObserver_;
  std::function<void()> writeAttemptObserver_;
  std::vector<CapturedFrame> written_;
  std::vector<TestFrameWriteCompleteEvent> completed_;
  size_t writeAttemptCount_{0};
  size_t fireExceptionCount_{0};
  bool awaitingWriteReady_{false};
  NoPipeline pipeline_;
  folly::exception_wrapper lastException_;
};

static_assert(channel_pipeline::OutboundHandler<Handler, TestContext>);
static_assert(std::same_as<
              Handler::PublishedEvents,
              channel_pipeline::Events<TestPublishedEvent>>);
static_assert(std::same_as<
              Handler::SubscribedEvents,
              channel_pipeline::Events<TestSubscribedEvent>>);

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
    std::memset(frame.data->writableData(), 'x', dataSize);
  }
  if (frameType == FrameType::PAYLOAD) {
    frame.next = true;
  }
  return channel_pipeline::TypeErasedBox(std::move(frame));
}

std::vector<uint32_t> streamIds(const std::vector<CapturedFrame>& frames) {
  std::vector<uint32_t> result;
  result.reserve(frames.size());
  for (const CapturedFrame& frame : frames) {
    result.push_back(frame.streamId);
  }
  return result;
}

std::vector<std::pair<uint32_t, size_t>> streamDataSizes(
    const std::vector<CapturedFrame>& frames) {
  std::vector<std::pair<uint32_t, size_t>> result;
  result.reserve(frames.size());
  for (const CapturedFrame& frame : frames) {
    result.emplace_back(frame.streamId, frame.dataSize);
  }
  return result;
}

size_t totalDataSize(const std::vector<CapturedFrame>& frames) {
  size_t total = 0;
  for (const CapturedFrame& frame : frames) {
    total += frame.dataSize;
  }
  return total;
}

class FifoFrameFragmentationHandlerTest : public Test {
 protected:
  void runLoop() { eventBase_.loopOnce(EVLOOP_NONBLOCK); }

  folly::EventBase eventBase_;
  TestContext context_{&eventBase_};
};

TEST_F(FifoFrameFragmentationHandlerTest, RefusesDataFramesForInvalidConfig) {
  const auto expectRejected =
      [this](const QuantumFragmentationHandlerConfig& config) {
        Handler handler(config);
        handler.handlerAdded(context_);

        EXPECT_THAT(
            handler.onWrite(
                context_, makeFrame(FrameType::REQUEST_RESPONSE, 1, 1)),
            Eq(channel_pipeline::Result::Error));
        EXPECT_THAT(context_.written(), IsEmpty());
        EXPECT_THAT(
            std::tuple(
                handler.pendingStreamCount(),
                handler.pendingQueueEntryCount(),
                handler.isScheduled()),
            Eq(std::tuple(size_t{0}, size_t{0}, false)));

        handler.handlerRemoved(context_);
      };

  expectRejected({.streamQuantum = 0});
  expectRejected({.streamQuantum = kMaxStreamQuantum + 1});
  expectRejected({.callbackBudget = 0});
}

TEST_F(
    FifoFrameFragmentationHandlerTest,
    RefusesMetadataAboveTheLargestStreamQuantum) {
  Handler handler;
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

  handler.handlerRemoved(context_);
}

TEST_F(
    FifoFrameFragmentationHandlerTest, RefusalLogsTheMetadataSizeAndTheLimit) {
  // The refusal logs at most once a minute in a process. A threadsafe death
  // test runs the refusal in a new process, so refusals of earlier tests in
  // this binary cannot hide the log line.
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  EXPECT_EXIT(
      {
        const auto logs = std::make_shared<folly::TestLogHandler>();
        folly::LoggerDB::get().getCategory("")->addHandler(logs);
        Handler handler;
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
          "FifoFrameFragmentationHandler refused a data frame with 16776704 "
          "metadata bytes. The limit is 16776703 bytes, and the config is "
          "valid."));
}

TEST_F(
    FifoFrameFragmentationHandlerTest, SendsAFrameThatFitsInOneQuantumAtOnce) {
  Handler handler;
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
              kStreamQuantum / 2,
              /*complete=*/false,
              kStreamQuantum / 2)),
      Eq(channel_pipeline::Result::Success));

  ASSERT_THAT(context_.written(), SizeIs(kDataFrameTypes.size() + 1));
  for (size_t index = 0; index < kDataFrameTypes.size(); ++index) {
    EXPECT_THAT(
        context_.written()[index].frameType, Eq(kDataFrameTypes[index]));
  }
  EXPECT_THAT(
      std::tuple(
          context_.written().back().dataSize,
          context_.written().back().follows),
      Eq(std::tuple(kStreamQuantum / 2, false)));
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
    FifoFrameFragmentationHandlerTest,
    AFrameThatFitsWaitsBehindTheQueuedFramesOfItsStream) {
  Handler handler;
  handler.handlerAdded(context_);
  (void)handler.onWrite(
      context_, makeFrame(FrameType::PAYLOAD, 1, kStreamQuantum + 1));

  EXPECT_THAT(
      handler.onWrite(context_, makeFrame(FrameType::PAYLOAD, 1, 2)),
      Eq(channel_pipeline::Result::Success));
  EXPECT_THAT(
      handler.onWrite(context_, makeFrame(FrameType::PAYLOAD, 3, 3)),
      Eq(channel_pipeline::Result::Success));

  EXPECT_THAT(streamIds(context_.written()), ElementsAre(3));

  runLoop();
  runLoop();
  runLoop();

  const std::vector<std::pair<uint32_t, size_t>> expected{
      {3, 3}, {1, kStreamQuantum}, {1, 1}, {1, 2}};
  EXPECT_THAT(streamDataSizes(context_.written()), Eq(expected));
  EXPECT_THAT(handler.hasPendingWrites(), IsFalse());
}

TEST_F(FifoFrameFragmentationHandlerTest, DefersAllDataFramesThatDoNotFit) {
  Handler handler;
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
        handler.onWrite(
            context_, makeFrame(frameType, streamId, kStreamQuantum + 1)),
        Eq(channel_pipeline::Result::Success));
    streamId += 2;
  }
  EXPECT_THAT(
      handler.onWrite(
          context_,
          makeFrame(
              FrameType::PAYLOAD,
              streamId,
              kStreamQuantum,
              /*complete=*/false,
              1)),
      Eq(channel_pipeline::Result::Success));

  EXPECT_THAT(context_.written(), IsEmpty());
  EXPECT_THAT(handler.pendingStreamCount(), Eq(6));
  EXPECT_THAT(handler.isScheduled(), IsTrue());
}

TEST_F(FifoFrameFragmentationHandlerTest, UsesAStrict64KiBQuantum) {
  Handler handler;
  handler.handlerAdded(context_);
  (void)handler.onWrite(
      context_, makeFrame(FrameType::PAYLOAD, 1, kStreamQuantum + 1));

  runLoop();

  ASSERT_THAT(context_.written(), SizeIs(1));
  EXPECT_THAT(context_.written()[0].dataSize, Eq(kStreamQuantum));
  EXPECT_THAT(context_.written()[0].follows, IsTrue());
  EXPECT_THAT(handler.hasPendingWrites(), IsTrue());

  runLoop();

  ASSERT_THAT(context_.written(), SizeIs(2));
  EXPECT_THAT(context_.written()[1].dataSize, Eq(1));
  EXPECT_THAT(context_.written()[1].follows, IsFalse());
}

TEST_F(FifoFrameFragmentationHandlerTest, UsesAStrict16MiBCallbackBudget) {
  Handler handler;
  handler.handlerAdded(context_);
  for (size_t turn = 0; turn <= kFullTurnsPerCallback; ++turn) {
    (void)handler.onWrite(
        context_,
        makeFrame(
            FrameType::PAYLOAD,
            static_cast<uint32_t>(2 * turn + 1),
            2 * kStreamQuantum));
  }

  runLoop();

  EXPECT_THAT(context_.written(), SizeIs(kFullTurnsPerCallback));
  EXPECT_THAT(totalDataSize(context_.written()), Eq(kCallbackBudget));
  EXPECT_THAT(handler.pendingStreamCount(), Eq(kFullTurnsPerCallback / 2 + 1));
}

TEST_F(
    FifoFrameFragmentationHandlerTest,
    FinishesAStartedTurnPastTheCallbackBudget) {
  const QuantumFragmentationHandlerConfig config{
      .streamQuantum = 1024, .callbackBudget = 2047};
  Handler handler(config);
  handler.handlerAdded(context_);
  (void)handler.onWrite(context_, makeFrame(FrameType::PAYLOAD, 1, 2048));
  (void)handler.onWrite(context_, makeFrame(FrameType::PAYLOAD, 3, 2048));
  (void)handler.onWrite(context_, makeFrame(FrameType::PAYLOAD, 5, 2048));

  runLoop();

  const std::vector<std::pair<uint32_t, size_t>> afterFirstPass{
      {1, 1024}, {1, 1024}};
  EXPECT_THAT(streamDataSizes(context_.written()), Eq(afterFirstPass));
  EXPECT_THAT(handler.pendingStreamCount(), Eq(2));

  runLoop();

  const std::vector<std::pair<uint32_t, size_t>> afterSecondPass{
      {1, 1024}, {1, 1024}, {3, 1024}, {3, 1024}};
  EXPECT_THAT(streamDataSizes(context_.written()), Eq(afterSecondPass));
  EXPECT_THAT(handler.pendingStreamCount(), Eq(1));
}

TEST_F(
    FifoFrameFragmentationHandlerTest,
    SkippedStreamsRunFirstInTheNextCallback) {
  const QuantumFragmentationHandlerConfig config{
      .streamQuantum = 1024, .callbackBudget = 3072};
  Handler handler(config);
  handler.handlerAdded(context_);
  for (const uint32_t streamId : {1u, 3u, 5u, 7u, 9u}) {
    (void)handler.onWrite(
        context_, makeFrame(FrameType::PAYLOAD, streamId, 2048));
  }

  runLoop();

  EXPECT_THAT(streamIds(context_.written()), ElementsAre(1, 1, 3));

  runLoop();

  EXPECT_THAT(streamIds(context_.written()), ElementsAre(1, 1, 3, 3, 5, 5));
}

TEST_F(
    FifoFrameFragmentationHandlerTest,
    HoldsZeroDataFramesUntilTheBudgetRefills) {
  const QuantumFragmentationHandlerConfig config{
      .streamQuantum = 1024, .callbackBudget = 1024};
  Handler handler(config);
  handler.handlerAdded(context_);
  (void)handler.onWrite(context_, makeFrame(FrameType::PAYLOAD, 1, 1025));
  // Metadata above the quantum keeps this frame queued, and its only
  // fragment carries no data.
  (void)handler.onWrite(
      context_, makeFrame(FrameType::PAYLOAD, 3, 0, /*complete=*/false, 1025));

  runLoop();

  EXPECT_THAT(streamIds(context_.written()), ElementsAre(1));

  runLoop();

  EXPECT_THAT(streamIds(context_.written()), ElementsAre(1, 1, 3));
  EXPECT_THAT(context_.written()[2].dataSize, Eq(0));
}

TEST_F(
    FifoFrameFragmentationHandlerTest,
    KeepsPendingCountsAndByteTotalsAcrossCallbacks) {
  const QuantumFragmentationHandlerConfig config{
      .streamQuantum = 1024, .callbackBudget = 2048};
  Handler handler(config);
  handler.handlerAdded(context_);
  (void)handler.onWrite(context_, makeFrame(FrameType::PAYLOAD, 1, 3000));
  (void)handler.onWrite(context_, makeFrame(FrameType::PAYLOAD, 3, 1500));

  runLoop();

  EXPECT_THAT(totalDataSize(context_.written()), Eq(2048));
  EXPECT_THAT(handler.pendingStreamCount(), Eq(2));

  runLoop();

  EXPECT_THAT(totalDataSize(context_.written()), Eq(4024));
  EXPECT_THAT(handler.pendingStreamCount(), Eq(1));

  runLoop();

  EXPECT_THAT(totalDataSize(context_.written()), Eq(4500));
  EXPECT_THAT(handler.pendingStreamCount(), Eq(0));

  runLoop();

  EXPECT_THAT(totalDataSize(context_.written()), Eq(4500));
  EXPECT_THAT(handler.pendingStreamCount(), Eq(0));
  EXPECT_THAT(handler.pendingQueueEntryCount(), Eq(0));
  EXPECT_THAT(handler.tracker().flushCount, Eq(1));
}

TEST_F(FifoFrameFragmentationHandlerTest, DefersALargeCompleteFrame) {
  Handler handler;
  handler.handlerAdded(context_);

  EXPECT_THAT(
      handler.onWrite(
          context_, makeFrame(FrameType::PAYLOAD, 1, kStreamQuantum + 1, true)),
      Eq(channel_pipeline::Result::Success));
  EXPECT_THAT(context_.written(), IsEmpty());
  EXPECT_THAT(handler.pendingStreamCount(), Eq(1));
  EXPECT_THAT(handler.pendingQueueEntryCount(), Eq(1));

  runLoop();
  runLoop();

  ASSERT_THAT(context_.written(), SizeIs(2));
  EXPECT_THAT(context_.written()[0].complete, IsFalse());
  EXPECT_THAT(context_.written()[1].streamId, Eq(1));
  EXPECT_THAT(context_.written()[1].dataSize, Eq(1));
  EXPECT_THAT(context_.written()[1].complete, IsTrue());
  EXPECT_THAT(handler.hasPendingWrites(), IsFalse());
}

TEST_F(
    FifoFrameFragmentationHandlerTest,
    RefusesASecondDataFrameOnARequestResponseStream) {
  Handler handler;
  handler.handlerAdded(context_);
  EXPECT_THAT(
      handler.onWrite(
          context_,
          makeFrame(FrameType::REQUEST_RESPONSE, 1, kStreamQuantum + 1)),
      Eq(channel_pipeline::Result::Success));

  EXPECT_THAT(
      handler.onWrite(context_, makeFrame(FrameType::PAYLOAD, 1, 3)),
      Eq(channel_pipeline::Result::Error));

  runLoop();
  runLoop();

  ASSERT_THAT(context_.written(), SizeIs(2));
  EXPECT_THAT(
      std::tuple(
          context_.written()[0].frameType, context_.written()[0].dataSize),
      Eq(std::tuple(FrameType::REQUEST_RESPONSE, kStreamQuantum)));
  EXPECT_THAT(context_.written()[1].dataSize, Eq(1));
  const std::vector<RecordingTracker::FragmentCall> expectedFragments{
      {.streamId = 1, .isLastFragment = false},
      {.streamId = 1, .isLastFragment = true}};
  EXPECT_THAT(handler.tracker().fragments, Eq(expectedFragments));
  EXPECT_THAT(handler.hasPendingWrites(), IsFalse());
}

TEST_F(FifoFrameFragmentationHandlerTest, SizesAPassByLiveStreamsAfterACancel) {
  Handler handler;
  handler.handlerAdded(context_);
  for (const uint32_t streamId : {1u, 3u, 5u}) {
    (void)handler.onWrite(
        context_, makeFrame(FrameType::PAYLOAD, streamId, 2 * kStreamQuantum));
  }
  EXPECT_THAT(
      handler.onWrite(context_, makeFrame(FrameType::CANCEL, 1)),
      Eq(channel_pipeline::Result::Success));

  runLoop();

  EXPECT_THAT(streamIds(context_.written()), ElementsAre(1, 3, 3));

  runLoop();

  EXPECT_THAT(streamIds(context_.written()), ElementsAre(1, 3, 3, 5));
}

TEST_F(FifoFrameFragmentationHandlerTest, VisitsEachStartingEntryOnce) {
  Handler handler;
  handler.handlerAdded(context_);
  (void)handler.onWrite(
      context_, makeFrame(FrameType::PAYLOAD, 1, 2 * kStreamQuantum));
  (void)handler.onWrite(
      context_, makeFrame(FrameType::PAYLOAD, 3, 2 * kStreamQuantum));

  runLoop();

  EXPECT_THAT(streamIds(context_.written()), ElementsAre(1, 1));
  EXPECT_THAT(handler.pendingStreamCount(), Eq(1));
  EXPECT_THAT(handler.isScheduled(), IsTrue());
}

TEST_F(FifoFrameFragmentationHandlerTest, AppendsSameStreamFrames) {
  Handler handler;
  handler.handlerAdded(context_);
  (void)handler.onWrite(
      context_, makeFrame(FrameType::PAYLOAD, 7, kStreamQuantum + 1));
  (void)handler.onWrite(context_, makeFrame(FrameType::PAYLOAD, 7, 2));

  EXPECT_THAT(handler.pendingStreamCount(), Eq(1));
  EXPECT_THAT(handler.pendingQueueEntryCount(), Eq(1));

  runLoop();
  runLoop();

  ASSERT_THAT(context_.written(), SizeIs(2));
  EXPECT_THAT(context_.written()[1].dataSize, Eq(1));

  runLoop();

  ASSERT_THAT(context_.written(), SizeIs(3));
  EXPECT_THAT(context_.written()[2].dataSize, Eq(2));
}

TEST_F(FifoFrameFragmentationHandlerTest, DefersNewArrivalsToTheNextPass) {
  Handler handler;
  handler.handlerAdded(context_);
  (void)handler.onWrite(
      context_, makeFrame(FrameType::PAYLOAD, 1, kStreamQuantum + 1));
  context_.setReentrantWrite([&] {
    EXPECT_THAT(
        handler.onWrite(
            context_, makeFrame(FrameType::PAYLOAD, 3, kStreamQuantum + 1)),
        Eq(channel_pipeline::Result::Success));
  });

  runLoop();

  EXPECT_THAT(streamIds(context_.written()), ElementsAre(1));
  EXPECT_THAT(handler.hasPendingStream(3), IsTrue());
  EXPECT_THAT(handler.isScheduled(), IsTrue());

  runLoop();

  EXPECT_THAT(streamIds(context_.written()), ElementsAre(1, 1, 3));
}

TEST_F(FifoFrameFragmentationHandlerTest, PassesControlFramesThroughInline) {
  Handler handler;
  handler.handlerAdded(context_);
  constexpr std::array<FrameType, 8> kControlFrameTypes{
      FrameType::SETUP,
      FrameType::LEASE,
      FrameType::KEEPALIVE,
      FrameType::REQUEST_N,
      FrameType::METADATA_PUSH,
      FrameType::RESUME,
      FrameType::RESUME_OK,
      FrameType::EXT,
  };

  uint32_t streamId = 20;
  for (const FrameType frameType : kControlFrameTypes) {
    EXPECT_THAT(
        handler.onWrite(context_, makeFrame(frameType, streamId)),
        Eq(channel_pipeline::Result::Success));
    streamId += 2;
  }

  ASSERT_THAT(context_.written(), SizeIs(kControlFrameTypes.size()));
  for (size_t index = 0; index < kControlFrameTypes.size(); ++index) {
    EXPECT_THAT(
        context_.written()[index].frameType, Eq(kControlFrameTypes[index]));
  }
  EXPECT_THAT(handler.isScheduled(), IsFalse());
  EXPECT_THAT(handler.hasPendingWrites(), IsFalse());
}

TEST(
    FifoFrameFragmentationHandlerTerminalTest,
    CancelAndErrorRemoveQueuedStreamData) {
  constexpr std::array<FrameType, 2> kTerminalFrameTypes{
      FrameType::CANCEL,
      FrameType::ERROR,
  };

  for (const FrameType frameType : kTerminalFrameTypes) {
    folly::EventBase eventBase;
    TestContext context(&eventBase);
    Handler handler;
    handler.handlerAdded(context);
    (void)handler.onWrite(
        context, makeFrame(FrameType::PAYLOAD, 11, kStreamQuantum + 1));

    EXPECT_THAT(
        handler.onWrite(context, makeFrame(frameType, 11)),
        Eq(channel_pipeline::Result::Success));

    EXPECT_THAT(handler.hasPendingWrites(), IsFalse());
    EXPECT_THAT(
        std::tuple(
            handler.pendingStreamCount(), handler.pendingQueueEntryCount()),
        Eq(std::tuple(size_t{0}, size_t{1})));
    ASSERT_THAT(context.written(), SizeIs(1));
    EXPECT_THAT(context.written()[0].frameType, Eq(frameType));

    eventBase.loopOnce(EVLOOP_NONBLOCK);
    EXPECT_THAT(context.written(), SizeIs(1));
    handler.handlerRemoved(context);
  }
}

TEST_F(
    FifoFrameFragmentationHandlerTest,
    CancelAndErrorRemoveQueuedOneFrameStreams) {
  Handler handler;
  handler.handlerAdded(context_);
  (void)handler.onWrite(
      context_,
      makeFrame(FrameType::REQUEST_RESPONSE, 1, 2 * kStreamQuantum + 1));
  (void)handler.onWrite(
      context_,
      makeFrame(FrameType::PAYLOAD, 3, kStreamQuantum + 1, /*complete=*/true));

  runLoop();
  ASSERT_THAT(streamIds(context_.written()), ElementsAre(1, 1));

  EXPECT_THAT(
      handler.onWrite(context_, makeFrame(FrameType::CANCEL, 1)),
      Eq(channel_pipeline::Result::Success));
  EXPECT_THAT(
      handler.onWrite(context_, makeFrame(FrameType::ERROR, 3)),
      Eq(channel_pipeline::Result::Success));

  ASSERT_THAT(context_.written(), SizeIs(4));
  EXPECT_THAT(context_.written()[2].frameType, Eq(FrameType::CANCEL));
  EXPECT_THAT(context_.written()[3].frameType, Eq(FrameType::ERROR));
  EXPECT_THAT(handler.hasPendingWrites(), IsFalse());
  EXPECT_THAT(handler.pendingStreamCount(), Eq(0));

  runLoop();

  EXPECT_THAT(context_.written(), SizeIs(4));
  EXPECT_THAT(handler.pendingQueueEntryCount(), Eq(2));
}

TEST_F(
    FifoFrameFragmentationHandlerTest, PipelineInactiveDuringAPassEndsThePass) {
  Handler handler;
  handler.handlerAdded(context_);
  // A socket write that fails inside the pass closes the pipeline before the
  // write returns.
  context_.setWriteObserver(
      [&](const CapturedFrame&) { handler.onPipelineInactive(context_); });
  (void)handler.onWrite(
      context_, makeFrame(FrameType::PAYLOAD, 1, kStreamQuantum + 1));
  (void)handler.onWrite(
      context_, makeFrame(FrameType::PAYLOAD, 3, kStreamQuantum + 1));

  runLoop();

  EXPECT_THAT(
      std::tuple(
          context_.writeAttemptCount(),
          handler.hasPendingWrites(),
          handler.isScheduled(),
          context_.fireExceptionCount()),
      Eq(std::tuple(size_t{1}, false, false, size_t{0})));
}

TEST_F(
    FifoFrameFragmentationHandlerTest,
    HandlerRemovalCancelsCallbackAndClearsWork) {
  Handler handler;
  handler.handlerAdded(context_);
  (void)handler.onWrite(
      context_, makeFrame(FrameType::PAYLOAD, 1, kStreamQuantum + 1));

  handler.handlerRemoved(context_);

  EXPECT_THAT(handler.isScheduled(), IsFalse());
  EXPECT_THAT(handler.pendingStreamCount(), Eq(0));
  EXPECT_THAT(handler.pendingQueueEntryCount(), Eq(0));
  runLoop();
  EXPECT_THAT(context_.written(), IsEmpty());
}

TEST_F(FifoFrameFragmentationHandlerTest, HandlerRemovalCancelsWriteReadyHook) {
  Handler handler;
  handler.handlerAdded(context_);
  context_.setResult(channel_pipeline::Result::Backpressure);
  (void)handler.onWrite(context_, makeFrame(FrameType::REQUEST_RESPONSE, 1, 1));
  runLoop();
  EXPECT_THAT(context_.awaitingWriteReady(), IsTrue());

  handler.handlerRemoved(context_);

  EXPECT_THAT(context_.awaitingWriteReady(), IsFalse());
  EXPECT_THAT(handler.hasPendingWrites(), IsFalse());
  EXPECT_THAT(handler.isScheduled(), IsFalse());
}

TEST_F(
    FifoFrameFragmentationHandlerTest,
    PipelineInactiveCancelsCallbackAndClearsWork) {
  Handler handler;
  handler.handlerAdded(context_);
  (void)handler.onWrite(
      context_, makeFrame(FrameType::PAYLOAD, 1, kStreamQuantum + 1));

  handler.onPipelineInactive(context_);

  EXPECT_THAT(handler.isScheduled(), IsFalse());
  EXPECT_THAT(handler.pendingStreamCount(), Eq(0));
  EXPECT_THAT(handler.pendingQueueEntryCount(), Eq(0));
  runLoop();
  EXPECT_THAT(context_.written(), IsEmpty());
}

TEST_F(
    FifoFrameFragmentationHandlerTest, PipelineInactiveCancelsWriteReadyHook) {
  Handler handler;
  handler.handlerAdded(context_);
  context_.setResult(channel_pipeline::Result::Backpressure);
  (void)handler.onWrite(context_, makeFrame(FrameType::REQUEST_RESPONSE, 1, 1));
  runLoop();
  EXPECT_THAT(context_.awaitingWriteReady(), IsTrue());

  handler.onPipelineInactive(context_);

  EXPECT_THAT(context_.awaitingWriteReady(), IsFalse());
  EXPECT_THAT(handler.hasPendingWrites(), IsFalse());
  EXPECT_THAT(handler.isScheduled(), IsFalse());
}

TEST_F(
    FifoFrameFragmentationHandlerTest, DownstreamErrorCancelsWriteReadyHook) {
  Handler handler;
  handler.handlerAdded(context_);
  (void)handler.onWrite(
      context_, makeFrame(FrameType::PAYLOAD, 1, kStreamQuantum + 1));
  (void)handler.onWrite(
      context_, makeFrame(FrameType::PAYLOAD, 3, kStreamQuantum + 1));
  context_.setResults({channel_pipeline::Result::Backpressure});
  context_.setErrorOnAttempt(2);

  runLoop();

  EXPECT_THAT(
      std::tuple(
          context_.fireExceptionCount(),
          context_.awaitingWriteReady(),
          handler.isBackpressured()),
      Eq(std::tuple(size_t{1}, false, false)));
}

TEST(
    FifoFrameFragmentationHandlerCloseTest,
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
    FifoFrameFragmentationHandlerCloseTest,
    ErrorBeforeCloseAbortsRecordAndClearsRemainder) {
  for (const size_t fragmentSize : kCloseFragmentSizes) {
    folly::EventBase eventBase;
    TestContext context(&eventBase);
    context.setErrorOnAttempt(2);
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
    FifoFrameFragmentationHandlerCloseTest,
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

TEST_F(
    FifoFrameFragmentationHandlerTest,
    DownstreamErrorClearsQueuedWorkAndPropagatesException) {
  Handler handler;
  handler.handlerAdded(context_);
  (void)handler.onWrite(
      context_, makeFrame(FrameType::PAYLOAD, 1, kStreamQuantum + 1));
  (void)handler.onWrite(
      context_, makeFrame(FrameType::PAYLOAD, 3, kStreamQuantum + 1));
  context_.setErrorOnAttempt(1);

  runLoop();

  EXPECT_THAT(context_.writeAttemptCount(), Eq(1));
  EXPECT_THAT(context_.written(), IsEmpty());
  EXPECT_THAT(handler.tracker().fragments, IsEmpty());
  EXPECT_THAT(handler.tracker().flushCount, Eq(0));
  EXPECT_THAT(handler.pendingStreamCount(), Eq(0));
  EXPECT_THAT(handler.pendingQueueEntryCount(), Eq(0));
  EXPECT_THAT(handler.hasPendingWrites(), IsFalse());
  EXPECT_THAT(handler.isScheduled(), IsFalse());
  EXPECT_THAT(context_.fireExceptionCount(), Eq(1));
  EXPECT_THAT(
      std::string(context_.lastException().what()),
      HasSubstr("FifoFrameFragmentationHandler: downstream write failed"));
}

TEST_F(FifoFrameFragmentationHandlerTest, BackpressureDoesNotStopThePass) {
  Handler handler;
  handler.handlerAdded(context_);
  context_.setResults(
      {channel_pipeline::Result::Success,
       channel_pipeline::Result::Backpressure});
  for (const uint32_t streamId : {1u, 3u, 5u}) {
    (void)handler.onWrite(
        context_,
        makeFrame(FrameType::REQUEST_RESPONSE, streamId, kStreamQuantum + 1));
  }

  runLoop();

  EXPECT_THAT(streamIds(context_.written()), ElementsAre(1, 1, 3));
  EXPECT_THAT(handler.tracker().fragments, SizeIs(3));
  EXPECT_THAT(
      std::tuple(
          handler.isBackpressured(),
          context_.awaitingWriteReady(),
          handler.isScheduled()),
      Eq(std::tuple(true, true, true)));
}

TEST_F(
    FifoFrameFragmentationHandlerTest,
    DataQueuedUnderBackpressureReturnsBackpressure) {
  Handler handler;
  handler.handlerAdded(context_);
  context_.setResult(channel_pipeline::Result::Backpressure);
  (void)handler.onWrite(context_, makeFrame(FrameType::REQUEST_RESPONSE, 1, 1));
  runLoop();

  EXPECT_THAT(
      handler.onWrite(
          context_,
          makeFrame(FrameType::REQUEST_RESPONSE, 3, kStreamQuantum + 1)),
      Eq(channel_pipeline::Result::Backpressure));
  EXPECT_THAT(
      std::tuple(handler.hasPendingStream(3), handler.isScheduled()),
      Eq(std::tuple(true, true)));
  EXPECT_THAT(streamIds(context_.written()), ElementsAre(1));

  runLoop();
  EXPECT_THAT(streamIds(context_.written()), ElementsAre(1, 3));
}

TEST_F(
    FifoFrameFragmentationHandlerTest,
    PassThroughBackpressureReportsUntilWriteReady) {
  Handler handler;
  handler.handlerAdded(context_);
  context_.setResults({channel_pipeline::Result::Backpressure});

  EXPECT_THAT(
      handler.onWrite(context_, makeFrame(FrameType::KEEPALIVE, 0)),
      Eq(channel_pipeline::Result::Backpressure));
  EXPECT_THAT(
      std::tuple(handler.isBackpressured(), context_.awaitingWriteReady()),
      Eq(std::tuple(true, true)));
  EXPECT_THAT(
      handler.onWrite(context_, makeFrame(FrameType::KEEPALIVE, 0)),
      Eq(channel_pipeline::Result::Backpressure));
}

TEST_F(
    FifoFrameFragmentationHandlerTest,
    WriteReadyClearsBackpressureWithoutSending) {
  Handler handler;
  handler.handlerAdded(context_);
  context_.setResult(channel_pipeline::Result::Backpressure);
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 1, kStreamQuantum + 1));
  runLoop();

  context_.setResult(channel_pipeline::Result::Success);
  handler.onWriteReady(context_);

  EXPECT_THAT(
      std::tuple(
          context_.awaitingWriteReady(),
          handler.isBackpressured(),
          context_.written().size()),
      Eq(std::tuple(false, false, size_t{1})));
  EXPECT_THAT(
      handler.onWrite(context_, makeFrame(FrameType::REQUEST_RESPONSE, 3, 1)),
      Eq(channel_pipeline::Result::Success));
}

TEST_F(
    FifoFrameFragmentationHandlerTest,
    QueuedInlineCompletionSeesTheFragmentRecord) {
  CompletionHandler handler;
  handler.handlerAdded(context_);
  context_.setWriteAttemptObserver([&] {
    handler.on<TestBatchWriteCompleteEvent>(
        context_,
        TestBatchWriteCompleteEvent{
            .status = transport::WriteCompletionStatus::Success,
            .frameCount = 1,
            .bytes = 1,
            .quiesced = true,
        });
  });
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 1, kStreamQuantum + 1));

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
    FifoFrameFragmentationHandlerTest,
    PassThroughInlineCompletionSeesTheFragmentRecord) {
  CompletionHandler handler;
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
    FifoFrameFragmentationHandlerTest,
    QueuedWriteErrorAbortsTheRecordedFragment) {
  Handler handler;
  handler.handlerAdded(context_);
  size_t recordsDuringWrite = 0;
  context_.setWriteAttemptObserver(
      [&] { recordsDuringWrite = handler.tracker().fragments.size(); });
  context_.setErrorOnAttempt(1);
  (void)handler.onWrite(
      context_, makeFrame(FrameType::REQUEST_RESPONSE, 1, kStreamQuantum + 1));

  runLoop();

  EXPECT_THAT(recordsDuringWrite, Eq(1));
  EXPECT_THAT(handler.tracker().fragments, IsEmpty());
  EXPECT_THAT(handler.tracker().abortCount, Eq(1));
}

TEST_F(
    FifoFrameFragmentationHandlerTest,
    PassThroughWriteErrorAbortsTheRecordedFragment) {
  Handler handler;
  handler.handlerAdded(context_);
  size_t recordsDuringWrite = 0;
  context_.setWriteAttemptObserver(
      [&] { recordsDuringWrite = handler.tracker().fragments.size(); });
  context_.setErrorOnAttempt(1);

  EXPECT_THAT(
      handler.onWrite(context_, makeFrame(FrameType::KEEPALIVE, 0)),
      Eq(channel_pipeline::Result::Error));

  EXPECT_THAT(recordsDuringWrite, Eq(1));
  EXPECT_THAT(handler.tracker().fragments, IsEmpty());
  EXPECT_THAT(handler.tracker().abortCount, Eq(1));
}

TEST_F(
    FifoFrameFragmentationHandlerTest,
    TracksAcceptedFragmentsAndFlushesOnlyAfterFullDrain) {
  Handler handler;
  handler.handlerAdded(context_);
  context_.setResult(channel_pipeline::Result::Backpressure);
  (void)handler.onWrite(
      context_, makeFrame(FrameType::PAYLOAD, 1, kStreamQuantum + 1));

  runLoop();

  const std::vector<RecordingTracker::FragmentCall> afterFirstPass{
      {.streamId = 1, .isLastFragment = false}};
  EXPECT_THAT(handler.tracker().fragments, Eq(afterFirstPass));
  EXPECT_THAT(handler.tracker().flushCount, Eq(0));

  context_.setResult(channel_pipeline::Result::Success);
  runLoop();

  const std::vector<RecordingTracker::FragmentCall> afterSecondPass{
      {.streamId = 1, .isLastFragment = false},
      {.streamId = 1, .isLastFragment = true}};
  EXPECT_THAT(handler.tracker().fragments, Eq(afterSecondPass));
  EXPECT_THAT(handler.tracker().flushCount, Eq(1));
}

TEST_F(
    FifoFrameFragmentationHandlerTest,
    PendingEventsIgnoreCancelledStaleEntries) {
  Handler handler;
  handler.handlerAdded(context_);
  (void)handler.onWrite(
      context_, makeFrame(FrameType::PAYLOAD, 1, kStreamQuantum + 1));
  handler.on<TestSubscribedEvent>(context_, 31);
  (void)handler.onWrite(context_, makeFrame(FrameType::CANCEL, 1));
  handler.on<TestSubscribedEvent>(context_, 32);

  EXPECT_THAT(handler.tracker().subscribedEvents, ElementsAre(31, 32));
  EXPECT_THAT(handler.tracker().pendingStates, ElementsAre(true, false));
  EXPECT_THAT(handler.pendingQueueEntryCount(), Eq(1));
}

TEST_F(
    FifoFrameFragmentationHandlerTest,
    EventBaseSentinelRunsBeforeContinuation) {
  Handler handler;
  handler.handlerAdded(context_);
  std::vector<int> order;
  context_.setWriteObserver(
      [&](const CapturedFrame& /*frame*/) { order.push_back(1); });
  (void)handler.onWrite(
      context_, makeFrame(FrameType::PAYLOAD, 1, kStreamQuantum + 1));
  eventBase_.runInLoop([&] { order.push_back(0); });

  runLoop();

  EXPECT_THAT(order, ElementsAre(1, 0));
  EXPECT_THAT(handler.hasPendingWrites(), IsTrue());

  runLoop();

  EXPECT_THAT(order, ElementsAre(1, 0, 1));
}

class FifoFrameFragmentationHandlerPipelineTest : public Test {
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
            fifo_fragmentation_test_tag, std::make_unique<Handler>())
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
        .addNextOutbound<Handler>(fifo_fragmentation_test_tag)
        .build();
  }

  void verifyHookWiring(auto pipeline) {
    transport_.setWriteResult(cp::Result::Backpressure);
    EXPECT_THAT(
        pipeline->fireWrite(makeFrame(FrameType::REQUEST_RESPONSE, 1, 1)),
        Eq(cp::Result::Backpressure));
    EXPECT_THAT(transport_.writeCount(), Eq(1));
    EXPECT_THAT(pipeline->hasPendingWriteReady(), IsTrue());

    EXPECT_THAT(
        pipeline->fireWrite(
            makeFrame(FrameType::REQUEST_RESPONSE, 3, kStreamQuantum + 1)),
        Eq(cp::Result::Backpressure));
    EXPECT_THAT(transport_.writeCount(), Eq(1));

    transport_.setWriteResult(cp::Result::Success);
    pipeline->onWriteReady();
    EXPECT_THAT(pipeline->hasPendingWriteReady(), IsFalse());
    EXPECT_THAT(transport_.writeCount(), Eq(1));
    eventBase_.loopOnce(EVLOOP_NONBLOCK);
    EXPECT_THAT(transport_.writeCount(), Eq(2));
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
            fifo_fragmentation_test_tag, std::make_unique<Handler>())
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
        .addNextOutbound<Handler>(fifo_fragmentation_test_tag)
        .addNextOutbound<QueueingWriter>(queueing_writer_test_tag)
        .build();
  }

  // A write-ready pass wakes each registered handler once, in the order they
  // registered. Here the writer above runs first while the fragmenter is still
  // blocked, so it gets Backpressure and keeps its last frame. Then the
  // fragmenter clears its block, and the writer must still get to send it.
  void verifyWriterAboveSendsItsQueueAfterTheBlockClears(auto pipeline) {
    transport_.setWriteResult(cp::Result::Backpressure);
    for (const uint32_t streamId : {1, 3, 5, 7}) {
      EXPECT_THAT(
          pipeline->fireWrite(
              makeFrame(FrameType::REQUEST_RESPONSE, streamId, 50)),
          Eq(cp::Result::Backpressure));
    }
    EXPECT_THAT(transport_.writeCount(), Eq(1));

    // The first write of the next pass fills the transport again. The
    // fragmenter then registers again, behind the writer.
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
    FifoFrameFragmentationHandlerPipelineTest,
    DynamicPipelineWiresWriteReadyHook) {
  verifyHookWiring(buildDynamicPipeline());
}

TEST_F(
    FifoFrameFragmentationHandlerPipelineTest,
    StaticPipelineWiresWriteReadyHook) {
  verifyHookWiring(buildStaticPipeline());
}

TEST_F(
    FifoFrameFragmentationHandlerPipelineTest,
    DynamicPipelineLetsTheWriterAboveSendAfterTheBlockClears) {
  verifyWriterAboveSendsItsQueueAfterTheBlockClears(
      buildDynamicPipelineWithWriterAbove());
}

TEST_F(
    FifoFrameFragmentationHandlerPipelineTest,
    StaticPipelineLetsTheWriterAboveSendAfterTheBlockClears) {
  verifyWriterAboveSendsItsQueueAfterTheBlockClears(
      buildStaticPipelineWithWriterAbove());
}

TEST_F(
    FifoFrameFragmentationHandlerPipelineTest,
    TheWriterAboveSendsAfterAFrameQueuedWhileBlocked) {
  auto pipeline = buildDynamicPipelineWithWriterAbove();
  transport_.setWriteResult(cp::Result::Backpressure);
  (void)pipeline->fireWrite(makeFrame(FrameType::REQUEST_RESPONSE, 1, 50));
  for (const uint32_t streamId : {3, 5, 7}) {
    (void)pipeline->fireWrite(makeFrame(
        FrameType::REQUEST_RESPONSE,
        streamId,
        streamId == 5 ? kStreamQuantum + 1 : 50));
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
    FifoFrameFragmentationHandlerPipelineTest,
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
    FifoFrameFragmentationHandlerPipelineTest,
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

} // namespace
} // namespace apache::thrift::fast_thrift::frame::write::handler
