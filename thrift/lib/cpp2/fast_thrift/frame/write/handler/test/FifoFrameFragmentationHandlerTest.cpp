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

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Handler.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/handler/FifoFrameFragmentationHandler.h>

namespace apache::thrift::fast_thrift::frame::write::handler {
namespace {

using namespace testing;

constexpr size_t kStreamQuantum = 64 * 1024;

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

  [[maybe_unused]] void onFragmentAborted() noexcept {}

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
  std::vector<int> subscribedEvents;
  std::vector<bool> pendingStates;
};

static_assert(FragmentCompletionTracker<RecordingTracker>);

using Handler = FifoFrameFragmentationHandlerT<RecordingTracker>;

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
    ++writeAttemptCount_;
    if (errorAttempt_.has_value() && writeAttemptCount_ == *errorAttempt_) {
      return channel_pipeline::Result::Error;
    }

    const ComposedFrame& frame = msg.get<ComposedFrame>();
    const CapturedFrame captured{
        .frameType = frame.frameType,
        .streamId = frame.streamId,
        .dataSize = frame.data ? frame.data->computeChainDataLength() : 0,
        .follows = frame.follows,
        .complete = frame.complete,
        .next = frame.next,
    };
    written_.push_back(captured);
    if (writeObserver_) {
      writeObserver_(captured);
    }
    if (const std::function<void()> reentrantWrite =
            std::exchange(reentrantWrite_, nullptr);
        reentrantWrite) {
      reentrantWrite();
    }
    return result_;
  }

  void fireException(folly::exception_wrapper&& exception) noexcept {
    ++fireExceptionCount_;
    lastException_ = std::move(exception);
  }

  void setResult(channel_pipeline::Result result) noexcept { result_ = result; }

  void setErrorOnAttempt(size_t attempt) noexcept { errorAttempt_ = attempt; }

  void setReentrantWrite(std::function<void()> write) {
    reentrantWrite_ = std::move(write);
  }

  void setWriteObserver(std::function<void(const CapturedFrame&)> observer) {
    writeObserver_ = std::move(observer);
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

  [[nodiscard]] const folly::exception_wrapper& lastException() const noexcept {
    return lastException_;
  }

 private:
  folly::EventBase* const eventBase_;
  channel_pipeline::Result result_{channel_pipeline::Result::Success};
  std::optional<size_t> errorAttempt_;
  std::function<void()> reentrantWrite_;
  std::function<void(const CapturedFrame&)> writeObserver_;
  std::vector<CapturedFrame> written_;
  size_t writeAttemptCount_{0};
  size_t fireExceptionCount_{0};
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

} // namespace
} // namespace apache::thrift::fast_thrift::frame::write::handler
