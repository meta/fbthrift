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

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/EventBase.h>
#include <folly/logging/LogCategory.h>
#include <folly/logging/LoggerDB.h>
#include <folly/logging/test/TestLogHandler.h>
#include <folly/portability/GMock.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Event.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Handler.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/handler/InlineFrameFragmentationHandler.h>
#include <thrift/lib/cpp2/fast_thrift/transport/WriteCompletion.h>

namespace apache::thrift::fast_thrift::frame::write::handler {
namespace {

using namespace testing;

static_assert(kMaxStreamQuantum == 16'776'703);

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

using Handler = InlineFrameFragmentationHandlerT<
    FragmentCompletionTrackerT<TestEventFactory>>;

struct CapturedFrame {
  FrameType frameType;
  uint32_t streamId;
  size_t metadataSize;
  size_t dataSize;
  bool follows;
  bool complete;
  bool next;

  bool operator==(const CapturedFrame&) const = default;
};

class TestContext {
 public:
  explicit TestContext(folly::EventBase* eventBase) : eventBase_(eventBase) {}

  folly::EventBase* eventBase() const noexcept { return eventBase_; }

  channel_pipeline::Result fireWrite(
      channel_pipeline::TypeErasedBox&& msg) noexcept {
    const size_t attempt = ++writeAttemptCount_;
    if (errorAttempt_.has_value() && attempt == *errorAttempt_) {
      return channel_pipeline::Result::Error;
    }
    const ComposedFrame& frame = msg.get<ComposedFrame>();
    written_.push_back({
        .frameType = frame.frameType,
        .streamId = frame.streamId,
        .metadataSize =
            frame.metadata ? frame.metadata->computeChainDataLength() : 0,
        .dataSize = frame.data ? frame.data->computeChainDataLength() : 0,
        .follows = frame.follows,
        .complete = frame.complete,
        .next = frame.next,
    });
    if (writeObserver_) {
      writeObserver_();
    }
    return attempt <= results_.size() ? results_[attempt - 1]
                                      : channel_pipeline::Result::Success;
  }

  template <channel_pipeline::PipelineEvent E>
  void fireEvent(const typename E::Payload& event) noexcept {
    static_assert(std::same_as<E, TestFrameWriteCompleteEvent>);
    completed_.push_back(event);
  }

  void awaitWriteReady() noexcept { awaitingWriteReady_ = true; }
  void cancelAwaitWriteReady() noexcept { awaitingWriteReady_ = false; }

  // The first writes return these results in order. Later writes return
  // Success.
  void setResults(std::vector<channel_pipeline::Result> results) {
    results_ = std::move(results);
  }

  void setErrorOnAttempt(size_t attempt) noexcept { errorAttempt_ = attempt; }

  // Runs inside fireWrite() after each accepted write, as a batch that
  // completes inside the write would.
  void setWriteObserver(std::function<void()> observer) {
    writeObserver_ = std::move(observer);
  }

  [[nodiscard]] const std::vector<CapturedFrame>& written() const noexcept {
    return written_;
  }

  [[nodiscard]] size_t writeAttemptCount() const noexcept {
    return writeAttemptCount_;
  }

  [[nodiscard]] bool awaitingWriteReady() const noexcept {
    return awaitingWriteReady_;
  }

  [[nodiscard]] const std::vector<TestFrameWriteCompleteEvent>& completed()
      const noexcept {
    return completed_;
  }

 private:
  folly::EventBase* const eventBase_;
  std::vector<channel_pipeline::Result> results_;
  std::optional<size_t> errorAttempt_;
  std::function<void()> writeObserver_;
  std::vector<CapturedFrame> written_;
  std::vector<TestFrameWriteCompleteEvent> completed_;
  size_t writeAttemptCount_{0};
  bool awaitingWriteReady_{false};
};

static_assert(channel_pipeline::OutboundHandler<Handler, TestContext>);
static_assert(std::same_as<
              Handler::PublishedEvents,
              channel_pipeline::Events<TestFrameWriteCompleteEvent>>);
static_assert(std::same_as<
              Handler::SubscribedEvents,
              channel_pipeline::Events<TestBatchWriteCompleteEvent>>);

std::unique_ptr<folly::IOBuf> makeBuffer(size_t size) {
  std::unique_ptr<folly::IOBuf> buffer = folly::IOBuf::create(size);
  buffer->append(size);
  return buffer;
}

channel_pipeline::TypeErasedBox makeFrame(
    FrameType frameType,
    uint32_t streamId,
    size_t dataSize,
    size_t metadataSize = 0,
    bool complete = false) {
  ComposedFrame frame{
      .frameType = frameType,
      .streamId = streamId,
      .complete = complete,
  };
  if (metadataSize > 0) {
    frame.metadata = makeBuffer(metadataSize);
  }
  if (dataSize > 0) {
    frame.data = makeBuffer(dataSize);
  }
  if (frameType == FrameType::PAYLOAD) {
    frame.next = true;
  }
  return channel_pipeline::TypeErasedBox(std::move(frame));
}

// A data frame with no metadata that splits into two full fragments and one
// of 1,024 bytes.
channel_pipeline::TypeErasedBox makeThreeFragmentFrame(uint32_t streamId) {
  return makeFrame(FrameType::PAYLOAD, streamId, 2 * kMaxStreamQuantum + 1'024);
}

TestFrameWriteCompleteEvent completion(
    uint32_t streamId,
    bool quiesced,
    transport::WriteCompletionStatus status =
        transport::WriteCompletionStatus::Success) {
  return {.streamId = streamId, .status = status, .quiesced = quiesced};
}

class InlineFrameFragmentationHandlerTest : public Test {
 protected:
  void completeBatch(
      size_t frameCount,
      bool quiesced,
      transport::WriteCompletionStatus status =
          transport::WriteCompletionStatus::Success) {
    handler_.on<TestBatchWriteCompleteEvent>(
        context_,
        TestBatchWriteCompleteEvent{
            .status = status,
            .frameCount = frameCount,
            .bytes = 0,
            .quiesced = quiesced,
        });
  }

  folly::EventBase eventBase_;
  TestContext context_{&eventBase_};
  Handler handler_;
};

TEST_F(InlineFrameFragmentationHandlerTest, SendsAFrameThatFitsTheLimitWhole) {
  EXPECT_THAT(
      handler_.onWrite(
          context_,
          makeFrame(
              FrameType::PAYLOAD,
              1,
              kMaxStreamQuantum - 1'024,
              1'024,
              /*complete=*/true)),
      Eq(channel_pipeline::Result::Success));

  EXPECT_THAT(
      context_.written(),
      ElementsAre(
          CapturedFrame{
              .frameType = FrameType::PAYLOAD,
              .streamId = 1,
              .metadataSize = 1'024,
              .dataSize = kMaxStreamQuantum - 1'024,
              .follows = false,
              .complete = true,
              .next = true,
          }));
}

TEST_F(
    InlineFrameFragmentationHandlerTest,
    SendsAllFragmentsOfALargerFrameInOneCall) {
  EXPECT_THAT(
      handler_.onWrite(
          context_,
          makeFrame(
              FrameType::REQUEST_STREAM, 1, 2 * kMaxStreamQuantum, 1'024)),
      Eq(channel_pipeline::Result::Success));

  EXPECT_THAT(eventBase_.getNumLoopCallbacks(), Eq(size_t{0}));
  EXPECT_THAT(
      context_.written(),
      ElementsAre(
          CapturedFrame{
              .frameType = FrameType::REQUEST_STREAM,
              .streamId = 1,
              .metadataSize = 1'024,
              .dataSize = kMaxStreamQuantum - 1'024,
              .follows = true,
              .complete = false,
              .next = false,
          },
          CapturedFrame{
              .frameType = FrameType::PAYLOAD,
              .streamId = 1,
              .metadataSize = 0,
              .dataSize = kMaxStreamQuantum,
              .follows = true,
              .complete = false,
              .next = true,
          },
          CapturedFrame{
              .frameType = FrameType::PAYLOAD,
              .streamId = 1,
              .metadataSize = 0,
              .dataSize = 1'024,
              .follows = false,
              .complete = false,
              .next = true,
          }));
}

TEST_F(InlineFrameFragmentationHandlerTest, SetsCompleteOnlyOnTheLastFragment) {
  EXPECT_THAT(
      handler_.onWrite(
          context_,
          makeFrame(
              FrameType::PAYLOAD,
              1,
              2 * kMaxStreamQuantum + 1'024,
              0,
              /*complete=*/true)),
      Eq(channel_pipeline::Result::Success));

  EXPECT_THAT(
      context_.written(),
      ElementsAre(
          CapturedFrame{
              .frameType = FrameType::PAYLOAD,
              .streamId = 1,
              .metadataSize = 0,
              .dataSize = kMaxStreamQuantum,
              .follows = true,
              .complete = false,
              .next = true,
          },
          CapturedFrame{
              .frameType = FrameType::PAYLOAD,
              .streamId = 1,
              .metadataSize = 0,
              .dataSize = kMaxStreamQuantum,
              .follows = true,
              .complete = false,
              .next = true,
          },
          CapturedFrame{
              .frameType = FrameType::PAYLOAD,
              .streamId = 1,
              .metadataSize = 0,
              .dataSize = 1'024,
              .follows = false,
              .complete = true,
              .next = true,
          }));
}

TEST_F(
    InlineFrameFragmentationHandlerTest,
    PassesFramesThatCannotBeFragmentedUnchanged) {
  EXPECT_THAT(
      std::tuple(
          handler_.onWrite(
              context_,
              makeFrame(FrameType::KEEPALIVE, 0, kMaxStreamQuantum + 1)),
          handler_.onWrite(context_, makeFrame(FrameType::ERROR, 1, 100)),
          handler_.onWrite(context_, makeFrame(FrameType::CANCEL, 3, 0))),
      Eq(std::tuple(
          channel_pipeline::Result::Success,
          channel_pipeline::Result::Success,
          channel_pipeline::Result::Success)));

  EXPECT_THAT(
      context_.written(),
      ElementsAre(
          CapturedFrame{
              .frameType = FrameType::KEEPALIVE,
              .streamId = 0,
              .metadataSize = 0,
              .dataSize = kMaxStreamQuantum + 1,
              .follows = false,
              .complete = false,
              .next = false,
          },
          CapturedFrame{
              .frameType = FrameType::ERROR,
              .streamId = 1,
              .metadataSize = 0,
              .dataSize = 100,
              .follows = false,
              .complete = false,
              .next = false,
          },
          CapturedFrame{
              .frameType = FrameType::CANCEL,
              .streamId = 3,
              .metadataSize = 0,
              .dataSize = 0,
              .follows = false,
              .complete = false,
              .next = false,
          }));
}

TEST_F(InlineFrameFragmentationHandlerTest, RefusesMetadataAboveTheLimit) {
  const channel_pipeline::Result tooLarge = handler_.onWrite(
      context_, makeFrame(FrameType::PAYLOAD, 1, 0, kMaxStreamQuantum + 1));
  const channel_pipeline::Result largest = handler_.onWrite(
      context_,
      makeFrame(FrameType::REQUEST_RESPONSE, 3, 1'024, kMaxStreamQuantum));
  // A record of the refused frame would come first and take the completion of
  // this batch.
  completeBatch(/*frameCount=*/2, /*quiesced=*/true);

  EXPECT_THAT(
      std::tuple(tooLarge, largest),
      Eq(std::tuple(
          channel_pipeline::Result::Error, channel_pipeline::Result::Success)));
  EXPECT_THAT(
      context_.written(),
      ElementsAre(
          CapturedFrame{
              .frameType = FrameType::REQUEST_RESPONSE,
              .streamId = 3,
              .metadataSize = kMaxStreamQuantum,
              .dataSize = 0,
              .follows = true,
              .complete = false,
              .next = false,
          },
          CapturedFrame{
              .frameType = FrameType::PAYLOAD,
              .streamId = 3,
              .metadataSize = 0,
              .dataSize = 1'024,
              .follows = false,
              .complete = false,
              .next = true,
          }));
  EXPECT_THAT(context_.completed(), ElementsAre(completion(3, true)));
}

TEST_F(
    InlineFrameFragmentationHandlerTest,
    RefusalLogsTheMetadataSizeAndTheLimit) {
  // The refusal logs at most once a minute in a process. A threadsafe death
  // test runs the refusal in a new process, so refusals of earlier tests in
  // this binary cannot hide the log line.
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  EXPECT_EXIT(
      {
        const auto logs = std::make_shared<folly::TestLogHandler>();
        folly::LoggerDB::get().getCategory("")->addHandler(logs);
        const channel_pipeline::Result result = handler_.onWrite(
            context_,
            makeFrame(FrameType::PAYLOAD, 1, 0, kMaxStreamQuantum + 1));
        for (const std::string& message : logs->getMessageValues()) {
          std::fputs(message.c_str(), stderr);
        }
        std::_Exit(result == channel_pipeline::Result::Error ? 0 : 1);
      },
      ExitedWithCode(0),
      HasSubstr(
          "InlineFrameFragmentationHandler refused a data frame with 16776704 "
          "metadata bytes. The limit is 16776703 bytes."));
}

TEST_F(
    InlineFrameFragmentationHandlerTest, ReportsOneCompletionForASplitFrame) {
  EXPECT_THAT(
      std::tuple(
          handler_.onWrite(context_, makeThreeFragmentFrame(1)),
          handler_.onWrite(context_, makeFrame(FrameType::PAYLOAD, 3, 100))),
      Eq(std::tuple(
          channel_pipeline::Result::Success,
          channel_pipeline::Result::Success)));

  completeBatch(
      /*frameCount=*/4,
      /*quiesced=*/true,
      transport::WriteCompletionStatus::Error);

  EXPECT_THAT(
      context_.completed(),
      ElementsAre(
          completion(1, false, transport::WriteCompletionStatus::Error),
          completion(3, true, transport::WriteCompletionStatus::Error)));
}

TEST_F(
    InlineFrameFragmentationHandlerTest,
    ReportsOneCompletionForCancelAndError) {
  EXPECT_THAT(
      std::tuple(
          handler_.onWrite(context_, makeFrame(FrameType::CANCEL, 1, 0)),
          handler_.onWrite(context_, makeFrame(FrameType::ERROR, 3, 100))),
      Eq(std::tuple(
          channel_pipeline::Result::Success,
          channel_pipeline::Result::Success)));

  completeBatch(/*frameCount=*/2, /*quiesced=*/true);

  EXPECT_THAT(
      context_.completed(),
      ElementsAre(completion(1, false), completion(3, true)));
}

TEST_F(InlineFrameFragmentationHandlerTest, DropsAFrameWhoseWriteFails) {
  context_.setErrorOnAttempt(1);

  EXPECT_THAT(
      std::tuple(
          handler_.onWrite(context_, makeFrame(FrameType::PAYLOAD, 1, 100)),
          handler_.onWrite(context_, makeFrame(FrameType::PAYLOAD, 3, 100))),
      Eq(std::tuple(
          channel_pipeline::Result::Error, channel_pipeline::Result::Success)));
  completeBatch(/*frameCount=*/1, /*quiesced=*/true);

  EXPECT_THAT(context_.completed(), ElementsAre(completion(3, true)));
}

TEST_F(InlineFrameFragmentationHandlerTest, StopsASplitFrameWhoseWriteFails) {
  context_.setErrorOnAttempt(2);

  const channel_pipeline::Result split =
      handler_.onWrite(context_, makeThreeFragmentFrame(1));
  const size_t splitAttempts = context_.writeAttemptCount();
  const channel_pipeline::Result next =
      handler_.onWrite(context_, makeFrame(FrameType::PAYLOAD, 3, 100));
  // The batch holds the first fragment of stream 1 and the frame of stream 3.
  completeBatch(/*frameCount=*/2, /*quiesced=*/true);

  EXPECT_THAT(
      std::tuple(split, splitAttempts, next),
      Eq(std::tuple(
          channel_pipeline::Result::Error,
          size_t{2},
          channel_pipeline::Result::Success)));
  EXPECT_THAT(context_.completed(), ElementsAre(completion(3, true)));
}

TEST_F(
    InlineFrameFragmentationHandlerTest, InlineBatchCompletionFindsTheRecord) {
  context_.setWriteObserver(
      [this] { completeBatch(/*frameCount=*/1, /*quiesced=*/false); });

  EXPECT_THAT(
      std::tuple(
          handler_.onWrite(context_, makeFrame(FrameType::PAYLOAD, 2, 100)),
          handler_.onWrite(context_, makeThreeFragmentFrame(1))),
      Eq(std::tuple(
          channel_pipeline::Result::Success,
          channel_pipeline::Result::Success)));

  EXPECT_THAT(
      context_.completed(),
      ElementsAre(completion(2, false), completion(1, false)));
}

TEST_F(InlineFrameFragmentationHandlerTest, KeepsSendingAfterBackpressure) {
  context_.setResults({
      channel_pipeline::Result::Backpressure,
      channel_pipeline::Result::Backpressure,
      channel_pipeline::Result::Backpressure,
      channel_pipeline::Result::Backpressure,
      channel_pipeline::Result::Success,
      channel_pipeline::Result::Success,
  });

  const channel_pipeline::Result allBackpressure =
      handler_.onWrite(context_, makeThreeFragmentFrame(1));
  const channel_pipeline::Result lastSuccess =
      handler_.onWrite(context_, makeThreeFragmentFrame(3));

  EXPECT_THAT(
      std::tuple(
          allBackpressure,
          lastSuccess,
          context_.written().size(),
          context_.awaitingWriteReady()),
      Eq(std::tuple(
          channel_pipeline::Result::Backpressure,
          channel_pipeline::Result::Success,
          size_t{6},
          false)));
}

TEST_F(InlineFrameFragmentationHandlerTest, ReportsPendingWritesDuringASplit) {
  const channel_pipeline::Result whole =
      handler_.onWrite(context_, makeFrame(FrameType::PAYLOAD, 3, 100));
  // During the first fragment of stream 1, the batch of stream 3 completes.
  context_.setWriteObserver([this, writes = 0]() mutable {
    if (++writes == 1) {
      completeBatch(/*frameCount=*/1, /*quiesced=*/true);
    }
  });
  const channel_pipeline::Result splitWithEarlierBatch =
      handler_.onWrite(context_, makeThreeFragmentFrame(1));
  context_.setWriteObserver(nullptr);
  completeBatch(/*frameCount=*/3, /*quiesced=*/true);
  // Each fragment of stream 5 completes its own batch inside its write.
  context_.setWriteObserver(
      [this] { completeBatch(/*frameCount=*/1, /*quiesced=*/true); });
  const channel_pipeline::Result splitWithOwnBatches =
      handler_.onWrite(context_, makeThreeFragmentFrame(5));

  EXPECT_THAT(
      std::tuple(whole, splitWithEarlierBatch, splitWithOwnBatches),
      Eq(std::tuple(
          channel_pipeline::Result::Success,
          channel_pipeline::Result::Success,
          channel_pipeline::Result::Success)));
  EXPECT_THAT(
      context_.completed(),
      ElementsAre(
          completion(3, false), completion(1, true), completion(5, true)));
}

} // namespace
} // namespace apache::thrift::fast_thrift::frame::write::handler
