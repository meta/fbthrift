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

#include <thrift/lib/cpp2/fast_thrift/frame/write/handler/FrameFragmentationHandler.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/HandlerTag.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineBuilder.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/StaticPipelineBuilder.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/test/MockAdapters.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/ComposedFrame.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/QuantumFragmentationHandlerConfig.h>
#include <thrift/lib/cpp2/fast_thrift/transport/WriteCompletion.h>

#include <gtest/gtest.h>
#include <folly/ExceptionWrapper.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/EventBase.h>
#include <folly/logging/LogCategory.h>
#include <folly/logging/LoggerDB.h>
#include <folly/logging/test/TestLogHandler.h>
#include <folly/portability/GMock.h>

#include <array>
#include <concepts>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

namespace apache::thrift::fast_thrift::frame::write::handler {
namespace {

using namespace testing;

// ============================================================================
// Standalone Mock Context
// ============================================================================

struct CapturedFrame {
  uint32_t streamId;
  FrameType frameType;
  size_t dataSize;
  bool follows;
  size_t metadataSize;

  bool operator==(const CapturedFrame&) const = default;
};

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

class MockContext {
 public:
  explicit MockContext(folly::EventBase* evb) : evb_(evb) {}

  folly::EventBase* eventBase() const { return evb_; }

  apache::thrift::fast_thrift::channel_pipeline::Result fireWrite(
      apache::thrift::fast_thrift::channel_pipeline::TypeErasedBox&&
          msg) noexcept {
    if (errorEnabled_ && written_.size() >= errorAt_) {
      return apache::thrift::fast_thrift::channel_pipeline::Result::Error;
    }
    const bool backpressured =
        backpressureAt_ > 0 && written_.size() >= backpressureAt_;

    auto& f = msg.get<ComposedFrame>();
    CapturedFrame cap{};
    cap.streamId = f.streamId;
    cap.frameType = f.frameType;
    cap.dataSize = f.data ? f.data->computeChainDataLength() : 0;
    cap.follows = f.follows;
    cap.metadataSize = f.metadata ? f.metadata->computeChainDataLength() : 0;
    written_.push_back(cap);
    if (f.data) {
      dataByStream_[f.streamId] += f.data->to<std::string>();
    }
    if (writeObserver_) {
      writeObserver_();
    }
    return backpressured ? channel_pipeline::Result::Backpressure
                         : channel_pipeline::Result::Success;
  }

  void fireException(folly::exception_wrapper&& e) noexcept {
    ++fireExceptionCount_;
    lastException_ = std::move(e);
  }

  template <channel_pipeline::PipelineEvent E>
  void fireEvent(const typename E::Payload& event) noexcept {
    static_assert(std::same_as<E, TestFrameWriteCompleteEvent>);
    completed_.push_back(event);
  }

  void awaitWriteReady() { awaitWriteReadyCalled_ = true; }
  void cancelAwaitWriteReady() { awaitWriteReadyCalled_ = false; }
  void deactivate() {}

  // Takes the handler's request for another write-ready round. These tests run
  // no rounds.
  struct NoPipeline {
    void onWriteReady() noexcept {}
  };
  NoPipeline* pipeline() noexcept { return &pipeline_; }

  void setBackpressureAt(size_t n) { backpressureAt_ = n; }
  void setErrorAt(size_t n) {
    errorEnabled_ = true;
    errorAt_ = n;
  }
  // Runs inside fireWrite() after downstream accepted the frame, as a batch
  // that completes inline does.
  void setWriteObserver(std::function<void()> observer) {
    writeObserver_ = std::move(observer);
  }
  const std::vector<CapturedFrame>& written() const { return written_; }
  const std::vector<TestFrameWriteCompleteEvent>& completed() const {
    return completed_;
  }
  // The data bytes of each stream in write order, across its fragments.
  const std::map<uint32_t, std::string>& dataByStream() const {
    return dataByStream_;
  }
  bool awaitWriteReadyCalled() const { return awaitWriteReadyCalled_; }
  size_t fireExceptionCount() const { return fireExceptionCount_; }
  const folly::exception_wrapper& lastException() const {
    return lastException_;
  }

 private:
  folly::EventBase* evb_;
  std::vector<CapturedFrame> written_;
  std::map<uint32_t, std::string> dataByStream_;
  std::function<void()> writeObserver_;
  std::vector<TestFrameWriteCompleteEvent> completed_;
  size_t backpressureAt_{0};
  size_t errorAt_{0};
  bool errorEnabled_{false};
  bool awaitWriteReadyCalled_{false};
  NoPipeline pipeline_;
  size_t fireExceptionCount_{0};
  folly::exception_wrapper lastException_;
};

// ============================================================================
// Recording tracker — captures the handler's onFragment / onFlush hook calls so
// handler-level tests can assert the doFlush wiring (per-fragment isLast flags,
// onFlush only on the success path, no record left for a fragment that
// errored).
// ============================================================================

struct RecordingTracker {
  using PublishedEvents = channel_pipeline::Events<>;
  using SubscribedEvents = channel_pipeline::Events<>;

  struct FragmentCall {
    uint32_t streamId;
    bool isLastFragment;
  };
  std::vector<FragmentCall> fragments;
  size_t flushCount{0};

  void onFragment(uint32_t streamId, bool isLastFragment) noexcept {
    fragments.push_back({streamId, isLastFragment});
  }

  void onFragmentAborted() noexcept { fragments.pop_back(); }
  void onFlush() noexcept { ++flushCount; }
};

using CompletionHandler =
    FrameFragmentationHandlerT<FragmentCompletionTrackerT<TestEventFactory>>;

constexpr size_t kCloseResponseSize = 8 * 1024 * 1024;
constexpr std::array<size_t, 2> kCloseFragmentSizes{512 * 1024, 1024 * 1024};

// Each 4-byte word holds its own index, so a lost, repeated or reordered chunk
// changes the bytes that downstream receives.
std::unique_ptr<folly::IOBuf> makePayload(size_t size) {
  auto buf = folly::IOBuf::create(size);
  buf->append(size);
  uint8_t* bytes = buf->writableData();
  for (size_t i = 0; i < size; ++i) {
    bytes[i] = static_cast<uint8_t>((i / 4) >> (i % 4 * 8));
  }
  return buf;
}

apache::thrift::fast_thrift::channel_pipeline::TypeErasedBox makePayloadFrame(
    uint32_t streamId, size_t dataSize, size_t metadataSize = 0) {
  ComposedFrame f;
  f.frameType = FrameType::PAYLOAD;
  f.streamId = streamId;
  if (metadataSize > 0) {
    f.metadata = makePayload(metadataSize);
  }
  f.data = makePayload(dataSize);
  f.next = true;
  return apache::thrift::fast_thrift::channel_pipeline::TypeErasedBox(
      std::move(f));
}

apache::thrift::fast_thrift::channel_pipeline::TypeErasedBox makeCancelFrame(
    uint32_t streamId) {
  ComposedFrame f;
  f.frameType = FrameType::CANCEL;
  f.streamId = streamId;
  return apache::thrift::fast_thrift::channel_pipeline::TypeErasedBox(
      std::move(f));
}

apache::thrift::fast_thrift::channel_pipeline::TypeErasedBox
makeKeepAliveFrame() {
  ComposedFrame f;
  f.frameType = FrameType::KEEPALIVE;
  f.streamId = 0;
  return apache::thrift::fast_thrift::channel_pipeline::TypeErasedBox(
      std::move(f));
}

apache::thrift::fast_thrift::channel_pipeline::TypeErasedBox makeErrorFrame(
    uint32_t streamId) {
  ComposedFrame f;
  f.frameType = FrameType::ERROR;
  f.streamId = streamId;
  f.errorCode = 0x00000201;
  return apache::thrift::fast_thrift::channel_pipeline::TypeErasedBox(
      std::move(f));
}

// ============================================================================
// Test Fixture
// ============================================================================

class FrameFragmentationHandlerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    evb_ = std::make_unique<folly::EventBase>();
    ctx_ = std::make_unique<MockContext>(evb_.get());
  }

  void TearDown() override {
    ctx_.reset();
    evb_.reset();
  }

  void runEventBaseLoop() { evb_->loopOnce(); }

  std::unique_ptr<folly::EventBase> evb_;
  std::unique_ptr<MockContext> ctx_;
};

// ============================================================================
// Single Stream Tests
// ============================================================================

TEST_F(FrameFragmentationHandlerTest, SmallFrameBypassesFragmentation) {
  FragmentationHandlerConfig config{.maxFragmentSize = 64 * 1024};
  FrameFragmentationHandler handler(config);
  handler.handlerAdded(*ctx_);

  auto result = handler.onWrite(*ctx_, makePayloadFrame(1, 32 * 1024));
  EXPECT_EQ(
      result, apache::thrift::fast_thrift::channel_pipeline::Result::Success);

  EXPECT_EQ(handler.immediateQueueSize(), 0);
  EXPECT_EQ(handler.pendingStreamCount(), 0);

  ASSERT_EQ(ctx_->written().size(), 1);
  EXPECT_EQ(ctx_->written()[0].streamId, 1);
  EXPECT_EQ(ctx_->written()[0].dataSize, 32 * 1024);
  EXPECT_FALSE(ctx_->written()[0].follows);
}

TEST_F(FrameFragmentationHandlerTest, LargeFrameFragmented) {
  FragmentationHandlerConfig config{.maxFragmentSize = 64 * 1024};
  FrameFragmentationHandler handler(config);
  handler.handlerAdded(*ctx_);

  auto result = handler.onWrite(*ctx_, makePayloadFrame(1, 192 * 1024));
  EXPECT_EQ(
      result, apache::thrift::fast_thrift::channel_pipeline::Result::Success);

  EXPECT_EQ(handler.immediateQueueSize(), 0);
  EXPECT_EQ(handler.pendingStreamCount(), 1);

  runEventBaseLoop();

  ASSERT_EQ(ctx_->written().size(), 3);
  EXPECT_TRUE(ctx_->written()[0].follows);
  EXPECT_TRUE(ctx_->written()[1].follows);
  EXPECT_FALSE(ctx_->written()[2].follows);
  // First fragment keeps original PAYLOAD type; continuations are PAYLOAD too.
  EXPECT_EQ(ctx_->written()[0].frameType, FrameType::PAYLOAD);
  EXPECT_EQ(ctx_->written()[1].frameType, FrameType::PAYLOAD);
  EXPECT_EQ(ctx_->written()[2].frameType, FrameType::PAYLOAD);
}

// ============================================================================
// Non-fragmentable bypass
// ============================================================================

TEST_F(FrameFragmentationHandlerTest, ErrorFrameBypasses) {
  FragmentationHandlerConfig config{.maxFragmentSize = 64 * 1024};
  FrameFragmentationHandler handler(config);
  handler.handlerAdded(*ctx_);

  (void)handler.onWrite(*ctx_, makeErrorFrame(1));
  ASSERT_EQ(ctx_->written().size(), 1);
  EXPECT_EQ(ctx_->written()[0].frameType, FrameType::ERROR);
  EXPECT_EQ(handler.pendingStreamCount(), 0);
  EXPECT_EQ(handler.immediateQueueSize(), 0);
}

// ============================================================================
// Multi-Stream Round-Robin Tests
// ============================================================================

TEST_F(FrameFragmentationHandlerTest, MultiStreamRoundRobinInterleaving) {
  FragmentationHandlerConfig config{.maxFragmentSize = 64 * 1024};
  FrameFragmentationHandler handler(config);
  handler.handlerAdded(*ctx_);

  (void)handler.onWrite(*ctx_, makePayloadFrame(1, 192 * 1024)); // 3 fragments
  (void)handler.onWrite(*ctx_, makePayloadFrame(2, 128 * 1024)); // 2 fragments
  (void)handler.onWrite(*ctx_, makePayloadFrame(3, 65 * 1024)); // 2 fragments

  EXPECT_EQ(handler.pendingStreamCount(), 3);

  runEventBaseLoop();

  ASSERT_EQ(ctx_->written().size(), 7);

  std::map<uint32_t, std::vector<size_t>> fragmentsByStream;
  for (size_t i = 0; i < ctx_->written().size(); ++i) {
    fragmentsByStream[ctx_->written()[i].streamId].push_back(i);
  }

  EXPECT_EQ(fragmentsByStream[1].size(), 3);
  EXPECT_EQ(fragmentsByStream[2].size(), 2);
  EXPECT_EQ(fragmentsByStream[3].size(), 2);

  for (const auto& [streamId, indices] : fragmentsByStream) {
    for (size_t i = 1; i < indices.size(); ++i) {
      EXPECT_GT(indices[i], indices[i - 1])
          << "Stream " << streamId << " fragments out of order";
    }
  }
}

TEST_F(FrameFragmentationHandlerTest, SmallFramesSentBeforeLargeFragments) {
  FragmentationHandlerConfig config{.maxFragmentSize = 64 * 1024};
  FrameFragmentationHandler handler(config);
  handler.handlerAdded(*ctx_);

  (void)handler.onWrite(*ctx_, makePayloadFrame(1, 192 * 1024));
  (void)handler.onWrite(*ctx_, makePayloadFrame(2, 32 * 1024));
  (void)handler.onWrite(*ctx_, makePayloadFrame(3, 128 * 1024));
  (void)handler.onWrite(*ctx_, makePayloadFrame(4, 16 * 1024));

  EXPECT_EQ(handler.immediateQueueSize(), 2);
  EXPECT_EQ(handler.pendingStreamCount(), 2);

  runEventBaseLoop();

  ASSERT_EQ(ctx_->written().size(), 7);

  EXPECT_EQ(ctx_->written()[0].streamId, 2);
  EXPECT_EQ(ctx_->written()[1].streamId, 4);
}

// ============================================================================
// Backpressure Tests
// ============================================================================

TEST_F(
    FrameFragmentationHandlerTest, ForcedFlushReportsPressureAfterFullDrain) {
  FragmentationHandlerConfig config{
      .maxFragmentSize = 64 * 1024,
      .maxPendingBytes = 64 * 1024,
  };
  FrameFragmentationHandler handler(config);
  handler.handlerAdded(*ctx_);
  ctx_->setBackpressureAt(1);

  EXPECT_EQ(
      handler.onWrite(*ctx_, makePayloadFrame(1, 192 * 1024)),
      channel_pipeline::Result::Backpressure);

  EXPECT_EQ(ctx_->written().size(), 3);
  EXPECT_EQ(handler.pendingStreamCount(), 0);
  EXPECT_TRUE(ctx_->awaitWriteReadyCalled());
}

TEST_F(FrameFragmentationHandlerTest, BackpressureDoesNotPauseFlush) {
  FragmentationHandlerConfig config{.maxFragmentSize = 64 * 1024};
  FrameFragmentationHandler handler(config);
  handler.handlerAdded(*ctx_);

  (void)handler.onWrite(*ctx_, makePayloadFrame(1, 128 * 1024));
  (void)handler.onWrite(*ctx_, makePayloadFrame(2, 128 * 1024));

  ctx_->setBackpressureAt(2);

  runEventBaseLoop();

  EXPECT_EQ(ctx_->written().size(), 4);
  EXPECT_TRUE(ctx_->awaitWriteReadyCalled());
}

TEST_F(
    FrameFragmentationHandlerTest,
    WriteReadyDoesNotRetransmitDrainedFragments) {
  FragmentationHandlerConfig config{.maxFragmentSize = 64 * 1024};
  FrameFragmentationHandler handler(config);
  handler.handlerAdded(*ctx_);

  (void)handler.onWrite(*ctx_, makePayloadFrame(1, 192 * 1024)); // 3 fragments

  ctx_->setBackpressureAt(2);

  runEventBaseLoop();
  EXPECT_EQ(ctx_->written().size(), 3);
  EXPECT_TRUE(ctx_->awaitWriteReadyCalled());

  ctx_->setBackpressureAt(0);
  handler.onWriteReady(*ctx_);

  EXPECT_EQ(handler.pendingStreamCount(), 0);
  EXPECT_EQ(ctx_->written().size(), 3);
}

TEST_F(
    FrameFragmentationHandlerTest, DownstreamErrorStopsWaitingForWriteReady) {
  const FragmentationHandlerConfig config{.maxFragmentSize = 64 * 1024};
  FrameFragmentationHandler handler(config);
  handler.handlerAdded(*ctx_);
  (void)handler.onWrite(*ctx_, makePayloadFrame(1, 192 * 1024)); // 3 fragments
  ctx_->setBackpressureAt(1);
  ctx_->setErrorAt(2);

  runEventBaseLoop();

  EXPECT_THAT(
      std::tuple(ctx_->fireExceptionCount(), ctx_->awaitWriteReadyCalled()),
      Eq(std::tuple(size_t{1}, false)));
}

TEST_F(
    FrameFragmentationHandlerTest, PipelineInactiveStopsWaitingForWriteReady) {
  FrameFragmentationHandler handler;
  handler.handlerAdded(*ctx_);
  ctx_->setBackpressureAt(1);
  (void)handler.onWrite(*ctx_, makePayloadFrame(1, 100));
  EXPECT_THAT(
      handler.onWrite(*ctx_, makePayloadFrame(3, 100)),
      Eq(channel_pipeline::Result::Backpressure));

  handler.onPipelineInactive(*ctx_);

  EXPECT_THAT(ctx_->awaitWriteReadyCalled(), IsFalse());
}

// ============================================================================
// Write-ready in a pipeline
// ============================================================================

namespace cp = channel_pipeline;

HANDLER_TAG(frame_fragmentation_test);
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

class FrameFragmentationHandlerPipelineTest : public Test {
 protected:
  static constexpr FragmentationHandlerConfig kConfig{.maxFragmentSize = 1024};

  cp::PipelineImpl::Ptr buildDynamicPipeline() {
    return cp::PipelineBuilder<
               PipelineHeadHandler,
               PipelineTailHandler,
               cp::test::TestAllocator>()
        .setEventBase(&eventBase_)
        .setHead(&transport_)
        .setTail(&app_)
        .setAllocator(&allocator_)
        .addNextOutbound<FrameFragmentationHandler>(
            frame_fragmentation_test_tag,
            std::make_unique<FrameFragmentationHandler>(kConfig))
        .build();
  }

  cp::PipelineImpl::Ptr buildDynamicPipelineWithWriterAbove() {
    return cp::PipelineBuilder<
               PipelineHeadHandler,
               PipelineTailHandler,
               cp::test::TestAllocator>()
        .setEventBase(&eventBase_)
        .setHead(&transport_)
        .setTail(&app_)
        .setAllocator(&allocator_)
        .addNextOutbound<FrameFragmentationHandler>(
            frame_fragmentation_test_tag,
            std::make_unique<FrameFragmentationHandler>(kConfig))
        .addNextOutbound<QueueingWriter>(
            queueing_writer_test_tag, std::make_unique<QueueingWriter>())
        .build();
  }

  auto buildStaticPipelineWithWriterAbove() {
    return cp::StaticPipelineBuilder<
               PipelineHeadHandler,
               PipelineTailHandler,
               cp::test::TestAllocator>()
        .setEventBase(&eventBase_)
        .setHead(&transport_)
        .setTail(&app_)
        .setAllocator(&allocator_)
        .addNextOutbound<FrameFragmentationHandler>(
            frame_fragmentation_test_tag, kConfig)
        .addNextOutbound<QueueingWriter>(queueing_writer_test_tag)
        .build();
  }

  // The writer above keeps three frames while the fragmenter is blocked. Each
  // write-ready pass wakes the fragmenter before the writer, so the writer
  // sends after the fragmenter has cleared its block.
  void verifyWriterAboveSendsItsQueueAfterTheBlockClears(auto pipeline) {
    transport_.setWriteResult(cp::Result::Backpressure);
    for (const uint32_t streamId : {1, 3, 5, 7}) {
      EXPECT_THAT(
          pipeline->fireWrite(makePayloadFrame(streamId, 50)),
          Eq(cp::Result::Backpressure));
    }
    EXPECT_THAT(transport_.writeCount(), Eq(1));

    // The first write of the next pass fills the transport again, so the
    // fragmenter blocks again and the writer keeps the rest for one more pass.
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
  cp::test::TestAllocator allocator_;
};

TEST_F(
    FrameFragmentationHandlerPipelineTest,
    DynamicPipelineLetsTheWriterAboveSendAfterTheBlockClears) {
  verifyWriterAboveSendsItsQueueAfterTheBlockClears(
      buildDynamicPipelineWithWriterAbove());
}

TEST_F(
    FrameFragmentationHandlerPipelineTest,
    StaticPipelineLetsTheWriterAboveSendAfterTheBlockClears) {
  verifyWriterAboveSendsItsQueueAfterTheBlockClears(
      buildStaticPipelineWithWriterAbove());
}

TEST_F(
    FrameFragmentationHandlerPipelineTest,
    TheWriterAboveSendsAFrameThatNeedsFragmentsAfterTheBlockClears) {
  auto pipeline = buildDynamicPipelineWithWriterAbove();
  transport_.setWriteResult(cp::Result::Backpressure);
  (void)pipeline->fireWrite(makePayloadFrame(1, 50));
  for (const uint32_t streamId : {3, 5, 7}) {
    (void)pipeline->fireWrite(
        makePayloadFrame(streamId, streamId == 5 ? 2000 : 50));
  }

  // Stream 3 fills the transport again. In the next pass the writer above
  // sends stream 5, which the fragmenter queues because it needs two
  // fragments. The fragmenter sends them from its flush in the event loop.
  transport_.setWriteResult(cp::Result::Success);
  transport_.setNextWriteResults({cp::Result::Backpressure});
  pipeline->onWriteReady();
  pipeline->onWriteReady();
  eventBase_.loopOnce(EVLOOP_NONBLOCK);

  EXPECT_THAT(
      std::tuple(transport_.writeCount(), pipeline->hasPendingWriteReady()),
      Eq(std::tuple(size_t{5}, false)));
}

TEST_F(
    FrameFragmentationHandlerPipelineTest,
    AFlushThatFillsTheTransportAgainEndsThePass) {
  auto pipeline = buildDynamicPipeline();
  transport_.setWriteResult(cp::Result::Backpressure);
  (void)pipeline->fireWrite(makePayloadFrame(1, 3000));
  eventBase_.loopOnce(EVLOOP_NONBLOCK);
  EXPECT_THAT(
      pipeline->fireWrite(makePayloadFrame(3, 3000)),
      Eq(cp::Result::Backpressure));

  // The flush inside onWriteReady() sends stream 3, and the transport stays
  // full, so the handler blocks again.
  pipeline->onWriteReady();

  EXPECT_THAT(
      std::tuple(
          app_.writeReadyCount(),
          transport_.writeCount(),
          pipeline->hasPendingWriteReady()),
      Eq(std::tuple(size_t{1}, size_t{6}, true)));
}

TEST_F(
    FrameFragmentationHandlerPipelineTest,
    AFullTransportGetsOneWriteReadyRound) {
  auto pipeline = buildDynamicPipeline();
  transport_.setWriteResult(cp::Result::Backpressure);
  EXPECT_THAT(
      pipeline->fireWrite(makePayloadFrame(1, 50)),
      Eq(cp::Result::Backpressure));
  // The application writes one more reply each time it hears write-ready, and
  // the transport stays full. The cap only keeps a failing run finite.
  uint32_t nextStreamId = 3;
  app_.setOnWriteReady([&] {
    if (nextStreamId <= 21) {
      (void)pipeline->fireWrite(makePayloadFrame(nextStreamId, 50));
      nextStreamId += 2;
    }
  });

  pipeline->onWriteReady();

  EXPECT_THAT(
      std::tuple(app_.writeReadyCount(), transport_.writeCount()),
      Eq(std::tuple(size_t{1}, size_t{2})));
}

TEST_F(
    FrameFragmentationHandlerPipelineTest,
    TwoWritesPerWakeGetOneRoundPerWriteReady) {
  auto pipeline = buildDynamicPipeline();
  transport_.setWriteResult(cp::Result::Backpressure);
  EXPECT_THAT(
      pipeline->fireWrite(makePayloadFrame(1, 50)),
      Eq(cp::Result::Backpressure));
  // The application writes two more replies each time it hears write-ready,
  // and the transport stays full. So the second reply reaches the handler
  // after the first one blocked it again. The cap only keeps a failing run
  // finite.
  uint32_t nextStreamId = 3;
  const auto writeReply = [&] {
    if (nextStreamId <= 41) {
      (void)pipeline->fireWrite(makePayloadFrame(nextStreamId, 50));
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

// ============================================================================
// Threshold Tests
// ============================================================================

TEST_F(FrameFragmentationHandlerTest, MaxPendingBytesForceFlush) {
  FragmentationHandlerConfig config{
      .maxFragmentSize = 64 * 1024,
      .maxPendingBytes = 256 * 1024,
  };
  FrameFragmentationHandler handler(config);
  handler.handlerAdded(*ctx_);

  (void)handler.onWrite(*ctx_, makePayloadFrame(1, 128 * 1024));
  EXPECT_EQ(ctx_->written().size(), 0);

  (void)handler.onWrite(*ctx_, makePayloadFrame(2, 192 * 1024));
  EXPECT_GT(ctx_->written().size(), 0);
}

TEST_F(FrameFragmentationHandlerTest, MaxPendingFramesForceFlush) {
  FragmentationHandlerConfig config{
      .maxFragmentSize = 64 * 1024,
      .maxPendingBytes = 10 * 1024 * 1024,
      .maxPendingFrames = 3,
  };
  FrameFragmentationHandler handler(config);
  handler.handlerAdded(*ctx_);

  (void)handler.onWrite(*ctx_, makePayloadFrame(1, 192 * 1024));
  EXPECT_EQ(ctx_->written().size(), 0);
  EXPECT_EQ(handler.pendingStreamCount(), 1);

  (void)handler.onWrite(*ctx_, makePayloadFrame(2, 192 * 1024));
  EXPECT_EQ(ctx_->written().size(), 0);
  EXPECT_EQ(handler.pendingStreamCount(), 2);

  (void)handler.onWrite(*ctx_, makePayloadFrame(3, 192 * 1024));
  EXPECT_EQ(ctx_->written().size(), 0);

  (void)handler.onWrite(*ctx_, makePayloadFrame(4, 192 * 1024));
  EXPECT_GT(ctx_->written().size(), 0);
}

TEST_F(FrameFragmentationHandlerTest, BackpressureAtExactByteLimit) {
  FragmentationHandlerConfig config{
      .maxFragmentSize = 64 * 1024,
      .maxPendingBytes = 128 * 1024,
  };
  FrameFragmentationHandler handler(config);
  handler.handlerAdded(*ctx_);

  (void)handler.onWrite(*ctx_, makePayloadFrame(1, 128 * 1024));
  EXPECT_EQ(ctx_->written().size(), 0);
  EXPECT_EQ(handler.pendingBytes(), 128 * 1024);

  (void)handler.onWrite(*ctx_, makePayloadFrame(2, 128 * 1024));
  EXPECT_GT(ctx_->written().size(), 0);
}

TEST_F(FrameFragmentationHandlerTest, BackpressureAtExactFrameLimit) {
  FragmentationHandlerConfig config{
      .maxFragmentSize = 64 * 1024,
      .maxPendingBytes = 10 * 1024 * 1024,
      .maxPendingFrames = 2,
  };
  FrameFragmentationHandler handler(config);
  handler.handlerAdded(*ctx_);

  (void)handler.onWrite(*ctx_, makePayloadFrame(1, 128 * 1024));
  EXPECT_EQ(ctx_->written().size(), 0);

  (void)handler.onWrite(*ctx_, makePayloadFrame(2, 128 * 1024));
  EXPECT_EQ(ctx_->written().size(), 0);

  (void)handler.onWrite(*ctx_, makePayloadFrame(3, 128 * 1024));
  EXPECT_GT(ctx_->written().size(), 0);
}

TEST_F(FrameFragmentationHandlerTest, BackpressureOnImmediateQueueDrain) {
  FragmentationHandlerConfig config{.maxFragmentSize = 64 * 1024};
  FrameFragmentationHandler handler(config);
  handler.handlerAdded(*ctx_);

  (void)handler.onWrite(*ctx_, makePayloadFrame(10, 192 * 1024));
  EXPECT_EQ(handler.pendingStreamCount(), 1);

  (void)handler.onWrite(*ctx_, makePayloadFrame(1, 2 * 1024));
  (void)handler.onWrite(*ctx_, makePayloadFrame(2, 2 * 1024));
  (void)handler.onWrite(*ctx_, makePayloadFrame(3, 2 * 1024));

  EXPECT_EQ(handler.immediateQueueSize(), 3);

  ctx_->setBackpressureAt(1);

  runEventBaseLoop();

  EXPECT_EQ(ctx_->written().size(), 6);
  EXPECT_TRUE(ctx_->awaitWriteReadyCalled());
}

TEST_F(FrameFragmentationHandlerTest, BackpressureOnFirstFragment) {
  FragmentationHandlerConfig config{.maxFragmentSize = 64 * 1024};
  FrameFragmentationHandler handler(config);
  handler.handlerAdded(*ctx_);

  (void)handler.onWrite(*ctx_, makePayloadFrame(1, 192 * 1024));

  EXPECT_EQ(handler.pendingStreamCount(), 1);

  ctx_->setBackpressureAt(1);

  runEventBaseLoop();

  EXPECT_EQ(ctx_->written().size(), 3);
  EXPECT_TRUE(ctx_->written()[0].follows);
  EXPECT_TRUE(ctx_->awaitWriteReadyCalled());
}

TEST_F(FrameFragmentationHandlerTest, BackpressureOnContinuationFragment) {
  FragmentationHandlerConfig config{.maxFragmentSize = 64 * 1024};
  FrameFragmentationHandler handler(config);
  handler.handlerAdded(*ctx_);

  (void)handler.onWrite(*ctx_, makePayloadFrame(1, 192 * 1024));

  EXPECT_EQ(handler.pendingStreamCount(), 1);

  ctx_->setBackpressureAt(1);

  runEventBaseLoop();

  EXPECT_EQ(ctx_->written().size(), 3);
  EXPECT_TRUE(ctx_->written()[0].follows);
  EXPECT_TRUE(ctx_->awaitWriteReadyCalled());

  ctx_->setBackpressureAt(0);
  handler.onWriteReady(*ctx_);

  EXPECT_EQ(ctx_->written().size(), 3);
}

// ============================================================================
// Edge Cases
// ============================================================================

TEST_F(FrameFragmentationHandlerTest, ExactFragmentSizeNotFragmented) {
  FragmentationHandlerConfig config{.maxFragmentSize = 64 * 1024};
  FrameFragmentationHandler handler(config);
  handler.handlerAdded(*ctx_);

  (void)handler.onWrite(*ctx_, makePayloadFrame(1, 64 * 1024));

  EXPECT_EQ(handler.immediateQueueSize(), 0);
  EXPECT_EQ(handler.pendingStreamCount(), 0);

  ASSERT_EQ(ctx_->written().size(), 1);
  EXPECT_FALSE(ctx_->written()[0].follows);
}

TEST_F(
    FrameFragmentationHandlerTest,
    RefusesMetadataAboveTheLargestStreamQuantum) {
  constexpr size_t kDataSize = 1024;
  FrameFragmentationHandler handler;
  handler.handlerAdded(*ctx_);

  const channel_pipeline::Result tooLarge = handler.onWrite(
      *ctx_, makePayloadFrame(1, kDataSize, kMaxStreamQuantum + 1));
  // The data fits in maxFragmentSize, but metadata at the limit leaves no room
  // for data in the first fragment.
  const channel_pipeline::Result largest =
      handler.onWrite(*ctx_, makePayloadFrame(3, kDataSize, kMaxStreamQuantum));
  runEventBaseLoop();

  const std::vector<CapturedFrame> expected{
      {.streamId = 3,
       .frameType = FrameType::PAYLOAD,
       .dataSize = 0,
       .follows = true,
       .metadataSize = kMaxStreamQuantum},
      {.streamId = 3,
       .frameType = FrameType::PAYLOAD,
       .dataSize = kDataSize,
       .follows = false,
       .metadataSize = 0},
  };
  const std::map<uint32_t, std::string> expectedData{
      {3, makePayload(kDataSize)->to<std::string>()}};
  EXPECT_THAT(
      std::tuple(tooLarge, largest, ctx_->written(), ctx_->dataByStream()),
      Eq(std::tuple(
          channel_pipeline::Result::Error,
          channel_pipeline::Result::Success,
          expected,
          expectedData)));
}

TEST_F(FrameFragmentationHandlerTest, RefusalLogsTheMetadataSizeAndTheLimit) {
  // The refusal logs at most once a minute in a process. A threadsafe death
  // test runs the refusal in a new process, so refusals of earlier tests in
  // this binary cannot hide the log line.
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  EXPECT_EXIT(
      {
        const auto logs = std::make_shared<folly::TestLogHandler>();
        folly::LoggerDB::get().getCategory("")->addHandler(logs);
        FrameFragmentationHandler handler;
        handler.handlerAdded(*ctx_);
        const channel_pipeline::Result result = handler.onWrite(
            *ctx_, makePayloadFrame(1, 1024, kMaxStreamQuantum + 1));
        for (const std::string& message : logs->getMessageValues()) {
          std::fputs(message.c_str(), stderr);
        }
        std::_Exit(result == channel_pipeline::Result::Error ? 0 : 1);
      },
      ExitedWithCode(0),
      HasSubstr(
          "FrameFragmentationHandler refused a data frame with 16776704 "
          "metadata bytes. The limit is 16776703 bytes."));
}

TEST_F(
    FrameFragmentationHandlerTest,
    CutsTheFirstFragmentToFitTheLargestStreamQuantum) {
  constexpr size_t kDataSize = 20 * 1024 * 1024;
  constexpr size_t kSmallMetadata = 100;
  constexpr size_t kLargerMetadata = 600;
  FrameFragmentationHandler handler;
  handler.handlerAdded(*ctx_);

  // Each frame has more data than maxPendingBytes, so it goes out inside
  // onWrite().
  const channel_pipeline::Result first =
      handler.onWrite(*ctx_, makePayloadFrame(1, kDataSize, kSmallMetadata));
  const channel_pipeline::Result second =
      handler.onWrite(*ctx_, makePayloadFrame(3, kDataSize, kLargerMetadata));

  const std::vector<CapturedFrame> expected{
      {.streamId = 1,
       .frameType = FrameType::PAYLOAD,
       .dataSize = kMaxStreamQuantum - kSmallMetadata,
       .follows = true,
       .metadataSize = kSmallMetadata},
      {.streamId = 1,
       .frameType = FrameType::PAYLOAD,
       .dataSize = kDataSize - (kMaxStreamQuantum - kSmallMetadata),
       .follows = false,
       .metadataSize = 0},
      {.streamId = 3,
       .frameType = FrameType::PAYLOAD,
       .dataSize = kMaxStreamQuantum - kLargerMetadata,
       .follows = true,
       .metadataSize = kLargerMetadata},
      {.streamId = 3,
       .frameType = FrameType::PAYLOAD,
       .dataSize = kDataSize - (kMaxStreamQuantum - kLargerMetadata),
       .follows = false,
       .metadataSize = 0},
  };
  const std::string data = makePayload(kDataSize)->to<std::string>();
  const std::map<uint32_t, std::string> expectedData{{1, data}, {3, data}};
  // A bool keeps a failure message from printing 20 MiB of data.
  const bool allDataInOrder = ctx_->dataByStream() == expectedData;
  EXPECT_THAT(
      std::tuple(first, second, ctx_->written(), allDataInOrder),
      Eq(std::tuple(
          channel_pipeline::Result::Success,
          channel_pipeline::Result::Success,
          expected,
          true)));
}

TEST_F(
    FrameFragmentationHandlerTest, OnlyTheFirstFragmentLeavesRoomForMetadata) {
  constexpr size_t kFragmentSize = 1024 * 1024;
  constexpr size_t kMetadataSize = kMaxStreamQuantum - kFragmentSize + 1;
  FrameFragmentationHandler handler({.maxFragmentSize = kFragmentSize});
  handler.handlerAdded(*ctx_);

  // The frame has more data than maxPendingBytes, so it goes out inside
  // onWrite().
  const channel_pipeline::Result result = handler.onWrite(
      *ctx_, makePayloadFrame(1, 2 * kFragmentSize, kMetadataSize));

  const std::vector<CapturedFrame> expected{
      {.streamId = 1,
       .frameType = FrameType::PAYLOAD,
       .dataSize = kFragmentSize - 1,
       .follows = true,
       .metadataSize = kMetadataSize},
      {.streamId = 1,
       .frameType = FrameType::PAYLOAD,
       .dataSize = kFragmentSize,
       .follows = true,
       .metadataSize = 0},
      {.streamId = 1,
       .frameType = FrameType::PAYLOAD,
       .dataSize = 1,
       .follows = false,
       .metadataSize = 0},
  };
  const std::map<uint32_t, std::string> expectedData{
      {1, makePayload(2 * kFragmentSize)->to<std::string>()}};
  // A bool keeps a failure message from printing 2 MiB of data.
  const bool allDataInOrder = ctx_->dataByStream() == expectedData;
  EXPECT_THAT(
      std::tuple(result, ctx_->written(), allDataInOrder),
      Eq(std::tuple(channel_pipeline::Result::Success, expected, true)));
}

TEST_F(
    FrameFragmentationHandlerTest,
    CutsEveryFragmentToFitTheLargestStreamQuantum) {
  // Without the limit, the second fragment would carry kMaxStreamQuantum + 1
  // data bytes.
  constexpr size_t kDataSize = 2 * kMaxStreamQuantum + 1;
  FrameFragmentationHandler handler({.maxFragmentSize = 32 * 1024 * 1024});
  handler.handlerAdded(*ctx_);

  // The frame has more data than maxPendingBytes, so it goes out inside
  // onWrite().
  const channel_pipeline::Result result =
      handler.onWrite(*ctx_, makePayloadFrame(1, kDataSize));

  const std::vector<CapturedFrame> expected{
      {.streamId = 1,
       .frameType = FrameType::PAYLOAD,
       .dataSize = kMaxStreamQuantum,
       .follows = true,
       .metadataSize = 0},
      {.streamId = 1,
       .frameType = FrameType::PAYLOAD,
       .dataSize = kMaxStreamQuantum,
       .follows = true,
       .metadataSize = 0},
      {.streamId = 1,
       .frameType = FrameType::PAYLOAD,
       .dataSize = 1,
       .follows = false,
       .metadataSize = 0},
  };
  const std::map<uint32_t, std::string> expectedData{
      {1, makePayload(kDataSize)->to<std::string>()}};
  // A bool keeps a failure message from printing 32 MiB of data.
  const bool allDataInOrder = ctx_->dataByStream() == expectedData;
  EXPECT_THAT(
      std::tuple(result, ctx_->written(), allDataInOrder),
      Eq(std::tuple(channel_pipeline::Result::Success, expected, true)));
}

TEST_F(FrameFragmentationHandlerTest, StreamCompletionRemovesFromMap) {
  FragmentationHandlerConfig config{.maxFragmentSize = 64 * 1024};
  FrameFragmentationHandler handler(config);
  handler.handlerAdded(*ctx_);

  (void)handler.onWrite(*ctx_, makePayloadFrame(1, 65 * 1024));

  EXPECT_EQ(handler.pendingStreamCount(), 1);

  runEventBaseLoop();

  EXPECT_EQ(handler.pendingStreamCount(), 0);
}

TEST_F(FrameFragmentationHandlerTest, UnevenFragmentSizes) {
  FragmentationHandlerConfig config{.maxFragmentSize = 64 * 1024};
  FrameFragmentationHandler handler(config);
  handler.handlerAdded(*ctx_);

  (void)handler.onWrite(*ctx_, makePayloadFrame(1, 100 * 1024));

  runEventBaseLoop();

  ASSERT_EQ(ctx_->written().size(), 2);

  EXPECT_EQ(ctx_->written()[0].dataSize, 64 * 1024);
  EXPECT_TRUE(ctx_->written()[0].follows);

  EXPECT_EQ(ctx_->written()[1].dataSize, 36 * 1024);
  EXPECT_FALSE(ctx_->written()[1].follows);
}

TEST_F(FrameFragmentationHandlerTest, CancelClearsStreamFragments) {
  FragmentationHandlerConfig config{.maxFragmentSize = 64 * 1024};
  FrameFragmentationHandler handler(config);
  handler.handlerAdded(*ctx_);

  (void)handler.onWrite(*ctx_, makePayloadFrame(1, 128 * 1024));

  EXPECT_EQ(handler.pendingStreamCount(), 1);
  EXPECT_TRUE(handler.hasPendingStream(1));

  (void)handler.onWrite(*ctx_, makeCancelFrame(1));

  EXPECT_EQ(handler.pendingStreamCount(), 0);
  EXPECT_FALSE(handler.hasPendingStream(1));

  runEventBaseLoop();

  bool cancelSent = false;
  for (const auto& f : ctx_->written()) {
    if (f.frameType == FrameType::CANCEL && f.streamId == 1) {
      cancelSent = true;
    }
  }
  EXPECT_TRUE(cancelSent);
}

TEST_F(FrameFragmentationHandlerTest, ErrorClearsStreamFragments) {
  FragmentationHandlerConfig config{.maxFragmentSize = 64 * 1024};
  FrameFragmentationHandler handler(config);
  handler.handlerAdded(*ctx_);

  (void)handler.onWrite(*ctx_, makePayloadFrame(1, 128 * 1024));

  EXPECT_EQ(handler.pendingStreamCount(), 1);
  EXPECT_TRUE(handler.hasPendingStream(1));

  (void)handler.onWrite(*ctx_, makeErrorFrame(1));

  EXPECT_EQ(handler.pendingStreamCount(), 0);
  EXPECT_FALSE(handler.hasPendingStream(1));

  runEventBaseLoop();

  bool errorSent = false;
  for (const auto& f : ctx_->written()) {
    if (f.frameType == FrameType::ERROR && f.streamId == 1) {
      errorSent = true;
    }
  }
  EXPECT_TRUE(errorSent);
}

TEST_F(FrameFragmentationHandlerTest, CancelOnlyAffectsTargetStream) {
  FragmentationHandlerConfig config{.maxFragmentSize = 64 * 1024};
  FrameFragmentationHandler handler(config);
  handler.handlerAdded(*ctx_);

  (void)handler.onWrite(*ctx_, makePayloadFrame(1, 128 * 1024));
  (void)handler.onWrite(*ctx_, makePayloadFrame(2, 128 * 1024));

  EXPECT_EQ(handler.pendingStreamCount(), 2);

  (void)handler.onWrite(*ctx_, makeCancelFrame(1));

  EXPECT_EQ(handler.pendingStreamCount(), 1);
  EXPECT_FALSE(handler.hasPendingStream(1));
  EXPECT_TRUE(handler.hasPendingStream(2));

  runEventBaseLoop();

  size_t stream2Fragments = 0;
  for (const auto& f : ctx_->written()) {
    if (f.streamId == 2 && f.frameType == FrameType::PAYLOAD) {
      stream2Fragments++;
    }
  }
  EXPECT_EQ(stream2Fragments, 2);
}

// ============================================================================
// Multi-frame-per-stream regression — exercises the per-stream FIFO.
// ============================================================================

TEST_F(FrameFragmentationHandlerTest, MultiFramesPerStreamAllSent) {
  FragmentationHandlerConfig config{.maxFragmentSize = 64 * 1024};
  FrameFragmentationHandler handler(config);
  handler.handlerAdded(*ctx_);

  // Two large PAYLOAD frames for the same stream. With the pre-rework
  // single-payload state the second would have clobbered the first's
  // remaining bytes; both must come out intact and in order.
  (void)handler.onWrite(*ctx_, makePayloadFrame(1, 100 * 1024)); // 2 fragments
  (void)handler.onWrite(*ctx_, makePayloadFrame(1, 100 * 1024)); // 2 fragments

  runEventBaseLoop();

  ASSERT_EQ(ctx_->written().size(), 4);
  size_t totalBytes = 0;
  for (const auto& f : ctx_->written()) {
    EXPECT_EQ(f.streamId, 1);
    EXPECT_EQ(f.frameType, FrameType::PAYLOAD);
    totalBytes += f.dataSize;
  }
  EXPECT_EQ(totalBytes, 200 * 1024);
}

// ============================================================================
// Error Propagation Tests (downstream Result::Error)
// ============================================================================

TEST_F(FrameFragmentationHandlerTest, AsyncFlushStopsImmediateQueueOnError) {
  // Forces several small frames into immediateQueue_ behind a fragmented
  // stream, then errors downstream after the first write. The Step 1 loop
  // must stop after the failed write, and the async-discard path must
  // surface a single inbound fireException.
  FragmentationHandlerConfig config{.maxFragmentSize = 64 * 1024};
  FrameFragmentationHandler handler(config);
  handler.handlerAdded(*ctx_);

  (void)handler.onWrite(*ctx_, makePayloadFrame(10, 192 * 1024));
  (void)handler.onWrite(*ctx_, makePayloadFrame(1, 2 * 1024));
  (void)handler.onWrite(*ctx_, makePayloadFrame(2, 2 * 1024));
  (void)handler.onWrite(*ctx_, makePayloadFrame(3, 2 * 1024));
  ASSERT_EQ(handler.immediateQueueSize(), 3);

  ctx_->setErrorAt(1);
  runEventBaseLoop();

  EXPECT_EQ(ctx_->written().size(), 1);
  EXPECT_EQ(ctx_->fireExceptionCount(), 1);
  EXPECT_NE(
      std::string(ctx_->lastException().what())
          .find("FrameFragmentationHandler: downstream write failed"),
      std::string::npos);
}

TEST_F(FrameFragmentationHandlerTest, AsyncFlushStopsSrptLoopOnError) {
  // Two streams in the SRPT heap; downstream errors after two writes. The
  // Step 2 SRPT loop must stop and the async-discard path must fire one
  // inbound exception.
  FragmentationHandlerConfig config{.maxFragmentSize = 64 * 1024};
  FrameFragmentationHandler handler(config);
  handler.handlerAdded(*ctx_);

  (void)handler.onWrite(*ctx_, makePayloadFrame(1, 192 * 1024));
  (void)handler.onWrite(*ctx_, makePayloadFrame(2, 192 * 1024));

  ctx_->setErrorAt(2);
  runEventBaseLoop();

  EXPECT_EQ(ctx_->written().size(), 2);
  EXPECT_EQ(ctx_->fireExceptionCount(), 1);
  EXPECT_NE(
      std::string(ctx_->lastException().what())
          .find("FrameFragmentationHandler: downstream write failed"),
      std::string::npos);
}

TEST_F(
    FrameFragmentationHandlerTest,
    OnWriteReadyFiresExceptionOnDownstreamError) {
  // Readiness may flush newly accepted data and must propagate errors.
  FragmentationHandlerConfig config{.maxFragmentSize = 64 * 1024};
  FrameFragmentationHandler handler(config);
  handler.handlerAdded(*ctx_);

  (void)handler.onWrite(*ctx_, makePayloadFrame(1, 192 * 1024));

  ctx_->setBackpressureAt(1);
  runEventBaseLoop();
  ASSERT_TRUE(ctx_->awaitWriteReadyCalled());
  ASSERT_EQ(ctx_->fireExceptionCount(), 0);

  (void)handler.onWrite(*ctx_, makePayloadFrame(2, 192 * 1024));
  ctx_->setBackpressureAt(0);
  ctx_->setErrorAt(ctx_->written().size());
  handler.onWriteReady(*ctx_);

  EXPECT_EQ(ctx_->fireExceptionCount(), 1);
  EXPECT_NE(
      std::string(ctx_->lastException().what())
          .find("FrameFragmentationHandler: downstream write failed"),
      std::string::npos);
}

TEST_F(FrameFragmentationHandlerTest, SyncThresholdFlushDoesNotFireException) {
  // onWrite triggers a sync flushNow via the maxPendingBytes threshold.
  // The caller (channel boundary) owns escalation on the sync path, so
  // onWrite must return Result::Error WITHOUT firing fireException — firing
  // both would double-fire at the channel.
  FragmentationHandlerConfig config{
      .maxFragmentSize = 64 * 1024,
      .maxPendingBytes = 128 * 1024,
  };
  FrameFragmentationHandler handler(config);
  handler.handlerAdded(*ctx_);

  (void)handler.onWrite(*ctx_, makePayloadFrame(1, 128 * 1024));
  ASSERT_EQ(ctx_->written().size(), 0);

  ctx_->setErrorAt(0);

  auto result = handler.onWrite(*ctx_, makePayloadFrame(2, 192 * 1024));
  EXPECT_EQ(
      result, apache::thrift::fast_thrift::channel_pipeline::Result::Error);
  EXPECT_EQ(ctx_->fireExceptionCount(), 0);
}

// ============================================================================
// Tracker wiring — FrameFragmentationHandlerT drives the tracker from doFlush.
// ============================================================================

TEST_F(
    FrameFragmentationHandlerTest, TrackerSeesEachFragmentWithFrameDoneFlag) {
  FragmentationHandlerConfig config{.maxFragmentSize = 64 * 1024};
  FrameFragmentationHandlerT<RecordingTracker> handler(config);
  handler.handlerAdded(*ctx_);

  // 192KB -> 3 fragments; only the last completes the original frame.
  (void)handler.onWrite(*ctx_, makePayloadFrame(1, 192 * 1024));
  runEventBaseLoop();

  const auto& frags = handler.tracker().fragments;
  ASSERT_EQ(frags.size(), 3u);
  EXPECT_EQ(frags[0].streamId, 1u);
  EXPECT_FALSE(frags[0].isLastFragment);
  EXPECT_FALSE(frags[1].isLastFragment);
  EXPECT_TRUE(frags[2].isLastFragment);
  EXPECT_EQ(handler.tracker().flushCount, 1u);
}

TEST_F(
    FrameFragmentationHandlerTest, TrackerSeesImmediateQueueFrameAsComplete) {
  FragmentationHandlerConfig config{.maxFragmentSize = 64 * 1024};
  FrameFragmentationHandlerT<RecordingTracker> handler(config);
  handler.handlerAdded(*ctx_);

  // Large frame parks a pending stream; the smaller frame lands in the
  // immediate queue and drains first as a single complete frame (isLast=true).
  (void)handler.onWrite(*ctx_, makePayloadFrame(10, 192 * 1024));
  (void)handler.onWrite(*ctx_, makePayloadFrame(1, 32 * 1024));
  ASSERT_EQ(handler.immediateQueueSize(), 1u);

  runEventBaseLoop();

  const auto& frags = handler.tracker().fragments;
  ASSERT_EQ(frags.size(), 4u);
  // Immediate queue drains first: one complete frame for stream 1.
  EXPECT_EQ(frags[0].streamId, 1u);
  EXPECT_TRUE(frags[0].isLastFragment);
  // Then the 3 SRPT fragments of stream 10.
  EXPECT_EQ(frags[1].streamId, 10u);
  EXPECT_FALSE(frags[1].isLastFragment);
  EXPECT_FALSE(frags[2].isLastFragment);
  EXPECT_TRUE(frags[3].isLastFragment);
  EXPECT_EQ(handler.tracker().flushCount, 1u);
}

TEST_F(
    FrameFragmentationHandlerTest, TrackerSkipsErroredFragmentAndDoesNotFlush) {
  FragmentationHandlerConfig config{.maxFragmentSize = 64 * 1024};
  FrameFragmentationHandlerT<RecordingTracker> handler(config);
  handler.handlerAdded(*ctx_);

  // 192KB -> 3 fragments; downstream errors on the 3rd (frame-completing) one.
  // The errored fragment must leave no record, and onFlush must not run on the
  // error path.
  (void)handler.onWrite(*ctx_, makePayloadFrame(1, 192 * 1024));
  ctx_->setErrorAt(2);
  runEventBaseLoop();

  const auto& frags = handler.tracker().fragments;
  ASSERT_EQ(frags.size(), 2u);
  EXPECT_FALSE(frags[0].isLastFragment);
  EXPECT_FALSE(frags[1].isLastFragment);
  EXPECT_EQ(handler.tracker().flushCount, 0u);
}

TEST_F(
    FrameFragmentationHandlerTest,
    TrackerSkipsErroredImmediateQueueFrameAndDoesNotFlush) {
  FragmentationHandlerConfig config{.maxFragmentSize = 64 * 1024};
  FrameFragmentationHandlerT<RecordingTracker> handler(config);
  handler.handlerAdded(*ctx_);

  // The smaller frame waits in the immediate queue and goes out first.
  // Downstream errors on it, so it must leave no record.
  (void)handler.onWrite(*ctx_, makePayloadFrame(10, 192 * 1024));
  (void)handler.onWrite(*ctx_, makePayloadFrame(1, 32 * 1024));
  ctx_->setErrorAt(0);
  runEventBaseLoop();

  EXPECT_THAT(
      std::tuple(
          handler.tracker().fragments.size(), handler.tracker().flushCount),
      Eq(std::tuple(size_t{0}, size_t{0})));
}

TEST_F(
    FrameFragmentationHandlerTest,
    TrackerCountsAndFlushesBackpressuredFragments) {
  FragmentationHandlerConfig config{.maxFragmentSize = 64 * 1024};
  FrameFragmentationHandlerT<RecordingTracker> handler(config);
  handler.handlerAdded(*ctx_);

  // Pressure on the final fragment still completes the accepted frame.
  (void)handler.onWrite(*ctx_, makePayloadFrame(1, 192 * 1024));
  ctx_->setBackpressureAt(2);
  runEventBaseLoop();

  EXPECT_TRUE(ctx_->awaitWriteReadyCalled());
  const auto& frags = handler.tracker().fragments;
  ASSERT_EQ(frags.size(), 3u);
  EXPECT_TRUE(frags[2].isLastFragment);
  EXPECT_EQ(handler.tracker().flushCount, 1u);
}

TEST(FrameFragmentationHandlerCloseTest, AllSuccessOwnsNoRemainder) {
  for (const size_t fragmentSize : kCloseFragmentSizes) {
    folly::EventBase eventBase;
    MockContext context(&eventBase);
    FrameFragmentationHandlerT<RecordingTracker> handler(
        {.maxFragmentSize = fragmentSize});
    handler.handlerAdded(context);

    EXPECT_THAT(
        handler.onWrite(context, makePayloadFrame(1, kCloseResponseSize)),
        Eq(channel_pipeline::Result::Success));

    EXPECT_THAT(context.written(), SizeIs(kCloseResponseSize / fragmentSize));
    EXPECT_THAT(
        handler.tracker().fragments, SizeIs(kCloseResponseSize / fragmentSize));
    EXPECT_THAT(handler.hasPendingWrites(), IsFalse());
  }
}

TEST(FrameFragmentationHandlerCloseTest, ErrorBeforeCloseDropsOwnedRemainder) {
  for (const size_t fragmentSize : kCloseFragmentSizes) {
    folly::EventBase eventBase;
    MockContext context(&eventBase);
    context.setErrorAt(1);
    FrameFragmentationHandlerT<RecordingTracker> handler(
        {.maxFragmentSize = fragmentSize});
    handler.handlerAdded(context);

    EXPECT_THAT(
        handler.onWrite(context, makePayloadFrame(1, kCloseResponseSize)),
        Eq(channel_pipeline::Result::Error));
    EXPECT_THAT(context.written(), SizeIs(1));
    EXPECT_THAT(handler.tracker().fragments, SizeIs(1));
    EXPECT_THAT(
        handler.pendingBytes(), Eq(kCloseResponseSize - 2 * fragmentSize));

    handler.onPipelineInactive(context);

    EXPECT_THAT(handler.hasPendingWrites(), IsFalse());
    EXPECT_THAT(context.written(), SizeIs(1));
  }
}

TEST(FrameFragmentationHandlerCloseTest, BackpressureBeforeCloseKeepsDraining) {
  for (const size_t fragmentSize : kCloseFragmentSizes) {
    folly::EventBase eventBase;
    MockContext context(&eventBase);
    context.setBackpressureAt(1);
    FrameFragmentationHandlerT<RecordingTracker> handler(
        {.maxFragmentSize = fragmentSize});
    handler.handlerAdded(context);

    EXPECT_THAT(
        handler.onWrite(context, makePayloadFrame(1, kCloseResponseSize)),
        Eq(channel_pipeline::Result::Backpressure));
    EXPECT_THAT(
        handler.tracker().fragments, SizeIs(kCloseResponseSize / fragmentSize));
    EXPECT_THAT(handler.pendingBytes(), Eq(size_t{0}));
    EXPECT_THAT(context.awaitWriteReadyCalled(), IsTrue());

    handler.onPipelineInactive(context);

    EXPECT_THAT(handler.hasPendingWrites(), IsFalse());
    EXPECT_THAT(context.written(), SizeIs(kCloseResponseSize / fragmentSize));
  }
}

// ============================================================================
// Tracker wiring — frames that BYPASS fragmentation.
//
// The tracker's FIFO is popped against a downstream batch frame count, and the
// batcher counts every frame it receives. So the invariant is not just "each
// fragment is recorded" but "every frame handed downstream is recorded,
// exactly once" — bypassed frames included. A bypass path that skipped the
// tracker would leave the FIFO permanently short and misattribute every later
// write completion.
// ============================================================================

TEST_F(FrameFragmentationHandlerTest, TrackerRecordsFastPathFrameAsComplete) {
  FragmentationHandlerConfig config{.maxFragmentSize = 64 * 1024};
  FrameFragmentationHandlerT<RecordingTracker> handler(config);
  handler.handlerAdded(*ctx_);

  // Under maxFragmentSize with nothing pending: forwarded inline, never
  // touching doFlush. This is the common case for small request-response
  // traffic and the one most likely to go unrecorded.
  (void)handler.onWrite(*ctx_, makePayloadFrame(1, 1024));

  ASSERT_EQ(ctx_->written().size(), 1u);
  const auto& frags = handler.tracker().fragments;
  ASSERT_EQ(frags.size(), 1u);
  EXPECT_EQ(frags[0].streamId, 1u);
  EXPECT_TRUE(frags[0].isLastFragment);
}

TEST_F(
    FrameFragmentationHandlerTest, TrackerRecordsNonFragmentableFrameOnBypass) {
  FrameFragmentationHandlerT<RecordingTracker> handler{};
  handler.handlerAdded(*ctx_);

  (void)handler.onWrite(*ctx_, makeKeepAliveFrame());

  ASSERT_EQ(ctx_->written().size(), 1u);
  const auto& frags = handler.tracker().fragments;
  ASSERT_EQ(frags.size(), 1u);
  EXPECT_TRUE(frags[0].isLastFragment);
}

TEST_F(FrameFragmentationHandlerTest, TrackerRecordsTerminalFrameOnBypass) {
  FrameFragmentationHandlerT<RecordingTracker> handler{};
  handler.handlerAdded(*ctx_);

  (void)handler.onWrite(*ctx_, makeCancelFrame(3));

  ASSERT_EQ(ctx_->written().size(), 1u);
  const auto& frags = handler.tracker().fragments;
  ASSERT_EQ(frags.size(), 1u);
  EXPECT_EQ(frags[0].streamId, 3u);
  EXPECT_TRUE(frags[0].isLastFragment);
}

TEST_F(FrameFragmentationHandlerTest, TrackerSkipsErroredBypassFrame) {
  FrameFragmentationHandlerT<RecordingTracker> handler{};
  handler.handlerAdded(*ctx_);

  // A frame the downstream rejected never reaches the batcher, so it must not
  // be counted either — same discipline the doFlush path uses.
  ctx_->setErrorAt(0);
  (void)handler.onWrite(*ctx_, makePayloadFrame(1, 1024));

  EXPECT_EQ(ctx_->written().size(), 0u);
  EXPECT_EQ(handler.tracker().fragments.size(), 0u);
}

TEST_F(
    FrameFragmentationHandlerTest,
    TrackerRecordCountMatchesDownstreamWritesAcrossMixedTraffic) {
  FragmentationHandlerConfig config{.maxFragmentSize = 64 * 1024};
  FrameFragmentationHandlerT<RecordingTracker> handler(config);
  handler.handlerAdded(*ctx_);

  // Interleave every path through onWrite: bypass (non-fragmentable), fast
  // path, fragmented, immediate-queue, and terminal.
  (void)handler.onWrite(*ctx_, makeKeepAliveFrame());
  (void)handler.onWrite(*ctx_, makePayloadFrame(1, 1024));
  (void)handler.onWrite(*ctx_, makePayloadFrame(10, 192 * 1024));
  (void)handler.onWrite(*ctx_, makePayloadFrame(2, 32 * 1024));
  (void)handler.onWrite(*ctx_, makeCancelFrame(5));
  runEventBaseLoop();

  // The invariant the batch FIFO depends on: one record per downstream write.
  EXPECT_EQ(handler.tracker().fragments.size(), ctx_->written().size());

  // And one frame-completing record per original frame, not per fragment.
  size_t completed = 0;
  for (const auto& f : handler.tracker().fragments) {
    completed += f.isLastFragment ? 1 : 0;
  }
  EXPECT_EQ(completed, 5u);
}

TEST_F(
    FrameFragmentationHandlerTest,
    PassThroughInlineCompletionSeesTheFragmentRecord) {
  CompletionHandler handler;
  handler.handlerAdded(*ctx_);
  ctx_->setWriteObserver([&] {
    handler.on<TestBatchWriteCompleteEvent>(
        *ctx_,
        TestBatchWriteCompleteEvent{
            .status = transport::WriteCompletionStatus::Success,
            .frameCount = 1,
            .bytes = 0,
            .quiesced = true,
        });
  });

  EXPECT_THAT(
      handler.onWrite(*ctx_, makeKeepAliveFrame()),
      Eq(channel_pipeline::Result::Success));

  const std::vector<TestFrameWriteCompleteEvent> expected{{
      .streamId = 0,
      .status = transport::WriteCompletionStatus::Success,
      .quiesced = true,
  }};
  EXPECT_THAT(ctx_->completed(), Eq(expected));
}

TEST_F(
    FrameFragmentationHandlerTest, FlushInlineCompletionSeesTheFragmentRecord) {
  CompletionHandler handler({.maxFragmentSize = 4096});
  handler.handlerAdded(*ctx_);
  ctx_->setWriteObserver([&] {
    handler.on<TestBatchWriteCompleteEvent>(
        *ctx_,
        TestBatchWriteCompleteEvent{
            .status = transport::WriteCompletionStatus::Success,
            .frameCount = 1,
            .bytes = 1,
            .quiesced = false,
        });
  });
  // Stream 1 needs two fragments. Stream 3 needs none, but it waits in the
  // immediate queue behind stream 1, so the flush writes all three frames.
  (void)handler.onWrite(*ctx_, makePayloadFrame(1, 2 * 4096));
  (void)handler.onWrite(*ctx_, makePayloadFrame(3, 2048));

  runEventBaseLoop();

  const std::vector<TestFrameWriteCompleteEvent> expected{
      {.streamId = 3,
       .status = transport::WriteCompletionStatus::Success,
       .quiesced = false},
      {.streamId = 1,
       .status = transport::WriteCompletionStatus::Success,
       .quiesced = false},
  };
  EXPECT_THAT(ctx_->completed(), Eq(expected));
}

} // namespace
} // namespace apache::thrift::fast_thrift::frame::write::handler
