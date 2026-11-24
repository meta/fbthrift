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

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>

#include <folly/io/IOBuf.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/AsyncTransport.h>
#include <folly/io/async/Liburing.h>
#include <folly/io/async/test/MockAsyncTransport.h>
#include <folly/portability/GMock.h>

#if FOLLY_HAS_LIBURING
#include <folly/io/async/IoUringBackend.h>
#endif

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineRef.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/StaticPipelineBuilder.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/test/MockAdapters.h>

#include <thrift/lib/cpp2/fast_thrift/frame/read/FrameLengthParser.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/FrameLength.h>
#include <thrift/lib/cpp2/fast_thrift/transport/TransportHandler.h>
#include <thrift/lib/cpp2/fast_thrift/transport/test/PassthroughParser.h>

namespace apache::thrift::fast_thrift::transport {

using namespace apache::thrift::fast_thrift::channel_pipeline;
using namespace apache::thrift::fast_thrift::channel_pipeline::test;
using MockAppHandler = StaticMockTailHandler;
using namespace testing;

// These tests exercise the transport itself, not any wire protocol, so they
// pair it with a parser that does no framing.
using TransportHandler = test::PassthroughTransportHandler;

namespace {
// Build a simple byte buffer for testing
std::unique_ptr<folly::IOBuf> buildTestData(size_t size) {
  auto buf = folly::IOBuf::create(size);
  std::memset(buf->writableData(), 'x', size);
  buf->append(size);
  return buf;
}

// Frames nothing. Exists only to be a Parser without consumeBuffer.
class NonMovableParser {
 public:
  void getReadBuffer(void** bufReturn, size_t* lenReturn) noexcept {
    *bufReturn = buffer_.data();
    *lenReturn = buffer_.size();
  }

  template <typename Sink>
  Result consume(size_t, Sink&&) noexcept {
    return Result::Success;
  }

  // NOLINTNEXTLINE(clang-diagnostic-unneeded-member-function)
  void setIOBufFactory(folly::IOBufFactory*) noexcept {}

  template <typename Sink>
  Result resume(Sink&&) noexcept {
    return Result::Success;
  }

  void reset() noexcept {}

 private:
  std::array<uint8_t, 256> buffer_{};
};

static_assert(Parser<NonMovableParser>);
static_assert(!MovableBufferParser<NonMovableParser>);

using NonMovableTransportHandler =
    TransportHandlerT<NoOpWriteCompleteEventFactory, NonMovableParser>;

class BufferCallbackSocket : public folly::AsyncSocket {
 public:
  using folly::AsyncSocket::AsyncSocket;

  BufferCallback* bufferCallback() const noexcept { return bufferCallback_; }
};
} // namespace

class TransportExceptionObserver {
 public:
  template <typename Context>
  Result onRead(Context& ctx, TypeErasedBox&& msg) noexcept {
    return ctx.fireRead(std::move(msg));
  }
  template <typename Context>
  Result onWrite(Context& ctx, TypeErasedBox&& msg) noexcept {
    return ctx.fireWrite(std::move(msg));
  }
  template <typename Context>
  void onException(Context& ctx, folly::exception_wrapper&& error) noexcept {
    ++exceptions_;
    if (onException_) {
      onException_(error);
    } else {
      ctx.fireException(std::move(error));
    }
  }

  void setOnException(
      std::function<void(const folly::exception_wrapper&)> callback) {
    onException_ = std::move(callback);
  }

  template <typename Context>
  void onPipelineActive(Context&) noexcept {
    ++activated_;
  }
  template <typename Context>
  void onPipelineInactive(Context&) noexcept {
    ++deactivated_;
  }
  template <typename Context>
  void onReadReady(Context&) noexcept {}
  template <typename Context>
  void onWriteReady(Context&) noexcept {}
  template <typename Context>
  void handlerAdded(Context&) noexcept {}
  template <typename Context>
  void handlerRemoved(Context&) noexcept {}

  int exceptionCount() const { return exceptions_; }
  int pipelineActivatedCount() const { return activated_; }
  int pipelineDeactivatedCount() const { return deactivated_; }

 private:
  std::function<void(const folly::exception_wrapper&)> onException_;
  int exceptions_{0};
  int activated_{0};
  int deactivated_{0};
};

HANDLER_TAG(exception_handler);

class TransportHandlerTest : public ::testing::Test {
 protected:
  void SetUp() override { appHandler_.reset(); }

  /**
   * Creates a TransportHandler and Pipeline.
   */
  std::pair<TransportHandler::Ptr, PipelineOwner> createHandlerAndPipeline() {
    return createHandlerAndPipeline(nullptr);
  }

  std::pair<TransportHandler::Ptr, PipelineOwner> createHandlerAndPipeline(
      folly::AsyncTransport* wrappedTransport) {
    return createHandlerAndPipeline<TransportHandler>(wrappedTransport);
  }

  template <typename Handler>
  std::pair<typename Handler::Ptr, PipelineOwner> createHandlerAndPipeline(
      folly::AsyncTransport* wrappedTransport) {
    auto socket = folly::AsyncTransport::UniquePtr(
        new NiceMock<folly::test::MockAsyncTransport>());
    mockSocket_ = static_cast<folly::test::MockAsyncTransport*>(socket.get());
    // onConnect() DCHECKs the socket is good. Tests that want to drive
    // not-good behavior override this in-line.
    ON_CALL(*mockSocket_, good()).WillByDefault(Return(true));
    // DrainTimeout needs a valid EventBase from the socket.
    ON_CALL(*mockSocket_, getEventBase()).WillByDefault(Return(&evb_));
    ON_CALL(*mockSocket_, getWrappedTransport())
        .WillByDefault(Return(wrappedTransport));

    auto handler = Handler::create(std::move(socket), 256, 4096);

    auto pipeline =
        StaticPipelineBuilder<Handler, MockAppHandler, SimpleBufferAllocator>()
            .setEventBase(&evb_)
            .setHead(handler.get())
            .setTail(&appHandler_)
            .setAllocator(&allocator_)
            .build();

    handler->setPipeline(pipeline.get());

    return {std::move(handler), PipelineOwner(std::move(pipeline))};
  }

  /**
   * Creates a TransportHandler and pipeline with an observer that can capture
   * exceptions.
   *
   * Pipeline must die before TransportHandler so its destruction defers via
   * TH's pipelineGuard_ and runs handlerRemoved while TH is still alive
   * (inside TH's ~T body via resetPipeline -> pipelineGuard_.reset()). Reverse
   * order causes UAF.
   *
   * Class member destruction order is reverse declaration order in both
   * libstdc++ and libc++, so declare `pipeline` last to make it destroyed
   * first. std::tuple does NOT have a portable destruction order — libstdc++
   * destroys front-to-back, libc++ destroys back-to-front — so we cannot
   * return one here.
   */
  struct HandlerAndPipelineWithExceptionHandler {
    TransportHandler::Ptr handler;
    TransportExceptionObserver* mockHandler;
    PipelineOwner pipeline;
  };

  HandlerAndPipelineWithExceptionHandler
  createHandlerAndPipelineWithExceptionHandler() {
    auto socket = folly::AsyncTransport::UniquePtr(
        new NiceMock<folly::test::MockAsyncTransport>());
    mockSocket_ = static_cast<folly::test::MockAsyncTransport*>(socket.get());
    // onConnect() DCHECKs the socket is good. Tests that want to drive
    // not-good behavior override this in-line.
    ON_CALL(*mockSocket_, good()).WillByDefault(Return(true));
    // DrainTimeout needs a valid EventBase from the socket.
    ON_CALL(*mockSocket_, getEventBase()).WillByDefault(Return(&evb_));

    auto handler = TransportHandler::create(std::move(socket), 256, 4096);

    auto pipeline =
        StaticPipelineBuilder<
            TransportHandler,
            MockAppHandler,
            SimpleBufferAllocator>()
            .setEventBase(&evb_)
            .setHead(handler.get())
            .setTail(&appHandler_)
            .setAllocator(&allocator_)
            .addNextDuplex<TransportExceptionObserver>(exception_handler_tag)
            .build();
    auto* mockHandlerPtr = &pipeline->handler(exception_handler_tag);
    handler->setPipeline(pipeline.get());

    return {
        std::move(handler), mockHandlerPtr, PipelineOwner(std::move(pipeline))};
  }

  folly::EventBase evb_;
  folly::test::MockAsyncTransport* mockSocket_{nullptr};
  MockAppHandler appHandler_;
  SimpleBufferAllocator allocator_;
};

class FramedTransportHandlerTest : public TransportHandlerTest,
                                   public WithParamInterface<bool> {
 protected:
  using Handler = TransportHandlerT<
      NoOpWriteCompleteEventFactory,
      frame::read::FrameLengthParser>;

  void feedFrames(Handler& handler) {
    folly::IOBufQueue queue{folly::IOBufQueue::cacheChainLength()};
    for (size_t size : {16, 32, 48}) {
      auto buf = folly::IOBuf::create(size + frame::kMetadataLengthSize);
      frame::write::writeFrameLength(buf->writableData(), size);
      std::memset(buf->writableData() + frame::kMetadataLengthSize, 'x', size);
      buf->append(size + frame::kMetadataLengthSize);
      queue.append(std::move(buf));
    }
    auto bytes = queue.move();
    if (GetParam()) {
      handler.readBufferAvailable(std::move(bytes));
    } else {
      void* buf = nullptr;
      size_t capacity = 0;
      handler.getReadBuffer(&buf, &capacity);
      bytes->coalesce();
      ASSERT_GE(capacity, bytes->length());
      std::memcpy(buf, bytes->data(), bytes->length());
      handler.readDataAvailable(bytes->length());
    }
  }
};

TEST_P(
    FramedTransportHandlerTest, ResumeDrainsBufferedFramesBeforeReadingSocket) {
  auto [handler, pipeline] = createHandlerAndPipeline<Handler>(nullptr);
  handler->onConnect();

  std::vector<size_t> sizes;
  Result result = Result::Backpressure;
  appHandler_.setOnReadCallback([&](TypeErasedBox&& msg) {
    sizes.push_back(msg.get<BytesPtr>()->computeChainDataLength());
    return result;
  });

  feedFrames(*handler);
  EXPECT_THAT(sizes, ElementsAre(16));
  ASSERT_TRUE(handler->debugState().isPaused);

  EXPECT_CALL(*mockSocket_, setReadCB(nullptr)).Times(AnyNumber());
  EXPECT_CALL(*mockSocket_, setReadCB(handler.get())).WillOnce([&](auto*) {
    EXPECT_THAT(sizes, ElementsAre(16, 32, 48));
  });
  result = Result::Success;
  pipeline->onReadReady();
  EXPECT_THAT(sizes, ElementsAre(16, 32, 48));
  EXPECT_FALSE(handler->debugState().isPaused);
}

TEST_P(FramedTransportHandlerTest, ResumeCanEncounterBackpressureAgain) {
  auto [handler, pipeline] = createHandlerAndPipeline<Handler>(nullptr);
  handler->onConnect();
  appHandler_.setOnReadCallback(
      [](TypeErasedBox&&) { return Result::Backpressure; });
  feedFrames(*handler);
  ASSERT_EQ(appHandler_.readCount(), 1);

  EXPECT_CALL(*mockSocket_, setReadCB(nullptr)).Times(AnyNumber());
  EXPECT_CALL(*mockSocket_, setReadCB(handler.get())).Times(0);
  pipeline->onReadReady();
  EXPECT_EQ(appHandler_.readCount(), 2);
  EXPECT_TRUE(handler->debugState().isPaused);
  Mock::VerifyAndClearExpectations(mockSocket_);

  appHandler_.setOnReadCallback(
      [](TypeErasedBox&&) { return Result::Success; });
  EXPECT_CALL(*mockSocket_, setReadCB(nullptr)).Times(AnyNumber());
  EXPECT_CALL(*mockSocket_, setReadCB(handler.get())).Times(1);
  pipeline->onReadReady();
  EXPECT_EQ(appHandler_.readCount(), 3);
  EXPECT_FALSE(handler->debugState().isPaused);
}

TEST_P(FramedTransportHandlerTest, CloseDuringResumeStopsBufferedDispatch) {
  auto [handler, pipeline] = createHandlerAndPipeline<Handler>(nullptr);
  handler->onConnect();
  appHandler_.setOnReadCallback(
      [](TypeErasedBox&&) { return Result::Backpressure; });
  feedFrames(*handler);
  ASSERT_EQ(appHandler_.readCount(), 1);

  appHandler_.setOnReadCallback([&](TypeErasedBox&&) {
    handler->close(folly::exception_wrapper{});
    return Result::Error;
  });
  EXPECT_CALL(*mockSocket_, setReadCB(nullptr)).Times(AnyNumber());
  EXPECT_CALL(*mockSocket_, setReadCB(handler.get())).Times(0);
  pipeline->onReadReady();
  EXPECT_EQ(appHandler_.readCount(), 2);
  EXPECT_EQ(handler->state(), Handler::State::Closed);
  pipeline->onReadReady();
  EXPECT_EQ(appHandler_.readCount(), 2);
}

INSTANTIATE_TEST_SUITE_P(
    ReadDelivery, FramedTransportHandlerTest, Values(false, true));

// Test: Write Path
TEST_F(TransportHandlerTest, WritePath) {
  auto [handler, pipeline] = createHandlerAndPipeline();
  handler->onConnect();

  std::shared_ptr<folly::IOBuf> capturedBuf;
  folly::AsyncTransport::WriteCallback* capturedCallback = nullptr;

  EXPECT_CALL(*mockSocket_, writeChain(_, _, _))
      .WillOnce(
          DoAll(SaveArg<0>(&capturedCallback), SaveArg<1>(&capturedBuf), [&]() {
            if (capturedCallback) {
              capturedCallback->writeSuccess();
            }
          }));

  auto bytes = folly::IOBuf::copyBuffer("test data");
  Result result = handler->onWrite(
      channel_pipeline::test::inertEndpointContext(),
      TypeErasedBox(std::move(bytes)));

  EXPECT_EQ(result, Result::Success);
  EXPECT_EQ(handler->debugState().pendingWrites, 0);
  EXPECT_FALSE(handler->debugState().isWriteSaturated);
}

// Test: Pause and Resume Read
TEST_F(TransportHandlerTest, PauseAndResumeRead) {
  auto [handler, pipeline] = createHandlerAndPipeline();

  // Initially readPaused_ is true (not reading yet)
  EXPECT_TRUE(handler->debugState().isPaused);

  // resumeRead should set the handler itself as read callback (implements
  // ReadCallback)
  EXPECT_CALL(*mockSocket_, setReadCB(handler.get())).Times(1);
  handler->resumeRead();
  EXPECT_FALSE(handler->debugState().isPaused);

  // Now pause
  EXPECT_CALL(*mockSocket_, setReadCB(nullptr)).Times(1);
  handler->pauseRead();
  EXPECT_TRUE(handler->debugState().isPaused);
}

// Test: Pause Read is idempotent
TEST_F(TransportHandlerTest, PauseReadIdempotent) {
  auto [handler, pipeline] = createHandlerAndPipeline();

  // Initially paused, so pauseRead should not call setReadCB
  EXPECT_TRUE(handler->debugState().isPaused);
  handler->pauseRead();
  EXPECT_TRUE(handler->debugState().isPaused);

  // Resume first
  EXPECT_CALL(*mockSocket_, setReadCB(handler.get())).Times(1);
  handler->resumeRead();
  EXPECT_FALSE(handler->debugState().isPaused);

  // Now pause should call setReadCB(nullptr)
  EXPECT_CALL(*mockSocket_, setReadCB(nullptr)).Times(1);
  handler->pauseRead();
  EXPECT_TRUE(handler->debugState().isPaused);

  // Second pause should not call setReadCB again
  handler->pauseRead();
  EXPECT_TRUE(handler->debugState().isPaused);
}

// Test: Resume Read is idempotent
TEST_F(TransportHandlerTest, ResumeReadIdempotent) {
  auto [handler, pipeline] = createHandlerAndPipeline();

  // Initially paused
  EXPECT_TRUE(handler->debugState().isPaused);

  // First resume should set callback
  EXPECT_CALL(*mockSocket_, setReadCB(handler.get())).Times(1);
  handler->resumeRead();
  EXPECT_FALSE(handler->debugState().isPaused);

  // Second resume should not call setReadCB again (already resumed)
  handler->resumeRead();
  EXPECT_FALSE(handler->debugState().isPaused);
}

TEST_F(TransportHandlerTest, OnReadReadyResumesRead) {
  auto [handler, pipeline] = createHandlerAndPipeline();

  EXPECT_TRUE(handler->debugState().isPaused);

  EXPECT_CALL(*mockSocket_, setReadCB(handler.get())).Times(1);
  handler->onReadReady();

  EXPECT_FALSE(handler->debugState().isPaused);
}

TEST_F(
    TransportHandlerTest,
    ReadBackpressureThenPipelineReadReadyResumesSocketRead) {
  auto [handler, pipeline] = createHandlerAndPipeline();

  appHandler_.setOnReadCallback(
      [&](TypeErasedBox&&) { return Result::Backpressure; });

  {
    InSequence seq;
    EXPECT_CALL(*mockSocket_, setReadCB(handler.get())).Times(1);
    EXPECT_CALL(*mockSocket_, setReadCB(nullptr)).Times(1);
    EXPECT_CALL(*mockSocket_, setReadCB(handler.get())).Times(1);
  }

  handler->resumeRead();
  EXPECT_FALSE(handler->debugState().isPaused);

  auto data = buildTestData(32);
  handler->readBufferAvailable(std::move(data));
  EXPECT_TRUE(handler->debugState().isPaused);

  pipeline->onReadReady();
  EXPECT_FALSE(handler->debugState().isPaused);
}

// Test: onConnect resumes reading and fires connect event to pipeline
TEST_F(TransportHandlerTest, OnConnectResumesReadAndFiresConnect) {
  auto [handler, mockHandler, pipeline] =
      createHandlerAndPipelineWithExceptionHandler();

  // Initially paused
  EXPECT_TRUE(handler->debugState().isPaused);
  EXPECT_EQ(mockHandler->pipelineActivatedCount(), 0);

  // onConnect should resume reading and fire connect to the pipeline.
  // resetPipeline / dtor also call setReadCB(nullptr) on teardown.
  EXPECT_CALL(*mockSocket_, setReadCB(handler.get())).Times(1);
  EXPECT_CALL(*mockSocket_, setReadCB(nullptr)).Times(AnyNumber());
  handler->onConnect();

  EXPECT_FALSE(handler->debugState().isPaused);
  EXPECT_EQ(mockHandler->pipelineActivatedCount(), 1);
}

// Test: Write Backpressure When Write Pending
TEST_F(TransportHandlerTest, WriteBackpressureWhenWritePending) {
  auto [handler, pipeline] = createHandlerAndPipeline();
  handler->onConnect();

  folly::AsyncTransport::WriteCallback* capturedCallback = nullptr;

  // With the new behavior, both writes go through to the socket
  EXPECT_CALL(*mockSocket_, writeChain(_, _, _))
      .Times(2)
      .WillRepeatedly(SaveArg<0>(&capturedCallback));

  auto bytes1 = folly::IOBuf::copyBuffer("first write");
  Result result1 = handler->onWrite(
      channel_pipeline::test::inertEndpointContext(),
      apache::thrift::fast_thrift::channel_pipeline::TypeErasedBox(
          std::move(bytes1)));

  // First write returns Backpressure to signal pending write
  EXPECT_EQ(result1, Result::Backpressure);
  EXPECT_EQ(handler->debugState().pendingWrites, 1);
  EXPECT_TRUE(handler->debugState().isWriteSaturated);

  auto bytes2 = folly::IOBuf::copyBuffer("second write");
  Result result2 = handler->onWrite(
      channel_pipeline::test::inertEndpointContext(),
      apache::thrift::fast_thrift::channel_pipeline::TypeErasedBox(
          std::move(bytes2)));

  // Second write also goes through and returns Backpressure
  EXPECT_EQ(result2, Result::Backpressure);
  EXPECT_EQ(handler->debugState().pendingWrites, 2);
  EXPECT_TRUE(handler->debugState().isWriteSaturated);
  // Drain both pending writes so close doesn't hang in Closing.
  capturedCallback->writeSuccess();
  capturedCallback->writeSuccess();
  EXPECT_EQ(handler->debugState().pendingWrites, 0);
  EXPECT_FALSE(handler->debugState().isWriteSaturated);
}

// Test: a write that lands inline never saturated the socket, so nothing
// upstream was told to stop and the pipeline must not be woken.
TEST_F(TransportHandlerTest, SynchronousWriteDoesNotSignalWriteReady) {
  auto [handler, pipeline] = createHandlerAndPipeline();
  handler->onConnect();

  folly::AsyncTransport::WriteCallback* capturedCallback = nullptr;
  EXPECT_CALL(*mockSocket_, writeChain(_, _, _))
      .WillOnce(DoAll(SaveArg<0>(&capturedCallback), [&]() {
        if (capturedCallback) {
          capturedCallback->writeSuccess();
        }
      }));

  auto bytes = folly::IOBuf::copyBuffer("inline write");
  Result result = handler->onWrite(
      channel_pipeline::test::inertEndpointContext(),
      TypeErasedBox(std::move(bytes)));

  EXPECT_EQ(result, Result::Success);
  EXPECT_EQ(handler->debugState().pendingWrites, 0);
  EXPECT_FALSE(handler->debugState().isWriteSaturated);
  EXPECT_EQ(appHandler_.onWriteReadyCount(), 0);
}

TEST_F(TransportHandlerTest, WrappedSocketBufferingControlsBackpressure) {
  folly::AsyncSocket::UniquePtr socket(new BufferCallbackSocket(&evb_));
  auto* rawSocket = static_cast<BufferCallbackSocket*>(socket.get());
  auto [handler, pipeline] = createHandlerAndPipeline(rawSocket);
  handler->onConnect();
  ASSERT_EQ(rawSocket->bufferCallback(), handler.get());
  EXPECT_CALL(*mockSocket_, setReadCB(nullptr)).Times(0);

  folly::AsyncTransport::WriteCallback* writeCallback = nullptr;
  EXPECT_CALL(*mockSocket_, writeChain(_, _, _))
      .Times(3)
      .WillRepeatedly(SaveArg<0>(&writeCallback));

  EXPECT_EQ(
      pipeline->fireWrite(TypeErasedBox(buildTestData(16))), Result::Success);
  EXPECT_EQ(handler->debugState().pendingWrites, 1);
  EXPECT_FALSE(handler->debugState().isWriteSaturated);

  rawSocket->bufferCallback()->onEgressBuffered();
  rawSocket->bufferCallback()->onEgressBuffered();
  for (int i = 0; i < 2; ++i) {
    EXPECT_EQ(
        pipeline->fireWrite(TypeErasedBox(buildTestData(16))),
        Result::Backpressure);
  }
  EXPECT_FALSE(handler->debugState().isPaused);

  for (int i = 0; i < 3; ++i) {
    writeCallback->writeSuccess();
  }
  EXPECT_EQ(appHandler_.onWriteReadyCount(), 0);
  rawSocket->bufferCallback()->onEgressBufferCleared();
  rawSocket->bufferCallback()->onEgressBufferCleared();
  EXPECT_EQ(appHandler_.onWriteReadyCount(), 1);
  EXPECT_FALSE(handler->debugState().isWriteSaturated);

  Mock::VerifyAndClearExpectations(mockSocket_);
}

TEST_F(TransportHandlerTest, InlineBufferingSignalsPressureBeforeWriteReturns) {
  folly::AsyncSocket::UniquePtr socket(new BufferCallbackSocket(&evb_));
  auto* rawSocket = static_cast<BufferCallbackSocket*>(socket.get());
  auto [handler, pipeline] = createHandlerAndPipeline(rawSocket);
  handler->onConnect();
  ASSERT_NE(rawSocket->bufferCallback(), nullptr);

  folly::AsyncTransport::WriteCallback* writeCallback = nullptr;
  EXPECT_CALL(*mockSocket_, writeChain(_, _, _))
      .Times(2)
      .WillRepeatedly(DoAll(SaveArg<0>(&writeCallback), [&]() {
        rawSocket->bufferCallback()->onEgressBuffered();
      }));

  for (int expected = 1; expected <= 2; ++expected) {
    EXPECT_EQ(
        pipeline->fireWrite(TypeErasedBox(buildTestData(16))),
        Result::Backpressure);
    rawSocket->bufferCallback()->onEgressBufferCleared();
    EXPECT_EQ(appHandler_.onWriteReadyCount(), expected - 1);
    writeCallback->writeSuccess();
    EXPECT_EQ(appHandler_.onWriteReadyCount(), expected);
  }
}

TEST_F(TransportHandlerTest, BufferClearedAfterWriteErrorDoesNotSignalReady) {
  folly::AsyncSocket::UniquePtr socket(new BufferCallbackSocket(&evb_));
  auto* rawSocket = static_cast<BufferCallbackSocket*>(socket.get());
  auto [handler, pipeline] = createHandlerAndPipeline(rawSocket);
  handler->onConnect();
  ASSERT_NE(rawSocket->bufferCallback(), nullptr);

  folly::AsyncTransport::WriteCallback* writeCallback = nullptr;
  EXPECT_CALL(*mockSocket_, writeChain(_, _, _))
      .WillOnce(DoAll(SaveArg<0>(&writeCallback), [&]() {
        rawSocket->bufferCallback()->onEgressBuffered();
      }));
  EXPECT_EQ(
      pipeline->fireWrite(TypeErasedBox(buildTestData(16))),
      Result::Backpressure);

  writeCallback->writeErr(
      0,
      folly::AsyncSocketException(
          folly::AsyncSocketException::NETWORK_ERROR, "write failed"));
  rawSocket->bufferCallback()->onEgressBufferCleared();
  EXPECT_EQ(handler->state(), TransportHandler::State::Closed);
  EXPECT_EQ(appHandler_.onWriteReadyCount(), 0);

  handler->resetPipeline();
  EXPECT_EQ(rawSocket->bufferCallback(), nullptr);
}

// Test: a write the socket buffered signals Backpressure, and the wake-up
// arrives once it completes.
TEST_F(TransportHandlerTest, QueuedWriteSignalsWriteReadyOnceDrained) {
  auto [handler, pipeline] = createHandlerAndPipeline();
  handler->onConnect();

  folly::AsyncTransport::WriteCallback* capturedCallback = nullptr;
  EXPECT_CALL(*mockSocket_, writeChain(_, _, _))
      .WillOnce(SaveArg<0>(&capturedCallback));

  auto bytes = folly::IOBuf::copyBuffer("queued write");
  Result result = handler->onWrite(
      channel_pipeline::test::inertEndpointContext(),
      TypeErasedBox(std::move(bytes)));

  EXPECT_EQ(result, Result::Backpressure);
  EXPECT_EQ(handler->debugState().pendingWrites, 1);
  EXPECT_TRUE(handler->debugState().isWriteSaturated);
  EXPECT_EQ(appHandler_.onWriteReadyCount(), 0);

  capturedCallback->writeSuccess();

  EXPECT_EQ(handler->debugState().pendingWrites, 0);
  EXPECT_FALSE(handler->debugState().isWriteSaturated);
  EXPECT_EQ(appHandler_.onWriteReadyCount(), 1);
}

// Test: with several writes outstanding only the final completion wakes the
// pipeline, so an absorber drains its queue in one pass instead of making one
// message of progress per completion.
TEST_F(TransportHandlerTest, MultipleInFlightWritesSignalWriteReadyOnce) {
  auto [handler, pipeline] = createHandlerAndPipeline();
  handler->onConnect();

  folly::AsyncTransport::WriteCallback* capturedCallback = nullptr;
  EXPECT_CALL(*mockSocket_, writeChain(_, _, _))
      .Times(3)
      .WillRepeatedly(SaveArg<0>(&capturedCallback));

  for (int i = 0; i < 3; ++i) {
    auto bytes = folly::IOBuf::copyBuffer("queued write");
    EXPECT_EQ(
        handler->onWrite(
            channel_pipeline::test::inertEndpointContext(),
            TypeErasedBox(std::move(bytes))),
        Result::Backpressure);
  }
  EXPECT_EQ(handler->debugState().pendingWrites, 3);
  EXPECT_TRUE(handler->debugState().isWriteSaturated);

  capturedCallback->writeSuccess();
  EXPECT_EQ(handler->debugState().pendingWrites, 2);
  EXPECT_EQ(appHandler_.onWriteReadyCount(), 0);
  capturedCallback->writeSuccess();
  EXPECT_EQ(handler->debugState().pendingWrites, 1);
  EXPECT_EQ(appHandler_.onWriteReadyCount(), 0);
  capturedCallback->writeSuccess();
  EXPECT_EQ(handler->debugState().pendingWrites, 0);
  EXPECT_FALSE(handler->debugState().isWriteSaturated);
  EXPECT_EQ(appHandler_.onWriteReadyCount(), 1);
}

// Test: the saturation latch re-arms, so a later queued write gets its own
// wake-up rather than the signal being delivered only once per connection.
TEST_F(TransportHandlerTest, WriteReadySignalReArmsAfterDrain) {
  auto [handler, pipeline] = createHandlerAndPipeline();
  handler->onConnect();

  folly::AsyncTransport::WriteCallback* capturedCallback = nullptr;
  EXPECT_CALL(*mockSocket_, writeChain(_, _, _))
      .Times(2)
      .WillRepeatedly(SaveArg<0>(&capturedCallback));

  for (int expected = 1; expected <= 2; ++expected) {
    auto bytes = folly::IOBuf::copyBuffer("queued write");
    EXPECT_EQ(
        handler->onWrite(
            channel_pipeline::test::inertEndpointContext(),
            TypeErasedBox(std::move(bytes))),
        Result::Backpressure);
    EXPECT_EQ(handler->debugState().pendingWrites, 1);
    EXPECT_TRUE(handler->debugState().isWriteSaturated);
    capturedCallback->writeSuccess();
    EXPECT_EQ(handler->debugState().pendingWrites, 0);
    EXPECT_FALSE(handler->debugState().isWriteSaturated);
    EXPECT_EQ(appHandler_.onWriteReadyCount(), expected);
  }
}

// Test: Write Success Clears Pending State
TEST_F(TransportHandlerTest, WriteSuccessClearsPendingState) {
  auto [handler, pipeline] = createHandlerAndPipeline();
  handler->onConnect();

  folly::AsyncTransport::WriteCallback* capturedCallback = nullptr;

  EXPECT_CALL(*mockSocket_, writeChain(_, _, _))
      .Times(2)
      .WillRepeatedly(SaveArg<0>(&capturedCallback));

  auto bytes1 = folly::IOBuf::copyBuffer("first write");
  Result result1 = handler->onWrite(
      channel_pipeline::test::inertEndpointContext(),
      apache::thrift::fast_thrift::channel_pipeline::TypeErasedBox(
          std::move(bytes1)));

  EXPECT_EQ(result1, Result::Backpressure);
  EXPECT_EQ(handler->debugState().pendingWrites, 1);

  capturedCallback->writeSuccess();

  EXPECT_EQ(handler->debugState().pendingWrites, 0);
  EXPECT_EQ(appHandler_.onWriteReadyCount(), 1);

  auto bytes2 = folly::IOBuf::copyBuffer("second write");
  Result result2 = handler->onWrite(
      channel_pipeline::test::inertEndpointContext(),
      apache::thrift::fast_thrift::channel_pipeline::TypeErasedBox(
          std::move(bytes2)));

  EXPECT_EQ(result2, Result::Backpressure);
  // Drain the second pending write so close doesn't hang in Closing.
  capturedCallback->writeSuccess();
}

// Test: Write Error Clears Pending State
TEST_F(TransportHandlerTest, WriteErrorClearsPendingState) {
  auto [handler, pipeline] = createHandlerAndPipeline();
  handler->onConnect();

  folly::AsyncTransport::WriteCallback* capturedCallback = nullptr;

  EXPECT_CALL(*mockSocket_, writeChain(_, _, _))
      .WillOnce(SaveArg<0>(&capturedCallback));

  auto bytes = folly::IOBuf::copyBuffer("test write");
  Result result = handler->onWrite(
      channel_pipeline::test::inertEndpointContext(),
      TypeErasedBox(std::move(bytes)));

  EXPECT_EQ(result, Result::Backpressure);
  EXPECT_EQ(handler->debugState().pendingWrites, 1);

  folly::AsyncSocketException ex(
      folly::AsyncSocketException::NETWORK_ERROR, "write failed");
  capturedCallback->writeErr(0, ex);

  EXPECT_EQ(handler->debugState().pendingWrites, 0);
  EXPECT_EQ(handler->state(), TransportHandler::State::Closed);
}

// Test: Multiple Writes After Completion
TEST_F(TransportHandlerTest, MultipleWritesAfterCompletion) {
  auto [handler, pipeline] = createHandlerAndPipeline();
  handler->onConnect();

  folly::AsyncTransport::WriteCallback* capturedCallback = nullptr;

  EXPECT_CALL(*mockSocket_, writeChain(_, _, _))
      .Times(3)
      .WillRepeatedly(SaveArg<0>(&capturedCallback));

  for (int i = 0; i < 3; ++i) {
    auto bytes = folly::IOBuf::copyBuffer("write " + std::to_string(i));
    Result result = handler->onWrite(
        channel_pipeline::test::inertEndpointContext(),
        TypeErasedBox(std::move(bytes)));
    EXPECT_EQ(result, Result::Backpressure);
    EXPECT_EQ(handler->debugState().pendingWrites, 1);

    capturedCallback->writeSuccess();
    EXPECT_EQ(handler->debugState().pendingWrites, 0);
    EXPECT_EQ(appHandler_.onWriteReadyCount(), i + 1);
  }
}

// --- Read Tests ---

// Test: readBufferAvailable forwards raw bytes to pipeline
TEST_F(TransportHandlerTest, ReadBufferAvailableForwardsToPipeline) {
  auto [handler, pipeline] = createHandlerAndPipeline();

  EXPECT_EQ(appHandler_.readCount(), 0);

  // Build raw bytes (no frame parsing - just raw data)
  auto data = buildTestData(20);
  handler->readBufferAvailable(std::move(data));

  EXPECT_EQ(appHandler_.readCount(), 1);
}

// Test: Multiple readBufferAvailable calls
TEST_F(TransportHandlerTest, MultipleReadBufferAvailableCalls) {
  auto [handler, pipeline] = createHandlerAndPipeline();

  for (int i = 0; i < 5; ++i) {
    auto data = buildTestData(10);
    handler->readBufferAvailable(std::move(data));
  }

  EXPECT_EQ(appHandler_.readCount(), 5);
}

// Test: the socket read buffer is sourced from the pipeline's allocator, and
// a partially consumed buffer is reused rather than re-allocated.
TEST_F(TransportHandlerTest, ReadBufferComesFromPipelineAllocator) {
  auto socket = folly::AsyncTransport::UniquePtr(
      new NiceMock<folly::test::MockAsyncTransport>());
  ON_CALL(
      *static_cast<folly::test::MockAsyncTransport*>(socket.get()),
      getEventBase())
      .WillByDefault(Return(&evb_));

  constexpr size_t kMinBufferSize = 256;
  constexpr size_t kMaxBufferSize = 4096;
  auto handler = TransportHandler::create(
      std::move(socket), kMinBufferSize, kMaxBufferSize);

  TestAllocator allocator;
  auto pipeline =
      StaticPipelineBuilder<TransportHandler, MockAppHandler, TestAllocator>()
          .setEventBase(&evb_)
          .setHead(handler.get())
          .setTail(&appHandler_)
          .setAllocator(&allocator)
          .build();
  handler->setPipeline(pipeline.get());

  void* buf = nullptr;
  size_t len = 0;
  handler->getReadBuffer(&buf, &len);

  EXPECT_EQ(allocator.allocationCount(), 1);
  EXPECT_EQ(allocator.totalBytesAllocated(), kMaxBufferSize);
  EXPECT_NE(buf, nullptr);
  EXPECT_GE(len, kMaxBufferSize);

  // Ample tailroom remains, so a repeated request reuses the same buffer.
  void* second = nullptr;
  handler->getReadBuffer(&second, &len);
  EXPECT_EQ(allocator.allocationCount(), 1);
  EXPECT_EQ(second, buf);

  // readDataAvailable moves the whole chain to the pipeline, so the next
  // request starts over with a fresh buffer from the allocator.
  handler->readDataAvailable(16);
  EXPECT_EQ(appHandler_.readCount(), 1);

  handler->getReadBuffer(&buf, &len);
  EXPECT_EQ(allocator.allocationCount(), 2);
}

// --- Lifecycle Tests ---

// --- Close Behavior Tests ---

// Test: readEOF closes socket and reaches Closed.
TEST_F(TransportHandlerTest, ReadEOFCloseBehavior) {
  auto [handler, pipeline] = createHandlerAndPipeline();
  handler->onConnect();

  EXPECT_TRUE(handler->debugState().hasPipeline);

  handler->readEOF();

  EXPECT_EQ(handler->state(), TransportHandler::State::Closed);
}

TEST_F(TransportHandlerTest, ClosedCallbackFiresOnceAtTerminalTransition) {
  auto [handler, pipeline] = createHandlerAndPipeline();
  size_t closes = 0;
  handler->setOnClosed([&]() noexcept { ++closes; });
  handler->onConnect();

  handler->readEOF();
  handler->readEOF();
  handler->close(folly::exception_wrapper{});

  EXPECT_EQ(closes, 1);
}

TEST_F(TransportHandlerTest, ResetPipelineSuppressesClosedCallback) {
  auto [handler, pipeline] = createHandlerAndPipeline();
  size_t closes = 0;
  handler->setOnClosed([&]() noexcept { ++closes; });

  handler->resetPipeline();

  EXPECT_EQ(closes, 0);
}

// Test: readErr closes socket and reaches Closed.
TEST_F(TransportHandlerTest, ReadErrCloseBehavior) {
  auto [handler, pipeline] = createHandlerAndPipeline();
  handler->onConnect();

  EXPECT_TRUE(handler->debugState().hasPipeline);

  folly::AsyncSocketException ex(
      folly::AsyncSocketException::NETWORK_ERROR, "read error");
  handler->readErr(ex);

  EXPECT_EQ(handler->state(), TransportHandler::State::Closed);
}

// Test: writeErr closes socket and reaches Closed.
TEST_F(TransportHandlerTest, WriteErrCloseBehavior) {
  auto [handler, pipeline] = createHandlerAndPipeline();
  handler->onConnect();

  folly::AsyncTransport::WriteCallback* capturedCallback = nullptr;

  EXPECT_CALL(*mockSocket_, writeChain(_, _, _))
      .WillOnce(SaveArg<0>(&capturedCallback));

  auto bytes = folly::IOBuf::copyBuffer("test write");
  Result result = handler->onWrite(
      channel_pipeline::test::inertEndpointContext(),
      TypeErasedBox(std::move(bytes)));
  EXPECT_EQ(result, Result::Backpressure);
  EXPECT_TRUE(handler->debugState().hasPipeline);

  folly::AsyncSocketException ex(
      folly::AsyncSocketException::NETWORK_ERROR, "write failed");
  capturedCallback->writeErr(0, ex);

  EXPECT_EQ(handler->state(), TransportHandler::State::Closed);
}

// Test: onClose closes socket and reaches Closed.
TEST_F(TransportHandlerTest, OnCloseCloseBehavior) {
  auto [handler, pipeline] = createHandlerAndPipeline();
  handler->onConnect();

  EXPECT_TRUE(handler->debugState().hasPipeline);

  handler->close(folly::exception_wrapper{});

  EXPECT_EQ(handler->state(), TransportHandler::State::Closed);
}

// --- setPipeline Death Tests ---

// Test: setPipeline without reset triggers FATAL
TEST_F(TransportHandlerTest, SetPipelineWithoutResetDeath) {
#ifdef NDEBUG
  return;
#endif
  auto [handler, pipeline] = createHandlerAndPipeline();

  // Create a second pipeline
  MockAppHandler appHandler2;
  auto pipeline2 = StaticPipelineBuilder<
                       TransportHandler,
                       MockAppHandler,
                       SimpleBufferAllocator>()
                       .setEventBase(&evb_)
                       .setHead(handler.get())
                       .setTail(&appHandler2)
                       .setAllocator(&allocator_)
                       .build();

  EXPECT_DEBUG_DEATH(handler->setPipeline(pipeline2.get()), "");
}

// Test: write with null bytes triggers DCHECK
TEST_F(TransportHandlerTest, WriteWithNullBytesDeath) {
#ifdef NDEBUG
  return;
#endif
  auto [handler, pipeline] = createHandlerAndPipeline();

  EXPECT_DEBUG_DEATH(
      (void)handler->onWrite(
          channel_pipeline::test::inertEndpointContext(),
          TypeErasedBox(BytesPtr{})),
      "bytes");
}

TEST_F(TransportHandlerTest, IsBufferMovableFollowsAMovableParser) {
  auto [handler, pipeline] = createHandlerAndPipeline();

  EXPECT_THAT(handler->isBufferMovable(), IsTrue());
}

// createWithParser, not create: NonMovableParser has no min/max read size for
// create() to take its defaults from.
TEST_F(TransportHandlerTest, IsBufferMovableFollowsANonMovableParser) {
  auto socket = folly::AsyncTransport::UniquePtr(
      new NiceMock<folly::test::MockAsyncTransport>());
  ON_CALL(
      *static_cast<folly::test::MockAsyncTransport*>(socket.get()),
      getEventBase())
      .WillByDefault(Return(&evb_));

  auto handler = NonMovableTransportHandler::createWithParser(
      std::move(socket), NonMovableParser());

  EXPECT_THAT(handler->isBufferMovable(), IsFalse());
}

#if FOLLY_HAS_LIBURING
TEST_F(TransportHandlerTest, RejectsANonMovableParserOnNativeIoUring) {
  if (!folly::IoUringBackend::isAvailable()) {
    GTEST_SKIP();
  }

  folly::EventBase evb(
      folly::EventBase::Options{}.setBackendFactory(
          []() -> std::unique_ptr<folly::EventBaseBackendBase> {
            folly::IoUringBackend::Options options;
            options.setCapacity(64).setMaxSubmit(32).setInitialProvidedBuffers(
                2048, 256);
            return std::make_unique<folly::IoUringBackend>(std::move(options));
          }));
  std::array<int, 2> fds{};
  const int socketPairResult =
      ::socketpair(AF_UNIX, SOCK_STREAM, 0, fds.data());
  EXPECT_THAT(socketPairResult, Eq(0));
  if (socketPairResult != 0) {
    return;
  }

  folly::AsyncSocket::UniquePtr peer(
      new folly::AsyncSocket(&evb, folly::NetworkSocket{fds[1]}));
  folly::AsyncTransport::UniquePtr socket(
      new folly::AsyncSocket(&evb, folly::NetworkSocket{fds[0]}));
  NonMovableTransportHandler::Ptr handler =
      NonMovableTransportHandler::createWithParser(
          std::move(socket), NonMovableParser());
  auto pipeline =
      StaticPipelineBuilder<
          NonMovableTransportHandler,
          MockAppHandler,
          SimpleBufferAllocator>()
          .setEventBase(&evb)
          .setHead(handler.get())
          .setTail(&appHandler_)
          .setAllocator(&allocator_)
          .addNextDuplex<TransportExceptionObserver>(exception_handler_tag)
          .build();
  auto* mockHandlerPtr = &pipeline->handler(exception_handler_tag);
  handler->setPipeline(pipeline.get());

  handler->onConnect();

  EXPECT_THAT(handler->state(), Eq(NonMovableTransportHandler::State::Closed));
  EXPECT_THAT(mockHandlerPtr->exceptionCount(), Eq(1));
}
#endif

TEST_F(TransportHandlerTest, ReadBufferAvailableOnANonMovableParserDeath) {
#ifdef NDEBUG
  return;
#endif
  auto socket = folly::AsyncTransport::UniquePtr(
      new NiceMock<folly::test::MockAsyncTransport>());
  ON_CALL(
      *static_cast<folly::test::MockAsyncTransport*>(socket.get()),
      getEventBase())
      .WillByDefault(Return(&evb_));

  auto handler = NonMovableTransportHandler::createWithParser(
      std::move(socket), NonMovableParser());

  EXPECT_DEBUG_DEATH(
      handler->readBufferAvailable(buildTestData(16)), "without consumeBuffer");
}

// Test: setPipeline is a pure setter and does not fire onConnect, even when
// the socket is already good. Activation is the caller's responsibility:
// they call onConnect() once the socket is connected (server: right after
// accept; client: from their AsyncSocket connect callback).
TEST_F(TransportHandlerTest, SetPipelineIsInert) {
  auto socket = folly::AsyncTransport::UniquePtr(
      new NiceMock<folly::test::MockAsyncTransport>());
  auto* mockSocket =
      static_cast<folly::test::MockAsyncTransport*>(socket.get());

  // Even with a good socket, setPipeline must not touch readCB.
  ON_CALL(*mockSocket, good()).WillByDefault(Return(true));
  ON_CALL(*mockSocket, getEventBase()).WillByDefault(Return(&evb_));
  EXPECT_CALL(*mockSocket, setReadCB(_)).Times(0);

  auto handler = TransportHandler::create(std::move(socket), 256, 4096);

  auto pipeline =
      StaticPipelineBuilder<
          TransportHandler,
          MockAppHandler,
          SimpleBufferAllocator>()
          .setEventBase(&evb_)
          .setHead(handler.get())
          .setTail(&appHandler_)
          .setAllocator(&allocator_)
          .addNextDuplex<TransportExceptionObserver>(exception_handler_tag)
          .build();
  auto* mockHandlerPtr = &pipeline->handler(exception_handler_tag);
  handler->setPipeline(pipeline.get());

  EXPECT_EQ(mockHandlerPtr->pipelineActivatedCount(), 0);
}

// Test: onConnect() fires activation when the socket is already connected.
TEST_F(TransportHandlerTest, OnConnectActivatesPipelineWhenSocketGood) {
  auto socket = folly::AsyncTransport::UniquePtr(
      new NiceMock<folly::test::MockAsyncTransport>());
  auto* mockSocket =
      static_cast<folly::test::MockAsyncTransport*>(socket.get());

  ON_CALL(*mockSocket, good()).WillByDefault(Return(true));
  ON_CALL(*mockSocket, getEventBase()).WillByDefault(Return(&evb_));
  // Expect setReadCB called twice: once from start->onConnect (resumeRead),
  // once from destructor (closeImmediately -> pauseRead since not paused).
  EXPECT_CALL(*mockSocket, setReadCB(_)).Times(2);
  EXPECT_CALL(*mockSocket, closeNow()).Times(1);

  auto handler = TransportHandler::create(std::move(socket), 256, 4096);

  auto pipeline =
      StaticPipelineBuilder<
          TransportHandler,
          MockAppHandler,
          SimpleBufferAllocator>()
          .setEventBase(&evb_)
          .setHead(handler.get())
          .setTail(&appHandler_)
          .setAllocator(&allocator_)
          .addNextDuplex<TransportExceptionObserver>(exception_handler_tag)
          .build();
  auto* mockHandlerPtr = &pipeline->handler(exception_handler_tag);
  handler->setPipeline(pipeline.get());
  EXPECT_EQ(mockHandlerPtr->pipelineActivatedCount(), 0);

  handler->onConnect();
  EXPECT_EQ(mockHandlerPtr->pipelineActivatedCount(), 1);
}

// --- Exception Firing Tests ---

// Test: readEOF fires exception to pipeline
TEST_F(TransportHandlerTest, ReadEOFFiresExceptionToPipeline) {
  auto [handler, mockHandler, pipeline] =
      createHandlerAndPipelineWithExceptionHandler();
  handler->onConnect();

  bool exceptionReceived = false;
  mockHandler->setOnException([&](const folly::exception_wrapper& e) {
    exceptionReceived = true;
    EXPECT_TRUE(
        e.is_compatible_with<apache::thrift::transport::TTransportException>());
  });

  handler->readEOF();

  EXPECT_TRUE(exceptionReceived);
  EXPECT_EQ(mockHandler->exceptionCount(), 1);
}

// Test: readErr fires exception to pipeline
TEST_F(TransportHandlerTest, ReadErrFiresExceptionToPipeline) {
  auto [handler, mockHandler, pipeline] =
      createHandlerAndPipelineWithExceptionHandler();
  handler->onConnect();

  bool exceptionReceived = false;
  mockHandler->setOnException([&](const folly::exception_wrapper& e) {
    exceptionReceived = true;
    EXPECT_TRUE(
        e.is_compatible_with<apache::thrift::transport::TTransportException>());
  });

  folly::AsyncSocketException ex(
      folly::AsyncSocketException::NETWORK_ERROR, "read error");
  handler->readErr(ex);

  EXPECT_TRUE(exceptionReceived);
  EXPECT_EQ(mockHandler->exceptionCount(), 1);
}

// Test: writeErr fires exception to pipeline
TEST_F(TransportHandlerTest, WriteErrFiresExceptionToPipeline) {
  auto [handler, mockHandler, pipeline] =
      createHandlerAndPipelineWithExceptionHandler();
  handler->onConnect();

  bool exceptionReceived = false;
  mockHandler->setOnException([&](const folly::exception_wrapper& e) {
    exceptionReceived = true;
    EXPECT_TRUE(e.is_compatible_with<folly::AsyncSocketException>());
  });

  folly::AsyncTransport::WriteCallback* capturedCallback = nullptr;

  EXPECT_CALL(*mockSocket_, writeChain(_, _, _))
      .WillOnce(SaveArg<0>(&capturedCallback));

  auto bytes = folly::IOBuf::copyBuffer("test write");
  Result result = handler->onWrite(
      channel_pipeline::test::inertEndpointContext(),
      TypeErasedBox(std::move(bytes)));
  EXPECT_EQ(result, Result::Backpressure);

  folly::AsyncSocketException ex(
      folly::AsyncSocketException::NETWORK_ERROR, "write failed");
  capturedCallback->writeErr(0, ex);

  EXPECT_TRUE(exceptionReceived);
  EXPECT_EQ(mockHandler->exceptionCount(), 1);
}

// Test: onClose with exception fires exception to pipeline
TEST_F(TransportHandlerTest, OnCloseWithExceptionFiresExceptionToPipeline) {
  auto [handler, mockHandler, pipeline] =
      createHandlerAndPipelineWithExceptionHandler();
  handler->onConnect();

  bool exceptionReceived = false;
  mockHandler->setOnException([&](const folly::exception_wrapper& e) {
    exceptionReceived = true;
    EXPECT_TRUE(e.is_compatible_with<std::runtime_error>());
  });

  handler->close(
      folly::make_exception_wrapper<std::runtime_error>("connection closed"));

  EXPECT_TRUE(exceptionReceived);
  EXPECT_EQ(mockHandler->exceptionCount(), 1);
}

// Test: onClose without exception does not fire exception to pipeline
TEST_F(TransportHandlerTest, OnCloseWithoutExceptionDoesNotFireException) {
  auto [handler, mockHandler, pipeline] =
      createHandlerAndPipelineWithExceptionHandler();
  handler->onConnect();

  handler->close(folly::exception_wrapper{});

  EXPECT_EQ(mockHandler->exceptionCount(), 0);
}

// --- Disconnect Event Tests ---

// Test: close fires disconnect to all handlers
TEST_F(TransportHandlerTest, CloseFiresDisconnectToPipeline) {
  auto [handler, mockHandler, pipeline] =
      createHandlerAndPipelineWithExceptionHandler();
  handler->onConnect();

  EXPECT_EQ(mockHandler->pipelineDeactivatedCount(), 0);

  handler->close(folly::exception_wrapper{});

  EXPECT_EQ(mockHandler->pipelineDeactivatedCount(), 1);
}

// Test: readEOF fires disconnect to pipeline
TEST_F(TransportHandlerTest, ReadEOFFiresDisconnectToPipeline) {
  auto [handler, mockHandler, pipeline] =
      createHandlerAndPipelineWithExceptionHandler();
  handler->onConnect();

  handler->readEOF();

  EXPECT_EQ(mockHandler->pipelineDeactivatedCount(), 1);
}

// Test: readErr fires disconnect to pipeline
TEST_F(TransportHandlerTest, ReadErrFiresDisconnectToPipeline) {
  auto [handler, mockHandler, pipeline] =
      createHandlerAndPipelineWithExceptionHandler();
  handler->onConnect();

  folly::AsyncSocketException ex(
      folly::AsyncSocketException::NETWORK_ERROR, "read error");
  handler->readErr(ex);

  EXPECT_EQ(mockHandler->pipelineDeactivatedCount(), 1);
}

// Test: writeErr fires disconnect to pipeline
TEST_F(TransportHandlerTest, WriteErrFiresDisconnectToPipeline) {
  auto [handler, mockHandler, pipeline] =
      createHandlerAndPipelineWithExceptionHandler();
  handler->onConnect();

  folly::AsyncTransport::WriteCallback* capturedCallback = nullptr;

  EXPECT_CALL(*mockSocket_, writeChain(_, _, _))
      .WillOnce(SaveArg<0>(&capturedCallback));

  auto bytes = folly::IOBuf::copyBuffer("test write");
  Result result = handler->onWrite(
      channel_pipeline::test::inertEndpointContext(),
      TypeErasedBox(std::move(bytes)));
  EXPECT_EQ(result, Result::Backpressure);

  folly::AsyncSocketException ex(
      folly::AsyncSocketException::NETWORK_ERROR, "write failed");
  capturedCallback->writeErr(0, ex);

  EXPECT_EQ(mockHandler->pipelineDeactivatedCount(), 1);
}

// Test: disconnect is only fired once even with multiple close calls
TEST_F(TransportHandlerTest, DisconnectFiredOnlyOnceOnMultipleClose) {
  auto [handler, mockHandler, pipeline] =
      createHandlerAndPipelineWithExceptionHandler();
  handler->onConnect();

  // Close multiple times
  handler->close(folly::exception_wrapper{});
  handler->close(folly::exception_wrapper{});
  handler->close(folly::exception_wrapper{});

  // Disconnect should only be called once (close is idempotent)
  EXPECT_EQ(mockHandler->pipelineDeactivatedCount(), 1);
}

// --- Destructor Tests ---

// Test: Destructor is idempotent when caller already called close() — no
// second deactivate.
TEST_F(TransportHandlerTest, DestructorNoOpWhenAlreadyClosed) {
  auto [handler, mockHandler, pipeline] =
      createHandlerAndPipelineWithExceptionHandler();
  handler->onConnect();

  handler->close(folly::exception_wrapper{});
  EXPECT_EQ(handler->state(), TransportHandler::State::Closed);
  EXPECT_EQ(mockHandler->pipelineDeactivatedCount(), 1);

  handler->resetPipeline();
  pipeline.reset();
  handler.reset();
}

// --- State Machine Tests ---

TEST_F(TransportHandlerTest, StateTransitions) {
  auto socket = folly::AsyncTransport::UniquePtr(
      new NiceMock<folly::test::MockAsyncTransport>());
  auto* mockSocket =
      static_cast<folly::test::MockAsyncTransport*>(socket.get());
  ON_CALL(*mockSocket, good()).WillByDefault(Return(true));
  ON_CALL(*mockSocket, getEventBase()).WillByDefault(Return(&evb_));

  auto handler = TransportHandler::create(std::move(socket), 256, 4096);
  EXPECT_EQ(handler->state(), TransportHandler::State::Created);
  EXPECT_EQ(handler->debugState().state, TransportHandler::State::Created);
  EXPECT_TRUE(handler->debugState().isPaused);
  EXPECT_FALSE(handler->debugState().isWriteSaturated);
  EXPECT_EQ(handler->debugState().pendingWrites, 0);
  EXPECT_FALSE(handler->debugState().hasPipeline);
  EXPECT_FALSE(handler->debugState().isDraining);

  auto pipeline = StaticPipelineBuilder<
                      TransportHandler,
                      MockAppHandler,
                      SimpleBufferAllocator>()
                      .setEventBase(&evb_)
                      .setHead(handler.get())
                      .setTail(&appHandler_)
                      .setAllocator(&allocator_)
                      .build();

  handler->setPipeline(pipeline.get());
  EXPECT_EQ(handler->state(), TransportHandler::State::Ready);
  EXPECT_EQ(handler->debugState().state, TransportHandler::State::Ready);
  EXPECT_TRUE(handler->debugState().hasPipeline);

  handler->onConnect();
  EXPECT_EQ(handler->state(), TransportHandler::State::Open);
  EXPECT_EQ(handler->debugState().state, TransportHandler::State::Open);
  EXPECT_FALSE(handler->debugState().isPaused);

  // No pending writes -> Open transitions straight to Closed (no drain).
  handler->close(folly::exception_wrapper{});
  EXPECT_EQ(handler->state(), TransportHandler::State::Closed);
  EXPECT_EQ(handler->debugState().state, TransportHandler::State::Closed);
  EXPECT_TRUE(handler->debugState().isPaused);
  EXPECT_FALSE(handler->debugState().isDraining);

  // resetPipeline drops the pipeline pointer; state stays Closed (terminal).
  handler->resetPipeline();
  EXPECT_EQ(handler->state(), TransportHandler::State::Closed);
  EXPECT_FALSE(handler->debugState().hasPipeline);

  pipeline.reset();
}

TEST_F(TransportHandlerTest, OnWriteAcceptedInReadyRejectedInClosed) {
  auto [handler, pipeline] = createHandlerAndPipeline();

  // Ready (pipeline attached, not connected yet) — AsyncSocket buffers
  // pre-connect writes, so we must accept them and hand off to the socket.
  // The mock's default writeChain is a no-op, so writePending_ stays at 1
  // and we return Backpressure.
  folly::AsyncTransport::WriteCallback* readyCb = nullptr;
  EXPECT_CALL(*mockSocket_, writeChain(_, _, _)).WillOnce(SaveArg<0>(&readyCb));
  EXPECT_EQ(
      handler->onWrite(
          channel_pipeline::test::inertEndpointContext(),
          TypeErasedBox(folly::IOBuf::copyBuffer("x"))),
      Result::Backpressure);
  readyCb->writeSuccess();

  handler->onConnect();
  folly::AsyncTransport::WriteCallback* openCb = nullptr;
  EXPECT_CALL(*mockSocket_, writeChain(_, _, _)).WillOnce(SaveArg<0>(&openCb));
  EXPECT_EQ(
      handler->onWrite(
          channel_pipeline::test::inertEndpointContext(),
          TypeErasedBox(folly::IOBuf::copyBuffer("x"))),
      Result::Backpressure);
  // Drain so socketDrainer doesn't pin us alive after close.
  openCb->writeSuccess();

  handler->close(folly::exception_wrapper{});
  // state == Closed (no pending writes -> direct transition)
  EXPECT_EQ(
      handler->onWrite(
          channel_pipeline::test::inertEndpointContext(),
          TypeErasedBox(folly::IOBuf::copyBuffer("x"))),
      Result::Error);

  handler->resetPipeline();
  pipeline.reset();
}

TEST_F(TransportHandlerTest, OnConnectFromCreatedDeath) {
#ifdef NDEBUG
  return;
#endif
  auto socket = folly::AsyncTransport::UniquePtr(
      new NiceMock<folly::test::MockAsyncTransport>());
  auto* mockSocket =
      static_cast<folly::test::MockAsyncTransport*>(socket.get());
  ON_CALL(*mockSocket, good()).WillByDefault(Return(true));
  ON_CALL(*mockSocket, getEventBase()).WillByDefault(Return(&evb_));

  auto handler = TransportHandler::create(std::move(socket), 256, 4096);
  EXPECT_DEBUG_DEATH(handler->onConnect(), "");
}

TEST_F(TransportHandlerTest, OnConnectTwiceDeath) {
#ifdef NDEBUG
  return;
#endif
  auto [handler, pipeline] = createHandlerAndPipeline();
  handler->onConnect();
  EXPECT_DEBUG_DEATH(handler->onConnect(), "");
}

TEST_F(TransportHandlerTest, ResetPipelineDeathFromOpen) {
#ifdef NDEBUG
  return;
#endif
  auto [handler, pipeline] = createHandlerAndPipeline();
  handler->onConnect();
  EXPECT_DEBUG_DEATH(handler->resetPipeline(), "");
}

// --- Drain Tests ---

// Multi-WR queued; peer disconnect cascades writeErrs through AsyncSocket's
// failAllWrites loop. The drainGuard inside SocketDrainer keeps the handler
// alive across the cascade; first writeErr flips Open -> Closing, the last
// drains writePending_ to 0 and finalizes to Closed.
TEST_F(TransportHandlerTest, MultiWriteErrCascadeReachesClosed) {
  auto socket = folly::AsyncTransport::UniquePtr(
      new NiceMock<folly::test::MockAsyncTransport>());
  auto* mockSocket =
      static_cast<folly::test::MockAsyncTransport*>(socket.get());
  ON_CALL(*mockSocket, good()).WillByDefault(Return(true));
  ON_CALL(*mockSocket, getEventBase()).WillByDefault(Return(&evb_));

  auto handler = TransportHandler::create(std::move(socket), 256, 4096);
  auto pipeline = StaticPipelineBuilder<
                      TransportHandler,
                      MockAppHandler,
                      SimpleBufferAllocator>()
                      .setEventBase(&evb_)
                      .setHead(handler.get())
                      .setTail(&appHandler_)
                      .setAllocator(&allocator_)
                      .build();
  handler->setPipeline(pipeline.get());
  handler->onConnect();

  std::vector<folly::AsyncTransport::WriteCallback*> capturedCallbacks;
  EXPECT_CALL(*mockSocket, writeChain(_, _, _))
      .Times(3)
      .WillRepeatedly(
          [&](folly::AsyncTransport::WriteCallback* cb,
              std::shared_ptr<folly::IOBuf>,
              folly::WriteFlags) { capturedCallbacks.push_back(cb); });

  for (int i = 0; i < 3; ++i) {
    (void)handler->onWrite(
        channel_pipeline::test::inertEndpointContext(),
        TypeErasedBox(folly::IOBuf::copyBuffer("x")));
  }
  ASSERT_EQ(capturedCallbacks.size(), 3);
  EXPECT_EQ(handler->debugState().pendingWrites, 3);

  folly::AsyncSocketException ex(
      folly::AsyncSocketException::NETWORK_ERROR, "broken pipe");

  capturedCallbacks[0]->writeErr(0, ex);
  EXPECT_EQ(handler->state(), TransportHandler::State::Closing);
  EXPECT_EQ(handler->debugState().pendingWrites, 2);
  EXPECT_TRUE(handler->debugState().isDraining);

  capturedCallbacks[1]->writeErr(0, ex);
  EXPECT_EQ(handler->state(), TransportHandler::State::Closing);
  EXPECT_EQ(handler->debugState().pendingWrites, 1);
  EXPECT_TRUE(handler->debugState().isDraining);

  capturedCallbacks[2]->writeErr(0, ex);
  EXPECT_EQ(handler->state(), TransportHandler::State::Closed);
  EXPECT_EQ(handler->debugState().pendingWrites, 0);
  EXPECT_FALSE(handler->debugState().isDraining);

  handler->resetPipeline();
  pipeline.reset();
}

TEST_F(TransportHandlerTest, DrainGuardReleasedByWriteSuccessAfterClose) {
  auto [handler, pipeline] = createHandlerAndPipeline();
  handler->onConnect();

  folly::AsyncTransport::WriteCallback* cb = nullptr;
  EXPECT_CALL(*mockSocket_, writeChain(_, _, _)).WillOnce(SaveArg<0>(&cb));
  EXPECT_EQ(
      handler->onWrite(
          channel_pipeline::test::inertEndpointContext(),
          TypeErasedBox(folly::IOBuf::copyBuffer("x"))),
      Result::Backpressure);
  EXPECT_EQ(handler->debugState().pendingWrites, 1);

  handler->close(folly::exception_wrapper{});
  // writePending_ > 0 → state Closing, drain guard held.
  EXPECT_EQ(handler->state(), TransportHandler::State::Closing);
  EXPECT_TRUE(handler->debugState().isDraining);

  // Pending write completes naturally; drainer transitions Closing -> Closed.
  cb->writeSuccess();
  EXPECT_EQ(handler->state(), TransportHandler::State::Closed);
  EXPECT_EQ(handler->debugState().pendingWrites, 0);
  EXPECT_FALSE(handler->debugState().isDraining);

  handler->resetPipeline();
  pipeline.reset();
}

TEST_F(TransportHandlerTest, NoDrainGuardWhenNoPendingWrites) {
  auto [handler, pipeline] = createHandlerAndPipeline();
  handler->onConnect();

  handler->close(folly::exception_wrapper{});
  EXPECT_EQ(handler->state(), TransportHandler::State::Closed);
  EXPECT_FALSE(handler->debugState().isDraining);

  handler->resetPipeline();
  pipeline.reset();
}

TEST_F(TransportHandlerTest, DrainTimeoutForcesClose) {
  // Use a short drain timeout so the test runs quickly.
  auto socket = folly::AsyncTransport::UniquePtr(
      new NiceMock<folly::test::MockAsyncTransport>());
  auto* mockSocket =
      static_cast<folly::test::MockAsyncTransport*>(socket.get());
  ON_CALL(*mockSocket, good()).WillByDefault(Return(true));
  ON_CALL(*mockSocket, getEventBase()).WillByDefault(Return(&evb_));

  auto handler = TransportHandler::create(
      std::move(socket), 256, 4096, std::chrono::milliseconds(20));

  auto pipeline = StaticPipelineBuilder<
                      TransportHandler,
                      MockAppHandler,
                      SimpleBufferAllocator>()
                      .setEventBase(&evb_)
                      .setHead(handler.get())
                      .setTail(&appHandler_)
                      .setAllocator(&allocator_)
                      .build();
  handler->setPipeline(pipeline.get());
  handler->onConnect();

  folly::AsyncTransport::WriteCallback* cb = nullptr;
  EXPECT_CALL(*mockSocket, writeChain(_, _, _)).WillOnce(SaveArg<0>(&cb));
  (void)handler->onWrite(
      channel_pipeline::test::inertEndpointContext(),
      TypeErasedBox(folly::IOBuf::copyBuffer("x")));

  handler->close(folly::exception_wrapper{});
  EXPECT_EQ(handler->state(), TransportHandler::State::Closing);
  EXPECT_EQ(handler->debugState().pendingWrites, 1);
  EXPECT_TRUE(handler->debugState().isDraining);

  // closeNow() in onDrainTimeout simulates AsyncSocket's force-cascade. We
  // simulate that by having the mock's closeNow invoke writeErr on the
  // captured callback (mimicking failAllWrites), then break out of the loop.
  bool forceCloseFired = false;
  EXPECT_CALL(*mockSocket, closeNow()).WillOnce([&]() {
    forceCloseFired = true;
    folly::AsyncSocketException ex(
        folly::AsyncSocketException::NETWORK_ERROR, "force-closed");
    cb->writeErr(0, ex);
    evb_.terminateLoopSoon();
  });

  // Run the EventBase until the drain timer fires and the mock terminates it.
  evb_.loop();

  EXPECT_TRUE(forceCloseFired);
  EXPECT_EQ(handler->state(), TransportHandler::State::Closed);
  EXPECT_EQ(handler->debugState().pendingWrites, 0);
  EXPECT_FALSE(handler->debugState().isDraining);

  handler->resetPipeline();
  pipeline.reset();
}

// =========================================================================
// Templated event-factory path: verify writeSuccess / writeErr invoke a
// non-NoOp factory with the expected (status, bytes) arguments. The default
// NoOp path is already covered by every other test in this file via the
// `TransportHandler` alias.
// =========================================================================

namespace {

struct CapturingFactoryState {
  static inline std::vector<std::pair<WriteCompletionStatus, size_t>> calls;
};

struct TestWriteCompleteEvent : channel_pipeline::EventTag<> {};

struct CapturingWriteCompleteEventFactory {
  using TransportWriteCompleteEventType = TestWriteCompleteEvent;
  using PublishedEvents = channel_pipeline::Events<TestWriteCompleteEvent>;

  static TestWriteCompleteEvent make(
      WriteCompletionStatus status, size_t bytes) noexcept {
    CapturingFactoryState::calls.emplace_back(status, bytes);
    return {};
  }
};

static_assert(
    WriteCompleteEventFactory<CapturingWriteCompleteEventFactory>,
    "test factory must satisfy WriteCompleteEventFactory");

} // namespace

class TransportHandlerEventTest : public ::testing::Test {
 protected:
  using HandlerT = TransportHandlerT<
      CapturingWriteCompleteEventFactory,
      test::PassthroughParser>;

  void SetUp() override {
    CapturingFactoryState::calls.clear();
    appHandler_.reset();
  }

  std::pair<HandlerT::Ptr, PipelineOwner> createHandlerAndPipeline() {
    auto socket = folly::AsyncTransport::UniquePtr(
        new NiceMock<folly::test::MockAsyncTransport>());
    mockSocket_ = static_cast<folly::test::MockAsyncTransport*>(socket.get());
    ON_CALL(*mockSocket_, good()).WillByDefault(Return(true));
    ON_CALL(*mockSocket_, getEventBase()).WillByDefault(Return(&evb_));

    auto handler = HandlerT::create(std::move(socket), 256, 4096);
    auto pipeline =
        StaticPipelineBuilder<HandlerT, MockAppHandler, SimpleBufferAllocator>()
            .setEventBase(&evb_)
            .setHead(handler.get())
            .setTail(&appHandler_)
            .setAllocator(&allocator_)
            .build();
    handler->setPipeline(pipeline.get());
    return {std::move(handler), PipelineOwner(std::move(pipeline))};
  }

  folly::EventBase evb_;
  folly::test::MockAsyncTransport* mockSocket_{nullptr};
  MockAppHandler appHandler_;
  SimpleBufferAllocator allocator_;
};

TEST_F(TransportHandlerEventTest, WriteSuccessInvokesFactoryWithSuccessStatus) {
  auto [handler, pipeline] = createHandlerAndPipeline();
  handler->onConnect();

  folly::AsyncTransport::WriteCallback* capturedCallback = nullptr;
  EXPECT_CALL(*mockSocket_, writeChain(_, _, _))
      .WillOnce(SaveArg<0>(&capturedCallback));

  Result result = handler->onWrite(
      channel_pipeline::test::inertEndpointContext(),
      TypeErasedBox(folly::IOBuf::copyBuffer("payload")));
  EXPECT_EQ(result, Result::Backpressure);
  ASSERT_NE(capturedCallback, nullptr);

  capturedCallback->writeSuccess();

  const std::vector<std::pair<WriteCompletionStatus, size_t>> expected = {
      {WriteCompletionStatus::Success, 0}};
  EXPECT_EQ(CapturingFactoryState::calls, expected);
}

TEST_F(
    TransportHandlerEventTest, WriteErrInvokesFactoryWithErrorStatusAndBytes) {
  auto [handler, pipeline] = createHandlerAndPipeline();
  handler->onConnect();

  folly::AsyncTransport::WriteCallback* capturedCallback = nullptr;
  EXPECT_CALL(*mockSocket_, writeChain(_, _, _))
      .WillOnce(SaveArg<0>(&capturedCallback));

  Result result = handler->onWrite(
      channel_pipeline::test::inertEndpointContext(),
      TypeErasedBox(folly::IOBuf::copyBuffer("payload")));
  EXPECT_EQ(result, Result::Backpressure);
  ASSERT_NE(capturedCallback, nullptr);

  constexpr size_t kBytesWritten = 42;
  folly::AsyncSocketException ex(
      folly::AsyncSocketException::NETWORK_ERROR, "test write failure");
  capturedCallback->writeErr(kBytesWritten, ex);

  const std::vector<std::pair<WriteCompletionStatus, size_t>> expected = {
      {WriteCompletionStatus::Error, kBytesWritten}};
  EXPECT_EQ(CapturingFactoryState::calls, expected);
}

// Completion events fire only while Open. A write that lands during the
// graceful drain (Closing) must not produce a success event.
TEST_F(TransportHandlerEventTest, WriteSuccessDuringDrainSuppressesEvent) {
  auto [handler, pipeline] = createHandlerAndPipeline();
  handler->onConnect();

  folly::AsyncTransport::WriteCallback* cb = nullptr;
  EXPECT_CALL(*mockSocket_, writeChain(_, _, _)).WillOnce(SaveArg<0>(&cb));
  (void)handler->onWrite(
      channel_pipeline::test::inertEndpointContext(),
      TypeErasedBox(folly::IOBuf::copyBuffer("payload")));
  ASSERT_NE(cb, nullptr);

  // Begin a graceful close while the write is still pending: state -> Closing.
  handler->close(folly::exception_wrapper{});
  ASSERT_EQ(handler->state(), HandlerT::State::Closing);

  // The pending write lands during the drain; no completion event fires.
  cb->writeSuccess();

  EXPECT_EQ(handler->state(), HandlerT::State::Closed);
  EXPECT_TRUE(CapturingFactoryState::calls.empty());

  handler->resetPipeline();
  pipeline.reset();
}

// In a write-error cascade only the error that lands while Open (and thus
// initiates the close) fires a completion event; errors that cascade during
// the drain are suppressed symmetrically with success.
TEST_F(TransportHandlerEventTest, WriteErrCascadeFiresOnlyInitiatingError) {
  auto [handler, pipeline] = createHandlerAndPipeline();
  handler->onConnect();

  std::vector<folly::AsyncTransport::WriteCallback*> cbs;
  EXPECT_CALL(*mockSocket_, writeChain(_, _, _))
      .Times(3)
      .WillRepeatedly([&](folly::AsyncTransport::WriteCallback* cb,
                          std::shared_ptr<folly::IOBuf>,
                          folly::WriteFlags) { cbs.push_back(cb); });

  for (int i = 0; i < 3; ++i) {
    (void)handler->onWrite(
        channel_pipeline::test::inertEndpointContext(),
        TypeErasedBox(folly::IOBuf::copyBuffer("x")));
  }

  constexpr size_t kBytesWritten = 7;
  folly::AsyncSocketException ex(
      folly::AsyncSocketException::NETWORK_ERROR, "broken pipe");

  // First error lands while Open: fires one completion event, begins close.
  cbs[0]->writeErr(kBytesWritten, ex);
  ASSERT_EQ(handler->state(), HandlerT::State::Closing);

  // Remaining errors cascade during the drain: suppressed.
  cbs[1]->writeErr(0, ex);
  cbs[2]->writeErr(0, ex);
  ASSERT_EQ(handler->state(), HandlerT::State::Closed);

  const std::vector<std::pair<WriteCompletionStatus, size_t>> expected = {
      {WriteCompletionStatus::Error, kBytesWritten}};
  EXPECT_EQ(CapturingFactoryState::calls, expected);

  handler->resetPipeline();
  pipeline.reset();
}

} // namespace apache::thrift::fast_thrift::transport
