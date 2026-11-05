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

/**
 * Each request gets exactly one write completion, even when its fragments go
 * out over several EventBase callbacks.
 *
 * The test pipeline puts the FIFO fragmentation handler over the real loop
 * batcher, the real completion trackers, and a mocked socket. The tail records
 * every FrameWriteCompleteEvent that the fragment tracker fires.
 */

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include <folly/io/IOBuf.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/AsyncTransport.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/test/MockAsyncTransport.h>
#include <folly/portability/GMock.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/BufferAllocator.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/HandlerTag.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineBuilder.h>
#include <thrift/lib/cpp2/fast_thrift/frame/FrameType.h>
#include <thrift/lib/cpp2/fast_thrift/frame/handler/FrameCodecHandler.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/ComposedFrame.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/QuantumFragmentationHandlerConfig.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/handler/FifoFrameFragmentationHandler.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/handler/FragmentCompletionTracker.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/handler/FrameLengthEncoderHandler.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/handler/LoopBatchingFrameHandler.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/handler/WriteCompletionTracker.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/client/Event.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/client/RocketClientEventFactory.h>
#include <thrift/lib/cpp2/fast_thrift/transport/TransportHandler.h>
#include <thrift/lib/cpp2/fast_thrift/transport/WriteCompletion.h>
#include <thrift/lib/cpp2/fast_thrift/transport/test/PassthroughParser.h>

namespace apache::thrift::fast_thrift::rocket::client {
namespace {

namespace cp = apache::thrift::fast_thrift::channel_pipeline;
namespace fw = apache::thrift::fast_thrift::frame::write::handler;
namespace transport = apache::thrift::fast_thrift::transport;
using transport::WriteCompletionStatus;
using namespace testing;

// Small enough that the 3072-byte request needs three turns, which puts its
// fragments in three EventBase callbacks.
constexpr size_t kStreamQuantum = 1024;
constexpr uint32_t kLargeStreamId = 1;
constexpr uint32_t kSmallStreamId = 3;
constexpr size_t kLargeRequestBytes = 3 * kStreamQuantum;
constexpr size_t kSmallRequestBytes = 100;

using TestTransportHandler = transport::TransportHandlerT<
    RocketClientEventFactory,
    apache::thrift::fast_thrift::transport::test::PassthroughParser>;
using TestBatcher = fw::LoopBatchingFrameHandlerT<
    fw::WriteCompletionTrackerT<RocketClientEventFactory>>;
using TestFragmenter = fw::FifoFrameFragmentationHandlerT<
    fw::FragmentCompletionTrackerT<RocketClientEventFactory>>;

HANDLER_TAG(batching);
HANDLER_TAG(length_encoder);
HANDLER_TAG(codec);
HANDLER_TAG(fragmentation);

struct CompletedRequest {
  uint32_t streamId;
  WriteCompletionStatus status;
  bool quiesced;

  bool operator==(const CompletedRequest&) const = default;
};

class EventCapturingAppHandler {
 public:
  using Result = cp::Result;

  Result onRead(cp::detail::ContextImpl&, cp::TypeErasedBox&&) noexcept {
    return Result::Success;
  }
  void onException(folly::exception_wrapper&&) noexcept {}
  void handlerAdded() noexcept {}
  void handlerRemoved() noexcept {}
  void onPipelineActive() noexcept {}
  void onPipelineInactive() noexcept {}
  void onWriteReady() noexcept {}

  using SubscribedEvents = cp::Events<FrameWriteCompleteEvent>;

  template <cp::PipelineEvent E>
    requires std::same_as<E, FrameWriteCompleteEvent>
  void on(const FrameWriteCompleteEvent& event) noexcept {
    completed_.push_back(
        {.streamId = event.streamId,
         .status = event.status,
         .quiesced = event.quiesced});
  }

  const std::vector<CompletedRequest>& completed() const noexcept {
    return completed_;
  }

 private:
  std::vector<CompletedRequest> completed_;
};

class RocketClientFifoWriteCompletionIntegrationTest : public Test {
 protected:
  struct SocketWrite {
    folly::AsyncTransport::WriteCallback* callback;
    size_t bytes;
  };

  RocketClientFifoWriteCompletionIntegrationTest() {
    auto socket = folly::AsyncTransport::UniquePtr(
        new NiceMock<folly::test::MockAsyncTransport>());
    mockSocket_ = static_cast<folly::test::MockAsyncTransport*>(socket.get());
    ON_CALL(*mockSocket_, good()).WillByDefault(Return(true));
    ON_CALL(*mockSocket_, getEventBase()).WillByDefault(Return(&evb_));
    ON_CALL(*mockSocket_, writeChain(_, _, _))
        .WillByDefault([this](
                           folly::AsyncTransport::WriteCallback* callback,
                           const std::shared_ptr<folly::IOBuf>& bytes,
                           folly::WriteFlags) {
          writes_.push_back(
              {.callback = callback, .bytes = bytes->computeChainDataLength()});
        });

    transportHandler_ =
        TestTransportHandler::create(std::move(socket), 256, 4096);

    auto fragmenter = std::make_unique<TestFragmenter>(
        frame::write::QuantumFragmentationHandlerConfig{
            .streamQuantum = kStreamQuantum});
    fragmenter_ = fragmenter.get();

    pipeline_ =
        cp::PipelineBuilder<
            TestTransportHandler,
            EventCapturingAppHandler,
            cp::SimpleBufferAllocator>()
            .setEventBase(&evb_)
            .setHead(transportHandler_.get())
            .setTail(&app_)
            .setAllocator(&allocator_)
            .addNextOutbound<TestBatcher>(batching_tag)
            .addNextOutbound<fw::FrameLengthEncoderHandler>(length_encoder_tag)
            .addNextDuplex<frame::handler::FrameCodecHandler>(codec_tag)
            .addNextOutbound<TestFragmenter>(
                fragmentation_tag, std::move(fragmenter))
            .build();

    transportHandler_->setPipeline(pipeline_.get());
    transportHandler_->onConnect();
  }

  ~RocketClientFifoWriteCompletionIntegrationTest() override {
    // Tear pipeline down before transport handler so the transport outlives
    // any in-flight WriteRequests the pipeline still holds.
    pipeline_.reset();
    transportHandler_.reset();
  }

  void sendRequest(uint32_t streamId, size_t dataBytes) {
    frame::ComposedFrame frame{
        .frameType = frame::FrameType::REQUEST_RESPONSE,
        .streamId = streamId,
    };
    frame.data = folly::IOBuf::create(dataBytes);
    frame.data->append(dataBytes);
    std::memset(frame.data->writableData(), 'x', dataBytes);
    EXPECT_THAT(
        pipeline_->fireWrite(cp::TypeErasedBox(std::move(frame))),
        Ne(cp::Result::Error));
  }

  void sendCancel(uint32_t streamId) {
    frame::ComposedFrame frame{
        .frameType = frame::FrameType::CANCEL,
        .streamId = streamId,
    };
    EXPECT_THAT(
        pipeline_->fireWrite(cp::TypeErasedBox(std::move(frame))),
        Ne(cp::Result::Error));
  }

  folly::EventBase evb_;
  folly::test::MockAsyncTransport* mockSocket_{nullptr};
  TestTransportHandler::Ptr transportHandler_;
  TestFragmenter* fragmenter_{nullptr};
  EventCapturingAppHandler app_;
  cp::SimpleBufferAllocator allocator_;
  cp::PipelineImpl::Ptr pipeline_;
  std::vector<SocketWrite> writes_;
};

TEST_F(
    RocketClientFifoWriteCompletionIntegrationTest,
    EachRequestCompletesOnceInWireOrder) {
  sendRequest(kLargeStreamId, kLargeRequestBytes);
  sendRequest(kSmallStreamId, kSmallRequestBytes);

  evb_.loopOnce(EVLOOP_NONBLOCK);
  ASSERT_THAT(writes_, SizeIs(1));
  writes_[0].callback->writeSuccess();
  const std::vector<CompletedRequest> afterFirstPass{
      {.streamId = kSmallStreamId,
       .status = WriteCompletionStatus::Success,
       .quiesced = false}};
  EXPECT_THAT(app_.completed(), Eq(afterFirstPass));

  evb_.loopOnce(EVLOOP_NONBLOCK);
  ASSERT_THAT(writes_, SizeIs(2));
  writes_[1].callback->writeSuccess();
  EXPECT_THAT(app_.completed(), Eq(afterFirstPass));

  evb_.loopOnce(EVLOOP_NONBLOCK);
  ASSERT_THAT(writes_, SizeIs(3));
  writes_[2].callback->writeSuccess();
  const std::vector<CompletedRequest> afterLastPass{
      {.streamId = kSmallStreamId,
       .status = WriteCompletionStatus::Success,
       .quiesced = false},
      {.streamId = kLargeStreamId,
       .status = WriteCompletionStatus::Success,
       .quiesced = true}};
  EXPECT_THAT(app_.completed(), Eq(afterLastPass));
}

TEST_F(
    RocketClientFifoWriteCompletionIntegrationTest,
    SocketErrorCompletesItsOwnWriteAndDropsTheRest) {
  sendRequest(kLargeStreamId, kLargeRequestBytes);
  sendRequest(kSmallStreamId, kSmallRequestBytes);

  evb_.loopOnce(EVLOOP_NONBLOCK);
  ASSERT_THAT(writes_, SizeIs(1));
  const folly::AsyncSocketException ex(
      folly::AsyncSocketException::NETWORK_ERROR, "no bytes hit wire");
  writes_[0].callback->writeErr(/*bytesWritten=*/0, ex);

  const std::vector<CompletedRequest> expected{
      {.streamId = kSmallStreamId,
       .status = WriteCompletionStatus::Error,
       .quiesced = false}};
  EXPECT_THAT(app_.completed(), Eq(expected));
  EXPECT_THAT(fragmenter_->hasPendingWrites(), IsFalse());
}

TEST_F(
    RocketClientFifoWriteCompletionIntegrationTest,
    CancelCompletesItselfAndDropsItsStream) {
  sendRequest(kLargeStreamId, kLargeRequestBytes);

  evb_.loopOnce(EVLOOP_NONBLOCK);
  ASSERT_THAT(writes_, SizeIs(1));

  sendCancel(kLargeStreamId);
  evb_.loopOnce(EVLOOP_NONBLOCK);
  ASSERT_THAT(writes_, SizeIs(2));

  writes_[0].callback->writeSuccess();
  writes_[1].callback->writeSuccess();
  evb_.loopOnce(EVLOOP_NONBLOCK);

  const std::vector<CompletedRequest> expected{
      {.streamId = kLargeStreamId,
       .status = WriteCompletionStatus::Success,
       .quiesced = true}};
  EXPECT_THAT(app_.completed(), Eq(expected));
  EXPECT_THAT(writes_, SizeIs(2));
}

} // namespace
} // namespace apache::thrift::fast_thrift::rocket::client
