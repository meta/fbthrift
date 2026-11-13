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
#include <memory>
#include <stdexcept>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include <folly/ExceptionWrapper.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/EventBase.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Event.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/HandlerTag.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineBuilder.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineRef.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/StaticPipelineBuilder.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/ErasedStaticHandler.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/test/MockAdapters.h>
#include <thrift/lib/cpp2/fast_thrift/frame/ErrorCode.h>
#include <thrift/lib/cpp2/fast_thrift/frame/FrameType.h>
#include <thrift/lib/cpp2/fast_thrift/frame/read/FrameParser.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/FrameHeaders.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/FrameWriter.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/client/Event.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/client/Messages.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/client/handler/RocketClientMetadataPushHandler.h>

namespace apache::thrift::fast_thrift::rocket::client::handler {
namespace {

using channel_pipeline::erase_and_box;
using channel_pipeline::PipelineBuilder;
using channel_pipeline::PipelineOwner;
using channel_pipeline::Result;
using channel_pipeline::StaticPipelineBuilder;
using channel_pipeline::TypeErasedBox;
using channel_pipeline::test::TestAllocator;

HANDLER_TAG(metadata_push);

struct MetadataPushHead {
  template <typename Context>
  Result onWrite(Context&, TypeErasedBox&&) noexcept {
    return Result::Success;
  }

  void onReadReady() noexcept {}
  void handlerAdded() noexcept {}
  void handlerRemoved() noexcept {}
  void onPipelineActive() noexcept {}
  void onPipelineInactive() noexcept {}
};

struct MetadataPushReceiver {
  using SubscribedEvents = channel_pipeline::Events<MetadataPushEvent>;

  template <typename Context>
  Result onRead(Context&, TypeErasedBox&& msg) noexcept {
    ++readCount;
    forwarded = std::move(msg);
    return readResult;
  }

  void onException(folly::exception_wrapper&& ew) noexcept {
    exception = std::move(ew);
  }

  template <channel_pipeline::PipelineEvent E>
  void on(typename E::Payload event) noexcept {
    static_assert(std::same_as<E, MetadataPushEvent>);
    ++eventCount;
    metadata = std::move(event->metadata);
    event->result = eventResult;
    event->error = eventError;
  }

  void onWriteReady() noexcept {}
  void handlerAdded() noexcept {}
  void handlerRemoved() noexcept {}
  void onPipelineActive() noexcept {}
  void onPipelineInactive() noexcept {}

  TypeErasedBox forwarded;
  std::unique_ptr<folly::IOBuf> metadata;
  folly::exception_wrapper exception;
  folly::exception_wrapper eventError;
  Result eventResult{Result::Success};
  Result readResult{Result::Success};
  size_t eventCount{0};
  size_t readCount{0};
};

RocketResponseMessage responseFromWire(std::unique_ptr<folly::IOBuf> wire) {
  RocketResponseMessage response;
  response.payload = frame::read::parseFrame(std::move(wire));
  return response;
}

class RocketClientMetadataPushHandlerTest : public ::testing::Test {
 protected:
  RocketClientMetadataPushHandlerTest()
      : pipeline_(
            PipelineBuilder<
                MetadataPushHead,
                MetadataPushReceiver,
                TestAllocator>()
                .setEventBase(&evb_)
                .setHead(&head_)
                .setTail(&receiver_)
                .setAllocator(&allocator_)
                .addNextInbound<RocketClientMetadataPushHandler>(
                    metadata_push_tag)
                .build()) {}

  ~RocketClientMetadataPushHandlerTest() override { pipeline_->close(); }

  Result read(std::unique_ptr<folly::IOBuf> wire) {
    return pipeline_->fireRead(
        erase_and_box(responseFromWire(std::move(wire))));
  }

  folly::EventBase evb_;
  MetadataPushHead head_;
  TestAllocator allocator_;
  MetadataPushReceiver receiver_;
  PipelineOwner pipeline_;
};

TEST_F(
    RocketClientMetadataPushHandlerTest, OpaqueMetadataIsDeliveredAsControl) {
  auto metadata = folly::IOBuf::copyBuffer("opaque non-Thrift metadata");
  const auto* data = metadata->data();
  auto wire = frame::write::serialize(
      frame::write::MetadataPushHeader{}, std::move(metadata));

  EXPECT_EQ(read(std::move(wire)), Result::Success);
  EXPECT_EQ(receiver_.eventCount, 1);
  EXPECT_EQ(receiver_.readCount, 0);
  EXPECT_FALSE(receiver_.exception);
  ASSERT_NE(receiver_.metadata, nullptr);
  EXPECT_EQ(receiver_.metadata->data(), data);
  EXPECT_EQ(
      receiver_.metadata->moveToFbString().toStdString(),
      "opaque non-Thrift metadata");
}

TEST_F(
    RocketClientMetadataPushHandlerTest, ChainedHeaderAndMetadataStayZeroCopy) {
  auto first = folly::IOBuf::copyBuffer("first");
  auto second = folly::IOBuf::copyBuffer("second");
  const auto* firstData = first->data();
  const auto* secondData = second->data();
  first->appendToChain(std::move(second));

  auto wire =
      frame::write::serialize(frame::write::MetadataPushHeader{}, nullptr);
  auto headerTail = wire->cloneOne();
  headerTail->trimStart(3);
  wire->trimEnd(3);
  wire->appendToChain(std::move(headerTail));
  wire->appendToChain(std::move(first));

  EXPECT_EQ(read(std::move(wire)), Result::Success);
  EXPECT_EQ(receiver_.eventCount, 1);
  EXPECT_EQ(receiver_.readCount, 0);
  EXPECT_FALSE(receiver_.exception);
  ASSERT_NE(receiver_.metadata, nullptr);
  EXPECT_EQ(receiver_.metadata->countChainElements(), 2);
  EXPECT_EQ(receiver_.metadata->data(), firstData);
  EXPECT_EQ(receiver_.metadata->next()->data(), secondData);
  EXPECT_EQ(receiver_.metadata->moveToFbString().toStdString(), "firstsecond");
}

TEST_F(
    RocketClientMetadataPushHandlerTest,
    DynamicAndStaticPipelinesDeliverToTheirOwnReceiver) {
  MetadataPushHead otherHead;
  TestAllocator otherAllocator;
  MetadataPushReceiver otherReceiver;
  otherReceiver.eventResult = Result::Backpressure;
  PipelineOwner otherPipeline(
      StaticPipelineBuilder<
          MetadataPushHead,
          MetadataPushReceiver,
          TestAllocator>()
          .setEventBase(&evb_)
          .setHead(&otherHead)
          .setTail(&otherReceiver)
          .setAllocator(&otherAllocator)
          .addNextInbound<RocketClientMetadataPushHandler>(metadata_push_tag)
          .build());

  MetadataPushHead erasedHead;
  TestAllocator erasedAllocator;
  MetadataPushReceiver erasedReceiver;
  erasedReceiver.eventResult = Result::Error;
  std::vector<channel_pipeline::detail::ErasedStaticHandler> erasedHandlers;
  erasedHandlers.push_back(
      channel_pipeline::detail::makeErasedStaticHandler<
          RocketClientMetadataPushHandler>(metadata_push_tag.id));
  PipelineOwner erasedPipeline(
      StaticPipelineBuilder<
          MetadataPushHead,
          MetadataPushReceiver,
          TestAllocator>()
          .setEventBase(&evb_)
          .setHead(&erasedHead)
          .setTail(&erasedReceiver)
          .setAllocator(&erasedAllocator)
          .addStaticHandlers(std::move(erasedHandlers))
          .build());

  for (const auto& [pipeline, receiver] :
       {std::pair{pipeline_.ref(), &receiver_},
        std::pair{otherPipeline.ref(), &otherReceiver},
        std::pair{erasedPipeline.ref(), &erasedReceiver},
        std::pair{pipeline_.ref(), &receiver_}}) {
    auto metadata = folly::IOBuf::copyBuffer("opaque");
    const auto* data = metadata->data();
    auto wire = frame::write::serialize(
        frame::write::MetadataPushHeader{}, std::move(metadata));
    EXPECT_EQ(
        pipeline.fireRead(erase_and_box(responseFromWire(std::move(wire)))),
        receiver->eventResult);
    ASSERT_NE(receiver->metadata, nullptr);
    EXPECT_EQ(receiver->metadata->data(), data);
    EXPECT_EQ(receiver->metadata->moveToFbString().toStdString(), "opaque");
    EXPECT_FALSE(receiver->exception);
  }
  EXPECT_EQ(receiver_.eventCount, 2);
  EXPECT_EQ(otherReceiver.eventCount, 1);
  EXPECT_EQ(erasedReceiver.eventCount, 1);
  EXPECT_EQ(receiver_.readCount, 0);
  EXPECT_EQ(otherReceiver.readCount, 0);
  EXPECT_EQ(erasedReceiver.readCount, 0);
  otherPipeline->close();
  erasedPipeline->close();
}

TEST_F(RocketClientMetadataPushHandlerTest, EmptyMetadataIsDelivered) {
  auto wire =
      frame::write::serialize(frame::write::MetadataPushHeader{}, nullptr);

  EXPECT_EQ(read(std::move(wire)), Result::Success);
  EXPECT_EQ(receiver_.eventCount, 1);
  EXPECT_EQ(receiver_.metadata, nullptr);
  EXPECT_EQ(receiver_.readCount, 0);
  EXPECT_FALSE(receiver_.exception);
}

TEST_F(RocketClientMetadataPushHandlerTest, MetadataWithoutMFlagIsDelivered) {
  auto wire = frame::write::serialize(
      frame::write::MetadataPushHeader{},
      folly::IOBuf::copyBuffer("metadata without M"));
  wire->writableData()[4] &=
      ~static_cast<uint8_t>(frame::detail::kMetadataBit >> 8);

  EXPECT_EQ(read(std::move(wire)), Result::Success);
  ASSERT_NE(receiver_.metadata, nullptr);
  EXPECT_EQ(
      receiver_.metadata->moveToFbString().toStdString(), "metadata without M");
  EXPECT_FALSE(receiver_.exception);
}

TEST_F(RocketClientMetadataPushHandlerTest, EventResultIsPreserved) {
  for (const auto result :
       {Result::Success, Result::Backpressure, Result::Error}) {
    receiver_.eventResult = result;
    auto wire = frame::write::serialize(
        frame::write::MetadataPushHeader{}, folly::IOBuf::copyBuffer("opaque"));

    EXPECT_EQ(read(std::move(wire)), result);
  }
  EXPECT_EQ(receiver_.eventCount, 3);
  EXPECT_EQ(receiver_.readCount, 0);
  EXPECT_FALSE(receiver_.exception);
}

TEST_F(RocketClientMetadataPushHandlerTest, ControlErrorForwardsItsCause) {
  receiver_.eventResult = Result::Error;
  receiver_.eventError =
      folly::make_exception_wrapper<std::runtime_error>("control failure");
  const auto* cause = receiver_.eventError.get_exception<std::runtime_error>();
  auto wire = frame::write::serialize(
      frame::write::MetadataPushHeader{}, folly::IOBuf::copyBuffer("opaque"));

  EXPECT_EQ(read(std::move(wire)), Result::Error);
  EXPECT_EQ(receiver_.exception.get_exception<std::runtime_error>(), cause);
  EXPECT_EQ(receiver_.eventCount, 1);
  EXPECT_EQ(receiver_.readCount, 0);
}

TEST_F(
    RocketClientMetadataPushHandlerTest, BackpressureDoesNotRaiseAnException) {
  receiver_.eventResult = Result::Backpressure;
  receiver_.eventError =
      folly::make_exception_wrapper<std::runtime_error>("control failure");
  auto wire = frame::write::serialize(
      frame::write::MetadataPushHeader{}, folly::IOBuf::copyBuffer("opaque"));

  EXPECT_EQ(read(std::move(wire)), Result::Backpressure);
  EXPECT_FALSE(receiver_.exception);
  EXPECT_EQ(receiver_.eventCount, 1);
  EXPECT_EQ(receiver_.readCount, 0);
}

TEST_F(RocketClientMetadataPushHandlerTest, NonzeroStreamIdIsRejected) {
  auto wire = frame::write::serialize(
      frame::write::MetadataPushHeader{}, folly::IOBuf::copyBuffer("opaque"));
  wire->writableData()[3] = 1;

  EXPECT_EQ(read(std::move(wire)), Result::Error);
  EXPECT_EQ(receiver_.eventCount, 0);
  EXPECT_EQ(receiver_.readCount, 0);
  EXPECT_NE(receiver_.exception.get_exception<std::runtime_error>(), nullptr);
}

TEST_F(RocketClientMetadataPushHandlerTest, PayloadPassesThroughUnchanged) {
  auto wire = frame::write::serialize(
      frame::write::PayloadHeader{
          .streamId = 7, .complete = true, .next = true},
      nullptr,
      folly::IOBuf::copyBuffer("response"));
  const auto* originalBuffer = wire.get();
  receiver_.readResult = Result::Backpressure;

  EXPECT_EQ(read(std::move(wire)), Result::Backpressure);
  EXPECT_EQ(receiver_.readCount, 1);
  EXPECT_EQ(receiver_.eventCount, 0);
  auto& parsed = receiver_.forwarded.get<RocketResponseMessage>()
                     .payload.get<frame::read::ParsedFrame>();
  EXPECT_EQ(parsed.streamId(), 7);
  EXPECT_EQ(parsed.buffer.get(), originalBuffer);
}

TEST_F(RocketClientMetadataPushHandlerTest, WireErrorPassesThrough) {
  auto wire = frame::write::serialize(
      frame::write::ErrorHeader{
          .streamId = 7,
          .errorCode =
              static_cast<uint32_t>(frame::ErrorCode::APPLICATION_ERROR)},
      nullptr,
      folly::IOBuf::copyBuffer("application error"));
  receiver_.readResult = Result::Error;

  EXPECT_EQ(read(std::move(wire)), Result::Error);
  EXPECT_EQ(receiver_.readCount, 1);
  EXPECT_EQ(receiver_.eventCount, 0);
  EXPECT_FALSE(receiver_.exception);
}

TEST_F(RocketClientMetadataPushHandlerTest, InProcessErrorPassesThrough) {
  RocketResponseMessage response;
  response.payload = RocketResponseError{
      .ew = folly::make_exception_wrapper<std::runtime_error>("request error"),
      .streamId = 9};

  EXPECT_EQ(
      pipeline_->fireRead(erase_and_box(std::move(response))), Result::Success);
  EXPECT_EQ(receiver_.readCount, 1);
  EXPECT_EQ(receiver_.eventCount, 0);
  auto& error = receiver_.forwarded.get<RocketResponseMessage>()
                    .payload.get<RocketResponseError>();
  EXPECT_EQ(error.streamId, 9);
  EXPECT_NE(error.ew.get_exception<std::runtime_error>(), nullptr);
}

} // namespace
} // namespace apache::thrift::fast_thrift::rocket::client::handler
