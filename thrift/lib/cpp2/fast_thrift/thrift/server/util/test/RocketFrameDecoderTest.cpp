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

#include <folly/io/IOBuf.h>
#include <folly/io/async/EventBase.h>
#include <thrift/lib/cpp2/fast_thrift/common/allocator/EvbAllocator.h>
#include <thrift/lib/cpp2/fast_thrift/frame/read/FrameParser.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/ComposedFrame.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/common/RequestMetadata.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/PayloadVariants.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/util/RocketFrameDecoder.h>

namespace apache::thrift::fast_thrift::thrift {

namespace {

folly::EventBase& decoderEventBase() {
  static folly::EventBase eventBase;
  return eventBase;
}

apache::thrift::RequestRpcMetadata makePopulatedRequestMetadata(
    const std::string& method) {
  apache::thrift::RequestRpcMetadata md;
  md.name() = method;
  md.kind() = apache::thrift::RpcKind::SINGLE_REQUEST_SINGLE_RESPONSE;
  md.protocol() = apache::thrift::ProtocolId::COMPACT;
  return md;
}

frame::read::ParsedFrame makeRequestResponseFrame(
    uint32_t streamId,
    apache::thrift::RequestRpcMetadata md,
    std::unique_ptr<folly::IOBuf> data) {
  ThriftRequestResponsePayload payload{
      .data = std::move(data),
      .metadata =
          std::make_unique<apache::thrift::RequestRpcMetadata>(std::move(md))};
  auto wire = std::move(payload)
                  .toRocketFrame(
                      ::apache::thrift::fast_thrift::rocket::server::
                          MetadataProtocol::BINARY)
                  .serialize();
  // The header.streamId in the constructed frame is kInvalidStreamId
  // because toRocketFrame hardcodes it for client outbound. Patch the
  // first 4 bytes so parsing yields the desired streamId.
  auto* p = wire->writableData();
  p[0] = static_cast<uint8_t>(streamId >> 24);
  p[1] = static_cast<uint8_t>(streamId >> 16);
  p[2] = static_cast<uint8_t>(streamId >> 8);
  p[3] = static_cast<uint8_t>(streamId);
  return frame::read::parseFrame(std::move(wire));
}

frame::read::ParsedFrame makeRequestStreamFrame(
    apache::thrift::RequestRpcMetadata md,
    std::unique_ptr<folly::IOBuf> data,
    uint32_t initialRequestN) {
  // No streamId patching: fromRocketFrame decodes REQUEST_STREAM into
  // ThriftRequestStreamPayload, which carries no streamId (the transport
  // adapter stamps it on the message downstream), so the frame's streamId is
  // irrelevant to what this test observes.
  ThriftRequestStreamPayload payload{
      .data = std::move(data),
      .metadata =
          std::make_unique<apache::thrift::RequestRpcMetadata>(std::move(md)),
      .initialRequestN = initialRequestN};
  auto wire = std::move(payload)
                  .toRocketFrame(
                      ::apache::thrift::fast_thrift::rocket::server::
                          MetadataProtocol::BINARY)
                  .serialize();
  return frame::read::parseFrame(std::move(wire));
}

frame::read::ParsedFrame makeFnfFrame(uint32_t streamId) {
  frame::ComposedFrame frame{
      .frameType = frame::FrameType::REQUEST_FNF,
      .streamId = streamId,
      .metadata = nullptr,
      .data = folly::IOBuf::copyBuffer("fnf")};
  auto wire = std::move(frame).serialize();
  return frame::read::parseFrame(std::move(wire));
}

frame::read::ParsedFrame makeKeepAliveFrame() {
  frame::ComposedFrame frame{.frameType = frame::FrameType::KEEPALIVE};
  auto wire = std::move(frame).serialize();
  return frame::read::parseFrame(std::move(wire));
}

frame::read::ParsedFrame makeRequestNFrame(uint32_t streamId, uint32_t n) {
  frame::ComposedFrame frame{
      .frameType = frame::FrameType::REQUEST_N,
      .streamId = streamId,
      .requestN = n};
  auto wire = std::move(frame).serialize();
  return frame::read::parseFrame(std::move(wire));
}

frame::read::ParsedFrame makeCancelFrame(uint32_t streamId) {
  frame::ComposedFrame frame{
      .frameType = frame::FrameType::CANCEL, .streamId = streamId};
  auto wire = std::move(frame).serialize();
  return frame::read::parseFrame(std::move(wire));
}

} // namespace

TEST(FromRocketFrameTest, RequestResponseDecodesToTypedPayload) {
  auto frame = makeRequestResponseFrame(
      /*streamId=*/7,
      makePopulatedRequestMetadata("Service.method"),
      folly::IOBuf::copyBuffer("hello"));

  auto result = fromRocketFrame(
      std::move(frame),
      apache::thrift::fast_thrift::rocket::server::MetadataProtocol::BINARY,
      decoderEventBase());
  ASSERT_TRUE(result.hasValue());
  ASSERT_TRUE(result->is<ThriftServerRequestResponsePayload>());

  auto& rr = result->get<ThriftServerRequestResponsePayload>();
  ASSERT_NE(rr.metadata, nullptr);
  ASSERT_TRUE(rr.metadata->name().has_value());
  EXPECT_EQ(rr.metadata->name()->view(), "Service.method");
  ASSERT_NE(rr.data, nullptr);
  EXPECT_EQ(rr.data->moveToFbString().toStdString(), "hello");
}

TEST(FromRocketFrameTest, RequestMetadataBypassesEventBaseAllocator) {
  auto& allocator = mem::EvbAllocator::getOrCreate(decoderEventBase());
  const auto baseline = allocator.snapshotStats().outstandingAllocations;

  {
    auto result = fromRocketFrame(
        makeRequestResponseFrame(
            /*streamId=*/7,
            makePopulatedRequestMetadata("Service.method"),
            folly::IOBuf::copyBuffer("hello")),
        apache::thrift::fast_thrift::rocket::server::MetadataProtocol::BINARY,
        decoderEventBase());
    ASSERT_TRUE(result.hasValue());
    EXPECT_EQ(allocator.snapshotStats().outstandingAllocations, baseline);
  }

  EXPECT_EQ(allocator.snapshotStats().outstandingAllocations, baseline);
}

TEST(FromRocketFrameTest, RecycledRequestMetadataCarriesNoStaleFields) {
  setServerRequestMetadataPoolLimit(4);
  auto decode = [](apache::thrift::RequestRpcMetadata md) {
    auto result = fromRocketFrame(
        makeRequestResponseFrame(
            /*streamId=*/7, std::move(md), folly::IOBuf::copyBuffer("hello")),
        apache::thrift::fast_thrift::rocket::server::MetadataProtocol::BINARY,
        decoderEventBase());
    EXPECT_TRUE(result.hasValue());
    return std::move(
        result->get<ThriftServerRequestResponsePayload>().metadata);
  };

  auto first = makePopulatedRequestMetadata("Service.first");
  first.clientTimeoutMs() = 50;
  first.otherMetadata().ensure()["header"] = "value";
  first.loadMetric() = "metric";
  auto decoded = decode(std::move(first));
  const auto* firstAddress = decoded.get();
  recycleServerRequestMetadata(decoded);
  EXPECT_EQ(decoded, nullptr);

  apache::thrift::RequestRpcMetadata second;
  second.name() = "Service.second";
  auto reused = decode(second);
  EXPECT_EQ(reused.get(), firstAddress);
  EXPECT_EQ(*reused, second);
  // Unset fields must hold defaults too, not the first request's values.
  EXPECT_EQ(reused->clientTimeoutMs().value_unchecked(), 0);
  EXPECT_TRUE(reused->otherMetadata().value_unchecked().empty());
  EXPECT_TRUE(reused->loadMetric().value_unchecked().empty());

  setServerRequestMetadataPoolLimit(0);
  recycleServerRequestMetadata(reused);
  EXPECT_NE(reused, nullptr);
}

TEST(FromRocketFrameTest, RequestResponseWithNoMetadataYieldsEmptyMetadata) {
  ThriftRequestResponsePayload payload{
      .data = folly::IOBuf::copyBuffer("body"),
      .metadata = std::make_unique<apache::thrift::RequestRpcMetadata>()};
  auto wire = std::move(payload)
                  .toRocketFrame(
                      ::apache::thrift::fast_thrift::rocket::server::
                          MetadataProtocol::BINARY)
                  .serialize();
  auto parsed = frame::read::parseFrame(std::move(wire));

  auto result = fromRocketFrame(
      std::move(parsed),
      apache::thrift::fast_thrift::rocket::server::MetadataProtocol::BINARY,
      decoderEventBase());
  ASSERT_TRUE(result.hasValue());
  auto& rr = result->get<ThriftServerRequestResponsePayload>();
  ASSERT_NE(rr.metadata, nullptr);
  EXPECT_FALSE(rr.metadata->name().has_value());
}

TEST(FromRocketFrameTest, MalformedMetadataReturnsError) {
  // Replace the metadata bytes with garbage that won't parse as Binary
  // protocol RequestRpcMetadata.
  frame::ComposedFrame frame{
      .frameType = frame::FrameType::REQUEST_RESPONSE,
      .streamId = 5,
      .metadata = folly::IOBuf::copyBuffer("\xFF\xFF\xFF\xFF garbage"),
      .data = folly::IOBuf::copyBuffer("data")};
  auto wire = std::move(frame).serialize();
  auto parsed = frame::read::parseFrame(std::move(wire));

  auto result = fromRocketFrame(
      std::move(parsed),
      apache::thrift::fast_thrift::rocket::server::MetadataProtocol::BINARY,
      decoderEventBase());
  EXPECT_FALSE(result.hasValue());
}

TEST(FromRocketFrameTest, RequestStreamDecodesToTypedPayload) {
  auto md = makePopulatedRequestMetadata("Service.streamMethod");
  md.kind() = apache::thrift::RpcKind::SINGLE_REQUEST_STREAMING_RESPONSE;
  auto frame = makeRequestStreamFrame(
      std::move(md), folly::IOBuf::copyBuffer("args"), /*initialRequestN=*/17);

  auto result = fromRocketFrame(
      std::move(frame),
      apache::thrift::fast_thrift::rocket::server::MetadataProtocol::BINARY,
      decoderEventBase());
  ASSERT_TRUE(result.hasValue());
  ASSERT_TRUE(result->is<ThriftRequestStreamPayload>());

  auto& stream = result->get<ThriftRequestStreamPayload>();
  ASSERT_NE(stream.metadata, nullptr);
  ASSERT_TRUE(stream.metadata->name().has_value());
  EXPECT_EQ(stream.metadata->name()->view(), "Service.streamMethod");
  ASSERT_NE(stream.data, nullptr);
  EXPECT_EQ(stream.data->moveToFbString().toStdString(), "args");
  EXPECT_EQ(stream.initialRequestN, 17u);
}

TEST(FromRocketFrameTest, RequestNDecodesToTypedPayload) {
  auto result = fromRocketFrame(
      makeRequestNFrame(/*streamId=*/9, /*n=*/42),
      apache::thrift::fast_thrift::rocket::server::MetadataProtocol::BINARY,
      decoderEventBase());
  ASSERT_TRUE(result.hasValue());
  ASSERT_TRUE(result->is<ThriftRequestNPayload>());

  const auto& requestN = result->get<ThriftRequestNPayload>();
  EXPECT_EQ(requestN.streamId, 9u);
  EXPECT_EQ(requestN.requestN, 42u);
}

TEST(FromRocketFrameTest, RequestFnfNotYetWired) {
  auto result = fromRocketFrame(
      makeFnfFrame(/*streamId=*/3),
      apache::thrift::fast_thrift::rocket::server::MetadataProtocol::BINARY,
      decoderEventBase());
  EXPECT_FALSE(result.hasValue());
}

TEST(FromRocketFrameTest, CancelDecodesWithoutMetadata) {
  auto result = fromRocketFrame(
      makeCancelFrame(/*streamId=*/9),
      apache::thrift::fast_thrift::rocket::server::MetadataProtocol::BINARY,
      decoderEventBase());
  ASSERT_TRUE(result.hasValue());
  EXPECT_TRUE(result->is<ThriftRequestCancellationPayload>());
}

TEST(FromRocketFrameTest, UnexpectedFrameTypeReturnsError) {
  auto result = fromRocketFrame(
      makeKeepAliveFrame(),
      apache::thrift::fast_thrift::rocket::server::MetadataProtocol::BINARY,
      decoderEventBase());
  EXPECT_FALSE(result.hasValue());
}

} // namespace apache::thrift::fast_thrift::thrift
