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

#include <algorithm>
#include <cstring>
#include <type_traits>
#include <utility>

#include <gtest/gtest.h>

#include <folly/Optional.h>
#include <folly/Range.h>
#include <folly/io/Cursor.h>
#include <folly/io/IOBuf.h>

#include <thrift/lib/cpp2/IOBufChain.h>
#include <thrift/lib/cpp2/IOBufChainCursor.h>
#include <thrift/lib/cpp2/transport/rocket/RocketException.h>
#include <thrift/lib/cpp2/transport/rocket/Types.h>
#include <thrift/lib/cpp2/transport/rocket/framing/ErrorCode.h>
#include <thrift/lib/cpp2/transport/rocket/framing/Flags.h>
#include <thrift/lib/cpp2/transport/rocket/framing/Frames.h>
#include <thrift/lib/cpp2/transport/rocket/framing/Serializer.h>
#include <thrift/lib/cpp2/transport/rocket/framing/test/Util.h>

using namespace apache::thrift::rocket;
using namespace apache::thrift::rocket::test;

namespace {
constexpr StreamId kTestStreamId{179};
constexpr folly::StringPiece kMetadata{"metadata"};
constexpr folly::StringPiece kData{"data"};

template <class Frame>
std::unique_ptr<folly::IOBuf> serialize(Frame frame) {
  Serializer writer;
  std::move(frame).serialize(writer);
  auto serializedFrameData = std::move(writer).move();
  serializedFrameData->coalesce();
  return serializedFrameData;
}

template <class Buffer, class Frame>
Frame serializeAndDeserializeFragmented(Frame frame) {
  auto serializedFrameData = serialize(std::move(frame));
  folly::Optional<Frame> returnFrame;
  folly::io::Cursor cursor(serializedFrameData.get());
  bool hitSplitLogic = false;
  while (!cursor.isAtEnd()) {
    const auto currentFrameSize = readFrameOrMetadataSize(cursor);
    auto frameBuf = folly::IOBuf::createCombined(currentFrameSize);
    cursor.clone(*frameBuf, currentFrameSize);
    if (!returnFrame) {
      returnFrame.emplace(
          Frame(makeFrameBuffer<Buffer>(std::move(frameBuf), 4096)));
    } else {
      hitSplitLogic = true;
      PayloadFrame pf(makeFrameBuffer<Buffer>(std::move(frameBuf), 4096));
      returnFrame->payload().append(std::move(pf.payload()));
      if (!pf.hasFollows()) {
        break;
      }
    }
  }
  EXPECT_TRUE(returnFrame);
  EXPECT_TRUE(hitSplitLogic);
  return std::move(*returnFrame);
}

folly::IOBuf makeLargeIOBuf() {
  // Ensure IOBuf will be serialized across multiple fragment frames
  constexpr size_t kLargeIOBufSize = 0x2ffffff;

  folly::IOBuf buf(folly::IOBuf::CreateOp(), kLargeIOBufSize);
  EXPECT_GT(buf.tailroom(), kLargeIOBufSize);
  auto* const p = buf.writableTail();
  size_t i = 0;
  // Fill buffer with some non-trivial data
  std::for_each(p, p + kLargeIOBufSize, [&i](auto& c) {
    constexpr size_t kAPrime = 251;
    c = i++ % kAPrime;
  });
  buf.append(kLargeIOBufSize);
  return buf;
}

template <class Cursor>
void expectBytes(Cursor& cursor, folly::ByteRange expected) {
  while (!expected.empty()) {
    const auto bytes = cursor.peekBytes();
    ASSERT_FALSE(bytes.empty());
    const auto length = std::min(bytes.size(), expected.size());
    EXPECT_EQ(expected.subpiece(0, length), bytes.subpiece(0, length));
    cursor.skip(length);
    expected.advance(length);
  }
}

void validatePayload(
    const Payload& payload, folly::ByteRange metadata, folly::ByteRange data) {
  ASSERT_EQ(metadata.size(), payload.metadataSize());
  ASSERT_EQ(data.size(), payload.dataSize());
  auto validate = [&](auto cursor) {
    expectBytes(cursor, metadata);
    expectBytes(cursor, data);
    EXPECT_TRUE(cursor.isAtEnd());
  };
  if (payload.usesIOBufChain()) {
    validate(apache::thrift::io::IOBufChainCursor(payload.chainBuffer()));
  } else {
    validate(folly::io::Cursor(payload.buffer()));
  }
}

void validateMetadataAndData(const Payload& payload) {
  validatePayload(
      payload, folly::ByteRange(kMetadata), folly::ByteRange(kData));
}

void validateChainMetadataAndData(const Payload& payload) {
  ASSERT_TRUE(payload.usesIOBufChain());
  validateMetadataAndData(payload);
}
} // namespace

namespace apache::thrift::rocket {

template <typename Buffer>
class FrameSerializationTest : public ::testing::Test {
 protected:
  template <typename Frame>
  Frame roundTrip(Frame frame) {
    auto bytes = serialize(std::move(frame));
    EXPECT_TRUE(isMaybeRocketFrame(*bytes));
    bytes->trimStart(Serializer::kBytesForFrameOrMetadataLength);
    auto result = Frame(makeFrameBuffer<Buffer>(std::move(bytes)));
    if constexpr (requires {
                    result.payload();
                  } && !std::is_same_v<Frame, ErrorFrame>) {
      EXPECT_EQ(
          (std::is_same_v<Buffer, IOBufChain>),
          result.payload().usesIOBufChain());
    }
    return result;
  }

  template <typename Frame>
  Frame fragmentedRoundTrip(Frame frame) {
    auto result = serializeAndDeserializeFragmented<Buffer>(std::move(frame));
    EXPECT_EQ(
        (std::is_same_v<Buffer, IOBufChain>),
        result.payload().usesIOBufChain());
    return result;
  }
};

using FrameBuffers =
    ::testing::Types<std::unique_ptr<folly::IOBuf>, IOBufChain>;
TYPED_TEST_SUITE(FrameSerializationTest, FrameBuffers);

TYPED_TEST(FrameSerializationTest, SetupSanity) {
  SetupFrame frame(Payload::makeFromMetadataAndData(kMetadata, kData), false);

  auto validate = [](const SetupFrame& f) {
    EXPECT_TRUE(f.rocketMimeTypes());
    // Resumption and lease flags are not currently supported
    EXPECT_FALSE(f.hasResumeIdentificationToken());
    EXPECT_FALSE(f.hasLease());
    validateMetadataAndData(f.payload());
  };

  validate(frame);
  validate(this->roundTrip(std::move(frame)));
}

TYPED_TEST(FrameSerializationTest, RequestResponseSanity) {
  RequestResponseFrame frame(
      kTestStreamId, Payload::makeFromMetadataAndData(kMetadata, kData));

  auto validate = [](const RequestResponseFrame& f) {
    EXPECT_EQ(kTestStreamId, f.streamId());
    EXPECT_FALSE(f.hasFollows());
    validateMetadataAndData(f.payload());
  };

  validate(frame);
  validate(this->roundTrip(std::move(frame)));
}

TEST(FrameSerialization, RequestResponseFromChainCursor) {
  auto serialized = serialize(RequestResponseFrame(
      kTestStreamId, Payload::makeFromMetadataAndData(kMetadata, kData)));
  serialized->trimStart(Serializer::kBytesForFrameOrMetadataLength);
  IOBufChain chain(std::move(serialized));
  io::IOBufChainCursor cursor(chain);
  const auto streamId = readStreamId(cursor);
  const auto [frameType, flags] = readFrameTypeAndFlags(cursor);

  EXPECT_EQ(FrameType::REQUEST_RESPONSE, frameType);
  RequestResponseFrame frame(streamId, flags, cursor, std::move(chain));
  EXPECT_EQ(kTestStreamId, frame.streamId());
  validateChainMetadataAndData(frame.payload());
}

TEST(FrameSerialization, RequestResponseNoHeadroomWriteForSharedBuf) {
  // Verify headroom write not attampted on shared IOBufs
  const std::string str = "12345678901234567890abcdef";
  auto dt = folly::IOBuf::wrapBuffer(str.data(), str.size());
  auto md = dt->clone();
  // share the same underlying buffer, but leave enough headroom for
  // frame header, if it was attempted to be written to the headroom
  md->trimStart(20);
  RequestResponseFrame frame(
      kTestStreamId,
      Payload::makeFromMetadataAndData(std::move(md), std::move(dt)));

  auto serialBuf = std::move(frame).serialize();
  // deserialize
  serialBuf->coalesce();
  serialBuf->trimStart(Serializer::kBytesForFrameOrMetadataLength);
  RequestResponseFrame frame2(std::move(serialBuf));

  auto dataAndMetadata = splitMetadataAndData(frame2.payload());
  // Explicitly check copies in comparison, in case of erroneous write
  // to headroom original input data would be overwritten.
  EXPECT_EQ("abcdef", getRange(*dataAndMetadata.first));
  EXPECT_EQ("12345678901234567890abcdef", getRange(*dataAndMetadata.second));
}

TYPED_TEST(FrameSerializationTest, RequestFnfSanity) {
  RequestFnfFrame frame(
      kTestStreamId, Payload::makeFromMetadataAndData(kMetadata, kData));

  auto validate = [](const RequestFnfFrame& f) {
    EXPECT_EQ(kTestStreamId, f.streamId());
    EXPECT_FALSE(f.hasFollows());
    validateMetadataAndData(f.payload());
  };

  validate(frame);
  validate(this->roundTrip(std::move(frame)));
}

TYPED_TEST(FrameSerializationTest, RequestStreamSanity) {
  RequestStreamFrame frame(
      kTestStreamId,
      Payload::makeFromMetadataAndData(kMetadata, kData),
      123456789 /* initialRequestN */);

  auto validate = [](const RequestStreamFrame& f) {
    EXPECT_EQ(kTestStreamId, f.streamId());
    EXPECT_FALSE(f.hasFollows());
    EXPECT_EQ(123456789, f.initialRequestN());
    validateMetadataAndData(f.payload());
  };

  validate(frame);
  validate(this->roundTrip(std::move(frame)));
}

TYPED_TEST(FrameSerializationTest, RequestChannelSanity) {
  RequestChannelFrame frame(
      kTestStreamId,
      Payload::makeFromMetadataAndData(kMetadata, kData),
      123456789 /* initialRequestN */);
  frame.setHasComplete(true);

  auto validate = [](const RequestChannelFrame& f) {
    EXPECT_EQ(kTestStreamId, f.streamId());
    EXPECT_FALSE(f.hasFollows());
    EXPECT_EQ(123456789, f.initialRequestN());
    validateMetadataAndData(f.payload());
    EXPECT_TRUE(f.hasComplete());
  };

  validate(frame);
  validate(this->roundTrip(std::move(frame)));
}

TYPED_TEST(FrameSerializationTest, RequestNSanity) {
  RequestNFrame frame(kTestStreamId, std::numeric_limits<int32_t>::max());

  auto validate = [](const RequestNFrame& f) {
    EXPECT_EQ(kTestStreamId, f.streamId());
    EXPECT_EQ(std::numeric_limits<int32_t>::max(), f.requestN());
  };

  validate(frame);
  validate(this->roundTrip(std::move(frame)));
}

TYPED_TEST(FrameSerializationTest, CancelSanity) {
  CancelFrame frame(kTestStreamId);

  auto validate = [](const CancelFrame& f) {
    EXPECT_EQ(kTestStreamId, f.streamId());
  };

  validate(frame);
  validate(this->roundTrip(std::move(frame)));
}

TYPED_TEST(FrameSerializationTest, PayloadSanity) {
  PayloadFrame frame(
      kTestStreamId,
      Payload::makeFromMetadataAndData(kMetadata, kData),
      Flags().follows(true).complete(true).next(true));

  auto validate = [](const PayloadFrame& f) {
    EXPECT_EQ(kTestStreamId, f.streamId());
    EXPECT_TRUE(f.hasFollows());
    EXPECT_TRUE(f.hasComplete());
    EXPECT_TRUE(f.hasNext());
    validateMetadataAndData(f.payload());
  };

  validate(frame);
  validate(this->roundTrip(std::move(frame)));
}

TEST(FrameSerialization, ChainPayloadFinalizationPreservesAdjacentBuffers) {
  const std::string storage{"abcdefgh"};
  IOBufChain first;
  first.append(folly::IOBuf::wrapBuffer(storage.data(), 4));
  IOBufChain second;
  second.append(folly::IOBuf::wrapBuffer(storage.data() + 4, 4));

  auto payload = Payload::makeCombined(std::move(first), 0);
  payload.append(Payload::makeCombined(std::move(second), 0));

  EXPECT_EQ(2, payload.chainBuffer().chainElements());
  auto data = std::move(payload).chainData();
  EXPECT_EQ(2, data.chainElements());
  io::IOBufChainCursor cursor(data);
  EXPECT_EQ(storage, cursor.readFixedString(storage.size()));
}

TEST(FrameSerialization, ChainPayloadFinalizationPreservesGaps) {
  const std::string storage{"abcdefghi"};
  IOBufChain first;
  first.append(folly::IOBuf::wrapBuffer(storage.data(), 4));
  IOBufChain second;
  second.append(folly::IOBuf::wrapBuffer(storage.data() + 5, 4));

  auto payload = Payload::makeCombined(std::move(first), 0);
  payload.append(Payload::makeCombined(std::move(second), 0));

  auto data = std::move(payload).chainData();
  EXPECT_EQ(2, data.chainElements());
  io::IOBufChainCursor cursor(data);
  EXPECT_EQ("abcdfghi", cursor.readFixedString(data.chainLength()));
}

TYPED_TEST(FrameSerializationTest, PayloadEmptyMetadataSanity) {
  auto validate = [](const PayloadFrame& f) {
    EXPECT_EQ(kTestStreamId, f.streamId());
    EXPECT_TRUE(f.hasFollows());
    EXPECT_TRUE(f.hasComplete());
    EXPECT_TRUE(f.hasNext());
    EXPECT_FALSE(f.payload().hasNonemptyMetadata());
    validatePayload(f.payload(), {}, folly::ByteRange(kData));
  };

  // No metadata
  {
    PayloadFrame frame(
        kTestStreamId,
        Payload::makeFromData(kData),
        Flags().follows(true).complete(true).next(true));

    validate(frame);
    validate(this->roundTrip(std::move(frame)));
  }

  // Empty metadata
  {
    PayloadFrame frame(
        kTestStreamId,
        Payload::makeFromMetadataAndData(
            folly::ByteRange{folly::StringPiece{""}}, kData),
        Flags().follows(true).complete(true).next(true));

    validate(frame);
    validate(this->roundTrip(std::move(frame)));
  }
}

TYPED_TEST(FrameSerializationTest, ErrorSanity) {
  constexpr folly::StringPiece kErrorMessage{"error_message"};

  ErrorFrame frame(
      kTestStreamId, ErrorCode::CANCELED, Payload::makeFromData(kErrorMessage));

  auto validate = [=](const ErrorFrame& f) {
    EXPECT_EQ(kTestStreamId, f.streamId());
    EXPECT_EQ(ErrorCode::CANCELED, f.errorCode());
    EXPECT_FALSE(f.payload().hasNonemptyMetadata());
    ASSERT_NE(nullptr, f.payload().buffer());
    validatePayload(f.payload(), {}, folly::ByteRange{kErrorMessage});
  };

  validate(frame);
  validate(this->roundTrip(std::move(frame)));
}

TYPED_TEST(FrameSerializationTest, ErrorFromException) {
  constexpr folly::StringPiece kErrorMessage{"error_message"};

  RocketException rex(
      ErrorCode::CANCELED, folly::IOBuf::copyBuffer(kErrorMessage));
  ErrorFrame frame(kTestStreamId, std::move(rex));

  auto validate = [=](const ErrorFrame& f) {
    EXPECT_EQ(kTestStreamId, f.streamId());
    EXPECT_EQ(ErrorCode::CANCELED, f.errorCode());
    EXPECT_FALSE(f.payload().hasNonemptyMetadata());
    ASSERT_NE(nullptr, f.payload().buffer());
    validatePayload(f.payload(), {}, folly::ByteRange{kErrorMessage});
  };

  validate(frame);
  validate(this->roundTrip(std::move(frame)));
}

TYPED_TEST(FrameSerializationTest, MetadataPushSanity) {
  constexpr folly::StringPiece kMeta{"transport_version"};

  auto makeMetadataPushFrame = [&] {
    std::unique_ptr<folly::IOBuf> buf(folly::IOBuf::create(6));
    buf->append(6);
    folly::io::RWPrivateCursor wcursor(buf.get());
    // write StreamId
    wcursor.writeBE<uint32_t>(static_cast<uint32_t>(0));
    // write frameType and flags
    wcursor.writeBE<uint16_t>(static_cast<uint16_t>(
        static_cast<uint8_t>(FrameType::METADATA_PUSH) << Flags::kBits |
        static_cast<uint16_t>(1 << 8)));

    wcursor.insert(folly::IOBuf::copyBuffer(kMeta));
    buf->coalesce();
    return MetadataPushFrame(std::move(buf));
  };

  auto validate = [=](MetadataPushFrame&& f) {
    EXPECT_EQ(kMeta, getRange(*std::move(f).metadata()));
  };

  validate(makeMetadataPushFrame());
  validate(this->roundTrip(makeMetadataPushFrame()));
}

TYPED_TEST(FrameSerializationTest, KeepAliveSanity) {
  constexpr folly::StringPiece kKeepAliveData{"keep_alive_data"};

  auto makeKeepAliveFrame = [&] {
    return KeepAliveFrame(
        Flags().respond(true), folly::IOBuf::copyBuffer(kKeepAliveData));
  };

  auto validate = [=](KeepAliveFrame&& f) {
    EXPECT_TRUE(f.hasRespondFlag());
    EXPECT_EQ(kKeepAliveData, getRange(*std::move(f).data()));
  };

  validate(makeKeepAliveFrame());
  validate(this->roundTrip(makeKeepAliveFrame()));
}

TYPED_TEST(FrameSerializationTest, KeepAliveSanityEmptyPayload) {
  constexpr folly::StringPiece kKeepAliveData = folly::StringPiece();

  auto makeKeepAliveFrame = [&] {
    return KeepAliveFrame(
        Flags().respond(true), folly::IOBuf::copyBuffer(kKeepAliveData));
  };

  auto validate = [=](KeepAliveFrame&& f) {
    EXPECT_TRUE(f.hasRespondFlag());
    EXPECT_EQ(kKeepAliveData, getRange(*std::move(f).data()));
  };
  auto validateSerialized = [=](KeepAliveFrame&& f) {
    EXPECT_TRUE(f.hasRespondFlag());
    EXPECT_EQ(nullptr, std::move(f).data());
  };

  validate(makeKeepAliveFrame());
  validateSerialized(this->roundTrip(makeKeepAliveFrame()));
}

TYPED_TEST(FrameSerializationTest, PayloadLargeMetadata) {
  const auto metadata = makeLargeIOBuf();

  PayloadFrame frame(
      kTestStreamId,
      Payload::makeFromMetadataAndData(
          metadata.clone(), folly::IOBuf::copyBuffer(kData)),
      Flags().complete(true).next(true));

  auto validate = [=](const PayloadFrame& f) {
    EXPECT_EQ(kTestStreamId, f.streamId());
    EXPECT_TRUE(f.payload().hasNonemptyMetadata());
    validatePayload(
        f.payload(),
        {metadata.data(), metadata.length()},
        folly::ByteRange(kData));
  };

  validate(frame);
  validate(this->fragmentedRoundTrip(std::move(frame)));
}

TYPED_TEST(FrameSerializationTest, PayloadLargeData) {
  const auto data = makeLargeIOBuf();

  PayloadFrame frame(
      kTestStreamId,
      Payload::makeFromMetadataAndData(
          folly::IOBuf::copyBuffer(kMetadata), data.clone()),
      Flags().complete(true).next(true));

  auto validate = [=](const PayloadFrame& f) {
    EXPECT_EQ(kTestStreamId, f.streamId());
    EXPECT_TRUE(f.payload().hasNonemptyMetadata());
    validatePayload(
        f.payload(), folly::ByteRange(kMetadata), {data.data(), data.length()});
  };

  validate(frame);
  validate(this->fragmentedRoundTrip(std::move(frame)));
}

TYPED_TEST(FrameSerializationTest, RequestResponseLargeMetadata) {
  const auto metadata = makeLargeIOBuf();

  RequestResponseFrame frame(
      kTestStreamId,
      Payload::makeFromMetadataAndData(
          metadata.clone(), folly::IOBuf::copyBuffer(kData)));

  auto validate = [=](const RequestResponseFrame& f) {
    EXPECT_EQ(kTestStreamId, f.streamId());
    EXPECT_TRUE(f.payload().hasNonemptyMetadata());
    validatePayload(
        f.payload(),
        {metadata.data(), metadata.length()},
        folly::ByteRange(kData));
  };

  validate(frame);
  validate(this->fragmentedRoundTrip(std::move(frame)));
}

TYPED_TEST(FrameSerializationTest, RequestResponseLargeData) {
  const auto data = makeLargeIOBuf();

  RequestResponseFrame frame(
      kTestStreamId,
      Payload::makeFromMetadataAndData(
          folly::IOBuf::copyBuffer(kMetadata), data.clone()));

  auto validate = [=](const RequestResponseFrame& f) {
    EXPECT_EQ(kTestStreamId, f.streamId());
    EXPECT_TRUE(f.payload().hasNonemptyMetadata());
    validatePayload(
        f.payload(), folly::ByteRange(kMetadata), {data.data(), data.length()});
  };

  validate(frame);
  validate(this->fragmentedRoundTrip(std::move(frame)));
}

TYPED_TEST(FrameSerializationTest, RequestFnfLargeMetadata) {
  const auto metadata = makeLargeIOBuf();

  RequestFnfFrame frame(
      kTestStreamId,
      Payload::makeFromMetadataAndData(
          metadata.clone(), folly::IOBuf::copyBuffer(kData)));

  auto validate = [=](const RequestFnfFrame& f) {
    EXPECT_EQ(kTestStreamId, f.streamId());
    EXPECT_TRUE(f.payload().hasNonemptyMetadata());
    validatePayload(
        f.payload(),
        {metadata.data(), metadata.length()},
        folly::ByteRange(kData));
  };

  validate(frame);
  validate(this->fragmentedRoundTrip(std::move(frame)));
}

TYPED_TEST(FrameSerializationTest, RequestFnfLargeData) {
  const auto data = makeLargeIOBuf();

  RequestFnfFrame frame(
      kTestStreamId,
      Payload::makeFromMetadataAndData(
          folly::IOBuf::copyBuffer(kMetadata), data.clone()));

  auto validate = [=](const RequestFnfFrame& f) {
    EXPECT_EQ(kTestStreamId, f.streamId());
    EXPECT_TRUE(f.payload().hasNonemptyMetadata());
    validatePayload(
        f.payload(), folly::ByteRange(kMetadata), {data.data(), data.length()});
  };

  validate(frame);
  validate(this->fragmentedRoundTrip(std::move(frame)));
}

TYPED_TEST(FrameSerializationTest, RequestStreamLargeMetadata) {
  const auto metadata = makeLargeIOBuf();

  RequestStreamFrame frame(
      kTestStreamId,
      Payload::makeFromMetadataAndData(
          metadata.clone(), folly::IOBuf::copyBuffer(kData)),
      123 /* initialRequestN */);

  auto validate = [=](const RequestStreamFrame& f) {
    EXPECT_EQ(kTestStreamId, f.streamId());
    EXPECT_TRUE(f.payload().hasNonemptyMetadata());
    validatePayload(
        f.payload(),
        {metadata.data(), metadata.length()},
        folly::ByteRange(kData));
  };

  validate(frame);
  validate(this->fragmentedRoundTrip(std::move(frame)));
}

TYPED_TEST(FrameSerializationTest, RequestStreamLargeData) {
  const auto data = makeLargeIOBuf();

  RequestStreamFrame frame(
      kTestStreamId,
      Payload::makeFromMetadataAndData(
          folly::IOBuf::copyBuffer(kMetadata), data.clone()),
      123 /* initialRequestN */);

  auto validate = [=](const RequestStreamFrame& f) {
    EXPECT_EQ(kTestStreamId, f.streamId());
    EXPECT_TRUE(f.payload().hasNonemptyMetadata());
    validatePayload(
        f.payload(), folly::ByteRange(kMetadata), {data.data(), data.length()});
  };

  validate(frame);
  validate(this->fragmentedRoundTrip(std::move(frame)));
}

TYPED_TEST(FrameSerializationTest, RequestChannelLargeMetadata) {
  const auto metadata = makeLargeIOBuf();

  RequestChannelFrame frame(
      kTestStreamId,
      Payload::makeFromMetadataAndData(
          metadata.clone(), folly::IOBuf::copyBuffer(kData)),
      123 /* initialRequestN */);
  frame.setHasComplete(true);

  auto validate = [=](const RequestChannelFrame& f) {
    EXPECT_EQ(kTestStreamId, f.streamId());
    EXPECT_TRUE(f.payload().hasNonemptyMetadata());
    validatePayload(
        f.payload(),
        {metadata.data(), metadata.length()},
        folly::ByteRange(kData));
    EXPECT_TRUE(f.hasComplete());
  };

  validate(frame);
  validate(this->fragmentedRoundTrip(std::move(frame)));
}

TYPED_TEST(FrameSerializationTest, RequestChannelLargeData) {
  const auto data = makeLargeIOBuf();

  RequestChannelFrame frame(
      kTestStreamId,
      Payload::makeFromMetadataAndData(
          folly::IOBuf::copyBuffer(kMetadata), data.clone()),
      123 /* initialRequestN */);
  frame.setHasComplete(true);

  auto validate = [=](const RequestChannelFrame& f) {
    EXPECT_EQ(kTestStreamId, f.streamId());
    EXPECT_TRUE(f.payload().hasNonemptyMetadata());
    validatePayload(
        f.payload(), folly::ByteRange(kMetadata), {data.data(), data.length()});
    EXPECT_TRUE(f.hasComplete());
  };

  validate(frame);
  validate(this->fragmentedRoundTrip(std::move(frame)));
}

TEST(FrameSerialization, PayloadFrameSerializeAPI) {
  auto validate = [](std::unique_ptr<folly::IOBuf> md,
                     std::unique_ptr<folly::IOBuf> data) {
    folly::IOBufEqualTo eq;
    PayloadFrame frame1(
        kTestStreamId,
        Payload::makeFromMetadataAndData(md->clone(), data->clone()),
        Flags().follows(true).complete(true).next(true));
    PayloadFrame frame2(
        kTestStreamId,
        Payload::makeFromMetadataAndData(md->clone(), data->clone()),
        Flags().follows(true).complete(true).next(true));
    auto altApi = std::move(frame1).serialize();
    Serializer writer;
    std::move(frame2).serialize(writer);
    auto regApi = std::move(writer).move();
    EXPECT_TRUE(eq(altApi, regApi));
  };

  // Base, No headroom in metadata.
  validate(
      folly::IOBuf::wrapBuffer(kMetadata), folly::IOBuf::wrapBuffer(kData));
  // Base, with headroom.
  validate(
      folly::IOBuf::wrapBuffer(kMetadata)->cloneCoalescedWithHeadroomTailroom(
          16, 256),
      folly::IOBuf::wrapBuffer(kData));

  // Bigger metadata, no headroom.
  constexpr size_t kBigish = 8 << 10;
  auto bigIOB = folly::IOBuf::create(kBigish);
  bigIOB->append(kBigish);
  validate(std::move(bigIOB), folly::IOBuf::wrapBuffer(kData));

  // Bigger metadata, with headroom.
  auto bigIOBWithHeadroom = folly::IOBuf::create(kBigish);
  bigIOBWithHeadroom->advance(16);
  bigIOBWithHeadroom->append(kBigish - 16);
  validate(std::move(bigIOBWithHeadroom), folly::IOBuf::wrapBuffer(kData));

  // Empty metadata
  validate(std::make_unique<folly::IOBuf>(), folly::IOBuf::wrapBuffer(kData));
}

TYPED_TEST(FrameSerializationTest, ExtUnknownSanity) {
  ExtFrame frame(
      kTestStreamId,
      Payload::makeFromMetadataAndData(kMetadata, kData),
      Flags().ignore(true),
      static_cast<ExtFrameType>(42));

  auto validate = [](const ExtFrame& f) {
    EXPECT_EQ(kTestStreamId, f.streamId());
    EXPECT_EQ(ExtFrameType::UNKNOWN, f.extFrameType());
    EXPECT_TRUE(f.hasIgnore());
    validateMetadataAndData(f.payload());
  };

  validate(this->roundTrip(std::move(frame)));
}

TYPED_TEST(FrameSerializationTest, ExtEmptyMetadataSanity) {
  auto validate = [](const ExtFrame& f) {
    EXPECT_EQ(kTestStreamId, f.streamId());
    EXPECT_EQ(ExtFrameType::UNKNOWN, f.extFrameType());
    EXPECT_TRUE(f.hasIgnore());
    EXPECT_FALSE(f.payload().hasNonemptyMetadata());
    validatePayload(f.payload(), {}, folly::ByteRange(kData));
  };

  // No metadata
  {
    ExtFrame frame(
        kTestStreamId,
        Payload::makeFromData(kData),
        Flags().ignore(true),
        ExtFrameType::UNKNOWN);

    validate(frame);
    validate(this->roundTrip(std::move(frame)));
  }

  // Empty metadata
  {
    ExtFrame frame(
        kTestStreamId,
        Payload::makeFromMetadataAndData(
            folly::ByteRange{folly::StringPiece{""}}, kData),
        Flags().ignore(true),
        ExtFrameType::UNKNOWN);

    validate(frame);
    validate(this->roundTrip(std::move(frame)));
  }
}

} // namespace apache::thrift::rocket
