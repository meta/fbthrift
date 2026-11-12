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

#include <thrift/lib/cpp2/fast_thrift/thrift/server/handler/ThriftServerCompressionHandler.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include <folly/ExceptionWrapper.h>
#include <folly/io/IOBuf.h>
#include <folly/portability/GMock.h>

#include <folly/compression/Compression.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/ConnectionPayloads.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/Messages.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/ThriftRequestContext.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/handler/ThriftServerChecksumHandler.h>
#include <thrift/lib/cpp2/protocol/CompactProtocol.h>
#include <thrift/lib/cpp2/transport/rocket/ChecksumGenerator.h>
#include <thrift/lib/thrift/gen-cpp2/RpcMetadata_types.h>

namespace apache::thrift::fast_thrift::thrift {
namespace {

using channel_pipeline::erase_and_box;
using channel_pipeline::Result;
using channel_pipeline::TypeErasedBox;
using namespace testing;

constexpr uint32_t kStreamId = 7;
constexpr std::string_view kRequestBody = "request payload bytes";

folly::EventBase& requestContextEventBase() {
  static folly::EventBase eventBase;
  return eventBase;
}

class FakeContext {
 public:
  Result fireRead(TypeErasedBox&& msg) noexcept {
    forwarded.push_back(std::move(msg));
    return Result::Success;
  }

  Result fireWrite(TypeErasedBox&& msg) noexcept {
    written.push_back(std::move(msg));
    return writeResult;
  }

  void fireException(folly::exception_wrapper&& e) noexcept {
    exception = std::move(e);
  }

  std::vector<TypeErasedBox> forwarded;
  std::vector<TypeErasedBox> written;
  folly::exception_wrapper exception;
  Result writeResult{Result::Success};
};

std::pair<folly::compression::CodecType, int> testCodecFor(
    apache::thrift::CompressionAlgorithm compression) {
  switch (compression) {
    case apache::thrift::CompressionAlgorithm::ZLIB:
      return {
          folly::compression::CodecType::ZLIB,
          folly::compression::COMPRESSION_LEVEL_DEFAULT};
    case apache::thrift::CompressionAlgorithm::ZLIB_LESS:
      return {
          folly::compression::CodecType::ZLIB,
          folly::compression::COMPRESSION_LEVEL_FASTEST};
    case apache::thrift::CompressionAlgorithm::ZSTD:
      return {
          folly::compression::CodecType::ZSTD,
          folly::compression::COMPRESSION_LEVEL_DEFAULT};
    case apache::thrift::CompressionAlgorithm::ZSTD_MORE:
      return {
          folly::compression::CodecType::ZSTD,
          folly::compression::COMPRESSION_LEVEL_BEST};
    case apache::thrift::CompressionAlgorithm::LZ4:
      return {
          folly::compression::CodecType::LZ4_VARINT_SIZE,
          folly::compression::COMPRESSION_LEVEL_DEFAULT};
    case apache::thrift::CompressionAlgorithm::NONE:
    case apache::thrift::CompressionAlgorithm::CUSTOM:
    case apache::thrift::CompressionAlgorithm::ZSTD_LESS:
    case apache::thrift::CompressionAlgorithm::LZ4_LESS:
    case apache::thrift::CompressionAlgorithm::LZ4_MORE:
    case apache::thrift::CompressionAlgorithm::ZLIB_MORE:
    default:
      throw std::invalid_argument("unsupported test compression");
  }
}

std::unique_ptr<folly::IOBuf> compressForTest(
    std::unique_ptr<folly::IOBuf> data,
    apache::thrift::CompressionAlgorithm compression) {
  const auto [codec, level] = testCodecFor(compression);
  return folly::compression::getCodec(codec, level)->compress(data.get());
}

std::unique_ptr<folly::IOBuf> uncompressForTest(
    const folly::IOBuf& data,
    apache::thrift::CompressionAlgorithm compression) {
  const auto [codec, level] = testCodecFor(compression);
  return folly::compression::getCodec(codec, level)->uncompress(&data);
}

apache::thrift::CompressionConfig makeCompressionConfig(
    apache::thrift::CompressionAlgorithm compression,
    size_t compressionSizeLimit = 0) {
  apache::thrift::CompressionConfig config;
  switch (compression) {
    case apache::thrift::CompressionAlgorithm::ZLIB:
      config.codecConfig().ensure().zlibConfig().ensure();
      break;
    case apache::thrift::CompressionAlgorithm::ZLIB_LESS:
      config.codecConfig().ensure().zlibConfig().ensure().levelPreset() =
          apache::thrift::ZlibCompressionLevelPreset::LESS;
      break;
    case apache::thrift::CompressionAlgorithm::ZSTD:
      config.codecConfig().ensure().zstdConfig().ensure();
      break;
    case apache::thrift::CompressionAlgorithm::ZSTD_MORE:
      config.codecConfig().ensure().zstdConfig().ensure().levelPreset() =
          apache::thrift::ZstdCompressionLevelPreset::MORE;
      break;
    case apache::thrift::CompressionAlgorithm::LZ4:
      config.codecConfig().ensure().lz4Config().ensure();
      break;
    case apache::thrift::CompressionAlgorithm::CUSTOM:
      config.codecConfig().ensure().customConfig().ensure();
      break;
    case apache::thrift::CompressionAlgorithm::NONE:
    case apache::thrift::CompressionAlgorithm::ZSTD_LESS:
    case apache::thrift::CompressionAlgorithm::LZ4_LESS:
    case apache::thrift::CompressionAlgorithm::LZ4_MORE:
    case apache::thrift::CompressionAlgorithm::ZLIB_MORE:
    default:
      throw std::invalid_argument("unsupported test compression config");
  }
  config.compressionSizeLimit() = static_cast<int64_t>(compressionSizeLimit);
  return config;
}

std::unique_ptr<apache::thrift::RequestRpcMetadata> makeMetadata(
    std::optional<apache::thrift::CompressionAlgorithm> compression =
        std::nullopt,
    std::optional<apache::thrift::CompressionConfig> responseCompression =
        std::nullopt) {
  auto metadata = std::make_unique<apache::thrift::RequestRpcMetadata>();
  if (compression.has_value()) {
    metadata->compression() = *compression;
  }
  if (responseCompression.has_value()) {
    metadata->compressionConfig() = std::move(*responseCompression);
  }
  return metadata;
}

ThriftServerRequestMessage makeRequest(
    std::unique_ptr<apache::thrift::RequestRpcMetadata> metadata,
    std::unique_ptr<folly::IOBuf> data =
        folly::IOBuf::copyBuffer(kRequestBody)) {
  return ThriftServerRequestMessage{
      .requestContext = makeThriftRequestContext(requestContextEventBase()),
      .payload =
          ThriftServerRequestResponsePayload{
              .data = std::move(data),
              .metadata = metadata == nullptr
                  ? std::unique_ptr<apache::thrift::RequestRpcMetadata>{}
                  : makeServerRequestMetadata(
                        requestContextEventBase(), std::move(*metadata)),
          },
      .streamId = kStreamId,
  };
}

ThriftServerResponseMessage makeResponse(
    uint32_t streamId = kStreamId, std::string_view data = kRequestBody) {
  return ThriftServerResponseMessage{
      .payload = ThriftInitialResponsePayload{
          .data = folly::IOBuf::copyBuffer(data),
          .metadata = std::make_unique<apache::thrift::ResponseRpcMetadata>(),
          .streamId = streamId,
      }};
}

const ThriftInitialResponsePayload& writtenResponsePayload(
    const FakeContext& ctx) {
  return ctx.written.back()
      .get<ThriftServerResponseMessage>()
      .payload.get<ThriftInitialResponsePayload>();
}

const ThriftServerRequestResponsePayload& forwardedPayload(
    const FakeContext& ctx) {
  return ctx.forwarded.front()
      .get<ThriftServerRequestMessage>()
      .payload.get<ThriftServerRequestResponsePayload>();
}

ThriftServerResponseMessage makeResponseForForwardedRequest(FakeContext& ctx) {
  auto response = makeResponse();
  auto& request = ctx.forwarded.front().get<ThriftServerRequestMessage>();
  response.requestContext = std::move(request.requestContext);
  ctx.forwarded.clear();
  return response;
}

apache::thrift::ResponseRpcError decodeError(const folly::IOBuf& data) {
  apache::thrift::CompactProtocolReader reader;
  reader.setInput(&data);
  apache::thrift::ResponseRpcError error;
  error.read(&reader);
  return error;
}

void expectParsingFailure(
    const FakeContext& ctx, std::string_view expectedMessage) {
  EXPECT_TRUE(ctx.forwarded.empty());
  ASSERT_EQ(ctx.written.size(), 1);
  const auto& response = ctx.written.front().get<ThriftServerResponseMessage>();
  EXPECT_NE(response.requestContext, nullptr);
  ASSERT_TRUE(response.payload.is<ThriftErrorPayload>());
  const auto& errorPayload = response.payload.get<ThriftErrorPayload>();
  EXPECT_EQ(errorPayload.streamId, kStreamId);
  ASSERT_NE(errorPayload.data, nullptr);
  const auto error = decodeError(*errorPayload.data);
  EXPECT_EQ(
      *error.code(),
      apache::thrift::ResponseRpcErrorCode::REQUEST_PARSING_FAILURE);
  EXPECT_NE(error.what_utf8()->find(expectedMessage), std::string::npos);
}

void expectDecompresses(apache::thrift::CompressionAlgorithm compression) {
  auto compressed =
      compressForTest(folly::IOBuf::copyBuffer(kRequestBody), compression);
  ThriftServerCompressionHandler<FakeContext> handler;
  FakeContext ctx;

  EXPECT_EQ(
      handler.onRead(
          ctx,
          erase_and_box(
              makeRequest(makeMetadata(compression), std::move(compressed)))),
      Result::Success);

  ASSERT_EQ(ctx.forwarded.size(), 1);
  EXPECT_TRUE(ctx.written.empty());
  const auto& payload = forwardedPayload(ctx);
  ASSERT_NE(payload.data, nullptr);
  EXPECT_EQ(payload.data->to<std::string>(), kRequestBody);
  EXPECT_FALSE(payload.metadata->compression().has_value());
}

void expectDecompressionLimit(
    apache::thrift::CompressionAlgorithm compression) {
  constexpr size_t kLimit = 32;
  const std::string atLimit(kLimit, 'a');
  auto compressed =
      compressForTest(folly::IOBuf::copyBuffer(atLimit), compression);
  ThriftServerCompressionHandler<FakeContext> handler{kLimit};
  FakeContext ctx;

  EXPECT_EQ(
      handler.onRead(
          ctx,
          erase_and_box(
              makeRequest(makeMetadata(compression), std::move(compressed)))),
      Result::Success);
  ASSERT_EQ(ctx.forwarded.size(), 1);
  EXPECT_EQ(forwardedPayload(ctx).data->to<std::string>(), atLimit);
  EXPECT_TRUE(ctx.written.empty());

  const std::string overLimit(kLimit + 1, 'a');
  compressed =
      compressForTest(folly::IOBuf::copyBuffer(overLimit), compression);
  ThriftServerCompressionHandler<FakeContext> rejectingHandler{kLimit};
  FakeContext rejectingCtx;

  EXPECT_EQ(
      rejectingHandler.onRead(
          rejectingCtx,
          erase_and_box(
              makeRequest(makeMetadata(compression), std::move(compressed)))),
      Result::Success);
  expectParsingFailure(rejectingCtx, "exceeds size limit");
}

void expectCompressesResponse(
    apache::thrift::CompressionAlgorithm compression) {
  ThriftServerCompressionHandler<FakeContext> handler;
  FakeContext ctx;

  EXPECT_EQ(
      handler.onRead(
          ctx,
          erase_and_box(makeRequest(
              makeMetadata(std::nullopt, makeCompressionConfig(compression))))),
      Result::Success);
  EXPECT_FALSE(forwardedPayload(ctx).metadata->compressionConfig().has_value());
  auto response = makeResponseForForwardedRequest(ctx);

  EXPECT_EQ(
      handler.onWrite(ctx, erase_and_box(std::move(response))),
      Result::Success);

  ASSERT_EQ(ctx.written.size(), 1);
  const auto& payload = writtenResponsePayload(ctx);
  ASSERT_NE(payload.metadata, nullptr);
  ASSERT_TRUE(payload.metadata->compression().has_value());
  EXPECT_EQ(*payload.metadata->compression(), compression);
  ASSERT_NE(payload.data, nullptr);
  EXPECT_EQ(
      uncompressForTest(*payload.data, compression)->to<std::string>(),
      kRequestBody);
}

} // namespace

TEST(ThriftServerCompressionHandlerTest, SetupPayloadPassesThrough) {
  ThriftServerRequestMessage request;
  request.payload = ThriftConnectionSetupPayload{
      .setup = std::make_unique<ConnectionSetupData>()};
  ThriftServerCompressionHandler<FakeContext> handler;
  FakeContext ctx;

  EXPECT_EQ(
      handler.onRead(ctx, erase_and_box(std::move(request))), Result::Success);

  ASSERT_EQ(ctx.forwarded.size(), 1);
  EXPECT_TRUE(ctx.forwarded.front()
                  .get<ThriftServerRequestMessage>()
                  .payload.is<ThriftConnectionSetupPayload>());
  EXPECT_TRUE(ctx.written.empty());
}

TEST(ThriftServerCompressionHandlerTest, MissingMetadataPassesThrough) {
  ThriftServerCompressionHandler<FakeContext> handler;
  FakeContext ctx;

  EXPECT_EQ(
      handler.onRead(ctx, erase_and_box(makeRequest(nullptr))),
      Result::Success);

  ASSERT_EQ(ctx.forwarded.size(), 1);
  EXPECT_EQ(forwardedPayload(ctx).metadata, nullptr);
  EXPECT_TRUE(ctx.written.empty());
}

TEST(ThriftServerCompressionHandlerTest, MissingCompressedDataIsRejected) {
  ThriftServerCompressionHandler<FakeContext> handler;
  FakeContext ctx;
  auto request = makeRequest(
      makeMetadata(apache::thrift::CompressionAlgorithm::ZLIB), nullptr);
  auto* const requestContext = request.requestContext.get();

  EXPECT_EQ(
      handler.onRead(ctx, erase_and_box(std::move(request))), Result::Success);

  expectParsingFailure(ctx, "missing request data");
  EXPECT_EQ(
      ctx.written.front()
          .get<ThriftServerResponseMessage>()
          .requestContext.get(),
      requestContext);
}

TEST(ThriftServerCompressionHandlerTest, BackpressuredRejectionReturnsSuccess) {
  ThriftServerCompressionHandler<FakeContext> handler;
  FakeContext ctx;
  ctx.writeResult = Result::Backpressure;

  EXPECT_THAT(
      handler.onRead(
          ctx,
          erase_and_box(makeRequest(
              makeMetadata(apache::thrift::CompressionAlgorithm::ZLIB),
              nullptr))),
      Eq(Result::Success));
}

TEST(ThriftServerCompressionHandlerTest, FailedRejectionReturnsError) {
  ThriftServerCompressionHandler<FakeContext> handler;
  FakeContext ctx;
  ctx.writeResult = Result::Error;

  EXPECT_THAT(
      handler.onRead(
          ctx,
          erase_and_box(makeRequest(
              makeMetadata(apache::thrift::CompressionAlgorithm::ZLIB),
              nullptr))),
      Eq(Result::Error));
}

TEST(ThriftServerCompressionHandlerTest, NoCompressionPassesThrough) {
  ThriftServerCompressionHandler<FakeContext> handler;
  FakeContext ctx;

  EXPECT_EQ(
      handler.onRead(ctx, erase_and_box(makeRequest(makeMetadata()))),
      Result::Success);

  ASSERT_EQ(ctx.forwarded.size(), 1);
  EXPECT_EQ(forwardedPayload(ctx).data->to<std::string>(), kRequestBody);
  EXPECT_FALSE(forwardedPayload(ctx).metadata->compression().has_value());
  EXPECT_TRUE(ctx.written.empty());
}

TEST(ThriftServerCompressionHandlerTest, NoneCompressionPassesThrough) {
  ThriftServerCompressionHandler<FakeContext> handler;
  FakeContext ctx;

  EXPECT_EQ(
      handler.onRead(
          ctx,
          erase_and_box(makeRequest(
              makeMetadata(apache::thrift::CompressionAlgorithm::NONE)))),
      Result::Success);

  ASSERT_EQ(ctx.forwarded.size(), 1);
  EXPECT_EQ(forwardedPayload(ctx).data->to<std::string>(), kRequestBody);
  EXPECT_FALSE(forwardedPayload(ctx).metadata->compression().has_value());
  EXPECT_TRUE(ctx.written.empty());
}

TEST(ThriftServerCompressionHandlerTest, DecompressesZlibRequest) {
  expectDecompresses(apache::thrift::CompressionAlgorithm::ZLIB);
}

TEST(ThriftServerCompressionHandlerTest, DecompressesZstdRequest) {
  expectDecompresses(apache::thrift::CompressionAlgorithm::ZSTD);
}

TEST(ThriftServerCompressionHandlerTest, DecompressesZlibLessRequest) {
  expectDecompresses(apache::thrift::CompressionAlgorithm::ZLIB_LESS);
}

TEST(ThriftServerCompressionHandlerTest, DecompressesLz4Request) {
  if (!folly::compression::hasCodec(
          folly::compression::CodecType::LZ4_VARINT_SIZE)) {
    GTEST_SKIP();
  }
  expectDecompresses(apache::thrift::CompressionAlgorithm::LZ4);
}

TEST(
    ThriftServerCompressionHandlerTest,
    EnforcesZlibDecompressedRequestSizeLimit) {
  expectDecompressionLimit(apache::thrift::CompressionAlgorithm::ZLIB);
}

TEST(
    ThriftServerCompressionHandlerTest,
    EnforcesZstdDecompressedRequestSizeLimit) {
  expectDecompressionLimit(apache::thrift::CompressionAlgorithm::ZSTD);
}

TEST(
    ThriftServerCompressionHandlerTest,
    EnforcesLz4DecompressedRequestSizeLimit) {
  if (!folly::compression::hasCodec(
          folly::compression::CodecType::LZ4_VARINT_SIZE)) {
    GTEST_SKIP();
  }
  expectDecompressionLimit(apache::thrift::CompressionAlgorithm::LZ4);
}

TEST(
    ThriftServerCompressionHandlerTest,
    TruncatedZlibRequestAtSizeLimitIsRejected) {
  constexpr size_t kLimit = 32;
  auto compressed = compressForTest(
      folly::IOBuf::copyBuffer(std::string(kLimit, 'a')),
      apache::thrift::CompressionAlgorithm::ZLIB);
  compressed->coalesce();
  ASSERT_GT(compressed->length(), 1);
  compressed->trimEnd(1);
  ThriftServerCompressionHandler<FakeContext> handler{kLimit};
  FakeContext ctx;

  EXPECT_EQ(
      handler.onRead(
          ctx,
          erase_and_box(makeRequest(
              makeMetadata(apache::thrift::CompressionAlgorithm::ZLIB),
              std::move(compressed)))),
      Result::Success);

  expectParsingFailure(ctx, "Request decompression failed");
}

TEST(ThriftServerCompressionHandlerTest, CustomCompressionIsRejected) {
  ThriftServerCompressionHandler<FakeContext> handler;
  FakeContext ctx;

  EXPECT_EQ(
      handler.onRead(
          ctx,
          erase_and_box(makeRequest(
              makeMetadata(apache::thrift::CompressionAlgorithm::CUSTOM)))),
      Result::Success);

  expectParsingFailure(ctx, "CUSTOM request compression");
}

TEST(ThriftServerCompressionHandlerTest, InvalidCompressedDataIsRejected) {
  ThriftServerCompressionHandler<FakeContext> handler;
  FakeContext ctx;

  EXPECT_EQ(
      handler.onRead(
          ctx,
          erase_and_box(makeRequest(
              makeMetadata(apache::thrift::CompressionAlgorithm::ZLIB),
              folly::IOBuf::copyBuffer("not compressed")))),
      Result::Success);

  expectParsingFailure(ctx, "Request decompression failed");
}

TEST(ThriftServerCompressionHandlerTest, CompressesZlibResponse) {
  expectCompressesResponse(apache::thrift::CompressionAlgorithm::ZLIB);
}

TEST(ThriftServerCompressionHandlerTest, CompressesZstdResponse) {
  expectCompressesResponse(apache::thrift::CompressionAlgorithm::ZSTD);
}

TEST(ThriftServerCompressionHandlerTest, CompressesZstdMoreResponse) {
  expectCompressesResponse(apache::thrift::CompressionAlgorithm::ZSTD_MORE);
}

TEST(ThriftServerCompressionHandlerTest, CompressesLz4Response) {
  if (!folly::compression::hasCodec(
          folly::compression::CodecType::LZ4_VARINT_SIZE)) {
    GTEST_SKIP();
  }
  expectCompressesResponse(apache::thrift::CompressionAlgorithm::LZ4);
}

TEST(ThriftServerCompressionHandlerTest, HonorsResponseCompressionThreshold) {
  ThriftServerCompressionHandler<FakeContext> handler;
  FakeContext ctx;
  auto config = makeCompressionConfig(
      apache::thrift::CompressionAlgorithm::ZLIB, kRequestBody.size());

  ASSERT_EQ(
      handler.onRead(
          ctx,
          erase_and_box(
              makeRequest(makeMetadata(std::nullopt, std::move(config))))),
      Result::Success);
  auto response = makeResponseForForwardedRequest(ctx);

  EXPECT_EQ(
      handler.onWrite(ctx, erase_and_box(std::move(response))),
      Result::Success);

  ASSERT_EQ(ctx.written.size(), 1);
  const auto& payload = writtenResponsePayload(ctx);
  EXPECT_FALSE(payload.metadata->compression().has_value());
  EXPECT_EQ(payload.data->to<std::string>(), kRequestBody);
}

TEST(ThriftServerCompressionHandlerTest, CustomResponsePreferenceFallsBack) {
  ThriftServerCompressionHandler<FakeContext> handler;
  FakeContext ctx;

  ASSERT_EQ(
      handler.onRead(
          ctx,
          erase_and_box(makeRequest(makeMetadata(
              std::nullopt,
              makeCompressionConfig(
                  apache::thrift::CompressionAlgorithm::CUSTOM))))),
      Result::Success);
  auto response = makeResponseForForwardedRequest(ctx);

  EXPECT_EQ(
      handler.onWrite(ctx, erase_and_box(std::move(response))),
      Result::Success);

  ASSERT_EQ(ctx.written.size(), 1);
  const auto& payload = writtenResponsePayload(ctx);
  EXPECT_FALSE(payload.metadata->compression().has_value());
  EXPECT_EQ(payload.data->to<std::string>(), kRequestBody);
}

TEST(
    ThriftServerCompressionHandlerTest,
    InvalidResponsePreferencePreservesRequestContextOnError) {
  ThriftServerCompressionHandler<FakeContext> handler;
  FakeContext ctx;
  auto config =
      makeCompressionConfig(apache::thrift::CompressionAlgorithm::ZLIB);
  config.codecConfig()->zlibConfig()->levelPreset() =
      static_cast<apache::thrift::ZlibCompressionLevelPreset>(127);

  ASSERT_EQ(
      handler.onRead(
          ctx,
          erase_and_box(
              makeRequest(makeMetadata(std::nullopt, std::move(config))))),
      Result::Success);
  auto response = makeResponseForForwardedRequest(ctx);
  auto* const requestContext = response.requestContext.get();

  EXPECT_EQ(
      handler.onWrite(ctx, erase_and_box(std::move(response))),
      Result::Success);

  ASSERT_EQ(ctx.written.size(), 1);
  const auto& error = ctx.written.front().get<ThriftServerResponseMessage>();
  EXPECT_TRUE(error.payload.is<ThriftErrorPayload>());
  EXPECT_EQ(error.requestContext.get(), requestContext);
}

TEST(ThriftServerCompressionHandlerTest, AdvertisesAvailableStandardCodecs) {
  ThriftServerCompressionHandler<FakeContext> handler;
  FakeContext ctx;
  auto setupResponse = std::make_unique<apache::thrift::SetupResponse>();
  setupResponse->zstdSupported() = false;
  setupResponse->lz4Supported() = false;
  ThriftServerResponseMessage response{
      .payload =
          ThriftSetupResponsePayload{.response = std::move(setupResponse)}};

  EXPECT_EQ(
      handler.onWrite(ctx, erase_and_box(std::move(response))),
      Result::Success);

  ASSERT_EQ(ctx.written.size(), 1);
  const auto& setup = ctx.written.front()
                          .get<ThriftServerResponseMessage>()
                          .payload.get<ThriftSetupResponsePayload>();
  ASSERT_NE(setup.response, nullptr);
  ASSERT_TRUE(setup.response->zstdSupported().has_value());
  EXPECT_EQ(
      *setup.response->zstdSupported(),
      folly::compression::hasCodec(folly::compression::CodecType::ZSTD));
  ASSERT_TRUE(setup.response->lz4Supported().has_value());
  EXPECT_EQ(
      *setup.response->lz4Supported(),
      folly::compression::hasCodec(
          folly::compression::CodecType::LZ4_VARINT_SIZE));
}

TEST(
    ThriftServerCompressionHandlerTest,
    ResponseWithoutRequestContextPassesThrough) {
  ThriftServerCompressionHandler<FakeContext> handler;
  FakeContext ctx;

  EXPECT_EQ(
      handler.onWrite(ctx, erase_and_box(makeResponse())), Result::Success);

  ASSERT_EQ(ctx.written.size(), 1);
  const auto& payload = writtenResponsePayload(ctx);
  EXPECT_FALSE(payload.metadata->compression().has_value());
  EXPECT_EQ(payload.data->to<std::string>(), kRequestBody);
}

TEST(ThriftServerCompressionHandlerTest, OutboundPassesThrough) {
  ThriftServerResponseMessage response{
      .payload = ThriftErrorPayload{
          .data = nullptr,
          .metadata = nullptr,
          .streamId = kStreamId,
          .errorCode = 0,
      }};
  ThriftServerCompressionHandler<FakeContext> handler;
  FakeContext ctx;

  EXPECT_EQ(
      handler.onWrite(ctx, erase_and_box(std::move(response))),
      Result::Success);

  ASSERT_EQ(ctx.written.size(), 1);
  EXPECT_TRUE(ctx.written.front()
                  .get<ThriftServerResponseMessage>()
                  .payload.is<ThriftErrorPayload>());
}

TEST(ThriftServerCompressionHandlerTest, DecompressesBeforeChecksumValidation) {
  auto uncompressed = folly::IOBuf::copyBuffer(kRequestBody);
  constexpr int64_t kSalt = 0x1234abcd;
  const auto expected = apache::thrift::rocket::ChecksumGenerator<
                            apache::thrift::rocket::XXH3_64>{}
                            .calculateChecksumFromIOBuf(*uncompressed, kSalt);
  auto metadata = makeMetadata(apache::thrift::CompressionAlgorithm::ZLIB);
  apache::thrift::Checksum checksum;
  checksum.algorithm() = apache::thrift::ChecksumAlgorithm::XXH3_64;
  checksum.checksum() = expected.checksum;
  checksum.salt() = kSalt;
  metadata->checksum() = checksum;
  auto request = makeRequest(
      std::move(metadata),
      compressForTest(
          std::move(uncompressed), apache::thrift::CompressionAlgorithm::ZLIB));
  ThriftServerCompressionHandler<FakeContext> compressionHandler;
  ThriftServerChecksumHandler<FakeContext> checksumHandler;
  FakeContext ctx;

  ASSERT_EQ(
      compressionHandler.onRead(ctx, erase_and_box(std::move(request))),
      Result::Success);
  ASSERT_EQ(ctx.forwarded.size(), 1);
  auto decompressedRequest = std::move(ctx.forwarded.front());
  ctx.forwarded.clear();

  EXPECT_EQ(
      checksumHandler.onRead(ctx, std::move(decompressedRequest)),
      Result::Success);

  ASSERT_EQ(ctx.forwarded.size(), 1);
  EXPECT_TRUE(ctx.written.empty());
  EXPECT_EQ(
      ctx.forwarded.front()
          .get<ThriftServerRequestMessage>()
          .requestContext->getChecksumAlgorithm(),
      apache::thrift::ChecksumAlgorithm::XXH3_64);
}

TEST(ThriftServerCompressionHandlerTest, ForwardsExceptions) {
  ThriftServerCompressionHandler<FakeContext> handler;
  FakeContext ctx;

  handler.onException(
      ctx, folly::make_exception_wrapper<std::runtime_error>("boom"));

  ASSERT_TRUE(ctx.exception);
  EXPECT_NE(ctx.exception.what().toStdString().find("boom"), std::string::npos);
}

} // namespace apache::thrift::fast_thrift::thrift
