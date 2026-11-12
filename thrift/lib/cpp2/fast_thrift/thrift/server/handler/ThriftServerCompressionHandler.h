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

#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>

#include <folly/ExceptionWrapper.h>
#include <folly/Range.h>
#include <folly/Varint.h>
#include <folly/compression/Compression.h>
#include <folly/io/Cursor.h>
#include <folly/io/IOBuf.h>
#include <folly/io/IOBufQueue.h>
#include <folly/lang/Hint.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Handler.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/ContextImpl.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/Messages.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/ThriftRequestContext.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/util/ResponsePayloads.h>
#include <thrift/lib/thrift/gen-cpp2/RpcMetadata_types.h>

namespace apache::thrift::fast_thrift::thrift {

/**
 * Applies standard Rocket compression in both directions.
 *
 * Inbound request data is decompressed before it reaches the application. The
 * response compression preference is stored in the request context, which
 * already follows the request through asynchronous response completion.
 *
 * Pipeline order: this handler must sit immediately before the checksum
 * handler. Inbound data is therefore decompressed before checksum validation;
 * outbound data is checksummed before compression.
 */
template <typename Context>
class ThriftServerCompressionHandler {
 public:
  static constexpr size_t kDefaultMaxDecompressedRequestSize = 16 * 1024 * 1024;

  explicit ThriftServerCompressionHandler(
      size_t maxDecompressedRequestSize =
          kDefaultMaxDecompressedRequestSize) noexcept
      : maxDecompressedRequestSize_(maxDecompressedRequestSize) {}

  void handlerAdded(Context& /*ctx*/) noexcept {}
  void handlerRemoved(Context& /*ctx*/) noexcept {}

  channel_pipeline::Result onRead(
      Context& ctx, channel_pipeline::TypeErasedBox&& msg) noexcept {
    auto& request = msg.get<ThriftServerRequestMessage>();
    if (!request.payload.template is<ThriftServerRequestResponsePayload>())
        [[unlikely]] {
      return ctx.fireRead(std::move(msg));
    }

    const auto streamId = request.streamId;
    auto& payload =
        request.payload.template get<ThriftServerRequestResponsePayload>();
    if (auto error = decompressRequest(payload)) {
      // On Backpressure the pipeline accepted the REQUEST_PARSING_FAILURE
      // response. As a read result, Backpressure would pause reads on the whole
      // connection.
      if (writeParsingFailure(
              ctx,
              streamId,
              std::move(request.requestContext),
              std::move(*error)) == channel_pipeline::Result::Error) {
        return channel_pipeline::Result::Error;
      }
      return channel_pipeline::Result::Success;
    }

    if (payload.metadata != nullptr) {
      if (auto config = payload.metadata->compressionConfig()) {
        auto responseCompressionConfig = std::move(*config);
        payload.metadata->compressionConfig().reset();
        request.requestContext->setResponseCompressionConfig(
            std::move(responseCompressionConfig));
      }
    }

    return ctx.fireRead(std::move(msg));
  }

  void onReadReady(Context& /*ctx*/) noexcept {}

  void onException(Context& ctx, folly::exception_wrapper&& e) noexcept {
    ctx.fireException(std::move(e));
  }

  void onPipelineActive(Context& /*ctx*/) noexcept {}

  channel_pipeline::Result onWrite(
      Context& ctx, channel_pipeline::TypeErasedBox&& msg) noexcept {
    auto& response = msg.get<ThriftServerResponseMessage>();

    if (response.payload.template is<ThriftSetupResponsePayload>()) {
      stampSetupCapabilities(
          response.payload.template get<ThriftSetupResponsePayload>());
      return ctx.fireWrite(std::move(msg));
    }

    if (!response.payload.template is<ThriftInitialResponsePayload>()) {
      return ctx.fireWrite(std::move(msg));
    }

    auto& payload =
        response.payload.template get<ThriftInitialResponsePayload>();
    const auto* config = response.requestContext == nullptr
        ? nullptr
        : response.requestContext->getResponseCompressionConfig();
    if (config == nullptr) {
      return ctx.fireWrite(std::move(msg));
    }
    return compressResponse(ctx, std::move(msg), payload, *config);
  }

  void onWriteReady(Context& /*ctx*/) noexcept {}

  void onPipelineInactive(Context& /*ctx*/) noexcept {}

 private:
  // CompressionAlgorithm is a cross-language Thrift-over-Rocket wire contract.
  // Peers expect raw ZLIB, ZSTD, or LZ4 data; Managed Compression uses
  // incompatible framing, so this is an external-protocol exception.
  using CodecAndLevel = std::pair<folly::compression::CodecType, int>;

  static constexpr size_t kDecompressionChunkSize = 64 * 1024;

  static std::optional<CodecAndLevel> codecForAlgorithm(
      apache::thrift::CompressionAlgorithm compression) noexcept {
    using apache::thrift::CompressionAlgorithm;
    using folly::compression::CodecType;

    switch (compression) {
      case CompressionAlgorithm::NONE:
        return CodecAndLevel{
            CodecType::NO_COMPRESSION,
            folly::compression::COMPRESSION_LEVEL_DEFAULT};
      case CompressionAlgorithm::ZLIB:
        return CodecAndLevel{
            CodecType::ZLIB, folly::compression::COMPRESSION_LEVEL_DEFAULT};
      case CompressionAlgorithm::ZSTD:
        return CodecAndLevel{
            CodecType::ZSTD, folly::compression::COMPRESSION_LEVEL_DEFAULT};
      case CompressionAlgorithm::LZ4:
        return CodecAndLevel{
            CodecType::LZ4_VARINT_SIZE,
            folly::compression::COMPRESSION_LEVEL_DEFAULT};
      case CompressionAlgorithm::ZLIB_LESS:
        return CodecAndLevel{
            CodecType::ZLIB, folly::compression::COMPRESSION_LEVEL_FASTEST};
      case CompressionAlgorithm::ZSTD_LESS:
        return CodecAndLevel{CodecType::ZSTD_FAST, 7};
      case CompressionAlgorithm::LZ4_LESS:
        return CodecAndLevel{
            CodecType::LZ4_VARINT_SIZE,
            folly::compression::COMPRESSION_LEVEL_FASTEST};
      case CompressionAlgorithm::ZLIB_MORE:
        return CodecAndLevel{
            CodecType::ZLIB, folly::compression::COMPRESSION_LEVEL_BEST};
      case CompressionAlgorithm::ZSTD_MORE:
        return CodecAndLevel{
            CodecType::ZSTD, folly::compression::COMPRESSION_LEVEL_BEST};
      case CompressionAlgorithm::LZ4_MORE:
        return CodecAndLevel{
            CodecType::LZ4_VARINT_SIZE,
            folly::compression::COMPRESSION_LEVEL_BEST};
      case CompressionAlgorithm::CUSTOM:
        return std::nullopt;
    }
    return std::nullopt;
  }

  std::optional<std::string> decompressRequest(
      ThriftServerRequestResponsePayload& payload) noexcept {
    if (payload.metadata == nullptr ||
        !payload.metadata->compression().has_value()) {
      return std::nullopt;
    }

    const auto compression = *payload.metadata->compression();
    const auto codec = codecForAlgorithm(compression);
    if (!codec.has_value()) [[unlikely]] {
      return compression == apache::thrift::CompressionAlgorithm::CUSTOM
          ? "CUSTOM request compression is not supported by FastThriftServer"
          : "Unsupported request compression algorithm";
    }

    if (compression == apache::thrift::CompressionAlgorithm::NONE) {
      payload.metadata->compression().reset();
      return std::nullopt;
    }
    if (payload.data == nullptr) [[unlikely]] {
      return "Request decompression failed: missing request data";
    }

    try {
      payload.data = decompressBounded(*payload.data, *codec);
      payload.metadata->compression().reset();
      return std::nullopt;
    } catch (const std::exception& ex) {
      return std::string("Request decompression failed: ") + ex.what();
    } catch (...) {
      return "Request decompression failed";
    }
  }

  std::unique_ptr<folly::IOBuf> decompressBounded(
      const folly::IOBuf& data, const CodecAndLevel& codec) const {
    if (codec.first == folly::compression::CodecType::LZ4_VARINT_SIZE) {
      return decompressLz4Bounded(data, codec);
    }
    if (!folly::compression::hasStreamCodec(codec.first)) {
      throw std::runtime_error(
          "compression algorithm does not support bounded decompression");
    }
    return decompressStreamBounded(data, codec);
  }

  std::unique_ptr<folly::IOBuf> decompressStreamBounded(
      const folly::IOBuf& data, const CodecAndLevel& codecAndLevel) const {
    auto codec = folly::compression::getStreamCodec(
        codecAndLevel.first, codecAndLevel.second);
    const auto declaredLength = codec->getUncompressedLength(&data);
    if (declaredLength.has_value() &&
        *declaredLength > maxDecompressedRequestSize_) {
      throw std::runtime_error("decompressed request exceeds size limit");
    }
    codec->resetStream(declaredLength);

    folly::IOBufQueue result;
    size_t decompressedSize = 0;
    auto* current = &data;
    folly::ByteRange input{current->data(), current->length()};

    while (true) {
      while (input.empty() && current->next() != &data) {
        current = current->next();
        input = folly::ByteRange{current->data(), current->length()};
      }

      const auto remaining = maxDecompressedRequestSize_ - decompressedSize;
      // At the limit, retain a one-byte sentinel so excess output trips the
      // size check. StreamCodec rejects repeated calls that neither consume
      // input nor produce output, so this loop cannot stall at the boundary.
      const auto outputSize = remaining < kDecompressionChunkSize
          ? remaining + 1
          : kDecompressionChunkSize;
      auto outputBuffer = folly::IOBuf::create(outputSize);
      outputBuffer->append(outputSize);
      folly::MutableByteRange output{outputBuffer->writableData(), outputSize};
      const bool isLastInput = current->next() == &data;
      const bool done = codec->uncompressStream(
          input,
          output,
          isLastInput ? folly::compression::StreamCodec::FlushOp::END
                      : folly::compression::StreamCodec::FlushOp::NONE);
      const auto bytesWritten = outputSize - output.size();
      outputBuffer->trimEnd(output.size());
      decompressedSize += bytesWritten;
      if (decompressedSize > maxDecompressedRequestSize_) [[unlikely]] {
        throw std::runtime_error("decompressed request exceeds size limit");
      }
      if (bytesWritten != 0) {
        result.append(std::move(outputBuffer));
      }

      if (!done) {
        continue;
      }
      if (!input.empty()) {
        throw std::runtime_error("junk after compressed request data");
      }
      for (auto* trailing = current->next(); trailing != &data;
           trailing = trailing->next()) {
        if (!trailing->empty()) {
          throw std::runtime_error("junk after compressed request data");
        }
      }
      if (declaredLength.has_value() && *declaredLength != decompressedSize) {
        throw std::runtime_error("invalid decompressed request size");
      }
      auto decompressed = result.move();
      if (decompressed == nullptr) {
        return folly::IOBuf::create(0);
      }
      return decompressed;
    }
  }

  std::unique_ptr<folly::IOBuf> decompressLz4Bounded(
      const folly::IOBuf& data, const CodecAndLevel& codecAndLevel) const {
    std::array<uint8_t, folly::kMaxVarintLength64> encodedLength{};
    const auto encodedLengthSize =
        std::min(data.computeChainDataLength(), encodedLength.size());
    folly::io::Cursor cursor{&data};
    cursor.pull(encodedLength.data(), encodedLengthSize);
    folly::ByteRange encodedLengthRange{
        encodedLength.data(), encodedLengthSize};
    const auto decompressedLength = folly::tryDecodeVarint(encodedLengthRange);
    if (!decompressedLength.hasValue()) {
      throw std::runtime_error("invalid LZ4 decompressed size");
    }
    if (*decompressedLength > maxDecompressedRequestSize_) {
      throw std::runtime_error("decompressed request exceeds size limit");
    }
    return folly::compression::getCodec(
               codecAndLevel.first, codecAndLevel.second)
        ->uncompress(&data, *decompressedLength);
  }

  static std::optional<apache::thrift::CompressionAlgorithm>
  compressionForConfig(
      const apache::thrift::CompressionConfig& config) noexcept {
    auto codec = config.codecConfig();
    if (!codec ||
        codec->getType() == apache::thrift::CodecConfig::Type::__EMPTY__) {
      return apache::thrift::CompressionAlgorithm::NONE;
    }

    switch (codec->getType()) {
      case apache::thrift::CodecConfig::Type::zlibConfig:
        switch (codec->zlibConfig()->levelPreset().value_or(
            apache::thrift::ZlibCompressionLevelPreset::DEFAULT)) {
          case apache::thrift::ZlibCompressionLevelPreset::DEFAULT:
            return apache::thrift::CompressionAlgorithm::ZLIB;
          case apache::thrift::ZlibCompressionLevelPreset::LESS:
            return apache::thrift::CompressionAlgorithm::ZLIB_LESS;
          case apache::thrift::ZlibCompressionLevelPreset::MORE:
            return apache::thrift::CompressionAlgorithm::ZLIB_MORE;
        }
        return std::nullopt;
      case apache::thrift::CodecConfig::Type::zstdConfig:
        switch (codec->zstdConfig()->levelPreset().value_or(
            apache::thrift::ZstdCompressionLevelPreset::DEFAULT)) {
          case apache::thrift::ZstdCompressionLevelPreset::DEFAULT:
            return apache::thrift::CompressionAlgorithm::ZSTD;
          case apache::thrift::ZstdCompressionLevelPreset::LESS:
            return apache::thrift::CompressionAlgorithm::ZSTD_LESS;
          case apache::thrift::ZstdCompressionLevelPreset::MORE:
            return apache::thrift::CompressionAlgorithm::ZSTD_MORE;
        }
        return std::nullopt;
      case apache::thrift::CodecConfig::Type::lz4Config:
        switch (codec->lz4Config()->levelPreset().value_or(
            apache::thrift::Lz4CompressionLevelPreset::DEFAULT)) {
          case apache::thrift::Lz4CompressionLevelPreset::DEFAULT:
            return apache::thrift::CompressionAlgorithm::LZ4;
          case apache::thrift::Lz4CompressionLevelPreset::LESS:
            return apache::thrift::CompressionAlgorithm::LZ4_LESS;
          case apache::thrift::Lz4CompressionLevelPreset::MORE:
            return apache::thrift::CompressionAlgorithm::LZ4_MORE;
        }
        return std::nullopt;
      case apache::thrift::CodecConfig::Type::customConfig:
        return apache::thrift::CompressionAlgorithm::CUSTOM;
      case apache::thrift::CodecConfig::Type::__EMPTY__:
        return apache::thrift::CompressionAlgorithm::NONE;
    }
    return std::nullopt;
  }

  channel_pipeline::Result compressResponse(
      Context& ctx,
      channel_pipeline::TypeErasedBox&& msg,
      ThriftInitialResponsePayload& payload,
      const apache::thrift::CompressionConfig& config) noexcept {
    if (payload.metadata == nullptr || payload.data == nullptr) {
      return ctx.fireWrite(std::move(msg));
    }

    payload.metadata->compression().reset();
    const auto compression = compressionForConfig(config);
    if (!compression.has_value()) [[unlikely]] {
      return writeCompressionFailure(
          ctx,
          payload.streamId,
          std::move(msg.get<ThriftServerResponseMessage>().requestContext),
          "Unsupported response compression config");
    }
    if (*compression == apache::thrift::CompressionAlgorithm::NONE ||
        *compression == apache::thrift::CompressionAlgorithm::CUSTOM) {
      return ctx.fireWrite(std::move(msg));
    }

    const auto payloadSize = payload.data->computeChainDataLength();
    if (payloadSize <=
        static_cast<size_t>(config.compressionSizeLimit().value_or(0))) {
      return ctx.fireWrite(std::move(msg));
    }

    const auto codec = codecForAlgorithm(*compression);
    if (!codec.has_value()) [[unlikely]] {
      return writeCompressionFailure(
          ctx,
          payload.streamId,
          std::move(msg.get<ThriftServerResponseMessage>().requestContext),
          "Unsupported response compression algorithm");
    }

    try {
      auto compressed =
          folly::compression::getCodec(codec->first, codec->second)
              ->compress(payload.data.get());
      if (compressed == nullptr) [[unlikely]] {
        return writeCompressionFailure(
            ctx,
            payload.streamId,
            std::move(msg.get<ThriftServerResponseMessage>().requestContext),
            "Response compression returned no data");
      }
      payload.data = std::move(compressed);
      payload.metadata->compression() = *compression;
    } catch (const std::exception& ex) {
      return writeCompressionFailure(
          ctx,
          payload.streamId,
          std::move(msg.get<ThriftServerResponseMessage>().requestContext),
          std::string("Response compression failed: ") + ex.what());
    } catch (...) {
      return writeCompressionFailure(
          ctx,
          payload.streamId,
          std::move(msg.get<ThriftServerResponseMessage>().requestContext),
          "Response compression failed");
    }

    return ctx.fireWrite(std::move(msg));
  }

  static void stampSetupCapabilities(
      ThriftSetupResponsePayload& payload) noexcept {
    if (payload.response == nullptr) {
      return;
    }
    payload.response->zstdSupported() =
        folly::compression::hasCodec(folly::compression::CodecType::ZSTD);
    payload.response->lz4Supported() = folly::compression::hasCodec(
        folly::compression::CodecType::LZ4_VARINT_SIZE);
  }
  static channel_pipeline::Result writeParsingFailure(
      Context& ctx,
      uint32_t streamId,
      ThriftRequestContextPtr requestContext,
      std::string message) noexcept {
    auto response = makeFrameworkErrorMessage(
        streamId,
        apache::thrift::ResponseRpcErrorCode::REQUEST_PARSING_FAILURE,
        std::move(message));
    response.requestContext = std::move(requestContext);
    return ctx.fireWrite(channel_pipeline::erase_and_box(std::move(response)));
  }

  static channel_pipeline::Result writeCompressionFailure(
      Context& ctx,
      uint32_t streamId,
      ThriftRequestContextPtr requestContext,
      std::string message) noexcept {
    auto response = makeFrameworkErrorMessage(
        streamId,
        apache::thrift::ResponseRpcErrorCode::UNKNOWN,
        std::move(message));
    response.requestContext = std::move(requestContext);
    return ctx.fireWrite(channel_pipeline::erase_and_box(std::move(response)));
  }

  size_t maxDecompressedRequestSize_;
};

// Concept checks need the pipeline's context type, which has no public name.
// NOLINTBEGIN(facebook-hte-DetailCall)
static_assert(
    channel_pipeline::OutboundHandler<
        ThriftServerCompressionHandler<channel_pipeline::detail::ContextImpl>,
        channel_pipeline::detail::ContextImpl>,
    "ThriftServerCompressionHandler must satisfy OutboundHandler concept");

static_assert(
    channel_pipeline::InboundHandler<
        ThriftServerCompressionHandler<channel_pipeline::detail::ContextImpl>,
        channel_pipeline::detail::ContextImpl>,
    "ThriftServerCompressionHandler must satisfy InboundHandler concept");
// NOLINTEND(facebook-hte-DetailCall)

} // namespace apache::thrift::fast_thrift::thrift
