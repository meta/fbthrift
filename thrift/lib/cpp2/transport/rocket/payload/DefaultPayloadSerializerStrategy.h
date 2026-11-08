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

#include <stdexcept>
#include <type_traits>

#include <thrift/lib/cpp/TApplicationException.h>
#include <thrift/lib/cpp2/async/StreamPayload.h>
#include <thrift/lib/cpp2/op/Encode.h>
#include <thrift/lib/cpp2/protocol/BinaryProtocol.h>
#include <thrift/lib/cpp2/protocol/CompactProtocol.h>
#include <thrift/lib/cpp2/transport/rocket/compression/CompressionManager.h>
#include <thrift/lib/cpp2/transport/rocket/payload/PayloadSerializerStrategy.h>

namespace apache::thrift::rocket {

/**
 * Port of PayloadUtils.h header free functions into a strategy class.
 */
class DefaultPayloadSerializerStrategy final
    : public PayloadSerializerStrategy<DefaultPayloadSerializerStrategy> {
 public:
  DefaultPayloadSerializerStrategy() : PayloadSerializerStrategy(*this) {}

  bool supportsChecksum() { return false; }

  template <class T>
  folly::Try<T> unpackAsCompressed(
      rocket::Payload&& payload, bool decodeMetadataUsingBinary) {
    return folly::makeTryWith([&]() {
      return unpackImpl<T>(std::move(payload), decodeMetadataUsingBinary);
    });
  }

  template <class T>
  folly::Try<T> unpack(
      rocket::Payload&& payload, bool decodeMetadataUsingBinary) {
    return folly::makeTryWith([&]() {
      T t = unpackImpl<T>(std::move(payload), decodeMetadataUsingBinary);
      if (auto compression = t.metadata.compression()) {
        const auto compressionAlgorithm = *compression;
        // Custom compression is supported in
        // CustomCompressionPayloadSerializerStrategy
        if (compressionAlgorithm != CompressionAlgorithm::NONE &&
            compressionAlgorithm != CompressionAlgorithm::CUSTOM) {
          uncompressPayload(t, compressionAlgorithm);
          // Clear compression only for StreamPayload. This signals to
          // decompressStreamPayload() that IO-thread decompression already
          // happened, preventing double decompression without a racy flag
          // check. FirstResponsePayload must retain compression metadata so
          // fillTHeaderFromResponseRpcMetadata() can set TTransforms for
          // ServiceRouter Scuba logging.
          if constexpr (std::is_same_v<T, StreamPayload>) {
            t.metadata.compression().reset();
          }
        }
      }
      return t;
    });
  }

  template <typename Metadata>
  rocket::Payload packWithFds(
      Metadata* metadata,
      std::unique_ptr<folly::IOBuf>&& payload,
      folly::SocketFds fds,
      bool encodeMetadataUsingBinary,
      folly::AsyncTransport* transport,
      folly::IOBufFactory* ioBufFactory = nullptr,
      bool skipCompression = false);

  template <typename T>
  std::unique_ptr<folly::IOBuf> packCompact(const T& data) {
    CompactProtocolWriter writer;
    folly::IOBufQueue queue;
    writer.setOutput(&queue);
    data.write(&writer);
    return queue.move();
  }

  template <typename T>
  size_t unpackCompact(T& output, const folly::IOBuf* buffer) {
    if (!buffer) [[unlikely]] {
      folly::throw_exception<std::runtime_error>("Underflow");
    }
    CompactProtocolReader reader;
    reader.setInput(buffer);
    output.read(&reader);
    return reader.getCursorPosition();
  }

  template <typename T>
  size_t unpackCompact(T& output, const folly::io::Cursor& cursor) {
    CompactProtocolReader reader;
    reader.setInput(cursor);
    output.read(&reader);
    return reader.getCursorPosition();
  }

  template <typename T>
  size_t unpackBinary(T& output, const folly::IOBuf* buffer) {
    if (!buffer) [[unlikely]] {
      folly::throw_exception<std::runtime_error>("Underflow");
    }
    BinaryProtocolReader reader;
    reader.setInput(buffer);
    output.read(&reader);
    return reader.getCursorPosition();
  }

  template <typename T>
  size_t unpackBinary(T& output, const folly::io::Cursor& cursor) {
    BinaryProtocolReader reader;
    reader.setInput(cursor);
    output.read(&reader);
    return reader.getCursorPosition();
  }

  template <typename T>
  size_t unpackBinary(T& output, const IOBufChain& buffer) {
    BinaryProtocolChainReader reader;
    reader.setInput(&buffer);
    op::decode<type::struct_t<T>>(reader, output);
    return reader.getCursorPosition();
  }

  template <typename T>
  size_t unpackBinary(T& output, const io::IOBufChainCursor& cursor) {
    BinaryProtocolChainReader reader;
    reader.setInput(cursor);
    op::decode<type::struct_t<T>>(reader, output);
    return reader.getCursorPosition();
  }

  template <class PayloadType>
  rocket::Payload pack(
      PayloadType&& payload,
      bool encodeMetadataUsingBinary,
      folly::AsyncTransport* transport) {
    auto metadata = std::forward<PayloadType>(payload).metadata;
    return packWithFds(
        &metadata,
        std::forward<PayloadType>(payload).payload,
        std::forward<PayloadType>(payload).fds,
        encodeMetadataUsingBinary,
        transport);
  }

  std::unique_ptr<folly::IOBuf> compressBuffer(
      std::unique_ptr<folly::IOBuf>&& buffer,
      CompressionAlgorithm compressionAlgorithm) {
    return CompressionManager().compressBuffer(
        std::move(buffer), compressionAlgorithm);
  }

  std::unique_ptr<folly::IOBuf> uncompressBuffer(
      std::unique_ptr<folly::IOBuf>&& buffer,
      CompressionAlgorithm compressionAlgorithm) {
    return CompressionManager().uncompressBuffer(
        std::move(buffer), compressionAlgorithm);
  }

  IOBufChain uncompressBuffer(
      IOBufChain&& buffer, CompressionAlgorithm compressionAlgorithm) {
    return CompressionManager().uncompressBuffer(
        std::move(buffer), compressionAlgorithm);
  }

 private:
  static constexpr size_t kHeadroomBytes = 16;
  static constexpr size_t kMinAllocBytes = 1024;

  template <typename Metadata>
  rocket::Payload finalizePayload(
      std::unique_ptr<folly::IOBuf>&& payload,
      Metadata* metadata,
      folly::SocketFds fds,
      bool encodeMetadataUsingBinary,
      folly::IOBufFactory* ioBufFactory);

  bool isHeadroomCandidate(const std::unique_ptr<folly::IOBuf>& data) const;

  bool canSerializeMetadataIntoDataBufferHeadroom(
      const std::unique_ptr<folly::IOBuf>& data, const size_t serSize) const;

  template <class Metadata, class ProtocolWriter>
  Payload makePayloadWithHeadroom(
      ProtocolWriter& writer,
      const Metadata& metadata,
      std::unique_ptr<folly::IOBuf> data,
      folly::IOBufFactory* ioBufFactory);

  template <class Metadata, class ProtocolWriter>
  Payload makePayloadWithoutHeadroom(
      size_t allocSize,
      ProtocolWriter& writer,
      const Metadata& metadata,
      std::unique_ptr<folly::IOBuf> data,
      folly::IOBufFactory* ioBufFactory);

  template <class Metadata, class ProtocolWriter>
  Payload makePayload(
      const Metadata& metadata,
      std::unique_ptr<folly::IOBuf> data,
      folly::IOBufFactory* ioBufFactory);

  void verifyMetadataSize(size_t metadataSize, size_t expectedSize) {
    if (metadataSize != expectedSize) {
      folly::throw_exception<std::out_of_range>("metadata size mismatch");
    }
  }

  template <typename T>
  static void storePayloadData(T& payload, IOBufChain&& data) {
    payload.payloadChain.emplace(std::move(data));
  }

  [[noreturn]] static void storePayloadData(StreamPayload&, IOBufChain&&) {
    folly::throw_exception<TApplicationException>(
        TApplicationException::UNSUPPORTED_CLIENT_TYPE,
        "IOBufChain stream payloads are not supported");
  }

  template <typename T>
  void uncompressPayload(T& payload, CompressionAlgorithm compression) {
    if (payloadUsesIOBufChain(payload)) {
      payload.payloadChain =
          uncompressBuffer(std::move(*payload.payloadChain), compression);
    } else {
      payload.payload =
          uncompressBuffer(std::move(payload.payload), compression);
    }
  }

  void uncompressPayload(
      StreamPayload& payload, CompressionAlgorithm compression) {
    payload.payload = uncompressBuffer(std::move(payload.payload), compression);
  }

  template <typename T>
  T unpackImpl(rocket::Payload&& payload, bool decodeMetadataUsingBinary) {
    T t = [] {
      if constexpr (std::is_aggregate_v<T>) {
        return T{};
      } else {
        return T{{}, {}};
      }
    }();
    unpackPayloadMetadata(t, payload, decodeMetadataUsingBinary);
    if (payload.usesIOBufChain()) {
      storePayloadData(t, std::move(payload).chainData());
    } else {
      t.payload = std::move(payload).data();
    }
    return t;
  }

  template <typename T>
  void unpackPayloadMetadata(
      T& t, rocket::Payload& payload, bool decodeMetadataUsingBinary) {
    if (payload.hasNonemptyMetadata()) {
      size_t metadataSize;
      if (payload.usesIOBufChain()) {
        if (decodeMetadataUsingBinary) {
          metadataSize = unpackBinary(t.metadata, payload.chainBuffer());
        } else {
          const auto& first = *payload.chainBuffer().begin();
          std::unique_ptr<folly::IOBuf> metadata;
          const folly::IOBuf* metadataBuffer = &first;
          if (first.length() < payload.metadataSize()) {
            metadata = payload.copyMetadataToIOBuf();
            metadataBuffer = metadata.get();
          }
          metadataSize = unpackCompact(t.metadata, metadataBuffer);
        }
      } else {
        metadataSize = decodeMetadataUsingBinary
            ? unpackBinary(t.metadata, payload.buffer())
            : unpackCompact(t.metadata, payload.buffer());
      }
      verifyMetadataSize(metadataSize, payload.metadataSize());
    }
  }
};
} // namespace apache::thrift::rocket
