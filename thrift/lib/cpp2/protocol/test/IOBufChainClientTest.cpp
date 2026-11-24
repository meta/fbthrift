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
#include <cstdint>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <gtest/gtest.h>

#include <folly/io/IOBuf.h>
#include <folly/io/IOBufQueue.h>
#include <thrift/lib/cpp/TApplicationException.h>
#include <thrift/lib/cpp2/IOBufChain.h>
#include <thrift/lib/cpp2/protocol/BinaryProtocol.h>
#include <thrift/lib/cpp2/protocol/CompactProtocol.h>
#include <thrift/lib/cpp2/protocol/test/gen-cpp2/ChainServiceAsyncClient.h>

namespace apache::thrift::test::iobuf_chain_rpc {
namespace {

template <typename Payload>
void expectPayload(const Payload& payload, size_t size) {
  if constexpr (std::is_same_v<Payload, IOBufChain>) {
    EXPECT_EQ(size, payload.chainLength());
    for (const auto& buffer : payload) {
      if (buffer.length() == 0) {
        continue;
      }
      EXPECT_TRUE(
          std::all_of(
              buffer.data(), buffer.data() + buffer.length(), [](uint8_t byte) {
                return byte == 'x';
              }));
    }
  } else {
    if (size == 0) {
      EXPECT_TRUE(!payload || payload->computeChainDataLength() == 0);
    } else {
      ASSERT_NE(nullptr, payload);
      EXPECT_EQ(std::string(size, 'x'), payload->toString());
    }
  }
}

void expectResponse(const Payload& response, size_t size) {
  EXPECT_EQ(123456789, *response.sequence());
  EXPECT_EQ("chain reader", *response.text());
  EXPECT_EQ(std::string(size, 'x'), *response.ordinaryBinary());
  EXPECT_EQ(42, *response.choice()->number());
  expectPayload(*response.iobufBinary(), size);
  expectPayload(*response.chainBinary(), size);
}

template <typename Writer>
std::unique_ptr<folly::IOBuf> serializeReply(size_t size) {
  Payload response;
  response.sequence() = 123456789;
  response.text() = "chain reader";
  response.ordinaryBinary() = std::string(size, 'x');
  response.iobufBinary() = folly::IOBuf::copyBuffer(std::string(size, 'x'));
  response.chainBinary() =
      IOBufChain{folly::IOBuf::copyBuffer(std::string(size, 'x'))};
  response.choice()->number() = 42;
  folly::IOBufQueue queue;
  Writer writer;
  writer.setOutput(&queue);
  writer.writeStructBegin("echo_result");
  writer.writeFieldBegin("success", protocol::T_STRUCT, 0);
  response.write(&writer);
  writer.writeFieldEnd();
  writer.writeFieldStop();
  writer.writeStructEnd();
  return queue.move();
}

IOBufChain fragment(const folly::IOBuf& serialized) {
  IOBufChain result;
  const auto* buffer = &serialized;
  do {
    for (size_t offset = 0; offset < buffer->length(); offset += 13) {
      const auto length = std::min(size_t{13}, buffer->length() - offset);
      auto part = buffer->cloneOne();
      part->trimStart(offset);
      part->trimEnd(part->length() - length);
      result.append(std::move(part));
    }
    buffer = buffer->next();
  } while (buffer != &serialized);
  return result;
}

template <typename Payload>
void expectSharedPayload(const Payload& payload, const IOBufChain& input) {
  const auto expectSharedBuffer = [&](const folly::IOBuf& outputBuffer) {
    const auto outputAddress = reinterpret_cast<uintptr_t>(outputBuffer.data());
    bool shared = false;
    for (const auto& inputBuffer : input) {
      const auto inputAddress = reinterpret_cast<uintptr_t>(inputBuffer.data());
      if (outputAddress >= inputAddress &&
          outputAddress + outputBuffer.length() <=
              inputAddress + inputBuffer.length()) {
        shared = true;
        break;
      }
    }
    EXPECT_TRUE(shared);
  };
  if constexpr (std::is_same_v<Payload, IOBufChain>) {
    EXPECT_GT(payload.chainElements(), 1);
    for (const auto& outputBuffer : payload) {
      expectSharedBuffer(outputBuffer);
    }
  } else {
    ASSERT_NE(nullptr, payload);
    EXPECT_GT(payload->countChainElements(), 1);
    const auto* buffer = payload.get();
    do {
      expectSharedBuffer(*buffer);
      buffer = buffer->next();
    } while (buffer != payload.get());
  }
}

ClientReceiveState makeState(
    protocol::PROTOCOL_TYPES protocol, SerializedResponse response) {
  return ClientReceiveState(
      protocol,
      MessageType::T_REPLY,
      std::move(response),
      nullptr,
      nullptr,
      RpcTransportStats{});
}

TEST(IOBufChainClientTest, NativeInputSupportsBinaryAndCompact) {
  for (const auto protocolId :
       {protocol::T_BINARY_PROTOCOL, protocol::T_COMPACT_PROTOCOL}) {
    for (const auto size : {0, 4096, 32768}) {
      SCOPED_TRACE(size);
      SCOPED_TRACE(protocolId);
      auto serialized = protocolId == protocol::T_BINARY_PROTOCOL
          ? serializeReply<BinaryProtocolWriter>(size)
          : serializeReply<CompactProtocolWriter>(size);
      auto state =
          makeState(protocolId, SerializedResponse(std::move(serialized)));
      EXPECT_TRUE(state.hasResponseBuffer());
      EXPECT_FALSE(state.serializedResponse().usesIOBufChain());
      Payload response;
      auto error = Client<ChainService>::recv_wrapped_echo(response, state);
      ASSERT_FALSE(error) << error.what();
      expectResponse(response, size);
    }
  }
}

TEST(IOBufChainClientTest, ChainInputDecodesAndSharesPayload) {
  for (const auto size : {0, 4096, 32768}) {
    SCOPED_TRACE(size);
    auto serialized = serializeReply<BinaryProtocolWriter>(size);
    auto state = makeState(
        protocol::T_BINARY_PROTOCOL, SerializedResponse(fragment(*serialized)));
    EXPECT_FALSE(state.hasResponseBuffer());
    EXPECT_TRUE(state.serializedResponse().usesIOBufChain());
    Payload response;
    auto error = Client<ChainService>::recv_wrapped_echo(response, state);
    ASSERT_FALSE(error) << error.what();
    expectResponse(response, size);
    if (size > 0) {
      ASSERT_TRUE(state.serializedResponse().chainBuffer.has_value());
      expectSharedPayload(
          *response.iobufBinary(), *state.serializedResponse().chainBuffer);
      expectSharedPayload(
          *response.chainBinary(), *state.serializedResponse().chainBuffer);
    }
  }
}

TEST(IOBufChainClientTest, CompactRpcBodyInChainInputIsRejected) {
  auto serialized = serializeReply<CompactProtocolWriter>(4096);
  auto state = makeState(
      protocol::T_COMPACT_PROTOCOL, SerializedResponse(fragment(*serialized)));
  Payload response;
  auto error = Client<ChainService>::recv_wrapped_echo(response, state);
  const auto* exception = error.get_exception<TApplicationException>();
  ASSERT_NE(nullptr, exception);
  EXPECT_EQ(
      TApplicationException::UNSUPPORTED_CLIENT_TYPE, exception->getType());
}

} // namespace
} // namespace apache::thrift::test::iobuf_chain_rpc
