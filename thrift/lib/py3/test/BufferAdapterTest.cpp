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

#include <string>
#include <utility>

#include <gtest/gtest.h>

#include <folly/io/IOBuf.h>
#include <folly/io/IOBufQueue.h>

#include <thrift/lib/cpp2/protocol/BinaryProtocol.h>
#include <thrift/lib/cpp2/protocol/CompactProtocol.h>
#include <thrift/lib/cpp2/protocol/JSONProtocol.h>
#include <thrift/lib/cpp2/protocol/SimpleJSONProtocol.h>
#include <thrift/lib/cpp2/type/ThriftType.h>
#include <thrift/lib/py3/test/BinaryTypes.h>

namespace test {
namespace {

using Tag = apache::thrift::type::binary_t;

TEST(BufferAdapterTest, ConversionOwnsBytesBeyondConstructorSourceLifetime) {
  const std::string expected("\0\x80\xff", 3);
  Buffer value;
  {
    std::string source = expected;
    value = Buffer(std::move(source));
    source = "replacement";
  }
  EXPECT_EQ(BufferAdapter::toThrift(value), expected);

  auto converted = BufferAdapter::fromThrift(expected);
  auto bytes = BufferAdapter::toThrift(converted);
  BufferAdapter::clear(converted);
  EXPECT_TRUE(BufferAdapter::isEmpty(converted));
  EXPECT_EQ(BufferAdapter::toThrift(converted), "");
  EXPECT_EQ(bytes, expected);
  EXPECT_EQ(BufferAdapter::toThrift(BufferAdapter::fromThrift({})), "");
}

template <typename Writer>
class BufferAdapterProtocolTest : public testing::Test {};

using Protocols = testing::Types<
    apache::thrift::BinaryProtocolWriter,
    apache::thrift::CompactProtocolWriter,
    apache::thrift::JSONProtocolWriter,
    apache::thrift::SimpleJSONProtocolWriter>;
TYPED_TEST_SUITE(BufferAdapterProtocolTest, Protocols);

TYPED_TEST(BufferAdapterProtocolTest, DecodeReplacesAndOwnsFragmentedBytes) {
  const std::string expected("a\0\x80\xffz", 5);
  Buffer decoded(std::string("old bytes"));
  {
    folly::IOBufQueue queue(folly::IOBufQueue::cacheChainLength());
    TypeParam writer;
    writer.setOutput(&queue);
    writer.writeBinary(expected);
    auto wire = queue.move();
    wire->coalesce();
    const auto split = wire->length() / 2;
    auto fragmented = folly::IOBuf::copyBuffer(wire->data(), split);
    fragmented->appendChain(
        folly::IOBuf::copyBuffer(wire->data() + split, wire->length() - split));
    typename TypeParam::ProtocolReader reader;
    reader.setInput(fragmented.get());
    BufferAdapter::decode<Tag>(reader, decoded);
  }
  EXPECT_EQ(BufferAdapter::toThrift(decoded), expected);

  folly::IOBufQueue emptyQueue(folly::IOBufQueue::cacheChainLength());
  TypeParam emptyWriter;
  emptyWriter.setOutput(&emptyQueue);
  emptyWriter.writeBinary(std::string());
  auto emptyWire = emptyQueue.move();
  typename TypeParam::ProtocolReader emptyReader;
  emptyReader.setInput(emptyWire.get());
  BufferAdapter::decode<Tag>(emptyReader, decoded);
  EXPECT_TRUE(BufferAdapter::isEmpty(decoded));
}

template <typename Writer>
class BufferAdapterZeroCopyTest : public testing::Test {};

using ZeroCopyProtocols = testing::Types<
    apache::thrift::BinaryProtocolWriter,
    apache::thrift::CompactProtocolWriter>;
TYPED_TEST_SUITE(BufferAdapterZeroCopyTest, ZeroCopyProtocols);

TYPED_TEST(BufferAdapterZeroCopyTest, EncodeSharesOwnedPayload) {
  const std::string expected(folly::IOBufQueue::kMaxPackCopy + 1, '\xff');
  Buffer value = BufferAdapter::fromThrift(expected);
  const auto* data = value.data();
  folly::IOBufQueue queue(folly::IOBufQueue::cacheChainLength());
  TypeParam writer;
  writer.setOutput(&queue);
  const auto size = BufferAdapter::encode<Tag>(writer, value);
  auto wire = queue.move();
  EXPECT_EQ(size, wire->computeChainDataLength());

  bool sharesPayload = false;
  const auto* node = wire.get();
  do {
    sharesPayload |= node->data() == data && node->length() == expected.size();
    node = node->next();
  } while (node != wire.get());
  EXPECT_TRUE(sharesPayload);

  value = Buffer();
  typename TypeParam::ProtocolReader reader;
  reader.setInput(wire.get());
  std::string decoded;
  reader.readBinary(decoded);
  EXPECT_EQ(decoded, expected);
}

TYPED_TEST(BufferAdapterZeroCopyTest, SizesMatchNativeIOBufOverloads) {
  using Legacy = apache::thrift::Cpp2Ops<Buffer>;
  TypeParam writer;
  for (const auto size :
       {std::size_t(0),
        std::size_t(3),
        std::size_t(folly::IOBufQueue::kMaxPackCopy + 1)}) {
    const auto value = BufferAdapter::fromThrift(std::string(size, 'x'));
    EXPECT_EQ(
        (BufferAdapter::serializedSize<false, Tag>(writer, value)),
        Legacy::serializedSize(&writer, &value));
    EXPECT_EQ(
        (BufferAdapter::serializedSize<true, Tag>(writer, value)),
        Legacy::serializedSizeZC(&writer, &value));
    if (size > folly::IOBufQueue::kMaxPackCopy) {
      EXPECT_LT(
          (BufferAdapter::serializedSize<true, Tag>(writer, value)),
          (BufferAdapter::serializedSize<false, Tag>(writer, value)));
    }
  }
}

} // namespace
} // namespace test
