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
#include <folly/io/IOBufQueue.h>
#include <thrift/lib/cpp2/StringTypeAdapter.h>
#include <thrift/lib/cpp2/op/Encode.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>
#include <thrift/test/gen-cpp2/ProtocolMethodsTest_types.h>

using namespace apache::thrift::test;

template <typename Reader, typename Writer, typename Tag>
void testStringWrapperAdapter() {
  using Adapter = apache::thrift::StringTypeAdapter<MyString>;
  using AdaptedTag = apache::thrift::type::adapted<Adapter, Tag>;
  const auto expected = std::string(256, 'x') + std::string("\n\0", 2);
  auto source = expected;
  const auto* storage = source.data();
  auto adapted = Adapter::fromThrift(std::move(source));
  EXPECT_EQ(adapted.data(), storage);
  EXPECT_EQ(Adapter::toThrift(adapted), expected);

  folly::IOBufQueue queue;
  Writer writer;
  writer.setOutput(&queue);
  apache::thrift::op::encode<AdaptedTag>(writer, adapted);
  auto encoded = queue.move();
  MyString decoded;
  decoded.reserve(expected.size());
  const auto* destinationStorage = decoded.data();
  Reader reader;
  reader.setInput(encoded.get());
  apache::thrift::op::decode<AdaptedTag>(reader, decoded);
  EXPECT_EQ(decoded.value, expected);
  EXPECT_EQ(decoded.data(), destinationStorage);
}

TEST(ProtocolMethodsTest, StringTypeAdapter) {
  using namespace apache::thrift;
  testStringWrapperAdapter<
      CompactProtocolReader,
      CompactProtocolWriter,
      type::string_t>();
  testStringWrapperAdapter<
      BinaryProtocolReader,
      BinaryProtocolWriter,
      type::binary_t>();
  testStringWrapperAdapter<
      SimpleJSONProtocolReader,
      SimpleJSONProtocolWriter,
      type::string_t>();
  testStringWrapperAdapter<
      JSONProtocolReader,
      JSONProtocolWriter,
      type::binary_t>();
}

TEST(ProtocolMethodsTest, roundtrip) {
  MyStruct before;
  before.num()->value = 42;
  before.str()->value = "foo";

  const auto after = apache::thrift::CompactSerializer::deserialize<MyStruct>(
      apache::thrift::CompactSerializer::serialize<std::string>(before));
  EXPECT_EQ(after, before);
}
