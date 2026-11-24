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

#include <thrift/lib/cpp2/dynamic/ValueSerializer.h>

#include <gtest/gtest.h>

#include <folly/io/IOBuf.h>
#include <folly/io/IOBufQueue.h>
#include <thrift/lib/cpp2/dynamic/test/gen-cpp2/json5_serialization_test_types.h>
#include <thrift/lib/cpp2/dynamic/test/gen-cpp2/json5_serialization_test_types_custom_protocol.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>
#include <thrift/lib/cpp2/runtime/SchemaRegistry.h>

#include <cstdint>
#include <limits>
#include <memory_resource>
#include <string>
#include <string_view>

namespace apache::thrift::dynamic {
namespace {

namespace gen = json5_test;

template <typename T>
type_system::TypeRef typeOf() {
  return SchemaRegistry::get().getTypeSystemNode<T>().asRef();
}

template <typename T>
DynamicValue toDynamic(const T& value) {
  return CompactValueSerializer::deserialize(
      apache::thrift::CompactSerializer::serialize<std::string>(value),
      typeOf<T>());
}

template <typename T>
T toStatic(const DynamicConstRef& value) {
  return apache::thrift::CompactSerializer::deserialize<T>(
      CompactValueSerializer::serialize<std::string>(value));
}

gen::Key makeKey(std::int32_t first, std::int32_t second) {
  gen::Key key;
  key.first() = first;
  key.second() = second;
  return key;
}

// No sets or maps, whose dynamic iteration order is unspecified.
gen::Scalars makeScalars() {
  gen::Scalars scalars;
  scalars.boolField() = true;
  scalars.byteField() = -7;
  scalars.i32Field() = 32;
  scalars.i16Field() = -16;
  scalars.i64Field() = std::numeric_limits<std::int64_t>::max();
  scalars.floatField() = 1.5f;
  scalars.doubleField() = -0.0;
  // Long enough to be allocated from the memory resource.
  scalars.stringField() = "a string that does not fit in a small buffer";
  scalars.binaryField() = std::string("\x00\xff\x10", 3);
  scalars.enumField() = gen::Color::BLUE;
  scalars.optionalField() = "set";
  return scalars;
}

gen::AllTypes makeAllTypes() {
  gen::AllTypes all;
  all.scalars() = makeScalars();
  auto& containers = *all.containers();
  containers.intList() = {3, 1, 2};
  containers.enumList() = {gen::Color::BLUE, gen::Color::RED};
  containers.stringSet() = {"b", "a", "c"};
  containers.intSet() = {5, -5, 0};
  containers.binarySet() = {"\xff", "a", ""};
  containers.structSet() = {makeKey(1, 9), makeKey(2, 0)};
  containers.listSet() = {{1, 2}, {1}};
  containers.stringMap() = {{"b", 2}, {"a", 1}};
  containers.intKeyMap() = {{2, "two"}, {-1, "neg"}};
  containers.enumKeyMap() = {{gen::Color::GREEN, "g"}, {gen::Color::RED, "r"}};
  containers.structKeyMap() = {{makeKey(5, 6), "k"}};
  containers.binaryKeyMap() = {{"bk", 9}};
  all.choice()->text() = "chosen";
  all.key() = makeKey(7, 8);
  return all;
}

TEST(ValueSerializerTest, WritesSameBytesAsStaticSerializer) {
  const auto scalars = makeScalars();
  const auto dynamicScalars = toDynamic(scalars);
  EXPECT_EQ(
      CompactValueSerializer::serialize<std::string>(dynamicScalars),
      apache::thrift::CompactSerializer::serialize<std::string>(scalars));
  EXPECT_EQ(
      BinaryValueSerializer::serialize<std::string>(dynamicScalars),
      apache::thrift::BinarySerializer::serialize<std::string>(scalars));
  EXPECT_EQ(
      SimpleJSONValueSerializer::serialize<std::string>(dynamicScalars),
      apache::thrift::SimpleJSONSerializer::serialize<std::string>(scalars));

  // JSON5 sorts sets and maps, so any value matches.
  const auto all = makeAllTypes();
  EXPECT_EQ(
      Json5ValueSerializer::serialize<std::string>(toDynamic(all)),
      json5::detail::toJsonImpl<type::infer_tag<gen::AllTypes>>(all, {}));
}

template <typename Serializer>
void expectRoundTrip(const DynamicValue& value) {
  const auto bytes = Serializer::template serialize<std::string>(value);
  EXPECT_EQ(Serializer::deserialize(bytes, value.type()), value);
  EXPECT_EQ(
      Serializer::deserialize(
          folly::ByteRange(folly::StringPiece(bytes)), value.type()),
      value);

  auto queue = Serializer::template serialize<folly::IOBufQueue>(value);
  const auto buf = queue.move();
  EXPECT_EQ(buf->computeChainDataLength(), bytes.size());
  EXPECT_EQ(Serializer::deserialize(buf.get(), value.type()), value);
}

TEST(ValueSerializerTest, RoundTripsThroughEveryProtocol) {
  const auto value = toDynamic(makeAllTypes());
  expectRoundTrip<CompactValueSerializer>(value);
  expectRoundTrip<BinaryValueSerializer>(value);
  expectRoundTrip<SimpleJSONValueSerializer>(value);
  expectRoundTrip<Json5ValueSerializer>(value);
}

TEST(ValueSerializerTest, ReadsStaticSerializerOutput) {
  const auto all = makeAllTypes();
  const auto type = typeOf<gen::AllTypes>();
  EXPECT_EQ(
      toStatic<gen::AllTypes>(CompactValueSerializer::deserialize(
          apache::thrift::CompactSerializer::serialize<std::string>(all),
          type)),
      all);
  EXPECT_EQ(
      toStatic<gen::AllTypes>(BinaryValueSerializer::deserialize(
          apache::thrift::BinarySerializer::serialize<std::string>(all), type)),
      all);
  EXPECT_EQ(
      toStatic<gen::AllTypes>(SimpleJSONValueSerializer::deserialize(
          apache::thrift::SimpleJSONSerializer::serialize<std::string>(all),
          type)),
      all);
  EXPECT_EQ(
      toStatic<gen::AllTypes>(Json5ValueSerializer::deserialize(
          json5::detail::toJsonImpl<type::infer_tag<gen::AllTypes>>(all, {}),
          type)),
      all);
}

// Like apache::thrift::Serializer::deserialize.
TEST(ValueSerializerTest, ReportsBytesReadAndLeavesTrailingBytes) {
  const auto value = toDynamic(makeAllTypes());
  const auto bytes = CompactValueSerializer::serialize<std::string>(value);
  size_t size = 0;
  EXPECT_EQ(
      CompactValueSerializer::deserialize(
          bytes + "trailing", value.type(), &size),
      value);
  EXPECT_EQ(size, bytes.size());
}

TEST(ValueSerializerTest, TruncatedInputThrows) {
  const auto value = toDynamic(makeAllTypes());
  const auto bytes = CompactValueSerializer::serialize<std::string>(value);
  const auto truncated = bytes.substr(0, bytes.size() / 2);
  EXPECT_ANY_THROW(
      CompactValueSerializer::deserialize(truncated, value.type()));
  EXPECT_ANY_THROW(
      apache::thrift::CompactSerializer::deserialize<gen::AllTypes>(truncated));
}

class CountingResource : public std::pmr::memory_resource {
 public:
  size_t allocations = 0;

 private:
  void* do_allocate(size_t bytes, size_t alignment) override {
    ++allocations;
    return std::pmr::new_delete_resource()->allocate(bytes, alignment);
  }
  void do_deallocate(void* p, size_t bytes, size_t alignment) override {
    std::pmr::new_delete_resource()->deallocate(p, bytes, alignment);
  }
  bool do_is_equal(const memory_resource& other) const noexcept override {
    return this == &other;
  }
};

TEST(ValueSerializerTest, AllocatesFromMemoryResource) {
  CountingResource resource;
  const auto bytes =
      apache::thrift::CompactSerializer::serialize<std::string>(makeAllTypes());
  const auto value = CompactValueSerializer::deserialize(
      bytes, typeOf<gen::AllTypes>(), nullptr, &resource);
  EXPECT_GT(resource.allocations, 0);
  EXPECT_EQ(toStatic<gen::AllTypes>(value), makeAllTypes());
}

TEST(ValueSerializerTest, StringOutputAppends) {
  const auto value = toDynamic(makeScalars());
  std::string out = "prefix";
  CompactValueSerializer::serialize(value, &out);
  EXPECT_EQ(
      out, "prefix" + CompactValueSerializer::serialize<std::string>(value));
}

TEST(ValueSerializerTest, Json5HelpersWriteLikeJson5ProtocolUtils) {
  const auto all = makeAllTypes();
  const auto value = toDynamic(all);
  EXPECT_EQ(toBasicJson(value), Json5ProtocolUtils::toBasicJson(all));
  EXPECT_EQ(toJson5(value), Json5ProtocolUtils::toJson5(all));
}

TEST(ValueSerializerTest, PassesOptionsToJson5Writer) {
  const auto all = makeAllTypes();
  json5::detail::Json5ProtocolWriter::Options options{
      .writer = json5::detail::kJson5Options,
      .enumAsInteger = true,
      .binaryAsBase64String = true,
      .mapPrimitiveKeysAsMemberNames = true,
  };
  EXPECT_EQ(
      Json5ValueSerializer::serialize<std::string>(
          toDynamic(all), COPY_EXTERNAL_BUFFER, options),
      json5::detail::toJsonImpl<type::infer_tag<gen::AllTypes>>(all, options));
}

TEST(ValueSerializerTest, FromJson5ReadsJson5AndBasicJson) {
  const auto all = makeAllTypes();
  const auto type = typeOf<gen::AllTypes>();
  EXPECT_EQ(
      toStatic<gen::AllTypes>(
          fromJson5(Json5ProtocolUtils::toJson5(all), type)),
      all);
  EXPECT_EQ(
      toStatic<gen::AllTypes>(
          fromJson5(Json5ProtocolUtils::toBasicJson(all), type)),
      all);

  const gen::Key key = makeKey(1, 2);
  EXPECT_EQ(
      toStatic<gen::Key>(fromJson5(
          R"json({first: 1, /* comment */ 'second': '2',})json",
          typeOf<gen::Key>())),
      key);
}

TEST(ValueSerializerTest, FromJson5RejectsTrailingContentLikeStatic) {
  const auto type = typeOf<gen::Key>();
  for (std::string_view json :
       {R"json({"first": 1}})json",
        R"json({"first": 1},)json",
        R"json({"first": 1}])json"}) {
    EXPECT_THROW(fromJson5(json, type), protocol::TProtocolException) << json;
    EXPECT_THROW(
        (void)Json5ProtocolUtils::fromJson5<gen::Key>(json),
        protocol::TProtocolException)
        << json;

    // Json5ValueSerializer::deserialize leaves them unread instead.
    size_t size = 0;
    Json5ValueSerializer::deserialize(json, type, &size);
    EXPECT_LT(size, json.size()) << json;
  }
}

} // namespace
} // namespace apache::thrift::dynamic
