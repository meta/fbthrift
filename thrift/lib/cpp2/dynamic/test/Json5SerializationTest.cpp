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

#include <thrift/lib/cpp2/dynamic/Serialization.h>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <folly/io/IOBuf.h>
#include <folly/io/IOBufQueue.h>
#include <thrift/lib/cpp2/dynamic/DynamicValue.h>
#include <thrift/lib/cpp2/dynamic/test/gen-cpp2/json5_serialization_test_types.h>
#include <thrift/lib/cpp2/dynamic/test/gen-cpp2/json5_serialization_test_types_custom_protocol.h>
#include <thrift/lib/cpp2/protocol/CompactProtocol.h>
#include <thrift/lib/cpp2/protocol/Json5Protocol.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>
#include <thrift/lib/cpp2/runtime/SchemaRegistry.h>
#include <thrift/test/json5/gen-cpp2/json5_compatibility_test_constants.h>
#include <thrift/test/json5/gen-cpp2/json5_compatibility_test_types.h>
#include <thrift/test/json5/gen-cpp2/json5_negative_test_constants.h>
#include <thrift/test/json5/gen-cpp2/json5_negative_test_types.h>
#include <thrift/test/json5/gen-cpp2/json5_test_constants.h>
#include <thrift/test/json5/gen-cpp2/json5_test_types.h>
#include <thrift/test/json5/gen-cpp2/json5_test_types_custom_protocol.h>

#include <cstdint>
#include <limits>
#include <map>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace apache::thrift::dynamic {
namespace {

namespace gen = json5_test;
namespace shared = facebook::thrift::json5;
using json5::detail::Json5ProtocolReader;
using json5::detail::Json5ProtocolWriter;
using json5::detail::JsonWriterOptions;
using testing::HasSubstr;

template <typename T>
type_system::TypeRef typeOf() {
  return SchemaRegistry::get().getTypeSystemNode<T>().asRef();
}

// Converts through Compact, which the dynamic path already supports.
template <typename T>
DynamicValue toDynamic(const T& value) {
  auto buf = CompactSerializer::serialize<folly::IOBufQueue>(value).move();
  CompactProtocolReader reader;
  reader.setInput(buf.get());
  return deserializeValue(reader, typeOf<T>());
}

template <typename T>
T toStatic(const DynamicConstRef& value) {
  folly::IOBufQueue queue;
  CompactProtocolWriter writer;
  writer.setOutput(&queue);
  serializeValue(writer, value);
  return CompactSerializer::deserialize<T>(queue.move().get());
}

Json5ProtocolWriter::Options basicJsonOptions() {
  return {.writer = {.indentWidth = 2}};
}

Json5ProtocolWriter::Options json5Options() {
  auto writer = json5::detail::kJson5Options;
  writer.indentWidth = 2;
  return {.writer = writer};
}

// Each of these changes how some values are encoded.
std::vector<Json5ProtocolWriter::Options> allWriterOptions() {
  std::vector<Json5ProtocolWriter::Options> ret;
  for (const auto& writer :
       {JsonWriterOptions{},
        JsonWriterOptions{.indentWidth = 2},
        json5Options().writer}) {
    ret.push_back({.writer = writer});
    ret.push_back({.writer = writer, .enumAsInteger = true});
    ret.push_back({.writer = writer, .binaryAsBase64String = true});
    ret.push_back({.writer = writer, .mapPrimitiveKeysAsMemberNames = true});
    ret.push_back(
        {.writer = writer,
         .binaryAsBase64String = true,
         .mapPrimitiveKeysAsMemberNames = true});
  }
  return ret;
}

std::string toJson(
    const DynamicConstRef& value,
    const Json5ProtocolWriter::Options& options = json5Options()) {
  folly::IOBufQueue queue;
  Json5ProtocolWriter writer(COPY_EXTERNAL_BUFFER, options);
  writer.setOutput(&queue);
  serializeValue(writer, value);
  return queue.moveAsValue().toString();
}

template <typename T>
void expectSameJsonAsStatic(const T& value) {
  const auto dynamicValue = toDynamic(value);
  for (const auto& options : allWriterOptions()) {
    EXPECT_EQ(
        toJson(dynamicValue, options),
        json5::detail::toJsonImpl<type::infer_tag<T>>(value, options));
  }
}

DynamicValue fromJson(std::string_view json, const type_system::TypeRef& type) {
  auto buf = folly::IOBuf::copyBuffer(json);
  Json5ProtocolReader reader;
  reader.setInput(buf.get());
  auto value = deserializeValue(reader, type);
  if (!reader.getCursor().isAtEnd()) {
    throw protocol::TProtocolException(
        protocol::TProtocolException::INVALID_DATA,
        "unexpected trailing content after JSON value");
  }
  return value;
}

template <typename T>
T fromJsonViaDynamic(std::string_view json) {
  return toStatic<T>(fromJson(json, typeOf<T>()));
}

template <typename T>
void expectSameReadAsStatic(std::string_view json) {
  EXPECT_EQ(fromJsonViaDynamic<T>(json), Json5ProtocolUtils::fromJson5<T>(json))
      << json;
}

template <typename T>
void expectBothReadsThrow(std::string_view json) {
  EXPECT_THROW(fromJson(json, typeOf<T>()), protocol::TProtocolException)
      << json;
  EXPECT_THROW(
      (void)Json5ProtocolUtils::fromJson5<T>(json),
      protocol::TProtocolException)
      << json;
}

gen::Key makeKey(std::int32_t first, std::int32_t second) {
  gen::Key key;
  key.first() = first;
  key.second() = second;
  return key;
}

gen::Choice makeNumberChoice(std::int32_t number) {
  gen::Choice choice;
  choice.number() = number;
  return choice;
}

gen::Choice makeTextChoice(std::string text) {
  gen::Choice choice;
  choice.text() = std::move(text);
  return choice;
}

gen::Choice makeColorChoice(gen::Color color) {
  gen::Choice choice;
  choice.color() = color;
  return choice;
}

gen::Scalars makeScalars() {
  gen::Scalars scalars;
  scalars.boolField() = true;
  scalars.byteField() = -7;
  scalars.i32Field() = 32;
  scalars.i16Field() = -16;
  scalars.i64Field() = std::numeric_limits<std::int64_t>::max();
  scalars.floatField() = 1.5f;
  scalars.doubleField() = -0.0;
  scalars.stringField() = R"(hello "world")";
  scalars.binaryField() = std::string("\x00\xff\x10", 3);
  scalars.enumField() = gen::Color::BLUE;
  scalars.optionalField() = "set";
  return scalars;
}

// Multi-element containers, whose static iteration order differs from both
// insertion order and the order of their stable comparison.
gen::AllTypes makeAllTypes() {
  gen::AllTypes all;
  all.scalars() = makeScalars();
  auto& containers = *all.containers();
  containers.intList() = {3, 1, 2};
  containers.enumList() = {gen::Color::BLUE, gen::Color::RED};
  containers.unionList() = {
      makeColorChoice(gen::Color::RED), makeTextChoice("t")};
  containers.stringSet() = {"b", "a", "c"};
  containers.intSet() = {5, -5, 0};
  containers.binarySet() = {"\xff", "a", ""};
  containers.enumSet() = {gen::Color::BLUE, gen::Color::RED};
  containers.structSet() = {makeKey(1, 9), makeKey(2, 0), makeKey(1, 3)};
  containers.unionSet() = {
      makeColorChoice(gen::Color::BLUE),
      makeNumberChoice(4),
      makeTextChoice("z")};
  containers.listSet() = {{1, 2}, {1}, {0, 5}};
  containers.stringMap() = {{"b", 2}, {"a", 1}};
  containers.intKeyMap() = {{2, "two"}, {-1, "neg"}};
  containers.enumKeyMap() = {{gen::Color::GREEN, "g"}, {gen::Color::RED, "r"}};
  containers.structKeyMap() = {{makeKey(5, 6), "k"}, {makeKey(1, 7), "j"}};
  containers.binaryKeyMap() = {{"bk", 9}, {"\x01", 1}};
  containers.enumValueMap() = {{"v", gen::Color::RED}, {"u", gen::Color::BLUE}};
  all.choice() = makeNumberChoice(42);
  all.key() = makeKey(7, 8);
  return all;
}

TEST(Json5SerializationTest, WritesStructFieldsInFieldIdOrder) {
  const auto key = makeKey(1, 2);
  EXPECT_EQ(toJson(toDynamic(key)), "{\n  first: 1,\n  second: 2,\n}");
  expectSameJsonAsStatic(key);
}

TEST(Json5SerializationTest, WritesEnumsByName) {
  gen::Containers containers;
  containers.enumList() = {gen::Color::RED, gen::Color::BLUE};
  containers.enumSet() = {gen::Color::GREEN};
  containers.enumKeyMap() = {{gen::Color::RED, "r"}};
  containers.enumValueMap() = {{"b", gen::Color::BLUE}};
  containers.unionList() = {makeColorChoice(gen::Color::GREEN)};

  const auto json = toJson(toDynamic(containers));
  EXPECT_THAT(json, HasSubstr("\"RED (1)\""));
  EXPECT_THAT(json, HasSubstr("\"GREEN (2)\""));
  EXPECT_THAT(json, HasSubstr("\"BLUE (3)\""));
  expectSameJsonAsStatic(containers);
}

TEST(Json5SerializationTest, WritesTopLevelEnumByName) {
  const auto value = DynamicValue::makeEnum(
      SchemaRegistry::get().getTypeSystemNode<gen::Color>(), 2);
  EXPECT_THAT(toJson(value), HasSubstr("GREEN (2)"));
  EXPECT_EQ(
      toJson(value),
      Json5ProtocolUtils::toJson5<type::enum_t<gen::Color>>(gen::Color::GREEN));
}

TEST(Json5SerializationTest, WritesUnknownEnumValueAsParenthesizedNumber) {
  auto scalars = makeScalars();
  scalars.enumField() = static_cast<gen::Color>(99);
  EXPECT_THAT(toJson(toDynamic(scalars)), HasSubstr("\"(99)\""));
  expectSameJsonAsStatic(scalars);
}

TEST(Json5SerializationTest, WritesNonStringMapKeysAsKeyValueArray) {
  gen::Containers containers;
  containers.intKeyMap() = {{1, "a"}};
  containers.structKeyMap() = {{makeKey(1, 2), "k"}};
  containers.binaryKeyMap() = {{"bin", 3}};
  EXPECT_THAT(toJson(toDynamic(containers)), HasSubstr("key: 1,"));
  expectSameJsonAsStatic(containers);
}

TEST(Json5SerializationTest, WritesScalarsLikeStaticPath) {
  expectSameJsonAsStatic(makeScalars());

  auto special = makeScalars();
  special.floatField() = std::numeric_limits<float>::quiet_NaN();
  special.doubleField() = -std::numeric_limits<double>::infinity();
  special.optionalField().reset();
  expectSameJsonAsStatic(special);
}

TEST(Json5SerializationTest, WritesUnionsLikeStaticPath) {
  expectSameJsonAsStatic(makeNumberChoice(7));
  expectSameJsonAsStatic(makeTextChoice("seven"));
  expectSameJsonAsStatic(makeColorChoice(gen::Color::RED));
  expectSameJsonAsStatic(gen::Choice{});
}

TEST(Json5SerializationTest, WritesSingleElementContainersLikeStaticPath) {
  gen::AllTypes all;
  all.scalars() = makeScalars();
  auto& containers = *all.containers();
  containers.intList() = {3, 1, 2};
  containers.enumList() = {gen::Color::BLUE, gen::Color::RED};
  containers.unionList() = {makeColorChoice(gen::Color::RED)};
  containers.stringSet() = {"only"};
  containers.intSet() = {-5};
  containers.binarySet() = {"b"};
  containers.enumSet() = {gen::Color::RED};
  containers.structSet() = {makeKey(3, 4)};
  containers.unionSet() = {makeColorChoice(gen::Color::BLUE)};
  containers.listSet() = {{1, 2}};
  containers.stringMap() = {{"a", 1}};
  containers.intKeyMap() = {{-1, "neg"}};
  containers.enumKeyMap() = {{gen::Color::GREEN, "g"}};
  containers.structKeyMap() = {{makeKey(5, 6), "k"}};
  containers.binaryKeyMap() = {{"bk", 9}};
  containers.enumValueMap() = {{"v", gen::Color::RED}};
  all.choice()->text() = "chosen";
  all.key() = makeKey(7, 8);
  expectSameJsonAsStatic(all);
}

TEST(Json5SerializationTest, RoundTripsThroughJson) {
  const auto value = toDynamic(makeAllTypes());
  for (const auto& options : allWriterOptions()) {
    EXPECT_EQ(fromJson(toJson(value, options), typeOf<gen::AllTypes>()), value);
  }
}

TEST(Json5SerializationTest, ReadsStaticOutput) {
  const auto all = makeAllTypes();
  for (const auto& options : allWriterOptions()) {
    EXPECT_EQ(
        fromJsonViaDynamic<gen::AllTypes>(
            json5::detail::toJsonImpl<type::infer_tag<gen::AllTypes>>(
                all, options)),
        all);
  }
}

TEST(Json5SerializationTest, ReadsEnumsInEveryAcceptedForm) {
  for (std::string_view json : {"\"BLUE (3)\"", "\"BLUE\"", "\"(3)\"", "3"}) {
    EXPECT_EQ(fromJson(json, typeOf<gen::Color>()).asEnum(), 3) << json;
  }
  // Accepted as a value added to the enum after the reader's schema.
  EXPECT_EQ(fromJson("\"PURPLE (99)\"", typeOf<gen::Color>()).asEnum(), 99);

  expectSameReadAsStatic<gen::Containers>(R"json({
    enumList: ["RED (1)", "GREEN", "(3)", 2],
    enumKeyMap: {"BLUE (3)": "b", RED: "r", "(2)": "g"},
    enumValueMap: {x: "GREEN (2)"},
    unionList: [{color: "BLUE"}],
  })json");
}

TEST(Json5SerializationTest, RejectsInvalidEnums) {
  for (std::string_view json :
       {"\"PURPLE\"", "\"RED (2)\"", "\"GREEN (1)\"", "true"}) {
    EXPECT_THROW(
        fromJson(json, typeOf<gen::Color>()), protocol::TProtocolException)
        << json;
  }
  expectBothReadsThrow<gen::Scalars>(R"json({enumField: "PURPLE"})json");
}

TEST(Json5SerializationTest, ReadsFieldsByIdOrByNameAndId) {
  EXPECT_EQ(
      fromJsonViaDynamic<gen::Key>(R"json({"(1)": 10, "second (2)": 20})json"),
      makeKey(10, 20));
  expectSameReadAsStatic<gen::Key>(R"json({"(1)": 10, "second (2)": 20})json");
  expectSameReadAsStatic<gen::Choice>(R"json({"(2)": "text"})json");
  // Unknown fields are skipped, whether named or numbered.
  expectSameReadAsStatic<gen::Key>(
      R"json({third: [1, {a: 2}], "(3)": 2, first: 5})json");
}

TEST(Json5SerializationTest, RejectsConflictingFieldNameAndId) {
  expectBothReadsThrow<gen::Key>(R"json({"first (2)": 1})json");
  expectBothReadsThrow<gen::Key>(R"json({"other (1)": 1})json");
  expectBothReadsThrow<gen::Choice>(R"json({"text (1)": "x"})json");
}

TEST(Json5SerializationTest, ReadsLenientEncodingsLikeStaticPath) {
  expectSameReadAsStatic<gen::Scalars>(R"json({
    boolField: "true",
    byteField: "-8",
    i32Field: "42",
    i64Field: "-9000000000",
    floatField: "-Infinity",
    doubleField: "1e3",
    binaryField: {"base64url": "AP8Q"},
    stringField: 'single quoted',
    // comments and null fields are ignored
    optionalField: null,
  })json");
  expectSameReadAsStatic<gen::Scalars>(
      R"json({binaryField: {"utf-8": "abc"}, doubleField: Infinity})json");
  expectSameReadAsStatic<gen::Containers>(R"json({
    stringMap: [{key: "a", value: 1}, {key: "b", value: 2}],
    intKeyMap: {"7": "seven", "-1": "neg"},
    stringSet: ["x", "y",],
  })json");
}

TEST(Json5SerializationTest, RejectsMismatchedTypes) {
  expectBothReadsThrow<gen::Scalars>(R"json({i32Field: "abc"})json");
  expectBothReadsThrow<gen::Scalars>(R"json({stringField: 1})json");
  expectBothReadsThrow<gen::Scalars>(R"json({boolField: 1})json");
  expectBothReadsThrow<gen::Containers>(R"json({intList: {}})json");
  expectBothReadsThrow<gen::Containers>(R"json({stringMap: 1})json");
  expectBothReadsThrow<gen::AllTypes>(R"json({key: [1, 2]})json");
}

struct RecordingCallbacks {
  std::vector<std::string> unknownFields;

  void onUnknownField(
      const type_system::StructuredNode&,
      int16_t,
      std::string_view name,
      protocol::TType) {
    unknownFields.emplace_back(name);
  }
  void onTypeMismatch(std::string_view, protocol::TType, protocol::TType) {}
  void onMultipleUnionFields(const type_system::UnionNode&, uint32_t) {}
};

TEST(Json5SerializationTest, ReportsUnknownFieldsToCallbacks) {
  auto buf = folly::IOBuf::copyBuffer(
      R"json({first: 1, extra: [1, {a: 2}], second: 2})json");
  Json5ProtocolReader reader;
  reader.setInput(buf.get());
  RecordingCallbacks callbacks;

  auto value = deserializeValue(reader, typeOf<gen::Key>(), nullptr, callbacks);

  EXPECT_EQ(toStatic<gen::Key>(value), makeKey(1, 2));
  EXPECT_EQ(callbacks.unknownFields, std::vector<std::string>{"extra"});
}

TEST(Json5SerializationTest, WritesSetsAndMapsInStableOrderLikeStaticPath) {
  expectSameJsonAsStatic(makeAllTypes());
}

TEST(Json5SerializationTest, SetAndMapOrderDoesNotDependOnInsertionOrder) {
  const auto& containersNode =
      SchemaRegistry::get().getTypeSystemNode<gen::Containers>();
  auto ascendingSet =
      DynamicValue::makeDefault(containersNode.at("intSet").type());
  auto descendingSet = ascendingSet;
  auto ascendingMap =
      DynamicValue::makeDefault(containersNode.at("stringMap").type());
  auto descendingMap = ascendingMap;
  std::set<std::int64_t> expectedSet;
  std::map<std::string, std::int32_t> expectedMap;
  for (std::int32_t i = 0; i < 100; ++i) {
    ascendingSet.asSet().insert(DynamicValue::makeI64(i - 50));
    descendingSet.asSet().insert(DynamicValue::makeI64(49 - i));
    expectedSet.insert(i - 50);
    ascendingMap.asMap().insert(
        DynamicValue::makeString(std::to_string(i)), DynamicValue::makeI32(i));
    descendingMap.asMap().insert(
        DynamicValue::makeString(std::to_string(99 - i)),
        DynamicValue::makeI32(99 - i));
    expectedMap.emplace(std::to_string(i), i);
  }

  const auto setJson =
      Json5ProtocolUtils::toJson5<type::set<type::i64_t>>(expectedSet);
  EXPECT_EQ(toJson(ascendingSet), setJson);
  EXPECT_EQ(toJson(descendingSet), setJson);
  const auto mapJson =
      Json5ProtocolUtils::toJson5<type::map<type::string_t, type::i32_t>>(
          expectedMap);
  EXPECT_EQ(toJson(ascendingMap), mapJson);
  EXPECT_EQ(toJson(descendingMap), mapJson);
}

class Json5SharedTest : public testing::TestWithParam<shared::TestCase> {};

TEST_P(Json5SharedTest, Writes) {
  const auto& tc = GetParam();
  if (!tc.example()) {
    GTEST_SKIP();
  }
  const auto value = toDynamic(*tc.example());
  EXPECT_EQ(toJson(value, basicJsonOptions()), *tc.json());
  EXPECT_EQ(toJson(value), *tc.json5());
}

TEST_P(Json5SharedTest, Reads) {
  const auto& tc = GetParam();
  std::vector<std::string> inputs = {*tc.json(), *tc.json5()};
  inputs.insert(
      inputs.end(),
      tc.additionalInputs()->begin(),
      tc.additionalInputs()->end());
  for (const auto& input : inputs) {
    const auto value = fromJson(input, typeOf<shared::Example>());
    EXPECT_EQ(toJson(value, basicJsonOptions()), *tc.json()) << input;
    EXPECT_EQ(toJson(value), *tc.json5()) << input;
    if (tc.example()) {
      EXPECT_EQ(toStatic<shared::Example>(value), *tc.example()) << input;
    }
  }
}

INSTANTIATE_TEST_SUITE_P(
    Shared,
    Json5SharedTest,
    testing::ValuesIn(shared::json5_test_constants::testCases()),
    [](const auto& info) { return *info.param.name(); });

class Json5SharedCompatibilityTest
    : public testing::TestWithParam<shared::CompatibilityTestCase> {};

TEST_P(Json5SharedCompatibilityTest, Reads) {
  for (const auto& json : *GetParam().inputs()) {
    EXPECT_EQ(fromJsonViaDynamic<shared::Example>(json), *GetParam().output())
        << json;
  }
}

INSTANTIATE_TEST_SUITE_P(
    Shared,
    Json5SharedCompatibilityTest,
    testing::ValuesIn(
        shared::json5_compatibility_test_constants::compatibilityTestCases()),
    [](const auto& info) { return *info.param.name(); });

class Json5SharedRoundTripTest
    : public testing::TestWithParam<shared::RoundTripTestCase> {};

TEST_P(Json5SharedRoundTripTest, WritesReadValue) {
  for (const auto& json : *GetParam().inputs()) {
    const auto value = fromJson(json, typeOf<shared::Example>());
    EXPECT_EQ(toJson(value, basicJsonOptions()), *GetParam().json()) << json;
    EXPECT_EQ(toJson(value), *GetParam().json5()) << json;
  }
}

INSTANTIATE_TEST_SUITE_P(
    Shared,
    Json5SharedRoundTripTest,
    testing::ValuesIn(
        shared::json5_compatibility_test_constants::roundTripTestCases()),
    [](const auto& info) { return *info.param.name(); });

class Json5SharedNegativeTest
    : public testing::TestWithParam<shared::NegativeTestCase> {};

TEST_P(Json5SharedNegativeTest, Rejects) {
  EXPECT_THROW(
      fromJson(*GetParam().json(), typeOf<shared::Example>()),
      protocol::TProtocolException);
}

INSTANTIATE_TEST_SUITE_P(
    EnumValidation,
    Json5SharedNegativeTest,
    testing::ValuesIn(
        shared::json5_negative_test_constants::enumValidationNegativeCases()),
    [](const auto& info) { return *info.param.name(); });

INSTANTIATE_TEST_SUITE_P(
    TypeValidation,
    Json5SharedNegativeTest,
    testing::ValuesIn(
        shared::json5_negative_test_constants::typeValidationNegativeCases()),
    [](const auto& info) { return *info.param.name(); });

INSTANTIATE_TEST_SUITE_P(
    TypeMismatch,
    Json5SharedNegativeTest,
    testing::ValuesIn(
        shared::json5_negative_test_constants::typeMismatchNegativeCases()),
    [](const auto& info) { return *info.param.name(); });

INSTANTIATE_TEST_SUITE_P(
    FormatValidation,
    Json5SharedNegativeTest,
    testing::ValuesIn(
        shared::json5_negative_test_constants::formatValidationNegativeCases()),
    [](const auto& info) { return *info.param.name(); });

INSTANTIATE_TEST_SUITE_P(
    OverflowValidation,
    Json5SharedNegativeTest,
    testing::ValuesIn(
        shared::json5_negative_test_constants::
            overflowValidationNegativeCases()),
    [](const auto& info) { return *info.param.name(); });

class Json5SharedInvalidUtf8Test
    : public testing::TestWithParam<shared::InvalidUtf8TestCase> {};

TEST_P(Json5SharedInvalidUtf8Test, Rejects) {
  EXPECT_THROW(
      fromJson(*GetParam().json(), typeOf<shared::Example>()),
      protocol::TProtocolException);
}

INSTANTIATE_TEST_SUITE_P(
    Shared,
    Json5SharedInvalidUtf8Test,
    testing::ValuesIn(
        shared::json5_negative_test_constants::invalidUtf8NegativeCases()),
    [](const auto& info) { return std::string(*info.param.name()); });

} // namespace
} // namespace apache::thrift::dynamic
