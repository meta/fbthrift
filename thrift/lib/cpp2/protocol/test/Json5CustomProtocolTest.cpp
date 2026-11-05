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

#include <thrift/lib/cpp2/protocol/Json5Protocol.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <unordered_map>
#include <vector>
#include <gtest/gtest.h>
#include <folly/json/dynamic.h>
#include <folly/json/json.h>
#include <thrift/lib/cpp2/type/Tag.h>
#include <thrift/test/gen-cpp2/References_types.h>
#include <thrift/test/json5/gen-cpp2/json5_test_constants.h>
#include <thrift/test/json5/gen-cpp2/json5_test_types.h>
#include <thrift/test/json5/gen-cpp2/json5_test_types_custom_protocol.h>

namespace apache::thrift {

using facebook::thrift::json5::Example;
using facebook::thrift::json5::TestCase;
using namespace facebook::thrift::json5::json5_test_constants;
using json5::detail::Json5ProtocolReader;
using json5::detail::Json5ProtocolWriter;
using json5::detail::kJson5Options;

namespace {

Example readExample(std::string_view json) {
  auto buf = folly::IOBuf::copyBuffer(json);
  Json5ProtocolReader reader;
  reader.setInput(buf.get());
  Example example;
  example.read(&reader);
  return example;
}

std::string writeExample(
    const Example& example, const Json5ProtocolWriter::Options& options) {
  folly::IOBufQueue queue;
  Json5ProtocolWriter writer(COPY_EXTERNAL_BUFFER, options);
  writer.setOutput(&queue);
  example.write(&writer);
  return queue.moveAsValue().toString();
}

} // namespace

// ── Struct tests driven by thrift test data ─────────────────────────────────

class Json5CustomProtocolTest : public ::testing::TestWithParam<TestCase> {};

TEST_P(Json5CustomProtocolTest, Decode) {
  const auto& tc = GetParam();
  auto json5Options = kJson5Options;
  json5Options.indentWidth = 2;
  std::vector<std::string> inputs = {*tc.json(), *tc.json5()};
  inputs.insert(
      inputs.end(),
      tc.additionalInputs()->begin(),
      tc.additionalInputs()->end());
  for (const auto& input : inputs) {
    auto out = readExample(input);
    EXPECT_EQ(writeExample(out, {.writer = {.indentWidth = 2}}), *tc.json())
        << input;
    EXPECT_EQ(writeExample(out, {.writer = json5Options}), *tc.json5())
        << input;
    if (tc.example()) {
      EXPECT_EQ(out, *tc.example()) << input;
    }
  }
}

TEST_P(Json5CustomProtocolTest, Encode) {
  const auto& tc = GetParam();
  if (!tc.example()) {
    GTEST_SKIP();
  }
  auto json5Options = kJson5Options;
  json5Options.indentWidth = 2;
  EXPECT_EQ(
      writeExample(*tc.example(), {.writer = {.indentWidth = 2}}), *tc.json());
  EXPECT_EQ(writeExample(*tc.example(), {.writer = json5Options}), *tc.json5());
}

INSTANTIATE_TEST_SUITE_P(
    Shared,
    Json5CustomProtocolTest,
    ::testing::ValuesIn(testCases()),
    [](const auto& info) { return *info.param.name(); });

// ── Tests for mapPrimitiveKeysAsMemberNames option
// ────────────────────────────

class Json5MapPrimitiveKeysTest : public ::testing::Test {
 protected:
  static std::string writeJson(const Example& example) {
    return writeExample(
        example, {.writer = {}, .mapPrimitiveKeysAsMemberNames = true});
  }
  static std::string writeJson5(const Example& example) {
    return writeExample(
        example,
        {.writer = kJson5Options, .mapPrimitiveKeysAsMemberNames = true});
  }
  static std::string writeJsonDefault(const Example& example) {
    return writeExample(example, {});
  }
  static std::string writeJson5Default(const Example& example) {
    return writeExample(example, {.writer = kJson5Options});
  }
};

TEST_F(Json5MapPrimitiveKeysTest, BoolAsKey) {
  Example example;
  example.boolAsKey() = {{true, 1}};
  EXPECT_EQ(writeJson(example), R"RAW({"boolAsKey":{"true":1}})RAW");
  EXPECT_EQ(writeJson5(example), R"RAW({boolAsKey:{true:1,},})RAW");
}

TEST_F(Json5MapPrimitiveKeysTest, I32AsKey) {
  Example example;
  example.i32AsKey() = {{1, 2}};
  EXPECT_EQ(writeJson(example), R"RAW({"i32AsKey":{"1":2}})RAW");
  EXPECT_EQ(writeJson5(example), R"RAW({i32AsKey:{"1":2,},})RAW");
}

TEST_F(Json5MapPrimitiveKeysTest, MultiEntryI32AsKey) {
  Example example;
  example.i32AsKey() = {{3, 4}, {1, 2}};
  EXPECT_EQ(writeJson(example), R"RAW({"i32AsKey":{"1":2,"3":4}})RAW");
  EXPECT_EQ(writeJson5(example), R"RAW({i32AsKey:{"1":2,"3":4,},})RAW");
}

TEST_F(Json5MapPrimitiveKeysTest, I64AsKey) {
  Example example;
  example.i64AsKey() = {{42, 1}};
  EXPECT_EQ(writeJson(example), R"RAW({"i64AsKey":{"42":1}})RAW");
  EXPECT_EQ(writeJson5(example), R"RAW({i64AsKey:{"42":1,},})RAW");
}

TEST_F(Json5MapPrimitiveKeysTest, BinaryAsKey) {
  Example example;
  example.binaryAsKey() = {{"?~", 1}, {"abcd", 2}, {"\xff", 3}};
  auto json = writeJson(example);
  EXPECT_EQ(json, R"RAW({"binaryAsKey":{"P34":1,"YWJjZA":2,"/w":3}})RAW");
  EXPECT_EQ(Json5ProtocolUtils::fromJson5<Example>(json), example);
  auto json5 = writeJson5(example);
  EXPECT_EQ(json5, R"RAW({binaryAsKey:{P34:1,YWJjZA:2,"/w":3,},})RAW");
  EXPECT_EQ(Json5ProtocolUtils::fromJson5<Example>(json5), example);
}

TEST_F(Json5MapPrimitiveKeysTest, BinaryAsKeyBase64) {
  Example example;
  example.binaryAsKey() = {{"?~", 1}};
  auto json = writeExample(
      example,
      {.writer = {},
       .binaryAsBase64String = true,
       .mapPrimitiveKeysAsMemberNames = true});
  EXPECT_EQ(json, R"RAW({"binaryAsKey":{"P34":1}})RAW");
  EXPECT_EQ(Json5ProtocolUtils::fromJson5<Example>(json), example);
}

TEST_F(Json5MapPrimitiveKeysTest, WholeNumberFloatWithoutFraction) {
  using DoubleMap = type::map<type::double_t, type::double_t>;
  const std::map<double, double> value{{12345, 1}, {0.5, 2.5}};
  Json5ProtocolWriter::Options options{
      .writer = {}, .mapPrimitiveKeysAsMemberNames = true};
  EXPECT_EQ(
      toJsonImpl<DoubleMap>(value, options), R"({"0.5":2.5,"12345.0":1.0})");

  options.writer.wholeNumberFloatWithoutFraction = true;
  auto json = toJsonImpl<DoubleMap>(value, options);
  EXPECT_EQ(json, R"({"0.5":2.5,"12345":1})");
  EXPECT_EQ(Json5ProtocolUtils::fromJson5<DoubleMap>(json), value);
  EXPECT_EQ(
      (toJsonImpl<type::map<type::float_t, type::float_t>>(
          {{-3, -3}}, options)),
      R"({"-3":-3})");
}

TEST_F(Json5MapPrimitiveKeysTest, StringAndEnumKeysUnchanged) {
  for (const auto& tc : testCases()) {
    const auto& name = *tc.name();
    if (name == "StringAsKey" || name == "MultiEntryStringAsKey" ||
        name == "EnumAsKey") {
      EXPECT_EQ(writeJson(*tc.example()), writeJsonDefault(*tc.example()))
          << name;
      EXPECT_EQ(writeJson5(*tc.example()), writeJson5Default(*tc.example()))
          << name;
    }
  }
}

TEST_F(Json5MapPrimitiveKeysTest, NonPrimitiveKeysUnchanged) {
  for (const auto& tc : testCases()) {
    const auto& name = *tc.name();
    if (name == "StructAsKey" || name == "ListAsKey" || name == "SetAsKey" ||
        name == "OutOfOrderFieldsInMap") {
      EXPECT_EQ(writeJson(*tc.example()), writeJsonDefault(*tc.example()))
          << name;
      EXPECT_EQ(writeJson5(*tc.example()), writeJson5Default(*tc.example()))
          << name;
    }
  }
}

TEST_F(Json5MapPrimitiveKeysTest, MapAsKey) {
  Example example;
  std::map<int32_t, int32_t> innerMap = {{1, 2}};
  example.mapAsKey() = {{innerMap, 3}};
  EXPECT_EQ(
      writeJson(example), R"RAW({"mapAsKey":[{"key":{"1":2},"value":3}]})RAW");
  EXPECT_EQ(
      writeJson5(example), R"RAW({mapAsKey:[{key:{"1":2,},value:3,},],})RAW");
}

TEST(Json5CustomProtocolExtraTest, CustomBinaryAppendsOnlyNonEmptyValues) {
  struct Binary {
    std::string data;
    int appends = 0;
    void clear() { data.clear(); }
    void append(const char* p, std::size_t n) {
      ++appends;
      data.append(p, n);
    }
  };
  auto read = [](std::string_view json) {
    auto buf = folly::IOBuf::copyBuffer(json);
    Json5ProtocolReader reader;
    reader.setInput(buf.get());
    Binary binary;
    reader.readBinary(binary);
    return binary;
  };
  auto emptyBinary = read(R"("")");
  EXPECT_EQ(emptyBinary.appends, 0);
  EXPECT_EQ(emptyBinary.data, "");
  auto nonEmpty = read(R"("AAEC")");
  EXPECT_EQ(nonEmpty.appends, 1);
  EXPECT_EQ(nonEmpty.data, std::string("\x00\x01\x02", 3));
}

// SimpleJSON writes -0.0 as `-0`.
TEST(Json5CustomProtocolExtraTest, NegativeZeroInteger) {
  auto firstKey = []<class Tag>(Tag, std::string_view json) {
    return Json5ProtocolUtils::fromJson5<Tag>(json).begin()->first;
  };
  using FloatKeyed = type::map<type::float_t, type::i32_t>;
  using DoubleKeyed = type::map<type::double_t, type::i32_t>;
  // A value after a non-primitive key, in key/value-array form.
  using ListKeyed = type::map<type::list<type::i32_t>, type::double_t>;
  EXPECT_TRUE(
      std::signbit(
          Json5ProtocolUtils::fromJson5<ListKeyed>(
              R"([{"key": [1], "value": -0}])")
              .begin()
              ->second));
  EXPECT_FALSE(std::signbit(firstKey(FloatKeyed{}, R"({"0": 1})")));
  EXPECT_FALSE(std::signbit(firstKey(DoubleKeyed{}, R"({"0": 1})")));
}

struct ClassDerivedFromThriftStruct : facebook::thrift::json5::NonFinalStruct {
};

TEST(Json5CustomProtocolExtraTest, ClassDerivedFromThriftStruct) {
  auto buf = folly::IOBuf::copyBuffer(R"({"value": 42})");
  Json5ProtocolReader reader;
  reader.setInput(buf.get());
  ClassDerivedFromThriftStruct derived;
  op::decode<type::struct_t<ClassDerivedFromThriftStruct>>(reader, derived);
  EXPECT_EQ(*derived.value(), 42);
}

TEST(Json5CustomProtocolExtraTest, InternBoxField) {
  auto obj = Json5ProtocolUtils::fromJson5<cpp2::StructuredAnnotation>(
      R"({"intern_box_field": {"field": 1}})");
  EXPECT_EQ(obj.intern_box_field()->field(), 1);
}

// ── Tests for Json5ProtocolWriter::Options ──────────────────────────────────

TEST(Json5WriterOptionsTest, EnumAsInteger) {
  Example example;
  example.enumValue() = facebook::thrift::json5::Enum::TWO;

  auto defaultResult = writeExample(example, {});
  EXPECT_EQ(defaultResult, R"RAW({"enumValue":"TWO (2)"})RAW");

  auto result = writeExample(example, {.writer = {}, .enumAsInteger = true});
  EXPECT_EQ(result, R"RAW({"enumValue":2})RAW");
}

TEST(Json5WriterOptionsTest, BinaryAsBase64String) {
  Example example;
  example.binaryValue() = std::string("\x00\x01\x02", 3);

  auto defaultResult = writeExample(example, {});
  EXPECT_EQ(defaultResult, R"RAW({"binaryValue":{"base64url":"AAEC"}})RAW");

  auto result =
      writeExample(example, {.writer = {}, .binaryAsBase64String = true});
  EXPECT_EQ(result, R"RAW({"binaryValue":"AAEC"})RAW");
}

// ── Tests for keyOrder option ──────────────────────────────────────────────

TEST(Json5WriterOptionsTest, KeyOrder) {
  Json5ProtocolWriter defaultWriter;
  EXPECT_EQ(defaultWriter.keyOrder(), KeyOrder::StableAscending);

  Json5ProtocolWriter ascendingWriter(
      COPY_EXTERNAL_BUFFER,
      {.writer = {}, .keyOrder = KeyOrder::StableAscending});
  EXPECT_EQ(ascendingWriter.keyOrder(), KeyOrder::StableAscending);

  Json5ProtocolWriter unspecifiedWriter(
      COPY_EXTERNAL_BUFFER, {.writer = {}, .keyOrder = KeyOrder::Unspecified});
  EXPECT_EQ(unspecifiedWriter.keyOrder(), KeyOrder::Unspecified);
}

namespace {

std::vector<int64_t> extractEmittedKeys(const std::string& json) {
  std::vector<int64_t> keys;
  for (const auto& entry : folly::parseJson(json)) {
    keys.push_back(entry["key"].asInt());
  }
  return keys;
}

} // namespace

TEST(Json5WriterOptionsTest, KeyOrderControlsMapKeyOutputOrder) {
  using Tag = type::cpp_type<
      std::unordered_map<int64_t, int64_t>,
      type::map<type::i64_t, type::i64_t>>;

  std::unordered_map<int64_t, int64_t> m;
  for (int64_t i = 0; i < 100; ++i) {
    m[i] = i;
  }

  std::vector<int64_t> iterationOrder;
  iterationOrder.reserve(m.size());
  for (const auto& [k, _] : m) {
    iterationOrder.push_back(k);
  }

  // Unspecified: output order == unordered_map iteration order.
  {
    auto json = json5::detail::toJsonImpl<Tag>(
        m, {.writer = {}, .keyOrder = KeyOrder::Unspecified});
    EXPECT_EQ(extractEmittedKeys(json), iterationOrder);
  }

  // Default (StableAscending): output keys are sorted.
  {
    auto json = json5::detail::toJsonImpl<Tag>(m, {});
    auto emittedKeys = extractEmittedKeys(json);
    EXPECT_EQ(emittedKeys.size(), m.size());
    EXPECT_TRUE(std::is_sorted(emittedKeys.begin(), emittedKeys.end()));
  }
}

} // namespace apache::thrift
