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

#include <limits>
#include <vector>
#include <gtest/gtest.h>
#include <folly/portability/GFlags.h>
#include <thrift/lib/cpp/protocol/TProtocolException.h>
#include <thrift/test/json5/gen-cpp2/json5_test_constants.h>
#include <thrift/test/json5/gen-cpp2/json5_test_types.h>

namespace apache::thrift {

using facebook::thrift::json5::Example;
using facebook::thrift::json5::Recursive;
using facebook::thrift::json5::TestCase;
using namespace facebook::thrift::json5::json5_test_constants;

// ── Struct encoding tests driven by thrift test data ─────────────────────────

namespace {

std::vector<TestCase> testCasesWithExample() {
  auto cases = testCases();
  std::erase_if(cases, [](const auto& tc) { return !tc.example(); });
  return cases;
}

} // namespace

class Json5EncoderStructTest : public ::testing::TestWithParam<TestCase> {};

TEST_P(Json5EncoderStructTest, Encode) {
  const auto& example = *GetParam().example();
  EXPECT_EQ(Json5ProtocolUtils::toBasicJson(example), *GetParam().json());
  EXPECT_EQ(Json5ProtocolUtils::toJson5(example), *GetParam().json5());
}

INSTANTIATE_TEST_SUITE_P(
    Encode,
    Json5EncoderStructTest,
    ::testing::ValuesIn(testCasesWithExample()),
    [](const auto& info) { return *info.param.name(); });

// ── Primitive type encoding tests ────────────────────────────────────────────

namespace {

enum class JsonFormat { Json, Json5 };

class Json5EncoderTest : public ::testing::TestWithParam<JsonFormat> {
 protected:
  bool isJson5() const { return GetParam() == JsonFormat::Json5; }

  template <class Tag, class T>
  std::string encode(const T& value) {
    if (isJson5()) {
      return Json5ProtocolUtils::toJson5<Tag>(value);
    }
    return Json5ProtocolUtils::toBasicJson<Tag>(value);
  }

  template <class T>
  std::string encode(const T& value) {
    if (isJson5()) {
      return Json5ProtocolUtils::toJson5(value);
    }
    return Json5ProtocolUtils::toBasicJson(value);
  }
};

INSTANTIATE_TEST_SUITE_P(
    /*InstantiationName=*/,
    /*TestSuiteName=*/Json5EncoderTest,
    /*param_generator=*/::testing::Values(JsonFormat::Json, JsonFormat::Json5),
    /*name_generator=*/[](const ::testing::TestParamInfo<JsonFormat>& info) {
      return info.param == JsonFormat::Json5 ? "Json5" : "Json";
    });

TEST_P(Json5EncoderTest, SerializeBool) {
  EXPECT_EQ(encode<type::bool_t>(true), "true");
  EXPECT_EQ(encode<type::bool_t>(false), "false");
}

TEST_P(Json5EncoderTest, SerializeInt) {
  EXPECT_EQ(encode<type::i32_t>(0), "0");
  EXPECT_EQ(encode<type::i32_t>(-0), "0");
  EXPECT_EQ(encode<type::i32_t>(42), "42");
  EXPECT_EQ(encode<type::i32_t>(-42), "-42");
  EXPECT_EQ(
      encode<type::i32_t>(std::numeric_limits<int32_t>::max()), "2147483647");
  EXPECT_EQ(
      encode<type::i32_t>(std::numeric_limits<int32_t>::min()), "-2147483648");
}

TEST_P(Json5EncoderTest, SerializeFloat) {
  EXPECT_EQ(encode<type::double_t>(0.0), "0.0");
  EXPECT_EQ(encode<type::double_t>(1.5), "1.5");
  EXPECT_EQ(encode<type::double_t>(-1.5), "-1.5");
  EXPECT_EQ(encode<type::double_t>(-0.0), "-0.0");
  EXPECT_EQ(
      encode<type::double_t>(
          0.1000000000000000055511151231257827021181583404541015625),
      "0.1");
  EXPECT_EQ(encode<type::float_t>(-0.0), "-0.0");
  EXPECT_EQ(encode<type::float_t>(0.100000001490116119384765625F), "0.1");
}

TEST_P(Json5EncoderTest, SerializeNanInf) {
  if (!isJson5()) {
    EXPECT_EQ(
        encode<type::double_t>(std::numeric_limits<double>::quiet_NaN()),
        R"("NaN")");
    EXPECT_EQ(
        encode<type::double_t>(-std::numeric_limits<double>::quiet_NaN()),
        R"("-NaN")");
    EXPECT_EQ(
        encode<type::double_t>(std::numeric_limits<double>::infinity()),
        R"("Infinity")");
    EXPECT_EQ(
        encode<type::double_t>(-std::numeric_limits<double>::infinity()),
        R"("-Infinity")");
    EXPECT_EQ(
        encode<type::float_t>(std::numeric_limits<float>::quiet_NaN()),
        R"("NaN")");
    EXPECT_EQ(
        encode<type::float_t>(-std::numeric_limits<float>::quiet_NaN()),
        R"("-NaN")");
    EXPECT_EQ(
        encode<type::float_t>(std::numeric_limits<float>::infinity()),
        R"("Infinity")");
    EXPECT_EQ(
        encode<type::float_t>(-std::numeric_limits<float>::infinity()),
        R"("-Infinity")");
  } else {
    EXPECT_EQ(
        encode<type::double_t>(std::numeric_limits<double>::quiet_NaN()),
        "NaN");
    EXPECT_EQ(
        encode<type::double_t>(-std::numeric_limits<double>::quiet_NaN()),
        "-NaN");
    EXPECT_EQ(
        encode<type::double_t>(std::numeric_limits<double>::infinity()),
        "Infinity");
    EXPECT_EQ(
        encode<type::double_t>(-std::numeric_limits<double>::infinity()),
        "-Infinity");
    EXPECT_EQ(
        encode<type::float_t>(std::numeric_limits<float>::quiet_NaN()), "NaN");
    EXPECT_EQ(
        encode<type::float_t>(-std::numeric_limits<float>::quiet_NaN()),
        "-NaN");
    EXPECT_EQ(
        encode<type::float_t>(std::numeric_limits<float>::infinity()),
        "Infinity");
    EXPECT_EQ(
        encode<type::float_t>(-std::numeric_limits<float>::infinity()),
        "-Infinity");
  }
}

TEST_P(Json5EncoderTest, SerializeString) {
  EXPECT_EQ(encode<type::string_t>(std::string("")), R"("")");
  EXPECT_EQ(
      encode<type::string_t>(std::string("Hello, world!")),
      R"("Hello, world!")");
  EXPECT_EQ(
      encode<type::string_t>(std::string(R"(Say "Hello"!)")),
      R"("Say \"Hello\"!")");
  EXPECT_EQ(
      encode<type::string_t>(std::string(R"""(Hello,
world!)""")),
      "\"Hello,\\nworld!\"");
  EXPECT_EQ(encode<type::string_t>(std::string("👋")), R"("👋")");
  // U+FFFD (replacement character) is valid UTF-8
  EXPECT_EQ(
      encode<type::string_t>(std::string("\xEF\xBF\xBD")), "\"\xEF\xBF\xBD\"");
}

TEST_P(Json5EncoderTest, StringValueInvalidUtf8) {
  Example data;
  data.stringValue() = "\x80\x81\x82";
  EXPECT_THROW((void)encode(data), std::runtime_error);
}

// Each level nests a struct and a list, i.e. two levels of protocol depth.
Recursive nested(int levels) {
  Recursive value;
  auto& children = value.children().ensure();
  if (levels > 1) {
    children.push_back(nested(levels - 1));
  }
  return value;
}

void encode(const Recursive& value) {
  (void)Json5ProtocolUtils::toJson5(value);
}

TEST(Json5EncoderDepthLimitTest, BoundsNesting) {
  const int levels = FLAGS_thrift_protocol_max_depth / 2;
  EXPECT_NO_THROW(encode(nested(levels)));
  EXPECT_THROW(encode(nested(levels + 1)), protocol::TProtocolException);
}

TEST(Json5EncoderDepthLimitTest, SiblingsDoNotAccumulateDepth) {
  Recursive child;
  child.children().ensure();
  child.byName().ensure();
  child.ids().ensure();
  // Leaking one level per sibling would exceed the limit.
  Recursive value;
  value.children() =
      std::vector<Recursive>(FLAGS_thrift_protocol_max_depth, child);
  EXPECT_NO_THROW(encode(value));
}

} // namespace

} // namespace apache::thrift
