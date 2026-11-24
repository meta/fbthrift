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

// Tests JSON decoder negative cases for rejecting invalid input formats.
// The JSON5 decoder should reject malformed or type-mismatched values
// to ensure strict validation of incoming data.

#include <thrift/lib/cpp2/protocol/Json5Protocol.h>

#include <cstdint>
#include <string>

#include <gtest/gtest.h>
#include <folly/lang/Pretty.h>
#include <folly/portability/GFlags.h>
#include <thrift/lib/cpp/protocol/TProtocolException.h>
#include <thrift/test/json5/gen-cpp2/json5_negative_test_constants.h>
#include <thrift/test/json5/gen-cpp2/json5_negative_test_types.h>
#include <thrift/test/json5/gen-cpp2/json5_test_types.h>
#include <thrift/test/json5/gen-cpp2/json5_test_types_custom_protocol.h>

namespace apache::thrift {

using facebook::thrift::json5::Example;
using facebook::thrift::json5::InvalidUtf8TestCase;
using facebook::thrift::json5::NegativeTestCase;
using facebook::thrift::json5::Recursive;
using namespace facebook::thrift::json5::json5_negative_test_constants;

class JsonDecoderNegativeTest
    : public ::testing::TestWithParam<NegativeTestCase> {};

TEST_P(JsonDecoderNegativeTest, RejectsInvalidInput) {
  try {
    (void)Json5ProtocolUtils::fromJson5<Example>(*GetParam().json());
    ADD_FAILURE() << "expected the reader to reject the input";
  } catch (const protocol::TProtocolException& e) {
    EXPECT_EQ(e.getType(), protocol::TProtocolException::INVALID_DATA)
        << e.what();
  }
}

class JsonDecoderInvalidUtf8Test
    : public ::testing::TestWithParam<InvalidUtf8TestCase> {};

TEST_P(JsonDecoderInvalidUtf8Test, RejectsInvalidInput) {
  EXPECT_THROW(
      (void)Json5ProtocolUtils::fromJson5<Example>(*GetParam().json()),
      protocol::TProtocolException);
}

INSTANTIATE_TEST_SUITE_P(
    InvalidUtf8,
    JsonDecoderInvalidUtf8Test,
    ::testing::ValuesIn(invalidUtf8NegativeCases()),
    [](const auto& info) { return std::string(*info.param.name()); });

TEST(JsonDecoderErrorTypeTest, TypeMismatchNamesTheFoundType) {
  try {
    (void)Json5ProtocolUtils::fromJson5<Example>(R"({"boolValue": 1})");
    ADD_FAILURE() << "expected rejection";
  } catch (const protocol::TProtocolException& e) {
    auto expected = std::string("cannot parse `") +
        folly::pretty_name<std::int64_t>() + "` as `bool`";
    EXPECT_NE(std::string_view(e.what()).find(expected), std::string_view::npos)
        << e.what();
  }
}

namespace {
// Each level nests a struct and a list, i.e. two levels of protocol depth.
std::string nested(int levels, std::string_view innermost = "") {
  std::string open, close;
  for (int i = 0; i < levels; ++i) {
    open += R"({"children": [)";
    close += "]}";
  }
  return open + std::string(innermost) + close;
}

void decode(std::string_view json) {
  (void)Json5ProtocolUtils::fromJson5<Recursive>(json);
}
} // namespace

TEST(JsonDecoderDepthLimitTest, BoundsTypedNesting) {
  const int levels = FLAGS_thrift_protocol_max_depth / 2;
  EXPECT_NO_THROW(decode(nested(levels)));
  EXPECT_THROW(decode(nested(levels + 1)), protocol::TProtocolException);
  EXPECT_THROW(decode(nested(100'000)), protocol::TProtocolException);
}

TEST(JsonDecoderDepthLimitTest, SkippedFieldsShareTheBudget) {
  // Nesting 20 short of the limit, plus the innermost struct, leaves room for
  // 19 skipped arrays.
  auto withSkippedArrays = [](int n) {
    return nested(
        FLAGS_thrift_protocol_max_depth / 2 - 10,
        R"({"unknown": )" + std::string(n, '[') + std::string(n, ']') + "}");
  };
  EXPECT_NO_THROW(decode(withSkippedArrays(19)));
  EXPECT_THROW(decode(withSkippedArrays(20)), protocol::TProtocolException);
}

TEST(JsonDecoderDepthLimitTest, SiblingsDoNotAccumulateDepth) {
  // Every kind of container, including a skipped object, gives its depth back.
  // Leaking one level per sibling would exceed the limit.
  std::string json = R"({"children": [)";
  for (int i = 0; i < FLAGS_thrift_protocol_max_depth; ++i) {
    json +=
        R"({"children": [], "byName": {}, "ids": [], "unknown": {"a": {}}},)";
  }
  EXPECT_NO_THROW(decode(json + "]}"));
}

INSTANTIATE_TEST_SUITE_P(
    EnumValidation,
    JsonDecoderNegativeTest,
    ::testing::ValuesIn(enumValidationNegativeCases()),
    [](const auto& info) { return std::string(*info.param.name()); });

INSTANTIATE_TEST_SUITE_P(
    TypeValidation,
    JsonDecoderNegativeTest,
    ::testing::ValuesIn(typeValidationNegativeCases()),
    [](const auto& info) { return std::string(*info.param.name()); });

INSTANTIATE_TEST_SUITE_P(
    TypeMismatch,
    JsonDecoderNegativeTest,
    ::testing::ValuesIn(typeMismatchNegativeCases()),
    [](const auto& info) { return std::string(*info.param.name()); });

INSTANTIATE_TEST_SUITE_P(
    FormatValidation,
    JsonDecoderNegativeTest,
    ::testing::ValuesIn(formatValidationNegativeCases()),
    [](const auto& info) { return std::string(*info.param.name()); });

INSTANTIATE_TEST_SUITE_P(
    OverflowValidation,
    JsonDecoderNegativeTest,
    ::testing::ValuesIn(overflowValidationNegativeCases()),
    [](const auto& info) { return std::string(*info.param.name()); });

} // namespace apache::thrift
