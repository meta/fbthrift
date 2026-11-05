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

#include <gtest/gtest.h>
#include <thrift/test/json5/gen-cpp2/json5_test_constants.h>
#include <thrift/test/json5/gen-cpp2/json5_test_types.h>
#include <thrift/test/json5/gen-cpp2/json5_test_types_custom_protocol.h>

namespace apache::thrift {

using facebook::thrift::json5::Example;
using facebook::thrift::json5::TestCase;
using namespace facebook::thrift::json5::json5_test_constants;

class Json5DecoderTest : public testing::TestWithParam<TestCase> {};

TEST_P(Json5DecoderTest, Decode) {
  const auto& tc = GetParam();
  std::vector<std::string> inputs = {*tc.json(), *tc.json5()};
  inputs.insert(
      inputs.end(),
      tc.additionalInputs()->begin(),
      tc.additionalInputs()->end());
  for (const auto& input : inputs) {
    auto out = Json5ProtocolUtils::fromJson5<Example>(input);
    EXPECT_EQ(Json5ProtocolUtils::toBasicJson(out), *tc.json()) << input;
    EXPECT_EQ(Json5ProtocolUtils::toJson5(out), *tc.json5()) << input;
    if (tc.example()) {
      EXPECT_EQ(out, *tc.example()) << input;
    }
  }
}

INSTANTIATE_TEST_SUITE_P(
    Decode,
    Json5DecoderTest,
    testing::ValuesIn(testCases()),
    [](const auto& info) { return *info.param.name(); });

// Not assignable from std::string; filled through clear() and append(), like
// test::Buffer in thrift/lib/py3/test/BinaryTypes.h.
class AppendOnlyBuffer {
 public:
  void clear() { data_.clear(); }
  void append(const char* data, std::size_t size) { data_.append(data, size); }
  const std::string& str() const { return data_; }

 private:
  std::string data_;
};

TEST(Json5DecoderCustomTypeTest, CustomBinaryType) {
  using Tag = type::cpp_type<AppendOnlyBuffer, type::binary_t>;
  EXPECT_EQ(
      Json5ProtocolUtils::fromJson5<Tag>(R"({"utf-8": "hello"})").str(),
      "hello");
  EXPECT_EQ(Json5ProtocolUtils::fromJson5<Tag>(R"("aGVsbG8=")").str(), "hello");
}

} // namespace apache::thrift
