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

#include <thrift/lib/cpp2/protocol/test/Json5EnumDecoder.h>

#include <gtest/gtest.h>
#include <thrift/test/json5/gen-cpp2/json5_test_types.h>

namespace apache::thrift::test {

using facebook::thrift::json5::Enum;

TEST(Json5EnumDecoderTest, DecodeInSeparateLibrary) {
  // Links the enum's generated library ahead of the decoder library.
  std::string_view name;
  ASSERT_TRUE(TEnumTraits<Enum>::findName(Enum::ONE, &name));
  EXPECT_EQ(name, "ONE");

  EXPECT_EQ(
      decodeEnumWithJson5("\"ONE (1)\""), static_cast<std::int32_t>(Enum::ONE));
  EXPECT_EQ(
      decodeEnumWithJson5("\"TWO\""), static_cast<std::int32_t>(Enum::TWO));
  EXPECT_EQ(
      decodeEnumWithJson5("\"(3)\""), static_cast<std::int32_t>(Enum::MANY));
  EXPECT_EQ(
      decodeEnumWithJson5("-1"), static_cast<std::int32_t>(Enum::NEGATIVE_ONE));
  EXPECT_THROW(decodeEnumWithJson5("\"THREE\""), std::exception);
  EXPECT_THROW(decodeEnumWithJson5("\"ONE (2)\""), std::exception);
}

} // namespace apache::thrift::test
