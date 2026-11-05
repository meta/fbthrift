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

#include <thrift/lib/cpp2/type/Runtime.h>

#include <any>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <folly/io/IOBuf.h>
#include <thrift/lib/cpp2/type/Id.h>
#include <thrift/lib/cpp2/type/Tag.h>
#include <thrift/lib/thrift/gen-cpp2/schema_types.h>

using apache::thrift::type::DecodedUri;

namespace apache::thrift::type {
namespace {

// Test the exact case of AlignedPtr used by RuntimeType.
TEST(AlignedPtr, InfoPointer) {
  using info_pointer = detail::AlignedPtr<const detail::TypeInfo, 2>;
  EXPECT_EQ(
      ~info_pointer::kPointerMask, 3); // Everything except the last 2 bits.
  const auto* fullPtr = (const detail::TypeInfo*)(info_pointer::kPointerMask);

  info_pointer empty, full{fullPtr, 3};
  EXPECT_FALSE(empty.get<0>());
  EXPECT_FALSE(empty.get<1>());
  EXPECT_EQ(empty.get(), nullptr);
  EXPECT_TRUE(full.get<0>());
  EXPECT_TRUE(full.get<1>());
  EXPECT_EQ(full.get(), fullPtr);

  empty.set<0>();
  full.clear<0>();
  EXPECT_TRUE(empty.get<0>());
  EXPECT_FALSE(empty.get<1>());
  EXPECT_EQ(empty.get(), nullptr);
  EXPECT_FALSE(full.get<0>());
  EXPECT_TRUE(full.get<1>());
  EXPECT_EQ(full.get(), fullPtr);

  empty.set<1>();
  full.clear<1>();
  EXPECT_TRUE(empty.get<0>());
  EXPECT_TRUE(empty.get<1>());
  EXPECT_EQ(empty.get(), nullptr);
  EXPECT_FALSE(full.get<0>());
  EXPECT_FALSE(full.get<1>());
  EXPECT_EQ(full.get(), fullPtr);

  empty.clear<0>();
  full.set<0>();
  EXPECT_FALSE(empty.get<0>());
  EXPECT_TRUE(empty.get<1>());
  EXPECT_EQ(empty.get(), nullptr);
  EXPECT_TRUE(full.get<0>());
  EXPECT_FALSE(full.get<1>());
  EXPECT_EQ(full.get(), fullPtr);
}

TEST(RuntimeTest, Void) {
  Ref ref;
  EXPECT_EQ(ref.type(), Type::get<void_t>());
  EXPECT_TRUE(ref.empty());
  EXPECT_THROW(ref.get(FieldId{1}), std::logic_error);
  EXPECT_THROW(ref.get("field1"), std::logic_error);

  EXPECT_THROW(ref.as<string_t>(), std::bad_any_cast);
  EXPECT_TRUE(ref.tryAs<string_t>() == nullptr);
}

TEST(RuntimeTest, Int) {
  int32_t value = 1;
  Ref ref = Ref::to(value);
  EXPECT_EQ(ref.type(), Type::get<i32_t>());
  EXPECT_FALSE(ref.empty());
  EXPECT_EQ(ref.as<i32_t>(), Ref::to(1).as<i32_t>());
  EXPECT_EQ(ref.as<i32_t>(), Value::of(1).as<i32_t>());

  ref.mut<i32_t>() = 2;
  EXPECT_EQ(value, 2);
}

TEST(RuntimeTest, List) {
  std::vector<std::string> value = {"best"};
  auto ref = Ref::to<list<string_t>>(value);
  EXPECT_FALSE(ref.empty());
  EXPECT_EQ(ref.size(), 1);
  EXPECT_THAT(ref.as<list<string_t>>(), ::testing::ElementsAre("best"));
  EXPECT_THAT(ref, ::testing::SizeIs(1));
  EXPECT_EQ(ref.at(0).as<string_t>(), "best");
  EXPECT_EQ(ref.at(Ordinal{1}).as<string_t>(), "best");
  EXPECT_THROW(ref.at(1), std::out_of_range);
  EXPECT_THROW(ref.get(FieldId{1}), std::logic_error);
  EXPECT_THROW(ref.get("field1"), std::logic_error);

  value.clear();
  EXPECT_TRUE(ref.empty());
  EXPECT_THAT(ref, ::testing::IsEmpty());
}

TEST(RuntimeTest, Set) {
  std::set<std::string> value;
  auto ref = Ref::to<set<string_t>>(value);
  EXPECT_TRUE(ref.empty());
  EXPECT_EQ(ref.size(), 0);
  EXPECT_THROW(ref.values().begin(), std::logic_error);

  value.insert("best");
  EXPECT_FALSE(ref.empty());
  EXPECT_THAT(ref, ::testing::SizeIs(1));
  EXPECT_THROW(ref.get(FieldId{1}), std::logic_error);
  EXPECT_FALSE(ref.contains("the"));
  EXPECT_TRUE(ref.contains("best"));
}

TEST(RuntimeTest, Map) {
  std::map<std::string, int> value;
  auto ref = Ref::to<map<string_t, i32_t>>(value);
  EXPECT_TRUE(ref.empty());
  EXPECT_EQ(ref.size(), 0);
  EXPECT_THROW(ref.begin(), std::runtime_error);

  value["one"] = 1;
  EXPECT_FALSE(ref.empty());
  EXPECT_EQ(ref.size(), 1);
  EXPECT_TRUE(ref.contains("one"));
  EXPECT_EQ(ref.at("one").as<i32_t>(), 1);
  EXPECT_FALSE(ref.contains("two"));
  EXPECT_EQ(ref.get("two"), nullptr);
  EXPECT_THROW(ref.at("two"), std::out_of_range);
  EXPECT_THROW(ref.get(FieldId{1}), std::logic_error);
}

TEST(RuntimeTest, Struct) {
  DecodedUri actual;
  auto ref = Ref::to(actual);
  EXPECT_TRUE(ref.empty());
  EXPECT_EQ(ref.size(), 5);
  EXPECT_EQ(ref.get(FieldId{1}).as<string_t>(), "");

  actual.scheme() = "foo";
  EXPECT_EQ(ref.get(FieldId{1}).as<string_t>(), "foo");
  EXPECT_EQ(ref.get("scheme").as<string_t>(), "foo");

  EXPECT_THROW(ref.get(FieldId{}), std::out_of_range);
  EXPECT_THROW(ref.get("bad"), std::out_of_range);
  EXPECT_THROW(ref.at("bad"), std::out_of_range);
}

TEST(RuntimeTest, ConstRef) {
  constexpr int32_t one = 1;
  auto ref = Ref::to(one);
  EXPECT_FALSE(ref.empty());
  EXPECT_EQ(ref.as<i32_t>(), 1);
  // Cannot be modified.
  EXPECT_THROW(ref.mut<i32_t>(), std::logic_error);
}

TEST(RuntimeTest, VoidValue) {
  Value value;
  EXPECT_EQ(value.type(), Type::get<void_t>());
  EXPECT_TRUE(value.empty());

  EXPECT_THROW(value.as<string_t>(), std::bad_any_cast);
  EXPECT_TRUE(value.tryAs<string_t>() == nullptr);
}

TEST(RuntimeTest, IntValue) {
  Value value = Value::of<i32_t>(1);
  EXPECT_EQ(value.type(), Type::get<i32_t>());
  EXPECT_FALSE(value.empty());
  EXPECT_EQ(value.as<i32_t>(), 1);

  value.as<i32_t>() = 2;
  EXPECT_EQ(value.as<i32_t>(), 2);
}

TEST(RuntimeTest, ListValue) {
  Value value;
  value = Value::of<list<string_t>>(std::vector<std::string>{});
  EXPECT_TRUE(value.empty());
  EXPECT_THROW(value.at(0), std::out_of_range);
  EXPECT_THROW(value.at(Ordinal{1}), std::out_of_range);

  value.as<list<string_t>>().emplace_back("hi");
  EXPECT_FALSE(value.empty());
  Value other(value);
  EXPECT_FALSE(other.empty());
  value.as<list<string_t>>().clear();
  EXPECT_TRUE(value.empty());
  value = other;
  EXPECT_FALSE(value.empty());
}

TEST(RuntimeTest, SetValue) {
  Value value;
  value = Value::of<set<string_t>>(std::set<std::string>{});
  EXPECT_TRUE(value.empty());
  value.as<set<string_t>>().emplace("hi");
  EXPECT_FALSE(value.empty());
  Value other(value);
  EXPECT_FALSE(other.empty());
  value.as<set<string_t>>().clear();
  EXPECT_TRUE(value.empty());
  value = other;
  EXPECT_FALSE(value.empty());
}

TEST(RuntimeTest, MapValue) {
  Value value;
  value =
      Value::of<map<string_t, string_t>>(std::map<std::string, std::string>{});
  EXPECT_TRUE(value.empty());
  value.as<map<string_t, string_t>>().emplace("hi", "bye");
  EXPECT_FALSE(value.empty());
  Value other(value);
  EXPECT_FALSE(other.empty());
  value.as<map<string_t, string_t>>().clear();
  EXPECT_TRUE(value.empty());
  value = other;
  EXPECT_FALSE(value.empty());
}

TEST(RuntimeTest, IntInterOp) {
  int8_t data = 1;
  auto smallInt = Ref::to(data);
  auto largeInt = Value::of<i64_t>(2);

  EXPECT_EQ(smallInt.as<byte_t>(), int8_t(1));
  EXPECT_LT(largeInt.as<i64_t>(), int64_t(5L));

  EXPECT_EQ(smallInt.as<byte_t>(), 1);
  EXPECT_EQ(static_cast<double>(smallInt.as<byte_t>()), 1.0);
  EXPECT_EQ(largeInt.as<i64_t>(), 2);
  EXPECT_EQ(static_cast<float>(largeInt.as<i64_t>()), 2.0f);
  EXPECT_NE(static_cast<float>(largeInt.as<i64_t>()), 2.1f);
  EXPECT_NE(static_cast<double>(smallInt.as<byte_t>()), 0.9);
  EXPECT_LT(static_cast<float>(smallInt.as<byte_t>()), 1.5f);
  EXPECT_GT(static_cast<double>(largeInt.as<i64_t>()), 1.5);
}

TEST(RuntimeTest, FloatInterOp) {
  EXPECT_EQ(
      Ref::to<double_t>(2.0).as<double_t>(),
      static_cast<double>(Ref::to<float_t>(2.0f).as<float_t>()));
  EXPECT_LT(
      static_cast<double>(Ref::to<float_t>(1.0).as<float_t>()),
      Ref::to<double_t>(2.0f).as<double_t>());
  EXPECT_GT(
      Ref::to<double_t>(std::numeric_limits<double>::infinity()).as<double_t>(),
      static_cast<double>(
          Ref::to<float_t>(-std::numeric_limits<float>::infinity())
              .as<float_t>()));
}

TEST(RuntimeTest, StringBinaryInterOp) {
  using STag = type::string_t;
  using BTag = type::cpp_type<folly::IOBuf, type::binary_t>;

  auto stringHi = Value::of<STag>("hi");
  auto stringBye = Value::of<STag>("bye");

  auto binaryHiBuf = folly::IOBuf::wrapBufferAsValue("hi", 2);
  auto binaryHi = Ref::to<BTag>(binaryHiBuf);
  auto binaryByeBuf = folly::IOBuf::wrapBufferAsValue("bye", 3);
  auto binaryBye = ConstRef::to<BTag>(binaryByeBuf);

  EXPECT_EQ(stringHi.as<STag>(), binaryHi.as<BTag>().to<std::string>());
  EXPECT_EQ(binaryBye.as<BTag>().to<std::string>(), stringBye.as<STag>());
  EXPECT_NE(binaryHi.as<BTag>().to<std::string>(), stringBye.as<STag>());
  EXPECT_GT(stringHi.as<STag>(), binaryBye.as<BTag>().to<std::string>());
}

} // namespace
} // namespace apache::thrift::type
