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

#include <thrift/lib/cpp2/dynamic/StableCompare.h>

#include <gtest/gtest.h>

#include <folly/io/IOBufQueue.h>
#include <thrift/lib/cpp2/dynamic/DynamicValue.h>
#include <thrift/lib/cpp2/dynamic/Serialization.h>
#include <thrift/lib/cpp2/dynamic/TypeSystemBuilder.h>
#include <thrift/lib/cpp2/dynamic/ValueConversion.h>
#include <thrift/lib/cpp2/dynamic/test/gen-cpp2/json5_serialization_test_types.h>
#include <thrift/lib/cpp2/op/detail/Compare.h>
#include <thrift/lib/cpp2/protocol/CompactProtocol.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>
#include <thrift/lib/cpp2/runtime/SchemaRegistry.h>

#include <compare>
#include <cstdint>
#include <functional>
#include <limits>
#include <string>
#include <vector>

namespace apache::thrift::dynamic {
namespace {

namespace gen = json5_test;
using def = type_system::TypeSystemBuilder::DefinitionHelper;

template <typename T>
DynamicValue toDynamic(const T& value) {
  auto buf = CompactSerializer::serialize<folly::IOBufQueue>(value).move();
  CompactProtocolReader reader;
  reader.setInput(buf.get());
  return deserializeValue(
      reader, SchemaRegistry::get().getTypeSystemNode<T>().asRef());
}

template <typename T>
void expectSameOrderAsStatic(const std::vector<T>& values) {
  for (const auto& lhs : values) {
    for (const auto& rhs : values) {
      EXPECT_EQ(
          detail::stableCompare(toDynamic(lhs), toDynamic(rhs)),
          op::detail::StableComparePolicy<type::infer_tag<T>>{}(lhs, rhs))
          << toDynamic(lhs).debugString() << " vs "
          << toDynamic(rhs).debugString();
    }
  }
}

template <typename T>
std::vector<T> variantsOf(std::vector<std::function<void(T&)>> setters) {
  std::vector<T> ret(1);
  for (const auto& set : setters) {
    set(ret.emplace_back());
  }
  return ret;
}

gen::Key makeKey(std::int32_t first, std::int32_t second) {
  gen::Key key;
  key.first() = first;
  key.second() = second;
  return key;
}

TEST(StableCompareTest, OrdersStructFieldsById) {
  // Key declares `second` before `first`.
  expectSameOrderAsStatic(
      std::vector<gen::Key>{
          makeKey(1, 9), makeKey(2, 0), makeKey(1, 3), makeKey(0, 100)});
}

TEST(StableCompareTest, OrdersUnionsByActiveFieldThenValue) {
  using C = gen::Choice;
  expectSameOrderAsStatic(
      variantsOf<C>({
          [](C& c) { c.number() = 4; },
          [](C& c) { c.number() = 5; },
          [](C& c) { c.text() = "a"; },
          [](C& c) { c.text() = "b"; },
          [](C& c) { c.color() = gen::Color::RED; },
          [](C& c) { c.color() = gen::Color::BLUE; },
      }));
}

TEST(StableCompareTest, OrdersScalarsLikeStaticPath) {
  using S = gen::Scalars;
  expectSameOrderAsStatic(
      variantsOf<S>({
          [](S& s) { s.boolField() = true; },
          [](S& s) { s.byteField() = -1; },
          [](S& s) { s.byteField() = 1; },
          [](S& s) { s.i16Field() = -300; },
          [](S& s) { s.i32Field() = 7; },
          [](S& s) { s.i64Field() = std::numeric_limits<std::int64_t>::min(); },
          [](S& s) { s.floatField() = -0.0f; },
          [](S& s) {
            s.floatField() = std::numeric_limits<float>::quiet_NaN();
          },
          [](S& s) { s.doubleField() = 0.5; },
          [](S& s) {
            s.doubleField() = -std::numeric_limits<double>::infinity();
          },
          [](S& s) { s.stringField() = "a"; },
          [](S& s) { s.stringField() = "\xff"; },
          [](S& s) { s.stringField() = "ab"; },
          [](S& s) { s.binaryField() = "a"; },
          [](S& s) { s.binaryField() = "\xff"; },
          [](S& s) { s.enumField() = gen::Color::RED; },
          [](S& s) { s.enumField() = gen::Color::BLUE; },
          [](S& s) { s.optionalField() = ""; },
          [](S& s) { s.optionalField() = "z"; },
      }));
}

TEST(StableCompareTest, OrdersContainersLikeStaticPath) {
  using C = gen::Containers;
  expectSameOrderAsStatic(
      variantsOf<C>({
          [](C& c) { c.intList() = {1, 2}; },
          [](C& c) { c.intList() = {1}; },
          [](C& c) { c.intList() = {0, 5}; },
          [](C& c) { c.stringSet() = {"b"}; },
          [](C& c) { c.stringSet() = {"a", "c"}; },
          [](C& c) { c.intSet() = {-5, 3}; },
          [](C& c) { c.intSet() = {-5, 2, 9}; },
          [](C& c) { c.binarySet() = {"\xff"}; },
          [](C& c) { c.binarySet() = {"a", "b"}; },
          [](C& c) { c.enumSet() = {gen::Color::BLUE}; },
          [](C& c) { c.structSet() = {makeKey(1, 2), makeKey(0, 3)}; },
          [](C& c) { c.structSet() = {makeKey(0, 4)}; },
          [](C& c) {
            gen::Choice text;
            text.text() = "t";
            gen::Choice number;
            number.number() = 1;
            c.unionSet() = {text, number};
          },
          [](C& c) { c.listSet() = {{1, 2}, {1}}; },
          [](C& c) { c.listSet() = {{0, 9}}; },
          [](C& c) { c.stringMap() = {{"a", 1}}; },
          [](C& c) { c.stringMap() = {{"a", 2}}; },
          [](C& c) { c.stringMap() = {{"a", 1}, {"b", 0}}; },
          [](C& c) { c.intKeyMap() = {{-1, "x"}, {1, "y"}}; },
          [](C& c) { c.structKeyMap() = {{makeKey(2, 1), "x"}}; },
          [](C& c) { c.structKeyMap() = {{makeKey(1, 2), "y"}}; },
          [](C& c) { c.binaryKeyMap() = {{"\x01", 1}, {"\xff", 2}}; },
      }));
}

TEST(StableCompareTest, ComparesOpaqueAliasesByTargetType) {
  type_system::TypeSystemBuilder builder;
  builder.addType(
      "meta.com/thrift/test/IntAlias",
      def::OpaqueAlias(type_system::TypeIds::I32));
  auto typeSystem = std::move(builder).build();
  const auto aliasType =
      typeSystem->UserDefined("meta.com/thrift/test/IntAlias");

  const auto one = toDynamicValue(1, aliasType);
  const auto two = toDynamicValue(2, aliasType);

  EXPECT_EQ(detail::stableCompare(one, two), std::partial_ordering::less);
  EXPECT_EQ(detail::stableCompare(two, one), std::partial_ordering::greater);
  EXPECT_EQ(detail::stableCompare(one, one), std::partial_ordering::equivalent);
}

} // namespace
} // namespace apache::thrift::dynamic
