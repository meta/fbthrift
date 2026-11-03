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

#include <thrift/test/CppAllocatorTest.h>

#include <cstdint>
#include <map>
#include <set>
#include <type_traits>
#include <vector>

#include <folly/ScopeGuard.h>
#include <thrift/lib/cpp2/op/Get.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>
#include <thrift/test/gen-cpp2/CppAllocatorTest_types.h>

#include <gtest/gtest.h>

namespace apache::thrift::test {

static const char* kTooLong =
    "This is too long for the small string optimization";

// Workaround an ambiguity when comparing allocators with different value types
// which was introduced in C++20.
template <typename T>
static ScopedStatefulAlloc<> get_allocator(const T& container) {
  return ScopedStatefulAlloc<>(container.get_allocator());
}

TEST(CppAllocatorTest, AlwaysThrowAllocator) {
  ScopedAlwaysThrowAlloc<> alloc;
  AlwaysThrowParent s(alloc);

  EXPECT_THROW(s.child()->aa_list()->emplace_back(42), std::bad_alloc);
  EXPECT_THROW(s.child()->aa_set()->emplace(42), std::bad_alloc);
  EXPECT_THROW(s.child()->aa_map()->emplace(42, 42), std::bad_alloc);
  EXPECT_THROW(s.child()->aa_string()->assign(kTooLong), std::bad_alloc);

  EXPECT_NO_THROW(s.child()->not_aa_list()->emplace_back(42));
  EXPECT_NO_THROW(s.child()->not_aa_set()->emplace(42));
  EXPECT_NO_THROW(s.child()->not_aa_map()->emplace(42, 42));
  EXPECT_NO_THROW(s.child()->not_aa_string()->assign(kTooLong));
}

// Giving up noexcept is scoped to the structs that actually apply a default:
// dropping one allocates nothing, so those constructors keep the guarantee.
template <typename T>
inline constexpr bool alloc_ctor_is_noexcept_v =
    std::is_nothrow_constructible_v<T, const typename T::allocator_type&>;

static_assert(alloc_ctor_is_noexcept_v<HasContainerFields>);
static_assert(alloc_ctor_is_noexcept_v<HasContainerFieldsWithDefaults>);
static_assert(!alloc_ctor_is_noexcept_v<HasContainerFieldsWithAppliedDefaults>);

TEST(CppAllocatorTest, AllocCtorAppliedDefaultPropagatesBadAlloc) {
  ScopedAlwaysThrowAlloc<> alloc;
  // Without the conditional noexcept this terminates instead of throwing.
  EXPECT_THROW(AlwaysThrowWithAppliedDefault{alloc}, std::bad_alloc);
}

TEST(CppAllocatorTest, UsesAllocatorPmr) {
  PmrByteAlloc alloc(std::pmr::null_memory_resource());
  ParentPmr s(alloc);

  EXPECT_THROW(s.child()->aa_list()->emplace_back(42), std::bad_alloc);
  EXPECT_THROW(s.child()->aa_set()->emplace(42), std::bad_alloc);
  EXPECT_THROW(s.child()->aa_map()->emplace(42, 42), std::bad_alloc);
  EXPECT_THROW(s.child()->aa_string()->assign(kTooLong), std::bad_alloc);

  EXPECT_NO_THROW(s.child()->not_aa_list()->emplace_back(42));
  EXPECT_NO_THROW(s.child()->not_aa_set()->emplace(42));
  EXPECT_NO_THROW(s.child()->not_aa_map()->emplace(42, 42));
  EXPECT_NO_THROW(s.child()->not_aa_string()->assign(kTooLong));
}

TEST(CppAllocatorTest, GetAllocator) {
  ScopedStatefulAlloc<> alloc(42);

  NoAllocatorVia s1(alloc);
  EXPECT_EQ(alloc, s1.get_allocator());

  YesAllocatorVia s2(alloc);
  EXPECT_EQ(alloc, s2.get_allocator());
}

TEST(CppAllocatorTest, GetAllocatorPmr) {
  std::pmr::monotonic_buffer_resource res;
  PmrByteAlloc alloc(&res);

  NoAllocatorViaPmr s1(alloc);
  EXPECT_EQ(alloc, s1.get_allocator());

  YesAllocatorViaPmr s2(alloc);
  EXPECT_EQ(alloc, s2.get_allocator());
}

TEST(CppAllocatorTest, AllocatorVia) {
  NoAllocatorVia s1;
  YesAllocatorVia s2;
  EXPECT_GT(sizeof(s1), sizeof(s2));
}

TEST(CppAllocatorTest, AllocatorViaPmr) {
  NoAllocatorViaPmr s1;
  YesAllocatorViaPmr s2;
  EXPECT_GT(sizeof(s1), sizeof(s2));
}

TEST(CppAllocatorTest, EmptyAllocatorTakesNoSpace) {
  static_assert(std::is_empty_v<HasEmptyAllocator::allocator_type>);
  EXPECT_EQ(sizeof(HasEmptyAllocator), sizeof(std::int8_t));
}

TEST(CppAllocatorTest, AllocCtorIgnoresCustomDefaultWhenLegacy) {
  ScopedStatefulAlloc<> alloc(42);

  // `cpp.allocator` with no `cpp.allocator_via`: the no-argument constructor is
  // itself allocator-extended, so it ignores the custom defaults too.
  HasContainerFieldsWithDefaults def;
  EXPECT_TRUE(def.aa_list()->empty());
  EXPECT_TRUE(def.aa_set()->empty());
  EXPECT_TRUE(def.aa_map()->empty());
  EXPECT_EQ(*def.plain(), 0);

  HasContainerFieldsWithDefaults s(alloc);
  EXPECT_TRUE(s.aa_list()->empty());
  EXPECT_TRUE(s.aa_set()->empty());
  EXPECT_TRUE(s.aa_map()->empty());
  EXPECT_EQ(*s.plain(), 0);

  // The defaults are ignored, not the allocator.
  EXPECT_EQ(s.get_allocator(), alloc);
  EXPECT_EQ(get_allocator(*s.aa_list()), alloc);
  EXPECT_EQ(get_allocator(*s.aa_set()), alloc);
  EXPECT_EQ(get_allocator(*s.aa_map()), alloc);

  // With `cpp.allocator_via` the no-argument constructor applies the default
  // either way; only the allocator-extended one ignores it.
  HasAllocatorViaWithDefaults viaDef;
  EXPECT_TRUE(*viaDef.flag());

  HasAllocatorViaWithDefaults via(alloc);
  EXPECT_FALSE(*via.flag());
  EXPECT_EQ(get_allocator(*via.aa_list()), alloc);
}

TEST(CppAllocatorTest, AllocCtorAppliesCustomDefault) {
  ScopedStatefulAlloc<> alloc(42);

  const std::vector<int32_t> expectedList{1, 2, 3};
  const std::set<int32_t> expectedSet{4, 5};
  const std::map<int32_t, int32_t> expectedMap{{6, 7}};

  auto asVector = [](const auto& c) {
    return std::vector<int32_t>(c.begin(), c.end());
  };
  auto asSet = [](const auto& c) {
    return std::set<int32_t>(c.begin(), c.end());
  };
  auto asMap = [](const auto& c) {
    return std::map<int32_t, int32_t>(c.begin(), c.end());
  };

  HasContainerFieldsWithAppliedDefaults def;
  EXPECT_EQ(asVector(*def.aa_list()), expectedList);
  EXPECT_EQ(asSet(*def.aa_set()), expectedSet);
  EXPECT_EQ(asMap(*def.aa_map()), expectedMap);
  EXPECT_EQ(*def.plain(), 8);

  HasContainerFieldsWithAppliedDefaults s(alloc);
  EXPECT_EQ(asVector(*s.aa_list()), expectedList);
  EXPECT_EQ(asSet(*s.aa_set()), expectedSet);
  EXPECT_EQ(asMap(*s.aa_map()), expectedMap);
  EXPECT_EQ(*s.plain(), 8);

  // The default reaches the field without displacing the allocator.
  EXPECT_EQ(s.get_allocator(), alloc);
  EXPECT_EQ(get_allocator(*s.aa_list()), alloc);
  EXPECT_EQ(get_allocator(*s.aa_set()), alloc);
  EXPECT_EQ(get_allocator(*s.aa_map()), alloc);

  HasAllocatorViaWithAppliedDefaults viaDef;
  EXPECT_TRUE(*viaDef.flag());

  HasAllocatorViaWithAppliedDefaults via(alloc);
  EXPECT_TRUE(*via.flag());
  EXPECT_EQ(get_allocator(*via.aa_list()), alloc);
}

TEST(CppAllocatorTest, Deserialize) {
  using serializer = apache::thrift::CompactSerializer;

  HasContainerFields s1;
  s1.aa_list() = {1, 2, 3};
  s1.aa_set() = {1, 2, 3};
  s1.aa_map() = {{1, 1}, {2, 2}, {3, 3}};

  auto str = serializer::serialize<std::string>(s1);

  ScopedStatefulAlloc<> alloc(42);
  HasContainerFields s2(alloc);
  EXPECT_EQ(s2.get_allocator(), alloc);
  EXPECT_EQ(get_allocator(*s2.aa_list()), alloc);
  EXPECT_EQ(get_allocator(*s2.aa_set()), alloc);
  EXPECT_EQ(get_allocator(*s2.aa_map()), alloc);

  serializer::deserialize(str, s2);
  EXPECT_EQ(s2.aa_list(), (StatefulAllocVector<int32_t>{1, 2, 3}));
  EXPECT_EQ(s2.aa_set(), (StatefulAllocSet<int32_t>{1, 2, 3}));
  EXPECT_EQ(
      s2.aa_map(),
      (StatefulAllocMap<int32_t, int32_t>{{1, 1}, {2, 2}, {3, 3}}));

  EXPECT_EQ(s2.get_allocator(), alloc);
  EXPECT_EQ(get_allocator(*s2.aa_list()), alloc);
  EXPECT_EQ(get_allocator(*s2.aa_set()), alloc);
  EXPECT_EQ(get_allocator(*s2.aa_map()), alloc);
}

TEST(CppAllocatorTest, DeserializePmr) {
  using serializer = apache::thrift::CompactSerializer;

  HasContainerFieldsPmr s1;
  s1.aa_list() = {1, 2, 3};
  s1.aa_set() = {1, 2, 3};
  s1.aa_map() = {{1, 1}, {2, 2}, {3, 3}};

  auto str = serializer::serialize<std::string>(s1);

  std::pmr::monotonic_buffer_resource res;
  PmrByteAlloc alloc(&res);

  HasContainerFieldsPmr s2(alloc);
  EXPECT_EQ(s2.get_allocator(), alloc);
  EXPECT_EQ(s2.aa_list()->get_allocator(), alloc);
  EXPECT_EQ(s2.aa_set()->get_allocator(), alloc);
  EXPECT_EQ(s2.aa_map()->get_allocator(), alloc);

  serializer::deserialize(str, s2);
  EXPECT_EQ(s2.aa_list(), (std::pmr::vector<int32_t>{1, 2, 3}));
  EXPECT_EQ(s2.aa_set(), (std::pmr::set<int32_t>{1, 2, 3}));
  EXPECT_EQ(
      s2.aa_map(), (std::pmr::map<int32_t, int32_t>{{1, 1}, {2, 2}, {3, 3}}));

  EXPECT_EQ(s2.get_allocator(), alloc);
  EXPECT_EQ(s2.aa_list()->get_allocator(), alloc);
  EXPECT_EQ(s2.aa_set()->get_allocator(), alloc);
  EXPECT_EQ(s2.aa_map()->get_allocator(), alloc);
}

TEST(CppAllocatorTest, UsesTypedef) {
  ScopedStatefulAlloc<> alloc(42);
  UsesTypedef s(alloc);
  EXPECT_EQ(alloc, s.get_allocator());
}

TEST(CppAllocatorTest, UsesTypedefPmr) {
  std::pmr::monotonic_buffer_resource res;
  PmrByteAlloc alloc(&res);
  UsesTypedefPmr s(alloc);
  EXPECT_EQ(alloc, s.get_allocator());
}

TEST(CppAllocatorTest, DeserializeNested) {
  using serializer = apache::thrift::CompactSerializer;

  HasNestedContainerFields s1;
  s1.aa_map_of_map() = {{42, {{42, 42}}}};
  s1.aa_map_of_set() = {{42, {42}}};

  auto str = serializer::serialize<std::string>(s1);

  ScopedStatefulAlloc<> alloc(42);
  HasNestedContainerFields s2(alloc);

  EXPECT_EQ(s2.get_allocator(), alloc);
  EXPECT_EQ(get_allocator(*s2.aa_map_of_map()), alloc);
  EXPECT_EQ(get_allocator(*s2.aa_map_of_set()), alloc);

  serializer::deserialize(str, s2);
  EXPECT_EQ(get_allocator(*s2.aa_map_of_map()), alloc);
  EXPECT_EQ(get_allocator(s2.aa_map_of_map()->at(42)), alloc);
  EXPECT_EQ(get_allocator(*s2.aa_map_of_set()), alloc);
  EXPECT_EQ(get_allocator(s2.aa_map_of_set()->at(42)), alloc);
}

namespace {
template <typename Serializer, typename Struct>
void checkDeserializePropagatesAllocator(const Struct& s1) {
  // Set the default resource to null_memory_resource to make sure it is never
  // used.
  auto prev = set_default_resource(std::pmr::null_memory_resource());
  auto guard = folly::makeGuard([&] { set_default_resource(prev); });

  // A distinct resource (not s1's default) so the two allocators are
  // distinguishable below.
  std::pmr::monotonic_buffer_resource fieldRes(std::pmr::new_delete_resource());
  PmrByteAlloc fieldAlloc(&fieldRes);
  Struct s2(fieldAlloc);
  Serializer::deserialize(Serializer::template serialize<std::string>(s1), s2);

  EXPECT_EQ(s1, s2);
  EXPECT_NE(s1.field()->get_allocator(), fieldAlloc);
  EXPECT_EQ(s2.field()->get_allocator(), fieldAlloc);
}

// SimpleJSON is size-omitting (consume_elem path); Compact/Binary are
// size-prefixed (deserialize_known_length_{set,map} path).
using DeserializeSerializers = ::testing::Types<
    apache::thrift::SimpleJSONSerializer,
    apache::thrift::CompactSerializer,
    apache::thrift::BinarySerializer>;

template <typename Serializer>
class CppAllocatorDeserializeTest : public ::testing::Test {};
TYPED_TEST_SUITE(CppAllocatorDeserializeTest, DeserializeSerializers);

TYPED_TEST(CppAllocatorDeserializeTest, ElementPropagatesAllocator) {
  // kTooLong disables SSO so the element/key read allocates.
  HasAllocatorAwareListElement list;
  list.field() = {kTooLong, kTooLong};
  checkDeserializePropagatesAllocator<TypeParam>(list);

  HasAllocatorAwareSetElement set;
  set.field() = {kTooLong};
  checkDeserializePropagatesAllocator<TypeParam>(set);

  HasAllocatorAwareMapElement map;
  map.field() = {{kTooLong, kTooLong}};
  checkDeserializePropagatesAllocator<TypeParam>(map);
}

TYPED_TEST(CppAllocatorDeserializeTest, SortedVectorMapPropagatesAllocator) {
  HasAllocatorAwareSortedVectorMapValue value;
  value.field() = {{1, kTooLong}};
  checkDeserializePropagatesAllocator<TypeParam>(value);
}
} // namespace

TEST(CppAllocatorTest, DeserializeNestedPmr) {
  using serializer = apache::thrift::CompactSerializer;

  HasNestedContainerFieldsPmr s1;
  s1.aa_map_of_map() = {{42, {{42, 42}}}};
  s1.aa_map_of_set() = {{42, {42}}};

  auto str = serializer::serialize<std::string>(s1);

  std::pmr::monotonic_buffer_resource res;
  PmrByteAlloc alloc(&res);

  HasNestedContainerFieldsPmr s2(alloc);

  EXPECT_EQ(s2.get_allocator(), alloc);
  EXPECT_EQ(s2.aa_map_of_map()->get_allocator(), alloc);
  EXPECT_EQ(s2.aa_map_of_set()->get_allocator(), alloc);

  serializer::deserialize(str, s2);
  EXPECT_EQ(s2.aa_map_of_map()->get_allocator(), alloc);
  EXPECT_EQ(s2.aa_map_of_map()->at(42).get_allocator(), alloc);
  EXPECT_EQ(s2.aa_map_of_set()->get_allocator(), alloc);
  EXPECT_EQ(s2.aa_map_of_set()->at(42).get_allocator(), alloc);
}

TEST(CppAllocatorTest, AllocatorExtendedCtorsKeepBoxedField) {
  std::pmr::monotonic_buffer_resource res;
  PmrByteAlloc alloc(&res);

  BoxedPayload payload;
  payload.value() = 13;

  HasBoxedField src;
  src.boxed() = 42;
  src.intern_boxed() = payload;
  src.not_boxed() = 7;
  ASSERT_TRUE(
      is_non_optional_field_set_manually_or_by_serializer(src.intern_boxed()));

  // `__isset` is copied wholesale, so the intern-boxed bit stays true even when
  // the ctor drops the value. Assert the bit first: if the value is gone, the
  // read below dereferences a null box rather than reporting a mismatch.
  HasBoxedField copied(src, alloc);
  EXPECT_EQ(copied.boxed(), 42);
  EXPECT_EQ(copied.not_boxed(), 7);
  EXPECT_TRUE(is_non_optional_field_set_manually_or_by_serializer(
      copied.intern_boxed()));
  EXPECT_EQ(copied.intern_boxed()->value(), 13);

  HasBoxedField moved(std::move(src), alloc);
  EXPECT_EQ(moved.boxed(), 42);
  EXPECT_EQ(moved.not_boxed(), 7);
  EXPECT_TRUE(is_non_optional_field_set_manually_or_by_serializer(
      moved.intern_boxed()));
  EXPECT_EQ(moved.intern_boxed()->value(), 13);
}

TEST(CppAllocatorTest, DeserializeSortedUniqueConstructible) {
  using serializer = apache::thrift::CompactSerializer;

  HasSortedUniqueConstructibleFields s1;
  s1.aa_set() = {1, 2, 3};
  s1.aa_map() = {{1, 1}, {2, 2}, {3, 3}};

  auto str = serializer::serialize<std::string>(s1);

  ScopedStatefulAlloc<> alloc(42);
  HasSortedUniqueConstructibleFields s2(alloc);
  EXPECT_EQ(s2.get_allocator(), alloc);
  EXPECT_EQ(get_allocator(*s2.aa_set()), alloc);
  EXPECT_EQ(get_allocator(*s2.aa_map()), alloc);

  serializer::deserialize(str, s2);
  EXPECT_EQ(s2.aa_set(), (StatefulAllocSortedVectorSet<int32_t>{1, 2, 3}));
  EXPECT_EQ(
      s2.aa_map(),
      (StatefulAllocSortedVectorMap<int32_t, int32_t>{{1, 1}, {2, 2}, {3, 3}}));

  EXPECT_EQ(s2.get_allocator(), alloc);
  EXPECT_EQ(get_allocator(*s2.aa_set()), alloc);
  EXPECT_EQ(get_allocator(*s2.aa_map()), alloc);
}

#define EXPECT_ALLOC(a, x)      \
  {                             \
    auto c = a.getCount();      \
    x;                          \
    EXPECT_GT(a.getCount(), c); \
  }
#define EXPECT_NO_ALLOC(a, x)   \
  {                             \
    auto c = a.getCount();      \
    x;                          \
    EXPECT_EQ(a.getCount(), c); \
  }

TEST(CppAllocatorTest, CountingAllocator) {
  ScopedCountingAlloc<> alloc;
  CountingParent s(alloc);

  EXPECT_ALLOC(alloc, s.aa_child_list()->emplace_back());
  auto& aa_child = s.aa_child_list()[0];

  EXPECT_ALLOC(alloc, aa_child.aa_list()->emplace_back(42));
  EXPECT_ALLOC(alloc, aa_child.aa_set()->emplace(42));
  EXPECT_ALLOC(alloc, aa_child.aa_map()->emplace(42, 42));
  EXPECT_ALLOC(alloc, aa_child.aa_string()->assign(kTooLong));

  EXPECT_NO_ALLOC(alloc, aa_child.not_aa_list()->emplace_back(42));
  EXPECT_NO_ALLOC(alloc, aa_child.not_aa_set()->emplace(42));
  EXPECT_NO_ALLOC(alloc, aa_child.not_aa_map()->emplace(42, 42));
  EXPECT_NO_ALLOC(alloc, aa_child.not_aa_string()->assign(kTooLong));

  EXPECT_NO_ALLOC(alloc, s.not_aa_child_list()->emplace_back());
  auto& not_aa_child = s.not_aa_child_list()[0];

  EXPECT_NO_ALLOC(alloc, not_aa_child.aa_list()->emplace_back(42));
  EXPECT_NO_ALLOC(alloc, not_aa_child.aa_set()->emplace(42));
  EXPECT_NO_ALLOC(alloc, not_aa_child.aa_map()->emplace(42, 42));
  EXPECT_NO_ALLOC(alloc, not_aa_child.aa_string()->assign(kTooLong));

  EXPECT_NO_ALLOC(alloc, not_aa_child.not_aa_list()->emplace_back(42));
  EXPECT_NO_ALLOC(alloc, not_aa_child.not_aa_set()->emplace(42));
  EXPECT_NO_ALLOC(alloc, not_aa_child.not_aa_map()->emplace(42, 42));
  EXPECT_NO_ALLOC(alloc, not_aa_child.not_aa_string()->assign(kTooLong));
}

TEST(CppAllocatorTest, DeserializeCountingStringRetainsAllocator) {
  using serializer = apache::thrift::CompactSerializer;

  CountingChild source;
  source.aa_string() = kTooLong;

  ScopedCountingAlloc<> alloc;
  CountingChild destination(alloc);
  EXPECT_EQ(destination.aa_string()->get_allocator(), alloc);

  serializer::deserialize(
      serializer::serialize<std::string>(source), destination);

  EXPECT_EQ(destination.aa_string(), kTooLong);
  EXPECT_EQ(destination.aa_string()->get_allocator(), alloc);
}

TEST(CppAllocatorTest, CountingResource) {
  CountingPmrResource res;
  PmrByteAlloc alloc(&res);
  ParentPmr s(alloc);

  EXPECT_ALLOC(res, s.aa_child_list()->emplace_back());
  auto& aa_child = s.aa_child_list()[0];

  EXPECT_ALLOC(res, aa_child.aa_list()->emplace_back(42));
  EXPECT_ALLOC(res, aa_child.aa_set()->emplace(42));
  EXPECT_ALLOC(res, aa_child.aa_map()->emplace(42, 42));
  EXPECT_ALLOC(res, aa_child.aa_string()->assign(kTooLong));

  EXPECT_NO_ALLOC(res, aa_child.not_aa_list()->emplace_back(42));
  EXPECT_NO_ALLOC(res, aa_child.not_aa_set()->emplace(42));
  EXPECT_NO_ALLOC(res, aa_child.not_aa_map()->emplace(42, 42));
  EXPECT_NO_ALLOC(res, aa_child.not_aa_string()->assign(kTooLong));

  EXPECT_NO_ALLOC(res, s.not_aa_child_list()->emplace_back());
  auto& not_aa_child = s.not_aa_child_list()[0];

  EXPECT_NO_ALLOC(res, not_aa_child.aa_list()->emplace_back(42));
  EXPECT_NO_ALLOC(res, not_aa_child.aa_set()->emplace(42));
  EXPECT_NO_ALLOC(res, not_aa_child.aa_map()->emplace(42, 42));
  EXPECT_NO_ALLOC(res, not_aa_child.aa_string()->assign(kTooLong));

  EXPECT_NO_ALLOC(res, not_aa_child.not_aa_list()->emplace_back(42));
  EXPECT_NO_ALLOC(res, not_aa_child.not_aa_set()->emplace(42));
  EXPECT_NO_ALLOC(res, not_aa_child.not_aa_map()->emplace(42, 42));
  EXPECT_NO_ALLOC(res, not_aa_child.not_aa_string()->assign(kTooLong));
}

TEST(CppAllocatorTest, PropagateAllAlloc) {
  const PropagateAllAlloc alloc1(1);
  const PropagateAllAlloc alloc2(2);

  {
    const PropagateAllAllocStruct src(alloc1);
    PropagateAllAllocStruct dst(alloc2);
    dst = src;
    EXPECT_EQ(src.get_allocator(), alloc1);
    EXPECT_EQ(dst.get_allocator(), alloc1);
  }

  {
    PropagateAllAllocStruct src(alloc1);
    PropagateAllAllocStruct dst(alloc2);
    dst = std::move(src);
    // NOLINTNEXTLINE(bugprone-use-after-move): checks the moved-from allocator
    EXPECT_EQ(src.get_allocator(), alloc1);
    EXPECT_EQ(dst.get_allocator(), alloc1);
  }

  {
    PropagateAllAllocStruct src(alloc1);
    PropagateAllAllocStruct dst(alloc2);
    swap(dst, src);
    EXPECT_EQ(src.get_allocator(), alloc2);
    EXPECT_EQ(dst.get_allocator(), alloc1);
  }
}

TEST(CppAllocatorTest, PropagateNoneAlloc) {
  const PropagateNoneAlloc alloc1(1);
  const PropagateNoneAlloc alloc2(2);

  {
    const PropagateNoneAllocStruct src(alloc1);
    PropagateNoneAllocStruct dst(alloc2);
    dst = src;
    EXPECT_EQ(src.get_allocator(), alloc1);
    EXPECT_EQ(dst.get_allocator(), alloc2);
  }

  {
    PropagateNoneAllocStruct src(alloc1);
    PropagateNoneAllocStruct dst(alloc2);
    dst = std::move(src);
    // NOLINTNEXTLINE(bugprone-use-after-move): checks the moved-from allocator
    EXPECT_EQ(src.get_allocator(), alloc1);
    EXPECT_EQ(dst.get_allocator(), alloc2);
  }

  {
    PropagateNoneAllocStruct src(alloc1);
    PropagateNoneAllocStruct dst(alloc2);
    swap(dst, src);
    EXPECT_EQ(src.get_allocator(), alloc1);
    EXPECT_EQ(dst.get_allocator(), alloc2);
  }
}

TEST(CppAllocatorTest, PropagateOnlyCopyAlloc) {
  const PropagateOnlyCopyAlloc alloc1(1);
  const PropagateOnlyCopyAlloc alloc2(2);

  {
    const PropagateOnlyCopyAllocStruct src(alloc1);
    PropagateOnlyCopyAllocStruct dst(alloc2);
    dst = src;
    EXPECT_EQ(src.get_allocator(), alloc1);
    EXPECT_EQ(dst.get_allocator(), alloc1);
  }

  {
    PropagateOnlyCopyAllocStruct src(alloc1);
    PropagateOnlyCopyAllocStruct dst(alloc2);
    dst = std::move(src);
    // NOLINTNEXTLINE(bugprone-use-after-move): checks the moved-from allocator
    EXPECT_EQ(src.get_allocator(), alloc1);
    EXPECT_EQ(dst.get_allocator(), alloc2);
  }

  {
    PropagateOnlyCopyAllocStruct src(alloc1);
    PropagateOnlyCopyAllocStruct dst(alloc2);
    swap(dst, src);
    EXPECT_EQ(src.get_allocator(), alloc1);
    EXPECT_EQ(dst.get_allocator(), alloc2);
  }
}

// Copy assignment that adopts the source's allocator builds the fields with it,
// although that allocator does not propagate on swap.
TEST(CppAllocatorTest, CopyAssignBuildsFieldsWithPropagatedAllocator) {
  const ScopedCopyPropagatingAlloc<> alloc1(
      CopyPropagatingStatefulAlloc<char>(1));
  const ScopedCopyPropagatingAlloc<> alloc2(
      CopyPropagatingStatefulAlloc<char>(2));
  CopyPropagatingParent src(alloc1);
  src.s() = kTooLong;
  src.c_ref()->l()->assign({1, 2});
  CopyPropagatingParent dst(alloc2);

  dst = src;

  EXPECT_EQ(dst.get_allocator(), alloc1);
  EXPECT_EQ(dst.s()->get_allocator(), alloc1);
  EXPECT_EQ(*dst.s(), kTooLong);
  ASSERT_NE(dst.c_ref(), nullptr);
  EXPECT_NE(dst.c_ref(), src.c_ref());
  EXPECT_EQ(dst.c_ref()->get_allocator(), alloc1);
  EXPECT_EQ(
      ScopedCopyPropagatingAlloc<>(dst.c_ref()->l()->get_allocator()), alloc1);
  EXPECT_EQ(dst.c_ref()->l()->size(), 2);
}

TEST(CppAllocatorTest, PropagateOnlyMoveAlloc) {
  const PropagateOnlyMoveAlloc alloc1(1);
  const PropagateOnlyMoveAlloc alloc2(2);

  {
    const PropagateOnlyMoveAllocStruct src(alloc1);
    PropagateOnlyMoveAllocStruct dst(alloc2);
    dst = src;
    EXPECT_EQ(src.get_allocator(), alloc1);
    EXPECT_EQ(dst.get_allocator(), alloc2);
  }

  {
    PropagateOnlyMoveAllocStruct src(alloc1);
    PropagateOnlyMoveAllocStruct dst(alloc2);
    dst = std::move(src);
    // NOLINTNEXTLINE(bugprone-use-after-move): checks the moved-from allocator
    EXPECT_EQ(src.get_allocator(), alloc1);
    EXPECT_EQ(dst.get_allocator(), alloc1);
  }

  {
    PropagateOnlyMoveAllocStruct src(alloc1);
    PropagateOnlyMoveAllocStruct dst(alloc2);
    swap(dst, src);
    EXPECT_EQ(src.get_allocator(), alloc1);
    EXPECT_EQ(dst.get_allocator(), alloc2);
  }
}

TEST(CppAllocatorTest, PropagateOnlySwapAlloc) {
  const PropagateOnlySwapAlloc alloc1(1);
  const PropagateOnlySwapAlloc alloc2(2);

  {
    const PropagateOnlySwapAllocStruct src(alloc1);
    PropagateOnlySwapAllocStruct dst(alloc2);
    dst = src;
    EXPECT_EQ(src.get_allocator(), alloc1);
    EXPECT_EQ(dst.get_allocator(), alloc2);
  }

  {
    PropagateOnlySwapAllocStruct src(alloc1);
    PropagateOnlySwapAllocStruct dst(alloc2);
    dst = std::move(src);
    // NOLINTNEXTLINE(bugprone-use-after-move): checks the moved-from allocator
    EXPECT_EQ(src.get_allocator(), alloc1);
    EXPECT_EQ(dst.get_allocator(), alloc2);
  }

  {
    PropagateOnlySwapAllocStruct src(alloc1);
    PropagateOnlySwapAllocStruct dst(alloc2);
    swap(dst, src);
    EXPECT_EQ(src.get_allocator(), alloc2);
    EXPECT_EQ(dst.get_allocator(), alloc1);
  }
}

TEST(CppAllocatorTest, PropagateCopyMoveAlloc) {
  const PropagateCopyMoveAlloc alloc1(1);
  const PropagateCopyMoveAlloc alloc2(2);

  {
    const PropagateCopyMoveAllocStruct src(alloc1);
    PropagateCopyMoveAllocStruct dst(alloc2);
    dst = src;
    EXPECT_EQ(src.get_allocator(), alloc1);
    EXPECT_EQ(dst.get_allocator(), alloc1);
  }

  {
    PropagateCopyMoveAllocStruct src(alloc1);
    PropagateCopyMoveAllocStruct dst(alloc2);
    dst = std::move(src);
    // NOLINTNEXTLINE(bugprone-use-after-move): checks the moved-from allocator
    EXPECT_EQ(src.get_allocator(), alloc1);
    EXPECT_EQ(dst.get_allocator(), alloc1);
  }

  {
    PropagateCopyMoveAllocStruct src(alloc1);
    PropagateCopyMoveAllocStruct dst(alloc2);
    swap(dst, src);
    EXPECT_EQ(src.get_allocator(), alloc1);
    EXPECT_EQ(dst.get_allocator(), alloc2);
  }
}

TEST(CppAllocatorTest, PropagateCopySwapAlloc) {
  const PropagateCopySwapAlloc alloc1(1);
  const PropagateCopySwapAlloc alloc2(2);

  {
    const PropagateCopySwapAllocStruct src(alloc1);
    PropagateCopySwapAllocStruct dst(alloc2);
    dst = src;
    EXPECT_EQ(src.get_allocator(), alloc1);
    EXPECT_EQ(dst.get_allocator(), alloc1);
  }

  {
    PropagateCopySwapAllocStruct src(alloc1);
    PropagateCopySwapAllocStruct dst(alloc2);
    dst = std::move(src);
    // NOLINTNEXTLINE(bugprone-use-after-move): checks the moved-from allocator
    EXPECT_EQ(src.get_allocator(), alloc1);
    EXPECT_EQ(dst.get_allocator(), alloc2);
  }

  {
    PropagateCopySwapAllocStruct src(alloc1);
    PropagateCopySwapAllocStruct dst(alloc2);
    swap(dst, src);
    EXPECT_EQ(src.get_allocator(), alloc2);
    EXPECT_EQ(dst.get_allocator(), alloc1);
  }
}

TEST(CppAllocatorTest, PropagateMoveSwapAlloc) {
  const PropagateMoveSwapAlloc alloc1(1);
  const PropagateMoveSwapAlloc alloc2(2);

  {
    const PropagateMoveSwapAllocStruct src(alloc1);
    PropagateMoveSwapAllocStruct dst(alloc2);
    dst = src;
    EXPECT_EQ(src.get_allocator(), alloc1);
    EXPECT_EQ(dst.get_allocator(), alloc2);
  }

  {
    PropagateMoveSwapAllocStruct src(alloc1);
    PropagateMoveSwapAllocStruct dst(alloc2);
    dst = std::move(src);
    // NOLINTNEXTLINE(bugprone-use-after-move): checks the moved-from allocator
    EXPECT_EQ(src.get_allocator(), alloc1);
    EXPECT_EQ(dst.get_allocator(), alloc1);
  }

  {
    PropagateMoveSwapAllocStruct src(alloc1);
    PropagateMoveSwapAllocStruct dst(alloc2);
    swap(dst, src);
    EXPECT_EQ(src.get_allocator(), alloc2);
    EXPECT_EQ(dst.get_allocator(), alloc1);
  }
}

TEST(CppAllocatorTest, AlwaysThrowAllocatorCppRef) {
  ScopedAlwaysThrowAlloc<> alloc;
  // this is just death instead of a throw since it is throwing an exception on
  // constuctor somehow which results in calling std::terminate
  EXPECT_DEATH({ AlwaysThrowCppRefParent parent(alloc); }, "");
}

TEST(CppAllocatorTest, CopyConstructorPropagatesAllocator) {
  ScopedStatefulAlloc<> alloc(42);
  AAStruct s1(alloc);
  AAStruct s2(s1);
  EXPECT_EQ(alloc, s1.get_allocator());
  EXPECT_EQ(alloc, s2.get_allocator());
}

// A copy may outlive the source's resource, so pmr hands it the default one.
TEST(CppAllocatorTest, CopyConstructorSelectsAllocatorPmr) {
  std::pmr::monotonic_buffer_resource res;
  PmrByteAlloc alloc(&res);

  ChildPmr src(alloc);
  ChildPmr copy(src);

  std::pmr::vector<int> stlSrc(alloc);
  std::pmr::vector<int> stlCopy(stlSrc);

  // Both sources keep the resource they were given.
  EXPECT_EQ(src.get_allocator().resource(), &res);
  EXPECT_EQ(stlSrc.get_allocator().resource(), &res);

  // Neither copy inherits it.
  EXPECT_EQ(copy.get_allocator().resource(), std::pmr::get_default_resource());
  EXPECT_EQ(
      stlCopy.get_allocator().resource(), std::pmr::get_default_resource());
}

TEST(CppAllocatorTest, MoveConstructorPropagatesAllocator) {
  ScopedStatefulAlloc<> alloc(42);
  AAStruct s1(alloc);
  AAStruct s2(std::move(s1));
  // NOLINTNEXTLINE(bugprone-use-after-move): checks the moved-from allocator
  EXPECT_EQ(s1.get_allocator(), alloc);
  EXPECT_EQ(s2.get_allocator(), alloc);
}

// The FragileConstructor has no allocator parameter, so cpp.allocator structs
// don't have one.
static_assert(std::is_constructible_v<
              BoxedPayload,
              apache::thrift::FragileConstructor,
              int32_t>);
static_assert(!std::is_constructible_v<
              HasContainerFields,
              apache::thrift::FragileConstructor,
              StatefulAllocVector<int32_t>,
              StatefulAllocSet<int32_t>,
              StatefulAllocMap<int32_t, int32_t>>);
static_assert(!std::is_constructible_v<
              HasContainerFieldsPmr,
              apache::thrift::FragileConstructor,
              std::pmr::vector<int32_t>,
              std::pmr::set<int32_t>,
              std::pmr::map<int32_t, int32_t>>);
static_assert(!std::is_constructible_v<
              YesAllocatorVia,
              apache::thrift::FragileConstructor,
              AAStruct>);
static_assert(!std::is_constructible_v<
              YesAllocatorViaPmr,
              apache::thrift::FragileConstructor,
              AAStructPmr>);

TEST(CppAllocatorTest, DefaultConstructor1allocator) {
  CountingParent s;
  ScopedCountingAlloc<> alloc = s.get_allocator();
  EXPECT_ALLOC(alloc, s.aa_child_list()->emplace_back());
  auto& child = s.aa_child_list()[0];
  EXPECT_ALLOC(alloc, child.aa_list()->emplace_back(42));
  child.aa_string() = "abcdefg"; // Regardless of short strings optimization
  EXPECT_EQ(alloc, ScopedCountingAlloc<>(s.aa_child_list()->get_allocator()));
  EXPECT_EQ(alloc, ScopedCountingAlloc<>(child.aa_list()->get_allocator()));
  EXPECT_EQ(alloc, ScopedCountingAlloc<>(child.aa_string()->get_allocator()));
}

// The allocator does not propagate on move assignment, so across resources a
// cpp.ref pointee is rebuilt on the target's resource; between equal resources
// the pointer moves as is.
TEST(CppAllocatorTest, PmrMoveAssignRebuildsRefPointeeAcrossResources) {
  std::pmr::monotonic_buffer_resource res1;
  std::pmr::monotonic_buffer_resource res2;
  OptionalRefPmr a{PmrByteAlloc(&res1)};
  OptionalRefPmr b{PmrByteAlloc(&res2)};
  b.c_ref()->aa_string() = kTooLong;

  a = std::move(b);
  EXPECT_EQ(*a.c_ref()->aa_string(), kTooLong);
  EXPECT_EQ(a.c_ref()->get_allocator().resource(), &res1);
  EXPECT_EQ(a.c_ref()->aa_string()->get_allocator().resource(), &res1);

  OptionalRefPmr c{PmrByteAlloc(&res1)};
  const auto* pointee = a.c_ref().get();
  c = std::move(a);
  EXPECT_EQ(c.c_ref().get(), pointee);

  OptionalRefPmr nullRef{PmrByteAlloc(&res2)};
  nullRef.c_ref().reset();
  c = std::move(nullRef);
  EXPECT_EQ(c.c_ref(), nullptr);
}

// Other owners may share the pointee, so it is copied rather than moved from.
TEST(CppAllocatorTest, PmrMoveAssignCopiesSharedRefPointee) {
  std::pmr::monotonic_buffer_resource res1;
  std::pmr::monotonic_buffer_resource res2;
  OptionalRefPmr a{PmrByteAlloc(&res1)};
  OptionalRefPmr b{PmrByteAlloc(&res2)};
  b.c_ref()->not_aa_string() = kTooLong;
  const auto otherOwner = b.c_ref();

  a = std::move(b);

  EXPECT_EQ(*a.c_ref()->not_aa_string(), kTooLong);
  EXPECT_EQ(*otherOwner->not_aa_string(), kTooLong);
}

TEST(CppAllocatorTest, AllocExtendedCtorsWithNullOptionalRef) {
  std::pmr::monotonic_buffer_resource res;
  std::pmr::monotonic_buffer_resource res2;
  OptionalRefPmr src{PmrByteAlloc(&res)};
  src.c_ref().reset();
  src.s_ref().reset();

  OptionalRefPmr moved(std::move(src));
  EXPECT_EQ(moved.c_ref(), nullptr);
  EXPECT_EQ(moved.s_ref(), nullptr);

  const OptionalRefPmr copied(moved, PmrByteAlloc(&res2));
  EXPECT_EQ(copied.c_ref(), nullptr);
  EXPECT_EQ(copied.s_ref(), nullptr);

  const OptionalRefPmr movedWithAlloc(std::move(moved), PmrByteAlloc(&res2));
  EXPECT_EQ(movedWithAlloc.c_ref(), nullptr);
  EXPECT_EQ(movedWithAlloc.s_ref(), nullptr);
}

TEST(CppAllocatorTest, AllocExtendedMoveRebuildsNonNullOptionalRef) {
  std::pmr::monotonic_buffer_resource res;
  std::pmr::monotonic_buffer_resource res2;
  OptionalRefPmr src{PmrByteAlloc(&res)};
  src.c_ref()->aa_string() = kTooLong;
  src.s_ref()->aa_list()->assign({1, 2});

  const OptionalRefPmr moved(std::move(src), PmrByteAlloc(&res2));

  ASSERT_NE(moved.c_ref(), nullptr);
  ASSERT_NE(moved.s_ref(), nullptr);
  EXPECT_EQ(*moved.c_ref()->aa_string(), kTooLong);
  EXPECT_EQ(moved.c_ref()->get_allocator().resource(), &res2);
  EXPECT_EQ(moved.c_ref()->aa_string()->get_allocator().resource(), &res2);
  EXPECT_EQ(moved.s_ref()->aa_list()->size(), 2);
}

// A pointee that does not use the allocator, or one on an equal allocator, is
// taken over, as the plain move ctor does.
TEST(CppAllocatorTest, AllocExtendedMoveTakesOverRefs) {
  CountingIdentityResource counting;
  const PmrByteAlloc alloc(&counting);
  OptionalRefPmr src(alloc);
  src.c_ref()->aa_string() = kTooLong;
  src.s_ref()->aa_list()->assign({1, 2});
  const auto* c = src.c_ref().get();
  const auto* s = src.s_ref().get();
  const int before = counting.allocations();

  const OptionalRefPmr moved(std::move(src), alloc);

  EXPECT_EQ(moved.c_ref().get(), c);
  EXPECT_EQ(moved.s_ref().get(), s);
  EXPECT_EQ(counting.allocations(), before);
}

namespace {
// A const pointee on `alloc`, holding kTooLong.
std::shared_ptr<const ChildPmr> makeConstChild(const PmrByteAlloc& alloc) {
  auto child = std::allocate_shared<ChildPmr>(alloc);
  child->aa_string() = kTooLong;
  return child;
}
} // namespace

// A `shared_ptr<const T>` field is built with the struct's allocator by every
// constructor and assignment that builds a pointee, as a `shared_ptr<T>` one
// is.
TEST(CppAllocatorTest, ConstSharedRefPointeeUsesAllocator) {
  std::pmr::monotonic_buffer_resource res1;
  std::pmr::monotonic_buffer_resource res2;

  const ConstRefPmr built{PmrByteAlloc(&res1)};
  ASSERT_NE(built.c_ref(), nullptr);
  EXPECT_EQ(built.c_ref()->get_allocator().resource(), &res1);

  ConstRefPmr src{PmrByteAlloc(&res1)};
  src.c_ref() = makeConstChild(PmrByteAlloc(&res1));

  const ConstRefPmr copied(src, PmrByteAlloc(&res2));
  EXPECT_EQ(*copied.c_ref()->aa_string(), kTooLong);
  EXPECT_EQ(copied.c_ref()->get_allocator().resource(), &res2);
  EXPECT_EQ(copied.c_ref()->aa_string()->get_allocator().resource(), &res2);

  ConstRefPmr assigned{PmrByteAlloc(&res2)};
  assigned = src;
  EXPECT_EQ(*assigned.c_ref()->aa_string(), kTooLong);
  EXPECT_EQ(assigned.c_ref()->get_allocator().resource(), &res2);

  ConstRefPmr moveAssigned{PmrByteAlloc(&res2)};
  moveAssigned = ConstRefPmr(src, PmrByteAlloc(&res1));
  EXPECT_EQ(*moveAssigned.c_ref()->aa_string(), kTooLong);
  EXPECT_EQ(moveAssigned.c_ref()->get_allocator().resource(), &res2);

  const auto* pointee = src.c_ref().get();
  const ConstRefPmr sameResource(std::move(src), PmrByteAlloc(&res1));
  EXPECT_EQ(sameResource.c_ref().get(), pointee);

  const ConstRefPmr otherResource(
      ConstRefPmr(sameResource, PmrByteAlloc(&res1)), PmrByteAlloc(&res2));
  EXPECT_EQ(*otherResource.c_ref()->aa_string(), kTooLong);
  EXPECT_EQ(otherResource.c_ref()->get_allocator().resource(), &res2);
}

TEST(CppAllocatorTest, ConstSharedRefDeserializesOntoAllocator) {
  ConstRefPmr src;
  src.c_ref() = makeConstChild(PmrByteAlloc());
  const auto bytes = CompactSerializer::serialize<std::string>(src);

  auto prev = set_default_resource(std::pmr::null_memory_resource());
  auto guard = folly::makeGuard([&] { set_default_resource(prev); });
  std::pmr::monotonic_buffer_resource res(std::pmr::new_delete_resource());
  ConstRefPmr dst{PmrByteAlloc(&res)};
  CompactSerializer::deserialize(bytes, dst);

  EXPECT_EQ(*dst.c_ref()->aa_string(), kTooLong);
  EXPECT_EQ(dst.c_ref()->get_allocator().resource(), &res);
  EXPECT_EQ(dst.c_ref()->aa_string()->get_allocator().resource(), &res);
}

// Other owners may share the pointee, so it is copied rather than moved from.
TEST(CppAllocatorTest, AllocExtendedMoveCopiesSharedRefPointee) {
  std::pmr::monotonic_buffer_resource res1;
  std::pmr::monotonic_buffer_resource res2;
  OptionalRefPmr b{PmrByteAlloc(&res2)};
  b.c_ref()->not_aa_string() = kTooLong;
  const auto otherOwner = b.c_ref();

  const OptionalRefPmr a(std::move(b), PmrByteAlloc(&res1));

  EXPECT_EQ(*a.c_ref()->not_aa_string(), kTooLong);
  EXPECT_EQ(a.c_ref()->get_allocator().resource(), &res1);
  EXPECT_EQ(*otherOwner->not_aa_string(), kTooLong);
}

TEST(CppAllocatorTest, AllocExtendedCtorsRunAdapterConstruct) {
  std::pmr::monotonic_buffer_resource res;
  PmrByteAlloc alloc(&res);
  auto& count = ConstructCountingAdapter::constructCount;
  count = 0;

  AdaptedFieldPmr fromAlloc(alloc);
  EXPECT_EQ(count, 1);
  AdaptedFieldPmr copied(fromAlloc, alloc);
  EXPECT_EQ(count, 2);
  AdaptedFieldPmr movedWithAlloc(std::move(copied), alloc);
  EXPECT_EQ(count, 3);
  const AdaptedFieldPmr moved(std::move(movedWithAlloc));
  EXPECT_EQ(count, 4);
}

TEST(CppAllocatorTest, ReadIntoEmptyContainerFieldKeepsBuffer) {
  HasContainerFieldsPmr src;
  src.aa_list()->assign({1, 2, 3});
  std::pmr::monotonic_buffer_resource res;
  HasContainerFieldsPmr s{PmrByteAlloc(&res)};
  s.aa_list()->reserve(16);
  const auto* data = s.aa_list()->data();

  CompactSerializer::deserialize(
      CompactSerializer::serialize<std::string>(src), s);

  EXPECT_EQ(s, src);
  EXPECT_EQ(s.aa_list()->data(), data);
  EXPECT_EQ(s.aa_list()->get_allocator().resource(), &res);
}

TEST(CppAllocatorTest, ReadReplacesNonEmptyContainerField) {
  std::pmr::monotonic_buffer_resource res;
  HasContainerFieldsPmr expected;
  expected.aa_list()->assign({4, 5});
  expected.aa_set()->insert(6);
  expected.aa_map()->emplace(7, 8);
  HasContainerFieldsPmr s{PmrByteAlloc(&res)};
  s.aa_list()->assign({1, 2, 3});
  s.aa_set()->insert(1);
  s.aa_map()->emplace(1, 1);

  CompactSerializer::deserialize(
      CompactSerializer::serialize<std::string>(expected), s);

  EXPECT_EQ(s, expected);
  EXPECT_EQ(s.aa_list()->get_allocator().resource(), &res);
  EXPECT_EQ(s.aa_set()->get_allocator().resource(), &res);
  EXPECT_EQ(s.aa_map()->get_allocator().resource(), &res);
}

TEST(CppAllocatorTest, ResetContainerFieldForRead) {
  std::pmr::vector<int> pmrEmpty;
  pmrEmpty.reserve(16);
  detail::reset_container_field_for_read(pmrEmpty, PmrByteAlloc());
  EXPECT_GE(pmrEmpty.capacity(), 16);

  std::vector<int> stdEmpty;
  stdEmpty.reserve(16);
  detail::reset_container_field_for_read(stdEmpty);
  EXPECT_EQ(stdEmpty.capacity(), 0);

  PropagatingStatefulVector<int> propagating(
      ScopedPropagatingStatefulAlloc<int>(2));
  detail::reset_container_field_for_read(
      propagating, ScopedPropagatingStatefulAlloc<>(1));
  EXPECT_EQ(propagating.get_allocator().state_, 1);
}

} // namespace apache::thrift::test
