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

#include <cstdio>
#include <map>
#include <memory>
#include <memory_resource>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>
#include <folly/container/F14Map.h>
#include <folly/container/sorted_vector_types.h>
#include <folly/container/test/TrackingTypes.h>

#include <thrift/lib/cpp2/protocol/detail/protocol_methods.h>

using namespace apache::thrift;

namespace {

using TrackedKey = folly::test::Tracked<0>;
using TrackedValue = folly::test::Tracked<1>;

using TransparentTrackedKeyHash = folly::test::TransparentTrackedHash<0>;
using TransparentTrackedKeyEqual = folly::test::TransparentTrackedEqual<0>;

const folly::test::Counts kDefaultOnly{/* copyConstruct */ 0,
                                       /* moveConstruct */ 0,
                                       /* copyConvert */ 0,
                                       /* moveConvert */ 0,
                                       /* copyAssign */ 0,
                                       /* moveAssign */ 0,
                                       /* defaultConstruct */ 1,
                                       /* destroyed */ 0};
const folly::test::Counts kDefaultThenMoved{/* copyConstruct */ 0,
                                            /* moveConstruct */ 1,
                                            /* copyConvert */ 0,
                                            /* moveConvert */ 0,
                                            /* copyAssign */ 0,
                                            /* moveAssign */ 0,
                                            /* defaultConstruct */ 1,
                                            /* destroyed */ 1};

class DeserializeKnownLengthMapTest : public testing::Test {
 public:
  void SetUp() override { folly::test::resetTracking(); }
};

enum TrackedValues {
  KEY = 1,
  VALUE = 2,
};

void deserializeTrackedKey(TrackedKey& key) {
  key.val_ = TrackedValues::KEY;
}

void deserializeTrackedValue(TrackedValue& value) {
  value.val_ = TrackedValues::VALUE;
}

template <typename Vec>
void deserializeVecOfTrackedValues(Vec& vec) {
  vec.resize(1);
  vec[0].val_ = TrackedValues::VALUE;
}

template <typename K, typename M, typename H, typename E, typename A>
class MinimalMap : private folly::F14FastMap<K, M, H, E, A> {
  using Super = folly::F14FastMap<K, M, H, E, A>;

 public:
  using typename Super::allocator_type;
  using typename Super::iterator;
  using typename Super::key_type;
  using typename Super::mapped_type;
  using typename Super::size_type;
  using typename Super::value_type;

  MinimalMap() = default;

  using Super::operator[];
  using Super::get_allocator;
  using Super::size;

  // `MinimalMap` has `emplace` method, but not `emplace_hint`.
  template <class... Args>
  std::pair<iterator, bool> emplace(Args&&... args) {
    return Super::emplace(std::forward<Args>(args)...);
  }
};

template <typename K, typename M, typename H, typename E, typename A>
class HintOnlyMap : private folly::F14FastMap<K, M, H, E, A> {
  using Super = folly::F14FastMap<K, M, H, E, A>;

 public:
  using typename Super::allocator_type;
  using typename Super::iterator;
  using typename Super::key_type;
  using typename Super::mapped_type;
  using typename Super::size_type;
  using typename Super::value_type;

  HintOnlyMap() = default;

  using Super::operator[];
  using Super::end;
  using Super::get_allocator;
  using Super::size;

  // `HintOnlyMap` has `emplace_hint` method, but not `try_emplace`.
  template <class... Args>
  iterator emplace_hint(iterator hint, Args&&... args) {
    return Super::emplace_hint(hint, std::forward<Args>(args)...);
  }
};

} // namespace

namespace folly::test {

bool operator<(const TrackedKey& a, const TrackedKey& b) {
  return a.val_ < b.val_;
}

} // namespace folly::test

TEST_F(DeserializeKnownLengthMapTest, SortedUniqueConstuctCount) {
  using Map = folly::sorted_vector_map<TrackedKey, TrackedValue>;

  static_assert(detail::pm::sorted_unique_constructible_v<Map>);
  static_assert(!detail::alloc_should_propagate_map<Map>);

  Map map;
  detail::pm::deserialize_known_length_map(
      map, 1, deserializeTrackedKey, deserializeTrackedValue);

  EXPECT_EQ(map.size(), 1);
  EXPECT_EQ(TrackedKey::counts(), kDefaultOnly);
  EXPECT_EQ(TrackedValue::counts(), kDefaultOnly);
}

TEST_F(DeserializeKnownLengthMapTest, EmplaceHintConstuctCount) {
  using Map = folly::F14FastMap<TrackedKey, TrackedValue>;

  static_assert(!detail::pm::sorted_unique_constructible_v<Map>);
  static_assert(detail::pm::map_emplace_hint_is_invocable_v<Map>);
  static_assert(!detail::alloc_should_propagate_map<Map>);

  Map map;
  detail::pm::deserialize_known_length_map(
      map, 1, deserializeTrackedKey, deserializeTrackedValue);

  EXPECT_EQ(map.size(), 1);
  EXPECT_EQ(TrackedKey::counts(), kDefaultThenMoved);
  EXPECT_EQ(TrackedValue::counts(), kDefaultOnly);
}

TEST_F(DeserializeKnownLengthMapTest, EmplaceHintConstuctCountVec) {
  using Vec = std::vector<TrackedValue>;
  using Map = folly::F14FastMap<TrackedKey, Vec>;

  static_assert(!detail::pm::sorted_unique_constructible_v<Map>);
  static_assert(detail::pm::map_emplace_hint_is_invocable_v<Map>);
  static_assert(!detail::alloc_should_propagate_map<Map>);

  Map map;
  detail::pm::deserialize_known_length_map(
      map, 1, deserializeTrackedKey, deserializeVecOfTrackedValues<Vec>);

  EXPECT_EQ(map.size(), 1);
  EXPECT_EQ(TrackedKey::counts(), kDefaultThenMoved);
  EXPECT_EQ(TrackedValue::counts(), kDefaultOnly);
}

TEST_F(
    DeserializeKnownLengthMapTest, EmplaceHintWithoutTryEmplaceConstuctCount) {
  using Allocator = std::allocator<std::pair<const TrackedKey, TrackedValue>>;
  using Map = HintOnlyMap<
      TrackedKey,
      TrackedValue,
      TransparentTrackedKeyHash,
      TransparentTrackedKeyEqual,
      Allocator>;

  static_assert(!detail::pm::sorted_unique_constructible_v<Map>);
  static_assert(detail::pm::map_emplace_hint_is_invocable_v<Map>);
  static_assert(!detail::pm::map_try_emplace_is_invocable_v<Map>);
  static_assert(!detail::pm::map_supports_in_place_deserialize_v<Map>);
  static_assert(!detail::alloc_should_propagate_map<Map>);

  Map map;
  detail::pm::deserialize_known_length_map(
      map, 1, deserializeTrackedKey, deserializeTrackedValue);

  EXPECT_EQ(map.size(), 1);
  EXPECT_EQ(TrackedKey::counts(), kDefaultThenMoved);
  EXPECT_EQ(TrackedValue::counts(), kDefaultOnly);
}

TEST_F(DeserializeKnownLengthMapTest, EmplaceConstuctCount) {
  using Allocator = std::allocator<std::pair<const TrackedKey, TrackedValue>>;
  using Map = MinimalMap<
      TrackedKey,
      TrackedValue,
      TransparentTrackedKeyHash,
      TransparentTrackedKeyEqual,
      Allocator>;

  static_assert(!detail::pm::sorted_unique_constructible_v<Map>);
  static_assert(!detail::pm::map_emplace_hint_is_invocable_v<Map>);
  static_assert(!detail::alloc_should_propagate_map<Map>);

  Map map;
  detail::pm::deserialize_known_length_map(
      map, 1, deserializeTrackedKey, deserializeTrackedValue);

  EXPECT_EQ(map.size(), 1);
  EXPECT_EQ(TrackedKey::counts(), kDefaultThenMoved);
  EXPECT_EQ(TrackedValue::counts(), kDefaultOnly);
}

TEST_F(DeserializeKnownLengthMapTest, EmplaceConstuctCountVec) {
  using Vec = std::vector<TrackedValue>;
  using Allocator = std::allocator<std::pair<const TrackedKey, Vec>>;
  using Map = MinimalMap<
      TrackedKey,
      Vec,
      TransparentTrackedKeyHash,
      TransparentTrackedKeyEqual,
      Allocator>;

  static_assert(!detail::pm::sorted_unique_constructible_v<Map>);
  static_assert(!detail::pm::map_emplace_hint_is_invocable_v<Map>);
  static_assert(!detail::alloc_should_propagate_map<Map>);

  Map map;
  detail::pm::deserialize_known_length_map(
      map, 1, deserializeTrackedKey, deserializeVecOfTrackedValues<Vec>);

  EXPECT_EQ(map.size(), 1);
  EXPECT_EQ(TrackedKey::counts(), kDefaultThenMoved);
  EXPECT_EQ(TrackedValue::counts(), kDefaultOnly);
}

#if FOLLY_HAS_MEMORY_RESOURCE

TEST_F(DeserializeKnownLengthMapTest, EmplaceHintConstuctCountVecAlloc) {
  using Map =
      folly::pmr::F14FastMap<TrackedKey, std::pmr::vector<TrackedValue>>;

  static_assert(!detail::pm::sorted_unique_constructible_v<Map>);
  static_assert(detail::pm::map_emplace_hint_is_invocable_v<Map>);
  static_assert(detail::alloc_should_propagate_map<Map>);

  Map map;
  detail::pm::deserialize_known_length_map(
      map,
      1,
      deserializeTrackedKey,
      deserializeVecOfTrackedValues<std::pmr::vector<TrackedValue>>);
  EXPECT_EQ(map.size(), 1);
  EXPECT_EQ(TrackedKey::counts(), kDefaultThenMoved);
  EXPECT_EQ(TrackedValue::counts(), kDefaultOnly);
}

TEST_F(DeserializeKnownLengthMapTest, SortedUniqueConstuctCountVecAlloc) {
  using Map =
      folly::pmr::sorted_vector_map<TrackedKey, std::pmr::vector<TrackedValue>>;

  static_assert(detail::pm::sorted_unique_constructible_v<Map>);
  static_assert(detail::alloc_should_propagate_map<Map>);

  std::pmr::monotonic_buffer_resource resource;
  Map map{Map::allocator_type(&resource)};
  detail::pm::deserialize_known_length_map(
      map,
      1,
      deserializeTrackedKey,
      deserializeVecOfTrackedValues<std::pmr::vector<TrackedValue>>);
  ASSERT_EQ(map.size(), 1);
  EXPECT_EQ(map.begin()->second.get_allocator().resource(), &resource);
  EXPECT_EQ(TrackedKey::counts(), kDefaultOnly);
  EXPECT_EQ(TrackedValue::counts(), kDefaultOnly);
}

TEST_F(DeserializeKnownLengthMapTest, EmplaceConstuctCountVecAlloc) {
  using Vec = std::pmr::vector<TrackedValue>;
  using Allocator =
      std::pmr::polymorphic_allocator<std::pair<const TrackedKey, Vec>>;
  using Map = MinimalMap<
      TrackedKey,
      Vec,
      TransparentTrackedKeyHash,
      TransparentTrackedKeyEqual,
      Allocator>;

  static_assert(!detail::pm::sorted_unique_constructible_v<Map>);
  static_assert(!detail::pm::map_emplace_hint_is_invocable_v<Map>);
  static_assert(detail::alloc_should_propagate_map<Map>);

  Map map;
  detail::pm::deserialize_known_length_map(
      map, 1, deserializeTrackedKey, deserializeVecOfTrackedValues<Vec>);

  EXPECT_EQ(map.size(), 1);
  EXPECT_EQ(TrackedKey::counts(), kDefaultThenMoved);
  EXPECT_EQ(TrackedValue::counts(), kDefaultOnly);
}

class CountingResource : public std::pmr::memory_resource {
 public:
  int allocs = 0;

 private:
  void* do_allocate(std::size_t bytes, std::size_t align) override {
    ++allocs;
    return std::pmr::new_delete_resource()->allocate(bytes, align);
  }
  void do_deallocate(void* p, std::size_t bytes, std::size_t align) override {
    std::pmr::new_delete_resource()->deallocate(p, bytes, align);
  }
  bool do_is_equal(
      const std::pmr::memory_resource& other) const noexcept override {
    return this == &other;
  }
};

class DefaultResourceGuard {
 public:
  explicit DefaultResourceGuard(std::pmr::memory_resource* r)
      : prev_(std::pmr::set_default_resource(r)) {}
  ~DefaultResourceGuard() { std::pmr::set_default_resource(prev_); }

 private:
  std::pmr::memory_resource* prev_;
};

// An allocator-aware mapped value that counts how it is built and destroyed.
struct AAValue {
  using allocator_type = std::pmr::polymorphic_allocator<>;

  AAValue() : AAValue(allocator_type{}) {}
  explicit AAValue(const allocator_type& alloc) : vals(alloc) { ++built; }
  AAValue(const AAValue& other) : vals(other.vals) { ++built; }
  AAValue(const AAValue& other, const allocator_type& alloc)
      : vals(other.vals, alloc) {
    ++built;
  }
  AAValue(AAValue&& other) noexcept : vals(std::move(other.vals)) { ++moved; }
  AAValue(AAValue&& other, const allocator_type& alloc)
      : vals(std::move(other.vals), alloc) {
    ++moved;
  }
  AAValue& operator=(const AAValue&) = default;
  AAValue& operator=(AAValue&&) = default;
  ~AAValue() { ++destroyed; }

  allocator_type get_allocator() const { return vals.get_allocator(); }

  std::pmr::vector<int> vals;

  static void reset() { built = moved = destroyed = 0; }
  static inline int built = 0;
  static inline int moved = 0;
  static inline int destroyed = 0;
};

TEST_F(DeserializeKnownLengthMapTest, EmplaceConstuctCountAllocAwareValue) {
  using Map = MinimalMap<
      TrackedKey,
      AAValue,
      TransparentTrackedKeyHash,
      TransparentTrackedKeyEqual,
      std::pmr::polymorphic_allocator<std::pair<const TrackedKey, AAValue>>>;

  static_assert(!detail::pm::map_emplace_hint_is_invocable_v<Map>);
  static_assert(detail::alloc_should_propagate_map<Map>);

  CountingResource arena;
  DefaultResourceGuard arenaGuard(&arena);
  Map map;
  CountingResource heap;
  DefaultResourceGuard heapGuard(&heap);
  AAValue::reset();
  detail::pm::deserialize_known_length_map(
      map, 1, deserializeTrackedKey, [](AAValue& value) {
        value.vals.assign({1, 2, 3});
      });

  EXPECT_EQ(AAValue::built, 1);
  EXPECT_EQ(AAValue::moved, 0);
  EXPECT_EQ(AAValue::destroyed, 0);
  EXPECT_EQ(heap.allocs, 0);
  ASSERT_EQ(map.size(), 1);
  TrackedKey key;
  deserializeTrackedKey(key);
  EXPECT_EQ(map[key].get_allocator().resource(), &arena);
}

// Longer than the SSO buffer, so keys allocate; zero-padded, so they come in
// key order.
std::string aaKey(int i) {
  char buf[8];
  std::snprintf(buf, sizeof(buf), "%04d", i);
  return std::string(24, 'k') + buf;
}

std::vector<int> aaVals(int i) {
  return std::vector<int>(i % 5, i);
}

template <typename Map>
void deserializeAAMap(Map& map, int size) {
  int nextKey = 0;
  int nextVal = 0;
  detail::pm::deserialize_known_length_map(
      map,
      size,
      [&](std::pmr::string& key) { key = aaKey(nextKey++); },
      [&](AAValue& value) {
        const auto vals = aaVals(nextVal++);
        value.vals.assign(vals.begin(), vals.end());
      });
}

template <typename Map>
class InPlaceAllocAwareMapValues : public testing::Test {};

using InPlaceAllocAwareMaps = testing::Types<
    folly::pmr::sorted_vector_map<std::pmr::string, AAValue>,
    std::pmr::map<std::pmr::string, AAValue>,
    std::pmr::unordered_map<std::pmr::string, AAValue>,
    folly::pmr::F14NodeMap<std::pmr::string, AAValue>,
    folly::pmr::F14ValueMap<std::pmr::string, AAValue>,
    folly::pmr::F14VectorMap<std::pmr::string, AAValue>,
    folly::pmr::F14FastMap<std::pmr::string, AAValue>>;
TYPED_TEST_SUITE(InPlaceAllocAwareMapValues, InPlaceAllocAwareMaps);

TYPED_TEST(InPlaceAllocAwareMapValues, NoTemporaryAndAllOnTheMapResource) {
  using Map = TypeParam;
  static_assert(detail::alloc_should_propagate_map<Map>);

  constexpr int kSize = 40;
  CountingResource heap;
  CountingResource arena;
  DefaultResourceGuard guard(&heap);
  Map map{typename Map::allocator_type(&arena)};
  AAValue::reset();
  deserializeAAMap(map, kSize);

  EXPECT_EQ(AAValue::built, kSize);
  EXPECT_EQ(AAValue::moved, 0);
  EXPECT_EQ(AAValue::destroyed, 0);
  EXPECT_EQ(heap.allocs, 0);
  ASSERT_EQ(map.size(), kSize);
  for (int i = 0; i < kSize; ++i) {
    const auto it = map.find(std::pmr::string(aaKey(i)));
    ASSERT_NE(it, map.end());
    EXPECT_EQ(it->first.get_allocator().resource(), &arena);
    EXPECT_EQ(it->second.get_allocator().resource(), &arena);
    EXPECT_EQ(
        std::vector<int>(it->second.vals.begin(), it->second.vals.end()),
        aaVals(i));
  }
}

template <typename Map>
void expectFirstValueKept() {
  CountingResource arena;
  Map map{typename Map::allocator_type(&arena)};
  const std::string key = aaKey(7);
  int nextVal = 1;
  AAValue::reset();
  detail::pm::deserialize_known_length_map(
      map,
      2,
      [&](std::pmr::string& k) { k = key; },
      [&](AAValue& value) { value.vals.assign(nextVal, nextVal), ++nextVal; });
  ASSERT_EQ(map.size(), 1);
  EXPECT_EQ(map.begin()->second.vals, std::pmr::vector<int>({1}));
}

TEST_F(DeserializeKnownLengthMapTest, InPlaceDuplicateKeyKeepsFirstValue) {
  // Not sorted_vector_map: which duplicate it keeps after sorting is
  // unspecified.
  expectFirstValueKept<std::pmr::map<std::pmr::string, AAValue>>();
  expectFirstValueKept<std::pmr::unordered_map<std::pmr::string, AAValue>>();
  expectFirstValueKept<folly::pmr::F14NodeMap<std::pmr::string, AAValue>>();
  expectFirstValueKept<folly::pmr::F14ValueMap<std::pmr::string, AAValue>>();
  expectFirstValueKept<folly::pmr::F14VectorMap<std::pmr::string, AAValue>>();
}

template <typename Map>
void expectValidAfterValueReaderThrows() {
  CountingResource arena;
  Map map{typename Map::allocator_type(&arena)};
  int nextKey = 0;
  int nextVal = 0;
  EXPECT_THROW(
      detail::pm::deserialize_known_length_map(
          map,
          3,
          [&](std::pmr::string& key) { key = aaKey(nextKey++); },
          [&](AAValue& value) {
            if (nextVal == 1) {
              throw std::runtime_error("bad value");
            }
            value.vals.assign(1, nextVal++);
          }),
      std::runtime_error);
  // The entry being decoded stays, partly decoded.
  EXPECT_EQ(map.size(), 2);
  map.clear();
  EXPECT_TRUE(map.empty());
}

TEST_F(DeserializeKnownLengthMapTest, InPlaceValueReaderThrowsLeavesMapValid) {
  expectValidAfterValueReaderThrows<std::pmr::map<std::pmr::string, AAValue>>();
  expectValidAfterValueReaderThrows<
      std::pmr::unordered_map<std::pmr::string, AAValue>>();
  expectValidAfterValueReaderThrows<
      folly::pmr::F14FastMap<std::pmr::string, AAValue>>();
}

struct MoveCountedValue {
  MoveCountedValue() = default;
  MoveCountedValue(MoveCountedValue&&) noexcept { ++moved; }
  MoveCountedValue& operator=(MoveCountedValue&&) noexcept {
    ++moved;
    return *this;
  }

  static inline int moved = 0;
};

TEST_F(DeserializeKnownLengthMapTest, InPlaceOrderedStdAllocator) {
  using Map = std::map<int, MoveCountedValue>;
  static_assert(!detail::alloc_should_propagate_map<Map>);
  MoveCountedValue::moved = 0;
  Map map;
  int nextKey = 0;
  detail::pm::deserialize_known_length_map(
      map, 3, [&](int& key) { key = nextKey++; }, [](MoveCountedValue&) {});
  EXPECT_EQ(map.size(), 3);
  EXPECT_EQ(MoveCountedValue::moved, 0);
}

TEST_F(
    DeserializeKnownLengthMapTest, InPlaceOrderedDuplicateKeyKeepsFirstValue) {
  std::map<int, std::string> map;
  int nextVal = 0;
  detail::pm::deserialize_known_length_map(
      map,
      2,
      [](int& key) { key = 7; },
      [&](std::string& value) { value = std::to_string(nextVal++); });
  EXPECT_EQ(map, (std::map<int, std::string>{{7, "0"}}));
}

static_assert(
    !detail::pm::map_supports_in_place_deserialize_v<std::map<int, int>>);
static_assert(!detail::pm::map_supports_in_place_deserialize_v<
              std::map<std::string, int>>);
static_assert(!detail::pm::map_supports_in_place_deserialize_v<
              std::pmr::map<std::pmr::string, int>>);
static_assert(detail::pm::map_supports_in_place_deserialize_v<
              std::unordered_map<int, int>>);
static_assert(!detail::pm::map_supports_in_place_deserialize_v<
              std::pmr::unordered_map<std::pmr::string, int>>);
static_assert(detail::pm::map_supports_in_place_deserialize_v<
              std::unordered_map<int, std::string>>);

#endif
