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

#include <thrift/lib/cpp2/pmr/Rebind.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <deque>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <memory_resource>
#include <set>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
#include <folly/FBString.h>
#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>
#include <folly/container/FBVector.h>
#include <folly/container/HeterogeneousAccess.h>
#include <folly/container/sorted_vector_types.h>

namespace {

template <class T, class A = std::allocator<T>>
struct UserVector : std::vector<T, A> {};

template <class T>
struct UserAllocator {
  using value_type = T;
  UserAllocator() = default;
  template <class U>
  explicit UserAllocator(const UserAllocator<U>&) noexcept {}
  T* allocate(std::size_t n) { return std::allocator<T>{}.allocate(n); }
  void deallocate(T* p, std::size_t n) noexcept {
    std::allocator<T>{}.deallocate(p, n);
  }
  friend bool operator==(const UserAllocator&, const UserAllocator&) = default;
};

} // namespace

namespace apache::thrift::pmr {
// NOLINTNEXTLINE(facebook-hte-MisplacedTemplateSpecialization)
template <class T, class A>
struct Rebind<UserVector<T, A>> {
  using type = UserVector<
      typename Rebind<T>::type,
      std::pmr::polymorphic_allocator<typename Rebind<T>::type>>;
};
} // namespace apache::thrift::pmr

namespace {

using apache::thrift::pmr::Rebind;

template <class T, class Expected>
constexpr bool kRebindsTo = std::is_same_v<typename Rebind<T>::type, Expected>;

template <class T>
using SideEffectFreeDtor =
    apache::thrift::pmr::SideEffectFreeDtor<typename Rebind<T>::type>;

template <class T>
constexpr bool kTrusted = SideEffectFreeDtor<T>::value &&
    SideEffectFreeDtor<T>::bypass_constexpr_dtor_check_internal_do_not_use;

enum class Color { Red };
struct Struct {
  int i;
  std::string s;
};

static_assert(kRebindsTo<int, int>);
static_assert(kRebindsTo<double, double>);
static_assert(kRebindsTo<Color, Color>);
static_assert(kRebindsTo<Struct, Struct>);

static_assert(kRebindsTo<std::string, std::pmr::string>);
static_assert(kRebindsTo<std::u16string, std::pmr::u16string>);
static_assert(kRebindsTo<std::vector<int>, std::pmr::vector<int>>);
static_assert(kRebindsTo<std::vector<Struct>, std::pmr::vector<Struct>>);
static_assert(
    kRebindsTo<std::vector<std::string>, std::pmr::vector<std::pmr::string>>);
static_assert(
    kRebindsTo<std::deque<std::string>, std::pmr::deque<std::pmr::string>>);
static_assert(
    kRebindsTo<std::list<std::string>, std::pmr::list<std::pmr::string>>);
static_assert(
    kRebindsTo<std::set<std::string>, std::pmr::set<std::pmr::string>>);
static_assert(kRebindsTo<
              std::multiset<std::string>,
              std::pmr::multiset<std::pmr::string>>);
static_assert(kRebindsTo<
              std::map<std::string, std::string>,
              std::pmr::map<std::pmr::string, std::pmr::string>>);
static_assert(kRebindsTo<
              std::multimap<std::string, std::string>,
              std::pmr::multimap<std::pmr::string, std::pmr::string>>);
static_assert(kRebindsTo<
              std::unordered_set<std::string>,
              std::pmr::unordered_set<std::pmr::string>>);
static_assert(kRebindsTo<
              std::unordered_multiset<std::string>,
              std::pmr::unordered_multiset<std::pmr::string>>);
static_assert(kRebindsTo<
              std::unordered_map<std::string, std::string>,
              std::pmr::unordered_map<std::pmr::string, std::pmr::string>>);
static_assert(
    kRebindsTo<
        std::unordered_multimap<std::string, std::string>,
        std::pmr::unordered_multimap<std::pmr::string, std::pmr::string>>);

static_assert(kRebindsTo<
              std::vector<std::map<std::string, std::vector<std::string>>>,
              std::pmr::vector<std::pmr::map<
                  std::pmr::string,
                  std::pmr::vector<std::pmr::string>>>>);
static_assert(kRebindsTo<
              std::unordered_map<int, std::set<std::string>>,
              std::pmr::unordered_map<int, std::pmr::set<std::pmr::string>>>);

// Rebound types map to themselves.
static_assert(kRebindsTo<std::pmr::string, std::pmr::string>);
static_assert(kRebindsTo<
              std::pmr::vector<std::pmr::map<
                  std::pmr::string,
                  std::pmr::vector<std::pmr::string>>>,
              std::pmr::vector<std::pmr::map<
                  std::pmr::string,
                  std::pmr::vector<std::pmr::string>>>>);
static_assert(kRebindsTo<
              std::pmr::unordered_set<std::pmr::string>,
              std::pmr::unordered_set<std::pmr::string>>);

// Custom allocators are replaced.
static_assert(
    kRebindsTo<std::vector<int, UserAllocator<int>>, std::pmr::vector<int>>);
static_assert(
    kRebindsTo<
        std::basic_string<char, std::char_traits<char>, UserAllocator<char>>,
        std::pmr::string>);
static_assert(kRebindsTo<
              std::map<
                  std::string,
                  std::string,
                  std::less<std::string>,
                  UserAllocator<std::pair<const std::string, std::string>>>,
              std::pmr::map<std::pmr::string, std::pmr::string>>);

// std::less, std::greater, std::hash and std::equal_to of any key type the
// container rebinds are replaced; other functors are kept.
static_assert(kRebindsTo<
              std::set<std::vector<std::string>>,
              std::pmr::set<std::pmr::vector<std::pmr::string>>>);
static_assert(kRebindsTo<
              std::set<std::string, std::greater<std::string>>,
              std::pmr::set<std::pmr::string, std::greater<std::pmr::string>>>);
static_assert(kRebindsTo<
              std::set<std::string, std::less<>>,
              std::pmr::set<std::pmr::string, std::less<>>>);
static_assert(kRebindsTo<
              std::set<std::string, std::less<std::string_view>>,
              std::pmr::set<std::pmr::string, std::less<std::string_view>>>);
static_assert(kRebindsTo<
              std::map<std::string, int, std::greater<>>,
              std::pmr::map<std::pmr::string, int, std::greater<>>>);
static_assert(kRebindsTo<
              std::unordered_set<
                  std::string,
                  std::hash<std::string_view>,
                  std::equal_to<>>,
              std::pmr::unordered_set<
                  std::pmr::string,
                  std::hash<std::string_view>,
                  std::equal_to<>>>);
static_assert(
    kRebindsTo<
        std::unordered_map<std::string, int, std::hash<std::string_view>>,
        std::pmr::
            unordered_map<std::pmr::string, int, std::hash<std::string_view>>>);

template <class T>
using PmrFbvector = folly::fbvector<T, std::pmr::polymorphic_allocator<T>>;

struct GrowthPolicy;

static_assert(kRebindsTo<folly::fbstring, std::pmr::string>);
static_assert(kRebindsTo<folly::basic_fbstring<char16_t>, std::pmr::u16string>);
static_assert(kRebindsTo<folly::fbvector<int>, PmrFbvector<int>>);
static_assert(
    kRebindsTo<folly::fbvector<std::string>, PmrFbvector<std::pmr::string>>);
static_assert(kRebindsTo<
              folly::F14NodeSet<std::string>,
              folly::pmr::F14NodeSet<std::pmr::string>>);
static_assert(kRebindsTo<
              folly::F14ValueSet<std::string>,
              folly::pmr::F14ValueSet<std::pmr::string>>);
static_assert(kRebindsTo<
              folly::F14VectorSet<std::string>,
              folly::pmr::F14VectorSet<std::pmr::string>>);
static_assert(kRebindsTo<
              folly::F14FastSet<std::string>,
              folly::pmr::F14FastSet<std::pmr::string>>);
static_assert(kRebindsTo<
              folly::F14NodeMap<std::string, std::string>,
              folly::pmr::F14NodeMap<std::pmr::string, std::pmr::string>>);
static_assert(kRebindsTo<
              folly::F14ValueMap<std::string, std::string>,
              folly::pmr::F14ValueMap<std::pmr::string, std::pmr::string>>);
static_assert(kRebindsTo<
              folly::F14VectorMap<std::string, std::string>,
              folly::pmr::F14VectorMap<std::pmr::string, std::pmr::string>>);
static_assert(kRebindsTo<
              folly::F14FastMap<std::string, std::string>,
              folly::pmr::F14FastMap<std::pmr::string, std::pmr::string>>);
static_assert(kRebindsTo<
              folly::sorted_vector_set<std::string>,
              folly::pmr::sorted_vector_set<std::pmr::string>>);
static_assert(kRebindsTo<
              folly::sorted_vector_map<std::string, std::vector<std::string>>,
              folly::pmr::sorted_vector_map<
                  std::pmr::string,
                  std::pmr::vector<std::pmr::string>>>);

static_assert(kRebindsTo<
              folly::F14FastMap<
                  folly::fbstring,
                  folly::fbvector<folly::F14FastSet<folly::fbstring>>>,
              folly::pmr::F14FastMap<
                  std::pmr::string,
                  PmrFbvector<folly::pmr::F14FastSet<std::pmr::string>>>>);
static_assert(kRebindsTo<
              std::vector<folly::sorted_vector_map<int, folly::fbstring>>,
              std::pmr::vector<
                  folly::pmr::sorted_vector_map<int, std::pmr::string>>>);

static_assert(
    kRebindsTo<PmrFbvector<std::pmr::string>, PmrFbvector<std::pmr::string>>);
static_assert(kRebindsTo<
              folly::pmr::F14NodeSet<std::pmr::string>,
              folly::pmr::F14NodeSet<std::pmr::string>>);
static_assert(kRebindsTo<
              folly::pmr::F14VectorMap<std::pmr::string, std::pmr::string>,
              folly::pmr::F14VectorMap<std::pmr::string, std::pmr::string>>);
static_assert(kRebindsTo<
              folly::pmr::sorted_vector_set<std::pmr::string>,
              folly::pmr::sorted_vector_set<std::pmr::string>>);
static_assert(kRebindsTo<
              folly::pmr::F14FastMap<
                  std::pmr::string,
                  PmrFbvector<folly::pmr::F14FastSet<std::pmr::string>>>,
              folly::pmr::F14FastMap<
                  std::pmr::string,
                  PmrFbvector<folly::pmr::F14FastSet<std::pmr::string>>>>);
static_assert(kRebindsTo<
              folly::pmr::sorted_vector_map<
                  std::pmr::string,
                  std::pmr::vector<std::pmr::string>>,
              folly::pmr::sorted_vector_map<
                  std::pmr::string,
                  std::pmr::vector<std::pmr::string>>>);

// The F14 default hasher and key-equality functor of any key type the
// container rebinds are replaced, also in std containers; other functors and
// sorted vector growth policies are kept.
static_assert(kRebindsTo<
              folly::F14FastSet<std::vector<std::string>>,
              folly::pmr::F14FastSet<std::pmr::vector<std::pmr::string>>>);
static_assert(kRebindsTo<
              std::unordered_set<
                  std::string,
                  folly::HeterogeneousAccessHash<std::string>,
                  folly::HeterogeneousAccessEqualTo<std::string>>,
              std::pmr::unordered_set<
                  std::pmr::string,
                  folly::HeterogeneousAccessHash<std::pmr::string>,
                  folly::HeterogeneousAccessEqualTo<std::pmr::string>>>);
static_assert(kRebindsTo<
              folly::F14FastMap<
                  std::string,
                  int,
                  std::hash<std::string>,
                  std::equal_to<std::string>>,
              folly::pmr::F14FastMap<
                  std::pmr::string,
                  int,
                  std::hash<std::pmr::string>,
                  std::equal_to<std::pmr::string>>>);
static_assert(kRebindsTo<
              folly::F14FastSet<
                  std::string,
                  std::hash<std::string_view>,
                  std::equal_to<>>,
              folly::pmr::F14FastSet<
                  std::pmr::string,
                  std::hash<std::string_view>,
                  std::equal_to<>>>);
static_assert(kRebindsTo<
              folly::sorted_vector_set<std::string, std::less<>>,
              folly::pmr::sorted_vector_set<std::pmr::string, std::less<>>>);
static_assert(kRebindsTo<
              folly::sorted_vector_map<
                  int,
                  std::string,
                  std::less<int>,
                  std::allocator<std::pair<int, std::string>>,
                  GrowthPolicy>,
              folly::pmr::sorted_vector_map<
                  int,
                  std::pmr::string,
                  std::less<int>,
                  GrowthPolicy>>);

// std::less, std::hash and std::equal_to of folly::fbstring become those of
// std::pmr::string.
static_assert(kRebindsTo<
              std::map<folly::fbstring, int>,
              std::pmr::map<std::pmr::string, int>>);
static_assert(kRebindsTo<
              std::unordered_map<folly::fbstring, int>,
              std::pmr::unordered_map<std::pmr::string, int>>);

// Thrift structs are often incomplete where their containers are rebound:
// recursive structs, and structs defined later in the file.
struct Incomplete;
static_assert(
    kRebindsTo<std::vector<Incomplete>, std::pmr::vector<Incomplete>>);
static_assert(kRebindsTo<std::set<Incomplete>, std::pmr::set<Incomplete>>);
static_assert(kRebindsTo<
              std::map<std::string, Incomplete>,
              std::pmr::map<std::pmr::string, Incomplete>>);
static_assert(kRebindsTo<folly::fbvector<Incomplete>, PmrFbvector<Incomplete>>);
static_assert(kRebindsTo<
              folly::F14NodeMap<std::string, Incomplete>,
              folly::pmr::F14NodeMap<std::pmr::string, Incomplete>>);
static_assert(kRebindsTo<
              folly::sorted_vector_map<int, Incomplete>,
              folly::pmr::sorted_vector_map<int, Incomplete>>);

struct Node {
  Rebind<std::vector<Node>>::type children;
};
static_assert(std::is_same_v<decltype(Node::children), std::pmr::vector<Node>>);

struct Later;
struct Earlier {
  Rebind<std::vector<Later>>::type later;
};
struct Later {
  Rebind<std::vector<Earlier>>::type earlier;
};
static_assert(
    std::is_same_v<decltype(Earlier::later), std::pmr::vector<Later>>);
static_assert(
    std::is_same_v<decltype(Later::earlier), std::pmr::vector<Earlier>>);

// A user specialization takes precedence over the error for unknown class
// templates, and composes with Thrift's.
static_assert(kRebindsTo<
              std::vector<UserVector<std::string>>,
              std::pmr::vector<UserVector<
                  std::pmr::string,
                  std::pmr::polymorphic_allocator<std::pmr::string>>>>);

static_assert(kTrusted<std::string>);
static_assert(kTrusted<std::vector<int>>);
static_assert(kTrusted<std::deque<int>>);
static_assert(kTrusted<std::list<int>>);
static_assert(kTrusted<std::set<int>>);
static_assert(kTrusted<std::multiset<int>>);
static_assert(kTrusted<std::map<int, int>>);
static_assert(kTrusted<std::multimap<int, int>>);
static_assert(kTrusted<std::unordered_set<int>>);
static_assert(kTrusted<std::unordered_multiset<int>>);
static_assert(kTrusted<std::unordered_map<int, int>>);
static_assert(kTrusted<std::unordered_multimap<int, int>>);
static_assert(kTrusted<folly::fbstring>);
static_assert(kTrusted<folly::fbvector<int>>);
static_assert(kTrusted<folly::F14NodeSet<int>>);
static_assert(kTrusted<folly::F14ValueSet<int>>);
static_assert(kTrusted<folly::F14VectorSet<int>>);
static_assert(kTrusted<folly::F14FastSet<int>>);
static_assert(kTrusted<folly::F14NodeMap<int, int>>);
static_assert(kTrusted<folly::F14ValueMap<int, int>>);
static_assert(kTrusted<folly::F14VectorMap<int, int>>);
static_assert(kTrusted<folly::F14FastMap<int, int>>);
static_assert(kTrusted<folly::sorted_vector_set<int>>);
static_assert(kTrusted<folly::sorted_vector_map<int, int>>);
static_assert(!SideEffectFreeDtor<int>::value);
static_assert(!SideEffectFreeDtor<Struct>::value);
static_assert(!SideEffectFreeDtor<UserVector<int>>::value);

class DefaultResourceGuard {
 public:
  explicit DefaultResourceGuard(std::pmr::memory_resource* r)
      : prev_(std::pmr::set_default_resource(r)) {}
  DefaultResourceGuard(const DefaultResourceGuard&) = delete;
  DefaultResourceGuard& operator=(const DefaultResourceGuard&) = delete;
  ~DefaultResourceGuard() { std::pmr::set_default_resource(prev_); }

 private:
  std::pmr::memory_resource* prev_;
};

// A monotonic_buffer_resource over a fixed buffer. While it lives, the
// default resource is null_memory_resource(), so any pmr allocation that
// does not come from the arena throws.
class TestArena {
 public:
  std::pmr::memory_resource* resource() { return &resource_; }

  bool owns(const void* p) const {
    std::less<const void*> less;
    return !less(p, buffer_.data()) && less(p, buffer_.data() + buffer_.size());
  }

 private:
  std::array<std::byte, 1 << 16> buffer_{};
  std::pmr::monotonic_buffer_resource resource_{
      buffer_.data(), buffer_.size(), std::pmr::null_memory_resource()};
  DefaultResourceGuard guard_{std::pmr::null_memory_resource()};
};

constexpr std::string_view kText =
    "a string too long for the small string buffer";

TEST(RebindTest, NestedValuesUseTheContainerResource) {
  TestArena arena;

  Rebind<std::vector<std::map<std::string, std::vector<std::string>>>>::type
      values(arena.resource());
  auto& map = values.emplace_back();
  auto& [key, strings] = *map.emplace(
                                 std::piecewise_construct,
                                 std::forward_as_tuple(kText),
                                 std::forward_as_tuple())
                              .first;
  strings.emplace_back(kText);

  EXPECT_EQ(map.get_allocator().resource(), arena.resource());
  EXPECT_EQ(key.get_allocator().resource(), arena.resource());
  EXPECT_EQ(strings.get_allocator().resource(), arena.resource());
  EXPECT_EQ(strings.front().get_allocator().resource(), arena.resource());
  EXPECT_EQ(key, kText);
  EXPECT_EQ(strings.front(), kText);
}

constexpr std::size_t kEntries = 32;

// Inserts enough entries for the map to grow several times, each with a
// distinct key and a vector holding one string, all too long for the small
// string buffer.
template <class Map>
void fillWithLongStrings(Map& map) {
  for (std::size_t i = 0; i < kEntries; ++i) {
    std::string key = std::string(kText) + std::to_string(i);
    // NOLINTNEXTLINE(facebook-expensive-flat-container-operation)
    auto it = map.emplace(
                     std::piecewise_construct,
                     std::forward_as_tuple(key),
                     std::forward_as_tuple())
                  .first;
    it->second.emplace_back(kText);
  }
}

// Whether the map's entries, their keys, the vectors' buffers and the strings
// in them are all in the arena's buffer.
template <class Map>
bool allocatedFrom(const TestArena& arena, const Map& map) {
  return std::all_of(map.begin(), map.end(), [&](const auto& entry) {
    const auto& [key, strings] = entry;
    return arena.owns(&entry) && arena.owns(key.data()) &&
        arena.owns(strings.data()) &&
        std::all_of(strings.begin(), strings.end(), [&](const auto& s) {
             return arena.owns(s.data());
           });
  });
}

TEST(RebindTest, F14MapValuesUseTheMapResource) {
  TestArena arena;

  Rebind<folly::F14FastMap<std::string, std::vector<std::string>>>::type map(
      arena.resource());
  fillWithLongStrings(map);

  EXPECT_EQ(map.size(), kEntries);
  EXPECT_TRUE(allocatedFrom(arena, map));
}

TEST(RebindTest, SortedVectorMapValuesUseTheMapResource) {
  TestArena arena;

  Rebind<folly::sorted_vector_map<std::string, std::vector<std::string>>>::type
      map(arena.resource());
  fillWithLongStrings(map);

  EXPECT_EQ(map.size(), kEntries);
  EXPECT_TRUE(allocatedFrom(arena, map));
}

TEST(RebindTest, FbvectorValuesUseTheVectorResource) {
  TestArena arena;

  Rebind<folly::fbvector<std::vector<std::string>>>::type values(
      arena.resource());
  for (std::size_t i = 0; i < kEntries; ++i) {
    values.emplace_back().emplace_back(kText);
  }

  EXPECT_EQ(values.size(), kEntries);
  EXPECT_TRUE(arena.owns(values.data()));
  EXPECT_TRUE(
      std::all_of(values.begin(), values.end(), [&](const auto& strings) {
        return arena.owns(strings.data()) && arena.owns(strings.front().data());
      }));
}

} // namespace
