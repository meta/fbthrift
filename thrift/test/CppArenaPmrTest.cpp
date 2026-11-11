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

#include <compare>
#include <cstdint>
#include <deque>
#include <limits>
#include <map>
#include <memory_resource>
#include <set>
#include <sstream>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
#include <folly/FBString.h>

#include <thrift/lib/cpp2/op/Compare.h>
#include <thrift/lib/cpp2/op/Get.h>
#include <thrift/lib/cpp2/op/Hash.h>
#include <thrift/lib/cpp2/pmr/Rebind.h>
#include <thrift/lib/cpp2/protocol/DebugProtocol.h>
#include <thrift/lib/cpp2/protocol/Json5Protocol.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>
#include <thrift/test/gen-cpp2/CppArenaPmrTest_types.h>
#include <thrift/test/gen-cpp2/CppArenaPmrTest_types_custom_protocol.h>

namespace apache::thrift::test::arena_pmr {
namespace {

namespace t = ::apache::thrift::type;

// The C++ type `@cpp.Arena` gives a level whose C++ type is `N` without it.
template <class N>
using R = typename ::apache::thrift::pmr::Rebind<N>::type;

// The tag of a string, binary or container level whose C++ type is `N`
// without `@cpp.Arena` and whose Thrift tag is `Tag`, built from its rebound
// children.
template <class N, class Tag>
using A = t::cpp_type<R<N>, Tag>;

using S = std::string;
using I = std::int32_t;

// Whether the field has the C++ type and type tag that `@cpp.Arena` gives a
// field whose C++ type is `Native` and whose Thrift tag is `Tag` without it.
template <class Struct, class Id, class Native, class Tag>
constexpr bool kIsArenaField =
    std::is_same_v<op::get_native_type<Struct, Id>, R<Native>> &&
    std::is_same_v<op::get_type_tag<Struct, Id>, A<Native, Tag>>;

static_assert(std::is_same_v<
              Strings::allocator_type,
              std::pmr::polymorphic_allocator<std::byte>>);
static_assert(kIsArenaField<Strings, ident::name, S, t::string_t>);
static_assert(kIsArenaField<Strings, ident::data, S, t::binary_t>);
static_assert(kIsArenaField<Strings, ident::nickname, S, t::string_t>);
static_assert(kIsArenaField<Strings, ident::terse_name, S, t::string_t>);
static_assert(
    std::is_same_v<op::get_native_type<Strings, ident::id>, std::int64_t>);
static_assert(std::is_same_v<op::get_native_type<Strings, ident::plain>, S>);
static_assert(
    std::is_same_v<op::get_type_tag<Strings, ident::plain>, t::string_t>);
static_assert(std::is_same_v<R<S>, std::pmr::string>);

static_assert(kIsArenaField<
              Containers,
              ident::names,
              std::vector<S>,
              t::list<A<S, t::string_t>>>);
static_assert(kIsArenaField<
              Containers,
              ident::tags,
              std::set<S>,
              t::set<A<S, t::string_t>>>);
static_assert(kIsArenaField<
              Containers,
              ident::groups,
              std::map<S, std::set<I>>,
              t::map<A<S, t::string_t>, A<std::set<I>, t::set<t::i32_t>>>>);
static_assert(kIsArenaField<
              Containers,
              ident::matrix,
              std::vector<std::vector<S>>,
              t::list<A<std::vector<S>, t::list<A<S, t::string_t>>>>>);
static_assert(kIsArenaField<
              Containers,
              ident::blobs,
              std::map<S, std::vector<S>>,
              t::map<
                  A<S, t::string_t>,
                  A<std::vector<S>, t::list<A<S, t::binary_t>>>>>);
static_assert(
    kIsArenaField<
        Containers,
        ident::nested,
        std::vector<std::map<I, std::set<S>>>,
        t::list<
            A<std::map<I, std::set<S>>,
              t::map<t::i32_t, A<std::set<S>, t::set<A<S, t::binary_t>>>>>>>);
static_assert(kIsArenaField<
              Containers,
              ident::values,
              std::vector<double>,
              t::list<t::double_t>>);
static_assert(kIsArenaField<
              Containers,
              ident::by_color,
              std::map<Color, S>,
              t::map<t::enum_t<Color>, A<S, t::string_t>>>);
static_assert(kIsArenaField<
              Containers,
              ident::maybe,
              std::vector<S>,
              t::list<A<S, t::string_t>>>);
static_assert(kIsArenaField<
              Containers,
              ident::terse_tags,
              std::set<S>,
              t::set<A<S, t::string_t>>>);
static_assert(
    std::is_same_v<
        op::get_native_type<Containers, ident::nested>,
        std::pmr::vector<std::pmr::map<I, std::pmr::set<std::pmr::string>>>>);

static_assert(kIsArenaField<
              StructElements,
              ident::plains,
              std::vector<PlainInner>,
              t::list<t::struct_t<PlainInner>>>);
static_assert(kIsArenaField<
              StructElements,
              ident::plain_by_name,
              std::map<S, PlainInner>,
              t::map<A<S, t::string_t>, t::struct_t<PlainInner>>>);
static_assert(kIsArenaField<
              StructElements,
              ident::arenas,
              std::vector<ArenaInner>,
              t::list<t::struct_t<ArenaInner>>>);
static_assert(kIsArenaField<
              StructElements,
              ident::arena_by_name,
              std::map<S, ArenaInner>,
              t::map<A<S, t::string_t>, t::struct_t<ArenaInner>>>);
static_assert(kIsArenaField<WholeStruct, ident::name, S, t::string_t>);
static_assert(kIsArenaField<
              WholeStruct,
              ident::groups,
              std::map<S, std::vector<S>>,
              t::map<
                  A<S, t::string_t>,
                  A<std::vector<S>, t::list<A<S, t::string_t>>>>>);
static_assert(
    std::is_same_v<op::get_native_type<WholeStruct, ident::id>, std::int64_t>);
static_assert(kIsArenaField<ArenaUnion, ident::name, S, t::string_t>);
static_assert(kIsArenaField<
              ArenaUnion,
              ident::names,
              std::vector<S>,
              t::list<A<S, t::string_t>>>);
static_assert(kIsArenaField<WholeUnion, ident::name, S, t::string_t>);
static_assert(kIsArenaField<
              WholeUnion,
              ident::groups,
              std::map<S, std::vector<S>>,
              t::map<
                  A<S, t::string_t>,
                  A<std::vector<S>, t::list<A<S, t::string_t>>>>>);
static_assert(
    std::is_same_v<op::get_native_type<WholeUnion, ident::id>, std::int64_t>);

static_assert(kIsArenaField<
              Typedefs,
              ident::names,
              Names,
              t::list<A<Name, t::string_t>>>);
static_assert(
    kIsArenaField<
        Typedefs,
        ident::by_name,
        std::map<Name, Names>,
        t::map<A<Name, t::string_t>, A<Names, t::list<A<Name, t::string_t>>>>>);

static_assert(kIsArenaField<
              CustomTypes,
              ident::by_name,
              std::unordered_map<S, std::deque<S>>,
              t::map<
                  A<S, t::string_t>,
                  A<std::deque<S>, t::list<A<S, t::string_t>>>>>);
static_assert(
    kIsArenaField<CustomTypes, ident::fbstring, folly::fbstring, t::string_t>);
static_assert(kIsArenaField<
              CustomTypes,
              ident::u32s,
              std::vector<std::uint32_t>,
              t::list<t::cpp_type<std::uint32_t, t::i32_t>>>);
static_assert(std::is_same_v<
              op::get_native_type<CustomTypes, ident::fbstring>,
              std::pmr::string>);
static_assert(std::is_same_v<
              op::get_native_type<CustomTypes, ident::iobufs>,
              std::pmr::map<std::pmr::string, folly::IOBuf>>);

// PlainInner has no `@cpp.Arena`, so its members keep their types.
static_assert(std::is_same_v<op::get_native_type<PlainInner, ident::name>, S>);
static_assert(std::uses_allocator_v<
              ArenaInner,
              std::pmr::polymorphic_allocator<ArenaInner>>);
static_assert(!std::uses_allocator_v<
              PlainInner,
              std::pmr::polymorphic_allocator<PlainInner>>);
static_assert(std::uses_allocator_v<
              ArenaUnion,
              std::pmr::polymorphic_allocator<ArenaUnion>>);

constexpr const char* kLong =
    "This is too long for the small string optimization";
constexpr const char* kOther =
    "This is also too long for the small string optimization";

class ScopedDefaultResource {
 public:
  explicit ScopedDefaultResource(std::pmr::memory_resource* resource)
      : prev_(std::pmr::set_default_resource(resource)) {}
  ScopedDefaultResource(const ScopedDefaultResource&) = delete;
  ScopedDefaultResource& operator=(const ScopedDefaultResource&) = delete;
  ~ScopedDefaultResource() { std::pmr::set_default_resource(prev_); }

 private:
  std::pmr::memory_resource* prev_;
};

class NoDefaultResource : public ScopedDefaultResource {
 public:
  NoDefaultResource()
      : ScopedDefaultResource(std::pmr::null_memory_resource()) {}
};

class Arena : public std::pmr::monotonic_buffer_resource {
 public:
  Arena() : monotonic_buffer_resource(std::pmr::new_delete_resource()) {}
};

template <class T>
std::pmr::memory_resource* resource(const T& value) {
  return value.get_allocator().resource();
}

void fill(Strings& s) {
  s.name() = kLong;
  s.data() = kOther;
  s.nickname() = kLong;
  s.terse_name() = kOther;
  s.id() = 7;
  s.plain() = kLong;
}

void expectOn(const Strings& s, std::pmr::memory_resource* r) {
  EXPECT_EQ(resource(s), r);
  EXPECT_EQ(resource(*s.name()), r);
  EXPECT_EQ(resource(*s.data()), r);
  EXPECT_EQ(resource(*s.nickname()), r);
  EXPECT_EQ(resource(*s.terse_name()), r);
}

void fill(Containers& c) {
  c.names()->emplace_back(kLong);
  const std::pmr::string key(kLong, resource(*c.names()));
  c.tags()->emplace(kLong);
  c.tags()->emplace(kOther);
  (*c.groups())[key].insert(3);
  c.matrix()->emplace_back().emplace_back(kOther);
  (*c.blobs())[key].emplace_back(kOther);
  c.nested()->emplace_back()[7].emplace(kLong);
  c.values()->push_back(2.5);
  (*c.by_color())[Color::GREEN] = kOther;
  c.maybe().ensure().emplace_back(kLong);
  c.terse_tags()->emplace(kOther);
}

void expectOn(const Containers& c, std::pmr::memory_resource* r) {
  EXPECT_EQ(resource(c), r);
  EXPECT_EQ(resource(*c.names()), r);
  EXPECT_EQ(resource(c.names()->front()), r);
  EXPECT_EQ(resource(*c.tags()), r);
  EXPECT_EQ(resource(*c.tags()->begin()), r);
  EXPECT_EQ(resource(*c.groups()), r);
  EXPECT_EQ(resource(c.groups()->begin()->first), r);
  EXPECT_EQ(resource(c.groups()->begin()->second), r);
  EXPECT_EQ(resource(*c.matrix()), r);
  EXPECT_EQ(resource(c.matrix()->front()), r);
  EXPECT_EQ(resource(c.matrix()->front().front()), r);
  EXPECT_EQ(resource(*c.blobs()), r);
  EXPECT_EQ(resource(c.blobs()->begin()->second.front()), r);
  EXPECT_EQ(resource(*c.nested()), r);
  EXPECT_EQ(resource(c.nested()->front()), r);
  EXPECT_EQ(resource(c.nested()->front().at(7)), r);
  EXPECT_EQ(resource(*c.nested()->front().at(7).begin()), r);
  EXPECT_EQ(resource(*c.values()), r);
  EXPECT_EQ(resource(*c.by_color()), r);
  EXPECT_EQ(resource(c.by_color()->at(Color::GREEN)), r);
  EXPECT_EQ(resource(*c.maybe()), r);
  EXPECT_EQ(resource(c.maybe()->front()), r);
  EXPECT_EQ(resource(*c.terse_tags()), r);
  EXPECT_EQ(resource(*c.terse_tags()->begin()), r);
}

void fill(StructElements& s) {
  const std::pmr::string key(kLong, resource(*s.plains()));
  PlainInner& plain = s.plains()->emplace_back();
  plain.name() = kLong;
  plain.values()->push_back(1);
  (*s.plain_by_name())[key].name() = kOther;
  ArenaInner& arena = s.arenas()->emplace_back();
  arena.name() = kLong;
  arena.tags()->emplace_back(kOther);
  ArenaInner& named = (*s.arena_by_name())[key];
  named.name() = kOther;
  named.tags()->emplace_back(kLong);
}

void expectOn(const StructElements& s, std::pmr::memory_resource* r) {
  EXPECT_EQ(resource(*s.plains()), r);
  EXPECT_EQ(resource(*s.plain_by_name()), r);
  EXPECT_EQ(resource(s.plain_by_name()->begin()->first), r);
  EXPECT_EQ(resource(*s.arenas()), r);
  EXPECT_EQ(resource(s.arenas()->front()), r);
  EXPECT_EQ(resource(*s.arenas()->front().name()), r);
  EXPECT_EQ(resource(*s.arenas()->front().tags()), r);
  EXPECT_EQ(resource(s.arenas()->front().tags()->front()), r);
  EXPECT_EQ(resource(*s.arena_by_name()), r);
  const ArenaInner& named = s.arena_by_name()->begin()->second;
  EXPECT_EQ(resource(named), r);
  EXPECT_EQ(resource(*named.name()), r);
  EXPECT_EQ(resource(named.tags()->front()), r);
}

void fill(CustomTypes& c) {
  const std::pmr::string key(kLong, resource(*c.by_name()));
  (*c.by_name())[key].emplace_back(kOther);
  c.fbstring() = kLong;
}

void expectOn(const CustomTypes& c, std::pmr::memory_resource* r) {
  EXPECT_EQ(resource(c), r);
  EXPECT_EQ(resource(*c.by_name()), r);
  EXPECT_EQ(resource(c.by_name()->begin()->first), r);
  EXPECT_EQ(resource(c.by_name()->begin()->second), r);
  EXPECT_EQ(resource(c.by_name()->begin()->second.front()), r);
  EXPECT_EQ(resource(*c.fbstring()), r);
}

void fill(WholeStruct& s) {
  s.name() = kLong;
  const std::pmr::string key(kLong, resource(*s.groups()));
  (*s.groups())[key].emplace_back(kOther);
}

void expectOn(const WholeStruct& s, std::pmr::memory_resource* r) {
  EXPECT_EQ(resource(s), r);
  EXPECT_EQ(resource(*s.name()), r);
  EXPECT_EQ(resource(*s.groups()), r);
  EXPECT_EQ(resource(s.groups()->begin()->first), r);
  EXPECT_EQ(resource(s.groups()->begin()->second.front()), r);
}

void fill(ArenaUnion& u) {
  u.names().emplace().emplace_back(kLong);
}

void expectOn(const ArenaUnion& u, std::pmr::memory_resource* r) {
  EXPECT_EQ(resource(u), r);
  EXPECT_EQ(resource(*u.names()), r);
  EXPECT_EQ(resource(u.names()->front()), r);
}

// Moving a PMR string or container to another resource copies it, which can
// throw.
static_assert(!std::is_nothrow_move_assignable_v<Strings>);
static_assert(std::is_nothrow_move_constructible_v<Strings>);
static_assert(!std::is_nothrow_move_assignable_v<HoldsStrings>);
static_assert(!std::is_nothrow_move_assignable_v<ArenaUnion>);
static_assert(!std::is_nothrow_move_assignable_v<HoldsArenaUnion>);

Strings makeStrings(std::pmr::memory_resource* r, const char* name) {
  ScopedDefaultResource defaultResource(r);
  Strings s;
  fill(s);
  s.name() = name;
  return s;
}

TEST(CppArenaPmrTest, SwapAndMoveAssignmentKeepEachResource) {
  Arena arena1;
  Arena arena2;
  Strings a = makeStrings(&arena1, kLong);
  Strings b = makeStrings(&arena2, kOther);
  swap(a, b);
  EXPECT_EQ(*a.name(), kOther);
  EXPECT_EQ(*b.name(), kLong);
  expectOn(a, &arena1);
  expectOn(b, &arena2);
  a = std::move(b);
  EXPECT_EQ(*a.name(), kLong);
  expectOn(a, &arena1);
}

void fill(WholeUnion& u) {
  auto& groups = u.groups().emplace();
  groups[std::pmr::string(kLong, resource(groups))].emplace_back(kOther);
}

void expectOn(const WholeUnion& u, std::pmr::memory_resource* r) {
  EXPECT_EQ(resource(u), r);
  EXPECT_EQ(resource(*u.groups()), r);
  EXPECT_EQ(resource(u.groups()->begin()->first), r);
  EXPECT_EQ(resource(u.groups()->begin()->second.front()), r);
}

void fill(HoldsUnions& h) {
  fill(h.unions()->emplace_back());
}

void expectOn(const HoldsUnions& h, std::pmr::memory_resource* r) {
  EXPECT_EQ(resource(h), r);
  EXPECT_EQ(resource(*h.unions()), r);
  expectOn(h.unions()->front(), r);
}

void fill(ArenaError& e) {
  e.message() = kLong;
  e.details()->emplace_back(kOther);
}

void expectOn(const ArenaError& e, std::pmr::memory_resource* r) {
  EXPECT_EQ(resource(e), r);
  EXPECT_EQ(resource(*e.message()), r);
  EXPECT_EQ(resource(*e.details()), r);
  EXPECT_EQ(resource(e.details()->front()), r);
}

TEST(CppArenaPmrTest, AllocatorConstructorPutsEverythingOnTheResource) {
  Arena arena;
  NoDefaultResource noDefault;
  Strings s{&arena};
  fill(s);
  expectOn(s, &arena);
  Containers c{&arena};
  fill(c);
  expectOn(c, &arena);
  StructElements e{&arena};
  fill(e);
  expectOn(e, &arena);
  WholeStruct w{&arena};
  fill(w);
  expectOn(w, &arena);
  ArenaUnion fieldUnion{&arena};
  fill(fieldUnion);
  expectOn(fieldUnion, &arena);
  WholeUnion wholeUnion{&arena};
  fill(wholeUnion);
  expectOn(wholeUnion, &arena);
  HoldsUnions h{&arena};
  fill(h);
  expectOn(h, &arena);
  ArenaError error{&arena};
  fill(error);
  expectOn(error, &arena);
  CustomTypes u{&arena};
  fill(u);
  expectOn(u, &arena);
}

TEST(CppArenaPmrTest, DefaultConstructionUsesTheDefaultResource) {
  Arena arena;
  ScopedDefaultResource defaultResource(&arena);
  Strings s;
  fill(s);
  expectOn(s, &arena);
  Containers c;
  fill(c);
  expectOn(c, &arena);
  StructElements e;
  fill(e);
  expectOn(e, &arena);
  WholeStruct w;
  fill(w);
  expectOn(w, &arena);
  ArenaUnion fieldUnion;
  fill(fieldUnion);
  expectOn(fieldUnion, &arena);
  WholeUnion wholeUnion;
  fill(wholeUnion);
  expectOn(wholeUnion, &arena);
  CustomTypes u;
  fill(u);
  expectOn(u, &arena);
}

template <class Serializer, class T>
void expectRoundTrip() {
  T in;
  fill(in);
  EXPECT_EQ(
      Serializer::template deserialize<T>(
          Serializer::template serialize<std::string>(in)),
      in);
}

template <class T>
void expectRoundTrip() {
  expectRoundTrip<BinarySerializer, T>();
  expectRoundTrip<CompactSerializer, T>();
  expectRoundTrip<SimpleJSONSerializer, T>();
}

TEST(CppArenaPmrTest, SerializationRoundTrips) {
  expectRoundTrip<Strings>();
  expectRoundTrip<Containers>();
  expectRoundTrip<StructElements>();
  expectRoundTrip<WholeStruct>();
  expectRoundTrip<ArenaUnion>();
  expectRoundTrip<WholeUnion>();
  expectRoundTrip<CustomTypes>();
}

template <class Serializer, class T>
void expectDeserializationLandsOnTheResource() {
  T in;
  fill(in);
  const auto buf = Serializer::template serialize<std::string>(in);
  Arena arena;
  T out{&arena};
  {
    NoDefaultResource noDefault;
    Serializer::deserialize(buf, out);
  }
  EXPECT_EQ(out, in);
  expectOn(out, &arena);
}

template <class T>
void expectDeserializationLandsOnTheResource() {
  expectDeserializationLandsOnTheResource<BinarySerializer, T>();
  expectDeserializationLandsOnTheResource<CompactSerializer, T>();
  expectDeserializationLandsOnTheResource<SimpleJSONSerializer, T>();
}

TEST(CppArenaPmrTest, DeserializationLandsOnTheResource) {
  expectDeserializationLandsOnTheResource<Strings>();
  expectDeserializationLandsOnTheResource<Containers>();
  expectDeserializationLandsOnTheResource<StructElements>();
  expectDeserializationLandsOnTheResource<WholeStruct>();
  expectDeserializationLandsOnTheResource<ArenaUnion>();
  expectDeserializationLandsOnTheResource<WholeUnion>();
  expectDeserializationLandsOnTheResource<HoldsUnions>();
  expectDeserializationLandsOnTheResource<ArenaError>();
  expectDeserializationLandsOnTheResource<CustomTypes>();
}

TEST(CppArenaPmrTest, CopyGoesToTheDefaultResource) {
  Arena arena;
  Containers src{&arena};
  fill(src);
  const Containers copy(src);
  EXPECT_EQ(copy, src);
  expectOn(copy, std::pmr::get_default_resource());
}

TEST(CppArenaPmrTest, MoveConstructionKeepsTheResource) {
  Arena arena;
  Containers src{&arena};
  fill(src);
  const Containers expected(src);
  const std::pmr::string* names = src.names()->data();

  NoDefaultResource noDefault;
  const Containers moved(std::move(src));
  EXPECT_EQ(moved, expected);
  expectOn(moved, &arena);
  EXPECT_EQ(moved.names()->data(), names);
}

TEST(CppArenaPmrTest, MoveAssignmentAcrossResourcesCopies) {
  Arena arena;
  Arena other;
  Containers src{&arena};
  fill(src);
  const Containers expected(src);
  Containers dst{&other};
  dst = std::move(src);
  EXPECT_EQ(dst, expected);
  expectOn(dst, &other);
}

TEST(CppArenaPmrTest, UnionCopyAndMove) {
  Arena arena;
  ArenaUnion src{&arena};
  fill(src);
  const ArenaUnion copy(src);
  EXPECT_EQ(copy, src);
  expectOn(copy, std::pmr::get_default_resource());

  Arena other;
  ArenaUnion assigned{&other};
  assigned = src;
  EXPECT_EQ(assigned, src);
  expectOn(assigned, &other);

  NoDefaultResource noDefault;
  const ArenaUnion moved(std::move(src));
  EXPECT_EQ(moved, copy);
  expectOn(moved, &arena);
}

TEST(CppArenaPmrTest, OpFunctions) {
  Containers a;
  fill(a);
  Containers b;
  fill(b);
  using Tag = t::struct_t<Containers>;
  EXPECT_TRUE(op::equal<Tag>(a, b));
  EXPECT_TRUE(op::identical<Tag>(a, b));
  EXPECT_EQ(op::hash<Tag>(a), op::hash<Tag>(b));

  b.names()->emplace_back(kOther);
  EXPECT_FALSE(op::equal<Tag>(a, b));
  EXPECT_FALSE(op::identical<Tag>(a, b));
  EXPECT_NE(op::hash<Tag>(a), op::hash<Tag>(b));

  // The double level keeps its exact tag, so NaN is identical but not equal.
  constexpr double kNaN = std::numeric_limits<double>::quiet_NaN();
  a.values()->push_back(kNaN);
  b.values()->push_back(kNaN);
  using ValuesTag = op::get_type_tag<Containers, ident::values>;
  EXPECT_FALSE(op::equal<ValuesTag>(*a.values(), *b.values()));
  EXPECT_TRUE(op::identical<ValuesTag>(*a.values(), *b.values()));

  using GroupsTag = op::get_type_tag<Containers, ident::groups>;
  EXPECT_TRUE(op::equal<GroupsTag>(*a.groups(), *b.groups()));
  EXPECT_EQ(op::hash<GroupsTag>(*a.groups()), op::hash<GroupsTag>(*b.groups()));
}

Orderable makeOrderable(const char* key) {
  Orderable o;
  o.keys()->emplace(key);
  (*o.values())[key].emplace_back(kOther);
  return o;
}

TEST(CppArenaPmrTest, StructsWithArenaSetsAndMapsStayOrderable) {
  const Orderable a = makeOrderable("a long enough key for the heap, first");
  const Orderable b = makeOrderable("a long enough key for the heap, second");
  EXPECT_TRUE(a < b);
  EXPECT_EQ(a <=> b, std::partial_ordering::less);
  EXPECT_EQ(
      op::compare<t::struct_t<Orderable>>(a, b), std::partial_ordering::less);
  EXPECT_TRUE(op::less<t::struct_t<Orderable>>(a, b));
  EXPECT_EQ(a, Orderable(a));
  EXPECT_NE(a, b);

  HoldsOrderableSet holder;
  holder.items()->insert(b);
  holder.items()->insert(a);
  holder.items()->insert(Orderable(a));
  ASSERT_EQ(holder.items()->size(), 2);
  EXPECT_EQ(*holder.items()->begin(), a);
  EXPECT_EQ(
      CompactSerializer::deserialize<HoldsOrderableSet>(
          CompactSerializer::serialize<std::string>(holder)),
      holder);
}

TEST(CppArenaPmrTest, DebugStringAndJson5) {
  Containers c;
  fill(c);
  EXPECT_NE(debugString(c).find(kOther), std::string::npos);
  EXPECT_NE(Json5ProtocolUtils::toJson5(c).find(kOther), std::string::npos);
  StructElements e;
  fill(e);
  EXPECT_NE(debugString(e).find(kLong), std::string::npos);
  EXPECT_NE(Json5ProtocolUtils::toJson5(e).find(kLong), std::string::npos);
  std::ostringstream os;
  PrintTo(e, &os); // apache::thrift::PrintTo
  EXPECT_NE(os.str().find(kLong), std::string::npos);
}

} // namespace
} // namespace apache::thrift::test::arena_pmr
