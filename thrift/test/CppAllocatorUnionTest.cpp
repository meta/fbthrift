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

#include <memory_resource>
#include <string>
#include <type_traits>
#include <vector>

#include <folly/io/IOBuf.h>
#include <thrift/lib/cpp2/folly_dynamic/folly_dynamic.h>
#include <thrift/lib/cpp2/op/Clear.h>
#include <thrift/lib/cpp2/op/Encode.h>
#include <thrift/lib/cpp2/op/Get.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>
#include <thrift/test/gen-cpp2/CppAllocatorUnionTest_types.h>

#include <gtest/gtest.h>

namespace apache::thrift::test {

static_assert(
    std::uses_allocator_v<UnionPmr, std::pmr::polymorphic_allocator<UnionPmr>>);
static_assert(std::is_nothrow_move_constructible_v<UnionPmr>);
// Move assignment copies the member if the allocators differ and don't
// propagate.
static_assert(!std::is_nothrow_move_assignable_v<UnionPmr>);
static_assert(std::is_nothrow_move_assignable_v<PropagatingUnion>);
static_assert(std::is_nothrow_constructible_v<UnionPmr, const PmrByteAlloc&>);
static_assert(
    std::is_constructible_v<UnionPmr, const UnionPmr&, const PmrByteAlloc&>);
static_assert(
    std::is_constructible_v<UnionPmr, UnionPmr&&, const PmrByteAlloc&>);
static_assert(!std::is_constructible_v<
              NoncopyableUnionPmr,
              const NoncopyableUnionPmr&,
              const PmrByteAlloc&>);
static_assert(std::is_constructible_v<
              NoncopyableUnionPmr,
              NoncopyableUnionPmr&&,
              const PmrByteAlloc&>);
static_assert(std::is_constructible_v<
              EmptyUnionPmr,
              EmptyUnionPmr&&,
              const PmrByteAlloc&>);
// Only cpp.use_allocator members' refs carry the union's allocator.
static_assert(std::is_same_v<
              decltype(std::declval<UnionPmr&>().aa_string_ref()),
              union_field_ref<UPmrString&, PmrByteAlloc>>);
static_assert(std::is_same_v<
              decltype(std::declval<const UnionPmr&>().aa_string_ref()),
              decltype(std::declval<UnionPmr&>().aa_string_ref().as_const())>);
static_assert(std::is_same_v<
              decltype(std::declval<UnionPmr&>().not_aa_pmr_string_ref()),
              union_field_ref<NotAAPmrString&>>);

namespace {

const char* const kTooLong =
    "This is too long for the small string optimization";

// With null_memory_resource(), anything that allocates from the default
// resource instead of a union's allocator throws.
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

// A default-constructed pmr allocator takes the default resource, so this
// makes a union whose allocator is on `r` without an allocator constructor.
template <class T>
T makeOn(std::pmr::memory_resource* r) {
  DefaultResourceGuard guard(r);
  return T();
}

template <class T>
std::pmr::memory_resource* resourceOf(const T& t) {
  return t.get_allocator().resource();
}

template <class T>
int stateOf(const T& t) {
  return t.get_allocator().outer_allocator().state_;
}

std::pmr::monotonic_buffer_resource makeResource() {
  return std::pmr::monotonic_buffer_resource(std::pmr::new_delete_resource());
}

void expectOnResource(const InnerUnionPmr& u, std::pmr::memory_resource* r) {
  EXPECT_EQ(resourceOf(u), r);
  if (u.getType() == InnerUnionPmr::Type::s) {
    EXPECT_EQ(resourceOf(*u.s_ref()), r);
  } else if (u.getType() == InnerUnionPmr::Type::l) {
    EXPECT_EQ(resourceOf(*u.l_ref()), r);
  }
}

// The union and every allocator-aware part of its active member use `r`.
void expectOnResource(const UnionPmr& u, std::pmr::memory_resource* r) {
  EXPECT_EQ(resourceOf(u), r);
  switch (u.getType()) {
    case UnionPmr::Type::aa_string:
      EXPECT_EQ(resourceOf(*u.aa_string_ref()), r);
      break;
    case UnionPmr::Type::aa_string_list:
      EXPECT_EQ(resourceOf(*u.aa_string_list_ref()), r);
      for (const auto& s : *u.aa_string_list_ref()) {
        EXPECT_EQ(resourceOf(s), r);
      }
      break;
    case UnionPmr::Type::aa_map:
      EXPECT_EQ(resourceOf(*u.aa_map_ref()), r);
      for (const auto& [key, value] : *u.aa_map_ref()) {
        EXPECT_EQ(resourceOf(key), r);
        EXPECT_EQ(resourceOf(value), r);
      }
      break;
    case UnionPmr::Type::aa_child:
      EXPECT_EQ(resourceOf(*u.aa_child_ref()), r);
      EXPECT_EQ(resourceOf(*u.aa_child_ref()->name()), r);
      EXPECT_EQ(resourceOf(*u.aa_child_ref()->ints()), r);
      break;
    case UnionPmr::Type::aa_inner:
      expectOnResource(*u.aa_inner_ref(), r);
      break;
    case UnionPmr::Type::__EMPTY__:
    case UnionPmr::Type::not_a_container:
    case UnionPmr::Type::not_aa_string:
    case UnionPmr::Type::not_aa_pmr_string:
      break;
  }
}

// One allocating value per member kind of UnionPmr, except not_aa_pmr_string:
// its copies allocate from the default resource, which several tests make
// null_memory_resource().
std::vector<UnionPmr> sampleUnions() {
  std::vector<UnionPmr> out;
  out.emplace_back().not_aa_string_ref() = kTooLong;
  out.emplace_back().aa_string_ref() = kTooLong;
  out.emplace_back().aa_string_list_ref().emplace().emplace_back(kTooLong);
  out.emplace_back().aa_map_ref().emplace()[kTooLong] = UPmrIntList{1, 2, 3};
  auto& child = out.emplace_back().aa_child_ref().emplace();
  child.name() = kTooLong;
  child.ints()->assign({4, 5, 6});
  out.emplace_back().aa_inner_ref().emplace().s_ref() = kTooLong;
  return out;
}

UStatefulIntList statefulList(int state) {
  return UStatefulIntList({1, 2, 3}, ScopedStatefulAlloc<int>(state));
}

PropagatingIntList propagatingList(int state) {
  return PropagatingIntList(
      {1, 2, 3}, ScopedPropagatingStatefulAlloc<int32_t>(state));
}

} // namespace

// A default-constructed counting allocator has its own counter, so it compares
// unequal to every other one.
TEST(CppAllocatorUnionTest, CopyCtorUsesSelectOnContainerCopyConstruction) {
  const CountingUnion src;
  const CountingUnion copy(src);
  EXPECT_EQ(copy.get_allocator(), src.get_allocator());
}

TEST(CppAllocatorUnionTest, MoveCtorKeepsSourceAllocator) {
  auto res = makeResource();
  auto src = makeOn<UnionPmr>(&res);
  src.aa_string_ref() = UPmrString(kTooLong, &res);

  const UnionPmr moved(std::move(src));
  EXPECT_EQ(resourceOf(moved), &res);
  EXPECT_EQ(resourceOf(*moved.aa_string_ref()), &res);

  CountingUnion counting;
  const auto alloc = counting.get_allocator();
  const CountingUnion movedCounting(std::move(counting));
  EXPECT_EQ(movedCounting.get_allocator(), alloc);
}

TEST(CppAllocatorUnionTest, AllocatorCtorStoresAllocatorAndIsEmpty) {
  auto res = makeResource();
  const UnionPmr u{PmrByteAlloc(&res)};
  EXPECT_EQ(u.getType(), UnionPmr::Type::__EMPTY__);
  EXPECT_EQ(resourceOf(u), &res);
}

TEST(CppAllocatorUnionTest, AllocatorExtendedCopyAndMoveCtors) {
  auto res = makeResource();
  auto res2 = makeResource();
  UnionPmr src{PmrByteAlloc(&res)};
  src.aa_string_ref() = UPmrString(kTooLong, &res);

  const UnionPmr copy(src, PmrByteAlloc(&res2));
  EXPECT_EQ(copy, src);
  EXPECT_EQ(resourceOf(copy), &res2);
  EXPECT_EQ(resourceOf(*copy.aa_string_ref()), &res2);

  const UnionPmr moved(std::move(src), PmrByteAlloc(&res2));
  EXPECT_EQ(moved, copy);
  EXPECT_EQ(resourceOf(moved), &res2);
  EXPECT_EQ(resourceOf(*moved.aa_string_ref()), &res2);
}

TEST(CppAllocatorUnionTest, PmrVectorOfUnions) {
  auto res = makeResource();
  auto res2 = makeResource();
  std::pmr::vector<UnionPmr> v(&res);
  for (int i = 0; i < 32; ++i) {
    v.emplace_back().not_a_container_ref() = i;
  }
  for (const auto& u : v) {
    EXPECT_EQ(resourceOf(u), &res);
  }
  const std::pmr::vector<UnionPmr> w(v, &res2);
  for (const auto& u : w) {
    EXPECT_EQ(resourceOf(u), &res2);
  }
}

TEST(CppAllocatorUnionTest, UnionFieldOfAllocatorAwareStruct) {
  auto res = makeResource();
  auto res2 = makeResource();
  const HasUnionPmr s{PmrByteAlloc(&res)};
  EXPECT_EQ(resourceOf(*s.u()), &res);
  EXPECT_EQ(resourceOf(*s.heap_u()), std::pmr::get_default_resource());
  const HasUnionPmr copy(s, PmrByteAlloc(&res2));
  EXPECT_EQ(resourceOf(*copy.u()), &res2);
}

TEST(CppAllocatorUnionTest, SetterOfIOBufMember) {
  IOBufUnionPmr u;
  auto buf = folly::IOBuf::copyBuffer(kTooLong);
  const auto* data = buf.get();
  EXPECT_EQ(u.set_buf(std::move(buf)).get(), data);
}

// With a propagating allocator, assigning through the member can leave it on
// another allocator than the union's; moves rebuild it on the union's.
TEST(CppAllocatorUnionTest, MovesRebuildMemberOnUnionAllocator) {
  PropagatingUnion u(ScopedPropagatingStatefulAlloc<>(1));
  u.aa_list_ref().ensure() = propagatingList(2);
  ASSERT_EQ(stateOf(*u.aa_list_ref()), 2);

  PropagatingUnion moved(std::move(u));
  EXPECT_EQ(stateOf(moved), 1);
  EXPECT_EQ(stateOf(*moved.aa_list_ref()), 1);
  const auto* data = moved.aa_list_ref()->data();

  PropagatingUnion assigned(ScopedPropagatingStatefulAlloc<>(3));
  assigned = std::move(moved);
  EXPECT_EQ(stateOf(assigned), 1);
  EXPECT_EQ(assigned.aa_list_ref()->data(), data);

  const auto alloc = assigned.get_allocator();
  const PropagatingUnion withAlloc(std::move(assigned), alloc);
  EXPECT_EQ(withAlloc.aa_list_ref()->data(), data);
}

TEST(CppAllocatorUnionTest, CopyAssignKeepsTargetAllocator) {
  auto res = makeResource();
  auto res2 = makeResource();
  auto src = makeOn<UnionPmr>(&res);
  src.aa_string_ref() = UPmrString(kTooLong, &res);
  auto dst = makeOn<UnionPmr>(&res2);

  dst = src;
  EXPECT_EQ(dst, src);
  EXPECT_EQ(resourceOf(dst), &res2);
  EXPECT_EQ(resourceOf(*dst.aa_string_ref()), &res2);
}

// Copies deep-copy members that hold std::unique_ptr<folly::IOBuf>.
TEST(CppAllocatorUnionTest, CopiesDeepCopyIOBufMembers) {
  auto res = makeResource();
  auto res2 = makeResource();
  IOBufUnionPmr buf{PmrByteAlloc(&res)};
  buf.buf_ref().emplace(*folly::IOBuf::copyBuffer(kTooLong));
  IOBufUnionPmr bufs{PmrByteAlloc(&res)};
  bufs.bufs_ref().emplace().push_back(folly::IOBuf::copyBuffer(kTooLong));

  const IOBufUnionPmr bufCopy(buf);
  EXPECT_NE(&*bufCopy.buf_ref(), &*buf.buf_ref());
  EXPECT_TRUE(folly::IOBufEqualTo{}(*bufCopy.buf_ref(), *buf.buf_ref()));
  IOBufUnionPmr bufsCopy(bufs, PmrByteAlloc(&res2));
  IOBufUnionPmr bufsAssigned{PmrByteAlloc(&res2)};
  bufsAssigned = bufs;
  for (const IOBufUnionPmr* u : {&bufsCopy, &bufsAssigned}) {
    ASSERT_EQ(u->bufs_ref()->size(), 1);
    EXPECT_NE(u->bufs_ref()->front().get(), bufs.bufs_ref()->front().get());
    EXPECT_TRUE(
        folly::IOBufEqualTo{}(
            *u->bufs_ref()->front(), *bufs.bufs_ref()->front()));
  }
}

TEST(CppAllocatorUnionTest, CopyAssignPropagatesPoccaAllocator) {
  PropagatingUnion src(ScopedPropagatingStatefulAlloc<>(1));
  src.n_ref() = 5;
  PropagatingUnion dst(ScopedPropagatingStatefulAlloc<>(2));

  dst = src;
  EXPECT_EQ(stateOf(dst), 1);
  EXPECT_EQ(dst, src);

  // The member is copied after the allocator propagates.
  src.aa_list_ref().emplace(size_t(3), 1);
  PropagatingUnion listDst(ScopedPropagatingStatefulAlloc<>(2));
  listDst = src;
  EXPECT_EQ(stateOf(*listDst.aa_list_ref()), 1);
}

TEST(CppAllocatorUnionTest, MoveAssignKeepsTargetAllocator) {
  auto res = makeResource();
  auto res2 = makeResource();
  auto src = makeOn<UnionPmr>(&res);
  src.aa_string_ref() = UPmrString(kTooLong, &res);
  const UnionPmr expected = src;
  auto dst = makeOn<UnionPmr>(&res2);

  dst = std::move(src);
  EXPECT_EQ(dst, expected);
  EXPECT_EQ(resourceOf(dst), &res2);
  EXPECT_EQ(resourceOf(*dst.aa_string_ref()), &res2);
}

TEST(CppAllocatorUnionTest, UnequalAllocatorMovesOfIOBufMembers) {
  auto res = makeResource();
  auto res2 = makeResource();
  IOBufUnionPmr buf{PmrByteAlloc(&res)};
  buf.buf_ref().emplace(*folly::IOBuf::copyBuffer(kTooLong));
  const IOBufUnionPmr movedBuf(std::move(buf), PmrByteAlloc(&res2));
  EXPECT_TRUE(
      folly::IOBufEqualTo{}(
          *movedBuf.buf_ref(), *folly::IOBuf::copyBuffer(kTooLong)));

  IOBufUnionPmr bufs{PmrByteAlloc(&res)};
  bufs.bufs_ref().emplace().push_back(folly::IOBuf::copyBuffer(kTooLong));
  const auto* data = bufs.bufs_ref()->front().get();
  const IOBufUnionPmr movedBufs(std::move(bufs), PmrByteAlloc(&res2));
  EXPECT_EQ(movedBufs.bufs_ref()->front().get(), data);
}

TEST(CppAllocatorUnionTest, MoveAssignPropagatesPocmaAllocator) {
  PropagatingUnion src(ScopedPropagatingStatefulAlloc<>(1));
  src.n_ref() = 5;
  PropagatingUnion dst(ScopedPropagatingStatefulAlloc<>(2));

  dst = std::move(src);
  EXPECT_EQ(stateOf(dst), 1);
  EXPECT_EQ(*dst.n_ref(), 5);
}

TEST(CppAllocatorUnionTest, SwapUnequalAllocatorsKeepsEachSidesAllocator) {
  auto res = makeResource();
  auto res2 = makeResource();
  auto a = makeOn<UnionPmr>(&res);
  a.aa_string_ref() = UPmrString(kTooLong, &res);
  auto b = makeOn<UnionPmr>(&res2);
  b.aa_string_ref() = UPmrString("b", &res2);
  const UnionPmr expectedA = b;
  const UnionPmr expectedB = a;

  swap(a, b);
  EXPECT_EQ(a, expectedA);
  EXPECT_EQ(b, expectedB);
  EXPECT_EQ(resourceOf(a), &res);
  EXPECT_EQ(resourceOf(b), &res2);
  EXPECT_EQ(resourceOf(*a.aa_string_ref()), &res);
  EXPECT_EQ(resourceOf(*b.aa_string_ref()), &res2);
}

// Swap is a move ctor and two move assignments, so allocators follow POCMA.
TEST(CppAllocatorUnionTest, SwapExchangesPropagatingAllocators) {
  PropagatingUnion a(ScopedPropagatingStatefulAlloc<>(1));
  a.aa_list_ref().emplace(size_t(3), 1);
  PropagatingUnion b(ScopedPropagatingStatefulAlloc<>(2));
  b.n_ref() = 5;

  swap(a, b);
  EXPECT_EQ(stateOf(a), 2);
  EXPECT_EQ(*a.n_ref(), 5);
  EXPECT_EQ(stateOf(b), 1);
  EXPECT_EQ(stateOf(*b.aa_list_ref()), 1);
  EXPECT_EQ(*b.aa_list_ref(), PropagatingIntList(size_t(3), 1));
}

TEST(CppAllocatorUnionTest, FieldRefEnsureEmplaceAssignUseUnionAllocator) {
  auto res = makeResource();
  auto res2 = makeResource();
  const UPmrString onRes2(kTooLong, &res2);
  auto u = makeOn<UnionPmr>(&res);
  DefaultResourceGuard guard(std::pmr::null_memory_resource());

  u.aa_string_ref().ensure().assign(kTooLong);
  EXPECT_EQ(resourceOf(*u.aa_string_ref()), &res);
  EXPECT_EQ(u.aa_string_ref().emplace(onRes2), onRes2);
  EXPECT_EQ(resourceOf(*u.aa_string_ref()), &res);

  u.not_a_container_ref() = 1;
  u.aa_string_ref() = onRes2; // inactive: emplace
  EXPECT_EQ(resourceOf(*u.aa_string_ref()), &res);
}

// Assigning the argument would carry over its propagating allocator.
TEST(CppAllocatorUnionTest, FieldRefEmplaceIgnoresArgumentAllocator) {
  PropagatingUnion u(ScopedPropagatingStatefulAlloc<>(1));

  EXPECT_EQ(u.aa_list_ref().emplace(propagatingList(2)), propagatingList(2));
  EXPECT_EQ(stateOf(*u.aa_list_ref()), 1);
  u.n_ref() = 1;
  const auto list = propagatingList(2);
  u.aa_list_ref() = list; // inactive: emplace
  EXPECT_EQ(*u.aa_list_ref(), list);
  EXPECT_EQ(stateOf(*u.aa_list_ref()), 1);
  EXPECT_EQ(stateOf(u.aa_list_ref().emplace({1, 2, 3})), 1);
}

// A member that does not take the union's allocator is moved in as is.
TEST(CppAllocatorUnionTest, FieldRefLeavesOtherMembersAlone) {
  auto res = makeResource();
  auto res2 = makeResource();
  UnionPmr u{PmrByteAlloc(&res)};
  std::pmr::string s(kTooLong, &res2);
  const auto* data = s.data();
  DefaultResourceGuard guard(std::pmr::null_memory_resource());

  u.not_aa_pmr_string_ref() = std::move(s);

  EXPECT_EQ(u.not_aa_pmr_string_ref()->data(), data);
  EXPECT_EQ(resourceOf(*u.not_aa_pmr_string_ref()), &res2);
}

TEST(CppAllocatorUnionTest, FieldRefEmplaceThrowLeavesUnionEmpty) {
  UnionPmr u{PmrByteAlloc(std::pmr::null_memory_resource())};
  u.not_a_container_ref() = 1;
  EXPECT_THROW(u.aa_string_ref().emplace(kTooLong), std::bad_alloc);
  EXPECT_EQ(u.getType(), UnionPmr::Type::__EMPTY__);
}

TEST(CppAllocatorUnionTest, OpDecodeUsesUnionAllocator) {
  auto res = makeResource();
  UnionPmr src;
  src.aa_string_ref() = kTooLong;
  const IOBufChain input{
      folly::IOBuf::copyBuffer(BinarySerializer::serialize<std::string>(src))};
  // The chain reader takes op::decode's generic UnionDecode path, which
  // creates the member through union_field_ref::ensure(); other readers call
  // the generated readNoXfer.
  BinaryProtocolChainReader reader;
  reader.setInput(&input);
  auto dst = makeOn<UnionPmr>(&res);
  DefaultResourceGuard guard(std::pmr::null_memory_resource());

  op::decode<type::union_t<UnionPmr>>(reader, dst);
  EXPECT_EQ(dst, src);
  EXPECT_EQ(resourceOf(*dst.aa_string_ref()), &res);
}

TEST(CppAllocatorUnionTest, FieldRefUsesUnionAllocatorForNestedMembers) {
  auto res = makeResource();
  auto res2 = makeResource();
  DefaultResourceGuard guard(std::pmr::null_memory_resource());
  UnionPmr u{PmrByteAlloc(&res)};

  u.aa_string_list_ref().emplace().emplace_back(kTooLong);
  expectOnResource(u, &res);
  EXPECT_EQ(
      u.aa_string_ref().emplace(size_t(40), 'x'), UPmrString(40, 'x', &res2));
  expectOnResource(u, &res);
  auto& inner = u.aa_inner_ref().emplace();
  EXPECT_EQ(inner.l_ref().emplace({1, 2, 3}), UPmrIntList({1, 2, 3}, &res2));
  expectOnResource(u, &res);
}

TEST(CppAllocatorUnionTest, CopiesForEveryMemberKind) {
  auto res = makeResource();
  for (const auto& sample : sampleUnions()) {
    const UnionPmr src(sample, PmrByteAlloc(&res));
    // Tests the copy constructor.
    // NOLINTNEXTLINE(performance-unnecessary-copy-initialization)
    const UnionPmr copy(src);
    EXPECT_EQ(copy, src);
    expectOnResource(copy, std::pmr::get_default_resource());
  }

  StatefulUnion s(ScopedStatefulAlloc<>(7));
  s.aa_list_ref() = statefulList(7);
  const StatefulUnion scopy(s);
  EXPECT_EQ(stateOf(scopy), 7);
  EXPECT_EQ(stateOf(*scopy.aa_list_ref()), 7);
}

TEST(CppAllocatorUnionTest, AllocatorExtendedCtorsForEveryMemberKind) {
  auto res = makeResource();
  auto res2 = makeResource();
  const auto samples = sampleUnions();
  DefaultResourceGuard guard(std::pmr::null_memory_resource());
  for (const auto& sample : samples) {
    UnionPmr src(sample, PmrByteAlloc(&res));
    expectOnResource(src, &res);
    const UnionPmr copy(src, PmrByteAlloc(&res2));
    EXPECT_EQ(copy, sample);
    expectOnResource(copy, &res2);
    const UnionPmr moved(std::move(src), PmrByteAlloc(&res2));
    EXPECT_EQ(moved, sample);
    expectOnResource(moved, &res2);
  }
}

TEST(CppAllocatorUnionTest, MoveCtorKeepsSourceAllocatorForEveryMemberKind) {
  auto res = makeResource();
  for (const auto& sample : sampleUnions()) {
    UnionPmr src(sample, PmrByteAlloc(&res));

    const UnionPmr moved(std::move(src));

    EXPECT_EQ(moved, sample);
    expectOnResource(moved, &res);
    // NOLINTNEXTLINE(bugprone-use-after-move)
    EXPECT_EQ(src.getType(), UnionPmr::Type::__EMPTY__);
    EXPECT_EQ(resourceOf(src), &res);
  }
}

// Moves between equal allocators keep the member's cpp.ref fields and
// allocate nothing.
TEST(CppAllocatorUnionTest, EqualAllocatorMovesTakeOverMember) {
  CountingIdentityResource counting;
  const PmrByteAlloc alloc(&counting);
  RefChildUnionPmr src(alloc);
  src.child_ref().emplace().s_ref()->assign(kTooLong);
  const auto* s = src.child_ref()->s_ref().get();
  const int before = counting.allocations();

  RefChildUnionPmr moved(std::move(src));
  EXPECT_EQ(moved.get_allocator(), alloc);
  EXPECT_EQ(moved.child_ref()->s_ref().get(), s);
  RefChildUnionPmr assigned(alloc);
  assigned = std::move(moved);
  EXPECT_EQ(assigned.child_ref()->s_ref().get(), s);
  RefChildUnionPmr extended(std::move(assigned), alloc);
  EXPECT_EQ(extended.child_ref()->s_ref().get(), s);
  RefChildUnionPmr other(alloc);
  swap(extended, other);
  EXPECT_EQ(other.child_ref()->s_ref().get(), s);
  EXPECT_EQ(counting.allocations(), before);
}

TEST(CppAllocatorUnionTest, AssignmentsAndSwapForEveryMemberKind) {
  auto res = makeResource();
  auto res2 = makeResource();
  const auto samples = sampleUnions();
  DefaultResourceGuard guard(std::pmr::null_memory_resource());
  for (const auto& sample : samples) {
    for (bool emptyTarget : {true, false}) {
      const UnionPmr src(sample, PmrByteAlloc(&res));
      UnionPmr copied{PmrByteAlloc(&res2)};
      UnionPmr moved{PmrByteAlloc(&res2)};
      if (!emptyTarget) {
        copied.not_aa_string_ref() = kTooLong;
        moved.not_aa_string_ref() = kTooLong;
      }
      copied = src;
      EXPECT_EQ(copied, sample);
      expectOnResource(copied, &res2);
      moved = UnionPmr(src, PmrByteAlloc(&res));
      EXPECT_EQ(moved, sample);
      expectOnResource(moved, &res2);

      UnionPmr a(sample, PmrByteAlloc(&res));
      UnionPmr b{PmrByteAlloc(&res2)};
      if (!emptyTarget) {
        b = samples[0];
      }
      const UnionPmr expectedA(b, PmrByteAlloc(&res));
      swap(a, b);
      EXPECT_EQ(a, expectedA);
      EXPECT_EQ(b, sample);
      expectOnResource(a, &res);
      expectOnResource(b, &res2);
    }
  }
}

TEST(CppAllocatorUnionTest, NestedMembersInPmrVectorAndStruct) {
  auto res = makeResource();
  auto res2 = makeResource();
  DefaultResourceGuard guard(std::pmr::null_memory_resource());
  std::pmr::vector<UnionPmr> v(&res);
  for (int i = 0; i < 32; ++i) {
    v.emplace_back().aa_string_ref() = kTooLong;
  }
  for (const auto& u : v) {
    expectOnResource(u, &res);
  }
  const std::pmr::vector<UnionPmr> w(v, &res2);
  for (const auto& u : w) {
    expectOnResource(u, &res2);
  }

  HasUnionPmr s{PmrByteAlloc(&res)};
  s.u()->aa_child_ref().emplace().ints()->assign({1, 2, 3});
  expectOnResource(*s.u(), &res);
  const HasUnionPmr copy(s, PmrByteAlloc(&res2));
  expectOnResource(*copy.u(), &res2);
  EXPECT_EQ(copy, s);
}

TEST(CppAllocatorUnionTest, OpDecodeForEveryMemberKind) {
  auto res = makeResource();
  for (const auto& sample : sampleUnions()) {
    const IOBufChain input{folly::IOBuf::copyBuffer(
        BinarySerializer::serialize<std::string>(sample))};
    BinaryProtocolChainReader reader;
    reader.setInput(&input);
    DefaultResourceGuard guard(std::pmr::null_memory_resource());
    UnionPmr dst{PmrByteAlloc(&res)};
    op::decode<type::union_t<UnionPmr>>(reader, dst);
    EXPECT_EQ(dst, sample);
    expectOnResource(dst, &res);
  }
}

TEST(CppAllocatorUnionTest, OpDecodeConstSharedRefMember) {
  auto res = makeResource();
  SharedRefUnionPmr src;
  src.const_s_ref().emplace(kTooLong);
  const IOBufChain input{
      folly::IOBuf::copyBuffer(BinarySerializer::serialize<std::string>(src))};
  BinaryProtocolChainReader reader;
  reader.setInput(&input);
  SharedRefUnionPmr dst{PmrByteAlloc(&res)};
  DefaultResourceGuard guard(std::pmr::null_memory_resource());

  op::decode<type::union_t<SharedRefUnionPmr>>(reader, dst);
  EXPECT_EQ(*dst.const_s_ref(), kTooLong);
  EXPECT_EQ(resourceOf(*dst.const_s_ref()), &res);
}

TEST(CppAllocatorUnionTest, OpGetValueOrNull) {
  UnionPmr u;
  EXPECT_EQ(op::get_value_or_null(u.aa_string_ref()), nullptr);
  u.aa_string_ref() = kTooLong;
  EXPECT_EQ(op::get_value_or_null(u.aa_string_ref()), &*u.aa_string_ref());
}

TEST(CppAllocatorUnionTest, OpClearMember) {
  UnionPmr u;
  u.not_a_container_ref() = 1;
  op::clear<ident::aa_string>(u);
  EXPECT_EQ(u.getType(), UnionPmr::Type::not_a_container);

  u.aa_string_ref() = kTooLong;
  op::clear<ident::aa_string>(u);
  EXPECT_EQ(u.getType(), UnionPmr::Type::__EMPTY__);
}

TEST(CppAllocatorUnionTest, ToDynamic) {
  UnionPmr u;
  u.aa_string_ref() = kTooLong;
  EXPECT_EQ(
      facebook::thrift::to_dynamic(
          u, facebook::thrift::dynamic_format::PORTABLE),
      folly::dynamic(folly::dynamic::object("aa_string", kTooLong)));
}

namespace {

// SimpleJSON is size-omitting; Compact and Binary are size-prefixed.
using DeserializeSerializers = ::testing::Types<
    apache::thrift::SimpleJSONSerializer,
    apache::thrift::CompactSerializer,
    apache::thrift::BinarySerializer>;

template <typename Serializer>
class CppAllocatorUnionDeserializeTest : public ::testing::Test {};
TYPED_TEST_SUITE(CppAllocatorUnionDeserializeTest, DeserializeSerializers);

TYPED_TEST(CppAllocatorUnionDeserializeTest, MembersLandOnUnionAllocator) {
  auto res = makeResource();
  UnionPmr src;
  src.aa_string_ref() = kTooLong;
  const auto bytes = TypeParam::template serialize<std::string>(src);
  auto dst = makeOn<UnionPmr>(&res);
  DefaultResourceGuard guard(std::pmr::null_memory_resource());

  TypeParam::deserialize(bytes, dst);
  EXPECT_EQ(dst, src);
  EXPECT_EQ(resourceOf(*dst.aa_string_ref()), &res);
}

TYPED_TEST(
    CppAllocatorUnionDeserializeTest, EveryMemberKindLandsOnUnionAllocator) {
  auto res = makeResource();
  for (const auto& sample : sampleUnions()) {
    const auto bytes = TypeParam::template serialize<std::string>(sample);
    DefaultResourceGuard guard(std::pmr::null_memory_resource());
    UnionPmr dst{PmrByteAlloc(&res)};
    TypeParam::deserialize(bytes, dst);
    EXPECT_EQ(dst, sample);
    expectOnResource(dst, &res);
  }
}

TYPED_TEST(CppAllocatorUnionDeserializeTest, EmptyUnionKeepsAllocator) {
  auto res = makeResource();
  const auto bytes = TypeParam::template serialize<std::string>(UnionPmr());
  DefaultResourceGuard guard(std::pmr::null_memory_resource());
  UnionPmr dst{PmrByteAlloc(&res)};
  dst.aa_string_ref() = kTooLong;
  TypeParam::deserialize(bytes, dst);
  EXPECT_EQ(dst.getType(), UnionPmr::Type::__EMPTY__);
  EXPECT_EQ(resourceOf(dst), &res);
}

TYPED_TEST(CppAllocatorUnionDeserializeTest, UnionInsideAllocatorAwareStruct) {
  HasUnionPmr src;
  src.u()->aa_inner_ref().emplace().l_ref().emplace(size_t(10), 1);
  for (auto& sample : sampleUnions()) {
    src.us()->push_back(std::move(sample));
  }
  const auto bytes = TypeParam::template serialize<std::string>(src);

  auto res = makeResource();
  DefaultResourceGuard guard(std::pmr::null_memory_resource());
  HasUnionPmr dst{PmrByteAlloc(&res)};
  TypeParam::deserialize(bytes, dst);
  EXPECT_EQ(dst, src);
  expectOnResource(*dst.u(), &res);
  EXPECT_EQ(resourceOf(*dst.us()), &res);
  for (const auto& u : *dst.us()) {
    expectOnResource(u, &res);
  }
}

TYPED_TEST(CppAllocatorUnionDeserializeTest, StatefulUnionRoundTrip) {
  StatefulUnion list;
  list.aa_list_ref() = statefulList(0);
  StatefulUnion map;
  map.aa_map_ref().emplace().emplace(1, 2);
  StatefulUnion str;
  str.aa_string_ref() = kTooLong;
  StatefulUnion child;
  auto& c = child.aa_child_ref().emplace();
  c.aa_list()->assign({1, 2});
  c.plain() = 3;

  for (const auto* src : {&list, &map, &str, &child}) {
    StatefulUnion dst(ScopedStatefulAlloc<>(7));
    TypeParam::deserialize(
        TypeParam::template serialize<std::string>(*src), dst);
    EXPECT_EQ(dst, *src);
    switch (dst.getType()) {
      case StatefulUnion::Type::aa_list:
        EXPECT_EQ(stateOf(*dst.aa_list_ref()), 7);
        break;
      case StatefulUnion::Type::aa_map:
        EXPECT_EQ(stateOf(*dst.aa_map_ref()), 7);
        break;
      case StatefulUnion::Type::aa_string:
        EXPECT_EQ(stateOf(*dst.aa_string_ref()), 7);
        break;
      case StatefulUnion::Type::aa_child:
        EXPECT_EQ(stateOf(*dst.aa_child_ref()->aa_list()), 7);
        break;
      case StatefulUnion::Type::__EMPTY__:
      case StatefulUnion::Type::not_a_container:
      case StatefulUnion::Type::not_aa_list:
      case StatefulUnion::Type::not_aa_string:
        ADD_FAILURE() << "unexpected member";
        break;
    }
  }
}

TEST(CppAllocatorUnionTest, SharedRefMembersUseUnionAllocator) {
  auto res = makeResource();
  auto res2 = makeResource();
  DefaultResourceGuard guard(std::pmr::null_memory_resource());
  SharedRefUnionPmr u{PmrByteAlloc(&res)};
  u.const_s_ref().ensure();
  EXPECT_EQ(resourceOf(*u.const_s_ref()), &res);
  u.shared_s_ref().emplace(kTooLong);
  EXPECT_EQ(resourceOf(*u.shared_s_ref()), &res);

  const SharedRefUnionPmr copy(u, PmrByteAlloc(&res2));
  EXPECT_NE(&*copy.shared_s_ref(), &*u.shared_s_ref());
  EXPECT_EQ(resourceOf(*copy.shared_s_ref()), &res2);
}

// Moves take the pointer over between equal allocators, and otherwise copy the
// pointee, which other owners may share, onto the target's allocator.
TEST(CppAllocatorUnionTest, MovesOfSharedRefMembers) {
  auto res = makeResource();
  auto res2 = makeResource();
  DefaultResourceGuard guard(std::pmr::null_memory_resource());
  SharedRefUnionPmr u{PmrByteAlloc(&res)};
  const auto* s = &u.shared_s_ref().emplace(kTooLong);

  SharedRefUnionPmr moved(std::move(u));
  EXPECT_EQ(&*moved.shared_s_ref(), s);
  SharedRefUnionPmr assigned{PmrByteAlloc(&res2)};
  assigned = std::move(moved);
  EXPECT_NE(&*assigned.shared_s_ref(), s);
  EXPECT_EQ(resourceOf(*assigned.shared_s_ref()), &res2);
  EXPECT_EQ(*assigned.shared_s_ref(), kTooLong);
}

TYPED_TEST(
    CppAllocatorUnionDeserializeTest, SharedRefMemberLandsOnUnionAllocator) {
  auto res = makeResource();
  SharedRefUnionPmr src;
  src.shared_s_ref().emplace(kTooLong);
  const auto bytes = TypeParam::template serialize<std::string>(src);
  DefaultResourceGuard guard(std::pmr::null_memory_resource());
  SharedRefUnionPmr dst{PmrByteAlloc(&res)};

  TypeParam::deserialize(bytes, dst);
  EXPECT_EQ(*dst.shared_s_ref(), kTooLong);
  EXPECT_EQ(resourceOf(*dst.shared_s_ref()), &res);
}

TYPED_TEST(
    CppAllocatorUnionDeserializeTest,
    FieldUseAllocatorSharedRefMemberLandsOnUnionAllocator) {
  auto res = makeResource();
  SharedRefUnionPmr src;
  src.field_aa_s_ref().emplace(kTooLong);
  const auto bytes = TypeParam::template serialize<std::string>(src);
  DefaultResourceGuard guard(std::pmr::null_memory_resource());
  SharedRefUnionPmr dst{PmrByteAlloc(&res)};

  TypeParam::deserialize(bytes, dst);
  EXPECT_EQ(*dst.field_aa_s_ref(), kTooLong);
  EXPECT_EQ(resourceOf(*dst.field_aa_s_ref()), &res);
}

} // namespace

} // namespace apache::thrift::test
