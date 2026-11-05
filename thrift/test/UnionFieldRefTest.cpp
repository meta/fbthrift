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

#include <cstddef>
#include <memory_resource>
#include <utility>

#include <gtest/gtest.h>
#include <folly/io/IOBuf.h>
#include <thrift/lib/cpp2/BadFieldAccess.h>
#include <thrift/test/gen-cpp2/UnionFieldRef_types.h>
using namespace std;

namespace apache::thrift::test {

TEST(UnionFieldTest, basic) {
  Basic a;

  EXPECT_EQ(a.getType(), Basic::Type::__EMPTY__);
  EXPECT_FALSE(a.int64());
  EXPECT_THROW(a.int64().value(), bad_union_field_access);
  EXPECT_THROW(a.int64().as_const().value(), bad_union_field_access);
  EXPECT_FALSE(a.list_i32());
  EXPECT_THROW(a.list_i32().value(), bad_union_field_access);
  EXPECT_THROW(a.list_i32().as_const().value(), bad_union_field_access);
  EXPECT_FALSE(a.str());
  EXPECT_THROW(a.str().value(), bad_union_field_access);
  EXPECT_THROW(a.str().as_const().value(), bad_union_field_access);

  const int64_t int64 = 42LL << 42;
  a.int64() = int64;
  EXPECT_EQ(a.getType(), Basic::Type::int64);
  EXPECT_TRUE(a.int64());
  EXPECT_EQ(a.int64().value(), int64);
  EXPECT_EQ(a.int64().as_const().value(), int64);
  EXPECT_FALSE(a.list_i32());
  EXPECT_THROW(a.list_i32().value(), bad_union_field_access);
  EXPECT_THROW(a.list_i32().as_const().value(), bad_union_field_access);
  EXPECT_FALSE(a.str());
  EXPECT_THROW(a.str().value(), bad_union_field_access);
  EXPECT_THROW(a.str().as_const().value(), bad_union_field_access);

  const vector<int32_t> list_i32 = {3, 1, 2};
  a.list_i32() = list_i32;
  EXPECT_EQ(a.getType(), Basic::Type::list_i32);
  EXPECT_FALSE(a.int64());
  EXPECT_THROW(a.int64().value(), bad_union_field_access);
  EXPECT_THROW(a.int64().as_const().value(), bad_union_field_access);
  EXPECT_TRUE(a.list_i32());
  EXPECT_EQ(a.list_i32().value(), list_i32);
  EXPECT_EQ(a.list_i32().as_const().value(), list_i32);
  EXPECT_FALSE(a.str());
  EXPECT_THROW(a.str().value(), bad_union_field_access);
  EXPECT_THROW(a.str().as_const().value(), bad_union_field_access);

  const string str = "foo";
  a.str() = str;
  EXPECT_EQ(a.getType(), Basic::Type::str);
  EXPECT_FALSE(a.int64());
  EXPECT_THROW(a.int64().value(), bad_union_field_access);
  EXPECT_THROW(a.int64().as_const().value(), bad_union_field_access);
  EXPECT_FALSE(a.list_i32());
  EXPECT_THROW(a.list_i32().value(), bad_union_field_access);
  EXPECT_THROW(a.list_i32().as_const().value(), bad_union_field_access);
  EXPECT_TRUE(a.str());
  EXPECT_EQ(a.str().value(), str);
  EXPECT_EQ(a.str().as_const().value(), str);
}

TEST(UnionFieldTest, operator_deref) {
  Basic a;
  EXPECT_THROW(*a.int64(), bad_union_field_access);
  EXPECT_THROW(*a.str(), bad_union_field_access);
  a.int64() = 42;
  EXPECT_EQ(*a.int64(), 42);
  EXPECT_THROW(*a.str(), bad_union_field_access);
  a.str() = "foo";
  EXPECT_EQ(*a.str(), "foo");
  EXPECT_THROW(*a.int64(), bad_union_field_access);
}

TEST(UnionFieldTest, move_to_optional) {
  Basic a;
  std::optional<std::vector<int32_t>> opt = a.list_i32().move_to_optional();
  EXPECT_FALSE(opt);

  const std::vector<int32_t> expected = {1, 2, 3};
  a.list_i32() = expected;
  const auto* data = a.list_i32()->data();
  opt = a.list_i32().move_to_optional();

  ASSERT_TRUE(opt);
  EXPECT_EQ(*opt, expected);
  EXPECT_TRUE(a.list_i32().has_value());
  EXPECT_EQ(opt->data(), data);
}

TEST(UnionFieldTest, operator_assign) {
  Basic a;
  a.int64() = 4;
  a.list_i32() = {1, 2};

  // `folly::StringPiece` has explicit support for explicit conversion to
  // `std::string`-like types, make sure that keeps working here.
  folly::StringPiece lvalue = "lvalue";
  a.str() = lvalue;
  a.str() = folly::StringPiece("xvalue");
}

TEST(UnionFieldTest, duplicate_type) {
  DuplicateType a;

  EXPECT_EQ(a.getType(), DuplicateType::Type::__EMPTY__);
  EXPECT_FALSE(a.str1());
  EXPECT_THROW(a.str1().value(), bad_union_field_access);
  EXPECT_FALSE(a.list_i32());
  EXPECT_THROW(a.list_i32().value(), bad_union_field_access);
  EXPECT_FALSE(a.str2());
  EXPECT_THROW(a.str2().value(), bad_union_field_access);

  a.str1() = string(1000, '1');
  EXPECT_EQ(a.getType(), DuplicateType::Type::str1);
  EXPECT_TRUE(a.str1());
  EXPECT_EQ(a.str1().value(), string(1000, '1'));
  EXPECT_FALSE(a.list_i32());
  EXPECT_THROW(a.list_i32().value(), bad_union_field_access);
  EXPECT_FALSE(a.str2());
  EXPECT_THROW(a.str2().value(), bad_union_field_access);

  a.list_i32() = vector<int32_t>(1000, 2);
  EXPECT_EQ(a.getType(), DuplicateType::Type::list_i32);
  EXPECT_FALSE(a.str1());
  EXPECT_THROW(a.str1().value(), bad_union_field_access);
  EXPECT_TRUE(a.list_i32());
  EXPECT_EQ(a.list_i32().value(), vector<int32_t>(1000, 2));
  EXPECT_FALSE(a.str2());
  EXPECT_THROW(a.str2().value(), bad_union_field_access);

  a.str2() = string(1000, '3');
  EXPECT_EQ(a.getType(), DuplicateType::Type::str2);
  EXPECT_FALSE(a.str1());
  EXPECT_THROW(a.str1().value(), bad_union_field_access);
  EXPECT_FALSE(a.list_i32());
  EXPECT_THROW(a.list_i32().value(), bad_union_field_access);
  EXPECT_TRUE(a.str2());
  EXPECT_EQ(a.str2().value(), string(1000, '3'));

  a.str1() = string(1000, '4');
  EXPECT_EQ(a.getType(), DuplicateType::Type::str1);
  EXPECT_TRUE(a.str1());
  EXPECT_EQ(a.str1().value(), string(1000, '4'));
  EXPECT_FALSE(a.list_i32());
  EXPECT_THROW(a.list_i32().value(), bad_union_field_access);
  EXPECT_FALSE(a.str2());
  EXPECT_THROW(a.str2().value(), bad_union_field_access);

  a.str1() = string(1000, '5');
  EXPECT_EQ(a.getType(), DuplicateType::Type::str1);
  EXPECT_TRUE(a.str1());
  EXPECT_EQ(a.str1().value(), string(1000, '5'));
  EXPECT_FALSE(a.list_i32());
  EXPECT_THROW(a.list_i32().value(), bad_union_field_access);
  EXPECT_FALSE(a.str2());
  EXPECT_THROW(a.str2().value(), bad_union_field_access);
}

TEST(UnionFieldTest, const_union) {
  {
    const Basic a;
    EXPECT_EQ(a.getType(), Basic::Type::__EMPTY__);
    EXPECT_FALSE(a.int64());
    EXPECT_THROW(a.int64().value(), bad_union_field_access);
    EXPECT_FALSE(a.list_i32());
    EXPECT_THROW(a.list_i32().value(), bad_union_field_access);
    EXPECT_FALSE(a.str());
    EXPECT_THROW(a.str().value(), bad_union_field_access);
  }

  {
    Basic b;
    const string str = "foo";
    b.str() = str;

    const Basic& a = b;
    EXPECT_EQ(a.getType(), Basic::Type::str);
    EXPECT_FALSE(a.int64());
    EXPECT_THROW(a.int64().value(), bad_union_field_access);
    EXPECT_FALSE(a.list_i32());
    EXPECT_THROW(a.list_i32().value(), bad_union_field_access);
    EXPECT_TRUE(a.str());
    EXPECT_EQ(a.str().value(), str);
  }
}
TEST(UnionFieldTest, emplace) {
  DuplicateType a;

  EXPECT_EQ(a.str1().emplace(5, '0'), "00000");
  EXPECT_EQ(*a.str1(), "00000");
  EXPECT_TRUE(a.str1().has_value());

  EXPECT_EQ(a.str2().emplace({'1', '2', '3', '4', '5'}), "12345");
  EXPECT_EQ(*a.str2(), "12345");
  EXPECT_TRUE(a.str2().has_value());

  const vector<int32_t> list_i32 = {3, 1, 2};
  EXPECT_EQ(a.list_i32().emplace(list_i32), list_i32);
  EXPECT_EQ(*a.list_i32(), list_i32);
  EXPECT_TRUE(a.list_i32().has_value());
}

using ByteAlloc = std::pmr::polymorphic_allocator<std::byte>;

// Moving, with or without an allocator, and move assignment are noexcept, so
// the checks below depend only on union_field_ref. A thrift union's members
// can't show this: their allocator-extended moves may allocate. Not in an
// anonymous namespace: the checks never call the members, which draws
// unused-function warnings when the type has internal linkage.
struct NothrowMove {
  using allocator_type = std::pmr::polymorphic_allocator<char>;
  NothrowMove() = default;
  NothrowMove(NothrowMove&&) noexcept = default;
  NothrowMove(NothrowMove&&, const allocator_type&) noexcept {}
  NothrowMove& operator=(NothrowMove&&) noexcept = default;
};

static_assert(
    noexcept(std::declval<union_field_ref<NothrowMove&>&>() = NothrowMove()));
static_assert(noexcept(
    std::declval<union_field_ref<NothrowMove&, ByteAlloc>&>() = NothrowMove()));

TEST(UnionFieldTest, ensure) {
  Basic a;

  EXPECT_FALSE(a.str());
  EXPECT_EQ(a.str().ensure(), "");
  EXPECT_TRUE(a.str());
  EXPECT_EQ(a.str().emplace("123"), "123");
  EXPECT_EQ(a.str().ensure(), "123");
  EXPECT_EQ(*a.str(), "123");

  EXPECT_EQ(a.int64().ensure(), 0);
  EXPECT_FALSE(a.str());
}

TEST(UnionFieldTest, member_of_pointer_operator) {
  Basic a;
  EXPECT_THROW(a.list_i32()->push_back(3), bad_union_field_access);
  a.list_i32().emplace({1, 2});
  a.list_i32()->push_back(3);
  EXPECT_EQ(a.list_i32(), (vector<int32_t>{1, 2, 3}));
}

TEST(UnionFieldTest, comparison) {
  auto test = [](auto i) {
    Basic a;
    auto ref = a.int64();

    ref = i;

    EXPECT_LE(ref, i + 1);
    EXPECT_LE(ref, i);
    EXPECT_LE(i, ref);
    EXPECT_LE(i - 1, ref);

    EXPECT_LT(ref, i + 1);
    EXPECT_LT(i - 1, ref);

    EXPECT_GT(ref, i - 1);
    EXPECT_GT(i + 1, ref);

    EXPECT_GE(ref, i - 1);
    EXPECT_GE(ref, i);
    EXPECT_GE(i, ref);
    EXPECT_GE(i + 1, ref);

    EXPECT_EQ(ref, i);
    EXPECT_EQ(i, ref);

    EXPECT_NE(ref, i - 1);
    EXPECT_NE(i - 1, ref);
  };

  {
    SCOPED_TRACE("same type");
    test(int64_t(10));
  }
  {
    SCOPED_TRACE("different type");
    test('a');
  }
}

TEST(UnionFieldTest, field_ref_api) {
  Basic a;
  a.str() = "foo";
  EXPECT_EQ(*a.str(), "foo");
  EXPECT_EQ(*as_const(a).str(), "foo");
  EXPECT_EQ(*std::move(a).str(), "foo");
  EXPECT_EQ(*std::move(as_const(a)).str(), "foo");
}

TEST(UnionFieldTest, TreeNode) {
  TreeNode root;
  root.nodes().emplace(2);
  (*root.nodes())[0].data() = 10;
  (*root.nodes())[1].data() = 20;

  EXPECT_EQ(root.nodes()->size(), 2);
  EXPECT_EQ((*root.nodes())[0].data(), 10);
  EXPECT_EQ((*root.nodes())[1].data(), 20);

  EXPECT_THROW(*root.data(), bad_union_field_access);
  EXPECT_THROW(*(*root.nodes())[0].nodes(), bad_union_field_access);
  EXPECT_THROW(*(*root.nodes())[1].nodes(), bad_union_field_access);
}

TEST(UnionFieldTest, CppRef) {
  CppRef s;

  static_assert(std::is_same_v<
                std::remove_cvref_t<decltype(s.get_cppref())>,
                std::unique_ptr<CppRef>>);

  static_assert(std::is_same_v<
                std::remove_cvref_t<decltype(s.get_shared_mutable())>,
                std::shared_ptr<CppRef>>);

  static_assert(std::is_same_v<
                std::remove_cvref_t<decltype(s.get_shared_const())>,
                std::shared_ptr<const std::string>>);

  s.str() = "foo";

  EXPECT_EQ(std::as_const(s).str(), "foo");
  EXPECT_EQ(s.str(), "foo");

  EXPECT_THROW(*std::as_const(s).cppref(), bad_union_field_access);
  EXPECT_THROW(*s.cppref(), bad_union_field_access);

  s.cppref().emplace().str() = "ref";

  EXPECT_EQ(std::as_const(s).cppref()->str(), "ref");
  EXPECT_EQ(s.cppref()->str(), "ref");
  EXPECT_THROW(*s.shared_const(), bad_union_field_access);

  s.shared_mutable().emplace().str() = "shared";

  EXPECT_EQ(std::as_const(s).shared_mutable()->str(), "shared");
  EXPECT_EQ(s.shared_mutable()->str(), "shared");
  EXPECT_THROW(*s.cppref(), bad_union_field_access);

  s.box().emplace().str() = "box";

  EXPECT_EQ(std::as_const(s).box()->str(), "box");
  EXPECT_EQ(s.box()->str(), "box");
  EXPECT_THROW(*s.shared_mutable(), bad_union_field_access);

  s.shared_const().emplace("shared_const");

  EXPECT_EQ(std::as_const(s).shared_const(), "shared_const");
  EXPECT_EQ(s.shared_const(), "shared_const");
  EXPECT_THROW(*s.box(), bad_union_field_access);
}

TEST(UnionFieldTest, CppRefEmplaceStorage) {
  CppRef value;
  auto storage = std::make_shared<std::string>("shared_const");
  const auto* storedValue = storage.get();

  value.shared_const().emplace(
      std::shared_ptr<const std::string>(std::move(storage)));

  EXPECT_FALSE(storage);
  EXPECT_EQ(value.shared_const().operator->(), storedValue);
  EXPECT_EQ(value.shared_const(), "shared_const");
}

TEST(UnionFieldTest, CppRefCopyAndMove) {
  CppRef s;
  s.cppref().emplace().str() = "ref";
  CppRef copy = s;
  EXPECT_EQ(copy.cppref()->str(), "ref");
  EXPECT_NE(&*copy.cppref(), &*s.cppref());
  CppRef moved = std::move(copy);
  EXPECT_EQ(moved.cppref()->str(), "ref");

  s.box().emplace().str() = "box";
  copy = s;
  EXPECT_EQ(copy.box()->str(), "box");
  moved = std::move(copy);
  EXPECT_EQ(moved.box()->str(), "box");

  s.shared_const().emplace("shared_const");
  copy = s;
  EXPECT_EQ(copy.shared_const(), "shared_const");
  moved = std::move(copy);
  EXPECT_EQ(moved.shared_const(), "shared_const");
}

TEST(UnionFieldTest, DeprecatedFieldCopyAndMove) {
  Deprecated s;
  FOLLY_PUSH_WARNING
  FOLLY_GNU_DISABLE_WARNING("-Wdeprecated-declarations")
  s.deprecated_field() = 42;
  Deprecated copy = s;
  EXPECT_EQ(copy.deprecated_field(), 42);
  Deprecated moved = std::move(copy);
  EXPECT_EQ(moved.deprecated_field(), 42);
  copy = moved;
  EXPECT_EQ(copy.deprecated_field(), 42);
  FOLLY_POP_WARNING
}

TEST(UnionFieldTest, CopyAssignFromOwnMember) {
  Boxed s;
  s.box().emplace().int32() = 42;
  s = *s.box();
  EXPECT_EQ(s.int32(), 42);

  s.box().emplace().str() = std::string(100, 'x');
  s = *s.box();
  EXPECT_EQ(s.str(), std::string(100, 'x'));
}

TEST(UnionFieldTest, MoveAssignFromOwnMember) {
  Boxed s;
  s.box().emplace().int32() = 42;
  s = std::move(*s.box());
  EXPECT_EQ(s.int32(), 42);

  s.box().emplace().str() = std::string(100, 'x');
  s = std::move(*s.box());
  EXPECT_EQ(s.str(), std::string(100, 'x'));
}

TEST(UnionFieldTest, AssignFromMemberInList) {
  Bar s;
  s.foo().emplace().bar()->emplace_back().str() = std::string(100, 'x');
  s = s.foo()->bar()->at(0);
  EXPECT_EQ(s.str(), std::string(100, 'x'));

  s.foo().emplace().bar()->emplace_back().str() = std::string(100, 'y');
  s = std::move(s.foo()->bar()->at(0));
  EXPECT_EQ(s.str(), std::string(100, 'y'));
}

TEST(UnionFieldTest, MoveListOfIncomplete) {
  ListOfIncomplete s;
  s.structs().emplace().emplace_back();
  ListOfIncomplete moved;
  moved = std::move(s);
  EXPECT_EQ(moved.structs()->size(), 1);
  // NOLINTNEXTLINE(bugprone-use-after-move)
  EXPECT_EQ(s.getType(), ListOfIncomplete::Type::__EMPTY__);
}

TEST(UnionFieldTest, CppRefAssignFromOwnMember) {
  CppRef s;
  s.cppref().emplace().cppref().emplace().str() = std::string(100, 'x');
  s = *s.cppref();
  EXPECT_EQ(s.cppref()->str(), std::string(100, 'x'));

  s.cppref().emplace().cppref().emplace().str() = std::string(100, 'y');
  s = std::move(*s.cppref());
  EXPECT_EQ(s.cppref()->str(), std::string(100, 'y'));

  s.shared_mutable().emplace().shared_mutable().emplace().str() = "z";
  s = *s.shared_mutable();
  EXPECT_EQ(s.shared_mutable()->str(), "z");

  s.shared_mutable().emplace().shared_mutable().emplace().str() = "w";
  s = std::move(*s.shared_mutable());
  EXPECT_EQ(s.shared_mutable()->str(), "w");
}

TEST(UnionFieldTest, NullCppRefCopyAndMove) {
  CppRef s;
  s.set_cppref(std::unique_ptr<CppRef>());
  CppRef copy = s;
  EXPECT_EQ(copy.get_cppref(), nullptr);
  CppRef moved = std::move(copy);
  EXPECT_EQ(moved.get_cppref(), nullptr);
}

TEST(UnionFieldTest, SharedCppRefCopyAndMove) {
  CppRef s;
  s.shared_mutable().emplace().str() = "shared";
  std::shared_ptr<CppRef> shared = s.get_shared_mutable();

  CppRef copy = s;
  EXPECT_NE(copy.get_shared_mutable(), shared);
  EXPECT_EQ(copy.shared_mutable()->str(), "shared");

  CppRef moved = std::move(s);
  EXPECT_EQ(moved.get_shared_mutable(), shared);
  EXPECT_EQ(shared->str(), "shared");
}

TEST(UnionFieldTest, IOBufPtrCopyAndMove) {
  IOBufPtr s;
  s.buf().emplace(folly::IOBuf::COPY_BUFFER, "buf");
  IOBufPtr copy = s;
  EXPECT_EQ(copy.buf()->to<std::string>(), "buf");
  EXPECT_NE(&*copy.buf(), &*s.buf());
  IOBufPtr moved = std::move(copy);
  EXPECT_EQ(moved.buf()->to<std::string>(), "buf");

  copy = s;
  EXPECT_EQ(copy.buf()->to<std::string>(), "buf");
  moved = std::move(copy);
  EXPECT_EQ(moved.buf()->to<std::string>(), "buf");
}

TEST(UnionFieldTest, IOBufPtrAssignFromOwnMember) {
  IOBufPtr s;
  s.box().emplace().buf().emplace(folly::IOBuf::COPY_BUFFER, "copy");
  s = *s.box();
  EXPECT_EQ(s.buf()->to<std::string>(), "copy");

  s.box().emplace().buf().emplace(folly::IOBuf::COPY_BUFFER, "move");
  s = std::move(*s.box());
  EXPECT_EQ(s.buf()->to<std::string>(), "move");
}

TEST(UnionFieldTest, NullIOBufPtrCopyAndMove) {
  IOBufPtr s;
  s.set_buf(std::unique_ptr<folly::IOBuf>());
  IOBufPtr copy = s;
  EXPECT_EQ(copy.getType(), IOBufPtr::Type::buf);
  EXPECT_EQ(copy.get_buf(), nullptr);
  IOBufPtr moved = std::move(copy);
  EXPECT_EQ(moved.getType(), IOBufPtr::Type::buf);
  EXPECT_EQ(moved.get_buf(), nullptr);

  copy = s;
  EXPECT_EQ(copy.get_buf(), nullptr);
  moved = std::move(copy);
  EXPECT_EQ(moved.get_buf(), nullptr);
}

TEST(UnionFieldTest, CppMethods) {
  CppMethods t;
  EXPECT_FALSE(t.field().has_value());
}
} // namespace apache::thrift::test
