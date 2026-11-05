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

#include <thrift/test/gen-cpp2/UnionSetterTest_types.h>

#include <gtest/gtest.h>

namespace apache::thrift::test::union_setter {
namespace {

using Type = PlainThrowingUnion::Type;

const ThrowWhenArmedVector<int32_t> kList{1, 2, 3};
const ThrowWhenArmedString kLongString(100, 'x');

PlainThrowingUnion makeList() {
  PlainThrowingUnion u;
  u.l_ref() = kList;
  return u;
}

PlainThrowingUnion makeString() {
  PlainThrowingUnion u;
  u.s_ref() = kLongString;
  return u;
}

PlainThrowingUnion makeHolder() {
  PlainThrowingUnion u;
  u.holder_ref().emplace().l() = kList;
  return u;
}

PlainThrowingUnion makeRefList() {
  PlainThrowingUnion u;
  u.ref_l_ref().emplace(kList);
  return u;
}

TEST(UnionSetterTest, ThrowingCopyKeepsPreviousMember) {
  // Copy assignment copies the active member before clearing the union.
  for (const auto& src :
       {makeList(), makeString(), makeHolder(), makeRefList()}) {
    PlainThrowingUnion u;
    u.n_ref() = 1;
    {
      ScopedThrowingAllocations armed;
      EXPECT_THROW(u = src, std::bad_alloc);
    }
    EXPECT_EQ(u.getType(), Type::n);
    EXPECT_EQ(*u.n_ref(), 1);
  }
}

TEST(UnionSetterTest, CopyConstructsMembers) {
  for (const auto& src :
       {makeList(), makeString(), makeHolder(), makeRefList()}) {
    PlainThrowingUnion u;
    u.n_ref() = 1;
    u = src;
    EXPECT_EQ(u, src);
  }
}

} // namespace
} // namespace apache::thrift::test::union_setter
