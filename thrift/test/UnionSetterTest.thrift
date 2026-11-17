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

include "thrift/annotation/cpp.thrift"

package "facebook.com/thrift/test/union_setter"

namespace cpp2 apache.thrift.test.union_setter

cpp_include "thrift/test/UnionSetterTest.h"

@cpp.Type{template = "::apache::thrift::test::union_setter::ThrowWhenArmedVector"}
typedef list<i32> ThrowingIntList
@cpp.Adapter{name = "::apache::thrift::StringTypeAdapter<::apache::thrift::test::union_setter::ThrowWhenArmedString>"}
typedef string ThrowingString

struct ThrowingListHolder {
  1: ThrowingIntList l;
}

union PlainThrowingUnion {
  1: ThrowingIntList l;
  2: i32 n;
  3: ThrowingString s;
  4: ThrowingListHolder holder;
  @cpp.Ref{type = cpp.RefType.Unique}
  5: ThrowingIntList ref_l;
}
