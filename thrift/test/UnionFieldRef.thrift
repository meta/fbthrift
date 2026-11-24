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

include "thrift/annotation/thrift.thrift"
include "thrift/annotation/cpp.thrift"

@thrift.AllowLegacyMissingUris
package;

namespace cpp2 apache.thrift.test

union Basic {
  2: string str;
  1: i64 int64;
  4: list<i32> list_i32;
}

union DuplicateType {
  1: string str1;
  2: list<i32> list_i32;
  3: string str2;
}

union TreeNode {
  1: list<TreeNode> nodes;
  2: i32 data;
}

union CppRef {
  1: string str;

  @cpp.Ref{type = cpp.RefType.Unique}
  2: CppRef cppref;

  @cpp.Ref{type = cpp.RefType.SharedMutable}
  3: CppRef shared_mutable;

  @thrift.Box
  4: CppRef box;

  @cpp.Ref{type = cpp.RefType.Shared}
  5: string shared_const;
}

union CppMethods {
  1: i32 field;
}

union Deprecated {
  1: string str;

  @thrift.Deprecated
  2: i32 deprecated_field;
}

union Boxed {
  1: i32 int32;

  @thrift.Box
  2: Boxed box;

  3: string str;
}

struct Foo {
  1: list<Bar> bar;
}

union Bar {
  1: Foo foo;
  2: string str;
}

union ListOfIncomplete {
  1: list<HasListOfIncomplete> structs;
}

struct HasListOfIncomplete {
  1: ListOfIncomplete u;
}

union IOBufPtr {
  @cpp.Type{name = "std::unique_ptr<folly::IOBuf>"}
  1: binary buf;

  @thrift.Box
  2: IOBufPtr box;
}
