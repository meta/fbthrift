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

package "facebook.com/thrift/test/fixtures/cpp_allocator_ref"

namespace cpp2 test.fixtures.cpp_allocator_ref

include "thrift/annotation/cpp.thrift"
include "thrift/annotation/thrift.thrift"

cpp_include "<memory_resource>"

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::std::pmr::polymorphic_allocator<char>"},
}
struct Child {
  1: i32 value;
}

// `cpp.use_allocator` on the `cpp.ref` field itself, not on a typedef.
@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::std::pmr::polymorphic_allocator<char>"},
}
struct FieldUseAllocatorRef {
  @cpp.Ref{type = cpp.RefType.SharedMutable}
  @cpp.AllowLegacyNonOptionalRef
  @thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
  1: Child child;
}
