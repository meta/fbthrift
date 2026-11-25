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

package "facebook.com/thrift/test/fixtures/cpp_allocator_via"

namespace cpp2 test.fixtures.cpp_allocator_via

include "thrift/annotation/cpp.thrift"
include "thrift/annotation/thrift.thrift"

cpp_include "<memory_resource>"

@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{name = "::std::pmr::string"}
typedef string PmrString

@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "::std::pmr::vector"}
typedef list<PmrString> PmrStringList

// The via field is not first, so in the allocator-extended constructors the
// fields built before it take the allocator from the source object, not from
// the via field itself.
@thrift.DeprecatedUnvalidatedAnnotations{
  items = {
    "cpp.allocator": "::std::pmr::polymorphic_allocator<char>",
    "cpp.allocator_via": "aa_list",
  },
}
struct AllocatorVia {
  1: PmrString aa_string;
  2: PmrStringList aa_list;
  3: i32 plain_int;
}
