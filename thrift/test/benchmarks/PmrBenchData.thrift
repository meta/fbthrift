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

package "meta.com/thrift/test/benchmark/pmr"

namespace cpp2 thrift.benchmark.pmr

include "thrift/annotation/cpp.thrift"
include "thrift/annotation/thrift.thrift"

cpp_include "<memory_resource>"
cpp_include "<unordered_map>"
cpp_include "folly/sorted_vector_types.h"

@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{name = "std::pmr::string"}
typedef string PmrString

@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "std::pmr::vector"}
typedef list<i32> PmrListI32

@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "std::pmr::map"}
typedef map<i32, i32> PmrMapI32

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::std::pmr::polymorphic_allocator<>"},
}
struct PmrMixed {
  1: i32 int32;
  2: i64 int64;
  3: bool b;
  4: PmrString str;
  5: PmrListI32 empty_lst;
  6: PmrMapI32 empty_map;
}

@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "std::pmr::vector"}
typedef list<PmrMixed> PmrListMixed

@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "std::pmr::map"}
typedef map<i32, PmrMixed> PmrMapI32Mixed

@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "std::pmr::unordered_map"}
typedef map<i32, PmrMixed> PmrUnorderedMapI32Mixed

@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "folly::pmr::sorted_vector_map"}
typedef map<i32, PmrMixed> PmrSortedVecMapI32Mixed

@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "std::pmr::map"}
typedef map<PmrString, i32> PmrMapStrI32

@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "std::pmr::unordered_map"}
typedef map<PmrString, i32> PmrUnorderedMapStrI32

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::std::pmr::polymorphic_allocator<>"},
}
struct PmrBigListInt {
  1: PmrListI32 lst;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::std::pmr::polymorphic_allocator<>"},
}
@cpp.UseOpEncode
struct OpPmrBigListInt {
  1: PmrListI32 lst;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::std::pmr::polymorphic_allocator<>"},
}
struct PmrBigListMixed {
  1: PmrListMixed lst;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::std::pmr::polymorphic_allocator<>"},
}
struct PmrMapInt {
  1: PmrMapI32 m;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::std::pmr::polymorphic_allocator<>"},
}
struct PmrMapMixed {
  1: PmrMapI32Mixed m;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::std::pmr::polymorphic_allocator<>"},
}
struct PmrUnorderedMapMixed {
  1: PmrUnorderedMapI32Mixed m;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::std::pmr::polymorphic_allocator<>"},
}
struct PmrSortedVecMapMixed {
  1: PmrSortedVecMapI32Mixed m;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::std::pmr::polymorphic_allocator<>"},
}
struct PmrMapStrInt {
  1: PmrMapStrI32 m;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::std::pmr::polymorphic_allocator<>"},
}
struct PmrUnorderedMapStrInt {
  1: PmrUnorderedMapStrI32 m;
}
