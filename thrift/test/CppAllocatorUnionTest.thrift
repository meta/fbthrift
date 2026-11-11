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
include "thrift/annotation/thrift.thrift"

package "facebook.com/thrift/test/cpp_allocator_union"

namespace cpp2 apache.thrift.test

cpp_include "thrift/test/CppAllocatorTest.h"

@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "::StatefulAllocVector"}
typedef list<i32> UStatefulIntList
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "::StatefulAllocMap"}
typedef map<i32, i32> UStatefulIntMap
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{name = "::StatefulAllocString"}
typedef string UStatefulString

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::ScopedStatefulAlloc<>"},
}
struct UStatefulChild {
  1: UStatefulIntList aa_list;
  2: i32 plain;
}
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
typedef UStatefulChild UStatefulChildAA

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::ScopedStatefulAlloc<>"},
}
union StatefulUnion {
  1: UStatefulIntList aa_list;
  2: UStatefulIntMap aa_map;
  3: UStatefulString aa_string;
  4: UStatefulChildAA aa_child;
  5: i32 not_a_container;
  6: list<i32> not_aa_list;
  7: string not_aa_string;
}

@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "::PropagatingStatefulVector"}
typedef list<i32> PropagatingIntList
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "::PropagatingSortedVectorSet"}
typedef set<i32> PropagatingIntSet

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::ScopedPropagatingStatefulAlloc<>"},
}
union PropagatingUnion {
  1: PropagatingIntList aa_list;
  2: i32 n;
  3: PropagatingIntSet aa_set;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::ScopedCountingAlloc<>"},
}
union CountingUnion {
  1: i32 n;
}

@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{name = "std::pmr::string"}
typedef string UPmrString
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "std::pmr::vector"}
typedef list<UPmrString> UPmrStringList
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "std::pmr::vector"}
typedef list<i32> UPmrIntList
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "std::pmr::map"}
typedef map<UPmrString, UPmrIntList> UPmrStringToIntList

// No cpp.use_allocator: never takes the union's allocator.
@cpp.Type{name = "std::pmr::string"}
typedef string NotAAPmrString

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "PmrByteAlloc"},
}
struct UnionChildPmr {
  1: UPmrString name;
  2: UPmrIntList ints;
}
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
typedef UnionChildPmr UnionChildPmrAA

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "PmrByteAlloc"},
}
union InnerUnionPmr {
  1: UPmrString s;
  2: UPmrIntList l;
}
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
typedef InnerUnionPmr InnerUnionPmrAA

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "PmrByteAlloc"},
}
union UnionPmr {
  1: UPmrString aa_string;
  2: UPmrStringList aa_string_list;
  3: UPmrStringToIntList aa_map;
  4: UnionChildPmrAA aa_child;
  5: InnerUnionPmrAA aa_inner;
  6: i32 not_a_container;
  7: string not_aa_string;
  8: NotAAPmrString not_aa_pmr_string;
}
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
typedef UnionPmr UnionPmrAA
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "std::pmr::vector"}
typedef list<UnionPmr> UPmrUnionList

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "PmrByteAlloc"},
}
struct HasUnionPmr {
  1: UnionPmrAA u;
  2: UPmrUnionList us;
  // No cpp.use_allocator: on the default resource, like a struct field.
  3: UnionPmr heap_u;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "PmrByteAlloc"},
}
struct RefChildPmr {
  // @lint-ignore THRIFTCHECKS thrift-parser
  @cpp.Ref{type = cpp.RefType.SharedMutable}
  @cpp.AllowLegacyNonOptionalRef
  1: UPmrString s;
}
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
typedef RefChildPmr RefChildPmrAA

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "PmrByteAlloc"},
}
union RefChildUnionPmr {
  1: RefChildPmrAA child;
}

@cpp.Type{name = "std::unique_ptr<folly::IOBuf>"}
typedef binary IOBufPtr

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "PmrByteAlloc"},
}
union IOBufUnionPmr {
  1: IOBufPtr buf;
  2: list<IOBufPtr> bufs;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "PmrByteAlloc", "cpp.noncopyable": "1"},
}
union NoncopyableUnionPmr {
  1: NotAAPmrString s;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "PmrByteAlloc"},
}
union EmptyUnionPmr {}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "PmrByteAlloc"},
}
union SharedRefUnionPmr {
  @cpp.Ref{type = cpp.RefType.SharedMutable}
  1: UPmrString shared_s;
  @cpp.Ref{type = cpp.RefType.Shared}
  2: UPmrString const_s;
  // `cpp.use_allocator` on the field rather than its typedef.
  @cpp.Ref{type = cpp.RefType.SharedMutable}
  @thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
  3: NotAAPmrString field_aa_s;
}
