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
include "thrift/annotation/internal.thrift"
include "thrift/annotation/thrift.thrift"

@thrift.AllowLegacyMissingUris
package;

namespace cpp2 apache.thrift.test

cpp_include "thrift/test/CppAllocatorTest.h"

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::ScopedAlwaysThrowAlloc<>"},
}
struct AlwaysThrowChild {
  1: list_i32_1528 aa_list;
  2: set_i32_2716 aa_set;
  3: map_i32_i32_4816 aa_map;
  4: string_4597 aa_string;
  5: i32 not_a_container;
  6: list<i32> not_aa_list;
  7: set<i32> not_aa_set;
  8: map<i32, i32> not_aa_map;
  9: string not_aa_string;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::ScopedAlwaysThrowAlloc<>"},
}
struct AlwaysThrowParent {
  1: AlwaysThrowChild_4275 child;
}

// Applying a non-empty default allocates, which this allocator always refuses.
// The allocator-extended constructor therefore cannot be noexcept.
@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::ScopedAlwaysThrowAlloc<>"},
}
struct AlwaysThrowWithAppliedDefault {
  @cpp.AllowCustomDefaultInAllocCtor
  @thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
  @cpp.Type{template = "::AlwaysThrowVector"}
  1: list<i32> aa_list = [1, 2, 3];
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "PmrByteAlloc"},
}
struct ChildPmr {
  1: list_i32_8699 aa_list;
  2: set_i32_4098 aa_set;
  3: map_i32_i32_7355 aa_map;
  4: string_8090 aa_string;
  5: i32 not_a_container;
  6: list<i32> not_aa_list;
  7: set<i32> not_aa_set;
  8: map<i32, i32> not_aa_map;
  9: string not_aa_string;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "PmrByteAlloc"},
}
struct ParentPmr {
  1: ChildPmr_7470 child;
  2: list_ChildPmr_9632 aa_child_list;
  3: list<ChildPmr> not_aa_child_list;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::ScopedStatefulAlloc<>"},
}
struct AAStruct {
  1: i32 foo;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::ScopedStatefulAlloc<>"},
}
struct NoAllocatorVia {
  1: AAStruct_7495 foo;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {
    "cpp.allocator": "::ScopedStatefulAlloc<>",
    "cpp.allocator_via": "bar",
  },
}
struct YesAllocatorVia {
  @cpp.Name{value = "bar"}
  1: AAStruct_7495 foo;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "PmrByteAlloc"},
}
struct AAStructPmr {
  1: i32 foo;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "PmrByteAlloc"},
}
struct NoAllocatorViaPmr {
  1: AAStructPmr_1852 foo;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "PmrByteAlloc", "cpp.allocator_via": "bar"},
}
struct YesAllocatorViaPmr {
  @cpp.Name{value = "bar"}
  1: AAStructPmr_1852 foo;
}

@internal.CppUseAllocator
@cpp.Type{template = "std::pmr::vector"}
typedef list<i32> StructuredPmrList

@internal.CppAllocator{name = "PmrByteAlloc"}
struct StructuredAllocatorPmr {
  1: StructuredPmrList aa_list;
  @internal.CppUseAllocator
  @cpp.Type{name = "std::pmr::string"}
  2: string aa_string;
  // Uses the unstructured `cpp.use_allocator`.
  3: list_i32_8699 legacy_aa_list;
}

@internal.CppAllocator{name = "PmrByteAlloc"}
@internal.CppAllocatorVia{name = "aa_list"}
struct StructuredAllocatorViaPmr {
  1: StructuredPmrList aa_list;
  @internal.CppUseAllocator
  @cpp.Type{name = "std::pmr::string"}
  2: string aa_string;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::std::allocator<char>"},
}
@thrift.TerseWrite
struct HasEmptyAllocator {
  1: byte foo;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::ScopedStatefulAlloc<>"},
}
struct HasContainerFields {
  @thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
  @cpp.Type{template = "::StatefulAllocVector"}
  1: list<i32> aa_list;
  @thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
  @cpp.Type{template = "::StatefulAllocSet"}
  2: set<i32> aa_set;
  @thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
  @cpp.Type{template = "::StatefulAllocMap"}
  3: map<i32, i32> aa_map;
}

// HasContainerFields, with a custom default on every allocator-aware field,
// opted out of applying it: the allocator-taking constructors value-initialize
// these fields.
@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::ScopedStatefulAlloc<>"},
}
struct HasContainerFieldsWithDefaults {
  @cpp.AllowCustomDefaultInAllocCtor{legacy_ignore_custom_default = true}
  @thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
  @cpp.Type{template = "::StatefulAllocVector"}
  1: list<i32> aa_list = [1, 2, 3];
  @cpp.AllowCustomDefaultInAllocCtor{legacy_ignore_custom_default = true}
  @thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
  @cpp.Type{template = "::StatefulAllocSet"}
  2: set<i32> aa_set = [4, 5];
  @cpp.AllowCustomDefaultInAllocCtor{legacy_ignore_custom_default = true}
  @thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
  @cpp.Type{template = "::StatefulAllocMap"}
  3: map<i32, i32> aa_map = {6: 7};
  // Not allocator-aware: takes the other branch of the field initializer.
  @cpp.AllowCustomDefaultInAllocCtor{legacy_ignore_custom_default = true}
  4: i32 plain = 8;
}

// HasContainerFieldsWithDefaults, applying the custom defaults instead of
// dropping them. Same shape, so the two structs differ only in the annotation
// under test.
@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::ScopedStatefulAlloc<>"},
}
struct HasContainerFieldsWithAppliedDefaults {
  @cpp.AllowCustomDefaultInAllocCtor
  @thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
  @cpp.Type{template = "::StatefulAllocVector"}
  1: list<i32> aa_list = [1, 2, 3];
  @cpp.AllowCustomDefaultInAllocCtor
  @thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
  @cpp.Type{template = "::StatefulAllocSet"}
  2: set<i32> aa_set = [4, 5];
  @cpp.AllowCustomDefaultInAllocCtor
  @thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
  @cpp.Type{template = "::StatefulAllocMap"}
  3: map<i32, i32> aa_map = {6: 7};
  // Not allocator-aware: takes the other branch of the field initializer.
  @cpp.AllowCustomDefaultInAllocCtor
  4: i32 plain = 8;
}

// `cpp.allocator_via` routes the no-argument constructor through a path that
// applies the default either way, so only the allocator-extended constructor
// is governed by the annotation.
@thrift.DeprecatedUnvalidatedAnnotations{
  items = {
    "cpp.allocator": "::ScopedStatefulAlloc<>",
    "cpp.allocator_via": "aa_list",
  },
}
struct HasAllocatorViaWithDefaults {
  @thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
  @cpp.Type{template = "::StatefulAllocVector"}
  1: list<i32> aa_list;
  @cpp.AllowCustomDefaultInAllocCtor{legacy_ignore_custom_default = true}
  2: bool flag = true;
}

// HasAllocatorViaWithDefaults, applying the custom default instead.
@thrift.DeprecatedUnvalidatedAnnotations{
  items = {
    "cpp.allocator": "::ScopedStatefulAlloc<>",
    "cpp.allocator_via": "aa_list",
  },
}
struct HasAllocatorViaWithAppliedDefaults {
  @thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
  @cpp.Type{template = "::StatefulAllocVector"}
  1: list<i32> aa_list;
  @cpp.AllowCustomDefaultInAllocCtor
  2: bool flag = true;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "PmrByteAlloc"},
}
struct HasContainerFieldsPmr {
  1: list_i32_8699 aa_list;
  2: set_i32_4098 aa_set;
  3: map_i32_i32_7355 aa_map;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "PmrByteAlloc"},
}
struct OptionalRefPmr {
  // @lint-ignore THRIFTCHECKS thrift-parser
  @cpp.Ref{type = cpp.RefType.SharedMutable}
  @cpp.AllowLegacyNonOptionalRef
  1: ChildPmr_7470 c;
  2: list_ChildPmr_9632 l;
  // @lint-ignore THRIFTCHECKS thrift-parser
  @cpp.Ref{type = cpp.RefType.SharedMutable}
  @cpp.AllowLegacyNonOptionalRef
  3: ChildPmr s;
}

// A `shared_ptr<const T>` field whose pointee uses the struct's allocator.
@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "PmrByteAlloc"},
}
struct ConstRefPmr {
  // @lint-ignore THRIFTCHECKS thrift-parser
  @cpp.Ref{type = cpp.RefType.Shared}
  @cpp.AllowLegacyNonOptionalRef
  1: ChildPmr_7470 c;
}

// `cpp.use_allocator` on a `cpp.ref` field rather than on its typedef.
@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "PmrByteAlloc"},
}
struct FieldUseAllocatorRefPmr {
  // @lint-ignore THRIFTCHECKS thrift-parser
  @cpp.Ref{type = cpp.RefType.SharedMutable}
  @cpp.AllowLegacyNonOptionalRef
  @thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
  1: ChildPmr c;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "PmrByteAlloc"},
}
struct AdaptedFieldPmr {
  @cpp.Adapter{name = "::ConstructCountingAdapter"}
  1: i32 counted;
  2: list_i32_8699 aa_list;
}

@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "::StatefulAllocMap"}
typedef map<i32, i32> StatefulAllocIntMap

@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "::StatefulAllocMap"}
typedef map<i32, StatefulAllocIntMap> StatefulAllocMapMap

@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "::StatefulAllocSet"}
typedef set<i32> StatefulAllocIntSet

@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "::StatefulAllocMap"}
typedef map<i32, StatefulAllocIntSet> StatefulAllocMapSet

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {
    "cpp.allocator": "::ScopedStatefulAlloc<>",
    "cpp.allocator_via": "aa_map",
  },
}
struct UsesTypedef {
  1: StatefulAllocIntMap aa_map;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::ScopedStatefulAlloc<>"},
}
struct HasNestedContainerFields {
  1: StatefulAllocMapMap aa_map_of_map;
  2: StatefulAllocMapSet aa_map_of_set;
}

@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "std::pmr::map"}
typedef map<i32, i32> PmrIntMap

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "PmrByteAlloc", "cpp.allocator_via": "aa_map"},
}
struct UsesTypedefPmr {
  1: PmrIntMap aa_map;
}

@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "std::pmr::map"}
typedef map<i32, PmrIntMap> PmrMapMap

@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "std::pmr::set"}
typedef set<i32> PmrIntSet

@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "std::pmr::map"}
typedef map<i32, PmrIntSet> PmrMapSet

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "PmrByteAlloc"},
}
struct HasNestedContainerFieldsPmr {
  1: PmrMapMap aa_map_of_map;
  2: PmrMapSet aa_map_of_set;
}

// Set whose element type is itself allocator-aware, used to test element
// allocator propagation on deserialize.
@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "PmrByteAlloc"},
}
struct HasAllocatorAwareSetElement {
  @thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
  @cpp.Type{template = "std::pmr::set"}
  1: set<string_8090> field;
}

// List whose element type is itself allocator-aware, used to test element
// allocator propagation on deserialize through emplace_back_default.
@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "PmrByteAlloc"},
}
struct HasAllocatorAwareListElement {
  @thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
  @cpp.Type{template = "std::pmr::vector"}
  1: list<string_8090> field;
}

// Map whose key and value types are themselves allocator-aware, used to test
// key/value allocator propagation on deserialize.
@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "PmrByteAlloc"},
}
struct HasAllocatorAwareMapElement {
  @thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
  @cpp.Type{template = "std::pmr::map"}
  1: map<string_8090, string_8090> field;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "PmrByteAlloc"},
}
struct HasAllocatorAwareSortedVectorMapValue {
  @thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
  @cpp.Type{template = "::PmrSortedVectorMap"}
  1: map<i32, string_8090> field;
}

struct BoxedPayload {
  1: i32 value;
}

// Boxed fields are not allocator-backed. Only `intern_boxed` carries an isset
// bit: `field_has_isset` excludes `@thrift.Box` but not `@thrift.InternBox`.
@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "PmrByteAlloc"},
}
struct HasBoxedField {
  @thrift.Box
  1: optional i32 boxed;
  @thrift.InternBox
  2: BoxedPayload intern_boxed;
  3: i32 not_boxed;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::ScopedStatefulAlloc<>"},
}
struct HasSortedUniqueConstructibleFields {
  1: set_i32_992 aa_set;
  2: map_i32_i32_4068 aa_map;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::ScopedCountingAlloc<>"},
}
struct CountingChild {
  1: list_i32_8474 aa_list;
  2: set_i32_9206 aa_set;
  3: map_i32_i32_1496 aa_map;
  4: string_186 aa_string;
  5: i32 not_a_container;
  6: list<i32> not_aa_list;
  7: set<i32> not_aa_set;
  8: map<i32, i32> not_aa_map;
  9: string not_aa_string;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::ScopedCountingAlloc<>"},
}
struct CountingParent {
  1: list_CountingChild_2391 aa_child_list;
  2: list<CountingChild> not_aa_child_list;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::PropagateAllAlloc"},
}
struct PropagateAllAllocStruct {
  1: i32 foo;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::PropagateNoneAlloc"},
}
struct PropagateNoneAllocStruct {
  1: i32 foo;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::PropagateOnlyCopyAlloc"},
}
struct PropagateOnlyCopyAllocStruct {
  1: i32 foo;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::PropagateOnlyMoveAlloc"},
}
struct PropagateOnlyMoveAllocStruct {
  1: i32 foo;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::PropagateOnlySwapAlloc"},
}
struct PropagateOnlySwapAllocStruct {
  1: i32 foo;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::PropagateCopyMoveAlloc"},
}
struct PropagateCopyMoveAllocStruct {
  1: i32 foo;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::PropagateCopySwapAlloc"},
}
struct PropagateCopySwapAllocStruct {
  1: i32 foo;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::PropagateMoveSwapAlloc"},
}
struct PropagateMoveSwapAllocStruct {
  1: i32 foo;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::ScopedAlwaysThrowAlloc<>"},
}
struct AlwaysThrowCppRefChild {
  1: i32 value1;
  2: i64 value2;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::ScopedAlwaysThrowAlloc<>"},
}
struct AlwaysThrowCppRefParent {
  @cpp.Ref{type = cpp.RefType.SharedMutable}
  @cpp.AllowLegacyNonOptionalRef
  2: AlwaysThrowCppRefChild_9703 sharedChild;
  3: i32 no_alloc;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::ScopedCountingAlloc<>"},
}
struct CountingCppRefChild {
  1: i32 value1;
  2: i64 value2;
}

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::ScopedCountingAlloc<>"},
}
struct CountingCppRefParent {
  @cpp.Ref{type = cpp.RefType.SharedMutable}
  @cpp.AllowLegacyNonOptionalRef
  2: CountingCppRefChild_4892 sharedChild;
  @cpp.Ref{type = cpp.RefType.Unique}
  @cpp.AllowLegacyNonOptionalRef
  3: CountingCppRefChild noAllocUniqueChild;
  @cpp.Ref{type = cpp.RefType.SharedMutable}
  @cpp.AllowLegacyNonOptionalRef
  4: CountingCppRefChild noAllocSharedChild;
  5: i32 no_alloc;
  @cpp.Ref{type = cpp.RefType.SharedMutable}
  @cpp.AllowLegacyNonOptionalRef
  6: list_i32_8474 allocVector;
  @thrift.Box
  7: optional CountingCppRefChild noAllocBoxedChild;
}

// The following were automatically generated and may benefit from renaming.
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
typedef AAStructPmr AAStructPmr_1852
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
typedef AAStruct AAStruct_7495
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
typedef AlwaysThrowChild AlwaysThrowChild_4275
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
typedef AlwaysThrowCppRefChild AlwaysThrowCppRefChild_9703
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
typedef ChildPmr ChildPmr_7470
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
typedef CountingCppRefChild CountingCppRefChild_4892
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "std::pmr::vector"}
typedef list<ChildPmr> list_ChildPmr_9632
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "::CountingVector"}
typedef list<CountingChild> list_CountingChild_2391
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "::AlwaysThrowVector"}
typedef list<i32> list_i32_1528
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "::CountingVector"}
typedef list<i32> list_i32_8474
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "std::pmr::vector"}
typedef list<i32> list_i32_8699
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "::CountingMap"}
typedef map<i32, i32> map_i32_i32_1496
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "::StatefulAllocSortedVectorMap"}
typedef map<i32, i32> map_i32_i32_4068
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "::AlwaysThrowMap"}
typedef map<i32, i32> map_i32_i32_4816
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "std::pmr::map"}
typedef map<i32, i32> map_i32_i32_7355
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "::AlwaysThrowSet"}
typedef set<i32> set_i32_2716
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "std::pmr::set"}
typedef set<i32> set_i32_4098
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "::CountingSet"}
typedef set<i32> set_i32_9206
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "::StatefulAllocSortedVectorSet"}
typedef set<i32> set_i32_992
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{name = "::CountingString"}
typedef string string_186
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{name = "::AlwaysThrowString"}
typedef string string_4597
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{name = "std::pmr::string"}
typedef string string_8090

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::ScopedCopyPropagatingAlloc<>"},
}
struct CopyPropagatingChild {
  @thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
  @cpp.Type{template = "::CopyPropagatingVector"}
  1: list<i32> l;
}

@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
typedef CopyPropagatingChild CopyPropagatingChildAA

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {"cpp.allocator": "::ScopedCopyPropagatingAlloc<>"},
}
struct CopyPropagatingParent {
  @thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
  @cpp.Type{name = "::CopyPropagatingString"}
  1: string s;
  // @lint-ignore THRIFTCHECKS thrift-parser
  @cpp.Ref{type = cpp.RefType.SharedMutable}
  @cpp.AllowLegacyNonOptionalRef
  2: CopyPropagatingChildAA c;
}

// PMR fields without `cpp.allocator`. The IOBuf pointer makes copy assignment
// copy and swap.
struct PmrTypedFields {
  @cpp.Type{name = "std::pmr::string"}
  1: string name;
  @cpp.Type{template = "std::pmr::vector"}
  2: list<i32> ids;
  3: i32 count;
  @cpp.Type{name = "std::unique_ptr<folly::IOBuf>"}
  4: binary iobuf;
}
