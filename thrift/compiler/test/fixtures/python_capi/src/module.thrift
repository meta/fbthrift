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

include "thrift/annotation/scope.thrift"
include "thrift/annotation/cpp.thrift"
include "thrift/annotation/thrift.thrift"
include "thrift/compiler/test/fixtures/python_capi/src/thrift_dep.thrift"
include "thrift/compiler/test/fixtures/python_capi/src/serialized_dep.thrift"
include "thrift/lib/thrift/id.thrift"
include "thrift/lib/thrift/schema.thrift"

cpp_include "<deque>"
cpp_include "<unordered_map>"
cpp_include "<unordered_set>"
cpp_include "<folly/container/F14Set.h>"
cpp_include "<memory_resource>"
cpp_include "<folly/container/F14Map.h>"
cpp_include "<folly/small_vector.h>"
cpp_include "thrift/test/python_capi/adapter.h"

package "test.dev/fixtures/python_capi"

@thrift.AllowLegacyTypedefUri
@cpp.Type{name = "uint64_t"}
typedef i64 uint64
@thrift.AllowLegacyTypedefUri
typedef uint64 ui64
@thrift.AllowLegacyTypedefUri
@cpp.Type{name = "uint8_t"}
typedef byte uint8

@thrift.AllowLegacyTypedefUri
@cpp.Type{template = "folly::small_vector"}
typedef list<uint8> small_vector_uint8
@thrift.AllowLegacyTypedefUri
@cpp.Type{template = "folly::fbvector"}
typedef list<uint8> fbvector_uint8
@thrift.AllowLegacyTypedefUri
@cpp.Type{template = "folly::fbvector"}
typedef list<fbvector_uint8> fbvector2_uint8
@thrift.AllowLegacyTypedefUri
@cpp.Type{template = "folly::fbvector"}
typedef list<double> fbvector_double

enum MyEnum {
  MyValue1 = 0,
  MyValue2 = 1,
}

@cpp.Name{value = "NormalDecentEnum"}
enum AnnoyingEnum {
  @cpp.Name{value = "l0O1"}
  FOO = 1,
  @cpp.Name{value = "FuBaR"}
  BAR = 2,
}

struct MyStruct {
  1: i64 inty;
  2: string stringy;
  3: MyDataItem myItemy;
  4: MyEnum myEnumy;
  @cpp.Name{value = "boulet"}
  5: bool booly;
  6: list<float> floatListy;
  7: map<binary, string> strMappy;
  8: set<i32> intSetty;
}

struct MyDataItem {
  1: string s;
}

@cpp.Adapter{name = "::thrift::test::lib::StructDoubler"}
@scope.Transitive
struct TransitiveDoubler {}

@TransitiveDoubler
struct DoubledPair {
  1: string s;
  2: i32 x;
}

struct StringPair {
  1: string normal;
  @cpp.Adapter{name = "::thrift::test::lib::StringDoubler"}
  2: string doubled;
}

@cpp.Name{value = "VapidStruct"}
struct EmptyStruct {}

@thrift.AllowLegacyTypedefUri
typedef byte signed_byte
@thrift.AllowLegacyTypedefUri
@cpp.Type{name = "folly::IOBuf"}
typedef binary IOBuf
@thrift.AllowLegacyTypedefUri
@cpp.Type{name = "std::unique_ptr<folly::IOBuf>"}
typedef binary IOBufPtr

struct PrimitiveStruct {
  1: bool booly;
  2: signed_byte charry;
  @cpp.Name{value = "shortay"}
  @cpp.Type{name = "uint16_t"}
  3: i16 shorty;
  5: i32 inty;
  @cpp.Type{name = "uint64_t"}
  7: i64 longy;
  8: optional float floaty;
  @thrift.Box
  9: optional double dubby;
  @cpp.Ref{type = cpp.RefType.Unique}
  12: optional string stringy;
  @cpp.Ref{type = cpp.RefType.Shared}
  13: optional binary bytey;
  14: IOBuf buffy;
  15: IOBufPtr pointbuffy;
  18: MyStruct patched_struct;
  19: EmptyStruct empty_struct;
  @cpp.Type{name = "folly::IOBuf"}
  20: binary custom_iobuf;
  @cpp.Type{name = "::std::pmr::string"}
  21: string pmr_string;
  22: thrift_dep.SomeError some_error;
}

struct AdaptedFields {
  1: id.ProtocolId adapted_int;
  2: list<id.FieldId> list_adapted_int;
  3: schema.AnnotationIds set_adapted_int;
  @cpp.Adapter{
    name = "::apache::thrift::type::detail::StrongIntegerAdapter<::apache::thrift::type::ValueId>",
  }
  4: id.ExternId inline_adapted_int;
}

@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.noncopyable": "1"}}
struct ListStruct {
  1: list<bool> boolz;
  2: optional list<i64> intz;
  @thrift.Box
  3: optional list<string> stringz;
  @cpp.Type{template = "std::deque"}
  4: list<binary> encoded;
  @cpp.Type{template = "std::deque"}
  5: list<uint64> uidz;
  6: list<list<double>> matrix;
  @cpp.Type{template = "folly::small_vector"}
  7: list<small_vector_uint8> ucharz;
  @cpp.Type{template = "folly::fbvector"}
  8: list<fbvector2_uint8> voxels;
  9: list<IOBufPtr> buf_ptrs;
}
@thrift.AllowLegacyTypedefUri
typedef ListStruct ListAlias

@cpp.EnableCustomTypeOrdering
struct SetStruct {
  1: set<MyEnum> enumz;
  2: optional set<i32> intz;
  @thrift.Box
  3: optional set<binary> binnaz;
  @cpp.Type{template = "std::unordered_set"}
  4: set<binary> encoded;
  @cpp.Type{template = "std::unordered_set"}
  5: set<uint64> uidz;
  @cpp.Type{template = "folly::F14FastSet"}
  6: set<uint8> charz;
  7: list<set<i64>> setz;
}

@cpp.EnableCustomTypeOrdering
struct MapStruct {
  1: map<MyEnum, string> enumz;
  2: optional map<i32, string> intz;
  @thrift.Box
  3: optional map<binary, PrimitiveStruct> binnaz;
  @cpp.Type{template = "std::unordered_map"}
  4: map<string, double> encoded;
  @cpp.Type{template = "std::unordered_map"}
  5: map<uint64, float> flotz;
  6: list<map<i32, i64>> map_list;
  7: map<i32, list<i64>> list_map;
  @cpp.Type{template = "folly::F14FastMap"}
  8: map<i32, fbvector_double> fast_list_map;
  9: map<binary, IOBufPtr> buf_map;
  10: map<ui64, list<ui64>> unsigned_list_map;
}

@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.noncopyable": "1"}}
struct ComposeStruct {
  1: MyEnum enum_;
  2: AnnoyingEnum renamed_;
  3: PrimitiveStruct primitive;
  @cpp.Ref{type = cpp.RefType.Shared}
  @cpp.AllowLegacyNonOptionalRef
  4: ListAlias aliased;
  6: thrift_dep.DepStruct xstruct;
  5: thrift_dep.DepEnum xenum;
  8: serialized_dep.SerializedStruct serial_struct;
  7: list<thrift_dep.DepStruct> friends;
  9: serialized_dep.SerializedUnion serial_union;
  10: serialized_dep.SerializedError serial_error;
}

@cpp.Name{value = "Shallot"}
union Onion {
  1: MyEnum myEnum;
  2: PrimitiveStruct myStruct;
  @thrift.Box
  6: set<i64> intSet;
  4: string myString;
  @cpp.Ref{type = cpp.RefType.Shared}
  @cpp.AllowLegacyNonOptionalRef
  8: list<double> doubleList;
  @cpp.Ref{type = cpp.RefType.Unique}
  @cpp.AllowLegacyNonOptionalRef
  9: map<binary, string> strMap;
  10: id.ProtocolId adapted_int;
}

union SomeBinary {
  1: IOBuf iobuf;
  2: IOBufPtr iobuf_ptr;
  @cpp.Ref{type = cpp.RefType.Unique}
  @cpp.AllowLegacyNonOptionalRef
  3: IOBuf iobufRef;
}
