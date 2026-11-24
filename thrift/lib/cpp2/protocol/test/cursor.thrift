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

namespace cpp2 apache.thrift.test

cpp_include "thrift/lib/cpp2/protocol/CursorBasedSerializer.h"
cpp_include "thrift/lib/cpp2/util/ManagedStringView.h"
cpp_include "thrift/test/AdapterTest.h"

@thrift.Experimental
package "apache.org/thrift/test/cursor"

union Inner {
  1: binary binary_field;
}

@cpp.Adapter{name = "::apache::thrift::CursorSerializationAdapter"}
typedef Struct StructCursor

@cpp.Adapter{name = "::apache::thrift::StringTypeAdapter<::apache::thrift::ManagedStringViewWithConversions>"}
typedef string ManagedStringViewField

@cpp.EnableCustomTypeOrdering
struct Struct {
  1: optional string string_field;
  2: i32 i32_field;
  3: Inner union_field;
  4: list<byte> list_field;
  5: list<set<Stringish>> set_nested_field;
  @cpp.Type{template = "std::unordered_map"}
  6: map<byte, byte> map_field;
  7: bool bool_field;
}

struct Qualifiers {
  1: optional i32 opt;
  2: i32 unq = 1;
  @thrift.TerseWrite
  3: i32 terse;
}

struct Cookie {
  1: i16 id = 2;
  2: string fortune = "About time I got out of that cookie!!";
  3: list<i32> lucky_numbers = [508, 493, 425];
  4: string flavor = "Sugar";
}
struct Meal {
  1: i16 appetizer = 1;
  2: i32 drink = -8;
  3: i16 main = 2;
  4: Cookie cookie;
  5: i16 dessert = 3;
}

union Stringish {
  1: string string_field;
  @cpp.Type{name = "folly::IOBuf"}
  2: binary binary_field;
}

struct StructWithCppType {
  @cpp.Type{name = "std::uint32_t"}
  1: i32 someId;
  2: ManagedStringViewField someName;
}

struct StructWithOptional {
  1: optional string optional_string;
  2: optional list<i64> optional_list;
  3: optional map<i32, i32> optional_map;
  4: optional Containers optional_containers;
}

// Struct with non-optional fields followed by an optional field at the end.
// Used to test that reading an absent trailing optional field doesn't trigger
// a spurious "out of order" error when the cursor hits T_STOP.
struct TrailingOptional {
  1: i32 required_i32;
  2: string required_string;
  3: optional i64 trailing_opt;
}

enum E {
  UNKNOWN = 0,
  A = 1,
  B = 2,
}
struct Numerics {
  1: optional i16 int16;
  @cpp.Type{name = "uint32_t"}
  2: i32 uint32;
  3: E enm;
  4: float flt;
}

struct OutOfOrder {
  5: i16 field1;
  1: i16 field2;
  12: i16 field3 = 1;
  7: i16 field4;
  4: i16 field5;
  11: optional i16 field6;
}

struct Containers {
  1: list<string> list_of_string;
}

struct Types {
  @cpp.Type{name = "folly::IOBuf"}
  1: binary iobuf;
  @cpp.Type{name = "std::unique_ptr<folly::IOBuf>"}
  2: binary iobufptr;
  @cpp.Adapter{name = "::apache::thrift::test::AdaptTestMsAdapter"}
  3: i64 ms;
}

struct FieldLookup12 {
  1: i64 first;
  2: string second;
  16: byte third;
  23: i16 fourth;
  38: i32 fifth;
  39: bool sixth;
  63: double seventh;
  101: list<i32> eighth;
  202: set<i32> ninth;
  303: map<i32, i32> tenth;
  404: i32 eleventh;
  32767: bool last;
}

struct FieldLookup13 {
  1: i64 first;
  2: string second;
  16: byte third;
  23: i16 fourth;
  38: i32 fifth;
  39: bool sixth;
  63: double seventh;
  101: list<i32> eighth;
  202: set<i32> ninth;
  303: map<i32, i32> tenth;
  404: i32 eleventh;
  505: i32 twelfth;
  32767: bool last;
}

struct FieldLookupConcurrent {
  1: i64 first;
  2: string second;
  3: i32 third;
  4: i32 fourth;
  5: i32 fifth;
  6: i32 sixth;
  7: i32 seventh;
  8: i32 eighth;
  9: i32 ninth;
  10: i32 tenth;
  11: i32 eleventh;
  12: i32 twelfth;
  32767: bool last;
}

struct Empty {}

struct Recursive {
  @thrift.Box
  1: optional Recursive child;
}

@cpp.UseCursorSerialization
typedef Empty EmptyWrapper

service Example {
  EmptyWrapper identity(1: EmptyWrapper empty);
}

struct ReadRemaining {
  1: list<string> aaa;
  2: list<i32> bbb;
  3: bool ccc;
}

@cpp.UseCursorSerialization
typedef ReadRemaining ReadRemainingWrapper

@cpp.UseCursorSerialization
struct Refs {
  @cpp.Ref{type = cpp.RefType.Unique}
  1: optional Empty unique;
  @cpp.Ref{type = cpp.RefType.Shared}
  2: optional Empty shared;
  @cpp.Ref{type = cpp.RefType.SharedMutable}
  3: optional Empty shared_mutable;
  @thrift.Box
  4: optional Empty box;
  @thrift.InternBox
  5: Empty intern_box;
}
