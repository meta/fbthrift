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

package "apache.org/thrift/test/testset"

namespace py3 thrift.test
namespace py thrift.test.Terse
namespace py.asyncio thrift_asyncio.test.Terse
namespace java.swift org.apache.thrift.test
namespace go thrift.test.testset.Terse
namespace csharp Apache.Thrift.Test.Terse

include "thrift/annotation/rust.thrift"
include "thrift/annotation/thrift.thrift"

enum TerseEnum {
  zero = 0,
  one = 1,
}

struct TerseEnumStruct {
  @thrift.TerseWrite
  1: TerseEnum field;
}

union TerseUnion {
  1: i32 field_1;
}

struct TerseUnionStruct {
  @thrift.TerseWrite
  1: TerseUnion field;
}

@rust.NewType
typedef i32 NewTypeI32

@rust.NewType
typedef string NewTypeString

@rust.NewType
typedef list<i32> NewTypeListI32

struct TerseNewTypeI32Struct {
  @thrift.TerseWrite
  1: NewTypeI32 field;
}

struct TerseNewTypeStringStruct {
  @thrift.TerseWrite
  1: NewTypeString field;
}

struct TerseNewTypeListI32Struct {
  @thrift.TerseWrite
  1: NewTypeListI32 field;
}

struct Emptiable {
  1: optional i32 optional_field;
  @thrift.TerseWrite
  2: string terse_field;
}

struct NonEmptiable {
  1: i32 field;
}

exception EmptiableException {
  1: optional string message;
}

struct TerseEmptiableStruct {
  @thrift.TerseWrite
  1: Emptiable field;
}

struct TerseNonEmptiableStruct {
  @thrift.TerseWrite
  1: NonEmptiable field;
}

struct TerseEmptiableExceptionStruct {
  @thrift.TerseWrite
  1: EmptiableException field;
}

@rust.Adapter{name = "::fbthrift::adapter::IdentityAdapter<>"}
typedef string AdaptedString

struct TerseAdaptedFieldStruct {
  @rust.Adapter{name = "::fbthrift::adapter::IdentityAdapter<>"}
  @thrift.TerseWrite
  1: string field;
}

struct TerseAdaptedTypedefStruct {
  @thrift.TerseWrite
  1: AdaptedString field;
}

struct TerseNestedAdaptedFieldStruct {
  @thrift.TerseWrite
  1: TerseAdaptedFieldStruct field;
}
