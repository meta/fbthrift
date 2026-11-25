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

include "thrift/annotation/rust.thrift"

package "facebook.com/thrift/test/fixtures/rust_enum_type"

namespace rust test_fixtures_rust_enum_type

@rust.EnumType{type = rust.EnumUnderlyingType.I8}
enum SmallEnum {
  A = 0,
  B = 1,
  C = 2,
}

@rust.EnumType{type = rust.EnumUnderlyingType.I16}
enum SignedEnum {
  NEG = -10,
  ZERO = 0,
  POS = 10,
}

@rust.EnumType{type = rust.EnumUnderlyingType.U16}
enum MediumEnum {
  X = 0,
  Y = 1000,
}

@rust.EnumType{type = rust.EnumUnderlyingType.U32}
enum LargeEnum {
  BIG = 0,
  BIGGER = 100000,
}

enum DefaultEnum {
  M = 0,
  N = 1,
}
