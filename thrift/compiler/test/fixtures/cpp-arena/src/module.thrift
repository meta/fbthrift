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

package "facebook.com/thrift/test/fixtures/cpp_arena"

namespace cpp2 test.fixtures.cpp_arena

struct Inner {
  1: string name;
}

struct Fields {
  @cpp.Arena
  1: string name;
  @cpp.Arena
  2: map<string, set<i32>> groups;
  @cpp.Arena
  3: list<Inner> inners;
  4: i64 id;
  @cpp.Arena
  @cpp.Type{template = "std::unordered_map"}
  5: map<string, string> custom;
}

@cpp.Arena
struct WholeStruct {
  1: list<string> names;
  2: i64 id;
}

struct HoldsWholeStruct {
  1: WholeStruct whole;
}

// Unlike the structs above, has no string or container field.
struct ArenaMember {
  @cpp.Arena
  1: WholeStruct arena;
  2: WholeStruct plain;
}

@cpp.Arena
union WholeUnion {
  1: list<string> names;
  2: i64 id;
}
