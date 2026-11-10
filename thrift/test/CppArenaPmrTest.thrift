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

package "facebook.com/thrift/test/arena_pmr"

namespace cpp2 apache.thrift.test.arena_pmr

enum Color {
  RED = 1,
  GREEN = 2,
}

struct Strings {
  @cpp.Arena
  1: string name;
  @cpp.Arena
  2: binary data;
  @cpp.Arena
  3: optional string nickname;
  @cpp.Arena
  @thrift.TerseWrite
  4: string terse_name;
  5: i64 id;
  6: string plain;
}

// Holds arena fields only through `Strings`.
struct HoldsStrings {
  1: Strings strings;
}

struct Containers {
  @cpp.Arena
  1: list<string> names;
  @cpp.Arena
  2: set<string> tags;
  @cpp.Arena
  3: map<string, set<i32>> groups;
  @cpp.Arena
  4: list<list<string>> matrix;
  @cpp.Arena
  5: map<string, list<binary>> blobs;
  @cpp.Arena
  6: list<map<i32, set<binary>>> nested;
  @cpp.Arena
  7: list<double> values;
  @cpp.Arena
  8: map<Color, string> by_color;
  @cpp.Arena
  9: optional list<string> maybe;
  @cpp.Arena
  @thrift.TerseWrite
  10: set<string> terse_tags;
}

// No `@cpp.Arena`: an arena container of these puts only its own nodes on the
// arena.
struct PlainInner {
  1: string name;
  2: list<i32> values;
}

// Allocator-aware: an arena container of these passes its resource on.
struct ArenaInner {
  @cpp.Arena
  1: string name;
  @cpp.Arena
  2: list<string> tags;
}

struct StructElements {
  @cpp.Arena
  1: list<PlainInner> plains;
  @cpp.Arena
  2: map<string, PlainInner> plain_by_name;
  @cpp.Arena
  3: list<ArenaInner> arenas;
  @cpp.Arena
  4: map<string, ArenaInner> arena_by_name;
}

union ArenaUnion {
  @cpp.Arena
  1: string name;
  @cpp.Arena
  2: list<string> names;
  3: i64 id;
}

// Holds arena fields only through `ArenaUnion`.
struct HoldsArenaUnion {
  1: ArenaUnion u;
}

typedef string Name
typedef list<Name> Names

struct Typedefs {
  @cpp.Arena
  1: Names names;
  @cpp.Arena
  2: map<Name, Names> by_name;
}

@cpp.Type{template = "std::deque"}
typedef list<string> Deque

@cpp.Type{name = "::std::uint32_t"}
typedef i32 U32

@cpp.Type{name = "::folly::IOBuf"}
typedef binary IOBuf

struct CustomTypes {
  @cpp.Arena
  @cpp.Type{template = "std::unordered_map"}
  1: map<string, Deque> by_name;
  @cpp.Arena
  @cpp.Type{name = "::folly::fbstring"}
  2: string fbstring;
  @cpp.Arena
  3: list<U32> u32s;
  @cpp.Arena
  4: map<string, IOBuf> iobufs;
}

struct Orderable {
  @cpp.Arena
  1: set<string> keys;
  @cpp.Arena
  2: map<string, list<string>> values;
  3: i32 rank;
}

struct HoldsOrderableSet {
  // @lint-ignore THRIFTCHECKS bad-key-type
  1: set<Orderable> items;
}
