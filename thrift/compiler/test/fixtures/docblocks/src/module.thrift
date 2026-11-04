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
include "thrift/annotation/python.thrift"
include "thrift/annotation/rust.thrift"

package "facebook.com/thrift/test/fixtures/docblocks"

/// Matches `src/*/*.thrift` and quotes a /* nested comment */.
struct Escapes {
  /// Says """hi""" and ends */ early.
  1: string quotes;
  /// Ends with a backslash \
  2: string backslash;
}

/// Renamed in every language.
@cpp.Name{value = "CppRenamed"}
@python.Name{name = "PyRenamed"}
@rust.Name{name = "RustRenamed"}
struct Renamed {
  @cpp.Name{value = "cpp_field"}
  @python.Name{name = "py_field"}
  @rust.Name{name = "rust_field"}
  1: i32 field;
}

struct Retyped {
  /// Stored in a custom type.
  @cpp.Type{name = "folly::fbstring"}
  @python.Adapter{name = "my.Adapter", typeHint = "my.AdaptedString[]"}
  @rust.Type{name = "smol_str::SmolStr"}
  1: string retyped;
}

struct Undocumented {
  1: i32 plain;
}

/// Ends */ early.
const i32 ESCAPED = 1;

service Documented {
  /// Ends */ early.
  i32 escapes(1: string text);
  @cpp.Name{value = "cpp_renamed"}
  @python.Name{name = "py_renamed"}
  @rust.Name{name = "rust_renamed"}
  void renamed();
}
