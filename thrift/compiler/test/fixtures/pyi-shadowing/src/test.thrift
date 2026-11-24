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

include "thrift/annotation/thrift.thrift"

@thrift.AllowLegacyMissingUris
package;

enum Color {
  RED = 0,
  BLUE = 1,
}

struct Inner {
  1: i32 value;
}

// Fields named after types defined in this file. In the stub, each field is a
// property of the class, so the class body must still be able to refer to the
// types after the field.
struct FieldNamedAfterType {
  1: Inner Inner;
  2: Inner other;
  3: Color Color;
  4: Color another_color;
}

// A field named after the struct that contains it.
struct Node {
  1: i32 Node;
}

// Fields named after builtin types.
struct FieldNamedAfterBuiltin {
  1: string str;
  2: string name;
  3: i32 int;
  4: i32 count;
}
