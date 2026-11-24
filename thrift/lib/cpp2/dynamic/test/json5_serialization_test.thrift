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

package "meta.com/thrift/dynamic/test/json5"

namespace cpp2 apache.thrift.dynamic.json5_test

enum Color {
  RED = 1,
  GREEN = 2,
  BLUE = 3,
}

// Fields are declared out of field id order: JSON5 field order and stable
// ordering both follow field ids.
struct Key {
  2: i32 second;
  1: i32 first;
}

union Choice {
  1: i32 number;
  2: string text;
  3: Color color;
}

struct Scalars {
  3: i32 i32Field;
  1: bool boolField;
  2: byte byteField;
  4: i16 i16Field;
  5: i64 i64Field;
  6: float floatField;
  7: double doubleField;
  8: string stringField;
  9: binary binaryField;
  10: Color enumField;
  11: optional string optionalField;
}

struct Containers {
  1: list<i32> intList;
  2: list<Color> enumList;
  3: list<Choice> unionList;
  4: set<string> stringSet;
  5: set<i64> intSet;
  6: set<binary> binarySet;
  7: set<Color> enumSet;
  // @lint-ignore THRIFTCHECKS bad-key-type
  8: set<Key> structSet;
  // @lint-ignore THRIFTCHECKS bad-key-type
  9: set<Choice> unionSet;
  // @lint-ignore THRIFTCHECKS bad-key-type
  10: set<list<i32>> listSet;
  11: map<string, i32> stringMap;
  12: map<i32, string> intKeyMap;
  13: map<Color, string> enumKeyMap;
  // @lint-ignore THRIFTCHECKS bad-key-type
  14: map<Key, string> structKeyMap;
  15: map<binary, i32> binaryKeyMap;
  16: map<string, Color> enumValueMap;
}

struct AllTypes {
  2: Containers containers;
  1: Scalars scalars;
  3: Choice choice;
  4: Key key;
}
