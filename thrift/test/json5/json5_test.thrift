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

package "facebook.com/thrift/json5"

@thrift.Sealed
struct Nested {
  1: string nested;
}

@thrift.Sealed
struct OutOfOrderFields {
  3: i32 Three;
  1: i32 One;
  2: i32 Two;
}

exception OutOfOrderFieldsException {
  3: i32 Three;
  1: i32 One;
  2: i32 Two;
}

union ExampleUnion {
  1: i64 intValue;
  2: string stringValue;
  3: Nested nestedValue;
}

struct Recursive {
  1: optional list<Recursive> children;
  2: optional map<string, Recursive> byName;
  3: optional set<i32> ids;
}

// Unqualified fields that Hack leaves null unless set.
@thrift.Sealed
struct Unqualified {
  1: Enum enumValue;
  2: Nested structValue;
}

union EmptyUnion {}

enum Enum {
  NEGATIVE_ONE = -1,
  DEFAULT = 0,
  ONE = 1,
  TWO = 2,
  MANY = 3,
}

struct Example {
  30: optional bool boolValue;
  31: optional byte byteValue;
  32: optional i16 i16Value;
  33: optional i32 i32Value;
  1: optional i64 i64Value;
  2: optional string stringValue;
  3: optional binary binaryValue;
  4: optional float floatValue;
  5: optional double doubleValue;
  6: optional double infValue;
  7: optional double nanValue;
  8: optional list<Nested> listValue;
  // @lint-ignore THRIFTCHECKS bad-key-type
  9: optional set<Nested> setValue;
  10: optional Enum enumValue;
  // @lint-ignore THRIFTCHECKS bad-key-type
  11: optional map<Nested, Nested> structAsKey;
  12: optional map<Enum, Enum> enumAsKey;
  // @lint-ignore THRIFTCHECKS bad-key-type
  13: optional map<bool, i32> boolAsKey;
  14: optional map<i32, i32> i32AsKey;
  15: optional map<binary, i32> binaryAsKey;
  // @lint-ignore THRIFTCHECKS bad-key-type
  16: optional map<list<i32>, i32> listAsKey;
  // @lint-ignore THRIFTCHECKS bad-key-type
  17: optional map<set<i32>, i32> setAsKey;
  // @lint-ignore THRIFTCHECKS bad-key-type
  18: optional map<map<i32, i32>, i32> mapAsKey;
  // @lint-ignore THRIFTCHECKS bad-key-type
  19: optional map<map<map<i32, i32>, i32>, i32> nestedMapAsKey;
  // @lint-ignore THRIFTCHECKS bad-key-type
  20: optional set<OutOfOrderFields> outOfOrderFieldsInSet;
  // @lint-ignore THRIFTCHECKS bad-key-type
  21: optional map<OutOfOrderFields, i32> outOfOrderFieldsInMap;
  22: optional map<string, string> stringAsKey;
  23: optional map<i64, i64> i64AsKey;
  24: optional set<i32> i32SetValue;
  25: optional ExampleUnion unionValue;
  26: optional list<double> doubleListValue;
  27: optional list<float> floatListValue;
  // @lint-ignore THRIFTCHECKS bad-key-type
  28: optional map<double, i32> doubleAsKey;
  // @lint-ignore THRIFTCHECKS bad-key-type
  29: optional map<float, i32> floatAsKey;
  34: optional Unqualified unqualifiedValue;
  // @lint-ignore THRIFTCHECKS bad-key-type
  35: optional set<Unqualified> unqualifiedSet;
  36: optional EmptyUnion emptyUnion;
  // @lint-ignore THRIFTCHECKS bad-key-type
  @thrift.AllowUnsafeNonSealedKeyType
  37: optional set<OutOfOrderFieldsException> outOfOrderFieldsExceptionInSet;
}

// --
// test expectations are defined here so that they can
// be shared between different languages.
// --

// Every input, `json`, `json5` and each of `additionalInputs`, must decode to
// `example` and be written back as `json` and `json5`.
struct TestCase {
  1: string name;
  // Unset for values thrift constants cannot express: NaN, the infinities and
  // negative zero, and floats whose generated Hack constant is rounded twice.
  2: optional Example example;
  // Expected basic JSON output.
  3: string json;
  // Expected JSON5 output.
  4: string json5;
  // Other spellings the decoder accepts for the same value.
  5: list<string> additionalInputs;
}

const list<TestCase> testCases = [
  TestCase{
    name = "Primitive",
    example = Example{
      boolValue = true,
      i64Value = 42,
      stringValue = "hello",
      binaryValue = "?~",
      floatValue = 0.10000000894069671630859375,
      doubleValue = 0.10000000000000001942890293094,
      enumValue = Enum.ONE,
    },
    json = "{
  \"i64Value\": 42,
  \"stringValue\": \"hello\",
  \"binaryValue\": {
    \"utf-8\": \"?~\"
  },
  \"floatValue\": 0.10000001,
  \"doubleValue\": 0.10000000000000002,
  \"enumValue\": \"ONE (1)\",
  \"boolValue\": true
}",
    json5 = "{
  i64Value: 42,
  stringValue: \"hello\",
  binaryValue: {
    \"utf-8\": \"?~\",
  },
  floatValue: 0.10000001,
  doubleValue: 0.10000000000000002,
  enumValue: \"ONE (1)\",
  boolValue: true,
}",
  },
  TestCase{
    name = "NegativeEnum",
    example = Example{enumValue = Enum.NEGATIVE_ONE},
    json = "{
  \"enumValue\": \"NEGATIVE_ONE (-1)\"
}",
    json5 = "{
  enumValue: \"NEGATIVE_ONE (-1)\",
}",
    additionalInputs = [
      "{\"enumValue\": \"NEGATIVE_ONE (-1)\"}",
      "{\"enumValue\": \"NEGATIVE_ONE\"}",
      "{\"enumValue\": \"(-1)\"}",
      "{\"enumValue\": \"-1\"}",
      "{\"enumValue\": -1}",
      "{\"enumValue\": -0x1}",
    ],
  },
  TestCase{
    name = "ListValue",
    example = Example{listValue = [Nested{nested = "item1"}]},
    json = "{
  \"listValue\": [
    {
      \"nested\": \"item1\"
    }
  ]
}",
    json5 = "{
  listValue: [
    {
      nested: \"item1\",
    },
  ],
}",
  },
  TestCase{
    name = "MultiElementList",
    example = Example{listValue = [Nested{nested = "a"}, Nested{nested = "b"}]},
    json = "{
  \"listValue\": [
    {
      \"nested\": \"a\"
    },
    {
      \"nested\": \"b\"
    }
  ]
}",
    json5 = "{
  listValue: [
    {
      nested: \"a\",
    },
    {
      nested: \"b\",
    },
  ],
}",
  },
  TestCase{
    name = "SetValue",
    example = Example{setValue = [Nested{nested = "item1"}]},
    json = "{
  \"setValue\": [
    {
      \"nested\": \"item1\"
    }
  ]
}",
    json5 = "{
  setValue: [
    {
      nested: \"item1\",
    },
  ],
}",
  },
  TestCase{
    name = "MultiElementSet",
    example = Example{
      setValue = [Nested{nested = "item2"}, Nested{nested = "item1"}],
    },
    json = "{
  \"setValue\": [
    {
      \"nested\": \"item1\"
    },
    {
      \"nested\": \"item2\"
    }
  ]
}",
    json5 = "{
  setValue: [
    {
      nested: \"item1\",
    },
    {
      nested: \"item2\",
    },
  ],
}",
  },
  TestCase{
    name = "BoolAsKey",
    example = Example{boolAsKey = {1: 1}},
    json = "{
  \"boolAsKey\": [
    {
      \"key\": true,
      \"value\": 1
    }
  ]
}",
    json5 = "{
  boolAsKey: [
    {
      key: true,
      value: 1,
    },
  ],
}",
  },
  TestCase{
    name = "I32AsKey",
    example = Example{i32AsKey = {1: 2}},
    json = "{
  \"i32AsKey\": [
    {
      \"key\": 1,
      \"value\": 2
    }
  ]
}",
    json5 = "{
  i32AsKey: [
    {
      key: 1,
      value: 2,
    },
  ],
}",
  },
  TestCase{
    name = "BinaryAsKey",
    example = Example{binaryAsKey = {"?~": 1}},
    json = "{
  \"binaryAsKey\": [
    {
      \"key\": {
        \"utf-8\": \"?~\"
      },
      \"value\": 1
    }
  ]
}",
    json5 = "{
  binaryAsKey: [
    {
      key: {
        \"utf-8\": \"?~\",
      },
      value: 1,
    },
  ],
}",
  },
  TestCase{
    name = "EnumAsKey",
    example = Example{enumAsKey = {1: 2}},
    json = "{
  \"enumAsKey\": {
    \"ONE (1)\": \"TWO (2)\"
  }
}",
    json5 = "{
  enumAsKey: {
    \"ONE (1)\": \"TWO (2)\",
  },
}",
    // Enum as map key should accept "enum-name (enum-value)", "enum-name" and
    // "(enum-value)"
    additionalInputs = [
      "{\"enumAsKey\": {\"ONE (1)\": \"TWO (2)\"}}",
      "{\"enumAsKey\": {\"ONE\": \"TWO\"}}",
      "{\"enumAsKey\": {\"(1)\": \"(2)\"}}",
      "{\"enumAsKey\": {\"1\": \"2\"}}",
      "{\"enumAsKey\": {\"0x1\": \"TWO (0x2)\"}}",
    ],
  },
  TestCase{
    name = "ListAsKey",
    example = Example{listAsKey = {[2, 1]: 3}},
    json = "{
  \"listAsKey\": [
    {
      \"key\": [
        2,
        1
      ],
      \"value\": 3
    }
  ]
}",
    json5 = "{
  listAsKey: [
    {
      key: [
        2,
        1,
      ],
      value: 3,
    },
  ],
}",
  },
  TestCase{
    name = "SetAsKey",
    example = Example{setAsKey = {[2, 1]: 3}},
    json = "{
  \"setAsKey\": [
    {
      \"key\": [
        1,
        2
      ],
      \"value\": 3
    }
  ]
}",
    json5 = "{
  setAsKey: [
    {
      key: [
        1,
        2,
      ],
      value: 3,
    },
  ],
}",
    additionalInputs = ["{\"setAsKey\": [{\"key\": [2, 1], \"value\": 3}]}"],
  },
  TestCase{
    name = "MapAsKey",
    example = Example{mapAsKey = {{1: 2}: 3}},
    json = "{
  \"mapAsKey\": [
    {
      \"key\": [
        {
          \"key\": 1,
          \"value\": 2
        }
      ],
      \"value\": 3
    }
  ]
}",
    json5 = "{
  mapAsKey: [
    {
      key: [
        {
          key: 1,
          value: 2,
        },
      ],
      value: 3,
    },
  ],
}",
  },
  TestCase{
    name = "NestedMapAsKey",
    example = Example{nestedMapAsKey = {{{1: 2}: 3}: 4}},
    json = "{
  \"nestedMapAsKey\": [
    {
      \"key\": [
        {
          \"key\": [
            {
              \"key\": 1,
              \"value\": 2
            }
          ],
          \"value\": 3
        }
      ],
      \"value\": 4
    }
  ]
}",
    json5 = "{
  nestedMapAsKey: [
    {
      key: [
        {
          key: [
            {
              key: 1,
              value: 2,
            },
          ],
          value: 3,
        },
      ],
      value: 4,
    },
  ],
}",
  },
  TestCase{
    name = "StructAsKey",
    example = Example{
      structAsKey = {
        Nested{nested = "key2"}: Nested{nested = "value2"},
        Nested{nested = "key1"}: Nested{nested = "value1"},
      },
    },
    json = "{
  \"structAsKey\": [
    {
      \"key\": {
        \"nested\": \"key1\"
      },
      \"value\": {
        \"nested\": \"value1\"
      }
    },
    {
      \"key\": {
        \"nested\": \"key2\"
      },
      \"value\": {
        \"nested\": \"value2\"
      }
    }
  ]
}",
    json5 = "{
  structAsKey: [
    {
      key: {
        nested: \"key1\",
      },
      value: {
        nested: \"value1\",
      },
    },
    {
      key: {
        nested: \"key2\",
      },
      value: {
        nested: \"value2\",
      },
    },
  ],
}",
  },
  TestCase{
    name = "BinaryBase64",
    example = Example{binaryValue = "\xde\xad\xbe\xef"},
    json = "{
  \"binaryValue\": {
    \"base64url\": \"3q2-7w\"
  }
}",
    json5 = "{
  binaryValue: {
    base64url: \"3q2-7w\",
  },
}",
  },
  TestCase{
    name = "BinaryUnprintableUtf8",
    example = Example{binaryValue = "hello\x01"},
    json = "{
  \"binaryValue\": {
    \"base64url\": \"aGVsbG8B\"
  }
}",
    json5 = "{
  binaryValue: {
    base64url: \"aGVsbG8B\",
  },
}",
  },
  TestCase{
    name = "BinaryUnprintableDel",
    example = Example{binaryValue = "a\x7fb"},
    json = "{
  \"binaryValue\": {
    \"base64url\": \"YX9i\"
  }
}",
    json5 = "{
  binaryValue: {
    base64url: \"YX9i\",
  },
}",
  },
  TestCase{
    name = "BinaryJsonEscapable",
    example = Example{binaryValue = "a\tb\nc"},
    json = "{
  \"binaryValue\": {
    \"utf-8\": \"a\\tb\\nc\"
  }
}",
    json5 = "{
  binaryValue: {
    \"utf-8\": \"a\\tb\\nc\",
  },
}",
  },
  TestCase{
    name = "OutOfOrderFields",
    example = Example{
      outOfOrderFieldsInSet = [
        OutOfOrderFields{Three = 0, One = 0, Two = 0},
        OutOfOrderFields{Three = 1, One = 1, Two = 1},
        OutOfOrderFields{Three = 0, One = 0, Two = 1},
        OutOfOrderFields{Three = 0, One = 1, Two = 0},
        OutOfOrderFields{Three = 1, One = 0, Two = 0},
        OutOfOrderFields{Three = 1, One = 1, Two = 0},
        OutOfOrderFields{Three = 1, One = 0, Two = 1},
        OutOfOrderFields{Three = 0, One = 1, Two = 1},
        OutOfOrderFields{Three = 0, One = 0, Two = 2},
      ],
    },
    json = "{
  \"outOfOrderFieldsInSet\": [
    {
      \"One\": 0,
      \"Two\": 0,
      \"Three\": 0
    },
    {
      \"One\": 0,
      \"Two\": 0,
      \"Three\": 1
    },
    {
      \"One\": 0,
      \"Two\": 1,
      \"Three\": 0
    },
    {
      \"One\": 0,
      \"Two\": 1,
      \"Three\": 1
    },
    {
      \"One\": 0,
      \"Two\": 2,
      \"Three\": 0
    },
    {
      \"One\": 1,
      \"Two\": 0,
      \"Three\": 0
    },
    {
      \"One\": 1,
      \"Two\": 0,
      \"Three\": 1
    },
    {
      \"One\": 1,
      \"Two\": 1,
      \"Three\": 0
    },
    {
      \"One\": 1,
      \"Two\": 1,
      \"Three\": 1
    }
  ]
}",
    json5 = "{
  outOfOrderFieldsInSet: [
    {
      One: 0,
      Two: 0,
      Three: 0,
    },
    {
      One: 0,
      Two: 0,
      Three: 1,
    },
    {
      One: 0,
      Two: 1,
      Three: 0,
    },
    {
      One: 0,
      Two: 1,
      Three: 1,
    },
    {
      One: 0,
      Two: 2,
      Three: 0,
    },
    {
      One: 1,
      Two: 0,
      Three: 0,
    },
    {
      One: 1,
      Two: 0,
      Three: 1,
    },
    {
      One: 1,
      Two: 1,
      Three: 0,
    },
    {
      One: 1,
      Two: 1,
      Three: 1,
    },
  ],
}",
  },
  // Exception fields are ordered by id too, so One decides here; by
  // declaration order, Three would.
  TestCase{
    name = "OutOfOrderFieldsExceptionInSet",
    example = Example{
      outOfOrderFieldsExceptionInSet = [
        OutOfOrderFieldsException{Three = 0, One = 1, Two = 0},
        OutOfOrderFieldsException{Three = 1, One = 0, Two = 0},
      ],
    },
    json = "{
  \"outOfOrderFieldsExceptionInSet\": [
    {
      \"One\": 0,
      \"Two\": 0,
      \"Three\": 1
    },
    {
      \"One\": 1,
      \"Two\": 0,
      \"Three\": 0
    }
  ]
}",
    json5 = "{
  outOfOrderFieldsExceptionInSet: [
    {
      One: 0,
      Two: 0,
      Three: 1,
    },
    {
      One: 1,
      Two: 0,
      Three: 0,
    },
  ],
}",
  },
  TestCase{
    name = "OutOfOrderFieldsInMap",
    example = Example{
      outOfOrderFieldsInMap = {
        OutOfOrderFields{Three = 0, One = 0, Two = 0}: 0,
        OutOfOrderFields{Three = 1, One = 1, Two = 1}: 1,
        OutOfOrderFields{Three = 0, One = 0, Two = 1}: 2,
        OutOfOrderFields{Three = 0, One = 1, Two = 0}: 3,
        OutOfOrderFields{Three = 1, One = 0, Two = 0}: 4,
        OutOfOrderFields{Three = 1, One = 1, Two = 0}: 5,
        OutOfOrderFields{Three = 1, One = 0, Two = 1}: 6,
        OutOfOrderFields{Three = 0, One = 1, Two = 1}: 7,
        OutOfOrderFields{Three = 0, One = 0, Two = 2}: 8,
      },
    },
    json = "{
  \"outOfOrderFieldsInMap\": [
    {
      \"key\": {
        \"One\": 0,
        \"Two\": 0,
        \"Three\": 0
      },
      \"value\": 0
    },
    {
      \"key\": {
        \"One\": 0,
        \"Two\": 0,
        \"Three\": 1
      },
      \"value\": 4
    },
    {
      \"key\": {
        \"One\": 0,
        \"Two\": 1,
        \"Three\": 0
      },
      \"value\": 2
    },
    {
      \"key\": {
        \"One\": 0,
        \"Two\": 1,
        \"Three\": 1
      },
      \"value\": 6
    },
    {
      \"key\": {
        \"One\": 0,
        \"Two\": 2,
        \"Three\": 0
      },
      \"value\": 8
    },
    {
      \"key\": {
        \"One\": 1,
        \"Two\": 0,
        \"Three\": 0
      },
      \"value\": 3
    },
    {
      \"key\": {
        \"One\": 1,
        \"Two\": 0,
        \"Three\": 1
      },
      \"value\": 5
    },
    {
      \"key\": {
        \"One\": 1,
        \"Two\": 1,
        \"Three\": 0
      },
      \"value\": 7
    },
    {
      \"key\": {
        \"One\": 1,
        \"Two\": 1,
        \"Three\": 1
      },
      \"value\": 1
    }
  ]
}",
    json5 = "{
  outOfOrderFieldsInMap: [
    {
      key: {
        One: 0,
        Two: 0,
        Three: 0,
      },
      value: 0,
    },
    {
      key: {
        One: 0,
        Two: 0,
        Three: 1,
      },
      value: 4,
    },
    {
      key: {
        One: 0,
        Two: 1,
        Three: 0,
      },
      value: 2,
    },
    {
      key: {
        One: 0,
        Two: 1,
        Three: 1,
      },
      value: 6,
    },
    {
      key: {
        One: 0,
        Two: 2,
        Three: 0,
      },
      value: 8,
    },
    {
      key: {
        One: 1,
        Two: 0,
        Three: 0,
      },
      value: 3,
    },
    {
      key: {
        One: 1,
        Two: 0,
        Three: 1,
      },
      value: 5,
    },
    {
      key: {
        One: 1,
        Two: 1,
        Three: 0,
      },
      value: 7,
    },
    {
      key: {
        One: 1,
        Two: 1,
        Three: 1,
      },
      value: 1,
    },
  ],
}",
  },
  // ── Small integer types (bool, byte, i16) ─────────────────────────────────
  TestCase{
    name = "SmallIntegers",
    example = Example{boolValue = false, byteValue = 42, i16Value = 1000},
    json = "{
  \"boolValue\": false,
  \"byteValue\": 42,
  \"i16Value\": 1000
}",
    json5 = "{
  boolValue: false,
  byteValue: 42,
  i16Value: 1000,
}",
  },
  // ── Large I64 boundary ──────────────────────────────────────────────────────
  TestCase{
    name = "I64Max",
    example = Example{i64Value = 9223372036854775807},
    json = "{
  \"i64Value\": 9223372036854775807
}",
    json5 = "{
  i64Value: 9223372036854775807,
}",
  },
  // ── Empty struct ────────────────────────────────────────────────────────────
  TestCase{
    name = "EmptyStruct",
    example = Example{},
    json = "{}",
    json5 = "{}",
    // A null member is skipped before its name or id is checked.
    additionalInputs = ["{\"boolValue (31)\": null, \"NonExist (30)\": null}"],
  },
  // ── Empty values ────────────────────────────────────────────────────────────
  TestCase{
    name = "EmptyValues",
    example = Example{stringValue = "", binaryValue = "", listValue = []},
    json = "{
  \"stringValue\": \"\",
  \"binaryValue\": {
    \"utf-8\": \"\"
  },
  \"listValue\": []
}",
    json5 = "{
  stringValue: \"\",
  binaryValue: {
    \"utf-8\": \"\",
  },
  listValue: [],
}",
  },
  // ── String as map key ───────────────────────────────────────────────────────
  TestCase{
    name = "StringAsKey",
    example = Example{stringAsKey = {"hello": "world"}},
    json = "{
  \"stringAsKey\": {
    \"hello\": \"world\"
  }
}",
    json5 = "{
  stringAsKey: {
    hello: \"world\",
  },
}",
    // String-keyed maps should accept both object form and array form.
    additionalInputs = [
      "{\"stringAsKey\": {\"hello\": \"world\"}}",
      "{\"stringAsKey\": [{\"key\": \"hello\", \"value\": \"world\"}]}",
    ],
  },
  TestCase{
    name = "MultiEntryStringAsKey",
    example = Example{stringAsKey = {"b": "2", "a": "1"}},
    json = "{
  \"stringAsKey\": {
    \"a\": \"1\",
    \"b\": \"2\"
  }
}",
    json5 = "{
  stringAsKey: {
    a: \"1\",
    b: \"2\",
  },
}",
  },
  // ── I64 as map key ──────────────────────────────────────────────────────────
  TestCase{
    name = "I64AsKey",
    example = Example{i64AsKey = {42: 1}},
    json = "{
  \"i64AsKey\": [
    {
      \"key\": 42,
      \"value\": 1
    }
  ]
}",
    json5 = "{
  i64AsKey: [
    {
      key: 42,
      value: 1,
    },
  ],
}",
    // I64-keyed maps should accept both array form and object form with string keys.
    additionalInputs = [
      "{\"i64AsKey\": [{\"key\": 42, \"value\": 1}]}",
      "{\"i64AsKey\": {\"42\": 1}}",
    ],
  },
  // ── Multiple entries in non-enum maps ───────────────────────────────────────
  TestCase{
    name = "MultiEntryI32AsKey",
    example = Example{i32AsKey = {3: 4, 1: 2}},
    json = "{
  \"i32AsKey\": [
    {
      \"key\": 1,
      \"value\": 2
    },
    {
      \"key\": 3,
      \"value\": 4
    }
  ]
}",
    json5 = "{
  i32AsKey: [
    {
      key: 1,
      value: 2,
    },
    {
      key: 3,
      value: 4,
    },
  ],
}",
    // Map should accept both object form {k: v} and array form [{"key": k, "value": v}]
    additionalInputs = [
      "{\"i32AsKey\": {\"1\": 2, \"3\": 4}}",
      "{\"i32AsKey\": {\"0x1\": 2, \"+3\": 4}}",
      "{\"i32AsKey\": [{\"key\": 1, \"value\": 2}, {\"key\": 3, \"value\": 4}]}",
    ],
  },
  // ── Set of integers ─────────────────────────────────────────────────────────
  TestCase{
    name = "I32Set",
    example = Example{i32SetValue = [3, 1, 2]},
    json = "{
  \"i32SetValue\": [
    1,
    2,
    3
  ]
}",
    json5 = "{
  i32SetValue: [
    1,
    2,
    3,
  ],
}",
  },
  // ── Union types ─────────────────────────────────────────────────────────────
  TestCase{
    name = "UnionInt",
    example = Example{unionValue = ExampleUnion{intValue = 42}},
    json = "{
  \"unionValue\": {
    \"intValue\": 42
  }
}",
    json5 = "{
  unionValue: {
    intValue: 42,
  },
}",
  },
  TestCase{
    name = "UnionString",
    example = Example{unionValue = ExampleUnion{stringValue = "hello"}},
    json = "{
  \"unionValue\": {
    \"stringValue\": \"hello\"
  }
}",
    json5 = "{
  unionValue: {
    stringValue: \"hello\",
  },
}",
  },
  // Floating-point values are written in their shortest round-trip form, in
  // fixed notation for decimal exponents in [-4, 16) and in scientific
  // notation otherwise. Whole numbers get ".0".
  TestCase{
    name = "DoubleFormatting",
    example = Example{
      doubleListValue = [
        0.0001,
        0.00001,
        1.5e-10,
        100000.0,
        1e15,
        1e16,
        1e20,
        123456789.123,
        0.30000000000000004,
        1.7976931348623157e308,
        5e-324,
        -2.5,
      ],
    },
    json = "{
  \"doubleListValue\": [
    0.0001,
    1e-05,
    1.5e-10,
    100000.0,
    1000000000000000.0,
    1e+16,
    1e+20,
    123456789.123,
    0.30000000000000004,
    1.7976931348623157e+308,
    5e-324,
    -2.5
  ]
}",
    json5 = "{
  doubleListValue: [
    0.0001,
    1e-05,
    1.5e-10,
    100000.0,
    1000000000000000.0,
    1e+16,
    1e+20,
    123456789.123,
    0.30000000000000004,
    1.7976931348623157e+308,
    5e-324,
    -2.5,
  ],
}",
  },
  // Float values use the shortest digits that round-trip in single precision.
  TestCase{
    name = "FloatFormatting",
    example = Example{
      floatListValue = [
        0.100000001490116119384765625,
        0.00000999999974737875163555145263671875,
        16777216.0,
        1234567.0,
        340282346638528859811704183484516925440.0,
        0.000000000000000000000000000000000000000000001401298464324817070923729583289916131280261941876515771757068283889791082685860601486638188362121582031250,
        -0.5,
      ],
    },
    json = "{
  \"floatListValue\": [
    0.1,
    1e-05,
    1.6777216e+07,
    1234567.0,
    3.4028235e+38,
    1e-45,
    -0.5
  ]
}",
    json5 = "{
  floatListValue: [
    0.1,
    1e-05,
    1.6777216e+07,
    1234567.0,
    3.4028235e+38,
    1e-45,
    -0.5,
  ],
}",
  },
  // Non-ASCII characters are written as raw UTF-8, never as escapes.
  TestCase{
    name = "NonBmpString",
    example = Example{stringValue = "😀 café"},
    json = "{
  \"stringValue\": \"😀 café\"
}",
    json5 = "{
  stringValue: \"😀 café\",
}",
  },
  TestCase{
    name = "FloatingPointKeys",
    example = Example{
      doubleAsKey = {-1.5: 1, 1e20: 2, 0.1: 3},
      floatAsKey = {16777216.0: 1},
    },
    json = "{
  \"doubleAsKey\": [
    {
      \"key\": -1.5,
      \"value\": 1
    },
    {
      \"key\": 0.1,
      \"value\": 3
    },
    {
      \"key\": 1e+20,
      \"value\": 2
    }
  ],
  \"floatAsKey\": [
    {
      \"key\": 1.6777216e+07,
      \"value\": 1
    }
  ]
}",
    json5 = "{
  doubleAsKey: [
    {
      key: -1.5,
      value: 1,
    },
    {
      key: 0.1,
      value: 3,
    },
    {
      key: 1e+20,
      value: 2,
    },
  ],
  floatAsKey: [
    {
      key: 1.6777216e+07,
      value: 1,
    },
  ],
}",
  },
  // More of the layout of the shortest digits: fixed notation for decimal
  // exponents in [-4, 16) for double and [-4, 7) for float.
  TestCase{
    name = "MoreDoubleFormatting",
    example = Example{doubleListValue = [1.0, 0.1, 0.10000000000000002, 0.3333333333333333, 9.999999999999999e-5, 9999999999999998.0, 9007199254740992.0, 9223372036854775808.0, 1e23, 1e100, 1e-100, -1.5e300, 2.2250738585072014e-308]},
    json = "{
  \"doubleListValue\": [
    1.0,
    0.1,
    0.10000000000000002,
    0.3333333333333333,
    9.999999999999999e-05,
    9999999999999998.0,
    9007199254740992.0,
    9.223372036854776e+18,
    1e+23,
    1e+100,
    1e-100,
    -1.5e+300,
    2.2250738585072014e-308
  ]
}",
    json5 = "{
  doubleListValue: [
    1.0,
    0.1,
    0.10000000000000002,
    0.3333333333333333,
    9.999999999999999e-05,
    9999999999999998.0,
    9007199254740992.0,
    9.223372036854776e+18,
    1e+23,
    1e+100,
    1e-100,
    -1.5e+300,
    2.2250738585072014e-308,
  ],
}",
  },
  TestCase{
    name = "MoreFloatFormatting",
    example = Example{floatListValue = [0.3333333432674407958984375, 4194304.0, 10000000.0, 16777215.0, 999999986991104.0, 10000000272564224.0, 0.000000000000000000000000000000000000011754943508222875079687365372222456778186655567720875215087517062784172594547271728515625, 0.0000000000000000000000000703853130814879132477466099505324860128273323223169199991389177739620208740234375]},
    json = "{
  \"floatListValue\": [
    0.33333334,
    4194304.0,
    1e+07,
    1.6777215e+07,
    1e+15,
    1e+16,
    1.1754944e-38,
    7.0385313e-26
  ]
}",
    json5 = "{
  floatListValue: [
    0.33333334,
    4194304.0,
    1e+07,
    1.6777215e+07,
    1e+15,
    1e+16,
    1.1754944e-38,
    7.0385313e-26,
  ],
}",
  },
  // Enum keys are ordered by value, not by name.
  TestCase{
    name = "EnumKeysInValueOrder",
    example = Example{enumAsKey = {Enum.MANY: Enum.ONE, Enum.TWO: Enum.ONE, Enum.NEGATIVE_ONE: Enum.ONE}},
    json = "{
  \"enumAsKey\": {
    \"NEGATIVE_ONE (-1)\": \"ONE (1)\",
    \"TWO (2)\": \"ONE (1)\",
    \"MANY (3)\": \"ONE (1)\"
  }
}",
    json5 = "{
  enumAsKey: {
    \"NEGATIVE_ONE (-1)\": \"ONE (1)\",
    \"TWO (2)\": \"ONE (1)\",
    \"MANY (3)\": \"ONE (1)\",
  },
}",
  },
  // List keys are ordered element by element, a prefix first.
  TestCase{
    name = "ListKeysLexicographic",
    example = Example{listAsKey = {[1, 3]: 1, [1, 2]: 2, [1]: 3, [2]: 4, []: 5}},
    json = "{
  \"listAsKey\": [
    {
      \"key\": [],
      \"value\": 5
    },
    {
      \"key\": [
        1
      ],
      \"value\": 3
    },
    {
      \"key\": [
        1,
        2
      ],
      \"value\": 2
    },
    {
      \"key\": [
        1,
        3
      ],
      \"value\": 1
    },
    {
      \"key\": [
        2
      ],
      \"value\": 4
    }
  ]
}",
    json5 = "{
  listAsKey: [
    {
      key: [],
      value: 5,
    },
    {
      key: [
        1,
      ],
      value: 3,
    },
    {
      key: [
        1,
        2,
      ],
      value: 2,
    },
    {
      key: [
        1,
        3,
      ],
      value: 1,
    },
    {
      key: [
        2,
      ],
      value: 4,
    },
  ],
}",
  },
  // Keys are ordered by their bytes, a prefix first, so "10" comes before "9"
  // and "é" last.
  TestCase{
    name = "StringKeysInByteOrder",
    example = Example{stringAsKey = {"é": "e", "ab": "ab", "a": "a", "B": "B", "9": "9", "10": "10"}},
    json = "{
  \"stringAsKey\": {
    \"10\": \"10\",
    \"9\": \"9\",
    \"B\": \"B\",
    \"a\": \"a\",
    \"ab\": \"ab\",
    \"é\": \"e\"
  }
}",
    json5 = "{
  stringAsKey: {
    \"10\": \"10\",
    \"9\": \"9\",
    B: \"B\",
    a: \"a\",
    ab: \"ab\",
    \"é\": \"e\",
  },
}",
  },
  // A C1 control is not printable, so the bytes are written as base64url; a
  // no-break space is.
  TestCase{
    name = "BinaryC1Control",
    example = Example{binaryValue = "\u0080"},
    json = "{
  \"binaryValue\": {
    \"base64url\": \"woA\"
  }
}",
    json5 = "{
  binaryValue: {
    base64url: \"woA\",
  },
}",
  },
  TestCase{
    name = "BinaryNoBreakSpace",
    example = Example{binaryValue = "\u00a0"},
    json = "{
  \"binaryValue\": {
    \"utf-8\": \"\u00a0\"
  }
}",
    json5 = "{
  binaryValue: {
    \"utf-8\": \"\u00a0\",
  },
}",
  },
  TestCase{
    name = "UnionMemberWithEmptyValue",
    example = Example{unionValue = ExampleUnion{stringValue = ""}},
    json = "{
  \"unionValue\": {
    \"stringValue\": \"\"
  }
}",
    json5 = "{
  unionValue: {
    stringValue: \"\",
  },
}",
  },
  TestCase{
    name = "BinaryLoneSurrogate",
    example = Example{binaryValue = "\xed\xa0\x80"},
    json = "{
  \"binaryValue\": {
    \"base64url\": \"7aCA\"
  }
}",
    json5 = "{
  binaryValue: {
    base64url: \"7aCA\",
  },
}",
  },
  TestCase{
    name = "BinaryOverlongEncoding",
    example = Example{binaryValue = "\xc0\x80"},
    json = "{
  \"binaryValue\": {
    \"base64url\": \"wIA\"
  }
}",
    json5 = "{
  binaryValue: {
    base64url: \"wIA\",
  },
}",
  },
  TestCase{
    name = "BinaryAboveMaxCodePoint",
    example = Example{binaryValue = "\xf4\x90\x80\x80"},
    json = "{
  \"binaryValue\": {
    \"base64url\": \"9JCAgA\"
  }
}",
    json5 = "{
  binaryValue: {
    base64url: \"9JCAgA\",
  },
}",
  },
  TestCase{
    name = "BinaryReplacementCharacter",
    example = Example{binaryValue = "\ufffd"},
    json = "{
  \"binaryValue\": {
    \"utf-8\": \"\ufffd\"
  }
}",
    json5 = "{
  binaryValue: {
    \"utf-8\": \"\ufffd\",
  },
}",
  },
  TestCase{
    name = "NumericStringKeys",
    example = Example{binaryAsKey = {"1": 1}, stringAsKey = {"1": "a", "-1": "b", "01": "c"}},
    json = "{
  \"binaryAsKey\": [
    {
      \"key\": {
        \"utf-8\": \"1\"
      },
      \"value\": 1
    }
  ],
  \"stringAsKey\": {
    \"-1\": \"b\",
    \"01\": \"c\",
    \"1\": \"a\"
  }
}",
    json5 = "{
  binaryAsKey: [
    {
      key: {
        \"utf-8\": \"1\",
      },
      value: 1,
    },
  ],
  stringAsKey: {
    \"-1\": \"b\",
    \"01\": \"c\",
    \"1\": \"a\",
  },
}",
  },
  TestCase{
    name = "UnqualifiedFieldsWithDefaultValues",
    example = Example{unqualifiedValue = Unqualified{}, unqualifiedSet = [Unqualified{enumValue = Enum.ONE}, Unqualified{}, Unqualified{enumValue = Enum.NEGATIVE_ONE}]},
    json = "{
  \"unqualifiedValue\": {
    \"enumValue\": \"DEFAULT (0)\",
    \"structValue\": {
      \"nested\": \"\"
    }
  },
  \"unqualifiedSet\": [
    {
      \"enumValue\": \"NEGATIVE_ONE (-1)\",
      \"structValue\": {
        \"nested\": \"\"
      }
    },
    {
      \"enumValue\": \"DEFAULT (0)\",
      \"structValue\": {
        \"nested\": \"\"
      }
    },
    {
      \"enumValue\": \"ONE (1)\",
      \"structValue\": {
        \"nested\": \"\"
      }
    }
  ]
}",
    json5 = "{
  unqualifiedValue: {
    enumValue: \"DEFAULT (0)\",
    structValue: {
      nested: \"\",
    },
  },
  unqualifiedSet: [
    {
      enumValue: \"NEGATIVE_ONE (-1)\",
      structValue: {
        nested: \"\",
      },
    },
    {
      enumValue: \"DEFAULT (0)\",
      structValue: {
        nested: \"\",
      },
    },
    {
      enumValue: \"ONE (1)\",
      structValue: {
        nested: \"\",
      },
    },
  ],
}",
  },
  // ── Bool Format Compatibility ────────────────────────────────────────────
  // Bools should accept both true/false bare literals and JSON strings.
  TestCase{
    name = "BoolTrue",
    example = Example{boolValue = true},
    json = "{
  \"boolValue\": true
}",
    json5 = "{
  boolValue: true,
}",
    additionalInputs = ["{\"boolValue\": true}", "{\"boolValue\": \"true\"}"],
  },
  TestCase{
    name = "BoolFalse",
    example = Example{boolValue = false},
    json = "{
  \"boolValue\": false
}",
    json5 = "{
  boolValue: false,
}",
    additionalInputs = ["{\"boolValue\": false}", "{\"boolValue\": \"false\"}"],
  },
  // ── Integer Format Compatibility ──────────────────────────────────────────
  // Integers should accept both JSON numbers and JSON strings, with the same
  // JSON5 integer syntax.
  TestCase{
    name = "Integer42",
    example = Example{i64Value = 42},
    json = "{
  \"i64Value\": 42
}",
    json5 = "{
  i64Value: 42,
}",
    additionalInputs = [
      "{\"i64Value\": 42}",
      "{\"i64Value\": \"42\"}",
      "{\"i64Value\": \"+42\"}",
      "{\"i64Value\": \"0x2A\"}",
      "{\"i64Value\": \"0x002a\"}",
    ],
  },
  TestCase{
    name = "IntegerNegative",
    example = Example{i64Value = -123},
    json = "{
  \"i64Value\": -123
}",
    json5 = "{
  i64Value: -123,
}",
    additionalInputs = ["{\"i64Value\": \"-123\"}", "{\"i64Value\": \"-0x7B\"}"],
  },
  // ── Float Format Compatibility ────────────────────────────────────────────
  // Floats should accept both JSON numbers and JSON strings.
  TestCase{
    name = "Float314",
    example = Example{floatValue = 3.14},
    json = "{
  \"floatValue\": 3.14
}",
    json5 = "{
  floatValue: 3.14,
}",
    additionalInputs = ["{\"floatValue\": 3.14}", "{\"floatValue\": \"3.14\"}"],
  },
  TestCase{
    name = "DoublePi",
    example = Example{doubleValue = 3.14159265358979},
    json = "{
  \"doubleValue\": 3.14159265358979
}",
    json5 = "{
  doubleValue: 3.14159265358979,
}",
    additionalInputs = [
      "{\"doubleValue\": 3.14159265358979}",
      "{\"doubleValue\": \"3.14159265358979\"}",
    ],
  },
  // A double literal just above the point halfway between the largest
  // subnormal and DBL_MIN rounds up to DBL_MIN.
  TestCase{
    name = "DoubleJustAboveSubnormalMidpoint",
    example = Example{doubleValue = 2.2250738585072014e-308},
    json = "{
  \"doubleValue\": 2.2250738585072014e-308
}",
    json5 = "{
  doubleValue: 2.2250738585072014e-308,
}",
    additionalInputs = [
      "{\"doubleValue\": 2.2250738585072012e-308}",
      "{\"doubleValue\": \"2.2250738585072012e-308\"}",
    ],
  },
  TestCase{
    name = "FloatFromInteger",
    example = Example{floatValue = 42.0},
    json = "{
  \"floatValue\": 42.0
}",
    json5 = "{
  floatValue: 42.0,
}",
    additionalInputs = ["{\"floatValue\": 42}", "{\"floatValue\": \"42\"}"],
  },
  // A float literal is rounded to binary32 directly. Each literal is within
  // 1e-30 of a point halfway between two binary32 values, so rounding it to
  // binary64 first gives that point, which rounds to the wrong neighbour.
  TestCase{
    name = "FloatNearBinary32Midpoint",
    example = Example{
      floatListValue = [1.00000011920928955078125, 1.00000011920928955078125],
    },
    json = "{
  \"floatListValue\": [
    1.0000001,
    1.0000001
  ]
}",
    json5 = "{
  floatListValue: [
    1.0000001,
    1.0000001,
  ],
}",
    additionalInputs = [
      "{\"floatListValue\": [1.000000059604644775390625000001, 1.000000178813934326171874999999]}",
      "{\"floatListValue\": [\"1.000000059604644775390625000001\", \"1.000000178813934326171874999999\"]}",
    ],
  },
  TestCase{
    name = "FloatNegativeNearBinary32Midpoint",
    example = Example{
      floatListValue = [-1.00000011920928955078125, -1.00000011920928955078125],
    },
    json = "{
  \"floatListValue\": [
    -1.0000001,
    -1.0000001
  ]
}",
    json5 = "{
  floatListValue: [
    -1.0000001,
    -1.0000001,
  ],
}",
    additionalInputs = [
      "{\"floatListValue\": [-1.000000059604644775390625000001, -1.000000178813934326171874999999]}",
      "{\"floatListValue\": [\"-1.000000059604644775390625000001\", \"-1.000000178813934326171874999999\"]}",
    ],
  },
  // ── Enum Format Compatibility ─────────────────────────────────────────────
  // Enum should accept "enum-name (enum-value)", "enum-name", "(enum-value)"
  // and raw integer.
  TestCase{
    name = "EnumOne",
    example = Example{enumValue = 1},
    json = "{
  \"enumValue\": \"ONE (1)\"
}",
    json5 = "{
  enumValue: \"ONE (1)\",
}",
    additionalInputs = [
      "{\"enumValue\": \"ONE (1)\"}",
      "{\"enumValue\": \"ONE(1)\"}",
      "{\"enumValue\": \"ONE \\t (1)\"}",
      "{\"enumValue\": \"ONE\"}",
      "{\"enumValue\": \"(1)\"}",
      "{\"enumValue\": \"1\"}",
      "{\"enumValue\": 1}",
      "{\"enumValue\": 0x1}",
      "{\"enumValue\": \"ONE (0x1)\"}",
      "{\"enumValue\": \"(+1)\"}",
      "{\"enumValue\": \"0x1\"}",
      "{\"enumValue\": \"+1\"}",
    ],
  },
  TestCase{
    name = "EnumTwo",
    example = Example{enumValue = 2},
    json = "{
  \"enumValue\": \"TWO (2)\"
}",
    json5 = "{
  enumValue: \"TWO (2)\",
}",
    additionalInputs = [
      "{\"enumValue\": \"TWO (2)\"}",
      "{\"enumValue\": \"(2)\"}",
      "{\"enumValue\": 0x2}",
    ],
  },
  TestCase{
    name = "EnumUnregistered",
    example = Example{enumValue = -5},
    json = "{
  \"enumValue\": \"(-5)\"
}",
    json5 = "{
  enumValue: \"(-5)\",
}",
    additionalInputs = [
      "{\"enumValue\": \"(-5)\"}",
      "{\"enumValue\": \"SomeName (-5)\"}",
      "{\"enumValue\": -5}",
    ],
  },
  TestCase{
    name = "EnumDefault",
    example = Example{enumValue = 0},
    json = "{
  \"enumValue\": \"DEFAULT (0)\"
}",
    json5 = "{
  enumValue: \"DEFAULT (0)\",
}",
    additionalInputs = [
      "{\"enumValue\": \"DEFAULT (0)\"}",
      "{\"enumValue\": \"(0)\"}",
      "{\"enumValue\": 0x0}",
    ],
  },
  // ── Binary Encoding Compatibility ─────────────────────────────────────────
  // Binary should accept utf-8, base64, and base64url object forms.
  TestCase{
    name = "Binary",
    example = Example{binaryValue = "~_~!"},
    json = "{
  \"binaryValue\": {
    \"utf-8\": \"~_~!\"
  }
}",
    json5 = "{
  binaryValue: {
    \"utf-8\": \"~_~!\",
  },
}",
    additionalInputs = [
      "{binaryValue: {base64url: \"fl9-IQ==\"}}",
      "{binaryValue: {base64url: \"fl9-IQ\"}}",
      "{binaryValue: {base64: \"fl9+IQ==\"}}",
      "{binaryValue: {base64: \"fl9+IQ\"}}",
      "{binaryValue: \"fl9-IQ==\"}",
      "{binaryValue: \"fl9-IQ\"}",
      "{binaryValue: \"fl9+IQ==\"}",
      "{binaryValue: \"fl9+IQ\"}",
      "{binaryValue: {\"utf-8\": \"~_~!\"}}",
    ],
  },
  TestCase{
    name = "BinaryAsKeyAlphabets",
    example = Example{binaryAsKey = {"~_~!": 1}},
    json = "{
  \"binaryAsKey\": [
    {
      \"key\": {
        \"utf-8\": \"~_~!\"
      },
      \"value\": 1
    }
  ]
}",
    json5 = "{
  binaryAsKey: [
    {
      key: {
        \"utf-8\": \"~_~!\",
      },
      value: 1,
    },
  ],
}",
    additionalInputs = [
      "{binaryAsKey: {\"fl9-IQ\": 1}}",
      "{binaryAsKey: {\"fl9+IQ\": 1}}",
      "{binaryAsKey: {\"fl9+IQ==\": 1}}",
    ],
  },
  TestCase{
    name = "BinaryAsKeyBase64",
    example = Example{binaryAsKey = {"\xff": 1}},
    json = "{
  \"binaryAsKey\": [
    {
      \"key\": {
        \"base64url\": \"_w\"
      },
      \"value\": 1
    }
  ]
}",
    json5 = "{
  binaryAsKey: [
    {
      key: {
        base64url: \"_w\",
      },
      value: 1,
    },
  ],
}",
  },
  // ── Bool as Map Key Compatibility ─────────────────────────────────────────
  // Bool keys in maps are encoded as strings "true" or "false"
  TestCase{
    name = "BoolAsKeyTrue",
    example = Example{boolAsKey = {1: 42}},
    json = "{
  \"boolAsKey\": [
    {
      \"key\": true,
      \"value\": 42
    }
  ]
}",
    json5 = "{
  boolAsKey: [
    {
      key: true,
      value: 42,
    },
  ],
}",
    additionalInputs = ["{\"boolAsKey\": {\"true\": 42}}"],
  },
  TestCase{
    name = "BoolAsKeyFalse",
    example = Example{boolAsKey = {0: 42}},
    json = "{
  \"boolAsKey\": [
    {
      \"key\": false,
      \"value\": 42
    }
  ]
}",
    json5 = "{
  boolAsKey: [
    {
      key: false,
      value: 42,
    },
  ],
}",
    additionalInputs = ["{\"boolAsKey\": {\"false\": 42}}"],
  },
  TestCase{
    name = "BoolAsKeyBoth",
    example = Example{boolAsKey = {1: 1, 0: 0}},
    json = "{
  \"boolAsKey\": [
    {
      \"key\": false,
      \"value\": 0
    },
    {
      \"key\": true,
      \"value\": 1
    }
  ]
}",
    json5 = "{
  boolAsKey: [
    {
      key: false,
      value: 0,
    },
    {
      key: true,
      value: 1,
    },
  ],
}",
    additionalInputs = ["{\"boolAsKey\": {\"true\": 1, \"false\": 0}}"],
  },
  // ── Field Identifier Format Compatibility ────────────────────────────────────
  // Accepts: "name", "name (id)", "(id)", "id".
  TestCase{
    name = "FieldIdentifierFormats",
    example = Example{boolValue = true},
    json = "{
  \"boolValue\": true
}",
    json5 = "{
  boolValue: true,
}",
    additionalInputs = [
      "{\"boolValue\": true}",
      "{\"boolValue (30)\": true}",
      "{\"(30)\": true}",
      "{\"30\": true}",
      "{\"boolValue (0x1E)\": true}",
      "{\"(0x1E)\": true}",
      "{\"+30\": true}",
    ],
  },
  // ── Unknown Field Skipping Compatibility ────────────────────────────────────
  // Unknown fields should be silently skipped during deserialization.
  TestCase{
    name = "SkipUnknownFields",
    example = Example{i64Value = 42},
    json = "{
  \"i64Value\": 42
}",
    json5 = "{
  i64Value: 42,
}",
    additionalInputs = [
      "{\"i64Value\": 42, \"unknownStr\": \"hello\"}",
      "{\"i64Value\": 42, \"unknownStr (123)\": \"hello\"}",
      "{\"unknownNum\": 123, \"i64Value\": 42}",
      "{\"unknownBool\": true, \"i64Value\": 42}",
      "{\"unknownNull\": null, \"i64Value\": 42}",
      "{\"unknownObj\": {\"a\": 1, \"b\": [2, 3]}, \"i64Value\": 42}",
      "{\"unknownArr\": [1, \"two\", true], \"i64Value\": 42}",
      "{\"x\": 1, \"y\": \"s\", \"z\": [1], \"w\": {\"a\": 1}, \"i64Value\": 42}",
    ],
  },
  // ── String Line Continuation Compatibility ──────────────────────────────────
  // A backslash followed by a literal newline (LF, CR, or CRLF) is a line
  // continuation: the backslash and newline are stripped from the result.
  TestCase{
    name = "StringLineContinuation",
    example = Example{stringValue = "hello, world"},
    json = "{
  \"stringValue\": \"hello, world\"
}",
    json5 = "{
  stringValue: \"hello, world\",
}",
    additionalInputs = [
      "{\"stringValue\": \"hello, world\"}",
      "{\"stringValue\": \"hello, \\\nworld\"}",
      "{\"stringValue\": \"hello, \\\rworld\"}",
      "{\"stringValue\": \"hello, \\\r\nworld\"}",
    ],
  },
  // ── Null Field Handling ──────────────────────────────────────────────────────
  // Null field values should be treated as absent (field not set).
  TestCase{
    name = "NullFieldsSkipped",
    example = Example{boolValue = true},
    json = "{
  \"boolValue\": true
}",
    json5 = "{
  boolValue: true,
}",
    additionalInputs = [
      "{\"i64Value\": null, \"boolValue\": true, \"stringValue\": null}",
    ],
  },
  // ── Single-Quoted String Compatibility ──────────────────────────────────────
  TestCase{
    name = "SingleQuotedString",
    example = Example{stringValue = "hello"},
    json = "{
  \"stringValue\": \"hello\"
}",
    json5 = "{
  stringValue: \"hello\",
}",
    additionalInputs = ["{\"stringValue\": 'hello'}", "{'stringValue': 'hello'}"],
  },
  // ── JSON5 Comments Compatibility ────────────────────────────────────────────
  TestCase{
    name = "Json5Comments",
    example = Example{i64Value = 42},
    json = "{
  \"i64Value\": 42
}",
    json5 = "{
  i64Value: 42,
}",
    additionalInputs = [
      "{/* comment */\"i64Value\": 42}",
      "{\"i64Value\": /* inline */ 42}",
      "{\"i64Value\": 42 // line comment\n}",
    ],
  },
  // ── Double from Integer Compatibility ───────────────────────────────────────
  TestCase{
    name = "DoubleFromInteger",
    example = Example{doubleValue = 42.0},
    json = "{
  \"doubleValue\": 42.0
}",
    json5 = "{
  doubleValue: 42.0,
}",
    additionalInputs = ["{\"doubleValue\": 42}", "{\"doubleValue\": \"42\"}"],
  },
  TestCase{
    name = "DoubleFromLargestExactIntegerBelow2Pow63",
    example = Example{doubleValue = 9223372036854774784.0},
    json = "{
  \"doubleValue\": 9.223372036854775e+18
}",
    json5 = "{
  doubleValue: 9.223372036854775e+18,
}",
    additionalInputs = ["{\"doubleValue\": 9223372036854774784}"],
  },
  TestCase{
    name = "DoubleFromInt64Min",
    example = Example{doubleValue = -9223372036854775808.0},
    json = "{
  \"doubleValue\": -9.223372036854776e+18
}",
    json5 = "{
  doubleValue: -9.223372036854776e+18,
}",
    additionalInputs = ["{\"doubleValue\": -9223372036854775808}"],
  },
  TestCase{
    name = "DoubleFromLargeDecimalOrExponent",
    example = Example{doubleValue = 100000000000000000000.0},
    json = "{
  \"doubleValue\": 1e+20
}",
    json5 = "{
  doubleValue: 1e+20,
}",
    additionalInputs = [
      "{\"doubleValue\": 1e20}",
      "{\"doubleValue\": \"1e20\"}",
      "{\"doubleValue\": 100000000000000000000.0}",
      "{\"doubleValue\": \"100000000000000000000.0\"}",
    ],
  },
  // ── Union Compatibility ─────────────────────────────────────────────────────
  TestCase{
    name = "UnionEmpty",
    example = Example{unionValue = ExampleUnion{}},
    json = "{
  \"unionValue\": {}
}",
    json5 = "{
  unionValue: {},
}",
    additionalInputs = ["{\"unionValue\": {}}"],
  },
  // A repeated union field is reset, as in Binary and Compact: an empty object
  // clears it and a known field replaces it.
  TestCase{
    name = "RepeatedUnionFieldEmptyObjectClears",
    example = Example{unionValue = ExampleUnion{}},
    json = "{
  \"unionValue\": {}
}",
    json5 = "{
  unionValue: {},
}",
    additionalInputs = [
      "{\"unionValue\": {\"intValue\": 1}, \"unionValue\": {}}",
      "{\"unionValue\": {\"intValue\": 1}, \"unionValue\": {\"intValue\": null}}",
    ],
  },
  TestCase{
    name = "RepeatedUnionFieldOtherMemberReplaces",
    example = Example{
      unionValue = ExampleUnion{intValue = 2},
    },
    json = "{
  \"unionValue\": {
    \"intValue\": 2
  }
}",
    json5 = "{
  unionValue: {
    intValue: 2,
  },
}",
    additionalInputs = [
      "{\"unionValue\": {\"stringValue\": \"a\"}, \"unionValue\": {\"intValue\": 2}}",
    ],
  },
  TestCase{
    name = "RepeatedUnionFieldSameMemberReplaces",
    example = Example{
      unionValue = ExampleUnion{nestedValue = Nested{}},
    },
    json = "{
  \"unionValue\": {
    \"nestedValue\": {
      \"nested\": \"\"
    }
  }
}",
    json5 = "{
  unionValue: {
    nestedValue: {
      nested: \"\",
    },
  },
}",
    additionalInputs = [
      "{\"unionValue\": {\"nestedValue\": {\"nested\": \"a\"}}, \"unionValue\": {\"nestedValue\": {}}}",
    ],
  },
  // ── Strings ─────────────────────────────────────────────────────────────
  TestCase{
    name = "SurrogatePairEscape",
    example = Example{stringValue = "😀"},
    json = "{
  \"stringValue\": \"😀\"
}",
    json5 = "{
  stringValue: \"😀\",
}",
    additionalInputs = [
      "{\"stringValue\": \"\\ud83d\\ude00\"}",
      "{\"stringValue\": \"\\uD83D\\uDE00\"}",
    ],
  },
  // ── Floating-Point Keys ─────────────────────────────────────────────────
  // Keys are decoded with the key type's rules, so integers must convert
  // exactly.
  TestCase{
    name = "DoubleAsKeyFromInteger",
    example = Example{doubleAsKey = {9007199254740992.0: 1}},
    json = "{
  \"doubleAsKey\": [
    {
      \"key\": 9007199254740992.0,
      \"value\": 1
    }
  ]
}",
    json5 = "{
  doubleAsKey: [
    {
      key: 9007199254740992.0,
      value: 1,
    },
  ],
}",
    additionalInputs = [
      "{\"doubleAsKey\": {\"9007199254740992\": 1}}",
      "{\"doubleAsKey\": {\"0x20000000000000\": 1}}",
      "{\"doubleAsKey\": [{\"key\": 9007199254740992, \"value\": 1}]}",
    ],
  },
  TestCase{
    name = "DoubleAsKeyFromDecimal",
    example = Example{doubleAsKey = {1.5: 1, 1e20: 2}},
    json = "{
  \"doubleAsKey\": [
    {
      \"key\": 1.5,
      \"value\": 1
    },
    {
      \"key\": 1e+20,
      \"value\": 2
    }
  ]
}",
    json5 = "{
  doubleAsKey: [
    {
      key: 1.5,
      value: 1,
    },
    {
      key: 1e+20,
      value: 2,
    },
  ],
}",
    additionalInputs = [
      "{\"doubleAsKey\": {\"1.5\": 1, \"1e20\": 2}}",
      "{\"doubleAsKey\": [{\"key\": 1.5, \"value\": 1}, {\"key\": \"1e20\", \"value\": 2}]}",
    ],
  },
  TestCase{
    name = "FloatAsKeyFromInteger",
    example = Example{floatAsKey = {16777216.0: 1}},
    json = "{
  \"floatAsKey\": [
    {
      \"key\": 1.6777216e+07,
      \"value\": 1
    }
  ]
}",
    json5 = "{
  floatAsKey: [
    {
      key: 1.6777216e+07,
      value: 1,
    },
  ],
}",
    additionalInputs = ["{\"floatAsKey\": {\"16777216\": 1}}"],
  },
  TestCase{
    name = "IntegerNegativeZero",
    example = Example{i64Value = 0},
    json = "{
  \"i64Value\": 0
}",
    json5 = "{
  i64Value: 0,
}",
    additionalInputs = [
      "{\"i64Value\": -0}",
      "{\"i64Value\": \"-0\"}",
      "{\"i64Value\": -0x0}",
    ],
  },
  TestCase{
    name = "CommentAfterValue",
    example = Example{i64Value = 1},
    json = "{
  \"i64Value\": 1
}",
    json5 = "{
  i64Value: 1,
}",
    additionalInputs = [
      "{\"i64Value\": 1} // note",
      "{\"i64Value\": 1} /* note */",
    ],
  },
  // Field id -32768 stands for "no id".
  TestCase{
    name = "FieldIdInt16MinMeansNoId",
    example = Example{boolValue = true},
    json = "{
  \"boolValue\": true
}",
    json5 = "{
  boolValue: true,
}",
    additionalInputs = ["{\"boolValue (-32768)\": true}"],
  },
  TestCase{
    name = "UnionWithOnlyUnknownField",
    example = Example{unionValue = ExampleUnion{}},
    json = "{
  \"unionValue\": {}
}",
    json5 = "{
  unionValue: {},
}",
    additionalInputs = ["{\"unionValue\": {\"unknown\": 1}}"],
  },
  TestCase{
    name = "FloatAsKeyFromString",
    example = Example{floatAsKey = {0.5: 1, 16777216.0: 2}},
    json = "{
  \"floatAsKey\": [
    {
      \"key\": 0.5,
      \"value\": 1
    },
    {
      \"key\": 1.6777216e+07,
      \"value\": 2
    }
  ]
}",
    json5 = "{
  floatAsKey: [
    {
      key: 0.5,
      value: 1,
    },
    {
      key: 1.6777216e+07,
      value: 2,
    },
  ],
}",
    additionalInputs = [
      "{\"floatAsKey\": [{\"key\": 0.5, \"value\": 1}, {\"key\": \"16777216\", \"value\": 2}]}",
    ],
  },
  // Bare base64 strings: padding is optional, and nonzero trailing bits ("QR")
  // are ignored.
  TestCase{
    name = "BinaryBareBase64OneByte",
    example = Example{binaryValue = "A"},
    json = "{
  \"binaryValue\": {
    \"utf-8\": \"A\"
  }
}",
    json5 = "{
  binaryValue: {
    \"utf-8\": \"A\",
  },
}",
    additionalInputs = [
      "{\"binaryValue\": \"QQ\"}",
      "{\"binaryValue\": \"QQ==\"}",
      "{\"binaryValue\": \"QR\"}",
    ],
  },
  TestCase{
    name = "BinaryBareBase64TwoBytes",
    example = Example{binaryValue = "AB"},
    json = "{
  \"binaryValue\": {
    \"utf-8\": \"AB\"
  }
}",
    json5 = "{
  binaryValue: {
    \"utf-8\": \"AB\",
  },
}",
    additionalInputs = ["{\"binaryValue\": \"QUI\"}", "{\"binaryValue\": \"QUI=\"}"],
  },
  TestCase{
    name = "BinaryBareBase64ThreeBytes",
    example = Example{binaryValue = "ABC"},
    json = "{
  \"binaryValue\": {
    \"utf-8\": \"ABC\"
  }
}",
    json5 = "{
  binaryValue: {
    \"utf-8\": \"ABC\",
  },
}",
    additionalInputs = ["{\"binaryValue\": \"QUJD\"}"],
  },
  TestCase{
    name = "BinaryBareBase64Empty",
    example = Example{binaryValue = ""},
    json = "{
  \"binaryValue\": {
    \"utf-8\": \"\"
  }
}",
    json5 = "{
  binaryValue: {
    \"utf-8\": \"\",
  },
}",
    additionalInputs = ["{\"binaryValue\": \"\"}"],
  },
  // 1 + 2^-24 is exactly halfway between two binary32 values.
  TestCase{
    name = "FloatBinary32MidpointTiesToEven",
    example = Example{floatValue = 1.0},
    json = "{
  \"floatValue\": 1.0
}",
    json5 = "{
  floatValue: 1.0,
}",
    additionalInputs = ["{\"floatValue\": 1.000000059604644775390625}"],
  },
  // Above a binary32 midpoint whose exact decimal has 83 significant digits,
  // but below it rounded to 41 digits.
  TestCase{
    name = "FloatJustAboveLongBinary32Midpoint",
    example = Example{
      floatValue = 0.0000000000000000000000000703853130814879132477466099505324860128273323223169199991389177739620208740234375,
    },
    json = "{
  \"floatValue\": 7.0385313e-26
}",
    json5 = "{
  floatValue: 7.0385313e-26,
}",
    additionalInputs = ["{\"floatValue\": 7.03853100000000022281692450609677778769437e-26}"],
  },
  // Below the same midpoint. Unset: the generated Hack constant, the shortest
  // digits, reads as binary64 exactly at the midpoint, which then rounds to
  // the wrong binary32 neighbour.
  TestCase{
    name = "FloatJustBelowLongBinary32Midpoint",
    json = "{
  \"floatListValue\": [
    7.038531e-26,
    -7.038531e-26
  ]
}",
    json5 = "{
  floatListValue: [
    7.038531e-26,
    -7.038531e-26,
  ],
}",
    additionalInputs = [
      "{\"floatListValue\": [7.03853100000000022281692450609677778769436e-26, -7.03853100000000022281692450609677778769436e-26]}",
    ],
  },
  TestCase{
    name = "FloatJustBelowMidpointAfterFltMax",
    example = Example{
      floatValue = 340282346638528859811704183484516925440.0,
    },
    json = "{
  \"floatValue\": 3.4028235e+38
}",
    json5 = "{
  floatValue: 3.4028235e+38,
}",
    additionalInputs = ["{\"floatValue\": 340282356779733661637539395458142568447.999999}"],
  },
  TestCase{
    name = "FloatJustAboveHalfSmallestSubnormal",
    example = Example{
      floatValue = 0.000000000000000000000000000000000000000000001401298464324817070923729583289916131280261941876515771757068283889791082685860601486638188362121582031250,
    },
    json = "{
  \"floatValue\": 1e-45
}",
    json5 = "{
  floatValue: 1e-45,
}",
    additionalInputs = ["{\"floatValue\": 7.00649232162408535461864791644958065640130970938257885878534141944895541342930300743319094181060791015625000001e-46}"],
  },
  // Exactly halfway between the largest subnormal and DBL_MIN.
  TestCase{
    name = "DoubleSubnormalMidpointTiesToEven",
    example = Example{doubleValue = 2.2250738585072014e-308},
    json = "{
  \"doubleValue\": 2.2250738585072014e-308
}",
    json5 = "{
  doubleValue: 2.2250738585072014e-308,
}",
    additionalInputs = ["{\"doubleValue\": 2.22507385850720113605740979670913197593481954635164564802342610972482222202107694551652952390813508791414915891303962110687008643869459464552765720740782062174337998814106326732925355228688137214901298112245145188984905722230728525513315575501591439747639798341180199932396254828901710708185069063066665599493827577257201576306269066333264756530000924588831643303777979186961204949739037782970490505108060994073026293712895895000358379996720725430436028407889577179615094551674824347103070260914462157228988025818254518032570701886087211312807951223342628836862232150377566662250398253433597456888442390026549819838548794829220689472168983109969836584681402285424333066033985088644580400103493397042756718644338377048603786162277173854562306587467901408672332763671875e-308}"],
  },
  TestCase{
    name = "DoubleJustBelowSubnormalMidpoint",
    example = Example{doubleValue = 2.225073858507201e-308},
    json = "{
  \"doubleValue\": 2.225073858507201e-308
}",
    json5 = "{
  doubleValue: 2.225073858507201e-308,
}",
    additionalInputs = [
      "{\"doubleValue\": 2.225073858507201136057409796709131975934819546351645648e-308}",
    ],
  },
  // ── NaN, Infinity and Negative Zero ────────────────────────────────────────
  TestCase{
    name = "Infinity",
    json = "{
  \"infValue\": \"Infinity\"
}",
    json5 = "{
  infValue: Infinity,
}",
    additionalInputs = [
      "{\"infValue\": Infinity}",
      "{\"infValue\": +Infinity}",
      "{\"infValue\": \"Infinity\"}",
      "{\"infValue\": \"+Infinity\"}",
    ],
  },
  TestCase{
    name = "NegativeInfinity",
    json = "{
  \"infValue\": \"-Infinity\"
}",
    json5 = "{
  infValue: -Infinity,
}",
    additionalInputs = [
      "{\"infValue\": -Infinity}",
      "{\"infValue\": \"-Infinity\"}",
    ],
  },
  TestCase{
    name = "NaN",
    json = "{
  \"nanValue\": \"NaN\"
}",
    json5 = "{
  nanValue: NaN,
}",
    additionalInputs = [
      "{\"nanValue\": NaN}",
      "{\"nanValue\": +NaN}",
      "{\"nanValue\": \"NaN\"}",
    ],
  },
  TestCase{
    name = "NegativeNaN",
    json = "{
  \"nanValue\": \"-NaN\"
}",
    json5 = "{
  nanValue: -NaN,
}",
    additionalInputs = ["{\"nanValue\": -NaN}", "{\"nanValue\": \"-NaN\"}"],
  },
  TestCase{
    name = "FloatSpecialValues",
    json = "{
  \"floatListValue\": [
    \"NaN\",
    \"-NaN\",
    \"Infinity\",
    \"-Infinity\"
  ]
}",
    json5 = "{
  floatListValue: [
    NaN,
    -NaN,
    Infinity,
    -Infinity,
  ],
}",
    additionalInputs = [
      "{\"floatListValue\": [NaN, -NaN, Infinity, -Infinity]}",
      "{\"floatListValue\": [\"NaN\", \"-NaN\", \"Infinity\", \"-Infinity\"]}",
    ],
  },
  // SimpleJSON writes -0.0 as `-0`.
  TestCase{
    name = "NegativeZero",
    json = "{
  \"floatValue\": -0.0,
  \"doubleValue\": -0.0
}",
    json5 = "{
  floatValue: -0.0,
  doubleValue: -0.0,
}",
    additionalInputs = [
      "{\"floatValue\": -0.0, \"doubleValue\": -0.0}",
      "{\"floatValue\": -0, \"doubleValue\": -0}",
      "{\"floatValue\": \"-0\", \"doubleValue\": \"-0\"}",
      "{\"floatValue\": -0x0, \"doubleValue\": -0x0}",
      "{\"floatValue\": \"-0x0\", \"doubleValue\": \"-0x0\"}",
      "{\"floatValue\": \"-0.0\", \"doubleValue\": \"-0.0\"}",
    ],
  },
  TestCase{
    name = "PositiveZero",
    json = "{
  \"floatValue\": 0.0,
  \"doubleValue\": 0.0
}",
    json5 = "{
  floatValue: 0.0,
  doubleValue: 0.0,
}",
    additionalInputs = [
      "{\"floatValue\": 0, \"doubleValue\": 0}",
      "{\"floatValue\": \"0\", \"doubleValue\": \"0\"}",
      "{\"floatValue\": +0, \"doubleValue\": +0}",
      "{\"floatValue\": 0.0, \"doubleValue\": 0.0}",
    ],
  },
  TestCase{
    name = "NegativeZeroKey",
    json = "{
  \"doubleAsKey\": [
    {
      \"key\": -0.0,
      \"value\": 1
    }
  ],
  \"floatAsKey\": [
    {
      \"key\": -0.0,
      \"value\": 2
    }
  ]
}",
    json5 = "{
  doubleAsKey: [
    {
      key: -0.0,
      value: 1,
    },
  ],
  floatAsKey: [
    {
      key: -0.0,
      value: 2,
    },
  ],
}",
    additionalInputs = [
      "{\"doubleAsKey\": {\"-0\": 1}, \"floatAsKey\": {\"-0\": 2}}",
      "{\"doubleAsKey\": {\"-0x0\": 1}, \"floatAsKey\": {\"-0x0\": 2}}",
      "{\"doubleAsKey\": [{\"key\": -0, \"value\": 1}], \"floatAsKey\": [{\"key\": -0, \"value\": 2}]}",
    ],
  },
  TestCase{
    name = "SignsOfZeroAndNaNInDoubleList",
    json = "{
  \"doubleListValue\": [
    -0.0,
    \"-NaN\",
    0.0,
    \"NaN\"
  ]
}",
    json5 = "{
  doubleListValue: [
    -0.0,
    -NaN,
    0.0,
    NaN,
  ],
}",
    additionalInputs = [
      "{\"doubleListValue\": [-0.0, -NaN, 0.0, NaN]}",
      "{\"doubleListValue\": [-0, \"-NaN\", 0, \"NaN\"]}",
      "{doubleListValue: [\"-0x0\", -NaN, \"0.0\", +NaN]}",
    ],
  },
  TestCase{
    name = "FloatNegativeNaN",
    json = "{
  \"floatValue\": \"-NaN\"
}",
    json5 = "{
  floatValue: -NaN,
}",
    additionalInputs = ["{\"floatValue\": -NaN}", "{\"floatValue\": \"-NaN\"}"],
  },
  TestCase{
    name = "FloatOverflowsToInfinity",
    json = "{
  \"floatValue\": \"Infinity\"
}",
    json5 = "{
  floatValue: Infinity,
}",
    additionalInputs = [
      "{\"floatValue\": 1e39}",
      "{\"floatValue\": 340282356779733661637539395458142568448.000001}",
    ],
  },
];

// Not final, so the test can derive a class from it.
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.virtual": "1"}}
struct NonFinalStruct {
  1: i64 value;
}
