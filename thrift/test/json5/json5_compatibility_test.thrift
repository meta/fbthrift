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

// Backward compatibility test cases for JSON decoder.
// The JSON5 decoder should accept multiple representations for the same value
// to maintain compatibility with different serialization outputs.
// Test data is defined here so it can be shared between different languages.

include "thrift/test/json5/json5_test.thrift"

package "facebook.com/thrift/json5"

struct CompatibilityTestCase {
  1: string name;
  2: list<string> inputs;
  3: json5_test.Example output;
}

// @lint-ignore-every THRIFTFORMAT
const list<CompatibilityTestCase> compatibilityTestCases = [
  // ── Bool Format Compatibility ────────────────────────────────────────────
  // Bools should accept both true/false bare literals and JSON strings.
  CompatibilityTestCase{
    name = "BoolTrue",
    inputs = ["{\"boolValue\": true}", "{\"boolValue\": \"true\"}"],
    output = json5_test.Example{boolValue = true},
  },
  CompatibilityTestCase{
    name = "BoolFalse",
    inputs = ["{\"boolValue\": false}", "{\"boolValue\": \"false\"}"],
    output = json5_test.Example{boolValue = false},
  },
  // ── Map Format Compatibility ──────────────────────────────────────────────
  // Map should accept both object form {k: v} and array form [{"key": k, "value": v}]
  CompatibilityTestCase{
    name = "Map",
    inputs = [
      "{\"i32AsKey\": {\"1\": 2, \"3\": 4}}",
      "{\"i32AsKey\": {\"0x1\": 2, \"+3\": 4}}",
      "{\"i32AsKey\": [{\"key\": 1, \"value\": 2}, {\"key\": 3, \"value\": 4}]}",
    ],
    output = json5_test.Example{i32AsKey = {1: 2, 3: 4}},
  },
  // ── Integer Format Compatibility ──────────────────────────────────────────
  // Integers should accept both JSON numbers and JSON strings, with the same
  // JSON5 integer syntax.
  CompatibilityTestCase{
    name = "Integer42",
    inputs = [
      "{\"i64Value\": 42}",
      "{\"i64Value\": \"42\"}",
      "{\"i64Value\": \"+42\"}",
      "{\"i64Value\": \"0x2A\"}",
      "{\"i64Value\": \"0x002a\"}",
    ],
    output = json5_test.Example{i64Value = 42},
  },
  CompatibilityTestCase{
    name = "IntegerNegative",
    inputs = ["{\"i64Value\": \"-123\"}", "{\"i64Value\": \"-0x7B\"}"],
    output = json5_test.Example{i64Value = -123},
  },
  // ── Float Format Compatibility ────────────────────────────────────────────
  // Floats should accept both JSON numbers and JSON strings.
  CompatibilityTestCase{
    name = "Float314",
    inputs = ["{\"floatValue\": 3.14}", "{\"floatValue\": \"3.14\"}"],
    output = json5_test.Example{floatValue = 3.14},
  },
  CompatibilityTestCase{
    name = "DoublePi",
    inputs = [
      "{\"doubleValue\": 3.14159265358979}",
      "{\"doubleValue\": \"3.14159265358979\"}",
    ],
    output = json5_test.Example{doubleValue = 3.14159265358979},
  },
  CompatibilityTestCase{
    name = "FloatFromInteger",
    inputs = ["{\"floatValue\": 42}", "{\"floatValue\": \"42\"}"],
    output = json5_test.Example{floatValue = 42.0},
  },
  // ── Enum Format Compatibility ─────────────────────────────────────────────
  // Enum should accept "enum-name (enum-value)", "enum-name", "(enum-value)"
  // and raw integer.
  CompatibilityTestCase{
    name = "EnumOne",
    inputs = [
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
    output = json5_test.Example{enumValue = 1},
  },
  CompatibilityTestCase{
    name = "EnumTwo",
    inputs = [
      "{\"enumValue\": \"TWO (2)\"}",
      "{\"enumValue\": \"(2)\"}",
      "{\"enumValue\": 0x2}",
    ],
    output = json5_test.Example{enumValue = 2},
  },
  CompatibilityTestCase{
    name = "EnumNegativeOne",
    inputs = [
      "{\"enumValue\": \"NEGATIVE_ONE (-1)\"}",
      "{\"enumValue\": \"NEGATIVE_ONE\"}",
      "{\"enumValue\": \"(-1)\"}",
      "{\"enumValue\": \"-1\"}",
      "{\"enumValue\": -1}",
      "{\"enumValue\": -0x1}",
    ],
    output = json5_test.Example{enumValue = -1},
  },
  CompatibilityTestCase{
    name = "EnumUnregistered",
    inputs = [
      "{\"enumValue\": \"(-5)\"}",
      "{\"enumValue\": \"SomeName (-5)\"}",
      "{\"enumValue\": -5}",
    ],
    output = json5_test.Example{enumValue = -5},
  },
  CompatibilityTestCase{
    name = "EnumDefault",
    inputs = [
      "{\"enumValue\": \"DEFAULT (0)\"}",
      "{\"enumValue\": \"(0)\"}",
      "{\"enumValue\": 0x0}",
    ],
    output = json5_test.Example{enumValue = 0},
  },
  // ── Enum as Map Key Compatibility ─────────────────────────────────────────
  // Enum as map key should accept "enum-name (enum-value)", "enum-name" and
  // "(enum-value)"
  CompatibilityTestCase{
    name = "EnumAsKey",
    inputs = [
      "{\"enumAsKey\": {\"ONE (1)\": \"TWO (2)\"}}",
      "{\"enumAsKey\": {\"ONE\": \"TWO\"}}",
      "{\"enumAsKey\": {\"(1)\": \"(2)\"}}",
      "{\"enumAsKey\": {\"1\": \"2\"}}",
      "{\"enumAsKey\": {\"0x1\": \"TWO (0x2)\"}}",
    ],
    output = json5_test.Example{enumAsKey = {1: 2}},
  },
  // ── Binary Encoding Compatibility ─────────────────────────────────────────
  // Binary should accept utf-8, base64, and base64url object forms.
  CompatibilityTestCase{
    name = "Binary",
    inputs = [
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
    output = json5_test.Example{binaryValue = "~_~!"},
  },
  CompatibilityTestCase{
    name = "BinaryAsKeyAlphabets",
    inputs = [
      "{binaryAsKey: {\"fl9-IQ\": 1}}",
      "{binaryAsKey: {\"fl9+IQ\": 1}}",
      "{binaryAsKey: {\"fl9+IQ==\": 1}}",
    ],
    output = json5_test.Example{binaryAsKey = {"~_~!": 1}},
  },
  // ── Bool as Map Key Compatibility ─────────────────────────────────────────
  // Bool keys in maps are encoded as strings "true" or "false"
  CompatibilityTestCase{
    name = "BoolAsKeyTrue",
    inputs = ["{\"boolAsKey\": {\"true\": 42}}"],
    output = json5_test.Example{boolAsKey = {1: 42}},
  },
  CompatibilityTestCase{
    name = "BoolAsKeyFalse",
    inputs = ["{\"boolAsKey\": {\"false\": 42}}"],
    output = json5_test.Example{boolAsKey = {0: 42}},
  },
  CompatibilityTestCase{
    name = "BoolAsKeyBoth",
    inputs = ["{\"boolAsKey\": {\"true\": 1, \"false\": 0}}"],
    output = json5_test.Example{boolAsKey = {1: 1, 0: 0}},
  },
  // ── Field Identifier Format Compatibility ────────────────────────────────────
  // Accepts: "name", "name (id)", "(id)", "id".
  CompatibilityTestCase{
    name = "FieldIdentifierFormats",
    inputs = [
      "{\"boolValue\": true}",
      "{\"boolValue (30)\": true}",
      "{\"(30)\": true}",
      "{\"30\": true}",
      "{\"boolValue (0x1E)\": true}",
      "{\"(0x1E)\": true}",
      "{\"+30\": true}",
    ],
    output = json5_test.Example{boolValue = true},
  },
  // ── Unknown Field Skipping Compatibility ────────────────────────────────────
  // Unknown fields should be silently skipped during deserialization.
  CompatibilityTestCase{
    name = "SkipUnknownFields",
    inputs = [
      "{\"i64Value\": 42, \"unknownStr\": \"hello\"}",
      "{\"i64Value\": 42, \"unknownStr (123)\": \"hello\"}",
      "{\"unknownNum\": 123, \"i64Value\": 42}",
      "{\"unknownBool\": true, \"i64Value\": 42}",
      "{\"unknownNull\": null, \"i64Value\": 42}",
      "{\"unknownObj\": {\"a\": 1, \"b\": [2, 3]}, \"i64Value\": 42}",
      "{\"unknownArr\": [1, \"two\", true], \"i64Value\": 42}",
      "{\"x\": 1, \"y\": \"s\", \"z\": [1], \"w\": {\"a\": 1}, \"i64Value\": 42}",
    ],
    output = json5_test.Example{i64Value = 42},
  },
  // ── String as Map Key Compatibility ────────────────────────────────────────
  // String-keyed maps should accept both object form and array form.
  CompatibilityTestCase{
    name = "StringAsKeyObjectForm",
    inputs = [
      "{\"stringAsKey\": {\"hello\": \"world\"}}",
      "{\"stringAsKey\": [{\"key\": \"hello\", \"value\": \"world\"}]}",
    ],
    output = json5_test.Example{stringAsKey = {"hello": "world"}},
  },
  // ── String Line Continuation Compatibility ──────────────────────────────────
  // A backslash followed by a literal newline (LF, CR, or CRLF) is a line
  // continuation: the backslash and newline are stripped from the result.
  CompatibilityTestCase{
    name = "StringLineContinuation",
    inputs = [
      "{\"stringValue\": \"hello, world\"}",
      "{\"stringValue\": \"hello, \\\nworld\"}",
      "{\"stringValue\": \"hello, \\\rworld\"}",
      "{\"stringValue\": \"hello, \\\r\nworld\"}",
    ],
    output = json5_test.Example{stringValue = "hello, world"},
  },
  // ── I64 as Map Key Compatibility ─────────────────────────────────────────────
  // I64-keyed maps should accept both array form and object form with string keys.
  CompatibilityTestCase{
    name = "I64AsKeyArrayForm",
    inputs = [
      "{\"i64AsKey\": [{\"key\": 42, \"value\": 1}]}",
      "{\"i64AsKey\": {\"42\": 1}}",
    ],
    output = json5_test.Example{i64AsKey = {42: 1}},
  },
  // ── Null Field Handling ──────────────────────────────────────────────────────
  // Null field values should be treated as absent (field not set).
  CompatibilityTestCase{
    name = "NullFieldsSkipped",
    inputs = [
      "{\"i64Value\": null, \"boolValue\": true, \"stringValue\": null}",
    ],
    output = json5_test.Example{boolValue = true},
  },
  // ── Single-Quoted String Compatibility ──────────────────────────────────────
  CompatibilityTestCase{
    name = "SingleQuotedString",
    inputs = ["{\"stringValue\": 'hello'}", "{'stringValue': 'hello'}"],
    output = json5_test.Example{stringValue = "hello"},
  },
  // ── JSON5 Comments Compatibility ────────────────────────────────────────────
  CompatibilityTestCase{
    name = "Json5Comments",
    inputs = [
      "{/* comment */\"i64Value\": 42}",
      "{\"i64Value\": /* inline */ 42}",
      "{\"i64Value\": 42 // line comment\n}",
    ],
    output = json5_test.Example{i64Value = 42},
  },
  // ── Double from Integer Compatibility ───────────────────────────────────────
  CompatibilityTestCase{
    name = "DoubleFromInteger",
    inputs = ["{\"doubleValue\": 42}", "{\"doubleValue\": \"42\"}"],
    output = json5_test.Example{doubleValue = 42.0},
  },
  CompatibilityTestCase{
    name = "DoubleFromLargestExactIntegerBelow2Pow63",
    inputs = ["{\"doubleValue\": 9223372036854774784}"],
    output = json5_test.Example{doubleValue = 9223372036854774784.0},
  },
  CompatibilityTestCase{
    name = "DoubleFromInt64Min",
    inputs = ["{\"doubleValue\": -9223372036854775808}"],
    output = json5_test.Example{doubleValue = -9223372036854775808.0},
  },
  CompatibilityTestCase{
    name = "DoubleFromLargeDecimalOrExponent",
    inputs = [
      "{\"doubleValue\": 1e20}",
      "{\"doubleValue\": \"1e20\"}",
      "{\"doubleValue\": 100000000000000000000.0}",
      "{\"doubleValue\": \"100000000000000000000.0\"}",
    ],
    output = json5_test.Example{doubleValue = 100000000000000000000.0},
  },
  // ── Union Compatibility ─────────────────────────────────────────────────────
  CompatibilityTestCase{
    name = "UnionEmpty",
    inputs = ["{\"unionValue\": {}}"],
    output = json5_test.Example{unionValue = json5_test.ExampleUnion{}},
  },
  // A repeated union field is reset, as in Binary and Compact: an empty object
  // clears it and a known field replaces it.
  CompatibilityTestCase{
    name = "RepeatedUnionFieldEmptyObjectClears",
    inputs = [
      "{\"unionValue\": {\"intValue\": 1}, \"unionValue\": {}}",
      "{\"unionValue\": {\"intValue\": 1}, \"unionValue\": {\"intValue\": null}}",
    ],
    output = json5_test.Example{unionValue = json5_test.ExampleUnion{}},
  },
  CompatibilityTestCase{
    name = "RepeatedUnionFieldOtherMemberReplaces",
    inputs = [
      "{\"unionValue\": {\"stringValue\": \"a\"}, \"unionValue\": {\"intValue\": 2}}",
    ],
    output = json5_test.Example{
      unionValue = json5_test.ExampleUnion{intValue = 2},
    },
  },
  CompatibilityTestCase{
    name = "RepeatedUnionFieldSameMemberReplaces",
    inputs = [
      "{\"unionValue\": {\"nestedValue\": {\"nested\": \"a\"}}, \"unionValue\": {\"nestedValue\": {}}}",
    ],
    output = json5_test.Example{
      unionValue = json5_test.ExampleUnion{nestedValue = json5_test.Nested{}},
    },
  },
  // ── Strings ─────────────────────────────────────────────────────────────
  CompatibilityTestCase{
    name = "SurrogatePairEscape",
    inputs = [
      "{\"stringValue\": \"\\ud83d\\ude00\"}",
      "{\"stringValue\": \"\\uD83D\\uDE00\"}",
    ],
    output = json5_test.Example{stringValue = "😀"},
  },
  // ── Floating-Point Keys ─────────────────────────────────────────────────
  // Keys are decoded with the key type's rules, so integers must convert
  // exactly.
  CompatibilityTestCase{
    name = "DoubleAsKeyFromInteger",
    inputs = [
      "{\"doubleAsKey\": {\"9007199254740992\": 1}}",
      "{\"doubleAsKey\": {\"0x20000000000000\": 1}}",
      "{\"doubleAsKey\": [{\"key\": 9007199254740992, \"value\": 1}]}",
    ],
    output = json5_test.Example{doubleAsKey = {9007199254740992.0: 1}},
  },
  CompatibilityTestCase{
    name = "DoubleAsKeyFromDecimal",
    inputs = [
      "{\"doubleAsKey\": {\"1.5\": 1, \"1e20\": 2}}",
      "{\"doubleAsKey\": [{\"key\": 1.5, \"value\": 1}, {\"key\": \"1e20\", \"value\": 2}]}",
    ],
    output = json5_test.Example{doubleAsKey = {1.5: 1, 1e20: 2}},
  },
  CompatibilityTestCase{
    name = "FloatAsKeyFromInteger",
    inputs = ["{\"floatAsKey\": {\"16777216\": 1}}"],
    output = json5_test.Example{floatAsKey = {16777216.0: 1}},
  },
  CompatibilityTestCase{
    name = "IntegerNegativeZero",
    inputs = [
      "{\"i64Value\": -0}",
      "{\"i64Value\": \"-0\"}",
      "{\"i64Value\": -0x0}",
    ],
    output = json5_test.Example{i64Value = 0},
  },
  CompatibilityTestCase{
    name = "CommentAfterValue",
    inputs = [
      "{\"i64Value\": 1} // note",
      "{\"i64Value\": 1} /* note */",
    ],
    output = json5_test.Example{i64Value = 1},
  },
  // A null member is skipped before its name or id is checked.
  CompatibilityTestCase{
    name = "NullMembersAreNotResolved",
    inputs = ["{\"boolValue (31)\": null, \"NonExist (30)\": null}"],
    output = json5_test.Example{},
  },
  // Field id -32768 stands for "no id".
  CompatibilityTestCase{
    name = "FieldIdInt16MinMeansNoId",
    inputs = ["{\"boolValue (-32768)\": true}"],
    output = json5_test.Example{boolValue = true},
  },
  CompatibilityTestCase{
    name = "UnionWithOnlyUnknownField",
    inputs = ["{\"unionValue\": {\"unknown\": 1}}"],
    output = json5_test.Example{unionValue = json5_test.ExampleUnion{}},
  },
  CompatibilityTestCase{
    name = "SetDuplicateElements",
    inputs = [
      "{\"i32SetValue\": [2, 1, 2], \"setValue\": [{\"nested\": \"b\"}, {\"nested\": \"a\"}, {\"nested\": \"b\"}]}",
    ],
    output = json5_test.Example{
      i32SetValue = [1, 2],
      setValue = [json5_test.Nested{nested = "a"}, json5_test.Nested{nested = "b"}],
    },
  },
  CompatibilityTestCase{
    name = "MapDuplicateKeyLastWins",
    inputs = [
      "{\"i32AsKey\": [{\"key\": 1, \"value\": 2}, {\"key\": 1, \"value\": 3}]}",
      "{\"i32AsKey\": {\"1\": 2, \"0x1\": 3}}",
    ],
    output = json5_test.Example{i32AsKey = {1: 3}},
  },
  CompatibilityTestCase{
    name = "StructKeyDuplicateLastWins",
    inputs = [
      "{\"structAsKey\": [{\"key\": {\"nested\": \"k\"}, \"value\": {\"nested\": \"v\"}}, {\"key\": {\"nested\": \"k\"}, \"value\": {\"nested\": \"w\"}}]}",
    ],
    output = json5_test.Example{
      structAsKey = {json5_test.Nested{nested = "k"}: json5_test.Nested{nested = "w"}},
    },
  },
  CompatibilityTestCase{
    name = "SetAsKeyDuplicateElements",
    inputs = ["{\"setAsKey\": [{\"key\": [2, 1, 2], \"value\": 3}]}"],
    output = json5_test.Example{setAsKey = {[1, 2]: 3}},
  },
  CompatibilityTestCase{
    name = "FloatAsKeyFromString",
    inputs = [
      "{\"floatAsKey\": [{\"key\": 0.5, \"value\": 1}, {\"key\": \"16777216\", \"value\": 2}]}",
    ],
    output = json5_test.Example{floatAsKey = {0.5: 1, 16777216.0: 2}},
  },
  // Bare base64 strings: padding is optional, and nonzero trailing bits ("QR")
  // are ignored.
  CompatibilityTestCase{
    name = "BinaryBareBase64OneByte",
    inputs = [
      "{\"binaryValue\": \"QQ\"}",
      "{\"binaryValue\": \"QQ==\"}",
      "{\"binaryValue\": \"QR\"}",
    ],
    output = json5_test.Example{binaryValue = "A"},
  },
  CompatibilityTestCase{
    name = "BinaryBareBase64TwoBytes",
    inputs = ["{\"binaryValue\": \"QUI\"}", "{\"binaryValue\": \"QUI=\"}"],
    output = json5_test.Example{binaryValue = "AB"},
  },
  CompatibilityTestCase{
    name = "BinaryBareBase64ThreeBytes",
    inputs = ["{\"binaryValue\": \"QUJD\"}"],
    output = json5_test.Example{binaryValue = "ABC"},
  },
  CompatibilityTestCase{
    name = "BinaryBareBase64Empty",
    inputs = ["{\"binaryValue\": \"\"}"],
    output = json5_test.Example{binaryValue = ""},
  },
  // 1 + 2^-24 is exactly halfway between two binary32 values.
  CompatibilityTestCase{
    name = "FloatBinary32MidpointTiesToEven",
    inputs = ["{\"floatValue\": 1.000000059604644775390625}"],
    output = json5_test.Example{floatValue = 1.0},
  },
  // Above a binary32 midpoint whose exact decimal has 83 significant digits,
  // but below it rounded to 41 digits.
  CompatibilityTestCase{
    name = "FloatJustAboveLongBinary32Midpoint",
    inputs = ["{\"floatValue\": 7.03853100000000022281692450609677778769437e-26}"],
    output = json5_test.Example{
      floatValue = 0.0000000000000000000000000703853130814879132477466099505324860128273323223169199991389177739620208740234375,
    },
  },
  CompatibilityTestCase{
    name = "FloatJustBelowMidpointAfterFltMax",
    inputs = ["{\"floatValue\": 340282356779733661637539395458142568447.999999}"],
    output = json5_test.Example{
      floatValue = 340282346638528859811704183484516925440.0,
    },
  },
  CompatibilityTestCase{
    name = "FloatJustAboveHalfSmallestSubnormal",
    inputs = ["{\"floatValue\": 7.00649232162408535461864791644958065640130970938257885878534141944895541342930300743319094181060791015625000001e-46}"],
    output = json5_test.Example{
      floatValue = 0.000000000000000000000000000000000000000000001401298464324817070923729583289916131280261941876515771757068283889791082685860601486638188362121582031250,
    },
  },
  // Exactly halfway between the largest subnormal and DBL_MIN.
  CompatibilityTestCase{
    name = "DoubleSubnormalMidpointTiesToEven",
    inputs = ["{\"doubleValue\": 2.22507385850720113605740979670913197593481954635164564802342610972482222202107694551652952390813508791414915891303962110687008643869459464552765720740782062174337998814106326732925355228688137214901298112245145188984905722230728525513315575501591439747639798341180199932396254828901710708185069063066665599493827577257201576306269066333264756530000924588831643303777979186961204949739037782970490505108060994073026293712895895000358379996720725430436028407889577179615094551674824347103070260914462157228988025818254518032570701886087211312807951223342628836862232150377566662250398253433597456888442390026549819838548794829220689472168983109969836584681402285424333066033985088644580400103493397042756718644338377048603786162277173854562306587467901408672332763671875e-308}"],
    output = json5_test.Example{doubleValue = 2.2250738585072014e-308},
  },
  CompatibilityTestCase{
    name = "DoubleJustBelowSubnormalMidpoint",
    inputs = [
      "{\"doubleValue\": 2.225073858507201136057409796709131975934819546351645648e-308}",
    ],
    output = json5_test.Example{doubleValue = 2.225073858507201e-308},
  },
];

// Values that thrift constants cannot express: NaN, the infinities and
// negative zero. Each input must decode to a value that is written as `json`
// and as `json5`.
struct RoundTripTestCase {
  1: string name;
  2: list<string> inputs;
  3: string json;
  4: string json5;
}

// @lint-ignore-every THRIFTFORMAT
const list<RoundTripTestCase> roundTripTestCases = [
  RoundTripTestCase{
    name = "Infinity",
    inputs = [
      "{\"infValue\": Infinity}",
      "{\"infValue\": +Infinity}",
      "{\"infValue\": \"Infinity\"}",
      "{\"infValue\": \"+Infinity\"}",
    ],
    json = "{
  \"infValue\": \"Infinity\"
}",
    json5 = "{
  infValue: Infinity,
}",
  },
  RoundTripTestCase{
    name = "NegativeInfinity",
    inputs = [
      "{\"infValue\": -Infinity}",
      "{\"infValue\": \"-Infinity\"}",
    ],
    json = "{
  \"infValue\": \"-Infinity\"
}",
    json5 = "{
  infValue: -Infinity,
}",
  },
  RoundTripTestCase{
    name = "NaN",
    inputs = [
      "{\"nanValue\": NaN}",
      "{\"nanValue\": +NaN}",
      "{\"nanValue\": \"NaN\"}",
    ],
    json = "{
  \"nanValue\": \"NaN\"
}",
    json5 = "{
  nanValue: NaN,
}",
  },
  RoundTripTestCase{
    name = "NegativeNaN",
    inputs = ["{\"nanValue\": -NaN}", "{\"nanValue\": \"-NaN\"}"],
    json = "{
  \"nanValue\": \"-NaN\"
}",
    json5 = "{
  nanValue: -NaN,
}",
  },
  RoundTripTestCase{
    name = "FloatSpecialValues",
    inputs = [
      "{\"floatListValue\": [NaN, -NaN, Infinity, -Infinity]}",
      "{\"floatListValue\": [\"NaN\", \"-NaN\", \"Infinity\", \"-Infinity\"]}",
    ],
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
  },
  // SimpleJSON writes -0.0 as `-0`.
  RoundTripTestCase{
    name = "NegativeZero",
    inputs = [
      "{\"floatValue\": -0.0, \"doubleValue\": -0.0}",
      "{\"floatValue\": -0, \"doubleValue\": -0}",
      "{\"floatValue\": \"-0\", \"doubleValue\": \"-0\"}",
      "{\"floatValue\": -0x0, \"doubleValue\": -0x0}",
      "{\"floatValue\": \"-0x0\", \"doubleValue\": \"-0x0\"}",
      "{\"floatValue\": \"-0.0\", \"doubleValue\": \"-0.0\"}",
    ],
    json = "{
  \"floatValue\": -0.0,
  \"doubleValue\": -0.0
}",
    json5 = "{
  floatValue: -0.0,
  doubleValue: -0.0,
}",
  },
  RoundTripTestCase{
    name = "PositiveZero",
    inputs = [
      "{\"floatValue\": 0, \"doubleValue\": 0}",
      "{\"floatValue\": \"0\", \"doubleValue\": \"0\"}",
      "{\"floatValue\": +0, \"doubleValue\": +0}",
      "{\"floatValue\": 0.0, \"doubleValue\": 0.0}",
    ],
    json = "{
  \"floatValue\": 0.0,
  \"doubleValue\": 0.0
}",
    json5 = "{
  floatValue: 0.0,
  doubleValue: 0.0,
}",
  },
  RoundTripTestCase{
    name = "NegativeZeroKey",
    inputs = [
      "{\"doubleAsKey\": {\"-0\": 1}, \"floatAsKey\": {\"-0\": 2}}",
      "{\"doubleAsKey\": {\"-0x0\": 1}, \"floatAsKey\": {\"-0x0\": 2}}",
      "{\"doubleAsKey\": [{\"key\": -0, \"value\": 1}], \"floatAsKey\": [{\"key\": -0, \"value\": 2}]}",
    ],
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
  },
  RoundTripTestCase{
    name = "SignsOfZeroAndNaNInDoubleList",
    inputs = [
      "{\"doubleListValue\": [-0.0, -NaN, 0.0, NaN]}",
      "{\"doubleListValue\": [-0, \"-NaN\", 0, \"NaN\"]}",
      "{doubleListValue: [\"-0x0\", -NaN, \"0.0\", +NaN]}",
    ],
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
  },
  RoundTripTestCase{
    name = "FloatNegativeNaN",
    inputs = ["{\"floatValue\": -NaN}", "{\"floatValue\": \"-NaN\"}"],
    json = "{
  \"floatValue\": \"-NaN\"
}",
    json5 = "{
  floatValue: -NaN,
}",
  },
  RoundTripTestCase{
    name = "FloatOverflowsToInfinity",
    inputs = [
      "{\"floatValue\": 1e39}",
      "{\"floatValue\": 340282356779733661637539395458142568448.000001}",
    ],
    json = "{
  \"floatValue\": \"Infinity\"
}",
    json5 = "{
  floatValue: Infinity,
}",
  },
];
