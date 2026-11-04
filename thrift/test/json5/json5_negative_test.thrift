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

// Negative test cases for JSON decoder validation.
// The JSON5 decoder should reject these malformed or type-mismatched inputs.
// Test data is defined here so it can be shared between different languages.

package "facebook.com/thrift/json5"

struct NegativeTestCase {
  1: string name;
  2: string json;
}

// @lint-ignore-every THRIFTFORMAT
const list<NegativeTestCase> enumValidationNegativeCases = [
  NegativeTestCase{
    name = "NameValueMismatch",
    json = "{\"enumValue\": \"ONE (2)\"}",
  },
  NegativeTestCase{
    name = "NameValueMismatchNegative",
    json = "{\"enumValue\": \"ONE (-1)\"}",
  },
  NegativeTestCase{
    name = "InvalidIdentifier",
    json = "{\"enumValue\": \"$ONE\"}",
  },
  NegativeTestCase{
    name = "InvalidName",
    json = "{\"enumValue\": \"INVALID_ENUM\"}",
  },
  NegativeTestCase{
    name = "InvalidNameWithValue",
    json = "{\"enumValue\": \"INVALID_ENUM (1)\"}",
  },
  NegativeTestCase{
    name = "ExtraWhitespace",
    json = "{\"enumValue\": \"ONE \"}",
  },
  NegativeTestCase{name = "ExtraWhitespace2", json = "{\"enumValue\": \"1 \"}"},
  NegativeTestCase{
    name = "MissingCloseParen",
    json = "{\"enumValue\": \"ONE (1\"}",
  },
  NegativeTestCase{
    name = "EmptyParentheses",
    json = "{\"enumValue\": \"ONE ()\"}",
  },
  NegativeTestCase{
    name = "ReversedNameValue",
    json = "{\"enumValue\": \"1 (ONE)\"}",
  },
  NegativeTestCase{name = "OnlyOpenParen", json = "{\"enumValue\": \"ONE (\"}"},
  NegativeTestCase{
    name = "OnlyCloseParen",
    json = "{\"enumValue\": \"ONE )\"}",
  },
  NegativeTestCase{
    name = "ParenNotAtEnd",
    json = "{\"enumValue\": \"ONE (1) extra\"}",
  },
  NegativeTestCase{
    name = "NonIntegerInParens",
    json = "{\"enumValue\": \"ONE (abc)\"}",
  },
  NegativeTestCase{name = "Float", json = "{\"enumValue\": 1.0}"},
  NegativeTestCase{name = "FloatInString", json = "{\"enumValue\": \"1.0\"}"},
  NegativeTestCase{
    name = "FloatInParens",
    json = "{\"enumValue\": \"ONE (1.0)\"}",
  },
  NegativeTestCase{name = "LeadingZeros", json = "{\"enumValue\": \"01\"}"},
  NegativeTestCase{
    name = "LeadingZerosInParens",
    json = "{\"enumValue\": \"ONE (01)\"}",
  },
  NegativeTestCase{
    name = "KeyMismatch",
    json = "{\"enumAsKey\": {\"ONE (2)\": \"TWO (2)\"}}",
  },
  NegativeTestCase{
    name = "KeyInvalidName",
    json = "{\"enumAsKey\": {\"INVALID\": \"TWO\"}}",
  },
  NegativeTestCase{
    name = "ValueMismatch",
    json = "{\"enumAsKey\": {\"ONE (1)\": \"TWO (1)\"}}",
  },
  NegativeTestCase{
    name = "ValueInvalidName",
    json = "{\"enumAsKey\": {\"ONE (1)\": \"INVALID (2)\"}}",
  },
];

const list<NegativeTestCase> typeValidationNegativeCases = [
  NegativeTestCase{
    name = "InvalidBinaryKey",
    json = "{\"binaryValue\": {\"invalid\": \"data\"}}",
  },
  NegativeTestCase{
    name = "BinaryMixedAlphabets",
    json = "{\"binaryValue\": \"3q2-+w\"}",
  },
  NegativeTestCase{
    name = "BinaryMapKeyMixedAlphabets",
    json = "{\"binaryAsKey\": {\"3q2-+w\": 1}}",
  },
  NegativeTestCase{
    name = "BinaryBase64UrlWithStandardAlphabet",
    json = "{\"binaryValue\": {\"base64url\": \"3q2+7w\"}}",
  },
  NegativeTestCase{
    name = "BinaryBase64WithUrlAlphabet",
    json = "{\"binaryValue\": {\"base64\": \"3q2-7w\"}}",
  },
  NegativeTestCase{
    name = "BinaryBase64IncompletePadding",
    json = "{\"binaryValue\": {\"base64\": \"3q2+7w=\"}}",
  },
  NegativeTestCase{
    name = "ListGotString",
    json = "{\"listValue\": \"not_an_array\"}",
  },
  NegativeTestCase{
    name = "ListGotObject",
    json = "{\"listValue\": {\"nested\": \"item1\"}}",
  },
  NegativeTestCase{name = "IntegerInvalidBool", json = "{\"i64Value\": true}"},
  NegativeTestCase{name = "IntegerInvalidFloat", json = "{\"i64Value\": 0.0}"},
  NegativeTestCase{
    name = "IntegerInvalidString",
    json = "{\"i64Value\": \"123_not_a_number\"}",
  },
  NegativeTestCase{name = "IntegerEmptyString", json = "{\"i64Value\": \"\"}"},
  NegativeTestCase{
    name = "IntegerLeadingZerosQuoted",
    json = "{\"i64Value\": \"007\"}",
  },
  NegativeTestCase{
    name = "IntegerWhitespaceQuoted",
    json = "{\"i32Value\": \" 42\"}",
  },
  NegativeTestCase{
    name = "IntegerMapKeyLeadingZeros",
    json = "{\"i32AsKey\": {\"01\": 1}}",
  },
  NegativeTestCase{name = "FloatInvalidBool", json = "{\"floatValue\": true}"},
  NegativeTestCase{
    name = "FloatInvalidString",
    json = "{\"floatValue\": \"123_not_a_float\"}",
  },
  NegativeTestCase{name = "FloatEmptyString", json = "{\"floatValue\": \"\"}"},
  NegativeTestCase{
    name = "FloatPrecisionLoss",
    json = "{\"floatValue\": 123456789}",
  },
  NegativeTestCase{
    name = "FloatPrecisionLossString",
    json = "{\"floatValue\": \"123456789\"}",
  },
  NegativeTestCase{
    name = "IntegerMapKeyNotANumber",
    json = "{\"i32AsKey\": {\"abc\": 1}}",
  },
  NegativeTestCase{
    name = "IntegerMapKeyOutOfRange",
    json = "{\"i32AsKey\": {\"3000000000\": 1}}",
  },
  NegativeTestCase{
    name = "DoublePrecisionLoss",
    json = "{\"doubleValue\": 9007199254740993}",
  },
  NegativeTestCase{
    name = "DoubleFromInt64Max",
    json = "{\"doubleValue\": 9223372036854775807}",
  },
  NegativeTestCase{
    name = "DoubleFromInt64MaxString",
    json = "{\"doubleValue\": \"9223372036854775807\"}",
  },
  NegativeTestCase{
    name = "DoubleRoundingUpTo2Pow63",
    json = "{\"doubleValue\": 9223372036854775296}",
  },
  NegativeTestCase{
    name = "FloatFromInt64Max",
    json = "{\"floatValue\": 9223372036854775807}",
  },
  NegativeTestCase{
    name = "DoubleFromQuoted2Pow63",
    json = "{\"doubleValue\": \"9223372036854775808\"}",
  },
  NegativeTestCase{
    name = "DoubleFromQuotedIntegerAboveInt64",
    json = "{\"doubleValue\": \"9223372036854775809\"}",
  },
  NegativeTestCase{
    name = "DoubleFromQuotedIntegerBelowInt64",
    json = "{\"doubleValue\": \"-9223372036854775809\"}",
  },
  NegativeTestCase{
    name = "FloatFromQuotedIntegerAboveInt64",
    json = "{\"floatValue\": \"99999999999999999999\"}",
  },
  NegativeTestCase{
    name = "DoubleFromQuotedIntegerAfterFormFeed",
    json = "{\"doubleValue\": \"\\f9007199254740993\"}",
  },
  NegativeTestCase{
    name = "DoubleFromQuotedIntegerBeforeVerticalTab",
    json = "{\"doubleValue\": \"9007199254740993\\u000B\"}",
  },
  NegativeTestCase{name = "BoolGotNumber", json = "{\"boolValue\": 1}"},
  NegativeTestCase{
    name = "BoolKeyInvalidString",
    json = "{\"boolAsKey\": {\"yes\": 42}}",
  },
  NegativeTestCase{
    name = "BoolKeyNumericString",
    json = "{\"boolAsKey\": {\"1\": 42}}",
  },
  NegativeTestCase{
    name = "BinaryInvalidBase64",
    json = "{\"binaryValue\": {\"base64\": \"!!!!\"}}",
  },
  // 2^53 + 1 and 2^63 have no exact double; 2^24 + 1 has no exact float.
  NegativeTestCase{
    name = "DoubleKeyPrecisionLoss",
    json = "{\"doubleAsKey\": {\"9007199254740993\": 1}}",
  },
  NegativeTestCase{
    name = "DoubleKeyAboveInt64",
    json = "{\"doubleAsKey\": {\"9223372036854775808\": 1}}",
  },
  NegativeTestCase{
    name = "FloatKeyPrecisionLoss",
    json = "{\"floatAsKey\": {\"16777217\": 1}}",
  },
  NegativeTestCase{name = "BinaryBareBase64OneCharacter", json = "{\"binaryValue\": \"Q\"}"},
  NegativeTestCase{name = "BinaryBareBase64FiveCharacters", json = "{\"binaryValue\": \"QUJDR\"}"},
  NegativeTestCase{name = "BinaryBareBase64PartialPadding", json = "{\"binaryValue\": \"QQ=\"}"},
  NegativeTestCase{name = "BinaryBareBase64PaddingAfterCompleteQuantum", json = "{\"binaryValue\": \"QUJD====\"}"},
  NegativeTestCase{name = "BinaryBareBase64ThreePaddingCharacters", json = "{\"binaryValue\": \"Q===\"}"},
  NegativeTestCase{name = "BinaryBareBase64OnlyPadding", json = "{\"binaryValue\": \"====\"}"},
  NegativeTestCase{name = "BinaryBareBase64PaddingInside", json = "{\"binaryValue\": \"QQ==QQ==\"}"},
  NegativeTestCase{name = "BinaryBareBase64Space", json = "{\"binaryValue\": \"QQ Q\"}"},
  NegativeTestCase{name = "BinaryBareBase64OtherCharacter", json = "{\"binaryValue\": \"QQ!Q\"}"},
  NegativeTestCase{name = "MapGotString", json = "{\"i32AsKey\": \"x\"}"},
  NegativeTestCase{name = "MapEntryMisnamedValue", json = "{\"i32AsKey\": [{\"key\": 1, \"val\": 2}]}"},
  NegativeTestCase{name = "StructKeyInObjectForm", json = "{\"structAsKey\": {\"k\": {}}}"},
  NegativeTestCase{name = "ListKeyInObjectForm", json = "{\"listAsKey\": {k: 1}}"},
  NegativeTestCase{name = "MapKeyInObjectForm", json = "{\"mapAsKey\": {\"k\": 1}}"},
];

const list<NegativeTestCase> typeMismatchNegativeCases = [
  NegativeTestCase{
    name = "BoolCaseVariant",
    json = "{\"boolValue\": \"TRUE\"}",
  },
  NegativeTestCase{
    name = "BoolAlternativeString",
    json = "{\"boolValue\": \"yes\"}",
  },
  NegativeTestCase{
    name = "NullListElement",
    json = "{\"listValue\": [{\"nested\": \"a\"}, null]}",
  },
  NegativeTestCase{
    name = "NullMapValue",
    json = "{\"i32AsKey\": [{\"key\": 1, \"value\": null}]}",
  },
  NegativeTestCase{
    name = "NullMapKey",
    json = "{\"i32AsKey\": [{\"key\": null, \"value\": 1}]}",
  },
  NegativeTestCase{
    name = "BinaryMultipleKeys",
    json = "{\"binaryValue\": {\"utf-8\": \"hi\", \"base64\": \"aGk=\"}}",
  },
  NegativeTestCase{
    name = "BinaryInvalidBareString",
    json = "{\"binaryValue\": \"Not Valid!\"}",
  },
  NegativeTestCase{name = "IntegerInvalidArray", json = "{\"i64Value\": [42]}"},
  NegativeTestCase{
    name = "IntegerExponentNotationQuoted",
    json = "{\"i64Value\": \"1e3\"}",
  },
  NegativeTestCase{name = "StringGotInteger", json = "{\"stringValue\": 42}"},
  NegativeTestCase{name = "StringGotBool", json = "{\"stringValue\": true}"},
  NegativeTestCase{name = "StringGotFloat", json = "{\"stringValue\": 1.5}"},
  NegativeTestCase{name = "BinaryGotInteger", json = "{\"binaryValue\": 42}"},
  NegativeTestCase{
    name = "BinaryObjectGotInteger",
    json = "{\"binaryValue\": {\"utf-8\": 42}}",
  },
];

const list<NegativeTestCase> formatValidationNegativeCases = [
  NegativeTestCase{
    name = "MapMissingKey",
    json = "{\"i32AsKey\": [{\"value\": 2}]}",
  },
  NegativeTestCase{
    name = "MapMissingValue",
    json = "{\"i32AsKey\": [{\"key\": 1}]}",
  },
  NegativeTestCase{
    name = "MapExtraElement",
    json = "{\"i32AsKey\": [{\"key\": 1, \"value\": 2, \"foo\": 3}]}",
  },
  NegativeTestCase{name = "MissingClosingBrace", json = "{\"i64Value\": 42"},
  NegativeTestCase{name = "ExtraComma", json = "{\"i64Value\": 42,,}"},
  NegativeTestCase{name = "EmptyInput", json = ""},
  NegativeTestCase{name = "ExtraContent", json = "{}garbage"},
  NegativeTestCase{name = "ExtraClosingBracket", json = "{\"i64Value\": 1}]"},
  NegativeTestCase{
    name = "CommaAfterTopLevelValue",
    json = "{\"i64Value\": 1},",
  },
  NegativeTestCase{
    name = "FieldNameNotFound",
    json = "{\"NonExist (30)\": true}",
  },
  NegativeTestCase{
    name = "FieldIdConflict",
    json = "{\"boolValue (31)\": true}",
  },
  NegativeTestCase{name = "TopLevelNull", json = "null"},
  NegativeTestCase{
    name = "FieldIdOverflowI16Positive",
    json = "{\"(32768)\": true}",
  },
  NegativeTestCase{
    name = "FieldIdOverflowI16Negative",
    json = "{\"(-32769)\": true}",
  },
  NegativeTestCase{name = "FieldIdLeadingZeros", json = "{\"030\": true}"},
  NegativeTestCase{
    name = "FieldIdLeadingZerosInParens",
    json = "{\"boolValue (030)\": true}",
  },
  NegativeTestCase{
    name = "UnionMultipleFields",
    json = "{\"unionValue\": {\"intValue\": 1, \"stringValue\": \"a\"}}",
  },
  NegativeTestCase{
    name = "UnionDuplicateField",
    json = "{\"unionValue\": {\"intValue\": 1, \"intValue\": 2}}",
  },
  NegativeTestCase{
    name = "UnionKnownThenUnknownField",
    json = "{\"unionValue\": {\"intValue\": 1, \"unknown\": 2}}",
  },
  NegativeTestCase{
    name = "UnionUnknownThenKnownField",
    json = "{\"unionValue\": {\"unknown\": 1, \"intValue\": 2}}",
  },
  NegativeTestCase{name = "TopLevelList", json = "[]"},
  NegativeTestCase{name = "EmptyFieldName", json = "{\"\": 1}"},
  NegativeTestCase{name = "EndOfInputAfterFieldName", json = "{\"i64Value\":"},
  NegativeTestCase{name = "SkippedValueMissing", json = "{\"unknown\": }"},
  NegativeTestCase{name = "UnionNullUnknownFieldBetweenFields", json = "{\"unionValue\": {\"intValue\": 1, \"unknown\": null, \"stringValue\": \"a\"}}"},
];

const list<NegativeTestCase> overflowValidationNegativeCases = [
  // byte overflow (range: -128 to 127)
  NegativeTestCase{
    name = "BytePositiveOverflow",
    json = "{\"byteValue\": 128}",
  },
  NegativeTestCase{
    name = "ByteNegativeOverflow",
    json = "{\"byteValue\": -129}",
  },
  // i16 overflow (range: -32768 to 32767)
  NegativeTestCase{
    name = "I16PositiveOverflow",
    json = "{\"i16Value\": 32768}",
  },
  NegativeTestCase{
    name = "I16NegativeOverflow",
    json = "{\"i16Value\": -32769}",
  },
  // i32 overflow (range: -2147483648 to 2147483647)
  NegativeTestCase{
    name = "I32PositiveOverflow",
    json = "{\"i32Value\": 2147483648}",
  },
  NegativeTestCase{
    name = "I32NegativeOverflow",
    json = "{\"i32Value\": -2147483649}",
  },
  // i64 overflow (range: -9223372036854775808 to 9223372036854775807)
  NegativeTestCase{
    name = "I64StringPositiveOverflow",
    json = "{\"i64Value\": \"9223372036854775808\"}",
  },
  NegativeTestCase{
    name = "I64StringNegativeOverflow",
    json = "{\"i64Value\": \"-9223372036854775809\"}",
  },
  NegativeTestCase{
    name = "I64LiteralPositiveOverflow",
    json = "{\"i64Value\": 99999999999999999999}",
  },
  NegativeTestCase{
    name = "I64LiteralNegativeOverflow",
    json = "{\"i64Value\": -99999999999999999999}",
  },
  // enum overflow (range: -2147483648 to 2147483647)
  NegativeTestCase{
    name = "EnumPositiveOverflow1",
    json = "{\"enumValue\": 2147483648}",
  },
  NegativeTestCase{
    name = "EnumPositiveOverflow2",
    json = "{\"enumValue\": \"2147483648\"}",
  },
  NegativeTestCase{
    name = "EnumPositiveOverflow3",
    json = "{\"enumValue\": \"(2147483648)\"}",
  },
  NegativeTestCase{
    name = "EnumNegativeOverflow1",
    json = "{\"enumValue\": -2147483649}",
  },
  NegativeTestCase{
    name = "EnumNegativeOverflow2",
    json = "{\"enumValue\": \"-2147483649\"}",
  },
  NegativeTestCase{
    name = "EnumNegativeOverflow3",
    json = "{\"enumValue\": \"(-2147483649)\"}",
  },
];

struct InvalidUtf8TestCase {
  1: string name;
  // Raw bytes, as they are not valid UTF-8.
  2: binary json;
}

// Strings must be valid UTF-8, including map keys and the "utf-8" form of
// binary. The base64 forms of binary may carry any bytes.
// @lint-ignore-every THRIFTFORMAT
const list<InvalidUtf8TestCase> invalidUtf8NegativeCases = [
  InvalidUtf8TestCase{
    name = "StringValue",
    json = "{\"stringValue\": \"\x80\x81\x82\"}",
  },
  InvalidUtf8TestCase{
    name = "BinaryUtf8Form",
    json = "{\"binaryValue\": {\"utf-8\": \"\xc3\"}}",
  },
  InvalidUtf8TestCase{
    name = "StringMapKey",
    json = "{\"stringAsKey\": {\"\xe0\x80\x80\": \"x\"}}",
  },
  InvalidUtf8TestCase{
    name = "StringValueSplitByLineContinuation",
    json = "{\"stringValue\": \"\xc3\\\n\xa9\"}",
  },
  InvalidUtf8TestCase{
    name = "StringMapKeySplitByLineContinuation",
    json = "{\"stringAsKey\": {\"\xc3\\\n\xa9\": \"x\"}}",
  },
];
