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

// JSON5 syntax cases, independent of any Thrift schema.

package "facebook.com/thrift/json5"

struct ReaderTestCase {
  1: string name;
  // Raw bytes, as they may not be valid UTF-8.
  2: binary input;
  // `input` read as a single value, with floating-point numbers in double
  // precision, and written back as compact basic JSON with bare `NaN` and
  // `Infinity`. Unset if `input` must be rejected.
  3: optional string output;
}

// @lint-ignore-every THRIFTFORMAT
const list<ReaderTestCase> readerTestCases = [
  // Literals
  ReaderTestCase{name = "True", input = "true", output = "true"},
  ReaderTestCase{name = "False", input = "false", output = "false"},
  ReaderTestCase{name = "Null", input = "null", output = "null"},
  ReaderTestCase{name = "UnknownIdentifier", input = "undefined"},
  ReaderTestCase{name = "InfinityMisspelled", input = "Inf"},
  // Strings
  ReaderTestCase{name = "StringEscapes", input = "\"line1\\n\\\"line2\\\"\\\nsame_line\"", output = "\"line1\\n\\\"line2\\\"same_line\""},
  ReaderTestCase{name = "Json5OnlyEscapes", input = "\"\\v\\0\\x41\\xe9\"", output = "\"\\u000b\\u0000Aé\""},
  ReaderTestCase{name = "IdentityEscapes", input = "\"\\q\\\"\\'\\/\\\\\"", output = "\"q\\\"'/\\\\\""},
  ReaderTestCase{name = "EscapedNonAscii", input = "\"\\é\"", output = "\"é\""},
  ReaderTestCase{name = "LineContinuationCr", input = "\"a\\\rb\\\r\nc\"", output = "\"abc\""},
  ReaderTestCase{name = "LineContinuationLineSeparator", input = "\"x\\\u2028y\\\u2029z\"", output = "\"xyz\""},
  ReaderTestCase{name = "OctalEscape", input = "\"\\01\""},
  ReaderTestCase{name = "DigitEscape", input = "\"\\1\""},
  ReaderTestCase{name = "ShortHexEscape", input = "\"\\x4\""},
  ReaderTestCase{name = "UnescapedNewline", input = "\"hello\nworld\""},
  ReaderTestCase{name = "SingleQuotedString", input = "'\\u0041'", output = "\"A\""},
  // Unicode escapes
  ReaderTestCase{name = "UnicodeEscape", input = "\"\\u20ac\"", output = "\"€\""},
  ReaderTestCase{name = "UnicodeEscapeUppercase", input = "\"\\u20AC\"", output = "\"€\""},
  ReaderTestCase{name = "UnicodeEscapeAfterText", input = "\"caf\\u00e9\"", output = "\"café\""},
  ReaderTestCase{name = "UnicodeEscapesAroundDigit", input = "\"\\u00482\\u004F\"", output = "\"H2O\""},
  ReaderTestCase{name = "UnicodeEscapeNul", input = "\"\\u0000\"", output = "\"\\u0000\""},
  ReaderTestCase{name = "UnicodeEscapeTruncated", input = "\"\\u00\""},
  ReaderTestCase{name = "UnicodeEscapeInvalidHex", input = "\"\\u00GZ\""},
  ReaderTestCase{name = "SurrogatePair", input = "\"\\ud83d\\ude00\"", output = "\"😀\""},
  ReaderTestCase{name = "SurrogatePairUppercase", input = "\"\\uD83D\\uDE00\"", output = "\"😀\""},
  ReaderTestCase{name = "SurrogatePairBetweenText", input = "\"a\\ud83d\\ude00b\"", output = "\"a😀b\""},
  ReaderTestCase{name = "HighSurrogateAlone", input = "\"\\ud83d\""},
  ReaderTestCase{name = "HighSurrogateThenText", input = "\"\\ud83dA\""},
  ReaderTestCase{name = "HighSurrogateThenNonSurrogate", input = "\"\\ud83d\\u0041\""},
  ReaderTestCase{name = "LowSurrogateAlone", input = "\"\\ude00\""},
  // UTF-8 in strings
  ReaderTestCase{name = "ReplacementCharacter", input = "\"�\"", output = "\"�\""},
  ReaderTestCase{name = "FourByteUtf8", input = "\"👋\"", output = "\"👋\""},
  ReaderTestCase{name = "EscapeThenRawUtf8", input = "\"\\u00e9é\"", output = "\"éé\""},
  ReaderTestCase{name = "RawNonAsciiInString", input = "\"café\"", output = "\"café\""},
  ReaderTestCase{name = "InvalidUtf8ContinuationBytes", input = "\"\x80\x81\x82\""},
  ReaderTestCase{name = "InvalidUtf8Truncated", input = "\"hello\xdf\""},
  ReaderTestCase{name = "InvalidUtf8Overlong", input = "\"\xe0\x80\x80\""},
  ReaderTestCase{name = "InvalidUtf8Surrogate", input = "\"\xed\xa0\x80\""},
  ReaderTestCase{name = "InvalidUtf8AboveMaxCodePoint", input = "\"\xf5\x80\x80\x80\""},
  ReaderTestCase{name = "InvalidUtf8LeadByteThenEscape", input = "\"\xc3\\u00A9\""},
  ReaderTestCase{name = "InvalidUtf8SplitByLineContinuation", input = "\"\xc3\\\n\xa9\""},
  ReaderTestCase{name = "InvalidUtf8EscapedContinuationByte", input = "\"\\\xa0\""},
  ReaderTestCase{name = "InvalidUtf8SingleQuoted", input = "'\x80'"},
  ReaderTestCase{name = "InvalidUtf8ObjectName", input = "{\"\xdf\": 1}"},
  // Numbers
  ReaderTestCase{name = "Integer", input = "42", output = "42"},
  ReaderTestCase{name = "DecimalInt64Max", input = "9223372036854775807", output = "9223372036854775807"},
  ReaderTestCase{name = "DecimalInt64Min", input = "-9223372036854775808", output = "-9223372036854775808"},
  ReaderTestCase{name = "DecimalAboveInt64Max", input = "9223372036854775808"},
  ReaderTestCase{name = "DecimalBelowInt64Min", input = "-9223372036854775809"},
  ReaderTestCase{name = "DecimalAboveUint64Max", input = "18446744073709551616"},
  ReaderTestCase{name = "DecimalFarAboveInt64Max", input = "99999999999999999999"},
  ReaderTestCase{name = "DecimalFarBelowInt64Min", input = "-99999999999999999999"},
  ReaderTestCase{name = "NegativeZeroInteger", input = "-0", output = "0"},
  ReaderTestCase{name = "LeadingZeros00", input = "00"},
  ReaderTestCase{name = "LeadingZeros01", input = "01"},
  ReaderTestCase{name = "LeadingZeros007", input = "007"},
  ReaderTestCase{name = "LeadingZeros0123", input = "0123"},
  ReaderTestCase{name = "Hex0", input = "0x0", output = "0"},
  ReaderTestCase{name = "Hex00", input = "0x00", output = "0"},
  ReaderTestCase{name = "HexFF", input = "0xFF", output = "255"},
  ReaderTestCase{name = "HexLowercase", input = "0xdead", output = "57005"},
  ReaderTestCase{name = "HexUppercasePrefix", input = "0XDEAD", output = "57005"},
  ReaderTestCase{name = "HexMixedCase", input = "0xDeAd", output = "57005"},
  ReaderTestCase{name = "HexPositive", input = "+0xDEAD", output = "57005"},
  ReaderTestCase{name = "HexNegative", input = "-0xDEAD", output = "-57005"},
  ReaderTestCase{name = "HexNegativeZero", input = "-0x0", output = "0"},
  ReaderTestCase{name = "HexInt64Max", input = "0x7fffffffffffffff", output = "9223372036854775807"},
  ReaderTestCase{name = "HexInt64Min", input = "-0x8000000000000000", output = "-9223372036854775808"},
  ReaderTestCase{name = "HexAboveInt64Max", input = "0x8000000000000000"},
  ReaderTestCase{name = "HexBelowInt64Min", input = "-0x8000000000000001"},
  ReaderTestCase{name = "HexInContainers", input = "[0x1, 0xA, {color: 0xFF00FF}]", output = "[1,10,{\"color\":16711935}]"},
  ReaderTestCase{name = "HexNoDigits", input = "0x"},
  ReaderTestCase{name = "HexNoLeadingZero", input = "x0"},
  ReaderTestCase{name = "HexTwoLeadingZeros", input = "00x0"},
  ReaderTestCase{name = "HexSpaceAfterZero", input = "0 x0"},
  ReaderTestCase{name = "HexSpaceAfterX", input = "0x 0"},
  ReaderTestCase{name = "HexWrongPrefix", input = "0Y0"},
  ReaderTestCase{name = "HexInvalidDigit", input = "0xfg"},
  ReaderTestCase{name = "HexFraction", input = "0x1.0"},
  ReaderTestCase{name = "HexFloat", input = "0x1.0p1"},
  ReaderTestCase{name = "Float", input = "3.14", output = "3.14"},
  ReaderTestCase{name = "FloatNegativeZero", input = "-0.0", output = "-0.0"},
  ReaderTestCase{name = "FloatShortestDigits", input = "0.10000001", output = "0.10000001"},
  ReaderTestCase{name = "FloatExponent", input = "3.14e0", output = "3.14"},
  ReaderTestCase{name = "FloatNegativeExponent", input = "314e-2", output = "3.14"},
  ReaderTestCase{name = "FloatPositiveExponentUppercase", input = "0.314E+1", output = "3.14"},
  ReaderTestCase{name = "FloatLeadingDecimalPoint", input = ".314e1", output = "3.14"},
  ReaderTestCase{name = "FloatSmallMantissa", input = "0.00314e3", output = "3.14"},
  ReaderTestCase{name = "FloatIntegerMantissa", input = "31400E-4", output = "3.14"},
  ReaderTestCase{name = "FloatTrailingDecimalPoint", input = "5.", output = "5.0"},
  ReaderTestCase{name = "FloatPositive", input = "+1.5", output = "1.5"},
  ReaderTestCase{name = "FloatOnlyDecimalPoint", input = "."},
  ReaderTestCase{name = "FloatExponentNoDigits", input = "1e"},
  ReaderTestCase{name = "FloatExponentSignNoDigits", input = "1e+"},
  ReaderTestCase{name = "FloatNegativeExponentSignNoDigits", input = "2.5E-"},
  ReaderTestCase{name = "SignOnly", input = "+"},
  ReaderTestCase{name = "NanAndInfinity", input = "[NaN, +NaN, -NaN, Infinity, +Infinity, -Infinity]", output = "[NaN,NaN,-NaN,Infinity,Infinity,-Infinity]"},
  // Containers
  ReaderTestCase{name = "EmptyContainers", input = "{\n  \"empty\": \"\",\n  \"emptyObj\": {},\n  \"emptyList\": []\n}", output = "{\"empty\":\"\",\"emptyObj\":{},\"emptyList\":[]}"},
  ReaderTestCase{name = "EmptyContainersJson5", input = "{\n  empty: \"\",\n  emptyObj: {},\n  emptyList: [],\n}", output = "{\"empty\":\"\",\"emptyObj\":{},\"emptyList\":[]}"},
  ReaderTestCase{name = "Primitives", input = "{\n  \"bool\": false,\n  \"integer\": 42,\n  \"$float\": 3.14\n}", output = "{\"bool\":false,\"integer\":42,\"$float\":3.14}"},
  ReaderTestCase{name = "PrimitivesJson5", input = "{\n  bool: false,\n  integer: 42,\n  $float: 3.14,\n}", output = "{\"bool\":false,\"integer\":42,\"$float\":3.14}"},
  ReaderTestCase{name = "Nested", input = "{\n  \"list\": [\n    1,\n    3,\n    2\n  ],\n  \"object\": {\n    \"1\": 3,\n    \"2\": 4\n  }\n}", output = "{\"list\":[1,3,2],\"object\":{\"1\":3,\"2\":4}}"},
  ReaderTestCase{name = "NestedJson5", input = "{\n  list: [\n    1,\n    3,\n    2,\n  ],\n  object: {\n    \"1\": 3,\n    \"2\": 4,\n  },\n}", output = "{\"list\":[1,3,2],\"object\":{\"1\":3,\"2\":4}}"},
  ReaderTestCase{name = "CompactList", input = "[1,2]", output = "[1,2]"},
  ReaderTestCase{name = "CompactListTrailingComma", input = "[1,2,]", output = "[1,2]"},
  ReaderTestCase{name = "CompactObject", input = "{\"name\":\"Alice\"}", output = "{\"name\":\"Alice\"}"},
  ReaderTestCase{name = "CompactObjectJson5", input = "{name:\"Alice\",}", output = "{\"name\":\"Alice\"}"},
  ReaderTestCase{name = "MixedValues", input = "{\n    \"string\": \"hello\",\n    \"array\": [-1, \"two\", false],\n    \"nested\": {\"inner\": 99}\n  }", output = "{\"string\":\"hello\",\"array\":[-1,\"two\",false],\"nested\":{\"inner\":99}}"},
  ReaderTestCase{name = "MixedValuesJson5", input = "{\n    string: 'hello', // comment\n    array: [-1, \"two\", false,],\n    nested: {inner: /* comment */ +99,},\n  }", output = "{\"string\":\"hello\",\"array\":[-1,\"two\",false],\"nested\":{\"inner\":99}}"},
  ReaderTestCase{name = "UnterminatedObject", input = "{\"a\": 1"},
  ReaderTestCase{name = "UnterminatedList", input = "[1, 2"},
  ReaderTestCase{name = "ConsecutiveCommas", input = "[1,,2]"},
  ReaderTestCase{name = "MissingComma", input = "[1 2]"},
  ReaderTestCase{name = "MissingCommaBetweenContainers", input = "[[][]]"},
  ReaderTestCase{name = "ObjectNameStartsWithDigit", input = "{123: 456}"},
  ReaderTestCase{name = "MissingColon", input = "{\"key\" 123}"},
  ReaderTestCase{name = "NonAsciiUnquotedObjectName", input = "{é: 1}"},
  // Comments and whitespace
  ReaderTestCase{name = "LineCommentBefore", input = "// line comment\n42", output = "42"},
  ReaderTestCase{name = "LineCommentAfter", input = "42// trailing", output = "42"},
  ReaderTestCase{name = "BlockCommentBefore", input = "/* block */42", output = "42"},
  ReaderTestCase{name = "MultiLineBlockComment", input = "/* multi\n   line */42", output = "42"},
  ReaderTestCase{name = "AdjacentBlockComments", input = "/*a*//*b*/42/**/", output = "42"},
  ReaderTestCase{name = "LineThenBlockComment", input = "// line\n/* block */42", output = "42"},
  ReaderTestCase{name = "BlockCommentWithSlashStar", input = "/*/* stars inside */42", output = "42"},
  ReaderTestCase{name = "LineCommentEndsAtLineSeparator", input = "// ends at U+2028\u202842", output = "42"},
  ReaderTestCase{name = "LineCommentEndsAtParagraphSeparator", input = "// ends at U+2029\u202942", output = "42"},
  ReaderTestCase{name = "UnterminatedBlockComment", input = "42/* unterminated"},
  ReaderTestCase{name = "CommentOnly", input = "// Comment-only, no value"},
  ReaderTestCase{name = "CommentsInStringsAndContainers", input = "{\n    // a comment\n    \"/* comment inside string */\" /* inline */: /* before value */ [\n      // first element\n      \"/* multi-line \\n string1 */\",\n      \"// multi-line \\n string2\",\n      \"/* single-line \\\n          string3 */\",\n      \"// single-line \\\n          string4\"\n    ]\n    // trailing\n  }", output = "{\"/* comment inside string */\":[\"/* multi-line \\n string1 */\",\"// multi-line \\n string2\",\"/* single-line           string3 */\",\"// single-line           string4\"]}"},
  ReaderTestCase{name = "UnicodeWhitespace", input = "\u00A0\u1680\u2000\u200A\u2028\u2029\u202F\u205F\u3000\uFEFF42", output = "42"},
  ReaderTestCase{name = "ZeroWidthSpace", input = "\u200B42"},
  ReaderTestCase{name = "ByteOrderMark", input = "\uFEFF42"},
  ReaderTestCase{name = "Latin1NoBreakSpace", input = "\xa042"},
  ReaderTestCase{name = "Latin1SuperscriptTwo", input = "4\xb2"},
  // Top level
  ReaderTestCase{name = "Empty", input = ""},
  ReaderTestCase{name = "ValueAfterComma", input = "{\"a\": 1}, {}"},
  ReaderTestCase{name = "ValueAfterCommaInList", input = "[1], 2"},
  ReaderTestCase{name = "ValueAfterCommaAndComment", input = "42 /* comment */ , 43"},
  ReaderTestCase{name = "LoneSlash", input = "1/2"},
  ReaderTestCase{name = "SignThenIdentifier", input = "-null"},
  ReaderTestCase{name = "SignThenInfinityMisspelled", input = "-Inf"},
  ReaderTestCase{name = "SignedDecimalPointThenExponent", input = "-.e5"},
];
