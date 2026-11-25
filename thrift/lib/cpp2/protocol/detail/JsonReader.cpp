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

#include <thrift/lib/cpp2/protocol/detail/JsonReader.h>

#include <algorithm>
#include <charconv>
#include <cstdint>
#include <iterator>
#include <limits>
#include <utility>

#include <fmt/core.h>
#include <folly/Conv.h>
#include <folly/Range.h>
#include <folly/String.h>
#include <folly/Unicode.h>
#include <thrift/lib/cpp/protocol/TProtocolException.h>

namespace apache::thrift::json5::detail {

namespace {

constexpr bool isAsciiDigit(char c) {
  return c >= '0' && c <= '9';
}

constexpr bool isAsciiHexDigit(char c) {
  return isAsciiDigit(c) || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
}

constexpr bool isNumberStart(char c) {
  return isAsciiDigit(c) || c == '-' || c == '+' || c == '.' || c == 'N' ||
      c == 'I';
}

constexpr bool isAsciiAlpha(char c) {
  return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
}

constexpr bool isAsciiSpace(char c) {
  return c == ' ' || c == '\t' || c == '\n' || c == '\v' || c == '\f' ||
      c == '\r';
}

// JSON5 WhiteSpace and LineTerminator beyond ASCII: Unicode Zs, U+2028, U+2029
// and U+FEFF.
constexpr bool isUnicodeSpace(char32_t c) {
  return c == U'\u00A0' || c == U'\u1680' ||
      (c >= U'\u2000' && c <= U'\u200A') || c == U'\u2028' || c == U'\u2029' ||
      c == U'\u202F' || c == U'\u205F' || c == U'\u3000' || c == U'\uFEFF';
}

// Returns the code point at `cursor` and its length in bytes, or U+FFFD and 1
// if the input there is not well-formed UTF-8.
std::pair<char32_t, std::size_t> peekCodePoint(folly::io::Cursor cursor) {
  unsigned char buf[4];
  const unsigned char* p = buf;
  const char32_t codePoint = folly::utf8ToCodePoint(
      p, buf + cursor.pullAtMost(buf, sizeof(buf)), /*skipOnError=*/true);
  return {codePoint, p - buf};
}

bool isIdentifierStart(char c) {
  return isAsciiAlpha(c) || c == '_' || c == '$';
}

bool isIdentifierPart(char c) {
  return isIdentifierStart(c) || isAsciiDigit(c);
}

// Messages may echo input bytes, but must stay valid UTF-8 (thrift-python
// decodes them strictly).
[[noreturn]] void throwParseError(const std::string& msg) {
  throw protocol::TProtocolException(
      protocol::TProtocolException::INVALID_DATA,
      "Json5Reader: " + folly::backslashify(msg));
}

template <typename T>
T parseNumberOrThrow(std::string_view str) {
  auto result = folly::tryTo<T>(str);
  if (!result.hasValue()) {
    throwParseError(folly::makeConversionError(result.error(), str).what());
  }
  return *result;
}

// Like parseNumberOrThrow<std::uint64_t>, without its overhead for the digits
// of most integers: up to 19 digits cannot overflow.
std::uint64_t parseDecimalMagnitude(std::string_view digits) {
  if (digits.size() > std::numeric_limits<std::uint64_t>::digits10) {
    return parseNumberOrThrow<std::uint64_t>(digits);
  }
  std::uint64_t magnitude = 0;
  for (char digit : digits) {
    magnitude = magnitude * 10 + (digit - '0');
  }
  return magnitude;
}

std::uint64_t parseHexMagnitude(std::string_view str) {
  uint64_t uval = 0;
  auto [ptr, ec] =
      std::from_chars(str.data(), str.data() + str.size(), uval, 16);

  if (ec != std::errc{} || ptr != str.data() + str.size()) {
    throwParseError(fmt::format("invalid hex number {}", str));
  }
  return uval;
}

// Rejects integers outside the int64_t range.
Json5Reader::Integer makeInteger(
    bool negative, std::uint64_t magnitude, std::string_view literal) {
  constexpr std::uint64_t kInt64Max = std::numeric_limits<std::int64_t>::max();
  const std::uint64_t maxMagnitude = negative ? kInt64Max + 1 : kInt64Max;
  if (magnitude > maxMagnitude) {
    throwParseError(fmt::format("integer {} out of range", literal));
  }
  return {.negative = negative, .magnitude = magnitude};
}

std::uint16_t readHexDigits(folly::io::Cursor& cursor, std::size_t count) {
  if (!cursor.canAdvance(count)) {
    throwParseError(fmt::format("expected {} hex digits", count));
  }
  return static_cast<std::uint16_t>(
      parseHexMagnitude(cursor.readFixedString(count)));
}

// The writer escapes strings with folly::json::escapeString's validate_utf8, so
// accepting invalid UTF-8 would produce values that cannot be written back.
void validateUtf8(std::string_view str) {
  auto* p = reinterpret_cast<const unsigned char*>(str.data());
  auto* const end = p + str.size();
  while (p != end) {
    try {
      folly::utf8ToCodePoint(p, end, /*skipOnError=*/false);
    } catch (const std::exception& e) {
      throwParseError(fmt::format("invalid UTF-8 in string: {}", e.what()));
    }
  }
}

// Decodes a `\uXXXX` escape, with the cursor just past the `u`. A code point
// outside the BMP is written as a UTF-16 surrogate pair, i.e. two escapes.
void decodeUnicodeEscape(folly::io::Cursor& cursor, std::string& out) {
  const std::uint16_t first = readHexDigits(cursor, 4);
  char32_t codePoint = first;

  if (folly::utf16_code_unit_is_high_surrogate(first)) {
    if (!cursor.canAdvance(2) || cursor.readFixedString(2) != "\\u") {
      throwParseError("expected a second escape to complete surrogate pair");
    }
    const std::uint16_t second = readHexDigits(cursor, 4);
    if (!folly::utf16_code_unit_is_low_surrogate(second)) {
      throwParseError("invalid second half of surrogate pair");
    }
    codePoint =
        folly::unicode_code_point_from_utf16_surrogate_pair(first, second);
  } else if (!folly::utf16_code_unit_is_bmp(first)) {
    throwParseError("unpaired low surrogate");
  }

  folly::appendCodePointToUtf8(codePoint, out);
}

// Returns the leading bytes that can be copied into the string as is:
// everything before the closing `quote`, a backslash, `\n` or `\r`.
folly::ByteRange findCopyableRange(folly::ByteRange bytes, char quote) {
  const unsigned char q = quote;
  auto isSpecial = [q](unsigned char b) {
    return b == q || b == '\\' || b == '\n' || b == '\r';
  };
  // Most strings (member names, enum names) are short, and a plain loop finds
  // their end faster than the setup of the vectorized search below.
  constexpr std::size_t kShortSize = 16;
  const folly::ByteRange prefix = bytes.subpiece(0, kShortSize);
  const auto end = std::find_if(prefix.begin(), prefix.end(), isSpecial);
  if (end != prefix.end() || prefix.size() == bytes.size()) {
    return {bytes.begin(), end};
  }
  const unsigned char specials[] = {q, '\\', '\n', '\r'};
  const std::size_t pos =
      bytes.subpiece(kShortSize).find_first_of({specials, std::end(specials)});
  return pos == folly::ByteRange::npos ? bytes
                                       : bytes.subpiece(0, kShortSize + pos);
}

bool isNumberPart(char c) {
  return isIdentifierPart(c) || c == '.' || c == '+' || c == '-';
}

// Parses the JSON5 number at the start of `input` and removes it from `input`,
// leaving any trailing characters.
Json5Reader::Primitive parseNumberPrefix(
    std::string_view& input, Json5Reader::FloatingPointPrecision precision) {
  using enum Json5Reader::FloatingPointPrecision;
  const std::string_view start = input;
  auto peek = [&] { return input.empty() ? '\0' : input.front(); };
  auto skipWhile = [&](auto pred) {
    auto end = std::find_if_not(input.begin(), input.end(), pred);
    auto skipped = input.substr(0, end - input.begin());
    input.remove_prefix(skipped.size());
    return skipped;
  };
  auto consumed = [&] { return start.substr(0, start.size() - input.size()); };

  char c = peek();
  const int8_t sign = (c == '-' ? -1 : 1);
  if (c == '+' || c == '-') {
    input.remove_prefix(1);
    c = peek();
  }

  // Infinity or NaN
  if (c == 'I' || c == 'N') {
    const auto word = skipWhile(isIdentifierPart);
    if (word != "Infinity" && word != "NaN") {
      throwParseError(
          fmt::format("expected 'Infinity' or 'NaN', got '{}'", word));
    }
    const double d = std::copysign(
        word == "NaN" ? std::numeric_limits<double>::quiet_NaN()
                      : std::numeric_limits<double>::infinity(),
        sign);
    if (precision == Single) {
      return float(d);
    }
    return d;
  }
  // Hex literal: 0x...
  if (c == '0') {
    input.remove_prefix(1);
    c = peek();
    if (c == 'x' || c == 'X') {
      const auto prefix = consumed();
      input.remove_prefix(1);
      const auto digits = skipWhile(isAsciiHexDigit);
      return makeInteger(
          sign < 0,
          parseHexMagnitude(digits),
          fmt::format("{}x{}", prefix, digits));
    }
  } else if (c != '.') {
    skipWhile(isAsciiDigit);
    c = peek();
  }

  bool isFloating = false;

  if (c == '.') {
    isFloating = true;
    input.remove_prefix(1);
    skipWhile(isAsciiDigit);
    c = peek();
  }

  if (c == 'e' || c == 'E') {
    isFloating = true;
    input.remove_prefix(1);
    if (peek() == '+' || peek() == '-') {
      input.remove_prefix(1);
    }
    skipWhile(isAsciiDigit);
  }

  const std::string_view numStr = consumed();
  if (numStr.empty() || numStr == "+" || numStr == "-") {
    throwParseError("expected number");
  }

  if (!isFloating) {
    std::string_view digits = numStr;
    if (digits.front() == '+' || digits.front() == '-') {
      digits.remove_prefix(1);
    }
    return makeInteger(sign < 0, parseDecimalMagnitude(digits), numStr);
  }
  if (precision == Single) {
    return parseNumberOrThrow<float>(numStr);
  }
  return parseNumberOrThrow<double>(numStr);
}

char peekNext(folly::io::Cursor& cursor) {
  return cursor.isAtEnd() ? '\0' : static_cast<char>(cursor.peek().front());
}

char readNext(folly::io::Cursor& cursor) {
  if (cursor.isAtEnd()) {
    throwParseError("unexpected end of input");
  }
  return cursor.read<char>();
}

// Returns the length of the JSON5 LineTerminatorSequence (LF, CR, CR LF, U+2028
// or U+2029) at `cursor`, or 0.
std::size_t lineTerminatorSize(folly::io::Cursor cursor) {
  switch (peekNext(cursor)) {
    case '\n':
      return 1;
    case '\r':
      cursor.skip(1);
      return peekNext(cursor) == '\n' ? 2 : 1;
    case '\xE2': {
      auto [codePoint, size] = peekCodePoint(cursor);
      return codePoint == U'\u2028' || codePoint == U'\u2029' ? size : 0;
    }
    default:
      return 0;
  }
}

} // namespace

// -- cursor operations ----------------------------------------------------

void Json5Reader::setCursor(folly::io::Cursor c) {
  in_ = std::move(c);
  depth_ = 0;
  skipWhitespaceAndComments();
}

const folly::io::Cursor& Json5Reader::getCursor() const {
  return in_.value();
}

folly::io::Cursor& Json5Reader::cursor() {
  return in_.value();
}

char Json5Reader::peekChar() {
  return peekNext(cursor());
}

char Json5Reader::readChar() {
  return readNext(cursor());
}

void Json5Reader::consume(char expected) {
  char got = readChar();
  if (got != expected) {
    throwParseError(fmt::format("expected '{}', got '{}'", expected, got));
  }
  skipWhitespaceAndComments();
}

// -- whitespace and comments ----------------------------------------------

bool Json5Reader::skipComment() {
  if (peekChar() != '/') {
    return false;
  }
  cursor().skip(1);
  switch (readChar()) {
    case '/':
      // Skip "// ..." comment
      while (!cursor().isAtEnd() && lineTerminatorSize(cursor()) == 0) {
        cursor().skip(1);
      }
      return true;
    case '*': {
      // Skip "/* ... */" comment
      char prev = 0;
      cursor().skipWhile([&prev](char curr) {
        if (prev == '*' && curr == '/') {
          return false;
        }
        prev = curr;
        return true;
      });
      if (cursor().isAtEnd()) {
        throwParseError("unterminated block comment");
      } else {
        cursor().skip(1); // skip "/"
      }
      return true;
    }
    default:
      throwParseError("expected '//' or '/*' comment");
  }
}

bool Json5Reader::skipUnicodeSpace() {
  if (static_cast<unsigned char>(peekChar()) < 0x80) {
    return false;
  }
  auto [codePoint, size] = peekCodePoint(cursor());
  if (!isUnicodeSpace(codePoint)) {
    return false;
  }
  cursor().skip(size);
  return true;
}

void Json5Reader::skipWhitespaceAndComments() {
  // This runs after every token, and in compact JSON nothing follows most of
  // them, so check the one byte that would have to start whitespace or a
  // comment (ASCII space, '/', or the lead byte of a Unicode space) before
  // paying for the loop below.
  const char c = peekChar();
  if (!isAsciiSpace(c) && c != '/' && static_cast<unsigned char>(c) < 0x80) {
    return;
  }
  do {
    cursor().skipWhile(isAsciiSpace);
    // Loop to handle adjacent comments, e.g. `/*a*//*b*/`.
  } while (skipUnicodeSpace() || skipComment());
}

// -- comma handling -------------------------------------------------------

void Json5Reader::expectCommaOrEnd() {
  skipWhitespaceAndComments();
  char c = peekChar();
  if (c == ',') {
    if (depth_ == 0) {
      return; // A comma after a top-level value belongs to the caller.
    }
    cursor().skip(1);
    skipWhitespaceAndComments();
    if (peekChar() == ',') {
      throwParseError("unexpected consecutive commas"); // e.g., "[1,,2]"
    }
  } else if (c != ']' && c != '}' && c != '\0') {
    throwParseError(
        fmt::format("expected ',' or closing delimiter, got '{}'", c));
  }
}

// -- token peeking --------------------------------------------------------

Json5Reader::Token Json5Reader::peekToken() {
  switch (peekChar()) {
    case '[':
      return Token::ListBegin;
    case ']':
      return Token::ListEnd;
    case '{':
      return Token::ObjectBegin;
    case '}':
      return Token::ObjectEnd;
    case '\0':
      throwParseError("unexpected end of input");
    default:
      return Token::Primitive;
  }
}

// -- strings and identifiers ----------------------------------------------

void Json5Reader::parseString(char quote, std::string& result) {
  folly::io::Cursor start = cursor();
  // OR of every input byte copied as is; bit 7 is set if any was non-ASCII.
  unsigned char rawBytesOr = 0;
  auto appendRaw = [&](char ch) {
    rawBytesOr |= static_cast<unsigned char>(ch);
    result.push_back(ch);
  };
  while (true) {
    const folly::ByteRange bytes = cursor().peekBytes();
    const folly::ByteRange copyableRange = findCopyableRange(bytes, quote);
    for (auto byte : copyableRange) {
      rawBytesOr |= byte;
    }
    result.append(
        reinterpret_cast<const char*>(copyableRange.data()),
        copyableRange.size());
    cursor().skip(copyableRange.size());
    if (copyableRange.size() == bytes.size() && !copyableRange.empty()) {
      continue; // The string may continue in the next buffer.
    }

    char c = readChar();
    if (c == quote) {
      // Validate the input, not `result`: a line continuation can join
      // ill-formed input bytes into a well-formed `result`.
      if ((rawBytesOr & 0x80) != 0) {
        validateUtf8(start.readFixedString(cursor() - start - 1));
      }
      return;
    }
    if (c == '\n' || c == '\r') {
      throwParseError("unescaped newline in string");
    }
    if (auto size = lineTerminatorSize(cursor())) {
      // Line continuation
      cursor().skip(size);
      continue;
    }
    char esc = readChar();
    switch (esc) {
      case 'b':
        result.push_back('\b');
        break;
      case 'f':
        result.push_back('\f');
        break;
      case 'n':
        result.push_back('\n');
        break;
      case 'r':
        result.push_back('\r');
        break;
      case 't':
        result.push_back('\t');
        break;
      case 'v':
        result.push_back('\v');
        break;
      case '0':
        if (isAsciiDigit(peekChar())) {
          throwParseError("octal escape in string");
        }
        result.push_back('\0');
        break;
      case 'x':
        folly::appendCodePointToUtf8(readHexDigits(cursor(), 2), result);
        break;
      case 'u':
        decodeUnicodeEscape(cursor(), result);
        break;
      default:
        if (isAsciiDigit(esc)) {
          throwParseError(fmt::format("unknown escape '\\{}' in string", esc));
        }
        // Any other character escapes itself, e.g. `\"` and `\q`.
        appendRaw(esc);
    }
  }
}

std::string Json5Reader::readObjectName() {
  std::string name;
  readObjectName(name);
  return name;
}

void Json5Reader::readObjectName(std::string& name) {
  char c = peekChar();
  if (c == '"' || c == '\'') {
    cursor().skip(1);
    name.clear();
    parseString(c, name);
  } else if (isIdentifierStart(c)) {
    name = cursor().readWhile(isIdentifierPart);
  } else {
    throwParseError(fmt::format("expected object name, got '{}'", c));
  }
  skipWhitespaceAndComments();
  consume(':');
}

bool Json5Reader::tryReadString(std::string& out) {
  const char c = peekChar();
  if (c != '"' && c != '\'') {
    return false;
  }
  cursor().skip(1);
  out.clear();
  parseString(c, out);
  expectCommaOrEnd();
  return true;
}

// -- numbers --------------------------------------------------------------

Json5Reader::Primitive Json5Reader::parseNumber(
    folly::io::Cursor& cursor, FloatingPointPrecision precision) {
  // Parse in place, unless the characters that can make up a number run into
  // the next buffer.
  const folly::ByteRange bytes = cursor.peekBytes();
  std::string_view input(
      reinterpret_cast<const char*>(bytes.data()), bytes.size());
  std::string spanning;
  // The number can only run into the next buffer if this one ends in a
  // character that can be part of it.
  if ((input.empty() || isNumberPart(input.back())) &&
      std::all_of(input.begin(), input.end(), isNumberPart)) {
    spanning = folly::io::Cursor(cursor).readWhile(isNumberPart);
    input = spanning;
  }
  std::string_view rest = input;
  auto number = parseNumberPrefix(rest, precision);
  cursor.skip(input.size() - rest.size());
  return number;
}

Json5Reader::Primitive Json5Reader::parseNumber(
    std::string_view str, FloatingPointPrecision precision) {
  if (!str.empty() && isNumberStart(str.front())) {
    std::string_view rest = str;
    auto number = parseNumberPrefix(rest, precision);
    if (rest.empty()) {
      return number;
    }
  }
  throwParseError(fmt::format("invalid number '{}'", str));
}

// -- values ---------------------------------------------------------------

Json5Reader::Primitive Json5Reader::readPrimitive(
    FloatingPointPrecision precision) {
  auto result = parsePrimitive(precision);
  expectCommaOrEnd();
  return result;
}

Json5Reader::Primitive Json5Reader::parsePrimitive(
    FloatingPointPrecision precision) {
  char c = peekChar();
  if (c == '"' || c == '\'') {
    cursor().skip(1);
    Primitive result;
    parseString(c, result.emplace<std::string>());
    return result;
  }
  if (isNumberStart(c)) {
    return parseNumber(cursor(), precision);
  }
  if (isIdentifierStart(c)) {
    std::string word = cursor().readWhile(isIdentifierPart);
    if (word == "null") {
      return std::monostate{};
    }
    if (word == "true") {
      return true;
    }
    if (word == "false") {
      return false;
    }
    throwParseError(fmt::format("unexpected identifier '{}'", word));
  }
  throwParseError(fmt::format("expected value, got '{}'", c));
}

} // namespace apache::thrift::json5::detail
