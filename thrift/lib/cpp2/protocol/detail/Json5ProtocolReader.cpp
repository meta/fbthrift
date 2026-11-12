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

#include <thrift/lib/cpp2/protocol/detail/Json5ProtocolReader.h>

#include <limits>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <variant>

#include <folly/Exception.h>
#include <folly/Range.h>
#include <folly/String.h>
#include <folly/base64.h>
#include <folly/lang/Pretty.h>
#include <thrift/lib/cpp2/protocol/detail/JsonUtils.h>

namespace apache::thrift::json5::detail {

[[noreturn]] void Json5ProtocolReader::throwError(std::string_view message) {
  throw protocol::TProtocolException(
      protocol::TProtocolException::INVALID_DATA,
      fmt::format("Json5ProtocolReader: {}", message));
}

template <typename To, typename From>
To Json5ProtocolReader::convertTo(const From& from) {
  auto result = folly::tryTo<To>(from);
  if (!result.hasValue()) {
    throwError(
        folly::makeConversionError(result.error(), folly::to<std::string>(from))
            .what());
  }
  return *result;
}

namespace {
template <typename Variant>
const char* getVariantTypePrettyName(const Variant& variant) {
  return std::visit(
      [](const auto& value) {
        using T = std::remove_cvref_t<decltype(value)>;
        if constexpr (std::is_same_v<T, Json5Reader::Integer>) {
          return folly::pretty_name<std::int64_t>();
        } else {
          return folly::pretty_name<T>();
        }
      },
      variant);
}
} // namespace

std::string Json5ProtocolReader::takeStringValue(
    Json5Reader::Primitive&& primitive, std::string_view expected) {
  auto* s = std::get_if<std::string>(&primitive);
  if (!s) {
    throwError(
        fmt::format(
            "cannot parse `{}` as `{}`",
            getVariantTypePrettyName(primitive),
            expected));
  }
  return std::move(*s);
}

template <typename T>
T Json5ProtocolReader::parseInteger(std::string_view s) {
  auto number =
      Json5Reader::parseNumber(s, Json5Reader::FloatingPointPrecision::Double);
  auto* i = std::get_if<Json5Reader::Integer>(&number);
  if (i == nullptr) {
    throwError(fmt::format("invalid integer '{}'", s));
  }
  return convertTo<T>(i->value());
}

// ============================================================================
// Cursor Methods
// ============================================================================

void Json5ProtocolReader::setInput(const folly::io::Cursor& cursor) {
  reader_.setCursor(cursor);
}

void Json5ProtocolReader::setInput(const folly::IOBuf* buf) {
  reader_.setCursor(folly::io::Cursor(buf));
}

const folly::io::Cursor& Json5ProtocolReader::getCursor() const {
  return reader_.getCursor();
}

size_t Json5ProtocolReader::getCursorPosition() const {
  return reader_.getCursor().getCurrentPosition();
}

// ============================================================================
// Value Lifecycle Helpers
// ============================================================================

void Json5ProtocolReader::beginReadValue() {
  // Primitive keys are read as member names before this is reached, so this
  // is a struct or container key, which a member name cannot hold.
  if (containerStack_.inObjectMapExpectingKey()) {
    throwError("struct and container map keys must use the array form");
  }

  auto* map = containerStack_.mapState();
  if (map == nullptr ||
      map->encodedAs != CompoundTypeTracker::MapForm::KeyValueArray) {
    return;
  }

  switch (map->expectingToken) {
    case CompoundTypeTracker::MapToken::Key: {
      // We are reading a complex key, we need to read `{"key":`
      //
      //   [{"key":              [{"key":
      //    ^            =>              ^
      // before                       after
      reader_.readObjectBegin();
      auto name = reader_.readObjectName();
      if (name != "key") {
        throwError("expected 'key' in map array form, got '" + name + "'");
      }
      break;
    }
    case CompoundTypeTracker::MapToken::Value: {
      // We are reading a value after a complex key, we need to read `"value":`
      //
      //   [{"key": ..., "value":              [{"key": ..., "value":
      //                 ^             =>                            ^
      //              before                                      after
      auto name = reader_.readObjectName();
      if (name != "value") {
        throwError("expected 'value' in map array form, got '" + name + "'");
      }
      break;
    }
  }
}

void Json5ProtocolReader::endReadValue() {
  auto* map = containerStack_.mapState();
  if (map == nullptr) {
    return;
  }

  if (map->encodedAs == CompoundTypeTracker::MapForm::KeyValueArray &&
      map->expectingToken == CompoundTypeTracker::MapToken::Value) {
    // We read a value after a complex key, we need to read `}`:
    //
    //   [{"key": ..., "value": ...}         [{"key": ..., "value": ...},
    //                             ^    =>                               ^
    //                          before                                 after
    reader_.readObjectEnd();
  }

  containerStack_.toggleExpectingKeyValue();
}

std::optional<std::string> Json5ProtocolReader::tryReadObjectMapKey() {
  if (!containerStack_.inObjectMapExpectingKey()) {
    return std::nullopt;
  }
  auto name = reader_.readObjectName();
  containerStack_.toggleExpectingKeyValue();
  return name;
}

Json5Reader::Primitive Json5ProtocolReader::readPrimitiveOrMapKey(
    Json5Reader::FloatingPointPrecision precision) {
  Json5Reader::Primitive primitive;
  if (auto key = tryReadObjectMapKey()) {
    primitive = std::move(*key);
  } else {
    beginReadValue();
    primitive = reader_.readPrimitive(precision);
    endReadValue();
  }
  return primitive;
}

// ============================================================================
// Struct Methods
// ============================================================================

void Json5ProtocolReader::readStructBegin(std::string& /*name*/) {
  descend();
  beginReadValue();
  containerStack_.beginStruct();
  reader_.readObjectBegin();
}

void Json5ProtocolReader::readStructEnd() {
  containerStack_.endStruct();
  reader_.readObjectEnd();
  endReadValue();
  ascend();
}

void Json5ProtocolReader::readFieldBegin(
    std::string& name, protocol::TType& fieldType, std::int16_t& fieldId) {
  if (reader_.peekToken() == Json5Reader::Token::ObjectEnd) {
    fieldType = protocol::T_STOP;
    fieldId = 0;
    return;
  }

  {
    auto parsed = parseIdentifierString<std::int16_t>(reader_.readObjectName());
    name = std::move(parsed.name);
    fieldId = parsed.value.value_or(std::numeric_limits<std::int16_t>::min());
    fieldType = protocol::T_VOID;
  }

  if (reader_.peekToken() == Json5Reader::Token::Primitive &&
      folly::io::Cursor(reader_.getCursor()).read<char>() == 'n') {
    // Skip fields with null value
    [[maybe_unused]] bool isNull =
        std::holds_alternative<std::monostate>(reader_.readPrimitive());
    DCHECK(isNull);
    FOLLY_ATTR_MUSTTAIL return readFieldBegin(name, fieldType, fieldId);
  }
}

void Json5ProtocolReader::readFieldEnd() {}

// ============================================================================
// Map Methods
// ============================================================================

void Json5ProtocolReader::readMapBegin(
    protocol::TType& /*keyType*/,
    protocol::TType& /*valType*/,
    std::uint32_t& size) {
  descend();
  beginReadValue();
  size = std::numeric_limits<std::uint32_t>::max();

  auto token = reader_.peekToken();
  if (token == Json5Reader::Token::ObjectBegin) {
    reader_.readObjectBegin();
    containerStack_.beginMap(CompoundTypeTracker::MapForm::Object);
  } else if (token == Json5Reader::Token::ListBegin) {
    reader_.readListBegin();
    containerStack_.beginMap(CompoundTypeTracker::MapForm::KeyValueArray);
  } else {
    throwError("expected map (object or array)");
  }
}

void Json5ProtocolReader::readMapEnd() {
  auto* map = containerStack_.mapState();
  CHECK_THROW(map != nullptr, std::logic_error);

  // Map must end after a complete key-value pair (i.e., expecting next key).
  CHECK_THROW(
      map->expectingToken == CompoundTypeTracker::MapToken::Key,
      std::logic_error);

  auto encodedAs = map->encodedAs;
  containerStack_.endMap();
  if (encodedAs == CompoundTypeTracker::MapForm::KeyValueArray) {
    reader_.readListEnd();
  } else {
    reader_.readObjectEnd();
  }
  endReadValue();
  ascend();
}

// ============================================================================
// List Methods
// ============================================================================

void Json5ProtocolReader::readListBegin(
    protocol::TType& /*elemType*/, std::uint32_t& size) {
  descend();
  beginReadValue();
  size = std::numeric_limits<std::uint32_t>::max();
  containerStack_.beginList();
  reader_.readListBegin();
}

void Json5ProtocolReader::readListEnd() {
  containerStack_.endList();
  reader_.readListEnd();
  endReadValue();
  ascend();
}

// ============================================================================
// Set Methods
// ============================================================================

void Json5ProtocolReader::readSetBegin(
    protocol::TType& /*elemType*/, std::uint32_t& size) {
  descend();
  beginReadValue();
  size = std::numeric_limits<std::uint32_t>::max();
  containerStack_.beginSet();
  reader_.readListBegin();
}

void Json5ProtocolReader::readSetEnd() {
  containerStack_.endSet();
  reader_.readListEnd();
  endReadValue();
  ascend();
}

// ============================================================================
// Peek Methods
// ============================================================================

bool Json5ProtocolReader::peekMap() {
  auto* map = containerStack_.mapState();
  if (map == nullptr) {
    return false;
  }
  auto endToken = map->encodedAs == CompoundTypeTracker::MapForm::Object
      ? Json5Reader::Token::ObjectEnd
      : Json5Reader::Token::ListEnd;
  return reader_.peekToken() != endToken;
}

bool Json5ProtocolReader::peekList() {
  return reader_.peekToken() != Json5Reader::Token::ListEnd;
}

bool Json5ProtocolReader::peekSet() {
  return peekList();
}

// ============================================================================
// Skip Methods
// ============================================================================

void Json5ProtocolReader::skip(protocol::TType /*type*/, int depth) {
  if (depth >= FLAGS_thrift_protocol_max_depth) {
    protocol::TProtocolException::throwExceededDepthLimit();
  }

  // JSON5 is self-describing, so we ignore the TType parameter and dispatch
  // based on the next JSON5 token, similar to SimpleJSONProtocolReader::skip().
  auto token = reader_.peekToken();
  switch (token) {
    case Json5Reader::Token::ObjectBegin: {
      descend();
      reader_.readObjectBegin();
      while (reader_.peekToken() != Json5Reader::Token::ObjectEnd) {
        // Skip key
        reader_.readObjectName();
        // Skip value
        skip(protocol::T_VOID, depth + 1);
      }
      reader_.readObjectEnd();
      ascend();
      break;
    }
    case Json5Reader::Token::ListBegin: {
      descend();
      reader_.readListBegin();
      while (reader_.peekToken() != Json5Reader::Token::ListEnd) {
        skip(protocol::T_VOID, depth + 1);
      }
      reader_.readListEnd();
      ascend();
      break;
    }
    case Json5Reader::Token::Primitive: {
      reader_.readPrimitive();
      break;
    }
    case Json5Reader::Token::ObjectEnd:
    case Json5Reader::Token::ListEnd:
    default:
      throwError("unexpected token during skip");
  }
}

[[noreturn]] void Json5ProtocolReader::skipBytes(size_t /*bytes*/) {
  throwError("not supported");
}

// ============================================================================
// Enum Parsing Helpers
// ============================================================================

Json5ProtocolReader::IdentifierReadResult<>
Json5ProtocolReader::readEnumImpl() {
  // When reading enum as a map key in Object form, the enum value is the
  // JSON object property name (a string), not a JSON value.
  auto primitive = readPrimitiveOrMapKey();
  if (auto* i = std::get_if<Json5Reader::Integer>(&primitive)) {
    return {.name = {}, .value = convertTo<std::int32_t>(i->value())};
  }

  return parseIdentifierString(takeStringValue(std::move(primitive), "enum"));
}

template <typename T>
Json5ProtocolReader::IdentifierReadResult<T>
Json5ProtocolReader::parseIdentifierString(std::string_view s) {
  if (s.empty()) {
    throwError("Cannot parse empty string as identifier");
  }

  // bare identifier, e.g. "NAME"
  if (isIdentifier(s)) {
    return {.name = std::string(s), .value = std::nullopt};
  }

  // bare integer, e.g. "123" or "-0x7B"
  if (!s.ends_with(')')) {
    return {.name = {}, .value = parseInteger<T>(s)};
  }

  // "NAME (value)" or "(value)"
  auto parenOpen = s.find('(');
  if (parenOpen == std::string_view::npos) {
    throwError(fmt::format("invalid identifier: '{}'", s));
  }

  // Parsing NAME in "NAME (value)" or "(value)"
  auto name = folly::rtrimWhitespace(s.substr(0, parenOpen));
  if (!name.empty() && !isIdentifier(name)) {
    throwError(fmt::format("invalid identifier: '{}'", s));
  }

  // Parsing value in "NAME (value)" or "(value)"
  auto valueStr = s.substr(parenOpen + 1); // "value)"
  valueStr.remove_suffix(1); // "value"
  return {.name = std::string(name), .value = parseInteger<T>(valueStr)};
}

template Json5ProtocolReader::IdentifierReadResult<std::int32_t>
    Json5ProtocolReader::parseIdentifierString<std::int32_t>(std::string_view);
template Json5ProtocolReader::IdentifierReadResult<std::int16_t>
    Json5ProtocolReader::parseIdentifierString<std::int16_t>(std::string_view);

// ============================================================================
// Internal Value Reading Helpers
// ============================================================================

std::string Json5ProtocolReader::readStringValue() {
  return takeStringValue(readPrimitiveOrMapKey(), "string");
}

namespace {
// Like `s.find_first_of(chars) != npos`, which is a nested loop in libstdc++;
// `folly::qfind_first_of` uses SSE4.2.
bool containsAnyOf(std::string_view s, std::string_view chars) {
  return folly::qfind_first_of(
             folly::StringPiece(s), folly::StringPiece(chars)) !=
      std::string_view::npos;
}

std::string_view bareStringEncoding(std::string_view encoded) {
  return containsAnyOf(encoded, "-_") ? "base64url" : "base64";
}
} // namespace

std::string Json5ProtocolReader::readBinaryValue() {
  if (auto key = tryReadObjectMapKey()) {
    auto encoding = bareStringEncoding(*key);
    return decodeBinary(encoding, std::move(*key));
  }

  beginReadValue();
  std::string encoding;
  std::string encoded;
  if (reader_.peekToken() == Json5Reader::Token::ObjectBegin) {
    // {"utf-8": ...}, {"base64": ...} or {"base64url": ...}
    reader_.readObjectBegin();
    encoding = reader_.readObjectName();
    encoded = takeStringValue(reader_.readPrimitive(), "binary");
    reader_.readObjectEnd();
  } else {
    encoded = takeStringValue(reader_.readPrimitive(), "binary");
    encoding = bareStringEncoding(encoded);
  }
  endReadValue();
  return decodeBinary(encoding, std::move(encoded));
}

std::string Json5ProtocolReader::decodeBinary(
    std::string_view encoding, std::string encoded) {
  if (encoding == "utf-8") {
    return encoded;
  }
  if (encoding != "base64" && encoding != "base64url") {
    throwError(
        fmt::format(
            "Unsupported encoding type for binary object: {}", encoding));
  }
  // folly::base64URLDecode accepts both alphabets, with or without padding.
  if (containsAnyOf(encoded, encoding == "base64" ? "-_" : "+/")) {
    throwError(fmt::format("invalid character in {} binary value", encoding));
  }
  try {
    return folly::base64URLDecode(encoded);
  } catch (const folly::base64_decode_error& e) {
    throwError(fmt::format("invalid {} binary value: {}", encoding, e.what()));
  }
}

std::int64_t Json5ProtocolReader::readIntegralValue() {
  auto primitive = readPrimitiveOrMapKey();
  if (auto* i = std::get_if<Json5Reader::Integer>(&primitive)) {
    return i->value();
  }
  return parseInteger<std::int64_t>(
      takeStringValue(std::move(primitive), "integer"));
}

template <std::floating_point T>
T Json5ProtocolReader::readFloatingPointValue() {
  constexpr auto precision = std::is_same_v<T, float>
      ? Json5Reader::FloatingPointPrecision::Single
      : Json5Reader::FloatingPointPrecision::Double;

  auto primitive = readPrimitiveOrMapKey(precision);
  if (auto* s = std::get_if<std::string>(&primitive)) {
    primitive = Json5Reader::parseNumber(*s, precision);
  }

  if (auto* f = std::get_if<T>(&primitive)) {
    return *f;
  }
  if (auto* i = std::get_if<Json5Reader::Integer>(&primitive)) {
    // The magnitude is at most 2^63, so it rounds to at most 2^63 and
    // converting back is defined.
    T result = static_cast<T>(i->magnitude);
    if (static_cast<std::uint64_t>(result) != i->magnitude) {
      throwError("precision loss converting integer to floating point");
    }
    // Negating after the conversion keeps the sign of `-0`.
    return i->negative ? -result : result;
  }
  throwError(
      fmt::format(
          "cannot parse `{}` as `{}`",
          getVariantTypePrettyName(primitive),
          folly::pretty_name<T>()));
}

// ============================================================================
// Primitive Type Read Methods
// ============================================================================

void Json5ProtocolReader::readBool(bool& value) {
  auto parseBool = [](const std::string& s) {
    if (s == "true") {
      return true;
    }
    if (s == "false") {
      return false;
    }
    throwError("expected boolean, got '" + s + "'");
  };

  auto primitive = readPrimitiveOrMapKey();
  if (auto* b = std::get_if<bool>(&primitive)) {
    value = *b;
    return;
  }
  value = parseBool(takeStringValue(std::move(primitive), "bool"));
}

void Json5ProtocolReader::readBool(std::vector<bool>::reference value) {
  bool tmp = false;
  readBool(tmp);
  value = tmp;
}

void Json5ProtocolReader::readByte(std::int8_t& value) {
  value = convertTo<std::int8_t>(readIntegralValue());
}

void Json5ProtocolReader::readI16(std::int16_t& value) {
  value = convertTo<std::int16_t>(readIntegralValue());
}

void Json5ProtocolReader::readI32(std::int32_t& value) {
  value = convertTo<std::int32_t>(readIntegralValue());
}

void Json5ProtocolReader::readI64(std::int64_t& value) {
  value = readIntegralValue();
}

void Json5ProtocolReader::readFloat(float& value) {
  value = readFloatingPointValue<float>();
}

void Json5ProtocolReader::readDouble(double& value) {
  value = readFloatingPointValue<double>();
}

// ============================================================================
// Binary/String Read Methods
// ============================================================================

void Json5ProtocolReader::readBinary(std::unique_ptr<folly::IOBuf>& str) {
  str = folly::IOBuf::copyBuffer(readBinaryValue());
}

void Json5ProtocolReader::readBinary(folly::IOBuf& str) {
  str = folly::IOBuf(folly::IOBuf::CopyBufferOp{}, readBinaryValue());
}

} // namespace apache::thrift::json5::detail
