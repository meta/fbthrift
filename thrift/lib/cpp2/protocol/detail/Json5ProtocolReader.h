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

/**
 * @file Json5ProtocolReader.h
 * @brief JSON/JSON5 deserialization for Thrift values.
 *
 * Json5ProtocolReader provides a Thrift protocol reader interface that parses
 * JSON5 or "basic" JSON input:
 * - "JSON5" refers to the "JSON5 Data Interchange Format (version 1.0.0/March
 *   2018)" as defined in https://spec.json5.org/. It is a subset of ECMAScript
 *   5.1 (https://262.ecma-international.org/5.1/).
 * - "Basic JSON" refers to the "JavaScript Object Notation (JSON) Data
 *   Interchange Format" as defined in RFC7159
 *   (https://datatracker.ietf.org/doc/html/rfc7159)
 *
 * The API of this class is modeled after SimpleJSONProtocolReader.
 *
 * ## Current status
 *
 * This is EXPERIMENTAL. The output is unstable and there are missing features.
 * APIs may break with no prior notice. USE AT YOUR OWN RISK (preferably NOT IN
 * PRODUCTION).
 *
 * ## Compatibility Features
 *
 * To support backward compatibility with existing JSON formats, this reader
 * accepts multiple input formats for certain types:
 *
 * ### Bool Fields
 * - Accepts both true/false bare literals and quoted strings: `true`, `"true"`
 *
 * ### Integer Fields
 * - Accepts both JSON numbers and quoted strings: `42` and `"42"`
 *
 * ### Floating-Point Fields
 * - Accepts both JSON numbers and quoted strings: `3.14` and `"3.14"`
 * - Supports NaN and Infinity as bare literals or quoted: `NaN`, `"NaN"`,
 *   `Infinity`, `"Infinity"`, `-Infinity`, `"-Infinity"`
 *
 * ### Enum Fields
 * - Accepts: "NAME (value)", "NAME", "(value)", or bare integer
 * - Examples: `"ONE (1)"`, `"ONE"`, `"(1)"`, `1`
 *
 * ### Map Fields
 * - Object form: `{"key1": value1, "key2": value2}`
 * - Array form: `[{"key": k1, "value": v1}, {"key": k2, "value": v2}]`
 * - Struct and container keys must use the array form, since JSON5 member
 *   names are strings.
 */

#pragma once

#include <concepts>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include <fmt/core.h>
#include <folly/Conv.h>
#include <folly/Exception.h>
#include <folly/io/Cursor.h>
#include <folly/io/IOBuf.h>
#include <thrift/lib/cpp/protocol/TProtocol.h>
#include <thrift/lib/cpp/util/EnumUtils.h>
#include <thrift/lib/cpp2/detail/Enum.h>
#include <thrift/lib/cpp2/protocol/Protocol.h>
#include <thrift/lib/cpp2/protocol/detail/CompoundTypeTracker.h>
#include <thrift/lib/cpp2/protocol/detail/JsonReader.h>

namespace apache::thrift::json5::detail {

/**
 * Resolves enum names and values for Json5ProtocolReader::readEnum when the
 * enum is only known at runtime (e.g. dynamic Thrift). `findName` returns an
 * empty string for unknown values.
 */
template <typename T>
concept EnumResolver =
    requires(const T& resolver, std::int32_t value, std::string_view name) {
      { resolver.findName(value) } -> std::convertible_to<std::string_view>;
      { resolver.findValue(name) } -> std::same_as<std::optional<std::int32_t>>;
    };

/**
 * Json5ProtocolReader provides a Thrift protocol reader interface that parses
 * JSON or JSON5 format. The implementation is based on JsonReader - it forwards
 * function calls to JsonReader, which reads directly from a Cursor.
 *
 * Key features:
 * - API similar to SimpleJSONProtocolReader
 * - Takes input from folly::io::Cursor or folly::IOBuf
 *
 * Unavailable metadata uses sentinel values (consistent with
 * SimpleJSONProtocolReader):
 *   - field id: std::numeric_limits<std::int16_t>::min()
 *   - field type: protocol::T_VOID
 *   - container size: std::numeric_limits<std::uint32_t>::max()
 */
class Json5ProtocolReader final
    : public ::apache::thrift::detail::ProtocolBase {
 public:
  explicit Json5ProtocolReader(
      ExternalBufferSharing /*sharing*/ = COPY_EXTERNAL_BUFFER /* ignored */) {}

  static constexpr bool kUsesFieldNames() { return true; }
  static constexpr bool kOmitsContainerSizes() { return true; }
  static constexpr bool kOmitsStringSizes() { return true; }
  static constexpr bool kOmitsContainerElemTypes() { return true; }
  static constexpr bool kHasDeferredRead() { return false; }
  static constexpr bool kCanReadStringView() { return false; }

  static constexpr ProtocolType protocolType() {
    return ProtocolType::T_JSON5_PROTOCOL;
  }

  void setInput(const folly::io::Cursor& cursor);
  void setInput(const folly::IOBuf* buf);

  void readStructBegin(std::string& name);
  void readStructEnd();
  void readFieldBegin(
      std::string& name, protocol::TType& fieldType, std::int16_t& fieldId);
  void readFieldEnd();

  void readMapBegin(
      protocol::TType& keyType, protocol::TType& valType, std::uint32_t& size);
  void readMapEnd();
  void readListBegin(protocol::TType& elemType, std::uint32_t& size);
  void readListEnd();
  void readSetBegin(protocol::TType& elemType, std::uint32_t& size);
  void readSetEnd();

  void readBool(bool& value);
  void readBool(std::vector<bool>::reference value);
  void readByte(std::int8_t& value);
  void readI16(std::int16_t& value);
  void readI32(std::int32_t& value);
  void readI64(std::int64_t& value);
  void readFloat(float& value);
  void readDouble(double& value);

  template <typename StrType>
  void readString(StrType& str) {
    str = readStringValue();
  }
  template <typename StrType>
  void readBinary(StrType& str) {
    if constexpr (std::is_assignable_v<StrType&, std::string>) {
      str = readBinaryValue();
    } else {
      // Custom binary types, which SimpleJSONProtocolReader fills the same way:
      // without calling append() for an empty value, which some types reject.
      auto value = readBinaryValue();
      str.clear();
      if (!value.empty()) {
        str.append(value.data(), value.size());
      }
    }
  }
  void readBinary(std::unique_ptr<folly::IOBuf>& str);
  void readBinary(folly::IOBuf& str);

  // Enum support: parses "NAME (value)", "NAME", "(value)", or integer
  template <typename EnumType>
  void readEnum(EnumType& value);

  template <class Int>
  void readEnum(
      std::int32_t& value, ::apache::thrift::detail::st::enum_find<Int>& map);

  template <EnumResolver Resolver>
  void readEnum(std::int32_t& value, const Resolver& resolver);

  bool peekMap();
  bool peekList();
  bool peekSet();

  void skip(protocol::TType type, int depth = 0);
  [[noreturn]] void skipBytes(size_t bytes);

  static constexpr std::size_t fixedSizeInContainer(protocol::TType) {
    return 0;
  }

  const folly::io::Cursor& getCursor() const;
  size_t getCursorPosition() const;

 private:
  // Invoked before reading any value. For maps with complex keys (KeyValueArray
  // form), reads `{"key":` when expecting a key, or `"value":` when expecting
  // a value.
  void beginReadValue();

  // Invoked after reading any value. For maps with complex keys, reads the
  // closing `}` after a value completes a key-value pair. Also toggles the map
  // state between expecting a key and expecting a value.
  void endReadValue();

  // Returns the object key name if we are inside an object-form map expecting
  // a key, otherwise returns nullopt. Toggles the map state when a key is read.
  std::optional<std::string> tryReadObjectMapKey();

  // Like reader_.readPrimitive, but when an object-form map expects a key,
  // reads the key instead and returns it as a string.
  Json5Reader::Primitive readPrimitiveOrMapKey(
      Json5Reader::FloatingPointPrecision precision =
          Json5Reader::FloatingPointPrecision::Double);

  [[noreturn]] static void throwError(std::string_view message);

  // Like folly::to, but reports conversion failures through throwError.
  template <typename To, typename From>
  static To convertTo(const From& from);

  // Returns the string alternative of `primitive`; any other alternative is a
  // parse error saying it cannot be parsed as `expected`.
  static std::string takeStringValue(
      Json5Reader::Primitive&& primitive, std::string_view expected);

  // Parses all of `s` as a JSON5 integer that fits in T.
  template <typename T>
  static T parseInteger(std::string_view s);

  std::int64_t readIntegralValue();

  template <std::floating_point T>
  T readFloatingPointValue();

  std::string readStringValue();
  std::string readBinaryValue();
  // `encoding` is "utf-8", "base64" or "base64url".
  static std::string decodeBinary(
      std::string_view encoding, std::string encoded);

  template <typename T = std::int32_t>
  struct IdentifierReadResult {
    std::string name;
    std::optional<T> value;
  };

  // Parse enum from JSON input, returning parsed name and optional integer
  // value. The caller is responsible for resolving the name to an enum value.
  IdentifierReadResult<> readEnumImpl();

  // Parse a string in "NAME (value)", "(value)", or "NAME" format.
  // Used for both enum values and field identifiers.
  template <typename T = std::int32_t>
  static IdentifierReadResult<T> parseIdentifierString(std::string_view s);

  CompoundTypeTracker containerStack_;
  Json5Reader reader_;
};

template <typename EnumType>
void Json5ProtocolReader::readEnum(EnumType& value) {
  std::int32_t intValue;
  readEnum(
      intValue, ::apache::thrift::detail::st::enum_find_instance<EnumType>());
  value = static_cast<EnumType>(intValue);
}

template <class Int>
void Json5ProtocolReader::readEnum(
    std::int32_t& value, ::apache::thrift::detail::st::enum_find<Int>& map) {
  struct EnumFindResolver {
    ::apache::thrift::detail::st::enum_find<Int>& map;

    std::string_view findName(std::int32_t enumValue) const {
      return map.find_name(Int(enumValue), map).result;
    }
    std::optional<std::int32_t> findValue(std::string_view name) const {
      if (auto r = map.find_value(name, map)) {
        return std::int32_t(r.result);
      }
      return std::nullopt;
    }
  };
  readEnum(value, EnumFindResolver{map});
}

template <EnumResolver Resolver>
void Json5ProtocolReader::readEnum(
    std::int32_t& value, const Resolver& resolver) {
  auto [providedName, providedValue] = readEnumImpl();
  std::string_view localSchemaName;
  std::optional<std::int32_t> localSchemaValue;

  if (providedValue.has_value()) {
    localSchemaName = resolver.findName(*providedValue);
  }

  if (!providedName.empty()) {
    localSchemaValue = resolver.findValue(providedName);
  }

  if (providedName.empty() && providedValue.has_value()) {
    // Enum is "(enum-value)", use provided enum value directly.
    value = providedValue.value();
    return;
  }

  if (!providedName.empty() && !providedValue.has_value()) {
    // Enum is "enum-name", use the value found in local schema.
    if (!localSchemaValue.has_value()) {
      throwError(
          fmt::format(
              "enum name '{}' does not exist in local schema", providedName));
    }
    value = localSchemaValue.value();
    return;
  }

  CHECK_THROW(
      !providedName.empty() && providedValue.has_value(), std::logic_error);

  // Enum is "enum-name (enum-value)" since both provided name and provided
  // value has value.
  if (localSchemaName.empty() && !localSchemaValue.has_value()) {
    // Both provided name and value can't be found from the local schema.
    // Accept by considering this to be a newly added enum value that reader's
    // schema is not up to date yet.
    value = providedValue.value();
    return;
  }

  if (!localSchemaName.empty() && localSchemaName != providedName) {
    throwError(
        fmt::format(
            "enum '{} ({})' does not match IDL name {}",
            providedName,
            *providedValue,
            localSchemaName));
  }

  if (localSchemaValue.has_value() &&
      localSchemaValue != providedValue.value()) {
    throwError(
        fmt::format(
            "enum '{} ({})' does not match IDL value {}",
            providedName,
            *providedValue,
            *localSchemaValue));
  }

  // both provided name and value found in local schema and match.
  value = providedValue.value();
}

static_assert(ThriftProtocolReader<Json5ProtocolReader>);

} // namespace apache::thrift::json5::detail
