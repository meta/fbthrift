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

#include <thrift/lib/cpp2/protocol/detail/JsonWriter.h>

#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>
#include <string>
#include <type_traits>
#include <fmt/compile.h>
#include <fmt/format.h>
#include <folly/CPortability.h>
#include <folly/Exception.h>
#include <folly/json.h>
#include <thrift/lib/cpp2/protocol/detail/JsonUtils.h>

namespace apache::thrift::json5::detail {

// -- helpers ----------------------------------------------------------

bool JsonWriter::inContainer(ContainerType type) const {
  return !containerStack_.empty() && containerStack_.back() == type;
}

std::uint32_t JsonWriter::appendNewlineAndIndent() {
  std::uint32_t n = 0;
  if (options_.indentWidth > 0) {
    out_.write('\n');
    n += 1;
    size_t spaces = containerStack_.size() * options_.indentWidth;
    out_.ensure(spaces);
    memset(out_.writableData(), ' ', spaces);
    out_.append(spaces);
    n += spaces;
  }
  return n;
}

// Inline, like beginValue and endValue, so that each scalar writer keeps the
// output position in registers across the comma and the value.
FOLLY_ALWAYS_INLINE std::uint32_t JsonWriter::writePendingComma() {
  if (!commaPending_) {
    return 0;
  }
  out_.write(',');
  return 1;
}

FOLLY_ALWAYS_INLINE void JsonWriter::endValue() {
  commaPending_ = true;
}

char JsonWriter::getOpenBracket(ContainerType type) {
  if (type == ContainerType::List) {
    return '[';
  }
  CHECK_THROW(type == ContainerType::Object, std::logic_error);
  return '{';
}
char JsonWriter::getCloseBracket(ContainerType type) {
  if (type == ContainerType::List) {
    return ']';
  }
  CHECK_THROW(type == ContainerType::Object, std::logic_error);
  return '}';
}

bool JsonWriter::trailingComma(ContainerType type) const {
  if (type == ContainerType::List) {
    return options_.listTrailingComma;
  }
  CHECK_THROW(type == ContainerType::Object, std::logic_error);
  return options_.objectTrailingComma;
}

// Called before writing any JSON value to insert indentation if needed.
FOLLY_ALWAYS_INLINE std::uint32_t JsonWriter::beginValue() {
  std::uint32_t n = 0;
  if (inContainer(ContainerType::List)) {
    // Only add indentation when writing value in a list. Why? If it's in a
    // object, we should indent when writing object name, not when writing
    // object value.
    n += writePendingComma();
    n += appendNewlineAndIndent();
  } else if (inContainer(ContainerType::Object)) {
    CHECK_THROW(lastWrittenValueWasObjectName_, std::runtime_error);
    lastWrittenValueWasObjectName_ = false;
  }
  return n;
}

// -- containers -------------------------------------------------------

std::uint32_t JsonWriter::openContainer(ContainerType type) {
  std::uint32_t n = 0;
  n += beginValue();
  containerStack_.push_back(type);
  out_.write(getOpenBracket(type));
  n += 1;
  commaPending_ = false;
  return n;
}

std::uint32_t JsonWriter::closeContainer(ContainerType type) {
  // Verify the container type matches the corresponding openContainer call.
  CHECK_THROW(inContainer(type), std::runtime_error);

  // A container must not be closed immediately after an object name (a value
  // is expected first).
  CHECK_THROW(!lastWrittenValueWasObjectName_, std::runtime_error);

  std::uint32_t n = 0;
  const bool empty = !commaPending_;
  containerStack_.pop_back();
  if (!empty) {
    if (trailingComma(type)) {
      out_.write(',');
      n += 1;
    }
    n += appendNewlineAndIndent();
  }
  out_.write(getCloseBracket(type));
  n += 1;
  endValue();
  return n;
}

// -- scalars ----------------------------------------------------------

template <std::floating_point T>
std::uint32_t JsonWriter::writeFloatingPoint(T t) {
  CHECK_THROW(options_.allowNanInf || std::isfinite(t), std::invalid_argument);
  std::uint32_t n = 0;
  n += beginValue();
  if (std::isnan(t)) {
    auto sp = std::signbit(t) ? folly::StringPiece("-NaN")
                              : folly::StringPiece("NaN");
    n += sp.size();
    out_.push(sp);
  } else if (std::isinf(t)) {
    auto sp = t > 0 ? folly::StringPiece("Infinity")
                    : folly::StringPiece("-Infinity");
    n += sp.size();
    out_.push(sp);
  } else {
    fmt::memory_buffer buf;
    fmt::format_to(fmt::appender(buf), FMT_COMPILE("{}"), t);
    // Re-reads `buf` on every call: writing to it below can reallocate it.
    auto formatted = [&] { return std::string_view(buf.data(), buf.size()); };
    if (std::is_same_v<T, float> && !options_.useSimpleJsonFloatNotation &&
        std::abs(t) >= 1e7f && formatted().find('e') == std::string::npos) {
      // fmt prints the shortest digits that read back as the same float, so
      // whatever comes after those digits is padding zeros.
      size_t digits = formatted().find_last_not_of('0') + 1 - std::signbit(t);
      buf.clear();
      fmt::format_to(fmt::appender(buf), "{:.{}e}", t, digits - 1);
    }
    if (!options_.wholeNumberFloatWithoutFraction &&
        formatted().find_first_of(".eE") == std::string::npos) {
      // The shortest round-trip form of a whole-valued double has no '.', 'e',
      // or 'E' (e.g. "100000"), which would read back as an integer, so append
      // ".0" to keep it a floating-point literal. Values that already carry a
      // fraction or exponent are left untouched.
      //
      // fmt::format("{:#}", t) would also force a decimal point, but it emits
      // "1.e-05" for scientific-notation values, which is invalid JSON.
      buf.push_back('.');
      buf.push_back('0');
    }
    out_.push(folly::StringPiece(formatted()));
    n += buf.size();
  }
  endValue();
  return n;
}

template std::uint32_t JsonWriter::writeFloatingPoint(float);
template std::uint32_t JsonWriter::writeFloatingPoint(double);

std::uint32_t JsonWriter::writeIntegral(std::integral auto t) {
  std::uint32_t n = beginValue();
  // Formats straight into the output, with room for a sign and all digits.
  out_.ensure(std::numeric_limits<decltype(t)>::digits10 + 2);
  auto* begin = reinterpret_cast<char*>(out_.writableData());
  const auto size = static_cast<std::uint32_t>(
      fmt::format_to(begin, FMT_COMPILE("{}"), t) - begin);
  out_.append(size);
  endValue();
  return n + size;
}

template std::uint32_t JsonWriter::writeIntegral(std::int8_t);
template std::uint32_t JsonWriter::writeIntegral(std::int16_t);
template std::uint32_t JsonWriter::writeIntegral(std::int32_t);
template std::uint32_t JsonWriter::writeIntegral(std::int64_t);

std::uint32_t JsonWriter::writeBool(bool b) {
  std::uint32_t n = beginValue();
  auto s = b ? folly::StringPiece("true") : folly::StringPiece("false");
  out_.push(s);
  endValue();
  return n + static_cast<std::uint32_t>(s.size());
}

// -- strings ----------------------------------------------------------

std::uint32_t JsonWriter::writeQuotedString(std::string_view s) {
  if (isAsciiWithNothingToEscape(s)) {
    out_.write('"');
    out_.push(folly::StringPiece(s));
    out_.write('"');
    return static_cast<std::uint32_t>(s.size() + 2);
  }
  static const folly::json::serialization_opts kOpts{.validate_utf8 = true};
  escaped_.clear();
  folly::json::escapeString(s, escaped_, kOpts);
  out_.push(folly::StringPiece(escaped_));
  return static_cast<std::uint32_t>(escaped_.size());
}

std::uint32_t JsonWriter::writeObjectName(std::string_view s) {
  CHECK_THROW(inContainer(ContainerType::Object), std::runtime_error);

  // Consecutive object names without an intervening value are not allowed.
  CHECK_THROW(!lastWrittenValueWasObjectName_, std::runtime_error);

  std::uint32_t n = 0;
  lastWrittenValueWasObjectName_ = true;
  n += writePendingComma();
  n += appendNewlineAndIndent();
  const std::string_view sep = options_.indentWidth > 0 ? ": " : ":";
  if (!isIdentifier(s, /*allowDollarSign=*/true)) {
    n += writeQuotedString(s);
    out_.push(folly::StringPiece(sep));
    return n + static_cast<std::uint32_t>(sep.size());
  }
  // Member names are short, so write the name and separator into one
  // reserved range rather than appending each piece.
  const bool quoted = !options_.unquoteObjectName;
  const std::size_t size = s.size() + (quoted ? 2 : 0) + sep.size();
  out_.ensure(size);
  char* p = reinterpret_cast<char*>(out_.writableData());
  if (quoted) {
    *p++ = '"';
  }
  p = std::copy(s.begin(), s.end(), p);
  if (quoted) {
    *p++ = '"';
  }
  std::copy(sep.begin(), sep.end(), p);
  out_.append(size);
  return n + static_cast<std::uint32_t>(size);
}

std::uint32_t JsonWriter::writeString(std::string_view s) {
  std::uint32_t n = 0;
  n += beginValue();
  n += writeQuotedString(s);
  endValue();
  return n;
}

std::uint32_t JsonWriter::writeNull() {
  std::uint32_t n = 0;
  n += beginValue();
  out_.push(folly::StringPiece("null"));
  n += 4;
  endValue();
  return n;
}

} // namespace apache::thrift::json5::detail
