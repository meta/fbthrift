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

#include <thrift/lib/cpp2/protocol/detail/JsonUtils.h>

#include <algorithm>
#include <array>

namespace apache::thrift::json5::detail {

namespace {
// Whether each byte can be part of an identifier: one lookup per byte instead
// of a chain of range comparisons.
constexpr std::array<bool, 256> makeIdentifierCharTable(bool allowDollarSign) {
  std::array<bool, 256> table{};
  for (int c = 0; c < 256; ++c) {
    table[c] = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
        (c >= '0' && c <= '9') || c == '_' || (allowDollarSign && c == '$');
  }
  return table;
}
constexpr auto kIdentifierChars = makeIdentifierCharTable(false);
constexpr auto kIdentifierCharsWithDollarSign = makeIdentifierCharTable(true);
} // namespace

bool isIdentifier(std::string_view sv, bool allowDollarSign) {
  if (sv.empty() || (sv[0] >= '0' && sv[0] <= '9')) {
    return false;
  }
  const auto& table =
      allowDollarSign ? kIdentifierCharsWithDollarSign : kIdentifierChars;
  return std::all_of(sv.begin(), sv.end(), [&table](char c) {
    return table[static_cast<unsigned char>(c)];
  });
}

bool isAsciiWithNothingToEscape(std::string_view s) {
  // No early exit (`&=`, not `&&`): it stops clang from vectorizing the loop,
  // and made writing a 10 KB string 12-20x slower in ProtocolBench. It would
  // only help strings that need escaping, where escaping costs 9-40x more than
  // this check (16 B to 10 KB), so it saved at most 2%.
  bool result = true;
  for (char c : s) {
    auto uc = static_cast<unsigned char>(c);
    result &= 32 <= uc && uc <= 127 && uc != '"' && uc != '\\';
  }
  return result;
}

} // namespace apache::thrift::json5::detail
