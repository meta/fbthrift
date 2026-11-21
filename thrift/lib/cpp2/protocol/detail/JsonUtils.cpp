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

namespace apache::thrift::json5::detail {

bool isIdentifier(std::string_view sv, bool allowDollarSign) {
  if (sv.empty() || (sv[0] >= '0' && sv[0] <= '9')) {
    return false;
  }
  auto isIdentifierChar = [allowDollarSign](char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
        (c >= '0' && c <= '9') || c == '_' || (allowDollarSign && c == '$');
  };
  return std::all_of(sv.begin(), sv.end(), isIdentifierChar);
}

bool isAsciiWithNothingToEscape(std::string_view s) {
  // No early exit (`&=`, not `&&`): it stops clang from vectorizing the loop,
  // and made writing a 10 KB string 12-20x slower in ProtocolBench. It would
  // only help strings that need escaping, where escaping costs 9-40x more than
  // this check (16 B to 10 KB), so it saved at most 2%.
  bool result = true;
  for (char c : s) {
    result &= 32 <= c && c <= 127 && c != '"' && c != '\\';
  }
  return result;
}

} // namespace apache::thrift::json5::detail
