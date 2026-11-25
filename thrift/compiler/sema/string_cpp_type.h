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

#pragma once

#include <cctype>
#include <string_view>

namespace apache::thrift::compiler {

// Matches C++ tokens, not whitespace-stripped text: `IO Buf` is not `IOBuf`,
// and `: :` is not a scope-resolution token.
inline bool is_supported_string_cpp_type(std::string_view name) {
  auto consume = [&](std::string_view token) {
    while (!name.empty() &&
           std::isspace(static_cast<unsigned char>(name.front()))) {
      name.remove_prefix(1);
    }
    if (!name.starts_with(token)) {
      return false;
    }
    name.remove_prefix(token.size());
    return true;
  };
  consume("::");
  bool matched;
  if (consume("folly")) {
    matched = consume("::") && consume("IOBuf");
  } else if (consume("apache")) {
    matched = consume("::") && consume("thrift") && consume("::") &&
        consume("IOBufChain");
  } else if (consume("std") && consume("::")) {
    if (consume("pmr")) {
      matched = consume("::") && consume("string");
    } else {
      matched = consume("unique_ptr") && consume("<");
      consume("::");
      matched = matched && consume("folly") && consume("::") &&
          consume("IOBuf") && consume(">");
    }
  } else {
    matched = false;
  }
  consume("");
  return matched && name.empty();
}

} // namespace apache::thrift::compiler
