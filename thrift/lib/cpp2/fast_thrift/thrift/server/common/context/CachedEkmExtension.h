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

#include <cstddef>
#include <utility>
#include <vector>

#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/ExtensionSlots.h>
#include <thrift/lib/cpp2/server/Cpp2ConnContext.h>

namespace apache::thrift::fast_thrift::thrift {

struct CachedEkmExtension {
  EXTENSION_ID(thrift_cached_ekms);
  using ConnState = std::vector<apache::thrift::detail::EkmInfo>;

  static void destroyConnState(ConnState* state) noexcept { delete state; }
};

inline ExtensionLayout withoutCachedEkmExtension(
    const ExtensionLayout& layout) {
  ExtensionLayoutBuilder builder;
  const auto* entries = layout.entries();
  for (std::size_t i = 0; i < layout.entryCount(); ++i) {
    if (entries[i].id != CachedEkmExtension::kId) {
      builder.add(entries[i].id, entries[i].deleter);
    }
  }
  return std::move(builder).build();
}

} // namespace apache::thrift::fast_thrift::thrift
