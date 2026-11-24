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

#include <thrift/lib/cpp2/pmr/ArenaResource.h>

#if FOLLY_HAS_MEMORY_RESOURCE

#include <typeinfo>

namespace apache::thrift::pmr {

ArenaResource::~ArenaResource() = default;

bool isArenaResource(const std::pmr::memory_resource& resource) noexcept {
  return typeid(resource) == typeid(std::pmr::monotonic_buffer_resource) ||
      dynamic_cast<const ArenaResource*>(&resource) != nullptr;
}

} // namespace apache::thrift::pmr

#endif // FOLLY_HAS_MEMORY_RESOURCE
