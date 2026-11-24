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

#include <folly/CPortability.h>
#include <folly/memory/MemoryResource.h>

#if FOLLY_HAS_MEMORY_RESOURCE

#include <memory_resource>

namespace apache::thrift::pmr {

/**
 * Base class for memory resources that reclaim all the memory they hand out
 * when released or destroyed, like std::pmr::monotonic_buffer_resource. Thrift
 * may skip destroying @cpp.Arena values allocated from such a resource.
 */
class FOLLY_EXPORT ArenaResource : public std::pmr::memory_resource {
 public:
  // Out of line, so the type_info is emitted once and dynamic_cast works across
  // shared libraries.
  ~ArenaResource() override;
};

/**
 * True for std::pmr::monotonic_buffer_resource (but not classes derived from
 * it, which may override do_deallocate) and for classes derived from
 * ArenaResource.
 *
 * This is necessary but not sufficient for skipping a destructor: every member,
 * recursively, must also free only memory that came from this resource. Values
 * holding an IOBuf, a @cpp.Ref or @cpp.Box field, or a std::string still need
 * destroying.
 */
bool isArenaResource(const std::pmr::memory_resource& resource) noexcept;

template <class T>
bool isArenaResource(const std::pmr::polymorphic_allocator<T>& alloc) noexcept {
  return isArenaResource(*alloc.resource());
}

} // namespace apache::thrift::pmr

#endif // FOLLY_HAS_MEMORY_RESOURCE
