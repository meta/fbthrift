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

#include <cstddef>
#include <cstdlib>
#include <memory_resource>
#include <string>

#include <gtest/gtest.h>

namespace {

using apache::thrift::pmr::ArenaResource;
using apache::thrift::pmr::isArenaResource;

class LeakyResource : public ArenaResource {
 private:
  void* do_allocate(std::size_t bytes, std::size_t) override {
    return std::malloc(bytes);
  }
  void do_deallocate(void*, std::size_t, std::size_t) override {}
  bool do_is_equal(
      const std::pmr::memory_resource& other) const noexcept override {
    return this == &other;
  }
};

class DerivedFromLeakyResource final : public LeakyResource {};

class DerivedFromMonotonicBufferResource final
    : public std::pmr::monotonic_buffer_resource {};

TEST(ArenaResourceTest, MonotonicBufferResourceIsAnArena) {
  std::pmr::monotonic_buffer_resource resource;
  EXPECT_TRUE(isArenaResource(resource));
}

TEST(ArenaResourceTest, SubclassOfMonotonicBufferResourceIsNotAnArena) {
  DerivedFromMonotonicBufferResource resource;
  EXPECT_FALSE(isArenaResource(resource));
}

TEST(ArenaResourceTest, ResourcesDerivedFromArenaResourceAreArenas) {
  LeakyResource leakyResource;
  DerivedFromLeakyResource derived;
  EXPECT_TRUE(isArenaResource(leakyResource));
  EXPECT_TRUE(isArenaResource(derived));
}

TEST(ArenaResourceTest, OtherStandardResourcesAreNotArenas) {
  std::pmr::synchronized_pool_resource synchronizedPool;
  std::pmr::unsynchronized_pool_resource unsynchronizedPool;
  EXPECT_FALSE(isArenaResource(*std::pmr::new_delete_resource()));
  EXPECT_FALSE(isArenaResource(*std::pmr::null_memory_resource()));
  EXPECT_FALSE(isArenaResource(synchronizedPool));
  EXPECT_FALSE(isArenaResource(unsynchronizedPool));
}

TEST(ArenaResourceTest, AllocatorOverloadChecksTheAllocatorResource) {
  std::pmr::monotonic_buffer_resource monotonicBuffer;
  LeakyResource leakyResource;
  std::pmr::unsynchronized_pool_resource pool;
  const std::pmr::string onArena(&monotonicBuffer);
  EXPECT_TRUE(isArenaResource(onArena.get_allocator()));
  EXPECT_TRUE(isArenaResource(
      std::pmr::polymorphic_allocator<std::byte>(&monotonicBuffer)));
  EXPECT_TRUE(
      isArenaResource(std::pmr::polymorphic_allocator<int>(&leakyResource)));
  EXPECT_FALSE(
      isArenaResource(std::pmr::polymorphic_allocator<std::byte>(&pool)));
  EXPECT_FALSE(isArenaResource(std::pmr::polymorphic_allocator<std::byte>()));
}

} // namespace
