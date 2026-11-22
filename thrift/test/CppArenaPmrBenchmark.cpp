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

// Teardown of `@cpp.Arena` values: destroying the value, and releasing the
// arena it was built on, if any.

#include <cstddef>
#include <memory_resource>
#include <optional>
#include <string>
#include <tuple>
#include <utility>

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <thrift/test/gen-cpp2/CppArenaPmrTest_types.h>

namespace apache::thrift::test::arena_pmr {
namespace {

// Too long for the small string optimization, so every string allocates.
std::string text(std::size_t i) {
  return std::to_string(i) + std::string(32, 'x');
}

void fill(Containers& c, std::size_t n) {
  for (std::size_t i = 0; i < n; ++i) {
    c.tags()->emplace(text(i));
  }
}

void fill(WholeStruct& s, std::size_t n) {
  for (std::size_t i = 0; i < n; ++i) {
    auto& values = s.groups()
                       ->emplace(
                           std::piecewise_construct,
                           std::forward_as_tuple(text(i)),
                           std::forward_as_tuple())
                       .first->second;
    for (std::size_t j = 0; j < 4; ++j) {
      values.emplace_back(text(j));
    }
  }
}

void fill(StructElements& s, std::size_t n) {
  s.arenas()->reserve(n);
  for (std::size_t i = 0; i < n; ++i) {
    ArenaInner& inner = s.arenas()->emplace_back();
    inner.name() = text(i);
    inner.tags()->emplace_back(text(0));
    inner.tags()->emplace_back(text(1));
  }
}

template <class T>
void teardown(std::size_t iters, std::size_t n, bool onArena) {
  for (std::size_t i = 0; i < iters; ++i) {
    std::optional<std::pmr::monotonic_buffer_resource> arena;
    std::optional<T> value;
    BENCHMARK_SUSPEND {
      std::pmr::memory_resource* resource =
          onArena ? &arena.emplace() : std::pmr::get_default_resource();
      fill(value.emplace(resource), n);
    }
    value.reset();
    arena.reset();
  }
}

void setOfStrings(std::size_t iters, std::size_t n, bool onArena) {
  teardown<Containers>(iters, n, onArena);
}

// map<string, list<string>>, with 4 strings per list
void mapOfLists(std::size_t iters, std::size_t n, bool onArena) {
  teardown<WholeStruct>(iters, n, onArena);
}

// list<ArenaInner>, each with a string and a list of 2 strings
void listOfStructs(std::size_t iters, std::size_t n, bool onArena) {
  teardown<StructElements>(iters, n, onArena);
}

BENCHMARK_NAMED_PARAM(setOfStrings, heap_16, 16, false)
BENCHMARK_RELATIVE_NAMED_PARAM(setOfStrings, arena_16, 16, true)
BENCHMARK_NAMED_PARAM(setOfStrings, heap_1024, 1024, false)
BENCHMARK_RELATIVE_NAMED_PARAM(setOfStrings, arena_1024, 1024, true)
BENCHMARK_DRAW_LINE();
BENCHMARK_NAMED_PARAM(mapOfLists, heap_16, 16, false)
BENCHMARK_RELATIVE_NAMED_PARAM(mapOfLists, arena_16, 16, true)
BENCHMARK_NAMED_PARAM(mapOfLists, heap_1024, 1024, false)
BENCHMARK_RELATIVE_NAMED_PARAM(mapOfLists, arena_1024, 1024, true)
BENCHMARK_DRAW_LINE();
BENCHMARK_NAMED_PARAM(listOfStructs, heap_16, 16, false)
BENCHMARK_RELATIVE_NAMED_PARAM(listOfStructs, arena_16, 16, true)
BENCHMARK_NAMED_PARAM(listOfStructs, heap_1024, 1024, false)
BENCHMARK_RELATIVE_NAMED_PARAM(listOfStructs, arena_1024, 1024, true)

} // namespace
} // namespace apache::thrift::test::arena_pmr

int main(int argc, char** argv) {
  const folly::Init init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
