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

// Each benchmark runs at doubling sizes so the output reads as a scaling
// curve: a row ~2x the previous one is linear, ~4x is quadratic.

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <thrift/lib/cpp2/patch/DynamicPatch.h>

namespace apache::thrift::protocol {
namespace {

Value makeI32List(size_t size, size_t zeros) {
  Value v;
  auto& list = v.ensure_list();
  for (size_t i = 0; i < size; ++i) {
    list.emplace_back().emplace_i32(i < zeros ? 0 : 1);
  }
  return v;
}

// `src` = [0]*(n/2) + [1] is not a prefix of `dst` = [0]*n, but n/2 elements
// match at every offset of `dst`.
void diffListNotPrefix(unsigned iters, size_t n) {
  Value src, dst;
  BENCHMARK_SUSPEND {
    src = makeI32List(n / 2 + 1, n / 2);
    dst = makeI32List(n, n);
  }
  while (iters--) {
    folly::doNotOptimizeAway(DiffVisitorBase{}.diff(src, dst));
  }
}

} // namespace

BENCHMARK_PARAM(diffListNotPrefix, 100)
BENCHMARK_PARAM(diffListNotPrefix, 200)
BENCHMARK_PARAM(diffListNotPrefix, 400)
BENCHMARK_PARAM(diffListNotPrefix, 800)
BENCHMARK_PARAM(diffListNotPrefix, 1600)
BENCHMARK_DRAW_LINE();

} // namespace apache::thrift::protocol

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
