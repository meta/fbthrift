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

/**
 * ProducerTailAdapter microbenchmark — the per-frame cost of the tail
 * endpoint's one characteristic operation, the inbound demand backstop. In an
 * optimized build the backstop returns Result::Error without the debug
 * assertion, so this measures the guard + out-of-line dispatch the tail adds on
 * the read path.
 */

#include <cstddef>
#include <utility>
#include <vector>

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/stream/common/Messages.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/stream/handler/ProducerTailAdapter.h>

using namespace folly;
using namespace apache::thrift::fast_thrift::channel_pipeline;
using namespace apache::thrift::fast_thrift::thrift::stream;

namespace {

// onRead ignores its context; a stub satisfies the signature.
struct BenchCtx {};

BENCHMARK(OnRead_DemandBackstop, iters) {
  BenchmarkSuspender suspender;
  ProducerTailAdapter handler;
  BenchCtx ctx;
  std::vector<TypeErasedBox> items;
  items.reserve(iters);
  for (size_t i = 0; i < iters; ++i) {
    items.push_back(erase_and_box(ControlMessage{.payload = RequestN{.n = 1}}));
  }
  suspender.dismiss();

  for (auto& item : items) {
    doNotOptimizeAway(handler.onRead(ctx, std::move(item)));
  }
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  runBenchmarks();
  return 0;
}
