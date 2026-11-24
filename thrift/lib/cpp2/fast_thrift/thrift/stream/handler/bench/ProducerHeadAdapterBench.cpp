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
 * ProducerHeadAdapter microbenchmarks — the per-frame cost of the head
 * endpoint's outbound path: the direction/type guard plus the indirect dispatch
 * to the injected sink (the only cost it adds, since a well-formed frame is
 * checked and handed off with no state touched).
 */

#include <cstddef>
#include <utility>
#include <vector>

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/stream/common/Messages.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/stream/handler/ProducerHeadAdapter.h>

using namespace folly;
using namespace apache::thrift::fast_thrift::channel_pipeline;
using namespace apache::thrift::fast_thrift::thrift::stream;

namespace {

// onWrite ignores its context; a stub satisfies the signature.
struct BenchCtx {};

// Absorbs whatever the head endpoint hands to the sink.
Result absorbSink(DataMessage&&) noexcept {
  return Result::Success;
}

template <typename Make>
std::vector<TypeErasedBox> boxes(size_t n, Make&& make) {
  std::vector<TypeErasedBox> items;
  items.reserve(n);
  for (size_t i = 0; i < n; ++i) {
    items.push_back(erase_and_box(make()));
  }
  return items;
}

BENCHMARK(OnWrite_DeliverPayloadToSink, iters) {
  BenchmarkSuspender suspender;
  ProducerHeadAdapter handler(StreamSink{&absorbSink});
  BenchCtx ctx;
  auto items = boxes(
      iters, [] { return DataMessage{.payload = Payload{.data = nullptr}}; });
  suspender.dismiss();

  for (auto& item : items) {
    doNotOptimizeAway(handler.onWrite(ctx, std::move(item)));
  }
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  runBenchmarks();
  return 0;
}
