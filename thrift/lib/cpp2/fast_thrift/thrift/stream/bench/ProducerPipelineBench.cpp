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
 * ProducerPipeline microbenchmark — the per-unit cost of driving one grant of
 * demand through a Builder-built producing-end sub-pipeline: an inbound
 * RequestN entering the head, a producer emitting one payload, and that payload
 * exiting through the injected sink. Measures the round-trip the assembled
 * endpoints and sink add over a bare handler call.
 */

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <folly/io/async/EventBase.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/HandlerTag.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/stream/ProducerPipeline.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/stream/common/Messages.h>

using namespace folly;
using namespace apache::thrift::fast_thrift::channel_pipeline;
using namespace apache::thrift::fast_thrift::thrift::stream;

namespace {

// Emits exactly the demand it is handed, one payload per requested item.
template <typename Ctx>
class EchoProducer {
 public:
  void handlerAdded(Ctx& /*ctx*/) noexcept {}
  void handlerRemoved(Ctx& /*ctx*/) noexcept {}
  void onPipelineActive(Ctx& /*ctx*/) noexcept {}
  void onPipelineInactive(Ctx& /*ctx*/) noexcept {}
  void onReadReady(Ctx& /*ctx*/) noexcept {}
  void onWriteReady(Ctx& /*ctx*/) noexcept {}
  void onException(Ctx& ctx, folly::exception_wrapper&& e) noexcept {
    ctx.fireException(std::move(e));
  }
  Result onWrite(Ctx& ctx, TypeErasedBox&& msg) noexcept {
    return ctx.fireWrite(std::move(msg));
  }
  Result onRead(Ctx& ctx, TypeErasedBox&& msg) noexcept {
    auto& m = msg.get<ControlMessage>();
    if (m.payload.is<RequestN>()) {
      const uint64_t n = m.payload.get<RequestN>().n;
      for (uint64_t i = 0; i < n; ++i) {
        (void)ctx.fireWrite(
            erase_and_box(DataMessage{.payload = Payload{.data = nullptr}}));
      }
      return Result::Success;
    }
    return ctx.fireRead(std::move(msg));
  }
};

Result absorbSink(DataMessage&&) noexcept {
  return Result::Success;
}

HANDLER_TAG(producer);

BENCHMARK(DemandToSinkRoundTrip, iters) {
  BenchmarkSuspender suspender;
  folly::EventBase evb;

  auto pipeline =
      ProducerPipeline::build(&evb, StreamSink{&absorbSink}, [](auto builder) {
        return std::move(builder).template addNextDuplexTemplate<EchoProducer>(
            producer_tag);
      });

  std::vector<TypeErasedBox> demands;
  demands.reserve(iters);
  for (size_t i = 0; i < iters; ++i) {
    demands.push_back(
        erase_and_box(ControlMessage{.payload = RequestN{.n = 1}}));
  }
  suspender.dismiss();

  for (auto& d : demands) {
    doNotOptimizeAway(pipeline.fireRead(std::move(d)));
  }
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  runBenchmarks();
  return 0;
}
