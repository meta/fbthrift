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
 * ThriftServerStreamMuxHandler microbenchmark — the per-unit cost the mux adds
 * routing one grant of demand to the wire: an inbound RequestN entering the
 * mux, being routed into a stream's sub-pipeline where a producer emits one
 * payload, and that payload being wrapped as an outbound chunk and injected
 * back onto the pipeline's write path.
 */

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include <folly/Benchmark.h>
#include <folly/ExceptionWrapper.h>
#include <folly/init/Init.h>
#include <folly/io/async/EventBase.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/HandlerTag.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/StaticPipelineBuilder.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/test/MockAdapters.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/common/ThriftControlPayloads.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/common/ThriftResponsePayloads.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/Messages.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/StreamResponsePayloads.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/handler/ThriftServerStreamMuxHandler.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/stream/ProducerPipeline.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/stream/common/Messages.h>

using namespace folly;
using namespace apache::thrift::fast_thrift::channel_pipeline;
using namespace apache::thrift::fast_thrift::thrift;

namespace {

// Emits exactly the demand it is handed, one payload per requested item; never
// completes, so the stream stays alive across the timed loop.
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
    auto& m = msg.get<stream::ControlMessage>();
    if (m.payload.is<stream::RequestN>()) {
      const uint64_t n = m.payload.get<stream::RequestN>().n;
      for (uint64_t i = 0; i < n; ++i) {
        (void)ctx.fireWrite(
            erase_and_box(stream::DataMessage{.payload = stream::Payload{}}));
      }
      return Result::Success;
    }
    return ctx.fireRead(std::move(msg));
  }
};

HANDLER_TAG(mux);
HANDLER_TAG(producer);

std::unique_ptr<stream::ProducerPipeline::ConfigFunc> makeEchoRecipe() {
  return std::make_unique<stream::ProducerPipeline::ConfigFunc>(
      [](folly::EventBase* evb, stream::StreamSink sink) {
        return stream::ProducerPipeline::build(
            evb, std::move(sink), [](auto builder) {
              return std::move(builder)
                  .template addNextDuplexTemplate<EchoProducer>(producer_tag);
            });
      });
}

} // namespace

BENCHMARK(RequestNRoutedToWire, iters) {
  BenchmarkSuspender suspender;
  folly::EventBase evb;
  test::TestAllocator alloc;
  test::StaticMockHeadHandler head;
  test::StaticMockTailHandler tail;
  head.setOnWriteCallback([](TypeErasedBox&& box) -> Result {
    doNotOptimizeAway(box);
    return Result::Success;
  });

  auto pipeline =
      StaticPipelineBuilder<
          test::StaticMockHeadHandler,
          test::StaticMockTailHandler,
          test::TestAllocator>()
          .setEventBase(&evb)
          .setHead(&head)
          .setTail(&tail)
          .setAllocator(&alloc)
          .addNextDuplexTemplate<ThriftServerStreamMuxHandler>(mux_tag)
          .build();

  ThriftServerResponseMessage open;
  open.payload = ThriftServerStreamOpenPayload{
      .initialResponse = ThriftStreamInitialResponsePayload{.streamId = 2},
      .configFunc = makeEchoRecipe()};
  (void)pipeline->fireWrite(erase_and_box(std::move(open)));

  std::vector<TypeErasedBox> demands;
  demands.reserve(iters);
  for (size_t i = 0; i < iters; ++i) {
    ThriftServerRequestMessage req;
    req.payload = ThriftRequestNPayload{.streamId = 2, .requestN = 1};
    req.streamId = 2;
    demands.push_back(erase_and_box(std::move(req)));
  }
  suspender.dismiss();

  for (auto& d : demands) {
    doNotOptimizeAway(pipeline->fireRead(std::move(d)));
  }
}

BENCHMARK(OpenStreamOnWrite, iters) {
  BenchmarkSuspender suspender;
  folly::EventBase evb;
  test::TestAllocator alloc;
  test::StaticMockHeadHandler head;
  test::StaticMockTailHandler tail;
  head.setOnWriteCallback([](TypeErasedBox&& box) -> Result {
    doNotOptimizeAway(box);
    return Result::Success;
  });

  auto pipeline =
      StaticPipelineBuilder<
          test::StaticMockHeadHandler,
          test::StaticMockTailHandler,
          test::TestAllocator>()
          .setEventBase(&evb)
          .setHead(&head)
          .setTail(&tail)
          .setAllocator(&alloc)
          .addNextDuplexTemplate<ThriftServerStreamMuxHandler>(mux_tag)
          .build();

  // One open message per iteration, each on a distinct streamId so every
  // onWrite registers a new sub-pipeline. Streams are never torn down here, so
  // the mux's streamId map grows monotonically across the timed loop.
  std::vector<TypeErasedBox> opens;
  opens.reserve(iters);
  for (size_t i = 0; i < iters; ++i) {
    ThriftServerResponseMessage open;
    open.payload = ThriftServerStreamOpenPayload{
        .initialResponse =
            ThriftStreamInitialResponsePayload{
                .streamId = static_cast<uint32_t>(i + 1)},
        .configFunc = makeEchoRecipe()};
    opens.push_back(erase_and_box(std::move(open)));
  }
  suspender.dismiss();

  for (auto& o : opens) {
    doNotOptimizeAway(pipeline->fireWrite(std::move(o)));
  }
}

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  runBenchmarks();
  return 0;
}
