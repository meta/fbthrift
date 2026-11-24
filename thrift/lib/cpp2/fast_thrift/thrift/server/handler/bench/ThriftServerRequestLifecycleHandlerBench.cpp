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

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <folly/io/async/EventBase.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/Event.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/Messages.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/ThriftRequestContext.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/handler/ThriftServerRequestLifecycleHandler.h>

namespace apache::thrift::fast_thrift::thrift {
namespace {

using channel_pipeline::erase_and_box;
using channel_pipeline::Result;
using channel_pipeline::TypeErasedBox;

struct BenchContext {
  Result fireRead(TypeErasedBox&& msg) noexcept {
    request = std::move(msg);
    return Result::Success;
  }

  Result fireWrite(TypeErasedBox&& msg) noexcept {
    auto consumed = std::move(msg);
    folly::doNotOptimizeAway(consumed);
    return Result::Success;
  }

  template <channel_pipeline::PipelineEvent E>
  void fireEvent(const typename E::Payload& event) noexcept {
    folly::doNotOptimizeAway(event.streamId);
  }

  TypeErasedBox request;
};

template <bool Cancel>
void runLifecycle(unsigned int iters) {
  folly::BenchmarkSuspender suspender;
  folly::EventBase evb;
  BenchContext ctx;
  ThriftServerRequestLifecycleHandler<BenchContext> handler;
  suspender.dismiss();

  for (unsigned int i = 0; i < iters; ++i) {
    ThriftServerRequestMessage request;
    request.streamId = 1;
    request.requestContext = makeThriftRequestContext(evb);
    auto readResult = handler.onRead(ctx, erase_and_box(std::move(request)));
    folly::doNotOptimizeAway(readResult);
    if constexpr (Cancel) {
      handler.template on<ThriftServerRequestCancellationEvent>(
          ctx, ThriftServerRequestCancellationEvent{.streamId = 1});
    }
    ThriftServerResponseMessage response{
        .payload = ThriftInitialResponsePayload{.streamId = 1}};
    response.requestContext =
        std::move(ctx.request.get<ThriftServerRequestMessage>().requestContext);
    auto writeResult = handler.onWrite(ctx, erase_and_box(std::move(response)));
    folly::doNotOptimizeAway(writeResult);
    folly::doNotOptimizeAway(handler.requestCount());
  }
  suspender.rehire();
}

BENCHMARK(RequestLifecycle_Response, iters) {
  runLifecycle<false>(iters);
}

BENCHMARK(RequestLifecycle_Cancellation, iters) {
  runLifecycle<true>(iters);
}

} // namespace
} // namespace apache::thrift::fast_thrift::thrift

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  folly::runBenchmarks();
}
