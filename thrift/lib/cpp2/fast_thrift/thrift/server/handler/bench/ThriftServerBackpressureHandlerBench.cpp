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
 * ThriftServerBackpressureHandler microbenchmarks.
 *
 * Measures accepted response writes with and without transport pressure,
 * and releasing the read pause.
 */

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include <cstdint>
#include <utility>
#include <vector>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/Messages.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/handler/ThriftServerBackpressureHandler.h>

using namespace folly;
using namespace apache::thrift::fast_thrift::channel_pipeline;
using namespace apache::thrift::fast_thrift::thrift;

namespace {

// Stand-in for the pipeline that the handler asks to resume reads after drain.
struct BenchPipeline {
  void onReadReady() noexcept {}
};

// Local bench context with a configurable fireWrite result so we can
// arm the handler's backpressure flag from outside. fireRead/fireException
// are unused by the bench surface but kept so BenchCtx remains a complete
// stand-in for the ContextApi concept.
class BenchCtx {
 public:
  // NOLINTNEXTLINE(clang-diagnostic-unused-member-function)
  Result fireRead(TypeErasedBox&&) noexcept { return Result::Success; }
  Result fireWrite(TypeErasedBox&&) noexcept { return nextWriteResult; }
  // NOLINTNEXTLINE(clang-diagnostic-unused-member-function)
  void fireException(folly::exception_wrapper&&) noexcept {}

  // Backpressure / ready-signal stubs — no-ops so the bench measures
  // only the handler's own work, not the registration plumbing.
  void awaitWriteReady() noexcept {}
  void cancelAwaitWriteReady() noexcept {}
  BenchPipeline* pipeline() noexcept { return &pipeline_; }

  Result nextWriteResult{Result::Success};
  BenchPipeline pipeline_;
};

ThriftServerResponseMessage makeResponse(uint32_t streamId) {
  return ThriftServerResponseMessage{
      .payload = ThriftInitialResponsePayload{
          .data = nullptr,
          .metadata = nullptr,
          .streamId = streamId,
      }};
}

BENCHMARK(OnWrite_PassThrough, iters) {
  BenchmarkSuspender suspender;

  ReadBackpressureArbiter arbiter;
  ThriftServerBackpressureHandler<BenchCtx> handler(&arbiter);
  BenchCtx ctx;

  std::vector<TypeErasedBox> requests;
  requests.reserve(iters);
  for (size_t i = 0; i < iters; ++i) {
    requests.push_back(erase_and_box(makeResponse(static_cast<uint32_t>(i))));
  }

  suspender.dismiss();

  for (auto& req : requests) {
    auto result = handler.onWrite(ctx, std::move(req));
    doNotOptimizeAway(result);
  }
}

// Includes translating accepted write backpressure to Success.
BENCHMARK(OnWrite_ForwardWhileBackpressured, iters) {
  BenchmarkSuspender suspender;

  ReadBackpressureArbiter arbiter;
  ThriftServerBackpressureHandler<BenchCtx> handler(&arbiter);
  BenchCtx ctx;

  // Arm the backpressure flag with one priming write.
  ctx.nextWriteResult = Result::Backpressure;
  (void)handler.onWrite(ctx, erase_and_box(makeResponse(0)));

  std::vector<TypeErasedBox> requests;
  requests.reserve(iters);
  for (size_t i = 0; i < iters; ++i) {
    requests.push_back(
        erase_and_box(makeResponse(static_cast<uint32_t>(i + 1))));
  }

  suspender.dismiss();

  for (auto& req : requests) {
    auto result = handler.onWrite(ctx, std::move(req));
    doNotOptimizeAway(result);
  }
}

// Forward a group of responses, then release its read pause.
constexpr size_t kDrainBatch = 8;

BENCHMARK(OnWriteReady_ReleaseReadPause, iters) {
  BenchmarkSuspender suspender;

  ReadBackpressureArbiter arbiter;
  ThriftServerBackpressureHandler<BenchCtx> handler(&arbiter);
  BenchCtx ctx;

  const size_t cycles = (iters + kDrainBatch - 1) / kDrainBatch;

  suspender.dismiss();

  for (size_t c = 0; c < cycles; ++c) {
    // Saturate and forward the remaining responses.
    ctx.nextWriteResult = Result::Backpressure;
    (void)handler.onWrite(
        ctx, erase_and_box(makeResponse(static_cast<uint32_t>(c))));
    for (size_t i = 0; i < kDrainBatch - 1; ++i) {
      (void)handler.onWrite(
          ctx, erase_and_box(makeResponse(static_cast<uint32_t>(c))));
    }

    // Downstream signals room; release the read pause.
    ctx.nextWriteResult = Result::Success;
    handler.onWriteReady(ctx);
    doNotOptimizeAway(handler.isBackpressured());
  }
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  runBenchmarks();
  return 0;
}
