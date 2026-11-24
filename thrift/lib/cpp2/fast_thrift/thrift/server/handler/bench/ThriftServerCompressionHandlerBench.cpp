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
#include <folly/io/IOBuf.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <folly/compression/Compression.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/bench/BenchContext.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/Messages.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/ThriftRequestContext.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/handler/ThriftServerCompressionHandler.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/util/RequestMetadata.h>
#include <thrift/lib/thrift/gen-cpp2/RpcMetadata_types.h>

using namespace folly;
using namespace apache::thrift::fast_thrift::channel_pipeline;
using namespace apache::thrift::fast_thrift::thrift;

namespace {

using apache::thrift::fast_thrift::rocket::bench::BenchContext;

const std::string kRequestBody(1024, 'a');

folly::EventBase& requestMetadataEventBase() {
  static folly::EventBase eventBase;
  return eventBase;
}

apache::thrift::CompressionConfig zlibCompressionConfig() {
  apache::thrift::CompressionConfig config;
  config.codecConfig().ensure().zlibConfig().ensure();
  return config;
}

ThriftServerRequestMessage makeRequest(
    uint32_t streamId,
    std::optional<apache::thrift::CompressionAlgorithm> compression) {
  auto metadata = std::make_unique<apache::thrift::RequestRpcMetadata>();
  auto data = folly::IOBuf::copyBuffer(kRequestBody);
  if (compression.has_value()) {
    metadata->compression() = *compression;
    data = folly::compression::getCodec(
               folly::compression::CodecType::ZLIB,
               folly::compression::COMPRESSION_LEVEL_DEFAULT)
               ->compress(data.get());
  }
  return ThriftServerRequestMessage{
      .requestContext = ThriftRequestContextPtr{},
      .payload =
          ThriftServerRequestResponsePayload{
              .data = std::move(data),
              .metadata = makeServerRequestMetadata(
                  requestMetadataEventBase(), std::move(*metadata)),
          },
      .streamId = streamId,
  };
}

ThriftServerResponseMessage makeResponse(
    folly::EventBase& eventBase,
    uint32_t streamId,
    bool enableCompression = false) {
  ThriftRequestContextPtr requestContext;
  if (enableCompression) {
    requestContext = makeThriftRequestContext(eventBase);
    requestContext->setResponseCompressionConfig(zlibCompressionConfig());
  }
  return ThriftServerResponseMessage{
      .requestContext = std::move(requestContext),
      .payload = ThriftInitialResponsePayload{
          .data = folly::IOBuf::copyBuffer(kRequestBody),
          .metadata = std::make_unique<apache::thrift::ResponseRpcMetadata>(),
          .streamId = streamId,
      }};
}

BENCHMARK(OnRead_NoCompression, iters) {
  BenchmarkSuspender suspender;
  ThriftServerCompressionHandler<BenchContext> handler;
  BenchContext ctx;
  std::vector<TypeErasedBox> requests;
  requests.reserve(iters);
  for (size_t i = 0; i < iters; ++i) {
    requests.push_back(erase_and_box(
        makeRequest(static_cast<uint32_t>(i), /*compression=*/std::nullopt)));
  }
  suspender.dismiss();

  for (auto& request : requests) {
    auto result = handler.onRead(ctx, std::move(request));
    doNotOptimizeAway(result);
  }
}

BENCHMARK(OnRead_ZlibDecompression, iters) {
  BenchmarkSuspender suspender;
  ThriftServerCompressionHandler<BenchContext> handler;
  BenchContext ctx;
  std::vector<TypeErasedBox> requests;
  requests.reserve(iters);
  for (size_t i = 0; i < iters; ++i) {
    requests.push_back(erase_and_box(makeRequest(
        static_cast<uint32_t>(i), apache::thrift::CompressionAlgorithm::ZLIB)));
  }
  suspender.dismiss();

  for (auto& request : requests) {
    auto result = handler.onRead(ctx, std::move(request));
    doNotOptimizeAway(result);
  }
}

BENCHMARK(OnWrite_NoCompression, iters) {
  BenchmarkSuspender suspender;
  ThriftServerCompressionHandler<BenchContext> handler;
  BenchContext ctx;
  std::vector<TypeErasedBox> responses;
  responses.reserve(iters);
  for (size_t i = 0; i < iters; ++i) {
    responses.push_back(erase_and_box(
        makeResponse(*ctx.eventBase(), static_cast<uint32_t>(i + 1))));
  }
  suspender.dismiss();

  for (auto& response : responses) {
    auto result = handler.onWrite(ctx, std::move(response));
    doNotOptimizeAway(result);
  }
}

BENCHMARK(OnWrite_ZlibCompression, iters) {
  BenchmarkSuspender suspender;
  ThriftServerCompressionHandler<BenchContext> handler;
  BenchContext ctx;
  std::vector<TypeErasedBox> responses;
  responses.reserve(iters);
  for (size_t i = 0; i < iters; ++i) {
    const auto streamId = static_cast<uint32_t>(i + 1);
    responses.push_back(erase_and_box(
        makeResponse(*ctx.eventBase(), streamId, /*enableCompression=*/true)));
  }
  suspender.dismiss();

  for (auto& response : responses) {
    auto result = handler.onWrite(ctx, std::move(response));
    doNotOptimizeAway(result);
  }
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  runBenchmarks();
  return 0;
}
