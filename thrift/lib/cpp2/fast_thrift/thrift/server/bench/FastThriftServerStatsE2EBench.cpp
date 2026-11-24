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
 * What attaching server stats costs a request, end to end.
 *
 * The handler microbenchmarks price a single incrementValue() against a stub
 * context. That answers "what does the handler cost" but not "what does
 * turning stats on cost", which is the question a service owner actually
 * asks: the handlers sit in two different pipelines, and their cost has to be
 * read against a whole request rather than against a bare fireRead.
 *
 * So each pair below runs the same request through the same server twice,
 * differing only in whether the two metrics handlers are in the pipelines —
 * which is exactly what FastThriftServer::setStats decides. The NoStats side
 * is not "stats disabled", it is stats absent: no handler, and therefore no
 * branch to skip one.
 *
 *   BENCHMARK(NoStats_*)            — pipelines built without the handlers
 *   BENCHMARK_RELATIVE(WithStats_*) — RocketMetricsHandler in the rocket
 *                                     pipeline, ThriftMetricsHandler in the
 *                                     thrift pipeline, at the positions
 *                                     ThriftServerConnectionFactory uses
 *
 * Both fixtures drive the production dispatch path (FastThriftServerAppAdapter
 * + FastServiceHandler) over BenchAsyncTransport, so the delta is the metrics
 * handlers and nothing else.
 *
 * The fixture holds a bare ServerStatsShard rather than a ServerStats. A
 * connection reaches its shard once, at accept; per request it only ever
 * touches the shard itself, so routing through ServerStats would add setup
 * this benchmark is not trying to measure.
 */

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <folly/io/IOBuf.h>
#include <folly/io/IOBufQueue.h>
#include <folly/io/async/EventBase.h>

#include <thrift/lib/cpp2/async/AsyncProcessor.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/BufferAllocator.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineRef.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/StaticPipelineBuilder.h>
#include <thrift/lib/cpp2/fast_thrift/common/Stats.h>
#include <thrift/lib/cpp2/fast_thrift/frame/FrameType.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/FrameHeaders.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/FrameWriter.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/server/common/RocketServerConnection.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/common/handler/ThriftMetricsHandler.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/RocketServerPipeline.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/adapter/ThriftServerAppAdapter.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/adapter/ThriftServerTransportAdapter.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/test/if/gen-cpp2/FastThriftServer.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/test/if/gen-cpp2/FastThriftServer.tcc>
#include <thrift/lib/cpp2/fast_thrift/transport/bench/BenchAsyncTransport.h>
#include <thrift/lib/cpp2/protocol/BinaryProtocol.h>
#include <thrift/lib/thrift/gen-cpp2/RpcMetadata_types.h>

using namespace folly;
using namespace apache::thrift::fast_thrift::channel_pipeline;
using namespace apache::thrift::fast_thrift;
using namespace apache::thrift::fast_thrift::frame;
using namespace apache::thrift::fast_thrift::frame::write;
using namespace apache::thrift::fast_thrift::transport::bench;
using namespace apache::thrift::fast_thrift::thrift::test::integration;

namespace {

constexpr size_t kPayloadSize = 4'096;

using ThriftMetrics = ThriftMetricsHandler<Direction::Server, ServerStatsShard>;

class FastHandler
    : public ::apache::thrift::FastServiceHandler<FastThriftServer> {
 public:
  bool noReply{false};

  void async_tm_ping(
      ::apache::thrift::fast_thrift::thrift::FastHandlerCallbackPtr<void>
          callback) override {
    if (noReply) {
      return;
    }
    callback->done();
  }

  void async_tm_echo(
      ::apache::thrift::fast_thrift::thrift::FastHandlerCallbackPtr<
          std::unique_ptr<EchoResponse>> callback,
      std::unique_ptr<std::string> message) override {
    if (noReply) {
      return;
    }
    auto response = std::make_unique<EchoResponse>();
    response->message() = std::move(*message);
    callback->result(std::move(response));
  }
};

// =============================================================================
// Frame construction
// =============================================================================

std::unique_ptr<folly::IOBuf> serializeRequestMetadata(
    const apache::thrift::RequestRpcMetadata& metadata) {
  apache::thrift::BinaryProtocolWriter writer;
  folly::IOBufQueue queue(folly::IOBufQueue::cacheChainLength());
  writer.setOutput(&queue);
  metadata.write(&writer);
  return queue.move();
}

apache::thrift::RequestRpcMetadata makeRequestMetadata(
    std::string_view methodName) {
  apache::thrift::RequestRpcMetadata metadata;
  metadata.name() = std::string(methodName);
  metadata.kind() = apache::thrift::RpcKind::SINGLE_REQUEST_SINGLE_RESPONSE;
  metadata.protocol() = apache::thrift::ProtocolId::BINARY;
  return metadata;
}

std::unique_ptr<folly::IOBuf> serializeEmptyPargs() {
  apache::thrift::BinaryProtocolWriter writer;
  folly::IOBufQueue queue(folly::IOBufQueue::cacheChainLength());
  writer.setOutput(&queue);
  FastThriftServer_ping_pargs pargs;
  pargs.write(&writer);
  return queue.move();
}

std::unique_ptr<folly::IOBuf> serializeEchoPargs(const std::string& payload) {
  apache::thrift::BinaryProtocolWriter writer;
  folly::IOBufQueue queue(folly::IOBufQueue::cacheChainLength());
  writer.setOutput(&queue);
  FastThriftServer_echo_pargs pargs;
  pargs.template get<0>().value = const_cast<std::string*>(&payload);
  pargs.write(&writer);
  return queue.move();
}

std::unique_ptr<folly::IOBuf> createRequestFrame(
    uint32_t streamId,
    std::unique_ptr<folly::IOBuf> metadata,
    std::unique_ptr<folly::IOBuf> data) {
  return serialize(
      RequestResponseHeader{.streamId = streamId},
      std::move(metadata),
      std::move(data));
}

std::unique_ptr<folly::IOBuf> prependLengthPrefix(
    std::unique_ptr<folly::IOBuf> frame) {
  size_t frameLength = frame->computeChainDataLength();
  auto lengthPrefix = folly::IOBuf::create(kMetadataLengthSize);
  uint8_t* data = lengthPrefix->writableData();
  data[0] = static_cast<uint8_t>((frameLength >> 16) & 0xFF);
  data[1] = static_cast<uint8_t>((frameLength >> 8) & 0xFF);
  data[2] = static_cast<uint8_t>(frameLength & 0xFF);
  lengthPrefix->append(kMetadataLengthSize);
  lengthPrefix->appendChain(std::move(frame));
  return lengthPrefix;
}

// =============================================================================
// Pipelines — identical either side of the comparison but for the two
// metrics handlers, which sit where ThriftServerConnectionFactory puts them:
// rocket closest to the tail, thrift closest to the head.
// =============================================================================

// Non-blocking throughout: BenchAsyncTransport::injectReadData delivers
// straight into the read callback rather than scheduling on the EventBase, so
// a blocking loopOnce() would wait on an event that is never coming. The loop
// still has to run, because outbound writes are flushed from a LoopCallback.
void injectSetupFrame(BenchAsyncTransport* transport, folly::EventBase& evb) {
  auto setupFrame = serialize(
      SetupHeader{
          .majorVersion = 1,
          .minorVersion = 0,
          .keepaliveTime = 30000,
          .maxLifetime = 60000},
      nullptr,
      nullptr);
  transport->injectReadData(prependLengthPrefix(std::move(setupFrame)));
  evb.loopOnce(EVLOOP_NONBLOCK);
}

struct BenchFixture {
  folly::EventBase evb;
  BenchAsyncTransport* testTransport{nullptr};

  // Stands in for the per-IO-thread shard a real connection is handed.
  ServerStatsShard shard;
  SimpleBufferAllocator rocketAllocator;

  std::unique_ptr<thrift::server::ThriftServerTransportAdapter>
      transportAdapter;
  std::shared_ptr<FastHandler> handler;
  std::unique_ptr<
      FastThriftServerAppAdapter,
      folly::DelayedDestruction::Destructor>
      adapter;
  SimpleBufferAllocator thriftAllocator;
  PipelineOwner thriftPipeline;

  void setup(bool withStats, bool noReply) {
    auto* shardPtr = withStats ? &shard : nullptr;
    auto transport =
        folly::AsyncTransport::UniquePtr(new BenchAsyncTransport(&evb));
    testTransport = static_cast<BenchAsyncTransport*>(transport.get());
    auto rocketConn =
        thrift::server::buildStaticRocketServerConnection<NoHandlerOverrides>(
            std::move(transport), {}, rocketAllocator, shardPtr);

    handler = std::make_shared<FastHandler>();
    handler->noReply = noReply;
    adapter.reset(new FastThriftServerAppAdapter(handler));

    transportAdapter =
        std::make_unique<thrift::server::ThriftServerTransportAdapter>(
            std::move(rocketConn));

    auto builder = StaticPipelineBuilder<
                       thrift::server::ThriftServerTransportAdapter,
                       FastThriftServerAppAdapter,
                       SimpleBufferAllocator>()
                       .setEventBase(&evb)
                       .setHead(transportAdapter.get())
                       .setTail(adapter.get())
                       .setAllocator(&thriftAllocator);
    if (shardPtr != nullptr) {
      thriftPipeline = std::move(builder)
                           .addNextDuplex<ThriftMetrics>(
                               thrift_metrics_handler_tag, shardPtr)
                           .build();
    } else {
      thriftPipeline = std::move(builder).build();
    }

    transportAdapter->setPipeline(thriftPipeline.get());
    adapter->setPipeline(thriftPipeline.get());

    transportAdapter->rocketConnection().start();
    injectSetupFrame(testTransport, evb);
  }

  void injectFrame(std::unique_ptr<folly::IOBuf> frame) {
    testTransport->injectReadData(prependLengthPrefix(std::move(frame)));
  }
};

// =============================================================================
// Bench bodies
// =============================================================================

void runRequestBench(BenchFixture& fixture, uint32_t iters) {
  auto metadataTemplate = serializeRequestMetadata(makeRequestMetadata("ping"));
  auto pargsTemplate = serializeEmptyPargs();

  std::vector<std::unique_ptr<folly::IOBuf>> requests;
  requests.reserve(iters);
  for (uint32_t i = 0; i < iters; ++i) {
    requests.push_back(createRequestFrame(
        i * 2 + 1, metadataTemplate->clone(), pargsTemplate->clone()));
  }

  for (uint32_t i = 0; i < iters; ++i) {
    fixture.injectFrame(std::move(requests[i]));
    fixture.evb.loopOnce(EVLOOP_NONBLOCK);
  }
}

void runResponseBench(
    BenchFixture& fixture, const std::string& payload, uint32_t iters) {
  auto metadataTemplate = serializeRequestMetadata(makeRequestMetadata("echo"));
  auto pargsTemplate = serializeEchoPargs(payload);

  std::vector<std::unique_ptr<folly::IOBuf>> requests;
  requests.reserve(iters);
  for (uint32_t i = 0; i < iters; ++i) {
    requests.push_back(createRequestFrame(
        i * 2 + 1, metadataTemplate->clone(), pargsTemplate->clone()));
  }

  for (uint32_t i = 0; i < iters; ++i) {
    fixture.injectFrame(std::move(requests[i]));
    fixture.evb.loopOnce(EVLOOP_NONBLOCK);
    fixture.testTransport->clearWrittenData();
  }
}

} // namespace

// =============================================================================
// Request only — the smallest request there is, so the metrics handlers are
// the largest fraction of it they will ever be.
// =============================================================================

BENCHMARK(NoStats_Request, iters) {
  folly::BenchmarkSuspender suspender;
  BenchFixture fixture;
  fixture.setup(/*withStats=*/false, /*noReply=*/true);
  suspender.dismiss();

  runRequestBench(fixture, iters);
}

BENCHMARK_RELATIVE(WithStats_Request, iters) {
  folly::BenchmarkSuspender suspender;
  BenchFixture fixture;
  fixture.setup(/*withStats=*/true, /*noReply=*/true);
  suspender.dismiss();

  runRequestBench(fixture, iters);
}

BENCHMARK_DRAW_LINE();

// =============================================================================
// Request and response — a realistic echo, where the same fixed handler cost
// is measured against a request that does real work.
// =============================================================================

BENCHMARK(NoStats_RequestResponse, iters) {
  folly::BenchmarkSuspender suspender;
  BenchFixture fixture;
  fixture.setup(/*withStats=*/false, /*noReply=*/false);
  std::string payload(kPayloadSize, 'x');
  suspender.dismiss();

  runResponseBench(fixture, payload, iters);
}

BENCHMARK_RELATIVE(WithStats_RequestResponse, iters) {
  folly::BenchmarkSuspender suspender;
  BenchFixture fixture;
  fixture.setup(/*withStats=*/true, /*noReply=*/false);
  std::string payload(kPayloadSize, 'x');
  suspender.dismiss();

  runResponseBench(fixture, payload, iters);
}

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv, true);
  folly::runBenchmarks();
  return 0;
}
