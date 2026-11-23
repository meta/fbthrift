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
 * FastThriftServer Integration Microbenchmarks
 *
 * Server-side counterpart to FastThriftClientIntegrationBench. Compares the
 * generated server-side dispatch paths through the rocket pipeline with a
 * mock socket:
 *
 *   BENCHMARK(FastThriftChannel_*)        — ThriftServerChannel +
 *                                           apache::thrift::ServiceHandler<
 *                                               FastThriftChannelServer>
 *                                           (legacy generated handler)
 *   BENCHMARK_RELATIVE(FastThriftHandler_*) — FastThriftServerAppAdapter +
 *                                             FastServiceHandler<FastThriftServer>
 *                                             (new generated handler)
 *
 * Both fixtures share the same rocket-server pipeline +
 * ThriftServerTransportAdapter and use BenchAsyncTransport so the comparison
 * isolates the dispatch path (parse → route → invoke handler → write reply).
 *
 * Request benchmarks stop after handler invocation; callback completion and the
 * response path run while timing is suspended. Round-trip benchmarks measure
 * the complete request and successful response path with bounded output state.
 */

#include <exception>
#include <optional>

#include <folly/Benchmark.h>
#include <folly/SocketAddress.h>
#include <folly/init/Init.h>
#include <folly/io/IOBuf.h>
#include <folly/io/IOBufQueue.h>
#include <folly/io/async/EventBase.h>

#include <thrift/lib/cpp2/async/AsyncProcessor.h>
#include <thrift/lib/cpp2/async/processor/HandlerCallback.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/BufferAllocator.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineRef.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/StaticPipelineBuilder.h>
#include <thrift/lib/cpp2/fast_thrift/frame/FrameType.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/FrameHeaders.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/FrameWriter.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/server/common/RocketServerConnection.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/RocketServerPipeline.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/ThriftServerChannel.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/ThriftServerConnectionFactory.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/adapter/ThriftServerTransportAdapter.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/handler/ThriftServerSetupHandler.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/test/if/gen-cpp2/FastThriftChannelServer.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/test/if/gen-cpp2/FastThriftServer.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/test/if/gen-cpp2/FastThriftServer.tcc>
#include <thrift/lib/cpp2/fast_thrift/transport/bench/BenchAsyncTransport.h>
#include <thrift/lib/cpp2/protocol/BinaryProtocol.h>
#include <thrift/lib/cpp2/server/Cpp2Worker.h>
#include <thrift/lib/thrift/gen-cpp2/RpcMetadata_types.h>

using namespace folly;
using namespace apache::thrift::fast_thrift::channel_pipeline;
using namespace apache::thrift::fast_thrift;
using namespace apache::thrift::fast_thrift::frame;
using namespace apache::thrift::fast_thrift::frame::write;
using namespace apache::thrift::fast_thrift::transport::bench;
using namespace apache::thrift::fast_thrift::thrift::test::integration;

namespace {

// =============================================================================
// Constants
// =============================================================================

constexpr size_t kPayloadSize = 4'096;

HANDLER_TAG(thrift_server_setup_handler);

// =============================================================================
// Generated handlers — one per dispatch path. Request-only benchmarks retain
// the ping callback and complete it while timing is suspended. Round-trip
// benchmarks complete callbacks normally with the echoed payload.
// =============================================================================

class ChannelHandler
    : public apache::thrift::ServiceHandler<FastThriftChannelServer> {
 public:
  using PingCallback = apache::thrift::HandlerCallbackPtr<void>;

  bool deferReply{false};

  void async_tm_ping(PingCallback callback) override {
    if (deferReply) {
      deferredPing_ = std::move(callback);
      return;
    }
    callback->done();
  }

  void async_tm_echo(
      apache::thrift::HandlerCallbackPtr<std::unique_ptr<EchoResponse>>
          callback,
      std::unique_ptr<std::string> message) override {
    auto response = std::make_unique<EchoResponse>();
    response->message() = std::move(*message);
    callback->result(std::move(response));
  }

  void completeDeferredPing() {
    auto callback = std::move(deferredPing_);
    if (!callback) {
      std::terminate();
    }
    callback->done();
  }

 private:
  PingCallback deferredPing_;
};

class FastHandler
    : public ::apache::thrift::FastServiceHandler<FastThriftServer> {
 public:
  using PingCallback =
      ::apache::thrift::fast_thrift::thrift::FastHandlerCallbackPtr<void>;

  bool deferReply{false};

  void async_tm_ping(PingCallback callback) override {
    if (deferReply) {
      deferredPing_ = std::move(callback);
      return;
    }
    callback->done();
  }

  void async_tm_echo(
      ::apache::thrift::fast_thrift::thrift::FastHandlerCallbackPtr<
          std::unique_ptr<EchoResponse>> callback,
      std::unique_ptr<std::string> message) override {
    auto response = std::make_unique<EchoResponse>();
    response->message() = std::move(*message);
    callback->result(std::move(response));
  }

  void completeDeferredPing() {
    auto callback = std::move(deferredPing_);
    if (!callback) {
      std::terminate();
    }
    callback->done();
  }

 private:
  PingCallback deferredPing_;
};

// =============================================================================
// Helpers — frame + pargs construction
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

apache::thrift::RequestRpcMetadata makeRequestMetadataWithHeaders(
    std::string_view methodName) {
  auto metadata = makeRequestMetadata(methodName);
  metadata.clientTimeoutMs() = 10000;
  metadata.queueTimeoutMs() = 5000;

  auto& headers = metadata.otherMetadata().ensure();
  headers["request_id"] = "abc123";
  headers["trace_id"] = "trace-456";
  headers["client_name"] = "benchmark_client";
  headers["routing_key"] = "shard-42";
  headers["priority"] = "high";

  return metadata;
}

std::unique_ptr<folly::IOBuf> serializeEmptyPargs() {
  apache::thrift::BinaryProtocolWriter writer;
  folly::IOBufQueue queue(folly::IOBufQueue::cacheChainLength());
  writer.setOutput(&queue);
  // ping_pargs has no fields; both legacy and fast generators emit
  // structurally identical empty pargs, so reuse one for both fixtures.
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

std::unique_ptr<folly::IOBuf> createFastThriftRequestFrame(
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

// =============================================================================
// FastThriftChannel fixture — ThriftServerChannel + legacy generated handler
// =============================================================================

struct ChannelBenchFixture {
  folly::EventBase evb;
  BenchAsyncTransport* testTransport{nullptr};
  SimpleBufferAllocator rocketAllocator;
  SimpleBufferAllocator thriftAllocator;

  std::unique_ptr<thrift::server::ThriftServerTransportAdapter>
      transportAdapter;
  std::shared_ptr<ChannelHandler> handler;
  std::shared_ptr<thrift::ThriftServerChannel> serverChannel;
  PipelineOwner thriftPipeline;

  void setup(bool deferReply) {
    auto transport =
        folly::AsyncTransport::UniquePtr(new BenchAsyncTransport(&evb));
    testTransport = static_cast<BenchAsyncTransport*>(transport.get());
    auto rocketConn =
        thrift::server::buildStaticRocketServerConnection<NoHandlerOverrides>(
            std::move(transport), {}, rocketAllocator, nullptr);

    handler = std::make_shared<ChannelHandler>();
    handler->deferReply = deferReply;
    serverChannel = std::make_shared<thrift::ThriftServerChannel>(handler);

    transportAdapter =
        std::make_unique<thrift::server::ThriftServerTransportAdapter>(
            std::move(rocketConn));

    thriftPipeline =
        StaticPipelineBuilder<
            thrift::server::ThriftServerTransportAdapter,
            thrift::ThriftServerChannel,
            SimpleBufferAllocator>()
            .setEventBase(&evb)
            .setHead(transportAdapter.get())
            .setTail(serverChannel.get())
            .setAllocator(&thriftAllocator)
            .addNextDuplexTemplate<thrift::ThriftServerSetupHandler>(
                thrift_server_setup_handler_tag)
            .build();

    transportAdapter->setPipeline(thriftPipeline.get());
    serverChannel->setPipelineRef(thriftPipeline.ref());
    serverChannel->setWorker(apache::thrift::Cpp2Worker::createDummy(&evb));

    transportAdapter->rocketConnection().start();
    injectSetupFrame(testTransport, evb);
  }

  void injectFrame(std::unique_ptr<folly::IOBuf> frame) {
    testTransport->injectReadData(prependLengthPrefix(std::move(frame)));
  }

  void completeDeferredRequest() { handler->completeDeferredPing(); }
};

// =============================================================================
// FastThriftHandler fixture — FastThriftServerAppAdapter + new generated
// handler
// =============================================================================

struct FastBenchFixture {
  folly::EventBase evb;
  BenchAsyncTransport* testTransport{nullptr};
  std::shared_ptr<FastHandler> handler;
  std::optional<thrift::server::ThriftServerConnectionFactory> factory;
  std::optional<thrift::server::ThriftServerConnection> connection;

  void setup(bool deferReply) {
    handler = std::make_shared<FastHandler>();
    handler->deferReply = deferReply;
    thrift::server::ThriftServerConnectionFactoryConfig config;
    config.handler = handler;
    factory.emplace(std::move(config));

    auto transport =
        folly::AsyncTransport::UniquePtr(new BenchAsyncTransport(&evb));
    testTransport = static_cast<BenchAsyncTransport*>(transport.get());
    connection.emplace(factory->getConnection(
        std::move(transport),
        folly::SocketAddress("127.0.0.1", 1234),
        nullptr));
    connection->start();
    injectSetupFrame(testTransport, evb);
  }

  void injectFrame(std::unique_ptr<folly::IOBuf> frame) {
    testTransport->injectReadData(prependLengthPrefix(std::move(frame)));
  }

  void completeDeferredRequest() { handler->completeDeferredPing(); }
};

// =============================================================================
// Bench bodies
// =============================================================================

template <typename Fixture>
void runRequestBench(
    Fixture& fixture,
    const apache::thrift::RequestRpcMetadata& metadataProto,
    uint32_t iters) {
  auto metadataTemplate = serializeRequestMetadata(metadataProto);
  auto pargsTemplate = serializeEmptyPargs();

  std::vector<std::unique_ptr<folly::IOBuf>> requests;
  requests.reserve(iters);
  for (uint32_t i = 0; i < iters; ++i) {
    requests.push_back(createFastThriftRequestFrame(
        i * 2 + 1, metadataTemplate->clone(), pargsTemplate->clone()));
  }

  for (uint32_t i = 0; i < iters; ++i) {
    fixture.injectFrame(std::move(requests[i]));
    fixture.evb.loopOnce(EVLOOP_NONBLOCK);
    {
      folly::BenchmarkSuspender cleanupSuspender;
      fixture.completeDeferredRequest();
      fixture.evb.loopOnce(EVLOOP_NONBLOCK);
      fixture.testTransport->clearWrittenData();
    }
  }
}

template <typename Fixture>
void runRoundTripBench(
    Fixture& fixture,
    const apache::thrift::RequestRpcMetadata& metadataProto,
    const std::string& payload,
    uint32_t iters) {
  auto metadataTemplate = serializeRequestMetadata(metadataProto);
  auto pargsTemplate = serializeEchoPargs(payload);

  std::vector<std::unique_ptr<folly::IOBuf>> requests;
  requests.reserve(iters);
  for (uint32_t i = 0; i < iters; ++i) {
    requests.push_back(createFastThriftRequestFrame(
        i * 2 + 1, metadataTemplate->clone(), pargsTemplate->clone()));
  }

  for (uint32_t i = 0; i < iters; ++i) {
    fixture.injectFrame(std::move(requests[i]));
    fixture.evb.loopOnce(EVLOOP_NONBLOCK);
    fixture.testTransport->clearWrittenData();
  }
}

// =============================================================================
// Request-only - callback completion and response processing are not timed
// =============================================================================

BENCHMARK(FastThriftChannel_Request_MinimalMetadata, iters) {
  folly::BenchmarkSuspender suspender;
  ChannelBenchFixture fixture;
  fixture.setup(/*deferReply=*/true);
  suspender.dismiss();

  runRequestBench(fixture, makeRequestMetadata("ping"), iters);
  suspender.rehire();
}

BENCHMARK_RELATIVE(FastThriftHandler_Request_MinimalMetadata, iters) {
  folly::BenchmarkSuspender suspender;
  FastBenchFixture fixture;
  fixture.setup(/*deferReply=*/true);
  suspender.dismiss();

  runRequestBench(fixture, makeRequestMetadata("ping"), iters);
  suspender.rehire();
}

BENCHMARK_DRAW_LINE();

BENCHMARK(FastThriftChannel_Request_WithHeaders, iters) {
  folly::BenchmarkSuspender suspender;
  ChannelBenchFixture fixture;
  fixture.setup(/*deferReply=*/true);
  suspender.dismiss();

  runRequestBench(fixture, makeRequestMetadataWithHeaders("ping"), iters);
  suspender.rehire();
}

BENCHMARK_RELATIVE(FastThriftHandler_Request_WithHeaders, iters) {
  folly::BenchmarkSuspender suspender;
  FastBenchFixture fixture;
  fixture.setup(/*deferReply=*/true);
  suspender.dismiss();

  runRequestBench(fixture, makeRequestMetadataWithHeaders("ping"), iters);
  suspender.rehire();
}

BENCHMARK_DRAW_LINE();

// =============================================================================
// Full request-response round trip
// =============================================================================

BENCHMARK(FastThriftChannel_RequestResponse_RoundTrip, iters) {
  folly::BenchmarkSuspender suspender;
  ChannelBenchFixture fixture;
  fixture.setup(/*deferReply=*/false);
  std::string payload(kPayloadSize, 'x');
  suspender.dismiss();

  runRoundTripBench(fixture, makeRequestMetadata("echo"), payload, iters);
  suspender.rehire();
}

BENCHMARK_RELATIVE(FastThriftHandler_RequestResponse_RoundTrip, iters) {
  folly::BenchmarkSuspender suspender;
  FastBenchFixture fixture;
  fixture.setup(/*deferReply=*/false);
  std::string payload(kPayloadSize, 'x');
  suspender.dismiss();

  runRoundTripBench(fixture, makeRequestMetadata("echo"), payload, iters);
  suspender.rehire();
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  runBenchmarks();
  return 0;
}
