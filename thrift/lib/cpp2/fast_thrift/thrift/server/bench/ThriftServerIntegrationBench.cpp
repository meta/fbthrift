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
 * ThriftServer Integration Microbenchmarks
 *
 * Compares two fast_thrift server pipeline implementations:
 * 1. FastThrift with ThriftServerChannel — two-pipeline via transport adapter:
 *    Rocket pipeline: TransportHandler → [rocket handlers] →
 *    RocketServerAppAdapter
 *    Thrift pipeline: ThriftServerTransportAdapter → ThriftServerChannel
 *    (baseline)
 * 2. FastThrift with ThriftServerAppAdapter — two-pipeline via transport
 *    adapter:
 *    Rocket pipeline: TransportHandler → [rocket handlers] →
 *    RocketServerAppAdapter
 *    Thrift pipeline: ThriftServerTransportAdapter → ThriftServerAppAdapter
 *
 * Uses BENCHMARK for ThriftServerChannel baseline and BENCHMARK_RELATIVE for
 * ThriftServerAppAdapter, so output shows relative performance.
 */

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <folly/io/IOBuf.h>
#include <folly/io/IOBufQueue.h>
#include <folly/io/async/EventBase.h>

#include <thrift/lib/cpp2/async/AsyncProcessor.h>
#include <thrift/lib/cpp2/async/ResponseChannel.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/BufferAllocator.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineRef.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/StaticPipelineBuilder.h>
#include <thrift/lib/cpp2/fast_thrift/frame/FrameType.h>
#include <thrift/lib/cpp2/fast_thrift/frame/handler/FrameCodecHandler.h>
#include <thrift/lib/cpp2/fast_thrift/frame/read/FrameLengthParser.h>
#include <thrift/lib/cpp2/fast_thrift/frame/read/handler/FrameDefragmentationHandler.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/FragmentationHandlerConfig.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/FrameHeaders.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/FrameWriter.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/handler/FrameFragmentationHandler.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/handler/FrameLengthEncoderHandler.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/common/RocketStreamContext.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/server/Messages.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/server/RocketServerEventFactory.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/server/adapter/RocketServerAppAdapter.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/server/common/RocketServerConnection.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/server/handler/RocketServerMessageMarshalHandler.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/server/handler/RocketServerRequestResponseHandler.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/server/handler/RocketServerSetupFrameHandler.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/server/handler/RocketServerStreamStateHandler.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/ThriftServerChannel.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/adapter/ThriftServerAppAdapter.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/adapter/ThriftServerTransportAdapter.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/Messages.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/MethodMetadata.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/ThriftConnContext.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/handler/ThriftServerCompressionHandler.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/handler/ThriftServerConnectionCloseHandler.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/handler/ThriftServerRequestContextHandler.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/handler/ThriftServerSetupHandler.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/util/ResponsePayloads.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/test/if/gen-cpp2/FastThriftServer.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/test/if/gen-cpp2/FastThriftServer.tcc>
#include <thrift/lib/cpp2/fast_thrift/transport/TransportHandler.h>
#include <thrift/lib/cpp2/fast_thrift/transport/bench/BenchAsyncTransport.h>
#include <thrift/lib/cpp2/protocol/BinaryProtocol.h>
#include <thrift/lib/cpp2/server/Cpp2Worker.h>
#include <thrift/lib/thrift/gen-cpp2/RpcMetadata_types.h>

using namespace folly;
using namespace apache::thrift::fast_thrift::channel_pipeline;
using namespace apache::thrift::fast_thrift;
using namespace apache::thrift::fast_thrift::frame;
using namespace apache::thrift::fast_thrift::frame::read::handler;
using namespace apache::thrift::fast_thrift::frame::write::handler;
using namespace apache::thrift::fast_thrift::frame::write;
using namespace apache::thrift::fast_thrift::transport::bench;
using namespace apache::thrift::fast_thrift::thrift::test::integration;

namespace {

// =============================================================================
// Constants
// =============================================================================

constexpr size_t kPayloadSize = 4'096;

HANDLER_TAG(frame_length_encoder_handler);
HANDLER_TAG(frame_codec_handler);
HANDLER_TAG(frame_defragmentation_handler);
HANDLER_TAG(frame_fragmentation_handler);
HANDLER_TAG(rocket_server_message_marshal_handler);
HANDLER_TAG(server_setup_frame_handler);
HANDLER_TAG(server_request_response_frame_handler);
HANDLER_TAG(server_stream_state_handler);
HANDLER_TAG(thrift_server_request_context_handler);
HANDLER_TAG(thrift_server_compression_handler);
HANDLER_TAG(thrift_server_connection_close_handler);
HANDLER_TAG(thrift_server_setup_handler);

// =============================================================================
// Echo AsyncProcessor — synchronously echoes request payload as reply
// =============================================================================

class EchoProcessor : public apache::thrift::AsyncProcessor {
 public:
  void processSerializedCompressedRequestWithMetadata(
      apache::thrift::ResponseChannelRequest::UniquePtr req,
      apache::thrift::SerializedCompressedRequest&& serializedRequest,
      const apache::thrift::AsyncProcessorFactory::MethodMetadata&,
      apache::thrift::protocol::PROTOCOL_TYPES,
      apache::thrift::Cpp2RequestContext*,
      folly::EventBase*,
      apache::thrift::concurrency::ThreadManager*) override {
    if (!req->isOneway()) {
      auto data = std::move(serializedRequest).uncompress().buffer;
      req->sendReply(
          apache::thrift::ResponsePayload::create(std::move(data)),
          nullptr,
          folly::none);
    }
  }

  void processInteraction(apache::thrift::ServerRequest&&) override {}
};

class NoopProcessor : public apache::thrift::AsyncProcessor {
 public:
  void processSerializedCompressedRequestWithMetadata(
      apache::thrift::ResponseChannelRequest::UniquePtr,
      apache::thrift::SerializedCompressedRequest&&,
      const apache::thrift::AsyncProcessorFactory::MethodMetadata&,
      apache::thrift::protocol::PROTOCOL_TYPES,
      apache::thrift::Cpp2RequestContext*,
      folly::EventBase*,
      apache::thrift::concurrency::ThreadManager*) override {}

  void processInteraction(apache::thrift::ServerRequest&&) override {}
};

template <typename ProcessorT>
class BenchProcessorFactory : public apache::thrift::AsyncProcessorFactory {
 public:
  BenchProcessorFactory() : processor_(std::make_shared<ProcessorT>()) {}

  std::unique_ptr<apache::thrift::AsyncProcessor> getProcessor() override {
    return std::unique_ptr<apache::thrift::AsyncProcessor>(
        new ForwardingProcessor(processor_));
  }

  std::vector<apache::thrift::ServiceHandlerBase*> getServiceHandlers()
      override {
    return {};
  }

  CreateMethodMetadataResult createMethodMetadata() override { return {}; }

 private:
  class ForwardingProcessor : public apache::thrift::AsyncProcessor {
   public:
    explicit ForwardingProcessor(std::shared_ptr<ProcessorT> inner)
        : inner_(std::move(inner)) {}

    void processSerializedCompressedRequestWithMetadata(
        apache::thrift::ResponseChannelRequest::UniquePtr req,
        apache::thrift::SerializedCompressedRequest&& serializedRequest,
        const apache::thrift::AsyncProcessorFactory::MethodMetadata& metadata,
        apache::thrift::protocol::PROTOCOL_TYPES protocolId,
        apache::thrift::Cpp2RequestContext* ctx,
        folly::EventBase* evb,
        apache::thrift::concurrency::ThreadManager* tm) override {
      inner_->processSerializedCompressedRequestWithMetadata(
          std::move(req),
          std::move(serializedRequest),
          metadata,
          protocolId,
          ctx,
          evb,
          tm);
    }

    void processInteraction(apache::thrift::ServerRequest&&) override {}

   private:
    std::shared_ptr<ProcessorT> inner_;
  };

  std::shared_ptr<ProcessorT> processor_;
};

// =============================================================================
// BenchServerAppAdapter — test subclass exposing addMethodHandler
// =============================================================================

class BenchServerAppAdapter : public thrift::ThriftServerAppAdapter {
 public:
  using Ptr = std::unique_ptr<BenchServerAppAdapter, Destructor>;

  void registerMethod(std::string_view name, RequestResponseProcessFn handler) {
    addMethodHandler(name, handler);
  }
};

// =============================================================================
// Helpers — fast_thrift frame construction
// =============================================================================

std::unique_ptr<folly::IOBuf> serializeRequestMetadata(
    const apache::thrift::RequestRpcMetadata& metadata) {
  apache::thrift::BinaryProtocolWriter writer;
  folly::IOBufQueue queue(folly::IOBufQueue::cacheChainLength());
  writer.setOutput(&queue);
  metadata.write(&writer);
  return queue.move();
}

apache::thrift::RequestRpcMetadata createMinimalRequestMetadata() {
  apache::thrift::RequestRpcMetadata metadata;
  metadata.name() = "benchMethod";
  metadata.kind() = apache::thrift::RpcKind::SINGLE_REQUEST_SINGLE_RESPONSE;
  metadata.protocol() = apache::thrift::ProtocolId::BINARY;
  return metadata;
}

apache::thrift::RequestRpcMetadata createRequestMetadataWithHeaders() {
  apache::thrift::RequestRpcMetadata metadata;
  metadata.name() = "benchMethod";
  metadata.kind() = apache::thrift::RpcKind::SINGLE_REQUEST_SINGLE_RESPONSE;
  metadata.protocol() = apache::thrift::ProtocolId::BINARY;
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

// =============================================================================
// Rocket Pipeline Builder — builds the rocket pipeline for two-pipeline mode
// =============================================================================

// Build a rocket::server::RocketServerConnection wired with the standard
// rocket handler stack. Caller owns the returned connection; pass it into
// ThriftServerTransportAdapter to anchor lifecycle. The test transport
// pointer is captured into *outTransport.
std::unique_ptr<rocket::server::RocketServerConnection> buildRocketConnection(
    folly::EventBase* evb, BenchAsyncTransport** outTransport) {
  auto rocketConn = std::make_unique<rocket::server::RocketServerConnection>();

  auto transport =
      folly::AsyncTransport::UniquePtr(new BenchAsyncTransport(evb));
  *outTransport = static_cast<BenchAsyncTransport*>(transport.get());

  auto transportHandler = transport::TransportHandlerT<
      rocket::server::RocketServerEventFactory>::create(std::move(transport));

  rocketConn->pipeline =
      StaticPipelineBuilder<
          transport::TransportHandlerT<
              rocket::server::RocketServerEventFactory>,
          rocket::server::RocketServerAppAdapter,
          SimpleBufferAllocator>()
          .setEventBase(evb)
          .setHead(transportHandler.get())
          .setTail(rocketConn->appAdapter.get())
          .setAllocator(&rocketConn->allocator)
          .addState<apache::thrift::fast_thrift::rocket::RocketStreamContexts>()

          .addNextOutbound<FrameLengthEncoderHandler>(
              frame_length_encoder_handler_tag)
          .addNextDuplex<frame::handler::FrameCodecHandler>(
              frame_codec_handler_tag)
          .addNextInbound<frame::read::handler::FrameDefragmentationHandler>(
              frame_defragmentation_handler_tag)
          .addNextOutbound<frame::write::handler::FrameFragmentationHandler>(
              frame_fragmentation_handler_tag,
              frame::write::FragmentationHandlerConfig{})
          .addNextDuplex<
              rocket::server::handler::RocketServerMessageMarshalHandler>(
              rocket_server_message_marshal_handler_tag)
          .addNextDuplex<
              rocket::server::handler::RocketServerSetupFrameHandler>(
              server_setup_frame_handler_tag)
          .addNextDuplex<
              rocket::server::handler::RocketServerStreamStateHandler>(
              server_stream_state_handler_tag)
          .addNextDuplex<
              rocket::server::handler::RocketServerRequestResponseHandler>(
              server_request_response_frame_handler_tag)
          .build();

  rocketConn->appAdapter->setPipeline(rocketConn->pipeline.get());
  transportHandler->setPipeline(rocketConn->pipeline.get());
  rocketConn->transportHandler = std::move(transportHandler);
  rocketConn->transportHandler->onConnect();

  return rocketConn;
}

// =============================================================================
// ThriftServerChannel Benchmark Fixture (two-pipeline)
//
// ThriftServerChannel consumes ThriftServerRequestMessage, so it requires
// the two-pipeline architecture with ThriftServerTransportAdapter bridging
// between the rocket pipeline and the thrift pipeline.
// =============================================================================

template <typename ProcessorT>
struct ChannelBenchFixture {
  folly::EventBase evb;
  BenchAsyncTransport* testTransport{nullptr};

  // Thrift pipeline
  std::unique_ptr<thrift::server::ThriftServerTransportAdapter>
      transportAdapter;
  std::shared_ptr<thrift::ThriftServerChannel> serverChannel;
  std::shared_ptr<BenchProcessorFactory<ProcessorT>> processorFactory;
  SimpleBufferAllocator thriftAllocator;
  PipelineOwner thriftPipeline;

  void setup() {
    auto rocketConn = buildRocketConnection(&evb, &testTransport);

    processorFactory = std::make_shared<BenchProcessorFactory<ProcessorT>>();
    serverChannel =
        std::make_shared<thrift::ThriftServerChannel>(processorFactory);

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

    auto setupFrame = serialize(
        SetupHeader{
            .majorVersion = 1,
            .minorVersion = 0,
            .keepaliveTime = 30000,
            .maxLifetime = 60000},
        nullptr,
        nullptr);
    testTransport->injectReadData(prependLengthPrefix(std::move(setupFrame)));
    evb.loopOnce(EVLOOP_NONBLOCK);
  }

  void injectFrame(std::unique_ptr<folly::IOBuf> frame) {
    testTransport->injectReadData(prependLengthPrefix(std::move(frame)));
  }
};

// =============================================================================
// ThriftServerAppAdapter Benchmark Fixture (two-pipeline)
// =============================================================================

struct AppAdapterBenchFixture {
  folly::EventBase evb;
  BenchAsyncTransport* testTransport{nullptr};
  thrift::ThriftServerMethodMetadataRegistry methodMetadata;

  // Thrift pipeline (transport adapter → app adapter)
  std::unique_ptr<thrift::server::ThriftServerTransportAdapter>
      transportAdapter;
  BenchServerAppAdapter::Ptr adapter;
  SimpleBufferAllocator thriftAllocator;
  PipelineOwner thriftPipeline;
  boost::intrusive_ptr<thrift::ThriftConnContext> connContext;

  void setup(bool echo, bool withContextHandlers = false) {
    auto rocketConn = buildRocketConnection(&evb, &testTransport);

    adapter.reset(new BenchServerAppAdapter());

    if (echo) {
      adapter->registerMethod(
          "benchMethod",
          +[](thrift::ThriftServerAppAdapter* self,
              uint32_t streamId,
              std::unique_ptr<folly::IOBuf> data,
              apache::thrift::ProtocolId,
              thrift::ThriftRequestContextPtr) noexcept {
            auto md = std::make_unique<apache::thrift::ResponseRpcMetadata>();
            thrift::fillSuccessResponseMetadata(*md);
            self->writeResponse(
                thrift::makeResponseMessage(
                    streamId, std::move(data), std::move(md)));
          });
    } else {
      adapter->registerMethod(
          "benchMethod",
          +[](thrift::ThriftServerAppAdapter*,
              uint32_t,
              std::unique_ptr<folly::IOBuf>,
              apache::thrift::ProtocolId,
              thrift::ThriftRequestContextPtr) noexcept {});
    }

    transportAdapter =
        std::make_unique<thrift::server::ThriftServerTransportAdapter>(
            std::move(rocketConn));

    using Builder = StaticPipelineBuilder<
        thrift::server::ThriftServerTransportAdapter,
        BenchServerAppAdapter,
        SimpleBufferAllocator>;

    if (withContextHandlers) {
      methodMetadata.add(
          thrift::ThriftServerMethodMetadata{
              .serviceName = "BenchService",
              .definingServiceName = "BenchService",
              .methodName = "benchMethod",
              .qualifiedMethodName = "BenchService.benchMethod",
          });
      connContext = new thrift::ThriftConnContext();
      thriftPipeline =
          Builder()
              .setEventBase(&evb)
              .setHead(transportAdapter.get())
              .setTail(adapter.get())
              .setAllocator(&thriftAllocator)
              .addNextDuplexTemplate<thrift::ThriftServerCompressionHandler>(
                  thrift_server_compression_handler_tag)
              .addNextDuplexTemplate<
                  thrift::ThriftServerConnectionCloseHandler>(
                  thrift_server_connection_close_handler_tag)
              .addNextDuplexTemplate<thrift::ThriftServerRequestContextHandler>(
                  thrift_server_request_context_handler_tag,
                  /*requestExtensionLayout=*/nullptr,
                  &methodMetadata,
                  connContext,
                  /*enableRequestHeaders=*/false)
              .addNextDuplexTemplate<thrift::ThriftServerSetupHandler>(
                  thrift_server_setup_handler_tag)
              .build();
    } else {
      thriftPipeline =
          Builder()
              .setEventBase(&evb)
              .setHead(transportAdapter.get())
              .setTail(adapter.get())
              .setAllocator(&thriftAllocator)
              .addNextDuplexTemplate<thrift::ThriftServerCompressionHandler>(
                  thrift_server_compression_handler_tag)
              .addNextDuplexTemplate<
                  thrift::ThriftServerConnectionCloseHandler>(
                  thrift_server_connection_close_handler_tag)
              .addNextDuplexTemplate<thrift::ThriftServerSetupHandler>(
                  thrift_server_setup_handler_tag)
              .build();
    }

    transportAdapter->setPipeline(thriftPipeline.get());
    adapter->setPipeline(thriftPipeline.get());

    auto setupFrame = serialize(
        SetupHeader{
            .majorVersion = 1,
            .minorVersion = 0,
            .keepaliveTime = 30000,
            .maxLifetime = 60000},
        nullptr,
        nullptr);
    testTransport->injectReadData(prependLengthPrefix(std::move(setupFrame)));
    evb.loopOnce(EVLOOP_NONBLOCK);
  }

  void injectFrame(std::unique_ptr<folly::IOBuf> frame) {
    testTransport->injectReadData(prependLengthPrefix(std::move(frame)));
  }
};

// =============================================================================
// Request Path Benchmarks - Minimal Metadata
// =============================================================================

BENCHMARK(Channel_Request, iters) {
  folly::BenchmarkSuspender suspender;
  ChannelBenchFixture<NoopProcessor> fixture;
  fixture.setup();
  suspender.dismiss();

  auto metadataTemplate =
      serializeRequestMetadata(createMinimalRequestMetadata());
  auto payloadTemplate = serializeEmptyPargs();

  std::vector<std::unique_ptr<folly::IOBuf>> requests;
  requests.reserve(iters);
  for (uint32_t i = 0; i < iters; ++i) {
    requests.push_back(createFastThriftRequestFrame(
        i * 2 + 1, metadataTemplate->clone(), payloadTemplate->clone()));
  }

  for (uint32_t i = 0; i < iters; ++i) {
    fixture.injectFrame(std::move(requests[i]));
    fixture.evb.loopOnce(EVLOOP_NONBLOCK);
  }
  suspender.rehire();
}

BENCHMARK_RELATIVE(AppAdapter_Request, iters) {
  folly::BenchmarkSuspender suspender;
  AppAdapterBenchFixture fixture;
  fixture.setup(false);
  suspender.dismiss();

  auto metadataTemplate =
      serializeRequestMetadata(createMinimalRequestMetadata());
  auto payloadTemplate = serializeEmptyPargs();

  std::vector<std::unique_ptr<folly::IOBuf>> requests;
  requests.reserve(iters);
  for (uint32_t i = 0; i < iters; ++i) {
    requests.push_back(createFastThriftRequestFrame(
        i * 2 + 1, metadataTemplate->clone(), payloadTemplate->clone()));
  }

  for (uint32_t i = 0; i < iters; ++i) {
    fixture.injectFrame(std::move(requests[i]));
    fixture.evb.loopOnce(EVLOOP_NONBLOCK);
  }
  suspender.rehire();
}

BENCHMARK_RELATIVE(AppAdapter_Request_Ctx, iters) {
  folly::BenchmarkSuspender suspender;
  AppAdapterBenchFixture fixture;
  fixture.setup(/*echo=*/false, /*withContextHandlers=*/true);
  suspender.dismiss();

  auto metadataTemplate =
      serializeRequestMetadata(createMinimalRequestMetadata());
  auto payloadTemplate = serializeEmptyPargs();

  std::vector<std::unique_ptr<folly::IOBuf>> requests;
  requests.reserve(iters);
  for (uint32_t i = 0; i < iters; ++i) {
    requests.push_back(createFastThriftRequestFrame(
        i * 2 + 1, metadataTemplate->clone(), payloadTemplate->clone()));
  }

  for (uint32_t i = 0; i < iters; ++i) {
    fixture.injectFrame(std::move(requests[i]));
    fixture.evb.loopOnce(EVLOOP_NONBLOCK);
  }
  suspender.rehire();
}

BENCHMARK_DRAW_LINE();

// =============================================================================
// Request Path Benchmarks - With Headers
// =============================================================================

BENCHMARK(Channel_Request_Headers, iters) {
  folly::BenchmarkSuspender suspender;
  ChannelBenchFixture<NoopProcessor> fixture;
  fixture.setup();
  suspender.dismiss();

  auto metadataTemplate =
      serializeRequestMetadata(createRequestMetadataWithHeaders());
  auto payloadTemplate = serializeEmptyPargs();

  std::vector<std::unique_ptr<folly::IOBuf>> requests;
  requests.reserve(iters);
  for (uint32_t i = 0; i < iters; ++i) {
    requests.push_back(createFastThriftRequestFrame(
        i * 2 + 1, metadataTemplate->clone(), payloadTemplate->clone()));
  }

  for (uint32_t i = 0; i < iters; ++i) {
    fixture.injectFrame(std::move(requests[i]));
    fixture.evb.loopOnce(EVLOOP_NONBLOCK);
  }
  suspender.rehire();
}

BENCHMARK_RELATIVE(AppAdapter_Request_Headers, iters) {
  folly::BenchmarkSuspender suspender;
  AppAdapterBenchFixture fixture;
  fixture.setup(false);
  suspender.dismiss();

  auto metadataTemplate =
      serializeRequestMetadata(createRequestMetadataWithHeaders());
  auto payloadTemplate = serializeEmptyPargs();

  std::vector<std::unique_ptr<folly::IOBuf>> requests;
  requests.reserve(iters);
  for (uint32_t i = 0; i < iters; ++i) {
    requests.push_back(createFastThriftRequestFrame(
        i * 2 + 1, metadataTemplate->clone(), payloadTemplate->clone()));
  }

  for (uint32_t i = 0; i < iters; ++i) {
    fixture.injectFrame(std::move(requests[i]));
    fixture.evb.loopOnce(EVLOOP_NONBLOCK);
  }
  suspender.rehire();
}

BENCHMARK_DRAW_LINE();

// =============================================================================
// Request-Response Round Trip
// =============================================================================

BENCHMARK(Channel_RoundTrip, iters) {
  folly::BenchmarkSuspender suspender;
  ChannelBenchFixture<EchoProcessor> fixture;
  fixture.setup();
  suspender.dismiss();

  auto metadataTemplate =
      serializeRequestMetadata(createMinimalRequestMetadata());
  auto payloadTemplate = serializeEchoPargs(std::string(kPayloadSize, 'x'));

  std::vector<std::unique_ptr<folly::IOBuf>> requests;
  requests.reserve(iters);
  for (uint32_t i = 0; i < iters; ++i) {
    requests.push_back(createFastThriftRequestFrame(
        i * 2 + 1, metadataTemplate->clone(), payloadTemplate->clone()));
  }

  for (uint32_t i = 0; i < iters; ++i) {
    fixture.injectFrame(std::move(requests[i]));
    fixture.evb.loopOnce(EVLOOP_NONBLOCK);
    fixture.testTransport->clearWrittenData();
  }
  suspender.rehire();
}

BENCHMARK_RELATIVE(AppAdapter_RoundTrip, iters) {
  folly::BenchmarkSuspender suspender;
  AppAdapterBenchFixture fixture;
  fixture.setup(true);
  suspender.dismiss();

  auto metadataTemplate =
      serializeRequestMetadata(createMinimalRequestMetadata());
  auto payloadTemplate = serializeEchoPargs(std::string(kPayloadSize, 'x'));

  std::vector<std::unique_ptr<folly::IOBuf>> requests;
  requests.reserve(iters);
  for (uint32_t i = 0; i < iters; ++i) {
    requests.push_back(createFastThriftRequestFrame(
        i * 2 + 1, metadataTemplate->clone(), payloadTemplate->clone()));
  }

  for (uint32_t i = 0; i < iters; ++i) {
    fixture.injectFrame(std::move(requests[i]));
    fixture.evb.loopOnce(EVLOOP_NONBLOCK);
    fixture.testTransport->clearWrittenData();
  }
  suspender.rehire();
}

BENCHMARK_RELATIVE(AppAdapter_RoundTrip_Ctx, iters) {
  folly::BenchmarkSuspender suspender;
  AppAdapterBenchFixture fixture;
  fixture.setup(/*echo=*/true, /*withContextHandlers=*/true);
  suspender.dismiss();

  auto metadataTemplate =
      serializeRequestMetadata(createMinimalRequestMetadata());
  auto payloadTemplate = serializeEchoPargs(std::string(kPayloadSize, 'x'));

  std::vector<std::unique_ptr<folly::IOBuf>> requests;
  requests.reserve(iters);
  for (uint32_t i = 0; i < iters; ++i) {
    requests.push_back(createFastThriftRequestFrame(
        i * 2 + 1, metadataTemplate->clone(), payloadTemplate->clone()));
  }

  for (uint32_t i = 0; i < iters; ++i) {
    fixture.injectFrame(std::move(requests[i]));
    fixture.evb.loopOnce(EVLOOP_NONBLOCK);
    fixture.testTransport->clearWrittenData();
  }
  suspender.rehire();
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  runBenchmarks();
  return 0;
}
