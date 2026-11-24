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

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include <gtest/gtest.h>
#include <fizz/client/AsyncFizzClient.h>
#include <fizz/client/FizzClientContext.h>

#include <folly/ExceptionWrapper.h>
#include <folly/io/Cursor.h>
#include <folly/io/IOBuf.h>
#include <folly/io/IOBufQueue.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/ScopedEventBaseThread.h>
#include <folly/portability/GMock.h>
#include <folly/synchronization/Baton.h>

#include <thrift/lib/cpp2/Flags.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/BufferAllocator.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/HandlerTag.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineRef.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/StaticPipelineBuilder.h>
#include <thrift/lib/cpp2/fast_thrift/frame/read/AlignedParser.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/handler/FrameLengthEncoderHandler.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/client/adapter/RocketClientAppAdapter.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/client/common/RocketClientConnection.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/client/handler/RocketClientConnectionErrorHandler.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/client/handler/RocketClientFrameCodecHandler.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/client/handler/RocketClientRequestResponseHandler.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/client/handler/RocketClientSetupFrameHandler.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/client/handler/RocketClientStreamStateHandler.h>
#include <thrift/lib/cpp2/fast_thrift/security/FizzServerCertConfig.h>
#include <thrift/lib/cpp2/fast_thrift/security/test/TestCert.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/client/ThriftClientAppAdapter.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/client/adapter/ThriftClientTransportAdapter.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/client/handler/ThriftClientRequestTimeoutHandler.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/FastThriftServer.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/test/if/gen-cpp2/AlignedParserServer.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/test/if/gen-cpp2/AlignedParserServer.tcc>
#include <thrift/lib/cpp2/fast_thrift/thrift/test/if/gen-cpp2/AlignedParserServerFastClient.h>
#include <thrift/lib/cpp2/fast_thrift/transport/TransportHandler.h>
#include <thrift/lib/cpp2/protocol/BinaryProtocol.h>
#include <thrift/lib/thrift/gen-cpp2/RpcMetadata_constants.h>
#include <thrift/lib/thrift/gen-cpp2/RpcMetadata_types.h>

namespace apache::thrift::fast_thrift::thrift::test::aligned_parser {

using namespace testing;

namespace {

HANDLER_TAG(client_frame_length_encoder_handler);
HANDLER_TAG(rocket_client_frame_codec_handler);
HANDLER_TAG(rocket_client_setup_handler);
HANDLER_TAG(rocket_client_request_response_handler);
HANDLER_TAG(rocket_client_connection_error_handler);
HANDLER_TAG(rocket_client_stream_state_handler);
HANDLER_TAG(thrift_client_request_timeout_handler);

constexpr std::chrono::milliseconds kDefaultRequestTimeout{60'000};

using ClientConnection =
    ::apache::thrift::fast_thrift::rocket::client::RocketClientConnectionT<
        ::apache::thrift::fast_thrift::transport::NoOpWriteCompleteEventFactory,
        ::apache::thrift::fast_thrift::frame::read::AlignedParser>;
using ClientTransportHandler = ClientConnection::TransportHandler;
using ClientTransportAdapter = ::apache::thrift::fast_thrift::thrift::client::
    ThriftClientTransportAdapterT<
        ::apache::thrift::fast_thrift::transport::NoOpWriteCompleteEventFactory,
        ::apache::thrift::fast_thrift::frame::read::AlignedParser>;
using ClientAppAdapter =
    ::apache::thrift::fast_thrift::thrift::ThriftClientAppAdapter;
using FastClientType =
    ::apache::thrift::FastClient<AlignedParserServer, ClientAppAdapter>;

class Handler : public apache::thrift::FastServiceHandler<AlignedParserServer> {
 public:
  void async_tm_consume(
      FastHandlerCallbackPtr<void> callback,
      std::unique_ptr<Request> request) override {
    const folly::IOBuf& data = *request->data().value();
    addressRemainder_.store(
        reinterpret_cast<uintptr_t>(data.data()) % 16,
        std::memory_order_relaxed);
    chainElements_.store(data.countChainElements(), std::memory_order_relaxed);
    dataSize_.store(data.computeChainDataLength(), std::memory_order_relaxed);
    callback->done();
  }

  void async_tm_echo(
      FastHandlerCallbackPtr<std::unique_ptr<std::string>> callback,
      std::unique_ptr<std::string> value) override {
    callback->result(std::move(value));
  }

  size_t addressRemainder() const noexcept {
    return addressRemainder_.load(std::memory_order_relaxed);
  }

  size_t chainElements() const noexcept {
    return chainElements_.load(std::memory_order_relaxed);
  }

  size_t dataSize() const noexcept {
    return dataSize_.load(std::memory_order_relaxed);
  }

 private:
  std::atomic<size_t> addressRemainder_{16};
  std::atomic<size_t> chainElements_{0};
  std::atomic<size_t> dataSize_{0};
};

FastThriftServerConfig makeServerConfig() {
  FastThriftServerConfig config;
  config.address = folly::SocketAddress("::1", 0);
  config.numIOThreads = 1;
  config.useAlignedParser = true;
  return config;
}

security::FizzServerCertConfig makeTlsConfig() {
  const security::test::TestCert cert = security::test::makeTestCert();
  security::FizzServerCertConfig config;
  config.certPem = cert.certPem;
  config.keyPem = cert.keyPem;
  config.clientAuth = fizz::server::ClientAuthMode::None;
  return config;
}

folly::AsyncTransport::UniquePtr connectFizz(
    folly::EventBase* evb, const folly::SocketAddress& address) {
  constexpr int kConnectTimeoutMs = 5000;

  struct Callback : public folly::AsyncSocket::ConnectCallback,
                    public fizz::client::AsyncFizzClient::HandshakeCallback {
    folly::Baton<>& done;
    folly::exception_wrapper& error;
    fizz::client::AsyncFizzClient::UniquePtr& client;

    Callback(
        folly::Baton<>& done,
        folly::exception_wrapper& error,
        fizz::client::AsyncFizzClient::UniquePtr& client)
        : done(done), error(error), client(client) {}

    void connectSuccess() noexcept override {
      client->connect(
          this,
          /*verifier=*/nullptr,
          /*sni=*/folly::none,
          /*pskIdentity=*/folly::none,
          /*echConfigs=*/folly::none);
    }

    void connectErr(const folly::AsyncSocketException& ex) noexcept override {
      error = folly::exception_wrapper(ex);
      done.post();
    }

    void fizzHandshakeSuccess(
        fizz::client::AsyncFizzClient*) noexcept override {
      done.post();
    }

    void fizzHandshakeError(
        fizz::client::AsyncFizzClient*,
        folly::exception_wrapper ex) noexcept override {
      error = std::move(ex);
      done.post();
    }
  };

  folly::Baton<> done;
  folly::exception_wrapper error;
  fizz::client::AsyncFizzClient::UniquePtr client;
  Callback callback{done, error, client};

  evb->runInEventBaseThreadAndWait([&] {
    folly::AsyncSocket::UniquePtr socket = folly::AsyncSocket::newSocket(evb);
    std::shared_ptr<fizz::client::FizzClientContext> context =
        std::make_shared<fizz::client::FizzClientContext>();
    folly::AsyncSocket* const socketPtr = socket.get();
    client.reset(new fizz::client::AsyncFizzClient(std::move(socket), context));
    socketPtr->connect(&callback, address, kConnectTimeoutMs);
  });

  done.wait();
  if (error) {
    evb->runInEventBaseThreadAndWait([&] { client.reset(); });
    return nullptr;
  }
  return folly::AsyncTransport::UniquePtr(client.release());
}

} // namespace

class FastThriftServerAlignedParserE2ETest : public Test {
 protected:
  void SetUp() override {
    handler_ = std::make_shared<Handler>();
    server_ = std::make_unique<FastThriftServer>(makeServerConfig());
    server_->setSSLConfig(makeTlsConfig());
    server_->setInterface(handler_);
    server_->start();
    clientThread_ = std::make_unique<folly::ScopedEventBaseThread>();
  }

  void TearDown() override {
    clientThread_->getEventBase()->runInEventBaseThreadAndWait([&] {
      if (clientPipeline_) {
        clientPipeline_->deactivate();
        clientPipeline_->close();
      }
      if (clientTransportAdapter_) {
        clientTransportAdapter_->resetPipeline();
      }
      client_.reset();
      clientPipeline_.reset();
      clientTransportAdapter_.reset();
    });
    clientThread_.reset();
    if (server_ != nullptr) {
      server_->stop();
    }
    server_.reset();
  }

  void createClient() {
    folly::EventBase* const evb = clientThread_->getEventBase();
    folly::AsyncTransport::UniquePtr transport =
        connectFizz(evb, server_->getAddress());
    if (transport == nullptr) {
      return;
    }

    ClientAppAdapter::Ptr appAdapter(new ClientAppAdapter(
        static_cast<uint16_t>(apache::thrift::protocol::T_BINARY_PROTOCOL)));

    evb->runInEventBaseThreadAndWait([&] {
      std::unique_ptr<ClientConnection> connection =
          std::make_unique<ClientConnection>();
      ClientTransportHandler::Ptr transportHandler =
          ClientTransportHandler::createWithParser(
              std::move(transport), frame::read::AlignedParser{});
      ClientTransportHandler* const transportHandlerPtr =
          transportHandler.get();
      clientTransportIsBufferMovable_ = transportHandlerPtr->isBufferMovable();
      connection->transportHandler = std::move(transportHandler);

      auto setupFactory = []() {
        apache::thrift::RequestSetupMetadata metadata;
        metadata.minVersion() = 8;
        metadata.maxVersion() = 10;
        metadata.clientMetadata().ensure().agent() =
            "fast_thrift_aligned_parser_e2e_test";

        apache::thrift::BinaryProtocolWriter writer;
        folly::IOBufQueue metadataBytes;
        writer.setOutput(&metadataBytes);
        metadata.write(&writer);

        folly::IOBufQueue setup;
        const uint32_t protocolKey =
            apache::thrift::RpcMetadata_constants::kRocketProtocolKey();
        folly::io::QueueAppender appender(&setup, sizeof(protocolKey));
        appender.writeBE<uint32_t>(protocolKey);
        setup.append(metadataBytes.move());
        return std::make_pair(setup.move(), std::unique_ptr<folly::IOBuf>());
      };

      connection->pipeline =
          channel_pipeline::StaticPipelineBuilder<
              ClientTransportHandler,
              rocket::client::RocketClientAppAdapter,
              channel_pipeline::SimpleBufferAllocator>()
              .setEventBase(evb)
              .setHead(transportHandlerPtr)
              .setTail(connection->appAdapter.get())
              .setAllocator(&connection->allocator)
              .addState<rocket::client::RocketClientStreamContexts>()
              .addNextOutbound<
                  frame::write::handler::FrameLengthEncoderHandler>(
                  client_frame_length_encoder_handler_tag)
              .addNextDuplex<
                  rocket::client::handler::RocketClientFrameCodecHandler>(
                  rocket_client_frame_codec_handler_tag)
              .addNextDuplex<
                  rocket::client::handler::RocketClientSetupFrameHandler>(
                  rocket_client_setup_handler_tag, std::move(setupFactory))
              .addNextInbound<
                  rocket::client::handler::RocketClientConnectionErrorHandler>(
                  rocket_client_connection_error_handler_tag)
              .addNextDuplex<
                  rocket::client::handler::RocketClientStreamStateHandler>(
                  rocket_client_stream_state_handler_tag)
              .addNextInbound<
                  rocket::client::handler::RocketClientRequestResponseHandler>(
                  rocket_client_request_response_handler_tag)
              .build();

      connection->appAdapter->setPipeline(connection->pipeline.get());
      connection->transportHandler->setPipeline(connection->pipeline.get());

      clientTransportAdapter_ =
          std::make_unique<ClientTransportAdapter>(std::move(connection));
      clientPipeline_ =
          channel_pipeline::StaticPipelineBuilder<
              ClientTransportAdapter,
              ClientAppAdapter,
              channel_pipeline::SimpleBufferAllocator>()
              .setEventBase(evb)
              .setHead(clientTransportAdapter_.get())
              .setTail(appAdapter.get())
              .setAllocator(&clientAllocator_)
              .addNextDuplex<
                  thrift::client::handler::ThriftClientRequestTimeoutHandler>(
                  thrift_client_request_timeout_handler_tag,
                  kDefaultRequestTimeout)
              .build();

      appAdapter->setPipeline(clientPipeline_.get());
      clientTransportAdapter_->setPipeline(clientPipeline_.get());
      transportHandlerPtr->onConnect();
    });

    client_ = std::make_unique<FastClientType>(std::move(appAdapter));
  }

  std::shared_ptr<Handler> handler_;
  std::unique_ptr<FastThriftServer> server_;
  std::unique_ptr<folly::ScopedEventBaseThread> clientThread_;
  channel_pipeline::SimpleBufferAllocator clientAllocator_;
  std::unique_ptr<FastClientType> client_;
  std::unique_ptr<ClientTransportAdapter> clientTransportAdapter_;
  channel_pipeline::PipelineOwner clientPipeline_;
  bool clientTransportIsBufferMovable_{true};
};

TEST_F(FastThriftServerAlignedParserE2ETest, AlignsRequestData) {
  constexpr size_t kDataSize = 4096;
  Request request;
  request.data() = folly::IOBuf::copyBuffer(std::string(kDataSize, 'd'));
  request.marker() = 42;

  createClient();
  EXPECT_THAT(client_, NotNull());
  if (client_ == nullptr) {
    return;
  }
  client_->sync_consume(request);

  EXPECT_THAT(handler_->addressRemainder(), Eq(0));
  EXPECT_THAT(handler_->chainElements(), Eq(1));
  EXPECT_THAT(handler_->dataSize(), Eq(kDataSize));
}

TEST_F(FastThriftServerAlignedParserE2ETest, ClientParsesPayloadResponse) {
  const std::string value(4096, 'd');
  createClient();
  EXPECT_THAT(client_, NotNull());
  if (client_ == nullptr) {
    return;
  }

  EXPECT_THAT(clientTransportIsBufferMovable_, IsFalse());
  std::string response;
  client_->sync_echo(response, value);
  EXPECT_THAT(response, Eq(value));
}

} // namespace apache::thrift::fast_thrift::thrift::test::aligned_parser
