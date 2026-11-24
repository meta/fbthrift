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
#include <memory>
#include <stdexcept>
#include <string>

#include <folly/Synchronized.h>
#include <folly/coro/GtestHelpers.h>
#include <folly/futures/Promise.h>
#include <folly/io/IOBuf.h>
#include <folly/io/IOBufQueue.h>
#include <thrift/lib/cpp2/async/ClientCompressionFlags.h>
#include <thrift/lib/cpp2/async/RequestCallback.h>
#include <thrift/lib/cpp2/async/RocketClientChannel.h>
#include <thrift/lib/cpp2/protocol/CompactProtocol.h>
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include <thrift/lib/cpp2/test/e2e/E2ETestFixture.h>
#include <thrift/lib/cpp2/test/e2e/gen-cpp2/TestRequestResponseService.h>
#include <thrift/lib/cpp2/transport/rocket/compression/CompressionManager.h>
#include <thrift/lib/cpp2/util/Checksum.h>

THRIFT_FLAG_DECLARE_bool(thrift_server_compress_response_on_cpu);

namespace apache::thrift {

namespace {

using detail::test::RRDeclaredException;
using detail::test::TestRequestResponseService;

const std::string kMessage(4096, 'x');

class CompressedResponsePassthroughE2ETest : public test::E2ETestFixture {
 protected:
  void SetUp() override {
    THRIFT_FLAG_SET_MOCK(thrift_server_compress_response_on_cpu, false);
    THRIFT_FLAG_SET_MOCK(thrift_client_compress_request_on_cpu, false);
  }

  using ServerConfigCb = ScopedServerInterfaceThread::ServerConfigCb;

  void startServer(
      std::shared_ptr<AsyncProcessorFactory> handler,
      const std::optional<CompressionConfig>& clientCompressionConfig,
      ServerConfigCb serverConfigCb = {}) {
    testConfig(
        {std::move(handler),
         std::move(serverConfigCb),
         [clientCompressionConfig](
             folly::AsyncSocket::UniquePtr socket) -> RequestChannel::Ptr {
           auto channel = RocketClientChannel::newChannel(std::move(socket));
           if (clientCompressionConfig) {
             channel->setDesiredCompressionConfig(*clientCompressionConfig);
           }
           return std::move(channel);
         }});
  }
};

CompressionConfig compressionConfigFor(CodecConfig codec) {
  CompressionConfig config;
  config.codecConfig() = std::move(codec);
  return config;
}

CompressionConfig zstdConfig() {
  CodecConfig codec;
  codec.zstdConfig().ensure();
  return compressionConfigFor(std::move(codec));
}

PayloadMetadata responsePayloadMetadata() {
  PayloadMetadata metadata;
  metadata.responseMetadata().ensure();
  return metadata;
}

std::string toString(SerializedResponse& response) {
  if (response.usesIOBufChain()) {
    auto bytes = response.chainBuffer->coalesce();
    return std::string(bytes.begin(), bytes.end());
  }
  return response.buffer ? response.buffer->to<std::string>() : std::string();
}

std::string compress(const std::string& data, CompressionAlgorithm algo) {
  return rocket::CompressionManager()
      .compressBuffer(folly::IOBuf::copyBuffer(data), algo)
      ->to<std::string>();
}

std::string uncompress(const std::string& data, CompressionAlgorithm algo) {
  return rocket::CompressionManager()
      .uncompressBuffer(folly::IOBuf::copyBuffer(data), algo)
      ->to<std::string>();
}

// Envelope-free compact struct `{fieldId: string}` -- the shape of both the
// echo() arguments and its result.
std::string serializeStringStruct(int16_t fieldId, const std::string& value) {
  folly::IOBufQueue queue;
  CompactProtocolWriter writer;
  writer.setOutput(&queue);
  writer.writeStructBegin("");
  writer.writeFieldBegin("", protocol::T_STRING, fieldId);
  writer.writeString(value);
  writer.writeFieldEnd();
  writer.writeFieldStop();
  writer.writeStructEnd();
  return queue.move()->to<std::string>();
}

std::string serializeEmptyStruct() {
  folly::IOBufQueue queue;
  CompactProtocolWriter writer;
  writer.setOutput(&queue);
  writer.writeStructBegin("");
  writer.writeFieldStop();
  writer.writeStructEnd();
  return queue.move()->to<std::string>();
}

// Reads `{fieldId: string}` from an echo() result, or the message of
// `{1: RRDeclaredException}` from a canThrow() result.
std::string readStringField(const std::string& data, int16_t expectedId) {
  auto buf = folly::IOBuf::copyBuffer(data);
  CompactProtocolReader reader;
  reader.setInput(buf.get());
  std::string name;
  protocol::TType type;
  int16_t id;
  reader.readStructBegin(name);
  reader.readFieldBegin(name, type, id);
  EXPECT_EQ(id, expectedId);
  if (type == protocol::T_STRUCT) {
    reader.readStructBegin(name);
    reader.readFieldBegin(name, type, id);
  }
  EXPECT_EQ(type, protocol::T_STRING);
  std::string value;
  reader.readString(value);
  return value;
}

class CapturingCallback : public RequestClientCallback {
 public:
  explicit CapturingCallback(folly::Promise<ClientReceiveState> promise)
      : promise_(std::move(promise)) {}

  void onResponse(ClientReceiveState&& state) noexcept override {
    promise_.setValue(std::move(state));
    delete this;
  }
  void onResponseError(folly::exception_wrapper ew) noexcept override {
    promise_.setException(std::move(ew));
    delete this;
  }
  bool isInlineSafe() const override { return true; }

 private:
  folly::Promise<ClientReceiveState> promise_;
};

// Sends a unary request straight to the channel, the way a proxy does,
// bypassing generated client code (which decompresses on its own).
folly::SemiFuture<ClientReceiveState> sendRawRequest(
    RequestChannel& channel,
    std::string_view method,
    const std::string& serializedArgs,
    bool acceptsCompressed,
    std::shared_ptr<transport::THeader> header =
        std::make_shared<transport::THeader>()) {
  auto [promise, future] = folly::makePromiseContract<ClientReceiveState>();
  header->setProtocolId(protocol::T_COMPACT_PROTOCOL);
  header->setAcceptsCompressedResponse(acceptsCompressed);
  channel.sendRequestResponse(
      RpcOptions(),
      MethodMetadata(method),
      SerializedRequest(folly::IOBuf::copyBuffer(serializedArgs)),
      std::move(header),
      RequestClientCallback::Ptr(new CapturingCallback(std::move(promise))),
      nullptr);
  return std::move(future);
}

struct EchoHandler : public ServiceHandler<TestRequestResponseService> {
  folly::coro::Task<std::unique_ptr<std::string>> co_echo(
      std::unique_ptr<std::string> message) override {
    co_return std::move(message);
  }
  folly::coro::Task<void> co_voidMethod() override { co_return; }
  [[noreturn]] folly::coro::Task<void> co_canThrow() override {
    throw RRDeclaredException{kMessage};
  }
};

// Replies to echo() with a body it compressed itself, like a proxy forwarding
// an upstream reply. Falls back to a normal reply when the request refuses
// the pre-compressed body.
struct PreCompressedEchoHandler
    : public ServiceHandler<TestRequestResponseService> {
  CompressionAlgorithm algorithm{CompressionAlgorithm::ZSTD};
  PayloadMetadata payloadMetadata{responsePayloadMetadata()};
  std::atomic<std::optional<bool>> accepted;
  folly::Synchronized<std::string> sentBody;

  void async_tm_echo(
      HandlerCallbackPtr<std::unique_ptr<std::string>> callback,
      std::unique_ptr<std::string> message) override {
    const bool ok = callback->getRequest()->setPreCompressedResponse(
        algorithm, payloadMetadata);
    accepted = ok;
    if (!ok) {
      callback->result(std::move(*message));
      return;
    }
    auto body = compress(serializeStringStruct(0, *message), algorithm);
    *sentBody.wlock() = body;
    auto* evb = callback->getEventBase();
    evb->runInEventBaseThread([callback = std::move(callback),
                               buf = folly::IOBuf::copyBuffer(body)]() mutable {
      callback->getRequest()->sendReply(
          ResponsePayload::create(std::move(buf)), nullptr, folly::none);
    });
  }
};

enum class ReplacementException {
  UNDECLARED,
  DECLARED,
  UNFINISHED,
  FORWARDED,
  DIRECT_EXCEPTION
};

struct ReplacementExceptionHandler
    : public ServiceHandler<TestRequestResponseService> {
  ReplacementException kind{ReplacementException::UNDECLARED};
  bool arm{true};
  std::atomic<std::optional<bool>> accepted;

  void async_tm_canThrow(HandlerCallbackPtr<void> callback) override {
    auto metadata = responsePayloadMetadata();
    if (kind == ReplacementException::FORWARDED) {
      metadata.exceptionMetadata()
          .ensure()
          .metadata()
          .ensure()
          .declaredException()
          .ensure();
    }
    if (arm) {
      accepted = callback->getRequest()->setPreCompressedResponse(
          CompressionAlgorithm::ZSTD, std::move(metadata));
      EXPECT_EQ(accepted.load(), true);
    }
    switch (kind) {
      case ReplacementException::UNDECLARED:
        callback->exception(std::runtime_error(kMessage));
        return;
      case ReplacementException::DECLARED:
        callback->exception(RRDeclaredException{kMessage});
        return;
      case ReplacementException::UNFINISHED:
        return;
      case ReplacementException::DIRECT_EXCEPTION: {
        folly::IOBufQueue queue;
        CompactProtocolWriter writer;
        writer.setOutput(&queue);
        writer.writeMessageBegin(
            "canThrow",
            MessageType::T_EXCEPTION,
            callback->getRequestContext()->getProtoSeqId());
        TApplicationException(kMessage).write(&writer);
        writer.writeMessageEnd();
        callback->getEventBase()->runInEventBaseThread(
            [callback = std::move(callback), body = queue.move()]() mutable {
              callback->getRequest()->sendException(
                  ResponsePayload::create(std::move(body)), nullptr);
            });
        return;
      }
      case ReplacementException::FORWARDED: {
        folly::IOBufQueue queue;
        CompactProtocolWriter writer;
        writer.setOutput(&queue);
        writer.writeStructBegin("");
        writer.writeFieldBegin("", protocol::T_STRUCT, 1);
        writer.writeStructBegin("");
        writer.writeFieldBegin("", protocol::T_STRING, 1);
        writer.writeString(kMessage);
        writer.writeFieldEnd();
        writer.writeFieldStop();
        writer.writeStructEnd();
        writer.writeFieldEnd();
        writer.writeFieldStop();
        writer.writeStructEnd();
        auto body = rocket::CompressionManager().compressBuffer(
            queue.move(), CompressionAlgorithm::ZSTD);
        callback->getEventBase()->runInEventBaseThread(
            [callback = std::move(callback), body = std::move(body)]() mutable {
              callback->getRequest()->sendReply(
                  ResponsePayload::create(std::move(body)));
            });
        return;
      }
    }
  }
};

} // namespace

CO_TEST_F(
    CompressedResponsePassthroughE2ETest, ArmedUndeclaredCallbackException) {
  auto handler = std::make_shared<ReplacementExceptionHandler>();
  startServer(handler, std::nullopt);
  auto client = makeClient<TestRequestResponseService>();
  try {
    co_await client->co_canThrow();
    ADD_FAILURE() << "expected replacement application exception";
  } catch (const TApplicationException& ex) {
    EXPECT_NE(ex.getMessage().find(kMessage), std::string::npos);
  }
  EXPECT_EQ(handler->accepted.load(), true);
}

CO_TEST_F(
    CompressedResponsePassthroughE2ETest, ArmedDeclaredCallbackException) {
  auto handler = std::make_shared<ReplacementExceptionHandler>();
  handler->kind = ReplacementException::DECLARED;
  startServer(handler, std::nullopt);
  auto client = makeClient<TestRequestResponseService>();
  try {
    co_await client->co_canThrow();
    ADD_FAILURE() << "expected replacement declared exception";
  } catch (const RRDeclaredException& ex) {
    EXPECT_EQ(*ex.message(), kMessage);
  }
  EXPECT_EQ(handler->accepted.load(), true);
}

CO_TEST_F(
    CompressedResponsePassthroughE2ETest, ArmedDeclaredRawCallbackException) {
  auto handler = std::make_shared<ReplacementExceptionHandler>();
  handler->kind = ReplacementException::DECLARED;
  startServer(handler, std::nullopt);
  auto client = makeClient<TestRequestResponseService>();
  auto state = co_await sendRawRequest(
      *client->getChannel(), "canThrow", serializeEmptyStruct(), true);
  EXPECT_EQ(handler->accepted.load(), true);
  CO_ASSERT_FALSE(state.isException());
  CO_ASSERT_EQ(
      state.header()->getResponseCompressionAlgorithm(),
      CompressionAlgorithm::NONE);
  EXPECT_EQ(readStringField(toString(state.serializedResponse()), 1), kMessage);
}

CO_TEST_F(CompressedResponsePassthroughE2ETest, ArmedUnfinishedCallback) {
  auto handler = std::make_shared<ReplacementExceptionHandler>();
  handler->kind = ReplacementException::UNFINISHED;
  handler->arm = false;
  startServer(handler, std::nullopt);
  auto client = makeClient<TestRequestResponseService>();
  auto expectedType = TApplicationException::UNKNOWN;
  try {
    co_await client->co_canThrow();
    ADD_FAILURE() << "expected unarmed unfinished callback exception";
  } catch (const TApplicationException& ex) {
    expectedType = ex.getType();
    EXPECT_EQ(ex.getMessage(), "apache::thrift::HandlerCallback not completed");
  }
  handler->arm = true;
  try {
    co_await client->co_canThrow();
    ADD_FAILURE() << "expected unfinished callback exception";
  } catch (const TApplicationException& ex) {
    EXPECT_EQ(ex.getType(), expectedType);
    EXPECT_EQ(ex.getMessage(), "apache::thrift::HandlerCallback not completed");
  }
  EXPECT_EQ(handler->accepted.load(), true);
}

CO_TEST_F(CompressedResponsePassthroughE2ETest, ArmedDirectExceptionWriter) {
  auto handler = std::make_shared<ReplacementExceptionHandler>();
  handler->kind = ReplacementException::DIRECT_EXCEPTION;
  startServer(handler, std::nullopt);
  auto client = makeClient<TestRequestResponseService>();
  try {
    co_await client->co_canThrow();
    ADD_FAILURE() << "expected direct replacement exception";
  } catch (const TApplicationException& ex) {
    EXPECT_EQ(ex.getMessage(), kMessage);
  }
  EXPECT_EQ(handler->accepted.load(), true);
}

CO_TEST_F(
    CompressedResponsePassthroughE2ETest, UnarmedReplacementExceptionControl) {
  auto handler = std::make_shared<ReplacementExceptionHandler>();
  handler->arm = false;
  startServer(handler, std::nullopt);
  auto client = makeClient<TestRequestResponseService>();
  EXPECT_THROW(co_await client->co_canThrow(), TApplicationException);
  handler->kind = ReplacementException::DECLARED;
  EXPECT_THROW(co_await client->co_canThrow(), RRDeclaredException);
  handler->kind = ReplacementException::UNFINISHED;
  EXPECT_THROW(co_await client->co_canThrow(), TApplicationException);
}

CO_TEST_F(
    CompressedResponsePassthroughE2ETest,
    ExplicitPreCompressedDeclaredExceptionControl) {
  auto handler = std::make_shared<ReplacementExceptionHandler>();
  handler->kind = ReplacementException::FORWARDED;
  startServer(handler, std::nullopt);
  auto client = makeClient<TestRequestResponseService>();
  EXPECT_THROW(co_await client->co_canThrow(), RRDeclaredException);
  EXPECT_EQ(handler->accepted.load(), true);
  auto state = co_await sendRawRequest(
      *client->getChannel(), "canThrow", serializeEmptyStruct(), true);
  CO_ASSERT_FALSE(state.isException());
  CO_ASSERT_EQ(
      state.header()->getResponseCompressionAlgorithm(),
      CompressionAlgorithm::NONE);
  EXPECT_EQ(readStringField(toString(state.serializedResponse()), 1), kMessage);
}

CO_TEST_F(
    CompressedResponsePassthroughE2ETest, CpuPolicyKeepsRawRequestsCompressed) {
  THRIFT_FLAG_SET_MOCK(thrift_client_compress_request_on_cpu, true);
  startServer(std::make_shared<EchoHandler>(), zstdConfig());
  auto client = makeClient<TestRequestResponseService>();
  auto state = co_await sendRawRequest(
      *client->getChannel(),
      "echo",
      serializeStringStruct(1, kMessage),
      /*acceptsCompressed=*/false);
  CO_ASSERT_FALSE(state.isException());
  CO_ASSERT_EQ(
      state.header()->getResponseCompressionAlgorithm(),
      CompressionAlgorithm::ZSTD);
  EXPECT_EQ(
      readStringField(
          uncompress(
              toString(state.serializedResponse()), CompressionAlgorithm::ZSTD),
          0),
      kMessage);
}

CO_TEST_F(
    CompressedResponsePassthroughE2ETest,
    AcceptingCallbackReceivesCompressedBody) {
  startServer(std::make_shared<EchoHandler>(), zstdConfig());
  auto client = makeClient<TestRequestResponseService>();

  auto state = co_await sendRawRequest(
      *client->getChannel(),
      "echo",
      serializeStringStruct(1, kMessage),
      /*acceptsCompressed=*/true);

  EXPECT_FALSE(state.isException());
  EXPECT_EQ(
      state.header()->getResponseCompressionAlgorithm(),
      CompressionAlgorithm::ZSTD);
  auto body = toString(state.serializedResponse());
  EXPECT_LT(body.size(), kMessage.size());
  EXPECT_EQ(
      readStringField(uncompress(body, CompressionAlgorithm::ZSTD), 0),
      kMessage);
}

CO_TEST_F(
    CompressedResponsePassthroughE2ETest, DefaultCallbackReceivesPlaintext) {
  startServer(std::make_shared<EchoHandler>(), zstdConfig());
  auto client = makeClient<TestRequestResponseService>();

  auto state = co_await sendRawRequest(
      *client->getChannel(),
      "echo",
      serializeStringStruct(1, kMessage),
      /*acceptsCompressed=*/false);

  EXPECT_FALSE(state.isException());
  EXPECT_EQ(
      state.header()->getResponseCompressionAlgorithm(),
      CompressionAlgorithm::NONE);
  EXPECT_EQ(readStringField(toString(state.serializedResponse()), 0), kMessage);
}

CO_TEST_F(
    CompressedResponsePassthroughE2ETest,
    AcceptingCallbackReceivesDeclaredExceptionDecompressed) {
  startServer(std::make_shared<EchoHandler>(), zstdConfig());
  auto client = makeClient<TestRequestResponseService>();

  auto state = co_await sendRawRequest(
      *client->getChannel(),
      "canThrow",
      serializeEmptyStruct(),
      /*acceptsCompressed=*/true);

  EXPECT_FALSE(state.isException());
  EXPECT_EQ(
      state.header()->getResponseCompressionAlgorithm(),
      CompressionAlgorithm::NONE);
  EXPECT_EQ(readStringField(toString(state.serializedResponse()), 1), kMessage);
}

CO_TEST_F(
    CompressedResponsePassthroughE2ETest, GeneratedClientUnaffectedByFlagOff) {
  startServer(std::make_shared<EchoHandler>(), zstdConfig());
  auto client = makeClient<TestRequestResponseService>();

  EXPECT_EQ(co_await client->co_echo(kMessage), kMessage);
  EXPECT_THROW(co_await client->co_canThrow(), RRDeclaredException);
}

CO_TEST_F(
    CompressedResponsePassthroughE2ETest,
    ServerSendsPreCompressedBodyWithoutEnvelopeOrRecompression) {
  auto handler = std::make_shared<PreCompressedEchoHandler>();
  startServer(handler, zstdConfig());
  auto client = makeClient<TestRequestResponseService>();

  EXPECT_EQ(co_await client->co_echo(kMessage), kMessage);
  EXPECT_EQ(handler->accepted.load(), true);

  auto state = co_await sendRawRequest(
      *client->getChannel(),
      "echo",
      serializeStringStruct(1, kMessage),
      /*acceptsCompressed=*/true);

  EXPECT_EQ(
      state.header()->getResponseCompressionAlgorithm(),
      CompressionAlgorithm::ZSTD);
  EXPECT_EQ(toString(state.serializedResponse()), *handler->sentBody.rlock());
}

CO_TEST_F(
    CompressedResponsePassthroughE2ETest,
    ServerSendsAlgorithmClientDidNotRequest) {
  auto handler = std::make_shared<PreCompressedEchoHandler>();
  CodecConfig lz4;
  lz4.lz4Config().ensure();
  startServer(handler, compressionConfigFor(std::move(lz4)));
  auto client = makeClient<TestRequestResponseService>();

  EXPECT_EQ(co_await client->co_echo(kMessage), kMessage);
  EXPECT_EQ(handler->accepted.load(), true);
}

CO_TEST_F(
    CompressedResponsePassthroughE2ETest,
    ServerSendsWhenClientRequestedNoCompression) {
  auto handler = std::make_shared<PreCompressedEchoHandler>();
  handler->algorithm = CompressionAlgorithm::LZ4;
  startServer(handler, std::nullopt);
  auto client = makeClient<TestRequestResponseService>();

  EXPECT_EQ(co_await client->co_echo(kMessage), kMessage);
  EXPECT_EQ(handler->accepted.load(), true);

  auto state = co_await sendRawRequest(
      *client->getChannel(),
      "echo",
      serializeStringStruct(1, kMessage),
      /*acceptsCompressed=*/true);

  EXPECT_EQ(
      state.header()->getResponseCompressionAlgorithm(),
      CompressionAlgorithm::LZ4);
  EXPECT_EQ(toString(state.serializedResponse()), *handler->sentBody.rlock());
}

CO_TEST_F(CompressedResponsePassthroughE2ETest, ServerRejectsCustomAndNone) {
  auto handler = std::make_shared<PreCompressedEchoHandler>();
  startServer(handler, zstdConfig());
  auto client = makeClient<TestRequestResponseService>();

  for (auto algorithm :
       {CompressionAlgorithm::CUSTOM, CompressionAlgorithm::NONE}) {
    handler->algorithm = algorithm;
    EXPECT_EQ(co_await client->co_echo(kMessage), kMessage);
    EXPECT_EQ(handler->accepted.load(), false);
  }
}

CO_TEST_F(
    CompressedResponsePassthroughE2ETest, ServerRejectsEmptyPayloadMetadata) {
  auto handler = std::make_shared<PreCompressedEchoHandler>();
  handler->payloadMetadata = PayloadMetadata();
  startServer(handler, zstdConfig());
  auto client = makeClient<TestRequestResponseService>();

  EXPECT_EQ(co_await client->co_echo(kMessage), kMessage);
  EXPECT_EQ(handler->accepted.load(), false);
}

CO_TEST_F(
    CompressedResponsePassthroughE2ETest,
    ServerRejectsWhenClientRequestedChecksum) {
  auto handler = std::make_shared<PreCompressedEchoHandler>();
  startServer(handler, zstdConfig());
  auto client = makeClient<TestRequestResponseService>();

  for (auto checksum :
       {RpcOptions::Checksum::CRC32, RpcOptions::Checksum::XXH3_64}) {
    handler->accepted = std::nullopt;
    RpcOptions rpcOptions;
    rpcOptions.setChecksum(checksum);
    EXPECT_EQ(co_await client->co_echo(rpcOptions, kMessage), kMessage);
    EXPECT_EQ(handler->accepted.load(), false);
  }
}

CO_TEST_F(
    CompressedResponsePassthroughE2ETest,
    ServerRejectsWhenClientRequestedLegacyCrc32c) {
  auto handler = std::make_shared<PreCompressedEchoHandler>();
  // A size limit above the request size keeps the request itself
  // uncompressed: a server does not decompress a request carrying crc32c.
  auto config = zstdConfig();
  config.compressionSizeLimit() = 1 << 20;
  startServer(handler, config);
  auto client = makeClient<TestRequestResponseService>();

  const auto args = serializeStringStruct(1, kMessage);
  auto header = std::make_shared<transport::THeader>();
  header->setCrc32c(checksum::crc32c(*folly::IOBuf::copyBuffer(args)));
  auto state = co_await sendRawRequest(
      *client->getChannel(),
      "echo",
      args,
      /*acceptsCompressed=*/true,
      std::move(header));

  EXPECT_FALSE(state.isException());
  EXPECT_EQ(handler->accepted.load(), false);
}

CO_TEST_F(
    CompressedResponsePassthroughE2ETest,
    ResponseTooBigErrorIsNotSentAsPreCompressed) {
  auto handler = std::make_shared<PreCompressedEchoHandler>();
  startServer(handler, zstdConfig(), [](ThriftServer& server) {
    server.setMaxResponseSize(8);
  });
  auto client = makeClient<TestRequestResponseService>();

  try {
    co_await client->co_echo(kMessage);
    ADD_FAILURE() << "expected TApplicationException";
  } catch (const TApplicationException& ex) {
    EXPECT_EQ(ex.getMessage(), "Response size too big");
  }
  EXPECT_EQ(handler->accepted.load(), true);
}

} // namespace apache::thrift
