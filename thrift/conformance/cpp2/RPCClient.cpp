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

#include <exception>
#include <glog/logging.h>
#include <folly/init/Init.h>

#include <folly/MapUtil.h>
#include <folly/base64.h>
#include <folly/coro/AsyncGenerator.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Collect.h>
#include <folly/coro/Sleep.h>
#include <folly/coro/Timeout.h>
#include <folly/executors/GlobalExecutor.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/AsyncTransport.h>

#include <folly/io/async/AsyncSSLSocket.h>
#include <folly/io/async/SSLContext.h>
#include <thrift/conformance/if/gen-cpp2/RPCConformanceService.h>
#include <thrift/conformance/if/gen-cpp2/RPCConformanceSetupService.h>
#include <thrift/conformance/if/gen-cpp2/rpc_types.h>
#include <thrift/lib/cpp2/async/HTTPClientChannel.h>
#include <thrift/lib/cpp2/async/HeaderClientChannel.h>
#include <thrift/lib/cpp2/async/PooledRequestChannel.h>
#include <thrift/lib/cpp2/async/RocketClientChannel.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>
#include <thrift/lib/cpp2/transport/rocket/payload/PayloadSerializer.h>

DEFINE_int32(port, 7777, "Port for Conformance Verification Server");
DEFINE_string(protocol, "compact", "compact or binary");
DEFINE_string(transport, "rocket", "rocket, header or http2");
DEFINE_bool(tls, false, "Connect over TLS, presenting --tls-cert");
DEFINE_string(tls_cert, "", "The client's certificate");
DEFINE_string(tls_key, "", "The client certificate's key");
DEFINE_string(tls_ca, "", "The CA the server's certificate must be signed by");
DEFINE_bool(checksum, false, "Verify the checksum a response carries");

using namespace apache::thrift;
using namespace apache::thrift::conformance;
using apache::thrift::transport::TTransportException;

apache::thrift::protocol::PROTOCOL_TYPES protocolId() {
  if (FLAGS_protocol == "binary") {
    return apache::thrift::protocol::T_BINARY_PROTOCOL;
  }
  CHECK_EQ(FLAGS_protocol, "compact");
  return apache::thrift::protocol::T_COMPACT_PROTOCOL;
}

folly::AsyncTransport::UniquePtr connect(folly::EventBase& eb) {
  const folly::SocketAddress address("::1", FLAGS_port);
  if (!FLAGS_tls) {
    return folly::AsyncTransport::UniquePtr(
        new folly::AsyncSocket(&eb, address));
  }
  CHECK_EQ(FLAGS_transport, "rocket");
  auto context = std::make_shared<folly::SSLContext>();
  context->loadCertKeyPairFromFiles(
      FLAGS_tls_cert.c_str(), FLAGS_tls_key.c_str());
  context->loadTrustedCertificates(FLAGS_tls_ca.c_str());
  context->setVerificationOption(folly::SSLContext::SSLVerifyPeerEnum::VERIFY);
  context->setAdvertisedNextProtocols({"rs"});
  auto socket = folly::AsyncSSLSocket::newSocket(context, &eb);
  socket->connect(nullptr, address);
  return folly::AsyncTransport::UniquePtr(socket.release());
}

ClientChannel::Ptr newChannel(folly::EventBase& eb) {
  auto socket = connect(eb);
  if (FLAGS_transport == "header") {
    return HeaderClientChannel::newChannel(
        std::move(socket),
        HeaderClientChannel::Options().setProtocolId(protocolId()));
  }
  if (FLAGS_transport == "http2") {
    auto channel = HTTPClientChannel::newHTTP2Channel(std::move(socket));
    channel->setProtocolId(protocolId());
    channel->setHTTPHost("localhost");
    channel->setHTTPUrl("/");
    channel->setTimeout(60'000);
    return channel;
  }
  CHECK_EQ(FLAGS_transport, "rocket");
  auto channel = RocketClientChannel::newChannel(std::move(socket));
  channel->setProtocolId(protocolId());
  return channel;
}

RequestChannel::Ptr newPooledChannel(
    folly::Function<ClientChannel::Ptr(folly::EventBase&)> factory =
        newChannel) {
  return PooledRequestChannel::newChannel(std::move(factory), 1, protocolId());
}

std::unique_ptr<Client<RPCConformanceService>> createClient() {
  return std::make_unique<Client<RPCConformanceService>>(newPooledChannel());
}

std::unique_ptr<Client<RPCConformanceSetupService>> createSetupClient() {
  return std::make_unique<Client<RPCConformanceSetupService>>(
      newPooledChannel());
}

template <class InteractionClientInstruction>
Client<RPCConformanceService>::BasicInteraction createInteraction(
    const InteractionClientInstruction& instruction) {
  auto client = createClient();
  if (instruction.initialSum().has_value()) {
    return folly::coro::blockingWait(
        client->co_basicInteractionFactoryFunction(*instruction.initialSum()));
  } else {
    return client->createBasicInteraction();
  }
}

// =================== Request-Response ===================
RequestResponseBasicClientTestResult runRequestResponseBasicTest(
    const RequestResponseBasicClientInstruction& instruction) {
  RequestResponseBasicClientTestResult result;
  auto client = createClient();
  auto rpcOptions = RpcOptions().setChecksum(
      static_cast<RpcOptions::Checksum>(
          instruction.checksum().value_or(ChecksumKind::NONE)));
  client->sync_requestResponseBasic(
      rpcOptions, result.response().emplace(), *instruction.request());
  return result;
}

RequestResponseDeclaredExceptionClientTestResult
requestResponseDeclaredExceptionTest(
    const RequestResponseDeclaredExceptionClientInstruction& instruction) {
  RequestResponseDeclaredExceptionClientTestResult result;
  auto client = createClient();
  try {
    client->sync_requestResponseDeclaredException(*instruction.request());
  } catch (const UserException& e) {
    result.userException() = e;
  }
  return result;
}

RequestResponseUndeclaredExceptionClientTestResult
requestResponseUndeclaredExceptionTest(
    const RequestResponseUndeclaredExceptionClientInstruction& instruction) {
  RequestResponseUndeclaredExceptionClientTestResult result;
  auto client = createClient();
  try {
    client->sync_requestResponseUndeclaredException(*instruction.request());
  } catch (const TApplicationException& e) {
    result.exceptionMessage() = e.getMessage();
  }
  return result;
}

RequestResponseAnyExceptionClientTestResult requestResponseAnyExceptionTest(
    const RequestResponseAnyExceptionClientInstruction& instruction) {
  RequestResponseAnyExceptionClientTestResult result;
  apache::thrift::RpcOptions rpcOptions;
  auto client = createClient();
  try {
    client->sync_requestResponseUndeclaredException(
        rpcOptions, *instruction.request());
  } catch (const TApplicationException& e) {
    result.exceptionMessage() = e.getMessage();
  }
  const auto& headers = rpcOptions.getReadHeaders();
  if (const auto* typeUri = folly::get_ptr(headers, "anyext")) {
    result.typeUri() = *typeUri;
  }
  if (const auto* data = folly::get_ptr(headers, "anyex")) {
    result.userException() = CompactSerializer::deserialize<UserException>(
        folly::base64Decode(*data));
  }
  return result;
}

RequestResponseServerErrorClientTestResult requestResponseServerErrorTest(
    const RequestResponseServerErrorClientInstruction& instruction) {
  RequestResponseServerErrorClientTestResult result;
  try {
    if (*instruction.unknownMethod()) {
      createSetupClient()->sync_createRPCConformanceServiceClient("", "");
    } else {
      Response response;
      createClient()->sync_requestResponseBasic(
          response, *instruction.request());
    }
  } catch (const TApplicationException& e) {
    result.exceptionType() = static_cast<int32_t>(e.getType());
    result.exceptionMessage() = e.getMessage();
  }
  return result;
}

RequestResponseMetadataClientTestResult requestResponseMetadataTest(
    const RequestResponseMetadataClientInstruction& instruction) {
  RequestResponseMetadataClientTestResult result;
  apache::thrift::RpcOptions rpcOptions;
  for (const auto& [key, value] : *instruction.writeHeaders()) {
    rpcOptions.setWriteHeader(key, value);
  }
  if (auto timeoutMs = instruction.timeoutMs()) {
    rpcOptions.setTimeout(std::chrono::milliseconds(*timeoutMs));
  }
  if (auto queueTimeoutMs = instruction.queueTimeoutMs()) {
    rpcOptions.setQueueTimeout(std::chrono::milliseconds(*queueTimeoutMs));
  }
  rpcOptions.setClientOnlyTimeouts(*instruction.clientOnlyTimeouts());
  if (auto priority = instruction.priority()) {
    rpcOptions.setPriority(
        static_cast<apache::thrift::concurrency::PRIORITY>(*priority));
  }
  auto client =
      std::make_unique<Client<RPCConformanceService>>(newPooledChannel(
          [headers = *instruction.persistentHeaders()](folly::EventBase& eb) {
            auto channel = newChannel(eb);
            for (const auto& [key, value] : headers) {
              channel->setPersistentHeader(key, value);
            }
            return channel;
          }));
  client->sync_requestResponseBasic(
      rpcOptions, result.response().emplace(), *instruction.request());
  return result;
}

RequestResponseConcurrentClientTestResult requestResponseConcurrentTest(
    const RequestResponseConcurrentClientInstruction& instruction) {
  auto client = createClient();
  return folly::coro::blockingWait(
      [&]() -> folly::coro::Task<RequestResponseConcurrentClientTestResult> {
        std::vector<folly::coro::Task<Response>> calls;
        for (const auto& request : *instruction.requests()) {
          calls.push_back(client->co_requestResponseBasic(request));
        }
        RequestResponseConcurrentClientTestResult result;
        for (auto& response :
             co_await folly::coro::collectAllRange(std::move(calls))) {
          result.responses()->push_back(std::move(response));
        }
        co_return result;
      }());
}

RequestResponseNoArgVoidResponseClientTestResult
requestResponseNoArgVoidResponseTest(
    const RequestResponseNoArgVoidResponseClientInstruction&) {
  RequestResponseNoArgVoidResponseClientTestResult result;
  auto client = createClient();
  client->sync_requestResponseNoArgVoidResponse();
  return result;
}

RequestResponseTimeoutClientTestResult runRequestResponseTimeoutTest(
    const RequestResponseTimeoutClientInstruction& instruction) {
  RequestResponseTimeoutClientTestResult result;
  Response response;
  apache::thrift::RpcOptions rpcOptions{};
  rpcOptions.setTimeout(std::chrono::milliseconds{*instruction.timeoutMs()});
  auto client = createClient();
  try {
    client->sync_requestResponseTimeout(
        rpcOptions, response, *instruction.request());
  } catch (const TTransportException& e) {
    if (e.getType() ==
        TTransportException::TTransportExceptionType::TIMED_OUT) {
      result.timeoutException() = true;
    }
  }
  return result;
}

RequestResponseBadContentLengthClientTestResult
requestResponseBadContentLengthTest(
    const RequestResponseBadContentLengthClientInstruction& instruction) {
  RequestResponseBadContentLengthClientTestResult result;
  auto client = createClient();
  try {
    Response response;
    client->sync_requestResponseBasic(response, *instruction.request());
  } catch (const TTransportException& e) {
    result.transportExceptionType() = static_cast<int32_t>(e.getType());
  }
  return result;
}

// =================== Stream ===================
StreamBasicClientTestResult streamBasicTest(
    StreamBasicClientInstruction& instruction) {
  auto client = createClient();
  apache::thrift::RpcOptions rpcOptions{};
  if (auto memoryBufferSize = instruction.memoryBufferSize()) {
    rpcOptions.setMemoryBufferSize(
        static_cast<size_t>(*memoryBufferSize->targetBytes()),
        *memoryBufferSize->initialChunks(),
        *memoryBufferSize->maxChunks());
  } else {
    rpcOptions.setChunkBufferSize(*instruction.bufferSize());
  }
  rpcOptions.setChecksum(
      static_cast<RpcOptions::Checksum>(
          instruction.checksum().value_or(ChecksumKind::NONE)));
  return folly::coro::blockingWait(
      [&]() -> folly::coro::Task<StreamBasicClientTestResult> {
        auto gen = (co_await client->co_streamBasic(
                        rpcOptions, *instruction.request()))
                       .toAsyncGenerator();
        StreamBasicClientTestResult result;
        while (auto val = co_await gen.next()) {
          result.streamPayloads()->push_back(std::move(*val));
        }
        co_return result;
      }());
}

template <typename T>
folly::coro::Task<void> dropUnread(folly::coro::Task<T> call, int64_t afterMs) {
  auto onCpu = folly::coro::co_withExecutor(
      folly::getGlobalCPUExecutor(), std::move(call));
  auto started = std::move(onCpu).start();
  co_await folly::coro::sleep(std::chrono::milliseconds(afterMs));
}

StreamCancelClientTestResult streamCancelTest(
    Client<RPCConformanceService>& client,
    const StreamCancelClientInstruction& instruction) {
  return folly::coro::blockingWait(
      [&]() -> folly::coro::Task<StreamCancelClientTestResult> {
        StreamCancelClientTestResult result;
        auto call = client.co_streamBasic(*instruction.request());
        if (auto giveUpAfterMs = instruction.giveUpAfterMs()) {
          auto giveUpAfter = std::chrono::milliseconds(*giveUpAfterMs);
          co_await folly::coro::co_awaitTry(
              folly::coro::timeout(std::move(call), giveUpAfter));
          co_return result;
        }
        if (auto dropUnreadAfterMs = instruction.dropUnreadAfterMs()) {
          co_await dropUnread(std::move(call), *dropUnreadAfterMs);
          co_return result;
        }
        auto gen = (co_await std::move(call)).toAsyncGenerator();
        while (static_cast<int64_t>(result.streamPayloads()->size()) <
               *instruction.consumeCount()) {
          auto val = co_await gen.next();
          if (!val) {
            break;
          }
          result.streamPayloads()->push_back(std::move(*val));
        }
        co_return result;
      }());
}

StreamServerErrorClientTestResult streamServerErrorTest(
    const StreamServerErrorClientInstruction& instruction) {
  StreamServerErrorClientTestResult result;
  auto client = createClient();
  try {
    folly::coro::blockingWait([&]() -> folly::coro::Task<void> {
      auto gen = (co_await client->co_streamBasic(*instruction.request()))
                     .toAsyncGenerator();
      while (co_await gen.next()) {
      }
    }());
  } catch (const TApplicationException& e) {
    result.exceptionType() = e.getType();
    result.exceptionMessage() = e.getMessage();
  }
  return result;
}

StreamChunkTimeoutClientTestResult streamChunkTimeoutTest(
    StreamChunkTimeoutClientInstruction& instruction) {
  auto client = createClient();
  StreamChunkTimeoutClientTestResult result;
  apache::thrift::RpcOptions rpcOptions{};
  rpcOptions.setChunkTimeout(
      std::chrono::milliseconds{*instruction.chunkTimeoutMs()});
  try {
    folly::coro::blockingWait([&]() -> folly::coro::Task<void> {
      auto gen = (co_await client->co_streamChunkTimeout(
                      rpcOptions, *instruction.request()))
                     .toAsyncGenerator();
      while (auto val = co_await gen.next()) {
        result.streamPayloads()->push_back(std::move(*val));
      }
    }());
  } catch (const TTransportException& e) {
    if (e.getType() ==
        TTransportException::TTransportExceptionType::TIMED_OUT) {
      result.chunkTimeoutException() = true;
    }
  }
  return result;
}

StreamInitialResponseClientTestResult streamInitialResponseTest(
    const StreamInitialResponseClientInstruction& instruction) {
  auto client = createClient();
  StreamInitialResponseClientTestResult testResult;
  folly::coro::blockingWait([&]() -> folly::coro::Task<void> {
    auto result =
        co_await client->co_streamInitialResponse(*instruction.request());
    testResult.initialResponse() = std::move(result.response);
    auto gen = std::move(result.stream).toAsyncGenerator();
    while (auto val = co_await gen.next()) {
      testResult.streamPayloads()->push_back(std::move(*val));
    }
  }());
  return testResult;
}

StreamCreditTimeoutClientTestResult streamCreditTimeoutTest(
    const StreamCreditTimeoutClientInstruction& instruction) {
  auto client = createClient();
  StreamCreditTimeoutClientTestResult result;
  folly::coro::blockingWait([&]() -> folly::coro::Task<void> {
    auto gen = (co_await client->co_streamCreditTimeout(
                    RpcOptions().setChunkBufferSize(0), *instruction.request()))
                   .toAsyncGenerator();
    try {
      co_await gen.next();
      // Sleep longer than the stream expiration time, so that the server
      // will run out of credit and throw a credit timeout.
      co_await folly::coro::sleep(
          std::chrono::milliseconds{*instruction.creditTimeoutMs()});
      co_await gen.next();
    } catch (const TApplicationException& e) {
      if (e.getType() ==
          TApplicationException::TApplicationExceptionType::TIMEOUT) {
        result.creditTimeoutException() = true;
      }
    }
  }());
  return result;
}

StreamDeclaredExceptionClientTestResult streamDeclaredExceptionTest(
    StreamDeclaredExceptionClientInstruction& instruction) {
  auto client = createClient();
  return folly::coro::blockingWait(
      [&]() -> folly::coro::Task<StreamDeclaredExceptionClientTestResult> {
        auto gen = (co_await client->co_streamDeclaredException(
                        *instruction.request()))
                       .toAsyncGenerator();
        StreamDeclaredExceptionClientTestResult result;
        try {
          co_await gen.next();
        } catch (const UserException& e) {
          result.userException() = e;
        }
        co_return result;
      }());
}

StreamUndeclaredExceptionClientTestResult streamUndeclaredExceptionTest(
    StreamUndeclaredExceptionClientInstruction& instruction) {
  auto client = createClient();
  return folly::coro::blockingWait(
      [&]() -> folly::coro::Task<StreamUndeclaredExceptionClientTestResult> {
        auto gen = (co_await client->co_streamUndeclaredException(
                        *instruction.request()))
                       .toAsyncGenerator();
        StreamUndeclaredExceptionClientTestResult result;
        try {
          co_await gen.next();
        } catch (const TApplicationException& e) {
          result.exceptionMessage() = e.getMessage();
        }
        co_return result;
      }());
}

StreamInitialDeclaredExceptionClientTestResult
streamInitialDeclaredExceptionTest(
    StreamInitialDeclaredExceptionClientInstruction& instruction) {
  auto client = createClient();
  return folly::coro::blockingWait(
      [&]()
          -> folly::coro::Task<StreamInitialDeclaredExceptionClientTestResult> {
        StreamInitialDeclaredExceptionClientTestResult result;
        try {
          co_await client->co_streamInitialDeclaredException(
              *instruction.request());
        } catch (const UserException& e) {
          result.userException() = e;
        }
        co_return result;
      }());
}

StreamInitialUndeclaredExceptionClientTestResult
streamInitialUndeclaredExceptionTest(
    StreamInitialUndeclaredExceptionClientInstruction& instruction) {
  auto client = createClient();
  return folly::coro::blockingWait(
      [&]() -> folly::coro::Task<
                StreamInitialUndeclaredExceptionClientTestResult> {
        StreamInitialUndeclaredExceptionClientTestResult result;
        try {
          co_await client->co_streamInitialUndeclaredException(
              *instruction.request());
        } catch (const TApplicationException& e) {
          result.exceptionMessage() = e.getMessage();
        }
        co_return result;
      }());
}

StreamInitialTimeoutClientTestResult streamInitialTimeoutTest(
    StreamInitialTimeoutClientInstruction& instruction) {
  auto client = createClient();
  return folly::coro::blockingWait(
      [&]() -> folly::coro::Task<StreamInitialTimeoutClientTestResult> {
        StreamInitialTimeoutClientTestResult result;
        try {
          co_await client->co_streamInitialTimeout(
              RpcOptions().setTimeout(
                  std::chrono::milliseconds(*instruction.timeoutMs())),
              *instruction.request());
        } catch (const TTransportException& e) {
          if (e.getType() ==
              TTransportException::TTransportExceptionType::TIMED_OUT) {
            result.timeoutException() = true;
          }
        }
        co_return result;
      }());
}

// =================== Sink ===================
SinkBasicClientTestResult sinkBasicTest(
    SinkBasicClientInstruction& instruction) {
  auto client = createClient();
  auto rpcOptions = RpcOptions().setChecksum(
      static_cast<RpcOptions::Checksum>(
          instruction.checksum().value_or(ChecksumKind::NONE)));
  return folly::coro::blockingWait(
      [&]() -> folly::coro::Task<SinkBasicClientTestResult> {
        auto sink =
            co_await client->co_sinkBasic(rpcOptions, *instruction.request());
        auto finalResponse =
            co_await sink.sink([&]() -> folly::coro::AsyncGenerator<Request&&> {
              for (auto& payload : *instruction.sinkPayloads()) {
                co_yield std::move(payload);
              }
            }());
        SinkBasicClientTestResult result;
        result.finalResponse() = std::move(finalResponse);
        co_return result;
      }());
}

SinkChunkTimeoutClientTestResult sinkChunkTimeoutTest(
    const SinkChunkTimeoutClientInstruction& instruction) {
  SinkChunkTimeoutClientTestResult result;
  auto client = createClient();
  try {
    folly::coro::blockingWait([&]() -> folly::coro::Task<void> {
      auto sink = co_await client->co_sinkChunkTimeout(*instruction.request());
      co_await sink.sink([&]() -> folly::coro::AsyncGenerator<Request&&> {
        for (auto payload : *instruction.sinkPayloads()) {
          co_yield std::move(payload);
        }
        co_await folly::coro::sleep(
            std::chrono::milliseconds{*instruction.chunkTimeoutMs()});
      }());
    }());
  } catch (const TApplicationException& e) {
    if (e.getType() == TApplicationException::TIMEOUT) {
      result.chunkTimeoutException() = true;
    }
  }
  return result;
}

SinkAbortClientTestResult sinkAbortTest(
    Client<RPCConformanceService>& client,
    const SinkAbortClientInstruction& instruction) {
  folly::coro::blockingWait([&]() -> folly::coro::Task<void> {
    auto call = client.co_sinkBasic(*instruction.request());
    if (auto giveUpAfterMs = instruction.giveUpAfterMs()) {
      auto giveUpAfter = std::chrono::milliseconds(*giveUpAfterMs);
      co_await folly::coro::co_awaitTry(
          folly::coro::timeout(std::move(call), giveUpAfter));
    } else if (auto dropUnreadAfterMs = instruction.dropUnreadAfterMs()) {
      co_await dropUnread(std::move(call), *dropUnreadAfterMs);
    } else {
      co_await std::move(call);
    }
  }());
  return {};
}

SinkServerErrorClientTestResult sinkServerErrorTest(
    const SinkServerErrorClientInstruction& instruction) {
  SinkServerErrorClientTestResult result;
  auto client = createClient();
  try {
    folly::coro::blockingWait([&]() -> folly::coro::Task<void> {
      auto sink = co_await client->co_sinkBasic(*instruction.request());
      co_await sink.sink(
          []() -> folly::coro::AsyncGenerator<Request&&> { co_return; }());
    }());
  } catch (const TApplicationException& e) {
    result.exceptionType() = e.getType();
    result.exceptionMessage() = e.getMessage();
  }
  return result;
}

SinkInitialResponseClientTestResult sinkInitialResponseTest(
    SinkInitialResponseClientInstruction& instruction) {
  auto client = createClient();
  return folly::coro::blockingWait(
      [&]() -> folly::coro::Task<SinkInitialResponseClientTestResult> {
        SinkInitialResponseClientTestResult result;
        auto sinkAndResponse =
            co_await client->co_sinkInitialResponse(*instruction.request());
        result.initialResponse() = sinkAndResponse.response;
        result.finalResponse() = co_await sinkAndResponse.sink.sink(
            [&]() -> folly::coro::AsyncGenerator<Request&&> {
              for (auto& payload : *instruction.sinkPayloads()) {
                co_yield std::move(payload);
              }
            }());
        co_return result;
      }());
}

SinkDeclaredExceptionClientTestResult sinkDeclaredExceptionTest(
    SinkDeclaredExceptionClientInstruction& instruction) {
  auto client = createClient();
  return folly::coro::blockingWait(
      [&]() -> folly::coro::Task<SinkDeclaredExceptionClientTestResult> {
        auto sink =
            co_await client->co_sinkDeclaredException(*instruction.request());
        auto finalResponse = co_await folly::coro::co_awaitTry(
            sink.sink([&]() -> folly::coro::AsyncGenerator<Request&&> {
              throw UserException(*instruction.userException());
              co_return;
            }()));
        SinkDeclaredExceptionClientTestResult result;
        result.sinkThrew() = finalResponse.hasException<SinkThrew>();
        co_return result;
      }());
}

SinkUndeclaredExceptionClientTestResult sinkUndeclaredExceptionTest(
    SinkUndeclaredExceptionClientInstruction& instruction) {
  auto client = createClient();
  return folly::coro::blockingWait(
      [&]() -> folly::coro::Task<SinkUndeclaredExceptionClientTestResult> {
        auto sink =
            co_await client->co_sinkUndeclaredException(*instruction.request());
        auto finalResponse = co_await folly::coro::co_awaitTry(
            sink.sink([&]() -> folly::coro::AsyncGenerator<Request&&> {
              throw std::runtime_error(*instruction.exceptionMessage());
              co_return;
            }()));
        SinkUndeclaredExceptionClientTestResult result;
        result.sinkThrew() = finalResponse.hasException<SinkThrew>();
        co_return result;
      }());
}

SinkInitialDeclaredExceptionClientTestResult sinkInitialDeclaredExceptionTest(
    SinkInitialDeclaredExceptionClientInstruction& instruction) {
  auto client = createClient();
  return folly::coro::blockingWait(
      [&]() -> folly::coro::Task<SinkInitialDeclaredExceptionClientTestResult> {
        SinkInitialDeclaredExceptionClientTestResult result;
        try {
          co_await client->co_sinkInitialDeclaredException(
              *instruction.request());
        } catch (...) {
          result.sinkThrew() = true;
        }
        co_return result;
      }());
}

SinkServerDeclaredExceptionClientTestResult sinkServerDeclaredExceptionTest(
    SinkServerDeclaredExceptionClientInstruction& instruction) {
  auto client = createClient();
  return folly::coro::blockingWait(
      [&]() -> folly::coro::Task<SinkServerDeclaredExceptionClientTestResult> {
        SinkServerDeclaredExceptionClientTestResult result;
        try {
          auto sink = co_await client->co_sinkServerDeclaredException(
              *instruction.request());
          co_await sink.sink(
              []() -> folly::coro::AsyncGenerator<Request&&> { co_return; }());
        } catch (...) {
          result.sinkThrew() = true;
        }
        co_return result;
      }());
}

// =================== Bidi ===================
BidiBasicClientTestResult bidiBasicTest(
    BidiBasicClientInstruction& instruction) {
  auto client = createClient();
  auto rpcOptions = RpcOptions().setChecksum(
      static_cast<RpcOptions::Checksum>(
          instruction.checksum().value_or(ChecksumKind::NONE)));
  return folly::coro::blockingWait(
      [&]() -> folly::coro::Task<BidiBasicClientTestResult> {
        auto bidi =
            co_await client->co_bidiBasic(rpcOptions, *instruction.request());
        BidiBasicClientTestResult result;
        co_await folly::coro::collectAll(
            folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
              co_await bidi.sink.sink(
                  [&]() -> folly::coro::AsyncGenerator<Request&&> {
                    for (auto payload : *instruction.sinkPayloads()) {
                      co_yield std::move(payload);
                    }
                  }());
            }),
            folly::coro::co_invoke(
                [&, gen = std::move(bidi.stream).toAsyncGenerator()]() mutable
                    -> folly::coro::Task<void> {
                  while (auto val = co_await gen.next()) {
                    result.streamPayloads()->push_back(std::move(*val));
                  }
                }));
        co_return result;
      }());
}

BidiInitialResponseClientTestResult bidiInitialResponseTest(
    BidiInitialResponseClientInstruction& instruction) {
  auto client = createClient();
  return folly::coro::blockingWait(
      [&]() -> folly::coro::Task<BidiInitialResponseClientTestResult> {
        auto bidi =
            co_await client->co_bidiInitialResponse(*instruction.request());
        BidiInitialResponseClientTestResult result;
        result.initialResponse() = std::move(bidi.response);
        co_await folly::coro::collectAll(
            folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
              co_await bidi.sink.sink(
                  [&]() -> folly::coro::AsyncGenerator<Request&&> {
                    for (auto payload : *instruction.sinkPayloads()) {
                      co_yield std::move(payload);
                    }
                  }());
            }),
            folly::coro::co_invoke(
                [&, gen = std::move(bidi.stream).toAsyncGenerator()]() mutable
                    -> folly::coro::Task<void> {
                  while (auto val = co_await gen.next()) {
                    result.streamPayloads()->push_back(std::move(*val));
                  }
                }));
        co_return result;
      }());
}

BidiStreamDeclaredExceptionClientTestResult bidiStreamDeclaredExceptionTest(
    BidiStreamDeclaredExceptionClientInstruction& instruction) {
  auto client = createClient();
  return folly::coro::blockingWait(
      [&]() -> folly::coro::Task<BidiStreamDeclaredExceptionClientTestResult> {
        auto bidi = co_await client->co_bidiStreamDeclaredException(
            *instruction.request());
        BidiStreamDeclaredExceptionClientTestResult result;
        co_await folly::coro::collectAll(
            folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
              co_await bidi.sink.sink(
                  [&]() -> folly::coro::AsyncGenerator<Request&&> {
                    for (auto payload : *instruction.sinkPayloads()) {
                      co_yield std::move(payload);
                    }
                  }());
            }),
            folly::coro::co_invoke(
                [&, gen = std::move(bidi.stream).toAsyncGenerator()]() mutable
                    -> folly::coro::Task<void> {
                  try {
                    while (auto val = co_await gen.next()) {
                      result.streamPayloads()->push_back(std::move(*val));
                    }
                  } catch (const UserException& e) {
                    result.userException() = e;
                  }
                }));
        co_return result;
      }());
}

BidiStreamUndeclaredExceptionClientTestResult bidiStreamUndeclaredExceptionTest(
    BidiStreamUndeclaredExceptionClientInstruction& instruction) {
  auto client = createClient();
  return folly::coro::blockingWait(
      [&]()
          -> folly::coro::Task<BidiStreamUndeclaredExceptionClientTestResult> {
        auto bidi = co_await client->co_bidiStreamUndeclaredException(
            *instruction.request());
        BidiStreamUndeclaredExceptionClientTestResult result;
        co_await folly::coro::collectAll(
            folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
              co_await bidi.sink.sink(
                  [&]() -> folly::coro::AsyncGenerator<Request&&> {
                    for (auto payload : *instruction.sinkPayloads()) {
                      co_yield std::move(payload);
                    }
                  }());
            }),
            folly::coro::co_invoke(
                [&, gen = std::move(bidi.stream).toAsyncGenerator()]() mutable
                    -> folly::coro::Task<void> {
                  try {
                    while (auto val = co_await gen.next()) {
                      result.streamPayloads()->push_back(std::move(*val));
                    }
                  } catch (const TApplicationException& e) {
                    // BiDi transport prefixes exception type to message
                    auto msg = e.getMessage();
                    auto pos = msg.find(": ");
                    result.exceptionMessage() =
                        (pos != std::string::npos) ? msg.substr(pos + 2) : msg;
                  }
                }));
        co_return result;
      }());
}

BidiSinkDeclaredExceptionClientTestResult bidiSinkDeclaredExceptionTest(
    BidiSinkDeclaredExceptionClientInstruction& instruction) {
  auto client = createClient();
  return folly::coro::blockingWait(
      [&]() -> folly::coro::Task<BidiSinkDeclaredExceptionClientTestResult> {
        auto bidi = co_await client->co_bidiSinkDeclaredException(
            *instruction.request());
        BidiSinkDeclaredExceptionClientTestResult result;
        co_await folly::coro::collectAll(
            folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
              auto sinkResult =
                  co_await folly::coro::co_awaitTry(bidi.sink.sink(
                      [&]() -> folly::coro::AsyncGenerator<Request&&> {
                        for (auto payload : *instruction.sinkPayloads()) {
                          co_yield std::move(payload);
                        }
                        throw *instruction.userException();
                        co_return;
                      }()));
              result.sinkThrew() = sinkResult.hasException<SinkThrew>();
            }),
            folly::coro::co_invoke(
                [&, gen = std::move(bidi.stream).toAsyncGenerator()]() mutable
                    -> folly::coro::Task<void> {
                  while (auto val = co_await gen.next()) {
                    result.streamPayloads()->push_back(std::move(*val));
                  }
                }));
        co_return result;
      }());
}

BidiSinkUndeclaredExceptionClientTestResult bidiSinkUndeclaredExceptionTest(
    BidiSinkUndeclaredExceptionClientInstruction& instruction) {
  auto client = createClient();
  return folly::coro::blockingWait(
      [&]() -> folly::coro::Task<BidiSinkUndeclaredExceptionClientTestResult> {
        auto bidi = co_await client->co_bidiSinkUndeclaredException(
            *instruction.request());
        BidiSinkUndeclaredExceptionClientTestResult result;
        co_await folly::coro::collectAll(
            folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
              auto sinkResult =
                  co_await folly::coro::co_awaitTry(bidi.sink.sink(
                      [&]() -> folly::coro::AsyncGenerator<Request&&> {
                        for (auto payload : *instruction.sinkPayloads()) {
                          co_yield std::move(payload);
                        }
                        throw std::runtime_error(
                            *instruction.exceptionMessage());
                        co_return;
                      }()));
              result.sinkThrew() = sinkResult.hasException<SinkThrew>();
            }),
            folly::coro::co_invoke(
                [&, gen = std::move(bidi.stream).toAsyncGenerator()]() mutable
                    -> folly::coro::Task<void> {
                  while (auto val = co_await gen.next()) {
                    result.streamPayloads()->push_back(std::move(*val));
                  }
                }));
        co_return result;
      }());
}

BidiMethodDeclaredExceptionClientTestResult bidiMethodDeclaredExceptionTest(
    BidiMethodDeclaredExceptionClientInstruction& instruction) {
  auto client = createClient();
  BidiMethodDeclaredExceptionClientTestResult result;
  folly::coro::blockingWait([&]() -> folly::coro::Task<void> {
    try {
      co_await client->co_bidiMethodDeclaredException(*instruction.request());
    } catch (const UserException& e) {
      result.userException() = e;
    }
  }());
  return result;
}

BidiMethodUndeclaredExceptionClientTestResult bidiMethodUndeclaredExceptionTest(
    BidiMethodUndeclaredExceptionClientInstruction& instruction) {
  auto client = createClient();
  BidiMethodUndeclaredExceptionClientTestResult result;
  folly::coro::blockingWait([&]() -> folly::coro::Task<void> {
    try {
      co_await client->co_bidiMethodUndeclaredException(*instruction.request());
    } catch (const TApplicationException& e) {
      // BiDi transport prefixes exception type to message
      auto msg = e.getMessage();
      auto pos = msg.find(": ");
      result.exceptionMessage() =
          (pos != std::string::npos) ? msg.substr(pos + 2) : msg;
    }
  }());
  return result;
}

BidiAbortClientTestResult bidiAbortTest(
    Client<RPCConformanceService>& client,
    const BidiAbortClientInstruction& instruction) {
  folly::coro::blockingWait([&]() -> folly::coro::Task<void> {
    auto call = client.co_bidiBasic(*instruction.request());
    if (auto giveUpAfterMs = instruction.giveUpAfterMs()) {
      auto giveUpAfter = std::chrono::milliseconds(*giveUpAfterMs);
      co_await folly::coro::co_awaitTry(
          folly::coro::timeout(std::move(call), giveUpAfter));
    } else if (auto dropUnreadAfterMs = instruction.dropUnreadAfterMs()) {
      co_await dropUnread(std::move(call), *dropUnreadAfterMs);
    } else {
      co_await std::move(call);
    }
  }());
  return {};
}

BidiServerErrorClientTestResult bidiServerErrorTest(
    const BidiServerErrorClientInstruction& instruction) {
  BidiServerErrorClientTestResult result;
  auto client = createClient();
  try {
    folly::coro::blockingWait([&]() -> folly::coro::Task<void> {
      auto bidi = co_await client->co_bidiBasic(*instruction.request());
      auto gen = std::move(bidi.stream).toAsyncGenerator();
      while (co_await gen.next()) {
      }
    }());
  } catch (const TApplicationException& e) {
    result.exceptionType() = e.getType();
    result.exceptionMessage() = e.getMessage();
  }
  return result;
}

ClosingFrameClientTestResult closingFrameTest(
    Client<RPCConformanceService>& client,
    const ClosingFrameClientInstruction& instruction) {
  folly::coro::blockingWait([&]() -> folly::coro::Task<void> {
    const auto& request = *instruction.request();
    switch (*instruction.call()) {
      case CallKind::REQUEST_RESPONSE:
        co_await folly::coro::co_awaitTry(
            client.co_requestResponseBasic(request));
        break;
      case CallKind::STREAM:
        co_await folly::coro::co_awaitTry(client.co_streamBasic(request));
        break;
      case CallKind::SINK:
        co_await folly::coro::co_awaitTry(client.co_sinkBasic(request));
        break;
      case CallKind::BIDI:
        co_await folly::coro::co_awaitTry(client.co_bidiBasic(request));
        break;
    }
  }());
  return {};
}

// =================== Interactions ===================
InteractionConstructorClientTestResult interactionConstructorTest(
    InteractionConstructorClientInstruction&) {
  auto client = createClient();
  return folly::coro::blockingWait(
      [&]() -> folly::coro::Task<InteractionConstructorClientTestResult> {
        auto interaction = client->createBasicInteraction();
        co_await interaction.co_init();
        co_return InteractionConstructorClientTestResult();
      }());
}

InteractionFactoryFunctionClientTestResult interactionFactoryFunctionTest(
    InteractionFactoryFunctionClientInstruction& instruction) {
  auto client = createClient();
  return folly::coro::blockingWait(
      [&]() -> folly::coro::Task<InteractionFactoryFunctionClientTestResult> {
        co_await client->co_basicInteractionFactoryFunction(
            *instruction.initialSum());
        co_return InteractionFactoryFunctionClientTestResult();
      }());
}

InteractionPersistsStateClientTestResult interactionPersistsStateTest(
    InteractionPersistsStateClientInstruction& instruction) {
  auto client = createClient();
  return folly::coro::blockingWait(
      [&]() -> folly::coro::Task<InteractionPersistsStateClientTestResult> {
        auto interaction = createInteraction(instruction);
        InteractionPersistsStateClientTestResult result;
        for (auto& arg : *instruction.valuesToAdd()) {
          result.responses()->emplace_back(co_await interaction.co_add(arg));
        }
        co_return result;
      }());
}

InteractionTerminationClientTestResult interactionTerminationTest(
    InteractionTerminationClientInstruction& instruction) {
  auto client = createClient();
  return folly::coro::blockingWait(
      [&]() -> folly::coro::Task<InteractionTerminationClientTestResult> {
        auto interaction = createInteraction(instruction);
        co_await interaction.co_init();
        co_return InteractionTerminationClientTestResult();
      }());
}

int main(int argc, char** argv) {
  const folly::Init init(&argc, &argv);
  if (FLAGS_checksum) {
    rocket::PayloadSerializer::initialize(
        rocket::ChecksumPayloadSerializerStrategy<
            rocket::DefaultPayloadSerializerStrategy>());
  }

  RpcTestCase testCase;
  auto client = createClient();
  client->sync_getTestCase(testCase);
  auto& clientInstruction = *testCase.clientInstruction();

  ClientTestResult result;
  switch (clientInstruction.getType()) {
    case ClientInstruction::Type::requestResponseBasic:
      result.requestResponseBasic() = runRequestResponseBasicTest(
          *clientInstruction.requestResponseBasic());
      break;
    case ClientInstruction::Type::requestResponseDeclaredException:
      result.requestResponseDeclaredException() =
          requestResponseDeclaredExceptionTest(
              *clientInstruction.requestResponseDeclaredException());
      break;
    case ClientInstruction::Type::requestResponseUndeclaredException:
      result.requestResponseUndeclaredException() =
          requestResponseUndeclaredExceptionTest(
              *clientInstruction.requestResponseUndeclaredException());
      break;
    case ClientInstruction::Type::requestResponseNoArgVoidResponse:
      result.requestResponseNoArgVoidResponse() =
          requestResponseNoArgVoidResponseTest(
              *clientInstruction.requestResponseNoArgVoidResponse());
      break;
    case ClientInstruction::Type::requestResponseTimeout:
      result.requestResponseTimeout() = runRequestResponseTimeoutTest(
          *clientInstruction.requestResponseTimeout());
      break;
    case ClientInstruction::Type::requestResponseAnyException:
      result.requestResponseAnyException() = requestResponseAnyExceptionTest(
          *clientInstruction.requestResponseAnyException());
      break;
    case ClientInstruction::Type::requestResponseServerError:
      result.requestResponseServerError() = requestResponseServerErrorTest(
          *clientInstruction.requestResponseServerError());
      break;
    case ClientInstruction::Type::requestResponseMetadata:
      result.requestResponseMetadata() = requestResponseMetadataTest(
          *clientInstruction.requestResponseMetadata());
      break;
    case ClientInstruction::Type::requestResponseConcurrent:
      result.requestResponseConcurrent() = requestResponseConcurrentTest(
          *clientInstruction.requestResponseConcurrent());
      break;
    case ClientInstruction::Type::requestResponseBadContentLength:
      result.requestResponseBadContentLength() =
          requestResponseBadContentLengthTest(
              *clientInstruction.requestResponseBadContentLength());
      break;
    case ClientInstruction::Type::streamBasic:
      result.streamBasic() = streamBasicTest(*clientInstruction.streamBasic());
      break;
    case ClientInstruction::Type::streamChunkTimeout:
      result.streamChunkTimeout() =
          streamChunkTimeoutTest(*clientInstruction.streamChunkTimeout());
      break;
    case ClientInstruction::Type::streamInitialResponse:
      result.streamInitialResponse() =
          streamInitialResponseTest(*clientInstruction.streamInitialResponse());
      break;
    case ClientInstruction::Type::streamCreditTimeout:
      result.streamCreditTimeout() =
          streamCreditTimeoutTest(*clientInstruction.streamCreditTimeout());
      break;
    case ClientInstruction::Type::streamDeclaredException:
      result.streamDeclaredException() = streamDeclaredExceptionTest(
          *clientInstruction.streamDeclaredException());
      break;
    case ClientInstruction::Type::streamUndeclaredException:
      result.streamUndeclaredException() = streamUndeclaredExceptionTest(
          *clientInstruction.streamUndeclaredException());
      break;
    case ClientInstruction::Type::streamInitialDeclaredException:
      result.streamInitialDeclaredException() =
          streamInitialDeclaredExceptionTest(
              *clientInstruction.streamInitialDeclaredException());
      break;
    case ClientInstruction::Type::streamInitialUndeclaredException:
      result.streamInitialUndeclaredException() =
          streamInitialUndeclaredExceptionTest(
              *clientInstruction.streamInitialUndeclaredException());
      break;
    case ClientInstruction::Type::streamInitialTimeout:
      result.streamInitialTimeout() =
          streamInitialTimeoutTest(*clientInstruction.streamInitialTimeout());
      break;
    case ClientInstruction::Type::streamCancel:
      result.streamCancel() =
          streamCancelTest(*client, *clientInstruction.streamCancel());
      break;
    case ClientInstruction::Type::streamServerError:
      result.streamServerError() =
          streamServerErrorTest(*clientInstruction.streamServerError());
      break;
    case ClientInstruction::Type::sinkBasic:
      result.sinkBasic() = sinkBasicTest(*clientInstruction.sinkBasic());
      break;
    case ClientInstruction::Type::sinkInitialResponse:
      result.sinkInitialResponse() =
          sinkInitialResponseTest(*clientInstruction.sinkInitialResponse());
      break;
    case ClientInstruction::Type::sinkChunkTimeout:
      result.sinkChunkTimeout() =
          sinkChunkTimeoutTest(*clientInstruction.sinkChunkTimeout());
      break;
    case ClientInstruction::Type::sinkAbort:
      result.sinkAbort() =
          sinkAbortTest(*client, *clientInstruction.sinkAbort());
      break;
    case ClientInstruction::Type::sinkServerError:
      result.sinkServerError() =
          sinkServerErrorTest(*clientInstruction.sinkServerError());
      break;
    case ClientInstruction::Type::sinkDeclaredException:
      result.sinkDeclaredException() =
          sinkDeclaredExceptionTest(*clientInstruction.sinkDeclaredException());
      break;
    case ClientInstruction::Type::sinkUndeclaredException:
      result.sinkUndeclaredException() = sinkUndeclaredExceptionTest(
          *clientInstruction.sinkUndeclaredException());
      break;
    case ClientInstruction::Type::sinkInitialDeclaredException:
      result.sinkInitialDeclaredException() = sinkInitialDeclaredExceptionTest(
          *clientInstruction.sinkInitialDeclaredException());
      break;
    case ClientInstruction::Type::sinkServerDeclaredException:
      result.sinkServerDeclaredException() = sinkServerDeclaredExceptionTest(
          *clientInstruction.sinkServerDeclaredException());
      break;
    case ClientInstruction::Type::interactionConstructor:
      result.interactionConstructor() = interactionConstructorTest(
          *clientInstruction.interactionConstructor());
      break;
    case ClientInstruction::Type::interactionFactoryFunction:
      result.interactionFactoryFunction() = interactionFactoryFunctionTest(
          *clientInstruction.interactionFactoryFunction());
      break;
    case ClientInstruction::Type::interactionPersistsState:
      result.interactionPersistsState() = interactionPersistsStateTest(
          *clientInstruction.interactionPersistsState());
      break;
    case ClientInstruction::Type::interactionTermination:
      result.interactionTermination() = interactionTerminationTest(
          *clientInstruction.interactionTermination());
      break;
    case ClientInstruction::Type::bidiBasic:
      result.bidiBasic() = bidiBasicTest(*clientInstruction.bidiBasic());
      break;
    case ClientInstruction::Type::bidiInitialResponse:
      result.bidiInitialResponse() =
          bidiInitialResponseTest(*clientInstruction.bidiInitialResponse());
      break;
    case ClientInstruction::Type::bidiStreamDeclaredException:
      result.bidiStreamDeclaredException() = bidiStreamDeclaredExceptionTest(
          *clientInstruction.bidiStreamDeclaredException());
      break;
    case ClientInstruction::Type::bidiStreamUndeclaredException:
      result.bidiStreamUndeclaredException() =
          bidiStreamUndeclaredExceptionTest(
              *clientInstruction.bidiStreamUndeclaredException());
      break;
    case ClientInstruction::Type::bidiSinkDeclaredException:
      result.bidiSinkDeclaredException() = bidiSinkDeclaredExceptionTest(
          *clientInstruction.bidiSinkDeclaredException());
      break;
    case ClientInstruction::Type::bidiSinkUndeclaredException:
      result.bidiSinkUndeclaredException() = bidiSinkUndeclaredExceptionTest(
          *clientInstruction.bidiSinkUndeclaredException());
      break;
    case ClientInstruction::Type::bidiMethodDeclaredException:
      result.bidiMethodDeclaredException() = bidiMethodDeclaredExceptionTest(
          *clientInstruction.bidiMethodDeclaredException());
      break;
    case ClientInstruction::Type::bidiMethodUndeclaredException:
      result.bidiMethodUndeclaredException() =
          bidiMethodUndeclaredExceptionTest(
              *clientInstruction.bidiMethodUndeclaredException());
      break;
    case ClientInstruction::Type::bidiAbort:
      result.bidiAbort() =
          bidiAbortTest(*client, *clientInstruction.bidiAbort());
      break;
    case ClientInstruction::Type::bidiServerError:
      result.bidiServerError() =
          bidiServerErrorTest(*clientInstruction.bidiServerError());
      break;
    case ClientInstruction::Type::closingFrame:
      result.closingFrame() =
          closingFrameTest(*client, *clientInstruction.closingFrame());
      break;
    default:
      throw std::runtime_error("Invalid TestCase Type.");
  }

  client->sync_sendTestResult(result);
  return 0;
}
