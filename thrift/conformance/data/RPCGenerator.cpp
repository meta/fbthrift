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

#include <thrift/conformance/data/RPCGenerator.h>

#include <thrift/lib/cpp/TApplicationException.h>
#include <thrift/lib/cpp/transport/TTransportException.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>
#include <thrift/lib/thrift/gen-cpp2/RpcMetadata_types.h>

namespace apache::thrift::conformance::data {

namespace {
const uint32_t kMaxFrameSize = (1 << 24) - 1;
const std::string kLargeData = std::string(kMaxFrameSize * 2, 'a');
const uint64_t kDefaultBufferSize = 100;
const int32_t kRocketApplicationError = 0x201;
const int32_t kRocketCanceled = 0x203;
const int32_t kRocketCancelFrame = 0x09;
const int32_t kRocketErrorFrame = 0x0B;
const std::string kUndecodableMetadata = "\x1f";
const std::string kProcessFirstResponseError =
    "apache::thrift::TApplicationException: process first response error";
const auto kUnknownCompression = static_cast<CompressionAlgorithm>(99);
const std::string kUnknownCompressionMessage =
    "Unknown CompressionAlgorithm enum value: 99";

// =================== Request-Response ===================
Test createRequestResponseBasicTest() {
  Test ret;
  ret.name() = "RequestResponseBasicTest";
  ret.tags()->emplace("spec/protocol/interface/#request-response");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "RequestResponseBasic/Success";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .requestResponseBasic()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";
  rpcTest.clientTestResult()
      .emplace()
      .requestResponseBasic()
      .emplace()
      .response()
      .emplace()
      .data() = "world";

  rpcTest.serverInstruction()
      .emplace()
      .requestResponseBasic()
      .emplace()
      .response()
      .emplace()
      .data() = "world";
  rpcTest.serverTestResult()
      .emplace()
      .requestResponseBasic()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";

  return ret;
}

Test createRequestResponseDeclaredExceptionTest() {
  Test ret;
  ret.name() = "RequestResponseDeclaredExceptionTest";
  ret.tags()->emplace(
      "spec/protocol/interface/#declared-response-and-declared-exception");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "RequestResponseDeclaredException/Success";

  UserException userException;
  userException.msg() = "world";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .requestResponseDeclaredException()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";
  rpcTest.clientTestResult()
      .emplace()
      .requestResponseDeclaredException()
      .emplace()
      .userException() = userException;

  rpcTest.serverInstruction()
      .emplace()
      .requestResponseDeclaredException()
      .emplace()
      .userException() = userException;
  rpcTest.serverTestResult()
      .emplace()
      .requestResponseDeclaredException()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";

  return ret;
}

Test createRequestResponseUndeclaredExceptionTest() {
  Test ret;
  ret.name() = "RequestResponseUndeclaredExceptionTest";
  ret.tags()->emplace("spec/protocol/interface/#undeclared-exception");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "RequestResponseUndeclaredException/Success";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .requestResponseUndeclaredException()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";
  rpcTest.clientTestResult()
      .emplace()
      .requestResponseUndeclaredException()
      .emplace()
      .exceptionMessage() = "my undeclared exception";

  rpcTest.serverInstruction()
      .emplace()
      .requestResponseUndeclaredException()
      .emplace()
      .exceptionMessage() = "my undeclared exception";
  rpcTest.serverTestResult()
      .emplace()
      .requestResponseUndeclaredException()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";

  return ret;
}

Test createRequestResponseAnyExceptionTest() {
  Test ret;
  ret.name() = "RequestResponseAnyExceptionTest";
  ret.tags()->emplace("spec/protocol/interface/#undeclared-exception");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "RequestResponseAnyException/Success";

  const std::string typeUri =
      "facebook.com/thrift/conformance/rpc/ForwardedUserException";
  const std::string exceptionMessage = "my forwarded exception";
  UserException userException;
  userException.msg() = "forwarded";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .requestResponseAnyException()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";
  auto& clientTestResult = rpcTest.clientTestResult()
                               .emplace()
                               .requestResponseAnyException()
                               .emplace();
  clientTestResult.exceptionMessage() = exceptionMessage;
  clientTestResult.typeUri() = typeUri;
  clientTestResult.userException() = userException;

  auto& serverInstruction = rpcTest.serverInstruction()
                                .emplace()
                                .requestResponseAnyException()
                                .emplace();
  serverInstruction.exceptionMessage() = exceptionMessage;
  serverInstruction.typeUri() = typeUri;
  serverInstruction.userException() = userException;
  rpcTest.serverTestResult()
      .emplace()
      .requestResponseAnyException()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";

  return ret;
}

// The server sheds the call before any handler runs, the way a preprocess
// function rejects work when the server is overloaded. What reaches the client
// is the error the server made up, not an exception a handler threw.
Test createRequestResponseLoadsheddingTest() {
  Test ret;
  ret.name() = "RequestResponseLoadsheddingTest";
  ret.tags()->emplace("spec/protocol/interface/#internal-server-error");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "RequestResponseLoadshedding/Success";

  const std::string message = "shedding the conformance call";
  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .requestResponseServerError()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";
  auto& clientTestResult = rpcTest.clientTestResult()
                               .emplace()
                               .requestResponseServerError()
                               .emplace();
  clientTestResult.exceptionType() = TApplicationException::LOADSHEDDING;
  clientTestResult.exceptionMessage() = message;
  rpcTest.serverInstruction()
      .emplace()
      .requestResponseServerError()
      .emplace()
      .error()
      .emplace()
      .loadshedding() = message;
  // The handler never runs, so the server records nothing.
  rpcTest.serverTestResult().emplace();

  return ret;
}

Test createRequestResponseUnexpectedErrorCodeTest() {
  Test ret;
  ret.name() = "RequestResponseUnexpectedErrorCodeTest";
  ret.tags()->emplace("spec/compatibility/rocket#internal-server-error");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "RequestResponseUnexpectedErrorCode/Success";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .requestResponseServerError()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";
  auto& clientTestResult = rpcTest.clientTestResult()
                               .emplace()
                               .requestResponseServerError()
                               .emplace();
  clientTestResult.exceptionType() = TApplicationException::UNKNOWN;
  clientTestResult.exceptionMessage() = "Unexpected error frame type: 513";
  auto& errorFrame = rpcTest.serverInstruction()
                         .emplace()
                         .requestResponseServerError()
                         .emplace()
                         .error()
                         .emplace()
                         .errorFrame()
                         .emplace();
  errorFrame.code() = kRocketApplicationError;
  errorFrame.message() = "rejected before the first response";
  rpcTest.serverTestResult().emplace();

  return ret;
}

Test createRequestResponseTaskExpiredTest() {
  Test ret;
  ret.name() = "RequestResponseTaskExpiredTest";
  ret.tags()->emplace("spec/protocol/interface/#internal-server-error");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "RequestResponseTaskExpired/Success";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .requestResponseServerError()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";
  auto& clientTestResult = rpcTest.clientTestResult()
                               .emplace()
                               .requestResponseServerError()
                               .emplace();
  clientTestResult.exceptionType() = TApplicationException::TIMEOUT;
  clientTestResult.exceptionMessage() = "Task expired";
  auto& taskExpired = rpcTest.serverInstruction()
                          .emplace()
                          .requestResponseServerError()
                          .emplace()
                          .error()
                          .emplace()
                          .taskExpired()
                          .emplace();
  taskExpired.taskExpireTimeMs() = 500;
  taskExpired.handlerTimeMs() = 1500;
  rpcTest.serverTestResult()
      .emplace()
      .requestResponseServerError()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";

  return ret;
}

Test createRequestResponseUndeclaredExceptionTypeTest() {
  Test ret;
  ret.name() = "RequestResponseUndeclaredExceptionTypeTest";
  ret.tags()->emplace("spec/protocol/interface/#undeclared-exception");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "RequestResponseUndeclaredExceptionType/Success";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .requestResponseServerError()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";
  auto& clientTestResult = rpcTest.clientTestResult()
                               .emplace()
                               .requestResponseServerError()
                               .emplace();
  clientTestResult.exceptionType() = TApplicationException::UNKNOWN;
  clientTestResult.exceptionMessage() =
      "std::runtime_error: my undeclared exception";
  rpcTest.serverInstruction()
      .emplace()
      .requestResponseServerError()
      .emplace()
      .error()
      .emplace()
      .undeclaredException() = "my undeclared exception";
  rpcTest.serverTestResult()
      .emplace()
      .requestResponseServerError()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";

  return ret;
}

Test createRequestResponseTooBigTest() {
  Test ret;
  ret.name() = "RequestResponseTooBigTest";
  ret.tags()->emplace("spec/protocol/interface/#internal-server-error");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "RequestResponseTooBig/Success";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .requestResponseServerError()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";
  auto& clientTestResult = rpcTest.clientTestResult()
                               .emplace()
                               .requestResponseServerError()
                               .emplace();
  clientTestResult.exceptionType() = TApplicationException::INTERNAL_ERROR;
  clientTestResult.exceptionMessage() = "Response size too big";
  auto& responseTooBig = rpcTest.serverInstruction()
                             .emplace()
                             .requestResponseServerError()
                             .emplace()
                             .error()
                             .emplace()
                             .responseTooBig()
                             .emplace();
  responseTooBig.maxResponseSize() = 4096;
  responseTooBig.responseSize() = 65536;
  rpcTest.serverTestResult()
      .emplace()
      .requestResponseServerError()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";

  return ret;
}

Test createRequestResponseUnknownMethodTest() {
  Test ret;
  ret.name() = "RequestResponseUnknownMethodTest";
  ret.tags()->emplace("spec/protocol/interface/#internal-server-error");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "RequestResponseUnknownMethod/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction = rpcTest.clientInstruction()
                                .emplace()
                                .requestResponseServerError()
                                .emplace();
  clientInstruction.request().emplace().data() = "hello";
  clientInstruction.unknownMethod() = true;
  auto& clientTestResult = rpcTest.clientTestResult()
                               .emplace()
                               .requestResponseServerError()
                               .emplace();
  clientTestResult.exceptionType() = TApplicationException::UNKNOWN_METHOD;
  clientTestResult.exceptionMessage() =
      "Method name createRPCConformanceServiceClient not found";
  rpcTest.serverInstruction().emplace().requestResponseServerError().emplace();
  rpcTest.serverTestResult().emplace();

  return ret;
}

Test createRequestResponseWriteHeadersTest() {
  Test ret;
  ret.name() = "RequestResponseWriteHeadersTest";
  ret.tags()->emplace("spec/protocol/interface/#request");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "RequestResponseWriteHeaders/Success";

  const std::map<std::string, std::string> headers{
      {"conformance-first", "one"}, {"conformance-second", "two"}};
  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction =
      rpcTest.clientInstruction().emplace().requestResponseMetadata().emplace();
  clientInstruction.request().emplace().data() = "hello";
  clientInstruction.writeHeaders() = headers;
  rpcTest.clientTestResult()
      .emplace()
      .requestResponseMetadata()
      .emplace()
      .response()
      .emplace()
      .data() = "world";
  rpcTest.serverInstruction()
      .emplace()
      .requestResponseMetadata()
      .emplace()
      .response()
      .emplace()
      .data() = "world";
  auto& serverTestResult =
      rpcTest.serverTestResult().emplace().requestResponseMetadata().emplace();
  serverTestResult.request().emplace().data() = "hello";
  serverTestResult.headers() = headers;

  return ret;
}

Test createRequestResponsePersistentHeadersTest() {
  Test ret;
  ret.name() = "RequestResponsePersistentHeadersTest";
  ret.tags()->emplace("spec/protocol/interface/#request");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "RequestResponsePersistentHeaders/Success";

  const std::map<std::string, std::string> headers{
      {"conformance-persistent", "on every request"}};
  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction =
      rpcTest.clientInstruction().emplace().requestResponseMetadata().emplace();
  clientInstruction.request().emplace().data() = "hello";
  clientInstruction.persistentHeaders() = headers;
  rpcTest.clientTestResult()
      .emplace()
      .requestResponseMetadata()
      .emplace()
      .response()
      .emplace()
      .data() = "world";
  rpcTest.serverInstruction()
      .emplace()
      .requestResponseMetadata()
      .emplace()
      .response()
      .emplace()
      .data() = "world";
  auto& serverTestResult =
      rpcTest.serverTestResult().emplace().requestResponseMetadata().emplace();
  serverTestResult.request().emplace().data() = "hello";
  serverTestResult.headers() = headers;

  return ret;
}

Test createRequestResponseTimeoutsSentTest() {
  Test ret;
  ret.name() = "RequestResponseTimeoutsSentTest";
  ret.tags()->emplace("spec/protocol/interface/#client-timeout");
  ret.tags()->emplace("spec/protocol/interface/#queue-timeout");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "RequestResponseTimeoutsSent/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction =
      rpcTest.clientInstruction().emplace().requestResponseMetadata().emplace();
  clientInstruction.request().emplace().data() = "hello";
  clientInstruction.timeoutMs() = 7000;
  clientInstruction.queueTimeoutMs() = 5000;
  rpcTest.clientTestResult()
      .emplace()
      .requestResponseMetadata()
      .emplace()
      .response()
      .emplace()
      .data() = "world";
  rpcTest.serverInstruction()
      .emplace()
      .requestResponseMetadata()
      .emplace()
      .response()
      .emplace()
      .data() = "world";
  auto& serverTestResult =
      rpcTest.serverTestResult().emplace().requestResponseMetadata().emplace();
  serverTestResult.request().emplace().data() = "hello";
  serverTestResult.clientTimeoutMs() = 7000;
  serverTestResult.queueTimeoutMs() = 5000;

  return ret;
}

Test createRequestResponseClientOnlyTimeoutsTest() {
  Test ret;
  ret.name() = "RequestResponseClientOnlyTimeoutsTest";
  ret.tags()->emplace("spec/protocol/interface/#client-timeout");
  ret.tags()->emplace("spec/protocol/interface/#queue-timeout");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "RequestResponseClientOnlyTimeouts/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction =
      rpcTest.clientInstruction().emplace().requestResponseMetadata().emplace();
  clientInstruction.request().emplace().data() = "hello";
  clientInstruction.timeoutMs() = 7000;
  clientInstruction.queueTimeoutMs() = 5000;
  clientInstruction.clientOnlyTimeouts() = true;
  rpcTest.clientTestResult()
      .emplace()
      .requestResponseMetadata()
      .emplace()
      .response()
      .emplace()
      .data() = "world";
  rpcTest.serverInstruction()
      .emplace()
      .requestResponseMetadata()
      .emplace()
      .response()
      .emplace()
      .data() = "world";
  auto& serverTestResult =
      rpcTest.serverTestResult().emplace().requestResponseMetadata().emplace();
  serverTestResult.request().emplace().data() = "hello";
  serverTestResult.clientTimeoutMs() = 0;
  serverTestResult.queueTimeoutMs() = 0;

  return ret;
}

Test createRequestResponsePriorityTest() {
  Test ret;
  ret.name() = "RequestResponsePriorityTest";
  ret.tags()->emplace("spec/protocol/interface/#request");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "RequestResponsePriority/Success";

  // apache::thrift::concurrency::BEST_EFFORT, not the default NORMAL.
  const int32_t priority = 4;
  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction =
      rpcTest.clientInstruction().emplace().requestResponseMetadata().emplace();
  clientInstruction.request().emplace().data() = "hello";
  clientInstruction.priority() = priority;
  rpcTest.clientTestResult()
      .emplace()
      .requestResponseMetadata()
      .emplace()
      .response()
      .emplace()
      .data() = "world";
  rpcTest.serverInstruction()
      .emplace()
      .requestResponseMetadata()
      .emplace()
      .response()
      .emplace()
      .data() = "world";
  auto& serverTestResult =
      rpcTest.serverTestResult().emplace().requestResponseMetadata().emplace();
  serverTestResult.request().emplace().data() = "hello";
  serverTestResult.priority() = priority;

  return ret;
}

Test createRequestResponseConcurrentTest() {
  Test ret;
  ret.name() = "RequestResponseConcurrentTest";
  ret.tags()->emplace("spec/protocol/interface/#request-response");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "RequestResponseConcurrent/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction = rpcTest.clientInstruction()
                                .emplace()
                                .requestResponseConcurrent()
                                .emplace();
  auto& clientTestResult = rpcTest.clientTestResult()
                               .emplace()
                               .requestResponseConcurrent()
                               .emplace();
  auto& serverTestResult = rpcTest.serverTestResult()
                               .emplace()
                               .requestResponseConcurrent()
                               .emplace();
  for (int i = 0; i < 3; i++) {
    auto data = folly::to<std::string>(i);
    clientInstruction.requests()->emplace_back().data() = data;
    clientTestResult.responses()->emplace_back().data() = data;
    serverTestResult.requests()->emplace_back().data() = data;
  }
  serverTestResult.allInFlight() = true;
  rpcTest.serverInstruction()
      .emplace()
      .requestResponseConcurrent()
      .emplace()
      .requestCount() = 3;

  return ret;
}

Test createRequestResponseBadContentLengthTest() {
  Test ret;
  ret.name() = "RequestResponseBadContentLengthTest";
  ret.tags()->emplace("spec/protocol/interface/#request-response");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "RequestResponseBadContentLength/Success";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .requestResponseBadContentLength()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";
  rpcTest.clientTestResult()
      .emplace()
      .requestResponseBadContentLength()
      .emplace()
      .transportExceptionType() = transport::TTransportException::UNKNOWN;

  auto& serverInstruction = rpcTest.serverInstruction()
                                .emplace()
                                .requestResponseBadContentLength()
                                .emplace();
  serverInstruction.response().emplace().data() = "world";
  serverInstruction.contentLength() = 1;
  rpcTest.serverTestResult()
      .emplace()
      .requestResponseBadContentLength()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";

  return ret;
}

Test createRequestResponseNoArgVoidResponse() {
  Test ret;
  ret.name() = "RequestResponseNoArgVoidResponseTest";
  ret.tags()->emplace("spec/protocol/interface/#request-response");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "RequestResponseNoArgVoidResponse/Success";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .requestResponseNoArgVoidResponse()
      .emplace();
  rpcTest.clientTestResult()
      .emplace()
      .requestResponseNoArgVoidResponse()
      .emplace();

  rpcTest.serverInstruction()
      .emplace()
      .requestResponseNoArgVoidResponse()
      .emplace();
  rpcTest.serverTestResult()
      .emplace()
      .requestResponseNoArgVoidResponse()
      .emplace();

  return ret;
}

Test createRequestResponseTimeoutTest() {
  Test ret;
  ret.name() = "RequestResponseTimeoutTest";
  ret.tags()->emplace("spec/protocol/interface/#client-timeout");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "RequestResponseTimeout/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction =
      rpcTest.clientInstruction().emplace().requestResponseTimeout().emplace();
  clientInstruction.request().emplace().data() = "hello";
  clientInstruction.timeoutMs() = 100;

  rpcTest.clientTestResult()
      .emplace()
      .requestResponseTimeout()
      .emplace()
      .timeoutException() = true;

  rpcTest.serverInstruction()
      .emplace()
      .requestResponseTimeout()
      .emplace()
      .timeoutMs() = 150;
  rpcTest.serverTestResult()
      .emplace()
      .requestResponseTimeout()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";

  return ret;
}

Test createRequestResponseFragmentationTest() {
  Test ret;
  ret.name() = "RequestResponseFragmentationTest";
  ret.tags()->emplace("spec/protocol/interface/rocket#request-response");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "RequestResponseFragmentation/Success";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .requestResponseBasic()
      .emplace()
      .request()
      .emplace()
      .data() = kLargeData;
  rpcTest.clientTestResult()
      .emplace()
      .requestResponseBasic()
      .emplace()
      .response()
      .emplace()
      .data() = kLargeData;

  rpcTest.serverInstruction()
      .emplace()
      .requestResponseBasic()
      .emplace()
      .response()
      .emplace()
      .data() = kLargeData;
  rpcTest.serverTestResult()
      .emplace()
      .requestResponseBasic()
      .emplace()
      .request()
      .emplace()
      .data() = kLargeData;

  return ret;
}

// =================== Stream ===================
Test createStreamBasicTest() {
  Test ret;
  ret.name() = "StreamBasicTest";
  ret.tags()->emplace("spec/protocol/interface/#stream");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "StreamBasic/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction =
      rpcTest.clientInstruction().emplace().streamBasic().emplace();
  clientInstruction.request().emplace().data() = "hello";
  clientInstruction.bufferSize() = kDefaultBufferSize;

  auto& serverInstruction =
      rpcTest.serverInstruction().emplace().streamBasic().emplace();
  for (int i = 0; i < 100; i++) {
    auto& payload = serverInstruction.streamPayloads()->emplace_back();
    payload.data() = folly::to<std::string>(i);
  }

  rpcTest.clientTestResult()
      .emplace()
      .streamBasic()
      .emplace()
      .streamPayloads()
      .copy_from(serverInstruction.streamPayloads());

  rpcTest.serverTestResult()
      .emplace()
      .streamBasic()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";

  return ret;
}

Test createStreamCancelTest() {
  Test ret;
  ret.name() = "StreamCancelTest";
  ret.tags()->emplace("spec/protocol/interface/#stream-cancellation");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "StreamCancel/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction =
      rpcTest.clientInstruction().emplace().streamCancel().emplace();
  clientInstruction.request().emplace().data() = "hello";
  clientInstruction.consumeCount() = 5;
  auto& clientTestResult =
      rpcTest.clientTestResult().emplace().streamCancel().emplace();
  for (int i = 0; i < 5; i++) {
    clientTestResult.streamPayloads()->emplace_back().data() =
        folly::to<std::string>(i);
  }
  rpcTest.serverInstruction().emplace().streamCancel().emplace();
  auto& serverTestResult =
      rpcTest.serverTestResult().emplace().streamCancel().emplace();
  serverTestResult.request().emplace().data() = "hello";
  serverTestResult.cancelled() = true;

  return ret;
}

Test createStreamCancelBeforeInitialResponseTest() {
  Test ret;
  ret.name() = "StreamCancelBeforeInitialResponseTest";
  ret.tags()->emplace("spec/protocol/interface/#stream-cancellation");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "StreamCancelBeforeInitialResponse/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction =
      rpcTest.clientInstruction().emplace().streamCancel().emplace();
  clientInstruction.request().emplace().data() = "hello";
  clientInstruction.giveUpAfterMs() = 100;
  rpcTest.clientTestResult().emplace().streamCancel().emplace();
  rpcTest.serverInstruction()
      .emplace()
      .streamCancel()
      .emplace()
      .initialResponseDelayMs() = 500;
  auto& serverTestResult =
      rpcTest.serverTestResult().emplace().streamCancel().emplace();
  serverTestResult.request().emplace().data() = "hello";
  serverTestResult.cancelled() = true;

  return ret;
}

Test createStreamCancelUnreadInitialResponseTest() {
  Test ret;
  ret.name() = "StreamCancelUnreadInitialResponseTest";
  ret.tags()->emplace("spec/protocol/interface/#stream-cancellation");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "StreamCancelUnreadInitialResponse/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction =
      rpcTest.clientInstruction().emplace().streamCancel().emplace();
  clientInstruction.request().emplace().data() = "hello";
  clientInstruction.dropUnreadAfterMs() = 200;
  rpcTest.clientTestResult().emplace().streamCancel().emplace();
  rpcTest.serverInstruction().emplace().streamCancel().emplace();
  auto& serverTestResult =
      rpcTest.serverTestResult().emplace().streamCancel().emplace();
  serverTestResult.request().emplace().data() = "hello";
  serverTestResult.cancelled() = true;

  return ret;
}

Test createStreamLoadsheddingTest() {
  Test ret;
  ret.name() = "StreamLoadsheddingTest";
  ret.tags()->emplace("spec/protocol/interface/#loadshedding");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "StreamLoadshedding/Success";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .streamServerError()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";
  auto& clientTestResult =
      rpcTest.clientTestResult().emplace().streamServerError().emplace();
  clientTestResult.exceptionType() = TApplicationException::LOADSHEDDING;
  clientTestResult.exceptionMessage() = "shedding the conformance call";
  rpcTest.serverInstruction()
      .emplace()
      .streamServerError()
      .emplace()
      .error()
      .emplace()
      .loadshedding() = "shedding the conformance call";
  rpcTest.serverTestResult().emplace();

  return ret;
}

Test createStreamUnexpectedErrorCodeTest() {
  Test ret;
  ret.name() = "StreamUnexpectedErrorCodeTest";
  ret.tags()->emplace("spec/compatibility/rocket#internal-server-error");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "StreamUnexpectedErrorCode/Success";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .streamServerError()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";
  auto& clientTestResult =
      rpcTest.clientTestResult().emplace().streamServerError().emplace();
  clientTestResult.exceptionType() = TApplicationException::UNKNOWN;
  clientTestResult.exceptionMessage() = "Unexpected error frame type: 513";
  auto& errorFrame = rpcTest.serverInstruction()
                         .emplace()
                         .streamServerError()
                         .emplace()
                         .error()
                         .emplace()
                         .errorFrame()
                         .emplace();
  errorFrame.code() = kRocketApplicationError;
  errorFrame.message() = "rejected before the first response";
  rpcTest.serverTestResult().emplace();

  return ret;
}

Test createStreamMemoryBufferSizeTest() {
  Test ret;
  ret.name() = "StreamMemoryBufferSizeTest";
  ret.tags()->emplace("spec/protocol/interface/#stream-flow-control");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "StreamMemoryBufferSize/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction =
      rpcTest.clientInstruction().emplace().streamBasic().emplace();
  clientInstruction.request().emplace().data() = "hello";
  auto& memoryBufferSize = clientInstruction.memoryBufferSize().emplace();
  memoryBufferSize.targetBytes() = 16 * 1024;
  memoryBufferSize.initialChunks() = 2;
  memoryBufferSize.maxChunks() = 8;
  auto& serverInstruction =
      rpcTest.serverInstruction().emplace().streamBasic().emplace();
  for (int i = 0; i < 64; i++) {
    serverInstruction.streamPayloads()->emplace_back().data() =
        std::string(2048, static_cast<char>('a' + i % 26));
  }
  rpcTest.clientTestResult()
      .emplace()
      .streamBasic()
      .emplace()
      .streamPayloads()
      .copy_from(serverInstruction.streamPayloads());
  rpcTest.serverTestResult()
      .emplace()
      .streamBasic()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";

  return ret;
}

Test createStreamChunkTimeoutTest() {
  Test ret;
  ret.name() = "StreamChunkTimeoutTest";
  ret.tags()->emplace("spec/protocol/interface/#stream-chunk-timeout");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "StreamChunkTimeout/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction =
      rpcTest.clientInstruction().emplace().streamChunkTimeout().emplace();
  clientInstruction.request().emplace().data() = "hello";
  clientInstruction.chunkTimeoutMs() = 100;

  auto& serverInstruction =
      rpcTest.serverInstruction().emplace().streamChunkTimeout().emplace();
  for (int i = 0; i < 100; i++) {
    auto& payload = serverInstruction.streamPayloads()->emplace_back();
    payload.data() = folly::to<std::string>(i);
  }
  serverInstruction.chunkTimeoutMs() = 150;

  auto& clientTestResult =
      rpcTest.clientTestResult().emplace().streamChunkTimeout().emplace();
  clientTestResult.streamPayloads().copy_from(
      serverInstruction.streamPayloads());
  clientTestResult.chunkTimeoutException() = true;

  rpcTest.serverTestResult()
      .emplace()
      .streamChunkTimeout()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";

  return ret;
}

Test createStreamFragmentationTest() {
  Test ret;
  ret.name() = "StreamFragmentationTest";
  ret.tags()->emplace("spec/protocol/interface/rocket/#stream");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "StreamFragmentation/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction =
      rpcTest.clientInstruction().emplace().streamBasic().emplace();
  clientInstruction.request().emplace().data() = kLargeData;
  clientInstruction.bufferSize() = kDefaultBufferSize;

  auto& serverInstruction =
      rpcTest.serverInstruction().emplace().streamBasic().emplace();
  serverInstruction.streamPayloads()->emplace_back().data() = kLargeData;

  rpcTest.clientTestResult()
      .emplace()
      .streamBasic()
      .emplace()
      .streamPayloads()
      .copy_from(serverInstruction.streamPayloads());

  rpcTest.serverTestResult()
      .emplace()
      .streamBasic()
      .emplace()
      .request()
      .emplace()
      .data() = kLargeData;

  return ret;
}

Test createStreamInitialResponseTest() {
  Test ret;
  ret.name() = "StreamInitialResponseTest";
  ret.tags()->emplace("spec/protocol/interface/#stream-initial-response");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "StreamInitialResponse/Success";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .streamInitialResponse()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";

  auto& serverInstruction =
      rpcTest.serverInstruction().emplace().streamInitialResponse().emplace();
  for (int i = 0; i < 100; i++) {
    auto& payload = serverInstruction.streamPayloads()->emplace_back();
    payload.data() = folly::to<std::string>(i);
  }
  serverInstruction.initialResponse().emplace().data() = "world";

  auto& clientTestResult =
      rpcTest.clientTestResult().emplace().streamInitialResponse().emplace();
  clientTestResult.streamPayloads().copy_from(
      serverInstruction.streamPayloads());
  clientTestResult.initialResponse().emplace().data() = "world";

  rpcTest.serverTestResult()
      .emplace()
      .streamInitialResponse()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";

  return ret;
}

Test createStreamSubsequentCreditsTest() {
  // Same as StreamBasicTest but the chunk buffer size is smaller than the
  // number of stream payloads, so the client must keep sending subsequent
  // credits in order to receive all stream payloads.
  Test ret;
  ret.name() = "StreamSubsequentCreditsTest";
  ret.tags()->emplace("spec/protocol/interface/#stream-flow-control");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "StreamSubsequentCredits/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction =
      rpcTest.clientInstruction().emplace().streamBasic().emplace();
  clientInstruction.request().emplace().data() = "hello";
  clientInstruction.bufferSize() = 10;

  auto& serverInstruction =
      rpcTest.serverInstruction().emplace().streamBasic().emplace();
  for (int i = 0; i < 100; i++) {
    auto& payload = serverInstruction.streamPayloads()->emplace_back();
    payload.data() = folly::to<std::string>(i);
  }

  rpcTest.clientTestResult()
      .emplace()
      .streamBasic()
      .emplace()
      .streamPayloads()
      .copy_from(serverInstruction.streamPayloads());

  rpcTest.serverTestResult()
      .emplace()
      .streamBasic()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";

  return ret;
}

Test createStreamCreditTimeoutTest() {
  Test ret;
  ret.name() = "StreamCreditTimeoutTest";
  ret.tags()->emplace("spec/protocol/interface/#stream-expire-time");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "StreamCreditTimeout/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction =
      rpcTest.clientInstruction().emplace().streamCreditTimeout().emplace();
  clientInstruction.request().emplace().data() = "hello";
  clientInstruction.creditTimeoutMs() = 100;

  auto& serverInstruction =
      rpcTest.serverInstruction().emplace().streamCreditTimeout().emplace();
  for (int i = 0; i < 100; i++) {
    auto& payload = serverInstruction.streamPayloads()->emplace_back();
    payload.data() = folly::to<std::string>(i);
  }
  serverInstruction.streamExpireTime() = 50;

  auto& clientTestResult =
      rpcTest.clientTestResult().emplace().streamCreditTimeout().emplace();
  clientTestResult.creditTimeoutException() = true;

  rpcTest.serverTestResult()
      .emplace()
      .streamCreditTimeout()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";

  return ret;
}

Test createStreamDeclaredExceptionTest() {
  Test ret;
  ret.name() = "StreamDeclaredExceptionTest";
  ret.tags()->emplace("spec/protocol/interface/rocket#stream-exception");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "StreamDeclaredException/Success";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .streamDeclaredException()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";

  rpcTest.serverInstruction()
      .emplace()
      .streamDeclaredException()
      .emplace()
      .userException()
      .emplace()
      .msg() = "world";

  rpcTest.clientTestResult()
      .emplace()
      .streamDeclaredException()
      .emplace()
      .userException()
      .emplace()
      .msg() = "world";

  rpcTest.serverTestResult()
      .emplace()
      .streamDeclaredException()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";

  return ret;
}

Test createStreamUndeclaredExceptionTest() {
  Test ret;
  ret.name() = "StreamUndeclaredExceptionTest";
  ret.tags()->emplace("spec/protocol/interface/rocket#stream-exception");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "StreamUndeclaredException/Success";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .streamUndeclaredException()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";

  rpcTest.serverInstruction()
      .emplace()
      .streamUndeclaredException()
      .emplace()
      .exceptionMessage() = "world";

  rpcTest.clientTestResult()
      .emplace()
      .streamUndeclaredException()
      .emplace()
      .exceptionMessage() = "world";

  rpcTest.serverTestResult()
      .emplace()
      .streamUndeclaredException()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";

  return ret;
}

Test createStreamInitialDeclaredExceptionTest() {
  Test ret;
  ret.name() = "StreamInitialDeclaredExceptionTest";
  ret.tags()->emplace("spec/protocol/interface/#stream-initial-response");
  ret.tags()->emplace("spec/protocol/interface/rocket#stream-initial-response");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "StreamInitialDeclaredException/Success";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .streamInitialDeclaredException()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";

  rpcTest.serverInstruction()
      .emplace()
      .streamInitialDeclaredException()
      .emplace()
      .userException()
      .emplace()
      .msg() = "world";

  rpcTest.clientTestResult()
      .emplace()
      .streamInitialDeclaredException()
      .emplace()
      .userException()
      .emplace()
      .msg() = "world";

  rpcTest.serverTestResult()
      .emplace()
      .streamInitialDeclaredException()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";

  return ret;
}

Test createStreamInitialUndeclaredExceptionTest() {
  Test ret;
  ret.name() = "StreamInitialUndeclaredExceptionTest";
  ret.tags()->emplace("spec/protocol/interface/#stream-initial-response");
  ret.tags()->emplace("spec/protocol/interface/rocket#stream-initial-response");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "StreamInitialUndeclaredException/Success";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .streamInitialUndeclaredException()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";

  rpcTest.serverInstruction()
      .emplace()
      .streamInitialUndeclaredException()
      .emplace()
      .exceptionMessage() = "world";

  rpcTest.clientTestResult()
      .emplace()
      .streamInitialUndeclaredException()
      .emplace()
      .exceptionMessage() = "world";

  rpcTest.serverTestResult()
      .emplace()
      .streamInitialUndeclaredException()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";

  return ret;
}

Test createStreamInitialTimeoutTest() {
  Test ret;
  ret.name() = "StreamInitialTimeoutTest";
  ret.tags()->emplace("spec/protocol/interface/#client-timeout");
  ret.tags()->emplace("spec/protocol/interface/#client-detected-exceptions");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "StreamInitialTimeout/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction =
      rpcTest.clientInstruction().emplace().streamInitialTimeout().emplace();
  clientInstruction.request().emplace().data() = "hello";
  clientInstruction.timeoutMs() = 100;

  rpcTest.serverInstruction()
      .emplace()
      .streamInitialTimeout()
      .emplace()
      .timeoutMs() = 150;

  rpcTest.clientTestResult()
      .emplace()
      .streamInitialTimeout()
      .emplace()
      .timeoutException() = true;

  rpcTest.serverTestResult()
      .emplace()
      .streamInitialTimeout()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";

  return ret;
}

// =================== Sink ===================
Test createSinkBasicTest() {
  Test ret;
  ret.name() = "SinkBasicTest";
  ret.tags()->emplace("spec/protocol/interface/#sink");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "SinkBasic/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction =
      rpcTest.clientInstruction().emplace().sinkBasic().emplace();
  clientInstruction.request().emplace().data() = "hello";
  for (int i = 0; i < 100; i++) {
    auto& sinkPayload = clientInstruction.sinkPayloads()->emplace_back();
    sinkPayload.data() = folly::to<std::string>(i);
  }

  rpcTest.clientTestResult()
      .emplace()
      .sinkBasic()
      .emplace()
      .finalResponse()
      .emplace()
      .data() = "world";

  auto& serverInstruction =
      rpcTest.serverInstruction().emplace().sinkBasic().emplace();
  serverInstruction.finalResponse().emplace().data() = "world";
  serverInstruction.bufferSize() = kDefaultBufferSize;

  auto& serverResult =
      rpcTest.serverTestResult().emplace().sinkBasic().emplace();
  serverResult.request().emplace().data() = "hello";
  serverResult.sinkPayloads().copy_from(clientInstruction.sinkPayloads());

  return ret;
}

Test createSinkFragmentationTest() {
  Test ret;
  ret.name() = "SinkFragmentationTest";
  ret.tags()->emplace("spec/protocol/interface/rocket/#sink");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "SinkFragmentation/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction =
      rpcTest.clientInstruction().emplace().sinkBasic().emplace();
  clientInstruction.request().emplace().data() = kLargeData;
  clientInstruction.sinkPayloads()->emplace_back().data() = kLargeData;

  rpcTest.clientTestResult()
      .emplace()
      .sinkBasic()
      .emplace()
      .finalResponse()
      .emplace()
      .data() = kLargeData;

  auto& serverInstruction =
      rpcTest.serverInstruction().emplace().sinkBasic().emplace();
  serverInstruction.finalResponse().emplace().data() = kLargeData;
  serverInstruction.bufferSize() = kDefaultBufferSize;

  auto& serverResult =
      rpcTest.serverTestResult().emplace().sinkBasic().emplace();
  serverResult.request().emplace().data() = kLargeData;
  serverResult.sinkPayloads().copy_from(clientInstruction.sinkPayloads());

  return ret;
}

Test createSinkSubsequentCreditsTest() {
  // Same as SinkBasicTest but the server buffer size is smaller than the number
  // of sink payloads, so the server must keep sending subsequent credits in
  // order to receive all sink payloads.
  Test ret;
  ret.name() = "SinkSubsequentCreditsTest";
  ret.tags()->emplace("spec/protocol/interface/#sink-flow-control");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "SinkSubsequestCredits/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction =
      rpcTest.clientInstruction().emplace().sinkBasic().emplace();
  clientInstruction.request().emplace().data() = "hello";
  for (int i = 0; i < 100; i++) {
    auto& sinkPayload = clientInstruction.sinkPayloads()->emplace_back();
    sinkPayload.data() = folly::to<std::string>(i);
  }

  rpcTest.clientTestResult()
      .emplace()
      .sinkBasic()
      .emplace()
      .finalResponse()
      .emplace()
      .data() = "world";

  auto& serverInstruction =
      rpcTest.serverInstruction().emplace().sinkBasic().emplace();
  serverInstruction.finalResponse().emplace().data() = "world";
  serverInstruction.bufferSize() = 10;

  auto& serverResult =
      rpcTest.serverTestResult().emplace().sinkBasic().emplace();
  serverResult.request().emplace().data() = "hello";
  serverResult.sinkPayloads().copy_from(clientInstruction.sinkPayloads());

  return ret;
}

Test createSinkEarlyFinalResponseTest() {
  Test ret;
  ret.name() = "SinkEarlyFinalResponseTest";
  ret.tags()->emplace("spec/protocol/interface/#sink-final-response");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "SinkEarlyFinalResponse/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction =
      rpcTest.clientInstruction().emplace().sinkBasic().emplace();
  clientInstruction.request().emplace().data() = "hello";
  for (int i = 0; i < 10; i++) {
    clientInstruction.sinkPayloads()->emplace_back().data() =
        folly::to<std::string>(i);
  }
  rpcTest.clientTestResult()
      .emplace()
      .sinkBasic()
      .emplace()
      .finalResponse()
      .emplace()
      .data() = "world";

  auto& serverInstruction =
      rpcTest.serverInstruction().emplace().sinkBasic().emplace();
  serverInstruction.finalResponse().emplace().data() = "world";
  serverInstruction.bufferSize() = 2;
  serverInstruction.consumeCount() = 2;

  auto& serverResult =
      rpcTest.serverTestResult().emplace().sinkBasic().emplace();
  serverResult.request().emplace().data() = "hello";
  serverResult.sinkPayloads()->push_back(
      (*clientInstruction.sinkPayloads())[0]);
  serverResult.sinkPayloads()->push_back(
      (*clientInstruction.sinkPayloads())[1]);

  return ret;
}

Test createSinkAbortTest() {
  Test ret;
  ret.name() = "SinkAbortTest";
  ret.tags()->emplace("spec/protocol/interface/#sink");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "SinkAbort/Success";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .sinkAbort()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";
  rpcTest.clientTestResult().emplace().sinkAbort().emplace();
  rpcTest.serverInstruction().emplace().sinkAbort().emplace();
  auto& serverTestResult =
      rpcTest.serverTestResult().emplace().sinkAbort().emplace();
  serverTestResult.request().emplace().data() = "hello";
  serverTestResult.aborted() = true;

  return ret;
}

Test createSinkAbortBeforeInitialResponseTest() {
  Test ret;
  ret.name() = "SinkAbortBeforeInitialResponseTest";
  ret.tags()->emplace("spec/protocol/interface/#sink");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "SinkAbortBeforeInitialResponse/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction =
      rpcTest.clientInstruction().emplace().sinkAbort().emplace();
  clientInstruction.request().emplace().data() = "hello";
  clientInstruction.giveUpAfterMs() = 100;
  rpcTest.clientTestResult().emplace().sinkAbort().emplace();
  rpcTest.serverInstruction()
      .emplace()
      .sinkAbort()
      .emplace()
      .initialResponseDelayMs() = 500;
  auto& serverTestResult =
      rpcTest.serverTestResult().emplace().sinkAbort().emplace();
  serverTestResult.request().emplace().data() = "hello";
  serverTestResult.aborted() = true;

  return ret;
}

Test createSinkAbortUnreadInitialResponseTest() {
  Test ret;
  ret.name() = "SinkAbortUnreadInitialResponseTest";
  ret.tags()->emplace("spec/protocol/interface/#sink");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "SinkAbortUnreadInitialResponse/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction =
      rpcTest.clientInstruction().emplace().sinkAbort().emplace();
  clientInstruction.request().emplace().data() = "hello";
  clientInstruction.dropUnreadAfterMs() = 200;
  rpcTest.clientTestResult().emplace().sinkAbort().emplace();
  rpcTest.serverInstruction().emplace().sinkAbort().emplace();
  auto& serverTestResult =
      rpcTest.serverTestResult().emplace().sinkAbort().emplace();
  serverTestResult.request().emplace().data() = "hello";
  serverTestResult.aborted() = true;

  return ret;
}

Test createSinkLoadsheddingTest() {
  Test ret;
  ret.name() = "SinkLoadsheddingTest";
  ret.tags()->emplace("spec/protocol/interface/#loadshedding");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "SinkLoadshedding/Success";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .sinkServerError()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";
  auto& clientTestResult =
      rpcTest.clientTestResult().emplace().sinkServerError().emplace();
  clientTestResult.exceptionType() = TApplicationException::LOADSHEDDING;
  clientTestResult.exceptionMessage() = "shedding the conformance call";
  rpcTest.serverInstruction()
      .emplace()
      .sinkServerError()
      .emplace()
      .error()
      .emplace()
      .loadshedding() = "shedding the conformance call";
  rpcTest.serverTestResult().emplace();

  return ret;
}

Test createSinkUnexpectedErrorCodeTest() {
  Test ret;
  ret.name() = "SinkUnexpectedErrorCodeTest";
  ret.tags()->emplace("spec/compatibility/rocket#internal-server-error");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "SinkUnexpectedErrorCode/Success";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .sinkServerError()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";
  auto& clientTestResult =
      rpcTest.clientTestResult().emplace().sinkServerError().emplace();
  clientTestResult.exceptionType() = TApplicationException::UNKNOWN;
  clientTestResult.exceptionMessage() = "Unexpected error frame type: 513";
  auto& errorFrame = rpcTest.serverInstruction()
                         .emplace()
                         .sinkServerError()
                         .emplace()
                         .error()
                         .emplace()
                         .errorFrame()
                         .emplace();
  errorFrame.code() = kRocketApplicationError;
  errorFrame.message() = "rejected before the first response";
  rpcTest.serverTestResult().emplace();

  return ret;
}

Test createSinkChunkTimeoutTest() {
  Test ret;
  ret.name() = "SinkChunkTimeoutTest";
  ret.tags()->emplace("spec/protocol/interface/#sink-chunk-timeout");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "SinkChunkTimeout/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction =
      rpcTest.clientInstruction().emplace().sinkChunkTimeout().emplace();
  clientInstruction.request().emplace().data() = "hello";
  for (int i = 0; i < 100; i++) {
    auto& sinkPayload = clientInstruction.sinkPayloads()->emplace_back();
    sinkPayload.data() = folly::to<std::string>(i);
  }
  clientInstruction.chunkTimeoutMs() = 150;

  rpcTest.clientTestResult()
      .emplace()
      .sinkChunkTimeout()
      .emplace()
      .chunkTimeoutException() = true;

  auto& serverInstruction =
      rpcTest.serverInstruction().emplace().sinkChunkTimeout().emplace();
  serverInstruction.finalResponse().emplace().data() = "world";
  serverInstruction.chunkTimeoutMs() = 100;

  auto& serverResult =
      rpcTest.serverTestResult().emplace().sinkChunkTimeout().emplace();
  serverResult.request().emplace().data() = "hello";
  serverResult.sinkPayloads().copy_from(clientInstruction.sinkPayloads());
  serverResult.chunkTimeoutException() = true;

  return ret;
}

Test createSinkChunkDoesNotTimeoutTest() {
  Test ret;
  ret.name() = "SinkChunkDoesNotTimeoutTest";
  ret.tags()->emplace("spec/protocol/interface/#sink-chunk-timeout");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "SinkChunkDoesNotTimeout/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction =
      rpcTest.clientInstruction().emplace().sinkChunkTimeout().emplace();
  clientInstruction.request().emplace().data() = "hello";
  for (int i = 0; i < 100; i++) {
    auto& sinkPayload = clientInstruction.sinkPayloads()->emplace_back();
    sinkPayload.data() = folly::to<std::string>(i);
  }
  // Client will go quiet for 2000ms
  clientInstruction.chunkTimeoutMs() = 2000;

  rpcTest.clientTestResult()
      .emplace()
      .sinkChunkTimeout()
      .emplace()
      .chunkTimeoutException() = false;

  auto& serverInstruction =
      rpcTest.serverInstruction().emplace().sinkChunkTimeout().emplace();
  serverInstruction.finalResponse().emplace().data() = "world";
  // The server is longer than the default (1000) and longer than the client's
  // silent time, so we should not see any timeout errors
  serverInstruction.chunkTimeoutMs() = 3000;

  auto& serverResult =
      rpcTest.serverTestResult().emplace().sinkChunkTimeout().emplace();
  serverResult.request().emplace().data() = "hello";
  serverResult.sinkPayloads().copy_from(clientInstruction.sinkPayloads());
  serverResult.chunkTimeoutException() = false;

  return ret;
}

Test createSinkInitialResponseTest() {
  Test ret;
  ret.name() = "SinkInitialResponseTest";
  ret.tags()->emplace("spec/protocol/interface/#sink-initial-response");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "SinkInitialResponse/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction =
      rpcTest.clientInstruction().emplace().sinkInitialResponse().emplace();
  clientInstruction.request().emplace().data() = "hello";
  for (int i = 0; i < 100; i++) {
    auto& sinkPayload = clientInstruction.sinkPayloads()->emplace_back();
    sinkPayload.data() = folly::to<std::string>(i);
  }

  auto& testResult =
      rpcTest.clientTestResult().emplace().sinkInitialResponse().emplace();
  testResult.initialResponse().emplace().data() = "hello";
  testResult.finalResponse().emplace().data() = "world";

  auto& serverInstruction =
      rpcTest.serverInstruction().emplace().sinkInitialResponse().emplace();

  serverInstruction.initialResponse().emplace().data() = "hello";
  serverInstruction.finalResponse().emplace().data() = "world";
  serverInstruction.bufferSize() = kDefaultBufferSize;

  auto& serverResult =
      rpcTest.serverTestResult().emplace().sinkInitialResponse().emplace();
  serverResult.request().emplace().data() = "hello";
  serverResult.sinkPayloads().copy_from(clientInstruction.sinkPayloads());

  return ret;
}

Test createSinkDeclaredExceptionTest() {
  Test ret;
  ret.name() = "SinkDeclaredExceptionTest";
  ret.tags()->emplace("spec/protocol/interface/#sink-declared-exception");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "SinkDeclaredException/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction =
      rpcTest.clientInstruction().emplace().sinkDeclaredException().emplace();
  clientInstruction.request().emplace().data() = "request";
  clientInstruction.userException().emplace().msg() = "user_exception";

  rpcTest.clientTestResult()
      .emplace()
      .sinkDeclaredException()
      .emplace()
      .sinkThrew() = true;

  rpcTest.serverInstruction()
      .emplace()
      .sinkDeclaredException()
      .emplace()
      .bufferSize() = kDefaultBufferSize;

  auto& serverResult =
      rpcTest.serverTestResult().emplace().sinkDeclaredException().emplace();
  serverResult.request().emplace().data() = "request";
  serverResult.userException().copy_from(clientInstruction.userException());

  return ret;
}

Test createSinkUndeclaredExceptionTest() {
  Test ret;
  ret.name() = "SinkUndeclaredExceptionTest";
  ret.tags()->emplace("spec/protocol/interface/#sink-undeclared-exception");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "SinkUndeclaredException/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction =
      rpcTest.clientInstruction().emplace().sinkUndeclaredException().emplace();
  clientInstruction.request().emplace().data() = "request";
  clientInstruction.exceptionMessage() = "undeclared_exception";

  rpcTest.clientTestResult()
      .emplace()
      .sinkUndeclaredException()
      .emplace()
      .sinkThrew() = true;

  rpcTest.serverInstruction()
      .emplace()
      .sinkUndeclaredException()
      .emplace()
      .bufferSize() = kDefaultBufferSize;

  auto& serverResult =
      rpcTest.serverTestResult().emplace().sinkUndeclaredException().emplace();
  serverResult.request().copy_from(clientInstruction.request());
  serverResult.exceptionMessage().copy_from(
      clientInstruction.exceptionMessage());

  return ret;
}

Test createSinkInitialDeclaredExceptionTest() {
  Test ret;
  ret.name() = "SinkInitialDeclaredExceptionTest";
  ret.tags()->emplace(
      "spec/protocol/interface/#sink-initial-declared-exception");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "SinkInitialDeclaredException/Success";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .sinkInitialDeclaredException()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";

  rpcTest.clientTestResult()
      .emplace()
      .sinkInitialDeclaredException()
      .emplace()
      .sinkThrew() = true;

  rpcTest.serverInstruction()
      .emplace()
      .sinkInitialDeclaredException()
      .emplace()
      .bufferSize() = kDefaultBufferSize;
  rpcTest.serverInstruction()
      ->sinkInitialDeclaredException()
      ->userException()
      .emplace()
      .msg() = "initial declared exception";

  rpcTest.serverTestResult()
      .emplace()
      .sinkInitialDeclaredException()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";

  return ret;
}

Test createSinkServerDeclaredExceptionTest() {
  Test ret;
  ret.name() = "SinkServerDeclaredExceptionTest";
  ret.tags()->emplace(
      "spec/protocol/interface/#sink-server-declared-exception");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "SinkServerDeclaredException/Success";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .sinkServerDeclaredException()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";

  rpcTest.clientTestResult()
      .emplace()
      .sinkServerDeclaredException()
      .emplace()
      .sinkThrew() = true;

  rpcTest.serverInstruction()
      .emplace()
      .sinkServerDeclaredException()
      .emplace()
      .bufferSize() = kDefaultBufferSize;
  rpcTest.serverInstruction()
      ->sinkServerDeclaredException()
      ->userException()
      .emplace()
      .msg() = "server declared exception";

  rpcTest.serverTestResult()
      .emplace()
      .sinkServerDeclaredException()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";

  return ret;
}

// =================== BiDi Streaming ===================
Test createBidiBasicTest() {
  Test ret;
  ret.name() = "BidiBasicTest";
  ret.tags()->emplace("spec/protocol/interface/#bidi");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "BidiBasic/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction =
      rpcTest.clientInstruction().emplace().bidiBasic().emplace();
  clientInstruction.request().emplace().data() = "hello";
  for (int i = 0; i < 100; i++) {
    auto& sinkPayload = clientInstruction.sinkPayloads()->emplace_back();
    sinkPayload.data() = folly::to<std::string>(i);
  }

  auto& serverInstruction =
      rpcTest.serverInstruction().emplace().bidiBasic().emplace();
  for (int i = 0; i < 100; i++) {
    auto& payload = serverInstruction.streamPayloads()->emplace_back();
    payload.data() = folly::to<std::string>(i);
  }

  rpcTest.clientTestResult()
      .emplace()
      .bidiBasic()
      .emplace()
      .streamPayloads()
      .copy_from(serverInstruction.streamPayloads());

  auto& serverResult =
      rpcTest.serverTestResult().emplace().bidiBasic().emplace();
  serverResult.request().emplace().data() = "hello";
  serverResult.sinkPayloads().copy_from(clientInstruction.sinkPayloads());

  return ret;
}

Test createBidiInitialResponseTest() {
  Test ret;
  ret.name() = "BidiInitialResponseTest";
  ret.tags()->emplace("spec/protocol/interface/#bidi-initial-response");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "BidiInitialResponse/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction =
      rpcTest.clientInstruction().emplace().bidiInitialResponse().emplace();
  clientInstruction.request().emplace().data() = "hello";
  for (int i = 0; i < 100; i++) {
    auto& sinkPayload = clientInstruction.sinkPayloads()->emplace_back();
    sinkPayload.data() = folly::to<std::string>(i);
  }

  auto& serverInstruction =
      rpcTest.serverInstruction().emplace().bidiInitialResponse().emplace();
  for (int i = 0; i < 100; i++) {
    auto& payload = serverInstruction.streamPayloads()->emplace_back();
    payload.data() = folly::to<std::string>(i);
  }
  serverInstruction.initialResponse().emplace().data() = "world";

  auto& clientTestResult =
      rpcTest.clientTestResult().emplace().bidiInitialResponse().emplace();
  clientTestResult.streamPayloads().copy_from(
      serverInstruction.streamPayloads());
  clientTestResult.initialResponse().emplace().data() = "world";

  auto& serverResult =
      rpcTest.serverTestResult().emplace().bidiInitialResponse().emplace();
  serverResult.request().emplace().data() = "hello";
  serverResult.sinkPayloads().copy_from(clientInstruction.sinkPayloads());

  return ret;
}

Test createBidiStreamDeclaredExceptionTest() {
  Test ret;
  ret.name() = "BidiStreamDeclaredExceptionTest";
  ret.tags()->emplace("spec/protocol/interface/#bidi-stream-exception");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "BidiStreamDeclaredException/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction = rpcTest.clientInstruction()
                                .emplace()
                                .bidiStreamDeclaredException()
                                .emplace();
  clientInstruction.request().emplace().data() = "hello";
  for (int i = 0; i < 100; i++) {
    auto& sinkPayload = clientInstruction.sinkPayloads()->emplace_back();
    sinkPayload.data() = folly::to<std::string>(i);
  }

  auto& serverInstruction = rpcTest.serverInstruction()
                                .emplace()
                                .bidiStreamDeclaredException()
                                .emplace();
  for (int i = 0; i < 100; i++) {
    auto& payload = serverInstruction.streamPayloads()->emplace_back();
    payload.data() = folly::to<std::string>(i);
  }
  serverInstruction.userException().emplace().msg() = "stream_error";

  rpcTest.clientTestResult()
      .emplace()
      .bidiStreamDeclaredException()
      .emplace()
      .userException()
      .emplace()
      .msg() = "stream_error";
  rpcTest.clientTestResult()
      ->bidiStreamDeclaredException()
      ->streamPayloads()
      .copy_from(serverInstruction.streamPayloads());

  auto& serverResult = rpcTest.serverTestResult()
                           .emplace()
                           .bidiStreamDeclaredException()
                           .emplace();
  serverResult.request().emplace().data() = "hello";
  serverResult.sinkPayloads().copy_from(clientInstruction.sinkPayloads());

  return ret;
}

Test createBidiStreamUndeclaredExceptionTest() {
  Test ret;
  ret.name() = "BidiStreamUndeclaredExceptionTest";
  ret.tags()->emplace("spec/protocol/interface/#bidi-stream-exception");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "BidiStreamUndeclaredException/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction = rpcTest.clientInstruction()
                                .emplace()
                                .bidiStreamUndeclaredException()
                                .emplace();
  clientInstruction.request().emplace().data() = "hello";
  for (int i = 0; i < 100; i++) {
    auto& sinkPayload = clientInstruction.sinkPayloads()->emplace_back();
    sinkPayload.data() = folly::to<std::string>(i);
  }

  auto& serverInstruction = rpcTest.serverInstruction()
                                .emplace()
                                .bidiStreamUndeclaredException()
                                .emplace();
  for (int i = 0; i < 100; i++) {
    auto& payload = serverInstruction.streamPayloads()->emplace_back();
    payload.data() = folly::to<std::string>(i);
  }
  serverInstruction.exceptionMessage() = "stream_error";

  rpcTest.clientTestResult()
      .emplace()
      .bidiStreamUndeclaredException()
      .emplace()
      .exceptionMessage() = "stream_error";
  rpcTest.clientTestResult()
      ->bidiStreamUndeclaredException()
      ->streamPayloads()
      .copy_from(serverInstruction.streamPayloads());

  auto& serverResult = rpcTest.serverTestResult()
                           .emplace()
                           .bidiStreamUndeclaredException()
                           .emplace();
  serverResult.request().emplace().data() = "hello";
  serverResult.sinkPayloads().copy_from(clientInstruction.sinkPayloads());

  return ret;
}

Test createBidiSinkDeclaredExceptionTest() {
  Test ret;
  ret.name() = "BidiSinkDeclaredExceptionTest";
  ret.tags()->emplace("spec/protocol/interface/#bidi-sink-exception");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "BidiSinkDeclaredException/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction = rpcTest.clientInstruction()
                                .emplace()
                                .bidiSinkDeclaredException()
                                .emplace();
  clientInstruction.request().emplace().data() = "request";
  for (int i = 0; i < 100; i++) {
    auto& sinkPayload = clientInstruction.sinkPayloads()->emplace_back();
    sinkPayload.data() = folly::to<std::string>(i);
  }
  clientInstruction.userException().emplace().msg() = "user_exception";

  auto& serverInstruction = rpcTest.serverInstruction()
                                .emplace()
                                .bidiSinkDeclaredException()
                                .emplace();
  for (int i = 0; i < 100; i++) {
    auto& payload = serverInstruction.streamPayloads()->emplace_back();
    payload.data() = folly::to<std::string>(i);
  }

  auto& clientTestResult = rpcTest.clientTestResult()
                               .emplace()
                               .bidiSinkDeclaredException()
                               .emplace();
  clientTestResult.streamPayloads().copy_from(
      serverInstruction.streamPayloads());
  clientTestResult.sinkThrew() = true;

  auto& serverResult = rpcTest.serverTestResult()
                           .emplace()
                           .bidiSinkDeclaredException()
                           .emplace();
  serverResult.request().emplace().data() = "request";
  serverResult.userException().copy_from(clientInstruction.userException());
  serverResult.sinkPayloads().copy_from(clientInstruction.sinkPayloads());

  return ret;
}

Test createBidiSinkUndeclaredExceptionTest() {
  Test ret;
  ret.name() = "BidiSinkUndeclaredExceptionTest";
  ret.tags()->emplace("spec/protocol/interface/#bidi-sink-exception");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "BidiSinkUndeclaredException/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction = rpcTest.clientInstruction()
                                .emplace()
                                .bidiSinkUndeclaredException()
                                .emplace();
  clientInstruction.request().emplace().data() = "request";
  for (int i = 0; i < 100; i++) {
    auto& sinkPayload = clientInstruction.sinkPayloads()->emplace_back();
    sinkPayload.data() = folly::to<std::string>(i);
  }
  clientInstruction.exceptionMessage() = "undeclared_exception";

  auto& serverInstruction = rpcTest.serverInstruction()
                                .emplace()
                                .bidiSinkUndeclaredException()
                                .emplace();
  for (int i = 0; i < 100; i++) {
    auto& payload = serverInstruction.streamPayloads()->emplace_back();
    payload.data() = folly::to<std::string>(i);
  }

  auto& clientTestResult = rpcTest.clientTestResult()
                               .emplace()
                               .bidiSinkUndeclaredException()
                               .emplace();
  clientTestResult.streamPayloads().copy_from(
      serverInstruction.streamPayloads());
  clientTestResult.sinkThrew() = true;

  auto& serverResult = rpcTest.serverTestResult()
                           .emplace()
                           .bidiSinkUndeclaredException()
                           .emplace();
  serverResult.request().copy_from(clientInstruction.request());
  serverResult.exceptionMessage().copy_from(
      clientInstruction.exceptionMessage());
  serverResult.sinkPayloads().copy_from(clientInstruction.sinkPayloads());

  return ret;
}

Test createBidiMethodDeclaredExceptionTest() {
  Test ret;
  ret.name() = "BidiMethodDeclaredExceptionTest";
  ret.tags()->emplace("spec/protocol/interface/#bidi-method-exception");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "BidiMethodDeclaredException/Success";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .bidiMethodDeclaredException()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";

  rpcTest.serverInstruction()
      .emplace()
      .bidiMethodDeclaredException()
      .emplace()
      .userException()
      .emplace()
      .msg() = "method_error";

  rpcTest.clientTestResult()
      .emplace()
      .bidiMethodDeclaredException()
      .emplace()
      .userException()
      .emplace()
      .msg() = "method_error";

  rpcTest.serverTestResult()
      .emplace()
      .bidiMethodDeclaredException()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";

  return ret;
}

Test createBidiMethodUndeclaredExceptionTest() {
  Test ret;
  ret.name() = "BidiMethodUndeclaredExceptionTest";
  ret.tags()->emplace("spec/protocol/interface/#bidi-method-exception");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "BidiMethodUndeclaredException/Success";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .bidiMethodUndeclaredException()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";

  rpcTest.serverInstruction()
      .emplace()
      .bidiMethodUndeclaredException()
      .emplace()
      .exceptionMessage() = "method_error";

  rpcTest.clientTestResult()
      .emplace()
      .bidiMethodUndeclaredException()
      .emplace()
      .exceptionMessage() = "method_error";

  rpcTest.serverTestResult()
      .emplace()
      .bidiMethodUndeclaredException()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";

  return ret;
}

Test createBidiAbortTest() {
  Test ret;
  ret.name() = "BidiAbortTest";
  ret.tags()->emplace("spec/protocol/interface/#bidi");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "BidiAbort/Success";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .bidiAbort()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";
  rpcTest.clientTestResult().emplace().bidiAbort().emplace();
  rpcTest.serverInstruction().emplace().bidiAbort().emplace();
  auto& serverTestResult =
      rpcTest.serverTestResult().emplace().bidiAbort().emplace();
  serverTestResult.request().emplace().data() = "hello";
  serverTestResult.sinkAborted() = true;

  return ret;
}

Test createBidiAbortBeforeInitialResponseTest() {
  Test ret;
  ret.name() = "BidiAbortBeforeInitialResponseTest";
  ret.tags()->emplace("spec/protocol/interface/#bidi");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "BidiAbortBeforeInitialResponse/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction =
      rpcTest.clientInstruction().emplace().bidiAbort().emplace();
  clientInstruction.request().emplace().data() = "hello";
  clientInstruction.giveUpAfterMs() = 100;
  rpcTest.clientTestResult().emplace().bidiAbort().emplace();
  rpcTest.serverInstruction()
      .emplace()
      .bidiAbort()
      .emplace()
      .initialResponseDelayMs() = 500;
  auto& serverTestResult =
      rpcTest.serverTestResult().emplace().bidiAbort().emplace();
  serverTestResult.request().emplace().data() = "hello";
  serverTestResult.sinkAborted() = true;

  return ret;
}

Test createBidiAbortUnreadInitialResponseTest() {
  Test ret;
  ret.name() = "BidiAbortUnreadInitialResponseTest";
  ret.tags()->emplace("spec/protocol/interface/#bidi");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "BidiAbortUnreadInitialResponse/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction =
      rpcTest.clientInstruction().emplace().bidiAbort().emplace();
  clientInstruction.request().emplace().data() = "hello";
  clientInstruction.dropUnreadAfterMs() = 200;
  rpcTest.clientTestResult().emplace().bidiAbort().emplace();
  rpcTest.serverInstruction().emplace().bidiAbort().emplace();
  auto& serverTestResult =
      rpcTest.serverTestResult().emplace().bidiAbort().emplace();
  serverTestResult.request().emplace().data() = "hello";
  serverTestResult.sinkAborted() = true;

  return ret;
}

Test createBidiLoadsheddingTest() {
  Test ret;
  ret.name() = "BidiLoadsheddingTest";
  ret.tags()->emplace("spec/protocol/interface/#loadshedding");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "BidiLoadshedding/Success";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .bidiServerError()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";
  auto& clientTestResult =
      rpcTest.clientTestResult().emplace().bidiServerError().emplace();
  clientTestResult.exceptionType() = TApplicationException::LOADSHEDDING;
  clientTestResult.exceptionMessage() = "shedding the conformance call";
  rpcTest.serverInstruction()
      .emplace()
      .bidiServerError()
      .emplace()
      .error()
      .emplace()
      .loadshedding() = "shedding the conformance call";
  rpcTest.serverTestResult().emplace();

  return ret;
}

Test createBidiUnexpectedErrorCodeTest() {
  Test ret;
  ret.name() = "BidiUnexpectedErrorCodeTest";
  ret.tags()->emplace("spec/compatibility/rocket#internal-server-error");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "BidiUnexpectedErrorCode/Success";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .bidiServerError()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";
  auto& clientTestResult =
      rpcTest.clientTestResult().emplace().bidiServerError().emplace();
  clientTestResult.exceptionType() = TApplicationException::UNKNOWN;
  clientTestResult.exceptionMessage() = "Unexpected error frame type: 513";
  auto& errorFrame = rpcTest.serverInstruction()
                         .emplace()
                         .bidiServerError()
                         .emplace()
                         .error()
                         .emplace()
                         .errorFrame()
                         .emplace();
  errorFrame.code() = kRocketApplicationError;
  errorFrame.message() = "rejected before the first response";
  rpcTest.serverTestResult().emplace();

  return ret;
}

ClientFrame cancelFrame() {
  ClientFrame frame;
  frame.type() = kRocketCancelFrame;
  return frame;
}

ClientFrame canceledErrorFrame() {
  ClientFrame frame;
  frame.type() = kRocketErrorFrame;
  frame.errorCode() = kRocketCanceled;
  frame.errorData() = "";
  return frame;
}

ClientFrame processFirstResponseErrorFrame() {
  ClientFrame frame;
  frame.type() = kRocketErrorFrame;
  frame.errorCode() = kRocketApplicationError;
  frame.errorData() = kProcessFirstResponseError;
  return frame;
}

ClientFrame neverCalledSinkFrame() {
  ClientFrame frame;
  frame.type() = kRocketErrorFrame;
  frame.errorCode() = kRocketApplicationError;
  frame.errorData() =
      "apache::thrift::TApplicationException: never called sink object";
  return frame;
}

std::string undeclaredExceptionMetadata() {
  ResponseRpcMetadata metadata;
  auto& exception =
      metadata.payloadMetadata().emplace().exceptionMetadata().emplace();
  exception.what_utf8() = "the first response is an exception";
  exception.metadata().emplace().appUnknownException().emplace();
  return CompactSerializer::serialize<std::string>(metadata);
}

std::string emptyPayloadMetadata() {
  ResponseRpcMetadata metadata;
  metadata.payloadMetadata().emplace();
  return CompactSerializer::serialize<std::string>(metadata);
}

Test createClosingFrameTest(
    const std::string& kind, CallKind call, std::vector<ClientFrame> frames) {
  Test ret;
  ret.name() = kind + "ClosingFrameTest";
  ret.tags()->emplace("spec/compatibility/rocket#internal-server-error");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = kind + "ClosingFrame/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction =
      rpcTest.clientInstruction().emplace().closingFrame().emplace();
  clientInstruction.call() = call;
  clientInstruction.request().emplace().data() = "hello";
  rpcTest.clientTestResult().emplace().closingFrame().emplace();
  auto& serverInstruction =
      rpcTest.serverInstruction().emplace().closingFrame().emplace();
  serverInstruction.call() = call;
  auto& errorFrame = serverInstruction.firstResponse()->errorFrame().emplace();
  errorFrame.code() = kRocketApplicationError;
  errorFrame.message() = "rejected before the first response";
  rpcTest.serverTestResult().emplace().closingFrame().emplace().frames() =
      std::move(frames);

  return ret;
}

Test createUndecodableFirstResponseTest(
    const std::string& kind, CallKind call, std::vector<ClientFrame> frames) {
  Test ret;
  ret.name() = kind + "UndecodableFirstResponseTest";
  ret.tags()->emplace("spec/compatibility/rocket#stream-initial-response");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = kind + "UndecodableFirstResponse/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction =
      rpcTest.clientInstruction().emplace().closingFrame().emplace();
  clientInstruction.call() = call;
  clientInstruction.request().emplace().data() = "hello";
  rpcTest.clientTestResult().emplace().closingFrame().emplace();
  auto& serverInstruction =
      rpcTest.serverInstruction().emplace().closingFrame().emplace();
  serverInstruction.call() = call;
  serverInstruction.firstResponse()->metadata() = kUndecodableMetadata;
  rpcTest.serverTestResult().emplace().closingFrame().emplace().frames() =
      std::move(frames);

  return ret;
}

Test createInvalidPayloadMetadataTest(
    const std::string& kind, CallKind call, std::vector<ClientFrame> frames) {
  Test ret;
  ret.name() = kind + "InvalidPayloadMetadataTest";
  ret.tags()->emplace("spec/compatibility/rocket#stream-initial-response");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = kind + "InvalidPayloadMetadata/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction =
      rpcTest.clientInstruction().emplace().closingFrame().emplace();
  clientInstruction.call() = call;
  clientInstruction.request().emplace().data() = "hello";
  rpcTest.clientTestResult().emplace().closingFrame().emplace();
  auto& serverInstruction =
      rpcTest.serverInstruction().emplace().closingFrame().emplace();
  serverInstruction.call() = call;
  serverInstruction.firstResponse()->metadata() = emptyPayloadMetadata();
  rpcTest.serverTestResult().emplace().closingFrame().emplace().frames() =
      std::move(frames);

  return ret;
}

Test createUndeclaredExceptionFirstResponseTest(
    const std::string& kind, CallKind call, std::vector<ClientFrame> frames) {
  Test ret;
  ret.name() = kind + "UndeclaredExceptionFirstResponseTest";
  ret.tags()->emplace("spec/compatibility/rocket#stream-initial-response");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = kind + "UndeclaredExceptionFirstResponse/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction =
      rpcTest.clientInstruction().emplace().closingFrame().emplace();
  clientInstruction.call() = call;
  clientInstruction.request().emplace().data() = "hello";
  rpcTest.clientTestResult().emplace().closingFrame().emplace();
  auto& serverInstruction =
      rpcTest.serverInstruction().emplace().closingFrame().emplace();
  serverInstruction.call() = call;
  serverInstruction.firstResponse()->metadata() = undeclaredExceptionMetadata();
  rpcTest.serverTestResult().emplace().closingFrame().emplace().frames() =
      std::move(frames);

  return ret;
}

std::string unknownCompressionMetadata() {
  ResponseRpcMetadata metadata;
  metadata.payloadMetadata().emplace().responseMetadata().emplace();
  metadata.compression() = kUnknownCompression;
  return CompactSerializer::serialize<std::string>(metadata);
}

std::string unknownCompressionChunkMetadata() {
  StreamPayloadMetadata metadata;
  metadata.compression() = kUnknownCompression;
  return CompactSerializer::serialize<std::string>(metadata);
}

ServerError withFirstResponse(std::string metadata) {
  ServerError error;
  error.firstResponseMetadata() = std::move(metadata);
  return error;
}

ServerError withChunk(std::string metadata) {
  ServerError error;
  error.chunkMetadata() = std::move(metadata);
  return error;
}

std::string unknownCompressionTag(const ServerError& error) {
  return error.chunkMetadata()
      ? "spec/compatibility/rocket#stream-responses"
      : "spec/compatibility/rocket#stream-initial-response";
}

Test createRequestResponseUnknownCompressionTest() {
  Test ret;
  ret.name() = "RequestResponseUnknownCompressionTest";
  ret.tags()->emplace("spec/compatibility/rocket#request-response");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "RequestResponseUnknownCompression/Success";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .requestResponseServerError()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";
  auto& clientTestResult = rpcTest.clientTestResult()
                               .emplace()
                               .requestResponseServerError()
                               .emplace();
  clientTestResult.exceptionType() = TApplicationException::PROTOCOL_ERROR;
  clientTestResult.exceptionMessage() = kUnknownCompressionMessage;
  rpcTest.serverInstruction()
      .emplace()
      .requestResponseServerError()
      .emplace()
      .error() = withFirstResponse(unknownCompressionMetadata());
  rpcTest.serverTestResult().emplace();

  return ret;
}

Test createStreamUnknownCompressionTest(
    const std::string& name, ServerError error) {
  Test ret;
  ret.name() = name + "UnknownCompressionTest";
  ret.tags()->emplace(unknownCompressionTag(error));

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = name + "UnknownCompression/Success";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .streamServerError()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";
  auto& clientTestResult =
      rpcTest.clientTestResult().emplace().streamServerError().emplace();
  clientTestResult.exceptionType() = TApplicationException::PROTOCOL_ERROR;
  clientTestResult.exceptionMessage() = kUnknownCompressionMessage;
  rpcTest.serverInstruction().emplace().streamServerError().emplace().error() =
      std::move(error);
  rpcTest.serverTestResult().emplace();

  return ret;
}

Test createSinkUnknownCompressionTest(
    const std::string& name, ServerError error) {
  Test ret;
  ret.name() = name + "UnknownCompressionTest";
  ret.tags()->emplace(unknownCompressionTag(error));

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = name + "UnknownCompression/Success";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .sinkServerError()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";
  auto& clientTestResult =
      rpcTest.clientTestResult().emplace().sinkServerError().emplace();
  clientTestResult.exceptionType() = TApplicationException::PROTOCOL_ERROR;
  clientTestResult.exceptionMessage() = kUnknownCompressionMessage;
  rpcTest.serverInstruction().emplace().sinkServerError().emplace().error() =
      std::move(error);
  rpcTest.serverTestResult().emplace();

  return ret;
}

Test createBidiUnknownCompressionTest(
    const std::string& name, ServerError error) {
  Test ret;
  ret.name() = name + "UnknownCompressionTest";
  ret.tags()->emplace(unknownCompressionTag(error));

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = name + "UnknownCompression/Success";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .bidiServerError()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";
  auto& clientTestResult =
      rpcTest.clientTestResult().emplace().bidiServerError().emplace();
  clientTestResult.exceptionType() = TApplicationException::PROTOCOL_ERROR;
  clientTestResult.exceptionMessage() = kUnknownCompressionMessage;
  rpcTest.serverInstruction().emplace().bidiServerError().emplace().error() =
      std::move(error);
  rpcTest.serverTestResult().emplace();

  return ret;
}

std::string mismatchedChecksumMetadata() {
  ResponseRpcMetadata metadata;
  metadata.payloadMetadata().emplace().responseMetadata().emplace();
  auto& checksum = metadata.checksum().emplace();
  checksum.algorithm() = ChecksumAlgorithm::XXH3_64;
  checksum.checksum() = 1;
  checksum.salt() = 0;
  return CompactSerializer::serialize<std::string>(metadata);
}

Test createRequestResponseChecksumMismatchTest() {
  Test ret;
  ret.name() = "RequestResponseChecksumMismatchTest";
  ret.tags()->emplace("spec/compatibility/rocket#stream-initial-response");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "RequestResponseChecksumMismatch/Success";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .requestResponseServerError()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";
  auto& clientTestResult = rpcTest.clientTestResult()
                               .emplace()
                               .requestResponseServerError()
                               .emplace();
  clientTestResult.exceptionType() = TApplicationException::CHECKSUM_MISMATCH;
  clientTestResult.exceptionMessage() = "Checksum mismatch";
  rpcTest.serverInstruction()
      .emplace()
      .requestResponseServerError()
      .emplace()
      .error()
      .emplace()
      .firstResponseMetadata() = mismatchedChecksumMetadata();
  rpcTest.serverTestResult().emplace();

  return ret;
}

Test createStreamChecksumMismatchTest() {
  Test ret;
  ret.name() = "StreamChecksumMismatchTest";
  ret.tags()->emplace("spec/compatibility/rocket#stream-initial-response");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "StreamChecksumMismatch/Success";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .streamServerError()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";
  auto& clientTestResult =
      rpcTest.clientTestResult().emplace().streamServerError().emplace();
  clientTestResult.exceptionType() = TApplicationException::CHECKSUM_MISMATCH;
  clientTestResult.exceptionMessage() = "Checksum mismatch";
  rpcTest.serverInstruction()
      .emplace()
      .streamServerError()
      .emplace()
      .error()
      .emplace()
      .firstResponseMetadata() = mismatchedChecksumMetadata();
  rpcTest.serverTestResult().emplace();

  return ret;
}

Test createSinkChecksumMismatchTest() {
  Test ret;
  ret.name() = "SinkChecksumMismatchTest";
  ret.tags()->emplace("spec/compatibility/rocket#stream-initial-response");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "SinkChecksumMismatch/Success";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .sinkServerError()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";
  auto& clientTestResult =
      rpcTest.clientTestResult().emplace().sinkServerError().emplace();
  clientTestResult.exceptionType() = TApplicationException::CHECKSUM_MISMATCH;
  clientTestResult.exceptionMessage() = "Checksum mismatch";
  rpcTest.serverInstruction()
      .emplace()
      .sinkServerError()
      .emplace()
      .error()
      .emplace()
      .firstResponseMetadata() = mismatchedChecksumMetadata();
  rpcTest.serverTestResult().emplace();

  return ret;
}

Test createBidiChecksumMismatchTest() {
  Test ret;
  ret.name() = "BidiChecksumMismatchTest";
  ret.tags()->emplace("spec/compatibility/rocket#stream-initial-response");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "BidiChecksumMismatch/Success";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .bidiServerError()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";
  auto& clientTestResult =
      rpcTest.clientTestResult().emplace().bidiServerError().emplace();
  clientTestResult.exceptionType() = TApplicationException::CHECKSUM_MISMATCH;
  clientTestResult.exceptionMessage() = "Checksum mismatch";
  rpcTest.serverInstruction()
      .emplace()
      .bidiServerError()
      .emplace()
      .error()
      .emplace()
      .firstResponseMetadata() = mismatchedChecksumMetadata();
  rpcTest.serverTestResult().emplace();

  return ret;
}

Test createRequestResponseChecksumTest(
    const std::string& algorithm, ChecksumKind checksum) {
  Test ret;
  ret.name() = "RequestResponse" + algorithm + "ChecksumTest";
  ret.tags()->emplace("spec/compatibility/rocket#request-response");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "RequestResponse" + algorithm + "Checksum/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction =
      rpcTest.clientInstruction().emplace().requestResponseBasic().emplace();
  clientInstruction.request().emplace().data() = "hello";
  clientInstruction.checksum() = checksum;
  rpcTest.clientTestResult()
      .emplace()
      .requestResponseBasic()
      .emplace()
      .response()
      .emplace()
      .data() = "world";
  rpcTest.serverInstruction()
      .emplace()
      .requestResponseBasic()
      .emplace()
      .response()
      .emplace()
      .data() = "world";
  auto& serverTestResult =
      rpcTest.serverTestResult().emplace().requestResponseBasic().emplace();
  serverTestResult.request().emplace().data() = "hello";
  serverTestResult.checksum() = checksum;

  return ret;
}

Test createStreamChecksumTest(
    const std::string& algorithm, ChecksumKind checksum) {
  Test ret;
  ret.name() = "Stream" + algorithm + "ChecksumTest";
  ret.tags()->emplace("spec/compatibility/rocket#stream");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "Stream" + algorithm + "Checksum/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction =
      rpcTest.clientInstruction().emplace().streamBasic().emplace();
  clientInstruction.request().emplace().data() = "hello";
  clientInstruction.bufferSize() = kDefaultBufferSize;
  clientInstruction.checksum() = checksum;

  auto& serverInstruction =
      rpcTest.serverInstruction().emplace().streamBasic().emplace();
  for (int i = 0; i < 100; i++) {
    auto& payload = serverInstruction.streamPayloads()->emplace_back();
    payload.data() = folly::to<std::string>(i);
  }

  rpcTest.clientTestResult()
      .emplace()
      .streamBasic()
      .emplace()
      .streamPayloads()
      .copy_from(serverInstruction.streamPayloads());

  auto& serverTestResult =
      rpcTest.serverTestResult().emplace().streamBasic().emplace();
  serverTestResult.request().emplace().data() = "hello";
  serverTestResult.checksum() = checksum;

  return ret;
}

Test createSinkChecksumTest(
    const std::string& algorithm, ChecksumKind checksum) {
  Test ret;
  ret.name() = "Sink" + algorithm + "ChecksumTest";
  ret.tags()->emplace("spec/compatibility/rocket#sink");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "Sink" + algorithm + "Checksum/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction =
      rpcTest.clientInstruction().emplace().sinkBasic().emplace();
  clientInstruction.request().emplace().data() = "hello";
  for (int i = 0; i < 100; i++) {
    auto& sinkPayload = clientInstruction.sinkPayloads()->emplace_back();
    sinkPayload.data() = folly::to<std::string>(i);
  }
  clientInstruction.checksum() = checksum;

  rpcTest.clientTestResult()
      .emplace()
      .sinkBasic()
      .emplace()
      .finalResponse()
      .emplace()
      .data() = "world";

  auto& serverInstruction =
      rpcTest.serverInstruction().emplace().sinkBasic().emplace();
  serverInstruction.finalResponse().emplace().data() = "world";
  serverInstruction.bufferSize() = kDefaultBufferSize;

  auto& serverTestResult =
      rpcTest.serverTestResult().emplace().sinkBasic().emplace();
  serverTestResult.request().emplace().data() = "hello";
  serverTestResult.sinkPayloads().copy_from(clientInstruction.sinkPayloads());
  serverTestResult.checksum() = checksum;

  return ret;
}

Test createBidiChecksumTest(
    const std::string& algorithm, ChecksumKind checksum) {
  Test ret;
  ret.name() = "Bidi" + algorithm + "ChecksumTest";
  ret.tags()->emplace("spec/compatibility/rocket#bidi");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "Bidi" + algorithm + "Checksum/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction =
      rpcTest.clientInstruction().emplace().bidiBasic().emplace();
  clientInstruction.request().emplace().data() = "hello";
  for (int i = 0; i < 100; i++) {
    auto& sinkPayload = clientInstruction.sinkPayloads()->emplace_back();
    sinkPayload.data() = folly::to<std::string>(i);
  }
  clientInstruction.checksum() = checksum;

  auto& serverInstruction =
      rpcTest.serverInstruction().emplace().bidiBasic().emplace();
  for (int i = 0; i < 100; i++) {
    auto& payload = serverInstruction.streamPayloads()->emplace_back();
    payload.data() = folly::to<std::string>(i);
  }

  rpcTest.clientTestResult()
      .emplace()
      .bidiBasic()
      .emplace()
      .streamPayloads()
      .copy_from(serverInstruction.streamPayloads());

  auto& serverTestResult =
      rpcTest.serverTestResult().emplace().bidiBasic().emplace();
  serverTestResult.request().emplace().data() = "hello";
  serverTestResult.sinkPayloads().copy_from(clientInstruction.sinkPayloads());
  serverTestResult.checksum() = checksum;

  return ret;
}

std::string mismatchedChunkChecksumMetadata() {
  StreamPayloadMetadata metadata;
  auto& checksum = metadata.checksum().emplace();
  checksum.algorithm() = ChecksumAlgorithm::XXH3_64;
  checksum.checksum() = 1;
  checksum.salt() = 0;
  return CompactSerializer::serialize<std::string>(metadata);
}

Test createStreamChunkChecksumMismatchTest() {
  Test ret;
  ret.name() = "StreamChunkChecksumMismatchTest";
  ret.tags()->emplace("spec/compatibility/rocket#stream-responses");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "StreamChunkChecksumMismatch/Success";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .streamServerError()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";
  auto& clientTestResult =
      rpcTest.clientTestResult().emplace().streamServerError().emplace();
  clientTestResult.exceptionType() = TApplicationException::CHECKSUM_MISMATCH;
  clientTestResult.exceptionMessage() = "Checksum mismatch";
  rpcTest.serverInstruction()
      .emplace()
      .streamServerError()
      .emplace()
      .error()
      .emplace()
      .chunkMetadata() = mismatchedChunkChecksumMetadata();
  rpcTest.serverTestResult().emplace();

  return ret;
}

Test createBidiChunkChecksumMismatchTest() {
  Test ret;
  ret.name() = "BidiChunkChecksumMismatchTest";
  ret.tags()->emplace("spec/compatibility/rocket#stream-responses");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "BidiChunkChecksumMismatch/Success";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .bidiServerError()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";
  auto& clientTestResult =
      rpcTest.clientTestResult().emplace().bidiServerError().emplace();
  clientTestResult.exceptionType() = TApplicationException::CHECKSUM_MISMATCH;
  clientTestResult.exceptionMessage() = "Checksum mismatch";
  rpcTest.serverInstruction()
      .emplace()
      .bidiServerError()
      .emplace()
      .error()
      .emplace()
      .chunkMetadata() = mismatchedChunkChecksumMetadata();
  rpcTest.serverTestResult().emplace();

  return ret;
}

Test createSinkFinalResponseChecksumMismatchTest() {
  Test ret;
  ret.name() = "SinkFinalResponseChecksumMismatchTest";
  ret.tags()->emplace("spec/compatibility/rocket#sink-final-response");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "SinkFinalResponseChecksumMismatch/Success";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .sinkServerError()
      .emplace()
      .request()
      .emplace()
      .data() = "hello";
  auto& clientTestResult =
      rpcTest.clientTestResult().emplace().sinkServerError().emplace();
  clientTestResult.exceptionType() = TApplicationException::CHECKSUM_MISMATCH;
  clientTestResult.exceptionMessage() = "Checksum mismatch";
  rpcTest.serverInstruction()
      .emplace()
      .sinkServerError()
      .emplace()
      .error()
      .emplace()
      .chunkMetadata() = mismatchedChunkChecksumMetadata();
  rpcTest.serverTestResult().emplace();

  return ret;
}

Test createChecksumMismatchClosingFrameTest(
    const std::string& kind, CallKind call, std::vector<ClientFrame> frames) {
  Test ret;
  ret.name() = kind + "ChecksumMismatchClosingFrameTest";
  ret.tags()->emplace("spec/compatibility/rocket#stream-initial-response");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = kind + "ChecksumMismatchClosingFrame/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction =
      rpcTest.clientInstruction().emplace().closingFrame().emplace();
  clientInstruction.call() = call;
  clientInstruction.request().emplace().data() = "hello";
  rpcTest.clientTestResult().emplace().closingFrame().emplace();
  auto& serverInstruction =
      rpcTest.serverInstruction().emplace().closingFrame().emplace();
  serverInstruction.call() = call;
  serverInstruction.firstResponse()->metadata() = mismatchedChecksumMetadata();
  rpcTest.serverTestResult().emplace().closingFrame().emplace().frames() =
      std::move(frames);

  return ret;
}

// =================== Interactions ===================
Test createInteractionConstructorTest() {
  Test ret;
  ret.name() = "InteractionConstructorTest";
  ret.tags()->emplace("spec/protocol/interface/#constructors-deprecated");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "InteractionConstructorTest/Success";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction().emplace().interactionConstructor().emplace();

  rpcTest.clientTestResult().emplace().interactionConstructor().emplace();

  rpcTest.serverInstruction().emplace().interactionConstructor().emplace();

  rpcTest.serverTestResult()
      .emplace()
      .interactionConstructor()
      .emplace()
      .constructorCalled() = true;

  return ret;
}

Test createInteractionFactoryFunctionTest() {
  Test ret;
  ret.name() = "InteractionFactoryFunctionTest";
  ret.tags()->emplace("spec/protocol/interface/#factory-functions");
  ret.tags()->emplace("spec/protocol/interface/rocket/#factory-functions");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "InteractionFactoryFunctionTest/Success";

  constexpr int initialSum = 10;

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .interactionFactoryFunction()
      .emplace()
      .initialSum() = initialSum;

  rpcTest.clientTestResult().emplace().interactionFactoryFunction().emplace();

  rpcTest.serverInstruction().emplace().interactionFactoryFunction().emplace();

  rpcTest.serverTestResult()
      .emplace()
      .interactionFactoryFunction()
      .emplace()
      .initialSum() = initialSum;

  return ret;
}

Test createInteractionConstructorPersistsStateTest() {
  Test ret;
  ret.name() = "InteractionConstructorPersistsStateTest";
  ret.tags()->emplace("spec/protocol/interface/#interactions");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "InteractionConstructorPersistsStateTest/Success";

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction = rpcTest.clientInstruction()
                                .emplace()
                                .interactionPersistsState()
                                .emplace();

  auto& clientTestResult =
      rpcTest.clientTestResult().emplace().interactionPersistsState().emplace();

  int sum = 0;
  for (int i = 1; i <= 5; i++) {
    sum += i;
    clientInstruction.valuesToAdd()->emplace_back(i);
    clientTestResult.responses()->emplace_back(sum);
  }

  rpcTest.serverInstruction().emplace().interactionPersistsState().emplace();

  rpcTest.serverTestResult().emplace().interactionPersistsState().emplace();

  return ret;
}

Test createInteractionFactoryFunctionPersistsStateTest() {
  Test ret;
  ret.name() = "InteractionFactoryFunctionPersistsStateTest";
  ret.tags()->emplace("spec/protocol/interface/#interactions");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "InteractionFactoryFunctionPersistsStateTest/Success";

  constexpr int initialSum = 10;

  auto& rpcTest = testCase.rpc().emplace();
  auto& clientInstruction = rpcTest.clientInstruction()
                                .emplace()
                                .interactionPersistsState()
                                .emplace();
  clientInstruction.initialSum() = initialSum;

  auto& clientTestResult =
      rpcTest.clientTestResult().emplace().interactionPersistsState().emplace();

  int sum = initialSum;
  for (int i = 1; i <= 5; i++) {
    sum += i;
    clientInstruction.valuesToAdd()->emplace_back(i);
    clientTestResult.responses()->emplace_back(sum);
  }

  rpcTest.serverInstruction().emplace().interactionPersistsState().emplace();

  rpcTest.serverTestResult().emplace().interactionPersistsState().emplace();

  return ret;
}

Test createInteractionConstructorTerminationTest() {
  Test ret;
  ret.name() = "InteractionConstructorTerminationTest";
  ret.tags()->emplace("spec/protocol/interface/#termination");
  ret.tags()->emplace("spec/protocol/interface/rocket/#termination");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "InteractionConstructorTerminationTest/Success";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction().emplace().interactionTermination().emplace();

  rpcTest.clientTestResult().emplace().interactionTermination().emplace();

  rpcTest.serverInstruction().emplace().interactionTermination().emplace();

  rpcTest.serverTestResult()
      .emplace()
      .interactionTermination()
      .emplace()
      .terminationReceived() = true;

  return ret;
}

Test createInteractionFactoryFunctionTerminationTest() {
  Test ret;
  ret.name() = "InteractionFactoryFunctionTerminationTest";
  ret.tags()->emplace("spec/protocol/interface/#sink-chunk-timeout");
  ret.tags()->emplace("spec/protocol/interface/rocket/#termination");

  auto& testCase = ret.testCases()->emplace_back();
  testCase.name() = "InteractionFactoryFunctionTerminationTest/Success";

  auto& rpcTest = testCase.rpc().emplace();
  rpcTest.clientInstruction()
      .emplace()
      .interactionTermination()
      .emplace()
      .initialSum() = 10;

  rpcTest.clientTestResult().emplace().interactionTermination().emplace();

  rpcTest.serverInstruction().emplace().interactionTermination().emplace();

  rpcTest.serverTestResult()
      .emplace()
      .interactionTermination()
      .emplace()
      .terminationReceived() = true;

  return ret;
}

void addCommonRequestResponseTests(TestSuite& suite) {
  // =================== Request-Response ===================
  suite.tests()->push_back(createRequestResponseBasicTest());
  suite.tests()->push_back(createRequestResponseDeclaredExceptionTest());
  suite.tests()->push_back(createRequestResponseUndeclaredExceptionTest());
  suite.tests()->push_back(createRequestResponseNoArgVoidResponse());
  suite.tests()->push_back(createRequestResponseFragmentationTest());
}

void addCommonRPCTests(TestSuite& suite) {
  // =================== Request-Response ===================
  addCommonRequestResponseTests(suite);
  // =================== Stream ===================
  suite.tests()->push_back(createStreamBasicTest());
  suite.tests()->push_back(createStreamFragmentationTest());
  suite.tests()->push_back(createStreamInitialResponseTest());
  suite.tests()->push_back(createStreamSubsequentCreditsTest());
  suite.tests()->push_back(createStreamDeclaredExceptionTest());
  suite.tests()->push_back(createStreamUndeclaredExceptionTest());
  suite.tests()->push_back(createStreamInitialDeclaredExceptionTest());
  suite.tests()->push_back(createStreamInitialUndeclaredExceptionTest());
  // =================== Sink ===================
  suite.tests()->push_back(createSinkBasicTest());
  suite.tests()->push_back(createSinkFragmentationTest());
  suite.tests()->push_back(createSinkSubsequentCreditsTest());
  suite.tests()->push_back(createSinkInitialResponseTest());
  suite.tests()->push_back(createSinkDeclaredExceptionTest());
  suite.tests()->push_back(createSinkUndeclaredExceptionTest());
  suite.tests()->push_back(createSinkInitialDeclaredExceptionTest());
  suite.tests()->push_back(createSinkServerDeclaredExceptionTest());
  // =================== BiDi Streaming ===================
  suite.tests()->push_back(createBidiBasicTest());
  suite.tests()->push_back(createBidiInitialResponseTest());
  suite.tests()->push_back(createBidiStreamDeclaredExceptionTest());
  suite.tests()->push_back(createBidiStreamUndeclaredExceptionTest());
  suite.tests()->push_back(createBidiSinkDeclaredExceptionTest());
  suite.tests()->push_back(createBidiSinkUndeclaredExceptionTest());
  suite.tests()->push_back(createBidiMethodDeclaredExceptionTest());
  suite.tests()->push_back(createBidiMethodUndeclaredExceptionTest());
  // =================== Interactions ===================
  suite.tests()->push_back(createInteractionConstructorTest());
  suite.tests()->push_back(createInteractionFactoryFunctionTest());
  suite.tests()->push_back(createInteractionConstructorPersistsStateTest());
  suite.tests()->push_back(createInteractionFactoryFunctionPersistsStateTest());
  suite.tests()->push_back(createInteractionConstructorTerminationTest());
  suite.tests()->push_back(createInteractionFactoryFunctionTerminationTest());
}

void addExtendedRequestResponseTests(TestSuite& suite) {
  suite.tests()->push_back(createRequestResponseAnyExceptionTest());
  suite.tests()->push_back(createRequestResponseLoadsheddingTest());
  suite.tests()->push_back(createRequestResponseTaskExpiredTest());
  suite.tests()->push_back(createRequestResponseTooBigTest());
  suite.tests()->push_back(createRequestResponseUndeclaredExceptionTypeTest());
  suite.tests()->push_back(createRequestResponseUnknownMethodTest());
  suite.tests()->push_back(createRequestResponseWriteHeadersTest());
  suite.tests()->push_back(createRequestResponsePersistentHeadersTest());
  suite.tests()->push_back(createRequestResponseTimeoutsSentTest());
  suite.tests()->push_back(createRequestResponseClientOnlyTimeoutsTest());
  suite.tests()->push_back(createRequestResponsePriorityTest());
  suite.tests()->push_back(createRequestResponseConcurrentTest());
}

} // namespace

TestSuite createRPCServerTestSuite() {
  TestSuite suite;
  suite.name() = "ThriftRPCServerTest";
  suite.tags()->emplace("spec/protocol/interface/");
  addCommonRPCTests(suite);
  // =================== Sink ===================
  suite.tests()->push_back(createSinkChunkTimeoutTest());
  suite.tests()->push_back(createSinkChunkDoesNotTimeoutTest());
  return suite;
}

TestSuite createBasicRPCServerTestSuite() {
  TestSuite suite;
  suite.name() = "ThriftBasicRPCServerTest";
  suite.tags()->emplace("spec/protocol/interface/");
  addCommonRequestResponseTests(suite);
  return suite;
}

TestSuite createRPCClientTestSuite() {
  TestSuite suite;
  suite.name() = "ThriftRPCClientTest";
  suite.tags()->emplace("spec/protocol/interface/");
  addCommonRPCTests(suite);
  // =================== Request-Response ===================
  suite.tests()->push_back(createRequestResponseTimeoutTest());
  // =================== Stream ===================
  suite.tests()->push_back(createStreamChunkTimeoutTest());
  suite.tests()->push_back(createStreamCreditTimeoutTest());
  suite.tests()->push_back(createStreamInitialTimeoutTest());
  return suite;
}

TestSuite createRPCClientExtendedTestSuite() {
  TestSuite suite;
  suite.name() = "ThriftRPCClientTest";
  suite.tags()->emplace("spec/protocol/interface/");
  addExtendedRequestResponseTests(suite);
  suite.tests()->push_back(createRequestResponseUnexpectedErrorCodeTest());
  suite.tests()->push_back(createStreamCancelTest());
  suite.tests()->push_back(createStreamCancelBeforeInitialResponseTest());
  suite.tests()->push_back(createStreamCancelUnreadInitialResponseTest());
  suite.tests()->push_back(createStreamLoadsheddingTest());
  suite.tests()->push_back(createStreamUnexpectedErrorCodeTest());
  suite.tests()->push_back(createStreamMemoryBufferSizeTest());
  suite.tests()->push_back(createSinkChunkTimeoutTest());
  suite.tests()->push_back(createSinkEarlyFinalResponseTest());
  suite.tests()->push_back(createSinkAbortTest());
  suite.tests()->push_back(createSinkAbortBeforeInitialResponseTest());
  suite.tests()->push_back(createSinkAbortUnreadInitialResponseTest());
  suite.tests()->push_back(createSinkLoadsheddingTest());
  suite.tests()->push_back(createSinkUnexpectedErrorCodeTest());
  suite.tests()->push_back(createBidiAbortTest());
  suite.tests()->push_back(createBidiAbortBeforeInitialResponseTest());
  suite.tests()->push_back(createBidiAbortUnreadInitialResponseTest());
  suite.tests()->push_back(createBidiLoadsheddingTest());
  suite.tests()->push_back(createBidiUnexpectedErrorCodeTest());
  suite.tests()->push_back(createClosingFrameTest(
      "RequestResponse", CallKind::REQUEST_RESPONSE, {}));
  suite.tests()->push_back(
      createClosingFrameTest("Stream", CallKind::STREAM, {cancelFrame()}));
  suite.tests()->push_back(
      createClosingFrameTest("Sink", CallKind::SINK, {canceledErrorFrame()}));
  suite.tests()->push_back(
      createClosingFrameTest("Bidi", CallKind::BIDI, {canceledErrorFrame()}));
  suite.tests()->push_back(createUndecodableFirstResponseTest(
      "RequestResponse", CallKind::REQUEST_RESPONSE, {}));
  suite.tests()->push_back(createUndecodableFirstResponseTest(
      "Stream", CallKind::STREAM, {cancelFrame()}));
  suite.tests()->push_back(createUndecodableFirstResponseTest(
      "Sink", CallKind::SINK, {canceledErrorFrame()}));
  suite.tests()->push_back(createUndecodableFirstResponseTest(
      "Bidi", CallKind::BIDI, {canceledErrorFrame()}));
  suite.tests()->push_back(createInvalidPayloadMetadataTest(
      "RequestResponse", CallKind::REQUEST_RESPONSE, {}));
  suite.tests()->push_back(createInvalidPayloadMetadataTest(
      "Stream", CallKind::STREAM, {cancelFrame()}));
  suite.tests()->push_back(createInvalidPayloadMetadataTest(
      "Sink", CallKind::SINK, {processFirstResponseErrorFrame()}));
  suite.tests()->push_back(createInvalidPayloadMetadataTest(
      "Bidi",
      CallKind::BIDI,
      {cancelFrame(), processFirstResponseErrorFrame()}));
  suite.tests()->push_back(createUndeclaredExceptionFirstResponseTest(
      "RequestResponse", CallKind::REQUEST_RESPONSE, {}));
  suite.tests()->push_back(createUndeclaredExceptionFirstResponseTest(
      "Stream", CallKind::STREAM, {cancelFrame()}));
  suite.tests()->push_back(createUndeclaredExceptionFirstResponseTest(
      "Sink", CallKind::SINK, {neverCalledSinkFrame()}));
  suite.tests()->push_back(createUndeclaredExceptionFirstResponseTest(
      "Bidi", CallKind::BIDI, {neverCalledSinkFrame(), cancelFrame()}));
  suite.tests()->push_back(createRequestResponseUnknownCompressionTest());
  suite.tests()->push_back(createStreamUnknownCompressionTest(
      "Stream", withFirstResponse(unknownCompressionMetadata())));
  suite.tests()->push_back(createStreamUnknownCompressionTest(
      "StreamChunk", withChunk(unknownCompressionChunkMetadata())));
  suite.tests()->push_back(createSinkUnknownCompressionTest(
      "Sink", withFirstResponse(unknownCompressionMetadata())));
  suite.tests()->push_back(createSinkUnknownCompressionTest(
      "SinkFinalResponse", withChunk(unknownCompressionChunkMetadata())));
  suite.tests()->push_back(createBidiUnknownCompressionTest(
      "Bidi", withFirstResponse(unknownCompressionMetadata())));
  suite.tests()->push_back(createBidiUnknownCompressionTest(
      "BidiChunk", withChunk(unknownCompressionChunkMetadata())));
  return suite;
}

TestSuite createRPCClientRequestResponseTestSuite() {
  TestSuite suite;
  suite.name() = "ThriftRPCClientTest";
  suite.tags()->emplace("spec/protocol/interface/");
  addCommonRequestResponseTests(suite);
  suite.tests()->push_back(createRequestResponseTimeoutTest());
  addExtendedRequestResponseTests(suite);
  return suite;
}

TestSuite createRPCClientHTTP2TestSuite() {
  TestSuite suite;
  suite.name() = "ThriftRPCClientTest";
  suite.tags()->emplace("spec/protocol/interface/");
  suite.tests()->push_back(createRequestResponseBadContentLengthTest());
  return suite;
}

TestSuite createRPCClientChecksumTestSuite() {
  TestSuite suite;
  suite.name() = "ThriftRPCClientTest";
  suite.tags()->emplace("spec/compatibility/");
  suite.tests()->push_back(
      createRequestResponseChecksumTest("Crc32", ChecksumKind::CRC32));
  suite.tests()->push_back(
      createRequestResponseChecksumTest("Xxh3", ChecksumKind::XXH3_64));
  suite.tests()->push_back(
      createStreamChecksumTest("Crc32", ChecksumKind::CRC32));
  suite.tests()->push_back(
      createStreamChecksumTest("Xxh3", ChecksumKind::XXH3_64));
  suite.tests()->push_back(
      createSinkChecksumTest("Crc32", ChecksumKind::CRC32));
  suite.tests()->push_back(
      createSinkChecksumTest("Xxh3", ChecksumKind::XXH3_64));
  suite.tests()->push_back(
      createBidiChecksumTest("Crc32", ChecksumKind::CRC32));
  suite.tests()->push_back(
      createBidiChecksumTest("Xxh3", ChecksumKind::XXH3_64));
  suite.tests()->push_back(createRequestResponseChecksumMismatchTest());
  suite.tests()->push_back(createStreamChecksumMismatchTest());
  suite.tests()->push_back(createSinkChecksumMismatchTest());
  suite.tests()->push_back(createBidiChecksumMismatchTest());
  suite.tests()->push_back(createChecksumMismatchClosingFrameTest(
      "RequestResponse", CallKind::REQUEST_RESPONSE, {}));
  suite.tests()->push_back(createChecksumMismatchClosingFrameTest(
      "Stream", CallKind::STREAM, {cancelFrame()}));
  suite.tests()->push_back(createChecksumMismatchClosingFrameTest(
      "Sink", CallKind::SINK, {canceledErrorFrame()}));
  suite.tests()->push_back(createChecksumMismatchClosingFrameTest(
      "Bidi", CallKind::BIDI, {canceledErrorFrame()}));
  suite.tests()->push_back(createStreamChunkChecksumMismatchTest());
  suite.tests()->push_back(createBidiChunkChecksumMismatchTest());
  suite.tests()->push_back(createSinkFinalResponseChecksumMismatchTest());
  return suite;
}

} // namespace apache::thrift::conformance::data
