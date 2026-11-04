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

#include <thrift/lib/cpp2/fast_thrift/thrift/server/compatibility/context/NativeThriftContextView.h>

#include <gtest/gtest.h>

#include <folly/io/async/EventBase.h>
#include <folly/io/async/ssl/BasicTransportCertificate.h>
#include <folly/io/async/test/MockAsyncTransport.h>
#include <folly/portability/GMock.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/extension/ThriftExtension.h>

namespace apache::thrift::fast_thrift::thrift::server::compatibility {
namespace {

TEST(
    NativeThriftContextViewTest,
    ReadOnlyExtensionViewCannotWriteResponseHeaders) {
  folly::EventBase eventBase;
  ThriftServerRequestMessage request;
  request.requestContext = makeThriftRequestContext(eventBase);
  request.requestContext->setHeaders({{"trace", "id"}});
  request.requestContext->setMethodName("method");
  ThriftServerMethodMetadata method;
  method.qualifiedMethodName = "Service.method";
  request.requestContext->setMethodMetadata(&method);

  const ThriftRequestView read(request);
  const ThriftRequestContextView view(read);

  EXPECT_EQ(
      view.header("trace"), &request.requestContext->getHeaders().at("trace"));
  EXPECT_EQ(view.methodName(), "Service.method");
  EXPECT_THROW(
      view.setResponseHeader("trace", std::string("id")), std::logic_error);

  ThriftRequestMutator write(request);
  const ThriftRequestContextView mutator(write);
  EXPECT_TRUE(mutator.setResponseHeader("trace", std::string("response-id")));
  EXPECT_EQ(*request.requestContext->getResponseHeader("trace"), "response-id");
  EXPECT_EQ(*view.header("trace"), "id");
}

TEST(
    NativeThriftContextViewTest,
    ResponseViewBorrowsRequestHeadersAndIsReadOnly) {
  folly::EventBase eventBase;
  ThriftServerResponseMessage response;
  response.requestContext = makeThriftRequestContext(eventBase);
  response.requestContext->setHeaders({{"trace", "id"}});
  response.requestContext->setMethodName("method");
  const ThriftResponseView read(response);
  const ThriftRequestContextView view(read);

  EXPECT_EQ(
      view.header("trace"), &response.requestContext->getHeaders().at("trace"));
  EXPECT_EQ(view.methodName(), "method");
  EXPECT_THROW(
      view.setResponseHeader("trace", std::string("id")), std::logic_error);
}

TEST(
    NativeThriftContextViewTest,
    MissingContextStillExposesExtensionMethodName) {
  ThriftServerRequestMessage request;
  auto metadata = std::make_unique<apache::thrift::RequestRpcMetadata>();
  metadata->name() = "method";
  request.payload =
      ThriftServerRequestResponsePayload{.metadata = std::move(metadata)};
  const ThriftRequestView read(request);
  const ThriftRequestContextView view(read);

  EXPECT_FALSE(view);
  EXPECT_EQ(view.methodName(), "method");
  EXPECT_EQ(view.header("trace"), nullptr);
  EXPECT_FALSE(view.connectionContext());

  ThriftRequestMutator write(request);
  const ThriftRequestContextView mutator(write);
  EXPECT_FALSE(mutator);
  EXPECT_THROW(
      mutator.setResponseHeader("trace", std::string("id")), std::logic_error);
}

TEST(
    NativeThriftContextViewTest,
    UsesLiveCertificateAndRetainsHandshakeInformation) {
  auto retained = std::make_shared<folly::ssl::BasicTransportCertificate>(
      "retained", nullptr);
  const folly::ssl::BasicTransportCertificate live("live", nullptr);
  testing::NiceMock<folly::test::MockAsyncTransport> transport;
  ThriftConnContext context;
  context.setPeerCertificate(retained);
  context.setSecurityProtocol("TLS1.3");
  context.setTransport(&transport);
  const ThriftConnectionContextView view(&context);

  EXPECT_CALL(transport, getPeerCertificate()).WillOnce(testing::Return(&live));
  EXPECT_EQ(view.peerCertificate(), &live);
  EXPECT_CALL(transport, getPeerCertificate())
      .WillOnce(testing::Return(nullptr));
  EXPECT_EQ(view.peerCertificate(), nullptr);
  EXPECT_TRUE(view.peerCertReceived());
  EXPECT_EQ(view.securityProtocol(), "TLS1.3");

  context.setTransport(nullptr);
  const ThriftConnectionContextView detached(&context);
  EXPECT_EQ(detached.peerCertificate(), retained.get());
  EXPECT_TRUE(detached.peerCertReceived());
  EXPECT_EQ(detached.securityProtocol(), "TLS1.3");
}

TEST(NativeThriftContextViewTest, LiveCertificateExceptionsPropagate) {
  testing::NiceMock<folly::test::MockAsyncTransport> transport;
  ThriftConnContext context;
  context.setTransport(&transport);
  const ThriftConnectionContextView view(&context);

  EXPECT_CALL(transport, getPeerCertificate())
      .WillOnce(testing::Throw(std::runtime_error("certificate unavailable")));
  EXPECT_THROW(view.peerCertificate(), std::runtime_error);
}

TEST(
    NativeThriftContextViewTest,
    CachedEkmMatchesLabelAndLengthAndClonesStorage) {
  using ConnectionView =
      ThriftConnectionContextView<ThriftContextBackend::Native>;
  const auto value = folly::IOBuf::copyBuffer("key-material");
  const ConnectionView::CachedEkm cached[] = {
      {"label", 12, value.get()}, {"empty", 12, nullptr}};
  ConnectionView::NativeInputs inputs;
  inputs.valid = true;
  inputs.cachedEkms = cached;
  const ConnectionView view(inputs);

  const auto first = view.cachedEkm("label", 12);
  const auto second = view.cachedEkm("label", 12);
  ASSERT_NE(first, nullptr);
  ASSERT_NE(second, nullptr);
  EXPECT_NE(first.get(), value.get());
  EXPECT_NE(first.get(), second.get());
  EXPECT_EQ(first->clone()->moveToFbString(), "key-material");
  EXPECT_EQ(second->clone()->moveToFbString(), "key-material");
  EXPECT_EQ(view.cachedEkm("label", 11), nullptr);
  EXPECT_EQ(view.cachedEkm("other", 12), nullptr);
  EXPECT_EQ(view.cachedEkm("empty", 12), nullptr);
}

} // namespace
} // namespace apache::thrift::fast_thrift::thrift::server::compatibility
