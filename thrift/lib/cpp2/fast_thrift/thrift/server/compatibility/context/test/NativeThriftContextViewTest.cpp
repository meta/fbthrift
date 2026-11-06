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

#include <array>
#include <memory>
#include <stdexcept>
#include <type_traits>
#include <utility>

#include <boost/intrusive_ptr.hpp>
#include <gtest/gtest.h>

#include <folly/io/async/EventBase.h>
#include <folly/io/async/ssl/BasicTransportCertificate.h>
#include <folly/io/async/test/MockAsyncTransport.h>
#include <folly/portability/GMock.h>
#include <folly/portability/OpenSSL.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/ExtensionSlots.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/ThriftConnContext.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/ThriftRequestContext.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/extension/ThriftConnectionExtension.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/extension/ThriftExtension.h>

namespace apache::thrift::fast_thrift::thrift::server::compatibility {
namespace {

struct LookupState {
  explicit LookupState(int value) : LookupState(value, nullptr) {}
  LookupState(int value, int* destructions)
      : value(value), destructions(destructions) {}
  ~LookupState() {
    if (destructions != nullptr) {
      ++*destructions;
    }
  }
  LookupState(const LookupState&) = delete;
  LookupState& operator=(const LookupState&) = delete;

  int value;
  int* destructions;
};

struct StateExtension {
  EXTENSION_ID(native_compat_state);
  using ConnState = LookupState;
  using RequestState = LookupState;
};

struct OtherStateExtension {
  EXTENSION_ID(native_compat_other_state);
  using ConnState = LookupState;
  using RequestState = LookupState;
};

struct OwnedStateExtension {
  EXTENSION_ID(native_compat_owned_state);
  using RequestState = LookupState;
};

ExtensionLayout borrowedStateLayout() {
  ExtensionLayoutBuilder builder;
  builder.add(StateExtension::kId);
  builder.add(OtherStateExtension::kId);
  return std::move(builder).build();
}

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
  const auto header = view.headerContainer();

  EXPECT_EQ(
      view.header("trace"), &request.requestContext->getHeaders().at("trace"));
  EXPECT_EQ(view.methodName(), "Service.method");
  EXPECT_THROW(
      view.setResponseHeader("trace", std::string("id")), std::logic_error);
  const std::string id = "id";
  EXPECT_THROW(view.setResponseHeader("trace", id), std::logic_error);
  EXPECT_FALSE(request.requestContext->hasResponseHeaders());
  EXPECT_TRUE(header);
  EXPECT_EQ(&header.headers(), &request.requestContext->getHeaders());
  EXPECT_THROW(header.setResponseHeader("header-copy", id), std::logic_error);
  EXPECT_FALSE(request.requestContext->hasResponseHeaders());

  ThriftRequestMutator write(request);
  const ThriftRequestContextView mutator(write);
  EXPECT_TRUE(mutator.setResponseHeader("trace", std::string("response-id")));
  EXPECT_EQ(*request.requestContext->getResponseHeader("trace"), "response-id");
  EXPECT_EQ(*view.header("trace"), "id");
  const auto mutableHeader = mutator.headerContainer();
  EXPECT_TRUE(mutableHeader.setResponseHeader(
      "header-copy", mutableHeader.headers().at("trace")));
  EXPECT_TRUE(
      mutableHeader.setResponseHeader("header-moved", std::string("moved-id")));
  EXPECT_EQ(*request.requestContext->getResponseHeader("header-copy"), "id");
  EXPECT_EQ(
      *request.requestContext->getResponseHeader("header-moved"), "moved-id");

  auto originalContext = std::move(request.requestContext);
  request.requestContext = makeThriftRequestContext(eventBase);
  request.requestContext->setHeaders({{"trace", "replacement-id"}});
  request.requestContext->setMethodName("replacement-method");

  EXPECT_TRUE(view);
  ASSERT_EQ(view.requestHeaders(), &request.requestContext->getHeaders());
  EXPECT_EQ(*view.header("trace"), "replacement-id");
  EXPECT_EQ(view.methodName(), "replacement-method");
  EXPECT_EQ(mutator.methodName(), "replacement-method");
  EXPECT_THROW(view.setResponseHeader("trace", id), std::logic_error);
  EXPECT_TRUE(mutator.setResponseHeader("trace", id));
  EXPECT_EQ(*request.requestContext->getResponseHeader("trace"), id);
  EXPECT_EQ(*originalContext->getResponseHeader("trace"), "response-id");
  EXPECT_EQ(&header.headers(), &request.requestContext->getHeaders());
  EXPECT_THROW(header.setResponseHeader("header-copy", id), std::logic_error);
  EXPECT_TRUE(mutableHeader.setResponseHeader(
      "header-copy", mutableHeader.headers().at("trace")));
  EXPECT_EQ(
      *request.requestContext->getResponseHeader("header-copy"),
      "replacement-id");
  EXPECT_EQ(*originalContext->getResponseHeader("header-copy"), "id");

  request.requestContext.reset();
  EXPECT_FALSE(view);
  EXPECT_FALSE(mutator);
  EXPECT_FALSE(view.hasHeaderContainer());
  EXPECT_EQ(view.requestHeaders(), nullptr);
  EXPECT_EQ(view.header("trace"), nullptr);
  EXPECT_THROW(mutator.setResponseHeader("trace", id), std::logic_error);
  EXPECT_FALSE(header);
  EXPECT_FALSE(mutableHeader);
  EXPECT_TRUE(header.headers().empty());
  EXPECT_THROW(
      mutableHeader.setResponseHeader("header-copy", id), std::logic_error);
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
  const auto header = view.headerContainer();

  EXPECT_EQ(
      view.header("trace"), &response.requestContext->getHeaders().at("trace"));
  EXPECT_EQ(view.methodName(), "method");
  EXPECT_THROW(
      view.setResponseHeader("trace", std::string("id")), std::logic_error);
  const std::string id = "id";
  EXPECT_THROW(view.setResponseHeader("trace", id), std::logic_error);
  EXPECT_TRUE(header);
  EXPECT_EQ(header.headers().at("trace"), "id");
  EXPECT_THROW(header.setResponseHeader("trace", id), std::logic_error);

  response.requestContext = makeThriftRequestContext(eventBase);
  response.requestContext->setHeaders({{"trace", "replacement-id"}});
  ThriftServerMethodMetadata method;
  method.qualifiedMethodName = "Service.replacement";
  response.requestContext->setMethodMetadata(&method);

  EXPECT_TRUE(view);
  ASSERT_EQ(view.requestHeaders(), &response.requestContext->getHeaders());
  EXPECT_EQ(*view.header("trace"), "replacement-id");
  EXPECT_EQ(view.methodName(), "Service.replacement");
  EXPECT_THROW(view.setResponseHeader("trace", id), std::logic_error);
  EXPECT_EQ(&header.headers(), &response.requestContext->getHeaders());
  EXPECT_EQ(header.headers().at("trace"), "replacement-id");
  EXPECT_THROW(header.setResponseHeader("trace", id), std::logic_error);
  EXPECT_FALSE(response.requestContext->hasResponseHeaders());

  response.requestContext.reset();
  EXPECT_FALSE(view);
  EXPECT_FALSE(view.hasHeaderContainer());
  EXPECT_EQ(view.requestHeaders(), nullptr);
  EXPECT_EQ(view.header("trace"), nullptr);
  EXPECT_TRUE(view.methodName().empty());
  EXPECT_THROW(view.setResponseHeader("trace", id), std::logic_error);
  EXPECT_FALSE(header);
  EXPECT_TRUE(header.headers().empty());
  EXPECT_THROW(header.setResponseHeader("trace", id), std::logic_error);
}

TEST(NativeThriftContextViewTest, ResponseRetirementFollowsCurrentPayload) {
  ThriftServerResponseMessage response;
  const ThriftResponseView view(response);

  response.payload = ThriftServerStreamOpenPayload{};
  EXPECT_FALSE(view.isRequestRetiring());
  response.payload = ThriftStreamInitialResponsePayload{.complete = false};
  EXPECT_TRUE(view.isRequestRetiring());
  response.payload = ThriftStreamPayload{};
  EXPECT_FALSE(view.isRequestRetiring());
  response.payload = ThriftInitialResponsePayload{};
  EXPECT_TRUE(view.isRequestRetiring());
  response.payload = ThriftStreamPayload{.complete = true};
  EXPECT_FALSE(view.isRequestRetiring());
  response.payload = ThriftErrorPayload{};
  EXPECT_TRUE(view.isRequestRetiring());
  response.payload = ThriftSetupResponsePayload{};
  EXPECT_FALSE(view.isRequestRetiring());
  response.payload = ThriftSetupRejectionPayload{};
  EXPECT_FALSE(view.isRequestRetiring());
}

TEST(
    NativeThriftContextViewTest,
    MissingContextStillExposesExtensionMethodName) {
  folly::EventBase eventBase;
  ThriftServerRequestMessage request;
  auto metadata = std::make_unique<apache::thrift::RequestRpcMetadata>();
  metadata->name() = "method";
  auto* requestMetadata = metadata.get();
  request.payload =
      ThriftServerRequestResponsePayload{.metadata = std::move(metadata)};
  const ThriftRequestView read(request);
  const ThriftRequestContextView view(read);

  EXPECT_FALSE(view);
  EXPECT_EQ(view.methodName(), "method");
  EXPECT_EQ(view.header("trace"), nullptr);
  EXPECT_EQ(view.tryState<StateExtension>(), nullptr);
  EXPECT_EQ(view.tryMutableState<StateExtension>(), nullptr);
  const auto connection = view.connectionContext();
  EXPECT_FALSE(connection);
  EXPECT_EQ(connection.tryState<StateExtension>(), nullptr);
  EXPECT_EQ(connection.tryMutableState<StateExtension>(), nullptr);

  ThriftRequestMutator write(request);
  const ThriftRequestContextView mutator(write);
  EXPECT_FALSE(mutator);
  EXPECT_EQ(mutator.tryState<StateExtension>(), nullptr);
  EXPECT_EQ(mutator.tryMutableState<StateExtension>(), nullptr);
  EXPECT_THROW(
      mutator.setResponseHeader("trace", std::string("id")), std::logic_error);

  ThriftServerResponseMessage response;
  const ThriftRequestContextView responseView{ThriftResponseView(response)};
  EXPECT_FALSE(responseView);
  EXPECT_EQ(responseView.tryState<StateExtension>(), nullptr);
  EXPECT_EQ(responseView.tryMutableState<StateExtension>(), nullptr);

  requestMetadata->name() = "updated-method";
  EXPECT_EQ(view.methodName(), "updated-method");
  EXPECT_EQ(mutator.methodName(), "updated-method");

  request.requestContext = makeThriftRequestContext(eventBase);
  request.requestContext->setMethodName("context-method");
  EXPECT_TRUE(view);
  EXPECT_TRUE(mutator);
  EXPECT_EQ(view.methodName(), "context-method");
  EXPECT_EQ(mutator.methodName(), "context-method");

  ThriftServerMethodMetadata method;
  method.qualifiedMethodName = "Service.method";
  request.requestContext->setMethodMetadata(&method);
  EXPECT_EQ(view.methodName(), "Service.method");
  request.requestContext->setMethodMetadata(nullptr);
  EXPECT_EQ(view.methodName(), "context-method");
  request.requestContext->setMethodName("");
  EXPECT_EQ(view.methodName(), "updated-method");

  response.requestContext = std::move(request.requestContext);
  EXPECT_FALSE(view);
  EXPECT_FALSE(mutator);
  EXPECT_EQ(view.methodName(), "updated-method");
  EXPECT_TRUE(responseView);
  EXPECT_TRUE(responseView.methodName().empty());
}

TEST(NativeThriftContextViewTest, LiveRequestStateLookupAcrossSourceViews) {
  using RequestView = ThriftRequestContextView<ThriftContextBackend::Native>;
  const auto layout = borrowedStateLayout();
  LookupState first(1);
  LookupState replacement(2);
  LookupState messageReplacement(3);
  folly::EventBase eventBase;
  ThriftServerRequestMessage request;
  request.requestContext = makeThriftRequestContext(eventBase);
  auto* context = request.requestContext.get();

  const RequestView direct(context);
  const RequestView read{ThriftRequestView(request)};
  ThriftRequestMutator write(request);
  const RequestView mutator(write);
  ThriftServerResponseMessage response;
  const RequestView responseView{ThriftResponseView(response)};
  const auto copy = direct;
  const std::array views{direct, read, mutator, copy};

  static_assert(std::is_same_v<
                decltype(direct.tryState<StateExtension>()),
                const LookupState*>);
  static_assert(std::is_same_v<
                decltype(direct.tryMutableState<StateExtension>()),
                LookupState*>);
  for (const auto& view : views) {
    EXPECT_EQ(view.tryState<StateExtension>(), nullptr);
    EXPECT_EQ(view.tryMutableState<StateExtension>(), nullptr);
  }
  EXPECT_EQ(responseView.tryState<StateExtension>(), nullptr);
  EXPECT_EQ(responseView.tryMutableState<StateExtension>(), nullptr);

  context->installExtensions(layout);
  for (auto* state : std::array{&first, &replacement}) {
    context->setState<StateExtension>(state);
    for (const auto& view : views) {
      ASSERT_EQ(view.tryState<StateExtension>(), state);
      ASSERT_EQ(view.tryMutableState<StateExtension>(), state);
    }
  }
  read.tryMutableState<StateExtension>()->value = 17;
  for (const auto& view : views) {
    EXPECT_EQ(view.tryState<StateExtension>()->value, 17);
  }

  response.requestContext = std::move(request.requestContext);
  const std::array movedViews{direct, copy, responseView};
  for (const auto& view : movedViews) {
    EXPECT_EQ(view.tryState<StateExtension>(), &replacement);
    EXPECT_EQ(view.tryMutableState<StateExtension>(), &replacement);
  }
  for (const auto& view : std::array{read, mutator}) {
    EXPECT_FALSE(view);
    EXPECT_EQ(view.tryState<StateExtension>(), nullptr);
    EXPECT_EQ(view.tryMutableState<StateExtension>(), nullptr);
  }

  request.requestContext = makeThriftRequestContext(eventBase);
  request.requestContext->installExtensions(layout);
  request.requestContext->setState<StateExtension>(&messageReplacement);
  for (const auto& view : std::array{read, mutator}) {
    EXPECT_TRUE(view);
    EXPECT_EQ(view.tryState<StateExtension>(), &messageReplacement);
    EXPECT_EQ(view.tryMutableState<StateExtension>(), &messageReplacement);
  }
  context->setState<StateExtension>(nullptr);
  for (const auto& view : movedViews) {
    EXPECT_EQ(view.tryState<StateExtension>(), nullptr);
    EXPECT_EQ(view.tryMutableState<StateExtension>(), nullptr);
  }
  for (const auto& view : std::array{read, mutator}) {
    EXPECT_EQ(view.tryState<StateExtension>(), &messageReplacement);
  }
  request.requestContext->setState<StateExtension>(nullptr);
  for (const auto& view : std::array{read, mutator}) {
    EXPECT_EQ(view.tryState<StateExtension>(), nullptr);
    EXPECT_EQ(view.tryMutableState<StateExtension>(), nullptr);
  }
}

TEST(
    NativeThriftContextViewTest,
    LiveConnectionStateLookupIsScopeAndOwnerIsolated) {
  using ConnectionView =
      ThriftConnectionContextView<ThriftContextBackend::Native>;
  const auto layout = borrowedStateLayout();
  LookupState connectionState(1);
  LookupState replacement(2);
  LookupState otherConnectionState(3);
  LookupState requestState(4);
  LookupState otherRequestState(5);
  LookupState otherExtensionState(6);
  folly::EventBase eventBase;
  const folly::SocketAddress peer("127.0.0.1", 1234);
  const folly::SocketAddress otherPeer("127.0.0.1", 4321);
  auto connection =
      boost::intrusive_ptr<ThriftConnContext>(new ThriftConnContext);
  auto otherConnection =
      boost::intrusive_ptr<ThriftConnContext>(new ThriftConnContext);
  connection->setPeerAddress(peer);
  otherConnection->setPeerAddress(otherPeer);
  auto request = makeThriftRequestContext(eventBase);
  auto otherRequest = makeThriftRequestContext(eventBase);
  request->setConnectionContext(connection);
  otherRequest->setConnectionContext(otherConnection);
  const ThriftRequestContextView requestView(request.get());
  const ThriftRequestContextView otherRequestView(otherRequest.get());
  const ConnectionView direct(connection.get());
  const ConnectionView temporary{ThriftConnectionView(*connection)};
  const auto copy = direct;
  const std::array views{
      direct, temporary, requestView.connectionContext(), copy};
  const ConnectionView other(otherConnection.get());

  static_assert(std::is_same_v<
                decltype(direct.tryState<StateExtension>()),
                const LookupState*>);
  static_assert(std::is_same_v<
                decltype(direct.tryMutableState<StateExtension>()),
                LookupState*>);
  for (const auto& view : views) {
    EXPECT_EQ(view.tryState<StateExtension>(), nullptr);
    EXPECT_EQ(view.tryMutableState<StateExtension>(), nullptr);
  }

  connection->installExtensions(layout);
  otherConnection->installExtensions(layout);
  request->installExtensions(layout);
  otherRequest->installExtensions(layout);
  connection->setState<StateExtension>(&connectionState);
  connection->setState<OtherStateExtension>(&otherExtensionState);
  otherConnection->setState<StateExtension>(&otherConnectionState);
  connection->setPeerIdentities(&connectionState);
  otherConnection->setPeerIdentities(&otherConnectionState);
  request->setState<StateExtension>(&requestState);
  otherRequest->setState<StateExtension>(&otherRequestState);
  for (const auto& view : views) {
    EXPECT_EQ(view.tryState<StateExtension>(), &connectionState);
    EXPECT_EQ(view.peerIdentities(), &connectionState);
    EXPECT_EQ(view.tryState<OtherStateExtension>(), &otherExtensionState);
    EXPECT_EQ(
        view.tryMutableState<OtherStateExtension>(), &otherExtensionState);
  }
  EXPECT_EQ(requestView.tryState<StateExtension>(), &requestState);
  EXPECT_EQ(requestView.tryMutableState<StateExtension>(), &requestState);
  EXPECT_EQ(requestView.tryState<OtherStateExtension>(), nullptr);
  EXPECT_EQ(requestView.tryMutableState<OtherStateExtension>(), nullptr);
  EXPECT_EQ(otherRequestView.tryState<StateExtension>(), &otherRequestState);
  EXPECT_EQ(
      otherRequestView.tryMutableState<StateExtension>(), &otherRequestState);
  EXPECT_EQ(other.tryState<StateExtension>(), &otherConnectionState);
  EXPECT_EQ(other.tryMutableState<StateExtension>(), &otherConnectionState);
  EXPECT_EQ(other.tryState<OtherStateExtension>(), nullptr);

  connection->setState<StateExtension>(&replacement);
  connection->setPeerIdentities(&replacement);
  for (const auto& view : views) {
    EXPECT_EQ(view.tryState<StateExtension>(), &replacement);
    EXPECT_EQ(view.tryMutableState<StateExtension>(), &replacement);
    EXPECT_EQ(view.peerIdentities(), &replacement);
  }
  connection->setState<StateExtension>(nullptr);
  connection->setPeerIdentities(nullptr);
  for (const auto& view : views) {
    EXPECT_EQ(view.tryState<StateExtension>(), nullptr);
    EXPECT_EQ(view.tryMutableState<StateExtension>(), nullptr);
    EXPECT_EQ(view.tryState<OtherStateExtension>(), &otherExtensionState);
    EXPECT_EQ(
        view.tryMutableState<OtherStateExtension>(), &otherExtensionState);
    EXPECT_EQ(view.peerIdentities(), nullptr);
  }
  EXPECT_EQ(requestView.tryState<StateExtension>(), &requestState);
  EXPECT_EQ(otherRequestView.tryState<StateExtension>(), &otherRequestState);
  EXPECT_EQ(other.tryState<StateExtension>(), &otherConnectionState);

  request->setConnectionContext(otherConnection);
  EXPECT_EQ(
      requestView.connectionContext().tryState<StateExtension>(),
      &otherConnectionState);
  EXPECT_EQ(
      requestView.requiredConnectionContext().tryMutableState<StateExtension>(),
      &otherConnectionState);
  EXPECT_EQ(
      requestView.connectionContext().peerIdentities(), &otherConnectionState);
  ASSERT_NE(requestView.peerAddress(), nullptr);
  EXPECT_EQ(*requestView.peerAddress(), otherPeer);
  EXPECT_EQ(requestView.tryState<StateExtension>(), &requestState);
  for (const auto& view : views) {
    EXPECT_EQ(view.tryState<StateExtension>(), nullptr);
    EXPECT_EQ(view.tryState<OtherStateExtension>(), &otherExtensionState);
    EXPECT_EQ(view.peerIdentities(), nullptr);
    ASSERT_NE(view.peerAddress(), nullptr);
    EXPECT_EQ(*view.peerAddress(), peer);
  }
}

TEST(
    NativeThriftContextViewTest,
    OwnedRequestStateOutlivesViewsAndClearsLiveLookup) {
  ExtensionLayoutBuilder builder;
  builder.addOwned<OwnedStateExtension::RequestState>(OwnedStateExtension::kId);
  const auto layout = std::move(builder).build();
  int destructions = 0;
  folly::EventBase eventBase;

  {
    auto context = makeThriftRequestContext(eventBase);
    context->installExtensions(layout);
    const ThriftRequestContextView view(context.get());
    EXPECT_EQ(view.tryState<OwnedStateExtension>(), nullptr);
    auto* first =
        &context->emplaceState<OwnedStateExtension>(11, &destructions);
    {
      const auto copy = view;
      EXPECT_EQ(copy.tryState<OwnedStateExtension>(), first);
      EXPECT_EQ(copy.tryMutableState<OwnedStateExtension>(), first);
    }
    EXPECT_EQ(destructions, 0);
    EXPECT_EQ(context->tryState<OwnedStateExtension>(), first);
    context->clearOwnedState<OwnedStateExtension>();
    EXPECT_EQ(destructions, 1);
    EXPECT_EQ(view.tryState<OwnedStateExtension>(), nullptr);
    EXPECT_EQ(view.tryMutableState<OwnedStateExtension>(), nullptr);

    {
      auto state = std::make_unique<LookupState>(22, &destructions);
      const auto* second = state.get();
      context->adoptState<OwnedStateExtension>(state.release());
      const auto copy = view;
      EXPECT_EQ(copy.tryState<OwnedStateExtension>(), second);
    }
    EXPECT_EQ(destructions, 1);
    EXPECT_NE(view.tryState<OwnedStateExtension>(), nullptr);
  }
  EXPECT_EQ(destructions, 2);
}

TEST(
    NativeThriftContextViewTest,
    UsesLiveCertificateAndRetainsHandshakeInformation) {
  auto retained = std::make_shared<folly::ssl::BasicTransportCertificate>(
      "retained", folly::ssl::X509UniquePtr(X509_new()));
  const auto retainedX509 = retained->getX509();
  ASSERT_NE(retainedX509, nullptr);
  ASSERT_EQ(
      X509_NAME_add_entry_by_txt(
          X509_get_subject_name(retainedX509.get()),
          "CN",
          MBSTRING_ASC,
          reinterpret_cast<const unsigned char*>("retained"),
          -1,
          -1,
          0),
      1);
  auto replacement = std::make_shared<folly::ssl::BasicTransportCertificate>(
      "replacement", folly::ssl::X509UniquePtr(X509_new()));
  const auto replacementX509 = replacement->getX509();
  ASSERT_NE(replacementX509, nullptr);
  const folly::ssl::BasicTransportCertificate live("live", nullptr);
  testing::NiceMock<folly::test::MockAsyncTransport> transport;
  testing::NiceMock<folly::test::MockAsyncTransport> replacementTransport;
  ThriftConnContext context;
  context.setPeerCertificate(retained);
  context.setSecurityProtocol("TLS1.3");
  context.setTransport(&transport);
  const ThriftConnectionContextView view(&context);

  EXPECT_EQ(view.transport(), &transport);
  EXPECT_EQ(view.peerX509Certificate(), retainedX509.get());
  EXPECT_CALL(transport, getPeerCertificate()).WillOnce(testing::Return(&live));
  EXPECT_EQ(view.peerCertificate(), &live);
  EXPECT_CALL(transport, getPeerCertificate())
      .WillOnce(testing::Return(nullptr));
  EXPECT_EQ(view.peerCertificate(), nullptr);
  EXPECT_TRUE(view.peerCertReceived());
  EXPECT_EQ(view.securityProtocol(), "TLS1.3");

  context.setTransport(nullptr);
  EXPECT_EQ(view.transport(), nullptr);
  EXPECT_EQ(view.peerCertificate(), retained.get());
  EXPECT_EQ(view.peerX509Certificate(), retainedX509.get());
  EXPECT_EQ(view.peerCommonName(), "retained");
  EXPECT_TRUE(view.peerCertReceived());
  EXPECT_EQ(view.securityProtocol(), "TLS1.3");

  context.setPeerCertificate(replacement);
  context.setSecurityProtocol("TLSv1.3");
  EXPECT_EQ(view.peerCertificate(), replacement.get());
  EXPECT_EQ(view.peerX509Certificate(), replacementX509.get());
  EXPECT_EQ(view.securityProtocol(), "TLSv1.3");

  context.setTransport(&replacementTransport);
  EXPECT_EQ(view.transport(), &replacementTransport);
  EXPECT_CALL(replacementTransport, getPeerCertificate())
      .WillOnce(testing::Return(&live));
  EXPECT_EQ(view.peerCertificateSource().get(), &live);

  context.setTransport(nullptr);
  context.setPeerCertificate(nullptr);
  context.setSecurityProtocol("");
  EXPECT_EQ(view.peerCertificate(), nullptr);
  EXPECT_EQ(view.peerX509Certificate(), nullptr);
  EXPECT_FALSE(view.peerCertReceived());
  EXPECT_TRUE(view.securityProtocol().empty());
}

TEST(NativeThriftContextViewTest, LiveCertificateExceptionsPropagate) {
  testing::NiceMock<folly::test::MockAsyncTransport> transport;
  ThriftConnContext context;
  context.setTransport(&transport);
  const ThriftConnectionContextView view(&context);

  EXPECT_CALL(transport, getPeerCertificate())
      .WillOnce(testing::Throw(std::runtime_error("certificate unavailable")));
  EXPECT_THROW(view.peerCertificate(), std::runtime_error);
  EXPECT_CALL(transport, getPeerCertificate())
      .WillOnce(testing::Throw(std::runtime_error("certificate unavailable")));
  EXPECT_THROW(view.peerCommonName(), std::runtime_error);
  EXPECT_EQ(view.peerX509Certificate(), nullptr);

  context.setTransport(nullptr);
  EXPECT_EQ(view.peerCertificate(), nullptr);
  EXPECT_TRUE(view.peerCommonName().empty());
}

} // namespace
} // namespace apache::thrift::fast_thrift::thrift::server::compatibility
