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
#include <type_traits>
#include <utility>

#include <boost/intrusive_ptr.hpp>
#include <gtest/gtest.h>

#include <folly/io/async/EventBase.h>
#include <folly/io/async/ssl/BasicTransportCertificate.h>
#include <folly/io/async/test/MockAsyncTransport.h>
#include <folly/portability/GMock.h>
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
}

TEST(NativeThriftContextViewTest, LiveRequestStateLookupAcrossSourceViews) {
  using RequestView = ThriftRequestContextView<ThriftContextBackend::Native>;
  const auto layout = borrowedStateLayout();
  LookupState first(1);
  LookupState replacement(2);
  folly::EventBase eventBase;
  ThriftServerRequestMessage request;
  request.requestContext = makeThriftRequestContext(eventBase);
  auto* context = request.requestContext.get();

  const RequestView direct(context);
  const RequestView read{ThriftRequestView(request)};
  ThriftRequestMutator write(request);
  const RequestView mutator(write);
  ThriftServerResponseMessage response;
  response.requestContext = std::move(request.requestContext);
  const RequestView responseView{ThriftResponseView(response)};
  RequestView::NativeInputs inputs;
  inputs.valid = true;
  inputs.extensionSlots = &context->extensionSlots();
  const RequestView captured(inputs);
  const std::array views{direct, read, mutator, responseView, captured};

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

  context->setState<StateExtension>(nullptr);
  for (const auto& view : views) {
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
  auto connection =
      boost::intrusive_ptr<ThriftConnContext>(new ThriftConnContext);
  auto otherConnection =
      boost::intrusive_ptr<ThriftConnContext>(new ThriftConnContext);
  auto request = makeThriftRequestContext(eventBase);
  auto otherRequest = makeThriftRequestContext(eventBase);
  request->setConnectionContext(connection);
  otherRequest->setConnectionContext(otherConnection);
  const ThriftRequestContextView requestView(request.get());
  const ThriftRequestContextView otherRequestView(otherRequest.get());
  const ConnectionView direct(connection.get());
  const ConnectionView temporary{ThriftConnectionView(*connection)};
  ConnectionView::NativeInputs inputs;
  inputs.valid = true;
  inputs.extensionSlots = &connection->extensionSlots();
  const ConnectionView captured(inputs);
  const std::array views{
      direct, temporary, requestView.connectionContext(), captured};
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
  request->setState<StateExtension>(&requestState);
  otherRequest->setState<StateExtension>(&otherRequestState);
  for (const auto& view : views) {
    EXPECT_EQ(view.tryState<StateExtension>(), &connectionState);
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
  for (const auto& view : views) {
    EXPECT_EQ(view.tryState<StateExtension>(), &replacement);
    EXPECT_EQ(view.tryMutableState<StateExtension>(), &replacement);
  }
  connection->setState<StateExtension>(nullptr);
  for (const auto& view : views) {
    EXPECT_EQ(view.tryState<StateExtension>(), nullptr);
    EXPECT_EQ(view.tryMutableState<StateExtension>(), nullptr);
    EXPECT_EQ(view.tryState<OtherStateExtension>(), &otherExtensionState);
    EXPECT_EQ(
        view.tryMutableState<OtherStateExtension>(), &otherExtensionState);
  }
  EXPECT_EQ(requestView.tryState<StateExtension>(), &requestState);
  EXPECT_EQ(otherRequestView.tryState<StateExtension>(), &otherRequestState);
  EXPECT_EQ(other.tryState<StateExtension>(), &otherConnectionState);
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
  EXPECT_EQ(view.tryState<StateExtension>(), nullptr);
  EXPECT_EQ(view.tryMutableState<StateExtension>(), nullptr);

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
