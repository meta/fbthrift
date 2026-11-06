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

#include <thrift/lib/cpp2/fast_thrift/thrift/server/compatibility/context/ClassicThriftContextView.h>

#include <memory>
#include <type_traits>
#include <utility>

#include <gtest/gtest.h>

#include <thrift/lib/cpp2/PluggableFunction.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/ExtensionSlots.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/ThriftRequestContext.h>

namespace apache::thrift::fast_thrift::thrift::server::compatibility {
namespace {

struct ConnectionFields {
  int value{0};
  std::shared_ptr<int> lifetime;
};

struct RequestFields {
  int value{0};
  std::shared_ptr<int> lifetime;
};

struct TestExtension {
  EXTENSION_ID(thrift_compatibility_context_view_test);
  using ConnState = ConnectionFields;
  using RequestState = RequestFields;
};

} // namespace
} // namespace apache::thrift::fast_thrift::thrift::server::compatibility

namespace apache::thrift::detail {

THRIFT_PLUGGABLE_FUNC_SET(
    ConnectionInternalFieldsT, createPerConnectionInternalFields) {
  using State =
      fast_thrift::thrift::server::compatibility::TestExtension::ConnState;
  return ConnectionInternalFieldsT{std::in_place_type<State>};
}

THRIFT_PLUGGABLE_FUNC_SET(
    RequestInternalFieldsT, createPerRequestInternalFields) {
  using State =
      fast_thrift::thrift::server::compatibility::TestExtension::RequestState;
  return RequestInternalFieldsT{std::in_place_type<State>};
}

} // namespace apache::thrift::detail

namespace apache::thrift::fast_thrift::thrift::server::compatibility {
namespace {

class HeaderSwitchingRequestContext
    : public apache::thrift::Cpp2RequestContext {
 public:
  using apache::thrift::Cpp2RequestContext::Cpp2RequestContext;

  void setHeaderContainer(apache::thrift::transport::THeader* header) {
    header_ = header;
  }
};

template <class View>
void stampTraceResponse(const View& request) {
  if (const auto* trace = request.header("trace")) {
    request.setResponseHeader("trace", *trace);
    request.setResponseHeader("status", std::string("traced"));
  }
}

template <ThriftContextBackend Backend>
void stampTraceResponse(const ThriftRequestHeaderView<Backend>& header) {
  if (!header) {
    return;
  }
  const auto trace = header.headers().find("trace");
  if (trace == header.headers().end()) {
    return;
  }
  header.setResponseHeader("trace", trace->second);
  header.setResponseHeader("status", std::string("traced"));
}

template <class View>
void checkHeaderAccess(
    const View& request, const typename View::HeaderMap& headers) {
  const auto traceKey = std::string_view("trace");
  const auto hashToken = headers.prehash(traceKey);
  EXPECT_EQ(request.requestHeaders(), &headers);
  EXPECT_EQ(request.header(traceKey), &headers.at("trace"));
  EXPECT_EQ(request.header(hashToken, traceKey), &headers.at("trace"));
  EXPECT_EQ(request.header("missing"), nullptr);
  EXPECT_TRUE(request.hasHeaders());
}

template <class View>
void copyConnectionState(
    const View& request,
    RequestFields& requestFields,
    ConnectionFields& connectionFields) {
  static_assert(std::is_same_v<
                decltype(request.template tryState<TestExtension>()),
                const RequestFields*>);
  auto* requestState = request.template tryMutableState<TestExtension>();
  const auto connection = request.connectionContext();
  static_assert(std::is_same_v<
                decltype(connection.template tryState<TestExtension>()),
                const ConnectionFields*>);
  const auto* connectionState = connection.template tryState<TestExtension>();
  ASSERT_EQ(requestState, &requestFields);
  ASSERT_EQ(connectionState, &connectionFields);
  EXPECT_EQ(request.template tryState<TestExtension>(), &requestFields);
  EXPECT_EQ(
      connection.template tryMutableState<TestExtension>(), &connectionFields);
  requestState->value = connectionState->value;
}

TEST(ThriftContextViewTest, SharesHeaderLogicAcrossClassicAndNativeContexts) {
  const ThriftRequestHeaderView<ThriftContextBackend::Classic>::HeaderMap
      replacementHeaders{
          {"trace", "captured-trace-id"}, {"request-only", "inbound"}};
  apache::thrift::transport::THeader header;
  header.setReadHeader("trace", "trace-id");
  header.setReadHeader("request-only", "inbound");
  apache::thrift::transport::THeader reboundHeader;
  HeaderSwitchingRequestContext classic(nullptr, &header, "Service.method");

  ThriftRequestContext native;
  native.setMethodName("method");
  ThriftServerMethodMetadata method;
  method.qualifiedMethodName = "Service.method";
  native.setMethodMetadata(&method);
  native.setHeaders(header.getHeaders());

  const ThriftRequestContextView classicView(&classic);
  const ThriftRequestContextView nativeView(&native);
  const auto classicHeader = classicView.headerContainer();
  const auto nativeHeader = nativeView.headerContainer();

  EXPECT_EQ(classicView.methodName(), nativeView.methodName());
  checkHeaderAccess(classicView, header.getHeaders());
  checkHeaderAccess(nativeView, native.getHeaders());
  stampTraceResponse(classicView);
  stampTraceResponse(nativeView);

  const auto written = native.extractResponseHeaders();
  EXPECT_EQ(header.getWriteHeaders(), written);
  EXPECT_EQ(written.at("trace"), "trace-id");
  EXPECT_EQ(written.at("status"), "traced");
  EXPECT_FALSE(written.contains("request-only"));
  EXPECT_EQ(native.getHeaders().at("trace"), "trace-id");

  header.setReadHeader("trace", "updated-trace-id");
  native.setHeader("trace", "updated-trace-id");
  checkHeaderAccess(classicView, header.getHeaders());
  checkHeaderAccess(nativeView, native.getHeaders());
  stampTraceResponse(classicView);
  stampTraceResponse(nativeView);
  EXPECT_EQ(native.extractResponseHeaders(), header.getWriteHeaders());

  header.setReadHeadersView(replacementHeaders);
  native.setHeaders(replacementHeaders);
  reboundHeader.setReadHeader("trace", "rebound-id");
  classic.setHeaderContainer(&reboundHeader);
  EXPECT_EQ(*classicView.header("trace"), "rebound-id");
  EXPECT_EQ(&classicHeader.headers(), &replacementHeaders);
  stampTraceResponse(classicHeader);
  stampTraceResponse(nativeHeader);
  const auto captured = native.extractResponseHeaders();
  EXPECT_EQ(captured, header.getWriteHeaders());
  EXPECT_EQ(captured.at("trace"), "captured-trace-id");
  EXPECT_EQ(captured.at("status"), "traced");
  EXPECT_FALSE(captured.contains("request-only"));
  EXPECT_TRUE(reboundHeader.getWriteHeaders().empty());
  const auto rebound = classicView.headerContainer();
  stampTraceResponse(rebound);
  EXPECT_EQ(reboundHeader.getWriteHeaders().at("trace"), "rebound-id");

  native.setMethodMetadata(nullptr);
  native.setMethodName("updated-method");
  EXPECT_EQ(nativeView.methodName(), "updated-method");
}

TEST(ThriftContextViewTest, SharesStateLogicAcrossClassicAndNativeContexts) {
  ExtensionLayoutBuilder builder;
  builder.add(TestExtension::kId);
  const auto layout = std::move(builder).build();
  ConnectionFields nativeConnectionFields;
  RequestFields nativeRequestFields;
  boost::intrusive_ptr<ThriftConnContext> nativeConnection(
      new ThriftConnContext);
  nativeConnection->installExtensions(layout);
  ThriftRequestContext native;
  native.installExtensions(layout);
  native.setConnectionContext(nativeConnection);
  const ThriftRequestContextView nativeView(&native);

  EXPECT_EQ(nativeView.tryState<TestExtension>(), nullptr);
  EXPECT_EQ(nativeView.connectionContext().tryState<TestExtension>(), nullptr);
  nativeConnection->setState<TestExtension>(&nativeConnectionFields);
  native.setState<TestExtension>(&nativeRequestFields);

  apache::thrift::Cpp2ConnContext classicConnection;
  apache::thrift::Cpp2RequestContext classic(&classicConnection);
  const ThriftRequestContextView classicView(&classic);
  auto& classicConnectionFields =
      classicConnection.getInternalFields<TestExtension::ConnState>();
  auto& classicRequestFields =
      classic.getInternalFields<TestExtension::RequestState>();
  classicConnectionFields.value = 42;
  nativeConnectionFields.value = 42;

  copyConnectionState(
      classicView, classicRequestFields, classicConnectionFields);
  copyConnectionState(nativeView, nativeRequestFields, nativeConnectionFields);

  EXPECT_EQ(classicRequestFields.value, 42);
  EXPECT_EQ(nativeRequestFields.value, classicRequestFields.value);
  native.setState<TestExtension>(nullptr);
  EXPECT_EQ(nativeView.tryState<TestExtension>(), nullptr);
  EXPECT_EQ(nativeView.tryMutableState<TestExtension>(), nullptr);
}

TEST(ThriftContextViewTest, StateLifetimeBelongsToTheContextOwner) {
  ExtensionLayoutBuilder builder;
  builder.addOwned<TestExtension::RequestState>(TestExtension::kId);
  const auto layout = std::move(builder).build();
  std::weak_ptr<int> classicConnectionLifetime;
  std::weak_ptr<int> classicRequestLifetime;
  std::weak_ptr<int> nativeRequestLifetime;
  {
    apache::thrift::Cpp2ConnContext classicConnection;
    apache::thrift::Cpp2RequestContext classic(&classicConnection);
    auto& connectionFields =
        classicConnection.getInternalFields<TestExtension::ConnState>();
    auto& requestFields =
        classic.getInternalFields<TestExtension::RequestState>();
    connectionFields.lifetime = std::make_shared<int>(1);
    requestFields.lifetime = std::make_shared<int>(2);
    classicConnectionLifetime = connectionFields.lifetime;
    classicRequestLifetime = requestFields.lifetime;
    const ThriftRequestContextView classicView(&classic);
    const auto classicCopy = classicView;
    EXPECT_EQ(classicCopy.tryState<TestExtension>(), &requestFields);
    EXPECT_EQ(
        classicCopy.connectionContext().tryState<TestExtension>(),
        &connectionFields);
    {
      ThriftRequestContext native;
      native.installExtensions(layout);
      auto& nativeFields = native.emplaceState<TestExtension>();
      nativeFields.lifetime = std::make_shared<int>(3);
      nativeRequestLifetime = nativeFields.lifetime;
      const ThriftRequestContextView nativeView(&native);
      const auto nativeCopy = nativeView;

      EXPECT_EQ(nativeCopy.tryState<TestExtension>(), &nativeFields);
      EXPECT_EQ(classicConnectionLifetime.use_count(), 1);
      EXPECT_EQ(classicRequestLifetime.use_count(), 1);
      EXPECT_EQ(nativeRequestLifetime.use_count(), 1);
    }
    EXPECT_TRUE(nativeRequestLifetime.expired());
    EXPECT_FALSE(classicConnectionLifetime.expired());
    EXPECT_FALSE(classicRequestLifetime.expired());
  }
  EXPECT_TRUE(classicConnectionLifetime.expired());
  EXPECT_TRUE(classicRequestLifetime.expired());
}

TEST(ThriftContextViewTest, ConnectionAccessUsesTheRequestsOwnConnection) {
  const folly::SocketAddress peer("127.0.0.1", 1234);
  apache::thrift::Cpp2ConnContext classicConnection(&peer);
  apache::thrift::Cpp2RequestContext classic(&classicConnection);
  boost::intrusive_ptr<ThriftConnContext> nativeConnection(
      new ThriftConnContext);
  nativeConnection->setPeerAddress(peer);
  ThriftRequestContext native;
  native.setConnectionContext(nativeConnection);

  const ThriftRequestContextView classicView(&classic);
  const ThriftRequestContextView nativeView(&native);
  ASSERT_NE(classicView.connectionContext().peerAddress(), nullptr);
  ASSERT_NE(nativeView.connectionContext().peerAddress(), nullptr);
  EXPECT_EQ(*classicView.connectionContext().peerAddress(), peer);
  EXPECT_EQ(*nativeView.connectionContext().peerAddress(), peer);
  EXPECT_EQ(
      classicView.peerAddress(), classicView.connectionContext().peerAddress());
  EXPECT_EQ(
      nativeView.peerAddress(), nativeView.connectionContext().peerAddress());
  EXPECT_EQ(
      classicView.requiredConnectionContext().peerAddress(),
      classicView.connectionContext().peerAddress());
  EXPECT_EQ(
      nativeView.requiredConnectionContext().peerAddress(),
      nativeView.connectionContext().peerAddress());

  EXPECT_FALSE(classicView.connectionContext().clientMetadata());
  EXPECT_EQ(nativeView.connectionContext().clientMetadata(), nullptr);
  apache::thrift::ClientMetadata metadata;
  apache::thrift::detail::Cpp2ConnContextInternalAPI(classicConnection)
      .setClientMetadata(metadata);
  nativeConnection->setClientMetadata(metadata);
  const auto classicMetadata = classicView.connectionContext().clientMetadata();
  const auto* nativeMetadata = nativeView.connectionContext().clientMetadata();
  ASSERT_TRUE(classicMetadata);
  ASSERT_NE(nativeMetadata, nullptr);
  EXPECT_FALSE(classicMetadata->getHostname());
  EXPECT_FALSE(nativeMetadata->hostname().has_value());
  EXPECT_FALSE(nativeView.connectionContext().clientHostname());

  metadata.hostname() = "client-host";
  apache::thrift::detail::Cpp2ConnContextInternalAPI(classicConnection)
      .setClientMetadata(metadata);
  nativeConnection->setClientMetadata(metadata);
  EXPECT_EQ(
      classicView.connectionContext().clientMetadata()->getHostname(),
      "client-host");
  EXPECT_EQ(
      nativeView.connectionContext().clientMetadata()->hostname(),
      "client-host");
  EXPECT_EQ(nativeView.connectionContext().clientHostname(), "client-host");

  metadata.hostname() = "updated-client-host";
  apache::thrift::detail::Cpp2ConnContextInternalAPI(classicConnection)
      .setClientMetadata(metadata);
  nativeConnection->setClientMetadata(metadata);
  EXPECT_EQ(
      classicView.connectionContext().clientHostname(), "updated-client-host");
  EXPECT_EQ(
      nativeView.connectionContext().clientHostname(), "updated-client-host");

  metadata.hostname().reset();
  apache::thrift::detail::Cpp2ConnContextInternalAPI(classicConnection)
      .setClientMetadata(metadata);
  nativeConnection->setClientMetadata(metadata);
  EXPECT_FALSE(classicView.connectionContext().clientHostname());
  EXPECT_FALSE(nativeView.connectionContext().clientHostname());
}

TEST(
    ThriftContextViewTest,
    DistinguishesMissingHeaderContainerFromEmptyHeaders) {
  apache::thrift::Cpp2RequestContext withoutHeader(nullptr);
  apache::thrift::transport::THeader header;
  apache::thrift::Cpp2RequestContext withHeader(nullptr, &header);
  ThriftRequestContext native;

  const ThriftRequestContextView missing(&withoutHeader);
  const ThriftRequestContextView empty(&withHeader);
  const ThriftRequestContextView nativeView(&native);

  EXPECT_TRUE(missing);
  EXPECT_FALSE(missing.hasHeaderContainer());
  EXPECT_EQ(missing.requestHeaders(), nullptr);
  EXPECT_FALSE(missing.hasHeaders());
  EXPECT_TRUE(empty.hasHeaderContainer());
  EXPECT_NE(empty.requestHeaders(), nullptr);
  EXPECT_FALSE(empty.hasHeaders());
  EXPECT_TRUE(nativeView.hasHeaderContainer());
  EXPECT_NE(nativeView.requestHeaders(), nullptr);
  EXPECT_FALSE(nativeView.hasHeaders());

  const auto missingHeader = missing.headerContainer();
  const auto emptyHeader = empty.headerContainer();
  const auto nativeHeader = nativeView.headerContainer();
  EXPECT_FALSE(missingHeader);
  EXPECT_TRUE(emptyHeader);
  EXPECT_TRUE(nativeHeader);
  stampTraceResponse(missingHeader);
  stampTraceResponse(emptyHeader);
  stampTraceResponse(nativeHeader);
  EXPECT_FALSE(missingHeader.setResponseHeader("trace", std::string("id")));
  EXPECT_TRUE(header.getWriteHeaders().empty());
  EXPECT_FALSE(native.hasResponseHeaders());
}

TEST(ThriftContextViewTest, ClassicViewsPreserveConstContextCapabilities) {
  apache::thrift::transport::THeader header;
  apache::thrift::Cpp2ConnContext connection;
  apache::thrift::Cpp2RequestContext context(&connection, &header);
  const ThriftRequestContextView view(std::as_const(context));
  const ThriftConnectionContextView connectionView(std::as_const(connection));
  auto& requestFields =
      context.getInternalFields<TestExtension::RequestState>();
  auto& connectionFields =
      connection.getInternalFields<TestExtension::ConnState>();

  EXPECT_EQ(view.tryState<TestExtension>(), &requestFields);
  EXPECT_EQ(view.tryMutableState<TestExtension>(), nullptr);
  EXPECT_EQ(connectionView.tryState<TestExtension>(), &connectionFields);
  EXPECT_EQ(connectionView.tryMutableState<TestExtension>(), nullptr);

  // Cpp2 constness is shallow: a const request exposes a mutable connection.
  auto* mutableConnection =
      view.connectionContext().tryMutableState<TestExtension>();
  ASSERT_EQ(mutableConnection, &connectionFields);
  EXPECT_EQ(
      view.requiredConnectionContext().tryMutableState<TestExtension>(),
      &connectionFields);
  mutableConnection->value = 42;
  EXPECT_EQ(connectionFields.value, 42);

  EXPECT_THROW(
      view.setResponseHeader("trace", std::string("id")), std::logic_error);
  const std::string id = "id";
  EXPECT_THROW(view.setResponseHeader("trace", id), std::logic_error);
  EXPECT_TRUE(header.getWriteHeaders().empty());

  const auto cachedHeader = view.headerContainer();
  EXPECT_TRUE(cachedHeader.setResponseHeader("trace", std::string("id")));
  EXPECT_TRUE(cachedHeader.setResponseHeader("trace", id));
  EXPECT_EQ(header.getWriteHeaders().at("trace"), id);
  header.setReadHeader("trace", "cached-id");
  stampTraceResponse(cachedHeader);
  EXPECT_EQ(header.getWriteHeaders().at("trace"), "cached-id");
  const ThriftRequestHeaderView<ThriftContextBackend::Classic> rawMutable(
      &header);
  header.setReadHeader("trace", "raw-id");
  stampTraceResponse(rawMutable);
  EXPECT_EQ(header.getWriteHeaders().at("trace"), "raw-id");

  const ThriftRequestHeaderView<ThriftContextBackend::Classic> rawConst(
      &std::as_const(header));
  EXPECT_THROW(stampTraceResponse(rawConst), std::logic_error);
  EXPECT_EQ(header.getWriteHeaders().at("trace"), "raw-id");
  const apache::thrift::transport::THeader immutableHeader;
  const ThriftRequestHeaderView<ThriftContextBackend::Classic> immutable(
      &immutableHeader);
  EXPECT_THROW(
      immutable.setResponseHeader("trace", std::string("id")),
      std::logic_error);
  EXPECT_TRUE(immutableHeader.getWriteHeaders().empty());
}

TEST(ThriftContextViewTest, EmptyViewsAllowInspectionAndRejectMutation) {
  const ThriftRequestContextView classic(
      static_cast<apache::thrift::Cpp2RequestContext*>(nullptr));
  const ThriftRequestContextView native(
      static_cast<ThriftRequestContext*>(nullptr));

  EXPECT_FALSE(classic);
  EXPECT_FALSE(native);
  EXPECT_EQ(classic.header("trace"), nullptr);
  EXPECT_EQ(native.header("trace"), nullptr);
  EXPECT_TRUE(classic.headers().empty());
  EXPECT_TRUE(native.headers().empty());
  EXPECT_FALSE(classic.connectionContext());
  EXPECT_FALSE(native.connectionContext());
  EXPECT_EQ(classic.tryState<TestExtension>(), nullptr);
  EXPECT_EQ(classic.tryMutableState<TestExtension>(), nullptr);
  EXPECT_EQ(classic.connectionContext().tryState<TestExtension>(), nullptr);
  EXPECT_EQ(
      classic.connectionContext().tryMutableState<TestExtension>(), nullptr);
  EXPECT_EQ(native.tryState<TestExtension>(), nullptr);
  EXPECT_EQ(native.tryMutableState<TestExtension>(), nullptr);
  EXPECT_EQ(native.connectionContext().tryState<TestExtension>(), nullptr);
  EXPECT_EQ(
      native.connectionContext().tryMutableState<TestExtension>(), nullptr);
  EXPECT_THROW(
      classic.setResponseHeader("trace", std::string("id")), std::logic_error);
  EXPECT_THROW(
      native.setResponseHeader("trace", std::string("id")), std::logic_error);

  const ThriftRequestHeaderView<ThriftContextBackend::Classic> defaultHeader;
  const auto classicHeader = classic.headerContainer();
  const auto nativeHeader = native.headerContainer();
  EXPECT_FALSE(defaultHeader);
  EXPECT_FALSE(classicHeader);
  EXPECT_FALSE(nativeHeader);
  stampTraceResponse(defaultHeader);
  stampTraceResponse(classicHeader);
  stampTraceResponse(nativeHeader);
}

} // namespace
} // namespace apache::thrift::fast_thrift::thrift::server::compatibility
