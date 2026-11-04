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

#include <gtest/gtest.h>

#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/ThriftRequestContext.h>

namespace apache::thrift::fast_thrift::thrift::server::compatibility {
namespace {

template <class View>
void stampTraceResponse(const View& request) {
  if (const auto* trace = request.header("trace")) {
    request.setResponseHeader("trace", *trace);
    request.setResponseHeader("status", std::string("traced"));
  }
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

TEST(ThriftContextViewTest, SharesHeaderLogicAcrossClassicAndNativeContexts) {
  apache::thrift::transport::THeader header;
  header.setReadHeader("trace", "trace-id");
  header.setReadHeader("request-only", "inbound");
  apache::thrift::Cpp2RequestContext classic(
      nullptr, &header, "Service.method");

  ThriftRequestContext native;
  native.setMethodName("method");
  ThriftServerMethodMetadata method;
  method.qualifiedMethodName = "Service.method";
  native.setMethodMetadata(&method);
  native.setHeaders(header.getHeaders());

  const ThriftRequestContextView classicView(&classic);
  const ThriftRequestContextView nativeView(&native);

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
  const auto* nativeMetadata =
      ThriftRequestContextView(&native).connectionContext().clientMetadata();
  ASSERT_TRUE(classicMetadata);
  ASSERT_NE(nativeMetadata, nullptr);
  EXPECT_FALSE(classicMetadata->getHostname());
  EXPECT_FALSE(nativeMetadata->hostname().has_value());

  metadata.hostname() = "client-host";
  apache::thrift::detail::Cpp2ConnContextInternalAPI(classicConnection)
      .setClientMetadata(metadata);
  nativeConnection->setClientMetadata(metadata);
  EXPECT_EQ(
      classicView.connectionContext().clientMetadata()->getHostname(),
      "client-host");
  EXPECT_EQ(
      ThriftRequestContextView(&native)
          .connectionContext()
          .clientMetadata()
          ->hostname(),
      "client-host");
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
}

TEST(ThriftContextViewTest, ConstClassicContextRejectsResponseHeaderMutation) {
  apache::thrift::transport::THeader header;
  apache::thrift::Cpp2RequestContext context(nullptr, &header);
  const ThriftRequestContextView view(std::as_const(context));

  EXPECT_THROW(
      view.setResponseHeader("trace", std::string("id")), std::logic_error);
  const std::string id = "id";
  EXPECT_THROW(view.setResponseHeader("trace", id), std::logic_error);
  EXPECT_TRUE(header.getWriteHeaders().empty());
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
  EXPECT_THROW(
      classic.setResponseHeader("trace", std::string("id")), std::logic_error);
  EXPECT_THROW(
      native.setResponseHeader("trace", std::string("id")), std::logic_error);
}

} // namespace
} // namespace apache::thrift::fast_thrift::thrift::server::compatibility
