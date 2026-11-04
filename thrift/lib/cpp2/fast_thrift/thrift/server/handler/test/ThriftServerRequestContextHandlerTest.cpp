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

#include <thrift/lib/cpp2/fast_thrift/thrift/server/handler/ThriftServerRequestContextHandler.h>

#include <memory>
#include <set>
#include <vector>

#include <gtest/gtest.h>

#include <folly/ExceptionWrapper.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/Request.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/Messages.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/MethodMetadata.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/ThriftConnContext.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/ThriftRequestContext.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/extension/ThriftExtension.h>
#include <thrift/lib/thrift/gen-cpp2/RpcMetadata_types.h>

namespace apache::thrift::fast_thrift::thrift {

namespace {

using channel_pipeline::erase_and_box;
using channel_pipeline::Result;
using channel_pipeline::TypeErasedBox;

class FakeContext {
 public:
  folly::EventBase* eventBase() noexcept { return &eventBase_; }

  Result fireRead(TypeErasedBox&& msg) noexcept {
    forwardedRequestContexts.push_back(folly::RequestContext::saveContext());
    forwarded.push_back(std::move(msg));
    return Result::Success;
  }

  Result fireWrite(TypeErasedBox&& msg) noexcept {
    writtenRequestContexts.push_back(folly::RequestContext::saveContext());
    written.push_back(std::move(msg));
    return Result::Success;
  }

  void fireException(folly::exception_wrapper&& e) noexcept {
    exception = std::move(e);
  }

  folly::EventBase eventBase_;
  std::vector<TypeErasedBox> forwarded;
  std::vector<TypeErasedBox> written;
  std::vector<std::shared_ptr<folly::RequestContext>> forwardedRequestContexts;
  std::vector<std::shared_ptr<folly::RequestContext>> writtenRequestContexts;
  folly::exception_wrapper exception;
};

ThriftServerRequestMessage makeRequest(uint32_t streamId = 1) {
  ThriftServerRequestMessage req;
  req.streamId = streamId;
  return req;
}

// Request-response message carrying real metadata, as the transport adapter
// would hand it over.
ThriftServerRequestMessage makeRequestWithMetadata(
    folly::EventBase&,
    uint32_t streamId,
    std::unique_ptr<apache::thrift::RequestRpcMetadata> metadata) {
  ThriftServerRequestMessage req;
  req.streamId = streamId;
  req.payload =
      ThriftServerInboundPayloadVariant{ThriftServerRequestResponsePayload{
          .data = folly::IOBuf::copyBuffer("body"),
          .metadata = std::make_unique<apache::thrift::RequestRpcMetadata>(
              std::move(*metadata))}};
  return req;
}

ThriftServerRequestMessage makeStreamRequestWithMetadata(
    uint32_t streamId,
    std::unique_ptr<apache::thrift::RequestRpcMetadata> metadata) {
  ThriftServerRequestMessage req;
  req.streamId = streamId;
  req.payload = ThriftServerInboundPayloadVariant{ThriftRequestStreamPayload{
      .data = folly::IOBuf::copyBuffer("body"),
      .metadata = std::move(metadata),
      .initialRequestN = 1}};
  return req;
}

// Outbound message as the tail adapter emits it: typed metadata plus the
// originating context, whose response headers are still on the context.
ThriftServerResponseMessage makeResponseWithHeaders(
    ThriftRequestContextPtr requestContext) {
  ThriftServerResponseMessage resp;
  resp.requestContext = std::move(requestContext);
  resp.payload =
      ThriftServerOutboundPayloadVariant{ThriftInitialResponsePayload{
          .data = folly::IOBuf::copyBuffer("body"),
          .metadata = std::make_unique<apache::thrift::ResponseRpcMetadata>(),
          .streamId = 7}};
  return resp;
}

const apache::thrift::ResponseRpcMetadata& writtenMetadata(
    const TypeErasedBox& box) {
  return *box.get<ThriftServerResponseMessage>()
              .payload.get<ThriftInitialResponsePayload>()
              .metadata;
}

} // namespace

// The context knows which method the request named, copied off the metadata
// because the context outlives the payload that carried it.
TEST(ThriftServerRequestContextHandlerTest, StampsTheMethodName) {
  ThriftServerRequestContextHandler<FakeContext> handler{nullptr};
  FakeContext ctx;

  auto metadata = std::make_unique<apache::thrift::RequestRpcMetadata>();
  metadata->name() = "Service.method";
  auto req = makeRequestWithMetadata(
      *ctx.eventBase(), /*streamId=*/1, std::move(metadata));

  EXPECT_EQ(
      handler.onRead(ctx, erase_and_box(std::move(req))), Result::Success);

  ASSERT_EQ(ctx.forwarded.size(), 1);
  auto& forwarded = ctx.forwarded.front().get<ThriftServerRequestMessage>();
  ASSERT_NE(forwarded.requestContext, nullptr);
  EXPECT_EQ(forwarded.requestContext->getMethodName(), "Service.method");
}

TEST(ThriftServerRequestContextHandlerTest, ResolvesMethodMetadataOnce) {
  ThriftServerMethodMetadataRegistry methods;
  methods.add(
      ThriftServerMethodMetadata{
          .serviceName = "Service",
          .definingServiceName = "Service",
          .methodName = "method",
          .qualifiedMethodName = "Service.method",
      });
  ThriftServerRequestContextHandler<FakeContext> handler{nullptr, &methods};
  FakeContext ctx;

  auto metadata = std::make_unique<apache::thrift::RequestRpcMetadata>();
  metadata->name() = "method";
  auto req = makeRequestWithMetadata(
      *ctx.eventBase(), /*streamId=*/1, std::move(metadata));

  EXPECT_EQ(
      handler.onRead(ctx, erase_and_box(std::move(req))), Result::Success);

  const auto& forwarded =
      ctx.forwarded.front().get<ThriftServerRequestMessage>();
  const auto* resolved = forwarded.requestContext->getMethodMetadata();
  ASSERT_NE(resolved, nullptr);
  EXPECT_EQ(resolved->qualifiedMethodName, "Service.method");
  EXPECT_EQ(resolved, methods.find("method"));
  const ThriftRequestView view(forwarded);
  EXPECT_EQ(view.methodName(), "method");
  EXPECT_EQ(view.qualifiedMethodName(), "Service.method");
}

TEST(
    ThriftServerRequestContextHandlerTest,
    StreamRequestsPopulateResolvedMethodAndHeaders) {
  ThriftServerMethodMetadataRegistry methods;
  methods.add(
      ThriftServerMethodMetadata{
          .serviceName = "Service",
          .definingServiceName = "Service",
          .methodName = "stream",
          .qualifiedMethodName = "Service.stream",
      });
  ThriftServerRequestContextHandler<FakeContext> handler{
      nullptr,
      &methods,
      boost::intrusive_ptr<ThriftConnContext>{new ThriftConnContext()},
      true};
  FakeContext ctx;
  auto metadata = std::make_unique<apache::thrift::RequestRpcMetadata>();
  metadata->name() = "stream";
  metadata->otherMetadata().ensure()["request-header"] = "value";

  EXPECT_EQ(
      handler.onRead(
          ctx,
          erase_and_box(makeStreamRequestWithMetadata(
              /*streamId=*/1, std::move(metadata)))),
      Result::Success);

  const auto& request = ctx.forwarded.front().get<ThriftServerRequestMessage>();
  ASSERT_NE(request.requestContext, nullptr);
  EXPECT_EQ(request.requestContext->getMethodName(), "stream");
  EXPECT_EQ(
      request.requestContext->getMethodMetadata(), methods.find("stream"));
  ASSERT_NE(request.requestContext->getHeader("request-header"), nullptr);
  EXPECT_EQ(*request.requestContext->getHeader("request-header"), "value");
}

TEST(
    ThriftServerRequestContextHandlerTest,
    StreamControlFramesReuseTheOpenStreamsContext) {
  ThriftServerRequestContextHandler<FakeContext> handler{nullptr};
  FakeContext ctx;
  folly::RequestContextScopeGuard callerGuard;
  auto* const callerContext = folly::RequestContext::try_get();
  auto request = makeRequest(/*streamId=*/1);
  request.payload = ThriftRequestNPayload{.streamId = 1, .requestN = 10};

  EXPECT_EQ(
      handler.onRead(ctx, erase_and_box(std::move(request))), Result::Success);

  ASSERT_EQ(ctx.forwarded.size(), 1u);
  EXPECT_EQ(
      ctx.forwarded.front().get<ThriftServerRequestMessage>().requestContext,
      nullptr);
  ASSERT_EQ(ctx.forwardedRequestContexts.size(), 1u);
  EXPECT_EQ(ctx.forwardedRequestContexts.front().get(), callerContext);
  EXPECT_EQ(folly::RequestContext::try_get(), callerContext);
}

// A request whose metadata names no method leaves the context without one,
// rather than an empty name that reads as if it had been set.
TEST(ThriftServerRequestContextHandlerTest, NoMethodNameLeavesItEmpty) {
  ThriftServerRequestContextHandler<FakeContext> handler{nullptr};
  FakeContext ctx;

  auto req = makeRequestWithMetadata(
      *ctx.eventBase(),
      /*streamId=*/1,
      std::make_unique<apache::thrift::RequestRpcMetadata>());

  EXPECT_EQ(
      handler.onRead(ctx, erase_and_box(std::move(req))), Result::Success);

  ASSERT_EQ(ctx.forwarded.size(), 1);
  auto& forwarded = ctx.forwarded.front().get<ThriftServerRequestMessage>();
  ASSERT_NE(forwarded.requestContext, nullptr);
  EXPECT_TRUE(forwarded.requestContext->getMethodName().empty());
}

TEST(
    ThriftServerRequestContextHandlerTest,
    StampsDefaultRequestContextOnInboundMessage) {
  ThriftServerRequestContextHandler<FakeContext> handler{nullptr};
  FakeContext ctx;

  EXPECT_EQ(
      handler.onRead(ctx, erase_and_box(makeRequest(/*sid=*/42))),
      Result::Success);

  ASSERT_EQ(ctx.forwarded.size(), 1);
  auto& forwarded = ctx.forwarded.front().get<ThriftServerRequestMessage>();
  EXPECT_EQ(forwarded.streamId, 42);
  ASSERT_NE(forwarded.requestContext, nullptr);
  // The one-argument constructor remains useful where no connection exists.
  EXPECT_EQ(forwarded.requestContext->getConnectionContext(), nullptr);
}

TEST(ThriftServerRequestContextHandlerTest, PopulatesConnectionAndHeaders) {
  boost::intrusive_ptr<ThriftConnContext> connContext{new ThriftConnContext()};
  connContext->setSecurityProtocol("TLS1.3");
  ThriftServerRequestContextHandler<FakeContext> handler{
      nullptr, nullptr, connContext, true};
  FakeContext ctx;

  auto metadata = std::make_unique<apache::thrift::RequestRpcMetadata>();
  auto& headers = metadata->otherMetadata().ensure();
  headers["kcb_identity"] = "svc:foo";
  headers["client_identifier"] = "client-123";

  EXPECT_EQ(
      handler.onRead(
          ctx,
          erase_and_box(makeRequestWithMetadata(
              *ctx.eventBase(), /*streamId=*/42, std::move(metadata)))),
      Result::Success);

  ASSERT_EQ(ctx.forwarded.size(), 1);
  auto& forwarded = ctx.forwarded.front().get<ThriftServerRequestMessage>();
  ASSERT_NE(forwarded.requestContext, nullptr);
  EXPECT_EQ(
      forwarded.requestContext->getConnectionContext(), connContext.get());
  EXPECT_EQ(forwarded.requestContext->getHeaders().size(), 2);
  ASSERT_NE(forwarded.requestContext->getHeader("kcb_identity"), nullptr);
  EXPECT_EQ(*forwarded.requestContext->getHeader("kcb_identity"), "svc:foo");
}

TEST(ThriftServerRequestContextHandlerTest, DisabledHeadersStayInMetadata) {
  ThriftServerRequestContextHandler<FakeContext> handler{
      nullptr,
      nullptr,
      boost::intrusive_ptr<ThriftConnContext>{new ThriftConnContext()},
      false};
  FakeContext ctx;

  auto metadata = std::make_unique<apache::thrift::RequestRpcMetadata>();
  metadata->otherMetadata().ensure()["present"] = "yes";

  EXPECT_EQ(
      handler.onRead(
          ctx,
          erase_and_box(makeRequestWithMetadata(
              *ctx.eventBase(), /*streamId=*/1, std::move(metadata)))),
      Result::Success);

  ASSERT_EQ(ctx.forwarded.size(), 1);
  auto& forwarded = ctx.forwarded.front().get<ThriftServerRequestMessage>();
  EXPECT_TRUE(forwarded.requestContext->getHeaders().empty());
  const auto* forwardedMetadata = forwarded.payload.getRequestRpcMetadata();
  ASSERT_NE(forwardedMetadata, nullptr);
  ASSERT_TRUE(forwardedMetadata->otherMetadata().has_value());
  EXPECT_EQ(forwardedMetadata->otherMetadata()->at("present"), "yes");
}

TEST(ThriftServerRequestContextHandlerTest, EachRequestGetsItsOwnContext) {
  ThriftServerRequestContextHandler<FakeContext> handler{nullptr};
  FakeContext ctx;
  folly::RequestContextScopeGuard callerGuard;
  auto* const callerContext = folly::RequestContext::try_get();

  for (uint32_t sid = 1; sid <= 3; ++sid) {
    EXPECT_EQ(
        handler.onRead(ctx, erase_and_box(makeRequest(sid))), Result::Success);
    EXPECT_EQ(folly::RequestContext::try_get(), callerContext);
  }

  std::set<ThriftRequestContext*> distinct;
  std::set<folly::RequestContext*> distinctFollyContexts;
  ASSERT_EQ(ctx.forwarded.size(), ctx.forwardedRequestContexts.size());
  for (size_t i = 0; i < ctx.forwarded.size(); ++i) {
    auto& box = ctx.forwarded[i];
    auto& m = box.get<ThriftServerRequestMessage>();
    ASSERT_NE(m.requestContext, nullptr);
    distinct.insert(m.requestContext.get());
    ASSERT_NE(ctx.forwardedRequestContexts[i], nullptr);
    EXPECT_EQ(
        ctx.forwardedRequestContexts[i],
        m.requestContext->getFollyRequestContext().capture());
    EXPECT_NE(ctx.forwardedRequestContexts[i].get(), callerContext);
    distinctFollyContexts.insert(ctx.forwardedRequestContexts[i].get());
  }
  EXPECT_EQ(distinct.size(), 3) << "each request must get its own context";
  EXPECT_EQ(distinctFollyContexts.size(), 3)
      << "each request must get its own folly context";
}

TEST(
    ThriftServerRequestContextHandlerTest, StampsMethodNameOntoRequestContext) {
  auto metadata = std::make_unique<apache::thrift::RequestRpcMetadata>();
  metadata->name() = "Service.method";

  ThriftServerRequestContextHandler<FakeContext> handler{nullptr};
  FakeContext ctx;

  EXPECT_EQ(
      handler.onRead(
          ctx,
          erase_and_box(makeRequestWithMetadata(
              *ctx.eventBase(), /*streamId=*/7, std::move(metadata)))),
      Result::Success);

  ASSERT_EQ(ctx.forwarded.size(), 1);
  auto& forwarded = ctx.forwarded.front().get<ThriftServerRequestMessage>();
  ASSERT_NE(forwarded.requestContext, nullptr);
  EXPECT_EQ(forwarded.requestContext->getMethodName(), "Service.method");
}

// Routing reads the context-owned name after this handler, so the metadata no
// longer retains a second allocation.
TEST(ThriftServerRequestContextHandlerTest, MovesMethodNameOutOfMetadata) {
  auto metadata = std::make_unique<apache::thrift::RequestRpcMetadata>();
  metadata->name() = "Service.method";

  ThriftServerRequestContextHandler<FakeContext> handler{nullptr};
  FakeContext ctx;

  EXPECT_EQ(
      handler.onRead(
          ctx,
          erase_and_box(makeRequestWithMetadata(
              *ctx.eventBase(), /*streamId=*/7, std::move(metadata)))),
      Result::Success);

  ASSERT_EQ(ctx.forwarded.size(), 1);
  auto& forwarded = ctx.forwarded.front().get<ThriftServerRequestMessage>();
  const auto* stamped = forwarded.payload.getRequestRpcMetadata();
  ASSERT_NE(stamped, nullptr);
  ASSERT_TRUE(stamped->name().has_value());
  EXPECT_TRUE(stamped->name()->view().empty());
}

// A request whose metadata carries no name still gets a context; the name is
// simply empty.
TEST(
    ThriftServerRequestContextHandlerTest, MethodNameEmptyWhenMetadataHasNone) {
  ThriftServerRequestContextHandler<FakeContext> handler{nullptr};
  FakeContext ctx;

  EXPECT_EQ(
      handler.onRead(
          ctx,
          erase_and_box(makeRequestWithMetadata(
              *ctx.eventBase(),
              /*streamId=*/7,
              std::make_unique<apache::thrift::RequestRpcMetadata>()))),
      Result::Success);

  ASSERT_EQ(ctx.forwarded.size(), 1);
  auto& forwarded = ctx.forwarded.front().get<ThriftServerRequestMessage>();
  ASSERT_NE(forwarded.requestContext, nullptr);
  EXPECT_TRUE(forwarded.requestContext->getMethodName().empty());
}

// The closing half of the handler's job: response headers accumulated on the
// context reach the outgoing metadata on the way out. Everything upstream of
// here writes them to the context and nowhere else.
TEST(ThriftServerRequestContextHandlerTest, DrainsResponseHeadersOnWrite) {
  ThriftServerRequestContextHandler<FakeContext> handler{nullptr};
  FakeContext ctx;
  folly::RequestContextScopeGuard callerGuard;
  auto* const callerContext = folly::RequestContext::try_get();
  auto requestContext = makeThriftRequestContext(*ctx.eventBase());
  const auto requestFollyContext =
      requestContext->getFollyRequestContext().capture();
  requestContext->setResponseHeader("shard", "42");

  EXPECT_EQ(
      handler.onWrite(
          ctx,
          erase_and_box(makeResponseWithHeaders(std::move(requestContext)))),
      Result::Success);
  EXPECT_EQ(folly::RequestContext::try_get(), callerContext);

  ASSERT_EQ(ctx.written.size(), 1);
  ASSERT_EQ(ctx.writtenRequestContexts.size(), 1);
  EXPECT_EQ(ctx.writtenRequestContexts.front(), requestFollyContext);
  const ThriftRequestContext::HeaderMap expected{{"shard", "42"}};
  const auto& metadata = writtenMetadata(ctx.written.front());
  ASSERT_TRUE(metadata.otherMetadata().has_value());
  EXPECT_EQ(*metadata.otherMetadata(), expected);
}

// A response created before request-context construction still goes out
// untouched.
TEST(ThriftServerRequestContextHandlerTest, WriteWithoutContextIsPassThrough) {
  ThriftServerRequestContextHandler<FakeContext> handler{nullptr};
  FakeContext ctx;

  EXPECT_EQ(
      handler.onWrite(ctx, erase_and_box(makeResponseWithHeaders({}))),
      Result::Success);

  ASSERT_EQ(ctx.written.size(), 1);
  EXPECT_FALSE(
      writtenMetadata(ctx.written.front()).otherMetadata().has_value());
}

TEST(ThriftServerRequestContextHandlerTest, ForwardsExceptions) {
  ThriftServerRequestContextHandler<FakeContext> handler{nullptr};
  FakeContext ctx;

  handler.onException(
      ctx, folly::make_exception_wrapper<std::runtime_error>("boom"));

  ASSERT_TRUE(ctx.exception);
  EXPECT_NE(ctx.exception.what().toStdString().find("boom"), std::string::npos);
}

} // namespace apache::thrift::fast_thrift::thrift
