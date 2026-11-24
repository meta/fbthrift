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

#include <array>
#include <functional>
#include <memory>
#include <set>
#include <vector>

#include <gtest/gtest.h>

#include <folly/ExceptionWrapper.h>
#include <folly/ScopeGuard.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/Request.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/HandlerTag.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/StaticPipelineBuilder.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/test/MockAdapters.h>
#include <thrift/lib/cpp2/fast_thrift/frame/ErrorCode.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/adapter/ThriftServerAppAdapter.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/adapter/util/ThriftServerMethodDispatchTable.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/ConnectionPayloads.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/ExtensionStateStore.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/Messages.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/MethodMetadata.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/ThriftConnContext.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/ThriftRequestContext.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/extension/ThriftExtension.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/extension/ThriftExtensionPipelineHandler.h>
#include <thrift/lib/cpp2/protocol/CompactProtocol.h>
#include <thrift/lib/thrift/gen-cpp2/RpcMetadata_types.h>

namespace apache::thrift::fast_thrift::thrift {

namespace {

using channel_pipeline::erase_and_box;
using channel_pipeline::Result;
using channel_pipeline::TypeErasedBox;

HANDLER_TAG(request_context);
HANDLER_TAG(request_probe);

struct RequestProbe {
  explicit RequestProbe(int& calls) : calls(calls) {}

  RequestVerdict onRequest(const ThriftRequestView&) noexcept {
    ++calls;
    return RequestVerdict::proceed();
  }

  int& calls;
};

template <typename Context>
using RequestProbePipelineHandler =
    server::ThriftExtensionPipelineHandler<RequestProbe, Context>;

class CountingAppAdapter : public ThriftServerAppAdapter {
 public:
  using Ptr = std::unique_ptr<CountingAppAdapter, Destructor>;

  CountingAppAdapter()
      : ThriftServerAppAdapter(
            std::make_shared<ThriftServerMethodDispatchTable>(
                std::vector<ThriftServerMethodDispatchTable::Method>{
                    {"known", processRequest}, {"method", processRequest}})) {}

  int calls{0};

 private:
  static Result processRequest(
      ThriftServerAppAdapter* adapter, TypeErasedBox&&) noexcept {
    ++static_cast<CountingAppAdapter*>(adapter)->calls;
    return Result::Success;
  }
};

constexpr std::array kInitialRpcKinds{
    apache::thrift::RpcKind::SINGLE_REQUEST_SINGLE_RESPONSE,
    apache::thrift::RpcKind::SINGLE_REQUEST_STREAMING_RESPONSE};

std::unique_ptr<apache::thrift::RequestRpcMetadata> makeMetadata(
    apache::thrift::RpcKind kind) {
  auto metadata = std::make_unique<apache::thrift::RequestRpcMetadata>();
  metadata->protocol() = apache::thrift::ProtocolId::COMPACT;
  metadata->name() = "method";
  metadata->kind() = kind;
  return metadata;
}

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

ThriftServerRequestMessage makeRequest(uint32_t streamId) {
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
          .metadata = std::move(metadata)}};
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

void expectMetadataRejection(
    const FakeContext& ctx,
    uint32_t streamId,
    const ThriftConnContext* connection,
    const folly::RequestContext* callerContext) {
  EXPECT_TRUE(ctx.forwarded.empty());
  ASSERT_EQ(ctx.written.size(), 1u);
  const auto& response = ctx.written.front().get<ThriftServerResponseMessage>();
  ASSERT_NE(response.requestContext, nullptr);
  EXPECT_EQ(response.requestContext->getConnectionContext(), connection);
  EXPECT_TRUE(response.requestContext->getMethodName().empty());
  EXPECT_EQ(response.requestContext->getMethodMetadata(), nullptr);
  EXPECT_TRUE(response.requestContext->getHeaders().empty());
  ASSERT_EQ(ctx.writtenRequestContexts.size(), 1u);
  EXPECT_EQ(
      ctx.writtenRequestContexts.front(),
      response.requestContext->getFollyRequestContext().capture());
  EXPECT_NE(ctx.writtenRequestContexts.front().get(), callerContext);
  EXPECT_EQ(folly::RequestContext::try_get(), callerContext);

  ASSERT_TRUE(response.payload.is<ThriftErrorPayload>());
  const auto& payload = response.payload.get<ThriftErrorPayload>();
  EXPECT_EQ(payload.streamId, streamId);
  EXPECT_EQ(
      payload.errorCode,
      static_cast<uint32_t>(
          apache::thrift::fast_thrift::frame::ErrorCode::INVALID));
  ASSERT_NE(payload.data, nullptr);
  apache::thrift::CompactProtocolReader reader;
  reader.setInput(payload.data.get());
  apache::thrift::ResponseRpcError error;
  error.read(&reader);
  ASSERT_TRUE(error.code().has_value());
  EXPECT_EQ(
      *error.code(),
      apache::thrift::ResponseRpcErrorCode::REQUEST_PARSING_FAILURE);
  ASSERT_TRUE(error.category().has_value());
  EXPECT_EQ(
      *error.category(),
      apache::thrift::ResponseRpcErrorCategory::INVALID_REQUEST);
  ASSERT_TRUE(error.what_utf8().has_value());
  EXPECT_EQ(*error.what_utf8(), "Invalid metadata object");
}

} // namespace

// The context knows which method the request named, copied off the metadata
// because the context outlives the payload that carried it.
TEST(ThriftServerRequestContextHandlerTest, StampsTheMethodName) {
  ThriftServerRequestContextHandler<FakeContext> handler{nullptr};
  FakeContext ctx;

  auto metadata =
      makeMetadata(apache::thrift::RpcKind::SINGLE_REQUEST_SINGLE_RESPONSE);
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

  auto metadata =
      makeMetadata(apache::thrift::RpcKind::SINGLE_REQUEST_SINGLE_RESPONSE);
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
  auto metadata =
      makeMetadata(apache::thrift::RpcKind::SINGLE_REQUEST_STREAMING_RESPONSE);
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
    MissingRegistryMethodRejectsBeforeExtensionsAndApplication) {
  ThriftServerMethodMetadataRegistry methods;
  methods.add(
      ThriftServerMethodMetadata{
          .serviceName = "Service",
          .definingServiceName = "Service",
          .methodName = "known",
          .qualifiedMethodName = "Service.known"});

  for (const auto kind : kInitialRpcKinds) {
    SCOPED_TRACE(static_cast<int>(kind));
    FakeContext ctx;
    channel_pipeline::test::StaticMockHeadHandler head;
    head.setOnWriteCallback(
        [&](TypeErasedBox&& msg) { return ctx.fireWrite(std::move(msg)); });
    CountingAppAdapter::Ptr adapter{new CountingAppAdapter()};
    ExtensionStateStore store;
    int extensionCalls = 0;
    boost::intrusive_ptr<ThriftConnContext> connection{new ThriftConnContext()};
    auto pipeline =
        channel_pipeline::StaticPipelineBuilder<
            channel_pipeline::test::StaticMockHeadHandler,
            CountingAppAdapter>()
            .setEventBase(ctx.eventBase())
            .setHead(&head)
            .setTail(adapter.get())
            .emplaceAllocator()
            .addNextDuplexTemplate<ThriftServerRequestContextHandler>(
                request_context_tag, nullptr, &methods, connection, true)
            .addNextInboundTemplate<RequestProbePipelineHandler>(
                request_probe_tag, std::ref(store), std::ref(extensionCalls))
            .build();
    adapter->setPipeline(pipeline.get());
    auto resetPipeline = folly::makeGuard([&] { adapter->resetPipeline(); });
    pipeline->activate();
    folly::RequestContextScopeGuard callerGuard;
    auto* const callerContext = folly::RequestContext::try_get();
    auto metadata = makeMetadata(kind);
    metadata->otherMetadata().ensure()["request-header"] = "value";
    auto request = kind == kInitialRpcKinds.front()
        ? makeRequestWithMetadata(
              *ctx.eventBase(), /*streamId=*/1, std::move(metadata))
        : makeStreamRequestWithMetadata(/*streamId=*/1, std::move(metadata));

    EXPECT_EQ(
        pipeline->fireRead(erase_and_box(std::move(request))), Result::Success);

    EXPECT_EQ(extensionCalls, 0);
    EXPECT_EQ(adapter->calls, 0);
    ASSERT_EQ(ctx.written.size(), 1u);
    const auto& response =
        ctx.written.front().get<ThriftServerResponseMessage>();
    ASSERT_NE(response.requestContext, nullptr);
    EXPECT_EQ(
        response.requestContext->getConnectionContext(), connection.get());
    EXPECT_EQ(response.requestContext->getMethodName(), "method");
    EXPECT_EQ(response.requestContext->getMethodMetadata(), nullptr);
    ASSERT_NE(response.requestContext->getHeader("request-header"), nullptr);
    EXPECT_EQ(*response.requestContext->getHeader("request-header"), "value");
    ASSERT_EQ(ctx.writtenRequestContexts.size(), 1u);
    EXPECT_EQ(
        ctx.writtenRequestContexts.front(),
        response.requestContext->getFollyRequestContext().capture());
    EXPECT_EQ(folly::RequestContext::try_get(), callerContext);
    ASSERT_TRUE(response.payload.is<ThriftErrorPayload>());
    const auto& payload = response.payload.get<ThriftErrorPayload>();
    EXPECT_EQ(payload.streamId, 1u);
    ASSERT_NE(payload.data, nullptr);
    apache::thrift::CompactProtocolReader reader;
    reader.setInput(payload.data.get());
    apache::thrift::ResponseRpcError error;
    error.read(&reader);
    ASSERT_TRUE(error.code().has_value());
    EXPECT_EQ(
        *error.code(), apache::thrift::ResponseRpcErrorCode::UNKNOWN_METHOD);
    ASSERT_TRUE(error.what_utf8().has_value());
    EXPECT_EQ(*error.what_utf8(), "Unknown method: method");

    auto knownMetadata = makeMetadata(kind);
    knownMetadata->name() = "known";
    auto knownRequest = kind == kInitialRpcKinds.front()
        ? makeRequestWithMetadata(
              *ctx.eventBase(), /*streamId=*/3, std::move(knownMetadata))
        : makeStreamRequestWithMetadata(
              /*streamId=*/3, std::move(knownMetadata));

    EXPECT_EQ(
        pipeline->fireRead(erase_and_box(std::move(knownRequest))),
        Result::Success);

    EXPECT_EQ(extensionCalls, 1);
    EXPECT_EQ(adapter->calls, 1);
    EXPECT_EQ(ctx.written.size(), 1u);
    EXPECT_EQ(folly::RequestContext::try_get(), callerContext);
  }
}

TEST(
    ThriftServerRequestContextHandlerTest,
    SetupForwardsConnectionWithoutRequestContext) {
  boost::intrusive_ptr<ThriftConnContext> connection{new ThriftConnContext()};
  ThriftServerRequestContextHandler<FakeContext> handler{
      nullptr, nullptr, connection, true};
  FakeContext ctx;
  folly::RequestContextScopeGuard callerGuard;
  auto* const callerContext = folly::RequestContext::try_get();
  auto request = makeRequest(/*streamId=*/0);
  request.payload = ThriftConnectionSetupPayload{
      .setup = std::make_unique<ConnectionSetupData>()};

  EXPECT_EQ(
      handler.onRead(ctx, erase_and_box(std::move(request))), Result::Success);

  ASSERT_EQ(ctx.forwarded.size(), 1u);
  const auto& forwarded =
      ctx.forwarded.front().get<ThriftServerRequestMessage>();
  EXPECT_EQ(forwarded.requestContext, nullptr);
  EXPECT_EQ(
      forwarded.payload.get<ThriftConnectionSetupPayload>().setup->connContext,
      connection.get());
  EXPECT_TRUE(ctx.written.empty());
  ASSERT_EQ(ctx.forwardedRequestContexts.size(), 1u);
  EXPECT_EQ(ctx.forwardedRequestContexts.front().get(), callerContext);
  EXPECT_EQ(folly::RequestContext::try_get(), callerContext);
}

TEST(
    ThriftServerRequestContextHandlerTest,
    StreamControlFramesReuseTheOpenStreamsContext) {
  ThriftServerRequestContextHandler<FakeContext> handler{nullptr};
  FakeContext ctx;
  folly::RequestContextScopeGuard callerGuard;
  auto* const callerContext = folly::RequestContext::try_get();
  std::array payloads{
      ThriftServerInboundPayloadVariant{
          ThriftRequestNPayload{.streamId = 1, .requestN = 10}},
      ThriftServerInboundPayloadVariant{ThriftRequestCancellationPayload{}}};

  for (auto& payload : payloads) {
    auto request = makeRequest(/*streamId=*/1);
    request.payload = std::move(payload);
    EXPECT_EQ(
        handler.onRead(ctx, erase_and_box(std::move(request))),
        Result::Success);
  }

  ASSERT_EQ(ctx.forwarded.size(), 2u);
  ASSERT_EQ(ctx.forwardedRequestContexts.size(), 2u);
  for (size_t i = 0; i < ctx.forwarded.size(); ++i) {
    EXPECT_EQ(
        ctx.forwarded[i].get<ThriftServerRequestMessage>().requestContext,
        nullptr);
    EXPECT_EQ(ctx.forwardedRequestContexts[i].get(), callerContext);
  }
  EXPECT_TRUE(ctx.written.empty());
  EXPECT_EQ(folly::RequestContext::try_get(), callerContext);
}

TEST(
    ThriftServerRequestContextHandlerTest,
    InvalidMetadataRejectsBeforeDispatch) {
  enum class Failure { MissingProtocol, MissingName, MissingKind, WrongKind };
  constexpr std::array failures{
      Failure::MissingProtocol,
      Failure::MissingName,
      Failure::MissingKind,
      Failure::WrongKind};
  ThriftServerMethodMetadataRegistry methods;
  methods.add(
      ThriftServerMethodMetadata{
          .serviceName = "Service",
          .definingServiceName = "Service",
          .methodName = "method",
          .qualifiedMethodName = "Service.method"});
  boost::intrusive_ptr<ThriftConnContext> connection{new ThriftConnContext()};

  for (const auto kind : kInitialRpcKinds) {
    SCOPED_TRACE(static_cast<int>(kind));
    for (const auto failure : failures) {
      SCOPED_TRACE(static_cast<int>(failure));
      FakeContext ctx;
      ThriftServerRequestContextHandler<FakeContext> handler{
          nullptr, &methods, connection, true};
      auto metadata = makeMetadata(kind);
      metadata->otherMetadata().ensure()["request-header"] = "value";
      switch (failure) {
        case Failure::MissingProtocol:
          metadata->protocol().reset();
          break;
        case Failure::MissingName:
          metadata->name().reset();
          break;
        case Failure::MissingKind:
          metadata->kind().reset();
          break;
        case Failure::WrongKind:
          metadata->kind() = kind == kInitialRpcKinds.front()
              ? kInitialRpcKinds.back()
              : kInitialRpcKinds.front();
          break;
      }
      auto request = kind == kInitialRpcKinds.front()
          ? makeRequestWithMetadata(
                *ctx.eventBase(), /*streamId=*/1, std::move(metadata))
          : makeStreamRequestWithMetadata(
                /*streamId=*/1, std::move(metadata));
      folly::RequestContextScopeGuard callerGuard;
      auto* const callerContext = folly::RequestContext::try_get();

      EXPECT_EQ(
          handler.onRead(ctx, erase_and_box(std::move(request))),
          Result::Success);
      expectMetadataRejection(ctx, 1, connection.get(), callerContext);
    }
  }
}

TEST(
    ThriftServerRequestContextHandlerTest,
    MetadataValidationAcceptsPresentProtocolAndName) {
  for (const auto kind : kInitialRpcKinds) {
    SCOPED_TRACE(static_cast<int>(kind));
    FakeContext ctx;
    ThriftServerRequestContextHandler<FakeContext> handler{nullptr};
    auto metadata = makeMetadata(kind);
    metadata->protocol() = static_cast<apache::thrift::ProtocolId>(123);
    metadata->name() = "";
    auto request = kind == kInitialRpcKinds.front()
        ? makeRequestWithMetadata(
              *ctx.eventBase(), /*streamId=*/1, std::move(metadata))
        : makeStreamRequestWithMetadata(/*streamId=*/1, std::move(metadata));

    EXPECT_EQ(
        handler.onRead(ctx, erase_and_box(std::move(request))),
        Result::Success);

    ASSERT_EQ(ctx.forwarded.size(), 1u);
    const auto& forwarded =
        ctx.forwarded.front().get<ThriftServerRequestMessage>();
    ASSERT_NE(forwarded.requestContext, nullptr);
    EXPECT_TRUE(forwarded.requestContext->getMethodName().empty());
    EXPECT_TRUE(ctx.written.empty());
  }
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

  auto metadata =
      makeMetadata(apache::thrift::RpcKind::SINGLE_REQUEST_SINGLE_RESPONSE);
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

  auto metadata =
      makeMetadata(apache::thrift::RpcKind::SINGLE_REQUEST_SINGLE_RESPONSE);
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
  auto metadata =
      makeMetadata(apache::thrift::RpcKind::SINGLE_REQUEST_SINGLE_RESPONSE);
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
  auto metadata =
      makeMetadata(apache::thrift::RpcKind::SINGLE_REQUEST_SINGLE_RESPONSE);
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

TEST(
    ThriftServerRequestContextHandlerTest,
    MissingMetadataRejectsBeforeDispatch) {
  for (const auto kind : kInitialRpcKinds) {
    SCOPED_TRACE(static_cast<int>(kind));
    FakeContext ctx;
    ThriftServerRequestContextHandler<FakeContext> handler{nullptr};
    auto request = kind == kInitialRpcKinds.front()
        ? makeRequestWithMetadata(*ctx.eventBase(), /*streamId=*/7, nullptr)
        : makeStreamRequestWithMetadata(/*streamId=*/7, nullptr);
    folly::RequestContextScopeGuard callerGuard;
    auto* const callerContext = folly::RequestContext::try_get();

    EXPECT_EQ(
        handler.onRead(ctx, erase_and_box(std::move(request))),
        Result::Success);
    expectMetadataRejection(ctx, 7, nullptr, callerContext);
  }
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

TEST(
    ThriftServerRequestContextHandlerTest,
    DrainsResponseHeadersWrittenThroughBorrowedMap) {
  ThriftServerRequestContextHandler<FakeContext> handler{nullptr};
  FakeContext ctx;
  auto requestContext = makeThriftRequestContext(*ctx.eventBase());
  auto* responseHeaders = &requestContext->mutableResponseHeaders();
  responseHeaders->emplace("shard", "42");

  EXPECT_EQ(
      handler.onWrite(
          ctx,
          erase_and_box(makeResponseWithHeaders(std::move(requestContext)))),
      Result::Success);

  ASSERT_EQ(ctx.written.size(), 1);
  const ThriftRequestContext::HeaderMap expected{{"shard", "42"}};
  EXPECT_EQ(writtenMetadata(ctx.written.front()).otherMetadata(), expected);
  EXPECT_TRUE(responseHeaders->empty());
}

TEST(ThriftServerRequestContextHandlerTest, ExtensionViewsBorrowHeaderMaps) {
  folly::EventBase eventBase;
  ThriftServerRequestMessage request;
  request.requestContext = makeThriftRequestContext(eventBase);
  request.requestContext->setHeaders({{"request", "value"}});

  ThriftRequestMutator view(request);

  const auto requestHeaders = view.requestHeaders();
  ASSERT_TRUE(requestHeaders.has_value());
  EXPECT_EQ(&requestHeaders->get(), &request.requestContext->getHeaders());
  const auto responseHeaders = view.mutableResponseHeaders();
  ASSERT_TRUE(responseHeaders.has_value());
  EXPECT_EQ(
      &responseHeaders->get(),
      &request.requestContext->mutableResponseHeaders());
  responseHeaders->get().emplace("shard", "42");
  const auto* shard = request.requestContext->getResponseHeader("shard");
  ASSERT_NE(shard, nullptr);
  EXPECT_EQ(*shard, "42");

  auto response = makeResponseWithHeaders(makeThriftRequestContext(eventBase));
  response.requestContext->setHeaders({{"response", "value"}});
  const ThriftResponseView responseView(response);
  const auto responseRequestHeaders = responseView.requestHeaders();
  ASSERT_TRUE(responseRequestHeaders.has_value());
  EXPECT_EQ(
      &responseRequestHeaders->get(), &response.requestContext->getHeaders());

  request.requestContext->setHeader("request", "updated");
  response.requestContext->setHeader("response", "updated");
  EXPECT_EQ(requestHeaders->get().at("request"), "updated");
  EXPECT_EQ(responseRequestHeaders->get().at("response"), "updated");
}

TEST(
    ThriftServerRequestContextHandlerTest,
    ExtensionViewsPreserveHeaderPresence) {
  folly::EventBase eventBase;
  ThriftServerRequestMessage request;
  ThriftServerResponseMessage response;
  ThriftRequestMutator requestView(request);
  const ThriftResponseView responseView(response);

  EXPECT_FALSE(requestView.mutableResponseHeaders().has_value());
  EXPECT_FALSE(requestView.requestHeaders().has_value());
  EXPECT_FALSE(responseView.requestHeaders().has_value());

  request.requestContext = makeThriftRequestContext(eventBase);
  response.requestContext = makeThriftRequestContext(eventBase);
  const auto requestHeaders = requestView.requestHeaders();
  const auto responseHeaders = requestView.mutableResponseHeaders();
  const auto responseRequestHeaders = responseView.requestHeaders();
  ASSERT_TRUE(responseHeaders.has_value());
  EXPECT_TRUE(responseHeaders->get().empty());
  ASSERT_TRUE(requestHeaders.has_value());
  ASSERT_TRUE(responseRequestHeaders.has_value());
  EXPECT_TRUE(requestHeaders->get().empty());
  EXPECT_TRUE(responseRequestHeaders->get().empty());
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
