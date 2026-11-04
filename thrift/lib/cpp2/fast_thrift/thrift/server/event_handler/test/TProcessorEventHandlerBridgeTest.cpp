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

#include <thrift/lib/cpp2/fast_thrift/thrift/server/event_handler/TProcessorEventHandlerBridge.h>

#include <algorithm>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include <folly/io/IOBuf.h>
#include <folly/io/async/AsyncSocket.h>

#include <thrift/lib/cpp/TApplicationException.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/protocol/CompactProtocol.h>

namespace apache::thrift::fast_thrift::thrift::server {

namespace {

using channel_pipeline::erase_and_box;
using channel_pipeline::Result;
using channel_pipeline::TypeErasedBox;

class FakeContext {
 public:
  folly::EventBase* eventBase() noexcept { return &eventBase_; }

  Result fireRead(TypeErasedBox&& msg) noexcept {
    forwardedAmbientContext = folly::RequestContext::saveContext();
    read.push_back(std::move(msg));
    return Result::Success;
  }
  Result fireWrite(TypeErasedBox&& msg) noexcept {
    forwardedWriteAmbientContext = folly::RequestContext::saveContext();
    written.push_back(std::move(msg));
    return Result::Success;
  }

  folly::EventBase eventBase_;
  std::vector<TypeErasedBox> read;
  std::vector<TypeErasedBox> written;
  std::shared_ptr<folly::RequestContext> forwardedAmbientContext;
  std::shared_ptr<folly::RequestContext> forwardedWriteAmbientContext;
};

using Bridge = TProcessorEventHandlerBridge<FakeContext>;

constexpr ThriftServerMethodMetadata kPingMethod{
    .serviceName = "TestService",
    .definingServiceName = "TestService",
    .methodName = "ping",
    .qualifiedMethodName = "TestService.ping",
};
constexpr ThriftServerMethodMetadata kEchoMethod{
    .serviceName = "TestService",
    .definingServiceName = "TestService",
    .methodName = "echo",
    .qualifiedMethodName = "TestService.echo",
};
constexpr ThriftServerMethodMetadata kInheritedMethod{
    .serviceName = "LeafService",
    .definingServiceName = "BaseService",
    .methodName = "baseMethod",
    .qualifiedMethodName = "BaseService.baseMethod",
};

// Records the callback sequence across every request on the connection, and
// what each callback was told, so ordering and naming can be asserted
// together.
struct CallLog {
  std::vector<std::string> calls;
  std::string serviceName;
  std::string methodName;
  std::string readMethodName;
  std::size_t readBytes{0};
  apache::thrift::protocol::PROTOCOL_TYPES readProtocol{};
  std::string writeMethodName;
  std::size_t writeBytes{0};
  apache::thrift::protocol::PROTOCOL_TYPES writeProtocol{};
  bool declaredException{false};
  // Only valid until the request's response is written — the bridge destroys
  // the per-request contexts there.
  const apache::thrift::Cpp2RequestContext* readContext{nullptr};
  const apache::thrift::Cpp2ConnContext* connFromNewConnection{nullptr};
  const apache::thrift::Cpp2ConnContext* connFromRequest{nullptr};
  std::string ambientMarker;
  uint32_t postReadBytes{0};
  uint32_t postWriteBytes{0};
  bool mutateReadHeaders{false};
  bool refusePreRead{false};
  bool refuseGetServiceContext{false};
};

// Stamped onto the ambient context by preRead, the way an event handler
// publishes per-request state for whatever runs downstream of it.
struct AmbientMarker : folly::RequestData {
  explicit AmbientMarker(std::string v) : value(std::move(v)) {}
  bool hasCallback() override { return false; }
  std::string value;
};

const folly::RequestToken& markerToken() {
  static const folly::RequestToken token{
      "processor_event_handler_bridge_test_marker"};
  return token;
}

struct AmbientDepth : folly::RequestData {
  explicit AmbientDepth(int value) : value(value) {}
  bool hasCallback() override { return false; }
  int value;
};

const folly::RequestToken& depthToken() {
  static const folly::RequestToken token{
      "processor_event_handler_bridge_test_depth"};
  return token;
}

int currentAmbientDepth() {
  auto* const data = folly::RequestContext::get()->getContextData(depthToken());
  return data == nullptr ? -1 : static_cast<AmbientDepth*>(data)->value;
}

std::shared_ptr<folly::RequestContext> makeAmbientContext(int depth) {
  auto context = std::make_shared<folly::RequestContext>();
  context->setContextData(depthToken(), std::make_unique<AmbientDepth>(depth));
  return context;
}

struct NestedAmbientContext {
  NestedAmbientContext(
      int value, apache::thrift::TConnectionContext* requestContext)
      : depth(value),
        requestContext(requestContext),
        guard(makeAmbientContext(value)) {}

  int depth;
  apache::thrift::TConnectionContext* requestContext;
  folly::RequestContextScopeGuard guard;
};

class ContextPushingEventHandler
    : public apache::thrift::TProcessorEventHandler {
 public:
  ContextPushingEventHandler(
      int depth,
      std::vector<int>* readDepths,
      std::vector<int>* writeDepths,
      std::vector<int>* freeDepths)
      : depth_(depth),
        readDepths_(readDepths),
        writeDepths_(writeDepths),
        freeDepths_(freeDepths) {}

  void* getServiceContext(
      std::string_view,
      std::string_view,
      apache::thrift::TConnectionContext* requestContext) override {
    return new NestedAmbientContext(depth_, requestContext);
  }

  void preRead(void* context, std::string_view) override {
    EXPECT_EQ(static_cast<NestedAmbientContext*>(context)->depth, depth_);
    readDepths_->push_back(currentAmbientDepth());
  }

  void postWrite(void* context, std::string_view, uint32_t) override {
    EXPECT_EQ(static_cast<NestedAmbientContext*>(context)->depth, depth_);
    writeDepths_->push_back(currentAmbientDepth());
  }

  void freeContext(void* context, std::string_view) override {
    ASSERT_NE(context, nullptr);
    auto* nested = static_cast<NestedAmbientContext*>(context);
    EXPECT_EQ(nested->depth, depth_);
    auto* requestContext = static_cast<apache::thrift::Cpp2RequestContext*>(
        nested->requestContext);
    ASSERT_NE(requestContext, nullptr);
    auto* connectionContext = requestContext->getConnectionContext();
    ASSERT_NE(connectionContext, nullptr);
    ASSERT_NE(connectionContext->getPeerAddress(), nullptr);
    EXPECT_EQ(connectionContext->getPeerAddress()->getPort(), 4321);
    freeDepths_->push_back(currentAmbientDepth());
    delete nested;
  }

 private:
  int depth_;
  std::vector<int>* readDepths_;
  std::vector<int>* writeDepths_;
  std::vector<int>* freeDepths_;
};

class RecordingEventHandler : public apache::thrift::TProcessorEventHandler {
 public:
  explicit RecordingEventHandler(CallLog* log) : log_(log) {}

  void* getServiceContext(
      std::string_view serviceName,
      std::string_view fnName,
      apache::thrift::TConnectionContext* connectionContext) override {
    log_->calls.emplace_back("getServiceContext");
    log_->serviceName = std::string(serviceName);
    log_->methodName = std::string(fnName);
    if (log_->refuseGetServiceContext) {
      throw apache::thrift::TApplicationException("no context for you");
    }
    return connectionContext;
  }

  void preRead(void* ctx, std::string_view /*fnName*/) override {
    log_->calls.emplace_back("preRead");
    auto* reqCtx = static_cast<apache::thrift::Cpp2RequestContext*>(ctx);
    log_->readContext = reqCtx;
    log_->connFromRequest = reqCtx->getConnectionContext();
    folly::RequestContext::get()->setContextData(
        markerToken(), std::make_unique<AmbientMarker>("stamped"));
    if (log_->mutateReadHeaders) {
      reqCtx->getHeader()->setReadHeader("cat", "classic");
    }
    if (log_->refusePreRead) {
      reqCtx->getHeader()->setHeader("denied-by", "recording-handler");
      throw apache::thrift::TApplicationException("refused");
    }
  }

  void onReadData(
      void* /*ctx*/,
      std::string_view /*fnName*/,
      const apache::thrift::SerializedMessage& message) override {
    log_->calls.emplace_back("onReadData");
    log_->readMethodName = message.methodName.str();
    log_->readBytes = message.computeChainDataLength();
    log_->readProtocol = message.protocolType;
  }

  void postRead(
      void* /*ctx*/,
      std::string_view /*fnName*/,
      apache::thrift::transport::THeader* header,
      uint32_t bytes) override {
    log_->calls.emplace_back("postRead");
    log_->postReadBytes = bytes;
    // The read headers the request arrived with are visible here.
    if (header != nullptr && header->getHeaders().contains("cat")) {
      log_->calls.emplace_back("sawCatHeader");
    }
  }

  void userExceptionWrapped(
      void* /*ctx*/,
      std::string_view /*fnName*/,
      bool declared,
      const folly::exception_wrapper& /*exception*/) override {
    log_->calls.emplace_back("userExceptionWrapped");
    log_->declaredException = declared;
  }

  void handlerErrorWrapped(
      void* /*ctx*/,
      std::string_view /*fnName*/,
      const folly::exception_wrapper& /*exception*/) override {
    log_->calls.emplace_back("handlerErrorWrapped");
  }

  void preWrite(void* /*ctx*/, std::string_view /*fnName*/) override {
    log_->calls.emplace_back("preWrite");
  }

  void onWriteData(
      void* /*ctx*/,
      std::string_view /*fnName*/,
      const apache::thrift::SerializedMessage& message) override {
    log_->calls.emplace_back("onWriteData");
    log_->writeMethodName = message.methodName.str();
    log_->writeBytes = message.buffer == nullptr
        ? 0
        : message.buffer->computeChainDataLength();
    log_->writeProtocol = message.protocolType;
  }

  void postWrite(
      void* ctx, std::string_view /*fnName*/, uint32_t bytes) override {
    log_->calls.emplace_back("postWrite");
    log_->postWriteBytes = bytes;
    if (auto* data =
            folly::RequestContext::get()->getContextData(markerToken())) {
      log_->ambientMarker = static_cast<AmbientMarker*>(data)->value;
    }
    static_cast<apache::thrift::Cpp2RequestContext*>(ctx)
        ->getHeader()
        ->setHeader("minted", "token");
  }

  void freeContext(void* /*ctx*/, std::string_view /*fnName*/) override {
    log_->calls.emplace_back("freeContext");
  }

 private:
  CallLog* log_;
};

class RecordingServerEventHandler
    : public apache::thrift::server::TServerEventHandler {
 public:
  explicit RecordingServerEventHandler(CallLog* log) : log_(log) {}

  void newConnection(apache::thrift::server::TConnectionContext* ctx) override {
    log_->calls.emplace_back("newConnection");
    log_->connFromNewConnection =
        static_cast<apache::thrift::Cpp2ConnContext*>(ctx);
  }
  void connectionDestroyed(
      apache::thrift::server::TConnectionContext* /*ctx*/) override {
    log_->calls.emplace_back("connectionDestroyed");
  }

 private:
  CallLog* log_;
};

TProcessorEventHandlerBridgeConfig makeConfig(CallLog* log) {
  auto handlers = std::make_shared<TProcessorEventHandlers>();
  handlers->processor.push_back(std::make_shared<RecordingEventHandler>(log));
  handlers->server.push_back(
      std::make_shared<RecordingServerEventHandler>(log));
  auto methodMetadata = std::make_shared<ThriftServerMethodMetadataRegistry>();
  methodMetadata->add(kPingMethod);
  methodMetadata->add(kEchoMethod);
  methodMetadata->add(kInheritedMethod);
  return TProcessorEventHandlerBridgeConfig{
      .handlers = std::move(handlers),
      .methodMetadata = std::move(methodMetadata)};
}

TProcessorEventHandlerBridgeConfig makeNestedContextConfig(
    std::vector<int>* readDepths,
    std::vector<int>* writeDepths,
    std::vector<int>* freeDepths) {
  auto handlers = std::make_shared<TProcessorEventHandlers>();
  for (int depth = 1; depth <= 3; ++depth) {
    handlers->processor.push_back(
        std::make_shared<ContextPushingEventHandler>(
            depth, readDepths, writeDepths, freeDepths));
  }
  auto methodMetadata = std::make_shared<ThriftServerMethodMetadataRegistry>();
  methodMetadata->add(kPingMethod);
  return TProcessorEventHandlerBridgeConfig{
      .handlers = std::move(handlers),
      .methodMetadata = std::move(methodMetadata)};
}

// The slot plan a server that registered the bridge's extension builds at
// start(). Static so it outlives every request context below.
const ExtensionLayout& bridgeRequestLayout() {
  static const ExtensionLayout layout = [] {
    ExtensionLayoutBuilder builder;
    builder.add(Cpp2BridgeExtension::kId, [](void* state) noexcept {
      Cpp2BridgeExtension::destroyRequestState(
          static_cast<Cpp2BridgeExtension::RequestState*>(state));
    });
    return std::move(builder).build();
  }();
  return layout;
}

const ExtensionLayout& bridgeConnectionLayout() {
  static const ExtensionLayout layout = [] {
    ExtensionLayoutBuilder builder;
    builder.add(Cpp2BridgeExtension::kId);
    return std::move(builder).build();
  }();
  return layout;
}

ThriftServerRequestMessage makeRequest(
    folly::EventBase& eventBase,
    const boost::intrusive_ptr<ThriftConnContext>& conn,
    uint32_t streamId,
    std::string_view method,
    ThriftRequestContext::HeaderMap headers = {}) {
  ThriftServerRequestMessage req;
  req.streamId = streamId;
  req.requestContext = makeThriftRequestContext(eventBase);
  req.requestContext->installExtensions(bridgeRequestLayout());
  req.requestContext->setConnectionContext(conn);
  req.requestContext->setHeaders(std::move(headers));
  auto metadata = std::make_unique<apache::thrift::RequestRpcMetadata>();
  metadata->name() = std::string(method);
  metadata->kind() = apache::thrift::RpcKind::SINGLE_REQUEST_SINGLE_RESPONSE;
  metadata->protocol() = apache::thrift::ProtocolId::BINARY;
  req.payload = ThriftServerRequestResponsePayload{
      .data = folly::IOBuf::copyBuffer("0123456789"),
      .metadata = makeServerRequestMetadata(eventBase, std::move(*metadata))};
  return req;
}

ThriftServerRequestMessage makeStreamRequest(
    folly::EventBase& eventBase,
    const boost::intrusive_ptr<ThriftConnContext>& conn,
    uint32_t streamId,
    std::string_view method) {
  ThriftServerRequestMessage req;
  req.streamId = streamId;
  req.requestContext = makeThriftRequestContext(eventBase);
  req.requestContext->installExtensions(bridgeRequestLayout());
  req.requestContext->setConnectionContext(conn);
  auto metadata = std::make_unique<apache::thrift::RequestRpcMetadata>();
  metadata->name() = std::string(method);
  metadata->kind() = apache::thrift::RpcKind::SINGLE_REQUEST_STREAMING_RESPONSE;
  metadata->protocol() = apache::thrift::ProtocolId::BINARY;
  req.payload = ThriftRequestStreamPayload{
      .data = folly::IOBuf::copyBuffer("0123456789"),
      .metadata = std::move(metadata),
      .initialRequestN = 1};
  return req;
}

ThriftServerResponseMessage makeResponse(uint32_t streamId) {
  auto metadata = std::make_unique<apache::thrift::ResponseRpcMetadata>();
  return ThriftServerResponseMessage{
      .payload = ThriftInitialResponsePayload{
          .data = folly::IOBuf::copyBuffer("hello"),
          .metadata = std::move(metadata),
          .streamId = streamId}};
}

// A framework error frame: no reply body, so nothing for the write-side
// callbacks to bracket.
ThriftServerResponseMessage makeErrorResponse(uint32_t streamId) {
  return ThriftServerResponseMessage{
      .payload = ThriftErrorPayload{
          .data = folly::IOBuf::copyBuffer("boom"),
          .metadata = nullptr,
          .streamId = streamId,
          .errorCode = 0}};
}

apache::thrift::ResponseRpcError deserializeResponseRpcError(
    const folly::IOBuf& data) {
  apache::thrift::ResponseRpcError error;
  apache::thrift::CompactProtocolReader reader;
  reader.setInput(&data);
  error.read(&reader);
  return error;
}

// The answer to a request that went downstream, carrying that request's
// context back the way the handler callback hands it over.
ThriftServerResponseMessage attachRequestContext(
    FakeContext& ctx, uint32_t streamId, ThriftServerResponseMessage response) {
  for (auto& box : ctx.read) {
    auto& request = box.get<ThriftServerRequestMessage>();
    if (request.streamId == streamId && request.requestContext != nullptr) {
      response.requestContext = std::move(request.requestContext);
      break;
    }
  }
  return response;
}

ThriftServerResponseMessage makeResponseFor(
    FakeContext& ctx, uint32_t streamId) {
  return attachRequestContext(ctx, streamId, makeResponse(streamId));
}

void recordExceptionFor(FakeContext& ctx, uint32_t streamId, bool declared) {
  for (auto& box : ctx.read) {
    auto& request = box.get<ThriftServerRequestMessage>();
    if (request.streamId == streamId && request.requestContext != nullptr) {
      recordCpp2BridgeException(
          request.requestContext.get(),
          folly::make_exception_wrapper<std::runtime_error>("boom"),
          declared);
      return;
    }
  }
  FAIL() << "request not found";
}

// Drives the connection to the point where it can carry requests: the bridge
// latches the context off the setup message, builds the Cpp2 context, and
// announces the connection before forwarding setup downstream.
void establish(
    Bridge& bridge,
    FakeContext& ctx,
    const boost::intrusive_ptr<ThriftConnContext>& conn) {
  auto setup = std::make_unique<ConnectionSetupData>();
  setup->connContext = conn.get();
  ThriftServerRequestMessage setupMsg;
  setupMsg.payload = ThriftConnectionSetupPayload{.setup = std::move(setup)};
  (void)bridge.onRead(ctx, erase_and_box(std::move(setupMsg)));
}

boost::intrusive_ptr<ThriftConnContext> makeConn() {
  boost::intrusive_ptr<ThriftConnContext> conn{new ThriftConnContext()};
  conn->installExtensions(bridgeConnectionLayout());
  conn->setPeerAddress(folly::SocketAddress("127.0.0.1", 4321));
  return conn;
}

} // namespace

// The classic callback order around one request, and the names the handler is
// given: the service, and the method qualified with it.
TEST(TProcessorEventHandlerBridgeTest, DrivesTheClassicCallbackOrder) {
  CallLog log;
  Bridge bridge(makeConfig(&log));
  FakeContext ctx;
  auto conn = makeConn();
  establish(bridge, ctx, conn);

  EXPECT_EQ(
      bridge.onRead(
          ctx, erase_and_box(makeRequest(*ctx.eventBase(), conn, 7, "ping"))),
      Result::Success);
  EXPECT_EQ(ctx.read.size(), 2); // setup + request
  EXPECT_EQ(
      bridge.onWrite(ctx, erase_and_box(makeResponseFor(ctx, 7))),
      Result::Success);

  EXPECT_EQ(
      log.calls,
      (std::vector<std::string>{
          "newConnection",
          "getServiceContext",
          "preRead",
          "onReadData",
          "postRead",
          "preWrite",
          "onWriteData",
          "postWrite",
          "freeContext"}));
  EXPECT_EQ(log.serviceName, "TestService");
  EXPECT_EQ(log.methodName, "TestService.ping");
  EXPECT_EQ(log.readMethodName, "TestService.ping");
  EXPECT_EQ(log.readBytes, 10);
  EXPECT_EQ(log.readProtocol, apache::thrift::protocol::T_BINARY_PROTOCOL);
  EXPECT_EQ(log.writeMethodName, "TestService.ping");
  EXPECT_EQ(log.writeBytes, 5);
  EXPECT_EQ(log.writeProtocol, apache::thrift::protocol::T_BINARY_PROTOCOL);
  // The connection the server event handler was told about is the one every
  // request on it reports.
  ASSERT_NE(log.connFromNewConnection, nullptr);
  EXPECT_EQ(log.connFromNewConnection, log.connFromRequest);
  EXPECT_EQ(log.postReadBytes, 10);
  EXPECT_EQ(log.postWriteBytes, 5);
}

TEST(TProcessorEventHandlerBridgeTest, DroppedRequestFreesContextExactlyOnce) {
  CallLog log;
  Bridge bridge(makeConfig(&log));
  FakeContext ctx;
  auto conn = makeConn();
  establish(bridge, ctx, conn);
  log.calls.clear();

  ASSERT_EQ(
      bridge.onRead(
          ctx, erase_and_box(makeRequest(*ctx.eventBase(), conn, 17, "ping"))),
      Result::Success);
  ASSERT_EQ(ctx.read.size(), 2);
  ctx.read.clear();

  EXPECT_EQ(std::count(log.calls.begin(), log.calls.end(), "freeContext"), 1);
  EXPECT_EQ(
      std::find(log.calls.begin(), log.calls.end(), "preWrite"),
      log.calls.end());
}

TEST(TProcessorEventHandlerBridgeTest, DeclaredExceptionRunsWriteCallbacks) {
  CallLog log;
  Bridge bridge(makeConfig(&log));
  FakeContext ctx;
  auto conn = makeConn();
  establish(bridge, ctx, conn);

  (void)bridge.onRead(
      ctx, erase_and_box(makeRequest(*ctx.eventBase(), conn, 7, "ping")));
  recordExceptionFor(ctx, 7, true);
  (void)bridge.onWrite(ctx, erase_and_box(makeResponseFor(ctx, 7)));

  EXPECT_TRUE(log.declaredException);
  EXPECT_EQ(
      log.calls,
      (std::vector<std::string>{
          "newConnection",
          "getServiceContext",
          "preRead",
          "onReadData",
          "postRead",
          "userExceptionWrapped",
          "preWrite",
          "onWriteData",
          "postWrite",
          "freeContext"}));
}

TEST(TProcessorEventHandlerBridgeTest, UndeclaredExceptionRunsErrorCallback) {
  CallLog log;
  Bridge bridge(makeConfig(&log));
  FakeContext ctx;
  auto conn = makeConn();
  establish(bridge, ctx, conn);

  (void)bridge.onRead(
      ctx, erase_and_box(makeRequest(*ctx.eventBase(), conn, 7, "ping")));
  recordExceptionFor(ctx, 7, false);
  auto response = makeUnknownExceptionMessage(
      7,
      folly::make_exception_wrapper<std::runtime_error>("boom"),
      apache::thrift::ErrorBlame::SERVER);
  for (auto& box : ctx.read) {
    auto& request = box.get<ThriftServerRequestMessage>();
    if (request.streamId == 7 && request.requestContext != nullptr) {
      response.requestContext = std::move(request.requestContext);
      break;
    }
  }
  (void)bridge.onWrite(ctx, erase_and_box(std::move(response)));

  EXPECT_FALSE(log.declaredException);
  EXPECT_EQ(
      log.calls,
      (std::vector<std::string>{
          "newConnection",
          "getServiceContext",
          "preRead",
          "onReadData",
          "postRead",
          "userExceptionWrapped",
          "handlerErrorWrapped",
          "freeContext"}));
}

// Request headers reach the THeader handlers read, and response headers they
// write on the way out reach the response.
TEST(TProcessorEventHandlerBridgeTest, HeadersCrossInBothDirections) {
  CallLog log;
  Bridge bridge(makeConfig(&log));
  FakeContext ctx;
  auto conn = makeConn();
  establish(bridge, ctx, conn);

  (void)bridge.onRead(
      ctx,
      erase_and_box(
          makeRequest(*ctx.eventBase(), conn, 1, "ping", {{"cat", "token"}})));
  (void)bridge.onWrite(ctx, erase_and_box(makeResponseFor(ctx, 1)));

  EXPECT_NE(
      std::find(log.calls.begin(), log.calls.end(), "sawCatHeader"),
      log.calls.end());

  ASSERT_EQ(ctx.written.size(), 1);
  auto& response = ctx.written.front().get<ThriftServerResponseMessage>();
  const auto& other = *response.payload.get<ThriftInitialResponsePayload>()
                           .metadata->otherMetadata();
  ASSERT_TRUE(other.contains("minted"));
  EXPECT_EQ(other.at("minted"), "token");
}

// A request served by the state a previous one returned sees none of that
// request's headers: the state's header is rebound, not added to.
TEST(TProcessorEventHandlerBridgeTest, ReusedStateDropsThePriorRequestHeaders) {
  CallLog log;
  Bridge bridge(makeConfig(&log));
  FakeContext ctx;
  auto conn = makeConn();
  establish(bridge, ctx, conn);

  (void)bridge.onRead(
      ctx,
      erase_and_box(
          makeRequest(*ctx.eventBase(), conn, 1, "ping", {{"cat", "token"}})));
  (void)bridge.onWrite(ctx, erase_and_box(makeResponseFor(ctx, 1)));

  (void)bridge.onRead(
      ctx, erase_and_box(makeRequest(*ctx.eventBase(), conn, 3, "ping")));
  (void)bridge.onWrite(ctx, erase_and_box(makeResponseFor(ctx, 3)));

  EXPECT_EQ(std::count(log.calls.begin(), log.calls.end(), "sawCatHeader"), 1);
}

// The ambient context a handler stamps in preRead is the one still installed
// when its write-side callbacks run — one context per request, not the
// process-global default.
TEST(TProcessorEventHandlerBridgeTest, AmbientContextSpansBothDirections) {
  CallLog log;
  Bridge bridge(makeConfig(&log));
  FakeContext ctx;
  auto conn = makeConn();
  establish(bridge, ctx, conn);

  (void)bridge.onRead(
      ctx, erase_and_box(makeRequest(*ctx.eventBase(), conn, 1, "ping")));
  (void)bridge.onWrite(ctx, erase_and_box(makeResponseFor(ctx, 1)));

  EXPECT_EQ(log.ambientMarker, "stamped");
  // Nothing leaked into the caller's context.
  EXPECT_EQ(
      folly::RequestContext::get()->getContextData(markerToken()), nullptr);
}

TEST(
    TProcessorEventHandlerBridgeTest,
    NestedAmbientContextsReachDispatchAndResponseCallbacks) {
  std::vector<int> readDepths;
  std::vector<int> writeDepths;
  std::vector<int> freeDepths;
  Bridge bridge(
      makeNestedContextConfig(&readDepths, &writeDepths, &freeDepths));
  FakeContext ctx;
  auto conn = makeConn();

  folly::RequestContextScopeGuard callerGuard;
  folly::RequestContext::get()->setContextData(
      depthToken(), std::make_unique<AmbientDepth>(99));
  establish(bridge, ctx, conn);

  (void)bridge.onRead(
      ctx, erase_and_box(makeRequest(*ctx.eventBase(), conn, 1, "ping")));

  EXPECT_EQ(currentAmbientDepth(), 99);
  ASSERT_NE(ctx.forwardedAmbientContext, nullptr);
  ASSERT_FALSE(ctx.read.empty());
  const auto& forwarded = ctx.read.back().get<ThriftServerRequestMessage>();
  ASSERT_NE(forwarded.requestContext, nullptr);
  EXPECT_EQ(
      captureFollyRequestContext(*forwarded.requestContext),
      ctx.forwardedAmbientContext);
  {
    folly::RequestContextScopeGuard forwardedGuard(ctx.forwardedAmbientContext);
    EXPECT_EQ(currentAmbientDepth(), 3);
  }

  (void)bridge.onWrite(ctx, erase_and_box(makeResponseFor(ctx, 1)));

  EXPECT_EQ(readDepths, (std::vector<int>{3, 3, 3}));
  EXPECT_EQ(writeDepths, (std::vector<int>{3, 3, 3}));
  EXPECT_EQ(freeDepths, (std::vector<int>{3, 3, 3}));
  EXPECT_EQ(currentAmbientDepth(), 99);
  ASSERT_NE(ctx.forwardedWriteAmbientContext, nullptr);
  {
    folly::RequestContextScopeGuard forwardedGuard(
        ctx.forwardedWriteAmbientContext);
    EXPECT_EQ(currentAmbientDepth(), 99);
  }
}

TEST(
    TProcessorEventHandlerBridgeTest,
    InFlightNestedContextsRetainConnectionStateAndRestoreCallerContext) {
  std::vector<int> readDepths;
  std::vector<int> writeDepths;
  std::vector<int> freeDepths;
  FakeContext ctx;
  auto conn = makeConn();

  folly::RequestContextScopeGuard callerGuard;
  folly::RequestContext::get()->setContextData(
      depthToken(), std::make_unique<AmbientDepth>(99));
  {
    Bridge bridge(
        makeNestedContextConfig(&readDepths, &writeDepths, &freeDepths));
    establish(bridge, ctx, conn);
    (void)bridge.onRead(
        ctx, erase_and_box(makeRequest(*ctx.eventBase(), conn, 1, "ping")));
    EXPECT_EQ(currentAmbientDepth(), 99);
    EXPECT_TRUE(freeDepths.empty());
  }

  EXPECT_TRUE(freeDepths.empty());
  ctx.read.clear();
  EXPECT_EQ(freeDepths, (std::vector<int>{3, 3, 3}));
  EXPECT_EQ(currentAmbientDepth(), 99);
}

// A handler throwing from preRead is refusing the request: it is dropped
// rather than forwarded, the client is answered, and — as on a classic server,
// where the exception precedes dispatch — the write-side callbacks never run.
TEST(
    TProcessorEventHandlerBridgeTest, RefusalDropsTheRequestAndSkipsPostWrite) {
  CallLog log;
  log.refusePreRead = true;
  Bridge bridge(makeConfig(&log));
  FakeContext ctx;
  auto conn = makeConn();
  establish(bridge, ctx, conn);

  EXPECT_EQ(
      bridge.onRead(
          ctx, erase_and_box(makeRequest(*ctx.eventBase(), conn, 3, "ping"))),
      Result::Success);

  // Only the setup message went downstream; the request did not.
  EXPECT_EQ(ctx.read.size(), 1);
  ASSERT_EQ(ctx.written.size(), 1);
  EXPECT_EQ(
      std::find(log.calls.begin(), log.calls.end(), "postWrite"),
      log.calls.end());

  // The refusal carries the header the handler set before throwing.
  auto& response = ctx.written.front().get<ThriftServerResponseMessage>();
  const auto& other = *response.payload.get<ThriftInitialResponsePayload>()
                           .metadata->otherMetadata();
  ASSERT_TRUE(other.contains("denied-by"));
  EXPECT_EQ(other.at("denied-by"), "recording-handler");
}

// A handler throwing while it is being handed the request's context is
// refusing the request too — not escaping the pipeline callback, which is
// noexcept. The contexts the handlers ahead of it already gave up come back.
TEST(TProcessorEventHandlerBridgeTest, GetServiceContextRefusalIsCaught) {
  CallLog log;
  log.refuseGetServiceContext = true;
  Bridge bridge(makeConfig(&log));
  FakeContext ctx;
  auto conn = makeConn();
  establish(bridge, ctx, conn);

  EXPECT_EQ(
      bridge.onRead(
          ctx, erase_and_box(makeRequest(*ctx.eventBase(), conn, 3, "ping"))),
      Result::Success);

  // Only the setup message went downstream; the request did not.
  EXPECT_EQ(ctx.read.size(), 1);
  EXPECT_EQ(ctx.written.size(), 1);
  EXPECT_EQ(
      std::find(log.calls.begin(), log.calls.end(), "preRead"),
      log.calls.end());
  EXPECT_NE(
      std::find(log.calls.begin(), log.calls.end(), "freeContext"),
      log.calls.end());
}

// Inherited methods retain the leaf service name while using the service that
// defined the method for the qualified name, matching generated processors.
TEST(TProcessorEventHandlerBridgeTest, UsesResolvedInheritedMethodNames) {
  CallLog log;
  Bridge bridge(makeConfig(&log));
  FakeContext ctx;
  auto conn = makeConn();
  establish(bridge, ctx, conn);

  (void)bridge.onRead(
      ctx, erase_and_box(makeRequest(*ctx.eventBase(), conn, 1, "baseMethod")));
  (void)bridge.onWrite(ctx, erase_and_box(makeResponseFor(ctx, 1)));

  EXPECT_EQ(log.serviceName, "LeafService");
  EXPECT_EQ(log.methodName, "BaseService.baseMethod");
}

TEST(TProcessorEventHandlerBridgeTest, UnknownMethodSkipsCallbacks) {
  CallLog log;
  Bridge bridge(makeConfig(&log));
  FakeContext ctx;
  auto conn = makeConn();
  establish(bridge, ctx, conn);
  log.calls.clear();

  EXPECT_EQ(
      bridge.onRead(
          ctx,
          erase_and_box(makeRequest(*ctx.eventBase(), conn, 1, "unknown"))),
      Result::Success);

  EXPECT_EQ(ctx.read.size(), 2);
  EXPECT_TRUE(ctx.written.empty());
  EXPECT_TRUE(log.calls.empty());
}

TEST(TProcessorEventHandlerBridgeTest, UnsupportedRpcKindSkipsCallbacks) {
  CallLog log;
  Bridge bridge(makeConfig(&log));
  FakeContext ctx;
  auto conn = makeConn();
  establish(bridge, ctx, conn);
  log.calls.clear();
  auto request = makeRequest(*ctx.eventBase(), conn, 1, "ping");
  request.payload.get<ThriftServerRequestResponsePayload>().metadata->kind() =
      apache::thrift::RpcKind::SINGLE_REQUEST_NO_RESPONSE;

  EXPECT_EQ(
      bridge.onRead(ctx, erase_and_box(std::move(request))), Result::Success);

  EXPECT_EQ(ctx.read.size(), 2);
  EXPECT_TRUE(ctx.written.empty());
  EXPECT_TRUE(log.calls.empty());
}

TEST(TProcessorEventHandlerBridgeTest, StreamingRequestIsRefusedFailClosed) {
  CallLog log;
  Bridge bridge(makeConfig(&log));
  FakeContext ctx;
  auto conn = makeConn();
  establish(bridge, ctx, conn);
  log.calls.clear();
  auto request = makeStreamRequest(*ctx.eventBase(), conn, 1, "ping");
  auto* const requestContext = request.requestContext.get();

  EXPECT_EQ(
      bridge.onRead(ctx, erase_and_box(std::move(request))), Result::Success);

  // Only setup was forwarded; the stream request did not reach the app.
  EXPECT_EQ(ctx.read.size(), 1);
  ASSERT_EQ(ctx.written.size(), 1);
  EXPECT_TRUE(log.calls.empty());
  const auto& response = ctx.written.front().get<ThriftServerResponseMessage>();
  EXPECT_EQ(response.requestContext.get(), requestContext);
  ASSERT_TRUE(response.payload.is<ThriftErrorPayload>());
  const auto& error = response.payload.get<ThriftErrorPayload>();
  ASSERT_NE(error.data, nullptr);
  const auto rpcError = deserializeResponseRpcError(*error.data);
  EXPECT_EQ(
      *rpcError.code(), apache::thrift::ResponseRpcErrorCode::WRONG_RPC_KIND);
}

// The write-side callbacks bracket serializing a reply body, so a framework
// error that carries none does not get them — matching the classic server,
// where an error sent outside the reply path never reaches them. The handlers'
// contexts are still returned, which is the pairing they are promised.
TEST(TProcessorEventHandlerBridgeTest, ErrorFrameSkipsWriteCallbacksButFrees) {
  CallLog log;
  Bridge bridge(makeConfig(&log));
  FakeContext ctx;
  auto conn = makeConn();
  establish(bridge, ctx, conn);

  (void)bridge.onRead(
      ctx, erase_and_box(makeRequest(*ctx.eventBase(), conn, 5, "ping")));
  (void)bridge.onWrite(
      ctx, erase_and_box(attachRequestContext(ctx, 5, makeErrorResponse(5))));

  EXPECT_EQ(
      std::find(log.calls.begin(), log.calls.end(), "preWrite"),
      log.calls.end());
  EXPECT_EQ(
      std::find(log.calls.begin(), log.calls.end(), "postWrite"),
      log.calls.end());
  EXPECT_NE(
      std::find(log.calls.begin(), log.calls.end(), "freeContext"),
      log.calls.end());
}

// An application error produced before the service reply skips write-side
// callbacks, matching classic ServiceInterceptor behavior.
TEST(
    TProcessorEventHandlerBridgeTest,
    ApplicationExceptionSkipsWriteCallbacksButFrees) {
  CallLog log;
  Bridge bridge(makeConfig(&log));
  FakeContext ctx;
  auto conn = makeConn();
  establish(bridge, ctx, conn);

  (void)bridge.onRead(
      ctx, erase_and_box(makeRequest(*ctx.eventBase(), conn, 5, "ping")));
  (void)bridge.onWrite(
      ctx,
      erase_and_box(attachRequestContext(
          ctx,
          5,
          makeUnknownExceptionMessage(
              5,
              folly::make_exception_wrapper<
                  apache::thrift::TApplicationException>("denied")))));

  EXPECT_EQ(
      std::find(log.calls.begin(), log.calls.end(), "preWrite"),
      log.calls.end());
  EXPECT_EQ(
      std::find(log.calls.begin(), log.calls.end(), "postWrite"),
      log.calls.end());
  EXPECT_NE(
      std::find(log.calls.begin(), log.calls.end(), "freeContext"),
      log.calls.end());
}

// Declared exceptions are service replies, so classic runs write-side
// callbacks for them just as it does for successful replies.
TEST(
    TProcessorEventHandlerBridgeTest,
    DeclaredExceptionRunsWriteCallbacksAndFrees) {
  CallLog log;
  Bridge bridge(makeConfig(&log));
  FakeContext ctx;
  auto conn = makeConn();
  establish(bridge, ctx, conn);

  (void)bridge.onRead(
      ctx, erase_and_box(makeRequest(*ctx.eventBase(), conn, 5, "ping")));
  auto metadata = std::make_unique<apache::thrift::ResponseRpcMetadata>();
  fillDeclaredExceptionMetadata(*metadata, "Busy", "busy");
  (void)bridge.onWrite(
      ctx,
      erase_and_box(attachRequestContext(
          ctx,
          5,
          makeResponseMessage(
              5, folly::IOBuf::copyBuffer("declared"), std::move(metadata)))));

  EXPECT_NE(
      std::find(log.calls.begin(), log.calls.end(), "preWrite"),
      log.calls.end());
  EXPECT_NE(
      std::find(log.calls.begin(), log.calls.end(), "postWrite"),
      log.calls.end());
  EXPECT_NE(
      std::find(log.calls.begin(), log.calls.end(), "freeContext"),
      log.calls.end());
}

// A response the bridge never saw a request for — a connection-level frame, or
// the answer to a request it refused — passes through untouched.
TEST(TProcessorEventHandlerBridgeTest, UnmatchedResponsePassesThrough) {
  CallLog log;
  Bridge bridge(makeConfig(&log));
  FakeContext ctx;
  auto conn = makeConn();
  establish(bridge, ctx, conn);

  EXPECT_EQ(
      bridge.onWrite(ctx, erase_and_box(makeResponse(99))), Result::Success);

  EXPECT_EQ(ctx.written.size(), 1);
  EXPECT_EQ(
      std::find(log.calls.begin(), log.calls.end(), "preWrite"),
      log.calls.end());
}

// Every request gets its own contexts even though they share the connection's.
TEST(TProcessorEventHandlerBridgeTest, ConcurrentRequestsAreTrackedSeparately) {
  CallLog log;
  Bridge bridge(makeConfig(&log));
  FakeContext ctx;
  auto conn = makeConn();
  establish(bridge, ctx, conn);

  (void)bridge.onRead(
      ctx, erase_and_box(makeRequest(*ctx.eventBase(), conn, 1, "ping")));
  const auto* first = log.readContext;
  (void)bridge.onRead(
      ctx, erase_and_box(makeRequest(*ctx.eventBase(), conn, 2, "echo")));
  const auto* second = log.readContext;

  EXPECT_NE(first, second);
  EXPECT_EQ(first->getConnectionContext(), second->getConnectionContext());

  // Responses arrive out of order; each still finds its own request.
  (void)bridge.onWrite(ctx, erase_and_box(makeResponseFor(ctx, 2)));
  (void)bridge.onWrite(ctx, erase_and_box(makeResponseFor(ctx, 1)));
  EXPECT_EQ(std::count(log.calls.begin(), log.calls.end(), "postWrite"), 2);
}

// A request that arrives with no context is a misconfigured server, not a
// request to let through: the bridge cannot tell whether a handler was going
// to authorize it.
TEST(TProcessorEventHandlerBridgeTest, RequestWithoutContextIsRefused) {
  CallLog log;
  Bridge bridge(makeConfig(&log));
  FakeContext ctx;
  auto conn = makeConn();
  establish(bridge, ctx, conn);

  auto request = makeRequest(*ctx.eventBase(), conn, 5, "ping");
  request.requestContext.reset();
  EXPECT_EQ(
      bridge.onRead(ctx, erase_and_box(std::move(request))), Result::Success);

  EXPECT_EQ(ctx.read.size(), 1); // setup only
  EXPECT_EQ(ctx.written.size(), 1);
  EXPECT_EQ(
      std::find(log.calls.begin(), log.calls.end(), "preRead"),
      log.calls.end());
}

TEST(
    TProcessorEventHandlerBridgeTest,
    MissingMethodRegistryResponseRetainsRequestContext) {
  CallLog log;
  auto config = makeConfig(&log);
  config.methodMetadata.reset();
  Bridge bridge(std::move(config));
  FakeContext ctx;
  auto conn = makeConn();
  establish(bridge, ctx, conn);
  auto request = makeRequest(*ctx.eventBase(), conn, 5, "ping");
  auto* const requestContext = request.requestContext.get();
  auto requestFollyContext =
      request.requestContext->getFollyRequestContext().capture();
  folly::RequestContextScopeGuard callerGuard;
  auto* const callerContext = folly::RequestContext::try_get();

  {
    folly::RequestContextScopeGuard requestGuard(requestFollyContext);
    EXPECT_EQ(
        bridge.onRead(ctx, erase_and_box(std::move(request))), Result::Success);
  }

  ASSERT_EQ(ctx.written.size(), 1);
  const auto& response = ctx.written.front().get<ThriftServerResponseMessage>();
  EXPECT_EQ(response.requestContext.get(), requestContext);
  EXPECT_EQ(ctx.forwardedWriteAmbientContext, requestFollyContext);
  EXPECT_EQ(folly::RequestContext::try_get(), callerContext);
}

TEST(
    TProcessorEventHandlerBridgeTest,
    MissingConnectionContextResponseRetainsRequestContext) {
  CallLog log;
  Bridge bridge(makeConfig(&log));
  FakeContext ctx;
  auto conn = makeConn();
  auto request = makeRequest(*ctx.eventBase(), conn, 5, "ping");
  auto* const requestContext = request.requestContext.get();
  auto requestFollyContext =
      request.requestContext->getFollyRequestContext().capture();
  folly::RequestContextScopeGuard callerGuard;
  auto* const callerContext = folly::RequestContext::try_get();

  {
    folly::RequestContextScopeGuard requestGuard(requestFollyContext);
    EXPECT_EQ(
        bridge.onRead(ctx, erase_and_box(std::move(request))), Result::Success);
  }

  ASSERT_EQ(ctx.written.size(), 1);
  const auto& response = ctx.written.front().get<ThriftServerResponseMessage>();
  EXPECT_EQ(response.requestContext.get(), requestContext);
  EXPECT_EQ(ctx.forwardedWriteAmbientContext, requestFollyContext);
  EXPECT_EQ(folly::RequestContext::try_get(), callerContext);
}

// A handler is promised one connectionDestroyed for the newConnection it was
// told about, however many times the event arrives.
TEST(
    TProcessorEventHandlerBridgeTest,
    ConnectionCloseIsAnnouncedOnceAndCancelsBeforeRequestCleanup) {
  CallLog log;
  Bridge bridge(makeConfig(&log));
  FakeContext ctx;
  auto conn = makeConn();
  auto socket = folly::AsyncSocket::newSocket(ctx.eventBase());
  conn->setTransport(socket.get());
  establish(bridge, ctx, conn);
  ASSERT_NE(log.connFromNewConnection, nullptr);
  EXPECT_EQ(log.connFromNewConnection->getTransport(), socket.get());
  auto cancellationToken = log.connFromNewConnection->getCancellationToken();
  EXPECT_FALSE(cancellationToken.isCancellationRequested());
  (void)bridge.onRead(
      ctx, erase_and_box(makeRequest(*ctx.eventBase(), conn, 1, "ping")));

  bridge.on<ThriftServerConnectionClosedEvent>(ctx);
  bridge.on<ThriftServerConnectionClosedEvent>(ctx);

  EXPECT_EQ(std::count(log.calls.begin(), log.calls.end(), "freeContext"), 0);
  EXPECT_EQ(
      std::count(log.calls.begin(), log.calls.end(), "connectionDestroyed"), 1);
  EXPECT_TRUE(cancellationToken.isCancellationRequested());
  EXPECT_EQ(log.connFromNewConnection->getTransport(), nullptr);
  ctx.read.clear();
  EXPECT_EQ(std::count(log.calls.begin(), log.calls.end(), "freeContext"), 1);
  EXPECT_LT(
      std::find(log.calls.begin(), log.calls.end(), "connectionDestroyed"),
      std::find(log.calls.begin(), log.calls.end(), "freeContext"));
}

TEST(
    TProcessorEventHandlerBridgeTest,
    HandlerRemovalAnnouncesConnectionClosureOnce) {
  CallLog log;
  Bridge bridge(makeConfig(&log));
  FakeContext ctx;
  auto conn = makeConn();
  establish(bridge, ctx, conn);
  ASSERT_NE(log.connFromNewConnection, nullptr);
  auto cancellationToken = log.connFromNewConnection->getCancellationToken();

  bridge.handlerRemoved(ctx);
  bridge.handlerRemoved(ctx);

  EXPECT_EQ(
      std::count(log.calls.begin(), log.calls.end(), "connectionDestroyed"), 1);
  EXPECT_TRUE(cancellationToken.isCancellationRequested());
}

TEST(
    TProcessorEventHandlerBridgeTest,
    BridgeDestruction_InFlightDetachedHeaders_PreservesNativeHeaders) {
  CallLog log;
  log.mutateReadHeaders = true;
  FakeContext ctx;
  auto conn = makeConn();

  {
    Bridge bridge(makeConfig(&log));
    establish(bridge, ctx, conn);
    EXPECT_EQ(
        bridge.onRead(
            ctx,
            erase_and_box(makeRequest(
                *ctx.eventBase(), conn, 1, "ping", {{"cat", "native"}}))),
        Result::Success);

    ASSERT_NE(log.readContext, nullptr);
    EXPECT_EQ(log.readContext->getHeader()->getHeaders().at("cat"), "classic");
  }

  auto& request = ctx.read.back().get<ThriftServerRequestMessage>();
  ASSERT_NE(request.requestContext, nullptr);
  EXPECT_EQ(request.requestContext->getHeaders().at("cat"), "native");
}

// With nothing installed the bridge is a pass-through, so a server can wire it
// unconditionally.
TEST(TProcessorEventHandlerBridgeTest, NoHandlersForwardsUntouched) {
  CallLog log;
  Bridge bridge(
      TProcessorEventHandlerBridgeConfig{
          .handlers = nullptr, .methodMetadata = nullptr});
  FakeContext ctx;
  auto conn = makeConn();
  establish(bridge, ctx, conn);

  EXPECT_EQ(
      bridge.onRead(
          ctx, erase_and_box(makeRequest(*ctx.eventBase(), conn, 1, "ping"))),
      Result::Success);

  EXPECT_EQ(ctx.read.size(), 2);
  EXPECT_TRUE(ctx.written.empty());
  EXPECT_TRUE(log.calls.empty());
}

} // namespace apache::thrift::fast_thrift::thrift::server
