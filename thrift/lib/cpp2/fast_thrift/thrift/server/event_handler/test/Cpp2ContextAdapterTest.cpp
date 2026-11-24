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

#include <thrift/lib/cpp2/fast_thrift/thrift/server/event_handler/Cpp2ContextAdapter.h>

#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include <folly/CancellationToken.h>
#include <folly/SocketAddress.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/ssl/BasicTransportCertificate.h>
#include <folly/io/async/ssl/OpenSSLTransportCertificate.h>
#include <folly/io/async/ssl/OpenSSLUtils.h>
#include <folly/io/async/test/MockAsyncTransport.h>
#include <folly/portability/GMock.h>

#include <thrift/lib/cpp2/server/Cpp2Worker.h>

namespace apache::thrift::fast_thrift::thrift::server {

namespace {

// Stands in for what a resolver produces: the opaque object
// Cpp2ConnContext::getPeerIdentities hands back.
struct ResolvedIdentities {
  std::string identity;
  const folly::AsyncTransportCertificate* certSeen{nullptr};
  const folly::AsyncTransport* transportSeen{nullptr};
  const X509* x509Seen{nullptr};
};

std::unique_ptr<void, void (*)(void*)> resolveIdentities(
    const folly::AsyncTransport* transport,
    X509* x509,
    const folly::SocketAddress& /*peerAddress*/) {
  const auto* cert = transport ? transport->getPeerCertificate() : nullptr;
  auto owned = std::make_unique<ResolvedIdentities>();
  owned->identity = cert ? cert->getIdentity()
      : x509             ? folly::ssl::OpenSSLUtils::getCommonName(x509)
                         : "plaintext";
  owned->certSeen = cert;
  owned->transportSeen = transport;
  owned->x509Seen = x509;
  return std::unique_ptr<void, void (*)(void*)>(
      owned.release(),
      +[](void* p) noexcept { delete static_cast<ResolvedIdentities*>(p); });
}

const ExtensionLayout& bridgeLayout();

boost::intrusive_ptr<ThriftConnContext> makeConnContext(
    std::shared_ptr<const folly::AsyncTransportCertificate> cert) {
  boost::intrusive_ptr<ThriftConnContext> conn{new ThriftConnContext()};
  conn->installExtensions(bridgeLayout());
  conn->setPeerAddress(folly::SocketAddress("127.0.0.1", 4321));
  conn->setSecurityProtocol("TLS1.3");
  conn->setPeerCertificate(std::move(cert));
  return conn;
}

// The slot plan a server that installed the bridge builds at start(). Static so
// it outlives every request context below, as the server's own layout does.
const ExtensionLayout& bridgeLayout() {
  static const ExtensionLayout layout = [] {
    ExtensionLayoutBuilder builder;
    builder.add(Cpp2BridgeExtension::kId);
    return std::move(builder).build();
  }();
  return layout;
}

} // namespace

TEST(Cpp2ContextAdapterTest, ConnectionContextCarriesWhatTheConnectionKnows) {
  auto conn = makeConnContext(nullptr);

  Cpp2ConnContextAdapter adapter(conn, &resolveIdentities);

  EXPECT_EQ(
      *adapter.get().getPeerAddress(), folly::SocketAddress("127.0.0.1", 4321));
  EXPECT_EQ(adapter.get().getSecurityProtocol(), "TLS1.3");
}

TEST(Cpp2ContextAdapterTest, HookReadsTransportIdentitiesWithoutX509) {
  auto cert = std::make_shared<folly::ssl::BasicTransportCertificate>(
      "peer.identity", nullptr);
  auto conn = makeConnContext(cert);
  testing::NiceMock<folly::test::MockAsyncTransport> transport;
  ON_CALL(transport, getPeerCertificate())
      .WillByDefault(testing::Return(cert.get()));
  conn->setTransport(&transport);

  Cpp2ConnContextAdapter adapter(conn, &resolveIdentities);

  auto* identities =
      static_cast<ResolvedIdentities*>(adapter.get().getPeerIdentities());
  ASSERT_NE(identities, nullptr);
  EXPECT_EQ(identities->identity, "peer.identity");
  EXPECT_EQ(identities->certSeen, cert.get());
}

TEST(Cpp2ContextAdapterTest, HookReadsTheLiveCertificateAtInvocation) {
  auto retained = std::make_shared<folly::ssl::BasicTransportCertificate>(
      "retained", nullptr);
  const folly::ssl::BasicTransportCertificate first("first", nullptr);
  const folly::ssl::BasicTransportCertificate second("second", nullptr);
  testing::NiceMock<folly::test::MockAsyncTransport> transport;
  EXPECT_CALL(transport, getPeerCertificate())
      .WillOnce(testing::Return(&first))
      .WillOnce(testing::Return(&first))
      .WillOnce(testing::Return(&second));
  auto conn = makeConnContext(retained);
  conn->setTransport(&transport);

  Cpp2ConnContextAdapter adapter(conn, &resolveIdentities);

  auto* identities =
      static_cast<ResolvedIdentities*>(adapter.get().getPeerIdentities());
  ASSERT_NE(identities, nullptr);
  EXPECT_EQ(identities->identity, "second");
  EXPECT_EQ(identities->certSeen, &second);
}

TEST(Cpp2ContextAdapterTest, CachedX509FallbackSurvivesTransportRemoval) {
  auto cert = std::make_shared<folly::ssl::BasicTransportCertificate>(
      "retained", folly::ssl::X509UniquePtr(X509_new()));
  auto x509 = cert->getX509();
  ASSERT_NE(x509, nullptr);
  ASSERT_EQ(
      X509_NAME_add_entry_by_txt(
          X509_get_subject_name(x509.get()),
          "CN",
          MBSTRING_ASC,
          reinterpret_cast<const unsigned char*>("retained"),
          -1,
          -1,
          0),
      1);
  const auto* expected = x509.get();
  auto conn = makeConnContext(cert);
  folly::EventBase evb;
  auto socket = folly::AsyncSocket::newSocket(&evb);
  conn->setTransport(socket.get());

  Cpp2ConnContextAdapter adapter(conn, &resolveIdentities);

  EXPECT_EQ(adapter.get().getPeerCertificate(), expected);
  auto* identities =
      static_cast<ResolvedIdentities*>(adapter.get().getPeerIdentities());
  ASSERT_NE(identities, nullptr);
  EXPECT_EQ(identities->certSeen, nullptr);
  EXPECT_EQ(identities->x509Seen, expected);
  EXPECT_EQ(identities->identity, "retained");
  x509.reset();
  cert->dropX509();
  conn->setPeerCertificate(nullptr);
  adapter.connectionClosed();
  EXPECT_EQ(adapter.get().getTransport(), nullptr);
  EXPECT_EQ(adapter.get().getPeerCertificate(), expected);
  EXPECT_EQ(
      folly::ssl::OpenSSLUtils::getCommonName(
          const_cast<X509*>(adapter.get().getPeerCertificate())),
      "retained");
}

enum class HookCertificateCase {
  NoTransport,
  NoCertificate,
  RetainedX509,
  LiveX509,
  LiveIdentityWithoutX509,
  LiveCertificateChanges,
};

class Cpp2ClientIdentityHookParityTest
    : public testing::TestWithParam<HookCertificateCase> {};

TEST_P(Cpp2ClientIdentityHookParityTest, MatchesDirectCpp2Construction) {
  auto retained = std::make_shared<folly::ssl::BasicTransportCertificate>(
      "retained", folly::ssl::X509UniquePtr(X509_new()));
  const folly::ssl::BasicTransportCertificate live(
      "live", folly::ssl::X509UniquePtr(X509_new()));
  const folly::ssl::BasicTransportCertificate identityOnly("identity", nullptr);
  const auto scenario = GetParam();
  auto conn = makeConnContext(
      scenario == HookCertificateCase::NoCertificate ? nullptr : retained);
  testing::NiceMock<folly::test::MockAsyncTransport> transport;
  if (scenario != HookCertificateCase::NoTransport) {
    conn->setTransport(&transport);
  }
  std::vector<std::string> trace;
  int reads = 0;
  ON_CALL(transport, getPeerCertificate())
      .WillByDefault([&]() -> const folly::AsyncTransportCertificate* {
        trace.push_back("certificate:" + std::to_string(++reads));
        switch (scenario) {
          case HookCertificateCase::LiveX509:
            return &live;
          case HookCertificateCase::LiveIdentityWithoutX509:
            return &identityOnly;
          case HookCertificateCase::LiveCertificateChanges:
            return reads <= 2 ? &live : &identityOnly;
          case HookCertificateCase::NoTransport:
          case HookCertificateCase::NoCertificate:
          case HookCertificateCase::RetainedX509:
            return nullptr;
        }
      });
  apache::thrift::ClientIdentityHook hook =
      [&](const folly::AsyncTransport* seenTransport,
          X509* seenX509,
          const folly::SocketAddress& seenAddress) {
        trace.emplace_back("hook");
        EXPECT_EQ(seenAddress, conn->getPeerAddress());
        return resolveIdentities(seenTransport, seenX509, seenAddress);
      };
  const auto worker = Cpp2Worker::createDummy(nullptr, nullptr);
  Cpp2ConnContext classic(
      &conn->getPeerAddress(),
      conn->getTransport(),
      nullptr,
      folly::OpenSSLTransportCertificate::tryExtractX509(
          conn->getPeerCertificate()),
      hook,
      worker.get(),
      0);
  const auto expectedTrace = trace;
  const auto* expected =
      static_cast<ResolvedIdentities*>(classic.getPeerIdentities());
  ASSERT_NE(expected, nullptr);
  trace.clear();
  reads = 0;

  Cpp2ConnContextAdapter adapter(conn, hook);

  const auto* actual =
      static_cast<ResolvedIdentities*>(adapter.get().getPeerIdentities());
  ASSERT_NE(actual, nullptr);
  EXPECT_EQ(trace, expectedTrace);
  EXPECT_EQ(actual->transportSeen, expected->transportSeen);
  EXPECT_EQ(actual->x509Seen, expected->x509Seen);
  EXPECT_EQ(actual->certSeen, expected->certSeen);
  EXPECT_EQ(actual->identity, expected->identity);
  EXPECT_EQ(adapter.get().getPeerCertificate(), classic.getPeerCertificate());
  EXPECT_EQ(conn->getPeerIdentities(), actual);
}

INSTANTIATE_TEST_SUITE_P(
    CertificateSources,
    Cpp2ClientIdentityHookParityTest,
    testing::Values(
        HookCertificateCase::NoTransport,
        HookCertificateCase::NoCertificate,
        HookCertificateCase::RetainedX509,
        HookCertificateCase::LiveX509,
        HookCertificateCase::LiveIdentityWithoutX509,
        HookCertificateCase::LiveCertificateChanges));

TEST(Cpp2ContextAdapterTest, HookExceptionMatchesClassicAndPublishesNoState) {
  auto conn = makeConnContext(nullptr);
  using HookResult = std::unique_ptr<void, void (*)(void*)>;
  testing::MockFunction<HookResult(
      const folly::AsyncTransport*, X509*, const folly::SocketAddress&)>
      hook;
  EXPECT_CALL(hook, Call(nullptr, nullptr, conn->getPeerAddress()))
      .Times(2)
      .WillRepeatedly(
          testing::Throw(std::runtime_error("identity extraction failed")));
  const auto clientIdentityHook = hook.AsStdFunction();
  EXPECT_THROW(
      Cpp2ConnContext(
          &conn->getPeerAddress(),
          nullptr,
          nullptr,
          nullptr,
          clientIdentityHook),
      std::runtime_error);
  EXPECT_THROW(
      Cpp2ConnContextAdapter(conn, clientIdentityHook), std::runtime_error);
  EXPECT_EQ(conn->getPeerIdentities(), nullptr);
  EXPECT_EQ(tryGetCpp2ConnContext(*conn), nullptr);
}

TEST(Cpp2ContextAdapterTest, TransportExceptionPreventsHookLikeClassic) {
  auto conn = makeConnContext(nullptr);
  testing::NiceMock<folly::test::MockAsyncTransport> transport;
  conn->setTransport(&transport);
  EXPECT_CALL(transport, getPeerCertificate())
      .WillRepeatedly(
          testing::Throw(std::runtime_error("certificate unavailable")));
  int calls = 0;
  apache::thrift::ClientIdentityHook hook =
      [&](const folly::AsyncTransport* t,
          X509* cert,
          const folly::SocketAddress& peer) {
        ++calls;
        return resolveIdentities(t, cert, peer);
      };
  const auto worker = Cpp2Worker::createDummy(nullptr, nullptr);
  EXPECT_THROW(
      Cpp2ConnContext(
          &conn->getPeerAddress(),
          &transport,
          nullptr,
          nullptr,
          hook,
          worker.get(),
          0),
      std::runtime_error);
  EXPECT_THROW(Cpp2ConnContextAdapter(conn, hook), std::runtime_error);
  EXPECT_EQ(calls, 0);
  EXPECT_EQ(conn->getPeerIdentities(), nullptr);
  EXPECT_EQ(tryGetCpp2ConnContext(*conn), nullptr);
}

TEST(Cpp2ContextAdapterTest, NoHookLeavesIdentitiesUnset) {
  auto conn = makeConnContext(nullptr);

  Cpp2ConnContextAdapter adapter(conn, /*clientIdentityHook=*/nullptr);

  EXPECT_EQ(adapter.get().getPeerIdentities(), nullptr);
}

// A handler that goes to the transport — for the local address, exported
// keying material, or the peer — finds the connection's own, not a null it
// has to tolerate.
TEST(Cpp2ContextAdapterTest, ConnectionContextCarriesTheTransport) {
  folly::EventBase evb;
  auto socket = folly::AsyncSocket::newSocket(&evb);
  auto conn = makeConnContext(nullptr);
  conn->setTransport(socket.get());

  Cpp2ConnContextAdapter adapter(conn, /*clientIdentityHook=*/nullptr);

  EXPECT_EQ(adapter.get().getTransport(), socket.get());
}

TEST(Cpp2ContextAdapterTest, ConnectionCloseDetachesTheTransport) {
  folly::EventBase evb;
  auto socket = folly::AsyncSocket::newSocket(&evb);
  auto conn = makeConnContext(nullptr);
  conn->setTransport(socket.get());
  Cpp2ConnContextAdapter adapter(conn, /*clientIdentityHook=*/nullptr);
  auto cancellationToken = adapter.get().getCancellationToken();
  const folly::AsyncTransport* transportDuringCancellation = socket.get();
  folly::CancellationCallback cancellationCallback{
      cancellationToken,
      [&] { transportDuringCancellation = adapter.get().getTransport(); }};

  adapter.connectionClosed();

  EXPECT_TRUE(cancellationToken.isCancellationRequested());
  EXPECT_EQ(transportDuringCancellation, nullptr);
  EXPECT_EQ(adapter.get().getTransport(), nullptr);
}

// The protocol is stamped from the handshake snapshot, so a connection that
// downgraded still reports what it negotiated even though its transport can
// no longer say.
TEST(Cpp2ContextAdapterTest, SecurityProtocolSurvivesADowngradedTransport) {
  folly::EventBase evb;
  // Plaintext, exactly as StopTLS leaves it.
  auto socket = folly::AsyncSocket::newSocket(&evb);
  auto cert = std::make_shared<folly::ssl::BasicTransportCertificate>(
      "peer.identity", nullptr);
  auto conn = makeConnContext(cert);
  conn->setTransport(socket.get());
  ASSERT_TRUE(socket->getSecurityProtocol().empty());

  Cpp2ConnContextAdapter adapter(conn, /*clientIdentityHook=*/nullptr);

  EXPECT_TRUE(conn->peerCertReceived());
  EXPECT_TRUE(adapter.get().peerCertReceived());
  EXPECT_EQ(adapter.get().getSecurityProtocol(), "TLS1.3");
}

// A handler that reports on the client goes to the classic context for it, so
// what the setup latched has to arrive there.
TEST(Cpp2ContextAdapterTest, ClientMetadataReachesTheClassicContext) {
  auto conn = makeConnContext(nullptr);
  apache::thrift::ClientMetadata metadata;
  metadata.hostname() = "client.host";
  conn->setClientMetadata(std::move(metadata));

  Cpp2ConnContextAdapter adapter(conn, /*clientIdentityHook=*/nullptr);

  const auto seen = adapter.get().getClientMetadataRef();
  ASSERT_TRUE(seen.has_value());
  EXPECT_EQ(seen->getHostname(), "client.host");
}

// A connection whose client sent none leaves the classic context saying so,
// rather than handing back an empty record that reads as if it had one.
TEST(Cpp2ContextAdapterTest, NoClientMetadataLeavesTheClassicContextEmpty) {
  auto conn = makeConnContext(nullptr);

  Cpp2ConnContextAdapter adapter(conn, /*clientIdentityHook=*/nullptr);

  EXPECT_FALSE(adapter.get().getClientMetadataRef().has_value());
}

// Handlers written against the classic server dereference getWorker() without
// checking; its server is a different matter and they have to guard.
TEST(Cpp2ContextAdapterTest, WorkerIsPresentButBelongsToNoServer) {
  auto conn = makeConnContext(nullptr);

  Cpp2ConnContextAdapter adapter(conn, /*clientIdentityHook=*/nullptr);

  ASSERT_NE(adapter.get().getWorker(), nullptr);
  EXPECT_EQ(adapter.get().getWorker()->getServer(), nullptr);
}

TEST(Cpp2ContextAdapterTest, RequestContextCarriesHeadersMethodAndConnection) {
  auto conn = makeConnContext(nullptr);
  Cpp2ConnContextAdapter connAdapter(conn, /*clientIdentityHook=*/nullptr);

  // Declared before the request: the classic context points at this, and the
  // request reaches the classic context, so it has to outlive both.
  transport::THeader header;
  ThriftRequestContext request;
  request.installExtensions(bridgeLayout());
  request.setConnectionContext(conn);
  request.setMethodName("ping");
  request.setHeaders(ThriftRequestContext::HeaderMap{{"cat", "token"}});

  Cpp2RequestContext cpp2Request(&connAdapter.get(), &header);
  Cpp2RequestContextAdapter requestAdapter(cpp2Request, header, request);

  EXPECT_EQ(requestAdapter.get().getMethodName(), "ping");
  EXPECT_EQ(&requestAdapter.get().getMethodName(), &request.getMethodName());
  EXPECT_EQ(requestAdapter.get().getConnectionContext(), &connAdapter.get());
  ASSERT_NE(requestAdapter.get().getHeader(), nullptr);

  // The classic header borrows the native map until a handler mutates it.
  const auto& read = requestAdapter.get().getHeader()->getHeaders();
  EXPECT_EQ(&read, &request.getHeaders());
  ASSERT_TRUE(read.contains("cat"));
  EXPECT_EQ(read.at("cat"), "token");
  EXPECT_TRUE(request.getHeaders().contains("cat"));
}

TEST(Cpp2ContextAdapterTest, BorrowsAndCanReleaseTheNativeMethodName) {
  auto conn = makeConnContext(nullptr);
  Cpp2ConnContextAdapter connAdapter(conn, /*clientIdentityHook=*/nullptr);
  transport::THeader header;
  ThriftRequestContext request;
  request.installExtensions(bridgeLayout());
  request.setConnectionContext(conn);
  request.setMethodName("taoMultiShardTransactionReserveRequest");

  Cpp2RequestContext cpp2Request(&connAdapter.get(), &header);
  Cpp2RequestContextAdapter requestAdapter(cpp2Request, header, request);

  EXPECT_EQ(&cpp2Request.getMethodName(), &request.getMethodName());

  const auto released = cpp2Request.releaseMethodName();
  EXPECT_EQ(released, request.getMethodName());
  EXPECT_TRUE(cpp2Request.getMethodName().empty());
}

TEST(Cpp2ContextAdapterTest, ClassicHeaderMutationDetachesFromNativeHeaders) {
  auto conn = makeConnContext(nullptr);
  Cpp2ConnContextAdapter connAdapter(conn, /*clientIdentityHook=*/nullptr);
  transport::THeader header;
  ThriftRequestContext request;
  request.installExtensions(bridgeLayout());
  request.setConnectionContext(conn);
  request.setHeaders(ThriftRequestContext::HeaderMap{{"cat", "native"}});

  Cpp2RequestContext cpp2Request(&connAdapter.get(), &header, "ping");
  Cpp2RequestContextAdapter requestAdapter(cpp2Request, header, request);
  requestAdapter.header().setReadHeader("cat", "classic");

  EXPECT_EQ(requestAdapter.header().getHeaders().at("cat"), "classic");
  EXPECT_EQ(request.getHeaders().at("cat"), "native");
}

TEST(Cpp2ContextAdapterTest, AdapterClearsBorrowedHeadersOnDestruction) {
  auto conn = makeConnContext(nullptr);
  Cpp2ConnContextAdapter connAdapter(conn, /*clientIdentityHook=*/nullptr);
  transport::THeader header;
  ThriftRequestContext request;
  request.installExtensions(bridgeLayout());
  request.setConnectionContext(conn);
  request.setHeaders(ThriftRequestContext::HeaderMap{{"cat", "token"}});
  Cpp2RequestContext cpp2Request(&connAdapter.get(), &header, "ping");

  {
    Cpp2RequestContextAdapter requestAdapter(cpp2Request, header, request);
    EXPECT_EQ(&header.getHeaders(), &request.getHeaders());
  }

  EXPECT_TRUE(header.getHeaders().empty());
}

// What a handler writes on the way out is collected once, for the response to
// carry.
TEST(Cpp2ContextAdapterTest, WriteHeadersAreHandedBack) {
  auto conn = makeConnContext(nullptr);
  Cpp2ConnContextAdapter connAdapter(conn, /*clientIdentityHook=*/nullptr);
  ThriftRequestContext request;
  request.installExtensions(bridgeLayout());
  request.setConnectionContext(conn);

  transport::THeader header;
  Cpp2RequestContext cpp2Request(&connAdapter.get(), &header, "ping");
  Cpp2RequestContextAdapter requestAdapter(cpp2Request, header, request);
  requestAdapter.header().setHeader("minted", "sat");

  auto written = requestAdapter.takeWriteHeaders();

  ASSERT_TRUE(written.contains("minted"));
  EXPECT_EQ(written.at("minted"), "sat");
  EXPECT_TRUE(requestAdapter.takeWriteHeaders().empty());
}

TEST(Cpp2ContextAdapterTest, AdapterUsesTheNativeRequestAmbientContext) {
  auto conn = makeConnContext(nullptr);
  Cpp2ConnContextAdapter connAdapter(conn, /*clientIdentityHook=*/nullptr);
  ThriftRequestContext firstRequest;
  firstRequest.installExtensions(bridgeLayout());
  firstRequest.setConnectionContext(conn);
  ThriftRequestContext secondRequest;
  secondRequest.installExtensions(bridgeLayout());
  secondRequest.setConnectionContext(conn);

  transport::THeader firstHeader;
  transport::THeader secondHeader;
  Cpp2RequestContext firstCpp2(&connAdapter.get(), &firstHeader, "ping");
  Cpp2RequestContext secondCpp2(&connAdapter.get(), &secondHeader, "ping");
  Cpp2RequestContextAdapter first(firstCpp2, firstHeader, firstRequest);
  Cpp2RequestContextAdapter second(secondCpp2, secondHeader, secondRequest);

  EXPECT_EQ(
      first.ambientContext(), firstRequest.getFollyRequestContext().capture());
  EXPECT_EQ(
      second.ambientContext(),
      secondRequest.getFollyRequestContext().capture());
  EXPECT_NE(first.ambientContext(), second.ambientContext());
  EXPECT_NE(first.ambientContext().get(), folly::RequestContext::try_get());
}

TEST(Cpp2ContextAdapterTest, ClearedAmbientContextFallsBackToRequestContext) {
  auto conn = makeConnContext(nullptr);
  Cpp2ConnContextAdapter connAdapter(conn, /*clientIdentityHook=*/nullptr);
  ThriftRequestContext request;
  request.installExtensions(bridgeLayout());
  request.setConnectionContext(conn);

  transport::THeader header;
  Cpp2RequestContext cpp2Request(&connAdapter.get(), &header, "ping");
  Cpp2RequestContextAdapter requestAdapter(cpp2Request, header, request);
  const auto ambient = requestAdapter.ambientContext();

  folly::RequestContextSaverScopeGuard guard;
  folly::RequestContext::setContext(nullptr);
  ASSERT_EQ(folly::RequestContext::try_get(), nullptr);

  requestAdapter.captureAmbientContext();

  EXPECT_EQ(requestAdapter.ambientContext(), ambient);
  EXPECT_EQ(folly::RequestContext::try_get(), ambient.get());
}

// The identities the resolver produced are reachable natively, so a reader
// does not need the classic context to find out who the peer is.
TEST(Cpp2ContextAdapterTest, PeerIdentitiesAreReachableFromTheConnection) {
  auto cert = std::make_shared<folly::ssl::BasicTransportCertificate>(
      "peer.identity", nullptr);
  auto conn = makeConnContext(cert);
  testing::NiceMock<folly::test::MockAsyncTransport> transport;
  ON_CALL(transport, getPeerCertificate())
      .WillByDefault(testing::Return(cert.get()));
  conn->setTransport(&transport);
  EXPECT_EQ(conn->getPeerIdentities(), nullptr);

  {
    Cpp2ConnContextAdapter connAdapter(conn, &resolveIdentities);

    ASSERT_NE(conn->getPeerIdentities(), nullptr);
    EXPECT_EQ(conn->getPeerIdentities(), connAdapter.get().getPeerIdentities());
    EXPECT_EQ(
        static_cast<ResolvedIdentities*>(conn->getPeerIdentities())->identity,
        "peer.identity");
  }
  EXPECT_EQ(conn->getPeerIdentities(), nullptr);
}

TEST(Cpp2ContextAdapterTest, ClassicContextIsReachableThroughTheConnection) {
  auto conn = makeConnContext(nullptr);
  EXPECT_EQ(tryGetCpp2ConnContext(*conn), nullptr);

  {
    Cpp2ConnContextAdapter connAdapter(conn, /*clientIdentityHook=*/nullptr);
    EXPECT_EQ(tryGetCpp2ConnContext(*conn), &connAdapter.get());
  }
  EXPECT_EQ(tryGetCpp2ConnContext(*conn), nullptr);
}

// The classic context is reachable while the bridge owns its adapter and is
// revoked before that adapter's extension state is destroyed.
TEST(Cpp2ContextAdapterTest, ClassicContextIsReachableThroughTheRequest) {
  auto conn = makeConnContext(nullptr);
  Cpp2ConnContextAdapter connAdapter(conn, /*clientIdentityHook=*/nullptr);
  transport::THeader header;
  ThriftRequestContext request;
  request.installExtensions(bridgeLayout());
  request.setConnectionContext(conn);
  request.setMethodName("ping");
  EXPECT_EQ(tryGetCpp2RequestContext(request), nullptr);

  Cpp2RequestContext cpp2Request(&connAdapter.get(), &header);
  Cpp2BridgeRequestState state{
      .context = &cpp2Request, .exception = nullptr, .owner = nullptr};
  request.setState<Cpp2BridgeExtension>(&state);
  {
    Cpp2RequestContextAdapter requestAdapter(cpp2Request, header, request);
    EXPECT_EQ(tryGetCpp2RequestContext(request), &requestAdapter.get());
  }
  request.setState<Cpp2BridgeExtension>(nullptr);
  EXPECT_EQ(tryGetCpp2RequestContext(request), nullptr);
}

// Releasing a state empties the slot rather than leaving it pointing at
// storage the next request rebuilds.
TEST(Cpp2ContextAdapterTest, ClearingTheSlotLeavesNothingToRead) {
  auto conn = makeConnContext(nullptr);
  Cpp2ConnContextAdapter connAdapter(conn, /*clientIdentityHook=*/nullptr);
  transport::THeader header;
  ThriftRequestContext request;
  request.installExtensions(bridgeLayout());
  request.setConnectionContext(conn);

  Cpp2RequestContext cpp2Request(&connAdapter.get(), &header, "ping");
  Cpp2BridgeRequestState state{
      .context = &cpp2Request, .exception = nullptr, .owner = nullptr};
  request.setState<Cpp2BridgeExtension>(&state);
  Cpp2RequestContextAdapter requestAdapter(cpp2Request, header, request);
  ASSERT_NE(tryGetCpp2RequestContext(request), nullptr);

  requestAdapter.ftContext().setState<Cpp2BridgeExtension>(nullptr);

  EXPECT_EQ(tryGetCpp2RequestContext(request), nullptr);
}

} // namespace apache::thrift::fast_thrift::thrift::server
