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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <future>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <pthread.h>
#include <sched.h>

#include <fizz/client/AsyncFizzClient.h>
#include <fizz/client/FizzClientContext.h>
#include <folly/ExceptionWrapper.h>
#include <folly/ScopeGuard.h>
#include <folly/SocketAddress.h>
#include <folly/Synchronized.h>
#include <folly/futures/Future.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/Request.h>
#include <folly/io/async/ScopedEventBaseThread.h>
#include <folly/synchronization/Baton.h>

#include <thrift/lib/cpp/TApplicationException.h>
#include <thrift/lib/cpp/Thrift.h>
#include <thrift/lib/cpp2/Flags.h>
#include <thrift/lib/cpp2/async/ResponseChannel.h>
#include <thrift/lib/cpp2/async/RocketClientChannel.h>
#include <thrift/lib/cpp2/async/RpcOptions.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/connection/util/ReusePortBpfProgram.h>
#include <thrift/lib/cpp2/fast_thrift/security/FizzServerCertConfig.h>
#include <thrift/lib/cpp2/fast_thrift/security/test/TestCert.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/FastThriftServer.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/extension/ThriftExtension.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/extension/ThriftExtensionPipelineHandler.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/framework/FastServerModule.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/framework/NativeThriftHandlerAllowlist.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/framework/ThriftPipelineHandler.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/test/if/gen-cpp2/FastThriftServer.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/test/if/gen-cpp2/FastThriftServerAsyncClient.h>
#include <thrift/lib/cpp2/security/extensions/ThriftParametersClientExtension.h>
#include <thrift/lib/cpp2/security/extensions/ThriftParametersContext.h>
#include <thrift/lib/cpp2/transport/core/RpcMetadataUtil.h>
#include <thrift/lib/thrift/gen-cpp2/RpcMetadata_types.h>

THRIFT_FLAG_DECLARE_bool(rocket_client_binary_rpc_metadata_encoding);

namespace apache::thrift::fast_thrift::thrift::test::integration::test {

namespace ftt = ::apache::thrift::fast_thrift::thrift;
namespace integration =
    ::apache::thrift::fast_thrift::thrift::test::integration;
using ::apache::thrift::FastServiceHandler;
using ::apache::thrift::fast_thrift::thrift::test::integration::EchoResponse;
using ::apache::thrift::fast_thrift::thrift::test::integration::
    NotFoundException;
using ::apache::thrift::fast_thrift::thrift::test::integration::
    PermissionDeniedException;

namespace {

struct FollyContextMarker : folly::RequestData {
  bool hasCallback() override { return false; }
};

const folly::RequestToken& follyContextMarkerToken() {
  static const folly::RequestToken token{"fast_thrift_server_test_marker"};
  return token;
}

bool hasFollyContextMarker() {
  auto* context = folly::RequestContext::try_get();
  return context != nullptr &&
      context->getContextData(follyContextMarkerToken()) != nullptr;
}

struct FollyContextObservations {
  std::atomic<folly::RequestContext*> request{nullptr};
  std::atomic<folly::RequestContext*> application{nullptr};
  std::atomic<folly::RequestContext*> response{nullptr};
  std::atomic<bool> applicationSawMarker{false};
  std::atomic<bool> responseSawMarker{false};
};

// User-implemented FastServiceHandler. Test sets flags to control behavior;
// methods call cb->result()/done()/exception() synchronously.
class TestHandler : public FastServiceHandler<integration::FastThriftServer> {
 public:
  bool throwNotFound{false};
  bool throwPermissionDenied{false};
  bool throwUndeclaredDirectly{false};

  void async_tm_ping(ftt::FastHandlerCallbackPtr<void> cb) override {
    cb->done();
  }

  void async_tm_add(
      ftt::FastHandlerCallbackPtr<int64_t> cb, int64_t a, int64_t b) override {
    cb->result(a + b);
  }

  void async_tm_echo(
      ftt::FastHandlerCallbackPtr<std::unique_ptr<EchoResponse>> cb,
      std::unique_ptr<std::string> message) override {
    auto resp = std::make_unique<EchoResponse>();
    resp->message() = std::string("echoed:") + *message;
    cb->result(std::move(resp));
  }

  void async_tm_lookup(
      ftt::FastHandlerCallbackPtr<std::unique_ptr<EchoResponse>> cb,
      int32_t id) override {
    if (throwUndeclaredDirectly) {
      throw std::runtime_error("direct handler failure");
    }
    if (throwNotFound) {
      NotFoundException ex;
      ex.id() = id;
      ex.message() = "not found";
      cb->exception(
          folly::make_exception_wrapper<NotFoundException>(std::move(ex)));
      return;
    }
    auto resp = std::make_unique<EchoResponse>();
    resp->message() = "found";
    cb->result(std::move(resp));
  }

  void async_tm_secureLookup(
      ftt::FastHandlerCallbackPtr<std::unique_ptr<EchoResponse>> cb,
      int32_t /*id*/,
      std::unique_ptr<std::string> user) override {
    if (throwPermissionDenied) {
      PermissionDeniedException ex;
      ex.user() = *user;
      ex.reason() = "no perms";
      cb->exception(
          folly::make_exception_wrapper<PermissionDeniedException>(
              std::move(ex)));
      return;
    }
    auto resp = std::make_unique<EchoResponse>();
    resp->message() = "ok";
    cb->result(std::move(resp));
  }
};

class FollyContextObservingHandler : public TestHandler {
 public:
  explicit FollyContextObservingHandler(FollyContextObservations& observations)
      : observations_(observations) {}

  void async_tm_add(
      ftt::FastHandlerCallbackPtr<int64_t> cb, int64_t a, int64_t b) override {
    observations_.application.store(
        folly::RequestContext::try_get(), std::memory_order_relaxed);
    observations_.applicationSawMarker.store(
        hasFollyContextMarker(), std::memory_order_relaxed);

    folly::RequestContextScopeGuard unrelatedCompletionContext;
    cb->result(a + b);
  }

 private:
  FollyContextObservations& observations_;
};

// Handler that retains the ping callback instead of completing it. Tests
// drive completion on their own schedule, after stop() has begun, to
// reproduce the race where an in-flight FHC would write back through a
// freed adapter.
class DeferredPingHandler
    : public FastServiceHandler<integration::FastThriftServer> {
 public:
  void async_tm_ping(ftt::FastHandlerCallbackPtr<void> cb) override {
    evb_ = cb->getEventBase();
    callback_ = std::move(cb);
    pingStarted_.post();
  }

  ftt::FastHandlerCallbackPtr<void> takeCallback() {
    return std::move(callback_);
  }

  folly::EventBase* getEventBase() const { return evb_; }

  folly::Baton<> pingStarted_;

 private:
  ftt::FastHandlerCallbackPtr<void> callback_;
  folly::EventBase* evb_{nullptr};
};

class ConcurrencyLimitedPingHandler
    : public FastServiceHandler<integration::FastThriftServer> {
 public:
  void async_tm_ping(ftt::FastHandlerCallbackPtr<void> cb) override {
    evb_ = cb->getEventBase();
    callbacks_.push_back(std::move(cb));
    const auto started = started_.fetch_add(1) + 1;
    if (started == 1) {
      firstStarted_.post();
    } else if (started == 2) {
      secondStarted_.set_value();
    }
  }

  void completeNext() {
    auto callback = std::move(callbacks_.front());
    callbacks_.erase(callbacks_.begin());
    std::move(callback)->done();
  }

  folly::EventBase* getEventBase() const { return evb_; }
  size_t started() const { return started_.load(); }

  folly::Baton<> firstStarted_;
  std::promise<void> secondStarted_;

 private:
  std::vector<ftt::FastHandlerCallbackPtr<void>> callbacks_;
  folly::EventBase* evb_{nullptr};
  std::atomic<size_t> started_{0};
};

// Establishes a fizz-client connection to `address` and returns the negotiated
// AsyncTransport. Blocks until handshake completes; returns nullptr on error.
folly::AsyncTransport::UniquePtr connectFizz(
    folly::EventBase* evb, const folly::SocketAddress& address) {
  struct Cb : public folly::AsyncSocket::ConnectCallback,
              public fizz::client::AsyncFizzClient::HandshakeCallback {
    folly::Baton<>& baton;
    folly::exception_wrapper& outErr;
    fizz::client::AsyncFizzClient::UniquePtr& outClient;
    Cb(folly::Baton<>& b,
       folly::exception_wrapper& e,
       fizz::client::AsyncFizzClient::UniquePtr& c)
        : baton(b), outErr(e), outClient(c) {}

    void connectSuccess() noexcept override {
      outClient->connect(
          this,
          /*verifier=*/nullptr,
          /*sni=*/folly::none,
          /*pskIdentity=*/folly::none,
          /*echConfigs=*/folly::none);
    }
    void connectErr(const folly::AsyncSocketException& ex) noexcept override {
      outErr = folly::exception_wrapper(ex);
      baton.post();
    }
    void fizzHandshakeSuccess(
        fizz::client::AsyncFizzClient*) noexcept override {
      baton.post();
    }
    void fizzHandshakeError(
        fizz::client::AsyncFizzClient*,
        folly::exception_wrapper ex) noexcept override {
      outErr = std::move(ex);
      baton.post();
    }
  };

  folly::Baton<> done;
  folly::exception_wrapper err;
  fizz::client::AsyncFizzClient::UniquePtr fizzClient;
  Cb cb(done, err, fizzClient);

  evb->runInEventBaseThreadAndWait([&] {
    auto sock = folly::AsyncSocket::newSocket(evb);
    auto fizzCtx = std::make_shared<fizz::client::FizzClientContext>();
    auto* sockPtr = sock.get();
    fizzClient.reset(
        new fizz::client::AsyncFizzClient(std::move(sock), fizzCtx));
    sockPtr->connect(&cb, address, /*timeout=*/std::chrono::seconds{5}.count());
  });

  done.wait();
  if (err) {
    return nullptr;
  }
  return folly::AsyncTransport::UniquePtr(fizzClient.release());
}

// Performs a fizz handshake that negotiates StopTLS V1 via the Thrift TLS
// extension. After the server tears down TLS, surfaces the underlying
// plaintext folly::AsyncSocket via connect()'s return value.
class FizzStopTLSConnector
    : public fizz::client::AsyncFizzClient::HandshakeCallback,
      public fizz::AsyncFizzBase::EndOfTLSCallback {
 public:
  ~FizzStopTLSConnector() override {
    // AsyncFizzClient uses DelayedDestruction — must be released on the
    // EVB thread that drove the handshake.
    if (client_ && evb_) {
      evb_->runInEventBaseThread([c = std::move(client_)]() mutable {});
    }
  }

  // Initiates the fizz handshake on `evb`'s thread and blocks the caller
  // until the StopTLS downgrade completes. Caller MUST NOT be running on
  // `evb`'s thread (this would deadlock by double-driving the EventBase).
  folly::AsyncSocket::UniquePtr connect(
      const folly::SocketAddress& address, folly::EventBase* evb) {
    evb_ = evb;

    evb->runInEventBaseThreadAndWait([&] {
      auto sock = folly::AsyncSocket::newSocket(evb_, address);
      auto ctx = std::make_shared<fizz::client::FizzClientContext>();
      ctx->setSupportedAlpns({"rs"});
      auto thriftParametersContext =
          std::make_shared<apache::thrift::ThriftParametersContext>();
      thriftParametersContext->setUseStopTLS(true);
      auto extension =
          std::make_shared<apache::thrift::ThriftParametersClientExtension>(
              thriftParametersContext);

      client_.reset(new fizz::client::AsyncFizzClient(
          std::move(sock), std::move(ctx), std::move(extension)));
      client_->connect(
          this,
          /*verifier=*/nullptr,
          /*sni=*/folly::none,
          /*pskIdentity=*/folly::none,
          folly::Optional<std::vector<fizz::ech::ParsedECHConfig>>(folly::none),
          /*timeout=*/std::chrono::seconds{5});
    });
    return std::move(promise_).getSemiFuture().get();
  }

  void fizzHandshakeSuccess(
      fizz::client::AsyncFizzClient* client) noexcept override {
    client->setEndOfTLSCallback(this);
  }

  void fizzHandshakeError(
      fizz::client::AsyncFizzClient* /*unused*/,
      folly::exception_wrapper ex) noexcept override {
    promise_.setException(std::move(ex));
  }

  void endOfTLS(
      fizz::AsyncFizzBase* transport,
      std::unique_ptr<folly::IOBuf> /*postData*/) override {
    auto* sock = transport->getUnderlyingTransport<folly::AsyncSocket>();
    DCHECK(sock);
    auto fd = sock->detachNetworkSocket();
    auto zcId = sock->getZeroCopyBufId();
    auto plaintext =
        folly::AsyncSocket::UniquePtr(new folly::AsyncSocket(evb_, fd, zcId));
    promise_.setValue(std::move(plaintext));
  }

 private:
  fizz::client::AsyncFizzClient::UniquePtr client_;
  folly::Promise<folly::AsyncSocket::UniquePtr> promise_;
  folly::EventBase* evb_{nullptr};
};

} // namespace

class FastThriftServerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    THRIFT_FLAG_SET_MOCK(rocket_client_binary_rpc_metadata_encoding, true);

    handler_ = std::make_shared<TestHandler>();

    ftt::FastThriftServerConfig config;
    config.address = folly::SocketAddress("::1", 0);
    config.numIOThreads = 1;
    config.numConnectionSetupThreads = 1;
    if (enableChecksum_) {
      config.enableChecksum = true;
    }
    if (enableStreamMux_) {
      config.enableStreamMux = true;
    }

    server_ = std::make_unique<ftt::FastThriftServer>(std::move(config));
    server_->setInterface(handler_);
    server_->start();
    ASSERT_TRUE(server_->isRunning());

    clientThread_ = std::make_unique<folly::ScopedEventBaseThread>();
  }

  void TearDown() override {
    clientThread_.reset();
    server_->stop();
    server_.reset();
  }

  std::unique_ptr<apache::thrift::Client<integration::FastThriftServer>>
  createClient(bool enableCompression = false) {
    auto* evb = clientThread_->getEventBase();
    std::unique_ptr<apache::thrift::Client<integration::FastThriftServer>>
        client;
    evb->runInEventBaseThreadAndWait([&] {
      auto socket = folly::AsyncSocket::newSocket(evb, server_->getAddress());
      auto channel =
          apache::thrift::RocketClientChannel::newChannel(std::move(socket));
      if (enableCompression) {
        apache::thrift::CompressionConfig compressionConfig;
        compressionConfig.codecConfig().ensure().set_zlibConfig();
        channel->setDesiredCompressionConfig(std::move(compressionConfig));
      }
      client = std::make_unique<
          apache::thrift::Client<integration::FastThriftServer>>(
          std::move(channel));
    });
    return client;
  }

  void destroyClientOnEvb(
      std::unique_ptr<apache::thrift::Client<integration::FastThriftServer>>&
          client) {
    clientThread_->getEventBase()->runInEventBaseThreadAndWait(
        [&] { client.reset(); });
  }

  // Runs a semifuture-returning lambda on the client EventBase and blocks
  // until the result is available. Returns the value on success and fails
  // the test on RPC error.
  template <typename Fn>
  auto syncCall(Fn&& fn) {
    using SemiFuture = std::invoke_result_t<Fn>;
    using T = typename SemiFuture::value_type;
    if constexpr (std::is_void_v<T>) {
      folly::Baton<> baton;
      auto* evb = clientThread_->getEventBase();
      evb->runInEventBaseThread([&] {
        fn().via(evb)
            .thenValue([&](folly::Unit) { baton.post(); })
            .thenError([&](const folly::exception_wrapper& ew) {
              ADD_FAILURE() << "RPC failed: " << folly::exceptionStr(ew);
              baton.post();
            });
      });
      baton.wait();
    } else {
      folly::Baton<> baton;
      T result{};
      auto* evb = clientThread_->getEventBase();
      evb->runInEventBaseThread([&] {
        fn().via(evb)
            .thenValue([&](T val) {
              result = std::move(val);
              baton.post();
            })
            .thenError([&](const folly::exception_wrapper& ew) {
              ADD_FAILURE() << "RPC failed: " << folly::exceptionStr(ew);
              baton.post();
            });
      });
      baton.wait();
      return result;
    }
  }

  // Like syncCall but expects the RPC to fail with the templated exception
  // type. Returns void; fails the test if the RPC succeeds or throws a
  // different exception type.
  template <typename ExpectedEx, typename Fn>
  void syncCallExpectException(Fn&& fn) {
    folly::Baton<> baton;
    bool gotExpected = false;
    std::string actual;
    auto* evb = clientThread_->getEventBase();
    evb->runInEventBaseThread([&] {
      fn().via(evb)
          .thenValue([&](auto&&) {
            ADD_FAILURE() << "RPC succeeded; expected exception "
                          << typeid(ExpectedEx).name();
            baton.post();
          })
          .thenError([&](const folly::exception_wrapper& ew) {
            if (ew.with_exception([](const ExpectedEx&) {})) {
              gotExpected = true;
            } else {
              actual = folly::exceptionStr(ew).toStdString();
            }
            baton.post();
          });
    });
    baton.wait();
    EXPECT_TRUE(gotExpected)
        << "Expected " << typeid(ExpectedEx).name() << " got: " << actual;
  }

  template <typename ExpectedEx, typename Fn>
  void syncCallExpectExceptionMessage(
      Fn&& fn, std::string_view expectedMessagePart) {
    folly::Baton<> baton;
    bool gotExpected = false;
    std::string actual;
    auto* evb = clientThread_->getEventBase();
    evb->runInEventBaseThread([&] {
      fn().via(evb)
          .thenValue([&](auto&&) {
            ADD_FAILURE() << "RPC succeeded; expected exception "
                          << typeid(ExpectedEx).name();
            baton.post();
          })
          .thenError([&](const folly::exception_wrapper& ew) {
            if (const auto* ex = ew.get_exception<ExpectedEx>()) {
              gotExpected = true;
              actual = ex->what();
            } else {
              actual = folly::exceptionStr(ew).toStdString();
            }
            baton.post();
          });
    });
    baton.wait();
    EXPECT_TRUE(gotExpected)
        << "Expected " << typeid(ExpectedEx).name() << " got: " << actual;
    EXPECT_NE(actual.find(expectedMessagePart), std::string::npos);
  }

  std::shared_ptr<TestHandler> handler_;
  std::unique_ptr<ftt::FastThriftServer> server_;
  std::unique_ptr<folly::ScopedEventBaseThread> clientThread_;
  bool enableChecksum_{false};
  bool enableStreamMux_{false};
};

// ---------------------------------------------------------------------------
// Success paths over real loopback rocket client
// ---------------------------------------------------------------------------

TEST_F(FastThriftServerTest, PingRoundTrip) {
  auto client = createClient();
  syncCall([&] { return client->semifuture_ping(); });
  destroyClientOnEvb(client);
}

TEST(FastThriftServerReusePortTest, RoundRobinConnectionsCompleteRpcs) {
  constexpr size_t kNumWorkers = 3;
  constexpr size_t kNumConnections = 30;
  using connection::ReusePortBpfMode;
  using connection::ReusePortBpfProgram;

  if (!ReusePortBpfProgram::createRoundRobin(kNumWorkers)) {
    GTEST_SKIP() << "BPF program loading is unavailable";
  }
  THRIFT_FLAG_SET_MOCK(rocket_client_binary_rpc_metadata_encoding, true);

  class RecordingHandler final : public TestHandler {
   public:
    void async_tm_ping(ftt::FastHandlerCallbackPtr<void> cb) override {
      workers.withWLock(
          [&](auto& evbs) { evbs.push_back(cb->getEventBase()); });
      cb->done();
    }

    folly::Synchronized<std::vector<folly::EventBase*>> workers;
  };
  auto handler = std::make_shared<RecordingHandler>();
  ftt::FastThriftServerConfig config;
  config.address = folly::SocketAddress("::1", 0);
  config.numIOThreads = kNumWorkers;
  config.numConnectionSetupThreads = 0;
  ftt::FastThriftServer server(std::move(config));
  server.setInterface(handler);
  server.setReusePortBpfMode(ReusePortBpfMode::RoundRobin);
  server.start();

  folly::ScopedEventBaseThread clientThread;
  auto* clientEvb = clientThread.getEventBase();
  cpu_set_t originalAffinity;
  int affinityError = 0;
  bool affinityPinned = false;
  SCOPE_EXIT {
    if (affinityPinned) {
      clientEvb->runInEventBaseThreadAndWait([&] {
        EXPECT_EQ(
            ::pthread_setaffinity_np(
                ::pthread_self(), sizeof(originalAffinity), &originalAffinity),
            0);
      });
    }
  };
  clientEvb->runInEventBaseThreadAndWait([&] {
    affinityError = ::pthread_getaffinity_np(
        ::pthread_self(), sizeof(originalAffinity), &originalAffinity);
    if (affinityError != 0) {
      return;
    }
    const int cpu = ::sched_getcpu();
    ASSERT_GE(cpu, 0);
    cpu_set_t oneCpu;
    CPU_ZERO(&oneCpu);
    CPU_SET(cpu, &oneCpu);
    affinityError =
        ::pthread_setaffinity_np(::pthread_self(), sizeof(oneCpu), &oneCpu);
    affinityPinned = affinityError == 0;
  });
  if (!affinityPinned) {
    GTEST_SKIP() << "Unable to pin client thread: " << affinityError;
  }

  for (size_t i = 0; i < kNumConnections; ++i) {
    SCOPED_TRACE(i);
    std::unique_ptr<apache::thrift::Client<integration::FastThriftServer>>
        client;
    SCOPE_EXIT {
      clientEvb->runInEventBaseThreadAndWait([&] { client.reset(); });
    };
    auto reply = folly::SemiFuture<folly::Unit>::makeEmpty();
    clientEvb->runInEventBaseThreadAndWait([&] {
      auto socket =
          folly::AsyncSocket::newSocket(clientEvb, server.getAddress());
      auto channel =
          apache::thrift::RocketClientChannel::newChannel(std::move(socket));
      client = std::make_unique<
          apache::thrift::Client<integration::FastThriftServer>>(
          std::move(channel));
      apache::thrift::RpcOptions options;
      options.setTimeout(std::chrono::seconds(2));
      reply = client->semifuture_ping(options);
    });
    ASSERT_NO_THROW(std::move(reply).get(std::chrono::seconds(5)));
  }

  const auto workers = handler->workers.copy();
  ASSERT_EQ(workers.size(), kNumConnections);
  const auto serverWorkers = server.getIOThreadPool()->getAllEventBases();
  EXPECT_EQ(
      (std::set<folly::EventBase*>(
           workers.begin(), workers.begin() + kNumWorkers)
           .size()),
      kNumWorkers);
  for (size_t i = kNumWorkers; i < workers.size(); ++i) {
    EXPECT_EQ(workers[i], workers[i % kNumWorkers]) << "connection " << i;
  }
  for (const auto& worker : serverWorkers) {
    EXPECT_EQ(
        std::count(workers.begin(), workers.end(), worker.get()),
        kNumConnections / kNumWorkers);
  }
}

TEST_F(FastThriftServerTest, AddReturnsSum) {
  auto client = createClient();
  auto result = syncCall([&] { return client->semifuture_add(7, 35); });
  EXPECT_EQ(result, 42);
  destroyClientOnEvb(client);
}

TEST_F(FastThriftServerTest, EchoReturnsComplex) {
  auto client = createClient();
  auto resp = syncCall([&] { return client->semifuture_echo("hello"); });
  EXPECT_EQ(*resp.message(), "echoed:hello");
  destroyClientOnEvb(client);
}

// ---------------------------------------------------------------------------
// Declared exception path round-trips a typed exception
// ---------------------------------------------------------------------------

TEST_F(FastThriftServerTest, LookupDeclaredExceptionPropagates) {
  handler_->throwNotFound = true;
  auto client = createClient();
  syncCallExpectException<NotFoundException>(
      [&] { return client->semifuture_lookup(/*id=*/99); });
  destroyClientOnEvb(client);
}

TEST_F(FastThriftServerTest, SecureLookupSecondExceptionPropagates) {
  handler_->throwPermissionDenied = true;
  auto client = createClient();
  syncCallExpectException<PermissionDeniedException>([&] {
    return client->semifuture_secureLookup(/*id=*/5, std::string("alice"));
  });
  destroyClientOnEvb(client);
}

TEST_F(FastThriftServerTest, DirectHandlerThrowPreservesMessage) {
  handler_->throwUndeclaredDirectly = true;
  auto client = createClient();
  syncCallExpectExceptionMessage<apache::thrift::TApplicationException>(
      [&] { return client->semifuture_lookup(1); }, "direct handler failure");
  destroyClientOnEvb(client);
}

// ---------------------------------------------------------------------------
// Stream mux handler wired into the real server pipeline (enableStreamMux).
// ---------------------------------------------------------------------------

class FastThriftServerStreamMuxTest : public FastThriftServerTest {
 protected:
  void SetUp() override {
    enableStreamMux_ = true;
    FastThriftServerTest::SetUp();
  }
};

// With the stream mux handler in the pipeline, ordinary unary requests must
// still round-trip untouched — the mux forwards non-stream traffic to the app.
// (End-to-end stream behavior is covered by the mux's own unit test; the client
// stream path is not wired yet.)
TEST_F(FastThriftServerStreamMuxTest, UnaryTrafficRoundTripsWithStreamMux) {
  auto client = createClient();
  auto resp = syncCall([&] { return client->semifuture_echo("muxed"); });
  EXPECT_EQ(*resp.message(), "echoed:muxed");
  destroyClientOnEvb(client);
}

// ---------------------------------------------------------------------------
// Checksum handler wired into the real server pipeline (enableChecksum).
// ---------------------------------------------------------------------------

class FastThriftServerChecksumTest : public FastThriftServerTest {
 protected:
  void SetUp() override {
    enableChecksum_ = true;
    FastThriftServerTest::SetUp();
  }
};

// With the checksum handler in the pipeline, ordinary (no-checksum) requests
// must still round-trip untouched.
TEST_F(FastThriftServerChecksumTest, NonChecksumTrafficRoundTrips) {
  auto client = createClient();
  auto resp = syncCall([&] { return client->semifuture_echo("plain"); });
  EXPECT_EQ(*resp.message(), "echoed:plain");
  destroyClientOnEvb(client);
}

// A request carrying an XXH3_64 checksum is validated by the server handler and
// the response echoes a checksum the client accepts — a full round-trip.
TEST_F(FastThriftServerChecksumTest, XXH3ChecksumRequestRoundTrips) {
  auto client = createClient();
  apache::thrift::RpcOptions options;
  options.setChecksum(apache::thrift::RpcOptions::Checksum::XXH3_64);
  auto resp =
      syncCall([&] { return client->semifuture_echo(options, "checksummed"); });
  EXPECT_EQ(*resp.message(), "echoed:checksummed");
  destroyClientOnEvb(client);
}

TEST_F(FastThriftServerChecksumTest, CompressionAndXXH3ChecksumRoundTrip) {
  auto client = createClient(/*enableCompression=*/true);
  apache::thrift::RpcOptions options;
  options.setChecksum(apache::thrift::RpcOptions::Checksum::XXH3_64);
  auto resp = syncCall(
      [&] { return client->semifuture_echo(options, "compressed-checksum"); });
  EXPECT_EQ(*resp.message(), "echoed:compressed-checksum");
  destroyClientOnEvb(client);
}

// Regression for the lifetime bug fixed by graceful drain. Before the fix,
// stop() synchronously dropped per-connection FastConnection state
// (including the ThriftServerAppAdapter) while a handler still held a
// FastHandlerCallbackPtr. The deferred completion then wrote a response
// through the freed adapter — a UAF caught by ASAN/TSAN. After the fix,
// stop() drives onException on every adapter;
// ThriftServerConnectionCloseHandler holds the pipeline open until in-flight
// responses drain, and stop() blocks on connectionsDrainedBaton_ until every
// adapter has fired its closeCallback. The retained callback can safely
// complete during stop.
TEST(
    FastThriftServerStandaloneTest,
    DeferredPingCallbackAfterStopDoesNotUseFreedAdapter) {
  THRIFT_FLAG_SET_MOCK(rocket_client_binary_rpc_metadata_encoding, true);

  auto handler = std::make_shared<DeferredPingHandler>();
  ftt::FastThriftServerConfig config;
  config.address = folly::SocketAddress("::1", 0);
  config.numIOThreads = 1;

  ftt::FastThriftServer server(std::move(config));
  server.setInterface(handler);
  server.start();

  folly::ScopedEventBaseThread clientThread;
  auto* evb = clientThread.getEventBase();
  std::unique_ptr<apache::thrift::Client<integration::FastThriftServer>> client;
  evb->runInEventBaseThreadAndWait([&] {
    auto socket = folly::AsyncSocket::newSocket(evb, server.getAddress());
    auto channel =
        apache::thrift::RocketClientChannel::newChannel(std::move(socket));
    client =
        std::make_unique<apache::thrift::Client<integration::FastThriftServer>>(
            std::move(channel));
  });

  folly::Baton<> clientDone;
  evb->runInEventBaseThread([&] {
    client->semifuture_ping()
        .via(evb)
        .thenValue([&](folly::Unit) { clientDone.post(); })
        .thenError([&](const folly::exception_wrapper&) { clientDone.post(); });
  });

  ASSERT_TRUE(handler->pingStarted_.try_wait_for(std::chrono::seconds{5}));

  std::thread stopThread([&] { server.stop(); });

  // stop() may close the wire, but the retained in-flight callback must not
  // dereference a freed per-connection adapter when it completes.
  ASSERT_NE(handler->getEventBase(), nullptr);
  handler->getEventBase()->runInEventBaseThreadAndWait([&] {
    auto callback = handler->takeCallback();
    ASSERT_NE(callback, nullptr);
    std::move(callback)->done();
  });
  stopThread.join();

  evb->runInEventBaseThreadAndWait([&] { client.reset(); });
}

TEST(
    FastThriftServerStandaloneTest,
    RequestConcurrencyLimitsAreSharedAcrossConnections) {
  THRIFT_FLAG_SET_MOCK(rocket_client_binary_rpc_metadata_encoding, true);

  auto verify = []<typename Server>() {
    auto handler = std::make_shared<ConcurrencyLimitedPingHandler>();
    auto secondStarted = handler->secondStarted_.get_future();
    ftt::FastThriftServerConfig config;
    config.address = folly::SocketAddress("::1", 0);
    config.numIOThreads = 1;
    config.requestConcurrencyLimits = ftt::RequestConcurrencyLimits{
        .maxActiveRequests = 1,
        .maxOutstandingRequests = 2,
    };

    Server server(std::move(config));
    server.setInterface(handler);
    server.start();

    folly::ScopedEventBaseThread clientThread;
    auto* clientEvb = clientThread.getEventBase();
    using Client = apache::thrift::Client<integration::FastThriftServer>;
    std::unique_ptr<Client> firstClient;
    std::unique_ptr<Client> secondClient;
    clientEvb->runInEventBaseThreadAndWait([&] {
      auto makeClient = [&] {
        auto socket =
            folly::AsyncSocket::newSocket(clientEvb, server.getAddress());
        auto channel =
            apache::thrift::RocketClientChannel::newChannel(std::move(socket));
        return std::make_unique<Client>(std::move(channel));
      };
      firstClient = makeClient();
      secondClient = makeClient();
    });

    std::atomic<size_t> completed{0};
    folly::Baton<> bothCompleted;
    clientEvb->runInEventBaseThread([&] {
      firstClient->semifuture_ping().via(clientEvb).thenTry(
          [&](folly::Try<folly::Unit>&& result) {
            if (result.hasException()) {
              ADD_FAILURE() << folly::exceptionStr(result.exception());
            }
            if (completed.fetch_add(1) + 1 == 2) {
              bothCompleted.post();
            }
          });
      secondClient->semifuture_ping().via(clientEvb).thenTry(
          [&](folly::Try<folly::Unit>&& result) {
            if (result.hasException()) {
              ADD_FAILURE() << folly::exceptionStr(result.exception());
            }
            if (completed.fetch_add(1) + 1 == 2) {
              bothCompleted.post();
            }
          });
    });

    ASSERT_TRUE(handler->firstStarted_.try_wait_for(std::chrono::seconds{5}));
    EXPECT_EQ(
        secondStarted.wait_for(std::chrono::milliseconds{100}),
        std::future_status::timeout);
    EXPECT_EQ(handler->started(), 1);

    ASSERT_NE(handler->getEventBase(), nullptr);
    handler->getEventBase()->runInEventBaseThreadAndWait(
        [&] { handler->completeNext(); });
    ASSERT_EQ(
        secondStarted.wait_for(std::chrono::seconds{5}),
        std::future_status::ready);
    handler->getEventBase()->runInEventBaseThreadAndWait(
        [&] { handler->completeNext(); });

    EXPECT_TRUE(bothCompleted.try_wait_for(std::chrono::seconds{5}));
    clientEvb->runInEventBaseThreadAndWait([&] {
      firstClient.reset();
      secondClient.reset();
    });
    server.stop();
  };
  verify.template operator()<ftt::FastThriftServer>();
  verify.template operator()<ftt::DynamicFastThriftServer>();
}

// ---------------------------------------------------------------------------
// TLS coverage — FastThriftServer with FizzServerConfig.
// ---------------------------------------------------------------------------

// What the connection context reports about the peer, sampled once the
// connection is established. A fresh extension is built per connection, so the
// sink lives in the fixture and every instance points at it.
struct PeerSecurityRecorder {
  std::atomic<bool> established{false};
  std::atomic<bool> hadCertificate{false};
  folly::Synchronized<std::string> securityProtocol;
};

// Inert observer: samples the connection context and always proceeds, so it
// can sit in a fixture shared with tests that are not about peer security.
struct PeerSecurityRecordingExtension {
  PeerSecurityRecorder* rec{nullptr};

  ftt::ConnectionVerdict onConnectionEstablished(
      const ftt::ThriftConnectionView& conn) noexcept {
    rec->hadCertificate.store(
        conn.peerCertificate() != nullptr, std::memory_order_relaxed);
    *rec->securityProtocol.wlock() = std::string(conn.securityProtocol());
    rec->established.store(true, std::memory_order_relaxed);
    return ftt::ConnectionVerdict::proceed();
  }
};

class FastThriftServerTlsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    THRIFT_FLAG_SET_MOCK(rocket_client_binary_rpc_metadata_encoding, true);

    cert_ = security::test::makeTestCert();
    handler_ = std::make_shared<TestHandler>();

    ftt::FastThriftServerConfig config;
    config.address = folly::SocketAddress("::1", 0);
    config.numIOThreads = 1;
    config.numConnectionSetupThreads = 1;
    server_ = std::make_unique<ftt::FastThriftServer>(std::move(config));

    security::FizzServerCertConfig sslConfig;
    sslConfig.certPem = cert_.certPem;
    sslConfig.keyPem = cert_.keyPem;
    // Skip mTLS — covered by the unit tests on FizzServerContextBuilder.
    sslConfig.clientAuth = fizz::server::ClientAuthMode::None;
    server_->setSSLConfig(std::move(sslConfig));

    server_->addModule(
        ftt::FastServerModule("peer_security_recorder")
            .addThriftExtension<PeerSecurityRecordingExtension>(&recorder_));

    server_->setInterface(handler_);
    server_->start();

    clientThread_ = std::make_unique<folly::ScopedEventBaseThread>();
  }

  void TearDown() override {
    clientThread_.reset();
    server_->stop();
    server_.reset();
  }

  security::test::TestCert cert_;
  std::shared_ptr<TestHandler> handler_;
  PeerSecurityRecorder recorder_;
  std::unique_ptr<ftt::FastThriftServer> server_;
  std::unique_ptr<folly::ScopedEventBaseThread> clientThread_;
};

TEST_F(FastThriftServerTlsTest, RoundTripOverTls) {
  auto* evb = clientThread_->getEventBase();
  auto transport = connectFizz(evb, server_->getAddress());
  ASSERT_NE(transport, nullptr);

  std::unique_ptr<apache::thrift::Client<integration::FastThriftServer>> client;
  evb->runInEventBaseThreadAndWait([&] {
    auto channel =
        apache::thrift::RocketClientChannel::newChannel(std::move(transport));
    client =
        std::make_unique<apache::thrift::Client<integration::FastThriftServer>>(
            std::move(channel));
  });

  folly::Baton<> done;
  EchoResponse echoed;
  folly::exception_wrapper rpcErr;
  evb->runInEventBaseThread([&] {
    client->semifuture_echo("over fizz")
        .via(evb)
        .thenValue([&](EchoResponse r) {
          echoed = std::move(r);
          done.post();
        })
        .thenError([&](const folly::exception_wrapper& ew) {
          rpcErr = ew;
          done.post();
        });
  });
  ASSERT_TRUE(done.try_wait_for(std::chrono::seconds{10}));
  EXPECT_FALSE(rpcErr) << rpcErr.what();
  EXPECT_EQ(*echoed.message(), "echoed:over fizz");

  evb->runInEventBaseThreadAndWait([&] { client.reset(); });
}

// What the peer proved during the handshake reaches the per-connection
// context, rather than being left for someone to ask the transport for later.
TEST_F(FastThriftServerTlsTest, ConnectionContextCarriesNegotiatedSecurity) {
  auto* evb = clientThread_->getEventBase();
  auto transport = connectFizz(evb, server_->getAddress());
  ASSERT_NE(transport, nullptr);

  std::unique_ptr<apache::thrift::Client<integration::FastThriftServer>> client;
  evb->runInEventBaseThreadAndWait([&] {
    auto channel =
        apache::thrift::RocketClientChannel::newChannel(std::move(transport));
    client =
        std::make_unique<apache::thrift::Client<integration::FastThriftServer>>(
            std::move(channel));
  });

  folly::Baton<> done;
  folly::exception_wrapper rpcErr;
  evb->runInEventBaseThread([&] {
    client->semifuture_ping()
        .via(evb)
        .thenValue([&](auto&&) { done.post(); })
        .thenError([&](const folly::exception_wrapper& ew) {
          rpcErr = ew;
          done.post();
        });
  });
  ASSERT_TRUE(done.try_wait_for(std::chrono::seconds{10}));
  EXPECT_FALSE(rpcErr) << rpcErr.what();

  EXPECT_TRUE(recorder_.established.load(std::memory_order_relaxed));
  EXPECT_FALSE(recorder_.securityProtocol.rlock()->empty());
  // clientAuth is None in this fixture, so the peer presents no certificate.
  EXPECT_FALSE(recorder_.hadCertificate.load(std::memory_order_relaxed));

  evb->runInEventBaseThreadAndWait([&] { client.reset(); });
}

TEST_F(FastThriftServerTlsTest, PlaintextClientFailsToConnect) {
  // A non-TLS client connects but its first frame will not parse as a
  // ClientHello; the server's fizz handshake fails and the connection is
  // dropped. The RPC should error out (not hang or succeed).
  auto* evb = clientThread_->getEventBase();
  std::unique_ptr<apache::thrift::Client<integration::FastThriftServer>> client;
  evb->runInEventBaseThreadAndWait([&] {
    auto socket = folly::AsyncSocket::newSocket(evb, server_->getAddress());
    auto channel =
        apache::thrift::RocketClientChannel::newChannel(std::move(socket));
    client =
        std::make_unique<apache::thrift::Client<integration::FastThriftServer>>(
            std::move(channel));
  });

  folly::Baton<> done;
  folly::exception_wrapper rpcErr;
  evb->runInEventBaseThread([&] {
    client->semifuture_ping()
        .via(evb)
        .thenValue([&](folly::Unit) { done.post(); })
        .thenError([&](const folly::exception_wrapper& ew) {
          rpcErr = ew;
          done.post();
        });
  });
  ASSERT_TRUE(done.try_wait_for(std::chrono::seconds{10}));
  EXPECT_TRUE(rpcErr) << "plaintext client should fail against TLS server";

  evb->runInEventBaseThreadAndWait([&] { client.reset(); });
}

// ---------------------------------------------------------------------------
// STOPTLS V1 — server tears down TLS after handshake; RPCs continue plaintext
// over the same FD with peer/self cert info preserved.
// ---------------------------------------------------------------------------

class FastThriftServerStopTlsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    THRIFT_FLAG_SET_MOCK(rocket_client_binary_rpc_metadata_encoding, true);

    cert_ = security::test::makeTestCert();
    handler_ = std::make_shared<TestHandler>();

    ftt::FastThriftServerConfig config;
    config.address = folly::SocketAddress("::1", 0);
    config.numIOThreads = 1;
    server_ = std::make_unique<ftt::FastThriftServer>(std::move(config));

    security::FizzServerCertConfig sslConfig;
    sslConfig.certPem = cert_.certPem;
    sslConfig.keyPem = cert_.keyPem;
    sslConfig.clientAuth = fizz::server::ClientAuthMode::None;
    server_->setSSLConfig(std::move(sslConfig));

    security::ThriftTlsConfig thriftConfig;
    thriftConfig.enableStopTLS = true;
    server_->setThriftConfig(std::move(thriftConfig));

    server_->addModule(
        ftt::FastServerModule("peer_security_recorder")
            .addThriftExtension<PeerSecurityRecordingExtension>(&recorder_));

    server_->setInterface(handler_);
    server_->start();

    clientThread_ = std::make_unique<folly::ScopedEventBaseThread>();
  }

  void TearDown() override {
    clientThread_.reset();
    server_->stop();
    server_.reset();
  }

  security::test::TestCert cert_;
  std::shared_ptr<TestHandler> handler_;
  PeerSecurityRecorder recorder_;
  std::unique_ptr<ftt::FastThriftServer> server_;
  std::unique_ptr<folly::ScopedEventBaseThread> clientThread_;
};

TEST_F(FastThriftServerStopTlsTest, FallsBackToTLSWhenClientDoesNotRequest) {
  // Server has enableStopTLS=true, but the client uses the plain fizz
  // connector (no Thrift extension). The server's extension's
  // getNegotiatedStopTLS() must return false, and the connection must
  // continue over the encrypted fizz transport. Catches a regression where
  // the server tears down TLS based on its own config rather than the
  // negotiation result.
  auto* evb = clientThread_->getEventBase();
  auto transport = connectFizz(evb, server_->getAddress());
  ASSERT_NE(transport, nullptr);

  std::unique_ptr<apache::thrift::Client<integration::FastThriftServer>> client;
  evb->runInEventBaseThreadAndWait([&] {
    auto channel =
        apache::thrift::RocketClientChannel::newChannel(std::move(transport));
    client =
        std::make_unique<apache::thrift::Client<integration::FastThriftServer>>(
            std::move(channel));
  });

  folly::Baton<> done;
  EchoResponse echoed;
  folly::exception_wrapper rpcErr;
  evb->runInEventBaseThread([&] {
    client->semifuture_echo("over fizz")
        .via(evb)
        .thenValue([&](EchoResponse r) {
          echoed = std::move(r);
          done.post();
        })
        .thenError([&](const folly::exception_wrapper& ew) {
          rpcErr = ew;
          done.post();
        });
  });
  ASSERT_TRUE(done.try_wait_for(std::chrono::seconds{10}));
  EXPECT_FALSE(rpcErr) << rpcErr.what();
  EXPECT_EQ(*echoed.message(), "echoed:over fizz");

  evb->runInEventBaseThreadAndWait([&] { client.reset(); });
}

TEST_F(FastThriftServerStopTlsTest, RoundTripAfterStopTLSDowngrade) {
  auto* evb = clientThread_->getEventBase();

  // connect() runs the fizz handshake on the EVB thread and blocks here
  // until StopTLS downgrade completes. Must NOT run inside a
  // runInEventBaseThread lambda (would double-drive evb).
  FizzStopTLSConnector connector;
  auto plaintext = connector.connect(server_->getAddress(), evb);
  ASSERT_NE(plaintext, nullptr);

  std::unique_ptr<apache::thrift::Client<integration::FastThriftServer>> client;
  evb->runInEventBaseThreadAndWait([&] {
    auto channel = apache::thrift::RocketClientChannel::newChannel(
        folly::AsyncTransport::UniquePtr(plaintext.release()));
    client =
        std::make_unique<apache::thrift::Client<integration::FastThriftServer>>(
            std::move(channel));
  });

  folly::Baton<> done;
  EchoResponse echoed;
  folly::exception_wrapper rpcErr;
  evb->runInEventBaseThread([&] {
    client->semifuture_echo("after stoptls")
        .via(evb)
        .thenValue([&](EchoResponse r) {
          echoed = std::move(r);
          done.post();
        })
        .thenError([&](const folly::exception_wrapper& ew) {
          rpcErr = ew;
          done.post();
        });
  });
  ASSERT_TRUE(done.try_wait_for(std::chrono::seconds{10}));
  EXPECT_FALSE(rpcErr) << rpcErr.what();
  EXPECT_EQ(*echoed.message(), "echoed:after stoptls");

  // The connection is plaintext by the time it is established. Preserve the
  // negotiated downgrade explicitly so observers can distinguish it from a
  // connection that was plaintext from the start.
  EXPECT_TRUE(recorder_.established.load(std::memory_order_relaxed));
  EXPECT_EQ(*recorder_.securityProtocol.rlock(), "stopTLS");

  evb->runInEventBaseThreadAndWait([&] { client.reset(); });
}

// ---------------------------------------------------------------------------
// Shared IO thread pool — embedder-supplied pool via setIOThreadPool.
// ---------------------------------------------------------------------------

namespace {

ftt::FastThriftServerConfig makeLoopbackConfig() {
  ftt::FastThriftServerConfig config;
  config.address = folly::SocketAddress("::1", 0);
  // Set to a value distinct from the embedder pool's size to make it obvious
  // in tests that the supplied pool is what gets used, not
  // config_.numIOThreads.
  config.numIOThreads = 4;
  return config;
}

// Build a min==max IO pool. setIOThreadPool requires this shape; encoding
// the convention in a test helper keeps the test sites tight.
std::shared_ptr<folly::IOThreadPoolExecutor> makeFixedSizePool(size_t threads) {
  return std::make_shared<folly::IOThreadPoolExecutor>(
      /*maxThreads=*/threads, /*minThreads=*/threads);
}

namespace cp = ::apache::thrift::fast_thrift::channel_pipeline;

// Observer extension: counts the requests and responses it sees, then lets
// them continue. When given a shared globalSeq counter, it records the sequence
// number of its first request into firstRequestSeq; extensions registered in
// order see strictly increasing first-request sequences (requests flow
// head→tail), which pins registration order == pipeline order.
struct CountingExtension {
  std::atomic<int>* requests{nullptr};
  std::atomic<int>* responses{nullptr};
  std::atomic<int>* globalSeq{nullptr};
  std::atomic<int>* firstRequestSeq{nullptr};

  ftt::RequestVerdict onRequest(
      const ftt::ThriftRequestView& /*request*/) noexcept {
    if (requests) {
      requests->fetch_add(1, std::memory_order_relaxed);
    }
    if (globalSeq && firstRequestSeq) {
      int seq = globalSeq->fetch_add(1, std::memory_order_relaxed);
      int expected = -1;
      firstRequestSeq->compare_exchange_strong(expected, seq);
    }
    return ftt::RequestVerdict::proceed();
  }

  void onResponse(const ftt::ThriftResponseView& /*response*/) noexcept {
    if (responses) {
      responses->fetch_add(1, std::memory_order_relaxed);
    }
  }
};

class StaticSelectionRecordingHandler {
 public:
  StaticSelectionRecordingHandler(
      std::atomic<int>* reads,
      std::atomic<int>* sequence,
      std::atomic<int>* firstRead)
      : reads_(reads), sequence_(sequence), firstRead_(firstRead) {}

  template <typename Context>
  void handlerAdded(Context&) noexcept {}
  template <typename Context>
  void handlerRemoved(Context&) noexcept {}
  template <typename Context>
  void onPipelineActive(Context&) noexcept {}
  template <typename Context>
  void onPipelineInactive(Context&) noexcept {}
  template <typename Context>
  void onReadReady(Context&) noexcept {}
  template <typename Context>
  void onWriteReady(Context&) noexcept {}
  template <typename Context>
  cp::Result onRead(Context& ctx, cp::TypeErasedBox&& msg) noexcept {
    reads_->fetch_add(1, std::memory_order_relaxed);
    const int next = sequence_->fetch_add(1, std::memory_order_relaxed);
    int expected = -1;
    firstRead_->compare_exchange_strong(expected, next);
    return ctx.fireRead(std::move(msg));
  }
  template <typename Context>
  cp::Result onWrite(Context& ctx, cp::TypeErasedBox&& msg) noexcept {
    return ctx.fireWrite(std::move(msg));
  }
  template <typename Context>
  void onException(Context& ctx, folly::exception_wrapper&& e) noexcept {
    ctx.fireException(std::move(e));
  }

 private:
  std::atomic<int>* reads_;
  std::atomic<int>* sequence_;
  std::atomic<int>* firstRead_;
};

struct RequestStateExtension {
  EXTENSION_ID(request_state_test);

  struct RequestState {
    int value{0};
  };

  explicit RequestStateExtension(std::atomic<int>* observed)
      : observed(observed) {}

  ftt::RequestVerdict onRequest(
      const ftt::ThriftRequestView& request) noexcept {
    request.emplaceState<RequestStateExtension>().value = 42;
    return ftt::RequestVerdict::proceed();
  }

  void onResponse(const ftt::ThriftResponseView& response) noexcept {
    const auto* state = response.tryState<RequestStateExtension>();
    observed->store(state == nullptr ? -1 : state->value);
  }

  std::atomic<int>* observed;
};
// Identity for a state object that outlives the object itself, so a test can
// still tell two of them apart. An address cannot do this job: a connection's
// store is destroyed with the connection, and the next one may be handed the
// same address.
int nextStateId() noexcept {
  static std::atomic<int> next{0};
  return next.fetch_add(1, std::memory_order_relaxed) + 1;
}

// What the extensions observed, published to the test body. Extensions run on
// the server's IO threads, so every field crosses a thread boundary and is
// atomic. Slots are claimed in call order, which is deterministic here:
// extensions on one connection run in pipeline order, and the tests' round
// trips are sequential.
struct StateObservations {
  static constexpr size_t kCapacity = 4;

  void record(int stateId, int requestsSeen) noexcept {
    const size_t slot = claimed.fetch_add(1, std::memory_order_relaxed);
    if (slot >= kCapacity) {
      return;
    }
    ids[slot].store(stateId, std::memory_order_relaxed);
    counts[slot].store(requestsSeen, std::memory_order_relaxed);
    // Bumped only after the payload is written, and it is the counter size()
    // reads: a reader that sees N observations sees the N payloads too. The
    // claim counter cannot serve this role — it is bumped before the stores.
    published.fetch_add(1, std::memory_order_release);
  }

  // Observations whose payload is readable. Never exceeds kCapacity: a claim
  // past the end publishes nothing.
  size_t size() const noexcept {
    return published.load(std::memory_order_acquire);
  }
  int id(size_t slot) const noexcept {
    return ids[slot].load(std::memory_order_relaxed);
  }
  int count(size_t slot) const noexcept {
    return counts[slot].load(std::memory_order_relaxed);
  }

  std::array<std::atomic<int>, kCapacity> ids{};
  std::array<std::atomic<int>, kCapacity> counts{};
  std::atomic<size_t> claimed{0};
  std::atomic<size_t> published{0};
};

// Per-connection state two cooperating extensions share by naming the same
// type. The writer stamps the caller it resolved; the reader authorizes against
// it, which is the SAP shape in miniature.
struct SharedCallerState {
  int id{nextStateId()};
  int requestsSeen{0};
};

// Reports which state object it was handed and the running count it saw, so a
// test can tell whether the two extensions on a connection got the same one.
// The count is published here rather than read back later: the store dies with
// the connection, which the client's teardown can close first.
struct SharedStateExtension {
  using ConnState = SharedCallerState;

  SharedStateExtension(SharedCallerState& state, StateObservations* observed)
      : state_(&state), observed_(observed) {}

  ftt::RequestVerdict onRequest(
      const ftt::ThriftRequestView& /*request*/) noexcept {
    ++state_->requestsSeen;
    if (observed_) {
      observed_->record(state_->id, state_->requestsSeen);
    }
    return ftt::RequestVerdict::proceed();
  }

  SharedCallerState* state_{nullptr};
  StateObservations* observed_{nullptr};
};

// A second extension type naming a different ConnState, to prove the slots do
// not alias. Ids come from the one counter both state types share, so an id
// from here can be compared against one from SharedCallerState.
struct OtherConnState {
  int id{nextStateId()};
};

struct OtherStateExtension {
  using ConnState = OtherConnState;

  OtherStateExtension(OtherConnState& state, StateObservations* observed)
      : state_(&state), observed_(observed) {}

  ftt::RequestVerdict onRequest(
      const ftt::ThriftRequestView& /*request*/) noexcept {
    if (observed_) {
      observed_->record(state_->id, 0);
    }
    return ftt::RequestVerdict::proceed();
  }

  OtherConnState* state_{nullptr};
  StateObservations* observed_{nullptr};
};

// Read/write extension that rejects every request. Reads the method name and
// stamps a header (exercising both halves of the mutator's access) then returns
// a rejecting verdict, which short-circuits the pipeline: the service handler
// must never run and the client receives an application error.
struct RejectingExtension {
  static constexpr bool kUsesHeaders = true;

  std::atomic<int>* rejections{nullptr};

  ftt::RequestVerdict onRequest(ftt::ThriftRequestMutator& request) noexcept {
    request.setResponseHeader(
        "x-rejected-by-extension", std::string(request.methodName()));
    if (rejections) {
      rejections->fetch_add(1, std::memory_order_relaxed);
    }
    return ftt::RequestVerdict::reject<apache::thrift::AppClientError>(
        "TypedExtensionRejection", "denied by extension");
  }
};

struct RejectionObservation {
  bool failed{false};
  std::string message;
  apache::thrift::transport::THeader::StringToStringMap headers;
};

RejectionObservation addRoundTripObservingRejection(
    const folly::SocketAddress& addr) {
  folly::EventBase evb;
  auto channel = apache::thrift::RocketClientChannel::newChannel(
      folly::AsyncSocket::newSocket(&evb, addr));
  apache::thrift::Client<integration::FastThriftServer> client(
      std::move(channel));

  apache::thrift::RpcOptions options;
  RejectionObservation observation;
  try {
    (void)client.sync_add(options, 7, 35);
  } catch (const apache::thrift::TApplicationException& ex) {
    observation.failed = true;
    observation.message = ex.what();
  }
  observation.headers = options.getReadHeaders();
  return observation;
}

// Drives one `add(7, 35)` RPC against `addr` and returns the result, so tests
// that only care about exercising the server pipeline stay tight.
int64_t addRoundTrip(const folly::SocketAddress& addr) {
  folly::ScopedEventBaseThread clientThread;
  auto* evb = clientThread.getEventBase();
  std::unique_ptr<apache::thrift::Client<integration::FastThriftServer>> client;
  evb->runInEventBaseThreadAndWait([&] {
    auto socket = folly::AsyncSocket::newSocket(evb, addr);
    auto channel =
        apache::thrift::RocketClientChannel::newChannel(std::move(socket));
    client =
        std::make_unique<apache::thrift::Client<integration::FastThriftServer>>(
            std::move(channel));
  });

  folly::Baton<> done;
  int64_t sum = 0;
  evb->runInEventBaseThread([&] {
    if (client == nullptr) {
      ADD_FAILURE() << "client was not constructed";
      done.post();
      return;
    }
    client->semifuture_add(7, 35)
        .via(evb)
        .thenValue([&](int64_t v) {
          sum = v;
          done.post();
        })
        .thenError([&](const folly::exception_wrapper& ew) {
          ADD_FAILURE() << "RPC failed: " << folly::exceptionStr(ew);
          done.post();
        });
  });
  EXPECT_TRUE(done.try_wait_for(std::chrono::seconds{10}));
  evb->runInEventBaseThreadAndWait([&] { client.reset(); });
  return sum;
}

// Drives one `add(7, 35)` RPC against `addr` and returns true iff the RPC
// failed (errored) rather than returning a value. Used by reject tests.
bool addRoundTripFails(const folly::SocketAddress& addr) {
  folly::ScopedEventBaseThread clientThread;
  auto* evb = clientThread.getEventBase();
  std::unique_ptr<apache::thrift::Client<integration::FastThriftServer>> client;
  evb->runInEventBaseThreadAndWait([&] {
    auto socket = folly::AsyncSocket::newSocket(evb, addr);
    auto channel =
        apache::thrift::RocketClientChannel::newChannel(std::move(socket));
    client =
        std::make_unique<apache::thrift::Client<integration::FastThriftServer>>(
            std::move(channel));
  });

  folly::Baton<> done;
  bool failed = false;
  evb->runInEventBaseThread([&] {
    if (client == nullptr) {
      ADD_FAILURE() << "client was not constructed";
      done.post();
      return;
    }
    client->semifuture_add(7, 35)
        .via(evb)
        .thenValue([&](int64_t) { done.post(); })
        .thenError([&](const folly::exception_wrapper&) {
          failed = true;
          done.post();
        });
  });
  EXPECT_TRUE(done.try_wait_for(std::chrono::seconds{10}));
  evb->runInEventBaseThreadAndWait([&] { client.reset(); });
  return failed;
}

ftt::FastThriftServerConfig makeHeadersConfig() {
  auto config = makeLoopbackConfig();
  // Inbound headers are populated on the per-request context only under this
  // setting; an extension declaring kUsesHeaders is refused without it.
  config.enableRequestHeaders = true;
  return config;
}

std::unique_ptr<void, void (*)(void*)> resolveFirstPeerIdentity(
    const folly::AsyncTransportCertificate*, const folly::SocketAddress&) {
  return {nullptr, +[](void*) noexcept {}};
}

std::unique_ptr<void, void (*)(void*)> resolveSecondPeerIdentity(
    const folly::AsyncTransportCertificate*, const folly::SocketAddress&) {
  return {nullptr, +[](void*) noexcept {}};
}

class CountingLegacyEventHandler final
    : public apache::thrift::TProcessorEventHandler,
      public apache::thrift::server::TServerEventHandler {
 public:
  explicit CountingLegacyEventHandler(
      std::atomic<int>& requests, std::atomic<int>& connections)
      : requests_(requests), connections_(connections) {}

  void* getServiceContext(
      std::string_view,
      std::string_view,
      apache::thrift::TConnectionContext*) override {
    requests_.fetch_add(1, std::memory_order_relaxed);
    return nullptr;
  }

  void newConnection(
      apache::thrift::server::TConnectionContext* context) override {
    if (context != nullptr) {
      connections_.fetch_add(1, std::memory_order_relaxed);
    }
  }

 private:
  std::atomic<int>& requests_;
  std::atomic<int>& connections_;
};

struct ClassicContextCheckingExtension {
  std::atomic<int>* connections{nullptr};
  std::atomic<bool>* connectionReady{nullptr};
  std::atomic<bool>* requestReady{nullptr};

  ftt::ConnectionVerdict onConnectionAttempted(
      const ftt::ThriftSetupConnectionView& connection) noexcept {
    connectionReady->store(
        connection.tryState<ftt::server::Cpp2BridgeExtension>() != nullptr &&
            connections->load(std::memory_order_relaxed) == 2,
        std::memory_order_relaxed);
    return ftt::ConnectionVerdict::proceed();
  }

  ftt::RequestVerdict onRequest(
      const ftt::ThriftRequestView& request) noexcept {
    EXPECT_TRUE(request.hasResolvedMethod());
    EXPECT_EQ(request.methodName(), "add");
    EXPECT_EQ(request.qualifiedMethodName(), "FastThriftServer.add");
    requestReady->store(
        ftt::server::tryGetCpp2RequestContext(request) != nullptr,
        std::memory_order_relaxed);
    return ftt::RequestVerdict::proceed();
  }
};

TEST(FastThriftServerExtensionTest, RejectsConflictingPeerIdentityResolvers) {
  ftt::FastThriftServer server(makeLoopbackConfig());

  server.setPeerIdentityResolver(&resolveFirstPeerIdentity);
  EXPECT_NO_THROW(server.setPeerIdentityResolver(&resolveFirstPeerIdentity));
  EXPECT_THROW(
      server.setPeerIdentityResolver(&resolveSecondPeerIdentity),
      std::logic_error);
  EXPECT_EQ(server.getPeerIdentityResolver(), &resolveFirstPeerIdentity);
}

TEST(FastThriftServerExtensionTest, RejectsNullClassicEventHandlers) {
  ftt::FastThriftServer server(makeLoopbackConfig());

  EXPECT_THROW(server.addProcessorEventHandler(nullptr), std::logic_error);
  EXPECT_THROW(server.addServerEventHandler(nullptr), std::logic_error);
}

TEST(FastThriftServerExtensionTest, RejectsClassicRegistrationAfterStart) {
  auto handler = std::make_shared<TestHandler>();
  std::atomic<int> requests{0};
  std::atomic<int> connections{0};
  auto eventHandler =
      std::make_shared<CountingLegacyEventHandler>(requests, connections);

  ftt::FastThriftServer server(makeLoopbackConfig());
  server.setInterface(handler);
  server.start();

  EXPECT_THROW(
      server.setPeerIdentityResolver(&resolveFirstPeerIdentity),
      std::logic_error);
  EXPECT_THROW(server.addProcessorEventHandler(eventHandler), std::logic_error);
  EXPECT_THROW(server.addServerEventHandler(eventHandler), std::logic_error);
}

TEST(FastThriftServerExtensionTest, CoalescesClassicEventHandlers) {
  THRIFT_FLAG_SET_MOCK(rocket_client_binary_rpc_metadata_encoding, true);

  auto handler = std::make_shared<TestHandler>();
  std::atomic<int> requests{0};
  std::atomic<int> connections{0};
  std::atomic<bool> connectionReady{false};
  std::atomic<bool> requestReady{false};
  auto first =
      std::make_shared<CountingLegacyEventHandler>(requests, connections);
  auto second =
      std::make_shared<CountingLegacyEventHandler>(requests, connections);

  auto config = makeLoopbackConfig();
  ftt::FastThriftServer server(std::move(config));
  server.setInterface(handler);
  server.addProcessorEventHandler(first);
  server.addServerEventHandler(first);
  server.addProcessorEventHandler(second);
  server.addServerEventHandler(second);
  server.addModule(
      ftt::FastServerModule("classic_context_checker")
          .addThriftExtension<ClassicContextCheckingExtension>(
              &connections, &connectionReady, &requestReady));
  server.start();

  EXPECT_EQ(addRoundTrip(server.getAddress()), 42);
  EXPECT_EQ(requests.load(), 2);
  EXPECT_EQ(connections.load(), 2);
  EXPECT_TRUE(connectionReady.load());
  EXPECT_TRUE(requestReady.load());
}

// Shared sink for what a connection extension observed. A fresh extension is
// constructed per connection, so the recorder lives in the test and every
// instance points at it.
struct ConnectionRecorder {
  std::atomic<int> seq{0};
  std::atomic<int> attempted{-1};
  std::atomic<int> established{-1};
  std::atomic<int> request{-1};
  std::atomic<int> closed{0};
  std::atomic<int> negotiatedVersion{0};
  std::atomic<bool> sawClientSetup{false};

  // Stamps `slot` with the next global sequence number, keeping the first
  // stamp only, so ordering assertions read the first connection's lifecycle
  // even if the test opens more.
  void stamp(std::atomic<int>& slot) noexcept {
    int next = seq.fetch_add(1, std::memory_order_relaxed);
    int expected = -1;
    slot.compare_exchange_strong(expected, next);
  }
};

struct PendingConnectionRecordingExtension {
  static void reset() noexcept {
    enqueued.store(0, std::memory_order_relaxed);
    dequeued.store(0, std::memory_order_relaxed);
    dropped.store(0, std::memory_order_relaxed);
  }

  static void onConnectionEnqueued() noexcept {
    enqueued.fetch_add(1, std::memory_order_relaxed);
  }

  static void onConnectionDequeued() noexcept {
    dequeued.fetch_add(1, std::memory_order_relaxed);
  }

  static void onConnectionDroppedWhileQueued() noexcept {
    dropped.fetch_add(1, std::memory_order_relaxed);
  }

  static inline std::atomic<int> enqueued{0};
  static inline std::atomic<int> dequeued{0};
  static inline std::atomic<int> dropped{0};
};

// Records every lifecycle point plus the request, so a single connection's
// callback order can be compared against the first request it carries.
struct ConnectionLifecycleExtension {
  ConnectionRecorder* rec{nullptr};

  ftt::ConnectionVerdict onConnectionAttempted(
      const ftt::ThriftSetupConnectionView& conn) noexcept {
    rec->stamp(rec->attempted);
    rec->negotiatedVersion.store(
        conn.negotiatedVersion(), std::memory_order_relaxed);
    rec->sawClientSetup.store(
        conn.clientSetup().maxVersion().has_value(), std::memory_order_relaxed);
    return ftt::ConnectionVerdict::proceed();
  }

  ftt::ConnectionVerdict onConnectionEstablished(
      const ftt::ThriftConnectionView& /*conn*/) noexcept {
    rec->stamp(rec->established);
    return ftt::ConnectionVerdict::proceed();
  }

  void onConnectionClosed(const ftt::ThriftConnectionView& /*conn*/) noexcept {
    rec->closed.fetch_add(1, std::memory_order_relaxed);
  }

  ftt::RequestVerdict onRequest(
      const ftt::ThriftRequestView& /*request*/) noexcept {
    rec->stamp(rec->request);
    return ftt::RequestVerdict::proceed();
  }
};

// Refuses at the first decision point. Also records the later points, so a
// test can assert they never run.
struct RejectAtAttemptedExtension {
  ConnectionRecorder* rec{nullptr};

  ftt::ConnectionVerdict onConnectionAttempted(
      const ftt::ThriftSetupConnectionView& /*conn*/) noexcept {
    rec->stamp(rec->attempted);
    return ftt::ConnectionVerdict::reject<std::runtime_error>(
        "denied at connection attempt");
  }

  ftt::ConnectionVerdict onConnectionEstablished(
      const ftt::ThriftConnectionView& /*conn*/) noexcept {
    rec->stamp(rec->established);
    return ftt::ConnectionVerdict::proceed();
  }

  ftt::RequestVerdict onRequest(
      const ftt::ThriftRequestView& /*request*/) noexcept {
    rec->stamp(rec->request);
    return ftt::RequestVerdict::proceed();
  }
};

// Contributes a security policy to the SETUP response, the way an
// authorization extension declares its enforcement level to the client.
struct PolicyContributingExtension {
  ConnectionRecorder* rec{nullptr};

  ftt::ConnectionVerdict onConnectionAttempted(
      ftt::ThriftSetupConnectionMutator& /*conn*/) noexcept {
    rec->stamp(rec->attempted);
    return ftt::ConnectionVerdict::proceed();
  }

  // Contributed on the way out: the response does not exist yet at
  // onConnectionAttempted.
  void onConnectionAnswering(
      ftt::ThriftSetupResponseMutator& response) noexcept {
    apache::thrift::SecurityPolicy policy;
    policy.authorization() = apache::thrift::SecurityPolicyStatus::ENFORCED;
    policy.authWall() = apache::thrift::SecurityPolicyStatus::ENABLED;
    response.setSecurityPolicy(std::move(policy));
  }
};

// Hooks only the teardown point — an extension is free to implement any subset
// of the callbacks, including no request callback at all.
struct ClosedOnlyExtension {
  ConnectionRecorder* rec{nullptr};

  void onConnectionClosed(const ftt::ThriftConnectionView& /*conn*/) noexcept {
    rec->closed.fetch_add(1, std::memory_order_relaxed);
  }
};

// What a backpressure extension observed. One extension is built per
// connection, so the sink lives in the test.
struct BackpressureRecorder {
  std::atomic<int> drained{0};
  std::atomic<int> attached{0};
  std::atomic<void*> userData{nullptr};
  // Set by the test to make the next request pause the connection once.
  std::atomic<bool> pauseOnce{false};
  // Whether that pause is ever lifted. A test that leaves it false pins what a
  // pause actually stops.
  std::atomic<bool> resumeAfterPause{true};
  // Whether the EventBase the resumer named was the one looping the connection
  // at the time the callback ran. An EventBase obtained any other way can be
  // one nothing ever loops, which strands the deferred resume silently.
  std::atomic<bool> resumerEvbLooping{false};

  std::mutex resumerMutex;
  ftt::ReadResumer resumer;
};

// Pauses from the request path, which is the only point the decision can still
// reach the transport, and resumes the way a real consumer must: from a later
// turn of the event loop, never inline, since onEgressDrained runs on the
// socket's write-completion path.
struct BackpressureExtension {
  explicit BackpressureExtension(BackpressureRecorder* r) noexcept : rec(r) {}

  ftt::RequestVerdict onRequest(
      const ftt::ThriftRequestView& /*request*/) noexcept {
    if (rec->pauseOnce.exchange(false, std::memory_order_relaxed)) {
      return ftt::RequestVerdict::backpressure();
    }
    return ftt::RequestVerdict::proceed();
  }

  void onBackpressureAttached(ftt::ReadResumer resumer) noexcept {
    {
      std::lock_guard<std::mutex> lock(rec->resumerMutex);
      rec->resumer = std::move(resumer);
    }
    rec->attached.fetch_add(1, std::memory_order_relaxed);
  }

  void onEgressDrained(const ftt::ThriftConnectionView& conn) noexcept {
    rec->userData.store(conn.userData(), std::memory_order_relaxed);
    rec->drained.fetch_add(1, std::memory_order_relaxed);
    if (!rec->resumeAfterPause.load(std::memory_order_relaxed)) {
      return;
    }
    ftt::ReadResumer resumer;
    {
      std::lock_guard<std::mutex> lock(rec->resumerMutex);
      resumer = rec->resumer;
    }
    rec->resumerEvbLooping.store(
        resumer.eventBase() != nullptr &&
            resumer.eventBase()->inRunningEventBaseThread(),
        std::memory_order_relaxed);
    resumer.eventBase()->runInLoop(
        [resumer]() mutable noexcept { resumer.resume(); });
  }

  BackpressureRecorder* rec{nullptr};
};

// Hooks only the admission-control points — an extension that never inspects a
// request still gets the drain notification and a resumer.
struct DrainOnlyExtension {
  void onBackpressureAttached(const ftt::ReadResumer& /*resumer*/) noexcept {}
  void onEgressDrained(const ftt::ThriftConnectionView& /*conn*/) noexcept {}
};

// One extension, both families: the request callback and the admission-control
// callback land on the same per-connection instance.
struct ObservingBackpressureExtension {
  explicit ObservingBackpressureExtension(BackpressureRecorder* r) noexcept
      : rec(r) {}

  ftt::RequestVerdict onRequest(
      const ftt::ThriftRequestView& /*request*/) noexcept {
    ++requests;
    return ftt::RequestVerdict::proceed();
  }

  void onBackpressureAttached(const ftt::ReadResumer& /*resumer*/) noexcept {
    rec->attached.fetch_add(1, std::memory_order_relaxed);
  }

  void onEgressDrained(const ftt::ThriftConnectionView& /*conn*/) noexcept {
    // Reads its own request count: proof the two callbacks share one instance.
    if (requests > 0) {
      rec->drained.fetch_add(1, std::memory_order_relaxed);
    }
  }

  BackpressureRecorder* rec{nullptr};
  int requests{0};
};

// `count` sequential RPCs over one connection, returning false rather than
// hanging if one stops landing.
//
// Two trips are enough to observe a pause taken on the first request: the
// verdict reaches the transport on the traversal that carried that request, so
// the second never arrives. If pausing ever regressed to being applied
// out-of-band it would take three, which is what these tests would catch.
bool roundTripsOnOneConnection(
    const folly::SocketAddress& addr,
    int count,
    std::chrono::seconds callTimeout = std::chrono::seconds{10}) {
  folly::ScopedEventBaseThread clientThread;
  auto* evb = clientThread.getEventBase();
  std::unique_ptr<apache::thrift::Client<integration::FastThriftServer>> client;
  evb->runInEventBaseThreadAndWait([&] {
    auto socket = folly::AsyncSocket::newSocket(evb, addr);
    client =
        std::make_unique<apache::thrift::Client<integration::FastThriftServer>>(
            apache::thrift::RocketClientChannel::newChannel(std::move(socket)));
  });

  bool ok = true;
  for (int i = 0; i < count && ok; ++i) {
    folly::Baton<> done;
    int64_t sum = 0;
    evb->runInEventBaseThread([&] {
      // Bound the call itself, not just the wait below. A request the server
      // is holding would otherwise sit until Thrift's default client timeout
      // and keep the client loop alive for that long, whatever this waits for.
      apache::thrift::RpcOptions options;
      options.setTimeout(callTimeout);
      client->semifuture_add(options, 7, 35)
          .via(evb)
          .thenValue([&](int64_t v) {
            sum = v;
            done.post();
          })
          .thenError(
              folly::tag_t<std::exception>{},
              [&](const std::exception&) { done.post(); });
    });
    ok = done.try_wait_for(callTimeout + std::chrono::seconds{5}) && sum == 42;
  }

  evb->runInEventBaseThreadAndWait([&] { client.reset(); });
  return ok;
}

// Spins until `value` reaches `target` or the deadline passes. Connection
// teardown completes asynchronously after the client goes away, so tests that
// assert on it cannot read the counter straight through.
bool waitFor(const std::atomic<int>& value, int target) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds{10};
  while (std::chrono::steady_clock::now() < deadline) {
    if (value.load(std::memory_order_relaxed) >= target) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{10});
  }
  return false;
}

} // namespace

// Embedder pool actually drives accept + dispatch end-to-end.
TEST(FastThriftServerSharedPoolTest, RoundTripWithEmbedderPool) {
  THRIFT_FLAG_SET_MOCK(rocket_client_binary_rpc_metadata_encoding, true);

  auto handler = std::make_shared<TestHandler>();
  auto pool = makeFixedSizePool(2);

  ftt::FastThriftServer server(makeLoopbackConfig());
  server.setIOThreadPool(pool);
  server.setInterface(handler);
  server.start();

  folly::ScopedEventBaseThread clientThread;
  auto* evb = clientThread.getEventBase();
  std::unique_ptr<apache::thrift::Client<integration::FastThriftServer>> client;
  evb->runInEventBaseThreadAndWait([&] {
    auto socket = folly::AsyncSocket::newSocket(evb, server.getAddress());
    auto channel =
        apache::thrift::RocketClientChannel::newChannel(std::move(socket));
    client =
        std::make_unique<apache::thrift::Client<integration::FastThriftServer>>(
            std::move(channel));
  });

  folly::Baton<> done;
  int64_t sum = 0;
  evb->runInEventBaseThread([&] {
    client->semifuture_add(7, 35)
        .via(evb)
        .thenValue([&](int64_t v) {
          sum = v;
          done.post();
        })
        .thenError([&](const folly::exception_wrapper& ew) {
          ADD_FAILURE() << "RPC failed: " << folly::exceptionStr(ew);
          done.post();
        });
  });
  ASSERT_TRUE(done.try_wait_for(std::chrono::seconds{10}));
  EXPECT_EQ(sum, 42);
  evb->runInEventBaseThreadAndWait([&] { client.reset(); });
}

// The per-connection context carries the peer address captured when the socket
// was accepted, not one re-derived from the transport at build time.
TEST(FastThriftServerConnContextTest, PeerAddressReachesConnContext) {
  THRIFT_FLAG_SET_MOCK(rocket_client_binary_rpc_metadata_encoding, true);

  auto handler = std::make_shared<TestHandler>();
  ftt::FastThriftServer server(makeLoopbackConfig());
  server.setInterface(handler);

  folly::Baton<> accepted;
  folly::SocketAddress observedPeer;
  server.setOnConnectionAccepted([&](ftt::ThriftConnContext* connContext) {
    if (connContext != nullptr) {
      observedPeer = connContext->getPeerAddress();
    }
    accepted.post();
  });
  server.start();

  folly::ScopedEventBaseThread clientThread;
  auto* evb = clientThread.getEventBase();
  std::shared_ptr<folly::AsyncSocket> socket;
  folly::SocketAddress clientLocalAddr;
  evb->runInEventBaseThreadAndWait([&] {
    socket = folly::AsyncSocket::newSocket(evb, server.getAddress());
    socket->getLocalAddress(&clientLocalAddr);
  });

  ASSERT_TRUE(accepted.try_wait_for(std::chrono::seconds{10}));
  EXPECT_EQ(observedPeer, clientLocalAddr);

  evb->runInEventBaseThreadAndWait([&] { socket.reset(); });
}

// Records what an extension could read back for a given inbound request header.
struct HeaderReadingExtension {
  static constexpr bool kUsesHeaders = true;

  std::string* seen{nullptr};
  std::atomic<bool>* ran{nullptr};

  ftt::RequestVerdict onRequest(
      const ftt::ThriftRequestView& request) noexcept {
    if (const auto* value = request.header("x-client-header")) {
      *seen = *value;
    }
    ran->store(true, std::memory_order_relaxed);
    return ftt::RequestVerdict::proceed();
  }
};

struct FollyContextObservingExtension {
  explicit FollyContextObservingExtension(
      FollyContextObservations* observations) noexcept
      : observations_(observations) {}

  ftt::RequestVerdict onRequest(
      const ftt::ThriftRequestView& /*request*/) noexcept {
    auto* context = folly::RequestContext::try_get();
    observations_->request.store(context, std::memory_order_relaxed);
    if (context != nullptr) {
      context->setContextData(
          follyContextMarkerToken(), std::make_unique<FollyContextMarker>());
    }
    sawRequest_ = true;
    return ftt::RequestVerdict::proceed();
  }

  void onResponse(const ftt::ThriftResponseView& /*response*/) noexcept {
    if (!sawRequest_) {
      return;
    }
    observations_->response.store(
        folly::RequestContext::try_get(), std::memory_order_relaxed);
    observations_->responseSawMarker.store(
        hasFollyContextMarker(), std::memory_order_relaxed);
  }

 private:
  FollyContextObservations* observations_{nullptr};
  bool sawRequest_{false};
};

// Drives one `add(7, 35)` RPC carrying a custom request header.
int64_t addRoundTripWithHeader(
    const folly::SocketAddress& addr,
    const std::string& key,
    const std::string& value) {
  folly::ScopedEventBaseThread clientThread;
  auto* evb = clientThread.getEventBase();
  std::unique_ptr<apache::thrift::Client<integration::FastThriftServer>> client;
  evb->runInEventBaseThreadAndWait([&] {
    auto socket = folly::AsyncSocket::newSocket(evb, addr);
    auto channel =
        apache::thrift::RocketClientChannel::newChannel(std::move(socket));
    client =
        std::make_unique<apache::thrift::Client<integration::FastThriftServer>>(
            std::move(channel));
  });

  folly::Baton<> done;
  int64_t sum = 0;
  evb->runInEventBaseThread([&] {
    apache::thrift::RpcOptions options;
    options.setWriteHeader(key, value);
    client->semifuture_add(options, 7, 35)
        .via(evb)
        .thenValue([&](int64_t v) {
          sum = v;
          done.post();
        })
        .thenError([&](const folly::exception_wrapper& ew) {
          ADD_FAILURE() << "RPC failed: " << folly::exceptionStr(ew);
          done.post();
        });
  });
  EXPECT_TRUE(done.try_wait_for(std::chrono::seconds{10}));
  evb->runInEventBaseThreadAndWait([&] { client.reset(); });
  return sum;
}

// enableRequestHeaders installs a handler that runs upstream of every
// extension and moves the client's header map onto the per-request context,
// which is where the extension reads it from.
TEST(FastThriftServerExtensionTest, ExtensionReadsRequestHeaderWithHeadersOn) {
  THRIFT_FLAG_SET_MOCK(rocket_client_binary_rpc_metadata_encoding, true);

  auto handler = std::make_shared<TestHandler>();
  std::string seen;
  std::atomic<bool> ran{false};

  ftt::FastThriftServer server(makeHeadersConfig());
  server.setInterface(handler);
  server.addModule(
      ftt::FastServerModule("headers")
          .addThriftExtension<HeaderReadingExtension>(&seen, &ran));
  server.start();

  EXPECT_EQ(
      addRoundTripWithHeader(server.getAddress(), "x-client-header", "abc"),
      42);
  EXPECT_TRUE(ran.load());
  EXPECT_EQ(seen, "abc");
}

TEST(
    FastThriftServerExtensionTest,
    FollyRequestContextSpansExtensionsAndApplication) {
  THRIFT_FLAG_SET_MOCK(rocket_client_binary_rpc_metadata_encoding, true);

  FollyContextObservations observations;
  auto handler = std::make_shared<FollyContextObservingHandler>(observations);
  auto config = makeLoopbackConfig();
  config.numCPUThreads = 1;
  ftt::FastThriftServer server(std::move(config));
  server.setInterface(handler);
  server.addModule(
      ftt::FastServerModule("folly_request_context")
          .addThriftExtension<FollyContextObservingExtension>(&observations));
  server.start();

  EXPECT_EQ(addRoundTrip(server.getAddress()), 42);

  auto* const requestContext =
      observations.request.load(std::memory_order_relaxed);
  ASSERT_NE(requestContext, nullptr);
  EXPECT_EQ(
      observations.application.load(std::memory_order_relaxed), requestContext);
  EXPECT_EQ(
      observations.response.load(std::memory_order_relaxed), requestContext);
  EXPECT_TRUE(
      observations.applicationSawMarker.load(std::memory_order_relaxed));
  EXPECT_TRUE(observations.responseSawMarker.load(std::memory_order_relaxed));
}

// With the header handler off there is nowhere for the extension to read
// headers from, so the server refuses the module outright rather than let it
// run against headers it will never see.
TEST(FastThriftServerExtensionTest, HeaderExtensionRejectedWithHeadersOff) {
  auto handler = std::make_shared<TestHandler>();
  std::string seen;
  std::atomic<bool> ran{false};

  ftt::FastThriftServer server(makeLoopbackConfig());
  server.setInterface(handler);

  EXPECT_THROW(
      server.addModule(
          ftt::FastServerModule("headers")
              .addThriftExtension<HeaderReadingExtension>(&seen, &ran)),
      std::logic_error);
}

// Stamps a response header on the way out, then reads it straight back. The
// read-back is what pins that the mutator and the view address the same place.
struct ResponseHeaderExtension {
  static constexpr bool kUsesHeaders = true;

  std::string* readBack{nullptr};

  void onResponse(ftt::ThriftResponseMutator& response) noexcept {
    response.setHeader("x-server-header", "xyz");
    if (const auto* value = response.header("x-server-header")) {
      *readBack = *value;
    }
  }
};

// Drives one `add(7, 35)` RPC and reports the response header the client saw.
//
// Synchronous, on an EventBase nothing else is looping: the blocking form is
// the one that projects response headers back into the caller's RpcOptions,
// where semifuture_ drops them with the ClientReceiveState.
std::string addRoundTripReadingHeader(
    const folly::SocketAddress& addr, const std::string& key) {
  folly::EventBase evb;
  auto channel = apache::thrift::RocketClientChannel::newChannel(
      folly::AsyncSocket::newSocket(&evb, addr));
  apache::thrift::Client<integration::FastThriftServer> client(
      std::move(channel));

  apache::thrift::RpcOptions options;
  EXPECT_EQ(client.sync_add(options, 7, 35), 42);

  const auto& headers = options.getReadHeaders();
  const auto it = headers.find(key);
  return it == headers.end() ? std::string{} : it->second;
}

// A response header an extension sets on the per-request context still reaches
// the wire: the context is drained onto the outgoing metadata by the request
// context handler, which sits downstream of every extension on the write path.
TEST(FastThriftServerExtensionTest, ExtensionResponseHeaderReachesClient) {
  THRIFT_FLAG_SET_MOCK(rocket_client_binary_rpc_metadata_encoding, true);

  auto handler = std::make_shared<TestHandler>();
  std::string readBack;

  ftt::FastThriftServer server(makeHeadersConfig());
  server.setInterface(handler);
  server.addModule(
      ftt::FastServerModule("response-headers")
          .addThriftExtension<ResponseHeaderExtension>(&readBack));
  server.start();

  EXPECT_EQ(
      addRoundTripReadingHeader(server.getAddress(), "x-server-header"), "xyz");
  EXPECT_EQ(readBack, "xyz");
}

template <typename Server, typename Factory>
concept AcceptsNativeHandlerFactory =
    requires(Server& server, std::vector<Factory> factories) {
      server.addNativeThriftPipelineHandlers(std::move(factories));
    };

template <typename Server, typename Module>
concept AcceptsModule = requires(Server& server, Module module) {
  server.addModule(std::move(module));
};

template <typename Server, typename Builder>
concept AcceptsRocketBuilder = requires(Server& server, Builder builder) {
  server.setRocketConnectionBuilder(std::move(builder));
};

template <typename Server>
concept ExposesServerImplementationTemplate = requires {
  typename Server::template FastThriftServerT<ftt::ChannelPipelineMode::Static>;
};

static_assert(
    !ExposesServerImplementationTemplate<ftt::detail::FastThriftServerBase>);

static_assert(
    ftt::FastThriftServer::kPipelineMode == ftt::ChannelPipelineMode::Static);
static_assert(
    ftt::DynamicFastThriftServer::kPipelineMode ==
    ftt::ChannelPipelineMode::Dynamic);
static_assert(!AcceptsNativeHandlerFactory<
              ftt::FastThriftServer,
              ftt::server::ThriftPipelineHandlerFactory>);
static_assert(AcceptsNativeHandlerFactory<
              ftt::FastThriftServer,
              ftt::server::StaticThriftPipelineHandlerFactory>);
static_assert(AcceptsNativeHandlerFactory<
              ftt::DynamicFastThriftServer,
              ftt::server::ThriftPipelineHandlerFactory>);
static_assert(
    !AcceptsModule<ftt::FastThriftServer, ftt::DynamicFastServerModule>);
static_assert(AcceptsModule<ftt::FastThriftServer, ftt::FastServerModule>);
static_assert(
    AcceptsModule<ftt::DynamicFastThriftServer, ftt::FastServerModule>);
static_assert(
    AcceptsModule<ftt::DynamicFastThriftServer, ftt::DynamicFastServerModule>);
static_assert(!AcceptsRocketBuilder<
              ftt::FastThriftServer,
              ftt::server::RocketServerConnectionBuilder>);
static_assert(AcceptsRocketBuilder<
              ftt::FastThriftServer,
              ftt::server::StaticRocketServerConnectionBuilder>);
static_assert(AcceptsRocketBuilder<
              ftt::DynamicFastThriftServer,
              ftt::server::RocketServerConnectionBuilder>);

TEST(FastThriftServerExtensionTest, DynamicModeAllowsDynamicHandler) {
  THRIFT_FLAG_SET_MOCK(rocket_client_binary_rpc_metadata_encoding, true);

  std::atomic<int> reads{0};
  std::atomic<int> sequence{0};
  std::atomic<int> firstRead{-1};
  ftt::DynamicFastThriftServer server(makeLoopbackConfig());
  server.setInterface(std::make_shared<TestHandler>());
  server.addNativeThriftPipelineHandlers(
      {ftt::server::ThriftPipelineHandlerFactory(
          [&](ftt::ExtensionStateStore&) {
            return cp::detail::makeHandlerNode<StaticSelectionRecordingHandler>(
                ftt::server::deriveThriftPipelineHandlerId("dynamic", 0),
                std::make_unique<StaticSelectionRecordingHandler>(
                    &reads, &sequence, &firstRead));
          })});
  server.start();

  EXPECT_EQ(addRoundTrip(server.getAddress()), 42);
  EXPECT_GT(reads.load(), 0);
}

TEST(
    FastThriftServerExtensionTest, DefaultStaticModeSupportsObserverExtension) {
  THRIFT_FLAG_SET_MOCK(rocket_client_binary_rpc_metadata_encoding, true);

  auto handler = std::make_shared<TestHandler>();
  std::atomic<int> requests{0};
  std::atomic<int> responses{0};
  auto config = makeLoopbackConfig();

  ftt::FastThriftServer server(std::move(config));
  server.setInterface(handler);
  server.addModule(
      ftt::FastServerModule("observer")
          .addThriftExtension<CountingExtension>(
              &requests, &responses, nullptr, nullptr));
  server.start();

  EXPECT_EQ(addRoundTrip(server.getAddress()), 42);
  EXPECT_GE(requests.load(), 1);
  EXPECT_GE(responses.load(), 1);
}

TEST(FastThriftServerExtensionTest, StaticModulePreservesRegistrationOrder) {
  THRIFT_FLAG_SET_MOCK(rocket_client_binary_rpc_metadata_encoding, true);

  std::atomic<int> sequence{0};
  std::atomic<int> first{-1};
  std::atomic<int> second{-1};
  std::atomic<int> third{-1};
  std::atomic<int> fourth{-1};
  auto config = makeLoopbackConfig();

  ftt::FastThriftServer server(std::move(config));
  server.setInterface(std::make_shared<TestHandler>());
  server.addModule(
      ftt::StaticFastServerModule("typed")
          .addThriftExtension<CountingExtension>(
              nullptr, nullptr, &sequence, &first)
          .addThriftExtension<CountingExtension>(
              nullptr, nullptr, &sequence, &second)
          .addThriftExtension<CountingExtension>(
              nullptr, nullptr, &sequence, &third)
          .addThriftExtension<CountingExtension>(
              nullptr, nullptr, &sequence, &fourth));
  server.start();

  EXPECT_EQ(addRoundTrip(server.getAddress()), 42);
  EXPECT_EQ(
      (std::array{first.load(), second.load(), third.load(), fourth.load()}),
      (std::array{0, 1, 2, 3}));
}

TEST(
    FastThriftServerExtensionTest,
    StaticModeKeepsMixedHandlerAndSegmentRegistrationsStatic) {
  THRIFT_FLAG_SET_MOCK(rocket_client_binary_rpc_metadata_encoding, true);

  std::atomic<int> dynamicFactories{0};
  std::atomic<int> staticFactories{0};
  std::atomic<int> handlerReads{0};
  std::atomic<int> sequence{0};
  std::atomic<int> handlerFirstRead{-1};
  std::atomic<int> extensionFirstRequest{-1};
  const auto handlerId =
      ftt::server::deriveThriftPipelineHandlerId("mixed-static", 0);

  std::vector<ftt::server::StaticThriftPipelineHandlerFactory> factories;
  factories.emplace_back(
      [&](ftt::ExtensionStateStore&) {
        dynamicFactories.fetch_add(1, std::memory_order_relaxed);
        return cp::detail::makeHandlerNode<StaticSelectionRecordingHandler>(
            handlerId,
            std::make_unique<StaticSelectionRecordingHandler>(
                &handlerReads, &sequence, &handlerFirstRead));
      },
      [&](ftt::ExtensionStateStore&) {
        staticFactories.fetch_add(1, std::memory_order_relaxed);
        return cp::detail::makeErasedStaticHandler<
            StaticSelectionRecordingHandler>(
            handlerId, &handlerReads, &sequence, &handlerFirstRead);
      });

  auto config = makeLoopbackConfig();
  ftt::FastThriftServer server(std::move(config));
  server.setInterface(std::make_shared<TestHandler>());
  server.addNativeThriftPipelineHandlers(std::move(factories));
  server.addModule(
      ftt::StaticFastServerModule("typed")
          .addThriftExtension<CountingExtension>(
              nullptr, nullptr, &sequence, &extensionFirstRequest));
  server.start();

  EXPECT_EQ(addRoundTrip(server.getAddress()), 42);
  EXPECT_EQ(dynamicFactories.load(), 0);
  EXPECT_GT(staticFactories.load(), 0);
  EXPECT_GT(handlerReads.load(), 0);
  EXPECT_GE(handlerFirstRead.load(), 0);
  EXPECT_GT(extensionFirstRequest.load(), handlerFirstRead.load());
}

TEST(FastThriftServerExtensionTest, RequestStateSurvivesUntilResponse) {
  THRIFT_FLAG_SET_MOCK(rocket_client_binary_rpc_metadata_encoding, true);

  auto handler = std::make_shared<TestHandler>();
  std::atomic<int> observed{0};
  ftt::FastThriftServer server(makeLoopbackConfig());
  server.setInterface(handler);
  server.addModule(
      ftt::FastServerModule("request_state")
          .addThriftExtension<RequestStateExtension>(&observed));
  server.start();

  EXPECT_EQ(addRoundTrip(server.getAddress()), 42);
  EXPECT_EQ(observed.load(), 42);
}
// Extensions are spliced in registration order (head→tail), across modules and
// within a module: module "first" (A) runs before module "second" (B, C), and
// within "second" B runs before C. First-request sequence numbers pin
// registration order == pipeline order.
TEST(FastThriftServerExtensionTest, ExtensionsSplicedInRegistrationOrder) {
  THRIFT_FLAG_SET_MOCK(rocket_client_binary_rpc_metadata_encoding, true);

  std::atomic<int> g{0};
  std::atomic<int> seqA{-1};
  std::atomic<int> seqB{-1};
  std::atomic<int> seqC{-1};

  auto handler = std::make_shared<TestHandler>();
  ftt::FastThriftServer server(makeLoopbackConfig());
  server.setInterface(handler);

  server.addModule(
      ftt::FastServerModule("first").addThriftExtension<CountingExtension>(
          nullptr, nullptr, &g, &seqA));
  server.addModule(
      ftt::FastServerModule("second")
          .addThriftExtension<CountingExtension>(nullptr, nullptr, &g, &seqB)
          .addThriftExtension<CountingExtension>(nullptr, nullptr, &g, &seqC));
  server.start();

  EXPECT_EQ(addRoundTrip(server.getAddress()), 42);
  EXPECT_GE(seqA.load(), 0);
  EXPECT_GE(seqB.load(), 0);
  EXPECT_GE(seqC.load(), 0);
  EXPECT_LT(seqA.load(), seqB.load());
  EXPECT_LT(seqB.load(), seqC.load());
}

// Two extensions on one connection that name the same ConnState are handed the
// same object — the property that lets an identity resolved by one gate be
// authorized against by the next without either reaching into a shared slot on
// the connection context.
TEST(FastThriftServerExtensionTest, ExtensionsShareConnStateOnAConnection) {
  THRIFT_FLAG_SET_MOCK(rocket_client_binary_rpc_metadata_encoding, true);

  auto handler = std::make_shared<TestHandler>();
  StateObservations observed;

  ftt::FastThriftServer server(makeLoopbackConfig());
  server.setInterface(handler);
  server.addModule(
      ftt::FastServerModule("shared")
          .addThriftExtension<SharedStateExtension>(&observed)
          .addThriftExtension<SharedStateExtension>(&observed));
  server.start();

  EXPECT_EQ(addRoundTrip(server.getAddress()), 42);

  ASSERT_EQ(observed.size(), 2);
  EXPECT_EQ(observed.id(0), observed.id(1));
  // Both extensions incremented the one object: the second saw the first's
  // write.
  EXPECT_EQ(observed.count(0), 1);
  EXPECT_EQ(observed.count(1), 2);
}

// Different ConnState types are different slots, so unrelated extensions on the
// same connection cannot alias however they are composed.
TEST(FastThriftServerExtensionTest, DistinctConnStateTypesDoNotAlias) {
  THRIFT_FLAG_SET_MOCK(rocket_client_binary_rpc_metadata_encoding, true);

  auto handler = std::make_shared<TestHandler>();
  StateObservations observed;

  ftt::FastThriftServer server(makeLoopbackConfig());
  server.setInterface(handler);
  server.addModule(
      ftt::FastServerModule("mixed")
          .addThriftExtension<SharedStateExtension>(&observed)
          .addThriftExtension<OtherStateExtension>(&observed));
  server.start();

  EXPECT_EQ(addRoundTrip(server.getAddress()), 42);

  ASSERT_EQ(observed.size(), 2);
  EXPECT_NE(observed.id(0), observed.id(1));
}

// The state is per connection, not per server: a second connection gets its own
// object, so nothing an extension caches can bleed between callers.
TEST(FastThriftServerExtensionTest, ConnStateIsPerConnection) {
  THRIFT_FLAG_SET_MOCK(rocket_client_binary_rpc_metadata_encoding, true);

  auto handler = std::make_shared<TestHandler>();
  StateObservations observed;

  ftt::FastThriftServer server(makeLoopbackConfig());
  server.setInterface(handler);
  server.addModule(
      ftt::FastServerModule("percon").addThriftExtension<SharedStateExtension>(
          &observed));
  server.start();

  // addRoundTrip tears its client down before returning, so these are two
  // distinct connections rather than two requests on one.
  EXPECT_EQ(addRoundTrip(server.getAddress()), 42);
  EXPECT_EQ(addRoundTrip(server.getAddress()), 42);

  ASSERT_EQ(observed.size(), 2);
  EXPECT_NE(observed.id(0), observed.id(1));
  // A fresh object, not a reused one carrying the first connection's count.
  EXPECT_EQ(observed.count(1), 1);
}

// The native-handler allowlist denies arbitrary user types and always permits
// the framework's own extension adapter — so addThriftExtension needs no
// allowlist entry while a raw user handler would be rejected at compile time by
// makeThriftPipelineHandlerFactory's static_assert.
TEST(FastThriftServerExtensionTest, NativeHandlerAllowlistGate) {
  static_assert(
      !ftt::server::kIsAllowedNativeThriftHandler<CountingExtension>,
      "a raw user type must not be an allowlisted native handler");
  static_assert(
      ftt::server::kIsAllowedNativeThriftHandler<
          ftt::server::ThriftExtensionPipelineHandler<CountingExtension>>,
      "the framework extension adapter must always be allowlisted");
}

// ThriftRequestMutator derives from ThriftRequestView, so a mutator binds to a
// const-view parameter but not vice versa. The adapter relies on that asymmetry
// to dispatch: it tests the view form first, which is the only reason a
// read/write extension is not silently handed a read-only view.
TEST(FastThriftServerExtensionTest, ExtensionDispatchDiscriminatesAccess) {
  static_assert(
      ftt::HasRequestViewCallback<CountingExtension>,
      "a read-only extension must match the view form");
  static_assert(
      ftt::HasRequestMutatorCallback<RejectingExtension>,
      "a read/write extension must match the mutator form");
  static_assert(
      !ftt::HasRequestViewCallback<RejectingExtension>,
      "a read/write extension must not match the view form, or view-first "
      "dispatch would strip its write access");
}

// A read/write extension that rejects short-circuits the pipeline: the client
// receives an error and the service handler never runs.
TEST(FastThriftServerExtensionTest, ModifierRejectionShortCircuitsService) {
  THRIFT_FLAG_SET_MOCK(rocket_client_binary_rpc_metadata_encoding, true);

  auto handler = std::make_shared<TestHandler>();
  std::atomic<int> rejections{0};
  FollyContextObservations observations;

  ftt::FastThriftServer server(makeHeadersConfig());
  server.setInterface(handler);
  server.addModule(
      ftt::FastServerModule("reject")
          .addThriftExtension<FollyContextObservingExtension>(&observations)
          .addThriftExtension<RejectingExtension>(&rejections));
  server.start();

  const auto observation = addRoundTripObservingRejection(server.getAddress());
  EXPECT_TRUE(observation.failed);
  EXPECT_EQ(observation.message, "denied by extension");
  EXPECT_EQ(
      observation.headers.at(std::string(apache::thrift::detail::kHeaderUex)),
      "TypedExtensionRejection");
  EXPECT_EQ(
      observation.headers.at(std::string(apache::thrift::detail::kHeaderEx)),
      kAppClientErrorCode);
  EXPECT_EQ(observation.headers.at("x-rejected-by-extension"), "add");
  EXPECT_GE(rejections.load(), 1);
  auto* const requestContext =
      observations.request.load(std::memory_order_relaxed);
  ASSERT_NE(requestContext, nullptr);
  EXPECT_EQ(observations.application.load(std::memory_order_relaxed), nullptr);
  EXPECT_EQ(
      observations.response.load(std::memory_order_relaxed), requestContext);
  EXPECT_TRUE(observations.responseSawMarker.load(std::memory_order_relaxed));
}

TEST(FastThriftServerExtensionTest, ResponseViewClassifiesApplicationErrors) {
  auto appError = ftt::makeAppErrorMessage(
      1,
      "TypedExtensionRejection",
      "denied by extension",
      apache::thrift::ErrorBlame::CLIENT);
  const ftt::ThriftResponseView appErrorView(appError);
  EXPECT_TRUE(appErrorView.isApplicationError());

  auto success = ftt::makeResponseMessage(
      1, nullptr, std::make_unique<apache::thrift::ResponseRpcMetadata>());
  const ftt::ThriftResponseView successView(success);
  EXPECT_FALSE(successView.isApplicationError());
}

// The connection callbacks bracket the SETUP exchange: attempted fires once
// the client's setup metadata is parsed and version-negotiated, established
// once the SETUP response is away, and both land before the first request.
// A negotiated version and a non-null client setup prove the events are driven
// by the real SETUP frame rather than by socket accept.
TEST(FastThriftServerConnectionExtensionTest, LifecycleBracketsSetupExchange) {
  THRIFT_FLAG_SET_MOCK(rocket_client_binary_rpc_metadata_encoding, true);

  auto handler = std::make_shared<TestHandler>();
  ConnectionRecorder rec;

  ftt::FastThriftServer server(makeLoopbackConfig());
  server.setInterface(handler);
  server.addModule(
      ftt::FastServerModule("lifecycle")
          .addThriftExtension<ConnectionLifecycleExtension>(&rec));
  server.start();

  EXPECT_EQ(addRoundTrip(server.getAddress()), 42);

  EXPECT_GE(rec.attempted.load(), 0);
  EXPECT_LT(rec.attempted.load(), rec.established.load());
  EXPECT_LT(rec.established.load(), rec.request.load());
  EXPECT_GT(rec.negotiatedVersion.load(), 0);
  EXPECT_TRUE(rec.sawClientSetup.load());
}

TEST(
    FastThriftServerConnectionExtensionTest,
    ModuleObservesDedicatedAcceptorQueue) {
  THRIFT_FLAG_SET_MOCK(rocket_client_binary_rpc_metadata_encoding, true);

  auto handler = std::make_shared<TestHandler>();
  PendingConnectionRecordingExtension::reset();
  auto config = makeLoopbackConfig();
  config.numIOThreads = 1;
  config.numConnectionSetupThreads = 1;

  ftt::FastThriftServer server(std::move(config));
  server.setInterface(handler);
  server.addModule(
      ftt::FastServerModule("pending-connections")
          .addThriftExtension<PendingConnectionRecordingExtension>());
  server.start();

  EXPECT_EQ(addRoundTrip(server.getAddress()), 42);
  EXPECT_TRUE(waitFor(PendingConnectionRecordingExtension::enqueued, 1));
  EXPECT_TRUE(waitFor(PendingConnectionRecordingExtension::dequeued, 1));
  EXPECT_EQ(
      PendingConnectionRecordingExtension::enqueued.load(
          std::memory_order_relaxed),
      PendingConnectionRecordingExtension::dequeued.load(
          std::memory_order_relaxed));
  EXPECT_EQ(
      PendingConnectionRecordingExtension::dropped.load(
          std::memory_order_relaxed),
      0);
}

// Refusing at the first decision point short-circuits: no SETUP response is
// built, the established callback never runs, and the client's RPC fails.
TEST(FastThriftServerConnectionExtensionTest, AttemptedRejectionRefusesSetup) {
  THRIFT_FLAG_SET_MOCK(rocket_client_binary_rpc_metadata_encoding, true);

  auto handler = std::make_shared<TestHandler>();
  ConnectionRecorder rec;

  ftt::FastThriftServer server(makeLoopbackConfig());
  server.setInterface(handler);
  server.addModule(
      ftt::FastServerModule("reject-attempt")
          .addThriftExtension<RejectAtAttemptedExtension>(&rec));
  server.start();

  EXPECT_TRUE(addRoundTripFails(server.getAddress()));
  EXPECT_GE(rec.attempted.load(), 0);
  EXPECT_EQ(rec.established.load(), -1);
  EXPECT_EQ(rec.request.load(), -1);
}

// An extension may hook teardown alone, with no request callback. The closed
// callback fires once the connection the client dropped has settled.
TEST(FastThriftServerConnectionExtensionTest, ClosedFiresForSettledConnection) {
  THRIFT_FLAG_SET_MOCK(rocket_client_binary_rpc_metadata_encoding, true);

  auto handler = std::make_shared<TestHandler>();
  ConnectionRecorder rec;

  ftt::FastThriftServer server(makeLoopbackConfig());
  server.setInterface(handler);
  server.addModule(
      ftt::FastServerModule("closed-only")
          .addThriftExtension<ClosedOnlyExtension>(&rec));
  server.start();

  // addRoundTrip tears its client down before returning, so the server-side
  // close is already in flight by the time we start waiting.
  EXPECT_EQ(addRoundTrip(server.getAddress()), 42);
  EXPECT_TRUE(waitFor(rec.closed, 1));
}

// An extension can contribute to the SETUP response the client receives, not
// just accept or refuse the connection. Dispatch must hand it a mutator: the
// contribution has to land before the response is serialized.
TEST(FastThriftServerConnectionExtensionTest, ContributesToSetupResponse) {
  static_assert(
      ftt::HasConnectionAttemptedMutatorCallback<PolicyContributingExtension>,
      "a contributing extension must match the mutator form");
  static_assert(
      !ftt::HasConnectionAttemptedViewCallback<PolicyContributingExtension>,
      "a contributing extension must not match the view form, or view-first "
      "dispatch would strip its write access");
  static_assert(
      ftt::HasConnectionAttemptedViewCallback<ConnectionLifecycleExtension>,
      "a read-only extension must match the view form");

  THRIFT_FLAG_SET_MOCK(rocket_client_binary_rpc_metadata_encoding, true);

  auto handler = std::make_shared<TestHandler>();
  ConnectionRecorder rec;

  ftt::FastThriftServer server(makeLoopbackConfig());
  server.setInterface(handler);
  server.addModule(
      ftt::FastServerModule("policy")
          .addThriftExtension<PolicyContributingExtension>(&rec));
  server.start();

  // The contribution must not disturb the exchange: the RPC still completes.
  EXPECT_EQ(addRoundTrip(server.getAddress()), 42);
  EXPECT_GE(rec.attempted.load(), 0);
}

// Callback families are independent: a teardown-only extension is a valid
// extension with no request callback, and an extension that hooks no
// connection event subscribes to nothing, so it is linked into no event list.
TEST(FastThriftServerConnectionExtensionTest, CallbackFamiliesAreIndependent) {
  static_assert(
      !ftt::ThriftExtensionHandler<ClosedOnlyExtension>,
      "a teardown-only extension has no request callback");
  static_assert(
      ftt::ThriftConnectionExtensionHandler<ClosedOnlyExtension>,
      "a teardown-only extension is still a connection extension");
  static_assert(
      !ftt::ThriftConnectionExtensionHandler<CountingExtension>,
      "a request/response extension hooks no connection event");
  static_assert(
      std::is_same_v<
          typename ftt::server::ThriftExtensionPipelineHandler<
              CountingExtension>::SubscribedEvents,
          cp::Events<>>,
      "an extension with no connection callback must subscribe to no events");
}

// Egress going idle reaches the extension, and the view hands it the
// per-connection slot the embedder populated at accept time — the state a
// service's admission decision is actually made against.
TEST(FastThriftServerBackpressureExtensionTest, EgressDrainedSeesUserData) {
  THRIFT_FLAG_SET_MOCK(rocket_client_binary_rpc_metadata_encoding, true);

  int marker = 0;
  BackpressureRecorder rec;
  auto handler = std::make_shared<TestHandler>();

  ftt::FastThriftServer server(makeLoopbackConfig());
  server.setInterface(handler);
  server.setOnConnectionAccepted([&](ftt::ThriftConnContext* conn) {
    conn->setUserData(
        apache::thrift::fast_thrift::rocket::with_custom_deleter(
            &marker, +[](void*) noexcept {}));
  });
  server.addModule(
      ftt::FastServerModule("backpressure")
          .addThriftExtension<BackpressureExtension>(&rec));
  server.start();

  EXPECT_EQ(addRoundTrip(server.getAddress()), 42);

  EXPECT_TRUE(waitFor(rec.drained, 1));
  EXPECT_EQ(rec.attached.load(), 1);
  EXPECT_EQ(rec.userData.load(), &marker);
}

// A pause that is never lifted stops the connection admitting requests. This
// is the test that pins the mechanism actually working — without it the
// resume test below would pass whether or not the pause ever engaged.
TEST(
    FastThriftServerBackpressureExtensionTest, PauseWithoutResumeStopsServing) {
  THRIFT_FLAG_SET_MOCK(rocket_client_binary_rpc_metadata_encoding, true);

  BackpressureRecorder rec;
  rec.pauseOnce.store(true, std::memory_order_relaxed);
  rec.resumeAfterPause.store(false, std::memory_order_relaxed);
  auto handler = std::make_shared<TestHandler>();

  // The connection is deliberately left wedged, so shut the terminal-phase
  // deadlines right down — otherwise teardown waits out the full drain and
  // reap before the test can finish.
  auto config = makeLoopbackConfig();
  config.drainTimeout = std::chrono::milliseconds{100};
  config.reapTimeout = std::chrono::milliseconds{200};

  ftt::FastThriftServer server(std::move(config));
  server.setInterface(handler);
  server.setIOThreadPool(makeFixedSizePool(1));
  server.addModule(
      ftt::FastServerModule("backpressure")
          .addThriftExtension<BackpressureExtension>(&rec));
  server.start();

  EXPECT_FALSE(roundTripsOnOneConnection(
      server.getAddress(), 2, std::chrono::seconds{5}));
  EXPECT_GE(rec.drained.load(), 1);
}

// The resumer lifts it again. Same shape as above, so the only difference is
// whether the deferred resume runs — which makes this a direct check that
// resume is what recovers the connection, not that the pause never took.
TEST(FastThriftServerBackpressureExtensionTest, PauseThenResumeKeepsServing) {
  THRIFT_FLAG_SET_MOCK(rocket_client_binary_rpc_metadata_encoding, true);

  auto verify = []<typename Server>() {
    BackpressureRecorder rec;
    rec.pauseOnce.store(true, std::memory_order_relaxed);
    auto handler = std::make_shared<TestHandler>();

    auto config = makeLoopbackConfig();
    Server server(std::move(config));
    server.setInterface(handler);
    server.setIOThreadPool(makeFixedSizePool(1));
    server.addModule(
        ftt::FastServerModule("backpressure")
            .addThriftExtension<BackpressureExtension>(&rec));
    server.start();

    EXPECT_TRUE(roundTripsOnOneConnection(server.getAddress(), 2));
    EXPECT_GE(rec.drained.load(), 1);
    // The resumer named the EventBase the connection was running on, so a
    // resume deferred onto it is guaranteed to run.
    EXPECT_TRUE(rec.resumerEvbLooping.load(std::memory_order_relaxed));
  };
  verify.template operator()<ftt::FastThriftServer>();
  verify.template operator()<ftt::DynamicFastThriftServer>();
}

// The extension outlives the connection, so a resume that arrives after
// teardown must do nothing rather than reach a dead pipeline. Deferred resume
// makes this ordinary, not exotic.
TEST(FastThriftServerBackpressureExtensionTest, ResumerIsInertAfterClose) {
  THRIFT_FLAG_SET_MOCK(rocket_client_binary_rpc_metadata_encoding, true);

  BackpressureRecorder rec;
  rec.pauseOnce.store(true, std::memory_order_relaxed);
  rec.resumeAfterPause.store(false, std::memory_order_relaxed);
  auto handler = std::make_shared<TestHandler>();

  ftt::FastThriftServer server(makeLoopbackConfig());
  server.setInterface(handler);
  server.addModule(
      ftt::FastServerModule("backpressure")
          .addThriftExtension<BackpressureExtension>(&rec));
  server.start();

  EXPECT_EQ(addRoundTrip(server.getAddress()), 42);
  EXPECT_TRUE(waitFor(rec.attached, 1));

  ftt::ReadResumer resumer;
  {
    std::lock_guard<std::mutex> lock(rec.resumerMutex);
    resumer = rec.resumer;
  }
  server.stop();

  EXPECT_FALSE(static_cast<bool>(resumer));
  resumer.resume(); // must not reach the torn-down pipeline
}

// One extension implementing both families gets one instance per connection:
// the drain callback can read state the request callback wrote. Registering
// the same extension twice to get both would give two instances and split that
// state, which is why there is a single registration entry point.
TEST(FastThriftServerBackpressureExtensionTest, OneInstanceServesBothFamilies) {
  THRIFT_FLAG_SET_MOCK(rocket_client_binary_rpc_metadata_encoding, true);

  BackpressureRecorder rec;
  auto handler = std::make_shared<TestHandler>();

  ftt::FastThriftServer server(makeLoopbackConfig());
  server.setInterface(handler);
  server.addModule(
      ftt::FastServerModule("both")
          .addThriftExtension<ObservingBackpressureExtension>(&rec));
  server.start();

  EXPECT_EQ(addRoundTrip(server.getAddress()), 42);

  // Only counted when the drain callback saw the request the same instance
  // handled.
  EXPECT_TRUE(waitFor(rec.drained, 1));
}

// The families are independent concepts: an observer is not an
// admission-control extension and vice versa, and an extension can be both.
TEST(FastThriftServerBackpressureExtensionTest, ConceptsAreIndependent) {
  static_assert(
      !ftt::ThriftBackpressureExtensionHandler<CountingExtension>,
      "a request/response extension does not control reads");
  static_assert(
      ftt::ThriftBackpressureExtensionHandler<DrainOnlyExtension>,
      "declaring onEgressDrained opts in to admission control");
  static_assert(
      !ftt::ThriftExtensionHandler<DrainOnlyExtension>,
      "an admission-control extension needs no request callback");
  static_assert(
      ftt::ThriftBackpressureExtensionHandler<ObservingBackpressureExtension> &&
          ftt::ThriftExtensionHandler<ObservingBackpressureExtension>,
      "one extension may implement both families");
}

TEST(
    FastThriftServerBackpressureExtensionTest,
    DefaultConfigAcceptsBackpressureExtension) {
  BackpressureRecorder rec;
  auto config = makeLoopbackConfig();
  ftt::FastThriftServer server(std::move(config));
  EXPECT_NO_THROW(server.addModule(
      ftt::FastServerModule("backpressure")
          .addThriftExtension<BackpressureExtension>(&rec)));
}

// Registering two modules with the same name on the server is rejected.
TEST(FastThriftServerPipelineHandlerTest, DuplicateModuleNameThrows) {
  ftt::FastThriftServer server(makeLoopbackConfig());
  server.addModule(ftt::FastServerModule("dup"));
  EXPECT_THROW(
      server.addModule(ftt::FastServerModule("dup")), std::logic_error);
}

TEST(FastThriftServerPipelineHandlerTest, InvalidModuleNamesThrow) {
  ftt::FastThriftServer server(makeLoopbackConfig());

  EXPECT_THROW(server.addModule(ftt::FastServerModule("")), std::logic_error);
  EXPECT_THROW(
      server.addModule(ftt::FastServerModule("__cpp2_event_handler_bridge")),
      std::logic_error);
}

// The two-level id derivation yields a distinct id for every (namespace,
// index) pair — no cross-namespace or arithmetic-adjacency collisions.
TEST(FastServerModuleTest, DerivedHandlerIdsAreUniqueAcrossNamespaces) {
  const std::vector<std::string_view> namespaces = {
      "", "a", "b", "moduleA", "moduleB", "logging", "metrics"};
  std::set<cp::HandlerId> ids;
  std::size_t count = 0;
  for (auto ns : namespaces) {
    for (std::size_t i = 0; i < 32; ++i) {
      ids.insert(ftt::server::deriveThriftPipelineHandlerId(ns, i));
      ++count;
    }
  }
  EXPECT_EQ(ids.size(), count);
}

// Server holds a reference to the embedder pool while running and releases
// it on destruction. The dtor joins the pool, so embedders must shut down
// all consumers of a shared pool together (same contract as ThriftServer).
TEST(FastThriftServerSharedPoolTest, ServerReleasesPoolOnDestroy) {
  auto pool = makeFixedSizePool(1);
  ASSERT_EQ(pool.use_count(), 1);

  {
    auto handler = std::make_shared<TestHandler>();
    ftt::FastThriftServer server(makeLoopbackConfig());
    server.setIOThreadPool(pool);
    server.setInterface(handler);
    server.start();
    EXPECT_EQ(pool.use_count(), 2);
    server.stop();
  }

  EXPECT_EQ(pool.use_count(), 1);
}

TEST(FastThriftServerSharedPoolTest, NullPoolCrashes) {
  ftt::FastThriftServer server(makeLoopbackConfig());
  EXPECT_DEATH(
      server.setIOThreadPool(nullptr),
      "FastThriftServer::setIOThreadPool requires a non-null pool");
}

TEST(FastThriftServerSharedPoolTest, AfterStartCrashes) {
  // start() spawns IO worker threads. Default "fast" death-test style only
  // forks; any glog/folly logging mutex held by a worker at fork time stays
  // locked in the child and the CHECK message never reaches stderr, so the
  // regex match fails even though the child does abort. "threadsafe" style
  // forks + execs, which re-runs the binary clean.
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  auto handler = std::make_shared<TestHandler>();
  ftt::FastThriftServer server(makeLoopbackConfig());
  server.setInterface(handler);
  server.start();
  EXPECT_DEATH(
      server.setIOThreadPool(makeFixedSizePool(1)),
      "FastThriftServer::setIOThreadPool must be called before");
  server.stop();
}

// ---------------------------------------------------------------------------
// Hot-reload — reloadTLSConfig swaps the fizz context on a running server.
// ---------------------------------------------------------------------------

TEST_F(FastThriftServerTlsTest, HotReloadServesNewCert) {
  // Establish baseline: RPC works with the cert SetUp installed.
  auto* evb = clientThread_->getEventBase();
  {
    auto transport = connectFizz(evb, server_->getAddress());
    ASSERT_NE(transport, nullptr);
    std::unique_ptr<apache::thrift::Client<integration::FastThriftServer>>
        client;
    evb->runInEventBaseThreadAndWait([&] {
      auto channel =
          apache::thrift::RocketClientChannel::newChannel(std::move(transport));
      client = std::make_unique<
          apache::thrift::Client<integration::FastThriftServer>>(
          std::move(channel));
    });
    folly::Baton<> done;
    EchoResponse echoed;
    folly::exception_wrapper rpcErr;
    evb->runInEventBaseThread([&] {
      client->semifuture_echo("pre-reload")
          .via(evb)
          .thenValue([&](EchoResponse r) {
            echoed = std::move(r);
            done.post();
          })
          .thenError([&](const folly::exception_wrapper& ew) {
            rpcErr = ew;
            done.post();
          });
    });
    ASSERT_TRUE(done.try_wait_for(std::chrono::seconds{10}));
    EXPECT_FALSE(rpcErr) << rpcErr.what();
    EXPECT_EQ(*echoed.message(), "echoed:pre-reload");
    evb->runInEventBaseThreadAndWait([&] { client.reset(); });
  }

  // Hot-reload with a freshly-generated cert. The swap fans out across
  // every EVB; future accepts use the new params, in-flight ones (none
  // here) keep the old via captured shared_ptr.
  auto newCert = security::test::makeTestCert();
  security::FizzServerCertConfig newConfig;
  newConfig.certPem = newCert.certPem;
  newConfig.keyPem = newCert.keyPem;
  newConfig.clientAuth = fizz::server::ClientAuthMode::None;
  server_->reloadTLSConfig(std::move(newConfig));

  // New RPC must still succeed. The server is serving the new cert
  // (verifying that explicitly would require client-side peer cert
  // inspection; here we settle for "swap didn't break anything").
  {
    auto transport = connectFizz(evb, server_->getAddress());
    ASSERT_NE(transport, nullptr);
    std::unique_ptr<apache::thrift::Client<integration::FastThriftServer>>
        client;
    evb->runInEventBaseThreadAndWait([&] {
      auto channel =
          apache::thrift::RocketClientChannel::newChannel(std::move(transport));
      client = std::make_unique<
          apache::thrift::Client<integration::FastThriftServer>>(
          std::move(channel));
    });
    folly::Baton<> done;
    EchoResponse echoed;
    folly::exception_wrapper rpcErr;
    evb->runInEventBaseThread([&] {
      client->semifuture_echo("post-reload")
          .via(evb)
          .thenValue([&](EchoResponse r) {
            echoed = std::move(r);
            done.post();
          })
          .thenError([&](const folly::exception_wrapper& ew) {
            rpcErr = ew;
            done.post();
          });
    });
    ASSERT_TRUE(done.try_wait_for(std::chrono::seconds{10}));
    EXPECT_FALSE(rpcErr) << rpcErr.what();
    EXPECT_EQ(*echoed.message(), "echoed:post-reload");
    evb->runInEventBaseThreadAndWait([&] { client.reset(); });
  }
}

TEST(FastThriftServerHotReloadTest, RequiresRunningServer) {
  // reloadTLSConfig before start() must CHECK-fail.
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  auto handler = std::make_shared<TestHandler>();
  ftt::FastThriftServer server(makeLoopbackConfig());
  server.setInterface(handler);
  // No start() — server is in kNotStarted.

  auto cert = security::test::makeTestCert();
  security::FizzServerCertConfig cfg;
  cfg.certPem = cert.certPem;
  cfg.keyPem = cert.keyPem;
  cfg.clientAuth = fizz::server::ClientAuthMode::None;
  EXPECT_DEATH(
      server.reloadTLSConfig(std::move(cfg)),
      "FastThriftServer::reloadTLSConfig requires a running server");
}

} // namespace apache::thrift::fast_thrift::thrift::test::integration::test
