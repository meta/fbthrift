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

#include <gtest/gtest.h>

#include <sys/socket.h>
#include <array>
#include <atomic>
#include <exception>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <folly/ExceptionWrapper.h>
#include <folly/SocketAddress.h>
#include <folly/init/Init.h>
#include <folly/io/async/AsyncTransport.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/ScopedEventBaseThread.h>
#include <folly/net/NetworkSocket.h>
#include <folly/observer/SimpleObservable.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/StaticPipelineBuilder.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/test/MockAdapters.h>
#include <thrift/lib/cpp2/fast_thrift/connection/ConnectionHandler.h>

namespace apache::thrift::fast_thrift::connection {
namespace {

// Test connection — keeps the transport alive, counts close calls via a
// shared atomic so tests can verify teardown, and treats drain() as an
// immediate close (firing the close callback so the handler can erase
// the entry).
//
// `closeOnStart` exists to exercise the synchronous-close-during-start
// invariant: when set, start() immediately invokes close(), which fires
// the close callback under the same installer-lambda frame that just
// registered this connection.
struct TestConnection {
  folly::AsyncTransport::UniquePtr transport;
  std::shared_ptr<std::atomic<size_t>> closeCount;
  std::shared_ptr<std::atomic<size_t>> closeCallbackCount;
  std::function<void()> closeCb;
  bool closed{false};
  bool closeOnStart{false};

  void setCloseCallback(std::function<void()> cb) {
    if (!cb) {
      closeCb = {};
      return;
    }
    closeCb = [count = closeCallbackCount, callback = std::move(cb)]() mutable {
      count->fetch_add(1, std::memory_order_relaxed);
      callback();
    };
  }

  void start() noexcept {
    if (closeOnStart) {
      close();
    }
  }

  void drain() noexcept { close(); }

  void close() noexcept {
    if (closed) {
      return;
    }
    closed = true;
    if (transport) {
      transport->closeNow();
    }
    if (closeCount) {
      closeCount->fetch_add(1, std::memory_order_relaxed);
    }
    if (closeCb) {
      auto cb = std::move(closeCb);
      cb();
    }
  }
};

// Factory satisfying connection::ConnectionFactory.
class TestConnectionFactory {
 public:
  explicit TestConnectionFactory(
      std::shared_ptr<std::atomic<size_t>> closeCount = nullptr,
      std::shared_ptr<std::atomic<size_t>> closeCallbackCount = nullptr,
      bool closeOnStart = false) noexcept
      : closeCount_(std::move(closeCount)),
        closeCallbackCount_(std::move(closeCallbackCount)),
        closeOnStart_(closeOnStart) {}

  TestConnection getConnection(
      folly::AsyncTransport::UniquePtr socket,
      const folly::SocketAddress& clientAddr,
      std::unique_ptr<PeerSecurityInfo> peerSecurity) {
    lastClientAddr = clientAddr;
    lastPeerSecurity = std::move(peerSecurity);
    return TestConnection{
        .transport = std::move(socket),
        .closeCount = closeCount_,
        .closeCallbackCount = closeCallbackCount_,
        .closeCb = {},
        .closeOnStart = closeOnStart_};
  }

  // Written on the data EVB; read back through that EVB.
  folly::SocketAddress lastClientAddr;
  std::unique_ptr<PeerSecurityInfo> lastPeerSecurity;

 private:
  std::shared_ptr<std::atomic<size_t>> closeCount_;
  std::shared_ptr<std::atomic<size_t>> closeCallbackCount_;
  bool closeOnStart_;
};

struct CaptureFailingFactory {
  std::exception_ptr cause;

  [[noreturn]] TestConnection getConnection(
      folly::AsyncTransport::UniquePtr /*socket*/,
      const folly::SocketAddress& /*clientAddr*/,
      std::unique_ptr<PeerSecurityInfo> /*peerSecurity*/) {
    throw CachedEkmCaptureFailure{cause};
  }
};

class DropFirstConnectionFactory : public TestConnectionFactory {
 public:
  using TestConnectionFactory::TestConnectionFactory;

  std::optional<TestConnection> tryGetConnectionForAccept(
      folly::AsyncTransport::UniquePtr socket,
      const folly::SocketAddress& clientAddr,
      std::unique_ptr<PeerSecurityInfo> peerSecurity) {
    if (std::exchange(dropNext_, false)) {
      return std::nullopt;
    }
    return getConnection(
        std::move(socket), clientAddr, std::move(peerSecurity));
  }

 private:
  bool dropNext_{true};
};

} // namespace

class ConnectionHandlerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    evbThread_ = std::make_unique<folly::ScopedEventBaseThread>();
    evb_ = evbThread_->getEventBase();
  }

  void TearDown() override {
    for (auto fd : clientFds_) {
      folly::netops::close(fd);
    }
    evbThread_.reset();
  }

  std::unique_ptr<ConnectionHandler> createConnectionHandler() {
    return std::make_unique<ConnectionHandler>(
        *evb_,
        fast_security::SSLPolicy::DISABLED,
        folly::observer::SimpleObservable<
            std::shared_ptr<const fast_security::TLSParams>>(
            std::shared_ptr<const fast_security::TLSParams>{})
            .getObserver(),
        SocketOptions{},
        std::nullopt);
  }

  // Wires the factory + starts the installation pipeline. Runs on the
  // owning EVB because setConnectionFactory builds + activates the pipeline.
  // Factory is stored on the test fixture so it outlives the handler.
  void wireFactory(ConnectionHandler& h, bool closeOnStart = false) {
    factory_ = std::make_unique<TestConnectionFactory>(
        closeCount_, closeCallbackCount_, closeOnStart);
    evb_->runInEventBaseThreadAndWait(
        [&] { h.setConnectionFactory(*factory_); });
  }

  void install(ConnectionHandler& handler) {
    std::array<int, 2> fds{};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds.data()), 0);
    clientFds_.emplace_back(fds[0]);
    evb_->runInEventBaseThreadAndWait([&] {
      handler.listener().connectionAccepted(
          folly::NetworkSocket{fds[1]},
          folly::SocketAddress{"127.0.0.1", 5001},
          folly::AsyncServerSocket::AcceptCallback::AcceptInfo{});
    });
  }

  std::unique_ptr<folly::ScopedEventBaseThread> evbThread_;
  folly::EventBase* evb_{nullptr};
  std::shared_ptr<std::atomic<size_t>> closeCount_ =
      std::make_shared<std::atomic<size_t>>(0);
  std::shared_ptr<std::atomic<size_t>> closeCallbackCount_ =
      std::make_shared<std::atomic<size_t>>(0);
  std::unique_ptr<TestConnectionFactory> factory_;
  std::vector<folly::NetworkSocket> clientFds_;
};

TEST_F(ConnectionHandlerTest, ConstructDoesNotStart) {
  auto handler = createConnectionHandler();
  EXPECT_NE(handler, nullptr);
  EXPECT_EQ(handler->connectionCount(), 0);
}

TEST_F(
    ConnectionHandlerTest, CaptureFailureIsReportedWithoutAcceptingOrStarting) {
  CaptureFailingFactory factory{
      std::make_exception_ptr(std::runtime_error("exporter unavailable"))};
  channel_pipeline::test::StaticMockHeadHandler head;
  channel_pipeline::test::StaticMockTailHandler tail;
  channel_pipeline::SimpleBufferAllocator allocator;
  folly::exception_wrapper diagnostic;
  int accepts = 0;
  int starts = 0;
  tail.setOnExceptionCallback(
      [&](folly::exception_wrapper&& error) { diagnostic = std::move(error); });
  tail.setOnReadCallback([&](channel_pipeline::TypeErasedBox&& message) {
    ++starts;
    message.take<TestConnection>().start();
    return channel_pipeline::Result::Success;
  });

  const folly::SocketAddress clientAddr("127.0.0.1", 5001);
  auto result = channel_pipeline::Result::Success;
  evb_->runInEventBaseThreadAndWait([&] {
    auto pipeline =
        channel_pipeline::StaticPipelineBuilder<
            channel_pipeline::test::StaticMockHeadHandler,
            channel_pipeline::test::StaticMockTailHandler>()
            .setEventBase(evb_)
            .setHead(&head)
            .setTail(&tail)
            .setAllocator(&allocator)
            .addNextInbound<ConnectionBuilderHandler<CaptureFailingFactory>>(
                connection_builder_handler_tag, std::ref(factory))
            .addNextInbound<ConnectionAcceptCallbackHandler<TestConnection>>(
                connection_accept_callback_handler_tag,
                [&](TestConnection&) { ++accepts; })
            .build();
    pipeline->activate();
    result = pipeline->fireRead(
        channel_pipeline::erase_and_box(
            ConnectionMessage{
                .transport = nullptr,
                .clientAddr = clientAddr,
                .peerSecurity = nullptr}));
    pipeline->deactivate();
    pipeline.reset();
  });

  EXPECT_EQ(result, channel_pipeline::Result::Error);
  EXPECT_EQ(tail.exceptionCount(), 1);
  EXPECT_EQ(tail.readCount(), 0);
  EXPECT_EQ(accepts, 0);
  EXPECT_EQ(starts, 0);
  ASSERT_TRUE(diagnostic);
  const auto message = diagnostic.what().toStdString();
  EXPECT_NE(message.find("EKM capture failed"), std::string::npos);
  EXPECT_NE(message.find("exporter unavailable"), std::string::npos);
  EXPECT_NE(message.find(clientAddr.describe()), std::string::npos);
}

TEST_F(ConnectionHandlerTest, AcceptsSingleConnection) {
  auto handler = createConnectionHandler();
  wireFactory(*handler);

  install(*handler);

  EXPECT_EQ(handler->connectionCount(), 1);
}

TEST_F(ConnectionHandlerTest, FactoryDropClosesSocketWithoutAccepting) {
  DropFirstConnectionFactory factory(closeCount_, closeCallbackCount_);
  int accepts = 0;
  auto handler = createConnectionHandler();
  evb_->runInEventBaseThreadAndWait([&] {
    handler->setConnectionFactory(factory, [&](TestConnection&) { ++accepts; });
  });

  install(*handler);
  EXPECT_EQ(handler->connectionCount(), 0);
  EXPECT_EQ(accepts, 0);
  ASSERT_EQ(clientFds_.size(), 1);
  char byte;
  EXPECT_EQ(::recv(clientFds_.front().toFd(), &byte, 1, MSG_DONTWAIT), 0);

  install(*handler);
  EXPECT_EQ(handler->connectionCount(), 1);
  EXPECT_EQ(accepts, 1);
}

TEST_F(ConnectionHandlerTest, AcceptsMultipleConnections) {
  auto handler = createConnectionHandler();
  wireFactory(*handler);

  for (int i = 0; i < 3; ++i) {
    install(*handler);
  }

  EXPECT_EQ(handler->connectionCount(), 3);
}

TEST_F(ConnectionHandlerTest, StopDrainsAllConnections) {
  auto handler = createConnectionHandler();
  wireFactory(*handler);

  for (int i = 0; i < 3; ++i) {
    install(*handler);
  }
  ASSERT_EQ(handler->connectionCount(), 3);

  // stop() is off-EVB — bounces internally for the synchronous phases.
  handler->stop();

  EXPECT_EQ(handler->connectionCount(), 0);
  EXPECT_EQ(closeCount_->load(), 3);
}

TEST_F(ConnectionHandlerTest, StopWithNoConnections) {
  auto handler = createConnectionHandler();
  wireFactory(*handler);
  ASSERT_EQ(handler->connectionCount(), 0);

  handler->stop();

  EXPECT_EQ(handler->connectionCount(), 0);
}

TEST_F(ConnectionHandlerTest, DestroyWithActiveConnections) {
  auto handler = createConnectionHandler();
  wireFactory(*handler);
  install(*handler);
  ASSERT_EQ(handler->connectionCount(), 1);

  // Destructor drives stop() — should release live connections cleanly.
  handler.reset();
  EXPECT_EQ(closeCount_->load(), 1);
}

TEST_F(ConnectionHandlerTest, DestroyOnEventBaseDetachesCloseCallback) {
  auto handler = createConnectionHandler();
  wireFactory(*handler);
  install(*handler);
  ASSERT_EQ(handler->connectionCount(), 1);

  evb_->runInEventBaseThreadAndWait([&] { handler.reset(); });

  EXPECT_EQ(closeCount_->load(), 1);
  EXPECT_EQ(closeCallbackCount_->load(), 0);
}

// Regression: the installer lambda must register the connection in
// connections_ BEFORE calling start(). A factory-built connection whose
// start() synchronously closes (modelling a post-StopTLS handoff that
// dispatches and tears down inline) would otherwise fire its close callback
// against a not-yet-registered entry, missing the erase + count decrement.
TEST_F(ConnectionHandlerTest, SynchronousCloseDuringStartIsSafe) {
  auto handler = createConnectionHandler();
  wireFactory(*handler, /*closeOnStart=*/true);
  install(*handler);
  EXPECT_EQ(handler->connectionCount(), 0)
      << "synchronous close during start() must remove the registered entry";
  EXPECT_EQ(closeCount_->load(), 1);
}

// The factory is handed the peer address observed at accept, so it never has
// to re-derive one from a transport whose peer may already be gone.
TEST_F(ConnectionHandlerTest, FactoryReceivesPeerAddress) {
  auto handler = createConnectionHandler();
  wireFactory(*handler);

  install(*handler);

  folly::SocketAddress seen;
  evb_->runInEventBaseThreadAndWait([&] { seen = factory_->lastClientAddr; });

  EXPECT_EQ(seen, folly::SocketAddress("127.0.0.1", 5001));
}

} // namespace apache::thrift::fast_thrift::connection

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv);
  return RUN_ALL_TESTS();
}
