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

#include <folly/Portability.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/test/SocketPair.h>
#include <folly/portability/GTest.h>
#include <thrift/lib/cpp2/transport/rocket/framing/test/Util.h>
#include <thrift/lib/cpp2/transport/rocket/server/RefactoredRocketServerConnection.h>

namespace apache::thrift::rocket {
namespace {

struct Calls {
  size_t setup{0};
  size_t closing{0};
  size_t requests{0};
};

class TestServerHandler : public RocketServerHandler {
 public:
  explicit TestServerHandler(Calls& calls) : calls_(calls) {}

  void handleSetupFrame(SetupFrame&&, IRocketServerConnection&) override {
    ++calls_.setup;
  }
  void handleRequestResponseFrame(
      RequestResponseFrame&&, RocketServerFrameContext&&) override {
    ++calls_.requests;
  }
  void handleRequestFnfFrame(
      RequestFnfFrame&&, RocketServerFrameContext&&) override {
    ++calls_.requests;
  }
  void handleRequestStreamFrame(
      RequestStreamFrame&&,
      RocketServerFrameContext&&,
      RocketStreamClientCallback*) override {
    ++calls_.requests;
  }
  void handleRequestChannelFrame(
      RequestChannelFrame&&,
      RocketServerFrameContext&&,
      ChannelRequestCallbackFactory) override {
    ++calls_.requests;
  }
  void connectionClosing() override { ++calls_.closing; }

 private:
  Calls& calls_;
};

std::unique_ptr<folly::IOBuf> header(FrameType type, uint16_t flags = 0) {
  std::string bytes(6, '\0');
  bytes[3] = type == FrameType::SETUP || type == FrameType::KEEPALIVE ? 0 : 1;
  const uint16_t raw = (static_cast<uint16_t>(type) << Flags::kBits) | flags;
  bytes[4] = static_cast<char>(raw >> 8);
  bytes[5] = static_cast<char>(raw);
  return folly::IOBuf::copyBuffer(bytes);
}

template <typename Buffer>
class IncomingFrameHandlerTest : public ::testing::Test {
 protected:
  void SetUp() override { resetConnection(); }
  void TearDown() override { destroyConnection(); }

  void destroyConnection() {
    if (connection_) {
      sockets_->closeFD1();
      evb_.loop();
      connection_.reset();
      guard_.reset();
    }
  }

  void resetConnection() {
    destroyConnection();
    calls_ = {};
    sockets_ = std::make_unique<folly::SocketPair>();
    IRocketServerConnection::Config cfg;
    cfg.parserStrategy = "strategy";
    connection_.reset(new RefactoredRocketServerConnection(
        folly::AsyncSocket::newSocket(&evb_, sockets_->extractNetworkSocket0()),
        std::make_unique<TestServerHandler>(calls_),
        ingress_,
        egress_,
        cfg));
    guard_.emplace(connection_.get());
  }

  void receive(std::unique_ptr<folly::IOBuf> bytes, size_t chunkSize = 1) {
    if constexpr (std::is_same_v<Buffer, std::unique_ptr<folly::IOBuf>>) {
      if (bytes->empty()) {
        connection_->handleFrame(std::move(bytes));
        return;
      }
    }
    connection_->handleFrame(
        test::makeFrameBuffer<Buffer>(std::move(bytes), chunkSize));
  }

  void setup() {
    auto bytes =
        SetupFrame(
            Payload::makeFromData(folly::IOBuf::copyBuffer("setup")), true)
            .serialize();
    bytes->trimStart(Serializer::kBytesForFrameOrMetadataLength);
    receive(std::move(bytes));
    ASSERT_EQ(calls_.setup, 1);
    ASSERT_EQ(calls_.closing, 0);
  }

  folly::EventBase evb_;
  MemoryTracker ingress_;
  MemoryTracker egress_;
  Calls calls_;
  std::unique_ptr<folly::SocketPair> sockets_;
  RefactoredRocketServerConnection::UniquePtr connection_;
  std::optional<RefactoredRocketServerConnection::DestructorGuard> guard_;
};

using FrameBuffers =
    ::testing::Types<std::unique_ptr<folly::IOBuf>, IOBufChain>;
TYPED_TEST_SUITE(IncomingFrameHandlerTest, FrameBuffers);

TYPED_TEST(IncomingFrameHandlerTest, EmptyBufferClosesConnection) {
  this->receive(folly::IOBuf::create(0));
  EXPECT_EQ(this->calls_.closing, 1);
  EXPECT_EQ(this->calls_.setup, 0);
}

TYPED_TEST(IncomingFrameHandlerTest, ShortHeadersCloseConnection) {
  for (size_t size = 0; size < 6; ++size) {
    SCOPED_TRACE(size);
    this->resetConnection();
    this->receive(folly::IOBuf::copyBuffer(std::string(size, '\0')));
    EXPECT_EQ(this->calls_.closing, 1);
    EXPECT_EQ(this->calls_.setup, 0);
  }
}

TYPED_TEST(IncomingFrameHandlerTest, TruncatedSetupClosesConnection) {
  this->receive(header(FrameType::SETUP));
  EXPECT_EQ(this->calls_.closing, 1);
  EXPECT_EQ(this->calls_.setup, 0);
}

TYPED_TEST(IncomingFrameHandlerTest, TruncatedBodiesCloseConnection) {
  for (auto type :
       {FrameType::REQUEST_RESPONSE,
        FrameType::REQUEST_FNF,
        FrameType::REQUEST_STREAM,
        FrameType::REQUEST_CHANNEL,
        FrameType::REQUEST_N,
        FrameType::PAYLOAD,
        FrameType::KEEPALIVE,
        FrameType::EXT}) {
    SCOPED_TRACE(static_cast<uint16_t>(type));
    this->resetConnection();
    this->setup();
    if constexpr (folly::kIsDebug) {
      if (type == FrameType::KEEPALIVE) {
        EXPECT_DEATH(this->receive(header(type)), "frameHeaderSize");
        continue;
      }
    }
    const bool hasPayload = type == FrameType::REQUEST_RESPONSE ||
        type == FrameType::REQUEST_FNF || type == FrameType::REQUEST_STREAM ||
        type == FrameType::REQUEST_CHANNEL || type == FrameType::PAYLOAD;
    this->receive(
        header(type, static_cast<uint16_t>(Flags().metadata(hasPayload))));
    EXPECT_EQ(this->calls_.closing, 1);
    EXPECT_EQ(this->calls_.requests, 0);
  }
}

TYPED_TEST(IncomingFrameHandlerTest, InvalidFlagsCloseConnection) {
  this->setup();
  this->receive(header(FrameType::REQUEST_RESPONSE, 1));
  EXPECT_EQ(this->calls_.closing, 1);
  EXPECT_EQ(this->calls_.requests, 0);
}

TYPED_TEST(IncomingFrameHandlerTest, ValidKeepAliveRemainsOpen) {
  this->setup();
  auto bytes = KeepAliveFrame(Flags(), folly::IOBuf::create(0)).serialize();
  bytes->trimStart(Serializer::kBytesForFrameOrMetadataLength);
  this->receive(std::move(bytes));
  EXPECT_EQ(this->calls_.closing, 0);
}

} // namespace
} // namespace apache::thrift::rocket
