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

#include <thrift/lib/cpp2/fast_thrift/thrift/server/ThriftServerConnectionFactory.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
#include <folly/io/async/EventBase.h>
#include <folly/logging/LogCategory.h>
#include <folly/logging/LogMessage.h>
#include <folly/logging/LoggerDB.h>
#include <folly/logging/test/TestLogHandler.h>
#include <folly/portability/GMock.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/HandlerNode.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/common/RocketTransportLifecycle.h>
#include <thrift/lib/cpp2/fast_thrift/transport/test/TestAsyncTransport.h>

namespace apache::thrift::fast_thrift::thrift::server::test {
namespace {

using namespace testing;

class TestAppAdapterFactory final : public ThriftServerAppAdapterFactory {
 public:
  ThriftServerAppAdapter::Ptr getAppAdapter(
      std::shared_ptr<ThriftServerAppAdapterFactory>) override {
    return ThriftServerAppAdapter::Ptr{new ThriftServerAppAdapter()};
  }

  void populateMethodMetadata(
      ThriftServerMethodMetadataRegistry&) const override {}
};

struct LifecycleCounts {
  int builderCalls{0};
  int handlerAdded{0};
  int handlerRemoved{0};
  int handlerDestroyed{0};
  int transportStarted{0};
  int transportDisconnected{0};
  int transportDestroyed{0};
};

class TestTransportOwner {
 public:
  TestTransportOwner(
      folly::AsyncTransport::UniquePtr socket, LifecycleCounts& counts)
      : socket_(std::move(socket)), counts_(counts) {}

  ~TestTransportOwner() { ++counts_.transportDestroyed; }

  void start() noexcept { ++counts_.transportStarted; }

  void disconnect(folly::exception_wrapper&&) noexcept {
    ++counts_.transportDisconnected;
    socket_->close();
  }

 private:
  folly::AsyncTransport::UniquePtr socket_;
  LifecycleCounts& counts_;
};

class TestNegotiationHandler {
 public:
  explicit TestNegotiationHandler(LifecycleCounts& counts) : counts_(counts) {}
  ~TestNegotiationHandler() { ++counts_.handlerDestroyed; }

  void handlerAdded(channel_pipeline::detail::ContextImpl&) noexcept {
    ++counts_.handlerAdded;
  }

  void handlerRemoved(channel_pipeline::detail::ContextImpl&) noexcept {
    ++counts_.handlerRemoved;
  }

  void onReadReady(channel_pipeline::detail::ContextImpl&) noexcept {}

 private:
  LifecycleCounts& counts_;
};

folly::AsyncTransport::UniquePtr makeTransport(folly::EventBase& eventBase) {
  return folly::AsyncTransport::UniquePtr(
      new transport::test::TestAsyncTransport(&eventBase));
}

ThriftServerConnectionFactoryConfig makeConfig() {
  ThriftServerConnectionFactoryConfig config;
  config.handler = std::make_shared<TestAppAdapterFactory>();
  return config;
}

TEST(ThriftServerConnectionFactoryTest, DefaultBuildsNativeRocketTransport) {
  folly::EventBase eventBase;
  ThriftServerConnectionFactory factory(makeConfig());

  auto connection = factory.getConnection(
      makeTransport(eventBase),
      folly::SocketAddress("127.0.0.1", 1234),
      nullptr);

  const auto& rocketConnection =
      connection.thriftTransportAdapter->rocketConnection();
  EXPECT_NE(rocketConnection.transportHandler, nullptr);
  EXPECT_TRUE(rocketConnection.pipeline);
  EXPECT_FALSE(rocketConnection.transportLifecycle);
}

TEST(ThriftServerConnectionFactoryTest, ZeroCopyWarningUsesFactoryLogCategory) {
  const auto logs = std::make_shared<folly::TestLogHandler>();
  folly::LoggerDB::get().getCategory("")->addHandler(logs);
  folly::EventBase eventBase;
  ThriftServerConnectionFactoryConfig config = makeConfig();
  config.zeroCopyThreshold = 1;
  ThriftServerConnectionFactory factory(std::move(config));

  // TestAsyncTransport refuses MSG_ZEROCOPY.
  const ThriftServerConnection connection = factory.getConnection(
      makeTransport(eventBase),
      folly::SocketAddress("127.0.0.1", 1234),
      nullptr);

  std::vector<std::string> warningCategories;
  for (const auto& [message, handlerCategory] : logs->getMessages()) {
    if (message.getMessage() == "MSG_ZEROCOPY not supported on this socket") {
      warningCategories.push_back(message.getCategory()->getName());
    }
  }
  // XLOG names the category after the source path, which starts with `xplat/`
  // in the xplat build.
  EXPECT_THAT(
      warningCategories,
      ElementsAre(
          EndsWith(".thrift.server.ThriftServerConnectionFactory.cpp")));
}

TEST(
    ThriftServerConnectionFactoryTest,
    CustomBuilderConnectionAndHandlerUseExistingOwnership) {
  folly::EventBase eventBase;
  LifecycleCounts counts;
  const folly::SocketAddress expectedClient("127.0.0.1", 4321);

  auto config = makeConfig();
  config.rocketConnectionBuilder = [&](folly::AsyncTransport::UniquePtr socket,
                                       const folly::SocketAddress& clientAddr,
                                       ServerStatsShard* statsShard) {
    ++counts.builderCalls;
    EXPECT_EQ(clientAddr, expectedClient);
    EXPECT_EQ(socket->getEventBase(), &eventBase);
    EXPECT_EQ(statsShard, nullptr);

    auto connection =
        std::make_unique<rocket::server::RocketServerConnection>();
    connection->transportLifecycle = rocket::RocketTransportLifecycle::own(
        std::make_unique<TestTransportOwner>(std::move(socket), counts));
    return RocketServerConnectionBuildResult{
        .connection = std::move(connection),
        .transportNegotiationHandler =
            channel_pipeline::detail::makeHandlerNode<TestNegotiationHandler>(
                channel_pipeline::fnv1a_hash(
                    "test_transport_negotiation_handler"),
                std::make_unique<TestNegotiationHandler>(counts)),
    };
  };

  {
    DynamicThriftServerConnectionFactory factory(std::move(config));
    auto connection = factory.getConnection(
        makeTransport(eventBase), expectedClient, nullptr);

    EXPECT_EQ(counts.builderCalls, 1);
    EXPECT_EQ(counts.handlerAdded, 1);
    EXPECT_EQ(counts.transportStarted, 0);

    connection.start();
    EXPECT_EQ(counts.transportStarted, 1);
  }

  EXPECT_EQ(counts.handlerRemoved, 1);
  EXPECT_EQ(counts.handlerDestroyed, 1);
  EXPECT_EQ(counts.transportDisconnected, 1);
  EXPECT_EQ(counts.transportDestroyed, 1);
}

} // namespace
} // namespace apache::thrift::fast_thrift::thrift::server::test
