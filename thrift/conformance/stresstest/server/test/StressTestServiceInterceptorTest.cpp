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

#include <thrift/conformance/stresstest/server/StressTestServiceInterceptor.h>

#include <memory>

#include <gtest/gtest.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/EventBase.h>
#include <folly/net/NetOps.h>
#include <thrift/conformance/stresstest/server/StressTestServerStats.h>
#include <thrift/lib/cpp2/server/Cpp2ConnContext.h>
#include <thrift/lib/cpp2/test/MockCpp2ConnContext.h>

namespace apache::thrift::stress {
namespace {

class EmptyAncillaryCallback final
    : public folly::AsyncSocket::ReadAncillaryDataCallback {
 public:
  folly::Expected<folly::Unit, folly::AsyncSocketException> ancillaryData(
      struct msghdr&) noexcept override {
    return folly::unit;
  }
  folly::MutableByteRange getAncillaryDataCtrlBuffer() override { return {}; }
};

class StressTestServiceInterceptorTest : public testing::Test {
 protected:
  void SetUp() override {
    folly::NetworkSocket fds[2];
    ASSERT_EQ(
        folly::netops::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, fds),
        0);
    socket_ = folly::AsyncSocket::newSocket(&eventBase_, fds[0]);
    peer_ = folly::AsyncSocket::newSocket(&eventBase_, fds[1]);
  }

  auto makeContext() {
    auto context =
        std::make_unique<apache::thrift::test::MockCpp2ConnContext>();
    EXPECT_CALL(*context, getTransport())
        .WillRepeatedly(testing::Return(socket_.get()));
    return context;
  }

  folly::EventBase eventBase_;
  EmptyAncillaryCallback replacement_;
  folly::AsyncSocket::UniquePtr socket_;
  folly::AsyncSocket::UniquePtr peer_;
  std::shared_ptr<StressTestServerStats> stats_ =
      std::make_shared<StressTestServerStats>();
  StressTestServiceInterceptor interceptor_{stats_, true};
};

TEST_F(StressTestServiceInterceptorTest, CloseUnregistersAndReleasesStatsSink) {
  auto context = makeContext();
  auto state = interceptor_.onConnectionEstablished({context.get()});
  ASSERT_TRUE(state.has_value());
  ASSERT_NE(*state, nullptr);
  ASSERT_NE(socket_->getReadAncillaryDataCallback(), nullptr);
  EXPECT_EQ(stats_.use_count(), 3);

  interceptor_.onConnectionClosed(&*state, {context.get()});
  EXPECT_EQ(*state, nullptr);
  EXPECT_EQ(socket_->getReadAncillaryDataCallback(), nullptr);
  EXPECT_EQ(stats_.use_count(), 2);

  interceptor_.onConnectionClosed(&*state, {context.get()});
  interceptor_.onConnectionClosed(nullptr, {});
}

TEST_F(
    StressTestServiceInterceptorTest,
    StateDestructionWithoutCloseHookCleansUp) {
  auto context = makeContext();
  auto state = interceptor_.onConnectionEstablished({context.get()});
  ASSERT_TRUE(state.has_value());
  ASSERT_NE(*state, nullptr);

  state.reset();
  EXPECT_EQ(socket_->getReadAncillaryDataCallback(), nullptr);
  EXPECT_EQ(stats_.use_count(), 2);
}

TEST_F(
    StressTestServiceInterceptorTest,
    StateCleanupAfterLogicalSocketDestruction) {
  auto context = makeContext();
  auto state = interceptor_.onConnectionEstablished({context.get()});
  ASSERT_TRUE(state.has_value());
  ASSERT_NE(*state, nullptr);

  socket_.reset();
  EXPECT_EQ(stats_.use_count(), 3);
  state.reset();
  EXPECT_EQ(stats_.use_count(), 2);
}

TEST_F(StressTestServiceInterceptorTest, DisabledTimestampingDoesNotRegister) {
  StressTestServiceInterceptor interceptor{stats_, false};
  auto context = makeContext();
  EXPECT_FALSE(
      interceptor.onConnectionEstablished({context.get()}).has_value());
  EXPECT_EQ(socket_->getReadAncillaryDataCallback(), nullptr);
  const auto result = stats_->publish({});
  EXPECT_EQ(result.connections()->at(ConnectionMode::Plaintext), 1);
}

TEST_F(StressTestServiceInterceptorTest, MissingContextDoesNotRegister) {
  EXPECT_FALSE(interceptor_.onConnectionEstablished({}).has_value());
  EXPECT_EQ(socket_->getReadAncillaryDataCallback(), nullptr);
}

TEST_F(StressTestServiceInterceptorTest, MissingTransportDoesNotRegister) {
  Cpp2ConnContext context;
  EXPECT_FALSE(interceptor_.onConnectionEstablished({&context}).has_value());
  EXPECT_EQ(socket_->getReadAncillaryDataCallback(), nullptr);
}

TEST_F(StressTestServiceInterceptorTest, ExistingCallbackIsPreserved) {
  socket_->setReadAncillaryDataCB(&replacement_);
  auto context = makeContext();
  EXPECT_FALSE(
      interceptor_.onConnectionEstablished({context.get()}).has_value());
  EXPECT_EQ(socket_->getReadAncillaryDataCallback(), &replacement_);
  socket_->setReadAncillaryDataCB(nullptr);
}

} // namespace
} // namespace apache::thrift::stress
