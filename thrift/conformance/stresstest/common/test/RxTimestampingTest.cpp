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

#include <thrift/conformance/stresstest/common/RxTimestamping.h>

#include <linux/net_tstamp.h>

#include <array>
#include <chrono>
#include <ctime>
#include <memory>

#include <gtest/gtest.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/EventBase.h>
#include <folly/net/NetOps.h>

namespace apache::thrift::stress {
namespace {

class TimestampMessage {
 public:
  TimestampMessage() {
    msg_.msg_control = control_.data();
    msg_.msg_controllen = control_.size();
    cmsg_ = CMSG_FIRSTHDR(&msg_);
    cmsg_->cmsg_level = SOL_SOCKET;
    cmsg_->cmsg_type = SO_TIMESTAMPING;
    cmsg_->cmsg_len = CMSG_LEN(sizeof(struct timespec) * 3);
  }

  struct msghdr& msg() { return msg_; }
  struct cmsghdr& cmsg() { return *cmsg_; }

  struct timespec* timestamps() {
    return reinterpret_cast<struct timespec*>(CMSG_DATA(cmsg_));
  }

 private:
  alignas(cmsghdr)
      std::array<uint8_t, CMSG_SPACE(sizeof(struct timespec) * 3)> control_{};
  struct msghdr msg_{};
  struct cmsghdr* cmsg_{nullptr};
};

class TruncatingAncillaryDataCallback final
    : public folly::AsyncSocket::ReadAncillaryDataCallback {
 public:
  explicit TruncatingAncillaryDataCallback(ReadAncillaryDataCallback* callback)
      : callback_(callback) {}

  folly::Expected<folly::Unit, folly::AsyncSocketException> ancillaryData(
      struct msghdr& msg) noexcept override {
    sawTruncatedControl = (msg.msg_flags & MSG_CTRUNC) != 0;
    if (callback_ != nullptr) {
      return callback_->ancillaryData(msg);
    }
    return folly::unit;
  }

  folly::MutableByteRange getAncillaryDataCtrlBuffer() override { return {}; }

  bool sawTruncatedControl{false};

 private:
  ReadAncillaryDataCallback* callback_;
};

TEST(RxTimestampingTest, ParsesSoftwareRxTimestamp) {
  TimestampMessage message;
  message.timestamps()[0] = {.tv_sec = 9, .tv_nsec = 500'000'000};

  EXPECT_EQ(
      detail::getRxTimestampCollect(message.msg(), std::chrono::seconds{10}),
      std::chrono::microseconds{500'000});
}

TEST(RxTimestampingTest, IgnoresUnrelatedControlMessage) {
  TimestampMessage message;
  message.cmsg().cmsg_type = SCM_RIGHTS;

  EXPECT_FALSE(
      detail::getRxTimestampCollect(message.msg(), std::chrono::seconds{10})
          .has_value());
}

TEST(RxTimestampingTest, IgnoresTruncatedTimestamp) {
  TimestampMessage message;
  message.cmsg().cmsg_len = CMSG_LEN(sizeof(struct timespec) * 2);

  EXPECT_FALSE(
      detail::getRxTimestampCollect(message.msg(), std::chrono::seconds{10})
          .has_value());
}

TEST(RxTimestampingTest, IgnoresTruncatedControlBuffer) {
  TimestampMessage message;
  message.msg().msg_flags = MSG_CTRUNC;
  message.timestamps()[0] = {.tv_sec = 9, .tv_nsec = 500'000'000};

  EXPECT_FALSE(
      detail::getRxTimestampCollect(message.msg(), std::chrono::seconds{10})
          .has_value());
}

TEST(RxTimestampingTest, IgnoresZeroTimestamp) {
  TimestampMessage message;

  EXPECT_FALSE(
      detail::getRxTimestampCollect(message.msg(), std::chrono::seconds{10})
          .has_value());
}

TEST(RxTimestampingTest, IgnoresFutureTimestamp) {
  TimestampMessage message;
  message.timestamps()[0] = {.tv_sec = 11, .tv_nsec = 0};

  EXPECT_FALSE(
      detail::getRxTimestampCollect(message.msg(), std::chrono::seconds{10})
          .has_value());
}

TEST(RxTimestampingTest, IgnoresImplausiblyOldTimestamp) {
  TimestampMessage message;
  message.timestamps()[0] = {.tv_sec = 1, .tv_nsec = 0};

  EXPECT_FALSE(
      detail::getRxTimestampCollect(message.msg(), std::chrono::seconds{12})
          .has_value());
}

class RxTimestampingInstallerTest : public testing::Test {
 protected:
  void SetUp() override {
    folly::NetworkSocket fds[2];
    ASSERT_EQ(
        folly::netops::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, fds),
        0);
    socket_ = folly::AsyncSocket::newSocket(&eventBase_, fds[0]);
    peer_ = folly::AsyncSocket::newSocket(&eventBase_, fds[1]);
  }

  bool installTimestamping(RxTimestampSink sink) {
    registration_ = installRxTimestamping(*socket_, std::move(sink));
    return registration_ != nullptr;
  }

  RxTimestampSink makeSink() {
    return [this, lifetime = std::make_unique<SinkLifetime>(sinkDestructions_)](
               std::chrono::microseconds) { sinkCalled_ = true; };
  }

  void expectCollectDelivered() {
    auto* callback = socket_->getReadAncillaryDataCallback();
    ASSERT_NE(callback, nullptr);

    TimestampMessage message;
    ASSERT_EQ(::clock_gettime(CLOCK_REALTIME, &message.timestamps()[0]), 0);
    EXPECT_TRUE(callback->ancillaryData(message.msg()).hasValue());
    EXPECT_TRUE(sinkCalled_);
  }

  void expectTruncatedControlHandled(RxTimestampSink sink) {
    ASSERT_TRUE(installTimestamping(std::move(sink)));
    auto* callback = socket_->getReadAncillaryDataCallback();
    ASSERT_NE(callback, nullptr);

    TimestampMessage message;
    ASSERT_EQ(::clock_gettime(CLOCK_REALTIME, &message.timestamps()[0]), 0);
    message.msg().msg_flags = MSG_CTRUNC | MSG_EOR;
    EXPECT_TRUE(callback->ancillaryData(message.msg()).hasValue());
    EXPECT_EQ(message.msg().msg_flags, MSG_EOR);
    EXPECT_FALSE(sinkCalled_);
  }

  void expectReplacementPreservedOnReset(
      folly::AsyncSocket::ReadAncillaryDataCallback* replacement) {
    ASSERT_TRUE(installTimestamping(makeSink()));
    socket_->setReadAncillaryDataCB(replacement);

    registration_.reset();
    EXPECT_EQ(socket_->getReadAncillaryDataCallback(), replacement);
    EXPECT_EQ(sinkDestructions_, 1);
    socket_->setReadAncillaryDataCB(nullptr);
  }

  void trackSocketDestruction() {
    class TrackedSocket final : public folly::AsyncSocket {
     public:
      TrackedSocket(folly::AsyncSocket* socket, int& destructions)
          : folly::AsyncSocket(socket), destructions_(destructions) {}

     protected:
      ~TrackedSocket() override { ++destructions_; }

     private:
      int& destructions_;
    };

    auto oldSocket = std::move(socket_);
    socket_.reset(new TrackedSocket(oldSocket.get(), socketDestructions_));
  }

  void expectTimestampingFlagsPreserved(int flags) {
    ASSERT_EQ(socket_->setSockOpt(SOL_SOCKET, SO_TIMESTAMPING, &flags), 0);
    ASSERT_TRUE(installTimestamping(makeSink()));

    int timestampingFlags = 0;
    socklen_t timestampingFlagsSize = sizeof(timestampingFlags);
    ASSERT_EQ(
        socket_->getSockOpt(
            SOL_SOCKET,
            SO_TIMESTAMPING,
            &timestampingFlags,
            &timestampingFlagsSize),
        0);
    EXPECT_EQ(
        timestampingFlags,
        flags | SOF_TIMESTAMPING_SOFTWARE | SOF_TIMESTAMPING_RX_SOFTWARE);
    expectCollectDelivered();
  }

  bool sinkCalled_{false};
  int sinkDestructions_{0};
  int socketDestructions_{0};
  folly::EventBase eventBase_;
  folly::AsyncSocket::UniquePtr socket_;
  folly::AsyncSocket::UniquePtr peer_;
  std::unique_ptr<RxTimestampingRegistration> registration_;

 private:
  struct SinkLifetime {
    explicit SinkLifetime(int& destructionCount)
        : destructionCount_(destructionCount) {}
    ~SinkLifetime() { ++destructionCount_; }

    int& destructionCount_;
  };
};

TEST_F(RxTimestampingInstallerTest, InstallerDeliversCollectToSink) {
  ASSERT_TRUE(installTimestamping(makeSink()));
  expectCollectDelivered();
}

TEST_F(RxTimestampingInstallerTest, ClearsTruncationWithoutCollecting) {
  expectTruncatedControlHandled(makeSink());
}

TEST_F(RxTimestampingInstallerTest, ClearsTruncationWithoutSink) {
  expectTruncatedControlHandled({});
}

TEST_F(RxTimestampingInstallerTest, DeliversPayloadWithTruncatedControl) {
  ASSERT_TRUE(installTimestamping(makeSink()));
  auto* timestampCallback = socket_->getReadAncillaryDataCallback();
  ASSERT_NE(timestampCallback, nullptr);

  int passCredentials = 1;
  ASSERT_EQ(socket_->setSockOpt(SOL_SOCKET, SO_PASSCRED, &passCredentials), 0);
  TruncatingAncillaryDataCallback ancillaryCallback{timestampCallback};
  socket_->setReadAncillaryDataCB(&ancillaryCallback);

  class ReadCallback final : public folly::AsyncTransport::ReadCallback {
   public:
    void getReadBuffer(void** buffer, size_t* length) override {
      *buffer = &payload;
      *length = sizeof(payload);
    }
    void readDataAvailable(size_t length) noexcept override {
      bytesRead += length;
    }
    void readEOF() noexcept override { ADD_FAILURE() << "Unexpected EOF"; }
    void readErr(const folly::AsyncSocketException& error) noexcept override {
      ADD_FAILURE() << error.what();
    }

    char payload{};
    size_t bytesRead{0};
  } readCallback;

  const char payload = 'x';
  ASSERT_EQ(
      folly::netops::send(
          peer_->getNetworkSocket(), &payload, sizeof(payload), MSG_DONTWAIT),
      sizeof(payload));
  socket_->setReadCB(&readCallback);
  eventBase_.loopOnce();
  socket_->setReadCB(nullptr);
  socket_->setReadAncillaryDataCB(timestampCallback);

  EXPECT_TRUE(ancillaryCallback.sawTruncatedControl);
  EXPECT_FALSE(sinkCalled_);
  EXPECT_EQ(readCallback.bytesRead, sizeof(payload));
  EXPECT_EQ(readCallback.payload, payload);
  EXPECT_FALSE(socket_->error());
}

TEST_F(RxTimestampingInstallerTest, MovingHandleKeepsCallbackInstalled) {
  ASSERT_TRUE(installTimestamping(makeSink()));
  auto* callback = socket_->getReadAncillaryDataCallback();
  auto registration = std::move(registration_);

  EXPECT_EQ(registration_, nullptr);
  EXPECT_EQ(socket_->getReadAncillaryDataCallback(), callback);
  expectCollectDelivered();

  registration.reset();
  EXPECT_EQ(socket_->getReadAncillaryDataCallback(), nullptr);
  EXPECT_EQ(sinkDestructions_, 1);
}

TEST_F(RxTimestampingInstallerTest, ReinstallsAfterSocketReplacement) {
  ASSERT_TRUE(installTimestamping(makeSink()));
  for (int replacementCount = 0; replacementCount < 2; ++replacementCount) {
    registration_.reset();
    EXPECT_EQ(socket_->getReadAncillaryDataCallback(), nullptr);
    EXPECT_EQ(sinkDestructions_, replacementCount + 1);

    auto oldSocket = std::move(socket_);
    socket_ =
        folly::AsyncSocket::UniquePtr{new folly::AsyncSocket(oldSocket.get())};
    EXPECT_EQ(socket_->getReadAncillaryDataCallback(), nullptr);
    oldSocket.reset();

    ASSERT_TRUE(installTimestamping(makeSink()));
    sinkCalled_ = false;
    expectCollectDelivered();
  }
  registration_.reset();
  EXPECT_EQ(sinkDestructions_, 3);
}

TEST_F(RxTimestampingInstallerTest, ResetDoesNotRestoreClearedCallback) {
  expectReplacementPreservedOnReset(nullptr);
}

TEST_F(RxTimestampingInstallerTest, ResetDoesNotOverwriteReplacement) {
  TruncatingAncillaryDataCallback replacement{nullptr};
  expectReplacementPreservedOnReset(&replacement);
}

TEST_F(RxTimestampingInstallerTest, ResetReleasesSinkExactlyOnce) {
  ASSERT_TRUE(installTimestamping(makeSink()));
  registration_.reset();
  EXPECT_EQ(socket_->getReadAncillaryDataCallback(), nullptr);
  EXPECT_EQ(sinkDestructions_, 1);
  registration_.reset();
  socket_.reset();
  EXPECT_EQ(sinkDestructions_, 1);
}

TEST_F(RxTimestampingInstallerTest, ScopeExitUnregistersCallback) {
  {
    auto registration = installRxTimestamping(*socket_, makeSink());
    ASSERT_NE(registration, nullptr);
    expectCollectDelivered();
  }
  EXPECT_EQ(socket_->getReadAncillaryDataCallback(), nullptr);
  EXPECT_EQ(sinkDestructions_, 1);
}

TEST_F(RxTimestampingInstallerTest, RejectsExistingCallback) {
  TruncatingAncillaryDataCallback replacement{nullptr};
  socket_->setReadAncillaryDataCB(&replacement);
  auto registration = installRxTimestamping(*socket_, makeSink());
  EXPECT_EQ(registration, nullptr);
  EXPECT_EQ(socket_->getReadAncillaryDataCallback(), &replacement);
  EXPECT_EQ(sinkDestructions_, 1);
  socket_->setReadAncillaryDataCB(nullptr);
}

TEST_F(RxTimestampingInstallerTest, FailedInstallReleasesSink) {
  socket_->closeNow();
  auto registration = installRxTimestamping(*socket_, makeSink());
  EXPECT_EQ(registration, nullptr);
  EXPECT_EQ(socket_->getReadAncillaryDataCallback(), nullptr);
  EXPECT_EQ(sinkDestructions_, 1);
}

TEST_F(RxTimestampingInstallerTest, EmptySinkRegistrationCleansUp) {
  ASSERT_TRUE(installTimestamping({}));
  registration_.reset();
  EXPECT_EQ(socket_->getReadAncillaryDataCallback(), nullptr);
}

TEST_F(RxTimestampingInstallerTest, CleanupAfterLogicalSocketDestruction) {
  trackSocketDestruction();
  ASSERT_TRUE(installTimestamping(makeSink()));
  auto* socket = socket_.get();

  socket_.reset();
  EXPECT_EQ(socketDestructions_, 0);
  EXPECT_TRUE(socket->getDestroyPending());
  EXPECT_EQ(sinkDestructions_, 0);

  registration_.reset();
  EXPECT_EQ(socketDestructions_, 1);
  EXPECT_EQ(sinkDestructions_, 1);
}

TEST_F(RxTimestampingInstallerTest, ResetLeavesTimestampingEnabled) {
  ASSERT_TRUE(installTimestamping(makeSink()));
  registration_.reset();

  int timestampingFlags = 0;
  socklen_t timestampingFlagsSize = sizeof(timestampingFlags);
  ASSERT_EQ(
      socket_->getSockOpt(
          SOL_SOCKET,
          SO_TIMESTAMPING,
          &timestampingFlags,
          &timestampingFlagsSize),
      0);
  EXPECT_EQ(
      timestampingFlags,
      SOF_TIMESTAMPING_SOFTWARE | SOF_TIMESTAMPING_RX_SOFTWARE);
}

TEST_F(RxTimestampingInstallerTest, PreservesExistingTxTimestampingFlags) {
  expectTimestampingFlagsPreserved(
      SOF_TIMESTAMPING_SOFTWARE | SOF_TIMESTAMPING_TX_SOFTWARE |
      SOF_TIMESTAMPING_TX_ACK | SOF_TIMESTAMPING_OPT_ID |
      SOF_TIMESTAMPING_OPT_TSONLY);
}

TEST_F(RxTimestampingInstallerTest, PreservesAlreadyEnabledRxTimestamping) {
  expectTimestampingFlagsPreserved(
      SOF_TIMESTAMPING_SOFTWARE | SOF_TIMESTAMPING_RX_SOFTWARE |
      SOF_TIMESTAMPING_TX_SOFTWARE | SOF_TIMESTAMPING_OPT_ID);
}

} // namespace
} // namespace apache::thrift::stress
