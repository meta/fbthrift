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

#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <thrift/lib/cpp2/transport/rocket/framing/test/Util.h>
#include <thrift/lib/cpp2/transport/rocket/server/RocketStreamClientCallback.h>
#include <thrift/lib/cpp2/transport/rocket/server/StreamFirstResponseLoggingCallback.h>
#include <thrift/lib/cpp2/transport/rocket/server/test/MockIRocketServerConnection.h>
#include <thrift/lib/cpp2/transport/rocket/server/test/ThrowingPayloadSerializer.h>
#include <thrift/lib/thrift/gen-cpp2/RpcMetadata_types.h>

THRIFT_FLAG_DEFINE_bool(enable_rocket_connection_observers, false);

using namespace ::testing;
using namespace apache::thrift;
using namespace apache::thrift::rocket;
using namespace apache::thrift::rocket::test;

class MockStreamServerCallback : public StreamServerCallback {
 public:
  MOCK_METHOD(bool, onStreamRequestN, (int32_t), (override));
  MOCK_METHOD(void, onStreamCancel, (), (override));
  MOCK_METHOD(void, resetClientCallback, (StreamClientCallback&), (override));
  MOCK_METHOD(void, pauseStream, (), (override));
  MOCK_METHOD(void, resumeStream, (), (override));
  MOCK_METHOD(bool, onSinkHeaders, (HeadersPayload&&), (override));
};

/**
 * Records which SendCallback method ran, mirroring LogRequestSampleCallback's
 * `delete this` semantics (which is what emits the request event sample).
 */
class FakeSendCallback : public MessageChannel::SendCallback {
 public:
  struct Record {
    bool queued{false};
    bool sent{false};
    bool sendError{false};
    bool destroyed{false};
  };

  explicit FakeSendCallback(Record& record) : record_(record) {}
  ~FakeSendCallback() override { record_.destroyed = true; }

  void sendQueued() override { record_.queued = true; }
  void messageSent() override {
    record_.sent = true;
    delete this;
  }
  void messageSendError(folly::exception_wrapper&&) override {
    record_.sendError = true;
    delete this;
  }

 private:
  Record& record_;
};

class RocketStreamClientCallbackTest : public ::testing::Test {
 protected:
  static constexpr StreamId kStreamId{1};
  static constexpr uint32_t kInitialRequestN{10};

  void SetUp() override {
    callback_ = std::make_unique<RocketStreamClientCallback>(
        kStreamId, connection_, kInitialRequestN);
  }

  static FirstResponsePayload makeFirstResponsePayload() {
    return FirstResponsePayload(
        folly::IOBuf::copyBuffer(""), ResponseRpcMetadata());
  }

  MessageChannel::SendCallbackPtr makeFakeSendCallback(
      FakeSendCallback::Record& record) {
    return MessageChannel::SendCallbackPtr(new FakeSendCallback(record));
  }

  /**
   * Captures the send callback that the initial-response write carries, so the
   * test can settle it explicitly instead of having gmock drop it.
   */
  void captureFirstWriteSendCallback(MessageChannel::SendCallbackPtr& into) {
    EXPECT_CALL(connection_, sendPayload(kStreamId, _, _, _))
        .WillOnce(
            [&](StreamId,
                Payload&&,
                Flags,
                MessageChannel::SendCallbackPtr cb) { into = std::move(cb); });
  }

  void captureFirstErrorSendCallback(MessageChannel::SendCallbackPtr& into) {
    EXPECT_CALL(connection_, sendError(kStreamId, _, _))
        .WillOnce(
            [&](StreamId,
                RocketException&&,
                MessageChannel::SendCallbackPtr cb) { into = std::move(cb); });
  }

  /** Cancels the stream before the first response has been delivered. */
  void cancelBeforeFirstResponse() {
    callback_->handleFrame(CancelFrame(kStreamId));
  }

  /**
   * Simulates the first response to transition the callback to "ready" state
   * with a server callback attached.
   */
  void makeReady() {
    EXPECT_CALL(serverCallback_, onStreamRequestN(_))
        .WillRepeatedly(Return(true));
    FirstResponsePayload firstResponse(
        folly::IOBuf::copyBuffer(""), ResponseRpcMetadata());
    callback_->onFirstResponse(
        std::move(firstResponse),
        &connection_.getEventBase(),
        &serverCallback_);
  }

  NiceMock<MockIRocketServerConnection> connection_;
  NiceMock<MockStreamServerCallback> serverCallback_;
  std::unique_ptr<RocketStreamClientCallback> callback_;
};

template <typename Buffer>
class RocketStreamClientCallbackFrameTest
    : public RocketStreamClientCallbackTest {};
using StreamFrameBuffers =
    ::testing::Types<std::unique_ptr<folly::IOBuf>, IOBufChain>;
TYPED_TEST_SUITE(RocketStreamClientCallbackFrameTest, StreamFrameBuffers);

TEST_F(RocketStreamClientCallbackTest, HandleRequestNPostReady) {
  makeReady();

  EXPECT_CALL(serverCallback_, onStreamRequestN(5)).WillOnce(Return(true));

  RequestNFrame frame(kStreamId, 5);
  callback_->handleFrame(std::move(frame));
}

TEST_F(RocketStreamClientCallbackTest, HandleRequestNPreReadyClosesConnection) {
  EXPECT_CALL(connection_, close(_)).Times(1);

  RequestNFrame frame(kStreamId, 5);
  callback_->handleFrame(std::move(frame));
}

TEST_F(RocketStreamClientCallbackTest, HandleCancelPreReady) {
  // Cancel before first response should mark as cancelled, not crash
  EXPECT_CALL(connection_, close(_)).Times(0);

  CancelFrame frame(kStreamId);
  callback_->handleFrame(std::move(frame));
}

TEST_F(RocketStreamClientCallbackTest, HandleCancelPostReady) {
  makeReady();

  EXPECT_CALL(serverCallback_, onStreamCancel()).Times(1);
  EXPECT_CALL(connection_, freeStream(kStreamId, true)).Times(1);

  CancelFrame frame(kStreamId);
  callback_->handleFrame(std::move(frame));
}

TEST_F(RocketStreamClientCallbackTest, HandleCancelCompletesInline) {
  makeReady();

  EXPECT_CALL(connection_, sendPayload(kStreamId, _, _, _)).Times(1);
  EXPECT_CALL(connection_, freeStream(kStreamId, true))
      .WillOnce([&](StreamId, bool) { callback_.reset(); })
      .WillOnce([](StreamId, bool) {});
  EXPECT_CALL(serverCallback_, onStreamCancel()).WillOnce([&] {
    callback_->onStreamComplete();
  });

  CancelFrame frame(kStreamId);
  callback_->handleFrame(std::move(frame));
}

TEST_F(RocketStreamClientCallbackTest, HandlePayloadClosesConnection) {
  makeReady();

  EXPECT_CALL(connection_, close(_)).Times(1);

  PayloadFrame frame(kStreamId, Payload{}, Flags());
  callback_->handleFrame(std::move(frame));
}

TEST_F(RocketStreamClientCallbackTest, HandleErrorClosesConnection) {
  makeReady();

  EXPECT_CALL(connection_, close(_)).Times(1);

  ErrorFrame frame(kStreamId, ErrorCode::APPLICATION_ERROR, Payload{});
  callback_->handleFrame(std::move(frame));
}

TYPED_TEST(
    RocketStreamClientCallbackFrameTest, HandleExtPreReadyClosesConnection) {
  EXPECT_CALL(this->connection_, close(_)).Times(1);

  ExtFrame frame(
      this->kStreamId,
      Payload::makeFromData(folly::IOBuf::create(0)),
      Flags(),
      ExtFrameType::UNKNOWN);
  auto decoded = roundTripFrame<TypeParam>(std::move(frame));
  EXPECT_EQ(
      (std::is_same_v<TypeParam, IOBufChain>),
      decoded.payload().usesIOBufChain());
  this->callback_->handleFrame(std::move(decoded));
}

TYPED_TEST(
    RocketStreamClientCallbackFrameTest,
    HandleExtPostReadyNoIgnoreClosesConnection) {
  this->makeReady();

  EXPECT_CALL(this->connection_, close(_)).Times(1);

  ExtFrame frame(
      this->kStreamId,
      Payload::makeFromData(folly::IOBuf::create(0)),
      Flags(),
      ExtFrameType::UNKNOWN);
  auto decoded = roundTripFrame<TypeParam>(std::move(frame));
  EXPECT_EQ(
      (std::is_same_v<TypeParam, IOBufChain>),
      decoded.payload().usesIOBufChain());
  this->callback_->handleFrame(std::move(decoded));
}

TYPED_TEST(RocketStreamClientCallbackFrameTest, HandleExtPostReadyIgnoreFlag) {
  this->makeReady();

  // ExtFrame with ignore flag set should be silently dropped
  EXPECT_CALL(this->connection_, close(_)).Times(0);

  Flags flags;
  flags.ignore(true);
  ExtFrame frame(
      this->kStreamId,
      Payload::makeFromData(folly::IOBuf::create(0)),
      flags,
      ExtFrameType::UNKNOWN);
  auto decoded = roundTripFrame<TypeParam>(std::move(frame));
  EXPECT_EQ(
      (std::is_same_v<TypeParam, IOBufChain>),
      decoded.payload().usesIOBufChain());
  this->callback_->handleFrame(std::move(decoded));
}

TEST_F(RocketStreamClientCallbackTest, HandleConnectionClose) {
  makeReady();

  EXPECT_CALL(connection_, sendErrorAfterDrain(kStreamId, _)).Times(1);
  EXPECT_CALL(serverCallback_, onStreamCancel()).Times(1);

  callback_->handleConnectionClose();
}

TEST_F(RocketStreamClientCallbackTest, HandleConnectionClosePreReady) {
  // Before first response, sendErrorAfterDrain should still be called
  // but server callback should not be notified
  EXPECT_CALL(connection_, sendErrorAfterDrain(kStreamId, _)).Times(1);
  EXPECT_CALL(serverCallback_, onStreamCancel()).Times(0);

  callback_->handleConnectionClose();
}

TEST_F(RocketStreamClientCallbackTest, HandleStreamHeadersPush) {
  makeReady();

  EXPECT_CALL(serverCallback_, onSinkHeaders(_)).WillOnce(Return(true));

  callback_->handleStreamHeadersPush(HeadersPayload(HeadersPayloadContent{}));
}

TEST_F(RocketStreamClientCallbackTest, HandleStreamHeadersPushPreReady) {
  // Before first response, headers push should be a no-op
  EXPECT_CALL(serverCallback_, onSinkHeaders(_)).Times(0);

  callback_->handleStreamHeadersPush(HeadersPayload(HeadersPayloadContent{}));
}

TEST_F(RocketStreamClientCallbackTest, HandlePausedByConnection) {
  makeReady();

  EXPECT_CALL(connection_, areStreamsPaused()).WillRepeatedly(Return(true));
  EXPECT_CALL(serverCallback_, pauseStream()).Times(1);

  callback_->handlePausedByConnection();
}

TEST_F(RocketStreamClientCallbackTest, HandleResumedByConnection) {
  makeReady();

  EXPECT_CALL(connection_, areStreamsPaused()).WillRepeatedly(Return(false));
  EXPECT_CALL(serverCallback_, resumeStream()).Times(1);

  callback_->handleResumedByConnection();
}

TEST_F(RocketStreamClientCallbackTest, HandlePausedByConnectionPreReady) {
  // Before first response, pause should be a no-op
  EXPECT_CALL(connection_, areStreamsPaused()).WillRepeatedly(Return(true));
  EXPECT_CALL(serverCallback_, pauseStream()).Times(0);

  callback_->handlePausedByConnection();
}

TEST_F(RocketStreamClientCallbackTest, HandleResumedByConnectionPreReady) {
  // Before first response, resume should be a no-op
  EXPECT_CALL(connection_, areStreamsPaused()).WillRepeatedly(Return(false));
  EXPECT_CALL(serverCallback_, resumeStream()).Times(0);

  callback_->handleResumedByConnection();
}

TEST_F(
    RocketStreamClientCallbackTest, FirstResponseSendCallbackCarriedByWrite) {
  FakeSendCallback::Record record;
  MessageChannel::SendCallbackPtr carried;
  captureFirstWriteSendCallback(carried);
  EXPECT_CALL(serverCallback_, onStreamRequestN(_))
      .WillRepeatedly(Return(true));

  callback_->setFirstResponseSendCallback(makeFakeSendCallback(record));
  EXPECT_TRUE(callback_->onFirstResponse(
      makeFirstResponsePayload(),
      &connection_.getEventBase(),
      &serverCallback_));

  ASSERT_NE(carried, nullptr);
  carried.release()->messageSent();
  EXPECT_TRUE(record.sent);
}

TEST_F(
    RocketStreamClientCallbackTest, FirstResponseSendCallbackNotReusedByItems) {
  FakeSendCallback::Record record;
  MessageChannel::SendCallbackPtr carried;
  captureFirstWriteSendCallback(carried);
  EXPECT_CALL(serverCallback_, onStreamRequestN(_))
      .WillRepeatedly(Return(true));

  callback_->setFirstResponseSendCallback(makeFakeSendCallback(record));
  EXPECT_TRUE(callback_->onFirstResponse(
      makeFirstResponsePayload(),
      &connection_.getEventBase(),
      &serverCallback_));

  // The next item gets its own send callback; gmock drops it, and dropping it
  // must not settle the initial response's callback a second time.
  EXPECT_CALL(connection_, sendPayload(kStreamId, _, _, _)).Times(1);
  EXPECT_TRUE(
      callback_->onStreamNext(StreamPayload(folly::IOBuf::copyBuffer(""), {})));

  EXPECT_FALSE(record.destroyed);
  carried.release()->messageSent();
  EXPECT_TRUE(record.sent);
}

TEST_F(
    RocketStreamClientCallbackTest,
    FirstResponseSendCallbackDroppedWhenCancelledBeforeResponse) {
  FakeSendCallback::Record record;
  callback_->setFirstResponseSendCallback(makeFakeSendCallback(record));
  cancelBeforeFirstResponse();

  EXPECT_CALL(serverCallback_, onStreamCancel()).Times(1);
  EXPECT_CALL(connection_, freeStream(kStreamId, true)).Times(1);
  EXPECT_CALL(connection_, sendPayload(kStreamId, _, _, _)).Times(0);

  EXPECT_FALSE(callback_->onFirstResponse(
      makeFirstResponsePayload(),
      &connection_.getEventBase(),
      &serverCallback_));

  EXPECT_TRUE(record.destroyed);
  EXPECT_FALSE(record.sendError);
}

TEST_F(RocketStreamClientCallbackTest, WrapperTimesFirstResponseThenRebinds) {
  FakeSendCallback::Record record;
  auto* wrapper = new StreamFirstResponseLoggingCallback(
      *callback_, makeFakeSendCallback(record));

  MessageChannel::SendCallbackPtr carried;
  captureFirstWriteSendCallback(carried);
  EXPECT_CALL(serverCallback_, onStreamRequestN(_))
      .WillRepeatedly(Return(true));
  // The wrapper deletes itself once the first response is forwarded, so it must
  // hand the server callback back to the wrapped callback on the way out.
  EXPECT_CALL(serverCallback_, resetClientCallback(_))
      .WillOnce([&](StreamClientCallback& clientCallback) {
        EXPECT_EQ(&clientCallback, callback_.get());
      });

  EXPECT_TRUE(wrapper->onFirstResponse(
      makeFirstResponsePayload(),
      &connection_.getEventBase(),
      &serverCallback_));

  ASSERT_NE(carried, nullptr);
  carried.release()->messageSent();
  EXPECT_TRUE(record.sent);
}

TEST_F(
    RocketStreamClientCallbackTest, WrapperPropagatesTerminatedFirstResponse) {
  FakeSendCallback::Record record;
  auto* wrapper = new StreamFirstResponseLoggingCallback(
      *callback_, makeFakeSendCallback(record));
  cancelBeforeFirstResponse();

  EXPECT_CALL(serverCallback_, resetClientCallback(_)).Times(1);
  EXPECT_CALL(serverCallback_, onStreamCancel()).Times(1);
  EXPECT_CALL(connection_, freeStream(kStreamId, true)).Times(1);

  // Reporting `true` here would let the caller keep driving a stream that the
  // wrapped callback already tore down.
  EXPECT_FALSE(wrapper->onFirstResponse(
      makeFirstResponsePayload(),
      &connection_.getEventBase(),
      &serverCallback_));

  EXPECT_TRUE(record.destroyed);
  EXPECT_FALSE(record.sendError);
}

TEST_F(RocketStreamClientCallbackTest, WrapperTimesFirstResponseError) {
  FakeSendCallback::Record record;
  auto* wrapper = new StreamFirstResponseLoggingCallback(
      *callback_, makeFakeSendCallback(record));

  MessageChannel::SendCallbackPtr carried;
  captureFirstErrorSendCallback(carried);
  EXPECT_CALL(connection_, freeStream(kStreamId, false)).Times(1);

  wrapper->onFirstResponseError(
      folly::make_exception_wrapper<RocketException>(
          ErrorCode::CANCELED, "cancelled"));

  ASSERT_NE(carried, nullptr);
  carried.release()->messageSent();
  EXPECT_TRUE(record.sent);
}

class RocketStreamClientCallbackPackFailureTest
    : public RocketStreamClientCallbackTest {
 protected:
  void SetUp() override {
    RocketStreamClientCallbackTest::SetUp();
    ON_CALL(connection_, getPayloadSerializer()).WillByDefault([this] {
      return serializer_.getNonOwningPtr();
    });
  }

  static FirstResponsePayload makeUncompressableFirstResponse() {
    ResponseRpcMetadata metadata;
    metadata.compression() = CompressionAlgorithm::CUSTOM;
    return FirstResponsePayload(
        folly::IOBuf::copyBuffer("data"), std::move(metadata));
  }

  ThrowingPayloadSerializer serializer_;
};

TEST_F(
    RocketStreamClientCallbackPackFailureTest,
    FirstResponseSendCallbackCarriedByErrorWrite) {
  FakeSendCallback::Record record;
  MessageChannel::SendCallbackPtr carried;
  captureFirstErrorSendCallback(carried);
  EXPECT_CALL(connection_, sendPayload(kStreamId, _, _, _)).Times(0);
  EXPECT_CALL(serverCallback_, onStreamCancel()).Times(1);
  EXPECT_CALL(connection_, freeStream(kStreamId, true)).Times(1);

  callback_->setFirstResponseSendCallback(makeFakeSendCallback(record));
  EXPECT_FALSE(callback_->onFirstResponse(
      makeUncompressableFirstResponse(),
      &connection_.getEventBase(),
      &serverCallback_));

  ASSERT_NE(carried, nullptr);
  carried.release()->messageSent();
  EXPECT_TRUE(record.sent);
}

TEST_F(
    RocketStreamClientCallbackPackFailureTest,
    EncodedFirstResponseErrorPackFailureCarriesSendCallback) {
  FakeSendCallback::Record record;
  MessageChannel::SendCallbackPtr carried;
  captureFirstErrorSendCallback(carried);
  EXPECT_CALL(connection_, sendPayload(kStreamId, _, _, _)).Times(0);
  EXPECT_CALL(connection_, freeStream(kStreamId, false)).Times(1);

  callback_->setFirstResponseSendCallback(makeFakeSendCallback(record));
  callback_->onFirstResponseError(
      folly::make_exception_wrapper<
          apache::thrift::detail::EncodedFirstResponseError>(
          makeUncompressableFirstResponse()));

  ASSERT_NE(carried, nullptr);
  carried.release()->messageSent();
  EXPECT_TRUE(record.sent);
}
