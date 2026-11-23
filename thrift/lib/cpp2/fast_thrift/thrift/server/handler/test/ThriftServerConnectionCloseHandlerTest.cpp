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

#include <thrift/lib/cpp2/fast_thrift/thrift/server/handler/ThriftServerConnectionCloseHandler.h>

#include <chrono>
#include <functional>
#include <vector>

#include <gtest/gtest.h>

#include <folly/ExceptionWrapper.h>
#include <folly/io/async/EventBase.h>

#include <thrift/lib/cpp/transport/TTransportException.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/frame/ErrorCode.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/Event.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/Messages.h>

namespace apache::thrift::fast_thrift::thrift {

namespace {

using channel_pipeline::erase_and_box;
using channel_pipeline::Result;
using channel_pipeline::TypeErasedBox;

// Tests can use onDeactivate to deliver onPipelineInactive from inside
// deactivate(), as the static pipeline does.
struct FakePipeline {
  int deactivateCount{0};
  std::function<void()> onDeactivate;
  void deactivate() noexcept {
    ++deactivateCount;
    if (onDeactivate) {
      onDeactivate();
    }
  }
};

class FakeContext {
 public:
  Result fireRead(TypeErasedBox&& msg) noexcept {
    reads.push_back(std::move(msg));
    return Result::Success;
  }

  Result fireWrite(TypeErasedBox&& msg) noexcept {
    writes.push_back(std::move(msg));
    return writeResult;
  }

  void fireException(folly::exception_wrapper&& e) noexcept {
    exceptions.push_back(std::move(e));
  }

  template <channel_pipeline::PipelineEvent E>
  void fireEvent() noexcept {
    static_assert(std::same_as<E, ThriftServerConnectionClosedEvent>);
    events.emplace_back();
  }

  folly::EventBase* eventBase() noexcept { return &evb_; }
  void deactivate() noexcept { pipeline_.deactivate(); }

  Result writeResult{Result::Success};
  FakePipeline pipeline_;
  folly::EventBase evb_;
  std::vector<TypeErasedBox> reads;
  std::vector<TypeErasedBox> writes;
  std::vector<folly::exception_wrapper> exceptions;
  std::vector<ThriftServerConnectionClosedEvent> events;
};

// Test fixture: ctx is declared first so it outlives handler (the
// handler's timers hold raw pointers to ctx.evb_). handlerAdded is
// invoked so the timers are wired before any test action.
struct Fixture {
  FakeContext ctx;
  ThriftServerConnectionCloseHandler<FakeContext> handler;

  Fixture() { handler.handlerAdded(ctx); }

  Fixture(
      std::chrono::milliseconds drainTimeout,
      std::chrono::milliseconds reapTimeout)
      : handler(drainTimeout, reapTimeout) {
    handler.handlerAdded(ctx);
  }
};

// A unary REQUEST_RESPONSE — an exchange opener, so it counts as request work.
ThriftServerRequestMessage makeRequest(uint32_t streamId = 1) {
  ThriftServerRequestMessage req;
  req.streamId = streamId;
  req.payload = ThriftServerRequestResponsePayload{};
  return req;
}

ThriftServerResponseMessage makeResponse(uint32_t streamId) {
  return ThriftServerResponseMessage{
      .payload = ThriftInitialResponsePayload{
          .data = nullptr,
          .metadata = nullptr,
          .streamId = streamId,
      }};
}

// The inbound stream-open request (REQUEST_STREAM) — an exchange opener, so it
// counts as request work.
ThriftServerRequestMessage makeStreamRequest(uint32_t streamId = 1) {
  ThriftServerRequestMessage req;
  req.streamId = streamId;
  req.payload = ThriftRequestStreamPayload{.initialRequestN = 1};
  return req;
}

// Established-stream flow-control frames — not request work.
ThriftServerRequestMessage makeRequestN(uint32_t streamId, uint32_t n = 1) {
  ThriftServerRequestMessage req;
  req.streamId = streamId;
  req.payload = ThriftRequestNPayload{.streamId = streamId, .requestN = n};
  return req;
}

// A stream's initial response — the message that retires the request.
ThriftServerResponseMessage makeStreamInitialResponse(uint32_t streamId) {
  return ThriftServerResponseMessage{
      .payload = ThriftStreamInitialResponsePayload{
          .streamId = streamId, .complete = false, .next = true}};
}

// A stream continuation chunk (data when !complete, terminal when complete) —
// not request work.
ThriftServerResponseMessage makeStreamChunk(uint32_t streamId, bool complete) {
  return ThriftServerResponseMessage{
      .payload = ThriftStreamPayload{
          .streamId = streamId, .complete = complete, .next = !complete}};
}

// Verifies a captured outbound box holds the stream-0 CONNECTION_CLOSE
// rocket frame the handler emits in response to a CloseConnection event.
void expectConnectionCloseFrame(const TypeErasedBox& box) {
  const auto& resp = box.get<ThriftServerResponseMessage>();
  ASSERT_TRUE(resp.payload.is<ThriftErrorPayload>());
  const auto& err = resp.payload.get<ThriftErrorPayload>();
  EXPECT_EQ(err.streamId, 0u);
  EXPECT_EQ(
      err.errorCode,
      static_cast<uint32_t>(
          apache::thrift::fast_thrift::frame::ErrorCode::CONNECTION_CLOSE));
}

} // namespace

// =============================================================================
// Inbound path
// =============================================================================

TEST(
    ThriftServerConnectionCloseHandlerTest,
    OnReadForwardsAndIncrementsInFlight) {
  Fixture f;

  EXPECT_EQ(
      f.handler.onRead(f.ctx, erase_and_box(makeRequest(1))), Result::Success);
  EXPECT_EQ(f.handler.inFlight(), 1u);
  EXPECT_EQ(f.ctx.reads.size(), 1u);
}

TEST(
    ThriftServerConnectionCloseHandlerTest,
    OnReadRejectsAfterCloseConnectionEvent) {
  Fixture f;

  ASSERT_EQ(
      f.handler.onRead(f.ctx, erase_and_box(makeRequest(1))), Result::Success);
  f.handler.on<ThriftServerCloseConnectionEvent>(
      f.ctx, ThriftServerCloseConnectionEvent{});
  ASSERT_TRUE(f.handler.isDraining());

  EXPECT_EQ(
      f.handler.onRead(f.ctx, erase_and_box(makeRequest(2))), Result::Error);
  EXPECT_EQ(f.handler.inFlight(), 1u);
}

// =============================================================================
// Stream in-flight accounting — a stream is one in-flight REQUEST, retired at
// its initial response (matching regular Thrift); its continuation is not
// request work.
// =============================================================================

TEST(
    ThriftServerConnectionCloseHandlerTest,
    StreamRequestRetiresAtInitialResponseThenContinuationIsUncounted) {
  Fixture f;

  // REQUEST_STREAM opens one in-flight request.
  ASSERT_EQ(
      f.handler.onRead(f.ctx, erase_and_box(makeStreamRequest(1))),
      Result::Success);
  EXPECT_EQ(f.handler.inFlight(), 1u);

  // The initial response retires it — exactly as a unary response would.
  ASSERT_EQ(
      f.handler.onWrite(f.ctx, erase_and_box(makeStreamInitialResponse(1))),
      Result::Success);
  EXPECT_EQ(f.handler.inFlight(), 0u);

  // The stream then continues: inbound demand and outbound chunks/terminal are
  // not request work, so the count stays at zero (no double-count, no
  // underflow/DCHECK).
  ASSERT_EQ(
      f.handler.onRead(f.ctx, erase_and_box(makeRequestN(1, 5))),
      Result::Success);
  EXPECT_EQ(f.handler.inFlight(), 0u);
  ASSERT_EQ(
      f.handler.onWrite(f.ctx, erase_and_box(makeStreamChunk(1, false))),
      Result::Success);
  ASSERT_EQ(
      f.handler.onWrite(f.ctx, erase_and_box(makeStreamChunk(1, true))),
      Result::Success);
  EXPECT_EQ(f.handler.inFlight(), 0u);
}

TEST(
    ThriftServerConnectionCloseHandlerTest,
    StreamFlowControlFramesAreNotRequestWork) {
  Fixture f;

  // REQUEST_N forwards through but never counts as request work. (CANCEL is no
  // longer an inbound message — it arrives as a
  // ThriftServerRequestCancellationEvent.)
  ASSERT_EQ(
      f.handler.onRead(f.ctx, erase_and_box(makeRequestN(3, 10))),
      Result::Success);
  EXPECT_EQ(f.handler.inFlight(), 0u);
  EXPECT_EQ(f.ctx.reads.size(), 1u);
}

TEST(
    ThriftServerConnectionCloseHandlerTest,
    DrainWaitsForStreamInitialResponseNotStreamCompletion) {
  Fixture f;

  ASSERT_EQ(
      f.handler.onRead(f.ctx, erase_and_box(makeStreamRequest(1))),
      Result::Success);
  f.handler.on<ThriftServerCloseConnectionEvent>(
      f.ctx, ThriftServerCloseConnectionEvent{});
  EXPECT_TRUE(f.handler.isDraining());
  EXPECT_FALSE(f.handler.isClosed());

  // The initial response alone settles the drain — the connection does not wait
  // for the (still-open) stream to finish producing.
  ASSERT_EQ(
      f.handler.onWrite(f.ctx, erase_and_box(makeStreamInitialResponse(1))),
      Result::Success);
  EXPECT_EQ(f.handler.inFlight(), 0u);
  EXPECT_TRUE(f.handler.isClosed());
  EXPECT_EQ(f.ctx.pipeline_.deactivateCount, 1);
  ASSERT_EQ(f.ctx.events.size(), 1u);
}

// =============================================================================
// Outbound path
// =============================================================================

TEST(
    ThriftServerConnectionCloseHandlerTest,
    OnWriteForwardsAndDecrementsInFlight) {
  Fixture f;

  ASSERT_EQ(
      f.handler.onRead(f.ctx, erase_and_box(makeRequest(7))), Result::Success);
  ASSERT_EQ(f.handler.inFlight(), 1u);

  EXPECT_EQ(
      f.handler.onWrite(f.ctx, erase_and_box(makeResponse(7))),
      Result::Success);
  EXPECT_EQ(f.handler.inFlight(), 0u);
  EXPECT_EQ(f.ctx.writes.size(), 1u);
}

TEST(
    ThriftServerConnectionCloseHandlerTest,
    OnWriteSurfacesPipelineWriteResult) {
  Fixture f;
  f.ctx.writeResult = Result::Error;

  ASSERT_EQ(
      f.handler.onRead(f.ctx, erase_and_box(makeRequest(1))), Result::Success);
  // Downstream error still decrements — the response was attempted.
  EXPECT_EQ(
      f.handler.onWrite(f.ctx, erase_and_box(makeResponse(1))), Result::Error);
  EXPECT_EQ(f.handler.inFlight(), 0u);
}

TEST(
    ThriftServerConnectionCloseHandlerTest,
    CancellationCompletionDecrementsWithoutResponse) {
  Fixture f;

  ASSERT_EQ(
      f.handler.onRead(f.ctx, erase_and_box(makeRequest(13))), Result::Success);
  ASSERT_EQ(f.handler.inFlight(), 1u);

  f.handler.on<ThriftServerRequestCompletedEvent>(
      f.ctx, ThriftServerRequestCompletedEvent{.streamId = 13});

  EXPECT_EQ(f.handler.inFlight(), 0u);
  EXPECT_TRUE(f.ctx.writes.empty());
}

// =============================================================================
// CloseConnection event — graceful path
// =============================================================================

TEST(
    ThriftServerConnectionCloseHandlerTest,
    CloseConnectionEventFiresConnectionCloseAndDeactivatesIfIdle) {
  Fixture f;

  f.handler.on<ThriftServerCloseConnectionEvent>(
      f.ctx, ThriftServerCloseConnectionEvent{});

  ASSERT_EQ(f.ctx.writes.size(), 1u);
  expectConnectionCloseFrame(f.ctx.writes.front());
  EXPECT_TRUE(f.handler.isClosed());
  EXPECT_EQ(f.ctx.pipeline_.deactivateCount, 1);
  ASSERT_EQ(f.ctx.events.size(), 1u);
}

// A refusal takes the same terminal path, answered with its own error code and
// reason instead of CONNECTION_CLOSE.
TEST(ThriftServerConnectionCloseHandlerTest, RejectionIsAnsweredWithItsReason) {
  Fixture f;

  f.handler.on<ThriftServerCloseConnectionEvent>(
      f.ctx,
      ThriftServerCloseConnectionEvent{
          .rejection = SetupRejection{
              .code =
                  apache::thrift::fast_thrift::frame::ErrorCode::REJECTED_SETUP,
              .reason = "not welcome here"}});

  ASSERT_EQ(f.ctx.writes.size(), 1u);
  const auto& payload =
      f.ctx.writes.front().get<ThriftServerResponseMessage>().payload;
  ASSERT_TRUE(payload.is<ThriftSetupRejectionPayload>());
  const auto& rejection = payload.get<ThriftSetupRejectionPayload>();
  EXPECT_EQ(
      rejection.errorCode,
      static_cast<uint32_t>(
          apache::thrift::fast_thrift::frame::ErrorCode::REJECTED_SETUP));
  EXPECT_EQ(rejection.reason->to<std::string>(), "not welcome here");
  EXPECT_TRUE(f.handler.isClosed());
  EXPECT_EQ(f.ctx.pipeline_.deactivateCount, 1);
}

// A CONNECTION_CLOSE that never reaches the wire leaves nothing to drain for:
// the client cannot act on a frame it will not see. Idle, that closes outright.
TEST(
    ThriftServerConnectionCloseHandlerTest,
    FailedCloseWriteClosesImmediatelyWhenIdle) {
  Fixture f;
  f.ctx.writeResult = Result::Error;

  f.handler.on<ThriftServerCloseConnectionEvent>(
      f.ctx, ThriftServerCloseConnectionEvent{});

  EXPECT_TRUE(f.handler.isClosed());
  EXPECT_EQ(f.ctx.pipeline_.deactivateCount, 1);
  ASSERT_EQ(f.ctx.events.size(), 1u);
}

// In-flight work does not buy a drain window when the frame never went out:
// close anyway and let the in-flight requests fail. This is the same close the
// reap timer performs with stragglers outstanding.
TEST(
    ThriftServerConnectionCloseHandlerTest,
    FailedCloseWriteClosesWithInFlightOutstanding) {
  Fixture f;

  ASSERT_EQ(
      f.handler.onRead(f.ctx, erase_and_box(makeRequest(1))), Result::Success);
  ASSERT_EQ(
      f.handler.onRead(f.ctx, erase_and_box(makeRequest(2))), Result::Success);
  ASSERT_EQ(f.handler.inFlight(), 2u);

  f.ctx.writeResult = Result::Error;
  f.handler.on<ThriftServerCloseConnectionEvent>(
      f.ctx, ThriftServerCloseConnectionEvent{});

  EXPECT_TRUE(f.handler.isClosed());
  EXPECT_FALSE(f.handler.isDraining()) << "no point draining for a lost frame";
  EXPECT_EQ(f.handler.inFlight(), 2u) << "the requests are failed, not awaited";
  EXPECT_EQ(f.ctx.pipeline_.deactivateCount, 1)
      << "the socket is still ours to tear down";
  ASSERT_EQ(f.ctx.events.size(), 1u);
}

TEST(
    ThriftServerConnectionCloseHandlerTest,
    CloseConnectionEventWaitsForInFlightToReachZero) {
  Fixture f;

  ASSERT_EQ(
      f.handler.onRead(f.ctx, erase_and_box(makeRequest(1))), Result::Success);
  ASSERT_EQ(
      f.handler.onRead(f.ctx, erase_and_box(makeRequest(2))), Result::Success);
  ASSERT_EQ(f.handler.inFlight(), 2u);

  f.handler.on<ThriftServerCloseConnectionEvent>(
      f.ctx, ThriftServerCloseConnectionEvent{});

  ASSERT_EQ(f.ctx.writes.size(), 1u);
  expectConnectionCloseFrame(f.ctx.writes.front());
  EXPECT_TRUE(f.handler.isDraining());
  EXPECT_FALSE(f.handler.isClosed());
  EXPECT_EQ(f.ctx.pipeline_.deactivateCount, 0);

  EXPECT_EQ(
      f.handler.onWrite(f.ctx, erase_and_box(makeResponse(1))),
      Result::Success);
  EXPECT_EQ(f.handler.inFlight(), 1u);
  EXPECT_EQ(f.ctx.pipeline_.deactivateCount, 0);

  EXPECT_EQ(
      f.handler.onWrite(f.ctx, erase_and_box(makeResponse(2))),
      Result::Success);
  EXPECT_EQ(f.handler.inFlight(), 0u);
  EXPECT_TRUE(f.handler.isClosed());
  EXPECT_EQ(f.ctx.pipeline_.deactivateCount, 1);
  ASSERT_EQ(f.ctx.events.size(), 1u);
}

TEST(ThriftServerConnectionCloseHandlerTest, CloseConnectionEventIsIdempotent) {
  Fixture f;

  f.handler.on<ThriftServerCloseConnectionEvent>(
      f.ctx, ThriftServerCloseConnectionEvent{});
  EXPECT_EQ(f.ctx.writes.size(), 1u);
  EXPECT_EQ(f.ctx.pipeline_.deactivateCount, 1);

  f.handler.on<ThriftServerCloseConnectionEvent>(
      f.ctx, ThriftServerCloseConnectionEvent{});
  EXPECT_EQ(f.ctx.writes.size(), 1u);
  EXPECT_EQ(f.ctx.pipeline_.deactivateCount, 1);
}

// =============================================================================
// Drain timeout — drain fails to settle, escalates to reap
// =============================================================================

TEST(
    ThriftServerConnectionCloseHandlerTest,
    DrainTimeoutForceDeactivatesAndEntersReaping) {
  Fixture f{/*drainTimeout=*/std::chrono::milliseconds{5},
            /*reapTimeout=*/std::chrono::milliseconds{60'000}};
  // Wire the pipeline cascade so deactivate() also fires
  // onPipelineInactive — that's the production behavior the state
  // machine relies on.
  f.ctx.pipeline_.onDeactivate = [&] { f.handler.onPipelineInactive(f.ctx); };

  ASSERT_EQ(
      f.handler.onRead(f.ctx, erase_and_box(makeRequest(1))), Result::Success);
  f.handler.on<ThriftServerCloseConnectionEvent>(
      f.ctx, ThriftServerCloseConnectionEvent{});
  ASSERT_TRUE(f.handler.isDraining());

  // Drive the EventBase past the drain deadline. Reap timer is long
  // enough to not fire within this window.
  f.ctx.evb_.runAfterDelay(
      [&] { f.ctx.evb_.terminateLoopSoon(); }, /*milliseconds=*/50);
  f.ctx.evb_.loopForever();

  EXPECT_EQ(f.ctx.pipeline_.deactivateCount, 1);
  EXPECT_TRUE(f.handler.isReaping());
  EXPECT_TRUE(f.ctx.events.empty());
}

// =============================================================================
// Reap timeout — force-close path (replaces previous LOG(FATAL))
// =============================================================================

TEST(
    ThriftServerConnectionCloseHandlerTest,
    ReapTimeoutForcesGracefulCloseWithInFlightOutstanding) {
  // Reap fires while in-flight is still nonzero. The new behavior is
  // a graceful force-close: warn log + transitionToClosed (fires
  // ConnectionClosed inbound) instead of LOG(FATAL). The adapter's
  // pipelineActive_ flag will pick up the event and cause straggler
  // writes to be dropped at that layer; here we just verify the
  // close handler's state and event emission.
  Fixture f{/*drainTimeout=*/std::chrono::milliseconds{5},
            /*reapTimeout=*/std::chrono::milliseconds{10}};
  f.ctx.pipeline_.onDeactivate = [&] { f.handler.onPipelineInactive(f.ctx); };

  ASSERT_EQ(
      f.handler.onRead(f.ctx, erase_and_box(makeRequest(1))), Result::Success);
  f.handler.on<ThriftServerCloseConnectionEvent>(
      f.ctx, ThriftServerCloseConnectionEvent{});
  ASSERT_TRUE(f.handler.isDraining());

  // Drive past both drain and reap deadlines. Drain → reap → force-close.
  f.ctx.evb_.runAfterDelay(
      [&] { f.ctx.evb_.terminateLoopSoon(); }, /*milliseconds=*/100);
  f.ctx.evb_.loopForever();

  EXPECT_TRUE(f.handler.isClosed());
  EXPECT_FALSE(f.handler.isReaping());
  // Pipeline deactivated exactly once (by the drain-timer escalation).
  // The reap-timeout force-close path does NOT re-deactivate.
  EXPECT_EQ(f.ctx.pipeline_.deactivateCount, 1);
  // ConnectionClosed event was fired despite in-flight > 0 — the
  // adapter receives this and tears down its pipeline binding.
  ASSERT_EQ(f.ctx.events.size(), 1u);
  // In-flight is intentionally NOT decremented — the straggler is
  // still alive in the application. The close handler is done with it.
  EXPECT_EQ(f.handler.inFlight(), 1u);
}

// =============================================================================
// Exception path
// =============================================================================

TEST(
    ThriftServerConnectionCloseHandlerTest,
    ExceptionsAreForwardedWithoutEngagingDrain) {
  Fixture f;

  using TX = apache::thrift::transport::TTransportException;
  f.handler.onException(
      f.ctx, folly::make_exception_wrapper<TX>(TX::END_OF_FILE, "peer FIN"));
  f.handler.onException(
      f.ctx,
      folly::make_exception_wrapper<TX>(
          TX::CORRUPTED_DATA, "real protocol fault"));

  EXPECT_FALSE(f.handler.isDraining());
  EXPECT_FALSE(f.handler.isClosed());
  EXPECT_EQ(f.ctx.pipeline_.deactivateCount, 0);
  EXPECT_EQ(f.ctx.exceptions.size(), 2u);
  EXPECT_TRUE(f.ctx.writes.empty());
}

// =============================================================================
// Pipeline lifecycle — forceful close paths
// =============================================================================

TEST(
    ThriftServerConnectionCloseHandlerTest,
    OnPipelineInactiveWithNoInFlightClosesImmediately) {
  Fixture f;

  f.handler.onPipelineInactive(f.ctx);

  EXPECT_TRUE(f.handler.isClosed());
  EXPECT_FALSE(f.handler.isReaping());
  EXPECT_EQ(f.ctx.pipeline_.deactivateCount, 0);
  ASSERT_EQ(f.ctx.events.size(), 1u);
}

TEST(
    ThriftServerConnectionCloseHandlerTest,
    OnPipelineInactiveWithInFlightEntersReaping) {
  Fixture f;

  ASSERT_EQ(
      f.handler.onRead(f.ctx, erase_and_box(makeRequest(1))), Result::Success);
  ASSERT_EQ(f.handler.inFlight(), 1u);

  f.handler.onPipelineInactive(f.ctx);

  EXPECT_TRUE(f.handler.isReaping());
  EXPECT_FALSE(f.handler.isClosed());
  EXPECT_TRUE(f.ctx.events.empty());
  EXPECT_EQ(f.ctx.pipeline_.deactivateCount, 0);
}

TEST(
    ThriftServerConnectionCloseHandlerTest,
    ForceCloseTerminatesReapingExactlyOnce) {
  Fixture f;

  ASSERT_EQ(
      f.handler.onRead(f.ctx, erase_and_box(makeRequest(1))), Result::Success);
  f.handler.onPipelineInactive(f.ctx);
  ASSERT_TRUE(f.handler.isReaping());

  const ThriftServerCloseConnectionEvent forceClose{
      .rejection = std::nullopt, .forceClose = true};
  f.handler.on<ThriftServerCloseConnectionEvent>(f.ctx, forceClose);
  f.handler.on<ThriftServerCloseConnectionEvent>(f.ctx, forceClose);

  EXPECT_TRUE(f.handler.isClosed());
  EXPECT_EQ(f.handler.inFlight(), 1u);
  EXPECT_TRUE(f.ctx.writes.empty());
  EXPECT_EQ(f.ctx.events.size(), 1u);
}

TEST(
    ThriftServerConnectionCloseHandlerTest,
    ReapingExitsOnLastInFlightDecrement) {
  Fixture f;

  ASSERT_EQ(
      f.handler.onRead(f.ctx, erase_and_box(makeRequest(1))), Result::Success);
  ASSERT_EQ(
      f.handler.onRead(f.ctx, erase_and_box(makeRequest(2))), Result::Success);
  f.handler.onPipelineInactive(f.ctx);
  ASSERT_TRUE(f.handler.isReaping());

  // Reaping bails before forwarding (the socket is gone) but still
  // decrements the counter so the close can finalize.
  EXPECT_EQ(
      f.handler.onWrite(f.ctx, erase_and_box(makeResponse(1))), Result::Error);
  EXPECT_TRUE(f.ctx.writes.empty());
  EXPECT_TRUE(f.handler.isReaping());
  EXPECT_TRUE(f.ctx.events.empty());

  EXPECT_EQ(
      f.handler.onWrite(f.ctx, erase_and_box(makeResponse(2))), Result::Error);
  EXPECT_TRUE(f.ctx.writes.empty());
  EXPECT_TRUE(f.handler.isClosed());
  // Reaping path does NOT re-deactivate the pipeline — that's what
  // triggered the reap in the first place.
  EXPECT_EQ(f.ctx.pipeline_.deactivateCount, 0);
  ASSERT_EQ(f.ctx.events.size(), 1u);
}

TEST(ThriftServerConnectionCloseHandlerTest, ReapingRejectsNewInbound) {
  Fixture f;

  ASSERT_EQ(
      f.handler.onRead(f.ctx, erase_and_box(makeRequest(1))), Result::Success);
  f.handler.onPipelineInactive(f.ctx);
  ASSERT_TRUE(f.handler.isReaping());

  EXPECT_EQ(
      f.handler.onRead(f.ctx, erase_and_box(makeRequest(2))), Result::Error);
  EXPECT_EQ(f.handler.inFlight(), 1u);
}

TEST(
    ThriftServerConnectionCloseHandlerTest,
    OnPipelineInactiveIsIdempotentInClosed) {
  Fixture f;

  f.handler.onPipelineInactive(f.ctx);
  ASSERT_TRUE(f.handler.isClosed());
  ASSERT_EQ(f.ctx.events.size(), 1u);

  f.handler.onPipelineInactive(f.ctx);
  EXPECT_EQ(f.ctx.events.size(), 1u);
}

// =============================================================================
// Timeout configuration
// =============================================================================

TEST(ThriftServerConnectionCloseHandlerTest, TimeoutOverridesHonored) {
  Fixture f{/*drainTimeout=*/std::chrono::milliseconds{7},
            /*reapTimeout=*/std::chrono::milliseconds{42}};
  EXPECT_EQ(f.handler.drainTimeout(), std::chrono::milliseconds{7});
  EXPECT_EQ(f.handler.reapTimeout(), std::chrono::milliseconds{42});
}

} // namespace apache::thrift::fast_thrift::thrift
