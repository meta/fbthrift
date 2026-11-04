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

#include <thrift/lib/cpp2/fast_thrift/thrift/server/handler/ThriftServerRequestLifecycleHandler.h>

#include <gtest/gtest.h>

#include <folly/CancellationToken.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/Request.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/Event.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/Messages.h>

namespace apache::thrift::fast_thrift::thrift {
namespace {

using channel_pipeline::erase_and_box;
using channel_pipeline::Result;
using channel_pipeline::TypeErasedBox;

class FakeContext {
 public:
  Result fireRead(TypeErasedBox&& msg) noexcept {
    if (retainReads) {
      reads.push_back(std::move(msg));
    } else {
      auto consumed = std::move(msg);
    }
    return readResult;
  }

  Result fireWrite(TypeErasedBox&& msg) noexcept {
    writes.push_back(std::move(msg));
    return Result::Success;
  }

  template <channel_pipeline::PipelineEvent E>
  void fireEvent(const typename E::Payload& event) noexcept {
    static_assert(std::same_as<E, ThriftServerRequestCompletedEvent>);
    completions.push_back(event);
  }

  std::vector<TypeErasedBox> reads;
  std::vector<TypeErasedBox> writes;
  std::vector<ThriftServerRequestCompletedEvent> completions;
  Result readResult{Result::Success};
  bool retainReads{true};
};

ThriftServerRequestMessage makeRequest(
    folly::EventBase& evb, uint32_t streamId) {
  ThriftServerRequestMessage request;
  request.streamId = streamId;
  request.requestContext = makeThriftRequestContext(evb);
  request.payload = ThriftServerRequestResponsePayload{};
  return request;
}

ThriftServerResponseMessage makeResponse(uint32_t streamId) {
  return ThriftServerResponseMessage{
      .payload = ThriftInitialResponsePayload{.streamId = streamId}};
}

ThriftServerResponseMessage makeResponseFor(
    FakeContext& ctx, uint32_t streamId) {
  auto response = makeResponse(streamId);
  for (auto& requestBox : ctx.reads) {
    auto& request = requestBox.get<ThriftServerRequestMessage>();
    if (request.streamId == streamId) {
      response.requestContext = std::move(request.requestContext);
      break;
    }
  }
  return response;
}

} // namespace

TEST(ThriftServerRequestLifecycleHandlerTest, ResponseWinsRace) {
  folly::EventBase evb;
  FakeContext ctx;
  ThriftServerRequestLifecycleHandler<FakeContext> handler;
  auto request = makeRequest(evb, 1);
  auto* requestContext = request.requestContext.get();

  EXPECT_EQ(
      handler.onRead(ctx, erase_and_box(std::move(request))), Result::Success);
  EXPECT_TRUE(requestContext->isCancellationEnabled());
  EXPECT_EQ(
      handler.onWrite(ctx, erase_and_box(makeResponseFor(ctx, 1))),
      Result::Success);
  EXPECT_EQ(ctx.writes.size(), 1);
  EXPECT_TRUE(ctx.completions.empty());
  EXPECT_EQ(handler.requestCount(), 0);
  EXPECT_FALSE(requestContext->requestCancellation());
}

TEST(ThriftServerRequestLifecycleHandlerTest, CancellationWinsRace) {
  folly::EventBase evb;
  FakeContext ctx;
  ThriftServerRequestLifecycleHandler<FakeContext> handler;
  auto request = makeRequest(evb, 3);
  auto* requestContext = request.requestContext.get();

  ASSERT_EQ(
      handler.onRead(ctx, erase_and_box(std::move(request))), Result::Success);
  auto token = requestContext->getCancellationToken();
  handler.on<ThriftServerRequestCancellationEvent>(
      ctx, ThriftServerRequestCancellationEvent{.streamId = 3});
  EXPECT_TRUE(token.isCancellationRequested());

  EXPECT_FALSE(requestContext->requestCancellation());

  EXPECT_EQ(
      handler.onWrite(ctx, erase_and_box(makeResponseFor(ctx, 3))),
      Result::Success);
  EXPECT_TRUE(ctx.writes.empty());
  ASSERT_EQ(ctx.completions.size(), 1);
  EXPECT_EQ(ctx.completions.front().streamId, 3);
  EXPECT_EQ(handler.requestCount(), 0);
}

TEST(
    ThriftServerRequestLifecycleHandlerTest,
    DuplicateCancellationIsIdempotent) {
  folly::EventBase evb;
  FakeContext ctx;
  ThriftServerRequestLifecycleHandler<FakeContext> handler;
  auto request = makeRequest(evb, 5);
  auto* requestContext = request.requestContext.get();
  int cancellationCallbacks = 0;
  folly::RequestContext* callbackContext = nullptr;
  folly::RequestContextScopeGuard callerGuard;
  auto* const callerContext = folly::RequestContext::try_get();

  ASSERT_EQ(
      handler.onRead(ctx, erase_and_box(std::move(request))), Result::Success);
  folly::CancellationCallback cancellationCallback(
      requestContext->getCancellationToken(), [&] {
        ++cancellationCallbacks;
        callbackContext = folly::RequestContext::try_get();
      });
  const auto event = ThriftServerRequestCancellationEvent{.streamId = 5};
  handler.on<ThriftServerRequestCancellationEvent>(ctx, event);
  handler.on<ThriftServerRequestCancellationEvent>(ctx, event);

  EXPECT_EQ(cancellationCallbacks, 1);
  EXPECT_EQ(callbackContext, callerContext);
  EXPECT_EQ(folly::RequestContext::try_get(), callerContext);
  EXPECT_EQ(handler.requestCount(), 1);
}

TEST(
    ThriftServerRequestLifecycleHandlerTest,
    CancelledStreamIdCannotBeReusedWhileApplicationIsRunning) {
  folly::EventBase evb;
  FakeContext ctx;
  ThriftServerRequestLifecycleHandler<FakeContext> handler;
  auto request = makeRequest(evb, 5);

  ASSERT_EQ(
      handler.onRead(ctx, erase_and_box(std::move(request))), Result::Success);
  handler.on<ThriftServerRequestCancellationEvent>(
      ctx, ThriftServerRequestCancellationEvent{.streamId = 5});

  EXPECT_EQ(
      handler.onRead(ctx, erase_and_box(makeRequest(evb, 5))), Result::Error);
  EXPECT_EQ(handler.requestCount(), 1);
  EXPECT_EQ(ctx.reads.size(), 1u);
}

TEST(
    ThriftServerRequestLifecycleHandlerTest,
    CancellationCallbackCanCompleteAndDestroyRequest) {
  folly::EventBase evb;
  for (bool inactive : {false, true}) {
    FakeContext ctx;
    ThriftServerRequestLifecycleHandler<FakeContext> handler;
    auto request = makeRequest(evb, 5);
    request.requestContext->enableCancellation();
    auto token = request.requestContext->getCancellationToken();
    int cancellationCallbacks = 0;
    folly::CancellationCallback cancellationCallback(token, [&] {
      ++cancellationCallbacks;
      handler.on<ThriftServerRequestCompletedEvent>(
          ctx, ThriftServerRequestCompletedEvent{.streamId = 5});
      ctx.reads.clear();
    });

    ASSERT_EQ(
        handler.onRead(ctx, erase_and_box(std::move(request))),
        Result::Success);
    if (inactive) {
      handler.onPipelineInactive(ctx);
    } else {
      handler.on<ThriftServerRequestCancellationEvent>(
          ctx, ThriftServerRequestCancellationEvent{.streamId = 5});
    }

    EXPECT_TRUE(token.isCancellationRequested());
    EXPECT_EQ(cancellationCallbacks, 1);
    EXPECT_EQ(handler.requestCount(), 0u);
    EXPECT_TRUE(ctx.reads.empty());
  }
}

TEST(
    ThriftServerRequestLifecycleHandlerTest,
    DownstreamReadFailureCancelsAndForgetsRequest) {
  folly::EventBase evb;
  FakeContext ctx;
  ctx.readResult = Result::Error;
  ThriftServerRequestLifecycleHandler<FakeContext> handler;
  auto request = makeRequest(evb, 6);
  auto* requestContext = request.requestContext.get();
  requestContext->enableCancellation();
  folly::RequestContextScopeGuard callerGuard;
  auto* const callerContext = folly::RequestContext::try_get();
  folly::RequestContext* callbackContext = nullptr;
  folly::CancellationCallback cancellationCallback(
      requestContext->getCancellationToken(),
      [&] { callbackContext = folly::RequestContext::try_get(); });

  EXPECT_EQ(
      handler.onRead(ctx, erase_and_box(std::move(request))), Result::Error);
  EXPECT_TRUE(requestContext->getCancellationToken().isCancellationRequested());
  EXPECT_EQ(callbackContext, callerContext);
  EXPECT_EQ(folly::RequestContext::try_get(), callerContext);
  EXPECT_EQ(handler.requestCount(), 0u);
}

TEST(
    ThriftServerRequestLifecycleHandlerTest,
    DownstreamReadFailureDestroysRequestBeforeCancellation) {
  folly::EventBase evb;
  FakeContext ctx;
  ctx.readResult = Result::Error;
  ctx.retainReads = false;
  ThriftServerRequestLifecycleHandler<FakeContext> handler;
  auto request = makeRequest(evb, 6);
  request.requestContext->enableCancellation();
  auto token = request.requestContext->getCancellationToken();
  int cancellationCallbacks = 0;
  folly::CancellationCallback cancellationCallback(
      token, [&] { ++cancellationCallbacks; });

  EXPECT_EQ(
      handler.onRead(ctx, erase_and_box(std::move(request))), Result::Error);
  EXPECT_TRUE(ctx.reads.empty());
  EXPECT_TRUE(token.isCancellationRequested());
  EXPECT_EQ(cancellationCallbacks, 1);
  EXPECT_EQ(handler.requestCount(), 0u);

  handler.on<ThriftServerRequestCancellationEvent>(
      ctx, ThriftServerRequestCancellationEvent{.streamId = 6});
  handler.onPipelineInactive(ctx);
  EXPECT_EQ(cancellationCallbacks, 1);
}

TEST(
    ThriftServerRequestLifecycleHandlerTest,
    StreamControlMessagesAreNotEnrolledAsRequests) {
  folly::EventBase evb;
  FakeContext ctx;
  ctx.retainReads = false;
  ThriftServerRequestLifecycleHandler<FakeContext> handler;
  auto request = makeRequest(evb, 6);
  request.payload = ThriftRequestNPayload{.streamId = 6, .requestN = 1};

  EXPECT_EQ(
      handler.onRead(ctx, erase_and_box(std::move(request))), Result::Success);
  EXPECT_EQ(handler.requestCount(), 0u);

  handler.on<ThriftServerRequestCancellationEvent>(
      ctx, ThriftServerRequestCancellationEvent{.streamId = 6});
  handler.onPipelineInactive(ctx);
  EXPECT_EQ(handler.requestCount(), 0u);
}

TEST(ThriftServerRequestLifecycleHandlerTest, ConnectionFailureCancelsAll) {
  folly::EventBase evb;
  FakeContext ctx;
  ThriftServerRequestLifecycleHandler<FakeContext> handler;
  auto first = makeRequest(evb, 7);
  auto second = makeRequest(evb, 9);
  auto* firstContext = first.requestContext.get();
  auto* secondContext = second.requestContext.get();

  ASSERT_EQ(
      handler.onRead(ctx, erase_and_box(std::move(first))), Result::Success);
  ASSERT_EQ(
      handler.onRead(ctx, erase_and_box(std::move(second))), Result::Success);
  auto firstToken = firstContext->getCancellationToken();
  auto secondToken = secondContext->getCancellationToken();
  handler.onPipelineInactive(ctx);

  EXPECT_TRUE(firstToken.isCancellationRequested());
  EXPECT_TRUE(secondToken.isCancellationRequested());
  EXPECT_EQ(handler.requestCount(), 0);

  ctx.reads.clear();
  handler.on<ThriftServerRequestCancellationEvent>(
      ctx, ThriftServerRequestCancellationEvent{.streamId = 7});
  EXPECT_EQ(handler.requestCount(), 0);
}

TEST(
    ThriftServerRequestLifecycleHandlerTest,
    CancellationAcknowledgementRetiresOnce) {
  folly::EventBase evb;
  FakeContext ctx;
  ThriftServerRequestLifecycleHandler<FakeContext> handler;
  auto request = makeRequest(evb, 11);

  ASSERT_EQ(
      handler.onRead(ctx, erase_and_box(std::move(request))), Result::Success);
  const auto event = ThriftServerRequestCompletedEvent{.streamId = 11};
  handler.on<ThriftServerRequestCompletedEvent>(ctx, event);
  handler.on<ThriftServerRequestCompletedEvent>(ctx, event);

  EXPECT_EQ(handler.requestCount(), 0);
}

} // namespace apache::thrift::fast_thrift::thrift
