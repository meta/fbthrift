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

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include <folly/io/IOBuf.h>
#include <folly/portability/GMock.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Handler.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/handler/FifoFrameFragmentationHandler.h>

namespace apache::thrift::fast_thrift::frame::write::handler {

using namespace testing;

namespace {

struct TestPublishedEvent : channel_pipeline::EventTag<int> {};
struct TestSubscribedEvent : channel_pipeline::EventTag<int> {};

struct RecordingTracker {
  using PublishedEvents = channel_pipeline::Events<TestPublishedEvent>;
  using SubscribedEvents = channel_pipeline::Events<TestSubscribedEvent>;

  struct FragmentCall {
    uint32_t streamId;
    bool isLastFragment;

    bool operator==(const FragmentCall&) const = default;
  };

  void onFragment(uint32_t streamId, bool isLastFragment) noexcept {
    fragments.push_back({streamId, isLastFragment});
  }

  [[maybe_unused]] void onFragmentAborted() noexcept {}

  [[maybe_unused]] void onFlush() noexcept {}

  template <channel_pipeline::PipelineEvent E, typename Context>
    requires std::same_as<E, TestSubscribedEvent>
  void on(
      Context& /*ctx*/,
      const int& event,
      bool handlerHasPendingWrites) noexcept {
    ++subscribedEventCount;
    subscribedEvent = event;
    hadPendingWrites = handlerHasPendingWrites;
  }

  std::vector<FragmentCall> fragments;
  size_t subscribedEventCount{0};
  int subscribedEvent{0};
  bool hadPendingWrites{true};
};

static_assert(FragmentCompletionTracker<RecordingTracker>);

using Handler = FifoFrameFragmentationHandlerT<RecordingTracker>;

class TestContext {
 public:
  explicit TestContext(channel_pipeline::Result result) : result_(result) {}

  channel_pipeline::Result fireWrite(
      channel_pipeline::TypeErasedBox&& msg) noexcept {
    forwarded_.emplace(msg.take<ComposedFrame>());
    return result_;
  }

  const ComposedFrame& forwarded() const noexcept { return *forwarded_; }

 private:
  const channel_pipeline::Result result_;
  std::optional<ComposedFrame> forwarded_;
};

static_assert(channel_pipeline::OutboundHandler<Handler, TestContext>);
static_assert(std::same_as<
              Handler::PublishedEvents,
              channel_pipeline::Events<TestPublishedEvent>>);
static_assert(std::same_as<
              Handler::SubscribedEvents,
              channel_pipeline::Events<TestSubscribedEvent>>);

std::unique_ptr<folly::IOBuf> buffer(std::string_view contents) {
  return folly::IOBuf::copyBuffer(contents.data(), contents.size());
}

auto frameData(const ComposedFrame& frame) {
  return std::tuple{
      frame.frameType,
      frame.streamId,
      frame.metadata.get(),
      frame.data.get(),
      frame.follows,
      frame.complete,
      frame.next,
      frame.ignore,
      frame.lease,
      frame.respond,
      frame.errorCode,
      frame.requestN,
      frame.initialRequestN,
      frame.extendedType,
      frame.lastReceivedPosition,
      frame.majorVersion,
      frame.minorVersion,
      frame.keepaliveTime,
      frame.maxLifetime};
}

TEST(FifoFrameFragmentationHandlerTest, ForwardsFrameUnchangedOnSuccess) {
  Handler handler;
  TestContext context(channel_pipeline::Result::Success);
  ComposedFrame frame{
      .frameType = FrameType::REQUEST_CHANNEL,
      .streamId = 17,
      .metadata = buffer("metadata"),
      .data = buffer("data"),
      .follows = true,
      .complete = true,
      .next = true,
      .ignore = true,
      .lease = true,
      .respond = true,
      .errorCode = 101,
      .requestN = 102,
      .initialRequestN = 103,
      .extendedType = 104,
      .lastReceivedPosition = 105,
      .majorVersion = 3,
      .minorVersion = 4,
      .keepaliveTime = 106,
      .maxLifetime = 107,
  };
  const std::tuple expectedFrameData = frameData(frame);

  const channel_pipeline::Result result = handler.onWrite(
      context, channel_pipeline::TypeErasedBox(std::move(frame)));

  EXPECT_THAT(result, Eq(channel_pipeline::Result::Success));
  EXPECT_THAT(frameData(context.forwarded()), Eq(expectedFrameData));

  const std::vector<RecordingTracker::FragmentCall> expectedFragments{
      {.streamId = 17, .isLastFragment = true}};
  EXPECT_THAT(handler.tracker().fragments, Eq(expectedFragments));
}

TEST(FifoFrameFragmentationHandlerTest, RecordsBackpressuredWrite) {
  Handler handler;
  TestContext context(channel_pipeline::Result::Backpressure);
  ComposedFrame frame{
      .frameType = FrameType::PAYLOAD,
      .streamId = 23,
  };

  const channel_pipeline::Result result = handler.onWrite(
      context, channel_pipeline::TypeErasedBox(std::move(frame)));

  EXPECT_THAT(result, Eq(channel_pipeline::Result::Backpressure));
  const std::vector<RecordingTracker::FragmentCall> expectedFragments{
      {.streamId = 23, .isLastFragment = true}};
  EXPECT_THAT(handler.tracker().fragments, Eq(expectedFragments));
}

TEST(FifoFrameFragmentationHandlerTest, ReturnsErrorWithoutRecordingWrite) {
  Handler handler;
  TestContext context(channel_pipeline::Result::Error);
  ComposedFrame frame{
      .frameType = FrameType::PAYLOAD,
      .streamId = 29,
  };

  const channel_pipeline::Result result = handler.onWrite(
      context, channel_pipeline::TypeErasedBox(std::move(frame)));

  EXPECT_THAT(result, Eq(channel_pipeline::Result::Error));
  EXPECT_THAT(handler.tracker().fragments, IsEmpty());
}

TEST(FifoFrameFragmentationHandlerTest, ForwardsEventsWithoutPendingWrites) {
  Handler handler;
  TestContext context(channel_pipeline::Result::Success);

  handler.handlerAdded(context);
  handler.onWriteReady(context);
  handler.on<TestSubscribedEvent>(context, 31);
  handler.onPipelineInactive(context);
  handler.handlerRemoved(context);

  EXPECT_THAT(handler.tracker().subscribedEventCount, Eq(1));
  EXPECT_THAT(handler.tracker().subscribedEvent, Eq(31));
  EXPECT_THAT(handler.tracker().hadPendingWrites, IsFalse());
}

} // namespace
} // namespace apache::thrift::fast_thrift::frame::write::handler
