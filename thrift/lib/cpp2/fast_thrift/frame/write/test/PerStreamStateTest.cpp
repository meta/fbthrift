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

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>
#include <utility>

#include <gtest/gtest.h>

#include <folly/io/IOBuf.h>
#include <folly/portability/GMock.h>

#include <thrift/lib/cpp2/fast_thrift/frame/write/PerStreamState.h>

namespace apache::thrift::fast_thrift::frame::write {

using namespace testing;

namespace {

constexpr uint32_t kStreamId = 7;

std::unique_ptr<folly::IOBuf> buffer(std::string_view contents) {
  return folly::IOBuf::copyBuffer(contents.data(), contents.size());
}

void expectBuffer(
    const std::unique_ptr<folly::IOBuf>& actual, std::string_view expected) {
  ASSERT_THAT(actual, NotNull());
  const std::unique_ptr<folly::IOBuf> expectedBuffer = buffer(expected);
  EXPECT_THAT(folly::IOBufEqualTo{}(*actual, *expectedBuffer), IsTrue());
}

TEST(PerStreamStateDeathTest, StartingFrameRequiresEmptyDataQueue) {
  PerStreamState state;
  state.streamId = kStreamId;

  ComposedFrame first{
      .frameType = FrameType::REQUEST_CHANNEL,
      .streamId = kStreamId,
  };
  state.enqueue(std::move(first), buffer("abc"), false);
  (void)state.nextFragment(1);
  state.frames.pop_front();

  ComposedFrame second{
      .frameType = FrameType::REQUEST_RESPONSE,
      .streamId = kStreamId,
  };
  state.enqueue(std::move(second), nullptr, false);

  EXPECT_DEBUG_DEATH((void)state.nextFragment(1), "dataQueue.empty\\(\\)");
}

TEST(PerStreamStateTest, EmitsQueuedFramesInOrderWithFragmentFields) {
  PerStreamState state;
  state.streamId = kStreamId;

  ComposedFrame first{
      .frameType = FrameType::REQUEST_CHANNEL,
      .streamId = kStreamId,
      .metadata = buffer("first"),
      .complete = true,
  };
  const bool firstComplete = first.isComplete();
  state.enqueue(std::move(first), buffer("abcde"), firstComplete);

  ComposedFrame second{
      .frameType = FrameType::REQUEST_RESPONSE,
      .streamId = kStreamId,
      .metadata = buffer("second"),
  };
  const bool secondComplete = second.isComplete();
  state.enqueue(std::move(second), buffer("fg"), secondComplete);

  EXPECT_THAT(state.remaining(), Eq(7));

  const PerStreamState::Fragment firstFragment = state.nextFragment(2);
  const PerStreamState::Fragment secondFragment = state.nextFragment(2);
  const PerStreamState::Fragment thirdFragment = state.nextFragment(2);
  const PerStreamState::Fragment fourthFragment = state.nextFragment(2);

  const std::array<FrameType, 4> frameTypes{
      firstFragment.outFrame.frameType,
      secondFragment.outFrame.frameType,
      thirdFragment.outFrame.frameType,
      fourthFragment.outFrame.frameType,
  };
  const std::array<bool, 4> follows{
      firstFragment.outFrame.follows,
      secondFragment.outFrame.follows,
      thirdFragment.outFrame.follows,
      fourthFragment.outFrame.follows,
  };
  const std::array<bool, 4> next{
      firstFragment.outFrame.next,
      secondFragment.outFrame.next,
      thirdFragment.outFrame.next,
      fourthFragment.outFrame.next,
  };
  const std::array<bool, 4> complete{
      firstFragment.outFrame.complete,
      secondFragment.outFrame.complete,
      thirdFragment.outFrame.complete,
      fourthFragment.outFrame.complete,
  };
  const std::array<size_t, 4> payloadBytes{
      firstFragment.payloadBytes,
      secondFragment.payloadBytes,
      thirdFragment.payloadBytes,
      fourthFragment.payloadBytes,
  };
  const std::array<bool, 4> frameDone{
      firstFragment.currentFrameDone,
      secondFragment.currentFrameDone,
      thirdFragment.currentFrameDone,
      fourthFragment.currentFrameDone,
  };

  EXPECT_THAT(
      frameTypes,
      ElementsAre(
          FrameType::REQUEST_CHANNEL,
          FrameType::PAYLOAD,
          FrameType::PAYLOAD,
          FrameType::REQUEST_RESPONSE));
  EXPECT_THAT(follows, ElementsAre(true, true, false, false));
  EXPECT_THAT(next, ElementsAre(false, true, true, false));
  EXPECT_THAT(complete, ElementsAre(false, false, true, false));
  EXPECT_THAT(payloadBytes, ElementsAre(2, 2, 1, 2));
  EXPECT_THAT(frameDone, ElementsAre(false, false, true, true));

  expectBuffer(firstFragment.outFrame.metadata, "first");
  EXPECT_THAT(secondFragment.outFrame.metadata, IsNull());
  EXPECT_THAT(thirdFragment.outFrame.metadata, IsNull());
  expectBuffer(fourthFragment.outFrame.metadata, "second");

  expectBuffer(firstFragment.outFrame.data, "ab");
  expectBuffer(secondFragment.outFrame.data, "cd");
  expectBuffer(thirdFragment.outFrame.data, "e");
  expectBuffer(fourthFragment.outFrame.data, "fg");

  EXPECT_THAT(state.remaining(), Eq(0));
  EXPECT_THAT(state.hasMore(), IsFalse());
}

TEST(PerStreamStateTest, EmitsZeroDataFrame) {
  PerStreamState state;
  state.streamId = kStreamId;

  ComposedFrame frame{
      .frameType = FrameType::REQUEST_RESPONSE,
      .streamId = kStreamId,
  };
  const bool originalComplete = frame.isComplete();
  state.enqueue(std::move(frame), nullptr, originalComplete);

  const PerStreamState::Fragment fragment = state.nextFragment(8);

  EXPECT_THAT(fragment.outFrame.frameType, Eq(FrameType::REQUEST_RESPONSE));
  EXPECT_THAT(fragment.outFrame.streamId, Eq(kStreamId));
  expectBuffer(fragment.outFrame.data, "");
  EXPECT_THAT(fragment.payloadBytes, Eq(0));
  EXPECT_THAT(fragment.currentFrameDone, IsTrue());
  EXPECT_THAT(state.remaining(), Eq(0));
  EXPECT_THAT(state.hasMore(), IsFalse());
}

} // namespace
} // namespace apache::thrift::fast_thrift::frame::write
