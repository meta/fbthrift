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

// Unit test for ThriftServerStreamOpenPayload. The mux always consumes this
// payload before it can reach the transport, so its toRocketFrame() is a
// never-happens invariant guard: fatal in debug, and a terminal ERROR frame in
// release (so a routing bug fails the stream fast instead of hanging it).

#include <cstdint>
#include <memory>
#include <utility>

#include <gtest/gtest.h>

#include <folly/Portability.h>

#include <thrift/lib/cpp2/fast_thrift/frame/ErrorCode.h>
#include <thrift/lib/cpp2/fast_thrift/frame/FrameType.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/server/MetadataProtocol.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/StreamResponsePayloads.h>

namespace apache::thrift::fast_thrift::thrift {

namespace {

ThriftServerStreamOpenPayload makeOpen(uint32_t streamId) {
  return ThriftServerStreamOpenPayload{
      .initialResponse =
          ThriftStreamInitialResponsePayload{.streamId = streamId},
      .configFunc = std::make_unique<stream::ProducerPipeline::ConfigFunc>(
          [](folly::EventBase* evb, stream::StreamSink sink) {
            return stream::ProducerPipeline::build(
                evb, std::move(sink), [](auto builder) { return builder; });
          }),
  };
}

} // namespace

TEST(
    StreamResponsePayloadsDeathTest, OpenPayloadToRocketFrameIsInvariantGuard) {
  auto open = makeOpen(/*streamId=*/7);

  // Reaching toRocketFrame means the mux never consumed the open payload.
  if (folly::kIsDebug) {
    EXPECT_DEATH(
        (void)std::move(open).toRocketFrame(
            rocket::server::MetadataProtocol::COMPACT),
        "un-consumed");
  } else {
    auto frame = std::move(open).toRocketFrame(
        rocket::server::MetadataProtocol::COMPACT);
    EXPECT_EQ(frame.frameType, frame::FrameType::ERROR);
    EXPECT_EQ(frame.streamId, 7u);
    EXPECT_EQ(
        frame.errorCode, static_cast<uint32_t>(frame::ErrorCode::INVALID));
  }
}

} // namespace apache::thrift::fast_thrift::thrift
