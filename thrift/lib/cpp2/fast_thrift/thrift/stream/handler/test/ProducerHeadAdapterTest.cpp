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

#include <thrift/lib/cpp2/fast_thrift/thrift/stream/handler/ProducerHeadAdapter.h>

#include <stdexcept>
#include <vector>

#include <gtest/gtest.h>

#include <folly/ExceptionWrapper.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/stream/common/Messages.h>

namespace apache::thrift::fast_thrift::thrift::stream {

namespace {

using channel_pipeline::erase_and_box;
using channel_pipeline::Result;
using channel_pipeline::TypeErasedBox;

// onWrite ignores its context; a stub satisfies the signature.
struct StubContext {};

// Records the terminal messages the head adapter hands to the sink. Wired via a
// sink that captures the recorder by reference, mirroring how the owning
// thrift-pipeline handler captures its own instance.
struct SinkRecorder {
  std::vector<DataMessage> msgs;

  Result record(DataMessage&& msg) noexcept {
    msgs.push_back(std::move(msg));
    return Result::Success;
  }
};

using Handler = ProducerHeadAdapter;

StreamSink recorderSink(SinkRecorder& recorder) {
  return StreamSink{[&recorder](DataMessage&& msg) noexcept {
    return recorder.record(std::move(msg));
  }};
}

TypeErasedBox payload() {
  return erase_and_box(DataMessage{.payload = Payload{.data = nullptr}});
}
TypeErasedBox complete() {
  return erase_and_box(DataMessage{.payload = Complete{}});
}
TypeErasedBox error() {
  return erase_and_box(
      DataMessage{
          .payload = Error{
              .ex =
                  folly::make_exception_wrapper<std::runtime_error>("boom")}});
}

} // namespace

// =============================================================================
// Write path (toward the wire) delivers producer output to the sink supplied at
// construction.
// =============================================================================

TEST(ProducerHeadAdapterTest, DeliversProducerOutputToSink) {
  SinkRecorder recorder;
  Handler handler(recorderSink(recorder));
  StubContext ctx;

  EXPECT_EQ(handler.onWrite(ctx, payload()), Result::Success);
  EXPECT_EQ(handler.onWrite(ctx, complete()), Result::Success);
  EXPECT_EQ(handler.onWrite(ctx, error()), Result::Success);

  ASSERT_EQ(recorder.msgs.size(), 3u);
  EXPECT_TRUE(recorder.msgs[0].payload.is<Payload>());
  EXPECT_TRUE(recorder.msgs[1].payload.is<Complete>());
  EXPECT_TRUE(recorder.msgs[2].payload.is<Error>());
}

} // namespace apache::thrift::fast_thrift::thrift::stream
