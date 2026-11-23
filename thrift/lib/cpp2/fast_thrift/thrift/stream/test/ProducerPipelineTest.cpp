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

// Unit tests for ProducerPipeline and its Builder: the Builder owns the fixed
// head/tail endpoints and the sub-pipeline's own allocator, and the application
// only composes a producer between them. The tests build a pipeline, install a
// recording sink, and assert that inbound demand drives producer output out
// through the sink.

#include <thrift/lib/cpp2/fast_thrift/thrift/stream/ProducerPipeline.h>

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include <folly/ExceptionWrapper.h>
#include <folly/io/async/EventBase.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/HandlerTag.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/stream/common/Messages.h>

namespace apache::thrift::fast_thrift::thrift::stream {

namespace cp = channel_pipeline;

namespace {

ControlMessage makeRequestN(uint64_t n) {
  return ControlMessage{.payload = RequestN{.n = n}};
}

// A sample producer: on each RequestN it emits up to `n` payloads (bounded by
// the total it was given) then a single Complete. Plugged in by value between
// the endpoints, exactly as an application would compose its own producer.
template <typename Ctx>
class FiniteProducer {
 public:
  explicit FiniteProducer(uint64_t total) noexcept : remaining_(total) {}

  void handlerAdded(Ctx& /*ctx*/) noexcept {}
  void handlerRemoved(Ctx& /*ctx*/) noexcept {}
  void onPipelineActive(Ctx& /*ctx*/) noexcept {}
  void onPipelineInactive(Ctx& /*ctx*/) noexcept {}
  void onReadReady(Ctx& /*ctx*/) noexcept {}
  void onWriteReady(Ctx& /*ctx*/) noexcept {}
  void onException(Ctx& ctx, folly::exception_wrapper&& e) noexcept {
    ctx.fireException(std::move(e));
  }
  cp::Result onWrite(Ctx& ctx, cp::TypeErasedBox&& msg) noexcept {
    return ctx.fireWrite(std::move(msg));
  }

  cp::Result onRead(Ctx& ctx, cp::TypeErasedBox&& msg) noexcept {
    auto& m = msg.get<ControlMessage>();
    if (m.payload.is<RequestN>()) {
      const uint64_t n = m.payload.get<RequestN>().n;
      for (uint64_t i = 0; i < n && remaining_ > 0; ++i) {
        (void)ctx.fireWrite(
            cp::erase_and_box(
                DataMessage{.payload = Payload{.data = nullptr}}));
        --remaining_;
      }
      if (remaining_ == 0 && !completed_) {
        completed_ = true;
        (void)ctx.fireWrite(
            cp::erase_and_box(DataMessage{.payload = Complete{}}));
      }
      return cp::Result::Success;
    }
    return ctx.fireRead(std::move(msg));
  }

 private:
  uint64_t remaining_;
  bool completed_{false};
};

// Records the terminal messages the head endpoint hands to the sink. Wired via
// a sink that captures the recorder by reference, mirroring how the owning
// thrift-pipeline handler captures its own instance.
struct SinkRecorder {
  std::vector<DataMessage> msgs;

  cp::Result record(DataMessage&& msg) noexcept {
    msgs.push_back(std::move(msg));
    return cp::Result::Success;
  }
};

StreamSink recorderSink(SinkRecorder& recorder) {
  return StreamSink{[&recorder](DataMessage&& msg) noexcept {
    return recorder.record(std::move(msg));
  }};
}

HANDLER_TAG(producer);

// Builds a producing-end sub-pipeline with a FiniteProducer composed in,
// exactly as the framework would: construct the Builder with the executor and
// output sink, add the application's producer, and build.
ProducerPipeline makeProducerPipeline(
    folly::EventBase& evb, uint64_t total, StreamSink sink) {
  return ProducerPipeline::build(&evb, std::move(sink), [total](auto builder) {
    return std::move(builder).template addNextDuplexTemplate<FiniteProducer>(
        producer_tag, total);
  });
}

} // namespace

TEST(ProducerPipelineTest, DemandDrivesProducerOutputToInjectedSink) {
  folly::EventBase evb;
  SinkRecorder recorder;
  auto pipeline =
      makeProducerPipeline(evb, /*total=*/3, recorderSink(recorder));

  // Grant more credit than there are items: the producer emits all 3 payloads
  // then completes, and every message exits through the injected sink.
  (void)pipeline.fireRead(cp::erase_and_box(makeRequestN(10)));

  ASSERT_EQ(recorder.msgs.size(), 4u);
  EXPECT_TRUE(recorder.msgs[0].payload.is<Payload>());
  EXPECT_TRUE(recorder.msgs[1].payload.is<Payload>());
  EXPECT_TRUE(recorder.msgs[2].payload.is<Payload>());
  EXPECT_TRUE(recorder.msgs[3].payload.is<Complete>());
}

TEST(ProducerPipelineTest, DemandBoundsProducerOutput) {
  folly::EventBase evb;
  SinkRecorder recorder;
  auto pipeline =
      makeProducerPipeline(evb, /*total=*/5, recorderSink(recorder));

  // Grant less credit than there are items: only the requested count is emitted
  // and the stream does not complete.
  (void)pipeline.fireRead(cp::erase_and_box(makeRequestN(2)));

  ASSERT_EQ(recorder.msgs.size(), 2u);
  EXPECT_TRUE(recorder.msgs[0].payload.is<Payload>());
  EXPECT_TRUE(recorder.msgs[1].payload.is<Payload>());
}

} // namespace apache::thrift::fast_thrift::thrift::stream
