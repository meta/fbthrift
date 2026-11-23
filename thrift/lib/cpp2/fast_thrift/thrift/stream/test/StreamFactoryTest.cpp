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

// Fixture test for StreamFactory<T>: a hand-written StreamElementEncoder<int>
// stands in for the codegen-supplied encoder. The application composes an
// IntProducer via a StreamFactory<int>; the framework binds the encoder to get
// a ProducerPipeline::ConfigFunc, builds the producing-end sub-pipeline,
// injects demand, and observes the encoded chunks leaving through the head's
// sink.

#include <thrift/lib/cpp2/fast_thrift/thrift/stream/StreamFactory.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include <folly/ExceptionWrapper.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/EventBase.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/HandlerTag.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/stream/ProducerPipeline.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/stream/StreamElementEncoder.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/stream/common/Messages.h>

namespace apache::thrift::fast_thrift::thrift::stream {

namespace cp = channel_pipeline;

namespace {

// Hand-written encoder: an int is serialized as its 4 raw bytes.
Payload encodeIntValue(int32_t&& value) noexcept {
  auto buf = folly::IOBuf::create(sizeof(int32_t));
  std::memcpy(buf->writableData(), &value, sizeof(int32_t));
  buf->append(sizeof(int32_t));
  return Payload{.data = std::move(buf)};
}

Error encodeIntError(folly::exception_wrapper&& ex) noexcept {
  return Error{.ex = std::move(ex)};
}

int32_t decodeInt(const folly::IOBuf& buf) {
  int32_t value = 0;
  std::memcpy(&value, buf.data(), sizeof(int32_t));
  return value;
}

HANDLER_TAG(producer);

// Application producer written in terms of int: on each RequestN it emits up to
// `n` encoded payloads (bounded by its backlog) then a single Complete. It uses
// the framework-supplied encoder, never naming the wire protocol.
template <typename Ctx>
class IntProducer {
 public:
  IntProducer(
      StreamElementEncoder<int32_t> encoder, std::vector<int32_t> values)
      : encoder_(encoder), values_(std::move(values)) {}

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
      for (uint64_t i = 0; i < n && next_ < values_.size(); ++i, ++next_) {
        (void)ctx.fireWrite(
            cp::erase_and_box(
                DataMessage{
                    .payload = encoder_.encodeValue(int32_t{values_[next_]})}));
      }
      if (next_ == values_.size() && !completed_) {
        completed_ = true;
        (void)ctx.fireWrite(
            cp::erase_and_box(DataMessage{.payload = Complete{}}));
      }
      return cp::Result::Success;
    }
    if (m.payload.is<Cancel>()) {
      return cp::Result::Success;
    }
    return ctx.fireRead(std::move(msg));
  }

 private:
  StreamElementEncoder<int32_t> encoder_;
  std::vector<int32_t> values_;
  size_t next_{0};
  bool completed_{false};
};

// A recorded chunk that left the sub-pipeline through the head's sink.
struct Recorded {
  enum class Kind { Payload, Complete, Error, Other };
  Kind kind;
  int32_t value{0};
};

// Builds a StreamFactory<int> whose producer emits `values`.
StreamFactory<int32_t> makeIntStreamFactory(std::vector<int32_t> values) {
  return StreamFactory<int32_t>(
      [values = std::move(values)](
          auto builder, const StreamElementEncoder<int32_t>& encoder) mutable {
        return std::move(builder).template addNextDuplexTemplate<IntProducer>(
            producer_tag, encoder, std::move(values));
      });
}

} // namespace

TEST(StreamElementEncoderTest, DefaultConstructedPointersAreNull) {
  StreamElementEncoder<int32_t> enc;
  EXPECT_EQ(enc.encodeValue, nullptr);
  EXPECT_EQ(enc.encodeError, nullptr);
}

TEST(StreamFactoryTest, BindEncodesElementsThroughSink) {
  folly::EventBase evb;
  std::vector<Recorded> recorded;

  // Framework side: bind the codegen-shaped encoder to the app's factory.
  auto configFunc =
      makeIntStreamFactory({10, 20, 30})
          .bind(
              StreamElementEncoder<int32_t>{&encodeIntValue, &encodeIntError});

  auto pipeline =
      configFunc(&evb, StreamSink{[&](DataMessage&& m) noexcept -> cp::Result {
        if (m.payload.is<Payload>()) {
          recorded.push_back(
              {Recorded::Kind::Payload,
               decodeInt(*m.payload.get<Payload>().data)});
        } else if (m.payload.is<Complete>()) {
          recorded.push_back({Recorded::Kind::Complete});
        } else if (m.payload.is<Error>()) {
          recorded.push_back({Recorded::Kind::Error});
        } else {
          recorded.push_back({Recorded::Kind::Other});
        }
        return cp::Result::Success;
      }});

  (void)pipeline.fireRead(
      cp::erase_and_box(ControlMessage{.payload = RequestN{.n = 10}}));

  ASSERT_EQ(recorded.size(), 4u);
  EXPECT_EQ(recorded[0].kind, Recorded::Kind::Payload);
  EXPECT_EQ(recorded[0].value, 10);
  EXPECT_EQ(recorded[1].value, 20);
  EXPECT_EQ(recorded[2].value, 30);
  EXPECT_EQ(recorded[3].kind, Recorded::Kind::Complete);
}

} // namespace apache::thrift::fast_thrift::thrift::stream
