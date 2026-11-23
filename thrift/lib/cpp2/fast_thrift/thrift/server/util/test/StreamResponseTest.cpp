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

// Tests the runtime stream-open glue: makeStreamOpenMessage binds a
// hand-written StreamElementEncoder<int> (standing in for the codegen-supplied
// one) to an app-composed StreamFactory<int>, and the resulting
// ThriftServerStreamOpenPayload carries a ConfigFunc that, run through a
// ProducerPipeline, emits the encoded elements. A static_assert pins
// detail::writeStreamOpen to the FastHandlerCallback ResultFn shape codegen
// will target.

#include <thrift/lib/cpp2/fast_thrift/thrift/server/util/StreamResponse.h>

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
#include <thrift/lib/cpp2/fast_thrift/thrift/server/util/FastHandlerCallback.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/stream/ProducerPipeline.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/stream/StreamElementEncoder.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/stream/StreamFactory.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/stream/common/Messages.h>

namespace apache::thrift::fast_thrift::thrift {

namespace cp = channel_pipeline;

namespace {

// Hand-written encoder: an int is serialized as its 4 raw bytes.
stream::Payload encodeIntValue(int32_t&& value) noexcept {
  auto buf = folly::IOBuf::create(sizeof(int32_t));
  std::memcpy(buf->writableData(), &value, sizeof(int32_t));
  buf->append(sizeof(int32_t));
  return stream::Payload{.data = std::move(buf)};
}

stream::Error encodeIntError(folly::exception_wrapper&& ex) noexcept {
  return stream::Error{.ex = std::move(ex)};
}

// Hand-written initial-response encoder: a string response is serialized as its
// raw bytes (standing in for the codegen-supplied presult serializer). Distinct
// type from the stream element (int32_t) so the two encoding paths cannot be
// confused.
std::unique_ptr<folly::IOBuf> encodeStringResponse(
    std::string&& value) noexcept {
  return folly::IOBuf::copyBuffer(value);
}

int32_t decodeInt(const folly::IOBuf& buf) {
  int32_t value = 0;
  std::memcpy(&value, buf.data(), sizeof(int32_t));
  return value;
}

HANDLER_TAG(producer);

// Application producer written in terms of int: on RequestN it emits up to `n`
// encoded payloads (bounded by its backlog) then Complete, using the
// framework-supplied encoder.
template <typename Ctx>
class IntProducer {
 public:
  IntProducer(
      stream::StreamElementEncoder<int32_t> encoder,
      std::vector<int32_t> values)
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
    auto& m = msg.get<stream::ControlMessage>();
    if (m.payload.is<stream::RequestN>()) {
      const uint64_t n = m.payload.get<stream::RequestN>().n;
      for (uint64_t i = 0; i < n && next_ < values_.size(); ++i, ++next_) {
        (void)ctx.fireWrite(
            cp::erase_and_box(
                stream::DataMessage{
                    .payload = encoder_.encodeValue(int32_t{values_[next_]})}));
      }
      if (next_ == values_.size() && !completed_) {
        completed_ = true;
        (void)ctx.fireWrite(
            cp::erase_and_box(
                stream::DataMessage{.payload = stream::Complete{}}));
      }
      return cp::Result::Success;
    }
    if (m.payload.is<stream::Cancel>()) {
      return cp::Result::Success;
    }
    return ctx.fireRead(std::move(msg));
  }

 private:
  stream::StreamElementEncoder<int32_t> encoder_;
  std::vector<int32_t> values_;
  size_t next_{0};
  bool completed_{false};
};

stream::StreamFactory<int32_t> makeIntStreamFactory(
    std::vector<int32_t> values) {
  return stream::StreamFactory<int32_t>(
      [values = std::move(values)](
          auto builder,
          const stream::StreamElementEncoder<int32_t>& encoder) mutable {
        return std::move(builder).template addNextDuplexTemplate<IntProducer>(
            producer_tag, encoder, std::move(values));
      });
}

// detail::writeStreamOpen must match FastHandlerCallback's ResultFn shape so
// codegen can hand it to makeFastHandlerCallback.
static_assert(std::is_same_v<
              FastHandlerCallback<stream::StreamFactory<int32_t>>::ResultFn,
              decltype(&detail::writeStreamOpen<
                       int32_t,
                       &encodeIntValue,
                       &encodeIntError>)>);

// detail::writeResponseStreamOpen must likewise match the ResultFn shape for a
// ResponseAndStreamFactory-returning stream RPC.
static_assert(
    std::is_same_v<
        FastHandlerCallback<
            stream::ResponseAndStreamFactory<std::string, int32_t>>::ResultFn,
        decltype(&detail::writeResponseStreamOpen<
                 std::string,
                 int32_t,
                 &encodeStringResponse,
                 &encodeIntValue,
                 &encodeIntError>)>);

} // namespace

TEST(StreamResponseTest, MakeStreamOpenMessageBindsFactoryAndEncoder) {
  auto message = makeStreamOpenMessage<int32_t>(
      /*streamId=*/7,
      detail::makeEmptyPresultData(),
      makeIntStreamFactory({10, 20, 30}),
      stream::StreamElementEncoder<int32_t>{&encodeIntValue, &encodeIntError});

  ASSERT_TRUE(message.payload.is<ThriftServerStreamOpenPayload>());
  auto& open = message.payload.get<ThriftServerStreamOpenPayload>();
  EXPECT_EQ(open.initialResponse.streamId, 7u);
  ASSERT_NE(open.configFunc, nullptr);

  // The bound ConfigFunc composes the producing-end sub-pipeline; injected
  // demand drives the producer to emit the encoded elements then Complete.
  folly::EventBase evb;
  std::vector<int32_t> got;
  bool completed = false;
  auto pipeline = (*open.configFunc)(
      &evb,
      stream::StreamSink{[&](stream::DataMessage&& m) noexcept -> cp::Result {
        if (m.payload.is<stream::Payload>()) {
          got.push_back(decodeInt(*m.payload.get<stream::Payload>().data));
        } else if (m.payload.is<stream::Complete>()) {
          completed = true;
        }
        return cp::Result::Success;
      }});

  (void)pipeline.fireRead(
      cp::erase_and_box(
          stream::ControlMessage{.payload = stream::RequestN{.n = 10}}));

  EXPECT_EQ(got, (std::vector<int32_t>{10, 20, 30}));
  EXPECT_TRUE(completed);
}

TEST(StreamResponseTest, ResponseAndStreamFactoryCarriesInitialResponse) {
  // A `R, stream<T>` handler returns the initial response paired with the
  // stream factory; the framework encodes the response into the first PAYLOAD's
  // data (what writeResponseStreamOpen does before handing off to the adapter)
  // while binding the element encoder to the factory exactly as a bare stream.
  stream::ResponseAndStreamFactory<std::string, int32_t> value{
      .response = "hello", .factory = makeIntStreamFactory({10, 20, 30})};

  auto message = makeStreamOpenMessage<int32_t>(
      /*streamId=*/9,
      encodeStringResponse(std::move(value.response)),
      std::move(value.factory),
      stream::StreamElementEncoder<int32_t>{&encodeIntValue, &encodeIntError});

  ASSERT_TRUE(message.payload.is<ThriftServerStreamOpenPayload>());
  auto& open = message.payload.get<ThriftServerStreamOpenPayload>();
  EXPECT_EQ(open.initialResponse.streamId, 9u);
  // First PAYLOAD carries a real presult (the encoded response), not the
  // single empty-struct STOP byte a void-initial-response stream would emit.
  ASSERT_NE(open.initialResponse.data, nullptr);
  EXPECT_EQ(open.initialResponse.data->moveToFbString(), "hello");
  EXPECT_FALSE(open.initialResponse.complete);
  EXPECT_TRUE(open.initialResponse.next);
  ASSERT_NE(open.configFunc, nullptr);

  // The bound stream still produces its elements: the initial response does not
  // consume or reorder the stream's first chunk.
  folly::EventBase evb;
  std::vector<int32_t> got;
  bool completed = false;
  auto pipeline = (*open.configFunc)(
      &evb,
      stream::StreamSink{[&](stream::DataMessage&& m) noexcept -> cp::Result {
        if (m.payload.is<stream::Payload>()) {
          got.push_back(decodeInt(*m.payload.get<stream::Payload>().data));
        } else if (m.payload.is<stream::Complete>()) {
          completed = true;
        }
        return cp::Result::Success;
      }});

  (void)pipeline.fireRead(
      cp::erase_and_box(
          stream::ControlMessage{.payload = stream::RequestN{.n = 10}}));

  EXPECT_EQ(got, (std::vector<int32_t>{10, 20, 30}));
  EXPECT_TRUE(completed);
}

} // namespace apache::thrift::fast_thrift::thrift
