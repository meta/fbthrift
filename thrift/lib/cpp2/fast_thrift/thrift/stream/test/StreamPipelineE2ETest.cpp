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

// End-to-end tests for the established-stream outbound sub-pipeline assembled
// through ProducerPipeline::build:
//
//   sink (wire) <- ProducerHeadAdapter <- InboundCreditHandler <-
//   PayloadPrefetchHandler <- <sample producer> <- ProducerTailAdapter (tail)
//
// The Builder owns the fixed boundary endpoints (ProducerHeadAdapter and
// ProducerTailAdapter) and the sub-pipeline's allocator; the credit/prefetch
// handlers and the sample producer are what an application composes between
// them. Consumer demand is injected as an inbound RequestN/Cancel at the head;
// the payloads/terminals the producer emits are observed as they exit through
// the injected sink. The producer is supplied by the test (the library ships no
// producer handler); these fixtures stand in for real application producers.

#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include <folly/ExceptionWrapper.h>
#include <folly/io/Cursor.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/EventBase.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/HandlerTag.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/stream/ProducerPipeline.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/stream/common/Messages.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/stream/handler/InboundCreditHandler.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/stream/handler/PayloadPrefetchHandler.h>

namespace apache::thrift::fast_thrift::thrift::stream {

namespace cp = channel_pipeline;

namespace {

DataMessage makeItem(uint8_t tag) {
  return DataMessage{
      .payload = Payload{.data = folly::IOBuf::copyBuffer(&tag, sizeof(tag))}};
}

ControlMessage makeRequestN(uint64_t n) {
  return ControlMessage{.payload = RequestN{.n = n}};
}

ControlMessage makeCancel() {
  return ControlMessage{.payload = Cancel{}};
}

// Shared boilerplate for the sample producers: the handler-concept lifecycle
// no-ops plus the pass-through onException/onWrite. Each producer supplies only
// its own onRead logic and state. No virtuals — these are plugged in by value.
template <typename Ctx>
struct ProducerBase {
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
};

// A terminal producer that emits a fixed number of payloads (tags 1..total)
// then Complete, producing only as much as the demand it is handed.
template <typename Ctx>
class FiniteProducer : public ProducerBase<Ctx> {
 public:
  explicit FiniteProducer(uint64_t total) noexcept : remaining_(total) {}

  cp::Result onRead(Ctx& ctx, cp::TypeErasedBox&& msg) noexcept {
    auto& m = msg.get<ControlMessage>();
    if (m.payload.is<RequestN>()) {
      const uint64_t n = m.payload.get<RequestN>().n;
      for (uint64_t i = 0; i < n && remaining_ > 0; ++i) {
        (void)ctx.fireWrite(cp::erase_and_box(makeItem(nextTag_++)));
        --remaining_;
      }
      if (remaining_ == 0 && !completed_) {
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
  uint64_t remaining_;
  uint8_t nextTag_{1};
  bool completed_{false};
};

// A terminal producer with unbounded content: it emits exactly the demand it is
// handed and never completes, stopping only on Cancel.
template <typename Ctx>
class InfiniteProducer : public ProducerBase<Ctx> {
 public:
  cp::Result onRead(Ctx& ctx, cp::TypeErasedBox&& msg) noexcept {
    auto& m = msg.get<ControlMessage>();
    if (m.payload.is<RequestN>()) {
      if (!cancelled_) {
        const uint64_t n = m.payload.get<RequestN>().n;
        for (uint64_t i = 0; i < n; ++i) {
          (void)ctx.fireWrite(cp::erase_and_box(makeItem(nextTag_++)));
        }
      }
      return cp::Result::Success;
    }
    if (m.payload.is<Cancel>()) {
      cancelled_ = true;
      return cp::Result::Success;
    }
    return ctx.fireRead(std::move(msg));
  }

 private:
  uint8_t nextTag_{1};
  bool cancelled_{false};
};

// A terminal producer that emits `before` payloads then fails the stream.
template <typename Ctx>
class ErrorProducer : public ProducerBase<Ctx> {
 public:
  explicit ErrorProducer(uint64_t before) noexcept : before_(before) {}

  cp::Result onRead(Ctx& ctx, cp::TypeErasedBox&& msg) noexcept {
    auto& m = msg.get<ControlMessage>();
    if (m.payload.is<RequestN>()) {
      if (!failed_) {
        const uint64_t n = m.payload.get<RequestN>().n;
        for (uint64_t i = 0; i < n && emitted_ < before_; ++i) {
          (void)ctx.fireWrite(cp::erase_and_box(makeItem(nextTag_++)));
          ++emitted_;
        }
        if (emitted_ == before_) {
          failed_ = true;
          (void)ctx.fireWrite(
              cp::erase_and_box(
                  DataMessage{
                      .payload = Error{
                          .ex =
                              folly::make_exception_wrapper<std::runtime_error>(
                                  "producer failed")}}));
        }
      }
      return cp::Result::Success;
    }
    if (m.payload.is<Cancel>()) {
      return cp::Result::Success;
    }
    return ctx.fireRead(std::move(msg));
  }

 private:
  uint64_t before_;
  uint64_t emitted_{0};
  uint8_t nextTag_{1};
  bool failed_{false};
};

HANDLER_TAG(credit);
HANDLER_TAG(prefetch);
HANDLER_TAG(producer);

// What reached the wire (the injected sink), in order.
struct WireSink {
  std::vector<uint8_t> tags;
  size_t completions{0};
  size_t errors{0};

  cp::Result record(DataMessage&& m) noexcept {
    if (m.payload.is<Payload>()) {
      if (auto& p = m.payload.get<Payload>(); p.data) {
        tags.push_back(folly::io::Cursor(p.data.get()).read<uint8_t>());
      }
    } else if (m.payload.is<Complete>()) {
      ++completions;
    } else if (m.payload.is<Error>()) {
      ++errors;
    }
    return cp::Result::Success;
  }
};

class StreamPipelineE2ETest : public ::testing::Test {
 protected:
  // Assemble the sub-pipeline through ProducerPipeline::build:
  //   HeadAdapter <- Credit <- Prefetch <- Producer <- TailAdapter
  // and record everything that reaches the injected sink. The Builder owns the
  // endpoints and the sub-pipeline's allocator; the test only composes the
  // middle handlers between them.
  template <template <typename> class Producer, typename... Args>
  ProducerPipeline& build(PayloadPrefetchConfig cfg, Args&&... args) {
    pipeline_ = ProducerPipeline::build(
        &evb_,
        StreamSink{[this](DataMessage&& m) noexcept {
          return wire_.record(std::move(m));
        }},
        [&](auto builder) {
          return std::move(builder)
              .template addNextDuplexTemplate<InboundCreditHandler>(credit_tag)
              .template addNextDuplexTemplate<PayloadPrefetchHandler>(
                  prefetch_tag, cfg)
              .template addNextDuplexTemplate<Producer>(
                  producer_tag, std::forward<Args>(args)...);
        });
    return *pipeline_;
  }

  // Consumer grants credit / cancels by injecting an inbound frame at the head.
  void grant(ProducerPipeline& pipeline, uint64_t n) {
    (void)pipeline.fireRead(cp::erase_and_box(makeRequestN(n)));
  }
  void cancel(ProducerPipeline& pipeline) {
    (void)pipeline.fireRead(cp::erase_and_box(makeCancel()));
  }

  folly::EventBase evb_;
  WireSink wire_;
  std::optional<ProducerPipeline> pipeline_;
};

PayloadPrefetchConfig config() {
  return PayloadPrefetchConfig{.capacity = 8, .replenishThreshold = 4};
}

} // namespace

TEST_F(StreamPipelineE2ETest, FiniteStreamDeliversAllItemsThenCompletes) {
  auto& pipeline = build<FiniteProducer>(config(), 3);

  grant(pipeline, 10);

  const std::vector<uint8_t> expected{1, 2, 3};
  EXPECT_EQ(wire_.tags, expected);
  EXPECT_EQ(wire_.completions, 1u);
  EXPECT_EQ(wire_.errors, 0u);
}

TEST_F(StreamPipelineE2ETest, CreditBoundsDeliveryAcrossGrants) {
  // 10 items available, buffer capacity 8.
  auto& pipeline = build<FiniteProducer>(config(), 10);

  // First grant delivers only what was requested; the stream is not done.
  grant(pipeline, 3);
  const std::vector<uint8_t> afterFirst{1, 2, 3};
  EXPECT_EQ(wire_.tags, afterFirst);
  EXPECT_EQ(wire_.completions, 0u);

  // The rest is delivered as more credit arrives, ending with completion.
  grant(pipeline, 100);
  const std::vector<uint8_t> all{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  EXPECT_EQ(wire_.tags, all);
  EXPECT_EQ(wire_.completions, 1u);
}

TEST_F(StreamPipelineE2ETest, InfiniteStreamRespectsCreditThenStopsOnCancel) {
  auto& pipeline = build<InfiniteProducer>(config());

  grant(pipeline, 3);
  grant(pipeline, 2);
  const std::vector<uint8_t> delivered{1, 2, 3, 4, 5};
  EXPECT_EQ(wire_.tags, delivered);
  EXPECT_EQ(wire_.completions, 0u);

  // Cancel is forwarded to the producer (which stops) and latches the buffer:
  // nothing more is sent, even for a large later grant, so delivery stays
  // exactly where the cancel left it.
  cancel(pipeline);
  grant(pipeline, 1000);
  grant(pipeline, 1000);
  EXPECT_EQ(wire_.tags, delivered) << "nothing is delivered after a cancel";
  EXPECT_EQ(wire_.completions, 0u);
}

TEST_F(StreamPipelineE2ETest, ErrorStreamDeliversThenFails) {
  auto& pipeline = build<ErrorProducer>(config(), 2);

  grant(pipeline, 10);

  const std::vector<uint8_t> delivered{1, 2};
  EXPECT_EQ(wire_.tags, delivered);
  EXPECT_EQ(wire_.errors, 1u);
  EXPECT_EQ(wire_.completions, 0u);
}

} // namespace apache::thrift::fast_thrift::thrift::stream
