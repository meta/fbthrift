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

#include <chrono>
#include <memory>

#include <boost/intrusive_ptr.hpp>

#include <folly/io/async/EventBase.h>
#include <folly/portability/GTest.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/HandlerTag.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/StaticPipelineBuilder.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/test/MockAdapters.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/Messages.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/ReadBackpressureArbiter.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/ThriftConnContext.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/handler/ThriftServerBackpressureHandler.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/handler/ThriftServerConnectionCloseHandler.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/handler/ThriftServerRequestContextHandler.h>

namespace apache::thrift::fast_thrift::thrift::server::test {

namespace cp = ::apache::thrift::fast_thrift::channel_pipeline;
namespace ft = ::apache::thrift::fast_thrift::thrift;

namespace {

HANDLER_TAG(req_ctx);
HANDLER_TAG(close);
HANDLER_TAG(server_backpressure);

auto buildPipeline(
    folly::EventBase& evb,
    cp::test::StaticMockHeadHandler& head,
    cp::test::StaticMockTailHandler& tail,
    cp::test::TestAllocator& allocator,
    ft::ReadBackpressureArbiter& arbiter) {
  return cp::StaticPipelineBuilder<
             cp::test::StaticMockHeadHandler,
             cp::test::StaticMockTailHandler,
             cp::test::TestAllocator>()
      .setEventBase(&evb)
      .setHead(&head)
      .setTail(&tail)
      .setAllocator(&allocator)
      .addNextInboundTemplate<ft::ThriftServerRequestContextHandler>(
          req_ctx_tag,
          /*requestExtensionLayout=*/nullptr,
          /*methodMetadata=*/nullptr,
          boost::intrusive_ptr<ft::ThriftConnContext>(
              new ft::ThriftConnContext{}),
          /*enableRequestHeaders=*/false)
      .addNextDuplexTemplate<ft::ThriftServerConnectionCloseHandler>(close_tag)
      .addNextDuplexTemplate<ft::ThriftServerBackpressureHandler>(
          server_backpressure_tag, &arbiter)
      .build();
}

ft::ThriftServerRequestMessage makeRequest(uint32_t streamId) {
  return ft::ThriftServerRequestMessage{
      .requestContext = ft::ThriftRequestContextPtr{},
      // A request-opening payload so ThriftServerConnectionCloseHandler counts
      // it as in-flight work (isRequestWork admits only REQUEST_RESPONSE /
      // REQUEST_STREAM); otherwise the paired response underflows the count.
      .payload = ft::ThriftServerRequestResponsePayload{},
      .streamId = streamId,
  };
}

ft::ThriftServerResponseMessage makeResponse(uint32_t streamId) {
  return ft::ThriftServerResponseMessage{
      .payload = ft::ThriftInitialResponsePayload{
          .data = nullptr,
          .metadata = nullptr,
          .streamId = streamId,
      }};
}

} // namespace

class ThriftServerBackpressureIntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    head_.reset();
    tail_.reset();
    allocator_.reset();

    pipeline_ = buildPipeline(evb_, head_, tail_, allocator_, arbiter_);
    pipeline_->activate();
  }

  void TearDown() override {
    // Drain any scheduled work before the pipeline goes away.
    evb_.loopOnce(EVLOOP_NONBLOCK);
    pipeline_.reset();
  }

  // Fire N requests through the inbound path so CloseHandler.inFlight == N.
  // Each request is a ThriftServerRequestMessage (what ReqCtxHandler expects).
  void fireRequests(uint32_t count) {
    for (uint32_t i = 1; i <= count; ++i) {
      (void)pipeline_->fireRead(cp::erase_and_box(makeRequest(i)));
    }
  }

  // Fire a response from the tail outward toward the head. Pairs with a
  // prior fireRequests() call so CloseHandler's inFlight invariant holds.
  cp::Result fireResponse(uint32_t streamId) {
    return pipeline_->fireWrite(cp::erase_and_box(makeResponse(streamId)));
  }

  folly::EventBase evb_;
  cp::test::StaticMockHeadHandler head_;
  cp::test::StaticMockTailHandler tail_;
  cp::test::TestAllocator allocator_;
  ft::ReadBackpressureArbiter arbiter_;
  decltype(buildPipeline(evb_, head_, tail_, allocator_, arbiter_)) pipeline_;
};

TEST_F(
    ThriftServerBackpressureIntegrationTest,
    WritesPassThroughWhenHeadAcceptsAllWrites) {
  fireRequests(3);
  head_.setOnWriteCallback(
      [](cp::TypeErasedBox&&) noexcept { return cp::Result::Success; });

  EXPECT_EQ(fireResponse(1), cp::Result::Success);
  EXPECT_EQ(fireResponse(2), cp::Result::Success);
  EXPECT_EQ(fireResponse(3), cp::Result::Success);

  EXPECT_FALSE(pipeline_->handler(server_backpressure_tag).isBackpressured());
  EXPECT_EQ(head_.writeCount(), 3);
  EXPECT_EQ(pipeline_->handler(close_tag).inFlight(), 0u);
}

TEST_F(
    ThriftServerBackpressureIntegrationTest,
    ResponsesRetireRequestsWhileReadsAreBackpressured) {
  fireRequests(3);
  head_.setOnWriteCallback(
      [](cp::TypeErasedBox&&) noexcept { return cp::Result::Backpressure; });

  EXPECT_EQ(fireResponse(1), cp::Result::Success);
  EXPECT_EQ(fireResponse(2), cp::Result::Success);
  EXPECT_EQ(fireResponse(3), cp::Result::Success);

  EXPECT_TRUE(pipeline_->handler(server_backpressure_tag).isBackpressured());
  EXPECT_EQ(head_.writeCount(), 3);
  EXPECT_EQ(pipeline_->handler(close_tag).inFlight(), 0u);
  EXPECT_EQ(arbiter_.holderCount(), 1u);
  EXPECT_TRUE(pipeline_->hasPendingWriteReady());
  EXPECT_EQ(head_.onReadReadyCount(), 0);

  pipeline_->onWriteReady();

  EXPECT_FALSE(pipeline_->handler(server_backpressure_tag).isBackpressured());
  EXPECT_FALSE(pipeline_->hasPendingWriteReady());
  EXPECT_EQ(head_.writeCount(), 3);
  EXPECT_EQ(head_.onReadReadyCount(), 1);
  EXPECT_EQ(arbiter_.holderCount(), 0u);
}

TEST_F(
    ThriftServerBackpressureIntegrationTest,
    InboundReadSurfacesBackpressureThroughEntireChain) {
  fireRequests(1);
  head_.setOnWriteCallback(
      [](cp::TypeErasedBox&&) noexcept { return cp::Result::Backpressure; });
  (void)fireResponse(1);

  EXPECT_EQ(
      pipeline_->fireRead(cp::erase_and_box(makeRequest(99))),
      cp::Result::Backpressure);
  EXPECT_EQ(tail_.readCount(), 2);
  EXPECT_EQ(fireResponse(99), cp::Result::Success);
  EXPECT_EQ(pipeline_->handler(close_tag).inFlight(), 0u);
  EXPECT_EQ(head_.writeCount(), 2);
}

} // namespace apache::thrift::fast_thrift::thrift::server::test
