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

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineBuilder.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineRef.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/test/MockAdapters.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/test/MockHandler.h>

#include <memory>

#include <folly/io/async/EventBase.h>
#include <folly/portability/GTest.h>

namespace apache::thrift::fast_thrift::channel_pipeline::test {
namespace {

HANDLER_TAG(counter);

// Small inline-eligible seam message.
struct Ping {
  int value{0};
};

// A tail endpoint that propagates inbound past itself via ctx.fireRead — the
// point binding hooks. Standalone it has no downstream; bound, the read crosses
// into the peer.
class ForwardingTail {
 public:
  Result onRead(detail::ContextImpl& ctx, TypeErasedBox&& msg) noexcept {
    ++onReadCount_;
    return ctx.fireRead(std::move(msg));
  }
  void onException(folly::exception_wrapper&&) noexcept {}
  void onWriteReady() noexcept {}
  void handlerAdded() noexcept {}
  void handlerRemoved() noexcept {}
  void onPipelineActive() noexcept {}
  void onPipelineInactive() noexcept {}

  int onReadCount() const { return onReadCount_; }

 private:
  int onReadCount_{0};
};

// The outbound mirror: a head endpoint that propagates past itself via
// ctx.fireWrite.
class ForwardingHead {
 public:
  Result onWrite(detail::ContextImpl& ctx, TypeErasedBox&& msg) noexcept {
    ++onWriteCount_;
    return ctx.fireWrite(std::move(msg));
  }
  void onReadReady() noexcept {}
  void handlerAdded() noexcept {}
  void handlerRemoved() noexcept {}
  void onPipelineActive() noexcept {}
  void onPipelineInactive() noexcept {}

  int onWriteCount() const { return onWriteCount_; }

 private:
  int onWriteCount_{0};
};

// Seam adapters that convert the message as it crosses, standing in for a real
// boundary transform. The composer supplies these; neither pipeline knows about
// the conversion.
struct DoublingReadAdapter {
  static TypeErasedBox adaptRead(TypeErasedBox&& msg) noexcept {
    return erase_and_box(Ping{msg.get<Ping>().value * 2});
  }
};

struct DoublingWriteAdapter {
  static TypeErasedBox adaptWrite(TypeErasedBox&& msg) noexcept {
    return erase_and_box(Ping{msg.get<Ping>().value * 2});
  }
};

} // namespace

// A pipeline whose tail propagates past itself with nothing bound has no
// downstream, so the past-tail read reports an error.
TEST(PipelineBindTest, UnboundTailForwardReturnsError) {
  folly::EventBase evb;
  SimpleBufferAllocator allocator;
  MockHeadHandler head;
  ForwardingTail tail;
  auto source = PipelineBuilder<MockHeadHandler, ForwardingTail>()
                    .setEventBase(&evb)
                    .setHead(&head)
                    .setTail(&tail)
                    .setAllocator(&allocator)
                    .build();

  EXPECT_EQ(source->fireRead(erase_and_box(Ping{1})), Result::Error);
  EXPECT_EQ(tail.onReadCount(), 1); // the tail still ran
}

// Once a peer is bound, the tail's past-tail read crosses into the peer, which
// delivers it to the peer's own tail; the peer's Result propagates back.
TEST(PipelineBindTest, BindTailForwardsPastTailToPeer) {
  folly::EventBase evb;
  SimpleBufferAllocator allocator;

  MockHeadHandler peerHead;
  MockTailHandler peerTail;
  int received = 0;
  peerTail.setOnReadCallback([&](TypeErasedBox&&) noexcept {
    ++received;
    return Result::Success;
  });
  auto peer = PipelineBuilder<MockHeadHandler, MockTailHandler>()
                  .setEventBase(&evb)
                  .setHead(&peerHead)
                  .setTail(&peerTail)
                  .setAllocator(&allocator)
                  .build();

  MockHeadHandler head;
  ForwardingTail tail;
  auto source = PipelineBuilder<MockHeadHandler, ForwardingTail>()
                    .setEventBase(&evb)
                    .setHead(&head)
                    .setTail(&tail)
                    .setAllocator(&allocator)
                    .build();
  source->adaptTail(PipelineRef(*peer));

  EXPECT_EQ(source->fireRead(erase_and_box(Ping{1})), Result::Success);
  EXPECT_EQ(received, 1);
}

// Binding changes nothing about how the pipeline's handlers and tail run — only
// where the tail's past-tail read goes. The same source run unbound then bound
// executes its handler and tail identically both times; only the destination of
// the past-tail read differs (error vs. the peer).
TEST(PipelineBindTest, HandlersRunRegardlessOfBind) {
  folly::EventBase evb;
  SimpleBufferAllocator allocator;

  MockHeadHandler peerHead;
  MockTailHandler peerTail;
  int received = 0;
  peerTail.setOnReadCallback([&](TypeErasedBox&&) noexcept {
    ++received;
    return Result::Success;
  });
  auto peer = PipelineBuilder<MockHeadHandler, MockTailHandler>()
                  .setEventBase(&evb)
                  .setHead(&peerHead)
                  .setTail(&peerTail)
                  .setAllocator(&allocator)
                  .build();

  MockHeadHandler head;
  ForwardingTail tail;
  auto handler = std::make_unique<MockHandler>();
  auto* handlerPtr = handler.get();
  auto source = PipelineBuilder<MockHeadHandler, ForwardingTail>()
                    .setEventBase(&evb)
                    .setHead(&head)
                    .setTail(&tail)
                    .setAllocator(&allocator)
                    .addNextDuplex(counter_tag, std::move(handler))
                    .build();

  // Unbound: handler and tail run; the past-tail read has nowhere to go.
  EXPECT_EQ(source->fireRead(erase_and_box(Ping{1})), Result::Error);
  EXPECT_EQ(handlerPtr->readCount(), 1);
  EXPECT_EQ(tail.onReadCount(), 1);
  EXPECT_EQ(received, 0);

  // Bound: handler and tail run identically; only the past-tail read now
  // crosses into the peer.
  source->adaptTail(PipelineRef(*peer));
  EXPECT_EQ(source->fireRead(erase_and_box(Ping{2})), Result::Success);
  EXPECT_EQ(handlerPtr->readCount(), 2);
  EXPECT_EQ(tail.onReadCount(), 2);
  EXPECT_EQ(received, 1);
}

// Outbound mirror of UnboundTailForwardReturnsError: a head that propagates
// past itself with nothing bound has no downstream, so the past-head write
// errors.
TEST(PipelineBindTest, UnboundHeadForwardReturnsError) {
  folly::EventBase evb;
  SimpleBufferAllocator allocator;
  ForwardingHead head;
  MockTailHandler tail;
  auto source = PipelineBuilder<ForwardingHead, MockTailHandler>()
                    .setEventBase(&evb)
                    .setHead(&head)
                    .setTail(&tail)
                    .setAllocator(&allocator)
                    .build();

  EXPECT_EQ(source->fireWrite(erase_and_box(Ping{1})), Result::Error);
  EXPECT_EQ(head.onWriteCount(), 1); // the head still ran
}

// The outbound mirror: adaptHead routes the head endpoint's past-head write
// into the peer.
TEST(PipelineBindTest, BindHeadForwardsPastHeadToPeer) {
  folly::EventBase evb;
  SimpleBufferAllocator allocator;

  MockHeadHandler peerHead;
  MockTailHandler peerTail;
  int received = 0;
  peerHead.setOnWriteCallback([&](TypeErasedBox&&) noexcept {
    ++received;
    return Result::Success;
  });
  auto peer = PipelineBuilder<MockHeadHandler, MockTailHandler>()
                  .setEventBase(&evb)
                  .setHead(&peerHead)
                  .setTail(&peerTail)
                  .setAllocator(&allocator)
                  .build();

  ForwardingHead head;
  MockTailHandler tail;
  auto source = PipelineBuilder<ForwardingHead, MockTailHandler>()
                    .setEventBase(&evb)
                    .setHead(&head)
                    .setTail(&tail)
                    .setAllocator(&allocator)
                    .build();
  source->adaptHead(PipelineRef(*peer));

  EXPECT_EQ(source->fireWrite(erase_and_box(Ping{2})), Result::Success);
  EXPECT_EQ(received, 1);
}

// The owning overload takes ownership of the peer: it is torn down together
// with the owner, not before.
TEST(PipelineBindTest, OwningBindDestroysPeerWithOwner) {
  folly::EventBase evb;
  SimpleBufferAllocator allocator;

  MockHeadHandler peerHead;
  MockTailHandler peerTail;
  auto peer = PipelineBuilder<MockHeadHandler, MockTailHandler>()
                  .setEventBase(&evb)
                  .setHead(&peerHead)
                  .setTail(&peerTail)
                  .setAllocator(&allocator)
                  .build();

  MockHeadHandler head;
  ForwardingTail tail;
  auto source = PipelineBuilder<MockHeadHandler, ForwardingTail>()
                    .setEventBase(&evb)
                    .setHead(&head)
                    .setTail(&tail)
                    .setAllocator(&allocator)
                    .build();
  source->adaptTail(PipelineOwner(std::move(peer)));

  EXPECT_EQ(peerTail.handlerRemovedCount(), 0);
  source.reset();
  EXPECT_EQ(peerTail.handlerRemovedCount(), 1);
}

// Linked pipelines must share an EventBase so a crossing is a direct call.
TEST(PipelineBindDeathTest, BindTailRejectsCrossEventBase) {
  folly::EventBase evbSource;
  folly::EventBase evbPeer;
  SimpleBufferAllocator allocator;

  MockHeadHandler peerHead;
  MockTailHandler peerTail;
  auto peer = PipelineBuilder<MockHeadHandler, MockTailHandler>()
                  .setEventBase(&evbPeer)
                  .setHead(&peerHead)
                  .setTail(&peerTail)
                  .setAllocator(&allocator)
                  .build();

  MockHeadHandler head;
  ForwardingTail tail;
  auto source = PipelineBuilder<MockHeadHandler, ForwardingTail>()
                    .setEventBase(&evbSource)
                    .setHead(&head)
                    .setTail(&tail)
                    .setAllocator(&allocator)
                    .build();

  EXPECT_DEATH(source->adaptTail(PipelineRef(*peer)), "EventBase");
}

// adaptTail can carry a composer-supplied adapter that converts the past-tail
// message as it crosses; the peer's tail receives the converted value.
TEST(PipelineBindTest, BindTailAppliesAdapterConversion) {
  folly::EventBase evb;
  SimpleBufferAllocator allocator;

  MockHeadHandler peerHead;
  MockTailHandler peerTail;
  int received = 0;
  peerTail.setOnReadCallback([&](TypeErasedBox&& msg) noexcept {
    received = msg.get<Ping>().value;
    return Result::Success;
  });
  auto peer = PipelineBuilder<MockHeadHandler, MockTailHandler>()
                  .setEventBase(&evb)
                  .setHead(&peerHead)
                  .setTail(&peerTail)
                  .setAllocator(&allocator)
                  .build();

  MockHeadHandler head;
  ForwardingTail tail;
  auto source = PipelineBuilder<MockHeadHandler, ForwardingTail>()
                    .setEventBase(&evb)
                    .setHead(&head)
                    .setTail(&tail)
                    .setAllocator(&allocator)
                    .build();
  source->adaptTail<DoublingReadAdapter>(PipelineRef(*peer));

  EXPECT_EQ(source->fireRead(erase_and_box(Ping{21})), Result::Success);
  EXPECT_EQ(received, 42);
}

// The outbound mirror: a head bind's adapter converts the past-head write.
TEST(PipelineBindTest, BindHeadAppliesAdapterConversion) {
  folly::EventBase evb;
  SimpleBufferAllocator allocator;

  MockHeadHandler peerHead;
  MockTailHandler peerTail;
  int received = 0;
  peerHead.setOnWriteCallback([&](TypeErasedBox&& msg) noexcept {
    received = msg.get<Ping>().value;
    return Result::Success;
  });
  auto peer = PipelineBuilder<MockHeadHandler, MockTailHandler>()
                  .setEventBase(&evb)
                  .setHead(&peerHead)
                  .setTail(&peerTail)
                  .setAllocator(&allocator)
                  .build();

  ForwardingHead head;
  MockTailHandler tail;
  auto source = PipelineBuilder<ForwardingHead, MockTailHandler>()
                    .setEventBase(&evb)
                    .setHead(&head)
                    .setTail(&tail)
                    .setAllocator(&allocator)
                    .build();
  source->adaptHead<DoublingWriteAdapter>(PipelineRef(*peer));

  EXPECT_EQ(source->fireWrite(erase_and_box(Ping{21})), Result::Success);
  EXPECT_EQ(received, 42);
}

} // namespace apache::thrift::fast_thrift::channel_pipeline::test
