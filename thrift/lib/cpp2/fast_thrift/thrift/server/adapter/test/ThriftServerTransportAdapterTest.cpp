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

/**
 * Unit tests for ThriftServerTransportAdapter lifecycle propagation.
 *
 * The bridge sits between the rocket pipeline (it subscribes to the rocket
 * app adapter's lifecycle) and the thrift pipeline (it owns the rocket
 * connection bundle). These tests validate lifecycle propagation in both
 * directions and that the connected_ guard absorbs the reentry that arises
 * when one direction triggers the other.
 */

#include <gtest/gtest.h>

#include <folly/io/async/EventBase.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/EndpointAdapter.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineRef.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/StaticPipelineBuilder.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/test/MockAdapters.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/server/Event.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/server/adapter/RocketServerAppAdapter.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/server/common/RocketServerConnection.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/adapter/ThriftServerTransportAdapter.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/Event.h>

namespace apache::thrift::fast_thrift::thrift::server::test {

namespace transport = apache::thrift::fast_thrift::transport;

using channel_pipeline::PipelineOwner;
using channel_pipeline::Result;
using channel_pipeline::StaticPipelineBuilder;
using channel_pipeline::TypeErasedBox;
using channel_pipeline::test::StaticMockHeadHandler;
using channel_pipeline::test::StaticMockTailHandler;
using channel_pipeline::test::TestAllocator;

static_assert(
    channel_pipeline::HeadEndpointHandler<ThriftServerTransportAdapter>,
    "ThriftServerTransportAdapter must satisfy HeadEndpointHandler");

namespace {

// Wires two pipelines together via the bridge, matching the production
// server topology:
//
//   Rocket pipeline (head → tail):
//       StaticMockHeadHandler → ... → RocketServerAppAdapter
//
//   Thrift pipeline (head → tail):
//       ThriftServerTransportAdapter (bridge) → ... → StaticMockTailHandler
//
// The bridge takes ownership of the rocket connection bundle. The rocket
// adapter inside that bundle is the tail of the rocket pipeline; the
// fixture keeps a raw pointer to it for activation/deactivation.
struct BridgeFixture {
  folly::EventBase evb;
  StaticMockHeadHandler rocketHead;
  TestAllocator rocketAllocator;
  TestAllocator thriftAllocator;
  // thriftTail must outlive the thrift pipeline; declare before the bridge
  // so it stays alive across teardown.
  StaticMockTailHandler thriftTail;
  rocket::server::RocketServerAppAdapter* rocketAdapter{nullptr};
  rocket::server::RocketServerConnection* rocketConnRaw{nullptr};
  std::unique_ptr<ThriftServerTransportAdapter> bridge;
  // Non-owning — the actual rocket pipeline lives inside the bundle that
  // the bridge owns (rocketConnRaw->pipeline).
  channel_pipeline::PipelineRef rocketPipeline;
  PipelineOwner thriftPipeline;

  BridgeFixture() {
    rocketHead.setOnWriteCallback(
        [](TypeErasedBox&&) { return Result::Success; });

    auto rocketConn =
        std::make_unique<rocket::server::RocketServerConnection>();
    rocketAdapter = rocketConn->appAdapter.get();
    rocketConnRaw = rocketConn.get();

    auto rocketPipelinePtr = StaticPipelineBuilder<
                                 StaticMockHeadHandler,
                                 rocket::server::RocketServerAppAdapter,
                                 TestAllocator>()
                                 .setEventBase(&evb)
                                 .setHead(&rocketHead)
                                 .setTail(rocketAdapter)
                                 .setAllocator(&rocketAllocator)
                                 .build();
    rocketAdapter->setPipeline(rocketPipelinePtr.get());
    rocketPipeline = channel_pipeline::PipelineRef(*rocketPipelinePtr);
    rocketConn->pipeline = std::move(rocketPipelinePtr);

    bridge =
        std::make_unique<ThriftServerTransportAdapter>(std::move(rocketConn));

    thriftPipeline = StaticPipelineBuilder<
                         ThriftServerTransportAdapter,
                         StaticMockTailHandler,
                         TestAllocator>()
                         .setEventBase(&evb)
                         .setHead(bridge.get())
                         .setTail(&thriftTail)
                         .setAllocator(&thriftAllocator)
                         .build();
    bridge->setPipeline(thriftPipeline.get());
  }

  ~BridgeFixture() {
    if (thriftPipeline) {
      thriftPipeline->deactivate();
      thriftPipeline->close();
    }
    bridge.reset();
  }
};

} // namespace

TEST(
    ThriftServerTransportAdapterTest,
    RocketActivePropagatesToThriftPipelineActivate) {
  BridgeFixture fixture;

  EXPECT_EQ(fixture.thriftTail.pipelineActiveCount(), 0);

  // Activate the rocket pipeline. The rocket adapter fires onConnect_ on
  // its lifecycle subscription, which the bridge translates into thrift
  // pipeline activate.
  fixture.rocketPipeline.activate();

  EXPECT_EQ(fixture.thriftTail.pipelineActiveCount(), 1);

  // Re-activating rocket must NOT re-fire the thrift active path
  // because of the bridge's connected_ guard.
  fixture.rocketPipeline.activate();
  EXPECT_EQ(fixture.thriftTail.pipelineActiveCount(), 1);
}

TEST(
    ThriftServerTransportAdapterTest,
    RocketInactivePropagatesToThriftPipelineDeactivate) {
  BridgeFixture fixture;
  fixture.rocketPipeline.activate();
  EXPECT_EQ(fixture.thriftTail.pipelineActiveCount(), 1);
  EXPECT_EQ(fixture.thriftTail.pipelineInactiveCount(), 0);

  // Rocket side goes inactive (socket closed). The bridge translates
  // this into thrift pipeline deactivate.
  fixture.rocketPipeline.deactivate();

  EXPECT_EQ(fixture.thriftTail.pipelineInactiveCount(), 1);
}

TEST(
    ThriftServerTransportAdapterTest,
    ThriftDeactivateDisconnectsRocketConnection) {
  BridgeFixture fixture;
  fixture.rocketPipeline.activate();
  EXPECT_FALSE(fixture.rocketConnRaw->disconnected_);

  // Thrift side initiates: deactivate the thrift pipeline. The bridge's
  // onPipelineInactive disconnects the owned rocket connection.
  fixture.thriftPipeline->deactivate();

  EXPECT_TRUE(fixture.rocketConnRaw->disconnected_);
}

TEST(ThriftServerTransportAdapterTest, ReactivateAfterDeactivateCyclesCleanly) {
  BridgeFixture fixture;

  fixture.rocketPipeline.activate();
  EXPECT_EQ(fixture.thriftTail.pipelineActiveCount(), 1);
  EXPECT_EQ(fixture.thriftTail.pipelineInactiveCount(), 0);

  fixture.rocketPipeline.deactivate();
  EXPECT_EQ(fixture.thriftTail.pipelineInactiveCount(), 1);

  // Re-activate — the bridge's connected_ flag was cleared by the
  // deactivate path, so the second activate must propagate again.
  fixture.rocketPipeline.activate();
  EXPECT_EQ(fixture.thriftTail.pipelineActiveCount(), 2);

  fixture.rocketPipeline.deactivate();
  EXPECT_EQ(fixture.thriftTail.pipelineInactiveCount(), 2);
}

TEST(
    ThriftServerTransportAdapterTest,
    ReentryFromRocketToThriftToRocketDoesNotLoop) {
  BridgeFixture fixture;
  fixture.rocketPipeline.activate();
  EXPECT_EQ(fixture.thriftTail.pipelineInactiveCount(), 0);
  EXPECT_FALSE(fixture.rocketConnRaw->disconnected_);

  // Rocket deactivates → bridge propagates to thrift pipeline (fires
  // onPipelineInactive on thriftTail). The thrift pipeline deactivate
  // also fires onPipelineInactive on the bridge itself, which would
  // normally call rocketConn->disconnect() — but the bridge's
  // connected_ guard was already cleared in the rocket→thrift
  // propagation path, so the reentry is absorbed. No infinite loop, and
  // disconnect() is never invoked (the rocket pipeline initiated, not
  // the bridge).
  fixture.rocketPipeline.deactivate();

  EXPECT_EQ(fixture.thriftTail.pipelineInactiveCount(), 1);
  EXPECT_FALSE(fixture.rocketConnRaw->disconnected_);
}

namespace {

// Thrift-pipeline tail that subscribes to the WriteComplete event and records
// the relayed payload — models the consumer the bridge fires toward.
struct WriteCompleteCapturingTail {
  template <typename Context>
  Result onRead(Context&, TypeErasedBox&&) noexcept {
    return Result::Success;
  }
  void onException(folly::exception_wrapper&&) noexcept {}
  void handlerAdded() noexcept {}
  void handlerRemoved() noexcept {}
  void onPipelineActive() noexcept {}
  void onPipelineInactive() noexcept {}
  void onWriteReady() noexcept {}

  using SubscribedEvents =
      channel_pipeline::Events<ThriftServerWriteCompleteEvent>;

  template <channel_pipeline::PipelineEvent E>
    requires std::same_as<E, ThriftServerWriteCompleteEvent>
  void on(const ThriftServerWriteCompleteEvent& event) noexcept {
    events.push_back(event);
  }

  std::vector<ThriftServerWriteCompleteEvent> events;
};

} // namespace

TEST(
    ThriftServerTransportAdapterTest,
    RocketWriteCompleteRelaysToThriftWriteCompleteEvent) {
  folly::EventBase evb;
  StaticMockHeadHandler rocketHead;
  rocketHead.setOnWriteCallback(
      [](TypeErasedBox&&) { return Result::Success; });
  TestAllocator rocketAllocator;
  TestAllocator thriftAllocator;
  WriteCompleteCapturingTail thriftTail;

  auto rocketConn = std::make_unique<rocket::server::RocketServerConnection>();
  auto* rocketAdapter = rocketConn->appAdapter.get();
  auto rocketPipeline = StaticPipelineBuilder<
                            StaticMockHeadHandler,
                            rocket::server::RocketServerAppAdapter,
                            TestAllocator>()
                            .setEventBase(&evb)
                            .setHead(&rocketHead)
                            .setTail(rocketAdapter)
                            .setAllocator(&rocketAllocator)
                            .build();
  rocketAdapter->setPipeline(rocketPipeline.get());
  rocketConn->pipeline = std::move(rocketPipeline);

  auto bridge =
      std::make_unique<ThriftServerTransportAdapter>(std::move(rocketConn));
  auto thriftPipeline = StaticPipelineBuilder<
                            ThriftServerTransportAdapter,
                            WriteCompleteCapturingTail,
                            TestAllocator>()
                            .setEventBase(&evb)
                            .setHead(bridge.get())
                            .setTail(&thriftTail)
                            .setAllocator(&thriftAllocator)
                            .build();
  bridge->setPipeline(thriftPipeline.get());

  // Simulate the rocket pipeline delivering an enriched write-completion to
  // the rocket app adapter; the bridge's callback relays it up as a thrift
  // WriteComplete event.
  rocketAdapter->on<rocket::server::RocketWriteCompleteEvent>(
      rocket::server::RocketWriteCompleteEvent{
          .streamId = 4,
          .status = transport::WriteCompletionStatus::Success,
          .quiesced = true,
      });

  ASSERT_EQ(thriftTail.events.size(), 1u);
  EXPECT_EQ(
      thriftTail.events[0].status, transport::WriteCompletionStatus::Success);
  EXPECT_EQ(thriftTail.events[0].streamId, 4u);
  EXPECT_TRUE(thriftTail.events[0].quiesced);

  thriftPipeline->deactivate();
  thriftPipeline->close();
  bridge.reset();
}

} // namespace apache::thrift::fast_thrift::thrift::server::test
