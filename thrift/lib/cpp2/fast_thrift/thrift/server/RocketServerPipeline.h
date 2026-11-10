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

#pragma once

#include <concepts>
#include <memory>
#include <type_traits>
#include <utility>

#include <folly/CppAttributes.h>
#include <folly/io/async/AsyncTransport.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/BufferAllocator.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/HandlerTag.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineRef.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/StaticPipelineBuilder.h>
#include <thrift/lib/cpp2/fast_thrift/common/ServerStats.h>
#include <thrift/lib/cpp2/fast_thrift/frame/handler/FrameCodecHandler.h>
#include <thrift/lib/cpp2/fast_thrift/frame/read/AlignedParser.h>
#include <thrift/lib/cpp2/fast_thrift/frame/read/FrameLengthParser.h>
#include <thrift/lib/cpp2/fast_thrift/frame/read/handler/FrameDefragmentationHandler.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/handler/FragmentCompletionTracker.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/handler/FrameFragmentationHandler.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/handler/FrameLengthEncoderHandler.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/handler/IntervalBatchingFrameHandler.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/handler/WriteCompletionTracker.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/common/RocketStreamContext.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/common/handler/RocketMetricsHandler.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/server/RocketServerEventFactory.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/server/adapter/RocketServerAppAdapter.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/server/common/RocketServerConnection.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/server/handler/RocketServerKeepAliveHandler.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/server/handler/RocketServerMessageMarshalHandler.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/server/handler/RocketServerRequestResponseHandler.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/server/handler/RocketServerSetupFrameHandler.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/server/handler/RocketServerStreamStateHandler.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/server/handler/RocketServerWriteCompletionHandler.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/ThriftServerConnectionFactory.h>
#include <thrift/lib/cpp2/fast_thrift/transport/TransportHandler.h>

namespace apache::thrift::fast_thrift::thrift::server {

// Both write-path handlers carry completion trackers. The transport's
// per-writev completion becomes the batcher's per-batch event, and the
// fragmenter resolves it into the per-frame completion of the rocket layer.
// Each stage has one subscriber, so a NoOp tracker strands every event below.
//
// The batcher takes its FlushWritesEvent subscription from its tracker, so it
// flushes its pending batch before teardown.
using ServerBatchingFrameHandler =
    frame::write::handler::IntervalBatchingFrameHandlerT<
        frame::write::handler::WriteCompletionTrackerT<
            rocket::server::RocketServerEventFactory>>;
using ServerFragmentationFrameHandler =
    frame::write::handler::FrameFragmentationHandlerT<
        frame::write::handler::FragmentCompletionTrackerT<
            rocket::server::RocketServerEventFactory>>;

// Handler overrides name the rocket pipeline handlers by these tags.
HANDLER_TAG(batching_frame_handler);
HANDLER_TAG(frame_length_encoder_handler);
HANDLER_TAG(frame_codec_handler);
HANDLER_TAG(frame_defragmentation_handler);
HANDLER_TAG(frame_fragmentation_handler);
HANDLER_TAG(server_write_completion_handler);
HANDLER_TAG(rocket_server_message_marshal_handler);
HANDLER_TAG(server_setup_frame_handler);
HANDLER_TAG(server_keepalive_handler);
HANDLER_TAG(server_request_response_frame_handler);
HANDLER_TAG(server_stream_state_handler);

namespace detail {

// The fragmenter is the batcher's only BatchWriteComplete subscriber in the
// server pipeline, so a fragmenter with a NoOp tracker strands that event.
template <typename Fragmenter>
inline constexpr bool kFragmenterTracksCompletion = !std::same_as<
    typename Fragmenter::SubscribedEvents,
    channel_pipeline::Events<>>;

// The dynamic pipeline adds this alias as its fragmenter.
static_assert(
    kFragmenterTracksCompletion<ServerFragmentationFrameHandler>,
    "the fragmenter is the batcher's only BatchWriteComplete subscriber in "
    "the server pipeline; a NoOp tracker here would strand it");

// Checks the batcher and the fragmenter that the built pipeline holds, because
// handler overrides can put other handlers under their tags.
template <typename Builder>
channel_pipeline::PipelineOwner buildCheckedRocketPipeline(Builder&& builder) {
  auto pipeline = std::forward<Builder>(builder).build();
  using Batcher = std::remove_reference_t<decltype(pipeline->handler(
      batching_frame_handler_tag))>;
  static_assert(
      requires {
        Batcher::PublishedEvents::template index<
            rocket::server::RocketServerEventFactory::
                BatchWriteCompleteEventType>;
      },
      "the batcher that handler overrides install must publish "
      "BatchWriteComplete, the fragmenter's only completion source");
  static_assert(
      requires {
        Batcher::SubscribedEvents::template index<
            rocket::server::RocketServerEventFactory::FlushWritesEventType>;
      },
      "the batcher that handler overrides install must subscribe to "
      "FlushWritesEvent, or it drops its pending batch at close");
  static_assert(
      kFragmenterTracksCompletion<std::remove_reference_t<
          decltype(pipeline->handler(frame_fragmentation_handler_tag))>>,
      "the fragmenter that handler overrides install needs a fragment "
      "completion tracker, or BatchWriteComplete has no subscriber");
  return channel_pipeline::PipelineOwner(std::move(pipeline));
}

template <typename Overrides, bool WithStats, typename TransportHandler>
channel_pipeline::PipelineOwner buildStaticRocketPipeline(
    const ThriftServerConnectionFactoryConfig& config,
    channel_pipeline::SimpleBufferAllocator* allocator,
    folly::EventBase* evb,
    TransportHandler* transportHandler,
    rocket::server::RocketServerAppAdapter* appAdapter,
    ServerStatsShard* FOLLY_NULLABLE statsShard) {
  auto builder =
      channel_pipeline::StaticPipelineBuilder<
          TransportHandler,
          rocket::server::RocketServerAppAdapter,
          channel_pipeline::SimpleBufferAllocator>()
          .setEventBase(evb)
          .setHead(transportHandler)
          .setTail(appAdapter)
          .setAllocator(allocator)
          .template addState<rocket::RocketStreamContexts>()
          .template withHandlerOverrides<Overrides>()
          .template addNextOutbound<ServerBatchingFrameHandler>(
              batching_frame_handler_tag, config.batchingConfig)
          .template addNextOutbound<
              frame::write::handler::FrameLengthEncoderHandler>(
              frame_length_encoder_handler_tag)
          .template addNextDuplex<frame::handler::FrameCodecHandler>(
              frame_codec_handler_tag)
          .template addNextInbound<
              frame::read::handler::FrameDefragmentationHandler>(
              frame_defragmentation_handler_tag)
          .template addNextOutbound<ServerFragmentationFrameHandler>(
              frame_fragmentation_handler_tag, config.fragmentationConfig)
          .template addNextDuplex<
              rocket::server::handler::RocketServerWriteCompletionHandler>(
              server_write_completion_handler_tag)
          .template addNextDuplex<
              rocket::server::handler::RocketServerMessageMarshalHandler>(
              rocket_server_message_marshal_handler_tag)
          .template addNextDuplex<
              rocket::server::handler::RocketServerSetupFrameHandler>(
              server_setup_frame_handler_tag)
          .template addNextDuplex<
              rocket::server::handler::RocketServerKeepAliveHandler>(
              server_keepalive_handler_tag)
          .template addNextDuplex<
              rocket::server::handler::RocketServerRequestResponseHandler>(
              server_request_response_frame_handler_tag)
          .template addNextDuplex<
              rocket::server::handler::RocketServerStreamStateHandler>(
              server_stream_state_handler_tag, config.enableCancellation);
  if constexpr (WithStats) {
    return buildCheckedRocketPipeline(
        std::move(builder)
            .template addNextDuplex<
                RocketMetricsHandler<Direction::Server, ServerStatsShard>>(
                rocket_metrics_handler_tag, CHECK_NOTNULL(statsShard)));
  } else {
    return buildCheckedRocketPipeline(std::move(builder));
  }
}

template <typename Overrides, typename TransportHandler>
channel_pipeline::PipelineOwner selectStaticRocketStats(
    const ThriftServerConnectionFactoryConfig& config,
    channel_pipeline::SimpleBufferAllocator* allocator,
    folly::EventBase* evb,
    TransportHandler* transportHandler,
    rocket::server::RocketServerAppAdapter* appAdapter,
    ServerStatsShard* FOLLY_NULLABLE statsShard) {
  if (statsShard != nullptr) {
    return buildStaticRocketPipeline<Overrides, true>(
        config, allocator, evb, transportHandler, appAdapter, statsShard);
  }
  return buildStaticRocketPipeline<Overrides, false>(
      config, allocator, evb, transportHandler, appAdapter, statsShard);
}

// XLOG takes the log category from the file name. So this function is defined
// in ThriftServerConnectionFactory.cpp, and the warning logs under the
// category of that file.
void warnZeroCopyNotSupported();

template <transport::Parser ParserT, typename BuildPipeline>
void initializeRocketConnection(
    folly::AsyncTransport::UniquePtr socket,
    const ThriftServerConnectionFactoryConfig& config,
    rocket::server::RocketServerConnection& rocketConnection,
    const BuildPipeline& buildPipeline) {
  using TransportHandler = transport::
      TransportHandlerT<rocket::server::RocketServerEventFactory, ParserT>;
  typename TransportHandler::Ptr transportHandler =
      TransportHandler::createWithParser(std::move(socket), ParserT{});
  channel_pipeline::PipelineOwner pipeline =
      buildPipeline(transportHandler.get(), rocketConnection.appAdapter.get());
  rocketConnection.appAdapter->setPipeline(pipeline.get());
  transportHandler->setPipeline(pipeline.get());

  if (config.zeroCopyThreshold > 0) {
    if (!transportHandler->setZeroCopy(true)) {
      warnZeroCopyNotSupported();
    }
    transportHandler->setZeroCopyEnableThreshold(config.zeroCopyThreshold);
  }
  rocketConnection.transportHandler =
      rocket::server::AnyTransportHandler(std::move(transportHandler));
  rocketConnection.pipeline = std::move(pipeline);
}

/**
 * Builds a rocket connection for `socket` with the frame parser that `config`
 * selects. `buildPipeline(transportHandler, appAdapter)` returns the rocket
 * pipeline between the two.
 */
template <typename BuildPipeline>
std::unique_ptr<rocket::server::RocketServerConnection>
buildRocketServerConnection(
    folly::AsyncTransport::UniquePtr socket,
    const ThriftServerConnectionFactoryConfig& config,
    const BuildPipeline& buildPipeline) {
  auto rocketConnection =
      std::make_unique<rocket::server::RocketServerConnection>();
  if (config.useAlignedParser) {
    initializeRocketConnection<frame::read::AlignedParser>(
        std::move(socket), config, *rocketConnection, buildPipeline);
  } else {
    initializeRocketConnection<frame::read::FrameLengthParser>(
        std::move(socket), config, *rocketConnection, buildPipeline);
  }
  return rocketConnection;
}

} // namespace detail

/**
 * Builds the rocket connection of the static server for `socket`. `Overrides`
 * replaces handlers as StaticPipelineBuilder::withHandlerOverrides()
 * describes, so FastThriftServer::setRocketConnectionBuilder() can install the
 * production pipeline with other handlers.
 *
 * The connection takes its batching, fragmentation, cancellation, zero copy and
 * frame parser settings from `config`. A rocket connection builder does not
 * get the server's settings, so `config` must repeat them.
 *
 * `allocator` must outlive the connection.
 */
template <typename Overrides>
std::unique_ptr<rocket::server::RocketServerConnection>
buildStaticRocketServerConnection(
    folly::AsyncTransport::UniquePtr socket,
    const ThriftServerConnectionFactoryConfig& config,
    channel_pipeline::SimpleBufferAllocator& allocator,
    ServerStatsShard* FOLLY_NULLABLE statsShard) {
  folly::EventBase* const evb = socket->getEventBase();
  return detail::buildRocketServerConnection(
      std::move(socket),
      config,
      [&](auto* transportHandler,
          rocket::server::RocketServerAppAdapter* appAdapter) {
        return detail::selectStaticRocketStats<Overrides>(
            config, &allocator, evb, transportHandler, appAdapter, statsShard);
      });
}

} // namespace apache::thrift::fast_thrift::thrift::server
