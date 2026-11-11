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

#include <thrift/lib/cpp2/fast_thrift/thrift/server/ThriftServerConnectionFactory.h>

#include <algorithm>
#include <functional>
#include <memory>
#include <stdexcept>
#include <type_traits>
#include <utility>

#include <folly/io/IOBuf.h>
#include <folly/logging/xlog.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Event.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/HandlerTag.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineBuilder.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineStorage.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/StaticPipelineBuilder.h>
#include <thrift/lib/cpp2/fast_thrift/frame/handler/FrameCodecHandler.h>
#include <thrift/lib/cpp2/fast_thrift/frame/read/handler/FrameDefragmentationHandler.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/handler/FrameLengthEncoderHandler.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/common/RocketStreamContext.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/common/handler/RocketMetricsHandler.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/server/Event.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/server/common/RocketServerConnection.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/server/handler/RocketServerKeepAliveHandler.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/server/handler/RocketServerMessageMarshalHandler.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/server/handler/RocketServerRequestResponseHandler.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/server/handler/RocketServerStreamStateHandler.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/server/handler/RocketServerWriteCompletionHandler.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/common/handler/ThriftMetricsHandler.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/RocketServerPipeline.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/SetupResponseBuilder.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/adapter/MetadataAppAdapter.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/adapter/ThriftServerCompositeAppAdapter.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/Event.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/ReadBackpressureArbiter.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/handler/ConcurrencyControlHandler.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/handler/ThriftServerBackpressureHandler.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/handler/ThriftServerChecksumHandler.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/handler/ThriftServerCompressionHandler.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/handler/ThriftServerConnectionCloseHandler.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/handler/ThriftServerRequestContextHandler.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/handler/ThriftServerRequestLifecycleHandler.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/handler/ThriftServerSetupHandler.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/handler/ThriftServerStreamMuxHandler.h>

namespace apache::thrift::fast_thrift::thrift::server {

namespace {
using channel_pipeline::PipelineBuilder;
using channel_pipeline::PipelineImpl;
using channel_pipeline::PipelineOwner;
using channel_pipeline::SimpleBufferAllocator;

template <typename Context>
using ThriftServerConcurrencyControlHandler =
    ConcurrencyControlHandler<Context, ThriftServerRequestCompletedEvent>;

HANDLER_TAG(thrift_server_request_context_handler);
HANDLER_TAG(thrift_server_request_lifecycle_handler);
HANDLER_TAG(thrift_server_compression_handler);
HANDLER_TAG(thrift_server_checksum_handler);
HANDLER_TAG(thrift_server_connection_close_handler);
HANDLER_TAG(thrift_server_backpressure_handler);
HANDLER_TAG(thrift_server_setup_handler);
HANDLER_TAG(thrift_server_stream_mux_handler);
HANDLER_TAG(thrift_server_concurrency_control_handler);

template <bool WithCancellation, bool WithStreamMux, typename Builder>
PipelineOwner finishStaticThriftPipelineTailImpl(Builder&& builder) {
  if constexpr (WithCancellation && WithStreamMux) {
    return PipelineOwner(
        std::forward<Builder>(builder)
            .template addNextDuplexTemplate<
                ThriftServerRequestLifecycleHandler>(
                thrift_server_request_lifecycle_handler_tag)
            .template addNextDuplexTemplate<ThriftServerStreamMuxHandler>(
                thrift_server_stream_mux_handler_tag)
            .build());
  } else if constexpr (WithCancellation) {
    return PipelineOwner(
        std::forward<Builder>(builder)
            .template addNextDuplexTemplate<
                ThriftServerRequestLifecycleHandler>(
                thrift_server_request_lifecycle_handler_tag)
            .build());
  } else if constexpr (WithStreamMux) {
    return PipelineOwner(
        std::forward<Builder>(builder)
            .template addNextDuplexTemplate<ThriftServerStreamMuxHandler>(
                thrift_server_stream_mux_handler_tag)
            .build());
  } else {
    return PipelineOwner(std::forward<Builder>(builder).build());
  }
}

template <bool WithCancellation, bool WithStreamMux, typename Builder>
PipelineOwner finishStaticThriftPipelineTail(
    Builder&& builder, const ThriftServerConnectionFactoryConfig& config) {
  auto withSetup =
      std::forward<Builder>(builder)
          .template addNextDuplexTemplate<ThriftServerSetupHandler>(
              thrift_server_setup_handler_tag);
  if (config.requestConcurrencyLimits) {
    const auto& limits = *config.requestConcurrencyLimits;
    return finishStaticThriftPipelineTailImpl<WithCancellation, WithStreamMux>(
        std::move(withSetup)
            .template addNextDuplexTemplate<
                ThriftServerConcurrencyControlHandler>(
                thrift_server_concurrency_control_handler_tag,
                limits.maxActiveRequests,
                limits.maxOutstandingRequests,
                std::ref(*config.concurrencyState)));
  }
  return finishStaticThriftPipelineTailImpl<WithCancellation, WithStreamMux>(
      std::move(withSetup));
}

template <typename Builder>
PipelineOwner selectStaticThriftPipelineTail(
    Builder&& builder,
    bool enableCancellation,
    bool enableStreamMux,
    const ThriftServerConnectionFactoryConfig& config) {
  if (enableCancellation) {
    if (enableStreamMux) {
      return finishStaticThriftPipelineTail<true, true>(
          std::forward<Builder>(builder), config);
    }
    return finishStaticThriftPipelineTail<true, false>(
        std::forward<Builder>(builder), config);
  }
  if (enableStreamMux) {
    return finishStaticThriftPipelineTail<false, true>(
        std::forward<Builder>(builder), config);
  }
  return finishStaticThriftPipelineTail<false, false>(
      std::forward<Builder>(builder), config);
}

template <bool WithExtensions, typename Builder>
PipelineOwner finishStaticThriftPipeline(
    Builder&& builder,
    const ThriftServerConnectionFactoryConfig& config,
    ExtensionStateStore& extensionStates) {
  if constexpr (WithExtensions) {
    const bool hasSegments = std::any_of(
        config.thriftPipelineRegistrations.begin(),
        config.thriftPipelineRegistrations.end(),
        [](const auto& registration) { return registration.isSegment(); });
    if (hasSegments) {
      std::vector<channel_pipeline::detail::ErasedStaticSegment> segments;
      segments.reserve(config.thriftPipelineRegistrations.size());
      for (const auto& registration : config.thriftPipelineRegistrations) {
        if (registration.isSegment()) {
          segments.push_back(registration.makeStaticSegment(extensionStates));
        } else {
          segments.push_back(
              channel_pipeline::makeErasedStaticHandlerSegment(
                  registration.makeStaticHandler(extensionStates)));
        }
      }
      return selectStaticThriftPipelineTail(
          std::forward<Builder>(builder).addStaticSegments(std::move(segments)),
          config.enableCancellation,
          config.enableStreamMux,
          config);
    }
    std::vector<channel_pipeline::detail::ErasedStaticHandler> handlers;
    handlers.reserve(config.thriftPipelineRegistrations.size());
    for (const auto& registration : config.thriftPipelineRegistrations) {
      handlers.push_back(registration.makeStaticHandler(extensionStates));
    }
    return selectStaticThriftPipelineTail(
        std::forward<Builder>(builder).addStaticHandlers(std::move(handlers)),
        config.enableCancellation,
        config.enableStreamMux,
        config);
  } else {
    return selectStaticThriftPipelineTail(
        std::forward<Builder>(builder),
        config.enableCancellation,
        config.enableStreamMux,
        config);
  }
}

template <bool WithExtensions, typename Builder>
PipelineOwner addStaticBackpressure(
    Builder&& builder,
    const ThriftServerConnectionFactoryConfig& config,
    ExtensionStateStore& extensionStates,
    ReadBackpressureArbiter& readBackpressureArbiter) {
  return finishStaticThriftPipeline<WithExtensions>(
      std::forward<Builder>(builder)
          .template addNextDuplexTemplate<ThriftServerBackpressureHandler>(
              thrift_server_backpressure_handler_tag, &readBackpressureArbiter),
      config,
      extensionStates);
}

template <bool WithChecksum, bool WithExtensions, typename Builder>
PipelineOwner addStaticChecksum(
    Builder&& builder,
    const ThriftServerConnectionFactoryConfig& config,
    ExtensionStateStore& extensionStates,
    ReadBackpressureArbiter& readBackpressureArbiter) {
  if constexpr (WithChecksum) {
    return addStaticBackpressure<WithExtensions>(
        std::forward<Builder>(builder)
            .template addNextDuplexTemplate<ThriftServerChecksumHandler>(
                thrift_server_checksum_handler_tag)
            .template addNextDuplexTemplate<ThriftServerConnectionCloseHandler>(
                thrift_server_connection_close_handler_tag,
                config.drainTimeout,
                config.reapTimeout),
        config,
        extensionStates,
        readBackpressureArbiter);
  } else {
    return addStaticBackpressure<WithExtensions>(
        std::forward<Builder>(builder)
            .template addNextDuplexTemplate<ThriftServerConnectionCloseHandler>(
                thrift_server_connection_close_handler_tag,
                config.drainTimeout,
                config.reapTimeout),
        config,
        extensionStates,
        readBackpressureArbiter);
  }
}

template <bool WithChecksum, bool WithExtensions, typename Builder>
PipelineOwner addStaticCompression(
    Builder&& builder,
    const ThriftServerConnectionFactoryConfig& config,
    ExtensionStateStore& extensionStates,
    ReadBackpressureArbiter& readBackpressureArbiter) {
  return addStaticChecksum<WithChecksum, WithExtensions>(
      std::forward<Builder>(builder)
          .template addNextDuplexTemplate<ThriftServerCompressionHandler>(
              thrift_server_compression_handler_tag),
      config,
      extensionStates,
      readBackpressureArbiter);
}

template <
    bool WithStats,
    bool WithHeaders,
    bool WithChecksum,
    bool WithExtensions,
    typename TailAdapter>
PipelineOwner buildStaticThriftPipeline(
    folly::EventBase* evb,
    ThriftServerTransportAdapter* transportAdapter,
    TailAdapter* tailAdapter,
    SimpleBufferAllocator* allocator,
    boost::intrusive_ptr<ThriftConnContext> connContext,
    ExtensionStateStore& extensionStates,
    const ThriftServerConnectionFactoryConfig& config,
    ServerStatsShard* statsShard) {
  auto builder = channel_pipeline::StaticPipelineBuilder<
                     ThriftServerTransportAdapter,
                     TailAdapter,
                     SimpleBufferAllocator>()
                     .setEventBase(evb)
                     .setHead(transportAdapter)
                     .setTail(tailAdapter)
                     .setAllocator(allocator);
  auto& readBackpressureArbiter = connContext->readBackpressureArbiter();
  if constexpr (WithStats) {
    return addStaticCompression<WithChecksum, WithExtensions>(
        std::move(builder)
            .template addNextDuplex<
                ThriftMetricsHandler<Direction::Server, ServerStatsShard>>(
                thrift_metrics_handler_tag, statsShard)
            .template addNextDuplexTemplate<ThriftServerRequestContextHandler>(
                thrift_server_request_context_handler_tag,
                config.requestExtensionLayout.get(),
                config.methodMetadata.get(),
                std::move(connContext),
                WithHeaders),
        config,
        extensionStates,
        readBackpressureArbiter);
  } else {
    return addStaticCompression<WithChecksum, WithExtensions>(
        std::move(builder)
            .template addNextDuplexTemplate<ThriftServerRequestContextHandler>(
                thrift_server_request_context_handler_tag,
                config.requestExtensionLayout.get(),
                config.methodMetadata.get(),
                std::move(connContext),
                WithHeaders),
        config,
        extensionStates,
        readBackpressureArbiter);
  }
}

template <
    bool WithStats,
    bool WithHeaders,
    bool WithChecksum,
    typename TailAdapter>
PipelineOwner selectStaticExtensions(
    const ThriftServerConnectionFactoryConfig& config,
    folly::EventBase* evb,
    ThriftServerTransportAdapter* transportAdapter,
    TailAdapter* tailAdapter,
    SimpleBufferAllocator* allocator,
    boost::intrusive_ptr<ThriftConnContext> connContext,
    ExtensionStateStore& extensionStates,
    ServerStatsShard* statsShard) {
  if (!config.thriftPipelineRegistrations.empty()) {
    return buildStaticThriftPipeline<
        WithStats,
        WithHeaders,
        WithChecksum,
        true>(
        evb,
        transportAdapter,
        tailAdapter,
        allocator,
        std::move(connContext),
        extensionStates,
        config,
        statsShard);
  }
  return buildStaticThriftPipeline<WithStats, WithHeaders, WithChecksum, false>(
      evb,
      transportAdapter,
      tailAdapter,
      allocator,
      std::move(connContext),
      extensionStates,
      config,
      statsShard);
}

template <bool WithStats, bool WithHeaders, typename TailAdapter>
PipelineOwner selectStaticChecksum(
    const ThriftServerConnectionFactoryConfig& config,
    folly::EventBase* evb,
    ThriftServerTransportAdapter* transportAdapter,
    TailAdapter* tailAdapter,
    SimpleBufferAllocator* allocator,
    boost::intrusive_ptr<ThriftConnContext> connContext,
    ExtensionStateStore& extensionStates,
    ServerStatsShard* statsShard) {
  if (config.enableChecksum) {
    return selectStaticExtensions<WithStats, WithHeaders, true>(
        config,
        evb,
        transportAdapter,
        tailAdapter,
        allocator,
        std::move(connContext),
        extensionStates,
        statsShard);
  }
  return selectStaticExtensions<WithStats, WithHeaders, false>(
      config,
      evb,
      transportAdapter,
      tailAdapter,
      allocator,
      std::move(connContext),
      extensionStates,
      statsShard);
}

template <bool WithStats, typename TailAdapter>
PipelineOwner selectStaticHeaders(
    const ThriftServerConnectionFactoryConfig& config,
    folly::EventBase* evb,
    ThriftServerTransportAdapter* transportAdapter,
    TailAdapter* tailAdapter,
    SimpleBufferAllocator* allocator,
    boost::intrusive_ptr<ThriftConnContext> connContext,
    ExtensionStateStore& extensionStates,
    ServerStatsShard* statsShard) {
  if (config.enableRequestHeaders) {
    return selectStaticChecksum<WithStats, true>(
        config,
        evb,
        transportAdapter,
        tailAdapter,
        allocator,
        std::move(connContext),
        extensionStates,
        statsShard);
  }
  return selectStaticChecksum<WithStats, false>(
      config,
      evb,
      transportAdapter,
      tailAdapter,
      allocator,
      std::move(connContext),
      extensionStates,
      statsShard);
}

template <typename TailAdapter>
PipelineOwner selectStaticThriftStats(
    const ThriftServerConnectionFactoryConfig& config,
    folly::EventBase* evb,
    ThriftServerTransportAdapter* transportAdapter,
    TailAdapter* tailAdapter,
    SimpleBufferAllocator* allocator,
    boost::intrusive_ptr<ThriftConnContext> connContext,
    ExtensionStateStore& extensionStates,
    ServerStatsShard* statsShard) {
  if (statsShard != nullptr) {
    return selectStaticHeaders<true>(
        config,
        evb,
        transportAdapter,
        tailAdapter,
        allocator,
        std::move(connContext),
        extensionStates,
        statsShard);
  }
  return selectStaticHeaders<false>(
      config,
      evb,
      transportAdapter,
      tailAdapter,
      allocator,
      std::move(connContext),
      extensionStates,
      statsShard);
}
} // namespace

template <ChannelPipelineMode Mode>
detail::ThriftServerConnectionFactoryT<Mode>::ThriftServerConnectionFactoryT(
    ThriftServerConnectionFactoryConfig config)
    : config_(std::move(config)),
      needsComposite_(
          static_cast<bool>(config_.monitoringHandler) ||
          static_cast<bool>(config_.statusHandler) ||
          static_cast<bool>(config_.debugHandler) ||
          static_cast<bool>(config_.controlHandler) ||
          static_cast<bool>(config_.securityHandler) ||
          static_cast<bool>(config_.metadataResponse)) {
  CHECK(config_.handler)
      << "ThriftServerConnectionFactory requires a non-null handler";
  if constexpr (Mode == ChannelPipelineMode::Static) {
    if (std::any_of(
            config_.thriftPipelineRegistrations.begin(),
            config_.thriftPipelineRegistrations.end(),
            [](const auto& registration) {
              return !registration.supportsStaticPipeline();
            })) {
      throw std::logic_error(
          "Dynamic thrift pipeline handlers require "
          "DynamicFastThriftServer");
    }
  }
  if (config_.requestConcurrencyLimits && !config_.concurrencyState) {
    config_.concurrencyState =
        std::make_shared<ConcurrencyControlStateHolder>();
  }
  if (!needsComposite_) {
    return;
  }

  std::vector<std::shared_ptr<const ThriftServerMethodDispatchTable>> tables;
  bool allTablesAvailable = true;
  auto appendTable = [&](const auto& factory) {
    if (!factory) {
      return;
    }
    auto table = factory->getMethodDispatchTable();
    if (!table) {
      allTablesAvailable = false;
      return;
    }
    tables.push_back(std::move(table));
  };
  appendTable(config_.handler);
  appendTable(config_.monitoringHandler);
  appendTable(config_.statusHandler);
  appendTable(config_.debugHandler);
  appendTable(config_.controlHandler);
  appendTable(config_.securityHandler);
  if (config_.metadataResponse) {
    tables.push_back(MetadataAppAdapter::methodDispatchTable());
  }
  if (allTablesAvailable) {
    compositeRoutes_ =
        ThriftServerCompositeRoutingTable::create(std::move(tables));
  }
}

template <ChannelPipelineMode Mode>
ThriftServerConnection
detail::ThriftServerConnectionFactoryT<Mode>::getConnection(
    folly::AsyncTransport::UniquePtr socket,
    const folly::SocketAddress& clientAddr,
    const std::shared_ptr<const connection::PeerSecurityInfo>& peerSecurity) {
  boost::intrusive_ptr<ThriftConnContext> connContext{new ThriftConnContext()};
  if (config_.connExtensionLayout != nullptr) {
    connContext->installExtensions(*config_.connExtensionLayout);
  }
  connContext->setPeerAddress(clientAddr);
  // Non-owning. The transport is owned by the transport adapter, which
  // ThriftServerConnection tears down after the pipeline that reads this —
  // so it outlives every handler holding the context. Security is snapshotted
  // separately because this is already the post-StopTLS transport when one was
  // negotiated, and reports nothing about the peer.
  connContext->setTransport(socket.get());
  if (peerSecurity != nullptr) {
    connContext->setPeerCertificate(peerSecurity->peerCertificate);
    connContext->setSecurityProtocol(peerSecurity->securityProtocol);
  }

  ThriftServerConnection conn = needsComposite_
      ? buildCompositeConnection(std::move(socket), clientAddr, connContext)
      : buildSimpleConnection(std::move(socket), clientAddr, connContext);

  // Expose the context on the connection so the connection-layer accept
  // callback can reach it post-construction.
  conn.connContext = std::move(connContext);

  // Note: the connection is fully wired but inert. Reading is started
  // separately via ThriftServerConnection::start() once the connection
  // layer has run its accept-time setup (e.g. onConnectionAccepted hook,
  // registration in the connection-manager map). Starting here would race
  // those steps: setReadCB can synchronously drain pre-received bytes
  // (post-StopTLS handoff) and dispatch the first request before the
  // accept hook has populated per-connection state.
  return conn;
}

template <ChannelPipelineMode Mode>
void detail::ThriftServerConnectionFactoryT<Mode>::attachCPUExecutor(
    ThriftServerAppAdapter& adapter) const {
  if (config_.cpuExecutor) {
    adapter.setCPUExecutor(config_.cpuExecutor);
  }
}

template <ChannelPipelineMode Mode>
ThriftServerConnection
detail::ThriftServerConnectionFactoryT<Mode>::buildSimpleConnection(
    folly::AsyncTransport::UniquePtr socket,
    const folly::SocketAddress& clientAddr,
    boost::intrusive_ptr<ThriftConnContext> connContext) {
  ThriftServerConnection::SimpleTail tail{
      .adapter = config_.handler->getAppAdapter(config_.handler)};
  auto* tailAdapter = tail.adapter.get();
  attachCPUExecutor(*tailAdapter);
  return buildConnectionImpl<
      ThriftServerAppAdapter,
      channel_pipeline::pipeline_storage::Automatic>(
      std::move(socket),
      clientAddr,
      std::move(tail),
      tailAdapter,
      std::move(connContext));
}

template <ChannelPipelineMode Mode>
ThriftServerConnection
detail::ThriftServerConnectionFactoryT<Mode>::buildCompositeConnection(
    folly::AsyncTransport::UniquePtr socket,
    const folly::SocketAddress& clientAddr,
    boost::intrusive_ptr<ThriftConnContext> connContext) {
  // Build the composite tail: user adapter + each wired aux + metadata.
  // children must outlive the composite (composite borrows raw T* into
  // them); ThriftServerConnection::CompositeTail field declaration order
  // guarantees this.
  ThriftServerConnection::CompositeTail tail;
  tail.children.push_back(config_.handler->getAppAdapter(config_.handler));
  attachCPUExecutor(*tail.children.back());
  // Aux interfaces offload like the user handler. Methods that must stay on
  // the EventBase are pinned per-method in their IDLs via
  // @cpp.ProcessInEbThreadUnsafe, not by withholding the executor here.
  if (config_.monitoringHandler) {
    tail.children.push_back(
        config_.monitoringHandler->getAppAdapter(config_.monitoringHandler));
    attachCPUExecutor(*tail.children.back());
  }
  if (config_.statusHandler) {
    tail.children.push_back(
        config_.statusHandler->getAppAdapter(config_.statusHandler));
    attachCPUExecutor(*tail.children.back());
  }
  if (config_.debugHandler) {
    tail.children.push_back(
        config_.debugHandler->getAppAdapter(config_.debugHandler));
    attachCPUExecutor(*tail.children.back());
  }
  if (config_.controlHandler) {
    tail.children.push_back(
        config_.controlHandler->getAppAdapter(config_.controlHandler));
    attachCPUExecutor(*tail.children.back());
  }
  if (config_.securityHandler) {
    tail.children.push_back(
        config_.securityHandler->getAppAdapter(config_.securityHandler));
    attachCPUExecutor(*tail.children.back());
  }
  // Deliberately not offloaded: MetadataAppAdapter is hand-written rather
  // than generated, so it never consults cpuExecutor() and completes through
  // the EventBase-only writeResponse overload. Attaching an executor would
  // compile and do nothing. Its body only re-serializes an immutable
  // prebuilt response, so there is nothing to move off the EventBase.
  if (config_.metadataResponse) {
    tail.children.push_back(
        ThriftServerAppAdapter::Ptr{
            new MetadataAppAdapter(config_.metadataResponse)});
  }
  tail.adapter = ThriftServerCompositeAppAdapter::Ptr{
      new ThriftServerCompositeAppAdapter(compositeRoutes_)};
  for (auto& child : tail.children) {
    tail.adapter->addChild(child.get());
  }
  auto* compositeAdapter = tail.adapter.get();
  return buildConnectionImpl<
      ThriftServerCompositeAppAdapter,
      channel_pipeline::pipeline_storage::Automatic>(
      std::move(socket),
      clientAddr,
      std::move(tail),
      compositeAdapter,
      std::move(connContext));
}

void detail::warnZeroCopyNotSupported() {
  XLOG(WARN) << "MSG_ZEROCOPY not supported on this socket";
}

template <ChannelPipelineMode Mode>
template <typename TailAdapter, typename Storage>
ThriftServerConnection
detail::ThriftServerConnectionFactoryT<Mode>::buildConnectionImpl(
    folly::AsyncTransport::UniquePtr socket,
    const folly::SocketAddress& clientAddr,
    std::variant<
        std::monostate,
        ThriftServerConnection::SimpleTail,
        ThriftServerConnection::CompositeTail> tail,
    TailAdapter* tailAdapter,
    boost::intrusive_ptr<ThriftConnContext> connContext) {
  folly::EventBase* const evb = socket->getEventBase();

  // Accept runs on the IO thread that will own this connection, which is the
  // thread holding the shard these counts belong in — both to look it up here
  // and to increment it from the pipeline later.
  ServerStatsShard* statsShard = nullptr;
  if (config_.stats) {
    DCHECK(evb->isInEventBaseThread());
    statsShard = &config_.stats->currentThreadShard();
  }

  ThriftServerConnection conn;
  conn.tail = std::move(tail);

  std::unique_ptr<rocket::server::RocketServerConnection> rocketConnection;
  std::optional<channel_pipeline::detail::HandlerNode>
      transportNegotiationHandler;
  if (config_.rocketConnectionBuilder) {
    auto buildResult = config_.rocketConnectionBuilder(
        std::move(socket), clientAddr, statsShard);
    rocketConnection = std::move(buildResult.connection);
    transportNegotiationHandler =
        std::move(buildResult.transportNegotiationHandler);
    CHECK(rocketConnection)
        << "RocketServerConnectionBuilder returned a null connection";
  } else {
    rocketConnection = detail::buildRocketServerConnection(
        std::move(socket),
        config_,
        [&](auto* transportHandler,
            rocket::server::RocketServerAppAdapter* appAdapter) {
          return buildRocketPipeline<
              channel_pipeline::pipeline_storage::Medium>(
              evb, transportHandler, appAdapter, statsShard);
        });
  }

  conn.thriftTransportAdapter = std::make_unique<ThriftServerTransportAdapter>(
      std::move(rocketConnection));
  ThriftServerTransportAdapter* const transportAdapterPtr =
      conn.thriftTransportAdapter.get();

  // Thrift pipeline templated on the tail adapter type. For the simple case
  // this works because generated adapters use the base adapter's shared
  // dispatch implementation; the composite tail also fans setPipeline out to
  // every child.
  using ReqCtxHandler =
      ThriftServerRequestContextHandler<channel_pipeline::detail::ContextImpl>;
  using CompressionHandler =
      ThriftServerCompressionHandler<channel_pipeline::detail::ContextImpl>;
  using ChecksumHandler =
      ThriftServerChecksumHandler<channel_pipeline::detail::ContextImpl>;
  using CloseHandler =
      ThriftServerConnectionCloseHandler<channel_pipeline::detail::ContextImpl>;
  using RequestLifecycleHandler = ThriftServerRequestLifecycleHandler<
      channel_pipeline::detail::ContextImpl>;
  using BackpressureHandler =
      ThriftServerBackpressureHandler<channel_pipeline::detail::ContextImpl>;
  using SetupHandler =
      ThriftServerSetupHandler<channel_pipeline::detail::ContextImpl>;
  using MuxHandler =
      ThriftServerStreamMuxHandler<channel_pipeline::detail::ContextImpl>;
  using ConcurrencyHandler = ThriftServerConcurrencyControlHandler<
      channel_pipeline::detail::ContextImpl>;
  PipelineOwner thriftPipeline;
  if constexpr (Mode == ChannelPipelineMode::Static) {
    CHECK(!transportNegotiationHandler)
        << "Static server cannot install a dynamic negotiation handler";
    thriftPipeline = selectStaticThriftStats(
        config_,
        evb,
        transportAdapterPtr,
        tailAdapter,
        &conn.thriftAllocator,
        std::move(connContext),
        conn.extensionStates,
        statsShard);
  } else {
    auto& readBackpressureArbiter = connContext->readBackpressureArbiter();
    PipelineBuilder<
        ThriftServerTransportAdapter,
        TailAdapter,
        SimpleBufferAllocator,
        Storage>
        thriftPipelineBuilder;
    thriftPipelineBuilder.setEventBase(evb)
        .setHead(transportAdapterPtr)
        .setTail(tailAdapter)
        .setAllocator(&conn.thriftAllocator);
    // Sits closest to the head so it sees every message crossing the thrift
    // layer, before any handler below can absorb or synthesize one.
    if (statsShard != nullptr) {
      thriftPipelineBuilder.template addNextDuplex<
          ThriftMetricsHandler<Direction::Server, ServerStatsShard>>(
          thrift_metrics_handler_tag, statsShard);
    }
    // Duplex: inbound it creates and populates the per-request context;
    // outbound it hands that context's response headers to the outgoing
    // metadata. Sitting closest to the head on the write path makes it the
    // last contributor downstream of every handler and extension.
    thriftPipelineBuilder.template addNextDuplex<ReqCtxHandler>(
        thrift_server_request_context_handler_tag,
        config_.requestExtensionLayout.get(),
        config_.methodMetadata.get(),
        std::move(connContext),
        config_.enableRequestHeaders);
    // Inbound decompression precedes checksum verification. Outbound traverses
    // the handlers in reverse, so the checksum is computed on the uncompressed
    // response before compression.
    thriftPipelineBuilder.template addNextDuplex<CompressionHandler>(
        thrift_server_compression_handler_tag);
    // The checksum handler is added after the context handler so inbound it
    // runs once the per-request ThriftRequestContext exists (it records the
    // algorithm there for the response to echo).
    if (config_.enableChecksum) {
      thriftPipelineBuilder.template addNextDuplex<ChecksumHandler>(
          thrift_server_checksum_handler_tag);
    }
    // Connection-close handler sits immediately upstream of the tail.
    // ThriftServerConnection::close() fires
    // ThriftServerCloseConnectionEvent through the pipeline; the handler
    // handles it through its typed callback and drives the terminal state
    // machine.
    thriftPipelineBuilder.template addNextDuplex<CloseHandler>(
        thrift_server_connection_close_handler_tag,
        config_.drainTimeout,
        config_.reapTimeout);
    // Backpressure handler sits between the context handler and the drain
    // handler. Placed above drain (closer to head) so its inbound
    // Backpressure signal propagates upstream toward the transport, and
    // outbound responses from the tail traverse drain → backpressure →
    // head.
    thriftPipelineBuilder.template addNextDuplex<BackpressureHandler>(
        thrift_server_backpressure_handler_tag, &readBackpressureArbiter);
    // Embedder-registered handlers go after all built-ins, in registration
    // order — the first sits closest to the head, the last immediately above
    // the tail adapter. Each factory constructs a fresh per-connection
    // instance.
    for (const auto& registration : config_.thriftPipelineRegistrations) {
      registration.appendDynamic(
          conn.extensionStates,
          [&](channel_pipeline::detail::HandlerNode&& handler) {
            thriftPipelineBuilder.addErasedHandler(std::move(handler));
          });
    }
    if (transportNegotiationHandler) {
      thriftPipelineBuilder.addErasedHandler(
          std::move(*transportNegotiationHandler));
    }
    // Last before the tail, and deliberately after the embedder handlers: this
    // terminates the connection-lifecycle messages, so everything that might
    // answer one has to run first. The application tail then only ever sees
    // requests.
    thriftPipelineBuilder.template addNextDuplex<SetupHandler>(
        thrift_server_setup_handler_tag);
    if (config_.requestConcurrencyLimits) {
      const auto& limits = *config_.requestConcurrencyLimits;
      thriftPipelineBuilder.template addNextDuplex<ConcurrencyHandler>(
          thrift_server_concurrency_control_handler_tag,
          limits.maxActiveRequests,
          limits.maxOutstandingRequests,
          std::ref(*config_.concurrencyState));
    }
    if (config_.enableCancellation) {
      thriftPipelineBuilder.template addNextDuplex<RequestLifecycleHandler>(
          thrift_server_request_lifecycle_handler_tag);
    }
    // Stream mux sits immediately head-ward of the tail app adapter (tail-ward
    // of every built-in, including setup). Outbound it intercepts the app's
    // ThriftServerStreamOpenPayload to build/own the per-stream sub-pipeline
    // and emits stream chunks that traverse compression/checksum/transport like
    // a unary response; inbound it routes RequestN/Cancel to the matching
    // sub-pipeline and passes unary requests through to the app. It answers no
    // connection-lifecycle message, so keeping it below setup leaves the setup
    // handler's "app sees only requests" invariant intact.
    if (config_.enableStreamMux) {
      thriftPipelineBuilder.template addNextDuplex<MuxHandler>(
          thrift_server_stream_mux_handler_tag);
    }
    thriftPipeline = thriftPipelineBuilder.build();
  }
  transportAdapterPtr->setPipeline(thriftPipeline.get());
  tailAdapter->setPipeline(thriftPipeline.get());
  conn.thriftPipeline = std::move(thriftPipeline);

  return conn;
}

template <ChannelPipelineMode Mode>
template <typename Storage, typename TransportHandler>
PipelineOwner detail::ThriftServerConnectionFactoryT<Mode>::buildRocketPipeline(
    folly::EventBase* evb,
    TransportHandler* transportHandler,
    rocket::server::RocketServerAppAdapter* appAdapter,
    ServerStatsShard* FOLLY_NULLABLE statsShard) {
  if constexpr (Mode == ChannelPipelineMode::Static) {
    return detail::selectStaticRocketStats<
        channel_pipeline::NoHandlerOverrides>(
        config_,
        &rocketAllocator_,
        evb,
        transportHandler,
        appAdapter,
        statsShard);
  }

  else {
    // addState rebinds: it returns a builder of an extended type and leaves the
    // original moved-from, so the chain up to and including it must be bound
    // here rather than continued on a pre-declared builder.
    auto builder = PipelineBuilder<
                       TransportHandler,
                       rocket::server::RocketServerAppAdapter,
                       SimpleBufferAllocator,
                       Storage>()
                       .setEventBase(evb)
                       .setHead(transportHandler)
                       .setTail(appAdapter)
                       .setAllocator(&rocketAllocator_)
                       .template addState<rocket::RocketStreamContexts>();
    // The completion tracker turns the transport's per-writev completions into
    // one event per rocket-frame batch. The fragmentation handler below turns
    // that into the rocket-level completion relayed to the thrift pipeline.
    builder.template addNextOutbound<ServerBatchingFrameHandler>(
        batching_frame_handler_tag, config_.batchingConfig);
    builder
        .template addNextOutbound<
            frame::write::handler::FrameLengthEncoderHandler>(
            frame_length_encoder_handler_tag)
        .template addNextDuplex<frame::handler::FrameCodecHandler>(
            frame_codec_handler_tag)
        .template addNextInbound<
            frame::read::handler::FrameDefragmentationHandler>(
            frame_defragmentation_handler_tag);
    // Fragmenter composed with the fragment-completion tracker. It records each
    // frame's streamId on the way down — below this handler the frame is
    // serialized bytes and the stream is no longer recoverable — and fans the
    // batcher's batch event back out into one completion per original frame.
    //
    // It holds no frame between writes. While it splits a frame, the fragments
    // it has not written yet are invisible from below, so it is the only
    // handler that can narrow the batcher's quiescence verdict.
    builder.template addNextOutbound<ServerFragmentationFrameHandler>(
        frame_fragmentation_handler_tag);
    builder
        // Restates the fragmenter's frame-layer completion as the rocket
        // layer's own, so nothing above the rocket boundary subscribes to a
        // frame event.
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
            rocket::server::handler::RocketServerStreamStateHandler>(
            server_stream_state_handler_tag, config_.enableCancellation)
        .template addNextDuplex<
            rocket::server::handler::RocketServerRequestResponseHandler>(
            server_request_response_frame_handler_tag);
    // Sits closest to the tail, so inbound it counts frames that survived
    // parsing/defragmentation and outbound it counts frames as the app emits
    // them, before batching or fragmentation can change the frame count.
    if (statsShard != nullptr) {
      builder.template addNextDuplex<
          RocketMetricsHandler<Direction::Server, ServerStatsShard>>(
          rocket_metrics_handler_tag, statsShard);
    }
    return PipelineOwner(builder.build());
  }
}

template class detail::ThriftServerConnectionFactoryT<
    ChannelPipelineMode::Static>;
template class detail::ThriftServerConnectionFactoryT<
    ChannelPipelineMode::Dynamic>;

} // namespace apache::thrift::fast_thrift::thrift::server
