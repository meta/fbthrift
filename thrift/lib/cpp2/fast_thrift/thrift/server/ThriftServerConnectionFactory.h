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

#include <chrono>
#include <cstddef>
#include <memory>
#include <optional>
#include <variant>
#include <vector>

#include <boost/intrusive_ptr.hpp>
#include <folly/CppAttributes.h>
#include <folly/Executor.h>
#include <folly/SocketAddress.h>
#include <folly/io/async/AsyncTransport.h>
#include <thrift/lib/cpp2/fast_thrift/connection/common/Messages.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/ExtensionSlots.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/ThriftConnContext.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/BufferAllocator.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineRef.h>
#include <thrift/lib/cpp2/fast_thrift/common/ServerStats.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/IntervalBatchingHandlerConfig.h>
#include <thrift/lib/cpp2/fast_thrift/interface/control/ControlServerInterface.h>
#include <thrift/lib/cpp2/fast_thrift/interface/debug/DebugServerInterface.h>
#include <thrift/lib/cpp2/fast_thrift/interface/monitor/MonitoringServerInterface.h>
#include <thrift/lib/cpp2/fast_thrift/interface/security/SecurityServerInterface.h>
#include <thrift/lib/cpp2/fast_thrift/interface/status/StatusServerInterface.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/server/adapter/RocketServerAppAdapter.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/server/handler/RocketServerSetupFrameHandler.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/FastThriftServerConfig.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/RocketServerConnectionBuilder.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/adapter/ThriftServerAppAdapterFactory.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/adapter/ThriftServerTransportAdapter.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/adapter/util/ThriftServerCompositeRoutingTable.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/MethodMetadata.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/ThriftServerConnection.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/framework/ThriftPipelineHandler.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/util/ConcurrencyControlState.h>
#include <thrift/lib/cpp2/fast_thrift/transport/TransportHandler.h>
#include <thrift/lib/thrift/gen-cpp2/metadata_types.h>

namespace apache::thrift::fast_thrift::thrift::server {

/**
 * Configuration consumed by ThriftServerConnectionFactory. Captured once at
 * construction time; the factory installs the resulting handler / aux
 * interfaces / metadata into every connection it builds.
 */
struct ThriftServerConnectionFactoryConfig {
  std::shared_ptr<ThriftServerAppAdapterFactory> handler;
  RocketServerConnectionBuilder rocketConnectionBuilder;
  // Executor for request deserialization, method execution, and response
  // serialization. Null keeps that service path on the connection's EventBase.
  //
  // Applied to the user handler and to the monitoring / status / debug /
  // security aux interfaces alike. Methods that must stay on the EventBase —
  // the liveness probe and the counter scrapes — are pinned per-method in
  // their IDLs with
  // @cpp.ProcessInEbThreadUnsafe, mirroring what the legacy stack pins in
  // common/thrift/thrift/status.thrift and fb303/thrift/fb303_core.thrift.
  //
  // Metadata is the exception: MetadataAppAdapter is hand-written, never
  // consults cpuExecutor(), and completes on the EventBase. See the comment
  // at its construction site in buildCompositeConnection.
  folly::Executor::KeepAlive<> cpuExecutor;
  std::shared_ptr<fast_thrift::MonitoringServerInterface> monitoringHandler;
  std::shared_ptr<fast_thrift::StatusServerInterface> statusHandler;
  std::shared_ptr<fast_thrift::DebugServerInterface> debugHandler;
  std::shared_ptr<fast_thrift::ControlServerInterface> controlHandler;
  std::shared_ptr<fast_thrift::SecurityServerInterface> securityHandler;
  std::shared_ptr<const apache::thrift::metadata::ThriftServiceMetadataResponse>
      metadataResponse;
  // Per-connection MSG_ZEROCOPY threshold; 0 disables zero-copy.
  std::size_t zeroCopyThreshold{0};

  // Slot plans for the extensions installed on this server, built once before
  // it accepts anything and shared by every context. Null when nothing
  // registered, in which case contexts carry no slots and allocate nothing.
  std::shared_ptr<const ExtensionLayout> connExtensionLayout;
  std::shared_ptr<const ExtensionLayout> requestExtensionLayout;
  std::shared_ptr<const ThriftServerMethodMetadataRegistry> methodMetadata;

  // When true, populate each request's ThriftRequestContext with the inbound
  // custom headers (RequestRpcMetadata.otherMetadata).
  bool enableRequestHeaders{false};

  // When true, insert ThriftServerChecksumHandler to validate the inbound
  // request checksum and, when the request carried one, fill a matching
  // checksum on the response. The response algorithm is carried on the
  // per-request ThriftRequestContext.
  bool enableChecksum{false};

  // When true, install request lifecycle arbitration and expose cancellable
  // tokens to service handlers. Disabled requests retain an invalid source.
  bool enableCancellation{false};

  std::optional<RequestConcurrencyLimits> requestConcurrencyLimits;
  std::shared_ptr<ConcurrencyControlStateHolder> concurrencyState;

  // When true, insert ThriftServerStreamMuxHandler immediately head-ward of the
  // tail app adapter, so it owns/multiplexes per-stream producing-end
  // sub-pipelines (converts the app's outbound stream-open, routes inbound
  // RequestN/Cancel). Unary traffic passes through; needs no per-request
  // context.
  bool enableStreamMux{false};

  // Outbound write batching. Default zero-interval flushes via LoopCallback
  // at end of each event loop iteration. Set batchingInterval > 0 to use
  // an HHWheelTimer-driven flush instead.
  frame::write::IntervalBatchingHandlerConfig batchingConfig{};

  // Per-connection terminal-phase deadlines passed to
  // ThriftServerConnectionCloseHandler. Defaults mirror that handler's
  // kDefaultDrainTimeout / kDefaultReapTimeout. See FastThriftServerConfig
  // for semantics.
  std::chrono::milliseconds drainTimeout{std::chrono::seconds{30}};
  std::chrono::milliseconds reapTimeout{std::chrono::seconds{60}};

  // Ordered factories for embedder-registered thrift pipeline handlers. Each
  // factory produces a fresh dynamic or static handler per connection; they are
  // spliced into the thrift pipeline after all built-in handlers, in
  // registration order — first registered sits closest to the head, last
  // registered sits immediately above the tail app adapter.
  std::vector<ThriftPipelineRegistration> thriftPipelineRegistrations;

  // When set, insert RocketMetricsHandler / ThriftMetricsHandler into the
  // rocket and thrift pipelines, counting messages and errors at each layer
  // into the accepting IO thread's shard. Null leaves both handlers out
  // entirely, so a server without stats pays nothing for them.
  //
  std::shared_ptr<ServerStats> stats;

  // See FastThriftServerConfig::useAlignedParser.
  bool useAlignedParser{false};
};

/**
 * Builds a fully-wired server connection for each accepted socket.
 * Its public API does not expose parser or tail-adapter types.
 */
namespace detail {

template <ChannelPipelineMode Mode>
class ThriftServerConnectionFactoryT {
 public:
  explicit ThriftServerConnectionFactoryT(
      ThriftServerConnectionFactoryConfig config);

  /**
   * Build a connection for `socket`, whose peer was at `clientAddr` when the
   * socket was accepted and proved `peerSecurity` (null if it proved nothing).
   * Constructs the per-connection ThriftConnContext, builds the rocket +
   * thrift pipelines, and fires `onConnect()` before returning.
   *
   * Satisfies the connection::ConnectionFactory concept.
   */
  ThriftServerConnection getConnection(
      folly::AsyncTransport::UniquePtr socket,
      const folly::SocketAddress& clientAddr,
      const std::shared_ptr<const connection::PeerSecurityInfo>& peerSecurity);

 private:
  // Builds the rocket pipeline (TransportHandler ... RocketServerAppAdapter).
  // Same shape as the legacy FastThriftServer::buildRocketPipeline; lives
  // here because the factory owns the shared rocket allocator.
  // `statsShard` is null when config_.stats is unset, in which case no
  // metrics handler is added.
  template <typename Storage, typename TransportHandler>
  channel_pipeline::PipelineOwner buildRocketPipeline(
      folly::EventBase* evb,
      TransportHandler* transportHandler,
      rocket::server::RocketServerAppAdapter* appAdapter,
      ServerStatsShard* FOLLY_NULLABLE statsShard);

  // Hands the configured CPU pool to a user adapter. No-op when no pool is
  // configured, leaving the adapter dispatching inline on its EventBase.
  void attachCPUExecutor(ThriftServerAppAdapter& adapter) const;

  ThriftServerConnection buildSimpleConnection(
      folly::AsyncTransport::UniquePtr socket,
      const folly::SocketAddress& clientAddr,
      boost::intrusive_ptr<ThriftConnContext> connContext);

  ThriftServerConnection buildCompositeConnection(
      folly::AsyncTransport::UniquePtr socket,
      const folly::SocketAddress& clientAddr,
      boost::intrusive_ptr<ThriftConnContext> connContext);

  // Shared body for the two builders above. Templated on the thrift-pipeline
  // tail type because PipelineBuilder<Head, Tail, Allocator> needs the
  // concrete tail at build time. Defined in the .cpp; implicitly instantiated
  // for ThriftServerAppAdapter and ThriftServerCompositeAppAdapter when
  // buildSimpleConnection / buildCompositeConnection call it from the same TU.
  template <typename TailAdapter, typename Storage>
  ThriftServerConnection buildConnectionImpl(
      folly::AsyncTransport::UniquePtr socket,
      const folly::SocketAddress& clientAddr,
      std::variant<
          std::monostate,
          ThriftServerConnection::SimpleTail,
          ThriftServerConnection::CompositeTail> tail,
      TailAdapter* tailAdapter,
      boost::intrusive_ptr<ThriftConnContext> connContext);

  ThriftServerConnectionFactoryConfig config_;
  // Computed once from config_ in the ctor: true iff any aux interface or
  // metadata response is wired (i.e. the thrift tail must be a composite).
  bool needsComposite_;
  // Shared by every composite connection. Null only when a hand-written
  // adapter factory does not expose an immutable method table.
  std::shared_ptr<const ThriftServerCompositeRoutingTable> compositeRoutes_;
  // Shared across all rocket pipelines built by this factory, matching the
  // pre-refactor behavior where FastThriftServer held a single
  // rocketAllocator_.
  channel_pipeline::SimpleBufferAllocator rocketAllocator_;
};

} // namespace detail

using ThriftServerConnectionFactory =
    detail::ThriftServerConnectionFactoryT<ChannelPipelineMode::Static>;
using DynamicThriftServerConnectionFactory =
    detail::ThriftServerConnectionFactoryT<ChannelPipelineMode::Dynamic>;

} // namespace apache::thrift::fast_thrift::thrift::server
