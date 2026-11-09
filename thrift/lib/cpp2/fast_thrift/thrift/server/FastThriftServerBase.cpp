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

#include <thrift/lib/cpp2/fast_thrift/thrift/server/FastThriftServerBase.h>

#include <chrono>
#include <csignal>
#include <stdexcept>
#include <utility>
#include <vector>

#include <fmt/core.h>

#include <folly/Executor.h>
#include <folly/Function.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/task_queue/StripedPriorityUnboundedBlockingQueue.h>
#include <folly/io/async/AsyncServerSocket.h>
#include <folly/io/async/AsyncSignalHandler.h>
#include <folly/io/async/DelayedDestruction.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/ScopedEventBaseThread.h>
#include <folly/logging/xlog.h>

#include <thrift/lib/cpp2/fast_thrift/connection/common/ConnectionStats.h>
#include <thrift/lib/cpp2/fast_thrift/connection/security/common/TLSStats.h>
#include <thrift/lib/cpp2/fast_thrift/security/FizzServerContextBuilder.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/ThriftServerConnectionFactory.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/adapter/MetadataAppAdapter.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/event_handler/TProcessorEventHandlerBridge.h>
#include <thrift/lib/cpp2/schema/detail/Merge.h>

namespace apache::thrift::fast_thrift::thrift {

using channel_pipeline::PipelineBuilder;
using channel_pipeline::PipelineImpl;
using channel_pipeline::SimpleBufferAllocator;

namespace {
constexpr std::string_view kEventHandlerBridgeName{
    "__cpp2_event_handler_bridge"};
constexpr std::string_view kPendingConnectionQueueTimeout{
    "Exceeded deadline for accepting connection"};
} // namespace

class detail::FastThriftServerBase::PendingConnectionEventCallback final
    : public folly::AsyncServerSocket::ConnectionEventCallback {
 public:
  using Callbacks = FastServerModule::PendingConnectionCallbacks;

  explicit PendingConnectionEventCallback(std::vector<Callbacks> callbacks)
      : callbacks_(std::move(callbacks)) {}

  void onConnectionAccepted(
      folly::NetworkSocket, const folly::SocketAddress&) noexcept override {}

  void onConnectionAcceptError(int) noexcept override {}

  void onConnectionDropped(
      folly::NetworkSocket,
      const folly::SocketAddress&,
      const std::string& errorMsg) noexcept override {
    if (errorMsg.find(kPendingConnectionQueueTimeout) == std::string::npos) {
      return;
    }
    for (const auto& callbacks : callbacks_) {
      callbacks.droppedWhileQueued();
    }
  }

  void onConnectionEnqueuedForAcceptorCallback(
      folly::NetworkSocket, const folly::SocketAddress&) noexcept override {
    for (const auto& callbacks : callbacks_) {
      callbacks.enqueued();
    }
  }

  void onConnectionDequeuedByAcceptorCallback(
      folly::NetworkSocket, const folly::SocketAddress&) noexcept override {
    for (const auto& callbacks : callbacks_) {
      callbacks.dequeued();
    }
  }

  void onBackoffStarted() noexcept override {}
  void onBackoffEnded() noexcept override {}
  void onBackoffError() noexcept override {}

 private:
  const std::vector<Callbacks> callbacks_;
};

detail::FastThriftServerBase::FastThriftServerBase(
    FastThriftServerConfig config, InstallConnectionFactory installFactory)
    : installFactory_(installFactory),
      config_(std::move(config)),
      methodMetadataRegistry_(
          std::make_shared<ThriftServerMethodMetadataRegistry>()) {
  rebuildMethodMetadataRegistry();
}

void detail::FastThriftServerBase::rebuildMethodMetadataRegistry() {
  methodMetadataRegistry_->clear();
  if (handler_) {
    handler_->populateMethodMetadata(*methodMetadataRegistry_);
  }
  if (auxInterfaces_.monitoringHandler) {
    auxInterfaces_.monitoringHandler->populateMethodMetadata(
        *methodMetadataRegistry_);
  }
  if (auxInterfaces_.statusHandler) {
    auxInterfaces_.statusHandler->populateMethodMetadata(
        *methodMetadataRegistry_);
  }
  if (auxInterfaces_.debugHandler) {
    auxInterfaces_.debugHandler->populateMethodMetadata(
        *methodMetadataRegistry_);
  }
  if (auxInterfaces_.controlHandler) {
    auxInterfaces_.controlHandler->populateMethodMetadata(
        *methodMetadataRegistry_);
  }
  if (auxInterfaces_.securityHandler) {
    auxInterfaces_.securityHandler->populateMethodMetadata(
        *methodMetadataRegistry_);
  }
  if (config_.enableMetadataService) {
    methodMetadataRegistry_->add(MetadataAppAdapter::methodMetadata());
  }
}

void detail::FastThriftServerBase::setInterface(
    std::shared_ptr<ThriftServerAppAdapterFactory> handler) {
  std::lock_guard<std::mutex> lock(lifecycleMutex_);
  CHECK(state_ == State::kNotStarted)
      << "FastThriftServer::setInterface must be called before start()/serve()";
  CHECK(handler)
      << "FastThriftServer::setInterface requires a non-null handler";
  CHECK(!handler_)
      << "FastThriftServer::setInterface called more than once; only a single "
         "handler is supported today";
  handler_ = std::move(handler);
  rebuildMethodMetadataRegistry();
}

void detail::FastThriftServerBase::setMonitoringInterface(
    std::shared_ptr<fast_thrift::MonitoringServerInterface> handler) {
  std::lock_guard<std::mutex> lock(lifecycleMutex_);
  CHECK(state_ == State::kNotStarted)
      << "FastThriftServer::setMonitoringInterface must be called before "
         "start()/serve()";
  CHECK(handler)
      << "FastThriftServer::setMonitoringInterface requires a non-null handler";
  CHECK(!auxInterfaces_.monitoringHandler)
      << "FastThriftServer::setMonitoringInterface called more than once";
  auxInterfaces_.monitoringHandler = std::move(handler);
  rebuildMethodMetadataRegistry();
}

void detail::FastThriftServerBase::setStatusInterface(
    std::shared_ptr<fast_thrift::StatusServerInterface> handler) {
  std::lock_guard<std::mutex> lock(lifecycleMutex_);
  CHECK(state_ == State::kNotStarted)
      << "FastThriftServer::setStatusInterface must be called before "
         "start()/serve()";
  CHECK(handler)
      << "FastThriftServer::setStatusInterface requires a non-null handler";
  CHECK(!auxInterfaces_.statusHandler)
      << "FastThriftServer::setStatusInterface called more than once";
  auxInterfaces_.statusHandler = std::move(handler);
  rebuildMethodMetadataRegistry();
}

void detail::FastThriftServerBase::setDebugInterface(
    std::shared_ptr<fast_thrift::DebugServerInterface> handler) {
  std::lock_guard<std::mutex> lock(lifecycleMutex_);
  CHECK(state_ == State::kNotStarted)
      << "FastThriftServer::setDebugInterface must be called before "
         "start()/serve()";
  CHECK(handler)
      << "FastThriftServer::setDebugInterface requires a non-null handler";
  CHECK(!auxInterfaces_.debugHandler)
      << "FastThriftServer::setDebugInterface called more than once";
  auxInterfaces_.debugHandler = std::move(handler);
  rebuildMethodMetadataRegistry();
}

void detail::FastThriftServerBase::setControlInterface(
    std::shared_ptr<fast_thrift::ControlServerInterface> handler) {
  std::lock_guard<std::mutex> lock(lifecycleMutex_);
  CHECK(state_ == State::kNotStarted)
      << "FastThriftServer::setControlInterface must be called before "
         "start()/serve()";
  CHECK(handler)
      << "FastThriftServer::setControlInterface requires a non-null handler";
  CHECK(!auxInterfaces_.controlHandler)
      << "FastThriftServer::setControlInterface called more than once";
  auxInterfaces_.controlHandler = std::move(handler);
  rebuildMethodMetadataRegistry();
}

void detail::FastThriftServerBase::setSecurityInterface(
    std::shared_ptr<fast_thrift::SecurityServerInterface> handler) {
  std::lock_guard<std::mutex> lock(lifecycleMutex_);
  CHECK(state_ == State::kNotStarted)
      << "FastThriftServer::setSecurityInterface must be called before "
         "start()/serve()";
  CHECK(handler)
      << "FastThriftServer::setSecurityInterface requires a non-null handler";
  CHECK(!auxInterfaces_.securityHandler)
      << "FastThriftServer::setSecurityInterface called more than once";
  auxInterfaces_.securityHandler = std::move(handler);
  rebuildMethodMetadataRegistry();
}

void detail::FastThriftServerBase::setStats(
    std::shared_ptr<ServerStats> stats) {
  std::lock_guard<std::mutex> lock(lifecycleMutex_);
  CHECK(state_ == State::kNotStarted)
      << "FastThriftServer::setStats must be called before start()/serve()";
  CHECK(stats) << "FastThriftServer::setStats requires non-null stats";
  CHECK(!stats_) << "FastThriftServer::setStats called more than once";
  stats_ = std::move(stats);
}

void detail::FastThriftServerBase::setRocketConnectionBuilderImpl(
    server::RocketServerConnectionBuilder builder) {
  std::lock_guard<std::mutex> lock(lifecycleMutex_);
  CHECK(state_ == State::kNotStarted)
      << "FastThriftServer::setRocketConnectionBuilder must be called before "
         "start()/serve()";
  CHECK(builder)
      << "FastThriftServer::setRocketConnectionBuilder requires a non-null "
         "builder";
  CHECK(!rocketConnectionBuilder_)
      << "FastThriftServer::setRocketConnectionBuilder called more than once";
  rocketConnectionBuilder_ = std::move(builder);
}

void detail::FastThriftServerBase::setConnectionStats(
    std::shared_ptr<connection::ConnectionStats> stats) {
  std::lock_guard<std::mutex> lock(lifecycleMutex_);
  CHECK(state_ == State::kNotStarted)
      << "FastThriftServer::setConnectionStats must be called before "
         "start()/serve()";
  CHECK(stats)
      << "FastThriftServer::setConnectionStats requires non-null stats";
  CHECK(!connectionStats_)
      << "FastThriftServer::setConnectionStats called more than once";
  connectionStats_ = std::move(stats);
}

void detail::FastThriftServerBase::setTLSStats(
    std::shared_ptr<connection::security::TLSStats> stats) {
  std::lock_guard<std::mutex> lock(lifecycleMutex_);
  CHECK(state_ == State::kNotStarted)
      << "FastThriftServer::setTLSStats must be called before start()/serve()";
  CHECK(stats) << "FastThriftServer::setTLSStats requires non-null stats";
  CHECK(!tlsStats_) << "FastThriftServer::setTLSStats called more than once";
  tlsStats_ = std::move(stats);
}

void detail::FastThriftServerBase::addNativeThriftPipelineHandlersImpl(
    std::vector<server::ThriftPipelineHandlerFactory> factories) {
  std::lock_guard<std::mutex> lock(lifecycleMutex_);
  CHECK(state_ == State::kNotStarted)
      << "FastThriftServer::addNativeThriftPipelineHandlers must be called "
         "before start()/serve()";
  thriftPipelineRegistrations_.reserve(
      thriftPipelineRegistrations_.size() + factories.size());
  for (auto& factory : factories) {
    thriftPipelineRegistrations_.emplace_back(std::move(factory));
  }
}

void detail::FastThriftServerBase::setClientIdentityHook(
    apache::thrift::ClientIdentityHook hook) {
  std::lock_guard<std::mutex> lock(lifecycleMutex_);
  if (state_ != State::kNotStarted) {
    throw std::logic_error(
        "FastThriftServer::setClientIdentityHook must be called before "
        "start()/serve()");
  }
  clientIdentityHook_ = std::move(hook);
}

void detail::FastThriftServerBase::addProcessorEventHandler(
    std::shared_ptr<apache::thrift::TProcessorEventHandler> eventHandler) {
  std::lock_guard<std::mutex> lock(lifecycleMutex_);
  if (state_ != State::kNotStarted) {
    throw std::logic_error(
        "FastThriftServer::addProcessorEventHandler must be called before "
        "start()/serve()");
  }
  if (!eventHandler) {
    throw std::logic_error(
        "FastThriftServer::addProcessorEventHandler requires a non-null "
        "handler");
  }
  eventHandlers_.processor.push_back(std::move(eventHandler));
}

void detail::FastThriftServerBase::addServerEventHandler(
    std::shared_ptr<apache::thrift::server::TServerEventHandler> eventHandler) {
  std::lock_guard<std::mutex> lock(lifecycleMutex_);
  if (state_ != State::kNotStarted) {
    throw std::logic_error(
        "FastThriftServer::addServerEventHandler must be called before "
        "start()/serve()");
  }
  if (!eventHandler) {
    throw std::logic_error(
        "FastThriftServer::addServerEventHandler requires a non-null handler");
  }
  eventHandlers_.server.push_back(std::move(eventHandler));
}

void detail::FastThriftServerBase::addModuleRegistrations(
    std::string name,
    bool requiresHeaders,
    std::vector<FastServerModule::PendingConnectionCallbacks>
        pendingConnectionCallbacks,
    const std::vector<FastServerModule::RequestStateRegistration>&
        requestStateRegistrations,
    std::vector<server::ThriftPipelineRegistration> registrations) {
  std::lock_guard<std::mutex> lock(lifecycleMutex_);
  CHECK(state_ == State::kNotStarted)
      << "FastThriftServer::addModule must be called before start()/serve()";
  if (name.empty()) {
    // The empty namespace is reserved for top-level
    // addNativeThriftPipelineHandlers ids; rejecting empty module names keeps
    // module and top-level id streams disjoint.
    throw std::logic_error(
        "FastThriftServer::addModule: module name must be non-empty");
  }
  if (name == kEventHandlerBridgeName) {
    throw std::logic_error(
        "FastThriftServer::addModule: module name is reserved by the server");
  }
  if (moduleNames_.contains(name)) {
    throw std::logic_error(
        fmt::format(
            "FastThriftServer::addModule: duplicate module name: {}", name));
  }
  // Same posture for headers: they are reachable only through the per-request
  // context, and only this setting puts them there. An extension that gates on
  // a header it never receives would admit everything.
  if (requiresHeaders && !config_.enableRequestHeaders) {
    throw std::logic_error(
        fmt::format(
            "FastThriftServer::addModule: module '{}' registers an extension "
            "that uses headers, which requires enableRequestHeaders",
            name));
  }
  for (auto registration : requestStateRegistrations) {
    registration(requestExtensionBuilder_);
  }
  pendingConnectionCallbacks_.reserve(
      pendingConnectionCallbacks_.size() + pendingConnectionCallbacks.size());
  pendingConnectionCallbacks_.insert(
      pendingConnectionCallbacks_.end(),
      std::make_move_iterator(pendingConnectionCallbacks.begin()),
      std::make_move_iterator(pendingConnectionCallbacks.end()));
  thriftPipelineRegistrations_.reserve(
      thriftPipelineRegistrations_.size() + registrations.size());
  for (auto& registration : registrations) {
    thriftPipelineRegistrations_.push_back(std::move(registration));
  }
  // Claim the name only once the splice succeeded, so a throwing splice does
  // not leave the name permanently reserved against a retry.
  moduleNames_.insert(std::move(name));
}
void detail::FastThriftServerBase::setOnConnectionAccepted(
    OnConnectionAcceptedFn cb) {
  std::lock_guard<std::mutex> lock(lifecycleMutex_);
  CHECK(state_ == State::kNotStarted)
      << "FastThriftServer::setOnConnectionAccepted must be called before "
         "start()/serve()";
  onConnectionAccepted_ = std::move(cb);
}

void detail::FastThriftServerBase::setSSLConfig(
    security::FizzServerCertConfig cfg) {
  std::lock_guard<std::mutex> lock(lifecycleMutex_);
  CHECK(state_ == State::kNotStarted)
      << "FastThriftServer::setSSLConfig must be called before start()/serve()";
  sslConfig_ = std::move(cfg);
}

void detail::FastThriftServerBase::setThriftConfig(
    security::ThriftTlsConfig cfg) {
  std::lock_guard<std::mutex> lock(lifecycleMutex_);
  CHECK(state_ == State::kNotStarted)
      << "FastThriftServer::setThriftConfig must be called before start()/serve()";
  thriftConfig_ = cfg;
}

void detail::FastThriftServerBase::setIOThreadPool(
    std::shared_ptr<folly::IOThreadPoolExecutorBase> pool) {
  std::lock_guard<std::mutex> lock(lifecycleMutex_);
  CHECK(state_ == State::kNotStarted)
      << "FastThriftServer::setIOThreadPool must be called before "
         "start()/serve()";
  CHECK(pool) << "FastThriftServer::setIOThreadPool requires a non-null pool";
  ioThreadPool_ = std::move(pool);
}

void detail::FastThriftServerBase::setCPUExecutor(
    folly::Executor::KeepAlive<> executor) {
  std::lock_guard<std::mutex> lock(lifecycleMutex_);
  CHECK(state_ == State::kNotStarted)
      << "FastThriftServer::setCPUExecutor must be called before "
         "start()/serve()";
  CHECK(executor)
      << "FastThriftServer::setCPUExecutor requires a non-null executor";
  cpuExecutor_ = std::move(executor);
}

void detail::FastThriftServerBase::setEnableReusePortBpfSpread(bool enable) {
  std::lock_guard<std::mutex> lock(lifecycleMutex_);
  CHECK(state_ == State::kNotStarted)
      << "FastThriftServer::setEnableReusePortBpfSpread must be called before "
         "start()/serve()";
  reusePortBpfMode_ = enable ? connection::ReusePortBpfMode::Random
                             : connection::ReusePortBpfMode::Disabled;
}

void detail::FastThriftServerBase::setReusePortBpfMode(
    connection::ReusePortBpfMode mode) {
  std::lock_guard<std::mutex> lock(lifecycleMutex_);
  CHECK(state_ == State::kNotStarted)
      << "FastThriftServer::setReusePortBpfMode must be called before "
         "start()/serve()";
  reusePortBpfMode_ = mode;
}

void detail::FastThriftServerBase::setSocketOptions(
    connection::SocketOptions opts) {
  std::lock_guard<std::mutex> lock(lifecycleMutex_);
  CHECK(state_ == State::kNotStarted)
      << "FastThriftServer::setSocketOptions must be called before "
         "start()/serve()";
  socketOptions_ = std::move(opts);
}

void detail::FastThriftServerBase::setListeningSocketOption(
    folly::SocketOptionKey key, folly::SocketOptionValue value) {
  std::lock_guard<std::mutex> lock(lifecycleMutex_);
  CHECK(state_ == State::kNotStarted)
      << "FastThriftServer::setListeningSocketOption must be called before "
         "start()/serve()";
  socketOptions_.listeningSocketOptions.insert_or_assign(key, std::move(value));
}

void detail::FastThriftServerBase::reloadTLSConfig(
    security::FizzServerCertConfig cfg) {
  // Snapshot thriftConfig_ under the lock: setThriftConfig writes it under
  // the same mutex, and reloadTLSConfig is documented as safe from any
  // thread, so an unsynchronized read would be a TSAN data race even though
  // the lifecycle states normally exclude an overlap.
  security::ThriftTlsConfig thriftConfigSnapshot;
  {
    std::lock_guard<std::mutex> lock(lifecycleMutex_);
    CHECK(state_ == State::kRunning)
        << "FastThriftServer::reloadTLSConfig requires a running server "
           "(call setSSLConfig before start() for the initial config)";
    thriftConfigSnapshot = thriftConfig_;
  }

  // Build outside the lock — buildTLSParams may do file IO and may throw
  // on unreadable cert/CA files or invalid verifier config; holding the
  // lifecycle mutex across that would block start()/stop()/setters, and a
  // throw leaves the running server untouched.
  auto newParams = std::make_shared<const security::TLSParams>(
      security::buildTLSParams(cfg, thriftConfigSnapshot));

  std::lock_guard<std::mutex> lock(lifecycleMutex_);
  // Re-check: stop() may have raced in between the two acquisitions. Drop
  // the reload silently rather than touching a torn-down connectionManager_.
  if (state_ != State::kRunning) {
    return;
  }
  sslConfig_ = std::move(cfg);
  connectionManager_->setTLSParams(std::move(newParams));
}

detail::FastThriftServerBase::~FastThriftServerBase() {
  stop();

  // Destroy the connection manager first: its IOObserver removal fans
  // unregisterEventBase across every IO thread synchronously, draining
  // per-EVB ConnectionHandlers (and the ConnectionFactory closure that
  // captures `this`) before we return.
  connectionManager_.reset();
}

void detail::FastThriftServerBase::start() {
  std::lock_guard<std::mutex> lock(lifecycleMutex_);
  CHECK(handler_)
      << "FastThriftServer::start called before setInterface — no handler "
         "registered";
  if (state_ != State::kNotStarted) {
    return;
  }

  // Build the merged metadata response once if enabled, before accepting
  // any connections. Java does the same (eager build at startup); the
  // cached response is then served from every per-connection
  // MetadataAppAdapter and from DefaultDebug.dumpThriftServiceMetadata.
  //
  // Merge order matters. genServiceMetadataResponse() is additive for the
  // metadata maps + `services` list but *overwrites* the deprecated
  // `context` field. Call extra interfaces first, user handler last, so
  // the response's `context` reflects the user service (matching legacy
  // multiplex behaviour — extras are siblings, not the primary).
  //
  // Without the merge, a debug-client `sendRequest <monitor-method>` fails
  // with "Function not found" because the metadata lookup misses Monitor /
  // Status / Debug methods even though they are dispatchable on the wire.
  if (config_.enableMetadataService) {
    auto resp = std::make_shared<
        apache::thrift::metadata::ThriftServiceMetadataResponse>();
    if (auxInterfaces_.monitoringHandler) {
      auxInterfaces_.monitoringHandler->getServiceMetadata(*resp);
    }
    if (auxInterfaces_.statusHandler) {
      auxInterfaces_.statusHandler->getServiceMetadata(*resp);
    }
    if (auxInterfaces_.debugHandler) {
      auxInterfaces_.debugHandler->getServiceMetadata(*resp);
    }
    if (auxInterfaces_.controlHandler) {
      auxInterfaces_.controlHandler->getServiceMetadata(*resp);
    }
    if (auxInterfaces_.securityHandler) {
      auxInterfaces_.securityHandler->getServiceMetadata(*resp);
    }
    handler_->getServiceMetadata(*resp);
    metadataResponse_ = std::move(resp);
  }

  std::shared_ptr<const security::TLSParams> tlsParams;
  security::SSLPolicy sslPolicy = security::SSLPolicy::DISABLED;
  if (sslConfig_) {
    sslPolicy = sslConfig_->sslPolicy;
    if (sslPolicy != security::SSLPolicy::DISABLED) {
      tlsParams = std::make_shared<const security::TLSParams>(
          security::buildTLSParams(*sslConfig_, thriftConfig_));
    }
  }
  // Materialize the default IO pool only when the embedder didn't supply
  // one via setIOThreadPool.
  //
  // Fixed-size pool: minThreads == maxThreads so threads cannot idle out.
  // The single-arg IOThreadPoolExecutor(N) constructor sets minThreads=0
  // when FLAGS_dynamic_iothreadpoolexecutor is true (the default in many
  // production configs), causing threads to time out and join when their
  // EventBase has no work. Each fast_thrift IO thread only ever does
  // accept() on its own SO_REUSEPORT listening socket, so an EVB that
  // the kernel happens not to route accepts to looks idle and dies —
  // collapsing the configured numIOThreads to a small fraction (~8 of
  // 188 observed in production) and permanently bottlenecking the
  // server. The two-arg constructor with min == max disables this
  // dynamic-shrink behavior and pins the pool size for the process
  // lifetime.
  if (!ioThreadPool_) {
    ioThreadPool_ = std::make_shared<folly::IOThreadPoolExecutor>(
        /*maxThreads=*/config_.numIOThreads,
        /*minThreads=*/config_.numIOThreads);
  }
  if (config_.numConnectionSetupThreads > 0) {
    connectionSetupThreadPool_ = std::make_shared<folly::IOThreadPoolExecutor>(
        /*maxThreads=*/config_.numConnectionSetupThreads,
        /*minThreads=*/config_.numConnectionSetupThreads);
  }

  // Materialize the CPU pool only when asked for one and the embedder didn't
  // supply it. Leaving it null keeps handlers inline on the IO threads.
  if (!cpuExecutor_ && config_.numCPUThreads > 0) {
    ownedCPUThreadPool_ = std::make_shared<folly::CPUThreadPoolExecutor>(
        std::pair<size_t, size_t>{config_.numCPUThreads, config_.numCPUThreads},
        folly::StripedPriorityUnboundedBlockingQueue<
            folly::CPUThreadPoolExecutor::CPUTask>::
            create(
                /*numPriorities=*/1));
    cpuExecutor_ = folly::getKeepAliveToken(ownedCPUThreadPool_.get());
  }
  // Materialize the counters config_.enableStats asks for, leaving alone any
  // layer the embedder already supplied via the setStats family. Done here
  // rather than in the constructor so those setters, which reject a second
  // instance, still have every pre-start call to themselves.
  if (config_.enableStats) {
    if (!stats_) {
      stats_ = std::make_shared<ServerStats>();
    }
    if (!tlsStats_) {
      tlsStats_ = std::make_shared<connection::security::TLSStats>();
    }
  }
  if ((config_.enableStats || config_.enablePerEventBaseConnectionStats) &&
      !connectionStats_) {
    connectionStats_ = std::make_shared<connection::ConnectionStats>();
  }

  connectionManager_ = connection::ConnectionManager::create(
      config_.address,
      folly::getKeepAliveToken(ioThreadPool_.get()),
      sslPolicy,
      std::move(tlsParams),
      socketOptions_,
      folly::getKeepAliveToken(getConnectionSetupThreadPool().get()));
  connectionManager_->setReusePortBpfMode(reusePortBpfMode_);
  if (config_.numConnectionSetupThreads > 0 &&
      !pendingConnectionCallbacks_.empty()) {
    pendingConnectionEventCallback_ =
        std::make_shared<PendingConnectionEventCallback>(
            std::move(pendingConnectionCallbacks_));
    connectionManager_->setConnectionEventCallback(
        pendingConnectionEventCallback_);
  }
  connectionManager_->setConnectionStats(connectionStats_.get());
  connectionManager_->setTLSStats(tlsStats_.get());

  const bool hasClassicEventHandlers =
      !eventHandlers_.processor.empty() || !eventHandlers_.server.empty();
  if (hasClassicEventHandlers) {
    registerExtension<server::Cpp2BridgeExtension>();
    eventHandlers_.clientIdentityHook = clientIdentityHook_;
    auto handlers = std::make_shared<const server::TProcessorEventHandlers>(
        std::move(eventHandlers_));
    thriftPipelineRegistrations_.insert(
        thriftPipelineRegistrations_.begin(),
        server::ThriftPipelineRegistration(
            server::makeThriftPipelineHandlerFactory<
                server::TProcessorEventHandlerBridge<
                    channel_pipeline::detail::ContextImpl>,
                server::TProcessorEventHandlerBridge<
                    channel_pipeline::detail::StaticHandlerContext>>(
                server::deriveThriftPipelineHandlerId(
                    kEventHandlerBridgeName, /*index=*/0),
                server::TProcessorEventHandlerBridgeConfig{
                    .handlers = std::move(handlers),
                    .methodMetadata = methodMetadataRegistry_})));
  }

  // Wire the per-connection factory. The factory carries all per-EVB-handler
  // config (user handler, aux interfaces, metadata, zero-copy threshold,
  // request-context wiring). The embedder onConnectionAccepted hook (if any)
  // runs from the connection-layer ConnectionAcceptCallbackHandler, reaching
  // the per-connection ThriftConnContext via ThriftServerConnection.
  // Frozen here: every extension has registered by now, and both layouts are
  // shared unchanged by every connection and request from this point on.
  connExtensionLayout_ = std::make_shared<const ExtensionLayout>(
      std::move(connExtensionBuilder_).build());
  requestExtensionLayout_ = std::make_shared<const ExtensionLayout>(
      std::move(requestExtensionBuilder_).build());

  server::ThriftServerConnectionFactoryConfig factoryConfig{
      .handler = handler_,
      .rocketConnectionBuilder = std::move(rocketConnectionBuilder_),
      .cpuExecutor = cpuExecutor_,
      .monitoringHandler = auxInterfaces_.monitoringHandler,
      .statusHandler = auxInterfaces_.statusHandler,
      .debugHandler = auxInterfaces_.debugHandler,
      .controlHandler = auxInterfaces_.controlHandler,
      .securityHandler = auxInterfaces_.securityHandler,
      .metadataResponse = metadataResponse_,
      .zeroCopyThreshold = config_.zeroCopyThreshold,
      .connExtensionLayout = connExtensionLayout_,
      .requestExtensionLayout = requestExtensionLayout_,
      .methodMetadata = methodMetadataRegistry_,
      .enableRequestHeaders = config_.enableRequestHeaders,
      .enableChecksum = config_.enableChecksum,
      .enableCancellation = config_.enableCancellation,
      .requestConcurrencyLimits = config_.requestConcurrencyLimits,
      .concurrencyState = nullptr,
      .enableStreamMux = config_.enableStreamMux,
      .batchingConfig = config_.batchingConfig,
      .drainTimeout = config_.drainTimeout,
      .reapTimeout = config_.reapTimeout,
      .thriftPipelineRegistrations = thriftPipelineRegistrations_,
      .stats = stats_,
      .useAlignedParser = config_.useAlignedParser,
  };
  std::function<void(server::ThriftServerConnection&)> onAccept;
  if (onConnectionAccepted_) {
    onAccept = [this](server::ThriftServerConnection& conn) {
      onConnectionAccepted_(conn.connContext.get());
    };
  }
  installFactory_(
      *connectionManager_, std::move(factoryConfig), std::move(onAccept));

  connectionManager_->start();
  state_ = State::kRunning;
  XLOG(INFO) << "FastThriftServer listening on "
             << connectionManager_->getAddress();
}

void detail::FastThriftServerBase::serve() {
  start();

  folly::ScopedEventBaseThread signalThread("FastThriftSignal");
  auto* evb = signalThread.getEventBase();

  folly::CallbackAsyncSignalHandler signalHandler(evb, [this](int signum) {
    XLOG(INFO) << "Received signal " << signum << ", shutting down...";
    stop();
  });

  evb->runInEventBaseThreadAndWait([&] {
    signalHandler.registerSignalHandler(SIGINT);
    signalHandler.registerSignalHandler(SIGTERM);
  });

  stopBaton_.wait();

  evb->runInEventBaseThreadAndWait([&] {
    signalHandler.unregisterSignalHandler(SIGINT);
    signalHandler.unregisterSignalHandler(SIGTERM);
  });
}

void detail::FastThriftServerBase::stop() {
  {
    std::lock_guard<std::mutex> lock(lifecycleMutex_);
    if (state_ != State::kRunning) {
      return;
    }
    state_ = State::kStopped;
  }

  // ConnectionManager::stop() handles the whole flow: stop accepting,
  // drain in-flight connections, force-close any stragglers.
  connectionManager_->stop();
  stopBaton_.post();
}

folly::SocketAddress detail::FastThriftServerBase::getAddress() const {
  CHECK(connectionManager_)
      << "FastThriftServer::getAddress called before start() — use "
         "isRunning() to gate this call when iterating servers via "
         "FastThriftServerRegistry";
  return connectionManager_->getAddress();
}

std::vector<folly::not_null<ThriftServerAppAdapterFactory*>>
detail::FastThriftServerBase::interfaces() const {
  std::vector<folly::not_null<ThriftServerAppAdapterFactory*>> result;
  for (ThriftServerAppAdapterFactory* factory :
       {static_cast<ThriftServerAppAdapterFactory*>(handler_.get()),
        static_cast<ThriftServerAppAdapterFactory*>(
            auxInterfaces_.monitoringHandler.get()),
        static_cast<ThriftServerAppAdapterFactory*>(
            auxInterfaces_.statusHandler.get()),
        static_cast<ThriftServerAppAdapterFactory*>(
            auxInterfaces_.debugHandler.get()),
        static_cast<ThriftServerAppAdapterFactory*>(
            auxInterfaces_.controlHandler.get()),
        static_cast<ThriftServerAppAdapterFactory*>(
            auxInterfaces_.securityHandler.get())}) {
    if (factory != nullptr) {
      result.emplace_back(factory);
    }
  }
  return result;
}

std::optional<folly::SocketAddress>
detail::FastThriftServerBase::getAddressIfRunning() const {
  std::lock_guard<std::mutex> lock(lifecycleMutex_);
  if (state_ != State::kRunning) {
    return std::nullopt;
  }
  return connectionManager_->getAddress();
}

::apache::thrift::schema::DefinitionsSchema
detail::FastThriftServerBase::getServiceSchema() const {
  std::vector<::apache::thrift::type::Schema> schemas;
  ::apache::thrift::schema::DefinitionsSchema result;
  for (auto factory : interfaces()) {
    if (auto schema = factory->getServiceSchema()) {
      schemas.push_back(std::move(schema->schema));
      result.definitions.insert(
          result.definitions.end(),
          std::make_move_iterator(schema->definitions.begin()),
          std::make_move_iterator(schema->definitions.end()));
    }
  }
  // As MultiplexAsyncProcessorFactory merges a C++ ThriftServer's schemas.
  result.schema =
      // NOLINTNEXTLINE(facebook-hte-DetailCall)
      ::apache::thrift::schema::detail::mergeSchemas(std::move(schemas));
  return result;
}

std::vector<folly::not_null<const ::apache::thrift::syntax_graph::ServiceNode*>>
detail::FastThriftServerBase::getServiceSchemaNodes() const {
  std::vector<
      folly::not_null<const ::apache::thrift::syntax_graph::ServiceNode*>>
      result;
  for (auto factory : interfaces()) {
    auto nodes = factory->getServiceSchemaNodes();
    result.insert(result.end(), nodes.begin(), nodes.end());
  }
  return result;
}

} // namespace apache::thrift::fast_thrift::thrift
