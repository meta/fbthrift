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

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <folly/Executor.h>
#include <folly/SocketAddress.h>
#include <folly/container/F14Set.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/synchronization/Baton.h>

#include <thrift/lib/cpp2/fast_thrift/common/ServerStats.h>
#include <thrift/lib/cpp2/fast_thrift/connection/ConnectionManager.h>
#include <thrift/lib/cpp2/fast_thrift/connection/SocketOptions.h>
#include <thrift/lib/cpp2/fast_thrift/interface/control/ControlServerInterface.h>
#include <thrift/lib/cpp2/fast_thrift/interface/debug/DebugServerInterface.h>
#include <thrift/lib/cpp2/fast_thrift/interface/monitor/MonitoringServerInterface.h>
#include <thrift/lib/cpp2/fast_thrift/interface/security/SecurityServerInterface.h>
#include <thrift/lib/cpp2/fast_thrift/interface/status/StatusServerInterface.h>
#include <thrift/lib/cpp2/fast_thrift/security/FizzServerCertConfig.h>
#include <thrift/lib/cpp2/fast_thrift/security/ThriftTlsConfig.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/FastThriftServerConfig.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/FastThriftServerRegistry.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/RocketServerConnectionBuilder.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/adapter/ThriftServerAppAdapterFactory.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/ExtensionSlots.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/ThriftConnContext.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/event_handler/TProcessorEventHandlerBridge.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/framework/FastServerModule.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/framework/ThriftPipelineHandler.h>

namespace apache::thrift::fast_thrift::thrift {

class FastThriftServer;
class DynamicFastThriftServer;
template <typename Fragmenter>
class FastThriftServerWithFragmenter;

namespace server {
struct ThriftServerConnectionFactoryConfig;
struct ThriftServerConnection;
struct DefaultServerFragmenter;
} // namespace server
namespace detail {

class FastThriftServerBase {
 private:
  friend class ::apache::thrift::fast_thrift::thrift::FastThriftServer;
  friend class ::apache::thrift::fast_thrift::thrift::DynamicFastThriftServer;
  template <typename Fragmenter>
  friend class ::apache::thrift::fast_thrift::thrift::
      FastThriftServerWithFragmenter;

  template <ChannelPipelineMode Mode, typename Fragmenter>
  class FastThriftServerT;

 protected:
  using InstallConnectionFactory = void (*)(
      connection::ConnectionManager&,
      server::ThriftServerConnectionFactoryConfig,
      std::function<void(server::ThriftServerConnection&)>);
  FastThriftServerBase(
      FastThriftServerConfig config, InstallConnectionFactory installFactory);
  ~FastThriftServerBase();

 public:
  FastThriftServerBase(const FastThriftServerBase&) = delete;
  FastThriftServerBase& operator=(const FastThriftServerBase&) = delete;
  FastThriftServerBase(FastThriftServerBase&&) = delete;
  FastThriftServerBase& operator=(FastThriftServerBase&&) = delete;

  /**
   * Attach the generated handler. Must be called before start()/serve().
   * User passes shared_ptr<MyHandler> — implicit upcast to
   * ThriftServerAppAdapterFactory.
   */
  void setInterface(std::shared_ptr<ThriftServerAppAdapterFactory> handler);

  /**
   * Returns the application interface factory registered by setInterface(),
   * or nullptr if no interface has been registered.
   *
   * This is the fast_thrift counterpart of ThriftServer::getProcessorFactory:
   * integrations can inspect the generated service metadata and schema before
   * start() without including server-provided auxiliary interfaces.
   */
  const std::shared_ptr<ThriftServerAppAdapterFactory>& getAppAdapterFactory()
      const noexcept {
    return handler_;
  }

  /**
   * Returns the methods currently dispatchable by this server. The registry
   * is updated as interfaces are configured and frozen when the server starts.
   */
  std::shared_ptr<const ThriftServerMethodMetadataRegistry>
  getMethodMetadataRegistry() const noexcept {
    return methodMetadataRegistry_;
  }

  /**
   * Attach an additional monitoring/debug handler. Methods on the monitoring
   * handler are dispatched on the same connection as the user handler;
   * routing is by method name with the user handler winning on conflict
   * (mirrors ThriftServer::setMonitoringInterface). Must be called before
   * start()/serve().
   *
   * The handler must derive from fast_thrift::MonitoringServerInterface — a
   * marker base that exists purely as a type-system guardrail to prevent
   * accidentally passing a user-facing handler here.
   *
   */
  void setMonitoringInterface(
      std::shared_ptr<fast_thrift::MonitoringServerInterface> handler);

  /**
   * Attach a Status handler. Methods on the status handler are dispatched
   * on the same connection as the user handler; routing is by method name
   * with the user handler winning on conflict (mirrors
   * ThriftServer::setStatusInterface). Must be called before
   * start()/serve().
   *
   * Container scheduler health checks call `getStatus()` on this interface.
   *
   * The handler must derive from fast_thrift::StatusServerInterface — a
   * marker base that exists purely as a type-system guardrail.
   */
  void setStatusInterface(
      std::shared_ptr<fast_thrift::StatusServerInterface> handler);

  /**
   * Attach a Debug handler. Methods on the debug handler are dispatched
   * on the same connection as the user handler; routing is by method name
   * with the user handler winning on conflict (mirrors
   * ThriftServer::setDebugInterface). Must be called before
   * start()/serve().
   *
   * Debug RPC clients (`sendRequest`, `getServerDbgInfo`, and the `info`
   * TUI) call into this interface.
   *
   * The handler must derive from fast_thrift::DebugServerInterface — a
   * marker base that exists purely as a type-system guardrail.
   */
  void setDebugInterface(
      std::shared_ptr<fast_thrift::DebugServerInterface> handler);

  /**
   * Attach a Control handler. Methods on the control handler are dispatched
   * on the same connection as the user handler; routing is by method name
   * with the user handler winning on conflict (mirrors
   * ThriftServer::setControlInterface). Must be called before
   * start()/serve().
   *
   * Reads and mutates the environment the server runs in — options, gflags,
   * settings. Nothing installs one by default on either stack, so a server
   * that wants this surface wires it explicitly.
   *
   * The handler must derive from fast_thrift::ControlServerInterface — a
   * marker base that exists purely as a type-system guardrail. No IDL ships
   * with it; see that header.
   */
  void setControlInterface(
      std::shared_ptr<fast_thrift::ControlServerInterface> handler);

  /**
   * Attach a Security handler. Methods on the security handler are dispatched
   * on the same connection as the user handler; routing is by method name
   * with the user handler winning on conflict (mirrors
   * ThriftServer::setSecurityInterface). Must be called before
   * start()/serve().
   *
   * Security-metadata introspection clients call into this interface.
   *
   * The handler must derive from fast_thrift::SecurityServerInterface — a
   * marker base that exists purely as a type-system guardrail. See that
   * header for the service-naming contract an authorization gate relies on.
   */
  void setSecurityInterface(
      std::shared_ptr<fast_thrift::SecurityServerInterface> handler);

  /**
   * Attach server counters. Wires the rocket- and thrift-layer metrics
   * handlers into every connection built after this point; leaving it unset
   * omits both handlers, so a server without stats pays nothing. Must be
   * called before start()/serve().
   *
   * Only needed to supply a specific instance — FastThriftServerConfig::
   * enableStats materializes one (and the connection and TLS counters) at
   * start() for embedders that just want the counters to exist.
   *
   * Counters are sharded per EventBase and readable only from the owning
   * EventBase thread (see ServerStats). To publish them to fb303, hand the
   * same instance to a FastThriftStatsPublisher.
   */
  void setStats(std::shared_ptr<ServerStats> stats);

  /**
   * Override construction of the Rocket transport below each Thrift pipeline.
   * The builder runs once per accepted connection on its owning EventBase and
   * must return a fully wired, inert RocketServerConnection. The connection's
   * existing owner starts and destroys it through ThriftServerTransportAdapter.
   */
 protected:
  void setRocketConnectionBuilderImpl(
      server::RocketServerConnectionBuilder builder);

 public:
  /// The counters attached via setStats, or nullptr if none were.
  const std::shared_ptr<ServerStats>& getStats() const noexcept {
    return stats_;
  }

  /**
   * Attach connection counters. Wires the connection metrics handlers into
   * the acceptance pipeline of every EventBase; leaving it unset omits them,
   * so a server without them pays nothing. Must be called before
   * start()/serve().
   *
   * Separate from setStats because the two describe different layers: these
   * count connection lifecycle, those count thrift and rocket messages. See
   * ConnectionStats for why the shards are not shared.
   */
  void setConnectionStats(std::shared_ptr<connection::ConnectionStats> stats);

  /// The counters attached via setConnectionStats, or nullptr if none were.
  const std::shared_ptr<connection::ConnectionStats>& getConnectionStats()
      const noexcept {
    return connectionStats_;
  }

  /**
   * Attach TLS counters. Wires the metrics handler into every TLS pipeline;
   * leaving it unset omits it, so a server without them pays nothing — as
   * does any server under SSLPolicy::DISABLED, which builds no TLS pipeline
   * for it to sit in. Must be called before start()/serve().
   *
   * Separate from setConnectionStats because a server may negotiate no
   * security at all, in which case these counters have nothing to describe.
   */
  void setTLSStats(std::shared_ptr<connection::security::TLSStats> stats);

  /// The counters attached via setTLSStats, or nullptr if none were.
  const std::shared_ptr<connection::security::TLSStats>& getTLSStats()
      const noexcept {
    return tlsStats_;
  }

  /**
   * Register raw ("native") embedder handlers to splice into the thrift
   * pipeline of every accepted connection. The handlers are inserted after all
   * built-in thrift handlers, immediately above the tail app adapter: the first
   * registered sits closest to the head, the last closest to the tail. Each
   * factory is invoked once per connection to construct a fresh handler
   * instance (handlers may hold per-connection state).
   *
   * Appends to any handlers already registered (via a prior call or addModule),
   * preserving registration order. Build the factories with
   * server::makeThriftPipelineHandlerFactory<T>(...). Must be called before
   * start()/serve().
   */
 protected:
  void addNativeThriftPipelineHandlersImpl(
      std::vector<server::ThriftPipelineHandlerFactory> factories);

 public:
  /**
   * Adds a classic processor event handler. All handlers added through this
   * API share one Cpp2 context adapter and one bridge in every connection.
   * The server owns bridge and extension registration; callers must not add
   * either themselves. Must be called before start()/serve().
   * @throws std::logic_error if called after start()/serve() or with a null
   *     handler.
   */
  void addProcessorEventHandler(
      std::shared_ptr<apache::thrift::TProcessorEventHandler> eventHandler);

  /**
   * Adds a classic server event handler to the same server-owned bridge.
   * The connection context and client identity hook are shared with processor
   * event handlers. Must be called before start()/serve().
   * @throws std::logic_error if called after start()/serve() or with a null
   *     handler.
   */
  void addServerEventHandler(
      std::shared_ptr<apache::thrift::server::TServerEventHandler>
          eventHandler);

  /**
   * Sets the Classic client identity hook invoked by Cpp2ConnContext.
   * Must be called before start()/serve().
   * @throws std::logic_error if called after start()/serve().
   */
  void setClientIdentityHook(apache::thrift::ClientIdentityHook hook);

  const apache::thrift::ClientIdentityHook& getClientIdentityHook()
      const noexcept {
    return clientIdentityHook_;
  }

  /**
   * Reserves per-connection and per-request storage for `Ext`, so that its
   * handlers reach it through `tryState<Ext>()` on either context.
   *
   * A slot is reserved for each scope the extension declares state for; an
   * extension declaring neither reserves nothing. Registering the same
   * extension twice aborts. Must be called before start()/serve().
   */
  template <class Ext>
  void registerExtension() {
    if constexpr (requires { typename Ext::ConnState; }) {
      if constexpr (requires(typename Ext::ConnState* state) {
                      Ext::destroyConnState(state);
                    }) {
        connExtensionBuilder_.add(Ext::kId, [](void* state) noexcept {
          Ext::destroyConnState(static_cast<typename Ext::ConnState*>(state));
        });
      } else {
        connExtensionBuilder_.add(Ext::kId);
      }
    }
    if constexpr (requires { typename Ext::RequestState; }) {
      if constexpr (requires(typename Ext::RequestState* state) {
                      Ext::destroyRequestState(state);
                    }) {
        requestExtensionBuilder_.add(Ext::kId, [](void* state) noexcept {
          Ext::destroyRequestState(
              static_cast<typename Ext::RequestState*>(state));
        });
      } else {
        requestExtensionBuilder_.add(Ext::kId);
      }
    }
  }

  /**
   * Register a module — a named, ordered bundle of thrift pipeline handlers.
   * The module's handlers are appended to the pipeline in call order relative
   * to other addModule / addNativeThriftPipelineHandlers calls, preserving
   * intra-module order. Module names must be non-empty, unique, and not
   * reserved by the server; violating any of those constraints throws
   * std::logic_error. Must be called before start()/serve().
   */

  /**
   * The configuration this server was constructed with, for an embedder whose
   * handlers only work under particular settings and want to say so at
   * registration time rather than fail at the first request.
   *
   * This is not a snapshot of effective runtime state. Later overrides such as
   * setIOThreadPool() and setCPUExecutor() do not update it.
   */
  const FastThriftServerConfig& getConfig() const noexcept { return config_; }

  /**
   * Configure TLS. After this is called, every accepted connection is wrapped
   * in a fizz::server::AsyncFizzServer; the connection factory only sees
   * fully-handshaked transports. Must be called before start()/serve().
   */
  void setSSLConfig(security::FizzServerCertConfig cfg);

  /**
   * Configure thrift-extension knobs negotiated during the fizz handshake
   * (StopTLS, params negotiation, etc.). Must be called before start()/serve().
   */
  void setThriftConfig(security::ThriftTlsConfig cfg);

  /**
   * Supply an IO thread pool to use instead of constructing one from
   * config_.numIOThreads. Lets multiple servers/subsystems share the same
   * IO threads (e.g., AsyncMcServer + FastThriftServer in ucache).
   *
   *
   * The pool MUST be constructed with min == max threads (the two-arg
   * IOThreadPoolExecutor ctor) — see the default-pool construction site
   * in start() for the rationale.
   *
   * Must be called before start()/serve(). When set, config_.numIOThreads
   * is ignored.
   */
  void setIOThreadPool(std::shared_ptr<folly::IOThreadPoolExecutorBase> pool);

  /**
   * Supply the CPU executor used by the generated service path, instead of
   * constructing a thread pool from config_.numCPUThreads. Lets multiple
   * servers/subsystems share one executor.
   *
   * The keep-alive token is held for the server's lifetime, so the executor
   * outlives every dispatch the server enqueues onto it.
   *
   * When no executor is configured (the default), argument deserialization,
   * the handler call, and response serialization run inline on the IO thread
   * owning the connection. That is the cheapest path, and correct for handlers
   * that never block.
   *
   * Applies to the user handler and to the monitoring, status and debug aux
   * interfaces. Methods pinned with @cpp.ProcessInEbThreadUnsafe in their
   * IDLs stay on the IO thread regardless — that is how the liveness probe
   * and counter scrapes keep answering while the executor is saturated.
   * Metadata never offloads; see ThriftServerConnectionFactoryConfig.
   *
   * Must be called before start()/serve(). When set, config_.numCPUThreads
   * is ignored.
   */
  void setCPUExecutor(folly::Executor::KeepAlive<> executor);

  /**
   * Configure BPF selection within the acceptor SO_REUSEPORT group. Random
   * uses the existing cBPF selector. RoundRobin uses one shared eBPF program
   * with a per-CPU cursor, avoiding contention between CPUs while cycling each
   * CPU's connections across every acceptor. If BPF is unavailable, startup
   * continues with the kernel's default reuseport hash selection. Only applies
   * when connection acceptance is colocated with the data IO workers; a
   * dedicated acceptor pool retains the kernel's default selection.
   *
   * Must be called before start()/serve().
   */
  void setReusePortBpfMode(connection::ReusePortBpfMode mode);

  /** Compatibility wrapper: true selects ReusePortBpfMode::Random. */
  void setEnableReusePortBpfSpread(bool enable);

  /**
   * Listening-socket and accept-path tuning knobs (listen backlog, TCP Fast
   * Open, max reads per event, max pending connections). Listener options apply
   * on acceptor threads; accepted-socket options apply on data IO threads. When
   * unset, defaults from connection/SocketOptions.h apply.
   *
   * Must be called before start()/serve().
   */
  void setSocketOptions(connection::SocketOptions opts);

  /**
   * Add or replace one option on every listening socket without disturbing
   * options installed by other integrations. The key's ApplyPos selects
   * whether the option is applied before or after bind.
   *
   * Must be called before start()/serve().
   */
  void setListeningSocketOption(
      folly::SocketOptionKey key, folly::SocketOptionValue value);

  /**
   * Per-connection accept callback. Invoked once per accepted connection
   * (after handshake completion when TLS is enabled), with a pointer to the
   * per-connection ThriftConnContext. Use this to attach embedder-owned
   * per-connection state by calling
   * `connContext->setUserData(rocket::TypeErasedPtr)` — the framework will
   * destroy that state when the connection (and any in-flight requests
   * holding the ThriftConnContext via intrusive_ptr) tear down.
   *
   * The pointer is always non-null.
   *
   * The callback runs on the IO event base that owns the connection. Must
   * be set before start()/serve(). Optional — if unset, no per-connection
   * hook runs and the connection goes straight into the pipeline as-is.
   *
   * Peer address is reachable via `connContext->getPeerAddress()`.
   */
  using OnConnectionAcceptedFn =
      std::function<void(ThriftConnContext* connContext)>;
  void setOnConnectionAccepted(OnConnectionAcceptedFn cb);

  /**
   * Hot-swap the TLS configuration after the server has started. Builds
   * fresh TLSParams from `cfg` (using the most-recently-set ThriftTlsConfig)
   * and atomically replaces the per-EventBase parameters used by future
   * accepts. In-flight handshakes hold the old fizz context via their
   * captured shared_ptr and continue safely.
   *
   * Throws if buildTLSParams throws (e.g. unreadable cert/CA file, missing
   * verifier with clientAuth=Required) — leaves the server's existing TLS
   * state untouched in that case.
   *
   * Intended for cert / ticket-key rotation. Must be called after start();
   * before start() the caller should set the initial config via
   * setSSLConfig instead.
   *
   * Safe to call from any thread.
   */
  void reloadTLSConfig(security::FizzServerCertConfig cfg);

  /// Start accepting connections without blocking.
  void start();

  /// Start accepting connections and block until stop() is called.
  void serve();

  /// Stop accepting new connections and shut down.
  void stop();

  /// Get the bound server address. Useful when binding to port 0. Must be
  /// called after start() — CHECK-fails otherwise. Gate with isRunning()
  /// when iterating via FastThriftServerRegistry, which exposes unstarted
  /// servers too.
  folly::SocketAddress getAddress() const;

  /// True iff start() has been called and stop() has not yet. Cheap, lock-
  /// free read of the lifecycle state — intended for debug introspection
  /// (gating accessors that require the server to be running).
  bool isRunning() const noexcept { return state_ == State::kRunning; }

  /// The bound address if the server is running, else nullopt. Read under the
  /// lifecycle lock, so a caller that gets an address also sees every
  /// interface and the metadata response written before start(). Those do not
  /// change after start(), so they can then be read without the lock.
  std::optional<folly::SocketAddress> getAddressIfRunning() const;

  /**
   * The IO pool backing this server, or nullptr before start() unless the
   * embedder supplied one via setIOThreadPool (start() materializes the
   * default pool otherwise).
   *
   * Exposed so a consumer of the server's per-EventBase state — chiefly
   * FastThriftStatsPublisher — can enumerate the EventBases it must hop onto.
   */
  const std::shared_ptr<folly::IOThreadPoolExecutorBase>& getIOThreadPool()
      const noexcept {
    return ioThreadPool_;
  }

  // The pool that owns listeners. Equal to getIOThreadPool() when dedicated
  // accept threads are disabled; TLS always runs on the data IO pool.
  const std::shared_ptr<folly::IOThreadPoolExecutorBase>&
  getConnectionSetupThreadPool() const noexcept {
    return connectionSetupThreadPool_ ? connectionSetupThreadPool_
                                      : ioThreadPool_;
  }

  /**
   * Returns the cached ThriftServiceMetadataResponse if
   * config.enableMetadataService was set and the server has been start()ed,
   * else nullptr. Read-only handle suitable for sharing — the response is
   * built once at start() and never mutated.
   *
   * Used by debug / introspection handlers that want to expose service
   * metadata without re-deriving it. Safe to call from any thread.
   */
  std::shared_ptr<const apache::thrift::metadata::ThriftServiceMetadataResponse>
  getMetadataResponse() const noexcept {
    return metadataResponse_;
  }

  /**
   * The bundled schema of the user interface and every wired auxiliary
   * interface, merged; empty if none of them has bundled schema, as for a C++
   * ThriftServer. The user interface comes first in `getServiceSchemaNodes()`,
   * the order debug clients use to pick the primary service. Read only once
   * getAddressIfRunning() returns an address: until start() the setters may
   * still be writing the interfaces.
   */
  ::apache::thrift::schema::DefinitionsSchema getServiceSchema() const;
  std::vector<
      folly::not_null<const ::apache::thrift::syntax_graph::ServiceNode*>>
  getServiceSchemaNodes() const;

  /// Snapshot of which auxiliary slots are wired (for debug introspection).
  bool hasMonitoringHandler() const noexcept {
    return static_cast<bool>(auxInterfaces_.monitoringHandler);
  }
  bool hasStatusHandler() const noexcept {
    return static_cast<bool>(auxInterfaces_.statusHandler);
  }
  bool hasDebugHandler() const noexcept {
    return static_cast<bool>(auxInterfaces_.debugHandler);
  }
  bool hasControlHandler() const noexcept {
    return static_cast<bool>(auxInterfaces_.controlHandler);
  }
  bool hasSecurityHandler() const noexcept {
    return static_cast<bool>(auxInterfaces_.securityHandler);
  }

 private:
  class PendingConnectionEventCallback;

  // Lifecycle states. Transitions are linear: kNotStarted → kRunning →
  // kStopped. start() and stop() are idempotent — calling either outside
  // its expected source state is a no-op. All transitions and reads are
  // serialized by lifecycleMutex_.
  enum class State : uint8_t {
    kNotStarted,
    kRunning,
    kStopped,
  };

  /**
   * Auxiliary interfaces like monitoring, status, debugging, etc. will live
   * here.
   */
  struct AuxiliaryInterfaces {
    std::shared_ptr<fast_thrift::MonitoringServerInterface> monitoringHandler{
        nullptr};
    std::shared_ptr<fast_thrift::StatusServerInterface> statusHandler{nullptr};
    std::shared_ptr<fast_thrift::DebugServerInterface> debugHandler{nullptr};
    std::shared_ptr<fast_thrift::ControlServerInterface> controlHandler{
        nullptr};
    std::shared_ptr<fast_thrift::SecurityServerInterface> securityHandler{
        nullptr};
  };

  void rebuildMethodMetadataRegistry();
  std::vector<folly::not_null<ThriftServerAppAdapterFactory*>> interfaces()
      const;

 protected:
  void addModuleRegistrations(
      std::string name,
      bool requiresHeaders,
      std::vector<FastServerModule::PendingConnectionCallbacks>
          pendingConnectionCallbacks,
      const std::vector<FastServerModule::RequestStateRegistration>&
          requestStateRegistrations,
      std::vector<server::ThriftPipelineRegistration> registrations);

 private:
  const InstallConnectionFactory installFactory_;
  const FastThriftServerConfig config_;
  std::shared_ptr<ThriftServerAppAdapterFactory> handler_;
  AuxiliaryInterfaces auxInterfaces_;
  std::shared_ptr<ThriftServerMethodMetadataRegistry> methodMetadataRegistry_;
  // Embedder-registered thrift pipeline handler factories, in registration
  // order. Copied into the per-connection factory config at start().
  std::vector<server::ThriftPipelineRegistration> thriftPipelineRegistrations_;
  server::TProcessorEventHandlers eventHandlers_;
  apache::thrift::ClientIdentityHook clientIdentityHook_;
  server::RocketServerConnectionBuilder rocketConnectionBuilder_;
  // Consumed into the layouts below at start(); untouched after.
  ExtensionLayoutBuilder connExtensionBuilder_;
  ExtensionLayoutBuilder requestExtensionBuilder_;
  std::shared_ptr<const ExtensionLayout> connExtensionLayout_;
  std::shared_ptr<const ExtensionLayout> requestExtensionLayout_;
  // Names of registered modules, for duplicate detection in addModule.
  folly::F14FastSet<std::string> moduleNames_;
  // Per-EventBase server counters, or null when the embedder never called
  // setStats — in which case no metrics handler is built into any pipeline.
  std::shared_ptr<ServerStats> stats_;
  // Per-EventBase connection-layer counters, or null when the embedder never
  // called setConnectionStats. Handed to the ConnectionManager at start().
  std::shared_ptr<connection::ConnectionStats> connectionStats_;
  // Per-EventBase TLS counters, same lifecycle as connectionStats_.
  std::shared_ptr<connection::security::TLSStats> tlsStats_;
  // Cached ThriftServiceMetadataResponse for the user's service. Built once
  // at start() when config_.enableMetadataService is set; null otherwise.
  // Shared across every per-connection MetadataAppAdapter.
  std::shared_ptr<const apache::thrift::metadata::ThriftServiceMetadataResponse>
      metadataResponse_;
  OnConnectionAcceptedFn onConnectionAccepted_;
  std::optional<security::FizzServerCertConfig> sslConfig_;
  security::ThriftTlsConfig thriftConfig_{};
  connection::ReusePortBpfMode reusePortBpfMode_{
      connection::ReusePortBpfMode::Disabled};
  std::vector<FastServerModule::PendingConnectionCallbacks>
      pendingConnectionCallbacks_;
  std::shared_ptr<PendingConnectionEventCallback>
      pendingConnectionEventCallback_;
  // Listening-socket tuning. Defaults from SocketOptions.h apply unless the
  // embedder calls setSocketOptions before start().
  connection::SocketOptions socketOptions_{};
  // IO thread pool. Either embedder-supplied via setIOThreadPool or
  // constructed in start() from config_.numIOThreads. Released on
  // destruction; the pool's own dtor joins when the last ref drops.
  std::shared_ptr<folly::IOThreadPoolExecutorBase> ioThreadPool_;
  // Null when setup is colocated on ioThreadPool_; retaining a second shared
  // reference there would change the ownership contract of setIOThreadPool().
  std::shared_ptr<folly::IOThreadPoolExecutorBase> connectionSetupThreadPool_;
  // Backs cpuExecutor_ only when start() had to construct the pool itself
  // from config_.numCPUThreads; an embedder-supplied executor is owned by
  // the embedder and this stays null. Declared before cpuExecutor_ so the
  // keep-alive is released before the pool it refers to is destroyed.
  std::shared_ptr<folly::Executor> ownedCPUThreadPool_;
  // CPU executor for the generated service path. Either embedder-supplied
  // via setCPUExecutor or a keep-alive on ownedCPUThreadPool_. Null keeps the
  // service path inline on the IO threads.
  folly::Executor::KeepAlive<> cpuExecutor_;
  connection::ConnectionManager::Ptr connectionManager_;
  folly::Baton<> stopBaton_;
  // Guards state_ and serializes lifecycle transitions so that stop()
  // observes the connectionManager_ assignment from start() with a proper
  // happens-before. Without this, TSAN reports a race when stop() runs on a
  // different thread from serve().
  mutable std::mutex lifecycleMutex_;
  State state_{State::kNotStarted};
  // Process-wide registration. Mutable nowhere — declared last so it is
  // destroyed first (its destructor blocks until any in-flight
  // forEachServer callback that observed this server returns, after which
  // the rest of FastThriftServer can tear down without UAF risk).
  instrumentation::ServerTracker tracker_{
      instrumentation::kFastThriftServerTrackerKey, *this};
};

} // namespace detail
} // namespace apache::thrift::fast_thrift::thrift
