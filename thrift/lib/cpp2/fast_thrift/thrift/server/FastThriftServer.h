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

#include <functional>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include <thrift/lib/cpp2/fast_thrift/thrift/server/FastThriftServerBase.h>

namespace apache::thrift::fast_thrift::thrift::detail {

template <ChannelPipelineMode Mode>
class FastThriftServerBase::FastThriftServerT : public FastThriftServerBase {
 public:
  static constexpr ChannelPipelineMode kPipelineMode = Mode;
  using HandlerFactory = std::conditional_t<
      Mode == ChannelPipelineMode::Static,
      server::StaticThriftPipelineHandlerFactory,
      server::ThriftPipelineHandlerFactory>;
  using RocketConnectionBuilder = std::conditional_t<
      Mode == ChannelPipelineMode::Static,
      server::StaticRocketServerConnectionBuilder,
      server::RocketServerConnectionBuilder>;

  void setRocketConnectionBuilder(RocketConnectionBuilder builder) {
    if constexpr (Mode == ChannelPipelineMode::Static) {
      CHECK(builder) << "Rocket connection builder must be non-null";
      setRocketConnectionBuilderImpl(
          [builder = std::move(builder)](
              folly::AsyncTransport::UniquePtr socket,
              const folly::SocketAddress& peer,
              ServerStatsShard* statsShard) mutable {
            return server::RocketServerConnectionBuildResult{
                .connection = builder(std::move(socket), peer, statsShard),
                .transportNegotiationHandler = {}};
          });
    } else {
      setRocketConnectionBuilderImpl(std::move(builder));
    }
  }

  void addNativeThriftPipelineHandlers(std::vector<HandlerFactory> factories) {
    std::vector<server::ThriftPipelineHandlerFactory> erased;
    erased.reserve(factories.size());
    for (auto& factory : factories) {
      erased.push_back(std::move(factory));
    }
    addNativeThriftPipelineHandlersImpl(std::move(erased));
  }

  template <ChannelPipelineMode ModuleMode>
    requires(
        Mode == ChannelPipelineMode::Dynamic ||
        ModuleMode == ChannelPipelineMode::Static)
  void addModule(module_detail::FastServerModuleT<ModuleMode> serverModule) {
    auto name = serverModule.name();
    const bool requiresHeaders = serverModule.requiresHeaders();
    auto callbacks = serverModule.pendingConnectionCallbacks();
    const auto& requestStateRegistrations =
        serverModule.requestStateRegistrations();
    auto factories = std::move(serverModule).handlers();
    std::vector<server::ThriftPipelineRegistration> registrations;
    registrations.reserve(factories.size());
    for (auto& factory : factories) {
      registrations.emplace_back(std::move(factory));
    }
    addModuleRegistrations(
        std::move(name),
        requiresHeaders,
        std::move(callbacks),
        requestStateRegistrations,
        std::move(registrations));
  }

  template <typename... Entry>
  void addModule(StaticFastServerModule<Entry...> serverModule) {
    auto name = serverModule.name();
    const bool requiresHeaders = serverModule.requiresHeaders();
    auto pendingConnectionCallbacks = serverModule.pendingConnectionCallbacks();
    std::vector<server::ThriftPipelineRegistration> registrations;
    registrations.push_back(std::move(serverModule).registration());
    addModuleRegistrations(
        std::move(name),
        requiresHeaders,
        std::move(pendingConnectionCallbacks),
        {},
        std::move(registrations));
  }

 protected:
  explicit FastThriftServerT(FastThriftServerConfig config)
      : FastThriftServerBase(std::move(config), &installConnectionFactory) {}

  ~FastThriftServerT() = default;

 private:
  static void installConnectionFactory(
      connection::ConnectionManager& manager,
      server::ThriftServerConnectionFactoryConfig config,
      std::function<void(server::ThriftServerConnection&)> onAccept);
};

} // namespace apache::thrift::fast_thrift::thrift::detail

namespace apache::thrift::fast_thrift::thrift {

/**
 * FastThriftServer — a standalone server that uses the fast_thrift pipeline
 * to serve Thrift RPCs through a generated FastSvAppAdapter handler (i.e.
 * services annotated with @cpp.FastServer).
 *
 * This is the "fast handler" variant. For the legacy AsyncProcessorFactory
 * path see FastThriftChannelServer in this directory.
 *
 *
 * The user supplies a generated ServiceFastHandler<Service> via setInterface;
 * the server asks the factory for a fresh app adapter per connection
 * (handler lifetime is shared via std::shared_ptr).
 *
 * Usage:
 *   auto handler = std::make_shared<MyServiceImpl>();   // : public
 * MyServiceFastHandler FastThriftServerConfig config;
 *   config.address.setFromLocalPort(5001);
 *   config.numIOThreads = 8;
 *
 *   FastThriftServer server(config);
 *   server.setInterface(handler);   // implicit upcast to
 *                                   // ThriftServerAppAdapterFactory
 *   server.serve();                 // Blocks until stop() is called.
 */
class FastThriftServer final
    : public detail::FastThriftServerBase::FastThriftServerT<
          ChannelPipelineMode::Static> {
 public:
  explicit FastThriftServer(FastThriftServerConfig config);
};

class DynamicFastThriftServer final
    : public detail::FastThriftServerBase::FastThriftServerT<
          ChannelPipelineMode::Dynamic> {
 public:
  explicit DynamicFastThriftServer(FastThriftServerConfig config);
};

} // namespace apache::thrift::fast_thrift::thrift
