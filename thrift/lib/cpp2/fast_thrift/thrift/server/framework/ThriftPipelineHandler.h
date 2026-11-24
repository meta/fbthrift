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
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <variant>

#include <folly/Function.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Handler.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/ContextImpl.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/ErasedStaticHandler.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/ErasedStaticSegment.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/HandlerNode.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/Event.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/ExtensionStateStore.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/framework/NativeThriftHandlerAllowlist.h>

namespace apache::thrift::fast_thrift::thrift::server {

/**
 * Context type seen by handlers registered into the thrift-layer pipeline. This
 * is the concrete pipeline context; a registered handler's onRead / onWrite /
 * lifecycle methods take a `ThriftPipelineHandlerContext&`.
 */
// NOLINTNEXTLINE(facebook-hte-DetailCall)
using ThriftPipelineHandlerContext = channel_pipeline::detail::ContextImpl;
// NOLINTNEXTLINE(facebook-hte-DetailCall)
using StaticThriftPipelineHandlerContext =
    channel_pipeline::detail::StaticHandlerContext;

template <typename H>
concept UsesCachedEkms = requires {
  { H::kUsesCachedEkms } -> std::convertible_to<bool>;
} && H::kUsesCachedEkms;

/**
 * A per-connection factory that produces a fresh, type-erased handler node for
 * the thrift pipeline. The factory is invoked once per accepted connection when
 * that connection's thrift pipeline is built, so each connection gets its own
 * handler instance (handlers may hold per-connection state).
 *
 * The store is that connection's shared extension state. Every factory for a
 * connection is passed the same one, which is how cooperating extensions reach
 * a common object; a handler that needs none ignores it.
 */
class ThriftPipelineHandlerFactory {
 public:
  using DynamicFactory = std::function<channel_pipeline::detail::HandlerNode(
      ExtensionStateStore&)>;
  using StaticFactory =
      std::function<channel_pipeline::detail::ErasedStaticHandler(
          ExtensionStateStore&)>;

  explicit ThriftPipelineHandlerFactory(DynamicFactory dynamicFactory)
      : dynamicFactory_(std::move(dynamicFactory)) {}

  ThriftPipelineHandlerFactory(
      DynamicFactory dynamicFactory, bool requiresCachedEkms)
      : dynamicFactory_(std::move(dynamicFactory)),
        requiresCachedEkms_(requiresCachedEkms) {}

 protected:
  ThriftPipelineHandlerFactory(
      DynamicFactory dynamicFactory, StaticFactory staticFactory)
      : dynamicFactory_(std::move(dynamicFactory)),
        staticFactory_(std::move(staticFactory)) {}

  ThriftPipelineHandlerFactory(
      DynamicFactory dynamicFactory,
      StaticFactory staticFactory,
      bool requiresCachedEkms)
      : dynamicFactory_(std::move(dynamicFactory)),
        staticFactory_(std::move(staticFactory)),
        requiresCachedEkms_(requiresCachedEkms) {}

 public:
  channel_pipeline::detail::HandlerNode operator()(
      ExtensionStateStore& store) const {
    return dynamicFactory_(store);
  }

  bool supportsStaticPipeline() const noexcept {
    return static_cast<bool>(staticFactory_);
  }

  bool requiresCachedEkms() const noexcept { return requiresCachedEkms_; }

  channel_pipeline::detail::ErasedStaticHandler makeStatic(
      ExtensionStateStore& store) const {
    if (!staticFactory_) {
      throw std::logic_error(
          "Thrift pipeline handler factory does not support static pipelines");
    }
    return staticFactory_(store);
  }

 private:
  DynamicFactory dynamicFactory_;
  StaticFactory staticFactory_;
  bool requiresCachedEkms_{false};
};

class StaticThriftPipelineHandlerFactory final
    : public ThriftPipelineHandlerFactory {
 public:
  StaticThriftPipelineHandlerFactory(
      DynamicFactory dynamicFactory, StaticFactory staticFactory)
      : ThriftPipelineHandlerFactory(
            std::move(dynamicFactory), std::move(staticFactory)) {
    if (!supportsStaticPipeline()) {
      throw std::invalid_argument("Static handler factory must be non-null");
    }
  }

  StaticThriftPipelineHandlerFactory(
      DynamicFactory dynamicFactory,
      StaticFactory staticFactory,
      bool requiresCachedEkms)
      : ThriftPipelineHandlerFactory(
            std::move(dynamicFactory),
            std::move(staticFactory),
            requiresCachedEkms) {
    if (!supportsStaticPipeline()) {
      throw std::invalid_argument("Static handler factory must be non-null");
    }
  }
};

class ThriftPipelineSegmentFactory {
 public:
  using DynamicFactory = std::function<void(
      ExtensionStateStore&,
      folly::FunctionRef<void(channel_pipeline::detail::HandlerNode&&)>)>;
  using StaticFactory =
      std::function<channel_pipeline::detail::ErasedStaticSegment(
          ExtensionStateStore&)>;

  ThriftPipelineSegmentFactory(
      DynamicFactory dynamicFactory, StaticFactory staticFactory)
      : dynamicFactory_(std::move(dynamicFactory)),
        staticFactory_(std::move(staticFactory)) {}

  void appendDynamic(
      ExtensionStateStore& store,
      folly::FunctionRef<void(channel_pipeline::detail::HandlerNode&&)> append)
      const {
    dynamicFactory_(store, append);
  }

  channel_pipeline::detail::ErasedStaticSegment makeStatic(
      ExtensionStateStore& store) const {
    return staticFactory_(store);
  }

 private:
  DynamicFactory dynamicFactory_;
  StaticFactory staticFactory_;
};

class ThriftPipelineRegistration {
 public:
  explicit ThriftPipelineRegistration(ThriftPipelineHandlerFactory factory)
      : registration_(std::move(factory)) {
    requiresCachedEkms_ = std::get<ThriftPipelineHandlerFactory>(registration_)
                              .requiresCachedEkms();
  }
  explicit ThriftPipelineRegistration(ThriftPipelineSegmentFactory factory)
      : registration_(std::move(factory)) {}
  ThriftPipelineRegistration(
      ThriftPipelineSegmentFactory factory, bool requiresCachedEkms)
      : registration_(std::move(factory)),
        requiresCachedEkms_(requiresCachedEkms) {}

  bool requiresCachedEkms() const noexcept { return requiresCachedEkms_; }

  bool isSegment() const noexcept {
    return std::holds_alternative<ThriftPipelineSegmentFactory>(registration_);
  }

  bool supportsStaticPipeline() const noexcept {
    if (const auto* handler =
            std::get_if<ThriftPipelineHandlerFactory>(&registration_)) {
      return handler->supportsStaticPipeline();
    }
    return true;
  }

  void appendDynamic(
      ExtensionStateStore& store,
      folly::FunctionRef<void(channel_pipeline::detail::HandlerNode&&)> append)
      const {
    if (const auto* handler =
            std::get_if<ThriftPipelineHandlerFactory>(&registration_)) {
      append((*handler)(store));
      return;
    }
    return std::get<ThriftPipelineSegmentFactory>(registration_)
        .appendDynamic(store, append);
  }

  channel_pipeline::detail::ErasedStaticHandler makeStaticHandler(
      ExtensionStateStore& store) const {
    return std::get<ThriftPipelineHandlerFactory>(registration_)
        .makeStatic(store);
  }

  channel_pipeline::detail::ErasedStaticSegment makeStaticSegment(
      ExtensionStateStore& store) const {
    return std::get<ThriftPipelineSegmentFactory>(registration_)
        .makeStatic(store);
  }

 private:
  std::variant<ThriftPipelineHandlerFactory, ThriftPipelineSegmentFactory>
      registration_;
  bool requiresCachedEkms_{false};
};

/**
 * Derive a pipeline handler id from a namespace (e.g. a module name, or empty
 * for top-level framework handlers) and a within-namespace index.
 *
 * The id is the FNV-1a hash of the composed tag "<ns>/<index>" — the same
 * hash-of-a-tag-string scheme HANDLER_TAG uses, so these ids live in the same
 * id space as the built-in handler tags. Composing before hashing (rather than
 * `hash(ns) + index`) makes it a genuine two-level tag: distinct
 * (namespace, index) pairs are distinct strings and hash independently, with no
 * arithmetic-adjacency collisions. Callers that reserve disjoint namespaces
 * (one per module; empty for top-level) get non-overlapping id streams. Ids key
 * PipelineImpl::handlerMap_, so this keeps context() lookups unambiguous;
 * dispatch itself is positional and unaffected either way.
 */
inline channel_pipeline::HandlerId deriveThriftPipelineHandlerId(
    std::string_view ns, std::size_t index) {
  std::string tag;
  tag.reserve(ns.size() + 21); // '/' + up to 20 digits
  tag.append(ns);
  tag.push_back('/');
  tag.append(std::to_string(index));
  return channel_pipeline::fnv1a_hash(tag);
}

/**
 * Build a ThriftPipelineHandlerFactory for handler type T.
 *
 * T's concrete type and constructor arguments are captured here, where T is in
 * scope; the returned factory erases T and constructs a fresh T per connection
 * from copies of `args`. The node is created with typed Thrift server events so
 * T may subscribe to server events (e.g. connection drain/close) exactly like
 * the built-in thrift pipeline handlers.
 *
 * @param id A pipeline-unique handler id (e.g. from a HANDLER_TAG or a
 *           registration-order-derived id). Used for context lookup and
 *           diagnostics.
 * @param args Constructor arguments, copied into every per-connection instance.
 */
template <typename T, typename StaticT = void, typename... Args>
auto makeThriftPipelineHandlerFactory(
    channel_pipeline::HandlerId id, Args... args) {
  static_assert(
      channel_pipeline::InboundHandler<T, ThriftPipelineHandlerContext> ||
          channel_pipeline::OutboundHandler<T, ThriftPipelineHandlerContext> ||
          channel_pipeline::DuplexHandler<T, ThriftPipelineHandlerContext>,
      "makeThriftPipelineHandlerFactory<T>: T must satisfy the Inbound, "
      "Outbound, or Duplex handler concept over ThriftPipelineHandlerContext");
  static_assert(
      kIsAllowedNativeThriftHandler<T>,
      "makeThriftPipelineHandlerFactory<T>: T is not on the native thrift "
      "pipeline handler allowlist. Native handlers are gated on an as-needed "
      "basis in framework/NativeThriftHandlerAllowlist.h (Thrift-owned). Prefer "
      "the constrained observer/modifier extension API "
      "(FastServerModule::addThriftExtension) for user handlers.");
  auto dynamicFactory = [id, args...](ExtensionStateStore&) {
    return channel_pipeline::detail::makeHandlerNode<T>(
        id, std::make_unique<T>(args...));
  };
  constexpr bool requiresCachedEkms =
      UsesCachedEkms<T> || UsesCachedEkms<StaticT>;
  if constexpr (std::is_void_v<StaticT>) {
    return ThriftPipelineHandlerFactory(
        std::move(dynamicFactory), requiresCachedEkms);
  } else {
    static_assert(
        channel_pipeline::
            InboundHandler<StaticT, StaticThriftPipelineHandlerContext> ||
        channel_pipeline::
            OutboundHandler<StaticT, StaticThriftPipelineHandlerContext> ||
        channel_pipeline::
            DuplexHandler<StaticT, StaticThriftPipelineHandlerContext>);
    static_assert(kIsAllowedNativeThriftHandler<StaticT>);
    auto staticFactory = [id, args...](ExtensionStateStore&) {
      return channel_pipeline::detail::makeErasedStaticHandler<StaticT>(
          id, args...);
    };
    return StaticThriftPipelineHandlerFactory(
        std::move(dynamicFactory),
        std::move(staticFactory),
        requiresCachedEkms);
  }
}

} // namespace apache::thrift::fast_thrift::thrift::server
