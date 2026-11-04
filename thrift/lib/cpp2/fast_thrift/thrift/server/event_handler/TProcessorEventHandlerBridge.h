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

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <glog/logging.h>

#include <folly/ExceptionWrapper.h>
#include <folly/io/async/Request.h>
#include <folly/lang/Exception.h>

#include <thrift/lib/cpp/TProcessorEventHandler.h>
#include <thrift/lib/cpp/server/TServerEventHandler.h>
#include <thrift/lib/cpp/transport/THeader.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Event.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/ConnectionPayloads.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/Event.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/Messages.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/MethodMetadata.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/ServerAppAdapter.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/event_handler/Cpp2ContextAdapter.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/event_handler/EventHandlerChain.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/util/ResponsePayloads.h>

namespace apache::thrift::fast_thrift::thrift::server {

/**
 * What the bridge drives, and on whose behalf. Built once and shared by every
 * connection, so nothing here changes after install.
 *
 * The handler lists are the ones an embedder would have installed on a
 * `ThriftServer`. Both may be empty; a bridge with no processor event handlers
 * forwards everything untouched.
 */
struct TProcessorEventHandlers {
  std::vector<std::shared_ptr<apache::thrift::TProcessorEventHandler>>
      processor;
  std::vector<std::shared_ptr<apache::thrift::server::TServerEventHandler>>
      server;
  apache::thrift::ClientIdentityHook clientIdentityHook;
};

struct TProcessorEventHandlerBridgeConfig {
  // Shared rather than held by value: this config is copied into every
  // connection's handler, and one refcount is the whole cost of that copy.
  std::shared_ptr<const TProcessorEventHandlers> handlers;
  std::shared_ptr<const ThriftServerMethodMetadataRegistry> methodMetadata;
};

struct TProcessorEventHandlerBridgeRequestStateOwner
    : boost::intrusive_ref_counter<
          TProcessorEventHandlerBridgeRequestStateOwner,
          boost::thread_unsafe_counter> {
  // Request contexts acquire and release this only on their connection's
  // EventBase, the same invariant that permits ThriftConnContext's non-atomic
  // intrusive refcount.
  explicit TProcessorEventHandlerBridgeRequestStateOwner(
      std::shared_ptr<const TProcessorEventHandlers> handlers)
      : handlers(std::move(handlers)) {}

  const std::shared_ptr<const TProcessorEventHandlers> handlers;
  std::unique_ptr<Cpp2ConnContextAdapter> context;
};

using TProcessorEventHandlerBridgeRequestStateOwnerPtr =
    boost::intrusive_ptr<TProcessorEventHandlerBridgeRequestStateOwner>;
struct TProcessorEventHandlerBridgeRequestState {
  explicit TProcessorEventHandlerBridgeRequestState(
      TProcessorEventHandlerBridgeRequestStateOwnerPtr owner)
      : owner(std::move(owner)), chain(this->owner->handlers->processor) {
    published.owner = this;
  }

  TProcessorEventHandlerBridgeRequestState(
      const TProcessorEventHandlerBridgeRequestState&) = delete;
  TProcessorEventHandlerBridgeRequestState& operator=(
      const TProcessorEventHandlerBridgeRequestState&) = delete;
  TProcessorEventHandlerBridgeRequestState(
      TProcessorEventHandlerBridgeRequestState&&) = delete;
  TProcessorEventHandlerBridgeRequestState& operator=(
      TProcessorEventHandlerBridgeRequestState&&) = delete;

  ~TProcessorEventHandlerBridgeRequestState() {
    releaseWithContextRestoration();
  }

  void releaseWithAmbientContextInstalled() noexcept {
    if (!context) {
      return;
    }
    DCHECK_EQ(
        folly::RequestContext::try_get(), context->ambientContext().get());
    release();
  }

  void releaseWithContextRestoration() noexcept {
    if (!context) {
      return;
    }
    folly::RequestContextSaverScopeGuard guard;
    release();
  }

  void release() noexcept {
    const auto& ambient = context->ambientContext();
    chain.unbind([&] {
      if (folly::RequestContext::try_get() != ambient.get()) {
        folly::RequestContext::setContext(ambient);
      }
    });
    published.ambientContext = nullptr;
    context.reset();
    cpp2Request.reset();
    published.context = nullptr;
  }

  TProcessorEventHandlerBridgeRequestStateOwnerPtr owner;
  Cpp2BridgeRequestState published;
  EventHandlerChain chain;
  apache::thrift::transport::THeader header;
  std::optional<apache::thrift::Cpp2RequestContext> cpp2Request;
  std::optional<Cpp2RequestContextAdapter> context;
  apache::thrift::protocol::PROTOCOL_TYPES protocolType{};
};

inline void Cpp2BridgeExtension::destroyRequestState(
    RequestState* state) noexcept {
  std::unique_ptr<TProcessorEventHandlerBridgeRequestState> owned(
      static_cast<TProcessorEventHandlerBridgeRequestState*>(state->owner));
}

namespace event_handler_detail {

inline std::uint32_t requestBytes(
    const ThriftServerRequestMessage& request) noexcept {
  if (!request.payload.is<ThriftServerRequestResponsePayload>()) {
    return 0;
  }
  const auto& rr = request.payload.get<ThriftServerRequestResponsePayload>();
  return rr.data == nullptr
      ? 0
      : static_cast<std::uint32_t>(rr.data->computeChainDataLength());
}

inline std::uint32_t payloadBytes(
    const std::unique_ptr<folly::IOBuf>& data) noexcept {
  return data == nullptr
      ? 0
      : static_cast<std::uint32_t>(data->computeChainDataLength());
}

// Moves what the handlers wrote onto the outgoing reply. Only the normal reply
// carries metadata; an error frame has nowhere to put them.
inline void stampWriteHeaders(
    ThriftInitialResponsePayload& reply,
    apache::thrift::transport::THeader::StringToStringMap headers) {
  if (headers.empty() || reply.metadata == nullptr) {
    return;
  }
  auto& other = reply.metadata->otherMetadata().ensure();
  for (auto& [key, value] : headers) {
    other[key] = std::move(value);
  }
}

} // namespace event_handler_detail

/**
 * Runs `TProcessorEventHandler`s / `TServerEventHandler`s on the
 * fast_thrift pipeline.
 *
 * These handlers are written against the classic server: they expect Cpp2
 * contexts, a `THeader` to read and write, an ambient `folly::RequestContext`,
 * and callbacks in a fixed order around a request. The bridge supplies all of
 * that at its own boundary and hands the pipeline back its own message types
 * unchanged — see Cpp2ContextAdapter.h for what is and is not
 * translatable.
 *
 * Mapping, per connection:
 *   Setup request    -> TServerEventHandler::newConnection
 *   ConnectionClosed -> TServerEventHandler::connectionDestroyed
 *
 * and per request:
 *   onRead   -> preRead, onReadData, postRead across the chain, then forward
 *   onWrite  -> exception callbacks when applicable, then preWrite,
 *               onWriteData, postWrite, and forward
 *
 * A `preRead`/`postRead` that throws is the handler refusing the request: the
 * request is dropped, an application error carrying the exception's name and
 * message goes back to the client, and the write-side callbacks do NOT run —
 * matching the classic server, where an exception thrown before dispatch means
 * `postWrite` never fires. Any response headers the handler set before
 * throwing still reach the client.
 *
 * A write-side callback that throws terminates the process. The request has
 * been served by then, so there is no verdict left to honour and nothing to
 * turn the exception into; swallowing it would only hide the handler's bug.
 *
 * NOT supported, because fast_thrift has no equivalent: streams, sinks,
 * interactions (`onInteractionTerminate`), and the server-lifecycle callbacks
 * (`preStart` / `preServe` / `postStop`). When processor handlers are
 * installed, a stream request is refused rather than dispatched without them;
 * the other unsupported callbacks are never driven.
 *
 * Requires `enableRequestHeaders` if any handler reads request headers. A
 * request arriving without a context from a manually composed pipeline is
 * refused rather than forwarded: the bridge cannot tell an authorization
 * handler from a logging one, so quietly skipping them is not safe.
 *
 * Every callback runs synchronously on the connection's EventBase. That is
 * load-bearing, not incidental: the pipeline context does not guard against a
 * closed pipeline, so a callback that deferred work and wrote later would be a
 * use-after-free.
 */
template <typename Context>
class TProcessorEventHandlerBridge {
 public:
  explicit TProcessorEventHandlerBridge(
      TProcessorEventHandlerBridgeConfig config)
      : requestStateOwner_(
            config.handlers == nullptr
                ? nullptr
                : new TProcessorEventHandlerBridgeRequestStateOwner(
                      std::move(config.handlers))),
        methodMetadataOwner_(std::move(config.methodMetadata)),
        handlers_(
            requestStateOwner_ == nullptr ? nullptr
                                          : requestStateOwner_->handlers.get()),
        methodMetadata_(methodMetadataOwner_.get()),
        drivesProcessorHandlers_(
            handlers_ != nullptr && !handlers_->processor.empty()) {}

  TProcessorEventHandlerBridge(const TProcessorEventHandlerBridge&) = delete;
  TProcessorEventHandlerBridge& operator=(const TProcessorEventHandlerBridge&) =
      delete;
  TProcessorEventHandlerBridge(TProcessorEventHandlerBridge&&) = delete;
  TProcessorEventHandlerBridge& operator=(TProcessorEventHandlerBridge&&) =
      delete;

  ~TProcessorEventHandlerBridge() = default;

  channel_pipeline::Result onRead(
      Context& ctx, channel_pipeline::TypeErasedBox&& msg) noexcept {
    auto& request = msg.get<ThriftServerRequestMessage>();

    // The setup exchange is not a request. Latch the connection context it
    // carries — the only place the pipeline offers one — and publish the
    // classic context before downstream connection extensions run.
    if (FOLLY_UNLIKELY(request.payload.is<ThriftConnectionSetupPayload>())) {
      auto& setup = *request.payload.get<ThriftConnectionSetupPayload>().setup;
      auto* const ftConnContext = setup.connContext;
      if (handlers_ != nullptr && ftConnContext != nullptr &&
          connectionContext_ == nullptr) {
        if (auto metadata = setup.clientSetup.clientMetadata()) {
          ftConnContext->setClientMetadata(*metadata);
        }
        requestStateOwner_->context = std::make_unique<Cpp2ConnContextAdapter>(
            boost::intrusive_ptr<ThriftConnContext>(ftConnContext),
            handlers_->clientIdentityHook);
        connectionContext_ = requestStateOwner_->context.get();
        for (const auto& handler : handlers_->server) {
          handler->newConnection(&connectionContext_->get());
        }
      }
      return ctx.fireRead(std::move(msg));
    }

    // No processor handlers means no per-request work, and so no classic
    // request context for anything downstream to read. A server configured
    // this way has no per-request security layer either, so a consumer that
    // finds nothing resolves nothing, which is the right answer. Note this is
    // narrower than the connection context, which is built for any handler at
    // all.
    if (!drivesProcessorHandlers_) {
      return ctx.fireRead(std::move(msg));
    }

    const auto routing = getServerRequestRoutingMetadata(request);
    if (FOLLY_UNLIKELY(routing.status != ServerRequestRoutingStatus::Ready)) {
      return ctx.fireRead(std::move(msg));
    }

    // Streams are deliberately outside this bridge's classic-callback
    // contract. The shared routing helper considers them ready because the
    // generated app adapters support them, but forwarding one without running
    // installed processor handlers could bypass authorization. Refuse it with
    // the same framework error an app adapter uses for an unsupported RPC kind.
    // Independently require the variant before accessing it below, so a
    // malformed kind/payload combination also fails closed.
    if (FOLLY_UNLIKELY(
            routing.kind !=
                apache::thrift::RpcKind::SINGLE_REQUEST_SINGLE_RESPONSE ||
            !request.payload.is<ThriftServerRequestResponsePayload>())) {
      auto response = makeWrongRpcKindMessage(request.streamId, routing.kind);
      response.requestContext = std::move(request.requestContext);
      return ctx.fireWrite(
          channel_pipeline::erase_and_box(std::move(response)));
    }

    const uint32_t streamId = request.streamId;
    if (FOLLY_UNLIKELY(methodMetadata_ == nullptr)) {
      auto response = makeAppErrorMessage(
          streamId,
          "TProcessorEventHandlerBridgeMisconfigured",
          "event handlers are installed but the server has no method "
          "metadata registry");
      response.requestContext = std::move(request.requestContext);
      return ctx.fireWrite(
          channel_pipeline::erase_and_box(std::move(response)));
    }
    const auto* method = methodMetadata_->find(routing.methodName);
    if (FOLLY_UNLIKELY(method == nullptr)) {
      return ctx.fireRead(std::move(msg));
    }

    // Fail closed. A handler may be the thing authorizing this request, and
    // there is no way to tell from here, so a request the bridge cannot run
    // them for is refused rather than let through.
    if (FOLLY_UNLIKELY(
            request.requestContext == nullptr ||
            connectionContext_ == nullptr)) {
      auto response = makeAppErrorMessage(
          streamId,
          "TProcessorEventHandlerBridgeMisconfigured",
          "event handlers are installed but the pipeline provided no "
          "request or connection context");
      response.requestContext = std::move(request.requestContext);
      return ctx.fireWrite(
          channel_pipeline::erase_and_box(std::move(response)));
    }

    auto ownedState =
        std::make_unique<TProcessorEventHandlerBridgeRequestState>(
            requestStateOwner_);
    auto* state = ownedState.get();
    const auto& requestResponse =
        request.payload.get<ThriftServerRequestResponsePayload>();
    state->protocolType = static_cast<apache::thrift::protocol::PROTOCOL_TYPES>(
        requestResponse.metadata->protocol().value_or(0));
    state->cpp2Request.emplace(&connectionContext_->get(), &state->header);
    state->context.emplace(
        *state->cpp2Request, state->header, *request.requestContext);
    state->published.context = &state->context->get();
    state->published.ambientContext = &state->context->ambientContext();
    request.requestContext->adoptState<Cpp2BridgeExtension>(
        &ownedState.release()->published);

    // Scoped across the forward as well as the callbacks: handlers stamp
    // identity onto the ambient context for the service to read, and an
    // executor hop downstream captures whatever is installed here.
    folly::RequestContextScopeGuard guard(state->context->ambientContext());

    // Binding is inside the refusal path with the callbacks: it runs
    // getServiceContext, which is handler code like any other, and an escape
    // out of a noexcept pipeline callback would take the process with it.
    auto refusal = folly::try_and_catch([&] {
      state->chain.bind(
          state->context->get(),
          method->serviceName,
          method->qualifiedMethodName);
      state->chain.preRead();
      state->chain.onReadData(
          apache::thrift::SerializedMessage{
              .protocolType = state->protocolType,
              .buffer = requestResponse.data.get(),
              .methodName = method->qualifiedMethodName,
          });
      state->chain.postRead(
          state->context->header(),
          event_handler_detail::requestBytes(request));
    });
    // A handler may have installed a private child RequestContext while
    // binding. Retain the effective top before the read-side scope unwinds;
    // its guards retain every predecessor below it.
    state->context->captureAmbientContext();
    if (refusal) {
      auto response = makeRejectionMessage(streamId, refusal);
      // A handler that set a response header before refusing meant it for the
      // client; the refusal is the only response there will be. A rejection is
      // always a reply payload, so it has somewhere to carry them.
      event_handler_detail::stampWriteHeaders(
          response.payload.template get<ThriftInitialResponsePayload>(),
          state->context->takeWriteHeaders());
      state->releaseWithAmbientContextInstalled();
      request.requestContext->clearOwnedState<Cpp2BridgeExtension>();
      response.requestContext = std::move(request.requestContext);
      auto boxed = channel_pipeline::erase_and_box(std::move(response));
      return ctx.fireWrite(std::move(boxed));
    }
    return ctx.fireRead(std::move(msg));
  }

  channel_pipeline::Result onWrite(
      Context& ctx, channel_pipeline::TypeErasedBox&& msg) noexcept {
    auto& response = msg.get<ThriftServerResponseMessage>();

    // Resolve the reply body when this response carries one.
    ThriftInitialResponsePayload* reply = nullptr;
    if (response.payload.is<ThriftInitialResponsePayload>()) {
      reply = &response.payload.get<ThriftInitialResponsePayload>();
    }

    auto* published = response.requestContext == nullptr
        ? nullptr
        : response.requestContext->tryState<Cpp2BridgeExtension>();
    if (published == nullptr) {
      // Setup responses, connection-level frames, and anything the bridge
      // refused — none of which a handler saw on the way in.
      return ctx.fireWrite(std::move(msg));
    }
    auto* state = static_cast<TProcessorEventHandlerBridgeRequestState*>(
        published->owner);

    {
      // Responses may arrive after an executor hop, on an EventBase carrying
      // a different request. Reinstall the effective context captured after
      // all read-side handlers bound for the callbacks and cleanup only;
      // downstream forwarding resumes under the caller's context.
      const auto& ambient = state->context->ambientContext();
      folly::RequestContextScopeGuard guard(ambient);

      // The write-side callbacks bracket a successful reply body. Classic
      // handlers do not receive them when the service or a downstream
      // interceptor rejects the request. The handlers' contexts are still
      // returned below, which is the pairing they are promised.
      if (reply != nullptr) {
        const auto* exception = published->exception.get();
        if (exception != nullptr) {
          state->chain.userExceptionWrapped(
              exception->declared, exception->exception);
          if (!exception->declared) {
            state->chain.handlerErrorWrapped(exception->exception);
          }
        }

        // Deliberately not caught: these run after the request has been
        // served, so there is no verdict left to honour and swallowing would
        // hide a handler bug. An escape terminates, as it would on a classic
        // server.
        if ((exception == nullptr && !isAppUnknownException(*reply)) ||
            (exception != nullptr && exception->declared)) {
          state->chain.preWrite();
          state->chain.onWriteData(state->protocolType, reply->data.get());
          state->chain.postWrite(
              event_handler_detail::payloadBytes(reply->data));
        }

        // After postWrite: handlers write response headers there, and the reply
        // has not been serialized yet.
        event_handler_detail::stampWriteHeaders(
            *reply, state->context->takeWriteHeaders());
      }
      state->releaseWithAmbientContextInstalled();
      response.requestContext->clearOwnedState<Cpp2BridgeExtension>();
    }

    return ctx.fireWrite(std::move(msg));
  }

  using SubscribedEvents =
      channel_pipeline::Events<ThriftServerConnectionClosedEvent>;

  template <channel_pipeline::PipelineEvent E>
    requires std::same_as<E, ThriftServerConnectionClosedEvent>
  void on(Context&) noexcept {
    closeConnection();
  }

  void onException(Context& ctx, folly::exception_wrapper&& e) noexcept {
    ctx.fireException(std::move(e));
  }

  void onReadReady(Context&) noexcept {}
  void onWriteReady(Context&) noexcept {}
  void onPipelineActive(Context&) noexcept {}
  void onPipelineInactive(Context&) noexcept {}
  void handlerAdded(Context&) noexcept {}
  void handlerRemoved(Context&) noexcept { closeConnection(); }

 private:
  void closeConnection() noexcept {
    if (handlers_ == nullptr || connectionContext_ == nullptr ||
        connectionDestroyed_) {
      return;
    }
    // Announce closure once even if the event arrives twice: each handler is
    // promised one connectionDestroyed for the newConnection it observed.
    connectionDestroyed_ = true;
    for (const auto& handler : handlers_->server) {
      handler->connectionDestroyed(&connectionContext_->get());
    }
    connectionContext_->connectionClosed();
  }

  const TProcessorEventHandlerBridgeRequestStateOwnerPtr requestStateOwner_;
  const std::shared_ptr<const ThriftServerMethodMetadataRegistry>
      methodMetadataOwner_;
  // Resolved once: the answer cannot change, and reaching it through the
  // shared_ptr on every request is two dependent loads into memory that every
  // connection shares.
  const TProcessorEventHandlers* const handlers_;
  const ThriftServerMethodMetadataRegistry* const methodMetadata_;
  const bool drivesProcessorHandlers_;
  // Sits here to land in the padding the flag above leaves, rather than widen
  // the connection by a word of its own.
  bool connectionDestroyed_{false};

  // Hot-path cache. Ownership stays with requestStateOwner_ so outstanding
  // request cleanup remains valid after this bridge is destroyed.
  Cpp2ConnContextAdapter* connectionContext_{nullptr};
};

} // namespace apache::thrift::fast_thrift::thrift::server
