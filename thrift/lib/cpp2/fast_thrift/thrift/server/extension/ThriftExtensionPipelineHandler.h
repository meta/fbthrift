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
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <typeinfo>
#include <utility>

#include <folly/CPortability.h>
#include <folly/Demangle.h>
#include <folly/ExceptionWrapper.h>
#include <folly/io/async/EventBase.h>

#include <fmt/core.h>
#include <folly/ExceptionString.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Event.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineRef.h>
#include <thrift/lib/cpp2/fast_thrift/frame/ErrorCode.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/ConnectionPayloads.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/Event.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/Messages.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/ReadBackpressureArbiter.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/extension/ThriftBackpressureExtension.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/extension/ThriftConnectionExtension.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/extension/ThriftExtension.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/framework/ThriftPipelineHandler.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/util/PayloadUtils.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/util/ResponsePayloads.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/util/SetupMessages.h>

namespace apache::thrift::fast_thrift::thrift::server {

namespace extension_detail {

template <bool Subscribe, channel_pipeline::PipelineEvent E>
using EventsIf = std::conditional_t<
    Subscribe,
    channel_pipeline::Events<E>,
    channel_pipeline::Events<>>;

template <typename A, typename B>
struct ConcatEventsImpl;
template <
    channel_pipeline::PipelineEvent... A,
    channel_pipeline::PipelineEvent... B>
struct ConcatEventsImpl<
    channel_pipeline::Events<A...>,
    channel_pipeline::Events<B...>> {
  using type = channel_pipeline::Events<A..., B...>;
};
template <typename A, typename B>
using ConcatEvents = typename ConcatEventsImpl<A, B>::type;

/**
 * The connection events H's adapter listens to. The two after-the-fact points
 * are events; onConnectionAttempted is a message on the read path. An H that
 * implements only onConnectionClosed still subscribes to SetupComplete,
 * because that is where the established latch gating it is set. An H that
 * hooks no connection point is linked into no event list.
 */
template <typename H>
using ConnectionEventSet = ConcatEvents<
    ConcatEvents<
        EventsIf<
            HasConnectionEstablishedCallback<H> ||
                HasConnectionClosedCallback<H>,
            ThriftServerSetupCompleteEvent>,
        EventsIf<
            HasConnectionClosedCallback<H>,
            ThriftServerConnectionClosedEvent>>,
    EventsIf<
        ThriftBackpressureExtensionHandler<H>,
        ThriftServerWriteCompleteEvent>>;
/**
 * Per-connection state an admission-control extension needs and nothing else
 * pays for: the resumer's control block, the pipeline to wake, and the latch
 * itself. Empty for every other extension.
 */
struct BackpressureStateEnabled {
  std::shared_ptr<backpressure_detail::ResumeControl> resumeControl_;
  channel_pipeline::PipelineRef pipeline_;
  ReadBackpressureArbiter* arbiter_{nullptr};
  bool paused_{false};
};
struct BackpressureStateDisabled {};

template <typename H>
using BackpressureState = std::conditional_t<
    ThriftBackpressureExtensionHandler<H>,
    BackpressureStateEnabled,
    BackpressureStateDisabled>;

// Callbacks without a request borrow the connection saved from setup. Its
// context handler outlives pipeline teardown; request/response callbacks borrow
// the connection through their message instead.
template <typename H>
inline constexpr bool kNeedsConnectionContext =
    HasConnectionEstablishedCallback<H> || HasConnectionClosedCallback<H> ||
    ThriftBackpressureExtensionHandler<H>;

struct ConnectionContextStateEnabled {
  ThriftConnContext* context{nullptr};
};
struct ConnectionContextStateDisabled {};

template <typename H>
using ConnectionContextState = std::conditional_t<
    kNeedsConnectionContext<H>,
    ConnectionContextStateEnabled,
    ConnectionContextStateDisabled>;

struct ConnectionClosedStateEnabled {
  bool established{false};
};
struct ConnectionClosedStateDisabled {};

template <typename H>
using ConnectionClosedState = std::conditional_t<
    HasConnectionClosedCallback<H>,
    ConnectionClosedStateEnabled,
    ConnectionClosedStateDisabled>;

/**
 * H's demangled type name, for attributing a refusal to the extension that
 * made it. Computed once per H and returned by reference, so the event can
 * hold a view rather than a copy.
 */
template <typename H>
const std::string& extensionName() {
  static const std::string kName =
      folly::demangle(typeid(H).name()).toStdString();
  return kName;
}

} // namespace extension_detail

template <typename H>
concept ThriftPerConnectionExtensionHandler = ThriftExtensionHandler<H> ||
    HasResponseCallback<H> || ThriftConnectionExtensionHandler<H> ||
    ThriftBackpressureExtensionHandler<H>;

/**
 * Adapts a user extension handler H into a duplex channel_pipeline handler.
 * This is the only place that touches the raw TypeErasedBox and pipeline
 * context — the extension itself only ever sees a ThriftRequestView /
 * ThriftRequestMutator, so it cannot retain, replace, or manually forward a
 * message.
 *
 * Forwarding / rejection contract:
 *   - H::onRequest returns RequestVerdict::proceed() → the request is
 *     forwarded toward the tail (fireRead).
 *   - H::onRequest returns RequestVerdict::reject(cause) → the request is
 *     dropped and an application error carrying the cause's name/message is
 *     emitted back down the outbound path (fireWrite), echoing the request's
 *     streamId. As the error travels out, extensions that already ran see their
 *     onResponse/onWrite in reverse order — the pipeline provides that
 * ordering.
 *
 * Response side: if H additionally implements onResponse, it is invoked (via a
 * view or mutator) on every outbound response before it is forwarded.
 *
 * Lifetime: one instance per connection (constructed by the pipeline handler
 * factory); H is owned by value and its constructor arguments are forwarded
 * from addThriftExtension. An H declaring a ConnState additionally receives
 * that connection's shared state as its first constructor argument. An H
 * declaring RequestState may lazily emplace a context-owned instance, visible
 * through both views until the response leaves the pipeline.
 */
template <typename H, typename Context = ThriftPipelineHandlerContext>
class ThriftExtensionPipelineHandler {
  static_assert(
      ThriftPerConnectionExtensionHandler<H>,
      "ThriftExtensionPipelineHandler<H>: H must implement at least one "
      "extension callback — onRequest taking either const ThriftRequestView& "
      "[read-only] or ThriftRequestMutator& [read/write] and returning "
      "RequestVerdict; onResponse over the response view or mutator; one of "
      "onConnectionAttempted / onConnectionAnswering / "
      "onConnectionEstablished / onConnectionClosed; or onEgressDrained for "
      "per-connection admission control. Every callback is noexcept.");

  static_assert(
      !ThriftBackpressureExtensionHandler<H> ||
          HasBackpressureAttachedCallback<H>,
      "ThriftExtensionPipelineHandler<H>: an extension declaring "
      "onEgressDrained must also declare onBackpressureAttached(ReadResumer). "
      "Pausing is only undone by the resumer, so an extension that can pause "
      "without holding one would stall its connection permanently.");

 public:
  template <typename... Args>
  explicit ThriftExtensionPipelineHandler(
      ExtensionStateStore& store, Args&&... args)
      : ThriftExtensionPipelineHandler(
            std::bool_constant<HasConnState<H>>{},
            store,
            std::forward<Args>(args)...) {}

  channel_pipeline::Result onRead(
      Context& ctx, channel_pipeline::TypeErasedBox&& msg) noexcept {
    bool setupMessage = false;
    if constexpr (
        ThriftExtensionHandler<H> || ThriftConnectionExtensionHandler<H> ||
        ThriftBackpressureExtensionHandler<H>) {
      auto& request = msg.get<ThriftServerRequestMessage>();

      // Lifecycle messages never reach the request callbacks, whatever H
      // implements: to a request extension the setup exchange is not a request,
      // and handing it one would run onRequest against a connection.
      setupMessage = request.payload.is<ThriftConnectionSetupPayload>();
      if (setupMessage) [[unlikely]] {
        if constexpr (
            ThriftConnectionExtensionHandler<H> ||
            ThriftBackpressureExtensionHandler<H>) {
          auto& setup =
              *request.payload.get<ThriftConnectionSetupPayload>().setup;
          if constexpr (extension_detail::kNeedsConnectionContext<H>) {
            connection_.context = setup.connContext;
          }
          if constexpr (ThriftBackpressureExtensionHandler<H>) {
            bp_.arbiter_ = &connection_.context->readBackpressureArbiter();
          }
          if constexpr (ThriftConnectionExtensionHandler<H>) {
            if (const auto result = onConnectionAttempted(ctx, setup)) {
              return *result;
            }
          }
        }
      } else if constexpr (ThriftExtensionHandler<H>) {
        if (!isStreamControlRequest(request.payload)) [[likely]] {
          if (const auto result = onRequest(ctx, request)) {
            return applyReadBackpressure(*result);
          }
        }
      }
    }
    const auto result = ctx.fireRead(std::move(msg));
    if constexpr (ThriftBackpressureExtensionHandler<H>) {
      if (setupMessage) [[unlikely]] {
        return result;
      }
    }
    return applyReadBackpressure(result);
  }

  using SubscribedEvents = extension_detail::ConnectionEventSet<H>;

  template <channel_pipeline::PipelineEvent E>
    requires std::same_as<E, ThriftServerWriteCompleteEvent> &&
      ThriftBackpressureExtensionHandler<H>
  void on(Context&, const ThriftServerWriteCompleteEvent& event) noexcept {
    onWriteComplete(event);
  }

  template <channel_pipeline::PipelineEvent E>
    requires std::same_as<E, ThriftServerSetupCompleteEvent> &&
      ThriftConnectionExtensionHandler<H>
  void on(Context&, ThriftServerSetupCompleteEvent* event) noexcept {
    onSetupComplete(*event);
  }

  template <channel_pipeline::PipelineEvent E>
    requires std::same_as<E, ThriftServerConnectionClosedEvent> &&
      HasConnectionClosedCallback<H>
  void on(Context&) noexcept {
    if (closed_.established && connection_.context != nullptr) {
      const ThriftConnectionView view(*connection_.context);
      handler_.onConnectionClosed(view);
    }
  }

 private:
  // A refusal stops the message here and answers outbound, exactly as a
  // rejected request does. Forwarding otherwise lets the next extension
  // contribute, and finally lets ThriftServerSetupHandler answer.
  std::optional<channel_pipeline::Result> onConnectionAttempted(
      Context& ctx, ConnectionSetupData& setup) noexcept
    requires ThriftConnectionExtensionHandler<H>
  {
    if constexpr (HasConnectionAttemptedCallback<H>) {
      // View form first: a mutator binds to a const view parameter, so an
      // extension that only reads is not handed write access it did not ask
      // for.
      ConnectionVerdict verdict = [&]() noexcept {
        if constexpr (HasConnectionAttemptedViewCallback<H>) {
          const ThriftSetupConnectionView view(setup);
          return handler_.onConnectionAttempted(view);
        } else {
          ThriftSetupConnectionMutator mutator(setup);
          return handler_.onConnectionAttempted(mutator);
        }
      }();
      if (verdict.isRejected()) [[unlikely]] {
        return refuse(ctx, "onConnectionAttempted", std::move(verdict));
      }
    }
    return std::nullopt;
  }

  // The setup answer is on the wire. Nothing is forwarded and nothing can be
  // contributed here — a refusal at this point reaches the client as an error
  // frame following the response, which the rocket setup handler emits once
  // this returns.
  void onSetupComplete(ThriftServerSetupCompleteEvent& event) noexcept
    requires ThriftConnectionExtensionHandler<H>
  {
    // Closed-only handlers still need to distinguish successful setup from a
    // connection that died before reaching the established point.
    if constexpr (HasConnectionClosedCallback<H>) {
      closed_.established = true;
    }
    if constexpr (HasConnectionEstablishedCallback<H>) {
      if (connection_.context == nullptr) {
        return;
      }
      const ThriftConnectionView view(*connection_.context);
      auto verdict = handler_.onConnectionEstablished(view);
      if (FOLLY_UNLIKELY(verdict.isRejected()) && !event.reject.has_value()) {
        event.reject = SetupRejection{
            .code =
                apache::thrift::fast_thrift::frame::ErrorCode::REJECTED_SETUP,
            .reason =
                rejectionReason("onConnectionEstablished", std::move(verdict))};
      }
    }
  }

  // Names the refusing extension and its cause, so an operator can tell which
  // one closed the connection without server-side logs.
  static std::string rejectionReason(
      std::string_view callback, ConnectionVerdict verdict) noexcept {
    return fmt::format(
        "ThriftExtension::{} rejected the connection:\n[{}] {}",
        callback,
        extension_detail::extensionName<H>(),
        folly::exceptionStr(std::move(verdict).cause()).toStdString());
  }

  channel_pipeline::Result refuse(
      Context& ctx,
      std::string_view callback,
      ConnectionVerdict verdict) noexcept {
    (void)ctx.fireWrite(
        channel_pipeline::erase_and_box(makeSetupRejectionMessage(
            apache::thrift::fast_thrift::frame::ErrorCode::REJECTED_SETUP,
            rejectionReason(callback, std::move(verdict)))));
    // Non-Success is how a refusal travels back to the transport adapter: it
    // stops the setup exchange there, so no established point is announced and
    // the connection is torn down.
    return channel_pipeline::Result::Error;
  }

  // Egress has gone idle, so the server has caught up on what it owed this
  // client — the point at which the extension reassesses the connection. An
  // errored completion also empties the write queue, but says nothing about a
  // connection that is already going away, so it is not offered as quiescence;
  // teardown reaches the extension through onPipelineInactive instead.
  //
  void onWriteComplete(const ThriftServerWriteCompleteEvent& event) noexcept
    requires ThriftBackpressureExtensionHandler<H>
  {
    if (!event.quiesced ||
        event.status !=
            apache::thrift::fast_thrift::transport::WriteCompletionStatus::
                Success) {
      return;
    }
    if (connection_.context == nullptr) [[unlikely]] {
      return;
    }
    const ThriftConnectionView view(*connection_.context);
    handler_.onEgressDrained(view);
  }

  // Reached only through a ReadResumer, which is documented to run on the
  // connection's EventBase. That is what lets bp_ stay a plain struct: every
  // reader and writer of it — this, onRead, handlerAdded, detachResumer — is
  // that one thread. The check is the contract, not a defence against a race
  // it could not fix anyway.
  void resumeReads() noexcept
    requires ThriftBackpressureExtensionHandler<H>
  {
    DCHECK(
        bp_.pipeline_ == nullptr ||
        bp_.pipeline_.eventBase()->isInEventBaseThread())
        << "ReadResumer::resume() must be called on the connection's EventBase";
    if (!bp_.paused_) {
      return;
    }
    bp_.paused_ = false;
    if (DCHECK_NOTNULL(bp_.arbiter_)->release() &&
        FOLLY_LIKELY(bp_.pipeline_ != nullptr)) {
      bp_.pipeline_.onReadReady();
    }
  }

  // Severs every resumer handed out: the extension may hold one past the
  // connection, and resuming a pipeline that is going away must do nothing.
  void detachResumer() noexcept
    requires ThriftBackpressureExtensionHandler<H>
  {
    if (bp_.paused_) {
      bp_.paused_ = false;
      (void)DCHECK_NOTNULL(bp_.arbiter_)->release();
    }
    bp_.pipeline_.reset();
    bp_.arbiter_ = nullptr;
    if (bp_.resumeControl_ != nullptr) {
      bp_.resumeControl_->owner = nullptr;
    }
  }

  FOLLY_ALWAYS_INLINE std::optional<channel_pipeline::Result> onRequest(
      Context& ctx, ThriftServerRequestMessage& request) noexcept
    requires ThriftExtensionHandler<H>
  {
    // Capture before any potential reject drops the inbound message: the
    // synthesized response must echo this streamId to correlate on the client.
    const uint32_t streamId = request.streamId;

    // View form first: a mutator binds to a const view parameter, so testing
    // the view form first hands each extension exactly the access it declared.
    const RequestVerdict verdict = [&]() noexcept {
      if constexpr (HasRequestViewCallback<H>) {
        const ThriftRequestView view(request);
        return handler_.onRequest(view);
      } else {
        ThriftRequestMutator mutator(request);
        return handler_.onRequest(mutator);
      }
    }();

    if (verdict.isRejected()) [[unlikely]] {
      return rejectRequest(ctx, request, streamId, verdict);
    }
    if (verdict.appliesBackpressure()) [[unlikely]] {
      // Latch it before forwarding: onRead reads this on the way back out
      // and reports Backpressure for this very message, so the socket stops
      // being read without a further request having to arrive first.
      if constexpr (ThriftBackpressureExtensionHandler<H>) {
        bp_.paused_ = true;
        DCHECK_NOTNULL(bp_.arbiter_)->acquire();
      } else {
        DCHECK(false)
            << "RequestVerdict::backpressure() from an extension that "
               "declares no onBackpressureAttached — nothing could resume it";
      }
    }
    return std::nullopt;
  }

  FOLLY_ALWAYS_INLINE channel_pipeline::Result applyReadBackpressure(
      channel_pipeline::Result result) noexcept {
    if constexpr (ThriftBackpressureExtensionHandler<H>) {
      // An accepted message carries the current pause state back to transport.
      // Downstream errors remain terminal even while reads are paused.
      if (bp_.paused_ && result != channel_pipeline::Result::Error)
          [[unlikely]] {
        return channel_pipeline::Result::Backpressure;
      }
    }
    return result;
  }

  FOLLY_NOINLINE static channel_pipeline::Result rejectRequest(
      Context& ctx,
      ThriftServerRequestMessage& request,
      uint32_t streamId,
      const RequestVerdict& verdict) noexcept {
    auto response = makeRejectionMessage(streamId, verdict.cause());
    response.requestContext = std::move(request.requestContext);
    return ctx.fireWrite(channel_pipeline::erase_and_box(std::move(response)));
  }

 public:
  channel_pipeline::Result onWrite(
      Context& ctx, channel_pipeline::TypeErasedBox&& msg) noexcept {
    if constexpr (HasConnectionAnsweringCallback<H>) {
      auto& outbound = msg.get<ThriftServerResponseMessage>();
      if (outbound.payload.template is<ThriftSetupResponsePayload>())
          [[unlikely]] {
        // The answer is assembled but not yet serialized: this is the one
        // window in which an extension can put something in front of the
        // client.
        auto& setupResponse =
            outbound.payload.template get<ThriftSetupResponsePayload>()
                .response;
        if (setupResponse != nullptr) [[likely]] {
          ThriftSetupResponseMutator mutator(*setupResponse);
          handler_.onConnectionAnswering(mutator);
        }
        return ctx.fireWrite(std::move(msg));
      }
    }
    if constexpr (HasResponseCallback<H>) {
      auto& response = msg.get<ThriftServerResponseMessage>();
      if constexpr (HasResponseViewCallback<H>) {
        const ThriftResponseView view(response);
        handler_.onResponse(view);
      } else {
        ThriftResponseMutator mutator(response);
        handler_.onResponse(mutator);
      }
    }
    return ctx.fireWrite(std::move(msg));
  }

  void onException(Context& ctx, folly::exception_wrapper&& e) noexcept {
    ctx.fireException(std::move(e));
  }

  void onPipelineActive(Context&) noexcept {}

  void onPipelineInactive(Context&) noexcept {
    if constexpr (ThriftBackpressureExtensionHandler<H>) {
      detachResumer();
    }
  }

  void onReadReady(Context&) noexcept {}
  void onWriteReady(Context&) noexcept {}

  void handlerAdded(Context& ctx) noexcept {
    if constexpr (ThriftBackpressureExtensionHandler<H>) {
      if constexpr (std::is_pointer_v<decltype(ctx.pipeline())>) {
        bp_.pipeline_ = channel_pipeline::PipelineRef(*ctx.pipeline());
      } else {
        bp_.pipeline_ = ctx.pipeline();
      }
      bp_.resumeControl_ =
          std::make_shared<backpressure_detail::ResumeControl>();
      bp_.resumeControl_->owner = this;
      bp_.resumeControl_->resumeFn = +[](void* owner) noexcept {
        static_cast<ThriftExtensionPipelineHandler*>(owner)->resumeReads();
      };
      handler_.onBackpressureAttached(
          ReadResumer(bp_.resumeControl_, ctx.eventBase()));
    }
  }

  void handlerRemoved(Context&) noexcept {
    if constexpr (ThriftBackpressureExtensionHandler<H>) {
      detachResumer();
    }
  }

 private:
  // Construct H directly: guaranteed elision does not apply to potentially
  // overlapping handler storage, and H may have no copy or move constructor.
  template <typename... Args>
  explicit ThriftExtensionPipelineHandler(
      std::false_type, ExtensionStateStore&, Args&&... args)
      : handler_(std::forward<Args>(args)...) {}

  template <typename... Args>
  explicit ThriftExtensionPipelineHandler(
      std::true_type, ExtensionStateStore& store, Args&&... args)
      : handler_(
            store.getOrCreate<typename H::ConnState>(),
            std::forward<Args>(args)...) {
    static_assert(
        std::is_default_constructible_v<typename H::ConnState>,
        "H::ConnState must be default-constructible: the connection's state "
        "is created by whichever extension names it first, so there is no "
        "one extension whose arguments could construct it.");
  }

  [[no_unique_address]] H handler_;
  [[no_unique_address]] extension_detail::BackpressureState<H> bp_;
  [[no_unique_address]] extension_detail::ConnectionContextState<H> connection_;
  [[no_unique_address]] extension_detail::ConnectionClosedState<H> closed_;
};

/**
 * Build a factory that splices extension H into a connection's thrift pipeline.
 *
 * The peer of makeThriftPipelineHandlerFactory for the extension API: it wraps
 * H in the adapter and threads the connection's ExtensionStateStore into it, so
 * an H declaring a ConnState is handed that state at construction. `args` are
 * copied and forwarded to every per-connection instance, after the state.
 */
template <typename H, typename... Args>
StaticThriftPipelineHandlerFactory makeThriftExtensionHandlerFactory(
    channel_pipeline::HandlerId id, Args... args) {
  using Adapter =
      ThriftExtensionPipelineHandler<H, ThriftPipelineHandlerContext>;
  using StaticAdapter =
      ThriftExtensionPipelineHandler<H, StaticThriftPipelineHandlerContext>;
  static_assert(
      channel_pipeline::InboundHandler<Adapter, ThriftPipelineHandlerContext> ||
      channel_pipeline::
          OutboundHandler<Adapter, ThriftPipelineHandlerContext> ||
      channel_pipeline::DuplexHandler<Adapter, ThriftPipelineHandlerContext>);
  static_assert(
      channel_pipeline::
          InboundHandler<StaticAdapter, StaticThriftPipelineHandlerContext> ||
      channel_pipeline::
          OutboundHandler<StaticAdapter, StaticThriftPipelineHandlerContext> ||
      channel_pipeline::
          DuplexHandler<StaticAdapter, StaticThriftPipelineHandlerContext>);
  auto dynamicFactory = [id, args...](ExtensionStateStore& store) {
    return channel_pipeline::detail::makeHandlerNode<Adapter>(
        id, std::make_unique<Adapter>(store, args...));
  };
  auto staticFactory = [id, args...](ExtensionStateStore& store) {
    return channel_pipeline::detail::makeErasedStaticHandler<StaticAdapter>(
        id, store, args...);
  };
  return StaticThriftPipelineHandlerFactory(
      std::move(dynamicFactory), std::move(staticFactory));
}

} // namespace apache::thrift::fast_thrift::thrift::server
