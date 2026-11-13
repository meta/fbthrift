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

#include <folly/ExceptionWrapper.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Event.h>

namespace apache::thrift::fast_thrift::channel_pipeline {

/**
 * HandlerLifecycle concept - defines lifecycle callbacks for handlers.
 *
 * Handlers implement these callbacks to:
 * - Initialize and cache references to other handlers (handlerAdded)
 * - Clean up cached pointers before destruction (handlerRemoved)
 *
 * Guarantees:
 * - handlerAdded() is called in order handlers are added
 * - handlerRemoved() is called in REVERSE order (LIFO)
 * - Between these calls, ctx.pipeline() is guaranteed non-null
 */
template <typename H, typename Ctx>
concept HandlerLifecycle = requires(H h, Ctx& ctx) {
  // Called after handler is added to pipeline - safe to cache other contexts
  { h.handlerAdded(ctx) } noexcept -> std::same_as<void>;

  // Called before handler is removed (reverse order) - clear cached pointers
  { h.handlerRemoved(ctx) } noexcept -> std::same_as<void>;
};

/**
 * InboundHandler concept - handles data flowing from head to tail.
 *
 * Inbound handlers process:
 * - onRead: Incoming data/messages
 * - onReadReady: Notification that reads can resume
 * - onException: Notification of an exception
 * - onPipelineActive: Notification that pipeline is activated
 *
 * Handlers return Result to signal backpressure:
 * - Result::Success — continue processing
 * - Result::Backpressure — slow down, upstream should pause
 * - Result::Error — something went wrong
 *
 * Read backpressure reaches the socket through TCP flow control. When a
 * handler returns Result::Backpressure from onRead, the transport adapter
 * should call pauseRead() on the socket.
 *
 * A handler can also wait with ctx.awaitReadReady(), for example after its
 * ctx.fireRead() returned Result::Backpressure. The code that clears the
 * block starts a read-ready pass with ctx.pipeline()->onReadReady().
 *
 * The pass wakes every waiting handler from the tail toward the head, so each
 * handler wakes after the handler that it reads into.
 *
 * onException returns void because exceptions must always be
 * handled/propagated.
 */
template <typename H, typename Ctx>
concept InboundHandler =
    HandlerLifecycle<H, Ctx> &&
    requires(
        H h, Ctx& ctx, TypeErasedBox&& message, folly::exception_wrapper&& e) {
      // Returns Result to propagate backpressure
      { h.onRead(ctx, std::move(message)) } noexcept -> std::same_as<Result>;
      // Notifications — must always be handled
      { h.onReadReady(ctx) } noexcept -> std::same_as<void>;
      { h.onException(ctx, std::move(e)) } noexcept -> std::same_as<void>;
      { h.onPipelineActive(ctx) } noexcept -> std::same_as<void>;
    };

/**
 * OutboundHandler concept - handles data flowing from tail to head.
 *
 * Outbound handlers process:
 * - onWrite: Outgoing data/messages
 * - onWriteReady: Notification that more data can be written
 * - onPipelineInactive: Notification that pipeline is deactivated
 *
 * Handlers return Result to signal backpressure:
 * - Result::Success — continue processing
 * - Result::Backpressure — slow down, upstream should pause
 * - Result::Error — something went wrong
 */
template <typename H, typename Ctx>
concept OutboundHandler = HandlerLifecycle<H, Ctx> &&
    requires(H h, Ctx& ctx, TypeErasedBox&& message) {
      // Returns Result to propagate backpressure
      { h.onWrite(ctx, std::move(message)) } noexcept -> std::same_as<Result>;
      // Notifications — must always be handled
      { h.onWriteReady(ctx) } noexcept -> std::same_as<void>;
      { h.onPipelineInactive(ctx) } noexcept -> std::same_as<void>;
    };

/**
 * DuplexHandler concept - handles both inbound and outbound data.
 *
 * A handler that satisfies both InboundHandler and OutboundHandler.
 * handlerAdded() and handlerRemoved() are called once per handler instance.
 */
template <typename H, typename Ctx>
concept DuplexHandler = InboundHandler<H, Ctx> && OutboundHandler<H, Ctx>;

template <typename H>
concept TypeEventPublisher = requires { typename H::PublishedEvents; } &&
    kIsEventSet<typename H::PublishedEvents>;

template <typename H, typename Ctx, PipelineEvent E>
constexpr bool subscriberHasOn() {
  if constexpr (std::is_void_v<typename E::Payload>) {
    return requires(H& h, Ctx& ctx) {
      { h.template on<E>(ctx) } noexcept -> std::same_as<void>;
    };
  } else {
    return requires(H& h, Ctx& ctx, const typename E::Payload& payload) {
      { h.template on<E>(ctx, payload) } noexcept -> std::same_as<void>;
    };
  }
}

template <typename H, PipelineEvent E>
constexpr bool endpointHasOn() {
  if constexpr (std::is_void_v<typename E::Payload>) {
    return requires(H& h) {
      { h.template on<E>() } noexcept -> std::same_as<void>;
    };
  } else {
    return requires(H& h, const typename E::Payload& payload) {
      { h.template on<E>(payload) } noexcept -> std::same_as<void>;
    };
  }
}

template <typename H, typename Ctx, PipelineEvent... Evs>
constexpr bool subscriberHasOn(Events<Evs...>) {
  return (subscriberHasOn<H, Ctx, Evs>() && ...);
}

template <typename H, PipelineEvent... Evs>
constexpr bool endpointHasOn(Events<Evs...>) {
  return (endpointHasOn<H, Evs>() && ...);
}

/**
 * A type-based event subscriber. SubscribedEvents explicitly supplies the
 * event types because C++ cannot enumerate on<E>() specializations.
 */
template <typename H, typename Ctx>
concept TypeEventSubscriber = requires { typename H::SubscribedEvents; } &&
    kIsEventSet<typename H::SubscribedEvents> &&
    subscriberHasOn<H, Ctx>(typename H::SubscribedEvents{});

template <typename H>
concept EndpointTypeEventSubscriber =
    requires { typename H::SubscribedEvents; } &&
    kIsEventSet<typename H::SubscribedEvents> &&
    endpointHasOn<H>(typename H::SubscribedEvents{});

} // namespace apache::thrift::fast_thrift::channel_pipeline
