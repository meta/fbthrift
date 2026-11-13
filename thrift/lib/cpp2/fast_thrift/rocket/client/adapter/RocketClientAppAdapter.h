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

#include <memory>

#include <folly/ExceptionWrapper.h>
#include <folly/Function.h>
#include <folly/io/async/DelayedDestruction.h>
#include <folly/lang/Assume.h>
#include <folly/logging/xlog.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineRef.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/client/Event.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/client/Messages.h>

namespace apache::thrift::fast_thrift::rocket::client {

/**
 * RocketClientAppAdapter — standalone rocket client endpoint adapter.
 *
 * Acts as the tail endpoint of a Rocket client pipeline, relaying responses
 * and control events to callbacks provided by its owner.
 *
 * Satisfies TailEndpointHandler (onRead/onException) and
 * RocketClientAppOutboundHandler (write).
 *
 * Inbound path (responses): The pipeline delivers messages to this adapter
 * via onRead(). The adapter forwards them to the registered onResponse
 * callback.
 *
 * Outbound path (requests): The owner calls write() to push
 * RocketRequestMessages into the pipeline.
 *
 * Usage:
 *   // 1. Create adapter
 *   RocketClientAppAdapter adapter;
 *   adapter.setOnResponse([&](TypeErasedBox&& msg) noexcept { ... });
 *   adapter.setOnError([&](exception_wrapper&& e) noexcept { ... });
 *
 *   // 2. Build rocket pipeline with adapter as tail
 *   auto pipeline = PipelineBuilder<
 *       TransportHandler, RocketClientAppAdapter, Allocator>()
 *       .setHead(transportHandler)
 *       .setTail(&adapter)
 *       ...
 *       .build();
 *
 *   // 3. Wire pipeline
 *   adapter.setPipeline(pipeline.get());
 *
 *   // 4. Send requests
 *   adapter.write(RocketRequestMessage{...});
 */
class RocketClientAppAdapter : public folly::DelayedDestruction {
 public:
  using Ptr = std::unique_ptr<RocketClientAppAdapter, Destructor>;

  using OnResponseFn = folly::Function<channel_pipeline::Result(
      channel_pipeline::TypeErasedBox&&) noexcept>;
  using OnErrorFn = folly::Function<void(folly::exception_wrapper&&) noexcept>;

  // Lifecycle callbacks — relay channel_pipeline lifecycle transitions to
  // the owner using rocket-domain terminology (connect/disconnect map to
  // the underlying onPipelineActive/onPipelineInactive). Distinct from
  // OnError: lifecycle events carry no exception, they just signal "the
  // connection became connected / disconnected".
  using OnConnectFn = folly::Function<void() noexcept>;
  using OnDisconnectFn = folly::Function<void() noexcept>;

  // Notification that the transport is about to close: the server sent
  // ERROR(CONNECTION_CLOSE) (graceful drain). NOT a lifecycle/destruction
  // edge — the connection is still live and in-flight work keeps flowing.
  // The receiver handles the graceful-close notification.
  using OnCloseFn = folly::Function<void() noexcept>;

  // Called when the Rocket transport becomes writable. The receiver can
  // propagate readiness to pending work.
  using OnWriteReadyFn = folly::Function<void() noexcept>;

  // Write-completion relay: invoked once per completed request write with
  // the RocketWriteCompleteEvent (requestContext, status).
  using OnWriteCompleteFn =
      folly::Function<void(const RocketWriteCompleteEvent&) noexcept>;

  using OnMetadataPushFn =
      folly::Function<channel_pipeline::Result(RocketMetadataPush&) noexcept>;

  RocketClientAppAdapter() = default;

  RocketClientAppAdapter(const RocketClientAppAdapter&) = delete;
  RocketClientAppAdapter& operator=(const RocketClientAppAdapter&) = delete;
  RocketClientAppAdapter(RocketClientAppAdapter&&) = delete;
  RocketClientAppAdapter& operator=(RocketClientAppAdapter&&) = delete;

  template <channel_pipeline::PipelineRefTarget P>
  void setPipeline(P* pipeline) noexcept {
    DCHECK(pipeline != nullptr);
    setPipeline(channel_pipeline::PipelineRef(*pipeline));
  }

  void setPipeline(channel_pipeline::PipelineRef pipeline) noexcept {
    DCHECK(pipeline);
    if (pipeline_) {
      XLOG(FATAL) << "must reset pipeline before setting a new one";
    }
    pipeline_ = pipeline;
    eventPublisher_ = pipeline_.template bindEvents<PublishedEvents>();
    pipelineGuard_ = pipeline_.guard();
  }

  /**
   * Release this adapter's hold on the pipeline. After this returns,
   * the pipeline may be destroyed (modulo other guards) and write() must
   * not be called.
   */
  void resetPipeline() noexcept {
    eventPublisher_ = {};
    pipeline_.reset();
    pipelineGuard_.reset();
  }

  void setResponseHandlers(
      OnResponseFn respHandler, OnErrorFn errHandler) noexcept {
    onResponse_ = std::move(respHandler);
    onError_ = std::move(errHandler);
  }

  void setLifecycleHandlers(
      OnConnectFn connectHandler, OnDisconnectFn disconnectHandler) noexcept {
    onConnect_ = std::move(connectHandler);
    onDisconnect_ = std::move(disconnectHandler);
  }

  void setOnWriteReady(OnWriteReadyFn fn) noexcept {
    onWriteReady_ = std::move(fn);
  }

  // Install the "transport about to close" (graceful-drain) notification.
  void setOnClose(OnCloseFn fn) noexcept { onClose_ = std::move(fn); }

  void setOnWriteComplete(OnWriteCompleteFn fn) noexcept {
    onWriteComplete_ = std::move(fn);
  }

  void setOnMetadataPush(OnMetadataPushFn fn) noexcept {
    onMetadataPush_ = std::move(fn);
  }

  /**
   * Fire onReadReady() on the rocket pipeline. The pipeline walks any
   * handlers awaiting read-ready notification and then signals the head
   * (TransportHandler::onReadReady → resumeRead). The receiver calls this
   * when it can accept inbound delivery again.
   */
  void notifyReadReady() noexcept {
    if (pipeline_) {
      pipeline_.onReadReady();
    }
  }

  // === RocketClientAppOutboundHandler interface ===

  /**
   * Write a rocket request message into the pipeline (outbound path).
   */
  channel_pipeline::Result write(RocketRequestMessage&& msg) noexcept {
    DCHECK(pipeline_);
    return pipeline_.fireWrite(channel_pipeline::erase_and_box(std::move(msg)));
  }

  void cancelRequest(void* requestContext) noexcept {
    if (pipeline_ != nullptr) {
      eventPublisher_.template fire<RocketCancelRequestEvent>(
          RocketCancelRequestEvent{.requestContext = requestContext});
    }
  }

  // === TailEndpointHandler interface ===

  /**
   * Called by the pipeline when a response message arrives (inbound path).
   * Forwards to the registered onResponse callback.
   */
  template <typename Context>
  channel_pipeline::Result onRead(
      Context&, channel_pipeline::TypeErasedBox&& msg) noexcept {
    if (!onResponse_) [[unlikely]] {
      return channel_pipeline::Result::Error;
    }
    return onResponse_(std::move(msg));
  }

  /**
   * Called when an exception propagates through the pipeline.
   * Forwards to the registered onError callback.
   */
  void onException(folly::exception_wrapper&& e) noexcept {
    if (!onError_) [[unlikely]] {
      return;
    }
    onError_(std::move(e));
  }

  void handlerAdded() noexcept {}

  void handlerRemoved() noexcept {
    DCHECK(disconnected_);
    // Defensive: pipeline removed us without a prior deactivate. Fan out
    // onDisconnect_ so the owner observes the disconnect.
    if (!disconnected_) {
      disconnected_ = true;
      if (onDisconnect_) {
        onDisconnect_();
      }
    }
    // Drop callbacks so any post-destroy use-after-detach (e.g. a queued
    // EventBase callback that still holds a stale captured `this`) sees
    // empty Functions and no-ops rather than firing into stale state.
    onResponse_ = {};
    onError_ = {};
    onConnect_ = {};
    onDisconnect_ = {};
    onClose_ = {};
    onWriteReady_ = {};
    onWriteComplete_ = {};
    onMetadataPush_ = {};
  }

  void onPipelineActive() noexcept {
    disconnected_ = false;
    if (onConnect_) {
      onConnect_();
    }
  }

  void onPipelineInactive() noexcept {
    disconnected_ = true;
    if (onDisconnect_) {
      onDisconnect_();
    }
  }

  void onWriteReady() noexcept {
    if (onWriteReady_) {
      onWriteReady_();
    }
  }

  using SubscribedEvents = channel_pipeline::
      Events<ConnectionCloseEvent, RocketWriteCompleteEvent, MetadataPushEvent>;

  template <channel_pipeline::PipelineEvent E>
    requires std::same_as<E, ConnectionCloseEvent>
  void on() noexcept {
    if (onClose_) {
      onClose_();
    }
  }

  template <channel_pipeline::PipelineEvent E>
    requires std::same_as<E, RocketWriteCompleteEvent>
  void on(const RocketWriteCompleteEvent& event) noexcept {
    if (!onWriteComplete_) [[unlikely]] {
      return;
    }
    onWriteComplete_(event);
  }

  template <channel_pipeline::PipelineEvent E>
    requires std::same_as<E, MetadataPushEvent>
  void on(RocketMetadataPush* event) noexcept {
    DestructorGuard guard(this);
    if (onMetadataPush_) {
      event->result = onMetadataPush_(*event);
    }
  }

 protected:
  ~RocketClientAppAdapter() override {
    DCHECK(!pipeline_);
    resetPipeline();
  }

 private:
  using PublishedEvents = channel_pipeline::Events<RocketCancelRequestEvent>;
  using EventPublisher =
      channel_pipeline::PipelineEventPublisherRef<PublishedEvents>;

  channel_pipeline::PipelineRef pipeline_;
  EventPublisher eventPublisher_;
  channel_pipeline::PipelineGuard pipelineGuard_;
  OnResponseFn onResponse_;
  OnErrorFn onError_;
  OnConnectFn onConnect_;
  OnDisconnectFn onDisconnect_;
  OnCloseFn onClose_;
  OnWriteReadyFn onWriteReady_;
  OnWriteCompleteFn onWriteComplete_;
  OnMetadataPushFn onMetadataPush_;
  bool disconnected_{true};
};

} // namespace apache::thrift::fast_thrift::rocket::client
