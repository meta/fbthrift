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
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <folly/ExceptionWrapper.h>
#include <folly/Executor.h>
#include <folly/Portability.h>
#include <folly/Synchronized.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/DelayedDestruction.h>
#include <folly/io/async/EventBase.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineRef.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/adapter/util/ThriftServerMethodDispatchTable.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/Event.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/Messages.h>
#include <thrift/lib/thrift/gen-cpp2/RpcMetadata_types.h>

namespace apache::thrift::fast_thrift::thrift {

/**
 * ThriftServerAppAdapter — base class for generated fast server handlers.
 *
 * The generated <Service>AppAdapter extends this class, inheriting pipeline
 * integration, request dispatch, and the single writeResponse write entry
 * point. Payload construction lives in ResponsePayloads.h — callers build a
 * ThriftServerResponseMessage there, then hand it to writeResponse().
 *
 * Satisfies TailEndpointHandler concept (onRead / onException) and the
 * ThriftAppAdapter concept consumed by ThriftServerCompositeAppAdapter.
 *
 * The adapter holds no terminal-phase state of its own. Close coordination
 * (drain, in-flight reap, deferred closeCallback) is owned by the
 * pipeline-resident ThriftServerConnectionCloseHandler; the adapter just
 * emits the CloseConnection event from close() and fires the user
 * closeCallback in response to the inbound ConnectionClosed event.
 *
 * Lifetime: upper-layer owners must hold the adapter Ptr until
 * closeCallback fires. After that, the adapter may persist independently
 * of the pipeline until in-flight FastHandlerCallbacks drain — each one
 * holds a DestructorGuard on the adapter, so the underlying object stays
 * valid even if the upper-layer Ptr is dropped. Once the pipeline is
 * gone (signaled to the adapter via ConnectionClosed), pipelineActive_
 * is false and straggler writeResponse calls are silently dropped.
 *
 * Threading: when a CPU executor is attached, the generated service path runs
 * off the connection's EventBase. Two refcounts reachable from a request
 * are deliberately non-atomic — this adapter's DelayedDestruction
 * guardCount_, and the ThriftConnContext refcount held via
 * ThriftRequestContext. Both are per-connection, so concurrent requests
 * would contend on them.
 *
 * The invariant that keeps that sound: *both are mutated only on this
 * adapter's EventBase.* Three things uphold it, and all three must survive
 * refactoring together —
 *   1. FastHandlerCallback is constructed on the EventBase, before any
 *      offload, so acquiring the adapter guard happens there.
 *   2. FastHandlerCallback destruction returns to the EventBase, so an
 *      abandoned request destroys its EventBase-local request context there.
 *   3. A completed callback moves the request context into the response
 *      without destroying it. writeResponse moves that response back to the
 *      EventBase before pipeline processing or destruction.
 * Serialized responses return through writeResponse, which performs the hop
 * back to the EventBase.
 */
class ThriftServerAppAdapter : public folly::DelayedDestruction {
 public:
  // Canonical owning-pointer alias. Adapters are folly::DelayedDestruction
  // objects, so they can't be deleted with plain delete. Every site that
  // owns an adapter (FastThriftServer's per-connection state, factory
  // returns, composite children) should use this alias.
  using Ptr = std::
      unique_ptr<ThriftServerAppAdapter, folly::DelayedDestruction::Destructor>;

  // Per-method handler signature for SINGLE_REQUEST_SINGLE_RESPONSE RPCs.
  // The dispatcher pre-extracts the components codegen actually needs from
  // the inbound frame so the handler doesn't have to reach back into
  // ThriftServerRequestMessage / ParsedFrame.
  //   streamId       — Rocket stream id; echo into the response so the client
  //                    can match it to the in-flight request.
  //   data           — request payload IOBuf, ready to feed into a protocol
  //                    reader for argument deserialization.
  //   protocol       — wire protocol id (Binary / Compact / ...) for the
  //                    codec.
  //   requestContext — per-request context stamped by
  //                    ThriftServerRequestContextHandler; ownership moves into
  //                    the FastHandlerCallback so it outlives async handlers.
  using RequestResponseProcessFn = void (*)(
      ThriftServerAppAdapter*,
      uint32_t streamId,
      std::unique_ptr<folly::IOBuf> data,
      apache::thrift::ProtocolId protocol,
      ThriftRequestContextPtr requestContext) noexcept;

  ThriftServerAppAdapter() = default;
  explicit ThriftServerAppAdapter(
      std::shared_ptr<const ThriftServerMethodDispatchTable> dispatchTable)
      : dispatchTable_(std::move(dispatchTable)) {}
  ThriftServerAppAdapter(ThriftServerAppAdapter&&) = delete;
  ThriftServerAppAdapter& operator=(ThriftServerAppAdapter&&) = delete;

  void setCloseCallback(std::function<void()> cb);

  template <channel_pipeline::PipelineRefTarget P>
  void setPipeline(P* pipeline) noexcept {
    DCHECK(pipeline != nullptr);
    setPipeline(channel_pipeline::PipelineRef(*pipeline));
  }

  void setPipeline(channel_pipeline::PipelineRef pipeline) noexcept;

  // Attach the executor that generated dispatchers use for request
  // deserialization, method execution, and response serialization. When
  // unset, the service path stays inline on the connection's EventBase.
  //
  // Must be called before the connection starts reading — the executor is
  // read without synchronization on the dispatch path.
  void setCPUExecutor(folly::Executor::KeepAlive<> executor) noexcept {
    cpuExecutor_ = std::move(executor);
  }

  // Drops the pipeline reference and releases the DestructorGuard taken in
  // setPipeline. Must be called before the adapter is destroyed.
  //
  // EventBase-only: it mutates guardCount_. The EventBase keepalive remains
  // until adapter destruction so callbacks surviving a reap timeout can still
  // return there and observe pipelineActive_ == false.
  void resetPipeline() noexcept;

  channel_pipeline::PipelineRef pipeline() const noexcept { return pipeline_; }

  // === TailEndpointHandler interface ===

  // Unresolved entry point, used when this adapter is the pipeline tail on
  // its own. Resolves the method against the shared immutable dispatch table
  // and invokes its unbound dispatch thunk with this connection's adapter.
  channel_pipeline::Result dispatchRequest(
      channel_pipeline::TypeErasedBox&& msg) noexcept;

  template <typename Context>
  channel_pipeline::Result onRead(
      Context&, channel_pipeline::TypeErasedBox&& msg) noexcept {
    return dispatchRequest(std::move(msg));
  }

  void onException(folly::exception_wrapper&& e) noexcept;

  void handlerAdded() noexcept {}
  void handlerRemoved() noexcept {}
  void onPipelineActive() noexcept {}
  void onPipelineInactive() noexcept {}

  using PublishedEvents = channel_pipeline::Events<
      ThriftServerCloseConnectionEvent,
      ThriftServerRequestCompletedEvent>;
  using SubscribedEvents =
      channel_pipeline::Events<ThriftServerConnectionClosedEvent>;

  template <channel_pipeline::PipelineEvent E>
    requires std::same_as<E, ThriftServerConnectionClosedEvent>
  void on() noexcept {
    onConnectionClosed();
  }
  void onWriteReady() noexcept {}

  // Write entry point for callers that may be off the EventBase.
  //
  // `adapterGuard` must be a guard the caller already owns — typically the
  // one a FastHandlerCallback took at construction time. It is *moved*, never
  // constructed here: constructing a DestructorGuard bumps this adapter's
  // non-atomic guardCount_, which is only safe on the EventBase, whereas
  // moving one just transfers a pointer. Ownership rides into the hop so the
  // adapter cannot be destroyed before the write runs, and the guard is
  // released on the EventBase.
  //
  // Build the message via the free functions in
  // thrift/server/util/ResponsePayloads.h (makeResponseMessage,
  // makeErrorMessage, makeFrameworkErrorMessage, makeUnknownExceptionMessage,
  // makeSuccessResponseMessage<>, makeDeclaredExceptionMessage<>, ...).
  void writeResponse(
      ThriftServerResponseMessage&& message,
      folly::DelayedDestruction::DestructorGuard&& adapterGuard) noexcept;

  // Write entry point for callers already on the EventBase, which therefore
  // need no guard to survive a hop. DCHECKs the thread.
  void writeResponse(ThriftServerResponseMessage&& message) noexcept;

  // Completes a request after its cancellation token has been observed. No
  // wire response is produced; pipeline bookkeeping is retired by event.
  void acknowledgeCancellation(
      uint32_t streamId,
      ThriftRequestContextPtr requestContext,
      folly::DelayedDestruction::DestructorGuard&& adapterGuard) noexcept;

  // Initiate connection close. Internally fires a
  // ThriftServerCloseConnectionEvent pipeline event; the
  // pipeline-resident ThriftServerConnectionCloseHandler picks it up and runs
  // the terminal state machine (drain timeout → reap → force-close on stuck
  // handler callbacks). The user closeCallback fires when the connection has
  // fully settled. No-op if the pipeline is not yet wired.
  void close() noexcept;

  const std::shared_ptr<const ThriftServerMethodDispatchTable>&
  methodDispatchTable() const noexcept {
    return dispatchTable_;
  }

  bool hasMethod(std::string_view name) const noexcept;

  static FOLLY_ALWAYS_INLINE channel_pipeline::Result dispatchRequestResponse(
      ThriftServerAppAdapter* owner,
      RequestResponseProcessFn method,
      channel_pipeline::TypeErasedBox&& msg) noexcept;

  template <typename Adapter, auto Process>
  static constexpr ThriftServerMethodDispatchTable::Method
  makeRequestResponseMethod(std::string_view name) noexcept {
    return {
        name,
        +[](ThriftServerAppAdapter* owner,
            channel_pipeline::TypeErasedBox&& msg) noexcept {
          return dispatchRequestResponse(
              owner,
              +[](ThriftServerAppAdapter* adapter,
                  uint32_t streamId,
                  std::unique_ptr<folly::IOBuf> data,
                  apache::thrift::ProtocolId protocol,
                  ThriftRequestContextPtr requestContext) noexcept {
                (static_cast<Adapter*>(adapter)->*Process)(
                    streamId,
                    std::move(data),
                    protocol,
                    std::move(requestContext));
              },
              std::move(msg));
        }};
  }

 protected:
  void addMethodHandler(
      std::string_view name, RequestResponseProcessFn handler);

  channel_pipeline::PipelineRef pipeline_;
  // Keeps pipeline_ alive while pipelineActive_ is true. Released on
  // ConnectionClosed (or in resetPipeline) so the pipeline can die
  // independently of any straggler FastHandlerCallbacks. Straggler
  // writes are gated by pipelineActive_ and never touch pipeline_
  // after release.
  channel_pipeline::PipelineGuard pipelineGuard_;
  // EVB-only. Set true in setPipeline(), cleared on ConnectionClosed
  // (and on adapter destruction via resetPipeline). When false,
  // writeResponseOnEventBase drops without touching pipeline_.
  bool pipelineActive_{false};

  ~ThriftServerAppAdapter() override;

  folly::EventBase* getEventBase() const { return evb_.get(); }

  // Executor for the generated service path, or null when it should run
  // inline on the EventBase.
  folly::Executor* cpuExecutor() const noexcept { return cpuExecutor_.get(); }

 private:
  FOLLY_NOINLINE void handleWrongRpcKind(
      uint32_t streamId,
      apache::thrift::RpcKind kind,
      ThriftRequestContextPtr requestContext) noexcept;
  FOLLY_NOINLINE void handleUnknownMethod(
      uint32_t streamId,
      std::string_view methodName,
      ThriftRequestContextPtr requestContext) noexcept;
  FOLLY_NOINLINE void handleMissingPipeline() noexcept;

  void onConnectionClosed() noexcept;

  // Must be called on evb_.
  void writeResponseOnEventBase(ThriftServerResponseMessage&& message) noexcept;
  void acknowledgeCancellationOnEventBase(
      uint32_t streamId, ThriftRequestContextPtr requestContext) noexcept;

  folly::Executor::KeepAlive<folly::EventBase> evb_{};
  // Null unless the server was configured with a CPU executor. Written once
  // at wiring time, read on every dispatch.
  folly::Executor::KeepAlive<> cpuExecutor_{};
  std::shared_ptr<const ThriftServerMethodDispatchTable> dispatchTable_;
  // Compatibility path for hand-written adapters. Generated adapters use a
  // shared immutable table and leave this vector empty.
  std::unique_ptr<std::vector<std::pair<std::string, RequestResponseProcessFn>>>
      localMethods_;
  folly::Synchronized<std::function<void()>> closeCallback_;

  void fireCloseCallback() noexcept;
};

FOLLY_ALWAYS_INLINE channel_pipeline::Result
ThriftServerAppAdapter::dispatchRequestResponse(
    ThriftServerAppAdapter* owner,
    RequestResponseProcessFn method,
    channel_pipeline::TypeErasedBox&& msg) noexcept {
  auto request = msg.take<ThriftServerRequestMessage>();
  DCHECK(request.streamId != 0) << "Invalid stream ID";
  DCHECK(owner != nullptr);
  DCHECK(method != nullptr);

  // A method dispatches identically whether it opens a unary exchange
  // (REQUEST_RESPONSE) or a server stream (REQUEST_STREAM): the generated
  // process function takes the same (streamId, data, protocol, context) and
  // picks its response shape. The two request payloads differ only in that the
  // stream payload also carries initialRequestN, which the mux — not the
  // adapter — consumes. Extract the shared fields and dispatch.
  auto& inbound = request.payload;
  const apache::thrift::RequestRpcMetadata* metadata = nullptr;
  std::unique_ptr<folly::IOBuf> data;
  ThriftServerRequestResponsePayload* requestResponse = nullptr;
  if (inbound.is<ThriftServerRequestResponsePayload>()) {
    requestResponse = &inbound.get<ThriftServerRequestResponsePayload>();
    metadata = requestResponse->metadata.get();
    data = std::move(requestResponse->data);
  } else if (inbound.is<ThriftRequestStreamPayload>()) {
    auto& requestStream = inbound.get<ThriftRequestStreamPayload>();
    metadata = requestStream.metadata.get();
    data = std::move(requestStream.data);
  } else {
    // Result::Error propagates to TransportHandler, which closes the
    // connection.
    return channel_pipeline::Result::Error;
  }
  DCHECK(metadata != nullptr);
  const auto protocol = metadata->protocol().value_or(0);
  method(
      owner,
      request.streamId,
      std::move(data),
      protocol,
      std::move(request.requestContext));
  // Nothing past the process function reads the metadata: the method name and
  // headers already moved into the request context.
  if (requestResponse != nullptr) {
    recycleServerRequestMetadata(requestResponse->metadata);
  }
  return channel_pipeline::Result::Success;
}

} // namespace apache::thrift::fast_thrift::thrift
