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
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include <fmt/core.h>
#include <glog/logging.h>
#include <folly/ExceptionWrapper.h>

#include <thrift/lib/cpp/TApplicationException.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Event.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/ContextImpl.h>
#include <thrift/lib/cpp2/fast_thrift/frame/read/DirectStreamMap.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/common/StreamPayloadMetadata.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/common/ThriftControlPayloads.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/common/ThriftRequestPayloads.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/common/ThriftResponsePayloads.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/Event.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/Messages.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/StreamResponsePayloads.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/util/ResponseMetadata.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/stream/ProducerPipeline.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/stream/common/Messages.h>

namespace apache::thrift::fast_thrift::thrift {

/**
 * ThriftServerStreamMuxHandler — a duplex handler in the thrift server pipeline
 * that owns and multiplexes per-stream producing-end sub-pipelines.
 *
 * Placement: just head-ward of the tail app adapter. Outbound stream chunks it
 * injects then traverse every outbound handler (checksum/compression/transport)
 * like a unary response; inbound RequestN reaches it after the upper handlers
 * and is routed here rather than forwarded to the app.
 *
 * Outbound (onWrite): a `ThriftServerStreamOpenPayload` opens a stream — the
 * mux runs its `ProducerPipeline::ConfigFunc` to build and own a sub-pipeline
 * (keyed by streamId), wires the sub-pipeline head's sink back onto this
 * pipeline's write path, and forwards the initial response chunk upward. Every
 * other outbound message passes through.
 *
 * Inbound (onRead): `RequestN` is looked up by streamId and injected into the
 * matching sub-pipeline (not forwarded to the app); an unknown/terminated
 * stream surfaces a protocol error. Unary requests pass through.
 *
 * Cancellation: a wire CANCEL frame is indistinguishable from a unary request
 * cancel (same rocket frame), so the transport adapter decodes it to a
 * broadcast `ThriftServerRequestCancellationEvent`. The mux subscribes and
 * claims the event only when its streamId names a stream it owns — injecting a
 * `stream::Cancel` and tearing the stream down. A streamId it doesn't own is a
 * unary cancel handled by the request-lifecycle handler, so the mux ignores it;
 * the two streamId spaces are disjoint.
 *
 * Teardown: a terminal (`Complete`/`Error` out, or `Cancel` in) surfaces while
 * we are executing inside the sub-pipeline's `fireRead` —
 * `fireRead`/`fireWrite` hold no DestructorGuard, so erasing the entry there
 * would destroy the pipeline mid-call. The terminal only marks the streamId for
 * removal; the mux erases marked entries after the driving `onRead` returns.
 * (Loop-deferred teardown for producers that emit terminals asynchronously —
 * outside an inbound drive — is a follow-up; today's producers emit
 * synchronously under demand.)
 */
template <typename Context>
class ThriftServerStreamMuxHandler {
 public:
  using SubscribedEvents =
      channel_pipeline::Events<ThriftServerRequestCancellationEvent>;

  void handlerAdded(Context& ctx) noexcept { mainCtx_ = &ctx; }
  void handlerRemoved(Context& /*ctx*/) noexcept {}
  void onPipelineActive(Context& /*ctx*/) noexcept {}
  void onReadReady(Context& /*ctx*/) noexcept {}
  void onWriteReady(Context& /*ctx*/) noexcept {}

  void onPipelineInactive(Context& /*ctx*/) noexcept {
    // No drive is in flight at deactivation, so tearing every sub-pipeline down
    // directly is safe. DelayedDestruction defers any child-internal cleanup.
    streams_.clear();
  }

  void onException(Context& ctx, folly::exception_wrapper&& e) noexcept {
    ctx.fireException(std::move(e));
  }

  channel_pipeline::Result onWrite(
      Context& ctx, channel_pipeline::TypeErasedBox&& msg) noexcept {
    auto& response = msg.get<ThriftServerResponseMessage>();
    if (!response.payload.is<ThriftServerStreamOpenPayload>()) {
      return ctx.fireWrite(std::move(msg));
    }
    auto& open = response.payload.get<ThriftServerStreamOpenPayload>();
    const uint32_t streamId = open.initialResponse.streamId;
    auto configFunc = std::move(open.configFunc);
    auto initialResponse = std::move(open.initialResponse);
    openStream(streamId, std::move(configFunc));
    // Forward the initial chunk upward as an ordinary stream response.
    response.payload = std::move(initialResponse);
    auto result = ctx.fireWrite(std::move(msg));
    // Deliver the client's initial REQUEST_N — captured from the inbound
    // REQUEST_STREAM — now that the stream is open and its headers are on the
    // wire. This must run after the initial response is forwarded: producer
    // chunks have to follow the stream's first response, never precede it.
    applyPendingInitialCredit(streamId);
    drainPendingRemoval();
    return result;
  }

  channel_pipeline::Result onRead(
      Context& ctx, channel_pipeline::TypeErasedBox&& msg) noexcept {
    auto& request = msg.get<ThriftServerRequestMessage>();
    if (request.payload.is<ThriftRequestNPayload>()) {
      const uint32_t n = request.payload.get<ThriftRequestNPayload>().requestN;
      const bool routed = routeInbound(
          request.streamId,
          stream::ControlMessage{.payload = stream::RequestN{.n = n}});
      drainPendingRemoval();
      if (!routed) [[unlikely]] {
        return fireUnknownStream(ctx, request.streamId);
      }
      return channel_pipeline::Result::Success;
    }
    if (request.payload.is<ThriftRequestStreamPayload>()) {
      // The stream-open request carries the client's initial demand, but the
      // sub-pipeline it credits does not exist until the handler responds and
      // openStream() runs. Create the stream entry now (synchronously, before
      // the request is dispatched) holding the demand; openStream() fills in
      // the pipeline and applyPendingInitialCredit() injects the demand once
      // the stream is open. The request itself still flows to the app adapter
      // via the fireRead below.
      const uint32_t n =
          request.payload.get<ThriftRequestStreamPayload>().initialRequestN;
      const bool inserted =
          streams_
              .emplace(request.streamId, StreamEntry{.pendingInitialCredit = n})
              .second;
      DCHECK(inserted) << "duplicate stream id " << request.streamId;
    }
    return ctx.fireRead(std::move(msg));
  }

  // Stream cancel. The event is broadcast to every subscriber (the transport
  // adapter can't tell a stream cancel from a unary one — same CANCEL frame),
  // so claim it only when streamId names a stream this mux owns. A streamId it
  // does not own is a unary request cancel handled by the request-lifecycle
  // handler; ignore it. routeInbound already reports false for
  // unknown/terminated streams, so no explicit ownership check is needed.
  template <channel_pipeline::PipelineEvent E>
    requires std::same_as<E, ThriftServerRequestCancellationEvent>
  void on(
      Context&, const ThriftServerRequestCancellationEvent& event) noexcept {
    const bool routed = routeInbound(
        event.streamId, stream::ControlMessage{.payload = stream::Cancel{}});
    if (routed) {
      markTerminated(event.streamId);
      drainPendingRemoval();
    }
  }

 private:
  // One established stream's producing-end sub-pipeline, owned by the mux and
  // stored by value in `streams_`. No external raw pointer aims at the entry:
  // the head's sink captures (this, streamId) by value rather than the entry,
  // so the map is free to relocate entries on grow/erase. `ProducerPipeline` is
  // noexcept-movable behind stable heap pointers, so relocating an entry only
  // moves a few pointers while the pipeline it owns stays put. The
  // ProducerPipeline owns its endpoints and pipeline and tears them down in the
  // correct order internally.
  struct StreamEntry {
    std::optional<stream::ProducerPipeline> pipeline{std::nullopt};
    // Initial REQUEST_N demand seen on the REQUEST_STREAM, held until the
    // stream opens and it is injected (once, then zeroed). Zero when the client
    // opened with no initial credit.
    uint32_t pendingInitialCredit{0};
    bool terminated{false};
  };

  void openStream(
      uint32_t streamId,
      std::unique_ptr<stream::ProducerPipeline::ConfigFunc>
          configFunc) noexcept {
    // The framework wires the endpoints/allocator/executor and the head's
    // output sink; the configFunc only composes the application's producer
    // between them. The sink captures (this, streamId) by value — never the
    // entry — so the entry needs no stable address.
    stream::ProducerPipeline::Builder builder(
        mainCtx_->eventBase(),
        stream::StreamSink{
            [this, streamId](stream::DataMessage&& msg) noexcept {
              return emitOutbound(streamId, std::move(msg));
            }});
    (*configFunc)(builder);
    // The entry usually already exists — the inbound REQUEST_STREAM created it
    // to stash the initial credit. It is absent only when the mux never saw
    // that request (e.g. a stream opened directly). `emplace` is find-or-create
    // and a no-op on collision, so it returns that entry either way. A live
    // streamId opens exactly once: a second open would find an entry that
    // already holds a pipeline — a framework bug, not client input. Guard it in
    // debug.
    auto* slot = streams_.emplace(streamId, StreamEntry{}).first;
    DCHECK(!slot->second.pipeline.has_value())
        << "duplicate stream id " << streamId;
    slot->second.pipeline = builder.build();
  }

  // Route an inbound flow-control message into the matching sub-pipeline.
  // Returns false when no open stream owns `streamId` — unknown, already
  // terminated, or seen (its entry exists) but not yet opened — the caller
  // surfaces that as an error rather than dropping it silently.
  bool routeInbound(uint32_t streamId, stream::ControlMessage&& msg) noexcept {
    auto* slot = streams_.find(streamId);
    if (slot == nullptr || slot->second.terminated ||
        !slot->second.pipeline.has_value()) {
      return false;
    }
    (void)slot->second.pipeline->fireRead(
        channel_pipeline::erase_and_box(std::move(msg)));
    return true;
  }

  // A RequestN arrived for a streamId this mux has no active stream for
  // (never opened, or already torn down). Rather than drop it silently, surface
  // it up the pipeline so the connection sees the protocol violation.
  channel_pipeline::Result fireUnknownStream(
      Context& ctx, uint32_t streamId) noexcept {
    ctx.fireException(
        folly::make_exception_wrapper<apache::thrift::TApplicationException>(
            apache::thrift::TApplicationException::PROTOCOL_ERROR,
            fmt::format(
                "stream flow-control frame for unknown stream {}", streamId)));
    return channel_pipeline::Result::Success;
  }

  // Convert a producer-emitted stream message to an outbound response chunk and
  // inject it on this pipeline's write path (head-ward from the mux). Marks the
  // stream for teardown on a terminal.
  channel_pipeline::Result emitOutbound(
      uint32_t streamId, stream::DataMessage&& msg) noexcept {
    ThriftServerResponseMessage response;
    bool terminal = false;
    auto& payload = msg.payload;
    if (payload.is<stream::Payload>()) {
      auto& streamPayload = payload.get<stream::Payload>();
      response.payload = ThriftStreamPayload{
          .data = std::move(streamPayload.data),
          .metadata = std::move(streamPayload.metadata),
          .streamId = streamId,
          .complete = false,
          .next = true};
    } else if (payload.is<stream::Complete>()) {
      response.payload = ThriftStreamPayload{
          .streamId = streamId, .complete = true, .next = false};
      terminal = true;
    } else if (payload.is<stream::Error>()) {
      // Match regular Thrift's undeclared-exception encoding: a terminal
      // PAYLOAD chunk carrying appUnknownException metadata (name/what/blame),
      // not an rsocket ERROR frame (that channel is for framework/connection
      // errors). The mux is method-generic, so every producer exception is
      // surfaced as undeclared; declared-exception fidelity needs the
      // per-method producing-end adapter and is a follow-up.
      auto& ex = payload.get<stream::Error>().ex;
      auto metadata = std::make_unique<apache::thrift::StreamPayloadMetadata>();
      metadata->payloadMetadata() = buildAppUnknownExceptionPayloadMetadata(
          ex.class_name().toStdString(), ex.what().toStdString());
      response.payload = ThriftStreamPayload{
          .metadata = std::move(metadata),
          .streamId = streamId,
          .complete = true,
          .next = true};
      terminal = true;
    }
    auto result = mainCtx_->fireWrite(
        channel_pipeline::erase_and_box(std::move(response)));
    if (terminal) {
      markTerminated(streamId);
    }
    return result;
  }

  // Inject the initial demand captured from the inbound REQUEST_STREAM into the
  // now-open sub-pipeline, then forget it. No-op when the client opened with
  // zero initial credit.
  void applyPendingInitialCredit(uint32_t streamId) noexcept {
    auto* slot = streams_.find(streamId);
    if (slot == nullptr || slot->second.pendingInitialCredit == 0) {
      return;
    }
    const uint32_t n = std::exchange(slot->second.pendingInitialCredit, 0);
    routeInbound(
        streamId, stream::ControlMessage{.payload = stream::RequestN{.n = n}});
  }

  void markTerminated(uint32_t streamId) noexcept {
    auto* slot = streams_.find(streamId);
    if (slot == nullptr || slot->second.terminated) {
      return;
    }
    slot->second.terminated = true;
    pendingRemoval_.push_back(streamId);
  }

  // Erase entries marked terminal during the just-finished inbound drive. Safe
  // here: the sub-pipeline's fireRead has returned, so no entry is on the stack
  // and the erase's backshift can freely relocate the remaining entries.
  void drainPendingRemoval() noexcept {
    for (uint32_t streamId : pendingRemoval_) {
      streams_.erase(streamId);
    }
    pendingRemoval_.clear();
  }

  Context* mainCtx_{nullptr};
  // One entry per stream the mux has seen, keyed by streamId: created by the
  // inbound REQUEST_STREAM (which stashes the initial credit), given its
  // sub-pipeline at stream-open, and erased at teardown. Holding the initial
  // credit here rather than in a second map keeps per-connection footprint to a
  // single table. An entry whose handler never opens the stream lingers until
  // the connection closes; bounding that is a follow-up.
  frame::read::DirectStreamMap<StreamEntry> streams_;
  std::vector<uint32_t> pendingRemoval_;
};

} // namespace apache::thrift::fast_thrift::thrift
