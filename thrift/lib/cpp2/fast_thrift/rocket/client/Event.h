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

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>

#include <folly/ExceptionWrapper.h>
#include <folly/io/IOBuf.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Event.h>
#include <thrift/lib/cpp2/fast_thrift/transport/WriteCompletion.h>

namespace apache::thrift::fast_thrift::rocket {
struct RocketResponseMessage;
}

namespace apache::thrift::fast_thrift::rocket::client {

/**
 * Owned, opaque connection metadata. The event pointer is borrowed during
 * synchronous dispatch. A consumer may move the metadata out and returns its
 * acceptance status through result.
 */
struct RocketMetadataPush {
  std::unique_ptr<folly::IOBuf> metadata;
  channel_pipeline::Result result{channel_pipeline::Result::Success};
  folly::exception_wrapper error{};
};

struct MetadataPushEvent : channel_pipeline::EventTag<RocketMetadataPush*> {};

/**
 * `TransportWriteCompleteEvent` reports the outcome of one
 * socket-level writev.
 */
struct TransportWriteCompleteEvent
    : channel_pipeline::EventTag<TransportWriteCompleteEvent> {
  apache::thrift::fast_thrift::transport::WriteCompletionStatus status;
  size_t bytes;
};

/**
 * `BatchWriteCompleteEvent` reports the completion of one
 * rocket-frame batch. `frameCount` is the number of rocket frames in that batch
 * (> 0). Consumed by the WriteCompletionRouter, which pops `frameCount` entries
 * from its outbound FIFO and fans them out as per-write RocketWriteComplete
 * events.
 */
struct BatchWriteCompleteEvent
    : channel_pipeline::EventTag<BatchWriteCompleteEvent> {
  apache::thrift::fast_thrift::transport::WriteCompletionStatus status;
  size_t frameCount;
  size_t bytes;
  // Whether this completion left the connection's egress idle: nothing further
  // handed to the socket, and nothing buffered awaiting a flush. Computed by
  // the shared tracker for both directions; no client-side consumer reads it
  // today.
  bool quiesced;
};

/**
 * `FrameWriteCompleteEvent` reports the completion of one
 * original outbound frame (or one reassembled request when fragmentation is
 * active). Carries the streamId so StreamStateHandler can look up the request
 * context from its per-stream slot map.
 */
struct FrameWriteCompleteEvent
    : channel_pipeline::EventTag<FrameWriteCompleteEvent> {
  uint32_t streamId;
  apache::thrift::fast_thrift::transport::WriteCompletionStatus status;
  // See BatchWriteCompleteEvent::quiesced, narrowed by whatever the handler
  // that fired this was still holding back. Set on at most one frame per batch
  // — it describes the connection, not this frame.
  bool quiesced;
};

/**
 * `RocketWriteCompleteEvent` reports the completion of one
 * individual write. `requestContext` is a NON-OWNING borrow of that request's
 * context (the same TypeErasedPtr the rocket layer carries opaquely); it is
 * only valid for the duration of the typed event callback — subscribers must
 * not retain it. The owning layer (which knows the concrete context type)
 * static_casts it.
 */
struct RocketWriteCompleteEvent
    : channel_pipeline::EventTag<RocketWriteCompleteEvent> {
  void* requestContext;
  apache::thrift::fast_thrift::transport::WriteCompletionStatus status;
};

/** Requests cancellation of the stream carrying an opaque request context. */
struct RocketCancelRequestEvent
    : channel_pipeline::EventTag<RocketCancelRequestEvent> {
  void* requestContext;
};

/**
 * Requests local stream retirement and transfers its owned request context
 * into a terminal response. The response is borrowed only for synchronous
 * event delivery; subscribers must not retain it. No frame is sent to the peer.
 */
struct RocketTerminateStreamEvent
    : channel_pipeline::EventTag<RocketTerminateStreamEvent> {
  uint32_t streamId;
  RocketResponseMessage& response;
};

/**
 * `FirstResponseFrameEvent` reports when the first wire
 * frame of a fragmented response for `streamId` was parsed by the client.
 *
 * Only fired for responses the server fragmented. A response that arrives as a
 * single frame is forwarded whole by the defragmentation handler, so the
 * subscriber's own observation of that frame is already the first-frame time
 * and no event is needed.
 */
struct FirstResponseFrameEvent
    : channel_pipeline::EventTag<FirstResponseFrameEvent> {
  uint32_t streamId{0};
  std::chrono::steady_clock::time_point arrivalTime{};
};

/** Signals that the server initiated a graceful connection drain. */
struct ConnectionCloseEvent : channel_pipeline::EventTag<> {};

} // namespace apache::thrift::fast_thrift::rocket::client
