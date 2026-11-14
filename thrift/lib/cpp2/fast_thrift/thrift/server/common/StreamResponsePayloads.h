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

#include <glog/logging.h>

#include <thrift/lib/cpp2/fast_thrift/frame/ErrorCode.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/ComposedFrame.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/server/MetadataProtocol.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/common/ThriftResponsePayloads.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/stream/ProducerPipeline.h>

namespace apache::thrift::fast_thrift::thrift {

/**
 * ThriftServerStreamOpenPayload — the app→mux message that opens a producing
 * stream. It is the "dedicated stream-response variant": distinct from the wire
 * payloads because it additionally carries a [[ProducerPipeline::ConfigFunc]]
 * the stream mux runs to build the stream's producing-end sub-pipeline.
 *
 * The mux consumes it: it constructs a [[ProducerPipeline::Builder]], runs the
 * configFunc to compose the application's producer into it, builds and
 * registers the sub-pipeline (keyed by `initialResponse.streamId`), then
 * forwards `initialResponse` upward as the stream's first chunk. The configFunc
 * never travels past the mux.
 *
 * The configFunc is held behind a `unique_ptr` so this payload stays
 * pointer-sized: it rides `ThriftServerResponseMessage` through
 * `TypeErasedBox`'s inline storage, which a by-value `folly::Function` would
 * overflow.
 *
 * This is the server-stream instance of a producing-open message; the same
 * pattern generalizes side-agnostically once the client-sink / bidi producing
 * paths come online (both also compose a `ProducerPipeline` via a configFunc).
 *
 * It satisfies ThriftPayloadConcept so it can ride the outbound variant, but
 * the mux always consumes it before it can reach the transport, so
 * `toRocketFrame()` is a never-happens guard rather than a real serialization
 * path — see below.
 */
struct ThriftServerStreamOpenPayload {
  using RocketFrame = apache::thrift::fast_thrift::frame::ComposedFrame;

  ThriftStreamInitialResponsePayload initialResponse;
  std::unique_ptr<thrift::stream::ProducerPipeline::ConfigFunc> configFunc;

  // Invariant guard: the mux consumes this payload in onWrite, so reaching the
  // transport means a routing/placement bug. Fatal in debug so it surfaces in
  // tests/canary; in release emit a terminal ERROR on the stream so the client
  // fails fast rather than hanging on a stream whose producing pipeline was
  // never registered (which forwarding the initial chunk would cause).
  RocketFrame toRocketFrame(
      rocket::server::MetadataProtocol /*metadataProtocol*/) && noexcept {
    DCHECK(false)
        << "ThriftServerStreamOpenPayload reached the transport un-consumed";
#ifdef NDEBUG
    return {
        .frameType = apache::thrift::fast_thrift::frame::FrameType::ERROR,
        .streamId = initialResponse.streamId,
        .errorCode = static_cast<uint32_t>(
            apache::thrift::fast_thrift::frame::ErrorCode::INVALID),
    };
#endif
  }
};

} // namespace apache::thrift::fast_thrift::thrift
