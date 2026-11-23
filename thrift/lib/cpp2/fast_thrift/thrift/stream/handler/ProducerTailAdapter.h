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

#include <glog/logging.h>
#include <folly/CPortability.h>
#include <folly/ExceptionWrapper.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/EndpointAdapter.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Handler.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>

namespace apache::thrift::fast_thrift::thrift::stream {

/**
 * ProducerTailAdapter — the fixed tail endpoint of an established stream's
 * producing-end sub-pipeline, sitting just past the application's producer.
 *
 * Owned by the owning thrift pipeline (which owns the sub-pipeline and the
 * paired head endpoint), it is the inbound demand backstop — not a bridge:
 * unlike the head, no traffic crosses back into the thrift pipeline here.
 *
 * As the tail endpoint it terminates the read (inbound demand) path: the
 * producer is the demand sink, so every `RequestN`/`Cancel` the consumer grants
 * is expected to be consumed by a handler before it reaches the tail. Any frame
 * that exits the pipeline here via `onRead` is unhandled demand — a
 * misconfigured pipeline, e.g. a missing producer — and is a protocol
 * violation: fatal in debug, `Result::Error` in release.
 *
 * The write (outbound) path is not its concern: producer output originates
 * upstream of the tail and flows toward the head, so a tail endpoint has no
 * `onWrite` callback. Exceptions that propagate toward the tail terminate here.
 *
 * Paired with [[ProducerHeadAdapter]], the fixed head endpoint.
 */
class ProducerTailAdapter {
 public:
  // EndpointHandlerLifecycle — a stateless backstop has nothing to set up or
  // tear down. Endpoint lifecycle callbacks take no context.
  void handlerAdded() noexcept {}
  void handlerRemoved() noexcept {}
  void onPipelineActive() noexcept {}
  void onPipelineInactive() noexcept {}
  void onWriteReady() noexcept {}

  // Inbound terminal backstop: demand should be consumed upstream by the
  // producer; anything reaching the tail is unhandled.
  channel_pipeline::Result onRead(
      auto& /*ctx*/, channel_pipeline::TypeErasedBox&& /*msg*/) noexcept {
    return onUnhandledDemand();
  }

  // Exceptions that propagate toward the tail terminate here; a producing-end
  // sub-pipeline has nothing downstream of the tail to forward them to.
  void onException(folly::exception_wrapper&& /*e*/) noexcept {}

 private:
  // An inbound frame reached the tail with no producer having consumed it. A
  // correctly composed producing pipeline keeps this unreachable, so it is a
  // bug: fatal in debug; in release the frame is dropped with Result::Error
  // (tearing the stream down) rather than silently ignored. Out-of-line to keep
  // the cold path off the hot path.
  FOLLY_NOINLINE static channel_pipeline::Result onUnhandledDemand() noexcept {
#ifndef NDEBUG
    CHECK(false)
        << "inbound stream frame reached the tail unhandled (no producer consumed the demand)";
#else
    return channel_pipeline::Result::Error;
#endif
  }
};

static_assert(
    channel_pipeline::TailEndpointHandler<ProducerTailAdapter>,
    "ProducerTailAdapter must satisfy TailEndpointHandler concept");

} // namespace apache::thrift::fast_thrift::thrift::stream
