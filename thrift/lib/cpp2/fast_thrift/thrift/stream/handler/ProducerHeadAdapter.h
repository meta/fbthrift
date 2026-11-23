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

#include <utility>

#include <glog/logging.h>
#include <folly/CPortability.h>
#include <folly/ExceptionWrapper.h>
#include <folly/Function.h>
#include <folly/Likely.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/EndpointAdapter.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Handler.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/stream/common/Messages.h>

namespace apache::thrift::fast_thrift::thrift::stream {

/**
 * StreamSink — the head endpoint's output destination: where producer output
 * leaves an established stream's producing-end sub-pipeline and crosses back
 * into the owning thrift pipeline. Passed to the head at construction so the
 * endpoint is always fully wired.
 *
 * A `noexcept`-qualified `folly::Function` so the owning handler can capture
 * whatever it needs to route the message (e.g. its instance plus the streamId)
 * by value. Small captures stay in-situ (no heap allocation); the `noexcept`
 * signature keeps the head's `onWrite` non-throwing at the type level. The head
 * adapter hands ownership of each terminal message to the sink; the sink
 * returns the downstream Result.
 */
using StreamSink =
    folly::Function<channel_pipeline::Result(DataMessage&&) noexcept>;

/**
 * ProducerHeadAdapter — the fixed head endpoint of an established stream's
 * producing-end sub-pipeline (the pipeline an application composes for its
 * stream handler), and the bridge between that sub-pipeline and the owning
 * thrift pipeline.
 *
 * Both directions cross the seam at the head: the owning thrift pipeline
 * injects the consumer's `RequestN`/`Cancel` demand here
 * (`ProducerPipeline::fireRead`), and producer output leaves here through the
 * [[StreamSink]] back into that pipeline. The adapter is owned by the owning
 * thrift pipeline, which also owns the sub-pipeline and the paired tail
 * endpoint.
 *
 * As the head endpoint it terminates the write (outbound) path: producer output
 * flows tail->head and exits here via `onWrite`. Rather than forwarding onward
 * (there is nothing past the head), the adapter guards the stream's outbound
 * contract — only `Payload`, `Complete`, and `Error` — and hands the message to
 * the sink, which delivers it to the owning thrift pipeline.
 *
 * The read (inbound) path — the consumer's `RequestN`/`Cancel` demand — is
 * *initiated* at the head by the owning pipeline (`ProducerPipeline::fireRead`)
 * and travels head->tail toward the producer; a head endpoint has no `onRead`
 * callback, so the adapter does not see it.
 *
 * Lifecycle: constructed with its output [[StreamSink]] and owned by the owning
 * thrift pipeline. Requiring the sink at construction keeps the head always
 * fully wired: there is no unwired window, so producer output can never reach a
 * head with no sink.
 *
 * Paired with [[ProducerTailAdapter]], the fixed tail endpoint.
 */
class ProducerHeadAdapter {
 public:
  explicit ProducerHeadAdapter(StreamSink sink) noexcept
      : sink_(std::move(sink)) {}

  // EndpointHandlerLifecycle — a stateless boundary has nothing to set up or
  // tear down. Endpoint lifecycle callbacks take no context.
  void handlerAdded() noexcept {}
  void handlerRemoved() noexcept {}
  void onPipelineActive() noexcept {}
  void onPipelineInactive() noexcept {}
  void onReadReady() noexcept {}

  // Outbound terminal: producer output exits the sub-pipeline here. The write
  // path carries only the Data set (Payload/Complete/Error), so the outbound
  // contract is enforced by the message type itself — no runtime guard needed.
  channel_pipeline::Result onWrite(
      auto& /*ctx*/, channel_pipeline::TypeErasedBox&& msg) noexcept {
    return sink_(msg.take<DataMessage>());
  }

 private:
  StreamSink sink_;
};

static_assert(
    channel_pipeline::HeadEndpointHandler<ProducerHeadAdapter>,
    "ProducerHeadAdapter must satisfy HeadEndpointHandler concept");

} // namespace apache::thrift::fast_thrift::thrift::stream
