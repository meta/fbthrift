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
#include <limits>
#include <utility>

#include <glog/logging.h>
#include <folly/CPortability.h>
#include <folly/ExceptionWrapper.h>
#include <folly/Likely.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Handler.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/ContextImpl.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/stream/common/Messages.h>

namespace apache::thrift::fast_thrift::thrift::stream {

/**
 * InboundCreditHandler — enforces the RSocket credit contract on an established
 * stream / sink / bidi exchange. The peer grants credit via REQUEST_N, where
 * one credit authorizes exactly one stream element (a `Payload`). This handler
 * owns the credit budget *privately* (the count never leaves it) and plays two
 * roles:
 *
 *   - Inbound `RequestN` adds to the credit budget and is then **forwarded** on
 *     as demand — the source produces in response to it. The count stays
 *     private; only the demand travels onward. Other inbound frames (e.g.
 *     `Cancel`) pass through untouched.
 *   - Outbound `Payload` spends one credit and is forwarded; the downstream
 *     result (including transport `Backpressure`) propagates unchanged. Writing
 *     a `Payload` while the budget is exhausted overruns the peer's demand — a
 *     credit-contract violation an upstream buffer/producer keeps unreachable
 * by honoring the demand it was granted, so it fatals in debug and is dropped
 *     with `Result::Error` in release. Non-`Payload` outbound frames
 * (`Complete`, `Error`) pass through ungated.
 *
 * Side-agnostic: a server stream producer and a client sink producer both spend
 * the credit their peer grants. Scope is credit accounting + contract
 * enforcement only; buffering and demand metering are separate concerns.
 */
template <typename Context>
class InboundCreditHandler {
 public:
  // HandlerLifecycle
  void handlerAdded(Context& /*ctx*/) noexcept {}
  void handlerRemoved(Context& /*ctx*/) noexcept {}
  void onPipelineActive(Context& /*ctx*/) noexcept {}
  void onReadReady(Context& /*ctx*/) noexcept {}

  // InboundHandler
  channel_pipeline::Result onRead(
      Context& ctx, channel_pipeline::TypeErasedBox&& msg) noexcept {
    auto& message = msg.get<ControlMessage>();
    if (message.payload.is<RequestN>()) {
      // Account the grant, then forward on only the demand we actually admitted
      // so the forwarded demand and the private budget stay in lockstep. On the
      // (practically unreachable) overflow the budget saturates, and we forward
      // just the remaining headroom rather than the raw ask — otherwise the
      // source would be authorized to produce elements the budget will not
      // admit, and they would trip the exhaustion-violation path on the way
      // out.
      auto& requestN = message.payload.get<RequestN>();
      const uint64_t before = credits_;
      credits_ = addSaturating(credits_, requestN.n);
      requestN.n = credits_ - before;
    }
    return ctx.fireRead(std::move(msg));
  }

  void onException(Context& ctx, folly::exception_wrapper&& e) noexcept {
    ctx.fireException(std::move(e));
  }

  // OutboundHandler
  channel_pipeline::Result onWrite(
      Context& ctx, channel_pipeline::TypeErasedBox&& msg) noexcept {
    auto& message = msg.get<DataMessage>();
    if (!message.payload.is<Payload>()) {
      return ctx.fireWrite(std::move(msg));
    }
    if (credits_ == 0) [[unlikely]] {
      return onCreditExhaustedViolation();
    }
    --credits_;
    // Forward and let the downstream result (including transport Backpressure)
    // propagate unchanged. Credit exhaustion is not signalled here: the source
    // already limits itself to the demand it was granted. The credit is spent
    // once here and not restored on a non-Success result: fireWrite is a sink,
    // so Backpressure means the element was consumed (only "slow down"), and
    // the element is moved away — there is no re-send at this layer to
    // double-charge.
    return ctx.fireWrite(std::move(msg));
  }

  void onWriteReady(Context& /*ctx*/) noexcept {}

  // Intentionally a no-op: granted credit is the peer's outstanding demand and
  // persists across a transport pause — it is not reset here. The credit dies
  // with this per-stream handler when the stream ends.
  void onPipelineInactive(Context& /*ctx*/) noexcept {}

 private:
  // A Payload reached this consuming sink while demand is exhausted: the
  // producer overran the credit contract. An upstream buffer/producer honoring
  // the demand it was granted keeps this unreachable, so reaching here is a bug
  // rather than a runtime condition — fail loudly in debug. In release the hard
  // contract still holds: the element is dropped with Result::Error (tearing
  // down the peer) rather than delivered beyond demand. Kept out-of-line so the
  // cold violation path adds nothing to the hot path.
  FOLLY_NOINLINE static channel_pipeline::Result
  onCreditExhaustedViolation() noexcept {
    DCHECK(false) << "Payload written while stream credit is exhausted "
                     "(credit-contract violation)";
    return channel_pipeline::Result::Error;
  }

  // A credit grant that would overflow the budget saturates to the maximum
  // rather than wrapping; a silent wrap would collapse outstanding demand back
  // toward zero and start dropping authorized elements. Overflow needs the peer
  // to grant ~2^64 credits, so it is practically unreachable — hence the
  // unlikely hint — but detecting it keeps the budget monotonic.
  static constexpr uint64_t addSaturating(
      uint64_t credits, uint64_t delta) noexcept {
    const uint64_t sum = credits + delta;
    if (sum < credits) [[unlikely]] {
      return std::numeric_limits<uint64_t>::max();
    }
    return sum;
  }

  uint64_t credits_{0};
};

static_assert(
    channel_pipeline::InboundHandler<
        InboundCreditHandler<channel_pipeline::detail::ContextImpl>,
        channel_pipeline::detail::ContextImpl>,
    "InboundCreditHandler must satisfy InboundHandler concept");

static_assert(
    channel_pipeline::OutboundHandler<
        InboundCreditHandler<channel_pipeline::detail::ContextImpl>,
        channel_pipeline::detail::ContextImpl>,
    "InboundCreditHandler must satisfy OutboundHandler concept");

} // namespace apache::thrift::fast_thrift::thrift::stream
