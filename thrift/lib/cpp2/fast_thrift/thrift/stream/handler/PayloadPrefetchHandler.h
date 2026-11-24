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

#include <cstddef>
#include <cstdint>
#include <tuple>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <folly/CPortability.h>
#include <folly/Likely.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Handler.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/ContextImpl.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/stream/common/Messages.h>

namespace apache::thrift::fast_thrift::thrift::stream {

/**
 * Configuration for PayloadPrefetchHandler.
 */
struct PayloadPrefetchConfig {
  // The prefetch reserve size and the hard ceiling on outstanding producer
  // demand: the handler tops the reserve (held elements plus requests in
  // flight) up to `capacity` and never requests beyond it, so a full buffer's
  // worth is kept ready while the ring can never be asked to overflow.
  size_t capacity{256};
  // Only issue a top-up request once the prefetch reserve (held + requested but
  // not yet produced) has fallen to or below this. A smaller value batches
  // larger, less frequent refill requests; it must be < capacity.
  size_t replenishThreshold{128};
};

namespace detail {

/**
 * A fixed-capacity, single-threaded FIFO ring of stream messages. The backing
 * store is allocated once, lazily on first push, so an idle handler holds no
 * buffer memory. push() must not be called when full().
 *
 * A ring rather than std::deque: with bounded occupancy the ring allocates
 * exactly once and then reuses its slots via modular indexing forever, so a
 * steady stream never touches the allocator. A deque has no capacity reserve
 * and, under the rolling push_back/pop_front pattern here, keeps allocating and
 * freeing blocks as the window slides even though live occupancy stays bounded.
 */
class BoundedMessageRing {
 public:
  explicit BoundedMessageRing(size_t capacity) noexcept : capacity_(capacity) {}

  bool empty() const noexcept { return count_ == 0; }
  bool full() const noexcept { return count_ == capacity_; }
  size_t size() const noexcept { return count_; }

  void push(DataMessage msg) noexcept {
    DCHECK(!full()) << "push() on a full ring; caller must check full() first";
    if (slots_.empty()) {
      slots_.resize(capacity_);
    }
    slots_[(head_ + count_) % capacity_] = std::move(msg);
    ++count_;
  }

  DataMessage& front() noexcept { return slots_[head_]; }

  void pop() noexcept {
    slots_[head_] = DataMessage{}; // release the element's IOBuf
    head_ = (head_ + 1) % capacity_;
    --count_;
  }

 private:
  size_t capacity_;
  size_t head_{0};
  size_t count_{0};
  std::vector<DataMessage> slots_;
};

} // namespace detail

/**
 * PayloadPrefetchHandler — a demand-relaying prefetch buffer for the
 * established stream sub-pipeline. It meters demand and holds a prefetch
 * reserve; stream teardown belongs to other handlers. It decouples two
 * quantities:
 *
 *   - **send-credit**: how many *payloads* it may send toward the wire, granted
 *     by the consumer's `RequestN`, spent one per payload. Terminals are not
 *     metered (see below).
 *   - **prefetch reserve**: up to `capacity` payloads pulled ahead of demand,
 *     so there is always something ready to send.
 *
 * Requesting is bounded: every relayed `RequestN` is sized to *free buffer
 * space* (`capacity - held - in-flight`), never the consumer's demand, so the
 * reserve can never outrun what the ring holds — a well-behaved producer
 * honoring its grants can never overflow the buffer. Because a single relay is
 * capped at `capacity`, demand larger than the ring is served by re-requesting
 * as the reserve drains (see `replenishReserve`), across as many producer
 * round-trips as it takes.
 *
 * Read path (`onRead`): a `Cancel` is recorded (we send nothing more) and
 * forwarded to the producer to tear the stream down; once cancelled, every
 * later frame is forwarded untouched (we can act on nothing). Any other
 * non-`RequestN` frame is forwarded untouched. A `RequestN` adds send-credit
 * and drains the buffer into it (each drained element spends a credit) until
 * the buffer empties or the credit is exhausted, then tops the reserve back up.
 *
 * Write path (`onWrite`): a `Payload` is sent straight through when the
 * transport is open, a credit is available, and nothing is queued ahead of it;
 * otherwise it is held in the FIFO ring. Delivering a payload frees a unit of
 * reserve, so the write path also tops the reserve back up — this is what keeps
 * an *asynchronous* producer (one that delivers outside our request stack)
 * driven when demand exceeds a ring's worth.
 *
 * Terminals (`Complete`/`Error`): per Reactive Streams a terminal is ordered
 * after every `onNext` but is *not* governed by demand — it is delivered
 * regardless of outstanding `RequestN`. So it is held apart from the payload
 * ring (consuming neither a ring slot nor a send-credit) and flushed as soon as
 * the payloads ahead of it have drained and the transport is open. If the
 * transport is congested or payloads are still queued, it waits; it never jumps
 * ahead of an undelivered payload. A terminal ends production: no payload
 * follows it, so requesting stops once one arrives.
 *
 * Reentrancy: because requesting happens on both paths, relaying a `RequestN`
 * can synchronously drive a producer that delivers back into `onWrite`, which
 * requests again — a cycle. It is broken by the `replenishing_` guard in
 * `replenishReserve`: a reentrant call returns immediately and the outermost
 * refill loop re-reads the reserve after each relay, turning recursion into
 * iteration. Reserve accounting is updated *before* each relay fires, so a
 * reentrant `onWrite` always observes consistent state.
 *
 * Transport backpressure: this handler absorbs it into the ring. A downstream
 * `Result::Backpressure` (the sink took the element but wants no more) arms
 * write-ready and marks the transport congested; while congested, both paths
 * hold elements in the buffer instead of forwarding, and resume on
 * `onWriteReady` when the transport drains. Crucially the producer is *not*
 * handed the `Backpressure` — it is throttled through the demand channel:
 * because requests are bounded to free buffer space, a ring filling with
 * absorbed elements drives the relayed request to zero, so a producer honoring
 * its grant stops on its own. A producer that ignores its grant and delivers
 * into a full ring hits a `Result::Error` — genuine misbehavior, unreachable
 * under the demand bound.
 */
template <typename Context>
class PayloadPrefetchHandler {
 public:
  // `replenishThreshold_ < capacity_` (with a non-zero capacity) is validated
  // unconditionally, not with DCHECK: a violating config would otherwise fire a
  // near-SIZE_MAX request or never batch refills, only in opt builds.
  explicit PayloadPrefetchHandler(PayloadPrefetchConfig config = {})
      : capacity_(config.capacity),
        replenishThreshold_(config.replenishThreshold),
        buffer_(config.capacity) {
    CHECK_GT(capacity_, 0u);
    CHECK_LT(replenishThreshold_, capacity_);
  }

  // HandlerLifecycle
  void handlerAdded(Context& /*ctx*/) noexcept {}
  void handlerRemoved(Context& /*ctx*/) noexcept {}
  void onPipelineActive(Context& /*ctx*/) noexcept {}
  void onReadReady(Context& /*ctx*/) noexcept {}

  void onException(Context& ctx, folly::exception_wrapper&& e) noexcept {
    ctx.fireException(std::move(e));
  }

  // InboundHandler — consumer demand flows head->tail to here.
  channel_pipeline::Result onRead(
      Context& ctx, channel_pipeline::TypeErasedBox&& msg) noexcept {
    auto& message = msg.get<ControlMessage>();

    // Once cancelled we can act on nothing more: forward every frame untouched
    // toward the producer rather than swallow it.
    if (cancelled_) {
      return ctx.fireRead(std::move(msg));
    }
    // A Cancel means we can send nothing more: record it and forward it toward
    // the producer so the stream tears down.
    if (message.payload.is<Cancel>()) {
      cancelled_ = true;
      return ctx.fireRead(std::move(msg));
    }
    // Non-demand frames pass through untouched.
    if (!message.payload.is<RequestN>()) {
      return ctx.fireRead(std::move(msg));
    }

    // Add the granted credit and drain the prefetch reserve into it (each
    // element sent spends one credit) until the buffer empties or the credit is
    // exhausted. A `Backpressure` with elements still queued stops the drain
    // and hands the rest to `onWriteReady`, so we do not dump a backlog into a
    // congested transport; while awaiting we bank credit rather than drain.
    sendAllowance_ += message.payload.get<RequestN>().n;
    while (sendAllowance_ > 0 && !buffer_.empty() && !awaitingWriteReady_) {
      auto box = channel_pipeline::erase_and_box(std::move(buffer_.front()));
      buffer_.pop();
      --sendAllowance_;
      if (ctx.fireWrite(std::move(box)) ==
          channel_pipeline::Result::Backpressure) {
        armWriteReady(ctx);
        break;
      }
    }

    // Still draining a backlog on write-ready: leave the refill for later.
    if (awaitingWriteReady_) {
      return channel_pipeline::Result::Success;
    }

    // Top the reserve back up to capacity now that some of it has drained into
    // the new demand, then flush a pending terminal if the payloads ahead of it
    // are gone.
    replenishReserve(ctx);
    flushTerminalIfReady(ctx);
    return channel_pipeline::Result::Success;
  }

  // OutboundHandler — producer output flows tail->head through here.
  // Re-requests as the reserve drains (via replenishReserve), so an
  // asynchronous producer delivering outside our request stack keeps being
  // topped up.
  channel_pipeline::Result onWrite(
      Context& ctx, channel_pipeline::TypeErasedBox&& msg) noexcept {
    auto& message = msg.get<DataMessage>();

    // Only stream data and terminals are handled here; anything else flows
    // through.
    const bool isPayload = message.payload.is<Payload>();
    const bool isTerminal =
        message.payload.is<Complete>() || message.payload.is<Error>();
    if (!isPayload && !isTerminal) {
      return ctx.fireWrite(std::move(msg));
    }
    // Once cancelled, or after a terminal already arrived, we send nothing
    // more; drop late producer output (Reactive Streams emits no signal after a
    // terminal, and a cancelled stream is discarded).
    if (cancelled_ || producerDone_) {
      return channel_pipeline::Result::Success;
    }

    // A terminal (Complete/Error) is ordered after every buffered payload but
    // is NOT gated by send-credit — Reactive Streams delivers it regardless of
    // outstanding demand. It is held apart from the payload ring (so it
    // consumes neither a ring slot nor a credit) and flushed once the payloads
    // ahead of it drain and the transport is open. No payload follows a
    // terminal, so requesting stops here.
    if (isTerminal) {
      producerDone_ = true;
      if (buffer_.empty() && !awaitingWriteReady_) {
        std::ignore = ctx.fireWrite(std::move(msg));
      } else {
        terminal_ = msg.take<DataMessage>();
        hasTerminal_ = true;
      }
      return channel_pipeline::Result::Success;
    }

    // This payload fulfills one unit of the reserve we requested.
    if (outstandingProducerDemand_ > 0) {
      --outstandingProducerDemand_;
    }

    // Fast path: send straight through only when the transport is open, a
    // credit is available, and nothing is queued ahead. If the sink signals
    // congestion
    // (`Backpressure` means it took this element but wants no more), arm
    // write-ready so everything after it is absorbed into the ring rather than
    // dumped onto a congested transport. The producer is answered `Success`
    // regardless — it is throttled through demand (a filling ring stops
    // requests), not by surfacing the transport's flow-control result.
    if (!awaitingWriteReady_ && buffer_.empty() && sendAllowance_ > 0) {
      --sendAllowance_;
      if (ctx.fireWrite(std::move(msg)) ==
          channel_pipeline::Result::Backpressure) {
        armWriteReady(ctx);
      }
      replenishReserve(ctx);
      return channel_pipeline::Result::Success;
    }

    // Otherwise hold it: no credit, a queue ahead, or the transport is
    // congested. Absorbing congestion here is safe because demand is bounded to
    // free buffer space — a full ring can only mean the producer delivered past
    // its (now-zero) grant, so refuse rather than overflow.
    if (buffer_.full()) [[unlikely]] {
      return channel_pipeline::Result::Error;
    }
    buffer_.push(msg.take<DataMessage>());
    // Delivering this element freed a unit of the reserve; top it back up.
    replenishReserve(ctx);
    return channel_pipeline::Result::Success;
  }

  // The transport drained: flush the ring the write/read paths filled while
  // congested. If it backpressures again, stay registered so we keep absorbing;
  // otherwise deregister so onWriteReady stops firing until the next
  // congestion.
  void onWriteReady(Context& ctx) noexcept {
    while (sendAllowance_ > 0 && !buffer_.empty()) {
      auto box = channel_pipeline::erase_and_box(std::move(buffer_.front()));
      buffer_.pop();
      --sendAllowance_;
      if (ctx.fireWrite(std::move(box)) ==
          channel_pipeline::Result::Backpressure) {
        return;
      }
    }
    if (awaitingWriteReady_) {
      awaitingWriteReady_ = false;
      ctx.cancelAwaitWriteReady();
    }
    // The drain freed reserve and may have exposed a pending terminal.
    flushTerminalIfReady(ctx);
    replenishReserve(ctx);
  }

  // Intentionally a no-op: held elements are the stream's unsent output. Going
  // inactive is not a stream end, so they are kept (a reactivated pipeline can
  // still deliver them) and freed only when this per-stream handler is
  // destroyed.
  void onPipelineInactive(Context& /*ctx*/) noexcept {}

 private:
  // Register to resume the paused credit-drain when the transport next drains.
  // Guarded so a single pause episode arms the registration exactly once.
  void armWriteReady(Context& ctx) noexcept {
    if (!awaitingWriteReady_) {
      awaitingWriteReady_ = true;
      ctx.awaitWriteReady();
    }
  }

  // Refill the prefetch reserve (held elements plus requests in flight) back up
  // to `capacity`, issuing bounded requests until it is full. Only starts once
  // the reserve has fallen below `replenishThreshold_`, so a stocked reserve
  // does not churn tiny requests.
  //
  // Reentrancy: a relay can synchronously drive the producer back into
  // `onWrite` (a synchronous producer delivering within our own `RequestN`),
  // which calls here again. The `replenishing_` guard turns that reentry into
  // iteration of the loop below rather than recursion — the reentrant call
  // returns at once and the outer loop re-reads the reserve after the relay.
  // That is what lets both a synchronous producer (drains within the relay, so
  // the loop keeps pulling `capacity`-sized chunks until demand is met) and an
  // asynchronous one (delivers later, so `outstanding` reaches `capacity` and
  // the loop stops after one relay, to be re-driven from the write path) share
  // this path.
  void replenishReserve(Context& ctx) noexcept {
    // A finished producer (terminal received) will send no more payloads, so
    // there is nothing left to request.
    if (replenishing_ || producerDone_) {
      return;
    }
    size_t reserve = buffer_.size() + outstandingProducerDemand_;
    if (reserve >= replenishThreshold_) {
      return;
    }
    replenishing_ = true;
    do {
      relayDemand(ctx, capacity_ - reserve);
      reserve = buffer_.size() + outstandingProducerDemand_;
    } while (reserve < capacity_);
    replenishing_ = false;
  }

  // Deliver a stashed terminal once the payloads ahead of it have drained and
  // the transport is open. A terminal is not credit-gated, so this needs no
  // send-credit — only ordering (an empty payload ring) and transport
  // readiness.
  void flushTerminalIfReady(Context& ctx) noexcept {
    if (hasTerminal_ && buffer_.empty() && !awaitingWriteReady_) {
      hasTerminal_ = false;
      std::ignore =
          ctx.fireWrite(channel_pipeline::erase_and_box(std::move(terminal_)));
    }
  }

  // Relay a single request of `n` to the producer. The caller bounds `n` to
  // free buffer space, so the reserve never exceeds capacity. Accounting is
  // updated before the fire so a synchronous producer delivering reentrantly
  // within this call observes a consistent reserve.
  void relayDemand(Context& ctx, uint64_t n) noexcept {
    outstandingProducerDemand_ += n;
    // The producer's flow-control result on a relayed request is not actionable
    // here: requesting is driven by reserve level, not by the request's own
    // return.
    std::ignore = ctx.fireRead(
        channel_pipeline::erase_and_box(
            ControlMessage{.payload = RequestN{.n = n}}));
  }

  size_t capacity_;
  size_t replenishThreshold_;
  uint64_t sendAllowance_{0};
  uint64_t outstandingProducerDemand_{0};
  bool awaitingWriteReady_{false};
  bool cancelled_{false};
  bool replenishing_{false};
  bool producerDone_{false};
  bool hasTerminal_{false};
  detail::BoundedMessageRing buffer_;
  // A terminal held apart from the payload ring: it is ordered after the ring's
  // payloads but is not credit-gated, so it never occupies a ring slot.
  DataMessage terminal_{};
};

static_assert(
    channel_pipeline::DuplexHandler<
        PayloadPrefetchHandler<channel_pipeline::detail::ContextImpl>,
        channel_pipeline::detail::ContextImpl>,
    "PayloadPrefetchHandler must satisfy DuplexHandler concept");

} // namespace apache::thrift::fast_thrift::thrift::stream
