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

#include <algorithm>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <stdexcept>
#include <type_traits>
#include <utility>

#include <folly/ExceptionWrapper.h>
#include <folly/logging/xlog.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Event.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineImpl.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineRef.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/util/ConcurrencyControlState.h>

namespace apache::thrift::fast_thrift::thrift {

template <typename Context, channel_pipeline::PipelineEvent CompletionEvent>
class ConcurrencyControlHandler final {
 public:
  using SubscribedEvents = channel_pipeline::Events<CompletionEvent>;

  ConcurrencyControlHandler(
      uint64_t maxActive,
      uint64_t maxOutstanding,
      ConcurrencyControlStateHolder& holder) noexcept
      : maxActive_(maxActive),
        maxOutstanding_(maxOutstanding),
        holder_(holder) {}

  void handlerAdded(Context& ctx) noexcept {
    state_ = &holder_.stateFor(*ctx.eventBase());
    state_->maxActiveSeen = std::max(state_->maxActiveSeen, maxActive_);
    state_->maxOutstandingSeen =
        std::max(state_->maxOutstandingSeen, maxOutstanding_);
    subscriber_ =
        state_->idle.insert(state_->idle.end(), [this, &ctx]() noexcept {
          auto guard = channel_pipeline::PipelineRef(*ctx.pipeline()).guard();
          onCapacityAvailable(ctx);
        });
  }

  void handlerRemoved(Context& ctx) noexcept {
    releaseCapacity(ctx);
    state_->idle.erase(subscriber_);
    state_ = nullptr;
  }

  void onPipelineActive(Context&) noexcept { failed_ = false; }

  void onPipelineInactive(Context& ctx) noexcept { releaseCapacity(ctx); }

  channel_pipeline::Result onRead(
      Context& ctx, channel_pipeline::TypeErasedBox&& msg) noexcept {
    if (failed_) {
      return channel_pipeline::Result::Error;
    }
    if (outstanding() >= maxOutstanding_) {
      fail(ctx);
      ctx.fireException(
          folly::make_exception_wrapper<std::runtime_error>(
              "channel pipeline concurrency limit exceeded"));
      return channel_pipeline::Result::Error;
    }

    ++state_->admitted;
    ++localAdmitted_;
    if (pending_.empty() && !ctx.isAwaitingReadReady() && canDispatch()) {
      const auto result = dispatch(ctx, std::move(msg));
      if (result == channel_pipeline::Result::Error) {
        return result;
      }
      updateWaiter(ctx);
      return result == channel_pipeline::Result::Backpressure ||
              outstanding() >= maxOutstanding_
          ? channel_pipeline::Result::Backpressure
          : result;
    }

    pending_.push_back(std::move(msg));
    updateWaiter(ctx);
    return ctx.isAwaitingReadReady() || outstanding() >= maxOutstanding_
        ? channel_pipeline::Result::Backpressure
        : channel_pipeline::Result::Success;
  }

  channel_pipeline::Result onWrite(
      Context& ctx, channel_pipeline::TypeErasedBox&& msg) noexcept {
    complete(ctx);
    return ctx.fireWrite(std::move(msg));
  }

  void onReadReady(Context& ctx) noexcept {
    ctx.cancelAwaitReadReady();
    drain(ctx);
  }

  void onWriteReady(Context&) noexcept {}

  void onException(Context& ctx, folly::exception_wrapper&& e) noexcept {
    fail(ctx);
    ctx.fireException(std::move(e));
  }

  template <channel_pipeline::PipelineEvent E>
    requires(
        std::same_as<E, CompletionEvent> && std::is_void_v<typename E::Payload>)
  void on(Context& ctx) noexcept {
    if (state_ == nullptr || ctx.pipeline()->isClosed()) {
      return;
    }
    complete(ctx);
  }

  template <channel_pipeline::PipelineEvent E>
    requires(
        std::same_as<E, CompletionEvent> &&
        !std::is_void_v<typename E::Payload>)
  void on(Context& ctx, const typename E::Payload&) noexcept {
    if (state_ != nullptr && !ctx.pipeline()->isClosed()) {
      complete(ctx);
    }
  }

  uint64_t admitted() const noexcept { return state_->admitted; }
  uint64_t released() const noexcept { return state_->released; }
  uint64_t outstanding() const noexcept { return state_->outstanding(); }
  uint64_t active() const noexcept { return state_->active(); }
  size_t pending() const noexcept { return pending_.size(); }

 private:
  void onCapacityAvailable(Context& ctx) noexcept {
    removeWaiter();
    if (failed_ || ctx.pipeline()->isClosed() || ctx.isAwaitingReadReady()) {
      return;
    }
    if (drain(ctx) && !ctx.isAwaitingReadReady() &&
        outstanding() < maxOutstanding_) {
      ctx.pipeline()->onReadReady();
    }
  }

  bool canDispatch() const noexcept { return active() < maxActive_; }
  uint64_t localOutstanding() const noexcept {
    return localAdmitted_ - localReleased_;
  }

  channel_pipeline::Result dispatch(
      Context& ctx, channel_pipeline::TypeErasedBox&& msg) noexcept {
    ++state_->dispatched;
    const auto result = ctx.fireRead(std::move(msg));
    if (ctx.pipeline()->isClosed()) {
      return channel_pipeline::Result::Error;
    }
    if (result == channel_pipeline::Result::Error) {
      // Error is terminal for the connection; retire all of its work even
      // when the failed dispatch produces no outbound response.
      fail(ctx);
    } else if (result == channel_pipeline::Result::Backpressure) {
      ctx.awaitReadReady();
    }
    return result;
  }

  bool drain(Context& ctx) noexcept {
    if (failed_) {
      return false;
    }
    while (!pending_.empty() && canDispatch()) {
      auto msg = std::move(pending_.front());
      pending_.pop_front();
      const auto result = dispatch(ctx, std::move(msg));
      if (result == channel_pipeline::Result::Error) {
        return false;
      }
      if (result == channel_pipeline::Result::Backpressure) {
        break;
      }
    }
    updateWaiter(ctx);
    return true;
  }

  void complete(Context& ctx) noexcept {
    // A write or completion event can retire only this pipeline's dispatched
    // work, never queued requests or another connection's contribution.
    if (localOutstanding() <= pending_.size()) {
      return;
    }
    ++localReleased_;
    ++state_->released;
    ++state_->finished;
    if (!pending_.empty() && !ctx.isAwaitingReadReady()) {
      addWaiter();
    }
    state_->notify();
  }

  void fail(Context& ctx) noexcept {
    failed_ = true;
    releaseCapacity(ctx);
  }

  void releaseCapacity(Context& ctx) noexcept {
    ctx.cancelAwaitReadReady();
    const auto outstanding = localOutstanding();
    state_->released += outstanding;
    DCHECK_GE(outstanding, pending_.size());
    state_->finished += outstanding - pending_.size();
    localReleased_ += outstanding;
    pending_.clear();
    removeWaiter();
    state_->notify();
  }

  void updateWaiter(Context& ctx) noexcept {
    if (!failed_ && !ctx.isAwaitingReadReady() &&
        (!pending_.empty() || outstanding() >= maxOutstanding_)) {
      addWaiter();
    } else {
      removeWaiter();
    }
  }

  void addWaiter() noexcept {
    if (!waiting_) {
      state_->waiters.splice(state_->waiters.end(), state_->idle, subscriber_);
      waiting_ = true;
    }
  }

  void removeWaiter() noexcept {
    if (waiting_) {
      state_->idle.splice(state_->idle.end(), state_->waiters, subscriber_);
      waiting_ = false;
    }
  }

  const uint64_t maxActive_;
  const uint64_t maxOutstanding_;
  ConcurrencyControlStateHolder& holder_;
  ConcurrencyControlEventBaseState* state_{nullptr};
  ConcurrencyControlEventBaseState::Subscribers::iterator subscriber_;
  // These monotonic counts isolate this connection's teardown contribution;
  // its active contribution is outstanding minus the local queue size.
  uint64_t localAdmitted_{0};
  uint64_t localReleased_{0};
  std::deque<channel_pipeline::TypeErasedBox> pending_;
  bool waiting_{false};
  bool failed_{false};
};

} // namespace apache::thrift::fast_thrift::thrift
