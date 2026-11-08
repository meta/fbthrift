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

#include <folly/Portability.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Backpressure.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/ReadBackpressureArbiter.h>

namespace apache::thrift::fast_thrift::thrift {

/**
 * Pauses inbound admission while outbound writes report backpressure.
 * Responses for accepted requests always continue toward the transport.
 * Accepted writes return Success to response producers; pressure is surfaced
 * on reads instead.
 */
template <typename Context>
class ThriftServerBackpressureHandler {
 public:
  ThriftServerBackpressureHandler() = default;
  explicit ThriftServerBackpressureHandler(
      ReadBackpressureArbiter* arbiter) noexcept
      : arbiter_(arbiter) {}

  // Detected by makeHandlerNode — registers this handler in the
  // pipeline's writeReadyList so onWriteReady() actually fires.
  channel_pipeline::WriteReadyHook writeReadyHook_;

  void handlerAdded(Context& /*ctx*/) noexcept {}
  void handlerRemoved(Context& /*ctx*/) noexcept {}
  void onPipelineActive(Context& /*ctx*/) noexcept {}
  void onReadReady(Context& /*ctx*/) noexcept {}

  // Inbound: forward, but signal Backpressure upstream as long as
  // outbound is saturated. The transport pauses reads to avoid
  // accumulating in-flight requests whose responses can't be drained.
  // Error from downstream is terminal and not overridden.
  channel_pipeline::Result onRead(
      Context& ctx, channel_pipeline::TypeErasedBox&& msg) noexcept {
    const auto result = ctx.fireRead(std::move(msg));
    if (backpressured_ && result != channel_pipeline::Result::Error)
        [[unlikely]] {
      return channel_pipeline::Result::Backpressure;
    }
    return result;
  }

  void onException(Context& ctx, folly::exception_wrapper&& e) noexcept {
    ctx.fireException(std::move(e));
  }

  channel_pipeline::Result onWrite(
      Context& ctx, channel_pipeline::TypeErasedBox&& msg) noexcept {
    const auto result = ctx.fireWrite(std::move(msg));
    if (result == channel_pipeline::Result::Backpressure) [[unlikely]] {
      if (!backpressured_) {
        backpressured_ = true;
        arbiter(ctx).acquire();
        ctx.awaitWriteReady();
      }
      return channel_pipeline::Result::Success;
    }
    return result;
  }

  void onWriteReady(Context& ctx) noexcept {
    if (!backpressured_) {
      return;
    }
    backpressured_ = false;
    ctx.cancelAwaitWriteReady();
    if (DCHECK_NOTNULL(arbiter_)->release()) {
      ctx.pipeline()->onReadReady();
    }
  }

  void onPipelineInactive(Context& ctx) noexcept {
    if (backpressured_) {
      backpressured_ = false;
      (void)DCHECK_NOTNULL(arbiter_)->release();
    }
    ctx.cancelAwaitWriteReady();
  }

  bool isBackpressured() const noexcept { return backpressured_; }

 private:
  ReadBackpressureArbiter& arbiter(Context& ctx) noexcept {
    if (arbiter_ == nullptr) {
      if constexpr (requires {
                      ctx.template state<ReadBackpressureArbiter>();
                    }) {
        arbiter_ = &ctx.template state<ReadBackpressureArbiter>();
      }
    }
    return *DCHECK_NOTNULL(arbiter_);
  }

  bool backpressured_{false};
  ReadBackpressureArbiter* arbiter_{nullptr};
};

} // namespace apache::thrift::fast_thrift::thrift
