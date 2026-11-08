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
#include <chrono>
#include <cstdint>
#include <memory>
#include <stdexcept>

#include <glog/logging.h>
#include <folly/ExceptionWrapper.h>
#include <folly/io/async/AsyncTimeout.h>
#include <folly/lang/Hint.h>
#include <folly/logging/xlog.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Handler.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/ContextImpl.h>
#include <thrift/lib/cpp2/fast_thrift/frame/FrameType.h>
#include <thrift/lib/cpp2/fast_thrift/frame/read/FrameViews.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/ComposedFrame.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/client/Messages.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/common/RSocketKeepAliveConfig.h>

namespace apache::thrift::fast_thrift::rocket::client::handler {

namespace frame = apache::thrift::fast_thrift::frame;

class RocketClientKeepAliveHandler {
 public:
  enum class StartPolicy : uint8_t {
    PipelineActive,
    FirstInboundFrame,
  };

  explicit RocketClientKeepAliveHandler(
      apache::thrift::fast_thrift::rocket::RSocketKeepAliveConfig config,
      StartPolicy startPolicy = StartPolicy::PipelineActive)
      : config_(config), startPolicy_(startPolicy) {
    CHECK(config_.valid());
  }

  template <typename Context>
  void handlerAdded(Context& ctx) noexcept {
    timer_ = folly::AsyncTimeout::make(
        *ctx.eventBase(), [this, &ctx]() noexcept { onTimeout(ctx); });
  }

  template <typename Context>
  void handlerRemoved(Context& /*ctx*/) noexcept {
    pipelineActive_ = false;
    active_ = false;
    timer_.reset();
  }

  template <typename Context>
  void onPipelineActive(Context& ctx) noexcept {
    pipelineActive_ = true;
    if (startPolicy_ == StartPolicy::FirstInboundFrame) {
      return;
    }
    start(ctx);
  }

  template <typename Context>
  void start(Context& ctx) noexcept {
    if (!pipelineActive_ || !config_.enabled() || active_) {
      return;
    }
    active_ = true;
    const auto now = Clock::now();
    lastReply_ = now;
    nextProbe_ = now + interval();
    scheduleNext(ctx, now);
  }

  template <typename Context>
  void onReadReady(Context& /*ctx*/) noexcept {}

  template <typename Context>
  [[nodiscard]] channel_pipeline::Result onRead(
      Context& ctx, channel_pipeline::TypeErasedBox&& msg) noexcept {
    if (startPolicy_ == StartPolicy::FirstInboundFrame && !active_) {
      start(ctx);
    }
    auto& response = msg.get<RocketResponseMessage>();
    if (!response.payload.template is<frame::read::ParsedFrame>()) [[likely]] {
      return ctx.fireRead(std::move(msg));
    }

    auto& parsed = response.payload.template get<frame::read::ParsedFrame>();
    if (parsed.type() != frame::FrameType::KEEPALIVE) [[likely]] {
      return ctx.fireRead(std::move(msg));
    }
    if (!parsed.isConnectionFrame()) [[unlikely]] {
      failRead(ctx, "KEEPALIVE must use stream ID 0");
      return channel_pipeline::Result::Error;
    }

    frame::read::KeepAliveView view(parsed);
    if (view.shouldRespond()) [[unlikely]] {
      XLOG(WARN) << "Ignoring respond-flagged KEEPALIVE from server";
      return channel_pipeline::Result::Success;
    }

    if (active_) {
      lastReply_ = Clock::now();
    }
    return channel_pipeline::Result::Success;
  }

  template <typename Context>
  void onException(Context& ctx, folly::exception_wrapper&& error) noexcept {
    ctx.fireException(std::move(error));
  }

  template <typename Context>
  [[nodiscard]] channel_pipeline::Result onWrite(
      Context& ctx, channel_pipeline::TypeErasedBox&& msg) noexcept {
    return ctx.fireWrite(std::move(msg));
  }

  template <typename Context>
  void onPipelineInactive(Context& /*ctx*/) noexcept {
    pipelineActive_ = false;
    active_ = false;
    if (timer_) {
      timer_->cancelTimeout();
    }
  }

  template <typename Context>
  void onWriteReady(Context& /*ctx*/) noexcept {}

 private:
  using Clock = std::chrono::steady_clock;
  using Milliseconds = std::chrono::milliseconds;

  Milliseconds interval() const noexcept {
    return Milliseconds{config_.intervalMs};
  }

  Milliseconds maxLifetime() const noexcept {
    return Milliseconds{config_.maxLifetimeMs};
  }

  template <typename Context>
  void onTimeout(Context& ctx) noexcept {
    if (!active_) {
      return;
    }

    const auto now = Clock::now();
    if (now >= lastReply_ + maxLifetime()) [[unlikely]] {
      failTimer(ctx, "Rocket keepalive reply timed out");
      return;
    }

    if (now >= nextProbe_) {
      RocketRequestMessage probe{
          .frame =
              frame::ComposedFrame{
                  .frameType = frame::FrameType::KEEPALIVE,
                  .streamId = frame::kConnectionStreamId,
                  .respond = true,
                  .lastReceivedPosition = 0,
              },
          .requestContext = {},
      };
      const auto result =
          ctx.fireWrite(channel_pipeline::erase_and_box(std::move(probe)));
      if (result == channel_pipeline::Result::Error) [[unlikely]] {
        failTimer(ctx, "Rocket keepalive write failed");
        return;
      }
      nextProbe_ = now + interval();
    }

    if (active_) {
      scheduleNext(ctx, now);
    }
  }

  template <typename Context>
  void scheduleNext(Context& /*ctx*/, Clock::time_point now) noexcept {
    const auto deadline = std::min(nextProbe_, lastReply_ + maxLifetime());
    auto delay = std::chrono::ceil<Milliseconds>(deadline - now);
    if (delay <= Milliseconds::zero()) {
      delay = Milliseconds{1};
    }
    timer_->scheduleTimeout(delay);
  }

  template <typename Context>
  void failTimer(Context& ctx, const char* reason) noexcept {
    active_ = false;
    XLOG(ERR) << reason;
    ctx.fireException(
        folly::make_exception_wrapper<std::runtime_error>(reason));
    ctx.pipeline()->deactivate();
  }

  template <typename Context>
  void failRead(Context& ctx, const char* reason) noexcept {
    active_ = false;
    if (timer_) {
      timer_->cancelTimeout();
    }
    XLOG(ERR) << reason;
    ctx.fireException(
        folly::make_exception_wrapper<std::runtime_error>(reason));
    ctx.close();
  }

  apache::thrift::fast_thrift::rocket::RSocketKeepAliveConfig config_;
  std::unique_ptr<folly::AsyncTimeout> timer_;
  Clock::time_point lastReply_{};
  Clock::time_point nextProbe_{};
  StartPolicy startPolicy_{StartPolicy::PipelineActive};
  bool pipelineActive_{false};
  bool active_{false};
};

static_assert(
    channel_pipeline::DuplexHandler<
        RocketClientKeepAliveHandler,
        channel_pipeline::detail::ContextImpl>,
    "RocketClientKeepAliveHandler must satisfy DuplexHandler concept");

} // namespace apache::thrift::fast_thrift::rocket::client::handler
