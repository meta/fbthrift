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
#include <cstring>
#include <memory>
#include <stdexcept>

#include <folly/ExceptionWrapper.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/AsyncTimeout.h>
#include <folly/lang/Hint.h>
#include <folly/logging/xlog.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Handler.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineImpl.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/ContextImpl.h>
#include <thrift/lib/cpp2/fast_thrift/frame/ErrorCode.h>
#include <thrift/lib/cpp2/fast_thrift/frame/FrameType.h>
#include <thrift/lib/cpp2/fast_thrift/frame/read/FrameViews.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/ComposedFrame.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/common/RSocketKeepAliveConfig.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/server/Messages.h>

namespace apache::thrift::fast_thrift::rocket::server::handler {

/**
 * RocketServerKeepAliveHandler - Pipeline handler for the RSocket
 * connection-level KEEPALIVE frame.
 *
 * Positioned on the transport side of RocketServerStreamStateHandler so its
 * echo, emitted via ctx.fireWrite, travels toward the transport without
 * re-entering StreamStateHandler::onWrite (which rejects streamId 0 as an
 * unknown stream).
 *
 * Pipeline position:
 *   Transport <-> ... <-> SetupFrameHandler <-> KeepAliveHandler <->
 *   StreamStateHandler <-> App
 *
 * Inbound:
 *   - KEEPALIVE with respond flag: echo a KEEPALIVE back with the respond
 *     flag cleared, mirroring the payload; lastReceivedPosition is 0 since
 *     stream resumption is unsupported. The frame is consumed.
 *   - KEEPALIVE without respond flag: the peer's reply to a keepalive this
 *     server never sends. Consumed silently.
 *   - KEEPALIVE with a non-zero stream ID: a protocol violation. Emit a
 *     connection-level ERROR(CONNECTION_ERROR) and close the connection.
 *   - Any other frame: forwarded downstream unchanged.
 *
 * Outbound: passthrough.
 */
class RocketServerKeepAliveHandler {
 public:
  explicit RocketServerKeepAliveHandler(
      bool enforcePeerLiveness = false,
      apache::thrift::fast_thrift::rocket::RSocketKeepAliveConfig config =
          {}) noexcept
      : enforcePeerLiveness_(enforcePeerLiveness), config_(config) {}

  // === HandlerLifecycle ===

  template <typename Context>
  void handlerAdded(Context& ctx) noexcept {
    if (!enforcePeerLiveness_) {
      return;
    }
    timer_ = folly::AsyncTimeout::make(
        *ctx.eventBase(), [this, &ctx]() noexcept { onTimeout(ctx); });
    arm();
  }

  template <typename Context>
  void handlerRemoved(Context& /*ctx*/) noexcept {
    active_ = false;
    timer_.reset();
  }

  // === InboundHandler ===

  template <typename Context>
  void onPipelineActive(Context& /*ctx*/) noexcept {}

  template <typename Context>
  void onReadReady(Context& /*ctx*/) noexcept {}

  template <typename Context>
  [[nodiscard]] apache::thrift::fast_thrift::channel_pipeline::Result onRead(
      Context& ctx,
      apache::thrift::fast_thrift::channel_pipeline::TypeErasedBox&&
          msg) noexcept {
    auto& request = msg.get<RocketRequestMessage>();
    auto& frame = request.frame;

    if (enforcePeerLiveness_ &&
        frame.type() == apache::thrift::fast_thrift::frame::FrameType::SETUP)
        [[unlikely]] {
      armFromSetup(frame);
      return ctx.fireRead(std::move(msg));
    }

    if (frame.type() !=
        apache::thrift::fast_thrift::frame::FrameType::KEEPALIVE) [[likely]] {
      return ctx.fireRead(std::move(msg));
    }

    // KEEPALIVE is a connection-level frame; a non-zero stream ID is a peer
    // protocol violation that terminates the connection (stock parity).
    if (!frame.isConnectionFrame()) [[unlikely]] {
      XLOG(ERR) << "Received KEEPALIVE with non-zero streamId="
                << frame.streamId() << "; closing connection";
      return sendConnectionError(ctx, "KEEPALIVE must use stream ID 0");
    }

    // Only reply when the peer sets the respond flag. A KEEPALIVE without it
    // is the peer's echo of a keepalive this server never initiates, so it is
    // consumed without a reply.
    apache::thrift::fast_thrift::frame::read::KeepAliveView view(frame);
    if (view.shouldRespond()) {
      if (active_) {
        lastProbe_ = Clock::now();
      }
      RocketResponseMessage echo{
          .frame =
              apache::thrift::fast_thrift::frame::ComposedFrame{
                  .frameType =
                      apache::thrift::fast_thrift::frame::FrameType::KEEPALIVE,
                  .streamId =
                      apache::thrift::fast_thrift::frame::kConnectionStreamId,
                  .data = std::move(frame).extractData(),
                  .respond = false,
                  .lastReceivedPosition = 0,
              },
      };
      if (ctx.fireWrite(
              apache::thrift::fast_thrift::channel_pipeline::erase_and_box(
                  std::move(echo))) ==
          apache::thrift::fast_thrift::channel_pipeline::Result::Error) {
        failRead(ctx, "Failed to write Rocket KEEPALIVE echo");
        return apache::thrift::fast_thrift::channel_pipeline::Result::Error;
      }
    }

    // Connection-level protocol frame fully handled here; never forwarded to
    // downstream stream handlers or the application.
    return apache::thrift::fast_thrift::channel_pipeline::Result::Success;
  }

  template <typename Context>
  void onException(Context& ctx, folly::exception_wrapper&& e) noexcept {
    ctx.fireException(std::move(e));
  }

  // === OutboundHandler ===

  template <typename Context>
  [[nodiscard]] apache::thrift::fast_thrift::channel_pipeline::Result onWrite(
      Context& ctx,
      apache::thrift::fast_thrift::channel_pipeline::TypeErasedBox&&
          msg) noexcept {
    return ctx.fireWrite(std::move(msg));
  }

  template <typename Context>
  void onPipelineInactive(Context& /*ctx*/) noexcept {
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

  void armFromSetup(
      const apache::thrift::fast_thrift::frame::read::ParsedFrame&
          frame) noexcept {
    const apache::thrift::fast_thrift::frame::read::SetupView view(frame);
    config_ = apache::thrift::fast_thrift::rocket::RSocketKeepAliveConfig{
        .intervalMs = view.keepaliveTime(),
        .maxLifetimeMs = view.maxLifetime(),
    };
    arm();
  }

  void arm() noexcept {
    if (!enforcePeerLiveness_ || !config_.enabled() || timer_ == nullptr) {
      return;
    }
    if (timer_->isScheduled()) {
      timer_->cancelTimeout();
    }
    active_ = true;
    lastProbe_ = Clock::now();
    timer_->scheduleTimeout(Milliseconds{config_.maxLifetimeMs});
  }

  template <typename Context>
  void onTimeout(Context& ctx) noexcept {
    if (!active_) {
      return;
    }

    const auto now = Clock::now();
    const auto deadline = lastProbe_ + Milliseconds{config_.maxLifetimeMs};
    if (now >= deadline) [[unlikely]] {
      failTimer(ctx, "Rocket keepalive probe timed out");
      return;
    }

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
    ctx.deactivate();
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

  template <typename Context>
  apache::thrift::fast_thrift::channel_pipeline::Result sendConnectionError(
      Context& ctx, const char* message) noexcept {
    try {
      auto errorData = ctx.copyBuffer(message, std::strlen(message));
      RocketResponseMessage response{
          .frame =
              apache::thrift::fast_thrift::frame::ComposedFrame{
                  .frameType =
                      apache::thrift::fast_thrift::frame::FrameType::ERROR,
                  .streamId =
                      apache::thrift::fast_thrift::frame::kConnectionStreamId,
                  .data = std::move(errorData),
                  .errorCode =
                      static_cast<uint32_t>(apache::thrift::fast_thrift::frame::
                                                ErrorCode::CONNECTION_ERROR),
              },
      };
      if (ctx.fireWrite(
              apache::thrift::fast_thrift::channel_pipeline::erase_and_box(
                  std::move(response))) ==
          apache::thrift::fast_thrift::channel_pipeline::Result::Error) {
        XLOG(WARN) << "Failed to deliver KEEPALIVE CONNECTION_ERROR frame";
      }
    } catch (...) {
      XLOG(ERR)
          << "Exception while delivering KEEPALIVE CONNECTION_ERROR frame";
    }

    ctx.close();

    return apache::thrift::fast_thrift::channel_pipeline::Result::Error;
  }

  bool enforcePeerLiveness_{false};
  apache::thrift::fast_thrift::rocket::RSocketKeepAliveConfig config_;
  std::unique_ptr<folly::AsyncTimeout> timer_;
  Clock::time_point lastProbe_{};
  bool active_{false};
};

static_assert(
    apache::thrift::fast_thrift::channel_pipeline::DuplexHandler<
        RocketServerKeepAliveHandler,
        // The concept check needs the pipeline's context type, which has no
        // public name.
        // NOLINTNEXTLINE(facebook-hte-DetailCall)
        apache::thrift::fast_thrift::channel_pipeline::detail::ContextImpl>,
    "RocketServerKeepAliveHandler must satisfy DuplexHandler concept");

} // namespace apache::thrift::fast_thrift::rocket::server::handler
