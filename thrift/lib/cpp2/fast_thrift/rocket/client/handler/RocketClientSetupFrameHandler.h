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

#include <functional>
#include <optional>

#include <glog/logging.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/EventBase.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Handler.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/ContextImpl.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/ComposedFrame.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/client/Messages.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/common/RSocketKeepAliveConfig.h>

namespace apache::thrift::fast_thrift::rocket::client::handler {

/**
 * RocketClientSetupFrameHandler - Pipeline handler for client-side SETUP frame
 * handling.
 *
 * This handler sends the RSocket SETUP frame when the pipeline becomes active.
 *
 * RSocket requires SETUP as the first frame of a connection. So until SETUP is
 * sent, onWrite returns Error for every write: before the first activation,
 * after a failed SETUP write, and after the pipeline becomes inactive.
 *
 * If the SETUP write returns Error, the handler closes the pipeline from an
 * EventBase loop callback, after the activation returns.
 *
 * The handler supports a factory function that is called at connect time
 * for dynamic metadata creation.
 *
 * Pipeline position: duplex, below RocketClientStreamStateHandler, whose
 * onWrite rejects SETUP:
 *   App <-> RocketClientStreamStateHandler <-> RocketClientSetupFrameHandler
 *       <-> frame codec <-> Transport
 */
class RocketClientSetupFrameHandler {
 public:
  // RSocket protocol version
  static constexpr uint16_t kRSocketMajorVersion = 1;
  static constexpr uint16_t kRSocketMinorVersion = 0;

  static constexpr uint32_t kMaxKeepaliveTime =
      apache::thrift::fast_thrift::rocket::kRSocketMaxKeepAliveTime;
  static constexpr uint32_t kMaxLifetime =
      apache::thrift::fast_thrift::rocket::kRSocketMaxLifetime;

  // Factory function type for dynamic metadata creation at connect time
  using SetupFactory = std::function<std::pair<
      std::unique_ptr<folly::IOBuf>,
      std::unique_ptr<folly::IOBuf>>()>;

  /**
   * Construct with a factory function for dynamic metadata creation.
   * The factory is called at connect time to create the metadata/data.
   */
  explicit RocketClientSetupFrameHandler(
      SetupFactory factory,
      apache::thrift::fast_thrift::rocket::RSocketKeepAliveConfig
          keepAliveConfig = {})
      : setupFactory_(std::move(factory)), keepAliveConfig_(keepAliveConfig) {
    CHECK(keepAliveConfig_.valid());
  }

  // === HandlerLifecycle ===

  template <typename Context>
  void handlerAdded(Context& /*ctx*/) noexcept {}

  template <typename Context>
  void handlerRemoved(Context& /*ctx*/) noexcept {
    // The deferred close can itself run this. StackFunctionLoopCallback moves
    // its function out before it calls it, so the reset is safe there too.
    deferredClose_.reset();
  }

  // === InboundHandler ===

  template <typename Context>
  void onPipelineActive(Context& ctx) noexcept {
    if (setupSent_) {
      return;
    }

    setupSent_ = true;
    // Since we have established a connection, we should send the setup
    // frame immediately.
    auto result = ctx.fireWrite(
        apache::thrift::fast_thrift::channel_pipeline::erase_and_box(
            makeSetupMessage()));
    if (result ==
        apache::thrift::fast_thrift::channel_pipeline::Result::Error) {
      // If we failed to send the setup frame, we should close the pipeline.
      // The handlers above this one are still being activated, and close()
      // removes every handler at once, so the close runs from a loop
      // callback, after the activation returns.
      setupSent_ = false;
      deferredClose_.emplace([&ctx]() noexcept { ctx.close(); });
      ctx.eventBase()->runInLoop(&*deferredClose_);
      return;
    }
  }

  template <typename Context>
  void onReadReady(Context& /*ctx*/) noexcept {}

  template <typename Context>
  apache::thrift::fast_thrift::channel_pipeline::Result onRead(
      Context& ctx,
      apache::thrift::fast_thrift::channel_pipeline::TypeErasedBox&&
          msg) noexcept {
    return ctx.fireRead(std::move(msg));
  }

  template <typename Context>
  void onException(Context& ctx, folly::exception_wrapper&& e) noexcept {
    ctx.fireException(std::move(e));
  }

  // === OutboundHandler ===

  template <typename Context>
  apache::thrift::fast_thrift::channel_pipeline::Result onWrite(
      Context& ctx,
      apache::thrift::fast_thrift::channel_pipeline::TypeErasedBox&&
          msg) noexcept {
    if (!setupSent_) {
      return apache::thrift::fast_thrift::channel_pipeline::Result::Error;
    }
    return ctx.fireWrite(std::move(msg));
  }

  template <typename Context>
  void onPipelineInactive(Context& /*ctx*/) noexcept {
    setupSent_ = false;
  }

  template <typename Context>
  void onWriteReady(Context& /*ctx*/) noexcept {}

 private:
  RocketRequestMessage makeSetupMessage() {
    auto [metadata, data] = setupFactory_();
    // SETUP is a connection-level frame — no stream, no streamType.
    return RocketRequestMessage{
        .frame =
            apache::thrift::fast_thrift::frame::ComposedFrame{
                .frameType =
                    apache::thrift::fast_thrift::frame::FrameType::SETUP,
                .streamId = 0,
                .metadata = std::move(metadata),
                .data = std::move(data),
                .majorVersion = kRSocketMajorVersion,
                .minorVersion = kRSocketMinorVersion,
                .keepaliveTime = keepAliveConfig_.intervalMs,
                .maxLifetime = keepAliveConfig_.maxLifetimeMs,
            },
        .requestContext = {},
    };
  }

  // Factory function for creating setup metadata and data
  bool setupSent_{false};
  SetupFactory setupFactory_;
  apache::thrift::fast_thrift::rocket::RSocketKeepAliveConfig keepAliveConfig_;
  // The close after a failed SETUP write, until it runs. Destroying it, as
  // handlerRemoved does, takes it off the EventBase queue.
  std::optional<folly::EventBase::StackFunctionLoopCallback> deferredClose_;
};

static_assert(
    apache::thrift::fast_thrift::channel_pipeline::InboundHandler<
        RocketClientSetupFrameHandler,
        apache::thrift::fast_thrift::channel_pipeline::detail::ContextImpl> &&
        apache::thrift::fast_thrift::channel_pipeline::OutboundHandler<
            RocketClientSetupFrameHandler,
            apache::thrift::fast_thrift::channel_pipeline::detail::ContextImpl>,
    "RocketClientSetupFrameHandler must satisfy InboundHandler and OutboundHandler concepts");

} // namespace apache::thrift::fast_thrift::rocket::client::handler
