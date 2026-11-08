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
#include <optional>
#include <utility>

#include <fizz/server/AsyncFizzServer.h>
#include <folly/ExceptionWrapper.h>
#include <folly/SocketAddress.h>
#include <folly/container/F14Map.h>
#include <folly/io/async/AsyncTransport.h>
#include <folly/io/async/DelayedDestruction.h>
#include <folly/logging/xlog.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/ContextImpl.h>
#include <thrift/lib/cpp2/fast_thrift/connection/security/common/Messages.h>
#include <thrift/lib/cpp2/fast_thrift/connection/security/common/Util.h>
#include <thrift/lib/cpp2/fast_thrift/connection/security/util/StopTLSHelper.h>

namespace apache::thrift::fast_thrift::connection::security::handler {

/**
 * Inner-pipeline middle handler that performs StopTLS V1 downgrade on
 * connections whose handshake negotiated it. Stateless apart from its
 * in-flight helper map: the per-connection decision lives entirely on the
 * incoming TLSRequestMessage::extension.
 *
 * onWrite (work path):
 *   - `extension` null OR `getNegotiatedStopTLS()` false → passthrough.
 *   - Otherwise: take ownership of the AsyncFizzServer transport, spawn
 *     a StopTLSHelper, and on success refire the message with the
 *     downgraded plaintext transport (extension preserved so callers
 *     retain access to negotiated parameters).
 *
 * StopTLS failures drop the connection and report it by firing an exception
 * onto the pipeline. See Util.h.
 *
 * The aggregate pending-connection handler bounds sockets across the complete
 * TLS pipeline, including downgrades parked here. Each downgrade is also
 * bounded by the handshake-timeout budget off the incoming message.
 */
class StopTLSV1Handler {
 public:
  StopTLSV1Handler() = default;

  ~StopTLSV1Handler() = default;

  StopTLSV1Handler(const StopTLSV1Handler&) = delete;
  StopTLSV1Handler& operator=(const StopTLSV1Handler&) = delete;
  StopTLSV1Handler(StopTLSV1Handler&&) = delete;
  StopTLSV1Handler& operator=(StopTLSV1Handler&&) = delete;

  template <typename Context>
  void handlerAdded(Context& ctx) noexcept {
    ctx_ = &ctx;
  }

  template <typename Context>
  void handlerRemoved(Context& /*ctx*/) noexcept {
    ctx_ = nullptr;
  }

  // === Outbound (work path) ===

  template <typename Context>
  channel_pipeline::Result onWrite(
      Context& ctx, channel_pipeline::TypeErasedBox&& msg) noexcept {
    auto incoming = msg.take<TLSRequestMessage>();

    // No extension or StopTLS V1 not negotiated → passthrough.
    if (!incoming.extension || !incoming.extension->getNegotiatedStopTLS()) {
      return ctx.fireWrite(
          channel_pipeline::erase_and_box(std::move(incoming)));
    }

    // Defensive: extension says V1 but transport isn't fizz. Forward as-is
    // and let downstream reject if it must — we don't have a fizz session
    // to drive StopTLS against.
    auto* fizzPtr =
        dynamic_cast<fizz::server::AsyncFizzServer*>(incoming.transport.get());
    if (fizzPtr == nullptr) [[unlikely]] {
      XLOG(DBG3)
          << "StopTLS V1 negotiated but transport is not AsyncFizzServer "
             "for "
          << incoming.clientAddr.describe();
      return ctx.fireWrite(
          channel_pipeline::erase_and_box(std::move(incoming)));
    }

    (void)incoming.transport.release();
    fizz::server::AsyncFizzServer::UniquePtr fizzServer(fizzPtr);
    auto clientAddr = std::move(incoming.clientAddr);
    auto extension = std::move(incoming.extension);
    auto stopTLSTimeout = incoming.tlsParams
        ? incoming.tlsParams->handshakeTimeout
        : std::nullopt;
    // The downgrade hands back a plaintext transport that can no longer report
    // the peer, so the handshake's record of it has to ride the callback.
    auto peerSecurity = std::move(incoming.peerSecurity);
    auto permit = std::move(incoming.pendingConnectionPermit);

    util::StopTLSHelper::UniquePtr helper(new util::StopTLSHelper(
        std::move(fizzServer),
        stopTLSTimeout,
        [this](util::StopTLSHelper* h) noexcept { inFlight_.erase(h); },
        [this, clientAddr, extension, peerSecurity, permit = std::move(permit)](
            folly::AsyncTransport::UniquePtr plaintext,
            const folly::exception_wrapper& ex) mutable noexcept {
          onStopTLSComplete(
              std::move(plaintext),
              ex,
              clientAddr,
              extension,
              peerSecurity,
              std::move(permit));
        }));
    auto* raw = helper.get();
    inFlight_.emplace(raw, std::move(helper));
    raw->start();

    // Message absorbed; downstream sees it again only on success.
    return channel_pipeline::Result::Success;
  }

  template <typename Context>
  void onException(Context& ctx, folly::exception_wrapper&& e) noexcept {
    ctx.fireException(std::move(e));
  }

  template <typename Context>
  void onPipelineActive(Context& /*ctx*/) noexcept {
    // Cleared, not just set on the way down: a pipeline may be deactivated
    // and activated again, and a latched flag would silently suppress every
    // future failure report for the life of the handler.
    shuttingDown_ = false;
  }

  template <typename Context>
  void onReadReady(Context& /*ctx*/) noexcept {}

  // === Inbound (passthrough) ===

  template <typename Context>
  channel_pipeline::Result onRead(
      Context& ctx, channel_pipeline::TypeErasedBox&& msg) noexcept {
    return ctx.fireRead(std::move(msg));
  }

  template <typename Context>
  void onPipelineInactive(Context& /*ctx*/) noexcept {
    // cancel() below drives each helper's completion callback synchronously
    // with a cancellation error. That is teardown, not a connection failing,
    // so it must not be reported as one — and firing into a pipeline that is
    // mid-deactivate would re-enter handlers that have already been told the
    // pipeline is inactive.
    shuttingDown_ = true;
    auto drained = std::move(inFlight_);
    for (auto& [_, helper] : drained) {
      folly::DelayedDestruction::DestructorGuard guard(helper.get());
      helper->cancel();
    }
  }

  template <typename Context>
  void onWriteReady(Context& /*ctx*/) noexcept {}

 private:
  void onStopTLSComplete(
      folly::AsyncTransport::UniquePtr plaintext,
      const folly::exception_wrapper& ex,
      const folly::SocketAddress& clientAddr,
      std::shared_ptr<apache::thrift::ThriftParametersServerExtension>
          extension,
      std::shared_ptr<const PeerSecurityInfo> peerSecurity,
      PendingConnectionPermit permit) noexcept {
    if (ex || !plaintext) {
      XLOG(DBG3) << "StopTLS V1 failed for " << clientAddr.describe() << ": "
                 << (ex ? ex.what().toStdString() : std::string("null"));
      if (!shuttingDown_) {
        fireConnectionFailure(ctx_, ex, "StopTLS V1 failed", clientAddr);
      }
      return;
    }
    if (!ctx_) [[unlikely]] {
      // Handler already removed from the pipeline: the downgrade landed after
      // teardown began, so there is nowhere to forward it. The transport dies
      // with this callback.
      XLOG_EVERY_MS(WARNING, 1000)
          << "Dropping downgraded connection from " << clientAddr.describe()
          << ": TLS pipeline already torn down";
      return;
    }
    TLSRequestMessage downgraded{
        .transport = std::move(plaintext),
        .clientAddr = clientAddr,
        .tlsParams = nullptr,
        .extension = std::move(extension),
        .peerSecurity = std::move(peerSecurity),
        .pendingConnectionPermit = std::move(permit),
    };
    auto result =
        ctx_->fireWrite(channel_pipeline::erase_and_box(std::move(downgraded)));
    switch (result) {
      case channel_pipeline::Result::Success:
        return;
      case channel_pipeline::Result::Backpressure:
        XLOG_EVERY_MS(WARN, 1000)
            << "Downstream backpressure after StopTLS V1 from "
            << clientAddr.describe();
        return;
      case channel_pipeline::Result::Error:
        XLOG(WARN) << "Downstream rejected post-StopTLS connection from "
                   << clientAddr.describe();
        return;
    }
  }

  folly::F14FastMap<util::StopTLSHelper*, util::StopTLSHelper::UniquePtr>
      inFlight_;
  channel_pipeline::detail::ContextImpl* ctx_{nullptr};
  // Set once teardown begins; see onPipelineInactive.
  bool shuttingDown_{false};
};

} // namespace apache::thrift::fast_thrift::connection::security::handler
