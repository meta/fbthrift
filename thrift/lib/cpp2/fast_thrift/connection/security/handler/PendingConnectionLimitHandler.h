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

#include <folly/ExceptionWrapper.h>
#include <folly/logging/xlog.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/connection/security/common/Messages.h>
#include <thrift/lib/cpp2/fast_thrift/connection/security/common/PendingConnection.h>

namespace apache::thrift::fast_thrift::connection::security::handler {

/**
 * Admits at most one worker's configured number of connections into the TLS
 * pipeline. The permit attached here spans every asynchronous TLS stage and
 * releases itself when the connection is established or dropped.
 */
class PendingConnectionLimitHandler {
 public:
  explicit PendingConnectionLimitHandler(PendingConnectionState& state) noexcept
      : state_(state) {}

  template <typename Context>
  [[nodiscard]] channel_pipeline::Result onWrite(
      Context& ctx, channel_pipeline::TypeErasedBox&& msg) noexcept {
    auto incoming = msg.take<TLSRequestMessage>();
    if (state_.atCapacity()) [[unlikely]] {
      XLOG_EVERY_MS(WARN, 1000)
          << "Refusing connection from " << incoming.clientAddr.describe()
          << ": already processing " << state_.maxPendingConnections
          << " pending connections on this worker";
      return channel_pipeline::Result::Error;
    }
    incoming.pendingConnectionPermit = PendingConnectionPermit(state_);
    return ctx.fireWrite(channel_pipeline::erase_and_box(std::move(incoming)));
  }

  template <typename Context>
  [[nodiscard]] channel_pipeline::Result onRead(
      Context& ctx, channel_pipeline::TypeErasedBox&& msg) noexcept {
    return ctx.fireRead(std::move(msg));
  }

  template <typename Context>
  void onException(Context& ctx, folly::exception_wrapper&& e) noexcept {
    ctx.fireException(std::move(e));
  }

  template <typename Context>
  void handlerAdded(Context& /*ctx*/) noexcept {}
  template <typename Context>
  void handlerRemoved(Context& /*ctx*/) noexcept {}
  template <typename Context>
  void onPipelineActive(Context& /*ctx*/) noexcept {}
  template <typename Context>
  void onPipelineInactive(Context& /*ctx*/) noexcept {}
  template <typename Context>
  void onReadReady(Context& /*ctx*/) noexcept {}
  template <typename Context>
  void onWriteReady(Context& /*ctx*/) noexcept {}

 private:
  PendingConnectionState& state_;
};

} // namespace apache::thrift::fast_thrift::connection::security::handler
