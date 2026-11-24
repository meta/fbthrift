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
#include <utility>

#include <folly/ExceptionWrapper.h>
#include <folly/logging/xlog.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>

namespace apache::thrift::fast_thrift::connection {

/**
 * Tail of the acceptance pipeline. Receives a typed Connection from the
 * preceding handler (ConnectionBuilderHandler, possibly via the optional
 * ConnectionAcceptCallbackHandler) and forwards it to the supplied
 * registerConnection callback — which stashes it in ConnectionHandler's
 * connections map.
 *
 * Templated on the concrete Connection type. The registerConnection
 * lambda is constructed inside ConnectionHandler::setConnectionFactory,
 * where the concrete type is known.
 */
template <typename Conn>
class ConnectionInstaller {
 public:
  using RegisterFn = std::function<void(Conn)>;

  explicit ConnectionInstaller(RegisterFn registerConnection) noexcept
      : registerConnection_(std::move(registerConnection)) {
    DCHECK(registerConnection_);
  }

  ConnectionInstaller(const ConnectionInstaller&) = delete;
  ConnectionInstaller& operator=(const ConnectionInstaller&) = delete;
  ConnectionInstaller(ConnectionInstaller&&) = delete;
  ConnectionInstaller& operator=(ConnectionInstaller&&) = delete;
  ~ConnectionInstaller() = default;

  template <typename Context>
  channel_pipeline::Result onRead(
      Context&, channel_pipeline::TypeErasedBox&& msg) noexcept {
    registerConnection_(msg.take<Conn>());
    return channel_pipeline::Result::Success;
  }

  void onException(folly::exception_wrapper&& e) noexcept {
    // Rate-limited: this is where every TLS stage's per-connection failure
    // converges, so a peer that can make handshakes fail can otherwise drive
    // one log line per connection attempt.
    XLOG_EVERY_MS(ERR, 1000) << "ConnectionInstaller: " << e.what();
  }

  void handlerAdded() noexcept {}
  void handlerRemoved() noexcept {}
  void onPipelineActive() noexcept {}
  void onPipelineInactive() noexcept {}
  void onWriteReady() noexcept {}

 private:
  RegisterFn registerConnection_;
};

} // namespace apache::thrift::fast_thrift::connection
