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

#include <folly/ExceptionWrapper.h>
#include <folly/SocketAddress.h>
#include <folly/io/async/AsyncServerSocket.h>
#include <folly/io/async/EventBase.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineRef.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/connection/SocketOptions.h>
#include <thrift/lib/cpp2/fast_thrift/connection/common/Messages.h>

namespace apache::thrift::fast_thrift::connection {

// Per-data-EventBase accept callback and head of the connection pipeline.
// ConnectionAcceptor owns the listening socket and dispatches accepted fds
// here, either inline or through AsyncServerSocket's remote-accept queue.
class ConnectionListener final
    : public folly::AsyncServerSocket::AcceptCallback {
 public:
  ConnectionListener(
      folly::EventBase& evb, SocketOptions socketOptions) noexcept
      : evb_(&evb), socketOptions_(std::move(socketOptions)) {}

  ~ConnectionListener() override = default;

  ConnectionListener(const ConnectionListener&) = delete;
  ConnectionListener& operator=(const ConnectionListener&) = delete;
  ConnectionListener(ConnectionListener&&) = delete;
  ConnectionListener& operator=(ConnectionListener&&) = delete;

  void setPipeline(channel_pipeline::PipelineRef pipeline) noexcept {
    DCHECK(pipeline);
    DCHECK(!pipeline_) << "setPipeline called twice without resetPipeline";
    pipeline_ = pipeline;
  }

  void resetPipeline() noexcept { pipeline_.reset(); }

  void connectionAccepted(
      folly::NetworkSocket fd,
      const folly::SocketAddress& clientAddr,
      AcceptInfo) noexcept override;
  void acceptError(folly::exception_wrapper ew) noexcept override;

  // A worker listener may be registered with several acceptors. Stopping one
  // acceptor must not affect the shared worker pipeline.
  void acceptStopped() noexcept override {}

  template <typename Context>
  channel_pipeline::Result onWrite(
      Context&, channel_pipeline::TypeErasedBox&&) noexcept {
    return channel_pipeline::Result::Error;
  }

  void handlerAdded() noexcept {}
  void handlerRemoved() noexcept {}
  void onPipelineActive() noexcept {}
  void onPipelineInactive() noexcept {}
  void onReadReady() noexcept {}

 private:
  folly::EventBase* evb_;
  SocketOptions socketOptions_;
  channel_pipeline::PipelineRef pipeline_;
};

} // namespace apache::thrift::fast_thrift::connection
