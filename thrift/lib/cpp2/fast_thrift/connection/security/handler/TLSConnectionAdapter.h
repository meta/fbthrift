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

#include <memory>
#include <utility>

#include <folly/ExceptionWrapper.h>
#include <folly/SocketAddress.h>
#include <folly/io/async/AsyncTransport.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/connection/security/common/Messages.h>

namespace apache::thrift::fast_thrift::connection::security::handler {

/**
 * Tail endpoint of the TLS pipeline and its single integration point with the
 * owner that drives it.
 *
 * When the resolved transport returns on the read path, onRead() dispatches
 * it to the owner. Exceptions raised inside the pipeline use a separate
 * callback so the owner can propagate them.
 */
class TLSConnectionAdapter {
 public:
  // Dispatches the resolved transport back to the owner once the pipeline
  // completes. Returns the downstream Result so backpressure / errors
  // propagate to the caller.
  using ResolvedFn = channel_pipeline::Result (*)(
      void*,
      folly::AsyncTransport::UniquePtr,
      folly::SocketAddress,
      std::unique_ptr<PeerSecurityInfo>) noexcept;
  // Dispatches an exception raised inside the pipeline back to the owner.
  using ExceptionFn = void (*)(void*, folly::exception_wrapper&&) noexcept;

  TLSConnectionAdapter() noexcept = default;
  TLSConnectionAdapter(
      void* owner, ResolvedFn onResolved, ExceptionFn onException) noexcept
      : owner_(owner), onResolved_(onResolved), onException_(onException) {}
  ~TLSConnectionAdapter() = default;
  TLSConnectionAdapter(const TLSConnectionAdapter&) = delete;
  TLSConnectionAdapter& operator=(const TLSConnectionAdapter&) = delete;
  TLSConnectionAdapter(TLSConnectionAdapter&&) = delete;
  TLSConnectionAdapter& operator=(TLSConnectionAdapter&&) = delete;

  // Register the owner and its dispatch trampolines. Must be called before the
  // pipeline runs.
  void setOwner(
      void* owner, ResolvedFn onResolved, ExceptionFn onException) noexcept {
    owner_ = owner;
    onResolved_ = onResolved;
    onException_ = onException;
  }

  // Read-exit: the resolved transport returns here. Hand it back to the owner.
  template <typename Context>
  channel_pipeline::Result onRead(
      Context&, channel_pipeline::TypeErasedBox&& msg) noexcept {
    auto resolved = msg.take<TLSResponseMessage>();
    DCHECK(onResolved_);
    return onResolved_(
        owner_,
        std::move(resolved.transport),
        std::move(resolved.clientAddr),
        std::move(resolved.peerSecurity));
  }

  void onException(folly::exception_wrapper&& e) noexcept {
    DCHECK(onException_);
    onException_(owner_, std::move(e));
  }

  void onWriteReady() noexcept {}
  void handlerAdded() noexcept {}
  void handlerRemoved() noexcept {}
  void onPipelineActive() noexcept {}
  void onPipelineInactive() noexcept {}

 private:
  void* owner_{nullptr};
  ResolvedFn onResolved_{nullptr};
  ExceptionFn onException_{nullptr};
};

} // namespace apache::thrift::fast_thrift::connection::security::handler
