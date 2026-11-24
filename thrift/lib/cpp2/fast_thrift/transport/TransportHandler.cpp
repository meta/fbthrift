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

// TransportHandlerT is header-only — bodies live in TransportHandler.h so any
// TU can implicit-instantiate with its own WriteCompleteEventFactory.

#include <thrift/lib/cpp2/fast_thrift/transport/TransportHandler.h>

#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/Liburing.h>

#if FOLLY_HAS_LIBURING
#include <folly/io/async/IoUringBackend.h>
#endif

namespace apache::thrift::fast_thrift::transport::detail {

bool setSocketBufferCallback(
    folly::AsyncTransport& transport,
    folly::AsyncTransport::BufferCallback* callback) noexcept {
  auto* socket = transport.getUnderlyingTransport<folly::AsyncSocket>();
  if (socket == nullptr) {
    return false;
  }
  socket->setBufferCallback(callback);
  return true;
}

bool requiresMovableReadCallback(
    const folly::AsyncTransport& transport) noexcept {
#if FOLLY_HAS_LIBURING
  const folly::AsyncSocket* const socket =
      dynamic_cast<const folly::AsyncSocket*>(&transport);
  return socket != nullptr &&
      dynamic_cast<const folly::IoUringBackend*>(
          socket->getEventBase()->getBackend()) != nullptr;
#else
  (void)transport;
  return false;
#endif
}

} // namespace apache::thrift::fast_thrift::transport::detail
