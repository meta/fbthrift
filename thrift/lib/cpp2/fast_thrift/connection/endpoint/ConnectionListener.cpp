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

#include <thrift/lib/cpp2/fast_thrift/connection/endpoint/ConnectionListener.h>

#include <netinet/in.h>

#include <folly/io/async/AsyncSocket.h>
#include <folly/logging/xlog.h>
#include <folly/net/NetworkSocket.h>

namespace apache::thrift::fast_thrift::connection {

void ConnectionListener::connectionAccepted(
    folly::NetworkSocket fd,
    const folly::SocketAddress& clientAddr,
    AcceptInfo) noexcept {
  DCHECK(evb_->isInEventBaseThread());
  if (!pipeline_) [[unlikely]] {
    folly::netops::close(fd);
    return;
  }

  auto socket = folly::AsyncSocket::newSocket(evb_, fd);
  socket->setMaxReadsPerEvent(socketOptions_.maxReadsPerEvent);
  if (socketOptions_.tcpNoDelay) {
    socket->setNoDelay(true);
  }
  if (socketOptions_.trafficClass > 0 && clientAddr.getFamily() == AF_INET6 &&
      socket->setSockOpt(
          IPPROTO_IPV6, IPV6_TCLASS, &socketOptions_.trafficClass) != 0) {
    XLOG_EVERY_MS(ERR, 1000)
        << "Failed to set IPV6_TCLASS=" << socketOptions_.trafficClass
        << " on accepted socket";
  }

  folly::AsyncTransport::UniquePtr transport(socket.release());
  const auto result = pipeline_->fireRead(
      channel_pipeline::erase_and_box(
          ConnectionMessage{
              .transport = std::move(transport),
              .clientAddr = clientAddr,
              .peerSecurity = nullptr,
          }));
  if (result != channel_pipeline::Result::Success) [[unlikely]] {
    XLOG_EVERY_MS(WARN, 1000) << "Connection pipeline rejected connection from "
                              << clientAddr.describe();
  }
}

void ConnectionListener::acceptError(folly::exception_wrapper ew) noexcept {
  XLOG_EVERY_MS(ERR, 1000) << "Accept dispatch failed: " << ew.what();
}

} // namespace apache::thrift::fast_thrift::connection
