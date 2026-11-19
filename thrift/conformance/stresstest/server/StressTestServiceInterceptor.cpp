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

#include <thrift/conformance/stresstest/server/StressTestServiceInterceptor.h>

#include <utility>

#include <glog/logging.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/AsyncTransport.h>
#include <thrift/conformance/stresstest/common/RxTimestamping.h>
#include <thrift/conformance/stresstest/server/StressTestServerStats.h>
#include <thrift/lib/cpp2/security/SSLUtil.h>
#include <thrift/lib/cpp2/server/Cpp2ConnContext.h>

namespace apache::thrift::stress {
namespace {

ConnectionMode classifyConnection(
    const apache::thrift::Cpp2ConnContext& context) {
  const auto& protocol = context.getSecurityProtocol();
  if (protocol == apache::thrift::kSecurityProtocolStopTLSV2) {
    return ConnectionMode::StopTlsV2;
  }
  if (context.isTransportEncrypted()) {
    return ConnectionMode::Tls;
  }
  return protocol.empty() ? ConnectionMode::Plaintext : ConnectionMode::Unknown;
}

std::unique_ptr<RxTimestampingRegistration> installServerRxTimestamping(
    const apache::thrift::Cpp2ConnContext& context,
    std::shared_ptr<StressTestServerStats> serverStats) {
  auto* transport = const_cast<folly::AsyncTransport*>(context.getTransport());
  auto* socket = transport == nullptr
      ? nullptr
      : transport->getUnderlyingTransport<folly::AsyncSocket>();
  if (socket == nullptr) {
    LOG_FIRST_N(WARNING, 10)
        << "Cannot enable server RX timestamping: no native AsyncSocket";
    return nullptr;
  }
  DCHECK(
      socket->getEventBase() == nullptr ||
      socket->getEventBase()->isInEventBaseThread());

  return installRxTimestamping(
      *socket,
      [serverStats =
           std::move(serverStats)](std::chrono::microseconds collect) {
        serverStats->recordRxTimestampCollect(collect);
      });
}

} // namespace

StressTestServiceInterceptor::StressTestServiceInterceptor(
    std::shared_ptr<StressTestServerStats> serverStats,
    bool rxTimestampingEnabled)
    : serverStats_(std::move(serverStats)),
      rxTimestampingEnabled_(rxTimestampingEnabled) {}

std::string StressTestServiceInterceptor::getName() const {
  return "StressTestServiceInterceptor";
}

folly::coro::Task<std::optional<folly::Unit>>
StressTestServiceInterceptor::onRequest(ConnectionState*, RequestInfo) {
  co_return std::nullopt;
}

folly::coro::Task<void> StressTestServiceInterceptor::onResponse(
    RequestState*, ConnectionState*, ResponseInfo) {
  co_return;
}

std::optional<StressTestServiceInterceptor::ConnectionState>
StressTestServiceInterceptor::onConnectionEstablished(ConnectionInfo info) {
  if (serverStats_ && info.context) {
    serverStats_->recordConnection(classifyConnection(*info.context));
    if (rxTimestampingEnabled_) {
      if (auto registration =
              installServerRxTimestamping(*info.context, serverStats_)) {
        return registration;
      }
    }
  }
  return std::nullopt;
}

void StressTestServiceInterceptor::onConnectionClosed(
    ConnectionState* state, ConnectionInfo) noexcept {
  if (state != nullptr) {
    state->reset();
  }
}

} // namespace apache::thrift::stress
