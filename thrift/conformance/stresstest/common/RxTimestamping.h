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

#include <sys/socket.h>

#include <chrono>
#include <memory>
#include <optional>

#include <folly/Function.h>

namespace folly {
class AsyncSocket;
}

namespace apache::thrift::stress {

using RxTimestampSink =
    folly::Function<void(std::chrono::microseconds collect)>;

class RxTimestampingRegistration {
 public:
  virtual ~RxTimestampingRegistration();

  RxTimestampingRegistration(const RxTimestampingRegistration&) = delete;
  RxTimestampingRegistration& operator=(const RxTimestampingRegistration&) =
      delete;

 protected:
  RxTimestampingRegistration() = default;
};

// Retain the registration while collecting timestamps. Install and destroy it
// on the socket's EventBase (or its owning thread when unattached), normally
// before destroying the transport. Reset it before replacing the socket, then
// reinstall on the completed replacement; registrations do not follow socket
// moves. Destruction unregisters only this callback and leaves SO_TIMESTAMPING
// enabled.
[[nodiscard]] std::unique_ptr<RxTimestampingRegistration> installRxTimestamping(
    folly::AsyncSocket& socket, RxTimestampSink sink) noexcept;

namespace detail {

std::optional<std::chrono::microseconds> getRxTimestampCollect(
    struct ::msghdr& msg, std::chrono::nanoseconds now) noexcept;

} // namespace detail

} // namespace apache::thrift::stress
