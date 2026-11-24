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

#include <stdexcept>
#include <folly/Try.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/EventBaseManager.h>
#include <thrift/lib/cpp2/async/HeaderClientChannel.h>
#include <thrift/lib/cpp2/async/RequestChannel.h>

namespace apache::thrift::python::test {

inline folly::Try<apache::thrift::RequestChannel::Ptr> lateChannelResult(
    bool fail) {
  if (fail) {
    return folly::Try<apache::thrift::RequestChannel::Ptr>(
        folly::make_exception_wrapper<std::runtime_error>(
            "late channel failure"));
  }
  return folly::Try<apache::thrift::RequestChannel::Ptr>(
      apache::thrift::HeaderClientChannel::newChannel(
          folly::AsyncSocket::newSocket(
              folly::EventBaseManager::get()->getEventBase())));
}

} // namespace apache::thrift::python::test
