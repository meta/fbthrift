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

#include <string>

#include <Python.h>
#include <folly/Executor.h>
#include <folly/Function.h>
#include <folly/futures/Future.h>

namespace apache::thrift::python::execution {

// Launches the request's Python-side processing where placement put it. The
// coroutine factory is borrowed only for the call.
using LaunchRequest = folly::Function<int(_object*)>;

// Carries one request to the execution system. The server supplies the request
// executor, which keeps the server alive while follow-up work holds it. When
// the execution system dispatches the request, it passes the follow-up
// executor that the request's native follow-up work runs on: the request
// executor for a request placed on control, and the Worker's executor for a
// request placed on a Worker.
class RequestDispatch {
 public:
  using Function = folly::Function<folly::SemiFuture<folly::Unit>(
      PyObject* selectedHandlerFunction,
      folly::Executor::KeepAlive<> followUpExecutor,
      LaunchRequest launchRequest)>;

  RequestDispatch(
      folly::Executor::KeepAlive<> requestExecutor, Function function)
      : requestExecutor_(std::move(requestExecutor)),
        function_(std::move(function)) {}

  const folly::Executor::KeepAlive<>& getRequestExecutor() const {
    return requestExecutor_;
  }

  folly::SemiFuture<folly::Unit> operator()(
      PyObject* selectedHandlerFunction,
      folly::Executor::KeepAlive<> followUpExecutor,
      LaunchRequest launchRequest) && {
    return function_(
        selectedHandlerFunction,
        std::move(followUpExecutor),
        std::move(launchRequest));
  }

 private:
  folly::Executor::KeepAlive<> requestExecutor_;
  Function function_;
};

} // namespace apache::thrift::python::execution
