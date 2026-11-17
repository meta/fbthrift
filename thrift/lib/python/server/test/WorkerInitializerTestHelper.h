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
#include <string>
#include <utility>

#include <Python.h>
#include <folly/futures/Future.h>
#include <folly/io/async/Request.h>
#include <thrift/lib/python/server/execution/ExecutionSystem.h>
#include <thrift/lib/python/server/execution/RequestDispatch.h>

namespace apache::thrift::python::test {

// Starts one request whose selected handler function is its own coroutine
// factory, so a test observes which function and loop each request received.
inline void startSelectedFunctionForTest(
    execution::ExecutionSystem& executionSystem, std::string method) {
  executionSystem.execute(
      std::move(method),
      execution::RequestDispatch(
          folly::Executor::KeepAlive<>{},
          [](PyObject* selectedFunction,
             const folly::Executor::KeepAlive<>&,
             execution::LaunchRequest launchRequest) {
            std::move(launchRequest)(selectedFunction);
            return folly::makeSemiFuture();
          }));
}

// Names the request whose RequestContext carries it. It stands in for the
// data that ContextProp keeps on a request's RequestContext, such as the
// request id and the privacy policy zone.
class RequestLabelForTest : public folly::RequestData {
 public:
  explicit RequestLabelForTest(std::string label) : label_(std::move(label)) {}

  bool hasCallback() override { return false; }

  const std::string& label() const { return label_; }

 private:
  std::string label_;
};

inline const folly::RequestToken& requestLabelTokenForTest() {
  static const folly::RequestToken token("thrift_python_test_request_label");
  return token;
}

// Starts one request as the server does, under a RequestContext of its own,
// which here carries `label`.
inline void startLabeledRequestForTest(
    execution::ExecutionSystem& executionSystem,
    std::string method,
    std::string label) {
  folly::RequestContextScopeGuard requestContextGuard;
  folly::RequestContext::get()->setContextData(
      requestLabelTokenForTest(),
      std::make_unique<RequestLabelForTest>(std::move(label)));
  startSelectedFunctionForTest(executionSystem, std::move(method));
}

// Returns the label on this thread's current RequestContext, or an empty
// string when that context carries none.
inline std::string currentRequestLabelForTest() {
  const auto* const requestLabel = dynamic_cast<const RequestLabelForTest*>(
      folly::RequestContext::get()->getContextData(requestLabelTokenForTest()));
  return requestLabel == nullptr ? std::string{} : requestLabel->label();
}

} // namespace apache::thrift::python::test
