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
#include <vector>

#include <folly/Synchronized.h>
#include <folly/Unit.h>

#include <thrift/lib/cpp2/server/ServiceInterceptor.h>

namespace apache::thrift::python::test {

// Drives the kill switch over serving a service's schema, so a test can assert
// what an interceptor sees with it both ways.
void setPopulateServiceSchemaForTest(bool enabled);
void unmockPopulateServiceSchemaForTest();

/**
 * A C++ ServiceInterceptor that renders each request's `dynamicArguments` to a
 * string, so a thrift-python server test can assert on what a C++ interceptor
 * sees. Renders the parameters of
 * `python_service_schema_test.thrift`'s `TestService.describe`.
 *
 * Reads as "<no schema>" when `dynamicArguments` is null, which is what a
 * service without a bundled schema gives an interceptor.
 */
class DynamicArgumentsRecorder
    : public apache::thrift::ServiceInterceptor<folly::Unit, folly::Unit> {
 public:
  std::string getName() const override { return "DynamicArgumentsRecorder"; }

#if FOLLY_HAS_COROUTINES
  folly::coro::Task<std::optional<folly::Unit>> onRequest(
      folly::Unit*, RequestInfo) override;
#endif

  std::vector<std::string> records() const { return *records_.rlock(); }

  void clear() { records_.wlock()->clear(); }

 private:
  static std::string render(const RequestInfo& requestInfo);

  folly::Synchronized<std::vector<std::string>> records_;
};

} // namespace apache::thrift::python::test
