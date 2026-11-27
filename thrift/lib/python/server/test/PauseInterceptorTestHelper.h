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

#include <atomic>
#include <chrono>
#include <optional>
#include <string>

#include <folly/Unit.h>
#include <folly/futures/Future.h>

#include <thrift/lib/cpp2/server/ServiceInterceptor.h>

namespace apache::thrift::python::test {

/**
 * Pauses every request on a 1 ms timer before the request reaches Python. The
 * timer fires on folly's timekeeper thread, so the request resumes there unless
 * the server moves it back to its own executor.
 */
class PauseInterceptor
    : public apache::thrift::ServiceInterceptor<folly::Unit, folly::Unit> {
 public:
  std::string getName() const override { return "PauseInterceptor"; }

#if FOLLY_HAS_COROUTINES
  folly::coro::Task<std::optional<folly::Unit>> onRequest(
      folly::Unit*, RequestInfo) override {
    co_await folly::futures::sleep(std::chrono::milliseconds(1));
    requests_.fetch_add(1, std::memory_order_relaxed);
    co_return std::nullopt;
  }
#endif

  int requests() const { return requests_.load(std::memory_order_relaxed); }

 private:
  std::atomic<int> requests_{0};
};

} // namespace apache::thrift::python::test
