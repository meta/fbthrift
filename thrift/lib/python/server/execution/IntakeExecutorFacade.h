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
#include <cstddef>
#include <cstdint>
#include <utility>

#include <folly/Executor.h>

namespace apache::thrift::python {
class PythonAsyncProcessorFactory;
} // namespace apache::thrift::python

namespace apache::thrift::python::execution {

/**
 * The one executor a thrift-python processor factory gives ThriftServer for the
 * server's whole life. It sends the server's work to the factory's control
 * executor, through a keep-alive it holds from creation.
 *
 * The executor counts its creator's keep-alive and every other holder's, and
 * deletes itself on the last release. It runs no callbacks of its own, so it
 * never waits for anyone; a server that outlives the creating factory keeps it,
 * and the control executor, alive.
 *
 * Only PythonAsyncProcessorFactory creates it; every other holder sees it as a
 * plain folly::Executor.
 */
class IntakeExecutorFacade final : public folly::Executor {
 protected:
  bool keepAliveAcquire() noexcept override {
    references_.fetch_add(1, std::memory_order_relaxed);
    return true;
  }

  void keepAliveRelease() noexcept override {
    if (references_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      delete this;
    }
  }

 private:
  friend class ::apache::thrift::python::PythonAsyncProcessorFactory;

  static KeepAlive<IntakeExecutorFacade> create(KeepAlive<> control) {
    return makeKeepAlive(new IntakeExecutorFacade(std::move(control)));
  }

  void add(folly::Func func) override { control_->add(std::move(func)); }

  void addWithPriority(folly::Func func, int8_t priority) override {
    control_->addWithPriority(std::move(func), priority);
  }

  uint8_t getNumPriorities() const override {
    return control_->getNumPriorities();
  }

  explicit IntakeExecutorFacade(KeepAlive<> control)
      : control_(std::move(control)) {}
  ~IntakeExecutorFacade() override = default;

  // create() adopts this first reference for the creator's keep-alive.
  std::atomic<std::size_t> references_{1};
  KeepAlive<> control_;
};

} // namespace apache::thrift::python::execution
