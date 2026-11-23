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
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <folly/Executor.h>
#include <thrift/lib/python/server/PythonAsyncProcessorFactory.h>
#include <thrift/lib/python/server/execution/ExecutionSystem.h>

namespace apache::thrift::python::test {

class ForwardingKeepAliveTrackingExecutor final : public folly::Executor {
 public:
  explicit ForwardingKeepAliveTrackingExecutor(folly::Executor* delegate)
      : delegate_(delegate) {}

  void add(folly::Func func) override { delegate_->add(std::move(func)); }

  bool keepAliveAcquire() noexcept override {
    keepAliveCount_.fetch_add(1, std::memory_order_relaxed);
    return true;
  }

  void keepAliveRelease() noexcept override {
    keepAliveCount_.fetch_sub(1, std::memory_order_relaxed);
  }

  std::size_t keepAliveCount() const noexcept {
    return keepAliveCount_.load(std::memory_order_relaxed);
  }

 private:
  folly::Executor* delegate_;
  std::atomic<std::size_t> keepAliveCount_{0};
};

inline bool isFreeThreadedBuild() noexcept {
#ifdef Py_GIL_DISABLED
  return true;
#else
  return false;
#endif
}

inline std::size_t createProcessorsWhileLegacyStopRuns(
    ForwardingKeepAliveTrackingExecutor& controlExecutor,
    const std::size_t iterationCount) {
  std::shared_ptr<execution::ExecutionSystem> executionSystem =
      execution::makeControlOnlyExecutionSystem(
          &controlExecutor,
          [](PyObject*) { return 0; },
          std::map<std::string, PyObject*>{});
  auto factory = PythonAsyncProcessorFactory::create(
      nullptr,
      {},
      {},
      std::move(executionSystem),
      folly::getKeepAliveToken(controlExecutor),
      "test.Service");
  std::atomic<std::size_t> readyCount{0};
  std::size_t processorCount = 0;
  auto waitForPeer = [&] {
    readyCount.fetch_add(1, std::memory_order_release);
    while (readyCount.load(std::memory_order_acquire) != 2) {
      std::this_thread::yield();
    }
  };
  std::thread processorThread([&] {
    waitForPeer();
    for (std::size_t i = 0; i < iterationCount; ++i) {
      auto processor = factory->getProcessor();
      ++processorCount;
    }
  });
  std::thread stopThread([&] {
    waitForPeer();
    for (std::size_t i = 0; i < iterationCount; ++i) {
      factory->semifuture_onStopRequested().get();
    }
  });
  processorThread.join();
  stopThread.join();
  return processorCount;
}

inline std::shared_ptr<PythonAsyncProcessorFactory> createHostedTestFactory(
    PyObject* pythonServer,
    FunctionMapType functions,
    std::vector<PyObject*> lifecycleFuncs,
    ForwardingKeepAliveTrackingExecutor& controlExecutor,
    execution::LaunchControlRequest launchControlRequest) {
  std::map<std::string, PyObject*> controlMethods;
  for (const auto& [name, function] : functions) {
    controlMethods.emplace(std::string(name), function.funcObject);
  }
  std::shared_ptr<execution::ExecutionSystem> executionSystem =
      execution::makeControlOnlyExecutionSystem(
          &controlExecutor,
          std::move(launchControlRequest),
          std::move(controlMethods));
  return PythonAsyncProcessorFactory::create(
      pythonServer,
      std::move(functions),
      std::move(lifecycleFuncs),
      std::move(executionSystem),
      folly::getKeepAliveToken(controlExecutor),
      "test.Service");
}

struct FactoryIntakeObservations {
  bool intakeIsDistinctFromControl = false;
  bool plainWorkRan = false;
  std::size_t plainWorkDrained = 0;
  bool priorityWorkRan = false;
  std::size_t priorityWorkDrained = 0;
  uint8_t priorityCount = 0;
  std::vector<int8_t> priorities;
  bool controlAliveAfterFactoryRelease = false;
  bool heldWorkRan = false;
  std::size_t heldWorkDrained = 0;
  bool controlReleasedAfterHolderRelease = false;
};

class ObservableControlExecutor final : public folly::Executor {
 public:
  explicit ObservableControlExecutor(std::shared_ptr<bool> destroyed)
      : destroyed_(std::move(destroyed)) {}

  void add(folly::Func func) override { pending_.push_back(std::move(func)); }

  std::size_t drain() {
    auto pending = std::move(pending_);
    for (auto& func : pending) {
      std::move(func)();
    }
    return pending.size();
  }

  void addWithPriority(folly::Func func, int8_t priority) override {
    priorities.push_back(priority);
    add(std::move(func));
  }

  uint8_t getNumPriorities() const override { return 3; }

  bool keepAliveAcquire() noexcept override {
    ++references_;
    return true;
  }

  void keepAliveRelease() noexcept override {
    if (--references_ == 0) {
      delete this;
    }
  }

  std::vector<int8_t> priorities;

 private:
  ~ObservableControlExecutor() override { *destroyed_ = true; }

  std::shared_ptr<bool> destroyed_;
  std::vector<folly::Func> pending_;
  std::size_t references_{0};
};

inline FactoryIntakeObservations observeFactoryIntake() {
  FactoryIntakeObservations observed;
  auto destroyed = std::make_shared<bool>(false);
  auto* control = new ObservableControlExecutor(destroyed);
  auto controlOwner = folly::getKeepAliveToken(control);
  auto factory = PythonAsyncProcessorFactory::create(
      nullptr,
      {},
      {},
      execution::makeControlOnlyExecutionSystem(
          control, [](PyObject*) { return 0; }, {}),
      controlOwner,
      "test.Service");
  auto holder = folly::getKeepAliveToken(factory->intakeExecutor());
  observed.intakeIsDistinctFromControl = holder.get() != control;
  controlOwner.reset();

  holder->add([&] { observed.plainWorkRan = true; });
  observed.plainWorkDrained = control->drain();
  observed.priorityCount = holder->getNumPriorities();
  holder->addWithPriority(
      [&] { observed.priorityWorkRan = true; }, folly::Executor::HI_PRI);
  observed.priorityWorkDrained = control->drain();
  observed.priorities = control->priorities;

  factory->releaseOwnedResources();
  factory.reset();
  observed.controlAliveAfterFactoryRelease = !*destroyed;
  if (!*destroyed) {
    holder->add([&] { observed.heldWorkRan = true; });
    observed.heldWorkDrained = control->drain();
  }
  holder.reset();
  observed.controlReleasedAfterHolderRelease = *destroyed;
  return observed;
}

} // namespace apache::thrift::python::test
