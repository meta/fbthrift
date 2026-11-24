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

#include <cstddef>
#include <deque>
#include <memory>
#include <unordered_map>
#include <utility>

#include <folly/Executor.h>

namespace apache::thrift::python::execution {

// See .llms/rules/work_scheduler.md for scheduler and lease contracts.
class WorkScheduler;

class WorkerHandle final {
 public:
  bool operator==(const WorkerHandle&) const = default;

  struct Hash final {
    std::size_t operator()(const WorkerHandle& worker) const {
      return std::hash<const Identity*>{}(worker.identity_.get());
    }
  };

 private:
  struct Identity final {};

  explicit WorkerHandle(std::shared_ptr<const Identity> identity)
      : identity_(std::move(identity)) {}

  std::shared_ptr<const Identity> identity_;

  friend class WorkScheduler;
};

class WorkerCapacityLease final {
 public:
  ~WorkerCapacityLease();
  WorkerCapacityLease(const WorkerCapacityLease&) = delete;
  WorkerCapacityLease& operator=(const WorkerCapacityLease&) = delete;
  WorkerCapacityLease(WorkerCapacityLease&&) = delete;
  WorkerCapacityLease& operator=(WorkerCapacityLease&&) = delete;

 private:
  WorkerCapacityLease(
      folly::Executor::KeepAlive<> executor,
      std::shared_ptr<WorkScheduler> workScheduler,
      WorkerHandle worker)
      : executor_(std::move(executor)),
        workScheduler_(std::move(workScheduler)),
        worker_(std::move(worker)) {}

  folly::Executor::KeepAlive<> executor_;
  std::shared_ptr<WorkScheduler> workScheduler_;
  WorkerHandle worker_;

  friend class WorkScheduler;
};

// Owned through std::shared_ptr; create it only with std::make_shared. Every
// operation, including close(), takes a share of that ownership through
// shared_from_this(). In-flight scheduler work and leases share ownership too,
// so the scheduler stays alive until all of them finish.
//
// A plain folly::Executor, so an executor facade can wrap it: keep-alive tokens
// on it do not keep it alive, and its destruction never waits for them.
class WorkScheduler final : public folly::Executor,
                            public std::enable_shared_from_this<WorkScheduler> {
 public:
  WorkScheduler();
  ~WorkScheduler() override = default;
  WorkScheduler(const WorkScheduler&) = delete;
  WorkScheduler& operator=(const WorkScheduler&) = delete;
  WorkScheduler(WorkScheduler&&) = delete;
  WorkScheduler& operator=(WorkScheduler&&) = delete;

  WorkerHandle registerWorker(folly::Executor::KeepAlive<> executor);
  // Returns only after the registration, including its executor keep-alive,
  // is released. Call only from outside scheduler work.
  void unregisterWorker(WorkerHandle worker);
  // Discards work in the queues that no worker has taken. Call only from
  // outside scheduler work.
  void close() noexcept;

  using AsynchronousWork = folly::Function<void(
      WorkerHandle,
      folly::Executor::KeepAlive<>,
      std::shared_ptr<WorkerCapacityLease>)>;

  void makeWorkerCapacityAvailable(WorkerHandle worker);
  void addAsynchronousWorkWithPriority(
      AsynchronousWork workItem, int8_t priority);

  void add(folly::Func workItem) override;
  void addWithPriority(folly::Func workItem, int8_t priority) override;
  uint8_t getNumPriorities() const override { return 2; }

 private:
  using WorkerRegistry = std::unordered_map<
      WorkerHandle,
      folly::Executor::KeepAlive<>,
      WorkerHandle::Hash>;

  enum class WorkPriority {
    Normal,
    Low,
  };

  static WorkPriority workPriorityFor(int8_t priority);
  std::deque<AsynchronousWork>& selectQueue(WorkPriority priority);
  void enqueue(AsynchronousWork workItem, WorkPriority priority);
  bool dispatchOne(std::deque<AsynchronousWork>& queue);
  void dispatchNextAvailableWorkItem();

  WorkerRegistry workers_;
  std::deque<WorkerHandle> workersWithAvailableCapacity_;
  std::deque<AsynchronousWork> normalPriorityWork_;
  std::deque<AsynchronousWork> lowPriorityWork_;
  // Serializes work-queue updates with Worker-capacity changes.
  folly::Executor::KeepAlive<> scheduler_;
};

// Convenience for callers holding only a raw executor. The executor must
// outlive registration; prefer the KeepAlive form when one is available.
// Returns shared ownership since the handle has no default constructor.
inline std::shared_ptr<WorkerHandle> registerWorkerExecutor(
    WorkScheduler& workScheduler, folly::Executor* executor) {
  return std::make_shared<WorkerHandle>(workScheduler.registerWorker(
      folly::Executor::getKeepAliveToken(executor)));
}

} // namespace apache::thrift::python::execution
