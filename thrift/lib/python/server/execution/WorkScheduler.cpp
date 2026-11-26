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

#include <thrift/lib/python/server/execution/WorkScheduler.h>

#include <folly/executors/InlineExecutor.h>
#include <folly/executors/SerialExecutor.h>
#include <folly/synchronization/Baton.h>

namespace apache::thrift::python::execution {

WorkerCapacityLease::~WorkerCapacityLease() {
  workScheduler_->makeWorkerCapacityAvailable(std::move(worker_));
}

WorkScheduler::WorkScheduler()
    : scheduler_(
          folly::SerialExecutor::create(
              folly::Executor::getKeepAliveToken(
                  folly::InlineExecutor::instance()))) {}

WorkerHandle WorkScheduler::registerWorker(
    folly::Executor::KeepAlive<> executor) {
  WorkerHandle worker(std::make_shared<const WorkerHandle::Identity>());
  workers_.emplace(worker, std::move(executor));
  return worker;
}

void WorkScheduler::unregisterWorker(WorkerHandle worker) {
  // This wait always completes. Only this library calls the scheduler, and its
  // contract rules out both ways the wait could hang:
  // - Queued scheduler work always runs. An idle scheduler runs it on this
  //   thread; a busy one runs it on the thread that already works through its
  //   queue. close() clears registrations but never stops that executor.
  // - The contract forbids calls from inside scheduler work, as for close():
  //   such a call would wait on itself.
  folly::Baton<> unregistered;
  scheduler_->add(
      [self = shared_from_this(), worker = std::move(worker), &unregistered] {
        // Neither erase can throw, so the post below always runs: the handle's
        // hash, comparison, and moves never throw, and erasure never allocates.
        self->workers_.erase(worker);
        std::erase(self->workersWithAvailableCapacity_, worker);
        unregistered.post();
      });
  unregistered.wait();
}

void WorkScheduler::close() noexcept {
  folly::Baton<> closed;
  scheduler_->add([self = shared_from_this(), &closed] {
    self->workersWithAvailableCapacity_.clear();
    self->workers_.clear();
    // The contract guarantees that this cleanup runs only after the server
    // drains, so every request a worker took has already finished, and the
    // normal queue is empty. Any work that remains in a queue belongs to a
    // request that the server already rejected with a queue-timeout error, and
    // no worker remains to run it. Clear both queues: no client still expects
    // this work.
    self->normalPriorityWork_.clear();
    self->lowPriorityWork_.clear();
    closed.post();
  });
  closed.wait();
}

void WorkScheduler::makeWorkerCapacityAvailable(WorkerHandle worker) {
  scheduler_->add(
      [self = shared_from_this(), worker = std::move(worker)]() mutable {
        if (!self->workers_.contains(worker)) {
          return;
        }
        self->workersWithAvailableCapacity_.push_back(std::move(worker));
        self->dispatchNextAvailableWorkItem();
      });
}

void WorkScheduler::addAsynchronousWorkWithPriority(
    AsynchronousWork workItem, int8_t priority) {
  enqueue(std::move(workItem), workPriorityFor(priority));
}

namespace {

// A plain closure becomes a work item that runs it on the item's assigned
// Worker.
WorkScheduler::AsynchronousWork workItemFor(folly::Func closure) {
  return [closure = std::move(closure)](
             const WorkerHandle&,
             const folly::Executor::KeepAlive<>& executor,
             std::shared_ptr<WorkerCapacityLease> lease) mutable {
    // While this worker runs the closure, the scheduler must not assign it
    // more, or new work would queue behind this work instead of reaching an
    // idle worker. The lease holds that capacity. A task on the same worker
    // takes the lease and runs on a later turn, after closure() returns or
    // throws, so the capacity returns only once the work ends.
    executor->add([lease = std::move(lease)] {});
    closure();
  };
}

} // namespace

void WorkScheduler::add(folly::Func closure) {
  addWithPriority(std::move(closure), folly::Executor::MID_PRI);
}

void WorkScheduler::addWithPriority(folly::Func closure, int8_t priority) {
  addAsynchronousWorkWithPriority(workItemFor(std::move(closure)), priority);
}

WorkScheduler::WorkPriority WorkScheduler::workPriorityFor(int8_t priority) {
  return priority == folly::Executor::LO_PRI ? WorkPriority::Low
                                             : WorkPriority::Normal;
}

std::deque<WorkScheduler::AsynchronousWork>& WorkScheduler::selectQueue(
    WorkPriority priority) {
  return priority == WorkPriority::Normal ? normalPriorityWork_
                                          : lowPriorityWork_;
}

void WorkScheduler::enqueue(AsynchronousWork workItem, WorkPriority priority) {
  scheduler_->add([self = shared_from_this(),
                   workItem = std::move(workItem),
                   priority]() mutable {
    self->selectQueue(priority).push_back(std::move(workItem));
    self->dispatchNextAvailableWorkItem();
  });
}

bool WorkScheduler::dispatchOne(std::deque<AsynchronousWork>& queue) {
  if (queue.empty() || workersWithAvailableCapacity_.empty()) {
    return false;
  }

  auto worker = std::move(workersWithAvailableCapacity_.front());
  workersWithAvailableCapacity_.pop_front();
  auto executor = workers_.at(worker);
  auto lease = std::shared_ptr<WorkerCapacityLease>(
      new WorkerCapacityLease(executor, shared_from_this(), worker));
  auto workItem = std::move(queue.front());
  queue.pop_front();
  executor->add([workItem = std::move(workItem),
                 worker,
                 executor,
                 lease = std::move(lease)]() mutable {
    std::move(workItem)(
        std::move(worker), std::move(executor), std::move(lease));
  });
  return true;
}

void WorkScheduler::dispatchNextAvailableWorkItem() {
  if (!dispatchOne(normalPriorityWork_)) {
    dispatchOne(lowPriorityWork_);
  }
}

} // namespace apache::thrift::python::execution
