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

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

#include <folly/coro/Baton.h>
#include <folly/coro/CurrentExecutor.h>
#include <folly/coro/Task.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/ManualExecutor.h>
#include <folly/portability/GTest.h>
#include <folly/synchronization/Baton.h>

namespace apache::thrift::python::execution {
namespace {

class KeepAliveTrackingExecutor final : public folly::Executor {
 public:
  explicit KeepAliveTrackingExecutor(folly::Executor& delegate)
      : delegate_(delegate) {}

  void add(folly::Func callback) override {
    delegate_.add(std::move(callback));
  }

  bool keepAliveAcquire() noexcept override {
    ++keepAliveCount_;
    return true;
  }

  void keepAliveRelease() noexcept override { --keepAliveCount_; }

  std::size_t keepAliveCount() const noexcept { return keepAliveCount_; }

 private:
  folly::Executor& delegate_;
  std::size_t keepAliveCount_{0};
};

// Lets a test freeze the scheduler mid-dispatch: each add() call waits until
// the test releases it.
class AddGateExecutor final : public folly::Executor {
 public:
  explicit AddGateExecutor(folly::Executor& delegate) : delegate_(delegate) {}

  void add(folly::Func callback) override {
    addEntered_.post();
    addReleased_.wait();
    delegate_.add(std::move(callback));
  }

  void waitForAdd() { addEntered_.wait(); }

  void releaseAdd() { addReleased_.post(); }

 private:
  folly::Executor& delegate_;
  folly::Baton<> addEntered_;
  folly::Baton<> addReleased_;
};

class CallbackGateExecutor final : public folly::Executor {
 public:
  CallbackGateExecutor(
      folly::ManualExecutor& eventLoop,
      std::size_t callbacksToPassBeforeGateCloses)
      : eventLoop_(eventLoop),
        callbacksToPassBeforeGateCloses_(callbacksToPassBeforeGateCloses),
        gateClosed_(callbacksToPassBeforeGateCloses == 0) {}

  void add(folly::Func callback) override {
    if (gateClosed_) {
      callbacksBehindGate_.push_back(std::move(callback));
      return;
    }
    eventLoop_.add(std::move(callback));
    if (callbacksToPassBeforeGateCloses_ > 0 &&
        --callbacksToPassBeforeGateCloses_ == 0) {
      gateClosed_ = true;
    }
  }

  void openGateAndForwardCallbacks() {
    gateClosed_ = false;
    auto callbacks = std::exchange(callbacksBehindGate_, {});
    for (auto& callback : callbacks) {
      eventLoop_.add(std::move(callback));
    }
  }

  std::size_t callbackCountBehindGate() const {
    return callbacksBehindGate_.size();
  }

 private:
  folly::ManualExecutor& eventLoop_;
  std::size_t callbacksToPassBeforeGateCloses_;
  bool gateClosed_;
  std::vector<folly::Func> callbacksBehindGate_;
};

} // namespace

namespace {

TEST(WorkSchedulerTest, CloseReleasesRegisteredWorkerExecutorKeepAlive) {
  // GIVEN
  const std::size_t expectedKeepAliveCountBeforeClose = 1;
  const std::size_t expectedKeepAliveCountAfterClose = 0;
  folly::ManualExecutor workerEventLoop;
  KeepAliveTrackingExecutor workerExecutor(workerEventLoop);
  auto workScheduler = std::make_shared<WorkScheduler>();
  workScheduler->registerWorker(
      folly::Executor::getKeepAliveToken(workerExecutor));

  // WHEN
  const auto actualKeepAliveCountBeforeClose = workerExecutor.keepAliveCount();
  workScheduler->close();
  const auto actualKeepAliveCountAfterClose = workerExecutor.keepAliveCount();

  // THEN
  EXPECT_EQ(expectedKeepAliveCountBeforeClose, actualKeepAliveCountBeforeClose);
  EXPECT_EQ(expectedKeepAliveCountAfterClose, actualKeepAliveCountAfterClose);
}

TEST(
    WorkSchedulerTest,
    UnregisterWorkerReturnsOnlyAfterItReleasesTheExecutorKeepAlive) {
  // GIVEN
  const std::size_t expectedKeepAliveCountAfterUnregister = 0;
  // A dispatch to this worker waits in its add(), which freezes the scheduler.
  folly::ManualExecutor busyWorkerEventLoop;
  AddGateExecutor busyWorkerExecutor(busyWorkerEventLoop);
  // Only the registration keeps the idle worker's executor alive, so its count
  // drops to zero exactly when unregistration releases it.
  folly::ManualExecutor idleWorkerEventLoop;
  KeepAliveTrackingExecutor idleWorkerExecutor(idleWorkerEventLoop);
  auto workScheduler = std::make_shared<WorkScheduler>();
  const auto busyWorker = workScheduler->registerWorker(
      folly::Executor::getKeepAliveToken(busyWorkerExecutor));
  const auto idleWorker = workScheduler->registerWorker(
      folly::Executor::getKeepAliveToken(idleWorkerExecutor));
  // Only the busy worker has capacity, so the work below goes to it.
  workScheduler->makeWorkerCapacityAvailable(busyWorker);
  // Make the scheduler busy on another thread. The scheduler has no thread of
  // its own, so this thread, which submits first, runs the dispatch and freezes
  // in the busy worker's add().
  std::thread schedulerThread([&] {
    workScheduler->addAsynchronousWorkWithPriority(
        [](const WorkerHandle&,
           const folly::Executor::KeepAlive<>&,
           std::shared_ptr<WorkerCapacityLease>) {},
        folly::Executor::MID_PRI);
  });
  // Continue only after the scheduler freezes.
  busyWorkerExecutor.waitForAdd();

  // WHEN
  std::size_t actualKeepAliveCountAfterUnregister = 0;
  folly::Baton<> unregisterReturned;
  // The scheduler is busy, so this removal waits behind the frozen dispatch.
  // Record the count the moment unregisterWorker() returns.
  std::thread unregisterThread([&] {
    workScheduler->unregisterWorker(idleWorker);
    actualKeepAliveCountAfterUnregister = idleWorkerExecutor.keepAliveCount();
    unregisterReturned.post();
  });
  // Give an early return time to show up: an unregisterWorker() that returns
  // before its removal runs records a count of 1 here. Correct code blocks
  // until the release, so it passes however long this wait lasts.
  unregisterReturned.try_wait_for(std::chrono::milliseconds(200));
  // Release the frozen dispatch. The scheduler thread then runs the queued
  // removal, which releases the keep-alive and lets unregisterWorker() return.
  busyWorkerExecutor.releaseAdd();
  unregisterThread.join();
  schedulerThread.join();
  // Run the dispatched work so it drops its capacity lease before teardown.
  busyWorkerEventLoop.drain();

  // THEN
  EXPECT_EQ(
      expectedKeepAliveCountAfterUnregister,
      actualKeepAliveCountAfterUnregister);
}

TEST(WorkSchedulerTest, LeaseReleasedAfterCloseDoesNotRestoreRegistration) {
  // GIVEN
  const std::size_t expectedKeepAliveCountWhileLeaseIsActive = 1;
  const std::size_t expectedKeepAliveCountAfterLeaseRelease = 0;
  folly::ManualExecutor workerEventLoop;
  KeepAliveTrackingExecutor workerExecutor(workerEventLoop);
  auto workScheduler = std::make_shared<WorkScheduler>();
  std::shared_ptr<WorkerCapacityLease> activeLease;
  const auto worker = workScheduler->registerWorker(
      folly::Executor::getKeepAliveToken(workerExecutor));
  workScheduler->makeWorkerCapacityAvailable(worker);
  workScheduler->addAsynchronousWorkWithPriority(
      [&](WorkerHandle,
          const folly::Executor::KeepAlive<>&,
          std::shared_ptr<WorkerCapacityLease> lease) {
        activeLease = std::move(lease);
      },
      folly::Executor::MID_PRI);
  workerEventLoop.drain();

  // WHEN
  workScheduler->close();
  const auto actualKeepAliveCountWhileLeaseIsActive =
      workerExecutor.keepAliveCount();
  activeLease.reset();
  const auto actualKeepAliveCountAfterLeaseRelease =
      workerExecutor.keepAliveCount();

  // THEN
  EXPECT_EQ(
      expectedKeepAliveCountWhileLeaseIsActive,
      actualKeepAliveCountWhileLeaseIsActive);
  EXPECT_EQ(
      expectedKeepAliveCountAfterLeaseRelease,
      actualKeepAliveCountAfterLeaseRelease);
}

TEST(WorkSchedulerTest, CloseReleasesWorkThatNoWorkerTook) {
  // GIVEN
  folly::CPUThreadPoolExecutor workerExecutor(1);
  auto workScheduler = std::make_shared<WorkScheduler>();
  const auto worker = workScheduler->registerWorker(
      folly::Executor::getKeepAliveToken(workerExecutor));
  workScheduler->makeWorkerCapacityAvailable(worker);
  folly::Baton<> workerBusy;
  folly::Baton<> releaseWorker;
  workScheduler->addWithPriority(
      [&] {
        workerBusy.post();
        releaseWorker.wait();
      },
      folly::Executor::MID_PRI);
  workerBusy.wait();
  // While the only worker is busy, this item waits for capacity, like the
  // server's low-priority dequeue for a request that already timed out in the
  // server's queue.
  auto pendingWork = std::make_shared<bool>(false);
  std::weak_ptr<bool> pendingWorkObserver = pendingWork;
  workScheduler->addWithPriority(
      [pendingWork = std::move(pendingWork)] { *pendingWork = true; },
      folly::Executor::LO_PRI);

  // WHEN
  workScheduler->close();
  releaseWorker.post();
  workerExecutor.join();

  // THEN
  EXPECT_TRUE(pendingWorkObserver.expired());
}

TEST(
    WorkSchedulerTest,
    CapacityRemainsUnavailableUntilFinalLeaseReferenceRelease) {
  // GIVEN
  folly::ManualExecutor workerExecutor;
  auto workScheduler = std::make_shared<WorkScheduler>();
  bool firstWorkExecuted = false;
  bool secondWorkExecuted = false;
  std::shared_ptr<WorkerCapacityLease> referenceToLease;
  std::shared_ptr<WorkerCapacityLease> additionalReferenceToLease;

  // WHEN
  const auto worker = workScheduler->registerWorker(
      folly::Executor::getKeepAliveToken(workerExecutor));
  workScheduler->makeWorkerCapacityAvailable(worker);
  workScheduler->addAsynchronousWorkWithPriority(
      [&](WorkerHandle,
          const folly::Executor::KeepAlive<>&,
          std::shared_ptr<WorkerCapacityLease> lease) {
        firstWorkExecuted = true;
        referenceToLease = std::move(lease);
        additionalReferenceToLease = referenceToLease;
      },
      folly::Executor::MID_PRI);
  workScheduler->addAsynchronousWorkWithPriority(
      [&](WorkerHandle,
          const folly::Executor::KeepAlive<>&,
          std::shared_ptr<WorkerCapacityLease>) { secondWorkExecuted = true; },
      folly::Executor::MID_PRI);
  workerExecutor.drain();
  const bool firstWorkExecutedAfterDispatch = firstWorkExecuted;
  const bool secondWorkExecutedWhileBothReferencesExist = secondWorkExecuted;
  referenceToLease.reset();
  workerExecutor.drain();
  const bool secondWorkExecutedAfterOneReferenceRelease = secondWorkExecuted;
  additionalReferenceToLease.reset();
  workerExecutor.drain();
  const bool secondWorkExecutedAfterFinalReferenceRelease = secondWorkExecuted;

  // THEN
  EXPECT_TRUE(firstWorkExecutedAfterDispatch);
  EXPECT_FALSE(secondWorkExecutedWhileBothReferencesExist);
  EXPECT_FALSE(secondWorkExecutedAfterOneReferenceRelease);
  EXPECT_TRUE(secondWorkExecutedAfterFinalReferenceRelease);
}

TEST(WorkSchedulerTest, OpaqueExecutorWorkReturnsWorkerAndHonorsPriority) {
  // GIVEN
  folly::ManualExecutor workerExecutor;
  auto workScheduler = std::make_shared<WorkScheduler>();
  std::vector<int> executionOrder;
  const std::vector<int> expected{2, 1};

  // WHEN
  const auto worker = workScheduler->registerWorker(
      folly::Executor::getKeepAliveToken(workerExecutor));
  workScheduler->addWithPriority(
      [&] { executionOrder.push_back(1); }, folly::Executor::LO_PRI);
  workScheduler->add([&] { executionOrder.push_back(2); });
  workScheduler->makeWorkerCapacityAvailable(worker);
  workerExecutor.drain();

  // THEN
  EXPECT_EQ(expected, executionOrder);
}

TEST(WorkSchedulerTest, NormalAsynchronousAndOpaqueWorkPreserveFifoOrder) {
  // GIVEN
  folly::ManualExecutor workerExecutor;
  auto workScheduler = std::make_shared<WorkScheduler>();
  std::vector<int> executionOrder;
  const std::vector<int> expected{1, 2};

  // WHEN
  const auto worker = workScheduler->registerWorker(
      folly::Executor::getKeepAliveToken(workerExecutor));
  workScheduler->addAsynchronousWorkWithPriority(
      [&](WorkerHandle,
          const folly::Executor::KeepAlive<>&,
          std::shared_ptr<WorkerCapacityLease>) {
        executionOrder.push_back(1);
      },
      folly::Executor::MID_PRI);
  workScheduler->add([&] { executionOrder.push_back(2); });
  workScheduler->makeWorkerCapacityAvailable(worker);
  workerExecutor.drain();

  // THEN
  EXPECT_EQ(expected, executionOrder);
}

TEST(WorkSchedulerTest, WorkItemThatThrowsReleasesCapacityForNextWorkItem) {
  // GIVEN
  folly::ManualExecutor workerExecutor;
  auto workScheduler = std::make_shared<WorkScheduler>();
  bool shouldThrow = true;
  bool nextWorkExecuted = false;
  // WHEN
  const auto worker = workScheduler->registerWorker(
      folly::Executor::getKeepAliveToken(workerExecutor));
  workScheduler->add([&] {
    if (shouldThrow) {
      throw std::runtime_error("opaque work failed");
    }
  });
  workScheduler->add([&] { nextWorkExecuted = true; });
  workScheduler->makeWorkerCapacityAvailable(worker);
  bool exceptionEscaped = false;
  try {
    workerExecutor.drain();
  } catch (const std::runtime_error&) {
    exceptionEscaped = true;
  }
  workerExecutor.drain();
  const bool nextWorkItemExecutedAfterCapacityRelease = nextWorkExecuted;

  // THEN
  EXPECT_TRUE(exceptionEscaped);
  EXPECT_TRUE(nextWorkItemExecutedAfterCapacityRelease);
}

// This test earns its value only under ThreadSanitizer. A plain run passed even
// while scheduler state raced, so run:
//   buck2 test @fbcode//mode/dev-tsan \
//     fbcode//thrift/lib/python/server/execution/test:work_scheduler_test
TEST(WorkSchedulerTest, ConcurrentCapacityReturnAndGeneralWorkMakeProgress) {
  // GIVEN
  folly::CPUThreadPoolExecutor workerExecutor(1);
  auto workScheduler = std::make_shared<WorkScheduler>();
  folly::Baton<> workerStarted;
  folly::Baton<> startConcurrentCalls;
  folly::Baton<> capacityReturnFinished;
  folly::Baton<> generalWorkFinished;

  // WHEN
  const auto worker = workScheduler->registerWorker(
      folly::Executor::getKeepAliveToken(workerExecutor));
  workScheduler->makeWorkerCapacityAvailable(worker);
  workScheduler->addAsynchronousWorkWithPriority(
      [&](WorkerHandle,
          const folly::Executor::KeepAlive<>&,
          std::shared_ptr<WorkerCapacityLease> execution) {
        workerStarted.post();
        startConcurrentCalls.wait();
        execution.reset();
        capacityReturnFinished.post();
      },
      folly::Executor::MID_PRI);
  const bool workerStartedBeforeTimeout =
      workerStarted.try_wait_for(std::chrono::seconds(5));
  startConcurrentCalls.post();
  workScheduler->addAsynchronousWorkWithPriority(
      [&](WorkerHandle,
          const folly::Executor::KeepAlive<>&,
          std::shared_ptr<WorkerCapacityLease>) { generalWorkFinished.post(); },
      folly::Executor::MID_PRI);
  const bool capacityReturnFinishedBeforeTimeout =
      capacityReturnFinished.try_wait_for(std::chrono::seconds(5));
  const bool generalWorkFinishedBeforeTimeout =
      generalWorkFinished.try_wait_for(std::chrono::seconds(5));

  // THEN
  EXPECT_TRUE(workerStartedBeforeTimeout);
  EXPECT_TRUE(capacityReturnFinishedBeforeTimeout);
  EXPECT_TRUE(generalWorkFinishedBeforeTimeout);
}

TEST(WorkSchedulerTest, YieldedRequestContinuesAfterNextRequestRuns) {
  // GIVEN
  enum class RequestEvent {
    Request1Started,
    Request2Ran,
    Request1Continued,
  };
  const std::vector<RequestEvent> expectedEvents{
      RequestEvent::Request1Started,
      RequestEvent::Request2Ran,
      RequestEvent::Request1Continued,
  };
  folly::ManualExecutor workerExecutor;
  auto workScheduler = std::make_shared<WorkScheduler>();
  folly::coro::Baton request1MayContinue;
  std::vector<RequestEvent> actualEvents;
  std::optional<folly::Try<void>> request1Result;
  auto request1 = [&]() -> folly::coro::Task<void> {
    actualEvents.push_back(RequestEvent::Request1Started);
    co_await request1MayContinue;
    actualEvents.push_back(RequestEvent::Request1Continued);
  };
  const auto worker = workScheduler->registerWorker(
      folly::Executor::getKeepAliveToken(workerExecutor));
  workScheduler->add([&] {
    folly::coro::co_withExecutor(&workerExecutor, request1())
        .startInlineUnsafe([&](folly::Try<void>&& result) {
          request1Result.emplace(std::move(result));
        });
  });
  workScheduler->add([&] {
    actualEvents.push_back(RequestEvent::Request2Ran);
    request1MayContinue.post();
  });

  // WHEN
  workScheduler->makeWorkerCapacityAvailable(worker);
  workerExecutor.drain();
  workScheduler->close();

  // THEN
  EXPECT_EQ(expectedEvents, actualEvents);
  ASSERT_TRUE(request1Result.has_value());
  EXPECT_NO_THROW(request1Result->value());
}

TEST(
    WorkSchedulerTest,
    AssignedWorkOwnsWorkerCapacityLeaseBeforeEventLoopEnqueue) {
  // GIVEN
  folly::ManualExecutor workerEventLoop;
  CallbackGateExecutor workerExecutor(
      workerEventLoop, /* callbacksToPassBeforeGateCloses */ 2);
  auto workScheduler = std::make_shared<WorkScheduler>();
  const std::size_t expectedSingleStepCount = 1;
  const std::size_t expectedCallbackCountBehindGate = 1;
  const std::size_t expectedCallbackCountBehindGateAfterDrain = 0;
  bool request1Started = false;
  bool request1SecondContinuationReached = false;
  bool request1ThirdContinuationReached = false;
  bool request2Executed = false;
  bool request3Executed = false;
  std::optional<std::size_t> callbackCountBehindGateBeforeThirdContinuation;
  std::optional<folly::Try<void>> request1Result;
  auto request1 = [&]() -> folly::coro::Task<void> {
    request1Started = true;
    co_await folly::coro::co_reschedule_on_current_executor;
    request1SecondContinuationReached = true;
    co_await folly::coro::co_reschedule_on_current_executor;
    callbackCountBehindGateBeforeThirdContinuation =
        workerExecutor.callbackCountBehindGate();
    request1ThirdContinuationReached = true;
  };
  const auto worker = workScheduler->registerWorker(
      folly::Executor::getKeepAliveToken(workerExecutor));
  workScheduler->add([&] {
    folly::coro::co_withExecutor(&workerEventLoop, request1())
        .startInlineUnsafe([&](folly::Try<void>&& result) {
          request1Result.emplace(std::move(result));
        });
  });
  workScheduler->add([&] { request2Executed = true; });

  // WHEN
  workScheduler->makeWorkerCapacityAvailable(worker);
  const auto callbackCountForRequest1FirstTurn = workerEventLoop.step();
  const auto callbackCountForRequest1CapacityReturn = workerEventLoop.step();
  const auto callbackCountBehindGateAfterRequest2Assignment =
      workerExecutor.callbackCountBehindGate();
  const bool request2ExecutedBeforeEventLoopEnqueue = request2Executed;
  // Request 2 already owns the Worker capacity while its callback waits behind
  // the gate, so request 3 must remain in the scheduler queue. Request 1's
  // existing coroutine continuations must remain free to run on the Worker
  // event loop.
  workScheduler->add([&] { request3Executed = true; });
  const auto callbackCountBehindGateAfterRequest3Accepted =
      workerExecutor.callbackCountBehindGate();
  const bool request3ExecutedWhilePending = request3Executed;
  const auto callbackCountForRequest1SecondContinuation =
      workerEventLoop.step();
  const auto callbackCountForRequest1ThirdContinuation = workerEventLoop.step();
  workerExecutor.openGateAndForwardCallbacks();
  workerEventLoop.drain();
  workScheduler->close();
  workerEventLoop.drain();
  const auto callbackCountBehindGateAfterDrain =
      workerExecutor.callbackCountBehindGate();

  // THEN
  EXPECT_EQ(expectedSingleStepCount, callbackCountForRequest1FirstTurn);
  EXPECT_TRUE(request1Started);
  EXPECT_EQ(expectedSingleStepCount, callbackCountForRequest1CapacityReturn);
  EXPECT_EQ(
      expectedCallbackCountBehindGate,
      callbackCountBehindGateAfterRequest2Assignment);
  EXPECT_FALSE(request2ExecutedBeforeEventLoopEnqueue);
  EXPECT_EQ(
      expectedCallbackCountBehindGate,
      callbackCountBehindGateAfterRequest3Accepted);
  EXPECT_FALSE(request3ExecutedWhilePending);
  EXPECT_EQ(
      expectedSingleStepCount, callbackCountForRequest1SecondContinuation);
  EXPECT_TRUE(request1SecondContinuationReached);
  EXPECT_EQ(expectedSingleStepCount, callbackCountForRequest1ThirdContinuation);
  ASSERT_TRUE(callbackCountBehindGateBeforeThirdContinuation.has_value());
  EXPECT_EQ(
      expectedCallbackCountBehindGate,
      *callbackCountBehindGateBeforeThirdContinuation);
  EXPECT_TRUE(request1ThirdContinuationReached);
  ASSERT_TRUE(request1Result.has_value());
  EXPECT_NO_THROW(request1Result->value());
  EXPECT_TRUE(request2Executed);
  EXPECT_TRUE(request3Executed);
  EXPECT_EQ(
      expectedCallbackCountBehindGateAfterDrain,
      callbackCountBehindGateAfterDrain);
}

TEST(WorkSchedulerTest, TwoWorkersRunGeneralWorkConcurrently) {
  // GIVEN
  enum class WorkEvent {
    FirstWorkEntered,
    SecondWorkEntered,
    FirstWorkExited,
    SecondWorkExited,
  };
  const int expectedEventCount = 1;
  folly::CPUThreadPoolExecutor firstExecutor(1);
  folly::CPUThreadPoolExecutor secondExecutor(1);
  auto workScheduler = std::make_shared<WorkScheduler>();
  std::mutex workEventsMutex;
  std::vector<WorkEvent> workEvents;
  folly::Baton<> firstWorkEntered;
  folly::Baton<> secondWorkEntered;
  folly::Baton<> firstWorkFinished;
  folly::Baton<> secondWorkFinished;
  bool firstWorkObservedSecondEntry = false;
  bool secondWorkObservedFirstEntry = false;
  const auto recordWorkEvent = [&](WorkEvent event) {
    std::lock_guard<std::mutex> lock(workEventsMutex);
    workEvents.push_back(event);
  };

  // WHEN
  const auto firstWorker = workScheduler->registerWorker(
      folly::Executor::getKeepAliveToken(firstExecutor));
  const auto secondWorker = workScheduler->registerWorker(
      folly::Executor::getKeepAliveToken(secondExecutor));
  workScheduler->makeWorkerCapacityAvailable(firstWorker);
  workScheduler->makeWorkerCapacityAvailable(secondWorker);
  workScheduler->addAsynchronousWorkWithPriority(
      [&](WorkerHandle,
          const folly::Executor::KeepAlive<>&,
          std::shared_ptr<WorkerCapacityLease>) {
        recordWorkEvent(WorkEvent::FirstWorkEntered);
        firstWorkEntered.post();
        firstWorkObservedSecondEntry =
            secondWorkEntered.try_wait_for(std::chrono::seconds(5));
        recordWorkEvent(WorkEvent::FirstWorkExited);
        firstWorkFinished.post();
      },
      folly::Executor::MID_PRI);
  workScheduler->addAsynchronousWorkWithPriority(
      [&](WorkerHandle,
          const folly::Executor::KeepAlive<>&,
          std::shared_ptr<WorkerCapacityLease>) {
        recordWorkEvent(WorkEvent::SecondWorkEntered);
        secondWorkEntered.post();
        secondWorkObservedFirstEntry =
            firstWorkEntered.try_wait_for(std::chrono::seconds(5));
        recordWorkEvent(WorkEvent::SecondWorkExited);
        secondWorkFinished.post();
      },
      folly::Executor::MID_PRI);
  const bool firstWorkFinishedBeforeTimeout =
      firstWorkFinished.try_wait_for(std::chrono::seconds(10));
  const bool secondWorkFinishedBeforeTimeout =
      secondWorkFinished.try_wait_for(std::chrono::seconds(10));
  std::vector<WorkEvent> actualWorkEvents;
  {
    std::lock_guard<std::mutex> lock(workEventsMutex);
    actualWorkEvents = workEvents;
  }

  // THEN
  EXPECT_TRUE(firstWorkObservedSecondEntry);
  EXPECT_TRUE(secondWorkObservedFirstEntry);
  EXPECT_TRUE(firstWorkFinishedBeforeTimeout);
  EXPECT_TRUE(secondWorkFinishedBeforeTimeout);
  ASSERT_EQ(
      expectedEventCount,
      std::count(
          actualWorkEvents.begin(),
          actualWorkEvents.end(),
          WorkEvent::FirstWorkEntered));
  ASSERT_EQ(
      expectedEventCount,
      std::count(
          actualWorkEvents.begin(),
          actualWorkEvents.end(),
          WorkEvent::SecondWorkEntered));
  ASSERT_EQ(
      expectedEventCount,
      std::count(
          actualWorkEvents.begin(),
          actualWorkEvents.end(),
          WorkEvent::FirstWorkExited));
  ASSERT_EQ(
      expectedEventCount,
      std::count(
          actualWorkEvents.begin(),
          actualWorkEvents.end(),
          WorkEvent::SecondWorkExited));
  const auto firstWorkEntry = std::find(
      actualWorkEvents.begin(),
      actualWorkEvents.end(),
      WorkEvent::FirstWorkEntered);
  const auto secondWorkEntry = std::find(
      actualWorkEvents.begin(),
      actualWorkEvents.end(),
      WorkEvent::SecondWorkEntered);
  const auto firstWorkExit = std::find(
      actualWorkEvents.begin(),
      actualWorkEvents.end(),
      WorkEvent::FirstWorkExited);
  const auto secondWorkExit = std::find(
      actualWorkEvents.begin(),
      actualWorkEvents.end(),
      WorkEvent::SecondWorkExited);
  EXPECT_LT(firstWorkEntry, firstWorkExit);
  EXPECT_LT(firstWorkEntry, secondWorkExit);
  EXPECT_LT(secondWorkEntry, firstWorkExit);
  EXPECT_LT(secondWorkEntry, secondWorkExit);
}

TEST(WorkSchedulerTest, AssignedWorkReceivesItsWorkersExecutor) {
  // GIVEN
  folly::ManualExecutor firstWorkerExecutor;
  folly::ManualExecutor secondWorkerExecutor;
  auto workScheduler = std::make_shared<WorkScheduler>();
  workScheduler->registerWorker(
      folly::Executor::getKeepAliveToken(firstWorkerExecutor));
  const auto secondWorker = workScheduler->registerWorker(
      folly::Executor::getKeepAliveToken(secondWorkerExecutor));
  // Only the second worker has capacity, so the work below goes to it.
  workScheduler->makeWorkerCapacityAvailable(secondWorker);
  folly::Executor* const expectedExecutor = &secondWorkerExecutor;
  folly::Executor* actualExecutor = nullptr;

  // WHEN
  workScheduler->addAsynchronousWorkWithPriority(
      [&](const WorkerHandle&,
          const folly::Executor::KeepAlive<>& executor,
          std::shared_ptr<WorkerCapacityLease>) {
        actualExecutor = executor.get();
      },
      folly::Executor::MID_PRI);
  secondWorkerExecutor.drain();
  workScheduler->close();

  // THEN
  EXPECT_EQ(expectedExecutor, actualExecutor);
}

TEST(WorkSchedulerTest, ServesAsFollyExecutor) {
  // GIVEN
  folly::ManualExecutor workerExecutor;
  auto workScheduler = std::make_shared<WorkScheduler>();
  const auto worker = workScheduler->registerWorker(
      folly::Executor::getKeepAliveToken(workerExecutor));
  workScheduler->makeWorkerCapacityAvailable(worker);
  std::shared_ptr<folly::Executor> executor = workScheduler;
  const std::uint8_t expectedNumberOfPriorities = 2;
  bool ran = false;

  // WHEN
  const auto actualNumberOfPriorities = executor->getNumPriorities();
  executor->addWithPriority([&] { ran = true; }, folly::Executor::LO_PRI);
  workerExecutor.drain();

  // THEN
  EXPECT_EQ(expectedNumberOfPriorities, actualNumberOfPriorities);
  EXPECT_TRUE(ran);
}

} // namespace
} // namespace apache::thrift::python::execution
