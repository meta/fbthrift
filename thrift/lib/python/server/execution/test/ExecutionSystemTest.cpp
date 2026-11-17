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

#include <thrift/lib/python/server/execution/ExecutionSystem.h>

#include <array>
#include <cstddef>
#include <map>
#include <memory>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <folly/Try.h>
#include <folly/executors/ManualExecutor.h>
#include <folly/futures/Future.h>
#include <folly/io/async/Request.h>
#include <folly/portability/GTest.h>
#include <thrift/lib/python/server/execution/WorkScheduler.h>

namespace apache::thrift::python::execution {
namespace {

class ResponseCallback final {
 public:
  void complete(folly::Try<int> result) {
    response_ = std::move(result).value();
  }

  const std::optional<int>& response() const { return response_; }

 private:
  std::optional<int> response_;
};

struct DispatchFailureTrace {
  bool exceptionEscaped;
  bool nextWorkExecuted;

  bool operator==(const DispatchFailureTrace&) const = default;

  friend void PrintTo(const DispatchFailureTrace& trace, std::ostream* out) {
    *out << "{exceptionEscaped: " << trace.exceptionEscaped
         << ", nextWorkExecuted: " << trace.nextWorkExecuted << '}';
  }
};

struct SynchronousDispatchFailureTrace {
  bool exceptionEscaped;
  bool nextWorkExecuted;
  bool failingDispatchExecuted;

  bool operator==(const SynchronousDispatchFailureTrace&) const = default;

  friend void PrintTo(
      const SynchronousDispatchFailureTrace& trace, std::ostream* out) {
    *out << "{exceptionEscaped: " << trace.exceptionEscaped
         << ", nextWorkExecuted: " << trace.nextWorkExecuted
         << ", failingDispatchExecuted: " << trace.failingDispatchExecuted
         << '}';
  }
};

struct RequestEvent {
  enum class Kind {
    BeforeDispatchDrain,
    Dispatch,
    RequestStart,
    AfterDispatchDrain,
    HandlerCompletion,
    BeforeCompletionDrain,
    Response,
    AfterCompletionDrain,
  };

  Kind kind;
  std::variant<std::monostate, PyObject*, int> subject;

  bool operator==(const RequestEvent&) const = default;

  friend void PrintTo(const RequestEvent& event, std::ostream* out) {
    static constexpr std::array names{
        "before dispatch drain",
        "dispatch",
        "start request",
        "after dispatch drain",
        "complete handler",
        "before completion drain",
        "respond",
        "after completion drain"};
    *out << names[static_cast<std::size_t>(event.kind)] << '('
         << ::testing::PrintToString(event.subject) << ')';
  }
};

TEST(ExecutionSystemTest, ExecuteStartsSelectedRequestOnExecutor) {
  // GIVEN
  std::vector<RequestEvent> events;
  folly::ManualExecutor selectedExecutor;
  const std::string methodName = "selected_method";
  auto* const selectedHandlerFunction = reinterpret_cast<PyObject*>(42);
  auto executionSystem = makeControlOnlyExecutionSystem(
      &selectedExecutor,
      [&events](PyObject* coroutineFactory) {
        events.push_back({RequestEvent::Kind::RequestStart, coroutineFactory});
        return 0;
      },
      {{methodName, selectedHandlerFunction}});
  auto [handlerPromise, handlerResultFuture] =
      folly::makePromiseContract<int>();
  auto* const coroutineFactory = reinterpret_cast<PyObject*>(84);
  const std::vector<RequestEvent> expected{
      {RequestEvent::Kind::BeforeDispatchDrain, std::monostate{}},
      {RequestEvent::Kind::Dispatch, selectedHandlerFunction},
      {RequestEvent::Kind::RequestStart, coroutineFactory},
      {RequestEvent::Kind::AfterDispatchDrain, std::monostate{}},
      {RequestEvent::Kind::HandlerCompletion, std::monostate{}},
      {RequestEvent::Kind::BeforeCompletionDrain, std::monostate{}},
      {RequestEvent::Kind::Response, 42},
      {RequestEvent::Kind::AfterCompletionDrain, std::monostate{}},
  };

  // WHEN
  executionSystem->execute(
      methodName,
      RequestDispatch(
          folly::Executor::KeepAlive<>{},
          [pendingHandlerFuture = std::move(handlerResultFuture),
           &events,
           coroutineFactory = coroutineFactory](
              PyObject* selectedFunction,
              const folly::Executor::KeepAlive<>&,
              LaunchRequest launchRequest) mutable {
            events.push_back({RequestEvent::Kind::Dispatch, selectedFunction});
            std::move(launchRequest)(coroutineFactory);
            return std::move(pendingHandlerFuture)
                .defer([&events](auto&& result) {
                  events.push_back(
                      {RequestEvent::Kind::Response,
                       std::move(result).value()});
                });
          }));
  events.push_back({RequestEvent::Kind::BeforeDispatchDrain, std::monostate{}});
  selectedExecutor.drain();
  events.push_back({RequestEvent::Kind::AfterDispatchDrain, std::monostate{}});
  events.push_back({RequestEvent::Kind::HandlerCompletion, std::monostate{}});
  handlerPromise.setValue(42);
  events.push_back(
      {RequestEvent::Kind::BeforeCompletionDrain, std::monostate{}});
  selectedExecutor.drain();
  events.push_back(
      {RequestEvent::Kind::AfterCompletionDrain, std::monostate{}});

  // THEN
  EXPECT_EQ(expected, events);
}

TEST(ExecutionSystemTest, AcceptedWorkCompletesAfterShutdown) {
  // GIVEN
  folly::ManualExecutor controlExecutor;
  const std::string methodName{"selected_method"};
  auto* const dummySelectedHandlerFunction = reinterpret_cast<PyObject*>(42);
  auto executionSystem = makeControlOnlyExecutionSystem(
      &controlExecutor,
      [](PyObject*) { return 0; },
      {{methodName, dummySelectedHandlerFunction}});
  auto [handlerPromise, handlerResultFuture] =
      folly::makePromiseContract<int>();
  ResponseCallback responseCallback;
  const std::optional<int> expected = 42;

  // WHEN
  executionSystem->execute(
      methodName,
      RequestDispatch(
          folly::Executor::KeepAlive<>{},
          [pendingHandlerFuture = std::move(handlerResultFuture),
           &responseCallback](
              PyObject*,
              const folly::Executor::KeepAlive<>&,
              LaunchRequest) mutable {
            return std::move(pendingHandlerFuture)
                .defer([&responseCallback](auto&& result) {
                  responseCallback.complete(std::move(result));
                });
          }));
  executionSystem->shutdown();
  // The first drain starts the request. The handler result queues the callback,
  // and the second drain runs it.
  controlExecutor.drain();
  handlerPromise.setValue(42);
  controlExecutor.drain();
  const auto actual = responseCallback.response();

  // THEN
  EXPECT_EQ(expected, actual);
}

TEST(
    ExecutionSystemTest,
    AsynchronousDispatchFailureDoesNotEscapeControlExecutor) {
  // GIVEN
  folly::ManualExecutor controlExecutor;
  const std::string methodName{"selected_method"};
  auto* const dummySelectedHandlerFunction = reinterpret_cast<PyObject*>(42);
  auto executionSystem = makeControlOnlyExecutionSystem(
      &controlExecutor,
      [](PyObject*) { return 0; },
      {{methodName, dummySelectedHandlerFunction}});
  const DispatchFailureTrace expected{
      .exceptionEscaped = false,
      .nextWorkExecuted = true,
  };

  // WHEN
  executionSystem->execute(
      methodName,
      RequestDispatch(
          folly::Executor::KeepAlive<>{},
          [](PyObject*, const folly::Executor::KeepAlive<>&, LaunchRequest) {
            return folly::makeSemiFuture<folly::Unit>(
                folly::make_exception_wrapper<std::runtime_error>(
                    "request dispatch test failure"));
          }));
  bool nextWorkExecuted = false;
  executionSystem->execute(
      methodName,
      RequestDispatch(
          folly::Executor::KeepAlive<>{},
          [&nextWorkExecuted](
              PyObject*, const folly::Executor::KeepAlive<>&, LaunchRequest) {
            nextWorkExecuted = true;
            return folly::makeSemiFuture();
          }));
  bool exceptionEscaped = false;
  try {
    controlExecutor.drain();
  } catch (const std::runtime_error&) {
    exceptionEscaped = true;
  }
  const DispatchFailureTrace actual{
      .exceptionEscaped = exceptionEscaped,
      .nextWorkExecuted = nextWorkExecuted,
  };

  // THEN
  EXPECT_EQ(expected, actual);
}

TEST(ExecutionSystemTest, SynchronousDispatchFailureDoesNotEscape) {
  // GIVEN
  folly::ManualExecutor controlExecutor;
  const std::string methodName{"selected_method"};
  auto* const dummySelectedHandlerFunction = reinterpret_cast<PyObject*>(42);
  auto executionSystem = makeControlOnlyExecutionSystem(
      &controlExecutor,
      [](PyObject*) { return 0; },
      {{methodName, dummySelectedHandlerFunction}});
  bool shouldThrow = true;
  const SynchronousDispatchFailureTrace expected{
      .exceptionEscaped = false,
      .nextWorkExecuted = true,
      .failingDispatchExecuted = true,
  };

  // WHEN
  executionSystem->execute(
      methodName,
      RequestDispatch(
          folly::Executor::KeepAlive<>{},

          [&shouldThrow](
              PyObject*, const folly::Executor::KeepAlive<>&, LaunchRequest)
              -> folly::SemiFuture<folly::Unit> {
            if (std::exchange(shouldThrow, false)) {
              throw std::runtime_error(
                  "synchronous request dispatch test failure");
            }
            return folly::makeSemiFuture();
          }));
  bool nextWorkExecuted = false;
  executionSystem->execute(
      methodName,
      RequestDispatch(
          folly::Executor::KeepAlive<>{},
          [&nextWorkExecuted](
              PyObject*, const folly::Executor::KeepAlive<>&, LaunchRequest) {
            nextWorkExecuted = true;
            return folly::makeSemiFuture();
          }));
  bool exceptionEscaped = false;
  try {
    controlExecutor.drain();
  } catch (const std::runtime_error&) {
    exceptionEscaped = true;
  }
  const SynchronousDispatchFailureTrace actual{
      .exceptionEscaped = exceptionEscaped,
      .nextWorkExecuted = nextWorkExecuted,
      .failingDispatchExecuted = !shouldThrow,
  };

  // THEN
  EXPECT_EQ(expected, actual);
}

// Lets a test see where its work runs: runs queued work only when drained,
// like ManualExecutor, and reports itself in a shared tracker while
// each callback runs.
class ExecutorThatReportsItself final : public folly::Executor {
 public:
  explicit ExecutorThatReportsItself(
      const folly::Executor*& currentExecutorTracker)
      : currentExecutorTracker_(currentExecutorTracker) {}

  void add(folly::Func callback) override {
    queue_.add([this, callback = std::move(callback)]() mutable {
      currentExecutorTracker_ = this;
      callback();
      currentExecutorTracker_ = nullptr;
    });
  }

  void drain() { queue_.drain(); }

 private:
  const folly::Executor*& currentExecutorTracker_;
  folly::ManualExecutor queue_;
};

enum class Launcher { Control, Worker };

void PrintTo(Launcher launcher, std::ostream* out) {
  *out << (launcher == Launcher::Control ? "Control" : "Worker");
}

struct PlacedRequest {
  const folly::Executor* ranOn;
  PyObject* selectedFunction;
  std::optional<Launcher> launcher;
  bool leaseHeld;

  bool operator==(const PlacedRequest&) const = default;

  friend void PrintTo(const PlacedRequest& request, std::ostream* out) {
    *out << "{ranOn: " << request.ranOn
         << ", selectedFunction: " << request.selectedFunction
         << ", launcher: " << ::testing::PrintToString(request.launcher)
         << ", leaseHeld: " << request.leaseHeld << '}';
  }
};

TEST(
    ExecutionSystemTest, WorkerCapableMethodsStartOnWorkersAndOthersOnControl) {
  // GIVEN
  const folly::Executor* currentExecutorTracker = nullptr;
  ExecutorThatReportsItself controlExecutor(currentExecutorTracker);
  ExecutorThatReportsItself workerExecutor(currentExecutorTracker);
  const std::string workerCapableMethod{"worker_capable_method"};
  const std::string controlMethod{"control_method"};
  // Each handler has a function for both methods, so a wrong route selects a
  // visibly wrong function.
  struct HandlerFunctions {
    PyObject* workerCapableMethod;
    PyObject* controlMethod;
  };
  const HandlerFunctions controlHandler{
      reinterpret_cast<PyObject*>(41), reinterpret_cast<PyObject*>(42)};
  const HandlerFunctions workerHandler{
      reinterpret_cast<PyObject*>(43), reinterpret_cast<PyObject*>(44)};
  auto* const coroutineFactory = reinterpret_cast<PyObject*>(84);
  std::vector<PlacedRequest> placedRequests;
  auto workScheduler = std::make_shared<WorkScheduler>();
  const auto worker = workScheduler->registerWorker(
      folly::Executor::getKeepAliveToken(workerExecutor));
  auto executionSystem = makeWorkerCapableExecutionSystem(
      &controlExecutor,
      [&placedRequests](PyObject*) {
        placedRequests.back().launcher = Launcher::Control;
        return 0;
      },
      {{workerCapableMethod, controlHandler.workerCapableMethod},
       {controlMethod, controlHandler.controlMethod}},
      workScheduler,
      [&placedRequests](PyObject*, std::shared_ptr<WorkerCapacityLease> lease) {
        placedRequests.back().launcher = Launcher::Worker;
        placedRequests.back().leaseHeld = lease != nullptr;
        return 0;
      },
      {workerCapableMethod});
  executionSystem->bindWorkerMethods(
      worker,
      {{workerCapableMethod, workerHandler.workerCapableMethod},
       {controlMethod, workerHandler.controlMethod}});
  workScheduler->makeWorkerCapacityAvailable(worker);
  const auto recordDispatch = [&currentExecutorTracker,
                               &placedRequests,
                               coroutineFactory]() {
    return RequestDispatch(
        folly::Executor::KeepAlive<>{},

        [&currentExecutorTracker, &placedRequests, coroutineFactory](
            PyObject* selectedFunction,
            const folly::Executor::KeepAlive<>&,
            LaunchRequest launchRequest) {
          placedRequests.push_back(
              {currentExecutorTracker, selectedFunction, std::nullopt, false});
          std::move(launchRequest)(coroutineFactory);
          return folly::makeSemiFuture();
        });
  };
  const std::vector<PlacedRequest> expected{
      {&controlExecutor,
       controlHandler.controlMethod,
       Launcher::Control,
       false},
      {&workerExecutor,
       workerHandler.workerCapableMethod,
       Launcher::Worker,
       true},
  };

  // WHEN
  executionSystem->execute(workerCapableMethod, recordDispatch());
  executionSystem->execute(controlMethod, recordDispatch());
  controlExecutor.drain();
  workerExecutor.drain();
  workScheduler->close();

  // THEN
  EXPECT_EQ(expected, placedRequests);
}

TEST(
    ExecutionSystemTest,
    WorkerCapableMethodsSendFollowUpWorkToWorkersAndOthersKeepTheirExecutor) {
  // GIVEN
  const folly::Executor* currentExecutorTracker = nullptr;
  ExecutorThatReportsItself controlExecutor(currentExecutorTracker);
  ExecutorThatReportsItself workerExecutor(currentExecutorTracker);
  folly::ManualExecutor serverRequestExecutor;
  const std::string workerCapableMethod{"worker_capable_method"};
  const std::string controlMethod{"control_method"};
  auto* const handlerFunction = reinterpret_cast<PyObject*>(41);
  auto* const coroutineFactory = reinterpret_cast<PyObject*>(84);
  auto workScheduler = std::make_shared<WorkScheduler>();
  const auto worker = workScheduler->registerWorker(
      folly::Executor::getKeepAliveToken(workerExecutor));
  auto executionSystem = makeWorkerCapableExecutionSystem(
      &controlExecutor,
      [](PyObject*) { return 0; },
      {{workerCapableMethod, handlerFunction},
       {controlMethod, handlerFunction}},
      workScheduler,
      [](PyObject*, std::shared_ptr<WorkerCapacityLease>) { return 0; },
      {workerCapableMethod});
  executionSystem->bindWorkerMethods(
      worker,
      {{workerCapableMethod, handlerFunction},
       {controlMethod, handlerFunction}});
  workScheduler->makeWorkerCapacityAvailable(worker);
  std::map<std::string, const folly::Executor*> followUpExecutors;
  const auto recordFollowUpExecutor = [&followUpExecutors,
                                       &serverRequestExecutor,
                                       coroutineFactory](std::string method) {
    return RequestDispatch(
        folly::Executor::getKeepAliveToken(serverRequestExecutor),
        [&followUpExecutors, method = std::move(method), coroutineFactory](
            PyObject*,
            const folly::Executor::KeepAlive<>& followUpExecutor,
            LaunchRequest launchRequest) {
          followUpExecutors[method] = followUpExecutor.get();
          std::move(launchRequest)(coroutineFactory);
          return folly::makeSemiFuture();
        });
  };
  const std::map<std::string, const folly::Executor*> expected{
      {workerCapableMethod, &workerExecutor},
      {controlMethod, &serverRequestExecutor},
  };

  // WHEN
  executionSystem->execute(
      workerCapableMethod, recordFollowUpExecutor(workerCapableMethod));
  executionSystem->execute(
      controlMethod, recordFollowUpExecutor(controlMethod));
  controlExecutor.drain();
  workerExecutor.drain();
  workScheduler->close();

  // THEN
  EXPECT_EQ(expected, followUpExecutors);
}

TEST(ExecutionSystemTest, EachRequestDispatchesUnderItsOwnRequestContext) {
  // GIVEN
  folly::ManualExecutor controlExecutor;
  folly::ManualExecutor workerExecutor;
  const std::string workerCapableMethod{"worker_capable_method"};
  const std::string controlMethod{"control_method"};
  auto* const handlerFunction = reinterpret_cast<PyObject*>(41);
  auto workScheduler = std::make_shared<WorkScheduler>();
  const auto worker = workScheduler->registerWorker(
      folly::Executor::getKeepAliveToken(workerExecutor));
  auto executionSystem = makeWorkerCapableExecutionSystem(
      &controlExecutor,
      [](PyObject*) { return 0; },
      {{workerCapableMethod, handlerFunction},
       {controlMethod, handlerFunction}},
      workScheduler,
      [](PyObject*, std::shared_ptr<WorkerCapacityLease>) { return 0; },
      {workerCapableMethod});
  executionSystem->bindWorkerMethods(
      worker,
      {{workerCapableMethod, handlerFunction},
       {controlMethod, handlerFunction}});
  workScheduler->makeWorkerCapacityAvailable(worker);
  const std::map<std::string, std::shared_ptr<folly::RequestContext>>
      requestContexts{
          {workerCapableMethod, std::make_shared<folly::RequestContext>()},
          {controlMethod, std::make_shared<folly::RequestContext>()},
      };
  // Executor threads hold whatever context their previous work left behind.
  const auto leftoverContext = std::make_shared<folly::RequestContext>();
  std::map<std::string, folly::RequestContext*> dispatchContexts;
  const auto recordDispatchContext = [&dispatchContexts](std::string method) {
    return RequestDispatch(
        folly::Executor::KeepAlive<>{},
        [&dispatchContexts, method = std::move(method)](
            PyObject*, const folly::Executor::KeepAlive<>&, LaunchRequest) {
          dispatchContexts[method] = folly::RequestContext::get();
          return folly::makeSemiFuture();
        });
  };
  const std::map<std::string, folly::RequestContext*> expected{
      {workerCapableMethod, requestContexts.at(workerCapableMethod).get()},
      {controlMethod, requestContexts.at(controlMethod).get()},
  };

  // WHEN
  for (const auto& [method, requestContext] : requestContexts) {
    folly::RequestContextScopeGuard requestContextGuard{requestContext};
    executionSystem->execute(method, recordDispatchContext(method));
  }
  {
    folly::RequestContextScopeGuard leftoverContextGuard{leftoverContext};
    controlExecutor.drain();
    workerExecutor.drain();
  }
  workScheduler->close();

  // THEN
  EXPECT_EQ(expected, dispatchContexts);
}

} // namespace
} // namespace apache::thrift::python::execution
