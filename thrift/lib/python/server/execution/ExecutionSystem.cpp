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

#include <folly/io/async/Request.h>

namespace apache::thrift::python::execution {

void ExecutionSystem::shutdown() noexcept {
  controlExecutor_.reset();
  workScheduler_.reset();
  workerMethods_.clear();
}

void ExecutionSystem::bindWorkerMethods(
    WorkerHandle worker, std::map<std::string, PyObject*> methods) {
  workerMethods_[std::move(worker)] =
      std::make_shared<const std::map<std::string, PyObject*>>(
          std::move(methods));
}

void ExecutionSystem::execute(
    std::string method, RequestDispatch requestDispatch) const {
  // The map holds every method the server accepts. A request reaches this
  // point only after the server accepted its method, so the lookup always
  // succeeds.
  const auto destination = executeOn_.at(method);
  (this->*destination)(std::move(method), std::move(requestDispatch));
}

void ExecutionSystem::executeOnControl(
    std::string method, RequestDispatch requestDispatch) const {
  folly::makeSemiFuture()
      .deferValue([method = std::move(method),
                   controlMethods = controlMethods_,
                   requestDispatch = std::move(requestDispatch),
                   launchRequest = LaunchRequest(
                       [controlExecutor = controlExecutor_,
                        launchControlRequest =
                            launchControlRequest_](_object* coroutineFactory) {
                         // Retain the selected destination until Cython starts
                         // the request.
                         (void)controlExecutor;
                         return launchControlRequest(coroutineFactory);
                       })](folly::Unit) mutable {
        // Both handler maps come from the same composition-root catalogue,
        // and the name already passed that catalogue lookup upstream in
        // executeRequest, so absence here is unreachable on valid paths.
        // Loop-local handlers are a future requirement. With loop-local
        // handlers, the handler function depends on the loop that executes
        // it, which is available only after the execution system schedules
        // a worker to take up the work. To prepare for that future, look
        // up the function by method name in today's single shared context.
        auto* selectedHandlerFunction = controlMethods->at(method);
        // Follow-up work stays on the server's request executor, which the
        // server joins at destruction in both modes.
        auto requestExecutor = requestDispatch.getRequestExecutor();
        return std::move(requestDispatch)(
            selectedHandlerFunction,
            std::move(requestExecutor),
            std::move(launchRequest));
      })
      .via(controlExecutor_);
}

void ExecutionSystem::executeOnWorker(
    std::string method, RequestDispatch requestDispatch) const {
  workScheduler_->addAsynchronousWorkWithPriority(
      [method = std::move(method),
       requestDispatch = std::move(requestDispatch),
       launchWorkerRequest = launchWorkerRequest_,
       requestContext = folly::RequestContext::saveContext(),
       this](
          const WorkerHandle& worker,
          const folly::Executor::KeepAlive<>& workerExecutor,
          std::shared_ptr<WorkerCapacityLease> lease) mutable {
        // Makes the RequestContext that was current at hand-off current while
        // the dispatch is set up; the dispatch's work on the worker inherits it
        // from there, as a future continuation does on control. The worker
        // thread gets its previous context back when this callback returns.
        folly::RequestContextScopeGuard requestContextGuard{
            std::move(requestContext)};
        auto* const selectedFunction = getWorkerHandlerFunction(worker, method);
        LaunchRequest launchRequest(
            [lease = std::move(lease),
             launchWorkerRequest](_object* coroutineFactory) mutable {
              return launchWorkerRequest(coroutineFactory, std::move(lease));
            });
        // Drive the dispatch future on the worker; dropping it undriven
        // cancels the request. A dispatch that suspends, as in a native
        // interceptor, then resumes on the worker's loop, where Python runs.
        // The dispatch reports each outcome to the client itself, as it does
        // on control, so nothing reads this future.
        folly::makeSemiFutureWith([requestDispatch = std::move(requestDispatch),
                                   selectedFunction = selectedFunction,
                                   workerExecutor = workerExecutor,
                                   launchRequest =
                                       std::move(launchRequest)]() mutable {
          return std::move(requestDispatch)(
              selectedFunction,
              std::move(workerExecutor),
              std::move(launchRequest));
        }).via(workerExecutor);
      },
      folly::Executor::MID_PRI);
}

PyObject* ExecutionSystem::getWorkerHandlerFunction(
    const WorkerHandle& worker, const std::string& method) const {
  // Both lookups succeed on every valid path. Each worker binds its table
  // before it publishes capacity, so work reaches only bound workers. Worker
  // start rejects a table that lacks a routed method. And shutdown clears the
  // tables only after worker teardown.
  return workerMethods_.at(worker)->at(method);
}

} // namespace apache::thrift::python::execution
