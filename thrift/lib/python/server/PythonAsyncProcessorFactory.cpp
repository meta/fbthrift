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

#include <memory>

#include <folly/python/error.h>
#include <folly/python/import.h>
#include <thrift/lib/python/server/PythonAsyncProcessor.h>
#include <thrift/lib/python/server/PythonAsyncProcessorFactory.h>
#include <thrift/lib/python/server/python_async_processor_api.h> // @manual

namespace apache::thrift::python {

namespace {

void do_python_import() {
  constinit static folly::python::import_cache_nocapture import(
      ::import_thrift__python__server_impl__python_async_processor);
  if (!import()) {
    folly::python::handlePythonError(
        "import thrift.python.server_impl.python_async_processor failed");
  }
}

} // namespace

enum class LifecycleFunc {
  ON_START_SERVING = 0,
  ON_STOP_REQUESTED = 1,
};

namespace {
std::string getLifecycleFuncName(LifecycleFunc func) {
  switch (func) {
    case LifecycleFunc::ON_START_SERVING: {
      return "onStartServing";
    }
    case LifecycleFunc::ON_STOP_REQUESTED: {
      return "onStopRequested";
    }
  }
}

std::shared_ptr<const PythonAsyncProcessor::PythonMetadata> makeMetadata(
    const HandlerFunc& function) {
  std::optional<std::string> interactionName;
  if (!function.interactionName.empty()) {
    interactionName = std::string(function.interactionName);
  }
  return std::make_shared<PythonAsyncProcessor::PythonMetadata>(
      PythonAsyncProcessor::PythonMetadata::ExecutorType::ANY,
      function.interactionType,
      function.kind,
      interactionName,
      function.createsInteraction);
}
} // namespace

folly::SemiFuture<folly::Unit>
PythonAsyncProcessorFactory::semifuture_onStartServing() {
  if (requestedThreadCount() > 1 && !factoryContextEntered_) {
    do_python_import();
    warnThreadsWithoutFactoryContext();
  }
  return callLifecycle(LifecycleFunc::ON_START_SERVING);
}

folly::SemiFuture<folly::Unit>
PythonAsyncProcessorFactory::semifuture_onStopRequested() {
  auto lifecycle = callLifecycle(LifecycleFunc::ON_STOP_REQUESTED);
#ifdef Py_GIL_DISABLED
  // In free-threaded Python, AsyncioExecutor::drop() can run before the factory
  // destructor and make destructor-time executor release unsafe. Factory
  // context exit prevents that race by retaining resources through native stop
  // and active-request drain. Hosts that never enter the factory context
  // release those resources here, at stop, instead. This release cannot cover
  // connections that arrive after it; the factory context gives a host the
  // exact lifetime boundary.
  if (!factoryContextEntered_) {
    std::lock_guard lock(executionResourcesMutex_);
    controlExecutor_ = folly::Executor::KeepAlive<>{};
    executionSystem_ = execution::makeReleasedExecutionSystem();
  }
#endif

  return lifecycle;
}

void PythonAsyncProcessorFactory::releaseOwnedResources() noexcept {
  std::lock_guard lock(executionResourcesMutex_);
  executionSystem_->shutdown();
  controlExecutor_.reset();
}

folly::SemiFuture<folly::Unit> PythonAsyncProcessorFactory::callLifecycle(
    LifecycleFunc funcType) {
  std::size_t ind = (std::size_t)funcType;
  if (ind < lifecycleFuncs_.size()) {
    if (auto func = lifecycleFuncs_[ind]) {
      auto [promise, future] = folly::makePromiseContract<folly::Unit>();
      do_python_import();
      handleLifecycleCallback(
          func, getLifecycleFuncName(funcType), std::move(promise));
      return std::move(future);
    }
  }
  return folly::makeSemiFuture();
}

PythonAsyncProcessorFactory::CreateMethodMetadataResult
PythonAsyncProcessorFactory::createMethodMetadata() {
  AsyncProcessorFactory::MethodMetadataMap result;

  for (const auto& [methodName, function] : functions_) {
    auto rpcKind = function.kind;
    switch (rpcKind) {
      case apache::thrift::RpcKind::SINGLE_REQUEST_SINGLE_RESPONSE:
      case apache::thrift::RpcKind::SINGLE_REQUEST_NO_RESPONSE:
      case apache::thrift::RpcKind::SINGLE_REQUEST_STREAMING_RESPONSE:
      case apache::thrift::RpcKind::SINK:
      case apache::thrift::RpcKind::BIDIRECTIONAL_STREAM:
        result.emplace(methodName, makeMetadata(function));
        break;
    }
  }

  return result;
}

std::shared_ptr<PythonAsyncProcessorFactory>
PythonAsyncProcessorFactory::create(
    PyObject* python_server,
    FunctionMapType functions,
    std::vector<PyObject*> lifecycleFuncs,
    std::shared_ptr<execution::ExecutionSystem> executionSystem,
    folly::Executor::KeepAlive<> controlExecutor,
    std::string serviceName) {
  return std::shared_ptr<PythonAsyncProcessorFactory>(
      new PythonAsyncProcessorFactory(
          python_server,
          std::move(functions),
          std::move(lifecycleFuncs),
          std::move(executionSystem),
          std::move(controlExecutor),
          std::move(serviceName)));
}

} // namespace apache::thrift::python
