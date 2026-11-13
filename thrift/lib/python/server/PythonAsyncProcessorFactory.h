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
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>
#include <Python.h>
#include <glog/logging.h>
#include <thrift/lib/cpp/protocol/TProtocolTypes.h>
#include <thrift/lib/cpp2/async/AsyncProcessor.h>
#include <thrift/lib/cpp2/gen/service_tcc.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>
#include <thrift/lib/python/server/PythonAsyncProcessor.h>
#include <thrift/lib/thrift/gen-cpp2/RpcMetadata_types.h>
#include <thrift/lib/thrift/gen-cpp2/metadata_types.h>

namespace apache::thrift::python {

// Held by pointer so that this header, which the Cython `.pxd` includes, does
// not pull in `SyntaxGraph.h` and the rest of the schema machinery.
class PythonServiceSchema;

class PythonAsyncProcessorFactory
    : public apache::thrift::AsyncProcessorFactory,
      public apache::thrift::ServiceHandlerBase {
 public:
  folly::SemiFuture<folly::Unit> semifuture_onStartServing() override;
  folly::SemiFuture<folly::Unit> semifuture_onStopRequested() override;

  // Release only after native request drain. No processor may dispatch after
  // this call; repeated release is supported.
  void releaseOwnedResources() noexcept;

  void markContextEntered() noexcept { factoryContextEntered_ = true; }

  // Replaces the execution system that processors dispatch through. Call it
  // before the server creates any processor. Nothing dispatches before then,
  // so this write takes no lock; ThreadSanitizer reports a race if a processor
  // is ever created during it.
  void setExecutionSystem(
      std::shared_ptr<execution::ExecutionSystem> executionSystem) {
    executionSystem_ = std::move(executionSystem);
  }

  // The host sets the count before factory context entry, on the thread that
  // enters it; entry and serving read it only afterwards.
  void setRequestedThreadCount(std::size_t count) noexcept {
    requestedThreadCount_ = count;
  }

  std::size_t requestedThreadCount() const noexcept {
    return requestedThreadCount_;
  }

  PythonAsyncProcessorFactory(PythonAsyncProcessorFactory&&) = delete;
  PythonAsyncProcessorFactory(const PythonAsyncProcessorFactory&) = delete;
  PythonAsyncProcessorFactory& operator=(PythonAsyncProcessorFactory&&) =
      delete;
  PythonAsyncProcessorFactory& operator=(const PythonAsyncProcessorFactory&) =
      delete;

  // Outlined because `PythonServiceSchema` is incomplete here.
  ~PythonAsyncProcessorFactory() override;

  std::unique_ptr<apache::thrift::AsyncProcessor> getProcessor() override {
    std::shared_ptr<execution::ExecutionSystem> executionSystem;
    folly::Executor::KeepAlive<> controlExecutor;
    {
      std::lock_guard lock(executionResourcesMutex_);
      executionSystem = executionSystem_;
      controlExecutor = controlExecutor_;
    }
    return std::make_unique<PythonAsyncProcessor>(
        python_server_,
        functions_,
        std::move(executionSystem),
        std::move(controlExecutor),
        serviceName_);
  }

  std::vector<apache::thrift::ServiceHandlerBase*> getServiceHandlers()
      override {
    return {this};
  }

  CreateMethodMetadataResult createMethodMetadata() override;

  std::vector<folly::not_null<const syntax_graph::ServiceNode*>>
  getServiceSchemaNodes() override;

  /**
   * This factory function is an extension of the python
   * PythonAsyncProcessorFactory.create() in python to handle the C++ side of
   * the python PythonAsyncProcessorFactory.create().
   *
   * `schemaBundles` are the schema bundles of the service's program and its
   * transitive includes, and `serviceDefinitionKey` the service's definition
   * key in them; see `PythonServiceSchema`. Pass no bundles to serve without
   * schema information.
   *
   * Callers must use this factory to create a PythonAsyncProcessorFactory
   * instance.
   */
  static std::shared_ptr<PythonAsyncProcessorFactory> create(
      PyObject* python_server,
      FunctionMapType functions,
      std::vector<PyObject*> lifecycleFuncs,
      std::shared_ptr<execution::ExecutionSystem> executionSystem,
      folly::Executor::KeepAlive<> controlExecutor,
      std::string serviceName,
      std::vector<std::string> schemaBundles = {},
      std::string_view serviceDefinitionKey = {});

 private:
  folly::SemiFuture<folly::Unit> callLifecycle(LifecycleFunc);

  PyObject* python_server_;
  // note that the key is std::string_view, which a view into buffer owned by
  // Python bytes keys of `dict funcMap` owned by cython `cdef class
  // PythonAsyncProcessorFactory`
  const FunctionMapType functions_;
  const std::vector<PyObject*> lifecycleFuncs_;
  // Serializes copies and release of resources shared with processors.
  std::mutex executionResourcesMutex_;
  std::shared_ptr<execution::ExecutionSystem> executionSystem_;
  folly::Executor::KeepAlive<> controlExecutor_;
  // Factory context entry completes before native service execution starts.
  bool factoryContextEntered_{false};
  std::size_t requestedThreadCount_{1};
  std::string serviceName_;
  // Built once, before any request is dispatched, and read-only afterwards.
  // Never null. Outlives every processor, so the nodes it owns stay valid for
  // the MethodMetadata and the interceptors that hold them.
  std::unique_ptr<const PythonServiceSchema> serviceSchema_;

  /**
   * Callers must use the create() factory function to create a
   * PythonAsyncProcessorFactory instance. Make the constructor
   * private to enforce this.
   *
   * Outlined for the same reason as the destructor.
   */
  PythonAsyncProcessorFactory(
      PyObject* python_server,
      FunctionMapType functions,
      std::vector<PyObject*> lifecycleFuncs,
      std::shared_ptr<execution::ExecutionSystem> executionSystem,
      folly::Executor::KeepAlive<> controlExecutor,
      std::string serviceName,
      std::unique_ptr<const PythonServiceSchema> serviceSchema);
};

} // namespace apache::thrift::python
