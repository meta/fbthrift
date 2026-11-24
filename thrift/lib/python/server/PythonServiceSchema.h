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

#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <folly/CppAttributes.h>
#include <folly/container/F14Map.h>
#include <folly/synchronization/DelayedInit.h>

#include <thrift/lib/cpp2/schema/SchemaV1.h>
#include <thrift/lib/cpp2/schema/SyntaxGraph.h>

namespace apache::thrift::python {

/**
 * The C++ view of a thrift-python service's schema.
 *
 * A generated thrift-python module bundles its program's schema as a
 * zstd-compressed, Compact-serialized `type::Schema` -- the encoding
 * `apache::thrift::schema::detail::readSchema` reads. The server runtime hands
 * those bundles (the service's own program, then the programs it references)
 * over from Python, and this builds a `SyntaxGraph` over them so that C++
 * ServiceInterceptors can deserialize a method's arguments from its parameter
 * types.
 *
 * Accessors return null when no schema is available. That is expected, not
 * exceptional: a Thrift library built without a bundled schema carries no
 * bundle at all, and an incomplete set of bundles leaves references that
 * `SyntaxGraph` rejects.
 */
class PythonServiceSchema {
 public:
  /**
   * Builds the schema of the service `serviceName` from `schemaBundles`.
   * `serviceDefinitionKey` is the service's definition key, the generated
   * interface's `__get_thrift_definition_key__()`: the merged bundles can hold
   * other programs that define a service of the same name, and the key picks
   * the service's own program.
   *
   * Returns an empty schema, rather than throwing, when they do not yield a
   * usable graph: a service must still serve without one. Also returns an
   * empty schema when the `python_server_populate_service_schema` Thrift flag
   * is off. The flag is read here, once per factory: turning it off affects
   * servers created afterwards, and a running server keeps the schema it
   * built until it restarts.
   */
  static PythonServiceSchema build(
      std::span<const std::string_view> schemaBundles,
      std::string_view serviceName,
      std::string_view serviceDefinitionKey);

  PythonServiceSchema() = default;

  const syntax_graph::ServiceNode* FOLLY_NULLABLE service() const {
    return serviceNode_;
  }

  /**
   * The schema node of a method, keyed the way the server dispatches it:
   * `method` for a service method, `Interaction.method` for one inside an
   * interaction.
   *
   * Called on the request path only when the server has ServiceInterceptors.
   */
  const syntax_graph::FunctionNode* FOLLY_NULLABLE
  function(std::string_view methodName) const;

  /**
   * The merged schema with the service's definition key, in the shape
   * `AsyncProcessorFactory::getServiceSchema()` returns. A copy, so call it
   * only off the request path.
   */
  std::optional<schema::DefinitionsSchema> definitionsSchema() const;

 private:
  // Declared before `syntaxGraph_`, which borrows it and so must be destroyed
  // first.
  std::unique_ptr<const type::Schema> schema_;
  // Owns the nodes that the accessors hand out.
  std::unique_ptr<syntax_graph::SyntaxGraph> syntaxGraph_;
  std::optional<type::DefinitionKey> serviceKey_;
  const syntax_graph::ServiceNode* serviceNode_{nullptr};
  folly::F14FastMap<std::string, const syntax_graph::FunctionNode*> functions_;
};

/**
 * The schema of one service, shared by every processor factory of the service
 * for the life of the process and built only when first asked for.
 *
 * A host can create a processor factory per request: Instagram's Django
 * servers do, in each of many worker processes. Building the schema there on
 * every factory cost milliseconds of CPU per request (S730485), and keeping a
 * built schema per service in every worker cost enough memory to make workers
 * recycle more often (S731977), as did collecting the bundles, which imports
 * every generated module the service's program includes. So a factory only
 * records how to collect the bundles, and they are collected and the schema
 * built the first time something needs it: a server that runs
 * ServiceInterceptors, or a request for the service schema. Neither happens on
 * that host's request path.
 */
class SharedServiceSchema {
 public:
  /**
   * Collects the schema bundles of a service when its schema is first built,
   * on any thread. It may take the GIL, so it is never called under a lock.
   * Returns no bundles when there are none or they cannot be collected.
   */
  using BundleLoader = std::function<std::vector<std::string>()>;

  /**
   * The shared schema of the service `serviceDefinitionKey`. The first call for
   * a key with a loader keeps `loadBundles`; later calls return the same schema
   * and ignore theirs.
   *
   * Returns an empty schema, without remembering it, when the
   * `python_server_populate_service_schema` Thrift flag is off,
   * `serviceDefinitionKey` is empty, or the key is new and comes without a
   * loader. A caller makes no loader when `needsLoader` said it need not, which
   * the flag turning on between the two calls would otherwise leave remembered
   * without one for the life of the process. The flag is read here, when a
   * factory is created. Never null.
   */
  static std::shared_ptr<const SharedServiceSchema> get(
      BundleLoader loadBundles,
      std::string_view serviceName,
      std::string_view serviceDefinitionKey);

  /**
   * Whether `get` would keep the loader passed to it, so the caller can skip
   * making one.
   */
  static bool needsLoader(std::string_view serviceDefinitionKey);

  // Forgets every schema `get` returned, so a test starts from none.
  static void clearForTest();

  SharedServiceSchema(
      BundleLoader loadBundles,
      std::string serviceName,
      std::string serviceDefinitionKey);

  // Collects the bundles and builds the schema on the first call, from any
  // thread.
  const PythonServiceSchema& schema() const;

  bool isBuilt() const { return schema_.has_value(); }

 private:
  BundleLoader loadBundles_;
  std::string serviceName_;
  std::string serviceDefinitionKey_;
  mutable folly::DelayedInit<PythonServiceSchema> schema_;
};

} // namespace apache::thrift::python
