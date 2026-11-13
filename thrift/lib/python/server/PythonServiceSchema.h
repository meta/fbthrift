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

#include <memory>
#include <span>
#include <string>
#include <string_view>

#include <folly/CppAttributes.h>
#include <folly/container/F14Map.h>

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
      std::span<const std::string> schemaBundles,
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
   * Called once per method while the server builds its method metadata, never
   * on the request path.
   */
  const syntax_graph::FunctionNode* FOLLY_NULLABLE
  function(std::string_view methodName) const;

 private:
  // Owns the nodes that the accessors hand out.
  std::unique_ptr<syntax_graph::SyntaxGraph> syntaxGraph_;
  const syntax_graph::ServiceNode* serviceNode_{nullptr};
  folly::F14FastMap<std::string, const syntax_graph::FunctionNode*> functions_;
};

} // namespace apache::thrift::python
