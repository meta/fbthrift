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

#include <string>
#include <vector>

#include <thrift/lib/cpp2/async/AsyncProcessorFactory.h>
#include <thrift/lib/cpp2/schema/SyntaxGraph.h>

namespace apache::thrift::python::test {

// Names each service that `factory.getServiceSchema()` describes, each
// followed by its base services, looked up in the returned schema.
inline std::vector<std::string> serviceSchemaLineage(
    AsyncProcessorFactory& factory) {
  auto schema = factory.getServiceSchema();
  if (!schema) {
    return {};
  }
  const auto graph =
      syntax_graph::SyntaxGraph::fromSchema(std::move(schema->schema));
  std::vector<std::string> names;
  for (const auto& key : schema->definitions) {
    const auto* service =
        &syntax_graph::detail::lookUpDefinition(graph, key).asService();
    for (; service != nullptr; service = service->baseService()) {
      names.emplace_back(service->definition().name());
    }
  }
  return names;
}

} // namespace apache::thrift::python::test
