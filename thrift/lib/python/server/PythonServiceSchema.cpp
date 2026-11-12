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

#include <thrift/lib/python/server/PythonServiceSchema.h>

#include <optional>
#include <string>
#include <vector>

#include <fmt/core.h>

#include <folly/Range.h>
#include <folly/logging/xlog.h>

#include <thrift/lib/cpp2/Flags.h>
#include <thrift/lib/cpp2/schema/detail/Merge.h>

// Kill switch over serving a thrift-python service's schema to C++
// ServiceInterceptors. Turning it off restores the behavior that predates it
// exactly: no schema is built, so `MethodMetadata::functionNode` stays null
// and an interceptor is handed a null `RequestInfo::dynamicArguments` -- which
// it must already handle, for a service that bundles no schema. It is read
// when a server's processor factory is created, so it takes effect on restart.
THRIFT_FLAG_DEFINE_bool(python_server_populate_service_schema, true);

namespace apache::thrift::python {

namespace {

// Indexes `service`'s methods, and those of the interactions they create,
// under the names the server dispatches them by: `method` for a service
// method, `Interaction.method` for one inside an interaction. Mirrors how the
// generated Python handler table is keyed.
//
// A name already indexed wins, so walking the base services after the derived
// one leaves a derived method shadowing the base method of the same name --
// again as the generated table composes them.
template <typename FunctionMap>
void indexFunctions(
    const syntax_graph::ServiceNode& service, FunctionMap& out) {
  for (const syntax_graph::ServiceNode* node = &service; node != nullptr;
       node = node->baseService()) {
    for (const syntax_graph::FunctionNode& function : node->functions()) {
      out.emplace(std::string(function.name()), &function);

      const auto* interaction = function.response().interaction();
      if (interaction == nullptr) {
        continue;
      }
      // Interactions do not nest, so one level down is all there is.
      const std::string_view interactionName = interaction->definition().name();
      for (const syntax_graph::FunctionNode& method :
           interaction->functions()) {
        out.emplace(
            fmt::format("{}.{}", interactionName, method.name()), &method);
      }
    }
  }
}

// The path of the program in `schema` that defines `definitionKey`.
std::optional<std::string> programPathOf(
    const type::Schema& schema, std::string_view definitionKey) {
  for (const auto& program : *schema.programs()) {
    for (const auto& key : *program.definitionKeys()) {
      if (key == definitionKey) {
        return *program.path();
      }
    }
  }
  return std::nullopt;
}

} // namespace

bool PythonServiceSchema::enabled() {
  return THRIFT_FLAG(python_server_populate_service_schema);
}

PythonServiceSchema PythonServiceSchema::build(
    std::span<const std::string> schemaBundles,
    std::string_view serviceName,
    std::string_view serviceDefinitionKey) {
  if (!enabled()) {
    return {};
  }

  if (schemaBundles.empty()) {
    // The service's Thrift library carries no bundled schema.
    return {};
  }

  PythonServiceSchema self;
  const std::vector<std::string_view> bundles(
      schemaBundles.begin(), schemaBundles.end());
  std::optional<std::string> programPath;
  try {
    // `loadBundledSchema` folds in the omnibus bundle, which carries the core
    // Thrift types that generated modules reference without bundling. It is
    // the call generated C++ handlers make for their own bundles.
    self.schema_ = std::make_unique<const type::Schema>(
        // NOLINTNEXTLINE(facebook-hte-DetailCall)
        schema::detail::loadBundledSchema(folly::range(bundles)));
    programPath = programPathOf(*self.schema_, serviceDefinitionKey);
    self.syntaxGraph_ = std::make_unique<syntax_graph::SyntaxGraph>(
        syntax_graph::SyntaxGraph::fromSchema(self.schema_.get()));
  } catch (const std::exception& ex) {
    // Most likely an incomplete set of bundles, which leaves references
    // unresolved.
    XLOG(WARNING) << "Cannot build the schema of Thrift service '"
                  << serviceName
                  << "'; interceptors will not see its arguments: "
                  << ex.what();
    return {};
  }

  try {
    // A service name is unique only within its program, which the definition
    // key picks.
    if (programPath) {
      for (const auto definition :
           self.syntaxGraph_->findProgramByPath(*programPath).definitions()) {
        if (definition->isService() && definition->name() == serviceName) {
          self.serviceNode_ = &definition->asService();
          self.serviceKey_ = type::DefinitionKey(serviceDefinitionKey);
          // Resolves the base services, which an incomplete set of bundles
          // may leave out.
          indexFunctions(*self.serviceNode_, self.functions_);
          return self;
        }
      }
    }
  } catch (const std::exception& ex) {
    XLOG(WARNING) << "Cannot resolve Thrift service '" << serviceName
                  << "' in its bundled schema; interceptors will not see its "
                     "arguments: "
                  << ex.what();
    return {};
  }

  XLOG(WARNING) << "Thrift service '" << serviceName
                << "' is absent from its own bundled schema; interceptors "
                   "will not see its arguments.";
  return {};
}

const syntax_graph::FunctionNode* PythonServiceSchema::function(
    std::string_view methodName) const {
  const auto it = functions_.find(std::string(methodName));
  return it == functions_.end() ? nullptr : it->second;
}

std::optional<schema::DefinitionsSchema>
PythonServiceSchema::definitionsSchema() const {
  if (!serviceKey_) {
    return std::nullopt;
  }
  schema::DefinitionsSchema result;
  result.definitions = {*serviceKey_};
  result.schema = *schema_;
  return result;
}

} // namespace apache::thrift::python
