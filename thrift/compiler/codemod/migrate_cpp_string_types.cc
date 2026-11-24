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

#include <cctype>
#include <map>
#include <set>
#include <string>
#include <string_view>

#include <thrift/compiler/ast/ast_visitor.h>
#include <thrift/compiler/ast/t_program_bundle.h>
#include <thrift/compiler/codemod/codemod.h>
#include <thrift/compiler/codemod/file_manager.h>

using apache::thrift::compiler::const_ast_visitor;
using apache::thrift::compiler::kCppAdapterUri;
using apache::thrift::compiler::kCppTypeUri;
using apache::thrift::compiler::kPythonPy3EnableCppAdapterUri;
using apache::thrift::compiler::source_manager;
using apache::thrift::compiler::source_range;
using apache::thrift::compiler::t_const;
using apache::thrift::compiler::t_field;
using apache::thrift::compiler::t_named;
using apache::thrift::compiler::t_program;
using apache::thrift::compiler::t_program_bundle;
using apache::thrift::compiler::t_type;
using apache::thrift::compiler::t_type_ref;
using apache::thrift::compiler::t_typedef;
namespace codemod = apache::thrift::compiler::codemod;

namespace {

// Matches C++ tokens without joining identifiers or separated ':' tokens.
bool is_supported_string_cpp_type(std::string_view name) {
  auto consume = [&](std::string_view token) {
    while (!name.empty() &&
           std::isspace(static_cast<unsigned char>(name.front()))) {
      name.remove_prefix(1);
    }
    if (!name.starts_with(token)) {
      return false;
    }
    name.remove_prefix(token.size());
    return true;
  };
  consume("::");
  bool matched;
  if (consume("folly")) {
    matched = consume("::") && consume("IOBuf");
  } else if (consume("apache")) {
    matched = consume("::") && consume("thrift") && consume("::") &&
        consume("IOBufChain");
  } else if (consume("std") && consume("::")) {
    if (consume("pmr")) {
      matched = consume("::") && consume("string");
    } else {
      matched = consume("unique_ptr") && consume("<");
      consume("::");
      matched = matched && consume("folly") && consume("::") &&
          consume("IOBuf") && consume(">");
    }
  } else {
    matched = false;
  }
  consume("");
  return matched && name.empty();
}

const t_const* direct_cpp_type(const t_named& node) {
  for (const t_const& annotation : node.structured_annotations()) {
    if (annotation.type() && annotation.type()->uri() == kCppTypeUri) {
      return &annotation;
    }
  }
  return nullptr;
}

std::string adapter_annotation(const std::string& name) {
  return "@cpp.Adapter{name = \"::apache::thrift::StringTypeAdapter<" + name +
      ">\"}";
}

class string_type_migrator {
 public:
  string_type_migrator(codemod::file_manager& file_manager, t_program& program)
      : file_manager_(file_manager),
        program_(program),
        py3_(program.namespaces().contains("py3")) {
    for (const auto& definition : program.definitions()) {
      used_names_.insert(definition.name());
    }
  }

  void migrate(
      const t_named& node, const t_type_ref& type, bool is_field = false) {
    if (!type.resolved()) {
      return;
    }
    const t_type* true_type = type->get_true_type();
    if (!true_type || !true_type->is_string_or_binary()) {
      return;
    }
    const auto* annotation = direct_cpp_type(node);
    if (!annotation || annotation->value()->get_map().size() != 1 ||
        node.has_structured_annotation(kCppAdapterUri)) {
      return;
    }
    const auto* name =
        annotation->get_value_from_structured_annotation_or_null("name");
    if (!name || name->get_string().empty() ||
        is_supported_string_cpp_type(name->get_string()) ||
        name->get_string().find_first_of("\"\\\n\r") != std::string::npos) {
      return;
    }
    const source_range range = annotation->src_range();
    std::string replacement = adapter_annotation(name->get_string());
    if (is_field && type->is<t_typedef>()) {
      const auto* inherited =
          t_typedef::get_first_structured_annotation_or_null(
              &*type, kCppTypeUri);
      const auto* inherited_name = inherited
          ? inherited->get_value_from_structured_annotation_or_null("name")
          : nullptr;
      const auto* inherited_adapter =
          t_typedef::get_first_structured_annotation_or_null(
              &*type, kCppAdapterUri);
      const auto* adapter_name = inherited_adapter
          ? inherited_adapter->get_value_from_structured_annotation_or_null(
                "name")
          : nullptr;
      const bool same_type = inherited_name &&
          inherited_name->get_string() == name->get_string() &&
          !inherited_adapter;
      const bool same_adapter = !inherited && adapter_name &&
          adapter_name->get_string() ==
              "::apache::thrift::StringTypeAdapter<" + name->get_string() + ">";
      if (same_type || same_adapter) {
        replacement.clear();
      } else if (inherited || inherited_adapter) {
        // Do not stack a string adapter on an already customized typedef.
        return;
      }
    }
    if (is_field && py3_ && !replacement.empty()) {
      const size_t type_begin = file_manager_.to_offset(type.src_range().begin);
      const size_t type_end = file_manager_.to_offset(type.src_range().end);
      const std::string thrift_type(file_manager_.old_content().substr(
          type_begin, type_end - type_begin));
      const auto key = std::pair{thrift_type, name->get_string()};
      auto [it, added] = generated_typedefs_.try_emplace(key);
      if (added) {
        const std::string base = "StringType_" + node.name();
        std::string typedef_name = base;
        for (int i = 2; used_names_.contains(typedef_name); ++i) {
          typedef_name = base + "_" + std::to_string(i);
        }
        used_names_.insert(typedef_name);
        it->second = typedef_name;
        generated_source_ += "\n@python.Py3EnableCppAdapter\n" + replacement +
            "\ntypedef " + thrift_type + " " + typedef_name + "\n";
      }
      file_manager_.add({type_begin, type_end, it->second});
      replacement.clear();
      file_manager_.add_include("thrift/annotation/python.thrift");
    } else if (
        !is_field && py3_ &&
        !node.has_structured_annotation(kPythonPy3EnableCppAdapterUri)) {
      replacement = "@python.Py3EnableCppAdapter\n" + replacement;
      file_manager_.add_include("thrift/annotation/python.thrift");
    }
    file_manager_.add(
        {file_manager_.to_offset(range.begin),
         file_manager_.to_offset(range.end),
         std::move(replacement)});
  }

  void finish() {
    if (!generated_source_.empty()) {
      // Insert before root definitions, after includes and namespace
      // directives.
      const size_t offset = file_manager_.to_offset(
          program_.definitions().front().src_range().begin);
      file_manager_.add({offset, offset, generated_source_ + "\n"});
    }
  }

 private:
  codemod::file_manager& file_manager_;
  t_program& program_;
  bool py3_;
  std::set<std::string> used_names_;
  std::map<std::pair<std::string, std::string>, std::string>
      generated_typedefs_;
  std::string generated_source_;
};

} // namespace

int main(int argc, char** argv) {
  return apache::thrift::compiler::run_codemod(
      argc, argv, [](source_manager& source_manager, t_program_bundle& bundle) {
        t_program& program = *bundle.root_program();
        codemod::file_manager file_manager(source_manager, program);
        string_type_migrator migrator(file_manager, program);
        auto migrate_field = [&](const t_field& field) {
          migrator.migrate(field, field.type(), true);
        };
        const_ast_visitor visitor;
        visitor.add_field_visitor(migrate_field);
        visitor.add_function_param_visitor(migrate_field);
        visitor.add_thrown_exception_visitor(migrate_field);
        visitor.add_typedef_visitor([&](const t_typedef& type) {
          migrator.migrate(type, type.type());
        });
        visitor(program);
        migrator.finish();
        file_manager.apply_replacements();
      });
}
