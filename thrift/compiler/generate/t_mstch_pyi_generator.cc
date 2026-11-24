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

#include <algorithm>
#include <filesystem>
#include <set>
#include <string>

#include <fmt/format.h>

#include <thrift/compiler/ast/t_program.h>
#include <thrift/compiler/ast/t_type.h>
#include <thrift/compiler/generate/common.h>
#include <thrift/compiler/generate/python/util.h>
#include <thrift/compiler/generate/t_whisker_generator.h>
#include <thrift/compiler/generate/templates.h>

namespace apache::thrift::compiler {

namespace {

// The name that thrift-py-deprecated gives a field or enum value: names in
// `get_python_reserved_names()` get a suffix, as in `t_py_generator`, so that
// the stubs match the generated code.
std::string get_filtered_name(const t_named& node) {
  return get_python_reserved_names().contains(node.name())
      ? fmt::format("{}_PY_RESERVED_KEYWORD", node.name())
      : node.name();
}

// TO-DO: remove duplicate in py3
bool has_types(const t_program* program) {
  assert(program != nullptr);

  return !(
      program->structured_definitions().empty() && program->enums().empty() &&
      program->typedefs().empty() && program->consts().empty());
}

std::vector<std::string> get_py_namespaces_raw(
    const t_program* program, bool is_asyncio, const std::string& tail = {}) {
  assert(program != nullptr);

  auto& py_namespace = program->get_namespace("py");
  auto& py_asyncio_namespace = program->get_namespace("py.asyncio");
  auto namespace_name = is_asyncio && !py_asyncio_namespace.empty()
      ? py_asyncio_namespace
      : py_namespace;

  auto ns = split_namespace(namespace_name);
  if (ns.empty()) {
    ns.push_back(program->name());
  }
  if (!tail.empty()) {
    ns.push_back(tail);
  }

  return ns;
}

const std::string* get_py_adapter(const t_type* type) {
  assert(type != nullptr);

  if (!type->get_true_type()->is<t_struct>() &&
      !type->get_true_type()->is<t_union>()) {
    return nullptr;
  }

  return t_typedef::get_first_unstructured_annotation_or_null(
      type, {"py.adapter"});
}

std::set<std::string> get_distinct_adapters(const t_program* program) {
  assert(program != nullptr);

  std::set<std::string> adapters;

  auto add_adapter = [&adapters](const t_type* type) {
    const auto* adapter = get_py_adapter(type);
    if (adapter != nullptr) {
      adapters.insert(adapter->substr(0, adapter->find_last_of('.')));
    }
  };

  for (const auto* strct : program->structs_and_unions()) {
    for (const auto* type : collect_types(strct)) {
      add_adapter(type);
    }
  }

  for (const auto* type : program->typedefs()) {
    add_adapter(type);
  }

  return adapters;
}

// Adds the programs that define the types which `type` is rendered as by the
// `type_name` template. Typedefs are rendered as the types they resolve to, so
// these can be programs that the rendered program only includes transitively.
void collect_rendered_programs(
    const t_type& type, std::set<const t_program*>& programs) {
  if (get_py_adapter(&type) != nullptr) {
    return; // Rendered as the adapter's type, whose module is imported.
  }
  const t_type* true_type = type.get_true_type();
  if (const auto* list = true_type->try_as<t_list>()) {
    collect_rendered_programs(*list->elem_type(), programs);
  } else if (const auto* set = true_type->try_as<t_set>()) {
    collect_rendered_programs(*set->elem_type(), programs);
  } else if (const auto* map = true_type->try_as<t_map>()) {
    collect_rendered_programs(*map->key_type(), programs);
    collect_rendered_programs(*map->val_type(), programs);
  } else if (true_type->is<t_structured>() || true_type->is<t_enum>()) {
    programs.insert(true_type->program());
  }
}

// The programs whose types the stubs of `program` refer to.
std::set<const t_program*> gather_rendered_programs(const t_program* program) {
  std::set<const t_program*> programs;
  for (const t_structured* structured : program->structured_definitions()) {
    for (const t_field& field : structured->fields()) {
      collect_rendered_programs(*field.type(), programs);
    }
  }
  for (const t_typedef* typedef_ : program->typedefs()) {
    collect_rendered_programs(*typedef_, programs);
  }
  for (const t_const* const_ : program->consts()) {
    collect_rendered_programs(*const_->type(), programs);
  }
  for (const t_service* service : program->services()) {
    for (const t_function& function : service->functions()) {
      collect_rendered_programs(*function.return_type(), programs);
      for (const t_field& param : function.params().fields()) {
        collect_rendered_programs(*param.type(), programs);
      }
    }
  }
  return programs;
}

std::vector<std::string> gather_import_modules(
    const t_program* program, bool is_asyncio) {
  assert(program != nullptr);

  std::vector<std::string> import_modules = {};

  for (const auto* prog : program->get_includes_for_codegen()) {
    if (prog->path() == program->path()) {
      continue;
    }

    if (has_types(prog)) {
      import_modules.push_back(
          fmt::format(
              "{}",
              fmt::join(
                  get_py_namespaces_raw(prog, is_asyncio, "ttypes"), ".")));
    }
  }

  auto adapters = get_distinct_adapters(program);
  for (const auto& adapter : adapters) {
    import_modules.push_back(adapter);
  }

  // Also import the modules of types that are only reachable through
  // typedefs of included programs, which the includes above do not cover.
  std::set<std::string> transitive_modules;
  for (const t_program* prog : gather_rendered_programs(program)) {
    if (prog == nullptr || prog->path() == program->path()) {
      continue;
    }
    std::string module = fmt::format(
        "{}",
        fmt::join(get_py_namespaces_raw(prog, is_asyncio, "ttypes"), "."));
    if (std::find(import_modules.begin(), import_modules.end(), module) ==
        import_modules.end()) {
      transitive_modules.insert(std::move(module));
    }
  }
  import_modules.insert(
      import_modules.end(),
      transitive_modules.begin(),
      transitive_modules.end());

  return import_modules;
}

// Generator

class t_mstch_pyi_generator : public t_whisker_generator {
 public:
  using t_whisker_generator::t_whisker_generator;

  std::string template_prefix() const override { return "pyi"; }

  void generate_program() override;

 private:
  std::filesystem::path root_path_;

  whisker::source_manager template_source_manager() const final {
    return whisker::source_manager{
        std::make_unique<in_memory_source_manager_backend>(
            create_templates_by_path())};
  }

  strictness_options strictness() const final {
    return strictness_options{
        // Templates still have non-boolean conditionals and implicit loops
        .boolean_conditional = false,
    };
  }

  void generate_init_files();
  void generate_constants();
  void generate_ttypes();
  void generate_services();
  void render_file(
      const std::string& template_name,
      const std::filesystem::path& path,
      const t_service* service = nullptr);
  std::filesystem::path get_root_path() const;

  const t_program* get_true_type_program(const t_type& type) const {
    const t_program* prog = type.get_true_type()->program();
    return prog != nullptr ? prog : program_;
  }

  prototype<t_field>::ptr make_prototype_for_field(
      const prototype_database& proto) const override {
    auto base = t_whisker_generator::make_prototype_for_field(proto);
    auto def =
        whisker::dsl::prototype_builder<h_field>::extends(std::move(base));

    def.property("py_name", &get_filtered_name);
    def.property("capitalizedName", [](const t_field& self) {
      std::string name = get_filtered_name(self);
      std::transform(name.begin(), name.end(), name.begin(), ::toupper);
      return name;
    });

    return std::move(def).make();
  }

  prototype<t_enum_value>::ptr make_prototype_for_enum_value(
      const prototype_database& proto) const override {
    auto base = t_whisker_generator::make_prototype_for_enum_value(proto);
    auto def = whisker::dsl::prototype_builder<h_enum_value>::extends(base);
    def.property("py_name", &get_filtered_name);
    return std::move(def).make();
  }

  prototype<t_function>::ptr make_prototype_for_function(
      const prototype_database& proto) const override {
    auto base = t_whisker_generator::make_prototype_for_function(proto);
    auto def =
        whisker::dsl::prototype_builder<h_function>::extends(std::move(base));

    def.property("isSupported?", [](const t_function& self) {
      // Stream and sink functions are not supported, see
      // t_py_generator::get_functions.
      return !self.is_interaction_constructor() &&
          self.sink_or_stream() == nullptr;
    });

    return std::move(def).make();
  }

  prototype<t_program>::ptr make_prototype_for_program(
      const prototype_database& proto) const override {
    auto base = t_whisker_generator::make_prototype_for_program(proto);
    auto def =
        whisker::dsl::prototype_builder<h_program>::extends(std::move(base));

    def.property("pyNamespace", [this](const t_program& self) {
      std::vector<std::string> namespaces =
          get_py_namespaces_raw(&self, has_compiler_option("asyncio"));
      return namespaces.empty()
          ? ""
          : fmt::format("{}.", fmt::join(namespaces, "."));
    });
    def.property("py3Namespace", [](const t_program& self) {
      std::vector<std::string> namespaces = get_py3_namespace(&self);
      return namespaces.empty()
          ? ""
          : fmt::format("{}.", fmt::join(namespaces, "."));
    });
    def.property("pythonNamespace", [](const t_program& self) {
      // TODO: take root_module_prefix into account
      std::vector<std::string> namespaces = get_py3_namespace(&self);
      return namespaces.empty()
          ? ""
          : fmt::format("{}.", fmt::join(namespaces, "."));
    });
    def.property("importModules", [this](const t_program& self) {
      std::vector<std::string> modules =
          gather_import_modules(&self, has_compiler_option("asyncio"));
      whisker::array::raw a;
      a.insert(
          a.end(),
          std::make_move_iterator(modules.begin()),
          std::make_move_iterator(modules.end()));
      return whisker::make::array(std::move(a));
    });
    def.property("enablePosArgs?", [this](const t_program&) {
      return has_compiler_option("enable_pos_args");
    });

    return std::move(def).make();
  }

  prototype<t_type>::ptr make_prototype_for_type(
      const prototype_database& proto) const override {
    auto base = t_whisker_generator::make_prototype_for_type(proto);
    auto def =
        whisker::dsl::prototype_builder<h_type>::extends(std::move(base));

    def.property("modulePath", [this](const t_type& self) {
      std::vector<std::string> namespaces = get_py_namespaces_raw(
          get_true_type_program(self),
          has_compiler_option("asyncio"),
          "ttypes");
      return fmt::format("{}", fmt::join(namespaces, "."));
    });
    def.property("externalProgram?", [this](const t_type& self) {
      return get_true_type_program(self) != program_;
    });
    def.property("adapter", [](const t_type& self) {
      const std::string* adapter = get_py_adapter(&self);
      return adapter == nullptr ? whisker::make::null
                                : whisker::make::string(*adapter);
    });

    return std::move(def).make();
  }
};

void t_mstch_pyi_generator::generate_program() {
  this->root_path_ = this->get_root_path();
  this->out_dir_base_ = "gen-py";

  this->generate_init_files();
  this->generate_constants();
  this->generate_ttypes();
  this->generate_services();
}

void t_mstch_pyi_generator::generate_init_files() {
  std::filesystem::path directory;
  for (const auto& part : this->root_path_) {
    directory /= part;

    this->render_file("__init__.pyi", directory / "__init__.pyi");
  }
}

void t_mstch_pyi_generator::generate_constants() {
  const std::string template_name = "constants.pyi";
  this->render_file(template_name, this->root_path_ / template_name);
}

void t_mstch_pyi_generator::generate_ttypes() {
  const std::string template_name = "ttypes.pyi";
  this->render_file(template_name, this->root_path_ / template_name);
}

void t_mstch_pyi_generator::generate_services() {
  const std::string template_name = "service.pyi";
  for (const auto* service : this->get_program()->services()) {
    const std::string module = service->name() + ".pyi";

    this->render_file(template_name, this->root_path_ / module, service);
  }
}

void t_mstch_pyi_generator::render_file(
    const std::string& template_name,
    const std::filesystem::path& path,
    const t_service* service) {
  whisker::object context = service == nullptr
      ? whisker::make::native_handle(
            render_state().prototypes->create<t_program>(*program_))
      : whisker::make::native_handle(
            render_state().prototypes->create<t_service>(*service));
  render_to_file(/*output_file=*/path,
                 /*template_file=*/template_name,
                 /*context=*/context);
}

std::filesystem::path t_mstch_pyi_generator::get_root_path() const {
  std::filesystem::path path;

  auto namespaces = get_py_namespaces_raw(
      this->get_program(), has_compiler_option("asyncio"));
  for (const auto& ns : namespaces) {
    path /= ns;
  }
  path += std::filesystem::path::preferred_separator;

  return path;
}

} // namespace

THRIFT_REGISTER_GENERATOR(
    mstch_pyi, "Legacy Python type information", "no arguments");

} // namespace apache::thrift::compiler
