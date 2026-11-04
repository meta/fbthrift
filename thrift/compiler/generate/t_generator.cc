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

#include <thrift/compiler/generate/t_generator.h>

#include <iterator>
#include <stdexcept>
#include <utility>
#include <vector>

#include <fmt/core.h>
#include <fmt/format.h>

#include <thrift/compiler/ast/node_list.h>
#include <thrift/compiler/ast/t_const.h>
#include <thrift/compiler/ast/t_enum.h>
#include <thrift/compiler/ast/t_enum_value.h>
#include <thrift/compiler/ast/t_exception.h>
#include <thrift/compiler/ast/t_field.h>
#include <thrift/compiler/ast/t_function.h>
#include <thrift/compiler/ast/t_interaction.h>
#include <thrift/compiler/ast/t_list.h>
#include <thrift/compiler/ast/t_map.h>
#include <thrift/compiler/ast/t_named.h>
#include <thrift/compiler/ast/t_paramlist.h>
#include <thrift/compiler/ast/t_primitive_type.h>
#include <thrift/compiler/ast/t_program.h>
#include <thrift/compiler/ast/t_service.h>
#include <thrift/compiler/ast/t_set.h>
#include <thrift/compiler/ast/t_sink.h>
#include <thrift/compiler/ast/t_stream.h>
#include <thrift/compiler/ast/t_struct.h>
#include <thrift/compiler/ast/t_throws.h>
#include <thrift/compiler/ast/t_type.h>
#include <thrift/compiler/ast/t_typedef.h>
#include <thrift/compiler/ast/t_union.h>

namespace apache::thrift::compiler {

namespace {
/**
 * Add consistent indentation and line breaks to the generator documentation
 * passed to `THRIFT_REGISTER_GENERATOR`.
 */
std::string normalize_documentation(std::string_view doc) {
  static constexpr auto kIndent = "    ";
  std::ostringstream out;
  while (!doc.empty()) {
    std::string_view line = doc.substr(0, doc.find_first_of('\n'));
    doc.remove_prefix(std::min(doc.size(), line.size() + 1));
    if (!line.empty()) {
      out << kIndent << line;
    }
    out << '\n';
  }
  return out.str();
}

std::string replace_all(
    std::string_view text, std::string_view from, std::string_view to) {
  if (from.empty()) {
    return std::string(text);
  }
  std::string result;
  size_t start = 0;
  for (size_t found; (found = text.find(from, start)) != std::string_view::npos;
       start = found + from.size()) {
    result.append(text.substr(start, found - start)).append(to);
  }
  result.append(text.substr(start));
  return result;
}

std::string escape_delimiters(
    std::string_view text, const docblock_style& style) {
  return replace_all(
      replace_all(text, style.terminator, style.escaped_terminator),
      style.nested_opener,
      style.escaped_nested_opener);
}

// Appends each field as `id: type name`, separated by commas.
void append_fields(
    std::string& out,
    node_list_view<const t_field> fields,
    const t_program& program) {
  std::string_view separator;
  for (const t_field& field : fields) {
    fmt::format_to(
        std::back_inserter(out),
        "{}{}: {} {}",
        separator,
        field.id(),
        thrift_type_name(field.type().deref(), program),
        field.name());
    separator = ", ";
  }
}

void append_throws(
    std::string& out, const t_throws* exceptions, const t_program& program) {
  if (get_elems(exceptions).empty()) {
    return;
  }
  out += " throws (";
  append_fields(out, get_elems(exceptions), program);
  out += ")";
}

std::string function_definition(
    const t_function& function, const t_program& program) {
  auto type_name = [&](const t_type_ref& type) {
    return thrift_type_name(type.deref(), program);
  };
  std::string out;
  switch (function.qualifier()) {
    case t_function_qualifier::none:
      break;
    case t_function_qualifier::oneway:
      out += "oneway ";
      break;
    case t_function_qualifier::idempotent:
      out += "idempotent ";
      break;
    case t_function_qualifier::readonly:
      out += "readonly ";
      break;
  }

  // The response lists what the function returns, as in `I, i32, stream<T>`,
  // and leaves out `void` unless it is all the function returns.
  std::vector<std::string> response;
  if (!function.interaction().empty()) {
    response.push_back(type_name(function.interaction()));
  }
  const t_sink* sink = function.sink();
  const t_stream* stream = function.stream();
  if (!function.has_void_initial_response() ||
      (response.empty() && sink == nullptr && stream == nullptr)) {
    response.push_back(type_name(function.return_type()));
  }
  if (sink != nullptr) {
    std::string part = "sink<" + type_name(sink->elem_type());
    append_throws(part, sink->sink_exceptions(), program);
    if (sink->final_response_type()) {
      part += ", " + type_name(sink->final_response_type());
      append_throws(part, sink->final_response_exceptions(), program);
    }
    response.push_back(part + ">");
  }
  if (stream != nullptr) {
    std::string part = "stream<" + type_name(stream->elem_type());
    append_throws(part, stream->exceptions(), program);
    response.push_back(part + ">");
  }

  fmt::format_to(
      std::back_inserter(out),
      "{} {}(",
      fmt::join(response, ", "),
      function.name());
  append_fields(out, function.params().fields(), program);
  out += ")";
  append_throws(out, function.exceptions(), program);
  return out;
}

std::string field_definition(const t_field& field, const t_program& program) {
  std::string_view qualifier;
  switch (field.qualifier()) {
    case t_field_qualifier::required:
      qualifier = "required ";
      break;
    case t_field_qualifier::optional:
      qualifier = "optional ";
      break;
    case t_field_qualifier::none:
    case t_field_qualifier::terse:
      break;
  }
  return fmt::format(
      "{}: {}{} {}",
      field.id(),
      qualifier,
      thrift_type_name(field.type().deref(), program),
      field.name());
}
} // namespace

docblock_style docblock_style::c_style() {
  return {
      .opening = "/**\n",
      .line_prefix = " * ",
      .closing = " */\n",
      .terminator = "*/",
      .escaped_terminator = "*\\/",
      .nested_opener = "/*",
      .escaped_nested_opener = "/\\*",
  };
}

docblock_style docblock_style::python_style() {
  // Escaping the third quote leaves no run of three unescaped quotes, even
  // where the text has more than three in a row.
  return {
      .opening = "r\"\"\"\n",
      .line_prefix = "",
      .closing = "\"\"\"\n",
      .terminator = R"(""")",
      .escaped_terminator = R"(""\")",
  };
}

docblock_style docblock_style::triple_slash_style() {
  return {.opening = "", .line_prefix = "/// ", .closing = ""};
}

docblock_style docblock_style::hash_style() {
  return {.opening = "", .line_prefix = "# ", .closing = ""};
}

std::string apply_docblock_style(
    std::string_view unescaped_text, const docblock_style& style) {
  const std::string escaped = escape_delimiters(unescaped_text, style);
  std::string_view text = escaped;
  std::string result(style.opening);
  while (!text.empty()) {
    const size_t newline = text.find('\n');
    std::string line(style.line_prefix);
    line.append(text.substr(0, newline));
    line.erase(line.find_last_not_of(" \t") + 1);
    result.append(line).push_back('\n');
    if (newline == std::string_view::npos) {
      break;
    }
    text.remove_prefix(newline + 1);
  }
  result.append(style.closing);
  return result;
}

std::string thrift_type_name(const t_type& type, const t_program& program) {
  if (const auto* tbase = type.try_as<t_primitive_type>()) {
    return tbase->name();
  }
  if (const auto* tmap = type.try_as<t_map>()) {
    return "map<" + thrift_type_name(tmap->key_type().deref(), program) + ", " +
        thrift_type_name(tmap->val_type().deref(), program) + ">";
  }
  if (const auto* tset = type.try_as<t_set>()) {
    return "set<" + thrift_type_name(tset->elem_type().deref(), program) + ">";
  }
  if (const auto* tlist = type.try_as<t_list>()) {
    return "list<" + thrift_type_name(tlist->elem_type().deref(), program) +
        ">";
  }
  const t_program* type_program = type.program();
  if (type_program != nullptr && type_program != &program) {
    return type_program->name() + "." + type.name();
  }
  return type.name();
}

std::string thrift_definition(const t_named& node, const t_program& program) {
  if (const auto* function = dynamic_cast<const t_function*>(&node)) {
    return function_definition(*function, program);
  }
  if (const auto* field = dynamic_cast<const t_field*>(&node)) {
    return field_definition(*field, program);
  }
  if (const auto* constant = dynamic_cast<const t_const*>(&node)) {
    return fmt::format(
        "const {} {}",
        thrift_type_name(constant->type_ref().deref(), program),
        constant->name());
  }
  if (const auto* typedf = dynamic_cast<const t_typedef*>(&node)) {
    return fmt::format(
        "typedef {} {}",
        thrift_type_name(typedf->type().deref(), program),
        typedf->name());
  }
  if (const auto* value = dynamic_cast<const t_enum_value*>(&node)) {
    return fmt::format("{} = {}", value->name(), value->get_value());
  }
  if (dynamic_cast<const t_interaction*>(&node) != nullptr) {
    return "interaction " + node.name();
  }
  if (const auto* service = dynamic_cast<const t_service*>(&node)) {
    if (const t_service* base = service->extends()) {
      return fmt::format(
          "service {} extends {}",
          service->name(),
          thrift_type_name(*base, program));
    }
    return "service " + service->name();
  }
  std::string_view kind;
  if (dynamic_cast<const t_struct*>(&node) != nullptr) {
    kind = "struct";
  } else if (dynamic_cast<const t_union*>(&node) != nullptr) {
    kind = "union";
  } else if (dynamic_cast<const t_exception*>(&node) != nullptr) {
    kind = "exception";
  } else if (dynamic_cast<const t_enum*>(&node) != nullptr) {
    kind = "enum";
  } else {
    return "";
  }
  return fmt::format("{} {}", kind, node.name());
}

std::string docblock_text(
    const t_named& node, const t_program& program, bool with_definition) {
  std::string text = node.has_doc() ? node.doc() : "";
  const size_t doc_end = text.find_last_not_of('\n');
  text.erase(doc_end == std::string::npos ? 0 : doc_end + 1);
  if (!with_definition) {
    return text;
  }
  const std::string definition = thrift_definition(node, program);
  if (definition.empty()) {
    return text;
  }
  return text.empty() ? definition : text + "\n\n" + definition;
}

void t_generator::process_options(
    const std::map<std::string, std::string>& options,
    std::string out_path,
    bool add_gen_dir) {
  std::filesystem::path path = {out_path};
  if (!out_path.empty() && out_path.back() != '/' && out_path.back() != '\\') {
    path += std::filesystem::path::preferred_separator;
  }
  out_path_ = path.make_preferred().string();
  add_gen_dir_ = add_gen_dir;
  process_options(options);
}

generator_factory::generator_factory(
    std::string name, std::string long_name, std::string_view documentation)
    : name_(std::move(name)),
      long_name_(std::move(long_name)),
      documentation_(normalize_documentation(documentation)) {
  generator_registry::register_generator(name_, this);
}

void generator_registry::register_generator(
    const std::string& name, generator_factory* factory) {
  if (!get_generators().insert({name, factory}).second) {
    throw std::logic_error(fmt::format("duplicate generator \"{}\"", name));
  }
}

std::unique_ptr<t_generator> generator_registry::make_generator(
    const std::string& name,
    t_program& p,
    t_program_bundle& pb,
    diagnostics_engine& diags) {
  generator_map& map = get_generators();
  auto iter = map.find(name);
  return iter != map.end() ? iter->second->make_generator(p, pb, diags)
                           : nullptr;
}

generator_registry::generator_map& generator_registry::get_generators() {
  // http://www.parashift.com/c++-faq-lite/ctors.html#faq-10.12
  static generator_map* map = new generator_map();
  return *map;
}

} // namespace apache::thrift::compiler
