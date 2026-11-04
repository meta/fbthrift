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

#include <stdexcept>
#include <utility>

#include <fmt/core.h>

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
