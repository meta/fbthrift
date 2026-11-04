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

#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_set>

#include <thrift/compiler/sema/ast_validator.h>

namespace apache::thrift::compiler {

class t_named;
class t_program;
class t_program_bundle;
class t_type;

struct docblock_style {
  std::string_view opening;
  std::string_view line_prefix;
  std::string_view closing;
  // Would end the docblock early if it appeared in the text, so it is replaced
  // with `escaped_terminator`.
  std::string_view terminator{};
  std::string_view escaped_terminator{};
  // Would start a comment nested in the docblock, which compilers reject under
  // `-Werror,-Wcomment`, so it is replaced with `escaped_nested_opener`.
  std::string_view nested_opener{};
  std::string_view escaped_nested_opener{};
  // Delimit a docblock rendered on a single line, e.g. `/** text */`.
  std::string_view inline_opening;
  std::string_view inline_closing;

  static docblock_style c_style();
  static docblock_style python_style();
  static docblock_style triple_slash_style();
  static docblock_style hash_style();
};

// Renders `text` as a docblock in `style`. Trailing whitespace is trimmed from
// every line, so a blank line renders as the bare prefix (` *`, `#`).
std::string apply_docblock_style(
    std::string_view text, const docblock_style& style);

// Like apply_docblock_style, but renders a one-line `text` on a single line.
std::string apply_compact_docblock_style(
    std::string_view text, const docblock_style& style);

// Returns the name of `type` as written in Thrift IDL, qualifying types defined
// outside of `program` with the name of their program.
std::string thrift_type_name(const t_type& type, const t_program& program);

// Returns `node`'s definition as Thrift IDL in `program`, without a body or
// value, e.g. `1: optional i32 foo` for a field. Empty for a node that is not a
// definition.
std::string thrift_definition(const t_named& node, const t_program& program);

// Returns the language-independent text of the docblock that documents `node`
// in code generated for `program`, to render with apply_docblock_style: its
// doc comment, followed by its thrift_definition if `with_definition`. Pass
// `with_definition` when the generated code names or types `node` differently
// from the IDL, so the docblock shows what it was generated from. A
// `code_fence` encloses the definition in a Markdown code block of that
// language, for docblocks rendered as Markdown.
std::string docblock_text(
    const t_named& node,
    const t_program& program,
    bool with_definition,
    std::string_view code_fence = {});

class t_generator {
 public:
  t_generator(t_program& p, t_program_bundle& pb, diagnostics_engine& diags)
      : program_(&p),
        source_mgr_(diags.source_mgr()),
        diags_(diags),
        program_bundle_(pb) {}
  virtual ~t_generator() = default;

  // Processes generator options.
  // out_path: A path to the output directory.
  // add_gen_dir: Specifies whether to add a generator-specific subdirectory
  //              (usually gen-<name>) to the output directory.
  void process_options(
      const std::map<std::string, std::string>& options,
      std::string out_path,
      bool add_gen_dir);

  bool add_gen_dir() const { return add_gen_dir_; }

  /**
   * Called before generate_program to process options.
   *
   * @throws std::exception if the given options are invalid.
   */
  virtual void process_options(const std::map<std::string, std::string>&) {}

  virtual void fill_validator_visitors(ast_validator&) const {}

  // Generate the program. Overridden by subclasses to implement program
  // generation.
  virtual void generate_program() = 0;

  virtual std::optional<docblock_style> get_docblock_style() const {
    return std::nullopt;
  }

  const t_program* get_program() const { return program_; }

  t_program* get_program() { return program_; }

  std::unordered_set<std::string> get_genfiles() { return generated_files_; }

  void record_genfile(const std::string& filename) {
    generated_files_.insert(filename);
  }
  void record_genfile(const std::filesystem::path& filename) {
    generated_files_.insert(filename.string());
  }

  /**
   * Get the current output directory
   */
  virtual std::string get_out_dir() const { return get_out_path().string(); }
  virtual std::filesystem::path get_out_path() const {
    auto path = std::filesystem::path{out_path_};
    if (add_gen_dir_) {
      path /= out_dir_base_;
    }
    path += std::filesystem::path::preferred_separator;
    return path;
  }

 protected:
  t_program* program_;

  source_manager& source_mgr_;
  diagnostics_engine& diags_;

  t_program_bundle& program_bundle_;

  /**
   * Output type-specific directory name ("gen-*").
   */
  std::string out_dir_base_;

  /**
   * The set of files generated by this generator.
   */
  std::unordered_set<std::string> generated_files_;

 private:
  std::string out_path_;
  bool add_gen_dir_ = false;
};

/**
 * A factory for producing generator classes of a particular language.
 *
 * This class is also responsible for:
 *  - Registering itself with the generator registry.
 *  - Providing documentation for the generators it produces.
 */
class generator_factory {
 public:
  generator_factory(
      std::string name, std::string long_name, std::string_view documentation);

  virtual ~generator_factory() = default;

  // Creates a generator for the specified program.
  virtual std::unique_ptr<t_generator> make_generator(
      t_program& p, t_program_bundle& pb, diagnostics_engine& diags) = 0;

  const std::string& name() const { return name_; }
  const std::string& long_name() const { return long_name_; }
  const std::string& documentation() const { return documentation_; }

 private:
  std::string name_;
  std::string long_name_;
  std::string documentation_;
};

namespace detail {
template <typename Generator>
class generator_factory_impl : public generator_factory {
 public:
  using generator_factory::generator_factory;

  std::unique_ptr<t_generator> make_generator(
      t_program& p, t_program_bundle& pb, diagnostics_engine& diags) override {
    return std::unique_ptr<t_generator>(new Generator(p, pb, diags));
  }
};
} // namespace detail

namespace generator_registry {

void register_generator(const std::string& name, generator_factory* factory);

std::unique_ptr<t_generator> make_generator(
    const std::string& name,
    t_program& p,
    t_program_bundle& pb,
    diagnostics_engine& diags);

// A map from generator names to factories.
using generator_map = std::map<std::string, generator_factory*>;
generator_map& get_generators();

} // namespace generator_registry

#define THRIFT_REGISTER_GENERATOR(name, long_name, doc)                   \
  static detail::generator_factory_impl<t_##name##_generator> registerer( \
      #name, long_name, doc)

} // namespace apache::thrift::compiler
