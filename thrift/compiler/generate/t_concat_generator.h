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

#ifndef T_CONCAT_GENERATOR_H
#define T_CONCAT_GENERATOR_H

#include <thrift/compiler/generate/t_generator.h>

#include <thrift/compiler/ast/t_program.h>

namespace apache::thrift::compiler {

/**
 * String concatenation based generator.
 * This is the older way of generating code, new generators should use
 * a template-based approach. See t_whisker_generator.h.
 */
class t_concat_generator : public t_generator {
 public:
  using t_generator::t_generator;

  /**
   * Framework generator method that iterates over all the parts of a program
   * and performs general actions. This is implemented by the base class and
   * should not normally be overwritten in the subclasses.
   */
  void generate_program() override;

  /**
   * @param trim_trailing_whitespace strips trailing whitespace from each
   *     emitted line, so that a blank line in `contents` yields a bare
   *     `line_prefix` rather than one padded to its full width.
   */
  void generate_docstring_comment(
      std::ostream& out,
      const std::string& comment_start,
      const std::string& line_prefix,
      const std::string& contents,
      const std::string& comment_end,
      bool trim_trailing_whitespace = false);

  /**
   * Escape string to use one in generated sources.
   */
  std::string escape_string(const std::string& in) const;

  std::string get_escaped_string(const t_const_value* constval) {
    return escape_string(constval->get_string());
  }

 protected:
  /**
   * Optional methods that may be imlemented by subclasses to take necessary
   * steps at the beginning or end of code generation.
   */

  virtual void init_generator() {}
  virtual void close_generator() {}

  virtual void generate_consts(std::vector<t_const*> consts);

  /**
   * Pure virtual methods implemented by the generator subclasses.
   */

  virtual void generate_typedef(const t_typedef* ttypedef) = 0;
  virtual void generate_enum(const t_enum* tenum) = 0;
  virtual void generate_const(const t_const* /*tconst*/) {}
  virtual void generate_forward_declaration(const t_structured* /*tstruct*/) {}
  virtual void generate_struct(const t_structured* tstruct) = 0;
  virtual void generate_service(const t_service* tservice) = 0;
  virtual void generate_xception(const t_structured* txception) {
    // By default exceptions are the same as structs
    generate_struct(txception);
  }

  /**
   * Method to get the service name, may be overridden
   */
  virtual std::string get_service_name(const t_service* tservice) {
    return tservice->name();
  }

  /**
   * Creates a unique temporary variable name, which is just "name" with a
   * number appended to it (i.e. name35)
   */
  std::string tmp(const std::string& name) {
    std::ostringstream out;
    out << name << tmp_++;
    return out.str();
  }

  /**
   * Get current indentation level.
   */
  int get_indent() const { return indent_; }

  /**
   * Indentation level modifiers
   */

  void indent_up() { ++indent_; }

  void indent_down() { --indent_; }

  /**
   * Get number of spaces to use for each indentation level.
   */
  virtual int get_indent_size() const { return 2; }

  /**
   * Get line length.
   */
  virtual int get_line_length() const { return 80; }

  /**
   * Indent by these many levels.
   */
  std::string indent(int levels) {
    return std::string(levels * get_indent_size(), ' ');
  }

  /**
   * Indentation print function
   */
  std::string indent() { return indent(get_indent()); }

  /**
   * Indentation utility wrapper
   */
  std::ostream& indent(std::ostream& os) { return os << indent(); }

  /**
   * Scoping, using curly braces!
   */
  void scope_up(std::ostream& out) {
    indent(out) << "{\n";
    indent_up();
  }

  void scope_down(std::ostream& out) {
    indent_down();
    indent(out) << "}\n";
  }

  /**
   * Capitalization helpers
   */
  std::string capitalize(std::string in) {
    in[0] = toupper(in[0]);
    return in;
  }
  std::string decapitalize(std::string in) {
    in[0] = tolower(in[0]);
    return in;
  }
  static std::string lowercase(std::string in) {
    for (size_t i = 0; i < in.size(); ++i) {
      in[i] = tolower(in[i]);
    }
    return in;
  }
  std::string uppercase(std::string in) {
    for (size_t i = 0; i < in.size(); ++i) {
      in[i] = toupper(in[i]);
    }
    return in;
  }
  /**
   * Transforms a camel case string to an equivalent one separated by
   * underscores
   * e.g. aMultiWord -> a_multi_word
   *      someName   -> some_name
   *      CamelCase  -> camel_case
   *      name       -> name
   *      Name       -> name
   */
  std::string underscore(std::string in) {
    in[0] = tolower(in[0]);
    for (size_t i = 1; i < in.size(); ++i) {
      if (isupper(in[i])) {
        in[i] = tolower(in[i]);
        in.insert(i, "_");
      }
    }
    return in;
  }
  /**
   * Transforms a string with words separated by underscores to a camel case
   * equivalent. By default, the first letter of the input string is not
   * capitlized.
   * e.g. a_multi_word -> aMultiWord
   *      some_name    ->  someName
   *      name         ->  name
   */
  std::string camelcase(std::string in, bool capitalize_first = false) {
    std::ostringstream out;
    bool underscore = false;

    if (capitalize_first) {
      in = capitalize(in);
    }

    for (size_t i = 0; i < in.size(); i++) {
      if (in[i] == '_') {
        underscore = true;
        continue;
      }
      if (underscore) {
        out << (char)toupper(in[i]);
        underscore = false;
        continue;
      }
      out << in[i];
    }

    return out.str();
  }

  /**
   * Generates a comment about this code being autogenerated, using C++ style
   * comments, which are also fair game in Java / PHP, yay!
   *
   * @return C-style comment mentioning that this file is autogenerated.
   */
  static std::string autogen_comment() {
    return std::string("/**\n") + " * Autogenerated by Thrift\n" + " *\n" +
        " * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE "
        "DOING\n" +
        " *  @"
        "generated\n" +
        " */\n";
  }

 protected:
  /**
   * Quick accessor for formatted service name that is currently being
   * generated.
   */
  std::string service_name_;

  /**
   * Map of characters to escape in string literals.
   */
  std::map<char, std::string> escape_ = {
      {'\n', "\\n"},
      {'\r', "\\r"},
      {'\t', "\\t"},
      {'"', "\\\""},
      {'\\', "\\\\"}};

  /**
   * Thrift AST nodes are meant to be non-copyable and non-movable, and should
   * not be cloned. This method exists for legacy reasons, as some older
   * concatentation based generators generate ephemeral structs for RPC
   * responses and need to copy exception fields from the parsed AST.
   */
  void legacy_copy_exception_fields(
      const t_throws& source, t_structured& destination) const {
    for (const t_field& field : source.fields()) {
      t_field& clone =
          destination.create_field(field.type(), field.name(), field.id());
      clone.set_src_range(field.src_range());
      if (field.default_value()) {
        clone.set_default_value(field.default_value()->clone());
      }
      // structured annotations
      for (const auto& annot : field.structured_annotations()) {
        clone.add_structured_annotation(annot.clone());
      }
      clone.set_qualifier(field.qualifier());
    }
  }

 private:
  /**
   * Current code indentation level
   */
  int indent_ = 0;

  /**
   * Temporary variable counter, for making unique variable names
   */
  int tmp_ = 0;
};

} // namespace apache::thrift::compiler

#endif // T_CONCAT_GENERATOR_H
