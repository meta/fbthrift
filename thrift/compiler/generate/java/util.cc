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

#include <thrift/compiler/generate/java/util.h>

#include <cassert>
#include <cctype>
#include <thrift/common/detail/string.h>
#include <thrift/compiler/ast/t_exception.h>
#include <thrift/compiler/ast/t_field.h>
#include <thrift/compiler/ast/t_structured.h>
#include <thrift/compiler/ast/t_typedef.h>
#include <thrift/compiler/ast/uri.h>
#include <thrift/compiler/sema/sema_context.h>

namespace apache::thrift::compiler::java {

std::string mangle_java_name(const std::string& ref, bool capitalize) {
  std::ostringstream res;
  // When we are capitalizing, initialize upCase to true in order to force first
  // character to upper case
  bool upcase = capitalize;
  // On the other hand, force first character to lowercase, unless both first
  // AND second characters are uppercase (this is so we don't mess up any
  // poorly-named method that might start with acronyms)
  bool acronym = ref.size() > 1 && std::isupper(ref[0]) && std::isupper(ref[1]);
  bool downcase = !capitalize && !acronym;
  for (typename std::string::size_type i = 0; i < ref.size(); ++i) {
    if (ref[i] == '_' && i < ref.size() - 1) {
      upcase = true;
      continue;
    } else {
      char ch = ref[i];
      if (downcase) {
        ch = std::tolower(ch);
      }
      if (upcase) {
        ch = std::toupper(ch);
      }
      res << ch;
      upcase = false;
      downcase = false;
    }
  }
  return res.str();
}

std::string mangle_java_constant_name(const std::string& ref) {
  std::ostringstream res;
  bool lowercase = false;
  for (typename std::string::size_type i = 0; i < ref.size(); ++i) {
    char ch = ref[i];
    if (std::isupper(ch)) {
      if (lowercase) {
        res << '_';
      }
      res << static_cast<char>(std::toupper(ch));
      lowercase = false;
    } else if (std::islower(ch)) {
      res << static_cast<char>(std::toupper(ch));
      lowercase = true;
    } else {
      // Not a letter, just emit it
      res << ch;
    }
  }
  return res.str();
}

std::string quote_java_string(const std::string& unescaped) {
  std::ostringstream quoted;
  quoted << '\"';
  for (unsigned char c : unescaped) {
    switch (c) {
      case '\\':
        quoted << "\\\\";
        break;
      case '"':
        quoted << "\\\"";
        break;
      case '\n':
        quoted << "\\n";
        break;
      default: {
        if (c < 0x20) {
          quoted << fmt::format("\\{:03o}", c);
        } else if (c >= 0xf8) {
          quoted << fmt::format("\\u{:04x}", c);
        } else {
          quoted << c;
        }
        break;
      }
    }
  }
  quoted << '\"';

  return quoted.str();
}

std::string package_to_path(std::string package) {
  if (package.find('/') != std::string::npos) {
    std::ostringstream err;
    err << "\"" << package << "\" is not a valid Java package name";
    throw std::runtime_error{err.str()};
  }
  apache::thrift::detail::replace_all(package, ".", "/");
  return package;
}

void validate_java_exception_cause_field(
    sema_context& ctx, const t_exception& ex) {
  for (const t_field& field : ex.fields()) {
    std::string java_name = field.get_unstructured_annotation(
        "java.swift.name", mangle_java_name(field.name(), false));
    if (mangle_java_name(java_name, true) == "Cause") {
      ctx.error(
          field,
          "Exception '{}' has field '{}' with Java name 'Cause', which collides with 'Throwable.getCause()', the generated '(..., Throwable cause)' constructor and 'Builder.setCause'; rename the field.",
          ex.name(),
          field.name());
    }
  }
}

const t_field* java_exception_cause_message_field(const t_structured& str) {
  const auto* ex = dynamic_cast<const t_exception*>(&str);
  if (ex == nullptr) {
    return nullptr;
  }
  const t_field* target = ex->get_message_field();
  if (target == nullptr) {
    target = ex->get_field_by_name("message");
  }
  if (target == nullptr) {
    return nullptr;
  }
  if (target->has_structured_annotation(kJavaWrapperUri) ||
      target->has_structured_annotation(kJavaAdapterUri)) {
    return nullptr;
  }
  const t_type* type = &target->type().deref();
  if (t_typedef::get_first_structured_annotation_or_null(
          type, kJavaAdapterUri) != nullptr ||
      t_typedef::get_first_structured_annotation_or_null(
          type, kJavaBinaryStringUri) != nullptr ||
      t_typedef::get_first_unstructured_annotation_or_null(
          type, {"java.swift.binary_string"}) != nullptr ||
      t_typedef::get_first_unstructured_annotation_or_null(
          type, {"java.swift.type"}) != nullptr) {
    return nullptr;
  }
  if (!type->get_true_type()->is_string()) {
    return nullptr;
  }
  return target;
}

} // namespace apache::thrift::compiler::java
