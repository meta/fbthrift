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

#include <thrift/compiler/generate/cpp/allocator.h>

#include <thrift/compiler/ast/t_const.h>
#include <thrift/compiler/ast/t_typedef.h>
#include <thrift/compiler/ast/uri.h>
#include <thrift/compiler/generate/cpp/util.h>

namespace apache::thrift::compiler::cpp2 {

namespace {
bool declares_custom_default(const t_field& field) {
  return field.default_value() != nullptr;
}

const std::string* find_name_annotation(
    const t_named& node, const char* uri, const char* unstructured_name) {
  if (const t_const* annotation =
          node.find_structured_annotation_or_null(uri)) {
    const t_const_value* name =
        annotation->get_value_from_structured_annotation_or_null("name");
    return name != nullptr ? &name->get_string() : nullptr;
  }
  return node.find_unstructured_annotation_or_null(unstructured_name);
}
} // namespace

const std::string* find_allocator(const t_named& node) {
  if (const std::string* allocator =
          find_name_annotation(node, kCppAllocatorUri, "cpp.allocator")) {
    return allocator;
  }
  static const auto& kArenaAllocator =
      *new std::string("::std::pmr::polymorphic_allocator<::std::byte>");
  const auto* strct = dynamic_cast<const t_structured*>(&node);
  return strct != nullptr && has_arena_fields(*strct) ? &kArenaAllocator
                                                      : nullptr;
}

const std::string* find_allocator_via(const t_named& node) {
  return find_name_annotation(node, kCppAllocatorViaUri, "cpp.allocator_via");
}

bool type_uses_allocator(const t_type& type) {
  return t_typedef::get_first_structured_annotation_or_null(
             &type, kCppUseAllocatorUri) != nullptr ||
      t_typedef::get_first_unstructured_annotation_or_null(
          &type, {"cpp.use_allocator"}) != nullptr;
}

bool uses_allocator(const t_field& field) {
  return is_arena_field(field) ||
      field.has_structured_annotation(kCppUseAllocatorUri) ||
      field.has_unstructured_annotation("cpp.use_allocator") ||
      type_uses_allocator(field.type().deref());
}

bool alloc_ctor_applies_custom_default(const t_field& field) {
  if (!declares_custom_default(field)) {
    return false;
  }
  const t_const* anno = field.find_structured_annotation_or_null(
      kCppAllowCustomDefaultInAllocCtorUri);
  if (anno == nullptr) {
    return false;
  }
  for (const auto& kv : anno->value()->get_map()) {
    if (kv.first->get_string() == "legacy_ignore_custom_default") {
      return !kv.second->get_bool();
    }
  }
  return true;
}

} // namespace apache::thrift::compiler::cpp2
