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

#include <thrift/compiler/ast/t_field.h>
#include <thrift/compiler/ast/t_named.h>
#include <thrift/compiler/ast/t_type.h>

namespace apache::thrift::compiler::cpp2 {

// Whether the allocator-extended constructors must apply this field's default
// rather than value-initialize it. Implies the field declares a default, so a
// template may emit that default on this alone.
bool alloc_ctor_applies_custom_default(const t_field& field);

// The value of `cpp.allocator`; null if not set.
const std::string* find_allocator(const t_named& node);

inline bool is_allocator_aware(const t_named& node) {
  return find_allocator(node) != nullptr;
}

// The value of `cpp.allocator_via`; null if not set.
const std::string* find_allocator_via(const t_named& node);

// Whether `cpp.use_allocator` is set on the type or a typedef it goes through.
bool type_uses_allocator(const t_type& type);

// Whether `cpp.use_allocator` is set on the field or its (typedef) type.
bool uses_allocator(const t_field& field);

} // namespace apache::thrift::compiler::cpp2
