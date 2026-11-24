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

// Type tags for describing the 'shape' of thrift types at compile-time.
//
// _t indicates a concrete type.
// _c indicates a class of types.
// no suffix means it is dependent on the parameters.
//
// For example, `type::list_c` represents a list of any type,
// `type::list<type::enum_c>` represents a list of any enum type, and
// `type::list<type::enum_t<MyEnum>>` represents a list of MyEnums.
#pragma once

#include <cstdint>
#include <type_traits>

namespace apache::thrift::type {

enum class FieldId : ::std::int16_t;

// Classes of types (_c suffix).
struct all_c {}; // all thrift types
struct primitive_c : all_c {}; // all number and string types
struct number_c : primitive_c {}; // all number types, incl. 'bool' and 'enum'
struct integral_c : number_c {}; // all integral types, not `enum`
struct floating_point_c : number_c {}; // all floating point types
struct enum_c : number_c {}; // all `enum` types
struct string_c : primitive_c {}; // `binary` and `string`
struct structured_c : all_c {}; // all structured types, including 'union'
struct struct_except_c : structured_c {}; // all `struct` and `exception` types
struct struct_c : struct_except_c {}; // all `struct` types
struct exception_c : struct_except_c {}; // all `exception` types
struct union_c : structured_c {}; // all `union` types
struct container_c : all_c {}; // all container types.
struct list_c : container_c {}; // all `list` types
struct set_c : container_c {}; // all `set` types
struct map_c : container_c {}; // all `map` types
struct service_c {}; // all `service` types

// Type tags for types that are always concrete (_t suffix).
struct void_t {};
struct bool_t : integral_c {};
struct byte_t : integral_c {};
struct i16_t : integral_c {};
struct i32_t : integral_c {};
struct i64_t : integral_c {};
struct float_t : floating_point_c {};
struct double_t : floating_point_c {};
struct string_t : string_c {}; // an utf-8 string
struct binary_t : string_c {}; // an opaque binary string

template <typename T> // the generated C++ type
struct enum_t : enum_c {};
template <typename T> // the generated C++ type
struct struct_t : struct_c {};
template <typename T> // the generated C++ type
struct union_t : union_c {};
template <typename T> // the generated C++ type
struct exception_t : exception_c {};

// Parameterized types.
template <typename VTag>
struct list : list_c {
  using value_tag = VTag;
};
template <typename KTag>
struct set : set_c {
  using value_tag = KTag;
};
template <typename KTag, typename VTag>
struct map : map_c {
  using key_tag = KTag;
  using value_tag = VTag;
};

// Extra compile-time metadata.

// An adapted type.
template <
    typename Adapter, // the C++ adapter to use
    typename Tag> // the thrift type being adapted
struct adapted : Tag {};

// A type mapped to a specific C++ type.
//
// The given type must be a 'drop in' replacement for the
// native_type it is overriding.
template <
    typename T, // the C++ type to use
    typename Tag> // the thrift type being overridden
struct cpp_type : Tag {};

namespace detail {
template <typename T, typename Tag>
struct CppPmrType {
  using type = Tag;
};
template <typename T, typename U, typename Tag>
struct CppPmrType<T, cpp_type<U, Tag>>
    : std::conditional_t<
          std::is_same_v<T, U>,
          std::type_identity<cpp_type<U, Tag>>,
          CppPmrType<T, Tag>> {};
template <typename T>
struct CppPmrType<T, string_t> {
  using type = cpp_type<T, string_t>;
};
template <typename T>
struct CppPmrType<T, binary_t> {
  using type = cpp_type<T, binary_t>;
};
template <typename T, typename VTag>
struct CppPmrType<T, list<VTag>> {
  using type = cpp_type<
      T,
      list<typename CppPmrType<typename T::value_type, VTag>::type>>;
};
template <typename T, typename KTag>
struct CppPmrType<T, set<KTag>> {
  using type =
      cpp_type<T, set<typename CppPmrType<typename T::key_type, KTag>::type>>;
};
template <typename T, typename KTag, typename VTag>
struct CppPmrType<T, map<KTag, VTag>> {
  using type = cpp_type<
      T,
      map<typename CppPmrType<typename T::key_type, KTag>::type,
          typename CppPmrType<typename T::mapped_type, VTag>::type>>;
};
} // namespace detail

// The tag of a C++ type `T` of the thrift type `Tag` whose strings and
// containers at every level may differ from those of `Tag`, as with
// `@cpp.Arena`: each string, binary and container level is a `cpp_type` of the
// corresponding member type of `T` (`value_type`, `key_type`, `mapped_type`).
// A `cpp_type` level that already names that member type is kept as is.
template <typename T, typename Tag>
using cpp_pmr_type = typename detail::CppPmrType<T, Tag>::type;

template <typename T> // the generated C++ type
struct service_t : service_c {};

// TODO(ytj): ensure all arguments are field_t
template <class... Tags>
struct fields;

template <typename Tag, typename Context>
struct field : Tag {};

} // namespace apache::thrift::type
