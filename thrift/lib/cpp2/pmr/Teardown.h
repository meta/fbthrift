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

#include <deque>
#include <list>
#include <map>
#include <memory>
#include <memory_resource>
#include <ranges>
#include <set>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <folly/Traits.h>
#include <folly/container/F14Map-fwd.h>
#include <folly/container/F14Set-fwd.h>
#include <folly/container/FBVector.h>
#include <folly/container/sorted_vector_types.h>

#include <thrift/lib/cpp2/FieldRef.h>
#include <thrift/lib/cpp2/FieldRefTraits.h>
#include <thrift/lib/cpp2/Thrift.h>
#include <thrift/lib/cpp2/op/Get.h>
#include <thrift/lib/cpp2/type/Tag.h>
#include <thrift/lib/cpp2/type/ThriftType.h>

namespace apache::thrift::pmr::detail {

// Don't specialize anything here: a wrong specialization leaks memory.

template <class T>
concept UsesPmrAllocator = folly::is_instantiation_of_v<
    std::pmr::polymorphic_allocator,
    typename T::allocator_type>;

template <class Tag>
inline constexpr bool kIsAdapted = false;
template <class Adapter, class Tag>
inline constexpr bool kIsAdapted<type::adapted<Adapter, Tag>> = true;

// False for `@cpp.Ref` and `@thrift.Box` fields, which hold a pointer.
template <class Ref>
inline constexpr bool kHoldsValue =
    apache::thrift::detail::is_field_ref_v<Ref> ||
    apache::thrift::detail::is_optional_field_ref_v<Ref> ||
    apache::thrift::detail::is_required_field_ref_v<Ref> ||
    apache::thrift::detail::is_terse_field_ref_v<Ref>;
template <class T>
inline constexpr bool kIsThriftClassContainerOrPrimitive =
    is_thrift_class_v<T> || std::ranges::range<T> || std::is_arithmetic_v<T> ||
    std::is_enum_v<T>;
// A union gives such a field the same ref type as other fields.
template <class Stored, class Allocator>
inline constexpr bool kHoldsValue<union_field_ref<Stored, Allocator>> =
    kIsThriftClassContainerOrPrimitive<std::remove_cvref_t<Stored>>;

template <class T, class Id, class... Seen>
consteval bool fieldAllocatesAllMemoryViaMemoryResource();

template <class Tag, class T, class... Seen>
consteval bool onResource();

template <class Tag, class T, class... Seen>
consteval bool listOnResource();

template <class Tag, class T, class... Seen>
consteval bool setOnResource();

template <class Tag, class T, class... Seen>
consteval bool mapOnResource();

// A struct nested in itself counts as true: the outer call checks its fields.
template <class T, class... Seen>
consteval bool structuredOnResource() {
  if constexpr ((std::is_same_v<T, Seen> || ...)) {
    return true;
  } else {
    bool result = true;
    op::for_each_field_id<T>([&]<class Id>(Id) {
      result = result &&
          fieldAllocatesAllMemoryViaMemoryResource<T, Id, T, Seen...>();
    });
    return result;
  }
}

// If true, destroying field `Id` of `T` on an arena only returns memory that
// the arena reclaims anyway.
template <class T, class Id, class... Seen>
consteval bool fieldAllocatesAllMemoryViaMemoryResource() {
  if constexpr (!kHoldsValue<op::get_field_ref<T, Id>>) {
    return false;
  } else if constexpr (apache::thrift::detail::qualifier::
                           is_field_using_allocator<T, Id>::value) {
    return onResource<
        op::get_type_tag<T, Id>,
        op::get_native_type<T, Id>,
        Seen...>();
  } else {
    return std::is_trivially_destructible_v<op::get_native_type<T, Id>>;
  }
}

template <class T, template <class...> class... Templates>
inline constexpr bool kIsAnyOf =
    (folly::is_instantiation_of_v<Templates, T> || ...);

// Only these std and folly strings and containers are trusted to free nothing
// but memory through their allocator.
template <class Tag, class T, class... Seen>
consteval bool onResource() {
  if constexpr (!UsesPmrAllocator<T>) {
    // A number, or a string, container or struct that doesn't take the memory
    // resource.
    return std::is_trivially_destructible_v<T>;
  } else if constexpr (kIsAdapted<Tag>) {
    // Adapters aren't supported yet.
    return false;
  } else if constexpr (type::is_a_v<Tag, type::structured_c>) {
    return structuredOnResource<T, Seen...>();
  } else if constexpr (type::is_a_v<Tag, type::list_c>) {
    return listOnResource<Tag, T, Seen...>();
  } else if constexpr (type::is_a_v<Tag, type::set_c>) {
    return setOnResource<Tag, T, Seen...>();
  } else if constexpr (type::is_a_v<Tag, type::map_c>) {
    return mapOnResource<Tag, T, Seen...>();
  } else {
    static_assert(type::is_a_v<Tag, type::string_c>, "Unknown type");
    return kIsAnyOf<T, std::basic_string>;
  }
}

// A sorted vector passes its allocator to its container without checking that
// the container uses it.
template <class K, class C, class A, class G, class Container>
consteval bool sortedVectorOnResource(
    folly::tag_t<folly::sorted_vector_set<K, C, A, G, Container>>) {
  return std::is_trivially_destructible_v<C> &&
      (std::is_void_v<G> || std::is_trivially_destructible_v<G>) &&
      std::is_same_v<Container, std::pmr::vector<K>>;
}
template <class K, class V, class C, class A, class G, class Container>
consteval bool sortedVectorOnResource(
    folly::tag_t<folly::sorted_vector_map<K, V, C, A, G, Container>>) {
  return std::is_trivially_destructible_v<C> &&
      (std::is_void_v<G> || std::is_trivially_destructible_v<G>) &&
      std::is_same_v<Container, std::pmr::vector<std::pair<K, V>>>;
}

template <class Tag, class T, class... Seen>
consteval bool listOnResource() {
  if (!onResource<typename Tag::value_tag, typename T::value_type, Seen...>()) {
    return false;
  }
  return kIsAnyOf<T, std::vector, std::deque, std::list, folly::fbvector>;
}

template <class Tag, class T, class... Seen>
consteval bool setOnResource() {
  if (!onResource<typename Tag::value_tag, typename T::value_type, Seen...>()) {
    return false;
  }
  if constexpr (kIsAnyOf<T, std::set, std::multiset>) {
    return std::is_trivially_destructible_v<typename T::key_compare>;
  } else if constexpr (kIsAnyOf<
                           T,
                           std::unordered_set,
                           std::unordered_multiset,
                           folly::F14NodeSet,
                           folly::F14ValueSet,
                           folly::F14VectorSet,
                           folly::F14FastSet>) {
    return std::is_trivially_destructible_v<typename T::hasher> &&
        std::is_trivially_destructible_v<typename T::key_equal>;
  } else if constexpr (kIsAnyOf<T, folly::sorted_vector_set>) {
    return sortedVectorOnResource(folly::tag<T>);
  } else {
    return false;
  }
}

template <class Tag, class T, class... Seen>
consteval bool mapOnResource() {
  if (!onResource<typename Tag::key_tag, typename T::key_type, Seen...>() ||
      !onResource<
          typename Tag::value_tag,
          typename T::mapped_type,
          Seen...>()) {
    return false;
  }
  if constexpr (kIsAnyOf<T, std::map, std::multimap>) {
    return std::is_trivially_destructible_v<typename T::key_compare>;
  } else if constexpr (kIsAnyOf<
                           T,
                           std::unordered_map,
                           std::unordered_multimap,
                           folly::F14NodeMap,
                           folly::F14ValueMap,
                           folly::F14VectorMap,
                           folly::F14FastMap>) {
    return std::is_trivially_destructible_v<typename T::hasher> &&
        std::is_trivially_destructible_v<typename T::key_equal>;
  } else if constexpr (kIsAnyOf<T, folly::sorted_vector_map>) {
    return sortedVectorOnResource(folly::tag<T>);
  } else {
    return false;
  }
}

// Move-assigns `src` to `dst`, field `Id` of `Struct`. If
// `canSkipDestroyingOldData` and the field allocates only from the struct's
// memory resource, move-constructs over the old value instead, leaving its
// memory for the arena to reclaim.
template <class Struct, class Id, class T>
void moveAssignField(T& dst, T&& src, bool canSkipDestroyingOldData) {
  if (canSkipDestroyingOldData &&
      fieldAllocatesAllMemoryViaMemoryResource<Struct, Id>()) {
    std::construct_at(std::addressof(dst), std::move(src));
  } else {
    dst = std::move(src);
  }
}

} // namespace apache::thrift::pmr::detail

namespace apache::thrift::pmr {

/**
 * True if every field of the Thrift struct or union `T` allocates memory only
 * from `T`'s memory resource, recursively. Then, on an arena (see
 * isArenaResource), the destructor of `T` skips all of its fields.
 */
template <class T>
  requires is_thrift_class_v<T>
inline constexpr bool allocate_all_memory_via_memory_resource_v =
    detail::structuredOnResource<T>();

} // namespace apache::thrift::pmr
