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

#include <cstddef>
#include <deque>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <memory_resource>
#include <set>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <folly/FBString.h>
#include <folly/Traits.h>
#include <folly/container/F14Map-fwd.h>
#include <folly/container/F14Set-fwd.h>
#include <folly/container/FBVector.h>
#include <folly/container/sorted_vector_types.h>

namespace apache::thrift::pmr {

/**
 * Maps a C++ type to one that allocates all of its memory, including that of
 * its elements, through std::pmr::polymorphic_allocator. @cpp.Arena gives a
 * string, binary or container field the type `Rebind<T>::type`, where T is
 * the field's type without @cpp.Arena.
 *
 * - std::basic_string, std::vector, std::deque, std::list and the std ordered
 *   and unordered sets and maps map to their std::pmr counterparts, with their
 *   element types rebound.
 * - folly::fbvector, the F14 sets and maps, folly::sorted_vector_set and
 *   folly::sorted_vector_map keep their template and get
 *   std::pmr::polymorphic_allocator and rebound element types. A sorted
 *   vector's container becomes a std::pmr::vector.
 *   folly::basic_fbstring ignores allocators, so it maps to std::basic_string
 *   with std::pmr::polymorphic_allocator: folly::fbstring becomes
 *   std::pmr::string.
 * - std::less, std::greater, std::hash, std::equal_to,
 *   folly::f14::DefaultHasher and folly::f14::DefaultKeyEqual of the key type
 *   become those of the rebound key type; other comparators and hashers are
 *   kept, so they must accept the rebound key type.
 * - Any other specialization of a class template whose parameters are all
 *   types, such as std::unique_ptr and std::shared_ptr, fails to build.
 *   Specialize Rebind to use such a class template with @cpp.Arena: map it to
 *   a type that uses std::pmr::polymorphic_allocator and rebinds its element
 *   types through Rebind, so that nested values use the same memory resource,
 *   or to itself if it never allocates memory.
 * - Every other type maps to itself. This template does not inspect T, so T
 *   may be incomplete, e.g. a struct holding a container of itself.
 *
 * A user specialization must be declared before any use of Rebind for its
 * type, in a header that the .thrift file includes with cpp_include.
 * Otherwise a non-template class silently maps to itself in the translation
 * units that don't see the specialization.
 */
template <class T>
struct Rebind {
  using type = T;
};

namespace detail {

template <class T>
using rebind_t = typename Rebind<T>::type;

// The comparator, hasher or key-equality functor F of a container with key
// type K, for the rebound key type.
template <class F, class K>
struct RebindKeyFunctor {
  using type = F;
};
template <class K>
struct RebindKeyFunctor<std::less<K>, K> {
  using type = std::less<rebind_t<K>>;
};
template <class K>
struct RebindKeyFunctor<std::greater<K>, K> {
  using type = std::greater<rebind_t<K>>;
};
template <class K>
struct RebindKeyFunctor<std::hash<K>, K> {
  using type = std::hash<rebind_t<K>>;
};
template <class K>
struct RebindKeyFunctor<std::equal_to<K>, K> {
  using type = std::equal_to<rebind_t<K>>;
};
template <class K>
struct RebindKeyFunctor<folly::f14::DefaultHasher<K>, K> {
  using type = folly::f14::DefaultHasher<rebind_t<K>>;
};
template <class K>
struct RebindKeyFunctor<folly::f14::DefaultKeyEqual<K>, K> {
  using type = folly::f14::DefaultKeyEqual<rebind_t<K>>;
};

template <class F, class K>
using rebind_key_functor_t = typename RebindKeyFunctor<F, K>::type;

} // namespace detail

template <template <class...> class C, class... Ts>
struct Rebind<C<Ts...>> {
  static_assert(
      folly::always_false<C<Ts...>>,
      "apache::thrift::pmr::Rebind has no specialization for this class "
      "template. Specialize apache::thrift::pmr::Rebind for it, mapping it to "
      "a type that uses std::pmr::polymorphic_allocator and whose element "
      "types are rebound through Rebind, or to itself if it never allocates "
      "memory.");
  // Only so that uses of `type` add no errors after the static_assert.
  using type = C<Ts...>;
};

template <class C, class Traits, class A>
struct Rebind<std::basic_string<C, Traits, A>> {
  using type = std::basic_string<C, Traits, std::pmr::polymorphic_allocator<C>>;
};

template <class T, class A>
struct Rebind<std::vector<T, A>> {
  using type = std::pmr::vector<detail::rebind_t<T>>;
};

template <class T, class A>
struct Rebind<std::deque<T, A>> {
  using type = std::pmr::deque<detail::rebind_t<T>>;
};

template <class T, class A>
struct Rebind<std::list<T, A>> {
  using type = std::pmr::list<detail::rebind_t<T>>;
};

template <class K, class C, class A>
struct Rebind<std::set<K, C, A>> {
  using type =
      std::pmr::set<detail::rebind_t<K>, detail::rebind_key_functor_t<C, K>>;
};

template <class K, class C, class A>
struct Rebind<std::multiset<K, C, A>> {
  using type = std::pmr::
      multiset<detail::rebind_t<K>, detail::rebind_key_functor_t<C, K>>;
};

template <class K, class V, class C, class A>
struct Rebind<std::map<K, V, C, A>> {
  using type = std::pmr::map<
      detail::rebind_t<K>,
      detail::rebind_t<V>,
      detail::rebind_key_functor_t<C, K>>;
};

template <class K, class V, class C, class A>
struct Rebind<std::multimap<K, V, C, A>> {
  using type = std::pmr::multimap<
      detail::rebind_t<K>,
      detail::rebind_t<V>,
      detail::rebind_key_functor_t<C, K>>;
};

template <class K, class H, class E, class A>
struct Rebind<std::unordered_set<K, H, E, A>> {
  using type = std::pmr::unordered_set<
      detail::rebind_t<K>,
      detail::rebind_key_functor_t<H, K>,
      detail::rebind_key_functor_t<E, K>>;
};

template <class K, class H, class E, class A>
struct Rebind<std::unordered_multiset<K, H, E, A>> {
  using type = std::pmr::unordered_multiset<
      detail::rebind_t<K>,
      detail::rebind_key_functor_t<H, K>,
      detail::rebind_key_functor_t<E, K>>;
};

template <class K, class V, class H, class E, class A>
struct Rebind<std::unordered_map<K, V, H, E, A>> {
  using type = std::pmr::unordered_map<
      detail::rebind_t<K>,
      detail::rebind_t<V>,
      detail::rebind_key_functor_t<H, K>,
      detail::rebind_key_functor_t<E, K>>;
};

template <class K, class V, class H, class E, class A>
struct Rebind<std::unordered_multimap<K, V, H, E, A>> {
  using type = std::pmr::unordered_multimap<
      detail::rebind_t<K>,
      detail::rebind_t<V>,
      detail::rebind_key_functor_t<H, K>,
      detail::rebind_key_functor_t<E, K>>;
};

template <class C, class Traits, class A, class Storage>
struct Rebind<folly::basic_fbstring<C, Traits, A, Storage>> {
  using type = std::basic_string<C, Traits, std::pmr::polymorphic_allocator<C>>;
};

// With an allocator other than std::allocator, fbvector copies copyable
// elements instead of moving them when it grows. Each reallocation then also
// copies every nested string and container, and on an arena the memory of the
// old copies is not reclaimed until the resource is released.
template <class T, class A>
struct Rebind<folly::fbvector<T, A>> {
  using type = folly::fbvector<
      detail::rebind_t<T>,
      std::pmr::polymorphic_allocator<detail::rebind_t<T>>>;
};

namespace detail {

template <template <class...> class Set, class K, class H, class E>
struct RebindF14Set {
  using type =
      Set<rebind_t<K>,
          rebind_key_functor_t<H, K>,
          rebind_key_functor_t<E, K>,
          std::pmr::polymorphic_allocator<rebind_t<K>>>;
};

template <template <class...> class Map, class K, class V, class H, class E>
struct RebindF14Map {
  using type =
      Map<rebind_t<K>,
          rebind_t<V>,
          rebind_key_functor_t<H, K>,
          rebind_key_functor_t<E, K>,
          std::pmr::polymorphic_allocator<
              std::pair<const rebind_t<K>, rebind_t<V>>>>;
};

} // namespace detail

template <class K, class H, class E, class A>
struct Rebind<folly::F14NodeSet<K, H, E, A>>
    : detail::RebindF14Set<folly::F14NodeSet, K, H, E> {};

template <class K, class H, class E, class A>
struct Rebind<folly::F14ValueSet<K, H, E, A>>
    : detail::RebindF14Set<folly::F14ValueSet, K, H, E> {};

template <class K, class H, class E, class A>
struct Rebind<folly::F14VectorSet<K, H, E, A>>
    : detail::RebindF14Set<folly::F14VectorSet, K, H, E> {};

template <class K, class H, class E, class A>
struct Rebind<folly::F14FastSet<K, H, E, A>>
    : detail::RebindF14Set<folly::F14FastSet, K, H, E> {};

template <class K, class V, class H, class E, class A>
struct Rebind<folly::F14NodeMap<K, V, H, E, A>>
    : detail::RebindF14Map<folly::F14NodeMap, K, V, H, E> {};

template <class K, class V, class H, class E, class A>
struct Rebind<folly::F14ValueMap<K, V, H, E, A>>
    : detail::RebindF14Map<folly::F14ValueMap, K, V, H, E> {};

template <class K, class V, class H, class E, class A>
struct Rebind<folly::F14VectorMap<K, V, H, E, A>>
    : detail::RebindF14Map<folly::F14VectorMap, K, V, H, E> {};

template <class K, class V, class H, class E, class A>
struct Rebind<folly::F14FastMap<K, V, H, E, A>>
    : detail::RebindF14Map<folly::F14FastMap, K, V, H, E> {};

template <class K, class C, class A, class G, class Container>
struct Rebind<folly::sorted_vector_set<K, C, A, G, Container>> {
  using type = folly::sorted_vector_set<
      detail::rebind_t<K>,
      detail::rebind_key_functor_t<C, K>,
      std::pmr::polymorphic_allocator<detail::rebind_t<K>>,
      G,
      std::pmr::vector<detail::rebind_t<K>>>;
};

template <class K, class V, class C, class A, class G, class Container>
struct Rebind<folly::sorted_vector_map<K, V, C, A, G, Container>> {
  using type = folly::sorted_vector_map<
      detail::rebind_t<K>,
      detail::rebind_t<V>,
      detail::rebind_key_functor_t<C, K>,
      std::pmr::polymorphic_allocator<
          std::pair<detail::rebind_t<K>, detail::rebind_t<V>>>,
      G,
      std::pmr::vector<std::pair<detail::rebind_t<K>, detail::rebind_t<V>>>>;
};

/**
 * Whether destroying a T, apart from destroying its elements, only frees
 * memory through its allocator. T is a type that Rebind maps to, such as
 * std::pmr::vector<std::pmr::string>. Specialize it for such a type so that
 * Thrift may skip destroying it on an arena.
 */
template <class T>
struct SideEffectFreeDtor : std::false_type {};

namespace detail {

// Base of the specializations Thrift ships. Thrift trusts them, so they bypass
// the check that user specializations are subject to.
struct ShippedSideEffectFreeDtor : std::true_type {
  static constexpr bool bypass_constexpr_dtor_check_internal_do_not_use = true;
};

} // namespace detail

template <class C, class Traits, class A>
struct SideEffectFreeDtor<std::basic_string<C, Traits, A>>
    : detail::ShippedSideEffectFreeDtor {};

template <class T, class A>
struct SideEffectFreeDtor<std::vector<T, A>>
    : detail::ShippedSideEffectFreeDtor {};

template <class T, class A>
struct SideEffectFreeDtor<std::deque<T, A>>
    : detail::ShippedSideEffectFreeDtor {};

template <class T, class A>
struct SideEffectFreeDtor<std::list<T, A>> : detail::ShippedSideEffectFreeDtor {
};

template <class K, class C, class A>
struct SideEffectFreeDtor<std::set<K, C, A>>
    : detail::ShippedSideEffectFreeDtor {};

template <class K, class C, class A>
struct SideEffectFreeDtor<std::multiset<K, C, A>>
    : detail::ShippedSideEffectFreeDtor {};

template <class K, class V, class C, class A>
struct SideEffectFreeDtor<std::map<K, V, C, A>>
    : detail::ShippedSideEffectFreeDtor {};

template <class K, class V, class C, class A>
struct SideEffectFreeDtor<std::multimap<K, V, C, A>>
    : detail::ShippedSideEffectFreeDtor {};

template <class K, class H, class E, class A>
struct SideEffectFreeDtor<std::unordered_set<K, H, E, A>>
    : detail::ShippedSideEffectFreeDtor {};

template <class K, class H, class E, class A>
struct SideEffectFreeDtor<std::unordered_multiset<K, H, E, A>>
    : detail::ShippedSideEffectFreeDtor {};

template <class K, class V, class H, class E, class A>
struct SideEffectFreeDtor<std::unordered_map<K, V, H, E, A>>
    : detail::ShippedSideEffectFreeDtor {};

template <class K, class V, class H, class E, class A>
struct SideEffectFreeDtor<std::unordered_multimap<K, V, H, E, A>>
    : detail::ShippedSideEffectFreeDtor {};

template <class T, class A>
struct SideEffectFreeDtor<folly::fbvector<T, A>>
    : detail::ShippedSideEffectFreeDtor {};

template <class K, class H, class E, class A>
struct SideEffectFreeDtor<folly::F14NodeSet<K, H, E, A>>
    : detail::ShippedSideEffectFreeDtor {};

template <class K, class H, class E, class A>
struct SideEffectFreeDtor<folly::F14ValueSet<K, H, E, A>>
    : detail::ShippedSideEffectFreeDtor {};

template <class K, class H, class E, class A>
struct SideEffectFreeDtor<folly::F14VectorSet<K, H, E, A>>
    : detail::ShippedSideEffectFreeDtor {};

template <class K, class H, class E, class A>
struct SideEffectFreeDtor<folly::F14FastSet<K, H, E, A>>
    : detail::ShippedSideEffectFreeDtor {};

template <class K, class V, class H, class E, class A>
struct SideEffectFreeDtor<folly::F14NodeMap<K, V, H, E, A>>
    : detail::ShippedSideEffectFreeDtor {};

template <class K, class V, class H, class E, class A>
struct SideEffectFreeDtor<folly::F14ValueMap<K, V, H, E, A>>
    : detail::ShippedSideEffectFreeDtor {};

template <class K, class V, class H, class E, class A>
struct SideEffectFreeDtor<folly::F14VectorMap<K, V, H, E, A>>
    : detail::ShippedSideEffectFreeDtor {};

template <class K, class V, class H, class E, class A>
struct SideEffectFreeDtor<folly::F14FastMap<K, V, H, E, A>>
    : detail::ShippedSideEffectFreeDtor {};

template <class K, class C, class A, class G, class Container>
struct SideEffectFreeDtor<folly::sorted_vector_set<K, C, A, G, Container>>
    : detail::ShippedSideEffectFreeDtor {};

template <class K, class V, class C, class A, class G, class Container>
struct SideEffectFreeDtor<folly::sorted_vector_map<K, V, C, A, G, Container>>
    : detail::ShippedSideEffectFreeDtor {};

} // namespace apache::thrift::pmr
