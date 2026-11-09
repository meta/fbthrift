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

#include <algorithm>
#include <limits>
#include <memory_resource>
#include <scoped_allocator>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include <folly/Traits.h>
#include <folly/Utility.h>
#include <folly/container/Reserve.h>
#include <folly/container/View.h>
#include <folly/container/range_traits.h>
#include <folly/functional/Invoke.h>
#include <folly/lang/VectorTraits.h>
#include <folly/memory/UninitializedMemoryHacks.h>

#include <thrift/lib/cpp/protocol/TProtocolException.h>
#include <thrift/lib/cpp/protocol/TType.h>
#include <thrift/lib/cpp/util/EnumUtils.h>
#include <thrift/lib/cpp2/op/detail/AllocatorPropagation.h>
#include <thrift/lib/cpp2/op/detail/Compare.h>
#include <thrift/lib/cpp2/protocol/Protocol.h>

namespace apache::thrift::op::detail {

template <typename Protocol, typename T>
void readEnum(Protocol& protocol, T& out) {
  if constexpr (requires { protocol.readEnum(out); }) {
    protocol.readEnum(out);
  } else {
    std::int32_t value;
    protocol.readI32(value);
    out = static_cast<T>(value);
  }
}

template <typename Protocol>
inline constexpr bool writes_enum_names_v = requires(Protocol& protocol) {
  protocol.writeEnum(std::string_view{}, std::int32_t{});
};

// Shared with dynamic serialization, which looks names up at runtime.
// `name()` is called only for protocols that write enum names; an empty name
// means the enum does not define the value.
template <typename Protocol, typename NameFn>
std::size_t writeEnumValue(
    Protocol& protocol, std::int32_t value, NameFn&& name) {
  if constexpr (writes_enum_names_v<Protocol>) {
    return protocol.writeEnum(std::string_view{name()}, value);
  } else {
    return protocol.writeI32(value);
  }
}

template <typename Protocol, typename T>
std::size_t writeEnum(Protocol& protocol, const T& in) {
  // Generic so that `enumName` is instantiated only for protocols that write
  // enum names: enums that Thrift did not generate have no `TEnumTraits`.
  return writeEnumValue(protocol, static_cast<std::int32_t>(in), [&](auto...) {
    const char* name = apache::thrift::util::enumName(in);
    return name ? name : "";
  });
}

template <typename Protocol, typename T>
std::size_t serializedSizeEnum(Protocol& protocol, const T& in) {
  return protocol.serializedSizeI32(static_cast<std::int32_t>(in));
}

template <typename C, typename... A>
using detect_resize = decltype(FOLLY_DECLVAL(C).resize(FOLLY_DECLVAL(A)...));
template <typename C, typename... A>
using detect_resize_without_initialization =
    decltype(folly::resizeWithoutInitialization(
        FOLLY_DECLVAL(C&), FOLLY_DECLVAL(A)...));

// Whether allocator_traits<Alloc>::construct() passes an allocator to the
// element it builds: a scoped allocator hands over its inner allocator
// ([allocator.adaptor]), polymorphic_allocator hands over itself
// ([mem.poly.allocator.mem]). An allocator absent from this list leaves the
// element with a default-constructed one.
template <typename Alloc>
inline constexpr bool alloc_propagates_to_elements = false;
template <typename Outer, typename... Inner>
inline constexpr bool alloc_propagates_to_elements<
    std::scoped_allocator_adaptor<Outer, Inner...>> = true;
template <typename T>
inline constexpr bool
    alloc_propagates_to_elements<std::pmr::polymorphic_allocator<T>> = true;

template <typename Container>
typename Container::reference emplace_back_default(Container& c) {
  // Guarded, not folded into the static_assert: only a propagating container is
  // guaranteed to have allocator_type.
  if constexpr (apache::thrift::detail::alloc_should_propagate<
                    Container,
                    typename Container::value_type>) {
    static_assert(
        alloc_propagates_to_elements<typename Container::allocator_type>,
        "This container's allocator does not pass itself to the elements it "
        "constructs, so they would be built with a default-constructed "
        "allocator. Wrap it in std::scoped_allocator_adaptor (or use "
        "std::pmr::polymorphic_allocator) before naming it in cpp.allocator.");
  }
  return c.emplace_back();
}

template <typename Map, typename KeyDeserializer, typename MappedDeserializer>
void deserialize_key_val_into_map(
    Map& m, const KeyDeserializer& kr, const MappedDeserializer& mr) {
  typename Map::key_type key = apache::thrift::detail::default_map_key(m);
  kr(key);
  mr(m[std::move(key)]);
}

template <typename Void, typename T>
inline constexpr bool sorted_unique_constructible_ = false;
template <typename T>
inline constexpr bool sorted_unique_constructible_<
    std::void_t<
        decltype(T(
            folly::sorted_unique,
            typename T::container_type(),
            std::declval<const typename T::key_compare&>())),
        decltype(T(
            typename T::container_type(),
            std::declval<const typename T::key_compare&>()))>,
    T> = true;
template <typename T>
inline constexpr bool sorted_unique_constructible_v =
    sorted_unique_constructible_<void, T>;

FOLLY_CREATE_MEMBER_INVOKER(emplace_hint_invoker, emplace_hint);
FOLLY_CREATE_MEMBER_INVOKER(try_emplace_invoker, try_emplace);

template <typename Container>
concept is_ordered_container = requires { typename Container::key_compare; };

template <typename T>
constexpr bool map_emplace_hint_is_invocable_v = std::is_invocable_v<
    emplace_hint_invoker,
    T,
    typename T::iterator,
    typename T::key_type,
    typename T::mapped_type>;

template <typename T>
constexpr bool map_try_emplace_is_invocable_v =
    std::is_invocable_v<try_emplace_invoker, T, typename T::key_type>;

// Unordered maps that do not propagate their allocator deserialize every mapped
// value in place via try_emplace. Other maps deserialize in place only the
// values that aren't trivially copyable.
template <typename T>
constexpr bool map_supports_in_place_deserialize_v = !is_ordered_container<T> &&
    !apache::thrift::detail::alloc_should_propagate_map<T> &&
    map_try_emplace_is_invocable_v<T>;

// Emplaces `key` with a default value through try_emplace or emplace_hint at
// end(), whichever is faster for the map.
template <typename Map>
std::pair<typename Map::iterator, bool> emplace_default(
    Map& map, typename Map::key_type&& key) {
  if constexpr (map_supports_in_place_deserialize_v<Map>) {
    return map.try_emplace(std::move(key));
  } else {
    const auto size = map.size();
    auto it = map.emplace_hint(
        map.end(),
        std::piecewise_construct,
        std::forward_as_tuple(std::move(key)),
        std::tuple<>());
    return {it, map.size() != size};
  }
}

template <typename T>
constexpr bool set_emplace_hint_is_invocable_v = std::is_invocable_v<
    emplace_hint_invoker,
    T,
    typename T::iterator,
    typename T::value_type>;

template <typename Map, typename KeyDeserializer, typename MappedDeserializer>
  requires(sorted_unique_constructible_v<Map>)
void deserialize_known_length_map(
    Map& map,
    std::uint32_t mapSize,
    const KeyDeserializer& kr,
    const MappedDeserializer& mr) {
  if (mapSize == 0) {
    return;
  }

  bool sorted = true;
  typename Map::container_type tmp(map.get_allocator());
  folly::reserve_if_available(tmp, mapSize);
  {
    decltype(auto) elem0 = tmp.emplace_back();
    kr(elem0.first);
    mr(elem0.second);
  }
  for (size_t i = 1; i < mapSize; ++i) {
    decltype(auto) elem = tmp.emplace_back();
    kr(elem.first);
    mr(elem.second);
    sorted = sorted && map.key_comp()(tmp[i - 1].first, elem.first);
  }

  using folly::sorted_unique;
  const auto compare = map.key_comp();
  map = sorted ? Map(sorted_unique, std::move(tmp), compare)
               : Map(std::move(tmp), compare);
}

// Trivially copyable values measured faster through a temporary.
template <typename Map, typename KeyDeserializer, typename MappedDeserializer>
  requires(
      !sorted_unique_constructible_v<Map> &&
      map_emplace_hint_is_invocable_v<Map> &&
      (map_supports_in_place_deserialize_v<Map> ||
       !std::is_trivially_copyable_v<typename Map::mapped_type>))
void deserialize_known_length_map(
    Map& map,
    std::uint32_t mapSize,
    const KeyDeserializer& kr,
    const MappedDeserializer& mr) {
  folly::reserve_if_available(map, mapSize);
  for (auto i = mapSize; i--;) {
    typename Map::key_type key = apache::thrift::detail::default_map_key(map);
    kr(key);
    auto [it, inserted] = emplace_default(map, std::move(key));
    if (inserted) {
      mr(it->second);
    } else {
      typename Map::mapped_type value =
          apache::thrift::detail::default_map_value(map);
      mr(value);
    }
  }
}

template <typename Map, typename KeyDeserializer, typename MappedDeserializer>
  requires(
      !sorted_unique_constructible_v<Map> &&
      map_emplace_hint_is_invocable_v<Map> &&
      !map_supports_in_place_deserialize_v<Map> &&
      std::is_trivially_copyable_v<typename Map::mapped_type>)
void deserialize_known_length_map(
    Map& map,
    std::uint32_t mapSize,
    const KeyDeserializer& kr,
    const MappedDeserializer& mr) {
  folly::reserve_if_available(map, mapSize);
  for (auto i = mapSize; i--;) {
    typename Map::key_type key = apache::thrift::detail::default_map_key(map);
    typename Map::mapped_type value =
        apache::thrift::detail::default_map_value(map);
    kr(key);
    mr(value);
    map.emplace_hint(map.end(), std::move(key), std::move(value));
  }
}

template <typename Map, typename KeyDeserializer, typename MappedDeserializer>
  requires(
      !sorted_unique_constructible_v<Map> &&
      !map_emplace_hint_is_invocable_v<Map>)
void deserialize_known_length_map(
    Map& map,
    std::uint32_t mapSize,
    const KeyDeserializer& kr,
    const MappedDeserializer& mr) {
  folly::reserve_if_available(map, mapSize);
  for (auto i = mapSize; i--;) {
    deserialize_key_val_into_map(map, kr, mr);
  }
}

template <typename Set, typename ValDeserializer>
  requires(sorted_unique_constructible_v<Set>)
void deserialize_known_length_set(
    Set& set, std::uint32_t setSize, const ValDeserializer& vr) {
  if (setSize == 0) {
    return;
  }

  bool sorted = true;
  typename Set::container_type tmp(set.get_allocator());
  folly::reserve_if_available(tmp, setSize);
  {
    auto& elem0 = emplace_back_default(tmp);
    vr(elem0);
  }
  for (size_t i = 1; i < setSize; ++i) {
    auto& elem = emplace_back_default(tmp);
    vr(elem);
    sorted = sorted && set.key_comp()(tmp[i - 1], elem);
  }

  using folly::sorted_unique;
  const auto compare = set.key_comp();
  set = sorted ? Set(sorted_unique, std::move(tmp), compare)
               : Set(std::move(tmp), compare);
}

template <typename Set, typename ValDeserializer>
  requires(
      !sorted_unique_constructible_v<Set> &&
      set_emplace_hint_is_invocable_v<Set>)
void deserialize_known_length_set(
    Set& set, std::uint32_t setSize, const ValDeserializer& vr) {
  folly::reserve_if_available(set, setSize);
  for (auto i = setSize; i--;) {
    typename Set::value_type value =
        apache::thrift::detail::default_set_element(set);
    vr(value);
    set.emplace_hint(set.end(), std::move(value));
  }
}

template <typename Set, typename ValDeserializer>
  requires(
      !sorted_unique_constructible_v<Set> &&
      !set_emplace_hint_is_invocable_v<Set>)
void deserialize_known_length_set(
    Set& set, std::uint32_t setSize, const ValDeserializer& vr) {
  folly::reserve_if_available(set, setSize);
  for (auto i = setSize; i--;) {
    typename Set::value_type value =
        apache::thrift::detail::default_set_element(set);
    vr(value);
    set.insert(std::move(value));
  }
}

inline uint32_t checked_container_size(size_t size) {
  const size_t limit = std::numeric_limits<int32_t>::max();
  if (size > limit) {
    protocol::TProtocolException::throwExceededSizeLimit(size, limit);
  }
  return static_cast<uint32_t>(size);
}

template <typename Protocol>
inline constexpr bool map_value_api_v = requires(Protocol& protocol) {
  protocol.writeMapValueBegin();
  protocol.writeMapValueEnd();
};

template <typename Protocol>
std::size_t writeMapValueBegin(Protocol& protocol) {
  if constexpr (map_value_api_v<Protocol>) {
    return protocol.writeMapValueBegin();
  }
  return 0;
}

template <typename Protocol>
std::size_t writeMapValueEnd(Protocol& protocol) {
  if constexpr (map_value_api_v<Protocol>) {
    return protocol.writeMapValueEnd();
  }
  return 0;
}

template <typename Protocol, typename = void>
struct supports_arithmetic_vectors : std::false_type {};
template <typename Protocol>
struct supports_arithmetic_vectors<
    Protocol,
    std::void_t<decltype(Protocol::kSupportsArithmeticVectors())>>
    : std::bool_constant<Protocol::kSupportsArithmeticVectors()> {};
template <typename Protocol>
inline constexpr bool supports_arithmetic_vectors_v =
    supports_arithmetic_vectors<Protocol>::value;

template <typename ElemType, typename ElemTType>
inline constexpr bool is_supported_arithmetic_elem_type_v =
    (std::is_same_v<ElemType, float> || std::is_same_v<ElemType, double> ||
     std::is_same_v<ElemType, std::int8_t> ||
     std::is_same_v<ElemType, std::uint8_t> ||
     std::is_same_v<ElemType, std::int64_t> ||
     std::is_same_v<ElemType, std::uint64_t> ||
     std::is_same_v<ElemType, std::int32_t> ||
     std::is_same_v<ElemType, std::uint32_t> ||
     std::is_same_v<ElemType, std::int16_t> ||
     std::is_same_v<ElemType, std::uint16_t>) &&
    (ElemTType::value == protocol::TType::T_BYTE ||
     ElemTType::value == protocol::TType::T_FLOAT ||
     ElemTType::value == protocol::TType::T_DOUBLE ||
     ElemTType::value == protocol::TType::T_U64 ||
     ElemTType::value == protocol::TType::T_I64 ||
     ElemTType::value == protocol::TType::T_I32 ||
     ElemTType::value == protocol::TType::T_I16 ||
     ElemTType::value == protocol::TType::T_I08);

template <typename Protocol, typename ElemTType, typename ContainerType>
inline constexpr bool should_process_as_arithmetic_vector_v =
    supports_arithmetic_vectors<Protocol>::value &&
    is_supported_arithmetic_elem_type_v<
        typename ContainerType::value_type,
        ElemTType> &&
    folly::is_contiguous_range_v<ContainerType> &&
    requires(typename ContainerType::value_type* value, size_t len) {
      Protocol::writeArithmeticVector(value, len);
    };

/*
 * Common helper that iterates over a set-like container in the order required
 * by `Protocol`, invoking `writeElem(elem)` for each element. Used by both
 * `protocol_methods<type_class::set<...>, ..., ...>::write` and
 * `op::detail::SetEncode<Tag>`.
 */
template <typename Tag, typename Protocol, typename Container, typename WriteFn>
void encodeSetElements(
    Protocol& protocol, const Container& set, WriteFn writeElem) {
  const KeyOrder keyOrder = protocol.keyOrder();
  const bool shouldSort = keyOrder == KeyOrder::StableAscending ||
      (keyOrder == KeyOrder::NativeAscending &&
       !is_ordered_container<Container>);

  if (shouldSort) {
    std::vector<typename Container::const_iterator> iters;
    iters.reserve(set.size());
    for (auto it = set.begin(); it != set.end(); ++it) {
      iters.push_back(it);
    }
    auto compare = [&](auto a, auto b) {
      if (keyOrder == KeyOrder::StableAscending) {
        return StableLessThan<Tag>{}(*a, *b);
      }
      return *a < *b;
    };
    std::sort(iters.begin(), iters.end(), compare);
    for (auto it : iters) {
      writeElem(*it);
    }
  } else {
    for (const auto& elem :
         folly::order_preserving_reinsertion_view_or_default(set)) {
      writeElem(elem);
    }
  }
}

/*
 * Common helper that iterates over a map-like container in the order required
 * by `Protocol`, invoking `writeEntry(key, value)` for each entry. Used by
 * `op::detail::MapEncodeImpl`.
 */
template <
    typename KeyTag,
    typename Protocol,
    typename Container,
    typename WriteFn>
void encodeMapElements(
    Protocol& protocol, const Container& map, WriteFn writeEntry) {
  const KeyOrder keyOrder = protocol.keyOrder();
  const bool shouldSort = keyOrder == KeyOrder::StableAscending ||
      (keyOrder == KeyOrder::NativeAscending &&
       !is_ordered_container<Container>);

  if (shouldSort) {
    std::vector<typename Container::const_iterator> iters;
    iters.reserve(map.size());
    for (auto it = map.begin(); it != map.end(); ++it) {
      iters.push_back(it);
    }
    auto compare = [&](auto a, auto b) {
      if (keyOrder == KeyOrder::StableAscending) {
        return StableLessThan<KeyTag>{}((*a).first, (*b).first);
      }
      return (*a).first < (*b).first;
    };
    std::sort(iters.begin(), iters.end(), compare);
    for (auto it : iters) {
      writeEntry((*it).first, (*it).second);
    }
  } else {
    for (const auto& elem :
         folly::order_preserving_reinsertion_view_or_default(map)) {
      writeEntry(elem.first, elem.second);
    }
  }
}

} // namespace apache::thrift::op::detail
