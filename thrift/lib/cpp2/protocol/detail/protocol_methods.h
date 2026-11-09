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

#include <bitset>
#include <iostream>
#include <iterator>
#include <limits>
#include <list>
#include <memory>
#include <type_traits>
#include <vector>

#include <folly/Traits.h>
#include <folly/container/View.h>
#include <folly/container/range_traits.h>
#include <folly/functional/Invoke.h>
#include <folly/io/IOBuf.h>
#include <folly/lang/VectorTraits.h>

#include <thrift/lib/cpp/protocol/TProtocolException.h>
#include <thrift/lib/cpp/protocol/TType.h>
#include <thrift/lib/cpp/util/EnumUtils.h>
#include <thrift/lib/cpp2/TypeClass.h>
#include <thrift/lib/cpp2/op/Encode.h>
#include <thrift/lib/cpp2/op/detail/AllocatorPropagation.h>
#include <thrift/lib/cpp2/op/detail/EncodeHelpers.h>
#include <thrift/lib/cpp2/protocol/Cpp2Ops.h>
#include <thrift/lib/cpp2/protocol/Protocol.h>
#include <thrift/lib/cpp2/protocol/ProtocolReaderWireTypeInfo.h>
#include <thrift/lib/cpp2/protocol/Traits.h>
#include <thrift/lib/cpp2/type/detail/TypeClassToTypeTag.h>

/**
 * Specializations of `protocol_methods` encapsulate a collection of
 * read/write/size/sizeZC methods that can be performed on Thrift
 * objects and primitives. TypeClass (see apache::thrift::type_class)
 * refers to the general type of data that Type is, and is passed around for
 * two reasons:
 *  - to provide support for generic containers which have a common interface
 *    for building collections (e.g. a `std::vector<int>` and `std::deque<int>`,
 *    which can back a Thrift list, and thus have
 *    `type_class::list<type_class::integral>`, or an
 *    `std::map<std::string, MyStruct>` would have
 *    `type_class::map<type_class::string, type_class::structure>``).
 *  - to differentiate between Thrift types that are represented with the
 *    same C++ type, e.g. both Thrift binaries and strings are represented
 *    with an `std::string`, TypeClass is used to decide which protocol
 *    method to call.
 *
 * Example:
 *
 * // MyModule.thrift:
 * struct MyStruct {
 *   1: list<set<string>> fieldA
 * }
 *
 * // C++
 *
 * using methods = protocol_methods<
 *    type_class::list<type_class::set<type_class::string>>,
 *    std::vector<std::set<std::string>>,
 *    type::list<type::set<type::string_t>>>
 *
 * MyStruct struct_instance;
 * CompactProtocolReader reader;
 * methods::read(struct_instance.fieldA, reader);
 */

namespace apache::thrift::detail::pm {

using op::detail::checked_container_size;
using op::detail::deserialize_key_val_into_map;
using op::detail::deserialize_known_length_map;
using op::detail::deserialize_known_length_set;
// NOLINTNEXTLINE(facebook-hte-DetailCall)
using op::detail::is_ordered_container;
using op::detail::ListDecodeImpl;
// NOLINTNEXTLINE(facebook-hte-DetailCall)
using op::detail::ListEncodeImpl;
using op::detail::map_emplace_hint_is_invocable_v;
// NOLINTNEXTLINE(facebook-hte-DetailCall)
using op::detail::map_supports_in_place_deserialize_v;
// NOLINTNEXTLINE(facebook-hte-DetailCall)
using op::detail::map_try_emplace_is_invocable_v;
using op::detail::set_emplace_hint_is_invocable_v;
using op::detail::sorted_unique_constructible_v;

/*
 * Primitive Types Specialization
 *
 * ExpectedTag is used to diagnose cases where the duck-typed design of
 * protocol_methods causes the behavior to diverge from the Thrift spec.
 */
template <typename TypeClass, typename Type, typename ExpectedTag>
struct protocol_methods;

template <typename ExpectedTag>
struct expected_value_tag_or_void {
  using type = typename ExpectedTag::value_tag;
};
template <>
struct expected_value_tag_or_void<void> {
  using type = void;
};
template <typename ExpectedTag>
using expected_value_tag_or_void_t =
    typename expected_value_tag_or_void<ExpectedTag>::type;

template <typename ExpectedTag>
struct expected_key_tag_or_void {
  using type = typename ExpectedTag::key_tag;
};
template <>
struct expected_key_tag_or_void<void> {
  using type = void;
};
template <typename ExpectedTag>
using expected_key_tag_or_void_t =
    typename expected_key_tag_or_void<ExpectedTag>::type;

template <typename ExpectedTag, typename WireTag>
inline constexpr bool matches_wire_tag_v =
    std::is_base_of_v<WireTag, ExpectedTag>;

template <typename ExpectedTag, typename Type>
inline constexpr bool matches_structured_wire_tag_v =
    matches_wire_tag_v<ExpectedTag, type::structured_c> &&
    (matches_wire_tag_v<ExpectedTag, type::union_c> == is_thrift_union_v<Type>);

template <typename ExpectedTag, typename WireTag>
inline constexpr bool matches_integral_wire_tag_v =
    matches_wire_tag_v<ExpectedTag, WireTag>;

template <typename ExpectedTag, typename WireTag>
inline constexpr bool matches_floating_point_wire_tag_v =
    matches_wire_tag_v<ExpectedTag, WireTag>;

#define THRIFT_PROTOCOL_METHODS_REGISTER_OP_INTEGRAL(Type, WireTag)   \
  template <typename ExpectedTag>                                     \
  struct protocol_methods<type_class::integral, Type, ExpectedTag> {  \
    static_assert(                                                    \
        matches_integral_wire_tag_v<ExpectedTag, WireTag>,            \
        "ExpectedTag does not match the selected integral overload"); \
    template <typename Protocol>                                      \
    static void read(Protocol& protocol, Type& out) {                 \
      op::decode<WireTag>(protocol, out);                             \
    }                                                                 \
    template <typename Protocol>                                      \
    static std::size_t write(Protocol& protocol, Type in) {           \
      return op::encode<WireTag>(protocol, in);                       \
    }                                                                 \
    template <bool ZeroCopy, typename Protocol>                       \
    static std::size_t serializedSize(Protocol& protocol, Type in) {  \
      return op::serialized_size<ZeroCopy, WireTag>(protocol, in);    \
    }                                                                 \
  }

THRIFT_PROTOCOL_METHODS_REGISTER_OP_INTEGRAL(std::int8_t, type::byte_t);
THRIFT_PROTOCOL_METHODS_REGISTER_OP_INTEGRAL(std::uint8_t, type::byte_t);
THRIFT_PROTOCOL_METHODS_REGISTER_OP_INTEGRAL(std::int16_t, type::i16_t);
THRIFT_PROTOCOL_METHODS_REGISTER_OP_INTEGRAL(std::uint16_t, type::i16_t);
THRIFT_PROTOCOL_METHODS_REGISTER_OP_INTEGRAL(std::int32_t, type::i32_t);
THRIFT_PROTOCOL_METHODS_REGISTER_OP_INTEGRAL(std::uint32_t, type::i32_t);
THRIFT_PROTOCOL_METHODS_REGISTER_OP_INTEGRAL(std::int64_t, type::i64_t);
THRIFT_PROTOCOL_METHODS_REGISTER_OP_INTEGRAL(std::uint64_t, type::i64_t);

#undef THRIFT_PROTOCOL_METHODS_REGISTER_OP_INTEGRAL

// std::vector<bool> isn't actually a container, so
// define a special overload which takes its specialized
// proxy type
template <typename ExpectedTag>
struct protocol_methods<type_class::integral, bool, ExpectedTag> {
  static_assert(
      matches_integral_wire_tag_v<ExpectedTag, type::bool_t>,
      "ExpectedTag does not match the selected integral overload");

  template <typename Protocol>
  static void read(Protocol& protocol, bool& out) {
    op::decode<type::bool_t>(protocol, out);
  }

  template <typename Protocol>
  static std::size_t write(Protocol& protocol, bool in) {
    return op::encode<type::bool_t>(protocol, in);
  }

  template <bool ZeroCopy, typename Protocol>
  static std::size_t serializedSize(Protocol& protocol, bool in) {
    return op::serialized_size<ZeroCopy, type::bool_t>(protocol, in);
  }

  template <
      typename Protocol,
      typename BitReference,
      typename =
          std::enable_if_t<folly::is_vector_bool_reference_v<BitReference>>>
  static void read(Protocol& protocol, BitReference out) {
    bool value;
    op::decode<type::bool_t>(protocol, value);
    out = value;
  }
};

#define THRIFT_PROTOCOL_METHODS_REGISTER_OP_FLOATING_POINT(Type, WireTag)   \
  template <typename ExpectedTag>                                           \
  struct protocol_methods<type_class::floating_point, Type, ExpectedTag> {  \
    static_assert(                                                          \
        matches_floating_point_wire_tag_v<ExpectedTag, WireTag>,            \
        "ExpectedTag does not match the selected floating-point overload"); \
    template <typename Protocol>                                            \
    static void read(Protocol& protocol, Type& out) {                       \
      op::decode<WireTag>(protocol, out);                                   \
    }                                                                       \
    template <typename Protocol>                                            \
    static std::size_t write(Protocol& protocol, Type in) {                 \
      return op::encode<WireTag>(protocol, in);                             \
    }                                                                       \
    template <bool ZeroCopy, typename Protocol>                             \
    static std::size_t serializedSize(Protocol& protocol, Type in) {        \
      return op::serialized_size<ZeroCopy, WireTag>(protocol, in);          \
    }                                                                       \
  }

THRIFT_PROTOCOL_METHODS_REGISTER_OP_FLOATING_POINT(double, type::double_t);
THRIFT_PROTOCOL_METHODS_REGISTER_OP_FLOATING_POINT(float, type::float_t);

#undef THRIFT_PROTOCOL_METHODS_REGISTER_OP_FLOATING_POINT

template <typename Type, typename ExpectedTag>
struct protocol_methods<type_class::string, Type, ExpectedTag> {
  static_assert(
      matches_wire_tag_v<ExpectedTag, type::string_t>,
      "ExpectedTag does not match the string wire tag");
  template <typename Protocol>
  static void read(Protocol& protocol, Type& out) {
    op::decode<type::string_t>(protocol, out);
  }
  template <typename Protocol>
  static std::size_t write(Protocol& protocol, const Type& in) {
    return op::encode<type::string_t>(protocol, in);
  }
  template <bool ZeroCopy, typename Protocol>
  static std::size_t serializedSize(Protocol& protocol, const Type& in) {
    return op::serialized_size<ZeroCopy, type::string_t>(protocol, in);
  }
};

template <typename Type, typename ExpectedTag>
struct protocol_methods<type_class::binary, Type, ExpectedTag> {
  static_assert(
      matches_wire_tag_v<ExpectedTag, type::binary_t>,
      "ExpectedTag does not match the binary wire tag");
  template <typename Protocol>
  static void read(Protocol& protocol, Type& out) {
    op::decode<type::binary_t>(protocol, out);
  }
  template <typename Protocol>
  static std::size_t write(Protocol& protocol, const Type& in) {
    return op::encode<type::binary_t>(protocol, in);
  }
  template <bool ZeroCopy, typename Protocol>
  static std::size_t serializedSize(Protocol& protocol, const Type& in) {
    return op::serialized_size<ZeroCopy, type::binary_t>(protocol, in);
  }
};

/*
 * Enum Specialization
 */

template <
    typename TypeClass,
    typename Type,
    typename IntType = std::underlying_type_t<Type>>
struct enum_protocol_methods {
  static_assert(std::is_enum_v<Type>, "must be enum");
  using WireTag = type::infer_tag<IntType>;

  template <typename Protocol>
  static void read(Protocol& protocol, Type& out) {
    if constexpr (std::is_same_v<TypeClass, type_class::enumeration>) {
      op::detail::readEnum(protocol, out);
    } else {
      IntType value;
      op::decode<WireTag>(protocol, value);
      out = static_cast<Type>(value);
    }
  }

  template <typename Protocol>
  static std::size_t write(Protocol& protocol, Type in) {
    if constexpr (std::is_same_v<TypeClass, type_class::enumeration>) {
      return op::detail::writeEnum(protocol, in);
    } else {
      return op::encode<WireTag>(protocol, static_cast<IntType>(in));
    }
  }

  template <bool ZeroCopy, typename Protocol>
  static std::size_t serializedSize(Protocol& protocol, Type in) {
    if constexpr (std::is_same_v<TypeClass, type_class::enumeration>) {
      return op::detail::serializedSizeEnum(protocol, in);
    } else {
      return op::serialized_size<ZeroCopy, WireTag>(
          protocol, static_cast<IntType>(in));
    }
  }
};

// Thrift enums are always read as int32_t
template <typename Type, typename ExpectedTag>
struct protocol_methods<type_class::enumeration, Type, ExpectedTag>
    : enum_protocol_methods<type_class::enumeration, Type, std::int32_t> {};

// Strong integral types keep their precision.
template <typename Type, typename ExpectedTag>
  requires std::is_enum_v<Type>
struct protocol_methods<type_class::integral, Type, ExpectedTag>
    : enum_protocol_methods<type_class::integral, Type> {};

/*
 * List Specialization
 */
template <typename ElemClass, typename Type, typename ExpectedTag>
struct protocol_methods<type_class::list<ElemClass>, Type, ExpectedTag> {
  static_assert(
      matches_wire_tag_v<ExpectedTag, type::list_c>,
      "ExpectedTag does not match the list container type");
  static_assert(
      !std::is_same<ElemClass, type_class::unknown>(),
      "Unable to serialize unknown list element");

  using elem_type = std::remove_cvref_t<typename Type::value_type>;
  using elem_methods = protocol_methods<
      ElemClass,
      elem_type,
      expected_value_tag_or_void_t<ExpectedTag>>;
  using elem_ttype = protocol_type<ElemClass, elem_type>;

  template <typename Protocol>
  static void read(Protocol& protocol, Type& out) {
    ListDecodeImpl<elem_ttype::value>{}(protocol, out, [&](auto&& elem) {
      elem_methods::read(protocol, elem);
    });
  }

  template <typename Protocol>
  static std::size_t write(Protocol& protocol, const Type& out) {
    return ListEncodeImpl<elem_ttype::value, elem_type>{}(
        protocol, out, [&](const auto& elem) {
          return elem_methods::write(protocol, elem);
        });
  }

  template <bool ZeroCopy, typename Protocol>
  static std::size_t serializedSize(Protocol& protocol, const Type& out) {
    return op::serialized_size<ZeroCopy, ExpectedTag>(protocol, out);
  }
};

/*
 * Set Specialization
 */
template <typename ElemClass, typename Type, typename ExpectedTag>
struct protocol_methods<type_class::set<ElemClass>, Type, ExpectedTag> {
  static_assert(
      matches_wire_tag_v<ExpectedTag, type::set_c>,
      "ExpectedTag does not match the set container type");
  static_assert(
      !std::is_same<ElemClass, type_class::unknown>(),
      "Unable to serialize unknown type");

  using elem_type = typename Type::value_type;
  using elem_methods = protocol_methods<
      ElemClass,
      elem_type,
      expected_value_tag_or_void_t<ExpectedTag>>;
  using elem_ttype = protocol_type<ElemClass, elem_type>;

  template <typename Protocol>
  static void read(Protocol& protocol, Type& out) {
    op::detail::SetDecodeImpl<elem_ttype::value>{}(
        protocol, out, [&](auto& value) {
          elem_methods::read(protocol, value);
        });
  }

  template <typename Protocol>
  static std::size_t write(Protocol& protocol, const Type& out) {
    using Tag = type_class::to_type_tag_t<ElemClass, elem_type>;
    return op::detail::SetEncodeImpl<elem_ttype::value, Tag>{}(
        protocol, out, [&](const auto& elem) {
          return elem_methods::write(protocol, elem);
        });
  }

  template <bool ZeroCopy, typename Protocol>
  static std::size_t serializedSize(Protocol& protocol, const Type& out) {
    return op::serialized_size<ZeroCopy, ExpectedTag>(protocol, out);
  }
};

/*
 * Map Specialization
 */
template <
    typename KeyClass,
    typename MappedClass,
    typename Type,
    typename ExpectedTag>
struct protocol_methods<
    type_class::map<KeyClass, MappedClass>,
    Type,
    ExpectedTag> {
  static_assert(
      matches_wire_tag_v<ExpectedTag, type::map_c>,
      "ExpectedTag does not match the map container type");
  static_assert(
      !std::is_same<KeyClass, type_class::unknown>(),
      "Unable to serialize unknown key type in map");
  static_assert(
      !std::is_same<MappedClass, type_class::unknown>(),
      "Unable to serialize unknown mapped type in map");

  using key_type = typename Type::key_type;
  using mapped_type = typename Type::mapped_type;
  using key_methods = protocol_methods<
      KeyClass,
      key_type,
      expected_key_tag_or_void_t<ExpectedTag>>;
  using mapped_methods = protocol_methods<
      MappedClass,
      mapped_type,
      expected_value_tag_or_void_t<ExpectedTag>>;
  using key_ttype = protocol_type<KeyClass, key_type>;
  using mapped_ttype = protocol_type<MappedClass, mapped_type>;

 protected:
  template <typename Protocol, typename U>
  static void consume_elem(Protocol& protocol, U& out) {
    deserialize_key_val_into_map(
        out,
        [&protocol](auto& key) { key_methods::read(protocol, key); },
        [&protocol](auto& value) { mapped_methods::read(protocol, value); });
  }

 public:
  template <typename Protocol, typename U>
  static void read(Protocol& protocol, U& out) {
    std::uint32_t map_size = -1;
    using WireTypeInfo = ProtocolReaderWireTypeInfo<Protocol>;
    using WireType = typename WireTypeInfo::WireType;

    WireType rpt_key_type = WireTypeInfo::defaultValue(),
             rpt_mapped_type = WireTypeInfo::defaultValue();

    protocol.readMapBegin(rpt_key_type, rpt_mapped_type, map_size);
    out.clear();
    if (protocol.kOmitsContainerSizes()) {
      while (protocol.peekMap()) {
        consume_elem(protocol, out);
      }
    } else {
      // CompactProtocol does not transmit key/mapped types if
      // the map is empty
      if (map_size > 0 &&
          (WireTypeInfo::fromTType(key_ttype::value) != rpt_key_type ||
           WireTypeInfo::fromTType(mapped_ttype::value) != rpt_mapped_type)) {
        apache::thrift::skip_n(
            protocol, map_size, {rpt_key_type, rpt_mapped_type});
      } else {
        if (!canReadNElements(
                protocol, map_size, {rpt_key_type, rpt_mapped_type})) {
          protocol::TProtocolException::throwTruncatedData();
        }
        const auto kreader = [&protocol](auto& key) {
          key_methods::read(protocol, key);
        };
        const auto vreader = [&protocol](auto& value) {
          mapped_methods::read(protocol, value);
        };
        deserialize_known_length_map(out, map_size, kreader, vreader);
      }
    }
    protocol.readMapEnd();
  }

  template <typename Protocol, typename U>
  static std::size_t write(Protocol& protocol, const U& out) {
    using KeyTag = type_class::to_type_tag_t<KeyClass, key_type>;
    return op::detail::
        MapEncodeImpl<key_ttype::value, mapped_ttype::value, KeyTag>{}(
            protocol,
            out,
            [&](const auto& key) { return key_methods::write(protocol, key); },
            [&](const auto& value) {
              return mapped_methods::write(protocol, value);
            });
  }

  template <bool ZeroCopy, typename Protocol, typename U>
  static std::size_t serializedSize(Protocol& protocol, const U& out) {
    return op::serialized_size<ZeroCopy, ExpectedTag>(protocol, out);
  }
};

/*
 * Struct with Indirection Specialization
 */
template <
    typename ElemClass,
    typename Indirection,
    typename Type,
    typename ExpectedTag>
struct protocol_methods<
    indirection_tag<ElemClass, Indirection>,
    Type,
    ExpectedTag> {
  using indirection = Indirection;
  using elem_type =
      std::remove_reference_t<std::invoke_result_t<indirection, Type&>>;
  using elem_methods = protocol_methods<ElemClass, elem_type, ExpectedTag>;

  template <typename Protocol>
  static void read(Protocol& protocol, Type& out) {
    elem_methods::read(protocol, indirection{}(out));
  }

  template <typename Protocol>
  static std::size_t write(Protocol& protocol, const Type& in) {
    return elem_methods::write(protocol, indirection{}(in));
  }
  template <bool ZeroCopy, typename Protocol>
  static std::size_t serializedSize(Protocol& protocol, const Type& in) {
    return elem_methods::template serializedSize<ZeroCopy>(
        protocol, indirection{}(in));
  }
};

template <typename Type>
struct structured_protocol_methods {
  template <typename Protocol>
  static void read(Protocol& protocol, Type& out) {
    Cpp2Ops<Type>::read(&protocol, &out);
  }
  template <typename Protocol>
  static std::size_t write(Protocol& protocol, const Type& in) {
    return Cpp2Ops<Type>::write(&protocol, &in);
  }
  template <bool ZeroCopy, typename Protocol>
  static std::size_t serializedSize(Protocol& protocol, const Type& in) {
    if (ZeroCopy) {
      return Cpp2Ops<Type>::serializedSizeZC(&protocol, &in);
    } else {
      return Cpp2Ops<Type>::serializedSize(&protocol, &in);
    }
  }
};

/*
 * Structured Type Specialization
 * Forwards to Cpp2Ops wrapper around member read/write/etc.
 */
template <typename Type, typename ExpectedTag>
struct protocol_methods<type_class::structure, Type, ExpectedTag>
    : structured_protocol_methods<Type> {
  static_assert(
      matches_structured_wire_tag_v<ExpectedTag, Type>,
      "ExpectedTag does not match the structured type");
};

/*
 * Union Specialization
 * Forwards to Cpp2Ops wrapper around member read/write/etc.
 */
template <typename Type, typename ExpectedTag>
struct protocol_methods<type_class::variant, Type, ExpectedTag>
    : structured_protocol_methods<Type> {
  static_assert(
      matches_wire_tag_v<ExpectedTag, type::union_c>,
      "ExpectedTag does not match the union type");
};

} // namespace apache::thrift::detail::pm

namespace apache::thrift::op::detail {

template <typename TypeClass, typename Type, typename ExpectedTag>
struct ProtocolMethodsBridge {
  template <typename Protocol, typename U>
    requires requires(Protocol& protocol, const U& value) {
      apache::thrift::detail::pm::
          protocol_methods<TypeClass, Type, ExpectedTag>::write(
              protocol, value);
    }
  static uint32_t write(Protocol& protocol, const U& value) {
    return apache::thrift::detail::pm::
        protocol_methods<TypeClass, Type, ExpectedTag>::write(protocol, value);
  }
};

} // namespace apache::thrift::op::detail
