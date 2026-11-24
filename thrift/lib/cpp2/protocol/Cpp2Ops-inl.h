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

#include <type_traits>

#include <folly/CPortability.h>
#include <folly/Traits.h>
#include <folly/Utility.h>
#include <folly/io/IOBuf.h>
#include <thrift/lib/cpp2/IOBufChain.h>
#include <thrift/lib/cpp2/op/Encode.h>
#include <thrift/lib/cpp2/op/detail/AllocatorPropagation.h>
#include <thrift/lib/cpp2/protocol/Cpp2Ops.h>
#include <thrift/lib/cpp2/type/Tag.h>

namespace apache {
namespace thrift {

namespace detail {

template <
    typename C,
    typename T = decltype(std::declval<C&>().push_back(
        std::declval<typename C::value_type>()))>
struct push_back_result {
  using type = T;
};

template <
    typename C,
    typename T = decltype(std::declval<C&>().insert(
        std::declval<typename C::key_type>()))>
struct insert_key_result {
  using type = T;
};

template <
    typename C,
    typename T =
        decltype(std::declval<C&>()[std::declval<typename C::key_type>()])>
struct subscript_key_result {
  using type = T;
};

template <
    typename C,
    typename T = decltype(std::declval<C&>().reserve(
        std::declval<typename C::size_type>()))>
struct reserve_result {
  using type = T;
};

// The Cpp2Ops container specializations below dispatch on these as
// std::enable_if_t<...> rather than on std::void_t<...> directly. std::void_t
// is a plain alias template, so every std::void_t<X> canonicalizes to void and
// all three specializations would collide as redefinitions of
// Cpp2Ops<T, void>. std::enable_if_t resolves through a class template, so each
// second argument stays a distinct dependent type.
template <typename C, typename = void>
inline constexpr bool has_push_back_v = false;
template <typename C>
inline constexpr bool
    has_push_back_v<C, std::void_t<typename push_back_result<C>::type>> = true;

template <typename C, typename = void>
inline constexpr bool has_insert_key_v = false;
template <typename C>
inline constexpr bool
    has_insert_key_v<C, std::void_t<typename insert_key_result<C>::type>> =
        true;

template <typename C, typename = void>
inline constexpr bool has_subscript_key_v = false;
template <typename C>
inline constexpr bool has_subscript_key_v<
    C,
    std::void_t<typename subscript_key_result<C>::type>> = true;

// Strings have push_back. This keeps them out of the list specialization, which
// would otherwise be ambiguous with the string one.
template <typename C>
inline constexpr bool is_char_string_v = false;
template <typename Alloc>
inline constexpr bool
    is_char_string_v<std::basic_string<char, std::char_traits<char>, Alloc>> =
        true;

template <typename C, typename = void>
struct Reserver {
  static void reserve(C&, typename C::size_type) {}
};

template <typename C>
struct Reserver<C, std::void_t<typename reserve_result<C>::type>> {
  static void reserve(C& container, typename C::size_type size) {
    container.reserve(size);
  }
};

#if defined(__clang__) && __clang_major__ >= 17
#define FBTHRIFT_ALWAYS_INLINE_CALLSITE(...)            \
  do {                                                  \
    FOLLY_PUSH_WARNING                                  \
    FOLLY_CLANG_DISABLE_WARNING("-Wignored-attributes") \
    [[clang::always_inline]] __VA_ARGS__;               \
    FOLLY_POP_WARNING                                   \
  } while (false)
#else
#define FBTHRIFT_ALWAYS_INLINE_CALLSITE(...) \
  do {                                       \
    __VA_ARGS__;                             \
  } while (false)
#endif

// Cpp2Ops for a type that op::encode/op::decode serialize as Tag.
template <class T, class Tag>
class Cpp2OpsViaOp {
 public:
  using Type = T;
  static constexpr protocol::TType thriftType() {
    return op::typeTagToTType<Tag>;
  }
  template <class Protocol>
  FOLLY_ALWAYS_INLINE static uint32_t write(Protocol* prot, const Type* value) {
    FBTHRIFT_ALWAYS_INLINE_CALLSITE(
        return static_cast<uint32_t>(op::encode<Tag>(*prot, *value)));
  }
  template <class Protocol>
  FOLLY_ALWAYS_INLINE static void read(Protocol* prot, Type* value) {
    FBTHRIFT_ALWAYS_INLINE_CALLSITE(op::decode<Tag>(*prot, *value));
  }
  template <class Protocol>
  static uint32_t serializedSize(Protocol* prot, const Type* value) {
    return static_cast<uint32_t>(
        op::serialized_size<false, Tag>(*prot, *value));
  }
  template <class Protocol>
  static uint32_t serializedSizeZC(Protocol* prot, const Type* value) {
    return static_cast<uint32_t>(op::serialized_size<true, Tag>(*prot, *value));
  }
};

#undef FBTHRIFT_ALWAYS_INLINE_CALLSITE

} // namespace detail

template <>
class Cpp2Ops<folly::fbstring>
    : public detail::Cpp2OpsViaOp<folly::fbstring, type::string_t> {};
template <class Alloc>
class Cpp2Ops<std::basic_string<char, std::char_traits<char>, Alloc>>
    : public detail::Cpp2OpsViaOp<
          std::basic_string<char, std::char_traits<char>, Alloc>,
          type::string_t> {};
template <>
class Cpp2Ops<bool> : public detail::Cpp2OpsViaOp<bool, type::bool_t> {};
template <>
class Cpp2Ops<int8_t> : public detail::Cpp2OpsViaOp<int8_t, type::byte_t> {};
template <>
class Cpp2Ops<int16_t> : public detail::Cpp2OpsViaOp<int16_t, type::i16_t> {};
template <>
class Cpp2Ops<int32_t> : public detail::Cpp2OpsViaOp<int32_t, type::i32_t> {};
template <>
class Cpp2Ops<int64_t> : public detail::Cpp2OpsViaOp<int64_t, type::i64_t> {};
template <>
class Cpp2Ops<uint8_t> : public detail::Cpp2OpsViaOp<uint8_t, type::byte_t> {};
template <>
class Cpp2Ops<uint16_t> : public detail::Cpp2OpsViaOp<uint16_t, type::i16_t> {};
template <>
class Cpp2Ops<uint32_t> : public detail::Cpp2OpsViaOp<uint32_t, type::i32_t> {};
template <>
class Cpp2Ops<uint64_t> : public detail::Cpp2OpsViaOp<uint64_t, type::i64_t> {};
template <>
class Cpp2Ops<float> : public detail::Cpp2OpsViaOp<float, type::float_t> {};
template <>
class Cpp2Ops<double> : public detail::Cpp2OpsViaOp<double, type::double_t> {};
template <class E>
class Cpp2Ops<E, typename std::enable_if<std::is_enum<E>::value>::type>
    : public detail::Cpp2OpsViaOp<E, type::enum_t<E>> {};

namespace detail {

template <class Protocol, class V>
void readIntoVector(Protocol* prot, V& vec) {
  using ElemType = typename V::value_type;
  for (auto& e : vec) {
    Cpp2Ops<ElemType>::read(prot, &e);
  }
}

template <class Protocol>
void readIntoVector(Protocol* prot, std::vector<bool>& vec) {
  for (auto e : vec) {
    // e is a proxy object because the elements don't have distinct addresses
    // (packed into a bitvector). We actually copy the proxy during iteration
    // (can't use non-const reference because iteration returns by value, can't
    // use const reference because we modify it), but it still points to the
    // actual element.
    bool b;
    Cpp2Ops<bool>::read(prot, &b);
    e = b;
  }
}

// Define the operation types
struct writeOp {};
struct serializedSizeOp {};
struct serializedSizeZCOp {};

template <class Operation, class Protocol, class V>
uint32_t forEachElement(Protocol* prot, const V* vec) {
  using ElemType = typename V::value_type;
  uint32_t xfer = 0;
  for (const auto& e : *vec) {
    if constexpr (std::is_same_v<Operation, writeOp>) {
      xfer += Cpp2Ops<ElemType>::write(prot, &e);
    } else if constexpr (std::is_same_v<Operation, serializedSizeOp>) {
      xfer += Cpp2Ops<ElemType>::serializedSize(prot, &e);
    } else {
      static_assert(
          std::is_same_v<Operation, serializedSizeZCOp>, "Invalid operation!");
      xfer += Cpp2Ops<ElemType>::serializedSizeZC(prot, &e);
    }
  }
  return xfer;
}

template <class Operation, class Protocol>
uint32_t forEachElement(Protocol* prot, const std::vector<bool>* vec) {
  uint32_t xfer = 0;
  // libc++ uses __bit_const_reference<vector>, a proxy type, as const_reference
  // for std::vector<bool>. Treating it like other types can lead to compilation
  // errors on macOS, hence the std::vector<bool> overload for compatibility.
  for (bool e : *vec) {
    if constexpr (std::is_same_v<Operation, writeOp>) {
      xfer += Cpp2Ops<bool>::write(prot, &e);
    } else if constexpr (std::is_same_v<Operation, serializedSizeOp>) {
      xfer += Cpp2Ops<bool>::serializedSize(prot, &e);
    } else {
      static_assert(
          std::is_same_v<Operation, serializedSizeZCOp>, "Invalid operation!");
      xfer += Cpp2Ops<bool>::serializedSizeZC(prot, &e);
    }
  }
  return xfer;
}

} // namespace detail

template <class L>
class Cpp2Ops<
    L,
    std::enable_if_t<
        apache::thrift::detail::has_push_back_v<L> &&
        !apache::thrift::detail::is_char_string_v<L>>> {
 private:
  // Need a resize func instead of c.resize(size, defaultElement(C)), because
  // some non-standard vectors do not support resize(size_type, T value = T()).
  template <typename C = L>
  std::enable_if_t<
      detail::alloc_should_propagate<C, typename C::value_type>,
      void> static resize(C& c, uint32_t size) {
    c.resize(size, typename C::value_type(c.get_allocator()));
  }
  template <typename C = L>
  std::enable_if_t<
      !detail::alloc_should_propagate<C, typename C::value_type>,
      void> static resize(C& c, uint32_t size) {
    c.resize(size);
  }

 public:
  using Type = L;
  static constexpr protocol::TType thriftType() { return protocol::T_LIST; }
  template <class Protocol>
  static uint32_t write(Protocol* prot, const Type* value) {
    using ElemType = typename Type::value_type;
    uint32_t xfer = 0;
    xfer += prot->writeListBegin(
        Cpp2Ops<ElemType>::thriftType(), folly::to_narrow(value->size()));
    xfer += detail::forEachElement<detail::writeOp>(prot, value);
    xfer += prot->writeListEnd();
    return xfer;
  }
  template <class Protocol>
  static void read(Protocol* prot, Type* value) {
    value->clear();
    uint32_t size;
    protocol::TType etype;
    prot->readListBegin(etype, size);
    resize(*value, size);
    apache::thrift::detail::readIntoVector(prot, *value);
    prot->readListEnd();
  }
  template <class Protocol>
  static uint32_t serializedSize(Protocol* prot, const Type* value) {
    using ElemType = typename Type::value_type;
    uint32_t xfer = 0;
    xfer += prot->serializedSizeListBegin(
        Cpp2Ops<ElemType>::thriftType(), value->size());
    xfer += detail::forEachElement<detail::serializedSizeOp>(prot, value);
    xfer += prot->serializedSizeListEnd();
    return xfer;
  }
  template <class Protocol>
  static uint32_t serializedSizeZC(Protocol* prot, const Type* value) {
    using ElemType = typename Type::value_type;
    uint32_t xfer = 0;
    xfer += prot->serializedSizeListBegin(
        Cpp2Ops<ElemType>::thriftType(), folly::to_narrow(value->size()));
    xfer += detail::forEachElement<detail::serializedSizeZCOp>(prot, value);
    xfer += prot->serializedSizeListEnd();
    return xfer;
  }
};

template <class S>
class Cpp2Ops<
    S,
    std::enable_if_t<apache::thrift::detail::has_insert_key_v<S>>> {
 private:
  template <typename C = S>
  std::enable_if_t<
      detail::alloc_should_propagate<C, typename C::key_type>,
      typename C::key_type> static defaultElement(C& c) {
    return typename C::key_type(c.get_allocator());
  }
  template <typename C = S>
  std::enable_if_t<
      !detail::alloc_should_propagate<C, typename C::key_type>,
      typename C::key_type> static defaultElement(C&) {
    return typename C::key_type{};
  }

 public:
  using Type = S;
  static constexpr protocol::TType thriftType() { return protocol::T_SET; }
  template <class Protocol>
  static uint32_t write(Protocol* prot, const Type* value) {
    using ElemType = typename Type::key_type;
    uint32_t xfer = 0;
    xfer += prot->writeSetBegin(Cpp2Ops<ElemType>::thriftType(), value->size());
    for (const auto& e : *value) {
      xfer += Cpp2Ops<ElemType>::write(prot, &e);
    }
    xfer += prot->writeSetEnd();
    return xfer;
  }
  template <class Protocol>
  static void read(Protocol* prot, Type* value) {
    using ElemType = typename Type::key_type;
    value->clear();
    uint32_t size;
    protocol::TType etype;
    prot->readSetBegin(etype, size);
    apache::thrift::detail::Reserver<Type>::reserve(*value, size);
    for (uint32_t i = 0; i < size; i++) {
      ElemType elem = defaultElement(*value);
      Cpp2Ops<ElemType>::read(prot, &elem);
      value->insert(std::move(elem));
    }
    prot->readSetEnd();
  }
  template <class Protocol>
  static uint32_t serializedSize(Protocol* prot, const Type* value) {
    using ElemType = typename Type::key_type;
    uint32_t xfer = 0;
    xfer += prot->serializedSizeSetBegin(
        Cpp2Ops<ElemType>::thriftType(), value->size());
    for (const auto& e : *value) {
      xfer += Cpp2Ops<ElemType>::serializedSize(prot, &e);
    }
    xfer += prot->serializedSizeSetEnd();
    return xfer;
  }
  template <class Protocol>
  static uint32_t serializedSizeZC(Protocol* prot, const Type* value) {
    using ElemType = typename Type::key_type;
    uint32_t xfer = 0;
    xfer += prot->serializedSizeSetBegin(
        Cpp2Ops<ElemType>::thriftType(), value->size());
    for (const auto& e : *value) {
      xfer += Cpp2Ops<ElemType>::serializedSizeZC(prot, &e);
    }
    xfer += prot->serializedSizeSetEnd();
    return xfer;
  }
};

template <class M>
class Cpp2Ops<
    M,
    std::enable_if_t<apache::thrift::detail::has_subscript_key_v<M>>> {
 private:
  template <typename Map, typename ValueType>
  std::enable_if_t<
      detail::alloc_should_propagate_map<Map>,
      ValueType&> static emplaceKey(Map& m, typename Map::key_type&& key) {
    return m.emplace(std::move(key), detail::default_map_value(m))
        .first->second;
  }
  template <typename Map, typename ValueType>
  std::enable_if_t<
      !detail::alloc_should_propagate_map<Map>,
      ValueType&> static emplaceKey(Map& m, typename Map::key_type&& key) {
    return m[std::move(key)];
  }

 public:
  using Type = M;
  static constexpr protocol::TType thriftType() { return protocol::T_MAP; }
  template <class Protocol>
  static uint32_t write(Protocol* prot, const Type* value) {
    using KeyType = typename Type::key_type;
    using PairType = typename std::remove_cv<
        typename std::remove_reference<decltype(*value->begin())>::type>::type;
    using ValueType = std::remove_cvref_t<typename PairType::second_type>;
    uint32_t xfer = 0;
    xfer += prot->writeMapBegin(
        Cpp2Ops<KeyType>::thriftType(),
        Cpp2Ops<ValueType>::thriftType(),
        value->size());
    for (const auto& e : *value) {
      xfer += Cpp2Ops<KeyType>::write(prot, &e.first);
      xfer += Cpp2Ops<ValueType>::write(prot, &e.second);
    }
    xfer += prot->writeMapEnd();
    return xfer;
  }
  template <class Protocol>
  static void read(Protocol* prot, Type* value) {
    using KeyType = typename Type::key_type;
    // We do this dance with decltype rather than just using Type::mapped_type
    // because different map implementations (such as Google's dense_hash_map)
    // call it data_type.
    using PairType = typename std::remove_cv<
        typename std::remove_reference<decltype(*value->begin())>::type>::type;
    using ValueType = std::remove_cvref_t<typename PairType::second_type>;
    value->clear();
    uint32_t size;
    protocol::TType keytype, valuetype;
    prot->readMapBegin(keytype, valuetype, size);
    apache::thrift::detail::Reserver<Type>::reserve(*value, size);
    for (uint32_t i = 0; i < size; i++) {
      KeyType key = detail::default_map_key(*value);
      Cpp2Ops<KeyType>::read(prot, &key);
      ValueType& val = emplaceKey<Type, ValueType>(*value, std::move(key));
      Cpp2Ops<ValueType>::read(prot, &val);
    }
    prot->readMapEnd();
  }
  template <class Protocol>
  static uint32_t serializedSize(Protocol* prot, const Type* value) {
    using KeyType = typename Type::key_type;
    using PairType = typename std::remove_cv<
        typename std::remove_reference<decltype(*value->begin())>::type>::type;
    using ValueType = std::remove_cvref_t<typename PairType::second_type>;
    uint32_t xfer = 0;
    xfer += prot->serializedSizeMapBegin(
        Cpp2Ops<KeyType>::thriftType(),
        Cpp2Ops<ValueType>::thriftType(),
        value->size());
    for (const auto& e : *value) {
      xfer += Cpp2Ops<KeyType>::serializedSize(prot, &e.first);
      xfer += Cpp2Ops<ValueType>::serializedSize(prot, &e.second);
    }
    xfer += prot->serializedSizeMapEnd();
    return xfer;
  }
  template <class Protocol>
  static uint32_t serializedSizeZC(Protocol* prot, const Type* value) {
    using KeyType = typename Type::key_type;
    using PairType = typename std::remove_cv<
        typename std::remove_reference<decltype(*value->begin())>::type>::type;
    using ValueType = std::remove_cvref_t<typename PairType::second_type>;
    uint32_t xfer = 0;
    xfer += prot->serializedSizeMapBegin(
        Cpp2Ops<KeyType>::thriftType(),
        Cpp2Ops<ValueType>::thriftType(),
        value->size());
    for (const auto& e : *value) {
      xfer += Cpp2Ops<KeyType>::serializedSizeZC(prot, &e.first);
      xfer += Cpp2Ops<ValueType>::serializedSizeZC(prot, &e.second);
    }
    xfer += prot->serializedSizeMapEnd();
    return xfer;
  }
};

template <>
class Cpp2Ops<folly::IOBuf>
    : public detail::Cpp2OpsViaOp<folly::IOBuf, type::binary_t> {};
template <>
class Cpp2Ops<IOBufChain>
    : public detail::Cpp2OpsViaOp<IOBufChain, type::binary_t> {};
template <>
class Cpp2Ops<std::unique_ptr<folly::IOBuf>>
    : public detail::
          Cpp2OpsViaOp<std::unique_ptr<folly::IOBuf>, type::binary_t> {};

template <class T>
class Cpp2Ops<
    T,
    std::enable_if_t<
        is_thrift_class_v<T> &&
        !folly::is_detected_v<detect_indirection_fn_t, T>>> {
 public:
  using Type = T;
  static constexpr protocol::TType thriftType() { return protocol::T_STRUCT; }
  template <class Protocol>
  static uint32_t write(Protocol* prot, const Type* value) {
    return value->write(prot);
  }
  template <class Protocol>
  static void read(Protocol* prot, Type* value) {
    detail::st::private_access::read_no_xfer_fn{}(*value, prot);
  }
  template <class Protocol>
  static uint32_t serializedSize(Protocol* prot, const Type* value) {
    return value->serializedSize(prot);
  }
  template <class Protocol>
  static uint32_t serializedSizeZC(Protocol* prot, const Type* value) {
    return value->serializedSizeZC(prot);
  }
};

template <class T>
class Cpp2Ops<
    T,
    std::enable_if_t<folly::is_detected_v<detect_indirection_fn_t, T>>> {
 private:
  using S = std::remove_cvref_t<
      std::invoke_result_t<detail::apply_indirection_fn, T>>;

 public:
  using Type = T;
  static constexpr protocol::TType thriftType() {
    return Cpp2Ops<S>::thriftType();
  }
  template <class Protocol>
  static uint32_t write(Protocol* prot, const Type* value) {
    return Cpp2Ops<S>::write(prot, &apply_indirection(*value));
  }
  template <class Protocol>
  static void read(Protocol* prot, Type* value) {
    return Cpp2Ops<S>::read(prot, &apply_indirection(*value));
  }
  template <class Protocol>
  static uint32_t serializedSize(Protocol* prot, const Type* value) {
    return Cpp2Ops<S>::serializedSize(prot, &apply_indirection(*value));
  }
  template <class Protocol>
  static uint32_t serializedSizeZC(Protocol* prot, const Type* value) {
    return Cpp2Ops<S>::serializedSizeZC(prot, &apply_indirection(*value));
  }
};

} // namespace thrift
} // namespace apache
