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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <memory_resource>
#include <set>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>
#include <gtest/gtest.h>
#include <folly/sorted_vector_types.h>
#include <thrift/conformance/cpp2/internal/AnyStructSerializer.h>
#include <thrift/lib/cpp/util/EnumUtils.h>
#include <thrift/lib/cpp2/TypeClass.h>
#include <thrift/lib/cpp2/op/Encode.h>
#include <thrift/lib/cpp2/protocol/CompactV1Protocol.h>
#include <thrift/lib/cpp2/protocol/DebugProtocol.h>
#include <thrift/lib/cpp2/protocol/Object.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>
#include <thrift/lib/cpp2/protocol/detail/protocol_methods.h>
#include <thrift/lib/cpp2/type/Tag.h>
#include <thrift/test/AdapterTest.h>
#include <thrift/test/gen-cpp2/SerializationInFieldIdOrder_types.h>
#include <thrift/test/gen-cpp2/structs_terse_types.h>
#include <thrift/test/testset/Testset.h>

using namespace ::apache::thrift::conformance;

using detail::protocol_reader_t;
using detail::protocol_writer_t;

namespace apache::thrift::op::detail {
namespace {
using apache::thrift::protocol::asValueStruct;
using apache::thrift::protocol::TType;
using detail::typeTagToTType;

struct WideCountProtocol {
  static constexpr std::size_t kElementBytes = static_cast<std::size_t>(1)
      << 32;

  std::size_t writeListBegin(TType, uint32_t) { return 5; }
  std::size_t writeI64(int64_t) { return kElementBytes; }
  std::size_t writeListEnd() { return 0; }
  std::size_t writeSetBegin(TType, uint32_t) { return 5; }
  std::size_t writeSetEnd() { return 7; }
  std::size_t writeMapBegin(TType, TType, uint32_t, bool) { return 5; }
  std::size_t writeMapEnd() { return 7; }
  KeyOrder keyOrder() const { return KeyOrder::Unspecified; }
  std::size_t serializedSizeListBegin(TType, uint32_t) { return 5; }
  std::size_t serializedSizeI64(int64_t) { return kElementBytes; }
  std::size_t serializedSizeListEnd() { return 0; }
};

TEST(EncodeTest, TypeTagToTType) {
  EXPECT_EQ(typeTagToTType<type::bool_t>, TType::T_BOOL);
  EXPECT_EQ(typeTagToTType<type::byte_t>, TType::T_BYTE);
  EXPECT_EQ(typeTagToTType<type::i16_t>, TType::T_I16);
  EXPECT_EQ(typeTagToTType<type::i32_t>, TType::T_I32);
  EXPECT_EQ(typeTagToTType<type::i64_t>, TType::T_I64);
  EXPECT_EQ(typeTagToTType<type::float_t>, TType::T_FLOAT);
  EXPECT_EQ(typeTagToTType<type::double_t>, TType::T_DOUBLE);
  EXPECT_EQ(typeTagToTType<type::string_t>, TType::T_UTF7);
  EXPECT_EQ(typeTagToTType<type::binary_t>, TType::T_STRING);
  EXPECT_EQ(typeTagToTType<type::enum_t<void>>, TType::T_I32);
  EXPECT_EQ(typeTagToTType<type::struct_t<void>>, TType::T_STRUCT);
  EXPECT_EQ(typeTagToTType<type::exception_t<void>>, TType::T_STRUCT);
  EXPECT_EQ(typeTagToTType<type::union_t<void>>, TType::T_STRUCT);
  EXPECT_EQ(typeTagToTType<type::list<type::string_t>>, TType::T_LIST);
  EXPECT_EQ(
      (typeTagToTType<type::set<type::list<type::bool_t>>>), TType::T_SET);
  EXPECT_EQ(
      (typeTagToTType<type::map<type::i32_t, type::list<type::bool_t>>>),
      TType::T_MAP);
  // test adapted
  EXPECT_EQ(
      (typeTagToTType<type::adapted<void, type::float_t>>), TType::T_FLOAT);
  EXPECT_EQ(
      (typeTagToTType<type::adapted<void, type::list<type::string_t>>>),
      TType::T_LIST);
  EXPECT_EQ(
      (typeTagToTType<type::adapted<void, type::struct_t<void>>>),
      TType::T_STRUCT);
}

TEST(EncodeTest, WideByteCounts) {
  WideCountProtocol protocol;

  const std::vector<int64_t> values{1};
  const auto expected = WideCountProtocol::kElementBytes + 5;
  EXPECT_EQ(expected, (op::encode<type::list<type::i64_t>>)(protocol, values));
  EXPECT_EQ(
      expected,
      (op::serialized_size<false, type::list<type::i64_t>>)(protocol, values));
}

struct EnumNameProtocol {
  std::string name;
  std::int32_t value = 0;

  std::size_t writeEnum(std::string_view enumName, std::int32_t enumValue) {
    name = enumName;
    value = enumValue;
    return 1;
  }
};

struct I32Protocol {
  std::int32_t value = 0;

  std::size_t writeI32(std::int32_t i32) {
    value = i32;
    return 4;
  }
};

TEST(EncodeTest, WriteEnumValueLooksUpTheNameOnlyForProtocolsThatWriteIt) {
  static_assert(writes_enum_names_v<EnumNameProtocol>);
  static_assert(!writes_enum_names_v<I32Protocol>);

  EnumNameProtocol named;
  EXPECT_EQ(writeEnumValue(named, 7, [] { return "SEVEN"; }), 1);
  EXPECT_EQ(named.name, "SEVEN");
  EXPECT_EQ(named.value, 7);

  I32Protocol numeric;
  bool lookedUp = false;
  EXPECT_EQ(
      writeEnumValue(
          numeric,
          7,
          [&] {
            lookedUp = true;
            return "SEVEN";
          }),
      4);
  EXPECT_EQ(numeric.value, 7);
  EXPECT_FALSE(lookedUp);
}

// Only Thrift-generated enums have `TEnumTraits` to look the name up in.
enum class NonThriftEnum { Seven = 7 };

TEST(EncodeTest, WriteEnumWritesNonThriftEnumsByValue) {
  I32Protocol protocol;
  EXPECT_EQ(writeEnum(protocol, NonThriftEnum::Seven), 4);
  EXPECT_EQ(protocol.value, 7);
}

struct NoSpecialHandlingCompactWriter : CompactProtocolWriter {
  static constexpr bool kHasFixedSerializedSizeForNumbers() { return false; }
};

template <typename Tag, typename T>
void testSerializedSizeForDifferentFlags(const T& value) {
  SCOPED_TRACE(folly::pretty_name<Tag>());
  CompactProtocolWriter writer;
  NoSpecialHandlingCompactWriter noSpecialHandling;
  EXPECT_EQ(
      (op::serialized_size<false, Tag>)(noSpecialHandling, value),
      (op::serialized_size<false, Tag>)(writer, value));
}

TEST(EncodeTest, SerializedSizeForDifferentFlags) {
  testSerializedSizeForDifferentFlags<type::list<type::i64_t>>(
      std::vector<int64_t>{1, 2, 3});
  testSerializedSizeForDifferentFlags<type::set<type::i64_t>>(
      std::set<int64_t>{1, 2, 3});
  testSerializedSizeForDifferentFlags<type::map<type::i64_t, type::i64_t>>(
      std::map<int64_t, int64_t>{{1, 1}, {2, 2}, {3, 3}});
  testSerializedSizeForDifferentFlags<
      type::set<type::cpp_type<uint64_t, type::i64_t>>>(
      std::set<uint64_t>{1, 2, 3});
  testSerializedSizeForDifferentFlags<type::map<type::i64_t, type::string_t>>(
      std::map<int64_t, std::string>{{1, "a"}, {2, "b"}, {3, "c"}});

  using Adapted = type::adapted<test::TemplatedTestAdapter, type::i64_t>;
  const std::set<test::Wrapper<int64_t>> wrapped{{1}, {2}, {3}};
  testSerializedSizeForDifferentFlags<type::set<Adapted>>(wrapped);
  testSerializedSizeForDifferentFlags<
      type::set<type::cpp_type<test::Wrapper<int64_t>, Adapted>>>(wrapped);
}

template <typename Tag, typename Writer>
void expectSameSerializedSizeForAllValues(Writer& writer) {
  SCOPED_TRACE(folly::pretty_name<Tag>());
  using T = type::native_type<Tag>;
  const auto size = [&](T value) {
    return op::serialized_size<false, Tag>(writer, value);
  };
  EXPECT_EQ(size(T{}), size(std::numeric_limits<T>::lowest()));
  EXPECT_EQ(size(T{}), size(std::numeric_limits<T>::max()));
}

template <typename Writer>
void expectFixedSerializedSizeForNumbers() {
  SCOPED_TRACE(folly::pretty_name<Writer>());
  EXPECT_TRUE(Writer::kHasFixedSerializedSizeForNumbers());
  Writer writer;
  expectSameSerializedSizeForAllValues<type::bool_t>(writer);
  expectSameSerializedSizeForAllValues<type::byte_t>(writer);
  expectSameSerializedSizeForAllValues<type::i16_t>(writer);
  expectSameSerializedSizeForAllValues<type::i32_t>(writer);
  expectSameSerializedSizeForAllValues<type::i64_t>(writer);
  expectSameSerializedSizeForAllValues<type::float_t>(writer);
  expectSameSerializedSizeForAllValues<type::double_t>(writer);
}

TEST(EncodeTest, StandardWritersHaveFixedSerializedSizeForNumbers) {
  expectFixedSerializedSizeForNumbers<BinaryProtocolWriter>();
  expectFixedSerializedSizeForNumbers<CompactProtocolWriter>();
  expectFixedSerializedSizeForNumbers<CompactV1ProtocolWriter>();
  expectFixedSerializedSizeForNumbers<JSONProtocolWriter>();
  expectFixedSerializedSizeForNumbers<SimpleJSONProtocolWriter>();
}

TEST(EncodeTest, SetWideByteCounts) {
  WideCountProtocol protocol;
  const std::set<int64_t> values{1, 2};
  const auto expected = 2 * WideCountProtocol::kElementBytes + 12;
  using Tag = type::set<type::i64_t>;
  using Methods = apache::thrift::detail::pm::protocol_methods<
      type_class::set<type_class::integral>,
      std::set<int64_t>,
      Tag>;
  EXPECT_EQ(expected, op::encode<Tag>(protocol, values));
  EXPECT_EQ(expected, Methods::write(protocol, values));
}

template <typename Protocol>
struct SetOrderingWriter : Protocol {
  KeyOrder order = KeyOrder::Unspecified;
  KeyOrder keyOrder() const { return order; }
};

template <typename Protocol, typename Set>
void testSetWriteParity(const Set& values) {
  using Tag = type::set<type::i32_t>;
  using Methods = apache::thrift::detail::pm::
      protocol_methods<type_class::set<type_class::integral>, Set, Tag>;
  for (auto order :
       {KeyOrder::Unspecified,
        KeyOrder::NativeAscending,
        KeyOrder::StableAscending}) {
    SCOPED_TRACE(static_cast<int>(order));
    SetOrderingWriter<Protocol> writer;
    writer.order = order;
    folly::IOBufQueue opQueue;
    writer.setOutput(&opQueue);
    const auto opSize = op::encode<Tag>(writer, values);
    folly::IOBufQueue methodsQueue;
    writer.setOutput(&methodsQueue);
    const auto methodsSize = Methods::write(writer, values);
    auto opBytes = opQueue.move();
    auto methodsBytes = methodsQueue.move();
    EXPECT_EQ(opSize, opBytes->computeChainDataLength());
    EXPECT_EQ(methodsSize, methodsBytes->computeChainDataLength());
    EXPECT_TRUE(folly::IOBufEqualTo{}(*opBytes, *methodsBytes));
  }
}

TEST(EncodeTest, SetWriteParity) {
  const std::set<int32_t, std::greater<int32_t>> ordered{3, 2, 1};
  const std::unordered_set<int32_t> unordered{3, 2, 1};
  testSetWriteParity<BinaryProtocolWriter>(ordered);
  testSetWriteParity<BinaryProtocolWriter>(unordered);
  testSetWriteParity<CompactProtocolWriter>(ordered);
  testSetWriteParity<CompactProtocolWriter>(unordered);
  testSetWriteParity<SimpleJSONProtocolWriter>(ordered);
  testSetWriteParity<SimpleJSONProtocolWriter>(unordered);
  testSetWriteParity<CompactProtocolWriter>(std::set<int32_t>{});
}

TEST(EncodeTest, MapWideByteCounts) {
  WideCountProtocol protocol;
  const std::map<int64_t, int64_t> values{{1, 2}};
  const auto expected = 2 * WideCountProtocol::kElementBytes + 12;
  using Tag = type::map<type::i64_t, type::i64_t>;
  using Methods = apache::thrift::detail::pm::protocol_methods<
      type_class::map<type_class::integral, type_class::integral>,
      std::map<int64_t, int64_t>,
      Tag>;
  EXPECT_EQ(expected, op::encode<Tag>(protocol, values));
  EXPECT_EQ(expected, Methods::write(protocol, values));
}

template <typename Protocol, typename Map>
void testMapWriteParity(const Map& values) {
  using Tag = type::map<type::i32_t, type::string_t>;
  using Methods = apache::thrift::detail::pm::protocol_methods<
      type_class::map<type_class::integral, type_class::string>,
      Map,
      Tag>;
  for (auto order :
       {KeyOrder::Unspecified,
        KeyOrder::NativeAscending,
        KeyOrder::StableAscending}) {
    SCOPED_TRACE(static_cast<int>(order));
    SetOrderingWriter<Protocol> writer;
    writer.order = order;
    folly::IOBufQueue opQueue;
    writer.setOutput(&opQueue);
    const auto opSize = op::encode<Tag>(writer, values);
    folly::IOBufQueue methodsQueue;
    writer.setOutput(&methodsQueue);
    const auto methodsSize = Methods::write(writer, values);
    auto opBytes = opQueue.move();
    auto methodsBytes = methodsQueue.move();
    EXPECT_EQ(opSize, opBytes->computeChainDataLength());
    EXPECT_EQ(methodsSize, methodsBytes->computeChainDataLength());
    EXPECT_TRUE(folly::IOBufEqualTo{}(*opBytes, *methodsBytes));
  }
}

TEST(EncodeTest, MapWriteParity) {
  const std::map<int32_t, std::string, std::greater<int32_t>> ordered{
      {3, "c"}, {2, "b"}, {1, "a"}};
  const std::unordered_map<int32_t, std::string> unordered{
      {3, "c"}, {2, "b"}, {1, "a"}};
  testMapWriteParity<BinaryProtocolWriter>(ordered);
  testMapWriteParity<BinaryProtocolWriter>(unordered);
  testMapWriteParity<CompactProtocolWriter>(ordered);
  testMapWriteParity<CompactProtocolWriter>(unordered);
  testMapWriteParity<SimpleJSONProtocolWriter>(ordered);
  testMapWriteParity<SimpleJSONProtocolWriter>(unordered);
  testMapWriteParity<CompactProtocolWriter>(std::map<int32_t, std::string>{});
}

struct MapBeginRecorder : BinaryProtocolWriter {
  bool alternativeKeyForm = false;
  uint32_t writeMapBegin(
      TType keyType, TType valueType, uint32_t size, bool alternative) {
    alternativeKeyForm = alternative;
    return BinaryProtocolWriter::writeMapBegin(
        keyType, valueType, size, alternative);
  }
};

template <typename KeyClass, typename KeyTag, typename Key>
void testMapAlternativeKeyForm(bool expected) {
  using Map = std::map<Key, int32_t>;
  using Tag = type::map<KeyTag, type::i32_t>;
  using Methods = apache::thrift::detail::pm::protocol_methods<
      type_class::map<KeyClass, type_class::integral>,
      Map,
      Tag>;
  folly::IOBufQueue queue;
  MapBeginRecorder opWriter;
  opWriter.setOutput(&queue);
  op::encode<Tag>(opWriter, Map{});
  EXPECT_EQ(opWriter.alternativeKeyForm, expected);
  MapBeginRecorder methodsWriter;
  methodsWriter.setOutput(&queue);
  Methods::write(methodsWriter, Map{});
  EXPECT_EQ(methodsWriter.alternativeKeyForm, expected);
}

TEST(EncodeTest, MapAlternativeKeyForm) {
  testMapAlternativeKeyForm<type_class::string, type::string_t, std::string>(
      true);
  testMapAlternativeKeyForm<type_class::binary, type::binary_t, std::string>(
      false);
  testMapAlternativeKeyForm<
      type_class::enumeration,
      type::enum_t<StandardProtocol>,
      StandardProtocol>(true);
  testMapAlternativeKeyForm<type_class::integral, type::i32_t, int32_t>(false);
}

template <
    conformance::StandardProtocol Protocol,
    bool ZeroCopy,
    typename Tag,
    typename TypeClass,
    bool IsAdapted = false,
    typename T>
void testSerializedSize(T value) {
  SCOPED_TRACE(folly::pretty_name<Tag>());
  protocol_writer_t<Protocol> writer;
  std::size_t size;
  if constexpr (IsAdapted) {
    using AdaptedTag = type::adapted<test::TemplatedTestAdapter, Tag>;
    size = op::serialized_size<ZeroCopy, AdaptedTag>(
        writer, test::TemplatedTestAdapter::fromThrift(value));
  } else {
    size = op::serialized_size<ZeroCopy, Tag>(writer, value);
  }
  std::size_t expected =
      apache::thrift::detail::pm::protocol_methods<TypeClass, T, Tag>::
          template serializedSize<ZeroCopy>(writer, value);
  EXPECT_EQ(size, expected);
}

template <
    conformance::StandardProtocol Protocol,
    typename Tag,
    typename TypeClass,
    bool IsAdapted = false,
    typename T>
void testSerializedSize(T value) {
  testSerializedSize<Protocol, false, Tag, TypeClass, IsAdapted>(value);
  testSerializedSize<Protocol, true, Tag, TypeClass, IsAdapted>(value);
}

template <conformance::StandardProtocol Protocol>
void testSerializedSizeBasicTypes() {
  SCOPED_TRACE(apache::thrift::util::enumNameSafe(Protocol));
  testSerializedSize<Protocol, type::bool_t, type_class::integral>(true);
  testSerializedSize<Protocol, type::byte_t, type_class::integral>((int8_t)1);
  testSerializedSize<Protocol, type::i16_t, type_class::integral>((int16_t)1);
  testSerializedSize<Protocol, type::i32_t, type_class::integral>((int32_t)1);
  testSerializedSize<Protocol, type::i64_t, type_class::integral>((int64_t)1);
  testSerializedSize<Protocol, type::float_t, type_class::floating_point>(1.5f);
  testSerializedSize<Protocol, type::double_t, type_class::floating_point>(1.5);
  testSerializedSize<Protocol, type::string_t, type_class::string>(
      std::string("foo"));
  testSerializedSize<Protocol, type::string_t, type_class::string>(
      folly::StringPiece("foo"));
  testSerializedSize<Protocol, type::string_t, type_class::string>("foo");
  testSerializedSize<Protocol, type::binary_t, type_class::binary>(
      std::string("foo"));
  testSerializedSize<Protocol, type::binary_t, type_class::binary>(
      folly::StringPiece("foo"));
  testSerializedSize<Protocol, type::binary_t, type_class::binary>("foo");
  testSerializedSize<
      Protocol,
      type::enum_t<StandardProtocol>,
      type_class::enumeration>(StandardProtocol::Binary);
}

template <conformance::StandardProtocol Protocol>
void testSerializedSizeUnsignedTypes() {
  SCOPED_TRACE(apache::thrift::util::enumNameSafe(Protocol));
  testSerializedSize<
      Protocol,
      type::cpp_type<uint8_t, type::byte_t>,
      type_class::integral>(uint8_t{128});
  testSerializedSize<
      Protocol,
      type::cpp_type<uint16_t, type::i16_t>,
      type_class::integral>(uint16_t{32768});
  testSerializedSize<
      Protocol,
      type::cpp_type<uint32_t, type::i32_t>,
      type_class::integral>(uint32_t{2147483648});
  testSerializedSize<
      Protocol,
      type::cpp_type<uint64_t, type::i64_t>,
      type_class::integral>(uint64_t{9223372036854775808ULL});
}

template <conformance::StandardProtocol Protocol>
void testSerializedSizeContainers() {
  SCOPED_TRACE(apache::thrift::util::enumNameSafe(Protocol));
  testSerializedSize<
      Protocol,
      type::list<type::bool_t>,
      type_class::list<type_class::integral>>(
      std::vector<bool>{true, false, true});
  testSerializedSize<
      Protocol,
      type::set<type::bool_t>,
      type_class::set<type_class::integral>>(std::set<bool>{true, false});
  testSerializedSize<
      Protocol,
      type::map<type::string_t, type::byte_t>,
      type_class::map<type_class::string, type_class::integral>>(
      std::map<std::string, int8_t>{
          {std::string("foo"), 1}, {std::string("foo"), 2}});
}

enum class EmptyEnum : std::int16_t {};

template <typename T>
class PropagatingAllocator {
 public:
  using value_type = T;
  using propagate_on_container_move_assignment = std::true_type;

  explicit PropagatingAllocator(int state = 0) : state_(state) {}

  template <typename U>
  PropagatingAllocator(const PropagatingAllocator<U>& other)
      : state_(other.state()) {}

  [[nodiscard]] T* allocate(std::size_t count) {
    return std::allocator<T>{}.allocate(count);
  }

  void deallocate(T* pointer, std::size_t count) {
    std::allocator<T>{}.deallocate(pointer, count);
  }

  int state() const { return state_; }

  template <typename U>
  bool operator==(const PropagatingAllocator<U>& other) const {
    return state_ == other.state();
  }

 private:
  int state_;
};

struct StatefulCompare {
  bool descending = false;

  bool operator()(std::int32_t lhs, std::int32_t rhs) const {
    return descending ? lhs > rhs : lhs < rhs;
  }
};

class ConstReferenceList {
 public:
  using value_type = std::int32_t;
  using reference = const value_type&;
  using size_type = std::vector<value_type>::size_type;

  void emplace_back(value_type value) { values_.emplace_back(value); }
  void clear() { values_.clear(); }

  const std::vector<value_type>& values() const { return values_; }

 private:
  std::vector<value_type> values_;
};

template <conformance::StandardProtocol Protocol>
void testSerializedSizeCppType() {
  SCOPED_TRACE(apache::thrift::util::enumNameSafe(Protocol));
  // test cpp_type with primitive
  testSerializedSize<
      Protocol,
      type::cpp_type<int, type::i16_t>,
      type_class::integral>((int16_t)1);

  // test strongly typed integer
  testSerializedSize<
      Protocol,
      type::cpp_type<EmptyEnum, type::i16_t>,
      type_class::integral>(static_cast<EmptyEnum>(1));

  {
    // test cpp_type with list
    using T = std::deque<int64_t>;
    auto value = T{1, 2, 3};
    using Tag = type::list<type::i64_t>;
    testSerializedSize<
        Protocol,
        type::cpp_type<T, Tag>,
        type_class::list<type_class::integral>>(value);
  }
  {
    // test cpp_type with set
    using T = std::unordered_set<std::string>;
    auto value = T{"foo", "bar"};
    using Tag = type::set<type::string_t>;
    testSerializedSize<
        Protocol,
        type::cpp_type<T, Tag>,
        type_class::set<type_class::string>>(value);
  }
  {
    // test cpp_type with map
    using T = std::unordered_map<std::string, int32_t>;
    auto value = T{{"foo", 1}, {"bar", 2}};
    using Tag = type::map<type::string_t, type::i32_t>;
    testSerializedSize<
        Protocol,
        type::cpp_type<T, Tag>,
        type_class::map<type_class::string, type_class::integral>>(value);
  }
}

template <
    conformance::StandardProtocol Protocol,
    typename Struct,
    typename Tag,
    typename TypeClass,
    bool IsAdapted = false,
    typename T>
void testSerializedSizeObject(T value) {
  Struct s;
  s.field_1_ref() = value;
  testSerializedSize<Protocol, Tag, TypeClass, IsAdapted>(s);
}

template <conformance::StandardProtocol Protocol>
void testSerializedSizeStruct() {
  SCOPED_TRACE(apache::thrift::util::enumNameSafe(Protocol));
  using Struct =
      test::testset::struct_with<type::map<type::string_t, type::i32_t>>;
  std::map<std::string, int> mapValues = {{"one", 1}, {"four", 4}, {"two", 2}};
  testSerializedSizeObject<
      Protocol,
      Struct,
      type::struct_t<Struct>,
      type_class::structure>(mapValues);
  using Union = test::testset::union_with<type::set<type::string_t>>;
  std::set<std::string> setValues = {"foo", "bar", "baz"};
  testSerializedSizeObject<
      Protocol,
      Union,
      type::union_t<Union>,
      type_class::structure>(setValues);
  using Exception = test::testset::exception_with<type::i64_t>;
  testSerializedSizeObject<
      Protocol,
      Exception,
      type::exception_t<Exception>,
      type_class::structure>(1);
}

template <conformance::StandardProtocol Protocol>
void testSerializedSizeAdapted() {
  SCOPED_TRACE(apache::thrift::util::enumNameSafe(Protocol));
  testSerializedSize<Protocol, type::string_t, type_class::string, true>("foo");
  testSerializedSize<
      Protocol,
      type::set<type::i32_t>,
      type_class::set<type_class::integral>,
      true>(std::set<int32_t>{1, 2, 3});
  using Struct = test::testset::struct_with<type::i32_t>;
  testSerializedSizeObject<
      Protocol,
      Struct,
      type::struct_t<Struct>,
      type_class::structure,
      true>(1);
  using Union = test::testset::union_with<type::i32_t>;
  testSerializedSizeObject<
      Protocol,
      Union,
      type::union_t<Union>,
      type_class::structure,
      true>(1);
}

TEST(SerializedSizeTest, SerializedSizeBasicTypes) {
  testSerializedSizeBasicTypes<conformance::StandardProtocol::Binary>();
  testSerializedSizeBasicTypes<conformance::StandardProtocol::Compact>();
}

TEST(SerializedSizeTest, SerializedSizeUnsignedTypes) {
  testSerializedSizeUnsignedTypes<conformance::StandardProtocol::Binary>();
  testSerializedSizeUnsignedTypes<conformance::StandardProtocol::Compact>();
}

TEST(SerializedSizeTest, SerializedSizeContainers) {
  testSerializedSizeContainers<conformance::StandardProtocol::Binary>();
  testSerializedSizeContainers<conformance::StandardProtocol::Compact>();
}

TEST(SerializedSizeTest, SerializedSizeCppType) {
  testSerializedSizeCppType<conformance::StandardProtocol::Binary>();
  testSerializedSizeCppType<conformance::StandardProtocol::Compact>();
}

TEST(SerializedSizeTest, SerializedSizeStruct) {
  testSerializedSizeStruct<conformance::StandardProtocol::Binary>();
  testSerializedSizeStruct<conformance::StandardProtocol::Compact>();
}

TEST(SerializedSizeTest, SerializedSizeAdapted) {
  testSerializedSizeAdapted<conformance::StandardProtocol::Binary>();
  testSerializedSizeAdapted<conformance::StandardProtocol::Compact>();
}

template <conformance::StandardProtocol Protocol, typename Tag>
protocol::Value encodeAndParseValue(
    type::native_type<Tag> value, TType ttype, bool string_to_binary = true) {
  protocol_writer_t<Protocol> writer;
  folly::IOBufQueue queue;
  writer.setOutput(&queue);
  encode<Tag>(writer, value);
  protocol_reader_t<Protocol> reader;
  auto serialized = queue.move();
  reader.setInput(serialized.get());
  return protocol::detail::parseValue(reader, ttype, string_to_binary);
}

template <conformance::StandardProtocol Protocol, typename Tag>
void testEncode(type::native_type<Tag> value, bool string_to_binary = true) {
  SCOPED_TRACE(folly::pretty_name<Tag>());
  auto result = encodeAndParseValue<Protocol, Tag>(
      value, typeTagToTType<Tag>, string_to_binary);
  EXPECT_EQ(result, asValueStruct<Tag>(value));
}

template <conformance::StandardProtocol Protocol>
void testEncodeBasicTypes() {
  SCOPED_TRACE(apache::thrift::util::enumNameSafe(Protocol));
  testEncode<Protocol, type::bool_t>(true);
  testEncode<Protocol, type::byte_t>(1);
  testEncode<Protocol, type::i16_t>(1);
  testEncode<Protocol, type::i32_t>(1);
  testEncode<Protocol, type::i64_t>(1);
  testEncode<Protocol, type::float_t>(1.5);
  testEncode<Protocol, type::double_t>(1.5);
  testEncode<Protocol, type::string_t>(std::string("foo"), false);
  testEncode<Protocol, type::cpp_type<folly::StringPiece, type::string_t>>(
      folly::StringPiece("foo"), false);
  testEncode<Protocol, type::string_t>("foo", false);
  testEncode<Protocol, type::binary_t>("foo");
  testEncode<Protocol, type::enum_t<StandardProtocol>>(
      StandardProtocol::Binary);
}

template <conformance::StandardProtocol Protocol>
void testEncodeUnsignedTypes() {
  SCOPED_TRACE(apache::thrift::util::enumNameSafe(Protocol));
  testEncode<Protocol, type::cpp_type<uint8_t, type::byte_t>>(128U);
  testEncode<Protocol, type::cpp_type<uint16_t, type::i16_t>>(32768U);
  testEncode<Protocol, type::cpp_type<uint32_t, type::i32_t>>(2147483648UL);
  testEncode<Protocol, type::cpp_type<uint64_t, type::i64_t>>(
      9223372036854775808ULL);
}

template <conformance::StandardProtocol Protocol>
void testEncodeContainers() {
  SCOPED_TRACE(apache::thrift::util::enumNameSafe(Protocol));
  testEncode<Protocol, type::list<type::enum_t<StandardProtocol>>>(
      std::vector<StandardProtocol>{
          StandardProtocol::Binary,
          StandardProtocol::Compact,
          StandardProtocol::Json});
  testEncode<Protocol, type::set<type::bool_t>>(std::set<bool>{true, false});
  testEncode<Protocol, type::map<type::string_t, type::byte_t>>(
      std::map<std::string, int8_t>{{"foo", 1}, {"bar", 2}}, false);
}

template <conformance::StandardProtocol Protocol>
void testEncodeCppType() {
  SCOPED_TRACE(apache::thrift::util::enumNameSafe(Protocol));
  {
    // test strongly typed integer
    auto result =
        encodeAndParseValue<Protocol, type::cpp_type<EmptyEnum, type::i16_t>>(
            static_cast<EmptyEnum>(1), TType::T_I16);
    EXPECT_EQ(result, asValueStruct<type::i16_t>(1));
  }
  {
    // test cpp_type with list
    using T = std::list<int64_t>;
    auto value = T{1, 2, 3};
    using Tag = type::list<type::i64_t>;
    auto result = encodeAndParseValue<Protocol, type::cpp_type<T, Tag>>(
        value, TType::T_LIST);
    EXPECT_EQ(result, asValueStruct<Tag>(value));
  }
  {
    // test cpp_type with set
    using T = std::unordered_set<std::string>;
    auto value = T{"foo", "bar"};
    using Tag = type::set<type::string_t>;
    auto result = encodeAndParseValue<Protocol, type::cpp_type<T, Tag>>(
        value, TType::T_SET, false);
    EXPECT_EQ(result, asValueStruct<Tag>(value));
  }
  {
    // test cpp_type with map
    using T = std::unordered_map<std::string, int32_t>;
    auto value = T{{"foo", 1}, {"bar", 2}};
    using Tag = type::map<type::string_t, type::i32_t>;
    auto result = encodeAndParseValue<Protocol, type::cpp_type<T, Tag>>(
        value, TType::T_MAP, false);
    EXPECT_EQ(result, asValueStruct<Tag>(value));
  }
}

// If IsAdapted is true, test op::encode with the given object with
// type::adapted<test::TemplatedTestAdapter, Tag> tag and fromThrift.
template <
    conformance::StandardProtocol Protocol,
    typename Struct,
    typename Tag,
    bool IsAdapted = false,
    typename T>
void testEncodeObject(T value) {
  SCOPED_TRACE(folly::pretty_name<Tag>());
  Struct foo;
  foo.field_1_ref() = value;
  protocol_writer_t<Protocol> w1, w2;
  folly::IOBufQueue o1, o2;
  w1.setOutput(&o1);
  w2.setOutput(&o2);
  if constexpr (IsAdapted) {
    using AdaptedTag = type::adapted<test::TemplatedTestAdapter, Tag>;
    encode<AdaptedTag>(w1, test::TemplatedTestAdapter::fromThrift(foo));
  } else {
    encode<Tag>(w1, foo);
  }
  foo.write(&w2);
  EXPECT_TRUE(folly::IOBufEqualTo{}(*o1.move(), *o2.move()));
}

template <conformance::StandardProtocol Protocol>
void testEncodeStruct() {
  SCOPED_TRACE(apache::thrift::util::enumNameSafe(Protocol));
  using Struct =
      test::testset::struct_with<type::map<type::string_t, type::i32_t>>;
  std::map<std::string, int> mapValues = {{"one", 1}, {"four", 4}, {"two", 2}};
  testEncodeObject<Protocol, Struct, type::struct_t<Struct>>(mapValues);
  using Union = test::testset::union_with<type::set<type::string_t>>;
  std::set<std::string> setValues = {"foo", "bar", "baz"};
  testEncodeObject<Protocol, Union, type::union_t<Union>>(setValues);
  using Exception = test::testset::exception_with<type::i64_t>;
  testEncodeObject<Protocol, Exception, type::exception_t<Exception>>(1);
}

template <conformance::StandardProtocol Protocol>
void testEncodeAdapted() {
  SCOPED_TRACE(apache::thrift::util::enumNameSafe(Protocol));
  {
    // test op::encode with adapted struct
    using Struct = test::testset::struct_with<type::i32_t>;
    testEncodeObject<Protocol, Struct, type::struct_t<Struct>, true>(1);
  }
  {
    // test op::encode with adapted primitive type
    using AdaptedTag = type::adapted<test::TemplatedTestAdapter, type::i16_t>;
    auto result = encodeAndParseValue<Protocol, AdaptedTag>(
        test::TemplatedTestAdapter::fromThrift(static_cast<int16_t>(1)),
        TType::T_I16);
    EXPECT_EQ(result, asValueStruct<type::i16_t>(1));
  }
  {
    // test op::encode with adapted container
    auto value = std::map<std::string, int32_t>{{"foo", 1}, {"bar", 2}};
    using Tag = type::map<type::string_t, type::i32_t>;
    using AdaptedTag = type::adapted<test::TemplatedTestAdapter, Tag>;
    auto result = encodeAndParseValue<Protocol, AdaptedTag>(
        test::TemplatedTestAdapter::fromThrift(value), TType::T_MAP, false);
    EXPECT_EQ(result, asValueStruct<Tag>(value));
  }
  {
    // test op::encode with Adapter::encode optimization
    using AdaptedTag = type::adapted<test::EncodeAdapter, type::i64_t>;
    test::Num value{1};
    auto result =
        encodeAndParseValue<Protocol, AdaptedTag>(value, TType::T_I64);
    EXPECT_EQ(result, asValueStruct<type::i64_t>(1));
  }
}

TEST(EncodeTest, EncodeBasicTypes) {
  testEncodeBasicTypes<conformance::StandardProtocol::Binary>();
  testEncodeBasicTypes<conformance::StandardProtocol::Compact>();
}

TEST(EncodeTest, EncodeUnsignedTypes) {
  testEncodeUnsignedTypes<conformance::StandardProtocol::Binary>();
  testEncodeUnsignedTypes<conformance::StandardProtocol::Compact>();
}

TEST(EncodeTest, EncodeContainers) {
  testEncodeContainers<conformance::StandardProtocol::Binary>();
  testEncodeContainers<conformance::StandardProtocol::Compact>();
}

TEST(EncodeTest, EncodeStruct) {
  testEncodeStruct<conformance::StandardProtocol::Binary>();
  testEncodeStruct<conformance::StandardProtocol::Compact>();
}

TEST(EncodeTest, EncodeCppType) {
  testEncodeCppType<conformance::StandardProtocol::Binary>();
  testEncodeCppType<conformance::StandardProtocol::Compact>();
}

TEST(EncodeTest, EncodeAdapted) {
  testEncodeAdapted<conformance::StandardProtocol::Binary>();
  testEncodeAdapted<conformance::StandardProtocol::Compact>();
}

// Encodes the given value with EncodeTag, and decodes the result with DecodeTag
// to DecodeT type using the Protocol.
template <
    conformance::StandardProtocol Protocol,
    typename EncodeTag,
    typename DecodeTag,
    typename DecodeT,
    typename EncodeT>
void encodeAndDecodeInto(const EncodeT& value, DecodeT& result) {
  protocol_writer_t<Protocol> writer;
  folly::IOBufQueue queue;
  writer.setOutput(&queue);
  encode<EncodeTag>(writer, value);

  protocol_reader_t<Protocol> reader;
  auto serialized = queue.move();
  reader.setInput(serialized.get());
  decode<DecodeTag>(reader, result);
}

template <
    conformance::StandardProtocol Protocol,
    typename EncodeTag,
    typename DecodeTag,
    typename DecodeT,
    typename EncodeT>
DecodeT encodeAndDecode(EncodeT value) {
  DecodeT result;
  encodeAndDecodeInto<Protocol, EncodeTag, DecodeTag>(value, result);
  return result;
}

template <conformance::StandardProtocol Protocol, typename Tag>
void testDecode(type::native_type<Tag> value) {
  SCOPED_TRACE(folly::pretty_name<Tag>());
  EXPECT_EQ(
      (encodeAndDecode<Protocol, Tag, Tag, decltype(value)>(value)), value);
}

template <conformance::StandardProtocol Protocol>
void testDecodeBasicTypes() {
  SCOPED_TRACE(apache::thrift::util::enumNameSafe(Protocol));
  testDecode<Protocol, type::bool_t>(true);
  testDecode<Protocol, type::byte_t>((int8_t)1);
  testDecode<Protocol, type::i16_t>((int16_t)11);
  testDecode<Protocol, type::i32_t>((int32_t)11);
  testDecode<Protocol, type::i64_t>((int64_t)11);
  testDecode<Protocol, type::float_t>(1.5f);
  testDecode<Protocol, type::double_t>(1.5);
  testDecode<Protocol, type::string_t>(std::string("foo"));
  testDecode<Protocol, type::binary_t>(std::string("foo"));
  testDecode<Protocol, type::enum_t<StandardProtocol>>(
      StandardProtocol::Binary);
}

template <conformance::StandardProtocol Protocol>
void testDecodeUnsignedTypes() {
  SCOPED_TRACE(apache::thrift::util::enumNameSafe(Protocol));
  testDecode<Protocol, type::cpp_type<uint8_t, type::byte_t>>(128U);
  testDecode<Protocol, type::cpp_type<uint16_t, type::i16_t>>(32768U);
  testDecode<Protocol, type::cpp_type<uint32_t, type::i32_t>>(2147483648UL);
  testDecode<Protocol, type::cpp_type<uint64_t, type::i64_t>>(
      9223372036854775808ULL);
}

template <conformance::StandardProtocol Protocol>
void testDecodeContainers() {
  SCOPED_TRACE(apache::thrift::util::enumNameSafe(Protocol));
  testDecode<Protocol, type::list<type::enum_t<StandardProtocol>>>(
      std::vector<StandardProtocol>{
          StandardProtocol::Binary,
          StandardProtocol::Compact,
          StandardProtocol::Json});
  testDecode<Protocol, type::list<type::bool_t>>(
      std::vector<bool>{true, false, true});
  testDecode<Protocol, type::set<type::bool_t>>(std::set<bool>{true, false});
  testDecode<
      Protocol,
      type::cpp_type<std::unordered_set<bool>, type::set<type::bool_t>>>(
      std::unordered_set<bool>{true, false});
  testDecode<Protocol, type::map<type::string_t, type::byte_t>>(
      std::map<std::string, int8_t>{
          {std::string("foo"), 1}, {std::string("foo"), 2}});
  testDecode<
      Protocol,
      type::cpp_type<
          std::unordered_map<std::string, int8_t>,
          type::map<type::string_t, type::byte_t>>>(
      std::unordered_map<std::string, int8_t>{
          {std::string("foo"), 1}, {std::string("foo"), 2}});
  testDecode<
      Protocol,
      type::
          cpp_type<folly::sorted_vector_set<int8_t>, type::set<type::byte_t>>>(
      folly::sorted_vector_set<int8_t>{3, 1, 2});
  testDecode<
      Protocol,
      type::cpp_type<
          folly::sorted_vector_map<std::string, int8_t>,
          type::map<type::string_t, type::byte_t>>>(
      folly::sorted_vector_map<std::string, int8_t>{
          {std::string("foo"), 1}, {std::string("foo"), 2}});

  // Test if it skips when value type doesn't match.
  {
    auto result = encodeAndDecode<
        Protocol,
        type::list<type::i32_t>,
        type::list<type::string_t>,
        std::vector<std::string>>(std::vector<int32_t>{1, 2, 3});
    EXPECT_TRUE(result.empty());
  }
  {
    auto result = encodeAndDecode<
        Protocol,
        type::set<type::i32_t>,
        type::set<type::i64_t>,
        std::set<int64_t>>(std::set<int32_t>{1, 2, 3});
    EXPECT_TRUE(result.empty());
  }
  {
    auto result = encodeAndDecode<
        Protocol,
        type::map<type::i32_t, type::bool_t>,
        type::map<type::string_t, type::bool_t>,
        std::map<std::string, bool>>(
        std::map<int32_t, bool>{{1, true}, {2, false}});
    EXPECT_TRUE(result.empty());
  }
  {
    auto result = encodeAndDecode<
        Protocol,
        type::map<type::string_t, type::bool_t>,
        type::map<type::string_t, type::i32_t>,
        std::map<std::string, int32_t>>(
        std::map<std::string, bool>{{"1", true}, {"2", false}});
    EXPECT_TRUE(result.empty());
  }
}

template <
    conformance::StandardProtocol Protocol,
    typename Struct,
    typename Tag,
    bool IsAdapted = false,
    typename T>
void testDecodeObject(T value) {
  Struct s;
  s.field_1_ref() = value;
  if constexpr (IsAdapted) {
    using AdaptedTag = type::adapted<test::TemplatedTestAdapter, Tag>;
    testDecode<Protocol, AdaptedTag>(test::TemplatedTestAdapter::fromThrift(s));
  } else {
    testDecode<Protocol, Tag>(s);
  }
}

template <conformance::StandardProtocol Protocol>
void testDecodeStruct() {
  SCOPED_TRACE(apache::thrift::util::enumNameSafe(Protocol));
  using Struct =
      test::testset::struct_with<type::map<type::string_t, type::i32_t>>;
  std::map<std::string, int> mapValues = {{"one", 1}, {"four", 4}, {"two", 2}};
  testDecodeObject<Protocol, Struct, type::struct_t<Struct>>(mapValues);
  using Union = test::testset::union_with<type::set<type::string_t>>;
  std::set<std::string> setValues = {"foo", "bar", "baz"};
  testDecodeObject<Protocol, Union, type::union_t<Union>>(setValues);
  using Exception = test::testset::exception_with<type::i64_t>;
  testDecodeObject<Protocol, Exception, type::exception_t<Exception>>(1);
}

template <conformance::StandardProtocol Protocol>
void testDecodeCppType() {
  SCOPED_TRACE(apache::thrift::util::enumNameSafe(Protocol));
  {
    // test strongly typed integer
    testDecode<Protocol, type::cpp_type<EmptyEnum, type::i16_t>>(
        static_cast<EmptyEnum>(1));
  }
  {
    // test cpp_type with list
    using T = std::deque<int64_t>;
    auto value = T{1, 2, 3};
    using Tag = type::list<type::i64_t>;
    testDecode<Protocol, type::cpp_type<T, Tag>>(value);
  }
  {
    // test cpp_type with set
    using T = std::unordered_set<std::string>;
    auto value = T{"foo", "bar"};
    using Tag = type::set<type::string_t>;
    testDecode<Protocol, type::cpp_type<T, Tag>>(value);
  }
  {
    // test cpp_type with map
    using T = std::unordered_map<std::string, int32_t>;
    auto value = T{{"foo", 1}, {"bar", 2}};
    using Tag = type::map<type::string_t, type::i32_t>;
    testDecode<Protocol, type::cpp_type<T, Tag>>(value);
  }
}

template <
    conformance::StandardProtocol Protocol,
    typename Struct,
    typename Tag,
    typename T>
void testEncodeAndDecodeFieldAdapted(T value) {
  SCOPED_TRACE(folly::pretty_name<Tag>());
  // s is used as a placeholder.
  Struct s;
  const test::AdaptedWithContext<T, Struct, 0> adapted{value};
  using AdaptedTag = type::adapted<test::TemplatedTestFieldAdapter, Tag>;
  using FieldTag = type::field<AdaptedTag, FieldContext<Struct, 0>>;

  protocol_writer_t<Protocol> writer;
  folly::IOBufQueue queue;
  writer.setOutput(&queue);
  encode<AdaptedTag>(writer, adapted);

  protocol_reader_t<Protocol> reader;
  auto serialized = queue.move();
  reader.setInput(serialized.get());
  test::AdaptedWithContext<T, Struct, 0> result;
  decode<FieldTag>(reader, result, s);
  EXPECT_EQ(adapted, result);

  using AdaptedTag2 = type::adapted<test::EncodeFieldAdapter, Tag>;
  using FieldTag2 = type::field<AdaptedTag2, FieldContext<Struct, 0>>;
  encode<AdaptedTag2>(writer, adapted);

  auto serialized2 = queue.move();
  reader.setInput(serialized2.get());
  decode<FieldTag2>(reader, result, s);
  EXPECT_EQ(adapted, result);
}

template <conformance::StandardProtocol Protocol>
void testDecodeAdapted() {
  SCOPED_TRACE(apache::thrift::util::enumNameSafe(Protocol));
  using AdaptedTag = type::adapted<test::TemplatedTestAdapter, type::string_t>;
  testDecode<Protocol, AdaptedTag>(
      test::TemplatedTestAdapter::fromThrift(std::string()));
  using Struct = test::testset::struct_with<type::i32_t>;
  testDecodeObject<Protocol, Struct, type::struct_t<Struct>, true>(1);
  using Union = test::testset::union_with<type::i32_t>;
  testDecodeObject<Protocol, Union, type::union_t<Union>, true>(1);
  testDecode<Protocol, type::adapted<test::EncodeAdapter, type::i64_t>>(
      test::Num{1});
  using Struct64 = test::testset::struct_with<type::i64_t>;
  testEncodeAndDecodeFieldAdapted<Protocol, Struct64, type::i64_t>(
      static_cast<int64_t>(42));
}

TEST(DecodeTest, DecodeBasicTypes) {
  testDecodeBasicTypes<conformance::StandardProtocol::Binary>();
  testDecodeBasicTypes<conformance::StandardProtocol::Compact>();
}

TEST(DecodeTest, DecodeUnsignedTypes) {
  testDecodeUnsignedTypes<conformance::StandardProtocol::Binary>();
  testDecodeUnsignedTypes<conformance::StandardProtocol::Compact>();
}

TEST(DecodeTest, DecodeContainers) {
  testDecodeContainers<conformance::StandardProtocol::Binary>();
  testDecodeContainers<conformance::StandardProtocol::Compact>();
}

TEST(DecodeTest, SetReadParity) {
  using Tag = type::set<type::i32_t>;
  using Methods = apache::thrift::detail::pm::protocol_methods<
      type_class::set<type_class::integral>,
      std::set<int32_t>,
      Tag>;
  for (auto wireType : {TType::T_I32, TType::T_I64}) {
    folly::IOBufQueue queue;
    BinaryProtocolWriter writer;
    writer.setOutput(&queue);
    writer.writeSetBegin(wireType, 3);
    for (auto value : {2, 1, 2}) {
      if (wireType == TType::T_I32) {
        writer.writeI32(value);
      } else {
        writer.writeI64(value);
      }
    }
    writer.writeSetEnd();
    writer.writeI32(42);
    for (bool useOp : {false, true}) {
      SCOPED_TRACE(useOp);
      BinaryProtocolReader reader;
      reader.setInput(queue.front());
      std::set<int32_t> result{99};
      if (useOp) {
        op::decode<Tag>(reader, result);
      } else {
        Methods::read(reader, result);
      }
      EXPECT_EQ(
          result,
          wireType == TType::T_I32 ? (std::set<int32_t>{1, 2})
                                   : std::set<int32_t>{});
      int32_t next;
      reader.readI32(next);
      EXPECT_EQ(next, 42);
    }
  }
}

TEST(DecodeTest, SetTruncatedInput) {
  using Tag = type::set<type::i32_t>;
  using Methods = apache::thrift::detail::pm::protocol_methods<
      type_class::set<type_class::integral>,
      std::set<int32_t>,
      Tag>;
  folly::IOBufQueue queue;
  BinaryProtocolWriter writer;
  writer.setOutput(&queue);
  writer.writeSetBegin(TType::T_I32, 5);
  writer.writeI32(1);
  for (bool useOp : {false, true}) {
    BinaryProtocolReader reader;
    reader.setInput(queue.front());
    std::set<int32_t> result{99};
    if (useOp) {
      EXPECT_THROW(op::decode<Tag>(reader, result), TProtocolException);
    } else {
      EXPECT_THROW(Methods::read(reader, result), TProtocolException);
    }
    EXPECT_TRUE(result.empty());
  }
}

TEST(DecodeTest, SetUnknownSizeAllocator) {
  using Set = std::pmr::set<std::pmr::string>;
  using Tag = type::set<type::string_t>;
  using Methods = apache::thrift::detail::pm::
      protocol_methods<type_class::set<type_class::string>, Set, Tag>;
  struct Reader : SimpleJSONProtocolReader {
    std::pmr::memory_resource* resource = nullptr;
    void readString(std::pmr::string& value) {
      EXPECT_EQ(value.get_allocator().resource(), resource);
      SimpleJSONProtocolReader::readString(value);
    }
  };
  const auto input = folly::IOBuf::copyBuffer(R"(["one","two"])");
  for (bool useOp : {false, true}) {
    std::pmr::monotonic_buffer_resource resource;
    Set result(&resource);
    result.emplace("old");
    Reader reader;
    reader.resource = &resource;
    reader.setInput(input.get());
    if (useOp) {
      op::decode<Tag>(reader, result);
    } else {
      Methods::read(reader, result);
    }
    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(*result.begin(), "one");
    EXPECT_EQ(*std::next(result.begin()), "two");
    EXPECT_EQ(result.get_allocator().resource(), &resource);
  }
}

TEST(DecodeTest, DecodeListWithConstReference) {
  using Tag = type::list<type::i32_t>;
  const std::vector<std::int32_t> values{1, 2, 3};
  ConstReferenceList result;

  encodeAndDecodeInto<
      conformance::StandardProtocol::Compact,
      Tag,
      type::cpp_type<ConstReferenceList, Tag>>(values, result);

  EXPECT_EQ(result.values(), values);
}

TEST(DecodeTest, PreservesContainerState) {
  using Protocol = conformance::StandardProtocol;

  using ListAllocator = PropagatingAllocator<std::int32_t>;
  std::vector<std::int32_t, ListAllocator> list({0}, ListAllocator{42});
  encodeAndDecodeInto<
      Protocol::Compact,
      type::list<type::i32_t>,
      type::list<type::i32_t>>(std::vector<std::int32_t>{1, 2, 3}, list);
  EXPECT_EQ(list.get_allocator().state(), 42);
  EXPECT_EQ(
      std::vector<std::int32_t>(list.begin(), list.end()),
      (std::vector<std::int32_t>{1, 2, 3}));

  using SetAllocator = PropagatingAllocator<std::int32_t>;
  std::set<std::int32_t, std::less<std::int32_t>, SetAllocator> set(
      {0}, std::less<std::int32_t>{}, SetAllocator{42});
  encodeAndDecodeInto<
      Protocol::Compact,
      type::set<type::i32_t>,
      type::set<type::i32_t>>(std::set<std::int32_t>{1, 2, 3}, set);
  EXPECT_EQ(set.get_allocator().state(), 42);
  EXPECT_EQ(
      std::set<std::int32_t>(set.begin(), set.end()),
      (std::set<std::int32_t>{1, 2, 3}));

  using MapValue = std::pair<const std::int32_t, std::int32_t>;
  using MapAllocator = PropagatingAllocator<MapValue>;
  std::map<std::int32_t, std::int32_t, std::less<std::int32_t>, MapAllocator>
      map({{0, 0}}, std::less<std::int32_t>{}, MapAllocator{42});
  encodeAndDecodeInto<
      Protocol::Compact,
      type::map<type::i32_t, type::i32_t>,
      type::map<type::i32_t, type::i32_t>>(
      std::map<std::int32_t, std::int32_t>{{1, 10}, {2, 20}}, map);
  EXPECT_EQ(map.get_allocator().state(), 42);
  EXPECT_EQ(
      (std::map<std::int32_t, std::int32_t>(map.begin(), map.end())),
      (std::map<std::int32_t, std::int32_t>{{1, 10}, {2, 20}}));

  folly::sorted_vector_set<std::int32_t, StatefulCompare> sortedSet(
      StatefulCompare{.descending = true});
  encodeAndDecodeInto<
      Protocol::Compact,
      type::set<type::i32_t>,
      type::set<type::i32_t>>(
      std::set<std::int32_t, std::greater<std::int32_t>>{1, 2, 3}, sortedSet);
  EXPECT_TRUE(sortedSet.key_comp().descending);
  EXPECT_EQ(
      std::vector<std::int32_t>(sortedSet.begin(), sortedSet.end()),
      (std::vector<std::int32_t>{3, 2, 1}));

  folly::sorted_vector_map<std::int32_t, std::int32_t, StatefulCompare>
      sortedMap(StatefulCompare{.descending = true});
  encodeAndDecodeInto<
      Protocol::Compact,
      type::map<type::i32_t, type::i32_t>,
      type::map<type::i32_t, type::i32_t>>(
      std::map<std::int32_t, std::int32_t, std::greater<std::int32_t>>{
          {1, 10}, {2, 20}, {3, 30}},
      sortedMap);
  EXPECT_TRUE(sortedMap.key_comp().descending);
  EXPECT_EQ(
      (std::vector<std::pair<std::int32_t, std::int32_t>>(
          sortedMap.begin(), sortedMap.end())),
      (std::vector<std::pair<std::int32_t, std::int32_t>>{
          {3, 30}, {2, 20}, {1, 10}}));
}

TEST(DecodeTest, DecodeStruct) {
  testDecodeStruct<conformance::StandardProtocol::Binary>();
  testDecodeStruct<conformance::StandardProtocol::Compact>();
}

TEST(DecodeTest, DecodeCppType) {
  testDecodeCppType<conformance::StandardProtocol::Binary>();
  testDecodeCppType<conformance::StandardProtocol::Compact>();
}

TEST(DecodeTest, DecodeAdapted) {
  testDecodeAdapted<conformance::StandardProtocol::Binary>();
  testDecodeAdapted<conformance::StandardProtocol::Compact>();
}

template <typename Tag, typename T>
T binaryChainRoundTrip(const T& value) {
  BinaryProtocolWriter writer;
  folly::IOBufQueue queue;
  writer.setOutput(&queue);
  op::encode<Tag>(writer, value);

  IOBufChain serialized(queue.move());
  BinaryProtocolChainReader reader;
  reader.setInput(&serialized);
  T result;
  op::decode<Tag>(reader, result);
  return result;
}

template <typename Tag, typename T>
T binaryDecode(const folly::IOBuf& serialized, T value) {
  BinaryProtocolReader reader;
  reader.setInput(&serialized);
  op::decode<Tag>(reader, value);
  return value;
}

template <typename Tag, typename T>
T binaryChainDecode(const folly::IOBuf& serialized, T value) {
  IOBufChain input{serialized.clone()};
  BinaryProtocolChainReader reader;
  reader.setInput(&input);
  op::decode<Tag>(reader, value);
  return value;
}

TEST(DecodeTest, BinaryChainStruct) {
  using Struct =
      test::testset::struct_with<type::map<type::string_t, type::i32_t>>;
  Struct expected;
  expected.field_1_ref() = {{"one", 1}, {"two", 2}};

  EXPECT_EQ((binaryChainRoundTrip<type::struct_t<Struct>>(expected)), expected);
}

TEST(DecodeTest, BinaryChainUnion) {
  using Union = test::testset::union_with<type::set<type::string_t>>;
  Union expected;
  expected.field_1_ref() = {"one", "two"};

  EXPECT_EQ((binaryChainRoundTrip<type::union_t<Union>>(expected)), expected);
}

TEST(DecodeTest, BinaryChainUnionWithSharedConstRef) {
  using Union = test::testset::union_with<
      type::list<type::bool_t>,
      test::testset::FieldModifier::ConstSharedReference>;
  Union expected;
  expected.field_1_ref().emplace(std::vector<bool>{true, false});

  EXPECT_EQ((binaryChainRoundTrip<type::union_t<Union>>(expected)), expected);
}

TEST(DecodeTest, BinaryChainUnionResetsMatchedFieldBeforeDecode) {
  BinaryProtocolWriter writer;
  folly::IOBufQueue queue;
  writer.setOutput(&queue);
  writer.writeStructBegin("FooUnion");
  writer.writeFieldBegin("foo", protocol::T_STRUCT, 1);
  writer.writeStructBegin("Foo");
  writer.writeFieldBegin("field2", protocol::T_I32, 1);
  writer.writeI32(42);
  writer.writeFieldEnd();
  writer.writeFieldStop();
  writer.writeStructEnd();
  writer.writeFieldEnd();
  writer.writeFieldStop();
  writer.writeStructEnd();

  test::FooUnion initial;
  auto& foo = initial.foo_ref().ensure();
  foo.field1_ref() = 1;
  foo.field2_ref() = 2;
  foo.field3_ref() = 3;

  auto serialized = queue.move();
  auto legacy =
      binaryDecode<type::union_t<test::FooUnion>>(*serialized, initial);
  auto chain =
      binaryChainDecode<type::union_t<test::FooUnion>>(*serialized, initial);

  EXPECT_EQ(chain, legacy);
  ASSERT_TRUE(chain.foo_ref().has_value());
  EXPECT_EQ(chain.foo_ref()->field1_ref(), 0);
  EXPECT_EQ(chain.foo_ref()->field2_ref(), 42);
  EXPECT_EQ(chain.foo_ref()->field3_ref(), 0);
}

void expectBinaryChainUnionSkipPreservesValue(int16_t fieldId) {
  using Union = test::testset::union_i32;
  using Tag = type::union_t<Union>;

  BinaryProtocolWriter writer;
  folly::IOBufQueue queue;
  writer.setOutput(&queue);
  writer.writeStructBegin("union_i32");
  writer.writeFieldBegin("field", protocol::T_STRING, fieldId);
  writer.writeString("skipped");
  writer.writeFieldEnd();
  writer.writeFieldStop();
  writer.writeStructEnd();

  Union initial;
  initial.field_1_ref() = 42;

  auto serialized = queue.move();
  auto legacy = binaryDecode<Tag>(*serialized, initial);
  auto chain = binaryChainDecode<Tag>(*serialized, initial);

  EXPECT_EQ(chain, legacy);
  ASSERT_TRUE(op::get<type::field_id<1>>(chain).has_value());
  EXPECT_EQ(*op::get<type::field_id<1>>(chain), 42);
}

TEST(DecodeTest, BinaryChainUnionWrongWireTypePreservesValueLikeGenerated) {
  expectBinaryChainUnionSkipPreservesValue(1);
}

TEST(DecodeTest, BinaryChainUnionUnknownFieldPreservesValueLikeGenerated) {
  expectBinaryChainUnionSkipPreservesValue(99);
}

TEST(DecodeTest, BinaryChainException) {
  using Exception = test::testset::exception_with<type::i64_t>;
  Exception expected;
  expected.field_1_ref() = 42;

  EXPECT_EQ(
      (binaryChainRoundTrip<type::exception_t<Exception>>(expected)), expected);
}

TEST(DecodeTest, BinaryChainSkipsKnownFieldWithWrongWireType) {
  BinaryProtocolWriter writer;
  folly::IOBufQueue queue;
  writer.setOutput(&queue);
  writer.writeStructBegin("struct_i32");
  writer.writeFieldBegin("field_1", protocol::T_STRING, 1);
  writer.writeString("wrong type");
  writer.writeFieldEnd();
  writer.writeFieldBegin("field_1", protocol::T_I32, 1);
  writer.writeI32(42);
  writer.writeFieldEnd();
  writer.writeFieldStop();
  writer.writeStructEnd();

  IOBufChain serialized(queue.move());
  BinaryProtocolChainReader reader;
  reader.setInput(&serialized);
  test::testset::struct_i32 actual;
  op::decode<type::struct_t<test::testset::struct_i32>>(reader, actual);

  EXPECT_EQ(*op::get<type::field_id<1>>(actual), 42);
}

TEST(DecodeTest, BinaryChainRejectsUnionWithMultipleFields) {
  BinaryProtocolWriter writer;
  folly::IOBufQueue queue;
  writer.setOutput(&queue);
  writer.writeStructBegin("union_i32");
  writer.writeFieldBegin("field_1", protocol::T_I32, 1);
  writer.writeI32(1);
  writer.writeFieldEnd();
  writer.writeFieldBegin("field_2", protocol::T_I32, 2);
  writer.writeI32(2);
  writer.writeFieldEnd();
  writer.writeFieldStop();
  writer.writeStructEnd();

  IOBufChain serialized(queue.move());
  BinaryProtocolChainReader reader;
  reader.setInput(&serialized);
  test::testset::union_i32 actual;
  EXPECT_THROW(
      op::decode<type::union_t<test::testset::union_i32>>(reader, actual),
      protocol::TProtocolException);
}
} // namespace

enum { UseWrite, UseStructEncode };

struct MockStruct {
  static const bool __fbthrift_cpp2_gen_json = false;
  template <class T>
  uint32_t write(T&&) const {
    return UseWrite;
  }
};

struct JSONMockStruct {
  static const bool __fbthrift_cpp2_gen_json = true;
  template <class T>
  uint32_t write(T&&) const {
    return UseWrite;
  }
};

template <>
struct StructEncode<MockStruct> {
  template <typename T, typename U>
  uint32_t operator()(T&&, const U&) const {
    return UseStructEncode;
  }
};

template <>
struct StructEncode<JSONMockStruct> {
  template <typename T, typename U>
  uint32_t operator()(T&&, const U&) const {
    return UseStructEncode;
  }
};

TEST(EncodeTest, EncodeMethod) {
  CompactProtocolWriter compact;
  BinaryProtocolWriter binary;
  SimpleJSONProtocolWriter simpleJson;
  JSONProtocolWriter json;
  DebugProtocolWriter debug;
  MockStruct mock;
  JSONMockStruct jsonMock;
  auto encodeMockStruct = op::encode<type::struct_t<MockStruct>>;
  auto encodeJSONMockStruct = op::encode<type::struct_t<JSONMockStruct>>;

  EXPECT_EQ(encodeMockStruct(compact, mock), UseWrite);
  EXPECT_EQ(encodeMockStruct(binary, mock), UseWrite);
  EXPECT_EQ(encodeMockStruct(simpleJson, mock), UseStructEncode);
  EXPECT_EQ(encodeMockStruct(json, mock), UseStructEncode);
  EXPECT_EQ(encodeMockStruct(debug, mock), UseStructEncode);
  EXPECT_EQ(encodeJSONMockStruct(compact, jsonMock), UseWrite);
  EXPECT_EQ(encodeJSONMockStruct(binary, jsonMock), UseWrite);
  EXPECT_EQ(encodeJSONMockStruct(simpleJson, jsonMock), UseWrite);
  EXPECT_EQ(encodeJSONMockStruct(json, jsonMock), UseStructEncode);
  EXPECT_EQ(encodeJSONMockStruct(debug, jsonMock), UseStructEncode);
}

TEST(EncodeTest, DeprecatedTerseWrite) {
  auto expectGenericEncodeMatchesGenerated = []<typename T>(const T& value) {
    SCOPED_TRACE(folly::pretty_name<T>());
    struct GenericCompactProtocolWriter : CompactProtocolWriter {};

    folly::IOBufQueue generatedOutput;
    CompactProtocolWriter generatedWriter;
    generatedWriter.setOutput(&generatedOutput);
    value.write(&generatedWriter);

    folly::IOBufQueue genericOutput;
    GenericCompactProtocolWriter genericWriter;
    genericWriter.setOutput(&genericOutput);
    op::encode<type::struct_t<T>>(genericWriter, value);

    auto generated = generatedOutput.move();
    auto generic = genericOutput.move();
    EXPECT_TRUE(folly::IOBufEqualTo{}(*generated, *generic));
  };

  test::EmptiableTerseFieldsStruct intrinsicDefaults;
  intrinsicDefaults.int_field() = 42;
  expectGenericEncodeMatchesGenerated(intrinsicDefaults);

  test::TerseFieldsWithCustomDefault customDefaults;
  customDefaults.int_field() = 0;
  customDefaults.cpp_shared_ref_exception_field() =
      std::make_shared<test::NestedException>();
  expectGenericEncodeMatchesGenerated(customDefaults);

  test::RefsWithStringAndContainerTerseWrites emptyRefs;
  emptyRefs.string_list_field() = std::make_shared<std::vector<std::string>>();
  emptyRefs.string_field() = std::make_shared<std::string>();
  expectGenericEncodeMatchesGenerated(emptyRefs);
}

TEST(EncodeTest, keyOrder) {
  struct SortingProtocolWriter : CompactProtocolWriter {
    static constexpr KeyOrder keyOrder() { return KeyOrder::NativeAscending; }
  };

  struct OrderPreservingSet : std::vector<int> {
    void emplace(int i) { push_back(i); }
    void insert(int i) { push_back(i); }
  };
  OrderPreservingSet reversedSet{{3, 2, 1}};
  OrderPreservingSet readSet;

  {
    folly::IOBufQueue queue;
    SortingProtocolWriter writer;
    writer.setOutput(&queue);
    op::encode<type::set<type::i32_t>>(writer, reversedSet);
    CompactProtocolReader reader;
    reader.setInput(queue.front());
    op::decode<type::set<type::i32_t>>(reader, readSet);
  }
  EXPECT_EQ(readSet, (OrderPreservingSet{{1, 2, 3}}));

  std::unordered_set<int> unorderedSet{3, 2, 1};
  {
    folly::IOBufQueue queue;
    SortingProtocolWriter writer;
    writer.setOutput(&queue);
    op::encode<type::set<type::i32_t>>(writer, unorderedSet);
    CompactProtocolReader reader;
    reader.setInput(queue.front());
    op::decode<type::set<type::i32_t>>(reader, readSet);
  }
  EXPECT_EQ(readSet, (OrderPreservingSet{{1, 2, 3}}));

  std::set<int, std::greater<>> orderedSet{3, 2, 1};
  {
    folly::IOBufQueue queue;
    SortingProtocolWriter writer;
    writer.setOutput(&queue);
    op::encode<type::set<type::i32_t>>(writer, orderedSet);
    CompactProtocolReader reader;
    reader.setInput(queue.front());
    op::decode<type::set<type::i32_t>>(reader, readSet);
  }
  // Does not re-sort if already sorted.
  EXPECT_EQ(readSet, (OrderPreservingSet{{3, 2, 1}}));

  struct OrderPreservingMap : std::vector<std::pair<int, int>> {
    using key_type = int;
    using mapped_type = int;
    void emplace(std::pair<int, int> p) { push_back(p); }
    int& operator[](int i) {
      push_back({i, 0});
      return back().second;
    }
  };
  OrderPreservingMap reversedMap{{{3, 0}, {2, 0}, {1, 0}}};
  OrderPreservingMap readMap;
  {
    folly::IOBufQueue queue;
    SortingProtocolWriter writer;
    writer.setOutput(&queue);
    op::encode<type::map<type::i32_t, type::i32_t>>(writer, reversedMap);
    CompactProtocolReader reader;
    reader.setInput(queue.front());
    op::decode<type::map<type::i32_t, type::i32_t>>(reader, readMap);
  }
  EXPECT_EQ(readMap, (OrderPreservingMap{{{1, 0}, {2, 0}, {3, 0}}}));
}

TEST(EncodeTest, SerializeInFieldIdOrder) {
  static_assert(::apache::thrift::detail::st::private_access::
                    has_serialize_in_field_id_order<test::Foo>);
  test::Foo foo;
  folly::IOBufQueue queue;
  CompactProtocolWriter writer;
  writer.setOutput(&queue);
  StructEncode<test::Foo>{}(writer, foo);
  auto serialized = queue.move();
  CompactProtocolReader reader;
  reader.setInput(serialized.get());
  std::string ignore;
  int16_t fieldId;
  TType fieldType;
  reader.readStructBegin(ignore);
  reader.readFieldBegin(ignore, fieldType, fieldId);
  EXPECT_EQ(fieldId, 1);
}
} // namespace apache::thrift::op::detail
