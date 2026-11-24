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

#include <gtest/gtest.h>

#include <array>
#include <barrier>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <thread>
#include <vector>

#include <folly/synchronization/CallOnce.h>
#include <thrift/lib/cpp2/gen/module_types_cpp.h>
#include <thrift/lib/cpp2/gen/tcc_struct_traits.h>
#include <thrift/lib/cpp2/protocol/BinaryProtocol.h>
#include <thrift/lib/cpp2/protocol/CompactProtocol.h>
#include <thrift/lib/cpp2/protocol/ProtocolReaderStructReadState.h>
#include <thrift/lib/cpp2/protocol/SimpleJSONProtocol.h>
#include <thrift/lib/cpp2/protocol/detail/protocol_methods.h>
#include <thrift/lib/cpp2/protocol/test/gen-cpp2/cursor_types.h>

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::detail::pm;
using namespace apache::thrift::test;

namespace {

constexpr int16_t kNoFieldId = std::numeric_limits<int16_t>::min();

struct FieldLookupResult {
  int16_t id;
  TType type;
  bool operator==(const FieldLookupResult&) const = default;
};

template <typename Struct>
FieldLookupResult lookupField(std::string_view name, int16_t id = kNoFieldId) {
  auto type = T_VOID;
  apache::thrift::detail::TccStructTraits<Struct>::translateFieldName(
      name, id, type);
  return {id, type};
}

template <typename Struct>
void testFieldNameTranslation() {
  EXPECT_EQ(lookupField<Struct>("first"), (FieldLookupResult{1, T_I64}));
  EXPECT_EQ(lookupField<Struct>("last"), (FieldLookupResult{32767, T_BOOL}));
  EXPECT_EQ(lookupField<Struct>("second", 2), (FieldLookupResult{2, T_STRING}));
  EXPECT_EQ(lookupField<Struct>({}, 1), (FieldLookupResult{1, T_I64}));
  EXPECT_EQ(lookupField<Struct>({}, 32767), (FieldLookupResult{32767, T_BOOL}));
  EXPECT_EQ(
      lookupField<Struct>("missing"), (FieldLookupResult{kNoFieldId, T_VOID}));
  EXPECT_EQ(lookupField<Struct>("missing", 7), (FieldLookupResult{7, T_VOID}));
  EXPECT_EQ(lookupField<Struct>({}, 7), (FieldLookupResult{7, T_VOID}));
  EXPECT_THROW(lookupField<Struct>("first", 2), TProtocolException);
  EXPECT_THROW(lookupField<Struct>("missing", 2), TProtocolException);
  int16_t id = 7;
  auto type = T_I16;
  using Traits = apache::thrift::detail::TccStructTraits<Struct>;
  Traits::translateFieldName("missing", id, type);
  EXPECT_EQ(id, 7);
  EXPECT_EQ(type, T_I16);
  id = 2;
  EXPECT_THROW(
      Traits::translateFieldName("first", id, type), TProtocolException);
  EXPECT_EQ(id, 2);
  EXPECT_EQ(type, T_I16);
  EXPECT_EQ(lookupField<Struct>({}, 2), (FieldLookupResult{2, T_STRING}));
  EXPECT_EQ(
      lookupField<Struct>(std::string_view("last_suffix", 4)),
      (FieldLookupResult{32767, T_BOOL}));
}

TEST(ProtocolReaderStructReadState, fieldNameLinearLookup) {
  testFieldNameTranslation<FieldLookup12>();
}

TEST(ProtocolReaderStructReadState, fieldNameCachedLookup) {
  testFieldNameTranslation<FieldLookup13>();
  testFieldNameTranslation<FieldLookup13>();
}

TEST(ProtocolReaderStructReadState, fieldNameConcurrentInitialization) {
  constexpr size_t kThreads = 8;
  std::barrier start(static_cast<std::ptrdiff_t>(kThreads));
  std::array<FieldLookupResult, kThreads> results{};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (size_t i = 0; i < kThreads; ++i) {
    threads.emplace_back([&, i] {
      start.arrive_and_wait();
      results[i] = i % 2 ? lookupField<FieldLookupConcurrent>({}, 1)
                         : lookupField<FieldLookupConcurrent>("last");
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  for (size_t i = 0; i < kThreads; ++i) {
    const auto expected =
        i % 2 ? FieldLookupResult{1, T_I64} : FieldLookupResult{32767, T_BOOL};
    EXPECT_EQ(results[i], expected);
  }
}

TEST(ProtocolReaderStructReadState, fieldNameInitializationFailureRetry) {
  constexpr std::array<std::string_view, 2> names{
      "first",
      "last",
  };
  constexpr std::array<int16_t, 2> ids{
      -9,
      32767,
  };
  constexpr std::array<TType, 2> types{
      T_I64,
      T_BOOL,
  };
  auto metadata = apache::thrift::detail::st::translate_field_name_table{
      names.size(), names.data(), ids.data(), types.data()};
  apache::thrift::detail::st::translate_field_name_cache cache{metadata};
  using Table = apache::thrift::detail::st::translate_field_name_hash_table;
  decltype(Table::map) emptyMap;
  metadata.size = emptyMap.max_size() + 1;
  int16_t id = kNoFieldId;
  auto type = T_VOID;
  EXPECT_THROW(
      apache::thrift::detail::st::translate_field_name_or_id(
          "first", id, type, cache),
      std::bad_alloc);
  EXPECT_FALSE(folly::test_once(cache.once));
  metadata.size = names.size();
  apache::thrift::detail::st::translate_field_name_or_id(
      "last", id, type, cache);
  EXPECT_TRUE(folly::test_once(cache.once));
  EXPECT_EQ(id, 32767);
  EXPECT_EQ(type, T_BOOL);
  id = -9;
  apache::thrift::detail::st::translate_field_name_or_id({}, id, type, cache);
  EXPECT_EQ(id, -9);
  EXPECT_EQ(type, T_I64);
  // Unlike the process-lifetime caches, this test's storage is local.
  std::destroy_at(std::launder(reinterpret_cast<Table*>(&cache.storage)));
}

template <class ProtocolWriter>
std::unique_ptr<folly::IOBuf> createTestInput() {
  folly::IOBufQueue queue;
  ProtocolWriter writer;
  writer.setOutput(&queue);
  writer.writeStructBegin("");
  writer.writeFieldBegin("", T_BYTE, 13);
  writer.writeByte(24);
  writer.writeFieldEnd();
  writer.writeFieldBegin("", T_BYTE, 23);
  writer.writeByte(123);
  writer.writeFieldEnd();
  // In compact, this would be encoded as a delta in CompactProtocol
  writer.writeFieldBegin("", T_I64, 38);
  writer.writeI64(123456789);
  writer.writeFieldEnd();
  // In compact, this would be encoded as a delta + bool in one byte.
  writer.writeFieldBegin("", T_BOOL, 39);
  writer.writeBool(true);
  writer.writeFieldEnd();
  // In compact, this would be encoded as a type header + 1 byte for field.
  writer.writeFieldBegin("", T_BOOL, 63);
  writer.writeBool(false);
  writer.writeFieldEnd();
  // In compact, this would be encoded as a type header + 2 byte for field.
  writer.writeFieldBegin("", T_I32, -8192);
  writer.writeI32(5678910);
  writer.writeFieldEnd();
  // In compact, this would be encoded as a type header + 3 byte for field.
  writer.writeFieldBegin("", T_I16, 16381);
  writer.writeI16(12345);
  writer.writeFieldEnd();

  writer.writeFieldStop();
  writer.writeStructEnd();
  return queue.move();
}

template <typename Protocol, typename T>
void readFieldContents(Protocol& protocol, T& out) {
  protocol_methods<type_class::integral, T, type::infer_tag<T>>::read(
      protocol, out);
}

template <class ProtocolReader>
void testAdvanceToNextFieldSuccess() {
  auto input = createTestInput<typename ProtocolReader::ProtocolWriter>();

  typename ProtocolReader::StructReadState state;
  ProtocolReader reader;
  reader.setInput(input.get());
  state.readStructBegin(&reader);
  EXPECT_TRUE(state.advanceToNextField(&reader, 0, 13, T_BYTE));
  {
    int8_t value;
    readFieldContents(reader, value);
    EXPECT_EQ(value, 24);
  }
  EXPECT_TRUE(state.advanceToNextField(&reader, 13, 23, T_BYTE));
  {
    int8_t value;
    readFieldContents(reader, value);
    EXPECT_EQ(value, 123);
  }
  EXPECT_TRUE(state.advanceToNextField(&reader, 23, 38, T_I64));
  {
    int64_t value;
    readFieldContents(reader, value);
    EXPECT_EQ(value, 123456789);
  }
  EXPECT_TRUE(state.advanceToNextField(&reader, 38, 39, T_BOOL));
  {
    bool value;
    readFieldContents(reader, value);
    EXPECT_EQ(value, true);
  }
  EXPECT_TRUE(state.advanceToNextField(&reader, 39, 63, T_BOOL));
  {
    bool value;
    readFieldContents(reader, value);
    EXPECT_EQ(value, false);
  }
  EXPECT_TRUE(state.advanceToNextField(&reader, 63, -8192, T_I32));
  {
    int32_t value;
    readFieldContents(reader, value);
    EXPECT_EQ(value, 5678910);
  }
  EXPECT_TRUE(state.advanceToNextField(&reader, -8192, 16381, T_I16));
  {
    int16_t value;
    readFieldContents(reader, value);
    EXPECT_EQ(value, 12345);
  }
  EXPECT_TRUE(state.advanceToNextField(&reader, 16381, 0, T_STOP));
  state.readStructEnd(&reader);
  EXPECT_TRUE(reader.getCursor().isAtEnd());
}

template <class ProtocolReader>
void testAdvanceToNextFieldFail() {
  auto input = createTestInput<typename ProtocolReader::ProtocolWriter>();

  typename ProtocolReader::StructReadState state;
  ProtocolReader reader;
  reader.setInput(input.get());
  state.readStructBegin(&reader);
  // Test missmatching type.
  EXPECT_FALSE(state.advanceToNextField(&reader, 0, 13, T_I32));
  // We failed to transition, state should reflect what was in the buffer.
  EXPECT_EQ(state.fieldId, 13);
  EXPECT_EQ(state.fieldType, T_BYTE);
  reader.skip(T_BYTE);
  // We should still be able to resume.
  EXPECT_TRUE(state.advanceToNextField(&reader, 13, 23, T_BYTE));
  {
    int8_t value;
    readFieldContents(reader, value);
    EXPECT_EQ(value, 123);
  }
  // Test mismatched field id.
  EXPECT_FALSE(state.advanceToNextField(&reader, 23, 37, T_I64));
  EXPECT_EQ(state.fieldId, 38);
  EXPECT_EQ(state.fieldType, T_I64);
  reader.skip(T_I64);

  EXPECT_FALSE(state.advanceToNextField(&reader, 38, 40, T_I32));
  EXPECT_EQ(state.fieldId, 39);
  EXPECT_EQ(state.fieldType, T_BOOL);
  reader.skip(T_BOOL);

  EXPECT_FALSE(state.advanceToNextField(&reader, 40, 65, T_BOOL));
  EXPECT_EQ(state.fieldId, 63);
  EXPECT_EQ(state.fieldType, T_BOOL);
  reader.skip(T_BOOL);

  // Fail to find T_STOP.
  EXPECT_FALSE(state.advanceToNextField(&reader, 63, 0, T_STOP));
  EXPECT_EQ(state.fieldId, -8192);
  EXPECT_EQ(state.fieldType, T_I32);
  reader.skip(T_I32);

  EXPECT_FALSE(state.advanceToNextField(&reader, -8192, 16380, T_STRING));
  EXPECT_EQ(state.fieldId, 16381);
  EXPECT_EQ(state.fieldType, T_I16);
  reader.skip(T_I16);

  // No such field in the buffer.
  EXPECT_FALSE(state.advanceToNextField(&reader, 16381, 123, T_STRING));
  EXPECT_EQ(state.fieldType, T_STOP);
  state.readStructEnd(&reader);
  EXPECT_TRUE(reader.getCursor().isAtEnd());
}

template <class ProtocolReader>
void testStructDepth() {
  typename ProtocolReader::StructReadState state{};
  ProtocolReader reader;
  reader.setHeight(1);
  state.readStructBegin(&reader);
  EXPECT_EQ(reader.getHeight(), 0);
  state.readStructEnd(&reader);
  EXPECT_EQ(reader.getHeight(), 1);

  reader.setHeight(0);
  EXPECT_THROW(state.readStructBegin(&reader), TProtocolException);
}

template <class ProtocolWriter>
std::unique_ptr<folly::IOBuf> makeRecursiveStruct(size_t depth) {
  folly::IOBufQueue queue;
  ProtocolWriter writer;
  writer.setOutput(&queue);
  auto write = [&](auto& self, size_t remaining) -> void {
    writer.writeStructBegin("");
    if (remaining > 1) {
      writer.writeFieldBegin("", T_STRUCT, 1);
      self(self, remaining - 1);
      writer.writeFieldEnd();
    }
    writer.writeFieldStop();
    writer.writeStructEnd();
  };
  write(write, depth);
  return queue.move();
}

template <class ProtocolReader>
void testGeneratedReadDepthLimit() {
  constexpr size_t kDepthLimit = 4;
  for (const auto depth : {kDepthLimit, kDepthLimit + 1}) {
    auto input =
        makeRecursiveStruct<typename ProtocolReader::ProtocolWriter>(depth);
    ProtocolReader reader;
    reader.setHeight(kDepthLimit);
    reader.setInput(input.get());
    Recursive value;
    if (depth == kDepthLimit) {
      EXPECT_NO_THROW(value.read(&reader));
    } else {
      EXPECT_THROW(value.read(&reader), TProtocolException);
    }
  }
}

TEST(BinaryProtocol, advanceToNextFieldSuccess) {
  testAdvanceToNextFieldSuccess<BinaryProtocolReader>();
}

TEST(CompactProtocol, advanceToNextFieldSuccess) {
  testAdvanceToNextFieldSuccess<CompactProtocolReader>();
}

TEST(BinaryProtocol, advanceToNextFieldFail) {
  testAdvanceToNextFieldSuccess<BinaryProtocolReader>();
}

TEST(CompactProtocol, advanceToNextFieldFail) {
  testAdvanceToNextFieldSuccess<CompactProtocolReader>();
}

TEST(BinaryProtocol, structReadStateTracksDepth) {
  testStructDepth<BinaryProtocolReader>();
}

TEST(CompactProtocol, structReadStateTracksDepth) {
  testStructDepth<CompactProtocolReader>();
}

TEST(ProtocolReaderStructReadState, generatedReadEnforcesDepthLimit) {
  testGeneratedReadDepthLimit<BinaryProtocolReader>();
  testGeneratedReadDepthLimit<CompactProtocolReader>();
}

// SimpleJSON uses field names and cannot optimize field skipping,
// so advanceToNextField always returns false. We test basic functionality.
TEST(SimpleJSONProtocol, basicStructReadState) {
  folly::IOBufQueue queue;
  SimpleJSONProtocolWriter writer;
  writer.setOutput(&queue);
  writer.writeStructBegin("");
  writer.writeFieldBegin("field1", T_BYTE, 13);
  writer.writeByte(24);
  writer.writeFieldEnd();
  writer.writeFieldBegin("field2", T_I32, 23);
  writer.writeI32(123456);
  writer.writeFieldEnd();
  writer.writeFieldStop();
  writer.writeStructEnd();
  auto input = queue.move();

  SimpleJSONProtocolReader::StructReadState state;
  SimpleJSONProtocolReader reader;
  reader.setInput(input.get());

  state.readStructBegin(&reader);
  state.readFieldBegin(&reader);
  EXPECT_EQ(state.fieldName(), "field1");
  EXPECT_EQ(state.fieldType, T_VOID); // SimpleJSON infers type from value
  {
    int8_t value;
    readFieldContents(reader, value);
    EXPECT_EQ(value, 24);
  }
  state.readFieldEnd(&reader);

  state.readFieldBegin(&reader);
  EXPECT_EQ(state.fieldName(), "field2");
  EXPECT_EQ(state.fieldType, T_VOID);
  {
    int32_t value;
    readFieldContents(reader, value);
    EXPECT_EQ(value, 123456);
  }
  state.readFieldEnd(&reader);

  state.readFieldBegin(&reader);
  EXPECT_TRUE(state.atStop());
  state.readStructEnd(&reader);
}

} // anonymous namespace
