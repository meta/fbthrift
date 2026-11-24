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

#include <memory_resource>
#include <string>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <folly/Utility.h>
#include <folly/io/IOBuf.h>
#include <folly/io/IOBufQueue.h>
#include <thrift/lib/cpp2/protocol/BinaryProtocol.h>
#include <thrift/lib/cpp2/protocol/CompactProtocol.h>
#include <thrift/lib/cpp2/protocol/DebugProtocol.h>
#include <thrift/lib/cpp2/protocol/JSONProtocol.h>
#include <thrift/lib/cpp2/protocol/SimpleJSONProtocol.h>
#include <thrift/test/gen-cpp2/Cpp2OpsNative_constants.h>
#include <thrift/test/gen-cpp2/Cpp2OpsNative_types.h>

namespace thrift::test::cpp2ops {

template <class T>
class Cpp2OpsTest : public testing::Test {};

TYPED_TEST_CASE_P(Cpp2OpsTest);

TYPED_TEST_P(Cpp2OpsTest, Simple) {
  std::unique_ptr<folly::IOBuf> buf;
  uint32_t size;
  {
    apache::thrift::CompactProtocolWriter writer;

    TypeParam obj;
    *obj.num_ref() = cpp2::Cpp2OpsNative_constants::num_test_value();
    *obj.str_ref() = cpp2::Cpp2OpsNative_constants::str_test_value();

    // This is mainly to test that it compiles...
    EXPECT_NE(
        0, apache::thrift::Cpp2Ops<TypeParam>::serializedSize(&writer, &obj));
    EXPECT_NE(
        0, apache::thrift::Cpp2Ops<TypeParam>::serializedSizeZC(&writer, &obj));

    folly::IOBufQueue queue;
    writer.setOutput(&queue);

    size = apache::thrift::Cpp2Ops<TypeParam>::write(&writer, &obj);
    EXPECT_NE(0, size);
    buf = queue.move();
  }

  {
    apache::thrift::CompactProtocolReader reader;
    reader.setInput(buf.get());

    TypeParam obj;
    apache::thrift::Cpp2Ops<TypeParam>::read(&reader, &obj);
    EXPECT_EQ(size, reader.getCursorPosition());

    EXPECT_EQ(cpp2::Cpp2OpsNative_constants::num_test_value(), *obj.num_ref());
    EXPECT_EQ(cpp2::Cpp2OpsNative_constants::str_test_value(), *obj.str_ref());
  }
}

REGISTER_TYPED_TEST_CASE_P(Cpp2OpsTest, Simple);

using Types = testing::Types<cpp2::Native>;
INSTANTIATE_TYPED_TEST_CASE_P(Cpp2OpsTest, Cpp2OpsTest, Types);

using apache::thrift::protocol::TType;

enum class WideEnum : int64_t { Small = 3, Wide = (int64_t{1} << 40) + 7 };

template <class T>
const T& comparable(const T& value) {
  return value;
}
std::string comparable(const folly::IOBuf& value) {
  return value.to<std::string>();
}
std::string comparable(const std::unique_ptr<folly::IOBuf>& value) {
  return value->to<std::string>();
}

template <class Writer, class Write>
std::pair<std::string, uint32_t> serializeWith(Write write) {
  Writer writer;
  folly::IOBufQueue queue;
  writer.setOutput(&queue);
  uint32_t xfer = write(writer);
  return {queue.move()->template to<std::string>(), xfer};
}

// Checks that Cpp2Ops<T> makes the same protocol calls as `write`, `size`,
// `sizeZC` and `read`, which spell out its established wire behavior.
template <
    class Writer,
    class T,
    class Write,
    class Size,
    class SizeZC,
    class Read>
void expectProtocolCalls(
    const T& value,
    TType type,
    Write write,
    Size size,
    SizeZC sizeZC,
    Read read) {
  using Ops = apache::thrift::Cpp2Ops<T>;
  EXPECT_EQ(type, Ops::thriftType());

  Writer sizer;
  EXPECT_EQ(size(sizer, value), Ops::serializedSize(&sizer, &value));
  EXPECT_EQ(sizeZC(sizer, value), Ops::serializedSizeZC(&sizer, &value));

  const auto expected =
      serializeWith<Writer>([&](Writer& w) { return write(w, value); });
  EXPECT_EQ(expected, serializeWith<Writer>([&](Writer& w) {
              return Ops::write(&w, &value);
            }));

  folly::IOBuf buf(
      folly::IOBuf::WRAP_BUFFER, folly::StringPiece(expected.first));
  typename Writer::ProtocolReader expectedReader;
  expectedReader.setInput(&buf);
  T expectedValue{};
  read(expectedReader, expectedValue);
  typename Writer::ProtocolReader reader;
  reader.setInput(&buf);
  T actualValue{};
  Ops::read(&reader, &actualValue);
  EXPECT_EQ(comparable(expectedValue), comparable(actualValue));
  EXPECT_EQ(expectedReader.getCursorPosition(), reader.getCursorPosition());
}

template <class Writer, class T, class Write, class Size, class Read>
void expectProtocolCalls(
    const T& value, TType type, Write write, Size size, Read read) {
  expectProtocolCalls<Writer>(value, type, write, size, size, read);
}

template <class Writer>
class Cpp2OpsScalarTest : public testing::Test {};

using Writers = testing::Types<
    apache::thrift::BinaryProtocolWriter,
    apache::thrift::CompactProtocolWriter,
    apache::thrift::SimpleJSONProtocolWriter,
    apache::thrift::JSONProtocolWriter>;
TYPED_TEST_SUITE(Cpp2OpsScalarTest, Writers);

TYPED_TEST(Cpp2OpsScalarTest, Bool) {
  expectProtocolCalls<TypeParam>(
      true,
      TType::T_BOOL,
      [](auto& p, bool v) { return p.writeBool(v); },
      [](auto& p, bool v) { return p.serializedSizeBool(v); },
      [](auto& p, bool& v) { p.readBool(v); });
}

TYPED_TEST(Cpp2OpsScalarTest, SignedIntegers) {
  expectProtocolCalls<TypeParam>(
      int8_t{-7},
      TType::T_BYTE,
      [](auto& p, int8_t v) { return p.writeByte(v); },
      [](auto& p, int8_t v) { return p.serializedSizeByte(v); },
      [](auto& p, int8_t& v) { p.readByte(v); });
  expectProtocolCalls<TypeParam>(
      int16_t{-300},
      TType::T_I16,
      [](auto& p, int16_t v) { return p.writeI16(v); },
      [](auto& p, int16_t v) { return p.serializedSizeI16(v); },
      [](auto& p, int16_t& v) { p.readI16(v); });
  expectProtocolCalls<TypeParam>(
      int32_t{-70000},
      TType::T_I32,
      [](auto& p, int32_t v) { return p.writeI32(v); },
      [](auto& p, int32_t v) { return p.serializedSizeI32(v); },
      [](auto& p, int32_t& v) { p.readI32(v); });
  expectProtocolCalls<TypeParam>(
      int64_t{-5000000000},
      TType::T_I64,
      [](auto& p, int64_t v) { return p.writeI64(v); },
      [](auto& p, int64_t v) { return p.serializedSizeI64(v); },
      [](auto& p, int64_t& v) { p.readI64(v); });
}

TYPED_TEST(Cpp2OpsScalarTest, UnsignedIntegers) {
  expectProtocolCalls<TypeParam>(
      uint8_t{200},
      TType::T_BYTE,
      [](auto& p, uint8_t v) { return p.writeByte(folly::to_signed(v)); },
      [](auto& p, uint8_t v) {
        return p.serializedSizeByte(folly::to_signed(v));
      },
      [](auto& p, uint8_t& v) {
        int8_t s;
        p.readByte(s);
        v = folly::to_unsigned(s);
      });
  expectProtocolCalls<TypeParam>(
      uint16_t{60000},
      TType::T_I16,
      [](auto& p, uint16_t v) { return p.writeI16(folly::to_signed(v)); },
      [](auto& p, uint16_t v) {
        return p.serializedSizeI16(folly::to_signed(v));
      },
      [](auto& p, uint16_t& v) {
        int16_t s;
        p.readI16(s);
        v = folly::to_unsigned(s);
      });
  expectProtocolCalls<TypeParam>(
      uint32_t{4000000000},
      TType::T_I32,
      [](auto& p, uint32_t v) { return p.writeI32(folly::to_signed(v)); },
      [](auto& p, uint32_t v) {
        return p.serializedSizeI32(folly::to_signed(v));
      },
      [](auto& p, uint32_t& v) {
        int32_t s;
        p.readI32(s);
        v = folly::to_unsigned(s);
      });
  expectProtocolCalls<TypeParam>(
      uint64_t{18000000000000000000u},
      TType::T_I64,
      [](auto& p, uint64_t v) { return p.writeI64(folly::to_signed(v)); },
      [](auto& p, uint64_t v) {
        return p.serializedSizeI64(folly::to_signed(v));
      },
      [](auto& p, uint64_t& v) {
        int64_t s;
        p.readI64(s);
        v = folly::to_unsigned(s);
      });
}

TYPED_TEST(Cpp2OpsScalarTest, FloatingPoint) {
  expectProtocolCalls<TypeParam>(
      1.5f,
      TType::T_FLOAT,
      [](auto& p, float v) { return p.writeFloat(v); },
      [](auto& p, float v) { return p.serializedSizeFloat(v); },
      [](auto& p, float& v) { p.readFloat(v); });
  expectProtocolCalls<TypeParam>(
      -2.25,
      TType::T_DOUBLE,
      [](auto& p, double v) { return p.writeDouble(v); },
      [](auto& p, double v) { return p.serializedSizeDouble(v); },
      [](auto& p, double& v) { p.readDouble(v); });
}

// Enums are i32 on the wire whatever their underlying type.
TYPED_TEST(Cpp2OpsScalarTest, Enum) {
  for (auto value : {WideEnum::Small, WideEnum::Wide}) {
    expectProtocolCalls<TypeParam>(
        value,
        TType::T_I32,
        [](auto& p, WideEnum v) { return p.writeI32(static_cast<int32_t>(v)); },
        [](auto& p, WideEnum v) {
          return p.serializedSizeI32(static_cast<int32_t>(v));
        },
        [](auto& p, WideEnum& v) {
          int32_t i;
          p.readI32(i);
          v = static_cast<WideEnum>(i);
        });
  }
}

// Strings use the string, not binary, protocol methods, which differ in JSON.
TYPED_TEST(Cpp2OpsScalarTest, String) {
  expectProtocolCalls<TypeParam>(
      std::string("h\xc3\xa9llo"),
      TType::T_STRING,
      [](auto& p, const std::string& v) { return p.writeString(v); },
      [](auto& p, const std::string& v) { return p.serializedSizeString(v); },
      [](auto& p, std::string& v) { p.readString(v); });
  expectProtocolCalls<TypeParam>(
      folly::fbstring("h\xc3\xa9llo"),
      TType::T_STRING,
      [](auto& p, const folly::fbstring& v) { return p.writeString(v); },
      [](auto& p, const folly::fbstring& v) {
        return p.serializedSizeString(v);
      },
      [](auto& p, folly::fbstring& v) { p.readString(v); });
}

TYPED_TEST(Cpp2OpsScalarTest, Binary) {
  const auto buf = folly::IOBuf::copyBuffer(
      "\xff\x01"
      "binary");
  expectProtocolCalls<TypeParam>(
      *buf,
      TType::T_STRING,
      [](auto& p, const folly::IOBuf& v) { return p.writeBinary(v); },
      [](auto& p, const folly::IOBuf& v) { return p.serializedSizeBinary(v); },
      [](auto& p, const folly::IOBuf& v) {
        return p.serializedSizeZCBinary(v);
      },
      [](auto& p, folly::IOBuf& v) { p.readBinary(v); });
  expectProtocolCalls<TypeParam>(
      buf->clone(),
      TType::T_STRING,
      [](auto& p, const std::unique_ptr<folly::IOBuf>& v) {
        return p.writeBinary(v);
      },
      [](auto& p, const std::unique_ptr<folly::IOBuf>& v) {
        return p.serializedSizeBinary(v);
      },
      [](auto& p, const std::unique_ptr<folly::IOBuf>& v) {
        return p.serializedSizeZCBinary(v);
      },
      [](auto& p, std::unique_ptr<folly::IOBuf>& v) { p.readBinary(v); });
}

template <class T>
std::string writeCompact(const T& value) {
  apache::thrift::CompactProtocolWriter writer;
  folly::IOBufQueue queue;
  writer.setOutput(&queue);
  apache::thrift::Cpp2Ops<T>::write(&writer, &value);
  return queue.move()->toString();
}

template <class T>
uint32_t sizeCompact(const T& value) {
  apache::thrift::CompactProtocolWriter writer;
  const uint32_t size =
      apache::thrift::Cpp2Ops<T>::serializedSize(&writer, &value);
  EXPECT_EQ(
      size, apache::thrift::Cpp2Ops<T>::serializedSizeZC(&writer, &value));
  return size;
}

TEST(Cpp2OpsTest, StringWithCustomAllocator) {
  const char* const kTooLong =
      "This is too long for the small string optimization";
  const std::vector<std::string> expected{kTooLong, "b"};
  const std::pmr::vector<std::pmr::string> value{kTooLong, "b"};
  const std::string bytes = writeCompact(value);
  EXPECT_EQ(bytes, writeCompact(expected));
  EXPECT_EQ(sizeCompact(value), sizeCompact(expected));
  EXPECT_EQ(
      apache::thrift::debugString(value),
      apache::thrift::debugString(expected));

  std::pmr::monotonic_buffer_resource res;
  std::pmr::vector<std::pmr::string> read(&res);
  const auto buf = folly::IOBuf::wrapBuffer(bytes.data(), bytes.size());
  apache::thrift::CompactProtocolReader reader;
  reader.setInput(buf.get());
  apache::thrift::Cpp2Ops<decltype(read)>::read(&reader, &read);
  EXPECT_EQ(read, value);
  EXPECT_EQ(read[0].get_allocator().resource(), &res);
}

} // namespace thrift::test::cpp2ops

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
