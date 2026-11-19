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

// CompactProtocol's writeArithmeticVector must write the same bytes as writing
// each element, and readArithmeticVector must read lists written one element at
// a time.

#include <algorithm>
#include <cstdint>
#include <memory>
#include <numeric>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>

#include <gtest/gtest.h>
#include <folly/io/IOBuf.h>
#include <folly/io/IOBufQueue.h>
#include <thrift/lib/cpp/util/VarintUtils.h>
#include <thrift/lib/cpp2/op/Encode.h>
#include <thrift/lib/cpp2/protocol/CompactProtocol.h>
#include <thrift/lib/cpp2/type/NativeType.h>

using namespace apache::thrift;

namespace {

class CompactProtocolArithmeticVectorTest : public testing::Test {
 protected:
  // `maxGrowth` limits the size of each output buffer, so that the bulk path
  // crosses buffer boundaries.
  template <typename T>
  static void expectBulkWriteMatchesEachElement(
      const std::vector<T>& values, size_t maxGrowth) {
    const std::string expected = writeEachElement(values);
    CompactProtocolWriter writer;
    folly::IOBufQueue queue;
    writer.setOutput(&queue, maxGrowth);
    const size_t written =
        writer.writeArithmeticVector(values.data(), values.size());
    EXPECT_EQ(written, expected.size());
    std::string bytes;
    queue.appendToString(bytes);
    EXPECT_EQ(bytes, expected);
  }

  // `inputBufferSize` limits the size of each input buffer, so that the bulk
  // path crosses buffer boundaries.
  template <typename T>
  static void expectBulkReadMatchesEachElement(
      const std::vector<T>& values, size_t inputBufferSize) {
    const std::string encoded = writeEachElement(values);
    const auto input = splitIntoBuffers(encoded, inputBufferSize);
    CompactProtocolReader reader;
    reader.setInput(input.get());
    std::vector<T> decoded(values.size());
    reader.readArithmeticVector(decoded.data(), decoded.size());
    EXPECT_EQ(reader.getCursor().getCurrentPosition(), encoded.size());
    EXPECT_EQ(decoded, values);
  }

 private:
  template <typename T>
  static std::string writeEachElement(const std::vector<T>& values) {
    CompactProtocolWriter writer;
    folly::IOBufQueue queue;
    writer.setOutput(&queue);
    for (T value : values) {
      op::encode<type::infer_tag<T>>(writer, value);
    }
    std::string encoded;
    queue.appendToString(encoded);
    return encoded;
  }

  static std::unique_ptr<folly::IOBuf> splitIntoBuffers(
      const std::string& data, size_t bufferSize) {
    auto chain = folly::IOBuf::create(0);
    for (size_t pos = 0; pos < data.size(); pos += bufferSize) {
      chain->appendToChain(
          folly::IOBuf::wrapBuffer(
              data.data() + pos, std::min(bufferSize, data.size() - pos)));
    }
    return chain;
  }
};

template <typename T>
class CompactProtocolArithmeticVectorTypedTest
    : public CompactProtocolArithmeticVectorTest {};

using ElementTypes = testing::Types<
    int8_t,
    uint8_t,
    int16_t,
    uint16_t,
    int32_t,
    uint32_t,
    int64_t,
    uint64_t,
    float,
    double>;
TYPED_TEST_SUITE(CompactProtocolArithmeticVectorTypedTest, ElementTypes);

// Positive and negative values of every varint length, through 64-byte output
// buffers and 7-byte input buffers, so that most elements straddle a buffer
// boundary.
TYPED_TEST(CompactProtocolArithmeticVectorTypedTest, SmallBuffers) {
  std::vector<TypeParam> values;
  for (int repeat = 0; repeat < 20; ++repeat) {
    for (int bits = 0; bits < 64; ++bits) {
      const int64_t magnitude = static_cast<int64_t>((uint64_t{1} << bits) - 1);
      values.push_back(static_cast<TypeParam>(magnitude));
      values.push_back(static_cast<TypeParam>(-magnitude));
    }
  }
  this->expectBulkWriteMatchesEachElement(values, 64);
  this->expectBulkReadMatchesEachElement(values, 7);
}

TYPED_TEST(CompactProtocolArithmeticVectorTypedTest, ShortLists) {
  const std::vector<TypeParam> values{1, 0, static_cast<TypeParam>(-1)};
  for (size_t size = 0; size <= values.size(); ++size) {
    const std::vector<TypeParam> prefix(values.begin(), values.begin() + size);
    this->expectBulkWriteMatchesEachElement(prefix, 1);
    this->expectBulkReadMatchesEachElement(prefix, 7);
  }
}

// Odd sizes, so that buffer boundaries fall at every offset within a varint.
constexpr size_t kMaxGrowth = 4093;
constexpr size_t kInputBufferSize = 4091;

TEST_F(CompactProtocolArithmeticVectorTest, Every16BitValue) {
  std::vector<uint16_t> unsignedValues(1 << 16);
  std::iota(unsignedValues.begin(), unsignedValues.end(), 0);
  const std::vector<int16_t> signedValues(
      unsignedValues.begin(), unsignedValues.end());
  expectBulkWriteMatchesEachElement(signedValues, kMaxGrowth);
  expectBulkReadMatchesEachElement(signedValues, kInputBufferSize);
  expectBulkWriteMatchesEachElement(unsignedValues, kMaxGrowth);
  expectBulkReadMatchesEachElement(unsignedValues, kInputBufferSize);
}

// Every int32_t, in 256 test cases that can run in parallel.
class CompactProtocolEvery32BitValueTest
    : public CompactProtocolArithmeticVectorTest,
      public testing::WithParamInterface<uint32_t> {};

TEST_P(CompactProtocolEvery32BitValueTest, Shard) {
  constexpr uint32_t kShardSize = uint32_t{1} << 24;
  constexpr uint32_t kChunkSize = uint32_t{1} << 20;
  for (uint32_t chunk = 0; chunk < kShardSize; chunk += kChunkSize) {
    std::vector<int32_t> values(kChunkSize);
    for (uint32_t i = 0; i < kChunkSize; ++i) {
      values[i] = static_cast<int32_t>(GetParam() * kShardSize + chunk + i);
    }
    expectBulkWriteMatchesEachElement(values, kMaxGrowth);
    expectBulkReadMatchesEachElement(values, kInputBufferSize);
  }
}

INSTANTIATE_TEST_SUITE_P(
    AllValues, CompactProtocolEvery32BitValueTest, testing::Range(0u, 256u));

// For each bit width of the zigzag encoding, so for each varint length: random
// values, and the smallest and largest values. Values of different widths are
// interleaved so that varint lengths change from one element to the next.
class CompactProtocol64BitValuesTest
    : public CompactProtocolArithmeticVectorTest,
      public testing::WithParamInterface<int> {};

TEST_P(CompactProtocol64BitValuesTest, ZigzagWidth) {
  const int width = GetParam();
  std::mt19937_64 rng(width);
  std::vector<int64_t> values;
  for (int i = 0; i < (1 << 20); ++i) {
    const int otherWidth = static_cast<int>(rng() % 65);
    for (int w : {width, otherWidth}) {
      const uint64_t topBit = w == 0 ? 0 : uint64_t{1} << (w - 1);
      const uint64_t lowBits = topBit == 0 ? 0 : rng() & (topBit - 1);
      values.push_back(util::zigzagToI64(topBit | lowBits));
    }
  }
  if (width > 0) {
    const uint64_t topBit = uint64_t{1} << (width - 1);
    values.push_back(util::zigzagToI64(topBit));
    values.push_back(util::zigzagToI64(topBit | (topBit - 1)));
  }
  const std::vector<uint64_t> unsignedValues(values.begin(), values.end());
  expectBulkWriteMatchesEachElement(values, kMaxGrowth);
  expectBulkReadMatchesEachElement(values, kInputBufferSize);
  expectBulkWriteMatchesEachElement(unsignedValues, kMaxGrowth);
  expectBulkReadMatchesEachElement(unsignedValues, kInputBufferSize);
}

INSTANTIATE_TEST_SUITE_P(
    AllWidths, CompactProtocol64BitValuesTest, testing::Range(0, 65));

template <typename T>
void expectReadArithmeticVectorThrows(const std::string& encoded) {
  const auto buf = folly::IOBuf::copyBuffer(encoded);
  CompactProtocolReader reader;
  reader.setInput(buf.get());
  std::vector<T> decoded(2);
  EXPECT_THROW(
      reader.readArithmeticVector(decoded.data(), decoded.size()),
      std::out_of_range);
}

TEST_F(CompactProtocolArithmeticVectorTest, RejectsOverlongVarints) {
  const std::string elevenBytes = std::string(10, '\xff') + '\x01';
  const std::string sixBytes = std::string(5, '\xff') + '\x01';
  // With room for a pair of the longest varints, and at the end of the input.
  const std::string padding(20, '\0');
  expectReadArithmeticVectorThrows<int64_t>(elevenBytes + padding);
  expectReadArithmeticVectorThrows<int64_t>(elevenBytes);
  expectReadArithmeticVectorThrows<int32_t>(sixBytes + padding);
  expectReadArithmeticVectorThrows<int32_t>(sixBytes);
}

} // namespace
