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

#include <thrift/lib/cpp2/protocol/CompactProtocol.h>

#include <array>
#include <bit>
#include <span>
#include <type_traits>

#include <folly/Conv.h>

#if FOLLY_ARM_FEATURE_NEON_SVE_BRIDGE

#include <arm_neon.h>
#include <arm_sve.h>

#include <arm_neon_sve_bridge.h> // @manual

#endif

namespace apache::thrift {

[[noreturn]] void CompactProtocolReader::throwBadProtocolIdentifier() {
  throw TProtocolException(
      TProtocolException::BAD_VERSION, "Bad protocol identifier");
}

[[noreturn]] void CompactProtocolReader::throwBadProtocolVersion() {
  throw TProtocolException(
      TProtocolException::BAD_VERSION, "Bad protocol version");
}

[[noreturn]] void CompactProtocolReader::throwBadType(const uint8_t type) {
  throw TProtocolException(
      folly::to<std::string>("don't know what type: ", type));
}

void CompactProtocolReader::readFieldBeginWithStateMediumSlow(
    StructReadState& state, int16_t prevFieldId) {
  auto byte = *in_.data();
  in_.skipNoAdvance(1);

  readFieldBeginWithStateImpl(state, prevFieldId, byte);
}

namespace {
// Floating-point elements are byte-swapped as integers of their width, as the
// per-element float and double methods and Binary's bulk methods do.
template <typename T>
using SwapAs = std::conditional_t<
    std::is_same_v<T, float>,
    uint32_t,
    std::conditional_t<std::is_same_v<T, double>, uint64_t, T>>;
} // namespace

// Function used with data types that are just sent as BE/LE bytes
template <class Cursor, typename T, bool BE>
static inline size_t writeUnencodedArithmeticVector(
    Cursor& c, const T* inputPtr, size_t numElements) {
  size_t len = numElements * sizeof(T);
  size_t i = 0;
  while (i < numElements) {
    c.ensureWithinMaxGrowth(len);
    // The cursor's maximum growth can be smaller than one element.
    c.ensure(sizeof(T));
    uint8_t* outPtr = c.writableData();
    size_t loopBound = std::min(numElements, (c.length() / sizeof(T)) + i);
    size_t j = 0;
    for (; i < loopBound; ++i, ++j) {
      auto bits = std::bit_cast<SwapAs<T>>(inputPtr[i]);
      folly::storeUnaligned<SwapAs<T>>(
          outPtr + j * sizeof(T), BE ? folly::Endian::big(bits) : bits);
    }
    c.append(j * sizeof(T));
  }
  return len;
}

#if FOLLY_ARM_FEATURE_NEON_SVE_BRIDGE

// Compute signedIntToZigzag on Eight 16-bit vars
static uint16x8_t wideSignedInt16ToZigzag(uint16x8_t n) {
  uint16x8_t cmpV = vcltzq_s16(vreinterpretq_s16_u16(n));
  n = vshlq_n_u16(n, 1);
  n = veorq_u16(n, cmpV);
  return n;
}

// Compute signedIntToZigzag on four 32-bit vars
static uint32x4_t wideSignedInt32ToZigzag(uint32x4_t n) {
  uint32x4_t cmpV = vcltzq_s32(vreinterpretq_s32_u32(n));
  n = vshlq_n_u32(n, 1);
  n = veorq_u32(n, cmpV);
  return n;
}

// Compute signedIntToZigzag on two 64-bit vars
static uint64x2_t wideSignedInt64ToZigzag(uint64x2_t n) {
  uint64x2_t cmpV = vcltzq_s64(vreinterpretq_s64_u64(n));
  n = vshlq_n_u64(n, 1);
  n = veorq_u64(n, cmpV);
  return n;
}

// NEON variations of function signedIntToZigzag, from VarintUtils-inl.h
template <typename T>
static poly128_t wideSignedIntToZigzag(poly128_t n) {
  if constexpr (sizeof(T) == 2) {
    return vreinterpretq_p128_u16(
        wideSignedInt16ToZigzag(vreinterpretq_u16_p128(n)));
  } else if constexpr (sizeof(T) == 4) {
    return vreinterpretq_p128_u32(
        wideSignedInt32ToZigzag(vreinterpretq_u32_p128(n)));
  } else {
    return vreinterpretq_p128_u64(
        wideSignedInt64ToZigzag(vreinterpretq_u64_p128(n)));
  }
}

template <class Cursor, class T>
static uint64_t writeTwoVarintSve(Cursor& c, uint64x2_t vec) {
  c.ensure(sizeof(T) == 4 ? 13 : 20);

  uint64_t upcastedValA = vec[0];
  uint64_t upcastedValB = vec[1];
  svuint8_t mask = svdup_n_u8(0x7f);
  svuint64_t clzMask = svreinterpret_u64_u8(svdup_n_u8(0x80));
  svbool_t clzMaskCheck;
  uint64x2_t zeroCheck = vceqzq_u64(vec);

  if constexpr (sizeof(T) == 8) {
    clzMaskCheck = svcmple_n_u64(
        svptrue_b64(), svset_neonq_u64(svundef_u64(), vec), (1ull << 56) - 1);
  } else {
    clzMaskCheck = svptrue_b64();
  }

  vec = svget_neonq_u64(svbdep_u64(
      svset_neonq_u64(svundef_u64(), vec), svreinterpret_u64_u8(mask)));

  zeroCheck = svget_neonq_u64(svand_n_u64_x(
      svptrue_b64(), svset_neonq_u64(svundef_u64(), zeroCheck), 1));

  svuint64_t clzV =
      svclz_u64_x(svptrue_b64(), svset_neonq_u64(svundef_u64(), vec));

  svuint64_t sizeSV = svlsr_n_u64_x(svptrue_b64(), clzV, 3);

  clzV = svand_n_u64_x(svptrue_b64(), clzV, 120);

  sizeSV = svsubr_n_u64_x(svptrue_b64(), sizeSV, 8);

  sizeSV = svset_neonq_u64(
      svundef_u64(), vaddq_u64(svget_neonq_u64(sizeSV), zeroCheck));

  clzV = svadd_n_u64_x(svptrue_b64(), clzV, 8);

  clzMask = svlsr_u64_m(clzMaskCheck, clzMask, clzV);

  uint64_t sizeA = sizeSV[0];
  uint64_t sizeB = sizeSV[1];

  vec = vorrq_u64(vec, svget_neonq_u64(clzMask));

  uint8_t* p = c.writableData();

  if constexpr (sizeof(T) == sizeof(uint32_t)) {
    vst1q_lane_u64(p, vec, 0);
    vst1q_lane_u64(p + sizeA, vec, 1);
  } else {
    vst1q_lane_u64(p, vec, 0);
    if (upcastedValA >= (1ull << 56)) {
      p[8] = upcastedValA >> 56;
      upcastedValA >>= 63;
      p[9] = upcastedValA;
      sizeA = 9 + upcastedValA;
    }
    p += sizeA;
    vst1q_lane_u64(p, vec, 1);
    if (upcastedValB >= (1ull << 56)) {
      p[8] = upcastedValB >> 56;
      upcastedValB >>= 63;
      p[9] = upcastedValB;
      sizeB = 9 + upcastedValB;
    }
  }

  uint64_t size = sizeA + sizeB;

  c.append(size);
  return size;
}

template <class Cursor, class T>
static uint64_t writeTwoVarint64Sve(Cursor& c, uint64x2_t vec) {
  return writeTwoVarintSve<Cursor, T>(c, vec);
}

template <class Cursor, class T>
static uint64_t writeTwoVarint32Sve(Cursor& c, uint64x2_t vec) {
  return writeTwoVarintSve<Cursor, T>(c, vec);
}

template <class Cursor, class T>
static uint64_t writeFourVarint32Sve(
    Cursor& c, uint64x2_t vec1, uint64x2_t vec2) {
  svuint8_t bdepMask = svdup_n_u8(0x7f);
  uint64x2_t clzMask = vreinterpretq_u64_u8(vdupq_n_u8(0xff));

  uint64x2_t zeroCheck1 = vceqzq_u64(vec1);
  uint64x2_t zeroCheck2 = vceqzq_u64(vec2);

  c.ensure(23);

  vec1 = svget_neonq_u64(svbdep_u64(
      svset_neonq_u64(svundef_u64(), vec1), svreinterpret_u64_u8(bdepMask)));
  vec2 = svget_neonq_u64(svbdep_u64(
      svset_neonq_u64(svundef_u64(), vec2), svreinterpret_u64_u8(bdepMask)));

  zeroCheck1 = svget_neonq_u64(svand_n_u64_x(
      svptrue_b64(), svset_neonq_u64(svundef_u64(), zeroCheck1), 1));

  zeroCheck2 = svget_neonq_u64(svand_n_u64_x(
      svptrue_b64(), svset_neonq_u64(svundef_u64(), zeroCheck2), 1));

  svuint64_t clzV1 =
      svclz_u64_x(svptrue_b64(), svset_neonq_u64(svundef_u64(), vec1));

  svuint64_t clzV2 =
      svclz_u64_x(svptrue_b64(), svset_neonq_u64(svundef_u64(), vec2));

  vec1 = svget_neonq_u8(svorr_n_u8_x(
      svptrue_b8(),
      svset_neonq_u8(svundef_u8(), vreinterpretq_u8_u64(vec1)),
      0x80));

  vec2 = svget_neonq_u8(svorr_n_u8_x(
      svptrue_b8(),
      svset_neonq_u8(svundef_u8(), vreinterpretq_u8_u64(vec2)),
      0x80));

  svuint64_t sizeSV1 = svlsr_n_u64_x(svptrue_b64(), clzV1, 3);

  svuint64_t sizeSV2 = svlsr_n_u64_x(svptrue_b64(), clzV2, 3);

  svuint64_t clzMask1 = svlsr_u64_x(
      svptrue_b64(), svset_neonq_u64(svundef_u64(), clzMask), clzV1);

  svuint64_t clzMask2 = svlsr_u64_x(
      svptrue_b64(), svset_neonq_u64(svundef_u64(), clzMask), clzV2);

  sizeSV1 = svset_neonq_u64(
      svundef_u64(), vsubq_u64(svget_neonq_u64(sizeSV1), zeroCheck1));

  sizeSV2 = svset_neonq_u64(
      svundef_u64(), vsubq_u64(svget_neonq_u64(sizeSV2), zeroCheck2));

  uint64_t sizeA = 8 - sizeSV1[0];
  uint64_t sizeB = 8 - sizeSV1[1];
  uint64_t sizeC = 8 - sizeSV2[0];
  uint64_t sizeD = 8 - sizeSV2[1];

  vec1 = vandq_u64(vec1, svget_neonq_u64(clzMask1));

  vec2 = vandq_u64(vec2, svget_neonq_u64(clzMask2));

  uint8_t* p = c.writableData();

  vst1q_lane_u64(p, vec1, 0);
  p += sizeA;
  vst1q_lane_u64(p, vec1, 1);
  p += sizeB;
  vst1q_lane_u64(p, vec2, 0);
  p += sizeC;
  vst1q_lane_u64(p, vec2, 1);

  uint64_t size = sizeA + sizeB + sizeC + sizeD;

  c.append(size);
  return size;
}

template <class Cursor, class T>
static uint64_t writeFourVarint16Sve(Cursor& c, uint32x4_t vec) {
  svuint8_t mask = svdup_n_u8(0x7f);
  uint64x2_t clzMask = vreinterpretq_u64_u8(vdupq_n_u8(0xff));
  uint32x4_t zeroCheck = vceqzq_u32(vec);

  c.ensure(13);

  vec = svget_neonq_u32(svbdep_u32(
      svset_neonq_u32(svundef_u32(), vec), svreinterpret_u32_u8(mask)));

  zeroCheck = svget_neonq_u32(svand_n_u32_x(
      svptrue_b32(), svset_neonq_u32(svundef_u32(), zeroCheck), 1));

  svuint32_t clzV = svset_neonq_u32(svundef_u32(), vclzq_u32(vec));

  svuint32_t sizeSV = svlsr_n_u32_x(svptrue_b32(), clzV, 3);

  vec = svget_neonq_u8(svorr_n_u8_x(
      svptrue_b8(),
      svset_neonq_u8(svundef_u8(), vreinterpretq_u8_u32(vec)),
      0x80));

  sizeSV = svset_neonq_u32(
      svundef_u32(), vsubq_u32(svget_neonq_u32(sizeSV), zeroCheck));

  clzMask = svget_neonq_u32(svlsr_u32_x(
      svptrue_b32(), svset_neonq_u32(svundef_u32(), clzMask), clzV));

  uint64_t sizeA = 4 - svget_neonq_u32(sizeSV)[0];
  uint64_t sizeB = 4 - sizeSV[1];
  uint64_t sizeC = 4 - sizeSV[2];
  uint64_t sizeD = 4 - sizeSV[3];

  uint64_t totalSize = sizeA + sizeB + sizeC + sizeD;

  vec = vandq_u64(vec, clzMask);

  uint8_t* p = c.writableData();

  vst1q_lane_u32(p, vec, 0);
  p += sizeA;
  vst1q_lane_u32(p, vec, 1);
  p += sizeB;
  vst1q_lane_u32(p, vec, 2);
  p += sizeC;
  vst1q_lane_u32(p, vec, 3);

  c.append(totalSize);
  return totalSize;
}

template <typename T>
constexpr size_t kSimdWriteWidth = sizeof(T) == 2 ? 4 : 2;

template <typename T>
static poly128_t readArithmeticElements(const T* inputPtr) {
  // Using svptrue_b8() instead of svptrue_b32()/svptrue_b64() results in one
  // less instruction being emitted. Issue tracked in T221893168
  if constexpr (sizeof(T) == 2) {
    return vreinterpretq_p128_u32(svget_neonq_u32(svld1uh_u32(
        svptrue_b8(), reinterpret_cast<const uint16_t*>(inputPtr))));
  } else if constexpr (sizeof(T) == 4) {
    return vreinterpretq_p128_u64(svget_neonq_u64(svld1uw_u64(
        svptrue_b8(), reinterpret_cast<const uint32_t*>(inputPtr))));
  } else {
    return vreinterpretq_p128_u64(
        vld1q_u64(reinterpret_cast<const uint64_t*>(inputPtr)));
  }
}

template <class Cursor, class T>
static uint64_t writeArithmeticElements(Cursor& c, poly128_t vec) {
  if constexpr (sizeof(T) == 2) {
    return writeFourVarint16Sve<Cursor, T>(c, vreinterpretq_u32_p128(vec));
  } else {
    return writeTwoVarint64Sve<Cursor, T>(c, vreinterpretq_u64_p128(vec));
  }
}

// Function used with data types that are encoded as zigzag and compacted
template <class Cursor, typename T>
static inline size_t writeEncodedArithmeticVector(
    Cursor& c, const T* inputPtr, size_t numElements) {
  uint64_t bytesWritten = 0;
  size_t numElementsMainLoop = numElements - numElements % kSimdWriteWidth<T>;
  size_t i = 0;
  for (; i < numElementsMainLoop; i += kSimdWriteWidth<T>) {
    auto vec = readArithmeticElements<T>(inputPtr + i);
    vec = wideSignedIntToZigzag<T>(vec);
    bytesWritten += writeArithmeticElements<Cursor, T>(c, vec);
  }

  if constexpr (kSimdWriteWidth<T> == 2) {
    // only one element may be left
    if (i < numElements) {
      if constexpr (sizeof(T) == 8) {
        bytesWritten += apache::thrift::util::writeVarint(
            c, apache::thrift::util::i64ToZigzag(inputPtr[i]));
      } else {
        bytesWritten += apache::thrift::util::writeVarint(
            c, apache::thrift::util::i32ToZigzag(inputPtr[i]));
      }
    }
  } else {
    for (; i < numElements; i += 1) {
      int16_t signedVal = (int16_t)inputPtr[i];
      bytesWritten += apache::thrift::util::writeVarint(
          c, apache::thrift::util::i32ToZigzag(signedVal));
    }
  }

  return bytesWritten;
}

// Loop unrolled variant of writeEncodedArithmeticVector
// Function used with 32-bit data types, these are zigzag encoded and compacted
template <class Cursor, typename T>
static inline size_t writeEncodedArithmeticVectorInterleaved(
    Cursor& c, const T* inputPtr, size_t numElements) {
  constexpr size_t kInterleavedSimdWriteWidth = 2 * kSimdWriteWidth<T>;
  uint64_t bytesWritten = 0;
  size_t numElementsModWidth = numElements % kInterleavedSimdWriteWidth;
  size_t numElementsMainLoop = numElements - numElementsModWidth;
  size_t i = 0;
  for (; i < numElementsMainLoop; i += kInterleavedSimdWriteWidth) {
    auto vec1 = readArithmeticElements<T>(inputPtr + i);
    auto vec2 = readArithmeticElements<T>(inputPtr + i + kSimdWriteWidth<T>);
    vec1 = wideSignedIntToZigzag<T>(vec1);
    vec2 = wideSignedIntToZigzag<T>(vec2);
    bytesWritten += writeFourVarint32Sve<Cursor, T>(
        c, vreinterpretq_u64_p128(vec1), vreinterpretq_u64_p128(vec2));
  }

  if (numElementsModWidth >= kSimdWriteWidth<T>) {
    auto vec = readArithmeticElements<T>(inputPtr + i);
    vec = wideSignedIntToZigzag<T>(vec);
    bytesWritten +=
        writeTwoVarint32Sve<Cursor, T>(c, vreinterpretq_u64_p128(vec));
    i += kSimdWriteWidth<T>;
  }

  if (numElementsModWidth & 1) {
    bytesWritten += apache::thrift::util::writeVarint(
        c, apache::thrift::util::i32ToZigzag(inputPtr[i]));
  }

  return bytesWritten;
}

template <>
size_t CompactProtocolWriter::writeArithmeticVector<int64_t>(
    const int64_t* inputPtr, size_t numElements) {
  return writeEncodedArithmeticVector<QueueAppender, int64_t>(
      out_, inputPtr, numElements);
}
template <>
size_t CompactProtocolWriter::writeArithmeticVector<uint64_t>(
    const uint64_t* inputPtr, size_t numElements) {
  return writeEncodedArithmeticVector<QueueAppender, uint64_t>(
      out_, inputPtr, numElements);
}
template <>
size_t CompactProtocolWriter::writeArithmeticVector<int32_t>(
    const int32_t* inputPtr, size_t numElements) {
  return writeEncodedArithmeticVectorInterleaved<QueueAppender, int32_t>(
      out_, inputPtr, numElements);
}
template <>
size_t CompactProtocolWriter::writeArithmeticVector<uint32_t>(
    const uint32_t* inputPtr, size_t numElements) {
  return writeEncodedArithmeticVectorInterleaved<QueueAppender, uint32_t>(
      out_, inputPtr, numElements);
}
template <>
size_t CompactProtocolWriter::writeArithmeticVector<int16_t>(
    const int16_t* inputPtr, size_t numElements) {
  return writeEncodedArithmeticVector<QueueAppender, int16_t>(
      out_, inputPtr, numElements);
}
template <>
size_t CompactProtocolWriter::writeArithmeticVector<uint16_t>(
    const uint16_t* inputPtr, size_t numElements) {
  return writeEncodedArithmeticVector<QueueAppender, uint16_t>(
      out_, inputPtr, numElements);
}
#else // FOLLY_ARM_FEATURE_NEON_SVE_BRIDGE

// Like writeI16, zigzag-encodes int16_t in 32 bits: in 16 bits, every element
// needs an extra zero extension before appendVarint.
template <typename T>
static auto toZigzag(T value) {
  if constexpr (sizeof(T) == 8) {
    return util::i64ToZigzag(static_cast<int64_t>(value));
  } else {
    return util::i32ToZigzag(static_cast<std::make_signed_t<T>>(value));
  }
}

#if FOLLY_X64 && THRIFT_UTIL_VARINTUTILS_BRANCH_FREE_ENCODER
// Both indexed by the number of leading zero bits of the 64-bit value, which
// fixes how many 7-bit groups its varint needs. Grouping the value's 64 bits
// by 7 from the bottom (bit 63 on its own):
//
//   zeros == 10:
//     0 0000000 001xxxx xxxxxxx xxxxxxx xxxxxxx xxxxxxx xxxxxxx xxxxxxx xxxxxxx
//     kVarintSizes[10] == 8
//     kVarintContinuationBitMasks[10] == 0x0080808080808080
constexpr auto kVarintSizes = [] {
  std::array<uint8_t, 65> sizes{};
  for (int zeros = 0; zeros <= 64; ++zeros) {
    sizes[zeros] = std::max(1, (64 - zeros + 6) / 7);
  }
  return sizes;
}();
constexpr auto kVarintContinuationBitMasks = [] {
  std::array<uint64_t, 65> bits{};
  for (int zeros = 0; zeros <= 64; ++zeros) {
    for (int byte = 0; byte < std::min(kVarintSizes[zeros] - 1, 8); ++byte) {
      bits[zeros] |= uint64_t{0x80} << (8 * byte);
    }
  }
  return bits;
}();
#endif

// The most bytes appendVarint stores for a U, whatever the varint's length.
template <typename U>
constexpr size_t kAppendVarintBytes =
    std::max(sizeof(uint64_t), util::detail::kVarintMaxBytes<U>);

// Returns the end of the varint, but may store up to kAppendVarintBytes<U>
// bytes past p. With BMI2 it has no branches, unlike util::writeVarint, so
// lists of mixed-length varints cost no mispredictions.
template <typename U>
static uint8_t* appendVarint(uint8_t* p, U value) {
#if FOLLY_X64 && THRIFT_UTIL_VARINTUTILS_BRANCH_FREE_ENCODER
  const int zeros = std::countl_zero(uint64_t{value});
  const uint64_t firstEightBytes =
      _pdep_u64(value, 0x7f7f7f7f7f7f7f7f) | kVarintContinuationBitMasks[zeros];
  folly::storeUnaligned<uint64_t>(p, firstEightBytes);
  if constexpr (sizeof(U) == 8) {
    p[8] = static_cast<uint8_t>(value >> 56);
    p[9] = static_cast<uint8_t>(value >> 63);
  }
  return p + kVarintSizes[zeros];
#else
  if constexpr (sizeof(U) == 8) {
    return p + util::writeVarint64(value, p);
  } else {
    return p + util::writeVarint32(value, p);
  }
#endif
}

template <typename T>
static size_t writeVarintArithmeticVector(
    QueueAppender& out, const T* inputPtr, size_t numElements) {
  constexpr size_t kBytesPerElement =
      kAppendVarintBytes<decltype(toZigzag(T()))>;
  size_t bytesWritten = 0;
  size_t i = 0;
  while (i < numElements) {
    out.ensure(kBytesPerElement);
    size_t loopBound =
        std::min(numElements, i + out.length() / kBytesPerElement);
    uint8_t* const begin = out.writableData();
    uint8_t* p = begin;
    for (; i < loopBound; ++i) {
      p = appendVarint(p, toZigzag(inputPtr[i]));
    }
    out.append(p - begin);
    bytesWritten += p - begin;
  }
  return bytesWritten;
}

template <>
size_t CompactProtocolWriter::writeArithmeticVector<int64_t>(
    const int64_t* inputPtr, size_t numElements) {
  return writeVarintArithmeticVector(out_, inputPtr, numElements);
}
template <>
size_t CompactProtocolWriter::writeArithmeticVector<uint64_t>(
    const uint64_t* inputPtr, size_t numElements) {
  return writeVarintArithmeticVector(out_, inputPtr, numElements);
}
template <>
size_t CompactProtocolWriter::writeArithmeticVector<int32_t>(
    const int32_t* inputPtr, size_t numElements) {
  return writeVarintArithmeticVector(out_, inputPtr, numElements);
}
template <>
size_t CompactProtocolWriter::writeArithmeticVector<uint32_t>(
    const uint32_t* inputPtr, size_t numElements) {
  return writeVarintArithmeticVector(out_, inputPtr, numElements);
}
template <>
size_t CompactProtocolWriter::writeArithmeticVector<int16_t>(
    const int16_t* inputPtr, size_t numElements) {
  return writeVarintArithmeticVector(out_, inputPtr, numElements);
}
template <>
size_t CompactProtocolWriter::writeArithmeticVector<uint16_t>(
    const uint16_t* inputPtr, size_t numElements) {
  return writeVarintArithmeticVector(out_, inputPtr, numElements);
}

#endif // FOLLY_ARM_FEATURE_NEON_SVE_BRIDGE

template <>
size_t CompactProtocolWriter::writeArithmeticVector<int8_t>(
    const int8_t* inputPtr, size_t numElements) {
  return writeUnencodedArithmeticVector<QueueAppender, int8_t, false>(
      out_, inputPtr, numElements);
}
template <>
size_t CompactProtocolWriter::writeArithmeticVector<uint8_t>(
    const uint8_t* inputPtr, size_t numElements) {
  return writeUnencodedArithmeticVector<QueueAppender, uint8_t, false>(
      out_, inputPtr, numElements);
}
template <>
size_t CompactProtocolWriter::writeArithmeticVector<float>(
    const float* inputPtr, size_t numElements) {
  return writeUnencodedArithmeticVector<QueueAppender, float, true>(
      out_, inputPtr, numElements);
}
template <>
size_t CompactProtocolWriter::writeArithmeticVector<double>(
    const double* inputPtr, size_t numElements) {
  return writeUnencodedArithmeticVector<QueueAppender, double, true>(
      out_, inputPtr, numElements);
}

// Room for the longest varint of T. The decoders may read all of it even when
// the varint is shorter.
template <typename T>
using VarintBytes = std::span<const uint8_t, util::detail::kVarintMaxBytes<T>>;

// Returns the number of bytes the varint takes.
template <typename T>
static size_t readVarint(T& value, VarintBytes<T> bytes) {
  return util::detail::readVarintMediumSlowUnrolled(value, bytes.data());
}

#if THRIFT_UTIL_VARINTUTILS_BMI2_DECODER
// readVarintMediumSlowUnrolled takes a branch per byte, which is slow for the
// 9- and 10-byte varints of values such as hashes; those take one branch here.
static size_t readVarint(int64_t& value, VarintBytes<int64_t> bytes) {
  const uint64_t firstEightBytes = folly::loadUnaligned<uint64_t>(bytes.data());
  constexpr uint64_t kContinuationBitMask = 0x8080808080808080;
  if ((firstEightBytes & kContinuationBitMask) != kContinuationBitMask) {
    // The varint is at most 8 bytes long.
    return util::detail::readVarintMediumSlowUnrolled(value, bytes.data());
  }
  const uint64_t ninthByte = bytes[8];
  const uint64_t tenthByte = bytes[9];
  const uint64_t hasTenthByte = ninthByte >> 7;
  if (hasTenthByte & (tenthByte >> 7)) {
    util::detail::throwInvalidVarint();
  }
  // Bits 0-55; pext drops the continuation bits.
  uint64_t result = _pext_u64(firstEightBytes, 0x7f7f7f7f7f7f7f7f);
  // Bits 56-62; "& 0x7f" drops the continuation bit.
  result |= (ninthByte & 0x7f) << 56;
  // Bit 63, if there is a tenth byte.
  result |= (tenthByte & hasTenthByte) << 63;
  value = static_cast<int64_t>(result);
  return 9 + hasTenthByte;
}
#endif

// Reads a varint in place if the buffer holds the longest one, else a byte at a
// time.
template <typename T, class Cursor>
FOLLY_ALWAYS_INLINE void readVarintAfterPairs(Cursor& c, T& value) {
  constexpr size_t kMaxVarintBytes = util::detail::kVarintMaxBytes<T>;
  if (c.length() >= kMaxVarintBytes) {
    c.skipNoAdvance(
        readVarint(value, VarintBytes<T>(c.data(), kMaxVarintBytes)));
  } else {
    util::detail::readVarintSlow(c, value);
  }
}

// Function used with data types that are decoded from compacted zigzag
template <class Cursor, typename T>
static inline void readEncodedArithmeticVector(
    Cursor& c, T* outputPtr, size_t numElements) {
  size_t i = 0;
  size_t loopBound = numElements - 1;
  while (i < numElements) {
    folly::ByteRange currentRange = c.peekBytes();
    const uint8_t* inPtr = currentRange.data();
    constexpr size_t kMaxVarintBytes = sizeof(T) == 2
        ? util::detail::kVarintMaxBytes<int32_t>
        : util::detail::kVarintMaxBytes<T>;
    constexpr size_t kMaxBytesIter = kMaxVarintBytes * 2;
    const uint8_t* endVec = inPtr + currentRange.size() - kMaxBytesIter;
    for (; i < loopBound && inPtr <= endVec; i += 2) {
      if constexpr (sizeof(T) == 2) {
        int32_t valueA;
        int32_t valueB;
        inPtr +=
            readVarint(valueA, VarintBytes<int32_t>(inPtr, kMaxVarintBytes));
        inPtr +=
            readVarint(valueB, VarintBytes<int32_t>(inPtr, kMaxVarintBytes));
        valueA = util::detail::zigzagToSignedInt(valueA);
        valueB = util::detail::zigzagToSignedInt(valueB);
        outputPtr[i] = (int16_t)valueA;
        outputPtr[i + 1] = (int16_t)valueB;
      } else {
        T valueA;
        T valueB;
        // The loop condition leaves room for two of the longest varints.
        inPtr += readVarint(valueA, VarintBytes<T>(inPtr, kMaxVarintBytes));
        inPtr += readVarint(valueB, VarintBytes<T>(inPtr, kMaxVarintBytes));
        valueA = util::detail::zigzagToSignedInt(valueA);
        valueB = util::detail::zigzagToSignedInt(valueB);
        outputPtr[i] = valueA;
        outputPtr[i + 1] = valueB;
      }
    }
    size_t consumed = inPtr - currentRange.data();
    c.skipNoAdvance(consumed);
    size_t len = currentRange.size() - consumed;
    // At least one element, so that readVarintSlow moves on to the next buffer
    // or throws at the end of the input.
    size_t trailingLoopBound =
        std::min(numElements, i + std::max<size_t>(len, 1));
    while (i < trailingLoopBound) {
      if constexpr (sizeof(T) == 2) {
        // Need to finish consuming current input buffer
        int32_t value;
        readVarintAfterPairs(c, value);
        outputPtr[i] = (int16_t)util::detail::zigzagToSignedInt(value);
      } else {
        // Need to finish consuming current input buffer
        T value;
        readVarintAfterPairs(c, value);
        outputPtr[i] = util::detail::zigzagToSignedInt(value);
      }
      ++i;
    }
  }
}

// Function used with data types that are just received as BE/LE bytes
template <class Cursor, typename T, bool BE>
static inline void readUnencodedArithmeticVector(
    Cursor& c, T* outputPtr, size_t numElements) {
  size_t i = 0;
  while (i < numElements) {
    folly::ByteRange currentRange = c.peekBytes();
    const uint8_t* inPtr = currentRange.data();
    size_t loopBound =
        std::min(numElements, i + currentRange.size() / sizeof(T));
    size_t j = 0;
    for (; i < loopBound; ++i, ++j) {
      auto bits = folly::loadUnaligned<SwapAs<T>>(inPtr + j * sizeof(T));
      outputPtr[i] = std::bit_cast<T>(BE ? folly::Endian::big(bits) : bits);
    }
    c.skipNoAdvance(j * sizeof(T));
    if (i < numElements) {
      if constexpr (sizeof(T) == 8) {
        uint64_t bits = c.template readBE<int64_t>();
        outputPtr[i] = std::bit_cast<double>(bits);
      } else if constexpr (sizeof(T) == 4) {
        uint32_t bits = c.template readBE<int32_t>();
        outputPtr[i] = std::bit_cast<float>(bits);
      } else {
        outputPtr[i] = c.template read<int8_t>();
      }
      ++i;
    }
  }
}

template <>
void CompactProtocolReader::readArithmeticVector<int64_t>(
    int64_t* outputPtr, size_t numElements) {
  return readEncodedArithmeticVector<Cursor, int64_t>(
      in_, outputPtr, numElements);
}
template <>
void CompactProtocolReader::readArithmeticVector<uint64_t>(
    uint64_t* outputPtr, size_t numElements) {
  return readEncodedArithmeticVector<Cursor, int64_t>(
      in_, reinterpret_cast<int64_t*>(outputPtr), numElements);
}
template <>
void CompactProtocolReader::readArithmeticVector<int32_t>(
    int32_t* outputPtr, size_t numElements) {
  return readEncodedArithmeticVector<Cursor, int32_t>(
      in_, outputPtr, numElements);
}
template <>
void CompactProtocolReader::readArithmeticVector<uint32_t>(
    uint32_t* outputPtr, size_t numElements) {
  return readEncodedArithmeticVector<Cursor, int32_t>(
      in_, reinterpret_cast<int32_t*>(outputPtr), numElements);
}
template <>
void CompactProtocolReader::readArithmeticVector<int16_t>(
    int16_t* outputPtr, size_t numElements) {
  return readEncodedArithmeticVector<Cursor, int16_t>(
      in_, outputPtr, numElements);
}
template <>
void CompactProtocolReader::readArithmeticVector<uint16_t>(
    uint16_t* outputPtr, size_t numElements) {
  return readEncodedArithmeticVector<Cursor, int16_t>(
      in_, reinterpret_cast<int16_t*>(outputPtr), numElements);
}
template <>
void CompactProtocolReader::readArithmeticVector<int8_t>(
    int8_t* outputPtr, size_t numElements) {
  return readUnencodedArithmeticVector<Cursor, int8_t, false>(
      in_, outputPtr, numElements);
}
template <>
void CompactProtocolReader::readArithmeticVector<uint8_t>(
    uint8_t* outputPtr, size_t numElements) {
  return readUnencodedArithmeticVector<Cursor, uint8_t, false>(
      in_, outputPtr, numElements);
}
template <>
void CompactProtocolReader::readArithmeticVector<float>(
    float* outputPtr, size_t numElements) {
  return readUnencodedArithmeticVector<Cursor, float, true>(
      in_, outputPtr, numElements);
}
template <>
void CompactProtocolReader::readArithmeticVector<double>(
    double* outputPtr, size_t numElements) {
  return readUnencodedArithmeticVector<Cursor, double, true>(
      in_, outputPtr, numElements);
}

} // namespace apache::thrift
