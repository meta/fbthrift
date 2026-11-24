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

#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <vector>

#include <fmt/core.h>
#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <folly/io/IOBufQueue.h>
#include <thrift/lib/cpp2/op/Encode.h>
#include <thrift/lib/cpp2/protocol/Json5Protocol.h>
#include <thrift/lib/cpp2/protocol/SimpleJSONProtocol.h>
#include <thrift/lib/cpp2/test/Structs.h>
#include <thrift/test/json5/gen-cpp2/json5_test_types.h>

using namespace apache::thrift;
using namespace thrift::benchmark;

// Each value is written and read through `op::encode`/`op::decode`, which is
// how C++ JSON5 consumers (e.g. `Json5ProtocolUtils`) reach the protocol.
// SimpleJSON is the baseline: it is the JSON protocol JSON5 replaces.
//
// - write/read: `Json5ProtocolWriter`/`Json5ProtocolReader` with default
//   options (compact basic JSON), into a reused queue, like ProtocolBench.
// - Json5Compat: the same with the options that make the output readable by
//   SimpleJSON.
// - toJson5/fromJson5: the public `Json5ProtocolUtils` API (indented JSON5,
//   unquoted names, trailing commas), including its string and IOBuf copies.

using MapI32I32 = std::map<std::int32_t, std::int32_t>;
using MapStringMixed = std::map<std::string, Mixed>;
using SetI32 = std::set<std::int32_t>;
using ListEnum = std::vector<facebook::thrift::json5::Enum>;

template <class T>
struct BenchTag {
  using type = type::infer_tag<T>;
};
template <>
struct BenchTag<MapI32I32> {
  using type = type::map<type::i32_t, type::i32_t>;
};
template <>
struct BenchTag<MapStringMixed> {
  using type = type::map<type::string_t, type::struct_t<Mixed>>;
};
template <>
struct BenchTag<SetI32> {
  using type = type::set<type::i32_t>;
};
template <>
struct BenchTag<ListEnum> {
  using type = type::list<type::enum_t<facebook::thrift::json5::Enum>>;
};
template <class T>
using tag_t = typename BenchTag<T>::type;

constexpr int kContainerSize = 10'000;

template <>
MapI32I32 create<MapI32I32>() {
  MapI32I32 m;
  for (int i = 0; i < kContainerSize; ++i) {
    m[i] = i * 7;
  }
  return m;
}

template <>
MapStringMixed create<MapStringMixed>() {
  MapStringMixed m;
  for (int i = 0; i < kContainerSize; ++i) {
    m[fmt::format("key{}", i)] = create<Mixed>();
  }
  return m;
}

template <>
SetI32 create<SetI32>() {
  SetI32 s;
  for (int i = 0; i < kContainerSize; ++i) {
    s.insert(i * 7);
  }
  return s;
}

template <>
ListEnum create<ListEnum>() {
  ListEnum l;
  for (int i = 0; i < kContainerSize; ++i) {
    l.push_back(static_cast<facebook::thrift::json5::Enum>(i % 5 - 1));
  }
  return l;
}

struct SimpleJson {
  template <class T>
  static void write(const T& value, folly::IOBufQueue& out) {
    SimpleJSONProtocolWriter writer;
    writer.setOutput(&out);
    op::encode<tag_t<T>>(writer, value);
  }
  template <class T>
  static void read(const folly::IOBuf& buf, T& value) {
    SimpleJSONProtocolReader reader;
    reader.setInput(&buf);
    op::decode<tag_t<T>>(reader, value);
  }
};

template <json5::detail::Json5ProtocolWriter::Options kOptions>
struct Json5WithOptions {
  template <class T>
  static void write(const T& value, folly::IOBufQueue& out) {
    json5::detail::Json5ProtocolWriter writer(COPY_EXTERNAL_BUFFER, kOptions);
    writer.setOutput(&out);
    op::encode<tag_t<T>>(writer, value);
  }
  template <class T>
  static void read(const folly::IOBuf& buf, T& value) {
    json5::detail::Json5ProtocolReader reader;
    reader.setInput(&buf);
    op::decode<tag_t<T>>(reader, value);
  }
};

using Json5 = Json5WithOptions<json5::detail::Json5ProtocolWriter::Options{}>;
using Json5Compat =
    Json5WithOptions<json5::detail::Json5ProtocolWriter::Options{
        .writer = {},
        .enumAsInteger = true,
        .binaryAsBase64String = true,
        .mapPrimitiveKeysAsMemberNames = true,
        .keyOrder = KeyOrder::Unspecified,
    }>;

template <class Protocol, class T>
void writeBench(size_t iters) {
  folly::BenchmarkSuspender susp;
  const auto value = create<T>();
  folly::IOBufQueue q;
  susp.dismiss();
  while (iters--) {
    Protocol::write(value, q);
    folly::doNotOptimizeAway(q);
    // Keep only the tail buffer, so the queue reuses it instead of allocating
    // for every value.
    if (auto head = q.front(); head && head->isChained()) {
      q.append(q.move()->prev()->unlink());
    }
  }
  susp.rehire();
}

template <class Protocol, class T>
void readBench(size_t iters) {
  folly::BenchmarkSuspender susp;
  folly::IOBufQueue q;
  Protocol::write(create<T>(), q);
  auto buf = q.move();
  buf->coalesce();
  susp.dismiss();
  while (iters--) {
    T value;
    Protocol::read(*buf, value);
    folly::doNotOptimizeAway(value);
  }
  susp.rehire();
}

template <class T>
void toJson5Bench(size_t iters) {
  folly::BenchmarkSuspender susp;
  const auto value = create<T>();
  susp.dismiss();
  while (iters--) {
    auto json5 = Json5ProtocolUtils::toJson5<tag_t<T>>(value);
    folly::doNotOptimizeAway(json5);
  }
  susp.rehire();
}

template <class T>
void fromJson5Bench(size_t iters) {
  folly::BenchmarkSuspender susp;
  const auto json5 = Json5ProtocolUtils::toJson5<tag_t<T>>(create<T>());
  susp.dismiss();
  while (iters--) {
    auto value = Json5ProtocolUtils::fromJson5<tag_t<T>>(json5);
    folly::doNotOptimizeAway(value);
  }
  susp.rehire();
}

// clang-format off
#define JSON5_BENCH(T)                                                      \
  BENCHMARK(SimpleJSON_write_##T, iters) { writeBench<SimpleJson, T>(iters); } \
  BENCHMARK_RELATIVE(Json5_write_##T, iters) { writeBench<Json5, T>(iters); } \
  BENCHMARK_RELATIVE(Json5Compat_write_##T, iters) {                         \
    writeBench<Json5Compat, T>(iters);                                       \
  }                                                                          \
  BENCHMARK_RELATIVE(Json5_toJson5_##T, iters) { toJson5Bench<T>(iters); }   \
  BENCHMARK(SimpleJSON_read_##T, iters) { readBench<SimpleJson, T>(iters); }  \
  BENCHMARK_RELATIVE(Json5_read_##T, iters) { readBench<Json5, T>(iters); }  \
  BENCHMARK_RELATIVE(Json5Compat_read_##T, iters) {                          \
    readBench<Json5Compat, T>(iters);                                        \
  }                                                                          \
  BENCHMARK_RELATIVE(Json5_fromJson5_##T, iters) { fromJson5Bench<T>(iters); } \
  BENCHMARK_DRAW_LINE();

// NOLINTBEGIN(facebook-avoid-non-const-global-variables)
JSON5_BENCH(Empty)
JSON5_BENCH(SmallInt)
JSON5_BENCH(BigInt)
JSON5_BENCH(SmallString)
JSON5_BENCH(BigString)
JSON5_BENCH(BigBinary)
JSON5_BENCH(Mixed)
JSON5_BENCH(MixedUnion)
JSON5_BENCH(LargeMixed)
JSON5_BENCH(SmallListInt)
JSON5_BENCH(BigListInt)
JSON5_BENCH(BigListBigInt)
JSON5_BENCH(BigListFloat)
JSON5_BENCH(BigListDouble)
JSON5_BENCH(BigListMixed)
JSON5_BENCH(SetI32)
JSON5_BENCH(MapI32I32)
JSON5_BENCH(MapStringMixed)
JSON5_BENCH(ListEnum)
// NOLINTEND(facebook-avoid-non-const-global-variables)
// clang-format on

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  folly::runBenchmarks();
}
