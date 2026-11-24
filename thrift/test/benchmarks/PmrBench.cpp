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

// Read times of cpp.allocator structs with std::pmr containers. A read
// includes destroying the value read.

#include <cstdio>
#include <memory>

#include <glog/logging.h>
#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <folly/io/IOBuf.h>
#include <folly/io/IOBufQueue.h>

#include <thrift/lib/cpp2/protocol/Serializer.h>
#include <thrift/test/benchmarks/gen-cpp2/PmrBenchData_types.h>

namespace {

using namespace thrift::benchmark::pmr;

constexpr int kSize = 10'000;

PmrMixed mixed() {
  PmrMixed d;
  d.int32() = 5;
  d.int64() = 12345;
  d.b() = true;
  d.str() = "hellohellohellohello";
  return d;
}

template <typename T>
T list() {
  T d;
  for (int i = 0; i < kSize; ++i) {
    d.lst()->push_back(i * 7919);
  }
  return d;
}

template <typename T>
T listMixed() {
  T d;
  for (int i = 0; i < kSize; ++i) {
    d.lst()->push_back(mixed());
  }
  return d;
}

template <typename T>
T mapInt() {
  T d;
  for (int i = 0; i < kSize; ++i) {
    d.m()[i] = i * 7919;
  }
  return d;
}

template <typename T>
T mapMixed() {
  T d;
  for (int i = 0; i < kSize; ++i) {
    d.m()[i] = mixed();
  }
  return d;
}

// Keys longer than the small-string buffer, so each one allocates.
template <typename T>
T mapStrInt() {
  T d;
  for (int i = 0; i < kSize; ++i) {
    char key[32];
    std::snprintf(key, sizeof(key), "key-with-no-sso-%08d", i);
    d.m()[key] = i;
  }
  return d;
}

template <typename Serializer, typename T>
std::unique_ptr<folly::IOBuf> serialize(const T& value) {
  folly::IOBufQueue q;
  Serializer::serialize(value, &q);
  auto buf = q.move();
  buf->coalesce();
  return buf;
}

template <typename Serializer, typename T>
void readBench(unsigned iters, T (*make)()) {
  std::unique_ptr<folly::IOBuf> buf;
  BENCHMARK_SUSPEND {
    const T value = make();
    buf = serialize<Serializer>(value);
    T roundTrip;
    Serializer::deserialize(buf.get(), roundTrip);
    CHECK(roundTrip == value);
  }
  while (iters--) {
    T data;
    Serializer::deserialize(buf.get(), data);
    folly::doNotOptimizeAway(data);
  }
}

#define READ(prefix, Proto, name, make)                        \
  BENCHMARK(prefix##Proto##Protocol_read_##name, iters) {      \
    readBench<apache::thrift::Proto##Serializer>(iters, make); \
  }
#define ROWS(name, make)     \
  READ(, Binary, name, make) \
  READ(, Compact, name, make)

ROWS(PmrMixed, mixed)
ROWS(PmrBigListInt, list<PmrBigListInt>)
READ(OpEncode, Binary, PmrBigListInt, list<OpPmrBigListInt>)
READ(OpEncode, Compact, PmrBigListInt, list<OpPmrBigListInt>)
ROWS(PmrBigListMixed, listMixed<PmrBigListMixed>)
ROWS(PmrMapInt, mapInt<PmrMapInt>)
ROWS(PmrMapMixed, mapMixed<PmrMapMixed>)
ROWS(PmrMapStrInt, mapStrInt<PmrMapStrInt>)
ROWS(PmrUnorderedMapMixed, mapMixed<PmrUnorderedMapMixed>)
ROWS(PmrUnorderedMapStrInt, mapStrInt<PmrUnorderedMapStrInt>)
ROWS(PmrSortedVecMapMixed, mapMixed<PmrSortedVecMapMixed>)

} // namespace

int main(int argc, char** argv) {
  const folly::Init init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
