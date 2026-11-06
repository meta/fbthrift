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

#include <thrift/lib/cpp2/frozen/FrozenUtil.h>
#include <thrift/lib/cpp2/protocol/Json5Protocol.h>
#include <thrift/lib/cpp2/protocol/Object.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>
#include <thrift/lib/cpp2/test/Structs.h>

#include <glog/logging.h>
#include <folly/Benchmark.h>
#include <folly/Optional.h>
#include <folly/init/Init.h>

using namespace apache::thrift;
using namespace thrift::benchmark;

template <class>
struct SerializerTraits;
template <class ReaderType, class WriterType>
struct SerializerTraits<Serializer<ReaderType, WriterType>> {
  using Reader = ReaderType;
  using Writer = WriterType;
};

template <class T>
using GetReader = typename SerializerTraits<T>::Reader;
template <class T>
using GetWriter = typename SerializerTraits<T>::Writer;

struct FrozenSerializer {
  template <class T>
  static void serialize(const T& obj, folly::IOBufQueue* out) {
    out->append(folly::IOBuf::fromString(frozen::freezeToString(obj)));
  }
  template <class T>
  static size_t deserialize(folly::IOBuf* iobuf, T& t) {
    auto view = frozen::mapFrozen<T>(iobuf->coalesce());
    t = view.thaw();
    return 0;
  }
};

// C++ JSON5 consumers go through `op::encode`/`op::decode` (as
// `Json5ProtocolUtils` does), not the generated code a `Serializer<>` alias
// would use.
template <class OptionsTag>
struct Json5OpSerializer {
  template <class T>
  static void serialize(const T& obj, folly::IOBufQueue* out) {
    json5::detail::Json5ProtocolWriter writer(
        COPY_EXTERNAL_BUFFER, OptionsTag::kOptions);
    writer.setOutput(out);
    op::encode<type::infer_tag<T>>(writer, obj);
  }
  template <class T>
  static size_t deserialize(folly::IOBuf* iobuf, T& t) {
    json5::detail::Json5ProtocolReader reader;
    reader.setInput(iobuf);
    op::decode<type::infer_tag<T>>(reader, t);
    return 0;
  }
};

struct Json5DefaultOptions {
  static constexpr json5::detail::Json5ProtocolWriter::Options kOptions{};
};
struct Json5CompatOptions {
  static constexpr json5::detail::Json5ProtocolWriter::Options kOptions{
      .writer = {},
      .enumAsInteger = true,
      .binaryAsBase64String = true,
      .mapPrimitiveKeysAsMemberNames = true,
      .keyOrder = KeyOrder::Unspecified,
  };
};
using Json5Serializer = Json5OpSerializer<Json5DefaultOptions>;
using Json5CompatSerializer = Json5OpSerializer<Json5CompatOptions>;

struct Json5OnSimpleJSONSerializer {
  template <class T>
  static void serialize(const T& obj, folly::IOBufQueue* out) {
    SimpleJSONSerializer::serialize(obj, out);
  }
  template <class T>
  static size_t deserialize(folly::IOBuf* iobuf, T& t) {
    return Json5Serializer::deserialize(iobuf, t);
  }
};

enum class SerializerMethod {
  Codegen,
  Object,
};

// The benckmark is to measure single struct use case, the iteration here is
// more like a benchmark artifact, so avoid doing optimizationon iteration
// usecase in this benchmark (e.g. move string definition out of while loop)

template <
    SerializerMethod kSerializerMethod,
    typename Serializer,
    typename Struct>
void writeBench(size_t iters) {
  folly::BenchmarkSuspender susp;
  auto strct = create<Struct>();
  protocol::Object obj;
  if constexpr (kSerializerMethod == SerializerMethod::Object) {
    folly::IOBufQueue q;
    Serializer::serialize(strct, &q);
    obj = protocol::parseObject<GetReader<Serializer>>(*q.move());
  }

  folly::IOBufQueue q;
  susp.dismiss();
  while (iters--) {
    if constexpr (kSerializerMethod == SerializerMethod::Object) {
      protocol::serializeObject<GetWriter<Serializer>>(obj, q);
      folly::doNotOptimizeAway(q);
    } else {
      Serializer::serialize(strct, &q);
      folly::doNotOptimizeAway(q);
    }

    // Reuse the queue across iterations to avoid allocating a new buffer for
    // each struct (which would dominate the measurement), but keep only the
    // tail to avoid unbounded growth.
    if (auto head = q.front(); head && head->isChained()) {
      q.append(q.move()->prev()->unlink());
    }
  }
  susp.rehire();
}

template <
    SerializerMethod kSerializerMethod,
    typename Serializer,
    typename Struct>
void readBench(size_t iters) {
  folly::BenchmarkSuspender susp;
  auto strct = create<Struct>();
  folly::IOBufQueue q;
  Serializer::serialize(strct, &q);
  auto buf = q.move();
  // coalesce the IOBuf chain to test fast path
  buf->coalesce();

  susp.dismiss();
  while (iters--) {
    if constexpr (kSerializerMethod == SerializerMethod::Object) {
      auto obj = protocol::parseObject<GetReader<Serializer>>(*buf);
      folly::doNotOptimizeAway(obj);
    } else {
      Struct data;
      Serializer::deserialize(buf.get(), data);
      folly::doNotOptimizeAway(data);
    }
  }
  susp.rehire();
}

constexpr SerializerMethod getSerializerMethod(std::string_view prefix) {
  return prefix == "" || prefix == "OpEncode" ? SerializerMethod::Codegen
      : prefix == "Object"
      ? SerializerMethod::Object
      : throw std::invalid_argument(std::string(prefix) + " is invalid");
}

// clang-format off
#define X1(Prefix, proto, rdwr, bench, benchprefix)                        \
  BENCHMARK(Prefix##proto##Protocol_##rdwr##_##bench, iters) {             \
    rdwr##Bench<                                                           \
        getSerializerMethod(#Prefix),                                      \
        proto##Serializer,                                                 \
        benchprefix##bench>(iters);                                        \
  }

#define X2(Prefix, proto, bench)  \
  X1(Prefix, proto, write, bench,) \
  X1(Prefix, proto, read, bench,)

#define OpEncodeX2(Prefix, proto, bench)  \
  X1(Prefix, proto, write, bench, Op) \
  X1(Prefix, proto, read, bench, Op)

#define APPLY(M, Prefix, proto)        \
  M(Prefix, proto, Empty)              \
  M(Prefix, proto, SmallInt)           \
  M(Prefix, proto, BigInt)             \
  M(Prefix, proto, SmallString)        \
  M(Prefix, proto, BigString)          \
  M(Prefix, proto, BigBinary)          \
  M(Prefix, proto, LargeBinary)        \
  M(Prefix, proto, Mixed)              \
  M(Prefix, proto, MixedUnion)         \
  M(Prefix, proto, MixedByte)           \
  M(Prefix, proto, MixedShort)         \
  M(Prefix, proto, MixedInt)           \
  M(Prefix, proto, MixedBigInt)        \
  M(Prefix, proto, LargeMixed)         \
  M(Prefix, proto, LargeMixedSparse)   \
  M(Prefix, proto, SmallListInt)       \
  M(Prefix, proto, BigListByte)        \
  M(Prefix, proto, BigListShort)       \
  M(Prefix, proto, BigListInt)         \
  M(Prefix, proto, BigListBigInt)      \
  M(Prefix, proto, BigListFloat)       \
  M(Prefix, proto, BigListDouble)      \
  M(Prefix, proto, BigListMixed)       \
  M(Prefix, proto, BigListMixedByte)   \
  M(Prefix, proto, BigListMixedShort)  \
  M(Prefix, proto, BigListMixedInt)    \
  M(Prefix, proto, BigListMixedBigInt) \
  M(Prefix, proto, LargeListMixed)     \
  M(Prefix, proto, LargeSetInt)        \
  M(Prefix, proto, UnorderedSetInt)    \
  M(Prefix, proto, SortedVecSetInt)    \
  M(Prefix, proto, LargeMapInt)        \
  M(Prefix, proto, LargeMapMixed)      \
  M(Prefix, proto, LargeUnorderedMapMixed)        \
  M(Prefix, proto, LargeSortedVecMapMixed)        \
  M(Prefix, proto, UnorderedMapInt)    \
  M(Prefix, proto, NestedMap)          \
  M(Prefix, proto, SortedVecNestedMap) \
  M(Prefix, proto, ComplexStruct)      \
  M(Prefix, proto, ComplexUnion)

#define X(Prefix, proto) APPLY(X2, Prefix, proto)              

#define OpEncodeX(Prefix, proto) APPLY(OpEncodeX2, Prefix, proto)              

// One struct per shape (scalars, strings, binary, unions, lists, sets, maps,
// nesting) keeps the run short. BigListFloat is left out: SimpleJSON writes
// 3499211520.0f as 3499211500, which Json5ProtocolReader rejects as an inexact
// integer-to-float conversion.
#define READ_SIMPLE_JSON_X(Prefix, proto) \
  X1(Prefix, proto, read, SmallInt,)      \
  X1(Prefix, proto, read, BigInt,)        \
  X1(Prefix, proto, read, SmallString,)   \
  X1(Prefix, proto, read, BigString,)     \
  X1(Prefix, proto, read, BigBinary,)     \
  X1(Prefix, proto, read, Mixed,)         \
  X1(Prefix, proto, read, MixedUnion,)    \
  X1(Prefix, proto, read, LargeMixed,)    \
  X1(Prefix, proto, read, BigListInt,)    \
  X1(Prefix, proto, read, BigListDouble,) \
  X1(Prefix, proto, read, BigListMixed,)  \
  X1(Prefix, proto, read, LargeSetInt,)   \
  X1(Prefix, proto, read, LargeMapInt,)   \
  X1(Prefix, proto, read, LargeMapMixed,) \
  X1(Prefix, proto, read, NestedMap,)     \
  X1(Prefix, proto, read, ComplexStruct,) \
  X1(Prefix, proto, read, ComplexUnion,)

// NOLINTBEGIN(facebook-avoid-non-const-global-variables)
X(, Binary)
X(, Compact)
X(, SimpleJSON)
X(, JSON)
X(, Json5)
X(, Json5Compat)
READ_SIMPLE_JSON_X(, Json5OnSimpleJSON)
X(, Frozen)
X(Object, Binary)
X(Object, Compact)
OpEncodeX(OpEncode, Binary)
OpEncodeX(OpEncode, Compact)

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  folly::runBenchmarks();
}
// NOLINTEND(facebook-avoid-non-const-global-variables)
// clang-format on
