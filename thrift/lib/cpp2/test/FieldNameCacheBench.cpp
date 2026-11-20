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

#include <thrift/lib/cpp2/protocol/Serializer.h>

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <folly/io/IOBuf.h>
#include <folly/json.h>
#include <thrift/lib/cpp2/gen/tcc_struct_traits.h>
#include <thrift/lib/cpp2/test/Structs.h>
#include <thrift/lib/cpp2/test/gen-cpp2/FieldNameCacheBench_data.h>
#include <thrift/lib/cpp2/test/gen-cpp2/FieldNameCacheBench_types.h>
#include <thrift/lib/cpp2/test/gen-cpp2/FieldNameCacheBench_types_custom_protocol.h>
#include <thrift/lib/cpp2/test/gen-cpp2/ProtocolBenchData_data.h>
#include <thrift/lib/cpp2/test/gen-cpp2/ProtocolBenchData_types_custom_protocol.h>

namespace {

using apache::thrift::BinarySerializer;
using apache::thrift::SimpleJSONSerializer;
using apache::thrift::protocol::TType;
using thrift::benchmark::Fields12;
using thrift::benchmark::Fields13;
using thrift::benchmark::LargeMixed;
using thrift::benchmark::LargeMixedSparse;
using thrift::benchmark::Mixed;
using thrift::benchmark::MixedInt;

// Payload preparation, correctness checks, and cache warming are not timed.
// Decode timing includes constructing and destroying a fresh output object.
template <typename Struct>
Struct makeFixture() {
  if constexpr (
      std::is_same_v<Struct, Fields12> || std::is_same_v<Struct, Fields13>) {
    Struct value;
    value.field01() = 101;
    value.field02() = 102;
    value.field03() = 103;
    value.field04() = 104;
    value.field05() = 105;
    value.field06() = 106;
    value.field07() = 107;
    value.field08() = 108;
    value.field09() = 109;
    value.field10() = 110;
    value.field11() = 111;
    value.field12() = 112;
    if constexpr (std::is_same_v<Struct, Fields13>) {
      value.field13() = 113;
    }
    return value;
  } else {
    return create<Struct>();
  }
}

enum class Payload {
  Forward,
  Reverse,
  UnknownFields,
};

template <typename Struct>
std::string makeJson(const Struct& value, Payload payload) {
  auto json = SimpleJSONSerializer::serialize<std::string>(value);
  if (payload == Payload::Forward) {
    return json;
  }
  const auto fields = folly::parseJson(json);
  std::vector<std::string> entries;
  for (auto name : apache::thrift::TStructDataStorage<Struct>::fields_names) {
    if (fields.count(name)) {
      entries.push_back(
          folly::toJson(folly::dynamic(name)) + ":" +
          folly::toJson(fields.at(name)));
    }
  }
  if (payload == Payload::Reverse) {
    std::reverse(entries.begin(), entries.end());
  } else {
    entries.insert(entries.begin(), "\"unknown_before\":0");
    entries.insert(
        entries.begin() + entries.size() / 2, "\"unknown_middle\":0");
    entries.emplace_back("\"unknown_after\":0");
  }
  json = "{";
  for (size_t i = 0; i < entries.size(); ++i) {
    if (i) {
      json += ',';
    }
    json += entries[i];
  }
  json += '}';
  return json;
}

template <typename Struct, typename Serializer = SimpleJSONSerializer>
void decode(unsigned iterations, Payload payload = Payload::Forward) {
  folly::BenchmarkSuspender suspension;
  const auto expected = makeFixture<Struct>();
  auto bytes = [&] {
    if constexpr (std::is_same_v<Serializer, SimpleJSONSerializer>) {
      return makeJson(expected, payload);
    } else {
      return Serializer::template serialize<std::string>(expected);
    }
  }();
  const auto input = folly::IOBuf::copyBuffer(bytes);
  Struct check;
  Serializer::deserialize(input.get(), check);
  if (check != expected) {
    throw std::runtime_error("benchmark payload did not round trip");
  }
  suspension.dismissing([&] {
    for (unsigned i = 0; i < iterations; ++i) {
      Struct output;
      Serializer::deserialize(input.get(), output);
      folly::doNotOptimizeAway(output);
    }
  });
}

enum class Lookup {
  Hits,
  First,
  Last,
  Miss,
  IdOnly,
};

struct Request {
  std::string_view name;
  int16_t id;
  TType expectedType;
  int16_t expectedId;
};

template <typename Struct>
unsigned lookup(unsigned iterations, Lookup mode) {
  using Data = apache::thrift::TStructDataStorage<Struct>;
  using Traits = apache::thrift::detail::TccStructTraits<Struct>;
  constexpr auto kNoId = std::numeric_limits<int16_t>::min();
  folly::BenchmarkSuspender suspension;
  std::vector<Request> requests;
  if (mode == Lookup::Miss) {
    requests.push_back({"unknown_field", kNoId, TType::T_STOP, kNoId});
  } else {
    const size_t begin = mode == Lookup::Last ? Data::fields_size - 1 : 0;
    const size_t end = mode == Lookup::Hits ? Data::fields_size : begin + 1;
    for (size_t i = begin; i < end; ++i) {
      requests.push_back({
          mode == Lookup::IdOnly ? std::string_view{} : Data::fields_names[i],
          mode == Lookup::IdOnly ? Data::fields_ids[i] : kNoId,
          Data::fields_types[i],
          Data::fields_ids[i],
      });
    }
  }
  for (const auto& request : requests) {
    int16_t id = request.id;
    auto type = TType::T_STOP;
    Traits::translateFieldName(request.name, id, type);
    if (std::pair{id, type} !=
        std::pair{request.expectedId, request.expectedType}) {
      throw std::runtime_error("benchmark lookup did not match metadata");
    }
  }
  const auto requestCount = static_cast<unsigned>(requests.size());
  if (iterations > std::numeric_limits<unsigned>::max() / requestCount) {
    throw std::overflow_error("benchmark operation count overflow");
  }
  suspension.dismissing([&] {
    for (unsigned i = 0; i < iterations; ++i) {
      for (const auto& request : requests) {
        int16_t id = request.id;
        auto type = TType::T_STOP;
        Traits::translateFieldName(request.name, id, type);
        folly::doNotOptimizeAway(id);
        folly::doNotOptimizeAway(type);
      }
    }
  });
  return iterations * requestCount;
}

BENCHMARK(SimpleJSON_Mixed4, iterations) {
  decode<Mixed>(iterations);
}
BENCHMARK(SimpleJSON_MixedInt12, iterations) {
  decode<MixedInt>(iterations);
}
BENCHMARK(SimpleJSON_Fields12, iterations) {
  decode<Fields12>(iterations);
}
BENCHMARK(SimpleJSON_Fields13, iterations) {
  decode<Fields13>(iterations);
}
BENCHMARK(SimpleJSON_Fields13Reverse, iterations) {
  decode<Fields13>(iterations, Payload::Reverse);
}
BENCHMARK(SimpleJSON_Fields13Unknown, iterations) {
  decode<Fields13>(iterations, Payload::UnknownFields);
}
BENCHMARK(SimpleJSON_Large100, iterations) {
  decode<LargeMixed>(iterations);
}
BENCHMARK(SimpleJSON_Large100Reverse, iterations) {
  decode<LargeMixed>(iterations, Payload::Reverse);
}
BENCHMARK(SimpleJSON_Sparse100, iterations) {
  decode<LargeMixedSparse>(iterations);
}
BENCHMARK(SimpleJSON_Sparse100Reverse, iterations) {
  decode<LargeMixedSparse>(iterations, Payload::Reverse);
}
BENCHMARK(SimpleJSON_Sparse100Unknown, iterations) {
  decode<LargeMixedSparse>(iterations, Payload::UnknownFields);
}
BENCHMARK(Binary_Fields13, iterations) {
  decode<Fields13, BinarySerializer>(iterations);
}
BENCHMARK(Binary_Large100, iterations) {
  decode<LargeMixed, BinarySerializer>(iterations);
}

#define THRIFT_FIELD_NAME_CACHE_LOOKUP_BENCHMARKS(Struct, Suffix) \
  BENCHMARK_MULTI(Lookup##Suffix##Hits, iterations) {             \
    return lookup<Struct>(iterations, Lookup::Hits);              \
  }                                                               \
  BENCHMARK_MULTI(Lookup##Suffix##First, iterations) {            \
    return lookup<Struct>(iterations, Lookup::First);             \
  }                                                               \
  BENCHMARK_MULTI(Lookup##Suffix##Last, iterations) {             \
    return lookup<Struct>(iterations, Lookup::Last);              \
  }                                                               \
  BENCHMARK_MULTI(Lookup##Suffix##Miss, iterations) {             \
    return lookup<Struct>(iterations, Lookup::Miss);              \
  }                                                               \
  BENCHMARK_MULTI(Lookup##Suffix##IdOnly, iterations) {           \
    return lookup<Struct>(iterations, Lookup::IdOnly);            \
  }

THRIFT_FIELD_NAME_CACHE_LOOKUP_BENCHMARKS(Fields12, 12)
THRIFT_FIELD_NAME_CACHE_LOOKUP_BENCHMARKS(Fields13, 13)
THRIFT_FIELD_NAME_CACHE_LOOKUP_BENCHMARKS(LargeMixed, 100)
#undef THRIFT_FIELD_NAME_CACHE_LOOKUP_BENCHMARKS

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  folly::runBenchmarks();
}
