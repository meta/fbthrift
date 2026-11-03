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

#include <benchmark/benchmark.h>

#include <thrift/lib/cpp2/op/Get.h>
#include <thrift/lib/cpp2/protocol/FieldMask.h>
#include <thrift/lib/cpp2/test/gen-cpp2/FieldMaskBenchmark_types.h>

namespace apache::thrift::benchmark::field_mask {
namespace {

protocol::Mask makeMask(int fields, bool excluded = false) {
  protocol::Mask mask;
  auto& included =
      excluded ? mask.excludes().emplace() : mask.includes().emplace();
  for (int16_t id = 1; id <= fields; ++id) {
    included.emplace(id, protocol::allMask());
  }
  return mask;
}

template <class T>
T makeSource() {
  T source;
  op::for_each_field_id<T>([&](auto id) {
    op::get<decltype(id)>(source) = static_cast<int64_t>(id());
  });
  return source;
}

template <class T>
bool checkFiltered(const T& result, int64_t fields, bool excluded = false) {
  bool correct = true;
  op::for_each_field_id<T>([&](auto id) {
    auto number = static_cast<int64_t>(id());
    correct &= *op::get<decltype(id)>(result) ==
        ((number <= fields) != excluded ? number : 0);
  });
  return correct;
}

template <class T, bool Excluded = false>
void validate(::benchmark::State& state) {
  auto mask = makeMask(static_cast<int>(state.range(0)), Excluded);
  for (auto _ : state) {
    ::benchmark::DoNotOptimize(mask);
    auto valid = protocol::detail::validate_mask<T>(protocol::MaskRef{mask});
    ::benchmark::DoNotOptimize(valid);
  }
}

template <class T, bool Excluded = false>
void filter(::benchmark::State& state) {
  auto mask = makeMask(static_cast<int>(state.range(0)), Excluded);
  const auto source = makeSource<T>();
  if (!checkFiltered(
          protocol::filter(mask, source), state.range(0), Excluded)) {
    state.SkipWithError("Filtered fields differ from the selected mask");
    return;
  }
  for (auto _ : state) {
    ::benchmark::DoNotOptimize(mask);
    auto result = protocol::filter(mask, source);
    ::benchmark::DoNotOptimize(result);
  }
}

template <bool Excluded>
void filterNestedImpl(::benchmark::State& state) {
  protocol::Mask mask;
  auto& included = mask.includes().emplace();
  included.emplace(1, makeMask(static_cast<int>(state.range(0)), Excluded));
  included.emplace(2, makeMask(static_cast<int>(state.range(0)), Excluded));
  Nested source;
  source.left() = makeSource<Medium>();
  source.right() = makeSource<Medium>();
  const auto sample = protocol::filter(mask, source);
  if (!checkFiltered(*sample.left(), state.range(0), Excluded) ||
      !checkFiltered(*sample.right(), state.range(0), Excluded)) {
    state.SkipWithError("Filtered nested fields differ from the selected mask");
    return;
  }
  for (auto _ : state) {
    ::benchmark::DoNotOptimize(mask);
    auto result = protocol::filter(mask, source);
    ::benchmark::DoNotOptimize(result);
  }
}

template <class T>
void validateExcluded(::benchmark::State& state) {
  validate<T, true>(state);
}

template <class T>
void filterExcluded(::benchmark::State& state) {
  filter<T, true>(state);
}

void filterNested(::benchmark::State& state) {
  filterNestedImpl<false>(state);
}

void filterNestedExcluded(::benchmark::State& state) {
  filterNestedImpl<true>(state);
}

BENCHMARK_TEMPLATE(validate, Small)->Arg(0)->Arg(1)->Arg(2);
BENCHMARK_TEMPLATE(validate, Medium)->Arg(0)->Arg(1)->Arg(8)->Arg(32);
BENCHMARK_TEMPLATE(validate, Large)->Arg(0)->Arg(1)->Arg(8)->Arg(256);

BENCHMARK_TEMPLATE(filter, Small)->Arg(1)->Arg(2);
BENCHMARK_TEMPLATE(filter, Medium)->Arg(1)->Arg(8)->Arg(32);
BENCHMARK_TEMPLATE(filter, Large)->Arg(1)->Arg(8)->Arg(256);
BENCHMARK(filterNested)->Arg(1)->Arg(8)->Arg(32);

BENCHMARK_TEMPLATE(validateExcluded, Small)->Arg(1)->Arg(2);
BENCHMARK_TEMPLATE(validateExcluded, Medium)->Arg(1)->Arg(32);
BENCHMARK_TEMPLATE(validateExcluded, Large)->Arg(1)->Arg(256);

BENCHMARK_TEMPLATE(filterExcluded, Small)->Arg(1)->Arg(2);
BENCHMARK_TEMPLATE(filterExcluded, Medium)->Arg(1)->Arg(32);
BENCHMARK_TEMPLATE(filterExcluded, Large)->Arg(1)->Arg(256);
BENCHMARK(filterNestedExcluded)->Arg(1)->Arg(8)->Arg(32);

} // namespace
} // namespace apache::thrift::benchmark::field_mask

BENCHMARK_MAIN();
