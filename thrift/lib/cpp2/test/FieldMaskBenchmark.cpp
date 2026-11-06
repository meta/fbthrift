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

#include <stdexcept>
#include <string>

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

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
void validate(size_t iters, int fields) {
  folly::BenchmarkSuspender susp;
  auto mask = makeMask(fields, Excluded);
  susp.dismiss();
  while (iters--) {
    folly::doNotOptimizeAway(mask);
    auto valid = protocol::detail::validate_mask<T>(protocol::MaskRef{mask});
    folly::doNotOptimizeAway(valid);
  }
  susp.rehire();
}

template <class T, bool Excluded = false>
void filter(size_t iters, int fields) {
  folly::BenchmarkSuspender susp;
  auto mask = makeMask(fields, Excluded);
  const auto source = makeSource<T>();
  if (!checkFiltered(protocol::filter(mask, source), fields, Excluded)) {
    throw std::logic_error("Filtered fields differ from the selected mask");
  }
  susp.dismiss();
  while (iters--) {
    folly::doNotOptimizeAway(mask);
    auto result = protocol::filter(mask, source);
    folly::doNotOptimizeAway(result);
  }
  susp.rehire();
}

template <bool Excluded = false>
void filterNested(size_t iters, int fields) {
  folly::BenchmarkSuspender susp;
  protocol::Mask mask;
  auto& included = mask.includes().emplace();
  included.emplace(1, makeMask(fields, Excluded));
  included.emplace(2, makeMask(fields, Excluded));
  Nested source;
  source.left() = makeSource<Medium>();
  source.right() = makeSource<Medium>();
  const auto sample = protocol::filter(mask, source);
  if (!checkFiltered(*sample.left(), fields, Excluded) ||
      !checkFiltered(*sample.right(), fields, Excluded)) {
    throw std::logic_error(
        "Filtered nested fields differ from the selected mask");
  }
  susp.dismiss();
  while (iters--) {
    folly::doNotOptimizeAway(mask);
    auto result = protocol::filter(mask, source);
    folly::doNotOptimizeAway(result);
  }
  susp.rehire();
}

// Registers `name/<fields>` for each number of masked fields.
void addBenchmarks(
    std::string_view name,
    void (*bench)(size_t, int),
    std::initializer_list<int> fieldCounts) {
  for (int fields : fieldCounts) {
    auto fullName = std::string(name) + "/" + std::to_string(fields);
    folly::addBenchmark(__FILE__, fullName, [=](unsigned iters) {
      bench(iters, fields);
      return iters;
    });
  }
}

} // namespace
} // namespace apache::thrift::benchmark::field_mask

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  using namespace apache::thrift::benchmark::field_mask;
  addBenchmarks("validate<Small>", validate<Small>, {0, 1, 2});
  addBenchmarks("validate<Medium>", validate<Medium>, {0, 1, 8, 32});
  addBenchmarks("validate<Large>", validate<Large>, {0, 1, 8, 256});

  addBenchmarks("filter<Small>", filter<Small>, {1, 2});
  addBenchmarks("filter<Medium>", filter<Medium>, {1, 8, 32});
  addBenchmarks("filter<Large>", filter<Large>, {1, 8, 256});
  addBenchmarks("filterNested", filterNested<>, {1, 8, 32});

  addBenchmarks("validateExcluded<Small>", validate<Small, true>, {1, 2});
  addBenchmarks("validateExcluded<Medium>", validate<Medium, true>, {1, 32});
  addBenchmarks("validateExcluded<Large>", validate<Large, true>, {1, 256});

  addBenchmarks("filterExcluded<Small>", filter<Small, true>, {1, 2});
  addBenchmarks("filterExcluded<Medium>", filter<Medium, true>, {1, 32});
  addBenchmarks("filterExcluded<Large>", filter<Large, true>, {1, 256});
  addBenchmarks("filterNestedExcluded", filterNested<true>, {1, 8, 32});

  folly::runBenchmarks();
}
