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

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/ReadyWaiters.h>

#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>

#include <array>
#include <cstddef>
#include <optional>
#include <tuple>
#include <vector>

namespace apache::thrift::fast_thrift::channel_pipeline::detail {
namespace {

using namespace testing;

// Looks up the armed indexes from the lowest up, as a write pass does.
std::vector<std::size_t> armedFromLowest(const ReadyWaiters& waiters) {
  std::vector<std::size_t> armed;
  std::size_t begin = 0;
  while (begin < ReadyWaiters::kMaxIndexes) {
    const std::optional<std::size_t> index = waiters.lowestArmedFrom(begin);
    if (!index) {
      break;
    }
    armed.push_back(*index);
    begin = *index + 1;
  }
  return armed;
}

// Looks up the armed indexes below end from the highest down, as a read pass
// does.
std::vector<std::size_t> armedFromHighestBelow(
    const ReadyWaiters& waiters, std::size_t end) {
  std::vector<std::size_t> armed;
  while (const std::optional<std::size_t> index =
             waiters.highestArmedBelow(end)) {
    armed.push_back(*index);
    end = *index;
  }
  return armed;
}

// Arms the lowest and the highest index and indexes in between, out of order.
void armOutOfOrder(ReadyWaiters& waiters) {
  for (const std::size_t index : std::array<std::size_t, 5>{32, 63, 0, 31, 1}) {
    waiters.arm(index);
  }
}

TEST(ReadyWaitersTest, FindsArmedIndexesFromTheLowestUp) {
  ReadyWaiters waiters;
  armOutOfOrder(waiters);

  EXPECT_THAT(
      armedFromLowest(waiters), Eq(std::vector<std::size_t>{0, 1, 31, 32, 63}));
}

TEST(ReadyWaitersTest, FindsArmedIndexesBelowEndFromTheHighestDown) {
  ReadyWaiters waiters;
  armOutOfOrder(waiters);

  EXPECT_THAT(
      armedFromHighestBelow(waiters, 63),
      Eq(std::vector<std::size_t>{32, 31, 1, 0}));
}

TEST(ReadyWaitersTest, DoesNotFindAnIndexBelowBegin) {
  ReadyWaiters waiters;
  waiters.arm(5);
  const std::optional<std::size_t> first = waiters.lowestArmedFrom(0);
  // The woken handler's callback makes a handler nearer the head arm.
  waiters.arm(2);

  EXPECT_THAT(
      std::tuple(first, waiters.lowestArmedFrom(6)),
      Eq(std::tuple(
          std::optional<std::size_t>{5}, std::optional<std::size_t>{})));
}

TEST(ReadyWaitersTest, DoesNotFindAnIndexAtOrAboveEnd) {
  ReadyWaiters waiters;
  waiters.arm(2);
  const std::optional<std::size_t> first = waiters.highestArmedBelow(63);
  // The woken handler's callback makes a handler nearer the tail arm.
  waiters.arm(5);

  EXPECT_THAT(
      std::tuple(first, waiters.highestArmedBelow(2)),
      Eq(std::tuple(
          std::optional<std::size_t>{2}, std::optional<std::size_t>{})));
}

TEST(ReadyWaitersTest, DoesNotFindACancelledIndex) {
  ReadyWaiters waiters;
  waiters.arm(1);
  waiters.arm(4);
  waiters.cancel(1);

  EXPECT_THAT(armedFromLowest(waiters), Eq(std::vector<std::size_t>{4}));
}

TEST(ReadyWaitersTest, ClearDisarmsEveryIndex) {
  ReadyWaiters waiters;
  armOutOfOrder(waiters);
  waiters.clear();

  EXPECT_THAT(
      std::tuple(waiters.empty(), waiters.isArmed(0), waiters.isArmed(63)),
      Eq(std::tuple(true, false, false)));
}

} // namespace
} // namespace apache::thrift::fast_thrift::channel_pipeline::detail
