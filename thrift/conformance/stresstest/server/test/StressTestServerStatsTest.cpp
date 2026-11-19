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

#include <thrift/conformance/stresstest/server/StressTestServerStats.h>

#include <chrono>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace apache::thrift::stress {
namespace {

ResultMetadata makeMetadata() {
  ResultMetadata metadata;
  metadata.runId() = "run";
  metadata.workload() = "workload";
  return metadata;
}

TEST(StressTestServerStatsTest, OmitsRxTimestampsWithoutSamples) {
  StressTestServerStats stats;
  stats.recordConnection(ConnectionMode::Tls);

  const auto result = stats.publish(makeMetadata());

  EXPECT_FALSE(result.rxTimestamps().has_value());
  EXPECT_EQ(result.connections()->at(ConnectionMode::Tls), 1);
}

TEST(StressTestServerStatsTest, PublishesRxTimestampPercentiles) {
  StressTestServerStats stats;
  stats.recordRxTimestampCollect(std::chrono::microseconds{100});
  stats.recordRxTimestampCollect(std::chrono::microseconds{200});
  stats.recordRxTimestampCollect(std::chrono::microseconds{300});

  const auto result = stats.publish(makeMetadata());

  ASSERT_TRUE(result.rxTimestamps().has_value());
  const auto& rxTimestamps = *result.rxTimestamps();
  EXPECT_EQ(*rxTimestamps.samples(), 3);
  EXPECT_LE(*rxTimestamps.p50Us(), *rxTimestamps.p99Us());
  EXPECT_LE(*rxTimestamps.p99Us(), *rxTimestamps.p100Us());
  EXPECT_GE(*rxTimestamps.p50Us(), 0);
  EXPECT_LE(*rxTimestamps.p100Us(), 400);
}

TEST(StressTestServerStatsTest, PublishesRxTimestampPercentilesAboveOneSecond) {
  StressTestServerStats stats;
  stats.recordRxTimestampCollect(std::chrono::microseconds{2'000'000});
  stats.recordRxTimestampCollect(std::chrono::microseconds{4'000'000});

  const auto result = stats.publish(makeMetadata());

  ASSERT_TRUE(result.rxTimestamps().has_value());
  const auto& rxTimestamps = *result.rxTimestamps();
  EXPECT_EQ(*rxTimestamps.samples(), 2);
  EXPECT_GT(*rxTimestamps.p50Us(), 1'000'000);
  EXPECT_GT(*rxTimestamps.p99Us(), 1'000'000);
  EXPECT_GT(*rxTimestamps.p100Us(), 1'000'000);
}

TEST(StressTestServerStatsTest, RecordsSamplesFromConcurrentIoThreads) {
  constexpr size_t kThreadCount = 4;
  constexpr size_t kSamplesPerThread = 1000;
  StressTestServerStats stats;
  std::vector<std::thread> threads;
  threads.reserve(kThreadCount);

  for (size_t i = 0; i < kThreadCount; ++i) {
    threads.emplace_back([&stats] {
      for (size_t sample = 0; sample < kSamplesPerThread; ++sample) {
        stats.recordRxTimestampCollect(std::chrono::microseconds{sample});
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }

  const auto result = stats.publish(makeMetadata());
  ASSERT_TRUE(result.rxTimestamps().has_value());
  EXPECT_EQ(
      *result.rxTimestamps()->samples(),
      static_cast<int64_t>(kThreadCount * kSamplesPerThread));
}

} // namespace
} // namespace apache::thrift::stress
