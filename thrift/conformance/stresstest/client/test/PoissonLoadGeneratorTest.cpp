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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <future>
#include <iterator>
#include <memory>
#include <mutex>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Task.h>
#include <folly/synchronization/Baton.h>
#include <thrift/conformance/stresstest/client/PoissonLoadGenerator.h>

using namespace apache::thrift;
using namespace apache::thrift::stress;
using namespace testing;
using namespace std::chrono_literals;

using Signals = folly::coro::AsyncGenerator<PoissonLoadGenerator::Count>;
using DetailedSignals =
    folly::coro::AsyncGenerator<BaseLoadGenerator::RequestSignal>;

// A client thread only gets its share of the target QPS, so a single generator
// runs at a low rate even when the aggregate rate is high.
TEST(PoissonLoadGeneratorTest, KeepsGeneratingAtLowRate) {
  PoissonLoadGenerator generator(125);
  generator.start();

  Signals signals = generator.getRequestCount();
  int64_t total = 0;
  for (int i = 0; i < 20; ++i) {
    Signals::NextResult count = folly::coro::blockingWait(signals.next());
    ASSERT_THAT(count.has_value(), IsTrue());
    total += *count;
  }

  EXPECT_THAT(total, Gt(0));
}

// 2000 qps means a 500 us mean gap, so under an exponential distribution only
// about 2% of gaps land under 10 us. A generator that releases a whole bucket
// at once puts nearly all of them there.
//
// The whole loop runs inside one blockingWait. Waiting per arrival instead
// costs more than 10 us on its own and hides the gaps being measured.
TEST(PoissonLoadGeneratorTest, SpreadsArrivalsOverTime) {
  PoissonLoadGenerator generator(2000);
  generator.start();

  const int backToBack =
      folly::coro::blockingWait([&]() -> folly::coro::Task<int> {
        Signals signals = generator.getRequestCount();
        std::chrono::steady_clock::time_point previous =
            std::chrono::steady_clock::now();
        int count = 0;
        for (int i = 0; i < 200; ++i) {
          Signals::NextResult arrival = co_await signals.next();
          EXPECT_THAT(arrival.has_value(), IsTrue());
          const std::chrono::steady_clock::time_point now =
              std::chrono::steady_clock::now();
          if (now - previous < std::chrono::microseconds(10)) {
            ++count;
          }
          previous = now;
        }
        co_return count;
      }());

  // Measured: 7 to 23 back-to-back arrivals with gaps drawn per request, 149
  // when a bucket is released at once.
  EXPECT_THAT(backToBack, Lt(75));
}

TEST(PoissonLoadGeneratorTest, StopWakesAWaitingConsumer) {
  PoissonLoadGenerator generator(0);
  generator.start();

  std::future<Signals::NextResult> next =
      std::async(std::launch::async, [&generator]() {
        Signals signals = generator.getRequestCount();
        return folly::coro::blockingWait(signals.next());
      });
  EXPECT_THAT(next.wait_for(50ms), Eq(std::future_status::timeout));

  generator.stop();

  EXPECT_THAT(next.wait_for(5s), Eq(std::future_status::ready));
  EXPECT_THAT(next.get().has_value(), IsFalse());
}

// The clock of BoundaryOrdersSignalsAndGenerationContinues. It stays far
// behind steady_clock, so the generator's timed waits return at once and only
// this clock paces the generator.
std::atomic<std::chrono::steady_clock::rep> manualClockTicks{0};

std::chrono::steady_clock::time_point manualClock() {
  return std::chrono::steady_clock::time_point{
      std::chrono::steady_clock::duration{manualClockTicks.load()}};
}

TEST(PoissonLoadGeneratorTest, BoundaryOrdersSignalsAndGenerationContinues) {
  manualClockTicks.store(0);
  std::mutex eventMutex;
  std::vector<BaseLoadGenerator::MarkerEnqueuedEvent> events;
  folly::Baton<> markerEnqueued;
  PoissonLoadGenerator generator(
      100000,
      [&](const BaseLoadGenerator::MarkerEnqueuedEvent& event) {
        {
          const std::lock_guard<std::mutex> lock(eventMutex);
          events.push_back(event);
        }
        markerEnqueued.post();
      },
      &manualClock);
  generator.start();
  const std::chrono::steady_clock::time_point measurementEnd =
      manualClock() + 1ms;
  generator.requestMeasurementBoundary(41, measurementEnd);
  // At 100,000 qps no gap reaches 1 ms, so moving the clock 10 ms past the
  // boundary makes the next arrival due.
  markerEnqueued.wait();
  manualClockTicks.fetch_add(std::chrono::steady_clock::duration{11ms}.count());

  const std::vector<BaseLoadGenerator::RequestSignal> received =
      folly::coro::blockingWait(
          [&]() -> folly::coro::Task<
                    std::vector<BaseLoadGenerator::RequestSignal>> {
            DetailedSignals signals = generator.getRequestSignals();
            std::vector<BaseLoadGenerator::RequestSignal> collected;
            bool consumedBoundary = false;
            while (!consumedBoundary ||
                   collected.back().type !=
                       BaseLoadGenerator::RequestSignal::Type::Request) {
              DetailedSignals::NextResult signal = co_await signals.next();
              EXPECT_THAT(signal.has_value(), IsTrue());
              if (!signal) {
                break;
              }
              consumedBoundary = consumedBoundary ||
                  signal->type ==
                      BaseLoadGenerator::RequestSignal::Type::
                          MeasurementBoundary;
              collected.push_back(*signal);
            }
            co_return collected;
          }());
  generator.stop();

  const auto marker =
      std::find_if(received.begin(), received.end(), [](const auto& signal) {
        return signal.type ==
            BaseLoadGenerator::RequestSignal::Type::MeasurementBoundary;
      });
  ASSERT_THAT(marker, Ne(received.end()));
  EXPECT_THAT(marker->measurementBoundary.windowId, Eq(41));
  EXPECT_THAT(marker->measurementBoundary.measurementEnd, Eq(measurementEnd));
  for (auto signal = received.begin(); signal != marker; ++signal) {
    EXPECT_THAT(signal->plannedArrival, Lt(measurementEnd));
    EXPECT_THAT(signal->sequence, Lt(marker->sequence));
  }
  ASSERT_THAT(std::next(marker), Ne(received.end()));
  EXPECT_THAT(std::next(marker)->plannedArrival, Ge(measurementEnd));
  EXPECT_THAT(std::next(marker)->sequence, Gt(marker->sequence));
  const std::lock_guard<std::mutex> lock(eventMutex);
  ASSERT_THAT(events, SizeIs(1));
  EXPECT_THAT(events.front().boundary.markerSequence, Eq(marker->sequence));
}

TEST(PoissonLoadGeneratorTest, EmptyPathEmitsOneBoundary) {
  std::atomic<size_t> enqueueEvents{0};
  std::unique_ptr<PoissonLoadGenerator> generator =
      std::make_unique<PoissonLoadGenerator>(
          0, [&](const BaseLoadGenerator::MarkerEnqueuedEvent&) {
            ++enqueueEvents;
          });
  generator->start();
  const std::chrono::steady_clock::time_point measurementEnd{
      std::chrono::seconds{1}};
  generator->requestMeasurementBoundary(9, measurementEnd);
  generator->requestMeasurementBoundary(9, measurementEnd);

  std::future<DetailedSignals::NextResult> next =
      std::async(std::launch::async, [&]() {
        DetailedSignals signals = generator->getRequestSignals();
        return folly::coro::blockingWait(signals.next());
      });
  const DetailedSignals::NextResult marker = next.get();
  // The generator runs the callback after it enqueues the marker. Destroying
  // it joins its thread, so the count below includes that callback.
  generator.reset();

  ASSERT_THAT(marker.has_value(), IsTrue());
  EXPECT_THAT(
      marker->type,
      Eq(BaseLoadGenerator::RequestSignal::Type::MeasurementBoundary));
  EXPECT_THAT(marker->measurementBoundary.windowId, Eq(9));
  EXPECT_THAT(enqueueEvents.load(), Eq(1));
}
