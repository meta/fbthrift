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

#pragma once

#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <optional>
#include <random>
#include <thread>
#include <folly/coro/SmallUnboundedQueue.h>
#include <thrift/conformance/stresstest/client/BaseLoadGenerator.h>

namespace apache::thrift::stress {

/**
 * Offers load as a Poisson process: each signal is one request, and the gaps
 * between them are drawn from an exponential distribution.
 *
 * targetQps is fractional because callers split an aggregate rate across
 * client threads, so a single generator often runs well below one request per
 * millisecond.
 *
 * Timing runs on a thread of its own and the signals reach the caller through
 * a queue. It has to stay that way: the last stretch before each arrival is
 * spun rather than slept, and spinning on the caller's EventBase would stall
 * the requests being measured.
 *
 * requestMeasurementBoundary() makes the timing thread queue every arrival
 * planned before measurementEnd at once, even if that time is still ahead.
 * Then it queues the boundary. An arrival can already be in the queue when the
 * timing thread takes the boundary request. That arrival stays ahead of the
 * boundary, even if it is planned after measurementEnd. After the boundary,
 * the timing thread calls markerEnqueuedCallback and continues normal request
 * generation. A boundary request is ignored while another one is pending, or
 * when its window already got a boundary.
 */
class PoissonLoadGenerator : public BaseLoadGenerator {
 public:
  // now returns the time that the generator plans arrivals on. The timed waits
  // use steady_clock deadlines, so another time source must stay behind
  // steady_clock::now(). The waits then return at once, and that time source
  // alone paces the generator.
  explicit PoissonLoadGenerator(
      double targetQps,
      MarkerEnqueuedCallback markerEnqueuedCallback = {},
      std::chrono::steady_clock::time_point (*now)() =
          &std::chrono::steady_clock::now);

  ~PoissonLoadGenerator() override;

  folly::coro::AsyncGenerator<Count> getRequestCount() override;
  folly::coro::AsyncGenerator<RequestSignal> getRequestSignals() override;
  void requestMeasurementBoundary(
      uint64_t windowId,
      std::chrono::steady_clock::time_point measurementEnd) override;
  void start() override;
  void stop() override;

 private:
  const double targetQps_;
  std::atomic<bool> running_{true};
  std::atomic<bool> started_{false};
  folly::coro::SmallUnboundedQueue<RequestSignal> queue_;
  std::mt19937_64 gen_{std::random_device()()};
  std::thread thread_;
  std::mutex controlMutex_;
  std::condition_variable controlCondition_;
  std::optional<MeasurementBoundary> requestedBoundary_;
  std::optional<uint64_t> emittedBoundaryWindow_;
  MarkerEnqueuedCallback markerEnqueuedCallback_;
  std::chrono::steady_clock::time_point (*const now_)();
  uint64_t nextSequence_{0};

  void generateRequestSignals();
  bool emitRequestedBoundary(
      std::chrono::steady_clock::time_point& nextArrival,
      std::exponential_distribution<double>* gaps);
  bool hasRequestedBoundary();
  void enqueueRequest(std::chrono::steady_clock::time_point plannedArrival);
};

} // namespace apache::thrift::stress
