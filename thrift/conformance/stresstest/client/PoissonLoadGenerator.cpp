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

#include <thrift/conformance/stresstest/client/PoissonLoadGenerator.h>

#include <algorithm>

#include <folly/portability/Asm.h>

namespace apache::thrift::stress {

namespace {
// Shortest gap we are willing to sleep through. Longer gaps are slept,
// shorter ones are spun.
//
// sleep_until wakes up late: measured on a devserver, 55 us at the median and
// 83 us at p99, almost regardless of the duration asked for. Most of that is a
// fixed offset, and a fixed offset cancels between one arrival and the next,
// so a gap is perturbed only by the spread around it.
//
// Replaying this loop at several thresholds gives the table below. "clumped"
// is the share of gaps that came out under 10 us, and "ideal" is what a true
// Poisson process produces at that rate. The difference between those two
// columns is clumping the generator invented.
//
//   qps/thread  threshold    spin   clumped   ideal
//          500       0 us    0.0%      2.9%    0.5%
//          500     150 us    0.3%      3.0%    0.5%
//          500     250 us    0.8%      3.0%    0.5%
//         4000     150 us   11.5%     14.1%    3.9%
//         4000     250 us   25.4%     11.1%    3.9%
//        10000     150 us   40.1%     19.8%    9.5%
//        10000     250 us   67.3%     14.0%    9.5%
//
// The suites run at a few hundred qps per thread, and there the threshold
// changes nothing and only costs cpu, so it is set low. Above a few thousand
// qps per thread no threshold gets near ideal without burning most of a core,
// and the answer there is more client threads rather than more spinning.
constexpr std::chrono::microseconds kMinSleepGap{150};

// Longest a single sleep may run. Bounds how long the destructor waits for
// the timing thread to notice that it should stop.
constexpr std::chrono::milliseconds kMaxSleepSlice{20};
} // namespace

PoissonLoadGenerator::PoissonLoadGenerator(
    const double targetQps,
    MarkerEnqueuedCallback markerEnqueuedCallback,
    std::chrono::steady_clock::time_point (*const now)())
    : targetQps_(targetQps),
      markerEnqueuedCallback_(std::move(markerEnqueuedCallback)),
      now_(now) {}

folly::coro::AsyncGenerator<PoissonLoadGenerator::Count>
PoissonLoadGenerator::getRequestCount() {
  folly::coro::AsyncGenerator<RequestSignal> signals = getRequestSignals();
  while (const folly::coro::AsyncGenerator<RequestSignal>::NextResult signal =
             co_await signals.next()) {
    if (signal->type == RequestSignal::Type::Request) {
      co_yield signal->count;
    }
  }
}

folly::coro::AsyncGenerator<PoissonLoadGenerator::RequestSignal>
PoissonLoadGenerator::getRequestSignals() {
  while (true) {
    const RequestSignal request = co_await queue_.dequeue();
    if (!running_.load(std::memory_order_relaxed)) {
      co_return;
    }
    co_yield request;
  }
}

void PoissonLoadGenerator::generateRequestSignals() {
  // An empty request path still stays alive to emit its measurement boundary.
  std::optional<std::exponential_distribution<double>> gaps;
  if (targetQps_ > 0.0) {
    gaps.emplace(targetQps_);
  }

  // Arrival times accumulate rather than restart from now(), so a late wakeup
  // is paid back on the following gaps and the long-run rate still holds.
  std::chrono::steady_clock::time_point nextArrival = now_();
  if (gaps) {
    nextArrival +=
        std::chrono::duration_cast<std::chrono::steady_clock::duration>(
            std::chrono::duration<double>((*gaps)(gen_)));
  }
  while (running_.load(std::memory_order_relaxed)) {
    if (emitRequestedBoundary(nextArrival, gaps ? &*gaps : nullptr)) {
      continue;
    }

    if (!gaps) {
      std::unique_lock lock(controlMutex_);
      controlCondition_.wait(lock, [this] {
        return !running_.load(std::memory_order_relaxed) ||
            requestedBoundary_.has_value();
      });
      continue;
    }

    // Sleeping is done in slices. sleep_until cannot be woken, so a single
    // sleep over a long gap would hold up the destructor for that whole gap;
    // slices cap teardown at kMaxSleepSlice instead.
    while (running_.load(std::memory_order_relaxed)) {
      const std::chrono::steady_clock::time_point now = now_();
      if (now + kMinSleepGap >= nextArrival) {
        break;
      }
      std::unique_lock lock(controlMutex_);
      // This wait keeps stop latency bounded and wakes for a boundary request.
      const bool boundaryRequested = controlCondition_.wait_until(
          lock, std::min(nextArrival, now + kMaxSleepSlice), [this] {
            return !running_.load(std::memory_order_relaxed) ||
                requestedBoundary_.has_value();
          });
      if (boundaryRequested) {
        break;
      }
    }

    if (emitRequestedBoundary(nextArrival, &*gaps)) {
      continue;
    }

    // The last stretch is too short to sleep on: the wakeup lands tens of
    // microseconds late, which over a gap this short is most of the gap, and
    // the arrival would miss its slot. Burning the time keeps it on time.
    while (running_.load(std::memory_order_relaxed) && now_() < nextArrival) {
      if (hasRequestedBoundary()) {
        break;
      }
      // Pause rather than yield. Yielding hands the core away and getting it
      // back can cost more than the gap we are waiting out, which defeats the
      // point of spinning.
      folly::asm_volatile_pause();
    }

    if (emitRequestedBoundary(nextArrival, &*gaps)) {
      continue;
    }

    if (!running_.load(std::memory_order_relaxed)) {
      break;
    }
    enqueueRequest(nextArrival);
    nextArrival +=
        std::chrono::duration_cast<std::chrono::steady_clock::duration>(
            std::chrono::duration<double>((*gaps)(gen_)));
  }
}

bool PoissonLoadGenerator::hasRequestedBoundary() {
  const std::lock_guard lock(controlMutex_);
  return requestedBoundary_.has_value();
}

bool PoissonLoadGenerator::emitRequestedBoundary(
    std::chrono::steady_clock::time_point& nextArrival,
    std::exponential_distribution<double>* const gaps) {
  std::optional<MeasurementBoundary> boundary;
  MarkerEnqueuedCallback callback;
  {
    std::lock_guard lock(controlMutex_);
    if (!requestedBoundary_) {
      return false;
    }
    boundary = requestedBoundary_;
    requestedBoundary_.reset();
    emittedBoundaryWindow_ = boundary->windowId;
    callback = markerEnqueuedCallback_;
  }

  while (gaps != nullptr && nextArrival < boundary->measurementEnd) {
    enqueueRequest(nextArrival);
    nextArrival +=
        std::chrono::duration_cast<std::chrono::steady_clock::duration>(
            std::chrono::duration<double>((*gaps)(gen_)));
  }

  boundary->markerSequence = nextSequence_++;
  queue_.enqueue(
      RequestSignal{
          .type = RequestSignal::Type::MeasurementBoundary,
          .count = 0,
          .plannedArrival = boundary->measurementEnd,
          .sequence = boundary->markerSequence,
          .measurementBoundary = *boundary,
      });
  const std::chrono::steady_clock::time_point enqueueTimestamp = now_();
  if (callback) {
    callback(
        MarkerEnqueuedEvent{
            .boundary = *boundary,
            .producerEnqueueTimestamp = enqueueTimestamp,
        });
  }
  return true;
}

void PoissonLoadGenerator::enqueueRequest(
    const std::chrono::steady_clock::time_point plannedArrival) {
  const uint64_t sequence = nextSequence_++;
  queue_.enqueue(
      RequestSignal{
          .type = RequestSignal::Type::Request,
          .count = 1,
          .plannedArrival = plannedArrival,
          .sequence = sequence,
          .measurementBoundary = {},
      });
}

void PoissonLoadGenerator::requestMeasurementBoundary(
    const uint64_t windowId,
    const std::chrono::steady_clock::time_point measurementEnd) {
  {
    std::lock_guard lock(controlMutex_);
    if (requestedBoundary_ || emittedBoundaryWindow_ == windowId) {
      return;
    }
    requestedBoundary_ = MeasurementBoundary{
        .windowId = windowId,
        .measurementEnd = measurementEnd,
    };
  }
  controlCondition_.notify_one();
}

void PoissonLoadGenerator::start() {
  bool f = false;
  if (started_.compare_exchange_strong(f, true)) {
    thread_ = std::thread([this] { generateRequestSignals(); });
  }
}

void PoissonLoadGenerator::stop() {
  if (running_.exchange(false, std::memory_order_relaxed)) {
    queue_.enqueue(RequestSignal{});
    controlCondition_.notify_one();
  }
}

PoissonLoadGenerator::~PoissonLoadGenerator() {
  stop();
  if (thread_.joinable()) {
    thread_.join();
  }
}

} // namespace apache::thrift::stress
