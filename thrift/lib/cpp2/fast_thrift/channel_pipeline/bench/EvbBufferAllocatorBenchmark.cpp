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

/**
 * Cost of allocating and releasing EvbBufferAllocator buffers.
 *
 * Each entry reports the time per buffer. A batch allocates 256 buffers on
 * the EventBase thread and then releases all of them.
 *
 * - same_thread_*: the benchmark thread drives its own EventBase, allocates
 *   and releases. The page recycler runs between batches, outside the timed
 *   region.
 * - cross_thread_postback_*: another thread runs the EventBase and allocates
 *   each batch. The benchmark thread releases each buffer by posting it back
 *   to the EventBase together with a KeepAlive, and then waits until every
 *   posted release has run. The allocation, the posts and the wait are timed.
 * - cross_thread_postback_release_only_*: the same, but only the benchmark
 *   thread's posts are timed.
 *
 * Results: devvm21880 (AMD EPYC-Genoa, 192 CPUs), Linux 6.16.1,
 * @fbcode//mode/opt, five runs of the binary built at commit 78a04078. Median
 * time per buffer, with the range from the fastest to the slowest run:
 *
 *   same_thread_256B                           20.3 ns (19.9 to 20.5)
 *   same_thread_8KiB                           19.4 ns (19.3 to 19.7)
 *   cross_thread_postback_256B                225.2 ns (198.6 to 248.4)
 *   cross_thread_postback_8KiB                230.7 ns (205.0 to 248.8)
 *   cross_thread_postback_release_only_256B    62.4 ns (57.4 to 79.9)
 *   cross_thread_postback_release_only_8KiB    58.3 ns (50.2 to 96.4)
 *
 * Every post-back range lies above the same-thread range of the same size.
 * The posts alone cost the releasing thread about three times a whole
 * same-thread allocate and release. With the allocation step and the wait for
 * the EventBase to run one release task per buffer, the cost is 11 to 12 times.
 */

#include <cstddef>
#include <optional>
#include <utility>
#include <vector>

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/ScopedEventBaseThread.h>
#include <folly/portability/Unistd.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/EvbBufferAllocator.h>

namespace apache::thrift::fast_thrift::channel_pipeline {
namespace {

constexpr size_t kBuffersPerBatch = 256;

unsigned sameThread(size_t size, unsigned iterations) {
  folly::BenchmarkSuspender suspender;
  folly::EventBase eventBase;
  EvbBufferAllocator allocator{eventBase};
  std::vector<BytesPtr> buffers;
  buffers.reserve(kBuffersPerBatch);
  suspender.dismiss();

  for (unsigned i = 0; i < iterations; ++i) {
    for (size_t j = 0; j < kBuffersPerBatch; ++j) {
      buffers.push_back(allocator.allocate(size));
    }
    buffers.clear();
    BENCHMARK_SUSPEND {
      // The page recycler reclaims retired pages on every second loop.
      eventBase.loopOnce(EVLOOP_NONBLOCK);
      eventBase.loopOnce(EVLOOP_NONBLOCK);
    }
  }

  suspender.rehire();
  return static_cast<unsigned>(iterations * kBuffersPerBatch);
}

unsigned crossThreadPostBack(
    size_t size, unsigned iterations, bool releaseOnly) {
  folly::BenchmarkSuspender suspender;
  folly::ScopedEventBaseThread eventBaseThread;
  folly::EventBase* const eventBase = eventBaseThread.getEventBase();
  std::optional<EvbBufferAllocator> allocator;
  eventBase->runInEventBaseThreadAndWait(
      [&] { allocator.emplace(*eventBase); });
  std::vector<std::pair<BytesPtr, folly::Executor::KeepAlive<folly::EventBase>>>
      buffers;
  buffers.reserve(kBuffersPerBatch);
  suspender.dismiss();

  for (unsigned i = 0; i < iterations; ++i) {
    if (releaseOnly) {
      suspender.rehire();
    }
    eventBase->runInEventBaseThreadAndWait([&] {
      for (size_t j = 0; j < kBuffersPerBatch; ++j) {
        buffers.emplace_back(
            allocator->allocate(size), folly::getKeepAliveToken(eventBase));
      }
    });
    if (releaseOnly) {
      suspender.dismiss();
    }

    for (auto& [pendingBuffer, pendingKeepAlive] : buffers) {
      eventBase->runInEventBaseThread(
          [buffer = std::move(pendingBuffer),
           keepAlive = std::move(pendingKeepAlive)]() mutable {
            buffer.reset();
          });
    }
    buffers.clear();

    if (releaseOnly) {
      suspender.rehire();
    }
    eventBase->runInEventBaseThreadAndWait([] {});
    if (releaseOnly) {
      suspender.dismiss();
    }
  }

  suspender.rehire();
  return static_cast<unsigned>(iterations * kBuffersPerBatch);
}

BENCHMARK_MULTI(same_thread_256B, iterations) {
  return sameThread(256, iterations);
}

BENCHMARK_MULTI(same_thread_8KiB, iterations) {
  return sameThread(8 << 10, iterations);
}

BENCHMARK_MULTI(cross_thread_postback_256B, iterations) {
  return crossThreadPostBack(256, iterations, false);
}

BENCHMARK_MULTI(cross_thread_postback_8KiB, iterations) {
  return crossThreadPostBack(8 << 10, iterations, false);
}

BENCHMARK_MULTI(cross_thread_postback_release_only_256B, iterations) {
  return crossThreadPostBack(256, iterations, true);
}

BENCHMARK_MULTI(cross_thread_postback_release_only_8KiB, iterations) {
  return crossThreadPostBack(8 << 10, iterations, true);
}

} // namespace
} // namespace apache::thrift::fast_thrift::channel_pipeline

int main(int argc, char** argv) {
  ::alarm(120);
  folly::Init init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
