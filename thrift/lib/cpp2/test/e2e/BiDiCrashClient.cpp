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
 * Standalone client binary for BiDi crash testing.
 *
 * Usage: BiDiCrashClient <port> <num_items>
 *
 * This binary exists as a separate process so we can simulate a hard client
 * crash (SIGKILL) during an active BiDi streaming session. In-process tests
 * cannot do this because killing a thread does not close the TCP socket — the
 * OS only sends RST/FIN when the owning process exits.
 *
 * Behavior:
 *   1. Connects to a BiDi echo server on localhost:<port>.
 *   2. Opens a bidirectional stream via co_echo().
 *   3. Sends <num_items> sink items, but keeps the sink open (does not
 *      complete it). This leaves the server blocked on input.next() waiting
 *      for the next item.
 *   4. Reads all <num_items> echoed responses from the stream to confirm the
 *      server processed them.
 *   5. Prints "ITEMS_SENT\n" to stdout (so the parent test can synchronize).
 *   6. Kills itself with SIGKILL after a brief delay.
 *
 * The SIGKILL bypasses all userspace cleanup (no destructors, no atexit, no
 * ASAN interceptors). The OS kernel closes the TCP socket, sending RST to the
 * server. The parent test (BiDiClientCrashE2ETest) then verifies that the
 * server handler unblocks promptly rather than hanging indefinitely.
 */

#include <csignal>
#include <cstdlib>
#include <thread>

#include <fmt/core.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Collect.h>
#include <folly/coro/Sleep.h>
#include <folly/coro/Task.h>
#include <folly/init/Init.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/ScopedEventBaseThread.h>
#include <thrift/lib/cpp2/async/RocketClientChannel.h>
#include <thrift/lib/cpp2/test/e2e/gen-cpp2/TestBiDiService.h>

using namespace apache::thrift;

folly::coro::Task<void> runClient(
    Client<detail::test::TestBiDiService>& client, int numItems) {
  auto bidi = co_await client.co_echo();

  auto sinkGen = folly::coro::co_invoke(
      [numItems]() -> folly::coro::AsyncGenerator<std::string&&> {
        for (int i = 0; i < numItems; ++i) {
          co_yield fmt::format("item-{}", i);
        }
        // Keep the sink open — don't return, just sleep forever.
        // SIGKILL below will kill us.
        co_await folly::coro::sleep(std::chrono::hours(24));
      });

  auto sinkTask = folly::coro::co_invoke(
      [sink = std::move(bidi.sink),
       sinkGen = std::move(sinkGen)]() mutable -> folly::coro::Task<void> {
        try {
          co_await std::move(sink).sink(std::move(sinkGen));
        } catch (...) {
        }
      });

  auto streamTask = folly::coro::co_invoke(
      [streamGen = std::move(bidi.stream).toAsyncGenerator(),
       numItems]() mutable -> folly::coro::Task<void> {
        for (int i = 0; i < numItems; ++i) {
          auto item = co_await streamGen.next();
          if (!item) {
            fmt::print(stderr, "Stream ended early at item {}\n", i);
            kill(getpid(), SIGKILL);
          }
        }
        // All items echoed back — the server is now blocked on input.next()
        // waiting for the next sink item.
        fmt::print(stdout, "ITEMS_SENT\n");
        std::fflush(stdout);
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        kill(getpid(), SIGKILL);
      });

  co_await folly::coro::collectAll(std::move(sinkTask), std::move(streamTask));
}

int main(int argc, char* argv[]) {
  folly::Init init(&argc, &argv);

  if (argc < 3) {
    fmt::print(stderr, "Usage: {} <port> <num_items>\n", argv[0]);
    return 1;
  }

  const int port = std::atoi(argv[1]);
  const int numItems = std::atoi(argv[2]);

  // Run the EventBase on a separate thread so socket IO is driven
  // independently of the coroutine execution.
  folly::ScopedEventBaseThread evbThread;
  auto* evb = evbThread.getEventBase();

  auto client =
      folly::via(evbThread.getEventBaseKeepAlive(), [evb, port] {
        return std::make_unique<Client<detail::test::TestBiDiService>>(
            RocketClientChannel::newChannel(
                folly::AsyncSocket::newSocket(
                    evb, folly::SocketAddress("::1", port))));
      }).get();

  folly::coro::blockingWait(runClient(*client, numItems));
  return 0;
}
