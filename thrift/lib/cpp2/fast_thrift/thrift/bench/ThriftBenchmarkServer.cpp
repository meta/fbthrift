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
 * Thrift E2E Benchmark Server
 *
 * Serves a BenchmarkService with --rpc_type controlling the server
 * implementation:
 *   - thrift:      Standard apache::thrift::ThriftServer
 *   - fast_thrift: fast_thrift ConnectionManager + ThriftServerChannel pipeline
 *
 * Usage:
 *   buck2 run @//mode/opt-clang-lto \
 *     //thrift/lib/cpp2/fast_thrift/thrift/bench:thrift_benchmark_server \
 *     -- --port=5001 --rpc_type=thrift
 */

#include <gflags/gflags.h>
#include <folly/init/Init.h>
#include <folly/logging/xlog.h>

#include <thrift/lib/cpp2/server/ThriftServer.h>

#include <thrift/lib/cpp2/fast_thrift/thrift/bench/if/gen-cpp2/BenchmarkService.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/FastThriftChannelServer.h>

DEFINE_int32(port, 0, "Port to listen on (0 = system assigned)");
DEFINE_int32(io_threads, 8, "Number of IO threads");
DEFINE_int32(
    connection_setup_threads,
    0,
    "Number of dedicated fast_thrift accept and TLS setup threads");
DEFINE_string(
    rpc_type,
    "thrift",
    "Server implementation: thrift (ThriftServer) or fast_thrift (pipeline)");

using namespace apache::thrift::fast_thrift;
using namespace apache::thrift::fast_thrift::thrift::bench;

namespace {

// =============================================================================
// Service Handler
// =============================================================================

class BenchmarkServiceHandler
    : public apache::thrift::ServiceHandler<BenchmarkService> {
 public:
  void ping() override {}

  void echo(
      std::string& response, std::unique_ptr<std::string> message) override {
    response = std::move(*message);
  }

  void sendResponse(std::string& response, int64_t size) override {
    response = std::string(static_cast<size_t>(size), 'x');
  }
};

// =============================================================================
// Standard Thrift Server
// =============================================================================

std::shared_ptr<apache::thrift::ThriftServer> createThriftServer(
    std::shared_ptr<BenchmarkServiceHandler> handler,
    uint16_t port,
    uint32_t numIOThreads) {
  auto server = std::make_shared<apache::thrift::ThriftServer>();
  server->setInterface(std::move(handler));
  server->setPort(port);
  server->setNumIOWorkerThreads(numIOThreads);
  return server;
}

} // namespace

int main(int argc, char* argv[]) {
  folly::Init init(&argc, &argv);

  auto handler = std::make_shared<BenchmarkServiceHandler>();
  auto port = static_cast<uint16_t>(FLAGS_port);

  if (FLAGS_rpc_type == "thrift") {
    auto server = createThriftServer(handler, port, FLAGS_io_threads);
    XLOG(INFO) << "Starting ThriftServer on port " << FLAGS_port;
    server->serve();
  } else if (FLAGS_rpc_type == "fast_thrift") {
    thrift::FastThriftServerConfig config;
    config.address.setFromLocalPort(port);
    config.numIOThreads = FLAGS_io_threads;
    config.numConnectionSetupThreads = FLAGS_connection_setup_threads;
    thrift::FastThriftChannelServer server(
        std::move(config), std::move(handler));
    server.serve();
  } else {
    XLOG(FATAL) << "Unknown rpc_type: " << FLAGS_rpc_type
                << " (expected: thrift, fast_thrift)";
  }

  return 0;
}
