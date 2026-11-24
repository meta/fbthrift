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

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>

#include <folly/SocketAddress.h>

#include <thrift/lib/cpp2/fast_thrift/frame/write/IntervalBatchingHandlerConfig.h>

namespace apache::thrift::fast_thrift::thrift {

struct RequestConcurrencyLimits {
  uint64_t maxActiveRequests;
  uint64_t maxOutstandingRequests;
};

enum class ChannelPipelineMode : uint8_t {
  Dynamic,
  Static,
};

/**
 * Shared configuration for FastThriftServer / FastThriftChannelServer.
 */
struct FastThriftServerConfig {
  // Address to bind to.
  folly::SocketAddress address;

  // Number of data IO threads. Each thread owns established connections and,
  // unless a setup pool is configured below, accepts via SO_REUSEPORT.
  uint32_t numIOThreads{1};

  // Number of dedicated connection-accept threads. These EventBases own the
  // listening sockets and dispatch accepted fds through bounded queues to the
  // data IO threads, where TLS and connection setup run. Zero keeps socket
  // acceptance colocated with connection IO.
  uint32_t numConnectionSetupThreads{0};

  // Size of the CPU thread pool used by the generated service path.
  // Zero keeps the pre-existing behavior: request deserialization, the
  // handler call, and response serialization all run inline on the IO thread
  // that owns the connection, so a handler that blocks stalls every other
  // connection on that thread.
  //
  // The server-owned pool uses a striped queue; size it to the available core
  // count for balanced LLC utilization. Ignored when an executor is supplied
  // via FastThriftServer::setCPUExecutor.
  // Auxiliary monitoring / status / debug interfaces dispatch to it too,
  // except for methods pinned with @cpp.ProcessInEbThreadUnsafe — see
  // setCPUExecutor.
  uint32_t numCPUThreads{0};

  // Minimum payload size in bytes for MSG_ZEROCOPY. 0 disables zero-copy.
  size_t zeroCopyThreshold{0};

  // Use AlignedParser for inbound frames. See AlignedParser for its buffer
  // guarantees and preconditions. The server rejects a direct native io_uring
  // connection because that receive path requires a movable read callback.
  // Fizz connections are accepted because Fizz copies decrypted bytes into
  // the buffers that AlignedParser provides.
  // Only FastThriftServer reads this field.
  bool useAlignedParser{false};

  // When true, auto-mount the ThriftMetadataService alongside the user
  // handler so introspection tools (e.g. Thrift Fiddle) can discover the
  // service schema. The response is built once at start() and served on
  // every getThriftServiceMetadata() RPC. For the response to be non-empty,
  // the underlying thrift_library must be built with `with_schema = True`.
  bool enableMetadataService{false};

  // When true, populate each request's ThriftRequestContext with the inbound
  // custom headers (RequestRpcMetadata.otherMetadata) so handlers can read
  // them via getHeaders()/getHeader(). The context is the only place headers
  // are readable, so an extension declaring kUsesHeaders is refused at
  // addModule while this is off. Only takes effect on FastThriftServer.
  bool enableRequestHeaders{false};

  // When true, insert ThriftServerChecksumHandler to validate the inbound
  // request checksum and echo a matching checksum on the response. The
  // response algorithm is carried on the per-request ThriftRequestContext.
  bool enableChecksum{false};

  // When true, propagate client cancellation into the running service
  // handler. Enabling this installs per-request lifecycle arbitration and
  // materializes a cancellable token on each request context. When false,
  // CANCEL is ignored: application work may finish and its response is sent.
  // Only takes effect on FastThriftServer.
  bool enableCancellation{false};

  // Shared per-EventBase admission limits.
  std::optional<RequestConcurrencyLimits> requestConcurrencyLimits;

  // When true, insert ThriftServerStreamMuxHandler immediately head-ward of the
  // tail app adapter. It owns and multiplexes per-stream producing-end
  // sub-pipelines: outbound it converts the app's stream-open into a registered
  // sub-pipeline and emits stream chunks; inbound it routes RequestN/Cancel to
  // the matching sub-pipeline. Unary traffic passes through untouched, and the
  // handler needs no per-request context, so it is independent of the other
  // flags. Default off while server streaming is still being built out.
  bool enableStreamMux{false};

  // When true, the server owns its counters: it materializes the message,
  // connection and TLS stats at start() and wires the corresponding metrics
  // handlers into every pipeline. Reach them afterwards via getStats() /
  // getConnectionStats() / getTLSStats(), or let a
  // FastThriftStatsRegistryPublisher discover them and publish to fb303.
  //
  // Per layer, an instance supplied via the setStats family wins — this flag
  // only fills the slots the embedder left empty. Only takes effect on
  // FastThriftServer; FastThriftChannelServer selects stats at compile time
  // through its Stats template parameter.
  bool enableStats{false};

  // When true, materialize connection counters and allow the fb303 publisher
  // to export one accepted/active/rejected series per IO EventBase. This is
  // independent of enableStats so embedders can inspect connection placement
  // without installing the message- and TLS-layer metrics handlers.
  bool enablePerEventBaseConnectionStats{false};

  // When true, FastThriftStatsRegistryPublisher samples the EvbAllocator on
  // each IO EventBase and exports its memory gauges to fb303. Collection is
  // off the allocation path and does not create allocators that do not exist.
  bool enableEvbAllocatorStats{false};

  // Outbound write batching. Default zero-interval flushes via LoopCallback
  // at end of each event loop iteration. Set batchingInterval > 0 to use
  // an HHWheelTimer-driven flush instead.
  frame::write::IntervalBatchingHandlerConfig batchingConfig{};

  // Per-connection terminal-phase deadlines. Consumed by
  // ThriftServerConnectionCloseHandler; defaults mirror its
  // kDefaultDrainTimeout / kDefaultReapTimeout.
  //
  //   drainTimeout — after a CloseConnection event, how long to wait for
  //     in-flight responses to flush before forcing the socket down.
  //   reapTimeout — after the socket is gone, how long to wait for
  //     in-flight handler callbacks to return before LOG(FATAL)ing.
  //     A handler that hasn't completed within this window is stuck and
  //     would otherwise silently leak the per-connection adapter.
  std::chrono::milliseconds drainTimeout{std::chrono::seconds{30}};
  std::chrono::milliseconds reapTimeout{std::chrono::seconds{60}};
};

} // namespace apache::thrift::fast_thrift::thrift
