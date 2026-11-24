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
 * ThriftServerCompositeAppAdapter routing microbenchmark.
 *
 * Isolates the per-request cost the composite adds over a bare adapter:
 * one F14 route lookup + direct invocation of an unbound child thunk.
 * The child's method handler is a no-op that does not touch the pipeline,
 * so each iter measures only the routing decision
 * (no rocket framing, no protocol parsing, no wire I/O).
 *
 * Comparisons:
 *   - BareAdapter_*           : dispatch straight into a ThriftServerAppAdapter
 *                               (no composite). Establishes the per-request
 *                               floor for the adapter machinery itself.
 *   - Composite_OneChild_*    : composite wrapping a single child. Measures
 *                               the additive cost of the composite layer when
 *                               there is no actual fan-out — i.e. the price
 *                               you pay by enabling composite at all.
 *   - Composite_NChildren_*   : composite over 2 / 4 children, hitting the
 *                               first vs the last child. F14 is O(1), so
 *                               first/last should be flat — these are
 *                               regression guards in case routing ever
 *                               grows non-trivial.
 */

#include <folly/Benchmark.h>
#include <folly/CppAttributes.h>
#include <folly/init/Init.h>
#include <folly/io/IOBuf.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/common/ThriftRequestPayloads.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/adapter/ThriftServerAppAdapter.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/adapter/ThriftServerCompositeAppAdapter.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/adapter/util/ThriftServerCompositeRoutingTable.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/adapter/util/ThriftServerMethodDispatchTable.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/Messages.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/util/RequestMetadata.h>
#include <thrift/lib/thrift/gen-cpp2/RpcMetadata_types.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace {

using apache::thrift::fast_thrift::channel_pipeline::erase_and_box;
using apache::thrift::fast_thrift::channel_pipeline::Result;
using apache::thrift::fast_thrift::thrift::makeServerRequestMetadata;
using apache::thrift::fast_thrift::thrift::ThriftRequestContextPtr;
using apache::thrift::fast_thrift::thrift::ThriftServerAppAdapter;
using apache::thrift::fast_thrift::thrift::ThriftServerCompositeAppAdapter;
using apache::thrift::fast_thrift::thrift::ThriftServerCompositeRoutingTable;
using apache::thrift::fast_thrift::thrift::ThriftServerInboundPayloadVariant;
using apache::thrift::fast_thrift::thrift::ThriftServerMethodDispatchTable;
using apache::thrift::fast_thrift::thrift::ThriftServerRequestMessage;
using apache::thrift::fast_thrift::thrift::ThriftServerRequestResponsePayload;
constexpr int kMethodsPerAdapter = 4;

folly::EventBase& requestMetadataEventBase() {
  static folly::EventBase eventBase;
  return eventBase;
}

struct UnusedEndpointContext {};

UnusedEndpointContext& endpointContext() noexcept {
  static UnusedEndpointContext context;
  return context;
}

// Adapter whose shared-table methods are no-op handlers. This isolates routing
// without pipeline writes, protocol decoding, or wire I/O.
class NoOpAdapter : public ThriftServerAppAdapter {
 public:
  using Ptr = std::unique_ptr<NoOpAdapter, Destructor>;

  explicit NoOpAdapter(
      std::shared_ptr<const ThriftServerMethodDispatchTable> dispatchTable)
      : ThriftServerAppAdapter(std::move(dispatchTable)) {}

  void process(
      uint32_t,
      std::unique_ptr<folly::IOBuf>,
      apache::thrift::ProtocolId,
      ThriftRequestContextPtr) noexcept {}
};

template <typename Adapter>
std::shared_ptr<const ThriftServerMethodDispatchTable> makeDispatchTable(
    const std::string& prefix) {
  std::vector<std::string> names;
  std::vector<ThriftServerMethodDispatchTable::Method> methods;
  names.reserve(kMethodsPerAdapter);
  methods.reserve(kMethodsPerAdapter);
  for (int methodIndex = 0; methodIndex < kMethodsPerAdapter; ++methodIndex) {
    names.push_back(prefix + std::to_string(methodIndex));
    methods.push_back(
        ThriftServerAppAdapter::
            makeRequestResponseMethod<Adapter, &Adapter::process>(
                names.back()));
  }
  return std::make_shared<const ThriftServerMethodDispatchTable>(
      std::move(methods));
}

ThriftServerRequestMessage makeRequest(
    std::string_view methodName, uint32_t streamId) {
  auto metadata = std::make_unique<apache::thrift::RequestRpcMetadata>();
  metadata->name().emplace(methodName);
  metadata->kind() = apache::thrift::RpcKind::SINGLE_REQUEST_SINGLE_RESPONSE;
  metadata->protocol() = apache::thrift::ProtocolId::BINARY;

  ThriftServerRequestMessage msg;
  msg.payload =
      ThriftServerInboundPayloadVariant{ThriftServerRequestResponsePayload{
          .data = folly::IOBuf::copyBuffer("payload"),
          .metadata = makeServerRequestMetadata(
              requestMetadataEventBase(), std::move(*metadata))}};
  msg.streamId = streamId;
  return msg;
}

// Build N children, each owning four distinct methods. Returns the composite,
// the child owners (composite borrows; caller keeps them alive), and the
// method names registered for child indices the bench wants to hit.
struct CompositeFixture {
  ThriftServerCompositeAppAdapter::Ptr FOLLY_NONNULL composite;
  std::vector<NoOpAdapter::Ptr> children;
  std::vector<std::string> methodNames; // one method name per child
};

CompositeFixture makeComposite(size_t numChildren) {
  CompositeFixture fixture;
  std::vector<std::shared_ptr<const ThriftServerMethodDispatchTable>>
      childTables;
  fixture.children.reserve(numChildren);
  fixture.methodNames.reserve(numChildren);
  childTables.reserve(numChildren);

  for (size_t childIndex = 0; childIndex < numChildren; ++childIndex) {
    auto prefix = "child" + std::to_string(childIndex) + "_method";
    auto table = makeDispatchTable<NoOpAdapter>(prefix);
    fixture.methodNames.push_back(prefix + "0");
    fixture.children.push_back(NoOpAdapter::Ptr{new NoOpAdapter(table)});
    childTables.push_back(std::move(table));
  }

  fixture.composite =
      ThriftServerCompositeAppAdapter::Ptr{new ThriftServerCompositeAppAdapter(
          ThriftServerCompositeRoutingTable::create(std::move(childTables)))};
  for (const auto& child : fixture.children) {
    fixture.composite->addChild(child.get());
  }
  return fixture;
}

NoOpAdapter::Ptr FOLLY_NONNULL makeBareAdapter() {
  return NoOpAdapter::Ptr{
      new NoOpAdapter(makeDispatchTable<NoOpAdapter>("bare_method"))};
}

// Pre-build `iters` request messages for `methodName`. Each iter consumes
// one (move-into-box semantics) so the loop body stays allocation-free.
std::vector<ThriftServerRequestMessage> prebuildRequests(
    size_t iters, std::string_view methodName) {
  std::vector<ThriftServerRequestMessage> requests;
  requests.reserve(iters);
  for (size_t i = 0; i < iters; ++i) {
    // streamId only needs to be non-zero — composite DCHECKs it but doesn't
    // act on the value otherwise.
    requests.push_back(makeRequest(methodName, /*streamId=*/1));
  }
  return requests;
}

// =============================================================================
// Baseline: dispatch directly into a bare adapter (no composite layer).
// =============================================================================

BENCHMARK(BareAdapter_HitMethod, iters) {
  folly::BenchmarkSuspender suspender;
  auto adapter = makeBareAdapter();
  auto& adapterRef = *adapter;
  auto requests = prebuildRequests(iters, "bare_method0");
  suspender.dismiss();

  for (size_t i = 0; i < iters; ++i) {
    auto result = adapterRef.onRead(
        endpointContext(), erase_and_box(std::move(requests[i])));
    folly::doNotOptimizeAway(result);
  }
}

// =============================================================================
// Composite-of-one: measures composite layer overhead with no fan-out.
// =============================================================================

BENCHMARK_RELATIVE(Composite_OneChild_HitMethod, iters) {
  folly::BenchmarkSuspender suspender;
  auto fixture = makeComposite(/*numChildren=*/1);
  auto& composite = *fixture.composite;
  auto requests = prebuildRequests(iters, fixture.methodNames[0]);
  suspender.dismiss();

  for (size_t i = 0; i < iters; ++i) {
    auto result = composite.onRead(
        endpointContext(), erase_and_box(std::move(requests[i])));
    folly::doNotOptimizeAway(result);
  }
}

BENCHMARK_DRAW_LINE();

// =============================================================================
// Two children: hitting first vs last child should be flat (F14 is O(1)).
// =============================================================================

BENCHMARK(Composite_TwoChildren_HitFirst, iters) {
  folly::BenchmarkSuspender suspender;
  auto fixture = makeComposite(/*numChildren=*/2);
  auto& composite = *fixture.composite;
  auto requests = prebuildRequests(iters, fixture.methodNames[0]);
  suspender.dismiss();

  for (size_t i = 0; i < iters; ++i) {
    auto result = composite.onRead(
        endpointContext(), erase_and_box(std::move(requests[i])));
    folly::doNotOptimizeAway(result);
  }
}

BENCHMARK_RELATIVE(Composite_TwoChildren_HitLast, iters) {
  folly::BenchmarkSuspender suspender;
  auto fixture = makeComposite(/*numChildren=*/2);
  auto& composite = *fixture.composite;
  auto requests = prebuildRequests(iters, fixture.methodNames[1]);
  suspender.dismiss();

  for (size_t i = 0; i < iters; ++i) {
    auto result = composite.onRead(
        endpointContext(), erase_and_box(std::move(requests[i])));
    folly::doNotOptimizeAway(result);
  }
}

BENCHMARK_DRAW_LINE();

// =============================================================================
// Four children: same first/last comparison at higher methodMap fill.
// =============================================================================

BENCHMARK(Composite_FourChildren_HitFirst, iters) {
  folly::BenchmarkSuspender suspender;
  auto fixture = makeComposite(/*numChildren=*/4);
  auto& composite = *fixture.composite;
  auto requests = prebuildRequests(iters, fixture.methodNames[0]);
  suspender.dismiss();

  for (size_t i = 0; i < iters; ++i) {
    auto result = composite.onRead(
        endpointContext(), erase_and_box(std::move(requests[i])));
    folly::doNotOptimizeAway(result);
  }
}

BENCHMARK_RELATIVE(Composite_FourChildren_HitLast, iters) {
  folly::BenchmarkSuspender suspender;
  auto fixture = makeComposite(/*numChildren=*/4);
  auto& composite = *fixture.composite;
  auto requests = prebuildRequests(iters, fixture.methodNames[3]);
  suspender.dismiss();

  for (size_t i = 0; i < iters; ++i) {
    auto result = composite.onRead(
        endpointContext(), erase_and_box(std::move(requests[i])));
    folly::doNotOptimizeAway(result);
  }
}

BENCHMARK_DRAW_LINE();

// =============================================================================
// Larger child counts — confirm F14 lookup stays flat (regression guard).
// =============================================================================

BENCHMARK(Composite_EightChildren_HitLast, iters) {
  folly::BenchmarkSuspender suspender;
  auto fixture = makeComposite(/*numChildren=*/8);
  auto& composite = *fixture.composite;
  auto requests = prebuildRequests(iters, fixture.methodNames[7]);
  suspender.dismiss();

  for (size_t i = 0; i < iters; ++i) {
    auto result = composite.onRead(
        endpointContext(), erase_and_box(std::move(requests[i])));
    folly::doNotOptimizeAway(result);
  }
}

BENCHMARK_RELATIVE(Composite_SixteenChildren_HitLast, iters) {
  folly::BenchmarkSuspender suspender;
  auto fixture = makeComposite(/*numChildren=*/16);
  auto& composite = *fixture.composite;
  auto requests = prebuildRequests(iters, fixture.methodNames[15]);
  suspender.dismiss();

  for (size_t i = 0; i < iters; ++i) {
    auto result = composite.onRead(
        endpointContext(), erase_and_box(std::move(requests[i])));
    folly::doNotOptimizeAway(result);
  }
}

BENCHMARK_DRAW_LINE();

// =============================================================================
// Heterogeneous children — two distinct concrete adapter types share the
// composite and dispatch through resolved methods. Expect parity with the
// homogeneous Composite_TwoChildren benches.
// =============================================================================

// Second adapter type whose runtime shape matches NoOpAdapter but whose
// concrete C++ type is distinct. Request dispatch uses the common adapter
// base, while lifecycle dispatch remains type-erased per concrete type.
class OtherNoOpAdapter : public ThriftServerAppAdapter {
 public:
  using Ptr = std::unique_ptr<OtherNoOpAdapter, Destructor>;

  explicit OtherNoOpAdapter(
      std::shared_ptr<const ThriftServerMethodDispatchTable> dispatchTable)
      : ThriftServerAppAdapter(std::move(dispatchTable)) {}

  void process(
      uint32_t,
      std::unique_ptr<folly::IOBuf>,
      apache::thrift::ProtocolId,
      ThriftRequestContextPtr) noexcept {}
};

struct HeterogeneousFixture {
  ThriftServerCompositeAppAdapter::Ptr FOLLY_NONNULL composite;
  NoOpAdapter::Ptr firstChild;
  OtherNoOpAdapter::Ptr secondChild;
  std::string firstMethod;
  std::string secondMethod;
};

HeterogeneousFixture makeHeterogeneousComposite() {
  HeterogeneousFixture fixture;
  auto firstTable = makeDispatchTable<NoOpAdapter>("first_method");
  auto secondTable = makeDispatchTable<OtherNoOpAdapter>("second_method");
  auto routes =
      ThriftServerCompositeRoutingTable::create({firstTable, secondTable});

  fixture.composite = ThriftServerCompositeAppAdapter::Ptr{
      new ThriftServerCompositeAppAdapter(std::move(routes))};
  fixture.firstChild = NoOpAdapter::Ptr{new NoOpAdapter(firstTable)};
  fixture.secondChild =
      OtherNoOpAdapter::Ptr{new OtherNoOpAdapter(secondTable)};
  fixture.firstMethod = "first_method0";
  fixture.secondMethod = "second_method0";
  fixture.composite->addChild(fixture.firstChild.get());
  fixture.composite->addChild(fixture.secondChild.get());
  return fixture;
}

BENCHMARK(Composite_HeterogeneousChildren_HitFirst, iters) {
  folly::BenchmarkSuspender suspender;
  auto fixture = makeHeterogeneousComposite();
  auto& composite = *fixture.composite;
  auto requests = prebuildRequests(iters, fixture.firstMethod);
  suspender.dismiss();

  for (size_t i = 0; i < iters; ++i) {
    auto result = composite.onRead(
        endpointContext(), erase_and_box(std::move(requests[i])));
    folly::doNotOptimizeAway(result);
  }
}

BENCHMARK_RELATIVE(Composite_HeterogeneousChildren_HitSecond, iters) {
  folly::BenchmarkSuspender suspender;
  auto fixture = makeHeterogeneousComposite();
  auto& composite = *fixture.composite;
  auto requests = prebuildRequests(iters, fixture.secondMethod);
  suspender.dismiss();

  for (size_t i = 0; i < iters; ++i) {
    auto result = composite.onRead(
        endpointContext(), erase_and_box(std::move(requests[i])));
    folly::doNotOptimizeAway(result);
  }
}

BENCHMARK_DRAW_LINE();

// =============================================================================
// Failure path: unknown method → framework-error fire. Establishes a ceiling
// for the failure path so future regressions in serializeResponseRpcError or
// the error fire surface in this microbench.
// =============================================================================

BENCHMARK(Composite_UnknownMethod, iters) {
  folly::BenchmarkSuspender suspender;
  auto fixture = makeComposite(/*numChildren=*/4);
  auto& composite = *fixture.composite;
  // Use a method name no child registered. Cannot share buildPipeline here
  // (bench has no pipeline wiring); composite's writeUnknownMethodError
  // returns Result::Error early when pipeline_ is unset. The hot work
  // exercised here is still the shared routing-table miss probe.
  auto requests = prebuildRequests(iters, "nobody_owns_this");
  suspender.dismiss();

  for (size_t i = 0; i < iters; ++i) {
    auto result = composite.onRead(
        endpointContext(), erase_and_box(std::move(requests[i])));
    folly::doNotOptimizeAway(result);
  }
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
