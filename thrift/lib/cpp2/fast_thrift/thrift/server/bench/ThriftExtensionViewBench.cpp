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

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

#include <boost/intrusive_ptr.hpp>

#include <folly/Benchmark.h>
#include <folly/CPortability.h>
#include <folly/CppAttributes.h>
#include <folly/init/Init.h>
#include <folly/io/async/EventBase.h>

#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/Messages.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/MethodMetadata.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/ThriftConnContext.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/ThriftRequestContext.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/compatibility/context/NativeThriftContextView.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/extension/ThriftExtension.h>

namespace apache::thrift::fast_thrift::thrift::server::view_benchmark {
namespace {

using NativeRequestView = compatibility::ThriftRequestContextView<
    compatibility::ThriftContextBackend::Native>;
using HeaderMap = ThriftRequestContext::HeaderMap;

struct RequestHeaderAccess {
  const std::string* FOLLY_NULLABLE requestHeader;
  HeaderMap* responseHeaders;
};

class Fixture {
 public:
  Fixture() {
    metadata.qualifiedMethodName = "Service.method";
    auto connection =
        boost::intrusive_ptr<ThriftConnContext>(new ThriftConnContext());
    requests[0].requestContext = makeContext(connection);
    responses[0].requestContext = makeContext(connection);
    requests[0].streamId = 1;
    requests[1].streamId = 3;
  }

  folly::EventBase eventBase;
  ThriftServerMethodMetadata metadata;
  std::array<ThriftServerRequestMessage, 2> requests;
  std::array<ThriftServerResponseMessage, 2> responses;
  std::string key{"key"};
  std::array<std::string, 2> values{"a", "b"};

 private:
  ThriftRequestContextPtr makeContext(
      const boost::intrusive_ptr<ThriftConnContext>& connection) {
    auto context = makeThriftRequestContext(eventBase);
    context->setConnectionContext(connection);
    context->setMethodName("method");
    context->setMethodMetadata(&metadata);
    context->setHeader(key, values[0]);
    context->setResponseHeader(key, values[0]);
    return context;
  }
};

uint32_t rawStreamIdInline(const ThriftServerRequestMessage& request) noexcept {
  return request.streamId;
}

uint32_t viewStreamIdInline(
    const ThriftServerRequestMessage& request) noexcept {
  const ThriftRequestView view(request);
  return view.streamId();
}

FOLLY_NOINLINE uint32_t
rawStreamIdOpaque(const ThriftServerRequestMessage& request) noexcept {
  const auto* requestPtr = &request;
  folly::makeUnpredictable(requestPtr);

  return requestPtr->streamId;
}

FOLLY_NOINLINE uint32_t
streamIdViewCallback(const ThriftRequestView& request) noexcept {
  const auto* requestPtr = &request;
  folly::makeUnpredictable(requestPtr);

  return requestPtr->streamId();
}

uint32_t viewStreamIdOpaque(
    const ThriftServerRequestMessage& request) noexcept {
  const ThriftRequestView view(request);
  return streamIdViewCallback(view);
}

RequestHeaderAccess rawRequestHeadersInline(
    ThriftServerRequestMessage& request, const std::string& key) noexcept {
  auto* context = request.requestContext.get();
  return {
      context ? context->getHeader(key) : nullptr,
      context ? &context->mutableResponseHeaders() : nullptr};
}

RequestHeaderAccess viewRequestHeadersInline(
    ThriftServerRequestMessage& request, const std::string& key) noexcept {
  ThriftRequestMutator view(request);
  const auto headers = view.mutableResponseHeaders();
  return {view.header(key), headers ? &headers->get() : nullptr};
}

FOLLY_NOINLINE RequestHeaderAccess rawRequestHeadersOpaque(
    ThriftServerRequestMessage& request, const std::string& key) noexcept {
  auto* requestPtr = &request;
  const auto* keyPtr = &key;
  folly::makeUnpredictable(requestPtr);
  folly::makeUnpredictable(keyPtr);

  return rawRequestHeadersInline(*requestPtr, *keyPtr);
}

FOLLY_NOINLINE RequestHeaderAccess requestHeadersViewCallback(
    ThriftRequestMutator& request, const std::string& key) noexcept {
  auto* requestPtr = &request;
  const auto* keyPtr = &key;
  folly::makeUnpredictable(requestPtr);
  folly::makeUnpredictable(keyPtr);

  const auto headers = requestPtr->mutableResponseHeaders();
  return {requestPtr->header(*keyPtr), headers ? &headers->get() : nullptr};
}

RequestHeaderAccess viewRequestHeadersOpaque(
    ThriftServerRequestMessage& request, const std::string& key) noexcept {
  ThriftRequestMutator view(request);
  return requestHeadersViewCallback(view, key);
}

void rawResponseHeaderSetter(
    ThriftServerResponseMessage& response,
    std::string key,
    std::string value) noexcept {
  if (auto* context = response.requestContext.get()) {
    context->mutableResponseHeaders()[std::move(key)] = std::move(value);
  }
}

const std::string* FOLLY_NULLABLE rawResponseHeadersInline(
    ThriftServerResponseMessage& response,
    const std::string& key,
    const std::string& value) noexcept {
  const auto* context = response.requestContext.get();
  const auto* previous = context ? context->getResponseHeader(key) : nullptr;
  rawResponseHeaderSetter(response, key, value);
  return previous;
}

const std::string* FOLLY_NULLABLE viewResponseHeadersInline(
    ThriftServerResponseMessage& response,
    const std::string& key,
    const std::string& value) noexcept {
  ThriftResponseMutator view(response);
  const auto* previous = view.header(key);
  view.setHeader(key, value);
  return previous;
}

FOLLY_NOINLINE const std::string* FOLLY_NULLABLE rawResponseHeadersOpaque(
    ThriftServerResponseMessage& response,
    const std::string& key,
    const std::string& value) noexcept {
  auto* responsePtr = &response;
  const auto* keyPtr = &key;
  const auto* valuePtr = &value;
  folly::makeUnpredictable(responsePtr);
  folly::makeUnpredictable(keyPtr);
  folly::makeUnpredictable(valuePtr);

  return rawResponseHeadersInline(*responsePtr, *keyPtr, *valuePtr);
}

FOLLY_NOINLINE const std::string* FOLLY_NULLABLE responseHeadersViewCallback(
    ThriftResponseMutator& response,
    const std::string& key,
    const std::string& value) noexcept {
  auto* responsePtr = &response;
  const auto* keyPtr = &key;
  const auto* valuePtr = &value;
  folly::makeUnpredictable(responsePtr);
  folly::makeUnpredictable(keyPtr);
  folly::makeUnpredictable(valuePtr);

  const auto* previous = responsePtr->header(*keyPtr);
  responsePtr->setHeader(*keyPtr, *valuePtr);
  return previous;
}

const std::string* FOLLY_NULLABLE viewResponseHeadersOpaque(
    ThriftServerResponseMessage& response,
    const std::string& key,
    const std::string& value) noexcept {
  ThriftResponseMutator view(response);
  return responseHeadersViewCallback(view, key, value);
}

std::string_view rawCompatMethodNameInline(
    const ThriftServerRequestMessage& request) noexcept {
  if (const auto* context = request.requestContext.get()) {
    if (const auto* metadata = context->getMethodMetadata()) {
      return metadata->qualifiedMethodName;
    }
    if (!context->getMethodName().empty()) {
      return context->getMethodName();
    }
  }
  const auto* metadata = request.payload.getRequestRpcMetadata();
  return metadata && metadata->name().has_value() ? metadata->name()->view()
                                                  : std::string_view{};
}

std::string_view compatMethodNameInline(
    const ThriftServerRequestMessage& request) noexcept {
  const ThriftRequestView extension(request);
  const NativeRequestView view(extension);
  return view.methodName();
}

FOLLY_NOINLINE std::string_view rawCompatMethodNameOpaque(
    const ThriftServerRequestMessage& request) noexcept {
  const auto* requestPtr = &request;
  folly::makeUnpredictable(requestPtr);

  return rawCompatMethodNameInline(*requestPtr);
}

FOLLY_NOINLINE std::string_view compatMethodNameCallback(
    const NativeRequestView& request) noexcept {
  const auto* requestPtr = &request;
  folly::makeUnpredictable(requestPtr);

  return requestPtr->methodName();
}

std::string_view compatMethodNameOpaque(
    const ThriftServerRequestMessage& request) noexcept {
  const ThriftRequestView extension(request);
  const NativeRequestView view(extension);
  return compatMethodNameCallback(view);
}

const HeaderMap* FOLLY_NULLABLE
rawCompatHeadersInline(const ThriftServerRequestMessage& request) noexcept {
  const auto* context = request.requestContext.get();
  return context ? &context->getHeaders() : nullptr;
}

const HeaderMap* FOLLY_NULLABLE
compatHeadersInline(const ThriftServerRequestMessage& request) noexcept {
  const ThriftRequestView extension(request);
  const NativeRequestView view(extension);
  return view.requestHeaders();
}

FOLLY_NOINLINE const HeaderMap* FOLLY_NULLABLE
rawCompatHeadersOpaque(const ThriftServerRequestMessage& request) noexcept {
  const auto* requestPtr = &request;
  folly::makeUnpredictable(requestPtr);

  return rawCompatHeadersInline(*requestPtr);
}

FOLLY_NOINLINE const HeaderMap* FOLLY_NULLABLE
compatHeadersCallback(const NativeRequestView& request) noexcept {
  const auto* requestPtr = &request;
  folly::makeUnpredictable(requestPtr);

  return requestPtr->requestHeaders();
}

const HeaderMap* FOLLY_NULLABLE
compatHeadersOpaque(const ThriftServerRequestMessage& request) noexcept {
  const ThriftRequestView extension(request);
  const NativeRequestView view(extension);
  return compatHeadersCallback(view);
}

const folly::SocketAddress* FOLLY_NULLABLE
rawCompatConnectionInline(const ThriftServerRequestMessage& request) noexcept {
  const auto* context = request.requestContext.get();
  const auto* connection = context ? context->getConnectionContext() : nullptr;
  return connection ? &connection->getPeerAddress() : nullptr;
}

const folly::SocketAddress* FOLLY_NULLABLE
compatConnectionInline(const ThriftServerRequestMessage& request) noexcept {
  const ThriftRequestView extension(request);
  const NativeRequestView view(extension);
  return view.connectionContext().peerAddress();
}

FOLLY_NOINLINE const folly::SocketAddress* FOLLY_NULLABLE
rawCompatConnectionOpaque(const ThriftServerRequestMessage& request) noexcept {
  const auto* requestPtr = &request;
  folly::makeUnpredictable(requestPtr);

  return rawCompatConnectionInline(*requestPtr);
}

FOLLY_NOINLINE const folly::SocketAddress* FOLLY_NULLABLE
compatConnectionCallback(const NativeRequestView& request) noexcept {
  const auto* requestPtr = &request;
  folly::makeUnpredictable(requestPtr);

  return requestPtr->connectionContext().peerAddress();
}

const folly::SocketAddress* FOLLY_NULLABLE
compatConnectionOpaque(const ThriftServerRequestMessage& request) noexcept {
  const ThriftRequestView extension(request);
  const NativeRequestView view(extension);
  return compatConnectionCallback(view);
}

template <auto Access>
void runRequestBenchmark(std::size_t iters) {
  folly::BenchmarkSuspender suspender;
  Fixture fixture;
  suspender.dismiss();
  for (std::size_t i = 0; i < iters; ++i) {
    auto* request = &fixture.requests[i & 1];
    folly::makeUnpredictable(request);
    if constexpr (requires { Access(*request, fixture.key); }) {
      const auto result = Access(*request, fixture.key);
      folly::doNotOptimizeAway(result);
    } else {
      const auto result = Access(*request);
      folly::doNotOptimizeAway(result);
    }
  }
  suspender.rehire();
}

template <auto Access>
void runResponseBenchmark(std::size_t iters) {
  folly::BenchmarkSuspender suspender;
  Fixture fixture;
  suspender.dismiss();
  for (std::size_t i = 0; i < iters; ++i) {
    auto* response = &fixture.responses[i & 1];
    folly::makeUnpredictable(response);
    const auto result =
        Access(*response, fixture.key, fixture.values[(i >> 1) & 1]);
    folly::doNotOptimizeAway(result);
  }
  suspender.rehire();
}

BENCHMARK(StreamId_RawInline, iters) {
  runRequestBenchmark<rawStreamIdInline>(iters);
}
BENCHMARK_RELATIVE(StreamId_ViewInline, iters) {
  runRequestBenchmark<viewStreamIdInline>(iters);
}
BENCHMARK(StreamId_RawOpaque, iters) {
  runRequestBenchmark<rawStreamIdOpaque>(iters);
}
BENCHMARK_RELATIVE(StreamId_ViewOpaque, iters) {
  runRequestBenchmark<viewStreamIdOpaque>(iters);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(RequestMutator_RawInline, iters) {
  runRequestBenchmark<rawRequestHeadersInline>(iters);
}
BENCHMARK_RELATIVE(RequestMutator_ViewInline, iters) {
  runRequestBenchmark<viewRequestHeadersInline>(iters);
}
BENCHMARK(RequestMutator_RawOpaque, iters) {
  runRequestBenchmark<rawRequestHeadersOpaque>(iters);
}
BENCHMARK_RELATIVE(RequestMutator_ViewOpaque, iters) {
  runRequestBenchmark<viewRequestHeadersOpaque>(iters);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(ResponseMutator_RawInline, iters) {
  runResponseBenchmark<rawResponseHeadersInline>(iters);
}
BENCHMARK_RELATIVE(ResponseMutator_ViewInline, iters) {
  runResponseBenchmark<viewResponseHeadersInline>(iters);
}
BENCHMARK(ResponseMutator_RawOpaque, iters) {
  runResponseBenchmark<rawResponseHeadersOpaque>(iters);
}
BENCHMARK_RELATIVE(ResponseMutator_ViewOpaque, iters) {
  runResponseBenchmark<viewResponseHeadersOpaque>(iters);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(CompatMethodName_RawInline, iters) {
  runRequestBenchmark<rawCompatMethodNameInline>(iters);
}
BENCHMARK_RELATIVE(CompatMethodName_ViewInline, iters) {
  runRequestBenchmark<compatMethodNameInline>(iters);
}
BENCHMARK(CompatMethodName_RawOpaque, iters) {
  runRequestBenchmark<rawCompatMethodNameOpaque>(iters);
}
BENCHMARK_RELATIVE(CompatMethodName_ViewOpaque, iters) {
  runRequestBenchmark<compatMethodNameOpaque>(iters);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(CompatHeaders_RawInline, iters) {
  runRequestBenchmark<rawCompatHeadersInline>(iters);
}
BENCHMARK_RELATIVE(CompatHeaders_ViewInline, iters) {
  runRequestBenchmark<compatHeadersInline>(iters);
}
BENCHMARK(CompatHeaders_RawOpaque, iters) {
  runRequestBenchmark<rawCompatHeadersOpaque>(iters);
}
BENCHMARK_RELATIVE(CompatHeaders_ViewOpaque, iters) {
  runRequestBenchmark<compatHeadersOpaque>(iters);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(CompatConnection_RawInline, iters) {
  runRequestBenchmark<rawCompatConnectionInline>(iters);
}
BENCHMARK_RELATIVE(CompatConnection_ViewInline, iters) {
  runRequestBenchmark<compatConnectionInline>(iters);
}
BENCHMARK(CompatConnection_RawOpaque, iters) {
  runRequestBenchmark<rawCompatConnectionOpaque>(iters);
}
BENCHMARK_RELATIVE(CompatConnection_ViewOpaque, iters) {
  runRequestBenchmark<compatConnectionOpaque>(iters);
}

} // namespace
} // namespace apache::thrift::fast_thrift::thrift::server::view_benchmark

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
