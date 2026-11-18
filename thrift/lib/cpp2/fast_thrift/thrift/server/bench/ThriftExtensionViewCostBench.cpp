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
#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include <folly/Benchmark.h>
#include <folly/CPortability.h>
#include <folly/CppAttributes.h>
#include <folly/init/Init.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/EventBase.h>
#include <folly/lang/SafeAssert.h>

#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/ConnectionPayloads.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/Messages.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/ExtensionSlots.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/ThriftConnContext.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/ThriftRequestContext.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/compatibility/context/NativeThriftContextView.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/extension/ThriftConnectionExtension.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/extension/ThriftExtension.h>

namespace apache::thrift::fast_thrift::thrift::server::view_cost_benchmark {
namespace {

using HeaderMap = ThriftRequestContext::HeaderMap;
using NativeRequestView = compatibility::ThriftRequestContextView<
    compatibility::ThriftContextBackend::Native>;
using NativeHeaderView = compatibility::ThriftRequestHeaderView<
    compatibility::ThriftContextBackend::Native>;

struct RequestState {
  std::uint64_t value;
};

struct StateExtension {
  EXTENSION_ID(view_cost_state);
  using RequestState = view_cost_benchmark::RequestState;
};

struct EarlierExtension {
  EXTENSION_ID(view_cost_earlier);
};

struct OwnedInput {
  std::string key;
  std::string value;
};

class HeaderFixture {
 public:
  HeaderFixture(bool longInputs, bool responsePresent)
      : key(longInputs ? std::string(96, 'k') : "key"),
        values{
            longInputs ? std::string(128, 'a') : "a",
            longInputs ? std::string(128, 'b') : "b"} {
    request.requestContext = makeThriftRequestContext(eventBase);
    request.requestContext->setHeader(key, values[0]);
    if (responsePresent) {
      response.requestContext = makeThriftRequestContext(eventBase);
      response.requestContext->setResponseHeader(key, values[0]);
    }
  }

  folly::EventBase eventBase;
  std::string key;
  std::array<std::string, 2> values;
  ThriftServerRequestMessage request;
  ThriftServerResponseMessage response;
};

class ReadFixture {
 public:
  ReadFixture() : layout(makeLayout()) {
    for (std::size_t i = 0; i < requests.size(); ++i) {
      auto metadata = std::make_unique<apache::thrift::RequestRpcMetadata>();
      if (i == 0) {
        metadata->protocol() = apache::thrift::ProtocolId::BINARY;
      }
      auto data = folly::IOBuf::copyBuffer("first");
      data->appendChain(folly::IOBuf::copyBuffer("second"));
      data->appendChain(folly::IOBuf::copyBuffer("third"));
      if (i != 0) {
        data->appendChain(folly::IOBuf::copyBuffer("fourth"));
      }
      requests[i].payload = ThriftServerRequestResponsePayload{
          .data = std::move(data), .metadata = std::move(metadata)};
    }
    requests[0].requestContext = makeThriftRequestContext(eventBase);
    requests[0].requestContext->installExtensions(layout);
    requests[0].requestContext->setState<StateExtension>(&state);
    requests[0].requestContext->setHeader("key", "value");
  }

  folly::EventBase eventBase;
  ExtensionLayout layout;
  RequestState state{17};
  std::array<ThriftServerRequestMessage, 2> requests;

 private:
  static ExtensionLayout makeLayout() {
    ExtensionLayoutBuilder builder;
    builder.add(EarlierExtension::kId);
    builder.add(StateExtension::kId);
    return std::move(builder).build();
  }
};

class SetupFixture {
 public:
  SetupFixture() : setup{} { setup.connContext = &connection; }

  ThriftConnContext connection;
  ConnectionSetupData setup;
  std::uint64_t identity{23};
};

void rawInputHeaderInline(
    ThriftServerRequestMessage& request, OwnedInput& input) noexcept {
  request.requestContext->setHeader(
      std::move(input.key), std::move(input.value));
}

void viewInputHeaderInline(
    ThriftServerRequestMessage& request, OwnedInput& input) noexcept {
  ThriftRequestMutator view(request);
  view.setHeader(std::move(input.key), std::move(input.value));
}

FOLLY_NOINLINE void rawInputHeaderOpaque(
    ThriftServerRequestMessage& request, OwnedInput& input) noexcept {
  auto* requestPtr = &request;
  auto* inputPtr = &input;
  folly::makeUnpredictable(requestPtr);
  folly::makeUnpredictable(inputPtr);

  rawInputHeaderInline(*requestPtr, *inputPtr);
}

FOLLY_NOINLINE void inputHeaderViewCallback(
    ThriftRequestMutator& request, OwnedInput& input) noexcept {
  auto* requestPtr = &request;
  auto* inputPtr = &input;
  folly::makeUnpredictable(requestPtr);
  folly::makeUnpredictable(inputPtr);

  requestPtr->setHeader(std::move(inputPtr->key), std::move(inputPtr->value));
}

void viewInputHeaderOpaque(
    ThriftServerRequestMessage& request, OwnedInput& input) noexcept {
  ThriftRequestMutator view(request);
  inputHeaderViewCallback(view, input);
}

// Match owned arguments before testing the response context.
void rawResponseSetter(
    ThriftServerResponseMessage& response,
    std::string key,
    std::string value) noexcept {
  if (auto* context = response.requestContext.get()) {
    context->mutableResponseHeaders()[std::move(key)] = std::move(value);
  }
}

void rawResponseOwnedInline(
    ThriftServerResponseMessage& response, OwnedInput& input) noexcept {
  rawResponseSetter(response, std::move(input.key), std::move(input.value));
}

void viewResponseOwnedInline(
    ThriftServerResponseMessage& response, OwnedInput& input) noexcept {
  ThriftResponseMutator view(response);
  view.setHeader(std::move(input.key), std::move(input.value));
}

FOLLY_NOINLINE void rawResponseOwnedOpaque(
    ThriftServerResponseMessage& response, OwnedInput& input) noexcept {
  auto* responsePtr = &response;
  auto* inputPtr = &input;
  folly::makeUnpredictable(responsePtr);
  folly::makeUnpredictable(inputPtr);

  rawResponseOwnedInline(*responsePtr, *inputPtr);
}

FOLLY_NOINLINE void responseOwnedViewCallback(
    ThriftResponseMutator& response, OwnedInput& input) noexcept {
  auto* responsePtr = &response;
  auto* inputPtr = &input;
  folly::makeUnpredictable(responsePtr);
  folly::makeUnpredictable(inputPtr);

  responsePtr->setHeader(std::move(inputPtr->key), std::move(inputPtr->value));
}

void viewResponseOwnedOpaque(
    ThriftServerResponseMessage& response, OwnedInput& input) noexcept {
  ThriftResponseMutator view(response);
  responseOwnedViewCallback(view, input);
}

void* FOLLY_NULLABLE rawSetupIdentityInline(
    ConnectionSetupData& setup, void* FOLLY_NULLABLE identity) noexcept {
  setup.connContext->setPeerIdentities(identity);
  return setup.connContext->getPeerIdentities();
}

void* FOLLY_NULLABLE viewSetupIdentityInline(
    ConnectionSetupData& setup, void* FOLLY_NULLABLE identity) noexcept {
  ThriftSetupConnectionMutator view(setup);
  view.setPeerIdentities(identity);
  return view.peerIdentities();
}

FOLLY_NOINLINE void* FOLLY_NULLABLE rawSetupIdentityOpaque(
    ConnectionSetupData& setup, void* FOLLY_NULLABLE identity) noexcept {
  auto* setupPtr = &setup;
  folly::makeUnpredictable(setupPtr);

  return rawSetupIdentityInline(*setupPtr, identity);
}

FOLLY_NOINLINE void* FOLLY_NULLABLE setupIdentityViewCallback(
    ThriftSetupConnectionMutator& setup,
    void* FOLLY_NULLABLE identity) noexcept {
  auto* setupPtr = &setup;
  folly::makeUnpredictable(setupPtr);

  setupPtr->setPeerIdentities(identity);
  return setupPtr->peerIdentities();
}

void* FOLLY_NULLABLE viewSetupIdentityOpaque(
    ConnectionSetupData& setup, void* FOLLY_NULLABLE identity) noexcept {
  ThriftSetupConnectionMutator view(setup);
  return setupIdentityViewCallback(view, identity);
}

apache::thrift::ProtocolId rawProtocolInline(
    const ThriftServerRequestMessage& request) noexcept {
  const auto* metadata = request.payload.getRequestRpcMetadata();
  return metadata && metadata->protocol().has_value()
      ? *metadata->protocol()
      : apache::thrift::ProtocolId::COMPACT;
}

apache::thrift::ProtocolId viewProtocolInline(
    const ThriftServerRequestMessage& request) noexcept {
  const ThriftRequestView view(request);
  return view.protocol();
}

FOLLY_NOINLINE apache::thrift::ProtocolId rawProtocolOpaque(
    const ThriftServerRequestMessage& request) noexcept {
  const auto* requestPtr = &request;
  folly::makeUnpredictable(requestPtr);

  return rawProtocolInline(*requestPtr);
}

FOLLY_NOINLINE apache::thrift::ProtocolId protocolViewCallback(
    const ThriftRequestView& request) noexcept {
  const auto* requestPtr = &request;
  folly::makeUnpredictable(requestPtr);

  return requestPtr->protocol();
}

apache::thrift::ProtocolId viewProtocolOpaque(
    const ThriftServerRequestMessage& request) noexcept {
  const ThriftRequestView view(request);
  return protocolViewCallback(view);
}

std::size_t rawSerializedSizeInline(
    const ThriftServerRequestMessage& request) noexcept {
  if (request.payload.is<ThriftServerRequestResponsePayload>()) {
    const auto& payload =
        request.payload.get<ThriftServerRequestResponsePayload>();
    return payload.data ? payload.data->computeChainDataLength() : 0;
  }
  return 0;
}

std::size_t viewSerializedSizeInline(
    const ThriftServerRequestMessage& request) noexcept {
  const ThriftRequestView view(request);
  return view.serializedSize();
}

FOLLY_NOINLINE std::size_t rawSerializedSizeOpaque(
    const ThriftServerRequestMessage& request) noexcept {
  const auto* requestPtr = &request;
  folly::makeUnpredictable(requestPtr);

  return rawSerializedSizeInline(*requestPtr);
}

FOLLY_NOINLINE std::size_t serializedSizeViewCallback(
    const ThriftRequestView& request) noexcept {
  const auto* requestPtr = &request;
  folly::makeUnpredictable(requestPtr);

  return requestPtr->serializedSize();
}

std::size_t viewSerializedSizeOpaque(
    const ThriftServerRequestMessage& request) noexcept {
  const ThriftRequestView view(request);
  return serializedSizeViewCallback(view);
}

RequestState* FOLLY_NULLABLE
rawStateInline(const ThriftServerRequestMessage& request) noexcept {
  const auto* context = request.requestContext.get();
  return context ? context->tryState<StateExtension>() : nullptr;
}

RequestState* FOLLY_NULLABLE
viewStateInline(const ThriftServerRequestMessage& request) noexcept {
  const ThriftRequestView view(request);
  return view.tryState<StateExtension>();
}

FOLLY_NOINLINE RequestState* FOLLY_NULLABLE
rawStateOpaque(const ThriftServerRequestMessage& request) noexcept {
  const auto* requestPtr = &request;
  folly::makeUnpredictable(requestPtr);

  return rawStateInline(*requestPtr);
}

FOLLY_NOINLINE RequestState* FOLLY_NULLABLE
stateViewCallback(const ThriftRequestView& request) noexcept {
  const auto* requestPtr = &request;
  folly::makeUnpredictable(requestPtr);

  return requestPtr->tryState<StateExtension>();
}

RequestState* FOLLY_NULLABLE
viewStateOpaque(const ThriftServerRequestMessage& request) noexcept {
  const ThriftRequestView view(request);
  return stateViewCallback(view);
}

const HeaderMap* FOLLY_NULLABLE
rawHeaderFacadeInline(const ThriftServerRequestMessage& request) noexcept {
  const auto* context = request.requestContext.get();
  return context ? &context->getHeaders() : nullptr;
}

const HeaderMap* FOLLY_NULLABLE
viewHeaderFacadeInline(const ThriftServerRequestMessage& request) noexcept {
  const ThriftRequestView extension(request);
  const NativeRequestView view(extension);
  const auto header = view.headerContainer();
  return header ? &header.headers() : nullptr;
}

FOLLY_NOINLINE const HeaderMap* FOLLY_NULLABLE
rawHeaderFacadeOpaque(const ThriftServerRequestMessage& request) noexcept {
  const auto* requestPtr = &request;
  folly::makeUnpredictable(requestPtr);

  return rawHeaderFacadeInline(*requestPtr);
}

FOLLY_NOINLINE const HeaderMap* FOLLY_NULLABLE
headerFacadeViewCallback(const NativeHeaderView& header) noexcept {
  const auto* headerPtr = &header;
  folly::makeUnpredictable(headerPtr);

  return *headerPtr ? &headerPtr->headers() : nullptr;
}

const HeaderMap* FOLLY_NULLABLE
viewHeaderFacadeOpaque(const ThriftServerRequestMessage& request) noexcept {
  const ThriftRequestView extension(request);
  const NativeRequestView view(extension);
  const auto header = view.headerContainer();
  return headerFacadeViewCallback(header);
}

template <auto Access, bool UseResponse>
void runOwnedBenchmark(
    std::size_t iters, bool longInputs, bool responsePresent) {
  folly::BenchmarkSuspender suspender;
  HeaderFixture fixture(longInputs, responsePresent);
  std::array<OwnedInput, 64> inputs;
  std::size_t completed = 0;
  while (completed < iters) {
    const std::size_t count = std::min(inputs.size(), iters - completed);
    // Allocation and API input copying are outside the timed section. Existing
    // map keys prevent node allocation; ownership destruction is common work.
    for (std::size_t i = 0; i < count; ++i) {
      inputs[i].key = fixture.key;
      inputs[i].value = fixture.values[(completed + i) & 1];
    }
    suspender.dismiss();
    for (std::size_t i = 0; i < count; ++i) {
      auto* input = &inputs[i];
      folly::makeUnpredictable(input);
      if constexpr (UseResponse) {
        auto* response = &fixture.response;
        folly::makeUnpredictable(response);
        Access(*response, *input);
        folly::doNotOptimizeAway(response);
      } else {
        auto* request = &fixture.request;
        folly::makeUnpredictable(request);
        Access(*request, *input);
        folly::doNotOptimizeAway(request);
      }
    }
    suspender.rehire();
    const auto& expected = fixture.values[(completed + count - 1) & 1];
    if constexpr (UseResponse) {
      if (responsePresent) {
        FOLLY_SAFE_CHECK(
            *fixture.response.requestContext->getResponseHeader(fixture.key) ==
                expected,
            "response setter result differs");
      }
    } else {
      FOLLY_SAFE_CHECK(
          *fixture.request.requestContext->getHeader(fixture.key) == expected,
          "input setter result differs");
    }
    completed += count;
  }
}

template <auto Access>
void runReadBenchmark(std::size_t iters) {
  folly::BenchmarkSuspender suspender;
  ReadFixture fixture;
  suspender.dismiss();
  for (std::size_t i = 0; i < iters; ++i) {
    auto* request = &fixture.requests[i & 1];
    folly::makeUnpredictable(request);
    const auto result = Access(*request);
    folly::doNotOptimizeAway(result);
  }
  suspender.rehire();
}

template <auto Access>
void runSetupBenchmark(std::size_t iters) {
  folly::BenchmarkSuspender suspender;
  SetupFixture fixture;
  suspender.dismiss();
  for (std::size_t i = 0; i < iters; ++i) {
    auto* setup = &fixture.setup;
    void* identity = (i & 1) ? &fixture.identity : nullptr;
    folly::makeUnpredictable(setup);
    folly::makeUnpredictable(identity);
    const auto result = Access(*setup, identity);
    folly::doNotOptimizeAway(result);
  }
  suspender.rehire();
}

template <auto RawInline, auto ViewInline, auto RawOpaque, auto ViewOpaque>
void verifyReadCase(ReadFixture& fixture) {
  for (const auto& request : fixture.requests) {
    const auto expected = RawInline(request);
    FOLLY_SAFE_CHECK(ViewInline(request) == expected, "inline read differs");
    FOLLY_SAFE_CHECK(RawOpaque(request) == expected, "opaque raw read differs");
    FOLLY_SAFE_CHECK(
        ViewOpaque(request) == expected, "opaque view read differs");
  }
}

void verifyCorrectness() {
  ReadFixture reads;
  verifyReadCase<
      rawProtocolInline,
      viewProtocolInline,
      rawProtocolOpaque,
      viewProtocolOpaque>(reads);
  verifyReadCase<
      rawSerializedSizeInline,
      viewSerializedSizeInline,
      rawSerializedSizeOpaque,
      viewSerializedSizeOpaque>(reads);
  verifyReadCase<
      rawStateInline,
      viewStateInline,
      rawStateOpaque,
      viewStateOpaque>(reads);
  verifyReadCase<
      rawHeaderFacadeInline,
      viewHeaderFacadeInline,
      rawHeaderFacadeOpaque,
      viewHeaderFacadeOpaque>(reads);

  for (bool longInputs : {false, true}) {
    HeaderFixture fixture(longInputs, true);
    for (auto access : std::array{
             &rawInputHeaderInline,
             &viewInputHeaderInline,
             &rawInputHeaderOpaque,
             &viewInputHeaderOpaque}) {
      fixture.request.requestContext->setHeader(fixture.key, fixture.values[0]);
      OwnedInput input{fixture.key, fixture.values[1]};
      access(fixture.request, input);
      FOLLY_SAFE_CHECK(
          *fixture.request.requestContext->getHeader(fixture.key) ==
              fixture.values[1],
          "input header setter differs");
    }
  }
  for (bool present : {false, true}) {
    HeaderFixture fixture(false, present);
    for (auto access : std::array{
             &rawResponseOwnedInline,
             &viewResponseOwnedInline,
             &rawResponseOwnedOpaque,
             &viewResponseOwnedOpaque}) {
      if (present) {
        fixture.response.requestContext->setResponseHeader(
            fixture.key, fixture.values[0]);
      }
      OwnedInput input{fixture.key, fixture.values[1]};
      access(fixture.response, input);
      if (present) {
        FOLLY_SAFE_CHECK(
            *fixture.response.requestContext->getResponseHeader(fixture.key) ==
                fixture.values[1],
            "response header setter differs");
      } else {
        FOLLY_SAFE_CHECK(
            fixture.response.requestContext == nullptr,
            "absent response gained a context");
      }
    }
  }
  SetupFixture setup;
  for (void* identity : std::array<void*, 2>{nullptr, &setup.identity}) {
    for (auto access : std::array{
             &rawSetupIdentityInline,
             &viewSetupIdentityInline,
             &rawSetupIdentityOpaque,
             &viewSetupIdentityOpaque}) {
      setup.connection.setPeerIdentities(
          identity == nullptr ? &setup.identity : nullptr);
      FOLLY_SAFE_CHECK(
          access(setup.setup, identity) == identity, "setup result differs");
      FOLLY_SAFE_CHECK(
          setup.connection.getPeerIdentities() == identity,
          "setup mutation differs");
    }
  }
}

BENCHMARK(InputHeader_OwnedShort_RawInline, iters) {
  runOwnedBenchmark<rawInputHeaderInline, false>(iters, false, false);
}

BENCHMARK(InputHeader_OwnedShort_ViewInline, iters) {
  runOwnedBenchmark<viewInputHeaderInline, false>(iters, false, false);
}

BENCHMARK(InputHeader_OwnedShort_RawOpaque, iters) {
  runOwnedBenchmark<rawInputHeaderOpaque, false>(iters, false, false);
}

BENCHMARK(InputHeader_OwnedShort_ViewOpaque, iters) {
  runOwnedBenchmark<viewInputHeaderOpaque, false>(iters, false, false);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(InputHeader_OwnedLong_RawInline, iters) {
  runOwnedBenchmark<rawInputHeaderInline, false>(iters, true, false);
}

BENCHMARK(InputHeader_OwnedLong_ViewInline, iters) {
  runOwnedBenchmark<viewInputHeaderInline, false>(iters, true, false);
}

BENCHMARK(InputHeader_OwnedLong_RawOpaque, iters) {
  runOwnedBenchmark<rawInputHeaderOpaque, false>(iters, true, false);
}

BENCHMARK(InputHeader_OwnedLong_ViewOpaque, iters) {
  runOwnedBenchmark<viewInputHeaderOpaque, false>(iters, true, false);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(ResponseHeader_OwnedPresent_RawInline, iters) {
  runOwnedBenchmark<rawResponseOwnedInline, true>(iters, false, true);
}

BENCHMARK(ResponseHeader_OwnedPresent_ViewInline, iters) {
  runOwnedBenchmark<viewResponseOwnedInline, true>(iters, false, true);
}

BENCHMARK(ResponseHeader_OwnedPresent_RawOpaque, iters) {
  runOwnedBenchmark<rawResponseOwnedOpaque, true>(iters, false, true);
}

BENCHMARK(ResponseHeader_OwnedPresent_ViewOpaque, iters) {
  runOwnedBenchmark<viewResponseOwnedOpaque, true>(iters, false, true);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(ResponseHeader_OwnedAbsent_RawInline, iters) {
  runOwnedBenchmark<rawResponseOwnedInline, true>(iters, false, false);
}

BENCHMARK(ResponseHeader_OwnedAbsent_ViewInline, iters) {
  runOwnedBenchmark<viewResponseOwnedInline, true>(iters, false, false);
}

BENCHMARK(ResponseHeader_OwnedAbsent_RawOpaque, iters) {
  runOwnedBenchmark<rawResponseOwnedOpaque, true>(iters, false, false);
}

BENCHMARK(ResponseHeader_OwnedAbsent_ViewOpaque, iters) {
  runOwnedBenchmark<viewResponseOwnedOpaque, true>(iters, false, false);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(SetupIdentity_RawInline, iters) {
  runSetupBenchmark<rawSetupIdentityInline>(iters);
}

BENCHMARK(SetupIdentity_ViewInline, iters) {
  runSetupBenchmark<viewSetupIdentityInline>(iters);
}

BENCHMARK(SetupIdentity_RawOpaque, iters) {
  runSetupBenchmark<rawSetupIdentityOpaque>(iters);
}

BENCHMARK(SetupIdentity_ViewOpaque, iters) {
  runSetupBenchmark<viewSetupIdentityOpaque>(iters);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(Protocol_RawInline, iters) {
  runReadBenchmark<rawProtocolInline>(iters);
}

BENCHMARK(Protocol_ViewInline, iters) {
  runReadBenchmark<viewProtocolInline>(iters);
}

BENCHMARK(Protocol_RawOpaque, iters) {
  runReadBenchmark<rawProtocolOpaque>(iters);
}

BENCHMARK(Protocol_ViewOpaque, iters) {
  runReadBenchmark<viewProtocolOpaque>(iters);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(SerializedSize_RawInline, iters) {
  runReadBenchmark<rawSerializedSizeInline>(iters);
}

BENCHMARK(SerializedSize_ViewInline, iters) {
  runReadBenchmark<viewSerializedSizeInline>(iters);
}

BENCHMARK(SerializedSize_RawOpaque, iters) {
  runReadBenchmark<rawSerializedSizeOpaque>(iters);
}

BENCHMARK(SerializedSize_ViewOpaque, iters) {
  runReadBenchmark<viewSerializedSizeOpaque>(iters);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(State_RawInline, iters) {
  runReadBenchmark<rawStateInline>(iters);
}

BENCHMARK(State_ViewInline, iters) {
  runReadBenchmark<viewStateInline>(iters);
}

BENCHMARK(State_RawOpaque, iters) {
  runReadBenchmark<rawStateOpaque>(iters);
}

BENCHMARK(State_ViewOpaque, iters) {
  runReadBenchmark<viewStateOpaque>(iters);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(HeaderFacade_RawInline, iters) {
  runReadBenchmark<rawHeaderFacadeInline>(iters);
}

BENCHMARK(HeaderFacade_ViewInline, iters) {
  runReadBenchmark<viewHeaderFacadeInline>(iters);
}

BENCHMARK(HeaderFacade_RawOpaque, iters) {
  runReadBenchmark<rawHeaderFacadeOpaque>(iters);
}

BENCHMARK(HeaderFacade_ViewOpaque, iters) {
  runReadBenchmark<viewHeaderFacadeOpaque>(iters);
}

BENCHMARK_DRAW_LINE();

} // namespace
} // namespace apache::thrift::fast_thrift::thrift::server::view_cost_benchmark

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  apache::thrift::fast_thrift::thrift::server::view_cost_benchmark::
      verifyCorrectness();
  folly::runBenchmarks();
  return 0;
}
