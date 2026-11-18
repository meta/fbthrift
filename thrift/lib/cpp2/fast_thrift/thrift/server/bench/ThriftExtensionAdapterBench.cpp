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
#include <cstdio>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>

#include <boost/intrusive_ptr.hpp>
#include <glog/logging.h>

#include <folly/Benchmark.h>
#include <folly/CPortability.h>
#include <folly/ExceptionWrapper.h>
#include <folly/init/Init.h>
#include <folly/io/async/EventBase.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Handler.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineRef.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/ConnectionPayloads.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/ExtensionStateStore.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/Messages.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/extension/ThriftExtensionPipelineHandler.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/util/PayloadUtils.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/util/ResponsePayloads.h>

namespace apache::thrift::fast_thrift::thrift::server::adapter_benchmark {

namespace cp = apache::thrift::fast_thrift::channel_pipeline;

namespace {

enum class RequestMode { None, Noop, Observe, OpaqueVerdict, OpaqueResult };

struct CallbackState {
  uint64_t requests{0};
  uint64_t requestStreamIds{0};
  uint64_t responses{0};
  uint64_t responseHeaderBytes{0};
  bool reject{false};
  bool pressure{false};
  folly::exception_wrapper cause{
      folly::make_exception_wrapper<std::runtime_error>("benchmark rejection")};
  std::string key{"key"};
  std::string value{"set"};
  ReadResumer resumer;
};

template <bool Needed>
struct CallbackBinding;

template <>
struct CallbackBinding<true> {
  explicit CallbackBinding(CallbackState& state) : state(&state) {}
  CallbackState* state;
};

template <>
struct CallbackBinding<false> {
  explicit CallbackBinding(CallbackState&) {}
};

FOLLY_ALWAYS_INLINE RequestVerdict
decide(uint32_t streamId, CallbackState& state) noexcept {
  ++state.requests;
  state.requestStreamIds += streamId;
  if (state.reject) {
    return RequestVerdict::reject(state.cause);
  }
  return state.pressure ? RequestVerdict::backpressure()
                        : RequestVerdict::proceed();
}

// Pointer barriers keep LTO from replacing callback borrows with scalar fields.
FOLLY_NOINLINE RequestVerdict rawVerdictCallback(
    const ThriftServerRequestMessage& request, CallbackState& state) noexcept {
  const auto* source = &request;
  auto* callbackState = &state;
  folly::makeUnpredictable(source);
  folly::makeUnpredictable(callbackState);
  return decide(source->streamId, *callbackState);
}

FOLLY_NOINLINE RequestVerdict viewVerdictCallback(
    const ThriftRequestView& request, CallbackState& state) noexcept {
  const auto* source = &request;
  auto* callbackState = &state;
  folly::makeUnpredictable(source);
  folly::makeUnpredictable(callbackState);
  return decide(source->streamId(), *callbackState);
}

template <RequestMode Mode, bool Response, bool Backpressure>
class ExtensionCallbacks {
  static constexpr bool kNeedsState =
      (Mode != RequestMode::None && Mode != RequestMode::Noop) || Response ||
      Backpressure;

 public:
  explicit ExtensionCallbacks(CallbackState& state) : binding_(state) {}

  RequestVerdict onRequest(const ThriftRequestView& request) noexcept
    requires(
        Mode == RequestMode::Noop || Mode == RequestMode::Observe ||
        Mode == RequestMode::OpaqueVerdict)
  {
    if constexpr (Mode == RequestMode::Noop) {
      return RequestVerdict::proceed();
    } else if constexpr (Mode == RequestMode::Observe) {
      ++binding_.state->requests;
      binding_.state->requestStreamIds += request.streamId();
      return RequestVerdict::proceed();
    } else {
      return viewVerdictCallback(request, *binding_.state);
    }
  }

  FOLLY_NOINLINE RequestVerdict
  onRequest(const ThriftRequestView& request) noexcept
    requires(Mode == RequestMode::OpaqueResult)
  {
    auto* instance = this;
    const auto* source = &request;
    folly::makeUnpredictable(instance);
    folly::makeUnpredictable(source);
    auto* callbackState = instance->binding_.state;
    folly::makeUnpredictable(callbackState);
    return decide(source->streamId(), *callbackState);
  }

  void onResponse(ThriftResponseMutator& response) noexcept
    requires Response
  {
    auto& state = *binding_.state;
    ++state.responses;
    if (const auto* previous = response.header(state.key)) {
      state.responseHeaderBytes += previous->size();
    }
    response.setHeader(state.key, state.value);
  }

  void onBackpressureAttached(ReadResumer resumer) noexcept
    requires Backpressure
  {
    binding_.state->resumer = std::move(resumer);
  }

  void onEgressDrained(const ThriftConnectionView&) noexcept
    requires Backpressure
  {}

 private:
  [[no_unique_address]] CallbackBinding<kNeedsState> binding_;
};

// The sink borrows each box without consuming it, allowing prepared messages
// to be reused while measuring synchronous forwarding.
class MockContext {
 public:
  explicit MockContext(folly::EventBase& eventBase) : eventBase_(&eventBase) {}

  cp::Result fireRead(cp::TypeErasedBox&& message) noexcept {
    ++reads;
    readStreamIds += message.get<ThriftServerRequestMessage>().streamId;
    return readResult;
  }

  cp::Result fireWrite(cp::TypeErasedBox&& message) noexcept {
    ++writes;
    const auto& response = message.get<ThriftServerResponseMessage>();
    lastResponseHasContext = response.requestContext != nullptr;
    if (response.payload.is<ThriftInitialResponsePayload>()) {
      const auto& payload =
          response.payload.get<ThriftInitialResponsePayload>();
      writeStreamIds += payload.streamId;
      lastResponseIsApplicationError = isAppUnknownException(payload);
    } else if (response.payload.is<ThriftErrorPayload>()) {
      writeStreamIds += response.payload.get<ThriftErrorPayload>().streamId;
    }
    return writeResult;
  }

  void fireException(folly::exception_wrapper&&) noexcept { ++exceptions; }
  cp::PipelineRef pipeline() const noexcept { return {}; }
  folly::EventBase* eventBase() const noexcept { return eventBase_; }

  void reset() noexcept {
    reads = 0;
    writes = 0;
    readStreamIds = 0;
    writeStreamIds = 0;
    exceptions = 0;
    lastResponseHasContext = false;
    lastResponseIsApplicationError = false;
  }

  uint64_t reads{0};
  uint64_t writes{0};
  uint64_t readStreamIds{0};
  uint64_t writeStreamIds{0};
  uint64_t exceptions{0};
  bool lastResponseHasContext{false};
  bool lastResponseIsApplicationError{false};
  cp::Result readResult{cp::Result::Success};
  cp::Result writeResult{cp::Result::Success};

 private:
  folly::EventBase* eventBase_;
};

struct EmptyBackpressureState {};
struct NativeBackpressureState {
  std::shared_ptr<backpressure_detail::ResumeControl> control;
  cp::PipelineRef pipeline;
  ReadBackpressureArbiter* arbiter{nullptr};
  bool paused{false};
};

// The control matches request/response effects; egress event delivery is
// covered by server tests and is outside this borrowing-sink benchmark.
template <RequestMode Mode, bool Response, bool Backpressure>
class NativeSafetyHandler {
  static constexpr bool kNeedsState =
      (Mode != RequestMode::None && Mode != RequestMode::Noop) || Response ||
      Backpressure;

 public:
  NativeSafetyHandler(ExtensionStateStore&, CallbackState& state)
      : binding_(state) {}

  cp::Result onRead(MockContext& context, cp::TypeErasedBox&& message) noexcept
    requires(Mode != RequestMode::OpaqueResult)
  {
    return read(context, std::move(message));
  }

  FOLLY_NOINLINE cp::Result onRead(
      MockContext& context, cp::TypeErasedBox&& message) noexcept
    requires(Mode == RequestMode::OpaqueResult)
  {
    auto* instance = this;
    auto* source = &message;
    folly::makeUnpredictable(instance);
    folly::makeUnpredictable(source);
    return instance->read(context, std::move(*source));
  }

  cp::Result onWrite(
      MockContext& context, cp::TypeErasedBox&& message) noexcept {
    if constexpr (Response) {
      auto& response = message.get<ThriftServerResponseMessage>();
      auto& state = *binding_.state;
      ++state.responses;
      if (response.requestContext != nullptr) {
        if (const auto* previous =
                response.requestContext->getResponseHeader(state.key)) {
          state.responseHeaderBytes += previous->size();
        }
        response.requestContext->setResponseHeader(state.key, state.value);
      }
    }
    return context.fireWrite(std::move(message));
  }

  void handlerAdded(MockContext& context) noexcept {
    if constexpr (Backpressure) {
      bp_.pipeline = context.pipeline();
      bp_.control = std::make_shared<backpressure_detail::ResumeControl>();
      bp_.control->owner = this;
      bp_.control->resumeFn = +[](void* owner) noexcept {
        static_cast<NativeSafetyHandler*>(owner)->resumeReads();
      };
      binding_.state->resumer = ReadResumer(bp_.control, context.eventBase());
    }
  }

  void handlerRemoved(MockContext&) noexcept { detach(); }
  void onPipelineInactive(MockContext&) noexcept { detach(); }
  void onPipelineActive(MockContext&) noexcept {}
  void onReadReady(MockContext&) noexcept {}
  void onWriteReady(MockContext&) noexcept {}
  void onException(
      MockContext& context, folly::exception_wrapper&& exception) noexcept {
    context.fireException(std::move(exception));
  }

 private:
  FOLLY_ALWAYS_INLINE cp::Result read(
      MockContext& context, cp::TypeErasedBox&& message) noexcept {
    if constexpr (
        (Mode == RequestMode::None || Mode == RequestMode::Noop) &&
        !Backpressure) {
      return context.fireRead(std::move(message));
    } else {
      auto& request = message.get<ThriftServerRequestMessage>();
      if (request.payload.is<ThriftConnectionSetupPayload>()) [[unlikely]] {
        if constexpr (Backpressure) {
          bp_.arbiter = &request.payload.get<ThriftConnectionSetupPayload>()
                             .setup->connContext->readBackpressureArbiter();
        }
        return context.fireRead(std::move(message));
      }
      const cp::Result result = [&]() noexcept {
        if (isStreamControlRequest(request.payload)) [[unlikely]] {
          return context.fireRead(std::move(message));
        }
        if constexpr (Mode == RequestMode::None || Mode == RequestMode::Noop) {
          return context.fireRead(std::move(message));
        } else if constexpr (Mode == RequestMode::Observe) {
          ++binding_.state->requests;
          binding_.state->requestStreamIds += request.streamId;
          return context.fireRead(std::move(message));
        } else {
          const uint32_t streamId = request.streamId;
          const RequestVerdict verdict = [&]() noexcept {
            if constexpr (Mode == RequestMode::OpaqueVerdict) {
              return rawVerdictCallback(request, *binding_.state);
            } else {
              auto* callbackState = binding_.state;
              folly::makeUnpredictable(callbackState);
              return decide(request.streamId, *callbackState);
            }
          }();
          if (!verdict.isRejected()) {
            if (verdict.appliesBackpressure()) [[unlikely]] {
              if constexpr (Backpressure) {
                bp_.paused = true;
                DCHECK_NOTNULL(bp_.arbiter)->acquire();
              } else {
                DCHECK(false) << "backpressure requires a resumer";
              }
            }
            return context.fireRead(std::move(message));
          }
          auto response = makeRejectionMessage(streamId, verdict.cause());
          response.requestContext = std::move(request.requestContext);
          return context.fireWrite(cp::erase_and_box(std::move(response)));
        }
      }();
      if constexpr (Backpressure) {
        if (bp_.paused && result != cp::Result::Error) [[unlikely]] {
          return cp::Result::Backpressure;
        }
      }
      return result;
    }
  }

  void resumeReads() noexcept
    requires Backpressure
  {
    DCHECK(
        bp_.pipeline == nullptr ||
        bp_.pipeline.eventBase()->isInEventBaseThread());
    if (!bp_.paused) {
      return;
    }
    bp_.paused = false;
    if (DCHECK_NOTNULL(bp_.arbiter)->release() && bp_.pipeline != nullptr) {
      bp_.pipeline.onReadReady();
    }
  }

  void detach() noexcept {
    if constexpr (Backpressure) {
      if (bp_.paused) {
        bp_.paused = false;
        (void)DCHECK_NOTNULL(bp_.arbiter)->release();
      }
      bp_.pipeline.reset();
      bp_.arbiter = nullptr;
      if (bp_.control != nullptr) {
        bp_.control->owner = nullptr;
      }
    }
  }

  [[no_unique_address]] CallbackBinding<kNeedsState> binding_;
  [[no_unique_address]] std::conditional_t<
      Backpressure,
      NativeBackpressureState,
      EmptyBackpressureState> bp_;
};

// Deliberately omits filtering; use only normal requests to measure the cost of
// the extension safety contract, not as an equivalent extension replacement.
class MinimalUnfilteredNativeHandler {
 public:
  MinimalUnfilteredNativeHandler(ExtensionStateStore&, CallbackState& state)
      : state_(&state) {}

  cp::Result onRead(
      MockContext& context, cp::TypeErasedBox&& message) noexcept {
    ++state_->requests;
    state_->requestStreamIds +=
        message.get<ThriftServerRequestMessage>().streamId;
    return context.fireRead(std::move(message));
  }
  cp::Result onWrite(
      MockContext& context, cp::TypeErasedBox&& message) noexcept {
    return context.fireWrite(std::move(message));
  }
  void handlerAdded(MockContext&) noexcept {}
  void handlerRemoved(MockContext&) noexcept {}
  void onPipelineActive(MockContext&) noexcept {}
  void onPipelineInactive(MockContext&) noexcept {}
  void onReadReady(MockContext&) noexcept {}
  void onWriteReady(MockContext&) noexcept {}
  void onException(
      MockContext& context, folly::exception_wrapper&& exception) noexcept {
    context.fireException(std::move(exception));
  }

 private:
  CallbackState* state_;
};

template <RequestMode Mode, bool Response, bool Backpressure>
using ExtensionAdapter = ThriftExtensionPipelineHandler<
    ExtensionCallbacks<Mode, Response, Backpressure>,
    MockContext>;

class Fixture {
 public:
  Fixture() : connection(new ThriftConnContext()) {
    ThriftServerRequestMessage request;
    request.requestContext = makeContext();
    request.payload = ThriftServerRequestResponsePayload{};
    request.streamId = 1;
    requests[0] = cp::erase_and_box(std::move(request));

    auto setup = std::make_unique<ConnectionSetupData>();
    setup->connContext = connection.get();
    ThriftServerRequestMessage setupMessage;
    setupMessage.payload = ThriftConnectionSetupPayload{std::move(setup)};
    requests[1] = cp::erase_and_box(std::move(setupMessage));

    ThriftServerRequestMessage control;
    control.payload = ThriftRequestNPayload{.streamId = 5, .requestN = 1};
    control.streamId = 5;
    requests[2] = cp::erase_and_box(std::move(control));

    ThriftServerRequestMessage cancel;
    cancel.payload = ThriftRequestCancellationPayload{};
    cancel.streamId = 7;
    requests[3] = cp::erase_and_box(std::move(cancel));

    ThriftServerResponseMessage response;
    response.requestContext = makeContext();
    response.payload = ThriftInitialResponsePayload{.streamId = 1};
    responses[0] = cp::erase_and_box(std::move(response));
    ThriftServerResponseMessage emptyResponse;
    emptyResponse.payload = ThriftInitialResponsePayload{.streamId = 3};
    responses[1] = cp::erase_and_box(std::move(emptyResponse));
  }

  folly::EventBase eventBase;
  ExtensionStateStore store;
  CallbackState state;
  boost::intrusive_ptr<ThriftConnContext> connection;
  std::array<cp::TypeErasedBox, 4> requests;
  std::array<cp::TypeErasedBox, 2> responses;

 private:
  ThriftRequestContextPtr makeContext() {
    auto context = makeThriftRequestContext(eventBase);
    context->setConnectionContext(connection);
    context->setResponseHeader(state.key, "seed");
    return context;
  }
};

enum class Scenario {
  Read,
  Filtered,
  Reject,
  Write,
  EmptyWrite,
  ForwardBackpressure,
  ForwardError,
  PauseAndResume
};

struct Outcome {
  std::array<cp::Result, 4> results{
      cp::Result::Success,
      cp::Result::Success,
      cp::Result::Success,
      cp::Result::Success};
  uint64_t reads{0};
  uint64_t writes{0};
  uint64_t readStreamIds{0};
  uint64_t writeStreamIds{0};
  uint64_t requestCalls{0};
  uint64_t requestStreamIds{0};
  uint64_t responseCalls{0};
  uint64_t responseHeaderBytes{0};
  bool requestHasContext{false};
  bool lastResponseHasContext{false};
  bool lastResponseIsApplicationError{false};
  std::string responseHeader;
  std::size_t arbiterHolders{0};
  bool operator==(const Outcome&) const = default;
};

template <class Handler>
Outcome observe(Scenario scenario) {
  static_assert(cp::DuplexHandler<Handler, MockContext>);
  Fixture fixture;
  MockContext context(fixture.eventBase);
  Handler handler(fixture.store, fixture.state);
  handler.handlerAdded(context);
  (void)handler.onRead(context, std::move(fixture.requests[1]));
  context.reset();
  Outcome outcome;
  auto read = [&](std::size_t index) {
    return handler.onRead(context, std::move(fixture.requests[index]));
  };
  switch (scenario) {
    case Scenario::Filtered:
      for (std::size_t i = 0; i < fixture.requests.size(); ++i) {
        outcome.results[i] = read(i);
      }
      break;
    case Scenario::Reject:
      fixture.state.reject = true;
      outcome.results[0] = read(0);
      break;
    case Scenario::Write:
      outcome.results[0] =
          handler.onWrite(context, std::move(fixture.responses[0]));
      break;
    case Scenario::EmptyWrite:
      outcome.results[0] =
          handler.onWrite(context, std::move(fixture.responses[1]));
      break;
    case Scenario::ForwardBackpressure:
      context.readResult = cp::Result::Backpressure;
      outcome.results[0] = read(0);
      break;
    case Scenario::ForwardError:
      context.readResult = cp::Result::Error;
      outcome.results[0] = read(0);
      break;
    case Scenario::PauseAndResume:
      fixture.state.pressure = true;
      outcome.results[0] = read(0);
      CHECK_EQ(fixture.connection->readBackpressureArbiter().holderCount(), 1);
      fixture.state.pressure = false;
      outcome.results[1] = read(0);
      fixture.state.resumer.resume();
      CHECK_EQ(fixture.connection->readBackpressureArbiter().holderCount(), 0);
      outcome.results[2] = read(0);
      context.readResult = cp::Result::Error;
      outcome.results[3] = read(0);
      break;
    case Scenario::Read:
      outcome.results[0] = read(0);
      break;
  }
  outcome.reads = context.reads;
  outcome.writes = context.writes;
  outcome.readStreamIds = context.readStreamIds;
  outcome.writeStreamIds = context.writeStreamIds;
  outcome.requestCalls = fixture.state.requests;
  outcome.requestStreamIds = fixture.state.requestStreamIds;
  outcome.responseCalls = fixture.state.responses;
  outcome.responseHeaderBytes = fixture.state.responseHeaderBytes;
  outcome.requestHasContext = fixture.requests[0]
                                  .template get<ThriftServerRequestMessage>()
                                  .requestContext != nullptr;
  outcome.lastResponseHasContext = context.lastResponseHasContext;
  outcome.lastResponseIsApplicationError =
      context.lastResponseIsApplicationError;
  const auto& response =
      fixture.responses[0].template get<ThriftServerResponseMessage>();
  outcome.responseHeader =
      *response.requestContext->getResponseHeader(fixture.state.key);
  outcome.arbiterHolders =
      fixture.connection->readBackpressureArbiter().holderCount();
  handler.handlerRemoved(context);
  CHECK_EQ(fixture.connection->readBackpressureArbiter().holderCount(), 0);
  CHECK(!fixture.state.resumer);
  return outcome;
}

template <RequestMode Mode, bool Response, bool Backpressure>
void verifyPair(Scenario scenario) {
  const auto native =
      observe<NativeSafetyHandler<Mode, Response, Backpressure>>(scenario);
  const auto extension =
      observe<ExtensionAdapter<Mode, Response, Backpressure>>(scenario);
  CHECK(native == extension);
  if (scenario == Scenario::Filtered) {
    CHECK_EQ(extension.reads, 4);
    CHECK_EQ(
        extension.requestCalls,
        Mode == RequestMode::None || Mode == RequestMode::Noop ? 0 : 1);
    CHECK_EQ(extension.writes, 0);
  } else if (scenario == Scenario::Reject) {
    CHECK_EQ(extension.reads, 0);
    CHECK_EQ(extension.writes, 1);
    CHECK(!extension.requestHasContext);
    CHECK(extension.lastResponseHasContext);
    CHECK(extension.lastResponseIsApplicationError);
  } else if (scenario == Scenario::PauseAndResume) {
    CHECK(extension.results[0] == cp::Result::Backpressure);
    CHECK(extension.results[1] == cp::Result::Backpressure);
    CHECK(extension.results[2] == cp::Result::Success);
    CHECK(extension.results[3] == cp::Result::Error);
  } else if (scenario == Scenario::Write) {
    CHECK_EQ(extension.writes, 1);
    CHECK_EQ(extension.responseHeader, Response ? "set" : "seed");
  }
}

using ObserveNative = NativeSafetyHandler<RequestMode::Observe, false, false>;
using ObserveExtension = ExtensionAdapter<RequestMode::Observe, false, false>;

FOLLY_NOINLINE cp::Result probeNativeRead(
    ObserveNative& handler,
    MockContext& context,
    cp::TypeErasedBox&& message) noexcept {
  auto* instance = &handler;
  auto* sink = &context;
  auto* source = &message;
  folly::makeUnpredictable(instance);
  folly::makeUnpredictable(sink);
  folly::makeUnpredictable(source);
  return instance->onRead(*sink, std::move(*source));
}

FOLLY_NOINLINE cp::Result probeExtensionRead(
    ObserveExtension& handler,
    MockContext& context,
    cp::TypeErasedBox&& message) noexcept {
  auto* instance = &handler;
  auto* sink = &context;
  auto* source = &message;
  folly::makeUnpredictable(instance);
  folly::makeUnpredictable(sink);
  folly::makeUnpredictable(source);
  return instance->onRead(*sink, std::move(*source));
}

void verifyEntryProbes() {
  Fixture nativeFixture;
  Fixture extensionFixture;
  MockContext nativeContext(nativeFixture.eventBase);
  MockContext extensionContext(extensionFixture.eventBase);
  ObserveNative native(nativeFixture.store, nativeFixture.state);
  ObserveExtension extension(extensionFixture.store, extensionFixture.state);
  auto* nativeMessage = &nativeFixture.requests[0];
  auto* extensionMessage = &extensionFixture.requests[0];
  folly::makeUnpredictable(nativeMessage);
  folly::makeUnpredictable(extensionMessage);
  CHECK(
      probeNativeRead(native, nativeContext, std::move(*nativeMessage)) ==
      cp::Result::Success);
  CHECK(
      probeExtensionRead(
          extension, extensionContext, std::move(*extensionMessage)) ==
      cp::Result::Success);
  CHECK_EQ(nativeContext.reads, extensionContext.reads);
  CHECK_EQ(nativeFixture.state.requests, extensionFixture.state.requests);
  CHECK_EQ(
      nativeFixture.state.requestStreamIds,
      extensionFixture.state.requestStreamIds);
}

void verifyContracts() {
  for (const auto scenario :
       {Scenario::Read,
        Scenario::Filtered,
        Scenario::ForwardBackpressure,
        Scenario::ForwardError,
        Scenario::Write,
        Scenario::EmptyWrite}) {
    verifyPair<RequestMode::Observe, false, false>(scenario);
    verifyPair<RequestMode::Noop, false, false>(scenario);
    verifyPair<RequestMode::None, true, false>(scenario);
  }
  for (const auto scenario :
       {Scenario::Read, Scenario::Filtered, Scenario::Reject}) {
    verifyPair<RequestMode::OpaqueVerdict, false, false>(scenario);
    verifyPair<RequestMode::OpaqueResult, false, false>(scenario);
    verifyPair<RequestMode::OpaqueVerdict, false, true>(scenario);
  }
  verifyPair<RequestMode::Observe, true, false>(Scenario::Write);
  verifyPair<RequestMode::Observe, true, false>(Scenario::EmptyWrite);
  verifyPair<RequestMode::OpaqueVerdict, false, true>(Scenario::PauseAndResume);
  verifyEntryProbes();
}

template <RequestMode Mode, bool Response, bool Backpressure>
void printSize(const char* label) {
  std::printf(
      "sizeof %s: native=%zu extension=%zu\n",
      label,
      sizeof(NativeSafetyHandler<Mode, Response, Backpressure>),
      sizeof(ExtensionAdapter<Mode, Response, Backpressure>));
}

void printSizes() {
  printSize<RequestMode::Observe, false, false>("request-only");
  printSize<RequestMode::Noop, false, false>("no-op");
  printSize<RequestMode::None, true, false>("response-only");
  printSize<RequestMode::OpaqueVerdict, false, true>("backpressure");
}

template <class Handler, bool Write, bool Filtered>
void runBenchmark(std::size_t iters) {
  static_assert(cp::DuplexHandler<Handler, MockContext>);
  folly::BenchmarkSuspender suspender;
  Fixture fixture;
  MockContext context(fixture.eventBase);
  Handler handler(fixture.store, fixture.state);
  handler.handlerAdded(context);
  (void)handler.onRead(context, std::move(fixture.requests[1]));
  context.reset();
  fixture.state.requests = 0;
  fixture.state.requestStreamIds = 0;
  suspender.dismiss();
  for (std::size_t i = 0; i < iters; ++i) {
    auto* instance = &handler;
    auto* sink = &context;
    auto* message =
        Write ? &fixture.responses[0] : &fixture.requests[Filtered ? i & 3 : 0];
    folly::makeUnpredictable(instance);
    folly::makeUnpredictable(sink);
    folly::makeUnpredictable(message);
    const auto result = [&]() noexcept {
      if constexpr (Write) {
        return instance->onWrite(*sink, std::move(*message));
      } else {
        return instance->onRead(*sink, std::move(*message));
      }
    }();
    folly::doNotOptimizeAway(result);
  }
  suspender.rehire();
  folly::doNotOptimizeAway(context.reads);
  folly::doNotOptimizeAway(context.writes);
  folly::doNotOptimizeAway(fixture.state.requests);
  folly::doNotOptimizeAway(fixture.state.responseHeaderBytes);
  handler.handlerRemoved(context);
}

BENCHMARK(RequestRead_NativeSafety, iters) {
  runBenchmark<ObserveNative, false, false>(iters);
}
BENCHMARK_RELATIVE(RequestRead_Extension, iters) {
  runBenchmark<ObserveExtension, false, false>(iters);
}
BENCHMARK_RELATIVE(RequestRead_MinimalNative_SafetyContractCost, iters) {
  runBenchmark<MinimalUnfilteredNativeHandler, false, false>(iters);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(RequestOpaque_ResultABI_NativeSafety, iters) {
  runBenchmark<
      NativeSafetyHandler<RequestMode::OpaqueResult, false, false>,
      false,
      false>(iters);
}
BENCHMARK_RELATIVE(RequestOpaque_VerdictABI_Extension, iters) {
  runBenchmark<
      ExtensionAdapter<RequestMode::OpaqueResult, false, false>,
      false,
      false>(iters);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(RequestOpaque_SameVerdictABI_NativeSafety, iters) {
  runBenchmark<
      NativeSafetyHandler<RequestMode::OpaqueVerdict, false, false>,
      false,
      false>(iters);
}
BENCHMARK_RELATIVE(RequestOpaque_SameVerdictABI_Extension, iters) {
  runBenchmark<
      ExtensionAdapter<RequestMode::OpaqueVerdict, false, false>,
      false,
      false>(iters);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(ResponseMutation_NativeSafety, iters) {
  runBenchmark<
      NativeSafetyHandler<RequestMode::Observe, true, false>,
      true,
      false>(iters);
}
BENCHMARK_RELATIVE(ResponseMutation_Extension, iters) {
  runBenchmark<
      ExtensionAdapter<RequestMode::Observe, true, false>,
      true,
      false>(iters);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(AbsentResponseCallback_NativeSafety, iters) {
  runBenchmark<ObserveNative, true, false>(iters);
}
BENCHMARK_RELATIVE(AbsentResponseCallback_Extension, iters) {
  runBenchmark<ObserveExtension, true, false>(iters);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(RequestNoop_NativeSafety, iters) {
  runBenchmark<
      NativeSafetyHandler<RequestMode::Noop, false, false>,
      false,
      false>(iters);
}
BENCHMARK_RELATIVE(RequestNoop_Extension, iters) {
  runBenchmark<ExtensionAdapter<RequestMode::Noop, false, false>, false, false>(
      iters);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(MixedControlFiltering_NativeSafety, iters) {
  runBenchmark<ObserveNative, false, true>(iters);
}
BENCHMARK_RELATIVE(MixedControlFiltering_Extension, iters) {
  runBenchmark<ObserveExtension, false, true>(iters);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(BackpressureSuccess_NativeSafety, iters) {
  runBenchmark<
      NativeSafetyHandler<RequestMode::OpaqueVerdict, false, true>,
      false,
      false>(iters);
}
BENCHMARK_RELATIVE(BackpressureSuccess_Extension, iters) {
  runBenchmark<
      ExtensionAdapter<RequestMode::OpaqueVerdict, false, true>,
      false,
      false>(iters);
}

} // namespace

} // namespace apache::thrift::fast_thrift::thrift::server::adapter_benchmark

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  namespace benchmark =
      apache::thrift::fast_thrift::thrift::server::adapter_benchmark;
  benchmark::verifyContracts();
  benchmark::printSizes();
  folly::runBenchmarks();
  return 0;
}
