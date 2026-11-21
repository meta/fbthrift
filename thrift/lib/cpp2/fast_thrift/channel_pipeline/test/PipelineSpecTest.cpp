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

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/ExecutionDomain.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/FollyExecutorDomain.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineAdapter.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineRef.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineSpec.h>

#include <concepts>
#include <cstddef>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include <folly/ExceptionWrapper.h>
#include <folly/Executor.h>
#include <folly/executors/ManualExecutor.h>
#include <folly/io/async/EventBase.h>
#include <folly/portability/GTest.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/BufferAllocator.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Event.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/HandlerTag.h>

namespace apache::thrift::fast_thrift::channel_pipeline {
namespace {

// Observation point owned by the test. The built pipeline is held through the
// erased PipelineOwner, which exposes no typed state accessor, so
// handlers record into counters the test owns (reached via the TaskState)
// rather than the test reading pipeline internals.
struct Counters {
  std::size_t pipelineSize{0};
  int added{0};
  int active{0};
  int readsAtTail{0};
  int writesAtHead{0};
  int inboundExceptions{0};
  int outboundExceptions{0};
  int writeArmed{0};
  int writeReady{0};
  int readArmed{0};
  int readReady{0};
  int eventSum{0};
};

// A control-plane event carrying an int payload.
struct ValueEvent : EventTag<int> {};

// Per-connection state: just a pointer to the test's counters.
struct EchoTaskState {
  Counters* counters{nullptr};
};

struct CountingBase {
  template <typename C>
  void handlerAdded(C& ctx) noexcept {
    ++ctx.taskState().counters->added;
  }
  template <typename C>
  void handlerRemoved(C&) noexcept {}
  template <typename C>
  void onPipelineActive(C& ctx) noexcept {
    ++ctx.taskState().counters->active;
  }
  template <typename C>
  void onPipelineInactive(C&) noexcept {}
  template <typename C>
  void onReadReady(C&) noexcept {}
  template <typename C>
  void onWriteReady(C&) noexcept {}
  template <typename C>
  void onException(C&, folly::exception_wrapper&&) noexcept {}
};

// Head (transport side): forwards reads inbound, terminates writes.
struct HeadHandler : CountingBase {
  template <typename C>
  Result onRead(C& ctx, TypeErasedBox&& msg) noexcept {
    return ctx.fireRead(std::move(msg));
  }
  template <typename C>
  Result onWrite(C& ctx, TypeErasedBox&&) noexcept {
    ++ctx.taskState().counters->writesAtHead;
    return Result::Success;
  }
};

// Interior: forwards both directions.
struct MiddleHandler : CountingBase {
  template <typename C>
  Result onRead(C& ctx, TypeErasedBox&& msg) noexcept {
    return ctx.fireRead(std::move(msg));
  }
  template <typename C>
  Result onWrite(C& ctx, TypeErasedBox&& msg) noexcept {
    return ctx.fireWrite(std::move(msg));
  }
};

// Tail (app side): terminates reads, forwards writes outbound.
struct TailHandler : CountingBase {
  template <typename C>
  Result onRead(C& ctx, TypeErasedBox&&) noexcept {
    ++ctx.taskState().counters->readsAtTail;
    return Result::Success;
  }
  template <typename C>
  Result onWrite(C& ctx, TypeErasedBox&& msg) noexcept {
    return ctx.fireWrite(std::move(msg));
  }
};

// Inbound handler that records the exceptions it receives.
struct InboundExceptionSpy : CountingBase {
  template <typename C>
  Result onRead(C& ctx, TypeErasedBox&& msg) noexcept {
    return ctx.fireRead(std::move(msg));
  }
  template <typename C>
  void onException(C& ctx, folly::exception_wrapper&&) noexcept {
    ++ctx.taskState().counters->inboundExceptions;
  }
};

// Outbound-only handler (no onRead) that also implements onException. It must
// be skipped by exception dispatch because it does not satisfy InboundHandler.
struct OutboundExceptionSpy : CountingBase {
  template <typename C>
  Result onWrite(C& ctx, TypeErasedBox&& msg) noexcept {
    return ctx.fireWrite(std::move(msg));
  }
  template <typename C>
  void onException(C& ctx, folly::exception_wrapper&&) noexcept {
    ++ctx.taskState().counters->outboundExceptions;
  }
};

struct ReadyMarker {};

// Opts into write-readiness via the writeReadyHook_ member: arms on every write
// and records each wake, then cancels. (Arming unconditionally is artificial —
// a real handler arms only after a Backpressure result — but it exercises the
// arm/notify plumbing.)
struct WriteBackpressureHandler : CountingBase {
  ReadyMarker writeReadyHook_;
  template <typename C>
  Result onRead(C& ctx, TypeErasedBox&& msg) noexcept {
    return ctx.fireRead(std::move(msg));
  }
  template <typename C>
  Result onWrite(C& ctx, TypeErasedBox&& msg) noexcept {
    ctx.awaitWriteReady();
    ++ctx.taskState().counters->writeArmed;
    return ctx.fireWrite(std::move(msg));
  }
  template <typename C>
  void onWriteReady(C& ctx) noexcept {
    ++ctx.taskState().counters->writeReady;
    ctx.cancelAwaitWriteReady();
  }
};

struct DuplexBackpressureHandler : WriteBackpressureHandler {
  ReadyMarker readReadyHook_;

  template <typename C>
  Result onRead(C& ctx, TypeErasedBox&& msg) noexcept {
    ctx.awaitReadReady();
    ++ctx.taskState().counters->readArmed;
    return ctx.fireRead(std::move(msg));
  }

  template <typename C>
  void onReadReady(C& ctx) noexcept {
    ++ctx.taskState().counters->readReady;
    ctx.cancelAwaitReadReady();
  }
};

// The read-side mirror: arms read-readiness on every read and records each
// wake.
struct ReadBackpressureHandler : CountingBase {
  ReadyMarker readReadyHook_;
  template <typename C>
  Result onRead(C& ctx, TypeErasedBox&& msg) noexcept {
    ctx.awaitReadReady();
    ++ctx.taskState().counters->readArmed;
    return ctx.fireRead(std::move(msg));
  }
  template <typename C>
  Result onWrite(C& ctx, TypeErasedBox&& msg) noexcept {
    return ctx.fireWrite(std::move(msg));
  }
  template <typename C>
  void onReadReady(C& ctx) noexcept {
    ++ctx.taskState().counters->readReady;
    ctx.cancelAwaitReadReady();
  }
};

// Forwards both directions and, on read, publishes a ValueEvent in-pipeline.
struct EventPublisher : CountingBase {
  template <typename C>
  Result onRead(C& ctx, TypeErasedBox&& msg) noexcept {
    ctx.template fireEvent<ValueEvent>(7);
    return ctx.fireRead(std::move(msg));
  }
  template <typename C>
  Result onWrite(C& ctx, TypeErasedBox&& msg) noexcept {
    return ctx.fireWrite(std::move(msg));
  }
};

// Subscribes to ValueEvent and accumulates the payload into the shared
// counters.
struct EventSubscriber : CountingBase {
  using SubscribedEvents = Events<ValueEvent>;
  template <typename C>
  Result onRead(C& ctx, TypeErasedBox&& msg) noexcept {
    return ctx.fireRead(std::move(msg));
  }
  template <typename C>
  Result onWrite(C& ctx, TypeErasedBox&& msg) noexcept {
    return ctx.fireWrite(std::move(msg));
  }
  template <PipelineEvent E, typename C>
  void on(C& ctx, const typename E::Payload& payload) noexcept {
    ctx.taskState().counters->eventSum += payload;
  }
};

HANDLER_TAG(head);
HANDLER_TAG(mid);
HANDLER_TAG(tail);

template <typename T>
constexpr bool kHasAllocator = requires(T& value) { value.allocate(0); };
template <typename T>
constexpr bool kHasEventBase = requires(T& value) { value.eventBase(); };

static_assert(kHasAllocator<PipelineRef>);
static_assert(kHasEventBase<PipelineRef>);

template <typename Spec>
constexpr bool kCanConfigureState =
    requires(Spec&& spec) { std::move(spec).state(EchoTaskState{}); };
template <typename Spec>
constexpr bool kCanConfigureAllocator = requires(Spec&& spec) {
  std::move(spec).allocator(SimpleBufferAllocator{});
};
template <typename Spec>
constexpr bool kCanBuild = requires(Spec&& spec) { std::move(spec).build(); };
template <typename Spec>
constexpr bool kCanReuse = requires(const Spec& spec) { spec.build(); };
template <typename Spec, typename Domain>
constexpr bool kCanBuildWith = requires(Spec&& spec, Domain domain) {
  std::move(spec).build(std::move(domain));
};
template <typename Spec, typename Domain>
constexpr bool kCanReuseWith = requires(const Spec& spec, Domain domain) {
  spec.build(std::move(domain));
};

using EmptySpec = decltype(PipelineSpec());
using ConfiguredSpec = decltype(PipelineSpec()
                                    .state(EchoTaskState{})
                                    .allocator(SimpleBufferAllocator{}));
using HandlerSpec =
    decltype(std::declval<ConfiguredSpec>().add<HeadHandler>(head_tag));
static_assert(kCanConfigureState<EmptySpec>);
static_assert(kCanConfigureAllocator<EmptySpec>);
static_assert(kCanConfigureState<ConfiguredSpec>);
static_assert(kCanConfigureAllocator<ConfiguredSpec>);
static_assert(!kCanConfigureState<HandlerSpec>);
static_assert(!kCanConfigureAllocator<HandlerSpec>);
static_assert(!kCanBuild<EmptySpec>);
static_assert(!kCanBuild<ConfiguredSpec>);
static_assert(kCanBuild<HandlerSpec>);
static_assert(kCanReuse<HandlerSpec>);
static_assert(!kCanBuildWith<EmptySpec, InlineExecution>);
static_assert(!kCanReuseWith<ConfiguredSpec, InlineExecution>);
static_assert(kCanBuildWith<HandlerSpec, InlineExecution>);
static_assert(kCanReuseWith<HandlerSpec, InlineExecution>);

PipelineOwner makePipeline(Counters& counters) {
  return PipelineSpec()
      .state(EchoTaskState{.counters = &counters})
      .add<HeadHandler>(head_tag)
      .add<MiddleHandler>(mid_tag)
      .add<TailHandler>(tail_tag)
      .build();
}

struct PipelineSizeHandler : MiddleHandler {
  template <typename C>
  void handlerAdded(C& ctx) noexcept {
    CountingBase::handlerAdded(ctx);
    ctx.taskState().counters->pipelineSize = sizeof(*ctx.pipeline());
  }
};

TEST(PipelineSpecTest, ThreeHandlerSpecFits128ByteAllocation) {
  if constexpr (sizeof(void*) != 8) {
    GTEST_SKIP() << "Allocation budget applies to 64-bit pipelines";
  }
  Counters counters;
  auto pipeline = PipelineSpec()
                      .state(EchoTaskState{&counters})
                      .add<HeadHandler>(head_tag)
                      .add<PipelineSizeHandler>(mid_tag)
                      .add<TailHandler>(tail_tag)
                      .build();
  EXPECT_LE(counters.pipelineSize, 128);
}

TEST(PipelineSpecTest, BuildYieldsErasedOwner) {
  Counters counters;
  PipelineOwner pipeline = makePipeline(counters);
  EXPECT_TRUE(static_cast<bool>(pipeline));
}

TEST(PipelineSpecTest, InlinePipelineHasNoResourceCapabilities) {
  Counters counters;
  auto pipeline = makePipeline(counters);
  EXPECT_EQ(pipeline->eventBase(), nullptr);
  EXPECT_DEATH(
      static_cast<void>(pipeline->allocate(1)),
      "pipeline has no allocation capability");
}

TEST(PipelineSpecTest, HandlerAddedRunsOverEveryHandler) {
  Counters counters;
  auto pipeline = makePipeline(counters);
  EXPECT_EQ(counters.added, 3);
}

TEST(PipelineSpecTest, ActivateRunsOverEveryHandler) {
  Counters counters;
  auto pipeline = makePipeline(counters);
  pipeline->activate();
  EXPECT_EQ(counters.active, 3);
}

TEST(PipelineSpecTest, ReadFlowsToTail) {
  Counters counters;
  auto pipeline = makePipeline(counters);
  pipeline->activate();
  EXPECT_EQ(pipeline->fireRead(TypeErasedBox{}), Result::Success);
  EXPECT_EQ(counters.readsAtTail, 1);
  EXPECT_EQ(counters.writesAtHead, 0);
}

TEST(PipelineSpecTest, WriteFlowsToHead) {
  Counters counters;
  auto pipeline = makePipeline(counters);
  pipeline->activate();
  EXPECT_EQ(pipeline->fireWrite(TypeErasedBox{}), Result::Success);
  EXPECT_EQ(counters.writesAtHead, 1);
  EXPECT_EQ(counters.readsAtTail, 0);
}

// One spec builds more than once: build() is const and reusable, copying the
// blueprint each time, so two independent pipelines result.
TEST(PipelineSpecTest, SpecBuildsMoreThanOnce) {
  Counters counters;
  const auto spec = PipelineSpec()
                        .state(EchoTaskState{&counters})
                        .add<HeadHandler>(head_tag)
                        .add<MiddleHandler>(mid_tag)
                        .add<TailHandler>(tail_tag);
  auto p1 = spec.build();
  auto p2 = spec.build();
  EXPECT_EQ(counters.added, 6);
  p1->activate();
  p2->activate();
  EXPECT_EQ(p1->fireRead(TypeErasedBox{}), Result::Success);
  EXPECT_EQ(p2->fireRead(TypeErasedBox{}), Result::Success);
  EXPECT_EQ(counters.readsAtTail, 2);
}

// Firing past the tail (a forwarding tail) crosses the seam, which is unbound
// on this base and returns Error rather than crashing.
TEST(PipelineSpecTest, ForwardPastTailHitsUnboundSeam) {
  Counters counters;
  PipelineOwner pipeline =
      PipelineSpec()
          .state(EchoTaskState{.counters = &counters})
          .add<HeadHandler>(head_tag)
          .add<MiddleHandler>(tail_tag) // forwards past the end
          .build();
  pipeline->activate();
  EXPECT_EQ(pipeline->fireRead(TypeErasedBox{}), Result::Error);
}

// An empty TaskState carries no storage (held as a [[no_unique_address]]
// member), and ctx.taskState() still resolves to the empty object, so a
// stateless pipeline builds and runs.
struct StatelessTaskState {};
static_assert(std::is_empty_v<StatelessTaskState>);

struct StatelessEcho {
  template <typename C>
  void handlerAdded(C&) noexcept {}
  template <typename C>
  void handlerRemoved(C&) noexcept {}
  template <typename C>
  void onPipelineActive(C&) noexcept {}
  template <typename C>
  void onPipelineInactive(C&) noexcept {}
  template <typename C>
  void onReadReady(C&) noexcept {}
  template <typename C>
  void onWriteReady(C&) noexcept {}
  template <typename C>
  void onException(C&, folly::exception_wrapper&&) noexcept {}
  template <typename C>
  Result onRead(C& ctx, TypeErasedBox&&) noexcept {
    // Touch the empty state to prove no-arg ctx.taskState() resolves for it.
    static_cast<void>(ctx.taskState());
    return Result::Success;
  }
  template <typename C>
  Result onWrite(C&, TypeErasedBox&&) noexcept {
    return Result::Success;
  }
};

TEST(PipelineSpecTest, StatelessTaskStateBuildsAndRuns) {
  PipelineOwner pipeline = PipelineSpec()
                               .state(StatelessTaskState{})
                               .add<StatelessEcho>(head_tag)
                               .build();
  pipeline->activate();
  EXPECT_EQ(pipeline->fireRead(TypeErasedBox{}), Result::Success);
}

TEST(PipelineSpecTest, DefaultResourcesBuildAndRun) {
  auto pipeline = PipelineSpec().add<StatelessEcho>(head_tag).build();
  pipeline->activate();
  EXPECT_EQ(pipeline->fireRead(TypeErasedBox{}), Result::Success);
}

struct ConfiguredTaskState {
  std::vector<int>* observed;
  int calls{0};
};

struct ConfiguredDomain : InlineExecution {
  explicit ConfiguredDomain(int value) : value(value) {}
  int value;
};

struct ConfiguredResourceHandler : StatelessEcho {
  template <typename C>
  Result onRead(C& ctx, TypeErasedBox&&) noexcept {
    auto& state = ctx.taskState();
    state.observed->push_back(++state.calls);
    state.observed->push_back(++ctx.executionDomain().value);
    return Result::Success;
  }
};

TEST(PipelineSpecTest, LambdaCopiesConfiguredResourcesIntoEachPipeline) {
  std::vector<int> observed;
  const auto spec = PipelineSpec()
                        .allocator(SimpleBufferAllocator{})
                        .state(ConfiguredTaskState{&observed, 10})
                        .state(ConfiguredTaskState{&observed})
                        .add<ConfiguredResourceHandler>(head_tag);
  const ConfiguredDomain domain{7};
  auto first = spec.build(domain);
  auto second = spec.build(domain);
  auto makePipeline = [spec, domain] { return spec.build(domain); };
  auto third = makePipeline();
  auto fourth = makePipeline();
  first->activate();
  second->activate();
  third->activate();
  fourth->activate();
  EXPECT_EQ(first->fireRead(TypeErasedBox{}), Result::Success);
  EXPECT_EQ(first->fireRead(TypeErasedBox{}), Result::Success);
  EXPECT_EQ(second->fireRead(TypeErasedBox{}), Result::Success);
  EXPECT_EQ(third->fireRead(TypeErasedBox{}), Result::Success);
  EXPECT_EQ(fourth->fireRead(TypeErasedBox{}), Result::Success);
  EXPECT_EQ(observed, (std::vector<int>{1, 8, 2, 9, 1, 8, 1, 8, 1, 8}));
  EXPECT_EQ(domain.value, 7);
}

struct DefaultConfiguredDomain : InlineExecution {
  int value{30};
};

TEST(PipelineSpecTest, BuildSelectsExecutionDomainPerPipeline) {
  std::vector<int> observed;
  const auto spec = PipelineSpec()
                        .state(ConfiguredTaskState{&observed})
                        .add<ConfiguredResourceHandler>(head_tag);
  auto first = spec.build(ConfiguredDomain{7});
  auto second = spec.build<ConfiguredDomain>(ConfiguredDomain{21});
  auto third = spec.build<DefaultConfiguredDomain>();
  for (auto* pipeline : {&first, &second, &third}) {
    (*pipeline)->activate();
    EXPECT_EQ((*pipeline)->fireRead(TypeErasedBox{}), Result::Success);
  }
  EXPECT_EQ(observed, (std::vector<int>{1, 8, 1, 22, 1, 31}));
}

struct OwnedTaskResource {
  std::vector<std::string>* lifetime;

  ~OwnedTaskResource() { lifetime->emplace_back("resource"); }
};

struct OwnedTaskState {
  std::unique_ptr<OwnedTaskResource> resource;
};

struct MoveOnlyDomain : InlineExecution {
  explicit MoveOnlyDomain(std::unique_ptr<int> value)
      : value(std::move(value)) {}
  std::unique_ptr<int> value;
};

static_assert(kCanReuseWith<HandlerSpec, MoveOnlyDomain>);
static_assert(!std::copy_constructible<MoveOnlyDomain>);
static_assert(!kCanBuildWith<HandlerSpec, std::unique_ptr<int>>);
static_assert(!kCanReuseWith<HandlerSpec, std::unique_ptr<int>>);

struct MoveOnlyDomainHandler : StatelessEcho {
  template <typename C>
  Result onRead(C& ctx, TypeErasedBox&&) noexcept {
    static_assert(
        std::same_as<decltype(ctx.executionDomain()), MoveOnlyDomain&>);
    static_assert(std::same_as<
                  decltype(std::as_const(ctx).executionDomain()),
                  const MoveOnlyDomain&>);
    auto& state = ctx.taskState();
    state.observed->push_back(++state.calls);
    state.observed->push_back(++*ctx.executionDomain().value);
    return Result::Success;
  }
};

TEST(PipelineSpecTest, ConstBuildAcceptsIndependentMoveOnlyDomains) {
  std::vector<int> observed;
  const auto spec = PipelineSpec()
                        .state(ConfiguredTaskState{&observed})
                        .add<MoveOnlyDomainHandler>(head_tag);
  auto domain = MoveOnlyDomain{std::make_unique<int>(7)};
  auto first = spec.build(std::move(domain));
  // NOLINTNEXTLINE(bugprone-use-after-move): verifies the moved-from contract.
  EXPECT_FALSE(domain.value);
  auto second = spec.build(MoveOnlyDomain{std::make_unique<int>(42)});
  first->activate();
  second->activate();
  EXPECT_EQ(first->fireRead(TypeErasedBox{}), Result::Success);
  EXPECT_EQ(first->fireRead(TypeErasedBox{}), Result::Success);
  EXPECT_EQ(second->fireRead(TypeErasedBox{}), Result::Success);
  EXPECT_EQ(observed, (std::vector<int>{1, 8, 2, 9, 1, 43}));
}

using MoveOnlySpec = decltype(PipelineSpec()
                                  .state(OwnedTaskState{})
                                  .add<StatelessEcho>(head_tag));
static_assert(kCanBuild<MoveOnlySpec>);
static_assert(!kCanReuse<MoveOnlySpec>);
static_assert(kCanBuildWith<MoveOnlySpec, MoveOnlyDomain>);
static_assert(!kCanReuseWith<MoveOnlySpec, MoveOnlyDomain>);

struct MoveOnlyResourceHandler : StatelessEcho {
  template <typename C>
  Result onRead(C& ctx, TypeErasedBox&&) noexcept {
    ctx.taskState().resource->lifetime->push_back(
        std::to_string(*ctx.executionDomain().value));
    return Result::Success;
  }
};

TEST(PipelineSpecTest, RvalueBuildMovesConfiguredStateAndDomain) {
  std::vector<std::string> lifetime;
  auto state = OwnedTaskState{
      std::unique_ptr<OwnedTaskResource>(new OwnedTaskResource{&lifetime})};
  auto domain = MoveOnlyDomain{std::make_unique<int>(42)};
  auto pipeline = PipelineSpec()
                      .state(std::move(state))
                      .add<MoveOnlyResourceHandler>(head_tag)
                      .build(std::move(domain));
  pipeline->activate();
  EXPECT_EQ(pipeline->fireRead(TypeErasedBox{}), Result::Success);
  EXPECT_EQ(lifetime, (std::vector<std::string>{"42"}));
  pipeline.reset();
  EXPECT_EQ(lifetime, (std::vector<std::string>{"42", "resource"}));
}

struct ResourceLifetimeHandler : StatelessEcho {
  OwnedTaskResource* resource{nullptr};

  template <typename C>
  void handlerAdded(C& ctx) noexcept {
    resource = ctx.taskState().resource.get();
  }
  template <typename C>
  void handlerRemoved(C&) noexcept {
    resource->lifetime->emplace_back("removed");
  }
  ~ResourceLifetimeHandler() {
    if (resource != nullptr) {
      resource->lifetime->emplace_back("handler");
    }
  }
};

struct AdapterResource {
  explicit AdapterResource(std::vector<std::string>& lifetime)
      : lifetime(&lifetime) {}
  ~AdapterResource() { lifetime->emplace_back("adapter"); }
  std::vector<std::string>* lifetime;
};

struct AllocatorResource {
  std::vector<std::string>* lifetime;
  ~AllocatorResource() { lifetime->emplace_back("allocator"); }
};

struct OwnedBufferAllocator : SimpleBufferAllocator {
  std::unique_ptr<AllocatorResource> resource;
};

struct LifetimeReadAdapter {
  std::unique_ptr<AdapterResource> resource;
  Result adaptRead(TypeErasedBox&&) noexcept { return Result::Success; }
};

TEST(
    PipelineSpecTest,
    GuardRetainsAdaptersStateAndAllocatorThroughHandlerDestruction) {
  std::vector<std::string> lifetime;
  auto pipeline = PipelineSpec()
                      .state(
                          OwnedTaskState{std::unique_ptr<OwnedTaskResource>(
                              new OwnedTaskResource{&lifetime})})
                      .allocator(
                          OwnedBufferAllocator{
                              .resource = std::unique_ptr<AllocatorResource>(
                                  new AllocatorResource{&lifetime})})
                      .add<ResourceLifetimeHandler>(head_tag)
                      .build();
  ASSERT_TRUE(pipeline->adaptRead(
      LifetimeReadAdapter{std::make_unique<AdapterResource>(lifetime)}));
  auto moved = std::move(pipeline);
  EXPECT_FALSE(pipeline);
  auto ref = moved.ref();
  auto guard = ref.guard();
  auto movedGuard = std::move(guard);
  EXPECT_FALSE(guard);

  moved.reset();
  ref.reset();
  EXPECT_FALSE(ref);
  EXPECT_TRUE(lifetime.empty());

  movedGuard.reset();
  EXPECT_EQ(
      lifetime,
      (std::vector<std::string>{
          "removed", "handler", "adapter", "resource", "allocator"}));
}

// Outbound-only endpoint: terminates writes, has no onRead. Satisfies
// OutboundHandler but not InboundHandler.
struct WriteOnlyHead : CountingBase {
  template <typename C>
  Result onWrite(C& ctx, TypeErasedBox&&) noexcept {
    ++ctx.taskState().counters->writesAtHead;
    return Result::Success;
  }
};

// Inbound-only endpoint: terminates reads, has no onWrite. Satisfies
// InboundHandler but not OutboundHandler.
struct ReadOnlyTail : CountingBase {
  template <typename C>
  Result onRead(C& ctx, TypeErasedBox&&) noexcept {
    ++ctx.taskState().counters->readsAtTail;
    return Result::Success;
  }
};

// One-direction handlers are each skipped on their opposite path: a read
// reaches the inbound-only tail past the outbound-only head, and a write
// reaches the outbound-only head past the inbound-only tail.
TEST(PipelineSpecTest, AsymmetricHandlersSkipOppositeDirection) {
  Counters counters;
  PipelineOwner pipeline = PipelineSpec()
                               .state(EchoTaskState{.counters = &counters})
                               .add<WriteOnlyHead>(head_tag)
                               .add<ReadOnlyTail>(tail_tag)
                               .build();
  pipeline->activate();

  EXPECT_EQ(pipeline->fireRead(TypeErasedBox{}), Result::Success);
  EXPECT_EQ(counters.readsAtTail, 1);
  EXPECT_EQ(counters.writesAtHead, 0);

  EXPECT_EQ(pipeline->fireWrite(TypeErasedBox{}), Result::Success);
  EXPECT_EQ(counters.writesAtHead, 1);
  EXPECT_EQ(counters.readsAtTail, 1);
}

struct MessageSink {
  std::vector<std::unique_ptr<int>> messages;
  Result result{Result::Success};

  Result accept(TypeErasedBox&& msg) noexcept {
    messages.push_back(msg.take<std::unique_ptr<int>>());
    return result;
  }
};

struct ForwardingHandler : StatelessEcho {
  template <typename C>
  Result onRead(C& ctx, TypeErasedBox&& msg) noexcept {
    return ctx.fireRead(std::move(msg));
  }
  template <typename C>
  Result onWrite(C& ctx, TypeErasedBox&& msg) noexcept {
    return ctx.fireWrite(std::move(msg));
  }
};

struct EventBaseReadAdapter {
  folly::EventBase* eventBase;
  MessageSink* sink;

  Result adaptRead(TypeErasedBox&& msg) noexcept {
    eventBase->runInLoop([sink = sink, msg = std::move(msg)]() mutable {
      static_cast<void>(sink->accept(std::move(msg)));
    });
    return Result::Success;
  }
};

TEST(PipelineSpecTest, ReadAdapterCanQueueDeliveryOnExternalEventBase) {
  MessageSink sink;
  folly::EventBase eventBase;
  const auto spec = PipelineSpec()
                        .state(StatelessTaskState{})
                        .add<ForwardingHandler>(tail_tag);
  auto pipeline = spec.build();
  ASSERT_TRUE(pipeline->adaptRead(EventBaseReadAdapter{&eventBase, &sink}));
  pipeline->activate();
  auto message = std::make_unique<int>(42);
  auto* original = message.get();
  EXPECT_EQ(
      pipeline->fireRead(erase_and_box(std::move(message))), Result::Success);
  EXPECT_TRUE(sink.messages.empty());
  pipeline.reset();

  EXPECT_TRUE(eventBase.loopOnce());
  ASSERT_EQ(sink.messages.size(), 1);
  EXPECT_EQ(sink.messages.front().get(), original);
  EXPECT_EQ(*sink.messages.front(), 42);
}

struct ExecutorReadAdapter {
  folly::Executor* executor;
  MessageSink* sink;

  Result adaptRead(TypeErasedBox&& msg) noexcept {
    executor->add([sink = sink, msg = std::move(msg)]() mutable {
      static_cast<void>(sink->accept(std::move(msg)));
    });
    return Result::Success;
  }
};

TEST(PipelineSpecTest, SpecInstancesCanBindDifferentExecutorAdapters) {
  MessageSink a;
  MessageSink b;
  folly::ManualExecutor executorA;
  folly::ManualExecutor executorB;
  const auto spec = PipelineSpec()
                        .state(StatelessTaskState{})
                        .add<ForwardingHandler>(tail_tag);
  auto p1 = spec.build();
  auto p2 = spec.build();
  ASSERT_TRUE(p1->adaptRead(ExecutorReadAdapter{&executorA, &a}));
  ASSERT_TRUE(p2->adaptRead(ExecutorReadAdapter{&executorB, &b}));
  p1->activate();
  p2->activate();
  EXPECT_EQ(
      p1->fireRead(erase_and_box(std::make_unique<int>(11))), Result::Success);
  EXPECT_EQ(
      p2->fireRead(erase_and_box(std::make_unique<int>(22))), Result::Success);
  EXPECT_TRUE(a.messages.empty());
  EXPECT_TRUE(b.messages.empty());

  EXPECT_EQ(executorA.drain(), 1);
  ASSERT_EQ(a.messages.size(), 1);
  EXPECT_EQ(*a.messages.front(), 11);
  EXPECT_TRUE(b.messages.empty());

  EXPECT_EQ(executorB.drain(), 1);
  ASSERT_EQ(b.messages.size(), 1);
  EXPECT_EQ(*b.messages.front(), 22);
}

struct CountingBufferAllocator : SimpleBufferAllocator {
  int* copies;

  BytesPtr copyBuffer(const void* data, size_t size) {
    ++*copies;
    return SimpleBufferAllocator::copyBuffer(data, size);
  }
};

struct AllocatorTaskState {
  BytesPtr* received;
};

struct AllocatingTail : StatelessEcho {
  template <typename C>
  Result onRead(C& ctx, TypeErasedBox&& msg) noexcept {
    auto& task = ctx.taskState();
    const auto& value = msg.template get<std::string>();
    *task.received = ctx.copyBuffer(value.data(), value.size());
    return Result::Success;
  }
};

TEST(PipelineSpecTest, SpecCopiesConfiguredAllocator) {
  int copies{0};
  BytesPtr received;
  const auto spec = PipelineSpec()
                        .state(AllocatorTaskState{.received = &received})
                        .allocator(CountingBufferAllocator{.copies = &copies})
                        .add<AllocatingTail>(tail_tag);
  auto p1 = spec.build();
  auto p2 = spec.build();
  p1->activate();
  p2->activate();

  EXPECT_EQ(p1->fireRead(erase_and_box(std::string("first"))), Result::Success);
  EXPECT_EQ(copies, 1);
  ASSERT_TRUE(received);
  EXPECT_EQ(received->moveToFbString().toStdString(), "first");

  EXPECT_EQ(
      p2->fireRead(erase_and_box(std::string("second"))), Result::Success);
  EXPECT_EQ(copies, 2);
  ASSERT_TRUE(received);
  EXPECT_EQ(received->moveToFbString().toStdString(), "second");

  auto buffer = p1->allocate(32);
  ASSERT_TRUE(buffer);
  EXPECT_GE(buffer->capacity(), 32);
}

struct SinkTaskState {
  MessageSink* reads;
  MessageSink* writes;
};

struct CollectingEndpoint : StatelessEcho {
  template <typename C>
  Result onRead(C& ctx, TypeErasedBox&& msg) noexcept {
    return ctx.taskState().reads->accept(std::move(msg));
  }
  template <typename C>
  Result onWrite(C& ctx, TypeErasedBox&& msg) noexcept {
    return ctx.taskState().writes->accept(std::move(msg));
  }
};

struct ForwardingReadAdapter {
  // The destination owner must outlive this adapter's use of the reference.
  PipelineRef destination;
  std::unique_ptr<int> calls;

  Result adaptRead(TypeErasedBox&& msg) noexcept {
    ++*calls;
    return destination.fireRead(std::move(msg));
  }
};

struct ForwardingWriteAdapter {
  PipelineRef destination;
  std::unique_ptr<int> calls;

  Result adaptWrite(TypeErasedBox&& msg) noexcept {
    ++*calls;
    return destination.fireWrite(std::move(msg));
  }
};

static_assert(ReadAdapter<ForwardingReadAdapter>);
static_assert(!WriteAdapter<ForwardingReadAdapter>);
static_assert(WriteAdapter<ForwardingWriteAdapter>);
static_assert(!ReadAdapter<ForwardingWriteAdapter>);

struct ThrowingReadAdapter {
  Result adaptRead(TypeErasedBox&&);
};
struct WrongResultWriteAdapter {
  void adaptWrite(TypeErasedBox&&) noexcept;
};
static_assert(!ReadAdapter<ThrowingReadAdapter>);
static_assert(!WriteAdapter<WrongResultWriteAdapter>);

TEST(PipelineSpecTest, ReadAdapterContinuesMessagesToAnotherPipeline) {
  MessageSink reads;
  MessageSink writes;
  auto destination =
      PipelineSpec()
          .state(SinkTaskState{.reads = &reads, .writes = &writes})
          .add<CollectingEndpoint>(tail_tag)
          .build();
  auto source = PipelineSpec()
                    .state(StatelessTaskState{})
                    .add<ForwardingHandler>(tail_tag)
                    .build();
  auto calls = std::make_unique<int>(0);
  auto* observedCalls = calls.get();
  ASSERT_TRUE(source->adaptRead(
      ForwardingReadAdapter{destination.ref(), std::move(calls)}));
  destination->activate();
  source->activate();

  for (auto result : {Result::Success, Result::Backpressure, Result::Error}) {
    reads.result = result;
    auto message = std::make_unique<int>(42);
    auto* original = message.get();
    EXPECT_EQ(source->fireRead(erase_and_box(std::move(message))), result);
    ASSERT_FALSE(reads.messages.empty());
    EXPECT_EQ(reads.messages.back().get(), original);
    EXPECT_EQ(*reads.messages.back(), 42);
  }
  EXPECT_EQ(reads.messages.size(), 3);
  EXPECT_EQ(*observedCalls, 3);
  EXPECT_TRUE(writes.messages.empty());
}

TEST(PipelineSpecTest, WriteAdapterContinuesMessagesToAnotherPipeline) {
  MessageSink reads;
  MessageSink writes;
  auto destination =
      PipelineSpec()
          .state(SinkTaskState{.reads = &reads, .writes = &writes})
          .add<CollectingEndpoint>(head_tag)
          .build();
  auto source = PipelineSpec()
                    .state(StatelessTaskState{})
                    .add<ForwardingHandler>(head_tag)
                    .build();
  auto calls = std::make_unique<int>(0);
  auto* observedCalls = calls.get();
  ASSERT_TRUE(source->adaptWrite(
      ForwardingWriteAdapter{destination.ref(), std::move(calls)}));
  destination->activate();
  source->activate();

  for (auto result : {Result::Success, Result::Backpressure, Result::Error}) {
    writes.result = result;
    auto message = std::make_unique<int>(7);
    auto* original = message.get();
    EXPECT_EQ(source->fireWrite(erase_and_box(std::move(message))), result);
    ASSERT_FALSE(writes.messages.empty());
    EXPECT_EQ(writes.messages.back().get(), original);
    EXPECT_EQ(*writes.messages.back(), 7);
  }
  EXPECT_EQ(writes.messages.size(), 3);
  EXPECT_EQ(*observedCalls, 3);
  EXPECT_TRUE(reads.messages.empty());
}

struct AdapterObservation {
  int reads{0};
  int writes{0};
  int destroyed{0};
  Result result{Result::Success};
};

struct AdapterState {
  explicit AdapterState(AdapterObservation& observation)
      : observation(&observation) {}
  ~AdapterState() { ++observation->destroyed; }
  AdapterObservation* observation;
};

struct CountingAdapter {
  std::unique_ptr<AdapterState> state;

  Result adaptRead(TypeErasedBox&&) noexcept {
    ++state->observation->reads;
    return state->observation->result;
  }
  Result adaptWrite(TypeErasedBox&&) noexcept {
    ++state->observation->writes;
    return state->observation->result;
  }
};

TEST(PipelineSpecTest, ReadAndWriteAdaptersOwnIndependentMoveOnlyState) {
  AdapterObservation reads;
  AdapterObservation writes;
  auto pipeline = PipelineSpec()
                      .state(StatelessTaskState{})
                      .add<ForwardingHandler>(head_tag)
                      .build();
  EXPECT_EQ(pipeline->fireRead(TypeErasedBox{}), Result::Error);
  EXPECT_EQ(pipeline->fireWrite(TypeErasedBox{}), Result::Error);
  ASSERT_TRUE(pipeline->adaptRead(
      CountingAdapter{std::make_unique<AdapterState>(reads)}));
  ASSERT_TRUE(pipeline->adaptWrite(
      CountingAdapter{std::make_unique<AdapterState>(writes)}));
  pipeline->activate();

  for (auto result : {Result::Success, Result::Backpressure, Result::Error}) {
    reads.result = result;
    writes.result = result;
    EXPECT_EQ(pipeline->fireRead(TypeErasedBox{}), result);
    EXPECT_EQ(pipeline->fireWrite(TypeErasedBox{}), result);
  }
  EXPECT_EQ(reads.reads, 3);
  EXPECT_EQ(reads.writes, 0);
  EXPECT_EQ(writes.reads, 0);
  EXPECT_EQ(writes.writes, 3);
  EXPECT_EQ(reads.destroyed, 0);
  EXPECT_EQ(writes.destroyed, 0);
  pipeline.reset();
  EXPECT_EQ(reads.destroyed, 1);
  EXPECT_EQ(writes.destroyed, 1);
}

TEST(PipelineSpecTest, InactiveInstanceCanReplaceAdapterAfterGuardsRelease) {
  AdapterObservation original;
  AdapterObservation replacement;
  AdapterObservation rejected;
  auto pipeline = PipelineSpec()
                      .state(StatelessTaskState{})
                      .add<ForwardingHandler>(head_tag)
                      .build();
  ASSERT_TRUE(pipeline->adaptRead(
      CountingAdapter{std::make_unique<AdapterState>(original)}));
  auto guard = pipeline->guard();
  EXPECT_FALSE(pipeline->adaptRead(
      CountingAdapter{std::make_unique<AdapterState>(rejected)}));
  EXPECT_EQ(rejected.destroyed, 1);
  EXPECT_EQ(original.destroyed, 0);
  guard.reset();

  ASSERT_TRUE(pipeline->adaptRead(
      CountingAdapter{std::make_unique<AdapterState>(replacement)}));
  EXPECT_EQ(original.destroyed, 1);
  EXPECT_EQ(pipeline->fireRead(TypeErasedBox{}), Result::Success);
  EXPECT_EQ(original.reads, 0);
  EXPECT_EQ(replacement.reads, 1);
  pipeline.reset();
  EXPECT_EQ(replacement.destroyed, 1);
}

TEST(PipelineSpecTest, ActiveAndClosedInstancesRejectAdapters) {
  AdapterObservation original;
  AdapterObservation rejected;
  auto pipeline = PipelineSpec()
                      .state(StatelessTaskState{})
                      .add<ForwardingHandler>(head_tag)
                      .build();
  ASSERT_TRUE(pipeline->adaptRead(
      CountingAdapter{std::make_unique<AdapterState>(original)}));
  pipeline->activate();
  EXPECT_FALSE(pipeline->adaptRead(
      CountingAdapter{std::make_unique<AdapterState>(rejected)}));
  EXPECT_FALSE(pipeline->adaptWrite(
      CountingAdapter{std::make_unique<AdapterState>(rejected)}));
  EXPECT_EQ(rejected.destroyed, 2);
  EXPECT_EQ(pipeline->fireRead(TypeErasedBox{}), Result::Success);
  EXPECT_EQ(original.reads, 1);

  pipeline->close();
  EXPECT_FALSE(pipeline->adaptRead(
      CountingAdapter{std::make_unique<AdapterState>(rejected)}));
  EXPECT_FALSE(pipeline->adaptWrite(
      CountingAdapter{std::make_unique<AdapterState>(rejected)}));
  EXPECT_EQ(rejected.destroyed, 4);
  EXPECT_EQ(pipeline->fireRead(TypeErasedBox{}), Result::Error);
  EXPECT_EQ(pipeline->fireWrite(TypeErasedBox{}), Result::Error);
  EXPECT_EQ(original.reads, 1);
}

struct RebindingReadAdapter {
  PipelineRef pipeline;
  AdapterObservation* candidate;
  bool* accepted;

  Result adaptRead(TypeErasedBox&&) noexcept {
    *accepted = pipeline.adaptRead(
        CountingAdapter{std::make_unique<AdapterState>(*candidate)});
    return Result::Backpressure;
  }
};

TEST(PipelineSpecTest, AdapterCannotReplaceItselfDuringDispatch) {
  AdapterObservation candidate;
  bool accepted{true};
  auto pipeline = PipelineSpec()
                      .state(StatelessTaskState{})
                      .add<ForwardingHandler>(head_tag)
                      .build();
  ASSERT_TRUE(pipeline->adaptRead(
      RebindingReadAdapter{pipeline.ref(), &candidate, &accepted}));
  // Even an inactive instance is guarded while delivering a message.
  EXPECT_EQ(pipeline->fireRead(TypeErasedBox{}), Result::Backpressure);
  EXPECT_FALSE(accepted);
  EXPECT_EQ(candidate.destroyed, 1);
  EXPECT_EQ(pipeline->fireRead(TypeErasedBox{}), Result::Backpressure);
  EXPECT_EQ(candidate.destroyed, 2);
  EXPECT_EQ(candidate.reads, 0);
}

struct ReleasingReadAdapter {
  PipelineOwner* owner;
  std::unique_ptr<AdapterResource> resource;

  Result adaptRead(TypeErasedBox&&) noexcept {
    owner->reset();
    resource->lifetime->emplace_back("callback finished");
    return Result::Backpressure;
  }
};

TEST(PipelineSpecTest, AdapterSurvivesOwnerReleaseInsideCallback) {
  std::vector<std::string> lifetime;
  auto pipeline = PipelineSpec()
                      .state(StatelessTaskState{})
                      .add<ForwardingHandler>(head_tag)
                      .build();
  ASSERT_TRUE(pipeline->adaptRead(
      ReleasingReadAdapter{
          &pipeline, std::make_unique<AdapterResource>(lifetime)}));
  auto ref = pipeline.ref();
  EXPECT_EQ(ref.fireRead(TypeErasedBox{}), Result::Backpressure);
  EXPECT_FALSE(pipeline);
  EXPECT_EQ(
      lifetime, (std::vector<std::string>{"callback finished", "adapter"}));
}

struct OwningReadAdapter {
  PipelineOwner destination;
  Result adaptRead(TypeErasedBox&& msg) noexcept {
    return destination->fireRead(std::move(msg));
  }
};

TEST(PipelineSpecTest, AdapterCanOwnDestinationPipeline) {
  MessageSink reads;
  MessageSink writes;
  auto destination = PipelineSpec()
                         .state(SinkTaskState{&reads, &writes})
                         .add<CollectingEndpoint>(head_tag)
                         .build();
  destination->activate();
  auto source = PipelineSpec()
                    .state(StatelessTaskState{})
                    .add<ForwardingHandler>(head_tag)
                    .build();
  ASSERT_TRUE(source->adaptRead(OwningReadAdapter{std::move(destination)}));
  EXPECT_FALSE(destination);
  EXPECT_EQ(
      source->fireRead(erase_and_box(std::make_unique<int>(7))),
      Result::Success);
  ASSERT_EQ(reads.messages.size(), 1);
  EXPECT_EQ(*reads.messages.front(), 7);
  EXPECT_TRUE(writes.messages.empty());
}

struct RoutedRequest {
  std::unique_ptr<int> payload;
  PipelineRef replyTo;
};
static_assert(sizeof(RoutedRequest) <= 120);
static_assert(alignof(RoutedRequest) <= 8);
static_assert(std::is_nothrow_move_constructible_v<RoutedRequest>);

struct SharedApplicationTaskState {
  std::vector<RoutedRequest> pending;
};
struct ReplyPending : EventTag<> {};

struct SharedApplicationHandler : ForwardingHandler {
  using SubscribedEvents = Events<ReplyPending>;

  template <typename C>
  Result onRead(C& ctx, TypeErasedBox&& msg) noexcept {
    ctx.taskState().pending.push_back(msg.template take<RoutedRequest>());
    return Result::Success;
  }

  template <typename E, typename C>
    requires std::same_as<E, ReplyPending>
  void on(C& ctx) noexcept {
    auto& pending = ctx.taskState().pending;
    while (!pending.empty()) {
      auto reply = std::move(pending.back());
      pending.pop_back();
      static_cast<void>(ctx.fireWrite(erase_and_box(std::move(reply))));
    }
  }
};

struct ConnectionReplyState {
  MessageSink* replies;
};

struct ConnectionHandler : ForwardingHandler {
  template <typename C>
  Result onWrite(C& ctx, TypeErasedBox&& msg) noexcept {
    return ctx.taskState().replies->accept(std::move(msg));
  }
};

struct RequestRouter {
  PipelineRef application;
  PipelineRef replyTo;

  Result adaptRead(TypeErasedBox&& msg) noexcept {
    return application.fireRead(erase_and_box(
        RoutedRequest{msg.take<std::unique_ptr<int>>(), replyTo}));
  }
};

struct ReplyRouter {
  std::vector<int>* order;

  Result adaptWrite(TypeErasedBox&& msg) noexcept {
    auto reply = msg.take<RoutedRequest>();
    order->push_back(*reply.payload);
    return reply.replyTo.fireWrite(erase_and_box(std::move(reply.payload)));
  }
};

TEST(PipelineSpecTest, SharedApplicationRoutesOutOfOrderRepliesToConnections) {
  std::vector<int> order;
  MessageSink repliesA;
  MessageSink repliesB;
  auto application = PipelineSpec()
                         .state(SharedApplicationTaskState{})
                         .add<SharedApplicationHandler>(head_tag)
                         .build();
  ASSERT_TRUE(application->adaptWrite(ReplyRouter{&order}));
  auto makeConnection = [](MessageSink& replies) {
    return PipelineSpec()
        .state(ConnectionReplyState{&replies})
        .add<ConnectionHandler>(head_tag)
        .build();
  };
  auto connectionA = makeConnection(repliesA);
  auto connectionB = makeConnection(repliesB);
  ASSERT_TRUE(connectionA->adaptRead(
      RequestRouter{application.ref(), connectionA.ref()}));
  ASSERT_TRUE(connectionB->adaptRead(
      RequestRouter{application.ref(), connectionB.ref()}));
  application->activate();
  connectionA->activate();
  connectionB->activate();

  auto requestA = std::make_unique<int>(11);
  auto requestB = std::make_unique<int>(22);
  auto* originalA = requestA.get();
  auto* originalB = requestB.get();
  EXPECT_EQ(
      connectionA->fireRead(erase_and_box(std::move(requestA))),
      Result::Success);
  EXPECT_EQ(
      connectionB->fireRead(erase_and_box(std::move(requestB))),
      Result::Success);
  EXPECT_TRUE(repliesA.messages.empty());
  EXPECT_TRUE(repliesB.messages.empty());

  application->bindEvents<Events<ReplyPending>>().fire<ReplyPending>();
  EXPECT_EQ(order, (std::vector<int>{22, 11}));
  ASSERT_EQ(repliesA.messages.size(), 1);
  ASSERT_EQ(repliesB.messages.size(), 1);
  EXPECT_EQ(repliesA.messages.front().get(), originalA);
  EXPECT_EQ(repliesB.messages.front().get(), originalB);
}

// Exceptions are delivered only to handlers satisfying the full InboundHandler
// concept (consistent with the other pipelines): an outbound-only handler is
// skipped even though it implements onException, and the exception reaches the
// first inbound handler.
TEST(PipelineSpecTest, ExceptionSkipsOutboundOnlyHandler) {
  Counters counters;
  auto pipeline = PipelineSpec()
                      .state(EchoTaskState{.counters = &counters})
                      .add<OutboundExceptionSpy>(head_tag)
                      .add<InboundExceptionSpy>(tail_tag)
                      .build();
  pipeline->activate();
  pipeline->fireException(
      folly::make_exception_wrapper<std::runtime_error>("boom"));
  EXPECT_EQ(counters.inboundExceptions, 1);
  EXPECT_EQ(counters.outboundExceptions, 0);
}

// A handler that armed write-readiness is woken by onWriteReady(), and once it
// cancels it is not woken again.
TEST(PipelineSpecTest, WriteReadyNotifiesArmedHandler) {
  Counters counters;
  auto pipeline = PipelineSpec()
                      .state(EchoTaskState{.counters = &counters})
                      .add<HeadHandler>(head_tag)
                      .add<WriteBackpressureHandler>(mid_tag)
                      .add<TailHandler>(tail_tag)
                      .build();
  pipeline->activate();

  EXPECT_EQ(pipeline->fireWrite(TypeErasedBox{}), Result::Success);
  EXPECT_EQ(counters.writeArmed, 1);
  EXPECT_EQ(counters.writeReady, 0);

  pipeline->onWriteReady();
  EXPECT_EQ(counters.writeReady, 1);

  // The woken handler cancelled; a second onWriteReady finds nothing armed.
  pipeline->onWriteReady();
  EXPECT_EQ(counters.writeReady, 1);
}

TEST(PipelineSpecTest, ReadAndWriteReadinessAreIndependentInOneHandler) {
  Counters counters;
  auto pipeline = PipelineSpec()
                      .state(EchoTaskState{.counters = &counters})
                      .add<HeadHandler>(head_tag)
                      .add<DuplexBackpressureHandler>(mid_tag)
                      .add<TailHandler>(tail_tag)
                      .build();
  pipeline->activate();

  EXPECT_EQ(pipeline->fireWrite(TypeErasedBox{}), Result::Success);
  EXPECT_EQ(pipeline->fireRead(TypeErasedBox{}), Result::Success);
  EXPECT_EQ(counters.writeArmed, 1);
  EXPECT_EQ(counters.readArmed, 1);

  pipeline->onWriteReady();
  EXPECT_EQ(counters.writeReady, 1);
  EXPECT_EQ(counters.readReady, 0);

  pipeline->onReadReady();
  EXPECT_EQ(counters.writeReady, 1);
  EXPECT_EQ(counters.readReady, 1);

  pipeline->onWriteReady();
  pipeline->onReadReady();
  EXPECT_EQ(counters.writeReady, 1);
  EXPECT_EQ(counters.readReady, 1);
}

// The read-side mirror: a handler that armed read-readiness is woken by
// onReadReady(), and once it cancels it is not woken again.
TEST(PipelineSpecTest, ReadReadyNotifiesArmedHandler) {
  Counters counters;
  auto pipeline = PipelineSpec()
                      .state(EchoTaskState{.counters = &counters})
                      .add<HeadHandler>(head_tag)
                      .add<ReadBackpressureHandler>(mid_tag)
                      .add<TailHandler>(tail_tag)
                      .build();
  pipeline->activate();

  EXPECT_EQ(pipeline->fireRead(TypeErasedBox{}), Result::Success);
  EXPECT_EQ(counters.readArmed, 1);
  EXPECT_EQ(counters.readReady, 0);

  pipeline->onReadReady();
  EXPECT_EQ(counters.readReady, 1);

  pipeline->onReadReady();
  EXPECT_EQ(counters.readReady, 1);
}

// An in-pipeline publisher's fireEvent reaches a subscribing handler.
TEST(PipelineSpecTest, EventBroadcastsToSubscriber) {
  Counters counters;
  auto pipeline = PipelineSpec()
                      .state(EchoTaskState{.counters = &counters})
                      .add<EventPublisher>(head_tag)
                      .add<EventSubscriber>(mid_tag)
                      .add<TailHandler>(tail_tag)
                      .build();
  pipeline->activate();
  EXPECT_EQ(pipeline->fireRead(TypeErasedBox{}), Result::Success);
  EXPECT_EQ(counters.eventSum, 7);
}

// An external publisher reaches the subscriber through the erased handle's
// bindEvents/fireBoundEvent path.
TEST(PipelineSpecTest, ExternalPublisherReachesSubscriber) {
  Counters counters;
  auto pipeline = PipelineSpec()
                      .state(EchoTaskState{.counters = &counters})
                      .add<HeadHandler>(head_tag)
                      .add<EventSubscriber>(mid_tag)
                      .add<TailHandler>(tail_tag)
                      .build();
  pipeline->activate();
  pipeline->bindEvents<Events<ValueEvent>>().fire<ValueEvent>(5);
  EXPECT_EQ(counters.eventSum, 5);
}

struct InlineTaskState {
  int* calls;
  const InlineExecution** observedDomain;
  Result result;
};

struct InlineResultHandler : StatelessEcho {
  template <typename C>
  Result onRead(C& ctx, TypeErasedBox&&) noexcept {
    static_assert(std::same_as<decltype(ctx.taskState()), InlineTaskState&>);
    static_assert(std::same_as<
                  decltype(std::as_const(ctx).taskState()),
                  const InlineTaskState&>);
    static_assert(
        std::same_as<decltype(ctx.executionDomain()), InlineExecution&>);
    static_assert(std::same_as<
                  decltype(std::as_const(ctx).executionDomain()),
                  const InlineExecution&>);
    auto& task = ctx.taskState();
    ++*task.calls;
    *task.observedDomain = &ctx.executionDomain();
    return task.result;
  }

  template <typename C>
  Result onWrite(C& ctx, TypeErasedBox&& msg) noexcept {
    return onRead(ctx, std::move(msg));
  }
};

TEST(PipelineSpecTest, InlineExecutionReturnsHandlerResultSynchronously) {
  for (auto result : {Result::Success, Result::Backpressure, Result::Error}) {
    int calls{0};
    const InlineExecution* observedDomain{nullptr};
    auto pipeline = PipelineSpec()
                        .state(InlineTaskState{&calls, &observedDomain, result})
                        .add<InlineResultHandler>(head_tag)
                        .build();
    pipeline->activate();

    EXPECT_EQ(pipeline->fireRead(TypeErasedBox{}), result);
    EXPECT_EQ(calls, 1);
    ASSERT_NE(observedDomain, nullptr);
    const auto* readDomain = observedDomain;

    EXPECT_EQ(pipeline->fireWrite(TypeErasedBox{}), result);
    EXPECT_EQ(calls, 2);
    EXPECT_EQ(observedDomain, readDomain);
  }
}

struct RvalueOnlyInlineTask {
  std::unique_ptr<int> value;
  int* observed;

  void operator()() && noexcept { *observed = *value; }
};

static_assert(std::is_empty_v<InlineExecution>);
static_assert(noexcept(std::declval<InlineExecution&>().post([] {})));
static_assert(!FollyExecutorDomain<InlineExecution>);

struct ExecutorViewDomain : InlineExecution {
  folly::Executor* getExecutor() const noexcept;
};

static_assert(FollyExecutorDomain<ExecutorViewDomain>);

TEST(InlineExecutionTest, ForwardsRvalueOnlyCallbacks) {
  InlineExecution domain;
  int observed = 0;
  domain.post(RvalueOnlyInlineTask{std::make_unique<int>(42), &observed});
  EXPECT_EQ(observed, 42);
  domain.post(RvalueOnlyInlineTask{std::make_unique<int>(7), &observed});
  EXPECT_EQ(observed, 7);
  domain.post([&] {
    domain.post(RvalueOnlyInlineTask{std::make_unique<int>(9), &observed});
    EXPECT_EQ(observed, 7);
  });
  EXPECT_EQ(observed, 9);
}

TEST(InlineExecutionTest, RunsImmediatelyOnCallerThread) {
  InlineExecution domain;
  EXPECT_EQ(domain, InlineExecution{});
  const auto caller = std::this_thread::get_id();
  int calls = 0;
  auto task = [&calls, caller, &domain, value = std::make_unique<int>(42)]() {
    EXPECT_EQ(std::this_thread::get_id(), caller);
    EXPECT_TRUE(domain.inRunningDomainThread());
    EXPECT_EQ(*value, 42);
    ++calls;
  };
  EXPECT_FALSE(domain.inRunningDomainThread());
  domain.post(std::move(task));
  EXPECT_EQ(calls, 1);
  EXPECT_FALSE(domain.inRunningDomainThread());
  domain.post([&calls, caller, value = std::make_unique<int>(7)]() {
    EXPECT_EQ(std::this_thread::get_id(), caller);
    EXPECT_EQ(*value, 7);
    ++calls;
  });
  EXPECT_EQ(calls, 2);
}

TEST(InlineExecutionTest, QueuesNestedPostsInOrder) {
  InlineExecution domain;
  InlineExecution otherDomain;
  std::vector<int> order;
  domain.post([&] {
    order.push_back(1);
    domain.post([&] {
      EXPECT_TRUE(domain.inRunningDomainThread());
      order.push_back(3);
      otherDomain.post([&] { order.push_back(5); });
      order.push_back(4);
    });
    order.push_back(2);
  });
  EXPECT_EQ(order, (std::vector<int>{1, 2, 3, 4, 5}));
  EXPECT_FALSE(domain.inRunningDomainThread());
}

TEST(InlineExecutionTest, SerializesOnlyWithinCallingThread) {
  InlineExecution domain;
  bool otherThreadInside = true;
  bool otherThreadRan = false;
  domain.post([&] {
    std::thread([&] {
      otherThreadInside = domain.inRunningDomainThread();
      domain.post([&] { otherThreadRan = true; });
    }).join();
    EXPECT_TRUE(otherThreadRan);
  });
  EXPECT_FALSE(otherThreadInside);
}

template <std::size_t Count>
auto largeSpec(Counters& counters) {
  if constexpr (Count == 0) {
    return PipelineSpec().state(EchoTaskState{&counters});
  } else {
    return largeSpec<Count - 1>(counters)
        .template add<DuplexBackpressureHandler>(HandlerTag<Count>{});
  }
}

TEST(PipelineSpecTest, ReadinessSupportsMoreThan63Handlers) {
  Counters counters;
  auto pipeline = largeSpec<64>(counters).add<TailHandler>(tail_tag).build();
  pipeline->activate();
  EXPECT_EQ(counters.added, 65);
  EXPECT_EQ(pipeline->fireRead(TypeErasedBox{}), Result::Success);
  EXPECT_EQ(pipeline->fireWrite(TypeErasedBox{}), Result::Error);
  EXPECT_EQ(counters.readArmed, 64);
  EXPECT_EQ(counters.writeArmed, 64);
  pipeline->onReadReady();
  pipeline->onWriteReady();
  EXPECT_EQ(counters.readReady, 64);
  EXPECT_EQ(counters.writeReady, 64);
  pipeline->onReadReady();
  pipeline->onWriteReady();
  EXPECT_EQ(counters.readReady, 64);
  EXPECT_EQ(counters.writeReady, 64);
}

struct ReadyTaskState {
  std::vector<int>* order;
  PipelineRef* pipeline;
};

struct RearmingHandler : ForwardingHandler {
  ReadyMarker readReadyHook_;
  int id;
  bool reenter;
  bool first{true};

  RearmingHandler(int id, bool reenter) : id(id), reenter(reenter) {}

  template <typename C>
  Result onRead(C& ctx, TypeErasedBox&& msg) noexcept {
    ctx.awaitReadReady();
    return ctx.fireRead(std::move(msg));
  }
  template <typename C>
  void onReadReady(C& ctx) noexcept {
    ctx.taskState().order->push_back(id);
    ctx.cancelAwaitReadReady();
    if (std::exchange(first, false)) {
      ctx.awaitReadReady();
      if (reenter) {
        ctx.taskState().pipeline->onReadReady();
      }
    }
  }
};

TEST(PipelineSpecTest, RearmingWaiterRunsOncePerPassInRegistrationOrder) {
  for (bool reenter : {false, true}) {
    std::vector<int> order;
    PipelineRef ref;
    auto pipeline = PipelineSpec()
                        .state(ReadyTaskState{&order, &ref})
                        .add<RearmingHandler>(head_tag, 1, reenter)
                        .add<RearmingHandler>(tail_tag, 2, false)
                        .build();
    ref = pipeline.ref();
    pipeline->activate();
    EXPECT_EQ(pipeline->fireRead(TypeErasedBox{}), Result::Error);
    pipeline->onReadReady();
    EXPECT_EQ(
        order,
        reenter ? (std::vector<int>{1, 2, 1, 2}) : (std::vector<int>{1, 2}));
    pipeline->onReadReady();
    EXPECT_EQ(order, (std::vector<int>{1, 2, 1, 2}));
    pipeline->onReadReady();
    EXPECT_EQ(order, (std::vector<int>{1, 2, 1, 2}));
  }
}

} // namespace
} // namespace apache::thrift::fast_thrift::channel_pipeline
