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
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineFactory.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineInstance.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineSpec.h>

#include <concepts>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>
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
// erased PipelineInstance::Ptr, which exposes no typed state accessor, so
// handlers record into counters the test owns (reached via the TaskState)
// rather than the test reading pipeline internals.
struct Counters {
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

static_assert(!kHasAllocator<PipelineInstance::Ref>);
static_assert(!kHasEventBase<PipelineInstance::Ref>);

PipelineInstance::Ptr makePipeline(Counters& counters) {
  return PipelineSpec<EchoTaskState, InlineExecution>{}
      .add<HeadHandler>(head_tag)
      .add<MiddleHandler>(mid_tag)
      .add<TailHandler>(tail_tag)
      .build(EchoTaskState{.counters = &counters}, InlineExecution{});
}

TEST(PipelineInstanceTest, BuildYieldsErasedOwner) {
  static_assert(std::same_as<PipelineInstance::Ptr, PipelineInstanceOwner>);
  Counters counters;
  PipelineInstance::Ptr pipeline = makePipeline(counters);
  EXPECT_TRUE(static_cast<bool>(pipeline));
}

TEST(PipelineInstanceTest, HandlerAddedRunsOverEveryHandler) {
  Counters counters;
  auto pipeline = makePipeline(counters);
  EXPECT_EQ(counters.added, 3);
}

TEST(PipelineInstanceTest, ActivateRunsOverEveryHandler) {
  Counters counters;
  auto pipeline = makePipeline(counters);
  pipeline->activate();
  EXPECT_EQ(counters.active, 3);
}

TEST(PipelineInstanceTest, ReadFlowsToTail) {
  Counters counters;
  auto pipeline = makePipeline(counters);
  pipeline->activate();
  EXPECT_EQ(pipeline->fireRead(TypeErasedBox{}), Result::Success);
  EXPECT_EQ(counters.readsAtTail, 1);
  EXPECT_EQ(counters.writesAtHead, 0);
}

TEST(PipelineInstanceTest, WriteFlowsToHead) {
  Counters counters;
  auto pipeline = makePipeline(counters);
  pipeline->activate();
  EXPECT_EQ(pipeline->fireWrite(TypeErasedBox{}), Result::Success);
  EXPECT_EQ(counters.writesAtHead, 1);
  EXPECT_EQ(counters.readsAtTail, 0);
}

// One spec builds more than once: build() is const and reusable, copying the
// blueprint each time, so two independent pipelines result.
TEST(PipelineInstanceTest, SpecBuildsMoreThanOnce) {
  auto spec = PipelineSpec<EchoTaskState, InlineExecution>{}
                  .add<HeadHandler>(head_tag)
                  .add<MiddleHandler>(mid_tag)
                  .add<TailHandler>(tail_tag);
  Counters a;
  Counters b;
  PipelineInstance::Ptr p1 =
      spec.build(EchoTaskState{.counters = &a}, InlineExecution{});
  PipelineInstance::Ptr p2 =
      spec.build(EchoTaskState{.counters = &b}, InlineExecution{});
  EXPECT_TRUE(static_cast<bool>(p1));
  EXPECT_TRUE(static_cast<bool>(p2));
  EXPECT_EQ(a.added, 3);
  EXPECT_EQ(b.added, 3);
}

// Firing past the tail (a forwarding tail) crosses the seam, which is unbound
// on this base and returns Error rather than crashing.
TEST(PipelineInstanceTest, ForwardPastTailHitsUnboundSeam) {
  Counters counters;
  PipelineInstance::Ptr pipeline =
      PipelineSpec<EchoTaskState, InlineExecution>{}
          .add<HeadHandler>(head_tag)
          .add<MiddleHandler>(tail_tag) // forwards past the end
          .build(EchoTaskState{.counters = &counters}, InlineExecution{});
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

TEST(PipelineInstanceTest, StatelessTaskStateBuildsAndRuns) {
  PipelineInstance::Ptr pipeline =
      PipelineSpec<StatelessTaskState, InlineExecution>{}
          .add<StatelessEcho>(head_tag)
          .build(StatelessTaskState{}, InlineExecution{});
  pipeline->activate();
  EXPECT_EQ(pipeline->fireRead(TypeErasedBox{}), Result::Success);
}

struct OwnedTaskResource {
  std::vector<std::string>* lifetime;

  ~OwnedTaskResource() { lifetime->emplace_back("resource"); }
};

struct OwnedTaskState {
  std::unique_ptr<OwnedTaskResource> resource;
};

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

TEST(
    PipelineInstanceTest, GuardRetainsOwnedTaskStateThroughHandlerDestruction) {
  std::vector<std::string> lifetime;
  auto pipeline = PipelineSpec<OwnedTaskState, InlineExecution>{}
                      .add<ResourceLifetimeHandler>(head_tag)
                      .build(
                          {std::unique_ptr<OwnedTaskResource>(
                              new OwnedTaskResource{&lifetime})},
                          InlineExecution{});
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
      lifetime, (std::vector<std::string>{"removed", "handler", "resource"}));
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
TEST(PipelineInstanceTest, AsymmetricHandlersSkipOppositeDirection) {
  Counters counters;
  PipelineInstance::Ptr pipeline =
      PipelineSpec<EchoTaskState, InlineExecution>{}
          .add<WriteOnlyHead>(head_tag)
          .add<ReadOnlyTail>(tail_tag)
          .build(EchoTaskState{.counters = &counters}, InlineExecution{});
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

struct CallbackTaskState {
  void* sink;
  Result (*accept)(void*, TypeErasedBox&&) noexcept;
};

struct CallbackTail : StatelessEcho {
  template <typename C>
  Result onRead(C& ctx, TypeErasedBox&& msg) noexcept {
    auto& task = ctx.taskState();
    return task.accept(task.sink, std::move(msg));
  }
};

struct EventBaseSink {
  folly::EventBase* eventBase;
  MessageSink* sink;

  static Result accept(void* owner, TypeErasedBox&& msg) noexcept {
    auto& self = *static_cast<EventBaseSink*>(owner);
    self.eventBase->runInLoop(
        [sink = self.sink, msg = std::move(msg)]() mutable {
          static_cast<void>(sink->accept(std::move(msg)));
        });
    return Result::Success;
  }
};

TEST(
    PipelineInstanceTest,
    InlineTailCanQueueDeliveryThroughExternalEventBaseSink) {
  MessageSink sink;
  folly::EventBase eventBase;
  EventBaseSink externalSink{&eventBase, &sink};
  auto factory = PipelineSpec<CallbackTaskState, InlineExecution>{}
                     .add<CallbackTail>(tail_tag)
                     .factory();
  auto pipeline = factory.create(
      {&externalSink, &EventBaseSink::accept}, InlineExecution{});
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

struct ExecutorSink {
  folly::Executor* executor;
  MessageSink* sink;

  static Result accept(void* owner, TypeErasedBox&& msg) noexcept {
    auto& self = *static_cast<ExecutorSink*>(owner);
    self.executor->add([sink = self.sink, msg = std::move(msg)]() mutable {
      static_cast<void>(sink->accept(std::move(msg)));
    });
    return Result::Success;
  }
};

TEST(PipelineInstanceTest, FactoryCanInjectDifferentExternalExecutorSinks) {
  MessageSink a;
  MessageSink b;
  folly::ManualExecutor executorA;
  folly::ManualExecutor executorB;
  ExecutorSink sinkA{&executorA, &a};
  ExecutorSink sinkB{&executorB, &b};
  auto factory = PipelineSpec<CallbackTaskState, InlineExecution>{}
                     .add<CallbackTail>(tail_tag)
                     .factory();
  auto p1 = factory.create({&sinkA, &ExecutorSink::accept}, InlineExecution{});
  auto p2 = factory.create({&sinkB, &ExecutorSink::accept}, InlineExecution{});
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
  CountingBufferAllocator allocator;
  BytesPtr* received;
};

struct AllocatingTail : StatelessEcho {
  template <typename C>
  Result onRead(C& ctx, TypeErasedBox&& msg) noexcept {
    auto& task = ctx.taskState();
    const auto& value = msg.template get<std::string>();
    *task.received = task.allocator.copyBuffer(value.data(), value.size());
    return Result::Success;
  }
};

TEST(PipelineInstanceTest, FactoryUsesEachTaskStatesAllocator) {
  int copiesA{0};
  int copiesB{0};
  BytesPtr receivedA;
  BytesPtr receivedB;
  auto factory = PipelineSpec<AllocatorTaskState, InlineExecution>{}
                     .add<AllocatingTail>(tail_tag)
                     .factory();
  auto p1 = factory.create(
      {.allocator = {.copies = &copiesA}, .received = &receivedA},
      InlineExecution{});
  auto p2 = factory.create(
      {.allocator = {.copies = &copiesB}, .received = &receivedB},
      InlineExecution{});
  p1->activate();
  p2->activate();

  EXPECT_EQ(p1->fireRead(erase_and_box(std::string("first"))), Result::Success);
  EXPECT_EQ(copiesA, 1);
  EXPECT_EQ(copiesB, 0);
  EXPECT_FALSE(receivedB);
  ASSERT_TRUE(receivedA);
  EXPECT_EQ(receivedA->moveToFbString().toStdString(), "first");

  EXPECT_EQ(
      p2->fireRead(erase_and_box(std::string("second"))), Result::Success);
  EXPECT_EQ(copiesA, 1);
  EXPECT_EQ(copiesB, 1);
  ASSERT_TRUE(receivedB);
  EXPECT_EQ(receivedB->moveToFbString().toStdString(), "second");
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

struct ForwardingTaskState {
  // The destination owner must outlive this context; forwarding is synchronous.
  PipelineInstance::Ref destination;
  Result (*sink)(PipelineInstance::Ref, TypeErasedBox&&) noexcept;
};

struct ForwardingTail : StatelessEcho {
  template <typename C>
  Result onRead(C& ctx, TypeErasedBox&& msg) noexcept {
    auto& task = ctx.taskState();
    return task.sink(task.destination, std::move(msg));
  }
};

struct ForwardingHead : StatelessEcho {
  template <typename C>
  Result onWrite(C& ctx, TypeErasedBox&& msg) noexcept {
    auto& task = ctx.taskState();
    return task.sink(task.destination, std::move(msg));
  }
};

TEST(PipelineInstanceTest, TailSinkContinuesReadsToAnotherPipeline) {
  MessageSink reads;
  MessageSink writes;
  auto destination =
      PipelineSpec<SinkTaskState, InlineExecution>{}
          .add<CollectingEndpoint>(tail_tag)
          .build({.reads = &reads, .writes = &writes}, InlineExecution{});
  auto source = PipelineSpec<ForwardingTaskState, InlineExecution>{}
                    .add<ForwardingTail>(tail_tag)
                    .build(
                        {
                            .destination = destination.ref(),
                            .sink =
                                +[](PipelineInstance::Ref next,
                                    TypeErasedBox&& msg) noexcept {
                                  return next.fireRead(std::move(msg));
                                },
                        },
                        InlineExecution{});
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
  EXPECT_TRUE(writes.messages.empty());
}

TEST(PipelineInstanceTest, HeadSinkContinuesWritesToAnotherPipeline) {
  MessageSink reads;
  MessageSink writes;
  auto destination =
      PipelineSpec<SinkTaskState, InlineExecution>{}
          .add<CollectingEndpoint>(head_tag)
          .build({.reads = &reads, .writes = &writes}, InlineExecution{});
  auto source = PipelineSpec<ForwardingTaskState, InlineExecution>{}
                    .add<ForwardingHead>(head_tag)
                    .build(
                        {
                            .destination = destination.ref(),
                            .sink =
                                +[](PipelineInstance::Ref next,
                                    TypeErasedBox&& msg) noexcept {
                                  return next.fireWrite(std::move(msg));
                                },
                        },
                        InlineExecution{});
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
  EXPECT_TRUE(reads.messages.empty());
}

// Exceptions are delivered only to handlers satisfying the full InboundHandler
// concept (consistent with the other pipelines): an outbound-only handler is
// skipped even though it implements onException, and the exception reaches the
// first inbound handler.
TEST(PipelineInstanceTest, ExceptionSkipsOutboundOnlyHandler) {
  Counters counters;
  auto pipeline =
      PipelineSpec<EchoTaskState, InlineExecution>{}
          .add<OutboundExceptionSpy>(head_tag)
          .add<InboundExceptionSpy>(tail_tag)
          .build(EchoTaskState{.counters = &counters}, InlineExecution{});
  pipeline->activate();
  pipeline->fireException(
      folly::make_exception_wrapper<std::runtime_error>("boom"));
  EXPECT_EQ(counters.inboundExceptions, 1);
  EXPECT_EQ(counters.outboundExceptions, 0);
}

// A handler that armed write-readiness is woken by onWriteReady(), and once it
// cancels it is not woken again.
TEST(PipelineInstanceTest, WriteReadyNotifiesArmedHandler) {
  Counters counters;
  auto pipeline =
      PipelineSpec<EchoTaskState, InlineExecution>{}
          .add<HeadHandler>(head_tag)
          .add<WriteBackpressureHandler>(mid_tag)
          .add<TailHandler>(tail_tag)
          .build(EchoTaskState{.counters = &counters}, InlineExecution{});
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

TEST(PipelineInstanceTest, ReadAndWriteReadinessAreIndependentInOneHandler) {
  Counters counters;
  auto pipeline =
      PipelineSpec<EchoTaskState, InlineExecution>{}
          .add<HeadHandler>(head_tag)
          .add<DuplexBackpressureHandler>(mid_tag)
          .add<TailHandler>(tail_tag)
          .build(EchoTaskState{.counters = &counters}, InlineExecution{});
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
TEST(PipelineInstanceTest, ReadReadyNotifiesArmedHandler) {
  Counters counters;
  auto pipeline =
      PipelineSpec<EchoTaskState, InlineExecution>{}
          .add<HeadHandler>(head_tag)
          .add<ReadBackpressureHandler>(mid_tag)
          .add<TailHandler>(tail_tag)
          .build(EchoTaskState{.counters = &counters}, InlineExecution{});
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
TEST(PipelineInstanceTest, EventBroadcastsToSubscriber) {
  Counters counters;
  auto pipeline =
      PipelineSpec<EchoTaskState, InlineExecution>{}
          .add<EventPublisher>(head_tag)
          .add<EventSubscriber>(mid_tag)
          .add<TailHandler>(tail_tag)
          .build(EchoTaskState{.counters = &counters}, InlineExecution{});
  pipeline->activate();
  EXPECT_EQ(pipeline->fireRead(TypeErasedBox{}), Result::Success);
  EXPECT_EQ(counters.eventSum, 7);
}

// An erased factory stamps out independent pipelines from one spec; traffic on
// one does not touch another.
TEST(PipelineInstanceTest, FactoryStampsIndependentPipelines) {
  PipelineFactory<EchoTaskState, InlineExecution> factory =
      PipelineSpec<EchoTaskState, InlineExecution>{}
          .add<HeadHandler>(head_tag)
          .add<MiddleHandler>(mid_tag)
          .add<TailHandler>(tail_tag)
          .factory();
  EXPECT_TRUE(static_cast<bool>(factory));
  Counters a;
  Counters b;
  PipelineInstance::Ptr p1 =
      factory.create(EchoTaskState{.counters = &a}, InlineExecution{});
  PipelineInstance::Ptr p2 =
      factory.create(EchoTaskState{.counters = &b}, InlineExecution{});
  p1->activate();
  EXPECT_EQ(p1->fireRead(TypeErasedBox{}), Result::Success);

  EXPECT_EQ(a.active, 3);
  EXPECT_EQ(a.readsAtTail, 1);
  // p2 was built (handlers added) but neither activated nor driven.
  EXPECT_EQ(b.added, 3);
  EXPECT_EQ(b.active, 0);
  EXPECT_EQ(b.readsAtTail, 0);
}

// The factory is move-only: move-constructible/assignable (the spec-taking
// constructor's requires-clause keeps it from shadowing the move constructor)
// and not copyable (it owns a folly::Function).
static_assert(std::is_move_constructible_v<
              PipelineFactory<EchoTaskState, InlineExecution>>);
static_assert(
    std::is_move_assignable_v<PipelineFactory<EchoTaskState, InlineExecution>>);
static_assert(!std::is_copy_constructible_v<
              PipelineFactory<EchoTaskState, InlineExecution>>);

// operator bool() reports whether the factory holds a create thunk: false when
// default-constructed, true once produced from a spec.
TEST(PipelineInstanceTest, DefaultFactoryIsEmptyBuiltFactoryIsNot) {
  PipelineFactory<EchoTaskState, InlineExecution> empty;
  EXPECT_FALSE(static_cast<bool>(empty));

  PipelineFactory<EchoTaskState, InlineExecution> built =
      PipelineSpec<EchoTaskState, InlineExecution>{}
          .add<TailHandler>(tail_tag)
          .factory();
  EXPECT_TRUE(static_cast<bool>(built));
}

// Moving a factory (via direct-initialization, where a perfect-forwarding
// constructor would shadow the move constructor if unconstrained) uses the real
// move constructor: the moved-to factory still stamps working pipelines and the
// moved-from factory is left empty.
TEST(PipelineInstanceTest, MovedFactoryStillStampsPipelines) {
  PipelineFactory<EchoTaskState, InlineExecution> factory =
      PipelineSpec<EchoTaskState, InlineExecution>{}
          .add<HeadHandler>(head_tag)
          .add<TailHandler>(tail_tag)
          .factory();

  PipelineFactory<EchoTaskState, InlineExecution> moved(std::move(factory));
  EXPECT_TRUE(static_cast<bool>(moved));
  EXPECT_FALSE(static_cast<bool>(factory)); // NOLINT(bugprone-use-after-move)

  Counters counters;
  PipelineInstance::Ptr pipeline =
      moved.create(EchoTaskState{.counters = &counters}, InlineExecution{});
  pipeline->activate();
  EXPECT_EQ(pipeline->fireRead(TypeErasedBox{}), Result::Success);
  EXPECT_EQ(counters.readsAtTail, 1);
}

// An external publisher reaches the subscriber through the erased handle's
// bindEvents/fireBoundEvent path.
TEST(PipelineInstanceTest, ExternalPublisherReachesSubscriber) {
  Counters counters;
  auto pipeline =
      PipelineSpec<EchoTaskState, InlineExecution>{}
          .add<HeadHandler>(head_tag)
          .add<EventSubscriber>(mid_tag)
          .add<TailHandler>(tail_tag)
          .build(EchoTaskState{.counters = &counters}, InlineExecution{});
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

TEST(PipelineInstanceTest, InlineExecutionReturnsHandlerResultSynchronously) {
  for (auto result : {Result::Success, Result::Backpressure, Result::Error}) {
    int calls{0};
    const InlineExecution* observedDomain{nullptr};
    auto pipeline =
        PipelineSpec<InlineTaskState, InlineExecution>{}
            .add<InlineResultHandler>(head_tag)
            .build({&calls, &observedDomain, result}, InlineExecution{});
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

TEST(PipelineInstanceTest, FactoryErasesDifferentHandlerCompositions) {
  PipelineFactory<EchoTaskState, InlineExecution> twoHandlers =
      PipelineSpec<EchoTaskState, InlineExecution>{}
          .add<HeadHandler>(head_tag)
          .add<TailHandler>(tail_tag)
          .factory();
  PipelineFactory<EchoTaskState, InlineExecution> threeHandlers =
      PipelineSpec<EchoTaskState, InlineExecution>{}
          .add<HeadHandler>(head_tag)
          .add<MiddleHandler>(mid_tag)
          .add<TailHandler>(tail_tag)
          .factory();
  Counters a;
  Counters b;
  auto p1 = twoHandlers.create({.counters = &a}, InlineExecution{});
  auto p2 = threeHandlers.create({.counters = &b}, InlineExecution{});
  EXPECT_EQ(a.added, 2);
  EXPECT_EQ(b.added, 3);
  p1->activate();
  p2->activate();
  EXPECT_EQ(p1->fireRead(TypeErasedBox{}), Result::Success);
  EXPECT_EQ(a.readsAtTail, 1);
  EXPECT_EQ(b.readsAtTail, 0);
  EXPECT_EQ(p2->fireRead(TypeErasedBox{}), Result::Success);
  EXPECT_EQ(b.readsAtTail, 1);
}

} // namespace
} // namespace apache::thrift::fast_thrift::channel_pipeline
