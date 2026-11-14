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
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineInstance.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineSpec.h>

#include <concepts>
#include <stdexcept>
#include <type_traits>

#include <folly/ExceptionWrapper.h>
#include <folly/portability/GTest.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
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
};

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

HANDLER_TAG(head);
HANDLER_TAG(mid);
HANDLER_TAG(tail);

PipelineInstance::Ptr makePipeline(Counters& counters) {
  return PipelineSpec<EchoTaskState, InlineExecution>{}
      .add<HeadHandler>(head_tag)
      .add<MiddleHandler>(mid_tag)
      .add<TailHandler>(tail_tag)
      .build(EchoTaskState{.counters = &counters}, InlineExecution{});
}

TEST(PipelineInstanceTest, BuildYieldsErasedOwner) {
  static_assert(std::same_as<PipelineInstance::Ptr, PipelineOwner>);
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

// The pipeline owns no allocator or executor. A handler that needs such an
// external resource receives it (owned outside) through the TaskState and
// reaches it via ctx.taskState() — the uniform "external resources via state"
// model.
struct Resource {
  int allocations{0};
};
struct ResourceTaskState {
  Counters* counters{nullptr};
  Resource* resource{nullptr};
};
struct ResourceTail : CountingBase {
  template <typename C>
  Result onRead(C& ctx, TypeErasedBox&&) noexcept {
    ++ctx.taskState().resource->allocations;
    ++ctx.taskState().counters->readsAtTail;
    return Result::Success;
  }
};

TEST(PipelineInstanceTest, ExternalResourceReachedThroughState) {
  Counters counters;
  Resource resource;
  auto pipeline =
      PipelineSpec<ResourceTaskState, InlineExecution>{}
          .add<ResourceTail>(tail_tag)
          .build(
              ResourceTaskState{.counters = &counters, .resource = &resource},
              InlineExecution{});
  pipeline->activate();
  EXPECT_EQ(pipeline->fireRead(TypeErasedBox{}), Result::Success);
  EXPECT_EQ(resource.allocations, 1);
  EXPECT_EQ(counters.readsAtTail, 1);
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

} // namespace
} // namespace apache::thrift::fast_thrift::channel_pipeline
