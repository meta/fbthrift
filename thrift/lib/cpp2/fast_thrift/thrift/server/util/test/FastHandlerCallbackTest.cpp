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

#include <gtest/gtest.h>

#include <atomic>
#include <optional>
#include <stdexcept>
#include <thread>
#include <utility>

#include <folly/executors/ManualExecutor.h>
#include <folly/io/async/DelayedDestruction.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/Request.h>
#include <folly/io/async/ScopedEventBaseThread.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/HandlerTag.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineBuilder.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/test/MockAdapters.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/util/FastHandlerCallback.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/util/FastHandlerCoro.h>

namespace apache::thrift::fast_thrift::thrift {

namespace {

HANDLER_TAG(completion_capture_handler);

// Records the invocations made by the FastHandlerCallback's static
// function-pointer dispatch. Subclasses ThriftServerAppAdapter so the
// callback's adapterGuard_ (a DestructorGuard) operates on a real
// DelayedDestruction; the static dispatch fns downcast to read counters.
class RecordingAdapter : public ThriftServerAppAdapter {
 public:
  explicit RecordingAdapter(bool* destroyed = nullptr)
      : destroyed_(destroyed) {}

  int resultCount{0};
  int doneCount{0};
  int exceptionCount{0};
  uint32_t lastStreamId{0};
  int lastValue{0};
  std::string lastExceptionMessage;
  FastHandlerCallback<int>* reentrantCallback{nullptr};
  // Recorded rather than kept alive: the thunks own the context and drop it,
  // so only its identity survives the call.
  const ThriftRequestContext* lastRequestContext{nullptr};
  folly::RequestContext* lastFollyRequestContext{nullptr};
  void markPipelineClosed() {
    pipelineActive_ = false;
    pipeline_.reset();
    pipelineGuard_.reset();
  }

 protected:
  ~RecordingAdapter() override {
    if (destroyed_) {
      *destroyed_ = true;
    }
  }

 private:
  bool* destroyed_;
};

RecordingAdapter& asRecorder(ThriftServerAppAdapter* p) {
  return *static_cast<RecordingAdapter*>(p);
}

// Impersonates the codegen result thunk: records the invocation instead of
// building/writing a response. The real thunk rides the requestContext onto
// the message; the fake just records which one it was handed.
void onResult(
    ThriftServerAppAdapter* a,
    uint32_t streamId,
    ThriftRequestContextPtr requestContext,
    folly::DelayedDestruction::DestructorGuard&& /*adapterGuard*/,
    int&& value) noexcept {
  auto& r = asRecorder(a);
  r.resultCount++;
  r.lastStreamId = streamId;
  r.lastValue = value;
  r.lastRequestContext = requestContext.get();
  r.lastFollyRequestContext = folly::RequestContext::try_get();
  if (auto* callback = std::exchange(r.reentrantCallback, nullptr)) {
    callback->result(456);
  }
}

// The by-value exception_wrapper matches the ExceptionFn signature.
void onException(
    ThriftServerAppAdapter* a,
    uint32_t streamId,
    ThriftRequestContextPtr requestContext,
    folly::DelayedDestruction::DestructorGuard&& /*adapterGuard*/,
    // NOLINTNEXTLINE(performance-unnecessary-value-param)
    folly::exception_wrapper ew) noexcept {
  auto& r = asRecorder(a);
  r.exceptionCount++;
  r.lastStreamId = streamId;
  r.lastExceptionMessage = ew.what().toStdString();
  r.lastRequestContext = requestContext.get();
  r.lastFollyRequestContext = folly::RequestContext::try_get();
}

struct ThrowingMoveValue {
  explicit ThrowingMoveValue(int value, int* moveCount = nullptr)
      : value(value), moveCount(moveCount) {}

  ThrowingMoveValue(const ThrowingMoveValue&) = delete;
  ThrowingMoveValue& operator=(const ThrowingMoveValue&) = delete;
  ThrowingMoveValue(ThrowingMoveValue&& other) noexcept(false)
      : value(std::exchange(other.value, 0)), moveCount(other.moveCount) {
    if (moveCount != nullptr) {
      ++*moveCount;
    }
  }
  ThrowingMoveValue& operator=(ThrowingMoveValue&&) = delete;

  int value;
  int* moveCount;
};

void onThrowingMoveResult(
    ThriftServerAppAdapter* a,
    uint32_t streamId,
    ThriftRequestContextPtr requestContext,
    folly::DelayedDestruction::DestructorGuard&& /*adapterGuard*/,
    ThrowingMoveValue&& value) noexcept {
  auto& r = asRecorder(a);
  r.resultCount++;
  r.lastStreamId = streamId;
  r.lastValue = value.value;
  r.lastRequestContext = requestContext.get();
  r.lastFollyRequestContext = folly::RequestContext::try_get();
}

void onDone(
    ThriftServerAppAdapter* a,
    uint32_t streamId,
    ThriftRequestContextPtr requestContext,
    folly::DelayedDestruction::DestructorGuard&& /*adapterGuard*/) noexcept {
  auto& r = asRecorder(a);
  r.doneCount++;
  r.lastStreamId = streamId;
  r.lastRequestContext = requestContext.get();
  r.lastFollyRequestContext = folly::RequestContext::try_get();
}

using RecordingAdapterPtr =
    folly::DelayedDestructionUniquePtr<RecordingAdapter>;

RecordingAdapterPtr makeRecorder(bool* destroyed = nullptr) {
  return folly::makeDelayedDestructionUniquePtr<RecordingAdapter>(destroyed);
}

void configureClosedPipeline(folly::EventBase& evb, RecordingAdapter& adapter) {
  channel_pipeline::test::MockHeadHandler head;
  channel_pipeline::test::TestAllocator allocator;
  auto pipeline = channel_pipeline::PipelineBuilder<
                      channel_pipeline::test::MockHeadHandler,
                      RecordingAdapter,
                      channel_pipeline::test::TestAllocator>()
                      .setEventBase(&evb)
                      .setHead(&head)
                      .setTail(&adapter)
                      .setAllocator(&allocator)
                      .build();
  adapter.setPipeline(pipeline.get());
  adapter.markPipelineClosed();
}

constexpr uint32_t kStreamId = 42;

class RejectingExecutor final : public folly::Executor {
 public:
  [[noreturn]] void add(folly::Func) override {
    throw std::runtime_error("rejected");
  }
};

class EnqueueThenThrowExecutor final : public folly::Executor {
 public:
  void add(folly::Func func) override {
    EXPECT_FALSE(task_.has_value());
    task_.emplace(std::move(func));
    throw std::runtime_error("rejected after enqueue");
  }

  void runStoredTask() {
    ASSERT_TRUE(task_.has_value());
    auto task = std::move(*task_);
    task_.reset();
    task();
  }

 private:
  std::optional<folly::Func> task_;
};

class DroppingExecutor final : public folly::Executor {
 public:
  void add(folly::Func) override {}
};

class CompletionCaptureHandler {
 public:
  using SubscribedEvents =
      channel_pipeline::Events<ThriftServerRequestCompletedEvent>;

  template <typename Context>
  channel_pipeline::Result onRead(
      Context& ctx, channel_pipeline::TypeErasedBox&& msg) noexcept {
    return ctx.fireRead(std::move(msg));
  }
  template <typename Context>
  channel_pipeline::Result onWrite(
      Context& ctx, channel_pipeline::TypeErasedBox&& msg) noexcept {
    return ctx.fireWrite(std::move(msg));
  }
  template <typename Context>
  void onException(Context& ctx, folly::exception_wrapper&& e) noexcept {
    ctx.fireException(std::move(e));
  }
  template <typename Context>
  void handlerAdded(Context&) noexcept {}
  template <typename Context>
  void handlerRemoved(Context&) noexcept {}
  template <typename Context>
  void onPipelineActive(Context&) noexcept {}
  template <typename Context>
  void onPipelineInactive(Context&) noexcept {}
  template <typename Context>
  void onReadReady(Context&) noexcept {}
  template <typename Context>
  void onWriteReady(Context&) noexcept {}

  template <channel_pipeline::PipelineEvent E, typename Context>
    requires std::same_as<E, ThriftServerRequestCompletedEvent>
  void on(Context&, const ThriftServerRequestCompletedEvent& event) noexcept {
    ++count;
    streamId = event.streamId;
  }

  int count{0};
  uint32_t streamId{0};
};

template <typename F>
void runOnHandlerExecutorForTest(
    folly::Executor* executor, folly::EventBase& evb, F&& fn) {
  using Task = detail::HandlerExecutorTask<std::decay_t<F>>;
  Task task(folly::getKeepAliveToken(&evb), static_cast<F&&>(fn));
  detail::runOnHandlerExecutor(executor, std::move(task));
}

folly::coro::Task<int> observeCancellationToken(bool& observedCancellation) {
  const auto& token = co_await folly::coro::co_current_cancellation_token;
  observedCancellation = token.isCancellationRequested();
  co_return 123;
}

} // namespace

TEST(FastHandlerCallbackTest, RejectedExecutorRunsTaskInline) {
  folly::EventBase evb;
  RejectingExecutor executor;
  int calls = 0;

  runOnHandlerExecutorForTest(&executor, evb, [&]() noexcept { ++calls; });

  EXPECT_EQ(calls, 1);
}

TEST(FastHandlerCallbackTest, ThrowAfterEnqueueRunsTaskExactlyOnce) {
  folly::EventBase evb;
  EnqueueThenThrowExecutor executor;
  int calls = 0;

  runOnHandlerExecutorForTest(&executor, evb, [&]() noexcept { ++calls; });
  EXPECT_EQ(calls, 0);

  executor.runStoredTask();
  EXPECT_EQ(calls, 1);
}

TEST(FastHandlerCallbackTest, DroppedExecutorTaskFallsBackToEventBase) {
  folly::EventBase evb;
  DroppingExecutor executor;
  int calls = 0;

  runOnHandlerExecutorForTest(&executor, evb, [&]() noexcept { ++calls; });
  evb.loopOnce();

  EXPECT_EQ(calls, 1);
}

TEST(FastHandlerCallbackTest, DroppedDispatchTaskCompletesWithError) {
  auto rec = makeRecorder();
  folly::EventBase evb;
  DroppingExecutor executor;
  folly::RequestContextScopeGuard callerGuard;
  auto* const callerContext = folly::RequestContext::try_get();
  auto requestContext = makeThriftRequestContext(evb);
  auto* const requestFollyContext =
      requestContext->getFollyRequestContext().get();
  auto cb = makeFastHandlerCallback<FastHandlerCallback<int>>(
      &onResult,
      &onException,
      rec.get(),
      kStreamId,
      evb,
      &executor,
      std::move(requestContext));

  executor.add([cb = std::move(cb)]() mutable {
    cb->markHandlerStarted();
    cb->result(123);
  });

  EXPECT_EQ(rec->resultCount, 0);
  EXPECT_EQ(rec->exceptionCount, 1);
  EXPECT_EQ(rec->lastFollyRequestContext, requestFollyContext);
  EXPECT_EQ(folly::RequestContext::try_get(), callerContext);
  EXPECT_NE(
      rec->lastExceptionMessage.find(detail::kHandlerExecutorUnavailable),
      std::string::npos);
}

TEST(FastHandlerCallbackTest, ResultInvokesResultFnAndSuppressesDestructor) {
  auto rec = makeRecorder();
  folly::EventBase evb;
  {
    auto cb = makeFastHandlerCallback<FastHandlerCallback<int>>(
        &onResult, &onException, rec.get(), kStreamId, evb, nullptr, nullptr);
    cb->result(123);
  }
  // Single success invocation, no synthetic destructor exception.
  EXPECT_EQ(rec->resultCount, 1);
  EXPECT_EQ(rec->exceptionCount, 0);
  EXPECT_EQ(rec->lastStreamId, kStreamId);
  EXPECT_EQ(rec->lastValue, 123);
}

TEST(FastHandlerCallbackTest, StorageBypassesEventBaseAllocator) {
  auto rec = makeRecorder();
  folly::EventBase evb;
  auto& allocator = mem::EvbAllocator::getOrCreate(evb);
  const auto baseline = allocator.snapshotStats().outstandingAllocations;

  {
    auto cb = makeFastHandlerCallback<FastHandlerCallback<int>>(
        &onResult, &onException, rec.get(), kStreamId, evb, nullptr, nullptr);
    EXPECT_EQ(allocator.snapshotStats().outstandingAllocations, baseline);
    cb->result(123);
  }

  EXPECT_EQ(allocator.snapshotStats().outstandingAllocations, baseline);
}

TEST(FastHandlerCallbackTest, CancellationAcknowledgementCompletesOnce) {
  folly::EventBase evb;
  channel_pipeline::test::MockHeadHandler head;
  channel_pipeline::test::TestAllocator allocator;
  auto rec = makeRecorder();
  auto capture = std::make_unique<CompletionCaptureHandler>();
  auto* capturePtr = capture.get();
  auto pipeline = channel_pipeline::PipelineBuilder<
                      channel_pipeline::test::MockHeadHandler,
                      RecordingAdapter,
                      channel_pipeline::test::TestAllocator>()
                      .setEventBase(&evb)
                      .setHead(&head)
                      .setTail(rec.get())
                      .setAllocator(&allocator)
                      .addNextDuplex<CompletionCaptureHandler>(
                          completion_capture_handler_tag, std::move(capture))
                      .build();
  rec->setPipeline(pipeline.get());
  auto requestContext = makeThriftRequestContext(evb);
  requestContext->enableCancellation();
  requestContext->requestCancellation();
  auto cb = makeFastHandlerCallback<FastHandlerCallback<int>>(
      &onResult,
      &onException,
      rec.get(),
      kStreamId,
      evb,
      nullptr,
      std::move(requestContext));

  cb->cancelled();
  cb->cancelled();
  cb->result(123);
  evb.loopOnce(EVLOOP_NONBLOCK);

  EXPECT_EQ(capturePtr->count, 1);
  EXPECT_EQ(capturePtr->streamId, kStreamId);
  EXPECT_EQ(rec->resultCount, 0);
  EXPECT_EQ(rec->exceptionCount, 0);

  cb.reset();
  rec->resetPipeline();
}

TEST(FastHandlerCallbackTest, CancellationAfterPipelineResetIsDropped) {
  folly::EventBase evb;
  channel_pipeline::test::MockHeadHandler head;
  channel_pipeline::test::TestAllocator allocator;
  auto rec = makeRecorder();
  auto pipeline = channel_pipeline::PipelineBuilder<
                      channel_pipeline::test::MockHeadHandler,
                      RecordingAdapter,
                      channel_pipeline::test::TestAllocator>()
                      .setEventBase(&evb)
                      .setHead(&head)
                      .setTail(rec.get())
                      .setAllocator(&allocator)
                      .build();
  rec->setPipeline(pipeline.get());
  auto requestContext = makeThriftRequestContext(evb);
  requestContext->enableCancellation();
  requestContext->requestCancellation();
  auto cb = makeFastHandlerCallback<FastHandlerCallback<int>>(
      &onResult,
      &onException,
      rec.get(),
      kStreamId,
      evb,
      nullptr,
      std::move(requestContext));

  rec->resetPipeline();
  cb->cancelled();
  evb.loopOnce(EVLOOP_NONBLOCK);

  EXPECT_EQ(rec->resultCount, 0);
  EXPECT_EQ(rec->exceptionCount, 0);
}

TEST(FastHandlerCallbackTest, CancellationCallbackCanAcknowledgeReentrantly) {
  folly::EventBase evb;
  channel_pipeline::test::MockHeadHandler head;
  channel_pipeline::test::TestAllocator allocator;
  auto rec = makeRecorder();
  auto capture = std::make_unique<CompletionCaptureHandler>();
  auto* capturePtr = capture.get();
  auto pipeline = channel_pipeline::PipelineBuilder<
                      channel_pipeline::test::MockHeadHandler,
                      RecordingAdapter,
                      channel_pipeline::test::TestAllocator>()
                      .setEventBase(&evb)
                      .setHead(&head)
                      .setTail(rec.get())
                      .setAllocator(&allocator)
                      .addNextDuplex<CompletionCaptureHandler>(
                          completion_capture_handler_tag, std::move(capture))
                      .build();
  rec->setPipeline(pipeline.get());
  auto requestContext = makeThriftRequestContext(evb);
  requestContext->enableCancellation();
  auto* requestContextPtr = requestContext.get();
  auto cb = makeFastHandlerCallback<FastHandlerCallback<int>>(
      &onResult,
      &onException,
      rec.get(),
      kStreamId,
      evb,
      nullptr,
      std::move(requestContext));
  folly::CancellationCallback cancellationCallback(
      cb->getCancellationToken(), [&] { cb->cancelled(); });

  EXPECT_TRUE(requestContextPtr->requestCancellation());
  EXPECT_EQ(capturePtr->count, 0);
  evb.loopOnce(EVLOOP_NONBLOCK);

  EXPECT_EQ(capturePtr->count, 1);
  EXPECT_EQ(capturePtr->streamId, kStreamId);
  cb.reset();
  rec->resetPipeline();
}

TEST(FastHandlerCallbackTest, CpuCancellationRacingResultCompletesOnce) {
  folly::EventBase evb;
  channel_pipeline::test::MockHeadHandler head;
  channel_pipeline::test::TestAllocator allocator;
  auto rec = makeRecorder();
  auto capture = std::make_unique<CompletionCaptureHandler>();
  auto* capturePtr = capture.get();
  auto pipeline = channel_pipeline::PipelineBuilder<
                      channel_pipeline::test::MockHeadHandler,
                      RecordingAdapter,
                      channel_pipeline::test::TestAllocator>()
                      .setEventBase(&evb)
                      .setHead(&head)
                      .setTail(rec.get())
                      .setAllocator(&allocator)
                      .addNextDuplex<CompletionCaptureHandler>(
                          completion_capture_handler_tag, std::move(capture))
                      .build();
  rec->setPipeline(pipeline.get());

  constexpr int kIterations = 100;
  for (int i = 0; i < kIterations; ++i) {
    folly::ManualExecutor executor;
    auto requestContext = makeThriftRequestContext(evb);
    requestContext->enableCancellation();
    auto cb = makeFastHandlerCallback<FastHandlerCallback<int>>(
        &onResult,
        &onException,
        rec.get(),
        kStreamId,
        evb,
        &executor,
        std::move(requestContext));
    cb->markHandlerStarted();

    std::atomic<bool> start{false};
    std::thread response([&] {
      detail::HandlerExecutorScope scope(&executor);
      while (!start.load(std::memory_order_acquire)) {
      }
      cb->result(123);
    });
    std::thread cancellation([&] {
      while (!start.load(std::memory_order_acquire)) {
      }
      cb->cancelled();
    });

    start.store(true, std::memory_order_release);
    response.join();
    cancellation.join();
    evb.loopOnce(EVLOOP_NONBLOCK);
    cb.reset();
  }

  EXPECT_EQ(rec->resultCount + capturePtr->count, kIterations);
  EXPECT_EQ(rec->exceptionCount, 0);
  rec->resetPipeline();
}

TEST(FastHandlerCallbackTest, CoroutineReceivesRequestCancellationToken) {
  folly::EventBase evb;
  channel_pipeline::test::MockHeadHandler head;
  channel_pipeline::test::TestAllocator allocator;
  auto rec = makeRecorder();
  auto capture = std::make_unique<CompletionCaptureHandler>();
  auto* capturePtr = capture.get();
  auto pipeline = channel_pipeline::PipelineBuilder<
                      channel_pipeline::test::MockHeadHandler,
                      RecordingAdapter,
                      channel_pipeline::test::TestAllocator>()
                      .setEventBase(&evb)
                      .setHead(&head)
                      .setTail(rec.get())
                      .setAllocator(&allocator)
                      .addNextDuplex<CompletionCaptureHandler>(
                          completion_capture_handler_tag, std::move(capture))
                      .build();
  rec->setPipeline(pipeline.get());
  auto requestContext = makeThriftRequestContext(evb);
  requestContext->enableCancellation();
  requestContext->requestCancellation();
  auto cb = makeFastHandlerCallback<FastHandlerCallback<int>>(
      &onResult,
      &onException,
      rec.get(),
      kStreamId,
      evb,
      nullptr,
      std::move(requestContext));
  bool observedCancellation = false;

  detail::fastRunCoro(
      std::move(cb), observeCancellationToken(observedCancellation));
  evb.loopOnce(EVLOOP_NONBLOCK);

  EXPECT_TRUE(observedCancellation);
  EXPECT_EQ(capturePtr->count, 1);
  EXPECT_EQ(rec->resultCount, 0);
  EXPECT_EQ(rec->exceptionCount, 0);
  rec->resetPipeline();
}

TEST(FastHandlerCallbackTest, DuplicateCompletionIsIgnored) {
  auto rec = makeRecorder();
  folly::EventBase evb;
  auto cb = makeFastHandlerCallback<FastHandlerCallback<int>>(
      &onResult, &onException, rec.get(), kStreamId, evb, nullptr, nullptr);

  cb->result(123);
  cb->result(456);

  EXPECT_EQ(rec->resultCount, 1);
  EXPECT_EQ(rec->lastValue, 123);
}

TEST(FastHandlerCallbackTest, ReentrantCompletionIsIgnored) {
  auto rec = makeRecorder();
  folly::EventBase evb;
  auto cb = makeFastHandlerCallback<FastHandlerCallback<int>>(
      &onResult, &onException, rec.get(), kStreamId, evb, nullptr, nullptr);
  rec->reentrantCallback = cb.get();

  cb->result(123);

  EXPECT_EQ(rec->resultCount, 1);
  EXPECT_EQ(rec->lastValue, 123);
}

TEST(FastHandlerCallbackTest, MarkHandlerStartedDoesNotRearmCompletion) {
  auto rec = makeRecorder();
  folly::EventBase evb;
  folly::ManualExecutor executor;
  auto cb = makeFastHandlerCallback<FastHandlerCallback<int>>(
      &onResult, &onException, rec.get(), kStreamId, evb, &executor, nullptr);

  {
    detail::HandlerExecutorScope scope(&executor);
    cb->result(123);
  }
  cb->markHandlerStarted();
  {
    detail::HandlerExecutorScope scope(&executor);
    cb->result(456);
  }

  EXPECT_EQ(rec->resultCount, 1);
  EXPECT_EQ(rec->lastValue, 123);
}

TEST(FastHandlerCallbackTest, ResultRunsOnConfiguredExecutor) {
  auto rec = makeRecorder();
  folly::EventBase evb;
  folly::ManualExecutor executor;
  folly::RequestContextScopeGuard callerGuard;
  auto* const callerContext = folly::RequestContext::try_get();
  auto requestContext = makeThriftRequestContext(evb);
  auto* const requestFollyContext =
      requestContext->getFollyRequestContext().get();
  {
    auto cb = makeFastHandlerCallback<FastHandlerCallback<int>>(
        &onResult,
        &onException,
        rec.get(),
        kStreamId,
        evb,
        &executor,
        std::move(requestContext));
    cb->result(123);
  }

  EXPECT_EQ(rec->resultCount, 0);
  EXPECT_EQ(executor.drain(), 1);
  EXPECT_EQ(rec->resultCount, 1);
  EXPECT_EQ(rec->lastValue, 123);
  EXPECT_EQ(rec->lastFollyRequestContext, requestFollyContext);
  EXPECT_EQ(folly::RequestContext::try_get(), callerContext);
}

TEST(FastHandlerCallbackTest, ThrowingMoveResultRunsOnConfiguredExecutor) {
  auto rec = makeRecorder();
  folly::EventBase evb;
  folly::ManualExecutor executor;
  {
    auto cb = makeFastHandlerCallback<FastHandlerCallback<ThrowingMoveValue>>(
        &onThrowingMoveResult,
        &onException,
        rec.get(),
        kStreamId,
        evb,
        &executor,
        nullptr);
    cb->result(ThrowingMoveValue{123});
  }

  EXPECT_EQ(rec->resultCount, 0);
  EXPECT_EQ(executor.drain(), 1);
  EXPECT_EQ(rec->resultCount, 1);
  EXPECT_EQ(rec->lastValue, 123);
}

TEST(FastHandlerCallbackTest, InlineResultDoesNotMoveReturnValueAgain) {
  auto rec = makeRecorder();
  folly::EventBase evb;
  folly::ManualExecutor executor;
  auto cb = makeFastHandlerCallback<FastHandlerCallback<ThrowingMoveValue>>(
      &onThrowingMoveResult,
      &onException,
      rec.get(),
      kStreamId,
      evb,
      &executor,
      nullptr);
  int moveCount = 0;

  {
    detail::HandlerExecutorScope scope(&executor);
    cb->result(ThrowingMoveValue{123, &moveCount});
  }

  EXPECT_EQ(moveCount, 0);
  EXPECT_EQ(rec->resultCount, 1);
  EXPECT_EQ(rec->lastValue, 123);
}

TEST(FastHandlerCallbackTest, ResultAlreadyOnConfiguredExecutorRunsInline) {
  auto rec = makeRecorder();
  folly::EventBase evb;
  folly::ManualExecutor executor;
  folly::RequestContextScopeGuard callerGuard;
  auto* const callerContext = folly::RequestContext::try_get();
  auto requestContext = makeThriftRequestContext(evb);
  auto* const requestFollyContext =
      requestContext->getFollyRequestContext().get();
  auto cb = makeFastHandlerCallback<FastHandlerCallback<int>>(
      &onResult,
      &onException,
      rec.get(),
      kStreamId,
      evb,
      &executor,
      std::move(requestContext));

  {
    detail::HandlerExecutorScope scope(&executor);
    cb->result(123);
  }

  EXPECT_EQ(rec->resultCount, 1);
  EXPECT_EQ(rec->lastFollyRequestContext, requestFollyContext);
  EXPECT_EQ(folly::RequestContext::try_get(), callerContext);
  EXPECT_EQ(executor.drain(), 0);
}

TEST(FastHandlerCallbackTest, ExceptionRunsOnConfiguredExecutor) {
  auto rec = makeRecorder();
  folly::EventBase evb;
  folly::ManualExecutor executor;
  auto cb = makeFastHandlerCallback<FastHandlerCallback<int>>(
      &onResult, &onException, rec.get(), kStreamId, evb, &executor, nullptr);

  cb->exception(
      folly::make_exception_wrapper<TApplicationException>(
          TApplicationException::UNKNOWN_METHOD, "boom"));

  EXPECT_EQ(rec->exceptionCount, 0);
  EXPECT_EQ(executor.drain(), 1);
  EXPECT_EQ(rec->exceptionCount, 1);
  EXPECT_NE(rec->lastExceptionMessage.find("boom"), std::string::npos);
}

TEST(FastHandlerCallbackTest, AppErrorRunsOnConfiguredExecutor) {
  folly::EventBase evb;
  bool adapterDestroyed = false;
  auto rec = makeRecorder(&adapterDestroyed);
  configureClosedPipeline(evb, *rec);
  folly::ManualExecutor executor;
  auto cb = makeFastHandlerCallback<FastHandlerCallback<int>>(
      &onResult, &onException, rec.get(), kStreamId, evb, &executor, nullptr);

  cb->sendAppError(folly::make_exception_wrapper<TApplicationException>());
  cb.reset();
  rec.reset();

  EXPECT_FALSE(adapterDestroyed);
  EXPECT_EQ(executor.drain(), 1);
  EXPECT_TRUE(adapterDestroyed);
}

TEST(
    FastHandlerCallbackTest,
    ExceptionInvokesExceptionFnAndSuppressesDestructor) {
  auto rec = makeRecorder();
  folly::EventBase evb;
  {
    auto cb = makeFastHandlerCallback<FastHandlerCallback<int>>(
        &onResult, &onException, rec.get(), kStreamId, evb, nullptr, nullptr);
    cb->exception(
        folly::make_exception_wrapper<TApplicationException>(
            TApplicationException::UNKNOWN_METHOD, "boom"));
  }
  EXPECT_EQ(rec->resultCount, 0);
  EXPECT_EQ(rec->exceptionCount, 1);
  EXPECT_EQ(rec->lastStreamId, kStreamId);
  EXPECT_NE(rec->lastExceptionMessage.find("boom"), std::string::npos);
}

TEST(FastHandlerCallbackTest, DestructorFiresExceptionWhenNotCompleted) {
  auto rec = makeRecorder();
  folly::EventBase evb;
  {
    auto cb = makeFastHandlerCallback<FastHandlerCallback<int>>(
        &onResult, &onException, rec.get(), kStreamId, evb, nullptr, nullptr);
    // Drop without completing — destructor must synthesize an error so the
    // peer never hangs.
  }
  EXPECT_EQ(rec->resultCount, 0);
  EXPECT_EQ(rec->exceptionCount, 1);
  EXPECT_EQ(rec->lastStreamId, kStreamId);
  EXPECT_NE(rec->lastExceptionMessage.find("not completed"), std::string::npos);
}

TEST(FastHandlerCallbackTest, DestructorErrorRunsOnConfiguredExecutor) {
  auto rec = makeRecorder();
  folly::EventBase evb;
  folly::ManualExecutor executor;
  folly::RequestContextScopeGuard callerGuard;
  auto* const callerContext = folly::RequestContext::try_get();
  auto requestContext = makeThriftRequestContext(evb);
  auto* const requestFollyContext =
      requestContext->getFollyRequestContext().get();
  {
    auto cb = makeFastHandlerCallback<FastHandlerCallback<int>>(
        &onResult,
        &onException,
        rec.get(),
        kStreamId,
        evb,
        &executor,
        std::move(requestContext));
    cb->markHandlerStarted();
  }

  EXPECT_EQ(rec->exceptionCount, 0);
  EXPECT_EQ(executor.drain(), 1);
  EXPECT_EQ(rec->exceptionCount, 1);
  EXPECT_EQ(rec->lastFollyRequestContext, requestFollyContext);
  EXPECT_EQ(folly::RequestContext::try_get(), callerContext);
  EXPECT_NE(rec->lastExceptionMessage.find("not completed"), std::string::npos);
}

TEST(FastHandlerCallbackTest, VoidDoneInvokesDoneFnAndSuppressesDestructor) {
  auto rec = makeRecorder();
  folly::EventBase evb;
  {
    auto cb = makeFastHandlerCallback<FastHandlerCallback<void>>(
        &onDone, &onException, rec.get(), kStreamId, evb, nullptr, nullptr);
    cb->done();
  }
  EXPECT_EQ(rec->doneCount, 1);
  EXPECT_EQ(rec->exceptionCount, 0);
  EXPECT_EQ(rec->lastStreamId, kStreamId);
}

TEST(FastHandlerCallbackTest, VoidDoneRunsOnConfiguredExecutor) {
  auto rec = makeRecorder();
  folly::EventBase evb;
  folly::ManualExecutor executor;
  folly::RequestContextScopeGuard callerGuard;
  auto* const callerContext = folly::RequestContext::try_get();
  auto requestContext = makeThriftRequestContext(evb);
  auto* const requestFollyContext =
      requestContext->getFollyRequestContext().get();
  {
    auto cb = makeFastHandlerCallback<FastHandlerCallback<void>>(
        &onDone,
        &onException,
        rec.get(),
        kStreamId,
        evb,
        &executor,
        std::move(requestContext));
    cb->done();
  }

  EXPECT_EQ(rec->doneCount, 0);
  EXPECT_EQ(executor.drain(), 1);
  EXPECT_EQ(rec->doneCount, 1);
  EXPECT_EQ(rec->lastFollyRequestContext, requestFollyContext);
  EXPECT_EQ(folly::RequestContext::try_get(), callerContext);
}

TEST(FastHandlerCallbackTest, VoidExceptionRunsOnConfiguredExecutor) {
  auto rec = makeRecorder();
  folly::EventBase evb;
  folly::ManualExecutor executor;
  auto cb = makeFastHandlerCallback<FastHandlerCallback<void>>(
      &onDone, &onException, rec.get(), kStreamId, evb, &executor, nullptr);

  cb->exception(
      folly::make_exception_wrapper<TApplicationException>(
          TApplicationException::UNKNOWN_METHOD, "boom"));

  EXPECT_EQ(rec->exceptionCount, 0);
  EXPECT_EQ(executor.drain(), 1);
  EXPECT_EQ(rec->exceptionCount, 1);
  EXPECT_NE(rec->lastExceptionMessage.find("boom"), std::string::npos);
}

TEST(FastHandlerCallbackTest, VoidAppErrorRunsOnConfiguredExecutor) {
  folly::EventBase evb;
  bool adapterDestroyed = false;
  auto rec = makeRecorder(&adapterDestroyed);
  configureClosedPipeline(evb, *rec);
  folly::ManualExecutor executor;
  auto cb = makeFastHandlerCallback<FastHandlerCallback<void>>(
      &onDone, &onException, rec.get(), kStreamId, evb, &executor, nullptr);

  cb->sendAppError(folly::make_exception_wrapper<TApplicationException>());
  cb.reset();
  rec.reset();

  EXPECT_FALSE(adapterDestroyed);
  EXPECT_EQ(executor.drain(), 1);
  EXPECT_TRUE(adapterDestroyed);
}

TEST(FastHandlerCallbackTest, VoidDestructorFiresExceptionWhenNotCompleted) {
  auto rec = makeRecorder();
  folly::EventBase evb;
  {
    auto cb = makeFastHandlerCallback<FastHandlerCallback<void>>(
        &onDone, &onException, rec.get(), kStreamId, evb, nullptr, nullptr);
  }
  EXPECT_EQ(rec->doneCount, 0);
  EXPECT_EQ(rec->exceptionCount, 1);
  EXPECT_NE(rec->lastExceptionMessage.find("not completed"), std::string::npos);
}

TEST(FastHandlerCallbackTest, GetEventBaseReturnsConfiguredEventBase) {
  auto rec = makeRecorder();
  folly::EventBase evb;
  auto cb = makeFastHandlerCallback<FastHandlerCallback<int>>(
      &onResult, &onException, rec.get(), kStreamId, evb, nullptr, nullptr);
  EXPECT_EQ(cb->getEventBase(), &evb);
  cb->result(0); // suppress destructor exception
}

TEST(FastHandlerCallbackTest, RequestContextAccessorReturnsStoredPointer) {
  auto rec = makeRecorder();
  folly::EventBase evb;
  auto requestContext = makeThriftRequestContext(evb);
  auto* requestContextPtr = requestContext.get();
  auto cb = makeFastHandlerCallback<FastHandlerCallback<int>>(
      &onResult,
      &onException,
      rec.get(),
      kStreamId,
      evb,
      nullptr,
      std::move(requestContext));
  EXPECT_EQ(cb->requestContext(), requestContextPtr);
  cb->result(0); // suppress destructor exception
}

// An exception reply is a response like any other: it must carry the same
// per-request context a success reply does, or write-side handlers (checksum
// today, response headers next) silently skip every error response.
TEST(FastHandlerCallbackTest, ExceptionForwardsRequestContextToThunk) {
  auto rec = makeRecorder();
  folly::EventBase evb;
  auto requestContext = makeThriftRequestContext(evb);
  auto* requestContextPtr = requestContext.get();
  {
    auto cb = makeFastHandlerCallback<FastHandlerCallback<int>>(
        &onResult,
        &onException,
        rec.get(),
        kStreamId,
        evb,
        nullptr,
        std::move(requestContext));
    cb->exception(
        folly::make_exception_wrapper<TApplicationException>(
            TApplicationException::UNKNOWN_METHOD, "boom"));
  }
  EXPECT_EQ(rec->exceptionCount, 1);
  EXPECT_EQ(rec->lastRequestContext, requestContextPtr);
}

// The dropped-callback path synthesizes its own error, so it has to hand the
// context over too.
TEST(FastHandlerCallbackTest, UncompletedDestructorForwardsRequestContext) {
  auto rec = makeRecorder();
  folly::EventBase evb;
  auto requestContext = makeThriftRequestContext(evb);
  auto* requestContextPtr = requestContext.get();
  {
    auto cb = makeFastHandlerCallback<FastHandlerCallback<void>>(
        &onDone,
        &onException,
        rec.get(),
        kStreamId,
        evb,
        nullptr,
        std::move(requestContext));
  }
  EXPECT_EQ(rec->exceptionCount, 1);
  EXPECT_EQ(rec->lastRequestContext, requestContextPtr);
}

TEST(
    FastHandlerCallbackTest,
    OffThreadCallbackDropDestroysRequestContextOnEventBase) {
  auto rec = makeRecorder();
  folly::RequestContextScopeGuard callerGuard;
  auto* const callerContext = folly::RequestContext::try_get();
  folly::ScopedEventBaseThread evbThread;
  auto* evb = evbThread.getEventBase();
  FastHandlerCallbackPtr<int> cb;
  const ThriftRequestContext* requestContextPtr{nullptr};
  folly::RequestContext* requestFollyContext{nullptr};
  evb->runInEventBaseThreadAndWait([&] {
    auto requestContext = makeThriftRequestContext(*evb);
    requestContextPtr = requestContext.get();
    requestFollyContext = requestContext->getFollyRequestContext().get();
    cb = makeFastHandlerCallback<FastHandlerCallback<int>>(
        &onResult,
        &onException,
        rec.get(),
        kStreamId,
        *evb,
        nullptr,
        std::move(requestContext));
  });

  cb->markHandlerStarted();
  cb.reset();
  evb->runInEventBaseThreadAndWait([] {});

  EXPECT_EQ(rec->exceptionCount, 1);
  EXPECT_EQ(rec->lastRequestContext, requestContextPtr);
  EXPECT_EQ(rec->lastFollyRequestContext, requestFollyContext);
  EXPECT_EQ(folly::RequestContext::try_get(), callerContext);
}

// =============================================================================
// Force-close behavior: FHC keeps adapter alive across straggler completions
// =============================================================================

TEST(FastHandlerCallbackTest, OutlivingFHCKeepsAdapterAlive) {
  // Simulates the force-close scenario: the upper layer drops its
  // adapter Ptr while an FHC is still alive. The FHC's DG must hold
  // the adapter object alive so the straggler completion is safe.
  folly::EventBase evb;
  auto rec = makeRecorder();
  RecordingAdapter* recPtr = rec.get();

  auto cb = makeFastHandlerCallback<FastHandlerCallback<int>>(
      &onResult, &onException, recPtr, kStreamId, evb, nullptr, nullptr);

  // Owner drops its Ptr while the FHC is still alive — the FHC's
  // adapterGuard_ must hold the adapter live.
  rec.reset();

  // Recorder is still alive: the static dispatch fn writes into it
  // successfully (would UAF if adapter had been freed).
  cb->result(99);
  EXPECT_EQ(recPtr->resultCount, 1);
  EXPECT_EQ(recPtr->lastValue, 99);

  // Releasing the FHC drops the last DG; adapter dies cleanly now.
  cb.reset();
}

TEST(FastHandlerCallbackTest, UncompletedFHCDestructorIsSafeAfterOwnerDrop) {
  // FHC dtor synthesizes an INTERNAL_ERROR and dispatches it through
  // the adapter. After the upper-layer Ptr is dropped, the FHC's
  // adapterGuard_ must keep the adapter alive across this dispatch.
  // ASAN would catch a UAF here; passing means the lifetime chain is
  // correct.
  //
  // We hold a separate DG so the adapter survives past the FHC's
  // destruction, letting us inspect the recorded exception state.
  folly::EventBase evb;
  auto rec = makeRecorder();
  RecordingAdapter* recPtr = rec.get();

  auto cb = makeFastHandlerCallback<FastHandlerCallback<int>>(
      &onResult, &onException, recPtr, kStreamId, evb, nullptr, nullptr);

  // Test-only: keep the adapter alive past cb.reset() so we can
  // observe state. In production, dropping the owner Ptr leaves only
  // the FHC's DG; the adapter destructs the moment the FHC does.
  folly::DelayedDestruction::DestructorGuard keepAlive(recPtr);
  rec.reset();
  cb.reset();

  EXPECT_EQ(recPtr->exceptionCount, 1);
  EXPECT_NE(
      recPtr->lastExceptionMessage.find("not completed"), std::string::npos);
}

} // namespace apache::thrift::fast_thrift::thrift
