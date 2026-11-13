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

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/StaticHandler.h>

#include <folly/portability/GTest.h>

#include <tuple>

namespace apache::thrift::fast_thrift::channel_pipeline::test {
namespace {

constexpr HandlerId kHandlerId = fnv1a_hash("static_handler");
constexpr std::size_t kHandlerIndex = 3;

struct State {
  int value{0};
};

struct FakePipeline {
  template <std::size_t Index>
  static constexpr std::size_t contextHandlerIndex() noexcept {
    return Index;
  }
  template <std::size_t Index>
  void deactivateFromContext() noexcept {
    deactivateFrom<Index + 1>();
  }
  template <std::size_t>
  Result fireReadFrom(TypeErasedBox&&) noexcept {
    ++reads;
    return Result::Success;
  }
  template <std::size_t>
  Result fireWriteFrom(TypeErasedBox&&) noexcept {
    ++writes;
    return Result::Backpressure;
  }
  template <std::size_t>
  void fireExceptionFrom(folly::exception_wrapper&&) noexcept {
    ++exceptions;
  }
  template <std::size_t>
  void deactivateFrom() noexcept {
    ++deactivations;
  }
  template <PipelineEvent E>
  void fireEvent() noexcept {
    static_cast<void>(sizeof(E));
  }
  template <PipelineEvent E>
  void fireEvent(const typename E::Payload&) noexcept {
    static_cast<void>(sizeof(E));
  }
  template <
      std::size_t,
      PipelineEvent E,
      std::size_t RouteIndex,
      typename... Args>
  void firePublishedEvent(Args&&...) noexcept {
    static_cast<void>(sizeof(E));
    static_cast<void>(RouteIndex);
  }
  BytesPtr allocate(std::size_t size) noexcept {
    return folly::IOBuf::create(size);
  }
  BytesPtr copyBuffer(const void* data, std::size_t size) noexcept {
    return folly::IOBuf::copyBuffer(data, size);
  }
  folly::EventBase* eventBase() noexcept { return &evb; }
  void close() noexcept { ++closes; }
  template <std::size_t>
  void awaitWriteReady() noexcept {
    awaitingWrite = true;
  }
  template <std::size_t>
  void cancelAwaitWriteReady() noexcept {
    awaitingWrite = false;
  }
  template <std::size_t>
  bool isAwaitingWriteReady() const noexcept {
    return awaitingWrite;
  }
  template <std::size_t>
  void awaitReadReady() noexcept {
    awaitingRead = true;
  }
  template <std::size_t>
  void cancelAwaitReadReady() noexcept {
    awaitingRead = false;
  }
  template <std::size_t>
  bool isAwaitingReadReady() const noexcept {
    return awaitingRead;
  }
  template <typename T>
  T& state() noexcept {
    return std::get<T>(stateStorage);
  }
  template <typename T>
  const T& state() const noexcept {
    return std::get<T>(stateStorage);
  }

  folly::EventBase evb;
  std::tuple<State> stateStorage;
  int reads{0};
  int writes{0};
  int exceptions{0};
  int deactivations{0};
  int closes{0};
  bool awaitingWrite{false};
  bool awaitingRead{false};
};

using Context = detail::
    StaticContext<FakePipeline, kHandlerIndex, kHandlerId, std::tuple<State>>;

struct NonMovableHandler {
  explicit NonMovableHandler(int value) : value(value) {}
  NonMovableHandler(const NonMovableHandler&) = delete;
  NonMovableHandler& operator=(const NonMovableHandler&) = delete;
  NonMovableHandler(NonMovableHandler&&) = delete;
  NonMovableHandler& operator=(NonMovableHandler&&) = delete;

  int value;
};

TEST(StaticContextTest, CallsTypedPipelineDirectly) {
  FakePipeline pipeline;
  Context ctx(&pipeline);

  EXPECT_EQ(ctx.handlerId(), kHandlerId);
  EXPECT_EQ(ctx.handlerIndex(), kHandlerIndex);
  EXPECT_EQ(ctx.pipeline(), &pipeline);
  EXPECT_EQ(ctx.eventBase(), &pipeline.evb);
  EXPECT_EQ(ctx.fireRead(TypeErasedBox{}), Result::Success);
  EXPECT_EQ(ctx.fireWrite(TypeErasedBox{}), Result::Backpressure);
  ctx.fireException({});
  ctx.deactivate();
  ctx.close();

  EXPECT_EQ(pipeline.reads, 1);
  EXPECT_EQ(pipeline.writes, 1);
  EXPECT_EQ(pipeline.exceptions, 1);
  EXPECT_EQ(pipeline.deactivations, 1);
  EXPECT_EQ(pipeline.closes, 1);
}

TEST(StaticContextTest, AccessesTypedPipelineState) {
  FakePipeline pipeline;
  Context ctx(&pipeline);

  ctx.state<State>().value = 42;

  EXPECT_EQ(std::get<State>(pipeline.stateStorage).value, 42);
}

TEST(StaticContextTest, ManagesReadyRegistration) {
  FakePipeline pipeline;
  Context ctx(&pipeline);

  ctx.awaitWriteReady();
  ctx.awaitReadReady();
  EXPECT_TRUE(ctx.isAwaitingWriteReady());
  EXPECT_TRUE(ctx.isAwaitingReadReady());

  ctx.cancelAwaitWriteReady();
  ctx.cancelAwaitReadReady();
  EXPECT_FALSE(ctx.isAwaitingWriteReady());
  EXPECT_FALSE(ctx.isAwaitingReadReady());
}

TEST(StaticHandlerTest, ConstructsNonMovableHandlerInline) {
  FakePipeline pipeline;
  detail::StaticHandler<
      NonMovableHandler,
      kHandlerId,
      FakePipeline,
      kHandlerIndex,
      std::tuple<State>>
      slot(&pipeline, 7);

  EXPECT_EQ(slot.handler().value, 7);
  EXPECT_EQ(slot.context().handlerId(), kHandlerId);
}

TEST(StaticHandlerTest, EmptyHandlerAddsOnlyPipelinePointer) {
  struct EmptyHandler {};
  using Slot = detail::
      StaticHandler<EmptyHandler, kHandlerId, FakePipeline, kHandlerIndex>;

  EXPECT_EQ(sizeof(Slot), sizeof(void*));
}

} // namespace
} // namespace apache::thrift::fast_thrift::channel_pipeline::test
