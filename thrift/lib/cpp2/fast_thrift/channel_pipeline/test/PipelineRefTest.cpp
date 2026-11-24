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

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineBuilder.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineRef.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/StaticPipelineBuilder.h>

#include <folly/portability/GTest.h>

namespace apache::thrift::fast_thrift::channel_pipeline::test {
namespace {

HANDLER_TAG(pipeline_ref_handler);

struct ValueEvent : EventTag<int> {};

struct Counts {
  int reads{0};
  int writes{0};
  int exceptions{0};
  int active{0};
  int inactive{0};
  int removed{0};
  int eventValue{0};
};

class RefHead {
 public:
  explicit RefHead(Counts& counts) noexcept : counts_(counts) {}

  template <typename Context>
  Result onWrite(Context&, TypeErasedBox&&) noexcept {
    ++counts_.writes;
    return Result::Success;
  }

  void onReadReady() noexcept {}
  void handlerAdded() noexcept {}
  void handlerRemoved() noexcept {}
  void onPipelineActive() noexcept {}
  void onPipelineInactive() noexcept {}

 private:
  Counts& counts_;
};

class RefTail {
 public:
  explicit RefTail(Counts& counts) noexcept : counts_(counts) {}

  template <typename Context>
  Result onRead(Context&, TypeErasedBox&&) noexcept {
    ++counts_.reads;
    return Result::Success;
  }

  void onException(folly::exception_wrapper&&) noexcept {
    ++counts_.exceptions;
  }
  void onWriteReady() noexcept {}
  void handlerAdded() noexcept {}
  void handlerRemoved() noexcept {}
  void onPipelineActive() noexcept {}
  void onPipelineInactive() noexcept {}

 private:
  Counts& counts_;
};

template <typename Context>
class RefHandler {
 public:
  using SubscribedEvents = Events<ValueEvent>;

  explicit RefHandler(Counts* counts) noexcept : counts_(counts) {}

  void handlerAdded(Context&) noexcept {}
  void handlerRemoved(Context&) noexcept { ++counts_->removed; }
  void onPipelineActive(Context&) noexcept { ++counts_->active; }
  void onPipelineInactive(Context&) noexcept { ++counts_->inactive; }
  void onReadReady(Context&) noexcept {}
  void onWriteReady(Context&) noexcept {}

  Result onRead(Context& ctx, TypeErasedBox&& message) noexcept {
    return ctx.fireRead(std::move(message));
  }

  Result onWrite(Context& ctx, TypeErasedBox&& message) noexcept {
    return ctx.fireWrite(std::move(message));
  }

  void onException(
      Context& ctx, folly::exception_wrapper&& exception) noexcept {
    ctx.fireException(std::move(exception));
  }

  template <PipelineEvent E>
    requires std::same_as<E, ValueEvent>
  void on(Context&, const int& value) noexcept {
    counts_->eventValue = value;
  }

 private:
  Counts* counts_;
};

template <typename P>
void exerciseRef(P& pipeline, Counts& counts, folly::EventBase& eventBase) {
  PipelineRef ref(pipeline);
  EXPECT_TRUE(ref);
  EXPECT_EQ(ref.eventBase(), &eventBase);
  EXPECT_EQ(ref.fireRead(TypeErasedBox{1}), Result::Success);
  EXPECT_EQ(ref.fireWrite(TypeErasedBox{2}), Result::Success);
  ref.fireException({});
  ref.activate();
  ref.deactivate();
  ref.onReadReady();
  ref.onWriteReady();
  EXPECT_GE(ref.allocate(8)->capacity(), 8);

  auto publisher = ref.bindEvents<Events<ValueEvent>>();
  publisher.fire<ValueEvent>(42);

  EXPECT_EQ(counts.reads, 1);
  EXPECT_EQ(counts.writes, 1);
  EXPECT_EQ(counts.exceptions, 1);
  EXPECT_EQ(counts.active, 1);
  EXPECT_EQ(counts.inactive, 1);
  EXPECT_EQ(counts.eventValue, 42);
}

TEST(PipelineRefTest, ErasesDynamicPipeline) {
  folly::EventBase eventBase;
  SimpleBufferAllocator allocator;
  Counts counts;
  RefHead head(counts);
  RefTail tail(counts);

  auto pipeline = PipelineBuilder<RefHead, RefTail>()
                      .setEventBase(&eventBase)
                      .setHead(&head)
                      .setTail(&tail)
                      .setAllocator(&allocator)
                      .addNextDuplex<RefHandler<detail::ContextImpl>>(
                          pipeline_ref_handler_tag, &counts)
                      .build();

  exerciseRef(*pipeline, counts, eventBase);
}

TEST(PipelineRefTest, ErasesStaticPipeline) {
  folly::EventBase eventBase;
  SimpleBufferAllocator allocator;
  Counts counts;
  RefHead head(counts);
  RefTail tail(counts);

  auto pipeline =
      StaticPipelineBuilder<RefHead, RefTail>()
          .setEventBase(&eventBase)
          .setHead(&head)
          .setTail(&tail)
          .setAllocator(&allocator)
          .addNextDuplexTemplate<RefHandler>(pipeline_ref_handler_tag, &counts)
          .build();

  exerciseRef(*pipeline, counts, eventBase);
}

template <typename P>
void expectGuardDefersDestruction(typename P::Ptr pipeline, Counts& counts) {
  PipelineRef ref(*pipeline);
  auto guard = ref.guard();
  pipeline.reset();
  EXPECT_EQ(counts.removed, 0);
  guard.reset();
  EXPECT_EQ(counts.removed, 1);
}

TEST(PipelineRefTest, GuardDefersDynamicPipelineDestruction) {
  folly::EventBase eventBase;
  SimpleBufferAllocator allocator;
  Counts counts;
  RefHead head(counts);
  RefTail tail(counts);
  auto pipeline = PipelineBuilder<RefHead, RefTail>()
                      .setEventBase(&eventBase)
                      .setHead(&head)
                      .setTail(&tail)
                      .setAllocator(&allocator)
                      .addNextDuplex<RefHandler<detail::ContextImpl>>(
                          pipeline_ref_handler_tag, &counts)
                      .build();
  expectGuardDefersDestruction<PipelineImpl>(std::move(pipeline), counts);
}

TEST(PipelineRefTest, GuardDefersStaticPipelineDestruction) {
  folly::EventBase eventBase;
  SimpleBufferAllocator allocator;
  Counts counts;
  RefHead head(counts);
  RefTail tail(counts);
  auto pipeline =
      StaticPipelineBuilder<RefHead, RefTail>()
          .setEventBase(&eventBase)
          .setHead(&head)
          .setTail(&tail)
          .setAllocator(&allocator)
          .addNextDuplexTemplate<RefHandler>(pipeline_ref_handler_tag, &counts)
          .build();
  using Pipeline = std::remove_reference_t<decltype(*pipeline)>;
  expectGuardDefersDestruction<Pipeline>(std::move(pipeline), counts);
}

template <typename P>
void expectOwnerDestroysPipeline(typename P::Ptr pipeline, Counts& counts) {
  {
    PipelineOwner owner;
    owner = std::move(pipeline);
    EXPECT_TRUE(owner);
    EXPECT_TRUE(owner.ref());

    PipelineOwner moved(std::move(owner));
    EXPECT_TRUE(moved);
    moved.reset();
    EXPECT_FALSE(moved);
  }
  EXPECT_EQ(counts.removed, 1);
}

TEST(PipelineRefTest, OwnerErasesDynamicPipeline) {
  folly::EventBase eventBase;
  SimpleBufferAllocator allocator;
  Counts counts;
  RefHead head(counts);
  RefTail tail(counts);
  auto pipeline = PipelineBuilder<RefHead, RefTail>()
                      .setEventBase(&eventBase)
                      .setHead(&head)
                      .setTail(&tail)
                      .setAllocator(&allocator)
                      .addNextDuplex<RefHandler<detail::ContextImpl>>(
                          pipeline_ref_handler_tag, &counts)
                      .build();
  expectOwnerDestroysPipeline<PipelineImpl>(std::move(pipeline), counts);
}

TEST(PipelineRefTest, OwnerErasesStaticPipeline) {
  folly::EventBase eventBase;
  SimpleBufferAllocator allocator;
  Counts counts;
  RefHead head(counts);
  RefTail tail(counts);
  auto pipeline =
      StaticPipelineBuilder<RefHead, RefTail>()
          .setEventBase(&eventBase)
          .setHead(&head)
          .setTail(&tail)
          .setAllocator(&allocator)
          .addNextDuplexTemplate<RefHandler>(pipeline_ref_handler_tag, &counts)
          .build();
  using Pipeline = std::remove_reference_t<decltype(*pipeline)>;
  expectOwnerDestroysPipeline<Pipeline>(std::move(pipeline), counts);
}

} // namespace
} // namespace apache::thrift::fast_thrift::channel_pipeline::test
