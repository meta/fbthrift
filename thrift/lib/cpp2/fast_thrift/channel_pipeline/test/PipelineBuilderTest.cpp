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

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/HandlerTag.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineBuilder.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineStorage.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/test/MockAdapters.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/test/MockHandler.h>

#include <cstdint>
#include <folly/io/async/EventBase.h>
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>

#include <algorithm>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace apache::thrift::fast_thrift::channel_pipeline::test {

using namespace testing;

class alignas(64) InlineStorageTestHandler : public MockHandler {
 public:
  InlineStorageTestHandler() noexcept { lastConstructed = this; }

  explicit InlineStorageTestHandler(std::string value) noexcept
      : value(std::move(value)) {
    lastConstructed = this;
  }

  ~InlineStorageTestHandler() { ++destructions; }

  static void* operator new(std::size_t size) {
    ++heapAllocations;
    return ::operator new(size);
  }

  static void operator delete(void* ptr) noexcept { ::operator delete(ptr); }

  static void reset() noexcept {
    heapAllocations = 0;
    destructions = 0;
    lastConstructed = nullptr;
  }

  static inline std::size_t heapAllocations{0};
  static inline std::size_t destructions{0};
  static inline InlineStorageTestHandler* lastConstructed{nullptr};
  std::string value;
};

// Shared teardown log so tests can assert the relative destruction order of
// handlers and owned endpoints/allocator.
inline std::vector<std::string>& teardownLog() {
  static std::vector<std::string> log;
  return log;
}

// Handler that records its destruction in the shared teardown log. Used to
// prove an owned allocator is destroyed after all handlers.
class TeardownLoggingHandler : public MockHandler {
 public:
  ~TeardownLoggingHandler() { teardownLog().emplace_back("handler"); }
};

// Owned head endpoint with observable lifetime and constructor-argument
// capture.
class OwnedHeadEndpoint {
 public:
  OwnedHeadEndpoint() noexcept { onConstructed(); }
  OwnedHeadEndpoint(int id, std::string name) noexcept
      : id_(id), name_(std::move(name)) {
    onConstructed();
  }
  explicit OwnedHeadEndpoint(std::unique_ptr<int> token) noexcept
      : id_(*token) {
    onConstructed();
  }
  ~OwnedHeadEndpoint() {
    ++destructions;
    teardownLog().emplace_back("head");
  }

  Result onWrite(detail::ContextImpl&, TypeErasedBox&&) noexcept {
    return Result::Success;
  }
  void onReadReady() noexcept {}
  void handlerAdded() noexcept {}
  void handlerRemoved() noexcept {}
  void onPipelineActive() noexcept {}
  void onPipelineInactive() noexcept {}

  int id() const { return id_; }
  const std::string& name() const { return name_; }

  static void reset() noexcept {
    constructions = 0;
    destructions = 0;
    lastConstructed = nullptr;
  }
  static inline std::size_t constructions{0};
  static inline std::size_t destructions{0};
  static inline OwnedHeadEndpoint* lastConstructed{nullptr};

 private:
  void onConstructed() noexcept {
    ++constructions;
    lastConstructed = this;
  }
  int id_{0};
  std::string name_;
};

// Owned tail endpoint with observable lifetime.
class OwnedTailEndpoint {
 public:
  OwnedTailEndpoint() noexcept { ++constructions; }
  ~OwnedTailEndpoint() {
    ++destructions;
    teardownLog().emplace_back("tail");
  }

  Result onRead(detail::ContextImpl&, TypeErasedBox&&) noexcept {
    return Result::Success;
  }
  void onException(folly::exception_wrapper&&) noexcept {}
  void onWriteReady() noexcept {}
  void handlerAdded() noexcept {}
  void handlerRemoved() noexcept {}
  void onPipelineActive() noexcept {}
  void onPipelineInactive() noexcept {}

  static void reset() noexcept {
    constructions = 0;
    destructions = 0;
  }
  static inline std::size_t constructions{0};
  static inline std::size_t destructions{0};
};

// Owned allocator with observable lifetime; counts heap allocations so tests
// can prove inline co-location performs none.
class OwnedAllocator {
 public:
  OwnedAllocator() noexcept { lastConstructed = this; }
  ~OwnedAllocator() {
    ++destructions;
    teardownLog().emplace_back("allocator");
  }

  BytesPtr allocate(size_t size) { return folly::IOBuf::create(size); }
  BytesPtr copyBuffer(const void* data, size_t size) {
    return folly::IOBuf::copyBuffer(data, size);
  }

  static void* operator new(std::size_t size) {
    ++heapAllocations;
    return ::operator new(size);
  }
  static void operator delete(void* ptr) noexcept { ::operator delete(ptr); }

  static void reset() noexcept {
    heapAllocations = 0;
    destructions = 0;
    lastConstructed = nullptr;
  }
  static inline std::size_t heapAllocations{0};
  static inline std::size_t destructions{0};
  static inline OwnedAllocator* lastConstructed{nullptr};
};

HANDLER_TAG(codec);
HANDLER_TAG(thrift);
HANDLER_TAG(logging);

class PipelineBuilderTest : public ::testing::Test {
 protected:
  void SetUp() override {
    MockHandler::resetOrderCounter();
    InlineStorageTestHandler::reset();
    OwnedHeadEndpoint::reset();
    OwnedTailEndpoint::reset();
    OwnedAllocator::reset();
    teardownLog().clear();
    transport_.reset();
    app_.reset();
    allocator_.reset();
  }

  folly::EventBase evb_;
  MockHeadHandler transport_; // Head = transport (onWrite)
  MockTailHandler app_; // Tail = app (onRead)
  TestAllocator allocator_;
};

TEST_F(PipelineBuilderTest, BuildEmptyPipeline) {
  auto pipeline =
      PipelineBuilder<MockHeadHandler, MockTailHandler, TestAllocator>()
          .setEventBase(&evb_)
          .setHead(&transport_)
          .setTail(&app_)
          .setAllocator(&allocator_)
          .build();

  if (pipeline == nullptr) {
    ADD_FAILURE() << "build() returned a null pipeline";
    return;
  }
  EXPECT_EQ(pipeline->handlerCount(), 0);
  EXPECT_EQ(pipeline->eventBase(), &evb_);
}

TEST_F(PipelineBuilderTest, BuildSingleHandlerPipeline) {
  auto handler = std::make_unique<MockHandler>();
  auto* handler_ptr = handler.get();

  auto pipeline =
      PipelineBuilder<MockHeadHandler, MockTailHandler, TestAllocator>()
          .setEventBase(&evb_)
          .setHead(&transport_)
          .setTail(&app_)
          .setAllocator(&allocator_)
          .addNextDuplex<MockHandler>(codec_tag, std::move(handler))
          .build();

  if (pipeline == nullptr) {
    ADD_FAILURE() << "build() returned a null pipeline";
    return;
  }
  EXPECT_EQ(pipeline->handlerCount(), 1);
  EXPECT_EQ(handler_ptr->handlerAddedCount(), 1);
}

TEST_F(PipelineBuilderTest, BuildMultiHandlerPipeline) {
  auto codec = std::make_unique<MockHandler>();
  auto thrift = std::make_unique<MockHandler>();
  auto logging = std::make_unique<MockHandler>();

  auto* codec_ptr = codec.get();
  auto* thrift_ptr = thrift.get();
  auto* logging_ptr = logging.get();

  auto pipeline =
      PipelineBuilder<MockHeadHandler, MockTailHandler, TestAllocator>()
          .setEventBase(&evb_)
          .setHead(&transport_)
          .setTail(&app_)
          .setAllocator(&allocator_)
          .addNextDuplex<MockHandler>(codec_tag, std::move(codec))
          .addNextDuplex<MockHandler>(thrift_tag, std::move(thrift))
          .addNextDuplex<MockHandler>(logging_tag, std::move(logging))
          .build();

  if (pipeline == nullptr) {
    ADD_FAILURE() << "build() returned a null pipeline";
    return;
  }
  EXPECT_EQ(pipeline->handlerCount(), 3);

  // All handlers should have handlerAdded called
  EXPECT_EQ(codec_ptr->handlerAddedCount(), 1);
  EXPECT_EQ(thrift_ptr->handlerAddedCount(), 1);
  EXPECT_EQ(logging_ptr->handlerAddedCount(), 1);
}

TEST_F(PipelineBuilderTest, HandlerAddedCalledInOrder) {
  auto codec = std::make_unique<MockHandler>();
  auto thrift = std::make_unique<MockHandler>();
  auto logging = std::make_unique<MockHandler>();

  auto* codec_ptr = codec.get();
  auto* thrift_ptr = thrift.get();
  auto* logging_ptr = logging.get();

  auto pipeline =
      PipelineBuilder<MockHeadHandler, MockTailHandler, TestAllocator>()
          .setEventBase(&evb_)
          .setHead(&transport_)
          .setTail(&app_)
          .setAllocator(&allocator_)
          .addNextDuplex<MockHandler>(codec_tag, std::move(codec))
          .addNextDuplex<MockHandler>(thrift_tag, std::move(thrift))
          .addNextDuplex<MockHandler>(logging_tag, std::move(logging))
          .build();

  // handlerAdded should be called in order: codec(0), thrift(1), logging(2)
  EXPECT_EQ(codec_ptr->handlerAddedOrder(), 0);
  EXPECT_EQ(thrift_ptr->handlerAddedOrder(), 1);
  EXPECT_EQ(logging_ptr->handlerAddedOrder(), 2);
}

TEST_F(PipelineBuilderTest, ContextLookupByTag) {
  auto codec = std::make_unique<MockHandler>();

  auto pipeline =
      PipelineBuilder<MockHeadHandler, MockTailHandler, TestAllocator>()
          .setEventBase(&evb_)
          .setHead(&transport_)
          .setTail(&app_)
          .setAllocator(&allocator_)
          .addNextDuplex<MockHandler>(codec_tag, std::move(codec))
          .build();

  if (pipeline == nullptr) {
    ADD_FAILURE() << "build() returned a null pipeline";
    return;
  }
  auto* ctx = pipeline->context(codec_tag);
  if (ctx == nullptr) {
    ADD_FAILURE() << "context lookup returned null";
    return;
  }
  EXPECT_EQ(ctx->handlerId(), codec_tag.id);

  // Non-existent handler returns nullptr
  auto* missing_ctx = pipeline->context(thrift_tag);
  EXPECT_EQ(missing_ctx, nullptr);
}

TEST_F(PipelineBuilderTest, ContextLookupByRuntimeId) {
  constexpr HandlerId kInboundConstructed = fnv1a_hash("inbound_constructed");
  constexpr HandlerId kInboundOwned = fnv1a_hash("inbound_owned");
  constexpr HandlerId kOutboundConstructed = fnv1a_hash("outbound_constructed");
  constexpr HandlerId kOutboundOwned = fnv1a_hash("outbound_owned");
  constexpr HandlerId kDuplexConstructed = fnv1a_hash("duplex_constructed");
  constexpr HandlerId kDuplexOwned = fnv1a_hash("duplex_owned");

  auto pipeline =
      PipelineBuilder<MockHeadHandler, MockTailHandler, TestAllocator>()
          .setEventBase(&evb_)
          .setHead(&transport_)
          .setTail(&app_)
          .setAllocator(&allocator_)
          .addNextInbound<MockHandler>(kInboundConstructed)
          .addNextInbound<MockHandler>(
              kInboundOwned, std::make_unique<MockHandler>())
          .addNextOutbound<MockHandler>(kOutboundConstructed)
          .addNextOutbound<MockHandler>(
              kOutboundOwned, std::make_unique<MockHandler>())
          .addNextDuplex<MockHandler>(kDuplexConstructed)
          .addNextDuplex<MockHandler>(
              kDuplexOwned, std::make_unique<MockHandler>())
          .build();

  if (pipeline == nullptr) {
    ADD_FAILURE() << "build() returned a null pipeline";
    return;
  }
  for (const auto id : {
           kInboundConstructed,
           kInboundOwned,
           kOutboundConstructed,
           kOutboundOwned,
           kDuplexConstructed,
           kDuplexOwned,
       }) {
    auto* ctx = pipeline->context(id);
    if (ctx == nullptr) {
      ADD_FAILURE() << "context lookup returned null for id " << id;
      continue;
    }
    EXPECT_EQ(ctx->handlerId(), id);
  }
}

TEST_F(PipelineBuilderTest, BuildThrowsWithoutEventBase) {
  using Builder =
      PipelineBuilder<MockHeadHandler, MockTailHandler, TestAllocator>;
  EXPECT_THROW(
      Builder()
          .setHead(&transport_)
          .setTail(&app_)
          .setAllocator(&allocator_)
          .build(),
      std::runtime_error);
}

TEST_F(PipelineBuilderTest, BuildThrowsWithoutHead) {
  using Builder =
      PipelineBuilder<MockHeadHandler, MockTailHandler, TestAllocator>;
  EXPECT_THROW(
      Builder()
          .setEventBase(&evb_)
          .setTail(&app_)
          .setAllocator(&allocator_)
          .build(),
      std::runtime_error);
}

TEST_F(PipelineBuilderTest, BuildThrowsWithoutTail) {
  using Builder =
      PipelineBuilder<MockHeadHandler, MockTailHandler, TestAllocator>;
  EXPECT_THROW(
      Builder()
          .setEventBase(&evb_)
          .setHead(&transport_)
          .setAllocator(&allocator_)
          .build(),
      std::runtime_error);
}

TEST_F(PipelineBuilderTest, BuildThrowsWithoutAllocator) {
  using Builder =
      PipelineBuilder<MockHeadHandler, MockTailHandler, TestAllocator>;
  EXPECT_THROW(
      Builder().setEventBase(&evb_).setHead(&transport_).setTail(&app_).build(),
      std::runtime_error);
}

TEST_F(PipelineBuilderTest, AddErasedHandlerAppendsPrebuiltNode) {
  auto handler = std::make_unique<MockHandler>();
  auto* handler_ptr = handler.get();
  auto node =
      detail::makeHandlerNode<MockHandler>(codec_tag, std::move(handler));

  auto pipeline =
      PipelineBuilder<MockHeadHandler, MockTailHandler, TestAllocator>()
          .setEventBase(&evb_)
          .setHead(&transport_)
          .setTail(&app_)
          .setAllocator(&allocator_)
          .addErasedHandler(std::move(node))
          .build();

  if (pipeline == nullptr) {
    ADD_FAILURE() << "build() returned a null pipeline";
    return;
  }
  EXPECT_EQ(pipeline->handlerCount(), 1);
  EXPECT_EQ(handler_ptr->handlerAddedCount(), 1);
  EXPECT_NE(pipeline->context(codec_tag), nullptr);
}

TEST_F(PipelineBuilderTest, AddErasedHandlerPreservesRegistrationOrder) {
  auto typed = std::make_unique<MockHandler>();
  auto erased = std::make_unique<MockHandler>();
  auto* typed_ptr = typed.get();
  auto* erased_ptr = erased.get();
  auto node =
      detail::makeHandlerNode<MockHandler>(logging_tag, std::move(erased));

  auto pipeline =
      PipelineBuilder<MockHeadHandler, MockTailHandler, TestAllocator>()
          .setEventBase(&evb_)
          .setHead(&transport_)
          .setTail(&app_)
          .setAllocator(&allocator_)
          .addNextDuplex<MockHandler>(codec_tag, std::move(typed))
          .addErasedHandler(std::move(node))
          .build();

  if (pipeline == nullptr) {
    ADD_FAILURE() << "build() returned a null pipeline";
    return;
  }
  EXPECT_EQ(pipeline->handlerCount(), 2);
  // Typed handler registered first (order 0), erased node second (order 1):
  // addErasedHandler appends at the current tail-most position.
  EXPECT_EQ(typed_ptr->handlerAddedOrder(), 0);
  EXPECT_EQ(erased_ptr->handlerAddedOrder(), 1);
}

TEST_F(PipelineBuilderTest, AddNextDuplexWithInPlaceConstruction) {
  struct TestHandler {
    std::string name;

    explicit TestHandler(std::string n) : name(std::move(n)) {}

    Result onRead(detail::ContextImpl& ctx, TypeErasedBox&& msg) noexcept {
      return ctx.fireRead(std::move(msg));
    }
    void onException(
        detail::ContextImpl& ctx, folly::exception_wrapper e) noexcept {
      ctx.fireException(std::move(e));
    }
    void onPipelineActive(detail::ContextImpl&) noexcept {}
    void onReadReady(detail::ContextImpl&) noexcept {}

    Result onWrite(detail::ContextImpl& ctx, TypeErasedBox&& msg) noexcept {
      return ctx.fireWrite(std::move(msg));
    }
    void onWriteReady(detail::ContextImpl&) noexcept {}
    void onPipelineInactive(detail::ContextImpl&) noexcept {}

    void handlerAdded(detail::ContextImpl&) noexcept {}
    void handlerRemoved(detail::ContextImpl&) noexcept {}
  };

  auto pipeline =
      PipelineBuilder<MockHeadHandler, MockTailHandler, TestAllocator>()
          .setEventBase(&evb_)
          .setHead(&transport_)
          .setTail(&app_)
          .setAllocator(&allocator_)
          .addNextDuplex<TestHandler>(codec_tag, "my_codec")
          .build();

  if (pipeline == nullptr) {
    ADD_FAILURE() << "build() returned a null pipeline";
    return;
  }
  EXPECT_EQ(pipeline->handlerCount(), 1);
}

template <typename Storage>
PipelineImpl::Ptr buildPipelineWithHandlers(
    folly::EventBase& evb,
    MockHeadHandler& head,
    MockTailHandler& tail,
    TestAllocator& allocator,
    std::size_t handlerCount) {
  PipelineBuilder<MockHeadHandler, MockTailHandler, TestAllocator, Storage>
      builder;
  builder.setEventBase(&evb).setHead(&head).setTail(&tail).setAllocator(
      &allocator);
  for (std::size_t i = 0; i < handlerCount; ++i) {
    builder.template addNextDuplex<MockHandler>(static_cast<HandlerId>(i + 1));
  }
  return builder.build();
}

TEST_F(PipelineBuilderTest, SmallStorageBuildsEmptyPipeline) {
  EXPECT_EQ(
      buildPipelineWithHandlers<pipeline_storage::Small>(
          evb_, transport_, app_, allocator_, 0)
          ->handlerCount(),
      0);
}

TEST_F(PipelineBuilderTest, SmallStorageAcceptsEightHandlersOnly) {
  EXPECT_EQ(
      buildPipelineWithHandlers<pipeline_storage::Small>(
          evb_, transport_, app_, allocator_, 8)
          ->handlerCount(),
      8);
  EXPECT_THROW(
      buildPipelineWithHandlers<pipeline_storage::Small>(
          evb_, transport_, app_, allocator_, 9),
      std::length_error);
}

TEST_F(PipelineBuilderTest, SmallStorageRejectsOverflowWhenAdded) {
  PipelineBuilder<
      MockHeadHandler,
      MockTailHandler,
      TestAllocator,
      pipeline_storage::Small>
      builder;
  builder.setEventBase(&evb_)
      .setHead(&transport_)
      .setTail(&app_)
      .setAllocator(&allocator_);
  for (std::size_t i = 0; i < pipeline_storage::Small::maxHandlers; ++i) {
    builder.addNextDuplex<MockHandler>(static_cast<HandlerId>(i + 1));
  }

  EXPECT_THROW(
      builder.addNextDuplex<MockHandler>(
          pipeline_storage::Small::maxHandlers + 1),
      std::length_error);
  EXPECT_EQ(builder.build()->handlerCount(), 8);
}

TEST_F(PipelineBuilderTest, MediumStorageAcceptsSixteenHandlersOnly) {
  EXPECT_EQ(
      buildPipelineWithHandlers<pipeline_storage::Medium>(
          evb_, transport_, app_, allocator_, 16)
          ->handlerCount(),
      16);
  EXPECT_THROW(
      buildPipelineWithHandlers<pipeline_storage::Medium>(
          evb_, transport_, app_, allocator_, 17),
      std::length_error);
}

TEST_F(PipelineBuilderTest, CustomStorageUsesConfiguredCapacity) {
  using ThreeHandlers = pipeline_storage::Custom<3>;
  auto pipeline = buildPipelineWithHandlers<ThreeHandlers>(
      evb_, transport_, app_, allocator_, 3);
  EXPECT_EQ(pipeline->handlerCount(), 3);
  for (HandlerId id = 1; id <= 3; ++id) {
    EXPECT_NE(pipeline->context(id), nullptr);
  }

  EXPECT_THROW(
      buildPipelineWithHandlers<ThreeHandlers>(
          evb_, transport_, app_, allocator_, 4),
      std::length_error);
}

TEST_F(PipelineBuilderTest, LargeStorageHasNoInlineLimit) {
  EXPECT_EQ(
      buildPipelineWithHandlers<pipeline_storage::Large>(
          evb_, transport_, app_, allocator_, 17)
          ->handlerCount(),
      17);
}

TEST_F(PipelineBuilderTest, RejectsMoreThan63Handlers) {
  EXPECT_THAT(
      buildPipelineWithHandlers<pipeline_storage::Large>(
          evb_, transport_, app_, allocator_, 63)
          ->handlerCount(),
      Eq(63));
  EXPECT_THAT(
      [&] {
        buildPipelineWithHandlers<pipeline_storage::Large>(
            evb_, transport_, app_, allocator_, 64);
      },
      Throws<std::length_error>());
}

TEST_F(PipelineBuilderTest, AddErasedHandlerRejectsThe64thHandler) {
  PipelineBuilder<
      MockHeadHandler,
      MockTailHandler,
      TestAllocator,
      pipeline_storage::Large>
      builder;
  builder.setEventBase(&evb_)
      .setHead(&transport_)
      .setTail(&app_)
      .setAllocator(&allocator_);
  for (std::size_t i = 0; i < 63; ++i) {
    builder.addNextDuplex<MockHandler>(static_cast<HandlerId>(i + 1));
  }

  EXPECT_THAT(
      [&] {
        builder.addErasedHandler(
            detail::makeHandlerNode<MockHandler>(
                static_cast<HandlerId>(64), std::make_unique<MockHandler>()));
      },
      Throws<std::length_error>());
}

TEST_F(
    PipelineBuilderTest, InlineStorageConstructsHandlerInPipelineAllocation) {
  {
    auto pipeline = PipelineBuilder<
                        MockHeadHandler,
                        MockTailHandler,
                        TestAllocator,
                        pipeline_storage::Small>()
                        .setEventBase(&evb_)
                        .setHead(&transport_)
                        .setTail(&app_)
                        .setAllocator(&allocator_)
                        .addNextDuplex<InlineStorageTestHandler>(codec_tag)
                        .build();

    ASSERT_NE(InlineStorageTestHandler::lastConstructed, nullptr);
    EXPECT_EQ(InlineStorageTestHandler::heapAllocations, 0);
    EXPECT_EQ(
        reinterpret_cast<std::uintptr_t>(
            InlineStorageTestHandler::lastConstructed) %
            alignof(InlineStorageTestHandler),
        0);
  }
  EXPECT_EQ(InlineStorageTestHandler::destructions, 1);
}

TEST_F(PipelineBuilderTest, InlineStorageOwnsDeferredConstructorArguments) {
  PipelineBuilder<
      MockHeadHandler,
      MockTailHandler,
      TestAllocator,
      pipeline_storage::Small>
      builder;
  builder.setEventBase(&evb_)
      .setHead(&transport_)
      .setTail(&app_)
      .setAllocator(&allocator_);

  {
    std::string value{"owned"};
    builder.addNextDuplex<InlineStorageTestHandler>(codec_tag, value);
    value = "changed";
  }

  auto pipeline = builder.build();
  ASSERT_NE(InlineStorageTestHandler::lastConstructed, nullptr);
  EXPECT_EQ(InlineStorageTestHandler::lastConstructed->value, "owned");
  EXPECT_EQ(InlineStorageTestHandler::heapAllocations, 0);
}

TEST_F(PipelineBuilderTest, AutomaticStorageUsesMediumThenLarge) {
  auto build = [&](std::size_t handlerCount) {
    PipelineBuilder<
        MockHeadHandler,
        MockTailHandler,
        TestAllocator,
        pipeline_storage::Automatic>
        builder;
    builder.setEventBase(&evb_)
        .setHead(&transport_)
        .setTail(&app_)
        .setAllocator(&allocator_);
    for (std::size_t i = 0; i < handlerCount; ++i) {
      builder.addNextDuplex<InlineStorageTestHandler>(
          static_cast<HandlerId>(i + 1));
    }
    return builder.build();
  };

  {
    auto pipeline = build(9);
    EXPECT_EQ(pipeline->handlerCount(), 9);
    EXPECT_EQ(InlineStorageTestHandler::heapAllocations, 0);
  }
  EXPECT_EQ(InlineStorageTestHandler::destructions, 9);

  InlineStorageTestHandler::reset();
  {
    auto pipeline = build(17);
    EXPECT_EQ(pipeline->handlerCount(), 17);
    EXPECT_EQ(InlineStorageTestHandler::heapAllocations, 17);
  }
  EXPECT_EQ(InlineStorageTestHandler::destructions, 17);
}

// === Owning setters (emplaceHead / emplaceTail / emplaceAllocator) ===

TEST_F(PipelineBuilderTest, EmplacedHeadIsOwnedAndDestroyedWithPipeline) {
  {
    auto pipeline =
        PipelineBuilder<OwnedHeadEndpoint, MockTailHandler, TestAllocator>()
            .setEventBase(&evb_)
            .emplaceHead()
            .setTail(&app_)
            .setAllocator(&allocator_)
            .build();

    ASSERT_NE(pipeline, nullptr);
    EXPECT_EQ(OwnedHeadEndpoint::constructions, 1);
    EXPECT_EQ(OwnedHeadEndpoint::destructions, 0);
  }
  EXPECT_EQ(OwnedHeadEndpoint::destructions, 1);
}

TEST_F(PipelineBuilderTest, EmplacedTailIsOwnedAndDestroyedWithPipeline) {
  {
    auto pipeline =
        PipelineBuilder<MockHeadHandler, OwnedTailEndpoint, TestAllocator>()
            .setEventBase(&evb_)
            .setHead(&transport_)
            .emplaceTail()
            .setAllocator(&allocator_)
            .build();

    ASSERT_NE(pipeline, nullptr);
    EXPECT_EQ(OwnedTailEndpoint::constructions, 1);
    EXPECT_EQ(OwnedTailEndpoint::destructions, 0);
  }
  EXPECT_EQ(OwnedTailEndpoint::destructions, 1);
}

TEST_F(PipelineBuilderTest, EmplacedAllocatorIsOwnedAndDestroyedWithPipeline) {
  {
    auto pipeline =
        PipelineBuilder<MockHeadHandler, MockTailHandler, OwnedAllocator>()
            .setEventBase(&evb_)
            .setHead(&transport_)
            .setTail(&app_)
            .emplaceAllocator()
            .build();

    ASSERT_NE(pipeline, nullptr);
    EXPECT_EQ(OwnedAllocator::destructions, 0);
  }
  EXPECT_EQ(OwnedAllocator::destructions, 1);
}

TEST_F(PipelineBuilderTest, EmplaceForwardsConstructorArguments) {
  auto pipeline =
      PipelineBuilder<OwnedHeadEndpoint, MockTailHandler, TestAllocator>()
          .setEventBase(&evb_)
          .emplaceHead(42, std::string("codec"))
          .setTail(&app_)
          .setAllocator(&allocator_)
          .build();

  ASSERT_NE(pipeline, nullptr);
  ASSERT_NE(OwnedHeadEndpoint::lastConstructed, nullptr);
  EXPECT_EQ(OwnedHeadEndpoint::lastConstructed->id(), 42);
  EXPECT_EQ(OwnedHeadEndpoint::lastConstructed->name(), "codec");
}

TEST_F(PipelineBuilderTest, EmplaceForwardsMoveOnlyConstructorArgument) {
  auto pipeline =
      PipelineBuilder<OwnedHeadEndpoint, MockTailHandler, TestAllocator>()
          .setEventBase(&evb_)
          .emplaceHead(std::make_unique<int>(7))
          .setTail(&app_)
          .setAllocator(&allocator_)
          .build();

  ASSERT_NE(pipeline, nullptr);
  ASSERT_NE(OwnedHeadEndpoint::lastConstructed, nullptr);
  EXPECT_EQ(OwnedHeadEndpoint::lastConstructed->id(), 7);
}

TEST_F(PipelineBuilderTest, EmplaceDecayCopiesDeferredArguments) {
  PipelineBuilder<OwnedHeadEndpoint, MockTailHandler, TestAllocator> builder;
  builder.setEventBase(&evb_).setTail(&app_).setAllocator(&allocator_);
  {
    std::string value{"owned"};
    builder.emplaceHead(1, value);
    value = "changed";
  }

  auto pipeline = builder.build();
  ASSERT_NE(OwnedHeadEndpoint::lastConstructed, nullptr);
  EXPECT_EQ(OwnedHeadEndpoint::lastConstructed->name(), "owned");
}

TEST_F(PipelineBuilderTest, OwnedAllocatorDestroyedAfterHandlersAndEndpoints) {
  {
    auto pipeline =
        PipelineBuilder<OwnedHeadEndpoint, OwnedTailEndpoint, OwnedAllocator>()
            .setEventBase(&evb_)
            .emplaceHead()
            .emplaceTail()
            .emplaceAllocator()
            .addNextDuplex<TeardownLoggingHandler>(codec_tag)
            .addNextDuplex<TeardownLoggingHandler>(thrift_tag)
            .build();
    ASSERT_NE(pipeline, nullptr);
  }

  const auto& log = teardownLog();
  ASSERT_FALSE(log.empty());
  // The allocator must outlive every handler and the head/tail endpoints.
  EXPECT_EQ(log.back(), "allocator");
  EXPECT_EQ(std::count(log.begin(), log.end(), "allocator"), 1);
  EXPECT_EQ(std::count(log.begin(), log.end(), "handler"), 2);
}

TEST_F(PipelineBuilderTest, MixedOwnedAndExternalComponents) {
  OwnedAllocator externalAllocator;
  const auto baselineDestructions = OwnedAllocator::destructions;
  {
    auto pipeline =
        PipelineBuilder<OwnedHeadEndpoint, MockTailHandler, OwnedAllocator>()
            .setEventBase(&evb_)
            .emplaceHead()
            .setTail(&app_)
            .setAllocator(&externalAllocator)
            .build();
    ASSERT_NE(pipeline, nullptr);
  }

  // Owned head is torn down by the pipeline; the external allocator is not.
  EXPECT_EQ(OwnedHeadEndpoint::destructions, 1);
  EXPECT_EQ(OwnedAllocator::destructions, baselineDestructions);
}

TEST_F(PipelineBuilderTest, SetterAfterEmplaceDropsDeferredConstruction) {
  OwnedHeadEndpoint externalHead;
  // Isolate the counters from the external object's own construction.
  OwnedHeadEndpoint::reset();

  auto pipeline =
      PipelineBuilder<OwnedHeadEndpoint, MockTailHandler, TestAllocator>()
          .setEventBase(&evb_)
          .emplaceHead(99, std::string("dropped"))
          .setHead(&externalHead)
          .setTail(&app_)
          .setAllocator(&allocator_)
          .build();

  ASSERT_NE(pipeline, nullptr);
  // Overriding a deferred emplace with an external pointer must not construct
  // the deferred object at all.
  EXPECT_EQ(OwnedHeadEndpoint::constructions, 0);
}

template <typename Storage>
void runEmplacedHeadLifetimeTest(folly::EventBase& evb, MockTailHandler& tail) {
  TestAllocator allocator;
  {
    auto pipeline = PipelineBuilder<
                        OwnedHeadEndpoint,
                        MockTailHandler,
                        TestAllocator,
                        Storage>()
                        .setEventBase(&evb)
                        .emplaceHead()
                        .setTail(&tail)
                        .setAllocator(&allocator)
                        .build();
    ASSERT_NE(pipeline, nullptr);
    EXPECT_EQ(OwnedHeadEndpoint::destructions, 0);
  }
  EXPECT_EQ(OwnedHeadEndpoint::destructions, 1);
}

TEST_F(PipelineBuilderTest, EmplacedHeadLifetimeAcrossStorageFlavors) {
  runEmplacedHeadLifetimeTest<pipeline_storage::Large>(evb_, app_);
  OwnedHeadEndpoint::reset();
  runEmplacedHeadLifetimeTest<pipeline_storage::Small>(evb_, app_);
  OwnedHeadEndpoint::reset();
  runEmplacedHeadLifetimeTest<pipeline_storage::Automatic>(evb_, app_);
}

TEST_F(PipelineBuilderTest, InlineStorageConstructsAllocatorWithoutHeap) {
  {
    auto pipeline = PipelineBuilder<
                        MockHeadHandler,
                        MockTailHandler,
                        OwnedAllocator,
                        pipeline_storage::Small>()
                        .setEventBase(&evb_)
                        .setHead(&transport_)
                        .setTail(&app_)
                        .emplaceAllocator()
                        .build();
    ASSERT_NE(pipeline, nullptr);
    EXPECT_EQ(OwnedAllocator::heapAllocations, 0);
  }
  EXPECT_EQ(OwnedAllocator::destructions, 1);
}

} // namespace apache::thrift::fast_thrift::channel_pipeline::test
