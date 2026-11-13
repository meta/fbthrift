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
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/test/MockAdapters.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/test/MockHandler.h>

#include <folly/io/async/EventBase.h>
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>

#include <deque>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace apache::thrift::fast_thrift::channel_pipeline::test {

using namespace testing;

HANDLER_TAG(head);
HANDLER_TAG(middle);
HANDLER_TAG(tail);

// An inbound-only handler: it has no onWriteReady(), and it asks for it on
// each read.
class InboundAwaitingWriteReady {
 public:
  void onPipelineActive(detail::ContextImpl&) noexcept {}
  Result onRead(detail::ContextImpl& ctx, TypeErasedBox&& msg) noexcept {
    ctx.awaitWriteReady();
    return ctx.fireRead(std::move(msg));
  }
  void onReadReady(detail::ContextImpl&) noexcept {}
  void onException(
      detail::ContextImpl& ctx, folly::exception_wrapper&& e) noexcept {
    ctx.fireException(std::move(e));
  }
  void handlerAdded(detail::ContextImpl&) noexcept {}
  void handlerRemoved(detail::ContextImpl&) noexcept {}
};

// An outbound-only handler: it has no onReadReady(), and it asks for it on
// each write.
class OutboundAwaitingReadReady {
 public:
  Result onWrite(detail::ContextImpl& ctx, TypeErasedBox&& msg) noexcept {
    ctx.awaitReadReady();
    return ctx.fireWrite(std::move(msg));
  }
  void onWriteReady(detail::ContextImpl&) noexcept {}
  void onPipelineInactive(detail::ContextImpl&) noexcept {}
  void handlerAdded(detail::ContextImpl&) noexcept {}
  void handlerRemoved(detail::ContextImpl&) noexcept {}
};

// The same handlers with the ready callback that each one asks for. Neither
// satisfies the handler concept of that direction.
class InboundWithWriteReadyCallback : public InboundAwaitingWriteReady {
 public:
  void onWriteReady(detail::ContextImpl&) noexcept {}
};

class OutboundWithReadReadyCallback : public OutboundAwaitingReadReady {
 public:
  void onReadReady(detail::ContextImpl&) noexcept {}
};

class BackpressureTest : public ::testing::Test {
 protected:
  void SetUp() override {
    MockHandler::resetOrderCounter();
    transport_.reset();
    app_.reset();
    allocator_.reset();
  }

  PipelineImpl::Ptr buildPipeline() {
    return PipelineBuilder<MockHeadHandler, MockTailHandler, TestAllocator>()
        .setEventBase(&evb_)
        .setHead(&app_)
        .setTail(&transport_)
        .setAllocator(&allocator_)
        .addNextDuplex<MockHandler>(head_tag, std::move(head_handler_))
        .addNextDuplex<MockHandler>(middle_tag, std::move(middle_handler_))
        .addNextDuplex<MockHandler>(tail_tag, std::move(tail_handler_))
        .build();
  }

  // Sends one read and one write through a pipeline with Inbound on the head
  // side and Outbound on the tail side. Returns both results and whether the
  // pipeline then waits for write ready and for read ready.
  template <typename Inbound, typename Outbound>
  std::tuple<Result, Result, bool, bool> readAndWriteThrough() {
    auto pipeline =
        PipelineBuilder<MockHeadHandler, MockTailHandler, TestAllocator>()
            .setEventBase(&evb_)
            .setHead(&app_)
            .setTail(&transport_)
            .setAllocator(&allocator_)
            .addNextInbound<Inbound>(head_tag)
            .template addNextOutbound<Outbound>(tail_tag)
            .build();
    // Braced initialization evaluates the calls from left to right.
    return std::tuple{
        pipeline->fireRead(TypeErasedBox(folly::IOBuf::create(64))),
        pipeline->fireWrite(TypeErasedBox(folly::IOBuf::create(64))),
        pipeline->hasPendingWriteReady(),
        pipeline->hasPendingReadReady()};
  }

  void createHandlers() {
    head_handler_ = std::make_unique<MockHandler>();
    middle_handler_ = std::make_unique<MockHandler>();
    tail_handler_ = std::make_unique<MockHandler>();
    head_ptr_ = head_handler_.get();
    middle_ptr_ = middle_handler_.get();
    tail_ptr_ = tail_handler_.get();
  }

  folly::EventBase evb_;
  MockTailHandler transport_;
  MockHeadHandler app_;
  TestAllocator allocator_;

  std::unique_ptr<MockHandler> head_handler_;
  std::unique_ptr<MockHandler> middle_handler_;
  std::unique_ptr<MockHandler> tail_handler_;
  MockHandler* head_ptr_{nullptr};
  MockHandler* middle_ptr_{nullptr};
  MockHandler* tail_ptr_{nullptr};
  PipelineImpl::Ptr pipeline_;
};

namespace {

// Makes a handler hold every write and answer Backpressure until its first
// onWriteReady(), which forwards what it holds. Later writes go straight
// through.
struct WriteHolder {
  void install(MockHandler& handler) {
    handler.setOnWrite([this](detail::ContextImpl& ctx, TypeErasedBox&& msg) {
      if (!blocked) {
        return ctx.fireWrite(std::move(msg));
      }
      held.push_back(std::move(msg));
      ctx.awaitWriteReady();
      return Result::Backpressure;
    });

    handler.setOnWriteReady([this](detail::ContextImpl& ctx) {
      blocked = false;
      ctx.cancelAwaitWriteReady();
      for (TypeErasedBox& msg : held) {
        (void)ctx.fireWrite(std::move(msg));
      }
      held.clear();
    });
  }

  bool blocked{true};
  std::vector<TypeErasedBox> held;
};

// Makes a handler queue every write and wait for write-ready. When it wakes, it
// writes the queued messages until a write returns Backpressure, and records
// each result.
struct QueuedWriter {
  void install(MockHandler& handler) {
    handler.setOnWrite([this](detail::ContextImpl& ctx, TypeErasedBox&& msg) {
      queued.push_back(std::move(msg));
      ctx.awaitWriteReady();
      return Result::Backpressure;
    });

    handler.setOnWriteReady([this](detail::ContextImpl& ctx) {
      ctx.cancelAwaitWriteReady();
      while (!queued.empty()) {
        const Result result = ctx.fireWrite(std::move(queued.front()));
        queued.pop_front();
        results.push_back(result);
        if (result == Result::Backpressure) {
          ctx.awaitWriteReady();
          break;
        }
      }
    });
  }

  std::deque<TypeErasedBox> queued;
  std::vector<Result> results;
};

// Makes a handler hold every read and answer Backpressure until its first
// onReadReady(), which forwards what it holds. Later reads go straight through.
struct ReadHolder {
  void install(MockHandler& handler) {
    handler.setOnRead([this](detail::ContextImpl& ctx, TypeErasedBox&& msg) {
      if (!blocked) {
        return ctx.fireRead(std::move(msg));
      }
      held.push_back(std::move(msg));
      ctx.awaitReadReady();
      return Result::Backpressure;
    });

    handler.setOnReadReady([this](detail::ContextImpl& ctx) {
      blocked = false;
      ctx.cancelAwaitReadReady();
      for (TypeErasedBox& msg : held) {
        (void)ctx.fireRead(std::move(msg));
      }
      held.clear();
    });
  }

  bool blocked{true};
  std::vector<TypeErasedBox> held;
};

// Makes a handler queue every read and wait for read-ready. When it wakes, it
// reads the queued messages on until a read returns Backpressure, and records
// each result.
struct QueuedReader {
  void install(MockHandler& handler) {
    handler.setOnRead([this](detail::ContextImpl& ctx, TypeErasedBox&& msg) {
      queued.push_back(std::move(msg));
      ctx.awaitReadReady();
      return Result::Backpressure;
    });

    handler.setOnReadReady([this](detail::ContextImpl& ctx) {
      ctx.cancelAwaitReadReady();
      while (!queued.empty()) {
        const Result result = ctx.fireRead(std::move(queued.front()));
        queued.pop_front();
        results.push_back(result);
        if (result == Result::Backpressure) {
          ctx.awaitReadReady();
          break;
        }
      }
    });
  }

  std::deque<TypeErasedBox> queued;
  std::vector<Result> results;
};

} // namespace

// ==================== Explicit Backpressure API Tests ====================

TEST_F(BackpressureTest, HeadEndpointCanAwaitWriteReady) {
  app_.setOnWriteWithContextCallback(
      [](detail::ContextImpl& ctx, TypeErasedBox&&) {
        ctx.awaitWriteReady();
        return Result::Backpressure;
      });
  app_.setOnWriteReadyCallback(
      [](detail::ContextImpl& ctx) { ctx.cancelAwaitWriteReady(); });

  auto pipeline =
      PipelineBuilder<MockHeadHandler, MockTailHandler, TestAllocator>()
          .setEventBase(&evb_)
          .setHead(&app_)
          .setTail(&transport_)
          .setAllocator(&allocator_)
          .build();

  EXPECT_EQ(
      pipeline->fireWrite(TypeErasedBox(folly::IOBuf::create(64))),
      Result::Backpressure);
  EXPECT_TRUE(pipeline->hasPendingWriteReady());

  pipeline->onWriteReady();

  EXPECT_EQ(app_.onWriteReadyCount(), 1);
  EXPECT_FALSE(pipeline->hasPendingWriteReady());
}

TEST_F(BackpressureTest, AwaitWriteReadyRegistersHandler) {
  createHandlers();

  middle_handler_->setOnWrite([](detail::ContextImpl& ctx, TypeErasedBox&&) {
    ctx.awaitWriteReady();
    return Result::Backpressure;
  });

  auto pipeline = buildPipeline();

  EXPECT_FALSE(pipeline->hasPendingWriteReady());

  auto buf = folly::IOBuf::create(64);
  auto result = pipeline->fireWrite(TypeErasedBox(std::move(buf)));

  EXPECT_EQ(result, Result::Backpressure);
  EXPECT_TRUE(pipeline->hasPendingWriteReady());
}

TEST_F(BackpressureTest, CancelAwaitWriteReadyUnregistersHandler) {
  createHandlers();

  middle_handler_->setOnWrite([](detail::ContextImpl& ctx, TypeErasedBox&&) {
    ctx.awaitWriteReady();
    return Result::Backpressure;
  });

  middle_handler_->setOnWriteReady(
      [](detail::ContextImpl& ctx) { ctx.cancelAwaitWriteReady(); });

  auto pipeline = buildPipeline();

  auto buf = folly::IOBuf::create(64);
  (void)pipeline->fireWrite(TypeErasedBox(std::move(buf)));

  EXPECT_TRUE(pipeline->hasPendingWriteReady());

  pipeline->onWriteReady();

  EXPECT_FALSE(pipeline->hasPendingWriteReady());
}

TEST_F(BackpressureTest, IsAwaitingWriteReadyReturnsCorrectState) {
  createHandlers();

  bool was_awaiting_before = false;
  bool was_awaiting_after = false;

  middle_handler_->setOnWrite([&](detail::ContextImpl& ctx, TypeErasedBox&&) {
    was_awaiting_before = ctx.isAwaitingWriteReady();
    ctx.awaitWriteReady();
    was_awaiting_after = ctx.isAwaitingWriteReady();
    return Result::Backpressure;
  });

  auto pipeline = buildPipeline();

  auto buf = folly::IOBuf::create(64);
  (void)pipeline->fireWrite(TypeErasedBox(std::move(buf)));

  EXPECT_FALSE(was_awaiting_before);
  EXPECT_TRUE(was_awaiting_after);
}

// ==================== Write Backpressure Tests ====================

TEST_F(BackpressureTest, HandlerReceivesWriteReadyAfterBackpressure) {
  createHandlers();

  bool write_ready_called = false;
  middle_handler_->setOnWrite([](detail::ContextImpl& ctx, TypeErasedBox&&) {
    ctx.awaitWriteReady();
    return Result::Backpressure;
  });
  middle_handler_->setOnWriteReady([&](detail::ContextImpl& ctx) {
    write_ready_called = true;
    ctx.cancelAwaitWriteReady();
  });

  auto pipeline = buildPipeline();

  auto buf = folly::IOBuf::create(64);
  (void)pipeline->fireWrite(TypeErasedBox(std::move(buf)));

  EXPECT_FALSE(write_ready_called);
  EXPECT_EQ(middle_ptr_->writeReadyCount(), 0);

  pipeline->onWriteReady();

  EXPECT_TRUE(write_ready_called);
  EXPECT_EQ(middle_ptr_->writeReadyCount(), 1);
}

TEST_F(BackpressureTest, OnWriteReadyCallsTailOnWriteReady) {
  createHandlers();
  auto pipeline = buildPipeline();

  EXPECT_EQ(transport_.onWriteReadyCount(), 0);

  pipeline->onWriteReady();

  EXPECT_EQ(transport_.onWriteReadyCount(), 1);
}

TEST_F(BackpressureTest, OnlyRegisteredHandlerReceivesWriteReady) {
  createHandlers();

  // Only middle handler registers for write ready
  middle_handler_->setOnWrite([](detail::ContextImpl& ctx, TypeErasedBox&&) {
    ctx.awaitWriteReady();
    return Result::Backpressure;
  });
  middle_handler_->setOnWriteReady(
      [](detail::ContextImpl& ctx) { ctx.cancelAwaitWriteReady(); });

  // Head and tail don't register
  head_handler_->setOnWrite(
      [](detail::ContextImpl&, TypeErasedBox&&) { return Result::Success; });
  tail_handler_->setOnWrite([](detail::ContextImpl& ctx, TypeErasedBox&& msg) {
    return ctx.fireWrite(std::move(msg));
  });

  auto pipeline = buildPipeline();

  auto buf = folly::IOBuf::create(64);
  (void)pipeline->fireWrite(TypeErasedBox(std::move(buf)));
  pipeline->onWriteReady();

  // Only middle registered, so only middle receives callback
  EXPECT_EQ(head_ptr_->writeReadyCount(), 0);
  EXPECT_EQ(middle_ptr_->writeReadyCount(), 1);
  EXPECT_EQ(tail_ptr_->writeReadyCount(), 0);
}

TEST_F(BackpressureTest, HeadEndpointReceivesWriteReadyBeforeHandlers) {
  createHandlers();
  std::vector<std::string> readinessOrder;

  middle_handler_->setOnWrite(
      [](detail::ContextImpl& ctx, TypeErasedBox&& msg) {
        ctx.awaitWriteReady();
        return ctx.fireWrite(std::move(msg));
      });
  middle_handler_->setOnWriteReady([&](detail::ContextImpl& ctx) {
    readinessOrder.emplace_back("handler");
    ctx.cancelAwaitWriteReady();
  });
  app_.setOnWriteWithContextCallback(
      [](detail::ContextImpl& ctx, TypeErasedBox&&) {
        ctx.awaitWriteReady();
        return Result::Backpressure;
      });
  app_.setOnWriteReadyCallback([&](detail::ContextImpl& ctx) {
    readinessOrder.emplace_back("head");
    ctx.cancelAwaitWriteReady();
  });

  auto pipeline = buildPipeline();
  EXPECT_EQ(
      pipeline->fireWrite(TypeErasedBox(folly::IOBuf::create(64))),
      Result::Backpressure);

  pipeline->onWriteReady();

  EXPECT_EQ(readinessOrder, (std::vector<std::string>{"head", "handler"}));
}

TEST_F(BackpressureTest, WriteReadyWakesHandlersFromHeadToTail) {
  createHandlers();
  std::vector<std::string> readiness;
  for (auto [handler, name] :
       {std::pair{head_handler_.get(), "head"},
        std::pair{middle_handler_.get(), "middle"},
        std::pair{tail_handler_.get(), "tail"}}) {
    handler->setOnWrite([](detail::ContextImpl& ctx, TypeErasedBox&& msg) {
      ctx.awaitWriteReady();
      return ctx.fireWrite(std::move(msg));
    });
    handler->setOnWriteReady(
        [&readiness, label = name](detail::ContextImpl& ctx) {
          readiness.emplace_back(label);
          ctx.cancelAwaitWriteReady();
        });
  }

  auto pipeline = buildPipeline();
  // The write reaches the handlers from the tail toward the head, so they arm
  // in the order tail, middle, head.
  EXPECT_THAT(
      pipeline->fireWrite(TypeErasedBox(folly::IOBuf::create(64))),
      Eq(Result::Success));
  pipeline->onWriteReady();

  EXPECT_THAT(readiness, ElementsAre("head", "middle", "tail"));
}

TEST_F(BackpressureTest, HandlerWithoutReadyCallbackCannotAwaitIt) {
  EXPECT_THAT(
      (readAndWriteThrough<
          InboundAwaitingWriteReady,
          OutboundAwaitingReadReady>()),
      Eq(std::tuple(Result::Success, Result::Success, false, false)));
}

TEST_F(BackpressureTest, ReadyCallbackAloneCannotAwaitIt) {
  EXPECT_THAT(
      (readAndWriteThrough<
          InboundWithWriteReadyCallback,
          OutboundWithReadReadyCallback>()),
      Eq(std::tuple(Result::Success, Result::Success, false, false)));
}

TEST_F(BackpressureTest, WriteReadyWakesHandlerAboveOneThatStaysArmed) {
  createHandlers();
  std::vector<std::string> readiness;
  for (MockHandler* handler : {head_handler_.get(), middle_handler_.get()}) {
    handler->setOnWrite([](detail::ContextImpl& ctx, TypeErasedBox&& msg) {
      ctx.awaitWriteReady();
      return ctx.fireWrite(std::move(msg));
    });
  }
  head_handler_->setOnWriteReady(
      [&readiness](detail::ContextImpl&) { readiness.emplace_back("below"); });
  middle_handler_->setOnWriteReady([&readiness](detail::ContextImpl& ctx) {
    readiness.emplace_back("above");
    ctx.cancelAwaitWriteReady();
  });

  auto pipeline = buildPipeline();
  EXPECT_THAT(
      pipeline->fireWrite(TypeErasedBox(folly::IOBuf::create(64))),
      Eq(Result::Success));
  pipeline->onWriteReady();

  // The handler below stays armed, and the handler above and the tail still
  // wake in the same pass.
  EXPECT_THAT(
      std::tuple(readiness, transport_.onWriteReadyCount()),
      Eq(std::tuple(std::vector<std::string>{"below", "above"}, 1)));
}

TEST_F(BackpressureTest, HandlerArmedBehindThePassWakesInTheNextPass) {
  createHandlers();
  WriteHolder below;
  below.install(*head_handler_);
  QueuedWriter above;
  above.install(*middle_handler_);
  auto pipeline = buildPipeline();
  // The writer above queues two messages. The handler below does not wait when
  // the pass starts.
  EXPECT_THAT(
      pipeline->fireWrite(TypeErasedBox(folly::IOBuf::create(64))),
      Eq(Result::Backpressure));
  EXPECT_THAT(
      pipeline->fireWrite(TypeErasedBox(folly::IOBuf::create(64))),
      Eq(Result::Backpressure));

  // The writer wakes and writes into the handler below, which holds the
  // message and starts to wait behind the pass. The pass does not wake it
  // again and notifies the tail.
  pipeline->onWriteReady();
  const auto afterPass = std::tuple(
      above.results,
      app_.writeCount(),
      pipeline->hasPendingWriteReady(),
      transport_.onWriteReadyCount());
  // The next pass wakes the handler below and then the writer.
  pipeline->onWriteReady();

  EXPECT_THAT(
      std::tuple(
          afterPass,
          above.results,
          app_.writeCount(),
          pipeline->hasPendingWriteReady(),
          transport_.onWriteReadyCount()),
      Eq(std::tuple(
          std::tuple(std::vector{Result::Backpressure}, 0, true, 1),
          std::vector{Result::Backpressure, Result::Success},
          2,
          false,
          2)));
}

TEST_F(BackpressureTest, HandlerThatArmsAgainInItsCallbackWakesOncePerPass) {
  createHandlers();
  head_handler_->setOnWrite([](detail::ContextImpl& ctx, TypeErasedBox&& msg) {
    ctx.awaitWriteReady();
    return ctx.fireWrite(std::move(msg));
  });
  // The handler arms again in each callback. After 3 wakes it stops, so that a
  // pass that wakes it again cannot loop forever.
  int wakes = 0;
  head_handler_->setOnWriteReady([&wakes](detail::ContextImpl& ctx) {
    ctx.cancelAwaitWriteReady();
    if (++wakes < 3) {
      ctx.awaitWriteReady();
    }
  });

  auto pipeline = buildPipeline();
  EXPECT_THAT(
      pipeline->fireWrite(TypeErasedBox(folly::IOBuf::create(64))),
      Eq(Result::Success));
  pipeline->onWriteReady();

  EXPECT_THAT(
      std::tuple(
          wakes,
          transport_.onWriteReadyCount(),
          pipeline->hasPendingWriteReady()),
      Eq(std::tuple(1, 1, true)));
}

TEST_F(BackpressureTest, HandlerArmedAboveThePassWakesInIt) {
  createHandlers();
  std::vector<std::string> readiness;
  head_handler_->setOnWriteReady([&readiness](detail::ContextImpl& ctx) {
    readiness.emplace_back("below");
    ctx.cancelAwaitWriteReady();
    ctx.pipeline()->context(middle_tag)->awaitWriteReady();
  });
  middle_handler_->setOnWriteReady([&readiness](detail::ContextImpl& ctx) {
    readiness.emplace_back("above");
    ctx.cancelAwaitWriteReady();
  });
  transport_.setOnWriteReadyCallback(
      [&readiness] { readiness.emplace_back("tail"); });
  auto pipeline = buildPipeline();
  pipeline->context(head_tag)->awaitWriteReady();

  // The handler below makes the handler above wait during the pass, and the
  // pass wakes it before the tail.
  pipeline->onWriteReady();

  EXPECT_THAT(
      std::tuple(readiness, pipeline->hasPendingWriteReady()),
      Eq(std::tuple(
          std::vector<std::string>{"below", "above", "tail"}, false)));
}

TEST_F(BackpressureTest, HeadEndpointRearmDefersUpstreamWriteReady) {
  createHandlers();
  bool headStillBackpressured = true;

  middle_handler_->setOnWrite(
      [](detail::ContextImpl& ctx, TypeErasedBox&& msg) {
        ctx.awaitWriteReady();
        return ctx.fireWrite(std::move(msg));
      });
  middle_handler_->setOnWriteReady(
      [](detail::ContextImpl& ctx) { ctx.cancelAwaitWriteReady(); });
  app_.setOnWriteWithContextCallback(
      [](detail::ContextImpl& ctx, TypeErasedBox&&) {
        ctx.awaitWriteReady();
        return Result::Backpressure;
      });
  app_.setOnWriteReadyCallback([&](detail::ContextImpl& ctx) {
    ctx.cancelAwaitWriteReady();
    if (headStillBackpressured) {
      ctx.awaitWriteReady();
    }
  });

  auto pipeline = buildPipeline();
  EXPECT_EQ(
      pipeline->fireWrite(TypeErasedBox(folly::IOBuf::create(64))),
      Result::Backpressure);

  pipeline->onWriteReady();

  EXPECT_EQ(app_.onWriteReadyCount(), 1);
  EXPECT_EQ(middle_ptr_->writeReadyCount(), 0);
  EXPECT_TRUE(pipeline->isHeadWriteBackpressured());

  headStillBackpressured = false;
  pipeline->onWriteReady();

  EXPECT_EQ(app_.onWriteReadyCount(), 2);
  EXPECT_EQ(middle_ptr_->writeReadyCount(), 1);
  EXPECT_FALSE(pipeline->hasPendingWriteReady());
}

TEST_F(BackpressureTest, HandlerHeadBackpressureDefersRemainingProducers) {
  createHandlers();

  middle_handler_->setOnWrite([](detail::ContextImpl& ctx, TypeErasedBox&&) {
    ctx.awaitWriteReady();
    return Result::Backpressure;
  });
  middle_handler_->setOnWriteReady([](detail::ContextImpl& ctx) {
    ctx.cancelAwaitWriteReady();
    EXPECT_EQ(
        ctx.fireWrite(TypeErasedBox(folly::IOBuf::create(64))),
        Result::Backpressure);
  });
  app_.setOnWriteWithContextCallback(
      [](detail::ContextImpl& ctx, TypeErasedBox&&) {
        ctx.awaitWriteReady();
        return Result::Backpressure;
      });
  app_.setOnWriteReadyCallback(
      [](detail::ContextImpl& ctx) { ctx.cancelAwaitWriteReady(); });

  auto pipeline = buildPipeline();
  EXPECT_EQ(
      pipeline->fireWrite(TypeErasedBox(folly::IOBuf::create(64))),
      Result::Backpressure);

  pipeline->onWriteReady();

  EXPECT_EQ(middle_ptr_->writeReadyCount(), 1);
  EXPECT_EQ(app_.writeCount(), 1);
  EXPECT_EQ(transport_.onWriteReadyCount(), 0);
  EXPECT_TRUE(pipeline->isHeadWriteBackpressured());

  pipeline->onWriteReady();

  EXPECT_EQ(app_.onWriteReadyCount(), 1);
  EXPECT_EQ(transport_.onWriteReadyCount(), 1);
  EXPECT_FALSE(pipeline->hasPendingWriteReady());
}

TEST_F(BackpressureTest, ReentrantWriteReadyIsCoalesced) {
  createHandlers();
  bool requestedNestedDispatch = false;

  head_handler_->setOnWrite([](detail::ContextImpl& ctx, TypeErasedBox&& msg) {
    ctx.awaitWriteReady();
    return ctx.fireWrite(std::move(msg));
  });
  middle_handler_->setOnWrite(
      [](detail::ContextImpl& ctx, TypeErasedBox&& msg) {
        ctx.awaitWriteReady();
        return ctx.fireWrite(std::move(msg));
      });
  head_handler_->setOnWriteReady([&](detail::ContextImpl& ctx) {
    ctx.cancelAwaitWriteReady();
    if (!requestedNestedDispatch) {
      requestedNestedDispatch = true;
      ctx.pipeline()->onWriteReady();
    }
  });
  middle_handler_->setOnWriteReady(
      [](detail::ContextImpl& ctx) { ctx.cancelAwaitWriteReady(); });

  auto pipeline = buildPipeline();
  EXPECT_EQ(
      pipeline->fireWrite(TypeErasedBox(folly::IOBuf::create(64))),
      Result::Success);

  pipeline->onWriteReady();

  EXPECT_TRUE(requestedNestedDispatch);
  EXPECT_EQ(head_ptr_->writeReadyCount(), 1);
  EXPECT_EQ(middle_ptr_->writeReadyCount(), 1);
  EXPECT_EQ(transport_.onWriteReadyCount(), 2);
  EXPECT_FALSE(pipeline->hasPendingWriteReady());
}

TEST_F(BackpressureTest, WriteReadyRequestedDuringAPassThatStopsIsDropped) {
  createHandlers();
  app_.setOnWriteWithContextCallback(
      [](detail::ContextImpl& ctx, TypeErasedBox&&) {
        ctx.awaitWriteReady();
        return Result::Backpressure;
      });
  app_.setOnWriteReadyCallback(
      [](detail::ContextImpl& ctx) { ctx.cancelAwaitWriteReady(); });

  // The handler asks for another pass. Its write then makes the head endpoint
  // wait again, which stops this pass.
  head_handler_->setOnWriteReady([](detail::ContextImpl& ctx) {
    ctx.cancelAwaitWriteReady();
    ctx.pipeline()->onWriteReady();
    (void)ctx.fireWrite(TypeErasedBox(folly::IOBuf::create(64)));
  });

  auto pipeline = buildPipeline();
  pipeline->context(head_tag)->awaitWriteReady();

  pipeline->onWriteReady();

  EXPECT_THAT(
      std::tuple(
          head_ptr_->writeReadyCount(),
          app_.onWriteReadyCount(),
          transport_.onWriteReadyCount(),
          pipeline->isHeadWriteBackpressured()),
      Eq(std::tuple(1, 0, 0, true)));
}

TEST_F(BackpressureTest, MultipleHandlersCanRegisterForWriteReady) {
  createHandlers();

  // Both middle and tail register for write ready
  middle_handler_->setOnWrite(
      [](detail::ContextImpl& ctx, TypeErasedBox&& msg) {
        ctx.awaitWriteReady();
        (void)ctx.fireWrite(std::move(msg)); // Forward to next
        return Result::Backpressure;
      });
  middle_handler_->setOnWriteReady(
      [](detail::ContextImpl& ctx) { ctx.cancelAwaitWriteReady(); });

  head_handler_->setOnWrite([](detail::ContextImpl& ctx, TypeErasedBox&&) {
    ctx.awaitWriteReady();
    return Result::Backpressure;
  });
  head_handler_->setOnWriteReady(
      [](detail::ContextImpl& ctx) { ctx.cancelAwaitWriteReady(); });

  auto pipeline = buildPipeline();

  auto buf = folly::IOBuf::create(64);
  (void)pipeline->fireWrite(TypeErasedBox(std::move(buf)));

  EXPECT_TRUE(pipeline->hasPendingWriteReady());

  pipeline->onWriteReady();

  // Both handlers that registered receive callback
  EXPECT_EQ(head_ptr_->writeReadyCount(), 1);
  EXPECT_EQ(middle_ptr_->writeReadyCount(), 1);
  EXPECT_EQ(tail_ptr_->writeReadyCount(), 0);
}

TEST_F(BackpressureTest, HandlerCanResumeWritingAfterWriteReady) {
  createHandlers();

  bool backpressure_active = true;
  int successful_writes = 0;

  middle_handler_->setOnWrite(
      [&backpressure_active, &successful_writes](
          detail::ContextImpl& ctx, TypeErasedBox&& msg) {
        if (backpressure_active) {
          ctx.awaitWriteReady();
          return Result::Backpressure;
        }
        successful_writes++;
        return ctx.fireWrite(std::move(msg));
      });

  middle_handler_->setOnWriteReady(
      [](detail::ContextImpl& ctx) { ctx.cancelAwaitWriteReady(); });

  auto pipeline = buildPipeline();

  auto buf1 = folly::IOBuf::create(64);
  auto result1 = pipeline->fireWrite(TypeErasedBox(std::move(buf1)));
  EXPECT_EQ(result1, Result::Backpressure);
  EXPECT_EQ(successful_writes, 0);
  EXPECT_EQ(app_.writeCount(), 0);

  backpressure_active = false;
  pipeline->onWriteReady();

  auto buf2 = folly::IOBuf::create(64);
  auto result2 = pipeline->fireWrite(TypeErasedBox(std::move(buf2)));
  EXPECT_EQ(result2, Result::Success);
  EXPECT_EQ(successful_writes, 1);
  EXPECT_EQ(middle_ptr_->writeReadyCount(), 1);
}

// ==================== Rapid Backpressure/Resume Cycles ====================

TEST_F(BackpressureTest, RapidBackpressureResumeCycles) {
  createHandlers();

  bool backpressure_active = true;

  middle_handler_->setOnWrite(
      [&backpressure_active](detail::ContextImpl& ctx, TypeErasedBox&& msg) {
        if (backpressure_active) {
          ctx.awaitWriteReady();
          return Result::Backpressure;
        }
        return ctx.fireWrite(std::move(msg));
      });

  middle_handler_->setOnWriteReady(
      [](detail::ContextImpl& ctx) { ctx.cancelAwaitWriteReady(); });

  auto pipeline = buildPipeline();

  for (int cycle = 0; cycle < 10; ++cycle) {
    backpressure_active = true;
    auto buf = folly::IOBuf::create(64);
    auto result = pipeline->fireWrite(TypeErasedBox(std::move(buf)));
    EXPECT_EQ(result, Result::Backpressure);
    EXPECT_TRUE(pipeline->hasPendingWriteReady());

    backpressure_active = false;
    pipeline->onWriteReady();
    EXPECT_FALSE(pipeline->hasPendingWriteReady());
  }

  EXPECT_EQ(middle_ptr_->writeReadyCount(), 10);
}

// ==================== EventBase Integration Tests ====================

TEST_F(BackpressureTest, AsyncResumeFromEventBase) {
  createHandlers();

  bool write_ready_received = false;
  middle_handler_->setOnWrite([](detail::ContextImpl& ctx, TypeErasedBox&&) {
    ctx.awaitWriteReady();
    return Result::Backpressure;
  });
  middle_handler_->setOnWriteReady([&](detail::ContextImpl& ctx) {
    write_ready_received = true;
    ctx.cancelAwaitWriteReady();
  });

  auto pipeline = buildPipeline();

  auto buf = folly::IOBuf::create(64);
  (void)pipeline->fireWrite(TypeErasedBox(std::move(buf)));

  EXPECT_FALSE(write_ready_received);

  evb_.runInEventBaseThread([&pipeline]() { pipeline->onWriteReady(); });
  evb_.loopOnce();

  EXPECT_TRUE(write_ready_received);
}

TEST_F(BackpressureTest, ContextValidDuringAsyncCallback) {
  createHandlers();

  detail::ContextImpl* captured_ctx = nullptr;
  bool callback_executed = false;

  middle_handler_->setOnWrite([&](detail::ContextImpl& ctx, TypeErasedBox&&) {
    captured_ctx = &ctx;
    ctx.awaitWriteReady();
    return Result::Backpressure;
  });

  middle_handler_->setOnWriteReady([&](detail::ContextImpl& ctx) {
    EXPECT_EQ(&ctx, captured_ctx);
    EXPECT_EQ(ctx.pipeline(), pipeline_.get());
    EXPECT_EQ(ctx.eventBase(), &evb_);
    callback_executed = true;
    ctx.cancelAwaitWriteReady();
  });

  pipeline_ = buildPipeline();

  auto buf = folly::IOBuf::create(64);
  (void)pipeline_->fireWrite(TypeErasedBox(std::move(buf)));

  evb_.runInEventBaseThread([this]() { pipeline_->onWriteReady(); });
  evb_.loopOnce();

  EXPECT_TRUE(callback_executed);
}

// ==================== Edge Cases ====================

TEST_F(BackpressureTest, ClosedPipelineIgnoresWriteReady) {
  createHandlers();

  middle_handler_->setOnWrite([](detail::ContextImpl& ctx, TypeErasedBox&&) {
    ctx.awaitWriteReady();
    return Result::Backpressure;
  });

  auto pipeline = buildPipeline();

  auto buf = folly::IOBuf::create(64);
  (void)pipeline->fireWrite(TypeErasedBox(std::move(buf)));

  EXPECT_TRUE(pipeline->hasPendingWriteReady());

  pipeline->close();

  EXPECT_FALSE(pipeline->hasPendingWriteReady());

  pipeline->onWriteReady();

  EXPECT_EQ(middle_ptr_->writeReadyCount(), 0);
  EXPECT_EQ(transport_.onWriteReadyCount(), 0);
}

TEST_F(BackpressureTest, WriteReadyCallbackThatClosesPipelineSkipsTail) {
  createHandlers();
  head_handler_->setOnWriteReady(
      [](detail::ContextImpl& ctx) { ctx.pipeline()->close(); });
  auto pipeline = buildPipeline();
  pipeline->context(head_tag)->awaitWriteReady();

  pipeline->onWriteReady();

  // After close the tail endpoint may be gone, so the pass must not call it.
  EXPECT_THAT(
      std::tuple(head_ptr_->writeReadyCount(), transport_.onWriteReadyCount()),
      Eq(std::tuple(1, 0)));
}

TEST_F(
    BackpressureTest,
    HeadEndpointWriteReadyCallbackThatClosesPipelineSkipsTail) {
  createHandlers();
  app_.setOnWriteWithContextCallback(
      [](detail::ContextImpl& ctx, TypeErasedBox&&) {
        ctx.awaitWriteReady();
        return Result::Backpressure;
      });
  app_.setOnWriteReadyCallback(
      [](detail::ContextImpl& ctx) { ctx.pipeline()->close(); });
  auto pipeline = buildPipeline();
  EXPECT_THAT(
      pipeline->fireWrite(TypeErasedBox(folly::IOBuf::create(64))),
      Eq(Result::Backpressure));

  pipeline->onWriteReady();

  EXPECT_THAT(
      std::tuple(app_.onWriteReadyCount(), transport_.onWriteReadyCount()),
      Eq(std::tuple(1, 0)));
}

TEST_F(BackpressureTest, NoBackpressureDoesNotMarkPending) {
  createHandlers();

  auto pipeline = buildPipeline();

  auto buf = folly::IOBuf::create(64);
  auto result = pipeline->fireWrite(TypeErasedBox(std::move(buf)));

  EXPECT_EQ(result, Result::Success);
  EXPECT_FALSE(pipeline->hasPendingWriteReady());
}

TEST_F(BackpressureTest, ErrorDoesNotMarkPending) {
  createHandlers();

  middle_handler_->setOnWrite(
      [](detail::ContextImpl&, TypeErasedBox&&) { return Result::Error; });

  auto pipeline = buildPipeline();

  auto buf = folly::IOBuf::create(64);
  auto result = pipeline->fireWrite(TypeErasedBox(std::move(buf)));

  EXPECT_EQ(result, Result::Error);
  EXPECT_FALSE(pipeline->hasPendingWriteReady());
}

TEST_F(BackpressureTest, MultipleWriteReadyCallsAreIdempotent) {
  createHandlers();

  middle_handler_->setOnWrite([](detail::ContextImpl& ctx, TypeErasedBox&&) {
    ctx.awaitWriteReady();
    return Result::Backpressure;
  });

  // Don't cancel on first callback - stay armed
  int callback_count = 0;
  middle_handler_->setOnWriteReady([&callback_count](detail::ContextImpl&) {
    callback_count++;
    // Note: Not canceling, so handler stays armed
  });

  auto pipeline = buildPipeline();

  auto buf = folly::IOBuf::create(64);
  (void)pipeline->fireWrite(TypeErasedBox(std::move(buf)));

  // First onWriteReady call
  pipeline->onWriteReady();
  EXPECT_EQ(callback_count, 1);

  // Handler is still armed, so second call should also trigger callback
  pipeline->onWriteReady();
  EXPECT_EQ(callback_count, 2);
}

TEST_F(BackpressureTest, AwaitWriteReadyIsIdempotent) {
  createHandlers();

  int await_calls = 0;
  middle_handler_->setOnWrite(
      [&await_calls](detail::ContextImpl& ctx, TypeErasedBox&&) {
        await_calls++;
        ctx.awaitWriteReady();
        ctx.awaitWriteReady(); // Call twice
        ctx.awaitWriteReady(); // Call thrice
        return Result::Backpressure;
      });

  middle_handler_->setOnWriteReady(
      [](detail::ContextImpl& ctx) { ctx.cancelAwaitWriteReady(); });

  auto pipeline = buildPipeline();

  auto buf = folly::IOBuf::create(64);
  (void)pipeline->fireWrite(TypeErasedBox(std::move(buf)));
  pipeline->onWriteReady();

  // Handler should only receive one callback despite multiple await calls
  EXPECT_EQ(middle_ptr_->writeReadyCount(), 1);
}

// ==================== Handler Cancel During Callback Tests
// ====================

TEST_F(BackpressureTest, HandlerCanCancelDuringOnWriteReady) {
  createHandlers();

  // Both handlers register
  head_handler_->setOnWrite([](detail::ContextImpl& ctx, TypeErasedBox&&) {
    ctx.awaitWriteReady();
    return Result::Backpressure;
  });
  head_handler_->setOnWriteReady(
      [](detail::ContextImpl& ctx) { ctx.cancelAwaitWriteReady(); });

  middle_handler_->setOnWrite(
      [](detail::ContextImpl& ctx, TypeErasedBox&& msg) {
        ctx.awaitWriteReady();
        (void)ctx.fireWrite(std::move(msg));
        return Result::Backpressure;
      });
  middle_handler_->setOnWriteReady(
      [](detail::ContextImpl& ctx) { ctx.cancelAwaitWriteReady(); });

  auto pipeline = buildPipeline();

  auto buf = folly::IOBuf::create(64);
  (void)pipeline->fireWrite(TypeErasedBox(std::move(buf)));

  // Should not crash - each handler cancels its own wait during the pass
  pipeline->onWriteReady();

  EXPECT_EQ(head_ptr_->writeReadyCount(), 1);
  EXPECT_EQ(middle_ptr_->writeReadyCount(), 1);
  EXPECT_FALSE(pipeline->hasPendingWriteReady());
}

// ==================== Backpressure Strategy Tests ====================

TEST_F(BackpressureTest, HandlerCanDropMessageOnBackpressure) {
  createHandlers();

  int drop_count = 0;

  // Handler drops message instead of buffering
  middle_handler_->setOnWrite(
      [&drop_count](detail::ContextImpl& ctx, TypeErasedBox&& msg) {
        auto result = ctx.fireWrite(std::move(msg));
        if (result == Result::Backpressure) {
          drop_count++;
          return Result::Success; // Drop the message, return success
        }
        return result;
      });

  // Head handler simulates downstream backpressure
  head_handler_->setOnWrite([](detail::ContextImpl&, TypeErasedBox&&) {
    return Result::Backpressure;
  });

  auto pipeline = buildPipeline();

  auto buf = folly::IOBuf::create(64);
  auto result = pipeline->fireWrite(TypeErasedBox(std::move(buf)));

  EXPECT_EQ(result, Result::Success); // Dropped, returned success
  EXPECT_EQ(drop_count, 1);
  EXPECT_FALSE(pipeline->hasPendingWriteReady()); // No registration
}

TEST_F(BackpressureTest, HandlerCanReturnErrorOnBackpressure) {
  createHandlers();

  int error_count = 0;

  // Handler returns error instead of buffering
  middle_handler_->setOnWrite(
      [&error_count](detail::ContextImpl& ctx, TypeErasedBox&& msg) {
        auto result = ctx.fireWrite(std::move(msg));
        if (result == Result::Backpressure) {
          error_count++;
          return Result::Error; // Signal error to caller
        }
        return result;
      });

  // Head handler simulates downstream backpressure
  head_handler_->setOnWrite([](detail::ContextImpl&, TypeErasedBox&&) {
    return Result::Backpressure;
  });

  auto pipeline = buildPipeline();

  auto buf = folly::IOBuf::create(64);
  auto result = pipeline->fireWrite(TypeErasedBox(std::move(buf)));

  EXPECT_EQ(result, Result::Error);
  EXPECT_EQ(error_count, 1);
  EXPECT_FALSE(pipeline->hasPendingWriteReady()); // No registration
}

// ==================== Read Ready Tests ====================

TEST_F(BackpressureTest, AwaitReadReadyRegistersHandler) {
  createHandlers();

  middle_handler_->setOnRead([](detail::ContextImpl& ctx, TypeErasedBox&& msg) {
    ctx.awaitReadReady();
    return ctx.fireRead(std::move(msg));
  });

  auto pipeline = buildPipeline();

  EXPECT_FALSE(pipeline->hasPendingReadReady());

  auto buf = folly::IOBuf::create(64);
  (void)pipeline->fireRead(TypeErasedBox(std::move(buf)));

  EXPECT_TRUE(pipeline->hasPendingReadReady());
}

TEST_F(BackpressureTest, CancelAwaitReadReadyUnregistersHandler) {
  createHandlers();

  middle_handler_->setOnRead([](detail::ContextImpl& ctx, TypeErasedBox&& msg) {
    ctx.awaitReadReady();
    return ctx.fireRead(std::move(msg));
  });
  middle_handler_->setOnReadReady(
      [](detail::ContextImpl& ctx) { ctx.cancelAwaitReadReady(); });

  auto pipeline = buildPipeline();

  auto buf = folly::IOBuf::create(64);
  (void)pipeline->fireRead(TypeErasedBox(std::move(buf)));

  EXPECT_TRUE(pipeline->hasPendingReadReady());

  pipeline->onReadReady();

  EXPECT_FALSE(pipeline->hasPendingReadReady());
}

TEST_F(BackpressureTest, HandlerReceivesReadReadyAfterAwait) {
  createHandlers();

  middle_handler_->setOnRead([](detail::ContextImpl& ctx, TypeErasedBox&& msg) {
    ctx.awaitReadReady();
    return ctx.fireRead(std::move(msg));
  });
  middle_handler_->setOnReadReady(
      [](detail::ContextImpl& ctx) { ctx.cancelAwaitReadReady(); });

  auto pipeline = buildPipeline();

  auto buf = folly::IOBuf::create(64);
  (void)pipeline->fireRead(TypeErasedBox(std::move(buf)));

  EXPECT_EQ(middle_ptr_->readReadyCount(), 0);
  pipeline->onReadReady();
  EXPECT_EQ(middle_ptr_->readReadyCount(), 1);
}

TEST_F(BackpressureTest, OnReadReadyCallsHeadOnReadReady) {
  createHandlers();
  auto pipeline = buildPipeline();

  EXPECT_EQ(app_.onReadReadyCount(), 0);

  pipeline->onReadReady();

  EXPECT_EQ(app_.onReadReadyCount(), 1);
}

TEST_F(BackpressureTest, ReadReadyWakesHandlersFromTailToHead) {
  createHandlers();
  std::vector<std::string> readiness;
  for (auto [handler, name] :
       {std::pair{head_handler_.get(), "head"},
        std::pair{middle_handler_.get(), "middle"},
        std::pair{tail_handler_.get(), "tail"}}) {
    handler->setOnRead([](detail::ContextImpl& ctx, TypeErasedBox&& msg) {
      ctx.awaitReadReady();
      return ctx.fireRead(std::move(msg));
    });
    handler->setOnReadReady(
        [&readiness, label = name](detail::ContextImpl& ctx) {
          readiness.emplace_back(label);
          ctx.cancelAwaitReadReady();
        });
  }

  auto pipeline = buildPipeline();
  // The read reaches the handlers from the head toward the tail, so they arm
  // in the order head, middle, tail.
  EXPECT_THAT(
      pipeline->fireRead(TypeErasedBox(folly::IOBuf::create(64))),
      Eq(Result::Success));
  pipeline->onReadReady();

  EXPECT_THAT(readiness, ElementsAre("tail", "middle", "head"));
}

TEST_F(BackpressureTest, NestedReadReadyFromAHandlerRunsAfterThePass) {
  createHandlers();
  std::vector<std::string> readiness;
  bool requestedNestedDispatch = false;

  middle_handler_->setOnRead([](detail::ContextImpl& ctx, TypeErasedBox&& msg) {
    ctx.awaitReadReady();
    return ctx.fireRead(std::move(msg));
  });
  // The handler asks for a nested pass and stays armed when it first wakes,
  // and it cancels when it wakes again.
  middle_handler_->setOnReadReady([&](detail::ContextImpl& ctx) {
    readiness.emplace_back("middle");
    if (!std::exchange(requestedNestedDispatch, true)) {
      ctx.pipeline()->onReadReady();
      return;
    }
    ctx.cancelAwaitReadReady();
  });
  app_.setOnReadReadyCallback([&readiness] { readiness.emplace_back("head"); });

  auto pipeline = buildPipeline();
  EXPECT_THAT(
      pipeline->fireRead(TypeErasedBox(folly::IOBuf::create(64))),
      Eq(Result::Success));

  pipeline->onReadReady();

  // The nested request runs as a second pass after the first one notifies the
  // head endpoint, so the handler and the head endpoint each wake twice.
  EXPECT_THAT(
      std::tuple(readiness, pipeline->hasPendingReadReady()),
      Eq(std::tuple(
          std::vector<std::string>{"middle", "head", "middle", "head"},
          false)));
}

TEST_F(BackpressureTest, NestedReadReadyFromTheHeadEndpointRunsAfterIt) {
  createHandlers();
  auto pipeline = buildPipeline();
  int depth = 0;
  std::vector<int> depths;
  app_.setOnReadReadyCallback([&] {
    ++depth;
    depths.push_back(depth);
    if (depths.size() == 1) {
      pipeline->onReadReady();
    }
    --depth;
  });

  pipeline->onReadReady();

  // The nested request runs as a second pass after the head endpoint's
  // callback returns, so the callback never runs inside itself.
  EXPECT_THAT(depths, Eq(std::vector<int>{1, 1}));
}

TEST_F(
    BackpressureTest, ReadReadyWakesAHandlerArmedBehindThePassInTheNextPass) {
  createHandlers();
  std::vector<std::string> readiness;
  middle_handler_->setOnRead([](detail::ContextImpl& ctx, TypeErasedBox&& msg) {
    ctx.awaitReadReady();
    return ctx.fireRead(std::move(msg));
  });
  // When the middle handler wakes, it reads one more message, and the handler
  // above it arms on that read.
  middle_handler_->setOnReadReady([&readiness](detail::ContextImpl& ctx) {
    readiness.emplace_back("middle");
    ctx.cancelAwaitReadReady();
    (void)ctx.fireRead(TypeErasedBox(folly::IOBuf::create(64)));
  });
  tail_handler_->setOnRead(
      [&readiness](detail::ContextImpl& ctx, TypeErasedBox&& msg) {
        if (!readiness.empty()) {
          ctx.awaitReadReady();
        }
        return ctx.fireRead(std::move(msg));
      });
  tail_handler_->setOnReadReady([&readiness](detail::ContextImpl& ctx) {
    readiness.emplace_back("above");
    ctx.cancelAwaitReadReady();
  });

  auto pipeline = buildPipeline();
  EXPECT_THAT(
      pipeline->fireRead(TypeErasedBox(folly::IOBuf::create(64))),
      Eq(Result::Success));
  // The handler above starts to wait behind the pass, so this pass does not
  // wake it.
  pipeline->onReadReady();
  const auto afterPass = std::tuple(
      readiness, app_.onReadReadyCount(), pipeline->hasPendingReadReady());
  pipeline->onReadReady();

  EXPECT_THAT(
      std::tuple(
          afterPass,
          readiness,
          app_.onReadReadyCount(),
          pipeline->hasPendingReadReady()),
      Eq(std::tuple(
          std::tuple(std::vector<std::string>{"middle"}, 1, true),
          std::vector<std::string>{"middle", "above"},
          2,
          false)));
}

TEST_F(BackpressureTest, ReadReadyWakesHandlerBelowOneThatStaysArmed) {
  createHandlers();
  std::vector<std::string> readiness;
  for (MockHandler* handler : {head_handler_.get(), middle_handler_.get()}) {
    handler->setOnRead([](detail::ContextImpl& ctx, TypeErasedBox&& msg) {
      ctx.awaitReadReady();
      return ctx.fireRead(std::move(msg));
    });
  }
  head_handler_->setOnReadReady([&readiness](detail::ContextImpl& ctx) {
    readiness.emplace_back("below");
    ctx.cancelAwaitReadReady();
  });
  middle_handler_->setOnReadReady(
      [&readiness](detail::ContextImpl&) { readiness.emplace_back("above"); });

  auto pipeline = buildPipeline();
  EXPECT_THAT(
      pipeline->fireRead(TypeErasedBox(folly::IOBuf::create(64))),
      Eq(Result::Success));
  pipeline->onReadReady();

  // The handler above stays armed, and the handler below and the head still
  // wake in the same pass.
  EXPECT_THAT(
      std::tuple(readiness, app_.onReadReadyCount()),
      Eq(std::tuple(std::vector<std::string>{"above", "below"}, 1)));
}

TEST_F(BackpressureTest, ReaderArmedBehindThePassWakesInTheNextPass) {
  createHandlers();
  QueuedReader below;
  below.install(*head_handler_);
  ReadHolder above;
  above.install(*middle_handler_);
  auto pipeline = buildPipeline();
  // The reader below queues two messages. The handler above does not wait when
  // the pass starts.
  EXPECT_THAT(
      pipeline->fireRead(TypeErasedBox(folly::IOBuf::create(64))),
      Eq(Result::Backpressure));
  EXPECT_THAT(
      pipeline->fireRead(TypeErasedBox(folly::IOBuf::create(64))),
      Eq(Result::Backpressure));

  // The reader wakes and reads into the handler above, which holds the message
  // and starts to wait behind the pass. The pass does not wake it again and
  // notifies the head.
  pipeline->onReadReady();
  const auto afterPass = std::tuple(
      below.results,
      transport_.readCount(),
      pipeline->hasPendingReadReady(),
      app_.onReadReadyCount());
  // The next pass wakes the handler above and then the reader.
  pipeline->onReadReady();

  EXPECT_THAT(
      std::tuple(
          afterPass,
          below.results,
          transport_.readCount(),
          pipeline->hasPendingReadReady(),
          app_.onReadReadyCount()),
      Eq(std::tuple(
          std::tuple(std::vector{Result::Backpressure}, 0, true, 1),
          std::vector{Result::Backpressure, Result::Success},
          2,
          false,
          2)));
}

TEST_F(BackpressureTest, ReaderThatArmsAgainInItsCallbackWakesOncePerPass) {
  createHandlers();
  head_handler_->setOnRead([](detail::ContextImpl& ctx, TypeErasedBox&& msg) {
    ctx.awaitReadReady();
    return ctx.fireRead(std::move(msg));
  });
  // The handler arms again in each callback. After 3 wakes it stops, so that a
  // pass that wakes it again cannot loop forever.
  int wakes = 0;
  head_handler_->setOnReadReady([&wakes](detail::ContextImpl& ctx) {
    ctx.cancelAwaitReadReady();
    if (++wakes < 3) {
      ctx.awaitReadReady();
    }
  });

  auto pipeline = buildPipeline();
  EXPECT_THAT(
      pipeline->fireRead(TypeErasedBox(folly::IOBuf::create(64))),
      Eq(Result::Success));
  pipeline->onReadReady();

  EXPECT_THAT(
      std::tuple(
          wakes, app_.onReadReadyCount(), pipeline->hasPendingReadReady()),
      Eq(std::tuple(1, 1, true)));
}

TEST_F(BackpressureTest, ReaderArmedBelowThePassWakesInIt) {
  createHandlers();
  std::vector<std::string> readiness;
  middle_handler_->setOnReadReady([&readiness](detail::ContextImpl& ctx) {
    readiness.emplace_back("above");
    ctx.cancelAwaitReadReady();
    ctx.pipeline()->context(head_tag)->awaitReadReady();
  });
  head_handler_->setOnReadReady([&readiness](detail::ContextImpl& ctx) {
    readiness.emplace_back("below");
    ctx.cancelAwaitReadReady();
  });
  app_.setOnReadReadyCallback([&readiness] { readiness.emplace_back("head"); });
  auto pipeline = buildPipeline();
  pipeline->context(middle_tag)->awaitReadReady();

  // The handler above makes the handler below wait during the pass, and the
  // pass wakes it before the head.
  pipeline->onReadReady();

  EXPECT_THAT(
      std::tuple(readiness, pipeline->hasPendingReadReady()),
      Eq(std::tuple(
          std::vector<std::string>{"above", "below", "head"}, false)));
}

TEST_F(BackpressureTest, MultipleHandlersCanRegisterForReadReady) {
  createHandlers();

  auto registerAwait = [](auto* h) {
    h->setOnRead([](detail::ContextImpl& ctx, TypeErasedBox&& msg) {
      ctx.awaitReadReady();
      return ctx.fireRead(std::move(msg));
    });
    h->setOnReadReady(
        [](detail::ContextImpl& ctx) { ctx.cancelAwaitReadReady(); });
  };
  registerAwait(head_handler_.get());
  registerAwait(middle_handler_.get());

  auto pipeline = buildPipeline();

  auto buf = folly::IOBuf::create(64);
  (void)pipeline->fireRead(TypeErasedBox(std::move(buf)));

  EXPECT_TRUE(pipeline->hasPendingReadReady());

  pipeline->onReadReady();

  EXPECT_EQ(head_ptr_->readReadyCount(), 1);
  EXPECT_EQ(middle_ptr_->readReadyCount(), 1);
  EXPECT_EQ(tail_ptr_->readReadyCount(), 0);
}

TEST_F(BackpressureTest, AwaitReadReadyIsIdempotent) {
  createHandlers();

  middle_handler_->setOnRead([](detail::ContextImpl& ctx, TypeErasedBox&& msg) {
    ctx.awaitReadReady();
    ctx.awaitReadReady();
    ctx.awaitReadReady();
    return ctx.fireRead(std::move(msg));
  });
  middle_handler_->setOnReadReady(
      [](detail::ContextImpl& ctx) { ctx.cancelAwaitReadReady(); });

  auto pipeline = buildPipeline();

  auto buf = folly::IOBuf::create(64);
  (void)pipeline->fireRead(TypeErasedBox(std::move(buf)));
  pipeline->onReadReady();

  // Handler should only receive one callback despite multiple await calls.
  EXPECT_EQ(middle_ptr_->readReadyCount(), 1);
}

TEST_F(BackpressureTest, HandlerCanCancelDuringOnReadReady) {
  createHandlers();

  auto registerAwait = [](auto* h) {
    h->setOnRead([](detail::ContextImpl& ctx, TypeErasedBox&& msg) {
      ctx.awaitReadReady();
      return ctx.fireRead(std::move(msg));
    });
    h->setOnReadReady(
        [](detail::ContextImpl& ctx) { ctx.cancelAwaitReadReady(); });
  };
  registerAwait(head_handler_.get());
  registerAwait(middle_handler_.get());

  auto pipeline = buildPipeline();

  auto buf = folly::IOBuf::create(64);
  (void)pipeline->fireRead(TypeErasedBox(std::move(buf)));

  // Should not crash — each handler cancels its own wait during the pass.
  pipeline->onReadReady();

  EXPECT_EQ(head_ptr_->readReadyCount(), 1);
  EXPECT_EQ(middle_ptr_->readReadyCount(), 1);
  EXPECT_FALSE(pipeline->hasPendingReadReady());
}

TEST_F(BackpressureTest, ClosedPipelineIgnoresReadReady) {
  createHandlers();

  middle_handler_->setOnRead([](detail::ContextImpl& ctx, TypeErasedBox&& msg) {
    ctx.awaitReadReady();
    return ctx.fireRead(std::move(msg));
  });

  auto pipeline = buildPipeline();

  auto buf = folly::IOBuf::create(64);
  (void)pipeline->fireRead(TypeErasedBox(std::move(buf)));

  EXPECT_TRUE(pipeline->hasPendingReadReady());

  pipeline->close();

  EXPECT_FALSE(pipeline->hasPendingReadReady());

  pipeline->onReadReady();

  EXPECT_EQ(middle_ptr_->readReadyCount(), 0);
  EXPECT_EQ(app_.onReadReadyCount(), 0);
}

TEST_F(BackpressureTest, ReadReadyCallbackThatClosesPipelineSkipsHead) {
  createHandlers();
  middle_handler_->setOnRead([](detail::ContextImpl& ctx, TypeErasedBox&& msg) {
    ctx.awaitReadReady();
    return ctx.fireRead(std::move(msg));
  });
  middle_handler_->setOnReadReady(
      [](detail::ContextImpl& ctx) { ctx.pipeline()->close(); });

  auto pipeline = buildPipeline();
  EXPECT_THAT(
      pipeline->fireRead(TypeErasedBox(folly::IOBuf::create(64))),
      Eq(Result::Success));
  pipeline->onReadReady();

  // After close the head endpoint may be gone, so the pass must not call it.
  EXPECT_THAT(app_.onReadReadyCount(), Eq(0));
}

} // namespace apache::thrift::fast_thrift::channel_pipeline::test
