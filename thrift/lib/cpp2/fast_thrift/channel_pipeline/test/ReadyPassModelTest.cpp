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

// These tests run random scenarios through a real pipeline and through a model
// of the documented ready passes. They compare what wakes, in order, and the
// waits that remain.
//
// A scenario makes some handlers wait, gives each handler a script of actions
// for its wakes, and starts one to three passes. An action makes any handler
// wait or stop waiting, starts a nested pass or closes the pipeline.

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/HandlerTag.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineBuilder.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/StaticPipelineBuilder.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/test/MockAdapters.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/test/MockHandler.h>

#include <folly/io/async/EventBase.h>
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>

#include <algorithm>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <random>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace apache::thrift::fast_thrift::channel_pipeline::test {
namespace {

using namespace testing;

constexpr std::uint64_t kScenarios = 10'000;

enum class Op : std::uint8_t {
  ArmWrite,
  CancelWrite,
  ArmRead,
  CancelRead,
  WritePass,
  ReadPass,
  Close,
};

struct Action {
  Op op;
  int target;
};

// Index `handlers` stands for the head endpoint, which waits only for write
// ready.
struct Scenario {
  int handlers{0};
  std::vector<bool> writeWaits;
  std::vector<bool> readWaits;
  std::vector<std::deque<std::vector<Action>>> writeScripts;
  std::vector<std::deque<std::vector<Action>>> readScripts;
  std::vector<Op> passes;
};

int below(std::mt19937_64& rng, int bound) {
  return static_cast<int>(rng() % static_cast<std::uint64_t>(bound));
}

// Up to three wakes, each with up to three actions.
std::deque<std::vector<Action>> randomScript(
    std::mt19937_64& rng, int handlers) {
  std::deque<std::vector<Action>> script(below(rng, 4));

  for (std::vector<Action>& actions : script) {
    const int count = below(rng, 4);
    for (int i = 0; i < count; ++i) {
      const int roll = below(rng, 100);
      if (roll < 30) {
        actions.push_back({Op::ArmWrite, below(rng, handlers + 1)});
      } else if (roll < 55) {
        actions.push_back({Op::CancelWrite, below(rng, handlers + 1)});
      } else if (roll < 70) {
        actions.push_back({Op::ArmRead, below(rng, handlers)});
      } else if (roll < 82) {
        actions.push_back({Op::CancelRead, below(rng, handlers)});
      } else if (roll < 89) {
        actions.push_back({Op::WritePass, 0});
      } else if (roll < 96) {
        actions.push_back({Op::ReadPass, 0});
      } else {
        actions.push_back({Op::Close, 0});
      }
    }
  }

  return script;
}

Scenario makeScenario(std::uint64_t seed, int handlers) {
  std::mt19937_64 rng(seed);
  Scenario scenario;
  scenario.handlers = handlers;
  // The head endpoint waits less often than a handler, so that more passes
  // reach the handlers.
  for (int i = 0; i <= handlers; ++i) {
    scenario.writeWaits.push_back(below(rng, 4) < (i == handlers ? 1 : 2));
    scenario.writeScripts.push_back(randomScript(rng, handlers));
  }
  for (int i = 0; i < handlers; ++i) {
    scenario.readWaits.push_back(below(rng, 2) == 0);
    scenario.readScripts.push_back(randomScript(rng, handlers));
  }

  const int passes = 1 + below(rng, 3);
  for (int i = 0; i < passes; ++i) {
    scenario.passes.push_back(
        below(rng, 2) == 0 ? Op::WritePass : Op::ReadPass);
  }
  return scenario;
}

// Runs a scenario and records each wake, each notification of the tail or the
// head endpoint, each close and the end of each pass, then the waits that
// remain. A woken handler runs the next actions of its script, or stops waiting
// when its script is empty.
class Driver {
 public:
  explicit Driver(Scenario scenario) : scenario_(std::move(scenario)) {}
  virtual ~Driver() = default;

  std::vector<std::string> run() {
    const int handlers = scenario_.handlers;
    for (int i = 0; i <= handlers; ++i) {
      if (scenario_.writeWaits[i]) {
        act({Op::ArmWrite, i});
      }
    }
    for (int i = 0; i < handlers; ++i) {
      if (scenario_.readWaits[i]) {
        act({Op::ArmRead, i});
      }
    }

    for (const Op pass : scenario_.passes) {
      act({pass, 0});
      trace_.emplace_back("|");
    }

    trace_.emplace_back(isClosed() ? "closed" : "open");
    for (int i = 0; i <= handlers; ++i) {
      if (writeWaits(i)) {
        trace_.push_back("waits W:" + std::to_string(i));
      }
    }
    for (int i = 0; i < handlers; ++i) {
      if (readWaits(i)) {
        trace_.push_back("waits R:" + std::to_string(i));
      }
    }
    return std::move(trace_);
  }

  void wakeWrite(int index) {
    trace_.push_back(
        index == handlers() ? std::string("W:head")
                            : "W:" + std::to_string(index));
    runScript(scenario_.writeScripts[index], {Op::CancelWrite, index});
  }
  void wakeRead(int index) {
    trace_.push_back("R:" + std::to_string(index));
    runScript(scenario_.readScripts[index], {Op::CancelRead, index});
  }
  void notifyTail() { trace_.emplace_back("W:tail"); }
  void notifyHead() { trace_.emplace_back("R:head"); }
  int handlers() const { return scenario_.handlers; }

 protected:
  virtual void act(const Action& action) = 0;
  virtual bool isClosed() = 0;
  virtual bool writeWaits(int index) = 0;
  virtual bool readWaits(int index) = 0;

 private:
  void runScript(
      std::deque<std::vector<Action>>& script, const Action& whenEmpty) {
    if (script.empty()) {
      act(whenEmpty);
      return;
    }
    const std::vector<Action> actions = std::move(script.front());
    script.pop_front();
    for (const Action& action : actions) {
      if (action.op == Op::Close && !isClosed()) {
        trace_.emplace_back("close");
      }
      act(action);
    }
  }

  Scenario scenario_;
  std::vector<std::string> trace_;
};

// The documented passes, on plain flags.
class Model : public Driver {
 public:
  // The dynamic pipeline drops a pass asked for during a pass that stopped
  // early, and the static pipeline runs it.
  Model(Scenario scenario, bool runsPassAskedForDuringAStoppedPass)
      : Driver(std::move(scenario)),
        writeWaits_(handlers() + 1, false),
        readWaits_(handlers(), false),
        runsPassAskedForDuringAStoppedPass_(
            runsPassAskedForDuringAStoppedPass) {}

 protected:
  void act(const Action& action) override {
    switch (action.op) {
      case Op::ArmWrite:
        if (!closed_) {
          writeWaits_[action.target] = true;
        }
        break;
      case Op::CancelWrite:
        writeWaits_[action.target] = false;
        break;

      case Op::ArmRead:
        if (!closed_) {
          readWaits_[action.target] = true;
        }
        break;
      case Op::CancelRead:
        readWaits_[action.target] = false;
        break;

      case Op::WritePass:
        writePass();
        break;
      case Op::ReadPass:
        readPass();
        break;
      case Op::Close:
        closed_ = true;
        writeWaits_.assign(writeWaits_.size(), false);
        readWaits_.assign(readWaits_.size(), false);
        break;
    }
  }
  bool isClosed() override { return closed_; }
  bool writeWaits(int index) override { return writeWaits_[index]; }
  bool readWaits(int index) override { return readWaits_[index]; }

 private:
  void writePass() {
    if (closed_) {
      return;
    }
    if (writePassRunning_) {
      writePassAskedFor_ = true;
      return;
    }

    writePassRunning_ = true;
    const int head = handlers();
    do {
      writePassAskedFor_ = false;
      bool stopped = false;
      if (writeWaits_[head]) {
        wakeWrite(head);
        stopped = closed_ || writeWaits_[head];
      }
      for (int i = 0; i < head && !stopped; ++i) {
        if (writeWaits_[i]) {
          wakeWrite(i);
          stopped = closed_ || writeWaits_[head];
        }
      }

      if (!stopped) {
        notifyTail();
      } else if (!runsPassAskedForDuringAStoppedPass_) {
        break;
      }
    } while (writePassAskedFor_ && !closed_);
    writePassRunning_ = false;
  }

  void readPass() {
    if (closed_) {
      return;
    }
    if (readPassRunning_) {
      readPassAskedFor_ = true;
      return;
    }

    readPassRunning_ = true;
    do {
      readPassAskedFor_ = false;
      for (int i = handlers() - 1; i >= 0 && !closed_; --i) {
        if (readWaits_[i]) {
          wakeRead(i);
        }
      }
      if (!closed_) {
        notifyHead();
      }
    } while (readPassAskedFor_ && !closed_);
    readPassRunning_ = false;
  }

  std::vector<bool> writeWaits_;
  std::vector<bool> readWaits_;
  const bool runsPassAskedForDuringAStoppedPass_;
  bool closed_{false};
  bool writePassRunning_{false};
  bool writePassAskedFor_{false};
  bool readPassRunning_{false};
  bool readPassAskedFor_{false};
};

class DynamicPipelineDriver : public Driver {
 public:
  explicit DynamicPipelineDriver(Scenario scenario)
      : Driver(std::move(scenario)) {
    PipelineBuilder<MockHeadHandler, MockTailHandler, TestAllocator> builder;
    builder.setEventBase(&eventBase_)
        .setHead(&head_)
        .setTail(&tail_)
        .setAllocator(&allocator_);

    for (int i = 0; i < handlers(); ++i) {
      auto handler = std::make_unique<MockHandler>();
      handler->setOnWriteReady(
          [this, i](detail::ContextImpl&) { wakeWrite(i); });
      handler->setOnReadReady([this, i](detail::ContextImpl&) { wakeRead(i); });
      builder.addNextDuplex<MockHandler>(
          static_cast<HandlerId>(i + 1), std::move(handler));
    }

    head_.setOnWriteWithContextCallback(
        [this](detail::ContextImpl& ctx, TypeErasedBox&&) {
          contexts_.push_back(&ctx);
          return Result::Success;
        });
    head_.setOnWriteReadyCallback(
        [this](detail::ContextImpl&) { wakeWrite(handlers()); });
    head_.setOnReadReadyCallback([this] { notifyHead(); });
    tail_.setOnWriteReadyCallback([this] { notifyTail(); });

    pipeline_ = builder.build();

    for (int i = 0; i < handlers(); ++i) {
      contexts_.push_back(pipeline_->context(static_cast<HandlerId>(i + 1)));
    }
    // The head endpoint's context reaches the test through a write.
    (void)pipeline_->fireWrite(TypeErasedBox(folly::IOBuf::create(1)));
  }

 protected:
  void act(const Action& action) override {
    switch (action.op) {
      case Op::ArmWrite:
        contexts_[action.target]->awaitWriteReady();
        break;
      case Op::CancelWrite:
        contexts_[action.target]->cancelAwaitWriteReady();
        break;
      case Op::ArmRead:
        contexts_[action.target]->awaitReadReady();
        break;
      case Op::CancelRead:
        contexts_[action.target]->cancelAwaitReadReady();
        break;

      case Op::WritePass:
        pipeline_->onWriteReady();
        break;
      case Op::ReadPass:
        pipeline_->onReadReady();
        break;
      case Op::Close:
        pipeline_->close();
        break;
    }
  }
  bool isClosed() override { return pipeline_->isClosed(); }
  bool writeWaits(int index) override {
    return index == handlers() ? pipeline_->isHeadWriteBackpressured()
                               : contexts_[index]->isAwaitingWriteReady();
  }
  bool readWaits(int index) override {
    return contexts_[index]->isAwaitingReadReady();
  }

 private:
  folly::EventBase eventBase_;
  MockHeadHandler head_;
  MockTailHandler tail_;
  TestAllocator allocator_;
  PipelineImpl::Ptr pipeline_;
  // The handlers' contexts, then the head endpoint's.
  std::vector<detail::ContextImpl*> contexts_;
};

HANDLER_TAG(model_0);
HANDLER_TAG(model_1);
HANDLER_TAG(model_2);
HANDLER_TAG(model_3);
HANDLER_TAG(model_4);
HANDLER_TAG(model_5);

// Calls on one context of the static pipeline, whose type depends on its
// handler.
struct ContextCalls {
  std::function<void(Op)> act;
  std::function<bool()> writeWaits;
  std::function<bool()> readWaits;
};

class StaticPipelineDriver : public Driver {
 public:
  explicit StaticPipelineDriver(Scenario scenario)
      : Driver(std::move(scenario)), contexts_(handlers() + 1) {}

  template <typename Context>
  void bind(int index, Context& ctx) {
    const auto act = [&ctx](Op op) {
      switch (op) {
        case Op::ArmWrite:
          ctx.awaitWriteReady();
          break;
        case Op::CancelWrite:
          ctx.cancelAwaitWriteReady();
          break;

        case Op::ArmRead:
          ctx.awaitReadReady();
          break;
        case Op::CancelRead:
          ctx.cancelAwaitReadReady();
          break;
        case Op::WritePass:
        case Op::ReadPass:
        case Op::Close:
          break;
      }
    };

    contexts_[index] = ContextCalls{
        .act = act,
        .writeWaits = [&ctx] { return ctx.isAwaitingWriteReady(); },
        .readWaits = [&ctx] { return ctx.isAwaitingReadReady(); },
    };
  }

  template <typename Pipeline>
  std::vector<std::string> runOn(Pipeline& pipeline) {
    passOrClose_ = [&pipeline](Op op) {
      if (op == Op::WritePass) {
        pipeline.onWriteReady();
      } else if (op == Op::ReadPass) {
        pipeline.onReadReady();
      } else {
        pipeline.close();
      }
    };
    isClosed_ = [&pipeline] { return pipeline.isClosed(); };
    // The head endpoint's context reaches the test through a write.
    (void)pipeline.fireWrite(erase_and_box(folly::IOBuf::create(0)));
    return run();
  }

 protected:
  void act(const Action& action) override {
    switch (action.op) {
      case Op::WritePass:
      case Op::ReadPass:
      case Op::Close:
        passOrClose_(action.op);
        break;
      case Op::ArmWrite:
      case Op::CancelWrite:
      case Op::ArmRead:
      case Op::CancelRead:
        contexts_[action.target].act(action.op);
        break;
    }
  }
  bool isClosed() override { return isClosed_(); }
  bool writeWaits(int index) override { return contexts_[index].writeWaits(); }
  bool readWaits(int index) override { return contexts_[index].readWaits(); }

 private:
  // The handlers' contexts, then the head endpoint's.
  std::vector<ContextCalls> contexts_;
  std::function<void(Op)> passOrClose_;
  std::function<bool()> isClosed_;
};

template <int Index>
class ScriptedHandler {
 public:
  WriteReadyHook writeReadyHook_;
  ReadReadyHook readReadyHook_;

  explicit ScriptedHandler(StaticPipelineDriver* driver) : driver_(driver) {}

  template <typename Context>
  void handlerAdded(Context& ctx) noexcept {
    driver_->bind(Index, ctx);
  }
  template <typename Context>
  void handlerRemoved(Context&) noexcept {}
  template <typename Context>
  void onPipelineActive(Context&) noexcept {}
  template <typename Context>
  void onPipelineInactive(Context&) noexcept {}
  template <typename Context>
  Result onRead(Context& ctx, TypeErasedBox&& msg) noexcept {
    return ctx.fireRead(std::move(msg));
  }
  template <typename Context>
  Result onWrite(Context& ctx, TypeErasedBox&& msg) noexcept {
    return ctx.fireWrite(std::move(msg));
  }
  template <typename Context>
  void onException(Context& ctx, folly::exception_wrapper&& e) noexcept {
    ctx.fireException(std::move(e));
  }
  template <typename Context>
  void onWriteReady(Context&) noexcept {
    driver_->wakeWrite(Index);
  }
  template <typename Context>
  void onReadReady(Context&) noexcept {
    driver_->wakeRead(Index);
  }

 private:
  StaticPipelineDriver* driver_;
};

class ScriptedStaticHead {
 public:
  explicit ScriptedStaticHead(StaticPipelineDriver* driver) : driver_(driver) {}

  template <typename Context>
  Result onWrite(Context& ctx, TypeErasedBox&&) noexcept {
    driver_->bind(driver_->handlers(), ctx);
    return Result::Success;
  }
  template <typename Context>
  void onWriteReady(Context&) noexcept {
    driver_->wakeWrite(driver_->handlers());
  }
  void onReadReady() noexcept { driver_->notifyHead(); }
  void handlerAdded() noexcept {}
  void handlerRemoved() noexcept {}
  void onPipelineActive() noexcept {}
  void onPipelineInactive() noexcept {}

 private:
  StaticPipelineDriver* driver_;
};

class ScriptedStaticTail {
 public:
  explicit ScriptedStaticTail(StaticPipelineDriver* driver) : driver_(driver) {}

  template <typename Context>
  Result onRead(Context&, TypeErasedBox&&) noexcept {
    return Result::Success;
  }
  void onException(folly::exception_wrapper&&) noexcept {}
  void onWriteReady() noexcept { driver_->notifyTail(); }
  void handlerAdded() noexcept {}
  void handlerRemoved() noexcept {}
  void onPipelineActive() noexcept {}
  void onPipelineInactive() noexcept {}

 private:
  StaticPipelineDriver* driver_;
};

bool closes(const std::vector<std::string>& trace) {
  return std::ranges::find(trace, "closed") != trace.end();
}

TEST(ReadyPassModelTest, DynamicPipelineMatchesTheModel) {
  int scenariosThatClose = 0;
  for (std::uint64_t seed = 1; seed <= kScenarios; ++seed) {
    for (const int handlers : {1, 3, 6}) {
      const std::vector<std::string> expected =
          Model(makeScenario(seed, handlers), false).run();
      ASSERT_THAT(
          std::tuple(
              seed,
              handlers,
              DynamicPipelineDriver(makeScenario(seed, handlers)).run()),
          Eq(std::tuple(seed, handlers, expected)));
      scenariosThatClose += closes(expected) ? 1 : 0;
    }
  }

  // Enough scenarios close the pipeline during a pass.
  EXPECT_THAT(scenariosThatClose, Gt(1000));
}

TEST(ReadyPassModelTest, StaticPipelineMatchesTheModel) {
  int scenariosThatClose = 0;
  for (std::uint64_t seed = 1; seed <= kScenarios; ++seed) {
    const std::vector<std::string> expected =
        Model(makeScenario(seed, 6), true).run();

    StaticPipelineDriver driver(makeScenario(seed, 6));
    folly::EventBase eventBase;
    ScriptedStaticHead head(&driver);
    ScriptedStaticTail tail(&driver);
    TestAllocator allocator;

    auto pipeline = StaticPipelineBuilder<
                        ScriptedStaticHead,
                        ScriptedStaticTail,
                        TestAllocator>()
                        .setEventBase(&eventBase)
                        .setHead(&head)
                        .setTail(&tail)
                        .setAllocator(&allocator)
                        .addNextDuplex<ScriptedHandler<0>>(model_0_tag, &driver)
                        .addNextDuplex<ScriptedHandler<1>>(model_1_tag, &driver)
                        .addNextDuplex<ScriptedHandler<2>>(model_2_tag, &driver)
                        .addNextDuplex<ScriptedHandler<3>>(model_3_tag, &driver)
                        .addNextDuplex<ScriptedHandler<4>>(model_4_tag, &driver)
                        .addNextDuplex<ScriptedHandler<5>>(model_5_tag, &driver)
                        .build();

    ASSERT_THAT(
        std::tuple(seed, driver.runOn(*pipeline)),
        Eq(std::tuple(seed, expected)));
    scenariosThatClose += closes(expected) ? 1 : 0;
  }

  // Enough scenarios close the pipeline during a pass.
  EXPECT_THAT(scenariosThatClose, Gt(300));
}

} // namespace
} // namespace apache::thrift::fast_thrift::channel_pipeline::test
