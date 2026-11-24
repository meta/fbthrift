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

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/EvbExecutionDomain.h>

#include <array>
#include <memory>
#include <optional>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include <folly/CPortability.h>
#include <folly/io/async/Request.h>
#include <folly/portability/GTest.h>

namespace apache::thrift::fast_thrift::channel_pipeline {
namespace {

static_assert(FollyExecutorDomain<EvbExecutionDomain>);
static_assert(!ExecutionDomain<folly::EventBase>);
static_assert(!std::is_polymorphic_v<EvbExecutionDomain>);

// Minimal generic domain with only the required operations.
struct OrdinaryDomain {
  bool inRunningDomainThread() const noexcept;
  template <typename Function>
  void post(Function&& function);
  bool operator==(const OrdinaryDomain&) const noexcept;
};

static_assert(ExecutionDomain<OrdinaryDomain>);
static_assert(!FollyExecutorDomain<OrdinaryDomain>);

FOLLY_PUSH_WARNING
FOLLY_GNU_DISABLE_WARNING("-Wdeprecated-declarations")
void drainLoop(folly::EventBase& eventBase) {
  // NOLINTNEXTLINE(facebook-hte-Deprecated)
  eventBase.loopIgnoreKeepAlive();
}
FOLLY_POP_WARNING

template <typename Function>
concept AcceptsCallable = requires(EvbExecutionDomain& domain, Function&& fn) {
  domain.post(std::forward<Function>(fn));
};

using SmallCallable = decltype([]() noexcept {});
using MoveOnlyCallable = decltype([owned = std::unique_ptr<int>{}]() noexcept {
  static_cast<void>(owned);
});

struct LargeCallable {
  std::array<unsigned char, 128> storage{};
  void operator()() noexcept {}
};

template <std::size_t Size>
struct ThrowingMoveCallable {
  std::array<unsigned char, Size> storage{};
  ThrowingMoveCallable() = default;
  ThrowingMoveCallable(const ThrowingMoveCallable&) = delete;
  [[noreturn]] ThrowingMoveCallable(ThrowingMoveCallable&&) noexcept(false) {
    throw std::runtime_error("move failed");
  }
  ThrowingMoveCallable& operator=(const ThrowingMoveCallable&) = delete;
  ThrowingMoveCallable& operator=(ThrowingMoveCallable&&) = delete;
  void operator()() noexcept {}
};

struct ThrowingCopyCallable {
  ThrowingCopyCallable() = default;
  [[noreturn]] ThrowingCopyCallable(const ThrowingCopyCallable&) {
    throw std::runtime_error("copy failed");
  }
  ThrowingCopyCallable(ThrowingCopyCallable&&) noexcept = default;
  ThrowingCopyCallable& operator=(const ThrowingCopyCallable&) = delete;
  ThrowingCopyCallable& operator=(ThrowingCopyCallable&&) = delete;
  void operator()() noexcept {}
};

struct RvalueOnlyCallable {
  void operator()() && noexcept {}
};

static_assert(AcceptsCallable<SmallCallable>);
static_assert(AcceptsCallable<MoveOnlyCallable>);
static_assert(AcceptsCallable<const ThrowingCopyCallable&>);
static_assert(!AcceptsCallable<MoveOnlyCallable&>);
static_assert(!AcceptsCallable<int>);
static_assert(!AcceptsCallable<std::nullptr_t>);
static_assert(!AcceptsCallable<void (*)(int)>);
static_assert(!AcceptsCallable<RvalueOnlyCallable>);
static_assert(noexcept(
    std::declval<EvbExecutionDomain&>().post(std::declval<SmallCallable>())));
static_assert(!noexcept(
    std::declval<EvbExecutionDomain&>().post(std::declval<LargeCallable>())));
static_assert(!noexcept(std::declval<EvbExecutionDomain&>().post(
    std::declval<ThrowingMoveCallable<1>>())));
static_assert(!noexcept(std::declval<EvbExecutionDomain&>().post(
    std::declval<ThrowingMoveCallable<128>>())));
static_assert(!noexcept(std::declval<EvbExecutionDomain&>().post(
    std::declval<const ThrowingCopyCallable&>())));

TEST(EvbExecutionDomainTest, AffinityAndForeignSubmission) {
  folly::EventBase eventBase;
  EvbExecutionDomain domain(eventBase);
  EXPECT_EQ(domain.getExecutor(), &eventBase);
  EXPECT_FALSE(domain.inRunningDomainThread());

  const auto loopThread = std::this_thread::get_id();
  bool submittedRan = false;
  std::thread producer([&] {
    EXPECT_FALSE(domain.inRunningDomainThread());
    domain.post([&] {
      EXPECT_EQ(std::this_thread::get_id(), loopThread);
      EXPECT_TRUE(domain.inRunningDomainThread());
      submittedRan = true;
    });
  });
  producer.join();
  EXPECT_FALSE(submittedRan);

  drainLoop(eventBase);
  EXPECT_TRUE(submittedRan);
  EXPECT_FALSE(domain.inRunningDomainThread());
}

TEST(EvbExecutionDomainTest, LocalSubmissionDefers) {
  folly::EventBase eventBase;
  EvbExecutionDomain domain(eventBase);
  bool idleSubmitted = false;
  domain.post([&] { idleSubmitted = true; });
  EXPECT_FALSE(idleSubmitted);

  bool runningSubmitted = false;
  bool executorSubmitted = false;
  domain.post([&] {
    domain.post([&] { runningSubmitted = true; });
    domain.getExecutor()->add([&] { executorSubmitted = true; });
    EXPECT_FALSE(runningSubmitted);
    EXPECT_FALSE(executorSubmitted);
  });

  drainLoop(eventBase);
  EXPECT_TRUE(idleSubmitted);
  EXPECT_TRUE(runningSubmitted);
  EXPECT_TRUE(executorSubmitted);
}

TEST(EvbExecutionDomainTest, PostDefersAndPreservesRequestContext) {
  folly::EventBase eventBase;
  EvbExecutionDomain domain(eventBase);
  auto original = folly::RequestContext::saveContext();
  auto expected = std::make_shared<folly::RequestContext>();
  bool ran = false;
  {
    folly::RequestContextScopeGuard context(expected);
    domain.post([&] {
      EXPECT_TRUE(domain.inRunningDomainThread());
      EXPECT_EQ(folly::RequestContext::saveContext(), expected);
      ran = true;
    });
    EXPECT_FALSE(ran);
  }
  drainLoop(eventBase);
  EXPECT_TRUE(ran);
  EXPECT_EQ(folly::RequestContext::saveContext(), original);
}

TEST(EvbExecutionDomainTest, PostPreservesHappensBeforeOrderAcrossThreads) {
  folly::EventBase eventBase;
  std::optional<EvbExecutionDomain> domain(std::in_place, eventBase);
  std::vector<int> order;
  auto record = [&](int value) {
    order.push_back(value);
    if (order.size() == 2) {
      domain.reset();
    }
  };

  domain->post([&] {
    std::thread producer([&] { domain->post([&] { record(1); }); });
    producer.join();
    domain->post([&] { record(2); });
  });

  eventBase.loop();
  EXPECT_EQ(order, (std::vector<int>{1, 2}));
}

TEST(EvbExecutionDomainTest, PostAcceptsMoveOnlyCallables) {
  folly::EventBase eventBase;
  EvbExecutionDomain domain(eventBase);
  std::vector<int> values;
  auto callback = [&](int value) {
    return [owned = std::make_unique<int>(value), &values] {
      values.push_back(*owned);
    };
  };

  domain.post(callback(1));
  domain.post(callback(2));
  domain.post(callback(3));
  drainLoop(eventBase);
  EXPECT_EQ(values, (std::vector<int>{1, 2, 3}));
}

TEST(EvbExecutionDomainTest, CallableConversionFailurePropagates) {
  folly::EventBase eventBase;
  EvbExecutionDomain domain(eventBase);
  EXPECT_THROW(domain.post(ThrowingMoveCallable<1>{}), std::runtime_error);
  EXPECT_THROW(domain.post(ThrowingMoveCallable<128>{}), std::runtime_error);
  const ThrowingCopyCallable callback;
  EXPECT_THROW(domain.post(callback), std::runtime_error);
  EXPECT_EQ(eventBase.getNotificationQueueSize(), 0);
}

TEST(EvbExecutionDomainTest, CopiesAndMovesRetainEventBase) {
  folly::EventBase eventBase;
  EXPECT_EQ(eventBase.loopKeepAliveCount(), 0);
  {
    EvbExecutionDomain domain(eventBase);
    EXPECT_EQ(eventBase.loopKeepAliveCount(), 1);
    {
      auto copy = domain;
      EXPECT_EQ(eventBase.loopKeepAliveCount(), 2);
      auto moved = std::move(copy);
      EXPECT_EQ(eventBase.loopKeepAliveCount(), 2);
      EXPECT_EQ(moved.getExecutor(), &eventBase);
      EXPECT_EQ(moved, domain);
    }
    EXPECT_EQ(eventBase.loopKeepAliveCount(), 1);
  }
  eventBase.loop();
  EXPECT_EQ(eventBase.loopKeepAliveCount(), 0);
}

TEST(EvbExecutionDomainTest, EqualityFollowsEventBase) {
  folly::EventBase eventBase;
  folly::EventBase otherEventBase;
  EvbExecutionDomain domain(eventBase);
  EXPECT_EQ(domain, EvbExecutionDomain(eventBase));
  EXPECT_NE(domain, EvbExecutionDomain(otherEventBase));
}

TEST(EvbExecutionDomainTest, LoopReturnsAfterLastBindingIsReleased) {
  folly::EventBase eventBase;
  std::optional<EvbExecutionDomain> domain(std::in_place, eventBase);
  bool ran = false;
  domain->post([&] {
    ran = true;
    domain.reset();
  });
  eventBase.loop();
  EXPECT_TRUE(ran);
  EXPECT_FALSE(domain.has_value());
  EXPECT_EQ(eventBase.loopKeepAliveCount(), 0);
}

TEST(EvbExecutionDomainTest, DestructorDrainsWorkUntilBindingIsReleased) {
  auto eventBase = std::make_unique<folly::EventBase>();
  std::optional<EvbExecutionDomain> domain(std::in_place, *eventBase);
  bool started = false;
  bool ran = false;
  bool completed = false;
  eventBase->runOnDestructionStart([&] { started = true; });
  eventBase->runOnDestruction([&] {
    EXPECT_TRUE(ran);
    completed = true;
  });
  domain->post([&] {
    EXPECT_TRUE(started);
    ran = true;
    domain.reset();
  });
  eventBase.reset();
  EXPECT_TRUE(started);
  EXPECT_TRUE(ran);
  EXPECT_TRUE(completed);
  EXPECT_FALSE(domain.has_value());
}

} // namespace
} // namespace apache::thrift::fast_thrift::channel_pipeline
