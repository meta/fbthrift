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

#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/ThriftRequestContext.h>

#include <gtest/gtest.h>

#include <array>
#include <memory>
#include <utility>

#include <folly/CancellationToken.h>
#include <folly/io/async/Request.h>

#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/ThriftConnContext.h>

namespace apache::thrift::fast_thrift::thrift {
namespace {

struct DestructionLog {
  std::array<folly::RequestContext*, 2> seen{};
  std::size_t count{0};
  std::shared_ptr<folly::RequestContext> replacement;
};

struct DestructionState {
  ~DestructionState() {
    log->seen[log->count++] = folly::RequestContext::try_get();
    folly::RequestContext::setContext(log->replacement);
  }

  DestructionLog* log;
};

struct FirstDestructionExtension {
  EXTENSION_ID(first_destruction_extension);
  using RequestState = DestructionState;
};

struct SecondDestructionExtension {
  EXTENSION_ID(second_destruction_extension);
  using RequestState = DestructionState;
};

const ExtensionLayout& destructionLayout() {
  static const ExtensionLayout layout = [] {
    ExtensionLayoutBuilder builder;
    const auto deleter = +[](void* state) noexcept {
      delete static_cast<DestructionState*>(state);
    };
    builder.add(FirstDestructionExtension::kId, deleter);
    builder.add(SecondDestructionExtension::kId, deleter);
    return std::move(builder).build();
  }();
  return layout;
}

void installDestructionStates(
    ThriftRequestContext& context, DestructionLog& log) {
  context.installExtensions(destructionLayout());
  context.adoptState<FirstDestructionExtension>(new DestructionState{&log});
  context.adoptState<SecondDestructionExtension>(new DestructionState{&log});
}

} // namespace

TEST(ThriftRequestContextTest, DefaultConstructedHasNoConnContext) {
  ThriftRequestContext rc;
  EXPECT_EQ(rc.getConnectionContext(), nullptr);
  EXPECT_NE(rc.getFollyRequestContext().get(), nullptr);
  EXPECT_FALSE(rc.isCancellationEnabled());
  EXPECT_FALSE(rc.getCancellationToken().canBeCancelled());
  EXPECT_FALSE(rc.requestCancellation());
}

TEST(ThriftRequestContextTest, SetConnectionContextStoresIt) {
  boost::intrusive_ptr<ThriftConnContext> conn{new ThriftConnContext()};
  conn->setSecurityProtocol("TLS1.3");

  ThriftRequestContext rc;
  rc.setConnectionContext(conn);
  ASSERT_EQ(rc.getConnectionContext(), conn.get());
  EXPECT_EQ(rc.getConnectionContext()->getSecurityProtocol(), "TLS1.3");
}

TEST(ThriftRequestContextTest, MethodNameIsEmptyUntilSet) {
  ThriftRequestContext rc;
  EXPECT_TRUE(rc.getMethodName().empty());

  rc.setMethodName("Service.method");
  EXPECT_EQ(rc.getMethodName(), "Service.method");
}

TEST(ThriftRequestContextTest, KeepsConnContextAliveAfterLocalReset) {
  boost::intrusive_ptr<ThriftConnContext> conn{new ThriftConnContext()};
  auto* raw = conn.get();

  ThriftRequestContext rc;
  rc.setConnectionContext(std::move(conn));
  EXPECT_EQ(raw->use_count(), 1);
  EXPECT_EQ(rc.getConnectionContext(), raw);
}

TEST(ThriftRequestContextTest, CancellationCallbackMayDestroyContext) {
  auto callerContext = std::make_shared<folly::RequestContext>();
  folly::RequestContextScopeGuard callerGuard(callerContext);
  auto context = std::make_unique<ThriftRequestContext>();
  context->enableCancellation();
  auto* contextPtr = context.get();
  auto requestContext = context->getFollyRequestContext().capture();
  bool callbackRan = false;
  folly::CancellationCallback callback(
      context->getCancellationToken(),
      [&, capturedContext = std::move(requestContext)] {
        callbackRan = true;
        EXPECT_EQ(folly::RequestContext::try_get(), callerContext.get());
        folly::RequestContextScopeGuard guard(capturedContext);
        EXPECT_EQ(folly::RequestContext::try_get(), capturedContext.get());
        context.reset();
        EXPECT_EQ(folly::RequestContext::try_get(), capturedContext.get());
      });

  EXPECT_TRUE(contextPtr->requestCancellation());
  EXPECT_TRUE(callbackRan);
  EXPECT_EQ(context, nullptr);
  EXPECT_EQ(folly::RequestContext::try_get(), callerContext.get());
}

TEST(ThriftRequestContextTest, CancellationSourceOutlivesContext) {
  auto context = std::make_unique<ThriftRequestContext>();
  context->enableCancellation();
  auto source = context->getCancellationSource();
  auto token = context->getCancellationToken();
  context.reset();

  EXPECT_TRUE(token.canBeCancelled());
  EXPECT_FALSE(source.requestCancellation());
  EXPECT_TRUE(token.isCancellationRequested());
}

TEST(ThriftRequestContextTest, OwnedStateDestructionUsesRequestContext) {
  auto callerContext = std::make_shared<folly::RequestContext>();
  folly::RequestContextScopeGuard callerGuard(callerContext);
  auto context = std::make_unique<ThriftRequestContext>();
  auto requestContext = context->getFollyRequestContext().capture();
  DestructionLog log{.replacement = std::make_shared<folly::RequestContext>()};
  installDestructionStates(*context, log);

  context.reset();

  EXPECT_EQ(log.count, 2);
  EXPECT_EQ(log.seen[0], requestContext.get());
  EXPECT_EQ(log.seen[1], requestContext.get());
  EXPECT_EQ(folly::RequestContext::try_get(), callerContext.get());
}

TEST(ThriftRequestContextTest, OwnedStateDestructionPreservesRequestAmbient) {
  auto context = std::make_unique<ThriftRequestContext>();
  auto* requestContext = context->getFollyRequestContext().get();
  DestructionLog log{.replacement = std::make_shared<folly::RequestContext>()};
  installDestructionStates(*context, log);
  folly::BorrowedRequestContextScopeGuard requestGuard(
      context->getFollyRequestContext());

  context.reset();

  EXPECT_EQ(log.count, 2);
  EXPECT_EQ(log.seen[0], requestContext);
  EXPECT_EQ(log.seen[1], requestContext);
  EXPECT_EQ(folly::RequestContext::try_get(), requestContext);
}

} // namespace apache::thrift::fast_thrift::thrift
