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

#include <thrift/lib/cpp2/fast_thrift/thrift/server/ThriftServerConnectionFactory.h>

#include <memory>
#include <new>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/EventBase.h>
#include <folly/logging/LogCategory.h>
#include <folly/logging/LogMessage.h>
#include <folly/logging/LoggerDB.h>
#include <folly/logging/test/TestLogHandler.h>
#include <folly/portability/GMock.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/ErasedStaticHandler.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/handler/FragmentCompletionTracker.h>
#include <thrift/lib/cpp2/fast_thrift/frame/write/handler/InlineFrameFragmentationHandler.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/common/RocketTransportLifecycle.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/server/RocketServerEventFactory.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/RocketServerPipeline.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/CachedEkmExtension.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/event_handler/Cpp2BridgeExtension.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/event_handler/Cpp2ContextAdapter.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/extension/ThriftConnectionExtension.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/extension/ThriftExtensionPipelineHandler.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/framework/FastServerModule.h>
#include <thrift/lib/cpp2/fast_thrift/transport/test/TestAsyncTransport.h>
#include <thrift/lib/cpp2/server/Cpp2ConnContext.h>

namespace {
struct EkmCaptureProbe {
  int captures{0};
  int adapters{0};
  bool failCapture{false};
  bool failCaptureAllocation{false};
};
EkmCaptureProbe* ekmCaptureProbe = nullptr;
constexpr folly::StringPiece kFactoryEkmLabel{"factory-ekm"};
constexpr uint16_t kFactoryEkmLength = 32;
} // namespace

namespace apache::thrift::detail {
THRIFT_PLUGGABLE_FUNC_SET_TEST(
    std::vector<EkmInfo>, populateCachedEkms, const folly::AsyncTransport&) {
  if (ekmCaptureProbe == nullptr) {
    return {};
  }
  ++ekmCaptureProbe->captures;
  if (ekmCaptureProbe->failCaptureAllocation) {
    throw std::bad_alloc{};
  }
  if (ekmCaptureProbe->failCapture) {
    throw std::runtime_error("test EKM capture failure");
  }
  std::vector<EkmInfo> entries;
  entries.push_back(
      EkmInfo{
          .label = kFactoryEkmLabel.str(),
          .length = kFactoryEkmLength,
          .buf = folly::IOBuf::copyBuffer("captured-ekm")});
  return entries;
}
} // namespace apache::thrift::detail

namespace apache::thrift::fast_thrift::thrift::server::test {
namespace {

using namespace testing;

class TestAppAdapterFactory final : public ThriftServerAppAdapterFactory {
 public:
  ThriftServerAppAdapter::Ptr getAppAdapter(
      std::shared_ptr<ThriftServerAppAdapterFactory>) override {
    return ThriftServerAppAdapter::Ptr{new ThriftServerAppAdapter()};
  }

  void populateMethodMetadata(
      ThriftServerMethodMetadataRegistry&) const override {}
};

struct LifecycleCounts {
  int builderCalls{0};
  int handlerAdded{0};
  int handlerRemoved{0};
  int handlerDestroyed{0};
  int transportStarted{0};
  int transportDisconnected{0};
  int transportDestroyed{0};
};

class TestTransportOwner {
 public:
  TestTransportOwner(
      folly::AsyncTransport::UniquePtr socket, LifecycleCounts& counts)
      : socket_(std::move(socket)), counts_(counts) {}

  ~TestTransportOwner() { ++counts_.transportDestroyed; }

  void start() noexcept { ++counts_.transportStarted; }

  void disconnect(folly::exception_wrapper&&) noexcept {
    ++counts_.transportDisconnected;
    socket_->close();
  }

 private:
  folly::AsyncTransport::UniquePtr socket_;
  LifecycleCounts& counts_;
};

class TestNegotiationHandler {
 public:
  explicit TestNegotiationHandler(LifecycleCounts& counts) : counts_(counts) {}
  ~TestNegotiationHandler() { ++counts_.handlerDestroyed; }

  void handlerAdded(channel_pipeline::detail::StaticHandlerContext&) noexcept {
    ++counts_.handlerAdded;
  }

  void handlerRemoved(
      channel_pipeline::detail::StaticHandlerContext&) noexcept {
    ++counts_.handlerRemoved;
  }

  void onPipelineActive(
      channel_pipeline::detail::StaticHandlerContext&) noexcept {}
  void onPipelineInactive(
      channel_pipeline::detail::StaticHandlerContext&) noexcept {}
  void onReadReady(channel_pipeline::detail::StaticHandlerContext&) noexcept {}
  channel_pipeline::Result onRead(
      channel_pipeline::detail::StaticHandlerContext& ctx,
      channel_pipeline::TypeErasedBox&& message) noexcept {
    return ctx.fireRead(std::move(message));
  }
  void onException(
      channel_pipeline::detail::StaticHandlerContext& ctx,
      folly::exception_wrapper&& error) noexcept {
    ctx.fireException(std::move(error));
  }

 private:
  LifecycleCounts& counts_;
};

folly::AsyncTransport::UniquePtr makeTransport(folly::EventBase& eventBase) {
  return folly::AsyncTransport::UniquePtr(
      new transport::test::TestAsyncTransport(&eventBase));
}

ThriftServerConnectionFactoryConfig makeConfig() {
  ThriftServerConnectionFactoryConfig config;
  config.handler = std::make_shared<TestAppAdapterFactory>();
  return config;
}

class EkmCheckingAdapterFactory final : public ThriftServerAppAdapterFactory {
 public:
  EkmCheckingAdapterFactory(EkmCaptureProbe& probe, int expectedCaptures)
      : probe_(probe), expectedCaptures_(expectedCaptures) {}

  ThriftServerAppAdapter::Ptr getAppAdapter(
      std::shared_ptr<ThriftServerAppAdapterFactory>) override {
    ++probe_.adapters;
    EXPECT_EQ(probe_.captures, expectedCaptures_);
    return ThriftServerAppAdapter::Ptr{new ThriftServerAppAdapter()};
  }

  void populateMethodMetadata(
      ThriftServerMethodMetadataRegistry&) const override {}

 private:
  EkmCaptureProbe& probe_;
  int expectedCaptures_;
};

struct RequiresEkmExtension {
  static constexpr bool kUsesCachedEkms = true;
  RequestVerdict onRequest(const ThriftRequestView&) noexcept {
    return RequestVerdict::proceed();
  }
};

class EkmFactoryTest : public ::testing::Test {
 protected:
  void SetUp() override { ekmCaptureProbe = &probe_; }
  void TearDown() override { ekmCaptureProbe = nullptr; }

  ThriftServerConnectionFactoryConfig cacheConfig(int expectedCaptures) {
    auto config = makeConfig();
    config.handler =
        std::make_shared<EkmCheckingAdapterFactory>(probe_, expectedCaptures);
    config.requiresCachedEkms = true;
    ExtensionLayoutBuilder builder;
    builder.addOwned<CachedEkmExtension::ConnState>(CachedEkmExtension::kId);
    config.connExtensionLayout =
        std::make_shared<const ExtensionLayout>(std::move(builder).build());
    return config;
  }

  folly::EventBase eventBase_;
  EkmCaptureProbe probe_;
  const folly::SocketAddress address_{"127.0.0.1", 4321};
};

template <typename Factory>
class BridgeEkmFactoryTest : public EkmFactoryTest {};

using BridgeEkmFactoryTypes = ::testing::
    Types<ThriftServerConnectionFactory, DynamicThriftServerConnectionFactory>;
TYPED_TEST_SUITE(BridgeEkmFactoryTest, BridgeEkmFactoryTypes);

TYPED_TEST(BridgeEkmFactoryTest, BridgeOwnsCacheDespiteNativeRequirement) {
  for (const bool preregisterNativeCache : {false, true}) {
    SCOPED_TRACE(preregisterNativeCache);
    const int capturesBefore = this->probe_.captures;
    const int adaptersBefore = this->probe_.adapters;
    auto config = this->cacheConfig(capturesBefore);
    ExtensionLayoutBuilder builder;
    builder.add(Cpp2BridgeExtension::kId);
    if (preregisterNativeCache) {
      builder.addOwned<CachedEkmExtension::ConnState>(CachedEkmExtension::kId);
    }
    config.connExtensionLayout =
        std::make_shared<const ExtensionLayout>(std::move(builder).build());
    auto module = StaticFastServerModule("bridge_cache_requirement")
                      .addThriftExtension<RequiresEkmExtension>();
    config.thriftPipelineRegistrations.push_back(
        std::move(module).registration());
    TypeParam factory(std::move(config));

    EXPECT_EQ(factory.getPreStopTLSEkmCapture().capture, nullptr);
    auto connection = factory.getConnection(
        makeTransport(this->eventBase_), this->address_, nullptr);
    EXPECT_EQ(this->probe_.captures, capturesBefore);
    EXPECT_EQ(this->probe_.adapters, adaptersBefore + 1);
    EXPECT_TRUE(connection.connContext->extensionSlots().installed(
        Cpp2BridgeExtension::kId));
    EXPECT_FALSE(connection.connContext->extensionSlots().installed(
        CachedEkmExtension::kId));
    EXPECT_EQ(
        connection.connContext->template tryState<CachedEkmExtension>(),
        nullptr);
    EXPECT_EQ(
        connection.connContext->getCachedEkm(
            kFactoryEkmLabel, kFactoryEkmLength),
        nullptr);

    Cpp2ConnContextAdapter bridge(connection.connContext, {});
    EXPECT_EQ(this->probe_.captures, capturesBefore + 1);
    const ThriftConnectionView view(*connection.connContext);
    const std::string_view label(
        kFactoryEkmLabel.data(), kFactoryEkmLabel.size());
    auto cached = view.cachedEkm(label, kFactoryEkmLength);
    ASSERT_NE(cached, nullptr);
    EXPECT_EQ(cached->to<std::string>(), "captured-ekm");
    bridge.connectionClosed();
    EXPECT_EQ(bridge.get().getTransport(), nullptr);
    auto closed = view.cachedEkm(label, kFactoryEkmLength);
    ASSERT_NE(closed, nullptr);
    EXPECT_EQ(closed->data(), cached->data());
    EXPECT_EQ(this->probe_.captures, capturesBefore + 1);
  }
}

TEST_F(EkmFactoryTest, CapturesBeforeAdapterConstructionWithoutRequests) {
  ThriftServerConnectionFactory factory(cacheConfig(1));
  auto connection =
      factory.getConnection(makeTransport(eventBase_), address_, nullptr);

  EXPECT_EQ(probe_.captures, 1);
  EXPECT_EQ(probe_.adapters, 1);
  auto cached =
      connection.connContext->getCachedEkm(kFactoryEkmLabel, kFactoryEkmLength);
  ASSERT_NE(cached, nullptr);
  EXPECT_EQ(cached->to<std::string>(), "captured-ekm");
}

TEST_F(EkmFactoryTest, NoRequirementSkipsCaptureAndCacheStorage) {
  auto config = cacheConfig(0);
  config.requiresCachedEkms = false;
  ThriftServerConnectionFactory factory(std::move(config));
  auto connection =
      factory.getConnection(makeTransport(eventBase_), address_, nullptr);

  EXPECT_EQ(probe_.captures, 0);
  EXPECT_EQ(probe_.adapters, 1);
  EXPECT_EQ(connection.connContext->tryState<CachedEkmExtension>(), nullptr);
  EXPECT_EQ(factory.getPreStopTLSEkmCapture().capture, nullptr);
}

TEST_F(EkmFactoryTest, FactoryCaptureIsAdoptedWithoutAnotherExport) {
  ThriftServerConnectionFactory factory(cacheConfig(1));
  auto socket = makeTransport(eventBase_);
  auto capture = factory.getPreStopTLSEkmCapture();
  ASSERT_NE(capture.capture, nullptr);
  auto peer = std::make_unique<connection::PeerSecurityInfo>();
  peer->securityProtocol = "stopTLS";
  peer->capturedEkms.emplace(capture.capture(capture.owner, *socket));
  auto* original = peer->capturedEkms->front().buf.get();
  auto connection =
      factory.getConnection(std::move(socket), address_, std::move(peer));

  EXPECT_EQ(probe_.captures, 1);
  EXPECT_EQ(probe_.adapters, 1);
  const auto* cache = connection.connContext->tryState<CachedEkmExtension>();
  ASSERT_NE(cache, nullptr);
  ASSERT_NE(cache->begin(), cache->end());
  EXPECT_EQ(cache->begin()->buf.get(), original);
  EXPECT_EQ(connection.connContext->getSecurityProtocol(), "stopTLS");
}

TEST_F(EkmFactoryTest, CapturedEmptyCacheIsNotRecaptured) {
  ThriftServerConnectionFactory factory(cacheConfig(0));
  auto peer = std::make_unique<connection::PeerSecurityInfo>();
  peer->capturedEkms.emplace();
  auto connection = factory.getConnection(
      makeTransport(eventBase_), address_, std::move(peer));

  EXPECT_EQ(probe_.captures, 0);
  EXPECT_EQ(probe_.adapters, 1);
  const auto* cache = connection.connContext->tryState<CachedEkmExtension>();
  ASSERT_NE(cache, nullptr);
  EXPECT_EQ(cache->begin(), cache->end());
}

TEST_F(EkmFactoryTest, BorrowedCacheSlotDoesNotEnableCapture) {
  ExtensionLayoutBuilder builder;
  builder.add(CachedEkmExtension::kId);
  auto config = cacheConfig(0);
  config.connExtensionLayout =
      std::make_shared<const ExtensionLayout>(std::move(builder).build());
  ThriftServerConnectionFactory factory(std::move(config));
  auto connection =
      factory.getConnection(makeTransport(eventBase_), address_, nullptr);

  EXPECT_EQ(probe_.captures, 0);
  EXPECT_TRUE(connection.connContext->extensionSlots().installed(
      CachedEkmExtension::kId));
  EXPECT_EQ(connection.connContext->tryState<CachedEkmExtension>(), nullptr);
  EXPECT_EQ(factory.getPreStopTLSEkmCapture().capture, nullptr);
}

TEST_F(EkmFactoryTest, ExtensionRequirementNeedsExplicitCacheRegistration) {
  for (const bool registerCache : {false, true}) {
    SCOPED_TRACE(registerCache);
    const int expectedCaptures = probe_.captures + (registerCache ? 1 : 0);
    auto config = cacheConfig(expectedCaptures);
    if (!registerCache) {
      config.connExtensionLayout.reset();
    }
    config.requiresCachedEkms = false;
    auto registration = makeThriftExtensionHandlerFactory<RequiresEkmExtension>(
        channel_pipeline::fnv1a_hash("test_cache_extension"));
    EXPECT_TRUE(registration.requiresCachedEkms());
    config.thriftPipelineRegistrations.emplace_back(std::move(registration));
    ThriftServerConnectionFactory factory(std::move(config));
    auto connection =
        factory.getConnection(makeTransport(eventBase_), address_, nullptr);

    EXPECT_EQ(probe_.captures, expectedCaptures);
    EXPECT_EQ(
        connection.connContext->extensionSlots().installed(
            CachedEkmExtension::kId),
        registerCache);
    EXPECT_EQ(
        connection.connContext->tryState<CachedEkmExtension>() != nullptr,
        registerCache);
    EXPECT_EQ(
        factory.getPreStopTLSEkmCapture().capture != nullptr, registerCache);
  }
}

TEST_F(EkmFactoryTest, TypedSegmentRequirementDoesNotRegisterNativeCache) {
  auto config = cacheConfig(0);
  config.connExtensionLayout.reset();
  config.requiresCachedEkms = false;
  auto module = StaticFastServerModule("test_cache_segment")
                    .addThriftExtension<RequiresEkmExtension>();
  EXPECT_TRUE(module.requiresCachedEkms());
  auto registration = std::move(module).registration();
  EXPECT_TRUE(registration.requiresCachedEkms());
  config.thriftPipelineRegistrations.push_back(std::move(registration));
  ThriftServerConnectionFactory factory(std::move(config));
  auto connection =
      factory.getConnection(makeTransport(eventBase_), address_, nullptr);

  EXPECT_EQ(probe_.captures, 0);
  EXPECT_FALSE(connection.connContext->extensionSlots().installed(
      CachedEkmExtension::kId));
  EXPECT_EQ(connection.connContext->tryState<CachedEkmExtension>(), nullptr);
  EXPECT_EQ(factory.getPreStopTLSEkmCapture().capture, nullptr);
}

TEST_F(EkmFactoryTest, DynamicRawRequirementDoesNotRegisterNativeCache) {
  using Adapter = ThriftExtensionPipelineHandler<
      RequiresEkmExtension,
      ThriftPipelineHandlerContext>;
  auto config = cacheConfig(0);
  config.connExtensionLayout.reset();
  config.requiresCachedEkms = false;
  auto registration = ThriftPipelineHandlerFactory(
      [](ExtensionStateStore& store) {
        return channel_pipeline::detail::makeHandlerNode<Adapter>(
            channel_pipeline::fnv1a_hash("test_dynamic_cache_extension"),
            std::make_unique<Adapter>(store));
      },
      true);
  EXPECT_TRUE(registration.requiresCachedEkms());
  config.thriftPipelineRegistrations.emplace_back(std::move(registration));
  DynamicThriftServerConnectionFactory factory(std::move(config));
  auto connection =
      factory.getConnection(makeTransport(eventBase_), address_, nullptr);

  EXPECT_EQ(probe_.captures, 0);
  EXPECT_FALSE(connection.connContext->extensionSlots().installed(
      CachedEkmExtension::kId));
  EXPECT_EQ(connection.connContext->tryState<CachedEkmExtension>(), nullptr);
  EXPECT_EQ(factory.getPreStopTLSEkmCapture().capture, nullptr);
}

TEST_F(EkmFactoryTest, CaptureFailurePreventsAdapterConstruction) {
  ThriftServerConnectionFactory factory(cacheConfig(1));
  probe_.failCapture = true;
  EXPECT_THROW(
      factory.getConnection(makeTransport(eventBase_), address_, nullptr),
      connection::CachedEkmCaptureFailure);
  EXPECT_EQ(probe_.captures, 1);
  EXPECT_EQ(probe_.adapters, 0);
}

TEST_F(EkmFactoryTest, AcceptDropsConnectionOnCaptureAllocationFailure) {
  ThriftServerConnectionFactory factory(cacheConfig(3));
  probe_.failCaptureAllocation = true;

  EXPECT_FALSE(factory.tryGetConnectionForAccept(
      makeTransport(eventBase_), address_, nullptr));
  EXPECT_THROW(
      factory.getConnection(makeTransport(eventBase_), address_, nullptr),
      std::bad_alloc);
  EXPECT_EQ(probe_.captures, 2);
  EXPECT_EQ(probe_.adapters, 0);

  probe_.failCaptureAllocation = false;
  auto connection = factory.tryGetConnectionForAccept(
      makeTransport(eventBase_), address_, nullptr);
  ASSERT_TRUE(connection);
  EXPECT_EQ(probe_.adapters, 1);
  EXPECT_NE(connection->connContext->tryState<CachedEkmExtension>(), nullptr);
}

TEST(ThriftServerConnectionFactoryTest, DefaultBuildsNativeRocketTransport) {
  folly::EventBase eventBase;
  ThriftServerConnectionFactory factory(makeConfig());

  auto connection = factory.getConnection(
      makeTransport(eventBase),
      folly::SocketAddress("127.0.0.1", 1234),
      nullptr);

  const auto& rocketConnection =
      connection.thriftTransportAdapter->rocketConnection();
  EXPECT_NE(rocketConnection.transportHandler, nullptr);
  EXPECT_TRUE(rocketConnection.pipeline);
  EXPECT_FALSE(rocketConnection.transportLifecycle);
}

TEST(ThriftServerConnectionFactoryTest, ZeroCopyWarningUsesFactoryLogCategory) {
  const auto logs = std::make_shared<folly::TestLogHandler>();
  folly::LoggerDB::get().getCategory("")->addHandler(logs);
  folly::EventBase eventBase;
  ThriftServerConnectionFactoryConfig config = makeConfig();
  config.zeroCopyThreshold = 1;
  ThriftServerConnectionFactory factory(std::move(config));

  // TestAsyncTransport refuses MSG_ZEROCOPY.
  const ThriftServerConnection connection = factory.getConnection(
      makeTransport(eventBase),
      folly::SocketAddress("127.0.0.1", 1234),
      nullptr);

  std::vector<std::string> warningCategories;
  for (const auto& [message, handlerCategory] : logs->getMessages()) {
    if (message.getMessage() == "MSG_ZEROCOPY not supported on this socket") {
      warningCategories.push_back(message.getCategory()->getName());
    }
  }
  // XLOG names the category after the source path, which starts with `xplat/`
  // in the xplat build.
  EXPECT_THAT(
      warningCategories,
      ElementsAre(
          EndsWith(".thrift.server.ThriftServerConnectionFactory.cpp")));
}

TEST(
    ThriftServerConnectionFactoryTest,
    CustomBuilderConnectionAndHandlerUseExistingOwnership) {
  folly::EventBase eventBase;
  LifecycleCounts counts;
  const folly::SocketAddress expectedClient("127.0.0.1", 4321);

  auto config = makeConfig();
  config.rocketConnectionBuilder = [&](folly::AsyncTransport::UniquePtr socket,
                                       const folly::SocketAddress& clientAddr,
                                       ServerStatsShard* statsShard) {
    ++counts.builderCalls;
    EXPECT_EQ(clientAddr, expectedClient);
    EXPECT_EQ(socket->getEventBase(), &eventBase);
    EXPECT_EQ(statsShard, nullptr);

    auto connection =
        std::make_unique<rocket::server::RocketServerConnection>();
    connection->transportLifecycle = rocket::RocketTransportLifecycle::own(
        std::make_unique<TestTransportOwner>(std::move(socket), counts));
    return RocketServerConnectionBuildResult{
        .connection = std::move(connection),
        .transportNegotiationHandler =
            channel_pipeline::detail::makeErasedStaticHandler<
                TestNegotiationHandler>(
                channel_pipeline::fnv1a_hash(
                    "test_transport_negotiation_handler"),
                counts),
    };
  };

  {
    ThriftServerConnectionFactory factory(std::move(config));
    auto connection = factory.getConnection(
        makeTransport(eventBase), expectedClient, nullptr);

    EXPECT_EQ(counts.builderCalls, 1);
    EXPECT_EQ(counts.handlerAdded, 1);
    EXPECT_EQ(counts.transportStarted, 0);

    connection.start();
    EXPECT_EQ(counts.transportStarted, 1);
  }

  EXPECT_EQ(counts.handlerRemoved, 1);
  EXPECT_EQ(counts.handlerDestroyed, 1);
  EXPECT_EQ(counts.transportDisconnected, 1);
  EXPECT_EQ(counts.transportDestroyed, 1);
}

// Both rocket pipelines build their fragmenter from this alias.
static_assert(
    std::is_same_v<
        ServerFragmentationFrameHandler,
        frame::write::handler::InlineFrameFragmentationHandlerT<
            frame::write::handler::FragmentCompletionTrackerT<
                rocket::server::RocketServerEventFactory>>>,
    "the rocket pipelines send every frame through the inline fragmenter");

} // namespace
} // namespace apache::thrift::fast_thrift::thrift::server::test
