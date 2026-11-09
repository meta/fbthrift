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

// End-to-end test for a server stream RPC: a fast_thrift server (streamEchoes
// returns a StreamFactory<EchoResponse>) driven by a *classic* RocketClient,
// the same way the unary fast-server e2e tests work. Exercises the whole path:
// inbound REQUEST_STREAM -> generated process_streamEchoes -> mux -> per-stream
// sub-pipeline -> encoded chunks -> wire, consumed by ClientBufferedStream.

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <folly/coro/AsyncGenerator.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Task.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/ScopedEventBaseThread.h>

#include <thrift/lib/cpp2/async/RocketClientChannel.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/HandlerTag.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/ContextImpl.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/FastThriftServer.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/stream/ProducerPipeline.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/stream/StreamElementEncoder.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/stream/StreamFactory.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/stream/common/Messages.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/test/if/gen-cpp2/FastThriftServer.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/test/if/gen-cpp2/FastThriftServerAsyncClient.h>

namespace apache::thrift::fast_thrift::thrift {

namespace ftt = apache::thrift::fast_thrift::thrift;
namespace cp = channel_pipeline;
namespace integration = apache::thrift::fast_thrift::thrift::test::integration;
using integration::EchoResponse;

namespace {

using Context = cp::detail::ContextImpl;

HANDLER_TAG(producer);

// Emits `count` EchoResponse items on demand (via the framework encoder), then
// Complete.
template <typename Ctx>
class EchoProducer {
 public:
  EchoProducer(
      stream::StreamElementEncoder<EchoResponse> encoder, int32_t count)
      : encoder_(encoder), remaining_(count) {}

  void handlerAdded(Ctx& /*ctx*/) noexcept {}
  void handlerRemoved(Ctx& /*ctx*/) noexcept {}
  void onPipelineActive(Ctx& /*ctx*/) noexcept {}
  void onPipelineInactive(Ctx& /*ctx*/) noexcept {}
  void onReadReady(Ctx& /*ctx*/) noexcept {}
  void onWriteReady(Ctx& /*ctx*/) noexcept {}
  void onException(Ctx& ctx, folly::exception_wrapper&& e) noexcept {
    ctx.fireException(std::move(e));
  }
  cp::Result onWrite(Ctx& ctx, cp::TypeErasedBox&& msg) noexcept {
    return ctx.fireWrite(std::move(msg));
  }
  cp::Result onRead(Ctx& ctx, cp::TypeErasedBox&& msg) noexcept {
    auto& m = msg.get<stream::ControlMessage>();
    if (m.payload.is<stream::RequestN>()) {
      const uint64_t n = m.payload.get<stream::RequestN>().n;
      for (uint64_t i = 0; i < n && remaining_ > 0; ++i, --remaining_) {
        EchoResponse resp;
        resp.message() = "item";
        (void)ctx.fireWrite(
            cp::erase_and_box(
                stream::DataMessage{
                    .payload = encoder_.encodeValue(std::move(resp))}));
      }
      if (remaining_ == 0 && !completed_) {
        completed_ = true;
        (void)ctx.fireWrite(
            cp::erase_and_box(
                stream::DataMessage{.payload = stream::Complete{}}));
      }
      return cp::Result::Success;
    }
    if (m.payload.is<stream::Cancel>()) {
      return cp::Result::Success;
    }
    return ctx.fireRead(std::move(msg));
  }

 private:
  stream::StreamElementEncoder<EchoResponse> encoder_;
  int32_t remaining_;
  bool completed_{false};
};

class StreamHandler : public FastServiceHandler<integration::FastThriftServer> {
 public:
  void async_tm_streamEchoes(
      ftt::FastHandlerCallbackPtr<stream::StreamFactory<EchoResponse>> cb,
      int32_t count) override {
    cb->result(
        stream::StreamFactory<EchoResponse>(
            [count](
                stream::ProducerPipeline::Builder& builder,
                const stream::StreamElementEncoder<EchoResponse>& encoder) {
              builder.addNextDuplex<EchoProducer<Context>>(
                  producer_tag,
                  std::make_unique<EchoProducer<Context>>(encoder, count));
            }));
  }

  void async_tm_streamEchoesWithResponse(
      ftt::FastHandlerCallbackPtr<
          stream::ResponseAndStreamFactory<EchoResponse, EchoResponse>> cb,
      int32_t count) override {
    EchoResponse initial;
    initial.message() = "initial";
    cb->result(
        stream::ResponseAndStreamFactory<EchoResponse, EchoResponse>{
            .response = std::move(initial),
            .factory = stream::StreamFactory<EchoResponse>(
                [count](
                    stream::ProducerPipeline::Builder& builder,
                    const stream::StreamElementEncoder<EchoResponse>& encoder) {
                  builder.addNextDuplex<EchoProducer<Context>>(
                      producer_tag,
                      std::make_unique<EchoProducer<Context>>(encoder, count));
                })});
  }
};

class FastThriftServerStreamE2ETest : public ::testing::Test {
 protected:
  void SetUp() override {
    handler_ = std::make_shared<StreamHandler>();
    ftt::FastThriftServerConfig config;
    config.address = folly::SocketAddress("::1", 0);
    config.numIOThreads = 1;
    config.enableStreamMux = true;
    server_ = std::make_unique<ftt::FastThriftServer>(std::move(config));
    server_->setInterface(handler_);
    server_->start();
    clientThread_ = std::make_unique<folly::ScopedEventBaseThread>();
  }

  void TearDown() override {
    clientThread_.reset();
    server_->stop();
    server_.reset();
  }

  std::shared_ptr<StreamHandler> handler_;
  std::unique_ptr<ftt::FastThriftServer> server_;
  std::unique_ptr<folly::ScopedEventBaseThread> clientThread_;
};

} // namespace

TEST_F(FastThriftServerStreamE2ETest, ClientReceivesStreamItems) {
  auto evb = clientThread_->getEventBaseKeepAlive();
  std::vector<std::string> items;
  folly::coro::blockingWait(
      folly::coro::co_withExecutor(
          evb, folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
            auto socket =
                folly::AsyncSocket::newSocket(evb.get(), server_->getAddress());
            auto channel = apache::thrift::RocketClientChannel::newChannel(
                std::move(socket));
            apache::thrift::Client<integration::FastThriftServer> client(
                std::move(channel));
            auto stream = co_await client.co_streamEchoes(3);
            auto gen = std::move(stream).toAsyncGenerator();
            while (auto item = co_await gen.next()) {
              items.push_back(*item->message());
            }
          })));

  EXPECT_EQ(items, (std::vector<std::string>{"item", "item", "item"}));
}

TEST_F(
    FastThriftServerStreamE2ETest,
    ClientReceivesInitialResponseAndStreamItems) {
  auto evb = clientThread_->getEventBaseKeepAlive();
  std::string initialResponse;
  std::vector<std::string> items;
  folly::coro::blockingWait(
      folly::coro::co_withExecutor(
          evb, folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
            auto socket =
                folly::AsyncSocket::newSocket(evb.get(), server_->getAddress());
            auto channel = apache::thrift::RocketClientChannel::newChannel(
                std::move(socket));
            apache::thrift::Client<integration::FastThriftServer> client(
                std::move(channel));
            auto [response, stream] =
                co_await client.co_streamEchoesWithResponse(3);
            initialResponse = *response.message();
            auto gen = std::move(stream).toAsyncGenerator();
            while (auto item = co_await gen.next()) {
              items.push_back(*item->message());
            }
          })));

  // The initial response is delivered before, and independently of, the stream
  // elements: the classic client decodes it from the stream's first PAYLOAD.
  EXPECT_EQ(initialResponse, "initial");
  EXPECT_EQ(items, (std::vector<std::string>{"item", "item", "item"}));
}

} // namespace apache::thrift::fast_thrift::thrift
