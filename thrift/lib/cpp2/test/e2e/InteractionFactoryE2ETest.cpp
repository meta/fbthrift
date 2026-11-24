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

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include <folly/coro/AsyncGenerator.h>
#include <folly/coro/GtestHelpers.h>
#include <thrift/lib/cpp2/async/BiDiStream.h>
#include <thrift/lib/cpp2/async/Sink.h>
#include <thrift/lib/cpp2/test/e2e/E2ETestFixture.h>
#include <thrift/lib/cpp2/test/e2e/gen-cpp2/Calculator.h>

namespace apache::thrift {
namespace {

using IntGenerator = folly::coro::AsyncGenerator<int32_t&&>;

IntGenerator oneTwoThree() {
  co_yield int32_t(1);
  co_yield int32_t(2);
  co_yield int32_t(3);
}

struct FactoryCalculatorHandler final
    : ServiceHandler<detail::test::Calculator> {
  struct AdditionTile final : AdditionIf {
    explicit AdditionTile(int32_t initialSum) : sum_(initialSum) {}

    void sync_accumulatePrimitive(int32_t value) override { sum_ += value; }
    int32_t sync_getPrimitive() override { return sum_; }

   private:
    int32_t sum_;
  };

  TileAndResponse<AdditionIf, SinkConsumer<int32_t, int32_t>>
  sync_additionWithSink(int32_t initialSum) override {
    return {std::make_unique<AdditionTile>(initialSum), sumSink()};
  }

  TileAndResponse<
      AdditionIf,
      ResponseAndSinkConsumer<int32_t, int32_t, int32_t>>
  sync_additionWithSinkAndResponse(int32_t initialSum) override {
    return {
        std::make_unique<AdditionTile>(initialSum), {initialSum, sumSink()}};
  }

  TileAndResponse<AdditionIf, StreamTransformation<int32_t, int32_t>>
  sync_additionWithBidi(int32_t initialSum) override {
    return {
        std::make_unique<AdditionTile>(initialSum),
        {[](IntGenerator input) -> IntGenerator {
          while (IntGenerator::NextResult item = co_await input.next()) {
            co_yield int32_t(*item * 2);
          }
        }}};
  }

 private:
  static SinkConsumer<int32_t, int32_t> sumSink() {
    return {
        [](IntGenerator input) -> folly::coro::Task<int32_t> {
          int32_t sum = 0;
          while (IntGenerator::NextResult item = co_await input.next()) {
            sum += *item;
          }
          co_return sum;
        },
        4};
  }
};

class InteractionFactoryE2ETest : public test::E2ETestFixture {};

CO_TEST_F(InteractionFactoryE2ETest, SinkFactoryReturnsHandleAndSink) {
  testConfig({std::make_shared<FactoryCalculatorHandler>()});
  std::unique_ptr<Client<detail::test::Calculator>> client =
      makeClient<detail::test::Calculator>();
  std::pair<
      Client<detail::test::Calculator>::Addition,
      ClientSink<int32_t, int32_t>>
      result = co_await client->co_additionWithSink(42);

  EXPECT_EQ(co_await result.second.sink(oneTwoThree()), 6);
  EXPECT_EQ(co_await result.first.co_getPrimitive(), 42);
  co_await result.first.co_accumulatePrimitive(9);
  EXPECT_EQ(co_await result.first.co_getPrimitive(), 51);
}

CO_TEST_F(InteractionFactoryE2ETest, SinkFactoryReturnsInitialResponse) {
  testConfig({std::make_shared<FactoryCalculatorHandler>()});
  std::unique_ptr<Client<detail::test::Calculator>> client =
      makeClient<detail::test::Calculator>();
  std::pair<
      Client<detail::test::Calculator>::Addition,
      ResponseAndClientSink<int32_t, int32_t, int32_t>>
      result = co_await client->co_additionWithSinkAndResponse(42);

  EXPECT_EQ(result.second.response, 42);
  EXPECT_EQ(co_await result.second.sink.sink(oneTwoThree()), 6);
  EXPECT_EQ(co_await result.first.co_getPrimitive(), 42);
  co_await result.first.co_accumulatePrimitive(9);
  EXPECT_EQ(co_await result.first.co_getPrimitive(), 51);
}

CO_TEST_F(InteractionFactoryE2ETest, BidiFactoryWithoutInitialResponse) {
  testConfig({std::make_shared<FactoryCalculatorHandler>()});
  std::unique_ptr<Client<detail::test::Calculator>> client =
      makeClient<detail::test::Calculator>();
  std::pair<
      Client<detail::test::Calculator>::Addition,
      BidirectionalStream<int32_t, int32_t>>
      result = co_await client->co_additionWithBidi(42);

  co_await result.second.sink.sink(oneTwoThree());
  IntGenerator output = std::move(result.second.stream).toAsyncGenerator();
  std::vector<int32_t> received;
  while (IntGenerator::NextResult item = co_await output.next()) {
    received.push_back(*item);
  }
  EXPECT_EQ(received, (std::vector<int32_t>{2, 4, 6}));
  EXPECT_EQ(co_await result.first.co_getPrimitive(), 42);
  co_await result.first.co_accumulatePrimitive(9);
  EXPECT_EQ(co_await result.first.co_getPrimitive(), 51);
}

} // namespace
} // namespace apache::thrift
