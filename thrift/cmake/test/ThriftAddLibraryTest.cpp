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

#include <gtest/gtest.h>
#include <folly/init/Init.h>

#include <thrift/cmake/test/gen-cpp2/Derived.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>
#include <thrift/lib/cpp2/util/ScopedServerInterfaceThread.h>

namespace thrift::cmake::test {

class DerivedHandler : public apache::thrift::ServiceHandler<Derived> {
 public:
  void echo(Base& result, std::unique_ptr<Base> base) override {
    result = *base;
  }

  void wrap(Wrapper& result, std::unique_ptr<Base> base) override {
    result.base() = *base;
  }
};

TEST(ThriftAddLibraryTest, Serialization) {
  Wrapper wrapper;
  wrapper.base()->value() = 42;
  wrapper.base()->count() = 7;
  auto data =
      apache::thrift::CompactSerializer::serialize<std::string>(wrapper);
  EXPECT_EQ(
      apache::thrift::CompactSerializer::deserialize<Wrapper>(data), wrapper);
}

TEST(ThriftAddLibraryTest, Service) {
  auto client = apache::thrift::makeTestClient<apache::thrift::Client<Derived>>(
      std::make_shared<DerivedHandler>());
  Base base;
  base.value() = 3;
  Base echoed;
  client->sync_echo(echoed, base);
  EXPECT_EQ(echoed, base);
  Wrapper wrapper;
  client->sync_wrap(wrapper, base);
  EXPECT_EQ(*wrapper.base(), base);
}

} // namespace thrift::cmake::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv);
  return RUN_ALL_TESTS();
}
