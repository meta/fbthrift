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
#include <memory_resource>
#include <string>
#include <vector>

#include <gtest/gtest.h>
#include <thrift/lib/cpp2/util/ScopedServerInterfaceThread.h>
#include <thrift/test/gen-cpp2/CppArenaRpcTest_clients.h>
#include <thrift/test/gen-cpp2/CppArenaRpcTest_handlers.h>

namespace apache::thrift::test::arena_rpc {
namespace {

using PmrStrings = std::pmr::vector<std::pmr::string>;

class Handler : public apache::thrift::ServiceHandler<ArenaRpc> {
 public:
  void sync_process(
      std::string& _return,
      std::unique_ptr<std::pmr::string> key,
      std::unique_ptr<PmrStrings> keys,
      std::int64_t id) override {
    _return = *key;
    for (const std::pmr::string& k : *keys) {
      _return += ':';
      _return += k;
    }
    _return += ':';
    _return += std::to_string(id);
  }
};

TEST(CppArenaRpcTest, ParametersArePmr) {
  auto client = makeTestClient(std::make_shared<Handler>());
  std::string result;
  client->sync_process(
      result, std::pmr::string("key"), PmrStrings{"a", "b"}, 7);
  EXPECT_EQ(result, "key:a:b:7");
}

} // namespace
} // namespace apache::thrift::test::arena_rpc
