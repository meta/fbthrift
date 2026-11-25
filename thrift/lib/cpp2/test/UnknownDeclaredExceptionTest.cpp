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

// A client built from an IDL that predates a server's newly declared exception
// skips the unknown exception field in the result struct. These tests pin what
// the C++ client then reports over Rocket.

#include <gtest/gtest.h>

#include <folly/executors/InlineExecutor.h>
#include <folly/futures/Promise.h>
#include <thrift/lib/cpp/TApplicationException.h>
#include <thrift/lib/cpp2/test/gen-cpp2/NewExceptionService.h>
#include <thrift/lib/cpp2/test/gen-cpp2/OldExceptionService.h>
#include <thrift/lib/cpp2/util/ScopedServerInterfaceThread.h>

namespace apache::thrift::test {

namespace {

constexpr std::string_view kMessage = "thrown by the new server";

AddedException makeAddedException() {
  AddedException ex;
  ex.message() = kMessage;
  return ex;
}

class NewExceptionHandler : public ServiceHandler<NewExceptionService> {
 public:
  void voidMethod() override { throw makeAddedException(); }
  int32_t valueMethod() override { throw makeAddedException(); }
};

class UnknownDeclaredExceptionTest : public ::testing::Test {
 protected:
  ScopedServerInterfaceThread runner_{std::make_shared<NewExceptionHandler>()};
};

} // namespace

TEST_F(UnknownDeclaredExceptionTest, ClientThatKnowsExceptionReceivesIt) {
  auto client = runner_.newClient<Client<NewExceptionService>>();

  EXPECT_THROW(client->semifuture_voidMethod().get(), AddedException);
  EXPECT_THROW(client->semifuture_valueMethod().get(), AddedException);
}

TEST_F(UnknownDeclaredExceptionTest, VoidMethodReportsSuccess) {
  auto client = runner_.newClient<Client<OldExceptionService>>();

  EXPECT_NO_THROW(client->semifuture_voidMethod().get());
}

TEST_F(UnknownDeclaredExceptionTest, VoidMethodDropsExceptionHeaders) {
  auto client = runner_.newClient<Client<OldExceptionService>>(
      &folly::InlineExecutor::instance());
  folly::Promise<ClientReceiveState> promise;
  auto future = promise.getSemiFuture();

  client->voidMethod([&promise](ClientReceiveState&& state) {
    promise.setValue(std::move(state));
  });
  auto state = std::move(future).get();

  EXPECT_FALSE(Client<OldExceptionService>::recv_wrapped_voidMethod(state));
  ASSERT_NE(state.header(), nullptr);
  const auto& headers = state.header()->getHeaders();
  EXPECT_EQ(headers.at("uex"), "apache::thrift::test::AddedException");
  EXPECT_EQ(headers.at("uexw"), kMessage);
}

TEST_F(UnknownDeclaredExceptionTest, ValueMethodReportsMissingResult) {
  auto client = runner_.newClient<Client<OldExceptionService>>();

  try {
    client->semifuture_valueMethod().get();
    ADD_FAILURE() << "Expected TApplicationException";
  } catch (const TApplicationException& ex) {
    EXPECT_EQ(ex.getType(), TApplicationException::MISSING_RESULT);
  }
}

} // namespace apache::thrift::test
