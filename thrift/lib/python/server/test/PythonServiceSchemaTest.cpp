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

#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <fmt/core.h>
#include <gtest/gtest.h>

#include <thrift/lib/cpp2/Flags.h>
#include <thrift/lib/cpp2/Thrift.h>
#include <thrift/lib/cpp2/schema/detail/Merge.h>
#include <thrift/lib/python/server/PythonServiceSchema.h>
#include <thrift/lib/python/server/test/gen-cpp2/python_service_schema_test_types.h>

THRIFT_FLAG_DECLARE_bool(python_server_populate_service_schema);

namespace apache::thrift::python {
namespace {

// The generated schema bundles for the test program and its transitive
// includes, in the same encoding and order that a generated thrift-python
// module hands to `PythonAsyncProcessorFactory`.
std::vector<std::string> testSchemaBundles() {
  const auto bundles = detail::
      TSchemaAssociation<apache::thrift::python::test::Anchor, false>::bundle();
  return std::vector<std::string>(bundles.begin(), bundles.end());
}

// `testSchemaBundles()` without the bundle of the program at a path ending in
// `pathSuffix`.
std::vector<std::string> testSchemaBundlesWithout(std::string_view pathSuffix) {
  std::vector<std::string> bundles;
  for (auto& bundle : testSchemaBundles()) {
    const auto schema = schema::detail::readSchema(bundle);
    if (schema && !schema->programs()->empty() &&
        schema->programs()->front().path()->ends_with(pathSuffix)) {
      continue;
    }
    bundles.push_back(std::move(bundle));
  }
  return bundles;
}

// The definition key of the service named `name` in the program at a path
// ending in `pathSuffix`, read off that program's own bundle.
std::string serviceKey(std::string_view pathSuffix, std::string_view name) {
  for (const auto& bundle : testSchemaBundles()) {
    const auto schema = schema::detail::readSchema(bundle);
    if (!schema) {
      continue;
    }
    for (const auto& program : *schema->programs()) {
      if (!program.path()->ends_with(pathSuffix)) {
        continue;
      }
      for (const auto& key : *program.definitionKeys()) {
        // Not every key a bundle lists is defined in that bundle.
        const auto it = schema->definitionsMap()->find(key);
        if (it == schema->definitionsMap()->end()) {
          continue;
        }
        const auto& definition = it->second;
        if (definition.serviceDef_ref().has_value() &&
            *definition.serviceDef_ref()->attrs()->name() == name) {
          return std::string(key.data(), key.size());
        }
      }
    }
  }
  throw std::out_of_range(fmt::format("no service {} in {}", name, pathSuffix));
}

constexpr std::string_view kTestProgram = "/python_service_schema_test.thrift";

// Builds the schema of the service named `name` in the test program.
PythonServiceSchema buildTestService(
    std::string_view name,
    const std::vector<std::string>& bundles = testSchemaBundles()) {
  return PythonServiceSchema::build(
      bundles, name, serviceKey(kTestProgram, name));
}

} // namespace

TEST(PythonServiceSchemaTest, ResolvesAServiceMethodAndItsParameters) {
  const auto schema = buildTestService("TestService");

  ASSERT_NE(schema.service(), nullptr);
  EXPECT_EQ(schema.service()->definition().name(), "TestService");

  const auto* describe = schema.function("describe");
  ASSERT_NE(describe, nullptr);
  EXPECT_EQ(describe->name(), "describe");

  // The parameter names and types are what an interceptor reads arguments by.
  const auto params = describe->params();
  ASSERT_EQ(params.size(), 2);
  EXPECT_EQ(params[0].name(), "point");
  // Resolving this one needs the included program's bundle, not just the
  // service's own.
  EXPECT_EQ(params[0].type().asStruct().definition().name(), "Point");
  EXPECT_EQ(params[1].name(), "label");
  EXPECT_TRUE(params[1].type().isPrimitive());
}

// Another program in the same bundles defines a `TestService` too, whose
// `describe` takes other parameters; the definition key picks between them.
TEST(PythonServiceSchemaTest, SelectsTheServiceByItsDefinitionKey) {
  const auto ours = buildTestService("TestService");
  const auto theirs = PythonServiceSchema::build(
      testSchemaBundles(),
      "TestService",
      serviceKey(
          "/python_service_schema_test_same_name.thrift", "TestService"));

  const auto* describe = ours.function("describe");
  ASSERT_NE(describe, nullptr);
  ASSERT_EQ(describe->params().size(), 2);
  EXPECT_EQ(describe->params()[0].name(), "point");
  const auto* otherDescribe = theirs.function("describe");
  ASSERT_NE(otherDescribe, nullptr);
  ASSERT_EQ(otherDescribe->params().size(), 1);
  EXPECT_EQ(otherDescribe->params()[0].name(), "other");
}

TEST(PythonServiceSchemaTest, ResolvesAnInheritedMethod) {
  const auto schema = buildTestService("TestService");

  // `ping` is defined on BaseService. The generated Python handler table
  // carries it under its bare name, so the schema must too.
  const auto* ping = schema.function("ping");
  ASSERT_NE(ping, nullptr);
  EXPECT_EQ(ping->name(), "ping");
  ASSERT_EQ(ping->params().size(), 1);
  EXPECT_EQ(ping->params()[0].name(), "seq");
}

TEST(PythonServiceSchemaTest, ResolvesAnInteractionMethodByQualifiedName) {
  const auto schema = buildTestService("TestService");

  // The server dispatches an interaction's method as `Interaction.method`.
  const auto* echo = schema.function("Session.echo");
  ASSERT_NE(echo, nullptr);
  EXPECT_EQ(echo->name(), "echo");
  ASSERT_EQ(echo->params().size(), 1);
  EXPECT_EQ(echo->params()[0].name(), "text");

  // The bare name belongs to no service method.
  EXPECT_EQ(schema.function("echo"), nullptr);
}

TEST(PythonServiceSchemaTest, ResolvesTheBaseServiceOnItsOwn) {
  const auto schema = buildTestService("BaseService");

  ASSERT_NE(schema.service(), nullptr);
  EXPECT_EQ(schema.service()->definition().name(), "BaseService");
  EXPECT_NE(schema.function("ping"), nullptr);
  // A derived service's method is not reachable from its base.
  EXPECT_EQ(schema.function("describe"), nullptr);
}

TEST(PythonServiceSchemaTest, ResolvesABaseServiceFromAnotherProgram) {
  const auto schema = buildTestService("ExtendsRemoteBase");

  ASSERT_NE(schema.service(), nullptr);
  EXPECT_NE(schema.function("noop"), nullptr);
  EXPECT_NE(schema.function("remotePing"), nullptr);
}

// A server must still start when the base service's bundle is missing.
TEST(PythonServiceSchemaTest, IsEmptyWhenTheBaseServiceIsMissing) {
  const auto bundles = testSchemaBundlesWithout("_base.thrift");
  ASSERT_LT(bundles.size(), testSchemaBundles().size());

  const auto schema = buildTestService("ExtendsRemoteBase", bundles);

  EXPECT_EQ(schema.service(), nullptr);
  EXPECT_EQ(schema.function("noop"), nullptr);
}

TEST(PythonServiceSchemaTest, HasNoFunctionForAnUnknownMethod) {
  const auto schema = buildTestService("TestService");

  EXPECT_EQ(schema.function("noSuchMethod"), nullptr);
  EXPECT_EQ(schema.function(""), nullptr);
}

// A Thrift library built without a bundled schema: the service still serves,
// interceptors just see no arguments.
TEST(PythonServiceSchemaTest, IsEmptyWithoutBundles) {
  const auto schema = PythonServiceSchema::build({}, "TestService", "");

  EXPECT_EQ(schema.service(), nullptr);
  EXPECT_EQ(schema.function("describe"), nullptr);
}

TEST(PythonServiceSchemaTest, IsEmptyForAServiceOutsideTheBundles) {
  const auto schema = PythonServiceSchema::build(
      testSchemaBundles(), "NotAServiceHere", "not a definition key");

  EXPECT_EQ(schema.service(), nullptr);
  EXPECT_EQ(schema.function("describe"), nullptr);
}

// An unreadable bundle must not take the server down at startup.
TEST(PythonServiceSchemaTest, IsEmptyForUnreadableBundles) {
  const std::vector<std::string> garbage = {"not a compressed schema"};

  const auto schema = PythonServiceSchema::build(garbage, "TestService", "");

  EXPECT_EQ(schema.service(), nullptr);
  EXPECT_EQ(schema.function("describe"), nullptr);
}

// The kill switch, which has to hold even for a service whose bundles are
// perfectly good.
TEST(PythonServiceSchemaTest, IsEmptyWhenTheFlagIsOff) {
  THRIFT_FLAG_SET_MOCK(python_server_populate_service_schema, false);

  const auto schema = buildTestService("TestService");

  EXPECT_EQ(schema.service(), nullptr);
  EXPECT_EQ(schema.function("describe"), nullptr);

  THRIFT_FLAG_UNMOCK(python_server_populate_service_schema);

  // The same bundles resolve once the flag is back on, so the empty schema
  // above is the flag's doing and not a broken fixture.
  EXPECT_NE(buildTestService("TestService").service(), nullptr);
}

TEST(PythonServiceSchemaTest, DefaultIsEmpty) {
  const PythonServiceSchema schema;

  EXPECT_EQ(schema.service(), nullptr);
  EXPECT_EQ(schema.function("describe"), nullptr);
}

} // namespace apache::thrift::python
