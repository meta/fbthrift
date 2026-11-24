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

#include <memory>
#include <stdexcept>
#include <string>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <thrift/compiler/ast/t_function.h>
#include <thrift/compiler/ast/t_paramlist.h>
#include <thrift/compiler/ast/t_primitive_type.h>
#include <thrift/compiler/ast/t_program.h>
#include <thrift/compiler/ast/t_program_bundle.h>
#include <thrift/compiler/ast/t_service.h>
#include <thrift/compiler/diagnostic.h>
#include <thrift/compiler/generate/t_whisker_generator.h>
#include <thrift/compiler/source_location.h>

namespace apache::thrift::compiler {
namespace {

class test_generator : public t_whisker_generator {
 public:
  using t_whisker_generator::render;
  using t_whisker_generator::t_whisker_generator;

  std::string template_prefix() const override { return "test"; }

  whisker::source_manager template_source_manager() const override {
    return whisker::source_manager{
        std::make_unique<in_memory_source_manager_backend>(
            in_memory_source_manager_backend::map{
                {"test/undefined_variable", "{{not_defined}}"},
                {"test/unclosed_tag", "{{#if true}}"},
            })};
  }

  void generate_program() override {}
};

class TWhiskerGeneratorTest : public ::testing::Test {
 protected:
  source_manager source_mgr_;
  diagnostic_results results_;
  diagnostics_engine diags_{source_mgr_, results_};
  t_program_bundle bundle_{
      std::make_unique<t_program>("test.thrift", "test.thrift")};
  test_generator generator_{*bundle_.root_program(), bundle_, diags_};
};

std::string render_error(
    test_generator& generator, std::string_view template_file) {
  try {
    generator.render(template_file, whisker::make::map());
  } catch (const std::runtime_error& e) {
    return e.what();
  }
  return "";
}

TEST_F(TWhiskerGeneratorTest, render_failure_includes_whisker_error) {
  EXPECT_THAT(
      render_error(generator_, "undefined_variable"),
      ::testing::AllOf(
          ::testing::StartsWith(
              "Failed to render template 'undefined_variable'\n[ERROR:"),
          ::testing::HasSubstr("not_defined")));
}

TEST_F(TWhiskerGeneratorTest, parse_failure_includes_whisker_error) {
  EXPECT_THAT(
      render_error(generator_, "unclosed_tag"),
      ::testing::StartsWith(
          "Failed to parse template 'unclosed_tag'\n[ERROR:"));
}

TEST(WhiskerGeneratorContextTest, param_parent_is_its_function) {
  t_program program("test.thrift", "test.thrift");
  auto params = std::make_unique<t_paramlist>();
  const t_field& param =
      params->create_field(t_primitive_type::t_string(), "name", 1);
  auto function = std::make_unique<t_function>(
      &program,
      t_type_ref::from_req_ptr(&t_primitive_type::t_void()),
      "process",
      std::move(params));
  const t_function* expected_function = function.get();
  auto service = std::make_unique<t_service>(&program, "Service");
  service->add_function(std::move(function));
  program.add_definition(std::move(service));

  whisker_generator_context context;
  t_whisker_generator::context_visitor visitor;
  context.register_visitors(visitor);
  t_whisker_generator::whisker_generator_visitor_context visitor_context;
  visitor(visitor_context, program);

  EXPECT_EQ(context.get_param_parent(&param), expected_function);
}

} // namespace
} // namespace apache::thrift::compiler
