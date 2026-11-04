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

#include <gtest/gtest.h>

#include <thrift/compiler/ast/t_const.h>
#include <thrift/compiler/ast/t_enum_value.h>
#include <thrift/compiler/ast/t_exception.h>
#include <thrift/compiler/ast/t_field.h>
#include <thrift/compiler/ast/t_function.h>
#include <thrift/compiler/ast/t_interaction.h>
#include <thrift/compiler/ast/t_paramlist.h>
#include <thrift/compiler/ast/t_primitive_type.h>
#include <thrift/compiler/ast/t_program.h>
#include <thrift/compiler/ast/t_sink.h>
#include <thrift/compiler/ast/t_stream.h>
#include <thrift/compiler/ast/t_struct.h>
#include <thrift/compiler/ast/t_throws.h>
#include <thrift/compiler/ast/t_union.h>
#include <thrift/compiler/generate/t_generator.h>

namespace apache::thrift::compiler {

TEST(TGeneratorTest, AppliesCStyleDocblock) {
  EXPECT_EQ(
      apply_docblock_style(
          R"(first line

third line)",
          docblock_style::c_style()),
      R"(/**
 * first line
 *
 * third line
 */
)");
}

TEST(TGeneratorTest, TrimsTrailingWhitespace) {
  EXPECT_EQ(
      apply_docblock_style(
          "first line \t\n\nthird line", docblock_style::hash_style()),
      R"(# first line
#
# third line
)");
}

TEST(TGeneratorTest, AppliesPythonDocblock) {
  EXPECT_EQ(
      apply_docblock_style(
          R"(first line
second line)",
          docblock_style::python_style()),
      R"(r"""
first line
second line
"""
)");
}

TEST(TGeneratorTest, AppliesTripleSlashDocblock) {
  EXPECT_EQ(
      apply_docblock_style(
          R"(first line
second line)",
          docblock_style::triple_slash_style()),
      R"(/// first line
/// second line
)");
}

TEST(TGeneratorTest, AppliesHashDocblock) {
  EXPECT_EQ(
      apply_docblock_style(
          R"(first line
second line)",
          docblock_style::hash_style()),
      R"(# first line
# second line
)");
}

TEST(TGeneratorTest, EscapesTerminator) {
  EXPECT_EQ(
      apply_docblock_style("ends */ early", docblock_style::c_style()),
      R"(/**
 * ends *\/ early
 */
)");
  EXPECT_EQ(
      apply_docblock_style(
          R"(four """" quotes)", docblock_style::python_style()),
      R"(r"""
four ""\"" quotes
"""
)");
}

TEST(TGeneratorTest, EscapesNestedCommentOpener) {
  EXPECT_EQ(
      apply_docblock_style("see /* this */", docblock_style::c_style()),
      R"(/**
 * see /\* this *\/
 */
)");
  EXPECT_EQ(
      apply_docblock_style("matches src/*/*.h", docblock_style::c_style()),
      R"(/**
 * matches src/\*\/\*.h
 */
)");
}

TEST(TGeneratorTest, AppliesStyleToEmptyText) {
  EXPECT_EQ(
      apply_docblock_style("", docblock_style::c_style()),
      R"(/**
 */
)");
}

TEST(TGeneratorTest, CompactsOneLineDocblock) {
  EXPECT_EQ(
      apply_compact_docblock_style("one line", docblock_style::c_style()),
      R"(/** one line */
)");
  EXPECT_EQ(
      apply_compact_docblock_style("one line", docblock_style::python_style()),
      R"(r"""one line"""
)");
  EXPECT_EQ(
      apply_compact_docblock_style(
          R"(first line
second line)",
          docblock_style::c_style()),
      R"(/**
 * first line
 * second line
 */
)");
}

TEST(TGeneratorTest, CompactFormEscapesTerminator) {
  EXPECT_EQ(
      apply_compact_docblock_style("ends */ early", docblock_style::c_style()),
      R"(/** ends *\/ early */
)");
}

TEST(TGeneratorTest, CompactFormAvoidsRunningIntoClosing) {
  EXPECT_EQ(
      apply_compact_docblock_style(
          R"(say "hi")", docblock_style::python_style()),
      R"(r"""
say "hi"
"""
)");
  EXPECT_EQ(
      apply_compact_docblock_style(R"(path\)", docblock_style::python_style()),
      R"(r"""
path\
"""
)");
}

TEST(TGeneratorTest, DocblockTextAddsDefinitionOnRequest) {
  t_program program("path/to/program.thrift", "/root/path/to/program.thrift");
  t_struct tstruct(&program, "Foo");
  EXPECT_EQ(docblock_text(tstruct, program, false), "");
  EXPECT_EQ(docblock_text(tstruct, program, true), "struct Foo");

  tstruct.set_doc("Struct doc.\n", source_range{});
  EXPECT_EQ(docblock_text(tstruct, program, false), "Struct doc.");
  EXPECT_EQ(
      docblock_text(tstruct, program, true),
      R"(Struct doc.

struct Foo)");
}

TEST(TGeneratorTest, ThriftDefinitionOfFunction) {
  t_program program("path/to/program.thrift", "/root/path/to/program.thrift");
  t_program other("path/to/other.thrift", "/root/path/to/other.thrift");
  t_exception bang(&other, "Bang");
  auto params = std::make_unique<t_paramlist>();
  params->create_field(t_primitive_type::t_i32(), "a", 1);
  params->create_field(t_primitive_type::t_string(), "b", 2);
  auto exceptions = std::make_unique<t_throws>();
  exceptions->create_field(bang, "bang", 1);
  t_function function(
      &program,
      t_type_ref::from_req_ptr(&t_primitive_type::t_string()),
      "thing",
      std::move(params));
  function.set_exceptions(std::move(exceptions));

  EXPECT_EQ(
      thrift_definition(function, program),
      "string thing(1: i32 a, 2: string b) throws (1: other.Bang bang)");
}

TEST(TGeneratorTest, ThriftDefinitionNamesKind) {
  t_program program("path/to/program.thrift", "/root/path/to/program.thrift");
  EXPECT_EQ(thrift_definition(t_struct(&program, "S"), program), "struct S");
  EXPECT_EQ(thrift_definition(t_union(&program, "U"), program), "union U");
  EXPECT_EQ(
      thrift_definition(t_exception(&program, "E"), program), "exception E");
  EXPECT_EQ(thrift_definition(t_enum_value("VALUE", 1), program), "VALUE = 1");
}

TEST(TGeneratorTest, ThriftDefinitionQualifiesOnlyOtherPrograms) {
  t_program program("path/to/program.thrift", "/root/path/to/program.thrift");
  t_program other("path/to/other.thrift", "/root/path/to/other.thrift");
  t_struct local(&program, "Local");
  t_struct remote(&other, "Remote");
  EXPECT_EQ(
      thrift_definition(t_field(local, "local", 1), program), "1: Local local");
  EXPECT_EQ(
      thrift_definition(t_field(remote, "remote", 2), program),
      "2: other.Remote remote");
  EXPECT_EQ(
      thrift_definition(t_const(&program, remote, "kRemote", nullptr), program),
      "const other.Remote kRemote");
}

TEST(TGeneratorTest, ThriftDefinitionOfFieldQualifier) {
  t_program program("path/to/program.thrift", "/root/path/to/program.thrift");
  t_field field(t_primitive_type::t_i32(), "foo", 1);
  field.set_qualifier(t_field_qualifier::optional);
  EXPECT_EQ(thrift_definition(field, program), "1: optional i32 foo");
}

TEST(TGeneratorTest, ThriftDefinitionOfStreamingFunctions) {
  t_program program("path/to/program.thrift", "/root/path/to/program.thrift");
  const auto i32_type = t_type_ref::from_req_ptr(&t_primitive_type::t_i32());
  const auto string_type =
      t_type_ref::from_req_ptr(&t_primitive_type::t_string());

  t_function sink(
      &program,
      i32_type,
      "upload",
      std::make_unique<t_paramlist>(),
      std::make_unique<t_sink>(i32_type, string_type));
  EXPECT_EQ(
      thrift_definition(sink, program), "i32, sink<i32, string> upload()");

  t_function bidi(
      &program,
      t_type_ref::from_req_ptr(&t_primitive_type::t_void()),
      "chat",
      std::make_unique<t_paramlist>(),
      std::make_unique<t_sink>(i32_type, t_type_ref()),
      std::make_unique<t_stream>(string_type));
  EXPECT_EQ(
      thrift_definition(bidi, program), "sink<i32>, stream<string> chat()");
}

TEST(TGeneratorTest, ThriftDefinitionOfStreamingExceptions) {
  t_program program("path/to/program.thrift", "/root/path/to/program.thrift");
  t_exception bang(&program, "Bang");
  auto throws_bang = [&] {
    auto exceptions = std::make_unique<t_throws>();
    exceptions->create_field(bang, "bang", 1);
    return exceptions;
  };
  const auto void_type = t_type_ref::from_req_ptr(&t_primitive_type::t_void());
  const auto i32_type = t_type_ref::from_req_ptr(&t_primitive_type::t_i32());
  const auto string_type =
      t_type_ref::from_req_ptr(&t_primitive_type::t_string());

  auto stream = std::make_unique<t_stream>(i32_type);
  stream->set_exceptions(throws_bang());
  t_function streaming(
      &program,
      void_type,
      "numbers",
      std::make_unique<t_paramlist>(),
      nullptr,
      std::move(stream));
  EXPECT_EQ(
      thrift_definition(streaming, program),
      "stream<i32 throws (1: Bang bang)> numbers()");

  auto sink = std::make_unique<t_sink>(i32_type, string_type);
  sink->set_sink_exceptions(throws_bang());
  sink->set_final_response_exceptions(throws_bang());
  t_function sinking(
      &program,
      void_type,
      "upload",
      std::make_unique<t_paramlist>(),
      std::move(sink));
  EXPECT_EQ(
      thrift_definition(sinking, program),
      "sink<i32 throws (1: Bang bang), string throws (1: Bang bang)> upload()");
}

TEST(TGeneratorTest, ThriftDefinitionOfOnewayFunction) {
  t_program program("path/to/program.thrift", "/root/path/to/program.thrift");
  t_function function(
      &program, t_type_ref::from_req_ptr(&t_primitive_type::t_void()), "ping");
  function.set_qualifier(t_function_qualifier::oneway);
  EXPECT_EQ(thrift_definition(function, program), "oneway void ping()");
}

TEST(TGeneratorTest, ThriftDefinitionOfInteractionResponses) {
  t_program program("path/to/program.thrift", "/root/path/to/program.thrift");
  t_interaction interaction(&program, "Session");
  const auto session = t_type_ref::from_req_ptr(&interaction);
  const auto void_type = t_type_ref::from_req_ptr(&t_primitive_type::t_void());
  const auto i32_type = t_type_ref::from_req_ptr(&t_primitive_type::t_i32());

  t_function open(
      &program,
      void_type,
      "open",
      std::make_unique<t_paramlist>(),
      nullptr,
      nullptr,
      session);
  EXPECT_EQ(thrift_definition(open, program), "Session open()");

  t_function start(
      &program,
      i32_type,
      "start",
      std::make_unique<t_paramlist>(),
      nullptr,
      nullptr,
      session);
  EXPECT_EQ(thrift_definition(start, program), "Session, i32 start()");

  t_function watch(
      &program,
      void_type,
      "watch",
      std::make_unique<t_paramlist>(),
      nullptr,
      std::make_unique<t_stream>(i32_type),
      session);
  EXPECT_EQ(thrift_definition(watch, program), "Session, stream<i32> watch()");
}

} // namespace apache::thrift::compiler
