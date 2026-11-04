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

} // namespace apache::thrift::compiler
