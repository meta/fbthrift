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

#include <thrift/compiler/ast/t_enum.h>

#include <string>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <thrift/compiler/ast/t_enum_value.h>
#include <thrift/compiler/ast/t_program.h>

namespace apache::thrift::compiler {
namespace {

TEST(TEnumTest, Unused) {
  t_program program("path/to/program.thrift", "path/to/program.thrift");
  program.set_package(t_package{"test.dev/foo/bar"});

  t_enum& def = program.add_def(std::make_unique<t_enum>(&program, "Enum"));
  EXPECT_EQ(def.unused(), 113);
  def.append_value(std::make_unique<t_enum_value>("zero", 0));
  def.append_value(std::make_unique<t_enum_value>("two", 2));
  def.append_value(std::make_unique<t_enum_value>("one", 1));
  def.append_value(std::make_unique<t_enum_value>("ntwo", -2));
  def.append_value(
      std::make_unique<t_enum_value>(
          "max", std::numeric_limits<int32_t>::max()));
  EXPECT_EQ(def.unused(), 113);

  def.append_value(std::make_unique<t_enum_value>("t1", 113));
  EXPECT_EQ(def.unused(), 113 + 1);
  def.append_value(std::make_unique<t_enum_value>("t2", 113 + 2));
  def.append_value(std::make_unique<t_enum_value>("t3", 113 + 3));
  def.append_value(std::make_unique<t_enum_value>("t4", 113 + 5));
  EXPECT_EQ(def.unused(), 113 + 1);
  def.append_value(std::make_unique<t_enum_value>("t5", def.unused()));
  EXPECT_EQ(def.unused(), 113 + 4);
  def.append_value(std::make_unique<t_enum_value>("t6", def.unused()));
  EXPECT_EQ(def.unused(), 113 + 6);
  def.append_value(std::make_unique<t_enum_value>("t7", def.unused()));
  EXPECT_EQ(def.unused(), 113 + 7);
}

TEST(TEnumTest, NameLookupAfterAppend) {
  t_program program("program.thrift", "program.thrift");
  t_enum def(&program, "Enum");
  t_enum_value_list values;
  values.push_back(std::make_unique<t_enum_value>("negative", -1));
  values.push_back(std::make_unique<t_enum_value>("implicit"));
  values.push_back(std::make_unique<t_enum_value>("alias", -1));
  def.set_values(std::move(values));
  const auto* negative = def.find_const_by_name("negative");
  ASSERT_NE(negative, nullptr);
  for (int i = 0; i < 1000; ++i) {
    def.append_value(std::make_unique<t_enum_value>(std::to_string(i), i));
  }
  EXPECT_EQ(def.find_const_by_name("negative"), negative);
  EXPECT_EQ(def.find_const_by_name("implicit")->value()->get_integer(), 0);
  EXPECT_EQ(def.find_const_by_name("alias")->value()->get_integer(), -1);
  EXPECT_EQ(def.find_const_by_name("999")->value()->get_integer(), 999);
  EXPECT_EQ(def.find_const_by_name("missing"), nullptr);
  EXPECT_EQ(def.find_value(-1), &def.values().front());
}

TEST(TEnumTest, SetValuesReplacesNameLookup) {
  t_program program("program.thrift", "program.thrift");
  t_enum def(&program, "Enum");
  def.append_value(std::make_unique<t_enum_value>("old", 1));
  t_enum_value_list values;
  values.push_back(std::make_unique<t_enum_value>("new", 2));
  def.set_values(std::move(values));
  EXPECT_EQ(def.find_const_by_name("old"), nullptr);
  ASSERT_NE(def.find_const_by_name("new"), nullptr);
  EXPECT_EQ(def.find_const_by_name("new")->value()->get_integer(), 2);
}

} // namespace
} // namespace apache::thrift::compiler
