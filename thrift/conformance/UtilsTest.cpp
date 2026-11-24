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

#include <thrift/conformance/Utils.h>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace apache::thrift::conformance {
namespace {
using ::testing::ElementsAre;
using ::testing::Pair;
using ::testing::UnorderedElementsAre;

TEST(UtilsTest, ParseNameAndCmd) {
  EXPECT_THAT(parseNameAndCmd("foo/bar"), Pair("foo", "foo/bar"));
  EXPECT_THAT(parseNameAndCmd("/foo/bar"), Pair("foo", "/foo/bar"));
  EXPECT_THAT(parseNameAndCmd("/baz/foo/bar"), Pair("foo", "/baz/foo/bar"));
  EXPECT_THAT(parseNameAndCmd("/foo/bar/baz"), Pair("bar", "/foo/bar/baz"));
  EXPECT_THAT(parseNameAndCmd("/foo/bar#"), Pair("foo", "/foo/bar"));
  EXPECT_THAT(parseNameAndCmd("/foo/bar##"), Pair("foo", "/foo/bar#"));
  EXPECT_THAT(parseNameAndCmd("/foo/bar#baz"), Pair("baz", "/foo/bar"));
  EXPECT_THAT(parseNameAndCmd("/foo/bar##baz"), Pair("baz", "/foo/bar#"));
  EXPECT_THAT(parseNameAndCmd("/foo/bar#baz#"), Pair("foo", "/foo/bar#baz"));
  EXPECT_THAT(parseNameAndCmd("/foo#bar/baz"), Pair("foo#bar", "/foo#bar/baz"));
}

TEST(UtilsTest, ParseCmds) {
  auto actual = parseCmds("/foo/bar, /foo/baz#biz ,foo#bar/baz");
  EXPECT_THAT(
      actual,
      UnorderedElementsAre(
          Pair("foo", "/foo/bar"),
          Pair("biz", "/foo/baz"),
          Pair("foo#bar", "foo#bar/baz")));
}

TEST(UtilsTest, ParseCmds_Config) {
  EXPECT_THAT(
      parseCmds("/foo/bar#rust[client=cpp,tls],/foo/baz#cpp2[tls]"),
      UnorderedElementsAre(
          Pair("rust[client=cpp,tls]", "/foo/bar"),
          Pair("cpp2[tls]", "/foo/baz")));
}

TEST(UtilsTest, ParseCmds_Dupe) {
  EXPECT_THROW(parseCmds("/foo/bar,/baz/foo/bar"), std::invalid_argument);
}

TEST(UtilsTest, Nonconforming) {
  using Line = std::optional<size_t>;
  const Nonconforming nonconforming(
      " # Comment\n"
      "go/RoundTrip/bool/true.Binary # Other comment\n"
      "java2/Compatibility/testset.binary/AddField.*\n"
      "rust[tls,client=cpp]/Rpc/Exact.Test\n"
      "rust[client=cpp,*]/Rpc/Bidi*\n"
      "*[transport=http2,*]/Rpc/Shed.Test\n"
      "rust[client=cpp]/Rpc/BidiBasic.Test\n");
  auto line = [&](std::string_view test) {
    auto entry = nonconforming.find(test);
    return entry ? Line(entry->line) : Line();
  };
  EXPECT_EQ(line("go/RoundTrip/bool/true.Binary"), Line(2));
  EXPECT_EQ(line("go/RoundTrip/bool/true.Compact"), Line());
  EXPECT_EQ(
      line("java2/Compatibility/testset.binary/AddField.Compact"), Line(3));
  EXPECT_EQ(
      line("java2/Compatibility/testset.string/AddField.Compact"), Line());
  EXPECT_EQ(line("rust[client=cpp,tls]/Rpc/Exact.Test"), Line(4));
  EXPECT_EQ(line("rust[client=cpp]/Rpc/Exact.Test"), Line());
  EXPECT_EQ(line("rust/Rpc/Exact.Test"), Line());
  EXPECT_EQ(line("rust[client=cpp]/Rpc/BidiBasic.Test"), Line(5));
  EXPECT_EQ(line("rust[client=cpp,tls]/Rpc/BidiAbort.Test"), Line(5));
  EXPECT_EQ(line("rust[client=native]/Rpc/BidiBasic.Test"), Line());
  EXPECT_EQ(line("rust/Rpc/BidiBasic.Test"), Line());
  EXPECT_EQ(line("cpp2[transport=http2]/Rpc/Shed.Test"), Line(6));
  EXPECT_EQ(line("rust[client=native,transport=http2]/Rpc/Shed.Test"), Line(6));
  EXPECT_EQ(line("cpp2[transport=header]/Rpc/Shed.Test"), Line());
  EXPECT_EQ(
      nonconforming.find("rust[client=cpp,tls]/Rpc/BidiAbort.Test")
          ->explainConforming("rust[client=cpp,tls]/Rpc/BidiAbort.Test"),
      "rust[client=cpp,tls]/Rpc/BidiAbort.Test conforms, but is named by\n"
      "    thrift/conformance/data/nonconforming.txt:5: "
      "rust[client=cpp,*]/Rpc/Bidi*\n"
      "If intentional, delete that line or narrow it so it no longer names "
      "this test (thrift/conformance/README.md shows how to list what it "
      "names).");
}

TEST(UtilsTest, Nonconforming_Stale) {
  const Nonconforming nonconforming(
      "go/RoundTrip/bool/true.Binary\n"
      "go/RoundTrip/bool/true.Json5\n"
      "go/RoundTrip/bool/*\n"
      "go/Compatibility/x.Binary\n"
      "rust[client=cpp,*]/Rpc/Bidi*\n"
      "rust[tls,client=cpp]/Rpc/X.Test\n");
  std::set<size_t> used;
  for (auto test :
       {"go/RoundTrip/bool/true.Binary",
        "go/RoundTrip/bool/true.Compact",
        "rust[client=cpp,tls]/Rpc/Y.Test"}) {
    if (auto entry = nonconforming.find(test)) {
      used.insert(entry->line);
    }
  }
  std::vector<size_t> lines;
  for (const auto& entry : nonconforming.stale(
           {"go/RoundTrip", "rust[client=cpp,tls]/Rpc"}, used)) {
    lines.push_back(entry.line);
  }
  EXPECT_THAT(lines, ElementsAre(2, 6));
}

TEST(UtilsTest, Nonconforming_Malformed) {
  EXPECT_THROW(Nonconforming("rust\n"), std::invalid_argument);
  EXPECT_THROW(
      Nonconforming("rust[client=cpp/Rpc/X.Test"), std::invalid_argument);
  EXPECT_THROW(
      Nonconforming("rust[client=cpp,]/Rpc/X.Test"), std::invalid_argument);
  EXPECT_THROW(Nonconforming("[tls]/Rpc/X.Test"), std::invalid_argument);
}

TEST(UtilsTest, ParseTestSuite) {}

} // namespace
} // namespace apache::thrift::conformance
