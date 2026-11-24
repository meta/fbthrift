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

#include <thrift/lib/cpp2/util/ManagedStringView.h>

#include <gtest/gtest.h>
#include <folly/io/IOBufQueue.h>
#include <thrift/lib/cpp2/StringTypeAdapter.h>
#include <thrift/lib/cpp2/op/Encode.h>
#include <thrift/lib/cpp2/protocol/BinaryProtocol.h>
#include <thrift/lib/cpp2/protocol/CompactProtocol.h>

using namespace apache::thrift;

TEST(ManagedStringViewTest, Basic) {
  static std::string s1 = "foo";
  static folly::StringPiece s2 = "bar";
  std::string_view sp1{s1}, sp2{s2};

  ManagedStringView s = ManagedStringView::from_static(sp1);
  EXPECT_EQ(s.str(), "foo");
  s1 = "qux";
  EXPECT_EQ(s.str(), "qux");
  EXPECT_EQ(s.view(), "qux");

  s = ManagedStringView::from_static(sp2);
  EXPECT_EQ(s.str(), "bar");

  {
    std::string s3 = "baz";
    s = ManagedStringView(s3);
  }
  EXPECT_EQ(s.str(), "baz");
  EXPECT_EQ(s.view(), "baz");

  {
    folly::StringPiece s4 = "myverylongstringthatwon'tfitinsso";
    s = ManagedStringView(s4);
  }
  EXPECT_EQ(std::move(s).str(), "myverylongstringthatwon'tfitinsso");
  EXPECT_EQ(std::move(s).str(), "");

  {
    const char* s5 = "qux";
    s = ManagedStringView(s5);
  }
  EXPECT_EQ(s.str(), "qux");
}

TEST(ManagedStringViewTest, AdapterConversionOwnsStorage) {
  using Adapter = StringTypeAdapter<ManagedStringViewWithConversions>;
  const std::string expected(256, 'x');
  ManagedStringViewWithConversions adapted;
  {
    auto source = expected;
    const auto* storage = source.data();
    adapted = Adapter::fromThrift(std::move(source));
    EXPECT_EQ(adapted.data(), storage);
  }
  EXPECT_EQ(adapted.view(), expected);
}

template <typename Reader, typename Writer>
void testAdaptedFragmentedString() {
  using Adapter = StringTypeAdapter<ManagedStringViewWithConversions>;
  using Tag = type::adapted<Adapter, type::string_t>;
  const std::string expected(256, 'x');
  auto adapted = ManagedStringViewWithConversions(
      ManagedStringView::from_static(std::string_view("static")));
  {
    folly::IOBufQueue queue;
    Writer writer;
    writer.setOutput(&queue);
    writer.writeString(expected);
    auto contiguous = queue.move()->cloneCoalesced();
    auto encoded = folly::IOBuf::copyBuffer(contiguous->data(), 6);
    encoded->appendChain(
        folly::IOBuf::copyBuffer(
            contiguous->data() + 6, contiguous->length() - 6));
    Reader reader;
    reader.setInput(encoded.get());
    op::decode<Tag>(reader, adapted);
  }
  EXPECT_EQ(adapted.view(), expected);
}

TEST(ManagedStringViewTest, AdapterDecodeFragmentedStaticView) {
  testAdaptedFragmentedString<BinaryProtocolReader, BinaryProtocolWriter>();
  testAdaptedFragmentedString<CompactProtocolReader, CompactProtocolWriter>();
}

TEST(ManagedStringViewTest, Conversions) {
  std::string src = "foo";

  ManagedStringView s(std::move(src));
  EXPECT_EQ(s.size(), 3);

  auto wc = ManagedStringViewWithConversions(std::move(s));
  // Expect to be moved to `wc`.
  EXPECT_EQ(s.size(), 0);
  EXPECT_EQ(wc.size(), 3);

  ManagedStringView s2(std::move(wc));

  // Expect to be moved to `wc`.
  EXPECT_EQ(s.size(), 0);
  // Expected to be moved to s2.
  EXPECT_EQ(wc.size(), 0);
  EXPECT_EQ(s2.size(), 3);

  static_assert(!std::is_convertible_v<ManagedStringView, std::string_view>);
  static_assert(!std::is_convertible_v<ManagedStringView, std::string>);
}
