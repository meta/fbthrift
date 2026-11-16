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

#include <thrift/lib/cpp2/protocol/detail/JsonReader.h>

#include <cmath>
#include <gtest/gtest.h>
#include <folly/io/IOBuf.h>
#include <thrift/lib/cpp/protocol/TProtocolException.h>
#include <thrift/lib/cpp2/protocol/test/Json5Transcoder.h>
#include <thrift/test/json5/gen-cpp2/json5_reader_test_constants.h>

namespace apache::thrift::json5::detail {
namespace {

using facebook::thrift::json5::ReaderTestCase;
using namespace facebook::thrift::json5::json5_reader_test_constants;

using enum Json5Reader::FloatingPointPrecision;

class Json5ReaderTest : public ::testing::Test {
 protected:
  std::unique_ptr<folly::IOBuf> buf_;

  Json5Reader reader(std::string_view input) {
    buf_ = folly::IOBuf::copyBuffer(input);
    Json5Reader r;
    r.setCursor(folly::io::Cursor(buf_.get()));
    return r;
  }
};

class Json5ReaderSharedTest : public ::testing::TestWithParam<ReaderTestCase> {
};

TEST_P(Json5ReaderSharedTest, Read) {
  const auto& input = *GetParam().input();
  const auto& output = GetParam().output();
  if (output.has_value()) {
    EXPECT_EQ(transcodeJson5(input, {.allowNanInf = true}), *output);
  } else {
    EXPECT_THROW(
        (void)transcodeJson5(input, {.allowNanInf = true}),
        protocol::TProtocolException);
  }
}

INSTANTIATE_TEST_SUITE_P(
    Shared,
    Json5ReaderSharedTest,
    ::testing::ValuesIn(readerTestCases()),
    [](const auto& info) { return *info.param.name(); });

TEST_F(Json5ReaderTest, MoreFloats) {
  auto verify = [this](std::string_view input) {
    auto r = reader(input);
    r.readObjectBegin();

    EXPECT_EQ(r.readObjectName(), "-Zero");
    auto negZero = std::get<float>(r.readPrimitive(Single));
    EXPECT_EQ(negZero, 0.0F);
    EXPECT_TRUE(std::signbit(negZero));

    EXPECT_EQ(r.readObjectName(), "0.1");
    EXPECT_EQ(
        std::get<float>(r.readPrimitive(Single)),
        0.100000001490116119384765625F);

    EXPECT_EQ(r.readObjectName(), "0.10000001");
    EXPECT_EQ(
        std::get<float>(r.readPrimitive(Single)),
        0.10000000894069671630859375F);

    r.readObjectEnd();
  };

  // Standard JSON (keys must be quoted since they contain special chars)
  verify(R"({
  "-Zero": -0.0,
  "0.1": 0.1,
  "0.10000001": 0.10000001
})");

  // JSON5: trailing comma
  verify(R"({
  "-Zero": -0.0,
  "0.1": 0.1,
  "0.10000001": 0.10000001,
})");
}

TEST_F(Json5ReaderTest, StringSpanningBuffers) {
  constexpr std::string_view input = "\"a\\\"b\\u00e9\xc3\xa9\"";
  auto chain = folly::IOBuf::create(0);
  for (char c : input) {
    chain->appendToChain(folly::IOBuf::copyBuffer(&c, 1));
  }
  Json5Reader r;
  r.setCursor(folly::io::Cursor(chain.get()));
  EXPECT_EQ(std::get<std::string>(r.readPrimitive()), "a\"b\xc3\xa9\xc3\xa9");
}

TEST_F(Json5ReaderTest, LongStringSpanningBuffers) {
  const std::string text(40, 'a');
  auto chain = folly::IOBuf::copyBuffer("\"" + text.substr(0, 20));
  chain->appendToChain(folly::IOBuf::copyBuffer(text.substr(20) + "\""));
  Json5Reader r;
  r.setCursor(folly::io::Cursor(chain.get()));
  EXPECT_EQ(std::get<std::string>(r.readPrimitive()), text);
}

TEST_F(Json5ReaderTest, NanInf) {
  auto verify = [&]<class FloatType>(auto fp, FloatType) {
    auto r = reader("[NaN, +NaN, -NaN, Infinity, +Infinity, -Infinity]");
    r.readListBegin();

    auto positiveNan = std::get<FloatType>(r.readPrimitive(fp));
    EXPECT_TRUE(std::isnan(positiveNan));
    EXPECT_FALSE(std::signbit(positiveNan));

    auto positiveNan2 = std::get<FloatType>(r.readPrimitive(fp));
    EXPECT_TRUE(std::isnan(positiveNan2));
    EXPECT_FALSE(std::signbit(positiveNan2));

    auto negativeNan = std::get<FloatType>(r.readPrimitive(fp));
    EXPECT_TRUE(std::isnan(negativeNan));
    EXPECT_TRUE(std::signbit(negativeNan));

    EXPECT_EQ(
        std::get<FloatType>(r.readPrimitive(fp)),
        std::numeric_limits<FloatType>::infinity());

    EXPECT_EQ(
        std::get<FloatType>(r.readPrimitive(fp)),
        std::numeric_limits<FloatType>::infinity());

    EXPECT_EQ(
        std::get<FloatType>(r.readPrimitive(fp)),
        -std::numeric_limits<FloatType>::infinity());

    r.readListEnd();
  };
  verify(Single, float{});
  verify(Double, double{});
}

// Like SimpleJSON, a top-level value doesn't consume what follows it, so a
// caller can read a comma-separated stream of values.
TEST_F(Json5ReaderTest, CommaAfterTopLevelValue) {
  auto rest = [](const Json5Reader& r) {
    return folly::io::Cursor(r.getCursor())
        .readFixedString(r.getCursor().totalLength());
  };
  {
    auto r = reader("{\"a\": 1}, {}");
    r.readObjectBegin();
    EXPECT_EQ(r.readObjectName(), "a");
    EXPECT_EQ(std::get<Json5Reader::Integer>(r.readPrimitive()).value(), 1);
    r.readObjectEnd();
    EXPECT_EQ(rest(r), ", {}");
  }
  {
    auto r = reader("[1], 2");
    r.readListBegin();
    EXPECT_EQ(std::get<Json5Reader::Integer>(r.readPrimitive()).value(), 1);
    r.readListEnd();
    EXPECT_EQ(rest(r), ", 2");
  }
  {
    auto r = reader("42 /* comment */ , 43");
    EXPECT_EQ(std::get<Json5Reader::Integer>(r.readPrimitive()).value(), 42);
    EXPECT_EQ(rest(r), ", 43");
  }
}

TEST_F(Json5ReaderTest, NegativeZeroInteger) {
  for (auto input : {"-0", "-0x0"}) {
    EXPECT_EQ(
        std::get<Json5Reader::Integer>(reader(input).readPrimitive()),
        (Json5Reader::Integer{.negative = true, .magnitude = 0}))
        << input;
  }
  EXPECT_EQ(
      std::get<Json5Reader::Integer>(reader("0").readPrimitive()),
      Json5Reader::Integer{});
}

TEST_F(Json5ReaderTest, ScientificNotation) {
  auto verify = [&]<class FloatType>(auto fp, FloatType) {
    for (auto s :
         {"3.14e0", "314e-2", "0.314E+1", ".314e1", "0.00314e3", "31400E-4"}) {
      EXPECT_EQ(
          std::get<FloatType>(reader(s).readPrimitive(fp)), FloatType(3.14));
    }
  };
  verify(Single, float{});
  verify(Double, double{});
}

TEST_F(Json5ReaderTest, PeekToken) {
  using Token = Json5Reader::Token;

  auto verify = [this](std::string_view input) {
    auto r = reader(input);

    // Object begin
    EXPECT_EQ(r.peekToken(), Token::ObjectBegin);
    EXPECT_EQ(r.peekToken(), Token::ObjectBegin); // Does not consume
    r.readObjectBegin();

    // string value
    EXPECT_EQ(r.readObjectName(), "string");
    EXPECT_EQ(r.peekToken(), Token::Primitive);
    EXPECT_EQ(std::get<std::string>(r.readPrimitive()), "hello");

    // array value
    EXPECT_EQ(r.readObjectName(), "array");
    EXPECT_EQ(r.peekToken(), Token::ListBegin);
    r.readListBegin();
    EXPECT_EQ(r.peekToken(), Token::Primitive);
    EXPECT_EQ(std::get<Json5Reader::Integer>(r.readPrimitive()).value(), -1);
    EXPECT_EQ(r.peekToken(), Token::Primitive);
    EXPECT_EQ(std::get<std::string>(r.readPrimitive()), "two");
    EXPECT_EQ(r.peekToken(), Token::Primitive);
    EXPECT_FALSE(std::get<bool>(r.readPrimitive()));
    EXPECT_EQ(r.peekToken(), Token::ListEnd);
    r.readListEnd();

    // nested object value
    EXPECT_EQ(r.readObjectName(), "nested");
    EXPECT_EQ(r.peekToken(), Token::ObjectBegin);
    r.readObjectBegin();
    EXPECT_EQ(r.readObjectName(), "inner");
    EXPECT_EQ(r.peekToken(), Token::Primitive);
    EXPECT_EQ(std::get<Json5Reader::Integer>(r.readPrimitive()).value(), 99);
    EXPECT_EQ(r.peekToken(), Token::ObjectEnd);
    r.readObjectEnd();

    // End of outer object
    EXPECT_EQ(r.peekToken(), Token::ObjectEnd);
    r.readObjectEnd();
  };

  // Standard JSON
  verify(R"({
    "string": "hello",
    "array": [-1, "two", false],
    "nested": {"inner": 99}
  })");

  // JSON5
  verify(R"({
    string: 'hello', // comment
    array: [-1, "two", false,],
    nested: {inner: /* comment */ +99,},
  })");
}

TEST_F(Json5ReaderTest, PeekTokenAfterEOF) {
  auto r = reader("42");
  EXPECT_EQ(std::get<Json5Reader::Integer>(r.readPrimitive()).value(), 42);
  EXPECT_THROW((void)r.peekToken(), protocol::TProtocolException);
}

TEST_F(Json5ReaderTest, ErrorMessagesEscapeInputBytes) {
  auto message = [this](std::string_view input) -> std::string {
    try {
      (void)reader(input).readPrimitive();
    } catch (const protocol::TProtocolException& e) {
      return e.what();
    }
    return "no error";
  };
  EXPECT_NE(
      message("\xA0").find(R"(expected value, got '\xa0')"), std::string::npos);
  EXPECT_NE(
      message(
          "\"\\u\xA0"
          "000\"")
          .find(R"(invalid hex number \xa0000)"),
      std::string::npos);
  // Backslashes in messages are escaped too.
  EXPECT_NE(
      message(R"("\1")").find(R"(unknown escape '\\1')"), std::string::npos);
}

// Syntax errors are in the shared readerTestCases; these check that each read
// method rejects the token it does not expect.
TEST_F(Json5ReaderTest, UnexpectedToken) {
  EXPECT_THROW(reader("123").readObjectBegin(), protocol::TProtocolException);
  EXPECT_THROW(reader("{}").readListBegin(), protocol::TProtocolException);
  {
    auto r = reader("{\"a\": 1");
    r.readObjectBegin();
    r.readObjectName();
    r.readPrimitive();
    EXPECT_THROW(r.readObjectEnd(), protocol::TProtocolException);
  }
  {
    auto r = reader("[1, 2");
    r.readListBegin();
    r.readPrimitive();
    r.readPrimitive();
    EXPECT_THROW(r.readListEnd(), protocol::TProtocolException);
  }
  {
    auto r = reader("[[][]]");
    r.readListBegin();
    r.readListBegin();
    auto f = [&] {
      r.readListEnd();
      r.readListBegin();
    };
    EXPECT_THROW(f(), protocol::TProtocolException);
  }
  {
    Json5Reader r;
    EXPECT_THROW((void)r.getCursor(), std::exception);
  }
}

} // namespace
} // namespace apache::thrift::json5::detail
