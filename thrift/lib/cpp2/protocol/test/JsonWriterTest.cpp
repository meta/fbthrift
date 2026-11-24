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

#include <thrift/lib/cpp2/protocol/detail/JsonUtils.h>
#include <thrift/lib/cpp2/protocol/detail/JsonWriter.h>

#include <limits>
#include <gtest/gtest.h>
#include <folly/io/IOBufQueue.h>
#include <folly/json/json.h>

namespace apache::thrift::json5::detail {
namespace {

enum class JsonFormat { Json, Json5 };

class JsonWriterTest : public ::testing::TestWithParam<JsonFormat> {
 protected:
  JsonWriterTest() { w.setOutput(appender()); }

  bool isJson5() const { return GetParam() == JsonFormat::Json5; }

  JsonWriterOptions options() const {
    auto opts = isJson5() ? kJson5Options : JsonWriterOptions{};
    opts.indentWidth = 2;
    return opts;
  }

  JsonWriterOptions compactOptions() const {
    auto opts = options();
    opts.indentWidth = 0;
    return opts;
  }

  folly::io::QueueAppender appender() {
    return folly::io::QueueAppender(&queue, 4096);
  }

  std::string output() { return queue.move()->toString(); }

 public:
  folly::IOBufQueue queue;
  JsonWriter w{options()};
};

INSTANTIATE_TEST_SUITE_P(
    /*InstantiationName=*/,
    /*TestSuiteName=*/JsonWriterTest,
    /*param_generator=*/::testing::Values(JsonFormat::Json, JsonFormat::Json5),
    /*name_generator=*/[](const ::testing::TestParamInfo<JsonFormat>& info) {
      return info.param == JsonFormat::Json5 ? "Json5" : "Json";
    });

TEST_P(JsonWriterTest, Bool) {
  w.writeBool(true);
  EXPECT_EQ(output(), "true");
}

TEST_P(JsonWriterTest, StringWithEscapes) {
  w.writeString("Say \"\nHello\"!");
  EXPECT_EQ(output(), "\"Say \\\"\\nHello\\\"!\"");
}

// Strings the check accepts skip `folly::json::escapeString`, so it must copy
// them unchanged.
TEST(JsonUtilsTest, IsAsciiWithNothingToEscape) {
  for (int i = 0; i < 256; ++i) {
    const std::string s(1, static_cast<char>(i));
    if (isAsciiWithNothingToEscape(s)) {
      std::string escaped;
      folly::json::escapeString(s, escaped, {.validate_utf8 = true});
      EXPECT_EQ('"' + s + '"', escaped) << i;
    }
  }
}

TEST_P(JsonWriterTest, EmptyContainers) {
  w.writeObjectBegin();
  w.writeObjectName("empty");
  w.writeString("");
  w.writeObjectName("emptyObj");
  w.writeObjectBegin();
  w.writeObjectEnd();
  w.writeObjectName("emptyList");
  w.writeListBegin();
  w.writeListEnd();
  w.writeObjectEnd();
  if (isJson5()) {
    EXPECT_EQ(
        output(),
        R"({
  empty: "",
  emptyObj: {},
  emptyList: [],
})");
  } else {
    EXPECT_EQ(output(), R"({
  "empty": "",
  "emptyObj": {},
  "emptyList": []
})");
  }
}

// Growth 1 puts each written byte in its own buffer, so closing the outer list
// trims a comma that is alone in its buffer.
TEST_P(JsonWriterTest, CallerQueueWithOneByteBuffers) {
  folly::IOBufQueue queue;
  queue.append(folly::IOBuf::copyBuffer("head"));
  queue.append(folly::IOBuf::create(0));
  JsonWriter w(compactOptions());
  w.setOutput(folly::io::QueueAppender(&queue, 1));
  w.writeListBegin();
  w.writeI32(1);
  w.writeListBegin();
  w.writeListEnd();
  w.writeListEnd();
  EXPECT_EQ(queue.move()->toString(), isJson5() ? "head[1,[],]" : "head[1,[]]");
}

TEST_P(JsonWriterTest, ClosingAfterCallerAppendsEmptyBufferTerminates) {
  folly::IOBufQueue queue;
  JsonWriter w(compactOptions());
  w.setOutput(folly::io::QueueAppender(&queue, 16));
  w.writeListBegin();
  w.writeI32(1);
  queue.append(folly::IOBuf::create(0));
  EXPECT_DEATH(w.writeListEnd(), "last output buffer is empty");
}

TEST_P(JsonWriterTest, PrimitiveTypes) {
  std::size_t size = 0;
  size += w.writeObjectBegin();
  size += w.writeObjectName("bool");
  size += w.writeBool(false);
  size += w.writeObjectName("integer");
  size += w.writeI64(42);
  size += w.writeObjectName("$float");
  size += w.writeDouble(3.14);
  size += w.writeObjectEnd();
  const auto json = output();
  EXPECT_EQ(size, json.size());
  if (isJson5()) {
    EXPECT_EQ(json, R"({
  bool: false,
  integer: 42,
  $float: 3.14,
})");
  } else {
    EXPECT_EQ(json, R"({
  "bool": false,
  "integer": 42,
  "$float": 3.14
})");
  }
}

TEST_P(JsonWriterTest, MoreFloats) {
  std::size_t size = 0;
  size += w.writeObjectBegin();
  size += w.writeObjectName("-Zero");
  size += w.writeDouble(-0.0);
  size += w.writeObjectName("0.1");
  size += w.writeFloat(0.100000001490116119384765625F);
  size += w.writeObjectName("0.10000001");
  size += w.writeFloat(0.10000000894069671630859375F);
  size += w.writeObjectEnd();
  const auto json = output();
  EXPECT_EQ(size, json.size());
  if (isJson5()) {
    EXPECT_EQ(
        json,
        R"({
  "-Zero": -0.0,
  "0.1": 0.1,
  "0.10000001": 0.10000001,
})");
  } else {
    EXPECT_EQ(
        json,
        R"({
  "-Zero": -0.0,
  "0.1": 0.1,
  "0.10000001": 0.10000001
})");
  }
}

TEST_P(JsonWriterTest, ScientificNotationFloats) {
  std::size_t size = 0;
  size += w.writeObjectBegin();
  size += w.writeObjectName("tiny");
  size += w.writeDouble(1e-05);
  size += w.writeObjectName("big");
  size += w.writeDouble(1e20);
  size += w.writeObjectName("withFrac");
  size += w.writeDouble(1.5e-10);
  size += w.writeObjectName("whole");
  size += w.writeDouble(100000.0);
  size += w.writeObjectEnd();
  const auto json = output();
  EXPECT_EQ(size, json.size());
  if (isJson5()) {
    EXPECT_EQ(
        json,
        R"({
  tiny: 1e-05,
  big: 1e+20,
  withFrac: 1.5e-10,
  whole: 100000.0,
})");
  } else {
    EXPECT_EQ(
        json,
        R"({
  "tiny": 1e-05,
  "big": 1e+20,
  "withFrac": 1.5e-10,
  "whole": 100000.0
})");
  }
}

TEST_P(JsonWriterTest, Containers) {
  std::size_t size = 0;
  size += w.writeObjectBegin();
  size += w.writeObjectName("list");
  size += w.writeListBegin();
  size += w.writeI32(1);
  size += w.writeI32(3);
  size += w.writeI32(2);
  size += w.writeListEnd();
  size += w.writeObjectName("object");
  size += w.writeObjectBegin();
  size += w.writeObjectName("1");
  size += w.writeI32(3);
  size += w.writeObjectName("2");
  size += w.writeI32(4);
  size += w.writeObjectEnd();
  size += w.writeObjectEnd();
  const auto json = output();
  EXPECT_EQ(json.size(), size);
  if (isJson5()) {
    EXPECT_EQ(json, R"({
  list: [
    1,
    3,
    2,
  ],
  object: {
    "1": 3,
    "2": 4,
  },
})");
  } else {
    EXPECT_EQ(json, R"({
  "list": [
    1,
    3,
    2
  ],
  "object": {
    "1": 3,
    "2": 4
  }
})");
  }
}

TEST_P(JsonWriterTest, CompactList) {
  JsonWriter compact(compactOptions());
  compact.setOutput(appender());
  compact.writeListBegin();
  compact.writeI32(1);
  compact.writeI32(2);
  compact.writeListEnd();
  if (isJson5()) {
    EXPECT_EQ(output(), "[1,2,]");
  } else {
    EXPECT_EQ(output(), "[1,2]");
  }
}

TEST_P(JsonWriterTest, CompactObject) {
  JsonWriter compact(compactOptions());
  compact.setOutput(appender());
  compact.writeObjectBegin();
  compact.writeObjectName("name");
  compact.writeString("Alice");
  compact.writeObjectEnd();
  if (isJson5()) {
    EXPECT_EQ(output(), "{name:\"Alice\",}");
  } else {
    EXPECT_EQ(output(), R"({"name":"Alice"})");
  }
}

TEST_P(JsonWriterTest, ListBeginThrowsAsObjectKey) {
  w.writeObjectBegin();
  EXPECT_THROW(w.writeListBegin(), std::runtime_error);
}

TEST_P(JsonWriterTest, ObjectBeginThrowsAsObjectKey) {
  w.writeObjectBegin();
  EXPECT_THROW(w.writeObjectBegin(), std::runtime_error);
}

TEST_P(JsonWriterTest, CloseWrongContainerType) {
  w.writeListBegin();
  EXPECT_THROW(w.writeObjectEnd(), std::runtime_error);
}

TEST_P(JsonWriterTest, CloseObjectAsListThrows) {
  w.writeObjectBegin();
  w.writeObjectName("k");
  w.writeI32(1);
  EXPECT_THROW(w.writeListEnd(), std::runtime_error);
}

TEST_P(JsonWriterTest, CloseListWhenNoneOpen) {
  EXPECT_THROW(w.writeListEnd(), std::runtime_error);
}

TEST_P(JsonWriterTest, CloseObjectWhenNoneOpen) {
  EXPECT_THROW(w.writeObjectEnd(), std::runtime_error);
}

TEST_P(JsonWriterTest, WriteObjectNameInList) {
  w.writeListBegin();
  EXPECT_THROW(w.writeObjectName("k"), std::runtime_error);
}

TEST_P(JsonWriterTest, InvalidUtf8StringThrows) {
  EXPECT_THROW(w.writeString("\x80\x81\x82"), std::runtime_error);
}

TEST_P(JsonWriterTest, IntegralAsObjectKeyThrows) {
  w.writeObjectBegin();
  EXPECT_THROW(w.writeI32(1), std::runtime_error);
}

TEST_P(JsonWriterTest, BoolAsObjectKeyThrows) {
  w.writeObjectBegin();
  EXPECT_THROW(w.writeBool(true), std::runtime_error);
}

TEST_P(JsonWriterTest, FloatAsObjectKeyThrows) {
  w.writeObjectBegin();
  EXPECT_THROW(w.writeFloat(1.0F), std::runtime_error);
}

TEST_P(JsonWriterTest, DoubleAsObjectKeyThrows) {
  w.writeObjectBegin();
  EXPECT_THROW(w.writeDouble(1.0), std::runtime_error);
}

TEST_P(JsonWriterTest, WriteStringAsObjectKeyThrows) {
  w.writeObjectBegin();
  EXPECT_THROW(w.writeString("key"), std::runtime_error);
}

TEST_P(JsonWriterTest, ConsecutiveObjectNameThrows) {
  w.writeObjectBegin();
  w.writeObjectName("a");
  EXPECT_THROW(w.writeObjectName("b"), std::runtime_error);
}

TEST_P(JsonWriterTest, NanInf) {
  if (isJson5()) {
    w.writeListBegin();
    w.writeFloat(std::numeric_limits<float>::quiet_NaN());
    w.writeFloat(std::numeric_limits<float>::infinity());
    w.writeFloat(-std::numeric_limits<float>::infinity());
    w.writeDouble(std::numeric_limits<double>::quiet_NaN());
    w.writeDouble(std::numeric_limits<double>::infinity());
    w.writeDouble(-std::numeric_limits<double>::infinity());
    w.writeListEnd();
    EXPECT_EQ(
        output(),
        R"([
  NaN,
  Infinity,
  -Infinity,
  NaN,
  Infinity,
  -Infinity,
])");
  } else {
    w.writeListBegin();
    EXPECT_THROW(
        w.writeFloat(std::numeric_limits<float>::quiet_NaN()),
        std::invalid_argument);
    EXPECT_THROW(
        w.writeFloat(std::numeric_limits<float>::infinity()),
        std::invalid_argument);
    EXPECT_THROW(
        w.writeFloat(-std::numeric_limits<float>::infinity()),
        std::invalid_argument);
    EXPECT_THROW(
        w.writeDouble(std::numeric_limits<double>::quiet_NaN()),
        std::invalid_argument);
    EXPECT_THROW(
        w.writeDouble(std::numeric_limits<double>::infinity()),
        std::invalid_argument);
    EXPECT_THROW(
        w.writeDouble(-std::numeric_limits<double>::infinity()),
        std::invalid_argument);
    w.writeListEnd();
  }
}

TEST_P(JsonWriterTest, NoSetOutput) {
  JsonWriter writer(options());
  EXPECT_THROW(writer.writeObjectBegin(), std::exception);
}

} // namespace
} // namespace apache::thrift::json5::detail
