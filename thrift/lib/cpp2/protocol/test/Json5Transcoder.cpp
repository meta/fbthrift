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

#include <thrift/lib/cpp2/protocol/test/Json5Transcoder.h>

#include <cstdint>
#include <variant>

#include <folly/Overload.h>
#include <folly/io/IOBuf.h>
#include <folly/io/IOBufQueue.h>
#include <thrift/lib/cpp/protocol/TProtocolException.h>
#include <thrift/lib/cpp2/protocol/detail/JsonReader.h>

namespace apache::thrift::json5::detail {

// Gives the transcoder access to JsonWriter's private writeNull() (the
// Attorney-Client idiom). JsonWriter befriends only this class, so it cannot
// be in the anonymous namespace.
class JsonWriterAttorney {
 public:
  JsonWriterAttorney() = delete;

  static std::uint32_t writeNull(JsonWriter& writer) {
    return writer.writeNull();
  }
};

namespace {

void writePrimitive(JsonWriter& writer, const Json5Reader::Primitive& value) {
  std::visit(
      folly::overload(
          [&](std::monostate) { JsonWriterAttorney::writeNull(writer); },
          [&](bool b) { writer.writeBool(b); },
          [&](Json5Reader::Integer i) { writer.writeI64(i.value()); },
          [&](float f) { writer.writeFloat(f); },
          [&](double d) { writer.writeDouble(d); },
          [&](const std::string& s) { writer.writeString(s); }),
      value);
}

void transcodeValue(Json5Reader& reader, JsonWriter& writer) {
  switch (reader.peekToken()) {
    case Json5Reader::Token::ListBegin:
      reader.readListBegin();
      writer.writeListBegin();
      while (reader.peekToken() != Json5Reader::Token::ListEnd) {
        transcodeValue(reader, writer);
      }
      reader.readListEnd();
      writer.writeListEnd();
      return;
    case Json5Reader::Token::ObjectBegin:
      reader.readObjectBegin();
      writer.writeObjectBegin();
      while (reader.peekToken() != Json5Reader::Token::ObjectEnd) {
        writer.writeObjectName(reader.readObjectName());
        transcodeValue(reader, writer);
      }
      reader.readObjectEnd();
      writer.writeObjectEnd();
      return;
    case Json5Reader::Token::Primitive:
      writePrimitive(writer, reader.readPrimitive());
      return;
    case Json5Reader::Token::ListEnd:
    case Json5Reader::Token::ObjectEnd:
      throw protocol::TProtocolException(
          protocol::TProtocolException::INVALID_DATA,
          "unexpected closing delimiter");
  }
}

} // namespace

std::string transcodeJson5(std::string_view input, JsonWriterOptions options) {
  auto buf = folly::IOBuf::wrapBufferAsValue(input.data(), input.size());
  Json5Reader reader;
  reader.setCursor(folly::io::Cursor(&buf));
  folly::IOBufQueue queue;
  JsonWriter writer(options);
  writer.setOutput(folly::io::QueueAppender(&queue, 1024));
  transcodeValue(reader, writer);
  if (!reader.getCursor().isAtEnd()) {
    throw protocol::TProtocolException(
        protocol::TProtocolException::INVALID_DATA,
        "unexpected trailing content after JSON value");
  }
  return queue.moveAsValue().toString();
}

} // namespace apache::thrift::json5::detail
