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

#pragma once

#include <thrift/lib/cpp/protocol/TProtocolException.h>
#include <thrift/lib/cpp2/dynamic/DynamicValue.h>
#include <thrift/lib/cpp2/dynamic/Serialization.h>
#include <thrift/lib/cpp2/protocol/BinaryProtocol.h>
#include <thrift/lib/cpp2/protocol/CompactProtocol.h>
#include <thrift/lib/cpp2/protocol/Json5Protocol.h>
#include <thrift/lib/cpp2/protocol/SimpleJSONProtocol.h>

#include <folly/Range.h>
#include <folly/io/IOBuf.h>
#include <folly/io/IOBufQueue.h>

#include <cstddef>
#include <memory_resource>
#include <string>
#include <string_view>
#include <utility>

namespace apache::thrift::dynamic {

/**
 * Serializes dynamic values with one protocol, like apache::thrift::Serializer
 * does for generated types, except that the type to read is given at runtime.
 *
 * Like apache::thrift::Serializer, deserialize reads one value from the start
 * of the input and does not require the value to end the input; `size`, if not
 * null, receives the number of bytes read.
 */
template <typename ProtocolReader, typename ProtocolWriter>
struct ValueSerializer {
  // `writerArgs` go to the writer's constructor, e.g.
  // `COPY_EXTERNAL_BUFFER, options` for Json5ProtocolWriter.
  template <typename... WriterArgs>
  static void serialize(
      const DynamicConstRef& value,
      folly::IOBufQueue* out,
      WriterArgs&&... writerArgs) {
    ProtocolWriter writer(std::forward<WriterArgs>(writerArgs)...);
    writer.setOutput(out);
    serializeValue(writer, value);
  }

  // Appends to `out`.
  template <typename... WriterArgs>
  static void serialize(
      const DynamicConstRef& value,
      std::string* out,
      WriterArgs&&... writerArgs) {
    folly::IOBufQueue queue(folly::IOBufQueue::cacheChainLength());
    serialize(value, &queue, std::forward<WriterArgs>(writerArgs)...);
    queue.appendToString(*out);
  }

  // `R` is std::string or folly::IOBufQueue.
  template <typename R, typename... WriterArgs>
  static R serialize(const DynamicConstRef& value, WriterArgs&&... writerArgs) {
    R ret;
    serialize(value, &ret, std::forward<WriterArgs>(writerArgs)...);
    return ret;
  }

  static DynamicValue deserialize(
      const folly::IOBuf* buf,
      const type_system::TypeRef& type,
      size_t* size = nullptr,
      std::pmr::memory_resource* mr = nullptr) {
    ProtocolReader reader;
    reader.setInput(buf);
    auto value = deserializeValue(reader, type, mr);
    if (size != nullptr) {
      *size = reader.getCursor().getCurrentPosition();
    }
    return value;
  }

  static DynamicValue deserialize(
      folly::ByteRange range,
      const type_system::TypeRef& type,
      size_t* size = nullptr,
      std::pmr::memory_resource* mr = nullptr) {
    const folly::IOBuf buf(folly::IOBuf::WRAP_BUFFER, range);
    return deserialize(&buf, type, size, mr);
  }

  static DynamicValue deserialize(
      folly::StringPiece range,
      const type_system::TypeRef& type,
      size_t* size = nullptr,
      std::pmr::memory_resource* mr = nullptr) {
    return deserialize(folly::ByteRange(range), type, size, mr);
  }
};

using CompactValueSerializer =
    ValueSerializer<CompactProtocolReader, CompactProtocolWriter>;
using BinaryValueSerializer =
    ValueSerializer<BinaryProtocolReader, BinaryProtocolWriter>;
using SimpleJSONValueSerializer =
    ValueSerializer<SimpleJSONProtocolReader, SimpleJSONProtocolWriter>;
using Json5ValueSerializer = ValueSerializer<
    json5::detail::Json5ProtocolReader,
    json5::detail::Json5ProtocolWriter>;

// Like Json5ProtocolUtils::toBasicJson: JSON that standard parsers read, with
// NaN and infinities as quoted strings.
inline std::string toBasicJson(const DynamicConstRef& value) {
  return Json5ValueSerializer::serialize<std::string>(
      value,
      COPY_EXTERNAL_BUFFER,
      json5::detail::Json5ProtocolWriter::Options{
          .writer = {.indentWidth = 2}});
}

// Like Json5ProtocolUtils::toJson5.
inline std::string toJson5(const DynamicConstRef& value) {
  auto writer = json5::detail::kJson5Options;
  writer.indentWidth = 2;
  return Json5ValueSerializer::serialize<std::string>(
      value,
      COPY_EXTERNAL_BUFFER,
      json5::detail::Json5ProtocolWriter::Options{.writer = writer});
}

// Like Json5ProtocolUtils::fromJson5: reads JSON or JSON5, and unlike
// Json5ValueSerializer::deserialize, rejects content after the value.
inline DynamicValue fromJson5(
    std::string_view json,
    const type_system::TypeRef& type,
    std::pmr::memory_resource* mr = nullptr) {
  size_t size = 0;
  auto value = Json5ValueSerializer::deserialize(
      folly::StringPiece(json), type, &size, mr);
  if (size != json.size()) {
    throw protocol::TProtocolException(
        protocol::TProtocolException::INVALID_DATA,
        "Json5ProtocolReader: unexpected trailing content after JSON value");
  }
  return value;
}

} // namespace apache::thrift::dynamic
