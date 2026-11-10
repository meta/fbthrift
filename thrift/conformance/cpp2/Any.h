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

#include <type_traits>

#include <thrift/conformance/cpp2/Protocol.h>
#include <thrift/conformance/cpp2/ThriftTypeInfo.h>
#include <thrift/conformance/if/gen-cpp2/any_types.h>
#include <thrift/lib/cpp2/Thrift.h>
#include <thrift/lib/cpp2/op/Encode.h>
#include <thrift/lib/cpp2/protocol/BinaryProtocol.h>
#include <thrift/lib/cpp2/protocol/CompactProtocol.h>
#include <thrift/lib/cpp2/type/Tag.h>
#include <thrift/lib/cpp2/type/UniversalName.h>

namespace apache::thrift::conformance {

// Returns the protocol used by the given any.
Protocol getProtocol(const Any& any) noexcept;

// Returns true iff the any is encoded using the given protocol.
bool hasProtocol(const Any& any, const Protocol& protocol) noexcept;

// Sets the given protocol on the given any.
void setProtocol(const Protocol& protocol, Any& any) noexcept;

// Raises std::invalid_argument if invalid.
void validateAny(const Any& any);

// Returns true iff the any holds a value of type T, identified either by its
// URI or by a type hash prefix of at least kMinTypeHashBytes. Unlike
// AnyRegistry::getTypeId, this does not require T to be registered.
template <typename T>
bool hasType(const Any& any) {
  if (any.type().has_value() && !any.type()->empty()) {
    return *any.type() == uri<T>();
  }
  if (!any.typeHashPrefixSha2_256().has_value()) {
    return false;
  }
  const auto& prefix = *any.typeHashPrefixSha2_256();
  static const auto hash =
      type::getUniversalHash(type::UniversalHashAlgorithm::Sha2_256, uri<T>());
  return prefix.size() >= static_cast<size_t>(kMinTypeHashBytes) &&
      type::matchesUniversalHash(hash, prefix);
}

// Encode a statically-typed value into a conformance::Any, bypassing the
// AnyRegistry. The type must have a Thrift URI (i.e. be a generated struct).
template <
    StandardProtocol P = StandardProtocol::Compact,
    typename T,
    std::enable_if_t<
        !std::is_same_v<std::decay_t<T>, Any> &&
            (P == StandardProtocol::Compact || P == StandardProtocol::Binary),
        int> = 0>
Any toStaticAny(const T& value) {
  using Tag = type::infer_tag<T>;
  using Writer = std::conditional_t<
      P == StandardProtocol::Binary,
      BinaryProtocolWriter,
      CompactProtocolWriter>;

  Writer writer;
  folly::IOBufQueue queue(folly::IOBufQueue::cacheChainLength());
  writer.setOutput(&queue);
  op::encode<Tag>(writer, value);

  Any result;
  result.type() = std::string(uri<T>());
  if constexpr (P != StandardProtocol::Compact) {
    result.protocol() = P;
  }
  result.data() = queue.moveAsValue();
  return result;
}

// Decode a conformance::Any into a statically-typed value, bypassing the
// AnyRegistry. Assumes the Any contains data of type T.
template <typename T>
T fromStaticAny(const Any& any) {
  using Tag = type::infer_tag<T>;
  T value;

  if (!apache::thrift::is_non_optional_field_set_manually_or_by_serializer(
          any.data())) {
    folly::throw_exception<std::runtime_error>(
        "fromStaticAny: Any has no data");
  }

  auto protocol = any.protocol().value_or(StandardProtocol::Compact);
  if (protocol == StandardProtocol::Compact) {
    CompactProtocolReader reader;
    reader.setInput(&*any.data());
    op::decode<Tag>(reader, value);
  } else if (protocol == StandardProtocol::Binary) {
    BinaryProtocolReader reader;
    reader.setInput(&*any.data());
    op::decode<Tag>(reader, value);
  } else {
    folly::throw_exception<std::runtime_error>(
        "fromStaticAny: unsupported protocol");
  }

  return value;
}

} // namespace apache::thrift::conformance
