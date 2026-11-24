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

#include <atomic>
#include <exception>
#include <memory>
#include <string_view>
#include <utility>

#include <folly/ExceptionString.h>
#include <folly/ExceptionWrapper.h>
#include <folly/io/async/EventBase.h>
#include <thrift/lib/cpp/TApplicationException.h>
#include <thrift/lib/cpp2/fast_thrift/frame/read/ParsedFrame.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/server/MetadataProtocol.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/Messages.h>
#include <thrift/lib/cpp2/protocol/BinaryProtocol.h>
#include <thrift/lib/cpp2/protocol/CompactProtocol.h>
#include <thrift/lib/thrift/gen-cpp2/RpcMetadata_types.h>

namespace apache::thrift::fast_thrift::thrift {

inline std::unique_ptr<apache::thrift::RequestRpcMetadata>
makeServerRequestMetadata(folly::EventBase&) {
  if (serverRequestMetadataPoolLimit().load(std::memory_order_relaxed) != 0) {
    if (auto metadata = detail::ServerRequestMetadataPool::get().take()) {
      return metadata;
    }
  }
  return std::make_unique<apache::thrift::RequestRpcMetadata>();
}

inline std::unique_ptr<apache::thrift::RequestRpcMetadata>
makeServerRequestMetadata(
    folly::EventBase&, apache::thrift::RequestRpcMetadata metadata) {
  return std::make_unique<apache::thrift::RequestRpcMetadata>(
      std::move(metadata));
}

inline folly::exception_wrapper deserializeRequestMetadata(
    const frame::read::ParsedFrame& frame,
    apache::thrift::RequestRpcMetadata& metadata,
    rocket::server::MetadataProtocol metadataProtocol) noexcept {
  if (!frame.hasMetadata() || frame.metadataSize() == 0) {
    return {};
  }
  try {
    if (metadataProtocol == rocket::server::MetadataProtocol::COMPACT) {
      apache::thrift::CompactProtocolReader reader;
      reader.setInput(frame.metadataCursor());
      metadata.read(&reader);
    } else {
      apache::thrift::BinaryProtocolReader reader;
      reader.setInput(frame.metadataCursor());
      metadata.read(&reader);
    }
  } catch (...) {
    return folly::make_exception_wrapper<apache::thrift::TApplicationException>(
        ("Failed to deserialize request metadata: " +
         folly::exceptionStr(std::current_exception()))
            .toStdString());
  }
  return {};
}

inline bool isRequestRpcMetadataValid(
    const apache::thrift::RequestRpcMetadata& metadata,
    apache::thrift::RpcKind expectedKind) noexcept {
  return metadata.protocol() && metadata.name() && metadata.kind() &&
      metadata.kind() == expectedKind;
}

enum class ServerRequestRoutingStatus {
  MissingMetadata,
  UnsupportedRpcKind,
  Ready,
};

struct ServerRequestRoutingMetadata {
  std::string_view methodName;
  apache::thrift::RpcKind kind{static_cast<apache::thrift::RpcKind>(-1)};
  ServerRequestRoutingStatus status{
      ServerRequestRoutingStatus::MissingMetadata};
};

inline ServerRequestRoutingMetadata getServerRequestRoutingMetadata(
    const ThriftServerRequestMessage& request) noexcept {
  const auto* metadata = request.payload.getRequestRpcMetadata();
  if (metadata == nullptr) {
    return {};
  }
  const auto kind =
      metadata->kind().value_or(static_cast<apache::thrift::RpcKind>(-1));
  if (kind != apache::thrift::RpcKind::SINGLE_REQUEST_SINGLE_RESPONSE &&
      kind != apache::thrift::RpcKind::SINGLE_REQUEST_STREAMING_RESPONSE) {
    return {{}, kind, ServerRequestRoutingStatus::UnsupportedRpcKind};
  }
  std::string_view methodName;
  if (request.requestContext != nullptr &&
      !request.requestContext->getMethodName().empty()) {
    methodName = request.requestContext->getMethodName();
  } else if (auto name = metadata->name(); name.has_value()) {
    methodName = name->view();
  }
  return {methodName, kind, ServerRequestRoutingStatus::Ready};
}

} // namespace apache::thrift::fast_thrift::thrift
