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

#ifndef FBTHRIFT_THRIFT_CONTEXT_VIEW_H
#error "This should only be included by ThriftContextView.h"
#endif

#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/Messages.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/ThriftConnContext.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/ThriftRequestContext.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/compatibility/context/ThriftContextView.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/extension/ThriftConnectionExtension.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/extension/ThriftExtension.h>

namespace apache::thrift::fast_thrift::thrift::server::compatibility::detail {

namespace ftt = apache::thrift::fast_thrift::thrift;

class NativeContextAccess {
  friend class NativeConnectionContextRef;
  friend class NativeRequestContextRef;

  static const ftt::ThriftConnContext* connection(
      const ftt::ThriftConnectionView& view) noexcept {
    return view.connContext_;
  }
  static const ftt::ThriftServerRequestMessage* request(
      const ftt::ThriftRequestView& view) noexcept {
    return &view.request_;
  }
  static const ftt::ThriftServerResponseMessage* response(
      const ftt::ThriftResponseView& view) noexcept {
    return &view.response_;
  }
};

inline NativeConnectionContextRef::NativeConnectionContextRef(
    const ftt::ThriftConnectionView& context) noexcept
    : context_(NativeContextAccess::connection(context)) {}

inline const folly::SocketAddress* NativeConnectionContextRef::peerAddress()
    const {
  const ftt::ThriftConnectionView view(context_);
  return view ? &view.peerAddress() : nullptr;
}

inline const folly::AsyncTransport* NativeConnectionContextRef::transport()
    const {
  const ftt::ThriftConnectionView view(context_);
  return view ? view.transport() : nullptr;
}

inline const folly::AsyncTransportCertificate*
NativeConnectionContextRef::peerCertificate() const {
  const ftt::ThriftConnectionView view(context_);
  return view ? view.peerCertificate() : nullptr;
}

inline bool NativeConnectionContextRef::peerCertReceived() const {
  const ftt::ThriftConnectionView view(context_);
  return view && view.peerCertReceived();
}

inline const X509* NativeConnectionContextRef::peerX509Certificate() const {
  const ftt::ThriftConnectionView view(context_);
  return view ? view.peerX509Certificate() : nullptr;
}

inline std::string NativeConnectionContextRef::peerCommonName() const {
  const ftt::ThriftConnectionView view(context_);
  return view ? view.peerCommonName() : "";
}

inline void* NativeConnectionContextRef::userData() const {
  const ftt::ThriftConnectionView view(context_);
  return view ? view.userData() : nullptr;
}

inline void* NativeConnectionContextRef::peerIdentities() const {
  const ftt::ThriftConnectionView view(context_);
  return view ? view.peerIdentities() : nullptr;
}

inline std::string_view NativeConnectionContextRef::securityProtocol() const {
  const ftt::ThriftConnectionView view(context_);
  return view ? view.securityProtocol() : std::string_view{};
}

inline std::optional<std::string_view>
NativeConnectionContextRef::clientHostname() const {
  const auto* metadata = clientMetadata();
  if (metadata != nullptr && metadata->hostname().has_value()) {
    return std::string_view(*metadata->hostname());
  }
  return std::nullopt;
}

inline const apache::thrift::ClientMetadata*
NativeConnectionContextRef::clientMetadata() const {
  const ftt::ThriftConnectionView view(context_);
  return view ? view.clientMetadata() : nullptr;
}

inline const ftt::ExtensionSlots* NativeConnectionContextRef::extensionSlots()
    const noexcept {
  return ftt::ThriftConnectionView(context_).extensionSlots();
}

inline std::unique_ptr<folly::IOBuf> NativeConnectionContextRef::cachedEkm(
    std::string_view label, uint16_t length) const {
  return ftt::ThriftConnectionView(context_).cachedEkm(label, length);
}

inline NativeRequestContextRef::NativeRequestContextRef(
    ftt::ThriftRequestContext* context) noexcept
    : context_{.context = context} {}

inline NativeRequestContextRef::NativeRequestContextRef(
    ftt::ThriftRequestMutator& context) noexcept
    : source_(Source::MutableRequest),
      context_{.request = NativeContextAccess::request(context)} {}

inline NativeRequestContextRef::NativeRequestContextRef(
    const ftt::ThriftRequestView& context) noexcept
    : source_(Source::Request),
      context_{.request = NativeContextAccess::request(context)} {}

inline NativeRequestContextRef::NativeRequestContextRef(
    const ftt::ThriftResponseView& context) noexcept
    : source_(Source::Response),
      context_{.response = NativeContextAccess::response(context)} {}

inline const ftt::ThriftRequestContext*
NativeRequestContextRef::requestContext() const noexcept {
  switch (source_) {
    case Source::Context:
      return context_.context;
    case Source::Request:
    case Source::MutableRequest:
      return context_.request->requestContext.get();
    case Source::Response:
      return context_.response->requestContext.get();
  }
  return nullptr;
}

inline const ftt::ThriftServerRequestMessage*
NativeRequestContextRef::requestMessage() const noexcept {
  if (source_ == Source::Request || source_ == Source::MutableRequest) {
    return context_.request;
  }
  return nullptr;
}

inline NativeRequestContextRef::operator bool() const noexcept {
  return requestContext() != nullptr;
}

inline NativeConnectionContextRef NativeRequestContextRef::connection() const {
  const auto* context = requestContext();
  return NativeConnectionContextRef(
      context ? context->getConnectionContext() : nullptr);
}

inline std::string_view NativeRequestContextRef::methodName() const noexcept {
  const auto* context = requestContext();
  if (context != nullptr) {
    if (const auto* metadata = context->getMethodMetadata()) {
      return metadata->qualifiedMethodName;
    }
    if (!context->getMethodName().empty()) {
      return context->getMethodName();
    }
  }
  if (const auto* request = requestMessage()) {
    const auto* metadata = request->payload.getRequestRpcMetadata();
    return metadata && metadata->name().has_value() ? metadata->name()->view()
                                                    : std::string_view{};
  }
  return context ? std::string_view(context->getMethodName())
                 : std::string_view{};
}

inline const NativeRequestContextRef::HeaderMap*
NativeRequestContextRef::requestHeaders() const {
  const auto* context = requestContext();
  return context ? &context->getHeaders() : nullptr;
}

inline NativeRequestContextRef::HeaderMap*
NativeRequestContextRef::responseHeaders() const {
  if (source_ == Source::MutableRequest) {
    const auto& context = context_.request->requestContext;
    return context ? &context->mutableResponseHeaders() : nullptr;
  }
  if (source_ == Source::Context && context_.context != nullptr) {
    return &context_.context->mutableResponseHeaders();
  }
  return nullptr;
}

inline bool NativeRequestContextRef::setResponseHeader(
    std::string_view key, const std::string& value) const {
  auto* headers = responseHeaders();
  if (headers == nullptr) {
    throw std::logic_error(
        "Thrift request context is unavailable or read-only");
  }
  (*headers)[key] = value;
  return true;
}

inline bool NativeRequestContextRef::setResponseHeader(
    std::string_view key, std::string&& value) const {
  auto* headers = responseHeaders();
  if (headers == nullptr) {
    throw std::logic_error(
        "Thrift request context is unavailable or read-only");
  }
  (*headers)[key] = std::move(value);
  return true;
}

inline const ftt::ExtensionSlots* NativeRequestContextRef::extensionSlots()
    const noexcept {
  const auto* context = requestContext();
  return context ? &context->extensionSlots() : nullptr;
}

} // namespace
  // apache::thrift::fast_thrift::thrift::server::compatibility::detail
