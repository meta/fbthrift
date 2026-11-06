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

#include <thrift/lib/cpp2/fast_thrift/thrift/server/compatibility/context/NativeThriftContextView.h>

#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/Messages.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/ThriftConnContext.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/ThriftRequestContext.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/extension/ThriftConnectionExtension.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/extension/ThriftExtension.h>

namespace apache::thrift::fast_thrift::thrift::server::compatibility {

namespace ftt = apache::thrift::fast_thrift::thrift;

namespace detail {

class NativeContextAccess {
 public:
  static const ftt::ThriftConnContext* connection(
      const ftt::ThriftConnectionView& view) noexcept {
    return view.connContext_;
  }
  static const ftt::ThriftServerRequestMessage* request(
      const ftt::ThriftRequestView& view) noexcept {
    return &view.request_;
  }
  static ftt::ThriftServerRequestMessage* request(
      ftt::ThriftRequestMutator& view) noexcept {
    return &view.request_;
  }
  static const ftt::ThriftServerResponseMessage* response(
      const ftt::ThriftResponseView& view) noexcept {
    return &view.response_;
  }
};

NativeConnectionContextRef::NativeConnectionContextRef(
    const ftt::ThriftConnectionView& context) noexcept
    : context_(NativeContextAccess::connection(context)) {}

const folly::SocketAddress* NativeConnectionContextRef::peerAddress() const {
  const ftt::ThriftConnectionView view(context_);
  return view ? &view.peerAddress() : nullptr;
}

const folly::AsyncTransport* NativeConnectionContextRef::transport() const {
  const ftt::ThriftConnectionView view(context_);
  return view ? view.transport() : nullptr;
}

const folly::AsyncTransportCertificate*
NativeConnectionContextRef::peerCertificate() const {
  const ftt::ThriftConnectionView view(context_);
  return view ? view.peerCertificate() : nullptr;
}

bool NativeConnectionContextRef::peerCertReceived() const {
  const ftt::ThriftConnectionView view(context_);
  return view && view.peerCertReceived();
}

const X509* NativeConnectionContextRef::peerX509Certificate() const {
  const ftt::ThriftConnectionView view(context_);
  return view ? view.peerX509Certificate() : nullptr;
}

void* NativeConnectionContextRef::userData() const {
  const ftt::ThriftConnectionView view(context_);
  return view ? view.userData() : nullptr;
}

void* NativeConnectionContextRef::peerIdentities() const {
  const ftt::ThriftConnectionView view(context_);
  return view ? view.peerIdentities() : nullptr;
}

std::string_view NativeConnectionContextRef::securityProtocol() const {
  const ftt::ThriftConnectionView view(context_);
  return view ? view.securityProtocol() : std::string_view{};
}

std::optional<std::string_view> NativeConnectionContextRef::clientHostname()
    const {
  const auto* metadata = clientMetadata();
  if (metadata != nullptr && metadata->hostname().has_value()) {
    return std::string_view(*metadata->hostname());
  }
  return std::nullopt;
}

const apache::thrift::ClientMetadata*
NativeConnectionContextRef::clientMetadata() const {
  const ftt::ThriftConnectionView view(context_);
  return view ? view.clientMetadata() : nullptr;
}

const ftt::ExtensionSlots* NativeConnectionContextRef::extensionSlots()
    const noexcept {
  return ftt::ThriftConnectionView(context_).extensionSlots();
}

NativeRequestContextRef::NativeRequestContextRef(
    ftt::ThriftRequestContext* context) noexcept
    : context_{.context = context} {}

NativeRequestContextRef::NativeRequestContextRef(
    ftt::ThriftRequestMutator& context) noexcept
    : source_(Source::MutableRequest),
      context_{.mutableRequest = NativeContextAccess::request(context)} {}

NativeRequestContextRef::NativeRequestContextRef(
    const ftt::ThriftRequestView& context) noexcept
    : source_(Source::Request),
      context_{.request = NativeContextAccess::request(context)} {}

NativeRequestContextRef::NativeRequestContextRef(
    const ftt::ThriftResponseView& context) noexcept
    : source_(Source::Response),
      context_{.response = NativeContextAccess::response(context)} {}

const ftt::ThriftRequestContext* NativeRequestContextRef::requestContext()
    const noexcept {
  switch (source_) {
    case Source::Context:
      return context_.context;
    case Source::Request:
      return context_.request->requestContext.get();
    case Source::MutableRequest:
      return context_.mutableRequest->requestContext.get();
    case Source::Response:
      return context_.response->requestContext.get();
  }
  return nullptr;
}

const ftt::ThriftServerRequestMessage* NativeRequestContextRef::requestMessage()
    const noexcept {
  if (source_ == Source::Request) {
    return context_.request;
  }
  if (source_ == Source::MutableRequest) {
    return context_.mutableRequest;
  }
  return nullptr;
}

NativeRequestContextRef::operator bool() const noexcept {
  return requestContext() != nullptr;
}

NativeConnectionContextRef NativeRequestContextRef::connection() const {
  if (const auto* request = requestMessage()) {
    return NativeConnectionContextRef(
        ftt::ThriftRequestView(*request).connection());
  }
  if (source_ == Source::Response) {
    return NativeConnectionContextRef(
        ftt::ThriftResponseView(*context_.response).connection());
  }
  return NativeConnectionContextRef(
      context_.context ? context_.context->getConnectionContext() : nullptr);
}

std::string_view NativeRequestContextRef::methodName() const noexcept {
  if (const auto* request = requestMessage()) {
    return ftt::ThriftRequestView(*request).qualifiedMethodName();
  }
  if (source_ == Source::Response) {
    return ftt::ThriftResponseView(*context_.response).qualifiedMethodName();
  }
  if (context_.context == nullptr) {
    return {};
  }
  if (const auto* metadata = context_.context->getMethodMetadata()) {
    return metadata->qualifiedMethodName;
  }
  return context_.context->getMethodName();
}

const NativeRequestContextRef::HeaderMap*
NativeRequestContextRef::requestHeaders() const {
  if (const auto* request = requestMessage()) {
    const auto headers = ftt::ThriftRequestView(*request).requestHeaders();
    return headers ? &headers->get() : nullptr;
  }
  if (source_ == Source::Response) {
    const auto headers =
        ftt::ThriftResponseView(*context_.response).requestHeaders();
    return headers ? &headers->get() : nullptr;
  }
  return context_.context ? &context_.context->getHeaders() : nullptr;
}

NativeRequestContextRef::HeaderMap* NativeRequestContextRef::responseHeaders()
    const {
  if (source_ == Source::MutableRequest) {
    ftt::ThriftRequestMutator view(*context_.mutableRequest);
    const auto headers = view.mutableResponseHeaders();
    return headers ? &headers->get() : nullptr;
  }
  if (source_ == Source::Context && context_.context != nullptr) {
    return &context_.context->mutableResponseHeaders();
  }
  return nullptr;
}

bool NativeRequestContextRef::setResponseHeader(
    std::string_view key, const std::string& value) const {
  auto* headers = responseHeaders();
  if (headers == nullptr) {
    throw std::logic_error(
        "Thrift request context is unavailable or read-only");
  }
  (*headers)[key] = value;
  return true;
}

bool NativeRequestContextRef::setResponseHeader(
    std::string_view key, std::string&& value) const {
  auto* headers = responseHeaders();
  if (headers == nullptr) {
    throw std::logic_error(
        "Thrift request context is unavailable or read-only");
  }
  (*headers)[key] = std::move(value);
  return true;
}

const ftt::ExtensionSlots* NativeRequestContextRef::extensionSlots()
    const noexcept {
  if (const auto* request = requestMessage()) {
    return ftt::ThriftRequestView(*request).extensionSlots();
  }
  if (source_ == Source::Response) {
    return ftt::ThriftResponseView(*context_.response).extensionSlots();
  }
  return context_.context ? &context_.context->extensionSlots() : nullptr;
}

} // namespace detail

template <ThriftContextBackend Backend>
ThriftConnectionContextView<Backend>::ThriftConnectionContextView(
    ftt::ThriftConnContext* context) noexcept
  requires(Backend == ThriftContextBackend::Native)
    : context_(context) {}

template <ThriftContextBackend Backend>
ThriftConnectionContextView<Backend>::ThriftConnectionContextView(
    const ftt::ThriftConnectionView& context) noexcept
  requires(Backend == ThriftContextBackend::Native)
    : context_(context) {}

template <ThriftContextBackend Backend>
ThriftRequestContextView<Backend>::ThriftRequestContextView(
    ftt::ThriftRequestContext* context) noexcept
  requires(Backend == ThriftContextBackend::Native)
    : context_(context) {}

template <ThriftContextBackend Backend>
ThriftRequestContextView<Backend>::ThriftRequestContextView(
    ftt::ThriftRequestMutator& context) noexcept
  requires(Backend == ThriftContextBackend::Native)
    : context_(context) {}

template <ThriftContextBackend Backend>
ThriftRequestContextView<Backend>::ThriftRequestContextView(
    const ftt::ThriftRequestView& context) noexcept
  requires(Backend == ThriftContextBackend::Native)
    : context_(context) {}

template <ThriftContextBackend Backend>
ThriftRequestContextView<Backend>::ThriftRequestContextView(
    const ftt::ThriftResponseView& context) noexcept
  requires(Backend == ThriftContextBackend::Native)
    : context_(context) {}

template class ThriftConnectionContextView<ThriftContextBackend::Native>;
template class ThriftRequestContextView<ThriftContextBackend::Native>;

} // namespace apache::thrift::fast_thrift::thrift::server::compatibility
