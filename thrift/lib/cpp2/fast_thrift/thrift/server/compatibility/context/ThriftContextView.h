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

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include <folly/CPortability.h>
#include <folly/SocketAddress.h>
#include <folly/container/F14Map.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/AsyncTransport.h>
#include <folly/io/async/AsyncTransportCertificate.h>
#include <folly/portability/OpenSSL.h>

#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/ExtensionSlots.h>

namespace apache::thrift {
class ClientMetadata;
class Cpp2ConnContext;
class Cpp2RequestContext;
namespace transport {
class THeader;
} // namespace transport
} // namespace apache::thrift

namespace apache::thrift::fast_thrift::thrift {
class ThriftConnContext;
class ThriftConnectionView;
class ThriftRequestContext;
class ThriftRequestMutator;
class ThriftRequestView;
class ThriftResponseView;
struct ThriftServerRequestMessage;
struct ThriftServerResponseMessage;
} // namespace apache::thrift::fast_thrift::thrift

namespace apache::thrift::fast_thrift::thrift::server::compatibility {

enum class ThriftContextBackend : std::uint8_t {
  Native,
  Classic,
};

struct ClassicPeerCertificateSource {
  const folly::AsyncTransport* transport{};

  explicit operator bool() const noexcept { return transport != nullptr; }
  const folly::AsyncTransportCertificate* FOLLY_NULLABLE get() const {
    return transport ? transport->getPeerCertificate() : nullptr;
  }
};

struct PeerCertificateSource {
  const folly::AsyncTransport* transport{};
  const folly::AsyncTransportCertificate* certificate{};

  explicit operator bool() const noexcept {
    return transport != nullptr || certificate != nullptr;
  }
  const folly::AsyncTransportCertificate* FOLLY_NULLABLE get() const {
    return transport ? transport->getPeerCertificate() : certificate;
  }
};

namespace detail {

class NativeContextAccess;

class NativeConnectionContextRef {
 public:
  constexpr NativeConnectionContextRef() = default;
  explicit NativeConnectionContextRef(
      const apache::thrift::fast_thrift::thrift::ThriftConnContext*
          context) noexcept
      : context_(context) {}
  explicit NativeConnectionContextRef(
      const apache::thrift::fast_thrift::thrift::ThriftConnectionView&
          context) noexcept;

  explicit operator bool() const noexcept { return context_ != nullptr; }
  const folly::SocketAddress* FOLLY_NULLABLE peerAddress() const;
  const folly::AsyncTransport* FOLLY_NULLABLE transport() const;
  const folly::AsyncTransportCertificate* FOLLY_NULLABLE
  peerCertificate() const;
  bool peerCertReceived() const;
  const X509* FOLLY_NULLABLE peerX509Certificate() const;
  void* FOLLY_NULLABLE userData() const;
  void* FOLLY_NULLABLE peerIdentities() const;
  std::string_view securityProtocol() const;
  std::optional<std::string_view> clientHostname() const;
  const apache::thrift::ClientMetadata* FOLLY_NULLABLE clientMetadata() const;
  const apache::thrift::fast_thrift::thrift::ExtensionSlots* FOLLY_NULLABLE
  extensionSlots() const noexcept;

 private:
  const apache::thrift::fast_thrift::thrift::ThriftConnContext* context_{};
};

class NativeRequestContextRef {
 public:
  using HeaderMap = folly::F14NodeMap<std::string, std::string>;

  constexpr NativeRequestContextRef() = default;
  explicit NativeRequestContextRef(
      apache::thrift::fast_thrift::thrift::ThriftRequestContext*
          context) noexcept;
  explicit NativeRequestContextRef(
      apache::thrift::fast_thrift::thrift::ThriftRequestMutator&
          context) noexcept;
  explicit NativeRequestContextRef(
      const apache::thrift::fast_thrift::thrift::ThriftRequestView&
          context) noexcept;
  explicit NativeRequestContextRef(
      const apache::thrift::fast_thrift::thrift::ThriftResponseView&
          context) noexcept;

  explicit operator bool() const noexcept;
  NativeConnectionContextRef connection() const;
  std::string_view methodName() const noexcept;
  const HeaderMap* FOLLY_NULLABLE requestHeaders() const;
  bool setResponseHeader(std::string_view key, const std::string& value) const;
  bool setResponseHeader(std::string_view key, std::string&& value) const;
  const apache::thrift::fast_thrift::thrift::ExtensionSlots* FOLLY_NULLABLE
  extensionSlots() const noexcept;

 private:
  enum class Source : std::uint8_t {
    Context,
    Request,
    MutableRequest,
    Response,
  };
  union SourceContext {
    apache::thrift::fast_thrift::thrift::ThriftRequestContext* context{};
    const apache::thrift::fast_thrift::thrift::ThriftServerRequestMessage*
        request;
    apache::thrift::fast_thrift::thrift::ThriftServerRequestMessage*
        mutableRequest;
    const apache::thrift::fast_thrift::thrift::ThriftServerResponseMessage*
        response;
  };

  const apache::thrift::fast_thrift::thrift::ThriftRequestContext*
      FOLLY_NULLABLE
      requestContext() const noexcept;
  const apache::thrift::fast_thrift::thrift::ThriftServerRequestMessage*
      FOLLY_NULLABLE
      requestMessage() const noexcept;
  HeaderMap* FOLLY_NULLABLE responseHeaders() const;

  Source source_{Source::Context};
  SourceContext context_{};
};

template <ThriftContextBackend Backend>
struct ClassicContextTypes {
  using Connection = apache::thrift::Cpp2ConnContext;
  using Request = apache::thrift::Cpp2RequestContext;
  using Header = apache::thrift::transport::THeader;
};

std::string peerCommonName(const folly::AsyncTransportCertificate* certificate);
const folly::F14NodeMap<std::string, std::string>& emptyHeaders();

} // namespace detail

template <ThriftContextBackend Backend>
class ThriftRequestContextView;

template <ThriftContextBackend Backend>
class ThriftRequestHeaderView {
  using ClassicHeader = typename detail::ClassicContextTypes<Backend>::Header;

 public:
  using HeaderMap = folly::F14NodeMap<std::string, std::string>;

  constexpr ThriftRequestHeaderView() = default;
  explicit ThriftRequestHeaderView(ClassicHeader* header) noexcept
    requires(Backend == ThriftContextBackend::Classic)
      : source_(ClassicInputs{header, header}) {}
  explicit ThriftRequestHeaderView(const ClassicHeader* header) noexcept
    requires(Backend == ThriftContextBackend::Classic)
      : source_(ClassicInputs{header, nullptr}) {}

  explicit operator bool() const noexcept {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return source_.header != nullptr;
    } else {
      return static_cast<bool>(source_);
    }
  }
  const HeaderMap& headers() const {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return source_.header ? source_.header->getHeaders()
                            : detail::emptyHeaders();
    } else {
      const auto* headers = source_.requestHeaders();
      return headers ? *headers : detail::emptyHeaders();
    }
  }
  bool setResponseHeader(std::string_view key, const std::string& value) const {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      if (source_.header == nullptr) {
        return false;
      }
      if (source_.mutableHeader == nullptr) {
        throw std::logic_error("Thrift request header is read-only");
      }
      source_.mutableHeader->setHeader(key, value);
      return true;
    } else {
      return source_.setResponseHeader(key, value);
    }
  }
  bool setResponseHeader(std::string_view key, std::string&& value) const {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      if (source_.header == nullptr) {
        return false;
      }
      if (source_.mutableHeader == nullptr) {
        throw std::logic_error("Thrift request header is read-only");
      }
      source_.mutableHeader->setHeader(key, std::move(value));
      return true;
    } else {
      return source_.setResponseHeader(key, std::move(value));
    }
  }

 private:
  friend class ThriftRequestContextView<Backend>;

  explicit ThriftRequestHeaderView(
      detail::NativeRequestContextRef source) noexcept
    requires(Backend == ThriftContextBackend::Native)
      : source_(source) {}

  struct ClassicInputs {
    const ClassicHeader* header{};
    ClassicHeader* mutableHeader{};
  };
  using Storage = std::conditional_t<
      Backend == ThriftContextBackend::Classic,
      ClassicInputs,
      detail::NativeRequestContextRef>;

  Storage source_{};
};

// Views borrow their source header, context or message and must not outlive it.
// Extension callback sources remain valid only for the duration of the
// callback. Transport and live certificate access require the connection's
// EventBase. For Classic state access, Ext's state alias must match the
// context's registered internal-fields type, as with Cpp2
// getInternalFields<T>(). Native lookups borrow extension state, which may be
// cleared before the context dies. Mutable state follows the source context's
// capabilities: const Classic contexts deny it; Native extension views permit
// it.
template <ThriftContextBackend Backend>
class ThriftConnectionContextView {
  using ClassicContext =
      typename detail::ClassicContextTypes<Backend>::Connection;

 public:
  using PeerCertificateSource = std::conditional_t<
      Backend == ThriftContextBackend::Classic,
      compatibility::ClassicPeerCertificateSource,
      compatibility::PeerCertificateSource>;

  constexpr ThriftConnectionContextView() = default;
  explicit ThriftConnectionContextView(
      apache::thrift::Cpp2ConnContext* context) noexcept
    requires(Backend == ThriftContextBackend::Classic)
      : context_(ClassicInputs{context, context}) {}
  explicit ThriftConnectionContextView(
      const apache::thrift::Cpp2ConnContext* context) noexcept
    requires(Backend == ThriftContextBackend::Classic)
      : context_(ClassicInputs{context, nullptr}) {}
  explicit ThriftConnectionContextView(
      apache::thrift::Cpp2ConnContext& context) noexcept
    requires(Backend == ThriftContextBackend::Classic)
      : ThriftConnectionContextView(&context) {}
  explicit ThriftConnectionContextView(
      const apache::thrift::Cpp2ConnContext& context) noexcept
    requires(Backend == ThriftContextBackend::Classic)
      : ThriftConnectionContextView(&context) {}
  explicit ThriftConnectionContextView(
      apache::thrift::fast_thrift::thrift::ThriftConnContext* context) noexcept
    requires(Backend == ThriftContextBackend::Native);
  explicit ThriftConnectionContextView(
      const apache::thrift::fast_thrift::thrift::ThriftConnectionView&
          context) noexcept
    requires(Backend == ThriftContextBackend::Native);

  explicit operator bool() const noexcept {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return classicContext() != nullptr;
    } else {
      return static_cast<bool>(context_);
    }
  }

  const ClassicContext* FOLLY_NULLABLE classicContext() const noexcept
    requires(Backend == ThriftContextBackend::Classic)
  {
    return context_.context;
  }

  const ClassicContext& requiredClassicContext() const noexcept
    requires(Backend == ThriftContextBackend::Classic)
  {
    return *classicContext();
  }

  template <class Ext>
  const typename Ext::ConnState* FOLLY_NULLABLE tryState() const noexcept {
    using State = typename Ext::ConnState;
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return classicContext()
          ? &classicContext()->template getInternalFields<State>()
          : nullptr;
    } else {
      const auto* slots = context_.extensionSlots();
      return slots ? slots->template find<State>(Ext::kId) : nullptr;
    }
  }
  template <class Ext>
  typename Ext::ConnState* FOLLY_NULLABLE tryMutableState() const noexcept {
    using State = typename Ext::ConnState;
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return mutableClassicContext()
          ? &mutableClassicContext()->template getInternalFields<State>()
          : nullptr;
    } else {
      const auto* slots = context_.extensionSlots();
      return slots ? slots->template find<State>(Ext::kId) : nullptr;
    }
  }

  const folly::SocketAddress* FOLLY_NULLABLE peerAddress() const {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return classicContext() ? classicContext()->getPeerAddress() : nullptr;
    } else {
      return context_.peerAddress();
    }
  }
  const folly::AsyncTransport* FOLLY_NULLABLE transport() const {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return classicContext() ? classicContext()->getTransport() : nullptr;
    } else {
      return context_.transport();
    }
  }
  PeerCertificateSource peerCertificateSource() const {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return {
          .transport =
              classicContext() ? classicContext()->getTransport() : nullptr};
    } else {
      return {
          .transport = context_.transport(),
          .certificate = context_.peerCertificate()};
    }
  }
  const folly::AsyncTransportCertificate* FOLLY_NULLABLE
  peerCertificate() const {
    return peerCertificateSource().get();
  }
  bool peerCertReceived() const {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return classicContext() ? classicContext()->peerCertReceived() : false;
    } else {
      return context_.peerCertReceived();
    }
  }
  const X509* FOLLY_NULLABLE peerX509Certificate() const {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return classicContext() ? classicContext()->getPeerCertificate()
                              : nullptr;
    } else {
      return context_.peerX509Certificate();
    }
  }
  std::string peerCommonName() const {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return classicContext() ? classicContext()->getPeerCommonName() : "";
    } else {
      return detail::peerCommonName(peerCertificate());
    }
  }
  std::string_view securityProtocol() const {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return classicContext() ? classicContext()->getSecurityProtocol()
                              : std::string_view{};
    } else {
      return context_.securityProtocol();
    }
  }
  void* FOLLY_NULLABLE userData() const {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return classicContext() ? classicContext()->getUserData() : nullptr;
    } else {
      return context_.userData();
    }
  }
  std::optional<std::string_view> clientHostname() const {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      if (classicContext()) {
        const auto metadata = classicContext()->getClientMetadataRef();
        return metadata ? metadata->getHostname() : std::nullopt;
      }
      return std::nullopt;
    } else {
      return context_.clientHostname();
    }
  }
  auto clientMetadata() const {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return classicContext() ? classicContext()->getClientMetadataRef()
                              : std::nullopt;
    } else {
      return context_.clientMetadata();
    }
  }
  void* FOLLY_NULLABLE peerIdentities() const {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return classicContext() ? classicContext()->getPeerIdentities() : nullptr;
    } else {
      return context_.peerIdentities();
    }
  }
  std::unique_ptr<folly::IOBuf> cachedEkm(
      std::string_view label, uint16_t length) const {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return classicContext()
          ? classicContext()->getCachedEkm(
                folly::StringPiece(label.data(), label.size()), length)
          : nullptr;
    } else {
      return nullptr;
    }
  }

 private:
  struct ClassicInputs {
    const ClassicContext* context{};
    ClassicContext* mutableContext{};
  };
  using Storage = std::conditional_t<
      Backend == ThriftContextBackend::Classic,
      ClassicInputs,
      detail::NativeConnectionContextRef>;

  friend class ThriftRequestContextView<Backend>;
  explicit ThriftConnectionContextView(
      detail::NativeConnectionContextRef context) noexcept
    requires(Backend == ThriftContextBackend::Native)
      : context_(context) {}

  ClassicContext* mutableClassicContext() const noexcept {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return context_.mutableContext;
    } else {
      return nullptr;
    }
  }
  Storage context_{};
};

template <ThriftContextBackend Backend>
class ThriftRequestContextView {
  using ClassicContext = typename detail::ClassicContextTypes<Backend>::Request;

 public:
  using HeaderMap = folly::F14NodeMap<std::string, std::string>;

  constexpr ThriftRequestContextView() = default;
  explicit ThriftRequestContextView(
      apache::thrift::Cpp2RequestContext* context) noexcept
    requires(Backend == ThriftContextBackend::Classic)
      : context_(ClassicInputs{context, context}) {}
  explicit ThriftRequestContextView(
      const apache::thrift::Cpp2RequestContext* context) noexcept
    requires(Backend == ThriftContextBackend::Classic)
      : context_(ClassicInputs{context, nullptr}) {}
  explicit ThriftRequestContextView(
      apache::thrift::Cpp2RequestContext& context) noexcept
    requires(Backend == ThriftContextBackend::Classic)
      : ThriftRequestContextView(&context) {}
  explicit ThriftRequestContextView(
      const apache::thrift::Cpp2RequestContext& context) noexcept
    requires(Backend == ThriftContextBackend::Classic)
      : ThriftRequestContextView(&context) {}
  explicit ThriftRequestContextView(
      apache::thrift::fast_thrift::thrift::ThriftRequestContext*
          context) noexcept
    requires(Backend == ThriftContextBackend::Native);
  explicit ThriftRequestContextView(
      apache::thrift::fast_thrift::thrift::ThriftRequestMutator&
          context) noexcept
    requires(Backend == ThriftContextBackend::Native);
  explicit ThriftRequestContextView(
      const apache::thrift::fast_thrift::thrift::ThriftRequestView&
          context) noexcept
    requires(Backend == ThriftContextBackend::Native);
  explicit ThriftRequestContextView(
      const apache::thrift::fast_thrift::thrift::ThriftResponseView&
          context) noexcept
    requires(Backend == ThriftContextBackend::Native);

  explicit operator bool() const noexcept {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return classicContext() != nullptr;
    } else {
      return static_cast<bool>(context_);
    }
  }

  const ClassicContext* FOLLY_NULLABLE classicContext() const noexcept
    requires(Backend == ThriftContextBackend::Classic)
  {
    return context_.context;
  }

  const ClassicContext& requiredClassicContext() const noexcept
    requires(Backend == ThriftContextBackend::Classic)
  {
    return *classicContext();
  }

  template <class Ext>
  const typename Ext::RequestState* FOLLY_NULLABLE tryState() const noexcept {
    using State = typename Ext::RequestState;
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return classicContext()
          ? &classicContext()->template getInternalFields<State>()
          : nullptr;
    } else {
      const auto* slots = context_.extensionSlots();
      return slots ? slots->template find<State>(Ext::kId) : nullptr;
    }
  }
  template <class Ext>
  typename Ext::RequestState* FOLLY_NULLABLE tryMutableState() const noexcept {
    using State = typename Ext::RequestState;
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return mutableClassicContext()
          ? &mutableClassicContext()->template getInternalFields<State>()
          : nullptr;
    } else {
      const auto* slots = context_.extensionSlots();
      return slots ? slots->template find<State>(Ext::kId) : nullptr;
    }
  }

  // Cpp2 request constness is shallow: its connection remains mutable.
  ThriftConnectionContextView<Backend> connectionContext() const {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return ThriftConnectionContextView<Backend>(
          classicContext() ? classicContext()->getConnectionContext()
                           : nullptr);
    } else {
      return ThriftConnectionContextView<Backend>(context_.connection());
    }
  }
  // Requires a valid classic request and a non-null classic connection.
  ThriftConnectionContextView<Backend> requiredConnectionContext() const {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      auto* connection = classicContext()->getConnectionContext();
#if FOLLY_HAS_BUILTIN(__builtin_assume)
      __builtin_assume(connection != nullptr);
#endif
      return ThriftConnectionContextView<Backend>(*connection);
    } else {
      return ThriftConnectionContextView<Backend>(context_.connection());
    }
  }

  std::string_view methodName() const noexcept {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return classicContext() ? classicContext()->getMethodName()
                              : std::string_view{};
    } else {
      return context_.methodName();
    }
  }
  const folly::SocketAddress* FOLLY_NULLABLE peerAddress() const {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return classicContext() ? classicContext()->getPeerAddress() : nullptr;
    } else {
      return context_.connection().peerAddress();
    }
  }
  void* FOLLY_NULLABLE userData() const {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return classicContext() ? classicContext()->getUserData() : nullptr;
    } else {
      return context_.connection().userData();
    }
  }
  ThriftRequestHeaderView<Backend> headerContainer() const {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return ThriftRequestHeaderView<Backend>(
          classicContext() ? classicContext()->getHeader() : nullptr);
    } else {
      return ThriftRequestHeaderView<Backend>(context_);
    }
  }
  bool hasHeaderContainer() const {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return classicContext() ? classicContext()->getHeader() != nullptr
                              : false;
    } else {
      return static_cast<bool>(context_);
    }
  }
  const HeaderMap* FOLLY_NULLABLE requestHeaders() const {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      const auto* header =
          classicContext() ? classicContext()->getHeader() : nullptr;
      return header ? &header->getHeaders() : nullptr;
    } else {
      return context_.requestHeaders();
    }
  }
  bool hasHeaders() const {
    const auto* headers = requestHeaders();
    return headers != nullptr && !headers->empty();
  }
  const HeaderMap& headers() const {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return classicContext() ? classicContext()->getHeaders()
                              : detail::emptyHeaders();
    } else {
      const auto* headers = context_.requestHeaders();
      return headers ? *headers : detail::emptyHeaders();
    }
  }
  const std::string* FOLLY_NULLABLE header(std::string_view key) const {
    const auto* headers = requestHeaders();
    if (headers == nullptr) {
      return nullptr;
    }
    const auto it = headers->find(key);
    return it == headers->end() ? nullptr : &it->second;
  }
  const std::string* FOLLY_NULLABLE
  header(const folly::F14HashToken& hashToken, std::string_view key) const {
    const auto* headers = requestHeaders();
    if (headers == nullptr) {
      return nullptr;
    }
    const auto it = headers->find(hashToken, key);
    return it == headers->end() ? nullptr : &it->second;
  }
  bool setResponseHeader(std::string_view key, const std::string& value) const {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      if (mutableClassicContext()) {
        return mutableClassicContext()->setHeader(key, value);
      }
      throw std::logic_error(
          "Thrift request context is unavailable or read-only");
    } else {
      return context_.setResponseHeader(key, value);
    }
  }
  bool setResponseHeader(std::string_view key, std::string&& value) const {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      if (mutableClassicContext()) {
        return mutableClassicContext()->setHeader(key, std::move(value));
      }
      throw std::logic_error(
          "Thrift request context is unavailable or read-only");
    } else {
      return context_.setResponseHeader(key, std::move(value));
    }
  }

 private:
  struct ClassicInputs {
    const ClassicContext* context{};
    ClassicContext* mutableContext{};
  };
  using Storage = std::conditional_t<
      Backend == ThriftContextBackend::Classic,
      ClassicInputs,
      detail::NativeRequestContextRef>;

  ClassicContext* mutableClassicContext() const noexcept {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return context_.mutableContext;
    } else {
      return nullptr;
    }
  }
  Storage context_{};
};

ThriftConnectionContextView()
    -> ThriftConnectionContextView<ThriftContextBackend::Native>;
ThriftConnectionContextView(apache::thrift::Cpp2ConnContext*)
    -> ThriftConnectionContextView<ThriftContextBackend::Classic>;
ThriftConnectionContextView(const apache::thrift::Cpp2ConnContext*)
    -> ThriftConnectionContextView<ThriftContextBackend::Classic>;
ThriftConnectionContextView(apache::thrift::Cpp2ConnContext&)
    -> ThriftConnectionContextView<ThriftContextBackend::Classic>;
ThriftConnectionContextView(const apache::thrift::Cpp2ConnContext&)
    -> ThriftConnectionContextView<ThriftContextBackend::Classic>;
ThriftConnectionContextView(
    apache::thrift::fast_thrift::thrift::ThriftConnContext*)
    -> ThriftConnectionContextView<ThriftContextBackend::Native>;

ThriftRequestContextView()
    -> ThriftRequestContextView<ThriftContextBackend::Native>;
ThriftRequestContextView(apache::thrift::Cpp2RequestContext*)
    -> ThriftRequestContextView<ThriftContextBackend::Classic>;
ThriftRequestContextView(const apache::thrift::Cpp2RequestContext*)
    -> ThriftRequestContextView<ThriftContextBackend::Classic>;
ThriftRequestContextView(apache::thrift::Cpp2RequestContext&)
    -> ThriftRequestContextView<ThriftContextBackend::Classic>;
ThriftRequestContextView(const apache::thrift::Cpp2RequestContext&)
    -> ThriftRequestContextView<ThriftContextBackend::Classic>;
ThriftRequestContextView(
    apache::thrift::fast_thrift::thrift::ThriftRequestContext*)
    -> ThriftRequestContextView<ThriftContextBackend::Native>;
ThriftConnectionContextView(
    const apache::thrift::fast_thrift::thrift::ThriftConnectionView&)
    -> ThriftConnectionContextView<ThriftContextBackend::Native>;
ThriftRequestContextView(
    apache::thrift::fast_thrift::thrift::ThriftRequestMutator&)
    -> ThriftRequestContextView<ThriftContextBackend::Native>;
ThriftRequestContextView(
    const apache::thrift::fast_thrift::thrift::ThriftRequestView&)
    -> ThriftRequestContextView<ThriftContextBackend::Native>;
ThriftRequestContextView(
    const apache::thrift::fast_thrift::thrift::ThriftResponseView&)
    -> ThriftRequestContextView<ThriftContextBackend::Native>;

} // namespace apache::thrift::fast_thrift::thrift::server::compatibility
