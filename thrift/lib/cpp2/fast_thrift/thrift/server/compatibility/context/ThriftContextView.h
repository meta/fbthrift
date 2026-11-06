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
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>

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
} // namespace apache::thrift

namespace apache::thrift::fast_thrift::thrift {
class ThriftConnContext;
class ThriftConnectionView;
class ThriftRequestContext;
class ThriftRequestMutator;
class ThriftRequestView;
class ThriftResponseView;
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

template <ThriftContextBackend Backend>
struct ClassicContextTypes {
  using Connection = apache::thrift::Cpp2ConnContext;
  using Request = apache::thrift::Cpp2RequestContext;
};

std::string peerCommonName(const folly::AsyncTransportCertificate* certificate);
const folly::F14NodeMap<std::string, std::string>& emptyHeaders();

} // namespace detail

// Views borrow storage from the context owner and must not outlive it.
// Transport and live certificate access require the connection's EventBase.
// For Classic state access, Ext's state alias must match the context's
// registered internal-fields type, as with Cpp2 getInternalFields<T>().
// Native lookups borrow extension state, which may be cleared before the
// context dies. Mutable state follows the source context's capabilities:
// const Classic contexts deny it; Native extension views permit it.
template <ThriftContextBackend Backend>
class ThriftConnectionContextView {
  using ClassicContext =
      typename detail::ClassicContextTypes<Backend>::Connection;

 public:
  using PeerCertificateSource = std::conditional_t<
      Backend == ThriftContextBackend::Classic,
      compatibility::ClassicPeerCertificateSource,
      compatibility::PeerCertificateSource>;

  struct CachedEkm {
    std::string_view label;
    uint16_t length{};
    const folly::IOBuf* value{};
  };

  // All pointers and views borrow storage from the connection owner.
  struct NativeInputs {
    bool valid{false};
    const folly::SocketAddress* peerAddress{};
    const folly::AsyncTransport* transport{};
    const folly::AsyncTransportCertificate* peerCertificate{};
    bool peerCertReceived{false};
    const X509* peerX509Certificate{};
    void* userData{};
    void* peerIdentities{};
    std::string_view securityProtocol;
    std::optional<std::string_view> clientHostname;
    const apache::thrift::ClientMetadata* clientMetadata{};
    std::span<const CachedEkm> cachedEkms;
    const apache::thrift::fast_thrift::thrift::ExtensionSlots* extensionSlots{};
  };

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
  explicit ThriftConnectionContextView(NativeInputs inputs) noexcept
    requires(Backend == ThriftContextBackend::Native)
      : context_(inputs) {}
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
      return context_.valid;
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
      return nativeInputs().extensionSlots
          ? nativeInputs().extensionSlots->template find<State>(Ext::kId)
          : nullptr;
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
      return nativeInputs().extensionSlots
          ? nativeInputs().extensionSlots->template find<State>(Ext::kId)
          : nullptr;
    }
  }

  const folly::SocketAddress* FOLLY_NULLABLE peerAddress() const {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return classicContext() ? classicContext()->getPeerAddress() : nullptr;
    } else {
      return nativeInputs().peerAddress;
    }
  }
  const folly::AsyncTransport* FOLLY_NULLABLE transport() const {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return classicContext() ? classicContext()->getTransport() : nullptr;
    } else {
      return nativeInputs().transport;
    }
  }
  PeerCertificateSource peerCertificateSource() const {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return {
          .transport =
              classicContext() ? classicContext()->getTransport() : nullptr};
    } else {
      return {
          .transport = nativeInputs().transport,
          .certificate = nativeInputs().peerCertificate};
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
      return nativeInputs().peerCertReceived;
    }
  }
  const X509* FOLLY_NULLABLE peerX509Certificate() const {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return classicContext() ? classicContext()->getPeerCertificate()
                              : nullptr;
    } else {
      return nativeInputs().peerX509Certificate;
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
      return nativeInputs().securityProtocol;
    }
  }
  void* FOLLY_NULLABLE userData() const {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return classicContext() ? classicContext()->getUserData() : nullptr;
    } else {
      return nativeInputs().userData;
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
      return nativeInputs().clientHostname;
    }
  }
  auto clientMetadata() const {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return classicContext() ? classicContext()->getClientMetadataRef()
                              : std::nullopt;
    } else {
      return nativeInputs().clientMetadata;
    }
  }
  void* FOLLY_NULLABLE peerIdentities() const {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return classicContext() ? classicContext()->getPeerIdentities() : nullptr;
    } else {
      return nativeInputs().peerIdentities;
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
      for (const auto& ekm : nativeInputs().cachedEkms) {
        if (ekm.label == label && ekm.length == length) {
          return ekm.value ? ekm.value->clone() : nullptr;
        }
      }
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
      NativeInputs>;

  ClassicContext* mutableClassicContext() const noexcept {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return context_.mutableContext;
    } else {
      return nullptr;
    }
  }
  const NativeInputs& nativeInputs() const noexcept {
    if constexpr (Backend == ThriftContextBackend::Native) {
      return context_;
    } else {
      static constexpr NativeInputs empty;
      return empty;
    }
  }

  Storage context_{};
};

template <ThriftContextBackend Backend>
class ThriftRequestContextView {
  using ClassicContext = typename detail::ClassicContextTypes<Backend>::Request;

 public:
  using HeaderMap = folly::F14NodeMap<std::string, std::string>;

  // All pointers and views borrow storage from the request/connection owner.
  struct NativeInputs {
    bool valid{false};
    bool hasHeaderContainer{false};
    ThriftConnectionContextView<ThriftContextBackend::Native> connection;
    const HeaderMap* requestHeaders{};
    HeaderMap* responseHeaders{};
    std::string_view methodName;
    const apache::thrift::fast_thrift::thrift::ExtensionSlots* extensionSlots{};
  };

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
  explicit ThriftRequestContextView(NativeInputs inputs) noexcept
    requires(Backend == ThriftContextBackend::Native)
      : context_(inputs) {}
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
      return context_.valid;
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
      return nativeInputs().extensionSlots
          ? nativeInputs().extensionSlots->template find<State>(Ext::kId)
          : nullptr;
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
      return nativeInputs().extensionSlots
          ? nativeInputs().extensionSlots->template find<State>(Ext::kId)
          : nullptr;
    }
  }

  // Cpp2 request constness is shallow: its connection remains mutable.
  ThriftConnectionContextView<Backend> connectionContext() const {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return ThriftConnectionContextView<Backend>(
          classicContext() ? classicContext()->getConnectionContext()
                           : nullptr);
    } else {
      return nativeInputs().connection;
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
      return nativeInputs().connection;
    }
  }

  std::string_view methodName() const noexcept {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return classicContext() ? classicContext()->getMethodName()
                              : std::string_view{};
    } else {
      return nativeInputs().methodName;
    }
  }
  const folly::SocketAddress* FOLLY_NULLABLE peerAddress() const {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return classicContext() ? classicContext()->getPeerAddress() : nullptr;
    } else {
      return nativeInputs().connection.peerAddress();
    }
  }
  void* FOLLY_NULLABLE userData() const {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return classicContext() ? classicContext()->getUserData() : nullptr;
    } else {
      return nativeInputs().connection.userData();
    }
  }
  bool hasHeaderContainer() const {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return classicContext() ? classicContext()->getHeader() != nullptr
                              : false;
    } else {
      return nativeInputs().hasHeaderContainer;
    }
  }
  const HeaderMap* FOLLY_NULLABLE requestHeaders() const {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      const auto* header =
          classicContext() ? classicContext()->getHeader() : nullptr;
      return header ? &header->getHeaders() : nullptr;
    } else {
      return nativeInputs().requestHeaders;
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
      return nativeInputs().requestHeaders ? *nativeInputs().requestHeaders
                                           : detail::emptyHeaders();
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
      if (nativeInputs().responseHeaders == nullptr) {
        throw std::logic_error(
            "Thrift request context is unavailable or read-only");
      }
      (*nativeInputs().responseHeaders)[key] = value;
      return true;
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
      if (nativeInputs().responseHeaders == nullptr) {
        throw std::logic_error(
            "Thrift request context is unavailable or read-only");
      }
      (*nativeInputs().responseHeaders)[key] = std::move(value);
      return true;
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
      NativeInputs>;

  ClassicContext* mutableClassicContext() const noexcept {
    if constexpr (Backend == ThriftContextBackend::Classic) {
      return context_.mutableContext;
    } else {
      return nullptr;
    }
  }
  const NativeInputs& nativeInputs() const noexcept {
    if constexpr (Backend == ThriftContextBackend::Native) {
      return context_;
    } else {
      static constexpr NativeInputs empty;
      return empty;
    }
  }

  Storage context_{};
};

ThriftConnectionContextView()
    -> ThriftConnectionContextView<ThriftContextBackend::Native>;
ThriftConnectionContextView(
    typename ThriftConnectionContextView<
        ThriftContextBackend::Native>::NativeInputs)
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
ThriftRequestContextView(
    typename ThriftRequestContextView<
        ThriftContextBackend::Native>::NativeInputs)
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
