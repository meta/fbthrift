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
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#include <boost/intrusive_ptr.hpp>

#include <folly/CancellationToken.h>
#include <folly/container/F14Map.h>
#include <folly/io/async/Request.h>

#include <folly/CppAttributes.h>

#include <thrift/lib/cpp2/fast_thrift/common/allocator/EvbAllocator.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/MethodMetadata.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/ExtensionSlots.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/ThriftConnContext.h>
#include <thrift/lib/thrift/gen-cpp2/RpcMetadata_types.h>

namespace apache::thrift::fast_thrift::thrift {

// Per-request context. Lives for the duration of one in-flight RPC.
//
// Construction also creates the request's folly::RequestContext. The first
// pipeline handler that needs request state creates this object; subsequent
// handlers populate its fields as the request traverses the chain.
class ThriftRequestContext {
 public:
  struct Deleter {
    void operator()(ThriftRequestContext* context) const noexcept {
      auto* evb = context->eventBase_;
      if (evb == nullptr || context->thread_ == std::this_thread::get_id()) {
        delete context;
      } else {
        evb->runInEventBaseThread(
            [context, keepAlive = folly::getKeepAliveToken(evb)] {
              delete context;
            });
      }
    }
  };

  // Same type as RequestRpcMetadata.otherMetadata, so headers move in with no
  // copy. F14's default hasher/equal are transparent, enabling find() by
  // string_view without building a std::string.
  using HeaderMap = folly::F14NodeMap<std::string, std::string>;

  explicit ThriftRequestContext(folly::EventBase* eventBase = nullptr)
      : eventBase_(eventBase) {
    folly::LocalRequestContextScopeGuard guard;
    follyRequestContext_ = folly::RequestContext::saveContextLocal();
  }

  ~ThriftRequestContext() {
    if (!extensionSlots_.hasOwnedState()) {
      return;
    }
    std::optional<folly::BorrowedRequestContextScopeGuard> guard;
    extensionSlots_.clearAllOwned([&] {
      guard.reset();
      guard.emplace(follyRequestContext_);
    });
  }

  ThriftRequestContext(const ThriftRequestContext&) = delete;
  ThriftRequestContext& operator=(const ThriftRequestContext&) = delete;
  // Extension storage hands out interior pointers, so a context stays where it
  // was built.
  ThriftRequestContext(ThriftRequestContext&&) = delete;
  ThriftRequestContext& operator=(ThriftRequestContext&&) = delete;

  void setConnectionContext(
      boost::intrusive_ptr<ThriftConnContext> ctx) noexcept {
    connContext_ = std::move(ctx);
  }

  ThriftConnContext* getConnectionContext() const noexcept {
    return connContext_.get();
  }

  const folly::RequestContextHandle& getFollyRequestContext() const noexcept {
    return follyRequestContext_;
  }

  void enableCancellation() {
    if (!cancellationSource_.canBeCancelled()) {
      cancellationSource_ = folly::CancellationSource{};
    }
  }

  bool isCancellationEnabled() const noexcept {
    return cancellationSource_.canBeCancelled();
  }

  folly::CancellationToken getCancellationToken() const noexcept {
    return cancellationSource_.getToken();
  }

  folly::CancellationSource getCancellationSource() const noexcept {
    return cancellationSource_;
  }

  // EventBase-only lifecycle transitions. The token itself may be observed
  // from any handler thread.
  bool requestCancellation() noexcept {
    if (!cancellationSource_.canBeCancelled()) {
      return false;
    }
    if (completionState_ == CompletionState::Completed) {
      return false;
    }
    if (completionState_ == CompletionState::Active) {
      completionState_ = CompletionState::CancellationRequested;
      // A callback may synchronously complete the request and destroy this
      // context. Keep the shared cancellation state alive independently until
      // notification has fully unwound.
      auto source = cancellationSource_;
      return !source.requestCancellation();
    }
    return false;
  }

  bool tryComplete() noexcept {
    if (completionState_ == CompletionState::Completed) {
      return false;
    }
    completionState_ = CompletionState::Completed;
    return true;
  }

  bool isCancellationRequested() const noexcept {
    return cancellationSource_.isCancellationRequested();
  }

  // Invoked method name (RequestRpcMetadata.name), stamped by
  // ThriftServerRequestContextHandler. Empty when the request carried no
  // method metadata.
  void setMethodName(std::string methodName) noexcept {
    methodName_ = std::move(methodName);
  }

  const std::string& getMethodName() const noexcept { return methodName_; }

  void setMethodMetadata(
      const ThriftServerMethodMetadata* FOLLY_NULLABLE metadata) noexcept {
    methodMetadata_ = metadata;
  }

  const ThriftServerMethodMetadata* FOLLY_NULLABLE
  getMethodMetadata() const noexcept {
    return methodMetadata_;
  }

  // Inbound custom request headers (RequestRpcMetadata.otherMetadata), moved
  // into the request context when request headers are enabled. Empty otherwise.
  void setHeaders(HeaderMap headers) noexcept { headers_ = std::move(headers); }

  // Adds to (or overwrites) a single inbound header. Used by extensions
  // injecting a header for the service to read; the bulk setter above is the
  // one the headers handler uses to move the client's map in wholesale.
  void setHeader(std::string key, std::string value) {
    headers_[std::move(key)] = std::move(value);
  }

  const HeaderMap& getHeaders() const noexcept { return headers_; }

  // Returns a pointer to the header value for `key`, or nullptr if absent.
  const std::string* getHeader(std::string_view key) const noexcept {
    auto it = headers_.find(key);
    return it == headers_.end() ? nullptr : &it->second;
  }

  // Custom headers to send back on this request's response, set by the service
  // handler. Kept separate from the inbound headers above: the two directions
  // are independent, and echoing what the client sent is never the intent.
  //
  // The map is the same type as ResponseRpcMetadata.otherMetadata, so it moves
  // onto the outgoing metadata with no copy or rehash.
  //
  // Only unary and stream initial responses carry metadata to put these on. A
  // request answered by a rocket ERROR frame, or by a setup rejection, has
  // nowhere on the wire for them and drops them.
  void setResponseHeader(std::string key, std::string value) {
    writeHeaders_[std::move(key)] = std::move(value);
  }

  HeaderMap& mutableResponseHeaders() noexcept { return writeHeaders_; }

  // Only reports what is still on the context. Once the response has been
  // handed its headers this reads empty, so it answers "what will this
  // response carry", not "what did it carry".
  const std::string* getResponseHeader(std::string_view key) const noexcept {
    auto it = writeHeaders_.find(key);
    return it == writeHeaders_.end() ? nullptr : &it->second;
  }

  bool hasResponseHeaders() const noexcept { return !writeHeaders_.empty(); }

  // Hands the accumulated headers to the response being built, leaving the
  // context with none. Called once per response, on the write path.
  HeaderMap extractResponseHeaders() noexcept {
    return std::exchange(writeHeaders_, {});
  }

  // Checksum algorithm the response must echo, captured from the inbound
  // request's checksum by ThriftServerChecksumHandler. NONE when the request
  // carried no checksum, so the response is left unchecksummed.
  void setChecksumAlgorithm(
      apache::thrift::ChecksumAlgorithm algorithm) noexcept {
    checksumAlgorithm_ = algorithm;
  }

  apache::thrift::ChecksumAlgorithm getChecksumAlgorithm() const noexcept {
    return checksumAlgorithm_;
  }

  void setResponseCompressionConfig(
      apache::thrift::CompressionConfig config) noexcept {
    responseCompressionConfig_ = std::move(config);
  }

  const apache::thrift::CompressionConfig* getResponseCompressionConfig()
      const noexcept {
    return responseCompressionConfig_ ? &*responseCompressionConfig_ : nullptr;
  }

  // Builds this request's extension storage from the server's request-scope
  // layout, which must outlive the request. Called once, by whoever creates
  // the context, before any handler sees it.
  void installExtensions(const ExtensionLayout& layout) {
    extensionSlots_.install(layout);
  }

  const ExtensionSlots& extensionSlots() const noexcept {
    return extensionSlots_;
  }

  // `Ext`'s per-request state, or null when `Ext` is not installed on this
  // server. An extension that declares no `RequestState` does not compile here,
  // so asking for a scope an extension does not have is caught at build time
  // rather than read as absent.
  template <class Ext>
  typename Ext::RequestState* FOLLY_NULLABLE tryState() const noexcept {
    return extensionSlots_.find<typename Ext::RequestState>(Ext::kId);
  }

  // Publishes `Ext`'s state on this context. Non-owning: the extension keeps
  // it alive while the context can reach it, and clears the slot when it does
  // not.
  template <class Ext>
  void setState(typename Ext::RequestState* FOLLY_NULLABLE state) noexcept {
    extensionSlots_.set(Ext::kId, state);
  }

  template <class Ext>
  void adoptState(typename Ext::RequestState* state) noexcept {
    extensionSlots_.adopt(Ext::kId, state);
  }

  template <class Ext>
  void clearOwnedState() noexcept {
    extensionSlots_.clearOwned(Ext::kId);
  }

  // Constructs `Ext`'s state in this context, which owns and destroys it.
  // Unlike `setState`, the extension does not manage the state's lifetime.
  template <class Ext, class... Args>
  typename Ext::RequestState& emplaceState(Args&&... args) {
    return extensionSlots_.template emplace<typename Ext::RequestState>(
        Ext::kId, std::forward<Args>(args)...);
  }

 private:
  enum class CompletionState : uint8_t {
    Active,
    CancellationRequested,
    Completed,
  };

  // Declared before extension storage so request-owned extension state is
  // destroyed before the ambient data it may reference.
  folly::RequestContextHandle follyRequestContext_;
  folly::EventBase* eventBase_;
  const std::thread::id thread_ = std::this_thread::get_id();
  boost::intrusive_ptr<ThriftConnContext> connContext_;
  folly::CancellationSource cancellationSource_{
      folly::CancellationSource::invalid()};
  std::string methodName_;
  const ThriftServerMethodMetadata* methodMetadata_{nullptr};
  HeaderMap headers_;
  HeaderMap writeHeaders_;
  apache::thrift::ChecksumAlgorithm checksumAlgorithm_{
      apache::thrift::ChecksumAlgorithm::NONE};
  ExtensionSlots extensionSlots_;
  std::optional<apache::thrift::CompressionConfig> responseCompressionConfig_;
  CompletionState completionState_{CompletionState::Active};
};

using ThriftRequestContextPtr =
    std::unique_ptr<ThriftRequestContext, ThriftRequestContext::Deleter>;

inline ThriftRequestContextPtr makeThriftRequestContext(folly::EventBase& evb) {
  return ThriftRequestContextPtr(new ThriftRequestContext(&evb));
}

} // namespace apache::thrift::fast_thrift::thrift
