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

#include <memory>
#include <string>

#include <boost/intrusive_ptr.hpp>

#include <folly/SocketAddress.h>
#include <folly/io/async/Request.h>

#include <thrift/lib/cpp/transport/THeader.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/ThriftConnContext.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/ThriftRequestContext.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/event_handler/Cpp2BridgeExtension.h>
#include <thrift/lib/cpp2/server/Cpp2ConnContext.h>

namespace apache::thrift::fast_thrift::thrift::server {

/**
 * The Cpp2 contexts a `TProcessorEventHandler` expects, synthesized
 * from what fast_thrift knows.
 *
 * fast_thrift carries `ThriftConnContext` / `ThriftRequestContext` end to end;
 * these types exist only so a handler written against the classic server can
 * still be driven, and they live entirely behind that boundary. Nothing on the
 * fast_thrift data path holds one, and nothing here is handed back out to it.
 *
 * Everything the connection knows is translated, including its transport, so
 * a handler reading local address, exported keying material or the peer
 * through `getTransport()` finds what it expects. Two things still differ from
 * a classic server, both unavoidable:
 *
 *   - the worker's server. `getWorker()` is non-null so handlers that reach
 *     for it do not fault, but it belongs to no `ThriftServer`, so
 *     `getWorker()->getServer()` is null and a handler that dereferences it
 *     must guard.
 *   - what the transport reports about security, on a connection that
 *     downgraded. StopTLS replaces the secured transport with a plaintext one
 *     before this is built, so the certificate and negotiated protocol come
 *     from the handshake snapshot instead. A handler that goes to the
 *     transport for them rather than to the context finds nothing — as it
 *     would on a classic server that downgraded.
 */

/**
 * Owns the `Cpp2ConnContext` for one connection, built from that connection's
 * `ThriftConnContext`. Construct once, when the setup request enters the
 * Thrift pipeline, and keep it for the connection's life: per-connection state
 * a handler writes (identity, authorization decision) lives inside it.
 */
class Cpp2ConnContextAdapter {
 public:
  // An empty hook leaves the peer identities unset.
  Cpp2ConnContextAdapter(
      boost::intrusive_ptr<ThriftConnContext> connContext,
      const apache::thrift::ClientIdentityHook& clientIdentityHook);

  ~Cpp2ConnContextAdapter();

  Cpp2ConnContextAdapter(const Cpp2ConnContextAdapter&) = delete;
  Cpp2ConnContextAdapter& operator=(const Cpp2ConnContextAdapter&) = delete;
  Cpp2ConnContextAdapter(Cpp2ConnContextAdapter&&) = delete;
  Cpp2ConnContextAdapter& operator=(Cpp2ConnContextAdapter&&) = delete;

  apache::thrift::Cpp2ConnContext& get() noexcept { return *cpp2ConnContext_; }
  const apache::thrift::Cpp2ConnContext& get() const noexcept {
    return *cpp2ConnContext_;
  }

  void connectionClosed();

  ThriftConnContext& ftContext() noexcept { return *ftConnContext_; }

 private:
  // Kept alive rather than borrowed: the loan below is revoked in the
  // destructor, which a context freed first would make a write to dead memory.
  const boost::intrusive_ptr<ThriftConnContext> ftConnContext_;
  std::unique_ptr<apache::thrift::Cpp2ConnContext> cpp2ConnContext_;
};

/**
 * The per-request state an event handler reads: the `Cpp2RequestContext`, a
 * `THeader` it reads request headers from and writes response headers to, and
 * the `folly::RequestContext` those handlers stamp ambient state onto.
 *
 * Its method name is a view of the string owned by the native request context,
 * which must therefore also outlive the adapted context.
 *
 * Lives from the point the request enters the pipeline until its response
 * leaves, so both directions of a handler's callbacks see the same objects.
 *
 * The `Cpp2RequestContext` and the `THeader` are both borrowed and must
 * outlive this. Everything on the header other than its read headers is left
 * as the owner left it, so an owner that hands the same one to a second
 * request carries over whatever a handler wrote outside the header maps.
 *
 * The `folly::RequestContext` starts as the native request's context. A classic
 * handler may install a nested context while binding; in that case
 * `captureAmbientContext()` retains the effective top in the bridge for
 * downstream dispatch and the response path. The handlers' scope
 * guards own the links below that top, so one shared pointer retains an
 * arbitrary-depth chain without a second stack allocation.
 */
class Cpp2RequestContextAdapter {
 public:
  // `cpp2RequestContext` is borrowed, not built here: the caller owns its
  // storage and must keep it alive for the whole request, which outlives this
  // adapter. It carries the unqualified method; handlers are given the
  // "Service.method" form separately, by whoever drives the callbacks.
  Cpp2RequestContextAdapter(
      apache::thrift::Cpp2RequestContext& cpp2RequestContext,
      apache::thrift::transport::THeader& header,
      ThriftRequestContext& requestContext);
  ~Cpp2RequestContextAdapter();

  Cpp2RequestContextAdapter(const Cpp2RequestContextAdapter&) = delete;
  Cpp2RequestContextAdapter& operator=(const Cpp2RequestContextAdapter&) =
      delete;
  Cpp2RequestContextAdapter(Cpp2RequestContextAdapter&&) = delete;
  Cpp2RequestContextAdapter& operator=(Cpp2RequestContextAdapter&&) = delete;

  apache::thrift::Cpp2RequestContext& get() noexcept {
    return *cpp2RequestContext_;
  }

  apache::thrift::transport::THeader& header() noexcept { return header_; }

  /**
   * The request's ambient context. Install it around every callback into a
   * event handler, and around the dispatch downstream of them so the service
   * reads what they stamped.
   */
  const std::shared_ptr<folly::RequestContext>& ambientContext()
      const noexcept {
    return ambientContext_;
  }

  /** Retains a nested context installed by a classic handler, if any. */
  void captureAmbientContext() noexcept;

  /**
   * The response headers handlers have written so far, moved out. Called once
   * the write-side callbacks have run, to stamp them onto the outgoing
   * response.
   */
  apache::thrift::transport::THeader::StringToStringMap takeWriteHeaders();

  ThriftRequestContext& ftContext() noexcept { return requestContext_; }

 private:
  apache::thrift::transport::THeader& header_;
  ThriftRequestContext& requestContext_;
  // Owned by the request context, which outlives this; this only reaches it.
  apache::thrift::Cpp2RequestContext* cpp2RequestContext_;
  std::shared_ptr<folly::RequestContext> ambientContext_;
};

} // namespace apache::thrift::fast_thrift::thrift::server
