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

#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/ThriftConnContext.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/context/ThriftRequestContext.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/extension/ThriftConnectionExtension.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/extension/ThriftExtension.h>

namespace apache::thrift::fast_thrift::thrift::server::compatibility {

namespace ftt = apache::thrift::fast_thrift::thrift;

template <ThriftContextBackend Backend>
ThriftConnectionContextView<Backend>::ThriftConnectionContextView(
    ftt::ThriftConnContext* context) noexcept
  requires(Backend == ThriftContextBackend::Native)
    : ThriftConnectionContextView(ftt::ThriftConnectionView(context)) {}

template <ThriftContextBackend Backend>
ThriftConnectionContextView<Backend>::ThriftConnectionContextView(
    const ftt::ThriftConnectionView& context) noexcept
  requires(Backend == ThriftContextBackend::Native)
{
  if (!context) {
    return;
  }
  context_.valid = true;
  context_.extensionSlots = context.extensionSlots();
  context_.peerAddress = &context.peerAddress();
  context_.transport = context.transport();
  context_.peerX509Certificate = context.peerX509Certificate();
  context_.peerCertReceived = context.peerCertReceived();
  context_.userData = context.userData();
  context_.peerIdentities = context.peerIdentities();
  context_.peerCertificate = context.peerCertificate();
  context_.securityProtocol = context.securityProtocol();
  const auto* metadata = context.clientMetadata();
  context_.clientMetadata = metadata;
  if (metadata != nullptr && metadata->hostname().has_value()) {
    context_.clientHostname = *metadata->hostname();
  }
}

template <ThriftContextBackend Backend>
ThriftRequestContextView<Backend>::ThriftRequestContextView(
    ftt::ThriftRequestContext* context) noexcept
  requires(Backend == ThriftContextBackend::Native)
{
  if (context == nullptr) {
    return;
  }
  context_.valid = true;
  context_.hasHeaderContainer = true;
  context_.extensionSlots = &context->extensionSlots();
  context_.connection =
      ThriftConnectionContextView<ThriftContextBackend::Native>(
          context->getConnectionContext());
  context_.requestHeaders = &context->getHeaders();
  context_.responseHeaders = &context->mutableResponseHeaders();
  if (const auto* metadata = context->getMethodMetadata()) {
    context_.methodName = metadata->qualifiedMethodName;
  } else {
    context_.methodName = context->getMethodName();
  }
}

template <ThriftContextBackend Backend>
ThriftRequestContextView<Backend>::ThriftRequestContextView(
    ftt::ThriftRequestMutator& context) noexcept
  requires(Backend == ThriftContextBackend::Native)
{
  context_.valid = context.requestHeaders().has_value();
  context_.hasHeaderContainer = context_.valid;
  context_.extensionSlots = context.extensionSlots();
  context_.connection =
      ThriftConnectionContextView<ThriftContextBackend::Native>(
          context.connection());
  context_.requestHeaders =
      context_.valid ? &context.requestHeaders()->get() : nullptr;
  const auto responseHeaders = context.mutableResponseHeaders();
  context_.responseHeaders =
      responseHeaders ? &responseHeaders->get() : nullptr;
  context_.methodName = context.qualifiedMethodName();
}

template <ThriftContextBackend Backend>
ThriftRequestContextView<Backend>::ThriftRequestContextView(
    const ftt::ThriftRequestView& context) noexcept
  requires(Backend == ThriftContextBackend::Native)
{
  context_.valid = context.requestHeaders().has_value();
  context_.hasHeaderContainer = context_.valid;
  context_.extensionSlots = context.extensionSlots();
  context_.connection =
      ThriftConnectionContextView<ThriftContextBackend::Native>(
          context.connection());
  context_.requestHeaders =
      context_.valid ? &context.requestHeaders()->get() : nullptr;
  context_.methodName = context.qualifiedMethodName();
}

template <ThriftContextBackend Backend>
ThriftRequestContextView<Backend>::ThriftRequestContextView(
    const ftt::ThriftResponseView& context) noexcept
  requires(Backend == ThriftContextBackend::Native)
{
  context_.valid = context.requestHeaders().has_value();
  context_.hasHeaderContainer = context_.valid;
  context_.extensionSlots = context.extensionSlots();
  context_.connection =
      ThriftConnectionContextView<ThriftContextBackend::Native>(
          context.connection());
  context_.requestHeaders =
      context_.valid ? &context.requestHeaders()->get() : nullptr;
  context_.methodName = context.qualifiedMethodName();
}

template class ThriftConnectionContextView<ThriftContextBackend::Native>;
template class ThriftRequestContextView<ThriftContextBackend::Native>;

} // namespace apache::thrift::fast_thrift::thrift::server::compatibility
