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

#include <thrift/lib/cpp2/fast_thrift/thrift/server/compatibility/context/ThriftContextView.h>

#include <folly/Indestructible.h>
#include <folly/io/async/ssl/OpenSSLTransportCertificate.h>
#include <folly/io/async/ssl/OpenSSLUtils.h>

namespace apache::thrift::fast_thrift::thrift::server::compatibility::detail {

std::string peerCommonName(
    const folly::AsyncTransportCertificate* certificate) {
  const auto* openssl =
      dynamic_cast<const folly::OpenSSLTransportCertificate*>(certificate);
  return openssl
      ? folly::ssl::OpenSSLUtils::getCommonName(openssl->getX509().get())
      : "";
}

const folly::F14NodeMap<std::string, std::string>& emptyHeaders() {
  static const folly::Indestructible<
      folly::F14NodeMap<std::string, std::string>>
      headers;
  return *headers;
}

} // namespace
  // apache::thrift::fast_thrift::thrift::server::compatibility::detail
