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

#include <folly/lang/Bits.h>
#include <thrift/lib/cpp2/security/extensions/ThriftParametersServerExtension.h>

namespace apache::thrift {

namespace detail {
THRIFT_PLUGGABLE_FUNC_REGISTER(
    void,
    onThriftPspEvent,
    PspEvent,
    const folly::Optional<folly::exception_wrapper>&) {
  return;
}

namespace {
PspEvent classifyNegotiation(uint64_t serverSupports, uint64_t clientSupports) {
  if ((serverSupports & clientSupports) != 0) {
    return PspEvent::NegotiationSuccess;
  }
  if (serverSupports == 0 && clientSupports == 0) {
    return PspEvent::NegotiationNoOffers;
  }
  if (clientSupports == 0) {
    return PspEvent::NegotiationNoPeerOffer;
  }
  if (serverSupports == 0) {
    return PspEvent::NegotiationNoServerOffer;
  }
  return PspEvent::NegotiationNoCommonVersion;
}
} // namespace
} // namespace detail

NegotiationParameters ThriftParametersServerExtension::negotiate(
    const ThriftParametersExt& clientParameters,
    const ThriftParametersContext& serverConfiguration) {
  std::uint64_t compressionAlgorithms = 0;
  for (const auto& comp :
       serverConfiguration.getSupportedCompressionAlgorithms()) {
    assert(comp != CompressionAlgorithm::NONE);
    compressionAlgorithms |= 1ull << (int(comp) - 1);
  }

  uint64_t serverPSP = [&] {
    const auto& clientExtension = clientParameters.params;
    uint64_t clientSupports = 0;
    if (auto clientPSP =
            apache::thrift::get_pointer(clientExtension.pspUpgradeProtocol())) {
      clientSupports |= *clientPSP;
    }

    uint64_t serverSupports = serverConfiguration.getSupportedPSPNegotiations();
    detail::onThriftPspEvent(
        detail::classifyNegotiation(serverSupports, clientSupports),
        folly::none);
    uint64_t common = clientSupports & serverSupports;
    auto highestBit = folly::findLastSet(common);
    if (highestBit > 0) {
      return uint64_t(1) << (highestBit - 1);
    } else {
      return uint64_t(0);
    }
  }();

  NegotiationParameters negotiatedParams;
  negotiatedParams.compressionAlgos() = compressionAlgorithms;
  negotiatedParams.useStopTLS() = serverConfiguration.getUseStopTLS();
  negotiatedParams.useStopTLSV2() = serverConfiguration.getUseStopTLSV2();
  negotiatedParams.useStopTLSForTTLSTunnel() =
      serverConfiguration.getUseStopTLSForTTLSTunnel();
  negotiatedParams.pspUpgradeProtocol() = serverPSP;
  return negotiatedParams;
}
} // namespace apache::thrift
