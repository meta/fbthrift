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

#include <thrift/lib/cpp2/fast_thrift/thrift/server/util/RocketFrameDecoder.h>

#include <fmt/core.h>

#include <thrift/lib/cpp/TApplicationException.h>
#include <thrift/lib/cpp2/fast_thrift/frame/FrameType.h>
#include <thrift/lib/cpp2/fast_thrift/frame/read/FrameViews.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/SetupResponseBuilder.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/util/RequestMetadata.h>
#include <thrift/lib/thrift/gen-cpp2/RpcMetadata_types.h>

namespace apache::thrift::fast_thrift::thrift {

folly::Expected<ThriftServerInboundPayloadVariant, folly::exception_wrapper>
fromRocketFrame(
    frame::read::ParsedFrame&& frame,
    rocket::server::MetadataProtocol metadataProtocol,
    folly::EventBase& eventBase) {
  using frame::FrameType;

  switch (frame.type()) {
    case FrameType::REQUEST_RESPONSE: {
      auto metadata = makeServerRequestMetadata(eventBase);
      if (auto ew =
              deserializeRequestMetadata(frame, *metadata, metadataProtocol)) {
        return folly::makeUnexpected(std::move(ew));
      }
      auto data = std::move(frame).extractData();
      return ThriftServerInboundPayloadVariant{
          ThriftServerRequestResponsePayload{
              .data = std::move(data), .metadata = std::move(metadata)}};
    }

    case FrameType::SETUP: {
      const frame::read::SetupView view(frame);
      rocket::server::SetupParameters rocketSetup{
          .majorVersion = view.majorVersion(),
          .minorVersion = view.minorVersion(),
          .keepaliveTime = view.keepaliveTime(),
          .maxLifetime = view.maxLifetime(),
          .hasLease = view.hasLease(),
          .metadataProtocol = metadataProtocol,
      };
      auto clientSetup = parseSetupMetadata(
          std::move(frame).extractMetadata(), metadataProtocol);
      if (!clientSetup.hasValue()) {
        return folly::makeUnexpected(
            folly::make_exception_wrapper<
                apache::thrift::TApplicationException>(
                "Failed to deserialize the client's SETUP metadata"));
      }
      auto setup = std::make_unique<ConnectionSetupData>();
      setup->clientSetup = std::move(clientSetup).value();
      setup->rocketSetup = rocketSetup;
      return ThriftServerInboundPayloadVariant{
          ThriftConnectionSetupPayload{.setup = std::move(setup)}};
    }

    case FrameType::REQUEST_N: {
      const auto n =
          frame::read::asView<frame::read::RequestNView>(frame).requestN();
      return ThriftServerInboundPayloadVariant{
          ThriftRequestNPayload{.streamId = frame.streamId(), .requestN = n}};
    }

    case FrameType::CANCEL:
      return ThriftServerInboundPayloadVariant{
          ThriftRequestCancellationPayload{}};

    case FrameType::REQUEST_STREAM: {
      const auto initialRequestN =
          frame::read::asView<frame::read::RequestStreamView>(frame)
              .initialRequestN();
      auto metadata = std::make_unique<apache::thrift::RequestRpcMetadata>();
      if (auto ew =
              deserializeRequestMetadata(frame, *metadata, metadataProtocol)) {
        return folly::makeUnexpected(std::move(ew));
      }
      auto data = std::move(frame).extractData();
      return ThriftServerInboundPayloadVariant{ThriftRequestStreamPayload{
          .data = std::move(data),
          .metadata = std::move(metadata),
          .initialRequestN = initialRequestN}};
    }

    case FrameType::REQUEST_FNF:
    case FrameType::REQUEST_CHANNEL:
      return folly::makeUnexpected(
          folly::make_exception_wrapper<apache::thrift::TApplicationException>(
              fmt::format(
                  "fromRocketFrame: frame type {} not yet wired",
                  static_cast<int>(frame.type()))));

    default:
      return folly::makeUnexpected(
          folly::make_exception_wrapper<apache::thrift::TApplicationException>(
              fmt::format(
                  "fromRocketFrame: unexpected inbound frame type {}",
                  static_cast<int>(frame.type()))));
  }
}

} // namespace apache::thrift::fast_thrift::thrift
