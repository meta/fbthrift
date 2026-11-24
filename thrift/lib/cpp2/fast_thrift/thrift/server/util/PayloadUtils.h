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

#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/PayloadVariants.h>

namespace apache::thrift::fast_thrift::thrift {

inline bool isStreamControlRequest(
    const ThriftServerInboundPayloadVariant& payload) noexcept {
  return payload.is<ThriftRequestNPayload>() ||
      payload.is<ThriftRequestCancellationPayload>();
}

inline bool isRequestRetiringResponse(
    const ThriftServerOutboundPayloadVariant& payload) noexcept {
  return payload.is<ThriftInitialResponsePayload>() ||
      payload.is<ThriftStreamInitialResponsePayload>() ||
      payload.is<ThriftErrorPayload>();
}

} // namespace apache::thrift::fast_thrift::thrift
