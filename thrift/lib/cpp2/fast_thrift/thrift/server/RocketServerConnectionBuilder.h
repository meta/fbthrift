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
#include <optional>
#include <variant>

#include <folly/CppAttributes.h>
#include <folly/Function.h>
#include <folly/SocketAddress.h>
#include <folly/io/async/AsyncTransport.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/ErasedStaticHandler.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/HandlerNode.h>
#include <thrift/lib/cpp2/fast_thrift/common/ServerStats.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/server/common/RocketServerConnection.h>

namespace apache::thrift::fast_thrift::thrift::server {

struct RocketServerConnectionBuildResult {
  std::unique_ptr<rocket::server::RocketServerConnection> connection;
  std::optional<std::variant<
      channel_pipeline::detail::ErasedStaticHandler,
      channel_pipeline::detail::HandlerNode>>
      transportNegotiationHandler;
};

using StaticRocketServerConnectionBuilder =
    folly::Function<std::unique_ptr<rocket::server::RocketServerConnection>(
        folly::AsyncTransport::UniquePtr,
        const folly::SocketAddress&,
        ServerStatsShard* FOLLY_NULLABLE)>;

using RocketServerConnectionBuilder =
    folly::Function<RocketServerConnectionBuildResult(
        folly::AsyncTransport::UniquePtr,
        const folly::SocketAddress&,
        ServerStatsShard* FOLLY_NULLABLE)>;

} // namespace apache::thrift::fast_thrift::thrift::server
