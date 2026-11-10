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

#include <thrift/lib/cpp2/fast_thrift/thrift/server/FastThriftServer.h>

#include <functional>
#include <utility>

#include <thrift/lib/cpp2/fast_thrift/thrift/server/ThriftServerConnectionFactory.h>

namespace apache::thrift::fast_thrift::thrift {

template <ChannelPipelineMode Mode>
void detail::FastThriftServerBase::FastThriftServerT<Mode>::
    installConnectionFactory(
        connection::ConnectionManager& manager,
        server::ThriftServerConnectionFactoryConfig config,
        std::function<void(server::ThriftServerConnection&)> onAccept) {
  manager.setConnectionFactory(
      server::detail::ThriftServerConnectionFactoryT<Mode>(std::move(config)),
      std::move(onAccept));
}

template class detail::FastThriftServerBase::FastThriftServerT<
    ChannelPipelineMode::Static>;
template class detail::FastThriftServerBase::FastThriftServerT<
    ChannelPipelineMode::Dynamic>;

FastThriftServer::FastThriftServer(FastThriftServerConfig config)
    : detail::FastThriftServerBase::FastThriftServerT<
          ChannelPipelineMode::Static>(std::move(config)) {}

DynamicFastThriftServer::DynamicFastThriftServer(FastThriftServerConfig config)
    : detail::FastThriftServerBase::FastThriftServerT<
          ChannelPipelineMode::Dynamic>(std::move(config)) {}

} // namespace apache::thrift::fast_thrift::thrift
