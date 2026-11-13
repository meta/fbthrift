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

#include <exception>
#include <stdexcept>
#include <utility>

#include <folly/ExceptionWrapper.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Handler.h>
#include <thrift/lib/cpp2/fast_thrift/frame/FrameType.h>
#include <thrift/lib/cpp2/fast_thrift/frame/read/ParsedFrame.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/client/Event.h>
#include <thrift/lib/cpp2/fast_thrift/rocket/client/Messages.h>

namespace apache::thrift::fast_thrift::rocket::client::handler {

class RocketClientMetadataPushHandler {
 public:
  using PublishedEvents = channel_pipeline::Events<MetadataPushEvent>;

  template <typename Context>
  void handlerAdded(Context&) noexcept {}

  template <typename Context>
  void handlerRemoved(Context&) noexcept {}

  template <typename Context>
  void onPipelineActive(Context&) noexcept {}

  template <typename Context>
  void onReadReady(Context&) noexcept {}

  template <typename Context>
  channel_pipeline::Result onRead(
      Context& ctx, channel_pipeline::TypeErasedBox&& msg) noexcept {
    auto& response = msg.get<RocketResponseMessage>();
    if (!response.payload.is<frame::read::ParsedFrame>()) {
      return ctx.fireRead(std::move(msg));
    }

    auto& parsed = response.payload.get<frame::read::ParsedFrame>();
    if (parsed.type() != frame::FrameType::METADATA_PUSH) {
      return ctx.fireRead(std::move(msg));
    }
    if (!parsed.isConnectionFrame()) {
      ctx.fireException(
          folly::make_exception_wrapper<std::runtime_error>(
              "METADATA_PUSH frame must use streamId 0"));
      return channel_pipeline::Result::Error;
    }

    try {
      RocketMetadataPush event{
          .metadata = std::move(parsed).extractMetadata(),
      };
      PublishedEvents::template fire<MetadataPushEvent>(ctx, &event);
      if (event.result == channel_pipeline::Result::Error && event.error) {
        ctx.fireException(std::move(event.error));
      }
      return event.result;
    } catch (...) {
      ctx.fireException(folly::exception_wrapper(std::current_exception()));
      return channel_pipeline::Result::Error;
    }
  }

  template <typename Context>
  void onException(Context& ctx, folly::exception_wrapper&& e) noexcept {
    ctx.fireException(std::move(e));
  }
};

static_assert(
    channel_pipeline::TypeEventPublisher<RocketClientMetadataPushHandler>);

} // namespace apache::thrift::fast_thrift::rocket::client::handler
