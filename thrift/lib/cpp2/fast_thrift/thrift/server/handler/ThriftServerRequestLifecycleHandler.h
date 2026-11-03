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

#include <concepts>
#include <cstdint>
#include <utility>

#include <folly/CancellationToken.h>
#include <folly/ExceptionWrapper.h>
#include <folly/Likely.h>
#include <folly/container/F14Map.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Event.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/TypeErasedBox.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/Event.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/server/common/Messages.h>

namespace apache::thrift::fast_thrift::thrift {

/**
 * Owns the EventBase-serialized cancel/response race for server requests.
 *
 * It sits immediately before the application tail. A normal response claims
 * completion and continues toward the wire. A response arriving after CANCEL
 * is consumed here and converted into RequestCompleted, allowing upstream
 * bookkeeping to retire without asking Rocket to write to an erased stream.
 */
template <typename Context>
class ThriftServerRequestLifecycleHandler {
 public:
  using PublishedEvents =
      channel_pipeline::Events<ThriftServerRequestCompletedEvent>;
  using SubscribedEvents = channel_pipeline::Events<
      ThriftServerRequestCancellationEvent,
      ThriftServerRequestCompletedEvent>;

  void handlerAdded(Context&) noexcept {}
  void handlerRemoved(Context&) noexcept { requests_.clear(); }
  void onPipelineActive(Context&) noexcept {}
  void onReadReady(Context&) noexcept {}
  void onWriteReady(Context&) noexcept {}

  channel_pipeline::Result onRead(
      Context& ctx, channel_pipeline::TypeErasedBox&& msg) noexcept {
    auto& request = msg.template get<ThriftServerRequestMessage>();
    DCHECK(request.requestContext != nullptr);
    const auto streamId = request.streamId;
    auto [it, inserted] =
        requests_.try_emplace(streamId, folly::CancellationSource::invalid());
    if (FOLLY_UNLIKELY(!inserted)) {
      return channel_pipeline::Result::Error;
    }
    request.requestContext->enableCancellation();
    it->second = request.requestContext->getCancellationSource();
    auto result = ctx.fireRead(std::move(msg));
    if (FOLLY_UNLIKELY(result == channel_pipeline::Result::Error)) {
      auto requestIt = requests_.find(streamId);
      if (requestIt != requests_.end()) {
        auto source = std::move(requestIt->second);
        requests_.erase(requestIt);
        source.requestCancellation();
      }
    }
    return result;
  }

  channel_pipeline::Result onWrite(
      Context& ctx, channel_pipeline::TypeErasedBox&& msg) noexcept {
    auto& response = msg.template get<ThriftServerResponseMessage>();
    const auto streamId = responseStreamId(response);
    auto it = requests_.find(streamId);
    if (it != requests_.end()) {
      requests_.erase(it);
    }

    if (response.requestContext != nullptr) {
      if (!response.requestContext->tryComplete()) {
        return channel_pipeline::Result::Success;
      }
      if (response.requestContext->isCancellationRequested()) {
        PublishedEvents::template fire<ThriftServerRequestCompletedEvent>(
            ctx, ThriftServerRequestCompletedEvent{.streamId = streamId});
        return channel_pipeline::Result::Success;
      }
    }
    return ctx.fireWrite(std::move(msg));
  }

  void onException(Context& ctx, folly::exception_wrapper&& e) noexcept {
    ctx.fireException(std::move(e));
  }

  void onPipelineInactive(Context&) noexcept {
    while (!requests_.empty()) {
      auto it = requests_.begin();
      auto source = std::move(it->second);
      requests_.erase(it);
      source.requestCancellation();
    }
  }

  template <channel_pipeline::PipelineEvent E>
    requires std::same_as<E, ThriftServerRequestCancellationEvent>
  void on(
      Context&, const ThriftServerRequestCancellationEvent& event) noexcept {
    auto it = requests_.find(event.streamId);
    if (it != requests_.end()) {
      auto source = it->second;
      source.requestCancellation();
    }
  }

  template <channel_pipeline::PipelineEvent E>
    requires std::same_as<E, ThriftServerRequestCompletedEvent>
  void on(Context&, const ThriftServerRequestCompletedEvent& event) noexcept {
    requests_.erase(event.streamId);
  }

  std::size_t requestCount() const noexcept { return requests_.size(); }

 private:
  static uint32_t responseStreamId(
      const ThriftServerResponseMessage& response) noexcept {
    if (response.payload.template is<ThriftInitialResponsePayload>()) {
      return response.payload.template get<ThriftInitialResponsePayload>()
          .streamId;
    }
    if (response.payload.template is<ThriftErrorPayload>()) {
      return response.payload.template get<ThriftErrorPayload>().streamId;
    }
    return 0;
  }

  folly::F14FastMap<uint32_t, folly::CancellationSource> requests_;
};

} // namespace apache::thrift::fast_thrift::thrift
