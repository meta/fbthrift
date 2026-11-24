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

#include <utility>

#include <folly/Function.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/BufferAllocator.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineRef.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/StaticPipelineBuilder.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/stream/handler/ProducerHeadAdapter.h>
#include <thrift/lib/cpp2/fast_thrift/thrift/stream/handler/ProducerTailAdapter.h>

namespace apache::thrift::fast_thrift::thrift::stream {

class ProducerPipeline {
 public:
  using ConfigFunc =
      folly::Function<ProducerPipeline(folly::EventBase*, StreamSink)>;

  template <typename Compose>
  static ProducerPipeline build(
      folly::EventBase* evb, StreamSink sink, Compose&& compose) {
    auto builder = channel_pipeline::StaticPipelineBuilder<
                       ProducerHeadAdapter,
                       ProducerTailAdapter,
                       channel_pipeline::SimpleBufferAllocator>()
                       .setEventBase(evb)
                       .emplaceHead(std::move(sink))
                       .emplaceTail()
                       .emplaceAllocator();
    auto configured = std::forward<Compose>(compose)(std::move(builder));
    return ProducerPipeline(
        channel_pipeline::PipelineOwner(std::move(configured).build()));
  }

  channel_pipeline::Result fireRead(
      channel_pipeline::TypeErasedBox&& msg) noexcept {
    return pipeline_->fireRead(std::move(msg));
  }

 private:
  explicit ProducerPipeline(channel_pipeline::PipelineOwner pipeline) noexcept
      : pipeline_(std::move(pipeline)) {}

  channel_pipeline::PipelineOwner pipeline_;
};

} // namespace apache::thrift::fast_thrift::thrift::stream
