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

#include <functional>
#include <utility>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/StaticHandlerContext.h>

namespace apache::thrift::fast_thrift::thrift::server::test {

// For direct endpoint calls whose implementation doesn't use the context.
inline channel_pipeline::detail::StaticHandlerContext&
inertStaticEndpointContext() noexcept {
  /* library-local */ static channel_pipeline::detail::StaticHandlerContext
      context;
  return context;
}

class WriteCaptureHandler {
 public:
  using Callback = std::function<channel_pipeline::Result(
      channel_pipeline::TypeErasedBox&&)>;

  explicit WriteCaptureHandler(Callback callback)
      : callback_(std::move(callback)) {}

  template <typename Context>
  channel_pipeline::Result onWrite(
      Context& ctx, channel_pipeline::TypeErasedBox&& msg) noexcept {
    if (callback_) {
      return callback_(std::move(msg));
    }
    return ctx.fireWrite(std::move(msg));
  }

  template <typename Context>
  void onPipelineInactive(Context&) noexcept {}
  template <typename Context>
  void onWriteReady(Context&) noexcept {}
  template <typename Context>
  void onReadReady(Context&) noexcept {}
  template <typename Context>
  void handlerAdded(Context&) noexcept {}
  template <typename Context>
  void handlerRemoved(Context&) noexcept {}

 private:
  Callback callback_;
};

} // namespace apache::thrift::fast_thrift::thrift::server::test
