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

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/StaticContext.h>

#include <concepts>
#include <cstddef>
#include <tuple>
#include <utility>

namespace apache::thrift::fast_thrift::channel_pipeline::detail {

/** Inline handler and context storage for a compile-time-shaped pipeline. */
template <
    typename H,
    HandlerId Id,
    typename Pipeline,
    std::size_t Index,
    typename StateTuple = std::tuple<>>
class StaticHandler {
 public:
  using Handler = H;
  using Context = StaticContext<Pipeline, Index, Id, StateTuple>;
  static constexpr HandlerId id = Id;

  template <typename... Args>
    requires std::constructible_from<H, Args...>
  explicit StaticHandler(Pipeline* pipeline, Args&&... args)
      : handler_(std::forward<Args>(args)...), context_(pipeline) {}

  H& handler() noexcept { return handler_; }
  const H& handler() const noexcept { return handler_; }
  Context& context() noexcept { return context_; }
  const Context& context() const noexcept { return context_; }

 private:
  [[no_unique_address]] H handler_;
  Context context_;
};

} // namespace apache::thrift::fast_thrift::channel_pipeline::detail
