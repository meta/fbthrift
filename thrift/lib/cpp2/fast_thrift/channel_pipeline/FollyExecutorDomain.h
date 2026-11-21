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

#include <folly/Executor.h>

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/ExecutionDomain.h>

namespace apache::thrift::fast_thrift::channel_pipeline {

/** An execution domain that folly coroutines can run on. */
template <typename Domain>
concept FollyExecutorDomain =
    ExecutionDomain<Domain> && requires(const Domain& constDomain) {
      // Stable for the domain's lifetime; equal domains return the same
      // executor. Work added through it runs in the same serialized domain but
      // is not ordered relative to post().
      { constDomain.getExecutor() } noexcept -> std::same_as<folly::Executor*>;
    };

} // namespace apache::thrift::fast_thrift::channel_pipeline
