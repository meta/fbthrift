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

#include <folly/Function.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/ExecutionDomain.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineInstance.h>

#include <concepts>
#include <utility>

namespace apache::thrift::fast_thrift::channel_pipeline {

/**
 * Reusable producer retaining the TaskState and execution-domain types while
 * erasing the handler composition. The create thunk owns a copy of the spec.
 * Factories are move-only; create() is const and reusable, with state and
 * execution-domain values supplied independently for each instance.
 */
template <typename TaskState, ExecutionDomain Domain>
class PipelineFactory {
 public:
  PipelineFactory() = default;

  // Construct from a spec with the same state and domain types; the spec is
  // copied into the create thunk. The constraint keeps this from shadowing the
  // move constructor.
  template <typename SpecT>
    requires(!std::same_as<std::decay_t<SpecT>, PipelineFactory>)
  explicit PipelineFactory(SpecT spec)
      : create_([spec = std::move(spec)](TaskState taskState, Domain domain) {
          return spec.build(std::move(taskState), std::move(domain));
        }) {}

  PipelineInstance::Ptr create(TaskState taskState, Domain domain) const {
    return create_(std::move(taskState), std::move(domain));
  }

  explicit operator bool() const noexcept { return static_cast<bool>(create_); }

 private:
  folly::Function<PipelineInstance::Ptr(TaskState, Domain) const> create_;
};

} // namespace apache::thrift::fast_thrift::channel_pipeline
