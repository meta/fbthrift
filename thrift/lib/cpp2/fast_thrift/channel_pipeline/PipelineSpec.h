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

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/BufferAllocator.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/ExecutionDomain.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/HandlerTag.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineRef.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/StaticPipeline.h>

#include <concepts>
#include <tuple>
#include <type_traits>
#include <utility>

namespace apache::thrift::fast_thrift::channel_pipeline {

namespace detail {

/**
 * Copyable handler blueprint, the accumulated entry in a PipelineSpec: holds a
 * handler's ctor args so a reusable spec can reproduce the handler on every
 * build(). The args must be copyable (enforced at add<H>); per-connection or
 * move-only state belongs on the TaskState instead. Consumed via operator H()
 * when a handler slot is constructed from a copy of the blueprint.
 */
template <typename H, HandlerId Id, typename... Args>
class SpecHandlerFactory {
 public:
  using Spec = StaticHandlerSpec<H, Id, StaticHandlerDirection::Automatic>;

  template <typename... Input>
    requires(sizeof...(Input) == sizeof...(Args))
  explicit SpecHandlerFactory(Input&&... args)
      : args_(std::forward<Input>(args)...) {}

  operator H() && {
    return std::apply(
        []<typename... StoredArgs>(StoredArgs&&... args) -> H {
          return H(std::forward<StoredArgs>(args)...);
        },
        std::move(args_));
  }

 private:
  std::tuple<Args...> args_;
};

struct SpecRebindTag {};
struct EmptyTaskState {};

} // namespace detail

/**
 * Handlers accumulate head-to-tail in the spec's type. State and allocator must
 * be configured before the first handler is added. The execution domain is
 * selected for each build.
 *
 *   auto spec = PipelineSpec()
 *       .state(Rocket{.socket = sock})
 *       .add<TransportHandler>(transport_tag)
 *       .add<FramingHandler>(framing_tag)
 *       .add<RequestHandler>(request_tag);
 *   PipelineOwner conn = std::move(spec).build(InlineExecution{});
 *
 * An rvalue build consumes the configured state and allocator. A const build
 * copies them into each instance and therefore requires copyable resources.
 * Both builds move the supplied execution domain into the pipeline; build()
 * defaults to InlineExecution.
 * Handler constructor arguments must be copyable for reusable specs.
 */
template <
    typename TaskState = detail::EmptyTaskState,
    typename Allocator = detail::NoSpecAllocator,
    typename... Entry>
class PipelineSpec {
  template <typename, typename, typename...>
  friend class PipelineSpec;

 public:
  PipelineSpec() = default;

  template <typename NewTaskState>
    requires(sizeof...(Entry) == 0 && std::move_constructible<NewTaskState>)
  auto state(NewTaskState state) && {
    return PipelineSpec<NewTaskState, Allocator>(
        detail::SpecRebindTag{}, std::move(state), std::move(allocator_), {});
  }

  // The allocator is owned by each pipeline and outlives its state and
  // handlers.
  template <BufferAllocator NewAllocator>
    requires(sizeof...(Entry) == 0 && std::move_constructible<NewAllocator>)
  auto allocator(NewAllocator allocator) && {
    return PipelineSpec<TaskState, NewAllocator>(
        detail::SpecRebindTag{},
        std::move(taskState_),
        std::move(allocator),
        {});
  }

  // Handlers may implement inbound, outbound, or both directions; dispatch
  // skips the unimplemented direction at compile time.
  template <typename H, HandlerId Id, typename... Args>
  auto add(HandlerTag<Id>, Args&&... args) && {
    static_assert(
        (std::is_copy_constructible_v<std::decay_t<Args>> && ...),
        "handler args must be copyable so the spec can reproduce the handler "
        "on each build(); pass move-only state via state() instead");
    using NewEntry = detail::SpecHandlerFactory<H, Id, std::decay_t<Args>...>;
    return PipelineSpec<TaskState, Allocator, Entry..., NewEntry>(
        detail::SpecRebindTag{},
        std::move(taskState_),
        std::move(allocator_),
        std::tuple_cat(
            std::move(factories_),
            std::tuple<NewEntry>(NewEntry(std::forward<Args>(args)...))));
  }

  template <ExecutionDomain Domain = InlineExecution>
    requires(sizeof...(Entry) >= 1)
  PipelineOwner build(Domain domain = {}) && {
    using ImplT = detail::StaticPipelineImpl<
        detail::NoStaticPipelineEndpoint,
        detail::NoStaticPipelineEndpoint,
        detail::NoStaticPipelineEndpoint,
        std::tuple<>,
        detail::NoErasedStaticHandlers,
        detail::SpecPipelinePolicy<TaskState, Domain, Allocator>,
        typename Entry::Spec...>;
    typename ImplT::Ptr pipeline(new ImplT(
        typename ImplT::Resources(
            std::move(taskState_), std::move(domain), std::move(allocator_)),
        factories_));
    pipeline->finishBuild();
    return PipelineOwner(std::move(pipeline));
  }

  template <ExecutionDomain Domain = InlineExecution>
    requires(
        sizeof...(Entry) >= 1 && std::copy_constructible<TaskState> &&
        std::copy_constructible<Allocator>)
  PipelineOwner build(Domain domain = {}) const& {
    auto copy = *this;
    return std::move(copy).build(std::move(domain));
  }

 private:
  PipelineSpec(
      detail::SpecRebindTag,
      TaskState state,
      Allocator allocator,
      std::tuple<Entry...>&& factories)
      : allocator_(std::move(allocator)),
        taskState_(std::move(state)),
        factories_(std::move(factories)) {}

  [[no_unique_address]] Allocator allocator_;
  [[no_unique_address]] TaskState taskState_;
  std::tuple<Entry...> factories_;
};

} // namespace apache::thrift::fast_thrift::channel_pipeline
