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

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/ExecutionDomain.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/HandlerTag.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineFactory.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineInstance.h>

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
  using Spec = pipeline_instance::HandlerSpec<H, Id>;

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

// Internal tag for PipelineSpec's rebind constructor (the accumulating add<H>
// chain constructs a sibling Spec instantiation through it).
struct SpecRebindTag {};

} // namespace detail

/**
 * Immutable description of a pipeline's shape, parameterized by its
 * TaskState and execution-domain types. Handlers are accumulated head-to-tail
 * by add<H>(); the first is the head, the last the tail, positionally. The
 * accumulated Entry list is carried by `auto`, so the only handwritten type is
 * PipelineSpec<TaskState, Domain>.
 *
 *   auto spec = PipelineSpec<Rocket, InlineExecution>{}
 *       .add<TransportHandler>(transport_tag)   // first = head
 *       .add<FramingHandler>(framing_tag)
 *       .add<RequestHandler>(request_tag);      // last = tail
 *   PipelineInstance::Ptr conn = spec.build(Rocket{.socket = sock},
 * InlineExecution{});
 *
 * build() is const and reusable: the blueprint is copied and consumed into a
 * fresh pipeline each call, which is why handler args must be copyable.
 */
template <typename TaskState, ExecutionDomain Domain, typename... Entry>
class PipelineSpec {
 public:
  PipelineSpec() = default;

  // Internal: used by add<H> to transfer the accumulated blueprint into a
  // sibling instantiation. The detail-namespace tag keeps it out of reach.
  PipelineSpec(detail::SpecRebindTag, std::tuple<Entry...>&& factories) noexcept
      : factories_(std::move(factories)) {}

  /**
   * Append the next handler in head-to-tail order; returns a rebound spec
   * carrying the extended Entry list (use fluently). Handler args must be
   * copyable so the spec can reproduce the handler on each build(); pass
   * per-connection or move-only state via the TaskState instead. The handler
   * need only satisfy one full direction (inbound, outbound, or duplex);
   * dispatch skips the unimplemented direction at compile time.
   */
  template <typename H, HandlerId Id, typename... Args>
  auto add(HandlerTag<Id>, Args&&... args) && {
    static_assert(
        (std::is_copy_constructible_v<std::decay_t<Args>> && ...),
        "handler args must be copyable so the spec can reproduce the handler "
        "on each build(); pass per-connection or move-only state via the "
        "TaskState instead");
    using NewEntry = detail::SpecHandlerFactory<H, Id, std::decay_t<Args>...>;
    return PipelineSpec<TaskState, Domain, Entry..., NewEntry>(
        detail::SpecRebindTag{},
        std::tuple_cat(
            std::move(factories_),
            std::tuple<NewEntry>(NewEntry(std::forward<Args>(args)...))));
  }

  /**
   * Instantiate a pipeline and return the erased owning handle. const and
   * reusable: the blueprint is copied and consumed into a fresh pipeline.
   * State and execution-domain values are supplied per instance. Both
   * outlive the handler objects during teardown.
   */
  PipelineInstance::Ptr build(TaskState taskState, Domain domain) const {
    static_assert(
        sizeof...(Entry) >= 1, "a pipeline needs at least one handler");
    using ImplT =
        PipelineInstance::Impl<TaskState, Domain, typename Entry::Spec...>;
    auto blueprint = factories_;
    typename ImplT::Ptr pipeline(
        new ImplT(std::move(taskState), std::move(domain), blueprint));
    pipeline->finishBuild();
    return PipelineInstance::Ptr(std::move(pipeline));
  }

  /**
   * Erase the handler composition into a reusable PipelineFactory (copies the
   * spec into the factory's create thunk), keeping the state and domain types.
   */
  PipelineFactory<TaskState, Domain> factory() const& {
    static_assert(
        sizeof...(Entry) >= 1, "a pipeline needs at least one handler");
    return PipelineFactory<TaskState, Domain>(*this);
  }

 private:
  std::tuple<Entry...> factories_;
};

} // namespace apache::thrift::fast_thrift::channel_pipeline
