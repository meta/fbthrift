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
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/EvbExecutionDomain.h>

#include <array>
#include <concepts>
#include <cstddef>
#include <type_traits>
#include <utility>

namespace apache::thrift::fast_thrift::channel_pipeline {

namespace detail {
class ContextImpl;
} // namespace detail

/**
 * Base for a pipeline event tag. Each event derives from its own EventTag
 * specialization and names the payload delivered to subscribers. Use void for
 * a signal with no payload.
 */
template <typename PayloadT = void>
struct EventTag {
  using Payload = PayloadT;
};

/** A distinct type deriving from EventTag<Payload>. */
template <typename E>
concept PipelineEvent = requires { typename E::Payload; } &&
    std::derived_from<E, EventTag<typename E::Payload>> &&
    (!std::same_as<E, EventTag<typename E::Payload>>);

/**
 * Compile-time set used by handlers as PublishedEvents or SubscribedEvents.
 * fire() is constrained to members of the set, making an undeclared publish a
 * compile error when publishers fire through their own PublishedEvents alias
 * and context. The event's position in this set selects its cached route.
 */
template <PipelineEvent... Evs>
struct Events {
 private:
  template <PipelineEvent E>
  static consteval std::size_t indexOf() {
    constexpr std::array matches{std::same_as<E, Evs>...};
    for (std::size_t i = 0; i < matches.size(); ++i) {
      if (matches[i]) {
        return i;
      }
    }
    return 0;
  }

 public:
  template <PipelineEvent E>
    requires((std::same_as<E, Evs> || ...))
  static constexpr std::size_t index = indexOf<E>();

  template <PipelineEvent E, typename Target, typename... Args>
    requires((std::same_as<E, Evs> || ...))
  static void fire(Target& target, Args&&... args) noexcept {
    if constexpr (requires {
                    target.template firePublishedEvent<E, index<E>>(
                        std::forward<Args>(args)...);
                  }) {
      target.template firePublishedEvent<E, index<E>>(
          std::forward<Args>(args)...);
    } else {
      target.template fireEvent<E>(std::forward<Args>(args)...);
    }
  }
};

template <typename T>
inline constexpr bool kIsEventSet = false;

template <PipelineEvent... Evs>
inline constexpr bool kIsEventSet<Events<Evs...>> = true;

/**
 * Process-local identity for an event type. Inline variable-template address
 * identity is collision-free and avoids maintaining a central numeric id
 * catalog. A PipelineImpl maps these keys to pipeline-local subscriber slots.
 */
struct alignas(8) EventTypeToken {};
using EventKey = const EventTypeToken*;

/** Opaque pipeline-local route for a bound type event. */
struct BoundEventRoute {
  const void* value{nullptr};
};

template <PipelineEvent E>
inline constexpr EventTypeToken kEventTypeToken{};

template <PipelineEvent E>
constexpr EventKey eventKey() noexcept {
  return &kEventTypeToken<E>;
}

using TypeEventDispatchFn = void (*)(
    void* target, detail::ContextImpl* ctx, const void* payload) noexcept;

/** A type-based event subscriber's pipeline-owned dispatch entry. */
struct TypeEventDispatch {
  TypeEventDispatchFn fn{nullptr};
  void* target{nullptr};
  detail::ContextImpl* ctx{nullptr};
};

struct TypeEventSubscription {
  EventKey key{nullptr};
  TypeEventDispatchFn thunk{nullptr};
};

template <ExecutionDomain D>
using TypeEventDispatchFnT = TypeEventDispatchFn;
template <ExecutionDomain D>
using TypeEventDispatchT = TypeEventDispatch;
template <ExecutionDomain D>
using TypeEventSubscriptionT = TypeEventSubscription;

template <PipelineEvent... Evs>
constexpr std::array<EventKey, sizeof...(Evs)> eventKeys(Events<Evs...>) {
  return {eventKey<Evs>()...};
}

} // namespace apache::thrift::fast_thrift::channel_pipeline
