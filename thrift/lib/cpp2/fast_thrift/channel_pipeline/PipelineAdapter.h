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

#include <concepts>
#include <utility>

namespace apache::thrift::fast_thrift::channel_pipeline {

// An adapter owns continuation past one end of a pipeline. Backpressure means
// the message was accepted. An adapter that queues delivery returns acceptance,
// rather than the eventual result from the destination.
template <typename A>
concept ReadAdapter = std::move_constructible<A> &&
    requires(A& adapter, TypeErasedBox&& message) {
      {
        adapter.adaptRead(std::move(message))
      } noexcept -> std::same_as<Result>;
    };

template <typename A>
concept WriteAdapter = std::move_constructible<A> &&
    requires(A& adapter, TypeErasedBox&& message) {
      {
        adapter.adaptWrite(std::move(message))
      } noexcept -> std::same_as<Result>;
    };

namespace detail::pipeline_instance {

class AdapterStorage {
 public:
  AdapterStorage() noexcept = default;
  AdapterStorage(const AdapterStorage&) = delete;
  AdapterStorage& operator=(const AdapterStorage&) = delete;

  AdapterStorage(AdapterStorage&& other) noexcept
      : object_(std::exchange(other.object_, nullptr)),
        ops_(std::exchange(other.ops_, nullptr)) {}

  AdapterStorage& operator=(AdapterStorage&& other) noexcept {
    AdapterStorage replacement(std::move(other));
    std::swap(object_, replacement.object_);
    std::swap(ops_, replacement.ops_);
    return *this;
  }

  ~AdapterStorage() {
    if (object_ != nullptr) {
      ops_->destroy(object_);
    }
  }

  template <ReadAdapter A>
  static AdapterStorage forRead(A adapter) {
    return AdapterStorage(new A(std::move(adapter)), &opsFor<A, true>());
  }

  template <WriteAdapter A>
  static AdapterStorage forWrite(A adapter) {
    return AdapterStorage(new A(std::move(adapter)), &opsFor<A, false>());
  }

  Result invoke(TypeErasedBox&& message) noexcept {
    return object_ != nullptr ? ops_->invoke(object_, std::move(message))
                              : Result::Error;
  }

 private:
  struct Ops {
    Result (*invoke)(void*, TypeErasedBox&&) noexcept;
    void (*destroy)(void*) noexcept;
  };

  AdapterStorage(void* object, const Ops* ops) noexcept
      : object_(object), ops_(ops) {}

  template <typename A, bool Read>
  static const Ops& opsFor() noexcept {
    static const Ops ops{
        .invoke =
            +[](void* object, TypeErasedBox&& message) noexcept {
              auto& adapter = *static_cast<A*>(object);
              if constexpr (Read) {
                return adapter.adaptRead(std::move(message));
              } else {
                return adapter.adaptWrite(std::move(message));
              }
            },
        .destroy =
            +[](void* object) noexcept { delete static_cast<A*>(object); },
    };
    return ops;
  }

  void* object_{nullptr};
  const Ops* ops_{nullptr};
};

} // namespace detail::pipeline_instance
} // namespace apache::thrift::fast_thrift::channel_pipeline
