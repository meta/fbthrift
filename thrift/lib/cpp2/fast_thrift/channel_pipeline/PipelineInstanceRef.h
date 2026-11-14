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

#include <folly/ExceptionWrapper.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Event.h>

#include <array>
#include <concepts>
#include <cstddef>
#include <memory>
#include <type_traits>
#include <utility>

namespace apache::thrift::fast_thrift::channel_pipeline {
class PipelineInstanceOwner;

class PipelineInstanceGuard;

template <typename EventSet>
class PipelineInstanceEventPublisherRef;

template <typename P>
concept PipelineInstanceRefTarget = requires(
    P& pipeline,
    TypeErasedBox&& message,
    folly::exception_wrapper&& exception,
    EventKey eventKey,
    BoundEventRoute route,
    const void* payload) {
  typename P::Guard;
  { pipeline.fireRead(std::move(message)) } noexcept -> std::same_as<Result>;
  { pipeline.fireWrite(std::move(message)) } noexcept -> std::same_as<Result>;
  {
    pipeline.fireException(std::move(exception))
  } noexcept -> std::same_as<void>;
  { pipeline.activate() } noexcept -> std::same_as<void>;
  { pipeline.deactivate() } noexcept -> std::same_as<void>;
  { pipeline.close() } noexcept -> std::same_as<void>;
  { pipeline.onReadReady() } noexcept -> std::same_as<void>;
  { pipeline.onWriteReady() } noexcept -> std::same_as<void>;
  { pipeline.bindEvent(eventKey) } noexcept -> std::same_as<BoundEventRoute>;
  { pipeline.fireBoundEvent(route, payload) } noexcept -> std::same_as<void>;
};

/**
 * Non-owning handle for a PipelineInstance. The target must outlive the
 * handle. A default-constructed or reset handle is empty; check it before
 * invoking an operation. Resources are accessed through the typed TaskState.
 */
class PipelineInstanceRef {
 public:
  PipelineInstanceRef() noexcept = default;

  template <PipelineInstanceRefTarget P>
  explicit PipelineInstanceRef(P& pipeline) noexcept
      : instance_(&pipeline), ops_(&opsFor<P>()) {}

  explicit operator bool() const noexcept { return instance_ != nullptr; }
  friend bool operator==(PipelineInstanceRef, PipelineInstanceRef) noexcept =
      default;

  friend bool operator==(
      PipelineInstanceRef pipeline, std::nullptr_t) noexcept {
    return !pipeline;
  }
  friend bool operator==(
      std::nullptr_t, PipelineInstanceRef pipeline) noexcept {
    return !pipeline;
  }

  void reset() noexcept {
    instance_ = nullptr;
    ops_ = nullptr;
  }

  Result fireRead(TypeErasedBox&& message) const noexcept {
    return ops_->fireRead(instance_, std::move(message));
  }

  Result fireWrite(TypeErasedBox&& message) const noexcept {
    return ops_->fireWrite(instance_, std::move(message));
  }

  void fireException(folly::exception_wrapper&& exception) const noexcept {
    ops_->fireException(instance_, std::move(exception));
  }

  void activate() const noexcept { ops_->activate(instance_); }
  void deactivate() const noexcept { ops_->deactivate(instance_); }
  void close() const noexcept { ops_->close(instance_); }
  void onReadReady() const noexcept { ops_->onReadReady(instance_); }
  void onWriteReady() const noexcept { ops_->onWriteReady(instance_); }

  PipelineInstanceGuard guard() const noexcept;

  template <typename EventSet>
    requires kIsEventSet<EventSet>
  PipelineInstanceEventPublisherRef<EventSet> bindEvents() const noexcept;

 private:
  struct Ops {
    Result (*fireRead)(void*, TypeErasedBox&&) noexcept;
    Result (*fireWrite)(void*, TypeErasedBox&&) noexcept;
    void (*fireException)(void*, folly::exception_wrapper&&) noexcept;
    void (*activate)(void*) noexcept;
    void (*deactivate)(void*) noexcept;
    void (*close)(void*) noexcept;
    void (*onReadReady)(void*) noexcept;
    void (*onWriteReady)(void*) noexcept;
    BoundEventRoute (*bindEvent)(void*, EventKey) noexcept;
    void (*fireBoundEvent)(void*, BoundEventRoute, const void*) noexcept;
    void (*constructGuard)(void*, void*) noexcept;
    void (*moveGuard)(void*, void*) noexcept;
    void (*destroyGuard)(void*) noexcept;
  };

  template <PipelineInstanceRefTarget P>
  static const Ops& opsFor() noexcept {
    using Guard = typename P::Guard;
    static_assert(sizeof(Guard) <= sizeof(void*));
    static_assert(alignof(Guard) <= alignof(void*));
    static const Ops ops{
        .fireRead =
            +[](void* p, TypeErasedBox&& message) noexcept {
              return static_cast<P*>(p)->fireRead(std::move(message));
            },
        .fireWrite =
            +[](void* p, TypeErasedBox&& message) noexcept {
              return static_cast<P*>(p)->fireWrite(std::move(message));
            },
        .fireException =
            +[](void* p, folly::exception_wrapper&& exception) noexcept {
              static_cast<P*>(p)->fireException(std::move(exception));
            },
        .activate = +[](void* p) noexcept { static_cast<P*>(p)->activate(); },
        .deactivate =
            +[](void* p) noexcept { static_cast<P*>(p)->deactivate(); },
        .close = +[](void* p) noexcept { static_cast<P*>(p)->close(); },
        .onReadReady =
            +[](void* p) noexcept { static_cast<P*>(p)->onReadReady(); },
        .onWriteReady =
            +[](void* p) noexcept { static_cast<P*>(p)->onWriteReady(); },
        .bindEvent =
            +[](void* p, EventKey key) noexcept {
              return static_cast<P*>(p)->bindEvent(key);
            },
        .fireBoundEvent =
            +[](void* p, BoundEventRoute route, const void* payload) noexcept {
              static_cast<P*>(p)->fireBoundEvent(route, payload);
            },
        .constructGuard =
            +[](void* storage, void* p) noexcept {
              std::construct_at(
                  static_cast<Guard*>(storage), static_cast<P*>(p));
            },
        .moveGuard =
            +[](void* to, void* from) noexcept {
              auto* source = static_cast<Guard*>(from);
              std::construct_at(static_cast<Guard*>(to), std::move(*source));
              std::destroy_at(source);
            },
        .destroyGuard =
            +[](void* storage) noexcept {
              std::destroy_at(static_cast<Guard*>(storage));
            },
    };
    return ops;
  }

  BoundEventRoute bindEvent(EventKey key) const noexcept {
    return ops_->bindEvent(instance_, key);
  }

  void fireBoundEvent(
      BoundEventRoute route, const void* payload) const noexcept {
    ops_->fireBoundEvent(instance_, route, payload);
  }

  void* instance_{nullptr};
  const Ops* ops_{nullptr};

  friend class PipelineInstanceGuard;
  friend class PipelineInstanceOwner;
  template <typename>
  friend class PipelineInstanceEventPublisherRef;
};

class PipelineInstanceOwner {
 public:
  PipelineInstanceOwner() noexcept = default;
  PipelineInstanceOwner(const PipelineInstanceOwner&) = delete;
  PipelineInstanceOwner& operator=(const PipelineInstanceOwner&) = delete;

  template <PipelineInstanceRefTarget P, typename D>
    requires std::is_empty_v<D> && std::default_initializable<D>
  explicit PipelineInstanceOwner(std::unique_ptr<P, D> pipeline) noexcept {
    if (auto* instance = pipeline.release()) {
      pipeline_ = PipelineInstanceRef(*instance);
      destroy_ = +[](void* p) noexcept { D{}(static_cast<P*>(p)); };
    }
  }

  template <PipelineInstanceRefTarget P, typename D>
    requires std::is_empty_v<D> && std::default_initializable<D>
  PipelineInstanceOwner& operator=(std::unique_ptr<P, D> pipeline) noexcept {
    PipelineInstanceOwner replacement(std::move(pipeline));
    *this = std::move(replacement);
    return *this;
  }

  PipelineInstanceOwner(PipelineInstanceOwner&& other) noexcept
      : pipeline_(std::exchange(other.pipeline_, {})),
        destroy_(std::exchange(other.destroy_, nullptr)) {}

  PipelineInstanceOwner& operator=(PipelineInstanceOwner&& other) noexcept {
    if (this != &other) {
      reset();
      pipeline_ = std::exchange(other.pipeline_, {});
      destroy_ = std::exchange(other.destroy_, nullptr);
    }
    return *this;
  }

  ~PipelineInstanceOwner() { reset(); }

  explicit operator bool() const noexcept {
    return static_cast<bool>(pipeline_);
  }

  const PipelineInstanceRef* operator->() const noexcept { return &pipeline_; }
  PipelineInstanceRef get() const noexcept { return pipeline_; }
  PipelineInstanceRef ref() const noexcept { return pipeline_; }

  void reset() noexcept {
    auto pipeline = std::exchange(pipeline_, {});
    auto* destroy = std::exchange(destroy_, nullptr);
    if (destroy != nullptr) {
      destroy(pipeline.instance_);
    }
  }

 private:
  PipelineInstanceRef pipeline_;
  void (*destroy_)(void*) noexcept {nullptr};
};

static_assert(sizeof(PipelineInstanceOwner) == 3 * sizeof(void*));

class PipelineInstanceGuard {
 public:
  PipelineInstanceGuard() noexcept = default;
  PipelineInstanceGuard(const PipelineInstanceGuard&) = delete;
  PipelineInstanceGuard& operator=(const PipelineInstanceGuard&) = delete;

  PipelineInstanceGuard(PipelineInstanceGuard&& other) noexcept
      : ops_(std::exchange(other.ops_, nullptr)) {
    if (ops_ != nullptr) {
      ops_->moveGuard(&storage_, &other.storage_);
    }
  }

  PipelineInstanceGuard& operator=(PipelineInstanceGuard&& other) noexcept {
    if (this != &other) {
      reset();
      ops_ = std::exchange(other.ops_, nullptr);
      if (ops_ != nullptr) {
        ops_->moveGuard(&storage_, &other.storage_);
      }
    }
    return *this;
  }

  ~PipelineInstanceGuard() { reset(); }

  explicit operator bool() const noexcept { return ops_ != nullptr; }

  void reset() noexcept {
    const auto* ops = std::exchange(ops_, nullptr);
    if (ops != nullptr) {
      ops->destroyGuard(&storage_);
    }
  }

 private:
  explicit PipelineInstanceGuard(const PipelineInstanceRef& pipeline) noexcept
      : ops_(pipeline.ops_) {
    if (ops_ != nullptr) {
      ops_->constructGuard(&storage_, pipeline.instance_);
    }
  }

  alignas(void*) std::byte storage_[sizeof(void*)]{};
  const PipelineInstanceRef::Ops* ops_{nullptr};

  friend class PipelineInstanceRef;
};

inline PipelineInstanceGuard PipelineInstanceRef::guard() const noexcept {
  return PipelineInstanceGuard(*this);
}

template <PipelineEvent... Evs>
class PipelineInstanceEventPublisherRef<Events<Evs...>> {
 public:
  PipelineInstanceEventPublisherRef() noexcept = default;

  template <PipelineEvent E>
    requires(
        (std::same_as<E, Evs> || ...) && std::is_void_v<typename E::Payload>)
  void fire() const noexcept {
    pipeline_.fireBoundEvent(
        routes_[Events<Evs...>::template index<E>], nullptr);
  }

  template <PipelineEvent E>
    requires(
        (std::same_as<E, Evs> || ...) && (!std::is_void_v<typename E::Payload>))
  void fire(const typename E::Payload& payload) const noexcept {
    pipeline_.fireBoundEvent(
        routes_[Events<Evs...>::template index<E>], &payload);
  }

 private:
  explicit PipelineInstanceEventPublisherRef(
      PipelineInstanceRef pipeline) noexcept
      : pipeline_(pipeline), routes_{pipeline.bindEvent(eventKey<Evs>())...} {}

  PipelineInstanceRef pipeline_;
  std::array<BoundEventRoute, sizeof...(Evs)> routes_{};

  friend class PipelineInstanceRef;
};

template <typename EventSet>
  requires kIsEventSet<EventSet>
PipelineInstanceEventPublisherRef<EventSet> PipelineInstanceRef::bindEvents()
    const noexcept {
  return PipelineInstanceEventPublisherRef<EventSet>{*this};
}

static_assert(sizeof(PipelineInstanceGuard) == 2 * sizeof(void*));
static_assert(sizeof(PipelineInstanceRef) == 2 * sizeof(void*));

} // namespace apache::thrift::fast_thrift::channel_pipeline
