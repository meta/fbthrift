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
#include <folly/io/async/EventBase.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Event.h>

#include <array>
#include <concepts>
#include <cstddef>
#include <memory>
#include <type_traits>
#include <utility>

namespace apache::thrift::fast_thrift::channel_pipeline {
class PipelineOwner;

class PipelineGuard;

template <typename EventSet>
class PipelineEventPublisherRef;

template <typename P>
concept PipelineRefTarget = requires(
    P& pipeline,
    TypeErasedBox&& message,
    folly::exception_wrapper&& exception,
    EventKey eventKey,
    BoundEventRoute route,
    const void* payload,
    std::size_t size) {
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
  { pipeline.allocate(size) } noexcept -> std::same_as<BytesPtr>;
  { pipeline.eventBase() } noexcept -> std::same_as<folly::EventBase*>;
  { pipeline.bindEvent(eventKey) } noexcept -> std::same_as<BoundEventRoute>;
  { pipeline.fireBoundEvent(route, payload) } noexcept -> std::same_as<void>;
};

/**
 * Non-owning reference to a pipeline. A default-constructed or reset reference
 * is empty; callers must check it before invoking any other operation.
 */
class PipelineRef {
 public:
  PipelineRef() noexcept = default;

  template <PipelineRefTarget P>
  explicit PipelineRef(P& pipeline) noexcept
      : instance_(&pipeline), ops_(&opsFor<P>()) {}

  explicit operator bool() const noexcept { return instance_ != nullptr; }
  friend bool operator==(PipelineRef, PipelineRef) noexcept = default;

  friend bool operator==(PipelineRef pipeline, std::nullptr_t) noexcept {
    return !pipeline;
  }
  friend bool operator==(std::nullptr_t, PipelineRef pipeline) noexcept {
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

  BytesPtr allocate(std::size_t size) const noexcept {
    return ops_->allocate(instance_, size);
  }

  folly::EventBase* eventBase() const noexcept {
    return ops_->eventBase(instance_);
  }

  PipelineGuard guard() const noexcept;

  template <typename EventSet>
    requires kIsEventSet<EventSet>
  PipelineEventPublisherRef<EventSet> bindEvents() const noexcept;

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
    BytesPtr (*allocate)(void*, std::size_t) noexcept;
    folly::EventBase* (*eventBase)(void*) noexcept;
    BoundEventRoute (*bindEvent)(void*, EventKey) noexcept;
    void (*fireBoundEvent)(void*, BoundEventRoute, const void*) noexcept;
    void (*constructGuard)(void*, void*) noexcept;
    void (*moveGuard)(void*, void*) noexcept;
    void (*destroyGuard)(void*) noexcept;
  };

  template <PipelineRefTarget P>
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
        .allocate =
            +[](void* p, std::size_t size) noexcept {
              return static_cast<P*>(p)->allocate(size);
            },
        .eventBase =
            +[](void* p) noexcept { return static_cast<P*>(p)->eventBase(); },
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

  friend class PipelineGuard;
  friend class PipelineOwner;
  template <typename>
  friend class PipelineEventPublisherRef;
};

class PipelineOwner {
 public:
  PipelineOwner() noexcept = default;
  PipelineOwner(const PipelineOwner&) = delete;
  PipelineOwner& operator=(const PipelineOwner&) = delete;

  template <PipelineRefTarget P, typename D>
    requires std::is_empty_v<D> && std::default_initializable<D>
  explicit PipelineOwner(std::unique_ptr<P, D> pipeline) noexcept {
    if (auto* instance = pipeline.release()) {
      pipeline_ = PipelineRef(*instance);
      destroy_ = +[](void* p) noexcept { D{}(static_cast<P*>(p)); };
    }
  }

  template <PipelineRefTarget P, typename D>
    requires std::is_empty_v<D> && std::default_initializable<D>
  PipelineOwner& operator=(std::unique_ptr<P, D> pipeline) noexcept {
    PipelineOwner replacement(std::move(pipeline));
    *this = std::move(replacement);
    return *this;
  }

  PipelineOwner(PipelineOwner&& other) noexcept
      : pipeline_(std::exchange(other.pipeline_, {})),
        destroy_(std::exchange(other.destroy_, nullptr)) {}

  PipelineOwner& operator=(PipelineOwner&& other) noexcept {
    if (this != &other) {
      reset();
      pipeline_ = std::exchange(other.pipeline_, {});
      destroy_ = std::exchange(other.destroy_, nullptr);
    }
    return *this;
  }

  ~PipelineOwner() { reset(); }

  explicit operator bool() const noexcept {
    return static_cast<bool>(pipeline_);
  }

  const PipelineRef* operator->() const noexcept { return &pipeline_; }
  PipelineRef get() const noexcept { return pipeline_; }
  PipelineRef ref() const noexcept { return pipeline_; }

  void reset() noexcept {
    auto pipeline = std::exchange(pipeline_, {});
    auto* destroy = std::exchange(destroy_, nullptr);
    if (destroy != nullptr) {
      destroy(pipeline.instance_);
    }
  }

 private:
  PipelineRef pipeline_;
  void (*destroy_)(void*) noexcept {nullptr};
};

static_assert(sizeof(PipelineOwner) == 3 * sizeof(void*));

class PipelineGuard {
 public:
  PipelineGuard() noexcept = default;
  PipelineGuard(const PipelineGuard&) = delete;
  PipelineGuard& operator=(const PipelineGuard&) = delete;

  PipelineGuard(PipelineGuard&& other) noexcept
      : ops_(std::exchange(other.ops_, nullptr)) {
    if (ops_ != nullptr) {
      ops_->moveGuard(&storage_, &other.storage_);
    }
  }

  PipelineGuard& operator=(PipelineGuard&& other) noexcept {
    if (this != &other) {
      reset();
      ops_ = std::exchange(other.ops_, nullptr);
      if (ops_ != nullptr) {
        ops_->moveGuard(&storage_, &other.storage_);
      }
    }
    return *this;
  }

  ~PipelineGuard() { reset(); }

  explicit operator bool() const noexcept { return ops_ != nullptr; }

  void reset() noexcept {
    const auto* ops = std::exchange(ops_, nullptr);
    if (ops != nullptr) {
      ops->destroyGuard(&storage_);
    }
  }

 private:
  explicit PipelineGuard(const PipelineRef& pipeline) noexcept
      : ops_(pipeline.ops_) {
    if (ops_ != nullptr) {
      ops_->constructGuard(&storage_, pipeline.instance_);
    }
  }

  alignas(void*) std::byte storage_[sizeof(void*)]{};
  const PipelineRef::Ops* ops_{nullptr};

  friend class PipelineRef;
};

inline PipelineGuard PipelineRef::guard() const noexcept {
  return PipelineGuard(*this);
}

template <PipelineEvent... Evs>
class PipelineEventPublisherRef<Events<Evs...>> {
 public:
  PipelineEventPublisherRef() noexcept = default;

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
  explicit PipelineEventPublisherRef(PipelineRef pipeline) noexcept
      : pipeline_(pipeline), routes_{pipeline.bindEvent(eventKey<Evs>())...} {}

  PipelineRef pipeline_;
  std::array<BoundEventRoute, sizeof...(Evs)> routes_{};

  friend class PipelineRef;
};

template <typename EventSet>
  requires kIsEventSet<EventSet>
PipelineEventPublisherRef<EventSet> PipelineRef::bindEvents() const noexcept {
  return PipelineEventPublisherRef<EventSet>{*this};
}

static_assert(sizeof(PipelineGuard) == 2 * sizeof(void*));
static_assert(sizeof(PipelineRef) == 2 * sizeof(void*));

/**
 * The seam-adapter concepts, one per bind direction: a tail bind's adapter
 * satisfies SeamReadAdapter (it provides a static adaptRead), a head bind's
 * satisfies SeamWriteAdapter (it provides a static adaptWrite). See
 * ForwardingAdapter below for what a seam adapter does and the default model.
 */
template <typename A>
concept SeamReadAdapter = requires(TypeErasedBox&& msg) {
  { A::adaptRead(std::move(msg)) } noexcept -> std::same_as<TypeErasedBox>;
};

template <typename A>
concept SeamWriteAdapter = requires(TypeErasedBox&& msg) {
  { A::adaptWrite(std::move(msg)) } noexcept -> std::same_as<TypeErasedBox>;
};

/**
 * A seam adapter is a stateless conversion applied to a message as it crosses
 * the boundary between two bound pipelines, so neither pipeline's type knows
 * anything about the other. Its adaptRead/adaptWrite are static (message in,
 * converted message out); the binding pipeline performs the injection into the
 * peer and owns backpressure. Being stateless, an adapter needs no storage —
 * bind records only a function pointer to the static conversion.
 *
 * A tail bind forwards reads, so its adapter provides adaptRead; a head bind
 * forwards writes, so its adapter provides adaptWrite.
 *
 * ForwardingAdapter is the default: return the message unchanged, so a bind
 * with no adapter forwards as-is.
 */
struct ForwardingAdapter {
  static TypeErasedBox adaptRead(TypeErasedBox&& msg) noexcept {
    return std::move(msg);
  }
  static TypeErasedBox adaptWrite(TypeErasedBox&& msg) noexcept {
    return std::move(msg);
  }
};

} // namespace apache::thrift::fast_thrift::channel_pipeline
