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

#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/ErasedStaticHandler.h>

#include <cstddef>
#include <memory>
#include <utility>

namespace apache::thrift::fast_thrift::channel_pipeline::detail {

/** A runtime-owned segment whose logical handlers remain independently visible.
 */
class ErasedStaticSegment {
 public:
  struct Ops {
    std::size_t (*handlerCount)(const void*) noexcept;
    HandlerId (*handlerId)(const void*, std::size_t) noexcept;
    void (*bindContext)(
        void*,
        void*,
        const StaticHandlerContextOps&,
        std::size_t,
        std::size_t) noexcept;
    Result (*onReadFirst)(void*, TypeErasedBox&&) noexcept;
    Result (*onReadAt)(void*, std::size_t, TypeErasedBox&&) noexcept;
    Result (*onWriteLast)(void*, TypeErasedBox&&) noexcept;
    Result (*onWriteAt)(void*, std::size_t, TypeErasedBox&&) noexcept;
    void (*onExceptionFirst)(void*, folly::exception_wrapper&&) noexcept;
    void (*onExceptionAt)(
        void*, std::size_t, folly::exception_wrapper&&) noexcept;
    void (*onWriteReady)(void*, std::size_t) noexcept;
    void (*onReadReady)(void*, std::size_t) noexcept;
    void (*onPipelineActive)(void*, std::size_t) noexcept;
    void (*onPipelineInactive)(void*, std::size_t) noexcept;
    void (*handlerAdded)(void*, std::size_t) noexcept;
    void (*handlerRemoved)(void*, std::size_t) noexcept;
    void (*fireEvent)(void*, std::size_t, EventKey, const void*) noexcept;
    bool (*handlesWriteReady)(void*, std::size_t) noexcept;
    bool (*handlesReadReady)(void*, std::size_t) noexcept;
    void (*destroy)(void*) noexcept;
  };

  ErasedStaticSegment() = delete;
  ~ErasedStaticSegment() { reset(); }
  ErasedStaticSegment(ErasedStaticSegment&& other) noexcept
      : owner_(std::exchange(other.owner_, nullptr)),
        ops_(std::exchange(other.ops_, nullptr)) {}
  ErasedStaticSegment& operator=(ErasedStaticSegment&& other) noexcept {
    if (this != &other) {
      reset();
      owner_ = std::exchange(other.owner_, nullptr);
      ops_ = std::exchange(other.ops_, nullptr);
    }
    return *this;
  }
  ErasedStaticSegment(const ErasedStaticSegment&) = delete;
  ErasedStaticSegment& operator=(const ErasedStaticSegment&) = delete;

  std::size_t handlerCount() const noexcept {
    return ops_->handlerCount(owner_);
  }
  HandlerId handlerId(std::size_t index) const noexcept {
    return ops_->handlerId(owner_, index);
  }
  void bindContext(
      void* pipeline,
      const StaticHandlerContextOps& ops,
      std::size_t baseIndex,
      std::size_t segmentIndex) noexcept {
    ops_->bindContext(owner_, pipeline, ops, baseIndex, segmentIndex);
  }
  Result onReadFirst(TypeErasedBox&& msg) noexcept {
    return ops_->onReadFirst(owner_, std::move(msg));
  }
  Result onReadAt(std::size_t index, TypeErasedBox&& msg) noexcept {
    return ops_->onReadAt(owner_, index, std::move(msg));
  }
  Result onWriteLast(TypeErasedBox&& msg) noexcept {
    return ops_->onWriteLast(owner_, std::move(msg));
  }
  Result onWriteAt(std::size_t index, TypeErasedBox&& msg) noexcept {
    return ops_->onWriteAt(owner_, index, std::move(msg));
  }
  void onExceptionFirst(folly::exception_wrapper&& e) noexcept {
    ops_->onExceptionFirst(owner_, std::move(e));
  }
  void onExceptionAt(std::size_t index, folly::exception_wrapper&& e) noexcept {
    ops_->onExceptionAt(owner_, index, std::move(e));
  }
  void onWriteReady(std::size_t index) noexcept {
    ops_->onWriteReady(owner_, index);
  }
  void onReadReady(std::size_t index) noexcept {
    ops_->onReadReady(owner_, index);
  }
  void onPipelineActive(std::size_t index) noexcept {
    ops_->onPipelineActive(owner_, index);
  }
  void onPipelineInactive(std::size_t index) noexcept {
    ops_->onPipelineInactive(owner_, index);
  }
  void handlerAdded(std::size_t index) noexcept {
    ops_->handlerAdded(owner_, index);
  }
  void handlerRemoved(std::size_t index) noexcept {
    ops_->handlerRemoved(owner_, index);
  }
  void fireEvent(
      std::size_t index, EventKey key, const void* payload) noexcept {
    ops_->fireEvent(owner_, index, key, payload);
  }
  bool handlesWriteReady(std::size_t index) noexcept {
    return ops_->handlesWriteReady(owner_, index);
  }
  bool handlesReadReady(std::size_t index) noexcept {
    return ops_->handlesReadReady(owner_, index);
  }

  template <typename Segment, typename... Args>
  static ErasedStaticSegment make(Args&&... args) {
    auto segment = std::make_unique<Segment>(std::forward<Args>(args)...);
    return ErasedStaticSegment(segment.release(), &opsFor<Segment>());
  }

 private:
  ErasedStaticSegment(void* owner, const Ops* ops) noexcept
      : owner_(owner), ops_(ops) {}

  template <typename Segment>
  static const Ops& opsFor() {
    static const Ops ops{
        .handlerCount =
            +[](const void* p) noexcept {
              return static_cast<const Segment*>(p)->handlerCount();
            },
        .handlerId =
            +[](const void* p, std::size_t i) noexcept {
              return static_cast<const Segment*>(p)->handlerId(i);
            },
        .bindContext =
            +[](void* p,
                void* pipeline,
                const StaticHandlerContextOps& contextOps,
                std::size_t base,
                std::size_t segmentIndex) noexcept {
              static_cast<Segment*>(p)->bindContext(
                  pipeline, contextOps, base, segmentIndex);
            },
        .onReadFirst =
            +[](void* p, TypeErasedBox&& msg) noexcept {
              return static_cast<Segment*>(p)->onReadFirst(std::move(msg));
            },
        .onReadAt =
            +[](void* p, std::size_t i, TypeErasedBox&& msg) noexcept {
              return static_cast<Segment*>(p)->onReadAt(i, std::move(msg));
            },
        .onWriteLast =
            +[](void* p, TypeErasedBox&& msg) noexcept {
              return static_cast<Segment*>(p)->onWriteLast(std::move(msg));
            },
        .onWriteAt =
            +[](void* p, std::size_t i, TypeErasedBox&& msg) noexcept {
              return static_cast<Segment*>(p)->onWriteAt(i, std::move(msg));
            },
        .onExceptionFirst =
            +[](void* p, folly::exception_wrapper&& e) noexcept {
              static_cast<Segment*>(p)->onExceptionFirst(std::move(e));
            },
        .onExceptionAt =
            +[](void* p, std::size_t i, folly::exception_wrapper&& e) noexcept {
              static_cast<Segment*>(p)->onExceptionAt(i, std::move(e));
            },
        .onWriteReady =
            +[](void* p, std::size_t i) noexcept {
              static_cast<Segment*>(p)->onWriteReady(i);
            },
        .onReadReady =
            +[](void* p, std::size_t i) noexcept {
              static_cast<Segment*>(p)->onReadReady(i);
            },
        .onPipelineActive =
            +[](void* p, std::size_t i) noexcept {
              static_cast<Segment*>(p)->onPipelineActive(i);
            },
        .onPipelineInactive =
            +[](void* p, std::size_t i) noexcept {
              static_cast<Segment*>(p)->onPipelineInactive(i);
            },
        .handlerAdded =
            +[](void* p, std::size_t i) noexcept {
              static_cast<Segment*>(p)->handlerAdded(i);
            },
        .handlerRemoved =
            +[](void* p, std::size_t i) noexcept {
              static_cast<Segment*>(p)->handlerRemoved(i);
            },
        .fireEvent =
            +[](void* p,
                std::size_t i,
                EventKey key,
                const void* payload) noexcept {
              static_cast<Segment*>(p)->fireEvent(i, key, payload);
            },
        .handlesWriteReady =
            +[](void* p, std::size_t i) noexcept {
              return static_cast<Segment*>(p)->handlesWriteReady(i);
            },
        .handlesReadReady =
            +[](void* p, std::size_t i) noexcept {
              return static_cast<Segment*>(p)->handlesReadReady(i);
            },
        .destroy = +[](void* p) noexcept { delete static_cast<Segment*>(p); },
    };
    return ops;
  }

  void reset() noexcept {
    auto* owner = std::exchange(owner_, nullptr);
    const auto* ops = std::exchange(ops_, nullptr);
    if (owner != nullptr) {
      ops->destroy(owner);
    }
  }

  void* owner_;
  const Ops* ops_;
};

static_assert(sizeof(ErasedStaticSegment) == 2 * sizeof(void*));

class ErasedStaticHandlerSegment {
 public:
  explicit ErasedStaticHandlerSegment(ErasedStaticHandler handler)
      : handler_(std::move(handler)) {}

  std::size_t handlerCount() const noexcept { return 1; }
  HandlerId handlerId(std::size_t) const noexcept {
    return handler_.handlerId();
  }
  void bindContext(
      void* pipeline,
      const StaticHandlerContextOps& ops,
      std::size_t baseIndex,
      std::size_t) noexcept {
    handler_.bindContext(pipeline, ops, baseIndex);
  }
  Result onReadFirst(TypeErasedBox&& msg) noexcept {
    return handler_.onRead(std::move(msg));
  }
  Result onReadAt(std::size_t, TypeErasedBox&& msg) noexcept {
    return handler_.onRead(std::move(msg));
  }
  Result onWriteLast(TypeErasedBox&& msg) noexcept {
    return handler_.onWrite(std::move(msg));
  }
  Result onWriteAt(std::size_t, TypeErasedBox&& msg) noexcept {
    return handler_.onWrite(std::move(msg));
  }
  void onExceptionFirst(folly::exception_wrapper&& e) noexcept {
    handler_.onException(std::move(e));
  }
  void onExceptionAt(std::size_t, folly::exception_wrapper&& e) noexcept {
    handler_.onException(std::move(e));
  }
  void onWriteReady(std::size_t) noexcept { handler_.onWriteReady(); }
  void onReadReady(std::size_t) noexcept { handler_.onReadReady(); }
  void onPipelineActive(std::size_t) noexcept { handler_.onPipelineActive(); }
  void onPipelineInactive(std::size_t) noexcept {
    handler_.onPipelineInactive();
  }
  void handlerAdded(std::size_t) noexcept { handler_.handlerAdded(); }
  void handlerRemoved(std::size_t) noexcept { handler_.handlerRemoved(); }
  void fireEvent(std::size_t, EventKey key, const void* payload) noexcept {
    handler_.fireEvent(key, payload);
  }
  bool handlesWriteReady(std::size_t) noexcept {
    return handler_.handlesWriteReady();
  }
  bool handlesReadReady(std::size_t) noexcept {
    return handler_.handlesReadReady();
  }

 private:
  ErasedStaticHandler handler_;
};

} // namespace apache::thrift::fast_thrift::channel_pipeline::detail

namespace apache::thrift::fast_thrift::channel_pipeline {

inline detail::ErasedStaticSegment makeErasedStaticHandlerSegment(
    detail::ErasedStaticHandler handler) {
  return detail::ErasedStaticSegment::make<detail::ErasedStaticHandlerSegment>(
      std::move(handler));
}

} // namespace apache::thrift::fast_thrift::channel_pipeline
