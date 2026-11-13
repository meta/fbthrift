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
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Handler.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/StaticHandlerContext.h>

#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>

namespace apache::thrift::fast_thrift::channel_pipeline::detail {

template <typename H>
class ErasedStaticHandlerSlot {
 public:
  using Handler = H;
  using Context = StaticHandlerContext;

  template <typename... Args>
    requires std::constructible_from<H, Args...>
  explicit ErasedStaticHandlerSlot(Args&&... args)
      : handler_(std::forward<Args>(args)...) {}

  H& handler() noexcept { return handler_; }
  Context& context() noexcept { return context_; }

 private:
  H handler_;
  StaticHandlerContext context_;
};

class ErasedStaticHandler {
 public:
  struct Ops {
    Result (*onRead)(void*, TypeErasedBox&&) noexcept;
    Result (*onWrite)(void*, TypeErasedBox&&) noexcept;
    void (*onException)(void*, folly::exception_wrapper&&) noexcept;
    void (*onWriteReady)(void*) noexcept;
    void (*onReadReady)(void*) noexcept;
    void (*onPipelineActive)(void*) noexcept;
    void (*onPipelineInactive)(void*) noexcept;
    void (*handlerAdded)(void*) noexcept;
    void (*handlerRemoved)(void*) noexcept;
    void (*fireEvent)(void*, EventKey, const void*) noexcept;
    // A handler may wait for write ready only if it satisfies OutboundHandler,
    // and for read ready only if it satisfies InboundHandler.
    bool handlesWriteReady;
    bool handlesReadReady;
    void (*bindContext)(
        void*, void*, const StaticHandlerContextOps&, std::size_t) noexcept;
    void (*destroy)(void*) noexcept;
  };

  ErasedStaticHandler() = delete;
  ~ErasedStaticHandler() { reset(); }
  ErasedStaticHandler(ErasedStaticHandler&& other) noexcept
      : handlerId_(std::exchange(other.handlerId_, 0)),
        owner_(std::exchange(other.owner_, nullptr)),
        ops_(std::exchange(other.ops_, nullptr)) {}
  ErasedStaticHandler& operator=(ErasedStaticHandler&& other) noexcept {
    if (this != &other) {
      reset();
      handlerId_ = std::exchange(other.handlerId_, 0);
      owner_ = std::exchange(other.owner_, nullptr);
      ops_ = std::exchange(other.ops_, nullptr);
    }
    return *this;
  }
  ErasedStaticHandler(const ErasedStaticHandler&) = delete;
  ErasedStaticHandler& operator=(const ErasedStaticHandler&) = delete;

  HandlerId handlerId() const noexcept { return handlerId_; }
  void bindContext(
      void* pipeline,
      const StaticHandlerContextOps& ops,
      std::size_t index) noexcept {
    ops_->bindContext(owner_, pipeline, ops, index);
  }
  Result onRead(TypeErasedBox&& msg) noexcept {
    return ops_->onRead(owner_, std::move(msg));
  }
  Result onWrite(TypeErasedBox&& msg) noexcept {
    return ops_->onWrite(owner_, std::move(msg));
  }
  void onException(folly::exception_wrapper&& e) noexcept {
    ops_->onException(owner_, std::move(e));
  }
  void onWriteReady() noexcept { ops_->onWriteReady(owner_); }
  void onReadReady() noexcept { ops_->onReadReady(owner_); }
  void onPipelineActive() noexcept { ops_->onPipelineActive(owner_); }
  void onPipelineInactive() noexcept { ops_->onPipelineInactive(owner_); }
  void handlerAdded() noexcept { ops_->handlerAdded(owner_); }
  void handlerRemoved() noexcept { ops_->handlerRemoved(owner_); }
  void fireEvent(EventKey key, const void* payload) noexcept {
    ops_->fireEvent(owner_, key, payload);
  }
  bool handlesWriteReady() const noexcept { return ops_->handlesWriteReady; }
  bool handlesReadReady() const noexcept { return ops_->handlesReadReady; }

  template <typename H, typename... Args>
  static ErasedStaticHandler make(HandlerId id, Args&&... args) {
    using Slot = ErasedStaticHandlerSlot<H>;
    const auto* ops = &opsFor<H>();
    auto slot = std::make_unique<Slot>(std::forward<Args>(args)...);
    return ErasedStaticHandler(id, slot.release(), ops);
  }

 private:
  template <typename H>
  static const Ops& opsFor() {
    using Slot = ErasedStaticHandlerSlot<H>;
    static const Ops ops = makeOps<Slot, H>();
    return ops;
  }

  ErasedStaticHandler(HandlerId handlerId, void* owner, const Ops* ops) noexcept
      : handlerId_(handlerId), owner_(owner), ops_(ops) {}

  void reset() noexcept {
    auto* owner = std::exchange(owner_, nullptr);
    const auto* ops = std::exchange(ops_, nullptr);
    if (owner != nullptr) {
      ops->destroy(owner);
    }
  }

  template <typename E, typename H, typename Context>
  static void dispatchEvent(
      H& handler,
      Context& context,
      EventKey key,
      const void* payload) noexcept {
    if (key != eventKey<E>()) {
      return;
    }
    if constexpr (std::is_void_v<typename E::Payload>) {
      handler.template on<E>(context);
    } else {
      handler.template on<E>(
          context, *static_cast<const typename E::Payload*>(payload));
    }
  }

  template <typename H, typename Context, PipelineEvent... Evs>
  static void dispatchEvents(
      H& handler,
      Context& context,
      EventKey key,
      const void* payload,
      Events<Evs...>) noexcept {
    (dispatchEvent<Evs>(handler, context, key, payload), ...);
  }

  template <typename Slot, typename H>
  static Ops makeOps() {
    using Context = typename Slot::Context;
    static_assert(
        InboundHandler<H, Context> || OutboundHandler<H, Context> ||
        DuplexHandler<H, Context>);
    static_assert(HandlerLifecycle<H, Context>);
    Ops ops{};
    ops.onRead = [](void* p, TypeErasedBox&& msg) noexcept {
      auto& slot = *static_cast<Slot*>(p);
      if constexpr (InboundHandler<H, Context>) {
        return slot.handler().onRead(slot.context(), std::move(msg));
      } else {
        return slot.context().fireRead(std::move(msg));
      }
    };
    ops.onWrite = [](void* p, TypeErasedBox&& msg) noexcept {
      auto& slot = *static_cast<Slot*>(p);
      if constexpr (OutboundHandler<H, Context>) {
        return slot.handler().onWrite(slot.context(), std::move(msg));
      } else {
        return slot.context().fireWrite(std::move(msg));
      }
    };
    ops.onException = [](void* p, folly::exception_wrapper&& e) noexcept {
      auto& slot = *static_cast<Slot*>(p);
      if constexpr (InboundHandler<H, Context>) {
        slot.handler().onException(slot.context(), std::move(e));
      } else {
        slot.context().fireException(std::move(e));
      }
    };
    ops.onWriteReady = [](void* p) noexcept {
      auto& slot = *static_cast<Slot*>(p);
      if constexpr (OutboundHandler<H, Context>) {
        slot.handler().onWriteReady(slot.context());
      }
    };
    ops.onReadReady = [](void* p) noexcept {
      auto& slot = *static_cast<Slot*>(p);
      if constexpr (InboundHandler<H, Context>) {
        slot.handler().onReadReady(slot.context());
      }
    };
    ops.onPipelineActive = [](void* p) noexcept {
      auto& slot = *static_cast<Slot*>(p);
      if constexpr (requires(H& handler, Context& context) {
                      {
                        handler.onPipelineActive(context)
                      } noexcept -> std::same_as<void>;
                    }) {
        slot.handler().onPipelineActive(slot.context());
      }
    };
    ops.onPipelineInactive = [](void* p) noexcept {
      auto& slot = *static_cast<Slot*>(p);
      if constexpr (requires(H& handler, Context& context) {
                      {
                        handler.onPipelineInactive(context)
                      } noexcept -> std::same_as<void>;
                    }) {
        slot.handler().onPipelineInactive(slot.context());
      }
    };
    ops.handlerAdded = [](void* p) noexcept {
      auto& slot = *static_cast<Slot*>(p);
      slot.handler().handlerAdded(slot.context());
    };
    ops.handlerRemoved = [](void* p) noexcept {
      auto& slot = *static_cast<Slot*>(p);
      slot.handler().handlerRemoved(slot.context());
    };
    ops.fireEvent = [](void* p, EventKey key, const void* payload) noexcept {
      auto& slot = *static_cast<Slot*>(p);
      if constexpr (requires { typename H::SubscribedEvents; }) {
        static_assert(TypeEventSubscriber<H, Context>);
        dispatchEvents(
            slot.handler(),
            slot.context(),
            key,
            payload,
            typename H::SubscribedEvents{});
      }
    };
    ops.handlesWriteReady = OutboundHandler<H, Context>;
    ops.handlesReadReady = InboundHandler<H, Context>;
    ops.bindContext = [](void* p,
                         void* pipeline,
                         const StaticHandlerContextOps& contextOps,
                         std::size_t index) noexcept {
      static_cast<Slot*>(p)->context().bind(pipeline, contextOps, index);
    };
    ops.destroy = [](void* p) noexcept { delete static_cast<Slot*>(p); };
    return ops;
  }

  HandlerId handlerId_{0};
  void* owner_{nullptr};
  const Ops* ops_{nullptr};
};

static_assert(sizeof(ErasedStaticHandler) == 3 * sizeof(void*));

template <typename H, typename... Args>
ErasedStaticHandler makeErasedStaticHandler(HandlerId id, Args&&... args) {
  return ErasedStaticHandler::make<H>(id, std::forward<Args>(args)...);
}

} // namespace apache::thrift::fast_thrift::channel_pipeline::detail
