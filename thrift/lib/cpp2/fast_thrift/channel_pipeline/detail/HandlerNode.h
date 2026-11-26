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
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Handler.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/ContextImpl.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/detail/TypedContext.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>

namespace apache::thrift::fast_thrift::channel_pipeline::detail {

struct HandlerNodeBase;
template <ExecutionDomain D>
struct HandlerNodeT;

using HandlerNode = HandlerNodeT<EvbExecutionDomain>;

struct HandlerConstructionOps {
  std::size_t size;
  std::size_t alignment;
  void (*constructInline)(HandlerNodeBase&, void*);
  void (*constructHeap)(HandlerNodeBase&);
};

template <ExecutionDomain D>
using HandlerConstructionOpsT = HandlerConstructionOps;

/**
 * HandlerNode stores a type-erased handler with function pointers for dispatch.
 *
 * This implements the External Polymorphism pattern:
 * - Handlers are plain classes (no virtual methods required)
 * - Function pointers are generated at compile-time when the handler is added
 * - Dispatch is a single function pointer call (no vtable lookup)
 *
 * Storage uses two pointers:
 * - `handlerPtr`: Raw pointer for fast function calls
 * - `owner`: Owning unique_ptr with type-erased deleter for destruction
 *
 * Before build(), a deferred handler stores its construction operations in
 * `handlerPtr`; a finalized node always has `handlerPtr == owner.get()`.
 */
struct HandlerNodeBase {
  // Handler identification
  HandlerId handlerId{0};

  // Raw pointer for fast dispatch (no indirection through unique_ptr)
  void* handlerPtr{nullptr};

  // Owning pointer with type-erased deleter
  std::unique_ptr<void, void (*)(void*)> owner{nullptr, nullptr};

  // Function pointer types
  using OnReadFn = Result (*)(void*, ContextImpl&, TypeErasedBox&&) noexcept;
  using OnWriteFn = Result (*)(void*, ContextImpl&, TypeErasedBox&&) noexcept;
  using OnExceptionFn =
      void (*)(void*, ContextImpl&, folly::exception_wrapper&&) noexcept;
  using OnWriteReadyFn = void (*)(void*, ContextImpl&) noexcept;
  using OnReadReadyFn = void (*)(void*, ContextImpl&) noexcept;
  using OnPipelineInactiveFn = void (*)(void*, ContextImpl&) noexcept;
  using OnPipelineActiveFn = void (*)(void*, ContextImpl&) noexcept;
  using HandlerAddedFn = void (*)(void*, ContextImpl&) noexcept;
  using HandlerRemovedFn = void (*)(void*, ContextImpl&) noexcept;

  // Inbound handler methods
  OnPipelineActiveFn onPipelineActiveFn{nullptr};
  OnReadFn onReadFn{nullptr};
  OnExceptionFn onExceptionFn{nullptr};

  // Outbound handler methods and the ready callbacks
  OnWriteFn onWriteFn{nullptr};
  OnWriteReadyFn onWriteReadyFn{nullptr};
  OnReadReadyFn onReadReadyFn{nullptr};
  OnPipelineInactiveFn onPipelineInactiveFn{nullptr};

  // Type-based event metadata. Published keys register the event with this
  // pipeline; subscriptions additionally carry the typed on<E>() thunk.
  const EventKey* publishedEvents{nullptr};
  std::size_t publishedEventCount{0};
  const TypeEventSubscription* typeSubscriptions{nullptr};
  std::size_t typeSubscriptionCount{0};

  // Lifecycle methods
  HandlerAddedFn handlerAddedFn{nullptr};
  HandlerRemovedFn handlerRemovedFn{nullptr};

  // Set at compile time by makeHandlerNode. A handler may wait for write ready
  // if it has onWriteReady(), and for read ready if it has onReadReady(),
  // whatever its direction.
  bool handlesWriteReady{false};
  bool handlesReadReady{false};

  // Move-only with explicit move constructor to nullify moved-from object
  HandlerNodeBase() = default;
  ~HandlerNodeBase() = default;

  const HandlerConstructionOps* pendingConstructionOps() const noexcept {
    if (handlerPtr == owner.get()) {
      return nullptr;
    }
    return static_cast<const HandlerConstructionOps*>(handlerPtr);
  }
  HandlerNodeBase(HandlerNodeBase&& other) noexcept
      : handlerId(other.handlerId),
        handlerPtr(other.handlerPtr),
        owner(std::move(other.owner)),
        onPipelineActiveFn(other.onPipelineActiveFn),
        onReadFn(other.onReadFn),
        onExceptionFn(other.onExceptionFn),
        onWriteFn(other.onWriteFn),
        onWriteReadyFn(other.onWriteReadyFn),
        onReadReadyFn(other.onReadReadyFn),
        onPipelineInactiveFn(other.onPipelineInactiveFn),
        publishedEvents(other.publishedEvents),
        publishedEventCount(other.publishedEventCount),
        typeSubscriptions(other.typeSubscriptions),
        typeSubscriptionCount(other.typeSubscriptionCount),
        handlerAddedFn(other.handlerAddedFn),
        handlerRemovedFn(other.handlerRemovedFn),
        handlesWriteReady(other.handlesWriteReady),
        handlesReadReady(other.handlesReadReady) {
    other.handlerId = 0;
    other.handlerPtr = nullptr;
    other.onPipelineActiveFn = nullptr;
    other.onReadFn = nullptr;
    other.onExceptionFn = nullptr;
    other.onWriteFn = nullptr;
    other.onWriteReadyFn = nullptr;
    other.onReadReadyFn = nullptr;
    other.onPipelineInactiveFn = nullptr;
    other.publishedEvents = nullptr;
    other.publishedEventCount = 0;
    other.typeSubscriptions = nullptr;
    other.typeSubscriptionCount = 0;
    other.handlerAddedFn = nullptr;
    other.handlerRemovedFn = nullptr;
    other.handlesWriteReady = false;
    other.handlesReadReady = false;
  }

  HandlerNodeBase& operator=(HandlerNodeBase&& other) noexcept {
    if (this != &other) {
      handlerId = other.handlerId;
      handlerPtr = other.handlerPtr;
      owner = std::move(other.owner);
      onPipelineActiveFn = other.onPipelineActiveFn;
      onReadFn = other.onReadFn;
      onExceptionFn = other.onExceptionFn;
      onWriteFn = other.onWriteFn;
      onWriteReadyFn = other.onWriteReadyFn;
      onReadReadyFn = other.onReadReadyFn;
      onPipelineInactiveFn = other.onPipelineInactiveFn;
      publishedEvents = other.publishedEvents;
      publishedEventCount = other.publishedEventCount;
      typeSubscriptions = other.typeSubscriptions;
      typeSubscriptionCount = other.typeSubscriptionCount;
      handlerAddedFn = other.handlerAddedFn;
      handlerRemovedFn = other.handlerRemovedFn;
      handlesWriteReady = other.handlesWriteReady;
      handlesReadReady = other.handlesReadReady;

      other.handlerId = 0;
      other.handlerPtr = nullptr;
      other.onPipelineActiveFn = nullptr;
      other.onReadFn = nullptr;
      other.onExceptionFn = nullptr;
      other.onWriteFn = nullptr;
      other.onWriteReadyFn = nullptr;
      other.onReadReadyFn = nullptr;
      other.onPipelineInactiveFn = nullptr;
      other.publishedEvents = nullptr;
      other.publishedEventCount = 0;
      other.typeSubscriptions = nullptr;
      other.typeSubscriptionCount = 0;
      other.handlerAddedFn = nullptr;
      other.handlerRemovedFn = nullptr;
      other.handlesWriteReady = false;
      other.handlesReadReady = false;
    }
    return *this;
  }

  HandlerNodeBase(const HandlerNodeBase&) = delete;
  HandlerNodeBase& operator=(const HandlerNodeBase&) = delete;
};

template <ExecutionDomain D>
struct HandlerNodeT : HandlerNodeBase {
  using ExecutionDomainType = D;

  HandlerNodeT() = default;
  ~HandlerNodeT() = default;
  HandlerNodeT(HandlerNodeT&&) noexcept = default;
  HandlerNodeT& operator=(HandlerNodeT&&) noexcept = default;
  HandlerNodeT(const HandlerNodeT&) = delete;
  HandlerNodeT& operator=(const HandlerNodeT&) = delete;
};

static_assert(sizeof(HandlerNode) == sizeof(HandlerNodeBase));
static_assert(alignof(HandlerNode) == alignof(HandlerNodeBase));

template <
    typename H,
    typename E,
    bool Endpoint,
    typename StateTuple,
    ExecutionDomain D = EvbExecutionDomain>
inline constexpr TypeEventDispatchFnT<D> kTypeSubThunk =
    +[](void* h, ContextImpl* ctx, const void* payload) noexcept {
      if constexpr (Endpoint) {
        if constexpr (std::is_void_v<typename E::Payload>) {
          static_cast<H*>(h)->template on<E>();
        } else {
          static_cast<H*>(h)->template on<E>(
              *static_cast<const typename E::Payload*>(payload));
        }
      } else {
        decltype(auto) c = contextFor<StateTuple, D>(*ctx);
        if constexpr (std::is_void_v<typename E::Payload>) {
          static_cast<H*>(h)->template on<E>(c);
        } else {
          static_cast<H*>(h)->template on<E>(
              c, *static_cast<const typename E::Payload*>(payload));
        }
      }
    };

template <
    ExecutionDomain D,
    typename H,
    bool Endpoint,
    typename StateTuple,
    PipelineEvent... Evs>
constexpr std::array<TypeEventSubscriptionT<D>, sizeof...(Evs)>
makeTypeSubscriptionsT(Events<Evs...>) {
  return {TypeEventSubscriptionT<D>{
      eventKey<Evs>(), kTypeSubThunk<H, Evs, Endpoint, StateTuple, D>}...};
}

template <typename H, bool Endpoint, typename StateTuple, PipelineEvent... Evs>
constexpr auto makeTypeSubscriptions(Events<Evs...> events) {
  return makeTypeSubscriptionsT<EvbExecutionDomain, H, Endpoint, StateTuple>(
      events);
}

template <typename H>
inline constexpr auto kHandlerPublishedEventKeys =
    eventKeys(typename H::PublishedEvents{});

template <
    typename H,
    bool Endpoint,
    typename StateTuple = std::tuple<>,
    ExecutionDomain D = EvbExecutionDomain>
inline constexpr auto kHandlerTypeSubscriptions =
    makeTypeSubscriptionsT<D, H, Endpoint, StateTuple>(
        typename H::SubscribedEvents{});

/**
 * Creates a HandlerNode from a concrete handler type.
 *
 * For a handler without InboundHandler, onRead() and onException() pass on to
 * the next handler, and onPipelineActive() does nothing. For a handler without
 * OutboundHandler, onWrite() passes on, and onPipelineInactive() does nothing.
 *
 * A handler of any direction gets onWriteReady() calls if it satisfies
 * WriteReadyHandler, and onReadReady() calls if it satisfies ReadReadyHandler.
 * Without the callback, that ready function does nothing. A ready callback in
 * another form fails the build.
 *
 * @tparam H The concrete handler type
 * @param handler_id The unique ID for this handler
 * @param handler Unique pointer to the handler instance
 * @return A fully initialized HandlerNode
 */
template <
    typename H,
    typename StateTuple = std::tuple<>,
    ExecutionDomain D = EvbExecutionDomain>
  requires HandlerLifecycle<H, ContextFor<StateTuple, D>>
HandlerNodeT<D> makeHandlerNode(
    HandlerId handlerId, std::unique_ptr<H> handler) {
  static_assert(sizeof(HandlerNodeT<D>) == sizeof(HandlerNodeBase));
  static_assert(alignof(HandlerNodeT<D>) == alignof(HandlerNodeBase));
  using Ctx = ContextFor<StateTuple, D>;

  HandlerNodeT<D> node;
  node.handlerId = handlerId;
  node.handlerPtr = handler.get();
  node.owner = std::unique_ptr<void, void (*)(void*)>(
      handler.release(), [](void* p) { delete static_cast<H*>(p); });

  // Lifecycle methods - always present (required by HandlerLifecycle concept)
  node.handlerAddedFn = [](void* h, ContextImpl& ctx) noexcept {
    decltype(auto) c = contextFor<StateTuple, D>(ctx);
    static_cast<H*>(h)->handlerAdded(c);
  };

  node.handlerRemovedFn = [](void* h, ContextImpl& ctx) noexcept {
    decltype(auto) c = contextFor<StateTuple, D>(ctx);
    static_cast<H*>(h)->handlerRemoved(c);
  };

  // Inbound methods - check if handler satisfies InboundHandler
  if constexpr (InboundHandler<H, Ctx>) {
    node.onPipelineActiveFn = [](void* h, ContextImpl& ctx) noexcept {
      decltype(auto) c = contextFor<StateTuple, D>(ctx);
      static_cast<H*>(h)->onPipelineActive(c);
    };

    node.onReadFn =
        [](void* h, ContextImpl& ctx, TypeErasedBox&& msg) noexcept -> Result {
      decltype(auto) c = contextFor<StateTuple, D>(ctx);
      return static_cast<H*>(h)->onRead(c, std::move(msg));
    };

    node.onExceptionFn =
        [](void* h, ContextImpl& ctx, folly::exception_wrapper&& e) noexcept {
          decltype(auto) c = contextFor<StateTuple, D>(ctx);
          static_cast<H*>(h)->onException(c, std::move(e));
        };
  } else {
    // Passthrough for non-inbound handlers
    node.onPipelineActiveFn = [](void*, ContextImpl&) noexcept {
      // No-op for non-inbound handlers (activation already propagates to all)
    };

    node.onReadFn =
        [](void*, ContextImpl& ctx, TypeErasedBox&& msg) noexcept -> Result {
      return ctx.fireRead(std::move(msg));
    };

    node.onExceptionFn =
        [](void*, ContextImpl& ctx, folly::exception_wrapper&& e) noexcept {
          ctx.fireException(std::move(e));
        };
  }

  // Outbound methods - check if handler satisfies OutboundHandler
  if constexpr (OutboundHandler<H, Ctx>) {
    node.onWriteFn =
        [](void* h, ContextImpl& ctx, TypeErasedBox&& msg) noexcept -> Result {
      decltype(auto) c = contextFor<StateTuple, D>(ctx);
      return static_cast<H*>(h)->onWrite(c, std::move(msg));
    };

    node.onPipelineInactiveFn = [](void* h, ContextImpl& ctx) noexcept {
      decltype(auto) c = contextFor<StateTuple, D>(ctx);
      static_cast<H*>(h)->onPipelineInactive(c);
    };
  } else {
    // Passthrough for non-outbound handlers
    node.onWriteFn =
        [](void*, ContextImpl& ctx, TypeErasedBox&& msg) noexcept -> Result {
      return ctx.fireWrite(std::move(msg));
    };

    node.onPipelineInactiveFn = [](void*, ContextImpl&) noexcept {
      // No-op for non-outbound handlers
    };
  }

  // Ready callbacks - a handler of any direction may have them
  checkReadyCallbacks<H, Ctx>();
  if constexpr (WriteReadyHandler<H, Ctx>) {
    node.onWriteReadyFn = [](void* h, ContextImpl& ctx) noexcept {
      decltype(auto) c = contextFor<StateTuple, D>(ctx);
      static_cast<H*>(h)->onWriteReady(c);
    };
    node.handlesWriteReady = true;
  } else {
    node.onWriteReadyFn = [](void*, ContextImpl&) noexcept {
      // No-op for handlers without onWriteReady()
    };
  }

  if constexpr (ReadReadyHandler<H, Ctx>) {
    node.onReadReadyFn = [](void* h, ContextImpl& ctx) noexcept {
      decltype(auto) c = contextFor<StateTuple, D>(ctx);
      static_cast<H*>(h)->onReadReady(c);
    };
    node.handlesReadReady = true;
  } else {
    node.onReadReadyFn = [](void*, ContextImpl&) noexcept {
      // No-op for handlers without onReadReady()
    };
  }

  if constexpr (requires { typename H::PublishedEvents; }) {
    static_assert(
        TypeEventPublisher<H>,
        "PublishedEvents must be a channel_pipeline::Events<...> set");
    if constexpr (TypeEventPublisher<H>) {
      node.publishedEvents = kHandlerPublishedEventKeys<H>.data();
      node.publishedEventCount = kHandlerPublishedEventKeys<H>.size();
    }
  }

  if constexpr (requires { typename H::SubscribedEvents; }) {
    static_assert(
        TypeEventSubscriber<H, Ctx>,
        "SubscribedEvents requires a noexcept on<Event>(ctx, payload) for "
        "every event; signal events omit payload");
    if constexpr (TypeEventSubscriber<H, Ctx>) {
      node.typeSubscriptions =
          kHandlerTypeSubscriptions<H, /*Endpoint=*/false, StateTuple, D>.data();
      node.typeSubscriptionCount =
          kHandlerTypeSubscriptions<H, /*Endpoint=*/false, StateTuple, D>.size();
    }
  }

  return node;
}

template <typename Arg>
using StoredHandlerArg = std::decay_t<Arg>;

template <typename Arg>
StoredHandlerArg<Arg> storeHandlerArg(Arg&& arg) {
  return std::forward<Arg>(arg);
}

template <typename Arg>
decltype(auto) forwardStoredHandlerArg(StoredHandlerArg<Arg>& arg) {
  return std::move(arg);
}

template <typename H>
void setHandlerPointer(HandlerNodeBase& node, H* handler) noexcept {
  node.handlerPtr = handler;
}

template <typename H, ExecutionDomain D>
void setHandlerPointer(HandlerNodeT<D>& node, H* handler) noexcept {
  setHandlerPointer(static_cast<HandlerNodeBase&>(node), handler);
}

template <
    ExecutionDomain D,
    typename H,
    typename StateTuple = std::tuple<>,
    typename... Args>
  requires HandlerLifecycle<H, ContextFor<StateTuple, D>>
HandlerNodeT<D> makeInlineHandlerNodeT(HandlerId handlerId, Args&&... args) {
  static_assert(
      alignof(H) <= alignof(ContextImpl),
      "Inline pipeline handler alignment exceeds pipeline alignment");

  using ArgsTuple = std::tuple<StoredHandlerArg<Args>...>;
  // Construction is deferred until build(), so own decay-copied arguments.
  // Callers requiring reference semantics must pass std::ref/std::cref and
  // keep the referenced object alive for the pipeline's lifetime.
  auto node =
      makeHandlerNode<H, StateTuple, D>(handlerId, std::unique_ptr<H>{});
  if constexpr (sizeof...(Args) != 0) {
    auto storedArgs = std::make_unique<ArgsTuple>(
        storeHandlerArg<Args>(std::forward<Args>(args))...);
    node.owner = decltype(node.owner)(
        storedArgs.release(),
        +[](void* p) noexcept { delete static_cast<ArgsTuple*>(p); });
  }

  static constexpr HandlerConstructionOps kConstructionOps{
      sizeof(H),
      alignof(H),
      [](HandlerNodeBase& node, void* storage) {
        auto* handler = [&] {
          if constexpr (sizeof...(Args) == 0) {
            return std::construct_at(static_cast<H*>(storage));
          } else {
            auto& storedArgs = *static_cast<ArgsTuple*>(node.owner.get());
            return std::apply(
                [storage](auto&... stored) {
                  return std::construct_at(
                      static_cast<H*>(storage),
                      forwardStoredHandlerArg<Args>(stored)...);
                },
                storedArgs);
          }
        }();
        auto previousOwner = std::move(node.owner);
        node.owner = decltype(node.owner)(
            handler,
            +[](void* p) noexcept { std::destroy_at(static_cast<H*>(p)); });
        node.handlerPtr = handler;
      },
      [](HandlerNodeBase& node) {
        auto handler = [&] {
          if constexpr (sizeof...(Args) == 0) {
            return std::make_unique<H>();
          } else {
            auto& storedArgs = *static_cast<ArgsTuple*>(node.owner.get());
            return std::apply(
                [](auto&... stored) {
                  return std::make_unique<H>(
                      forwardStoredHandlerArg<Args>(stored)...);
                },
                storedArgs);
          }
        }();
        auto previousOwner = std::move(node.owner);
        auto* handlerPtr = handler.release();
        node.owner = decltype(node.owner)(
            handlerPtr, +[](void* p) noexcept { delete static_cast<H*>(p); });
        node.handlerPtr = handlerPtr;
      }};
  node.handlerPtr = const_cast<HandlerConstructionOps*>(&kConstructionOps);
  return node;
}

template <typename H, typename StateTuple = std::tuple<>, typename... Args>
  requires HandlerLifecycle<H, ContextFor<StateTuple, EvbExecutionDomain>>
HandlerNode makeInlineHandlerNode(HandlerId handlerId, Args&&... args) {
  return makeInlineHandlerNodeT<EvbExecutionDomain, H, StateTuple>(
      handlerId, std::forward<Args>(args)...);
}

} // namespace apache::thrift::fast_thrift::channel_pipeline::detail
