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

#include <folly/CPortability.h>
#include <folly/ExceptionWrapper.h>
#include <folly/IntrusiveList.h>
#include <folly/Likely.h>
#include <folly/io/async/EventBase.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Event.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/ExecutionDomain.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Handler.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/HandlerTag.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineRef.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>

namespace apache::thrift::fast_thrift::channel_pipeline {

namespace detail::pipeline_instance {

/**
 * One entry in a pipeline's compile-time shape: the handler type and its id.
 * Handler is a (context-ignoring) alias so the same storage machinery can host
 * a context-templated handler later without changing the slot.
 */
template <typename H, HandlerId Id>
struct HandlerSpec {
  template <typename>
  using Handler = H;
  static constexpr HandlerId id = Id;
};

/**
 * Per-handler context for a PipelineInstance, parameterized on the owning
 * pipeline and the handler's compile-time position. It exposes exactly the
 * uniform pipeline's surface and nothing more: the TaskState and execution
 * domain (via taskState() and executionDomain()), directional propagation,
 * lifecycle, events, and readiness.
 *
 * Unlike StaticContext it carries no allocator/eventBase accessors (a
 * PipelineInstance owns neither — a handler reaches any execution infra through
 * ctx.taskState()) and no typed state<T>() (there is one TaskState). There
 * is no endpoint special case: fireRead always propagates toward the tail and
 * fireWrite toward the head, and firing past either end reaches the seam in the
 * owning Impl.
 */
template <typename Pipeline, std::size_t Index, HandlerId Id>
class Context {
 public:
  explicit Context(Pipeline* pipeline) noexcept : pipeline_(pipeline) {}

  static constexpr HandlerId handlerId() noexcept { return Id; }
  std::size_t handlerIndex() const noexcept {
    return pipeline_->template contextHandlerIndex<Index>();
  }

  void activate() noexcept {}
  void deactivate() noexcept {
    pipeline_->template deactivateFromContext<Index>();
  }
  void close() noexcept { pipeline_->close(); }

  FOLLY_ALWAYS_INLINE Result fireRead(TypeErasedBox&& msg) noexcept {
    return pipeline_->template fireReadFrom<Index + 1>(std::move(msg));
  }
  FOLLY_ALWAYS_INLINE Result fireWrite(TypeErasedBox&& msg) noexcept {
    return pipeline_->template fireWriteFrom<Index>(std::move(msg));
  }
  FOLLY_ALWAYS_INLINE void fireException(
      folly::exception_wrapper&& e) noexcept {
    pipeline_->template fireExceptionFrom<Index + 1>(std::move(e));
  }

  // Application state and resources, including allocators and sockets.
  decltype(auto) taskState() noexcept { return pipeline_->taskState(); }
  decltype(auto) taskState() const noexcept {
    return std::as_const(*pipeline_).taskState();
  }

  decltype(auto) executionDomain() noexcept {
    return pipeline_->executionDomain();
  }
  decltype(auto) executionDomain() const noexcept {
    return std::as_const(*pipeline_).executionDomain();
  }

  Pipeline* pipeline() const noexcept { return pipeline_; }

  // Backpressure readiness: a handler arms from its onWrite/onRead after a
  // Backpressure result and is woken by the pipeline's
  // onWriteReady/onReadReady.
  void awaitWriteReady() noexcept {
    pipeline_->template awaitWriteReady<Index>();
  }
  void cancelAwaitWriteReady() noexcept {
    pipeline_->template cancelAwaitWriteReady<Index>();
  }
  bool isAwaitingWriteReady() const noexcept {
    return pipeline_->template isAwaitingWriteReady<Index>();
  }
  void awaitReadReady() noexcept {
    pipeline_->template awaitReadReady<Index>();
  }
  void cancelAwaitReadReady() noexcept {
    pipeline_->template cancelAwaitReadReady<Index>();
  }
  bool isAwaitingReadReady() const noexcept {
    return pipeline_->template isAwaitingReadReady<Index>();
  }

 private:
  Pipeline* pipeline_;
};

struct ReadyHook {
  folly::IntrusiveListHook hook;
  std::size_t handlerIndex{0};
  std::size_t lastNotifiedGeneration{0};
};

using ReadyList = folly::IntrusiveList<ReadyHook, &ReadyHook::hook>;
struct NoReadyHook {};

/**
 * Inline handler + context storage for one slot. The handler is held with
 * [[no_unique_address]] so a stateless handler costs no bytes. A handler must
 * satisfy at least one full direction (inbound, outbound, or both); dispatch
 * skips the direction it does not implement at compile time.
 */
template <typename Spec, typename Pipeline, std::size_t Index>
class HandlerSlot {
 public:
  using Context = pipeline_instance::Context<Pipeline, Index, Spec::id>;
  using Handler = typename Spec::template Handler<Context>;
  static constexpr HandlerId id = Spec::id;

  static_assert(
      HandlerLifecycle<Handler, Context>,
      "a pipeline handler must implement handlerAdded/handlerRemoved");
  static_assert(
      InboundHandler<Handler, Context> || OutboundHandler<Handler, Context>,
      "a pipeline handler must satisfy at least one full direction "
      "(inbound, outbound, or duplex)");

  template <typename... Args>
    requires std::constructible_from<Handler, Args...>
  explicit HandlerSlot(Pipeline* pipeline, Args&&... args)
      : handler_(std::forward<Args>(args)...), context_(pipeline) {}

  Handler& handler() noexcept { return handler_; }
  const Handler& handler() const noexcept { return handler_; }
  Context& context() noexcept { return context_; }
  const Context& context() const noexcept { return context_; }

  // Public readiness markers opt into storage owned by the slot.
  ReadyHook* writeReadyHook() noexcept {
    if constexpr (requires(Handler& h) { h.writeReadyHook_; }) {
      return &writeReadyHook_;
    }
    return nullptr;
  }
  ReadyHook* readReadyHook() noexcept {
    if constexpr (requires(Handler& h) { h.readReadyHook_; }) {
      return &readReadyHook_;
    }
    return nullptr;
  }

 private:
  [[no_unique_address]] Handler handler_;
  Context context_;
  [[no_unique_address]] std::conditional_t<requires(Handler& h) {
    h.writeReadyHook_;
  }, ReadyHook, NoReadyHook> writeReadyHook_;
  [[no_unique_address]] std::conditional_t<requires(Handler& h) {
    h.readReadyHook_;
  }, ReadyHook, NoReadyHook> readReadyHook_;
};

/**
 * Co-located storage for a pipeline's handler slots, laid out by recursive
 * private inheritance (slot + tail) so every slot lives in the single Impl
 * allocation. get<Index>() returns the slot at a compile-time position.
 */
template <typename Pipeline, std::size_t Index, typename... Specs>
class HandlerStorage;

template <typename Pipeline, std::size_t Index>
class HandlerStorage<Pipeline, Index> {
 public:
  template <typename FactoryTuple>
  HandlerStorage(Pipeline*, FactoryTuple&) noexcept {}
};

template <typename Pipeline, std::size_t Index, typename Spec, typename... Rest>
class HandlerStorage<Pipeline, Index, Spec, Rest...>
    : private HandlerSlot<Spec, Pipeline, Index>,
      private HandlerStorage<Pipeline, Index + 1, Rest...> {
  using Slot = HandlerSlot<Spec, Pipeline, Index>;
  using Tail = HandlerStorage<Pipeline, Index + 1, Rest...>;

 public:
  template <typename FactoryTuple>
  HandlerStorage(Pipeline* pipeline, FactoryTuple& factories)
      : Slot(pipeline, std::move(std::get<Index>(factories))),
        Tail(pipeline, factories) {}

  template <std::size_t Target>
  decltype(auto) get() noexcept {
    if constexpr (Target == Index) {
      return static_cast<Slot&>(*this);
    } else {
      return Tail::template get<Target>();
    }
  }
  template <std::size_t Target>
  decltype(auto) get() const noexcept {
    if constexpr (Target == Index) {
      return static_cast<const Slot&>(*this);
    } else {
      return Tail::template get<Target>();
    }
  }
};

} // namespace detail::pipeline_instance

/**
 * A built, running pipeline — one logical unit of work (e.g. Thrift, Rocket) —
 * and the grouping for its related runtime types. PipelineInstance itself is
 * not templated: the template lives only on the concrete Impl, and the runtime
 * currency is erased.
 *
 *   - Impl<TaskState, Domain, Specs...> the concrete co-located pipeline
 * (uniform dispatch, state and execution domain). Reached through
 *                      PipelineSpec::build(); you rarely name it directly.
 *   - Ptr / Ref        erased owning / non-owning handles — the storable,
 *                      TaskState-agnostic runtime currency. Erasing the
 *                      handle is free on the hot path: per-message
 *                      fireRead/fireWrite run inside the concrete Impl via the
 *                      handlers' own contexts, never through the handle.
 *
 * The pipeline is a pure unit of work: handlers plus the single TaskState
 * state. It owns no executor or allocator; a handler that needs execution
 * infra takes it, owned outside the pipeline, through the TaskState and
 * reaches it via ctx.taskState().
 */
struct PipelineInstance {
  using Ptr = PipelineOwner;
  using Ref = PipelineRef;

  /**
   * Uniform static pipeline: an ordered, co-located set of equal handlers with
   * no distinguished endpoints. The first handler added is the logical head,
   * the last the tail, positionally. Reads enter at index 0 and flow toward the
   * tail; writes enter past the tail and flow toward the head; firing past
   * either end hits the seam (Result::Error until bind lands). The single
   * TaskState is stored as a [[no_unique_address]] member (zero bytes when
   * empty) and reached by handlers through ctx.taskState().
   *
   * allocate()/eventBase() exist only to satisfy the erased PipelineRefTarget
   * interface and always return null — the pipeline owns no allocator/executor,
   * and its Context does not expose them to handlers, so a handler reaching for
   * execution infra must take it through the TaskState instead.
   *
   * Backpressure readiness is implemented: generation-guarded
   * onWriteReady/onReadReady over per-handler hooks, un-privileged from the
   * endpoints. TODO(diff 3): user events (bindEvent/fireBoundEvent currently
   * stubbed). TODO(seam): route past-the-end fires to a bound peer once the
   * bind machinery is rebased in.
   */
  template <typename TaskState, ExecutionDomain Domain, typename... Specs>
  class Impl final {
    using ReadyHook = detail::pipeline_instance::ReadyHook;
    using ReadyList = detail::pipeline_instance::ReadyList;

   public:
    using Self = Impl;
    static constexpr std::size_t kHandlerCount = sizeof...(Specs);
    static_assert(kHandlerCount >= 1, "a pipeline needs at least one handler");

    class Guard {
     public:
      explicit Guard(Self* pipeline) noexcept : pipeline_(pipeline) {
        if (pipeline_ != nullptr) {
          ++pipeline_->guardCount_;
        }
      }
      Guard(const Guard&) = delete;
      Guard& operator=(const Guard&) = delete;
      Guard(Guard&& other) noexcept
          : pipeline_(std::exchange(other.pipeline_, nullptr)) {}
      Guard& operator=(Guard&&) = delete;
      ~Guard() {
        if (pipeline_ != nullptr && --pipeline_->guardCount_ == 0 &&
            pipeline_->destroyPending_) {
          pipeline_->destroyNow();
        }
      }

     private:
      Self* pipeline_;
    };

    struct Deleter {
      void operator()(Self* pipeline) const noexcept {
        if (pipeline != nullptr) {
          pipeline->destroy();
        }
      }
    };
    using Ptr = std::unique_ptr<Self, Deleter>;

    template <typename FactoryTuple>
    Impl(TaskState taskState, Domain domain, FactoryTuple& factories)
        : executionDomain_(std::move(domain)),
          taskState_(std::move(taskState)),
          handlers_(this, factories) {
      initializeHooks(std::make_index_sequence<kHandlerCount>{});
    }

    Impl(const Impl&) = delete;
    Impl& operator=(const Impl&) = delete;
    Impl(Impl&&) = delete;
    Impl& operator=(Impl&&) = delete;

    void finishBuild() noexcept { callHandlerAddedFrom<0>(); }

    // === Lifecycle (uniform: every handler, with ctx; added head->tail,
    // removed and deactivated tail->head) ===

    void activate() noexcept {
      Guard guard(this);
      if (state_ != State::Inactive) {
        return;
      }
      state_ = State::Active;
      activateFrom<0>();
    }

    void deactivate() noexcept {
      Guard guard(this);
      deactivateImpl();
    }

    // === Data path ===

    Result fireRead(TypeErasedBox&& msg) noexcept {
      if (FOLLY_UNLIKELY(isClosed())) {
        return Result::Error;
      }
      Guard guard(this);
      return fireReadFrom<0>(std::move(msg));
    }

    Result fireWrite(TypeErasedBox&& msg) noexcept {
      if (FOLLY_UNLIKELY(isClosed())) {
        return Result::Error;
      }
      Guard guard(this);
      return fireWriteFrom<kHandlerCount>(std::move(msg));
    }

    void fireException(folly::exception_wrapper&& e) noexcept {
      if (FOLLY_UNLIKELY(isClosed())) {
        return;
      }
      Guard guard(this);
      fireExceptionFrom<0>(std::move(e));
    }

    // Reads flow 0 -> kHandlerCount. Past the last handler is the read seam.
    template <std::size_t Index>
    Result fireReadFrom(TypeErasedBox&& msg) noexcept {
      if constexpr (Index == kHandlerCount) {
        return seamRead(std::move(msg)); // past tail
      } else {
        auto& slot = handlerAt<Index>();
        using H = typename std::remove_reference_t<decltype(slot)>::Handler;
        using Context =
            typename std::remove_reference_t<decltype(slot)>::Context;
        if constexpr (InboundHandler<H, Context>) {
          return slot.handler().onRead(slot.context(), std::move(msg));
        } else {
          return fireReadFrom<Index + 1>(std::move(msg));
        }
      }
    }

    // Writes flow kHandlerCount -> 0. Past the first handler is the write seam.
    template <std::size_t Count>
    Result fireWriteFrom(TypeErasedBox&& msg) noexcept {
      if constexpr (Count == 0) {
        return seamWrite(std::move(msg)); // past head
      } else {
        auto& slot = handlerAt<Count - 1>();
        using H = typename std::remove_reference_t<decltype(slot)>::Handler;
        using Context =
            typename std::remove_reference_t<decltype(slot)>::Context;
        if constexpr (OutboundHandler<H, Context>) {
          return slot.handler().onWrite(slot.context(), std::move(msg));
        } else {
          return fireWriteFrom<Count - 1>(std::move(msg));
        }
      }
    }

    template <std::size_t Index>
    void fireExceptionFrom(folly::exception_wrapper&& e) noexcept {
      if constexpr (Index == kHandlerCount) {
        return; // exception seam: dropped while unbound (TODO(seam): route)
      } else {
        auto& slot = handlerAt<Index>();
        using H = typename std::remove_reference_t<decltype(slot)>::Handler;
        using Context =
            typename std::remove_reference_t<decltype(slot)>::Context;
        // Exceptions follow the inbound path and, like the other pipelines, are
        // delivered only to handlers satisfying the full InboundHandler concept
        // (an outbound-only handler is skipped even if it has onException).
        if constexpr (InboundHandler<H, Context>) {
          slot.handler().onException(slot.context(), std::move(e));
        } else {
          fireExceptionFrom<Index + 1>(std::move(e));
        }
      }
    }

    // === Erased PipelineRefTarget surface ===

    // Required by the erased interface but unbacked: this pipeline owns no
    // allocator or executor, so these return null and are not exposed to
    // handlers (a handler reaches execution infra through the TaskState).
    BytesPtr allocate(std::size_t) noexcept { return {}; }
    folly::EventBase* eventBase() const noexcept { return nullptr; }

    // Backpressure readiness (uniform: any handler embedding a writeReadyHook_
    // / readReadyHook_ member participates; no head/tail privilege). A handler
    // arms from its callback via ctx.awaitWriteReady()/awaitReadReady() after a
    // fireWrite/fireRead returned Backpressure, and is woken when the transport
    // calls onWriteReady()/onReadReady().
    template <std::size_t Index>
    void awaitWriteReady() noexcept {
      if (isClosed()) {
        return;
      }
      auto* hook = writeReadyHook<Index>();
      if (hook != nullptr && !hook->hook.is_linked()) {
        writeReadyList_.push_back(*hook);
      }
    }
    template <std::size_t Index>
    void cancelAwaitWriteReady() noexcept {
      auto* hook = writeReadyHook<Index>();
      if (hook != nullptr && hook->hook.is_linked()) {
        hook->hook.unlink();
      }
    }
    template <std::size_t Index>
    bool isAwaitingWriteReady() const noexcept {
      auto* hook = const_cast<Self*>(this)->template writeReadyHook<Index>();
      return hook != nullptr && hook->hook.is_linked();
    }
    template <std::size_t Index>
    void awaitReadReady() noexcept {
      if (isClosed()) {
        return;
      }
      auto* hook = readReadyHook<Index>();
      if (hook != nullptr && !hook->hook.is_linked()) {
        readReadyList_.push_back(*hook);
      }
    }
    template <std::size_t Index>
    void cancelAwaitReadReady() noexcept {
      auto* hook = readReadyHook<Index>();
      if (hook != nullptr && hook->hook.is_linked()) {
        hook->hook.unlink();
      }
    }
    template <std::size_t Index>
    bool isAwaitingReadReady() const noexcept {
      auto* hook = const_cast<Self*>(this)->template readReadyHook<Index>();
      return hook != nullptr && hook->hook.is_linked();
    }

    // Wake armed handlers. Generation-guarded so a handler that re-arms during
    // dispatch is not re-notified until the next call; the pending flag
    // coalesces a re-entrant call into another pass. A handler still blocked
    // re-arms and stops itself, so order affects only how many passes it takes,
    // not correctness.
    void onWriteReady() noexcept {
      Guard guard(this);
      if (isClosed()) {
        return;
      }
      if (writeReadyDispatching_) {
        writeReadyDispatchPending_ = true;
        return;
      }
      writeReadyDispatching_ = true;
      do {
        writeReadyDispatchPending_ = false;
        const auto generation = ++writeReadyGeneration_;
        while (!isClosed()) {
          ReadyHook* next = nullptr;
          for (auto& hook : writeReadyList_) {
            if (hook.lastNotifiedGeneration != generation) {
              next = &hook;
              break;
            }
          }
          if (next == nullptr) {
            break;
          }
          next->lastNotifiedGeneration = generation;
          dispatchWriteReadyTarget(next->handlerIndex);
        }
      } while (writeReadyDispatchPending_ && !isClosed());
      writeReadyDispatching_ = false;
    }

    void onReadReady() noexcept {
      Guard guard(this);
      if (isClosed()) {
        return;
      }
      if (readReadyDispatching_) {
        readReadyDispatchPending_ = true;
        return;
      }
      readReadyDispatching_ = true;
      do {
        readReadyDispatchPending_ = false;
        const auto generation = ++readReadyGeneration_;
        while (!isClosed()) {
          ReadyHook* next = nullptr;
          for (auto& hook : readReadyList_) {
            if (hook.lastNotifiedGeneration != generation) {
              next = &hook;
              break;
            }
          }
          if (next == nullptr) {
            break;
          }
          next->lastNotifiedGeneration = generation;
          dispatchReadReadyTarget(next->handlerIndex);
        }
      } while (readReadyDispatchPending_ && !isClosed());
      readReadyDispatching_ = false;
    }

    // TODO(diff 3): bind/broadcast user events to subscribers.
    BoundEventRoute bindEvent(EventKey key) noexcept {
      return BoundEventRoute{key};
    }
    void fireBoundEvent(BoundEventRoute, const void*) noexcept {}

    void close() noexcept {
      Guard guard(this);
      if (isClosed()) {
        return;
      }
      state_ = State::Closing;
      deactivateFrom<kHandlerCount>();
      state_ = State::Closed;
      // Cleared after state_ == Closed so any re-arm during teardown is
      // rejected by the isClosed() guard in await*Ready; the hooks live in the
      // handlers, which are destroyed after these lists are emptied.
      writeReadyList_.clear();
      readReadyList_.clear();
      callHandlerRemovedFrom<kHandlerCount>();
    }
    bool isClosed() const noexcept {
      return state_ == State::Closing || state_ == State::Closed;
    }

    TaskState& taskState() noexcept { return taskState_; }
    const TaskState& taskState() const noexcept { return taskState_; }

    Domain& executionDomain() noexcept { return executionDomain_; }
    const Domain& executionDomain() const noexcept { return executionDomain_; }

    template <std::size_t Index>
    std::size_t contextHandlerIndex() const noexcept {
      return Index;
    }
    template <std::size_t Index>
    void deactivateFromContext() noexcept {
      deactivateFrom<Index + 1>();
    }

   private:
    enum class State : std::uint8_t { Inactive, Active, Closing, Closed };

    ~Impl() {
      if (state_ != State::Closed) {
        deactivateImpl();
        state_ = State::Closed;
        writeReadyList_.clear();
        readReadyList_.clear();
        callHandlerRemovedFrom<kHandlerCount>();
      }
    }

    void deactivateImpl() noexcept {
      if (state_ != State::Active) {
        return;
      }
      state_ = State::Inactive;
      deactivateFrom<kHandlerCount>();
    }

    void destroy() noexcept {
      if (guardCount_ != 0) {
        destroyPending_ = true;
        return;
      }
      destroyNow();
    }
    void destroyNow() noexcept { delete this; }

    // Seam: past the ends. Unbound on this base -> Error/drop.
    Result seamRead(TypeErasedBox&&) noexcept { return Result::Error; }
    Result seamWrite(TypeErasedBox&&) noexcept { return Result::Error; }

    template <std::size_t Index>
    decltype(auto) handlerAt() noexcept {
      return handlers_.template get<Index>();
    }
    template <std::size_t Index>
    decltype(auto) handlerAt() const noexcept {
      return handlers_.template get<Index>();
    }

    template <std::size_t Index>
    void activateFrom() noexcept {
      if constexpr (Index < kHandlerCount) {
        callLifecycleActive(handlerAt<Index>());
        activateFrom<Index + 1>();
      }
    }
    template <std::size_t Count>
    void deactivateFrom() noexcept {
      if constexpr (Count > 0) {
        callLifecycleInactive(handlerAt<Count - 1>());
        deactivateFrom<Count - 1>();
      }
    }
    template <std::size_t Index>
    void callHandlerAddedFrom() noexcept {
      if constexpr (Index < kHandlerCount) {
        callLifecycleAdded(handlerAt<Index>());
        callHandlerAddedFrom<Index + 1>();
      }
    }
    template <std::size_t Count>
    void callHandlerRemovedFrom() noexcept {
      if constexpr (Count > 0) {
        callLifecycleRemoved(handlerAt<Count - 1>());
        callHandlerRemovedFrom<Count - 1>();
      }
    }

    static void callLifecycleActive(auto& slot) noexcept {
      if constexpr (requires {
                      slot.handler().onPipelineActive(slot.context());
                    }) {
        slot.handler().onPipelineActive(slot.context());
      }
    }
    static void callLifecycleInactive(auto& slot) noexcept {
      if constexpr (requires {
                      slot.handler().onPipelineInactive(slot.context());
                    }) {
        slot.handler().onPipelineInactive(slot.context());
      }
    }
    static void callLifecycleAdded(auto& slot) noexcept {
      if constexpr (requires { slot.handler().handlerAdded(slot.context()); }) {
        slot.handler().handlerAdded(slot.context());
      }
    }
    static void callLifecycleRemoved(auto& slot) noexcept {
      if constexpr (requires {
                      slot.handler().handlerRemoved(slot.context());
                    }) {
        slot.handler().handlerRemoved(slot.context());
      }
    }

    // === Readiness plumbing ===

    // Stamp each slot's hook (if any) with its position so a wake can route
    // back to the right slot by index.
    template <std::size_t... Index>
    void initializeHooks(std::index_sequence<Index...>) noexcept {
      (initializeHook<Index>(), ...);
    }
    template <std::size_t Index>
    void initializeHook() noexcept {
      auto& slot = handlerAt<Index>();
      if (auto* hook = slot.writeReadyHook()) {
        hook->handlerIndex = Index;
      }
      if (auto* hook = slot.readReadyHook()) {
        hook->handlerIndex = Index;
      }
    }
    template <std::size_t Index>
    ReadyHook* writeReadyHook() noexcept {
      return handlerAt<Index>().writeReadyHook();
    }
    template <std::size_t Index>
    ReadyHook* readReadyHook() noexcept {
      return handlerAt<Index>().readReadyHook();
    }

    void dispatchWriteReadyTarget(std::size_t target) noexcept {
      dispatchWriteReady<0>(target);
    }
    template <std::size_t Index>
    void dispatchWriteReady(std::size_t target) noexcept {
      if constexpr (Index < kHandlerCount) {
        if (target == Index) {
          auto& slot = handlerAt<Index>();
          if constexpr (requires {
                          slot.handler().onWriteReady(slot.context());
                        }) {
            slot.handler().onWriteReady(slot.context());
          }
          return;
        }
        dispatchWriteReady<Index + 1>(target);
      }
    }
    void dispatchReadReadyTarget(std::size_t target) noexcept {
      dispatchReadReady<0>(target);
    }
    template <std::size_t Index>
    void dispatchReadReady(std::size_t target) noexcept {
      if constexpr (Index < kHandlerCount) {
        if (target == Index) {
          auto& slot = handlerAt<Index>();
          if constexpr (requires {
                          slot.handler().onReadReady(slot.context());
                        }) {
            slot.handler().onReadReady(slot.context());
          }
          return;
        }
        dispatchReadReady<Index + 1>(target);
      }
    }

    // State and execution resources outlive handler destruction. The ready
    // lists are intrusive (hooks live in the slots) and are cleared before
    // teardown.
    [[no_unique_address]] Domain executionDomain_;
    [[no_unique_address]] TaskState taskState_;
    State state_{State::Inactive};
    std::uint32_t guardCount_{0};
    bool destroyPending_{false};
    bool writeReadyDispatching_{false};
    bool writeReadyDispatchPending_{false};
    bool readReadyDispatching_{false};
    bool readReadyDispatchPending_{false};
    std::size_t writeReadyGeneration_{0};
    std::size_t readReadyGeneration_{0};
    ReadyList writeReadyList_;
    ReadyList readReadyList_;
    detail::pipeline_instance::HandlerStorage<Self, 0, Specs...> handlers_;
  };
};

} // namespace apache::thrift::fast_thrift::channel_pipeline
