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

#include <chrono>
#include <optional>
#include <type_traits>

#include <fmt/core.h>
#include <folly/ExceptionWrapper.h>
#include <folly/Function.h>
#include <folly/io/IOBuf.h>
#include <folly/io/IOBufQueue.h>
#include <folly/io/async/AsyncSocketException.h>
#include <folly/io/async/AsyncTimeout.h>
#include <folly/io/async/AsyncTransport.h>
#include <folly/io/async/DelayedDestruction.h>
#include <folly/logging/xlog.h>
#include <thrift/lib/cpp/transport/TTransportException.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/Common.h>
#include <thrift/lib/cpp2/fast_thrift/channel_pipeline/PipelineRef.h>
#include <thrift/lib/cpp2/fast_thrift/transport/Parser.h>
#include <thrift/lib/cpp2/fast_thrift/transport/WriteCompletion.h>

namespace apache::thrift::fast_thrift::frame::read {
class FrameLengthParser;
}

namespace apache::thrift::fast_thrift::transport {

namespace detail {

bool requiresMovableReadCallback(
    const folly::AsyncTransport& transport) noexcept;

bool setSocketBufferCallback(
    folly::AsyncTransport& transport,
    folly::AsyncTransport::BufferCallback* callback) noexcept;

} // namespace detail

/**
 * Bridges the channel_pipeline framework with an async socket.
 *
 * Templated on a `WriteCompleteEventFactory`-satisfying type (see
 * WriteCompletion.h). The default `NoOpWriteCompleteEventFactory` causes
 * signalWriteComplete (the event hook in writeSuccess/writeErr) to be fully
 * elided via `if constexpr` (detected via `std::is_same_v`). Pipelines that
 * want per-write notifications instantiate with a real factory.
 *
 * Also templated on a `Parser`-satisfying type (see Parser.h), which owns the
 * inbound buffer and cuts the byte stream into whole messages. Composing the
 * parser in rather than dispatching to it keeps the calls direct and the drain
 * loop inlined into readDataAvailable. A pipeline that wants to choose framing
 * per connection at runtime supplies a parser that dispatches internally; the
 * transport stays a direct call either way.
 *
 * FrameLengthParser is the default for fast_thrift's RSocket wire format.
 * Callers that use a different frame format supply its parser explicitly.
 *
 * State machine (one-directional; per-connection, not reused):
 *   Created   --setPipeline-->   Ready
 *   Ready     --onConnect-->     Open
 *   Open      --close (any)-->   Closing
 *   Closing   --drain done-->    Closed
 *
 * Disconnect signals (Open -> Closing): close(), readEOF, readErr, writeErr,
 * pipeline read error. The SocketDrainer takes a DestructorGuard while
 * draining so the handler outlives any WriteRequests still queued in
 * AsyncSocket. Closing -> Closed when writePending_ hits 0 (natural drain)
 * or the drain timeout fires (force closeNow cascades writeErrs).
 */
template <
    WriteCompleteEventFactory Factory,
    Parser ParserT = frame::read::FrameLengthParser>
class TransportHandlerT : public folly::DelayedDestruction,
                          public folly::AsyncTransport::ReadCallback,
                          public folly::AsyncTransport::WriteCallback,
                          public folly::AsyncTransport::BufferCallback {
 public:
  using BytesPtr = apache::thrift::fast_thrift::channel_pipeline::BytesPtr;
  using Result = apache::thrift::fast_thrift::channel_pipeline::Result;
  using TypeErasedBox =
      apache::thrift::fast_thrift::channel_pipeline::TypeErasedBox;
  using PublishedEvents = typename Factory::PublishedEvents;
  using EventPublisher =
      channel_pipeline::PipelineEventPublisherRef<PublishedEvents>;

  enum class State : uint8_t {
    Created,
    Ready,
    Open,
    Closing,
    Closed,
  };

  struct DebugState {
    State state;
    bool isPaused;
    bool isWriteSaturated;
    uint32_t pendingWrites;
    bool hasPipeline;
    bool isDraining;
  };

  static constexpr std::chrono::milliseconds kDefaultDrainTimeout{30'000};

  TransportHandlerT(const TransportHandlerT&) = delete;
  TransportHandlerT& operator=(const TransportHandlerT&) = delete;
  TransportHandlerT(TransportHandlerT&&) = delete;
  TransportHandlerT& operator=(TransportHandlerT&&) = delete;

  using Ptr =
      std::unique_ptr<TransportHandlerT, folly::DelayedDestruction::Destructor>;

  // Buffer sizing defaults come from the parser: each protocol knows what its
  // messages cost, and the transport no longer has an opinion.
  static Ptr create(
      folly::AsyncTransport::UniquePtr socket,
      size_t minBufferSize = ParserT::kDefaultMinBufferSize,
      size_t maxBufferSize = ParserT::kDefaultMaxBufferSize,
      std::chrono::milliseconds drainTimeout = kDefaultDrainTimeout) {
    return Ptr(new TransportHandlerT(
        std::move(socket),
        ParserT(minBufferSize, maxBufferSize),
        drainTimeout));
  }

  static Ptr createWithParser(
      folly::AsyncTransport::UniquePtr socket,
      ParserT parser,
      std::chrono::milliseconds drainTimeout = kDefaultDrainTimeout) {
    return Ptr(new TransportHandlerT(
        std::move(socket), std::move(parser), drainTimeout));
  }

  State state() const noexcept { return state_; }

  /**
   * Returns a value-only snapshot for diagnostics and tests.
   *
   * Like the rest of TransportHandler, this must be called on the owning
   * EventBase thread. It deliberately reports presence and activity as
   * booleans rather than exposing internal object addresses.
   */
  DebugState debugState() const noexcept {
    return DebugState{
        .state = state_,
        .isPaused = readPaused_,
        .isWriteSaturated = writeSaturated_,
        .pendingWrites = writePending_,
        .hasPipeline = static_cast<bool>(pipeline_),
        .isDraining = socketDrainer_.active(),
    };
  }

  /** Invoked once when an open transport reaches Closed. */
  void setOnClosed(folly::Function<void() noexcept> callback) {
    onClosed_ = std::move(callback);
  }

  template <channel_pipeline::PipelineRefTarget P>
  void setPipeline(P* pipeline) noexcept {
    DCHECK(pipeline != nullptr);
    setPipeline(channel_pipeline::PipelineRef(*pipeline));
  }

  void setPipeline(channel_pipeline::PipelineRef pipeline) noexcept {
    DCHECK(pipeline);
    DCHECK(state_ == State::Created);
    pipeline_ = pipeline;
    eventPublisher_ = pipeline_.bindEvents<PublishedEvents>();
    pipelineGuard_ = pipeline_.guard();
    // Read buffers come from the pipeline's allocator, same as every other
    // buffer the pipeline hands around. Wired here rather than at
    // construction because the pipeline is not known until now.
    bufFactory_ = [pipeline = pipeline_](size_t capacity) noexcept {
      return pipeline.allocate(capacity);
    };
    parser_.setIOBufFactory(&bufFactory_);
    state_ = State::Ready;
    bufferCallbackRegistered_ = detail::setSocketBufferCallback(*socket_, this);
  }

  bool setZeroCopy(bool enable) noexcept {
    return socket_->setZeroCopy(enable);
  }

  void setZeroCopyEnableThreshold(size_t threshold) noexcept {
    socket_->setZeroCopyEnableThreshold(threshold);
  }

  void onConnect() noexcept {
    DCHECK(state_ == State::Ready);
    DCHECK(socket_->good());
    state_ = State::Open;
    const bool unsupportedReadCallback = !MovableBufferParser<ParserT> &&
        detail::requiresMovableReadCallback(*socket_);
    if (unsupportedReadCallback) [[unlikely]] {
      pipeline_.activate();
      closeImmediately(
          folly::AsyncSocketException(
              folly::AsyncSocketException::NOT_SUPPORTED,
              "Transport requires a movable read callback"));
    } else {
      resumeRead();
      pipeline_.activate();
    }
  }

  // --- AsyncTransport::ReadCallback interface ---

  void getReadBuffer(void** bufReturn, size_t* lenReturn) override {
    parser_.getReadBuffer(bufReturn, lenReturn);
  }

  void readDataAvailable(size_t len) noexcept override {
    folly::DelayedDestruction::DestructorGuard dg(this);
    DCHECK(pipeline_);
    handleReadResult(parser_.consume(len, messageSink()));
  }

  void readBufferAvailable(
      std::unique_ptr<folly::IOBuf> buf) noexcept override {
    if constexpr (MovableBufferParser<ParserT>) {
      folly::DelayedDestruction::DestructorGuard dg(this);
      DCHECK(pipeline_);
      handleReadResult(parser_.consumeBuffer(std::move(buf), messageSink()));
    } else {
      DCHECK(false) << "readBufferAvailable on a parser without consumeBuffer";
      (void)buf;
    }
  }

  void readEOF() noexcept override {
    folly::DelayedDestruction::DestructorGuard dg(this);
    closeGracefully(
        apache::thrift::transport::TTransportException(
            apache::thrift::transport::TTransportException::
                TTransportExceptionType::END_OF_FILE,
            "Channel got EOF."));
  }

  void readErr(const folly::AsyncSocketException& ex) noexcept override {
    folly::DelayedDestruction::DestructorGuard dg(this);
    closeGracefully(apache::thrift::transport::TTransportException(ex));
  }

  bool isBufferMovable() noexcept override {
    return MovableBufferParser<ParserT>;
  }

  // --- TailEndpointHandler lifecycle ---

  void handlerAdded() noexcept {}

  void handlerRemoved() noexcept {
    folly::DelayedDestruction::DestructorGuard dg(this);
    closeImmediately(folly::exception_wrapper());
  }

  void onPipelineActive() noexcept {}
  void onPipelineInactive() noexcept {}

  void onReadReady() noexcept { resumeRead(); }

  // --- TailEndpointHandler interface (OutboundTransportHandler refines) ---

  template <typename Context>
  Result onWrite(Context&, TypeErasedBox&& msg) noexcept {
    folly::DelayedDestruction::DestructorGuard dg(this);
    auto bytes = std::move(msg.template get<BytesPtr>());
    DCHECK(bytes);
    // Accept writes from Ready (pre-connect) too — AsyncSocket buffers them
    // until the connect completes and then flushes. Without this, a client
    // that fires send() immediately after constructing the socket racing
    // against connectSuccess() silently drops the bytes. Only refuse in
    // terminal / pre-pipeline states.
    if (state_ != State::Open && state_ != State::Ready) [[unlikely]] {
      return Result::Error;
    }

    ++writePending_;

    try {
      socket_->writeChain(this, std::move(bytes));
    } catch (...) {
      --writePending_;
      return Result::Error;
    }

    // AsyncSocket reports buffering before writeChain returns. Transports
    // without an underlying AsyncSocket retain pending-write flow control.
    const bool backpressured =
        bufferCallbackRegistered_ ? socketBuffered_ : writePending_ != 0;
    if (!backpressured) {
      return Result::Success;
    }
    writeSaturated_ = true;
    return Result::Backpressure;
  }

  void pauseRead() noexcept {
    folly::DelayedDestruction::DestructorGuard dg(this);
    if (readPaused_) [[unlikely]] {
      return;
    }
    readPaused_ = true;
    socket_->setReadCB(nullptr);
  }

  void resumeRead() noexcept {
    folly::DelayedDestruction::DestructorGuard dg(this);
    if (!readPaused_ || state_ >= State::Closing) [[unlikely]] {
      return;
    }
    readPaused_ = false;
    handleReadResult(parser_.resume(messageSink()));
    if (!readPaused_ && state_ < State::Closing) {
      socket_->setReadCB(this);
    }
  }

  // Graceful disconnect: pending writes drain naturally; drain timeout is
  // the safety net. Idempotent (no-op outside Open).
  void close(folly::exception_wrapper&& ex) noexcept {
    folly::DelayedDestruction::DestructorGuard dg(this);
    closeGracefully(std::move(ex));
  }

  // Destruction helper: detaches the pipeline. Forces a non-graceful close
  // from Open so the queue is drained inline (no caller left to wait).
  // Not a public state-machine entry; ~T calls this.
  void resetPipeline() noexcept {
    folly::DelayedDestruction::DestructorGuard dg(this);
    // Pipeline must not be actively dispatching when we drop it.
    DCHECK(state_ != State::Open);
    if (bufferCallbackRegistered_) {
      const bool detached = detail::setSocketBufferCallback(*socket_, nullptr);
      DCHECK(detached);
      bufferCallbackRegistered_ = false;
    }
    onClosed_ = nullptr;
    eventPublisher_ = {};
    pipeline_.reset();
    pipelineGuard_.reset();
  }

  // --- AsyncTransport::WriteCallback interface ---

  void writeSuccess() noexcept override {
    folly::DelayedDestruction::DestructorGuard dg(this);
    --writePending_;
    if (state_ == State::Open) {
      DCHECK(pipeline_);
      signalWriteComplete(WriteCompletionStatus::Success, 0);
      maybeSignalWriteReady();
      return;
    }
    maybeReleaseDrainer();
  }

  void writeErr(
      size_t bytesWritten,
      const folly::AsyncSocketException& ex) noexcept override {
    folly::DelayedDestruction::DestructorGuard dg(this);
    --writePending_;
    signalWriteComplete(WriteCompletionStatus::Error, bytesWritten);
    closeGracefully(
        folly::AsyncSocketException(
            ex.getType(),
            fmt::format(
                "Write failed with bytesWritten={}: {}",
                bytesWritten,
                ex.what()),
            ex.getErrno()));
    maybeReleaseDrainer();
  }

  void onEgressBuffered() noexcept override { socketBuffered_ = true; }

  void onEgressBufferCleared() noexcept override {
    folly::DelayedDestruction::DestructorGuard dg(this);
    socketBuffered_ = false;
    if (state_ == State::Open) {
      maybeSignalWriteReady();
    }
  }

  ~TransportHandlerT() override {
    // Force a non-graceful close from Open so we reach Closed before
    // detaching the pipeline.
    if (state_ == State::Open) {
      closeImmediately(folly::exception_wrapper());
    }
    resetPipeline();
  }

 protected:
  TransportHandlerT(
      folly::AsyncTransport::UniquePtr socket,
      ParserT parser,
      std::chrono::milliseconds drainTimeout)
      : socket_(std::move(socket)),
        parser_(std::move(parser)),
        drainTimeoutDuration_(drainTimeout),
        socketDrainer_(this) {}

 private:
  // Build and fire the per-write completion event through Factory. Elided
  // entirely for the NoOp factory. Completion events fire only while Open:
  // once a close begins (Closing/Closed) both success and error completions
  // are suppressed symmetrically, so consumers must treat pipeline
  // deactivation as abandonment of any writes still in flight. The gate also
  // covers the destructor-triggered close cascade (writeErrs landing after
  // the pipeline detached). Open guarantees pipeline_ is attached. Shared by
  // writeSuccess + writeErr.
  void signalWriteComplete(
      WriteCompletionStatus status, size_t bytes) noexcept {
    if constexpr (!std::is_same_v<Factory, NoOpWriteCompleteEventFactory>) {
      if (state_ == State::Open) [[likely]] {
        DCHECK(pipeline_);
        using Event = typename Factory::TransportWriteCompleteEventType;
        if constexpr (std::is_void_v<typename Event::Payload>) {
          Factory::make(status, bytes);
          eventPublisher_.template fire<Event>();
        } else {
          eventPublisher_.template fire<Event>(Factory::make(status, bytes));
        }
      }
    }
  }

  // onWriteReady is an edge signal — "the socket has room again" — so it
  // fires only for handlers that were previously told to stop, and only once
  // every queued write has landed. Waking on each completion instead would
  // let an absorber drain one message before its next write re-saturated the
  // socket, so a queue would trickle out at one message per completion.
  //
  // This rests on handlers arming ctx.awaitWriteReady() only after a
  // downstream write returned Result::Backpressure, which transitively
  // originates here. A handler that arms on its own internal state instead
  // would never be woken.
  void maybeSignalWriteReady() noexcept {
    if (writeSaturated_ && writePending_ == 0 && !socketBuffered_) {
      writeSaturated_ = false;
      pipeline_.onWriteReady();
    }
  }

  // Sink the parser hands each complete message to. A parser may emit several
  // messages per read, and firing one can close the connection re-entrantly,
  // so the Open check gates every message rather than just the read: the
  // Error return unwinds the parser's drain loop, leaving unconsumed bytes
  // queued for a connection that will never read them again.
  auto messageSink() noexcept {
    return [this](BytesPtr&& bytes) noexcept {
      if (state_ == State::Closing || state_ == State::Closed) [[unlikely]] {
        return Result::Error;
      }
      return pipeline_.fireRead(TypeErasedBox(std::move(bytes)));
    };
  }

  // Dispatch a pipeline read result: Success falls through, Backpressure
  // pauses reads, Error closes gracefully. Shared by readDataAvailable +
  // readBufferAvailable.
  void handleReadResult(Result result) noexcept {
    if (result == Result::Success) [[likely]] {
      return;
    }
    if (result == Result::Backpressure) {
      pauseRead();
    } else {
      DCHECK(result == Result::Error);
      closeGracefully(
          folly::AsyncSocketException(
              folly::AsyncSocketException::UNKNOWN, "Pipeline read error"));
    }
  }

  // If draining toward close and the last pending write just landed, drop
  // the drain guard and finish closing. Shared by writeSuccess + writeErr.
  void maybeReleaseDrainer() noexcept {
    // Pre-connect writes (state == Ready) can land in writeSuccess too, but
    // they must not collapse the state machine into Closed.
    if (state_ == State::Closing && writePending_ == 0) {
      socketDrainer_.release();
    }
  }

  // Open -> Closing prelude: state flip, pause reads, notify pipeline.
  // Returns false if state wasn't Open (idempotent no-op). Doesn't decide
  // how to reach Closed — callers compose with the drain/closeNow step.
  bool beginClose(folly::exception_wrapper ex) noexcept {
    if (state_ != State::Open) {
      return false;
    }
    state_ = State::Closing;
    pauseRead();
    // Reads are done for good, so drop the inbound buffer now rather than at
    // destruction — a graceful close can sit in Closing for the whole drain
    // timeout while pending writes land.
    parser_.reset();
    if (pipeline_) {
      if (ex) {
        pipeline_.fireException(std::move(ex));
      }
      pipeline_.deactivate();
    }
    return true;
  }

  // Graceful: let pending writes finish via writeSuccess; SocketDrainer is
  // the timeout safety net. No pending writes collapses straight to Closed.
  void closeGracefully(folly::exception_wrapper ex) noexcept {
    if (!beginClose(std::move(ex))) {
      return;
    }
    if (writePending_ > 0) {
      socketDrainer_.start();
    } else {
      closeNow();
    }
  }

  // Immediate: closeNow() inline cascades writeErrs synchronously and drains
  // writePending_ to 0 before returning.
  void closeImmediately(folly::exception_wrapper ex) noexcept {
    if (!beginClose(std::move(ex))) {
      return;
    }
    closeNow();
  }

  // Closing -> Closed. Idempotent (no-op outside Closing).
  void closeNow() noexcept {
    if (state_ == State::Closed) {
      return;
    }
    // Set state first so any re-entrant writeErrs from the cascade below
    // see a terminal state and don't re-enter beginClose.
    state_ = State::Closed;
    socket_->closeNow();
    if (onClosed_) {
      auto callback = std::move(onClosed_);
      callback();
    }
  }

  // Owns the drain DestructorGuard and the timeout. start() takes the guard
  // and schedules the timer; release() drops both. The timer firing forces a
  // closeNow() on the socket to cascade writeErrs and drain the queue, then
  // releases itself.
  class SocketDrainer : public folly::AsyncTimeout {
   public:
    explicit SocketDrainer(TransportHandlerT* self)
        : folly::AsyncTimeout(self->socket_->getEventBase()), self_(self) {}

    void start() {
      if (guard_.has_value()) {
        return;
      }
      guard_.emplace(self_);
      scheduleTimeout(self_->drainTimeoutDuration_);
    }

    void release() {
      cancelTimeout();
      guard_.reset();
      self_->closeNow();
    }

    bool active() const noexcept { return guard_.has_value(); }

    void timeoutExpired() noexcept override {
      folly::DelayedDestruction::DestructorGuard dg(self_);
      if (self_->state_ == State::Closing && self_->writePending_ > 0) {
        XLOGF(
            WARNING,
            "TransportHandler drain timed out with writePending={}; force-closing socket",
            self_->writePending_);
      }
      release();
    }

   private:
    TransportHandlerT* self_;
    std::optional<folly::DelayedDestruction::DestructorGuard> guard_;
  };

  folly::AsyncTransport::UniquePtr socket_;
  // Borrowed by parser_ via setIOBufFactory, so it must outlive it: members
  // are destroyed in reverse declaration order, hence declared before it.
  folly::IOBufFactory bufFactory_;
  ParserT parser_;
  std::chrono::milliseconds drainTimeoutDuration_;
  channel_pipeline::PipelineRef pipeline_;
  EventPublisher eventPublisher_;
  channel_pipeline::PipelineGuard pipelineGuard_;
  folly::Function<void() noexcept> onClosed_;
  State state_{State::Created};
  bool readPaused_{true};
  bool writeSaturated_{false};
  bool socketBuffered_{false};
  bool bufferCallbackRegistered_{false};
  uint32_t writePending_{0};
  SocketDrainer socketDrainer_;
};

} // namespace apache::thrift::fast_thrift::transport
