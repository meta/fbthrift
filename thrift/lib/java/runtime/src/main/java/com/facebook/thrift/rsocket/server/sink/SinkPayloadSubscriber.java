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

package com.facebook.thrift.rsocket.server.sink;

import com.facebook.thrift.server.sink.SinkChunkTimeoutException;
import com.facebook.thrift.server.sink.SinkOptions;
import com.google.common.annotations.VisibleForTesting;
import io.netty.util.ReferenceCountUtil;
import io.rsocket.Payload;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.LongConsumer;
import org.apache.thrift.RpcKind;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

/**
 * The one subscriber to a channel's inbound payloads. It takes the first payload as the request
 * without a REQUEST_N, buffers the rest as sink payloads, and gives the reader a bounded credit
 * window that opens after the first response. It also ends the reader with the call, because only
 * the transport sees how the call ends.
 *
 * <p>Runtime-internal: used by the RSocket server transport in the parent package.
 */
public final class SinkPayloadSubscriber extends BaseSubscriber<Payload> {
  private static final LongConsumer NO_CREDIT_LISTENER = n -> {};

  // Only responder signals emit here, so the emits stay serial.
  private final Sinks.Many<Payload> buffer = Sinks.many().unicast().onBackpressureBuffer();

  // Only the first response emits here. The end of the call and a chunk timeout both end the read
  // on callEnd; the first emission wins.
  private final Sinks.Empty<Void> firstResponse = Sinks.empty();
  private final Sinks.Empty<Void> callEnd = Sinks.empty();

  // Set before callEnd fires, so a reader that the end of the call stops can tell how it ended.
  private volatile boolean aborted;

  private final AtomicInteger responsesSent = new AtomicInteger();

  private final Scheduler scheduler;
  private final ChunkTimeout chunkTimeout;

  private Payload setup;

  // The peer's inbound subscription, cancelled directly to queue the bidi CANCEL.
  private volatile Subscription inbound;

  /**
   * Reactor Context key for the listener that reports each REQUEST_N credit this subscriber sends
   * the peer, with the n actually requested. A dedicated key object, so no other Context entry can
   * collide with it. The sink dispatch sets it to the call's sink-credit hook; without it (a bidi
   * call, or a dispatch with no stream handlers) no credit fires.
   *
   * <p>Runtime-internal: set by the sink server package.
   */
  public static final Object SINK_CREDIT_LISTENER = new Object();

  /**
   * Reactor Context key for the options a handler applied to its sink with {@link
   * SinkOptions#applyTo}. The application nearest the sink wins. Without it the sink reads with the
   * defaults: no chunk timeout and a window of 100 payloads.
   *
   * <p>Runtime-internal: written by {@link SinkOptions#applyTo}.
   */
  public static final Object SINK_OPTIONS = new Object();

  /**
   * The scheduler runs the decoding of the sink payloads: the off-loop scheduler when execution is
   * forced off the event loop, else {@code Schedulers.immediate()}.
   *
   * <p>Runtime-internal: called by the RSocket server transport.
   */
  public SinkPayloadSubscriber(Scheduler scheduler) {
    this(scheduler, new ChunkTimeout());
  }

  @VisibleForTesting
  SinkPayloadSubscriber(Scheduler scheduler, ChunkTimeout chunkTimeout) {
    this.scheduler = scheduler;
    this.chunkTimeout = chunkTimeout;
  }

  /**
   * The first payload, which the subscribe call delivers before it returns.
   *
   * <p>Runtime-internal: read by the RSocket server transport.
   */
  public Payload setup() {
    return setup;
  }

  /**
   * Counts a response that passed the task timeout on its way to the peer.
   *
   * <p>Runtime-internal: called by the RSocket server transport.
   */
  public void responseSent() {
    responsesSent.incrementAndGet();
  }

  /**
   * Counts a response that passed the task timeout on its way to the peer.
   *
   * <p>Runtime-internal: read by the RSocket server transport.
   */
  public int responsesSent() {
    return responsesSent.get();
  }

  @Override
  protected void hookOnSubscribe(Subscription subscription) {
    inbound = subscription;
    subscription.request(1);
  }

  @Override
  protected void hookOnNext(Payload payload) {
    if (setup == null) {
      setup = payload;

      return;
    }

    chunkTimeout.chunkReceived();

    if (buffer.tryEmitNext(payload).isFailure()) {
      payload.release();
    }
  }

  @Override
  protected void hookOnComplete() {
    chunkTimeout.stopWatching();
    buffer.tryEmitComplete();
  }

  @Override
  protected void hookOnError(Throwable t) {
    chunkTimeout.stopWatching();
    buffer.tryEmitError(t);
  }

  /**
   * Returns the sink payloads of a sink call. A reader cancel stays local, because a sink peer
   * rejects a CANCEL before the final response; the end of the call cancels the sink.
   *
   * <p>Runtime-internal: read by the RSocket server transport.
   */
  public Flux<Payload> sinkPayloads() {
    return sinkPayloads(false);
  }

  /**
   * Returns the sink payloads of a bidi call: a reader cancel after the first response ends it.
   *
   * <p>Runtime-internal: read by the RSocket server transport.
   */
  public Flux<Payload> bidiSinkPayloads() {
    return sinkPayloads(true);
  }

  /**
   * Drops what the handler returns after a chunk timeout, before it serializes, and ends the
   * responses once the timed-out reader failed.
   *
   * <p>Runtime-internal: applied by the RSocket server transport before it encodes the responses.
   */
  public <T> Function<Flux<T>, Flux<T>> dropResponsesAfterChunkTimeout() {
    return chunkTimeout.dropResponsesAfterTimeout();
  }

  /**
   * Maps a failure that escapes after a chunk timeout to the chunk-timeout ERROR.
   *
   * <p>Runtime-internal: applied by the RSocket server transport before it maps other failures
   * after the first response.
   */
  public <T> Function<Flux<T>, Flux<T>> mapFailuresAfterChunkTimeout(RpcKind kind) {
    return chunkTimeout.mapFailuresAfterTimeout(kind);
  }

  /**
   * Ends timed-out responses with the chunk-timeout ERROR that a peer of this kind reads.
   *
   * <p>Runtime-internal: applied by the RSocket server transport last, after all error mapping.
   */
  public <T> Function<Flux<T>, Flux<T>> endWithChunkTimeoutError(RpcKind kind) {
    return chunkTimeout.endWithTimeoutError(kind);
  }

  /** Returns the sink payloads to one reader, with the options that reader applied. */
  private Flux<Payload> sinkPayloads(boolean cancelOnReaderCancel) {
    return Flux.deferContextual(
        context -> {
          Reader reader = new Reader(context, cancelOnReaderCancel);

          Flux<Payload> read =
              buffer.asFlux().doOnSubscribe(reader::accept).doOnRequest(reader::grantCredit);

          if (cancelOnReaderCancel) {
            read = read.doOnCancel(this::cancel);
          }

          return window(read, reader.bufferSize()).doFinally(reader::release);
        });
  }

  /**
   * Holds the reader's demand and cancel until the first response is queued, then hands the sink
   * payloads to the reader through an owned window that refills as the reader reads, on the
   * scheduler of this subscriber. The end of the call or a chunk timeout cancels the window, which
   * releases what it holds, and then ends the reader; only a completing other source makes
   * takeUntilOther cancel its source.
   */
  private Flux<Payload> window(Flux<Payload> sinkPayloads, int prefetch) {
    return SinkHandoff.read(
            sinkPayloads.delaySubscription(firstResponse.asMono()), scheduler, prefetch)
        .takeUntilOther(callEnd.asMono())
        .concatWith(Mono.defer(this::endOfRead))
        .doOnDiscard(Payload.class, ReferenceCountUtil::safeRelease);
  }

  /** Fails the reader for a chunk timeout or an abnormal end, else completes it. */
  private Mono<Payload> endOfRead() {
    if (chunkTimeout.hasTimedOut()) {
      return Mono.error(new SinkChunkTimeoutException());
    }

    return aborted
        ? Mono.error(new CancellationException("The call ended before its sink"))
        : Mono.empty();
  }

  /**
   * Starts the sink after the responder queued the first response. The responder queues a payload
   * inside onNext, so credit or a cancel after onNext returns follows that payload on any thread.
   * Reactor has no operator that runs after onNext, so a pass-through subscriber does it.
   *
   * <p>Runtime-internal: called by the RSocket server transport.
   */
  public Flux<Payload> startSinkAfterFirstResponse(Flux<Payload> responses) {
    Function<? super Publisher<Payload>, ? extends Publisher<Payload>> signal =
        Operators.lift((scannable, actual) -> new FirstResponseSignal(actual));

    return Flux.from(signal.apply(responses));
  }

  /**
   * Cancels the inbound peer subscription directly, so the expiry thread queues the bidi CANCEL
   * itself whatever the reader thread is doing. Safe to call twice; a later cancel is a no-op.
   */
  private void cancelInbound() {
    inbound.cancel();
  }

  /**
   * Ends the subscription and releases the sink payloads that no reader took. A normal end of the
   * call completes the reader; any other end fails it, so a handler never takes a cut sink for a
   * whole one.
   *
   * <p>Runtime-internal: called by the RSocket server transport.
   */
  public void end(SignalType signal) {
    dispose();

    aborted = signal != SignalType.ON_COMPLETE;
    chunkTimeout.stopWatching();
    callEnd.tryEmitEmpty();

    // The drain releases the sink payloads that no reader took. It fails at once when a reader took
    // the buffer: the end of the call then cancels that reader's window, which releases its
    // payloads, and a later sink payload finds the buffer cancelled and is released on arrival.
    buffer.asFlux().onErrorComplete().subscribe(ReferenceCountUtil::safeRelease);
  }

  /**
   * Ends the sink of a completed bidi call when no reader took it. A reader keeps the sink open
   * until the peer ends it or the reader cancels; a later reader fails.
   *
   * <p>Runtime-internal: called by the RSocket server transport.
   */
  public void endIfUnread() {
    if (buffer.currentSubscriberCount() == 0) {
      end(SignalType.CANCEL);
    }
  }

  /**
   * One subscription to the sink payloads, with the options it read from its Context once. The
   * buffer accepts only one reader: only that one grants credit and owns the chunk watchdog.
   */
  private final class Reader {
    private final SinkOptions options;
    private final LongConsumer creditListener;
    private final boolean cancelSinkOnTimeout;
    private volatile boolean accepted;

    Reader(ContextView context, boolean cancelSinkOnTimeout) {
      this.options = context.getOrDefault(SINK_OPTIONS, SinkOptions.builder().build());
      this.creditListener = context.getOrDefault(SINK_CREDIT_LISTENER, NO_CREDIT_LISTENER);
      this.cancelSinkOnTimeout = cancelSinkOnTimeout;
    }

    int bufferSize() {
      return options.getBufferSize();
    }

    /**
     * Marks the reader accepted and watches its sink. The buffer rejects a second reader with an
     * empty subscription, which never carries a chunk.
     */
    void accept(Subscription subscription) {
      if (subscription == Operators.emptySubscription()) {
        return;
      }

      accepted = true;
      chunkTimeout.watch(
          options.getChunkTimeout(),
          cancelSinkOnTimeout ? SinkPayloadSubscriber.this::cancelInbound : () -> {},
          callEnd::tryEmitEmpty);
    }

    /**
     * Forwards the window's demand to the peer as REQUEST_N and reports it to the credit listener.
     * The timeout counts it first, so a chunk that arrives on the request sees the new credit. The
     * hand-off between the reader and this tap caps the amounts to the window's own. None flows
     * from a rejected reader or after a chunk timeout.
     */
    void grantCredit(long n) {
      if (!accepted || chunkTimeout.hasTimedOut()) {
        return;
      }

      chunkTimeout.creditGranted(n);
      request(n);
      creditListener.accept(n);
    }

    /**
     * Only the accepted reader owns the watchdog, so only its end stops the timer and ends the
     * responses after a chunk timeout.
     */
    void release(SignalType signal) {
      if (accepted) {
        chunkTimeout.stopWatching();
        chunkTimeout.readerEnded();
      }
    }
  }

  /** Passes every signal through and starts the sink after the first onNext returned. */
  private final class FirstResponseSignal implements CoreSubscriber<Payload>, Subscription {
    private final CoreSubscriber<? super Payload> actual;
    private Subscription upstream;
    private boolean sent;

    FirstResponseSignal(CoreSubscriber<? super Payload> actual) {
      this.actual = actual;
    }

    @Override
    public void onSubscribe(Subscription subscription) {
      upstream = subscription;
      actual.onSubscribe(this);
    }

    @Override
    public void onNext(Payload payload) {
      actual.onNext(payload);

      if (!sent) {
        sent = true;
        firstResponse.tryEmitEmpty();
      }
    }

    @Override
    public void onError(Throwable t) {
      actual.onError(t);
    }

    @Override
    public void onComplete() {
      actual.onComplete();
    }

    @Override
    public void request(long n) {
      upstream.request(n);
    }

    @Override
    public void cancel() {
      upstream.cancel();
    }

    @Override
    public Context currentContext() {
      return actual.currentContext();
    }
  }
}
