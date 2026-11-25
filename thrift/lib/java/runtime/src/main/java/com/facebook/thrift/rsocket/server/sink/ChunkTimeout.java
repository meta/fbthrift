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

import com.facebook.thrift.rsocket.util.RocketErrorUtil;
import com.facebook.thrift.server.sink.SinkChunkTimeoutException;
import com.facebook.thrift.util.resources.RpcResources;
import io.netty.util.Timer;
import io.rsocket.RSocketErrorException;
import java.time.Duration;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.thrift.RpcKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;

/**
 * The chunk timeout of one sink or bidi call: it watches the peer for the accepted reader and, on a
 * stall, fails that reader before it ends the responses with the CHUNK_TIMEOUT ERROR. The reader
 * goes first, so a teardown can never replace its timeout. On the event loop, a reader blocked in
 * {@code onNext} also delays the ERROR.
 */
final class ChunkTimeout {
  private static final Logger LOGGER = LoggerFactory.getLogger(ChunkTimeout.class);

  // Bounds how long a reader stuck in user code can delay the responses' ERROR past the timeout.
  private static final long ERROR_BACKSTOP_DELAY_MILLIS = 10;

  private final Supplier<Timer> timer;
  private final Scheduler scheduler;
  private final LongSupplier nanoClock;

  // Only the accepted reader installs a watchdog, so wire arrivals reach the reader that owns the
  // credit.
  private final AtomicReference<ChunkWatchdog> watchdog = new AtomicReference<>();

  // The reader's terminal or the backstop ends the responses here, whichever comes first.
  private final Sinks.Empty<Void> responsesEnd = Sinks.empty();

  private volatile boolean timedOut;

  ChunkTimeout() {
    this(RpcResources::getHashedWheelTimer, RpcResources.getOffLoopScheduler(), System::nanoTime);
  }

  ChunkTimeout(Supplier<Timer> timer, Scheduler scheduler, LongSupplier nanoClock) {
    this.timer = timer;
    this.scheduler = scheduler;
    this.nanoClock = nanoClock;
  }

  /**
   * Watches the peer for the reader the buffer accepted, unless its timeout is zero. On a stall,
   * the sink half is cancelled first so the peer never sees ERROR on an open sink, then {@code
   * signalReadEnd} ends the read, which fails because {@link #hasTimedOut} holds.
   */
  void watch(Duration timeout, Runnable cancelSink, Runnable signalReadEnd) {
    if (timeout.isZero()) {
      return;
    }

    long delayNanos = TimeUnit.MILLISECONDS.toNanos(timeout.toMillis());

    watchdog.set(
        new ChunkWatchdog(
            timer.get(),
            scheduler,
            delayNanos,
            nanoClock,
            () -> onStall(signalReadEnd, cancelSink)));
  }

  /** Counts credit the accepted reader grants the peer, before the REQUEST_N goes out. */
  void creditGranted(long n) {
    ChunkWatchdog current = watchdog.get();

    if (current != null) {
      current.creditGranted(n);
    }
  }

  /** Records a sink payload from the wire. */
  void chunkReceived() {
    ChunkWatchdog current = watchdog.get();

    if (current != null) {
      current.chunkReceived();
    }
  }

  /** Drops the watchdog when the sink, the call or the accepted read ends. */
  void stopWatching() {
    ChunkWatchdog current = watchdog.getAndSet(null);

    if (current != null) {
      current.stopWatching();
    }
  }

  /**
   * After a timeout, a reader's terminal ends the responses; the backstop covers a stuck reader.
   */
  void readerEnded() {
    if (timedOut) {
      responsesEnd.tryEmitEmpty();
    }
  }

  /** Whether a stall ended this call: no credit flows and every read fails after it. */
  boolean hasTimedOut() {
    return timedOut;
  }

  /**
   * Drops what the handler returns after a timeout and ends the responses once the reader failed.
   * It sits before serialization, so a dropped response owns no buffers.
   */
  <T> Function<Flux<T>, Flux<T>> dropResponsesAfterTimeout() {
    return responses ->
        responses.filter(response -> !timedOut).takeUntilOther(responsesEnd.asMono());
  }

  /**
   * Maps a failure that escapes after a timeout to the CHUNK_TIMEOUT ERROR: it is the timeout's
   * fallout. An ERROR frame passes unchanged.
   */
  <T> Function<Flux<T>, Flux<T>> mapFailuresAfterTimeout(RpcKind kind) {
    return responses ->
        responses.onErrorMap(
            e -> !(e instanceof RSocketErrorException) && timedOut,
            e -> timeoutErrorWithFallout(kind, e));
  }

  /** Ends timed-out responses with the CHUNK_TIMEOUT ERROR that a peer of this kind reads. */
  <T> Function<Flux<T>, Flux<T>> endWithTimeoutError(RpcKind kind) {
    return responses ->
        responses.concatWith(
            Mono.defer(
                () ->
                    timedOut ? Mono.error(RocketErrorUtil.chunkTimeoutError(kind)) : Mono.empty()));
  }

  /**
   * Cancels the sink half first so the peer never sees ERROR on an open sink, then commits the
   * timeout and fails the stalled reader, letting its terminal end the responses. The backstop ends
   * them too, so a reader stuck in user code cannot delay the ERROR past it; if the backstop cannot
   * be scheduled, the responses end right after the reader failed, and a reader cancel in flight at
   * the same instant can still put the ERROR before its CANCEL.
   */
  private void onStall(Runnable signalReadEnd, Runnable cancelSink) {
    cancelSink.run();
    timedOut = true;
    LOGGER.warn("Sink chunk timeout; failing the sink read");

    boolean backstopScheduled = scheduleBackstop();

    signalReadEnd.run();

    if (!backstopScheduled) {
      responsesEnd.tryEmitEmpty();
    }
  }

  private boolean scheduleBackstop() {
    try {
      scheduler.schedule(
          responsesEnd::tryEmitEmpty, ERROR_BACKSTOP_DELAY_MILLIS, TimeUnit.MILLISECONDS);

      return true;
    } catch (RejectedExecutionException e) {
      return false;
    }
  }

  /** Keeps the fallout for the log; the timeout itself is not logged twice. */
  private static RSocketErrorException timeoutErrorWithFallout(RpcKind kind, Throwable fallout) {
    RSocketErrorException timeout = RocketErrorUtil.chunkTimeoutError(kind);

    if (!(fallout instanceof SinkChunkTimeoutException)) {
      LOGGER.warn("Failure after sink chunk timeout", fallout);
    }

    timeout.addSuppressed(fallout);

    return timeout;
  }
}
