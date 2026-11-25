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

import io.netty.util.Timeout;
import io.netty.util.Timer;
import java.util.Objects;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * Fails a stalled sink read: the timer runs only while the peer holds unused credit, and stands
 * down once the peer uses it all. One timer covers each idle window: steady chunks only stamp
 * progress, and each fired check rearms for the rest of the delay.
 *
 * <p>Runtime-internal: driven by {@link ChunkTimeout}, one per accepted reader.
 */
final class ChunkWatchdog {
  private static final int ACTIVE = 0;
  private static final int TERMINATED = 1;

  // Never run expiry on the timer thread: if the off-loop scheduler rejects, the common pool
  // (which does not reject) runs it instead.
  private static final Scheduler EXPIRY_FALLBACK =
      Schedulers.fromExecutor(ForkJoinPool.commonPool());

  private final Timer timer;
  private final Scheduler scheduler;
  private final long delayNanos;
  private final LongSupplier nanoClock;
  private final Runnable stallHandler;

  private final AtomicLong outstandingCredit = new AtomicLong();
  private final AtomicReference<Timeout> timeout = new AtomicReference<>();
  private final AtomicInteger state = new AtomicInteger(ACTIVE);

  /** The last credit grant or chunk arrival: the stall check measures silence from here. */
  private volatile long lastProgressNanos;

  ChunkWatchdog(
      Timer timer,
      Scheduler scheduler,
      long delayNanos,
      LongSupplier nanoClock,
      Runnable stallHandler) {
    this.timer = Objects.requireNonNull(timer, "timer");
    this.scheduler = Objects.requireNonNull(scheduler, "scheduler");

    if (delayNanos <= 0) {
      throw new IllegalArgumentException("delayNanos must be positive, was: " + delayNanos);
    }

    this.delayNanos = delayNanos;
    this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
    this.stallHandler = Objects.requireNonNull(stallHandler, "stallHandler");
  }

  /**
   * Records wire credit the subscriber granted the peer. A grant on an idle sink stamps progress
   * and arms the timer; a grant on a live one only counts.
   */
  void creditGranted(long n) {
    // Stamp before counting: credit and checks race on different threads, so a check between the
    // two sees either the old credit (and stands down) or the new credit with the new stamp, never
    // new credit with the old stamp.
    if (outstandingCredit.get() == 0) {
      lastProgressNanos = nanoClock.getAsLong();
    }

    outstandingCredit.addAndGet(n);
    startTimerIfNoneRunning();
  }

  /** Records a sink payload from the wire: it only stamps progress and lowers credit. */
  void chunkReceived() {
    lastProgressNanos = nanoClock.getAsLong();
    outstandingCredit.updateAndGet(c -> c > 0 ? c - 1 : 0);
  }

  /** The sink or the call ended: drop the timer and never arm again. */
  void stopWatching() {
    state.compareAndSet(ACTIVE, TERMINATED);
    cancelTimer();
  }

  /** Hands the fired timer off the timer thread: never runs expiry work here. */
  private void onTimerExpired(Timeout expired) {
    try {
      scheduler.schedule(this::checkForStall);
    } catch (RejectedExecutionException e) {
      EXPIRY_FALLBACK.schedule(this::checkForStall);
    }
  }

  /** Fails the sink when the full delay passed with no chunk while the peer holds unused credit. */
  private void checkForStall() {
    if (state.get() != ACTIVE) {
      return;
    }

    if (outstandingCredit.get() == 0) {
      cancelTimer();

      if (outstandingCredit.get() > 0) {
        startTimerIfNoneRunning();
      }

      return;
    }

    long elapsed = nanoClock.getAsLong() - lastProgressNanos;

    if (elapsed >= delayNanos) {
      failStalledSink();
    } else {
      restartTimer(delayNanos - elapsed);
    }
  }

  /** Arms one full-delay timer for an idle window, unless one already runs. */
  private void startTimerIfNoneRunning() {
    if (state.get() != ACTIVE || timeout.get() != null) {
      return;
    }

    Timeout next = timer.newTimeout(this::onTimerExpired, delayNanos, TimeUnit.NANOSECONDS);

    if (timeout.compareAndSet(null, next)) {
      if (state.get() != ACTIVE) {
        cancelTimer();
      }
    } else {
      next.cancel();
    }
  }

  /** Replaces the live timer with one for the remaining silence. */
  private void restartTimer(long remainingNanos) {
    Timeout next = timer.newTimeout(this::onTimerExpired, remainingNanos, TimeUnit.NANOSECONDS);
    Timeout prev = timeout.getAndSet(next);

    if (prev != null) {
      prev.cancel();
    }

    if (state.get() != ACTIVE) {
      cancelTimer();
    }
  }

  /** Drops the live timer, if any. */
  private void cancelTimer() {
    Timeout current = timeout.getAndSet(null);

    if (current != null) {
      current.cancel();
    }
  }

  /** Ends a stalled sink once: drops the timer and runs the stall handler. */
  private void failStalledSink() {
    if (!state.compareAndSet(ACTIVE, TERMINATED)) {
      return;
    }

    cancelTimer();
    stallHandler.run();
  }
}
