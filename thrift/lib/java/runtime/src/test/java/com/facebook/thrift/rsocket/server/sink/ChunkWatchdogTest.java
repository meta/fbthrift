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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class ChunkWatchdogTest {
  private final ManualTimer timer = new ManualTimer();
  private final AtomicLong clock = new AtomicLong();
  private final AtomicInteger stalls = new AtomicInteger();

  @Test
  public void firesWhenCreditIsOutstandingAndNoChunkArrives() {
    ChunkWatchdog watchdog = watchdog(Schedulers.immediate());

    watchdog.creditGranted(1);

    expirePastDelay();

    assertEquals(1, stalls.get());
    assertEquals(1, timer.created);
    assertEquals(TimeUnit.SECONDS.toNanos(1), timer.delay);
    assertEquals(TimeUnit.NANOSECONDS, timer.unit);
  }

  @Test
  public void stallRunsOnceAndStopsArming() {
    ChunkWatchdog watchdog = watchdog(Schedulers.immediate());

    watchdog.creditGranted(1);

    expirePastDelay();
    watchdog.creditGranted(1);

    assertEquals(1, stalls.get());
    assertEquals(1, timer.created);
  }

  @Test
  public void steadyChunksArmOneTimerPerIdleWindow() {
    ChunkWatchdog watchdog = watchdog(Schedulers.immediate());

    watchdog.creditGranted(100);

    for (int i = 0; i < 50; i++) {
      watchdog.chunkReceived();
    }

    expirePastDelay();

    assertEquals(1, stalls.get());
    assertEquals(1, timer.created);
  }

  @Test
  public void neverArmsWithoutCreditOrAfterStop() {
    ChunkWatchdog watchdog = watchdog(Schedulers.immediate());

    assertEquals(0, timer.created);

    watchdog.stopWatching();
    watchdog.creditGranted(1);

    assertEquals(0, timer.created);
    assertEquals(0, stalls.get());
  }

  @Test
  public void armsWhenCreditArrivesLate() {
    ChunkWatchdog watchdog = watchdog(Schedulers.immediate());

    assertEquals(0, timer.created);

    watchdog.creditGranted(1);

    assertEquals(1, timer.created);

    watchdog.chunkReceived();
    watchdog.stopWatching();

    assertTrue(timer.timeout.isCancelled());
    assertEquals(0, stalls.get());
  }

  @Test
  public void staleFireAfterRearmDoesNotStall() {
    List<Runnable> tasks = new ArrayList<>();
    ChunkWatchdog watchdog = watchdog(Schedulers.fromExecutor(tasks::add));

    watchdog.creditGranted(2);
    timer.timeouts.get(0).expire();

    watchdog.chunkReceived();

    assertEquals(1, tasks.size());

    tasks.forEach(Runnable::run);

    assertEquals(0, stalls.get());
    assertEquals(2, timer.created);

    watchdog.stopWatching();

    assertTrue(timer.timeouts.get(1).isCancelled());
  }

  @Test
  public void creditReturningAfterZeroStillStalls() {
    ChunkWatchdog watchdog = watchdog(Schedulers.immediate());

    watchdog.creditGranted(1);
    watchdog.chunkReceived();
    watchdog.creditGranted(1);

    expirePastDelay();

    assertEquals(1, stalls.get());
    assertEquals(1, timer.created);
  }

  @Test
  public void lateCheckAfterStopDoesNotStall() {
    List<Runnable> tasks = new ArrayList<>();
    ChunkWatchdog watchdog = watchdog(Schedulers.fromExecutor(tasks::add));

    watchdog.creditGranted(1);
    timer.timeouts.get(0).expire();

    watchdog.stopWatching();
    tasks.forEach(Runnable::run);

    assertEquals(0, stalls.get());
    assertEquals(1, timer.created);
  }

  @Test
  public void chunkBeforeCheckRearmsForTheRemainder() {
    List<Runnable> tasks = new ArrayList<>();
    ChunkWatchdog watchdog = watchdog(Schedulers.fromExecutor(tasks::add));

    watchdog.creditGranted(2);
    timer.timeouts.get(0).expire();

    clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(900));
    watchdog.chunkReceived();
    clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(50));
    tasks.forEach(Runnable::run);

    assertEquals(0, stalls.get());
    assertEquals(2, timer.created);
    assertEquals(TimeUnit.MILLISECONDS.toNanos(950), timer.delay);
    assertEquals(TimeUnit.NANOSECONDS, timer.unit);

    clock.addAndGet(TimeUnit.SECONDS.toNanos(2));
    timer.timeouts.get(1).expire();
    tasks.forEach(Runnable::run);

    assertEquals(1, stalls.get());
  }

  @Test
  public void zeroCreditDropsTheTimerAndRearmsOnCredit() {
    ChunkWatchdog watchdog = watchdog(Schedulers.immediate());

    watchdog.creditGranted(1);
    watchdog.chunkReceived();
    timer.timeouts.get(0).expire();

    assertTrue(timer.timeouts.get(0).isCancelled());
    assertEquals(1, timer.created);
    assertEquals(0, stalls.get());

    watchdog.creditGranted(1);

    assertEquals(2, timer.created);

    expirePastDelay();

    assertEquals(1, stalls.get());
  }

  @Test
  public void creditAfterStaleCheckRearmsWithoutStall() {
    List<Runnable> tasks = new ArrayList<>();
    ChunkWatchdog watchdog = watchdog(Schedulers.fromExecutor(tasks::add));

    watchdog.creditGranted(1);
    watchdog.chunkReceived();
    clock.addAndGet(TimeUnit.SECONDS.toNanos(2));
    timer.timeouts.get(0).expire();

    assertEquals(1, tasks.size());

    watchdog.creditGranted(1);
    tasks.forEach(Runnable::run);

    assertEquals(0, stalls.get());
    assertEquals(2, timer.created);
    assertTrue(timer.timeouts.get(0).isCancelled());

    watchdog.stopWatching();
  }

  @Test
  public void expiryRunsOnTheScheduler() {
    List<Runnable> tasks = new ArrayList<>();
    ChunkWatchdog watchdog = watchdog(Schedulers.fromExecutor(tasks::add));

    watchdog.creditGranted(1);

    expirePastDelay();

    assertEquals(0, stalls.get());
    assertEquals(1, tasks.size());

    tasks.forEach(Runnable::run);

    assertEquals(1, stalls.get());
  }

  @Test
  public void expiryRunsOffTheTimerThread() throws InterruptedException {
    AtomicReference<Thread> stallThread = new AtomicReference<>();
    CountDownLatch stalled = new CountDownLatch(1);
    Scheduler expiry = Schedulers.newSingle("chunk-watchdog-expiry");

    try {
      ChunkWatchdog watchdog =
          new ChunkWatchdog(
              timer,
              expiry,
              TimeUnit.SECONDS.toNanos(1),
              clock::get,
              () -> {
                stallThread.set(Thread.currentThread());
                stalled.countDown();
              });

      watchdog.creditGranted(1);

      expirePastDelay();

      // The stall handler runs on the scheduler worker: expiry never runs on the firing timer
      // thread.
      assertTrue(stalled.await(5, TimeUnit.SECONDS));
      assertTrue(stallThread.get().getName().startsWith("chunk-watchdog-expiry"));
      assertNotEquals(Thread.currentThread(), stallThread.get());
    } finally {
      expiry.dispose();
    }
  }

  @Test
  public void rejectedSchedulerStallsOnTheFallback() throws InterruptedException {
    AtomicReference<Thread> stallThread = new AtomicReference<>();
    CountDownLatch stalled = new CountDownLatch(1);
    Scheduler rejecting =
        Schedulers.fromExecutor(
            task -> {
              throw new RejectedExecutionException("saturated");
            });
    ChunkWatchdog watchdog =
        new ChunkWatchdog(
            timer,
            rejecting,
            TimeUnit.SECONDS.toNanos(1),
            clock::get,
            () -> {
              stallThread.set(Thread.currentThread());
              stalled.countDown();
            });

    watchdog.creditGranted(1);

    expirePastDelay();

    // The common pool never rejects, so the stall still runs, off the firing timer thread.
    assertTrue(stalled.await(5, TimeUnit.SECONDS));
    assertTrue(stallThread.get().getName().startsWith("ForkJoinPool.commonPool-worker"));
    assertNotEquals(Thread.currentThread(), stallThread.get());
  }

  @Test
  public void nonPositiveDelayIsRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new ChunkWatchdog(
                timer, Schedulers.immediate(), 0, clock::get, stalls::incrementAndGet));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new ChunkWatchdog(
                timer, Schedulers.immediate(), -1, clock::get, stalls::incrementAndGet));
  }

  private ChunkWatchdog watchdog(Scheduler scheduler) {
    return new ChunkWatchdog(
        timer, scheduler, TimeUnit.SECONDS.toNanos(1), clock::get, stalls::incrementAndGet);
  }

  /** Moves the clock past the 1-second test delay, then fires the armed timer. */
  private void expirePastDelay() {
    clock.addAndGet(TimeUnit.SECONDS.toNanos(2));
    timer.expire();
  }
}
