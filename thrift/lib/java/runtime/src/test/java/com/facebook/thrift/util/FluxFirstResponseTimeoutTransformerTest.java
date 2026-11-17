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

package com.facebook.thrift.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

public class FluxFirstResponseTimeoutTransformerTest {
  private final ManualTimer timer = new ManualTimer();

  private final AtomicInteger expiries = new AtomicInteger();

  @Test
  public void expirySwitchesToTheFallbackAndCancelsTheSourceOnce() {
    AtomicInteger cancels = new AtomicInteger();
    Flux<String> source = Flux.<String>never().doOnCancel(cancels::incrementAndGet);

    StepVerifier.create(source.transform(transformer(Schedulers.immediate())))
        .then(timer::expire)
        .verifyErrorMessage("expired");

    assertEquals(1, cancels.get());
    assertEquals(1, expiries.get());
    assertEquals(1, timer.created);
    assertEquals(1, timer.delay);
    assertEquals(TimeUnit.SECONDS, timer.unit);
  }

  @Test
  public void expiryRunsOnTheScheduler() {
    List<Runnable> tasks = new ArrayList<>();
    Flux<String> source = Flux.never();

    StepVerifier.create(source.transform(transformer(Schedulers.fromExecutor(tasks::add))))
        .then(timer::expire)
        .then(() -> assertEquals(0, expiries.get()))
        .then(() -> tasks.forEach(Runnable::run))
        .verifyErrorMessage("expired");

    assertEquals(1, tasks.size());
  }

  @Test
  public void rejectedExpiryStillEndsWithTheFallback() {
    Scheduler disposed = Schedulers.newSingle("disposed");
    disposed.dispose();

    StepVerifier.create(Flux.<String>never().transform(transformer(disposed)))
        .then(timer::expire)
        .verifyErrorMessage("expired");

    assertEquals(1, expiries.get());
  }

  @Test
  public void cancelAfterExpiryBeforeTheSchedulerRunsDropsTheFallback() {
    List<Runnable> tasks = new ArrayList<>();
    Flux<String> source = Flux.never();

    StepVerifier.create(source.transform(transformer(Schedulers.fromExecutor(tasks::add))))
        .then(timer::expire)
        .thenCancel()
        .verify();

    tasks.forEach(Runnable::run);

    assertEquals(0, expiries.get());
  }

  @Test
  public void firstElementStopsTheTimer() {
    TestPublisher<String> source = TestPublisher.create();

    StepVerifier.create(source.flux().transform(transformer(Schedulers.immediate())))
        .then(() -> source.next("first"))
        .expectNext("first")
        .then(timer::expire)
        .then(() -> source.next("second").complete())
        .expectNext("second")
        .verifyComplete();

    assertTrue(timer.timeout.isCancelled());
    assertEquals(0, expiries.get());
  }

  @Test
  public void synchronousFirstElementStopsTheTimer() {
    Flux<String> source = Flux.just("first").concatWith(Flux.never());

    StepVerifier.create(source.transform(transformer(Schedulers.immediate())))
        .expectNext("first")
        .then(() -> assertTrue(timer.timeout.isCancelled()))
        .thenCancel()
        .verify();

    assertEquals(0, expiries.get());
  }

  @Test
  public void completeStopsTheTimer() {
    StepVerifier.create(Flux.<String>empty().transform(transformer(Schedulers.immediate())))
        .verifyComplete();

    assertTrue(timer.timeout.isCancelled());
    assertEquals(0, expiries.get());
  }

  @Test
  public void errorStopsTheTimerAndPassesThrough() {
    Flux<String> source = Flux.error(new IllegalStateException("source failed"));

    StepVerifier.create(source.transform(transformer(Schedulers.immediate())))
        .verifyErrorMessage("source failed");

    assertTrue(timer.timeout.isCancelled());
    assertEquals(0, expiries.get());
  }

  @Test
  public void cancelStopsTheTimerAndCancelsTheSource() {
    AtomicBoolean cancelled = new AtomicBoolean();
    Flux<String> source = Flux.<String>never().doOnCancel(() -> cancelled.set(true));

    StepVerifier.create(source.transform(transformer(Schedulers.immediate())))
        .thenCancel()
        .verify();

    assertTrue(timer.timeout.isCancelled());
    assertTrue(cancelled.get());
    assertEquals(0, expiries.get());
  }

  @Test
  public void negativeDelayIsRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new FluxFirstResponseTimeoutTransformer<String>(
                Schedulers.immediate(), -1, TimeUnit.SECONDS, Flux.empty(), () -> timer));
  }

  @Test
  public void firstElementAndExpiryRaceSignalOnce() throws Exception {
    FluxFirstResponseTimeoutTransformer<String> transformer = transformer(Schedulers.immediate());

    for (int i = 0; i < 100; i++) {
      Sinks.Many<String> source = Sinks.many().unicast().onBackpressureBuffer();
      List<String> signals = new CopyOnWriteArrayList<>();
      source.asFlux().transform(transformer).subscribe(signals::add, e -> signals.add("error"));

      CyclicBarrier start = new CyclicBarrier(2);
      Thread expiry = new Thread(() -> awaitThen(start, timer::expire));

      expiry.start();
      awaitThen(start, () -> source.tryEmitNext("first"));
      expiry.join();

      assertEquals(1, signals.size(), "signals " + signals);
    }
  }

  /** The fallback has the shape of the server's: an error built when the timeout fires. */
  private FluxFirstResponseTimeoutTransformer<String> transformer(Scheduler scheduler) {
    Mono<String> fallback =
        Mono.error(
            () -> {
              expiries.incrementAndGet();
              return new IllegalStateException("expired");
            });

    return new FluxFirstResponseTimeoutTransformer<>(
        scheduler, 1, TimeUnit.SECONDS, fallback, () -> timer);
  }

  private static void awaitThen(CyclicBarrier barrier, Runnable action) {
    try {
      barrier.await(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      throw new AssertionError(e);
    }

    action.run();
  }

  /** A timer that holds its latest timeout until the test expires it. */
  private static final class ManualTimer implements Timer {
    volatile ManualTimeout timeout;
    volatile int created;
    volatile long delay;
    volatile TimeUnit unit;

    @Override
    public Timeout newTimeout(TimerTask task, long delay, TimeUnit unit) {
      this.created++;
      this.delay = delay;
      this.unit = unit;
      this.timeout = new ManualTimeout(this, task);

      return timeout;
    }

    void expire() {
      timeout.expire();
    }

    @Override
    public Set<Timeout> stop() {
      return Collections.emptySet();
    }
  }

  private static final class ManualTimeout implements Timeout {
    private final Timer timer;
    private final TimerTask task;
    private volatile boolean cancelled;

    ManualTimeout(Timer timer, TimerTask task) {
      this.timer = timer;
      this.task = task;
    }

    void expire() {
      if (cancelled) {
        return;
      }

      try {
        task.run(this);
      } catch (Exception e) {
        throw new AssertionError(e);
      }
    }

    @Override
    public Timer timer() {
      return timer;
    }

    @Override
    public TimerTask task() {
      return task;
    }

    @Override
    public boolean isExpired() {
      return false;
    }

    @Override
    public boolean isCancelled() {
      return cancelled;
    }

    @Override
    public boolean cancel() {
      cancelled = true;
      return true;
    }
  }
}
