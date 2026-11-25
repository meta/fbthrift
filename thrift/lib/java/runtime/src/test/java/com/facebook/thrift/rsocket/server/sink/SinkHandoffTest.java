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

import static com.facebook.thrift.rsocket.server.sink.SinkPayloadSubscriberTest.assertReleasedOnce;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.facebook.thrift.rsocket.server.sink.SinkPayloadSubscriberTest.CountedPayload;
import com.facebook.thrift.rsocket.server.sink.SinkPayloadSubscriberTest.IdleReader;
import com.facebook.thrift.util.resources.ForkJoinPoolScheduler;
import io.netty.util.ReferenceCountUtil;
import io.rsocket.Payload;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

public class SinkHandoffTest {
  @Test
  public void windowOpensAtPrefetchAndRefills() {
    List<Long> credits = new CopyOnWriteArrayList<>();
    AtomicReference<Sinks.Many<Payload>> peerRef = new AtomicReference<>();
    AtomicReference<ContextView> upstreamContext = new AtomicReference<>();
    Object marker = new Object();

    Flux<Payload> peer =
        Flux.deferContextual(
            context -> {
              upstreamContext.set(context);

              Sinks.Many<Payload> sunk = Sinks.many().unicast().onBackpressureBuffer();
              peerRef.set(sunk);

              return sunk.asFlux().doOnRequest(credits::add);
            });

    List<CountedPayload> first = counted(8);
    List<CountedPayload> second = counted(6);
    List<CountedPayload> third = counted(6);

    StepVerifier.create(
            SinkHandoff.read(peer, Schedulers.immediate(), 8)
                .doOnNext(ReferenceCountUtil::release)
                .contextWrite(Context.of(marker, marker)))
        .then(() -> sendAll(peerRef.get(), first))
        .expectNextCount(8)
        .then(() -> sendAll(peerRef.get(), second))
        .expectNextCount(6)
        .then(() -> sendAll(peerRef.get(), third))
        .expectNextCount(6)
        .then(() -> peerRef.get().tryEmitComplete())
        .verifyComplete();

    assertEquals(Arrays.asList(8L, 6L, 6L, 6L), credits);
    assertTrue(upstreamContext.get().hasKey(marker));

    List<CountedPayload> all = new ArrayList<>();
    all.addAll(first);
    all.addAll(second);
    all.addAll(third);
    assertReleasedOnce(all);
  }

  @Test
  public void rejectedDrainReleasesQueued() throws Exception {
    FlippingScheduler scheduler = new FlippingScheduler();

    Sinks.Many<Payload> peer = Sinks.many().unicast().onBackpressureBuffer();

    IdleReader reader =
        SinkHandoff.read(peer.asFlux(), scheduler, 100).subscribeWith(new IdleReader());

    List<CountedPayload> filled = counted(10);
    sendAll(peer, filled);

    scheduler.reject = true;
    List<CountedPayload> after = counted(5);
    sendAll(peer, after);

    Throwable error = reader.awaitError();
    assertTrue(error instanceof RejectedExecutionException, "expected a rejection, got " + error);

    List<CountedPayload> all = new ArrayList<>();
    all.addAll(filled);
    all.addAll(after);
    assertReleasedOnce(all);
  }

  @Test
  public void blockedPoolCancelReleasesWithoutDrainRunning() throws Exception {
    ForkJoinPoolScheduler scheduler = ForkJoinPoolScheduler.create("sink-handoff-test", 2);

    try {
      Sinks.Many<Payload> peer = Sinks.many().unicast().onBackpressureBuffer();

      IdleReader reader =
          SinkHandoff.read(peer.asFlux(), scheduler, 100).subscribeWith(new IdleReader());

      List<CountedPayload> payloads = counted(5);
      sendAll(peer, payloads);

      CountDownLatch blockersRunning = new CountDownLatch(2);
      CountDownLatch releaseBlockers = new CountDownLatch(1);

      ExecutorService pool = scheduler.getExecutor();
      pool.submit(blockUntil(blockersRunning, releaseBlockers));
      pool.submit(blockUntil(blockersRunning, releaseBlockers));

      assertTrue(blockersRunning.await(5, TimeUnit.SECONDS), "pool blockers never started");

      reader.dispose();

      assertReleasedOnce(payloads);

      releaseBlockers.countDown();
    } finally {
      scheduler.dispose();
    }
  }

  @Test
  public void blockedPoolQueuedDrainsReleaseOnUnblock() throws Exception {
    ForkJoinPoolScheduler scheduler = ForkJoinPoolScheduler.create("sink-handoff-test", 2);

    try {
      Sinks.Many<Payload> peer = Sinks.many().unicast().onBackpressureBuffer();

      CountDownLatch blockersRunning = new CountDownLatch(2);
      CountDownLatch releaseBlockers = new CountDownLatch(1);

      ExecutorService pool = scheduler.getExecutor();
      pool.submit(blockUntil(blockersRunning, releaseBlockers));
      pool.submit(blockUntil(blockersRunning, releaseBlockers));

      assertTrue(blockersRunning.await(5, TimeUnit.SECONDS), "pool blockers never started");

      IdleReader reader =
          SinkHandoff.read(peer.asFlux(), scheduler, 100).subscribeWith(new IdleReader());

      List<CountedPayload> payloads = counted(5);
      sendAll(peer, payloads);

      reader.dispose();
      releaseBlockers.countDown();

      for (CountedPayload payload : payloads) {
        assertTrue(payload.awaitRelease(5, TimeUnit.SECONDS), "queued payload never drained");
      }

      assertReleasedOnce(payloads);
    } finally {
      scheduler.dispose();
    }
  }

  @Test
  public void cancelReleasesInlineOnImmediate() {
    Sinks.Many<Payload> peer = Sinks.many().unicast().onBackpressureBuffer();

    IdleReader reader =
        SinkHandoff.read(peer.asFlux(), Schedulers.immediate(), 100)
            .subscribeWith(new IdleReader());

    List<CountedPayload> payloads = counted(3);
    sendAll(peer, payloads);

    reader.dispose();

    assertReleasedOnce(payloads);
  }

  private static List<CountedPayload> counted(int n) {
    List<CountedPayload> payloads = new ArrayList<>();

    for (int i = 0; i < n; i++) {
      payloads.add(new CountedPayload());
    }

    return payloads;
  }

  private static void sendAll(Sinks.Many<Payload> peer, List<CountedPayload> payloads) {
    for (CountedPayload payload : payloads) {
      if (peer.tryEmitNext(payload).isFailure()) {
        ReferenceCountUtil.safeRelease(payload);
      }
    }
  }

  private static Runnable blockUntil(CountDownLatch started, CountDownLatch release) {
    return () -> {
      started.countDown();

      try {
        assertTrue(release.await(10, TimeUnit.SECONDS), "pool blocker never released");
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };
  }

  /** Runs the handoff drain inline until the test flips it to reject every drain after. */
  private static final class FlippingScheduler implements Scheduler {
    final FlippingWorker worker = new FlippingWorker();

    volatile boolean reject;

    @Override
    public Disposable schedule(Runnable task) {
      return worker.schedule(task);
    }

    @Override
    public Worker createWorker() {
      return worker;
    }

    @Override
    public boolean isDisposed() {
      return false;
    }

    private final class FlippingWorker implements Worker {
      @Override
      public Disposable schedule(Runnable task) {
        if (reject) {
          throw new RejectedExecutionException("test rejects the handoff drain");
        }

        task.run();

        return Disposables.disposed();
      }

      @Override
      public void dispose() {}

      @Override
      public boolean isDisposed() {
        return false;
      }
    }
  }
}
