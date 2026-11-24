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

package com.facebook.thrift.rsocket.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.facebook.thrift.util.ResponseAndStreamBridge;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.ByteBufPayload;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Hooks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

class StreamHandoffTest {

  private static final Function<Throwable, String> ERRORS =
      t -> "mapped".equals(t.getMessage()) ? "error-frame" : null;

  private static final int FLOOD_FRAMES = 1000;

  private final List<Object> dropped = Collections.synchronizedList(new ArrayList<>());

  @BeforeEach
  void trackDropped() {
    Hooks.onErrorDropped(dropped::add);
    Hooks.onNextDropped(dropped::add);
  }

  @AfterEach
  void assertNothingDropped() {
    Hooks.resetOnErrorDropped();
    Hooks.resetOnNextDropped();

    assertThat(dropped).isEmpty();
  }

  @Test
  void directTransportSubscriptionEmitsOnlyDecodedValues() {
    ManualScheduler scheduler = new ManualScheduler();
    RSocket rsocket = mock(RSocket.class);
    Transport transport = new Transport();
    when(rsocket.requestStream(any())).thenAnswer(inv -> transport.flux());

    Payload request = requestPayload();
    Payload first = responsePayload();
    Payload second = responsePayload();

    Capture cap = new Capture();

    verifyNoInteractions(rsocket);

    Disposable sub =
        cap.subscribe(
            StreamHandoff.requestStream(
                rsocket, () -> request, scheduler, decoding(new AtomicInteger()), ERRORS));

    verify(rsocket, times(1)).requestStream(same(request));
    assertThat(transport.requests).containsExactly((long) ResponseAndStreamBridge.INITIAL_CREDITS);

    transport.driver.get().next(first);
    transport.driver.get().next(second);

    assertThat(cap.values).isEmpty();

    scheduler.runAll();

    assertThat(cap.values).containsExactly("v1", "v2");
    assertThat(cap.failure.get()).isNull();
    assertThat(cap.completed.get()).isFalse();
    assertThat(first.refCnt()).isEqualTo(1);
    assertThat(second.refCnt()).isEqualTo(1);

    sub.dispose();

    first.release();
    second.release();
    request.release();
  }

  @Test
  void requestStreamFactoryAllocatesQueuePerSubscription() {
    ManualScheduler scheduler = new ManualScheduler();
    RSocket rsocket = mock(RSocket.class);
    List<Transport> transports = new ArrayList<>();
    when(rsocket.requestStream(any()))
        .thenAnswer(
            inv -> {
              Transport transport = new Transport();
              transports.add(transport);
              return transport.flux();
            });

    List<Payload> requests = new ArrayList<>();
    Capture a = new Capture();
    Capture b = new Capture();

    Flux<String> result =
        StreamHandoff.requestStream(
            rsocket,
            () -> {
              Payload encoded = requestPayload();
              requests.add(encoded);
              return encoded;
            },
            scheduler,
            decoding(new AtomicInteger()),
            ERRORS);

    Disposable subA = a.subscribeHeld(result);
    Disposable subB = b.subscribeHeld(result);
    b.downstream.get().request(1);

    assertThat(transports).hasSize(2);
    verify(rsocket, times(2)).requestStream(any());

    Payload aFrame = responsePayload();
    transports.get(0).driver.get().next(aFrame);
    transports.get(1).driver.get().complete();

    scheduler.runAll();

    assertThat(b.values).isEmpty();
    assertThat(b.completed.get()).isTrue();
    assertThat(a.values).isEmpty();

    subA.dispose();

    scheduler.runAll();

    assertThat(aFrame.refCnt()).isEqualTo(1);

    subB.dispose();

    aFrame.release();
    requests.forEach(Payload::release);
  }

  @Test
  void successDecodesUnderDemandInOrder() {
    ManualScheduler scheduler = new ManualScheduler();
    RSocket rsocket = mock(RSocket.class);
    Transport transport = new Transport();
    when(rsocket.requestStream(any())).thenAnswer(inv -> transport.flux());

    Payload request = requestPayload();
    Payload first = responsePayload();
    Payload second = responsePayload();
    Payload third = responsePayload();

    Capture cap = new Capture();

    Disposable sub =
        cap.subscribeHeld(
            StreamHandoff.requestStream(
                rsocket, () -> request, scheduler, decoding(new AtomicInteger()), ERRORS));

    cap.downstream.get().request(10);

    transport.driver.get().next(first);
    transport.driver.get().next(second);
    transport.driver.get().next(third);

    scheduler.runAll();

    assertThat(transport.requests).containsExactly((long) ResponseAndStreamBridge.INITIAL_CREDITS);
    assertThat(cap.values).containsExactly("v1", "v2", "v3");
    assertThat(first.refCnt()).isEqualTo(1);
    assertThat(second.refCnt()).isEqualTo(1);
    assertThat(third.refCnt()).isEqualTo(1);
    assertThat(cap.completed.get()).isFalse();

    sub.dispose();

    first.release();
    second.release();
    third.release();
    request.release();
  }

  @Test
  void noDemandNothingEmittedThenCancelFreesAll() {
    ManualScheduler scheduler = new ManualScheduler();
    RSocket rsocket = mock(RSocket.class);
    Transport transport = new Transport();
    when(rsocket.requestStream(any())).thenAnswer(inv -> transport.flux());

    Payload request = requestPayload();

    Capture cap = new Capture();
    List<Payload> frames = new ArrayList<>();

    Disposable sub =
        cap.subscribeHeld(
            StreamHandoff.requestStream(
                rsocket, () -> request, scheduler, decoding(new AtomicInteger()), ERRORS));

    for (int i = 0; i < 5; i++) {
      Payload frame = responsePayload();
      frames.add(frame);
      transport.driver.get().next(frame);
    }

    scheduler.runAll();

    assertThat(cap.values).isEmpty();
    assertThat(cap.failure.get()).isNull();
    assertThat(cap.completed.get()).isFalse();

    sub.dispose();

    scheduler.runAll();

    for (Payload frame : frames) {
      assertThat(frame.refCnt()).isEqualTo(1);
      frame.release();
    }

    assertThat(cap.values).isEmpty();
    assertThat(cap.failure.get()).isNull();
    assertThat(cap.completed.get()).isFalse();

    request.release();
  }

  @Test
  void transformThrowFailsStreamAndFreesQueued() {
    ManualScheduler scheduler = new ManualScheduler();
    RSocket rsocket = mock(RSocket.class);
    Transport transport = new Transport();
    when(rsocket.requestStream(any())).thenAnswer(inv -> transport.flux());

    Payload request = requestPayload();
    Payload first = responsePayload();
    Payload second = responsePayload();
    Payload third = responsePayload();

    Capture cap = new Capture();

    cap.subscribe(
        StreamHandoff.requestStream(
            rsocket, () -> request, scheduler, failingOnSecond(new AtomicInteger()), ERRORS));

    transport.driver.get().next(first);
    transport.driver.get().next(second);
    transport.driver.get().next(third);

    scheduler.runAll();

    assertThat(cap.values).containsExactly("v1");
    assertThat(cap.failure.get()).hasMessage("decode boom");
    assertThat(cap.errorCount.get()).isEqualTo(1);
    assertThat(cap.completed.get()).isFalse();
    assertThat(first.refCnt()).isEqualTo(1);
    assertThat(second.refCnt()).isEqualTo(1);
    assertThat(third.refCnt()).isEqualTo(1);

    first.release();
    second.release();
    third.release();
    request.release();
  }

  @Test
  void cancelBeforeSubscribeNeverRequests() {
    ManualScheduler scheduler = new ManualScheduler();
    RSocket rsocket = mock(RSocket.class);
    AtomicReference<Subscriber<? super Payload>> transportSub = manualInbound(rsocket);

    Payload request = requestPayload();
    Subscription transportSubscription = mock(Subscription.class);

    Capture cap = new Capture();

    Disposable sub =
        cap.subscribeHeld(
            StreamHandoff.requestStream(
                rsocket, () -> request, scheduler, decoding(new AtomicInteger()), ERRORS));

    sub.dispose();

    transportSub.get().onSubscribe(transportSubscription);

    verify(transportSubscription).cancel();
    verify(transportSubscription, never()).request(anyLong());
    assertThat(cap.values).isEmpty();
    assertThat(cap.failure.get()).isNull();
    assertThat(cap.completed.get()).isFalse();

    request.release();
  }

  @Test
  void cancelWithQueuedFramesReleasedByDrain() {
    ManualScheduler scheduler = new ManualScheduler();
    RSocket rsocket = mock(RSocket.class);
    Transport transport = new Transport();
    when(rsocket.requestStream(any())).thenAnswer(inv -> transport.flux());

    Payload request = requestPayload();
    Payload first = responsePayload();
    Payload second = responsePayload();

    Capture cap = new Capture();

    Disposable sub =
        cap.subscribe(
            StreamHandoff.requestStream(
                rsocket, () -> request, scheduler, decoding(new AtomicInteger()), ERRORS));

    transport.driver.get().next(first);
    transport.driver.get().next(second);

    assertThat(scheduler.tasks).hasSize(1);

    sub.dispose();

    scheduler.runAll();

    assertThat(first.refCnt()).isEqualTo(1);
    assertThat(second.refCnt()).isEqualTo(1);
    assertThat(cap.values).isEmpty();
    assertThat(cap.failure.get()).isNull();
    assertThat(cap.completed.get()).isFalse();

    first.release();
    second.release();
    request.release();
  }

  @Test
  void overflowReleasesAndErrors() {
    ManualScheduler scheduler = new ManualScheduler();
    RSocket rsocket = mock(RSocket.class);
    AtomicReference<Subscriber<? super Payload>> transportSub = manualInbound(rsocket);

    Payload request = requestPayload();

    Capture cap = new Capture();
    List<Payload> frames = new ArrayList<>();

    cap.subscribe(
        StreamHandoff.requestStream(
            rsocket, () -> request, scheduler, decoding(new AtomicInteger()), ERRORS));

    transportSub.get().onSubscribe(mock(Subscription.class));

    for (int i = 0; i < FLOOD_FRAMES; i++) {
      Payload frame = responsePayload();
      frames.add(frame);
      transportSub.get().onNext(frame);
    }

    scheduler.runAll();

    assertThat(cap.failure.get()).hasMessageContaining("flow control");
    assertThat(cap.errorCount.get()).isEqualTo(1);
    assertThat(cap.completed.get()).isFalse();

    for (Payload frame : frames) {
      assertThat(frame.refCnt()).isEqualTo(1);
      frame.release();
    }

    request.release();
  }

  @Test
  void rejectionWithReentrantCancelDrainsLateFrames() {
    Queue<Runnable> tasks = new ConcurrentLinkedQueue<>();
    Scheduler scheduler = shutOnDispose(tasks);
    RSocket rsocket = mock(RSocket.class);
    AtomicReference<Subscriber<? super Payload>> transportSub = manualInbound(rsocket);

    Payload request = requestPayload();
    Payload first = responsePayload();

    Capture cap = new Capture();

    Disposable sub =
        cap.subscribe(
            StreamHandoff.requestStream(
                rsocket, () -> request, scheduler, decoding(new AtomicInteger()), ERRORS));

    Subscription transport = mock(Subscription.class);
    AtomicBoolean reentered = new AtomicBoolean(false);
    AtomicReference<Payload> reentrant = new AtomicReference<>();
    doAnswer(
            invocation -> {
              if (reentered.compareAndSet(false, true)) {
                Payload inner = responsePayload();
                reentrant.set(inner);
                transportSub.get().onNext(inner);
              }

              return null;
            })
        .when(transport)
        .cancel();
    transportSub.get().onSubscribe(transport);

    assertThat(tasks).hasSize(1);

    tasks.poll().run();

    scheduler.dispose();

    transportSub.get().onNext(first);

    Payload late = responsePayload();
    transportSub.get().onNext(late);

    assertThat(first.refCnt()).isEqualTo(1);
    assertThat(late.refCnt()).isEqualTo(1);
    assertThat(reentrant.get().refCnt()).isEqualTo(1);
    assertThat(cap.values).isEmpty();
    assertThat(cap.failure.get()).hasMessage("shut down");
    assertThat(cap.errorCount.get()).isEqualTo(1);
    assertThat(cap.completed.get()).isFalse();

    sub.dispose();

    first.release();
    late.release();
    reentrant.get().release();
    request.release();
  }

  @Test
  void rejectDuringQueuedDrainFreesCurrentAndLateFrames() {
    AtomicReference<Runnable> queued = new AtomicReference<>();
    Scheduler scheduler = mock(Scheduler.class);
    when(scheduler.schedule(any(Runnable.class)))
        .thenAnswer(
            invocation -> {
              Runnable task = invocation.getArgument(0);

              if (queued.compareAndSet(null, task)) {
                return Disposables.disposed();
              }

              throw new RejectedExecutionException("no workers");
            });
    RSocket rsocket = mock(RSocket.class);
    AtomicReference<Subscriber<? super Payload>> transportSub = manualInbound(rsocket);

    Payload request = requestPayload();
    Payload first = responsePayload();

    Capture cap = new Capture();

    cap.subscribe(
        StreamHandoff.requestStream(
            rsocket, () -> request, scheduler, decoding(new AtomicInteger()), ERRORS));

    transportSub.get().onSubscribe(mock(Subscription.class));

    queued.get().run();

    transportSub.get().onNext(first);

    Payload late = responsePayload();
    transportSub.get().onNext(late);

    assertThat(cap.failure.get()).hasMessage("no workers");
    assertThat(cap.errorCount.get()).isEqualTo(1);
    assertThat(first.refCnt()).isEqualTo(1);
    assertThat(late.refCnt()).isEqualTo(1);
    assertThat(cap.values).isEmpty();

    first.release();
    late.release();
    request.release();
  }

  @Test
  void cancelDuringTransformFreesOnce() throws Exception {
    ManualScheduler scheduler = new ManualScheduler();
    RSocket rsocket = mock(RSocket.class);
    Transport transport = new Transport();
    when(rsocket.requestStream(any())).thenAnswer(inv -> transport.flux());

    Payload request = requestPayload();
    Payload frame = responsePayload();

    Capture cap = new Capture();

    Disposable sub =
        cap.subscribe(
            StreamHandoff.requestStream(
                rsocket, () -> request, scheduler, decoding(new AtomicInteger()), ERRORS));

    transport.driver.get().next(frame);

    Thread worker = new Thread(scheduler::runAll);
    worker.setDaemon(true);
    worker.start();
    sub.dispose();
    worker.join(10_000);

    assertThat(worker.isAlive()).isFalse();
    assertThat(cap.values).hasSizeLessThanOrEqualTo(1);
    assertThat(cap.completed.get()).isFalse();
    assertThat(frame.refCnt()).isEqualTo(1);

    frame.release();
    request.release();
  }

  @Test
  void lateArrivalAfterCancelFrees() {
    ManualScheduler scheduler = new ManualScheduler();
    RSocket rsocket = mock(RSocket.class);
    AtomicReference<Subscriber<? super Payload>> transportSub = manualInbound(rsocket);

    Payload request = requestPayload();
    Subscription transportSubscription = mock(Subscription.class);
    Payload frame = responsePayload();

    Capture cap = new Capture();

    Disposable sub =
        cap.subscribeHeld(
            StreamHandoff.requestStream(
                rsocket, () -> request, scheduler, decoding(new AtomicInteger()), ERRORS));

    transportSub.get().onSubscribe(transportSubscription);
    sub.dispose();
    transportSub.get().onNext(frame);

    scheduler.runAll();

    assertThat(frame.refCnt()).isEqualTo(1);
    assertThat(cap.values).isEmpty();
    assertThat(cap.failure.get()).isNull();
    assertThat(cap.completed.get()).isFalse();

    frame.release();
    request.release();
  }

  @Test
  void scheduleRejectionErrorsAndFrees() {
    Scheduler scheduler = mock(Scheduler.class);
    RuntimeException rejection = new RuntimeException("no workers");
    when(scheduler.schedule(any(Runnable.class))).thenThrow(rejection);
    RSocket rsocket = mock(RSocket.class);
    AtomicReference<Subscriber<? super Payload>> transportSub = manualInbound(rsocket);

    Payload request = requestPayload();
    Payload frame = responsePayload();

    Capture cap = new Capture();

    cap.subscribe(
        StreamHandoff.requestStream(
            rsocket, () -> request, scheduler, decoding(new AtomicInteger()), ERRORS));

    transportSub.get().onSubscribe(mock(Subscription.class));
    transportSub.get().onNext(frame);

    assertThat(cap.failure.get()).isSameAs(rejection);
    assertThat(cap.errorCount.get()).isEqualTo(1);
    assertThat(frame.refCnt()).isEqualTo(1);

    frame.release();
    request.release();
  }

  @Test
  void rejectionAfterTransportErrorCarriesItAsSuppressed() {
    AtomicReference<Runnable> stashed = new AtomicReference<>();
    Scheduler scheduler = mock(Scheduler.class);
    RuntimeException rejection = new RuntimeException("no workers");
    when(scheduler.schedule(any(Runnable.class)))
        .thenAnswer(
            invocation -> {
              stashed.set(invocation.getArgument(0));

              return Disposables.disposed();
            })
        .thenThrow(rejection);
    RSocket rsocket = mock(RSocket.class);
    AtomicReference<Subscriber<? super Payload>> transportSub = manualInbound(rsocket);

    Payload request = requestPayload();
    RuntimeException transportError = new RuntimeException("boom");

    Capture cap = new Capture();

    cap.subscribe(
        StreamHandoff.requestStream(
            rsocket, () -> request, scheduler, decoding(new AtomicInteger()), ERRORS));

    transportSub.get().onSubscribe(mock(Subscription.class));

    // Drain once with no signals pending so the transport error below is the first drain
    // that schedules: its rejection carries the error as suppressed instead of losing it.
    stashed.get().run();

    transportSub.get().onError(transportError);

    assertThat(cap.failure.get()).isSameAs(rejection);
    assertThat(cap.failure.get().getSuppressed()).containsExactly(transportError);
    assertThat(cap.errorCount.get()).isEqualTo(1);

    request.release();
  }

  @Test
  void rejectionAfterTerminalStaysSilent() {
    Scheduler scheduler = mock(Scheduler.class);
    when(scheduler.schedule(any(Runnable.class))).thenThrow(new RuntimeException("no workers"));
    RSocket rsocket = mock(RSocket.class);
    AtomicReference<Subscriber<? super Payload>> transportSub = manualInbound(rsocket);

    Payload request = requestPayload();
    Payload frame = responsePayload();

    Capture cap = new Capture();

    cap.subscribe(
        StreamHandoff.requestStream(
            rsocket, () -> request, scheduler, decoding(new AtomicInteger()), ERRORS));

    transportSub.get().onSubscribe(mock(Subscription.class));
    transportSub.get().onNext(frame);
    transportSub.get().onComplete();

    assertThat(cap.errorCount.get()).isEqualTo(1);
    assertThat(cap.failure.get()).isInstanceOf(RuntimeException.class);

    frame.release();
    request.release();
  }

  @Test
  void emptyCompletesWithoutDemand() {
    ManualScheduler scheduler = new ManualScheduler();
    RSocket rsocket = mock(RSocket.class);
    when(rsocket.requestStream(any())).thenReturn(Flux.empty());

    Payload request = requestPayload();

    Capture cap = new Capture();

    cap.subscribeHeld(
        StreamHandoff.requestStream(
            rsocket, () -> request, scheduler, decoding(new AtomicInteger()), ERRORS));

    scheduler.runAll();

    assertThat(cap.values).isEmpty();
    assertThat(cap.failure.get()).isNull();
    assertThat(cap.completed.get()).isTrue();

    request.release();
  }

  @Test
  void mappedErrorDeliversValueAndCompletes() {
    ManualScheduler scheduler = new ManualScheduler();
    RSocket rsocket = mock(RSocket.class);
    when(rsocket.requestStream(any())).thenReturn(Flux.error(new RuntimeException("mapped")));

    Payload request = requestPayload();

    Capture cap = new Capture();

    cap.subscribe(
        StreamHandoff.requestStream(
            rsocket, () -> request, scheduler, decoding(new AtomicInteger()), ERRORS));

    scheduler.runAll();

    assertThat(cap.values).containsExactly("error-frame");
    assertThat(cap.completed.get()).isTrue();

    request.release();
  }

  @Test
  void unmappedErrorDeliversError() {
    ManualScheduler scheduler = new ManualScheduler();
    RSocket rsocket = mock(RSocket.class);
    RuntimeException failure = new RuntimeException("transport down");
    when(rsocket.requestStream(any())).thenReturn(Flux.error(failure));

    Payload request = requestPayload();

    Capture cap = new Capture();

    cap.subscribe(
        StreamHandoff.requestStream(
            rsocket, () -> request, scheduler, decoding(new AtomicInteger()), ERRORS));

    scheduler.runAll();

    assertThat(cap.values).isEmpty();
    assertThat(cap.failure.get()).isSameAs(failure);
    assertThat(cap.errorCount.get()).isEqualTo(1);
    assertThat(cap.completed.get()).isFalse();

    request.release();
  }

  @Test
  void errorValueThrowDeliversMappingError() {
    ManualScheduler scheduler = new ManualScheduler();
    RSocket rsocket = mock(RSocket.class);
    RuntimeException mappingError = new RuntimeException("mapping boom");
    RuntimeException failure = new RuntimeException("mapped");
    when(rsocket.requestStream(any())).thenReturn(Flux.error(failure));

    Payload request = requestPayload();

    Capture cap = new Capture();

    cap.subscribe(
        StreamHandoff.requestStream(
            rsocket,
            () -> request,
            scheduler,
            decoding(new AtomicInteger()),
            t -> {
              throw mappingError;
            }));

    scheduler.runAll();

    assertThat(cap.values).isEmpty();
    assertThat(cap.failure.get()).isSameAs(mappingError);
    assertThat(mappingError.getSuppressed()).containsExactly(failure);

    request.release();
  }

  @Test
  void tailFrameRacingTerminalCheckIsFreedWithoutSignals() throws Exception {
    ManualScheduler scheduler = new ManualScheduler();
    RSocket rsocket = mock(RSocket.class);
    AtomicReference<Subscriber<? super Payload>> transportSub = manualInbound(rsocket);

    Payload request = requestPayload();
    AtomicBoolean armed = new AtomicBoolean(true);
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch released = new CountDownLatch(1);
    Payload tail = responsePayload();
    Payload racer = responsePayload();
    Queue<Payload> queue =
        new ConcurrentLinkedQueue<Payload>() {
          @Override
          public boolean isEmpty() {
            boolean empty = super.isEmpty();

            if (empty && armed.compareAndSet(true, false)) {
              entered.countDown();

              try {
                released.await();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            }

            return empty;
          }
        };

    Capture cap = new Capture();
    Payload first = responsePayload();

    cap.subscribe(
        StreamHandoff.requestStream(
            rsocket, () -> request, scheduler, decoding(new AtomicInteger()), ERRORS, () -> queue));

    transportSub.get().onSubscribe(mock(Subscription.class));
    transportSub.get().onNext(first);
    transportSub.get().onNext(tail);
    transportSub.get().onComplete();

    Thread worker = new Thread(scheduler::runAll);
    worker.setDaemon(true);
    worker.start();

    try {
      assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();

      transportSub.get().onNext(racer);
    } finally {
      released.countDown();
    }

    worker.join(10_000);

    assertThat(worker.isAlive()).isFalse();

    scheduler.runAll();

    assertThat(cap.values).containsExactly("v1", "v2");
    assertThat(cap.completed.get()).isTrue();
    assertThat(cap.postTerminal.get()).isFalse();
    assertThat(cap.errorCount.get()).isEqualTo(0);
    assertThat(first.refCnt()).isEqualTo(1);
    assertThat(tail.refCnt()).isEqualTo(1);
    assertThat(racer.refCnt()).isEqualTo(1);

    first.release();
    tail.release();
    racer.release();
    request.release();
  }

  @Test
  void noRefillAfterCancelAtRefillBoundary() {
    ManualScheduler scheduler = new ManualScheduler();
    RSocket rsocket = mock(RSocket.class);
    Transport transport = new Transport();
    when(rsocket.requestStream(any())).thenAnswer(inv -> transport.flux());

    Payload request = requestPayload();
    AtomicInteger sequence = new AtomicInteger();
    AtomicReference<Disposable> self = new AtomicReference<>();

    Capture cap = new Capture();
    List<Payload> frames = new ArrayList<>();

    self.set(
        cap.subscribe(
            StreamHandoff.requestStream(
                rsocket,
                () -> request,
                scheduler,
                raw -> {
                  ReferenceCountUtil.safeRelease(raw);
                  String value = "v" + sequence.incrementAndGet();

                  if (sequence.get() == ResponseAndStreamBridge.REFILL_CREDITS) {
                    self.get().dispose();
                  }

                  return value;
                },
                ERRORS)));

    for (int i = 0; i < 60; i++) {
      Payload frame = responsePayload();
      frames.add(frame);
      transport.driver.get().next(frame);
    }

    scheduler.runAll();

    assertThat(transport.requests).containsExactly((long) ResponseAndStreamBridge.INITIAL_CREDITS);
    assertThat(transport.cancelled.get()).isTrue();

    for (Payload frame : frames) {
      assertThat(frame.refCnt()).isEqualTo(1);
      frame.release();
    }

    assertThat(cap.completed.get()).isFalse();
    assertThat(cap.failure.get()).isNull();

    request.release();
  }

  @Test
  void refillRequestsFiftyPerFiftyDecoded() {
    ManualScheduler scheduler = new ManualScheduler();
    RSocket rsocket = mock(RSocket.class);
    Transport transport = new Transport();
    when(rsocket.requestStream(any())).thenAnswer(inv -> transport.flux());

    Payload request = requestPayload();

    Capture cap = new Capture();
    List<Payload> frames = new ArrayList<>();

    cap.subscribe(
        StreamHandoff.requestStream(
            rsocket, () -> request, scheduler, decoding(new AtomicInteger()), ERRORS));

    for (int i = 0; i < 60; i++) {
      Payload frame = responsePayload();
      frames.add(frame);
      transport.driver.get().next(frame);
    }

    scheduler.runAll();

    assertThat(transport.requests)
        .containsExactly(
            (long) ResponseAndStreamBridge.INITIAL_CREDITS,
            (long) ResponseAndStreamBridge.REFILL_CREDITS);

    for (Payload frame : frames) {
      assertThat(frame.refCnt()).isEqualTo(1);
      frame.release();
    }

    request.release();
  }

  @Test
  void reentrantRequestUnderImmediateScheduler() {
    RSocket rsocket = mock(RSocket.class);
    AtomicReference<FluxSink<Payload>> driver = new AtomicReference<>();
    when(rsocket.requestStream(any())).thenAnswer(inv -> Flux.create(driver::set));

    Payload request = requestPayload();
    AtomicInteger sequence = new AtomicInteger();
    List<String> values = new ArrayList<>();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    AtomicReference<Subscription> downstream = new AtomicReference<>();
    int frames = 10_000;
    List<Payload> live = new ArrayList<>(frames);

    StreamHandoff.requestStream(
            rsocket, () -> request, Schedulers.immediate(), decoding(sequence), ERRORS)
        .subscribe(
            value -> {
              values.add(value);
              downstream.get().request(1);
            },
            failure::set,
            () -> {},
            downstream::set);

    downstream.get().request(1);

    for (int i = 0; i < frames; i++) {
      Payload frame = responsePayload();
      live.add(frame);
      driver.get().next(frame);
    }

    assertThat(values).hasSize(frames);

    for (int i = 0; i < frames; i++) {
      assertThat(values.get(i)).isEqualTo("v" + (i + 1));
    }

    assertThat(failure.get()).isNull();

    for (Payload frame : live) {
      assertThat(frame.refCnt()).isEqualTo(1);
      frame.release();
    }

    request.release();
  }

  @Test
  void cancelRacingCompletionDeliversNothing() {
    ManualScheduler scheduler = new ManualScheduler();
    RSocket rsocket = mock(RSocket.class);
    AtomicReference<FluxSink<Payload>> driver = new AtomicReference<>();
    when(rsocket.requestStream(any())).thenAnswer(inv -> Flux.create(driver::set));

    Payload request = requestPayload();

    Capture cap = new Capture();

    Disposable sub =
        cap.subscribe(
            StreamHandoff.requestStream(
                rsocket, () -> request, scheduler, decoding(new AtomicInteger()), ERRORS));

    driver.get().complete();
    sub.dispose();

    scheduler.runAll();

    assertThat(cap.values).isEmpty();
    assertThat(cap.failure.get()).isNull();
    assertThat(cap.completed.get()).isFalse();

    request.release();
  }

  private static Function<Payload, String> decoding(AtomicInteger sequence) {
    return raw -> {
      ReferenceCountUtil.safeRelease(raw);
      return "v" + sequence.incrementAndGet();
    };
  }

  private static Function<Payload, String> failingOnSecond(AtomicInteger sequence) {
    return raw -> {
      ReferenceCountUtil.safeRelease(raw);

      if (sequence.incrementAndGet() == 2) {
        throw new RuntimeException("decode boom");
      }

      return "v" + sequence.get();
    };
  }

  private static Payload requestPayload() {
    Payload request = ByteBufPayload.create(Unpooled.buffer(1), Unpooled.buffer(1));
    request.retain();
    return request;
  }

  private static Payload responsePayload() {
    Payload response = ByteBufPayload.create(Unpooled.buffer(1), Unpooled.buffer(1));
    response.retain();
    return response;
  }

  /**
   * A transport that captures the subscriber without handshaking, so tests drive signals manually:
   * cancellation never propagates back, like a peer that keeps sending.
   */
  private static AtomicReference<Subscriber<? super Payload>> manualInbound(RSocket rsocket) {
    AtomicReference<Subscriber<? super Payload>> captured = new AtomicReference<>();
    Flux<Payload> inbound =
        new Flux<Payload>() {
          @Override
          public void subscribe(CoreSubscriber<? super Payload> s) {
            captured.set(s);
          }
        };
    when(rsocket.requestStream(any())).thenReturn(inbound);
    return captured;
  }

  /**
   * A scheduler that queues tasks until disposed, then rejects: models a shutdown that drops an
   * accepted drain task.
   */
  private static Scheduler shutOnDispose(Queue<Runnable> tasks) {
    AtomicBoolean shut = new AtomicBoolean(false);
    Scheduler scheduler = mock(Scheduler.class);
    when(scheduler.schedule(any(Runnable.class)))
        .thenAnswer(
            invocation -> {
              if (shut.get()) {
                throw new RejectedExecutionException("shut down");
              }

              Runnable task = invocation.getArgument(0);
              tasks.add(task);
              return Disposables.disposed();
            });
    doAnswer(
            invocation -> {
              shut.set(true);
              tasks.clear();
              return null;
            })
        .when(scheduler)
        .dispose();

    return scheduler;
  }

  /** Records downstream signals; subscribing held stores the subscription without demand. */
  private static final class Capture {
    final List<String> values = new ArrayList<>();
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final AtomicBoolean completed = new AtomicBoolean();
    final AtomicBoolean postTerminal = new AtomicBoolean();
    final AtomicInteger errorCount = new AtomicInteger();
    final AtomicReference<Subscription> downstream = new AtomicReference<>();

    Disposable subscribe(Flux<String> flux) {
      return subscribe(flux, s -> s.request(Long.MAX_VALUE));
    }

    Disposable subscribeHeld(Flux<String> flux) {
      return subscribe(flux, downstream::set);
    }

    private Disposable subscribe(Flux<String> flux, Consumer<Subscription> onSubscribe) {
      return flux.subscribe(
          value -> {
            if (completed.get() || failure.get() != null) {
              postTerminal.set(true);
            } else {
              values.add(value);
            }
          },
          error -> {
            errorCount.incrementAndGet();

            if (completed.get()) {
              postTerminal.set(true);
            } else {
              failure.set(error);
            }
          },
          () -> completed.set(true),
          onSubscribe);
    }
  }

  /** A transport that records demand and cancellation behind a manually driven sink. */
  private static final class Transport {
    final AtomicReference<FluxSink<Payload>> driver = new AtomicReference<>();
    final List<Long> requests = new ArrayList<>();
    final AtomicBoolean cancelled = new AtomicBoolean();

    Flux<Payload> flux() {
      return Flux.<Payload>create(driver::set)
          .doOnRequest(requests::add)
          .doOnCancel(() -> cancelled.set(true));
    }
  }
}
