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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.ByteBufPayload;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.scheduler.Scheduler;

class ResponseHandoffTest {

  private static final Function<Payload, String> DECODE =
      raw -> {
        ReferenceCountUtil.safeRelease(raw);
        return "decoded";
      };

  private static final Function<Throwable, String> ERRORS =
      t -> "mapped".equals(t.getMessage()) ? "error-frame" : null;

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
    AtomicReference<MonoSink<Payload>> driver = new AtomicReference<>();
    when(rsocket.requestResponse(any())).thenAnswer(inv -> Mono.create(driver::set));

    Payload request = requestPayload();
    Payload response = responsePayload();

    List<String> values = new ArrayList<>();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    AtomicBoolean completed = new AtomicBoolean();

    verifyNoInteractions(rsocket);

    Disposable sub =
        ResponseHandoff.requestResponse(rsocket, () -> request, scheduler, DECODE, ERRORS)
            .subscribe(values::add, failure::set, () -> completed.set(true));

    verify(rsocket, times(1)).requestResponse(same(request));

    driver.get().success(response);

    assertThat(values).isEmpty();

    scheduler.runAll();

    assertThat(values).containsExactly("decoded");
    assertThat(failure.get()).isNull();
    assertThat(completed.get()).isTrue();
    assertThat(response.refCnt()).isEqualTo(1);

    sub.dispose();

    request.release();
    response.release();
  }

  @Test
  void requestResponseEncodesPerSubscription() {
    ManualScheduler scheduler = new ManualScheduler();
    RSocket rsocket = mock(RSocket.class);
    AtomicReference<MonoSink<Payload>> driver = new AtomicReference<>();
    when(rsocket.requestResponse(any())).thenAnswer(inv -> Mono.create(driver::set));

    AtomicInteger encodes = new AtomicInteger();
    List<Payload> requests = new ArrayList<>();

    List<String> values = new ArrayList<>();
    AtomicReference<Throwable> failure = new AtomicReference<>();

    Mono<String> result =
        ResponseHandoff.requestResponse(
            rsocket,
            () -> {
              Payload encoded = requestPayload();
              encodes.incrementAndGet();
              requests.add(encoded);
              return encoded;
            },
            scheduler,
            DECODE,
            ERRORS);

    Payload first = responsePayload();
    result.subscribe(values::add, failure::set);
    driver.get().success(first);

    scheduler.runAll();

    Payload second = responsePayload();
    result.subscribe(values::add, failure::set);
    driver.get().success(second);

    scheduler.runAll();

    assertThat(encodes.get()).isEqualTo(2);
    assertThat(requests.get(0)).isNotSameAs(requests.get(1));
    verify(rsocket, times(2)).requestResponse(any());
    assertThat(values).containsExactly("decoded", "decoded");
    assertThat(failure.get()).isNull();
    assertThat(first.refCnt()).isEqualTo(1);
    assertThat(second.refCnt()).isEqualTo(1);

    requests.forEach(Payload::release);
    first.release();
    second.release();
  }

  @Test
  void encodeFailureNeverCallsTransport() {
    ManualScheduler scheduler = new ManualScheduler();

    RSocket failingEncode = mock(RSocket.class);
    AtomicReference<Throwable> encodeFailure = new AtomicReference<>();
    RuntimeException encodeError = new RuntimeException("encode boom");

    ResponseHandoff.requestResponse(
            failingEncode,
            () -> {
              throw encodeError;
            },
            scheduler,
            DECODE,
            ERRORS)
        .subscribe(v -> {}, encodeFailure::set);

    verifyNoInteractions(failingEncode);
    assertThat(encodeFailure.get()).isSameAs(encodeError);
  }

  @Test
  void dispatchThrowReleasesRequest() {
    ManualScheduler scheduler = new ManualScheduler();

    RSocket failingDispatch = mock(RSocket.class);
    RuntimeException dispatchError = new RuntimeException("dispatch boom");
    when(failingDispatch.requestResponse(any())).thenThrow(dispatchError);
    Payload dispatchRequest = requestPayload();
    AtomicBoolean decodeRan = new AtomicBoolean(false);
    AtomicReference<Throwable> dispatchFailure = new AtomicReference<>();

    ResponseHandoff.requestResponse(
            failingDispatch,
            () -> dispatchRequest,
            scheduler,
            raw -> {
              decodeRan.set(true);
              ReferenceCountUtil.safeRelease(raw);
              return "decoded";
            },
            ERRORS)
        .subscribe(v -> {}, dispatchFailure::set);

    assertThat(dispatchFailure.get()).isSameAs(dispatchError);
    assertThat(decodeRan.get()).isFalse();
    assertThat(dispatchRequest.refCnt()).isEqualTo(1);

    dispatchRequest.release();
  }

  @Test
  void subscribeThrowReleasesRequest() {
    ManualScheduler scheduler = new ManualScheduler();

    RSocket failingSubscribe = mock(RSocket.class);
    RuntimeException subscribeError = new RuntimeException("subscribe boom");
    when(failingSubscribe.requestResponse(any()))
        .thenReturn(
            new Mono<Payload>() {
              @Override
              public void subscribe(CoreSubscriber<? super Payload> s) {
                throw subscribeError;
              }
            });
    Payload subscribeRequest = requestPayload();
    AtomicReference<Throwable> subscribeFailure = new AtomicReference<>();

    ResponseHandoff.requestResponse(
            failingSubscribe, () -> subscribeRequest, scheduler, DECODE, ERRORS)
        .subscribe(v -> {}, subscribeFailure::set);

    assertThat(subscribeFailure.get()).isSameAs(subscribeError);
    assertThat(subscribeRequest.refCnt()).isEqualTo(1);

    subscribeRequest.release();
  }

  @Test
  void successDecodesOffLoop() {
    ManualScheduler scheduler = new ManualScheduler();
    RSocket rsocket = mock(RSocket.class);
    Payload response = responsePayload();
    when(rsocket.requestResponse(any())).thenReturn(Mono.just(response));

    Payload request = requestPayload();

    List<String> values = new ArrayList<>();
    AtomicReference<Throwable> failure = new AtomicReference<>();

    ResponseHandoff.requestResponse(rsocket, () -> request, scheduler, DECODE, ERRORS)
        .subscribe(values::add, failure::set);

    assertThat(scheduler.tasks).hasSize(1);
    assertThat(values).isEmpty();

    scheduler.runAll();

    assertThat(values).containsExactly("decoded");
    assertThat(failure.get()).isNull();
    assertThat(response.refCnt()).isEqualTo(1);

    request.release();
    response.release();
  }

  @Test
  void decodeThrowReleasesAndErrors() {
    ManualScheduler scheduler = new ManualScheduler();
    RSocket rsocket = mock(RSocket.class);
    Payload response = responsePayload();
    when(rsocket.requestResponse(any())).thenReturn(Mono.just(response));

    Payload request = requestPayload();

    List<String> values = new ArrayList<>();
    AtomicReference<Throwable> failure = new AtomicReference<>();

    ResponseHandoff.requestResponse(
            rsocket,
            () -> request,
            scheduler,
            raw -> {
              ReferenceCountUtil.safeRelease(raw);
              throw new RuntimeException("decode boom");
            },
            ERRORS)
        .subscribe(values::add, failure::set);

    scheduler.runAll();

    assertThat(values).isEmpty();
    assertThat(failure.get()).hasMessage("decode boom");
    assertThat(response.refCnt()).isEqualTo(1);

    request.release();
    response.release();
  }

  @Test
  void cancelBeforeSubscribeNeverRequests() {
    ManualScheduler scheduler = new ManualScheduler();
    RSocket rsocket = mock(RSocket.class);
    AtomicReference<Subscriber<? super Payload>> transportSub = new AtomicReference<>();
    Publisher<Payload> inbound = transportSub::set;
    when(rsocket.requestResponse(any())).thenReturn(Mono.fromDirect(inbound));

    Payload request = requestPayload();
    Subscription transport = mock(Subscription.class);

    AtomicReference<Throwable> failure = new AtomicReference<>();

    Disposable sub =
        ResponseHandoff.requestResponse(rsocket, () -> request, scheduler, DECODE, ERRORS)
            .subscribe(v -> {}, failure::set);

    sub.dispose();

    transportSub.get().onSubscribe(transport);

    verify(transport).cancel();
    verify(transport, never()).request(anyLong());
    assertThat(failure.get()).isNull();

    request.release();
  }

  @Test
  void cancelWhileQueuedFreesWithoutSignals() {
    ManualScheduler scheduler = new ManualScheduler();
    RSocket rsocket = mock(RSocket.class);
    AtomicReference<MonoSink<Payload>> driver = new AtomicReference<>();
    when(rsocket.requestResponse(any())).thenAnswer(inv -> Mono.create(driver::set));

    Payload request = requestPayload();
    Payload response = responsePayload();

    List<String> values = new ArrayList<>();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    AtomicBoolean completed = new AtomicBoolean();

    Disposable sub =
        ResponseHandoff.requestResponse(rsocket, () -> request, scheduler, DECODE, ERRORS)
            .subscribe(values::add, failure::set, () -> completed.set(true));

    driver.get().success(response);

    assertThat(scheduler.tasks).hasSize(1);

    sub.dispose();
    scheduler.dispose();

    assertThat(scheduler.tasks).isEmpty();

    scheduler.runAll();

    assertThat(values).isEmpty();
    assertThat(completed.get()).isFalse();
    assertThat(failure.get()).isNull();
    assertThat(response.refCnt()).isEqualTo(1);

    request.release();
    response.release();
  }

  @Test
  void cancelAtDecodeEntryFreesOnce() throws Exception {
    ManualScheduler scheduler = new ManualScheduler();
    RSocket rsocket = mock(RSocket.class);
    AtomicReference<MonoSink<Payload>> driver = new AtomicReference<>();
    when(rsocket.requestResponse(any())).thenAnswer(inv -> Mono.create(driver::set));

    Payload request = requestPayload();
    Payload response = responsePayload();

    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);

    List<String> values = new ArrayList<>();
    AtomicReference<Throwable> failure = new AtomicReference<>();

    Disposable sub =
        ResponseHandoff.requestResponse(
                rsocket,
                () -> request,
                scheduler,
                raw -> {
                  entered.countDown();

                  try {
                    release.await(10, TimeUnit.SECONDS);
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  }

                  ReferenceCountUtil.safeRelease(raw);
                  return "decoded";
                },
                ERRORS)
            .subscribe(values::add, failure::set);

    driver.get().success(response);

    Thread worker = new Thread(scheduler::runAll);
    worker.setDaemon(true);
    worker.start();

    try {
      assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();

      sub.dispose();
    } finally {
      release.countDown();
    }

    worker.join(10_000);

    assertThat(worker.isAlive()).isFalse();
    assertThat(values).isEmpty();
    assertThat(failure.get()).isNull();
    assertThat(response.refCnt()).isEqualTo(1);

    request.release();
    response.release();
  }

  @Test
  void cancelBeforeUnaryPublicationIsSilent() throws Exception {
    ManualScheduler scheduler = new ManualScheduler();
    RSocket rsocket = mock(RSocket.class);
    AtomicReference<MonoSink<Payload>> driver = new AtomicReference<>();
    when(rsocket.requestResponse(any())).thenAnswer(inv -> Mono.create(driver::set));

    Payload request = requestPayload();
    Payload response = responsePayload();

    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch decoded = new CountDownLatch(1);
    CountDownLatch publish = new CountDownLatch(1);

    List<String> values = Collections.synchronizedList(new ArrayList<>());
    AtomicReference<Throwable> failure = new AtomicReference<>();
    AtomicInteger completions = new AtomicInteger();

    Disposable sub =
        ResponseHandoff.requestResponse(
                rsocket,
                () -> request,
                scheduler,
                raw -> {
                  entered.countDown();

                  try {
                    assertThat(release.await(10, TimeUnit.SECONDS)).isTrue();
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  }

                  ReferenceCountUtil.safeRelease(raw);
                  decoded.countDown();

                  try {
                    assertThat(publish.await(10, TimeUnit.SECONDS)).isTrue();
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  }

                  return "decoded";
                },
                ERRORS)
            .subscribe(values::add, failure::set, completions::incrementAndGet);

    driver.get().success(response);

    Thread worker = new Thread(scheduler::runAll);
    worker.setDaemon(true);
    worker.start();

    try {
      assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();

      release.countDown();

      assertThat(decoded.await(10, TimeUnit.SECONDS)).isTrue();

      // Cancel after decode but before publication: the value never reaches the caller.
      sub.dispose();
    } finally {
      publish.countDown();
    }

    worker.join(10_000);

    assertThat(worker.isAlive()).isFalse();
    assertThat(values).isEmpty();
    assertThat(failure.get()).isNull();
    assertThat(completions.get()).isZero();
    assertThat(response.refCnt()).isEqualTo(1);

    request.release();
    response.release();
  }

  @Test
  void cancelAfterUnaryCompletionIsSilent() {
    ManualScheduler scheduler = new ManualScheduler();
    RSocket rsocket = mock(RSocket.class);
    AtomicReference<MonoSink<Payload>> driver = new AtomicReference<>();
    when(rsocket.requestResponse(any())).thenAnswer(inv -> Mono.create(driver::set));

    Payload request = requestPayload();
    Payload response = responsePayload();

    List<String> values = new ArrayList<>();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    AtomicInteger completions = new AtomicInteger();

    Disposable sub =
        ResponseHandoff.requestResponse(rsocket, () -> request, scheduler, DECODE, ERRORS)
            .subscribe(values::add, failure::set, completions::incrementAndGet);

    driver.get().success(response);

    scheduler.runAll();

    assertThat(values).containsExactly("decoded");
    assertThat(failure.get()).isNull();
    assertThat(completions.get()).isEqualTo(1);

    // A late cancel delivers no second terminal.
    sub.dispose();

    assertThat(values).containsExactly("decoded");
    assertThat(failure.get()).isNull();
    assertThat(completions.get()).isEqualTo(1);
    assertThat(response.refCnt()).isEqualTo(1);

    request.release();
    response.release();
  }

  @Test
  void cancelFromInsideDecodeDropsValue() {
    ManualScheduler scheduler = new ManualScheduler();
    RSocket rsocket = mock(RSocket.class);
    AtomicReference<MonoSink<Payload>> driver = new AtomicReference<>();
    when(rsocket.requestResponse(any())).thenAnswer(inv -> Mono.create(driver::set));

    Payload request = requestPayload();
    Payload response = responsePayload();

    AtomicReference<Disposable> self = new AtomicReference<>();
    List<String> values = new ArrayList<>();
    AtomicReference<Throwable> failure = new AtomicReference<>();

    self.set(
        ResponseHandoff.requestResponse(
                rsocket,
                () -> request,
                scheduler,
                raw -> {
                  ReferenceCountUtil.safeRelease(raw);
                  self.get().dispose();
                  return "decoded";
                },
                ERRORS)
            .subscribe(values::add, failure::set));

    driver.get().success(response);

    scheduler.runAll();

    assertThat(values).isEmpty();
    assertThat(failure.get()).isNull();
    assertThat(response.refCnt()).isEqualTo(1);

    request.release();
    response.release();
  }

  @Test
  void lateArrivalAfterCancelFrees() {
    ManualScheduler scheduler = new ManualScheduler();
    RSocket rsocket = mock(RSocket.class);
    AtomicReference<Subscriber<? super Payload>> transportSub = new AtomicReference<>();
    Publisher<Payload> inbound = transportSub::set;
    when(rsocket.requestResponse(any())).thenReturn(Mono.fromDirect(inbound));

    Payload request = requestPayload();
    Subscription transport = mock(Subscription.class);
    Payload response = responsePayload();

    List<String> values = new ArrayList<>();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    AtomicBoolean completed = new AtomicBoolean();

    Disposable sub =
        ResponseHandoff.requestResponse(rsocket, () -> request, scheduler, DECODE, ERRORS)
            .subscribe(values::add, failure::set, () -> completed.set(true));

    transportSub.get().onSubscribe(transport);
    sub.dispose();
    transportSub.get().onNext(response);

    scheduler.runAll();

    assertThat(values).isEmpty();
    assertThat(completed.get()).isFalse();
    assertThat(failure.get()).isNull();
    assertThat(response.refCnt()).isEqualTo(1);

    request.release();
    response.release();
  }

  @Test
  void scheduleRejectionErrorsAndFrees() {
    Scheduler scheduler = mock(Scheduler.class);
    RuntimeException rejection = new RuntimeException("no workers");
    when(scheduler.schedule(any(Runnable.class))).thenThrow(rejection);
    RSocket rsocket = mock(RSocket.class);
    AtomicReference<MonoSink<Payload>> driver = new AtomicReference<>();
    when(rsocket.requestResponse(any())).thenAnswer(inv -> Mono.create(driver::set));

    Payload request = requestPayload();
    Payload response = responsePayload();

    List<String> values = new ArrayList<>();
    AtomicReference<Throwable> failure = new AtomicReference<>();

    ResponseHandoff.requestResponse(rsocket, () -> request, scheduler, DECODE, ERRORS)
        .subscribe(values::add, failure::set);

    driver.get().success(response);

    assertThat(values).isEmpty();
    assertThat(failure.get()).isSameAs(rejection);
    assertThat(response.refCnt()).isEqualTo(1);

    request.release();
    response.release();
  }

  @Test
  void unaryRejectsEachScheduledSignal() {
    Scheduler scheduler = mock(Scheduler.class);
    when(scheduler.schedule(any(Runnable.class))).thenThrow(new RuntimeException("no workers"));
    RSocket rsocket = mock(RSocket.class);

    Payload valueRequest = requestPayload();
    Payload valueResponse = responsePayload();
    AtomicReference<MonoSink<Payload>> valueDriver = new AtomicReference<>();
    when(rsocket.requestResponse(any())).thenAnswer(inv -> Mono.create(valueDriver::set));

    List<String> values = new ArrayList<>();
    AtomicReference<Throwable> valueFailure = new AtomicReference<>();

    ResponseHandoff.requestResponse(rsocket, () -> valueRequest, scheduler, DECODE, ERRORS)
        .subscribe(values::add, valueFailure::set);

    valueDriver.get().success(valueResponse);

    assertThat(values).isEmpty();
    assertThat(valueFailure.get()).hasMessage("no workers");
    assertThat(valueResponse.refCnt()).isEqualTo(1);

    valueRequest.release();
    valueResponse.release();

    Payload errorRequest = requestPayload();
    RuntimeException transportError = new RuntimeException("transport down");
    when(rsocket.requestResponse(any())).thenReturn(Mono.error(transportError));

    List<String> errorValues = new ArrayList<>();
    AtomicReference<Throwable> errorFailure = new AtomicReference<>();

    ResponseHandoff.requestResponse(rsocket, () -> errorRequest, scheduler, DECODE, ERRORS)
        .subscribe(errorValues::add, errorFailure::set);

    assertThat(errorValues).isEmpty();
    assertThat(errorFailure.get()).hasMessage("no workers");

    errorRequest.release();

    Payload emptyRequest = requestPayload();
    when(rsocket.requestResponse(any())).thenReturn(Mono.empty());

    List<String> emptyValues = new ArrayList<>();
    AtomicReference<Throwable> emptyFailure = new AtomicReference<>();
    AtomicBoolean emptyCompleted = new AtomicBoolean();

    ResponseHandoff.requestResponse(rsocket, () -> emptyRequest, scheduler, DECODE, ERRORS)
        .subscribe(emptyValues::add, emptyFailure::set, () -> emptyCompleted.set(true));

    assertThat(emptyValues).isEmpty();
    assertThat(emptyCompleted.get()).isFalse();
    assertThat(emptyFailure.get()).hasMessage("no workers");

    emptyRequest.release();
  }

  @Test
  void cancelWhileErrorTaskWaitsStaysSilent() {
    ManualScheduler scheduler = new ManualScheduler();
    RSocket rsocket = mock(RSocket.class);
    RuntimeException transportError = new RuntimeException("boom");
    when(rsocket.requestResponse(any())).thenReturn(Mono.error(transportError));

    Payload request = requestPayload();

    List<String> values = new ArrayList<>();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    AtomicBoolean completed = new AtomicBoolean();

    Disposable sub =
        ResponseHandoff.requestResponse(rsocket, () -> request, scheduler, DECODE, ERRORS)
            .subscribe(values::add, failure::set, () -> completed.set(true));

    assertThat(scheduler.tasks).hasSize(1);

    sub.dispose();

    scheduler.runAll();

    assertThat(values).isEmpty();
    assertThat(failure.get()).isNull();
    assertThat(completed.get()).isFalse();

    request.release();
  }

  @Test
  void cancelWhileEmptyTaskWaitsStaysSilent() {
    ManualScheduler scheduler = new ManualScheduler();
    RSocket rsocket = mock(RSocket.class);
    when(rsocket.requestResponse(any())).thenReturn(Mono.empty());

    Payload request = requestPayload();

    List<String> values = new ArrayList<>();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    AtomicBoolean completed = new AtomicBoolean();

    Disposable sub =
        ResponseHandoff.requestResponse(rsocket, () -> request, scheduler, DECODE, ERRORS)
            .subscribe(values::add, failure::set, () -> completed.set(true));

    assertThat(scheduler.tasks).hasSize(1);

    sub.dispose();

    scheduler.runAll();

    assertThat(values).isEmpty();
    assertThat(failure.get()).isNull();
    assertThat(completed.get()).isFalse();

    request.release();
  }

  @Test
  void rejectedErrorDeliveryCarriesOriginalAsSuppressed() {
    Scheduler scheduler = mock(Scheduler.class);
    RuntimeException rejection = new RuntimeException("no workers");
    when(scheduler.schedule(any(Runnable.class))).thenThrow(rejection);
    RSocket rsocket = mock(RSocket.class);
    RuntimeException transportError = new RuntimeException("boom");
    when(rsocket.requestResponse(any())).thenReturn(Mono.error(transportError));

    Payload request = requestPayload();

    AtomicReference<Throwable> failure = new AtomicReference<>();

    ResponseHandoff.requestResponse(rsocket, () -> request, scheduler, DECODE, ERRORS)
        .subscribe(v -> {}, failure::set);

    assertThat(failure.get()).isSameAs(rejection);
    assertThat(failure.get().getSuppressed()).containsExactly(transportError);

    request.release();
  }

  @Test
  void emptyCompletesOnScheduler() {
    ManualScheduler scheduler = new ManualScheduler();
    RSocket rsocket = mock(RSocket.class);
    when(rsocket.requestResponse(any())).thenReturn(Mono.empty());

    Payload request = requestPayload();

    List<String> values = new ArrayList<>();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    AtomicBoolean completed = new AtomicBoolean();

    ResponseHandoff.requestResponse(rsocket, () -> request, scheduler, DECODE, ERRORS)
        .subscribe(values::add, failure::set, () -> completed.set(true));

    assertThat(scheduler.tasks).hasSize(1);
    assertThat(completed.get()).isFalse();

    scheduler.runAll();

    assertThat(completed.get()).isTrue();
    assertThat(values).isEmpty();
    assertThat(failure.get()).isNull();

    request.release();
  }

  @Test
  void mappedErrorDeliversValue() {
    ManualScheduler scheduler = new ManualScheduler();
    RSocket rsocket = mock(RSocket.class);
    when(rsocket.requestResponse(any())).thenReturn(Mono.error(new RuntimeException("mapped")));

    Payload request = requestPayload();

    List<String> values = new ArrayList<>();
    AtomicReference<Throwable> failure = new AtomicReference<>();

    ResponseHandoff.requestResponse(rsocket, () -> request, scheduler, DECODE, ERRORS)
        .subscribe(values::add, failure::set);

    scheduler.runAll();

    assertThat(values).containsExactly("error-frame");
    assertThat(failure.get()).isNull();

    request.release();
  }

  @Test
  void unmappedErrorDeliversError() {
    ManualScheduler scheduler = new ManualScheduler();
    RSocket rsocket = mock(RSocket.class);
    RuntimeException transportError = new RuntimeException("transport down");
    when(rsocket.requestResponse(any())).thenReturn(Mono.error(transportError));

    Payload request = requestPayload();

    List<String> values = new ArrayList<>();
    AtomicReference<Throwable> failure = new AtomicReference<>();

    ResponseHandoff.requestResponse(rsocket, () -> request, scheduler, DECODE, ERRORS)
        .subscribe(values::add, failure::set);

    scheduler.runAll();

    assertThat(values).isEmpty();
    assertThat(failure.get()).isSameAs(transportError);

    request.release();
  }

  @Test
  void errorValueThrowDeliversMappingError() {
    ManualScheduler scheduler = new ManualScheduler();
    RSocket rsocket = mock(RSocket.class);
    RuntimeException mappingError = new RuntimeException("mapping boom");
    RuntimeException failure = new RuntimeException("mapped");
    when(rsocket.requestResponse(any())).thenReturn(Mono.error(failure));

    Payload request = requestPayload();

    List<String> values = new ArrayList<>();
    AtomicReference<Throwable> recorded = new AtomicReference<>();

    ResponseHandoff.requestResponse(
            rsocket,
            () -> request,
            scheduler,
            DECODE,
            t -> {
              throw mappingError;
            })
        .subscribe(values::add, recorded::set);

    scheduler.runAll();

    assertThat(values).isEmpty();
    assertThat(recorded.get()).isSameAs(mappingError);
    assertThat(mappingError.getSuppressed()).containsExactly(failure);

    request.release();
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
}
