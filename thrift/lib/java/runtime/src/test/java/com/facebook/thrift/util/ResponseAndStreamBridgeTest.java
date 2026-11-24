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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.facebook.thrift.client.ResponseWrapper;
import com.facebook.thrift.model.ResponseAndStream;
import com.facebook.thrift.model.StreamResponse;
import com.facebook.thrift.payload.ClientResponsePayload;
import com.google.common.collect.ImmutableMap;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.thrift.ResponseRpcMetadata;
import org.apache.thrift.StreamPayloadMetadata;
import org.apache.thrift.TApplicationException;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * Locks down {@link ResponseAndStreamBridge#toResponseAndStream}: the client-side bridge from a raw
 * decoded stream into {@code Mono<ResponseWrapper<ResponseAndStream<F, ResponseWrapper<E>>>>}.
 */
public class ResponseAndStreamBridgeTest {

  private static Flux<ClientResponsePayload<?>> sourceOf(ClientResponsePayload<?>... payloads) {
    // hide() defeats operator fusion, matching the async RSocket source; the one explicitly
    // fuseable test below covers Flux.fromArray without it.
    return Flux.fromArray(payloads).hide();
  }

  private static ClientResponsePayload<StreamResponse<String, String>> first(String value) {
    return ClientResponsePayload.createResult(
        StreamResponse.fromFirstResponse(value),
        ResponseRpcMetadata.defaultInstance(),
        StreamPayloadMetadata.defaultInstance(),
        true);
  }

  private static ClientResponsePayload<StreamResponse<String, String>> data(String value) {
    return data(value, ImmutableMap.of(), ImmutableMap.of());
  }

  private static ClientResponsePayload<StreamResponse<String, String>> data(
      String value, Map<String, String> headers, Map<String, byte[]> binaryHeaders) {
    ResponseRpcMetadata metadata =
        new ResponseRpcMetadata.Builder().setOtherMetadata(headers).build();
    return ClientResponsePayload.createStreamResult(
        StreamResponse.fromData(value),
        metadata,
        StreamPayloadMetadata.defaultInstance(),
        binaryHeaders,
        null);
  }

  private static ClientResponsePayload<StreamResponse<String, String>> headerOnly(
      Map<String, byte[]> binaryHeaders) {
    // Built like the production decode path: an empty body decodes to an empty `StreamResponse`
    // with the headers in the stream-scope metadata.
    return ClientResponsePayload.createEmptyStreamingResult(
        ResponseRpcMetadata.defaultInstance(),
        new StreamPayloadMetadata.Builder().setOtherMetadata(binaryHeaders).build());
  }

  private static ClientResponsePayload<StreamResponse<String, String>> failure(
      Exception exception) {
    return ClientResponsePayload.createException(
        exception,
        ResponseRpcMetadata.defaultInstance(),
        StreamPayloadMetadata.defaultInstance(),
        true);
  }

  private static Flux<String> valuesOf(ResponseAndStream<String, ResponseWrapper<String>> rs) {
    return rs.getStream().map(ResponseWrapper::getData);
  }

  @Test
  public void resolvesOnFirstAndStreamsRest() {
    Flux<ClientResponsePayload<?>> source = sourceOf(first("hello"), data("a"), data("b"));

    ResponseWrapper<ResponseAndStream<String, ResponseWrapper<String>>> wrapper =
        ResponseAndStreamBridge.<String, String>toResponseAndStream(source).block();
    assertEquals("hello", wrapper.getData().getFirstResponse());
    StepVerifier.create(valuesOf(wrapper.getData())).expectNext("a", "b").verifyComplete();
  }

  @Test
  public void fuseableSourceDelivers() {
    // Same flow over a fuseable (non-hidden) source: values must be identical.
    Flux<ClientResponsePayload<?>> source =
        Flux.fromArray(new ClientResponsePayload<?>[] {first("hello"), data("a"), data("b")});

    ResponseWrapper<ResponseAndStream<String, ResponseWrapper<String>>> wrapper =
        ResponseAndStreamBridge.<String, String>toResponseAndStream(source).block();
    assertEquals("hello", wrapper.getData().getFirstResponse());
    StepVerifier.create(valuesOf(wrapper.getData())).expectNext("a", "b").verifyComplete();
  }

  @Test
  public void headersComeFromFirstPayload() {
    ResponseRpcMetadata metadata =
        new ResponseRpcMetadata.Builder().setOtherMetadata(ImmutableMap.of("k", "v")).build();
    Flux<ClientResponsePayload<?>> source =
        sourceOf(
            ClientResponsePayload.createResult(
                StreamResponse.fromFirstResponse("hello"),
                metadata,
                StreamPayloadMetadata.defaultInstance(),
                true));

    StepVerifier.create(ResponseAndStreamBridge.toResponseAndStream(source))
        .assertNext(wrapper -> assertEquals("v", wrapper.getHeaders().get("k")))
        .verifyComplete();
  }

  @Test
  public void innerElementsCarryTheirOwnHeaders() {
    Map<String, byte[]> binaryHeaders = ImmutableMap.of("bink", new byte[] {1, 2, 3});
    Flux<ClientResponsePayload<?>> source =
        sourceOf(first("hello"), data("a", ImmutableMap.of("k", "v"), binaryHeaders));

    ResponseWrapper<ResponseAndStream<String, ResponseWrapper<String>>> wrapper =
        ResponseAndStreamBridge.<String, String>toResponseAndStream(source).block();
    StepVerifier.create(wrapper.getData().getStream())
        .assertNext(
            item -> {
              assertEquals("a", item.getData());
              assertEquals("v", item.getHeaders().get("k"));
              assertEquals(binaryHeaders, item.getBinaryHeaders());
            })
        .verifyComplete();
  }

  @Test
  public void demandIsWindowedUpFrontAndDecoupled() {
    AtomicLong requested = new AtomicLong();
    Flux<ClientResponsePayload<?>> source =
        sourceOf(first("hello"), data("a")).doOnRequest(requested::addAndGet);

    ResponseWrapper<ResponseAndStream<String, ResponseWrapper<String>>> wrapper =
        ResponseAndStreamBridge.<String, String>toResponseAndStream(source).block();
    assertEquals("hello", wrapper.getData().getFirstResponse());
    // The bridge opens the C++-parity window up front: the wire sees one demand signal.
    // Downstream demand (switch, holder refill, inner) is served from the window.
    assertEquals(1 + ResponseAndStreamBridge.STREAM_WINDOW, requested.get());

    // Subscribing the inner Flux with zero initial demand changes nothing upstream.
    StepVerifier.create(wrapper.getData().getStream(), 0)
        .expectSubscription()
        .then(() -> assertEquals(1 + ResponseAndStreamBridge.STREAM_WINDOW, requested.get()))
        // Explicit demand is served from the window without touching the transport: only
        // two elements flow here, far below the refill threshold.
        .thenRequest(2)
        .assertNext(item -> assertEquals("a", item.getData()))
        .verifyComplete();
    assertEquals(
        1 + ResponseAndStreamBridge.STREAM_WINDOW,
        requested.get(),
        "inner demand must stay downstream of the window");
  }

  @Test
  public void interactionPathKeepsWindowedUpstreamDemand() {
    AtomicLong requested = new AtomicLong();
    Flux<ClientResponsePayload<?>> source =
        sourceOf(first("hello"), data("a")).doOnRequest(requested::addAndGet);

    ResponseWrapper<ResponseAndStream<String, ResponseWrapper<String>>> wrapper =
        ResponseAndStreamBridge.<String, String>toResponseAndStream(source).block();
    assertEquals("hello", wrapper.getData().getFirstResponse());
    // The bridge window goes out regardless of mode; inner-side demand passes straight through.
    assertEquals(1 + ResponseAndStreamBridge.STREAM_WINDOW, requested.get());

    StepVerifier.create(wrapper.getData().getStream(), 0)
        .expectSubscription()
        .then(() -> assertEquals(1 + ResponseAndStreamBridge.STREAM_WINDOW, requested.get()))
        .thenRequest(2)
        .assertNext(item -> assertEquals("a", item.getData()))
        .verifyComplete();
  }

  @Test
  public void headerOnlyFrameIsDeliveredWithItsHeaders() {
    Flux<ClientResponsePayload<?>> source =
        sourceOf(
            first("hello"),
            headerOnly(
                ImmutableMap.of("RS_FlowStatus", "accepted".getBytes(StandardCharsets.UTF_8))),
            data("a"));

    ResponseWrapper<ResponseAndStream<String, ResponseWrapper<String>>> wrapper =
        ResponseAndStreamBridge.<String, String>toResponseAndStream(source).block();
    StepVerifier.create(wrapper.getData().getStream())
        .assertNext(
            item -> {
              assertNull(item.getData());
              assertEquals("accepted", item.getHeaders().get("RS_FlowStatus"));
            })
        .assertNext(item -> assertEquals("a", item.getData()))
        .verifyComplete();
  }

  @Test
  public void innerIsSingleSubscription() {
    Flux<ClientResponsePayload<?>> source = sourceOf(first("hello"));
    ResponseWrapper<ResponseAndStream<String, ResponseWrapper<String>>> wrapper =
        ResponseAndStreamBridge.<String, String>toResponseAndStream(source).block();
    assertEquals("hello", wrapper.getData().getFirstResponse());

    StepVerifier.create(valuesOf(wrapper.getData())).verifyComplete();
    // The one lifecycle gate is already claimed: a second subscription fails fast with
    // the bridge's own error instead of reaching the transport or hanging.
    StepVerifier.create(wrapper.getData().getStream())
        .verifyErrorMessage(
            "getStream() supports one subscription: to retry, resubscribe the outer Mono;"
                + " to share one stream, use publish() or cache()");
  }

  @Test
  public void firstPayloadExceptionIsOuterError() {
    TApplicationException exception =
        new TApplicationException(TApplicationException.INTERNAL_ERROR, "boom");
    Flux<ClientResponsePayload<?>> source = sourceOf(failure(exception));

    StepVerifier.create(ResponseAndStreamBridge.toResponseAndStream(source))
        .verifyErrorMessage("boom");
  }

  @Test
  public void emptySourceIsOuterError() {
    StepVerifier.create(ResponseAndStreamBridge.toResponseAndStream(sourceOf()))
        .verifyErrorMessage("Stream completed without a first response");
  }

  @Test
  public void headerOnlyAsFirstIsOuterError() {
    StepVerifier.create(
            ResponseAndStreamBridge.toResponseAndStream(sourceOf(headerOnly(ImmutableMap.of()))))
        .verifyErrorMessage("First stream element did not carry the first response");
  }

  @Test
  public void dataAsFirstIsOuterError() {
    StepVerifier.create(ResponseAndStreamBridge.toResponseAndStream(sourceOf(data("a"))))
        .verifyErrorMessage("First stream element did not carry the first response");
  }

  @Test
  public void exceptionAfterFirstIsInnerError() {
    TApplicationException exception =
        new TApplicationException(TApplicationException.INTERNAL_ERROR, "stream boom");
    Flux<ClientResponsePayload<?>> source = sourceOf(first("hello"), failure(exception));

    ResponseWrapper<ResponseAndStream<String, ResponseWrapper<String>>> wrapper =
        ResponseAndStreamBridge.<String, String>toResponseAndStream(source).block();
    assertEquals("hello", wrapper.getData().getFirstResponse());
    StepVerifier.create(valuesOf(wrapper.getData())).verifyErrorMessage("stream boom");
  }

  @Test
  public void sourceErrorAfterFirstIsInnerError() {
    Flux<ClientResponsePayload<?>> source =
        sourceOf(first("hello"))
            .concatWith(Flux.error(new IllegalStateException("transport boom")));

    ResponseWrapper<ResponseAndStream<String, ResponseWrapper<String>>> wrapper =
        ResponseAndStreamBridge.<String, String>toResponseAndStream(source).block();
    assertEquals("hello", wrapper.getData().getFirstResponse());
    StepVerifier.create(valuesOf(wrapper.getData())).verifyErrorMessage("transport boom");
  }

  @Test
  public void cancelInnerCancelsUpstream() {
    AtomicBoolean cancelled = new AtomicBoolean();
    Flux<ClientResponsePayload<?>> source =
        sourceOf(first("hello")).concatWith(Flux.never()).doOnCancel(() -> cancelled.set(true));

    ResponseWrapper<ResponseAndStream<String, ResponseWrapper<String>>> wrapper =
        ResponseAndStreamBridge.<String, String>toResponseAndStream(source).block();
    assertEquals("hello", wrapper.getData().getFirstResponse());

    StepVerifier.create(wrapper.getData().getStream()).thenCancel().verify();
    assertTrue(cancelled.get(), "cancelling the inner Flux must cancel the upstream stream");
  }

  @Test
  public void cancelOuterBeforeFirstCancelsUpstream() {
    AtomicBoolean cancelled = new AtomicBoolean();
    AtomicLong requested = new AtomicLong();
    // A pending source: subscribes (so the first-response credit goes out) but never emits.
    Flux<ClientResponsePayload<?>> source =
        Flux.<ClientResponsePayload<?>>create(sink -> {})
            .doOnCancel(() -> cancelled.set(true))
            .doOnRequest(requested::addAndGet);

    Mono<ResponseWrapper<ResponseAndStream<String, ResponseWrapper<String>>>> mono =
        ResponseAndStreamBridge.toResponseAndStream(source);
    reactor.core.Disposable subscription = mono.subscribe();
    assertEquals(
        1 + ResponseAndStreamBridge.STREAM_WINDOW,
        requested.get(),
        "outer subscribe opens the window once");
    subscription.dispose();

    assertTrue(cancelled.get(), "cancelling the outer Mono must cancel the upstream stream");
  }

  @Test
  public void disposeWithoutInnerSubscribeCancelsUpstream() throws Exception {
    AtomicBoolean cancelled = new AtomicBoolean();
    AtomicLong requested = new AtomicLong();
    Flux<ClientResponsePayload<?>> source =
        sourceOf(first("hello"))
            .concatWith(Flux.never())
            .doOnCancel(() -> cancelled.set(true))
            .doOnRequest(requested::addAndGet);

    ResponseWrapper<ResponseAndStream<String, ResponseWrapper<String>>> wrapper =
        ResponseAndStreamBridge.<String, String>toResponseAndStream(source).block();
    assertEquals(1 + ResponseAndStreamBridge.STREAM_WINDOW, requested.get());
    assertFalse(wrapper.getData().isDisposed());

    // Abandoning the inner stream (never subscribed, never disposed) cancels the transport.
    wrapper.getData().dispose();
    assertTrue(cancelled.get(), "disposing without subscribing must cancel the upstream stream");
    assertTrue(wrapper.getData().isDisposed());
    assertEquals(
        1 + ResponseAndStreamBridge.STREAM_WINDOW,
        requested.get(),
        "dispose must not request further credits");
    // Idempotent.
    wrapper.getData().dispose();
  }

  @Test
  public void sameMonoTwoHoldersAreIndependent() {
    AtomicLong cancels = new AtomicLong();
    // One Mono assembly, two subscriptions: the defer builds per-subscription lifecycle state,
    // so one holder's dispose cannot cancel the other's transport stream.
    Mono<ResponseWrapper<ResponseAndStream<String, ResponseWrapper<String>>>> mono =
        ResponseAndStreamBridge.toResponseAndStream(
            sourceOf(first("hello")).concatWith(Flux.never()).doOnCancel(cancels::incrementAndGet));

    ResponseWrapper<ResponseAndStream<String, ResponseWrapper<String>>> holderA = mono.block();
    ResponseWrapper<ResponseAndStream<String, ResponseWrapper<String>>> holderB = mono.block();

    // Disposing B tears down only B's upstream subscription.
    holderB.getData().dispose();
    assertEquals(1, cancels.get());
    assertTrue(holderB.getData().isDisposed());
    assertFalse(holderA.getData().isDisposed());

    // A's gate is untouched: its inner subscribes normally, and tearing it down cancels
    // exactly the one remaining upstream subscription.
    StepVerifier.create(holderA.getData().getStream()).expectSubscription().thenCancel().verify();
    assertEquals(2, cancels.get());
  }

  @Test
  public void subscribeAfterDisposeErrorsImmediately() {
    AtomicBoolean cancelled = new AtomicBoolean();
    Flux<ClientResponsePayload<?>> source =
        sourceOf(first("hello")).concatWith(Flux.never()).doOnCancel(() -> cancelled.set(true));

    ResponseWrapper<ResponseAndStream<String, ResponseWrapper<String>>> wrapper =
        ResponseAndStreamBridge.<String, String>toResponseAndStream(source).block();
    wrapper.getData().dispose();
    assertTrue(cancelled.get(), "disposing without subscribing must cancel the upstream stream");

    // The gate is claimed: subscribing now fails fast instead of hanging forever.
    StepVerifier.create(valuesOf(wrapper.getData()))
        .verifyErrorMessage(
            "getStream() was already disposed and the stream abandoned:"
                + " to retry, resubscribe the outer Mono");
  }

  @Test
  public void disposeCancelsActiveInner() {
    AtomicLong cancels = new AtomicLong();
    Flux<ClientResponsePayload<?>> source =
        sourceOf(first("hello"), data("a"))
            .concatWith(Flux.never())
            .doOnCancel(cancels::incrementAndGet);

    ResponseWrapper<ResponseAndStream<String, ResponseWrapper<String>>> wrapper =
        ResponseAndStreamBridge.<String, String>toResponseAndStream(source).block();
    // Subscribing the inner stream starts delivery; disposing the holder then cancels it
    // (the transport must not be left open).
    reactor.core.Disposable innerDisp =
        wrapper.getData().getStream().subscribe(item -> {}, error -> {}, () -> {});
    wrapper.getData().dispose();
    assertEquals(1, cancels.get(), "disposing with an active inner must cancel upstream once");
    // The user's own subscription handle still works and does not re-cancel.
    innerDisp.dispose();
    assertEquals(1, cancels.get(), "no second transport cancel for the same stream");
  }

  @Test
  public void disposeAfterInnerTerminalDoesNotRecancel() {
    AtomicLong cancels = new AtomicLong();
    Flux<ClientResponsePayload<?>> source =
        sourceOf(first("hello")).doOnCancel(cancels::incrementAndGet);

    ResponseWrapper<ResponseAndStream<String, ResponseWrapper<String>>> wrapper =
        ResponseAndStreamBridge.<String, String>toResponseAndStream(source).block();
    StepVerifier.create(valuesOf(wrapper.getData())).verifyComplete();
    assertEquals(0, cancels.get());
    // The stream already terminated: disposing the holder must not cancel the transport.
    wrapper.getData().dispose();
    assertEquals(0, cancels.get(), "disposing a terminated stream must not cancel upstream");
  }

  @Test
  public void disposeDuringInnerSubscribeFlightCancelsOnce() throws Exception {
    // Deterministic install-race cover: pause the subscribing thread after the bridge
    // installed the inner subscription but before user code runs, dispose from the main
    // thread, then resume. The transport must be cancelled exactly once however the
    // install and the dispose interleave.
    AtomicLong cancels = new AtomicLong();
    Flux<ClientResponsePayload<?>> source =
        sourceOf(first("hello")).concatWith(Flux.never()).doOnCancel(cancels::incrementAndGet);

    ResponseWrapper<ResponseAndStream<String, ResponseWrapper<String>>> wrapper =
        ResponseAndStreamBridge.<String, String>toResponseAndStream(source).block();
    CountDownLatch installed = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicReference<Throwable> error = new AtomicReference<>();
    Thread subscriber =
        new Thread(
            () ->
                wrapper
                    .getData()
                    .getStream()
                    .subscribe(
                        item -> {},
                        error::set,
                        () -> {},
                        subscription -> {
                          installed.countDown();
                          try {
                            release.await(5, TimeUnit.SECONDS);
                          } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                          }
                        }));
    subscriber.setDaemon(true);
    subscriber.start();
    assertTrue(installed.await(5, TimeUnit.SECONDS), "inner subscription must install");
    // The cell holds the live subscription now: disposing must cancel it.
    wrapper.getData().dispose();
    release.countDown();
    subscriber.join(5000);
    assertEquals(1, cancels.get(), "dispose in flight must cancel upstream exactly once");
    assertNull(error.get(), "no error expected on the torn-down inner");
  }

  @Test
  public void concurrentSubscribeVsDisposeAlwaysCancelsOnce() throws Exception {
    // Outcome-deterministic race loop: whatever order subscribe and dispose land in, the
    // upstream is cancelled exactly once and nobody hangs (loser fails fast with ISE).
    for (int i = 0; i < 300; i++) {
      AtomicLong cancels = new AtomicLong();
      Flux<ClientResponsePayload<?>> source =
          sourceOf(first("hello")).concatWith(Flux.never()).doOnCancel(cancels::incrementAndGet);
      ResponseWrapper<ResponseAndStream<String, ResponseWrapper<String>>> wrapper =
          ResponseAndStreamBridge.<String, String>toResponseAndStream(source).block();
      CountDownLatch start = new CountDownLatch(1);
      AtomicReference<Throwable> subscribeError = new AtomicReference<>();
      Thread a =
          new Thread(
              () -> {
                try {
                  start.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  return;
                }
                wrapper.getData().getStream().subscribe(item -> {}, subscribeError::set, () -> {});
              });
      Thread b =
          new Thread(
              () -> {
                try {
                  start.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  return;
                }
                wrapper.getData().dispose();
              });
      a.setDaemon(true);
      b.setDaemon(true);
      a.start();
      b.start();
      start.countDown();
      a.join(5000);
      b.join(5000);
      assertEquals(1, cancels.get(), "iteration " + i + ": upstream cancelled exactly once");
      Throwable t = subscribeError.get();
      assertTrue(
          t == null || t instanceof IllegalStateException,
          "iteration " + i + ": subscribe either wins or fails fast, got " + t);
    }
  }

  @Test
  public void disposeIfDroppedDisposesHoldersAndIgnoresPayloads() {
    // The discard hook's class filter also matches ClientResponsePayload (it implements
    // ResponseWrapper): holders must be disposed, bare payloads ignored without a
    // ClassCastException.
    AtomicBoolean holderDisposed = new AtomicBoolean();
    ResponseAndStream<String, ResponseWrapper<String>> holder =
        ResponseAndStream.of("hello", Flux.never(), () -> holderDisposed.set(true));
    ResponseAndStreamBridge.disposeIfDropped(
        ResponseWrapper.create(holder, ImmutableMap.of(), ImmutableMap.of()));
    assertTrue(holderDisposed.get(), "a dropped holder must be disposed");

    // A bare payload carries no ResponseAndStream: must be a silent no-op (the old inline
    // cast threw ClassCastException here, which Reactor only warn-logs).
    ResponseAndStreamBridge.disposeIfDropped(data("a"));
  }

  @Test
  public void lateFirstAfterOuterCancelDiscardsWithoutRecancel() {
    // A source that pushes the first payload even after the outer was cancelled: the late
    // payload is discarded (observed via a subscriber-side discard hook), must not be
    // mistaken for a holder (the old inline cast threw ClassCastException, which Reactor
    // only warn-logs), and must not cancel the already-torn-down upstream again.
    AtomicLong cancels = new AtomicLong();
    AtomicReference<org.reactivestreams.Subscriber<? super ClientResponsePayload<?>>> downstream =
        new AtomicReference<>();
    Subscription session =
        new Subscription() {
          @Override
          public void request(long n) {}

          @Override
          public void cancel() {
            cancels.incrementAndGet();
          }
        };
    Flux<ClientResponsePayload<?>> source =
        Flux.from(
            (org.reactivestreams.Subscriber<? super ClientResponsePayload<?>> subscriber) -> {
              downstream.set(subscriber);
              subscriber.onSubscribe(session);
            });

    List<Object> dropped = Collections.synchronizedList(new ArrayList<>());
    Mono<ResponseWrapper<ResponseAndStream<String, ResponseWrapper<String>>>> mono =
        ResponseAndStreamBridge.<String, String>toResponseAndStream(source)
            .doOnDiscard(ClientResponsePayload.class, dropped::add);
    reactor.core.Disposable subscription = mono.subscribe();
    // Cancel before the first payload: tears down the upstream.
    subscription.dispose();
    assertEquals(1, cancels.get(), "outer cancel must cancel upstream once");

    // Capture stderr around the push: a ClassCastException inside the production discard
    // hook surfaces only as a Reactor warn log.
    java.io.PrintStream savedErr = System.err;
    java.io.ByteArrayOutputStream errCapture = new java.io.ByteArrayOutputStream();
    System.setErr(new java.io.PrintStream(errCapture));
    try {
      // The source pushes anyway: the late first payload is discarded, not delivered and
      // not re-cancelled.
      downstream.get().onNext(first("hello"));
    } finally {
      System.setErr(savedErr);
    }
    assertEquals(1, dropped.size(), "the late first payload must be discarded");
    assertTrue(dropped.get(0) instanceof ClientResponsePayload);
    assertEquals(1, cancels.get(), "the discarded payload must not cancel upstream again");
    assertFalse(
        errCapture.toString().contains("ClassCastException"),
        "discarding a payload must log no warning: " + errCapture);
  }

  @Test
  public void outerTakeOneKeepsInnerUsable() {
    AtomicBoolean cancelled = new AtomicBoolean();
    Flux<ClientResponsePayload<?>> source =
        sourceOf(first("hello"), data("a")).doOnCancel(() -> cancelled.set(true));

    // take(1) cancels the outer after the value: the remainder must stay alive for the inner.
    ResponseWrapper<ResponseAndStream<String, ResponseWrapper<String>>> wrapper =
        ResponseAndStreamBridge.<String, String>toResponseAndStream(source)
            .flux()
            .take(1)
            .blockLast();
    assertFalse(cancelled.get(), "outer take(1) must not cancel the remainder stream");
    StepVerifier.create(valuesOf(wrapper.getData())).expectNext("a").verifyComplete();
  }

  @Test
  public void lateInnerSubscribeAfterCompletionSeesTerminal() {
    // The terminal arrived before anyone subscribed to the inner Flux: it is replayed.
    Flux<ClientResponsePayload<?>> source =
        Flux.just(first("hello"), headerOnly(ImmutableMap.of()));

    ResponseWrapper<ResponseAndStream<String, ResponseWrapper<String>>> wrapper =
        ResponseAndStreamBridge.<String, String>toResponseAndStream(source).block();
    StepVerifier.create(wrapper.getData().getStream())
        .assertNext(item -> assertNull(item.getData()))
        .verifyComplete();
  }

  @Test
  public void interactionLiveSignalsStayOnTransportThread() throws Exception {
    // The inner subscribes before the transport emits anything past the first response:
    // every live signal runs on the transport thread, with no bridge hop.
    AtomicReference<FluxSink<ClientResponsePayload<?>>> driver = new AtomicReference<>();
    Flux<ClientResponsePayload<?>> source =
        Flux.<ClientResponsePayload<?>>create(driver::set).hide();

    ExecutorService subscriber = Executors.newSingleThreadExecutor();
    ExecutorService transport = Executors.newSingleThreadExecutor();

    try {
      Set<String> valueThreads = ConcurrentHashMap.newKeySet();
      AtomicReference<String> transportThread = new AtomicReference<>();
      CountDownLatch innerSubscribed = new CountDownLatch(1);
      AtomicReference<Throwable> innerError = new AtomicReference<>();
      List<String> innerValues = Collections.synchronizedList(new ArrayList<>());
      CountDownLatch innerDone = new CountDownLatch(1);

      Future<ResponseWrapper<ResponseAndStream<String, ResponseWrapper<String>>>> resolved =
          subscriber.submit(
              () ->
                  ResponseAndStreamBridge.<String, String>toResponseAndStream(source)
                      .doOnNext(w -> valueThreads.add(Thread.currentThread().getName()))
                      .block(Duration.ofSeconds(5)));

      while (driver.get() == null) {
        Thread.yield();
      }

      Future<?> firstSent =
          transport.submit(
              () -> {
                transportThread.set(Thread.currentThread().getName());
                driver.get().next(first("hello"));
              });

      firstSent.get(5, TimeUnit.SECONDS);

      ResponseWrapper<ResponseAndStream<String, ResponseWrapper<String>>> wrapper =
          resolved.get(5, TimeUnit.SECONDS);

      wrapper
          .getData()
          .getStream()
          .doOnSubscribe(s -> innerSubscribed.countDown())
          .doOnNext(item -> valueThreads.add(Thread.currentThread().getName()))
          .subscribe(
              item -> innerValues.add(item.getData()),
              error -> {
                innerError.set(error);
                innerDone.countDown();
              },
              innerDone::countDown);

      assertTrue(innerSubscribed.await(5, TimeUnit.SECONDS));

      RuntimeException boom = new RuntimeException("boom");

      transport.execute(
          () -> {
            driver.get().next(data("a"));
            driver.get().error(boom);
          });

      assertTrue(innerDone.await(5, TimeUnit.SECONDS));

      assertEquals(Collections.singletonList("a"), innerValues);
      assertEquals(boom, innerError.get());
      assertFalse(valueThreads.isEmpty(), "expected signals to be delivered");
      assertEquals(
          Collections.singleton(transportThread.get()),
          valueThreads,
          "with no bridge hop every live signal runs on the transport thread");
    } finally {
      subscriber.shutdownNow();
      transport.shutdownNow();
    }
  }

  @Test
  public void interactionLateInnerReplaysWithoutScheduling() throws Exception {
    // The transport finishes before the inner subscribes: the buffered elements replay
    // on the subscribing thread, with no scheduler hop in either direction.
    AtomicReference<FluxSink<ClientResponsePayload<?>>> driver = new AtomicReference<>();
    Flux<ClientResponsePayload<?>> source =
        Flux.<ClientResponsePayload<?>>create(driver::set).hide();

    ExecutorService subscriber = Executors.newSingleThreadExecutor();
    ExecutorService transport = Executors.newSingleThreadExecutor();

    try {
      Future<ResponseWrapper<ResponseAndStream<String, ResponseWrapper<String>>>> resolved =
          subscriber.submit(
              () ->
                  ResponseAndStreamBridge.<String, String>toResponseAndStream(source)
                      .block(Duration.ofSeconds(5)));

      while (driver.get() == null) {
        Thread.yield();
      }

      Future<?> emitting =
          transport.submit(
              () -> {
                FluxSink<ClientResponsePayload<?>> sink = driver.get();
                sink.next(first("hello"));
                sink.next(data("a"));
                sink.next(data("b"));
                sink.complete();
              });

      emitting.get(5, TimeUnit.SECONDS);

      ResponseWrapper<ResponseAndStream<String, ResponseWrapper<String>>> wrapper =
          resolved.get(5, TimeUnit.SECONDS);

      String subscribingThread = Thread.currentThread().getName();
      Set<String> replayThreads = ConcurrentHashMap.newKeySet();

      List<String> replayed =
          wrapper
              .getData()
              .getStream()
              .map(item -> item.getData())
              .doOnNext(v -> replayThreads.add(Thread.currentThread().getName()))
              .collectList()
              .block(Duration.ofSeconds(5));

      assertEquals(Arrays.asList("a", "b"), replayed);
      assertEquals(
          Collections.singleton(subscribingThread),
          replayThreads,
          "replay runs on the subscribing thread");
    } finally {
      subscriber.shutdownNow();
      transport.shutdownNow();
    }
  }

  @Test
  public void concurrentStressPreservesOrderDemandAndTerminal() throws Exception {
    // Seeded, replayable multi-thread stress: concurrent inner requests and (in the second
    // half) a racing cancel. Retrying the same seed reproduces the same interleaving.
    for (int seed = 0; seed < 5; seed++) {
      stressOnce(seed, false);
      stressOnce(seed, true);
    }
  }

  private static void stressOnce(int seed, boolean withCancel) throws Exception {
    Random random = new Random(seed * 31 + (withCancel ? 1 : 0));
    int elements = 200;
    AtomicLong upstreamRequested = new AtomicLong();
    AtomicLong emitted = new AtomicLong();
    AtomicBoolean cancelled = new AtomicBoolean();
    AtomicReference<FluxSink<ClientResponsePayload<?>>> driver = new AtomicReference<>();
    Flux<ClientResponsePayload<?>> source =
        Flux.<ClientResponsePayload<?>>create(driver::set)
            .doOnRequest(upstreamRequested::addAndGet)
            .doOnCancel(() -> cancelled.set(true))
            .hide();

    // Emitter: emits while demand is outstanding (the window opens it immediately).
    // Frame 0 is the first response; every 7th data slot is a header-only empty frame
    // (delivered by the bridge as a null-data element). Starts before the outer subscribe:
    // the outer block() below waits for the first frame it emits.
    AtomicBoolean emitting = new AtomicBoolean(true);
    Thread emitter =
        new Thread(
            () -> {
              while (driver.get() == null && emitting.get()) {
                Thread.yield();
              }
              FluxSink<ClientResponsePayload<?>> sink = driver.get();
              long next = 0;
              while (emitting.get() && next < elements && sink != null) {
                long have = upstreamRequested.get() - emitted.get();
                if (have > 0) {
                  ClientResponsePayload<?> payload;
                  if (next == 0) {
                    payload = first("hello");
                  } else if (next % 7 == 6) {
                    payload = headerOnly(ImmutableMap.of());
                  } else {
                    payload = data("e" + next);
                  }
                  sink.next(payload);
                  emitted.incrementAndGet();
                  next++;
                } else {
                  Thread.yield();
                }
              }
              if (!cancelled.get() && sink != null) {
                sink.complete();
              }
            });
    emitter.setDaemon(true);
    emitter.start();

    ResponseWrapper<ResponseAndStream<String, ResponseWrapper<String>>> wrapper =
        ResponseAndStreamBridge.<String, String>toResponseAndStream(source).block();
    assertEquals(1 + ResponseAndStreamBridge.STREAM_WINDOW, upstreamRequested.get());

    List<String> received = Collections.synchronizedList(new ArrayList<>());
    AtomicReference<Throwable> terminalError = new AtomicReference<>();
    AtomicBoolean done = new AtomicBoolean(false);
    CountDownLatch terminal = new CountDownLatch(1);
    AtomicReference<Subscription> innerSub = new AtomicReference<>();
    wrapper
        .getData()
        .getStream()
        .subscribe(
            item -> received.add(item.getData()),
            error -> {
              terminalError.set(error);
              terminal.countDown();
            },
            () -> {
              done.set(true);
              terminal.countDown();
            },
            innerSub::set);

    // Emitter is already running (started before the outer subscribe above).

    // Racers: seeded request bursts, plus one racing cancel when enabled.
    int threads = 4;
    int opsPerThread = 100;
    ExecutorService racers = Executors.newFixedThreadPool(threads);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<?>> futures = new ArrayList<>();
    for (int t = 0; t < threads; t++) {
      futures.add(
          racers.submit(
              () -> {
                try {
                  start.await();
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  return;
                }
                for (int i = 0; i < opsPerThread; i++) {
                  long k = 1 + random.nextInt(5);
                  innerSub.get().request(k);
                }
              }));
    }
    if (withCancel) {
      futures.add(
          racers.submit(
              () -> {
                try {
                  start.await();
                  Thread.sleep(random.nextInt(50));
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  return;
                }
                innerSub.get().cancel();
                emitting.set(false);
              }));
    }
    start.countDown();
    for (Future<?> f : futures) {
      f.get(30, TimeUnit.SECONDS);
    }
    racers.shutdownNow();
    emitter.join(5000);
    assertFalse(emitter.isAlive(), "seed " + seed + ": emitter did not finish");
    // If the cancel won there is no terminal by design; otherwise completion must have arrived.
    boolean terminated =
        terminal.getCount() == 0 || (!cancelled.get() && terminal.await(5, TimeUnit.SECONDS));

    // Invariants: ordered, no duplicates, at most one terminal, no hang. Header-only
    // frames arrive as null-data elements alongside the data.
    List<String> snapshot = new ArrayList<>(received);
    List<String> dataOnly = new ArrayList<>();
    int nulls = 0;
    for (String value : snapshot) {
      if (value == null) {
        nulls++;
      } else {
        dataOnly.add(value);
      }
    }
    for (int i = 1; i < dataOnly.size(); i++) {
      int prev = Integer.parseInt(dataOnly.get(i - 1).substring(1));
      int cur = Integer.parseInt(dataOnly.get(i).substring(1));
      assertTrue(cur > prev, "seed " + seed + ": out of order or duplicate at " + i);
    }
    assertTrue(
        !(done.get() && terminalError.get() != null), "seed " + seed + ": both complete and error");
    if (!withCancel) {
      assertTrue(terminated && done.get(), "seed " + seed + ": must complete");
      int empties = 0;
      for (long n = 1; n < elements; n++) {
        if (n % 7 == 6) {
          empties++;
        }
      }
      assertEquals(elements - 1, snapshot.size(), "seed " + seed + ": lost elements");
      assertEquals(empties, nulls, "seed " + seed + ": header-only frames must be delivered");
      // The bridge windows the transport: the initial window goes out once, then half the
      // window per window consumed, paced by delivery (never by racer demand). With 200
      // elements the consumption crosses the refill threshold exactly four times.
      assertEquals(
          1
              + ResponseAndStreamBridge.STREAM_WINDOW
              + 4 * (ResponseAndStreamBridge.STREAM_WINDOW / 2),
          upstreamRequested.get(),
          "seed " + seed + ": upstream must see only window plus consumption refills");
    } else {
      assertTrue(terminated || cancelled.get(), "seed " + seed + ": must terminate or cancel");
    }
  }
}
