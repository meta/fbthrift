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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.facebook.thrift.server.sink.SinkChunkTimeoutException;
import com.facebook.thrift.server.sink.SinkOptions;
import com.facebook.thrift.util.resources.RpcResources;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCountUtil;
import io.rsocket.Payload;
import io.rsocket.util.ByteBufPayload;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

public class SinkPayloadSubscriberTest {
  private final Peer peer = new Peer();
  private final ManualTimer timer = new ManualTimer();
  private final AtomicLong clock = new AtomicLong();
  private final ChunkTimeout chunkTimeout =
      new ChunkTimeout(() -> timer, Schedulers.immediate(), clock::get);

  @Test
  public void endReleasesTheSinkPayloadsThatNoReaderTook() {
    Payload first = payload();
    Payload second = payload();
    SinkPayloadSubscriber subscriber = subscribe(Schedulers.immediate());

    peer.send(first);
    peer.send(second);
    subscriber.end(SignalType.CANCEL);

    assertEquals(0, first.refCnt());
    assertEquals(0, second.refCnt());
  }

  @Test
  public void endReleasesTheSinkPayloadsBufferedBeforeThePeerFailed() {
    Payload buffered = payload().retain();
    SinkPayloadSubscriber subscriber = subscribe(Schedulers.immediate());

    peer.send(buffered);
    peer.fail(new ClosedChannelException());
    subscriber.end(SignalType.CANCEL);

    assertEquals(1, buffered.refCnt());
    buffered.release();
  }

  @Test
  public void aCancelledReaderReleasesItsQueuedSinkPayloadOnceOnTheEventLoop() throws Exception {
    assertReleasedOnceAfterTheReaderCancels(Schedulers.immediate(), false);
  }

  @Test
  public void aCancelledReaderReleasesItsQueuedSinkPayloadOnceOffTheEventLoop() throws Exception {
    assertReleasedOnceAfterTheReaderCancels(RpcResources.getOffLoopScheduler(), false);
  }

  @Test
  public void aReaderCancelledAfterTheConnectionClosedReleasesItsQueuedSinkPayloadOnceOnTheLoop()
      throws Exception {
    assertReleasedOnceAfterTheReaderCancels(Schedulers.immediate(), true);
  }

  @Test
  public void aReaderCancelledAfterTheConnectionClosedReleasesItsQueuedSinkPayloadOnceOffTheLoop()
      throws Exception {
    assertReleasedOnceAfterTheReaderCancels(RpcResources.getOffLoopScheduler(), true);
  }

  @Test
  public void anAbnormalEndReleasesWhatAnIdleReaderHoldsOnTheEventLoop() throws Exception {
    assertAnAbnormalEndReleasesWhatTheReaderHolds(Schedulers.immediate());
  }

  @Test
  public void anAbnormalEndReleasesWhatAnIdleReaderHoldsOffTheEventLoop() throws Exception {
    assertAnAbnormalEndReleasesWhatTheReaderHolds(RpcResources.getOffLoopScheduler());
  }

  @Test
  public void aChunkTimeoutReleasesWhatAnIdleReaderHoldsOnTheEventLoop() throws Exception {
    assertAChunkTimeoutReleasesWhatTheReaderHolds(Schedulers.immediate());
  }

  @Test
  public void aChunkTimeoutReleasesWhatAnIdleReaderHoldsOffTheEventLoop() throws Exception {
    assertAChunkTimeoutReleasesWhatTheReaderHolds(RpcResources.getOffLoopScheduler());
  }

  @Test
  public void anEarlyReaderGetsItsCreditAfterTheFirstResponseOnTheEventLoop() {
    assertAnEarlyReaderGetsItsCredit(Schedulers.immediate());
  }

  @Test
  public void anEarlyReaderGetsItsCreditAfterTheFirstResponseOffTheEventLoop() {
    assertAnEarlyReaderGetsItsCredit(RpcResources.getOffLoopScheduler());
  }

  @Test
  public void creditsStayWithinTheWindowForAnUnboundedReaderOnTheEventLoop() {
    assertCreditsStayWithinTheWindow(Schedulers.immediate());
  }

  @Test
  public void creditsStayWithinTheWindowForAnUnboundedReaderOffTheEventLoop() {
    assertCreditsStayWithinTheWindow(RpcResources.getOffLoopScheduler());
  }

  @Test
  public void sinkCreditListenerSeesTheWindowItGrantsOnTheEventLoop() {
    assertSinkCreditListenerSeesTheWindow(Schedulers.immediate());
  }

  @Test
  public void sinkCreditListenerSeesTheWindowItGrantsOffTheEventLoop() {
    assertSinkCreditListenerSeesTheWindow(RpcResources.getOffLoopScheduler());
  }

  @Test
  public void aBidiReaderCancelBeforeTheFirstResponseStaysLocal() {
    SinkPayloadSubscriber subscriber = subscribe(Schedulers.immediate());

    subscriber.bidiSinkPayloads().subscribe().dispose();
    sendFirstResponse(subscriber);

    assertFalse(peer.cancelled.get());
    assertEquals(Collections.singletonList(1L), peer.credits);
  }

  @Test
  public void aBidiReaderCancelAfterTheFirstResponseCancelsTheSink() {
    SinkPayloadSubscriber subscriber = subscribe(Schedulers.immediate());

    sendFirstResponse(subscriber);
    subscriber.bidiSinkPayloads().subscribe().dispose();

    assertTrue(peer.cancelled.get());
  }

  @Test
  public void aSinkReaderCancelStaysLocal() {
    SinkPayloadSubscriber subscriber = subscribe(Schedulers.immediate());

    sendFirstResponse(subscriber);
    subscriber.sinkPayloads().subscribe().dispose();

    assertFalse(peer.cancelled.get());
  }

  @Test
  public void aNormalEndCompletesTheReader() {
    SinkPayloadSubscriber subscriber = subscribe(Schedulers.immediate());
    sendFirstResponse(subscriber);

    StepVerifier.create(subscriber.sinkPayloads())
        .then(() -> subscriber.end(SignalType.ON_COMPLETE))
        .verifyComplete();
  }

  @Test
  public void anAbnormalEndFailsTheReaderSoItNeverCommits() {
    AtomicBoolean committed = new AtomicBoolean();
    SinkPayloadSubscriber subscriber = subscribe(Schedulers.immediate());
    sendFirstResponse(subscriber);

    StepVerifier.create(
            subscriber
                .sinkPayloads()
                .doOnNext(ReferenceCountUtil::release)
                .then(Mono.fromRunnable(() -> committed.set(true))))
        .then(() -> peer.send(payload()))
        .then(() -> subscriber.end(SignalType.CANCEL))
        .verifyError(CancellationException.class);

    assertFalse(committed.get());
  }

  @Test
  public void aReaderThatWaitsForTheFirstResponseFailsWhenTheCallEndsBeforeIt() {
    SinkPayloadSubscriber subscriber = subscribe(Schedulers.immediate());

    StepVerifier.create(subscriber.bidiSinkPayloads())
        .then(() -> subscriber.end(SignalType.ON_ERROR))
        .verifyError(CancellationException.class);

    assertEquals(Collections.singletonList(1L), peer.credits);
  }

  @Test
  public void endIfUnreadKeepsATakenSinkOpenUntilThePeerEndsIt() {
    SinkPayloadSubscriber subscriber = subscribe(Schedulers.immediate());
    sendFirstResponse(subscriber);

    StepVerifier.create(subscriber.bidiSinkPayloads().doOnNext(ReferenceCountUtil::release))
        .then(subscriber::endIfUnread)
        .then(() -> peer.send(payload()))
        .expectNextCount(1)
        .then(peer::complete)
        .verifyComplete();

    assertFalse(peer.cancelled.get());
  }

  @Test
  public void endIfUnreadEndsAnUntakenSinkAndALaterReaderFails() {
    SinkPayloadSubscriber subscriber = subscribe(Schedulers.immediate());
    sendFirstResponse(subscriber);

    subscriber.endIfUnread();

    assertTrue(peer.cancelled.get());
    StepVerifier.create(subscriber.bidiSinkPayloads()).verifyError(CancellationException.class);
  }

  @Test
  public void peerErrorThenCancelReleasesQueuedOnImmediate() throws Exception {
    assertPeerErrorThenCancelReleasesQueued(Schedulers.immediate());
  }

  @Test
  public void peerErrorThenCancelReleasesQueuedOffLoop() throws Exception {
    assertPeerErrorThenCancelReleasesQueued(RpcResources.getOffLoopScheduler());
  }

  @Test
  public void secondBidiReaderFailsFirstReaderKeepsReading() {
    SinkPayloadSubscriber subscriber = subscribe(Schedulers.immediate());
    sendFirstResponse(subscriber);

    subscriber.bidiSinkPayloads().doOnNext(ReferenceCountUtil::release).subscribe();

    List<CountedPayload> first = sendCounted(3);
    assertReleasedOnce(first);

    StepVerifier.create(subscriber.bidiSinkPayloads()).verifyError(IllegalStateException.class);

    assertFalse(peer.cancelled.get());

    List<CountedPayload> second = sendCounted(2);
    assertReleasedOnce(second);

    peer.complete();
    subscriber.end(SignalType.CANCEL);
  }

  @Test
  public void bufferSizeSetsTheCreditWindow() {
    SinkPayloadSubscriber subscriber = subscribe(Schedulers.immediate());
    SinkOptions options = SinkOptions.builder().bufferSize(256).build();

    StepVerifier.create(
            subscriber
                .sinkPayloads()
                .contextWrite(Context.of(SinkPayloadSubscriber.SINK_OPTIONS, options)))
        .expectSubscription()
        .then(() -> sendFirstResponse(subscriber))
        .then(() -> subscriber.end(SignalType.ON_COMPLETE))
        .verifyComplete();

    assertEquals(Arrays.asList(1L, 256L), peer.credits);
  }

  @Test
  public void chunkTimeoutFailsTheReaderAndSignalsTheResponses() {
    SinkPayloadSubscriber subscriber = manualSubscriber();
    SinkOptions options = SinkOptions.builder().chunkTimeout(Duration.ofSeconds(1)).build();
    AtomicBoolean signalled = new AtomicBoolean();

    Flux.never()
        .transform(subscriber.dropResponsesAfterChunkTimeout())
        .doOnComplete(() -> signalled.set(true))
        .subscribe();

    StepVerifier.create(
            subscriber
                .sinkPayloads()
                .contextWrite(Context.of(SinkPayloadSubscriber.SINK_OPTIONS, options))
                .doOnNext(ReferenceCountUtil::release))
        .then(() -> sendFirstResponse(subscriber))
        .then(this::expirePastDelay)
        .verifyError(SinkChunkTimeoutException.class);

    assertTrue(chunkTimeout.hasTimedOut());
    assertTrue(signalled.get());
  }

  @Test
  public void chunkArrivingDuringTheGrantCountsAgainstIt() {
    SinkPayloadSubscriber subscriber =
        new SinkPayloadSubscriber(Schedulers.immediate(), chunkTimeout);
    SinkOptions options =
        SinkOptions.builder().bufferSize(1).chunkTimeout(Duration.ofSeconds(1)).build();

    // The peer answers every grant with a chunk on the granting thread: the watchdog must count
    // the grant before the request goes out, or the chunk clamps at zero and the grant idles.
    AtomicReference<Subscriber<? super Payload>> downstream = new AtomicReference<>();
    Publisher<Payload> inbound =
        s -> {
          downstream.set(s);

          s.onSubscribe(
              new Subscription() {
                @Override
                public void request(long n) {
                  downstream.get().onNext(payload());
                }

                @Override
                public void cancel() {}
              });
        };
    inbound.subscribe(subscriber);

    Payload setup = subscriber.setup();
    setup.release();

    StepVerifier.create(
            subscriber
                .sinkPayloads()
                .contextWrite(Context.of(SinkPayloadSubscriber.SINK_OPTIONS, options))
                .doOnNext(ReferenceCountUtil::release),
            0)
        .then(() -> sendFirstResponse(subscriber))
        .then(this::expirePastDelay)
        .thenRequest(1)
        .expectNextCount(1)
        .then(() -> downstream.get().onComplete())
        .then(() -> subscriber.end(SignalType.ON_COMPLETE))
        .verifyComplete();

    assertFalse(chunkTimeout.hasTimedOut());
  }

  @Test
  public void rejectedSecondReaderLeavesTheFirstWatchdogWatching() throws Exception {
    SinkPayloadSubscriber subscriber = manualSubscriber();
    SinkOptions options = SinkOptions.builder().chunkTimeout(Duration.ofSeconds(1)).build();
    CompletableFuture<Throwable> rejected = new CompletableFuture<>();

    StepVerifier.create(
            subscriber
                .sinkPayloads()
                .contextWrite(Context.of(SinkPayloadSubscriber.SINK_OPTIONS, options))
                .doOnNext(ReferenceCountUtil::release))
        .then(() -> sendFirstResponse(subscriber))
        .then(
            () ->
                subscriber
                    .sinkPayloads()
                    .contextWrite(Context.of(SinkPayloadSubscriber.SINK_OPTIONS, options))
                    .subscribe(v -> {}, rejected::complete))
        .then(() -> assertEquals(Arrays.asList(1L, 100L), peer.credits))
        .then(this::expireAllPastDelay)
        .verifyError(SinkChunkTimeoutException.class);

    assertNotNull(rejected.get(5, TimeUnit.SECONDS));
    assertTrue(chunkTimeout.hasTimedOut());
  }

  @Test
  public void aReaderThatCancelsEarlyFreesTheSinkForALaterReader() {
    SinkPayloadSubscriber subscriber = manualSubscriber();
    SinkOptions options = SinkOptions.builder().chunkTimeout(Duration.ofSeconds(1)).build();

    subscriber
        .sinkPayloads()
        .contextWrite(Context.of(SinkPayloadSubscriber.SINK_OPTIONS, options))
        .subscribe()
        .dispose();

    StepVerifier.create(
            subscriber
                .sinkPayloads()
                .contextWrite(Context.of(SinkPayloadSubscriber.SINK_OPTIONS, options))
                .doOnNext(ReferenceCountUtil::release))
        .then(() -> sendFirstResponse(subscriber))
        .then(
            () -> {
              peer.send(payload());
              peer.send(payload());
              peer.complete();
            })
        .expectNextCount(2)
        .verifyComplete();

    assertFalse(chunkTimeout.hasTimedOut());
  }

  @Test
  public void peerCompleteAfterCreditCompletesWithoutTimeout() {
    SinkPayloadSubscriber subscriber = manualSubscriber();
    SinkOptions options = SinkOptions.builder().chunkTimeout(Duration.ofSeconds(1)).build();

    StepVerifier.create(
            subscriber
                .sinkPayloads()
                .contextWrite(Context.of(SinkPayloadSubscriber.SINK_OPTIONS, options))
                .doOnNext(ReferenceCountUtil::release))
        .then(() -> sendFirstResponse(subscriber))
        .then(
            () -> {
              peer.send(payload());
              peer.complete();
            })
        .expectNextCount(1)
        .verifyComplete();

    assertFalse(chunkTimeout.hasTimedOut());
    assertTrue(timer.timeout.isCancelled());
  }

  @Test
  public void zeroTimeoutArmsNoTimer() {
    SinkPayloadSubscriber subscriber = manualSubscriber();

    StepVerifier.create(subscriber.sinkPayloads().doOnNext(ReferenceCountUtil::release))
        .then(() -> sendFirstResponse(subscriber))
        .then(
            () -> {
              peer.send(payload());
              peer.complete();
            })
        .expectNextCount(1)
        .verifyComplete();

    assertEquals(0, timer.created);
    assertFalse(chunkTimeout.hasTimedOut());
  }

  @Test
  public void anExhaustedWindowStandsDownThenRearmsOnDemand() {
    SinkPayloadSubscriber subscriber = manualSubscriber();
    SinkOptions options =
        SinkOptions.builder().bufferSize(1).chunkTimeout(Duration.ofSeconds(1)).build();

    StepVerifier.create(
            subscriber
                .sinkPayloads()
                .contextWrite(Context.of(SinkPayloadSubscriber.SINK_OPTIONS, options))
                .doOnNext(ReferenceCountUtil::release),
            0)
        .expectSubscription()
        .then(() -> sendFirstResponse(subscriber))
        .then(() -> peer.send(payload()))
        .then(this::expirePastDelay)
        .then(() -> assertFalse(chunkTimeout.hasTimedOut()))
        .thenRequest(1)
        .expectNextCount(1)
        .then(() -> assertEquals(2, timer.created))
        .then(() -> peer.complete())
        .verifyComplete();

    assertFalse(chunkTimeout.hasTimedOut());
  }

  @Test
  public void theApplicationNearestTheSinkWins() {
    SinkPayloadSubscriber subscriber = subscribe(Schedulers.immediate());
    SinkOptions outer = SinkOptions.builder().bufferSize(7).build();
    SinkOptions inner = SinkOptions.builder().bufferSize(9).build();

    StepVerifier.create(outer.applyTo(inner.applyTo(subscriber.sinkPayloads())))
        .expectSubscription()
        .then(() -> sendFirstResponse(subscriber))
        .then(() -> subscriber.end(SignalType.ON_COMPLETE))
        .verifyComplete();

    assertEquals(Arrays.asList(1L, 9L), peer.credits);
  }

  @Test
  public void chunkTimeoutStillReportsTheGrantedWindow() {
    SinkPayloadSubscriber subscriber = manualSubscriber();
    SinkOptions options = SinkOptions.builder().chunkTimeout(Duration.ofSeconds(1)).build();
    List<Long> fired = new CopyOnWriteArrayList<>();

    StepVerifier.create(
            subscriber
                .sinkPayloads()
                .contextWrite(Context.of(SinkPayloadSubscriber.SINK_OPTIONS, options))
                .contextWrite(
                    Context.of(
                        SinkPayloadSubscriber.SINK_CREDIT_LISTENER, (LongConsumer) fired::add))
                .doOnNext(ReferenceCountUtil::release))
        .then(() -> sendFirstResponse(subscriber))
        .then(this::expirePastDelay)
        .verifyError(SinkChunkTimeoutException.class);

    assertEquals(Collections.singletonList(100L), fired);
  }

  @Test
  public void theReaderFailsBeforeTheResponsesEnd() {
    DelayHoldingScheduler scheduler = new DelayHoldingScheduler();
    SinkPayloadSubscriber subscriber =
        new SinkPayloadSubscriber(
            Schedulers.immediate(), new ChunkTimeout(() -> timer, scheduler, clock::get));
    SinkOptions options = SinkOptions.builder().chunkTimeout(Duration.ofSeconds(1)).build();
    List<String> events = new CopyOnWriteArrayList<>();

    attach(subscriber);
    recordTheEndOfTheResponses(subscriber, events);

    StepVerifier.create(
            subscriber
                .sinkPayloads()
                .contextWrite(Context.of(SinkPayloadSubscriber.SINK_OPTIONS, options))
                .doOnError(e -> events.add("reader failed")))
        .then(() -> sendFirstResponse(subscriber))
        .then(this::expirePastDelay)
        .verifyError(SinkChunkTimeoutException.class);

    // The backstop is still queued, so the reader's own terminal ended the responses.
    assertEquals(Arrays.asList("reader failed", "responses ended"), events);
    assertEquals(1, scheduler.delayed.size());
  }

  @Test
  public void theBackstopEndsTheResponsesWhileTheReaderIsWithheld() {
    DelayHoldingScheduler scheduler = new DelayHoldingScheduler();
    SinkPayloadSubscriber subscriber =
        new SinkPayloadSubscriber(
            Schedulers.immediate(), new ChunkTimeout(() -> timer, scheduler, clock::get));
    SinkOptions options = SinkOptions.builder().chunkTimeout(Duration.ofSeconds(1)).build();
    List<String> events = new CopyOnWriteArrayList<>();

    attach(subscriber);
    recordTheEndOfTheResponses(subscriber, events);

    StepVerifier.create(
            subscriber
                .sinkPayloads()
                .contextWrite(Context.of(SinkPayloadSubscriber.SINK_OPTIONS, options))
                .doOnError(
                    e -> {
                      events.add("reader failed");
                      scheduler.runDelayed();
                      events.add("reader resumed");
                    }))
        .then(() -> sendFirstResponse(subscriber))
        .then(this::expirePastDelay)
        .verifyError(SinkChunkTimeoutException.class);

    assertEquals(Arrays.asList("reader failed", "responses ended", "reader resumed"), events);
  }

  @Test
  public void peerErrorAfterCreditStopsTheArmedWatchdog() {
    SinkPayloadSubscriber subscriber = manualSubscriber();
    SinkOptions options = SinkOptions.builder().chunkTimeout(Duration.ofSeconds(1)).build();

    StepVerifier.create(
            subscriber
                .sinkPayloads()
                .contextWrite(Context.of(SinkPayloadSubscriber.SINK_OPTIONS, options))
                .doOnNext(ReferenceCountUtil::release))
        .then(() -> sendFirstResponse(subscriber))
        .then(() -> peer.fail(new ClosedChannelException()))
        .then(this::expirePastDelay)
        .verifyError(ClosedChannelException.class);

    assertFalse(chunkTimeout.hasTimedOut());
    assertTrue(timer.timeout.isCancelled());
  }

  @Test
  public void readerCancelAfterCreditStopsTheArmedWatchdog() {
    SinkPayloadSubscriber subscriber = manualSubscriber();
    SinkOptions options = SinkOptions.builder().chunkTimeout(Duration.ofSeconds(1)).build();

    StepVerifier.create(
            subscriber
                .sinkPayloads()
                .contextWrite(Context.of(SinkPayloadSubscriber.SINK_OPTIONS, options))
                .doOnNext(ReferenceCountUtil::release))
        .then(() -> sendFirstResponse(subscriber))
        .thenCancel()
        .verify();

    expirePastDelay();

    assertFalse(chunkTimeout.hasTimedOut());
    assertTrue(timer.timeout.isCancelled());
    assertFalse(peer.cancelled.get());
  }

  @Test
  public void callCancelAfterCreditStopsTheArmedWatchdog() {
    SinkPayloadSubscriber subscriber = manualSubscriber();
    SinkOptions options = SinkOptions.builder().chunkTimeout(Duration.ofSeconds(1)).build();

    StepVerifier.create(
            subscriber
                .sinkPayloads()
                .contextWrite(Context.of(SinkPayloadSubscriber.SINK_OPTIONS, options))
                .doOnNext(ReferenceCountUtil::release))
        .then(() -> sendFirstResponse(subscriber))
        .then(() -> subscriber.end(SignalType.CANCEL))
        .then(this::expirePastDelay)
        .verifyError(CancellationException.class);

    assertFalse(chunkTimeout.hasTimedOut());
    assertTrue(timer.timeout.isCancelled());
  }

  @Test
  public void abnormalEndAfterCreditStopsTheArmedWatchdog() {
    SinkPayloadSubscriber subscriber = manualSubscriber();
    SinkOptions options = SinkOptions.builder().chunkTimeout(Duration.ofSeconds(1)).build();

    StepVerifier.create(
            subscriber
                .sinkPayloads()
                .contextWrite(Context.of(SinkPayloadSubscriber.SINK_OPTIONS, options))
                .doOnNext(ReferenceCountUtil::release))
        .then(() -> sendFirstResponse(subscriber))
        .then(() -> subscriber.end(SignalType.ON_ERROR))
        .then(this::expirePastDelay)
        .verifyError(CancellationException.class);

    assertFalse(chunkTimeout.hasTimedOut());
    assertTrue(timer.timeout.isCancelled());
  }

  @Test
  public void aReaderAfterTheCallEndedSendsNoCredit() {
    SinkPayloadSubscriber subscriber = manualSubscriber();
    SinkOptions options = SinkOptions.builder().chunkTimeout(Duration.ofSeconds(1)).build();

    sendFirstResponse(subscriber);
    subscriber.end(SignalType.CANCEL);

    StepVerifier.create(
            subscriber
                .sinkPayloads()
                .contextWrite(Context.of(SinkPayloadSubscriber.SINK_OPTIONS, options)))
        .verifyError(CancellationException.class);

    assertEquals(Collections.singletonList(1L), peer.credits);
    assertEquals(0, timer.created);
  }

  private SinkPayloadSubscriber subscribe(Scheduler scheduler) {
    SinkPayloadSubscriber subscriber = new SinkPayloadSubscriber(scheduler);
    attach(subscriber);

    return subscriber;
  }

  private SinkPayloadSubscriber manualSubscriber() {
    SinkPayloadSubscriber subscriber =
        new SinkPayloadSubscriber(Schedulers.immediate(), chunkTimeout);
    attach(subscriber);

    return subscriber;
  }

  private void attach(SinkPayloadSubscriber subscriber) {
    peer.flux().subscribe(subscriber);

    Payload setup = payload();
    peer.send(setup);
    assertSame(setup, subscriber.setup());
    setup.release();
  }

  private void assertAnEarlyReaderGetsItsCredit(Scheduler scheduler) {
    SinkPayloadSubscriber subscriber = subscribe(scheduler);

    StepVerifier.create(subscriber.sinkPayloads())
        .expectSubscription()
        .expectNoEvent(Duration.ofMillis(50))
        .then(() -> assertEquals(Collections.singletonList(1L), peer.credits))
        .then(() -> sendFirstResponse(subscriber))
        .then(() -> subscriber.end(SignalType.ON_COMPLETE))
        .verifyComplete();

    assertEquals(Long.valueOf(100), peer.credits.get(1));
  }

  private void assertSinkCreditListenerSeesTheWindow(Scheduler scheduler) {
    SinkPayloadSubscriber subscriber = subscribe(scheduler);
    List<Long> fired = new CopyOnWriteArrayList<>();

    StepVerifier.create(
            subscriber
                .sinkPayloads()
                .contextWrite(
                    Context.of(
                        SinkPayloadSubscriber.SINK_CREDIT_LISTENER, (LongConsumer) fired::add))
                .doOnNext(ReferenceCountUtil::release))
        .expectSubscription()
        .expectNoEvent(Duration.ofMillis(50))
        .then(() -> assertTrue(fired.isEmpty(), "credit fired before the first response: " + fired))
        .then(() -> sendFirstResponse(subscriber))
        .then(
            () -> {
              for (int i = 0; i < 300; i++) {
                peer.send(payload());
              }
              peer.complete();
            })
        .expectNextCount(300)
        .verifyComplete();

    // The tap fires for every grant the peer saw, in order: the initial window, then refills,
    // never the reader's unbounded demand. A trailing refill can lose to the terminal on another
    // thread, so the tap may hold one final refill the peer never saw.
    List<Long> sent = peer.credits.subList(1, peer.credits.size());
    assertTrue(
        fired.size() == sent.size() || fired.size() == sent.size() + 1,
        "fired " + fired + " sent " + sent);
    assertEquals(sent, fired.subList(0, sent.size()));
    assertEquals(Long.valueOf(100), fired.get(0));
    assertTrue(fired.stream().skip(1).allMatch(n -> n == 75), "refills " + fired);
  }

  private void assertCreditsStayWithinTheWindow(Scheduler scheduler) {
    SinkPayloadSubscriber subscriber = subscribe(scheduler);
    sendFirstResponse(subscriber);

    StepVerifier.create(subscriber.sinkPayloads().doOnNext(ReferenceCountUtil::release))
        .then(
            () -> {
              for (int i = 0; i < 300; i++) {
                peer.send(payload());
              }
              peer.complete();
            })
        .expectNextCount(300)
        .verifyComplete();

    assertEquals(1L, peer.credits.get(0));
    assertTrue(peer.credits.stream().skip(1).allMatch(n -> n <= 100), "credits " + peer.credits);
  }

  /**
   * A reader without demand leaves a sink payload in the window's queue. Its cancel releases that
   * payload, and the end of the call then finds nothing to release.
   */
  private void assertReleasedOnceAfterTheReaderCancels(Scheduler scheduler, boolean peerFails)
      throws InterruptedException {
    CountedPayload queued = new CountedPayload();
    queued.retain();
    SinkPayloadSubscriber subscriber = subscribe(scheduler);
    sendFirstResponse(subscriber);

    peer.send(queued);
    if (peerFails) {
      peer.fail(new ClosedChannelException());
    }
    subscriber.sinkPayloads().subscribeWith(new IdleReader()).dispose();

    assertTrue(queued.awaitRelease(5, TimeUnit.SECONDS));
    subscriber.end(SignalType.CANCEL);

    assertEquals(1, queued.refCnt());
    queued.release();
  }

  /**
   * A reader without demand leaves a sink payload in the window's queue. An abnormal end of the
   * call releases that payload and a sink payload that arrives after the end, and fails the reader.
   */
  private void assertAnAbnormalEndReleasesWhatTheReaderHolds(Scheduler scheduler)
      throws InterruptedException {
    CountedPayload queued = new CountedPayload();
    queued.retain();
    CountedPayload late = new CountedPayload();
    late.retain();
    SinkPayloadSubscriber subscriber = subscribe(scheduler);
    sendFirstResponse(subscriber);
    IdleReader reader = subscriber.sinkPayloads().subscribeWith(new IdleReader());

    peer.send(queued);
    subscriber.end(SignalType.CANCEL);
    subscriber.onNext(late);

    assertTrue(queued.awaitRelease(5, TimeUnit.SECONDS));
    assertTrue(late.awaitRelease(5, TimeUnit.SECONDS));
    assertTrue(reader.awaitError() instanceof CancellationException);
    assertEquals(1, queued.refCnt());
    assertEquals(1, late.refCnt());
    queued.release();
    late.release();
  }

  /**
   * A reader without demand leaves a sink payload in the window's queue. A chunk timeout releases
   * that payload and a sink payload that arrives after the timeout, and fails the reader.
   */
  private void assertAChunkTimeoutReleasesWhatTheReaderHolds(Scheduler scheduler)
      throws InterruptedException {
    CountedPayload queued = new CountedPayload();
    queued.retain();
    CountedPayload late = new CountedPayload();
    late.retain();

    ChunkTimeout timeout = new ChunkTimeout(() -> timer, Schedulers.immediate(), clock::get);
    SinkPayloadSubscriber subscriber = new SinkPayloadSubscriber(scheduler, timeout);
    attach(subscriber);
    sendFirstResponse(subscriber);

    SinkOptions options = SinkOptions.builder().chunkTimeout(Duration.ofSeconds(1)).build();
    IdleReader reader =
        subscriber
            .sinkPayloads()
            .contextWrite(Context.of(SinkPayloadSubscriber.SINK_OPTIONS, options))
            .subscribeWith(new IdleReader());

    peer.send(queued);
    expirePastDelay();
    subscriber.onNext(late);

    assertTrue(queued.awaitRelease(5, TimeUnit.SECONDS));
    assertTrue(late.awaitRelease(5, TimeUnit.SECONDS));

    assertTrue(reader.awaitError() instanceof SinkChunkTimeoutException);
    assertEquals(1, queued.refCnt());
    assertEquals(1, late.refCnt());
    assertEquals(1, queued.releases());
    assertEquals(1, late.releases());

    queued.release();
    late.release();
  }

  private void assertPeerErrorThenCancelReleasesQueued(Scheduler scheduler) throws Exception {
    SinkPayloadSubscriber subscriber = subscribe(scheduler);
    sendFirstResponse(subscriber);

    IdleReader reader = subscriber.sinkPayloads().subscribeWith(new IdleReader());

    CountedPayload queued = new CountedPayload();
    peer.send(queued);
    peer.fail(new ClosedChannelException());

    reader.dispose();
    subscriber.end(SignalType.CANCEL);

    assertTrue(queued.awaitRelease(5, TimeUnit.SECONDS), "queued payload not released on cancel");
    assertReleasedOnce(queued);
  }

  private List<CountedPayload> sendCounted(int sends) {
    List<CountedPayload> payloads = new ArrayList<>();

    for (int i = 0; i < sends; i++) {
      CountedPayload payload = new CountedPayload();
      payloads.add(payload);
      peer.send(payload);
    }

    return payloads;
  }

  static void assertReleasedOnce(CountedPayload payload) {
    assertEquals(0, payload.refCnt(), "sink payload not released");
    assertEquals(1, payload.releases(), "sink payload not released exactly once");
  }

  static void assertReleasedOnce(List<CountedPayload> payloads) {
    for (CountedPayload payload : payloads) {
      assertReleasedOnce(payload);
    }
  }

  /** Records when the responses end: the transport cuts them here on a chunk timeout. */
  private static void recordTheEndOfTheResponses(
      SinkPayloadSubscriber subscriber, List<String> events) {
    Flux.never()
        .transform(subscriber.dropResponsesAfterChunkTimeout())
        .doOnComplete(() -> events.add("responses ended"))
        .subscribe();
  }

  /** Queues a first response the way the responder does, which starts the sink. */
  private static void sendFirstResponse(SinkPayloadSubscriber subscriber) {
    subscriber
        .startSinkAfterFirstResponse(Flux.just(payload()).concatWith(Flux.never()))
        .subscribe(ReferenceCountUtil::release);
  }

  private static Payload payload() {
    return ByteBufPayload.create(ByteBufAllocator.DEFAULT.buffer().writeByte(1));
  }

  /** Moves the clock past the 1-second test timeout, then fires the armed timer. */
  private void expirePastDelay() {
    clock.addAndGet(TimeUnit.SECONDS.toNanos(2));
    timer.expire();
  }

  /**
   * Moves the clock past the 1-second test timeout, then fires every timer the test armed: with two
   * readers only the live watchdog's timer still runs, and a stopped one skips its fire.
   */
  private void expireAllPastDelay() {
    clock.addAndGet(TimeUnit.SECONDS.toNanos(2));
    timer.timeouts.forEach(ManualTimer.ManualTimeout::expire);
  }

  /** A reader that never requests, so the window holds the sink payloads it prefetched. */
  static final class IdleReader extends BaseSubscriber<Payload> {
    private final CompletableFuture<Throwable> error = new CompletableFuture<>();

    @Override
    protected void hookOnSubscribe(Subscription subscription) {}

    @Override
    protected void hookOnError(Throwable t) {
      error.complete(t);
    }

    Throwable awaitError() throws InterruptedException {
      try {
        return error.get(5, TimeUnit.SECONDS);
      } catch (ExecutionException | TimeoutException e) {
        throw new AssertionError(e);
      }
    }
  }

  /**
   * A sink payload that a test can wait on until another thread releases it: the window may discard
   * its queue on the off-loop scheduler, after the cancel returns.
   */
  static final class CountedPayload extends AbstractReferenceCounted implements Payload {
    private final ByteBuf data = Unpooled.wrappedBuffer(new byte[] {1});
    private final CountDownLatch released = new CountDownLatch(1);
    private final AtomicInteger releases = new AtomicInteger();

    boolean awaitRelease(long timeout, TimeUnit unit) throws InterruptedException {
      return released.await(timeout, unit);
    }

    int releases() {
      return releases.get();
    }

    @Override
    public boolean release() {
      boolean freed = super.release();
      releases.incrementAndGet();
      released.countDown();

      return freed;
    }

    @Override
    public boolean hasMetadata() {
      return false;
    }

    @Override
    public ByteBuf sliceMetadata() {
      return Unpooled.EMPTY_BUFFER;
    }

    @Override
    public ByteBuf sliceData() {
      return data.slice();
    }

    @Override
    public ByteBuf data() {
      return data;
    }

    @Override
    public ByteBuf metadata() {
      return Unpooled.EMPTY_BUFFER;
    }

    @Override
    public CountedPayload retain() {
      super.retain();
      return this;
    }

    @Override
    public CountedPayload retain(int increment) {
      super.retain(increment);
      return this;
    }

    @Override
    public CountedPayload touch() {
      return this;
    }

    @Override
    public CountedPayload touch(Object hint) {
      return this;
    }

    @Override
    protected void deallocate() {
      data.release();
    }
  }

  /** A peer that sends when the test says, and records the credits and the cancel it gets. */
  private static final class Peer {
    final List<Long> credits = new CopyOnWriteArrayList<>();
    final AtomicBoolean cancelled = new AtomicBoolean();
    private FluxSink<Payload> frames;

    Flux<Payload> flux() {
      return Flux.<Payload>create(sink -> frames = sink, FluxSink.OverflowStrategy.IGNORE)
          .doOnRequest(credits::add)
          .doOnCancel(() -> cancelled.set(true));
    }

    void send(Payload payload) {
      frames.next(payload);
    }

    void complete() {
      frames.complete();
    }

    void fail(Throwable t) {
      frames.error(t);
    }
  }
}
