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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.facebook.thrift.protocol.ProtocolUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.RSocketErrorException;
import io.rsocket.frame.ErrorFrameCodec;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.thrift.RpcKind;
import org.apache.thrift.StreamRpcError;
import org.apache.thrift.StreamRpcErrorCode;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

public class ChunkTimeoutTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(1);

  private final ManualTimer timer = new ManualTimer();
  private final AtomicLong clock = new AtomicLong();

  @Test
  public void backstopFiresTheErrorWhileTheReaderIsBlockedInUserCode() {
    Scheduler offLoop = Schedulers.newParallel("chunk-timeout-backstop", 2);
    ChunkTimeout timeout = new ChunkTimeout(() -> timer, offLoop, clock::get);
    CountDownLatch enteredReader = new CountDownLatch(1);
    CountDownLatch releaseReader = new CountDownLatch(1);
    CountDownLatch responsesEnded = new CountDownLatch(1);

    try {
      responses(timeout, Flux.never(), RpcKind.SINK)
          .subscribe(response -> {}, e -> responsesEnded.countDown());

      timeout.watch(
          TIMEOUT,
          () -> {},
          () -> {
            enteredReader.countDown();
            await(releaseReader);
          });
      timeout.creditGranted(1);

      expirePastDelay();

      // The reader stays parked in user code and never ends, so only the backstop can fire.
      await(enteredReader);
      await(responsesEnded);
      assertTrue(timeout.hasTimedOut());
    } finally {
      releaseReader.countDown();
      offLoop.dispose();
    }
  }

  @Test
  public void readerTerminalFiresTheErrorBeforeTheBackstop() {
    DelayHoldingScheduler scheduler = new DelayHoldingScheduler();
    ChunkTimeout timeout = new ChunkTimeout(() -> timer, scheduler, clock::get);
    AtomicInteger signals = new AtomicInteger();

    responses(timeout, Flux.never(), RpcKind.SINK)
        .subscribe(response -> {}, e -> signals.incrementAndGet());

    timeout.watch(TIMEOUT, () -> {}, () -> {});
    timeout.creditGranted(1);

    expirePastDelay();

    // The expiry only fails the reader and queues the backstop: the responses wait for one of them.
    assertEquals(0, signals.get());
    assertEquals(1, scheduler.delayed.size());
    assertTrue(timeout.hasTimedOut());

    timeout.readerEnded();

    assertEquals(1, signals.get());

    scheduler.runDelayed();

    assertEquals(1, signals.get());
  }

  @Test
  public void rejectedBackstopScheduleStillFailsTheReaderAndSignals() {
    ChunkTimeout timeout = new ChunkTimeout(() -> timer, new DelayRejectingScheduler(), clock::get);
    List<String> events = new CopyOnWriteArrayList<>();

    responses(timeout, Flux.never(), RpcKind.SINK)
        .subscribe(response -> {}, e -> events.add("responses ended"));

    timeout.watch(TIMEOUT, () -> {}, () -> events.add("reader failed"));
    timeout.creditGranted(1);

    expirePastDelay();

    assertEquals(Arrays.asList("reader failed", "responses ended"), events);
    assertTrue(timeout.hasTimedOut());
  }

  @Test
  public void aResponseAfterTheTimeoutIsDroppedAndTheResponsesEndWithTheError() {
    ChunkTimeout timeout = new ChunkTimeout(() -> timer, new DelayHoldingScheduler(), clock::get);
    Sinks.Many<String> handler = Sinks.many().unicast().onBackpressureBuffer();

    timeout.watch(TIMEOUT, () -> {}, () -> {});
    timeout.creditGranted(1);

    StepVerifier.create(responses(timeout, handler.asFlux(), RpcKind.SINK))
        .then(() -> handler.tryEmitNext("first"))
        .expectNext("first")
        .then(this::expirePastDelay)
        .then(() -> handler.tryEmitNext("final"))
        .then(timeout::readerEnded)
        .verifyErrorSatisfies(e -> assertChunkTimeout(ErrorFrameCodec.APPLICATION_ERROR, e));
  }

  @Test
  public void aFailureEscapingAfterTheTimeoutEndsWithTheTimeoutError() {
    ChunkTimeout timeout = new ChunkTimeout(() -> timer, new DelayHoldingScheduler(), clock::get);
    Sinks.Many<String> handler = Sinks.many().unicast().onBackpressureBuffer();
    IllegalStateException escaped = new IllegalStateException("reader callback failed");

    timeout.watch(TIMEOUT, () -> {}, () -> {});
    timeout.creditGranted(1);

    StepVerifier.create(responses(timeout, handler.asFlux(), RpcKind.SINK))
        .then(this::expirePastDelay)
        .then(() -> handler.tryEmitError(escaped))
        .verifyErrorSatisfies(
            e -> {
              assertChunkTimeout(ErrorFrameCodec.APPLICATION_ERROR, e);
              assertSame(escaped, e.getSuppressed()[0]);
            });
  }

  @Test
  public void aBidiTimeoutEndsWithACanceledError() {
    ChunkTimeout timeout = new ChunkTimeout(() -> timer, new DelayHoldingScheduler(), clock::get);

    timeout.watch(TIMEOUT, () -> {}, () -> {});
    timeout.creditGranted(1);

    StepVerifier.create(responses(timeout, Flux.never(), RpcKind.BIDIRECTIONAL_STREAM))
        .then(this::expirePastDelay)
        .then(timeout::readerEnded)
        .verifyErrorSatisfies(e -> assertChunkTimeout(ErrorFrameCodec.CANCELED, e));
  }

  @Test
  public void theSinkHalfCancelsBeforeTheTimeoutCommits() {
    ChunkTimeout timeout = new ChunkTimeout(() -> timer, new DelayHoldingScheduler(), clock::get);
    Sinks.Many<String> handler = Sinks.many().unicast().onBackpressureBuffer();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    AtomicBoolean timedOutAtCancel = new AtomicBoolean(true);

    timeout.watch(TIMEOUT, () -> timedOutAtCancel.set(timeout.hasTimedOut()), () -> {});
    timeout.creditGranted(1);

    responses(timeout, handler.asFlux(), RpcKind.BIDIRECTIONAL_STREAM)
        .subscribe(first -> {}, failure::set);

    expirePastDelay();

    assertFalse(timedOutAtCancel.get());

    timeout.readerEnded();

    assertChunkTimeout(ErrorFrameCodec.CANCELED, failure.get());
  }

  @Test
  public void responsesWithoutATimeoutPassUnchanged() {
    ChunkTimeout timeout = new ChunkTimeout(() -> timer, new DelayHoldingScheduler(), clock::get);
    IllegalStateException handlerError = new IllegalStateException("handler failed");

    StepVerifier.create(
            responses(
                timeout, Flux.just("first").concatWith(Flux.error(handlerError)), RpcKind.SINK))
        .expectNext("first")
        .verifyErrorSatisfies(e -> assertSame(handlerError, e));

    assertFalse(timeout.hasTimedOut());
    assertEquals(0, timer.created);
  }

  /** Runs handler responses through the response-side transformers in the transport's order. */
  private static <T> Flux<T> responses(ChunkTimeout timeout, Publisher<T> handler, RpcKind kind) {
    return Flux.from(handler)
        .transform(timeout.dropResponsesAfterTimeout())
        .transform(timeout.mapFailuresAfterTimeout(kind))
        .transform(timeout.endWithTimeoutError(kind));
  }

  /** Asserts an ERROR frame with the given code that carries a CHUNK_TIMEOUT stream error. */
  private static void assertChunkTimeout(int errorCode, Throwable e) {
    assertTrue(e instanceof RSocketErrorException, "error " + e);
    assertEquals(errorCode, ((RSocketErrorException) e).errorCode());

    StreamRpcError error = decodeStreamError(e.getMessage());

    assertEquals(StreamRpcErrorCode.CHUNK_TIMEOUT, error.getCode());
    assertEquals("Sink chunk timeout", error.getWhatUtf8());
  }

  private static StreamRpcError decodeStreamError(String body) {
    ByteBuf buffer = Unpooled.wrappedBuffer(body.getBytes(StandardCharsets.UTF_8));

    try {
      return ProtocolUtil.readCompact(StreamRpcError::read0, buffer);
    } finally {
      buffer.release();
    }
  }

  /** Moves the clock past the 1-second test timeout, then fires the armed timer. */
  private void expirePastDelay() {
    clock.addAndGet(TimeUnit.SECONDS.toNanos(2));
    timer.expire();
  }

  private static void await(CountDownLatch latch) {
    try {
      assertTrue(latch.await(5, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();

      throw new AssertionError(e);
    }
  }

  /** A scheduler that runs immediate tasks inline and rejects delayed ones. */
  private static final class DelayRejectingScheduler implements Scheduler {
    @Override
    public Disposable schedule(Runnable task) {
      task.run();

      return Disposables.disposed();
    }

    @Override
    public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
      throw new RejectedExecutionException("saturated");
    }

    @Override
    public Scheduler.Worker createWorker() {
      throw new UnsupportedOperationException();
    }
  }
}
