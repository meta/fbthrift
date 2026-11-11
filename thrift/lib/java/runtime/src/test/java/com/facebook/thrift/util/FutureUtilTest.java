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

import com.facebook.nifty.core.RequestContext;
import com.facebook.nifty.core.RequestContextScope;
import com.facebook.nifty.core.RequestContexts;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

public class FutureUtilTest {
  @Test
  public void testToMono() {
    String s = String.valueOf(System.nanoTime());
    RequestContext mock = Mockito.mock(RequestContext.class);
    Mockito.when(mock.getRequestHeader()).thenReturn(ImmutableMap.of("message", s));
    Mono<String> stringMono =
        FutureUtil.toMono(this::string)
            .contextWrite(context -> RequestContext.toContext(context, mock));

    StepVerifier.create(stringMono).expectNext(s).verifyComplete();
  }

  /** The supplier runs in a scope, so the subscribing thread is left as it was found. */
  @Test
  public void testToMonoLeavesNoContextOnTheSubscribingThread() {
    RequestContext mock = Mockito.mock(RequestContext.class);
    Mockito.when(mock.getRequestHeader()).thenReturn(ImmutableMap.of("message", "hi"));

    StepVerifier.create(
            FutureUtil.toMono(this::string)
                .contextWrite(context -> RequestContext.toContext(context, mock)))
        .expectNext("hi")
        .verifyComplete();

    Assertions.assertNull(RequestContexts.getCurrentContext());
  }

  /** Without a request in the Reactor context, the scope on the subscribing thread is used. */
  @Test
  public void testToMonoFallsBackToTheSubscribingThreadsScope() {
    RequestContext outer = Mockito.mock(RequestContext.class);

    RequestContext seenBySupplier =
        RequestContextScope.get(
            outer,
            () ->
                FutureUtil.toMono(
                        () ->
                            MoreExecutors.newDirectExecutorService()
                                .submit(RequestContexts::getCurrentContext))
                    .block());

    Assertions.assertSame(outer, seenBySupplier);
  }

  /** The supplier is scoped; the thread that completes the future is not, and stays clean. */
  @Test
  public void testToMonoScopesTheSupplierButNotTheCompletingThread() throws Exception {
    ExecutorService completer = Executors.newSingleThreadExecutor();
    try {
      RequestContext requestContext = Mockito.mock(RequestContext.class);
      AtomicReference<RequestContext> inSupplier = new AtomicReference<>();

      Boolean completerSawARequest =
          FutureUtil.toMono(
                  () -> {
                    inSupplier.set(RequestContexts.getCurrentContext());
                    return MoreExecutors.listeningDecorator(completer)
                        .submit(() -> RequestContexts.getCurrentContext() != null);
                  })
              .contextWrite(context -> RequestContext.toContext(context, requestContext))
              .block();

      Assertions.assertSame(requestContext, inSupplier.get());
      Assertions.assertFalse(completerSawARequest);
      Assertions.assertNull(completer.submit(RequestContexts::getCurrentContext).get());
    } finally {
      completer.shutdownNow();
    }
  }

  private ListenableFuture<String> string() {
    ListenableFuture<String> message =
        MoreExecutors.newDirectExecutorService()
            .submit(() -> RequestContexts.getCurrentContext().getRequestHeader().get("message"));
    return message;
  }

  @Test
  public void testShouldScheduleWorkOnDifferentThreadWithSynchronousFuture() {
    Scheduler myThread =
        Schedulers.fromExecutor(
            Executors.newSingleThreadExecutor(
                r -> {
                  Thread t = new Thread(r);
                  t.setDaemon(true);
                  t.setName("MyThread");
                  return t;
                }));

    Mono<String> stringMono = FutureUtil.toScheduledMono(this::hi, myThread);

    StepVerifier.create(stringMono)
        .assertNext(
            s -> {
              Thread thread = Thread.currentThread();
              Assertions.assertTrue(thread.getName().contains("MyThread"));
            })
        .verifyComplete();
  }

  private ListenableFuture<String> hi() {
    SettableFuture<String> future = SettableFuture.create();
    future.set("Hi");
    return future;
  }

  @Test
  public void testMonoToFutureUtil() throws Exception {
    Mono<Long> count = Flux.range(1, 100).count();

    ListenableFuture<Long> future = FutureUtil.toListenableFuture(count);
    Long aLong = future.get();
    Assertions.assertEquals(100L, aLong.longValue());
  }

  @Test
  public void testMonoEmpty() throws Exception {
    ListenableFuture future = FutureUtil.toListenableFuture(Mono.empty());
    Object o = future.get();
    Assertions.assertNull(o);
  }

  @Test
  public void testMonoThatDoesNotEmit() throws Exception {
    Mono m =
        new Mono() {
          @Override
          public void subscribe(CoreSubscriber actual) {
            actual.onSubscribe(Operators.emptySubscription());
            actual.onComplete();
          }
        };

    ListenableFuture future = FutureUtil.toListenableFuture(m);
    Object o = future.get();
    Assertions.assertNull(o);
  }

  @Test
  public void testWithScalarSubscription() throws Exception {
    Mono m =
        new Mono() {
          @Override
          public void subscribe(CoreSubscriber actual) {
            Subscription subscription = Operators.scalarSubscription(actual, 1);
            actual.onSubscribe(subscription);
          }
        };

    ListenableFuture future = FutureUtil.toListenableFuture(m);
    int i = (int) future.get();
    Assertions.assertEquals(1, i);
  }

  @Test
  public void testWithScalarMono() throws Exception {
    ListenableFuture future = FutureUtil.toListenableFuture(Mono.just(1));
    int i = (int) future.get();
    Assertions.assertEquals(1, i);
  }

  @Test
  public void testWithScalarMonoThatEmitsException() throws Exception {
    Assertions.assertThrows(
        ExecutionException.class,
        () -> {
          ListenableFuture future =
              FutureUtil.toListenableFuture(Mono.error(new RuntimeException()));
          future.get();
        });
  }

  @Test
  public void testFutureCancellationCancelsUpstreamSubscription() throws Exception {
    AtomicBoolean upstreamCancelled = new AtomicBoolean(false);
    AtomicBoolean finallyInvoked = new AtomicBoolean(false);
    CountDownLatch subscribed = new CountDownLatch(1);

    Mono<String> mono =
        Mono.<String>never()
            .doOnSubscribe(s -> subscribed.countDown())
            .doOnCancel(() -> upstreamCancelled.set(true))
            .doFinally(
                signal -> {
                  if (signal == SignalType.CANCEL) {
                    finallyInvoked.set(true);
                  }
                });

    ListenableFuture<String> future = FutureUtil.toListenableFuture(mono);

    // Wait for subscription to be established
    Assertions.assertTrue(
        subscribed.await(1, TimeUnit.SECONDS), "Subscription should be established");

    // Cancel the future
    future.cancel(true);

    // Give time for cancellation to propagate
    Thread.sleep(50);

    // Verify upstream was cancelled
    Assertions.assertTrue(upstreamCancelled.get(), "Upstream subscription should be cancelled");
    Assertions.assertTrue(finallyInvoked.get(), "doFinally should be invoked with CANCEL");
    Assertions.assertTrue(future.isCancelled(), "Future should report cancelled");
  }

  @Test
  public void testDoubleCancellationIsIdempotent() throws Exception {
    AtomicInteger cancelCount = new AtomicInteger(0);
    CountDownLatch subscribed = new CountDownLatch(1);

    Mono<String> mono =
        Mono.<String>never()
            .doOnSubscribe(s -> subscribed.countDown())
            .doOnCancel(cancelCount::incrementAndGet);

    ListenableFuture<String> future = FutureUtil.toListenableFuture(mono);

    // Wait for subscription
    Assertions.assertTrue(subscribed.await(1, TimeUnit.SECONDS));

    // Cancel multiple times
    future.cancel(true);
    future.cancel(true);
    future.cancel(true);

    // Give time for cancellation to propagate
    Thread.sleep(50);

    // Should only cancel upstream once
    Assertions.assertEquals(1, cancelCount.get(), "Should only cancel once");
  }

  @Test
  public void testConcurrentCancellationAndCompletion() throws Exception {
    int iterations = 100;
    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger cancelledCount = new AtomicInteger(0);
    AtomicInteger errorCount = new AtomicInteger(0);

    ExecutorService executor = Executors.newFixedThreadPool(2);

    try {
      for (int i = 0; i < iterations; i++) {
        Sinks.One<String> sink = Sinks.one();
        ListenableFuture<String> future = FutureUtil.toListenableFuture(sink.asMono());
        CountDownLatch latch = new CountDownLatch(2);

        // Thread 1: Cancel the future
        executor.submit(
            () -> {
              try {
                future.cancel(true);
              } finally {
                latch.countDown();
              }
            });

        // Thread 2: Complete the mono
        executor.submit(
            () -> {
              try {
                sink.tryEmitValue("value");
              } finally {
                latch.countDown();
              }
            });

        latch.await(1, TimeUnit.SECONDS);

        if (future.isCancelled()) {
          cancelledCount.incrementAndGet();
        } else {
          try {
            future.get(100, TimeUnit.MILLISECONDS);
            successCount.incrementAndGet();
          } catch (Exception e) {
            errorCount.incrementAndGet();
          }
        }
      }
    } finally {
      executor.shutdown();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }

    // Either cancelled or completed successfully - no exceptions
    Assertions.assertEquals(0, errorCount.get(), "Should have no errors");
    Assertions.assertEquals(
        iterations, successCount.get() + cancelledCount.get(), "All iterations accounted for");
  }
}
