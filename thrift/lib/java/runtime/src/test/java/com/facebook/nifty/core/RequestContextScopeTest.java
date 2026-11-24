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

package com.facebook.nifty.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.facebook.thrift.util.NettyNiftyRequestContext;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

public class RequestContextScopeTest {
  private final RequestContext requestA = newContext();
  private final RequestContext requestB = newContext();

  private ExecutorService worker;

  @BeforeEach
  public void setUp() {
    worker = Executors.newSingleThreadExecutor();
  }

  @AfterEach
  public void tearDown() throws Exception {
    worker.shutdownNow();
    worker.awaitTermination(10, TimeUnit.SECONDS);
    // The override slot is a real thread local; a failed assertion must not leak it into the
    // next test on this thread.
    RequestContexts.overrideBinding().remove();
  }

  @Test
  public void nothingIsBoundOutsideAScope() {
    assertNull(RequestContextScope.boundContext());
    assertNull(RequestContexts.getCurrentContext());
    assertNull(RequestContexts.tryGetCurrentContext());
  }

  @Test
  public void runBindsForItsExtentAndUnwinds() {
    AtomicReference<RequestContext> seen = new AtomicReference<>();
    RequestContextScope.run(requestA, () -> seen.set(RequestContexts.getCurrentContext()));

    assertSame(requestA, seen.get());
    assertNull(RequestContexts.getCurrentContext());
  }

  @Test
  public void unwindsWhenTheBodyThrows() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            RequestContextScope.run(
                requestA,
                () -> {
                  throw new IllegalArgumentException("boom");
                }));

    assertNull(RequestContexts.getCurrentContext());
  }

  @Test
  public void callAndGetReturnTheBodyResultAndUnwind() throws Exception {
    assertSame(requestA, RequestContextScope.call(requestA, RequestContexts::getCurrentContext));
    assertSame(requestB, RequestContextScope.get(requestB, RequestContexts::getCurrentContext));
    assertNull(RequestContexts.getCurrentContext());
  }

  @Test
  public void nestedScopeRestoresTheOuterBinding() {
    AtomicReference<RequestContext> inner = new AtomicReference<>();
    AtomicReference<RequestContext> afterInner = new AtomicReference<>();
    RequestContextScope.run(
        requestA,
        () -> {
          RequestContextScope.run(requestB, () -> inner.set(RequestContexts.getCurrentContext()));
          afterInner.set(RequestContexts.getCurrentContext());
        });

    assertSame(requestB, inner.get());
    assertSame(requestA, afterInner.get());
  }

  @Test
  public void nullBindsAbsenceRatherThanFallingThrough() {
    AtomicReference<RequestContext> inner = new AtomicReference<>(requestA);
    RequestContextScope.run(
        requestA,
        () -> RequestContextScope.run(null, () -> inner.set(RequestContexts.getCurrentContext())));

    assertNull(inner.get());
  }

  @Test
  @SuppressWarnings("deprecation")
  public void setterInsideAScopeWinsUntilTheScopeExits() {
    AtomicReference<RequestContext> afterSet = new AtomicReference<>();
    RequestContextScope.run(
        requestA,
        () -> {
          RequestContexts.setCurrentContext(requestB);
          afterSet.set(RequestContexts.getCurrentContext());
        });

    assertSame(requestB, afterSet.get());
    assertNull(RequestContexts.getCurrentContext());
  }

  @Test
  public void clearInsideAScopeMasksTheScopeUntilItExits() {
    AtomicReference<RequestContext> afterClear = new AtomicReference<>(requestA);
    RequestContextScope.run(
        requestA,
        () -> {
          RequestContexts.clearCurrentContext();
          afterClear.set(RequestContexts.getCurrentContext());
        });

    assertNull(afterClear.get());
    assertNull(RequestContexts.getCurrentContext());
  }

  @Test
  @SuppressWarnings("deprecation")
  public void scopeHidesAnOuterSetterValueAndPutsItBack() {
    RequestContexts.setCurrentContext(requestB);
    AtomicReference<RequestContext> inside = new AtomicReference<>();
    RequestContextScope.run(requestA, () -> inside.set(RequestContexts.getCurrentContext()));

    assertSame(requestA, inside.get());
    assertSame(requestB, RequestContexts.getCurrentContext());
  }

  @Test
  public void staticReadsRejectTheAssemblySentinel() {
    RequestContextScope.run(
        RequestContextScope.ASSEMBLY_SENTINEL,
        () -> {
          IllegalStateException thrown =
              assertThrows(IllegalStateException.class, RequestContexts::getCurrentContext);
          assertTrue(thrown.getMessage().contains("RequestContexts.currentContext()"));
          assertThrows(IllegalStateException.class, RequestContexts::getOrCreateCurrentContext);
          assertNull(RequestContexts.tryGetCurrentContext());
        });
  }

  @Test
  public void currentContextPrefersTheReactorContextOverTheScope() {
    RequestContext resolved =
        RequestContextScope.get(
            requestB,
            () ->
                RequestContexts.currentContext()
                    .contextWrite(context -> RequestContext.toContext(context, requestA))
                    .block());

    assertSame(requestA, resolved);
  }

  @Test
  public void currentContextFallsBackToTheScope() {
    assertSame(
        requestA,
        RequestContextScope.get(requestA, () -> RequestContexts.currentContext().block()));
  }

  @Test
  public void currentContextErrorsWithNeitherCarrier() {
    assertThrows(IllegalStateException.class, () -> RequestContexts.currentContext().block());
    assertThrows(
        IllegalStateException.class,
        () ->
            RequestContextScope.run(
                RequestContextScope.ASSEMBLY_SENTINEL,
                () -> RequestContexts.currentContext().block()));
  }

  @Test
  public void currentContextResolvesAtSubscribeTimeNotAssemblyTime() {
    Mono<RequestContext> assembledWithA =
        RequestContextScope.get(requestA, RequestContexts::currentContext);

    assertSame(
        requestB,
        assembledWithA
            .contextWrite(Context.of(RequestContext.REQUEST_CONTEXT_KEY, requestB))
            .block());
  }

  @Test
  public void toContextWithNoRequestRemovesAnInheritedOne() {
    Context inherited = Context.of(RequestContext.REQUEST_CONTEXT_KEY, requestA);

    assertFalse(
        RequestContext.toContext(inherited, null).hasKey(RequestContext.REQUEST_CONTEXT_KEY));
    assertSame(
        requestB,
        RequestContext.toContext(inherited, requestB).get(RequestContext.REQUEST_CONTEXT_KEY));
  }

  @Test
  public void wrappedExecutorCarriesTheCallersContextAndLeavesTheWorkerClean() throws Exception {
    AtomicReference<RequestContext> inTask = new AtomicReference<>();
    RequestContextScope.run(
        requestA,
        () ->
            RequestContextScope.wrap(worker)
                .execute(() -> inTask.set(RequestContexts.getCurrentContext())));

    assertSame(requestA, worker.submit(inTask::get).get(10, TimeUnit.SECONDS));
    assertNull(worker.submit(RequestContexts::getCurrentContext).get(10, TimeUnit.SECONDS));
  }

  @Test
  public void wrappedExecutorDoesNotCarryTheSentinelToTheWorker() throws Exception {
    AtomicReference<RequestContext> inTask = new AtomicReference<>(requestA);
    RequestContextScope.run(
        RequestContextScope.ASSEMBLY_SENTINEL,
        () ->
            RequestContextScope.wrap(worker)
                .execute(() -> inTask.set(RequestContexts.getCurrentContext())));

    assertNull(worker.submit(inTask::get).get(10, TimeUnit.SECONDS));
  }

  @Test
  public void captureKeepsTheRegisteringContextWhenAnotherRequestRunsTheTask() throws Exception {
    Runnable[] registered = new Runnable[1];
    AtomicReference<RequestContext> inCallback = new AtomicReference<>();
    RequestContextScope.run(
        requestA,
        () ->
            registered[0] =
                RequestContextScope.capture(
                    () -> inCallback.set(RequestContexts.getCurrentContext())));

    // The completing thread is serving request B when it runs the callback.
    RequestContext afterCallback =
        worker
            .submit(
                () ->
                    RequestContextScope.get(
                        requestB,
                        () -> {
                          registered[0].run();
                          return RequestContexts.getCurrentContext();
                        }))
            .get(10, TimeUnit.SECONDS);

    assertSame(requestA, inCallback.get());
    assertSame(requestB, afterCallback);
  }

  @Test
  public void captureCallableWorksWithSubmit() throws Exception {
    Callable<RequestContext> task =
        RequestContextScope.get(
            requestA, () -> RequestContextScope.capture(RequestContexts::getCurrentContext));

    assertSame(requestA, worker.submit(task).get(10, TimeUnit.SECONDS));
    assertNull(worker.submit(RequestContexts::getCurrentContext).get(10, TimeUnit.SECONDS));
  }

  @Test
  public void currentContextFallsBackToTheScopeBoundAtSubscribeTime() {
    Mono<RequestContext> assembledUnderA =
        RequestContextScope.get(requestA, RequestContexts::currentContext);

    assertSame(requestB, RequestContextScope.get(requestB, assembledUnderA::block));
  }

  @Test
  public void currentContextIgnoresASentinelInTheReactorContext() {
    RequestContext resolved =
        RequestContextScope.get(
            requestA,
            () ->
                RequestContexts.currentContext()
                    .contextWrite(
                        context ->
                            RequestContext.toContext(
                                context, RequestContextScope.ASSEMBLY_SENTINEL))
                    .block());

    assertSame(requestA, resolved);
  }

  @Test
  public void getOrCreateInsideANullScopeMasksOnlyUntilThatScopeExits() {
    AtomicReference<RequestContext> afterInner = new AtomicReference<>();
    RequestContextScope.run(
        requestA,
        () -> {
          RequestContextScope.run(
              null,
              () -> {
                RequestContext created = RequestContexts.getOrCreateCurrentContext();
                assertSame(created, RequestContexts.getCurrentContext());
              });
          afterInner.set(RequestContexts.getCurrentContext());
        });

    assertSame(requestA, afterInner.get());
  }

  @Test
  @SuppressWarnings("deprecation")
  public void setterBetweenScopesIsVisibleAgainAfterTheInnerScopeExits() {
    AtomicReference<RequestContext> inInner = new AtomicReference<>();
    AtomicReference<RequestContext> afterInner = new AtomicReference<>();
    RequestContextScope.run(
        requestA,
        () -> {
          RequestContexts.setCurrentContext(requestB);
          RequestContextScope.run(requestA, () -> inInner.set(RequestContexts.getCurrentContext()));
          afterInner.set(RequestContexts.getCurrentContext());
        });

    assertSame(requestA, inInner.get());
    assertSame(requestB, afterInner.get());
  }

  /** {@code wrap} reads the context when {@code execute} is called, not when it wraps. */
  @Test
  public void wrappedExecutorReadsTheContextOfTheThreadThatCallsExecute() throws Exception {
    Executor wrappedWithNothingBound = RequestContextScope.wrap(worker);
    AtomicReference<RequestContext> inTask = new AtomicReference<>();
    RequestContextScope.run(
        requestB,
        () ->
            wrappedWithNothingBound.execute(() -> inTask.set(RequestContexts.getCurrentContext())));

    assertSame(requestB, worker.submit(inTask::get).get(10, TimeUnit.SECONDS));
  }

  @Test
  public void wrappedExecutorRejectsANullTaskAtTheCallSite() {
    assertThrows(NullPointerException.class, () -> RequestContextScope.wrap(worker).execute(null));
  }

  /** The completing thread runs the callback, while it serves request B. */
  @Test
  public void capturedFutureCallbackSeesTheRegisteringRequestOnSuccessAndFailure()
      throws Exception {
    SettableFuture<String> succeeding = SettableFuture.create();
    SettableFuture<String> failing = SettableFuture.create();
    AtomicReference<RequestContext> onSuccess = new AtomicReference<>();
    AtomicReference<RequestContext> onFailure = new AtomicReference<>();
    FutureCallback<String> recorder =
        new FutureCallback<String>() {
          @Override
          public void onSuccess(String result) {
            onSuccess.set(RequestContexts.getCurrentContext());
          }

          @Override
          public void onFailure(Throwable t) {
            onFailure.set(RequestContexts.getCurrentContext());
          }
        };
    RequestContextScope.run(
        requestA,
        () -> {
          Futures.addCallback(
              succeeding, RequestContextScope.capture(recorder), MoreExecutors.directExecutor());
          Futures.addCallback(
              failing, RequestContextScope.capture(recorder), MoreExecutors.directExecutor());
        });

    RequestContext afterCallbacks =
        worker
            .submit(
                () ->
                    RequestContextScope.get(
                        requestB,
                        () -> {
                          succeeding.set("done");
                          failing.setException(new IllegalStateException("failed"));
                          return RequestContexts.getCurrentContext();
                        }))
            .get(10, TimeUnit.SECONDS);

    assertSame(requestA, onSuccess.get());
    assertSame(requestA, onFailure.get());
    assertSame(requestB, afterCallbacks);
  }

  @Test
  public void java25LoadsTheScopedValueVariant() throws Exception {
    assumeTrue(javaFeatureVersion() >= 25);

    assertEquals(
        "java.lang.ScopedValue",
        RequestContextScope.class.getDeclaredField("CTX").getType().getName());
  }

  private static RequestContext newContext() {
    return new NettyNiftyRequestContext(new HashMap<>(), null);
  }

  /** {@code Runtime.version()} does not exist on Java 8, which this source set also compiles on. */
  private static int javaFeatureVersion() {
    String specification = System.getProperty("java.specification.version");
    return specification.startsWith("1.")
        ? Integer.parseInt(specification.substring(2))
        : Integer.parseInt(specification);
  }
}
