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

package com.facebook.thrift.util.resources;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.facebook.nifty.core.RequestContext;
import com.facebook.nifty.core.RequestContextScope;
import com.facebook.nifty.core.RequestContexts;
import com.facebook.thrift.util.NettyNiftyRequestContext;
import java.util.HashMap;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * The Thrift schedulers carry no request context. The framework binds it inside the dispatched
 * task, and code that needs it on another thread asks for it explicitly, so a hop through a
 * scheduler neither delivers a context nor leaves one behind.
 */
public class SchedulerContextTest {
  private static final String TEST_KEY = "scheduler-context-test.key";

  private final RequestContext request = new NettyNiftyRequestContext(new HashMap<>(), null);

  /**
   * Registering a key used to install the two global hooks and turn copying on. The shim must do
   * neither, so the hops below carry nothing even after a registration.
   */
  @BeforeEach
  @SuppressWarnings("deprecation")
  public void registerAKeyAsOldCallersDid() {
    request.setContextData(TEST_KEY, "value");
    ContextPropagationRegistry.registerContextPropagationKey(TEST_KEY);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void theRegistryShimRegistersNothing() {
    assertFalse(ContextPropagationRegistry.isContextPropEnabled());
    assertTrue(ContextPropagationRegistry.getContextPropagationKeys().isEmpty());
  }

  @Test
  public void offLoopSchedulerCarriesNoThreadContext() {
    assertCarriesNoThreadContext(RpcResources.getOffLoopScheduler());
  }

  @Test
  public void clientOffLoopSchedulerCarriesNoThreadContext() {
    assertCarriesNoThreadContext(RpcResources.getClientOffLoopScheduler());
  }

  /** The Reactor context is what crosses a hop; the schedulers must not disturb it. */
  @Test
  public void theReactorContextSurvivesHopsOnBothSchedulers() {
    for (Scheduler scheduler :
        new Scheduler[] {
          RpcResources.getOffLoopScheduler(), RpcResources.getClientOffLoopScheduler()
        }) {
      RequestContext resolved =
          Mono.just(0)
              .publishOn(scheduler)
              .then(RequestContexts.currentContext())
              .subscribeOn(scheduler)
              .contextWrite(context -> RequestContext.toContext(context, request))
              .block();
      assertSame(request, resolved);
    }
  }

  /** One worker thread, so the probe runs on the same thread the scoped submission used. */
  @Test
  public void aScopedSubmissionLeavesNothingOnItsWorker() {
    Scheduler worker = Schedulers.newSingle("scheduler-context-test");
    try {
      RequestContextScope.run(request, () -> staticRead().subscribeOn(worker).block());

      assertEquals(Optional.empty(), staticRead().subscribeOn(worker).block());
    } finally {
      worker.dispose();
    }
  }

  private void assertCarriesNoThreadContext(Scheduler scheduler) {
    Optional<RequestContext> viaSubscribeOn =
        RequestContextScope.get(request, () -> staticRead().subscribeOn(scheduler).block());
    Optional<RequestContext> viaPublishOn =
        RequestContextScope.get(
            request, () -> Mono.just(0).publishOn(scheduler).then(staticRead()).block());

    assertEquals(Optional.empty(), viaSubscribeOn);
    assertEquals(Optional.empty(), viaPublishOn);
  }

  private static Mono<Optional<RequestContext>> staticRead() {
    return Mono.fromCallable(() -> Optional.ofNullable(RequestContexts.getCurrentContext()));
  }
}
