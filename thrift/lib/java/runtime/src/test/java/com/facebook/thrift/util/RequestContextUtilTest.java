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
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class RequestContextUtilTest {

  @Test
  public void testGetDurationFromString_ValidTime() {
    String time = "1000";
    Duration expectedResult = Duration.ofMillis(1000);
    Duration actualResult = RequestContextUtil.getDurationFromString(time);
    Assertions.assertEquals(expectedResult, actualResult);
  }

  @Test
  public void testGetDurationFromString_InvalidTime() {
    String time = "1sddsds";
    Duration expectedResult = null;
    Duration actualResult = RequestContextUtil.getDurationFromString(time);
    Assertions.assertEquals(expectedResult, actualResult);
  }

  @Test
  public void testGetDurationFromString_NullTime() {
    String time = null;
    Duration expectedResult = null;
    Duration actualResult = RequestContextUtil.getDurationFromString(time);
    Assertions.assertEquals(expectedResult, actualResult);
  }

  @Test
  public void testGetDurationFromString_EmptyTime() {
    String time = "";
    Duration expectedResult = null;
    Duration actualResult = RequestContextUtil.getDurationFromString(time);
    Assertions.assertEquals(expectedResult, actualResult);
  }

  /** The RPC is made after a hop, as it is when acquire() completes on a Netty thread. */
  @Test
  public void withCallerContextCarriesTheCallersRequestAcrossAHop() {
    RequestContext caller = new NettyNiftyRequestContext(new HashMap<>(), null);

    RequestContext seenAfterHop =
        RequestContextScope.get(
            caller,
            () ->
                RequestContextUtil.withCallerContext(
                        Mono.just(0)
                            .publishOn(Schedulers.single())
                            .flatMap(__ -> RequestContexts.currentContext()))
                    .block());

    Assertions.assertSame(caller, seenAfterHop);
  }

  @Test
  public void withCallerContextLeavesAnExistingReactorContextAlone() {
    RequestContext caller = new NettyNiftyRequestContext(new HashMap<>(), null);
    RequestContext composed = new NettyNiftyRequestContext(new HashMap<>(), null);

    RequestContext seen =
        RequestContextScope.get(
            caller,
            () ->
                RequestContextUtil.withCallerContext(RequestContexts.currentContext())
                    .contextWrite(context -> RequestContext.toContext(context, composed))
                    .block());

    Assertions.assertSame(composed, seen);
  }

  @Test
  public void withCallerContextAddsNothingWithoutABoundRequest() {
    Assertions.assertFalse(
        RequestContextUtil.withCallerContext(
                Mono.deferContextual(
                    view -> Mono.just(view.hasKey(RequestContext.REQUEST_CONTEXT_KEY))))
            .block());
  }

  /** A retry resubscribes from the thread that signalled the error, where nothing is bound. */
  @Test
  public void withCallerContextKeepsTheCallersRequestOnARetryFromAnotherThread() {
    RequestContext caller = new NettyNiftyRequestContext(new HashMap<>(), null);
    AtomicInteger attempts = new AtomicInteger();
    List<RequestContext> seenPerAttempt = new CopyOnWriteArrayList<>();

    RequestContextScope.run(
        caller,
        () ->
            RequestContextUtil.withCallerContext(
                    Mono.deferContextual(
                        view -> {
                          seenPerAttempt.add(
                              view.getOrDefault(RequestContext.REQUEST_CONTEXT_KEY, null));
                          return attempts.incrementAndGet() == 1
                              ? Mono.<Integer>error(new IllegalStateException("first attempt"))
                                  .publishOn(Schedulers.single())
                              : Mono.just(2);
                        }))
                .retry(1)
                .block());

    Assertions.assertEquals(2, seenPerAttempt.size());
    Assertions.assertSame(caller, seenPerAttempt.get(0));
    Assertions.assertSame(caller, seenPerAttempt.get(1));
  }

  @Test
  public void withCallerContextKeepsTheCallersRequestAcrossSubscribeOn() {
    RequestContext caller = new NettyNiftyRequestContext(new HashMap<>(), null);

    RequestContext seen =
        RequestContextScope.get(
            caller,
            () ->
                RequestContextUtil.withCallerContext(RequestContexts.currentContext())
                    .subscribeOn(Schedulers.single())
                    .block());

    Assertions.assertSame(caller, seen);
  }

  @Test
  public void withCallerContextRejectsANullCallAtTheCallSite() {
    Assertions.assertThrows(
        NullPointerException.class, () -> RequestContextUtil.withCallerContext(null));
  }
}
