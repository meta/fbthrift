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
import com.facebook.nifty.core.RequestContexts;
import com.google.common.base.Strings;
import java.time.Duration;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public final class RequestContextUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(RequestContextUtil.class);

  public static Duration getDurationFromString(String time) {
    if (!Strings.isNullOrEmpty(time)) {
      try {
        long clientTimeoutMs = Long.parseLong(time);
        return Duration.ofMillis(clientTimeoutMs);
      } catch (NumberFormatException e) {
        LOGGER.warn("Value '{}' is not a valid number", time);
      }
    }
    return null;
  }

  /**
   * Carries the context bound on the subscribing thread into {@code call}'s Reactor context.
   *
   * <p>A generated client makes the RPC inside {@code acquire().flatMap(...)}. On a cold or
   * reconnecting connection {@code acquire()} completes on a Netty thread, so the client event
   * handlers would look for a scope on that thread and find none. A blocking or future-based caller
   * subscribes on its own thread, so capturing there keeps its request. The context bound when the
   * call is made is the fallback, for a resubscription that runs on another thread. A reactive
   * caller already carries the request in its Reactor context, which is left as it is.
   */
  public static <T> Mono<T> withCallerContext(Mono<T> call) {
    Objects.requireNonNull(call, "call");
    // Also read when the call is made: a retry or a subscribeOn resubscribes from a thread with no
    // scope, where the subscribe-time read alone would drop the caller's request.
    RequestContext atCall = RequestContexts.tryGetCurrentContext();
    return Mono.defer(
        () -> {
          RequestContext atSubscribe = RequestContexts.tryGetCurrentContext();
          RequestContext caller = atSubscribe != null ? atSubscribe : atCall;
          if (caller == null) {
            return call;
          }
          return call.contextWrite(
              context ->
                  context.hasKey(RequestContext.REQUEST_CONTEXT_KEY)
                      ? context
                      : RequestContext.toContext(context, caller));
        });
  }
}
