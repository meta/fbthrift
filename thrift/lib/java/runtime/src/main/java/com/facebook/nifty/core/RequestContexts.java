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

import com.facebook.thrift.util.NettyNiftyRequestContext;
import java.util.HashMap;
import java.util.Optional;
import reactor.core.publisher.Mono;

public class RequestContexts {
  /**
   * What the imperative setters on this class write. Held separately from the binding {@link
   * RequestContextScope} establishes so that a setter call inside a scope overrides it for the rest
   * of the extent, and so that the scope can put the slot back as it found it on the way out.
   *
   * <p>Holds {@link AssemblySentinelContext#CLEARED} rather than nothing after {@link
   * #clearCurrentContext()}: an absent entry means "this thread said nothing", which must keep
   * deferring to the enclosing scope, while a clear means "this thread said there is no context".
   */
  private static final ThreadLocal<RequestContext> threadLocalContext = new ThreadLocal<>();

  private static final String ASSEMBLY_TIME_READ =
      "RequestContexts.getCurrentContext() is not supported inside a Mono/Flux-returning handler. "
          + "The handler body runs at assembly time, so the read happens before the request is "
          + "subscribed. Use RequestContexts.currentContext() instead.";

  private static final String NO_CONTEXT_IN_SCOPE =
      "No Thrift request context is in scope. RequestContexts.currentContext() is only callable "
          + "from inside a Thrift request pipeline.";

  private RequestContexts() {}

  /** The slot {@link RequestContextScope} consults before its own binding, and saves across it. */
  static ThreadLocal<RequestContext> overrideBinding() {
    return threadLocalContext;
  }

  /** The value {@link #overrideBinding()} holds to mean "explicitly no context". */
  static RequestContext clearedMarker() {
    return AssemblySentinelContext.CLEARED;
  }

  /**
   * Gets the {@link NiftyRequestContext} bound by the innermost enclosing {@link
   * RequestContextScope} on the current thread.
   *
   * @return The {@link NiftyRequestContext} of the current request, or null if nothing is bound
   * @throws IllegalStateException if called from the body of a {@code Mono}- or {@code
   *     Flux}-returning handler, which runs at assembly time; use {@link #currentContext()} there
   */
  public static RequestContext getCurrentContext() {
    return rejectAssemblySentinel(RequestContextScope.boundContext());
  }

  /**
   * The {@link RequestContext} of the request being served, for a caller that treats absence as
   * ordinary rather than exceptional.
   *
   * <p>Unlike {@link #getCurrentContext()} this never throws, and in particular reads null rather
   * than failing inside the assembly-time body of a {@code Mono}- or {@code Flux}-returning
   * handler. Cross-cutting code that observes a request -- tracing, metrics, logging -- wants this,
   * because it must not fail the call it is observing.
   *
   * @return the current request's context, or null when there is none to read
   */
  public static RequestContext tryGetCurrentContext() {
    RequestContext bound = RequestContextScope.boundContext();
    return RequestContextScope.isAssemblySentinel(bound) ? null : bound;
  }

  /**
   * Gets the {@link NiftyRequestContext} bound by the innermost enclosing {@link
   * RequestContextScope} on the current thread. If it is not available, creates a new empty context
   * and returns it. This context will have empty headers map and null conntextion context.
   *
   * @return The {@link NiftyRequestContext} of the current request
   * @throws IllegalStateException if called from the body of a {@code Mono}- or {@code
   *     Flux}-returning handler, which runs at assembly time; use {@link #currentContext()} there
   */
  public static RequestContext getOrCreateCurrentContext() {
    RequestContext currentContext = rejectAssemblySentinel(RequestContextScope.boundContext());
    if (currentContext == null) {
      currentContext = new NettyNiftyRequestContext(new HashMap<>(), null);
      threadLocalContext.set(currentContext);
    }
    return currentContext;
  }

  /**
   * Gets the {@link RequestContext} of the request being served, resolved when the returned {@link
   * Mono} is subscribed to rather than when it is assembled.
   *
   * <p>This is the read that works from the body of a {@code Mono}- or {@code Flux}-returning
   * handler, where {@link #getCurrentContext()} would run too early. The context is taken from the
   * subscriber's Reactor {@code Context} if the pipeline carries one, and otherwise from the {@link
   * RequestContextScope} bound on the subscribing thread.
   *
   * @return a {@link Mono} of the current request's context, erroring with {@link
   *     IllegalStateException} if no Thrift request context is in scope
   */
  public static Mono<RequestContext> currentContext() {
    return Mono.deferContextual(
        contextView -> {
          Optional<RequestContext> fromReactor = RequestContext.tryContextView(contextView);
          if (fromReactor.isPresent()
              && !RequestContextScope.isAssemblySentinel(fromReactor.get())) {
            return Mono.just(fromReactor.get());
          }
          RequestContext bound = RequestContextScope.boundContext();
          if (bound != null && !RequestContextScope.isAssemblySentinel(bound)) {
            return Mono.just(bound);
          }
          return Mono.error(new IllegalStateException(NO_CONTEXT_IN_SCOPE));
        });
  }

  /**
   * Sets the thread-local context for the currently running request.
   *
   * <p>This is normally called only by the server, but it can also be useful to call when
   * dispatching to another thread (e.g. a thread in an ExecutorService) if the code that will run
   * on that thread might also be interested in the {@link RequestContext}
   *
   * @deprecated The value overrides any enclosing {@link RequestContextScope} for the rest of that
   *     scope, and is discarded when the scope exits. Outside every scope nothing removes it, so it
   *     stays on the thread. Bind with {@link RequestContextScope#run} or {@link
   *     RequestContextScope#call} instead, which unwind on exit.
   */
  @Deprecated
  public static void setCurrentContext(RequestContext requestContext) {
    threadLocalContext.set(
        requestContext == null ? AssemblySentinelContext.CLEARED : requestContext);
  }

  /**
   * Clears the context for the rest of the enclosing {@link RequestContextScope}, so reads answer
   * null even where a scope has bound one. Outside every scope it clears the thread.
   */
  public static void clearCurrentContext() {
    threadLocalContext.set(AssemblySentinelContext.CLEARED);
  }

  private static RequestContext rejectAssemblySentinel(RequestContext context) {
    if (RequestContextScope.isAssemblySentinel(context)) {
      throw new IllegalStateException(ASSEMBLY_TIME_READ);
    }
    return context;
  }
}
