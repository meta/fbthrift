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

import com.google.common.util.concurrent.FutureCallback;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * Binds a {@link RequestContext} for the dynamic extent of a body of work.
 *
 * <p>Java 25 multi-release override of the Java 8 baseline, binding a {@link ScopedValue} rather
 * than a thread local. The runtime owns the unwind, so the binding cannot outlive the scope onto a
 * pooled thread, and it is inherited by structured-concurrency forks. Work running inside a scope
 * reads the binding back through {@link #boundContext()}.
 *
 * <p>A null context binds <em>absence</em>: {@link #boundContext()} answers null for the extent
 * rather than exposing whatever the thread carried beforehand. Work that belongs to no request
 * needs that, or it reads the context of whichever request last ran on the thread.
 *
 * <p>Only the reference is bound. {@link RequestContext} implementations back their context data
 * with a concurrent map, so {@link RequestContext#setContextData} keeps working inside a scope.
 *
 * <p>The imperative setters on {@link RequestContexts} write a separate slot that takes precedence
 * for the rest of the enclosing extent and is put back as it was found on the way out, so a legacy
 * caller reaching for the setter mid-scope still wins, and still cannot outlive the scope. The
 * baseline resolves in the same order against a thread local, so the two agree.
 */
public final class RequestContextScope {

  /**
   * Bound while a reactive handler's method body is assembling its pipeline.
   *
   * <p>Assembly runs before the request is subscribed to, so there is no request to describe yet.
   * Every accessor on this instance throws rather than answering with an absent or stale context.
   */
  public static final RequestContext ASSEMBLY_SENTINEL = AssemblySentinelContext.INSTANCE;

  /** Stands in for a null context, which a {@link ScopedValue} cannot distinguish from unbound. */
  private static final RequestContext NO_CONTEXT = AssemblySentinelContext.CLEARED;

  private static final ScopedValue<RequestContext> CTX = ScopedValue.newInstance();

  private RequestContextScope() {}

  /**
   * Calls {@code body} with {@code ctx} bound, or with no context bound when {@code ctx} is null.
   *
   * @return whatever {@code body} returns
   * @throws Exception whatever {@code body} throws
   */
  public static <T> T call(RequestContext ctx, Callable<T> body) throws Exception {
    ThreadLocal<RequestContext> override = RequestContexts.overrideBinding();
    RequestContext previousOverride = override.get();
    override.remove();
    try {
      return ScopedValue.where(CTX, ctx == null ? NO_CONTEXT : ctx).call(body::call);
    } finally {
      restoreOverride(override, previousOverride);
    }
  }

  /**
   * Invokes {@code body} with {@code ctx} bound, or with no context bound when {@code ctx} is null.
   *
   * @return whatever {@code body} returns
   */
  public static <T> T get(RequestContext ctx, Supplier<T> body) {
    ThreadLocal<RequestContext> override = RequestContexts.overrideBinding();
    RequestContext previousOverride = override.get();
    override.remove();
    try {
      return ScopedValue.where(CTX, ctx == null ? NO_CONTEXT : ctx).call(body::get);
    } finally {
      restoreOverride(override, previousOverride);
    }
  }

  /**
   * Runs {@code body} with {@code ctx} bound, or with no context bound when {@code ctx} is null.
   */
  public static void run(RequestContext ctx, Runnable body) {
    ThreadLocal<RequestContext> override = RequestContexts.overrideBinding();
    RequestContext previousOverride = override.get();
    override.remove();
    try {
      ScopedValue.where(CTX, ctx == null ? NO_CONTEXT : ctx).run(body);
    } finally {
      restoreOverride(override, previousOverride);
    }
  }

  /**
   * Adapts {@code delegate} so that each task observes the context bound on the thread that calls
   * {@code execute}, and only that context.
   *
   * <p>The context is read when {@code execute} is called, not when this executor is created. That
   * suits a direct {@code execute(task)} made while serving a request. It does not suit an executor
   * handed to {@code Futures.addCallback} or {@code addListener}: the thread that completes the
   * future calls {@code execute}, so the callback would observe that thread's context. Capture the
   * callback itself with {@link #capture(FutureCallback)} or {@link #capture(Runnable)} instead.
   *
   * @return an executor that captures the context bound on the calling thread and opens a scope
   *     around the task on the executing thread. A task submitted with nothing bound, or from a
   *     reactive handler body at assembly time, runs with nothing bound rather than inheriting
   *     whatever the worker last carried.
   */
  public static Executor wrap(Executor delegate) {
    return task -> {
      Objects.requireNonNull(task, "task");
      RequestContext captured = capturable(boundContext());
      delegate.execute(() -> run(captured, task));
    };
  }

  /**
   * Captures the context bound now, and returns a task that runs {@code task} with it bound.
   *
   * <p>Use this for a continuation that runs later on a thread this code does not control, such as
   * a future listener: {@code future.addListener(capture(task), executor)}.
   */
  public static Runnable capture(Runnable task) {
    RequestContext captured = capturable(boundContext());
    return () -> run(captured, task);
  }

  /**
   * Captures the context bound now, and returns a task that calls {@code task} with it bound.
   *
   * <p>Use this with {@code ExecutorService.submit} and other APIs that take a {@link Callable}.
   */
  public static <T> Callable<T> capture(Callable<T> task) {
    RequestContext captured = capturable(boundContext());
    return () -> call(captured, task);
  }

  /**
   * Captures the context bound now, and returns a callback that runs each method of {@code
   * callback} with it bound.
   *
   * <p>Use this with {@code Futures.addCallback(future, capture(callback), executor)}. The thread
   * that completes the future runs the callback, so without this the callback would observe that
   * thread's context.
   */
  public static <T> FutureCallback<T> capture(FutureCallback<T> callback) {
    Objects.requireNonNull(callback, "callback");
    RequestContext captured = capturable(boundContext());
    return new FutureCallback<T>() {
      @Override
      public void onSuccess(T result) {
        run(captured, () -> callback.onSuccess(result));
      }

      @Override
      public void onFailure(Throwable t) {
        run(captured, () -> callback.onFailure(t));
      }
    };
  }

  /**
   * The context bound by the innermost enclosing scope.
   *
   * <p>May answer {@link #ASSEMBLY_SENTINEL}, which is not a usable context. Callers that hand the
   * result onward should route through {@link RequestContexts#getCurrentContext()}, which rejects
   * it, or screen it with {@link #isAssemblySentinel}.
   *
   * @return the bound context, the assembly sentinel, or null when nothing is bound
   */
  public static RequestContext boundContext() {
    RequestContext override = RequestContexts.overrideBinding().get();
    if (override != null) {
      return override == RequestContexts.clearedMarker() ? null : override;
    }
    if (CTX.isBound()) {
      RequestContext scoped = CTX.get();
      return scoped == NO_CONTEXT ? null : scoped;
    }
    return null;
  }

  /**
   * Whether {@code ctx} is {@link #ASSEMBLY_SENTINEL} rather than a real request context.
   *
   * @return true if the caller is running at pipeline assembly time
   */
  public static boolean isAssemblySentinel(RequestContext ctx) {
    return ctx == ASSEMBLY_SENTINEL;
  }

  /** The sentinel describes the capturing thread's moment, so it must not travel to another. */
  private static RequestContext capturable(RequestContext ctx) {
    return isAssemblySentinel(ctx) ? null : ctx;
  }

  private static void restoreOverride(
      ThreadLocal<RequestContext> override, RequestContext previous) {
    if (previous == null) {
      override.remove();
    } else {
      override.set(previous);
    }
  }
}
