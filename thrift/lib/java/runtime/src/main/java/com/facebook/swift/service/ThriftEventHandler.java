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

package com.facebook.swift.service;

import com.facebook.nifty.core.RequestContext;
import java.util.List;
import org.apache.thrift.TException;

/**
 * Per-request lifecycle hooks. The lifecycle order is: {@link #getContext} once per request, then
 * {@link #preprocess} → {@link #preRead} → {@link #postRead} → user handler → {@link #preWrite} →
 * {@link #postWrite} → {@link #done}. The {@code Object context} returned by {@code getContext}
 * threads through every subsequent hook for stash-per-request state.
 */
public interface ThriftEventHandler {
  /**
   * Allocate per-request state; the returned object is passed to every other hook. The current
   * {@link RequestContext} is the {@code requestContext} argument; prefer it. {@link ContextChain}
   * runs every hook, on every transport, with that request context bound, so {@link
   * com.facebook.nifty.core.RequestContexts#getCurrentContext()} also answers it inside a hook.
   */
  default Object getContext(String methodName, RequestContext requestContext) {
    return null;
  }

  /**
   * Admission-control hook on the caller / I/O thread, BEFORE the request is queued onto the
   * off-loop scheduler. Throw {@link org.apache.thrift.TApplicationException} (typically with code
   * {@code LOADSHEDDING}) to reject; doing so frees the request buffer immediately via the
   * transport's {@code doFinally} and never pays queue time, parse CPU, or user-handler dispatch.
   *
   * <p>The request context is bound here, as in every hook. To keep it without a lookup, stash it
   * on the object returned by {@link #getContext} and read it back from the {@code context}
   * argument.
   *
   * <p>{@link #done} is guaranteed to fire for every handler whose {@link #getContext} succeeded,
   * regardless of whether this {@code preprocess} (or any other handler's) throws -- the dispatcher
   * attaches a top-level {@code doFinally} for {@code done()}. Pair state mutations across {@code
   * getContext} / {@code done} (not across {@code preprocess} / {@code done}) so the pairing is
   * unconditional.
   */
  default void preprocess(Object context, String methodName) throws TException {}

  /** Off-loop hook fired before the request args are deserialized. */
  default void preRead(Object context, String methodName) throws TException {}

  /** Off-loop hook fired after the request args are deserialized. */
  default void postRead(Object context, String methodName, Object[] args) throws TException {}

  /** Off-loop hook fired after the request args are deserialized. */
  default void postRead(Object context, String methodName, List args) throws TException {
    postRead(context, methodName, args.toArray());
  }

  /** Fired before the response is serialized. */
  default void preWrite(Object context, String methodName, Object result) throws TException {}

  /** Fired before an exception response is serialized. */
  default void preWriteException(Object context, String methodName, Throwable t)
      throws TException {}

  /** Fired after a successful response is written. */
  default void postWrite(Object context, String methodName, Object result) throws TException {}

  /** Fired after an exception response is written. */
  default void postWriteException(Object context, String methodName, Throwable t)
      throws TException {}

  /** Fired when the handler completes with a declared (IDL-declared) exception. */
  default void declaredUserException(Object o, String methodName, Throwable t) throws TException {}

  /** Fired when the handler completes with an undeclared exception. */
  default void undeclaredUserException(Object o, String methodName, Throwable t)
      throws TException {}

  /** Final hook fired after the request completes, success or failure. Use for cleanup. */
  default void done(Object context, String methodName) {}
}
