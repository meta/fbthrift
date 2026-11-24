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
import com.facebook.nifty.core.RequestContextScope;
import com.facebook.thrift.util.NettyUtil;
import com.facebook.thrift.util.resources.RpcResources;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.SignalType;

/**
 * Per-request fan-out helper that invokes each {@link ThriftEventHandler} at the matching lifecycle
 * point with the per-handler {@code context} returned by {@link ThriftEventHandler#getContext}.
 * First handler that throws short-circuits the rest.
 *
 * <p>Every hook runs with the request's context bound, on whichever thread invokes it, so a handler
 * that reads {@link com.facebook.nifty.core.RequestContexts#getCurrentContext()} sees its request
 * in every hook and on every transport. The binding unwinds when the hook returns.
 */
public class ContextChain {
  private static final Logger logger = LoggerFactory.getLogger(ContextChain.class);

  private final List<ThriftEventHandler> handlers;
  private final String methodName;
  private final RequestContext requestContext;
  private final List<Object> contexts;
  private final List<StreamEventHandler> streamHandlers;
  private final List<Object> streamContexts;

  // Stream/sink lifecycle, only touched under the chain monitor.
  private boolean streamActive;
  private boolean streamTerminated;
  private boolean sinkActive;
  private boolean sinkTerminated;
  private Throwable streamErrorCause;
  private long streamSequence;
  private long sinkSequence;

  /**
   * Calls {@code getContext} on every handler once and stores the result. If a later handler's
   * {@code getContext} throws, unwinds the already-succeeded prefix by calling {@code done()} in
   * reverse, then rethrows -- preserves the per-handler invariant that a successful {@code
   * getContext} is always paired with a {@code done()}.
   */
  public ContextChain(
      List<ThriftEventHandler> handlers, String methodName, RequestContext requestContext) {
    this.handlers = handlers;
    this.methodName = methodName;
    this.requestContext = requestContext;
    this.contexts = handlers.isEmpty() ? Collections.emptyList() : new ArrayList<>(handlers.size());
    RequestContextScope.run(
        requestContext,
        () -> {
          try {
            for (ThriftEventHandler handler : this.handlers) {
              this.contexts.add(handler.getContext(methodName, requestContext));
            }
          } catch (RuntimeException | Error t) {
            unwindPrefix(t);
            throw t;
          }
        });
    // Outlined so the constructor keeps the parent shape (this runs once per request).
    this.streamHandlers = collectStreamHandlers(this.handlers);
    this.streamContexts =
        streamHandlers.isEmpty()
            ? Collections.emptyList()
            : collectStreamContexts(this.handlers, this.contexts);
  }

  // Cold: only runs when a getContext throws. Outlined to keep the constructor inlineable.
  private void unwindPrefix(Throwable t) {
    for (int i = contexts.size() - 1; i >= 0; i--) {
      try {
        handlers.get(i).done(contexts.get(i), methodName);
      } catch (Throwable suppressed) {
        t.addSuppressed(suppressed);
      }
    }
  }

  private static List<StreamEventHandler> collectStreamHandlers(List<ThriftEventHandler> handlers) {
    List<StreamEventHandler> found = null;
    for (ThriftEventHandler handler : handlers) {
      if (handler instanceof StreamEventHandler) {
        if (found == null) {
          found = new ArrayList<>();
        }
        found.add((StreamEventHandler) handler);
      }
    }
    return found == null ? Collections.emptyList() : found;
  }

  private static List<Object> collectStreamContexts(
      List<ThriftEventHandler> handlers, List<Object> contexts) {
    List<Object> found = null;
    for (int i = 0; i < handlers.size(); i++) {
      if (handlers.get(i) instanceof StreamEventHandler) {
        if (found == null) {
          found = new ArrayList<>();
        }
        found.add(contexts.get(i));
      }
    }
    return found == null ? Collections.emptyList() : found;
  }

  /** True when at least one handler observes streams and sinks; otherwise those taps stay off. */
  public boolean hasStreamHandlers() {
    return !streamHandlers.isEmpty();
  }

  /**
   * Request completion for the generated streaming dispatcher. A begun stream stays out of the way
   * (its terminal and done() already fired together at the inner terminal site); otherwise the
   * stream never began and this terminates the leftovers first so a racing subscribe or terminal
   * records nothing after done().
   */
  public synchronized void finishRequest() {
    if (streamHandlers.isEmpty()) {
      done();
      return;
    }
    if (!streamActive) {
      streamTerminated = true;
      sinkTerminated = true;
      done();
    }
  }

  /**
   * Admission-control hook: invoked on the caller thread BEFORE off-loop queueing so a shed never
   * pays queue time, parse CPU, or pins the request buffer.
   */
  public void preprocess() throws TException {
    forEachHandler(i -> handlers.get(i).preprocess(contexts.get(i), methodName));
  }

  public void preRead() throws TException {
    forEachHandler(i -> handlers.get(i).preRead(contexts.get(i), methodName));
  }

  public void postRead(Object[] args) throws TException {
    forEachHandler(i -> handlers.get(i).postRead(contexts.get(i), methodName, args));
  }

  public void postRead(List args) throws TException {
    forEachHandler(i -> handlers.get(i).postRead(contexts.get(i), methodName, args));
  }

  public void preWrite(Object result) throws TException {
    forEachHandler(i -> handlers.get(i).preWrite(contexts.get(i), methodName, result));
  }

  public void preWriteException(Throwable t) throws TException {
    forEachHandler(i -> handlers.get(i).preWriteException(contexts.get(i), methodName, t));
  }

  public void postWrite(Object result) throws TException {
    forEachHandler(i -> handlers.get(i).postWrite(contexts.get(i), methodName, result));
  }

  public void postWriteException(Throwable t) throws TException {
    forEachHandler(i -> handlers.get(i).postWriteException(contexts.get(i), methodName, t));
  }

  public void declaredUserException(Throwable t) throws TException {
    forEachHandler(i -> handlers.get(i).declaredUserException(contexts.get(i), methodName, t));
  }

  public void undeclaredUserException(Throwable t) throws TException {
    forEachHandler(i -> handlers.get(i).undeclaredUserException(contexts.get(i), methodName, t));
  }

  /** Final hook fired after the request completes, success or failure. Use for cleanup. */
  public void done() {
    RequestContextScope.run(
        requestContext,
        () -> {
          for (int i = 0; i < handlers.size(); i++) {
            handlers.get(i).done(contexts.get(i), methodName);
          }
        });
  }

  @FunctionalInterface
  private interface Hook {
    void call(int handlerIndex) throws TException;
  }

  private void forEachHandler(Hook hook) throws TException {
    try {
      RequestContextScope.call(
          requestContext,
          () -> {
            for (int i = 0; i < handlers.size(); i++) {
              hook.call(i);
            }
            return null;
          });
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      // Unreachable: hooks declare only TException.
      throw new IllegalStateException(e);
    }
  }

  // A hook running on a Thrift event-loop thread while execution is forced off the loop is a
  // placement bug: warn (unthrottled) and still run it.
  private void safeguard(String hookName) {
    if (NettyUtil.isThriftEventLoopThread() && RpcResources.isForceExecutionOffEventLoop()) {
      logger.warn(
          "StreamEventHandler hook {} for method {} runs on event-loop thread {}; hooks must stay"
              + " off the loop when execution is forced off it",
          hookName,
          methodName,
          Thread.currentThread().getName());
    }
  }

  public synchronized void onStreamSubscribe() {
    if (streamHandlers.isEmpty() || streamTerminated) {
      return;
    }
    streamActive = true;
    safeguard("onStreamSubscribe");
    for (int i = 0; i < streamHandlers.size(); i++) {
      streamHandlers.get(i).onStreamSubscribe(streamContexts.get(i), methodName);
    }
  }

  public synchronized void onStreamNext() {
    if (streamHandlers.isEmpty() || streamTerminated) {
      return;
    }
    safeguard("onStreamNext");
    long sequenceNumber = ++streamSequence;
    for (int i = 0; i < streamHandlers.size(); i++) {
      streamHandlers.get(i).onStreamNext(streamContexts.get(i), methodName, sequenceNumber);
    }
  }

  public synchronized void onStreamCredit(int credits) {
    if (streamHandlers.isEmpty() || streamTerminated) {
      return;
    }
    safeguard("onStreamCredit");
    for (int i = 0; i < streamHandlers.size(); i++) {
      streamHandlers.get(i).onStreamCredit(streamContexts.get(i), methodName, credits);
    }
  }

  // Records the error before fanning out, so the ERROR terminal resolves correctly even when a
  // throwing error hook aborts the fan-out: the terminal still fires (and done with it).
  public synchronized void handleStreamError(Throwable t) {
    if (streamHandlers.isEmpty() || streamTerminated) {
      return;
    }
    if (streamErrorCause == null) {
      streamErrorCause = t;
    }
    safeguard("handleStreamError");
    for (int i = 0; i < streamHandlers.size(); i++) {
      streamHandlers.get(i).handleStreamError(streamContexts.get(i), methodName, t);
    }
  }

  /**
   * Single terminal signal for a stream: fires {@link StreamEventHandler#onStreamFinally} exactly
   * once when the stream began, strictly before {@link #done()}. A hook that throws propagates
   * (like every other handler hook); {@code done()} still runs.
   */
  public synchronized void finishStream(SignalType signal) {
    // Nothing began and nothing can begin (every tap returns early without handlers), so this
    // owns nothing -- the generated outer doFinally still calls finishRequest(), which fires
    // done(). Firing done() here too would double it.
    if (streamHandlers.isEmpty() || !streamActive || streamTerminated) {
      return;
    }
    streamTerminated = true;
    StreamEventHandler.StreamOutcome terminal =
        signal == SignalType.CANCEL
            ? StreamEventHandler.StreamOutcome.CANCEL
            : streamErrorCause != null
                ? StreamEventHandler.StreamOutcome.ERROR
                : StreamEventHandler.StreamOutcome.COMPLETE;
    safeguard("onStreamFinally");
    try {
      for (int i = 0; i < streamHandlers.size(); i++) {
        streamHandlers.get(i).onStreamFinally(streamContexts.get(i), methodName, terminal);
      }
    } finally {
      done();
    }
  }

  public synchronized void onSinkSubscribe() {
    if (streamHandlers.isEmpty() || sinkTerminated) {
      return;
    }
    sinkActive = true;
    safeguard("onSinkSubscribe");
    for (int i = 0; i < streamHandlers.size(); i++) {
      streamHandlers.get(i).onSinkSubscribe(streamContexts.get(i), methodName);
    }
  }

  public synchronized void onSinkNext() {
    if (streamHandlers.isEmpty() || sinkTerminated) {
      return;
    }
    safeguard("onSinkNext");
    long sequenceNumber = ++sinkSequence;
    for (int i = 0; i < streamHandlers.size(); i++) {
      streamHandlers.get(i).onSinkNext(streamContexts.get(i), methodName, sequenceNumber);
    }
  }

  public synchronized void onSinkConsumed() {
    if (streamHandlers.isEmpty() || sinkTerminated) {
      return;
    }
    safeguard("onSinkConsumed");
    for (int i = 0; i < streamHandlers.size(); i++) {
      streamHandlers.get(i).onSinkConsumed(streamContexts.get(i), methodName);
    }
  }

  public synchronized void onSinkCancel() {
    if (streamHandlers.isEmpty() || sinkTerminated) {
      return;
    }
    safeguard("onSinkCancel");
    for (int i = 0; i < streamHandlers.size(); i++) {
      streamHandlers.get(i).onSinkCancel(streamContexts.get(i), methodName);
    }
  }

  public synchronized void onSinkCredit(int credits) {
    if (streamHandlers.isEmpty() || sinkTerminated) {
      return;
    }
    safeguard("onSinkCredit");
    for (int i = 0; i < streamHandlers.size(); i++) {
      streamHandlers.get(i).onSinkCredit(streamContexts.get(i), methodName, credits);
    }
  }

  public synchronized void handleSinkError(Throwable t) {
    if (streamHandlers.isEmpty() || sinkTerminated) {
      return;
    }
    safeguard("handleSinkError");
    for (int i = 0; i < streamHandlers.size(); i++) {
      streamHandlers.get(i).handleSinkError(streamContexts.get(i), methodName, t);
    }
  }

  /**
   * Single terminal signal for a sink: fires {@link StreamEventHandler#onSinkFinally} exactly once
   * when the sink began. Never fires {@link #done()}: the sink owner owns request completion.
   */
  public synchronized void onSinkFinally(StreamEventHandler.SinkOutcome outcome) {
    if (streamHandlers.isEmpty() || !sinkActive || sinkTerminated) {
      return;
    }
    sinkTerminated = true;
    safeguard("onSinkFinally");
    for (int i = 0; i < streamHandlers.size(); i++) {
      streamHandlers.get(i).onSinkFinally(streamContexts.get(i), methodName, outcome);
    }
  }
}
