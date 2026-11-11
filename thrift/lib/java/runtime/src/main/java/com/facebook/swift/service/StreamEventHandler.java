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

/**
 * Stream and sink lifecycle hooks for streaming RPCs. To observe streams, change {@code implements
 * ThriftEventHandler} to {@code implements StreamEventHandler}: it goes into the one existing
 * handler list with no extra registration, and every hook receives the SAME context the handler
 * returned from {@code getContext} for this request.
 *
 * <p>Callback order per stream: {@code onStreamSubscribe} once, then any interleaving of {@code
 * onStreamNext} (1-based per-stream sequence) and {@code onStreamCredit}, with {@code
 * handleStreamError} before the terminal on errors, and {@code onStreamFinally} exactly once,
 * strictly before {@code done()}. An error before the stream starts is request-level: no stream
 * hook fires for it. The terminal carries the outcome only; the cause rode {@code
 * handleStreamError}. Sinks follow the same shape ({@code onSinkSubscribe}, {@code onSinkNext},
 * {@code onSinkConsumed}, {@code onSinkCancel}, {@code onSinkCredit}, {@code handleSinkError},
 * {@code onSinkFinally}).
 *
 * <p>Hooks run inline on the signal thread under one per-chain monitor, so they never overlap. Keep
 * them fast, non-blocking, and lock-free: a slow hook holds the signal thread (which can be an
 * event-loop thread), and a hook that takes a lock the producer holds while emitting deadlocks. A
 * hook that throws propagates like any other handler hook and fails the stream; a hook on a Thrift
 * event loop while forced off it logs a warning and still runs.
 */
public interface StreamEventHandler extends ThriftEventHandler {
  /** Terminal outcome of a stream. */
  enum StreamOutcome {
    COMPLETE,
    ERROR,
    CANCEL
  }

  /** Terminal outcome of a sink. */
  enum SinkOutcome {
    COMPLETE,
    COMPLETE_WITH_ERROR,
    ERROR
  }

  /**
   * Fired once when the stream starts, before any element or error. Never fires on initial error.
   */
  default void onStreamSubscribe(Object context, String methodName) {}

  /** Fired once per stream element after encoding, with a 1-based per-stream sequence number. */
  default void onStreamNext(Object context, String methodName, long sequenceNumber) {}

  /** Fired when client demand arrives. Values saturate at {@link Integer#MAX_VALUE}. */
  default void onStreamCredit(Object context, String methodName, int credits) {}

  /** Fired when a stream error is observed, before {@code onStreamFinally(ERROR)}. */
  default void handleStreamError(Object context, String methodName, Throwable error) {}

  /** Fired exactly once per stream with its terminal outcome, strictly before {@code done()}. */
  default void onStreamFinally(Object context, String methodName, StreamOutcome outcome) {}

  /** Fired once when a sink begins, before any upload or credit callback. */
  default void onSinkSubscribe(Object context, String methodName) {}

  /** Fired once per upload element, with a 1-based per-sink sequence number. */
  default void onSinkNext(Object context, String methodName, long sequenceNumber) {}

  /** Fired once per element fully consumed by the handler. */
  default void onSinkConsumed(Object context, String methodName) {}

  /** Fired once when the client cancels a sink before it completes. */
  default void onSinkCancel(Object context, String methodName) {}

  /** Fired when the server grants upload demand. Values saturate at {@link Integer#MAX_VALUE}. */
  default void onSinkCredit(Object context, String methodName, int credits) {}

  /** Fired when a sink error is observed, before the error terminal. */
  default void handleSinkError(Object context, String methodName, Throwable error) {}

  /** Fired exactly once per sink with its terminal outcome. */
  default void onSinkFinally(Object context, String methodName, SinkOutcome outcome) {}
}
