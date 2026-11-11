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

package com.facebook.thrift.model;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Return value for a Thrift stream method declared with a first response ({@code First,
 * stream<Elem>}).
 *
 * <p>The generated {@code Service.Reactive} signature is {@code Mono<ResponseAndStream<F, E>>}: the
 * outer {@link Mono} carries the first response, the inner {@link Flux} the remaining stream
 * elements.
 *
 * <p>Client semantics (see {@code ResponseAndStreamBridge.toResponseAndStream}, the only producer):
 * the outer {@link Mono} resolves on the first payload; {@link #getStream()} yields the remaining
 * items. The inner {@link Flux} supports a single subscription. The first request already asks for
 * {@code 1 + 100} credits (C++ parity), so up to a hundred elements may be buffered before {@link
 * #getStream()} is subscribed; cancelling or disposing the inner subscription cancels the
 * underlying RSocket stream. An error before the first payload surfaces as an outer {@link Mono}
 * error (the initial error); errors afterwards surface inside the inner {@link Flux} (stream
 * errors). A stream that completes without a first payload is an outer error.
 *
 * <p>Operator placement: put per-call policies on the half they govern.
 *
 * <ul>
 *   <li>Outer {@link Mono}: {@code timeout} bounds the time to the first response; {@code retry}
 *       starts a new RPC.
 *   <li>Inner {@link Flux} (from {@link #getStream()}): {@code timeout} bounds the gap between
 *       elements; {@code take} or cancel stops the stream; {@code retry} fails (single
 *       subscription).
 *   <li>To share one stream: {@code publish().autoConnect(n)} or {@code cache()}.
 * </ul>
 *
 * <p>Must consume or dispose: once the outer {@link Mono} resolves, either subscribe to {@link
 * #getStream()} or call {@link #dispose()}. An unconsumed, undisposed holder pins the transport
 * stream, its outstanding credit, and any elements buffered before subscription, until the
 * connection closes. {@link #dispose()} abandons the stream: before subscription it tears down the
 * transport stream, while the inner stream is active it cancels it, and after termination it is a
 * no-op — so never call it on a stream you intend to read. {@link #dispose()} is idempotent. A
 * holder delivered concurrently with the caller's own cancel can be dropped by the caller's
 * subscriber (reported via {@code onNextDropped}) instead of reaching user code: callers that
 * cancel must dispose through their own path (the bridge disposes holders it drops itself, but a
 * holder dropped by user-side operators is the caller's to dispose).
 *
 * <p>Correct usage:
 *
 * <pre>{@code
 * Mono<ResponseAndStream<First, Elem>> result = client.someStream(request);
 *
 * // Consume everything: inner demand drives the transport, cancel stops it.
 * result.flatMapMany(ResponseAndStream::getStream).subscribe(this::useElement);
 *
 * // ... or take only the first response and release the rest. Never call this
 * // on a stream you intend to read: disposing an active stream cancels it.
 * result.subscribe(ras -> {
 *   useFirst(ras.getFirstResponse());
 *   ras.dispose();
 * });
 *
 * // WRONG: dropping the holder leaks the transport stream and its credit.
 * result.subscribe(ras -> useFirst(ras.getFirstResponse())); // stream stays open!
 *
 * // Leak-proof: the release runs on complete, error and cancel, even if the lambda throws.
 * Flux<Elem> elements =
 *     Flux.usingWhen(
 *         client.someStream(request),
 *         ras -> {
 *           useFirst(ras.getFirstResponse());
 *           return ras.getStream();
 *         },
 *         ras -> Mono.fromRunnable(ras::dispose));
 * }</pre>
 *
 * <p>Server semantics: the handler's outer {@link Mono} error is encoded as the initial error
 * (declared exceptions ride the first-response payload, undeclared ones the ERROR frame), exactly
 * as a pre-stream throw today; the first response is sent when the {@link Mono} emits, then the
 * inner {@link Flux} items, then completion. An empty outer {@link Mono} is an error, matching the
 * existing null-first-response handling. Instances created with {@link #of(Object, Flux)} (as
 * server handlers do) carry no transport handle, so {@link #dispose()} on them only flips the flag:
 * the server framework owns the stream lifecycle and disposes holders the downstream never
 * consumes.
 */
public final class ResponseAndStream<F, E> implements Disposable {

  private final F firstResponse;
  private final Flux<E> stream;
  private final Disposable onDispose;
  private final AtomicBoolean disposed;

  private ResponseAndStream(
      F firstResponse, Flux<E> stream, Disposable onDispose, AtomicBoolean disposed) {
    this.firstResponse = firstResponse;
    this.stream = stream;
    this.onDispose = onDispose;
    this.disposed = disposed;
  }

  /**
   * Creates an instance with no transport handle, for server handlers and tests. The framework
   * subscribes to the stream exactly once; {@link #dispose()} is a no-op (there is nothing to tear
   * down), and no single-subscription gate is added — the stream keeps its own rules.
   */
  public static <F, E> ResponseAndStream<F, E> of(F firstResponse, Flux<E> stream) {
    return new ResponseAndStream<>(
        Objects.requireNonNull(firstResponse, "firstResponse"),
        Objects.requireNonNull(stream, "stream"),
        null,
        new AtomicBoolean(false));
  }

  /**
   * Creates an instance that cancels {@code onDispose} when {@link #dispose()} is called. For the
   * client runtime only: {@code onDispose} cancels the underlying transport stream when the inner
   * stream was never subscribed or is still active. Server handlers use {@link #of(Object, Flux)}.
   */
  public static <F, E> ResponseAndStream<F, E> of(
      F firstResponse, Flux<E> stream, Disposable onDispose) {
    return new ResponseAndStream<>(
        Objects.requireNonNull(firstResponse, "firstResponse"),
        Objects.requireNonNull(stream, "stream"),
        Objects.requireNonNull(onDispose, "onDispose"),
        new AtomicBoolean(false));
  }

  public F getFirstResponse() {
    return firstResponse;
  }

  /**
   * Returns the inner stream of remaining elements. Single subscription: the first request already
   * asked for {@code 1 + 100} credits (C++ parity), so up to a hundred elements may be buffered
   * before this is subscribed, and cancelling the subscription stops the stream early. An
   * undisposed, unsubscribed holder keeps the buffered elements and the stream until the connection
   * closes. Never subscribe after {@link #dispose()}. To retry, resubscribe the outer {@link Mono};
   * to share one stream, use {@code publish()} or {@code cache()}.
   */
  public Flux<E> getStream() {
    return stream;
  }

  /**
   * Returns an instance with the inner stream replaced by {@code mapper} applied to it, keeping the
   * first response, the dispose handle, and the disposed flag: views share one flag with the
   * source, so {@link #isDisposed()} agrees across them. Used by the generated plain (non-{@code
   * Wrapper}) methods to drop header-only elements and unwrap per-element headers.
   */
  public <E2> ResponseAndStream<F, E2> mapStream(Function<Flux<E>, Flux<E2>> mapper) {
    return new ResponseAndStream<>(
        firstResponse, Objects.requireNonNull(mapper, "mapper").apply(stream), onDispose, disposed);
  }

  /**
   * Abandons the stream when only the first response is needed. Before the inner stream is
   * subscribed, tears down the underlying transport stream (client-created instances carry one);
   * while the inner stream is active, cancels it; after termination, a no-op. Runs at most once,
   * and is a no-op for instances without a transport handle. Never call this on a stream you intend
   * to read. Call it instead of dropping the holder: an undisposed, unsubscribed holder pins the
   * transport stream and its credit until the connection closes.
   */
  @Override
  public void dispose() {
    if (disposed.compareAndSet(false, true) && onDispose != null) {
      onDispose.dispose();
    }
  }

  @Override
  public boolean isDisposed() {
    return disposed.get() || (onDispose != null && onDispose.isDisposed());
  }
}
