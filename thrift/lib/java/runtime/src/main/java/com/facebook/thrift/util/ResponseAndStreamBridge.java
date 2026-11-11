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

import com.facebook.thrift.client.ResponseWrapper;
import com.facebook.thrift.model.ResponseAndStream;
import com.facebook.thrift.model.StreamResponse;
import com.facebook.thrift.payload.ClientResponsePayload;
import com.facebook.thrift.util.resources.RpcResources;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.scheduler.Scheduler;
import reactor.util.concurrent.Queues;

/**
 * Bridges a raw stream of decoded {@code ClientResponsePayload} elements carrying {@code
 * StreamResponse} data (as produced by {@code RpcClient.singleRequestStreamingResponse} for a
 * stream-with-first-response method) into the user-facing {@code
 * Mono<ResponseWrapper<ResponseAndStream<F, ResponseWrapper<E>>>>}.
 */
public final class ResponseAndStreamBridge {

  private ResponseAndStreamBridge() {}

  /**
   * Initial outstanding window for stream-with-first-response methods, matching the C++ client: one
   * credit for the first response plus {@code STREAM_WINDOW} data credits in the initial
   * REQUEST_STREAM frame, with {@code STREAM_WINDOW / 2} refills as elements are consumed. A second
   * demand signal in the same task as first-response processing wedges ServiceRouter-proxied
   * streams, so the bridge must never forward downstream demand itself: everything below requests
   * against this window, and the wire sees refills only as items arrive, long after the first
   * payload settles.
   */
  static final int STREAM_WINDOW = 100;

  /**
   * Bridges the raw decoded stream into the user-facing holder.
   *
   * <p>This is the service-level path: the bridge itself performs no thread hop, so the outer
   * holder resolves on the transport event loop. Subscribe the inner stream without blocking that
   * thread (for example with {@code flatMapMany}); nesting a blocking subscription inside the outer
   * {@code onNext} deadlocks the connection. The transport stream carries an initial window of
   * {@code 1 + STREAM_WINDOW} credits (one for the first response, matching the C++ client) and
   * {@code STREAM_WINDOW / 2} refills as elements are consumed; downstream demand (the switch's own
   * first credit, the inner stream, the replay skip) is served from that window and is only
   * forwarded upstream as consumption-paced refills, never as an early extra frame. The generated
   * interaction {@code Wrapper} goes through {@link #toInteractionResponseAndStream} instead, which
   * keeps the historical off-loop hop (with its prefetch window) on top of this bridge.
   *
   * <p>Lifecycle, matching the {@link ResponseAndStream} contract:
   *
   * <ul>
   *   <li>The outer {@link Mono} resolves on the first payload, with the first payload's headers. A
   *       first-payload exception (the initial error) is an outer error.
   *   <li>All lifecycle state is built per outer subscription (inside {@code Mono.defer}), so two
   *       holders resolved from the same {@link Mono} cannot cancel each other's transport stream.
   *   <li>One atomic gate arbitrates the inner stream against an early dispose: exactly one of
   *       "inner subscribed" and "holder disposed first" wins. A subscription that loses (second
   *       subscription, or any subscription after dispose) fails immediately instead of hanging.
   *   <li>Disposing the resolved {@link ResponseAndStream} without subscribing the inner stream
   *       subscribes the remainder with a subscriber that cancels in {@code onSubscribe}, tearing
   *       down the live upstream without requesting further credits. Disposing with an active inner
   *       stream cancels that subscription; disposing a terminated stream is a transport no-op.
   *   <li>Cancelling the outer {@link Mono} before the first payload cancels the upstream stream. A
   *       holder that loses a delivery race against the caller's own cancel is reported through the
   *       discard channel and disposed there, so the orphaned upstream is still torn down.
   *   <li>The inner {@link Flux} supports a single subscription; a second subscription fails. A
   *       terminal state that arrives before it subscribes is replayed to it. Each inner element
   *       carries its own payload headers. A header-only frame arrives as an element with null
   *       data. Installing the inner subscription races disposal through a cancellation sentinel: a
   *       subscription installed after disposal or termination cancels itself immediately, so the
   *       transport can be neither stranded nor double-cancelled.
   *   <li>A source that completes without a first payload, or whose first element does not carry
   *       the first response, is an outer error.
   * </ul>
   *
   * @param responses decoded stream elements whose data is {@code StreamResponse<F, E>}
   * @return Mono of the response wrapper holding the first response and the stream
   */
  public static <F, E>
      Mono<ResponseWrapper<ResponseAndStream<F, ResponseWrapper<E>>>> toResponseAndStream(
          Flux<? extends ClientResponsePayload<?>> responses) {
    return Mono.defer(
        () ->
            responses
                .limitRate(1 + STREAM_WINDOW, STREAM_WINDOW / 2)
                .switchOnFirst(
                    (firstSignal, rest) -> {
                      if (firstSignal.hasValue()) {
                        return ResponseAndStreamBridge.<F, E>onFirstPayload(
                            firstSignal.get(), rest);
                      }
                      if (firstSignal.isOnError()) {
                        return Mono.error(firstSignal.getThrowable());
                      }
                      return Mono.error(
                          new IllegalStateException("Stream completed without a first response"));
                    },
                    false)
                // A holder the downstream never takes (outer cancelled while the value is in
                // flight) is reported through the discard channel: dispose it so the orphaned
                // upstream stream is torn down instead of stranded. The class filter also
                // matches ClientResponsePayload (it implements ResponseWrapper), which upstream
                // operators discard on routine early disposal: disposeIfDropped tells the two
                // apart (only holders carry a ResponseAndStream).
                .single()
                .doOnDiscard(ResponseWrapper.class, holder -> disposeIfDropped(holder)));
  }

  /**
   * Interaction variant of {@link #toResponseAndStream}: keeps the historical interaction behavior
   * exactly (same flag, same scheduler, same prefetch as the generated {@code Wrapper} applied at
   * the parent revision). When forcing off the event loop is enabled, the outer holder resolution
   * and every inner element are delivered off-loop, and the wire sees the first-response credit
   * plus the {@link #INTERACTION_STREAM_PREFETCH} window regardless of user demand. When it is
   * disabled, this is the bare bridge: the wire carries the initial window plus consumption-paced
   * refills.
   *
   * @param responses decoded stream elements whose data is {@code StreamResponse<F, E>}
   * @return Mono of the response wrapper holding the first response and the stream
   */
  public static <F, E>
      Mono<ResponseWrapper<ResponseAndStream<F, ResponseWrapper<E>>>>
          toInteractionResponseAndStream(Flux<? extends ClientResponsePayload<?>> responses) {
    boolean forceOffLoop = RpcResources.isForceExecutionOffEventLoop();
    Scheduler offLoopScheduler = forceOffLoop ? RpcResources.getOffLoopScheduler() : null;
    Mono<ResponseWrapper<ResponseAndStream<F, ResponseWrapper<E>>>> outer =
        ResponseAndStreamBridge.<F, E>toResponseAndStream(responses)
            .map(
                holder ->
                    ResponseWrapper.create(
                        holder
                            .getData()
                            .mapStream(
                                (Flux<ResponseWrapper<E>> inner) ->
                                    ResponseAndStreamBridge.hopInner(
                                        inner, forceOffLoop, offLoopScheduler)),
                        holder.getHeaders(),
                        holder.getBinaryHeaders()));
    if (!forceOffLoop) {
      return outer;
    }
    return guardUndeliveredResponse(outer, offLoopScheduler);
  }

  /**
   * Prefetch window the interaction path requests on top of the first-response credit: the default
   * a single-argument {@code publishOn} requests ({@code Flux.publishOn(Scheduler)} delegates to
   * this constant), so the wire behavior is identical to the generated {@code Wrapper} hop at the
   * parent revision.
   *
   * <p>Hook for a future per-call knob (the pending {@code RpcOptions.chunkBufferSize} draft):
   * replace this constant with {@code size + 1} to match the C++ Rocket client semantics.
   */
  static final int INTERACTION_STREAM_PREFETCH = Queues.SMALL_BUFFER_SIZE;

  /** Applies the interaction inner-stream hop when forcing off-loop, untouched otherwise. */
  private static <E> Flux<ResponseWrapper<E>> hopInner(
      Flux<ResponseWrapper<E>> inner, boolean forceOffLoop, Scheduler offLoopScheduler) {
    return forceOffLoop ? inner.publishOn(offLoopScheduler, INTERACTION_STREAM_PREFETCH) : inner;
  }

  /**
   * Delivers an already-resolved holder across an off-loop hop while keeping transport ownership
   * until the holder actually reaches the subscriber. If the outer subscription is cancelled while
   * the holder is still queued in the hop (the hop operator clears its queue without a discard
   * signal), the pending holder is disposed, which tears down the otherwise orphaned upstream
   * stream exactly once. Rejected execution (a shut-down scheduler) takes the same path: the error
   * terminates the sequence and any holder emitted before the rejection is disposed.
   *
   * <p>The pending cell lives per subscription (inside {@code Mono.defer}): sharing it across two
   * subscriptions lets one subscription's cleanup dispose the other's holder. A closed sentinel
   * closes the race between installation and cleanup: a holder installed after cleanup disposes
   * itself, and cleanup after delivery is a no-op.
   */
  @SuppressWarnings("unchecked")
  static <F, E>
      Mono<ResponseWrapper<ResponseAndStream<F, ResponseWrapper<E>>>> guardUndeliveredResponse(
          Mono<ResponseWrapper<ResponseAndStream<F, ResponseWrapper<E>>>> outer,
          Scheduler scheduler) {
    return Mono.defer(
        () -> {
          Object closed = new Object();
          AtomicReference<Object> pending = new AtomicReference<>();
          return outer
              .doOnNext(
                  holder -> {
                    if (!pending.compareAndSet(null, holder)) {
                      holder.getData().dispose();
                    }
                  })
              .publishOn(scheduler)
              .doOnNext(holder -> pending.compareAndSet(holder, closed))
              .doFinally(
                  signal -> {
                    Object holder = pending.getAndSet(closed);
                    if (holder != null && holder != closed) {
                      ((ResponseWrapper<ResponseAndStream<F, ResponseWrapper<E>>>) holder)
                          .getData()
                          .dispose();
                    }
                  });
        });
  }

  private static <F, E>
      Mono<ResponseWrapper<ResponseAndStream<F, ResponseWrapper<E>>>> onFirstPayload(
          ClientResponsePayload<?> first, Flux<? extends ClientResponsePayload<?>> rest) {
    final F firstResponse;
    try {
      firstResponse = ResponseAndStreamBridge.extractFirstResponse(first);
    } catch (Throwable t) {
      // extractFirstResponse propagates checked failures wrapped: unwrap so the outer Mono
      // carries the raw throwable, exactly as the pre-split code passed it to Mono.error.
      return Mono.error(Exceptions.unwrap(t));
    }
    StreamLifecycle lifecycle = new StreamLifecycle(rest);
    Flux<ResponseWrapper<E>> inner =
        Flux.defer(
            () ->
                lifecycle.claimForSubscribe()
                    ? ResponseAndStreamBridge.<E>decodeElements(rest)
                        .doOnSubscribe(lifecycle::install)
                        .doFinally(signal -> lifecycle.parkSentinel())
                    : Flux.<ResponseWrapper<E>>error(
                        new IllegalStateException(
                            lifecycle.isDisposed()
                                ? "getStream() was already disposed and the stream abandoned:"
                                    + " to retry, resubscribe the outer Mono"
                                : "getStream() supports one subscription: to retry, resubscribe"
                                    + " the outer Mono; to share one stream, use publish() or"
                                    + " cache()")));
    Disposable onDispose = lifecycle::dispose;
    return Mono.just(
        ResponseWrapper.create(
            ResponseAndStream.of(firstResponse, inner, onDispose),
            first.getHeaders(),
            first.getBinaryHeaders()));
  }

  /**
   * Validates the first payload and returns the first response it carries. Throws the initial error
   * (an outer {@link Mono} error) when the first payload carries an exception, does not carry a
   * first response, or carries a null one.
   */
  @SuppressWarnings("unchecked")
  private static <F> F extractFirstResponse(ClientResponsePayload<?> first) {
    if (first.getException() != null) {
      throw Exceptions.propagate(first.getException());
    }
    Object data = first.getData();
    if (!(data instanceof StreamResponse) || !((StreamResponse<?, ?>) data).isSetFirstResponse()) {
      throw new IllegalStateException("First stream element did not carry the first response");
    }
    F firstResponse = (F) ((StreamResponse<?, ?>) data).getFirstResponse();
    if (firstResponse == null) {
      throw new NullPointerException("the first response must not be null");
    }
    return firstResponse;
  }

  /**
   * Decodes the remaining stream elements (after the held first payload) into per-element {@code
   * ResponseWrapper}s carrying each payload's headers. A header-only frame (a non-first-response
   * {@code StreamResponse} with null data) decodes to a null-data element so its headers still
   * reach the caller. A first response arriving mid-stream is a protocol violation and fails the
   * stream.
   */
  @SuppressWarnings("unchecked")
  private static <E> Flux<ResponseWrapper<E>> decodeElements(
      Flux<? extends ClientResponsePayload<?>> rest) {
    // skip(1) discards the held first payload without requesting further credits: its own
    // discarding request is absorbed by the held payload, so downstream demand still starts
    // from the initial window.
    return rest.skip(1)
        .<ResponseWrapper<E>>handle(
            (element, sink) -> {
              if (element.getException() != null) {
                throw Exceptions.propagate(element.getException());
              }
              Object elementData = element.getData();
              StreamResponse<?, ?> streamResponse =
                  elementData instanceof StreamResponse ? (StreamResponse<?, ?>) elementData : null;
              if (streamResponse == null) {
                return;
              }
              if (streamResponse.isSetFirstResponse()) {
                throw new IllegalStateException(
                    "Stream element carried an unexpected first response");
              }
              sink.next(
                  ResponseWrapper.create(
                      (E) streamResponse.getData(),
                      element.getHeaders(),
                      element.getBinaryHeaders()));
            });
  }

  /**
   * The subscribe/dispose lifecycle of one resolved holder: the claimed gate arbitrates the inner
   * subscription against an early dispose (exactly one wins), and the inner-subscription cell
   * tracks the live subscription behind a cancellation sentinel so a late install cancels itself
   * and disposal after termination is a transport no-op.
   */
  private static final class StreamLifecycle {
    private final Flux<? extends ClientResponsePayload<?>> rest;
    private final AtomicBoolean claimed = new AtomicBoolean(false);
    private final AtomicBoolean disposed = new AtomicBoolean(false);
    private final AtomicReference<Subscription> innerSubscription = new AtomicReference<>();

    StreamLifecycle(Flux<? extends ClientResponsePayload<?>> rest) {
      this.rest = rest;
    }

    /**
     * Claims the single inner subscription. Returns false when the holder was already subscribed or
     * disposed: the caller must fail instead of subscribing.
     */
    boolean claimForSubscribe() {
      return claimed.compareAndSet(false, true);
    }

    /** Whether {@link #dispose()} ran: distinguishes the two claim-failure messages. */
    boolean isDisposed() {
      return disposed.get();
    }

    /**
     * Installs a live inner subscription. A subscription installed after disposal (or after
     * termination) is cancelled immediately: the cell holds the cancellation sentinel then, and the
     * CAS fails. The sentinel itself is an inert cancelled subscription, so even a mistaken use is
     * safe.
     */
    void install(Subscription subscription) {
      if (!innerSubscription.compareAndSet(null, subscription)) {
        subscription.cancel();
      }
    }

    /**
     * Parks the cancellation sentinel in the cell on terminal states (and on cancel): disposing a
     * terminated stream is a transport no-op, and a late install still cancels itself. A cancel
     * parks it in the same pass, so a later dispose cannot re-cancel either.
     */
    void parkSentinel() {
      innerSubscription.set(SENTINEL);
    }

    /**
     * Abandons the stream. When the inner stream was never subscribed, subscribes the remainder
     * with a subscriber that cancels in {@code onSubscribe}: the switch admits it as the
     * remainder's first subscriber (or rejects it if the outer already terminated) and the cancel
     * tears down the live upstream subscription, discarding the held first payload without
     * requesting further credits. When the inner stream won the race, cancels its live subscription
     * when it is still around. The cell yields its value once, so this is exactly-once against a
     * concurrent user cancel; taking the sentinel is a no-op by construction.
     */
    void dispose() {
      disposed.set(true);
      if (claimed.compareAndSet(false, true)) {
        rest.subscribe(CancelOnSubscribe.instance());
      } else {
        Subscription live = innerSubscription.getAndSet(SENTINEL);
        if (live != null) {
          live.cancel();
        }
      }
    }
  }

  /** Cancellation sentinel for the inner-subscription cell: inert, so taking it is a no-op. */
  private static final Subscription SENTINEL = Operators.cancelledSubscription();

  /**
   * Disposes a holder dropped before delivery. The discard filter also matches {@code
   * ClientResponsePayload} (it implements {@code ResponseWrapper}), which upstream operators
   * discard on routine early disposal: only holders carry a {@code ResponseAndStream}, so anything
   * else is ignored instead of cast.
   */
  static void disposeIfDropped(ResponseWrapper<?> holder) {
    if (holder.getData() instanceof ResponseAndStream) {
      ((ResponseAndStream<?, ?>) holder.getData()).dispose();
    }
  }

  /** Stateless subscriber that cancels in {@code onSubscribe}, discarding whatever was held. */
  private enum CancelOnSubscribe implements CoreSubscriber<Object> {
    INSTANCE;

    @SuppressWarnings("unchecked")
    static <T> CoreSubscriber<T> instance() {
      return (CoreSubscriber<T>) INSTANCE;
    }

    @Override
    public void onSubscribe(Subscription s) {
      s.cancel();
    }

    @Override
    public void onNext(Object t) {}

    @Override
    public void onError(Throwable t) {}

    @Override
    public void onComplete() {}
  }
}
