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

package com.facebook.thrift.rsocket.server;

import io.netty.util.ReferenceCountUtil;
import io.rsocket.Payload;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.util.context.Context;

/**
 * The one subscriber to a channel's inbound payloads. It takes the first payload as the request
 * without a REQUEST_N, buffers the rest as sink payloads, and gives the reader a bounded credit
 * window that opens after the first response. It also ends the reader with the call, because only
 * the transport sees how the call ends.
 */
final class SinkPayloadSubscriber extends BaseSubscriber<Payload> {
  /**
   * Unread sink payloads one call may hold: it bounds the memory a fast peer fills behind a slow
   * reader.
   */
  private static final int WINDOW = 100;

  // Only responder signals emit here, so the emits stay serial.
  private final Sinks.Many<Payload> buffer = Sinks.many().unicast().onBackpressureBuffer();

  // Only the first response emits here and only the end of the call emits on callEnd, so each sink
  // has one emitter and no emit can fail on a race.
  private final Sinks.Empty<Void> firstResponse = Sinks.empty();
  private final Sinks.Empty<Void> callEnd = Sinks.empty();

  // Set before callEnd fires, so a reader that the end of the call stops can tell how it ended.
  private volatile boolean aborted;

  private final AtomicInteger responsesSent = new AtomicInteger();

  private final Scheduler scheduler;

  private Payload setup;

  /**
   * The scheduler runs the decoding of the sink payloads: the off-loop scheduler when execution is
   * forced off the event loop, else {@code Schedulers.immediate()}.
   */
  SinkPayloadSubscriber(Scheduler scheduler) {
    this.scheduler = scheduler;
  }

  /** The first payload, which the subscribe call delivers before it returns. */
  Payload setup() {
    return setup;
  }

  /** Counts a response that passed the task timeout on its way to the peer. */
  void responseSent() {
    responsesSent.incrementAndGet();
  }

  int responsesSent() {
    return responsesSent.get();
  }

  @Override
  protected void hookOnSubscribe(Subscription subscription) {
    subscription.request(1);
  }

  @Override
  protected void hookOnNext(Payload payload) {
    if (setup == null) {
      setup = payload;
    } else if (buffer.tryEmitNext(payload).isFailure()) {
      payload.release();
    }
  }

  @Override
  protected void hookOnComplete() {
    buffer.tryEmitComplete();
  }

  @Override
  protected void hookOnError(Throwable t) {
    buffer.tryEmitError(t);
  }

  /**
   * Returns the sink payloads of a sink call. A reader cancel stays local, because a sink peer
   * rejects a CANCEL before the final response; the end of the call cancels the sink.
   */
  Flux<Payload> sinkPayloads() {
    return window(buffer.asFlux().doOnRequest(this::request));
  }

  /** Returns the sink payloads of a bidi call: a reader cancel after the first response ends it. */
  Flux<Payload> bidiSinkPayloads() {
    return window(buffer.asFlux().doOnRequest(this::request).doOnCancel(this::cancel));
  }

  /**
   * Holds the reader's demand and cancel until the first response is queued, then gives credits in
   * a window that refills as the reader reads, on the scheduler of this subscriber. The end of the
   * call cancels the window, which releases what it holds, and then fails the reader unless the
   * call ended normally; only a completing other source makes takeUntilOther cancel its source.
   */
  private Flux<Payload> window(Flux<Payload> sinkPayloads) {
    return sinkPayloads
        .delaySubscription(firstResponse.asMono())
        .publishOn(scheduler, WINDOW)
        .takeUntilOther(callEnd.asMono())
        .concatWith(
            Mono.defer(
                () ->
                    aborted
                        ? Mono.error(new CancellationException("The call ended before its sink"))
                        : Mono.empty()))
        .doOnDiscard(Payload.class, ReferenceCountUtil::safeRelease);
  }

  /**
   * Starts the sink after the responder queued the first response. The responder queues a payload
   * inside onNext, so credit or a cancel after onNext returns follows that payload on any thread.
   * Reactor has no operator that runs after onNext, so a pass-through subscriber does it.
   */
  Flux<Payload> startSinkAfterFirstResponse(Flux<Payload> responses) {
    Function<? super Publisher<Payload>, ? extends Publisher<Payload>> signal =
        Operators.lift((scannable, actual) -> new FirstResponseSignal(actual));

    return Flux.from(signal.apply(responses));
  }

  /**
   * Ends the subscription and releases the sink payloads that no reader took. A normal end of the
   * call completes the reader; any other end fails it, so a handler never takes a cut sink for a
   * whole one.
   */
  void end(SignalType signal) {
    dispose();

    aborted = signal != SignalType.ON_COMPLETE;
    callEnd.tryEmitEmpty();

    // The drain releases the sink payloads that no reader took. It fails at once when a reader took
    // the buffer: the end of the call then cancels that reader's window, which releases its
    // payloads, and a later sink payload finds the buffer cancelled and is released on arrival.
    buffer.asFlux().onErrorComplete().subscribe(ReferenceCountUtil::safeRelease);
  }

  /**
   * Ends the sink of a completed bidi call when no reader took it. A reader keeps the sink open
   * until the peer ends it or the reader cancels; a later reader fails.
   */
  void endIfUnread() {
    if (buffer.currentSubscriberCount() == 0) {
      end(SignalType.CANCEL);
    }
  }

  /** Passes every signal through and starts the sink after the first onNext returned. */
  private final class FirstResponseSignal implements CoreSubscriber<Payload>, Subscription {
    private final CoreSubscriber<? super Payload> actual;
    private Subscription upstream;
    private boolean sent;

    FirstResponseSignal(CoreSubscriber<? super Payload> actual) {
      this.actual = actual;
    }

    @Override
    public void onSubscribe(Subscription subscription) {
      upstream = subscription;
      actual.onSubscribe(this);
    }

    @Override
    public void onNext(Payload payload) {
      actual.onNext(payload);

      if (!sent) {
        sent = true;
        firstResponse.tryEmitEmpty();
      }
    }

    @Override
    public void onError(Throwable t) {
      actual.onError(t);
    }

    @Override
    public void onComplete() {
      actual.onComplete();
    }

    @Override
    public void request(long n) {
      upstream.request(n);
    }

    @Override
    public void cancel() {
      upstream.cancel();
    }

    @Override
    public Context currentContext() {
      return actual.currentContext();
    }
  }
}
