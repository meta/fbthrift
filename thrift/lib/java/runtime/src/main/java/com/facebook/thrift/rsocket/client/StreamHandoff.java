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

package com.facebook.thrift.rsocket.client;

import com.facebook.thrift.util.ResponseAndStreamBridge;
import com.google.common.annotations.VisibleForTesting;
import io.netty.util.ReferenceCountUtil;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Function;
import java.util.function.Supplier;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Operators;
import reactor.core.scheduler.Scheduler;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

/**
 * Moves stream frames from the transport thread to the client scheduler. Frames wait in a
 * single-producer single-consumer queue sized to the granted credits (rounded up to a power of
 * two); only the drain-guard holder ({@code wip}) polls, so each frame is decoded or released
 * exactly once even when cancel races an arrival or a drain. A frame arriving after cancel or a
 * terminal signal is released without touching the queue. A scheduler that discards an accepted
 * task (only at shutdown) leaves the frames queued at that moment unreleased. Performs the
 * transport call itself, so raw frames never pass through another operator.
 */
final class StreamHandoff<T> implements CoreSubscriber<Payload> {
  @SuppressWarnings("rawtypes")
  private static final AtomicIntegerFieldUpdater<StreamHandoff> WIP =
      AtomicIntegerFieldUpdater.newUpdater(StreamHandoff.class, "wip");

  @SuppressWarnings("rawtypes")
  private static final AtomicLongFieldUpdater<StreamHandoff> REQUESTED =
      AtomicLongFieldUpdater.newUpdater(StreamHandoff.class, "requested");

  private final FluxSink<T> sink;
  private final Scheduler scheduler;
  private final Function<Payload, T> decode;
  private final Function<Throwable, T> errorValue;
  private final Queue<Payload> queue;
  private final Context context;

  private volatile int wip;
  private volatile long requested;
  private volatile Subscription upstream;
  private volatile boolean cancelled;
  private volatile boolean done;
  private volatile boolean terminated;
  // Written before the volatile done write; read after done is observed.
  private Throwable error;
  // drainLoop only, under wip.
  private int consumed;

  /**
   * Calls request-stream on the transport and hands the raw frames to the scheduler under
   * downstream demand. The request is encoded per subscription; only decoded values reach the
   * caller. The default BUFFER strategy only ever holds the terminal mapped error frame: every
   * normal emission is gated on tracked demand, so the buffer stays empty.
   */
  static <T> Flux<T> requestStream(
      RSocket rsocket,
      Supplier<Payload> request,
      Scheduler scheduler,
      Function<Payload, T> decode,
      Function<Throwable, T> errorValue) {
    return requestStream(
        rsocket, request, scheduler, decode, errorValue, StreamHandoff::defaultQueue);
  }

  /**
   * @param errorValue maps a terminal error to a last value; returns null to deliver the error
   *     unchanged
   */
  @VisibleForTesting
  static <T> Flux<T> requestStream(
      RSocket rsocket,
      Supplier<Payload> request,
      Scheduler scheduler,
      Function<Payload, T> decode,
      Function<Throwable, T> errorValue,
      Supplier<Queue<Payload>> queues) {
    return Flux.create(
        sink -> {
          StreamHandoff<T> handoff =
              new StreamHandoff<>(sink, scheduler, decode, errorValue, queues.get());
          sink.onRequest(handoff::request);
          sink.onDispose(handoff::cancel);

          Payload encoded;

          try {
            encoded = request.get();
          } catch (Throwable t) {
            sink.error(t);

            return;
          }

          try {
            rsocket.requestStream(encoded).subscribe(handoff);
          } catch (Throwable t) {
            ReferenceCountUtil.safeRelease(encoded);
            sink.error(t);
          }
        });
  }

  /**
   * Calls request-channel on the transport and hands the raw frames to the scheduler under
   * downstream demand. Only decoded values reach the caller.
   *
   * @param errorValue maps a terminal error to a last value; returns null to deliver the error
   *     unchanged
   */
  static <T> Flux<T> requestChannel(
      RSocket rsocket,
      Publisher<Payload> outbound,
      Scheduler scheduler,
      Function<Payload, T> decode,
      Function<Throwable, T> errorValue) {
    return Flux.create(
        sink -> {
          StreamHandoff<T> handoff =
              new StreamHandoff<>(sink, scheduler, decode, errorValue, defaultQueue());
          sink.onRequest(handoff::request);
          sink.onDispose(handoff::cancel);

          try {
            rsocket.requestChannel(outbound).subscribe(handoff);
          } catch (Throwable t) {
            sink.error(t);
          }
        });
  }

  private static Queue<Payload> defaultQueue() {
    return Queues.<Payload>get(ResponseAndStreamBridge.INITIAL_CREDITS).get();
  }

  private StreamHandoff(
      FluxSink<T> sink,
      Scheduler scheduler,
      Function<Payload, T> decode,
      Function<Throwable, T> errorValue,
      Queue<Payload> queue) {
    this.sink = sink;
    this.scheduler = scheduler;
    this.decode = decode;
    this.errorValue = errorValue;
    this.queue = queue;
    this.context = Context.of(sink.contextView());
  }

  @Override
  public void onSubscribe(Subscription subscription) {
    upstream = subscription;

    if (cancelled) {
      subscription.cancel();

      return;
    }

    subscription.request(ResponseAndStreamBridge.INITIAL_CREDITS);
  }

  @Override
  public void onNext(Payload frame) {
    if (cancelled || terminated) {
      ReferenceCountUtil.safeRelease(frame);

      return;
    }

    if (!queue.offer(frame)) {
      ReferenceCountUtil.safeRelease(frame);
      fail(Exceptions.failWithOverflow("Peer ignored flow control: more frames than granted"));

      return;
    }

    drain();
  }

  @Override
  public void onError(Throwable t) {
    error = t;
    done = true;
    drain();
  }

  @Override
  public void onComplete() {
    done = true;
    drain();
  }

  void request(long n) {
    Operators.addCap(REQUESTED, this, n);
    drain();
  }

  void cancel() {
    cancelled = true;

    cancelUpstream();

    // Only the wip holder polls: a running or scheduled drain observes cancelled and releases.
    drain();
  }

  private void cancelUpstream() {
    Subscription current = upstream;

    if (current != null) {
      current.cancel();
    }
  }

  @Override
  public Context currentContext() {
    return context;
  }

  private void drain() {
    if (WIP.getAndIncrement(this) != 0) {
      return;
    }

    try {
      scheduler.schedule(this::drainLoop);
    } catch (Throwable rejected) {
      cancelled = true;

      cancelUpstream();

      // No drain runs from here: release everything the missed signals left behind, then give
      // wip back one round at a time so a frame arriving with the cleanup is not stranded.
      int missed = 1;

      for (; ; ) {
        releaseQueued();
        missed = WIP.addAndGet(this, -missed);

        if (missed == 0) {
          break;
        }
      }

      if (!terminated) {
        terminated = true;
        sink.error(error != null ? Exceptions.addSuppressed(rejected, error) : rejected);
      }
    }
  }

  private void drainLoop() {
    int missed = 1;

    for (; ; ) {
      if (cancelled) {
        releaseQueued();
      } else {
        long demand = requested;
        long emitted = 0;

        while (emitted != demand) {
          if (cancelled || terminated) {
            releaseQueued();
            break;
          }

          Payload frame = queue.poll();

          if (frame == null) {
            break;
          }

          emit(frame);
          emitted++;

          refillIfDue();
        }

        if (demand != Long.MAX_VALUE && emitted != 0) {
          Operators.produced(REQUESTED, this, emitted);
        }

        if (done && queue.isEmpty()) {
          finish();
        }
      }

      missed = WIP.addAndGet(this, -missed);

      if (missed == 0) {
        return;
      }
    }
  }

  private void refillIfDue() {
    if (++consumed != ResponseAndStreamBridge.REFILL_CREDITS) {
      return;
    }

    consumed = 0;

    Subscription current = upstream;

    if (current != null && !cancelled) {
      current.request(ResponseAndStreamBridge.REFILL_CREDITS);
    }
  }

  private void emit(Payload frame) {
    T decoded;

    try {
      decoded = decode.apply(frame);
    } catch (Throwable t) {
      fail(t);
      return;
    }

    try {
      sink.next(decoded);
    } catch (Throwable t) {
      fail(t);
    }
  }

  private void fail(Throwable t) {
    if (terminated) {
      return;
    }

    terminated = true;
    cancel();
    sink.error(t);
  }

  private void finish() {
    if (terminated) {
      return;
    }

    terminated = true;
    Throwable failure = error;

    if (failure == null) {
      sink.complete();
      return;
    }

    T value;

    try {
      value = errorValue.apply(failure);
    } catch (Throwable applyError) {
      sink.error(Exceptions.addSuppressed(applyError, failure));

      return;
    }

    if (value == null) {
      sink.error(failure);
    } else {
      sink.next(value);
      sink.complete();
    }
  }

  private void releaseQueued() {
    Payload frame;

    while ((frame = queue.poll()) != null) {
      ReferenceCountUtil.safeRelease(frame);
    }
  }
}
