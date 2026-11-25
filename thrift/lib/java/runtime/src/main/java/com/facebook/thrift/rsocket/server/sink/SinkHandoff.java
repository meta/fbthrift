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

package com.facebook.thrift.rsocket.server.sink;

import io.netty.util.ReferenceCountUtil;
import io.rsocket.Payload;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
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
 * Moves sink payloads from the transport thread to the reader's scheduler through an owned queue
 * bounded by the credit window; only the drain-guard holder polls, so each payload is delivered or
 * released exactly once even when cancel races an arrival. Cancel releases inline under the guard
 * when no drain is outstanding, and a rejected drain releases the same way, so a rejected drain
 * strands nothing. A drain the scheduler accepts and then drops only strands at scheduler shutdown.
 */
final class SinkHandoff implements CoreSubscriber<Payload> {
  @SuppressWarnings("rawtypes")
  private static final AtomicIntegerFieldUpdater<SinkHandoff> WIP =
      AtomicIntegerFieldUpdater.newUpdater(SinkHandoff.class, "wip");

  @SuppressWarnings("rawtypes")
  private static final AtomicLongFieldUpdater<SinkHandoff> REQUESTED =
      AtomicLongFieldUpdater.newUpdater(SinkHandoff.class, "requested");

  private final AtomicBoolean terminated = new AtomicBoolean();

  private final FluxSink<Payload> sink;
  private final Scheduler scheduler;
  private final int prefetch;
  private final int refill;
  private final Queue<Payload> queue;
  private final Context context;

  private volatile int wip;
  private volatile long requested;
  private volatile Subscription upstream;
  private volatile boolean cancelled;
  private volatile boolean done;
  // Written before the volatile done write; read after done is observed.
  private Throwable error;
  // drainLoop only, under wip.
  private int consumed;

  /**
   * Reads the delayed payloads through an owned window on the scheduler: the reader's demand gates
   * delivery, and the window refills from upstream as the reader reads. The caller keeps the
   * payloads it receives, like any other subscription to the sink.
   */
  static Flux<Payload> read(Flux<Payload> delayed, Scheduler scheduler, int prefetch) {
    return Flux.create(
        sink -> {
          SinkHandoff handoff = new SinkHandoff(sink, scheduler, prefetch);

          sink.onRequest(handoff::request);
          sink.onCancel(handoff::cancel);

          delayed.subscribe(handoff);
        });
  }

  private SinkHandoff(FluxSink<Payload> sink, Scheduler scheduler, int prefetch) {
    this.sink = sink;
    this.scheduler = scheduler;
    this.prefetch = prefetch;
    this.refill = prefetch - (prefetch >> 2);
    this.queue = Queues.<Payload>get(Queues.ceilingNextPowerOfTwo(prefetch)).get();
    this.context = Context.of(sink.contextView());
  }

  @Override
  public void onSubscribe(Subscription subscription) {
    upstream = subscription;

    if (cancelled) {
      subscription.cancel();

      return;
    }

    subscription.request(prefetch);
  }

  @Override
  public void onNext(Payload payload) {
    if (cancelled || terminated.get()) {
      ReferenceCountUtil.safeRelease(payload);

      return;
    }

    if (!queue.offer(payload)) {
      ReferenceCountUtil.safeRelease(payload);
      fail(
          Exceptions.failWithOverflow(
              "Peer ignored flow control: more sink payloads than granted"));

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

    // Never schedule from here: with no drain outstanding this releases inline without racing
    // one, and an outstanding drain observes cancelled and releases.
    if (WIP.getAndIncrement(this) != 0) {
      return;
    }

    int missed = 1;

    for (; ; ) {
      releaseQueued();
      missed = WIP.addAndGet(this, -missed);

      if (missed == 0) {
        return;
      }
    }
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

      error = error != null ? Exceptions.addSuppressed(rejected, error) : rejected;
      done = true;

      // Claim the terminal before relinquishing wip, so only one path signals the reader.
      boolean signal = terminated.compareAndSet(false, true);

      // No drain runs from here: release everything the missed signals left behind, then give
      // wip back one round at a time so a payload arriving with the cleanup is not stranded.
      int missed = 1;

      for (; ; ) {
        releaseQueued();
        missed = WIP.addAndGet(this, -missed);

        if (missed == 0) {
          break;
        }
      }

      if (signal) {
        sink.error(error);
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
          if (cancelled || terminated.get()) {
            releaseQueued();
            break;
          }

          Payload payload = queue.poll();

          if (payload == null) {
            break;
          }

          sink.next(payload);
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
    if (++consumed != refill) {
      return;
    }

    consumed = 0;

    Subscription current = upstream;

    if (current != null && !cancelled) {
      current.request(refill);
    }
  }

  private void fail(Throwable t) {
    if (!terminated.compareAndSet(false, true)) {
      return;
    }

    cancel();
    sink.error(t);
  }

  private void finish() {
    if (!terminated.compareAndSet(false, true)) {
      return;
    }

    Throwable failure = error;

    if (failure == null) {
      sink.complete();
    } else {
      sink.error(failure);
    }
  }

  private void releaseQueued() {
    Payload payload;

    while ((payload = queue.poll()) != null) {
      ReferenceCountUtil.safeRelease(payload);
    }
  }
}
