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

import com.facebook.thrift.util.resources.RpcResources;
import com.google.common.annotations.VisibleForTesting;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.scheduler.Scheduler;

/**
 * Switches a stream to a fallback when it sends no first element in time, like the task timeout of
 * a request-response call. The time starts when the source sends onSubscribe and stops at the first
 * element, at a terminal or at a cancel, because a stream can be idle after its first response. The
 * given scheduler runs the expiry; the timer thread runs it only when the scheduler rejects it.
 *
 * @param <T> the element type
 */
public final class FluxFirstResponseTimeoutTransformer<T> implements Function<Flux<T>, Flux<T>> {
  private final Scheduler scheduler;
  private final long delay;
  private final TimeUnit unit;
  private final Publisher<? extends T> fallback;
  private final Supplier<Timer> timerSupplier;

  public FluxFirstResponseTimeoutTransformer(
      Scheduler scheduler, long delay, TimeUnit unit, Publisher<? extends T> fallback) {
    this(scheduler, delay, unit, fallback, RpcResources::getHashedWheelTimer);
  }

  /** The timer comes from a supplier at subscribe time, so a restart of the resources applies. */
  @VisibleForTesting
  FluxFirstResponseTimeoutTransformer(
      Scheduler scheduler,
      long delay,
      TimeUnit unit,
      Publisher<? extends T> fallback,
      Supplier<Timer> timerSupplier) {
    this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    if (delay < 0) {
      throw new IllegalArgumentException("delay must be non-negative, was: " + delay);
    }

    this.delay = delay;
    this.unit = Objects.requireNonNull(unit, "unit");
    this.fallback = Objects.requireNonNull(fallback, "fallback");
    this.timerSupplier = Objects.requireNonNull(timerSupplier, "timerSupplier");
  }

  /**
   * Reactor's timeout operator lets only one of the first element and the expiry win, and on expiry
   * it cancels the source once before it subscribes the fallback.
   */
  @Override
  public Flux<T> apply(Flux<T> source) {
    return source.timeout(expiry(), element -> Mono.never(), fallback);
  }

  /** Completes on the scheduler when the timer expires. A cancel stops the timer. */
  private Mono<Void> expiry() {
    return Mono.create(
        sink -> {
          Timeout timeout = timerSupplier.get().newTimeout(ignored -> offload(sink), delay, unit);

          sink.onCancel(timeout::cancel);
        });
  }

  /**
   * A scheduler that rejects the expiry leaves it on the timer thread. The handler already ran, so
   * the call must end with the timeout and not with an overload.
   */
  private void offload(MonoSink<Void> sink) {
    try {
      scheduler.schedule(sink::success);
    } catch (RejectedExecutionException e) {
      sink.success();
    }
  }
}
