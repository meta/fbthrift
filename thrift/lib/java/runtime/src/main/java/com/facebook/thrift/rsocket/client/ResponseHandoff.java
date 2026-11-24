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

import io.netty.util.ReferenceCountUtil;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import java.util.function.Function;
import java.util.function.Supplier;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.scheduler.Scheduler;
import reactor.util.context.Context;

/**
 * Moves one response from the transport thread to the client scheduler without putting the buffer
 * in a Reactor queue. The call can end before or after the buffer arrives, so the handle owns the
 * buffer until decode claims it. One scheduler.schedule per response; decoding stays a plain
 * function at the call site. Performs the transport call itself, so the raw response never passes
 * through another operator.
 */
final class ResponseHandoff<T> implements CoreSubscriber<Payload> {
  private final MonoSink<T> sink;
  private final Scheduler scheduler;
  private final Function<Payload, T> decode;
  private final Function<Throwable, T> errorValue;
  private final ResponseHandle handle = new ResponseHandle();
  private final Context context;

  private volatile Subscription upstream;
  private volatile boolean cancelled;
  private volatile boolean received;

  /**
   * Calls request-response on the transport and hands the single raw response to the scheduler.
   * Only decoded values reach the caller. {@code request} runs once per subscription.
   */
  static <T> Mono<T> requestResponse(
      RSocket rsocket,
      Supplier<Payload> request,
      Scheduler scheduler,
      Function<Payload, T> decode,
      Function<Throwable, T> errorValue) {
    return Mono.create(
        sink -> {
          ResponseHandoff<T> handoff = new ResponseHandoff<>(sink, scheduler, decode, errorValue);
          sink.onDispose(handoff::cancel);

          Payload encoded;

          try {
            encoded = request.get();
          } catch (Throwable t) {
            sink.error(t);

            return;
          }

          try {
            rsocket.requestResponse(encoded).subscribe(handoff);
          } catch (Throwable t) {
            ReferenceCountUtil.safeRelease(encoded);
            sink.error(t);
          }
        });
  }

  private ResponseHandoff(
      MonoSink<T> sink,
      Scheduler scheduler,
      Function<Payload, T> decode,
      Function<Throwable, T> errorValue) {
    this.sink = sink;
    this.scheduler = scheduler;
    this.decode = decode;
    this.errorValue = errorValue;
    this.context = Context.of(sink.contextView());
  }

  @Override
  public void onSubscribe(Subscription subscription) {
    upstream = subscription;

    // A cancel from before the subscription lets rsocket free the unsent request instead of
    // sending it and then cancelling.
    if (cancelled) {
      subscription.cancel();

      return;
    }

    subscription.request(1);
  }

  @Override
  public void onNext(Payload response) {
    received = true;
    handle.offer(response);
    schedule(this::decodeOffLoop);
  }

  @Override
  public void onError(Throwable t) {
    scheduleError(t);
  }

  @Override
  public void onComplete() {
    if (!received) {
      schedule(this::deliverEmpty);
    }
  }

  void cancel() {
    cancelled = true;

    cancelUpstream();
    handle.close();
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

  private void decodeOffLoop() {
    Payload owned = handle.claim();

    if (owned == null) {
      return;
    }

    try {
      sink.success(decode.apply(owned));
    } catch (Throwable t) {
      sink.error(t);
    }
  }

  private void deliverError(Throwable t) {
    // A cancel or timeout that lands while this task waits must not signal the disposed sink.
    if (cancelled) {
      return;
    }

    T value;

    try {
      value = errorValue.apply(t);
    } catch (Throwable applyError) {
      sink.error(Exceptions.addSuppressed(applyError, t));

      return;
    }

    if (value != null) {
      sink.success(value);
    } else {
      sink.error(t);
    }
  }

  private void deliverEmpty() {
    // Same late-cancel guard as deliverError: never signal a disposed sink.
    if (cancelled) {
      return;
    }

    sink.success();
  }

  private void schedule(Runnable task) {
    try {
      scheduler.schedule(task);
    } catch (Throwable rejected) {
      handle.close();
      sink.error(rejected);
    }
  }

  private void scheduleError(Throwable t) {
    try {
      scheduler.schedule(() -> deliverError(t));
    } catch (Throwable rejected) {
      handle.close();
      sink.error(Exceptions.addSuppressed(rejected, t));
    }
  }
}
