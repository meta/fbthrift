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

package com.facebook.thrift.server.sink;

import com.facebook.swift.service.ContextChain;
import com.facebook.swift.service.StreamEventHandler;
import com.facebook.thrift.model.ResponseAndStream;
import com.facebook.thrift.model.StreamResponse;
import com.facebook.thrift.payload.FieldReader;
import com.facebook.thrift.payload.ServerRequestPayload;
import com.facebook.thrift.payload.ServerResponsePayload;
import com.facebook.thrift.rsocket.server.sink.SinkPayloadSubscriber;
import com.facebook.thrift.server.stream.ServerStreams;
import com.facebook.thrift.server.stream.StreamMethod;
import java.util.List;
import java.util.function.LongConsumer;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

/**
 * Serves generated sink methods. A sink reads its sink elements and answers with one final
 * response, so the final takes the place of a stream's elements. A sink fires only its sink hooks,
 * so that event handlers do not count it as a stream.
 */
public final class ServerSinks {
  private ServerSinks() {}

  /**
   * Serves a sink call: the delegate reads the sink elements and returns exactly one final
   * response. The transport starts the sink after the first response and ends it with the call.
   */
  public static <F, R, U> Flux<ServerResponsePayload> handleSink(
      ServerRequestPayload requestPayload,
      ContextChain chain,
      SinkMethod<F, R, U> method,
      Publisher<ServerRequestPayload> sinkPayloads) {
    boolean hooks = chain.hasStreamHandlers();

    Flux<U> sinkElements =
        Flux.from(sinkPayloads)
            .map(payload -> ServerSinks.<U>readSinkElement(payload, method.sinkReaders));
    if (hooks) {
      sinkElements = withSinkHooks(sinkElements, chain);
    }

    StreamMethod<F, R> stream =
        method.asStreamMethod(
            sinkElements,
            finalResponse -> {
              Flux<R> single = finalResponse.single().flux();
              return hooks ? withFinalHooks(single, chain) : single;
            });

    Flux<ServerResponsePayload> call =
        ServerStreams.handleStream(requestPayload, chain, stream, false);

    // The stream dispatch moves a cancel to another thread on its way up, so the hooks see it here,
    // before the dispatcher's finishRequest fires done().
    return hooks ? call.doOnCancel(() -> endWithCancel(chain)) : call;
  }

  /**
   * Splits the results of a sink with a first response into that response and the final one. The
   * final waits for sink elements that flow only after the first response leaves, so one replayed
   * subscription, in the context of the caller, feeds both halves until the holder is disposed. The
   * replay keeps three results, so an extra final makes the final an error.
   */
  public static <F, R> Mono<ResponseAndStream<F, R>> firstAndFinal(
      Flux<StreamResponse<F, R>> results) {
    return Mono.deferContextual(
        context -> {
          Disposable.Swap connection = Disposables.swap();
          Flux<StreamResponse<F, R>> shared =
              results.contextWrite(context).replay(3).autoConnect(1, connection::update);

          return shared
              .next()
              .map(
                  first ->
                      ResponseAndStream.of(
                          first.getFirstResponse(),
                          shared.skip(1).map(StreamResponse::getData),
                          connection))
              .doOnError(e -> connection.dispose())
              .doOnCancel(connection::dispose);
        });
  }

  /**
   * Returns the sink element in a sink payload, or throws the declared sink exception that the peer
   * sent in its place.
   *
   * <p>Runtime-internal: read by the bidi server package for its client-to-server half.
   */
  @SuppressWarnings("unchecked")
  public static <U> U readSinkElement(
      ServerRequestPayload sinkPayload, List<FieldReader<?>> sinkReaders) {
    List<Object> fields = sinkPayload.getData(sinkReaders);
    for (int i = 1; i < fields.size(); i++) {
      if (fields.get(i) != null) {
        throw Exceptions.propagate((Throwable) fields.get(i));
      }
    }

    return (U) fields.get(0);
  }

  /**
   * Attaches the sink hooks: subscribe/next/consumed, a peer sink error before the delegate sees
   * it, and the credit the transport grants the peer. Only a sink dispatch sets the credit
   * listener, so a bidi sink half stays hook-free.
   */
  private static <U> Flux<U> withSinkHooks(Flux<U> sinkElements, ContextChain chain) {
    LongConsumer credit = n -> chain.onSinkCredit(saturateCredits(n));

    return sinkElements
        .doOnSubscribe(s -> chain.onSinkSubscribe())
        .doOnNext(
            element -> {
              chain.onSinkNext();
              chain.onSinkConsumed();
            })
        .doOnError(
            t -> {
              chain.handleSinkError(t);
              chain.onSinkFinally(StreamEventHandler.SinkOutcome.ERROR);
            })
        .contextWrite(Context.of(SinkPayloadSubscriber.SINK_CREDIT_LISTENER, credit));
  }

  /** Unbounded demand saturates. */
  private static int saturateCredits(long n) {
    return n >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) n;
  }

  /**
   * Otherwise the final response ends a sink. The hooks see it before the final leaves, so they run
   * before the request completes.
   */
  private static <R> Flux<R> withFinalHooks(Flux<R> finalResponse, ContextChain chain) {
    return finalResponse
        .doOnNext(response -> chain.onSinkFinally(StreamEventHandler.SinkOutcome.COMPLETE))
        .doOnError(
            t -> {
              chain.handleSinkError(t);
              chain.onSinkFinally(StreamEventHandler.SinkOutcome.COMPLETE_WITH_ERROR);
            });
  }

  /** A call that ends with a cancel, before its final, ends the sink with an error. */
  private static void endWithCancel(ContextChain chain) {
    chain.onSinkCancel();
    chain.onSinkFinally(StreamEventHandler.SinkOutcome.ERROR);
  }
}
