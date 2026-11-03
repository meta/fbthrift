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

package com.facebook.thrift.server.generated;

import static com.facebook.thrift.util.RpcPayloadUtil.declaredExceptionResponseMetadata;
import static com.facebook.thrift.util.RpcPayloadUtil.declaredExceptionStreamMetadata;
import static com.facebook.thrift.util.RpcPayloadUtil.declaredExceptionWriter;
import static com.facebook.thrift.util.RpcPayloadUtil.streamFirstResponseMetadata;
import static com.facebook.thrift.util.RpcPayloadUtil.streamPayloadMetadata;
import static com.facebook.thrift.util.RpcPayloadUtil.undeclaredExceptionResponseMetadata;
import static com.facebook.thrift.util.RpcPayloadUtil.undeclaredExceptionStreamMetadata;
import static com.facebook.thrift.util.RpcPayloadUtil.undeclaredExceptionWriter;
import static com.facebook.thrift.util.RpcPayloadUtil.withPreWriteException;

import com.facebook.swift.service.ContextChain;
import com.facebook.thrift.model.ResponseAndStream;
import com.facebook.thrift.payload.ServerRequestPayload;
import com.facebook.thrift.payload.ServerResponsePayload;
import com.facebook.thrift.payload.ThriftSerializable;
import com.facebook.thrift.payload.Writer;
import com.facebook.thrift.rsocket.server.ThriftServerRSocket;
import com.facebook.thrift.util.Writers;
import com.facebook.thrift.util.resources.RpcResources;
import java.util.List;
import org.apache.thrift.TApplicationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * The untyped shared half of server stream handling: every streaming method is a response phase
 * (read the args, invoke the delegate, write the first payload) followed by a stream phase (write
 * each element, then completion). A plain stream is a first-response stream whose first response is
 * the empty init.
 *
 * <p>Anything before the first payload encodes as the initial error, anything after as a stream
 * error. Credit behavior is concat's: the first payload consumes one downstream credit and the rest
 * forwards to the user flux. Generated code owns demand policy; this class never requests.
 */
public final class ServerStreams {
  private static final Logger logger = LoggerFactory.getLogger(ServerStreams.class);

  /**
   * First-response value for plain streams. The {@link #EMPTY_FIRST_RESPONSE_WRITER} ignores it; it
   * exists only because a first response must never be null.
   */
  public static final Object NO_FIRST_RESPONSE = new Object();

  /** Writes the empty init first response for plain streams. Shared: it is type-free. */
  public static final ResponseWriterFactory EMPTY_FIRST_RESPONSE_WRITER =
      (o, chain, requestPayload) -> Writers.emptyStruct();

  private ServerStreams() {}

  public static <F, E> Flux<ServerResponsePayload> handleStream(
      ServerRequestPayload requestPayload, ContextChain chain, StreamMethod<F, E> method) {
    Flux<ServerResponsePayload> response =
        Mono.defer(
                () -> {
                  final List<Object> data;
                  try {
                    chain.preRead();
                    data = requestPayload.getData(method.readers);
                    chain.postRead(data);
                    return method.invoke.apply(data);
                  } catch (Throwable t) {
                    logger.warn("Can not handle stream", t);
                    return Mono.<ResponseAndStream<F, E>>error(t);
                  } finally {
                    // The request bytes are fully read, so release the buffer before the stream
                    // starts instead of pinning it for the stream lifetime.
                    requestPayload.releaseRequestData();
                  }
                })
            .switchIfEmpty(
                Mono.error(new NullPointerException("the first response must not be null")))
            .flatMapMany(
                responseAndStream ->
                    Flux.concat(
                            Mono.fromCallable(
                                () ->
                                    firstPayload(
                                        requestPayload,
                                        chain,
                                        method,
                                        responseAndStream.getFirstResponse())),
                            streamPhase(
                                requestPayload, chain, method, responseAndStream.getStream()))
                        // A downstream that never consumes the stream must not pin a forwarded
                        // backend stream: dispose the holder. A no-op once the inner stream took
                        // over its own lifecycle.
                        .doFinally(signal -> responseAndStream.dispose()))
            .onErrorResume(t -> Mono.just(initialErrorPayload(requestPayload, chain, method, t)));
    if (chain.hasStreamHandlers()) {
      response =
          response
              .doOnRequest(
                  n -> {
                    if (n > 0) {
                      chain.onStreamCredit(saturateCredits(n));
                    }
                  })
              .doFinally(chain::finishStream);
    }
    // Unconditional so user cancel cleanup never moves with registration.
    return response.cancelOn(ThriftServerRSocket.cancelScheduler());
  }

  private static <F, E> Flux<ServerResponsePayload> streamPhase(
      ServerRequestPayload requestPayload,
      ContextChain chain,
      StreamMethod<F, E> method,
      Flux<E> stream) {
    if (RpcResources.isForceExecutionOffEventLoop()) {
      stream = stream.subscribeOn(RpcResources.getOffLoopScheduler());
    }
    if (chain.hasStreamHandlers()) {
      return stream
          .doOnSubscribe(s -> chain.onStreamSubscribe())
          .map(element -> elementPayload(requestPayload, chain, method, element))
          .doOnNext(payload -> chain.onStreamNext())
          .onErrorResume(
              t -> {
                // The error encodes as a terminal frame completing normally, so fire the error
                // hook now: it always precedes the ERROR terminal.
                chain.handleStreamError(t);
                return Mono.just(streamErrorPayload(requestPayload, chain, method, t));
              });
    }
    return stream
        .map(element -> elementPayload(requestPayload, chain, method, element))
        .onErrorResume(t -> Mono.just(streamErrorPayload(requestPayload, chain, method, t)));
  }

  /** Unbounded demand saturates. */
  private static int saturateCredits(long n) {
    return n >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) n;
  }

  private static <F, E> ServerResponsePayload firstPayload(
      ServerRequestPayload requestPayload, ContextChain chain, StreamMethod<F, E> method, F first) {
    Writer writer =
        method.firstResponseWriterFactory.createResponseWriter(first, chain, requestPayload);
    return ServerResponsePayload.create(writer, streamFirstResponseMetadata(), null, true);
  }

  private static <F, E> ServerResponsePayload elementPayload(
      ServerRequestPayload requestPayload,
      ContextChain chain,
      StreamMethod<F, E> method,
      E element) {
    Writer writer =
        method.responseWriterFactory.createResponseWriter(element, chain, requestPayload);
    return ServerResponsePayload.create(writer, null, streamPayloadMetadata(requestPayload), true);
  }

  /**
   * Whether an exception is declared. Function-level failures check the throws clause, stream
   * failures the stream exceptions.
   *
   * @param checkStreamExceptions true for stream-level, false for function-level
   * @return the field ID if known, -1 if unknown
   */
  private static int isKnownException(
      Throwable t, StreamMethod<?, ?> method, boolean checkStreamExceptions) {
    if (t instanceof ThriftSerializable) {
      Class<? extends Throwable> aClass = t.getClass();
      if (checkStreamExceptions) {
        // We're in the stream - only stream-declared exceptions are known
        for (int i = 0; i < method.streamExceptions.length; i++) {
          if (aClass == method.streamExceptions[i]) {
            return method.streamExceptionIds[i];
          }
        }
      } else {
        // We're at function level - only function-declared exceptions are known
        for (int i = 0; i < method.functionExceptions.length; i++) {
          if (aClass == method.functionExceptions[i]) {
            return method.functionExceptionIds[i];
          }
        }
      }
    }
    return -1;
  }

  private static ServerResponsePayload initialErrorPayload(
      ServerRequestPayload requestPayload,
      ContextChain chain,
      StreamMethod<?, ?> method,
      Throwable t) {
    int fieldId = isKnownException(t, method, false);
    if (fieldId == -1) {
      return unknownErrorPayload(t, method.name, requestPayload, chain, false);
    }
    return knownErrorPayload(t, requestPayload, chain, fieldId, false);
  }

  private static ServerResponsePayload streamErrorPayload(
      ServerRequestPayload requestPayload,
      ContextChain chain,
      StreamMethod<?, ?> method,
      Throwable t) {
    int fieldId = isKnownException(t, method, true);
    if (fieldId == -1) {
      return unknownErrorPayload(t, method.name, requestPayload, chain, true);
    }
    return knownErrorPayload(t, requestPayload, chain, fieldId, true);
  }

  private static ServerResponsePayload knownErrorPayload(
      Throwable throwable,
      ServerRequestPayload requestPayload,
      ContextChain chain,
      int fieldId,
      boolean streamException) {
    Writer knownExceptionWriter = createKnownExceptionWriter(throwable, chain, fieldId);
    if (streamException) {
      return ServerResponsePayload.createWithTApplicationException(
          knownExceptionWriter,
          null,
          declaredExceptionStreamMetadata(requestPayload, throwable),
          true);
    }
    return ServerResponsePayload.createWithTApplicationException(
        knownExceptionWriter,
        declaredExceptionResponseMetadata(requestPayload, throwable),
        null,
        false);
  }

  private static Writer createKnownExceptionWriter(
      Throwable t, ContextChain chain, final int fieldId) {
    // Same order as the request-response and unknown-exception paths: preWriteException
    // fires before the declared/undeclared classification hooks.
    return withPreWriteException(t, chain, declaredExceptionWriter(t, chain, fieldId));
  }

  private static Writer createUnknownExceptionWriter(TApplicationException t, ContextChain chain) {
    return withPreWriteException(t, chain, undeclaredExceptionWriter(t, chain));
  }

  private static ServerResponsePayload unknownErrorPayload(
      Throwable throwable,
      String name,
      ServerRequestPayload requestPayload,
      ContextChain chain,
      boolean streamException) {
    String errorMessage =
        String.format(
            "Internal error processing %s: %s",
            name, throwable.getMessage() == null ? "<null>" : throwable.getMessage());
    TApplicationException exception =
        new TApplicationException(TApplicationException.INTERNAL_ERROR, errorMessage);
    exception.initCause(throwable);

    Writer unknownExceptionWriter = createUnknownExceptionWriter(exception, chain);

    if (streamException) {
      return ServerResponsePayload.createWithTApplicationException(
          unknownExceptionWriter,
          null,
          undeclaredExceptionStreamMetadata(
              requestPayload, throwable.getClass().getName(), throwable.getMessage()),
          true);
    }

    return ServerResponsePayload.createWithTApplicationException(
        unknownExceptionWriter,
        undeclaredExceptionResponseMetadata(
            requestPayload, throwable.getClass().getName(), throwable.getMessage()),
        null,
        false);
  }
}
