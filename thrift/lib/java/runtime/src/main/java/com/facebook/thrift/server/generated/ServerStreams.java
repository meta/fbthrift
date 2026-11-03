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
 * the empty init, written by {@link #EMPTY_FIRST_RESPONSE_WRITER} for the {@link
 * #NO_FIRST_RESPONSE} sentinel.
 *
 * <p>Error scope follows from where the error happens, with no flags: anything before the first
 * payload is emitted encodes as the initial error (checked against the function-declared
 * exceptions), anything after as a stream error (checked against the stream-declared exceptions).
 * An empty invoke {@link Mono} fails like a null first response.
 *
 * <p>Credit behavior is concat's: the first payload consumes one downstream credit and the rest of
 * the demand forwards to the user's {@link Flux}. Generated code owns demand policy; this class
 * never requests.
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
    return Mono.defer(
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
                // getData has fully read the request bytes, so the request buffer is released
                // here, after invoke returns and before the stream starts. This is the streaming
                // analog of the eager release the generated request-response/oneway handlers do
                // right after their read -- it keeps the request buffer from being pinned for the
                // entire stream lifetime. Idempotent, and a no-op for payloads that carry no
                // owned buffer.
                requestPayload.releaseRequestData();
              }
            })
        .switchIfEmpty(Mono.error(new NullPointerException("the first response must not be null")))
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
                        streamPhase(requestPayload, chain, method, responseAndStream.getStream()))
                    // The downstream may never consume the stream (cancelled before the first
                    // request, or a throw while building the first payload): dispose the holder
                    // so a forwarded backend stream is torn down. A no-op after completion or
                    // once the inner stream took over its own lifecycle.
                    .doFinally(signal -> responseAndStream.dispose()))
        .onErrorResume(t -> Mono.just(initialErrorPayload(requestPayload, chain, method, t)));
  }

  private static <F, E> Flux<ServerResponsePayload> streamPhase(
      ServerRequestPayload requestPayload,
      ContextChain chain,
      StreamMethod<F, E> method,
      Flux<E> stream) {
    if (RpcResources.isForceExecutionOffEventLoop()) {
      stream = stream.subscribeOn(RpcResources.getOffLoopScheduler());
    }
    return stream
        .map(element -> elementPayload(requestPayload, chain, method, element))
        .onErrorResume(t -> Mono.just(streamErrorPayload(requestPayload, chain, method, t)));
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
   * Check if an exception is a known (declared) exception.
   *
   * <p>Per Thrift IDL specification: - Function-level exceptions (throws clause): only declared at
   * initial response level - Stream-level exceptions (stream<T throws>): only declared within the
   * stream
   *
   * @param t the exception to check
   * @param checkStreamExceptions if true, check stream exceptions; if false, check function
   *     exceptions
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
