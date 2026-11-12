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

import static com.facebook.thrift.rsocket.util.PayloadUtil.createPayload;
import static com.facebook.thrift.rsocket.util.RocketErrorUtil.fromApplicationException;
import static com.facebook.thrift.rsocket.util.RocketErrorUtil.serverError;
import static com.facebook.thrift.rsocket.util.RocketErrorUtil.taskExpired;

import com.facebook.nifty.core.ConnectionContext;
import com.facebook.nifty.core.NiftyConnectionContext;
import com.facebook.nifty.core.RequestContext;
import com.facebook.swift.service.ThriftServerConfig;
import com.facebook.thrift.compression.CompressionManager;
import com.facebook.thrift.compression.ThriftCompressor;
import com.facebook.thrift.payload.RequestData;
import com.facebook.thrift.payload.ServerRequestPayload;
import com.facebook.thrift.payload.ServerResponsePayload;
import com.facebook.thrift.payload.Writer;
import com.facebook.thrift.protocol.ByteBufTProtocol;
import com.facebook.thrift.protocol.ProtocolUtil;
import com.facebook.thrift.protocol.TProtocolType;
import com.facebook.thrift.server.OverloadedException;
import com.facebook.thrift.server.RpcServerHandler;
import com.facebook.thrift.server.UnknownMethodException;
import com.facebook.thrift.util.MonoTimeoutTransformer;
import com.facebook.thrift.util.NettyNiftyRequestContext;
import com.facebook.thrift.util.RpcServerUtils;
import com.facebook.thrift.util.resources.RpcResources;
import io.airlift.units.Duration;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.ClientAuth;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketErrorException;
import io.rsocket.frame.ErrorFrameCodec;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.thrift.CompressionAlgorithm;
import org.apache.thrift.RequestRpcMetadata;
import org.apache.thrift.ResponseRpcErrorCode;
import org.apache.thrift.RpcKind;
import org.apache.thrift.TApplicationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class ThriftServerRSocket implements RSocket {
  private static final Logger LOGGER = LoggerFactory.getLogger(ThriftServerRSocket.class);

  private static final ThriftServerConfig DEFAULT_CONFIG =
      new ThriftServerConfig().setClientAuth(ClientAuth.OPTIONAL);

  private final RpcServerHandler rpcServerHandler;
  private final ByteBufAllocator alloc;
  private final ConnectionContext connectionContext;
  private final Duration taskExpirationTimeout;

  public ThriftServerRSocket(RpcServerHandler rpcServerHandler, ByteBufAllocator alloc) {
    this(rpcServerHandler, alloc, null);
  }

  public ThriftServerRSocket(
      RpcServerHandler rpcServerHandler,
      ByteBufAllocator alloc,
      ConnectionContext connectionContext) {
    this(rpcServerHandler, alloc, connectionContext, DEFAULT_CONFIG.getTaskExpirationTimeout());
  }

  public ThriftServerRSocket(
      RpcServerHandler rpcServerHandler,
      ByteBufAllocator alloc,
      ConnectionContext connectionContext,
      Duration taskExpirationTimeout) {
    this.rpcServerHandler = rpcServerHandler;
    this.alloc = alloc;
    this.connectionContext =
        connectionContext != null ? connectionContext : new NiftyConnectionContext();
    this.taskExpirationTimeout = taskExpirationTimeout;
  }

  /**
   * Note the one place this diverges from the C++ server. When the task timeout fires, {@link
   * MonoTimeoutTransformer} cancels the handler subscription; C++ sends the error and lets the
   * handler run to completion, then discards its result. Cancellation is best effort: a handler
   * that blocks keeps its thread until it returns, so a blocking handler should be written to be
   * cancellable if it must stop early.
   */
  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    final RequestRpcMetadata requestRpcMetadata;
    try {
      requestRpcMetadata = decodeRequestRpcMetadata(payload);
    } catch (Throwable t) {
      ReferenceCountUtil.safeRelease(payload);

      LOGGER.warn("Invalid request metadata; sending REQUEST_PARSING_FAILURE", t);

      return Mono.error(
          serverError(ResponseRpcErrorCode.REQUEST_PARSING_FAILURE, "Invalid metadata object"));
    }

    try {
      checkRequest(requestRpcMetadata, RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE);
    } catch (Throwable t) {
      ReferenceCountUtil.safeRelease(payload);

      LOGGER.warn(
          "Invalid request for method {}; sending REQUEST_PARSING_FAILURE",
          requestRpcMetadata.getName(),
          t);

      return Mono.error(t);
    }

    RequestContext requestContext = createRequestContext(requestRpcMetadata.getOtherMetadata());

    ByteBuf data;
    try {
      data = maybeDecompressRequestData(requestRpcMetadata, payload);
    } catch (Throwable t) {
      ReferenceCountUtil.safeRelease(payload);

      LOGGER.warn(
          "Decompression failure for method {}; sending REQUEST_PARSING_FAILURE",
          requestRpcMetadata.getName(),
          t);

      return Mono.error(
          serverError(
              ResponseRpcErrorCode.REQUEST_PARSING_FAILURE,
              "decompression failure: " + nonNullMessage(t)));
    }

    // The decoded request data is now owned independently of the RSocket Payload (retained slice
    // in the uncompressed case, freshly allocated in the compressed case). Release the Payload
    // immediately; the request buffer alone goes on the wire as the ServerRequestPayload's owned
    // buffer.
    ReferenceCountUtil.safeRelease(payload);
    payload = null;

    final ServerRequestPayload requestPayload;
    try {
      requestPayload = deserializeRequest(data, requestRpcMetadata, requestContext, data);
    } catch (Throwable t) {
      ReferenceCountUtil.safeRelease(data);

      LOGGER.warn(
          "Invalid request for method {}; sending REQUEST_PARSING_FAILURE",
          requestRpcMetadata.getName(),
          t);

      return Mono.error(
          serverError(ResponseRpcErrorCode.REQUEST_PARSING_FAILURE, "Invalid metadata object"));
    }

    Mono<ServerResponsePayload> response =
        Mono.defer(() -> rpcServerHandler.singleRequestSingleResponse(requestPayload));

    long timeoutMillis = resolveTaskTimeoutMillis(requestRpcMetadata);
    if (timeoutMillis > 0) {
      final String methodName = requestRpcMetadata.getName();
      response =
          response.transform(
              createTimeoutTransformer(
                  timeoutMillis, Mono.error(() -> reportTaskExpired(methodName, timeoutMillis))));
    }

    return response
        .map(responsePayload -> handleResponse(alloc, requestPayload, responsePayload))
        .onErrorMap(
            e -> e instanceof RejectedExecutionException, ThriftServerRSocket::rejectedOverload)
        .onErrorMap(e -> !isThriftFrame(e), ThriftServerRSocket::toServerError)
        // The generated handler releases the request buffer right after it reads the args; this
        // is the backstop for paths where no method body runs (unsupported RpcKind, unknown
        // method, scheduler rejection, task timeout, or cancellation before the read). It can
        // run on a different thread from the read, so ServerRequestPayload serializes the two.
        .doFinally(__ -> requestPayload.releaseRequestData());
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    final RequestRpcMetadata requestRpcMetadata;
    try {
      requestRpcMetadata = decodeRequestRpcMetadata(payload);
    } catch (Throwable t) {
      ReferenceCountUtil.safeRelease(payload);

      LOGGER.warn("Invalid request metadata; sending REQUEST_PARSING_FAILURE", t);

      return Flux.error(
          serverError(ResponseRpcErrorCode.REQUEST_PARSING_FAILURE, "Invalid metadata object"));
    }

    try {
      checkRequest(requestRpcMetadata, RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE);
    } catch (Throwable t) {
      ReferenceCountUtil.safeRelease(payload);

      LOGGER.warn(
          "Invalid request for method {}; sending REQUEST_PARSING_FAILURE",
          requestRpcMetadata.getName(),
          t);

      return Flux.error(t);
    }

    RequestContext requestContext = createRequestContext(requestRpcMetadata.getOtherMetadata());

    ByteBuf data;
    try {
      data = maybeDecompressRequestData(requestRpcMetadata, payload);
    } catch (Throwable t) {
      ReferenceCountUtil.safeRelease(payload);

      LOGGER.warn(
          "Decompression failure for method {}; sending REQUEST_PARSING_FAILURE",
          requestRpcMetadata.getName(),
          t);

      return Flux.error(
          serverError(
              ResponseRpcErrorCode.REQUEST_PARSING_FAILURE,
              "decompression failure: " + nonNullMessage(t)));
    }

    ReferenceCountUtil.safeRelease(payload);
    payload = null;

    final ServerRequestPayload requestPayload;
    try {
      requestPayload = deserializeRequest(data, requestRpcMetadata, requestContext, data);
    } catch (Throwable t) {
      ReferenceCountUtil.safeRelease(data);

      LOGGER.warn(
          "Invalid request for method {}; sending REQUEST_PARSING_FAILURE",
          requestRpcMetadata.getName(),
          t);

      return Flux.error(
          serverError(ResponseRpcErrorCode.REQUEST_PARSING_FAILURE, "Invalid metadata object"));
    }

    final ServerRequestPayload finalRequestPayload = requestPayload;
    // Only the first response may become a rejection; later stream errors stay in band. The
    // flag flips only after a response is emitted, so a failure serializing the first response
    // still counts as before the first response.
    final AtomicBoolean firstResponse = new AtomicBoolean(true);

    return Flux.defer(() -> rpcServerHandler.singleRequestStreamingResponse(finalRequestPayload))
        .map(
            responsePayload ->
                handleStreamResponse(
                    alloc, finalRequestPayload, responsePayload, firstResponse.get()))
        .doOnNext(__ -> firstResponse.set(false))
        .onErrorMap(
            e -> e instanceof RejectedExecutionException && firstResponse.get(),
            ThriftServerRSocket::rejectedOverload)
        .onErrorMap(
            e -> !isThriftFrame(e) && firstResponse.get(), ThriftServerRSocket::toServerError)
        // The stream handler reads the args once when the stream is subscribed and releases the
        // request buffer then, so it is freed up front instead of being pinned for the whole
        // stream; this is the idempotent backstop for paths where no method body runs.
        .doFinally(__ -> finalRequestPayload.releaseRequestData());
  }

  private static Payload handleStreamResponse(
      ByteBufAllocator alloc,
      ServerRequestPayload requestPayload,
      ServerResponsePayload responsePayload,
      boolean firstResponse) {
    ByteBuf data = null;
    ByteBuf metadata = null;

    try {
      data = alloc.buffer();

      serializeResponse(requestPayload, responsePayload, data);
      if (firstResponse) {
        throwIfOverloaded(responsePayload);
      }

      metadata = alloc.buffer();
      serializeStreamMetadata(responsePayload, metadata);

      return createPayload(alloc, null, data, metadata);
    } catch (Throwable t) {
      if (data != null && data.refCnt() > 0) {
        data.release();
      }

      if (metadata != null && metadata.refCnt() > 0) {
        metadata.release();
      }
      throw Exceptions.propagate(t);
    }
  }

  /**
   * Throws the ERROR frame for a response that carries an unknown method or an overload reason.
   * Returns normally so any other serialized body stays the response.
   */
  private static void throwIfOverloaded(ServerResponsePayload responsePayload) {
    TApplicationException exception = responsePayload.getApplicationException();
    if (exception instanceof UnknownMethodException) {
      LOGGER.warn("Unknown method; sending UNKNOWN_METHOD: {}", exception.getMessage(), exception);

      throw serverError(ResponseRpcErrorCode.UNKNOWN_METHOD, exception.getMessage());
    }

    RSocketErrorException rejection = fromApplicationException(exception);
    if (rejection != null) {
      throw rejection;
    }
  }

  /**
   * Rejects metadata that cannot route to a handler method: a missing protocol or name, or a kind
   * that does not match the frame.
   */
  private static void checkRequest(RequestRpcMetadata metadata, RpcKind expectedKind) {
    if (metadata.getProtocol() == null
        || metadata.getName() == null
        || metadata.getName().isEmpty()
        || metadata.getKind() != expectedKind) {
      throw serverError(ResponseRpcErrorCode.REQUEST_PARSING_FAILURE, "Invalid metadata object");
    }
  }

  /**
   * Maps anything that escapes the response pipeline before the first response to an ERROR frame,
   * so a response serialization failure or a hook that throws inside error mapping never leaves as
   * a bare APPLICATION_ERROR. Errors that already are frames pass through unchanged.
   */
  /** Never renders a null message as the text "null": falls back to the throwable itself. */
  private static String nonNullMessage(Throwable t) {
    return t.getMessage() == null ? t.toString() : t.getMessage();
  }

  private static RSocketErrorException toServerError(Throwable e) {
    if (e instanceof UnknownMethodException) {
      LOGGER.warn("Unknown method; sending UNKNOWN_METHOD: {}", e.getMessage(), e);

      return serverError(ResponseRpcErrorCode.UNKNOWN_METHOD, e.getMessage());
    }

    // An unexpected failure (a serialization failure, a hook throwing inside error mapping):
    // leave a server-side signal, since the UNKNOWN frame carries no stack trace.
    LOGGER.error("Unexpected error before the first response; sending UNKNOWN", e);

    return serverError(
        ResponseRpcErrorCode.UNKNOWN, e.getMessage() == null ? "Unknown error" : e.getMessage());
  }

  /**
   * Whether the error already is a Thrift frame the C++ client accepts: REJECTED, INVALID, or
   * CANCELED. Any other error, including any other RSocket error code, still needs mapping.
   */
  private static boolean isThriftFrame(Throwable e) {
    if (!(e instanceof RSocketErrorException)) {
      return false;
    }

    int code = ((RSocketErrorException) e).errorCode();
    return code == ErrorFrameCodec.REJECTED
        || code == ErrorFrameCodec.INVALID
        || code == ErrorFrameCodec.CANCELED;
  }

  private RequestContext createRequestContext(Map<String, String> requestHeaders) {
    return new NettyNiftyRequestContext(requestHeaders, connectionContext);
  }

  private long resolveTaskTimeoutMillis(RequestRpcMetadata requestRpcMetadata) {
    return RpcServerUtils.resolveTaskTimeoutMillis(requestRpcMetadata, taskExpirationTimeout);
  }

  private static <T> MonoTimeoutTransformer<T> createTimeoutTransformer(
      long timeoutMillis, Mono<T> fallback) {
    return new MonoTimeoutTransformer<>(
        RpcResources.getOffLoopScheduler(), timeoutMillis, TimeUnit.MILLISECONDS, fallback);
  }

  private static Throwable reportTaskExpired(String methodName, long timeoutMillis) {
    LOGGER.warn("method {} timed out after {}ms", methodName, timeoutMillis);
    return taskExpired(methodName, timeoutMillis);
  }

  /**
   * Maps a saturated off-loop scheduler to the overload rejection: when the pool is full the
   * handler hop throws instead of running, which must read as a REJECTED OVERLOAD like the Header
   * transports report, not an APPLICATION_ERROR. No exception hooks fire because the handler never
   * ran.
   */
  private static RSocketErrorException rejectedOverload(Throwable e) {
    return fromApplicationException(OverloadedException.host(e.getMessage()));
  }

  /**
   * Returns the request data, decompressing if the metadata specifies compression. The returned
   * buffer is always independently owned — the caller must release it. Always retains {@code
   * payload.sliceData()} first; in the uncompressed case that retained slice is returned, in the
   * compressed case the compressor takes ownership of it (releasing it in its own finally
   * regardless of success or failure) and returns a freshly allocated uncompressed buffer. Looking
   * up the compressor is the only pre-ownership-transfer step; if it throws (e.g. unknown
   * algorithm), the retained slice is released here.
   */
  private ByteBuf maybeDecompressRequestData(RequestRpcMetadata metadata, Payload payload) {
    ByteBuf data = payload.sliceData();
    data.retain();
    CompressionAlgorithm compression = metadata.getCompression();
    if (compression == null || compression == CompressionAlgorithm.NONE) {
      return data;
    }
    ThriftCompressor compressor;
    try {
      compressor = CompressionManager.getCompressor(compression);
    } catch (Throwable t) {
      ReferenceCountUtil.safeRelease(data);
      throw t;
    }
    return compressor.decompress(alloc, data); // ownership of `data` transferred to the compressor
  }

  private static ServerRequestPayload deserializeRequest(
      ByteBuf data,
      RequestRpcMetadata requestRpcMetadata,
      RequestContext requestContext,
      ReferenceCounted requestData) {
    ByteBufTProtocol protocol =
        TProtocolType.fromProtocolId(requestRpcMetadata.getProtocol()).apply(data);
    // The payload owns the decoded request data buffer, bound to the reader positioned on it, so
    // the generated handler reads and frees it as one claim rather than holding the buffer until
    // the response completes.
    return ServerRequestPayload.create(
        requestRpcMetadata, requestContext, RequestData.of(requestData, protocol));
  }

  /**
   * Releases what is still owned by this method on a synchronous failure before the reactive
   * pipeline is returned. The success path nulls out {@code payload} (already released after
   * decode) and {@code data} (ownership transferred to {@code requestPayload}); this method
   * therefore safely no-ops on whichever has already been handed off.
   *
   * <p>If {@code requestPayload} exists, route the buffer release through its idempotent {@link
   * ServerRequestPayload#releaseRequestData()} so the catch and success paths free it exactly once;
   * otherwise the local {@code data} buffer is released directly. The original {@code payload} is
   * always {@link ReferenceCountUtil#safeRelease(Object) safeRelease}d in case the failure happened
   * before the post-decode release.
   */
  private static void releaseOnError(
      ServerRequestPayload requestPayload, ByteBuf data, Payload payload) {
    if (requestPayload != null) {
      requestPayload.releaseRequestData();
    } else {
      ReferenceCountUtil.safeRelease(data);
    }
    ReferenceCountUtil.safeRelease(payload);
  }

  private static RequestRpcMetadata decodeRequestRpcMetadata(Payload payload) {
    return ProtocolUtil.readCompact(RequestRpcMetadata::read0, payload.sliceMetadata());
  }

  private static Payload handleResponse(
      ByteBufAllocator alloc,
      ServerRequestPayload requestPayload,
      ServerResponsePayload responsePayload) {
    ByteBuf data = null;
    ByteBuf metadata = null;

    try {
      data = alloc.buffer();

      serializeResponse(requestPayload, responsePayload, data);
      throwIfOverloaded(responsePayload);

      metadata = alloc.buffer();
      serializeResponseMetadata(responsePayload, metadata);

      return createPayload(
          alloc, responsePayload.getResponseRpcMetadata().getCompression(), data, metadata);

    } catch (Throwable t) {
      if (data != null && data.refCnt() > 0) {
        data.release();
      }

      if (metadata != null && metadata.refCnt() > 0) {
        metadata.release();
      }
      throw Exceptions.propagate(t);
    }
  }

  private static void serializeResponseMetadata(
      ServerResponsePayload responsePayload, ByteBuf metadata) {
    ProtocolUtil.writeCompact(responsePayload.getResponseRpcMetadata()::write0, metadata);
  }

  private static void serializeStreamMetadata(
      ServerResponsePayload responsePayload, ByteBuf metadata) {
    if (responsePayload.getResponseRpcMetadata() != null) {
      ProtocolUtil.writeCompact(responsePayload.getResponseRpcMetadata()::write0, metadata);
    } else {
      ProtocolUtil.writeCompact(responsePayload.getStreamPayloadMetadata()::write0, metadata);
    }
  }

  private static void serializeResponse(
      ServerRequestPayload requestPayload, ServerResponsePayload responsePayload, ByteBuf data) {
    final ByteBufTProtocol in = getTProtocol(requestPayload, data);
    final Writer writer = responsePayload.getDataWriter();
    writer.write(in);
  }

  private static ByteBufTProtocol getTProtocol(ServerRequestPayload requestPayload, ByteBuf data) {
    return TProtocolType.fromProtocolId(requestPayload.getRequestRpcMetadata().getProtocol())
        .apply(data);
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    ByteBuf data = null;
    ServerRequestPayload requestPayload = null;
    try {
      RequestRpcMetadata requestRpcMetadata = decodeRequestRpcMetadata(payload);

      RequestContext requestContext = createRequestContext(requestRpcMetadata.getOtherMetadata());

      data = maybeDecompressRequestData(requestRpcMetadata, payload);
      ReferenceCountUtil.safeRelease(payload);
      payload = null;

      requestPayload = deserializeRequest(data, requestRpcMetadata, requestContext, data);
      data = null; // ownership transferred to ServerRequestPayload

      checkRequest(requestRpcMetadata, RpcKind.SINGLE_REQUEST_NO_RESPONSE);

      final ServerRequestPayload finalRequestPayload = requestPayload;
      Mono<Void> response = rpcServerHandler.singleRequestNoResponse(requestPayload);

      long timeoutMillis = resolveTaskTimeoutMillis(requestRpcMetadata);
      if (timeoutMillis > 0) {
        final String methodName = requestRpcMetadata.getName();
        // A oneway request has no response channel, so an expired task is dropped rather than
        // reported. C++ arms the timer for fire-and-forget too and suppresses only the reply, so
        // the dropped response matches; the cancellation above does not.
        response =
            response.transform(
                createTimeoutTransformer(
                    timeoutMillis,
                    Mono.fromRunnable(
                        () ->
                            LOGGER.warn(
                                "oneway method {} timed out after {}ms, dropping",
                                methodName,
                                timeoutMillis))));
      }

      return response
          // The generated handler releases the request buffer right after it reads the args; this
          // is the idempotent backstop for paths where no method body runs.
          .doFinally(__ -> finalRequestPayload.releaseRequestData());
    } catch (Throwable t) {
      releaseOnError(requestPayload, data, payload);

      if (t instanceof UnknownMethodException) {
        LOGGER.warn("Unknown oneway method; dropping", t);
      } else if (requestPayload == null || t instanceof RSocketErrorException) {
        LOGGER.warn("Invalid oneway request; dropping", t);
      } else {
        LOGGER.error("Unexpected oneway handler failure; dropping", t);
      }

      return Mono.error(t);
    }
  }

  /**
   * The scheduler a client CANCEL hops through on its way upstream: the off-loop scheduler with a
   * common-pool fallback, because {@code cancelOn} has no rejection fallback and a rejected cancel
   * would never reach upstream.
   */
  public static Scheduler cancelScheduler() {
    return CancelSchedulerHolder.INSTANCE;
  }

  private static final class CancelSchedulerHolder {
    static final Scheduler INSTANCE =
        new CancelScheduler(Schedulers.fromExecutorService(ForkJoinPool.commonPool()));
  }

  /**
   * Never rejects: the off-loop scheduler with a common-pool fallback, so a cancel is never lost.
   */
  private static final class CancelScheduler implements Scheduler {
    private final Scheduler fallback;

    CancelScheduler(Scheduler fallback) {
      this.fallback = fallback;
    }

    @Override
    public Disposable schedule(Runnable task) {
      try {
        return RpcResources.getOffLoopScheduler().schedule(task);
      } catch (RejectedExecutionException e) {
        return fallback.schedule(task);
      }
    }

    @Override
    public Scheduler.Worker createWorker() {
      return RpcResources.getOffLoopScheduler().createWorker();
    }

    @Override
    public boolean isDisposed() {
      return false;
    }
  }
}
