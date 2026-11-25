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

import static com.facebook.thrift.rsocket.util.MetadataUtil.decodePayloadMetadata;
import static com.facebook.thrift.rsocket.util.MetadataUtil.decodeStreamingPayloadMetadata;
import static com.facebook.thrift.rsocket.util.PayloadUtil.createPayload;
import static com.facebook.thrift.rsocket.util.RocketErrorUtil.decodeRocketError;
import static com.facebook.thrift.rsocket.util.RocketErrorUtil.isRocketError;
import static com.facebook.thrift.rsocket.util.RocketErrorUtil.toApplicationException;
import static com.facebook.thrift.rsocket.util.RocketErrorUtil.toResponseMetadata;
import static com.facebook.thrift.util.RpcClientUtils.getExceptionString;
import static com.facebook.thrift.util.RpcClientUtils.getUndeclaredException;

import com.facebook.thrift.client.RpcClient;
import com.facebook.thrift.client.RpcOptions;
import com.facebook.thrift.compression.CompressionManager;
import com.facebook.thrift.payload.ClientRequestPayload;
import com.facebook.thrift.payload.ClientResponsePayload;
import com.facebook.thrift.payload.Writer;
import com.facebook.thrift.protocol.ByteBufTProtocol;
import com.facebook.thrift.protocol.TProtocolType;
import com.facebook.thrift.util.RpcClientUtils;
import com.facebook.thrift.util.resources.RpcResources;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.ByteBufPayload;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.thrift.ClientPushMetadata;
import org.apache.thrift.CodecConfig;
import org.apache.thrift.CompressionAlgorithm;
import org.apache.thrift.CompressionConfig;
import org.apache.thrift.ProtocolId;
import org.apache.thrift.RequestRpcMetadata;
import org.apache.thrift.ResponseRpcError;
import org.apache.thrift.ResponseRpcMetadata;
import org.apache.thrift.StreamPayloadMetadata;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TStruct;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public final class RSocketRpcClient implements RpcClient {
  private static final Logger LOG = LoggerFactory.getLogger(RSocketRpcClient.class);
  private static final TStruct PARAMETERS_STRUCT = new TStruct();

  private final RSocket rsocket;
  private final ByteBufAllocator alloc;
  private final Scheduler offLoopScheduler;

  RSocketRpcClient(final RSocket rsocket) {
    this(
        rsocket,
        RpcResources.isForceClientExecutionOffEventLoop()
            ? RpcResources.getClientOffLoopScheduler()
            : Schedulers.immediate());
  }

  /** An immediate {@code offLoopScheduler} keeps decode on the transport thread. */
  RSocketRpcClient(final RSocket rsocket, final Scheduler offLoopScheduler) {
    this.rsocket = rsocket;
    this.alloc = ByteBufAllocator.DEFAULT;
    this.offLoopScheduler = offLoopScheduler;
  }

  @Override
  public Mono<Void> onClose() {
    LOG.debug("RSocketRpcClient onClose invoked", new Exception("onClose"));
    return rsocket.onClose();
  }

  @Override
  public void dispose() {
    LOG.debug("RSocketRpcClient dispose invoked", new Exception("dispose"));
    rsocket.dispose();
  }

  @Override
  public boolean isDisposed() {
    return rsocket.isDisposed();
  }

  private static boolean hasTimeoutSettings(RpcOptions options) {
    if (options == null) {
      return false;
    }
    return options.getClientTimeoutMs() != null || options.getQueueTimeoutMs() != null;
  }

  /**
   * Returns the request metadata with the timeouts and the compression of the options. The request
   * is compressed when its data is larger than the size limit of the compression config, as in C++.
   * When {@code expectResponse} is true and the codec is not {@code customConfig}, the metadata
   * carries {@code compressionConfig} so the server compresses responses.
   */
  private static RequestRpcMetadata requestMetadata(
      RequestRpcMetadata metadata, RpcOptions options, int dataSize, boolean expectResponse) {
    if (options == null) {
      return metadata;
    }

    CompressionConfig compressionConfig = options.getCompressionConfig();
    if (!hasTimeoutSettings(options) && compressionConfig == null) {
      return metadata;
    }

    RequestRpcMetadata.Builder builder = new RequestRpcMetadata.Builder(metadata);
    if (options.getClientTimeoutMs() != null && options.getClientTimeoutMs() != 0) {
      builder.setClientTimeoutMs(options.getClientTimeoutMs());
    }
    if (options.getQueueTimeoutMs() != null && options.getQueueTimeoutMs() != 0) {
      builder.setQueueTimeoutMs(options.getQueueTimeoutMs());
    }

    if (compressionConfig != null) {
      CompressionAlgorithm compression =
          CompressionManager.algorithmFor(compressionConfig, dataSize);
      if (compression != CompressionAlgorithm.NONE) {
        builder.setCompression(compression);
      }

      // This client cannot decompress a CUSTOM response, so it does not ask for one.
      CodecConfig codecConfig = compressionConfig.getCodecConfig();
      if (expectResponse && codecConfig != null && !codecConfig.isSetCustomConfig()) {
        builder.setCompressionConfig(compressionConfig);
      }
    }

    return builder.build();
  }

  @Override
  public <T> Mono<ClientResponsePayload<T>> singleRequestSingleResponse(
      ClientRequestPayload<T> payload, RpcOptions options) {
    final ProtocolId protocol = payload.getRequestRpcMetadata().getProtocol();
    final TProtocolType protocolType = TProtocolType.fromProtocolId(protocol);

    return ResponseHandoff.requestResponse(
        rsocket,
        () -> clientRequestPayloadToRSocketPayload(payload, protocolType, options, true),
        offLoopScheduler,
        response -> rsocketPayloadToClientResponsePayload(payload, response, protocolType),
        error -> isRocketError(error) ? getErrorFrame(error) : null);
  }

  /**
   * INVALID, CANCELLED and REJECTED rsocket error codes are defined as internal server error in
   * rocket protocol. Payload will be compact protocol serialized ResponseRpcError struct.
   *
   * <p>The C++ client reports these as a {@code TApplicationException} carrying the error's own
   * type and {@code what_utf8}, plus the {@code ex} header. This does the same.
   *
   * @param t Throwable contains rsocket error code
   * @param <T>
   * @return ClientResponsePayload
   */
  private static <T> ClientResponsePayload<T> getErrorFrame(Throwable t) {
    ResponseRpcError err = decodeRocketError(t);
    if (err == null) {
      // Unreadable frame: report the parse failure rather than letting it replace the server's
      // error with an unrelated exception, as the C++ client does.
      return ClientResponsePayload.createException(toApplicationException(t), null, null, false);
    }
    return ClientResponsePayload.createException(
        toApplicationException(err), toResponseMetadata(err), null, false);
  }

  @Override
  public <T> Mono<Void> singleRequestNoResponse(
      ClientRequestPayload<T> payload, RpcOptions options) {
    final ProtocolId protocol = payload.getRequestRpcMetadata().getProtocol();
    final TProtocolType protocolType = TProtocolType.fromProtocolId(protocol);

    return Mono.defer(
        () -> {
          Payload rsocketPayload =
              clientRequestPayloadToRSocketPayload(payload, protocolType, options, false);
          try {
            return rsocket.fireAndForget(rsocketPayload);
          } catch (Throwable t) {
            ReferenceCountUtil.safeRelease(rsocketPayload);
            return Mono.error(t);
          }
        });
  }

  /**
   * ImmediateScheduler is a singleton (Schedulers.immediate() returns its instance), so identity
   * against it tells inline decode from the off-loop handoff with no second flag.
   */
  private boolean isInline() {
    return offLoopScheduler == Schedulers.immediate();
  }

  /** Inline chain: a rocket error maps to an error frame, then each frame decodes inline. */
  private static <K> Flux<ClientResponsePayload<K>> inlineStream(
      Flux<Payload> inbound, Function<Payload, ClientResponsePayload<K>> decoder, String name) {
    return inbound
        .onErrorResume(
            t -> {
              if (isRocketError(t)) {
                return Flux.just(ByteBufPayload.create(getExceptionString(t, name)));
              }

              return Flux.error(t);
            })
        .map(decoder);
  }

  /** Maps a terminal error to a last value, or null to deliver the error unchanged. */
  @Nullable
  private static <K> ClientResponsePayload<K> rocketErrorValue(
      Throwable error, Function<Payload, ClientResponsePayload<K>> decoder, String name) {
    if (!isRocketError(error)) {
      return null;
    }

    return decoder.apply(ByteBufPayload.create(getExceptionString(error, name)));
  }

  @Override
  public <T, K> Flux<ClientResponsePayload<K>> singleRequestStreamingResponse(
      ClientRequestPayload<T> payload, RpcOptions options) {
    final ProtocolId protocol = payload.getRequestRpcMetadata().getProtocol();
    final TProtocolType protocolType = TProtocolType.fromProtocolId(protocol);
    final String name = payload.getRequestRpcMetadata().getName();

    if (isInline()) {
      // The decoder carries per-stream first-payload state, so each subscription builds its
      // own, exactly like master.
      return Flux.defer(
          () -> {
            Payload rsocketPayload =
                clientRequestPayloadToRSocketPayload(payload, protocolType, options, true);
            Function<Payload, ClientResponsePayload<K>> decoder =
                new StreamingResponseHandler<>(alloc, payload);

            try {
              return inlineStream(rsocket.requestStream(rsocketPayload), decoder, name);
            } catch (Throwable t) {
              ReferenceCountUtil.safeRelease(rsocketPayload);
              return Flux.error(t);
            }
          });
    }

    return Flux.defer(
        () -> {
          Function<Payload, ClientResponsePayload<K>> decoder =
              new StreamingResponseHandler<>(alloc, payload);

          return StreamHandoff.requestStream(
              rsocket,
              () -> clientRequestPayloadToRSocketPayload(payload, protocolType, options, true),
              offLoopScheduler,
              decoder,
              error -> rocketErrorValue(error, decoder, name));
        });
  }

  @Override
  public <T, K> Flux<ClientResponsePayload<K>> streamingRequestStreamingResponse(
      Publisher<ClientRequestPayload<T>> payloads, RpcOptions options) {
    return Flux.from(payloads)
        .switchOnFirst(
            (signal, flux) -> {
              final ClientRequestPayload<T> clientRequestPayload = signal.get();

              final ProtocolId protocol =
                  clientRequestPayload.getRequestRpcMetadata().getProtocol();
              final TProtocolType protocolType = TProtocolType.fromProtocolId(protocol);

              final AtomicBoolean isFirstPayload = new AtomicBoolean(true);
              final Flux<Payload> payloadFlux =
                  flux.map(
                          t -> {
                            if (isFirstPayload.compareAndSet(true, false)) {
                              return clientRequestPayloadToRSocketPayload(
                                  t, protocolType, options, true);
                            } else {
                              return sinkPayloadToRSocketPayload(t, protocolType, options);
                            }
                          })
                      .doOnDiscard(Payload.class, ReferenceCountUtil::safeRelease);

              Function<Payload, ClientResponsePayload<K>> decoder =
                  new StreamingResponseHandler<>(alloc, clientRequestPayload);
              String name = clientRequestPayload.getRequestRpcMetadata().getName();

              if (isInline()) {
                return inlineStream(rsocket.requestChannel(payloadFlux), decoder, name);
              }

              return StreamHandoff.requestChannel(
                  rsocket,
                  payloadFlux,
                  offLoopScheduler,
                  decoder,
                  error -> rocketErrorValue(error, decoder, name));
            });
  }

  @Override
  public Mono<Void> metadataPush(ClientPushMetadata clientMetadata, RpcOptions options) {
    return Mono.defer(
        () -> {
          Payload payload = null;
          ByteBuf metadata = alloc.buffer();
          try {
            ByteBufTProtocol metadataProtocol = TProtocolType.TCompact.apply(metadata);
            clientMetadata.write0(metadataProtocol);
            payload = ByteBufPayload.create(Unpooled.EMPTY_BUFFER, metadata);
            return rsocket.metadataPush(payload);
          } catch (Throwable t) {
            if (payload != null) {
              ReferenceCountUtil.safeRelease(payload);
            } else {
              ReferenceCountUtil.safeRelease(metadata);
            }
            return Mono.error(t);
          }
        });
  }

  private <T> Payload clientRequestPayloadToRSocketPayload(
      ClientRequestPayload<T> payload,
      TProtocolType protocolType,
      RpcOptions options,
      boolean expectResponse) {
    ByteBuf data = null;
    ByteBuf metadata = null;
    try {
      data = alloc.buffer();
      metadata = alloc.buffer();

      final ByteBufTProtocol in = protocolType.apply(data);
      in.writeStructBegin(PARAMETERS_STRUCT);

      final Writer writer = payload.getDataWriter();
      writer.write(in);

      in.writeFieldStop();
      in.writeStructEnd();

      final RequestRpcMetadata requestMetadata =
          requestMetadata(
              payload.getRequestRpcMetadata(), options, data.readableBytes(), expectResponse);
      requestMetadata.write0(TProtocolType.TCompact.apply(metadata));

      return createPayload(alloc, requestMetadata.getCompression(), data, metadata);

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

  private <T> Payload sinkPayloadToRSocketPayload(
      ClientRequestPayload<T> payload, TProtocolType protocolType, RpcOptions options) {
    ByteBuf data = null;
    ByteBuf metadata = null;

    try {
      data = alloc.buffer();
      metadata = alloc.buffer();

      final ByteBufTProtocol in = protocolType.apply(data);
      in.writeStructBegin(PARAMETERS_STRUCT);

      final Writer writer = payload.getDataWriter();
      writer.write(in);

      in.writeFieldStop();
      in.writeStructEnd();

      // Subsequent sink payload frames must use StreamPayloadMetadata,
      // not RequestRpcMetadata. The first frame (handled by
      // clientRequestPayloadToRSocketPayload) uses RequestRpcMetadata
      // to establish the RPC. The compression field must be set on
      // StreamPayloadMetadata to match the actual compression applied
      // by createPayload.
      final CompressionConfig config = options == null ? null : options.getCompressionConfig();
      final CompressionAlgorithm compression =
          config == null
              ? payload.getRequestRpcMetadata().getCompression()
              : CompressionManager.algorithmFor(config, data.readableBytes());

      final ByteBufTProtocol metadataProtocol = TProtocolType.TCompact.apply(metadata);
      StreamPayloadMetadata.Builder streamMetadataBuilder = new StreamPayloadMetadata.Builder();

      if (compression != null && compression != CompressionAlgorithm.NONE) {
        streamMetadataBuilder.setCompression(compression);
      }

      streamMetadataBuilder.build().write0(metadataProtocol);

      return createPayload(alloc, compression, data, metadata);
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

  private <T> ClientResponsePayload<T> rsocketPayloadToClientResponsePayload(
      ClientRequestPayload<T> requestPayload, Payload response, TProtocolType protocolType) {
    ByteBuf data = null;
    try {
      final ResponseRpcMetadata rpcMetadata = decodePayloadMetadata(response);

      Optional<? extends TException> undeclaredException = getUndeclaredException(rpcMetadata);
      if (undeclaredException.isPresent()) {
        return ClientResponsePayload.createException(
            undeclaredException.get(), rpcMetadata, null, false);
      }

      data =
          CompressionManager.decompress(
              rpcMetadata.getCompression(), alloc, response.sliceData().retain());
      final ByteBufTProtocol out = protocolType.apply(data);

      return RpcClientUtils.decodeRSocketPayload(
          requestPayload.getResponseReader(),
          requestPayload.getExceptionReaders(),
          out,
          rpcMetadata);
    } catch (Exception e) {
      throw Exceptions.propagate(e);
    } finally {
      ReferenceCountUtil.safeRelease(data);
      if (response.refCnt() > 0) {
        response.release();
      }
    }
  }

  private static class StreamingResponseHandler<T, K>
      implements Function<Payload, ClientResponsePayload<K>> {

    private final ByteBufAllocator alloc;
    private final ClientRequestPayload<T> requestPayload;
    private boolean isFirstPayload = true;

    private StreamingResponseHandler(
        ByteBufAllocator alloc, ClientRequestPayload<T> requestPayload) {
      this.alloc = alloc;
      this.requestPayload = requestPayload;
    }

    @Override
    public ClientResponsePayload<K> apply(Payload response) {
      try {
        StreamPayloadMetadata streamPayloadMetadata;
        ResponseRpcMetadata rpcMetadata;
        if (isFirstPayload) {
          streamPayloadMetadata = StreamPayloadMetadata.defaultInstance();
          rpcMetadata = decodePayloadMetadata(response);
        } else {
          streamPayloadMetadata = decodeStreamingPayloadMetadata(response);
          rpcMetadata = ResponseRpcMetadata.defaultInstance();
        }

        Optional<? extends TException> undeclaredException = getUndeclaredException(rpcMetadata);
        if (!undeclaredException.isPresent() && !isFirstPayload) {
          undeclaredException = getUndeclaredException(streamPayloadMetadata);
        }
        if (undeclaredException.isPresent()) {
          return ClientResponsePayload.createException(
              undeclaredException.get(), rpcMetadata, streamPayloadMetadata, true);
        }
        ClientResponsePayload<K> responsePayload =
            RpcClientUtils.decodeStreamPayload(
                alloc,
                requestPayload,
                response,
                streamPayloadMetadata,
                rpcMetadata,
                isFirstPayload);
        isFirstPayload = false;
        return responsePayload;
      } catch (Exception e) {
        throw Exceptions.propagate(e);
      } finally {
        if (response.refCnt() > 0) {
          response.release();
        }
      }
    }
  }
}
