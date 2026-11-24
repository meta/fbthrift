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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.facebook.thrift.client.RpcOptions;
import com.facebook.thrift.payload.ClientRequestPayload;
import com.facebook.thrift.payload.ClientResponsePayload;
import com.facebook.thrift.protocol.TProtocolType;
import com.facebook.thrift.rsocket.util.RocketErrorUtil;
import com.facebook.thrift.util.RpcPayloadUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.ByteBufPayload;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.thrift.CodecConfig;
import org.apache.thrift.CompressionAlgorithm;
import org.apache.thrift.CompressionConfig;
import org.apache.thrift.PayloadMetadata;
import org.apache.thrift.ProtocolId;
import org.apache.thrift.RequestRpcMetadata;
import org.apache.thrift.ResponseRpcMetadata;
import org.apache.thrift.RpcKind;
import org.apache.thrift.ZstdCompressionCodecConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.Context;

class RSocketRpcClientTest {

  private final List<Object> dropped = Collections.synchronizedList(new ArrayList<>());

  @BeforeEach
  void trackDropped() {
    Hooks.onErrorDropped(dropped::add);
    Hooks.onNextDropped(dropped::add);
  }

  @AfterEach
  void assertNothingDropped() {
    Hooks.resetOnErrorDropped();
    Hooks.resetOnNextDropped();

    assertThat(dropped).isEmpty();
  }

  private static final RpcOptions ZSTD_OPTIONS =
      new RpcOptions.Builder()
          .setCompressionConfig(
              new CompressionConfig.Builder()
                  .setCodecConfig(
                      CodecConfig.fromZstdConfig(new ZstdCompressionCodecConfig.Builder().build()))
                  .build())
          .build();

  @Test
  void singleResponseRequestSendsTheCompressionConfig() {
    RSocket rsocket = mock(RSocket.class);
    AtomicReference<Payload> sent = new AtomicReference<>();
    when(rsocket.requestResponse(any()))
        .thenAnswer(
            invocation -> {
              sent.set(invocation.getArgument(0));
              return Mono.never();
            });

    new RSocketRpcClient(rsocket)
        .singleRequestSingleResponse(
            requestPayload(RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE), ZSTD_OPTIONS)
        .subscribe();

    RequestRpcMetadata metadata = readMetadata(sent.get());

    assertThat(metadata.getCompression()).isEqualTo(CompressionAlgorithm.ZSTD);
    assertThat(metadata.getCompressionConfig()).isEqualTo(ZSTD_OPTIONS.getCompressionConfig());
    sent.get().release();
  }

  @Test
  void streamingRequestDoesNotAskForCompressedResponses() {
    RSocket rsocket = mock(RSocket.class);
    AtomicReference<Payload> sent = new AtomicReference<>();
    when(rsocket.requestStream(any()))
        .thenAnswer(
            invocation -> {
              sent.set(invocation.getArgument(0));
              return Flux.never();
            });

    new RSocketRpcClient(rsocket)
        .singleRequestStreamingResponse(
            requestPayload(RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE), ZSTD_OPTIONS)
        .subscribe();

    RequestRpcMetadata metadata = readMetadata(sent.get());

    assertThat(metadata.getCompression()).isEqualTo(CompressionAlgorithm.ZSTD);
    assertThat(metadata.getCompressionConfig()).isNull();
    sent.get().release();
  }

  @Test
  void singleResponseDecodesOffEventLoop() {
    Scheduler scheduler = Schedulers.newSingle("test-offloop");

    try {
      RSocket rsocket = mock(RSocket.class);
      when(rsocket.requestResponse(any())).thenReturn(Mono.just(errorResponsePayload()));

      AtomicReference<String> decodeThread = new AtomicReference<>();

      ClientResponsePayload<Object> result =
          new RSocketRpcClient(rsocket, scheduler)
              .singleRequestSingleResponse(
                  requestPayload(RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE), null)
              .doOnNext(ignored -> decodeThread.set(Thread.currentThread().getName()))
              .block();

      assertThat(result.getException()).hasMessage("boom");
      assertThat(decodeThread.get().toLowerCase()).contains("offloop");
    } finally {
      scheduler.dispose();
    }
  }

  @Test
  void rocketErrorMapsToErrorFrame() {
    RSocket rsocket = mock(RSocket.class);
    when(rsocket.requestResponse(any()))
        .thenReturn(Mono.error(RocketErrorUtil.taskExpired("ping", 5000L)));

    ClientResponsePayload<Object> result =
        new RSocketRpcClient(rsocket)
            .singleRequestSingleResponse(
                requestPayload(RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE), null)
            .block();

    assertThat(result.getException()).hasMessage("Task expired for ping after 5000ms");
  }

  @Test
  void singleCancelBeforeSubscribeNeverRequests() {
    ManualScheduler scheduler = new ManualScheduler();
    AtomicReference<CoreSubscriber<? super Payload>> downstream = new AtomicReference<>();
    Mono<Payload> responses =
        new Mono<Payload>() {
          @Override
          public void subscribe(CoreSubscriber<? super Payload> actual) {
            downstream.set(actual);
          }
        };

    RSocket rsocket = mock(RSocket.class);
    when(rsocket.requestResponse(any())).thenReturn(responses);

    Subscription upstream = mock(Subscription.class);
    Disposable subscription =
        new RSocketRpcClient(rsocket, scheduler)
            .singleRequestSingleResponse(
                requestPayload(RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE), null)
            .subscribe();

    subscription.dispose();
    downstream.get().onSubscribe(upstream);

    verify(upstream).cancel();
    verify(upstream, never()).request(anyLong());
  }

  @Test
  void singleCancelWhileQueuedReleasesPayload() {
    ManualScheduler scheduler = new ManualScheduler();
    AtomicReference<MonoSink<Payload>> captured = new AtomicReference<>();
    RSocket rsocket = mock(RSocket.class);
    when(rsocket.requestResponse(any())).thenAnswer(invocation -> Mono.create(captured::set));

    Payload response = errorResponsePayload();
    response.retain();

    Disposable subscription =
        new RSocketRpcClient(rsocket, scheduler)
            .singleRequestSingleResponse(
                requestPayload(RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE), null)
            .subscribe();

    captured.get().success(response);

    assertThat(scheduler.tasks).hasSize(1);

    subscription.dispose();
    scheduler.dispose();

    assertThat(scheduler.tasks).isEmpty();
    assertThat(response.refCnt()).isEqualTo(1);

    response.release();
  }

  @Test
  void singleResponseAfterCancelReleasesPayload() {
    ManualScheduler scheduler = new ManualScheduler();
    AtomicReference<CoreSubscriber<? super Payload>> downstream = new AtomicReference<>();
    Mono<Payload> responses =
        new Mono<Payload>() {
          @Override
          public void subscribe(CoreSubscriber<? super Payload> actual) {
            downstream.set(actual);

            actual.onSubscribe(
                new Subscription() {
                  @Override
                  public void request(long n) {}

                  @Override
                  public void cancel() {}
                });
          }
        };

    RSocket rsocket = mock(RSocket.class);
    when(rsocket.requestResponse(any())).thenReturn(responses);

    Payload response = errorResponsePayload();
    response.retain();

    Disposable subscription =
        new RSocketRpcClient(rsocket, scheduler)
            .singleRequestSingleResponse(
                requestPayload(RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE), null)
            .subscribe();

    subscription.dispose();
    downstream.get().onNext(response);

    scheduler.runAll();

    assertThat(response.refCnt()).isEqualTo(1);

    response.release();
  }

  @Test
  void singleDecodesWithCapturedScheduler() {
    ManualScheduler scheduler = new ManualScheduler();
    AtomicReference<MonoSink<Payload>> captured = new AtomicReference<>();
    RSocket rsocket = mock(RSocket.class);
    when(rsocket.requestResponse(any())).thenAnswer(invocation -> Mono.create(captured::set));

    Payload response = errorResponsePayload();
    response.retain();

    AtomicReference<ClientResponsePayload<Object>> result = new AtomicReference<>();
    new RSocketRpcClient(rsocket, scheduler)
        .singleRequestSingleResponse(requestPayload(RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE), null)
        .doOnNext(result::set)
        .subscribe();

    captured.get().success(response);
    scheduler.runAll();

    assertThat(result.get().getException()).hasMessage("boom");
    assertThat(response.refCnt()).isEqualTo(1);

    response.release();
  }

  @Test
  void singleImmediateSchedulerDecodesInline() {
    RSocket rsocket = mock(RSocket.class);
    when(rsocket.requestResponse(any())).thenReturn(Mono.just(errorResponsePayload()));

    AtomicReference<String> decodeThread = new AtomicReference<>();

    ClientResponsePayload<Object> result =
        new RSocketRpcClient(rsocket, Schedulers.immediate())
            .singleRequestSingleResponse(
                requestPayload(RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE), null)
            .doOnNext(ignored -> decodeThread.set(Thread.currentThread().getName()))
            .block();

    assertThat(result.getException()).hasMessage("boom");
    assertThat(decodeThread.get()).isEqualTo(Thread.currentThread().getName());
  }

  @Test
  void rocketErrorMapsOnScheduler() {
    ManualScheduler scheduler = new ManualScheduler();
    RSocket rsocket = mock(RSocket.class);
    when(rsocket.requestResponse(any()))
        .thenReturn(Mono.error(RocketErrorUtil.taskExpired("ping", 5000L)));

    AtomicReference<ClientResponsePayload<Object>> result = new AtomicReference<>();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    new RSocketRpcClient(rsocket, scheduler)
        .singleRequestSingleResponse(requestPayload(RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE), null)
        .subscribe(result::set, failure::set);

    assertThat(scheduler.tasks).hasSize(1);
    assertThat(result.get()).isNull();
    assertThat(failure.get()).isNull();

    scheduler.runAll();

    assertThat(result.get().getException()).hasMessage("Task expired for ping after 5000ms");
    assertThat(failure.get()).isNull();
  }

  @Test
  void nonRocketErrorDeliversOnScheduler() {
    ManualScheduler scheduler = new ManualScheduler();
    RSocket rsocket = mock(RSocket.class);
    when(rsocket.requestResponse(any()))
        .thenReturn(Mono.error(new RuntimeException("transport down")));

    AtomicReference<ClientResponsePayload<Object>> result = new AtomicReference<>();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    new RSocketRpcClient(rsocket, scheduler)
        .singleRequestSingleResponse(requestPayload(RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE), null)
        .subscribe(result::set, failure::set);

    assertThat(scheduler.tasks).hasSize(1);
    assertThat(result.get()).isNull();
    assertThat(failure.get()).isNull();

    scheduler.runAll();

    assertThat(result.get()).isNull();
    assertThat(failure.get()).hasMessage("transport down");
  }

  @Test
  void singleDecodeErrorReleasesPayload() {
    RSocket rsocket = mock(RSocket.class);
    Payload response = garbageResponsePayload();
    response.retain();
    when(rsocket.requestResponse(any())).thenReturn(Mono.just(response));

    assertThatThrownBy(
        () ->
            new RSocketRpcClient(rsocket)
                .singleRequestSingleResponse(
                    requestPayload(RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE), null)
                .block());

    assertThat(response.refCnt()).isEqualTo(1);

    response.release();
  }

  @Test
  void singleCancelDuringDecodeReleasesOnce() throws Exception {
    ManualScheduler scheduler = new ManualScheduler();
    AtomicReference<MonoSink<Payload>> captured = new AtomicReference<>();
    RSocket rsocket = mock(RSocket.class);
    when(rsocket.requestResponse(any())).thenAnswer(invocation -> Mono.create(captured::set));

    Payload response = errorResponsePayload();
    response.retain();

    Disposable subscription =
        new RSocketRpcClient(rsocket, scheduler)
            .singleRequestSingleResponse(
                requestPayload(RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE), null)
            .subscribe();

    captured.get().success(response);

    Thread worker = new Thread(scheduler::runAll);
    worker.start();
    subscription.dispose();
    worker.join();

    assertThat(response.refCnt()).isEqualTo(1);

    response.release();
  }

  @Test
  void singleScheduleRejectionErrorsAndReleases() {
    Scheduler scheduler = mock(Scheduler.class);
    when(scheduler.schedule(any(Runnable.class)))
        .thenThrow(new RejectedExecutionException("no workers"));

    AtomicReference<MonoSink<Payload>> captured = new AtomicReference<>();
    RSocket rsocket = mock(RSocket.class);
    when(rsocket.requestResponse(any())).thenAnswer(invocation -> Mono.create(captured::set));

    Payload response = errorResponsePayload();
    response.retain();

    AtomicReference<Throwable> failure = new AtomicReference<>();
    new RSocketRpcClient(rsocket, scheduler)
        .singleRequestSingleResponse(requestPayload(RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE), null)
        .subscribe(value -> {}, failure::set);

    captured.get().success(response);

    assertThat(failure.get()).hasMessage("no workers");
    assertThat(response.refCnt()).isEqualTo(1);

    response.release();
  }

  @Test
  void singleEmptyResponseCompletesEmpty() {
    RSocket rsocket = mock(RSocket.class);
    when(rsocket.requestResponse(any())).thenReturn(Mono.empty());

    ClientResponsePayload<Object> result =
        new RSocketRpcClient(rsocket)
            .singleRequestSingleResponse(
                requestPayload(RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE), null)
            .block();

    assertThat(result).isNull();
  }

  @Test
  void singlePropagatesContextToTransport() {
    AtomicReference<CoreSubscriber<? super Payload>> downstream = new AtomicReference<>();
    Mono<Payload> responses =
        new Mono<Payload>() {
          @Override
          public void subscribe(CoreSubscriber<? super Payload> actual) {
            downstream.set(actual);

            actual.onSubscribe(
                new Subscription() {
                  @Override
                  public void request(long n) {}

                  @Override
                  public void cancel() {}
                });
          }
        };

    RSocket rsocket = mock(RSocket.class);
    when(rsocket.requestResponse(any())).thenReturn(responses);

    Disposable subscription =
        new RSocketRpcClient(rsocket)
            .singleRequestSingleResponse(
                requestPayload(RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE), null)
            .contextWrite(Context.of("marker", "value"))
            .subscribe();

    assertThat(downstream.get().currentContext().getOrDefault("marker", "")).isEqualTo("value");

    subscription.dispose();
  }

  private static Payload garbageResponsePayload() {
    return ByteBufPayload.create(
        Unpooled.wrappedBuffer(new byte[] {1, 2, 3}), Unpooled.wrappedBuffer(new byte[] {4, 5, 6}));
  }

  private static Payload errorResponsePayload() {
    ResponseRpcMetadata metadata =
        new ResponseRpcMetadata.Builder()
            .setPayloadMetadata(
                PayloadMetadata.fromExceptionMetadata(
                    RpcPayloadUtil.undeclaredExceptionMetadata("boom", "boom")))
            .build();

    ByteBuf metadataBuf = Unpooled.buffer();
    metadata.write0(TProtocolType.TCompact.apply(metadataBuf));

    return ByteBufPayload.create(Unpooled.EMPTY_BUFFER, metadataBuf);
  }

  @SuppressWarnings("unchecked")
  private static ClientRequestPayload<Object> requestPayload(RpcKind kind) {
    ClientRequestPayload<Object> payload = mock(ClientRequestPayload.class);
    when(payload.getDataWriter()).thenReturn(out -> {});
    when(payload.getResponseReader()).thenReturn(protocol -> null);
    when(payload.getExceptionReaders()).thenReturn(Collections.emptyMap());
    when(payload.getPersistentHeaders()).thenReturn(Collections.emptyMap());
    when(payload.getRequestRpcMetadata())
        .thenReturn(
            new RequestRpcMetadata.Builder()
                .setProtocol(ProtocolId.COMPACT)
                .setName("ping")
                .setKind(kind)
                .build());
    return payload;
  }

  private static RequestRpcMetadata readMetadata(Payload payload) {
    return RequestRpcMetadata.read0(TProtocolType.TCompact.apply(payload.sliceMetadata()));
  }
}
