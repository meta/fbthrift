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
import com.facebook.thrift.client.SinkErrors;
import com.facebook.thrift.model.StreamResponse;
import com.facebook.thrift.payload.ClientRequestPayload;
import com.facebook.thrift.payload.ClientResponsePayload;
import com.facebook.thrift.payload.Reader;
import com.facebook.thrift.protocol.ByteBufTProtocol;
import com.facebook.thrift.protocol.TProtocolType;
import com.facebook.thrift.rsocket.util.MetadataUtil;
import com.facebook.thrift.rsocket.util.PayloadUtil;
import com.facebook.thrift.rsocket.util.RocketErrorUtil;
import com.facebook.thrift.test.rocket.TestServerException;
import com.facebook.thrift.util.ResponseAndStreamBridge;
import com.facebook.thrift.util.RpcPayloadUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketErrorException;
import io.rsocket.frame.ErrorFrameCodec;
import io.rsocket.util.ByteBufPayload;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.apache.thrift.CodecConfig;
import org.apache.thrift.CompressionAlgorithm;
import org.apache.thrift.CompressionConfig;
import org.apache.thrift.PayloadMetadata;
import org.apache.thrift.ProtocolId;
import org.apache.thrift.RequestRpcMetadata;
import org.apache.thrift.ResponseRpcMetadata;
import org.apache.thrift.RpcKind;
import org.apache.thrift.StreamPayloadMetadata;
import org.apache.thrift.ZstdCompressionCodecConfig;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.BaseSubscriber;
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
  void streamingRequestSendsTheCompressionConfig() {
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

    try {
      RequestRpcMetadata metadata = readMetadata(sent.get());

      assertThat(metadata.getCompression()).isEqualTo(CompressionAlgorithm.ZSTD);
      assertThat(metadata.getCompressionConfig()).isEqualTo(ZSTD_OPTIONS.getCompressionConfig());
    } finally {
      sent.get().release();
    }
  }

  @Test
  void onewayRequestDoesNotSendTheCompressionConfig() {
    RSocket rsocket = mock(RSocket.class);
    AtomicReference<Payload> sent = new AtomicReference<>();
    when(rsocket.fireAndForget(any()))
        .thenAnswer(
            invocation -> {
              sent.set(invocation.getArgument(0));
              return Mono.empty();
            });

    new RSocketRpcClient(rsocket)
        .singleRequestNoResponse(requestPayload(RpcKind.SINGLE_REQUEST_NO_RESPONSE), ZSTD_OPTIONS)
        .block();

    try {
      RequestRpcMetadata metadata = readMetadata(sent.get());

      assertThat(metadata.getCompression()).isEqualTo(CompressionAlgorithm.ZSTD);
      assertThat(metadata.getCompressionConfig()).isNull();
    } finally {
      sent.get().release();
    }
  }

  @Test
  void channelSinkItemsUseOptionsCompression() throws Exception {
    AtomicReference<List<Payload>> sent = new AtomicReference<>();
    CountDownLatch done = new CountDownLatch(1);
    RSocket rsocket = captureChannelFrames(sent, done);

    Disposable subscription =
        new RSocketRpcClient(rsocket)
            .streamingRequestStreamingResponse(
                Flux.just(
                    requestPayloadWithData(RpcKind.BIDIRECTIONAL_STREAM, "frame-0"),
                    requestPayloadWithData(RpcKind.BIDIRECTIONAL_STREAM, "frame-1"),
                    requestPayloadWithData(RpcKind.BIDIRECTIONAL_STREAM, "frame-2")),
                ZSTD_OPTIONS)
            .subscribe();

    assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();

    List<Payload> frames = sent.get();

    try {
      assertThat(frames).hasSize(3);

      RequestRpcMetadata firstMetadata = readMetadata(frames.get(0));
      assertThat(firstMetadata.getCompression()).isEqualTo(CompressionAlgorithm.ZSTD);
      assertThat(firstMetadata.getCompressionConfig())
          .isEqualTo(ZSTD_OPTIONS.getCompressionConfig());

      assertSinkFrameDecompressesTo(frames.get(1), "frame-1");
      assertSinkFrameDecompressesTo(frames.get(2), "frame-2");
    } finally {
      subscription.dispose();

      for (Payload frame : frames) {
        frame.release();
      }
    }
  }

  @Test
  void channelSinkItemsRespectPerFrameSizeLimit() throws Exception {
    RpcOptions options = new RpcOptions.Builder().setCompressionConfig(zstdConfig(64L)).build();

    AtomicReference<List<Payload>> sent = new AtomicReference<>();
    CountDownLatch done = new CountDownLatch(1);
    RSocket rsocket = captureChannelFrames(sent, done);

    String largeText = String.join("", Collections.nCopies(500, "a"));

    Disposable subscription =
        new RSocketRpcClient(rsocket)
            .streamingRequestStreamingResponse(
                Flux.just(
                    requestPayloadWithData(RpcKind.BIDIRECTIONAL_STREAM, "first"),
                    requestPayloadWithData(RpcKind.BIDIRECTIONAL_STREAM, "tiny"),
                    requestPayloadWithData(RpcKind.BIDIRECTIONAL_STREAM, largeText)),
                options)
            .subscribe();

    assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();

    List<Payload> frames = sent.get();

    try {
      assertThat(frames).hasSize(3);

      assertUncompressedSinkFrameIs(frames.get(1), "tiny");
      assertSinkFrameDecompressesTo(frames.get(2), largeText);
    } finally {
      subscription.dispose();

      for (Payload frame : frames) {
        frame.release();
      }
    }
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
  void streamingResponseDecodesOffEventLoop() {
    Scheduler scheduler = Schedulers.newSingle("test-offloop");

    try {
      RSocket rsocket = mock(RSocket.class);
      when(rsocket.requestStream(any())).thenReturn(Flux.just(errorResponsePayload()));

      AtomicReference<String> decodeThread = new AtomicReference<>();

      List<ClientResponsePayload<Object>> results =
          new RSocketRpcClient(rsocket, scheduler)
              .singleRequestStreamingResponse(
                  requestPayload(RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE), null)
              .doOnNext(
                  ignored -> decodeThread.compareAndSet(null, Thread.currentThread().getName()))
              .collectList()
              .block();

      assertThat(results).hasSize(1);
      assertThat(results.get(0).getException()).hasMessage("boom");
      assertThat(decodeThread.get().toLowerCase()).contains("offloop");
    } finally {
      scheduler.dispose();
    }
  }

  @Test
  void channelResponseDecodesOffEventLoop() {
    Scheduler scheduler = Schedulers.newSingle("test-offloop");

    try {
      RSocket rsocket = mock(RSocket.class);
      when(rsocket.requestChannel(any())).thenReturn(Flux.just(errorResponsePayload()));

      AtomicReference<String> decodeThread = new AtomicReference<>();

      List<ClientResponsePayload<Object>> results =
          new RSocketRpcClient(rsocket, scheduler)
              .streamingRequestStreamingResponse(
                  Flux.just(requestPayload(RpcKind.BIDIRECTIONAL_STREAM)), null)
              .doOnNext(
                  ignored -> decodeThread.compareAndSet(null, Thread.currentThread().getName()))
              .collectList()
              .block();

      assertThat(results).hasSize(1);
      assertThat(results.get(0).getException()).hasMessage("boom");
      assertThat(decodeThread.get().toLowerCase()).contains("offloop");
    } finally {
      scheduler.dispose();
    }
  }

  @ParameterizedTest
  @MethodSource("streamCompressionCases")
  void streamDecodesPerFrameCompression(
      Interaction interaction, CompressionAlgorithm firstCompression, boolean offLoop)
      throws Exception {
    Payload first = streamFrame("first", firstCompression, true);
    Payload item = streamFrame("item", CompressionAlgorithm.ZSTD, false);

    RSocket rsocket = mock(RSocket.class);
    stubTransport(rsocket, interaction, Flux.just(first, item));

    Scheduler scheduler =
        offLoop ? Schedulers.newSingle("test-stream-compression") : Schedulers.immediate();

    try {
      List<Object> values =
          call(
                  new RSocketRpcClient(rsocket, scheduler),
                  interaction,
                  stringStreamPayload(interactionKind(interaction)))
              .map(RSocketRpcClientTest::decodedData)
              .collectList()
              .block();

      assertThat(values).containsExactly("first", "item");
      assertThat(first.refCnt()).isZero();
      assertThat(item.refCnt()).isZero();
    } finally {
      if (offLoop) {
        scheduler.dispose();
      }
    }
  }

  @Test
  void streamingCustomCompressionFailsWithoutLeak() throws Exception {
    Payload first = streamFrame("first", null, true);
    Payload second = rawStreamFrame("second", CompressionAlgorithm.CUSTOM, false);

    RSocket rsocket = mock(RSocket.class);
    when(rsocket.requestStream(any())).thenReturn(Flux.just(first, second));

    assertThatThrownBy(
            () ->
                new RSocketRpcClient(rsocket, Schedulers.immediate())
                    .singleRequestStreamingResponse(
                        stringStreamPayload(RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE), null)
                    .collectList()
                    .block())
        .isInstanceOf(UnsupportedOperationException.class);

    assertThat(first.refCnt()).isZero();
    assertThat(second.refCnt()).isZero();
  }

  @ParameterizedTest
  @EnumSource(Interaction.class)
  void cancelReleasesQueuedPayloads(Interaction interaction) {
    ManualScheduler scheduler = new ManualScheduler();
    RSocket rsocket = mock(RSocket.class);
    Payload first = errorResponsePayload();
    Payload queued = errorResponsePayload();
    first.retain();
    queued.retain();
    stubTransport(rsocket, interaction, Flux.just(first, queued));

    Disposable subscription =
        call(
                new RSocketRpcClient(rsocket, scheduler),
                interaction,
                requestPayload(interactionKind(interaction)))
            .subscribe();

    subscription.dispose();

    assertThat(scheduler.tasks).hasSize(1);

    scheduler.runAll();

    assertThat(first.refCnt()).isEqualTo(1);
    assertThat(queued.refCnt()).isEqualTo(1);

    first.release();
    queued.release();
  }

  @Test
  void streamingHoldsAtMostInitialCreditsWithoutDemand() {
    ManualScheduler scheduler = new ManualScheduler();
    AtomicInteger created = new AtomicInteger();
    List<Payload> live = Collections.synchronizedList(new ArrayList<>());
    Flux<Payload> responses = Flux.range(0, 1000).map(ignored -> admitFrame(live, created));

    RSocket rsocket = mock(RSocket.class);
    when(rsocket.requestStream(any())).thenReturn(responses);

    Disposable subscription =
        new RSocketRpcClient(rsocket, scheduler)
            .singleRequestStreamingResponse(
                requestPayload(RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE), null)
            .subscribeWith(
                new BaseSubscriber<ClientResponsePayload<Object>>() {
                  @Override
                  protected void hookOnSubscribe(Subscription subscription) {}
                });

    int admitted = created.get();

    subscription.dispose();

    scheduler.runAll();

    assertThat(admitted).isEqualTo(ResponseAndStreamBridge.INITIAL_CREDITS);

    for (Payload frame : live) {
      assertThat(frame.refCnt()).isZero();
    }
  }

  private static Payload admitFrame(List<Payload> live, AtomicInteger created) {
    created.incrementAndGet();

    Payload frame = errorResponsePayload();
    live.add(frame);

    return frame;
  }

  @ParameterizedTest
  @EnumSource(Interaction.class)
  void flagsOffRocketErrorDecodesToErrorFrame(Interaction interaction) {
    RSocket rsocket = mock(RSocket.class);
    stubTransport(rsocket, interaction, Flux.error(RocketErrorUtil.taskExpired("ping", 5000L)));

    ClientResponsePayload<Object> result =
        call(
                new RSocketRpcClient(rsocket, Schedulers.immediate()),
                interaction,
                requestPayload(interactionKind(interaction)))
            .blockLast();

    assertThat(result.getException()).hasMessage("TASK_EXPIRED");
  }

  @Test
  void streamingRocketErrorDecodesToErrorFrame() {
    RSocket rsocket = mock(RSocket.class);
    when(rsocket.requestStream(any()))
        .thenReturn(Flux.error(RocketErrorUtil.taskExpired("ping", 5000L)));

    ClientResponsePayload<Object> result =
        new RSocketRpcClient(rsocket)
            .singleRequestStreamingResponse(
                requestPayload(RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE), null)
            .blockLast();

    assertThat(result.getException()).hasMessage("TASK_EXPIRED");
  }

  @Test
  void streamingRocketErrorMappingThrowDeliversBothErrors() {
    RSocket rsocket = mock(RSocket.class);
    RSocketErrorException failure = new RSocketErrorException(ErrorFrameCodec.CANCELED, null);
    when(rsocket.requestStream(any())).thenReturn(Flux.error(failure));

    assertThatThrownBy(
            () ->
                new RSocketRpcClient(rsocket)
                    .singleRequestStreamingResponse(
                        requestPayload(RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE), null)
                    .blockLast())
        .satisfies(
            e -> {
              assertThat(e).isNotSameAs(failure);
              // blockLast appends its own "#block terminated" suppressed marker.
              assertThat(e.getSuppressed()).contains(failure);
            });
  }

  @ParameterizedTest
  @EnumSource(Interaction.class)
  void flagsOffPassesDemandToTransport(Interaction interaction) {
    RSocket rsocket = mock(RSocket.class);
    List<Long> requests = Collections.synchronizedList(new ArrayList<>());
    List<Payload> live = Collections.synchronizedList(new ArrayList<>());
    AtomicInteger sent = new AtomicInteger();
    stubTransport(
        rsocket,
        interaction,
        Flux.create(
            sink ->
                sink.onRequest(
                    n -> {
                      requests.add(n);

                      while (sent.get() < 3) {
                        Payload frame = errorResponsePayload();
                        frame.retain();
                        live.add(frame);
                        sent.incrementAndGet();
                        sink.next(frame);
                      }

                      sink.complete();
                    })));

    List<ClientResponsePayload<Object>> received = new ArrayList<>();

    call(
            new RSocketRpcClient(rsocket, Schedulers.immediate()),
            interaction,
            requestPayload(interactionKind(interaction)))
        .subscribeWith(
            new BaseSubscriber<ClientResponsePayload<Object>>() {
              @Override
              protected void hookOnSubscribe(Subscription subscription) {
                request(1);
              }

              @Override
              protected void hookOnNext(ClientResponsePayload<Object> value) {
                received.add(value);

                if (received.size() == 1) {
                  request(2);
                }
              }
            });

    assertThat(requests).containsExactly(1L, 2L);
    assertThat(received).hasSize(3);

    for (Payload frame : live) {
      assertThat(frame.refCnt()).isEqualTo(1);

      frame.release();
    }
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

  @Test
  void streamPropagatesContextToTransport() {
    AtomicReference<CoreSubscriber<? super Payload>> downstream = new AtomicReference<>();
    Flux<Payload> responses =
        new Flux<Payload>() {
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
    when(rsocket.requestStream(any())).thenReturn(responses);

    Disposable subscription =
        new RSocketRpcClient(rsocket)
            .singleRequestStreamingResponse(
                requestPayload(RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE), null)
            .contextWrite(Context.of("marker", "value"))
            .subscribe();

    assertThat(downstream.get().currentContext().getOrDefault("marker", "")).isEqualTo("value");

    subscription.dispose();
  }

  @Test
  void resubscribeDecodesFirstResponseOnOffLoopScheduler() throws Exception {
    resubscribeDecodesFirstResponsePerSubscription(new ManualScheduler());
  }

  @Test
  void resubscribeDecodesFirstResponseInline() throws Exception {
    resubscribeDecodesFirstResponsePerSubscription(Schedulers.immediate());
  }

  private void resubscribeDecodesFirstResponsePerSubscription(Scheduler scheduler)
      throws Exception {
    RSocket rsocket = mock(RSocket.class);
    Payload first = structFrame();
    Payload second = structFrame();
    first.retain(2);
    second.retain(2);
    when(rsocket.requestStream(any())).thenReturn(Flux.just(first, second));

    Flux<ClientResponsePayload<Object>> call =
        new RSocketRpcClient(rsocket, scheduler)
            .singleRequestStreamingResponse(
                firstResponsePayload(RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE), null);

    List<Object> firstValues = new ArrayList<>();
    call.map(RSocketRpcClientTest::decodedData).subscribe(firstValues::add);

    if (scheduler instanceof ManualScheduler) {
      ((ManualScheduler) scheduler).runAll();
    }

    List<Object> secondValues = new ArrayList<>();
    call.map(RSocketRpcClientTest::decodedData).subscribe(secondValues::add);

    if (scheduler instanceof ManualScheduler) {
      ((ManualScheduler) scheduler).runAll();
    }

    // A decoder shared across subscriptions would read the second first frame as data.
    assertThat(firstValues).containsExactly("first", "data");
    assertThat(secondValues).containsExactly("first", "data");
    assertThat(first.refCnt()).isEqualTo(1);
    assertThat(second.refCnt()).isEqualTo(1);

    first.release();
    second.release();
  }

  private static Object decodedData(ClientResponsePayload<Object> payload) {
    Object data = payload.getData();

    if (data instanceof StreamResponse) {
      StreamResponse<?, ?> response = (StreamResponse<?, ?>) data;

      return response.isSetFirstResponse() ? response.getFirstResponse() : response.getData();
    }

    return data;
  }

  private static Payload structFrame() throws Exception {
    ByteBuf data = Unpooled.buffer();
    ByteBufTProtocol out = TProtocolType.TCompact.apply(data);
    out.writeStructBegin(new TStruct());
    out.writeFieldBegin(new TField("value", TType.STRING, (short) 0));
    out.writeString("v");
    out.writeFieldEnd();
    out.writeFieldStop();
    out.writeStructEnd();

    return ByteBufPayload.create(data);
  }

  @SuppressWarnings("unchecked")
  private static ClientRequestPayload<Object> firstResponsePayload(RpcKind kind) {
    ClientRequestPayload<Object> payload = mock(ClientRequestPayload.class);
    when(payload.getDataWriter()).thenReturn(out -> {});
    when(payload.getResponseReader()).thenReturn(protocol -> "data");
    when(payload.getFirstResponseReader()).thenReturn(protocol -> "first");
    when(payload.getExceptionReaders()).thenReturn(Collections.emptyMap());
    when(payload.getStreamExceptionReaders()).thenReturn(Collections.emptyMap());
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

  @Test
  void streamingUsesBoundedWireWindow() throws Exception {
    ManualScheduler scheduler = new ManualScheduler();
    AtomicLong initialRequest = new AtomicLong(-1);
    Flux<Payload> responses =
        Flux.create(
            sink ->
                sink.onRequest(
                    n -> {
                      if (initialRequest.compareAndSet(-1, n)) {
                        sink.complete();
                      }
                    }));

    RSocket rsocket = mock(RSocket.class);
    when(rsocket.requestStream(any())).thenReturn(responses);

    CountDownLatch terminal = new CountDownLatch(1);

    new RSocketRpcClient(rsocket, scheduler)
        .singleRequestStreamingResponse(
            requestPayload(RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE), null)
        .subscribe(value -> {}, error -> terminal.countDown(), terminal::countDown);

    scheduler.runAll();

    assertThat(terminal.await(5, TimeUnit.SECONDS)).isTrue();

    // Whatever the downstream wants, the handoff asks the transport for the initial window
    // up front.
    assertThat(initialRequest.get()).isEqualTo(ResponseAndStreamBridge.INITIAL_CREDITS);
  }

  @Test
  void sinkErrorFrameCarriesExceptionMetadata() throws Exception {
    RSocket rsocket = mock(RSocket.class);
    List<Payload> sent = new ArrayList<>();
    when(rsocket.requestChannel(any()))
        .thenAnswer(
            invocation -> {
              Publisher<Payload> outbound = invocation.getArgument(0);
              return Flux.from(outbound).doOnNext(sent::add).thenMany(Flux.<Payload>empty());
            });

    ClientRequestPayload<Object> first = requestPayload(RpcKind.SINK);
    ClientRequestPayload<Object> error =
        SinkErrors.sinkError(first, new TestServerException("boom"), (short) 1);

    new RSocketRpcClient(rsocket, Schedulers.immediate())
        .streamingRequestStreamingResponse(
            Flux.just(first, requestPayload(RpcKind.SINK), error), RpcOptions.EMPTY)
        .blockLast();

    assertThat(sent).hasSize(3);

    try {
      assertThat(MetadataUtil.decodeStreamingPayloadMetadata(sent.get(1)).getPayloadMetadata())
          .isNull();

      assertThat(
              MetadataUtil.decodeStreamingPayloadMetadata(sent.get(2))
                  .getPayloadMetadata()
                  .getExceptionMetadata()
                  .getMetadata()
                  .isSetDeclaredException())
          .isTrue();
    } finally {
      sent.forEach(Payload::release);
    }
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

  private static RpcKind interactionKind(Interaction interaction) {
    return interaction == Interaction.STREAM
        ? RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE
        : RpcKind.BIDIRECTIONAL_STREAM;
  }

  private static void stubTransport(
      RSocket rsocket, Interaction interaction, Flux<Payload> responses) {
    if (interaction == Interaction.STREAM) {
      when(rsocket.requestStream(any())).thenReturn(responses);
    } else {
      when(rsocket.requestChannel(any())).thenReturn(responses);
    }
  }

  private static Flux<ClientResponsePayload<Object>> call(
      RSocketRpcClient client, Interaction interaction, ClientRequestPayload<Object> request) {
    if (interaction == Interaction.STREAM) {
      return client.singleRequestStreamingResponse(request, null);
    }

    return client.streamingRequestStreamingResponse(Flux.just(request), null);
  }

  private enum Interaction {
    STREAM,
    CHANNEL,
  }

  private static RequestRpcMetadata readMetadata(Payload payload) {
    return RequestRpcMetadata.read0(TProtocolType.TCompact.apply(payload.sliceMetadata()));
  }

  private static StreamPayloadMetadata readStreamMetadata(Payload payload) {
    return StreamPayloadMetadata.read0(TProtocolType.TCompact.apply(payload.sliceMetadata()));
  }

  private static RSocket captureChannelFrames(
      AtomicReference<List<Payload>> sent, CountDownLatch done) {
    RSocket rsocket = mock(RSocket.class);

    when(rsocket.requestChannel(any()))
        .thenAnswer(
            invocation -> {
              Publisher<Payload> outbound = invocation.getArgument(0);

              return Flux.from(outbound)
                  .collectList()
                  .doOnNext(
                      frames -> {
                        sent.set(frames);
                        done.countDown();
                      })
                  .thenMany(Flux.never());
            });

    return rsocket;
  }

  private static void assertSinkFrameDecompressesTo(Payload frame, String expected)
      throws Exception {
    StreamPayloadMetadata metadata = readStreamMetadata(frame);
    assertThat(metadata.getCompression()).isEqualTo(CompressionAlgorithm.ZSTD);

    ByteBuf decoded =
        PayloadUtil.getData(ByteBufAllocator.DEFAULT, metadata.getCompression(), frame);

    try {
      TProtocol protocol = TProtocolType.TCompact.apply(decoded);
      protocol.readStructBegin();

      assertThat(protocol.readString()).isEqualTo(expected);
    } finally {
      decoded.release();
    }
  }

  private static void assertUncompressedSinkFrameIs(Payload frame, String expected)
      throws Exception {
    StreamPayloadMetadata metadata = readStreamMetadata(frame);
    assertThat(metadata.getCompression()).isNull();

    TProtocol protocol = TProtocolType.TCompact.apply(frame.sliceData());
    protocol.readStructBegin();

    assertThat(protocol.readString()).isEqualTo(expected);
  }

  private static ClientRequestPayload<Object> requestPayloadWithData(RpcKind kind, String text) {
    ClientRequestPayload<Object> payload = requestPayload(kind);

    when(payload.getDataWriter()).thenReturn(out -> out.writeString(text));

    return payload;
  }

  private static ClientRequestPayload<Object> stringStreamPayload(RpcKind kind) {
    ClientRequestPayload<Object> payload = requestPayload(kind);

    Reader<Object> stringReader = TProtocol::readString;
    when(payload.getResponseReader()).thenReturn(stringReader);
    when(payload.getFirstResponseReader()).thenReturn(stringReader);

    return payload;
  }

  private static Payload streamFrame(String text, CompressionAlgorithm compression, boolean first)
      throws Exception {
    ByteBuf data = Unpooled.buffer();
    ByteBuf metadata = Unpooled.buffer();

    try {
      writeStreamFrame(text, compression, first, data, metadata);

      return PayloadUtil.createPayload(ByteBufAllocator.DEFAULT, compression, data, metadata);
    } catch (Throwable t) {
      ReferenceCountUtil.safeRelease(data);
      ReferenceCountUtil.safeRelease(metadata);

      throw Exceptions.propagate(t);
    }
  }

  private static Payload rawStreamFrame(
      String text, CompressionAlgorithm compression, boolean first) throws Exception {
    ByteBuf data = Unpooled.buffer();
    ByteBuf metadata = Unpooled.buffer();

    try {
      writeStreamFrame(text, compression, first, data, metadata);

      return ByteBufPayload.create(data, metadata);
    } catch (Throwable t) {
      ReferenceCountUtil.safeRelease(data);
      ReferenceCountUtil.safeRelease(metadata);

      throw Exceptions.propagate(t);
    }
  }

  private static void writeStreamFrame(
      String text, CompressionAlgorithm compression, boolean first, ByteBuf data, ByteBuf metadata)
      throws Exception {
    ByteBufTProtocol out = TProtocolType.TCompact.apply(data);
    out.writeStructBegin(new TStruct());
    out.writeFieldBegin(new TField("value", TType.STRING, (short) 0));
    out.writeString(text);
    out.writeFieldEnd();
    out.writeFieldStop();
    out.writeStructEnd();

    ByteBufTProtocol metadataOut = TProtocolType.TCompact.apply(metadata);

    if (first) {
      ResponseRpcMetadata rpcMetadata =
          compression == null
              ? new ResponseRpcMetadata.Builder().build()
              : new ResponseRpcMetadata.Builder().setCompression(compression).build();
      rpcMetadata.write0(metadataOut);
    } else {
      StreamPayloadMetadata streamMetadata =
          compression == null
              ? new StreamPayloadMetadata.Builder().build()
              : new StreamPayloadMetadata.Builder().setCompression(compression).build();
      streamMetadata.write0(metadataOut);
    }
  }

  private static Stream<Arguments> streamCompressionCases() {
    List<Arguments> cases = new ArrayList<>();

    for (Interaction interaction : Interaction.values()) {
      for (CompressionAlgorithm firstCompression : Arrays.asList(null, CompressionAlgorithm.ZSTD)) {
        cases.add(Arguments.of(interaction, firstCompression, false));
        cases.add(Arguments.of(interaction, firstCompression, true));
      }
    }

    return cases.stream();
  }

  private static CompressionConfig zstdConfig(long sizeLimit) {
    return new CompressionConfig.Builder()
        .setCodecConfig(
            CodecConfig.fromZstdConfig(new ZstdCompressionCodecConfig.Builder().build()))
        .setCompressionSizeLimit(sizeLimit)
        .build();
  }
}
