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

package com.facebook.thrift.legacy.client;

import static com.facebook.thrift.legacy.client.LegacyTestFixtures.inboundReply;
import static com.facebook.thrift.legacy.client.LegacyTestFixtures.mockProbingPayload;
import static com.facebook.thrift.legacy.client.LegacyTestFixtures.writeReply;
import static com.facebook.thrift.util.ThriftProtocolConstants.HEADER_FLAGS_NONE;
import static com.facebook.thrift.util.ThriftProtocolConstants.HEADER_MAGIC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.facebook.thrift.client.RpcOptions;
import com.facebook.thrift.legacy.codec.HeaderTransportCodec;
import com.facebook.thrift.legacy.codec.LegacyTransportType;
import com.facebook.thrift.legacy.codec.ThriftFrame;
import com.facebook.thrift.payload.ClientRequestPayload;
import com.facebook.thrift.payload.ClientResponsePayload;
import com.facebook.thrift.protocol.TProtocolType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.thrift.CodecConfig;
import org.apache.thrift.CompressionConfig;
import org.apache.thrift.ProtocolId;
import org.apache.thrift.RequestRpcMetadata;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TTransform;
import org.apache.thrift.ZstdCompressionCodecConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import reactor.core.Disposables;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

class ThriftClientHandlerTest {
  private final List<Object> dropped = Collections.synchronizedList(new ArrayList<>());

  private Scheduler scheduler;

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

  @AfterEach
  void disposeScheduler() {
    if (scheduler != null) {
      scheduler.dispose();
      scheduler = null;
    }
  }

  /**
   * A response whose sequenceId matches no in-flight request (a duplicate, late, or otherwise
   * unexpected response) must not leak its decoded frame. Before the fix, {@code
   * requestContexts.remove(sequenceId)} returned null and the handler dereferenced it (NPE) before
   * wiring the {@code doFinally(... frame.release())}, leaking the underlying direct buffer.
   */
  @Test
  void releasesFrameWhenNoRequestContextMatchesSequenceId() {
    EmbeddedChannel channel =
        new EmbeddedChannel(new ThriftClientHandler(LegacyTransportType.HEADER));

    ByteBuf message = Unpooled.buffer().writeZero(8);
    ThriftFrame frame =
        new ThriftFrame(
            4242, // sequenceId with no matching in-flight request
            message,
            Collections.emptyMap(),
            Collections.emptyMap(),
            LegacyTransportType.HEADER,
            TProtocolType.TCompact,
            false);

    channel.writeInbound(frame);

    assertThat(message.refCnt()).isZero();

    channel.finishAndReleaseAll();
  }

  /**
   * A request written after the channel closed never reaches {@link ThriftClientHandler} -- the
   * pipeline has been torn down, so the {@link RequestContext} travels straight to head, which
   * disposes of it with {@code ReferenceCountUtil.release(msg)}. That is a no-op unless the context
   * itself is reference counted, which leaked the request buffer allocated by {@code
   * LegacyRpcClient.encodeRequest}.
   */
  @Test
  void releasesEncodedRequestWhenChannelClosedBeforeWrite() {
    EmbeddedChannel channel =
        new EmbeddedChannel(new ThriftClientHandler(LegacyTransportType.HEADER));
    channel.close().syncUninterruptibly();
    channel.runPendingTasks();

    ByteBuf encodedRequest = Unpooled.buffer().writeZero(8);
    channel.writeAndFlush(newRequestContext(mock(ClientRequestPayload.class), encodedRequest));

    assertThat(encodedRequest.refCnt()).isZero();
  }

  /**
   * Encoding the frame reads from the payload and can throw. The request buffer is owned by the
   * context until the frame takes it over, so a failure before that hand-off must release the
   * context.
   */
  @Test
  void releasesEncodedRequestWhenFrameEncodingFails() {
    EmbeddedChannel channel =
        new EmbeddedChannel(new ThriftClientHandler(LegacyTransportType.HEADER));

    ClientRequestPayload<?> payload = mock(ClientRequestPayload.class);
    when(payload.getRequestRpcMetadata()).thenThrow(new IllegalStateException("boom"));

    ByteBuf encodedRequest = Unpooled.buffer().writeZero(8);
    channel.writeAndFlush(newRequestContext(payload, encodedRequest));

    assertThat(encodedRequest.refCnt()).isZero();

    channel.finishAndReleaseAll();
  }

  @Test
  @SuppressWarnings("unchecked")
  void requestFrameCarriesTheTransformOfItsCompressionConfig() {
    EmbeddedChannel channel =
        new EmbeddedChannel(new ThriftClientHandler(LegacyTransportType.HEADER));
    ClientRequestPayload<?> payload = mock(ClientRequestPayload.class);
    when(payload.getRequestRpcMetadata())
        .thenReturn(
            new RequestRpcMetadata.Builder()
                .setProtocol(ProtocolId.COMPACT)
                .setOtherMetadata(new HashMap<>())
                .build());
    when(payload.getPersistentHeaders()).thenReturn(Collections.emptyMap());
    RpcOptions options =
        new RpcOptions.Builder()
            .setCompressionConfig(
                new CompressionConfig.Builder()
                    .setCodecConfig(
                        CodecConfig.fromZstdConfig(
                            new ZstdCompressionCodecConfig.Builder().build()))
                    .build())
            .build();

    channel.writeAndFlush(
        new RequestContext<>(
            Sinks.one(),
            (ClientRequestPayload<Object>) payload,
            Unpooled.buffer().writeZero(8),
            options,
            false,
            4244));
    ThriftFrame frame = channel.readOutbound();

    assertThat(frame.getTransforms()).containsExactly(TTransform.ZSTD.getValue());
    frame.release();
    channel.finishAndReleaseAll();
  }

  @Test
  void decodeRunsOnInjectedScheduler() throws Exception {
    scheduler = Schedulers.newSingle("test-offloop");

    EmbeddedChannel channel =
        new EmbeddedChannel(new ThriftClientHandler(LegacyTransportType.HEADER, scheduler));

    AtomicReference<Thread> readerThread = new AtomicReference<>();
    ClientRequestPayload<Object> payload = mockProbingPayload(readerThread);
    Sinks.One<ClientResponsePayload<Object>> processor = Sinks.one();
    ThriftFrame response = inboundReply(4246, "probe");
    ByteBuf witness = response.getMessage().retain();

    sendRequest(channel, payload, processor, 4246);

    channel.writeInbound(response);

    ClientResponsePayload<Object> result = processor.asMono().block(Duration.ofSeconds(10));

    assertThat(result).isNotNull();

    awaitIdle(scheduler);

    assertThat(witness.refCnt()).isEqualTo(1);
    assertThat(readerThread.get().getName()).startsWith("test-offloop");

    witness.release();

    channel.finishAndReleaseAll();
  }

  @Test
  void immediateSchedulerDecodesOnCallingThread() throws Exception {
    EmbeddedChannel channel =
        new EmbeddedChannel(
            new ThriftClientHandler(LegacyTransportType.HEADER, Schedulers.immediate()));

    AtomicReference<Thread> readerThread = new AtomicReference<>();
    ClientRequestPayload<Object> payload = mockProbingPayload(readerThread);
    Sinks.One<ClientResponsePayload<Object>> processor = Sinks.one();
    ThriftFrame response = inboundReply(4247, "probe");
    ByteBuf witness = response.getMessage().retain();

    sendRequest(channel, payload, processor, 4247);

    channel.writeInbound(response);

    ClientResponsePayload<Object> result = processor.asMono().block(Duration.ofSeconds(10));

    assertThat(result).isNotNull();
    assertThat(witness.refCnt()).isEqualTo(1);
    assertThat(readerThread.get()).isSameAs(Thread.currentThread());

    witness.release();

    channel.finishAndReleaseAll();
  }

  /**
   * A bounded off-loop scheduler rejects under load shedding. The retained frame must be released
   * and the sink must fail with the rejection instead of hanging.
   */
  @Test
  void rejectedOffLoopScheduleReleasesFrameAndErrorsSink() throws Exception {
    Scheduler scheduler = mock(Scheduler.class);
    when(scheduler.schedule(any(Runnable.class))).thenThrow(new RejectedExecutionException("full"));

    EmbeddedChannel channel =
        new EmbeddedChannel(new ThriftClientHandler(LegacyTransportType.HEADER, scheduler));

    AtomicReference<Thread> readerThread = new AtomicReference<>();
    ClientRequestPayload<Object> payload = mockProbingPayload(readerThread);
    Sinks.One<ClientResponsePayload<Object>> processor = Sinks.one();
    AtomicReference<Throwable> sinkError = new AtomicReference<>();
    processor.asMono().subscribe(v -> {}, sinkError::set);

    ThriftFrame response = inboundReply(4248, "probe");
    ByteBuf witness = response.getMessage().retain();

    sendRequest(channel, payload, processor, 4248);

    channel.writeInbound(response);

    assertThat(witness.refCnt()).isEqualTo(1);
    assertThat(sinkError.get()).isInstanceOf(RejectedExecutionException.class);
    assertThat(readerThread.get()).isNull();

    witness.release();

    channel.finishAndReleaseAll();
  }

  @Test
  void disposedSubscriptionStillReleasesFrameAfterOffLoopDecode() throws Exception {
    AtomicReference<Runnable> task = new AtomicReference<>();
    EmbeddedChannel channel =
        new EmbeddedChannel(
            new ThriftClientHandler(LegacyTransportType.HEADER, newCapturingScheduler(task)));

    AtomicReference<Thread> readerThread = new AtomicReference<>();
    ClientRequestPayload<Object> payload = mockProbingPayload(readerThread);
    Sinks.One<ClientResponsePayload<Object>> processor = Sinks.one();
    processor.asMono().subscribe(v -> {}).dispose();

    ThriftFrame response = inboundReply(4249, "probe");
    ByteBuf witness = response.getMessage().retain();

    sendRequest(channel, payload, processor, 4249);

    channel.writeInbound(response);
    task.get().run();

    assertThat(witness.refCnt()).isEqualTo(1);
    assertThat(processor.asMono().block(Duration.ofSeconds(10))).isNotNull();

    witness.release();

    channel.finishAndReleaseAll();
  }

  @Test
  void channelCloseBeforeOffLoopDecodeStillReleasesFrame() throws Exception {
    AtomicReference<Runnable> task = new AtomicReference<>();
    EmbeddedChannel channel =
        new EmbeddedChannel(
            new ThriftClientHandler(LegacyTransportType.HEADER, newCapturingScheduler(task)));

    AtomicReference<Thread> readerThread = new AtomicReference<>();
    ClientRequestPayload<Object> payload = mockProbingPayload(readerThread);
    Sinks.One<ClientResponsePayload<Object>> processor = Sinks.one();

    ThriftFrame response = inboundReply(4250, "probe");
    ByteBuf witness = response.getMessage().retain();

    sendRequest(channel, payload, processor, 4250);

    channel.writeInbound(response);
    channel.close().syncUninterruptibly();
    channel.runPendingTasks();
    task.get().run();

    assertThat(witness.refCnt()).isEqualTo(1);
    assertThat(processor.asMono().block(Duration.ofSeconds(10))).isNotNull();

    witness.release();

    channel.finishAndReleaseAll();
  }

  @Test
  void offLoopDecodeFailureErrorsSinkAndReleasesFrame() throws Exception {
    AtomicReference<Runnable> task = new AtomicReference<>();
    EmbeddedChannel channel =
        new EmbeddedChannel(
            new ThriftClientHandler(LegacyTransportType.HEADER, newCapturingScheduler(task)));

    AtomicReference<Thread> readerThread = new AtomicReference<>();
    ClientRequestPayload<Object> payload = mockProbingPayload(readerThread);
    Sinks.One<ClientResponsePayload<Object>> processor = Sinks.one();
    AtomicReference<Throwable> sinkError = new AtomicReference<>();
    processor.asMono().subscribe(v -> {}, sinkError::set);

    ThriftFrame response = inboundReply(4251, "other");
    ByteBuf witness = response.getMessage().retain();

    sendRequest(channel, payload, processor, 4251);

    channel.writeInbound(response);
    task.get().run();

    assertThat(witness.refCnt()).isEqualTo(1);
    assertThat(sinkError.get()).isInstanceOf(TApplicationException.class);
    assertThat(readerThread.get()).isNull();

    witness.release();

    channel.finishAndReleaseAll();
  }

  @Test
  void compressedResponseDecompressesAndDecodesOffTheLoop() throws Exception {
    scheduler = Schedulers.newSingle("test-offloop");

    EmbeddedChannel channel =
        new EmbeddedChannel(
            new HeaderTransportCodec(false),
            new ThriftClientHandler(LegacyTransportType.HEADER, scheduler));

    AtomicReference<Thread> readerThread = new AtomicReference<>();
    ClientRequestPayload<Object> payload = mockProbingPayload(readerThread);
    Sinks.One<ClientResponsePayload<Object>> processor = Sinks.one();
    ByteBuf wire = compressedReplyWire(4252);
    ByteBuf witness = wire.retain();

    sendRequest(channel, payload, processor, 4252);

    channel.writeInbound(wire);

    ClientResponsePayload<Object> result = processor.asMono().block(Duration.ofSeconds(10));

    assertThat(result).isNotNull();
    assertThat(witness.refCnt()).isEqualTo(1);
    assertThat(readerThread.get().getName()).startsWith("test-offloop");

    witness.release();

    channel.finishAndReleaseAll();
  }

  @ParameterizedTest
  @EnumSource(TransformCase.class)
  void malformedOrUnknownHeaderTransformFailsOnlyMatchedRequest(TransformCase transformCase)
      throws Exception {
    scheduler = Schedulers.newSingle("test-offloop");

    EmbeddedChannel channel =
        new EmbeddedChannel(
            new HeaderTransportCodec(false),
            new ThriftClientHandler(LegacyTransportType.HEADER, scheduler));

    AtomicReference<Thread> readerA = new AtomicReference<>();
    AtomicReference<Thread> readerB = new AtomicReference<>();
    ClientRequestPayload<Object> payloadA = mockProbingPayload(readerA);
    ClientRequestPayload<Object> payloadB = mockProbingPayload(readerB);
    Sinks.One<ClientResponsePayload<Object>> processorA = Sinks.one();
    Sinks.One<ClientResponsePayload<Object>> processorB = Sinks.one();
    AtomicReference<Throwable> errorA = new AtomicReference<>();
    CountDownLatch failedA = new CountDownLatch(1);
    processorA
        .asMono()
        .subscribe(
            v -> {},
            error -> {
              errorA.set(error);
              failedA.countDown();
            });
    ByteBuf wireA = transformCase.badWire(4260);
    ByteBuf witnessA = wireA.retain();
    ByteBuf wireB = compressedReplyWire(4261);
    ByteBuf witnessB = wireB.retain();

    sendRequest(channel, payloadA, processorA, 4260);
    sendRequest(channel, payloadB, processorB, 4261);

    channel.writeInbound(wireA);
    channel.writeInbound(wireB);

    assertThat(failedA.await(5, TimeUnit.SECONDS)).isTrue();

    // A decompression failure, not a struct failure: the reader never runs.
    assertThat(errorA.get()).isNotNull();
    assertThat(errorA.get()).isNotInstanceOf(TApplicationException.class);
    assertThat(readerA.get()).isNull();

    ClientResponsePayload<Object> resultB = processorB.asMono().block(Duration.ofSeconds(10));

    assertThat(resultB).isNotNull();
    assertThat(readerB.get().getName()).startsWith("test-offloop");
    assertThat(witnessA.refCnt()).isEqualTo(1);
    assertThat(witnessB.refCnt()).isEqualTo(1);

    witnessA.release();
    witnessB.release();

    channel.finishAndReleaseAll();
  }

  private enum TransformCase {
    MALFORMED_ZSTD,
    MALFORMED_ZLIB,
    UNKNOWN_TRANSFORM;

    /**
     * Corrupts the compressed tail of a valid wire frame (the message is last on the wire):
     * truncated zstd ends the body early, a flipped zlib byte breaks its checksum. Either way
     * decompression itself fails; compressing garbage would only fail the later struct read.
     */
    ByteBuf badWire(int sequenceId) throws Exception {
      if (this == UNKNOWN_TRANSFORM) {
        return unknownTransformWire(sequenceId);
      }

      ByteBuf wire =
          this == MALFORMED_ZSTD ? compressedReplyWire(sequenceId) : zlibReplyWire(sequenceId);

      if (this == MALFORMED_ZSTD) {
        wire.writerIndex(wire.writerIndex() - 5);
      } else {
        int at = wire.writerIndex() - 3;
        wire.setByte(at, wire.getByte(at) ^ 0xFF);
      }

      return wire;
    }
  }

  /**
   * Assembles a header wire frame by hand: the client codec refuses to encode an unknown transform,
   * but a hostile peer can still send one.
   */
  private static ByteBuf unknownTransformWire(int sequenceId) throws Exception {
    ByteBuf message = Unpooled.buffer();

    writeReply(message, sequenceId, "probe");

    ByteBuf info = Unpooled.buffer();
    info.writeByte(TProtocolType.TCompact.getProtocolId().getValue());
    writeVarInt32(info, 1);
    writeVarInt32(info, 999);

    int padding = 4 - (info.readableBytes() & 0b11);
    int paddedHeaderSize = info.readableBytes() + padding;

    ByteBuf frameHeader = Unpooled.buffer(10);
    frameHeader.writeShort(HEADER_MAGIC);
    frameHeader.writeShort(HEADER_FLAGS_NONE);
    frameHeader.writeInt(sequenceId);
    frameHeader.writeShort(paddedHeaderSize >> 2);

    ByteBuf headers = Unpooled.buffer(padding);
    headers.writeZero(padding);

    return Unpooled.wrappedBuffer(frameHeader, info, headers, message);
  }

  private static void writeVarInt32(ByteBuf out, int n) {
    while (true) {
      if ((n & ~0x7F) == 0) {
        out.writeByte(n);
        return;
      }

      out.writeByte(n | 0x80);
      n >>>= 7;
    }
  }

  @SuppressWarnings("unchecked")
  private static RequestContext<Object, Object> newRequestContext(
      ClientRequestPayload<?> payload, ByteBuf encodedRequest) {
    return new RequestContext<>(
        Sinks.one(), (ClientRequestPayload<Object>) payload, encodedRequest, null, false, 4243);
  }

  /** Runs a no-op through the scheduler so all previously scheduled decodes have finished. */
  private static void awaitIdle(Scheduler scheduler) {
    Mono.fromRunnable(() -> {}).subscribeOn(scheduler).block();
  }

  private static Scheduler newCapturingScheduler(AtomicReference<Runnable> task) {
    Scheduler scheduler = mock(Scheduler.class);
    when(scheduler.schedule(any(Runnable.class)))
        .thenAnswer(
            invocation -> {
              task.set(invocation.getArgument(0));
              return Disposables.disposed();
            });

    return scheduler;
  }

  private static void sendRequest(
      EmbeddedChannel channel,
      ClientRequestPayload<Object> payload,
      Sinks.One<ClientResponsePayload<Object>> processor,
      int sequenceId) {
    channel.writeAndFlush(
        new RequestContext<>(
            processor, payload, Unpooled.buffer().writeZero(8), null, false, sequenceId));

    Object sent = channel.readOutbound();
    ReferenceCountUtil.release(sent);
  }

  private static ByteBuf compressedReplyWire(int sequenceId) throws Exception {
    return encodedReplyWire(sequenceId, Collections.singletonList(TTransform.ZSTD.getValue()));
  }

  private static ByteBuf zlibReplyWire(int sequenceId) throws Exception {
    return encodedReplyWire(sequenceId, Collections.singletonList(TTransform.ZLIB.getValue()));
  }

  private static ByteBuf encodedReplyWire(int sequenceId, List<Integer> transforms)
      throws Exception {
    ByteBuf plain = Unpooled.buffer();

    writeReply(plain, sequenceId, "probe");

    ThriftFrame frame =
        new ThriftFrame(
            sequenceId,
            plain,
            Collections.emptyMap(),
            Collections.emptyMap(),
            LegacyTransportType.HEADER,
            TProtocolType.TCompact,
            false,
            transforms);

    EmbeddedChannel encoder = new EmbeddedChannel(new HeaderTransportCodec(false));
    encoder.writeOutbound(frame);

    ByteBuf wire = encoder.readOutbound();
    encoder.finishAndReleaseAll();

    return wire;
  }
}
