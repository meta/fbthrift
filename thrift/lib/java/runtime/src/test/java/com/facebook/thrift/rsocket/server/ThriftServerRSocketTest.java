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

import static com.facebook.thrift.rsocket.util.RocketErrorUtil.decodeRocketError;
import static com.facebook.thrift.rsocket.util.RocketErrorUtil.isRocketError;
import static com.facebook.thrift.rsocket.util.RocketErrorUtil.toApplicationException;
import static org.junit.jupiter.api.Assertions.*;

import com.facebook.nifty.core.NiftyConnectionContext;
import com.facebook.nifty.core.RequestContext;
import com.facebook.swift.service.ContextChain;
import com.facebook.swift.service.ThriftEventHandler;
import com.facebook.thrift.compression.CompressionManager;
import com.facebook.thrift.loadbalancing.LoadHeaderRpcServerHandler;
import com.facebook.thrift.loadbalancing.LoadHeaderSupplier;
import com.facebook.thrift.model.ResponseAndStream;
import com.facebook.thrift.payload.DefaultServerResponsePayload;
import com.facebook.thrift.payload.ServerRequestPayload;
import com.facebook.thrift.payload.ServerResponsePayload;
import com.facebook.thrift.payload.Writer;
import com.facebook.thrift.protocol.ProtocolUtil;
import com.facebook.thrift.protocol.TProtocolType;
import com.facebook.thrift.rsocket.util.PayloadUtil;
import com.facebook.thrift.server.CompositeRpcServerHandler;
import com.facebook.thrift.server.OverloadedException;
import com.facebook.thrift.server.RpcServerHandler;
import com.facebook.thrift.test.rocket.InitialTestResponse;
import com.facebook.thrift.test.rocket.TestAnnotatedMessageException;
import com.facebook.thrift.test.rocket.TestClientException;
import com.facebook.thrift.test.rocket.TestException;
import com.facebook.thrift.test.rocket.TestFunctionException;
import com.facebook.thrift.test.rocket.TestMessageException;
import com.facebook.thrift.test.rocket.TestRequest;
import com.facebook.thrift.test.rocket.TestRequest2;
import com.facebook.thrift.test.rocket.TestResponse;
import com.facebook.thrift.test.rocket.TestServerException;
import com.facebook.thrift.test.rocket.TestService;
import com.facebook.thrift.util.NettyNiftyRequestContext;
import com.facebook.thrift.util.RpcPayloadUtil;
import com.facebook.thrift.util.TestResponseWriters;
import com.facebook.thrift.util.TestResponseWriters.CountingAllocator;
import com.facebook.thrift.util.Writers;
import com.facebook.thrift.util.resources.RpcResources;
import io.airlift.units.Duration;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import io.rsocket.Payload;
import io.rsocket.RSocketErrorException;
import io.rsocket.exceptions.ApplicationErrorException;
import io.rsocket.frame.ErrorFrameCodec;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import org.apache.thrift.CodecConfig;
import org.apache.thrift.CompressionAlgorithm;
import org.apache.thrift.CompressionConfig;
import org.apache.thrift.ErrorBlame;
import org.apache.thrift.ErrorClassification;
import org.apache.thrift.ErrorKind;
import org.apache.thrift.ErrorSafety;
import org.apache.thrift.PayloadAppUnknownExceptionMetdata;
import org.apache.thrift.PayloadDeclaredExceptionMetadata;
import org.apache.thrift.PayloadExceptionMetadata;
import org.apache.thrift.PayloadExceptionMetadataBase;
import org.apache.thrift.PayloadMetadata;
import org.apache.thrift.PayloadResponseMetadata;
import org.apache.thrift.ProtocolId;
import org.apache.thrift.RequestRpcMetadata;
import org.apache.thrift.ResponseRpcError;
import org.apache.thrift.ResponseRpcErrorCategory;
import org.apache.thrift.ResponseRpcErrorCode;
import org.apache.thrift.ResponseRpcMetadata;
import org.apache.thrift.RpcKind;
import org.apache.thrift.StreamPayloadMetadata;
import org.apache.thrift.StreamRpcError;
import org.apache.thrift.StreamRpcErrorCode;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.ZstdCompressionCodecConfig;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

public class ThriftServerRSocketTest {

  static Stream<Arguments> data() {
    return Stream.of(Arguments.of(ProtocolId.COMPACT), Arguments.of(ProtocolId.BINARY));
  }

  private ProtocolId protocolId;

  ByteBufAllocator alloc = RpcResources.getByteBufAllocator();

  RpcServerHandler handler =
      TestService.Reactive.serverHandlerBuilder(new TestServiceHandler()).build();

  ThriftServerRSocket rocket = new ThriftServerRSocket(handler, alloc);

  private ByteBuf createMetadata(RequestRpcMetadata metadata) {
    ByteBuf buf = alloc.buffer();
    TProtocol protocol = TProtocolType.fromProtocolId(ProtocolId.COMPACT).apply(buf);
    metadata.write0(protocol);
    buf.retain();

    return buf;
  }

  private ByteBuf createData(RequestRpcMetadata metadata, TestRequest request) {
    ByteBuf buf = alloc.buffer();
    TProtocol protocol = TProtocolType.fromProtocolId(metadata.getProtocol()).apply(buf);
    protocol.writeStructBegin(new TStruct());
    protocol.writeFieldBegin(new TField("struct", (byte) TType.STRUCT, (short) 1));
    request.write0(protocol);
    protocol.writeFieldEnd();
    protocol.writeFieldStop();
    protocol.writeStructEnd();
    buf.retain();

    return buf;
  }

  private ByteBuf createData(
      RequestRpcMetadata metadata, TestRequest request, TestRequest2 request2) {
    ByteBuf buf = alloc.buffer();
    TProtocol protocol = TProtocolType.fromProtocolId(metadata.getProtocol()).apply(buf);
    protocol.writeStructBegin(new TStruct());

    protocol.writeFieldBegin(new TField("struct", (byte) TType.STRUCT, (short) 2));
    request.write0(protocol);
    protocol.writeFieldEnd();

    protocol.writeFieldBegin(new TField("struct", (byte) TType.STRUCT, (short) 5));
    request2.write0(protocol);
    protocol.writeFieldEnd();

    protocol.writeFieldStop();
    protocol.writeStructEnd();
    buf.retain();

    return buf;
  }

  private TestResponse getTestResponse(RequestRpcMetadata metadata, Payload response) {
    return getTestResponse(metadata.getProtocol(), response);
  }

  private TestResponse getTestResponse(ProtocolId protocolId, Payload response) {
    TProtocol protocol = TProtocolType.fromProtocolId(protocolId).apply(response.data());
    TField field = protocol.readFieldBegin();
    assertEquals(0, field.id);
    assertEquals(TType.STRUCT, field.type);
    protocol.readStructBegin();
    TestResponse testResponse = TestResponse.read0(protocol);
    protocol.readStructEnd();

    return testResponse;
  }

  private InitialTestResponse getInitialTestResponse(ProtocolId protocolId, Payload response) {
    TProtocol protocol = TProtocolType.fromProtocolId(protocolId).apply(response.data());
    TField field = protocol.readFieldBegin();
    assertEquals(0, field.id);
    assertEquals(TType.STRUCT, field.type);
    protocol.readStructBegin();
    InitialTestResponse initialResponse = InitialTestResponse.read0(protocol);
    protocol.readStructEnd();

    return initialResponse;
  }

  private TestRequest createTestRequest(int intField, String strField) {
    return new TestRequest.Builder().setIntField(intField).setStrField(strField).build();
  }

  private ResponseRpcMetadata getResponseMetadata(Payload response) {
    TProtocol protocol =
        TProtocolType.fromProtocolId(ProtocolId.COMPACT).apply(response.metadata());
    return ResponseRpcMetadata.read0(protocol);
  }

  private void assertDataIsEmpty(Payload response) {
    assertEquals(1, response.data().readableBytes());
    assertEquals(0, response.data().readByte());
  }

  private void assertEmptyResponse(Payload response) {
    assertDataIsEmpty(response);
  }

  private StreamPayloadMetadata getStreamMetadata(Payload response) {
    TProtocol protocol =
        TProtocolType.fromProtocolId(ProtocolId.COMPACT).apply(response.metadata());
    return StreamPayloadMetadata.read0(protocol);
  }

  private RequestRpcMetadata createRequestRpcMetadata(String name, RpcKind rpc) {
    return new RequestRpcMetadata.Builder()
        .setProtocol(protocolId)
        .setName(name)
        .setKind(rpc)
        .build();
  }

  private void assertDataIsTestException(ProtocolId protocolId, Payload payload, int id) {
    TProtocol protocol = TProtocolType.fromProtocolId(protocolId).apply(payload.data());
    TField field = protocol.readFieldBegin();
    assertEquals(id, field.id);
    assertEquals(TType.STRUCT, field.type);
    protocol.readStructBegin();
    TestException exc = TestException.read0(protocol);
    protocol.readStructEnd();
    assertEquals("exc", exc.getMsg());
    assertEquals(TestException.class.getName(), exc.getMessage());
  }

  private void assertDataIsFunctionException(ProtocolId protocolId, Payload payload, int id) {
    TProtocol protocol = TProtocolType.fromProtocolId(protocolId).apply(payload.data());
    TField field = protocol.readFieldBegin();
    assertEquals(id, field.id);
    assertEquals(TType.STRUCT, field.type);
    protocol.readStructBegin();
    TestFunctionException exc = TestFunctionException.read0(protocol);
    protocol.readStructEnd();
    assertEquals("exc", exc.getMsg());
    assertEquals(TestFunctionException.class.getName(), exc.getMessage());
  }

  private void assertDataIsMessageException(ProtocolId protocolId, Payload payload, int id) {
    TProtocol protocol = TProtocolType.fromProtocolId(protocolId).apply(payload.data());
    TField field = protocol.readFieldBegin();
    assertEquals(id, field.id);
    assertEquals(TType.STRUCT, field.type);
    protocol.readStructBegin();
    TestMessageException exc = TestMessageException.read0(protocol);
    protocol.readStructEnd();
    assertEquals("exc", exc.getMessage());
  }

  private void assertDataIsAnnotatedMessageException(
      ProtocolId protocolId, Payload payload, int id) {
    TProtocol protocol = TProtocolType.fromProtocolId(protocolId).apply(payload.data());
    TField field = protocol.readFieldBegin();
    assertEquals(id, field.id);
    assertEquals(TType.STRUCT, field.type);
    protocol.readStructBegin();
    TestAnnotatedMessageException exc = TestAnnotatedMessageException.read0(protocol);
    protocol.readStructEnd();
    assertEquals("exc", exc.getMsg());
    assertEquals("exc", exc.getMessage());
  }

  private Payload createPayload(RequestRpcMetadata requestMetadata, int intField, String strField) {
    TestRequest input = createTestRequest(intField, strField);
    return PayloadUtil.createPayload(
        createData(requestMetadata, input), createMetadata(requestMetadata));
  }

  private void assertRpcMetadataIsDefault(Payload response) {
    ResponseRpcMetadata responseMetadata = getResponseMetadata(response);
    assertEquals(
        PayloadResponseMetadata.defaultInstance(),
        responseMetadata.getPayloadMetadata().getResponseMetadata());
  }

  private void assertStreamMetadataIsDefault(Payload response) {
    StreamPayloadMetadata streamMetadata = getStreamMetadata(response);
    assertEquals(
        PayloadResponseMetadata.defaultInstance(),
        streamMetadata.getPayloadMetadata().getResponseMetadata());
  }

  @Test
  public void testRequestChannelTakesTheSetupWithoutSinkDemand() {
    this.protocolId = ProtocolId.COMPACT;
    RequestRpcMetadata requestMetadata = createRequestRpcMetadata("noSuchSink", RpcKind.SINK);

    List<Long> requests = new CopyOnWriteArrayList<>();
    Flux<Payload> payloads =
        Flux.just(createPayload(requestMetadata, 0, "setup")).doOnRequest(requests::add);

    StepVerifier.create(rocket.requestChannel(payloads))
        .thenConsumeWhile(ReferenceCountUtil::release)
        .verifyError();

    assertEquals(Collections.singletonList(1L), requests);
  }

  @Test
  public void testRequestChannelRejectsNonChannelKind() {
    this.protocolId = ProtocolId.COMPACT;
    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata("streamResponse", RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE);
    ByteBuf data = createData(requestMetadata, createTestRequest(0, "foo"));
    ByteBuf metadata = createMetadata(requestMetadata);

    StepVerifier.create(rocket.requestChannel(Flux.just(PayloadUtil.createPayload(data, metadata))))
        .verifyErrorSatisfies(e -> assertParsingFailure(e, "Invalid metadata object"));

    assertRequestBuffersFreed(data, metadata);
  }

  @Test
  public void testRequestChannelWithoutAMethodNameRejectsAsInvalid() {
    assertChannelSetupRejected(
        new RequestRpcMetadata.Builder().setProtocol(ProtocolId.COMPACT).setKind(RpcKind.SINK),
        "Invalid metadata object");
  }

  @Test
  public void testRequestChannelWithoutAProtocolRejectsAsInvalid() {
    assertChannelSetupRejected(
        new RequestRpcMetadata.Builder().setName("sink").setKind(RpcKind.SINK),
        "Invalid metadata object");
  }

  @Test
  public void testRequestChannelWithAnUnknownProtocolIdRejectsAsInvalid() {
    assertChannelSetupRejected(
        new RequestRpcMetadata.Builder()
            .setProtocol(ProtocolId.fromInteger(77))
            .setName("sink")
            .setKind(RpcKind.SINK),
        "Invalid metadata object");
  }

  @Test
  public void testRequestChannelDecompressionFailureRejectsAsInvalid() {
    assertChannelSetupRejected(
        new RequestRpcMetadata.Builder()
            .setProtocol(ProtocolId.COMPACT)
            .setName("sink")
            .setKind(RpcKind.SINK)
            .setCompression(CompressionAlgorithm.CUSTOM),
        "decompression failure: CUSTOM compression is not supported");
  }

  @Test
  public void testRequestChannelUnknownMethodSendsUnknownMethod() {
    this.protocolId = ProtocolId.COMPACT;
    RequestRpcMetadata requestMetadata = createRequestRpcMetadata("noSuchSink", RpcKind.SINK);

    StepVerifier.create(rocket.requestChannel(Flux.just(createPayload(requestMetadata, 0, "foo"))))
        .verifyErrorSatisfies(
            t ->
                assertServerError(
                    t,
                    ErrorFrameCodec.INVALID,
                    ResponseRpcErrorCode.UNKNOWN_METHOD,
                    ResponseRpcErrorCategory.INVALID_REQUEST));
  }

  @Test
  public void testRequestChannelUndeclaredErrorBeforeTheFirstResponseSendsUnknown() {
    RpcServerHandler failing = channelHandler((initial, sinkPayloads) -> Flux.error(boom()));

    StepVerifier.create(channel(failing, RpcKind.BIDIRECTIONAL_STREAM, Flux.never()))
        .verifyErrorSatisfies(
            t -> {
              assertEquals(ResponseRpcErrorCode.UNKNOWN, decodeRocketError(t).getCode());
              assertEquals("boom", decodeRocketError(t).getWhatUtf8());
            });
  }

  @Test
  public void testRequestChannelBadSetupReleasesABufferedSinkPayload() {
    this.protocolId = ProtocolId.COMPACT;
    RequestRpcMetadata requestMetadata = createRequestRpcMetadata("sink", RpcKind.SINK);
    ByteBuf data = createData(requestMetadata, createTestRequest(0, "setup"));
    ByteBuf metadata = alloc.buffer().writeByte(0xff).retain();
    ByteBuf sinkData = createData(requestMetadata, createTestRequest(1, "sink"));
    ByteBuf sinkMetadata = createMetadata(requestMetadata);

    Payload badSetup = PayloadUtil.createPayload(data, metadata);
    Payload sinkPayload = PayloadUtil.createPayload(sinkData, sinkMetadata);

    StepVerifier.create(rocket.requestChannel(withoutCredit(badSetup, sinkPayload)))
        .verifyErrorSatisfies(e -> assertParsingFailure(e, "Invalid metadata object"));

    assertRequestBuffersFreed(data, metadata);
    assertRequestBuffersFreed(sinkData, sinkMetadata);
  }

  @Test
  public void testRequestChannelReleasesTheSinkPayloadsABidiHandlerNeverReads() {
    this.protocolId = ProtocolId.COMPACT;
    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata("bidi", RpcKind.BIDIRECTIONAL_STREAM);
    ByteBuf sinkData = createData(requestMetadata, createTestRequest(1, "sink"));
    ByteBuf sinkMetadata = createMetadata(requestMetadata);

    RpcServerHandler neverReads =
        channelHandler((initial, sinkPayloads) -> Flux.just(firstResponse()));
    Payload sinkPayload = PayloadUtil.createPayload(sinkData, sinkMetadata);

    StepVerifier.create(
            new ThriftServerRSocket(neverReads, alloc)
                .requestChannel(
                    withoutCredit(createPayload(requestMetadata, 0, "setup"), sinkPayload)))
        .expectNextMatches(ReferenceCountUtil::release)
        .verifyComplete();

    assertRequestBuffersFreed(sinkData, sinkMetadata);
  }

  @Test
  public void testRequestChannelWithoutAResponseEndsWithAnError() {
    RpcServerHandler empty = channelHandler((initial, sinkPayloads) -> Flux.empty());

    for (RpcKind kind : Arrays.asList(RpcKind.SINK, RpcKind.BIDIRECTIONAL_STREAM)) {
      StepVerifier.create(channel(empty, kind, Flux.never()))
          .verifyErrorSatisfies(
              t ->
                  assertEquals(
                      "Call ended without a first response", decodeRocketError(t).getWhatUtf8()));
    }
  }

  @Test
  public void testRequestChannelSinkInitialErrorSendsThePayloadThenAnError() {
    RpcServerHandler firstOnly =
        channelHandler((initial, sinkPayloads) -> Flux.just(firstResponse()));

    StepVerifier.create(channel(firstOnly, RpcKind.SINK, Flux.never()))
        .expectNextMatches(ReferenceCountUtil::release)
        .verifyErrorSatisfies(t -> assertStreamError(t, "Sink ended without a final response"));
  }

  @Test
  public void testRequestChannelErrorAfterTheFirstResponseSendsAStreamError() {
    RpcServerHandler failsLate =
        channelHandler(
            (initial, sinkPayloads) -> Flux.concat(Flux.just(firstResponse()), Flux.error(boom())));

    StepVerifier.create(channel(failsLate, RpcKind.BIDIRECTIONAL_STREAM, Flux.never()))
        .expectNextMatches(ReferenceCountUtil::release)
        .verifyErrorSatisfies(t -> assertStreamError(t, "boom"));
  }

  @Test
  public void testRequestChannelTimeoutWithAnEarlyReaderSendsNoSinkCredit() {
    List<Long> requests = new CopyOnWriteArrayList<>();
    RpcServerHandler readsBeforeItAnswers =
        channelHandler((initial, sinkPayloads) -> Flux.from(sinkPayloads).thenMany(Flux.never()));

    StepVerifier.create(
            channel(
                readsBeforeItAnswers,
                RpcKind.SINK,
                Flux.never(),
                Duration.valueOf("100ms"),
                payloads -> payloads.doOnRequest(requests::add)))
        .verifyError(RSocketErrorException.class);

    assertEquals(Collections.singletonList(1L), requests);
  }

  @Test
  public void testRequestChannelBidiTimeoutWithAnEarlyReaderSendsTheErrorBeforeAnyCancel() {
    List<String> events = new CopyOnWriteArrayList<>();
    RpcServerHandler readsBeforeItAnswers =
        channelHandler(
            (initial, sinkPayloads) ->
                Flux.from(sinkPayloads)
                    .thenMany(Flux.<ServerResponsePayload>never())
                    .cancelOn(Schedulers.parallel()));

    StepVerifier.create(
            channel(
                readsBeforeItAnswers,
                RpcKind.BIDIRECTIONAL_STREAM,
                Flux.never(),
                Duration.valueOf("100ms"),
                payloads -> payloads.doOnCancel(() -> events.add("cancel"))))
        .expectErrorSatisfies(t -> events.add("error"))
        .verify();

    assertEquals("error", events.get(0));
  }

  @Test
  public void testRequestChannelEarlyReaderGetsItsCreditAfterTheFirstResponseIsQueued()
      throws Exception {
    List<String> events = new CopyOnWriteArrayList<>();
    RpcServerHandler readsEarly =
        channelHandler(
            (initial, sinkPayloads) -> {
              Flux.from(sinkPayloads).subscribe();

              return Mono.fromCallable(ThriftServerRSocketTest::firstResponse)
                  .subscribeOn(Schedulers.parallel())
                  .concatWith(Flux.never());
            });

    QueuingResponder responder = new QueuingResponder(events);
    channel(
            readsEarly,
            RpcKind.BIDIRECTIONAL_STREAM,
            Flux.never(),
            null,
            payloads -> payloads.doOnRequest(n -> events.add("request " + n)))
        .subscribe(responder);

    assertTrue(responder.firstQueued.await(5, TimeUnit.SECONDS));
    awaitEvent(events, "request 100");
    responder.dispose();

    assertEquals(
        Arrays.asList("request 1", "first queued", "request 100"),
        events.subList(0, 3),
        "events " + events);
  }

  @Test
  public void testRequestChannelBidiReaderCancelBeforeTheFirstResponseSendsNoCancel() {
    AtomicBoolean cancelled = new AtomicBoolean();
    RpcServerHandler cancelsEarly =
        channelHandler(
            (initial, sinkPayloads) -> {
              Flux.from(sinkPayloads).subscribe().dispose();

              return Flux.just(firstResponse()).concatWith(Flux.never());
            });

    Disposable call =
        channel(
                cancelsEarly,
                RpcKind.BIDIRECTIONAL_STREAM,
                Flux.never(),
                null,
                payloads -> payloads.doOnCancel(() -> cancelled.set(true)))
            .subscribe(ReferenceCountUtil::release);

    assertFalse(cancelled.get());
    call.dispose();
  }

  @Test
  public void testRequestChannelBidiReaderCancelAfterTheFirstResponseEndsOnlyTheSink() {
    AtomicBoolean cancelled = new AtomicBoolean();
    RpcServerHandler takesOne =
        channelHandler(
            (initial, sinkPayloads) ->
                Flux.concat(
                    Flux.just(firstResponse()),
                    Flux.from(sinkPayloads).take(1).map(element -> streamElement(initial)),
                    Flux.just(streamElement(initial))));

    StepVerifier.create(
            channel(
                takesOne,
                RpcKind.BIDIRECTIONAL_STREAM,
                Flux.just(sinkPayload(1)).concatWith(Flux.never()),
                null,
                payloads -> payloads.doOnCancel(() -> cancelled.set(true))))
        .expectNextMatches(ReferenceCountUtil::release)
        .expectNextMatches(ReferenceCountUtil::release)
        .expectNextMatches(ReferenceCountUtil::release)
        .verifyComplete();

    assertTrue(cancelled.get());
  }

  @Test
  public void testRequestChannelDeclaredSinkErrorReachesTheReaderAndUndeclaredFailsIt() {
    List<Object> read = new CopyOnWriteArrayList<>();
    RpcServerHandler reads =
        channelHandler(
            (initial, sinkPayloads) -> {
              Flux.from(sinkPayloads).subscribe(read::add, read::add, () -> read.add("complete"));

              return Flux.just(firstResponse()).concatWith(Flux.never());
            });

    Disposable call =
        channel(
                reads,
                RpcKind.BIDIRECTIONAL_STREAM,
                Flux.just(declaredSinkError(), undeclaredSinkError("peer sink failed"))
                    .concatWith(Flux.never()))
            .subscribe(ReferenceCountUtil::release);

    awaitSize(read, 2);
    call.dispose();

    assertTrue(read.get(0) instanceof ServerRequestPayload, "read " + read);
    assertTrue(read.get(1) instanceof TApplicationException, "read " + read);
    assertEquals("peer sink failed", ((TApplicationException) read.get(1)).getMessage());
  }

  @Test
  public void testRequestChannelAbortedCallFailsTheReaderSoItNeverCommits() {
    AtomicBoolean committed = new AtomicBoolean();
    List<Object> read = new CopyOnWriteArrayList<>();
    RpcServerHandler commitsAtTheEnd =
        channelHandler(
            (initial, sinkPayloads) -> {
              Flux.from(sinkPayloads)
                  .doOnNext(read::add)
                  .then(Mono.fromRunnable(() -> committed.set(true)))
                  .subscribe(null, read::add);

              return Flux.just(firstResponse()).concatWith(Flux.never());
            });

    Disposable call =
        channel(commitsAtTheEnd, RpcKind.SINK, Flux.just(sinkPayload(1)).concatWith(Flux.never()))
            .subscribe(ReferenceCountUtil::release);

    awaitSize(read, 1);
    call.dispose();
    awaitSize(read, 2);

    assertFalse(committed.get());
    assertTrue(read.get(1) instanceof CancellationException, "read " + read);
  }

  @Test
  public void testRequestChannelReaderThatWaitsForTheFirstResponseFailsWhenTheCallFails() {
    List<Throwable> failures = new CopyOnWriteArrayList<>();
    RpcServerHandler readsThenFails =
        channelHandler(
            (initial, sinkPayloads) -> {
              Flux.from(sinkPayloads).subscribe(null, failures::add);

              return Flux.error(boom());
            });

    StepVerifier.create(channel(readsThenFails, RpcKind.BIDIRECTIONAL_STREAM, Flux.never()))
        .verifyError();

    assertTrue(failures.get(0) instanceof CancellationException, "failures " + failures);
  }

  @Test
  public void testRequestChannelBidiSinkStaysOpenAfterTheStreamCompletes() {
    List<Object> read = new CopyOnWriteArrayList<>();
    Sinks.Many<Payload> peer = Sinks.many().unicast().onBackpressureBuffer();
    RpcServerHandler readsAfterItsStream =
        channelHandler(
            (initial, sinkPayloads) -> {
              Flux.from(sinkPayloads).subscribe(read::add, read::add, () -> read.add("complete"));

              return Flux.just(firstResponse());
            });

    StepVerifier.create(channel(readsAfterItsStream, RpcKind.BIDIRECTIONAL_STREAM, peer.asFlux()))
        .expectNextMatches(ReferenceCountUtil::release)
        .verifyComplete();

    peer.tryEmitNext(sinkPayload(1));
    peer.tryEmitComplete();
    awaitSize(read, 2);

    assertEquals(2, read.size(), "read " + read);
    assertTrue(read.get(0) instanceof ServerRequestPayload);
    assertEquals("complete", read.get(1));
  }

  @Test
  public void testRequestChannelLateReaderOfACompletedBidiCallFails() {
    AtomicReference<Publisher<ServerRequestPayload>> sink = new AtomicReference<>();
    RpcServerHandler answersWithoutReading =
        channelHandler(
            (initial, sinkPayloads) -> {
              sink.set(sinkPayloads);

              return Flux.just(firstResponse());
            });

    StepVerifier.create(channel(answersWithoutReading, RpcKind.BIDIRECTIONAL_STREAM, Flux.never()))
        .expectNextMatches(ReferenceCountUtil::release)
        .verifyComplete();

    StepVerifier.create(sink.get()).verifyError(CancellationException.class);
  }

  /** A peer that sends a sink payload right after the setup, without credit for it. */
  private static Flux<Payload> withoutCredit(Payload setup, Payload sinkPayload) {
    return Flux.create(
        peer -> {
          peer.next(setup);
          peer.next(sinkPayload);
        },
        FluxSink.OverflowStrategy.IGNORE);
  }

  private void assertChannelSetupRejected(RequestRpcMetadata.Builder setup, String expected) {
    this.protocolId = ProtocolId.COMPACT;
    RequestRpcMetadata requestMetadata = setup.build();
    ByteBuf data =
        createData(createRequestRpcMetadata("sink", RpcKind.SINK), createTestRequest(0, "foo"));
    ByteBuf metadata = createMetadata(requestMetadata);

    StepVerifier.create(rocket.requestChannel(Flux.just(PayloadUtil.createPayload(data, metadata))))
        .verifyErrorSatisfies(e -> assertParsingFailure(e, expected));

    assertRequestBuffersFreed(data, metadata);
  }

  /** After the first response, the peer reads a StreamRpcError in a CANCELED frame. */
  private static void assertStreamError(Throwable t, String expected) {
    assertEquals(ErrorFrameCodec.CANCELED, ((RSocketErrorException) t).errorCode());

    StreamRpcError error =
        ProtocolUtil.readCompact(
            StreamRpcError::read0,
            Unpooled.wrappedBuffer(t.getMessage().getBytes(StandardCharsets.UTF_8)));
    assertEquals(StreamRpcErrorCode.UNKNOWN, error.getCode());
    assertEquals(expected, error.getWhatUtf8());
  }

  private void assertParsingFailure(Throwable t, String expected) {
    assertServerError(
        t,
        ErrorFrameCodec.INVALID,
        ResponseRpcErrorCode.REQUEST_PARSING_FAILURE,
        ResponseRpcErrorCategory.INVALID_REQUEST);
    assertEquals(expected, decodeRocketError(t).getWhatUtf8());
  }

  /** Opens a channel with a COMPACT setup for the given kind, then the given sink payloads. */
  private Flux<Payload> channel(RpcServerHandler handler, RpcKind kind, Flux<Payload> sink) {
    return channel(handler, kind, sink, null, UnaryOperator.identity());
  }

  /** The peer function sees every payload the transport takes, the setup included. */
  private Flux<Payload> channel(
      RpcServerHandler handler,
      RpcKind kind,
      Flux<Payload> sink,
      Duration taskTimeout,
      UnaryOperator<Flux<Payload>> peer) {
    this.protocolId = ProtocolId.COMPACT;
    Payload setup = createPayload(createRequestRpcMetadata("channel", kind), 0, "setup");

    ThriftServerRSocket server =
        taskTimeout == null
            ? new ThriftServerRSocket(handler, alloc)
            : new ThriftServerRSocket(handler, alloc, null, taskTimeout);
    return server.requestChannel(peer.apply(Flux.concat(Flux.just(setup), sink)));
  }

  /** A hand-written channel handler, as an application can write one against RpcServerHandler. */
  private static RpcServerHandler channelHandler(
      BiFunction<ServerRequestPayload, Publisher<ServerRequestPayload>, Flux<ServerResponsePayload>>
          serve) {
    return new RpcServerHandler() {
      @Override
      public Flux<ServerResponsePayload> streamingRequestStreamingResponse(
          ServerRequestPayload initial, Publisher<ServerRequestPayload> sinkPayloads) {
        return serve.apply(initial, sinkPayloads);
      }
    };
  }

  private static ServerResponsePayload firstResponse() {
    return ServerResponsePayload.create(
        Writers.emptyStruct(), RpcPayloadUtil.streamFirstResponseMetadata(), null, true);
  }

  private static ServerResponsePayload streamElement(ServerRequestPayload request) {
    return ServerResponsePayload.create(
        Writers.emptyStruct(), null, RpcPayloadUtil.streamPayloadMetadata(request), true);
  }

  private Payload sinkPayload(int intField) {
    return sinkPayload(intField, new StreamPayloadMetadata.Builder().build());
  }

  private Payload sinkPayload(int intField, StreamPayloadMetadata metadata) {
    ByteBuf encodedMetadata = alloc.buffer();
    metadata.write0(TProtocolType.TCompact.apply(encodedMetadata));

    ByteBuf data =
        createData(
            new RequestRpcMetadata.Builder().setProtocol(ProtocolId.COMPACT).build(),
            createTestRequest(intField, "sink"));
    // The payload owns the buffer: drop the helper's extra reference.
    data.release();

    return PayloadUtil.createPayload(data, encodedMetadata);
  }

  /**
   * A declared sink error stays in the body for the generated reader, so the transport passes it.
   */
  private Payload declaredSinkError() {
    return sinkPayload(
        2,
        new StreamPayloadMetadata.Builder()
            .setPayloadMetadata(
                PayloadMetadata.fromExceptionMetadata(
                    new PayloadExceptionMetadataBase.Builder()
                        .setNameUtf8("TestException")
                        .setWhatUtf8("declared")
                        .setMetadata(
                            PayloadExceptionMetadata.fromDeclaredException(
                                new PayloadDeclaredExceptionMetadata.Builder().build()))
                        .build()))
            .build());
  }

  private Payload undeclaredSinkError(String what) {
    return sinkPayload(
        3,
        new StreamPayloadMetadata.Builder()
            .setPayloadMetadata(
                PayloadMetadata.fromExceptionMetadata(
                    new PayloadExceptionMetadataBase.Builder()
                        .setNameUtf8("TApplicationException")
                        .setWhatUtf8(what)
                        .setMetadata(
                            PayloadExceptionMetadata.fromAppUnknownException(
                                new PayloadAppUnknownExceptionMetdata.Builder().build()))
                        .build()))
            .build());
  }

  private static IllegalStateException boom() {
    return new IllegalStateException("boom");
  }

  private static void awaitSize(List<?> list, int size) {
    for (int i = 0; i < 500 && list.size() < size; i++) {
      LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
    }

    assertTrue(list.size() >= size, "list " + list);
  }

  private static void awaitEvent(List<String> events, String event) {
    for (int i = 0; i < 500 && !events.contains(event); i++) {
      LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
    }

    assertTrue(events.contains(event), "events " + events);
  }

  /**
   * Stands in for the responder: it queues each payload inside onNext and takes its time, so a
   * credit that does not wait for onNext to return would show up before "first queued".
   */
  private static final class QueuingResponder extends BaseSubscriber<Payload> {
    final CountDownLatch firstQueued = new CountDownLatch(1);
    private final List<String> events;

    QueuingResponder(List<String> events) {
      this.events = events;
    }

    @Override
    protected void hookOnNext(Payload payload) {
      LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50));
      payload.release();

      events.add("first queued");
      firstQueued.countDown();
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testRequestResponse(ProtocolId protocolId) {
    this.protocolId = protocolId;
    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata("requestResponse", RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE);
    Payload request = createPayload(requestMetadata, 5, "foo");

    StepVerifier.create(rocket.requestResponse(request))
        .assertNext(
            response -> {
              assertRpcMetadataIsDefault(response);
              TestResponse result = getTestResponse(requestMetadata, response);
              assertEquals(5, result.getIntField());
              assertEquals("foo", result.getStrField());
            })
        .verifyComplete();
  }

  @Test
  public void testResponseUsesTheCompressionConfigOfTheRequest() {
    this.protocolId = ProtocolId.COMPACT;
    RequestRpcMetadata requestMetadata =
        new RequestRpcMetadata.Builder(
                createRequestRpcMetadata("requestResponse", RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE))
            .setCompressionConfig(zstdConfig(0L))
            .build();
    Payload request = createPayload(requestMetadata, 5, "foo");

    StepVerifier.create(rocket.requestResponse(request))
        .assertNext(
            response -> {
              assertEquals(
                  CompressionAlgorithm.ZSTD, getResponseMetadata(response).getCompression());
              ByteBuf data =
                  CompressionManager.decompress(
                      CompressionAlgorithm.ZSTD, alloc, response.sliceData().retain());
              try {
                TProtocol protocol = TProtocolType.TCompact.apply(data);
                protocol.readFieldBegin();
                protocol.readStructBegin();
                assertEquals("foo", TestResponse.read0(protocol).getStrField());
              } finally {
                data.release();
                response.release();
              }
            })
        .verifyComplete();
  }

  @Test
  public void testResponseBelowTheSizeLimitIsNotCompressed() {
    this.protocolId = ProtocolId.COMPACT;
    RequestRpcMetadata requestMetadata =
        new RequestRpcMetadata.Builder(
                createRequestRpcMetadata("requestResponse", RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE))
            .setCompressionConfig(zstdConfig(1_000_000L))
            .build();
    Payload request = createPayload(requestMetadata, 5, "foo");

    StepVerifier.create(rocket.requestResponse(request))
        .assertNext(
            response -> {
              try {
                assertEquals(null, getResponseMetadata(response).getCompression());
                assertEquals("foo", getTestResponse(requestMetadata, response).getStrField());
              } finally {
                response.release();
              }
            })
        .verifyComplete();
  }

  private static CompressionConfig zstdConfig(long sizeLimit) {
    return new CompressionConfig.Builder()
        .setCodecConfig(
            CodecConfig.fromZstdConfig(new ZstdCompressionCodecConfig.Builder().build()))
        .setCompressionSizeLimit(sizeLimit)
        .build();
  }

  private void checkRequestResponseDeclaredException(
      String funcName,
      String expectedName,
      String expectedWhat,
      BiConsumer<ProtocolId, Payload> payloadAssertion) {
    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata(funcName, RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE);
    Payload request = createPayload(requestMetadata, 0, "foo");

    StepVerifier.create(rocket.requestResponse(request))
        .assertNext(
            response -> {
              assertRpcMedatadataHasDeclaredException(response, expectedName, expectedWhat);
              payloadAssertion.accept(requestMetadata.getProtocol(), response);
            })
        .verifyComplete();
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testRequestResponseDeclaredException(ProtocolId protocolId) {
    this.protocolId = protocolId;
    checkRequestResponseDeclaredException(
        "requestResponseDeclaredException",
        TestException.class.getName(),
        TestException.class.getName(),
        (protocol, payload) -> assertDataIsTestException(protocol, payload, 1));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testRequestResponseDeclaredMessageFieldException(ProtocolId protocolId) {
    this.protocolId = protocolId;
    checkRequestResponseDeclaredException(
        "requestResponseDeclaredMessageFieldException",
        TestMessageException.class.getName(),
        "exc",
        (protocol, payload) -> assertDataIsMessageException(protocol, payload, 1));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testRequestResponseDeclaredAnnotatedMessageException(ProtocolId protocolId) {
    this.protocolId = protocolId;
    checkRequestResponseDeclaredException(
        "requestResponseDeclaredAnnotatedMessageException",
        TestAnnotatedMessageException.class.getName(),
        "exc",
        (protocol, payload) -> assertDataIsAnnotatedMessageException(protocol, payload, 1));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testRequestResponseDeclaredClientException(ProtocolId protocolId) {
    this.protocolId = protocolId;
    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata(
            "requestResponseDeclaredClientException", RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE);
    Payload request = createPayload(requestMetadata, 0, "foo");

    StepVerifier.create(rocket.requestResponse(request))
        .assertNext(
            response -> {
              assertRpcMedatadataHasDeclaredException(
                  response,
                  TestClientException.class.getName(),
                  TestClientException.class.getName(),
                  ErrorBlame.CLIENT,
                  ErrorKind.PERMANENT,
                  ErrorSafety.SAFE);
            })
        .verifyComplete();
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testRequestResponseDeclaredServerException(ProtocolId protocolId) {
    this.protocolId = protocolId;
    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata(
            "requestResponseDeclaredServerException", RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE);
    Payload request = createPayload(requestMetadata, 0, "foo");

    StepVerifier.create(rocket.requestResponse(request))
        .assertNext(
            response -> {
              assertRpcMedatadataHasDeclaredException(
                  response,
                  TestServerException.class.getName(),
                  TestServerException.class.getName(),
                  ErrorBlame.SERVER,
                  ErrorKind.TRANSIENT,
                  ErrorSafety.UNSPECIFIED);
            })
        .verifyComplete();
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testRequestResponseVoid(ProtocolId protocolId) {
    this.protocolId = protocolId;
    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata("requestResponseVoid", RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE);
    Payload request = createPayload(requestMetadata, 5, "foo");

    StepVerifier.create(rocket.requestResponse(request))
        .assertNext(
            response -> {
              assertRpcMetadataIsDefault(response);
              assertEquals(5, TestServiceHandler.inputParameter.getIntField());
              assertEquals("foo", TestServiceHandler.inputParameter.getStrField());
              assertDataIsEmpty(response);
            })
        .verifyComplete();
  }

  @Test
  public void testStreamBasicCompact() {
    checkStreamBasic(ProtocolId.COMPACT);
  }

  @Test
  public void testStreamBasicBinary() {
    checkStreamBasic(ProtocolId.BINARY);
  }

  private void checkStreamBasic(ProtocolId protocolId) {
    this.protocolId = protocolId;
    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata("streamResponse", RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE);
    Payload request = createPayload(requestMetadata, 0, "foo");

    StepVerifier.Step<Payload> steps =
        StepVerifier.create(rocket.requestStream(request))
            .assertNext(
                response -> {
                  assertRpcMetadataIsDefault(response);
                  assertDataIsEmpty(response);
                });

    final AtomicInteger counter = new AtomicInteger();
    for (int i = 0; i < 10; i++) {
      steps.assertNext(
          response -> {
            assertStreamMetadataIsDefault(response);

            TestResponse result = getTestResponse(requestMetadata, response);
            assertEquals(counter.get(), result.getIntField());
            assertEquals("foo" + counter.getAndIncrement(), result.getStrField());
          });
    }
    steps.verifyComplete();
  }

  @Test
  public void testNullStreamCompact() {
    checkNullStream(ProtocolId.COMPACT);
  }

  @Test
  public void testNullStreamBinary() {
    checkNullStream(ProtocolId.BINARY);
  }

  private void checkNullStream(ProtocolId protocolId) {
    this.protocolId = protocolId;
    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata("streamResponseNull", RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE);
    Payload request = createPayload(requestMetadata, 0, "foo");

    StepVerifier.create(rocket.requestStream(request))
        .assertNext(
            response -> {
              ResponseRpcMetadata responseMetadata = getResponseMetadata(response);
              assertEquals(
                  ErrorBlame.SERVER,
                  responseMetadata
                      .getPayloadMetadata()
                      .getExceptionMetadata()
                      .getMetadata()
                      .getAppUnknownException()
                      .getErrorClassification()
                      .getBlame());
            })
        .verifyComplete();
  }

  @Test
  public void testEmptyStreamCompact() {
    checkEmptyStream(ProtocolId.COMPACT);
  }

  @Test
  public void testEmptyStreamBinary() {
    checkEmptyStream(ProtocolId.BINARY);
  }

  private void checkEmptyStream(ProtocolId protocolId) {
    this.protocolId = protocolId;
    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata("streamResponseEmpty", RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE);
    Payload request = createPayload(requestMetadata, 0, "foo");

    StepVerifier.create(rocket.requestStream(request))
        .assertNext(
            response -> {
              assertRpcMetadataIsDefault(response);
              assertDataIsEmpty(response);
            })
        .verifyComplete();
  }

  @Test
  public void testStreamInitialResponseCompact() {
    checkStreamInitialResponse(ProtocolId.COMPACT);
  }

  @Test
  public void testStreamInitialResponseBinary() {
    checkStreamInitialResponse(ProtocolId.BINARY);
  }

  private void checkStreamInitialResponse(ProtocolId protocolId) {
    this.protocolId = protocolId;
    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata(
            "streamInitialResponse", RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE);
    Payload request = createPayload(requestMetadata, 0, "foo");

    StepVerifier.Step<Payload> steps =
        StepVerifier.create(rocket.requestStream(request))
            .assertNext(
                response -> {
                  assertRpcMetadataIsDefault(response);
                  InitialTestResponse initialResponse =
                      getInitialTestResponse(requestMetadata.getProtocol(), response);
                  assertEquals(100, initialResponse.getIntField());
                });

    final AtomicInteger counter = new AtomicInteger();
    for (int i = 0; i < 10; i++) {
      steps.assertNext(
          response -> {
            assertStreamMetadataIsDefault(response);

            TestResponse result = getTestResponse(requestMetadata, response);
            assertEquals(counter.get(), result.getIntField());
            assertEquals("foo" + counter.getAndIncrement(), result.getStrField());
          });
    }
    steps.verifyComplete();
  }

  private void checkUndeclaredExceptionNoStream(String funcName, String excName, String msg) {
    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata(funcName, RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE);
    Payload request = createPayload(requestMetadata, 0, "foo");

    StepVerifier.create(rocket.requestStream(request))
        .assertNext(
            response -> {
              ResponseRpcMetadata responseMetadata = getResponseMetadata(response);
              PayloadExceptionMetadataBase expMetadata =
                  responseMetadata.getPayloadMetadata().getExceptionMetadata();
              assertEquals(
                  ErrorBlame.SERVER,
                  expMetadata
                      .getMetadata()
                      .getAppUnknownException()
                      .getErrorClassification()
                      .getBlame());
              assertEquals(excName, expMetadata.getNameUtf8());
              assertEquals(msg, expMetadata.getWhatUtf8());
            })
        .verifyComplete();
  }

  private void checkDeclaredException(
      String funcName,
      String expectedName,
      String expectedWhat,
      BiConsumer<ProtocolId, Payload> payloadAssertion) {
    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata(funcName, RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE);
    Payload request = createPayload(requestMetadata, 0, "foo");

    StepVerifier.create(rocket.requestStream(request))
        .assertNext(
            response -> {
              assertRpcMetadataIsDefault(response);
              assertEmptyResponse(response);
            })
        .assertNext(
            response -> {
              assertStreamMedatadataHasDeclaredException(response, expectedName, expectedWhat);
              payloadAssertion.accept(requestMetadata.getProtocol(), response);
            })
        .verifyComplete();
  }

  @Test
  public void testStreamDeclaredExceptionCompact() {
    checkStreamDeclaredException(ProtocolId.COMPACT);
  }

  @Test
  public void testStreamDeclaredExceptionBinary() {
    checkStreamDeclaredException(ProtocolId.BINARY);
  }

  private void checkStreamDeclaredException(ProtocolId protocolId) {
    this.protocolId = protocolId;
    checkDeclaredException(
        "streamDeclaredException",
        TestException.class.getName(),
        TestException.class.getName(),
        (protocol, payload) -> assertDataIsTestException(protocol, payload, 1));
  }

  @Test
  public void testStreamDeclaredMessageFieldExceptionCompact() {
    checkStreamDeclaredMessageFieldException(ProtocolId.COMPACT);
  }

  @Test
  public void testStreamDeclaredMessageFieldExceptionBinary() {
    checkStreamDeclaredMessageFieldException(ProtocolId.BINARY);
  }

  private void checkStreamDeclaredMessageFieldException(ProtocolId protocolId) {
    this.protocolId = protocolId;
    checkDeclaredException(
        "streamDeclaredMessageFieldException",
        TestMessageException.class.getName(),
        "exc",
        (protocol, payload) -> assertDataIsMessageException(protocol, payload, 18));
  }

  @Test
  public void testStreamDeclaredAnnotatedMessageExceptionCompact() {
    checkStreamDeclaredAnnotatedMessageException(ProtocolId.COMPACT);
  }

  @Test
  public void testStreamDeclaredAnnotatedMessageExceptionBinary() {
    checkStreamDeclaredAnnotatedMessageException(ProtocolId.BINARY);
  }

  private void checkStreamDeclaredAnnotatedMessageException(ProtocolId protocolId) {
    this.protocolId = protocolId;
    checkDeclaredException(
        "streamDeclaredAnnotatedMessageException",
        TestAnnotatedMessageException.class.getName(),
        "exc",
        (protocol, payload) -> assertDataIsAnnotatedMessageException(protocol, payload, 19));
  }

  @Test
  public void testStreamDeclaredException2SyncThrowIsUndeclaredCompact() {
    checkStreamDeclaredException2SyncThrowIsUndeclared(ProtocolId.COMPACT);
  }

  @Test
  public void testStreamDeclaredException2SyncThrowIsUndeclaredBinary() {
    checkStreamDeclaredException2SyncThrowIsUndeclared(ProtocolId.BINARY);
  }

  private void checkStreamDeclaredException2SyncThrowIsUndeclared(ProtocolId protocolId) {
    this.protocolId = protocolId;
    // TestException is only declared at stream level, not function level.
    // A sync throw is a response-phase failure, so it is checked against the
    // function exception table (empty here) and treated as undeclared.
    checkUndeclaredExceptionNoStream(
        "streamDeclaredException2", TestException.class.getName(), TestException.class.getName());
  }

  @Test
  public void testFunctionOnlyExceptionInStreamIsUndeclaredCompact() {
    checkFunctionOnlyExceptionInStreamIsUndeclared(ProtocolId.COMPACT);
  }

  @Test
  public void testFunctionOnlyExceptionInStreamIsUndeclaredBinary() {
    checkFunctionOnlyExceptionInStreamIsUndeclared(ProtocolId.BINARY);
  }

  private void checkFunctionOnlyExceptionInStreamIsUndeclared(ProtocolId protocolId) {
    this.protocolId = protocolId;
    // TestFunctionException is declared at function level only (throws clause),
    // not at stream level (stream<T throws>). Thrown during the stream phase, it is
    // checked against the stream exception table which does not contain
    // TestFunctionException, so it is treated as undeclared.
    checkUndeclaredException(
        "streamDeclaredAndFunctionException",
        TestFunctionException.class.getName(),
        TestFunctionException.class.getName());
  }

  private void checkUndeclaredException(String funcName, String excName, String msg) {
    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata(funcName, RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE);
    Payload request = createPayload(requestMetadata, 0, "foo");

    StepVerifier.create(rocket.requestStream(request))
        .assertNext(
            response -> {
              assertRpcMetadataIsDefault(response);
              assertEmptyResponse(response);
            })
        .assertNext(
            response -> {
              StreamPayloadMetadata streamMetadata = getStreamMetadata(response);
              PayloadExceptionMetadataBase expMetadata =
                  streamMetadata.getPayloadMetadata().getExceptionMetadata();
              assertEquals(
                  ErrorBlame.SERVER,
                  expMetadata
                      .getMetadata()
                      .getAppUnknownException()
                      .getErrorClassification()
                      .getBlame());
              assertEquals(excName, expMetadata.getNameUtf8());
              assertEquals(msg, expMetadata.getWhatUtf8());
            })
        .verifyComplete();
  }

  private void checkUndeclaredException(String funcName) {
    checkUndeclaredException(funcName, IllegalArgumentException.class.getName(), "exc");
  }

  @Test
  public void testStreamUndeclaredExceptionCompact() {
    checkStreamUndeclaredException(ProtocolId.COMPACT);
  }

  @Test
  public void testStreamUndeclaredExceptionBinary() {
    checkStreamUndeclaredException(ProtocolId.BINARY);
  }

  private void checkStreamUndeclaredException(ProtocolId protocolId) {
    this.protocolId = protocolId;
    checkUndeclaredException("streamUndeclaredException");
  }

  @Test
  public void testStreamUndeclaredException2Compact() {
    checkStreamUndeclaredException2(ProtocolId.COMPACT);
  }

  @Test
  public void testStreamUndeclaredException2Binary() {
    checkStreamUndeclaredException2(ProtocolId.BINARY);
  }

  private void checkStreamUndeclaredException2(ProtocolId protocolId) {
    this.protocolId = protocolId;
    checkUndeclaredExceptionNoStream(
        "streamUndeclaredException2", IllegalArgumentException.class.getName(), "exc");
  }

  @Test
  public void testUndeclaredFunctionExceptionCompact() {
    checkUndeclaredFunctionException(ProtocolId.COMPACT);
  }

  @Test
  public void testUndeclaredFunctionExceptionBinary() {
    checkUndeclaredFunctionException(ProtocolId.BINARY);
  }

  private void checkUndeclaredFunctionException(ProtocolId protocolId) {
    this.protocolId = protocolId;
    checkUndeclaredException("streamUndeclaredAndFunctionException");
  }

  @Test
  public void testInitialResponseStreamOnlyExceptionIsUndeclaredCompact() {
    checkInitialResponseStreamOnlyExceptionIsUndeclared(ProtocolId.COMPACT);
  }

  @Test
  public void testInitialResponseStreamOnlyExceptionIsUndeclaredBinary() {
    checkInitialResponseStreamOnlyExceptionIsUndeclared(ProtocolId.BINARY);
  }

  private void checkInitialResponseStreamOnlyExceptionIsUndeclared(ProtocolId protocolId) {
    this.protocolId = protocolId;
    // TestException is only declared at stream level (stream<T throws (11: TestException)>),
    // not at function level. Thrown in the response phase, it has no function-level
    // declaration, so it is correctly treated as undeclared, matching C++ behavior.
    checkUndeclaredExceptionNoStream(
        "streamInitialResponseDeclaredException",
        TestException.class.getName(),
        TestException.class.getName());
  }

  @Test
  public void testUnconsumedForwardedHolderIsDisposedCompact() {
    checkUnconsumedForwardedHolderIsDisposed(ProtocolId.COMPACT);
  }

  @Test
  public void testUnconsumedForwardedHolderIsDisposedBinary() {
    checkUnconsumedForwardedHolderIsDisposed(ProtocolId.BINARY);
  }

  private void checkUnconsumedForwardedHolderIsDisposed(ProtocolId protocolId) {
    this.protocolId = protocolId;
    // The downstream takes only the first payload, then cancels before requesting any stream
    // elements: concat's internal subscription is torn down, and the framework must dispose
    // the forwarded (handle-carrying) holder instead of leaving the backend stream pinned.
    TestServiceHandler.forwardedHolderDisposed.set(false);
    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata(
            "streamForwardedHolder", RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE);
    Payload request = createPayload(requestMetadata, 0, "foo");

    StepVerifier.create(rocket.requestStream(request), 1)
        .assertNext(
            response -> {
              assertRpcMetadataIsDefault(response);
              InitialTestResponse initialResponse =
                  getInitialTestResponse(requestMetadata.getProtocol(), response);
              assertEquals(100, initialResponse.getIntField());
            })
        .thenCancel()
        .verify();
    assertTrue(TestServiceHandler.forwardedHolderDisposed.get());
  }

  @Test
  public void testStreamFirstPayloadTakesOneCreditCompact() {
    checkStreamFirstPayloadTakesOneCredit(ProtocolId.COMPACT);
  }

  @Test
  public void testStreamFirstPayloadTakesOneCreditBinary() {
    checkStreamFirstPayloadTakesOneCredit(ProtocolId.BINARY);
  }

  private void checkStreamFirstPayloadTakesOneCredit(ProtocolId protocolId) {
    this.protocolId = protocolId;
    // The first payload consumes exactly one downstream credit; the rest forwards to the
    // user's Flux. With an initial request of 1 only the empty first response arrives.
    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata("streamResponse", RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE);
    Payload request = createPayload(requestMetadata, 0, "foo");

    StepVerifier.create(rocket.requestStream(request), 1)
        .assertNext(
            response -> {
              assertRpcMetadataIsDefault(response);
              assertDataIsEmpty(response);
            })
        .expectNoEvent(java.time.Duration.ofSeconds(2))
        .thenRequest(10)
        .expectNextCount(10)
        .verifyComplete();
  }

  @Test
  public void testStreamInitialResponseTakesOneCreditCompact() {
    checkStreamInitialResponseTakesOneCredit(ProtocolId.COMPACT);
  }

  @Test
  public void testStreamInitialResponseTakesOneCreditBinary() {
    checkStreamInitialResponseTakesOneCredit(ProtocolId.BINARY);
  }

  private void checkStreamInitialResponseTakesOneCredit(ProtocolId protocolId) {
    this.protocolId = protocolId;
    // Same credit rule for first-response streams: one credit delivers only the first
    // response; the 10 elements need forwarded demand.
    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata(
            "streamInitialResponse", RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE);
    Payload request = createPayload(requestMetadata, 0, "foo");

    StepVerifier.create(rocket.requestStream(request), 1)
        .assertNext(
            response -> {
              assertRpcMetadataIsDefault(response);
              InitialTestResponse initialResponse =
                  getInitialTestResponse(requestMetadata.getProtocol(), response);
              assertEquals(100, initialResponse.getIntField());
            })
        .expectNoEvent(java.time.Duration.ofSeconds(2))
        .thenRequest(10)
        .expectNextCount(10)
        .verifyComplete();
  }

  private void assertRpcMedatadataHasDeclaredException(
      Payload payload, String expectedName, String expectedWhat) {
    assertRpcMedatadataHasDeclaredException(
        payload,
        expectedName,
        expectedWhat,
        ErrorBlame.UNSPECIFIED,
        ErrorKind.UNSPECIFIED,
        ErrorSafety.UNSPECIFIED);
  }

  private void assertRpcMedatadataHasDeclaredException(
      Payload payload,
      String expectedName,
      String expectedWhat,
      ErrorBlame expectedBlame,
      ErrorKind expectedKind,
      ErrorSafety expectedSafety) {
    ResponseRpcMetadata responseMetadata = getResponseMetadata(payload);
    PayloadExceptionMetadataBase expMetadata =
        responseMetadata.getPayloadMetadata().getExceptionMetadata();
    ErrorClassification classification =
        expMetadata.getMetadata().getDeclaredException().getErrorClassification();
    assertEquals(expectedBlame, classification.getBlame());
    assertEquals(expectedKind, classification.getKind());
    assertEquals(expectedSafety, classification.getSafety());
    assertEquals(expectedName, expMetadata.getNameUtf8());
    assertEquals(expectedWhat, expMetadata.getWhatUtf8());
  }

  private void assertStreamMedatadataHasDeclaredException(
      Payload payload, String expectedName, String expectedWhat) {
    StreamPayloadMetadata responseMetadata = getStreamMetadata(payload);
    PayloadExceptionMetadataBase expMetadata =
        responseMetadata.getPayloadMetadata().getExceptionMetadata();
    ErrorClassification classification =
        expMetadata.getMetadata().getDeclaredException().getErrorClassification();
    assertEquals(ErrorBlame.UNSPECIFIED, classification.getBlame());
    assertEquals(ErrorKind.UNSPECIFIED, classification.getKind());
    assertEquals(ErrorSafety.UNSPECIFIED, classification.getSafety());
    assertEquals(expectedName, expMetadata.getNameUtf8());
    assertEquals(expectedWhat, expMetadata.getWhatUtf8());
  }

  @Test
  public void testStreamDeclaredExceptionFluxErrorCompact() {
    checkStreamDeclaredExceptionFluxError(ProtocolId.COMPACT);
  }

  @Test
  public void testStreamDeclaredExceptionFluxErrorBinary() {
    checkStreamDeclaredExceptionFluxError(ProtocolId.BINARY);
  }

  private void checkStreamDeclaredExceptionFluxError(ProtocolId protocolId) {
    this.protocolId = protocolId;
    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata(
            "streamDeclaredExceptionFluxError", RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE);
    Payload request = createPayload(requestMetadata, 0, "foo");

    StepVerifier.create(rocket.requestStream(request))
        .expectNextCount(5)
        .assertNext(
            response -> {
              assertStreamMedatadataHasDeclaredException(
                  response, TestException.class.getName(), TestException.class.getName());
              assertDataIsTestException(requestMetadata.getProtocol(), response, 7);
            })
        .verifyComplete();
  }

  @Test
  public void testStreamMultiInputCompact() {
    checkStreamMultiInput(ProtocolId.COMPACT);
  }

  @Test
  public void testStreamMultiInputBinary() {
    checkStreamMultiInput(ProtocolId.BINARY);
  }

  private void checkStreamMultiInput(ProtocolId protocolId) {
    this.protocolId = protocolId;
    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata(
            "streamDeclaredExceptionMultiInput", RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE);
    TestRequest input = createTestRequest(3, "foo");
    TestRequest2 input2 =
        new TestRequest2.Builder().setShortField((short) 7).setStrField("bar").build();
    Payload request =
        PayloadUtil.createPayload(
            createData(requestMetadata, input, input2), createMetadata(requestMetadata));

    StepVerifier.create(rocket.requestStream(request))
        .assertNext(
            response -> {
              // initial response
              assertRpcMetadataIsDefault(response);
              assertDataIsEmpty(response);
            })
        .assertNext(
            response -> {
              // stream responses
              assertStreamMetadataIsDefault(response);

              TestResponse result = getTestResponse(requestMetadata, response);
              assertEquals(10, result.getIntField());
              assertEquals("foobar7", result.getStrField());
            })
        .verifyComplete();
  }

  @Test
  public void testStreamDeclaredExceptionAfterFirstResponseCompact() {
    checkStreamDeclaredExceptionAfterFirstResponse(ProtocolId.COMPACT);
  }

  @Test
  public void testStreamDeclaredExceptionAfterFirstResponseBinary() {
    checkStreamDeclaredExceptionAfterFirstResponse(ProtocolId.BINARY);
  }

  private void checkStreamDeclaredExceptionAfterFirstResponse(ProtocolId protocolId) {
    this.protocolId = protocolId;
    // Stream-declared exception thrown AFTER the first response is sent.
    // Thrown in the stream phase, it is checked against the stream exception table,
    // which declares TestException.
    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata(
            "streamInitialResponseDeclaredExceptionAfterFirst",
            RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE);
    Payload request = createPayload(requestMetadata, 0, "foo");

    StepVerifier.create(rocket.requestStream(request))
        .assertNext(
            response -> {
              assertRpcMetadataIsDefault(response);
              InitialTestResponse initialResponse =
                  getInitialTestResponse(requestMetadata.getProtocol(), response);
              assertEquals(100, initialResponse.getIntField());
            })
        .assertNext(
            response -> {
              assertStreamMedatadataHasDeclaredException(
                  response, TestException.class.getName(), TestException.class.getName());
              assertDataIsTestException(requestMetadata.getProtocol(), response, 14);
            })
        .verifyComplete();
  }

  @Test
  public void testStreamEmptyFirstResponseCompact() {
    checkStreamEmptyFirstResponse(ProtocolId.COMPACT);
  }

  @Test
  public void testStreamEmptyFirstResponseBinary() {
    checkStreamEmptyFirstResponse(ProtocolId.BINARY);
  }

  // A first-response stream with no elements emits exactly the first payload, then completes:
  // no trailing empty payload follows onComplete.
  private void checkStreamEmptyFirstResponse(ProtocolId protocolId) {
    this.protocolId = protocolId;
    TestService.Reactive service =
        new TestServiceHandler() {
          @Override
          public Mono<ResponseAndStream<InitialTestResponse, TestResponse>> streamInitialResponse(
              TestRequest request) {
            return super.streamInitialResponse(request)
                .map(
                    ras ->
                        ResponseAndStream.of(
                            ras.getFirstResponse(), Flux.<TestResponse>empty(), ras::dispose));
          }
        };
    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata(
            "streamInitialResponse", RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE);
    Payload request = createPayload(requestMetadata, 0, "foo");

    StepVerifier.create(rocketFor(service).requestStream(request))
        .assertNext(
            response -> {
              assertRpcMetadataIsDefault(response);
              InitialTestResponse initialResponse =
                  getInitialTestResponse(requestMetadata.getProtocol(), response);
              assertEquals(100, initialResponse.getIntField());
            })
        .verifyComplete();
  }

  @Test
  public void testStreamErrorAfterElementsFirstResponseCompact() {
    checkStreamErrorAfterElementsFirstResponse(ProtocolId.COMPACT);
  }

  @Test
  public void testStreamErrorAfterElementsFirstResponseBinary() {
    checkStreamErrorAfterElementsFirstResponse(ProtocolId.BINARY);
  }

  // A stream-declared exception after elements on a first-response stream: the first payload
  // and the elements flow, then one declared error frame, then completion with nothing after.
  private void checkStreamErrorAfterElementsFirstResponse(ProtocolId protocolId) {
    this.protocolId = protocolId;
    TestService.Reactive service =
        new TestServiceHandler() {
          @Override
          public Mono<ResponseAndStream<InitialTestResponse, TestResponse>>
              streamInitialResponseDeclaredExceptionAfterFirst(TestRequest request) {
            return Mono.just(
                ResponseAndStream.of(
                    new InitialTestResponse.Builder().setIntField(100).build(),
                    Flux.range(0, 4)
                        .map(
                            i ->
                                new TestResponse.Builder()
                                    .setIntField(i)
                                    .setStrField("foo" + i)
                                    .build())
                        .concatWith(Flux.<TestResponse>error(new TestException("exc"))),
                    () -> {}));
          }
        };
    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata(
            "streamInitialResponseDeclaredExceptionAfterFirst",
            RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE);
    Payload request = createPayload(requestMetadata, 0, "foo");

    StepVerifier.Step<Payload> steps =
        StepVerifier.create(rocketFor(service).requestStream(request))
            .assertNext(
                response -> {
                  assertRpcMetadataIsDefault(response);
                  InitialTestResponse initialResponse =
                      getInitialTestResponse(requestMetadata.getProtocol(), response);
                  assertEquals(100, initialResponse.getIntField());
                });

    final AtomicInteger counter = new AtomicInteger();
    for (int i = 0; i < 4; i++) {
      steps.assertNext(
          response -> {
            assertStreamMetadataIsDefault(response);
            TestResponse result = getTestResponse(requestMetadata, response);
            assertEquals(counter.get(), result.getIntField());
            assertEquals("foo" + counter.getAndIncrement(), result.getStrField());
          });
    }
    steps
        .assertNext(
            response -> {
              assertStreamMedatadataHasDeclaredException(
                  response, TestException.class.getName(), TestException.class.getName());
              assertDataIsTestException(requestMetadata.getProtocol(), response, 14);
            })
        .verifyComplete();
  }

  @Test
  public void testStreamUndeclaredErrorAfterElementsCompact() {
    checkStreamUndeclaredErrorAfterElements(ProtocolId.COMPACT, false);
  }

  @Test
  public void testStreamUndeclaredErrorAfterElementsBinary() {
    checkStreamUndeclaredErrorAfterElements(ProtocolId.BINARY, false);
  }

  @Test
  public void testStreamUndeclaredErrorAfterElementsFirstResponseCompact() {
    checkStreamUndeclaredErrorAfterElements(ProtocolId.COMPACT, true);
  }

  @Test
  public void testStreamUndeclaredErrorAfterElementsFirstResponseBinary() {
    checkStreamUndeclaredErrorAfterElements(ProtocolId.BINARY, true);
  }

  // An undeclared failure after elements: the payloads flow, then one undeclared error frame
  // in stream metadata, then completion with nothing after. Plain and first-response alike.
  private void checkStreamUndeclaredErrorAfterElements(
      ProtocolId protocolId, boolean firstResponse) {
    this.protocolId = protocolId;
    Flux<TestResponse> elements =
        Flux.range(0, 4)
            .map(i -> new TestResponse.Builder().setIntField(i).setStrField("foo" + i).build())
            .concatWith(Flux.<TestResponse>error(new IllegalArgumentException("exc")));
    TestService.Reactive service =
        new TestServiceHandler() {
          @Override
          public Flux<TestResponse> streamResponse(TestRequest request) {
            return firstResponse ? super.streamResponse(request) : elements;
          }

          @Override
          public Mono<ResponseAndStream<InitialTestResponse, TestResponse>> streamInitialResponse(
              TestRequest request) {
            return firstResponse
                ? super.streamInitialResponse(request)
                    .map(
                        ras -> ResponseAndStream.of(ras.getFirstResponse(), elements, ras::dispose))
                : super.streamInitialResponse(request);
          }
        };
    String method = firstResponse ? "streamInitialResponse" : "streamResponse";
    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata(method, RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE);
    Payload request = createPayload(requestMetadata, 0, "foo");

    StepVerifier.Step<Payload> steps =
        StepVerifier.create(rocketFor(service).requestStream(request))
            .assertNext(
                response -> {
                  assertRpcMetadataIsDefault(response);
                  if (firstResponse) {
                    InitialTestResponse initialResponse =
                        getInitialTestResponse(requestMetadata.getProtocol(), response);
                    assertEquals(100, initialResponse.getIntField());
                  } else {
                    assertDataIsEmpty(response);
                  }
                });

    final AtomicInteger counter = new AtomicInteger();
    for (int i = 0; i < 4; i++) {
      steps.assertNext(
          response -> {
            assertStreamMetadataIsDefault(response);
            TestResponse result = getTestResponse(requestMetadata, response);
            assertEquals(counter.get(), result.getIntField());
            assertEquals("foo" + counter.getAndIncrement(), result.getStrField());
          });
    }
    steps
        .assertNext(
            response -> {
              StreamPayloadMetadata streamMetadata = getStreamMetadata(response);
              PayloadExceptionMetadataBase expMetadata =
                  streamMetadata.getPayloadMetadata().getExceptionMetadata();
              assertEquals(
                  ErrorBlame.SERVER,
                  expMetadata
                      .getMetadata()
                      .getAppUnknownException()
                      .getErrorClassification()
                      .getBlame());
              assertEquals(IllegalArgumentException.class.getName(), expMetadata.getNameUtf8());
              assertEquals("exc", expMetadata.getWhatUtf8());
            })
        .verifyComplete();
  }

  private ThriftServerRSocket rocketFor(TestService.Reactive service) {
    return new ThriftServerRSocket(
        TestService.Reactive.serverHandlerBuilder(service).build(), alloc);
  }

  @Test
  public void testFunctionDeclaredExceptionBeforeStreamCompact() {
    checkFunctionDeclaredExceptionBeforeStream(ProtocolId.COMPACT);
  }

  @Test
  public void testFunctionDeclaredExceptionBeforeStreamBinary() {
    checkFunctionDeclaredExceptionBeforeStream(ProtocolId.BINARY);
  }

  private void checkFunctionDeclaredExceptionBeforeStream(ProtocolId protocolId) {
    this.protocolId = protocolId;
    // TestFunctionException is declared at function level for this method.
    // An outer-Mono error is a response-phase failure, so it is checked against the
    // function exception table, found, and encoded as a declared initial error:
    // exactly one frame, no stream frames follow.
    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata(
            "streamInitialResponseDeclaredAndFunctionException",
            RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE);
    Payload request = createPayload(requestMetadata, 0, "foo");

    StepVerifier.create(rocket.requestStream(request))
        .assertNext(
            response -> {
              assertRpcMedatadataHasDeclaredException(
                  response,
                  TestFunctionException.class.getName(),
                  TestFunctionException.class.getName());
              assertDataIsFunctionException(requestMetadata.getProtocol(), response, 13);
            })
        .verifyComplete();
  }

  @Test
  public void testFunctionDeclaredExceptionSyncThrowCompact() {
    checkFunctionDeclaredExceptionSyncThrow(ProtocolId.COMPACT);
  }

  @Test
  public void testFunctionDeclaredExceptionSyncThrowBinary() {
    checkFunctionDeclaredExceptionSyncThrow(ProtocolId.BINARY);
  }

  private void checkFunctionDeclaredExceptionSyncThrow(ProtocolId protocolId) {
    this.protocolId = protocolId;
    // TestFunctionException is declared at function level. A synchronous throw is a
    // response-phase failure, so it is checked against the function exception table,
    // found, and encoded as a declared initial error.
    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata(
            "streamFunctionExceptionSyncThrow", RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE);
    Payload request = createPayload(requestMetadata, 0, "foo");

    StepVerifier.create(rocket.requestStream(request))
        .assertNext(
            response -> {
              assertRpcMedatadataHasDeclaredException(
                  response,
                  TestFunctionException.class.getName(),
                  TestFunctionException.class.getName());
              assertDataIsFunctionException(requestMetadata.getProtocol(), response, 17);
            })
        .verifyComplete();
  }

  @Test
  public void testInvalidFirstResponseDoesNotEstablishStreamCompact() {
    checkInvalidFirstResponseDoesNotEstablishStream(ProtocolId.COMPACT);
  }

  @Test
  public void testInvalidFirstResponseDoesNotEstablishStreamBinary() {
    checkInvalidFirstResponseDoesNotEstablishStream(ProtocolId.BINARY);
  }

  private void checkInvalidFirstResponseDoesNotEstablishStream(ProtocolId protocolId) {
    this.protocolId = protocolId;
    // A malformed first response must be treated as a pre-stream failure.
    // The server should send an undeclared exception in ResponseRpcMetadata
    // and must not establish the stream.
    checkUndeclaredExceptionNoStream(
        "streamInitialResponseInvalidFirstResponse",
        NullPointerException.class.getName(),
        "the first response must not be null");
  }

  private static final Duration SHORT_TASK_TIMEOUT = new Duration(50, TimeUnit.MILLISECONDS);
  private static final Duration LONG_TASK_TIMEOUT = new Duration(1, TimeUnit.DAYS);
  private static final String TIMEOUT_METHOD = "someMethod";

  /** A handler that never answers, recording whether the transport cancelled it. */
  private static RpcServerHandler stuckHandler(AtomicBoolean cancelled) {
    return new RpcServerHandler() {
      @Override
      public Mono<ServerResponsePayload> singleRequestSingleResponse(ServerRequestPayload payload) {
        return Mono.<ServerResponsePayload>never().doOnCancel(() -> cancelled.set(true));
      }

      @Override
      public Mono<Void> singleRequestNoResponse(ServerRequestPayload payload) {
        return Mono.<Void>never().doOnCancel(() -> cancelled.set(true));
      }
    };
  }

  private static RpcServerHandler immediateHandler() {
    return new RpcServerHandler() {
      @Override
      public Mono<ServerResponsePayload> singleRequestSingleResponse(ServerRequestPayload payload) {
        return Mono.just(RpcPayloadUtil.createServerResponsePayload(payload, protocol -> {}));
      }
    };
  }

  private ThriftServerRSocket rocketWithTaskTimeout(
      RpcServerHandler handler, Duration taskExpirationTimeout) {
    return new ThriftServerRSocket(handler, alloc, null, taskExpirationTimeout);
  }

  private Payload createTimeoutPayload(RpcKind kind, Integer clientTimeoutMs) {
    RequestRpcMetadata.Builder builder =
        new RequestRpcMetadata.Builder()
            .setProtocol(ProtocolId.COMPACT)
            .setName(TIMEOUT_METHOD)
            .setKind(kind);
    if (clientTimeoutMs != null) {
      builder.setClientTimeoutMs(clientTimeoutMs);
    }

    ByteBuf metadata = alloc.buffer();
    ProtocolUtil.writeCompact(builder.build()::write0, metadata);
    return PayloadUtil.createPayload(alloc.buffer(), metadata);
  }

  @Test
  public void testRequestResponseTimesOutWithTaskExpired() {
    AtomicBoolean cancelled = new AtomicBoolean();

    StepVerifier.create(
            rocketWithTaskTimeout(stuckHandler(cancelled), SHORT_TASK_TIMEOUT)
                .requestResponse(
                    createTimeoutPayload(RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE, null)))
        .expectErrorSatisfies(
            t -> {
              assertTrue(isRocketError(t), "expected an RSocket error frame, got " + t);
              assertEquals(ErrorFrameCodec.CANCELED, ((RSocketErrorException) t).errorCode());

              ResponseRpcError decoded = decodeRocketError(t);
              assertEquals(ResponseRpcErrorCode.TASK_EXPIRED, decoded.getCode());
              assertTrue(decoded.getWhatUtf8().contains(TIMEOUT_METHOD));
            })
        .verify();

    assertTrue(cancelled.get(), "the handler subscription should be cancelled on timeout");
  }

  @Test
  public void testClientDeadlineOverridesTheServerTaskTimeout() {
    AtomicBoolean cancelled = new AtomicBoolean();

    // The server would wait a day; the caller's 50ms deadline plus ten percent is what fires.
    StepVerifier.create(
            rocketWithTaskTimeout(stuckHandler(cancelled), LONG_TASK_TIMEOUT)
                .requestResponse(createTimeoutPayload(RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE, 50)))
        .expectError(RSocketErrorException.class)
        .verify();

    assertTrue(cancelled.get(), "the handler subscription should be cancelled on timeout");
  }

  @Test
  public void testTaskTimeoutOfZeroLeavesTheRequestRunning() {
    AtomicBoolean cancelled = new AtomicBoolean();

    StepVerifier.create(
            rocketWithTaskTimeout(stuckHandler(cancelled), new Duration(0, TimeUnit.MILLISECONDS))
                .requestResponse(
                    createTimeoutPayload(RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE, null)))
        .expectTimeout(java.time.Duration.ofMillis(500))
        .verify();
  }

  @Test
  public void testFastRequestIsNotAffectedByTaskTimeout() {
    StepVerifier.create(
            rocketWithTaskTimeout(immediateHandler(), SHORT_TASK_TIMEOUT)
                .requestResponse(
                    createTimeoutPayload(RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE, null)))
        .expectNextCount(1)
        .verifyComplete();
  }

  @Test
  public void testOnewayTaskTimeoutCompletesWithoutAResponse() {
    AtomicBoolean cancelled = new AtomicBoolean();

    StepVerifier.create(
            rocketWithTaskTimeout(stuckHandler(cancelled), SHORT_TASK_TIMEOUT)
                .fireAndForget(createTimeoutPayload(RpcKind.SINGLE_REQUEST_NO_RESPONSE, null)))
        .verifyComplete();

    assertTrue(cancelled.get(), "the handler subscription should be cancelled on timeout");
  }

  /**
   * A preprocess LOADSHEDDING throw rejects with a REJECTED ERROR frame whose code follows the
   * reason, like the C++ server sends, instead of a response body whose type decodes as UNKNOWN. A
   * bare LOADSHEDDING maps to APP_OVERLOAD.
   */
  @Test
  public void testPreprocessShedRequestResponseCompact() {
    runPreprocessShedRequestResponse(ProtocolId.COMPACT);
  }

  @Test
  public void testPreprocessShedRequestResponseBinary() {
    runPreprocessShedRequestResponse(ProtocolId.BINARY);
  }

  private void runPreprocessShedRequestResponse(ProtocolId protocolId) {
    this.protocolId = protocolId;
    ThriftServerRSocket sheddingRocket =
        sheddingRocket(new TApplicationException(TApplicationException.LOADSHEDDING, "shed"));

    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata("requestResponse", RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE);
    Payload request = createPayload(requestMetadata, 0, "foo");

    StepVerifier.create(sheddingRocket.requestResponse(request))
        .expectErrorSatisfies(t -> assertShedError(t, ResponseRpcErrorCode.APP_OVERLOAD))
        .verify();
  }

  @Test
  public void testPreprocessShedStreamCompact() {
    runPreprocessShedStream(ProtocolId.COMPACT);
  }

  @Test
  public void testPreprocessShedStreamBinary() {
    runPreprocessShedStream(ProtocolId.BINARY);
  }

  private void runPreprocessShedStream(ProtocolId protocolId) {
    this.protocolId = protocolId;
    ThriftServerRSocket sheddingRocket =
        sheddingRocket(new TApplicationException(TApplicationException.LOADSHEDDING, "shed"));

    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata("streamResponse", RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE);
    Payload request = createPayload(requestMetadata, 0, "foo");

    StepVerifier.create(sheddingRocket.requestStream(request))
        .expectErrorSatisfies(t -> assertShedError(t, ResponseRpcErrorCode.APP_OVERLOAD))
        .verify();
  }

  /**
   * Only the first stream response may become a rejection: a LOADSHEDDING error after the first
   * response stays in band with stream metadata, and both responses are freed.
   */
  @Test
  public void testLoadsheddingAfterFirstStreamResponseStaysInBand() {
    this.protocolId = ProtocolId.COMPACT;
    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata("streamResponse", RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE);
    TApplicationException lateShed =
        new TApplicationException(TApplicationException.LOADSHEDDING, "late shed");
    RpcServerHandler stub =
        new RpcServerHandler() {
          @Override
          public Flux<ServerResponsePayload> singleRequestStreamingResponse(
              ServerRequestPayload payload) {
            return Flux.just(
                RpcPayloadUtil.fromTApplicationException(
                    new TApplicationException(TApplicationException.INTERNAL_ERROR, "first"),
                    requestMetadata,
                    null),
                new DefaultServerResponsePayload(
                    lateShed::write,
                    null,
                    RpcPayloadUtil.undeclaredExceptionStreamMetadata(
                        payload, lateShed.getClass().getName(), lateShed.getMessage()),
                    true,
                    lateShed));
          }
        };
    Payload request = createPayload(requestMetadata, 0, "foo");

    StepVerifier.create(new ThriftServerRSocket(stub, alloc).requestStream(request))
        .assertNext(
            first -> {
              ByteBuf data = first.data();
              ByteBuf metadata = first.metadata();
              first.release();
              assertEquals(0, data.refCnt(), "first response data must be freed");
              assertEquals(0, metadata.refCnt(), "first response metadata must be freed");
            })
        .assertNext(
            second -> {
              ByteBuf data = second.data();
              ByteBuf metadata = second.metadata();
              StreamPayloadMetadata streamMetadata = getStreamMetadata(second);
              PayloadExceptionMetadataBase exceptionMetadata =
                  streamMetadata.getPayloadMetadata().getExceptionMetadata();
              assertEquals(lateShed.getClass().getName(), exceptionMetadata.getNameUtf8());
              assertEquals(lateShed.getMessage(), exceptionMetadata.getWhatUtf8());
              second.release();
              assertEquals(0, data.refCnt(), "late error data must be freed");
              assertEquals(0, metadata.refCnt(), "late error metadata must be freed");
            })
        .verifyComplete();
  }

  /**
   * The rejection carries no response load, even when the response metadata has one: rsocket-java
   * writes ERROR frame data from the exception message, so only the code and the ASCII-safe message
   * survive.
   */
  @Test
  public void testRejectionOmitsResponseLoad() {
    this.protocolId = ProtocolId.COMPACT;
    LoadHeaderSupplier fixedLoad =
        new LoadHeaderSupplier() {
          @Override
          public void onRequest() {}

          @Override
          public void doFinally() {}

          @Override
          public long getLoad() {
            return 42;
          }
        };
    RpcServerHandler sheddingHandler =
        TestService.Reactive.serverHandlerBuilder(new TestServiceHandler())
            .setEventHandlers(
                Collections.singletonList(
                    new ThriftEventHandler() {
                      @Override
                      public void preprocess(Object context, String methodName) throws TException {
                        throw OverloadedException.host("busy");
                      }
                    }))
            .build();
    ThriftServerRSocket sheddingRocket =
        new ThriftServerRSocket(
            new LoadHeaderRpcServerHandler(sheddingHandler, key -> 0L, fixedLoad), alloc);

    RequestRpcMetadata requestMetadata =
        new RequestRpcMetadata.Builder()
            .setProtocol(protocolId)
            .setName("requestResponse")
            .setKind(RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE)
            .setOtherMetadata(Collections.singletonMap("load", "default"))
            .build();
    Payload request = createPayload(requestMetadata, 0, "foo");

    StepVerifier.create(sheddingRocket.requestResponse(request))
        .expectErrorSatisfies(
            t -> {
              assertShedError(t, ResponseRpcErrorCode.OVERLOAD);
              assertNull(decodeRocketError(t).getLoad(), "rejection must not carry load");
            })
        .verify();
  }

  /**
   * A saturated off-loop scheduler rejects with RejectedExecutionException instead of running the
   * handler; the transport reports that as a REJECTED OVERLOAD like the Header transports do, and
   * the request buffers are freed on the existing error path.
   */
  @Test
  public void testSchedulerSaturationRejectsRequestResponse() {
    this.protocolId = ProtocolId.COMPACT;
    RpcServerHandler stuckHandler =
        new RpcServerHandler() {
          @Override
          public Mono<ServerResponsePayload> singleRequestSingleResponse(
              ServerRequestPayload payload) {
            return Mono.<ServerResponsePayload>empty()
                .subscribeOn(
                    Schedulers.fromExecutor(
                        task -> {
                          throw new RejectedExecutionException("thrift offloop scheduler full");
                        }));
          }
        };
    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata("requestResponse", RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE);
    ByteBuf data = createData(requestMetadata, createTestRequest(0, "foo"));
    ByteBuf metadata = createMetadata(requestMetadata);
    Payload request = PayloadUtil.createPayload(data, metadata);

    StepVerifier.create(new ThriftServerRSocket(stuckHandler, alloc).requestResponse(request))
        .expectErrorSatisfies(t -> assertShedError(t, ResponseRpcErrorCode.OVERLOAD))
        .verify();

    assertEquals(1, data.refCnt(), "transport released its reference to request data");
    assertEquals(1, metadata.refCnt(), "transport released its reference to request metadata");
    data.release();
    metadata.release();
  }

  @Test
  public void testSchedulerSaturationRejectsStream() {
    this.protocolId = ProtocolId.COMPACT;
    RpcServerHandler stuckHandler =
        new RpcServerHandler() {
          @Override
          public Flux<ServerResponsePayload> singleRequestStreamingResponse(
              ServerRequestPayload payload) {
            return Flux.<ServerResponsePayload>empty()
                .subscribeOn(
                    Schedulers.fromExecutor(
                        task -> {
                          throw new RejectedExecutionException("thrift offloop scheduler full");
                        }));
          }
        };
    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata("streamResponse", RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE);
    ByteBuf data = createData(requestMetadata, createTestRequest(0, "foo"));
    ByteBuf metadata = createMetadata(requestMetadata);
    Payload request = PayloadUtil.createPayload(data, metadata);

    StepVerifier.create(new ThriftServerRSocket(stuckHandler, alloc).requestStream(request))
        .expectErrorSatisfies(t -> assertShedError(t, ResponseRpcErrorCode.OVERLOAD))
        .verify();

    assertEquals(1, data.refCnt(), "transport released its reference to request data");
    assertEquals(1, metadata.refCnt(), "transport released its reference to request metadata");
    data.release();
    metadata.release();
  }

  /**
   * The saturation mapping is gated on the first-response flag like the overload conversion: a
   * rejection after the first response stays as is.
   */
  @Test
  public void testSchedulerSaturationAfterFirstStreamResponseStaysAsIs() {
    this.protocolId = ProtocolId.COMPACT;
    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata("streamResponse", RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE);
    RpcServerHandler stub =
        new RpcServerHandler() {
          @Override
          public Flux<ServerResponsePayload> singleRequestStreamingResponse(
              ServerRequestPayload payload) {
            return Flux.concat(
                Flux.just(
                    RpcPayloadUtil.fromTApplicationException(
                        new TApplicationException(TApplicationException.INTERNAL_ERROR, "first"),
                        requestMetadata,
                        null)),
                Flux.error(new RejectedExecutionException("thrift offloop scheduler full")));
          }
        };
    Payload request = createPayload(requestMetadata, 0, "foo");

    StepVerifier.create(new ThriftServerRSocket(stub, alloc).requestStream(request))
        .assertNext(first -> first.release())
        .expectErrorSatisfies(t -> assertTrue(t instanceof RejectedExecutionException, "got " + t))
        .verify();
  }

  private ThriftServerRSocket sheddingRocket(TApplicationException shed) {
    ThriftEventHandler shedder =
        new ThriftEventHandler() {
          @Override
          public void preprocess(Object context, String methodName) throws TException {
            throw shed;
          }
        };
    RpcServerHandler sheddingHandler =
        TestService.Reactive.serverHandlerBuilder(new TestServiceHandler())
            .setEventHandlers(Collections.singletonList(shedder))
            .build();
    return new ThriftServerRSocket(sheddingHandler, alloc);
  }

  private void assertShedError(Throwable t, ResponseRpcErrorCode expectedCode) {
    assertTrue(isRocketError(t), "got " + t);
    assertEquals(ErrorFrameCodec.REJECTED, ((RSocketErrorException) t).errorCode());
    ResponseRpcError error = decodeRocketError(t);
    assertNotNull(error);
    assertEquals(expectedCode, error.getCode());
    assertEquals(ResponseRpcErrorCategory.LOADSHEDDING, error.getCategory());
    assertEquals(TApplicationException.LOADSHEDDING, toApplicationException(error).getType());
  }

  private void assertServerError(
      Throwable t,
      int expectedFrame,
      ResponseRpcErrorCode expectedCode,
      ResponseRpcErrorCategory expectedCategory) {
    assertTrue(isRocketError(t), "got " + t);
    assertEquals(expectedFrame, ((RSocketErrorException) t).errorCode());

    ResponseRpcError error = decodeRocketError(t);
    assertNotNull(error);
    assertEquals(expectedCode, error.getCode());
    assertEquals(expectedCategory, error.getCategory());
  }

  private void assertRequestBuffersFreed(ByteBuf data, ByteBuf metadata) {
    assertEquals(1, data.refCnt(), "transport released its reference to request data");
    assertEquals(1, metadata.refCnt(), "transport released its reference to request metadata");
    data.release();
    metadata.release();
  }

  @Test
  public void testBadMetadataRejectsAsInvalid() {
    this.protocolId = ProtocolId.COMPACT;
    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata("requestResponse", RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE);
    ByteBuf data = createData(requestMetadata, createTestRequest(0, "foo"));

    ByteBuf metadata = alloc.buffer();
    metadata.writeByte(0xff);
    metadata.retain();
    Payload request = PayloadUtil.createPayload(data, metadata);

    StepVerifier.create(rocket.requestResponse(request))
        .expectErrorSatisfies(
            t -> {
              assertServerError(
                  t,
                  ErrorFrameCodec.INVALID,
                  ResponseRpcErrorCode.REQUEST_PARSING_FAILURE,
                  ResponseRpcErrorCategory.INVALID_REQUEST);
              assertEquals("Invalid metadata object", decodeRocketError(t).getWhatUtf8());
            })
        .verify();

    assertRequestBuffersFreed(data, metadata);
  }

  @Test
  public void testKindMismatchRejectsAsInvalid() {
    this.protocolId = ProtocolId.COMPACT;
    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata("requestResponse", RpcKind.SINGLE_REQUEST_NO_RESPONSE);
    ByteBuf data = createData(requestMetadata, createTestRequest(0, "foo"));
    ByteBuf metadata = createMetadata(requestMetadata);
    Payload request = PayloadUtil.createPayload(data, metadata);

    StepVerifier.create(rocket.requestResponse(request))
        .expectErrorSatisfies(
            t -> {
              assertServerError(
                  t,
                  ErrorFrameCodec.INVALID,
                  ResponseRpcErrorCode.REQUEST_PARSING_FAILURE,
                  ResponseRpcErrorCategory.INVALID_REQUEST);
              assertEquals("Invalid metadata object", decodeRocketError(t).getWhatUtf8());
            })
        .verify();

    assertRequestBuffersFreed(data, metadata);
  }

  @Test
  public void testUnknownCompressionRejectsAsInvalid() {
    this.protocolId = ProtocolId.COMPACT;
    RequestRpcMetadata requestMetadata =
        new RequestRpcMetadata.Builder()
            .setProtocol(protocolId)
            .setName("requestResponse")
            .setKind(RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE)
            .setCompression(CompressionAlgorithm.CUSTOM)
            .build();

    ByteBuf data = createData(requestMetadata, createTestRequest(0, "foo"));
    ByteBuf metadata = createMetadata(requestMetadata);
    Payload request = PayloadUtil.createPayload(data, metadata);

    StepVerifier.create(rocket.requestResponse(request))
        .expectErrorSatisfies(
            t -> {
              assertServerError(
                  t,
                  ErrorFrameCodec.INVALID,
                  ResponseRpcErrorCode.REQUEST_PARSING_FAILURE,
                  ResponseRpcErrorCategory.INVALID_REQUEST);
              assertEquals(
                  "decompression failure: CUSTOM compression is not supported",
                  decodeRocketError(t).getWhatUtf8());
            })
        .verify();

    assertRequestBuffersFreed(data, metadata);
  }

  @Test
  public void testUnknownProtocolRejectsAsInvalid() {
    this.protocolId = ProtocolId.COMPACT;
    RequestRpcMetadata requestMetadata =
        new RequestRpcMetadata.Builder()
            .setName("requestResponse")
            .setKind(RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE)
            .build();

    ByteBuf data =
        createData(
            createRequestRpcMetadata("requestResponse", RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE),
            createTestRequest(0, "foo"));
    ByteBuf metadata = createMetadata(requestMetadata);
    Payload request = PayloadUtil.createPayload(data, metadata);

    StepVerifier.create(rocket.requestResponse(request))
        .expectErrorSatisfies(
            t -> {
              assertServerError(
                  t,
                  ErrorFrameCodec.INVALID,
                  ResponseRpcErrorCode.REQUEST_PARSING_FAILURE,
                  ResponseRpcErrorCategory.INVALID_REQUEST);
              assertEquals("Invalid metadata object", decodeRocketError(t).getWhatUtf8());
            })
        .verify();

    assertRequestBuffersFreed(data, metadata);
  }

  @Test
  public void testUnknownServiceRejectsAsInvalid() {
    this.protocolId = ProtocolId.COMPACT;
    ThriftServerRSocket compositeRocket =
        new ThriftServerRSocket(
            new CompositeRpcServerHandler(Collections.singletonList(handler)), alloc);

    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata(
            "NoSuchService.requestResponse", RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE);
    ByteBuf data = createData(requestMetadata, createTestRequest(0, "foo"));
    ByteBuf metadata = createMetadata(requestMetadata);
    Payload request = PayloadUtil.createPayload(data, metadata);

    StepVerifier.create(compositeRocket.requestResponse(request))
        .expectErrorSatisfies(
            t -> {
              assertServerError(
                  t,
                  ErrorFrameCodec.INVALID,
                  ResponseRpcErrorCode.UNKNOWN_METHOD,
                  ResponseRpcErrorCategory.INVALID_REQUEST);
              assertEquals(
                  "Method name NoSuchService.requestResponse not found",
                  decodeRocketError(t).getWhatUtf8());
            })
        .verify();

    assertRequestBuffersFreed(data, metadata);
  }

  @Test
  public void testUnknownServiceRejectsStreamAsInvalid() {
    this.protocolId = ProtocolId.COMPACT;
    ThriftServerRSocket compositeRocket =
        new ThriftServerRSocket(
            new CompositeRpcServerHandler(Collections.singletonList(handler)), alloc);

    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata(
            "NoSuchService.streamResponse", RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE);
    ByteBuf data = createData(requestMetadata, createTestRequest(0, "foo"));
    ByteBuf metadata = createMetadata(requestMetadata);
    Payload request = PayloadUtil.createPayload(data, metadata);

    StepVerifier.create(compositeRocket.requestStream(request))
        .expectErrorSatisfies(
            t ->
                assertServerError(
                    t,
                    ErrorFrameCodec.INVALID,
                    ResponseRpcErrorCode.UNKNOWN_METHOD,
                    ResponseRpcErrorCategory.INVALID_REQUEST))
        .verify();

    assertRequestBuffersFreed(data, metadata);
  }

  @Test
  public void testResponseSerializationFailureCancelsRequest() {
    this.protocolId = ProtocolId.COMPACT;
    RpcServerHandler brokenHandler =
        new RpcServerHandler() {
          @Override
          public Mono<ServerResponsePayload> singleRequestSingleResponse(
              ServerRequestPayload payload) {
            return Mono.just(
                ServerResponsePayload.create(
                    protocol -> {
                      throw new IllegalStateException("cannot serialize");
                    },
                    new ResponseRpcMetadata.Builder().build(),
                    null,
                    false));
          }
        };

    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata("requestResponse", RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE);
    ByteBuf data = createData(requestMetadata, createTestRequest(0, "foo"));
    ByteBuf metadata = createMetadata(requestMetadata);
    Payload request = PayloadUtil.createPayload(data, metadata);

    StepVerifier.create(new ThriftServerRSocket(brokenHandler, alloc).requestResponse(request))
        .expectErrorSatisfies(
            t ->
                assertServerError(
                    t,
                    ErrorFrameCodec.CANCELED,
                    ResponseRpcErrorCode.UNKNOWN,
                    ResponseRpcErrorCategory.INTERNAL_ERROR))
        .verify();

    assertRequestBuffersFreed(data, metadata);
  }

  @Test
  public void testFirstResponseSerializationFailureCancelsStream() {
    this.protocolId = ProtocolId.COMPACT;
    RpcServerHandler brokenHandler =
        new RpcServerHandler() {
          @Override
          public Flux<ServerResponsePayload> singleRequestStreamingResponse(
              ServerRequestPayload payload) {
            return Flux.just(
                ServerResponsePayload.create(
                    protocol -> {
                      throw new IllegalStateException("cannot serialize");
                    },
                    new ResponseRpcMetadata.Builder().build(),
                    null,
                    false));
          }
        };

    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata("streamResponse", RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE);
    ByteBuf data = createData(requestMetadata, createTestRequest(0, "foo"));
    ByteBuf metadata = createMetadata(requestMetadata);
    Payload request = PayloadUtil.createPayload(data, metadata);

    StepVerifier.create(new ThriftServerRSocket(brokenHandler, alloc).requestStream(request))
        .expectErrorSatisfies(
            t ->
                assertServerError(
                    t,
                    ErrorFrameCodec.CANCELED,
                    ResponseRpcErrorCode.UNKNOWN,
                    ResponseRpcErrorCategory.INTERNAL_ERROR))
        .verify();

    assertRequestBuffersFreed(data, metadata);
  }

  @Test
  public void testHookThrowInsideErrorMappingCancelsRequest() {
    this.protocolId = ProtocolId.COMPACT;
    ThriftEventHandler throwingHook =
        new ThriftEventHandler() {
          @Override
          public void preWriteException(Object context, String methodName, Throwable t) {
            throw new IllegalStateException("hook boom");
          }
        };

    RequestContext requestContext =
        new NettyNiftyRequestContext(Collections.emptyMap(), new NiftyConnectionContext());
    ContextChain chain =
        new ContextChain(
            Collections.singletonList(throwingHook), "requestResponse", requestContext);

    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata("requestResponse", RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE);
    ServerResponsePayload errorPayload =
        RpcPayloadUtil.fromTApplicationException(
            OverloadedException.host("busy"), requestMetadata, chain);

    RpcServerHandler hookHandler =
        new RpcServerHandler() {
          @Override
          public Mono<ServerResponsePayload> singleRequestSingleResponse(
              ServerRequestPayload payload) {
            return Mono.just(errorPayload);
          }
        };

    ByteBuf data = createData(requestMetadata, createTestRequest(0, "foo"));
    ByteBuf metadata = createMetadata(requestMetadata);
    Payload request = PayloadUtil.createPayload(data, metadata);

    StepVerifier.create(new ThriftServerRSocket(hookHandler, alloc).requestResponse(request))
        .expectErrorSatisfies(
            t ->
                assertServerError(
                    t,
                    ErrorFrameCodec.CANCELED,
                    ResponseRpcErrorCode.UNKNOWN,
                    ResponseRpcErrorCategory.INTERNAL_ERROR))
        .verify();

    assertRequestBuffersFreed(data, metadata);
  }

  @Test
  public void testApplicationErrorFrameCancelsRequest() {
    this.protocolId = ProtocolId.COMPACT;
    RpcServerHandler rawFrameHandler =
        new RpcServerHandler() {
          @Override
          public Mono<ServerResponsePayload> singleRequestSingleResponse(
              ServerRequestPayload payload) {
            return Mono.error(new ApplicationErrorException("boom"));
          }
        };

    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata("requestResponse", RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE);
    ByteBuf data = createData(requestMetadata, createTestRequest(0, "foo"));
    ByteBuf metadata = createMetadata(requestMetadata);
    Payload request = PayloadUtil.createPayload(data, metadata);

    StepVerifier.create(new ThriftServerRSocket(rawFrameHandler, alloc).requestResponse(request))
        .expectErrorSatisfies(
            t -> {
              assertServerError(
                  t,
                  ErrorFrameCodec.CANCELED,
                  ResponseRpcErrorCode.UNKNOWN,
                  ResponseRpcErrorCategory.INTERNAL_ERROR);
              assertEquals("boom", decodeRocketError(t).getWhatUtf8());
            })
        .verify();

    assertRequestBuffersFreed(data, metadata);
  }

  @Test
  public void testApplicationErrorFrameCancelsStream() {
    this.protocolId = ProtocolId.COMPACT;
    RpcServerHandler rawFrameHandler =
        new RpcServerHandler() {
          @Override
          public Flux<ServerResponsePayload> singleRequestStreamingResponse(
              ServerRequestPayload payload) {
            return Flux.error(new ApplicationErrorException("boom"));
          }
        };

    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata("streamResponse", RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE);
    ByteBuf data = createData(requestMetadata, createTestRequest(0, "foo"));
    ByteBuf metadata = createMetadata(requestMetadata);
    Payload request = PayloadUtil.createPayload(data, metadata);

    StepVerifier.create(new ThriftServerRSocket(rawFrameHandler, alloc).requestStream(request))
        .expectErrorSatisfies(
            t -> {
              assertServerError(
                  t,
                  ErrorFrameCodec.CANCELED,
                  ResponseRpcErrorCode.UNKNOWN,
                  ResponseRpcErrorCategory.INTERNAL_ERROR);
              assertEquals("boom", decodeRocketError(t).getWhatUtf8());
            })
        .verify();

    assertRequestBuffersFreed(data, metadata);
  }

  /**
   * A sized 64 MiB single response allocates once at exactly the hinted bound and never grows, with
   * data bytes identical to the flat serialization. A plain unhinted buffer would need many growth
   * steps here, so this fails without the sized path.
   */
  @Test
  public void testSizedSingleResponseNeverGrows() throws TException {
    this.protocolId = ProtocolId.COMPACT;
    CountingAllocator counting = new CountingAllocator();
    Writer writer = TestResponseWriters.sized(TestResponseWriters.largePayload());
    ServerResponsePayload payload =
        ServerResponsePayload.create(
            writer, new ResponseRpcMetadata.Builder().build(), null, false);

    RpcServerHandler stubHandler =
        new RpcServerHandler() {
          @Override
          public Mono<ServerResponsePayload> singleRequestSingleResponse(
              ServerRequestPayload request) {
            return Mono.just(payload);
          }
        };

    ThriftServerRSocket server = new ThriftServerRSocket(stubHandler, counting);
    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata("large", RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE);
    Payload request = createPayload(requestMetadata, 5, "foo");

    ByteBuf expected = flatRsocketBody(writer);

    StepVerifier.create(server.requestResponse(request))
        .assertNext(
            rsocketResponse -> {
              try {
                assertEquals(0, counting.growthSteps());
                assertFalse(rsocketResponse.data() instanceof CompositeByteBuf);
                assertEquals(
                    TestResponseWriters.bareBound(writer), rsocketResponse.data().capacity());
                assertTrue(ByteBufUtil.equals(rsocketResponse.data(), expected));
              } finally {
                rsocketResponse.release();
              }
            })
        .verifyComplete();

    expected.release();
  }

  /**
   * A sized 64 MiB first stream response allocates once at exactly the hinted bound and never
   * grows, with data bytes identical to the flat serialization.
   */
  @Test
  public void testSizedStreamResponseNeverGrows() throws TException {
    this.protocolId = ProtocolId.COMPACT;
    CountingAllocator counting = new CountingAllocator();
    Writer writer = TestResponseWriters.sized(TestResponseWriters.largePayload());
    ServerResponsePayload payload =
        ServerResponsePayload.create(
            writer, new ResponseRpcMetadata.Builder().build(), null, false);

    RpcServerHandler stubHandler =
        new RpcServerHandler() {
          @Override
          public Flux<ServerResponsePayload> singleRequestStreamingResponse(
              ServerRequestPayload request) {
            return Flux.just(payload);
          }
        };

    ThriftServerRSocket server = new ThriftServerRSocket(stubHandler, counting);
    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata("large", RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE);
    Payload request = createPayload(requestMetadata, 5, "foo");

    ByteBuf expected = flatRsocketBody(writer);

    StepVerifier.create(server.requestStream(request))
        .assertNext(
            rsocketResponse -> {
              try {
                assertEquals(0, counting.growthSteps());
                assertFalse(rsocketResponse.data() instanceof CompositeByteBuf);
                assertEquals(
                    TestResponseWriters.bareBound(writer), rsocketResponse.data().capacity());
                assertTrue(ByteBufUtil.equals(rsocketResponse.data(), expected));
              } finally {
                rsocketResponse.release();
              }
            })
        .verifyComplete();

    expected.release();
  }

  /**
   * A writer whose size hint throws still runs the write error path: the request is canceled with
   * UNKNOWN/INTERNAL_ERROR and all request buffers are freed.
   */
  @Test
  public void testThrowingSizeHintCancelsRequest() {
    this.protocolId = ProtocolId.COMPACT;
    Writer writer =
        new Writer() {
          @Override
          public void write(TProtocol protocol) throws TException {
            throw new TException("boom");
          }

          @Override
          public int sizeHint() {
            throw new RuntimeException("size boom");
          }
        };

    RpcServerHandler brokenHandler =
        new RpcServerHandler() {
          @Override
          public Mono<ServerResponsePayload> singleRequestSingleResponse(
              ServerRequestPayload request) {
            return Mono.just(
                ServerResponsePayload.create(
                    writer, new ResponseRpcMetadata.Builder().build(), null, false));
          }
        };

    RequestRpcMetadata requestMetadata =
        createRequestRpcMetadata("requestResponse", RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE);
    ByteBuf data = createData(requestMetadata, createTestRequest(0, "foo"));
    ByteBuf metadata = createMetadata(requestMetadata);
    Payload request = PayloadUtil.createPayload(data, metadata);

    StepVerifier.create(new ThriftServerRSocket(brokenHandler, alloc).requestResponse(request))
        .expectErrorSatisfies(
            t ->
                assertServerError(
                    t,
                    ErrorFrameCodec.CANCELED,
                    ResponseRpcErrorCode.UNKNOWN,
                    ResponseRpcErrorCategory.INTERNAL_ERROR))
        .verify();

    assertRequestBuffersFreed(data, metadata);
  }

  private ByteBuf flatRsocketBody(Writer writer) throws TException {
    ByteBuf buf = alloc.buffer();
    TProtocol out = TProtocolType.fromProtocolId(protocolId).apply(buf);
    writer.write(out);
    return buf;
  }
}
