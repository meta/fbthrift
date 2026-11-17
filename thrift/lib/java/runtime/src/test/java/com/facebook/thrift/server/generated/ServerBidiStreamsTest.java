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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.facebook.nifty.core.NiftyConnectionContext;
import com.facebook.nifty.core.RequestContext;
import com.facebook.swift.service.ContextChain;
import com.facebook.swift.service.StreamEventHandler;
import com.facebook.thrift.model.ResponseAndStream;
import com.facebook.thrift.payload.FieldReader;
import com.facebook.thrift.payload.RequestData;
import com.facebook.thrift.payload.ServerRequestPayload;
import com.facebook.thrift.payload.ServerResponsePayload;
import com.facebook.thrift.payload.Writer;
import com.facebook.thrift.protocol.ByteBufTProtocol;
import com.facebook.thrift.protocol.TProtocolType;
import com.facebook.thrift.test.rocket.TestException;
import com.facebook.thrift.test.rocket.TestRequest;
import com.facebook.thrift.test.rocket.TestResponse;
import com.facebook.thrift.util.NettyNiftyRequestContext;
import com.facebook.thrift.util.Readers;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiFunction;
import org.apache.thrift.ProtocolId;
import org.apache.thrift.RequestRpcMetadata;
import org.apache.thrift.RpcKind;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

public class ServerBidiStreamsTest {
  private final RequestContext requestContext =
      new NettyNiftyRequestContext(Collections.emptyMap(), new NiftyConnectionContext());

  @Test
  public void bidiWithoutAFirstResponseSendsAnEmptyOne() {
    BidiMethod<Object, TestResponse, TestRequest> method =
        method(
            (args, typed) ->
                Mono.just(
                    ResponseAndStream.of(
                        ServerStreams.NO_FIRST_RESPONSE, Flux.just(response("a")))));

    StepVerifier.create(handleBidi(method, Flux.empty()))
        .assertNext(first -> assertNotNull(first.getResponseRpcMetadata()))
        .assertNext(element -> assertNotNull(element.getStreamPayloadMetadata()))
        .verifyComplete();
  }

  @Test
  public void declaredSinkExceptionReachesTheDelegate() {
    List<Object> seen = new CopyOnWriteArrayList<>();
    Flux<ServerRequestPayload> sinkPayloads =
        Flux.just(
            payload(0, request("a")), payload(1, new TestException.Builder().setMsg("b").build()));

    BidiMethod<Object, TestResponse, TestRequest> method =
        method(
            (args, typed) ->
                Mono.just(
                    ResponseAndStream.of(
                        response("first"),
                        typed
                            .doOnNext(seen::add)
                            .doOnError(seen::add)
                            .onErrorResume(e -> Flux.empty())
                            .map(r -> response(r.getStrField())))));

    StepVerifier.create(handleBidi(method, sinkPayloads)).expectNextCount(2).verifyComplete();

    assertEquals(request("a"), seen.get(0));
    assertEquals("b", ((TestException) seen.get(1)).getMsg());
  }

  @Test
  public void declaredStreamExceptionTravelsInBandAndTheSinkStaysReadable() {
    for (int elements : new int[] {0, 1, 3}) {
      streamExceptionTravelsInBand(elements, new TestException.Builder().setMsg("x").build(), true);
    }
  }

  @Test
  public void undeclaredStreamExceptionTravelsInBandAndTheSinkStaysReadable() {
    for (int elements : new int[] {0, 1, 3}) {
      streamExceptionTravelsInBand(elements, new IllegalStateException("x"), false);
    }
  }

  /**
   * A stream exception completes the call with an exception payload, so the transport keeps a sink
   * that a reader took open.
   */
  private void streamExceptionTravelsInBand(int elements, Throwable error, boolean declared) {
    Sinks.Many<ServerRequestPayload> sink = Sinks.many().unicast().onBackpressureBuffer();
    List<Object> read = new CopyOnWriteArrayList<>();

    BidiMethod<Object, TestResponse, TestRequest> method =
        method(
            (args, typed) -> {
              typed.subscribe(read::add, read::add, () -> read.add("complete"));

              return Mono.just(
                  ResponseAndStream.of(
                      ServerStreams.NO_FIRST_RESPONSE,
                      Flux.range(0, elements)
                          .map(i -> response("s" + i))
                          .concatWith(Flux.error(error))));
            });

    StepVerifier.create(handleBidi(method, sink.asFlux()))
        .expectNextCount(1 + elements)
        .assertNext(
            last -> {
              if (declared) {
                assertTrue(
                    last.getStreamPayloadMetadata()
                        .getPayloadMetadata()
                        .getExceptionMetadata()
                        .getMetadata()
                        .isSetDeclaredException());
              } else {
                assertNotNull(last.getApplicationException());
              }
            })
        .verifyComplete();

    sink.tryEmitNext(payload(0, request("after")));
    sink.tryEmitComplete();
    assertEquals(Arrays.asList(request("after"), "complete"), read);
  }

  @Test
  public void sinkErrorLeavesTheStreamRunning() {
    BidiMethod<Object, TestResponse, TestRequest> method =
        method(
            (args, typed) ->
                Mono.just(
                    ResponseAndStream.of(
                        ServerStreams.NO_FIRST_RESPONSE,
                        typed
                            .map(r -> response(r.getStrField()))
                            .onErrorResume(e -> Flux.just(response("after"))))));

    StepVerifier.create(
            handleBidi(
                method,
                Flux.concat(
                    Flux.just(payload(0, request("a"))),
                    Flux.error(new IllegalStateException("sink")))))
        .expectNextCount(1)
        .assertNext(element -> assertNotNull(element.getStreamPayloadMetadata()))
        .assertNext(element -> assertNotNull(element.getStreamPayloadMetadata()))
        .verifyComplete();
  }

  @Test
  public void readerCancelLeavesTheStreamRunning() {
    BidiMethod<Object, TestResponse, TestRequest> method =
        method(
            (args, typed) ->
                Mono.just(
                    ResponseAndStream.of(
                        ServerStreams.NO_FIRST_RESPONSE,
                        typed
                            .take(1)
                            .map(r -> response(r.getStrField()))
                            .concatWith(Flux.just(response("after"))))));

    StepVerifier.create(
            handleBidi(method, Flux.concat(Flux.just(payload(0, request("a"))), Flux.never())))
        .expectNextCount(3)
        .verifyComplete();
  }

  @Test
  public void streamHooksEndWithTheStreamWhileTheSinkStaysOpen() {
    HookLog hooks = new HookLog();
    BidiMethod<Object, TestResponse, TestRequest> method =
        method(
            (args, typed) -> {
              typed.subscribe();

              return Mono.just(
                  ResponseAndStream.of(ServerStreams.NO_FIRST_RESPONSE, Flux.just(response("a"))));
            });
    ContextChain chain =
        new ContextChain(Collections.singletonList(hooks), "channel", requestContext);

    StepVerifier.create(
            ServerBidiStreams.handleBidi(payload(1, request("setup")), chain, method, Flux.never())
                .doFinally(signal -> chain.finishRequest()))
        .expectNextCount(2)
        .verifyComplete();

    // The sink of the call stays open, but the hooks end with the stream and fire no sink hook.
    assertEquals(
        Arrays.asList(
            "stream credit", "stream subscribe", "stream next", "stream finally COMPLETE", "done"),
        hooks.events);
  }

  private Flux<ServerResponsePayload> handleBidi(
      BidiMethod<Object, TestResponse, TestRequest> method,
      Flux<ServerRequestPayload> sinkPayloads) {
    return ServerBidiStreams.handleBidi(
        payload(1, request("setup")),
        new ContextChain(Collections.emptyList(), "channel", requestContext),
        method,
        sinkPayloads);
  }

  @SuppressWarnings("unchecked")
  private static BidiMethod<Object, TestResponse, TestRequest> method(
      BiFunction<List<Object>, Flux<TestRequest>, Mono<ResponseAndStream<Object, TestResponse>>>
          invoke) {
    return new BidiMethod<>(
        Collections.emptyList(),
        sinkReaders(),
        invoke,
        ServerStreams.EMPTY_FIRST_RESPONSE_WRITER,
        ServerBidiStreamsTest::writer,
        "channel",
        null,
        null,
        new Class[] {TestException.class},
        new Integer[] {1});
  }

  /** Records the stream hooks, any sink hook and the end of the request in call order. */
  private static final class HookLog implements StreamEventHandler {
    final List<String> events = new CopyOnWriteArrayList<>();

    @Override
    public void onStreamSubscribe(Object context, String methodName) {
      events.add("stream subscribe");
    }

    @Override
    public void onStreamNext(Object context, String methodName, long sequenceNumber) {
      events.add("stream next");
    }

    @Override
    public void onStreamCredit(Object context, String methodName, int credits) {
      events.add("stream credit");
    }

    @Override
    public void onStreamFinally(Object context, String methodName, StreamOutcome outcome) {
      events.add("stream finally " + outcome);
    }

    @Override
    public void onSinkSubscribe(Object context, String methodName) {
      events.add("sink subscribe");
    }

    @Override
    public void onSinkFinally(Object context, String methodName, SinkOutcome outcome) {
      events.add("sink finally " + outcome);
    }

    @Override
    public void done(Object context, String methodName) {
      events.add("done");
    }
  }

  private static List<FieldReader<?>> sinkReaders() {
    return Arrays.asList(
        new FieldReader<>(
            (short) 0, TType.STRUCT, Readers.wrap(TestRequest.asReader()), () -> null),
        new FieldReader<>(
            (short) 1, TType.STRUCT, Readers.wrap(TestException.asReader()), () -> null));
  }

  private static Writer writer(Object response, ContextChain chain) {
    return protocol -> {
      protocol.writeStructBegin(new TStruct("result"));
      protocol.writeFieldBegin(new TField("success", TType.STRUCT, (short) 0));
      ((TestResponse) response).write0(protocol);
      protocol.writeFieldEnd();
      protocol.writeFieldStop();
      protocol.writeStructEnd();
    };
  }

  private ServerRequestPayload payload(int fieldId, Object struct) {
    ByteBuf data = Unpooled.buffer();
    ByteBufTProtocol protocol = TProtocolType.TCompact.apply(data);
    protocol.writeStructBegin(new TStruct("payload"));
    protocol.writeFieldBegin(new TField("field", TType.STRUCT, (short) fieldId));
    if (struct instanceof TestRequest) {
      ((TestRequest) struct).write0(protocol);
    } else {
      ((TestException) struct).write0(protocol);
    }
    protocol.writeFieldEnd();
    protocol.writeFieldStop();
    protocol.writeStructEnd();

    RequestRpcMetadata metadata =
        new RequestRpcMetadata.Builder()
            .setProtocol(ProtocolId.COMPACT)
            .setName("channel")
            .setKind(RpcKind.BIDIRECTIONAL_STREAM)
            .build();

    return ServerRequestPayload.create(metadata, requestContext, RequestData.of(data, protocol));
  }

  private static TestRequest request(String value) {
    return new TestRequest.Builder().setStrField(value).build();
  }

  private static TestResponse response(String value) {
    return new TestResponse.Builder().setStrField(value).build();
  }
}
