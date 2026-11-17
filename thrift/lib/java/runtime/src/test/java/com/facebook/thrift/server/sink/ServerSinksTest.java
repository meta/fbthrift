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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.facebook.nifty.core.NiftyConnectionContext;
import com.facebook.nifty.core.RequestContext;
import com.facebook.swift.service.ContextChain;
import com.facebook.swift.service.StreamEventHandler;
import com.facebook.thrift.model.ResponseAndStream;
import com.facebook.thrift.model.StreamResponse;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
import reactor.test.StepVerifier;
import reactor.util.context.Context;

public class ServerSinksTest {
  private final RequestContext requestContext =
      new NettyNiftyRequestContext(Collections.emptyMap(), new NiftyConnectionContext());

  @Test
  public void sinkWithoutFinalResponseEndsWithAnInBandError() {
    SinkMethod<TestResponse, TestResponse, TestRequest> method =
        method(
            (args, typed) ->
                ServerSinks.firstAndFinal(
                    Flux.just(StreamResponse.fromFirstResponse(response("first")))));

    StepVerifier.create(handleSink(method, Flux.empty()))
        .assertNext(first -> assertNotNull(first.getResponseRpcMetadata()))
        .assertNext(
            error -> {
              assertNotNull(error.getStreamPayloadMetadata());
              assertNotNull(error.getApplicationException());
            })
        .verifyComplete();
  }

  @Test
  public void sinkResultsEndWhenTheFinalIsNeverRead() {
    AtomicBoolean cancelled = new AtomicBoolean();
    Flux<StreamResponse<TestResponse, TestResponse>> results =
        Flux.just(StreamResponse.<TestResponse, TestResponse>fromFirstResponse(response("first")))
            .concatWith(Flux.never())
            .doOnCancel(() -> cancelled.set(true));

    SinkMethod<TestResponse, TestResponse, TestRequest> method =
        new SinkMethod<>(
            Collections.emptyList(),
            sinkReaders(),
            (args, typed) -> ServerSinks.firstAndFinal(results),
            (o, chain) -> {
              throw new IllegalStateException("unwritable first response");
            },
            ServerSinksTest::writer,
            "channel",
            null,
            null,
            null,
            null);

    StepVerifier.create(handleSink(method, Flux.empty()))
        .assertNext(error -> assertNotNull(error.getApplicationException()))
        .verifyComplete();

    assertTrue(cancelled.get());
  }

  @Test
  public void sinkEndsAtItsFinalBeforeTheRequestIsDone() throws InterruptedException {
    HookLog hooks = new HookLog();
    SinkMethod<TestResponse, TestResponse, TestRequest> method =
        method(
            (args, typed) ->
                Mono.just(
                    ResponseAndStream.of(
                        response("first"),
                        typed
                            .doOnNext(element -> hooks.events.add("delegate"))
                            .then(Mono.just(response("final")))
                            .flux())));

    StepVerifier.create(handleSink(hooks, method, Flux.just(payload(0, request("a")))))
        .expectNextCount(2)
        .verifyComplete();

    await(hooks.done);
    assertEquals(
        Arrays.asList("subscribe", "next", "consumed", "delegate", "finally COMPLETE", "done"),
        hooks.events);
  }

  @Test
  public void sinkEndsAtAPeerSinkErrorBeforeTheDelegateSeesIt() throws InterruptedException {
    HookLog hooks = new HookLog();
    SinkMethod<TestResponse, TestResponse, TestRequest> method =
        method(
            (args, typed) ->
                Mono.just(
                    ResponseAndStream.of(
                        response("first"),
                        typed
                            .doOnError(e -> hooks.events.add("delegate"))
                            .onErrorResume(e -> Flux.empty())
                            .then(Mono.just(response("final")))
                            .flux())));

    StepVerifier.create(handleSink(hooks, method, Flux.error(new IllegalStateException("sink"))))
        .expectNextCount(2)
        .verifyComplete();

    await(hooks.done);
    assertEquals(
        Arrays.asList("subscribe", "error", "finally ERROR", "delegate", "done"), hooks.events);
  }

  @Test
  public void declaredPeerSinkErrorReachesTheDelegateUnwrapped() throws InterruptedException {
    HookLog hooks = new HookLog();
    List<Object> seen = new CopyOnWriteArrayList<>();
    SinkMethod<TestResponse, TestResponse, TestRequest> method =
        method(
            (args, typed) ->
                Mono.just(
                    ResponseAndStream.of(
                        response("first"),
                        typed
                            .doOnNext(seen::add)
                            .doOnError(seen::add)
                            .onErrorResume(e -> Flux.empty())
                            .then(Mono.just(response("final")))
                            .flux())));
    Flux<ServerRequestPayload> sinkPayloads =
        Flux.just(
            payload(0, request("a")), payload(1, new TestException.Builder().setMsg("b").build()));

    StepVerifier.create(handleSink(hooks, method, sinkPayloads))
        .expectNextCount(2)
        .verifyComplete();

    await(hooks.done);
    assertEquals(request("a"), seen.get(0));
    assertEquals("b", ((TestException) seen.get(1)).getMsg());
    assertEquals(
        Arrays.asList("subscribe", "next", "consumed", "error", "finally ERROR", "done"),
        hooks.events);
  }

  @Test
  public void sinkEndsWithAnErrorWhenTheCallIsCancelledBeforeItsFinal()
      throws InterruptedException {
    HookLog hooks = new HookLog();
    SinkMethod<TestResponse, TestResponse, TestRequest> method =
        method(
            (args, typed) ->
                Mono.just(
                    ResponseAndStream.of(
                        response("first"), typed.thenMany(Flux.<TestResponse>never()))));

    StepVerifier.create(handleSink(hooks, method, Flux.never()))
        .expectNextCount(1)
        .then(() -> await(hooks.subscribed))
        .thenCancel()
        .verify();

    await(hooks.done);
    assertEquals(Arrays.asList("subscribe", "cancel", "finally ERROR", "done"), hooks.events);
  }

  @Test
  public void sinkFiresNoStreamHooks() throws InterruptedException {
    HookLog hooks = new HookLog();
    SinkMethod<TestResponse, TestResponse, TestRequest> method =
        method(
            (args, typed) ->
                Mono.just(ResponseAndStream.of(response("first"), Flux.just(response("final")))));

    StepVerifier.create(handleSink(hooks, method, Flux.empty()))
        .expectNextCount(2)
        .verifyComplete();

    // The delegate never read its sink, so no sink hook fires either: only done().
    await(hooks.done);
    assertEquals(Collections.singletonList("done"), hooks.events);
  }

  @Test
  public void firstAndFinalKeepsTheContextOfTheCaller() {
    Flux<StreamResponse<TestResponse, TestResponse>> results =
        Flux.deferContextual(
            context ->
                Flux.just(
                    StreamResponse.fromFirstResponse(response(context.get("caller"))),
                    StreamResponse.fromData(response("final"))));

    StepVerifier.create(
            ServerSinks.firstAndFinal(results).contextWrite(Context.of("caller", "seen")))
        .assertNext(
            firstAndFinal -> {
              assertEquals("seen", firstAndFinal.getFirstResponse().getStrField());
              firstAndFinal.dispose();
            })
        .verifyComplete();
  }

  @Test
  public void extraFinalsMakeTheFinalAnError() {
    SinkMethod<TestResponse, TestResponse, TestRequest> method =
        method(
            (args, typed) ->
                ServerSinks.firstAndFinal(
                    Flux.just(
                        StreamResponse.fromFirstResponse(response("first")),
                        StreamResponse.fromData(response("final")),
                        StreamResponse.fromData(response("extra")),
                        StreamResponse.fromData(response("more")))));

    StepVerifier.create(handleSink(method, Flux.empty()))
        .assertNext(first -> assertNotNull(first.getResponseRpcMetadata()))
        .assertNext(error -> assertNotNull(error.getApplicationException()))
        .verifyComplete();
  }

  private Flux<ServerResponsePayload> handleSink(
      SinkMethod<TestResponse, TestResponse, TestRequest> method,
      Flux<ServerRequestPayload> sinkPayloads) {
    return handleSink(
        new ContextChain(Collections.emptyList(), "channel", requestContext), method, sinkPayloads);
  }

  private Flux<ServerResponsePayload> handleSink(
      HookLog hooks,
      SinkMethod<TestResponse, TestResponse, TestRequest> method,
      Flux<ServerRequestPayload> sinkPayloads) {
    return handleSink(
        new ContextChain(Collections.singletonList(hooks), "channel", requestContext),
        method,
        sinkPayloads);
  }

  /** Ends the request like the generated dispatcher does: finishRequest fires done() for a sink. */
  private Flux<ServerResponsePayload> handleSink(
      ContextChain chain,
      SinkMethod<TestResponse, TestResponse, TestRequest> method,
      Flux<ServerRequestPayload> sinkPayloads) {
    return ServerSinks.handleSink(payload(1, request("setup")), chain, method, sinkPayloads)
        .doFinally(signal -> chain.finishRequest());
  }

  private static SinkMethod<TestResponse, TestResponse, TestRequest> method(
      BiFunction<
              List<Object>, Flux<TestRequest>, Mono<ResponseAndStream<TestResponse, TestResponse>>>
          invoke) {
    return new SinkMethod<>(
        Collections.emptyList(),
        sinkReaders(),
        invoke,
        ServerSinksTest::writer,
        ServerSinksTest::writer,
        "channel",
        null,
        null,
        null,
        null);
  }

  private static void await(CountDownLatch latch) {
    try {
      assertTrue(latch.await(5, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      throw new AssertionError(e);
    }
  }

  /** Records the sink hooks, any stream hook and the end of the request in call order. */
  private static final class HookLog implements StreamEventHandler {
    final List<String> events = new CopyOnWriteArrayList<>();
    final CountDownLatch subscribed = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(1);

    @Override
    public void onStreamSubscribe(Object context, String methodName) {
      events.add("stream subscribe");
    }

    @Override
    public void onStreamNext(Object context, String methodName, long sequenceNumber) {
      events.add("stream next");
    }

    @Override
    public void onStreamFinally(Object context, String methodName, StreamOutcome outcome) {
      events.add("stream finally");
    }

    @Override
    public void onSinkSubscribe(Object context, String methodName) {
      events.add("subscribe");
      subscribed.countDown();
    }

    @Override
    public void onSinkNext(Object context, String methodName, long sequenceNumber) {
      events.add("next");
    }

    @Override
    public void onSinkConsumed(Object context, String methodName) {
      events.add("consumed");
    }

    @Override
    public void onSinkCancel(Object context, String methodName) {
      events.add("cancel");
    }

    @Override
    public void handleSinkError(Object context, String methodName, Throwable error) {
      events.add("error");
    }

    @Override
    public void onSinkFinally(Object context, String methodName, SinkOutcome outcome) {
      events.add("finally " + outcome);
    }

    @Override
    public void done(Object context, String methodName) {
      events.add("done");
      done.countDown();
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
            .setKind(RpcKind.SINK)
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
