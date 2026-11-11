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

package com.facebook.thrift.legacy.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.facebook.nifty.core.RequestContext;
import com.facebook.nifty.core.RequestContexts;
import com.facebook.swift.service.ThriftEventHandler;
import com.facebook.thrift.example.ping.PingRequest;
import com.facebook.thrift.example.ping.PingResponse;
import com.facebook.thrift.example.ping.PingService;
import com.facebook.thrift.example.ping.PingServiceRpcServerHandler;
import com.facebook.thrift.legacy.codec.LegacyTransportType;
import com.facebook.thrift.legacy.codec.ThriftFrame;
import com.facebook.thrift.payload.ServerRequestPayload;
import com.facebook.thrift.payload.ServerResponsePayload;
import com.facebook.thrift.protocol.TProtocolType;
import com.facebook.thrift.server.RpcServerHandler;
import io.airlift.units.Duration;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.thrift.ResponseRpcMetadata;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

class ThriftServerHandlerTest {

  /**
   * If request dispatch fails synchronously (here, the handler throws from {@code
   * singleRequestSingleResponse} while the response Mono is being assembled), the decoded frame
   * must still be released. Before the fix, the throw escaped {@code messageReceived} before the
   * reactive {@code doFinally} was engaged, leaking the underlying direct buffer.
   */
  @Test
  void releasesFrameWhenDispatchThrowsSynchronously() {
    RpcServerHandler throwingHandler =
        new RpcServerHandler() {
          @Override
          public Mono<ServerResponsePayload> singleRequestSingleResponse(
              ServerRequestPayload payload) {
            throw new RuntimeException("synchronous dispatch failure");
          }
        };

    EmbeddedChannel channel =
        new EmbeddedChannel(
            new ThriftServerHandler(throwingHandler, new Duration(60, TimeUnit.SECONDS)));

    ByteBuf message = Unpooled.buffer();
    TProtocol out = TProtocolType.TCompact.apply(message);
    // seqId != -1 -> SINGLE_REQUEST_SINGLE_RESPONSE; decodeMessage only reads the message begin.
    out.writeMessageBegin(new TMessage("doStuff", TMessageType.CALL, 1));

    ThriftFrame frame =
        new ThriftFrame(
            1,
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

  @Test
  void rawSchedulerRejectionStampsHostCode() throws Exception {
    EmbeddedChannel channel =
        new EmbeddedChannel(
            new ThriftServerHandler(
                mockHandler(Mono.error(new RejectedExecutionException("no threads"))),
                new Duration(60, TimeUnit.SECONDS)));

    channel.writeInbound(requestFrame(TProtocolType.TCompact, "ping", TMessageType.CALL, 3, false));

    ThriftFrame response = awaitResponse(channel);
    try {
      assertEquals("1", response.getHeaders().get("ex"));
      assertFalse(response.getHeaders().containsKey("uex"));
    } finally {
      response.release();
    }
    assertTrue(channel.isActive());
    channel.finishAndReleaseAll();
  }

  @Test
  void cppStyleOnewayBinaryRoutesNoResponse() throws Exception {
    cppStyleOnewayRoutesNoResponse(TProtocolType.TBinary);
  }

  @Test
  void cppStyleOnewayCompactRoutesNoResponse() throws Exception {
    cppStyleOnewayRoutesNoResponse(TProtocolType.TCompact);
  }

  private void cppStyleOnewayRoutesNoResponse(TProtocolType protocol) throws Exception {
    // C++ Header oneway (frame -1, T_CALL body 0) must dispatch to singleRequestNoResponse and
    // never to request-response; a raw scheduler rejection stays silent and releases the buffer.
    AtomicBoolean noResponse = new AtomicBoolean();
    AtomicBoolean requestResponse = new AtomicBoolean();
    AtomicBoolean terminal = new AtomicBoolean();
    RpcServerHandler handler =
        new RpcServerHandler() {
          @Override
          public Mono<Void> singleRequestNoResponse(ServerRequestPayload payload) {
            noResponse.set(true);
            return Mono.<Void>error(new RejectedExecutionException("no threads"))
                .doFinally(s -> terminal.set(true));
          }

          @Override
          public Mono<ServerResponsePayload> singleRequestSingleResponse(
              ServerRequestPayload payload) {
            requestResponse.set(true);
            return Mono.just(successPayload());
          }
        };
    EmbeddedChannel channel =
        new EmbeddedChannel(new ThriftServerHandler(handler, new Duration(60, TimeUnit.SECONDS)));

    ThriftFrame request = cppOnewayFrame(protocol, "ping");
    ByteBuf requestBytes = request.getMessage();
    channel.writeInbound(request);
    waitFor("oneway pipeline terminal", terminal);
    long releaseDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (requestBytes.refCnt() > 0) {
      if (System.nanoTime() > releaseDeadline) {
        throw new AssertionError("timed out waiting for request buffer release");
      }
      Thread.sleep(5);
    }
    channel.runPendingTasks();

    assertTrue(noResponse.get());
    assertFalse(requestResponse.get());
    assertNull(channel.readOutbound());
    assertTrue(channel.isActive());
    channel.finishAndReleaseAll();
  }

  @Test
  void unknownMethodHasNoOverloadSignal() throws Exception {
    RpcServerHandler handler =
        new PingServiceRpcServerHandler(
            Mockito.mock(PingService.class), Collections.<ThriftEventHandler>emptyList());
    EmbeddedChannel channel =
        new EmbeddedChannel(new ThriftServerHandler(handler, new Duration(60, TimeUnit.SECONDS)));

    channel.writeInbound(
        requestFrame(TProtocolType.TCompact, "noSuchMethod", TMessageType.CALL, 5, false));

    ThriftFrame response = awaitResponse(channel);
    try {
      assertEquals(TApplicationException.UNKNOWN_METHOD, readBody(response).getType());
      assertFalse(response.getHeaders().containsKey("ex"));
      assertFalse(response.getHeaders().containsKey("uex"));
    } finally {
      response.release();
    }
    assertTrue(channel.isActive());
    channel.finishAndReleaseAll();
  }

  @Test
  void inboundReservedHeadersNotEchoed() throws Exception {
    Map<String, String> inbound = new HashMap<>();
    inbound.put("ex", "1");
    inbound.put("uex", "Evil");
    ByteBuf message = Unpooled.buffer();
    TProtocol out = TProtocolType.TCompact.apply(message);
    out.writeMessageBegin(new TMessage("ping", TMessageType.CALL, 11));
    out.writeStructBegin(new TStruct("args"));
    out.writeFieldStop();
    out.writeStructEnd();
    out.writeMessageEnd();
    ThriftFrame frame =
        new ThriftFrame(
            11,
            message,
            inbound,
            Collections.emptyMap(),
            LegacyTransportType.HEADER,
            TProtocolType.TCompact,
            false);

    EmbeddedChannel channel =
        new EmbeddedChannel(
            new ThriftServerHandler(
                mockHandler(Mono.just(successPayload())), new Duration(60, TimeUnit.SECONDS)));
    channel.writeInbound(frame);

    ThriftFrame response = awaitResponse(channel);
    try {
      assertFalse(response.getHeaders().containsKey("ex"));
      assertFalse(response.getHeaders().containsKey("uex"));
    } finally {
      response.release();
    }
    channel.finishAndReleaseAll();
  }

  @Test
  void onewayTimeoutSendsNoResponse() throws Exception {
    AtomicBoolean cancelled = new AtomicBoolean();
    RpcServerHandler neverHandler =
        new RpcServerHandler() {
          @Override
          public Mono<Void> singleRequestNoResponse(ServerRequestPayload payload) {
            return Mono.<Void>never().doOnCancel(() -> cancelled.set(true));
          }
        };
    EmbeddedChannel channel =
        new EmbeddedChannel(
            new ThriftServerHandler(neverHandler, new Duration(50, TimeUnit.MILLISECONDS)));

    ThriftFrame request =
        requestFrame(TProtocolType.TCompact, "ping", TMessageType.ONEWAY, -1, false);
    ByteBuf requestBytes = request.getMessage();
    channel.writeInbound(request);
    // The timeout firing cancels the source and releases the request buffer.
    waitFor("oneway timeout cancellation", cancelled);
    long releaseDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (requestBytes.refCnt() > 0) {
      if (System.nanoTime() > releaseDeadline) {
        throw new AssertionError("timed out waiting for request buffer release");
      }
      Thread.sleep(5);
    }
    channel.runPendingTasks();

    assertNull(channel.readOutbound());
    assertTrue(channel.isActive());
    channel.finishAndReleaseAll();
  }

  @Test
  void hookAddedUexPreservedOnOrdinaryResponse() throws Exception {
    // A write hook stashes the request context at admission and labels the ordinary reply.
    // The real encode path must keep hook-added headers on non-overload responses.
    ThriftEventHandler uexSetter =
        new ThriftEventHandler() {
          @Override
          public Object getContext(String methodName, RequestContext requestContext) {
            requestContext.setResponseHeader("uex", "HookException");
            return null;
          }
        };
    PingService.Reactive delegate = Mockito.mock(PingService.Reactive.class);
    Mockito.when(delegate.ping(Mockito.any(PingRequest.class)))
        .thenReturn(Mono.just(new PingResponse.Builder().setResponse("ok").build()));
    RpcServerHandler handler =
        new PingServiceRpcServerHandler(delegate, Collections.singletonList(uexSetter));
    EmbeddedChannel channel =
        new EmbeddedChannel(new ThriftServerHandler(handler, new Duration(60, TimeUnit.SECONDS)));

    channel.writeInbound(pingFrame(TProtocolType.TCompact, 12));

    ThriftFrame response = awaitResponse(channel);
    try {
      assertEquals("HookException", response.getHeaders().get("uex"));
      assertFalse(response.getHeaders().containsKey("ex"));
    } finally {
      response.release();
    }
    channel.finishAndReleaseAll();
  }

  private static RpcServerHandler mockHandler(Mono<ServerResponsePayload> result) {
    return new RpcServerHandler() {
      @Override
      public Mono<ServerResponsePayload> singleRequestSingleResponse(ServerRequestPayload payload) {
        return result;
      }
    };
  }

  private static ServerResponsePayload successPayload() {
    return ServerResponsePayload.create(
        out -> {}, new ResponseRpcMetadata.Builder().build(), null, false);
  }

  private static ThriftFrame requestFrame(
      TProtocolType protocol, String method, byte messageType, int seqId, boolean outOfOrder) {
    ByteBuf message = Unpooled.buffer();
    TProtocol out = protocol.apply(message);
    out.writeMessageBegin(new TMessage(method, messageType, seqId));
    out.writeStructBegin(new TStruct("args"));
    out.writeFieldStop();
    out.writeStructEnd();
    out.writeMessageEnd();
    return new ThriftFrame(
        seqId,
        message,
        new HashMap<>(),
        Collections.emptyMap(),
        LegacyTransportType.HEADER,
        protocol,
        outOfOrder);
  }

  private static ThriftFrame cppOnewayFrame(TProtocolType protocol, String method) {
    // Valid C++ Header oneway: reserved frame sequence -1 with a regular T_CALL body seq 0.
    ByteBuf message = Unpooled.buffer();
    TProtocol out = protocol.apply(message);
    out.writeMessageBegin(new TMessage(method, TMessageType.CALL, 0));
    out.writeStructBegin(new TStruct("args"));
    out.writeFieldStop();
    out.writeStructEnd();
    out.writeMessageEnd();
    return new ThriftFrame(
        -1,
        message,
        new HashMap<>(),
        Collections.emptyMap(),
        LegacyTransportType.HEADER,
        protocol,
        false);
  }

  private static ThriftFrame pingFrame(TProtocolType protocol, int seqId) {
    ByteBuf message = Unpooled.buffer();
    TProtocol out = protocol.apply(message);
    out.writeMessageBegin(new TMessage("ping", TMessageType.CALL, seqId));
    out.writeStructBegin(new TStruct("args"));
    out.writeFieldBegin(new TField("key", TType.STRUCT, (short) 1));
    out.writeStructBegin(new TStruct("PingRequest"));
    out.writeFieldBegin(new TField("request", TType.STRING, (short) 1));
    out.writeString("hello");
    out.writeFieldEnd();
    out.writeFieldStop();
    out.writeStructEnd();
    out.writeFieldEnd();
    out.writeFieldStop();
    out.writeStructEnd();
    out.writeMessageEnd();
    return new ThriftFrame(
        seqId,
        message,
        new HashMap<>(),
        Collections.emptyMap(),
        LegacyTransportType.HEADER,
        protocol,
        false);
  }

  private static ThriftFrame awaitResponse(EmbeddedChannel channel) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    for (; ; ) {
      channel.runPendingTasks();
      Object outbound = channel.readOutbound();
      if (outbound != null) {
        assertTrue(outbound instanceof ThriftFrame);
        return (ThriftFrame) outbound;
      }
      if (System.nanoTime() > deadline) {
        throw new AssertionError("timed out waiting for response frame");
      }
      Thread.sleep(5);
    }
  }

  private static void waitFor(String what, AtomicBoolean flag) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (!flag.get()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("timed out waiting for " + what);
      }
      Thread.sleep(5);
    }
  }

  private static TApplicationException readBody(ThriftFrame frame) throws TException {
    TProtocol in = frame.getProtocol().apply(frame.getMessage());
    TMessage message = in.readMessageBegin();
    assertEquals(TMessageType.EXCEPTION, message.type);
    TApplicationException exception = TApplicationException.read(in);
    in.readMessageEnd();
    return exception;
  }

  /**
   * The dispatch binds the request only while it runs on the event loop. The old code set it on the
   * event loop and left it there until the pipeline terminated.
   */
  @Test
  void theEventLoopHoldsNoRequestAfterDispatch() {
    EmbeddedChannel channel =
        new EmbeddedChannel(
            new ThriftServerHandler(
                handlerAnswering(name -> Mono.never()), new Duration(60, TimeUnit.SECONDS)));

    channel.writeInbound(frame("pending", 1));

    assertThat(RequestContexts.getCurrentContext()).isNull();
    channel.finishAndReleaseAll();
  }

  /**
   * Request A is still in flight when request B fails on a worker. The old code restored the event
   * loop's previous value, request A, from B's doFinally on that worker. The embedded channel
   * writes on the calling thread, so B's doFinally runs on the worker here.
   */
  @Test
  void aRequestFinishingOnAWorkerLeavesNoOtherRequestThere() {
    Scheduler worker = Schedulers.newSingle("finishing-worker");
    try {
      EmbeddedChannel channel =
          new EmbeddedChannel(
              new ThriftServerHandler(
                  handlerAnswering(
                      name ->
                          "pending".equals(name)
                              ? Mono.never()
                              : Mono.<ServerResponsePayload>error(
                                      new IllegalStateException("request B failed"))
                                  .subscribeOn(worker)),
                  new Duration(60, TimeUnit.SECONDS)));

      channel.writeInbound(frame("pending", 1));
      channel.writeInbound(frame("fails", 2));

      // Single thread, so this probe runs after request B has finished on the worker.
      Optional<RequestContext> leftOnWorker =
          Mono.fromCallable(() -> Optional.ofNullable(RequestContexts.getCurrentContext()))
              .subscribeOn(worker)
              .block();
      assertThat(leftOnWorker).isEmpty();
      channel.finishAndReleaseAll();
    } finally {
      worker.dispose();
    }
  }

  private static RpcServerHandler handlerAnswering(
      Function<String, Mono<ServerResponsePayload>> byMethodName) {
    return new RpcServerHandler() {
      @Override
      public Mono<ServerResponsePayload> singleRequestSingleResponse(ServerRequestPayload payload) {
        return byMethodName.apply(payload.getRequestRpcMetadata().getName());
      }
    };
  }

  private static ThriftFrame frame(String methodName, int sequenceId) {
    ByteBuf message = Unpooled.buffer();
    TProtocol out = TProtocolType.TCompact.apply(message);
    out.writeMessageBegin(new TMessage(methodName, TMessageType.CALL, sequenceId));
    return new ThriftFrame(
        sequenceId,
        message,
        Collections.emptyMap(),
        Collections.emptyMap(),
        LegacyTransportType.HEADER,
        TProtocolType.TCompact,
        false);
  }
}
