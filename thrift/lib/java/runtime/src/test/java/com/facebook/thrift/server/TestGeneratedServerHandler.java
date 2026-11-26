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

package com.facebook.thrift.server;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.facebook.swift.service.ThriftServerConfig;
import com.facebook.thrift.client.RpcClientFactory;
import com.facebook.thrift.client.ThriftClientConfig;
import com.facebook.thrift.example.ping.CustomException;
import com.facebook.thrift.example.ping.ExtendedPing;
import com.facebook.thrift.example.ping.ExtendedPingRpcServerHandler;
import com.facebook.thrift.example.ping.PingRequest;
import com.facebook.thrift.example.ping.PingResponse;
import com.facebook.thrift.example.ping.PingService;
import com.facebook.thrift.example.ping.PingServiceRpcServerHandler;
import com.facebook.thrift.legacy.server.LegacyServerTransport;
import com.facebook.thrift.legacy.server.LegacyServerTransportFactory;
import com.facebook.thrift.legacy.server.testservices.BlockingPingService;
import com.facebook.thrift.model.ResponseAndStream;
import com.facebook.thrift.payload.RequestData;
import com.facebook.thrift.payload.ServerRequestPayload;
import com.facebook.thrift.payload.ServerResponsePayload;
import com.facebook.thrift.payload.Writer;
import com.facebook.thrift.protocol.ByteBufTBinaryProtocol;
import com.facebook.thrift.protocol.ByteBufTCompactProtocol;
import com.facebook.thrift.test.adapter.MaxBoundHolder;
import com.facebook.thrift.test.adapter.MaxBoundParent;
import com.facebook.thrift.test.adapter.SaturationService;
import com.facebook.thrift.test.adapter.SaturationServiceRpcServerHandler;
import io.airlift.units.Duration;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.ssl.ClientAuth;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.thrift.ProtocolId;
import org.apache.thrift.RequestRpcMetadata;
import org.apache.thrift.RpcKind;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class TestGeneratedServerHandler {
  PingService.Reactive client;

  @BeforeEach
  public void setup() {
    PingServiceRpcServerHandler serverHandler =
        new PingServiceRpcServerHandler(new BlockingPingService(), Collections.emptyList());
    LegacyServerTransportFactory transportFactory =
        new LegacyServerTransportFactory(
            new ThriftServerConfig().setClientAuth(ClientAuth.OPTIONAL).setSslEnabled(false));
    LegacyServerTransport serverTransport =
        transportFactory.createServerTransport(serverHandler).block();
    InetSocketAddress address = (InetSocketAddress) serverTransport.getAddress();

    RpcClientFactory rpcClientFactory =
        RpcClientFactory.builder()
            .setThriftClientConfig(
                new ThriftClientConfig()
                    .setDisableSSL(true)
                    .setRequestTimeout(Duration.succinctDuration(1, TimeUnit.DAYS)))
            .setDisableLoadBalancing(true)
            .setDisableReconnectingClient(true)
            .build();
    this.client =
        PingService.Reactive.clientBuilder()
            .setProtocolId(ProtocolId.BINARY)
            .build(rpcClientFactory, address);
  }

  @Test
  public void testPing() {
    StepVerifier.create(client.ping(new PingRequest.Builder().setRequest("ping").build()))
        .consumeNextWith(
            response -> MatcherAssert.assertThat(response.getResponse(), is("ping_pong_0")))
        .verifyComplete();
  }

  @Test
  public void testPingWithCustomException() {
    StepVerifier.create(client.pingException(new PingRequest.Builder().setRequest("ping").build()))
        .consumeErrorWith(
            error -> {
              MatcherAssert.assertThat(error, instanceOf(CustomException.class));
              MatcherAssert.assertThat(error.getMessage(), is("custom exception"));
            });
  }

  @Test
  public void testPingWithApplicationException() {
    StepVerifier.create(client.pingException(new PingRequest.Builder().setRequest("npe").build()))
        .consumeErrorWith(
            error -> {
              MatcherAssert.assertThat(error, instanceOf(TApplicationException.class));
              MatcherAssert.assertThat(error.getMessage(), is("npe from pingException"));
            });
  }

  private interface RequestArgs {
    void write(ByteBufTCompactProtocol protocol) throws Exception;
  }

  private static ServerRequestPayload requestPayload(String method, RequestArgs args)
      throws Exception {
    ByteBuf buf = Unpooled.buffer();
    ByteBufTCompactProtocol out = new ByteBufTCompactProtocol();
    out.wrap(buf);
    out.writeStructBegin(new TStruct(""));
    args.write(out);
    out.writeFieldStop();
    out.writeStructEnd();

    RequestRpcMetadata metadata =
        new RequestRpcMetadata.Builder()
            .setName(method)
            .setKind(RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE)
            .build();
    ByteBufTCompactProtocol in = new ByteBufTCompactProtocol();
    in.wrap(buf);
    return ServerRequestPayload.create(metadata, null, RequestData.of(buf, in));
  }

  private static void writePingRequest(ByteBufTCompactProtocol out) throws Exception {
    out.writeFieldBegin(new TField("", TType.STRUCT, (short) 1));
    new PingRequest.Builder().setRequest("hello").build().write0(out);
    out.writeFieldEnd();
  }

  private static void assertWriterSize(String name, Writer writer) throws Exception {
    int hint = writer.sizeHint();

    ByteBuf binaryBuf = Unpooled.buffer();
    int binaryWritten;

    try {
      ByteBufTBinaryProtocol binary = new ByteBufTBinaryProtocol();
      binary.wrap(binaryBuf);
      writer.write(binary);
      binaryWritten = binaryBuf.readableBytes();
    } finally {
      binaryBuf.release();
    }

    ByteBuf compactBuf = Unpooled.buffer();
    int compactWritten;

    try {
      ByteBufTCompactProtocol compact = new ByteBufTCompactProtocol();
      compact.wrap(compactBuf);
      writer.write(compact);
      compactWritten = compactBuf.readableBytes();
    } finally {
      compactBuf.release();
    }

    assertTrue(
        hint >= binaryWritten,
        () -> name + " binary undercount: hint=" + hint + " written=" + binaryWritten);
    assertTrue(
        hint >= compactWritten,
        () -> name + " compact undercount: hint=" + hint + " written=" + compactWritten);
  }

  @Test
  public void testResponseWriterSizeHint() throws Exception {
    PingService.Reactive delegate = Mockito.mock(PingService.Reactive.class);
    Mockito.when(delegate.ping(Mockito.any(PingRequest.class)))
        .thenReturn(Mono.just(new PingResponse.Builder().setResponse("pong").build()));
    PingServiceRpcServerHandler handler =
        new PingServiceRpcServerHandler(delegate, Collections.emptyList());

    ServerResponsePayload payload =
        handler
            .singleRequestSingleResponse(requestPayload("ping", p -> writePingRequest(p)))
            .block();

    assertWriterSize("ping-response", payload.getDataWriter());
  }

  @Test
  public void testSaturatedResponseWriterSizeHint() throws Exception {
    MaxBoundHolder holder = new MaxBoundHolder.Builder().setFirst("a").setSecond("b").build();
    MaxBoundParent response =
        new MaxBoundParent.Builder().setLeft(holder).setRight(holder).setTrailing("tail").build();
    SaturationService.Reactive delegate = Mockito.mock(SaturationService.Reactive.class);
    Mockito.when(delegate.saturate()).thenReturn(Mono.just(response));
    SaturationServiceRpcServerHandler handler =
        new SaturationServiceRpcServerHandler(delegate, Collections.emptyList());

    ServerResponsePayload payload =
        handler.singleRequestSingleResponse(requestPayload("saturate", p -> {})).block();

    assertEquals(Integer.MAX_VALUE, payload.getDataWriter().sizeHint());
  }

  @Test
  public void testExceptionWriterSizeHint() throws Exception {
    PingService.Reactive delegate = Mockito.mock(PingService.Reactive.class);
    Mockito.when(delegate.pingException(Mockito.any(PingRequest.class)))
        .thenReturn(Mono.error(new CustomException.Builder().setMessage("kaput").build()));
    PingServiceRpcServerHandler handler =
        new PingServiceRpcServerHandler(delegate, Collections.emptyList());

    ServerResponsePayload payload =
        handler
            .singleRequestSingleResponse(requestPayload("pingException", p -> writePingRequest(p)))
            .block();

    assertWriterSize("ping-exception", payload.getDataWriter());
  }

  @Test
  public void testVoidAndBinaryResponseWriters() throws Exception {
    PingService.Reactive delegate = Mockito.mock(PingService.Reactive.class);
    Mockito.when(delegate.pingVoid(Mockito.any(PingRequest.class))).thenReturn(Mono.empty());
    Mockito.when(delegate.pingBinary(Mockito.any(PingRequest.class)))
        .thenReturn(Mono.just(new byte[] {1, 2, 3}));
    PingServiceRpcServerHandler handler =
        new PingServiceRpcServerHandler(delegate, Collections.emptyList());

    ServerResponsePayload voidPayload =
        handler
            .singleRequestSingleResponse(requestPayload("pingVoid", p -> writePingRequest(p)))
            .block();

    assertWriterSize("ping-void", voidPayload.getDataWriter());

    ServerResponsePayload binaryPayload =
        handler
            .singleRequestSingleResponse(requestPayload("pingBinary", p -> writePingRequest(p)))
            .block();

    assertWriterSize("ping-binary", binaryPayload.getDataWriter());
  }

  @Test
  public void testStreamWriterSizeHint() throws Exception {
    PingService.Reactive delegate = Mockito.mock(PingService.Reactive.class);
    Mockito.when(delegate.streamOfPings(Mockito.any(PingRequest.class), Mockito.anyInt()))
        .thenReturn(
            Flux.just(new PingResponse.Builder().setResponse("s1 héllo 日本語 \uD83D\uDE00").build()));
    PingServiceRpcServerHandler handler =
        new PingServiceRpcServerHandler(delegate, Collections.emptyList());

    ServerRequestPayload request = streamRequestPayload("streamOfPings");

    // The first item is the empty first-response payload; the element writer is next.
    ServerResponsePayload first =
        handler.singleRequestStreamingResponse(request).skip(1).blockFirst();

    assertWriterSize("stream-element", first.getDataWriter());
  }

  @Test
  public void testFirstResponseWriterSizeHint() throws Exception {
    ExtendedPing.Reactive delegate = Mockito.mock(ExtendedPing.Reactive.class);
    Mockito.when(delegate.streamWithFirstResponse(Mockito.any(PingRequest.class), Mockito.anyInt()))
        .thenReturn(
            Mono.just(
                ResponseAndStream.of(
                    new PingResponse.Builder().setResponse("first héllo").build(),
                    Flux.just(new PingResponse.Builder().setResponse("s1").build()))));
    ExtendedPingRpcServerHandler handler =
        new ExtendedPingRpcServerHandler(delegate, Collections.emptyList());

    ServerRequestPayload request = streamRequestPayload("streamWithFirstResponse");

    ServerResponsePayload first = handler.singleRequestStreamingResponse(request).blockFirst();

    assertWriterSize("stream-first-response", first.getDataWriter());
  }

  private static ServerRequestPayload streamRequestPayload(String name) throws Exception {
    ByteBuf buf = Unpooled.buffer();
    ByteBufTCompactProtocol out = new ByteBufTCompactProtocol();
    out.wrap(buf);
    out.writeStructBegin(new TStruct(""));
    writePingRequest(out);

    out.writeFieldBegin(new TField("", TType.I32, (short) 2));
    out.writeI32(1);
    out.writeFieldEnd();
    out.writeFieldStop();
    out.writeStructEnd();

    RequestRpcMetadata metadata =
        new RequestRpcMetadata.Builder()
            .setName(name)
            .setKind(RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE)
            .build();
    ByteBufTCompactProtocol in = new ByteBufTCompactProtocol();
    in.wrap(buf);

    return ServerRequestPayload.create(metadata, null, RequestData.of(buf, in));
  }
}
