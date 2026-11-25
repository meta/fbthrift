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

package com.facebook.thrift.util;

import static com.facebook.thrift.util.NettyUtil.TransportType.EPOLL;
import static com.facebook.thrift.util.NettyUtil.TransportType.IO_URING;
import static com.facebook.thrift.util.NettyUtil.TransportType.NIO;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.facebook.thrift.client.ThriftClientConfig;
import com.facebook.thrift.compression.CompressionManager;
import com.facebook.thrift.example.ping.CustomException;
import com.facebook.thrift.example.ping.PingResponse;
import com.facebook.thrift.model.StreamResponse;
import com.facebook.thrift.payload.ClientRequestPayload;
import com.facebook.thrift.payload.ClientResponsePayload;
import com.facebook.thrift.payload.Reader;
import com.facebook.thrift.payload.Writer;
import com.facebook.thrift.protocol.ByteBufTProtocol;
import com.facebook.thrift.protocol.TProtocolType;
import com.facebook.thrift.util.resources.RpcResources;
import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueueDomainSocketChannel;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.channel.uring.IoUringDomainSocketChannel;
import io.netty.channel.uring.IoUringSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.util.internal.PlatformDependent;
import io.rsocket.Payload;
import io.rsocket.util.ByteBufPayload;
import io.rsocket.util.DefaultPayload;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.thrift.CompressionAlgorithm;
import org.apache.thrift.PayloadAppUnknownExceptionMetdata;
import org.apache.thrift.PayloadDeclaredExceptionMetadata;
import org.apache.thrift.PayloadExceptionMetadata;
import org.apache.thrift.PayloadExceptionMetadataBase;
import org.apache.thrift.PayloadMetadata;
import org.apache.thrift.PayloadProxyExceptionMetadata;
import org.apache.thrift.ProtocolId;
import org.apache.thrift.RequestRpcMetadata;
import org.apache.thrift.ResponseRpcMetadata;
import org.apache.thrift.StreamPayloadMetadata;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TTransportException;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import reactor.core.Exceptions;

public class RpcClientUtilsTest {

  private final Reader<PingResponse> reader =
      (oprot) -> {
        try {
          return PingResponse.read0(oprot);
        } catch (Throwable ex) {
          throw Exceptions.propagate(ex);
        }
      };

  private final Reader<CustomException> exReader =
      (oprot) -> {
        try {
          return CustomException.read0(oprot);
        } catch (Throwable ex) {
          throw Exceptions.propagate(ex);
        }
      };

  private final Map<Short, Reader> exceptionMap = ImmutableMap.of((short) 1, exReader);
  private final ClientRequestPayload<PingResponse> requestPayload =
      ClientRequestPayload.create(
          "test",
          null,
          reader,
          reader,
          exceptionMap,
          exceptionMap,
          new RequestRpcMetadata.Builder().setProtocol(ProtocolId.BINARY).build(),
          Collections.emptyMap());

  @Test
  public void testGetEventLoopGroup() {
    assertNotNull(RpcResources.getEventLoopGroup(), "EventLoopGroup should not be null");
  }

  @Test
  public void testDomainSocketChannelLinuxJava9() {
    assumeFalse(isMacos(), "Not Macos");
    assumeTrue(PlatformDependent.javaVersion() >= 9, "Java 9+");

    try (MockedStatic<NettyUtil> mockNettyUtil = mockStatic(NettyUtil.class)) {
      mockNettyUtil.when(NettyUtil::getTransportType).thenReturn(IO_URING);

      SocketAddress socketAddress = new DomainSocketAddress("/foo");
      Class<?> channelClass = RpcClientUtils.getChannelClass(socketAddress);
      assertEquals(IoUringDomainSocketChannel.class, channelClass);
    }
  }

  @Test
  public void testDomainSocketChannelLinux() {
    assumeFalse(isMacos(), "Not Macos");

    try (MockedStatic<NettyUtil> mockNettyUtil = mockStatic(NettyUtil.class)) {
      mockNettyUtil.when(NettyUtil::getTransportType).thenReturn(EPOLL);

      SocketAddress socketAddress = new DomainSocketAddress("/foo");
      Class<?> channelClass = RpcClientUtils.getChannelClass(socketAddress);
      assertEquals(EpollDomainSocketChannel.class, channelClass);
    }
  }

  @Test
  public void testEpollSocketChannel() {
    assumeFalse(isMacos());

    try (MockedStatic<NettyUtil> mockNettyUtil = mockStatic(NettyUtil.class)) {
      mockNettyUtil.when(NettyUtil::getTransportType).thenReturn(EPOLL);

      SocketAddress socketAddress = new InetSocketAddress(0);
      Class<?> channelClass = RpcClientUtils.getChannelClass(socketAddress);
      assertEquals(EpollSocketChannel.class, channelClass);
    }
  }

  @Test
  public void testIoUringSocketChannel() {
    assumeFalse(isMacos());
    assumeTrue(PlatformDependent.javaVersion() >= 9, "Java 9+");

    try (MockedStatic<NettyUtil> mockNettyUtil = mockStatic(NettyUtil.class)) {
      mockNettyUtil.when(NettyUtil::getTransportType).thenReturn(IO_URING);

      SocketAddress socketAddress = new InetSocketAddress(0);
      Class<?> channelClass = RpcClientUtils.getChannelClass(socketAddress);
      assertEquals(IoUringSocketChannel.class, channelClass);
    }
  }

  @Test
  public void testDomainSocketChannelMacos() {
    if (isMacos()) {
      SocketAddress socketAddress = new DomainSocketAddress("/foo");
      Class<?> channelClass = RpcClientUtils.getChannelClass(socketAddress);
      assertEquals(KQueueDomainSocketChannel.class, channelClass);
    }
  }

  @Test
  public void testKQueueSocketChannel() {
    if (isMacos()) {
      SocketAddress socketAddress = new InetSocketAddress(0);
      Class<?> channelClass = RpcClientUtils.getChannelClass(socketAddress);
      assertEquals(KQueueSocketChannel.class, channelClass);
    }
  }

  @Test
  public void testNioSocketChannel() {
    try (MockedStatic<NettyUtil> mockNettyUtil = mockStatic(NettyUtil.class)) {
      mockNettyUtil.when(NettyUtil::getTransportType).thenReturn(NIO);

      SocketAddress socketAddress = new InetSocketAddress(0);
      Class<?> channelClass = RpcClientUtils.getChannelClass(socketAddress);
      assertEquals(NioSocketChannel.class, channelClass);
    }
  }

  @Test
  public void testInvalidSocketGroupCombination() {
    assumeTrue(PlatformDependent.javaVersion() < 16);

    try (MockedStatic<NettyUtil> mockNettyUtil = mockStatic(NettyUtil.class)) {
      mockNettyUtil.when(NettyUtil::getTransportType).thenReturn(NIO);

      SocketAddress socketAddress = new DomainSocketAddress("/foo");
      assertThrows(
          UnsupportedOperationException.class, () -> RpcClientUtils.getChannelClass(socketAddress));
    }
  }

  @Test
  public void tesGetSslContextDomain() {
    SslContext context =
        RpcClientUtils.getSslContext(new ThriftClientConfig(), new DomainSocketAddress("/foo"));
    assertNull(context);
  }

  @Test
  public void tesDisabledSsl() {
    SslContext context =
        RpcClientUtils.getSslContext(
            new ThriftClientConfig().setDisableSSL(true), new InetSocketAddress(0));
    assertNull(context);
  }

  @Test
  public void tesSslContext() {
    SslContext context =
        RpcClientUtils.getSslContext(new ThriftClientConfig(), new InetSocketAddress(0));
    assertTrue(context.isClient());
  }

  @Test
  public void tesJdkSslContext() {
    SslContext context =
        RpcClientUtils.getSslContext(
            new ThriftClientConfig().setEnableJdkSsl(true), new InetSocketAddress(0));
    assertTrue(context.isClient());
  }

  @Test
  public void tesDecodeStreamPayload() {
    ByteBufTProtocol protocol = TProtocolType.TBinary.apply(Unpooled.buffer());
    protocol.writeStructBegin(new TStruct());
    protocol.writeFieldBegin(new TField("pingResponse", TType.STRUCT, (short) 0));
    PingResponse pingResponse = new PingResponse.Builder().setResponse("foo").build();
    pingResponse.write0(protocol);
    protocol.writeFieldEnd();
    protocol.writeStructEnd();

    StreamResponse<PingResponse, PingResponse> actual =
        (StreamResponse)
            RpcClientUtils.decodeStreamPayload(
                    requestPayload,
                    DefaultPayload.create(protocol.getByteBuf()),
                    StreamPayloadMetadata.defaultInstance(),
                    ResponseRpcMetadata.defaultInstance(),
                    false)
                .getData();
    assertEquals("foo", actual.getData().getResponse());
  }

  @Test
  public void tesDecodeFirstStreamPayload() {
    ByteBufTProtocol protocol = TProtocolType.TBinary.apply(Unpooled.buffer());
    protocol.writeStructBegin(new TStruct());
    protocol.writeFieldBegin(new TField("pingResponse", TType.STRUCT, (short) 0));
    PingResponse pingResponse = new PingResponse.Builder().setResponse("foo").build();
    pingResponse.write0(protocol);
    protocol.writeFieldEnd();
    protocol.writeStructEnd();

    StreamResponse<PingResponse, PingResponse> actual =
        (StreamResponse)
            RpcClientUtils.decodeStreamPayload(
                    requestPayload,
                    DefaultPayload.create(protocol.getByteBuf()),
                    StreamPayloadMetadata.defaultInstance(),
                    ResponseRpcMetadata.defaultInstance(),
                    true)
                .getData();
    assertEquals("foo", actual.getFirstResponse().getResponse());
  }

  @Test
  public void testVoidFirstResponse() {
    ByteBufTProtocol protocol = TProtocolType.TBinary.apply(Unpooled.buffer());
    protocol.writeStructBegin(new TStruct());
    protocol.writeFieldBegin(new TField("pingResponse", TType.STOP, (short) 0));
    PingResponse pingResponse = new PingResponse.Builder().setResponse("foo").build();
    pingResponse.write0(protocol);
    protocol.writeFieldEnd();
    protocol.writeStructEnd();

    StreamResponse actual =
        (StreamResponse)
            RpcClientUtils.decodeStreamPayload(
                    requestPayload,
                    DefaultPayload.create(protocol.getByteBuf()),
                    StreamPayloadMetadata.defaultInstance(),
                    ResponseRpcMetadata.defaultInstance(),
                    true)
                .getData();
    assertTrue(actual.isSetFirstResponse());
  }

  @Test
  public void testExceptionResponse() {

    ByteBufTProtocol protocol = TProtocolType.TBinary.apply(Unpooled.buffer());
    protocol.writeStructBegin(new TStruct());
    protocol.writeFieldBegin(new TField("ex", TType.STRUCT, (short) 1));
    CustomException exception = new CustomException.Builder().setMessage("foo").build();
    exception.write0(protocol);
    protocol.writeFieldEnd();
    protocol.writeStructEnd();

    ClientResponsePayload actual =
        RpcClientUtils.decodeStreamPayload(
            requestPayload,
            DefaultPayload.create(protocol.getByteBuf()),
            StreamPayloadMetadata.defaultInstance(),
            ResponseRpcMetadata.defaultInstance(),
            false);
    assertNotNull(actual.getException());
  }

  @Test
  public void testUndefinedExceptioStringResponse() {
    ClientRequestPayload<PingResponse> requestPayload =
        ClientRequestPayload.create(
            "test",
            null,
            reader,
            reader,
            Collections.emptyMap(),
            Collections.emptyMap(),
            new RequestRpcMetadata.Builder().setProtocol(ProtocolId.BINARY).build(),
            Collections.emptyMap());

    ByteBufTProtocol protocol = TProtocolType.TBinary.apply(Unpooled.buffer());
    protocol.writeStructBegin(new TStruct());
    protocol.writeFieldBegin(new TField("ex", TType.STRING, (short) 1));
    protocol.writeString("internal_error");
    protocol.writeFieldEnd();
    protocol.writeStructEnd();

    ClientResponsePayload actual =
        RpcClientUtils.decodeStreamPayload(
            requestPayload,
            DefaultPayload.create(protocol.getByteBuf()),
            StreamPayloadMetadata.defaultInstance(),
            ResponseRpcMetadata.defaultInstance(),
            false);
    assertEquals("internal_error", actual.getException().getMessage());
  }

  @Test
  public void testUndefinedExceptionResponse() {
    ClientRequestPayload<PingResponse> requestPayload =
        ClientRequestPayload.create(
            "test",
            null,
            reader,
            reader,
            Collections.emptyMap(),
            Collections.emptyMap(),
            new RequestRpcMetadata.Builder().setProtocol(ProtocolId.BINARY).build(),
            Collections.emptyMap());

    ClientResponsePayload actual =
        RpcClientUtils.decodeStreamPayload(
            requestPayload,
            DefaultPayload.create("Undefined exception"),
            StreamPayloadMetadata.defaultInstance(),
            ResponseRpcMetadata.defaultInstance(),
            false);
    assertEquals("Undefined exception", actual.getException().getMessage());
  }

  @Test
  public void testDecodeFirstStreamPayloadCompressed() throws Exception {
    ByteBufAllocator backing = RpcResources.getByteBufAllocator();
    List<ByteBuf> allocated = new ArrayList<>();
    ByteBufAllocator alloc = mock(ByteBufAllocator.class);
    when(alloc.directBuffer(anyInt()))
        .thenAnswer(
            invocation -> {
              ByteBuf buffer = backing.directBuffer(invocation.getArgument(0));
              allocated.add(buffer);

              return buffer;
            });

    ByteBuf data =
        binaryUnionField(
            backing,
            new TField("pingResponse", TType.STRUCT, (short) 0),
            out -> new PingResponse.Builder().setResponse("foo").build().write0(out));

    Payload payload = compressedPayload(backing, data);
    ResponseRpcMetadata metadata =
        new ResponseRpcMetadata.Builder().setCompression(CompressionAlgorithm.ZSTD).build();

    StreamResponse<PingResponse, PingResponse> actual;

    try {
      actual =
          (StreamResponse)
              RpcClientUtils.decodeStreamPayload(
                      alloc,
                      requestPayload,
                      payload,
                      StreamPayloadMetadata.defaultInstance(),
                      metadata,
                      true)
                  .getData();
    } finally {
      payload.release();
    }

    assertEquals("foo", actual.getFirstResponse().getResponse());
    assertEquals(1, allocated.size());

    for (ByteBuf buffer : allocated) {
      assertEquals(0, buffer.refCnt());
    }
  }

  @Test
  public void testDecodeStreamPayloadCompressed() throws Exception {
    ByteBufAllocator alloc = RpcResources.getByteBufAllocator();

    ByteBuf data =
        binaryUnionField(
            alloc,
            new TField("pingResponse", TType.STRUCT, (short) 0),
            out -> new PingResponse.Builder().setResponse("foo").build().write0(out));

    Payload payload = compressedPayload(alloc, data);
    StreamPayloadMetadata metadata =
        new StreamPayloadMetadata.Builder().setCompression(CompressionAlgorithm.ZSTD).build();

    ClientResponsePayload response;

    try {
      response =
          RpcClientUtils.decodeStreamPayload(
              alloc,
              requestPayload,
              payload,
              metadata,
              ResponseRpcMetadata.defaultInstance(),
              false);
    } finally {
      payload.release();
    }

    StreamResponse<PingResponse, PingResponse> actual = (StreamResponse) response.getData();

    assertEquals("foo", actual.getData().getResponse());
  }

  @Test
  public void testCompressedStreamExceptionResponse() throws Exception {
    ByteBufAllocator alloc = RpcResources.getByteBufAllocator();

    ByteBuf data =
        binaryUnionField(
            alloc,
            new TField("ex", TType.STRUCT, (short) 1),
            out -> new CustomException.Builder().setMessage("foo").build().write0(out));

    Payload payload = compressedPayload(alloc, data);
    StreamPayloadMetadata metadata =
        new StreamPayloadMetadata.Builder().setCompression(CompressionAlgorithm.ZSTD).build();

    ClientResponsePayload actual;

    try {
      actual =
          RpcClientUtils.decodeStreamPayload(
              alloc,
              requestPayload,
              payload,
              metadata,
              ResponseRpcMetadata.defaultInstance(),
              false);
    } finally {
      payload.release();
    }

    assertInstanceOf(CustomException.class, actual.getException());
    assertEquals("foo", actual.getException().getMessage());
  }

  @Test
  public void testCompressedUndefinedExceptionResponse() {
    ClientRequestPayload<PingResponse> requestPayload =
        ClientRequestPayload.create(
            "test",
            null,
            reader,
            reader,
            Collections.emptyMap(),
            Collections.emptyMap(),
            new RequestRpcMetadata.Builder().setProtocol(ProtocolId.BINARY).build(),
            Collections.emptyMap());

    ByteBufAllocator alloc = RpcResources.getByteBufAllocator();
    ByteBuf raw = Unpooled.wrappedBuffer("Undefined exception".getBytes(StandardCharsets.UTF_8));

    Payload payload = compressedPayload(alloc, raw);
    StreamPayloadMetadata metadata =
        new StreamPayloadMetadata.Builder().setCompression(CompressionAlgorithm.ZSTD).build();

    ClientResponsePayload<PingResponse> actual;

    try {
      actual =
          RpcClientUtils.decodeStreamPayload(
              alloc,
              requestPayload,
              payload,
              metadata,
              ResponseRpcMetadata.defaultInstance(),
              false);
    } finally {
      payload.release();
    }

    assertEquals("Undefined exception", actual.getException().getMessage());
  }

  @Test
  public void tesDecodeSinglePayload() {
    ByteBufTProtocol protocol = TProtocolType.TBinary.apply(Unpooled.buffer());
    protocol.writeStructBegin(new TStruct());
    protocol.writeFieldBegin(new TField("pingResponse", TType.STRUCT, (short) 0));
    PingResponse pingResponse = new PingResponse.Builder().setResponse("foo").build();
    pingResponse.write0(protocol);
    protocol.writeFieldEnd();
    protocol.writeStructEnd();

    PingResponse actual =
        RpcClientUtils.decodeRSocketPayload(
                reader, Collections.emptyMap(), protocol, new ResponseRpcMetadata.Builder().build())
            .getData();
    assertEquals("foo", actual.getResponse());
  }

  @Test
  public void testHasUndeclaredException() {
    ResponseRpcMetadata rpcMetadata =
        new ResponseRpcMetadata.Builder()
            .setPayloadMetadata(
                PayloadMetadata.fromExceptionMetadata(
                    new PayloadExceptionMetadataBase.Builder()
                        .setMetadata(
                            PayloadExceptionMetadata.fromAppUnknownException(
                                PayloadAppUnknownExceptionMetdata.defaultInstance()))
                        .build()))
            .build();
    Optional<? extends TException> undeclaredException =
        RpcClientUtils.getUndeclaredException(rpcMetadata);
    assertTrue(undeclaredException.isPresent());
    assertFalse(undeclaredException.get() instanceof TTransportException);
  }

  @Test
  public void testUndeclaredExceptionStackTrace1() {
    ResponseRpcMetadata rpcMetadata =
        new ResponseRpcMetadata.Builder()
            .setPayloadMetadata(
                PayloadMetadata.fromExceptionMetadata(
                    new PayloadExceptionMetadataBase.Builder()
                        .setWhatUtf8("Foo")
                        .setMetadata(
                            PayloadExceptionMetadata.fromAppUnknownException(
                                PayloadAppUnknownExceptionMetdata.defaultInstance()))
                        .build()))
            .build();
    TException undeclaredException = RpcClientUtils.getUndeclaredException(rpcMetadata).get();
    assertNotNull(undeclaredException);
    assertTrue(undeclaredException instanceof TApplicationException);
    assertEquals("Foo", undeclaredException.getMessage());
    assertEquals(
        "org.apache.thrift.TApplicationException",
        undeclaredException.getStackTrace()[0].getClassName());
  }

  @Test
  public void testUndeclaredExceptionStackTrace2() {
    ResponseRpcMetadata rpcMetadata =
        new ResponseRpcMetadata.Builder()
            .setPayloadMetadata(
                PayloadMetadata.fromExceptionMetadata(
                    new PayloadExceptionMetadataBase.Builder()
                        .setWhatUtf8("Foo")
                        .setMetadata(
                            PayloadExceptionMetadata.fromDEPRECATEDProxyException(
                                PayloadProxyExceptionMetadata.defaultInstance()))
                        .build()))
            .build();
    TException undeclaredException = RpcClientUtils.getUndeclaredException(rpcMetadata).get();
    assertNotNull(undeclaredException);
    assertTrue(undeclaredException instanceof TTransportException);
    assertEquals("Foo", undeclaredException.getMessage());
    assertEquals(
        "com.facebook.thrift.util.RpcClientUtils",
        undeclaredException.getStackTrace()[0].getClassName());
  }

  @Test
  public void testHasDeclaredException() {
    ResponseRpcMetadata rpcMetadata =
        new ResponseRpcMetadata.Builder()
            .setPayloadMetadata(
                PayloadMetadata.fromExceptionMetadata(
                    new PayloadExceptionMetadataBase.Builder()
                        .setMetadata(
                            PayloadExceptionMetadata.fromDeclaredException(
                                PayloadDeclaredExceptionMetadata.defaultInstance()))
                        .build()))
            .build();
    Optional<? extends TException> undeclaredException =
        RpcClientUtils.getUndeclaredException(rpcMetadata);
    assertFalse(undeclaredException.isPresent());
  }

  private static boolean isMacos() {
    return System.getProperty("os.name").startsWith("Mac");
  }

  private static Payload compressedPayload(ByteBufAllocator alloc, ByteBuf data) {
    ByteBuf compressed = CompressionManager.compress(CompressionAlgorithm.ZSTD, alloc, data);

    return ByteBufPayload.create(compressed, Unpooled.EMPTY_BUFFER);
  }

  private static ByteBuf binaryUnionField(ByteBufAllocator alloc, TField field, Writer value) {
    ByteBuf data = alloc.buffer();

    try {
      ByteBufTProtocol out = TProtocolType.TBinary.apply(data);
      out.writeStructBegin(new TStruct());
      out.writeFieldBegin(field);
      value.write(out);
      out.writeFieldEnd();
      out.writeStructEnd();

      return data;
    } catch (Throwable t) {
      data.release();

      throw Exceptions.propagate(t);
    }
  }
}
