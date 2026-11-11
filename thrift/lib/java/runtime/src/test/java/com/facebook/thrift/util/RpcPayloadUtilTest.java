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

import static com.facebook.thrift.util.RpcPayloadUtil.convertOtherData;
import static com.facebook.thrift.util.RpcPayloadUtil.declaredExceptionResponseMetadata;
import static com.facebook.thrift.util.RpcPayloadUtil.declaredExceptionWriter;
import static com.facebook.thrift.util.RpcPayloadUtil.undeclaredExceptionStreamMetadata;
import static com.facebook.thrift.util.RpcPayloadUtil.undeclaredExceptionWriter;
import static com.facebook.thrift.util.RpcPayloadUtil.withPreWriteException;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.facebook.swift.service.ContextChain;
import com.facebook.swift.service.ThriftEventHandler;
import com.facebook.thrift.payload.ServerRequestPayload;
import com.facebook.thrift.payload.ServerResponsePayload;
import com.facebook.thrift.protocol.TProtocolType;
import com.facebook.thrift.server.OverloadedException;
import com.facebook.thrift.test.rocket.TestServerException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.thrift.ErrorBlame;
import org.apache.thrift.ErrorKind;
import org.apache.thrift.PayloadExceptionMetadataBase;
import org.apache.thrift.RequestRpcMetadata;
import org.apache.thrift.ResponseRpcMetadata;
import org.apache.thrift.RpcKind;
import org.apache.thrift.StreamPayloadMetadata;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TType;
import org.junit.jupiter.api.Test;

public class RpcPayloadUtilTest {

  @Test
  public void convertOtherDataTreatsNullAsEmptyAndEncodesValuesAsUtf8() {
    assertTrue(convertOtherData(null).isEmpty());

    Map<String, String> otherData = new HashMap<>();
    otherData.put("header", "caf\u00e9 \u2713");
    Map<String, byte[]> converted = convertOtherData(otherData);

    assertEquals(Collections.singleton("header"), converted.keySet());
    assertArrayEquals("caf\u00e9 \u2713".getBytes(StandardCharsets.UTF_8), converted.get("header"));
  }

  @Test
  public void exceptionMetadataKeepsScopeOtherDataAndClassification() {
    ServerRequestPayload request = requestWithOtherData("header", "value");
    TestServerException declared = new TestServerException("boom");

    // First-response metadata passes the string map through; the declared exception keeps its IDL
    // classification (`transient server` in rocket_protocol.thrift).
    ResponseRpcMetadata response = declaredExceptionResponseMetadata(request, declared);
    assertEquals("value", response.getOtherMetadata().get("header"));
    PayloadExceptionMetadataBase declaredBase =
        response.getPayloadMetadata().getExceptionMetadata();
    assertEquals(TestServerException.class.getName(), declaredBase.getNameUtf8());
    assertEquals(declared.getMessage(), declaredBase.getWhatUtf8());
    assertTrue(declaredBase.getMetadata().isSetDeclaredException());
    assertEquals(
        ErrorBlame.SERVER,
        declaredBase.getMetadata().getDeclaredException().getErrorClassification().getBlame());
    assertEquals(
        ErrorKind.TRANSIENT,
        declaredBase.getMetadata().getDeclaredException().getErrorClassification().getKind());

    // Stream-scope metadata converts the map to bytes; undeclared is appUnknown blamed on the
    // server.
    StreamPayloadMetadata stream = undeclaredExceptionStreamMetadata(request, "Name", "what");
    assertArrayEquals(
        "value".getBytes(StandardCharsets.UTF_8), stream.getOtherMetadata().get("header"));
    PayloadExceptionMetadataBase undeclaredBase =
        stream.getPayloadMetadata().getExceptionMetadata();
    assertEquals("Name", undeclaredBase.getNameUtf8());
    assertEquals("what", undeclaredBase.getWhatUtf8());
    assertTrue(undeclaredBase.getMetadata().isSetAppUnknownException());
    assertEquals(
        ErrorBlame.SERVER,
        undeclaredBase.getMetadata().getAppUnknownException().getErrorClassification().getBlame());
  }

  @Test
  public void exceptionWritersEncodeTheBodyAndFireHooksInOrder() throws TException {
    List<String> hooks = new ArrayList<>();
    ContextChain chain = recordingChain(hooks);

    TestServerException declared = new TestServerException("boom");
    ByteBuf declaredBytes = Unpooled.buffer();
    try {
      withPreWriteException(declared, chain, declaredExceptionWriter(declared, chain, 7))
          .write(TProtocolType.TCompact.apply(declaredBytes));
      TProtocol in = TProtocolType.TCompact.apply(declaredBytes);
      in.readStructBegin();
      TField field = in.readFieldBegin();
      assertEquals((short) 7, field.id);
      assertEquals(TType.STRUCT, field.type);
      assertEquals("boom", TestServerException.read0(in).getMsg());
      in.readFieldEnd();
      assertEquals(TType.STOP, in.readFieldBegin().type);
    } finally {
      declaredBytes.release();
    }
    assertEquals(Arrays.asList("pre", "declared", "post"), hooks);

    hooks.clear();
    TApplicationException undeclared =
        new TApplicationException(TApplicationException.INTERNAL_ERROR, "bad");
    ByteBuf undeclaredBytes = Unpooled.buffer();
    try {
      undeclaredExceptionWriter(undeclared, chain)
          .write(TProtocolType.TCompact.apply(undeclaredBytes));
      TApplicationException read =
          TApplicationException.read(TProtocolType.TCompact.apply(undeclaredBytes));
      assertEquals(TApplicationException.INTERNAL_ERROR, read.getType());
      assertEquals("bad", read.getMessage());
    } finally {
      undeclaredBytes.release();
    }
    // The caller owns preWriteException for the undeclared writer.
    assertEquals(Arrays.asList("undeclared", "post"), hooks);
  }

  @Test
  public void fromTApplicationExceptionKeepsActualAndMetadata() throws Exception {
    ServerResponsePayload response =
        RpcPayloadUtil.fromTApplicationException(
            new TApplicationException(TApplicationException.LOADSHEDDING, "busy"),
            requestMetadata(Collections.singletonMap("k", "v")),
            null);

    assertEquals(TApplicationException.LOADSHEDDING, readBodyType(response));
    assertTrue(response.isTApplicationException());
    assertNotNull(response.getApplicationException());
    assertEquals(
        Collections.singletonMap("k", "v"), response.getResponseRpcMetadata().getOtherMetadata());
  }

  @Test
  public void fromTApplicationExceptionKeepsTypedActual() throws Exception {
    OverloadedException overload = OverloadedException.host("shed");
    ServerResponsePayload response =
        RpcPayloadUtil.fromTApplicationException(
            overload, requestMetadata(Collections.<String, String>emptyMap()), null);

    assertEquals(TApplicationException.LOADSHEDDING, readBodyType(response));
    assertTrue(response.isTApplicationException());
    assertSame(overload, response.getApplicationException());
  }

  @Test
  public void fromTApplicationExceptionKeepsInboundMetadataExactly() {
    // The shared layer never classifies: even an inbound "ex" passes through unchanged, so the
    // Header encoder is the only place an overload code is ever stamped.
    Map<String, String> inbound = new HashMap<>();
    inbound.put("ex", "9");
    inbound.put("uex", "EvilException");
    inbound.put("uexw", "evil");
    inbound.put("anyex", "evil");
    inbound.put("anyext", "evil");
    inbound.put("normal", "yes");

    ServerResponsePayload response =
        RpcPayloadUtil.fromTApplicationException(
            new TApplicationException(TApplicationException.LOADSHEDDING, "busy"),
            requestMetadata(inbound),
            null);

    assertEquals(inbound, response.getResponseRpcMetadata().getOtherMetadata());
  }

  @Test
  public void ordinaryErrorsKeepMetadataAndBodyType() throws Exception {
    Map<String, String> inbound = new HashMap<>();
    inbound.put("ex", "9");
    inbound.put("uex", "SomeException");
    inbound.put("k", "v");

    ServerResponsePayload unknown =
        RpcPayloadUtil.fromTApplicationException(
            new TApplicationException(TApplicationException.UNKNOWN_METHOD, "no such method"),
            requestMetadata(inbound),
            null);
    assertEquals(inbound, unknown.getResponseRpcMetadata().getOtherMetadata());
    assertEquals(TApplicationException.UNKNOWN_METHOD, readBodyType(unknown));

    ServerResponsePayload internal =
        RpcPayloadUtil.fromTApplicationException(
            new TApplicationException(TApplicationException.INTERNAL_ERROR, "boom"),
            requestMetadata(Collections.<String, String>emptyMap()),
            null);
    assertTrue(internal.getResponseRpcMetadata().getOtherMetadata().isEmpty());
    assertEquals(TApplicationException.INTERNAL_ERROR, readBodyType(internal));
  }

  @Test
  public void tapplicationExceptionWriterFiresHooksOnce() throws Exception {
    List<String> hooks = new ArrayList<>();
    ContextChain chain = recordingChain(hooks);

    ServerResponsePayload response =
        RpcPayloadUtil.fromTApplicationException(
            new TApplicationException(TApplicationException.LOADSHEDDING, "busy"),
            requestMetadata(Collections.<String, String>emptyMap()),
            chain);
    invokeWriter(response);

    assertEquals(Arrays.asList("pre", "undeclared", "post"), hooks);
    assertFalse(response.getResponseRpcMetadata().getOtherMetadata().containsKey("ex"));
  }

  @Test
  public void wrappedErrorsKeepOwnTypeRegardlessOfCause() throws Exception {
    // A generated INTERNAL_ERROR wrapper keeps its own type even when its direct cause is a
    // LOADSHEDDING rejection: no cause-chain recovery reinterprets it as overload.
    TApplicationException shed =
        new TApplicationException(TApplicationException.LOADSHEDDING, "async-shed");
    TApplicationException wrapped =
        new TApplicationException(TApplicationException.INTERNAL_ERROR, "Internal error");
    wrapped.initCause(shed);

    ServerResponsePayload sheddingCause =
        RpcPayloadUtil.fromTApplicationException(
            wrapped, requestMetadata(Collections.<String, String>emptyMap()), null);

    assertEquals(TApplicationException.INTERNAL_ERROR, readBodyType(sheddingCause));
    assertTrue(sheddingCause.isTApplicationException());
    assertSame(wrapped, sheddingCause.getApplicationException());
    assertFalse(sheddingCause.getResponseRpcMetadata().getOtherMetadata().containsKey("ex"));

    TApplicationException runtimeCause =
        new TApplicationException(TApplicationException.INTERNAL_ERROR, "Internal error");
    runtimeCause.initCause(new RuntimeException("npe"));

    ServerResponsePayload runtimeCauseResponse =
        RpcPayloadUtil.fromTApplicationException(
            runtimeCause, requestMetadata(Collections.<String, String>emptyMap()), null);

    assertEquals(TApplicationException.INTERNAL_ERROR, readBodyType(runtimeCauseResponse));
    assertFalse(runtimeCauseResponse.getResponseRpcMetadata().getOtherMetadata().containsKey("ex"));
  }

  @Test
  public void fromTAEDoesNotMutateInboundMap() {
    Map<String, String> inbound = new HashMap<>();
    inbound.put("k", "v");

    RpcPayloadUtil.fromTApplicationException(
        new TApplicationException(TApplicationException.LOADSHEDDING, "busy"),
        requestMetadata(inbound),
        null);

    assertEquals(Collections.singletonMap("k", "v"), inbound);
  }

  private static RequestRpcMetadata requestMetadata(Map<String, String> otherMetadata) {
    return new RequestRpcMetadata.Builder()
        .setName("method")
        .setKind(RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE)
        .setOtherMetadata(otherMetadata)
        .build();
  }

  private static int readBodyType(ServerResponsePayload response) throws Exception {
    ByteBuf bytes = Unpooled.buffer();
    try {
      response.getDataWriter().write(TProtocolType.TCompact.apply(bytes));
      return TApplicationException.read(TProtocolType.TCompact.apply(bytes)).getType();
    } finally {
      bytes.release();
    }
  }

  private static void invokeWriter(ServerResponsePayload response) throws Exception {
    ByteBuf bytes = Unpooled.buffer();
    try {
      response.getDataWriter().write(TProtocolType.TCompact.apply(bytes));
    } finally {
      bytes.release();
    }
  }

  private static ServerRequestPayload requestWithOtherData(String key, String value) {
    Map<String, String> otherData = new HashMap<>();
    otherData.put(key, value);
    return ServerRequestPayload.create(
        readers -> Collections.emptyList(),
        new RequestRpcMetadata.Builder()
            .setName("method")
            .setKind(RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE)
            .setOtherMetadata(otherData)
            .build(),
        new NettyNiftyRequestContext(null, null),
        1);
  }

  private static ContextChain recordingChain(List<String> hooks) {
    ThriftEventHandler handler =
        new ThriftEventHandler() {
          @Override
          public void preWriteException(Object context, String methodName, Throwable t) {
            hooks.add("pre");
          }

          @Override
          public void declaredUserException(Object o, String methodName, Throwable t) {
            hooks.add("declared");
          }

          @Override
          public void undeclaredUserException(Object o, String methodName, Throwable t) {
            hooks.add("undeclared");
          }

          @Override
          public void postWriteException(Object context, String methodName, Throwable t) {
            hooks.add("post");
          }
        };
    return new ContextChain(
        Collections.singletonList(handler), "Svc.method", new NettyNiftyRequestContext(null, null));
  }
}
