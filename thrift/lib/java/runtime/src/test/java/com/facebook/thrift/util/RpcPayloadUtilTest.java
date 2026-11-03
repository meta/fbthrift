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
import static org.assertj.core.api.Assertions.assertThat;

import com.facebook.swift.service.ContextChain;
import com.facebook.swift.service.ThriftEventHandler;
import com.facebook.thrift.payload.ServerRequestPayload;
import com.facebook.thrift.protocol.TProtocolType;
import com.facebook.thrift.test.rocket.TestServerException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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
    assertThat(convertOtherData(null)).isEmpty();

    Map<String, String> otherData = new HashMap<>();
    otherData.put("header", "caf\u00e9 \u2713");
    Map<String, byte[]> converted = convertOtherData(otherData);

    assertThat(converted).containsOnlyKeys("header");
    assertThat(converted.get("header"))
        .isEqualTo("caf\u00e9 \u2713".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void exceptionMetadataKeepsScopeOtherDataAndClassification() {
    ServerRequestPayload request = requestWithOtherData("header", "value");
    TestServerException declared = new TestServerException("boom");

    // First-response metadata passes the string map through; the declared exception keeps its IDL
    // classification (`transient server` in rocket_protocol.thrift).
    ResponseRpcMetadata response = declaredExceptionResponseMetadata(request, declared);
    assertThat(response.getOtherMetadata()).containsEntry("header", "value");
    PayloadExceptionMetadataBase declaredBase =
        response.getPayloadMetadata().getExceptionMetadata();
    assertThat(declaredBase.getNameUtf8()).isEqualTo(TestServerException.class.getName());
    assertThat(declaredBase.getWhatUtf8()).isEqualTo(declared.getMessage());
    assertThat(declaredBase.getMetadata().isSetDeclaredException()).isTrue();
    assertThat(
            declaredBase.getMetadata().getDeclaredException().getErrorClassification().getBlame())
        .isEqualTo(ErrorBlame.SERVER);
    assertThat(declaredBase.getMetadata().getDeclaredException().getErrorClassification().getKind())
        .isEqualTo(ErrorKind.TRANSIENT);

    // Stream-scope metadata converts the map to bytes; undeclared is appUnknown blamed on the
    // server.
    StreamPayloadMetadata stream = undeclaredExceptionStreamMetadata(request, "Name", "what");
    assertThat(stream.getOtherMetadata().get("header"))
        .isEqualTo("value".getBytes(StandardCharsets.UTF_8));
    PayloadExceptionMetadataBase undeclaredBase =
        stream.getPayloadMetadata().getExceptionMetadata();
    assertThat(undeclaredBase.getNameUtf8()).isEqualTo("Name");
    assertThat(undeclaredBase.getWhatUtf8()).isEqualTo("what");
    assertThat(undeclaredBase.getMetadata().isSetAppUnknownException()).isTrue();
    assertThat(
            undeclaredBase
                .getMetadata()
                .getAppUnknownException()
                .getErrorClassification()
                .getBlame())
        .isEqualTo(ErrorBlame.SERVER);
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
      assertThat(field.id).isEqualTo((short) 7);
      assertThat(field.type).isEqualTo(TType.STRUCT);
      assertThat(TestServerException.read0(in).getMsg()).isEqualTo("boom");
      in.readFieldEnd();
      assertThat(in.readFieldBegin().type).isEqualTo(TType.STOP);
    } finally {
      declaredBytes.release();
    }
    assertThat(hooks).containsExactly("pre", "declared", "post");

    hooks.clear();
    TApplicationException undeclared =
        new TApplicationException(TApplicationException.INTERNAL_ERROR, "bad");
    ByteBuf undeclaredBytes = Unpooled.buffer();
    try {
      undeclaredExceptionWriter(undeclared, chain)
          .write(TProtocolType.TCompact.apply(undeclaredBytes));
      TApplicationException read =
          TApplicationException.read(TProtocolType.TCompact.apply(undeclaredBytes));
      assertThat(read.getType()).isEqualTo(TApplicationException.INTERNAL_ERROR);
      assertThat(read.getMessage()).isEqualTo("bad");
    } finally {
      undeclaredBytes.release();
    }
    // The caller owns preWriteException for the undeclared writer.
    assertThat(hooks).containsExactly("undeclared", "post");
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
