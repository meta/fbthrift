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

package com.facebook.thrift.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.facebook.thrift.payload.ClientRequestPayload;
import com.facebook.thrift.payload.Writer;
import com.facebook.thrift.protocol.TProtocolType;
import com.facebook.thrift.test.rocket.TestServerException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.Collections;
import org.apache.thrift.ErrorBlame;
import org.apache.thrift.PayloadExceptionMetadataBase;
import org.apache.thrift.ProtocolId;
import org.apache.thrift.RequestRpcMetadata;
import org.apache.thrift.RpcKind;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

class SinkErrorsTest {

  @Test
  void declaredErrorKeepsFieldIdAndMetadata() throws Exception {
    ClientRequestPayload<String> request = request();
    TestServerException failure = new TestServerException("boom");

    ClientRequestPayload<String> error = SinkErrors.sinkError(request, failure, (short) 7);
    ByteBuf bytes = encode(error);

    try {
      TProtocol in = TProtocolType.TCompact.apply(bytes);
      in.readStructBegin();

      TField field = in.readFieldBegin();

      assertEquals((short) 7, field.id);
      assertEquals(TType.STRUCT, field.type);
      assertEquals("boom", TestServerException.read0(in).getMsg());
      in.readFieldEnd();

      assertEquals(TType.STOP, in.readFieldBegin().type);

      PayloadExceptionMetadataBase base = error.getSinkErrorMetadata().getExceptionMetadata();
      assertEquals(TestServerException.class.getName(), base.getNameUtf8());
      assertEquals(failure.getMessage(), base.getWhatUtf8());
      assertTrue(base.getMetadata().isSetDeclaredException());
      assertSame(request.getRequestRpcMetadata(), error.getRequestRpcMetadata());
      assertEquals("svc", error.getServiceName());
    } finally {
      bytes.release();
    }
  }

  @Test
  void undeclaredErrorSendsAppUnknownBlamedOnClient() throws Exception {
    ClientRequestPayload<String> request = request();
    RuntimeException failure = new RuntimeException("bad");

    ClientRequestPayload<String> error = SinkErrors.sinkError(request, failure, null);
    ByteBuf bytes = encode(error);

    try {
      TApplicationException read = TApplicationException.read(TProtocolType.TCompact.apply(bytes));

      assertEquals("bad", read.getMessage());

      PayloadExceptionMetadataBase base = error.getSinkErrorMetadata().getExceptionMetadata();
      assertEquals(RuntimeException.class.getName(), base.getNameUtf8());
      assertEquals("bad", base.getWhatUtf8());
      assertTrue(base.getMetadata().isSetAppUnknownException());
      assertEquals(
          ErrorBlame.CLIENT,
          base.getMetadata().getAppUnknownException().getErrorClassification().getBlame());
    } finally {
      bytes.release();
    }
  }

  @Test
  void nullMessageFallsBackToClassName() throws Exception {
    ClientRequestPayload<String> request = request();
    RuntimeException failure = new RuntimeException();

    ClientRequestPayload<String> error = SinkErrors.sinkError(request, failure, null);
    ByteBuf bytes = encode(error);

    try {
      TApplicationException read = TApplicationException.read(TProtocolType.TCompact.apply(bytes));

      assertEquals(RuntimeException.class.getName(), read.getMessage());

      PayloadExceptionMetadataBase base = error.getSinkErrorMetadata().getExceptionMetadata();
      assertEquals(RuntimeException.class.getName(), base.getWhatUtf8());
    } finally {
      bytes.release();
    }
  }

  @Test
  void errorChainEmitsErrorPayloadThenRethrows() {
    ClientRequestPayload<String> request = request();
    ClientRequestPayload<String> item = request();
    TestServerException failure = new TestServerException("boom");

    Flux<ClientRequestPayload<String>> chained =
        SinkErrors.withSinkError(
            request, Flux.just(item).concatWith(Flux.error(failure)), error -> (short) 7);

    StepVerifier.create(chained, 1)
        .expectNext(item)
        .thenRequest(1)
        .expectNextMatches(
            error ->
                error
                    .getSinkErrorMetadata()
                    .getExceptionMetadata()
                    .getMetadata()
                    .isSetDeclaredException())
        .expectErrorMatches(error -> error == failure)
        .verify();
  }

  @Test
  void errorChainWithoutDeclaredExceptionsSendsAppUnknown() {
    ClientRequestPayload<String> request = request();
    RuntimeException failure = new RuntimeException("bad");

    Flux<ClientRequestPayload<String>> chained =
        SinkErrors.withSinkError(request, Flux.error(failure), null);

    StepVerifier.create(chained, 1)
        .expectNextMatches(
            error -> {
              PayloadExceptionMetadataBase base =
                  error.getSinkErrorMetadata().getExceptionMetadata();

              return base.getMetadata().isSetAppUnknownException()
                  && base.getMetadata().getAppUnknownException().getErrorClassification().getBlame()
                      == ErrorBlame.CLIENT;
            })
        .expectErrorMatches(error -> error == failure)
        .verify();
  }

  private static ByteBuf encode(ClientRequestPayload<String> payload) throws Exception {
    ByteBuf bytes = Unpooled.buffer();

    TProtocol out = TProtocolType.TCompact.apply(bytes);
    out.writeStructBegin(new TStruct("args"));
    payload.getDataWriter().write(out);
    out.writeFieldStop();
    out.writeStructEnd();

    return bytes;
  }

  private static ClientRequestPayload<String> request() {
    return ClientRequestPayload.create(
        "svc",
        Writer.EMPTY_WRITER,
        protocol -> "response",
        Collections.emptyMap(),
        Collections.emptyMap(),
        new RequestRpcMetadata.Builder()
            .setName("sinkMethod")
            .setKind(RpcKind.SINK)
            .setProtocol(ProtocolId.COMPACT)
            .build(),
        Collections.emptyMap());
  }
}
