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

import com.facebook.swift.service.ThriftEventHandler;
import com.facebook.thrift.protocol.TProtocolType;
import com.facebook.thrift.rsocket.util.PayloadUtil;
import com.facebook.thrift.server.RpcServerHandler;
import com.facebook.thrift.test.rocket.TestRequest;
import com.facebook.thrift.test.rocket.TestService;
import com.facebook.thrift.util.resources.RpcResources;
import com.google.common.collect.ImmutableList;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Payload;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.apache.thrift.ProtocolId;
import org.apache.thrift.RequestRpcMetadata;
import org.apache.thrift.RpcKind;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.test.StepVerifier;

/**
 * Locks in the {@link ThriftEventHandler} order on the stream known-exception path: {@code
 * preWriteException} fires before the declared/undeclared classification hooks, exactly like the
 * request-response and stream unknown-exception paths. Fails without the fix (the known-exception
 * writer skipped {@code preWriteException}).
 */
public class TestStreamKnownExceptionEventOrder {

  static Stream<Arguments> data() {
    return Stream.of(Arguments.of(ProtocolId.COMPACT), Arguments.of(ProtocolId.BINARY));
  }

  private ProtocolId protocolId;
  private final ByteBufAllocator alloc = RpcResources.getByteBufAllocator();
  private final RecordingRequestHandler requestHook = new RecordingRequestHandler();
  private final RpcServerHandler handler =
      TestService.Reactive.serverHandlerBuilder(new TestServiceHandler())
          .setEventHandlers(ImmutableList.of(requestHook))
          .build();
  private final ThriftServerRSocket rocket = new ThriftServerRSocket(handler, alloc);

  @ParameterizedTest
  @MethodSource("data")
  public void knownStreamExceptionFiresPreWriteExceptionFirst(ProtocolId protocolId) {
    this.protocolId = protocolId;
    RequestRpcMetadata metadata =
        new RequestRpcMetadata.Builder()
            .setProtocol(protocolId)
            .setName("streamDeclaredExceptionFluxError")
            .setKind(RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE)
            .build();
    TestRequest input = new TestRequest.Builder().setIntField(0).setStrField("foo").build();

    ByteBuf meta = alloc.buffer();
    TProtocol metaProtocol = TProtocolType.fromProtocolId(ProtocolId.COMPACT).apply(meta);
    metadata.write0(metaProtocol);
    meta.retain();

    ByteBuf data = alloc.buffer();
    TProtocol dataProtocol = TProtocolType.fromProtocolId(metadata.getProtocol()).apply(data);
    dataProtocol.writeStructBegin(new TStruct());
    dataProtocol.writeFieldBegin(new TField("struct", (byte) TType.STRUCT, (short) 1));
    input.write0(dataProtocol);
    dataProtocol.writeFieldEnd();
    dataProtocol.writeFieldStop();
    dataProtocol.writeStructEnd();
    data.retain();

    // 1 first-response frame + 4 elements + 1 terminal declared-exception frame.
    StepVerifier.Step<Payload> steps =
        StepVerifier.create(rocket.requestStream(PayloadUtil.createPayload(data, meta)));
    for (int i = 0; i < 6; i++) {
      steps = steps.assertNext(response -> {});
    }
    steps.verifyComplete();

    Assertions.assertEquals(
        ImmutableList.of("preWriteException", "declaredUserException", "postWriteException"),
        requestHook.exceptionEvents);
  }

  private static class RecordingRequestHandler implements ThriftEventHandler {
    private final List<String> exceptionEvents = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void preWriteException(Object context, String methodName, Throwable t) {
      exceptionEvents.add("preWriteException");
    }

    @Override
    public void declaredUserException(Object context, String methodName, Throwable t) {
      exceptionEvents.add("declaredUserException");
    }

    @Override
    public void undeclaredUserException(Object context, String methodName, Throwable t) {
      exceptionEvents.add("undeclaredUserException");
    }

    @Override
    public void postWriteException(Object context, String methodName, Throwable t) {
      exceptionEvents.add("postWriteException");
    }
  }
}
