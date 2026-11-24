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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.facebook.thrift.client.v2.manager.ClientOwnership;
import com.facebook.thrift.client.v2.manager.RpcClientBinding;
import com.facebook.thrift.client.v2.manager.RpcClientManager;
import com.facebook.thrift.payload.ClientRequestPayload;
import com.facebook.thrift.payload.ClientResponsePayload;
import com.facebook.thrift.protocol.ByteBufTProtocol;
import com.facebook.thrift.protocol.TProtocolType;
import com.facebook.thrift.test.rocket.TestException;
import com.facebook.thrift.test.rocket.TestServiceReactiveClient;
import com.facebook.thrift.util.RpcClientUtils;
import io.netty.buffer.Unpooled;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import org.apache.thrift.ProtocolId;
import org.apache.thrift.ResponseRpcMetadata;
import org.apache.thrift.StreamPayloadMetadata;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * Proves declared exceptions on interaction functions deserialize to their typed form.
 *
 * <p>Interaction exception-reader maps in the generated reactive client were never populated, so
 * every interaction stream/unary error arrived as a TApplicationException. The generated client
 * below passes its (now populated) maps through a stub RpcClient that runs the real
 * RpcClientUtils.decodeStreamPayload over a hand-encoded BINARY error envelope.
 */
public class InteractionStreamExceptionTest {

  /** Answers every stream with one prebuilt ERROR payload via the real decode path. */
  private static final class ErrorRpcClient implements RpcClient {
    private final Payload errorPayload;
    private final boolean firstResponse;

    ErrorRpcClient(Payload errorPayload, boolean firstResponse) {
      this.errorPayload = errorPayload;
      this.firstResponse = firstResponse;
    }

    @Override
    public <T, K> Flux<ClientResponsePayload<K>> singleRequestStreamingResponse(
        ClientRequestPayload<T> payload, RpcOptions options) {
      return Flux.just(
          RpcClientUtils.decodeStreamPayload(
              payload,
              errorPayload,
              StreamPayloadMetadata.defaultInstance(),
              ResponseRpcMetadata.defaultInstance(),
              firstResponse));
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <T> Mono<ClientResponsePayload<T>> singleRequestSingleResponse(
        ClientRequestPayload<T> payload, RpcOptions options) {
      return Mono.just(
          (ClientResponsePayload<T>)
              RpcClientUtils.decodeRSocketPayload(
                  (com.facebook.thrift.payload.Reader) payload.getResponseReader(),
                  payload.getExceptionReaders(),
                  TProtocolType.TBinary.apply(errorPayload.sliceData()),
                  ResponseRpcMetadata.defaultInstance()));
    }

    @Override
    public Mono<Void> onClose() {
      return Mono.empty();
    }

    @Override
    public void dispose() {}

    @Override
    public boolean isDisposed() {
      return false;
    }
  }

  private static final class StubManager implements RpcClientManager {
    private final RpcClient client;

    StubManager(RpcClient client) {
      this.client = client;
    }

    @Override
    public Mono<RpcClient> acquire() {
      return Mono.just(client);
    }

    @Override
    public Mono<Void> onClose() {
      return Mono.empty();
    }

    @Override
    public void dispose() {}

    @Override
    public boolean isDisposed() {
      return false;
    }
  }

  private static Payload errorPayload(short fieldId, String msg) {
    ByteBufTProtocol protocol = TProtocolType.TBinary.apply(Unpooled.buffer());
    protocol.writeStructBegin(new TStruct());
    protocol.writeFieldBegin(new TField("e", TType.STRUCT, fieldId));
    new TestException.Builder().setMsg(msg).build().write0(protocol);
    protocol.writeFieldEnd();
    protocol.writeFieldStop();
    protocol.writeStructEnd();
    return DefaultPayload.create(protocol.getByteBuf());
  }

  private static TestServiceReactiveClient newClient(ErrorRpcClient rpc) {
    return new TestServiceReactiveClient(
        ProtocolId.BINARY, new RpcClientBinding(new StubManager(rpc), ClientOwnership.BORROWED));
  }

  @Test
  public void interactionStreamDeclaredExceptionArrivesTyped() {
    ErrorRpcClient rpc = new ErrorRpcClient(errorPayload((short) 21, "boom-21"), false);
    StepVerifier.create(newClient(rpc).createTestSession().streamDeclaredException())
        .expectErrorSatisfies(
            e -> {
              assertTrue(e instanceof TestException, "got " + e.getClass());
              assertEquals("boom-21", ((TestException) e).getMsg());
            })
        .verify();
  }

  @Test
  public void interactionStreamInitDeclaredExceptionArrivesTyped() {
    ErrorRpcClient rpc = new ErrorRpcClient(errorPayload((short) 23, "boom-23"), true);
    StepVerifier.create(newClient(rpc).createTestSession().streamInitDeclaredException())
        .expectErrorSatisfies(
            e -> {
              assertTrue(e instanceof TestException, "got " + e.getClass());
              assertEquals("boom-23", ((TestException) e).getMsg());
            })
        .verify();
  }

  @Test
  public void interactionUnaryDeclaredExceptionArrivesTyped() {
    ErrorRpcClient rpc = new ErrorRpcClient(errorPayload((short) 22, "boom-22"), true);
    StepVerifier.create(newClient(rpc).createTestSession().interactionUnaryDeclaredException())
        .expectErrorSatisfies(
            e -> {
              assertTrue(e instanceof TestException, "got " + e.getClass());
              assertEquals("boom-22", ((TestException) e).getMsg());
            })
        .verify();
  }
}
