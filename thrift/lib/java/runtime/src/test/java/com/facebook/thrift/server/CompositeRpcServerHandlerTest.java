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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.facebook.thrift.example.ping.PingRequest;
import com.facebook.thrift.example.ping.PingResponse;
import com.facebook.thrift.example.ping.SnakeCaseStreamService;
import com.facebook.thrift.example.ping.SnakeCaseStreamServiceRpcServerHandler;
import com.facebook.thrift.payload.ServerRequestPayload;
import com.facebook.thrift.util.NettyNiftyRequestContext;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.util.Arrays;
import java.util.Collections;
import org.apache.thrift.RequestRpcMetadata;
import org.apache.thrift.RpcKind;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

/**
 * Pins the routing contract {@link CompositeRpcServerHandler} depends on: generated handlers key
 * {@code getMethodMap()} by wire name, because every dispatch path looks methods up by the name in
 * request metadata. A camelCase Java name must never leak into the map keys.
 */
public class CompositeRpcServerHandlerTest {

  @Test
  public void generatedHandler_registersSnakeCaseStreamByWireName() {
    SnakeCaseStreamServiceRpcServerHandler handler =
        new SnakeCaseStreamServiceRpcServerHandler(
            mock(SnakeCaseStreamService.Reactive.class), Collections.emptyList());

    assertThat(handler.getMethodMap()).containsKey("get_items");
  }

  @Test
  public void composite_routesSnakeCaseStreamByWireName() {
    SnakeCaseStreamService.Reactive delegate = mock(SnakeCaseStreamService.Reactive.class);
    when(delegate.getItems(any(PingRequest.class)))
        .thenReturn(Flux.just(new PingResponse.Builder().setResponse("a").build()));

    RpcServerHandler handler =
        new SnakeCaseStreamServiceRpcServerHandler(delegate, Collections.emptyList());
    CompositeRpcServerHandler composite =
        new CompositeRpcServerHandler(Collections.singletonList(handler));

    StepVerifier.create(
            composite.singleRequestStreamingResponse(
                streamPayload("get_items", new PingRequest.Builder().setRequest("hi").build())))
        .expectNextCount(2)
        .verifyComplete();

    verify(delegate).getItems(any(PingRequest.class));
  }

  private static ServerRequestPayload streamPayload(String method, Object... args) {
    ByteBuf frame = ByteBufAllocator.DEFAULT.buffer().writeInt(1);

    return ServerRequestPayload.create(
        readers -> Arrays.asList(args),
        new RequestRpcMetadata.Builder()
            .setName(method)
            .setKind(RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE)
            .build(),
        new NettyNiftyRequestContext(null, null),
        1,
        frame);
  }
}
