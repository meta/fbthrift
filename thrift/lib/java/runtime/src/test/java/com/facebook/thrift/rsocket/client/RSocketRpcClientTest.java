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

package com.facebook.thrift.rsocket.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.facebook.thrift.client.RpcOptions;
import com.facebook.thrift.payload.ClientRequestPayload;
import com.facebook.thrift.protocol.TProtocolType;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.thrift.CodecConfig;
import org.apache.thrift.CompressionAlgorithm;
import org.apache.thrift.CompressionConfig;
import org.apache.thrift.ProtocolId;
import org.apache.thrift.RequestRpcMetadata;
import org.apache.thrift.RpcKind;
import org.apache.thrift.ZstdCompressionCodecConfig;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class RSocketRpcClientTest {

  private static final RpcOptions ZSTD_OPTIONS =
      new RpcOptions.Builder()
          .setCompressionConfig(
              new CompressionConfig.Builder()
                  .setCodecConfig(
                      CodecConfig.fromZstdConfig(new ZstdCompressionCodecConfig.Builder().build()))
                  .build())
          .build();

  @Test
  void singleResponseRequestSendsTheCompressionConfig() {
    RSocket rsocket = mock(RSocket.class);
    AtomicReference<Payload> sent = new AtomicReference<>();
    when(rsocket.requestResponse(any()))
        .thenAnswer(
            invocation -> {
              sent.set(invocation.getArgument(0));
              return Mono.never();
            });

    new RSocketRpcClient(rsocket)
        .singleRequestSingleResponse(
            requestPayload(RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE), ZSTD_OPTIONS)
        .subscribe();

    RequestRpcMetadata metadata = readMetadata(sent.get());
    assertThat(metadata.getCompression()).isEqualTo(CompressionAlgorithm.ZSTD);
    assertThat(metadata.getCompressionConfig()).isEqualTo(ZSTD_OPTIONS.getCompressionConfig());
    sent.get().release();
  }

  @Test
  void streamingRequestDoesNotAskForCompressedResponses() {
    RSocket rsocket = mock(RSocket.class);
    AtomicReference<Payload> sent = new AtomicReference<>();
    when(rsocket.requestStream(any()))
        .thenAnswer(
            invocation -> {
              sent.set(invocation.getArgument(0));
              return Flux.never();
            });

    new RSocketRpcClient(rsocket)
        .singleRequestStreamingResponse(
            requestPayload(RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE), ZSTD_OPTIONS)
        .subscribe();

    RequestRpcMetadata metadata = readMetadata(sent.get());
    assertThat(metadata.getCompression()).isEqualTo(CompressionAlgorithm.ZSTD);
    assertThat(metadata.getCompressionConfig()).isNull();
    sent.get().release();
  }

  @SuppressWarnings("unchecked")
  private static ClientRequestPayload<Object> requestPayload(RpcKind kind) {
    ClientRequestPayload<Object> payload = mock(ClientRequestPayload.class);
    when(payload.getDataWriter()).thenReturn(out -> {});
    when(payload.getPersistentHeaders()).thenReturn(Collections.emptyMap());
    when(payload.getRequestRpcMetadata())
        .thenReturn(
            new RequestRpcMetadata.Builder()
                .setProtocol(ProtocolId.COMPACT)
                .setName("ping")
                .setKind(kind)
                .build());
    return payload;
  }

  private static RequestRpcMetadata readMetadata(Payload payload) {
    return RequestRpcMetadata.read0(TProtocolType.TCompact.apply(payload.sliceMetadata()));
  }
}
