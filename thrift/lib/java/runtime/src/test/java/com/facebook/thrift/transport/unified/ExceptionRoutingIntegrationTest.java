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

package com.facebook.thrift.transport.unified;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.facebook.nifty.core.RequestContext;
import com.facebook.swift.service.ThriftEventHandler;
import com.facebook.swift.service.ThriftServerConfig;
import com.facebook.thrift.client.RpcClient;
import com.facebook.thrift.client.RpcClientFactory;
import com.facebook.thrift.client.RpcOptions;
import com.facebook.thrift.client.ThriftClientConfig;
import com.facebook.thrift.client.v2.manager.RpcClientBinding;
import com.facebook.thrift.example.ping.CustomException;
import com.facebook.thrift.example.ping.PingRequest;
import com.facebook.thrift.example.ping.PingResponse;
import com.facebook.thrift.example.ping.PingService;
import com.facebook.thrift.example.ping.PingServiceRpcServerHandler;
import com.facebook.thrift.legacy.server.LegacyServerTransport;
import com.facebook.thrift.legacy.server.LegacyServerTransportFactory;
import com.facebook.thrift.payload.ClientRequestPayload;
import com.facebook.thrift.payload.ServerRequestPayload;
import com.facebook.thrift.payload.ServerResponsePayload;
import com.facebook.thrift.payload.Writer;
import com.facebook.thrift.protocol.TProtocolType;
import com.facebook.thrift.server.RpcServerHandler;
import com.facebook.thrift.util.RpcServerUtils;
import com.facebook.thrift.util.SPINiftyMetrics;
import io.airlift.units.Duration;
import io.netty.handler.ssl.ClientAuth;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.thrift.RequestRpcMetadata;
import org.apache.thrift.RpcKind;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * End-to-end exception routing over Header transports with an in-process server and a real client:
 * overload rejections carry the explicit {@code ex} code the client can read, ordinary errors keep
 * their shape without it, and oneway calls stay silent.
 *
 * <p>Sections cover the unified Header path (ThriftConnectionAcceptor) and the legacy Header path
 * (ThriftServerHandler). A Rocket section is reserved below for the Rocket session's rows; this
 * file asserts no Rocket overload behavior.
 */
public class ExceptionRoutingIntegrationTest {

  private UnifiedServerTransport unifiedTransport;
  private LegacyServerTransport legacyTransport;
  private RpcClientBinding rawBinding;
  private PingService.Reactive typedClient;

  @BeforeAll
  public static void setUpClass() throws Exception {
    TestCertificateUtil.initialize();
  }

  @AfterAll
  public static void tearDownClass() {
    TestCertificateUtil.cleanup();
  }

  @AfterEach
  public void tearDown() {
    if (typedClient != null) {
      typedClient.dispose();
      typedClient = null;
    }
    if (rawBinding != null) {
      rawBinding.dispose();
      rawBinding = null;
    }
    if (unifiedTransport != null) {
      unifiedTransport.dispose();
      unifiedTransport = null;
    }
    if (legacyTransport != null) {
      legacyTransport.dispose();
      legacyTransport = null;
    }
  }

  // -------------------------------------------------------------------------
  // Unified Header path (ThriftConnectionAcceptor)
  // -------------------------------------------------------------------------

  @Test
  public void unifiedPreprocessShedBinaryCarriesAppCode() throws Exception {
    unifiedPreprocessShedCarriesAppCode(TProtocolType.TBinary);
  }

  @Test
  public void unifiedPreprocessShedCompactCarriesAppCode() throws Exception {
    unifiedPreprocessShedCarriesAppCode(TProtocolType.TCompact);
  }

  private void unifiedPreprocessShedCarriesAppCode(TProtocolType protocol) throws Exception {
    SocketAddress address = startUnifiedTransport(sheddingServerHandler("shed"));
    RpcClient raw = rawClient(address);

    StepVerifier.create(
            raw.singleRequestSingleResponse(pingRawRequest(protocol, "ping"), RpcOptions.EMPTY))
        .assertNext(
            response -> {
              assertEquals("22", response.getHeaders().get("ex"));
              assertTrue(response.getException() instanceof TApplicationException);
              assertEquals(
                  TApplicationException.LOADSHEDDING,
                  ((TApplicationException) response.getException()).getType());
            })
        .verifyComplete();
  }

  @Test
  public void unifiedAsyncSheddingPreservesTypeAndFiresHooksOnce() throws Exception {
    List<String> hooks = new ArrayList<>();
    List<Throwable> hookErrors = new ArrayList<>();
    TApplicationException shed =
        new TApplicationException(TApplicationException.LOADSHEDDING, "async-shed");
    PingService.Reactive delegate = Mockito.mock(PingService.Reactive.class);
    Mockito.when(delegate.ping(Mockito.any(PingRequest.class))).thenReturn(Mono.error(shed));
    SocketAddress address =
        startUnifiedTransport(
            new PingServiceRpcServerHandler(
                delegate, Collections.singletonList(recordingHandler(hooks, hookErrors))));
    RpcClient raw = rawClient(address);

    StepVerifier.create(
            raw.singleRequestSingleResponse(
                pingRawRequest(TProtocolType.TCompact, "ping"), RpcOptions.EMPTY))
        .assertNext(
            response -> {
              assertEquals("22", response.getHeaders().get("ex"));
              assertFalse(response.getHeaders().containsKey("uex"));
              assertTrue(response.getException() instanceof TApplicationException);
              assertEquals(
                  TApplicationException.LOADSHEDDING,
                  ((TApplicationException) response.getException()).getType());
            })
        .verifyComplete();
    assertEquals(Arrays.asList("pre", "undeclared", "post"), hooks);
    assertEquals(3, hookErrors.size());
    assertSame(shed, hookErrors.get(0));
    assertSame(shed, hookErrors.get(1));
    assertSame(shed, hookErrors.get(2));
  }

  @Test
  public void unifiedSyncSheddingPreservesType() throws Exception {
    PingService blocking = Mockito.mock(PingService.class);
    Mockito.when(blocking.ping(Mockito.any(PingRequest.class)))
        .thenThrow(new TApplicationException(TApplicationException.LOADSHEDDING, "sync-shed"));
    SocketAddress address =
        startUnifiedTransport(
            new PingServiceRpcServerHandler(blocking, Collections.<ThriftEventHandler>emptyList()));

    PingService.Reactive client = typedClient(address, TProtocolType.TCompact);
    StepVerifier.create(client.ping(new PingRequest.Builder().setRequest("hello").build()))
        .expectErrorSatisfies(
            error -> {
              assertTrue(error instanceof TApplicationException);
              assertEquals(
                  TApplicationException.LOADSHEDDING, ((TApplicationException) error).getType());
              assertEquals("sync-shed", error.getMessage());
            })
        .verify();
  }

  @Test
  public void unifiedDeclaredErrorPreserved() throws Exception {
    // IDL-declared errors stay in the result reply: the async generator never rewrites them.
    PingService blocking = Mockito.mock(PingService.class);
    Mockito.when(blocking.pingException(Mockito.any(PingRequest.class)))
        .thenThrow(new CustomException.Builder().setMessage("custom exception: hello").build());
    SocketAddress address =
        startUnifiedTransport(
            new PingServiceRpcServerHandler(blocking, Collections.<ThriftEventHandler>emptyList()));

    PingService.Reactive client = typedClient(address, TProtocolType.TCompact);
    StepVerifier.create(client.pingException(new PingRequest.Builder().setRequest("x").build()))
        .expectErrorSatisfies(
            error -> {
              assertTrue(error instanceof CustomException);
              assertEquals("custom exception: hello", error.getMessage());
            })
        .verify();
  }

  @Test
  public void unifiedOrdinaryErrorStaysInternalWithoutEx() throws Exception {
    PingService.Reactive delegate = Mockito.mock(PingService.Reactive.class);
    Mockito.when(delegate.ping(Mockito.any(PingRequest.class)))
        .thenReturn(Mono.error(new RuntimeException("npe")));
    SocketAddress address =
        startUnifiedTransport(
            new PingServiceRpcServerHandler(delegate, Collections.<ThriftEventHandler>emptyList()));
    RpcClient raw = rawClient(address);

    StepVerifier.create(
            raw.singleRequestSingleResponse(
                pingRawRequest(TProtocolType.TCompact, "ping"), RpcOptions.EMPTY))
        .assertNext(
            response -> {
              assertTrue(response.getException() instanceof TApplicationException);
              assertEquals(
                  TApplicationException.INTERNAL_ERROR,
                  ((TApplicationException) response.getException()).getType());
              assertFalse(response.getHeaders().containsKey("ex"));
            })
        .verifyComplete();
  }

  @Test
  public void unifiedHookInjectedUexCannotOverrideClassification() throws Exception {
    SocketAddress address =
        startUnifiedTransport(
            new PingServiceRpcServerHandler(
                Mockito.mock(PingService.class),
                Arrays.asList(sheddingHandler("shed"), uexInjector())));
    RpcClient raw = rawClient(address);

    StepVerifier.create(
            raw.singleRequestSingleResponse(
                pingRawRequest(TProtocolType.TCompact, "ping"), RpcOptions.EMPTY))
        .assertNext(
            response -> {
              assertEquals("22", response.getHeaders().get("ex"));
              assertFalse(response.getHeaders().containsKey("uex"));
              assertFalse(response.getHeaders().containsKey("anyex"));
            })
        .verifyComplete();
  }

  @Test
  public void unifiedShedOnceThenSuccess() throws Exception {
    // One typed client (one connection): the first call sheds, the second succeeds.
    PingService blocking = Mockito.mock(PingService.class);
    Mockito.when(blocking.ping(Mockito.any(PingRequest.class)))
        .thenReturn(new PingResponse.Builder().setResponse("hello_pong_0").build());
    SocketAddress address =
        startUnifiedTransport(
            new PingServiceRpcServerHandler(
                blocking, Collections.singletonList(shedOnceHandler("shed"))));

    PingService.Reactive client = typedClient(address, TProtocolType.TCompact);
    StepVerifier.create(client.ping(new PingRequest.Builder().setRequest("hello").build()))
        .expectErrorSatisfies(
            error -> {
              assertTrue(error instanceof TApplicationException);
              assertEquals(
                  TApplicationException.LOADSHEDDING, ((TApplicationException) error).getType());
            })
        .verify();
    StepVerifier.create(client.ping(new PingRequest.Builder().setRequest("hello").build()))
        .assertNext(response -> assertEquals("hello_pong_0", response.getResponse()))
        .verifyComplete();
  }

  @Test
  public void unifiedOnewaySheddingSendsNothing() throws Exception {
    CountDownLatch handled = new CountDownLatch(1);
    AtomicBoolean noResponse = new AtomicBoolean();
    RpcServerHandler generated = sheddingServerHandler("shed");
    RpcServerHandler handler =
        new RpcServerHandler() {
          @Override
          public Mono<Void> singleRequestNoResponse(ServerRequestPayload payload) {
            noResponse.set(true);
            handled.countDown();
            return generated.singleRequestNoResponse(payload);
          }

          @Override
          public Mono<ServerResponsePayload> singleRequestSingleResponse(
              ServerRequestPayload payload) {
            return generated.singleRequestSingleResponse(payload);
          }
        };
    SocketAddress address = startUnifiedTransport(handler);
    RpcClient raw = rawClient(address);

    StepVerifier.create(raw.singleRequestNoResponse(pingRawOnewayRequest(), RpcOptions.EMPTY))
        .verifyComplete();
    assertTrue(handled.await(10, TimeUnit.SECONDS));
    assertTrue(noResponse.get());

    // The server stays healthy with no stray reply: a follow-up call on a new client
    // routes to the shedding handler and fails with LOADSHEDDING.
    PingService.Reactive client = typedClient(address, TProtocolType.TCompact);
    StepVerifier.create(client.ping(new PingRequest.Builder().setRequest("hello").build()))
        .expectErrorSatisfies(
            error -> {
              assertTrue(error instanceof TApplicationException);
              assertEquals(
                  TApplicationException.LOADSHEDDING, ((TApplicationException) error).getType());
            })
        .verify();
  }

  // -------------------------------------------------------------------------
  // Legacy Header path (ThriftServerHandler)
  // -------------------------------------------------------------------------

  @Test
  public void legacyShedCarriesAppCode() throws Exception {
    SocketAddress address = startLegacyTransport(sheddingServerHandler("shed"));
    RpcClient raw = rawClient(address);

    StepVerifier.create(
            raw.singleRequestSingleResponse(
                pingRawRequest(TProtocolType.TCompact, "ping"), RpcOptions.EMPTY))
        .assertNext(
            response -> {
              assertEquals("22", response.getHeaders().get("ex"));
              assertTrue(response.getException() instanceof TApplicationException);
              assertEquals(
                  TApplicationException.LOADSHEDDING,
                  ((TApplicationException) response.getException()).getType());
            })
        .verifyComplete();
  }

  @Test
  public void legacyOnewaySheddingSendsNothing() throws Exception {
    CountDownLatch handled = new CountDownLatch(1);
    AtomicBoolean noResponse = new AtomicBoolean();
    RpcServerHandler generated = sheddingServerHandler("shed");
    RpcServerHandler handler =
        new RpcServerHandler() {
          @Override
          public Mono<Void> singleRequestNoResponse(ServerRequestPayload payload) {
            noResponse.set(true);
            handled.countDown();
            return generated.singleRequestNoResponse(payload);
          }

          @Override
          public Mono<ServerResponsePayload> singleRequestSingleResponse(
              ServerRequestPayload payload) {
            return generated.singleRequestSingleResponse(payload);
          }
        };
    SocketAddress address = startLegacyTransport(handler);
    RpcClient raw = rawClient(address);

    StepVerifier.create(raw.singleRequestNoResponse(pingRawOnewayRequest(), RpcOptions.EMPTY))
        .verifyComplete();
    assertTrue(handled.await(10, TimeUnit.SECONDS));
    assertTrue(noResponse.get());

    // The server stays healthy with no stray reply: a follow-up call on a new client
    // routes to the shedding handler and fails with LOADSHEDDING.
    PingService.Reactive client = typedClient(address, TProtocolType.TCompact);
    StepVerifier.create(client.ping(new PingRequest.Builder().setRequest("hello").build()))
        .expectErrorSatisfies(
            error -> {
              assertTrue(error instanceof TApplicationException);
              assertEquals(
                  TApplicationException.LOADSHEDDING, ((TApplicationException) error).getType());
            })
        .verify();
  }

  @Test
  public void legacyOrdinaryErrorClean() throws Exception {
    PingService.Reactive delegate = Mockito.mock(PingService.Reactive.class);
    Mockito.when(delegate.ping(Mockito.any(PingRequest.class)))
        .thenReturn(Mono.error(new RuntimeException("npe")));
    SocketAddress address =
        startLegacyTransport(
            new PingServiceRpcServerHandler(delegate, Collections.<ThriftEventHandler>emptyList()));
    RpcClient raw = rawClient(address);

    StepVerifier.create(
            raw.singleRequestSingleResponse(
                pingRawRequest(TProtocolType.TCompact, "ping"), RpcOptions.EMPTY))
        .assertNext(
            response -> {
              assertTrue(response.getException() instanceof TApplicationException);
              assertEquals(
                  TApplicationException.INTERNAL_ERROR,
                  ((TApplicationException) response.getException()).getType());
              assertFalse(response.getHeaders().containsKey("ex"));
              assertFalse(response.getHeaders().containsKey("uex"));
            })
        .verifyComplete();
  }

  // -------------------------------------------------------------------------
  // Rocket path: reserved for the Rocket session's rows. This file asserts no
  // Rocket overload behavior.
  // -------------------------------------------------------------------------

  private SocketAddress startUnifiedTransport(RpcServerHandler handler) throws Exception {
    ThriftServerConfig serverConfig =
        new ThriftServerConfig()
            .setSslEnabled(true)
            .setEnableJdkSsl(false)
            .setKeyFile(TestCertificateUtil.getKeyFilePath())
            .setCertFile(TestCertificateUtil.getCertFilePath())
            .setCAFile(TestCertificateUtil.getCAFilePath())
            .setEnableAlpn(true)
            .setAllowPlaintext(true)
            .setTaskExpirationTimeout(Duration.valueOf("60s"))
            .setConnectionLimit(10);
    SocketAddress address = new InetSocketAddress("localhost", RpcServerUtils.findFreePort());
    unifiedTransport =
        UnifiedServerTransport.createNewInstance(
                address, handler, serverConfig, new SPINiftyMetrics())
            .block();
    return unifiedTransport.getAddress();
  }

  private SocketAddress startLegacyTransport(RpcServerHandler handler) throws Exception {
    LegacyServerTransportFactory transportFactory =
        new LegacyServerTransportFactory(
            new ThriftServerConfig().setClientAuth(ClientAuth.OPTIONAL).setSslEnabled(false));
    legacyTransport = transportFactory.createServerTransport(handler).block();
    return legacyTransport.getAddress();
  }

  private RpcClient rawClient(SocketAddress address) {
    RpcClientFactory factory =
        RpcClientFactory.builder()
            .setThriftClientConfig(
                new ThriftClientConfig()
                    .setDisableSSL(true)
                    .setRequestTimeout(Duration.succinctDuration(30, TimeUnit.SECONDS)))
            .setDisableLoadBalancing(true)
            .setDisableReconnectingClient(true)
            .build();
    rawBinding = factory.createRpcClientBinding(address);
    RpcClient client = rawBinding.acquire().block();
    assertTrue(client != null);
    return client;
  }

  private PingService.Reactive typedClient(SocketAddress address, TProtocolType protocol) {
    RpcClientFactory factory =
        RpcClientFactory.builder()
            .setThriftClientConfig(
                new ThriftClientConfig()
                    .setDisableSSL(true)
                    .setRequestTimeout(Duration.succinctDuration(30, TimeUnit.SECONDS)))
            .setDisableLoadBalancing(true)
            .setDisableReconnectingClient(true)
            .build();
    typedClient =
        PingService.Reactive.clientBuilder()
            .setProtocolId(protocol.getProtocolId())
            .build(factory, address);
    return typedClient;
  }

  private static ClientRequestPayload<Object> pingRawRequest(
      TProtocolType protocol, String method) {
    return ClientRequestPayload.create(
        "PingService",
        pingArgsWriter("hello"),
        in -> {
          throw new IllegalStateException("unexpected body read");
        },
        Collections.emptyMap(),
        new RequestRpcMetadata.Builder()
            .setName(method)
            .setKind(RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE)
            .setProtocol(protocol.getProtocolId())
            .setOtherMetadata(Collections.emptyMap())
            .build(),
        Collections.emptyMap());
  }

  private static ClientRequestPayload<Object> pingRawOnewayRequest() {
    return ClientRequestPayload.create(
        "PingService",
        pingArgsWriter("hello"),
        in -> {
          throw new IllegalStateException("unexpected body read");
        },
        Collections.emptyMap(),
        new RequestRpcMetadata.Builder()
            .setName("ping")
            .setKind(RpcKind.SINGLE_REQUEST_NO_RESPONSE)
            .setProtocol(TProtocolType.TCompact.getProtocolId())
            .setOtherMetadata(Collections.emptyMap())
            .build(),
        Collections.emptyMap());
  }

  private static Writer pingArgsWriter(String request) {
    return out -> {
      out.writeFieldBegin(new TField("key", TType.STRUCT, (short) 1));
      out.writeStructBegin(new TStruct("PingRequest"));
      out.writeFieldBegin(new TField("request", TType.STRING, (short) 1));
      out.writeString(request);
      out.writeFieldEnd();
      out.writeFieldStop();
      out.writeStructEnd();
      out.writeFieldEnd();
    };
  }

  private static RpcServerHandler sheddingServerHandler(String message) {
    return new PingServiceRpcServerHandler(
        Mockito.mock(PingService.class), Collections.singletonList(sheddingHandler(message)));
  }

  private static ThriftEventHandler sheddingHandler(String message) {
    return new ThriftEventHandler() {
      @Override
      public void preprocess(Object context, String methodName) throws TException {
        throw new TApplicationException(TApplicationException.LOADSHEDDING, message);
      }
    };
  }

  private static ThriftEventHandler shedOnceHandler(String message) {
    AtomicBoolean armed = new AtomicBoolean(true);
    return new ThriftEventHandler() {
      @Override
      public void preprocess(Object context, String methodName) throws TException {
        if (armed.getAndSet(false)) {
          throw new TApplicationException(TApplicationException.LOADSHEDDING, message);
        }
      }
    };
  }

  private static ThriftEventHandler recordingHandler(List<String> hooks, List<Throwable> errors) {
    return new ThriftEventHandler() {
      @Override
      public void preWriteException(Object context, String methodName, Throwable t) {
        hooks.add("pre");
        errors.add(t);
      }

      @Override
      public void undeclaredUserException(Object o, String methodName, Throwable t) {
        hooks.add("undeclared");
        errors.add(t);
      }

      @Override
      public void postWriteException(Object context, String methodName, Throwable t) {
        hooks.add("post");
        errors.add(t);
      }
    };
  }

  private static ThriftEventHandler uexInjector() {
    return new ThriftEventHandler() {
      private RequestContext stashed;

      @Override
      public Object getContext(String methodName, RequestContext requestContext) {
        stashed = requestContext;
        return null;
      }

      @Override
      public void undeclaredUserException(Object context, String methodName, Throwable t) {
        if (stashed != null) {
          stashed.setResponseHeader("uex", "SneakyException");
          stashed.setResponseHeader("anyex", "sneaky");
        }
      }
    };
  }
}
