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
import static org.junit.jupiter.api.Assertions.assertNull;
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
import com.facebook.thrift.payload.ClientResponsePayload;
import com.facebook.thrift.payload.Reader;
import com.facebook.thrift.payload.ServerRequestPayload;
import com.facebook.thrift.payload.ServerResponsePayload;
import com.facebook.thrift.payload.Writer;
import com.facebook.thrift.protocol.TProtocolType;
import com.facebook.thrift.server.CompositeRpcServerHandler;
import com.facebook.thrift.server.OverloadedException;
import com.facebook.thrift.server.RpcServerHandler;
import com.facebook.thrift.util.RpcServerUtils;
import com.facebook.thrift.util.SPINiftyMetrics;
import com.facebook.thrift.util.resources.RpcResources;
import com.facebook.thrift.util.resources.ThriftScheduler;
import io.airlift.units.Duration;
import io.netty.handler.ssl.ClientAuth;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

/**
 * End-to-end exception routing with an in-process server and a real client: overload rejections
 * carry the explicit {@code ex} code the client can read on Header transports, and the matching
 * REJECTED reason code on Rocket; ordinary errors keep their shape without it, and oneway calls
 * stay silent.
 *
 * <p>Sections cover the unified Header path (ThriftConnectionAcceptor), the legacy Header path
 * (ThriftServerHandler), and the Rocket path over the unified transport (raw RSocket client).
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
  public void unifiedReactiveSyncThrowStaysInternal() throws Exception {
    // Same synchronous-throw contract over Header: INTERNAL_ERROR, hooks once, and the
    // connection serves the next call.
    List<String> hooks = new ArrayList<>();
    List<Throwable> hookErrors = new ArrayList<>();
    RuntimeException boom = new RuntimeException("boom");
    PingService.Reactive delegate = Mockito.mock(PingService.Reactive.class);
    Mockito.when(delegate.ping(Mockito.any(PingRequest.class)))
        .thenThrow(boom)
        .thenReturn(Mono.just(new PingResponse.Builder().setResponse("ok").build()));
    SocketAddress address =
        startUnifiedTransport(
            new PingServiceRpcServerHandler(
                delegate, Collections.singletonList(recordingHandler(hooks, hookErrors))));
    PingService.Reactive client = typedClient(address, TProtocolType.TCompact);

    StepVerifier.create(client.ping(new PingRequest.Builder().setRequest("hello").build()))
        .expectErrorSatisfies(
            error -> {
              assertTrue(error instanceof TApplicationException);
              assertEquals(
                  TApplicationException.INTERNAL_ERROR, ((TApplicationException) error).getType());
              assertTrue(error.getMessage().contains("boom"));
            })
        .verify();
    assertEquals(Arrays.asList("pre", "undeclared", "post"), hooks);
    assertEquals(3, hookErrors.size());
    assertSame(hookErrors.get(0), hookErrors.get(1));
    assertSame(hookErrors.get(1), hookErrors.get(2));
    assertSame(boom, hookErrors.get(0).getCause());

    StepVerifier.create(client.ping(new PingRequest.Builder().setRequest("hello").build()))
        .assertNext(response -> assertEquals("ok", response.getResponse()))
        .verifyComplete();
  }

  @Test
  public void unifiedReactiveSyncThrowDeclaredStaysDeclared() throws Exception {
    PingService.Reactive delegate = Mockito.mock(PingService.Reactive.class);
    Mockito.when(delegate.pingException(Mockito.any(PingRequest.class)))
        .thenThrow(new CustomException.Builder().setMessage("custom: sync").build());
    SocketAddress address =
        startUnifiedTransport(
            new PingServiceRpcServerHandler(delegate, Collections.<ThriftEventHandler>emptyList()));
    PingService.Reactive client = typedClient(address, TProtocolType.TCompact);

    StepVerifier.create(client.pingException(new PingRequest.Builder().setRequest("x").build()))
        .expectErrorSatisfies(
            error -> {
              assertTrue(error instanceof CustomException);
              assertEquals("custom: sync", error.getMessage());
            })
        .verify();
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
  // Rocket path (raw RSocket client over the unified transport)
  // -------------------------------------------------------------------------

  @Test
  public void rocketPreprocessShedBinaryRejectsWithAppOverload() throws Exception {
    runRocketPreprocessShed(
        TProtocolType.TBinary,
        new TApplicationException(TApplicationException.LOADSHEDDING, "shed"),
        "22");
  }

  @Test
  public void rocketPreprocessShedCompactRejectsWithAppOverload() throws Exception {
    runRocketPreprocessShed(
        TProtocolType.TCompact,
        new TApplicationException(TApplicationException.LOADSHEDDING, "shed"),
        "22");
  }

  @Test
  public void rocketHostOverloadRejectsWithOverload() throws Exception {
    runRocketPreprocessShed(TProtocolType.TCompact, OverloadedException.host("busy"), "1");
  }

  @Test
  public void rocketApplicationOverloadRejectsWithAppOverload() throws Exception {
    runRocketPreprocessShed(TProtocolType.TCompact, OverloadedException.application("busy"), "22");
  }

  @Test
  public void rocketQueueOverloadRejectsWithQueueOverloaded() throws Exception {
    // QUEUE_OVERLOADED reports the queue-overloaded ex code, like the C++ client decodes it.
    runRocketPreprocessShed(TProtocolType.TCompact, OverloadedException.queue("backed up"), "5");
  }

  @Test
  public void rocketHandlerExplicitOverloadRejectsWithReason() throws Exception {
    PingService.Reactive delegate = Mockito.mock(PingService.Reactive.class);
    Mockito.when(delegate.ping(Mockito.any(PingRequest.class)))
        .thenReturn(Mono.error(OverloadedException.host("busy")));
    SocketAddress address =
        startUnifiedTransport(
            new PingServiceRpcServerHandler(delegate, Collections.<ThriftEventHandler>emptyList()));
    RpcClient raw = rawRocketClient(address);

    StepVerifier.create(
            raw.singleRequestSingleResponse(
                pingRawRequest(TProtocolType.TCompact, "ping"), RpcOptions.EMPTY))
        .assertNext(
            response -> {
              assertEquals("1", response.getHeaders().get("ex"));
              assertFalse(response.getHeaders().containsKey("uex"));
              assertTrue(response.getException() instanceof TApplicationException);
              assertEquals(
                  TApplicationException.LOADSHEDDING,
                  ((TApplicationException) response.getException()).getType());
            })
        .verifyComplete();
  }

  @Test
  public void rocketHandlerPlainLoadsheddingRejectsWithAppOverload() throws Exception {
    // Current landed behavior: any LOADSHEDDING converts, so a handler-body plain LOADSHEDDING
    // rejects with APP_OVERLOAD like a preprocess shed does.
    PingService.Reactive delegate = Mockito.mock(PingService.Reactive.class);
    Mockito.when(delegate.ping(Mockito.any(PingRequest.class)))
        .thenReturn(
            Mono.error(new TApplicationException(TApplicationException.LOADSHEDDING, "shed")));
    SocketAddress address =
        startUnifiedTransport(
            new PingServiceRpcServerHandler(delegate, Collections.<ThriftEventHandler>emptyList()));
    RpcClient raw = rawRocketClient(address);

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
  }

  @Test
  public void rocketPreprocessShedRejectsStream() throws Exception {
    List<String> hooks = new ArrayList<>();
    List<Throwable> hookErrors = new ArrayList<>();
    TApplicationException shed =
        new TApplicationException(TApplicationException.LOADSHEDDING, "shed");
    SocketAddress address =
        startUnifiedTransport(
            new PingServiceRpcServerHandler(
                Mockito.mock(PingService.class),
                Arrays.asList(
                    throwingPreprocessHandler(shed), recordingHandler(hooks, hookErrors))));
    RpcClient raw = rawRocketClient(address);

    StepVerifier.create(
            raw.singleRequestStreamingResponse(
                pingRawRequest(
                    TProtocolType.TCompact,
                    "streamOfPings",
                    RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE,
                    throwingReader()),
                RpcOptions.EMPTY))
        .assertNext(
            response -> {
              // A stream rejection arrives as a response item carrying the rejection's reason
              // name.
              assertTrue(response.getException() instanceof TApplicationException);
              assertEquals("APP_OVERLOAD", response.getException().getMessage());
            })
        .verifyComplete();
    assertEquals(Arrays.asList("pre", "undeclared", "post"), hooks);
    assertEquals(3, hookErrors.size());
    assertSame(shed, hookErrors.get(0));
    assertSame(shed, hookErrors.get(1));
    assertSame(shed, hookErrors.get(2));
  }

  @Test
  public void rocketMaxActiveRequestsRejectsWithOverload() throws Exception {
    // A max-active-requests admission limit sheds the excess request with OVERLOAD (ex 1).
    AtomicInteger actives = new AtomicInteger();
    ThriftEventHandler maxOneActive =
        new ThriftEventHandler() {
          @Override
          public Object getContext(String methodName, RequestContext requestContext) {
            return actives.incrementAndGet();
          }

          @Override
          public void preprocess(Object context, String methodName) throws TException {
            if (context instanceof Integer && (Integer) context > 1) {
              throw OverloadedException.host("too many active requests");
            }
          }

          @Override
          public void done(Object context, String methodName) {
            actives.decrementAndGet();
          }
        };
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    PingService.Reactive delegate = Mockito.mock(PingService.Reactive.class);
    Mockito.when(delegate.ping(Mockito.any(PingRequest.class)))
        .thenReturn(
            Mono.fromCallable(
                () -> {
                  entered.countDown();
                  if (!release.await(30, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("slot holder never released");
                  }
                  return new PingResponse.Builder().setResponse("ok").build();
                }));
    SocketAddress address =
        startUnifiedTransport(
            new PingServiceRpcServerHandler(delegate, Collections.singletonList(maxOneActive)));
    RpcClient raw = rawRocketClient(address);

    CompletableFuture<ClientResponsePayload<Object>> first =
        raw.singleRequestSingleResponse(
                pingRawRequest(
                    TProtocolType.TCompact,
                    "ping",
                    RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE,
                    PingResponse::read0),
                RpcOptions.EMPTY)
            .toFuture();
    assertTrue(entered.await(30, TimeUnit.SECONDS), "slot never occupied");

    StepVerifier.create(
            raw.singleRequestSingleResponse(
                pingRawRequest(TProtocolType.TCompact, "ping"), RpcOptions.EMPTY))
        .assertNext(
            response -> {
              assertEquals("1", response.getHeaders().get("ex"));
              assertTrue(response.getException() instanceof TApplicationException);
              assertEquals(
                  TApplicationException.LOADSHEDDING,
                  ((TApplicationException) response.getException()).getType());
            })
        .verifyComplete();

    release.countDown();
    assertNull(first.get(30, TimeUnit.SECONDS).getException());
  }

  @Test
  public void rocketOrdinaryErrorStaysInternalWithoutEx() throws Exception {
    PingService.Reactive delegate = Mockito.mock(PingService.Reactive.class);
    Mockito.when(delegate.ping(Mockito.any(PingRequest.class)))
        .thenReturn(Mono.error(new RuntimeException("npe")));
    SocketAddress address =
        startUnifiedTransport(
            new PingServiceRpcServerHandler(delegate, Collections.<ThriftEventHandler>emptyList()));
    RpcClient raw = rawRocketClient(address);

    StepVerifier.create(
            raw.singleRequestSingleResponse(
                pingRawRequest(
                    TProtocolType.TCompact,
                    "ping",
                    RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE,
                    PingResponse::read0),
                RpcOptions.EMPTY))
        .assertNext(
            response -> {
              assertTrue(response.getException() instanceof TApplicationException);
              assertTrue(
                  response.getException().getMessage().contains("npe"),
                  "got " + response.getException());
              assertFalse(response.getHeaders().containsKey("ex"));
            })
        .verifyComplete();
  }

  @Test
  public void rocketDeclaredErrorStaysDeclared() throws Exception {
    PingService.Reactive delegate = Mockito.mock(PingService.Reactive.class);
    Mockito.when(delegate.pingException(Mockito.any(PingRequest.class)))
        .thenReturn(Mono.error(new CustomException.Builder().setMessage("custom: hello").build()));
    SocketAddress address =
        startUnifiedTransport(
            new PingServiceRpcServerHandler(delegate, Collections.<ThriftEventHandler>emptyList()));
    RpcClient raw = rawRocketClient(address);

    StepVerifier.create(
            raw.singleRequestSingleResponse(
                pingRawRequest(
                    TProtocolType.TCompact,
                    "pingException",
                    RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE,
                    PingResponse::read0),
                RpcOptions.EMPTY))
        .assertNext(
            response -> {
              // The raw client does not decode declared errors (typed-client scope, covered by
              // unifiedDeclaredErrorPreserved); the Rocket contract is a normal payload, not a
              // rejection: no ex, exception present.
              assertFalse(response.getHeaders().containsKey("ex"));
              assertTrue(response.getException() instanceof TApplicationException);
            })
        .verifyComplete();
  }

  @Test
  public void rocketLateStreamErrorStaysInBand() throws Exception {
    PingService.Reactive delegate = Mockito.mock(PingService.Reactive.class);
    Mockito.when(delegate.streamOfPings(Mockito.any(PingRequest.class), Mockito.anyInt()))
        .thenReturn(
            Flux.concat(
                Flux.just(new PingResponse.Builder().setResponse("v").build()),
                Flux.error(OverloadedException.host("late"))));
    SocketAddress address =
        startUnifiedTransport(
            new PingServiceRpcServerHandler(delegate, Collections.<ThriftEventHandler>emptyList()));
    RpcClient raw = rawRocketClient(address);

    StepVerifier.create(
            raw.singleRequestStreamingResponse(
                pingRawRequest(
                    TProtocolType.TCompact,
                    "streamOfPings",
                    RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE,
                    PingResponse::read0),
                RpcOptions.EMPTY))
        .assertNext(first -> assertNull(first.getException(), "value item must carry no error"))
        .assertNext(
            second ->
                assertNull(
                    second.getException(),
                    "late error must stay in band as a normal terminal, not a REJECTED item"))
        .assertNext(
            third -> {
              // The late error body arrives in band with its message, wrapped as INTERNAL_ERROR
              // like any mid-stream error: only the initial error may preserve the overload.
              assertTrue(third.getException() instanceof TApplicationException);
              assertTrue(
                  third.getException().getMessage().contains("late"),
                  "got " + third.getException().getMessage());
            })
        .verifyComplete();
  }

  @Test
  public void rocketStreamHandlerErrorStaysInBand() throws Exception {
    // A handler failure before the first element is a normal INTERNAL_ERROR payload, not an
    // ERROR frame: the terminal carries no exception, and the error body arrives in band with
    // its message (a REJECTED frame would surface as a single item with the bare reason name).
    PingService.Reactive delegate = Mockito.mock(PingService.Reactive.class);
    Mockito.when(delegate.streamOfPings(Mockito.any(PingRequest.class), Mockito.anyInt()))
        .thenReturn(Flux.error(new RuntimeException("boom")));
    SocketAddress address =
        startUnifiedTransport(
            new PingServiceRpcServerHandler(delegate, Collections.<ThriftEventHandler>emptyList()));
    RpcClient raw = rawRocketClient(address);

    StepVerifier.create(
            raw.singleRequestStreamingResponse(
                pingRawRequest(
                    TProtocolType.TCompact,
                    "streamOfPings",
                    RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE,
                    PingResponse::read0),
                RpcOptions.EMPTY))
        .assertNext(first -> assertNull(first.getException()))
        .assertNext(
            second -> {
              assertTrue(second.getException() instanceof TApplicationException);
              assertEquals("boom", second.getException().getMessage());
            })
        .verifyComplete();
  }

  @Test
  public void rocketReactiveSyncThrowStaysInternal() throws Exception {
    // A reactive handler that throws synchronously still gets a normal INTERNAL_ERROR payload,
    // not a raw APPLICATION_ERROR: the generated handler defers the delegate call into the
    // mapped chain. The exception hooks fire exactly once, and the same connection serves the
    // next call.
    List<String> hooks = new ArrayList<>();
    List<Throwable> hookErrors = new ArrayList<>();
    RuntimeException boom = new RuntimeException("boom");
    PingService.Reactive delegate = Mockito.mock(PingService.Reactive.class);
    Mockito.when(delegate.ping(Mockito.any(PingRequest.class)))
        .thenThrow(boom)
        .thenReturn(Mono.just(new PingResponse.Builder().setResponse("ok").build()));
    SocketAddress address =
        startUnifiedTransport(
            new PingServiceRpcServerHandler(
                delegate, Collections.singletonList(recordingHandler(hooks, hookErrors))));
    PingService.Reactive client = typedRocketClient(address, TProtocolType.TCompact);

    StepVerifier.create(client.ping(new PingRequest.Builder().setRequest("hello").build()))
        .expectErrorSatisfies(
            error -> {
              // The typed Rocket client decodes in-band errors as UNKNOWN (client scope); the
              // payload contract is the preserved message, not the type code.
              assertTrue(error instanceof TApplicationException);
              assertTrue(error.getMessage().contains("boom"));
            })
        .verify();
    assertEquals(Arrays.asList("pre", "undeclared", "post"), hooks);
    assertEquals(3, hookErrors.size());
    assertSame(hookErrors.get(0), hookErrors.get(1));
    assertSame(hookErrors.get(1), hookErrors.get(2));
    assertSame(boom, hookErrors.get(0).getCause());

    StepVerifier.create(client.ping(new PingRequest.Builder().setRequest("hello").build()))
        .assertNext(response -> assertEquals("ok", response.getResponse()))
        .verifyComplete();
  }

  @Test
  public void rocketReactiveSyncThrowDeclaredStaysDeclared() throws Exception {
    // A synchronously thrown declared exception stays declared over Rocket.
    PingService.Reactive delegate = Mockito.mock(PingService.Reactive.class);
    Mockito.when(delegate.pingException(Mockito.any(PingRequest.class)))
        .thenThrow(new CustomException.Builder().setMessage("custom: sync").build());
    SocketAddress address =
        startUnifiedTransport(
            new PingServiceRpcServerHandler(delegate, Collections.<ThriftEventHandler>emptyList()));
    PingService.Reactive client = typedRocketClient(address, TProtocolType.TCompact);

    StepVerifier.create(client.pingException(new PingRequest.Builder().setRequest("x").build()))
        .expectErrorSatisfies(
            error -> {
              assertTrue(error instanceof CustomException);
              assertEquals("custom: sync", error.getMessage());
            })
        .verify();
  }

  @Test
  public void rocketSchedulerRejectionRejectsWithOverload() throws Exception {
    // A real scheduler rejection: the off-loop hop itself throws, the delegate never runs, and
    // the rejection reads as REJECTED OVERLOAD. The typed client sees LOADSHEDDING, and the
    // next call on the same server and client succeeds.
    PingService.Reactive delegate = Mockito.mock(PingService.Reactive.class);
    Mockito.when(delegate.ping(Mockito.any(PingRequest.class)))
        .thenReturn(Mono.just(new PingResponse.Builder().setResponse("ok").build()));
    RpcServerHandler handler =
        new PingServiceRpcServerHandler(delegate, Collections.<ThriftEventHandler>emptyList());
    SocketAddress address = startUnifiedTransport(sheddingOffLoop(handler));

    PingService.Reactive client = typedRocketClient(address, TProtocolType.TCompact);
    StepVerifier.create(client.ping(new PingRequest.Builder().setRequest("hello").build()))
        .expectErrorSatisfies(
            error -> {
              assertTrue(error instanceof TApplicationException);
              assertEquals(
                  TApplicationException.LOADSHEDDING, ((TApplicationException) error).getType());
            })
        .verify();
    Mockito.verify(delegate, Mockito.never()).ping(Mockito.any(PingRequest.class));

    RpcClient raw = rawRocketClient(address);
    StepVerifier.create(
            raw.singleRequestSingleResponse(
                pingRawRequest(
                    TProtocolType.TCompact,
                    "ping",
                    RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE,
                    PingResponse::read0),
                RpcOptions.EMPTY))
        .assertNext(response -> assertNull(response.getException()))
        .verifyComplete();
  }

  @Test
  public void rocketSchedulerRejectionRejectsStream() throws Exception {
    // A real scheduler rejection on a stream reads as REJECTED OVERLOAD, and the next call on
    // the same server succeeds.
    PingService.Reactive delegate = Mockito.mock(PingService.Reactive.class);
    Mockito.when(delegate.streamOfPings(Mockito.any(PingRequest.class), Mockito.anyInt()))
        .thenReturn(Flux.just(new PingResponse.Builder().setResponse("ok").build()));
    RpcServerHandler handler =
        new PingServiceRpcServerHandler(delegate, Collections.<ThriftEventHandler>emptyList());
    SocketAddress address = startUnifiedTransport(sheddingOffLoop(handler));
    RpcClient raw = rawRocketClient(address);

    StepVerifier.create(
            raw.singleRequestStreamingResponse(
                pingRawRequest(
                    TProtocolType.TCompact,
                    "streamOfPings",
                    RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE,
                    PingResponse::read0),
                RpcOptions.EMPTY))
        .assertNext(
            response -> {
              // The rejection item carries the rejection's reason name.
              assertTrue(response.getException() instanceof TApplicationException);
              assertEquals("OVERLOAD", response.getException().getMessage());
            })
        .verifyComplete();
    Mockito.verify(delegate, Mockito.never())
        .streamOfPings(Mockito.any(PingRequest.class), Mockito.anyInt());

    StepVerifier.create(
            raw.singleRequestStreamingResponse(
                pingRawRequest(
                    TProtocolType.TCompact,
                    "streamOfPings",
                    RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE,
                    PingResponse::read0),
                RpcOptions.EMPTY))
        .assertNext(first -> assertNull(first.getException(), "value item must carry no error"))
        .assertNext(second -> assertNull(second.getException(), "terminal must carry no error"))
        .verifyComplete();
  }

  @Test
  public void rocketSyncOverloadRejectsStream() throws Exception {
    // A synchronous explicit overload before the first element converts: the initial error
    // preserves the overload, and the transport rejects it.
    PingService.Reactive delegate = Mockito.mock(PingService.Reactive.class);
    Mockito.when(delegate.streamOfPings(Mockito.any(PingRequest.class), Mockito.anyInt()))
        .thenThrow(OverloadedException.host("full"));
    SocketAddress address =
        startUnifiedTransport(
            new PingServiceRpcServerHandler(delegate, Collections.<ThriftEventHandler>emptyList()));
    RpcClient raw = rawRocketClient(address);

    StepVerifier.create(
            raw.singleRequestStreamingResponse(
                pingRawRequest(
                    TProtocolType.TCompact,
                    "streamOfPings",
                    RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE,
                    throwingReader()),
                RpcOptions.EMPTY))
        .assertNext(
            response -> {
              // The initial error preserved the explicit overload and the transport rejected
              // it: the item carries the rejection's reason name.
              assertTrue(response.getException() instanceof TApplicationException);
              assertEquals("OVERLOAD", response.getException().getMessage());
            })
        .verifyComplete();
  }

  @Test
  public void rocketThrowingPreReadStaysInternal() throws Exception {
    // A throwing read hook gets a normal INTERNAL_ERROR payload, not a raw error, and the next
    // call succeeds.
    SocketAddress address = startUnifiedTransport(throwingPreReadHandler());
    throwingPreReadStaysInternal(rawRocketClient(address), false);
  }

  @Test
  public void rocketContextChainFailureStaysInternal() throws Exception {
    SocketAddress address = startUnifiedTransport(failingContextChainHandler());
    contextChainFailureStaysInternal(rawRocketClient(address), false);
  }

  @Test
  public void rocketMalformedArgsStayInternal() throws Exception {
    SocketAddress address = startUnifiedTransport(okHandler());
    malformedArgsStayInternal(rawRocketClient(address), false);
  }

  @Test
  public void rocketThrowingPostReadStaysInternal() throws Exception {
    SocketAddress address = startUnifiedTransport(throwingPostReadHandler());
    throwingPostReadStaysInternal(rawRocketClient(address), false);
  }

  @Test
  public void rocketUnknownServiceRejectsAsInvalid() throws Exception {
    // A name no handler serves reports UNKNOWN_METHOD in an INVALID frame, and the connection
    // keeps serving the registered methods on the same server.
    PingService.Reactive delegate = Mockito.mock(PingService.Reactive.class);
    Mockito.when(delegate.ping(Mockito.any(PingRequest.class)))
        .thenReturn(Mono.just(new PingResponse.Builder().setResponse("ok").build()));

    SocketAddress address =
        startUnifiedTransport(
            new CompositeRpcServerHandler(
                Collections.singletonList(
                    new PingServiceRpcServerHandler(
                        delegate, Collections.<ThriftEventHandler>emptyList()))));
    RpcClient raw = rawRocketClient(address);

    StepVerifier.create(
            raw.singleRequestSingleResponse(
                pingRawRequest(TProtocolType.TCompact, "NoSuchService.ping"), RpcOptions.EMPTY))
        .assertNext(
            response -> {
              assertEquals("25", response.getHeaders().get("ex"));
              assertTrue(response.getException() instanceof TApplicationException);
              assertEquals(
                  TApplicationException.UNKNOWN_METHOD,
                  ((TApplicationException) response.getException()).getType());
            })
        .verifyComplete();

    Mockito.verify(delegate, Mockito.never()).ping(Mockito.any(PingRequest.class));

    StepVerifier.create(
            raw.singleRequestSingleResponse(
                pingRawRequest(TProtocolType.TCompact, "ping", PingResponse::read0),
                RpcOptions.EMPTY))
        .assertNext(response -> assertNull(response.getException(), "next call must succeed"))
        .verifyComplete();
  }

  @Test
  public void rocketBadMetadataRejectsAsInvalid() throws Exception {
    // A request without a method name cannot be routed: INVALID REQUEST_PARSING_FAILURE, and
    // the next call on the same connection succeeds.
    SocketAddress address = startUnifiedTransport(okHandler());
    RpcClient raw = rawRocketClient(address);

    StepVerifier.create(
            raw.singleRequestSingleResponse(
                pingRawRequest(TProtocolType.TCompact, null), RpcOptions.EMPTY))
        .assertNext(
            response -> {
              assertEquals("28", response.getHeaders().get("ex"));
              assertTrue(response.getException() instanceof TApplicationException);
              assertEquals(
                  TApplicationException.UNSUPPORTED_CLIENT_TYPE,
                  ((TApplicationException) response.getException()).getType());
            })
        .verifyComplete();

    StepVerifier.create(
            raw.singleRequestSingleResponse(
                pingRawRequest(TProtocolType.TCompact, "ping", PingResponse::read0),
                RpcOptions.EMPTY))
        .assertNext(response -> assertNull(response.getException(), "next call must succeed"))
        .verifyComplete();
  }

  @Test
  public void rocketRealSchedulerRejectionRejectsWithOverload() throws Exception {
    // The rejection travels the generated off-loop hop itself: with the real scheduler
    // rejecting, the delegate never runs and the raw frame reads as REJECTED OVERLOAD.
    // Restoring the scheduler lets the next call succeed. A codegen change that drops the hop
    // turns this red.
    PingService.Reactive delegate = Mockito.mock(PingService.Reactive.class);
    Mockito.when(delegate.ping(Mockito.any(PingRequest.class)))
        .thenReturn(Mono.just(new PingResponse.Builder().setResponse("ok").build()));

    SocketAddress address =
        startUnifiedTransport(
            new PingServiceRpcServerHandler(delegate, Collections.<ThriftEventHandler>emptyList()));
    RpcClient raw = rawRocketClient(address);

    try (RejectingOffLoop ignored = new RejectingOffLoop()) {
      StepVerifier.create(
              raw.singleRequestSingleResponse(
                  pingRawRequest(TProtocolType.TCompact, "ping"), RpcOptions.EMPTY))
          .assertNext(
              response -> {
                assertEquals("1", response.getHeaders().get("ex"));
                assertFalse(response.getHeaders().containsKey("uex"));
                assertTrue(response.getException() instanceof TApplicationException);
                assertEquals(
                    TApplicationException.LOADSHEDDING,
                    ((TApplicationException) response.getException()).getType());
              })
          .verifyComplete();

      Mockito.verify(delegate, Mockito.never()).ping(Mockito.any(PingRequest.class));
    }

    StepVerifier.create(
            raw.singleRequestSingleResponse(
                pingRawRequest(TProtocolType.TCompact, "ping", PingResponse::read0),
                RpcOptions.EMPTY))
        .assertNext(response -> assertNull(response.getException(), "next call must succeed"))
        .verifyComplete();
  }

  @Test
  public void rocketRealSchedulerRejectionRejectsStream() throws Exception {
    // The same real-hop rejection on a stream reads as REJECTED OVERLOAD, and the next call on
    // the same server succeeds.
    PingService.Reactive delegate = Mockito.mock(PingService.Reactive.class);
    Mockito.when(delegate.streamOfPings(Mockito.any(PingRequest.class), Mockito.anyInt()))
        .thenReturn(Flux.just(new PingResponse.Builder().setResponse("ok").build()));

    SocketAddress address =
        startUnifiedTransport(
            new PingServiceRpcServerHandler(delegate, Collections.<ThriftEventHandler>emptyList()));
    RpcClient raw = rawRocketClient(address);

    try (RejectingOffLoop ignored = new RejectingOffLoop()) {
      StepVerifier.create(
              raw.singleRequestStreamingResponse(
                  pingRawRequest(
                      TProtocolType.TCompact,
                      "streamOfPings",
                      RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE,
                      PingResponse::read0),
                  RpcOptions.EMPTY))
          .assertNext(
              response -> {
                assertTrue(response.getException() instanceof TApplicationException);
                assertEquals("OVERLOAD", response.getException().getMessage());
              })
          .verifyComplete();

      Mockito.verify(delegate, Mockito.never())
          .streamOfPings(Mockito.any(PingRequest.class), Mockito.anyInt());
    }

    StepVerifier.create(
            raw.singleRequestStreamingResponse(
                pingRawRequest(
                    TProtocolType.TCompact,
                    "streamOfPings",
                    RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE,
                    PingResponse::read0),
                RpcOptions.EMPTY))
        .assertNext(first -> assertNull(first.getException(), "value item must carry no error"))
        .assertNext(second -> assertNull(second.getException(), "terminal must carry no error"))
        .verifyComplete();
  }

  @Test
  public void rocketSyncThrowStreamStaysInBand() throws Exception {
    // The synchronous-throw control for rocketStreamHandlerErrorStaysInBand: a handler that
    // throws before its first element flows through the same initial-error mapping, so the
    // error body arrives in band with its message. Unlike the async failure there is no stream
    // yet, so the error is the only item; a REJECTED frame would carry the bare reason name.
    PingService.Reactive delegate = Mockito.mock(PingService.Reactive.class);
    Mockito.when(delegate.streamOfPings(Mockito.any(PingRequest.class), Mockito.anyInt()))
        .thenThrow(new RuntimeException("sync-boom"));

    SocketAddress address =
        startUnifiedTransport(
            new PingServiceRpcServerHandler(delegate, Collections.<ThriftEventHandler>emptyList()));
    RpcClient raw = rawRocketClient(address);

    StepVerifier.create(
            raw.singleRequestStreamingResponse(
                pingRawRequest(
                    TProtocolType.TCompact,
                    "streamOfPings",
                    RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE,
                    PingResponse::read0),
                RpcOptions.EMPTY))
        .assertNext(
            only -> {
              assertTrue(only.getException() instanceof TApplicationException);
              assertEquals("sync-boom", only.getException().getMessage());
            })
        .verifyComplete();
  }

  /**
   * Swaps the real server off-loop scheduler for one that rejects every task, until close. The
   * generated handler resolves this scheduler on its off-loop hop, so a request made inside the
   * block fails the way a saturated pool fails it.
   */
  /**
   * Test-only reflection swap of RpcResources.INSTANCE.holder.offLoopScheduler for a rejecting
   * scheduler; close() restores the original it pinned.
   */
  private static final class RejectingOffLoop implements AutoCloseable {
    private final Object holder;
    private final Field schedulerField;
    private final ThriftScheduler original;

    RejectingOffLoop() throws Exception {
      Field instance = RpcResources.class.getDeclaredField("INSTANCE");
      instance.setAccessible(true);
      Field holderField = RpcResources.class.getDeclaredField("holder");
      holderField.setAccessible(true);
      holder = holderField.get(instance.get(null));

      schedulerField = holder.getClass().getDeclaredField("offLoopScheduler");
      schedulerField.setAccessible(true);
      original = (ThriftScheduler) schedulerField.get(holder);

      ThriftScheduler rejecting =
          new ThriftScheduler() {
            private RejectedExecutionException full() {
              return new RejectedExecutionException("full");
            }

            @Override
            public Disposable schedule(Runnable task) {
              throw full();
            }

            @Override
            public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
              throw full();
            }

            @Override
            public Worker createWorker() {
              // The generated off-loop hop schedules through a worker, so the worker itself
              // rejects; creating it stays cheap and never throws at subscription time.
              return new Worker() {
                @Override
                public Disposable schedule(Runnable task) {
                  throw full();
                }

                @Override
                public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
                  throw full();
                }

                @Override
                public Disposable schedulePeriodically(
                    Runnable task, long initialDelay, long period, TimeUnit unit) {
                  throw full();
                }

                @Override
                public void dispose() {}

                @Override
                public boolean isDisposed() {
                  return false;
                }
              };
            }

            @Override
            public Map<String, Long> getStats() {
              return Collections.emptyMap();
            }

            @Override
            public ExecutorService getExecutor() {
              return original.getExecutor();
            }

            @Override
            public void dispose() {}

            @Override
            public boolean isDisposed() {
              return false;
            }
          };

      schedulerField.set(holder, rejecting);
    }

    @Override
    public void close() throws Exception {
      schedulerField.set(holder, original);
      assertSame(original, schedulerField.get(holder));
    }
  }

  @Test
  public void unifiedThrowingPreReadStaysInternal() throws Exception {
    SocketAddress address = startUnifiedTransport(throwingPreReadHandler());
    throwingPreReadStaysInternal(rawClient(address), true);
  }

  @Test
  public void unifiedContextChainFailureStaysInternal() throws Exception {
    SocketAddress address = startUnifiedTransport(failingContextChainHandler());
    contextChainFailureStaysInternal(rawClient(address), true);
  }

  @Test
  public void unifiedMalformedArgsStayInternal() throws Exception {
    SocketAddress address = startUnifiedTransport(okHandler());
    malformedArgsStayInternal(rawClient(address), true);
  }

  @Test
  public void unifiedThrowingPostReadStaysInternal() throws Exception {
    SocketAddress address = startUnifiedTransport(throwingPostReadHandler());
    throwingPostReadStaysInternal(rawClient(address), true);
  }

  private static RpcServerHandler okHandler() {
    PingService.Reactive delegate = Mockito.mock(PingService.Reactive.class);
    Mockito.when(delegate.ping(Mockito.any(PingRequest.class)))
        .thenReturn(Mono.just(new PingResponse.Builder().setResponse("ok").build()));
    return new PingServiceRpcServerHandler(delegate, Collections.<ThriftEventHandler>emptyList());
  }

  private static RpcServerHandler failingContextChainHandler() {
    AtomicBoolean armed = new AtomicBoolean(true);
    ThriftEventHandler failingGetContext =
        new ThriftEventHandler() {
          @Override
          public Object getContext(String methodName, RequestContext requestContext) {
            if (armed.getAndSet(false)) {
              throw new RuntimeException("no context");
            }
            return null;
          }
        };

    PingService.Reactive delegate = Mockito.mock(PingService.Reactive.class);
    Mockito.when(delegate.ping(Mockito.any(PingRequest.class)))
        .thenReturn(Mono.just(new PingResponse.Builder().setResponse("ok").build()));

    return new PingServiceRpcServerHandler(delegate, Collections.singletonList(failingGetContext));
  }

  private static void contextChainFailureStaysInternal(RpcClient raw, boolean exactType)
      throws Exception {
    // A ContextChain construction failure gets a normal INTERNAL_ERROR payload naming the
    // construction step, not a raw error, and the next call succeeds.
    StepVerifier.create(
            raw.singleRequestSingleResponse(
                pingRawRequest(
                    TProtocolType.TCompact,
                    "ping",
                    RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE,
                    PingResponse::read0),
                RpcOptions.EMPTY))
        .assertNext(
            response -> {
              assertTrue(response.getException() instanceof TApplicationException);
              if (exactType) {
                assertEquals(
                    TApplicationException.INTERNAL_ERROR,
                    ((TApplicationException) response.getException()).getType());
              }
              assertTrue(
                  response.getException().getMessage().contains("during context chain"),
                  "got " + response.getException().getMessage());
              assertFalse(response.getHeaders().containsKey("ex"));
            })
        .verifyComplete();

    StepVerifier.create(
            raw.singleRequestSingleResponse(
                pingRawRequest(
                    TProtocolType.TCompact,
                    "ping",
                    RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE,
                    PingResponse::read0),
                RpcOptions.EMPTY))
        .assertNext(response -> assertNull(response.getException()))
        .verifyComplete();
  }

  private static ClientRequestPayload<Object> malformedRawRequest() {
    return ClientRequestPayload.create(
        "PingService",
        out -> out.writeByte((byte) 0xFF),
        PingResponse::read0,
        Collections.emptyMap(),
        new RequestRpcMetadata.Builder()
            .setName("ping")
            .setKind(RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE)
            .setProtocol(TProtocolType.TCompact.getProtocolId())
            .setOtherMetadata(Collections.emptyMap())
            .build(),
        Collections.emptyMap());
  }

  private static void malformedArgsStayInternal(RpcClient raw, boolean exactType) throws Exception {
    // Undecodable args fail argument parsing inside the generated handler: a normal
    // INTERNAL_ERROR payload naming the processing step, and the next call succeeds.
    StepVerifier.create(raw.singleRequestSingleResponse(malformedRawRequest(), RpcOptions.EMPTY))
        .assertNext(
            response -> {
              assertTrue(response.getException() instanceof TApplicationException);
              if (exactType) {
                assertEquals(
                    TApplicationException.INTERNAL_ERROR,
                    ((TApplicationException) response.getException()).getType());
              }
              assertTrue(
                  response.getException().getMessage().contains("processing ping"),
                  "got " + response.getException().getMessage());
              assertFalse(response.getHeaders().containsKey("ex"));
            })
        .verifyComplete();

    StepVerifier.create(
            raw.singleRequestSingleResponse(
                pingRawRequest(
                    TProtocolType.TCompact,
                    "ping",
                    RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE,
                    PingResponse::read0),
                RpcOptions.EMPTY))
        .assertNext(response -> assertNull(response.getException()))
        .verifyComplete();
  }

  private static RpcServerHandler throwingPostReadHandler() {
    AtomicBoolean armed = new AtomicBoolean(true);
    ThriftEventHandler throwingPostRead =
        new ThriftEventHandler() {
          @Override
          public void postRead(Object context, String methodName, Object[] args) {
            if (armed.getAndSet(false)) {
              throw new RuntimeException("post-read hook");
            }
          }
        };

    PingService.Reactive delegate = Mockito.mock(PingService.Reactive.class);
    Mockito.when(delegate.ping(Mockito.any(PingRequest.class)))
        .thenReturn(Mono.just(new PingResponse.Builder().setResponse("ok").build()));

    return new PingServiceRpcServerHandler(delegate, Collections.singletonList(throwingPostRead));
  }

  private static void throwingPostReadStaysInternal(RpcClient raw, boolean exactType)
      throws Exception {
    // A throwing postRead hook gets a normal INTERNAL_ERROR payload, not a raw error, and the
    // next call succeeds.
    StepVerifier.create(
            raw.singleRequestSingleResponse(
                pingRawRequest(
                    TProtocolType.TCompact,
                    "ping",
                    RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE,
                    PingResponse::read0),
                RpcOptions.EMPTY))
        .assertNext(
            response -> {
              assertTrue(response.getException() instanceof TApplicationException);
              if (exactType) {
                assertEquals(
                    TApplicationException.INTERNAL_ERROR,
                    ((TApplicationException) response.getException()).getType());
              }
              assertTrue(
                  response.getException().getMessage().contains("post-read hook"),
                  "got " + response.getException().getMessage());
              assertFalse(response.getHeaders().containsKey("ex"));
            })
        .verifyComplete();

    StepVerifier.create(
            raw.singleRequestSingleResponse(
                pingRawRequest(
                    TProtocolType.TCompact,
                    "ping",
                    RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE,
                    PingResponse::read0),
                RpcOptions.EMPTY))
        .assertNext(response -> assertNull(response.getException()))
        .verifyComplete();
  }

  private static RpcServerHandler throwingPreReadHandler() {
    AtomicBoolean armed = new AtomicBoolean(true);
    ThriftEventHandler throwingPreRead =
        new ThriftEventHandler() {
          @Override
          public void preRead(Object context, String methodName) {
            if (armed.getAndSet(false)) {
              throw new RuntimeException("hook");
            }
          }
        };
    PingService.Reactive delegate = Mockito.mock(PingService.Reactive.class);
    Mockito.when(delegate.ping(Mockito.any(PingRequest.class)))
        .thenReturn(Mono.just(new PingResponse.Builder().setResponse("ok").build()));
    return new PingServiceRpcServerHandler(delegate, Collections.singletonList(throwingPreRead));
  }

  private static void throwingPreReadStaysInternal(RpcClient raw, boolean exactType)
      throws Exception {
    StepVerifier.create(
            raw.singleRequestSingleResponse(
                pingRawRequest(
                    TProtocolType.TCompact,
                    "ping",
                    RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE,
                    PingResponse::read0),
                RpcOptions.EMPTY))
        .assertNext(
            response -> {
              assertTrue(response.getException() instanceof TApplicationException);
              if (exactType) {
                assertEquals(
                    TApplicationException.INTERNAL_ERROR,
                    ((TApplicationException) response.getException()).getType());
              }
              assertTrue(
                  response.getException().getMessage().contains("hook"),
                  "got " + response.getException().getMessage());
              assertFalse(response.getHeaders().containsKey("ex"));
            })
        .verifyComplete();

    StepVerifier.create(
            raw.singleRequestSingleResponse(
                pingRawRequest(
                    TProtocolType.TCompact,
                    "ping",
                    RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE,
                    PingResponse::read0),
                RpcOptions.EMPTY))
        .assertNext(response -> assertNull(response.getException()))
        .verifyComplete();
  }

  @Test
  public void rocketOverloadFiresExceptionHooksOnce() throws Exception {
    List<String> hooks = new ArrayList<>();
    List<Throwable> hookErrors = new ArrayList<>();
    TApplicationException shed = OverloadedException.host("busy");
    SocketAddress address =
        startUnifiedTransport(
            new PingServiceRpcServerHandler(
                Mockito.mock(PingService.class),
                Arrays.asList(
                    throwingPreprocessHandler(shed), recordingHandler(hooks, hookErrors))));
    RpcClient raw = rawRocketClient(address);

    StepVerifier.create(
            raw.singleRequestSingleResponse(
                pingRawRequest(TProtocolType.TCompact, "ping"), RpcOptions.EMPTY))
        .assertNext(
            response -> {
              assertEquals("1", response.getHeaders().get("ex"));
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

  private void runRocketPreprocessShed(
      TProtocolType protocol, TApplicationException shed, String expectedEx) throws Exception {
    SocketAddress address = startUnifiedTransport(sheddingServerHandler(shed));
    RpcClient raw = rawRocketClient(address);

    StepVerifier.create(
            raw.singleRequestSingleResponse(pingRawRequest(protocol, "ping"), RpcOptions.EMPTY))
        .assertNext(
            response -> {
              assertEquals(expectedEx, response.getHeaders().get("ex"));
              assertFalse(response.getHeaders().containsKey("uex"));
              assertTrue(response.getException() instanceof TApplicationException);
              assertEquals(
                  TApplicationException.LOADSHEDDING,
                  ((TApplicationException) response.getException()).getType());
            })
        .verifyComplete();
  }

  private static RpcClientFactory rocketClientFactory() {
    return RpcClientFactory.builder()
        .setThriftClientConfig(
            new ThriftClientConfig()
                .setDisableSSL(false)
                .setEnableJdkSsl(false)
                .setCertFile(TestCertificateUtil.getCertFilePath())
                .setKeyFile(TestCertificateUtil.getKeyFilePath())
                .setCAFile(TestCertificateUtil.getCAFilePath())
                .setRequestTimeout(Duration.succinctDuration(30, TimeUnit.SECONDS)))
        .setDisableRSocket(false)
        .setDisableLoadBalancing(true)
        .setDisableReconnectingClient(true)
        .build();
  }

  private RpcClient rawRocketClient(SocketAddress address) {
    RpcClientFactory factory = rocketClientFactory();
    rawBinding = factory.createRpcClientBinding(address);
    RpcClient client = rawBinding.acquire().block();
    assertTrue(client != null);
    return client;
  }

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

  private PingService.Reactive typedRocketClient(SocketAddress address, TProtocolType protocol) {
    RpcClientFactory factory = rocketClientFactory();
    typedClient =
        PingService.Reactive.clientBuilder()
            .setProtocolId(protocol.getProtocolId())
            .build(factory, address);
    return typedClient;
  }

  private static ClientRequestPayload<Object> pingRawRequest(
      TProtocolType protocol, String method) {
    return pingRawRequest(
        protocol, method, RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE, throwingReader());
  }

  private static ClientRequestPayload<Object> pingRawRequest(
      TProtocolType protocol, String method, Reader<Object> reader) {
    return pingRawRequest(protocol, method, RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE, reader);
  }

  private static ClientRequestPayload<Object> pingRawRequest(
      TProtocolType protocol, String method, RpcKind kind, Reader<Object> reader) {
    return ClientRequestPayload.create(
        "PingService",
        pingArgsWriter("hello"),
        reader,
        Collections.emptyMap(),
        new RequestRpcMetadata.Builder()
            .setName(method)
            .setKind(kind)
            .setProtocol(protocol.getProtocolId())
            .setOtherMetadata(Collections.emptyMap())
            .build(),
        Collections.emptyMap());
  }

  private static Reader<Object> throwingReader() {
    return in -> {
      throw new IllegalStateException("unexpected body read");
    };
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
    return sheddingServerHandler(
        new TApplicationException(TApplicationException.LOADSHEDDING, message));
  }

  private static RpcServerHandler sheddingServerHandler(TApplicationException shed) {
    return new PingServiceRpcServerHandler(
        Mockito.mock(PingService.class),
        Collections.singletonList(throwingPreprocessHandler(shed)));
  }

  /**
   * Runs the handler on a scheduler that rejects one task, then runs the rest on the common pool:
   * the first subscription throws RejectedExecutionException before the delegate runs, and the next
   * call on the same server succeeds.
   */
  private static RpcServerHandler sheddingOffLoop(RpcServerHandler handler) {
    AtomicBoolean armed = new AtomicBoolean(true);
    Scheduler scheduler =
        Schedulers.fromExecutor(
            task -> {
              if (armed.getAndSet(false)) {
                throw new RejectedExecutionException("full");
              }
              ForkJoinPool.commonPool().execute(task);
            });
    return new RpcServerHandler() {
      @Override
      public Mono<ServerResponsePayload> singleRequestSingleResponse(ServerRequestPayload payload) {
        return handler.singleRequestSingleResponse(payload).subscribeOn(scheduler);
      }

      @Override
      public Flux<ServerResponsePayload> singleRequestStreamingResponse(
          ServerRequestPayload payload) {
        return handler.singleRequestStreamingResponse(payload).subscribeOn(scheduler);
      }
    };
  }

  private static ThriftEventHandler throwingPreprocessHandler(TApplicationException shed) {
    return new ThriftEventHandler() {
      @Override
      public void preprocess(Object context, String methodName) throws TException {
        throw shed;
      }
    };
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
