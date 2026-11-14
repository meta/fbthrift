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

import com.facebook.nifty.core.RequestContext;
import com.facebook.swift.service.StreamEventHandler;
import com.facebook.swift.service.ThriftEventHandler;
import com.facebook.swift.service.ThriftServerConfig;
import com.facebook.thrift.client.RpcClientFactory;
import com.facebook.thrift.client.ThriftClientConfig;
import com.facebook.thrift.example.ping.CustomException;
import com.facebook.thrift.example.ping.ExtendedPing;
import com.facebook.thrift.example.ping.PingRequest;
import com.facebook.thrift.example.ping.PingResponse;
import com.facebook.thrift.model.ResponseAndStream;
import com.facebook.thrift.server.RpcServerHandler;
import com.facebook.thrift.testservices.ExtendedReactivePingService;
import com.facebook.thrift.util.RpcServerUtils;
import com.facebook.thrift.util.SPINiftyMetrics;
import com.facebook.thrift.util.resources.RpcResources;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import io.netty.buffer.ByteBuf;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetectorFactory;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.apache.thrift.ProtocolId;
import org.apache.thrift.TApplicationException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * Java-to-Java streaming integration tests over the real unified transport (RSocket via ALPN):
 * plain streams (dispatched through the child-to-parent inheritance path), first-response streams,
 * declared/undeclared errors mid-stream and before any element, client cancel, and bounded client
 * demand. Every test runs on COMPACT and BINARY.
 *
 * <p>Dropping the client connection mid-stream drives the server CANCEL terminal hook (then done);
 * {@code connectionDropFiresCancelTerminal} pins it. {@code StreamConnectionCloseTest} covers the
 * same teardown at the raw-transport level.
 */
public class ExtendedPingStreamIntegrationTest {

  static Stream<Arguments> data() {
    return Stream.of(Arguments.of(ProtocolId.COMPACT), Arguments.of(ProtocolId.BINARY));
  }

  /**
   * A {@link ResourceLeakDetector} that records every detected leak instead of only logging it, so
   * the suite's {@code @AfterAll} can fail on any {@code ByteBuf} leak in these flows.
   *
   * <p>Installed via {@link ResourceLeakDetectorFactory} FIRST in {@code @BeforeAll}, before
   * anything (including cert setup, whose Netty SSL helpers reference {@code ByteBuf}) can trigger
   * {@code AbstractByteBuf}'s static initializer: Netty binds the factory's detector into that
   * static once, at class-load (verified in the 4.2 bytecode), so a later install would silently
   * record nothing. The calibration below proves the install won.
   */
  private static final Queue<String> RECORDED_LEAKS = new ConcurrentLinkedQueue<>();

  private static class RecordingLeakDetector<T> extends ResourceLeakDetector<T> {
    RecordingLeakDetector(Class<?> resourceType, int samplingInterval) {
      super(resourceType, samplingInterval);
    }

    @Override
    protected void reportTracedLeak(String resourceType, String records) {
      super.reportTracedLeak(resourceType, records);
      RECORDED_LEAKS.add(resourceType + " traced: " + records);
    }

    @Override
    protected void reportUntracedLeak(String resourceType) {
      super.reportUntracedLeak(resourceType);
      RECORDED_LEAKS.add(resourceType + " untraced");
    }
  }

  private static class RecordingLeakDetectorFactory extends ResourceLeakDetectorFactory {
    @Override
    public <T> ResourceLeakDetector<T> newResourceLeakDetector(
        Class<T> resource, int samplingInterval) {
      return new RecordingLeakDetector<>(resource, samplingInterval);
    }

    @Override
    @SuppressWarnings("deprecation")
    public <T> ResourceLeakDetector<T> newResourceLeakDetector(
        Class<T> resource, int samplingInterval, long maxActive) {
      return new RecordingLeakDetector<>(resource, samplingInterval);
    }
  }

  private static String installedDetectorDetail() throws Exception {
    java.lang.reflect.Field detectorField =
        Class.forName("io.netty.buffer.AbstractByteBuf").getDeclaredField("leakDetector");
    detectorField.setAccessible(true);
    return "detector="
        + detectorField.get(null).getClass().getName()
        + " level="
        + ResourceLeakDetector.getLevel();
  }

  private UnifiedServerTransport transport;
  private SocketAddress serverAddress;
  private UnifiedServerTransport backendTransport;
  private SocketAddress backendAddress;
  private ThriftServerConfig serverConfig;

  @BeforeAll
  public static void setUpClass() throws Exception {
    // Leak detector first: cert setup's Netty SSL helpers reference ByteBuf, which would
    // otherwise bind the default detector before the recording factory installs.
    ResourceLeakDetectorFactory.setResourceLeakDetectorFactory(new RecordingLeakDetectorFactory());
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    TestCertificateUtil.initialize();
    calibrateLeakDetection();
  }

  /**
   * Proves the recording detector is live for this suite: leaks one buffer through the same
   * allocator the tests use and requires it to be recorded. (Verified against Netty 4.2 sources:
   * pending leak reports are drained on subsequent tracked allocations -- hence the
   * allocate/release churn.)
   */
  private static void calibrateLeakDetection() throws Exception {
    ByteBuf probe = RpcResources.getByteBufAllocator().directBuffer(64);
    probe.writeZero(64);
    probe = null;
    String detectorDetail = installedDetectorDetail();
    long deadline = System.currentTimeMillis() + 15000;
    while (RECORDED_LEAKS.isEmpty() && System.currentTimeMillis() < deadline) {
      churnAllocator();
      Thread.sleep(200);
    }
    Assertions.assertFalse(
        RECORDED_LEAKS.isEmpty(),
        "leak-detection calibration failed: the deliberately leaked buffer was never recorded ("
            + detectorDetail
            + ")");
    RECORDED_LEAKS.clear();
  }

  /**
   * GC plus allocate/release churn: released buffers are not leaks, but each tracked allocation
   * drains Netty's pending leak-report queue so unreleased buffers surface.
   */
  private static void churnAllocator() {
    System.gc();
    for (int i = 0; i < 32; i++) {
      ByteBuf tmp = RpcResources.getByteBufAllocator().directBuffer(64);
      tmp.release();
    }
  }

  private static void collectGarbage() throws Exception {
    for (int i = 0; i < 5; i++) {
      churnAllocator();
      Thread.sleep(200);
    }
  }

  @AfterAll
  public static void tearDownClass() throws Exception {
    collectGarbage();
    Assertions.assertTrue(
        RECORDED_LEAKS.isEmpty(), () -> "Netty ByteBuf leaks detected: " + RECORDED_LEAKS);
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.SIMPLE);
    TestCertificateUtil.cleanup();
  }

  @BeforeEach
  public void setUp() throws Exception {
    serverConfig =
        new ThriftServerConfig()
            .setSslEnabled(true)
            .setEnableJdkSsl(false)
            .setKeyFile(TestCertificateUtil.getKeyFilePath())
            .setCertFile(TestCertificateUtil.getCertFilePath())
            .setCAFile(TestCertificateUtil.getCAFilePath())
            .setEnableAlpn(true)
            .setTaskExpirationTimeout(Duration.valueOf("60s"))
            .setConnectionLimit(10);
  }

  @AfterEach
  public void tearDown() {
    if (transport != null) {
      transport.dispose();
      transport = null;
    }
    if (backendTransport != null) {
      backendTransport.dispose();
      backendTransport = null;
    }
  }

  private RpcClientFactory createRSocketClientFactory() {
    ThriftClientConfig clientConfig =
        new ThriftClientConfig()
            .setDisableSSL(false)
            .setEnableJdkSsl(false)
            .setCertFile(TestCertificateUtil.getCertFilePath())
            .setKeyFile(TestCertificateUtil.getKeyFilePath())
            .setCAFile(TestCertificateUtil.getCAFilePath())
            .setRequestTimeout(Duration.succinctDuration(30, TimeUnit.SECONDS));

    return RpcClientFactory.builder()
        .setDisableRSocket(false)
        .setDisableLoadBalancing(true)
        .setDisableReconnectingClient(true)
        .setThriftClientConfig(clientConfig)
        .build();
  }

  private RecordingStreamHandler startServerWithHooks() {
    return startServerWithHooks(new ExtendedReactivePingService(), -1);
  }

  private RecordingStreamHandler startServerWithHooks(int expectedCreditSum) {
    return startServerWithHooks(new ExtendedReactivePingService(), expectedCreditSum);
  }

  private RecordingStreamHandler startServerWithHooks(
      ExtendedPing.Reactive service, int expectedCreditSum) {
    RecordingStreamHandler hooks = new RecordingStreamHandler(expectedCreditSum);
    startServer(service, ImmutableList.of(hooks));
    return hooks;
  }

  private RecordingStreamHandler startBackendWithHooks() {
    RecordingStreamHandler hooks = new RecordingStreamHandler();
    RpcServerHandler handler =
        ExtendedPing.Reactive.serverHandlerBuilder(new CappedStreamService(3))
            .setEventHandlers(ImmutableList.of(hooks))
            .build();
    // Port allocation races (parallel tests, TIME_WAIT reuse) surface as bind failures: retry
    // on a fresh port rather than flaking.
    for (int attempt = 0; ; attempt++) {
      backendAddress = new InetSocketAddress("localhost", RpcServerUtils.findFreePort());
      try {
        backendTransport =
            UnifiedServerTransport.createNewInstance(
                    backendAddress, handler, serverConfig, new SPINiftyMetrics())
                .block();
        break;
      } catch (RuntimeException e) {
        if (attempt >= 5 || !isBindCollision(e)) {
          throw e;
        }
      }
    }
    Assertions.assertNotNull(backendTransport);
    return hooks;
  }

  private void startServer(ExtendedPing.Reactive service, List<ThriftEventHandler> handlers) {
    RpcServerHandler handler =
        ExtendedPing.Reactive.serverHandlerBuilder(service).setEventHandlers(handlers).build();
    // Port allocation races (parallel tests, TIME_WAIT reuse) surface as bind failures: retry
    // on a fresh port rather than flaking.
    for (int attempt = 0; ; attempt++) {
      serverAddress = new InetSocketAddress("localhost", RpcServerUtils.findFreePort());
      try {
        transport =
            UnifiedServerTransport.createNewInstance(
                    serverAddress, handler, serverConfig, new SPINiftyMetrics())
                .block();
        break;
      } catch (RuntimeException e) {
        if (attempt >= 5 || !isBindCollision(e)) {
          throw e;
        }
      }
    }
    Assertions.assertNotNull(transport);
  }

  private static boolean isBindCollision(Throwable t) {
    while (t != null) {
      if (t instanceof BindException
          || String.valueOf(t.getMessage()).contains("Address already in use")) {
        return true;
      }
      t = t.getCause();
    }
    return false;
  }

  private ExtendedPing.Reactive client(ProtocolId protocolId) {
    return ExtendedPing.Reactive.clientBuilder()
        .setProtocolId(protocolId)
        .build(createRSocketClientFactory(), serverAddress);
  }

  private static PingRequest request() {
    return new PingRequest.Builder().setRequest("it").build();
  }

  /** Plain stream; also covers child-to-parent dispatch (streamOfPings is inherited). */
  @ParameterizedTest
  @MethodSource("data")
  public void plainStreamPingPong(ProtocolId protocolId) throws Exception {
    RecordingStreamHandler hooks = startServerWithHooks();

    StepVerifier.create(client(protocolId).streamOfPings(request(), 10))
        .assertNext(r -> Assertions.assertEquals("pong 1", r.getResponse()))
        .expectNextCount(9)
        .verifyComplete();

    Assertions.assertTrue(
        hooks.terminalLatch.await(10, TimeUnit.SECONDS), "terminal hook never fired");
    List<String> expected = new ArrayList<>();
    expected.add("subscribe");
    for (int i = 1; i <= 10; i++) {
      expected.add("element-" + i);
    }
    expected.add("terminal-COMPLETE");
    Assertions.assertEquals(expected, hooks.events());
    Assertions.assertEquals(StreamEventHandler.StreamOutcome.COMPLETE, hooks.terminalOutcome);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void firstResponseStream(ProtocolId protocolId) throws Exception {
    RecordingStreamHandler hooks = startServerWithHooks();

    StepVerifier.create(
            client(protocolId)
                .streamWithFirstResponse(request(), 10)
                .flatMapMany(
                    ras -> {
                      Assertions.assertEquals("it_first", ras.getFirstResponse().getResponse());
                      return ras.getStream();
                    }))
        .expectNextCount(10)
        .verifyComplete();

    Assertions.assertTrue(
        hooks.terminalLatch.await(10, TimeUnit.SECONDS), "terminal hook never fired");
    Assertions.assertEquals(10, hooks.elementCount());
    Assertions.assertEquals(StreamEventHandler.StreamOutcome.COMPLETE, hooks.terminalOutcome);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void firstResponseDeclaredInitialError(ProtocolId protocolId) throws Exception {
    RecordingStreamHandler hooks = startServerWithHooks();

    // The outer Mono fails with the declared exception before any first response: a
    // request-level initial error, so NO stream hook fires -- not even subscribe (the stream
    // phase never starts) -- but done() still does. The failure is visible to observers via
    // the ThriftEventHandler exception hooks.
    StepVerifier.create(client(protocolId).streamWithFirstResponse(request(), -1))
        .expectErrorSatisfies(
            t -> {
              Assertions.assertTrue(t instanceof CustomException, "got " + t);
              Assertions.assertTrue(
                  String.valueOf(t.getMessage()).contains("custom exception: it"),
                  "got " + t.getMessage());
            })
        .verify();

    Assertions.assertTrue(hooks.doneLatch.await(10, TimeUnit.SECONDS), "done never fired");
    Assertions.assertTrue(hooks.events().isEmpty(), "no stream hook fires on an initial error");
    Assertions.assertNull(hooks.terminalOutcome, "no stream terminal on an initial error");
    Assertions.assertEquals(0, hooks.terminals.get(), "no stream terminal on an initial error");
    Assertions.assertEquals(0, hooks.elementCount());
  }

  @ParameterizedTest
  @MethodSource("data")
  public void declaredExceptionMidStream(ProtocolId protocolId) throws Exception {
    RecordingStreamHandler hooks = startServerWithHooks();

    StepVerifier.create(client(protocolId).streamDeclaredExceptionMidStream(request(), 5))
        .expectNextCount(5)
        .expectErrorSatisfies(
            t -> {
              Assertions.assertTrue(t instanceof CustomException, "got " + t);
              Assertions.assertTrue(
                  String.valueOf(t.getMessage()).contains("custom exception: it"),
                  "got " + t.getMessage());
            })
        .verify();

    Assertions.assertTrue(
        hooks.terminalLatch.await(10, TimeUnit.SECONDS), "terminal hook never fired");
    Assertions.assertEquals(5, hooks.elementCount());
    Assertions.assertEquals(StreamEventHandler.StreamOutcome.ERROR, hooks.terminalOutcome);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void declaredExceptionBeforeFirst(ProtocolId protocolId) throws Exception {
    RecordingStreamHandler hooks = startServerWithHooks();

    StepVerifier.create(client(protocolId).streamDeclaredExceptionMidStream(request(), 0))
        .expectErrorSatisfies(
            t -> {
              Assertions.assertTrue(t instanceof CustomException, "got " + t);
              Assertions.assertTrue(
                  String.valueOf(t.getMessage()).contains("custom exception: it"),
                  "got " + t.getMessage());
            })
        .verify();

    Assertions.assertTrue(
        hooks.terminalLatch.await(10, TimeUnit.SECONDS), "terminal hook never fired");
    Assertions.assertEquals(0, hooks.elementCount());
    Assertions.assertEquals(StreamEventHandler.StreamOutcome.ERROR, hooks.terminalOutcome);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void undeclaredExceptionMidStream(ProtocolId protocolId) throws Exception {
    RecordingStreamHandler hooks = startServerWithHooks();

    StepVerifier.create(client(protocolId).streamUndeclaredExceptionMidStream(request(), 5))
        .expectNextCount(5)
        .expectErrorSatisfies(
            t -> {
              Assertions.assertTrue(t instanceof TApplicationException, "got " + t);
              Assertions.assertTrue(
                  String.valueOf(t.getMessage()).contains("undeclared: it"),
                  "got " + t.getMessage());
            })
        .verify();

    Assertions.assertTrue(
        hooks.terminalLatch.await(10, TimeUnit.SECONDS), "terminal hook never fired");
    Assertions.assertEquals(5, hooks.elementCount());
    Assertions.assertEquals(StreamEventHandler.StreamOutcome.ERROR, hooks.terminalOutcome);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void undeclaredExceptionBeforeFirst(ProtocolId protocolId) throws Exception {
    RecordingStreamHandler hooks = startServerWithHooks();

    StepVerifier.create(client(protocolId).streamUndeclaredExceptionMidStream(request(), -1))
        .expectErrorSatisfies(
            t -> {
              Assertions.assertTrue(t instanceof TApplicationException, "got " + t);
              Assertions.assertTrue(
                  String.valueOf(t.getMessage()).contains("undeclared: it"),
                  "got " + t.getMessage());
            })
        .verify();

    Assertions.assertTrue(
        hooks.terminalLatch.await(10, TimeUnit.SECONDS), "terminal hook never fired");
    Assertions.assertEquals(0, hooks.elementCount());
    Assertions.assertEquals(StreamEventHandler.StreamOutcome.ERROR, hooks.terminalOutcome);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void clientCancelMidStream(ProtocolId protocolId) throws Exception {
    RecordingStreamHandler hooks = startServerWithHooks();
    ExtendedPing.Reactive client = client(protocolId);

    // take(3) cancels upstream after 3 elements; the client sees normal completion.
    StepVerifier.create(client.streamOfPings(request(), 50).take(3))
        .expectNextCount(3)
        .verifyComplete();

    Assertions.assertTrue(
        hooks.terminalLatch.await(10, TimeUnit.SECONDS), "terminal hook never fired");
    Assertions.assertEquals(StreamEventHandler.StreamOutcome.CANCEL, hooks.terminalOutcome);
    Assertions.assertEquals(1, hooks.terminals.get());

    // The transport stays healthy and buffers are freed: a second stream completes fully.
    StepVerifier.create(client.streamOfPings(request(), 5)).expectNextCount(5).verifyComplete();
  }

  @ParameterizedTest
  @MethodSource("data")
  public void boundedClientDemand(ProtocolId protocolId) throws Exception {
    RecordingStreamHandler hooks = startServerWithHooks(10 + 1);

    StepVerifier.Step<PingResponse> steps =
        StepVerifier.create(client(protocolId).streamOfPings(request(), 10), 0).thenRequest(4);
    for (int i = 0; i < 4; i++) {
      steps = steps.assertNext(r -> {});
    }
    steps = steps.thenRequest(6);
    for (int i = 0; i < 6; i++) {
      steps = steps.assertNext(r -> {});
    }
    steps.verifyComplete();

    Assertions.assertTrue(
        hooks.terminalLatch.await(10, TimeUnit.SECONDS), "terminal hook never fired");
    // The only demand contract: the server sees exactly the requested frames plus the one
    // credit the protocol spends on the first payload, and no single grant is unbounded.
    Assertions.assertTrue(
        hooks.creditsCovered.await(10, TimeUnit.SECONDS),
        "server never saw the full demand: " + hooks.credits);
    Assertions.assertEquals(11, hooks.creditSum.get());
    Assertions.assertTrue(
        hooks.credits.stream().allMatch(c -> c <= 6), "unbounded demand: " + hooks.credits);
    Assertions.assertEquals(10, hooks.elementCount());
  }

  @Test
  public void hookThreadsNeverOnLoop() throws Exception {
    RecordingStreamHandler hooks = startServerWithHooks();
    String testThread = Thread.currentThread().getName();

    StepVerifier.create(
            ExtendedPing.Reactive.clientBuilder()
                .setProtocolId(ProtocolId.COMPACT)
                .build(createRSocketClientFactory(), serverAddress)
                .streamOfPings(request(), 5))
        .expectNextCount(5)
        .verifyComplete();

    Assertions.assertTrue(
        hooks.terminalLatch.await(10, TimeUnit.SECONDS), "terminal hook never fired");
    Assertions.assertFalse(hooks.hookThreads.isEmpty());
    for (String thread : hooks.hookThreads) {
      Assertions.assertNotEquals(testThread, thread, "hook ran on subscribing thread");
      Assertions.assertFalse(
          thread.startsWith("thrift-eventloop"), "hook ran on the event loop: " + thread);
    }
  }

  @Test
  public void disposeFirstResponseReleasesStreamCompact() throws Exception {
    runDisposeFirstResponseReleasesStream(ProtocolId.COMPACT);
  }

  @Test
  public void disposeFirstResponseReleasesStreamBinary() throws Exception {
    runDisposeFirstResponseReleasesStream(ProtocolId.BINARY);
  }

  /** Read the first response only, then dispose: the server must tear the stream down. */
  private void runDisposeFirstResponseReleasesStream(ProtocolId protocolId) throws Exception {
    CappedStreamService service = new CappedStreamService(0);
    RecordingStreamHandler hooks = startServerWithHooks(service, -1);

    CountDownLatch firstSeen = new CountDownLatch(1);
    client(protocolId)
        .streamWithFirstResponse(request(), 50)
        .subscribe(
            ras -> {
              try {
                Assertions.assertEquals("it_first", ras.getFirstResponse().getResponse());
              } finally {
                firstSeen.countDown();
                ras.dispose();
              }
            });
    Assertions.assertTrue(firstSeen.await(10, TimeUnit.SECONDS), "first response never arrived");

    // The undisposed half is abandoned: the server stream cancels, the CANCEL terminal fires,
    // then done.
    Assertions.assertTrue(
        service.streamCancelled.await(10, TimeUnit.SECONDS), "server stream never observed cancel");
    Assertions.assertTrue(
        hooks.terminalLatch.await(10, TimeUnit.SECONDS), "terminal hook never fired");
    Assertions.assertEquals(StreamEventHandler.StreamOutcome.CANCEL, hooks.terminalOutcome);
    Assertions.assertTrue(hooks.doneLatch.await(10, TimeUnit.SECONDS), "done never fired");
  }

  @Test
  public void forwardedStreamCancelReachesBackendCompact() throws Exception {
    runForwardedStreamCancelReachesBackend(ProtocolId.COMPACT);
  }

  @Test
  public void forwardedStreamCancelReachesBackendBinary() throws Exception {
    runForwardedStreamCancelReachesBackend(ProtocolId.BINARY);
  }

  /** A handler-returned backend stream forwards client cancel end to end. */
  private void runForwardedStreamCancelReachesBackend(ProtocolId protocolId) throws Exception {
    RecordingStreamHandler backendHooks = startBackendWithHooks();
    ExtendedPing.Reactive backendClient =
        ExtendedPing.Reactive.clientBuilder()
            .setProtocolId(protocolId)
            .build(createRSocketClientFactory(), backendAddress);
    RecordingStreamHandler frontendHooks =
        startServerWithHooks(new ForwardingService(backendClient), -1);

    StepVerifier.create(
            client(protocolId)
                .streamWithFirstResponse(request(), 50)
                .flatMapMany(
                    ras -> {
                      Assertions.assertEquals("it_first", ras.getFirstResponse().getResponse());
                      return ras.getStream();
                    })
                .take(3))
        .expectNextCount(3)
        .verifyComplete();

    Assertions.assertTrue(
        frontendHooks.terminalLatch.await(10, TimeUnit.SECONDS),
        "frontend terminal hook never fired");
    Assertions.assertEquals(StreamEventHandler.StreamOutcome.CANCEL, frontendHooks.terminalOutcome);
    Assertions.assertTrue(
        frontendHooks.doneLatch.await(10, TimeUnit.SECONDS), "frontend done never fired");
    Assertions.assertTrue(
        backendHooks.terminalLatch.await(10, TimeUnit.SECONDS),
        "backend terminal hook never fired");
    Assertions.assertEquals(StreamEventHandler.StreamOutcome.CANCEL, backendHooks.terminalOutcome);
    Assertions.assertTrue(
        backendHooks.doneLatch.await(10, TimeUnit.SECONDS), "backend done never fired");
  }

  @Test
  public void secondInnerSubscribeFailsCompact() throws Exception {
    runSecondInnerSubscribeFails(ProtocolId.COMPACT);
  }

  @Test
  public void secondInnerSubscribeFailsBinary() throws Exception {
    runSecondInnerSubscribeFails(ProtocolId.BINARY);
  }

  /** The inner stream supports one subscription; a second fails with an actionable error. */
  private void runSecondInnerSubscribeFails(ProtocolId protocolId) throws Exception {
    startServerWithHooks();

    CountDownLatch firstSeen = new CountDownLatch(1);
    AtomicReference<ResponseAndStream<PingResponse, PingResponse>> holder = new AtomicReference<>();
    client(protocolId)
        .streamWithFirstResponse(request(), 3)
        .subscribe(
            ras -> {
              holder.set(ras);
              firstSeen.countDown();
            });
    Assertions.assertTrue(firstSeen.await(10, TimeUnit.SECONDS), "first response never arrived");
    ResponseAndStream<PingResponse, PingResponse> ras = holder.get();

    // First claim with no demand, so the stream stays open for the second attempt.
    Disposable first = ras.getStream().subscribe(v -> {}, e -> {}, () -> {}, s -> {});
    CountDownLatch errorSeen = new CountDownLatch(1);
    AtomicReference<Throwable> error = new AtomicReference<>();
    ras.getStream()
        .subscribe(
            v -> {},
            e -> {
              error.set(e);
              errorSeen.countDown();
            });
    Assertions.assertTrue(errorSeen.await(10, TimeUnit.SECONDS), "second subscribe never failed");
    Assertions.assertTrue(error.get() instanceof IllegalStateException, "got " + error.get());
    Assertions.assertTrue(
        String.valueOf(error.get().getMessage()).contains("one subscription"),
        "got " + error.get().getMessage());
    first.dispose();
    ras.dispose();
  }

  @Test
  public void longPlainStreamInCreditStepsCompact() throws Exception {
    runLongPlainStreamInCreditSteps(ProtocolId.COMPACT);
  }

  @Test
  public void longPlainStreamInCreditStepsBinary() throws Exception {
    runLongPlainStreamInCreditSteps(ProtocolId.BINARY);
  }

  /** 10,000 elements arrive in order while the client refills demand in bounded steps. */
  private void runLongPlainStreamInCreditSteps(ProtocolId protocolId) throws Exception {
    RecordingStreamHandler hooks = startServerWithHooks();

    int[] seen = new int[1];
    StepVerifier.Step<PingResponse> steps =
        StepVerifier.create(client(protocolId).streamOfPings(request(), 10_000), 0);
    for (int chunk = 0; chunk < 10; chunk++) {
      steps = steps.thenRequest(1000);
      for (int i = 0; i < 1000; i++) {
        steps =
            steps.assertNext(r -> Assertions.assertEquals("pong " + (++seen[0]), r.getResponse()));
      }
    }
    steps.verifyComplete();

    Assertions.assertTrue(
        hooks.terminalLatch.await(30, TimeUnit.SECONDS), "terminal hook never fired");
    Assertions.assertEquals(StreamEventHandler.StreamOutcome.COMPLETE, hooks.terminalOutcome);
    Assertions.assertEquals(10_000, hooks.elementCount());
    Assertions.assertFalse(hooks.credits.isEmpty(), "server saw no credit refills");
    Assertions.assertTrue(
        hooks.credits.stream().allMatch(c -> c <= 1000), "unbounded grant: " + hooks.credits);
  }

  @Test
  public void longFirstResponseStreamInCreditStepsCompact() throws Exception {
    runLongFirstResponseStreamInCreditSteps(ProtocolId.COMPACT);
  }

  @Test
  public void longFirstResponseStreamInCreditStepsBinary() throws Exception {
    runLongFirstResponseStreamInCreditSteps(ProtocolId.BINARY);
  }

  /** Same as above through a first response. */
  private void runLongFirstResponseStreamInCreditSteps(ProtocolId protocolId) throws Exception {
    RecordingStreamHandler hooks = startServerWithHooks();

    int[] seen = new int[1];
    StepVerifier.Step<PingResponse> steps =
        StepVerifier.create(
            client(protocolId)
                .streamWithFirstResponse(request(), 10_000)
                .flatMapMany(
                    ras -> {
                      Assertions.assertEquals("it_first", ras.getFirstResponse().getResponse());
                      return ras.getStream();
                    }),
            0);
    for (int chunk = 0; chunk < 10; chunk++) {
      steps = steps.thenRequest(1000);
      for (int i = 0; i < 1000; i++) {
        steps =
            steps.assertNext(r -> Assertions.assertEquals("pong " + (++seen[0]), r.getResponse()));
      }
    }
    steps.verifyComplete();

    Assertions.assertTrue(
        hooks.terminalLatch.await(30, TimeUnit.SECONDS), "terminal hook never fired");
    Assertions.assertEquals(StreamEventHandler.StreamOutcome.COMPLETE, hooks.terminalOutcome);
    Assertions.assertEquals(10_000, hooks.elementCount());
    Assertions.assertFalse(hooks.credits.isEmpty(), "server saw no credit refills");
    Assertions.assertTrue(
        hooks.credits.stream().allMatch(c -> c <= 1000), "unbounded grant: " + hooks.credits);
  }

  @Test
  public void cancelBeforeFirstResponseCompact() throws Exception {
    runCancelBeforeFirstResponse(ProtocolId.COMPACT);
  }

  @Test
  public void cancelBeforeFirstResponseBinary() throws Exception {
    runCancelBeforeFirstResponse(ProtocolId.BINARY);
  }

  /**
   * Cancel while the handler is still producing the first response: no stream hooks, done fires.
   */
  private void runCancelBeforeFirstResponse(ProtocolId protocolId) throws Exception {
    GatedPingService service = new GatedPingService();
    RecordingStreamHandler hooks = startServerWithHooks(service, -1);
    try {
      Disposable call = client(protocolId).streamWithFirstResponse(request(), 10).subscribe();
      Assertions.assertTrue(service.entered.await(10, TimeUnit.SECONDS), "handler never entered");
      call.dispose();

      Assertions.assertTrue(
          service.handlerCancelled.await(10, TimeUnit.SECONDS), "handler never saw cancel");
      Assertions.assertTrue(hooks.doneLatch.await(10, TimeUnit.SECONDS), "done never fired");
      Assertions.assertTrue(
          hooks.events().isEmpty(), "stream hook fired before any first response");
      Assertions.assertNull(hooks.terminalOutcome, "stream terminal on an unbegun stream");
    } finally {
      service.gate.complete(null);
    }
  }

  @Test
  public void cancelAfterFirstResponseCompact() throws Exception {
    runCancelAfterFirstResponse(ProtocolId.COMPACT);
  }

  @Test
  public void cancelAfterFirstResponseBinary() throws Exception {
    runCancelAfterFirstResponse(ProtocolId.BINARY);
  }

  /** Subscribe to the inner stream with no demand, then cancel before any element arrives. */
  private void runCancelAfterFirstResponse(ProtocolId protocolId) throws Exception {
    // Capped at zero elements so no completion can race the cancel.
    CappedStreamService service = new CappedStreamService(0);
    RecordingStreamHandler hooks = startServerWithHooks(service, -1);

    CountDownLatch firstSeen = new CountDownLatch(1);
    client(protocolId)
        .streamWithFirstResponse(request(), 50)
        .subscribe(
            ras -> {
              try {
                Assertions.assertEquals("it_first", ras.getFirstResponse().getResponse());
                Disposable inner = ras.getStream().subscribe(v -> {}, e -> {}, () -> {}, s -> {});
                inner.dispose();
              } finally {
                firstSeen.countDown();
                ras.dispose();
              }
            });
    Assertions.assertTrue(firstSeen.await(10, TimeUnit.SECONDS), "first response never arrived");

    Assertions.assertTrue(
        hooks.terminalLatch.await(10, TimeUnit.SECONDS), "terminal hook never fired");
    Assertions.assertEquals(StreamEventHandler.StreamOutcome.CANCEL, hooks.terminalOutcome);
    Assertions.assertTrue(hooks.doneLatch.await(10, TimeUnit.SECONDS), "done never fired");
    Assertions.assertTrue(
        service.streamCancelled.await(10, TimeUnit.SECONDS), "server stream never observed cancel");
  }

  @Test
  public void streamWriteHooksFireOnceCompact() throws Exception {
    runStreamWriteHooksFireOnce(ProtocolId.COMPACT, false);
  }

  @Test
  public void streamWriteHooksFireOnceBinary() throws Exception {
    runStreamWriteHooksFireOnce(ProtocolId.BINARY, false);
  }

  @Test
  public void firstResponseWriteHooksFireOnceCompact() throws Exception {
    runStreamWriteHooksFireOnce(ProtocolId.COMPACT, true);
  }

  @Test
  public void firstResponseWriteHooksFireOnceBinary() throws Exception {
    runStreamWriteHooksFireOnce(ProtocolId.BINARY, true);
  }

  /**
   * The write hooks fire once per stream, on the first response before any element: exactly one
   * preWrite and one postWrite for the whole 3-element stream, never one per element. A plain
   * stream has no first response value, so both hooks see null.
   */
  private void runStreamWriteHooksFireOnce(ProtocolId protocolId, boolean firstResponse)
      throws Exception {
    RecordingWriteHandler hooks = new RecordingWriteHandler();
    startServer(new ExtendedReactivePingService(), ImmutableList.of(hooks));

    if (firstResponse) {
      StepVerifier.create(
              client(protocolId)
                  .streamWithFirstResponse(request(), 3)
                  .flatMapMany(ras -> ras.getStream()))
          .expectNextCount(3)
          .verifyComplete();
    } else {
      StepVerifier.create(client(protocolId).streamOfPings(request(), 3))
          .expectNextCount(3)
          .verifyComplete();
    }

    Assertions.assertTrue(hooks.doneLatch.await(10, TimeUnit.SECONDS), "done never fired");

    List<String> expected = new ArrayList<>();
    expected.add("preWrite");
    expected.add("postWrite");
    expected.add("element-1");
    expected.add("element-2");
    expected.add("element-3");
    expected.add("terminal-COMPLETE");
    Assertions.assertEquals(expected, hooks.events());

    if (firstResponse) {
      Assertions.assertEquals("it_first", ((PingResponse) hooks.preWriteArg).getResponse());
    } else {
      Assertions.assertNull(hooks.preWriteArg, "plain stream preWrite sees null");
    }
  }

  /** Records write hooks and stream elements in one order, pinning the once-per-stream contract. */
  private static class RecordingWriteHandler implements StreamEventHandler {
    final List<String> ordered = Collections.synchronizedList(new ArrayList<>());
    final CountDownLatch doneLatch = new CountDownLatch(1);
    volatile Object preWriteArg;

    List<String> events() {
      return new ArrayList<>(ordered);
    }

    @Override
    public Object getContext(String methodName, RequestContext requestContext) {
      return new Object();
    }

    @Override
    public void preWrite(Object context, String methodName, Object result) {
      ordered.add("preWrite");
      preWriteArg = result;
    }

    @Override
    public void postWrite(Object context, String methodName, Object result) {
      ordered.add("postWrite");
    }

    @Override
    public void onStreamNext(Object context, String methodName, long sequenceNumber) {
      ordered.add("element-" + sequenceNumber);
    }

    @Override
    public void onStreamFinally(
        Object context, String methodName, StreamEventHandler.StreamOutcome outcome) {
      ordered.add("terminal-" + outcome);
    }

    @Override
    public void done(Object context, String methodName) {
      doneLatch.countDown();
    }
  }

  @Test
  public void concurrentStreamsShareOneConnectionCompact() throws Exception {
    runConcurrentStreamsShareOneConnection(ProtocolId.COMPACT);
  }

  @Test
  public void concurrentStreamsShareOneConnectionBinary() throws Exception {
    runConcurrentStreamsShareOneConnection(ProtocolId.BINARY);
  }

  /**
   * 50 streams plus request-response calls multiplexed over one client: every stream delivers its
   * own elements in order and completes. The connection limit (10) would refuse per-call
   * connections, so passing proves sharing.
   */
  private void runConcurrentStreamsShareOneConnection(ProtocolId protocolId) throws Exception {
    RecordingStreamHandler hooks = startServerWithHooks();
    ExtendedPing.Reactive client = client(protocolId);

    int streams = 50;
    int requestResponse = 10;
    CountDownLatch finished = new CountDownLatch(streams + requestResponse);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    for (int i = 0; i < streams; i++) {
      client
          .streamOfPings(request(), 5)
          .collectList()
          .subscribe(
              list -> {
                try {
                  Assertions.assertEquals(5, list.size());
                  for (int j = 0; j < 5; j++) {
                    Assertions.assertEquals("pong " + (j + 1), list.get(j).getResponse());
                  }
                } catch (Throwable t) {
                  failure.compareAndSet(null, t);
                } finally {
                  finished.countDown();
                }
              },
              e -> {
                failure.compareAndSet(null, e);
                finished.countDown();
              });
    }
    for (int i = 0; i < requestResponse; i++) {
      String key = "rr" + i;
      client
          .ping(new PingRequest.Builder().setRequest(key).build())
          .subscribe(
              r -> {
                try {
                  Assertions.assertTrue(
                      r.getResponse().startsWith(key + "_pong_"), "got " + r.getResponse());
                } catch (Throwable t) {
                  failure.compareAndSet(null, t);
                } finally {
                  finished.countDown();
                }
              },
              e -> {
                failure.compareAndSet(null, e);
                finished.countDown();
              });
    }

    Assertions.assertTrue(finished.await(60, TimeUnit.SECONDS), "calls never finished");
    Assertions.assertNull(failure.get(), "a call failed: " + failure.get());
    Assertions.assertTrue(
        hooks.terminalLatch.await(10, TimeUnit.SECONDS), "terminal hook never fired");
    Assertions.assertEquals(streams, hooks.terminals.get());
    Assertions.assertEquals(streams * 5, hooks.elementCount());
  }

  @Test
  public void streamHookSeesRequestContextCompact() throws Exception {
    runStreamHookSeesRequestContext(ProtocolId.COMPACT);
  }

  @Test
  public void streamHookSeesRequestContextBinary() throws Exception {
    runStreamHookSeesRequestContext(ProtocolId.BINARY);
  }

  /** Stream hooks observe the same context the handler returned from getContext. */
  private void runStreamHookSeesRequestContext(ProtocolId protocolId) throws Exception {
    RecordingStreamHandler stream = new RecordingStreamHandler();
    RecordingPlainHandler plain = new RecordingPlainHandler();
    startServer(new ExtendedReactivePingService(), ImmutableList.of(plain, stream));

    StepVerifier.create(client(protocolId).streamOfPings(request(), 3))
        .expectNextCount(3)
        .verifyComplete();

    Assertions.assertTrue(
        stream.terminalLatch.await(10, TimeUnit.SECONDS), "terminal hook never fired");
    Assertions.assertTrue(stream.doneLatch.await(10, TimeUnit.SECONDS), "done never fired");
    Assertions.assertTrue(
        plain.doneLatch.await(10, TimeUnit.SECONDS), "plain handler done never fired");
    Assertions.assertNotNull(stream.contextAtPreRead);
    Assertions.assertEquals(3, stream.nextContexts.size());
    for (Object context : stream.nextContexts) {
      Assertions.assertSame(stream.contextAtPreRead, context);
    }
    Assertions.assertTrue(plain.calls.contains("preRead"), "plain handler saw: " + plain.calls);
    Assertions.assertTrue(plain.calls.contains("done"), "plain handler saw: " + plain.calls);
    Assertions.assertTrue(
        plain.calls.stream().noneMatch(c -> c.toLowerCase().contains("stream")),
        "plain handler saw stream calls: " + plain.calls);
  }

  @Test
  public void throwingStreamHookFailsClientStreamCompact() throws Exception {
    runThrowingStreamHookFailsClientStream(ProtocolId.COMPACT);
  }

  @Test
  public void throwingStreamHookFailsClientStreamBinary() throws Exception {
    runThrowingStreamHookFailsClientStream(ProtocolId.BINARY);
  }

  /** A throwing stream hook fails the client stream; the server still terminates and cleans up. */
  private void runThrowingStreamHookFailsClientStream(ProtocolId protocolId) throws Exception {
    RecordingStreamHandler hooks = new RecordingStreamHandler();
    StreamEventHandler throwing =
        new StreamEventHandler() {
          @Override
          public void onStreamNext(Object context, String methodName, long sequenceNumber) {
            if (sequenceNumber == 2) {
              throw new RuntimeException("boom-onStreamNext");
            }
          }
        };
    startServer(new ExtendedReactivePingService(), ImmutableList.of(hooks, throwing));

    StepVerifier.create(client(protocolId).streamOfPings(request(), 10))
        .expectNextCount(1)
        .expectErrorSatisfies(
            t -> Assertions.assertTrue(String.valueOf(t).contains("boom-onStreamNext"), "got " + t))
        .verify();

    Assertions.assertTrue(
        hooks.terminalLatch.await(10, TimeUnit.SECONDS), "terminal hook never fired");
    Assertions.assertEquals(StreamEventHandler.StreamOutcome.ERROR, hooks.terminalOutcome);
    Assertions.assertTrue(hooks.doneLatch.await(10, TimeUnit.SECONDS), "done never fired");
  }

  @Test
  public void cancelTerminalBeforeDoneCompact() throws Exception {
    runCancelTerminalBeforeDone(ProtocolId.COMPACT);
  }

  @Test
  public void cancelTerminalBeforeDoneBinary() throws Exception {
    runCancelTerminalBeforeDone(ProtocolId.BINARY);
  }

  /** On client cancel the terminal is recorded strictly before done: after done, it is last. */
  private void runCancelTerminalBeforeDone(ProtocolId protocolId) throws Exception {
    // Capped at exactly the take count so no completion can race the cancel.
    RecordingStreamHandler hooks = startServerWithHooks(new CappedStreamService(3), -1);

    StepVerifier.create(client(protocolId).streamOfPings(request(), 50).take(3))
        .expectNextCount(3)
        .verifyComplete();

    Assertions.assertTrue(hooks.doneLatch.await(10, TimeUnit.SECONDS), "done never fired");
    List<String> events = hooks.events();
    Assertions.assertEquals("terminal-CANCEL", events.get(events.size() - 1));
    Assertions.assertEquals(1, hooks.terminals.get());
  }

  @Test
  public void connectionDropFiresCancelTerminalCompact() throws Exception {
    runConnectionDropFiresCancelTerminal(ProtocolId.COMPACT);
  }

  @Test
  public void connectionDropFiresCancelTerminalBinary() throws Exception {
    runConnectionDropFiresCancelTerminal(ProtocolId.BINARY);
  }

  /** Dropping the client connection mid-stream drives the server CANCEL terminal, then done. */
  private void runConnectionDropFiresCancelTerminal(ProtocolId protocolId) throws Exception {
    // Capped past the two observed elements so no completion can race the drop.
    RecordingStreamHandler hooks = startServerWithHooks(new CappedStreamService(2), -1);
    ExtendedPing.Reactive client = client(protocolId);

    CountDownLatch twoSeen = new CountDownLatch(1);
    AtomicInteger seen = new AtomicInteger();
    Disposable subscription =
        client
            .streamOfPings(request(), 1000)
            .subscribe(
                r -> {
                  if (seen.incrementAndGet() == 2) {
                    twoSeen.countDown();
                  }
                });
    Assertions.assertTrue(twoSeen.await(10, TimeUnit.SECONDS), "stream never started");

    client.dispose();
    Assertions.assertTrue(
        hooks.terminalLatch.await(10, TimeUnit.SECONDS), "terminal hook never fired");
    Assertions.assertEquals(StreamEventHandler.StreamOutcome.CANCEL, hooks.terminalOutcome);
    Assertions.assertTrue(hooks.doneLatch.await(10, TimeUnit.SECONDS), "done never fired");
    subscription.dispose();
  }

  private static class RecordingStreamHandler implements StreamEventHandler {
    private final List<String> ordered = Collections.synchronizedList(new ArrayList<>());
    private final List<Integer> credits = Collections.synchronizedList(new ArrayList<>());
    private final List<String> hookThreads = Collections.synchronizedList(new ArrayList<>());
    private final CountDownLatch terminalLatch = new CountDownLatch(1);
    private final CountDownLatch doneLatch = new CountDownLatch(1);
    private final CountDownLatch creditsCovered = new CountDownLatch(1);
    private final AtomicInteger creditSum = new AtomicInteger();
    private final int expectedCreditSum;
    private volatile StreamOutcome terminalOutcome;
    private final AtomicInteger terminals = new AtomicInteger();
    private volatile Object contextAtPreRead;
    private final List<Object> nextContexts = Collections.synchronizedList(new ArrayList<>());

    RecordingStreamHandler() {
      this(-1);
    }

    RecordingStreamHandler(int expectedCreditSum) {
      this.expectedCreditSum = expectedCreditSum;
    }

    List<String> events() {
      return new ArrayList<>(ordered);
    }

    int elementCount() {
      int count = 0;
      for (String event : ordered) {
        if (event.startsWith("element-")) {
          count++;
        }
      }
      return count;
    }

    @Override
    public Object getContext(String methodName, RequestContext requestContext) {
      return new Object();
    }

    @Override
    public void preRead(Object context, String methodName) {
      contextAtPreRead = context;
    }

    @Override
    public void onStreamSubscribe(Object context, String methodName) {
      hookThreads.add(Thread.currentThread().getName());
      ordered.add("subscribe");
    }

    @Override
    public void onStreamNext(Object context, String methodName, long sequenceNumber) {
      hookThreads.add(Thread.currentThread().getName());
      ordered.add("element-" + sequenceNumber);
      nextContexts.add(context);
    }

    @Override
    public void onStreamCredit(Object context, String methodName, int credits) {
      hookThreads.add(Thread.currentThread().getName());
      this.credits.add(credits);
      if (creditSum.addAndGet(credits) == expectedCreditSum) {
        creditsCovered.countDown();
      }
    }

    @Override
    public void handleStreamError(Object context, String methodName, Throwable error) {
      errorCause = error;
    }

    private volatile Throwable errorCause;

    @Override
    public void onStreamFinally(Object context, String methodName, StreamOutcome outcome) {
      hookThreads.add(Thread.currentThread().getName());
      ordered.add("terminal-" + outcome);
      terminalOutcome = outcome;
      terminals.incrementAndGet();
      terminalLatch.countDown();
    }

    @Override
    public void done(Object context, String methodName) {
      doneLatch.countDown();
    }
  }

  /** Plain request-level observer: records the hooks it sees, none of which are stream hooks. */
  private static class RecordingPlainHandler implements ThriftEventHandler {
    final List<String> calls = Collections.synchronizedList(new ArrayList<>());
    final CountDownLatch doneLatch = new CountDownLatch(1);

    @Override
    public Object getContext(String methodName, RequestContext requestContext) {
      calls.add("getContext");
      return new Object();
    }

    @Override
    public void preRead(Object context, String methodName) {
      calls.add("preRead");
    }

    @Override
    public void done(Object context, String methodName) {
      calls.add("done");
      doneLatch.countDown();
    }
  }

  /**
   * First-response service that reports when the server stream is cancelled and lets only a capped
   * number of elements through, hanging the rest. take(n) requests unbounded upstream, so an
   * uncapped server usually outruns the cancel and completes; the cap makes CANCEL the only
   * possible terminal.
   */
  private static class CappedStreamService extends ExtendedReactivePingService {
    final CountDownLatch streamCancelled = new CountDownLatch(1);
    private final AtomicInteger remaining;

    CappedStreamService(int allowedElements) {
      this.remaining = new AtomicInteger(allowedElements);
    }

    private <T> Flux<T> cap(Flux<T> stream) {
      return stream
          .doOnCancel(streamCancelled::countDown)
          .concatMap(e -> remaining.getAndDecrement() > 0 ? Mono.just(e) : Mono.<T>never());
    }

    @Override
    public Mono<ResponseAndStream<PingResponse, PingResponse>> streamWithFirstResponse(
        PingRequest request, int numberOfPings) {
      return super.streamWithFirstResponse(request, numberOfPings)
          .map(ras -> ras.mapStream(this::cap));
    }

    @Override
    public Flux<PingResponse> streamOfPings(PingRequest request, int numberOfPings) {
      return cap(super.streamOfPings(request, numberOfPings));
    }
  }

  /** First-response service that serves every call from a backend server. */
  private static class ForwardingService extends ExtendedReactivePingService {
    private final ExtendedPing.Reactive backend;

    ForwardingService(ExtendedPing.Reactive backend) {
      this.backend = backend;
    }

    @Override
    public Mono<ResponseAndStream<PingResponse, PingResponse>> streamWithFirstResponse(
        PingRequest request, int numberOfPings) {
      return backend.streamWithFirstResponse(request, numberOfPings);
    }
  }

  /** First-response service gated on a latch, reporting handler cancellation. */
  private static class GatedPingService extends ExtendedReactivePingService {
    final CountDownLatch entered = new CountDownLatch(1);
    final CompletableFuture<Void> gate = new CompletableFuture<>();
    final CountDownLatch handlerCancelled = new CountDownLatch(1);

    @Override
    public Mono<ResponseAndStream<PingResponse, PingResponse>> streamWithFirstResponse(
        PingRequest request, int numberOfPings) {
      entered.countDown();
      return Mono.fromFuture(gate)
          .then(super.streamWithFirstResponse(request, numberOfPings))
          .doOnCancel(handlerCancelled::countDown);
    }
  }
}
