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
import com.facebook.swift.service.ContextChain;
import com.facebook.swift.service.StreamEventHandler;
import com.facebook.swift.service.ThriftEventHandler;
import com.facebook.swift.service.ThriftServerConfig;
import com.facebook.thrift.client.RpcClient;
import com.facebook.thrift.client.RpcClientFactory;
import com.facebook.thrift.client.RpcOptions;
import com.facebook.thrift.client.ThriftClientConfig;
import com.facebook.thrift.example.ping.CustomException;
import com.facebook.thrift.example.ping.ExtendedPing;
import com.facebook.thrift.example.ping.FinalException;
import com.facebook.thrift.example.ping.PingRequest;
import com.facebook.thrift.example.ping.PingResponse;
import com.facebook.thrift.example.ping.SinkException;
import com.facebook.thrift.model.ResponseAndStream;
import com.facebook.thrift.model.StreamResponse;
import com.facebook.thrift.payload.ClientRequestPayload;
import com.facebook.thrift.payload.ClientResponsePayload;
import com.facebook.thrift.payload.FieldReader;
import com.facebook.thrift.payload.ServerRequestPayload;
import com.facebook.thrift.payload.ServerResponsePayload;
import com.facebook.thrift.payload.Writer;
import com.facebook.thrift.protocol.TProtocolType;
import com.facebook.thrift.rsocket.client.RSocketRpcClientFactory;
import com.facebook.thrift.server.RpcServerHandler;
import com.facebook.thrift.server.bidi.BidiMethod;
import com.facebook.thrift.server.bidi.ServerBidiStreams;
import com.facebook.thrift.server.sink.ServerSinks;
import com.facebook.thrift.server.sink.SinkMethod;
import com.facebook.thrift.server.stream.ServerStreams;
import com.facebook.thrift.testservices.ExtendedReactivePingService;
import com.facebook.thrift.util.RpcClientUtils;
import com.facebook.thrift.util.RpcPayloadUtil;
import com.facebook.thrift.util.RpcServerUtils;
import com.facebook.thrift.util.SPINiftyMetrics;
import com.facebook.thrift.util.Writers;
import com.facebook.thrift.util.resources.RpcResources;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetectorFactory;
import io.netty.util.concurrent.EventExecutor;
import io.rsocket.DuplexConnection;
import io.rsocket.RSocketErrorException;
import io.rsocket.frame.ErrorFrameCodec;
import io.rsocket.frame.FrameHeaderCodec;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.RequestChannelFrameCodec;
import io.rsocket.frame.RequestNFrameCodec;
import io.rsocket.plugins.DuplexConnectionInterceptor;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import org.apache.thrift.ProtocolId;
import org.apache.thrift.RequestRpcMetadata;
import org.apache.thrift.RpcKind;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

/**
 * Java-to-Java streaming integration tests over the real unified transport (RSocket via ALPN):
 * plain streams (dispatched through the child-to-parent inheritance path), first-response streams,
 * declared/undeclared errors mid-stream and before any element, client cancel, and bounded client
 * demand. Every stream test runs on COMPACT and BINARY. The channel tests check the frames that the
 * client receives.
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

  // The server's sink window: unread sink payloads one channel may hold.
  private static final int SINK_WINDOW = 100;

  // A stream id the client's own calls in these tests never reach, for raw frames.
  private static final int RAW_STREAM_ID = 101;

  private UnifiedServerTransport transport;
  private SocketAddress serverAddress;
  private RpcClient channelClient;
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
    if (channelClient != null) {
      channelClient.dispose();
      channelClient = null;
    }
    if (transport != null) {
      transport.dispose();
      transport = null;
    }
    if (backendTransport != null) {
      backendTransport.dispose();
      backendTransport = null;
    }
  }

  private static ThriftClientConfig clientConfig() {
    return new ThriftClientConfig()
        .setDisableSSL(false)
        .setEnableJdkSsl(false)
        .setCertFile(TestCertificateUtil.getCertFilePath())
        .setKeyFile(TestCertificateUtil.getKeyFilePath())
        .setCAFile(TestCertificateUtil.getCAFilePath())
        .setRequestTimeout(Duration.succinctDuration(30, TimeUnit.SECONDS));
  }

  private RpcClientFactory createRSocketClientFactory() {
    return RpcClientFactory.builder()
        .setDisableRSocket(false)
        .setDisableLoadBalancing(true)
        .setDisableReconnectingClient(true)
        .setThriftClientConfig(clientConfig())
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
    startServer(
        ExtendedPing.Reactive.serverHandlerBuilder(service).setEventHandlers(handlers).build());
  }

  private void startServer(RpcServerHandler handler) {
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

  @Test
  public void sinkSendsItsFinalAsOneFrameAfterTheFirstResponse() throws Exception {
    sinkSendsItsFinalAsOneFrame(false);
  }

  @Test
  public void sinkSendsItsFinalAsOneFrameOffTheEventLoop() throws Exception {
    sinkSendsItsFinalAsOneFrame(true);
  }

  private void sinkSendsItsFinalAsOneFrame(boolean offLoop) throws Exception {
    FrameLog frames = new FrameLog();
    RpcClient client = channelClient(startChannelServer(countingSink(offLoop)), frames);

    StepVerifier.create(
            client.streamingRequestStreamingResponse(
                Flux.just(
                    channelRequest(RpcKind.SINK, pingArgsWriter("hello")),
                    channelRequest(RpcKind.SINK, sinkPayloadWriter("a")),
                    channelRequest(RpcKind.SINK, sinkPayloadWriter("b"))),
                RpcOptions.EMPTY))
        .expectNextCount(1)
        .assertNext(last -> Assertions.assertEquals("read 2", finalResponse(last)))
        .verifyComplete();

    Assertions.assertEquals(Arrays.asList("NEXT", "REQUEST_N", "NEXT_COMPLETE"), frames.fromServer);
  }

  @Test
  public void emptySinkGetsItsFinal() throws Exception {
    FrameLog frames = new FrameLog();
    RpcClient client = channelClient(startChannelServer(countingSink(false)), frames);

    StepVerifier.create(
            client.streamingRequestStreamingResponse(
                Flux.just(channelRequest(RpcKind.SINK, pingArgsWriter("hello"))), RpcOptions.EMPTY))
        .expectNextCount(1)
        .assertNext(last -> Assertions.assertEquals("read 0", finalResponse(last)))
        .verifyComplete();

    Assertions.assertEquals("NEXT", frames.fromServer.get(0));
    Assertions.assertEquals("NEXT_COMPLETE", frames.fromServer.get(frames.fromServer.size() - 1));
    Assertions.assertFalse(frames.fromServer.contains("COMPLETE"));
    Assertions.assertFalse(frames.fromServer.contains("CANCEL"));
  }

  @Test
  public void sinkThatStopsReadingSendsNoCancelBeforeItsFinal() throws Exception {
    FrameLog frames = new FrameLog();
    RpcServerHandler readsOne =
        channelHandler(
            (initial, sinkElements) ->
                Flux.concat(
                    Flux.just(firstResponse()),
                    sinkElements.next().map(element -> response(initial, element.getRequest()))));

    RpcClient client = channelClient(startChannelServer(readsOne), frames);

    StepVerifier.create(
            client.streamingRequestStreamingResponse(
                Flux.concat(
                    Flux.just(
                        channelRequest(RpcKind.SINK, pingArgsWriter("hello")),
                        channelRequest(RpcKind.SINK, sinkPayloadWriter("a")),
                        channelRequest(RpcKind.SINK, sinkPayloadWriter("b"))),
                    Flux.never()),
                RpcOptions.EMPTY))
        .expectNextCount(1)
        .assertNext(last -> Assertions.assertEquals("a", finalResponse(last)))
        .verifyComplete();

    frames.await("CANCEL");
    Assertions.assertEquals(
        Arrays.asList("NEXT", "REQUEST_N", "NEXT_COMPLETE", "CANCEL"), frames.fromServer);
  }

  @Test
  public void sinkInitialErrorEndsWithAnErrorFrame() throws Exception {
    FrameLog frames = new FrameLog();
    RpcServerHandler firstOnly =
        channelHandler((initial, sinkElements) -> Flux.just(firstResponse()));

    RpcClient client = channelClient(startChannelServer(firstOnly), frames);

    client
        .streamingRequestStreamingResponse(
            Flux.just(channelRequest(RpcKind.SINK, pingArgsWriter("hello"))), RpcOptions.EMPTY)
        .onErrorResume(e -> Flux.empty())
        .collectList()
        .block();

    Assertions.assertEquals(Arrays.asList("NEXT", "ERROR"), frames.fromServer);
  }

  @Test
  public void sinkFinalThatDoesNotSerializeEndsWithAnErrorFrame() throws Exception {
    FrameLog frames = new FrameLog();
    RpcServerHandler unwritable =
        channelHandler(
            (initial, sinkElements) -> Flux.just(firstResponse(), response(initial, "unwritable")));

    RpcClient client = channelClient(startChannelServer(unwritable), frames);

    List<ClientResponsePayload<Object>> items =
        client
            .<Object, Object>streamingRequestStreamingResponse(
                Flux.just(channelRequest(RpcKind.SINK, pingArgsWriter("hello"))), RpcOptions.EMPTY)
            .collectList()
            .block();

    Assertions.assertNotNull(items.get(1).getException(), "items " + items);
    Assertions.assertEquals(Arrays.asList("NEXT", "ERROR"), frames.fromServer);
  }

  @Test
  public void channelTaskTimeoutSendsOnlyAnErrorFrame() throws Exception {
    serverConfig.setTaskExpirationTimeout(Duration.valueOf("100ms"));
    RpcServerHandler silent = channelHandler((initial, sinkElements) -> Flux.never());

    channelSendsOnlyAnErrorFrame(startChannelServer(silent), RpcKind.SINK);
  }

  @Test
  public void channelOverloadSendsOnlyAnErrorFrame() throws Exception {
    // A full off-loop scheduler rejects the dispatch with this exception.
    RpcServerHandler overloaded =
        channelHandler(
            (initial, sinkElements) -> Flux.error(new RejectedExecutionException("overloaded")));

    channelSendsOnlyAnErrorFrame(startChannelServer(overloaded), RpcKind.BIDIRECTIONAL_STREAM);
  }

  @Test
  public void channelBadSetupSendsOnlyAnErrorFrame() throws Exception {
    channelSendsOnlyAnErrorFrame(
        startChannelServer(countingSink(false)), RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE);
  }

  /** The sink stays open while the call fails: rsocket-java ends it without a CANCEL frame. */
  private void channelSendsOnlyAnErrorFrame(SocketAddress address, RpcKind kind) {
    FrameLog frames = new FrameLog();
    RpcClient client = channelClient(address, frames);

    StepVerifier.create(
            client.streamingRequestStreamingResponse(
                Flux.concat(Flux.just(channelRequest(kind, pingArgsWriter("hello"))), Flux.never()),
                RpcOptions.EMPTY))
        .assertNext(
            only -> Assertions.assertTrue(only.getException() instanceof TApplicationException))
        .verifyComplete();

    Assertions.assertEquals(Collections.singletonList("ERROR"), frames.fromServer);
  }

  @Test
  public void channelCorruptMetadataSendsOnlyAnErrorFrame() throws Exception {
    FrameLog frames = new FrameLog();
    channelClient(startChannelServer(countingSink(false)), frames);

    frames.send(
        RequestChannelFrameCodec.encode(
            frames.alloc(),
            RAW_STREAM_ID,
            false,
            false,
            1,
            frames.alloc().buffer().writeBytes(new byte[] {(byte) 0xff, (byte) 0xff}),
            frames.alloc().buffer()));

    frames.await("ERROR");
    Assertions.assertEquals(Collections.singletonList("ERROR"), frames.fromServer);
  }

  @Test
  public void channelSetupWithFragmentedMetadataGetsAnErrorFrame() throws Exception {
    FrameLog frames = new FrameLog();
    channelClient(startChannelServer(countingSink(false)), frames);

    frames.send(
        RequestChannelFrameCodec.encode(
            frames.alloc(),
            RAW_STREAM_ID,
            true,
            false,
            1,
            frames.alloc().buffer().writeByte(0x18),
            frames.alloc().buffer()));

    frames.await("ERROR");
    Assertions.assertEquals(Collections.singletonList("ERROR"), frames.fromServer);
  }

  @Test
  public void malformedFrameClosesTheConnectionAndEndsTheActiveSink() throws Exception {
    FrameLog frames = new FrameLog();
    Recorded<Object> read = new Recorded<>();
    RpcClient client = channelClient(startChannelServer(recordingReader(read)), frames);

    CompletableFuture<List<ClientResponsePayload<Object>>> call =
        client
            .<Object, Object>streamingRequestStreamingResponse(openSink(), RpcOptions.EMPTY)
            .onErrorResume(e -> Flux.empty())
            .collectList()
            .toFuture();

    frames.await("REQUEST_N");
    frames.send(frames.alloc().buffer().writeInt(RAW_STREAM_ID));

    client.onClose().toFuture().get(5, TimeUnit.SECONDS);
    call.get(5, TimeUnit.SECONDS);
    read.awaitSize(1);
    Assertions.assertTrue(read.get(0) instanceof Throwable, "read " + read);
  }

  @Test
  public void sinkReadsItsElementsOffTheEventLoop() throws Exception {
    Assumptions.assumeTrue(RpcResources.isForceExecutionOffEventLoop());
    List<Thread> threads = new CopyOnWriteArrayList<>();

    RpcServerHandler sink =
        channelHandler(
            (initial, sinkElements) ->
                Flux.concat(
                    Flux.just(firstResponse()),
                    sinkElements
                        .doOnNext(element -> threads.add(Thread.currentThread()))
                        .count()
                        .map(n -> response(initial, "read " + n))));

    RpcClient client = channelClient(startChannelServer(sink), new FrameLog());

    StepVerifier.create(
            client.streamingRequestStreamingResponse(
                Flux.just(
                    channelRequest(RpcKind.SINK, pingArgsWriter("hello")),
                    channelRequest(RpcKind.SINK, sinkPayloadWriter("a")),
                    channelRequest(RpcKind.SINK, sinkPayloadWriter("b"))),
                RpcOptions.EMPTY))
        .expectNextCount(2)
        .verifyComplete();

    Assertions.assertEquals(2, threads.size());
    for (Thread thread : threads) {
      for (EventExecutor loop : RpcResources.getEventLoopGroup()) {
        Assertions.assertFalse(loop.inEventLoop(thread), thread.getName());
      }
    }
  }

  @Test
  public void sinkHoldsABoundedWindowOfPayloadsForASlowReader() throws Exception {
    Sinks.Empty<Void> release = Sinks.empty();
    FrameLog frames = new FrameLog();

    RpcServerHandler slowReader =
        channelHandler(
            (initial, sinkElements) ->
                Flux.concat(
                    Flux.just(firstResponse()),
                    sinkElements
                        .delayUntil(element -> release.asMono())
                        .count()
                        .map(n -> response(initial, "read " + n))));

    RpcClient client = channelClient(startChannelServer(slowReader), frames);
    Flux<ClientRequestPayload<Object>> requests =
        Flux.concat(
            Flux.just(channelRequest(RpcKind.SINK, pingArgsWriter("hello"))),
            Flux.range(0, 3 * SINK_WINDOW)
                .map(i -> channelRequest(RpcKind.SINK, sinkPayloadWriter("s" + i))));

    CompletableFuture<List<ClientResponsePayload<Object>>> call =
        client
            .<Object, Object>streamingRequestStreamingResponse(requests, RpcOptions.EMPTY)
            .collectList()
            .toFuture();

    try {
      // The reader holds its first sink element, so once the client filled the window a bounded
      // server has granted nothing more.
      frames.awaitSinkPayloadsSent(SINK_WINDOW);

      Assertions.assertEquals(Collections.singletonList((long) SINK_WINDOW), frames.credits);
      Assertions.assertEquals(SINK_WINDOW, frames.sinkPayloadsSent.get());
    } finally {
      release.tryEmitEmpty();
    }

    Assertions.assertEquals(
        "read " + 3 * SINK_WINDOW, finalResponse(call.get(10, TimeUnit.SECONDS).get(1)));
    Assertions.assertTrue(
        frames.credits.stream().mapToLong(Long::longValue).sum() <= 4 * SINK_WINDOW);
    Assertions.assertTrue(frames.credits.stream().allMatch(n -> n <= SINK_WINDOW));
  }

  @Test
  public void earlyReaderGetsItsCreditOnlyAfterTheFirstResponseOnTheWire() throws Exception {
    FrameLog frames = new FrameLog();
    RpcServerHandler readsEarly =
        channelHandler(
            (initial, sinkElements) -> {
              Flux<ServerResponsePayload> echo =
                  sinkElements.map(element -> response(initial, element.getRequest()));

              return Flux.concat(
                  Mono.fromCallable(ExtendedPingStreamIntegrationTest::firstResponse)
                      .subscribeOn(Schedulers.parallel()),
                  echo);
            });

    RpcClient client = channelClient(startChannelServer(readsEarly), frames);

    StepVerifier.create(
            client.streamingRequestStreamingResponse(
                Flux.just(
                    channelRequest(RpcKind.BIDIRECTIONAL_STREAM, pingArgsWriter("hello")),
                    channelRequest(RpcKind.BIDIRECTIONAL_STREAM, sinkPayloadWriter("a"))),
                RpcOptions.EMPTY))
        .expectNextCount(1)
        .assertNext(element -> Assertions.assertEquals("a", finalResponse(element)))
        .verifyComplete();

    Assertions.assertEquals(Arrays.asList("NEXT", "REQUEST_N"), frames.fromServer.subList(0, 2));
  }

  @Test
  public void bidiEarlyReaderWithATaskTimeoutGetsOnlyAnErrorFrame() throws Exception {
    serverConfig.setTaskExpirationTimeout(Duration.valueOf("100ms"));
    RpcServerHandler readsBeforeItAnswers =
        channelHandler(
            (initial, sinkElements) ->
                sinkElements
                    .thenMany(Flux.<ServerResponsePayload>never())
                    .cancelOn(Schedulers.parallel()));

    channelSendsOnlyAnErrorFrame(
        startChannelServer(readsBeforeItAnswers), RpcKind.BIDIRECTIONAL_STREAM);
  }

  @Test
  public void sinkOutlivesTheTaskTimeoutAfterItsFirstResponse() throws Exception {
    channelOutlivesTheTaskTimeout(RpcKind.SINK, "read 1");
  }

  @Test
  public void bidiOutlivesTheTaskTimeoutAfterItsFirstResponse() throws Exception {
    channelOutlivesTheTaskTimeout(RpcKind.BIDIRECTIONAL_STREAM, "read 1");
  }

  private void channelOutlivesTheTaskTimeout(RpcKind kind, String expected) throws Exception {
    serverConfig.setTaskExpirationTimeout(Duration.valueOf("100ms"));
    FrameLog frames = new FrameLog();
    RpcClient client = channelClient(startChannelServer(countingSink(false)), frames);
    Sinks.Many<ClientRequestPayload<Object>> sink = Sinks.many().unicast().onBackpressureBuffer();

    sink.tryEmitNext(channelRequest(kind, pingArgsWriter("hello")));
    CompletableFuture<List<ClientResponsePayload<Object>>> call =
        client
            .<Object, Object>streamingRequestStreamingResponse(sink.asFlux(), RpcOptions.EMPTY)
            .collectList()
            .toFuture();

    frames.await("REQUEST_N");
    // Idle for longer than the task timeout, which ends at the first response.
    Thread.sleep(300);
    sink.tryEmitNext(channelRequest(kind, sinkPayloadWriter("a")));
    sink.tryEmitComplete();

    Assertions.assertEquals(expected, finalResponse(call.get(10, TimeUnit.SECONDS).get(1)));
    Assertions.assertFalse(frames.fromServer.contains("ERROR"));
  }

  @Test
  public void peerErrorDuringASinkGetsNoServerFrameAndFailsTheReader() throws Exception {
    FrameLog frames = new FrameLog();
    Recorded<Object> read = new Recorded<>();
    RpcClient client = channelClient(startChannelServer(recordingReader(read)), frames);

    Disposable call =
        client.streamingRequestStreamingResponse(openSink(), RpcOptions.EMPTY).subscribe();

    frames.await("REQUEST_N");
    frames.send(
        ErrorFrameCodec.encode(frames.alloc(), 1, new IllegalStateException("sink failed")));

    read.awaitSize(1);
    call.dispose();
    Assertions.assertTrue(read.get(0) instanceof Throwable, "read " + read);
    Assertions.assertEquals(Arrays.asList("NEXT", "REQUEST_N"), frames.fromServer);
  }

  @Test
  public void clientCancelDuringASinkGetsNoServerFrameAndFailsTheReader() throws Exception {
    FrameLog frames = new FrameLog();
    Recorded<Object> read = new Recorded<>();
    RpcClient client = channelClient(startChannelServer(recordingReader(read)), frames);

    Disposable call =
        client.streamingRequestStreamingResponse(openSink(), RpcOptions.EMPTY).subscribe();

    frames.await("REQUEST_N");
    call.dispose();

    read.awaitSize(1);
    Assertions.assertTrue(read.get(0) instanceof CancellationException, "read " + read);
    Assertions.assertEquals(Arrays.asList("NEXT", "REQUEST_N"), frames.fromServer);
  }

  @Test
  public void bidiServerErrorAfterTheFirstResponseEndsTheCallAndFailsTheReader() throws Exception {
    FrameLog frames = new FrameLog();
    Recorded<Object> read = new Recorded<>();
    RpcServerHandler unwritable =
        channelHandler(
            (initial, sinkElements) -> {
              sinkElements.subscribe(read::add, read::add);

              return Flux.just(firstResponse(), response(initial, "unwritable"));
            });

    RpcClient client = channelClient(startChannelServer(unwritable), frames);

    client
        .streamingRequestStreamingResponse(openBidi(), RpcOptions.EMPTY)
        .onErrorResume(e -> Flux.empty())
        .collectList()
        .toFuture()
        .get(5, TimeUnit.SECONDS);

    frames.await("CANCEL");
    read.awaitSize(1);
    Assertions.assertTrue(read.get(0) instanceof Throwable, "read " + read);
    Assertions.assertEquals(
        Arrays.asList("NEXT", "REQUEST_N", "ERROR", "CANCEL"), frames.fromServer);
  }

  @Test
  public void bidiReaderCancelAfterTheFirstResponseEndsOnlyTheSink() throws Exception {
    FrameLog frames = new FrameLog();
    Sinks.Empty<Void> sinkCancelled = Sinks.empty();
    RpcServerHandler takesOne =
        channelHandler(
            (initial, sinkElements) ->
                Flux.concat(
                    Flux.just(firstResponse()),
                    sinkElements
                        .doOnCancel(sinkCancelled::tryEmitEmpty)
                        .take(1)
                        .map(element -> response(initial, element.getRequest())),
                    sinkCancelled.asMono().then(Mono.just(response(initial, "after")))));

    RpcClient client = channelClient(startChannelServer(takesOne), frames);

    StepVerifier.create(
            client.streamingRequestStreamingResponse(
                Flux.concat(
                    Flux.just(
                        channelRequest(RpcKind.BIDIRECTIONAL_STREAM, pingArgsWriter("hello")),
                        channelRequest(RpcKind.BIDIRECTIONAL_STREAM, sinkPayloadWriter("a"))),
                    Flux.never()),
                RpcOptions.EMPTY))
        .expectNextCount(1)
        .assertNext(element -> Assertions.assertEquals("a", finalResponse(element)))
        .assertNext(element -> Assertions.assertEquals("after", finalResponse(element)))
        .verifyComplete();

    Assertions.assertEquals(
        Arrays.asList("NEXT", "REQUEST_N", "NEXT", "CANCEL", "NEXT", "COMPLETE"),
        frames.fromServer);
  }

  @Test
  public void bidiSinkStaysOpenAfterTheStreamCompletes() throws Exception {
    FrameLog frames = new FrameLog();
    Sinks.Empty<Void> streamEnd = Sinks.empty();
    Recorded<Object> read = new Recorded<>();

    RpcServerHandler readsAfterItsStream =
        channelHandler(
            (initial, sinkElements) -> {
              sinkElements.subscribe(read::add, read::add, () -> read.add("complete"));

              return Flux.just(firstResponse(), response(initial, "a"))
                  .concatWith(streamEnd.asMono().then(Mono.empty()));
            });

    RpcClient client = channelClient(startChannelServer(readsAfterItsStream), frames);
    frames.holdSink();

    CompletableFuture<List<ClientResponsePayload<Object>>> call =
        client
            .<Object, Object>streamingRequestStreamingResponse(
                Flux.concat(
                    Flux.just(
                        channelRequest(RpcKind.BIDIRECTIONAL_STREAM, pingArgsWriter("hello"))),
                    Flux.range(0, 5)
                        .map(
                            i ->
                                channelRequest(
                                    RpcKind.BIDIRECTIONAL_STREAM, sinkPayloadWriter("s" + i)))),
                RpcOptions.EMPTY)
            .collectList()
            .toFuture();

    // The server sees the five sink payloads and the sink COMPLETE only after its stream ends.
    try {
      frames.awaitHeldSinkFrames(6);
      streamEnd.tryEmitEmpty();
      frames.await("COMPLETE");
    } finally {
      frames.releaseSink();
    }

    read.awaitSize(6);
    Assertions.assertEquals("complete", read.get(5));
    Assertions.assertEquals(2, call.get(5, TimeUnit.SECONDS).size());
    Assertions.assertEquals(
        Arrays.asList("NEXT", "REQUEST_N", "NEXT", "COMPLETE"), frames.fromServer);
  }

  @Test
  public void bidiSinkThatNoHandlerReadsEndsWhenTheStreamCompletes() throws Exception {
    FrameLog frames = new FrameLog();
    RpcServerHandler ignoresItsSink =
        channelHandler(
            (initial, sinkElements) -> Flux.just(firstResponse(), response(initial, "a")));

    RpcClient client = channelClient(startChannelServer(ignoresItsSink), frames);

    StepVerifier.create(client.streamingRequestStreamingResponse(openBidi(), RpcOptions.EMPTY))
        .expectNextCount(2)
        .verifyComplete();

    frames.await("CANCEL");
    Assertions.assertEquals(Arrays.asList("NEXT", "NEXT", "COMPLETE", "CANCEL"), frames.fromServer);
  }

  @Test
  public void connectionCloseBeforeTheFirstResponseFailsTheReader() throws Exception {
    Recorded<Object> read = new Recorded<>();
    RpcServerHandler neverAnswers =
        channelHandler(
            (initial, sinkElements) -> {
              sinkElements.subscribe(read::add, read::add);

              return Flux.never();
            });

    connectionCloseFailsTheReader(neverAnswers, read, null);
  }

  @Test
  public void connectionCloseDuringASinkFailsTheReader() throws Exception {
    Recorded<Object> read = new Recorded<>();

    connectionCloseFailsTheReader(recordingReader(read), read, "REQUEST_N");
  }

  @Test
  public void connectionCloseAfterTheStreamCompletedFailsAnOpenReader() throws Exception {
    Recorded<Object> read = new Recorded<>();
    RpcServerHandler completesItsStream =
        channelHandler(
            (initial, sinkElements) -> {
              sinkElements.subscribe(read::add, read::add);

              return Flux.just(firstResponse());
            });

    connectionCloseFailsTheReader(completesItsStream, read, "COMPLETE");
  }

  private void connectionCloseFailsTheReader(
      RpcServerHandler handler, Recorded<Object> read, String lastFrame) throws Exception {
    FrameLog frames = new FrameLog();
    RpcClient client = channelClient(startChannelServer(handler), frames);

    client.streamingRequestStreamingResponse(openBidi(), RpcOptions.EMPTY).subscribe(null, e -> {});

    if (lastFrame != null) {
      frames.await(lastFrame);
    }
    client.dispose();

    read.awaitSize(1);
    Assertions.assertTrue(read.get(0) instanceof Throwable, "read " + read);
  }

  @Test
  public void generatedSinkWithAFirstResponseReadsNoneOneOrManyElements() throws Exception {
    FrameLog frames = new FrameLog();
    RpcServerHandler counting =
        sinkHandler(
            (args, sinkElements) ->
                ServerSinks.firstAndFinal(
                    Flux.concat(
                        Mono.just(StreamResponse.fromFirstResponse(pingResponse("first"))),
                        sinkElements
                            .count()
                            .map(n -> StreamResponse.fromData(pingResponse("read " + n))))),
            null);

    RpcClient client = channelClient(startChannelServer(counting), frames);

    for (int elements : new int[] {0, 1, 3}) {
      Flux<ClientRequestPayload<Object>> sink =
          Flux.range(0, elements)
              .map(i -> channelRequest(RpcKind.SINK, sinkPayloadWriter("s" + i)));

      List<ClientResponsePayload<Object>> items =
          client
              .<Object, Object>streamingRequestStreamingResponse(
                  Flux.concat(Flux.just(channelRequest(RpcKind.SINK, pingArgsWriter("hi"))), sink),
                  RpcOptions.EMPTY)
              .collectList()
              .block();

      Assertions.assertEquals(2, items.size());
      Assertions.assertEquals("read " + elements, finalResponse(items.get(1)));
    }
  }

  @Test
  public void generatedSinkSendsADeclaredFinalExceptionInBand() throws Exception {
    sinkSendsItsFinalErrorInBand(new CustomException.Builder().setMessage("declared").build());
  }

  @Test
  public void generatedSinkSendsAnUndeclaredFinalExceptionInBand() throws Exception {
    sinkSendsItsFinalErrorInBand(new IllegalStateException("undeclared"));
  }

  private void sinkSendsItsFinalErrorInBand(Throwable finalError) throws Exception {
    FrameLog frames = new FrameLog();
    RpcServerHandler failing =
        sinkHandler(
            (args, sinkElements) ->
                Mono.just(
                    ResponseAndStream.of(
                        ServerStreams.NO_FIRST_RESPONSE,
                        sinkElements.then(Mono.<PingResponse>error(finalError)).flux())),
            null);

    RpcClient client = channelClient(startChannelServer(failing), frames);

    List<ClientResponsePayload<Object>> items =
        client
            .<Object, Object>streamingRequestStreamingResponse(
                Flux.just(channelRequest(RpcKind.SINK, pingArgsWriter("hi"))), RpcOptions.EMPTY)
            .collectList()
            .block();

    Assertions.assertNotNull(items.get(1).getException(), "items " + items);
    Assertions.assertEquals("NEXT_COMPLETE", frames.fromServer.get(frames.fromServer.size() - 1));
    Assertions.assertFalse(frames.fromServer.contains("ERROR"));
  }

  @Test
  public void generatedSinkFinalThatDoesNotSerializeEndsWithAnErrorFrame() throws Exception {
    FrameLog frames = new FrameLog();
    RpcServerHandler unwritable =
        sinkHandler(
            (args, sinkElements) ->
                Mono.just(
                    ResponseAndStream.of(
                        ServerStreams.NO_FIRST_RESPONSE,
                        sinkElements.then(Mono.just(pingResponse("unwritable"))).flux())),
            null);

    RpcClient client = channelClient(startChannelServer(unwritable), frames);

    client
        .streamingRequestStreamingResponse(
            Flux.just(channelRequest(RpcKind.SINK, pingArgsWriter("hi"))), RpcOptions.EMPTY)
        .collectList()
        .block();

    Assertions.assertEquals("ERROR", frames.fromServer.get(frames.fromServer.size() - 1));
    Assertions.assertFalse(frames.fromServer.contains("NEXT_COMPLETE"));
  }

  @Test
  public void generatedSinkReaderGetsTheDeclaredSinkExceptionOfThePeer() throws Exception {
    RpcServerHandler reportsItsSinkError =
        sinkHandler(
            (args, sinkElements) ->
                Mono.just(
                    ResponseAndStream.of(
                        ServerStreams.NO_FIRST_RESPONSE,
                        sinkElements
                            .then(Mono.just(pingResponse("no error")))
                            .onErrorResume(
                                CustomException.class,
                                e -> Mono.just(pingResponse("declared " + e.getMessage())))
                            .flux())),
            null);

    RpcClient client = channelClient(startChannelServer(reportsItsSinkError), new FrameLog());

    StepVerifier.create(
            client.streamingRequestStreamingResponse(
                Flux.just(
                    channelRequest(RpcKind.SINK, pingArgsWriter("hi")),
                    channelRequest(RpcKind.SINK, sinkPayloadWriter("a")),
                    channelRequest(RpcKind.SINK, declaredSinkExceptionWriter("peer"))),
                RpcOptions.EMPTY))
        .expectNextCount(1)
        .assertNext(last -> Assertions.assertEquals("declared peer", finalResponse(last)))
        .verifyComplete();
  }

  @Test
  public void generatedBidiInterleavesAndEndsWithItsOwnComplete() throws Exception {
    FrameLog frames = new FrameLog();
    RpcServerHandler echo =
        bidiHandler(
            (args, sinkElements) ->
                Mono.just(
                    ResponseAndStream.of(
                        ServerStreams.NO_FIRST_RESPONSE,
                        sinkElements.map(element -> pingResponse(element.getRequest())))));

    RpcClient client = channelClient(startChannelServer(echo), frames);

    StepVerifier.create(
            client.streamingRequestStreamingResponse(
                Flux.just(
                    channelRequest(RpcKind.BIDIRECTIONAL_STREAM, pingArgsWriter("hello")),
                    channelRequest(RpcKind.BIDIRECTIONAL_STREAM, sinkPayloadWriter("a")),
                    channelRequest(RpcKind.BIDIRECTIONAL_STREAM, sinkPayloadWriter("b"))),
                RpcOptions.EMPTY))
        .expectNextCount(1)
        .assertNext(element -> Assertions.assertEquals("a", finalResponse(element)))
        .assertNext(element -> Assertions.assertEquals("b", finalResponse(element)))
        .verifyComplete();

    Assertions.assertEquals(
        Arrays.asList("NEXT", "REQUEST_N", "NEXT", "NEXT", "COMPLETE"), frames.fromServer);
  }

  @Test
  public void generatedBidiSendsItsFirstResponseValue() throws Exception {
    RpcServerHandler withFirst =
        bidiHandler(
            (args, sinkElements) ->
                Mono.just(
                    ResponseAndStream.of(
                        pingResponse("first"),
                        sinkElements.map(element -> pingResponse(element.getRequest())))));

    RpcClient client = channelClient(startChannelServer(withFirst), new FrameLog());

    StepVerifier.create(
            client.streamingRequestStreamingResponse(
                Flux.just(
                    channelRequest(RpcKind.BIDIRECTIONAL_STREAM, pingArgsWriter("hello")),
                    channelRequest(RpcKind.BIDIRECTIONAL_STREAM, sinkPayloadWriter("a"))),
                RpcOptions.EMPTY))
        .assertNext(first -> Assertions.assertEquals("first", firstResponse(first)))
        .assertNext(element -> Assertions.assertEquals("a", finalResponse(element)))
        .verifyComplete();
  }

  @Test
  public void generatedBidiWithoutSinkOrStreamElementsSendsOnlyItsFirstResponse() throws Exception {
    bidiWithoutSinkElements(0);
  }

  @Test
  public void generatedBidiWithoutSinkElementsSendsItsStream() throws Exception {
    bidiWithoutSinkElements(3);
  }

  private void bidiWithoutSinkElements(int streamElements) throws Exception {
    FrameLog frames = new FrameLog();
    RpcServerHandler counts =
        bidiHandler(
            (args, sinkElements) ->
                Mono.just(
                    ResponseAndStream.of(
                        ServerStreams.NO_FIRST_RESPONSE,
                        Flux.range(0, streamElements).map(i -> pingResponse("s" + i)))));

    RpcClient client = channelClient(startChannelServer(counts), frames);

    StepVerifier.create(
            client.streamingRequestStreamingResponse(
                Flux.just(channelRequest(RpcKind.BIDIRECTIONAL_STREAM, pingArgsWriter("hello"))),
                RpcOptions.EMPTY))
        .expectNextCount(1 + streamElements)
        .verifyComplete();

    List<String> expected = new ArrayList<>(Collections.nCopies(1 + streamElements, "NEXT"));
    expected.add("COMPLETE");
    Assertions.assertEquals(expected, frames.fromServer.subList(0, expected.size()));
    Assertions.assertFalse(frames.fromServer.contains("ERROR"));
  }

  @Test
  public void generatedBidiStreamContinuesAfterTheSinkCompletes() throws Exception {
    FrameLog frames = new FrameLog();
    RpcServerHandler echoThenMore =
        bidiHandler(
            (args, sinkElements) ->
                Mono.just(
                    ResponseAndStream.of(
                        ServerStreams.NO_FIRST_RESPONSE,
                        sinkElements
                            .map(element -> pingResponse(element.getRequest()))
                            .concatWith(Flux.just(pingResponse("after"))))));

    RpcClient client = channelClient(startChannelServer(echoThenMore), frames);

    StepVerifier.create(
            client.streamingRequestStreamingResponse(
                Flux.just(
                    channelRequest(RpcKind.BIDIRECTIONAL_STREAM, pingArgsWriter("hello")),
                    channelRequest(RpcKind.BIDIRECTIONAL_STREAM, sinkPayloadWriter("a"))),
                RpcOptions.EMPTY))
        .expectNextCount(1)
        .assertNext(element -> Assertions.assertEquals("a", finalResponse(element)))
        .assertNext(element -> Assertions.assertEquals("after", finalResponse(element)))
        .verifyComplete();

    Assertions.assertEquals(
        Arrays.asList("NEXT", "REQUEST_N", "NEXT", "NEXT", "COMPLETE"), frames.fromServer);
  }

  @Test
  public void generatedBidiDeclaredSinkErrorEndsTheSinkAndTheStreamContinues() throws Exception {
    FrameLog frames = new FrameLog();
    RpcServerHandler reportsItsSinkError =
        bidiHandler(
            (args, sinkElements) ->
                Mono.just(
                    ResponseAndStream.of(
                        ServerStreams.NO_FIRST_RESPONSE,
                        sinkElements
                            .map(element -> pingResponse(element.getRequest()))
                            .onErrorResume(
                                CustomException.class,
                                e -> Flux.just(pingResponse("declared " + e.getMessage()))))));

    RpcClient client = channelClient(startChannelServer(reportsItsSinkError), frames);

    StepVerifier.create(
            client.streamingRequestStreamingResponse(
                Flux.just(
                    channelRequest(RpcKind.BIDIRECTIONAL_STREAM, pingArgsWriter("hello")),
                    channelRequest(RpcKind.BIDIRECTIONAL_STREAM, sinkPayloadWriter("a")),
                    channelRequest(
                        RpcKind.BIDIRECTIONAL_STREAM, declaredSinkExceptionWriter("peer"))),
                RpcOptions.EMPTY))
        .expectNextCount(1)
        .assertNext(element -> Assertions.assertEquals("a", finalResponse(element)))
        .assertNext(element -> Assertions.assertEquals("declared peer", finalResponse(element)))
        .verifyComplete();

    Assertions.assertEquals(
        Arrays.asList("NEXT", "REQUEST_N", "NEXT", "NEXT", "COMPLETE"), frames.fromServer);
  }

  @Test
  public void generatedBidiDeclaredStreamExceptionKeepsTheSinkOpen() throws Exception {
    bidiStreamExceptionKeepsTheSinkOpen(
        new CustomException.Builder().setMessage("declared").build());
  }

  @Test
  public void generatedBidiUndeclaredStreamExceptionKeepsTheSinkOpen() throws Exception {
    bidiStreamExceptionKeepsTheSinkOpen(new IllegalStateException("undeclared"));
  }

  /**
   * A stream exception travels in band, as a payload before the stream COMPLETE, so it ends only
   * the stream half: the reader still gets the sink payloads that the peer sends after it.
   */
  private void bidiStreamExceptionKeepsTheSinkOpen(Throwable streamError) throws Exception {
    FrameLog frames = new FrameLog();
    Recorded<Object> read = new Recorded<>();
    RpcServerHandler failsItsStream =
        bidiHandler(
            (args, sinkElements) -> {
              sinkElements.subscribe(
                  element -> read.add(element.getRequest()), read::add, () -> read.add("complete"));

              return Mono.just(
                  ResponseAndStream.of(
                      ServerStreams.NO_FIRST_RESPONSE,
                      Flux.just(pingResponse("a")).concatWith(Flux.error(streamError))));
            });

    RpcClient client = channelClient(startChannelServer(failsItsStream), frames);
    frames.holdSink();

    CompletableFuture<List<ClientResponsePayload<Object>>> call =
        client
            .<Object, Object>streamingRequestStreamingResponse(
                Flux.just(
                    channelRequest(RpcKind.BIDIRECTIONAL_STREAM, pingArgsWriter("hello")),
                    channelRequest(RpcKind.BIDIRECTIONAL_STREAM, sinkPayloadWriter("s0"))),
                RpcOptions.EMPTY)
            .collectList()
            .toFuture();

    try {
      frames.awaitHeldSinkFrames(2);
      frames.await("COMPLETE");
    } finally {
      frames.releaseSink();
    }

    read.awaitSize(2);
    Assertions.assertEquals(Arrays.asList("s0", "complete"), read);
    Assertions.assertNotNull(call.get(5, TimeUnit.SECONDS).get(2).getException());
    Assertions.assertEquals(
        Arrays.asList("NEXT", "REQUEST_N", "NEXT", "NEXT", "COMPLETE"), frames.fromServer);
  }

  @Test
  public void generatedBidiReaderGetsADeclaredSinkErrorAfterTheStreamCompletes() throws Exception {
    FrameLog frames = new FrameLog();
    Recorded<Object> read = new Recorded<>();
    RpcServerHandler readsAfterItsStream =
        bidiHandler(
            (args, sinkElements) -> {
              sinkElements.subscribe(element -> read.add(element.getRequest()), read::add);

              return Mono.just(
                  ResponseAndStream.of(
                      ServerStreams.NO_FIRST_RESPONSE, Flux.just(pingResponse("a"))));
            });

    RpcClient client = channelClient(startChannelServer(readsAfterItsStream), frames);
    frames.holdSink();

    client
        .<Object, Object>streamingRequestStreamingResponse(
            Flux.just(
                channelRequest(RpcKind.BIDIRECTIONAL_STREAM, pingArgsWriter("hello")),
                channelRequest(RpcKind.BIDIRECTIONAL_STREAM, sinkPayloadWriter("s0")),
                channelRequest(RpcKind.BIDIRECTIONAL_STREAM, declaredSinkExceptionWriter("peer"))),
            RpcOptions.EMPTY)
        .subscribe();

    try {
      frames.awaitHeldSinkFrames(2);
      frames.await("COMPLETE");
    } finally {
      frames.releaseSink();
    }

    read.awaitSize(2);
    Assertions.assertEquals("s0", read.get(0));
    Assertions.assertEquals("peer", ((CustomException) read.get(1)).getMessage());
  }

  @Test
  public void generatedBidiReadsItsSinkOffTheEventLoop() throws Exception {
    Assumptions.assumeTrue(RpcResources.isForceExecutionOffEventLoop());
    List<Thread> threads = new CopyOnWriteArrayList<>();
    RpcServerHandler echo =
        bidiHandler(
            (args, sinkElements) ->
                Mono.just(
                    ResponseAndStream.of(
                        ServerStreams.NO_FIRST_RESPONSE,
                        sinkElements
                            .doOnNext(element -> threads.add(Thread.currentThread()))
                            .map(element -> pingResponse(element.getRequest())))));

    RpcClient client = channelClient(startChannelServer(echo), new FrameLog());

    StepVerifier.create(
            client.streamingRequestStreamingResponse(
                Flux.just(
                    channelRequest(RpcKind.BIDIRECTIONAL_STREAM, pingArgsWriter("hello")),
                    channelRequest(RpcKind.BIDIRECTIONAL_STREAM, sinkPayloadWriter("a")),
                    channelRequest(RpcKind.BIDIRECTIONAL_STREAM, sinkPayloadWriter("b"))),
                RpcOptions.EMPTY))
        .expectNextCount(3)
        .verifyComplete();

    Assertions.assertEquals(2, threads.size());
    for (Thread thread : threads) {
      for (EventExecutor loop : RpcResources.getEventLoopGroup()) {
        Assertions.assertFalse(loop.inEventLoop(thread), thread.getName());
      }
    }
  }

  /** A generated bidi method on the runtime of this commit, as the codegen commit emits it. */
  private static RpcServerHandler bidiHandler(
      BiFunction<List<Object>, Flux<PingRequest>, Mono<ResponseAndStream<Object, PingResponse>>>
          invoke) {
    BidiMethod<Object, PingResponse, PingRequest> method =
        new BidiMethod<>(
            Collections.singletonList(
                new FieldReader<>((short) 1, TType.STRUCT, PingRequest::read0, () -> null)),
            sinkReaders(),
            invoke,
            (response, chain) ->
                response == ServerStreams.NO_FIRST_RESPONSE
                    ? Writers.emptyStruct()
                    : pingResponseWriter((PingResponse) response),
            (response, chain) -> pingResponseWriter((PingResponse) response),
            "channel",
            null,
            null,
            new Class[] {CustomException.class},
            new Integer[] {1});

    return new RpcServerHandler() {
      @Override
      public Flux<ServerResponsePayload> streamingRequestStreamingResponse(
          ServerRequestPayload initial, Publisher<ServerRequestPayload> sinkPayloads) {
        ContextChain chain =
            new ContextChain(Collections.emptyList(), "channel", initial.getRequestContext());

        return ServerBidiStreams.handleBidi(initial, chain, method, sinkPayloads)
            .doFinally(signal -> chain.finishRequest());
      }
    };
  }

  private static String firstResponse(ClientResponsePayload<Object> payload) {
    return ((PingResponse) ((StreamResponse<?, ?>) payload.getData()).getFirstResponse())
        .getResponse();
  }

  @Test
  public void generatedClientSinkReadsTheSinkAndEndsTheRequestOnce() throws Exception {
    SinkHookLog hooks = new SinkHookLog(false);
    startServer(new ExtendedReactivePingService(), ImmutableList.of(hooks));

    StepVerifier.create(
            client(ProtocolId.COMPACT)
                .sinkPings(pingRequest("hello"), Flux.just(pingRequest("a"), pingRequest("b"))))
        .assertNext(last -> Assertions.assertEquals("hello: a,b", last.getResponse()))
        .verifyComplete();

    Assertions.assertTrue(hooks.done.await(5, TimeUnit.SECONDS));
    Assertions.assertEquals(
        Arrays.asList("subscribe", "next", "next", "finally COMPLETE", "done"), hooks.events);
  }

  @Test
  public void generatedClientSinkWithAFirstResponse() throws Exception {
    startServer(new ExtendedReactivePingService(), Collections.emptyList());

    List<StreamResponse<PingResponse, PingResponse>> responses =
        client(ProtocolId.COMPACT)
            .sinkWithFirstResponse(pingRequest("hello"), Flux.just(pingRequest("a")))
            .collectList()
            .block();

    Assertions.assertEquals("hello_first", responses.get(0).getFirstResponse().getResponse());
    Assertions.assertEquals("read 1", responses.get(1).getData().getResponse());
  }

  @Test
  public void generatedSinkSendsADeclaredFunctionExceptionThrownBeforeItsSink() throws Exception {
    startServer(new ExtendedReactivePingService(), Collections.emptyList());

    StepVerifier.create(client(ProtocolId.COMPACT).sinkPings(pingRequest("throw"), Flux.never()))
        .expectError(CustomException.class)
        .verify();
  }

  /**
   * A sink without a first response maps an error that its final response carries against the
   * final-response exceptions, so a declared function exception that comes asynchronously is
   * undeclared there, as for streams.
   */
  @Test
  public void generatedSinkSendsAnAsynchronousFunctionExceptionAsUndeclared() throws Exception {
    startServer(new ExtendedReactivePingService(), Collections.emptyList());

    StepVerifier.create(
            client(ProtocolId.COMPACT).sinkPings(pingRequest("asyncThrow"), Flux.empty()))
        .expectErrorSatisfies(
            e -> Assertions.assertFalse(e instanceof CustomException, e.toString()))
        .verify();
  }

  @Test
  public void generatedSinkSendsADeclaredFinalException() throws Exception {
    startServer(new ExtendedReactivePingService(), Collections.emptyList());

    StepVerifier.create(
            client(ProtocolId.COMPACT).sinkPings(pingRequest("final"), Flux.just(pingRequest("a"))))
        .expectError(FinalException.class)
        .verify();
  }

  @Test
  public void generatedSinkReaderGetsADeclaredSinkException() throws Exception {
    startServer(new ExtendedReactivePingService(), Collections.emptyList());
    RpcClient client = channelClient(serverAddress, new FrameLog());

    StepVerifier.create(
            client.streamingRequestStreamingResponse(
                Flux.just(
                    channelRequest("sinkPings", RpcKind.SINK, pingArgsWriter("hello")),
                    channelRequest("sinkPings", RpcKind.SINK, sinkPayloadWriter("a")),
                    channelRequest(
                        "sinkPings",
                        RpcKind.SINK,
                        sinkExceptionWriter(
                            new SinkException.Builder().setMessage("peer").build()::write0))),
                RpcOptions.EMPTY))
        .expectNextCount(1)
        .assertNext(last -> Assertions.assertEquals("sink exception: peer", finalResponse(last)))
        .verifyComplete();
  }

  @Test
  public void generatedSinkCancelEndsTheSinkWithAnErrorAndTheRequestOnce() throws Exception {
    SinkHookLog hooks = new SinkHookLog(false);
    startServer(new ExtendedReactivePingService(), ImmutableList.of(hooks));

    Disposable call =
        client(ProtocolId.COMPACT)
            .sinkPings(pingRequest("hello"), Flux.concat(Flux.just(pingRequest("a")), Flux.never()))
            .subscribe(null, e -> {});

    hooks.events.awaitSize(2);
    call.dispose();

    Assertions.assertTrue(hooks.done.await(5, TimeUnit.SECONDS));
    Assertions.assertEquals(
        Arrays.asList("subscribe", "next", "cancel", "finally ERROR", "done"), hooks.events);
  }

  @Test
  public void generatedSinkShedByPreprocessEndsTheRequestOnce() throws Exception {
    SinkHookLog hooks = new SinkHookLog(true);
    startServer(new ExtendedReactivePingService(), ImmutableList.of(hooks));

    StepVerifier.create(client(ProtocolId.COMPACT).sinkPings(pingRequest("hello"), Flux.never()))
        .expectError()
        .verify();

    Assertions.assertTrue(hooks.done.await(5, TimeUnit.SECONDS));
    Assertions.assertEquals(Collections.singletonList("done"), hooks.events);
  }

  @Test
  public void generatedServiceSendsUnknownMethodForAChannelMethodItDoesNotHave() throws Exception {
    startServer(new ExtendedReactivePingService(), Collections.emptyList());
    FrameLog frames = new FrameLog();
    RpcClient client = channelClient(serverAddress, frames);

    StepVerifier.create(
            client.streamingRequestStreamingResponse(
                Flux.just(channelRequest("noSuchMethod", RpcKind.SINK, pingArgsWriter("hello"))),
                RpcOptions.EMPTY))
        .assertNext(
            only -> Assertions.assertTrue(only.getException() instanceof TApplicationException))
        .verifyComplete();

    Assertions.assertEquals(Collections.singletonList("ERROR"), frames.fromServer);
  }

  @Test
  public void generatedBidiAnswersEachSinkElement() throws Exception {
    startServer(new ExtendedReactivePingService(), Collections.emptyList());
    FrameLog frames = new FrameLog();
    RpcClient client = channelClient(serverAddress, frames);

    StepVerifier.create(
            client.streamingRequestStreamingResponse(
                Flux.just(
                    channelRequest("bidiPings", RpcKind.BIDIRECTIONAL_STREAM, pingArgsWriter("k")),
                    channelRequest(
                        "bidiPings", RpcKind.BIDIRECTIONAL_STREAM, sinkPayloadWriter("a")),
                    channelRequest(
                        "bidiPings",
                        RpcKind.BIDIRECTIONAL_STREAM,
                        sinkExceptionWriter(
                            new SinkException.Builder().setMessage("peer").build()::write0))),
                RpcOptions.EMPTY))
        .expectNextCount(1)
        .assertNext(element -> Assertions.assertEquals("k: a", finalResponse(element)))
        .assertNext(
            element -> Assertions.assertEquals("sink exception: peer", finalResponse(element)))
        .verifyComplete();

    Assertions.assertEquals(
        Arrays.asList("NEXT", "REQUEST_N", "NEXT", "NEXT", "COMPLETE"), frames.fromServer);
  }

  @Test
  public void generatedBidiSendsADeclaredStreamExceptionInBand() throws Exception {
    startServer(new ExtendedReactivePingService(), Collections.emptyList());
    FrameLog frames = new FrameLog();
    RpcClient client = channelClient(serverAddress, frames);

    List<ClientResponsePayload<Object>> items =
        client
            .<Object, Object>streamingRequestStreamingResponse(
                Flux.just(
                    channelRequest(
                        "bidiPings", RpcKind.BIDIRECTIONAL_STREAM, pingArgsWriter("streamThrow")),
                    channelRequest(
                        "bidiPings", RpcKind.BIDIRECTIONAL_STREAM, sinkPayloadWriter("a"))),
                RpcOptions.EMPTY)
            .collectList()
            .block();

    Assertions.assertEquals("streamThrow: a", finalResponse(items.get(1)));
    Assertions.assertNotNull(items.get(2).getException());
    Assertions.assertFalse(frames.fromServer.contains("ERROR"));
  }

  @Test
  public void generatedBidiSendsBoxedElementsAfterItsFirstResponse() throws Exception {
    startServer(new ExtendedReactivePingService(), Collections.emptyList());
    RpcClient client = channelClient(serverAddress, new FrameLog());

    List<ClientResponsePayload<Object>> items =
        client
            .<Object, Object>streamingRequestStreamingResponse(
                Flux.just(
                    sumsRequest(i32Field((short) 1, 10)),
                    sumsRequest(i32Field((short) 0, 1)),
                    sumsRequest(i32Field((short) 0, 2))),
                RpcOptions.EMPTY)
            .collectList()
            .block();

    Assertions.assertEquals(10, ((StreamResponse<?, ?>) items.get(0).getData()).getFirstResponse());
    Assertions.assertEquals(11L, ((StreamResponse<?, ?>) items.get(1).getData()).getData());
    Assertions.assertEquals(13L, ((StreamResponse<?, ?>) items.get(2).getData()).getData());
  }

  private static ClientRequestPayload<Object> sumsRequest(Writer writer) {
    return ClientRequestPayload.create(
        "PingService",
        writer,
        TProtocol::readI64,
        TProtocol::readI32,
        Collections.emptyMap(),
        Collections.emptyMap(),
        new RequestRpcMetadata.Builder()
            .setName("bidiSums")
            .setKind(RpcKind.BIDIRECTIONAL_STREAM)
            .setProtocol(TProtocolType.TCompact.getProtocolId())
            .setOtherMetadata(Collections.emptyMap())
            .build(),
        Collections.emptyMap());
  }

  private static Writer i32Field(short id, int value) {
    return out -> {
      out.writeFieldBegin(new TField("value", TType.I32, id));
      out.writeI32(value);
      out.writeFieldEnd();
    };
  }

  private static PingRequest pingRequest(String text) {
    return new PingRequest.Builder().setRequest(text).build();
  }

  /** Records the sink hooks and done() of a call in call order; it can shed the call instead. */
  private static final class SinkHookLog implements StreamEventHandler {
    final Recorded<String> events = new Recorded<>();
    final CountDownLatch done = new CountDownLatch(1);
    private final boolean shed;

    SinkHookLog(boolean shed) {
      this.shed = shed;
    }

    @Override
    public Object getContext(String methodName, RequestContext requestContext) {
      return new Object();
    }

    @Override
    public void preprocess(Object context, String methodName) throws TException {
      if (shed) {
        throw new TApplicationException(TApplicationException.LOADSHEDDING, "shed");
      }
    }

    @Override
    public void onSinkSubscribe(Object context, String methodName) {
      events.add("subscribe");
    }

    @Override
    public void onSinkNext(Object context, String methodName, long sequenceNumber) {
      events.add("next");
    }

    @Override
    public void onSinkCancel(Object context, String methodName) {
      events.add("cancel");
    }

    @Override
    public void handleSinkError(Object context, String methodName, Throwable error) {
      events.add("error");
    }

    @Override
    public void onSinkFinally(Object context, String methodName, SinkOutcome outcome) {
      events.add("finally " + outcome);
    }

    @Override
    public void done(Object context, String methodName) {
      events.add("done");
      done.countDown();
    }
  }

  /**
   * A generated sink method on the runtime of this commit, as the codegen commit emits it. A null
   * first-response value means no first response.
   */
  private static RpcServerHandler sinkHandler(
      BiFunction<List<Object>, Flux<PingRequest>, Mono<ResponseAndStream<Object, PingResponse>>>
          invoke,
      Scheduler offLoop) {
    SinkMethod<Object, PingResponse, PingRequest> method =
        new SinkMethod<>(
            Collections.singletonList(
                new FieldReader<>((short) 1, TType.STRUCT, PingRequest::read0, () -> null)),
            sinkReaders(),
            invoke,
            (response, chain) ->
                response == ServerStreams.NO_FIRST_RESPONSE
                    ? Writers.emptyStruct()
                    : pingResponseWriter((PingResponse) response),
            (response, chain) -> pingResponseWriter((PingResponse) response),
            "channel",
            null,
            null,
            new Class[] {CustomException.class},
            new Integer[] {1});

    return new RpcServerHandler() {
      @Override
      public Flux<ServerResponsePayload> streamingRequestStreamingResponse(
          ServerRequestPayload initial, Publisher<ServerRequestPayload> sinkPayloads) {
        ContextChain chain =
            new ContextChain(Collections.emptyList(), "channel", initial.getRequestContext());
        Flux<ServerResponsePayload> responses =
            ServerSinks.handleSink(initial, chain, method, sinkPayloads)
                .doFinally(signal -> chain.finishRequest());

        return offLoop == null ? responses : responses.subscribeOn(offLoop);
      }
    };
  }

  private static Writer declaredSinkExceptionWriter(String message) {
    return sinkExceptionWriter(new CustomException.Builder().setMessage(message).build()::write0);
  }

  /** A sink payload that carries a declared sink exception, at field 1, in place of an element. */
  private static Writer sinkExceptionWriter(Writer exception) {
    return out -> {
      out.writeFieldBegin(new TField("ex", TType.STRUCT, (short) 1));
      exception.write(out);
      out.writeFieldEnd();
    };
  }

  private SocketAddress startChannelServer(RpcServerHandler handler) {
    startServer(handler);
    return serverAddress;
  }

  private RpcClient channelClient(SocketAddress address, FrameLog frames) {
    channelClient =
        new RSocketRpcClientFactory(
                clientConfig(),
                RpcClientUtils.createRSocketConnector()
                    .interceptors(registry -> registry.forConnection(frames)))
            .createRpcClient(address)
            .block();

    return channelClient;
  }

  /** A hand-written channel handler, as an application can write one against RpcServerHandler. */
  private static RpcServerHandler channelHandler(
      BiFunction<ServerRequestPayload, Flux<PingRequest>, Flux<ServerResponsePayload>> serve) {
    return new RpcServerHandler() {
      @Override
      public Flux<ServerResponsePayload> streamingRequestStreamingResponse(
          ServerRequestPayload initial, Publisher<ServerRequestPayload> sinkPayloads) {
        Flux<PingRequest> sinkElements =
            Flux.from(sinkPayloads)
                .map(payload -> (PingRequest) payload.getData(sinkReaders()).get(0));

        return serve.apply(initial, sinkElements);
      }
    };
  }

  /** A channel that answers with the number of sink elements it read, on or off the loop. */
  private static RpcServerHandler countingSink(boolean offLoop) {
    RpcServerHandler counting =
        channelHandler(
            (initial, sinkElements) ->
                Flux.concat(
                    Flux.just(firstResponse()),
                    sinkElements.count().map(n -> response(initial, "read " + n))));

    return new RpcServerHandler() {
      @Override
      public Flux<ServerResponsePayload> streamingRequestStreamingResponse(
          ServerRequestPayload initial, Publisher<ServerRequestPayload> sinkPayloads) {
        Flux<ServerResponsePayload> responses =
            counting.streamingRequestStreamingResponse(initial, sinkPayloads);

        return offLoop ? responses.subscribeOn(RpcResources.getOffLoopScheduler()) : responses;
      }
    };
  }

  /** A channel whose handler records what its sink reader sees and keeps its stream open. */
  private static RpcServerHandler recordingReader(Recorded<Object> read) {
    return channelHandler(
        (initial, sinkElements) -> {
          sinkElements.subscribe(read::add, read::add, () -> read.add("complete"));

          return Flux.concat(Flux.just(firstResponse()), Flux.never());
        });
  }

  private static ServerResponsePayload firstResponse() {
    return ServerResponsePayload.create(
        Writers.emptyStruct(), RpcPayloadUtil.streamFirstResponseMetadata(), null, true);
  }

  private static ServerResponsePayload response(ServerRequestPayload request, String text) {
    return ServerResponsePayload.create(
        pingResponseWriter(pingResponse(text)),
        null,
        RpcPayloadUtil.streamPayloadMetadata(request),
        true);
  }

  /** A sink setup whose sink stays open, so only frames the test sends end it. */
  private static Flux<ClientRequestPayload<Object>> openSink() {
    return Flux.concat(
        Flux.just(channelRequest(RpcKind.SINK, pingArgsWriter("hello"))), Flux.never());
  }

  /** A bidi setup whose sink stays open, so only frames the test sends end it. */
  private static Flux<ClientRequestPayload<Object>> openBidi() {
    return Flux.concat(
        Flux.just(channelRequest(RpcKind.BIDIRECTIONAL_STREAM, pingArgsWriter("hello"))),
        Flux.never());
  }

  private static List<FieldReader<?>> sinkReaders() {
    return Arrays.asList(
        new FieldReader<>((short) 0, TType.STRUCT, PingRequest::read0, () -> null),
        new FieldReader<>((short) 1, TType.STRUCT, CustomException::read0, () -> null));
  }

  /** Writes a PingResponse; the response "unwritable" fails, like an element that cannot encode. */
  private static Writer pingResponseWriter(PingResponse response) {
    return out -> {
      if ("unwritable".equals(response.getResponse())) {
        throw new IllegalStateException("unwritable");
      }

      out.writeStructBegin(new TStruct("result"));
      out.writeFieldBegin(new TField("success", TType.STRUCT, (short) 0));
      response.write0(out);
      out.writeFieldEnd();
      out.writeFieldStop();
      out.writeStructEnd();
    };
  }

  private static ClientRequestPayload<Object> channelRequest(RpcKind kind, Writer writer) {
    return channelRequest("channel", kind, writer);
  }

  private static ClientRequestPayload<Object> channelRequest(
      String name, RpcKind kind, Writer writer) {
    return ClientRequestPayload.create(
        "PingService",
        writer,
        PingResponse::read0,
        PingResponse::read0,
        Collections.emptyMap(),
        Collections.emptyMap(),
        new RequestRpcMetadata.Builder()
            .setName(name)
            .setKind(kind)
            .setProtocol(TProtocolType.TCompact.getProtocolId())
            .setOtherMetadata(Collections.emptyMap())
            .build(),
        Collections.emptyMap());
  }

  private static Writer pingArgsWriter(String request) {
    return out -> {
      out.writeFieldBegin(new TField("key", TType.STRUCT, (short) 1));
      new PingRequest.Builder().setRequest(request).build().write0(out);
      out.writeFieldEnd();
    };
  }

  private static Writer sinkPayloadWriter(String request) {
    return out -> {
      out.writeFieldBegin(new TField("element", TType.STRUCT, (short) 0));
      new PingRequest.Builder().setRequest(request).build().write0(out);
      out.writeFieldEnd();
    };
  }

  private static PingResponse pingResponse(String response) {
    return new PingResponse.Builder().setResponse(response).build();
  }

  private static String finalResponse(ClientResponsePayload<Object> payload) {
    return ((PingResponse) ((StreamResponse<?, ?>) payload.getData()).getData()).getResponse();
  }

  /** A list that a test can wait on, because a handler records into it on another thread. */
  private static final class Recorded<T> extends CopyOnWriteArrayList<T> {
    private final Semaphore added = new Semaphore(0);

    @Override
    public boolean add(T element) {
      super.add(element);
      added.release();
      return true;
    }

    void awaitSize(int size) throws InterruptedException {
      Assertions.assertTrue(added.tryAcquire(size, 5, TimeUnit.SECONDS), "recorded " + this);
      added.release(size);
    }
  }

  /**
   * Records the type of each stream frame the client receives, in arrival order, and sends raw
   * frames as a peer whose halves end on their own. It can hold the sink frames of the client and
   * send them later, as a peer that sends on its sink after the server's stream ends.
   */
  private static final class FrameLog implements DuplexConnectionInterceptor {
    final List<String> fromServer = new CopyOnWriteArrayList<>();
    final List<Long> credits = new CopyOnWriteArrayList<>();
    final AtomicInteger sinkPayloadsSent = new AtomicInteger();
    private final BlockingQueue<String> arrivals = new LinkedBlockingQueue<>();
    private final Queue<ByteBuf> heldSinkFrames = new ConcurrentLinkedQueue<>();
    private final Semaphore heldSinkFrameCount = new Semaphore(0);
    private final Semaphore sinkPayloadCount = new Semaphore(0);
    private volatile boolean holdSink;
    private volatile DuplexConnection source;

    ByteBufAllocator alloc() {
      return source.alloc();
    }

    void send(ByteBuf frame) {
      source.sendFrame(FrameHeaderCodec.streamId(frame), frame);
    }

    void holdSink() {
      holdSink = true;
    }

    void awaitSinkPayloadsSent(int count) throws InterruptedException {
      Assertions.assertTrue(sinkPayloadCount.tryAcquire(count, 5, TimeUnit.SECONDS));
    }

    void awaitHeldSinkFrames(int count) throws InterruptedException {
      Assertions.assertTrue(heldSinkFrameCount.tryAcquire(count, 5, TimeUnit.SECONDS));
    }

    void releaseSink() {
      holdSink = false;
      for (ByteBuf frame = heldSinkFrames.poll(); frame != null; frame = heldSinkFrames.poll()) {
        send(frame);
      }
    }

    void await(String type) throws InterruptedException {
      String arrival;
      do {
        arrival = arrivals.poll(5, TimeUnit.SECONDS);
      } while (arrival != null && !arrival.equals(type));

      Assertions.assertEquals(type, arrival);
    }

    @Override
    public DuplexConnection apply(Type type, DuplexConnection connection) {
      if (type != Type.SOURCE) {
        return connection;
      }

      source = connection;
      return new DuplexConnection() {
        @Override
        public void sendFrame(int streamId, ByteBuf frame) {
          FrameType sent = FrameHeaderCodec.frameType(frame);
          if (streamId != 0 && sent == FrameType.NEXT) {
            sinkPayloadsSent.incrementAndGet();
            sinkPayloadCount.release();
          }

          if (holdSink && (sent == FrameType.NEXT || sent == FrameType.COMPLETE)) {
            heldSinkFrames.add(frame);
            heldSinkFrameCount.release();
            return;
          }
          connection.sendFrame(streamId, frame);
        }

        @Override
        public Flux<ByteBuf> receive() {
          return connection
              .receive()
              .doOnNext(
                  frame -> {
                    if (FrameHeaderCodec.streamId(frame) != 0) {
                      FrameType arrival = FrameHeaderCodec.frameType(frame);
                      if (arrival == FrameType.REQUEST_N) {
                        credits.add(RequestNFrameCodec.requestN(frame));
                      }
                      fromServer.add(arrival.name());
                      arrivals.add(arrival.name());
                    }
                  });
        }

        @Override
        public void sendErrorAndClose(RSocketErrorException errorException) {
          connection.sendErrorAndClose(errorException);
        }

        @Override
        public ByteBufAllocator alloc() {
          return connection.alloc();
        }

        @Override
        public SocketAddress remoteAddress() {
          return connection.remoteAddress();
        }

        @Override
        public void dispose() {
          connection.dispose();
        }

        @Override
        public Mono<Void> onClose() {
          return connection.onClose();
        }

        @Override
        public boolean isDisposed() {
          return connection.isDisposed();
        }
      };
    }
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
