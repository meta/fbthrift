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

import com.facebook.swift.service.ContextChain;
import com.facebook.swift.service.StreamEventHandler;
import com.facebook.swift.service.ThriftEventHandler;
import com.facebook.thrift.protocol.TProtocolType;
import com.facebook.thrift.rsocket.util.PayloadUtil;
import com.facebook.thrift.server.RpcServerHandler;
import com.facebook.thrift.test.rocket.TestException;
import com.facebook.thrift.test.rocket.TestRequest;
import com.facebook.thrift.test.rocket.TestResponse;
import com.facebook.thrift.test.rocket.TestService;
import com.facebook.thrift.util.resources.RpcResources;
import com.google.common.collect.ImmutableList;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.EventLoop;
import io.netty.channel.nio.NioEventLoopGroup;
import io.rsocket.Payload;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.apache.thrift.ProtocolId;
import org.apache.thrift.RequestRpcMetadata;
import org.apache.thrift.RpcKind;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SignalType;
import reactor.test.StepVerifier;

/**
 * Locks in the server-side {@link StreamEventHandler} contract for streaming RPCs: one registration
 * list serving plain and stream handlers together with the same per-request context, callback
 * order, per-element counts, credit visibility, the error hook before the terminal plus the
 * terminal outcome on complete / error / cancel, registration-order fan-out, off-loop execution, a
 * throwing hook failing the stream, no hook after the terminal, the terminal strictly before {@code
 * done()}, and serialization under concurrent load. Every test fails without the hook type, its
 * pipeline wiring, or the behavior it covers.
 *
 * <p>Hooks run inline on the signal thread under the chain's stream lock; thread placement off the
 * loop comes from the pipeline's {@code subscribeOn}/{@code cancelOn} hops, which these tests prove
 * by thread name (and by {@code loop.inEventLoop} on a real Netty loop for
 * subscribe/request/cancel).
 */
public class TestStreamEventHandlerCallback {

  static Stream<Arguments> data() {
    return Stream.of(Arguments.of(ProtocolId.COMPACT), Arguments.of(ProtocolId.BINARY));
  }

  private ProtocolId protocolId;
  private final ByteBufAllocator alloc = RpcResources.getByteBufAllocator();

  private RpcServerHandler handlerWith(ThriftEventHandler... handlers) {
    return TestService.Reactive.serverHandlerBuilder(new TestServiceHandler())
        .setEventHandlers(ImmutableList.copyOf(handlers))
        .build();
  }

  private Payload request(String method) {
    RequestRpcMetadata metadata =
        new RequestRpcMetadata.Builder()
            .setProtocol(protocolId)
            .setName(method)
            .setKind(RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE)
            .build();
    TestRequest input = new TestRequest.Builder().setIntField(0).setStrField("foo").build();

    ByteBuf meta = alloc.buffer();
    TProtocol metaProtocol = TProtocolType.fromProtocolId(ProtocolId.COMPACT).apply(meta);
    metadata.write0(metaProtocol);
    meta.retain();

    ByteBuf data = alloc.buffer();
    TProtocol protocol = TProtocolType.fromProtocolId(metadata.getProtocol()).apply(data);
    protocol.writeStructBegin(new TStruct());
    protocol.writeFieldBegin(new TField("struct", (byte) TType.STRUCT, (short) 1));
    input.write0(protocol);
    protocol.writeFieldEnd();
    protocol.writeFieldStop();
    protocol.writeStructEnd();
    data.retain();

    return PayloadUtil.createPayload(data, meta);
  }

  private ThriftServerRSocket rocket(RpcServerHandler handler) {
    return new ThriftServerRSocket(handler, alloc);
  }

  /**
   * Event-loop threads are named {@code thrift-eventloop-*} (see {@code
   * ResourceConfiguration.eventLoopGroupThreadPrefix}); no hook may run on one when execution is
   * forced off the loop, and no hook may run on the subscribing thread either.
   */
  private static void assertHooksOffLoop(RecordingStreamHandler hook, String subscribingThread) {
    Assertions.assertFalse(hook.hookThreads.isEmpty(), "expected hooks to have fired");
    for (String thread : hook.hookThreads) {
      Assertions.assertNotEquals(
          subscribingThread, thread, "hook ran on the subscribing thread: " + thread);
      Assertions.assertFalse(
          thread.startsWith("thrift-eventloop"), "hook ran on the event loop: " + thread);
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void streamCompleteOrderAndOutcome(ProtocolId protocolId) throws Exception {
    this.protocolId = protocolId;
    Assertions.assertTrue(
        RpcResources.isForceExecutionOffEventLoop(),
        "test requires the default force-execution-off-event-loop configuration");
    String subscribingThread = Thread.currentThread().getName();
    RecordingStreamHandler hook = new RecordingStreamHandler();
    ThriftServerRSocket rocket = rocket(handlerWith(hook));

    // 1 first-response frame + 10 elements, then completion.
    StepVerifier.Step<Payload> steps =
        StepVerifier.create(rocket.requestStream(request("streamResponse")));
    for (int i = 0; i < 11; i++) {
      steps = steps.assertNext(response -> {});
    }
    steps.verifyComplete();

    Assertions.assertTrue(
        hook.terminalLatch.await(5, TimeUnit.SECONDS), "terminal hook never fired");
    Assertions.assertTrue(hook.doneLatch.await(5, TimeUnit.SECONDS), "done never fired");
    List<String> expected = new ArrayList<>();
    expected.add("subscribe");
    for (int i = 1; i <= 10; i++) {
      expected.add("element-" + i);
    }
    expected.add("terminal-COMPLETE");
    expected.add("done");
    Assertions.assertEquals(expected, hook.orderedEvents());
    Assertions.assertEquals(StreamEventHandler.StreamOutcome.COMPLETE, hook.terminalOutcome);
    Assertions.assertNull(hook.errorCause, "complete fires no error hook");
    Assertions.assertEquals(1, hook.terminals.get());
    Assertions.assertEquals(1, hook.maxConcurrency.get(), "hooks must be serialized per stream");
    Assertions.assertFalse(hook.hookContexts.isEmpty(), "expected stream hooks to have fired");
    for (Object seen : hook.hookContexts) {
      Assertions.assertSame(hook.contextMarker, seen, "stream hooks share the getContext object");
    }
    assertHooksOffLoop(hook, subscribingThread);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void streamDeclaredErrorMidStream(ProtocolId protocolId) throws Exception {
    this.protocolId = protocolId;
    String subscribingThread = Thread.currentThread().getName();
    RecordingStreamHandler hook = new RecordingStreamHandler();
    RecordingRequestHandler requestHook = new RecordingRequestHandler();
    ThriftServerRSocket rocket = rocket(handlerWith(requestHook, hook));

    // 1 first-response frame + 4 elements + 1 terminal declared-exception frame.
    StepVerifier.Step<Payload> steps =
        StepVerifier.create(rocket.requestStream(request("streamDeclaredExceptionFluxError")));
    for (int i = 0; i < 6; i++) {
      steps = steps.assertNext(response -> {});
    }
    steps.verifyComplete();

    Assertions.assertTrue(
        hook.terminalLatch.await(5, TimeUnit.SECONDS), "terminal hook never fired");
    Assertions.assertTrue(hook.doneLatch.await(5, TimeUnit.SECONDS), "done never fired");
    List<String> expected = new ArrayList<>();
    expected.add("subscribe");
    for (int i = 1; i <= 4; i++) {
      expected.add("element-" + i);
    }
    expected.add("error");
    expected.add("terminal-ERROR");
    expected.add("done");
    Assertions.assertEquals(expected, hook.orderedEvents());
    Assertions.assertEquals(StreamEventHandler.StreamOutcome.ERROR, hook.terminalOutcome);
    Assertions.assertTrue(hook.errorCause instanceof TestException, "got " + hook.errorCause);
    // The terminal exception frame is not an element, and the known-exception path must fire
    // preWriteException exactly like the request-response and unknown-exception paths do.
    Assertions.assertEquals(
        ImmutableList.of("preWriteException", "declaredUserException", "postWriteException"),
        requestHook.exceptionEvents);
    assertHooksOffLoop(hook, subscribingThread);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void streamUndeclaredErrorTerminal(ProtocolId protocolId) throws Exception {
    this.protocolId = protocolId;
    String subscribingThread = Thread.currentThread().getName();
    RecordingStreamHandler hook = new RecordingStreamHandler();
    ThriftServerRSocket rocket = rocket(handlerWith(hook));

    StepVerifier.Step<Payload> steps =
        StepVerifier.create(rocket.requestStream(request("streamUndeclaredException")));
    // First-response frame + terminal undeclared-exception frame, no elements.
    steps = steps.assertNext(response -> {});
    steps = steps.assertNext(response -> {});
    steps.verifyComplete();

    Assertions.assertTrue(
        hook.terminalLatch.await(5, TimeUnit.SECONDS), "terminal hook never fired");
    Assertions.assertTrue(hook.doneLatch.await(5, TimeUnit.SECONDS), "done never fired");
    Assertions.assertEquals(
        ImmutableList.of("subscribe", "error", "terminal-ERROR", "done"), hook.orderedEvents());
    Assertions.assertEquals(StreamEventHandler.StreamOutcome.ERROR, hook.terminalOutcome);
    Assertions.assertTrue(
        hook.errorCause instanceof IllegalArgumentException, "got " + hook.errorCause);
    assertHooksOffLoop(hook, subscribingThread);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void streamFunctionDeclaredSyncThrowTerminal(ProtocolId protocolId) throws Exception {
    this.protocolId = protocolId;
    RecordingStreamHandler hook = new RecordingStreamHandler();
    RecordingRequestHandler requestHook = new RecordingRequestHandler();
    ThriftServerRSocket rocket = rocket(handlerWith(requestHook, hook));

    // The delegate throws synchronously, before the stream phase starts: a function-level
    // declared exception frame, then completion. The error is request-level, so NO stream hook
    // fires -- not even subscribe -- but done() still does.
    StepVerifier.Step<Payload> steps =
        StepVerifier.create(rocket.requestStream(request("streamFunctionExceptionSyncThrow")));
    steps = steps.assertNext(response -> {});
    steps.verifyComplete();

    Assertions.assertTrue(hook.doneLatch.await(5, TimeUnit.SECONDS), "done never fired");
    Assertions.assertEquals(ImmutableList.of("done"), hook.orderedEvents());
    Assertions.assertEquals(0, hook.terminals.get(), "no stream terminal on an initial error");
    Assertions.assertNull(hook.errorCause, "no error hook on an initial error");
    Assertions.assertEquals(
        ImmutableList.of("preWriteException", "declaredUserException", "postWriteException"),
        requestHook.exceptionEvents);
  }

  /**
   * A cancel (or assembly failure) that completes the request before the worker ever subscribes
   * must record only {@code done}: {@code finishRequest} terminates under the stream lock first, so
   * the late subscribe and terminal racing in record nothing after it.
   */
  @Test
  public void finishRequestBeforeSubscribeRecordsOnlyDone() {
    RecordingStreamHandler hook = new RecordingStreamHandler();
    ContextChain chain = new ContextChain(ImmutableList.of(hook), "Svc.method", null);

    chain.finishRequest();
    chain.onStreamSubscribe();
    chain.onStreamNext();
    chain.handleStreamError(new RuntimeException("late"));
    chain.finishStream(SignalType.ON_COMPLETE);

    Assertions.assertEquals(ImmutableList.of("done"), hook.orderedEvents());
    Assertions.assertEquals(0, hook.terminals.get());
  }

  @ParameterizedTest
  @MethodSource("data")
  public void streamCancelTerminal(ProtocolId protocolId) throws Exception {
    this.protocolId = protocolId;
    String subscribingThread = Thread.currentThread().getName();
    RecordingStreamHandler hook = new RecordingStreamHandler();
    ThriftServerRSocket rocket = rocket(handlerWith(hook));

    // Consume the first response AND one element before canceling: the element frame proves the
    // stream phase subscribed (subscribe fired), so the cancel deterministically terminates a
    // begun stream. Canceling on the first frame alone races stream-phase subscription, and a
    // cancel that wins records only done (see finishRequestBeforeSubscribeRecordsOnlyDone).
    StepVerifier.create(rocket.requestStream(request("streamResponse")), 2)
        .assertNext(response -> {})
        .assertNext(response -> {})
        .thenCancel()
        .verify();

    Assertions.assertTrue(
        hook.terminalLatch.await(5, TimeUnit.SECONDS), "terminal hook never fired");
    Assertions.assertTrue(hook.doneLatch.await(5, TimeUnit.SECONDS), "done never fired");
    Assertions.assertEquals(StreamEventHandler.StreamOutcome.CANCEL, hook.terminalOutcome);
    Assertions.assertNull(hook.errorCause, "cancel fires no error hook");
    Assertions.assertEquals(1, hook.terminals.get());
    Assertions.assertEquals("subscribe", hook.orderedEvents().get(0));
    List<String> cancelEvents = hook.orderedEvents();
    Assertions.assertEquals("terminal-CANCEL", cancelEvents.get(cancelEvents.size() - 2));
    Assertions.assertEquals("done", cancelEvents.get(cancelEvents.size() - 1));
    // Element sequence numbers stay strictly increasing even across the cancel race.
    List<Long> sequences = hook.elementSequences();
    for (int i = 1; i < sequences.size(); i++) {
      Assertions.assertTrue(sequences.get(i) > sequences.get(i - 1));
    }
    assertHooksOffLoop(hook, subscribingThread);
  }

  /**
   * An element paused upstream of the tap while a cancel lands must never surface as a hook after
   * the terminal: the terminated guard closes with the terminal and the late signal is discarded.
   */
  @ParameterizedTest
  @MethodSource("data")
  public void terminalLastWithElementInFlight(ProtocolId protocolId) throws Exception {
    this.protocolId = protocolId;
    RecordingStreamHandler hook = new RecordingStreamHandler();
    CountDownLatch inMap = new CountDownLatch(1);
    CountDownLatch releaseMap = new CountDownLatch(1);
    TestServiceHandler pausingService =
        new TestServiceHandler() {
          @Override
          public Flux<TestResponse> streamResponse(TestRequest request) {
            // Emission runs on a dedicated thread (never the subscribing thread), so pausing
            // the user map below cannot deadlock the test thread driving the subscription.
            return Flux.<TestResponse>create(
                    sink -> {
                      Thread emitter =
                          new Thread(
                              () ->
                                  sink.next(
                                      new TestResponse.Builder()
                                          .setIntField(1)
                                          .setStrField("one")
                                          .build()));
                      emitter.setDaemon(true);
                      emitter.start();
                    })
                .map(
                    response -> {
                      inMap.countDown();
                      try {
                        Assertions.assertTrue(
                            releaseMap.await(10, TimeUnit.SECONDS), "map gate timed out");
                      } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                      }
                      return response;
                    });
          }
        };
    RpcServerHandler handler =
        TestService.Reactive.serverHandlerBuilder(pausingService)
            .setEventHandlers(ImmutableList.of(hook))
            .build();
    ThriftServerRSocket rocket = new ThriftServerRSocket(handler, alloc);

    AtomicReference<Subscription> subscription = new AtomicReference<>();
    CountDownLatch subscribed = new CountDownLatch(1);
    rocket
        .requestStream(request("streamResponse"))
        .subscribe(
            new BaseSubscriber<Payload>() {
              @Override
              protected void hookOnSubscribe(Subscription s) {
                subscription.set(s);
                subscribed.countDown();
                request(Long.MAX_VALUE);
              }

              @Override
              protected void hookOnNext(Payload value) {
                value.release();
              }
            });
    Assertions.assertTrue(subscribed.await(5, TimeUnit.SECONDS), "never subscribed");
    // The element is now paused in the user map, upstream of the hook tap.
    Assertions.assertTrue(inMap.await(5, TimeUnit.SECONDS), "element never reached the map");
    subscription.get().cancel();
    Assertions.assertTrue(
        hook.terminalLatch.await(5, TimeUnit.SECONDS), "terminal hook never fired");
    Assertions.assertTrue(hook.doneLatch.await(5, TimeUnit.SECONDS), "done never fired");
    // Release the paused element only after the terminal fired: it must be discarded.
    releaseMap.countDown();
    Thread.sleep(500);

    Assertions.assertEquals(StreamEventHandler.StreamOutcome.CANCEL, hook.terminalOutcome);
    Assertions.assertEquals(
        ImmutableList.of("subscribe", "terminal-CANCEL", "done"), hook.orderedEvents());
  }

  @ParameterizedTest
  @MethodSource("data")
  public void streamCreditsReceived(ProtocolId protocolId) throws Exception {
    this.protocolId = protocolId;
    String subscribingThread = Thread.currentThread().getName();
    RecordingStreamHandler hook = new RecordingStreamHandler();
    ThriftServerRSocket rocket = rocket(handlerWith(hook));

    // 1 first-response frame + 10 elements; demand arrives as two explicit requests.
    StepVerifier.Step<Payload> steps =
        StepVerifier.create(rocket.requestStream(request("streamResponse")), 0).thenRequest(5);
    for (int i = 0; i < 5; i++) {
      steps = steps.assertNext(response -> {});
    }
    steps = steps.thenRequest(6);
    for (int i = 0; i < 6; i++) {
      steps = steps.assertNext(response -> {});
    }
    steps.verifyComplete();

    Assertions.assertTrue(
        hook.terminalLatch.await(5, TimeUnit.SECONDS), "terminal hook never fired");
    // done() appends to the same lists read below: await it first.
    Assertions.assertTrue(hook.doneLatch.await(5, TimeUnit.SECONDS), "done never fired");
    long deadline = System.currentTimeMillis() + 5000;
    while (!hook.creditEvents.equals(ImmutableList.of(5, 6))
        && System.currentTimeMillis() < deadline) {
      Thread.sleep(50);
    }
    Assertions.assertEquals(ImmutableList.of(5, 6), hook.creditEvents);
    Assertions.assertEquals(10, hook.elementSequences().size());
    assertHooksOffLoop(hook, subscribingThread);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void streamWithFirstResponseCountsDataElements(ProtocolId protocolId) throws Exception {
    this.protocolId = protocolId;
    String subscribingThread = Thread.currentThread().getName();
    RecordingStreamHandler hook = new RecordingStreamHandler();
    ThriftServerRSocket rocket = rocket(handlerWith(hook));

    // Typed first response + 10 elements: the first response is not an element.
    StepVerifier.Step<Payload> steps =
        StepVerifier.create(rocket.requestStream(request("streamInitialResponse")));
    for (int i = 0; i < 11; i++) {
      steps = steps.assertNext(response -> {});
    }
    steps.verifyComplete();

    Assertions.assertTrue(
        hook.terminalLatch.await(5, TimeUnit.SECONDS), "terminal hook never fired");
    Assertions.assertTrue(hook.doneLatch.await(5, TimeUnit.SECONDS), "done never fired");
    List<String> expected = new ArrayList<>();
    expected.add("subscribe");
    for (int i = 1; i <= 10; i++) {
      expected.add("element-" + i);
    }
    expected.add("terminal-COMPLETE");
    expected.add("done");
    Assertions.assertEquals(expected, hook.orderedEvents());
    assertHooksOffLoop(hook, subscribingThread);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void multipleHandlersFireInRegistrationOrder(ProtocolId protocolId) throws Exception {
    this.protocolId = protocolId;
    String subscribingThread = Thread.currentThread().getName();
    List<String> shared = Collections.synchronizedList(new ArrayList<>());
    RecordingStreamHandler first = new RecordingStreamHandler("A", shared);
    RecordingStreamHandler second = new RecordingStreamHandler("B", shared);
    ThriftServerRSocket rocket = rocket(handlerWith(first, second));

    StepVerifier.Step<Payload> steps =
        StepVerifier.create(rocket.requestStream(request("streamResponse")));
    for (int i = 0; i < 11; i++) {
      steps = steps.assertNext(response -> {});
    }
    steps.verifyComplete();

    Assertions.assertTrue(
        first.terminalLatch.await(5, TimeUnit.SECONDS), "terminal hook never fired");
    Assertions.assertTrue(
        second.terminalLatch.await(5, TimeUnit.SECONDS), "terminal hook never fired");
    Assertions.assertTrue(first.doneLatch.await(5, TimeUnit.SECONDS), "done never fired");
    Assertions.assertTrue(second.doneLatch.await(5, TimeUnit.SECONDS), "done never fired");
    // Every callback fans out to A before B: subscribe, each element, then terminal, then done.
    List<String> expected = new ArrayList<>();
    expected.add("A:subscribe");
    expected.add("B:subscribe");
    for (int i = 1; i <= 10; i++) {
      expected.add("A:element-" + i);
      expected.add("B:element-" + i);
    }
    expected.add("A:terminal-COMPLETE");
    expected.add("B:terminal-COMPLETE");
    expected.add("A:done");
    expected.add("B:done");
    Assertions.assertEquals(expected, shared);
    assertHooksOffLoop(first, subscribingThread);
    assertHooksOffLoop(second, subscribingThread);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void throwingHookPropagatesAndFailsStream(ProtocolId protocolId) throws Exception {
    this.protocolId = protocolId;
    String subscribingThread = Thread.currentThread().getName();
    StreamEventHandler throwing =
        new StreamEventHandler() {
          @Override
          public void onStreamNext(Object context, String methodName, long sequenceNumber) {
            throw new RuntimeException("boom onStreamNext");
          }
        };
    RecordingStreamHandler peer = new RecordingStreamHandler();
    ThriftServerRSocket rocket = rocket(handlerWith(throwing, peer));

    // A throwing hook propagates like any other handler hook: the first element trips it, so
    // the stream ends after the first response plus the terminal frame instead of completing
    // with all 11 frames.
    StepVerifier.Step<Payload> steps =
        StepVerifier.create(rocket.requestStream(request("streamResponse")));
    steps = steps.assertNext(response -> {});
    steps = steps.assertNext(response -> {});
    steps.verifyComplete();

    // The peer still sees subscribe, the error, the terminal, and done: only the throwing call
    // itself aborts; later fan-outs run to completion.
    Assertions.assertTrue(
        peer.terminalLatch.await(5, TimeUnit.SECONDS), "terminal hook never fired");
    Assertions.assertTrue(peer.doneLatch.await(5, TimeUnit.SECONDS), "done never fired");
    Assertions.assertEquals(
        ImmutableList.of("subscribe", "error", "terminal-ERROR", "done"), peer.orderedEvents());
    Assertions.assertTrue(
        peer.errorCause instanceof RuntimeException
            && peer.errorCause.getMessage().contains("boom onStreamNext"),
        "got " + peer.errorCause);
    assertHooksOffLoop(peer, subscribingThread);
  }

  /**
   * Seeded concurrent load: four threads hammering demand while 2000 elements flow. Hooks must
   * never overlap (max concurrency 1), every element is observed exactly once in emission order,
   * and every credit is observed, with exactly one COMPLETE terminal.
   */
  @ParameterizedTest
  @MethodSource("data")
  public void concurrentCreditAndElementLoadStaysSerialized(ProtocolId protocolId)
      throws Exception {
    this.protocolId = protocolId;
    String subscribingThread = Thread.currentThread().getName();
    RecordingStreamHandler hook = new RecordingStreamHandler();
    // Elements arrive on an independent emitter thread while demand threads hammer request():
    // credits and elements genuinely race on the chain lock (a Flux.range would emit on the
    // requesting worker instead).
    TestServiceHandler manyElementService =
        new TestServiceHandler() {
          @Override
          public Flux<TestResponse> streamResponse(TestRequest request) {
            return Flux.create(
                sink -> {
                  Thread emitter =
                      new Thread(
                          () -> {
                            for (int i = 0; i < 2000; i++) {
                              sink.next(
                                  new TestResponse.Builder()
                                      .setIntField(i)
                                      .setStrField("s" + i)
                                      .build());
                            }
                            sink.complete();
                          });
                  emitter.setDaemon(true);
                  emitter.start();
                });
          }
        };
    RpcServerHandler handler =
        TestService.Reactive.serverHandlerBuilder(manyElementService)
            .setEventHandlers(ImmutableList.of(hook))
            .build();
    ThriftServerRSocket rocket = new ThriftServerRSocket(handler, alloc);

    // 1 first-response frame + 2000 elements: seed demand as 1 + 4 x 500 from racing threads.
    AtomicReference<Subscription> subscription = new AtomicReference<>();
    CountDownLatch subscribed = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(1);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    rocket
        .requestStream(request("streamResponse"))
        .subscribe(
            new BaseSubscriber<Payload>() {
              @Override
              protected void hookOnSubscribe(Subscription s) {
                subscription.set(s);
                subscribed.countDown();
                request(1);
              }

              @Override
              protected void hookOnNext(Payload value) {
                value.release();
              }

              @Override
              protected void hookOnComplete() {
                done.countDown();
              }

              @Override
              protected void hookOnError(Throwable t) {
                failure.set(t);
                done.countDown();
              }
            });
    Assertions.assertTrue(subscribed.await(5, TimeUnit.SECONDS), "never subscribed");
    List<Thread> demandThreads = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      Thread t = new Thread(() -> subscription.get().request(500));
      t.setDaemon(true);
      demandThreads.add(t);
      t.start();
    }
    for (Thread t : demandThreads) {
      t.join(10000);
    }
    Assertions.assertTrue(done.await(15, TimeUnit.SECONDS), "stream never completed");
    Assertions.assertNull(failure.get(), "stream errored: " + failure.get());
    Assertions.assertTrue(
        hook.terminalLatch.await(5, TimeUnit.SECONDS), "terminal hook never fired");
    // done() appends to the same event list elementSequences() iterates: await it first, or
    // the append races the iteration.
    Assertions.assertTrue(hook.doneLatch.await(5, TimeUnit.SECONDS), "done never fired");

    Assertions.assertEquals(1, hook.maxConcurrency.get(), "hooks must be serialized per stream");
    List<Long> sequences = hook.elementSequences();
    Assertions.assertEquals(2000, sequences.size(), "every element observed exactly once");
    for (int i = 0; i < sequences.size(); i++) {
      Assertions.assertEquals(i + 1, sequences.get(i).longValue());
    }
    int credits = 0;
    for (int c : hook.creditEvents) {
      credits += c;
    }
    Assertions.assertEquals(2001, credits, "every credit observed, got " + hook.creditEvents);
    Assertions.assertEquals(StreamEventHandler.StreamOutcome.COMPLETE, hook.terminalOutcome);
    Assertions.assertEquals(1, hook.terminals.get());
    assertHooksOffLoop(hook, subscribingThread);
  }

  /**
   * One registration list serves a plain {@link ThriftEventHandler} and a {@link
   * StreamEventHandler} together: the plain handler observes the request-response lifecycle with
   * its own context and never sees stream calls, while the stream handler gets the SAME context
   * object it returned from {@code getContext} in every hook.
   */
  @Test
  public void oneListServesPlainAndStreamHandlersWithSameContext() {
    List<Object> plainPreRead = Collections.synchronizedList(new ArrayList<>());
    Object plainMarker = new Object();
    ThriftEventHandler plain =
        new ThriftEventHandler() {
          @Override
          public Object getContext(
              String methodName, com.facebook.nifty.core.RequestContext requestContext) {
            return plainMarker;
          }

          @Override
          public void preRead(Object context, String methodName) {
            plainPreRead.add(context);
          }
        };
    RecordingStreamHandler stream = new RecordingStreamHandler();
    ContextChain chain = new ContextChain(ImmutableList.of(plain, stream), "Svc.method", null);
    Assertions.assertTrue(chain.hasStreamHandlers());

    chain.preRead();
    chain.onStreamSubscribe();
    chain.onStreamNext();
    chain.handleStreamError(new RuntimeException("boom"));
    chain.finishStream(SignalType.ON_COMPLETE);

    Assertions.assertEquals(ImmutableList.of(plainMarker), plainPreRead);
    Assertions.assertFalse(stream.hookContexts.isEmpty(), "expected stream hooks to have fired");
    for (Object seen : stream.hookContexts) {
      Assertions.assertSame(stream.contextMarker, seen);
    }
    Assertions.assertEquals(
        ImmutableList.of("subscribe", "element-1", "error", "terminal-ERROR", "done"),
        stream.orderedEvents());
  }

  /**
   * Signals arriving after the terminal are discarded by the delivered-terminal guard, so the order
   * stays exact without any scheduler involved.
   */
  @Test
  public void inlineTerminalAdmitsNothingAfter() {
    RecordingStreamHandler hook = new RecordingStreamHandler();
    ContextChain chain = new ContextChain(ImmutableList.of(hook), "Svc.method", null);

    chain.onStreamSubscribe();
    chain.onStreamNext();
    chain.finishStream(SignalType.ON_COMPLETE);
    chain.onStreamNext();
    chain.onStreamCredit(5);

    Assertions.assertEquals(
        ImmutableList.of("subscribe", "element-1", "terminal-COMPLETE", "done"),
        hook.orderedEvents());
    Assertions.assertEquals(1, hook.terminals.get());
  }

  /**
   * Two threads hammering element and credit hooks directly on one chain: hooks never overlap (max
   * concurrency 1), the terminal fires exactly once, and nothing is recorded after it (late signals
   * are discarded by the terminated guard, so observed counts never exceed issued). The first hook
   * rendezvous-waits for a companion entrant, so removing the lock turns the max-concurrency
   * assertion red instead of passing by luck.
   */
  @Test
  public void concurrentHooksStaySerializedAndTerminalLast() throws Exception {
    AtomicInteger inside = new AtomicInteger();
    AtomicInteger maxObserved = new AtomicInteger();
    AtomicBoolean rendezvousArmed = new AtomicBoolean(true);
    CountDownLatch companionArrived = new CountDownLatch(1);
    CountDownLatch firstHook = new CountDownLatch(1);
    AtomicInteger terminals = new AtomicInteger();
    AtomicBoolean hookAfterTerminal = new AtomicBoolean(false);
    AtomicBoolean terminalSeen = new AtomicBoolean(false);
    StreamEventHandler rendezvous =
        new StreamEventHandler() {
          @Override
          public void onStreamNext(Object context, String methodName, long sequenceNumber) {
            enter();
          }

          @Override
          public void onStreamCredit(Object context, String methodName, int credits) {
            enter();
          }

          private void enter() {
            if (terminalSeen.get()) {
              hookAfterTerminal.set(true);
            }
            int current = inside.incrementAndGet();
            maxObserved.accumulateAndGet(current, Math::max);
            try {
              if (current >= 2) {
                companionArrived.countDown();
              } else if (rendezvousArmed.compareAndSet(true, false)) {
                try {
                  companionArrived.await(1, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
              }
            } finally {
              inside.decrementAndGet();
            }
            firstHook.countDown();
          }

          @Override
          public void onStreamFinally(Object context, String methodName, StreamOutcome outcome) {
            terminals.incrementAndGet();
            terminalSeen.set(true);
            firstHook.countDown();
          }
        };
    ContextChain chain = new ContextChain(ImmutableList.of(rendezvous), "Svc.method", null);
    int perThread = 500;
    CountDownLatch start = new CountDownLatch(1);
    Thread elements =
        new Thread(
            () -> {
              awaitUninterruptibly(start);
              for (int i = 1; i <= perThread; i++) {
                chain.onStreamNext();
              }
            });
    Thread credits =
        new Thread(
            () -> {
              awaitUninterruptibly(start);
              for (int i = 0; i < perThread; i++) {
                chain.onStreamCredit(1);
              }
            });
    elements.setDaemon(true);
    credits.setDaemon(true);
    // The stream begins before the hammers start; finishStream only fires the terminal for a
    // begun stream, mirroring the pipeline (doOnSubscribe always runs on subscription).
    chain.onStreamSubscribe();
    elements.start();
    credits.start();
    start.countDown();
    // Let the hammers land at least one hook before racing the terminal.
    Assertions.assertTrue(firstHook.await(10, TimeUnit.SECONDS), "no hook ever fired");
    chain.finishStream(SignalType.ON_COMPLETE);
    elements.join(10000);
    credits.join(10000);
    Assertions.assertFalse(elements.isAlive() || credits.isAlive(), "hammer threads hung");

    Assertions.assertEquals(1, maxObserved.get(), "hooks must be serialized per stream");
    Assertions.assertEquals(1, terminals.get(), "terminal fires exactly once");
    Assertions.assertFalse(hookAfterTerminal.get(), "no hook after the terminal");
  }

  private static void awaitUninterruptibly(CountDownLatch latch) {
    boolean interrupted = false;
    while (true) {
      try {
        latch.await();
        break;
      } catch (InterruptedException e) {
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * A chain with no stream handlers fires nothing: every stream and sink signal is a gated no-op,
   * and request completion still fires {@code done()} exactly once. The strict handler fails on ANY
   * hook call, so removing a gate turns this red instead of passing by luck.
   */
  @Test
  public void chainWithoutStreamHandlersSkipsStreamWork() {
    AtomicInteger doneCount = new AtomicInteger();
    ThriftEventHandler strict =
        new ThriftEventHandler() {
          private void unexpected(String hook) {
            throw new AssertionError("plain chain fired " + hook);
          }

          @Override
          public Object getContext(String methodName, com.facebook.nifty.core.RequestContext req) {
            return null;
          }

          @Override
          public void preprocess(Object context, String methodName) {
            unexpected("preprocess");
          }

          @Override
          public void preRead(Object context, String methodName) {
            unexpected("preRead");
          }

          @Override
          public void postRead(Object context, String methodName, Object[] args) {
            unexpected("postRead");
          }

          @Override
          public void preWrite(Object context, String methodName, Object result) {
            unexpected("preWrite");
          }

          @Override
          public void preWriteException(Object context, String methodName, Throwable t) {
            unexpected("preWriteException");
          }

          @Override
          public void postWrite(Object context, String methodName, Object result) {
            unexpected("postWrite");
          }

          @Override
          public void postWriteException(Object context, String methodName, Throwable t) {
            unexpected("postWriteException");
          }

          @Override
          public void done(Object context, String methodName) {
            doneCount.incrementAndGet();
          }
        };
    ContextChain chain = new ContextChain(ImmutableList.of(strict), "Svc.method", null);
    Assertions.assertFalse(chain.hasStreamHandlers());
    chain.onStreamSubscribe();
    chain.onStreamNext();
    chain.onStreamCredit(1);
    chain.handleStreamError(new RuntimeException("unobserved"));
    chain.onSinkSubscribe();
    chain.onSinkNext();
    chain.onSinkConsumed();
    chain.onSinkCancel();
    chain.onSinkCredit(1);
    chain.handleSinkError(new RuntimeException("unobserved"));
    chain.onSinkFinally(StreamEventHandler.SinkOutcome.COMPLETE);
    chain.finishStream(SignalType.ON_COMPLETE);
    chain.finishRequest();
    Assertions.assertEquals(1, doneCount.get(), "done fires exactly once");
  }

  /**
   * Subscription is initiated, extra demand is requested, and cancel is issued -- all on a real
   * Netty event-loop thread -- while hooks must never observe {@code loop.inEventLoop()}. Unlike
   * the thread-name assertions above, this fails if a hook ever runs on the loop itself.
   */
  @ParameterizedTest
  @MethodSource("data")
  public void hooksNeverOnNettyLoop(ProtocolId protocolId) throws Exception {
    this.protocolId = protocolId;
    RecordingStreamHandler hook = new RecordingStreamHandler();
    ThriftServerRSocket rocket = rocket(handlerWith(hook));

    NioEventLoopGroup group = new NioEventLoopGroup(1);
    EventLoop loop = group.next();
    hook.loop = loop;
    try {
      AtomicReference<Subscription> subscription = new AtomicReference<>();
      CountDownLatch subscribed = new CountDownLatch(1);
      CountDownLatch firstFrame = new CountDownLatch(1);
      CountDownLatch secondFrame = new CountDownLatch(2);
      // Demand covers the first response plus one element: the element frame proves the stream
      // phase subscribed, so the loop-issued cancel below deterministically terminates a begun
      // stream (canceling on the first frame alone could win the race with stream-phase
      // subscription and record only done).
      loop.submit(
              () ->
                  rocket
                      .requestStream(request("streamResponse"))
                      .subscribe(
                          new BaseSubscriber<Payload>() {
                            @Override
                            protected void hookOnSubscribe(Subscription s) {
                              subscription.set(s);
                              subscribed.countDown();
                              request(2);
                            }

                            @Override
                            protected void hookOnNext(Payload value) {
                              value.release();
                              firstFrame.countDown();
                              secondFrame.countDown();
                            }
                          }))
          .get(5, TimeUnit.SECONDS);
      Assertions.assertTrue(subscribed.await(5, TimeUnit.SECONDS), "never subscribed");
      Assertions.assertTrue(firstFrame.await(5, TimeUnit.SECONDS), "no frame arrived");
      Assertions.assertTrue(secondFrame.await(5, TimeUnit.SECONDS), "no element arrived");
      // Extra demand and cancel, both issued on the loop.
      loop.submit(() -> subscription.get().request(1)).get(5, TimeUnit.SECONDS);
      loop.submit(() -> subscription.get().cancel()).get(5, TimeUnit.SECONDS);
      Assertions.assertTrue(
          hook.terminalLatch.await(5, TimeUnit.SECONDS), "terminal hook never fired");
      Assertions.assertTrue(hook.doneLatch.await(5, TimeUnit.SECONDS), "done never fired");

      Assertions.assertEquals(0, hook.onLoop.get(), "a hook ran on the event loop");
      Assertions.assertEquals(StreamEventHandler.StreamOutcome.CANCEL, hook.terminalOutcome);
      Assertions.assertEquals(1, hook.terminals.get());
      Assertions.assertEquals("subscribe", hook.orderedEvents().get(0));
      List<String> loopEvents = hook.orderedEvents();
      Assertions.assertEquals("terminal-CANCEL", loopEvents.get(loopEvents.size() - 2));
      Assertions.assertEquals("done", loopEvents.get(loopEvents.size() - 1));
      List<Long> sequences = hook.elementSequences();
      for (int i = 1; i < sequences.size(); i++) {
        Assertions.assertTrue(sequences.get(i) > sequences.get(i - 1));
      }
    } finally {
      group.shutdownGracefully();
    }
  }

  /**
   * Sink callbacks fan out in call order with exactly one terminal, and nothing fires after it. The
   * sink-server diff wires these to the upload path; this pins the chain-level order.
   */
  @Test
  public void sinkCallbacksFireInOrderWithExactlyOneTerminal() {
    RecordingStreamHandler hook = new RecordingStreamHandler();
    ContextChain chain = new ContextChain(ImmutableList.of(hook), "Svc.method", null);
    Assertions.assertTrue(chain.hasStreamHandlers());

    chain.onSinkSubscribe();
    chain.onSinkNext();
    chain.onSinkConsumed();
    chain.onSinkCredit(7);
    chain.onSinkNext();
    chain.handleSinkError(new IllegalStateException("sink broke"));
    chain.onSinkFinally(StreamEventHandler.SinkOutcome.ERROR);
    chain.onSinkNext();
    chain.onSinkFinally(StreamEventHandler.SinkOutcome.ERROR);

    Assertions.assertEquals(
        ImmutableList.of(
            "sinkSubscribe",
            "sinkNext-1",
            "sinkConsumed",
            "sinkCredit-7",
            "sinkNext-2",
            "sinkError",
            "sinkFinally-ERROR"),
        hook.sinkEvents());
    Assertions.assertTrue(
        hook.sinkErrorCause instanceof IllegalStateException, "got " + hook.sinkErrorCause);
    Assertions.assertEquals(1, hook.sinkTerminals.get());
  }

  /** Records stream-hook order, threads, credits, elements, the error, the terminal, and done. */
  private static class RecordingStreamHandler implements StreamEventHandler {
    private final String prefix;
    private final List<String> sharedLog;
    private final List<String> events = Collections.synchronizedList(new ArrayList<>());
    private final List<Object> hookContexts = Collections.synchronizedList(new ArrayList<>());
    private final List<Integer> creditEvents = Collections.synchronizedList(new ArrayList<>());
    private final Set<String> hookThreads = Collections.synchronizedSet(new HashSet<>());
    private final CountDownLatch terminalLatch = new CountDownLatch(1);
    private final CountDownLatch doneLatch = new CountDownLatch(1);
    private volatile StreamOutcome terminalOutcome;
    private volatile Throwable errorCause;
    private final AtomicInteger terminals = new AtomicInteger();
    private final AtomicInteger active = new AtomicInteger();
    private final AtomicInteger maxConcurrency = new AtomicInteger();
    private final AtomicInteger onLoop = new AtomicInteger();
    private volatile EventLoop loop;
    private volatile Object contextMarker;

    RecordingStreamHandler() {
      this("", null);
    }

    RecordingStreamHandler(String prefix, List<String> sharedLog) {
      this.prefix = prefix;
      this.sharedLog = sharedLog;
    }

    @Override
    public Object getContext(String methodName, com.facebook.nifty.core.RequestContext request) {
      contextMarker = new Object();
      return contextMarker;
    }

    private void record(String event) {
      track(
          () -> {
            events.add(event);
            if (sharedLog != null) {
              sharedLog.add(prefix + ":" + event);
            }
          });
    }

    /**
     * Concurrency accounting shared by every hook, including credits (which log no event -- demand
     * interleaves loosely with elements, so it stays out of the ordered event list). The short
     * yield spin widens the overlap window so a missing lock shows up as maxConcurrency &gt; 1
     * instead of passing by luck.
     */
    private void track(Runnable body) {
      int inFlight = active.incrementAndGet();
      maxConcurrency.accumulateAndGet(inFlight, Math::max);
      try {
        for (int i = 0; i < 50; i++) {
          Thread.yield();
        }
        if (loop != null && loop.inEventLoop()) {
          onLoop.incrementAndGet();
        }
        hookThreads.add(Thread.currentThread().getName());
        body.run();
      } finally {
        active.decrementAndGet();
      }
    }

    List<String> orderedEvents() {
      return new ArrayList<>(events);
    }

    List<Long> elementSequences() {
      List<Long> sequences = new ArrayList<>();
      for (String event : events) {
        if (event.startsWith("element-")) {
          sequences.add(Long.parseLong(event.substring("element-".length())));
        }
      }
      return sequences;
    }

    List<String> sinkEvents() {
      List<String> sink = new ArrayList<>();
      for (String event : events) {
        if (event.startsWith("sink") || event.equals("sinkError")) {
          sink.add(event);
        }
      }
      return sink;
    }

    private volatile Throwable sinkErrorCause;
    private final AtomicInteger sinkTerminals = new AtomicInteger();

    @Override
    public void onStreamSubscribe(Object context, String methodName) {
      hookContexts.add(context);
      record("subscribe");
    }

    @Override
    public void onStreamNext(Object context, String methodName, long sequenceNumber) {
      hookContexts.add(context);
      record("element-" + sequenceNumber);
    }

    @Override
    public void onStreamCredit(Object context, String methodName, int credits) {
      hookContexts.add(context);
      track(() -> creditEvents.add(credits));
    }

    @Override
    public void handleStreamError(Object context, String methodName, Throwable error) {
      hookContexts.add(context);
      errorCause = error;
      record("error");
    }

    @Override
    public void onStreamFinally(Object context, String methodName, StreamOutcome outcome) {
      hookContexts.add(context);
      record("terminal-" + outcome);
      terminalOutcome = outcome;
      terminals.incrementAndGet();
      terminalLatch.countDown();
    }

    @Override
    public void done(Object context, String methodName) {
      record("done");
      doneLatch.countDown();
    }

    @Override
    public void onSinkSubscribe(Object context, String methodName) {
      record("sinkSubscribe");
    }

    @Override
    public void onSinkNext(Object context, String methodName, long sequenceNumber) {
      record("sinkNext-" + sequenceNumber);
    }

    @Override
    public void onSinkConsumed(Object context, String methodName) {
      record("sinkConsumed");
    }

    @Override
    public void onSinkCancel(Object context, String methodName) {
      record("sinkCancel");
    }

    @Override
    public void onSinkCredit(Object context, String methodName, int credits) {
      record("sinkCredit-" + credits);
    }

    @Override
    public void handleSinkError(Object context, String methodName, Throwable error) {
      sinkErrorCause = error;
      record("sinkError");
    }

    @Override
    public void onSinkFinally(
        Object context, String methodName, StreamEventHandler.SinkOutcome outcome) {
      record("sinkFinally-" + outcome);
      sinkTerminals.incrementAndGet();
    }
  }

  /** Records the request-response exception-hook order around stream errors. */
  private static class RecordingRequestHandler implements ThriftEventHandler {
    private final List<String> exceptionEvents = Collections.synchronizedList(new ArrayList<>());
    private final AtomicBoolean doneFired = new AtomicBoolean(false);

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

    @Override
    public void done(Object context, String methodName) {
      doneFired.set(true);
    }
  }
}
