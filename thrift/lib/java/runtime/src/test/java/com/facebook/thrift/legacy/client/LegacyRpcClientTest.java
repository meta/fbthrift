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

package com.facebook.thrift.legacy.client;

import static com.facebook.thrift.legacy.client.LegacyTestFixtures.inboundReply;
import static com.facebook.thrift.legacy.client.LegacyTestFixtures.mockProbingPayload;
import static org.assertj.core.api.Assertions.assertThat;

import com.facebook.thrift.client.CountingScheduler;
import com.facebook.thrift.client.ThriftClientConfig;
import com.facebook.thrift.legacy.codec.HeaderTransportCodec;
import com.facebook.thrift.legacy.codec.LegacyTransportType;
import com.facebook.thrift.legacy.codec.ThriftFrame;
import com.facebook.thrift.payload.ClientRequestPayload;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.thrift.TApplicationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * Proves the Legacy unary path hops exactly once (in the handler): the client adds no second hop,
 * while the oneway path keeps its single write-ack hop.
 */
class LegacyRpcClientTest {
  private final List<Scheduler> delegates = new ArrayList<>();

  @AfterEach
  void disposeSchedulers() {
    for (Scheduler delegate : delegates) {
      delegate.dispose();
    }

    delegates.clear();
  }

  @ParameterizedTest
  @EnumSource(Mode.class)
  void legacyUnaryUsesOnlyHandlerHandoff(Mode mode) throws Exception {
    CountingScheduler handlerCounting =
        mode == Mode.ENABLED
            ? offLoopCounting("legacy-test-handler")
            : new CountingScheduler(Schedulers.immediate());
    CountingScheduler clientCounting = offLoopCounting("legacy-test-client");

    EmbeddedChannel channel =
        new EmbeddedChannel(
            new HeaderTransportCodec(false),
            new ThriftClientHandler(LegacyTransportType.HEADER, handlerCounting));

    LegacyRpcClient client =
        new LegacyRpcClient(channel, clientCounting, true, new ThriftClientConfig());

    AtomicReference<Thread> readerA = new AtomicReference<>();
    AtomicReference<Thread> readerB = new AtomicReference<>();
    ClientRequestPayload<Object> payloadA = mockProbingPayload(readerA);
    ClientRequestPayload<Object> payloadB = mockProbingPayload(readerB);
    ThriftFrame goodReply = inboundReply(0, "probe");
    ByteBuf goodWitness = goodReply.getMessage().retain();
    ThriftFrame badReply = inboundReply(1, "other");
    ByteBuf badWitness = badReply.getMessage().retain();

    AtomicReference<Object> resultA = new AtomicReference<>();
    AtomicReference<Thread> callerA = new AtomicReference<>();
    AtomicReference<Throwable> errorA = new AtomicReference<>();
    CountDownLatch doneA = new CountDownLatch(1);

    client
        .singleRequestSingleResponse(payloadA, null)
        .subscribe(
            value -> {
              resultA.set(value.getData());
              callerA.set(Thread.currentThread());
              doneA.countDown();
            },
            error -> {
              errorA.set(error);
              callerA.set(Thread.currentThread());
              doneA.countDown();
            });

    channel.runPendingTasks();

    channel.writeInbound(goodReply);

    assertThat(doneA.await(5, TimeUnit.SECONDS)).isTrue();

    AtomicReference<Throwable> errorB = new AtomicReference<>();
    AtomicReference<Thread> callerB = new AtomicReference<>();
    CountDownLatch doneB = new CountDownLatch(1);

    client
        .singleRequestSingleResponse(payloadB, null)
        .subscribe(
            value -> {},
            error -> {
              errorB.set(error);
              callerB.set(Thread.currentThread());
              doneB.countDown();
            },
            doneB::countDown);

    channel.runPendingTasks();

    channel.writeInbound(badReply);

    assertThat(doneB.await(5, TimeUnit.SECONDS)).isTrue();

    assertThat(errorA.get()).isNull();
    assertThat(resultA.get()).isEqualTo("ok");
    assertThat(errorB.get()).isInstanceOf(TApplicationException.class);
    assertThat(readerB.get()).isNull();

    if (mode == Mode.ENABLED) {
      assertThat(readerA.get().getName()).startsWith("legacy-test-handler");
      assertThat(callerA.get().getName()).startsWith("legacy-test-handler");
      assertThat(callerB.get().getName()).startsWith("legacy-test-handler");
    } else {
      assertThat(readerA.get()).isSameAs(Thread.currentThread());
      assertThat(callerA.get()).isSameAs(Thread.currentThread());
      assertThat(callerB.get()).isSameAs(Thread.currentThread());
    }

    // One handler decode per reply; the client schedules nothing on top.
    assertThat(handlerCounting.schedules()).isEqualTo(2);
    assertThat(clientCounting.schedules()).isZero();

    // The frame release runs after the caller callback; wait out the worker first.
    awaitIdle(handlerCounting);

    assertThat(goodWitness.refCnt()).isEqualTo(1);
    assertThat(badWitness.refCnt()).isEqualTo(1);

    goodWitness.release();
    badWitness.release();

    releaseOutbound(channel);

    channel.finishAndReleaseAll();
  }

  @ParameterizedTest
  @EnumSource(Mode.class)
  void legacyOnewayKeepsItsHop(Mode mode) throws Exception {
    CountingScheduler handlerCounting = new CountingScheduler(Schedulers.immediate());
    CountingScheduler clientCounting = offLoopCounting("legacy-test-client");

    EmbeddedChannel channel =
        new EmbeddedChannel(
            new HeaderTransportCodec(false),
            new ThriftClientHandler(LegacyTransportType.HEADER, handlerCounting));

    LegacyRpcClient client =
        new LegacyRpcClient(
            channel, clientCounting, mode == Mode.ENABLED, new ThriftClientConfig());

    AtomicReference<Thread> terminalThread = new AtomicReference<>();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    CountDownLatch done = new CountDownLatch(1);

    client
        .singleRequestNoResponse(mockProbingPayload(new AtomicReference<>()), null)
        .subscribe(
            value -> {},
            error -> {
              failure.set(error);
              done.countDown();
            },
            () -> {
              terminalThread.set(Thread.currentThread());
              done.countDown();
            });

    channel.runPendingTasks();

    assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(failure.get()).isNull();

    if (mode == Mode.ENABLED) {
      assertThat(clientCounting.schedules()).isEqualTo(1);
      assertThat(terminalThread.get().getName()).startsWith("legacy-test-client");
    } else {
      assertThat(clientCounting.schedules()).isZero();
      assertThat(terminalThread.get()).isSameAs(Thread.currentThread());
    }

    // No reply exists, so the handler never schedules.
    assertThat(handlerCounting.schedules()).isZero();

    releaseOutbound(channel);

    channel.finishAndReleaseAll();
  }

  private CountingScheduler offLoopCounting(String name) {
    Scheduler delegate = Schedulers.newSingle(name);
    delegates.add(delegate);

    return new CountingScheduler(delegate);
  }

  /** Runs a no-op behind all queued decodes, so the frame release has finished. */
  private static void awaitIdle(Scheduler scheduler) {
    Mono.fromRunnable(() -> {}).subscribeOn(scheduler).block();
  }

  private static void releaseOutbound(EmbeddedChannel channel) {
    Object sent;

    while ((sent = channel.readOutbound()) != null) {
      ReferenceCountUtil.safeRelease(sent);
    }
  }

  private enum Mode {
    ENABLED,
    DISABLED,
  }
}
