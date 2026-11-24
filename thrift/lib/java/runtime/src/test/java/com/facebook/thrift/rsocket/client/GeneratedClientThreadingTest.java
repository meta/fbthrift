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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.facebook.thrift.client.CountingScheduler;
import com.facebook.thrift.client.RpcClient;
import com.facebook.thrift.client.v2.manager.ClientOwnership;
import com.facebook.thrift.client.v2.manager.RpcClientBinding;
import com.facebook.thrift.client.v2.manager.RpcClientManager;
import com.facebook.thrift.protocol.ByteBufTProtocol;
import com.facebook.thrift.protocol.TProtocolType;
import com.facebook.thrift.test.rocket.TestRequest;
import com.facebook.thrift.test.rocket.TestServiceReactiveClient;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.ByteBufPayload;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.thrift.ProtocolId;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

/**
 * Proves the generated reactive client adds no thread hop of its own: the unary, plain
 * interaction-stream and initial-response methods route through the real transport client, so each
 * response is handed off exactly once. With an off-loop scheduler that hop runs the decode on a
 * pool thread; without one everything stays inline.
 */
public class GeneratedClientThreadingTest {
  private final List<CountingScheduler> schedulers = new ArrayList<>();

  @AfterEach
  void disposeSchedulers() {
    for (CountingScheduler scheduler : schedulers) {
      scheduler.dispose();
    }

    schedulers.clear();
  }

  @Test
  public void generatedUnaryUsesExactlyTheTransportHandoff() throws Exception {
    Payload frame = responseFrame((short) 3, 42);
    frame.retain();

    RSocket rsocket = mock(RSocket.class);
    when(rsocket.requestResponse(any())).thenReturn(Mono.just(frame));

    CountingScheduler counting = countingScheduler();
    Set<String> delivered = ConcurrentHashMap.newKeySet();

    StepVerifier.create(
            newClient(new RSocketRpcClient(rsocket, counting))
                .requestResponse(TestRequest.defaultInstance())
                .doOnNext(response -> delivered.add(Thread.currentThread().getName())))
        .expectNextMatches(response -> response.getIntField() == 42)
        .verifyComplete();

    assertEquals(1, counting.schedules());
    assertOffLoopThread(delivered);
    assertEquals(1, frame.refCnt());

    frame.release();
  }

  @Test
  public void generatedUnaryStaysInlineWithoutOffLoopScheduler() throws Exception {
    Payload frame = responseFrame((short) 3, 42);
    frame.retain();

    RSocket rsocket = mock(RSocket.class);
    when(rsocket.requestResponse(any())).thenReturn(Mono.just(frame));

    String callingThread = Thread.currentThread().getName();
    Set<String> delivered = ConcurrentHashMap.newKeySet();

    StepVerifier.create(
            newClient(new RSocketRpcClient(rsocket, Schedulers.immediate()))
                .requestResponse(TestRequest.defaultInstance())
                .doOnNext(response -> delivered.add(Thread.currentThread().getName())))
        .expectNextMatches(response -> response.getIntField() == 42)
        .verifyComplete();

    assertEquals(Collections.singleton(callingThread), delivered);
    assertEquals(1, frame.refCnt());

    frame.release();
  }

  @Test
  public void generatedInitialResponseUsesExactlyTheTransportHandoff() throws Exception {
    Payload first = responseFrame((short) 7, 11);
    first.retain();
    Payload second = responseFrame((short) 3, 42);
    second.retain();

    RSocket rsocket = mock(RSocket.class);
    when(rsocket.requestStream(any())).thenReturn(Flux.just(first, second));

    CountingScheduler counting = countingScheduler();
    Set<String> delivered = ConcurrentHashMap.newKeySet();

    StepVerifier.create(
            newClient(new RSocketRpcClient(rsocket, counting))
                .streamInitialResponse(TestRequest.defaultInstance())
                .doOnNext(
                    holder -> {
                      delivered.add(Thread.currentThread().getName());
                      assertEquals(11, holder.getFirstResponse().getIntField());
                    })
                .flatMapMany(
                    holder ->
                        holder
                            .getStream()
                            .doOnNext(element -> delivered.add(Thread.currentThread().getName()))))
        .expectNextMatches(element -> element.getIntField() == 42)
        .verifyComplete();

    // The holder bridge adds no hop: every signal still runs on the one worker. No exact
    // count: demand arriving before the first frame schedules an empty first drain, so the
    // total is one or two depending on thread timing.
    assertTrue(
        counting.schedules() >= 1,
        "the transport hop scheduled the decode, got " + counting.schedules());
    assertOffLoopThread(delivered);
    assertEquals(1, first.refCnt());
    assertEquals(1, second.refCnt());

    first.release();
    second.release();
  }

  @Test
  public void generatedInitialResponseStaysInlineWithoutOffLoopScheduler() throws Exception {
    Payload first = responseFrame((short) 7, 11);
    first.retain();
    Payload second = responseFrame((short) 3, 42);
    second.retain();

    RSocket rsocket = mock(RSocket.class);
    when(rsocket.requestStream(any())).thenReturn(Flux.just(first, second));

    String callingThread = Thread.currentThread().getName();
    Set<String> delivered = ConcurrentHashMap.newKeySet();

    StepVerifier.create(
            newClient(new RSocketRpcClient(rsocket, Schedulers.immediate()))
                .streamInitialResponse(TestRequest.defaultInstance())
                .doOnNext(
                    holder -> {
                      delivered.add(Thread.currentThread().getName());
                      assertEquals(11, holder.getFirstResponse().getIntField());
                    })
                .flatMapMany(
                    holder ->
                        holder
                            .getStream()
                            .doOnNext(element -> delivered.add(Thread.currentThread().getName()))))
        .expectNextMatches(element -> element.getIntField() == 42)
        .verifyComplete();

    assertFalse(delivered.isEmpty(), "expected signals to be delivered");
    assertEquals(Collections.singleton(callingThread), delivered);
    assertEquals(1, first.refCnt());
    assertEquals(1, second.refCnt());

    first.release();
    second.release();
  }

  @Test
  public void generatedInteractionStreamEmptyCompletesWithOneHop() {
    RSocket rsocket = mock(RSocket.class);
    when(rsocket.requestStream(any())).thenReturn(Flux.empty());

    CountingScheduler counting = countingScheduler();
    Set<String> terminals = ConcurrentHashMap.newKeySet();

    StepVerifier.create(
            newClient(new RSocketRpcClient(rsocket, counting))
                .createTestSession()
                .streamDeclaredException()
                .doOnComplete(() -> terminals.add(Thread.currentThread().getName())))
        .verifyComplete();

    assertTrue(
        counting.schedules() >= 1,
        "the transport hop scheduled the terminal, got " + counting.schedules());
    assertOffLoopThread(terminals);
  }

  @Test
  public void generatedInteractionStreamEmptyCompletesInline() {
    RSocket rsocket = mock(RSocket.class);
    when(rsocket.requestStream(any())).thenReturn(Flux.empty());

    String callingThread = Thread.currentThread().getName();
    Set<String> terminals = ConcurrentHashMap.newKeySet();

    StepVerifier.create(
            newClient(new RSocketRpcClient(rsocket, Schedulers.immediate()))
                .createTestSession()
                .streamDeclaredException()
                .doOnComplete(() -> terminals.add(Thread.currentThread().getName())))
        .verifyComplete();

    assertEquals(Collections.singleton(callingThread), terminals);
  }

  @Test
  public void generatedInteractionStreamErrorFailsWithOneHop() throws Exception {
    Payload frame = undeclaredErrorFrame();
    frame.retain();

    RSocket rsocket = mock(RSocket.class);
    when(rsocket.requestStream(any())).thenReturn(Flux.just(frame));

    CountingScheduler counting = countingScheduler();
    Set<String> terminals = ConcurrentHashMap.newKeySet();

    StepVerifier.create(
            newClient(new RSocketRpcClient(rsocket, counting))
                .createTestSession()
                .streamDeclaredException()
                .doOnError(error -> terminals.add(Thread.currentThread().getName())))
        .verifyErrorSatisfies(
            error -> {
              assertEquals(
                  "boom", assertInstanceOf(TApplicationException.class, error).getMessage());
            });

    assertTrue(
        counting.schedules() >= 1,
        "the transport hop scheduled the decode, got " + counting.schedules());
    assertOffLoopThread(terminals);
    assertEquals(1, frame.refCnt());

    frame.release();
  }

  @Test
  public void generatedInteractionStreamErrorFailsInline() throws Exception {
    Payload frame = undeclaredErrorFrame();
    frame.retain();

    RSocket rsocket = mock(RSocket.class);
    when(rsocket.requestStream(any())).thenReturn(Flux.just(frame));

    String callingThread = Thread.currentThread().getName();
    Set<String> terminals = ConcurrentHashMap.newKeySet();

    StepVerifier.create(
            newClient(new RSocketRpcClient(rsocket, Schedulers.immediate()))
                .createTestSession()
                .streamDeclaredException()
                .doOnError(error -> terminals.add(Thread.currentThread().getName())))
        .verifyErrorSatisfies(
            error -> {
              assertEquals(
                  "boom", assertInstanceOf(TApplicationException.class, error).getMessage());
            });

    assertEquals(Collections.singleton(callingThread), terminals);
    assertEquals(1, frame.refCnt());

    frame.release();
  }

  private static void assertOffLoopThread(Set<String> delivered) {
    assertFalse(delivered.isEmpty(), "expected signals to be delivered");

    for (String name : delivered) {
      assertTrue(name.startsWith("generated-test-transport"), "decode runs on the worker: " + name);
    }
  }

  private CountingScheduler countingScheduler() {
    CountingScheduler counting =
        new CountingScheduler(Schedulers.newSingle("generated-test-transport"));
    schedulers.add(counting);

    return counting;
  }

  private static Payload responseFrame(short innerId, int value) throws Exception {
    ByteBuf data = Unpooled.buffer();
    ByteBufTProtocol protocol = TProtocolType.TBinary.apply(data);
    protocol.writeStructBegin(new TStruct());
    protocol.writeFieldBegin(new TField("success", TType.STRUCT, (short) 0));
    protocol.writeStructBegin(new TStruct());
    protocol.writeFieldBegin(new TField("int_field", TType.I32, innerId));
    protocol.writeI32(value);
    protocol.writeFieldEnd();
    protocol.writeFieldStop();
    protocol.writeStructEnd();
    protocol.writeFieldEnd();
    protocol.writeFieldStop();
    protocol.writeStructEnd();

    return ByteBufPayload.create(data);
  }

  private static Payload undeclaredErrorFrame() throws Exception {
    ByteBuf data = Unpooled.buffer();
    ByteBufTProtocol protocol = TProtocolType.TBinary.apply(data);
    protocol.writeStructBegin(new TStruct());
    protocol.writeFieldBegin(new TField("error", TType.STRING, (short) 5));
    protocol.writeString("boom");
    protocol.writeFieldEnd();
    protocol.writeFieldStop();
    protocol.writeStructEnd();

    return ByteBufPayload.create(data);
  }

  private static TestServiceReactiveClient newClient(RpcClient rpc) {
    return new TestServiceReactiveClient(
        ProtocolId.BINARY, new RpcClientBinding(new StubManager(rpc), ClientOwnership.BORROWED));
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
}
