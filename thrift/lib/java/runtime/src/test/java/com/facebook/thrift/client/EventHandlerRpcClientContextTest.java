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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.facebook.nifty.core.RequestContext;
import com.facebook.nifty.core.RequestContextScope;
import com.facebook.nifty.core.RequestContexts;
import com.facebook.swift.service.ThriftClientEventHandler;
import com.facebook.thrift.client.v2.manager.ClientOwnership;
import com.facebook.thrift.client.v2.manager.RpcClientBinding;
import com.facebook.thrift.client.v2.manager.RpcClientManager;
import com.facebook.thrift.payload.ClientRequestPayload;
import com.facebook.thrift.payload.ClientResponsePayload;
import com.facebook.thrift.payload.Reader;
import com.facebook.thrift.payload.Writer;
import com.facebook.thrift.test.rocket.TestRequest;
import com.facebook.thrift.test.rocket.TestResponse;
import com.facebook.thrift.test.rocket.TestService;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.thrift.ProtocolId;
import org.apache.thrift.RequestRpcMetadata;
import org.apache.thrift.client.ClientRequestContext;
import org.apache.thrift.protocol.TProtocol;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class EventHandlerRpcClientContextTest {
  private static final String CONTEXT_HEADER = "test_context";

  private final RequestContext requestA = Mockito.mock(RequestContext.class);
  private final RequestContext requestB = Mockito.mock(RequestContext.class);
  private final RecordingHandler handler = new RecordingHandler();
  private final RecordingTransport transport = new RecordingTransport();
  private final EventHandlerRpcClient client =
      new EventHandlerRpcClient(transport, Collections.singletonList(handler));

  /** One call, as a generated client builds it: its metadata map is shared by every attempt. */
  private final ClientRequestPayload<Object> call = callPayload();

  @Test
  public void handlersDoNotRunUntilTheCallIsSubscribed() {
    Mono<ClientResponsePayload<Object>> assembled = send();

    assertEquals(0, handler.contextsSeen.size());

    RequestContextScope.get(requestA, assembled::block);
    assertEquals(1, handler.contextsSeen.size());
  }

  @Test
  public void eachSubscriptionRunsTheHandlersOnceAndIsDoneOnce() {
    Mono<ClientResponsePayload<Object>> assembled = send();

    RequestContextScope.get(requestA, assembled::block);
    RequestContextScope.get(requestA, assembled::block);

    assertEquals(2, handler.contextsSeen.size());
    assertEquals(2, handler.doneContexts.size());
  }

  @Test
  public void theReactorContextWinsOverTheBoundScope() {
    RequestContextScope.get(
        requestB,
        () -> send().contextWrite(context -> RequestContext.toContext(context, requestA)).block());

    assertSame(requestA, handler.contextsSeen.get(0));
  }

  @Test
  public void theBoundScopeIsTheFallback() {
    RequestContextScope.get(requestA, () -> send().block());

    assertSame(requestA, handler.contextsSeen.get(0));
  }

  @Test
  public void theAssemblySentinelYieldsNoContextRatherThanAFailedCall() {
    RequestContextScope.get(RequestContextScope.ASSEMBLY_SENTINEL, () -> send().block());

    assertNull(handler.contextsSeen.get(0));
  }

  @Test
  public void aResubscribeWithoutAContextDoesNotResendThePreviousHeader() {
    Mono<ClientResponsePayload<Object>> assembled = send();

    RequestContextScope.get(requestA, assembled::block);
    assembled.block();

    assertEquals("A", transport.headersSent.get(0).get(CONTEXT_HEADER));
    assertFalse(transport.headersSent.get(1).containsKey(CONTEXT_HEADER));
    assertFalse(
        call.getRequestRpcMetadata().getOtherMetadata().containsKey(CONTEXT_HEADER),
        "the call's own metadata must not keep a subscription's headers");
  }

  @Test
  public void concurrentSubscriptionsEachSendTheirOwnHeader() {
    Mono<ClientResponsePayload<Object>> assembled = send();

    Mono.zip(
            assembled
                .contextWrite(context -> RequestContext.toContext(context, requestA))
                .subscribeOn(Schedulers.parallel()),
            assembled
                .contextWrite(context -> RequestContext.toContext(context, requestB))
                .subscribeOn(Schedulers.parallel()))
        .block();

    List<String> headers = new ArrayList<>();
    transport.headersSent.forEach(sent -> headers.add(sent.get(CONTEXT_HEADER)));
    Collections.sort(headers);
    assertEquals(Arrays.asList("A", "B"), headers);
  }

  @Test
  public void doneRunsInTheCallContextOnTheCompletingThreadAndLeavesItClean() throws Exception {
    Scheduler completing = Schedulers.newSingle("completing");
    try {
      transport.completeOn = completing;

      RequestContextScope.get(requestA, () -> send().block());

      // doFinally fires after the subscriber is released, so wait for it rather than for block().
      assertTrue(handler.firstDone.await(10, TimeUnit.SECONDS));
      assertSame(requestA, handler.doneContexts.get(0));
      assertEquals(
          Optional.empty(),
          Mono.fromCallable(() -> Optional.ofNullable(RequestContexts.getCurrentContext()))
              .subscribeOn(completing)
              .block());
    } finally {
      completing.dispose();
    }
  }

  @Test
  public void aFailedCallRunsOnErrorAndThenDoneInTheCallContext() {
    transport.failWith = new IllegalStateException("transport failed");

    assertThrows(
        IllegalStateException.class, () -> RequestContextScope.get(requestA, () -> send().block()));

    assertSame(requestA, handler.onErrorContexts.get(0));
    assertSame(requestA, handler.doneContexts.get(0));
  }

  @Test
  public void aCancelledCallIsDoneOnceInTheCallContext() {
    transport.neverComplete = true;

    Disposable subscription = RequestContextScope.get(requestA, () -> send().subscribe());
    subscription.dispose();

    assertEquals(1, handler.doneContexts.size());
    assertSame(requestA, handler.doneContexts.get(0));
  }

  /** The handler runs at subscribe time, so its failure is an error of the Mono, not a throw. */
  @Test
  public void aHandlerFailureSurfacesAsAnErrorOfTheReturnedMono() {
    EventHandlerRpcClient failingClient =
        new EventHandlerRpcClient(
            transport,
            Collections.singletonList(
                new ThriftClientEventHandler() {
                  @Override
                  public Object getContext(String methodName, ClientRequestContext context) {
                    throw new IllegalArgumentException("handler failed");
                  }
                }));

    Mono<ClientResponsePayload<Object>> assembled =
        failingClient.singleRequestSingleResponse(call, RpcOptions.EMPTY);

    assertThrows(IllegalArgumentException.class, assembled::block);
  }

  @Test
  public void theNoResponsePathResolvesTheContextTheSameWay() {
    RequestContextScope.run(
        requestA, () -> client.singleRequestNoResponse(call, RpcOptions.EMPTY).block());

    assertSame(requestA, handler.contextsSeen.get(0));
    assertEquals("A", transport.headersSent.get(0).get(CONTEXT_HEADER));
    assertSame(requestA, handler.doneContexts.get(0));
  }

  /** The transport writes and reads on its own thread; the handlers still see the call context. */
  @Test
  public void writeAndReadCallbacksRunInTheCallContextOnTheTransportThread() {
    transport.writeAndReadOn = Schedulers.single();

    RequestContextScope.get(requestA, () -> send().block());

    assertSame(requestA, handler.callbackContexts.get("preWrite"));
    assertSame(requestA, handler.callbackContexts.get("postWrite"));
    assertSame(requestA, handler.callbackContexts.get("preRead"));
    assertSame(requestA, handler.callbackContexts.get("postRead"));
  }

  /**
   * Through a generated client whose connection completes on another thread, as a cold connection
   * does: a blocking caller's request must still reach the handlers.
   */
  @Test
  public void aGeneratedClientCarriesTheCallersRequestOverAColdConnection() {
    RpcClientManager coldConnection =
        new RpcClientManager() {
          @Override
          public Mono<RpcClient> acquire() {
            return Mono.<RpcClient>just(client).publishOn(Schedulers.single());
          }

          @Override
          public Mono<Void> onClose() {
            return Mono.never();
          }

          @Override
          public void dispose() {}
        };
    transport.responseData = new TestResponse.Builder().build();
    // The fake protocol carries no bytes, so the generated reader has nothing to read.
    transport.readResponse = false;
    TestService.Reactive generated =
        TestService.Reactive.clientBuilder()
            .setProtocolId(ProtocolId.COMPACT)
            .build(new RpcClientBinding(coldConnection, ClientOwnership.BORROWED));

    RequestContextScope.get(
        requestA,
        () -> generated.requestResponse(new TestRequest.Builder().setIntField(1).build()).block());

    assertSame(requestA, handler.contextsSeen.get(0));
    assertEquals("A", transport.headersSent.get(0).get(CONTEXT_HEADER));
  }

  private Mono<ClientResponsePayload<Object>> send() {
    return client.singleRequestSingleResponse(call, RpcOptions.EMPTY);
  }

  @SuppressWarnings("unchecked")
  private static ClientRequestPayload<Object> callPayload() {
    RequestRpcMetadata metadata =
        new RequestRpcMetadata.Builder().setName("ping").setOtherMetadata(new HashMap<>()).build();
    ClientRequestPayload<Object> payload = Mockito.mock(ClientRequestPayload.class);
    Mockito.when(payload.getRequestRpcMetadata()).thenReturn(metadata);
    Mockito.when(payload.getDataWriter()).thenReturn(Writer.EMPTY_WRITER);
    Mockito.when(payload.getResponseReader()).thenReturn((Reader<Object>) protocol -> null);
    return payload;
  }

  private final class RecordingHandler extends ThriftClientEventHandler {
    final List<RequestContext> contextsSeen = Collections.synchronizedList(new ArrayList<>());
    final List<RequestContext> doneContexts = Collections.synchronizedList(new ArrayList<>());
    final List<RequestContext> onErrorContexts = Collections.synchronizedList(new ArrayList<>());
    final Map<String, RequestContext> callbackContexts = new ConcurrentHashMap<>();
    final CountDownLatch firstDone = new CountDownLatch(1);

    @Override
    public Object getContext(String methodName, ClientRequestContext requestContext) {
      RequestContext current = RequestContexts.getCurrentContext();
      contextsSeen.add(current);
      if (current == requestA) {
        requestContext.setRequestHeader(CONTEXT_HEADER, "A");
      } else if (current == requestB) {
        requestContext.setRequestHeader(CONTEXT_HEADER, "B");
      }
      return null;
    }

    @Override
    public void preWrite(Object context, String methodName, Object[] args) {
      record("preWrite");
    }

    @Override
    public void postWrite(Object context, String methodName, Object[] args) {
      record("postWrite");
    }

    @Override
    public void preRead(Object context, String methodName) {
      record("preRead");
    }

    @Override
    public void postRead(Object context, String methodName, Object result) {
      record("postRead");
    }

    @Override
    public void onError(Object context, String methodName, Throwable t) {
      onErrorContexts.add(RequestContexts.getCurrentContext());
    }

    @Override
    public void done(Object context, String methodName) {
      doneContexts.add(RequestContexts.getCurrentContext());
      firstDone.countDown();
    }

    private void record(String callback) {
      RequestContext current = RequestContexts.getCurrentContext();
      if (current != null) {
        callbackContexts.put(callback, current);
      }
    }
  }

  /** Reads the call's metadata when it writes the request, as a real transport does. */
  private static final class RecordingTransport implements RpcClient {
    final List<Map<String, String>> headersSent = Collections.synchronizedList(new ArrayList<>());
    Scheduler completeOn;
    Scheduler writeAndReadOn;
    RuntimeException failWith;
    boolean neverComplete;
    boolean readResponse = true;
    Object responseData;

    @Override
    @SuppressWarnings("unchecked")
    public <T> Mono<ClientResponsePayload<T>> singleRequestSingleResponse(
        ClientRequestPayload<T> payload, RpcOptions options) {
      if (neverComplete) {
        return Mono.never();
      }
      Mono<ClientResponsePayload<T>> response =
          Mono.fromCallable(
              () -> {
                sendAndRead(payload);
                if (failWith != null) {
                  throw failWith;
                }
                ClientResponsePayload<T> result = Mockito.mock(ClientResponsePayload.class);
                Mockito.when(result.getData()).thenReturn((T) responseData);
                return result;
              });
      if (writeAndReadOn != null) {
        response = response.subscribeOn(writeAndReadOn);
      }
      return completeOn == null ? response : response.publishOn(completeOn);
    }

    @Override
    public <T> Mono<Void> singleRequestNoResponse(
        ClientRequestPayload<T> payload, RpcOptions options) {
      return Mono.fromRunnable(() -> headersSent.add(headersOf(payload)));
    }

    private <T> void sendAndRead(ClientRequestPayload<T> payload) throws Exception {
      TProtocol protocol = Mockito.mock(TProtocol.class);
      payload.getDataWriter().write(protocol);
      headersSent.add(headersOf(payload));
      if (readResponse) {
        payload.getResponseReader().read(protocol);
      }
    }

    private static Map<String, String> headersOf(ClientRequestPayload<?> payload) {
      return new HashMap<>(payload.getRequestRpcMetadata().getOtherMetadata());
    }

    @Override
    public Mono<Void> onClose() {
      return Mono.never();
    }

    @Override
    public void dispose() {}
  }
}
