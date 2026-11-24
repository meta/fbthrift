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

package com.meta.thrift.conformance.rpc.server;

import com.facebook.thrift.client.ResponseWrapper;
import com.facebook.thrift.client.RpcOptions;
import com.facebook.thrift.model.ResponseAndStream;
import com.facebook.thrift.model.StreamResponse;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.conformance.BidiBasicServerTestResult;
import org.apache.thrift.conformance.BidiInitialResponseServerTestResult;
import org.apache.thrift.conformance.BidiMethodDeclaredExceptionServerTestResult;
import org.apache.thrift.conformance.BidiMethodUndeclaredExceptionServerTestResult;
import org.apache.thrift.conformance.BidiSinkDeclaredExceptionServerTestResult;
import org.apache.thrift.conformance.BidiSinkUndeclaredExceptionServerTestResult;
import org.apache.thrift.conformance.BidiStreamDeclaredExceptionServerTestResult;
import org.apache.thrift.conformance.BidiStreamUndeclaredExceptionServerTestResult;
import org.apache.thrift.conformance.ClientTestResult;
import org.apache.thrift.conformance.RPCConformanceService;
import org.apache.thrift.conformance.Request;
import org.apache.thrift.conformance.RequestResponseBasicServerTestResult;
import org.apache.thrift.conformance.RequestResponseDeclaredExceptionServerTestResult;
import org.apache.thrift.conformance.RequestResponseNoArgVoidResponseServerTestResult;
import org.apache.thrift.conformance.RequestResponseUndeclaredExceptionServerTestResult;
import org.apache.thrift.conformance.Response;
import org.apache.thrift.conformance.RpcTestCase;
import org.apache.thrift.conformance.ServerInstruction;
import org.apache.thrift.conformance.ServerTestResult;
import org.apache.thrift.conformance.SinkBasicServerTestResult;
import org.apache.thrift.conformance.SinkChunkTimeoutServerTestResult;
import org.apache.thrift.conformance.SinkDeclaredExceptionServerTestResult;
import org.apache.thrift.conformance.SinkInitialDeclaredExceptionServerTestResult;
import org.apache.thrift.conformance.SinkInitialResponseServerTestResult;
import org.apache.thrift.conformance.SinkServerDeclaredExceptionServerTestResult;
import org.apache.thrift.conformance.SinkUndeclaredExceptionServerTestResult;
import org.apache.thrift.conformance.StreamBasicServerTestResult;
import org.apache.thrift.conformance.StreamDeclaredExceptionServerTestResult;
import org.apache.thrift.conformance.StreamInitialDeclaredExceptionServerTestResult;
import org.apache.thrift.conformance.StreamInitialResponseServerTestResult;
import org.apache.thrift.conformance.StreamInitialUndeclaredExceptionServerTestResult;
import org.apache.thrift.conformance.StreamUndeclaredExceptionServerTestResult;
import org.apache.thrift.conformance.UserException;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

public class RpcServerReactiveHandler implements RPCConformanceService.Reactive {

  private ServerInstruction instruction;
  private Sinks.One<ServerTestResult> testResult = Sinks.one();

  @Override
  public Mono<Void> sendTestCase(RpcTestCase rpcTestCase) {
    instruction = rpcTestCase.getServerInstruction();
    testResult = Sinks.one();
    return Mono.empty();
  }

  /**
   * Waits up to one second for the case to record its result. The server reads its sink off the
   * event loop, so a client that ends its sink with an error can ask before that error is read.
   */
  @Override
  public Mono<ServerTestResult> getTestResult() {
    return testResult.asMono().timeout(Duration.ofSeconds(1));
  }

  private void record(ServerTestResult result) {
    testResult.tryEmitValue(result);
  }

  @Override
  public Mono<RpcTestCase> getTestCase() {
    throw new RuntimeException("Not implemented, needed for client testing only");
  }

  @Override
  public Mono<Void> sendTestResult(ClientTestResult clientTestResult) {
    throw new RuntimeException("Not implemented, needed for client testing only");
  }

  @Override
  public Mono<Response> requestResponseBasic(Request request) {
    record(
        ServerTestResult.fromRequestResponseBasic(
            new RequestResponseBasicServerTestResult.Builder().setRequest(request).build()));
    return Mono.just(instruction.getRequestResponseBasic().getResponse());
  }

  @Override
  public Mono<Response> requestResponseBasic(Request request, RpcOptions rpcOptions) {
    record(
        ServerTestResult.fromRequestResponseBasic(
            new RequestResponseBasicServerTestResult.Builder().setRequest(request).build()));
    return Mono.just(instruction.getRequestResponseBasic().getResponse());
  }

  @Override
  public Mono<Void> requestResponseDeclaredException(Request request) {
    record(
        ServerTestResult.fromRequestResponseDeclaredException(
            new RequestResponseDeclaredExceptionServerTestResult.Builder()
                .setRequest(request)
                .build()));
    return Mono.error(instruction.getRequestResponseDeclaredException().getUserException());
  }

  @Override
  public Mono<Void> requestResponseUndeclaredException(Request request) {
    record(
        ServerTestResult.fromRequestResponseUndeclaredException(
            new RequestResponseUndeclaredExceptionServerTestResult.Builder()
                .setRequest(request)
                .build()));
    return Mono.error(
        new RuntimeException(
            instruction.getRequestResponseUndeclaredException().getExceptionMessage()));
  }

  @Override
  public Mono<Void> requestResponseNoArgVoidResponse() {
    record(
        ServerTestResult.fromRequestResponseNoArgVoidResponse(
            new RequestResponseNoArgVoidResponseServerTestResult.Builder().build()));
    return Mono.empty();
  }

  @Override
  public Mono<Response> requestResponseTimeout(Request request) {
    return Mono.empty();
  }

  @Override
  public Mono<Void> basicInteractionFactoryFunction(int i) {
    return null;
  }

  @Override
  public Flux<Response> streamBasic(Request request) {
    record(
        ServerTestResult.fromStreamBasic(
            new StreamBasicServerTestResult.Builder().setRequest(request).build()));
    return Flux.fromIterable(instruction.getStreamBasic().getStreamPayloads());
  }

  public Flux<Response> streamBasic(Request req, RpcOptions rpcOptions) {
    throw new UnsupportedOperationException();
  }

  public Flux<ResponseWrapper<Response>> streamBasicWrapper(Request req, RpcOptions rpcOptions) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Flux<Response> streamChunkTimeout(Request request) {
    return null;
  }

  @Override
  public Mono<ResponseAndStream<Response, Response>> streamInitialResponse(Request request) {
    record(
        ServerTestResult.fromStreamInitialResponse(
            new StreamInitialResponseServerTestResult.Builder().setRequest(request).build()));
    return Mono.just(
        ResponseAndStream.of(
            instruction.getStreamInitialResponse().getInitialResponse(),
            Flux.fromIterable(instruction.getStreamInitialResponse().getStreamPayloads())));
  }

  @Override
  public Flux<Response> streamCreditTimeout(Request request) {
    return null;
  }

  @Override
  public Flux<Response> streamDeclaredException(Request request) {
    record(
        ServerTestResult.fromStreamDeclaredException(
            new StreamDeclaredExceptionServerTestResult.Builder().setRequest(request).build()));
    return Flux.error(instruction.getStreamDeclaredException().getUserException());
  }

  @Override
  public Flux<Response> streamUndeclaredException(Request request) {
    record(
        ServerTestResult.fromStreamUndeclaredException(
            new StreamUndeclaredExceptionServerTestResult.Builder().setRequest(request).build()));
    return Flux.error(
        new RuntimeException(instruction.getStreamUndeclaredException().getExceptionMessage()));
  }

  @Override
  public Flux<Response> streamInitialDeclaredException(Request request) {
    record(
        ServerTestResult.fromStreamInitialDeclaredException(
            new StreamInitialDeclaredExceptionServerTestResult.Builder()
                .setRequest(request)
                .build()));
    UserException ex = instruction.getStreamInitialDeclaredException().getUserException();
    throw new UserException.Builder().setMsg(ex.getMsg()).build();
  }

  @Override
  public Flux<Response> streamInitialUndeclaredException(Request request) {
    record(
        ServerTestResult.fromStreamInitialUndeclaredException(
            new StreamInitialUndeclaredExceptionServerTestResult.Builder()
                .setRequest(request)
                .build()));
    throw new RuntimeException(
        instruction.getStreamInitialUndeclaredException().getExceptionMessage());
  }

  @Override
  public Flux<Response> streamInitialTimeout(Request request) {
    throw new RuntimeException("Not implemented, needed for client testing only");
  }

  @Override
  public Mono<Response> sinkBasic(Request request, Publisher<Request> publisher) {
    return Flux.from(publisher)
        .collectList()
        .map(
            received -> {
              record(
                  ServerTestResult.fromSinkBasic(
                      new SinkBasicServerTestResult.Builder()
                          .setRequest(request)
                          .setSinkPayloads(received)
                          .build()));
              return instruction.getSinkBasic().getFinalResponse();
            });
  }

  @Override
  public Mono<Response> sinkChunkTimeout(Request request, Publisher<Request> publisher) {
    // The Java server has no sink chunk timer, so this case reads like a basic sink and stays
    // nonconforming.
    return Flux.from(publisher)
        .collectList()
        .map(
            received -> {
              record(
                  ServerTestResult.fromSinkChunkTimeout(
                      new SinkChunkTimeoutServerTestResult.Builder()
                          .setRequest(request)
                          .setSinkPayloads(received)
                          .build()));
              return instruction.getSinkChunkTimeout().getFinalResponse();
            });
  }

  @Override
  public Flux<StreamResponse<Response, Response>> sinkInitialResponse(
      Request request, Publisher<Request> publisher) {
    Mono<StreamResponse<Response, Response>> finalResponse =
        Flux.from(publisher)
            .collectList()
            .map(
                received -> {
                  record(
                      ServerTestResult.fromSinkInitialResponse(
                          new SinkInitialResponseServerTestResult.Builder()
                              .setRequest(request)
                              .setSinkPayloads(received)
                              .build()));
                  return StreamResponse.fromData(
                      instruction.getSinkInitialResponse().getFinalResponse());
                });

    return Flux.concat(
        Mono.just(
            StreamResponse.fromFirstResponse(
                instruction.getSinkInitialResponse().getInitialResponse())),
        finalResponse);
  }

  @Override
  public Mono<Response> sinkDeclaredException(Request request, Publisher<Request> publisher) {
    // The case sends the exception as the first sink element.
    return Flux.from(publisher)
        .next()
        .then(Mono.<Response>error(new IllegalStateException("Publisher didn't throw")))
        .onErrorResume(
            UserException.class,
            e -> {
              record(
                  ServerTestResult.fromSinkDeclaredException(
                      new SinkDeclaredExceptionServerTestResult.Builder()
                          .setRequest(request)
                          .setUserException(e)
                          .build()));
              return Mono.error(e);
            });
  }

  @Override
  public Mono<Response> sinkInitialDeclaredException(
      Request request, Publisher<Request> publisher) {
    record(
        ServerTestResult.fromSinkInitialDeclaredException(
            new SinkInitialDeclaredExceptionServerTestResult.Builder()
                .setRequest(request)
                .build()));

    throw instruction.getSinkInitialDeclaredException().getUserException();
  }

  @Override
  public Mono<Response> sinkUndeclaredException(Request request, Publisher<Request> publisher) {
    // The case sends the exception as the first sink element.
    return Flux.from(publisher)
        .next()
        .then(Mono.<Response>error(new IllegalStateException("Publisher didn't throw")))
        .onErrorResume(
            e -> {
              record(
                  ServerTestResult.fromSinkUndeclaredException(
                      new SinkUndeclaredExceptionServerTestResult.Builder()
                          .setRequest(request)
                          .setExceptionMessage(
                              e instanceof TApplicationException ? e.getMessage() : e.toString())
                          .build()));
              return Mono.error(e);
            });
  }

  @Override
  public Mono<Response> sinkServerDeclaredException(Request request, Publisher<Request> publisher) {
    return Flux.from(publisher)
        .then(
            Mono.defer(
                () -> {
                  record(
                      ServerTestResult.fromSinkServerDeclaredException(
                          new SinkServerDeclaredExceptionServerTestResult.Builder()
                              .setRequest(request)
                              .build()));
                  return Mono.error(
                      instruction.getSinkServerDeclaredException().getUserException());
                }));
  }

  @Override
  public Flux<Response> bidiBasic(Request request, Publisher<Request> publisher) {
    return streamThenSink(
        instruction.getBidiBasic().getStreamPayloads(),
        publisher,
        received -> {
          record(
              ServerTestResult.fromBidiBasic(
                  new BidiBasicServerTestResult.Builder()
                      .setRequest(request)
                      .setSinkPayloads(received)
                      .build()));
          return Flux.empty();
        });
  }

  @Override
  public Mono<ResponseAndStream<Response, Response>> bidiInitialResponse(
      Request request, Publisher<Request> publisher) {
    Flux<Response> stream =
        streamThenSink(
            instruction.getBidiInitialResponse().getStreamPayloads(),
            publisher,
            received -> {
              record(
                  ServerTestResult.fromBidiInitialResponse(
                      new BidiInitialResponseServerTestResult.Builder()
                          .setRequest(request)
                          .setSinkPayloads(received)
                          .build()));
              return Flux.empty();
            });

    return Mono.just(
        ResponseAndStream.of(instruction.getBidiInitialResponse().getInitialResponse(), stream));
  }

  @Override
  public Flux<Response> bidiStreamDeclaredException(Request request, Publisher<Request> publisher) {
    return streamThenSink(
        instruction.getBidiStreamDeclaredException().getStreamPayloads(),
        publisher,
        received -> {
          record(
              ServerTestResult.fromBidiStreamDeclaredException(
                  new BidiStreamDeclaredExceptionServerTestResult.Builder()
                      .setRequest(request)
                      .setSinkPayloads(received)
                      .build()));
          return Flux.error(instruction.getBidiStreamDeclaredException().getUserException());
        });
  }

  @Override
  public Flux<Response> bidiStreamUndeclaredException(
      Request request, Publisher<Request> publisher) {
    return streamThenSink(
        instruction.getBidiStreamUndeclaredException().getStreamPayloads(),
        publisher,
        received -> {
          record(
              ServerTestResult.fromBidiStreamUndeclaredException(
                  new BidiStreamUndeclaredExceptionServerTestResult.Builder()
                      .setRequest(request)
                      .setSinkPayloads(received)
                      .build()));
          return Flux.error(
              new RuntimeException(
                  instruction.getBidiStreamUndeclaredException().getExceptionMessage()));
        });
  }

  @Override
  public Flux<Response> bidiSinkDeclaredException(Request request, Publisher<Request> publisher) {
    List<Request> received = new CopyOnWriteArrayList<>();
    BidiSinkDeclaredExceptionServerTestResult.Builder result =
        new BidiSinkDeclaredExceptionServerTestResult.Builder()
            .setRequest(request)
            .setSinkPayloads(received);

    Flux<Response> readSink =
        Flux.from(publisher)
            .doOnNext(received::add)
            .onErrorResume(
                UserException.class,
                e -> {
                  result.setUserException(e);
                  return Mono.empty();
                })
            .thenMany(
                Flux.defer(
                    () -> {
                      record(ServerTestResult.fromBidiSinkDeclaredException(result.build()));
                      return Flux.empty();
                    }));

    return Flux.concat(
        Flux.fromIterable(instruction.getBidiSinkDeclaredException().getStreamPayloads()),
        readSink);
  }

  @Override
  public Flux<Response> bidiSinkUndeclaredException(Request request, Publisher<Request> publisher) {
    List<Request> received = new CopyOnWriteArrayList<>();

    Flux<Response> readSink =
        Flux.from(publisher)
            .doOnNext(received::add)
            .thenMany(Flux.<Response>empty())
            .onErrorResume(
                TApplicationException.class,
                e -> {
                  // The bidi client sends an undeclared sink exception as "<type>: <message>",
                  // and the case expects the message alone.
                  String message = e.getMessage();
                  int prefix = message == null ? -1 : message.indexOf(": ");
                  record(
                      ServerTestResult.fromBidiSinkUndeclaredException(
                          new BidiSinkUndeclaredExceptionServerTestResult.Builder()
                              .setRequest(request)
                              .setSinkPayloads(received)
                              .setExceptionMessage(
                                  prefix < 0 ? message : message.substring(prefix + 2))
                              .build()));
                  return Flux.empty();
                });

    return Flux.concat(
        Flux.fromIterable(instruction.getBidiSinkUndeclaredException().getStreamPayloads()),
        readSink);
  }

  @Override
  public Flux<Response> bidiMethodDeclaredException(Request request, Publisher<Request> publisher) {
    record(
        ServerTestResult.fromBidiMethodDeclaredException(
            new BidiMethodDeclaredExceptionServerTestResult.Builder().setRequest(request).build()));

    throw instruction.getBidiMethodDeclaredException().getUserException();
  }

  @Override
  public Flux<Response> bidiMethodUndeclaredException(
      Request request, Publisher<Request> publisher) {
    record(
        ServerTestResult.fromBidiMethodUndeclaredException(
            new BidiMethodUndeclaredExceptionServerTestResult.Builder()
                .setRequest(request)
                .build()));

    throw new RuntimeException(
        instruction.getBidiMethodUndeclaredException().getExceptionMessage());
  }

  /**
   * Sends the stream payloads of the case, then reads the whole sink. {@code end} records the
   * result from the sink payloads and returns the end of the stream.
   */
  private static Flux<Response> streamThenSink(
      List<Response> streamPayloads,
      Publisher<Request> publisher,
      Function<List<Request>, Flux<Response>> end) {
    return Flux.concat(
        Flux.fromIterable(streamPayloads), Flux.from(publisher).collectList().flatMapMany(end));
  }

  @Override
  public BasicInteraction createBasicInteraction() {
    return null;
  }

  @Override
  public void dispose() {}
}
