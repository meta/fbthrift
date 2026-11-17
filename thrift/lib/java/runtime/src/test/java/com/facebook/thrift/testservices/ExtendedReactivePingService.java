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

package com.facebook.thrift.testservices;

import com.facebook.thrift.example.ping.CustomException;
import com.facebook.thrift.example.ping.ExtendedPing;
import com.facebook.thrift.example.ping.FinalException;
import com.facebook.thrift.example.ping.PingRequest;
import com.facebook.thrift.example.ping.PingResponse;
import com.facebook.thrift.example.ping.SinkException;
import com.facebook.thrift.model.ResponseAndStream;
import com.facebook.thrift.model.StreamResponse;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * {@link ExtendedPing.Reactive} implementation for stream integration tests. Plain streams are
 * inherited from {@link ReactivePingService}; the child-service stream methods exercise
 * first-response, declared mid-stream, and undeclared mid-stream shapes, and the sink and bidi
 * methods exercise the generated channel dispatch.
 */
public class ExtendedReactivePingService extends ReactivePingService
    implements ExtendedPing.Reactive {

  @Override
  public Mono<Void> pingExtended(PingRequest pingRequest) {
    return Mono.empty();
  }

  @Override
  public Mono<ResponseAndStream<PingResponse, PingResponse>> streamWithFirstResponse(
      PingRequest request, int numberOfPings) {
    if (numberOfPings < 0) {
      // Declared initial error: the outer Mono fails before any first response.
      return Mono.error(
          new CustomException.Builder()
              .setMessage("custom exception: " + request.getRequest())
              .build());
    }
    return Mono.just(
        ResponseAndStream.of(
            new PingResponse.Builder().setResponse(request.getRequest() + "_first").build(),
            Flux.range(1, numberOfPings)
                .map(i -> new PingResponse.Builder().setResponse("pong " + i).build())));
  }

  @Override
  public Flux<PingResponse> streamDeclaredExceptionMidStream(
      PingRequest request, int numberOfPings) {
    CustomException declared =
        new CustomException.Builder()
            .setMessage("custom exception: " + request.getRequest())
            .build();
    if (numberOfPings <= 0) {
      // Declared error before any element.
      return Flux.error(declared);
    }
    return Flux.range(1, numberOfPings)
        .map(i -> new PingResponse.Builder().setResponse("pong " + i).build())
        .concatWith(Flux.error(declared));
  }

  @Override
  public Flux<PingResponse> streamUndeclaredExceptionMidStream(
      PingRequest request, int numberOfPings) {
    IllegalStateException undeclared =
        new IllegalStateException("undeclared: " + request.getRequest());
    if (numberOfPings < 0) {
      // Undeclared error before any element.
      return Flux.error(undeclared);
    }
    return Flux.range(1, numberOfPings)
        .map(i -> new PingResponse.Builder().setResponse("pong " + i).build())
        .concatWith(Flux.error(undeclared));
  }

  /**
   * Answers with the sink requests it read. The key selects a failure: "throw" throws the declared
   * function exception, "asyncThrow" returns it as an error, and "final" fails the final response.
   * A declared sink exception from the client becomes the final response.
   */
  @Override
  public Mono<PingResponse> sinkPings(PingRequest key, Publisher<PingRequest> payloads) {
    String name = key.getRequest();
    if ("throw".equals(name)) {
      throw new CustomException.Builder().setMessage("custom exception: throw").build();
    }

    if ("asyncThrow".equals(name)) {
      return Mono.error(
          new CustomException.Builder().setMessage("custom exception: asyncThrow").build());
    }

    return Flux.from(payloads)
        .map(PingRequest::getRequest)
        .collectList()
        .flatMap(
            requests ->
                "final".equals(name)
                    ? Mono.<PingResponse>error(
                        new FinalException.Builder().setMessage("final exception").build())
                    : Mono.just(response(name + ": " + String.join(",", requests))))
        .onErrorResume(
            SinkException.class, e -> Mono.just(response("sink exception: " + e.getMessage())));
  }

  @Override
  public Flux<StreamResponse<PingResponse, PingResponse>> sinkWithFirstResponse(
      PingRequest key, Publisher<PingRequest> payloads) {
    return Flux.concat(
        Mono.just(StreamResponse.fromFirstResponse(response(key.getRequest() + "_first"))),
        Flux.from(payloads).count().map(n -> StreamResponse.fromData(response("read " + n))));
  }

  /**
   * Answers each sink request. The key "streamThrow" ends the stream with the declared stream
   * exception after the first answer; a declared sink exception becomes the last answer.
   */
  @Override
  public Flux<PingResponse> bidiPings(PingRequest key, Publisher<PingRequest> payloads) {
    Flux<PingResponse> answers =
        Flux.from(payloads)
            .map(request -> response(key.getRequest() + ": " + request.getRequest()))
            .onErrorResume(
                SinkException.class, e -> Flux.just(response("sink exception: " + e.getMessage())));

    if ("streamThrow".equals(key.getRequest())) {
      return answers
          .take(1)
          .concatWith(
              Flux.error(
                  new CustomException.Builder().setMessage("custom exception: stream").build()));
    }

    return answers;
  }

  /** Sends the start as the first response, then the running sum of the sink elements. */
  @Override
  public Mono<ResponseAndStream<Integer, Long>> bidiSums(int start, Publisher<Integer> payloads) {
    Flux<Long> sums =
        Flux.from(payloads).scan((long) start, (sum, element) -> sum + element).skip(1);

    return Mono.just(ResponseAndStream.of(start, sums));
  }

  private static PingResponse response(String text) {
    return new PingResponse.Builder().setResponse(text).build();
  }
}
