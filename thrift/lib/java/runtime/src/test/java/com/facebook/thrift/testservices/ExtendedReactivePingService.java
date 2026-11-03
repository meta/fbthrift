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
import com.facebook.thrift.example.ping.PingRequest;
import com.facebook.thrift.example.ping.PingResponse;
import com.facebook.thrift.model.ResponseAndStream;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * {@link ExtendedPing.Reactive} implementation for stream integration tests. Plain streams are
 * inherited from {@link ReactivePingService}; the child-service stream methods exercise
 * first-response, declared mid-stream, and undeclared mid-stream shapes.
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
}
