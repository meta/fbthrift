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

package com.facebook.thrift.server.generated;

import com.facebook.swift.service.ContextChain;
import com.facebook.thrift.payload.ServerRequestPayload;
import com.facebook.thrift.payload.ServerResponsePayload;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

/** Serves generated bidirectional-stream methods: streams whose delegates also read a sink. */
public final class ServerBidiStreams {
  private ServerBidiStreams() {}

  /**
   * Serves a bidi call: the delegate reads the sink elements and returns the stream elements. The
   * transport starts the sink after the first response and ends it with the call, so the two halves
   * end on their own. The request hooks end with the stream, not with the sink.
   */
  public static <F, E, U> Flux<ServerResponsePayload> handleBidi(
      ServerRequestPayload requestPayload,
      ContextChain chain,
      BidiMethod<F, E, U> method,
      Publisher<ServerRequestPayload> sinkPayloads) {
    Flux<U> sinkElements =
        Flux.from(sinkPayloads)
            .map(payload -> ServerSinks.<U>readSinkElement(payload, method.sinkReaders));

    return ServerStreams.handleStream(requestPayload, chain, method.asStreamMethod(sinkElements));
  }
}
