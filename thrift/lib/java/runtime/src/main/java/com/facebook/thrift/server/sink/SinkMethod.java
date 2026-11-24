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

package com.facebook.thrift.server.sink;

import com.facebook.thrift.model.ResponseAndStream;
import com.facebook.thrift.payload.FieldReader;
import com.facebook.thrift.server.ResponseWriterFactory;
import com.facebook.thrift.server.stream.ServerStreams;
import com.facebook.thrift.server.stream.StreamMethod;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.thrift.TBaseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * The typed half of one generated sink method. Its delegate reads the sink elements and returns one
 * final response, so the method declares final-response exceptions where a stream declares element
 * exceptions.
 *
 * @param <F> the first-response type, or {@code Object} holding {@link
 *     ServerStreams#NO_FIRST_RESPONSE}
 * @param <R> the final-response type
 * @param <U> the sink element type
 */
public final class SinkMethod<F, R, U> {
  final List<FieldReader<?>> sinkReaders;

  private final List<FieldReader<?>> readers;
  private final BiFunction<List<Object>, Flux<U>, Mono<ResponseAndStream<F, R>>> invoke;
  private final ResponseWriterFactory firstResponseWriterFactory;
  private final ResponseWriterFactory finalResponseWriterFactory;
  private final String name;
  private final Class<? extends TBaseException>[] functionExceptions;
  private final Integer[] functionExceptionIds;
  private final Class<? extends TBaseException>[] finalResponseExceptions;
  private final Integer[] finalResponseExceptionIds;

  /**
   * @param sinkReaders field 0 reads the sink element; the other fields read the declared sink
   *     exceptions
   */
  public SinkMethod(
      List<FieldReader<?>> readers,
      List<FieldReader<?>> sinkReaders,
      BiFunction<List<Object>, Flux<U>, Mono<ResponseAndStream<F, R>>> invoke,
      ResponseWriterFactory firstResponseWriterFactory,
      ResponseWriterFactory finalResponseWriterFactory,
      String name,
      Class<? extends TBaseException>[] functionExceptions,
      Integer[] functionExceptionIds,
      Class<? extends TBaseException>[] finalResponseExceptions,
      Integer[] finalResponseExceptionIds) {
    this.readers = readers;
    this.sinkReaders = sinkReaders;
    this.invoke = invoke;
    this.firstResponseWriterFactory = firstResponseWriterFactory;
    this.finalResponseWriterFactory = finalResponseWriterFactory;
    this.name = name;
    this.functionExceptions = functionExceptions;
    this.functionExceptionIds = functionExceptionIds;
    this.finalResponseExceptions = finalResponseExceptions;
    this.finalResponseExceptionIds = finalResponseExceptionIds;
  }

  /** Serves the shared stream phase with the final response as its one element. */
  StreamMethod<F, R> asStreamMethod(
      Flux<U> sinkElements, Function<Flux<R>, Flux<R>> onFinalResponse) {
    return new StreamMethod<>(
        readers,
        args ->
            invoke.apply(args, sinkElements).map(responses -> responses.mapStream(onFinalResponse)),
        firstResponseWriterFactory,
        finalResponseWriterFactory,
        name,
        functionExceptions,
        functionExceptionIds,
        finalResponseExceptions,
        finalResponseExceptionIds);
  }
}
