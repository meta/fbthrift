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

import com.facebook.thrift.model.ResponseAndStream;
import com.facebook.thrift.payload.FieldReader;
import java.util.List;
import java.util.function.BiFunction;
import org.apache.thrift.TBaseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * The typed half of one generated bidirectional-stream method: a stream method whose delegate also
 * reads the sink elements.
 *
 * @param <F> the first-response type, or {@code Object} holding {@link
 *     ServerStreams#NO_FIRST_RESPONSE}
 * @param <E> the stream element type
 * @param <U> the sink element type
 */
public final class BidiMethod<F, E, U> {
  final List<FieldReader<?>> sinkReaders;

  private final List<FieldReader<?>> readers;
  private final BiFunction<List<Object>, Flux<U>, Mono<ResponseAndStream<F, E>>> invoke;
  private final ResponseWriterFactory firstResponseWriterFactory;
  private final ResponseWriterFactory responseWriterFactory;
  private final String name;
  private final Class<? extends TBaseException>[] functionExceptions;
  private final Integer[] functionExceptionIds;
  private final Class<? extends TBaseException>[] streamExceptions;
  private final Integer[] streamExceptionIds;

  /**
   * @param sinkReaders field 0 reads the sink element; the other fields read the declared sink
   *     exceptions
   */
  public BidiMethod(
      List<FieldReader<?>> readers,
      List<FieldReader<?>> sinkReaders,
      BiFunction<List<Object>, Flux<U>, Mono<ResponseAndStream<F, E>>> invoke,
      ResponseWriterFactory firstResponseWriterFactory,
      ResponseWriterFactory responseWriterFactory,
      String name,
      Class<? extends TBaseException>[] functionExceptions,
      Integer[] functionExceptionIds,
      Class<? extends TBaseException>[] streamExceptions,
      Integer[] streamExceptionIds) {
    this.readers = readers;
    this.sinkReaders = sinkReaders;
    this.invoke = invoke;
    this.firstResponseWriterFactory = firstResponseWriterFactory;
    this.responseWriterFactory = responseWriterFactory;
    this.name = name;
    this.functionExceptions = functionExceptions;
    this.functionExceptionIds = functionExceptionIds;
    this.streamExceptions = streamExceptions;
    this.streamExceptionIds = streamExceptionIds;
  }

  /** Serves the shared stream phase; the delegate reads the sink elements. */
  StreamMethod<F, E> asStreamMethod(Flux<U> sinkElements) {
    return new StreamMethod<>(
        readers,
        args -> invoke.apply(args, sinkElements),
        firstResponseWriterFactory,
        responseWriterFactory,
        name,
        functionExceptions,
        functionExceptionIds,
        streamExceptions,
        streamExceptionIds);
  }
}
