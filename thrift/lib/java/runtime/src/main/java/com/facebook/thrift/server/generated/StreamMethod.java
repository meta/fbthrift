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
import java.util.Objects;
import java.util.function.Function;
import org.apache.thrift.TBaseException;
import reactor.core.publisher.Mono;

/**
 * The typed half of one generated streaming method: everything the code generator knows (how to
 * read the args, how to invoke the delegate, how to write the two payload shapes, which exceptions
 * are declared where) packaged as lambdas and tables. Built once per method in the generated {@code
 * RpcServerHandler} and passed to {@link ServerStreams#handleStream} per request.
 *
 * @param <F> the first-response type (the declared return type, or {@code Object} holding {@link
 *     ServerStreams#NO_FIRST_RESPONSE} for plain streams)
 * @param <E> the stream element type
 */
public final class StreamMethod<F, E> {
  public final List<FieldReader<?>> readers;
  public final Function<List<Object>, Mono<ResponseAndStream<F, E>>> invoke;
  public final ResponseWriterFactory firstResponseWriterFactory;
  public final ResponseWriterFactory responseWriterFactory;
  public final String name;
  public final Class<? extends TBaseException>[] functionExceptions;
  public final Integer[] functionExceptionIds;
  public final Class<? extends TBaseException>[] streamExceptions;
  public final Integer[] streamExceptionIds;

  private static final Class[] NO_KNOWN_EXCEPTIONS = new Class[0];
  private static final Integer[] NO_KNOWN_EXCEPTION_IDS = new Integer[0];

  @SuppressWarnings("unchecked")
  public StreamMethod(
      List<FieldReader<?>> readers,
      Function<List<Object>, Mono<ResponseAndStream<F, E>>> invoke,
      ResponseWriterFactory firstResponseWriterFactory,
      ResponseWriterFactory responseWriterFactory,
      String name,
      Class<? extends TBaseException>[] functionExceptions,
      Integer[] functionExceptionIds,
      Class<? extends TBaseException>[] streamExceptions,
      Integer[] streamExceptionIds) {
    this.readers = Objects.requireNonNull(readers, "readers");
    this.invoke = Objects.requireNonNull(invoke, "invoke");
    this.firstResponseWriterFactory =
        Objects.requireNonNull(firstResponseWriterFactory, "firstResponseWriterFactory");
    this.responseWriterFactory =
        Objects.requireNonNull(responseWriterFactory, "responseWriterFactory");
    this.name = Objects.requireNonNull(name, "name");
    this.functionExceptions = functionExceptions == null ? NO_KNOWN_EXCEPTIONS : functionExceptions;
    this.functionExceptionIds =
        functionExceptionIds == null ? NO_KNOWN_EXCEPTION_IDS : functionExceptionIds;
    this.streamExceptions = streamExceptions == null ? NO_KNOWN_EXCEPTIONS : streamExceptions;
    this.streamExceptionIds =
        streamExceptionIds == null ? NO_KNOWN_EXCEPTION_IDS : streamExceptionIds;
  }
}
