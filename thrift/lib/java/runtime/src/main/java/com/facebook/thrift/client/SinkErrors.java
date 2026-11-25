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

import com.facebook.thrift.payload.ClientRequestPayload;
import com.facebook.thrift.payload.Reader;
import com.facebook.thrift.payload.ThriftSerializable;
import com.facebook.thrift.payload.Writer;
import com.facebook.thrift.util.RpcPayloadUtil;
import java.util.Map;
import java.util.function.Function;
import org.apache.thrift.ErrorBlame;
import org.apache.thrift.PayloadExceptionMetadataBase;
import org.apache.thrift.PayloadMetadata;
import org.apache.thrift.RequestRpcMetadata;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TType;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Turns a sink publisher failure into the last data frame. A declared exception keeps its field id,
 * anything else goes out as an application exception, so the peer reads the failure instead of
 * seeing the stream die. It is runtime-internal, used by generated code.
 */
public final class SinkErrors {
  private SinkErrors() {}

  /**
   * Wraps the request as its own last data frame. A null field id means the failure matches nothing
   * the IDL declares.
   */
  public static <T> ClientRequestPayload<T> sinkError(
      ClientRequestPayload<T> request, Throwable error, Short declaredFieldId) {
    Writer writer;
    PayloadExceptionMetadataBase metadata;

    if (declaredFieldId != null) {
      writer = declaredWriter(error, declaredFieldId);
      metadata = RpcPayloadUtil.declaredExceptionMetadata(error);
    } else {
      String message = error.getMessage() != null ? error.getMessage() : error.getClass().getName();

      writer = undeclaredWriter(message);
      metadata =
          RpcPayloadUtil.undeclaredExceptionMetadata(
              error.getClass().getName(), message, ErrorBlame.CLIENT);
    }

    return new SinkErrorPayload<>(request, writer, PayloadMetadata.fromExceptionMetadata(metadata));
  }

  /**
   * Ends a failing sink item stream with one error frame, then rethrows the failure. A null lookup
   * means the IDL declares no sink exceptions.
   */
  public static <T> Flux<ClientRequestPayload<T>> withSinkError(
      ClientRequestPayload<T> request,
      Publisher<ClientRequestPayload<T>> items,
      Function<Throwable, Short> declaredFieldId) {
    return Flux.from(items)
        .onErrorResume(
            error -> {
              Short fieldId = declaredFieldId == null ? null : declaredFieldId.apply(error);

              return Mono.just(sinkError(request, error, fieldId)).concatWith(Mono.error(error));
            });
  }

  private static Writer declaredWriter(Throwable error, short fieldId) {
    return protocol -> {
      protocol.writeFieldBegin(new TField("responseField", TType.STRUCT, fieldId));
      ((ThriftSerializable) error).write0(protocol);
      protocol.writeFieldEnd();
    };
  }

  private static Writer undeclaredWriter(String message) {
    return protocol -> {
      protocol.writeFieldBegin(new TField("message", TType.STRING, (short) 1));
      protocol.writeString(message);
      protocol.writeFieldEnd();

      protocol.writeFieldBegin(new TField("type", TType.I32, (short) 2));
      protocol.writeI32(TApplicationException.UNKNOWN);
      protocol.writeFieldEnd();
    };
  }

  @SuppressWarnings("rawtypes")
  private static final class SinkErrorPayload<T> implements ClientRequestPayload<T> {
    private final ClientRequestPayload<T> request;
    private final Writer writer;
    private final PayloadMetadata metadata;

    SinkErrorPayload(ClientRequestPayload<T> request, Writer writer, PayloadMetadata metadata) {
      this.request = request;
      this.writer = writer;
      this.metadata = metadata;
    }

    @Override
    public String getServiceName() {
      return request.getServiceName();
    }

    @Override
    public Writer getDataWriter() {
      return writer;
    }

    @Override
    public Reader<T> getResponseReader() {
      return request.getResponseReader();
    }

    @Override
    public Reader getFirstResponseReader() {
      return request.getFirstResponseReader();
    }

    @Override
    public Map<Short, Reader> getExceptionReaders() {
      return request.getExceptionReaders();
    }

    @Override
    public Map<Short, Reader> getStreamExceptionReaders() {
      return request.getStreamExceptionReaders();
    }

    @Override
    public RequestRpcMetadata getRequestRpcMetadata() {
      return request.getRequestRpcMetadata();
    }

    @Override
    public Map<String, String> getPersistentHeaders() {
      return request.getPersistentHeaders();
    }

    @Override
    public PayloadMetadata getSinkErrorMetadata() {
      return metadata;
    }
  }
}
