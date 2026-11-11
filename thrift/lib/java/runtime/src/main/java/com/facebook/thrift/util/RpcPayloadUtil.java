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

package com.facebook.thrift.util;

import com.facebook.swift.service.ContextChain;
import com.facebook.thrift.payload.ServerRequestPayload;
import com.facebook.thrift.payload.ServerResponsePayload;
import com.facebook.thrift.payload.ThriftSerializable;
import com.facebook.thrift.payload.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.thrift.ErrorBlame;
import org.apache.thrift.ErrorClassification;
import org.apache.thrift.ErrorKind;
import org.apache.thrift.ErrorSafety;
import org.apache.thrift.ExceptionBlame;
import org.apache.thrift.ExceptionKind;
import org.apache.thrift.ExceptionSafety;
import org.apache.thrift.PayloadAppUnknownExceptionMetdata;
import org.apache.thrift.PayloadDeclaredExceptionMetadata;
import org.apache.thrift.PayloadExceptionMetadata;
import org.apache.thrift.PayloadExceptionMetadataBase;
import org.apache.thrift.PayloadMetadata;
import org.apache.thrift.PayloadResponseMetadata;
import org.apache.thrift.RequestRpcMetadata;
import org.apache.thrift.ResponseRpcMetadata;
import org.apache.thrift.StreamPayloadMetadata;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TBaseException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import reactor.core.Exceptions;

public final class RpcPayloadUtil {
  public static final TStruct TSTRUCT = new TStruct();
  private static final String RESPONSE_FIELD_NAME = "responseField";
  private static final short RESPONSE_FIELD_ID = 0;
  public static final TField VOID_FIELD =
      new TField(RESPONSE_FIELD_NAME, TType.VOID, RESPONSE_FIELD_ID);
  public static final TField BOOL_FIELD =
      new TField(RESPONSE_FIELD_NAME, TType.BOOL, RESPONSE_FIELD_ID);
  public static final TField BYTE_FIELD =
      new TField(RESPONSE_FIELD_NAME, TType.BYTE, RESPONSE_FIELD_ID);
  public static final TField DOUBLE_FIELD =
      new TField(RESPONSE_FIELD_NAME, TType.DOUBLE, RESPONSE_FIELD_ID);
  public static final TField I16_FIELD =
      new TField(RESPONSE_FIELD_NAME, TType.I16, RESPONSE_FIELD_ID);
  public static final TField I32_FIELD =
      new TField(RESPONSE_FIELD_NAME, TType.I32, RESPONSE_FIELD_ID);
  public static final TField I64_FIELD =
      new TField(RESPONSE_FIELD_NAME, TType.I64, RESPONSE_FIELD_ID);
  public static final TField STRING_FIELD =
      new TField(RESPONSE_FIELD_NAME, TType.STRING, RESPONSE_FIELD_ID);
  public static final TField STRUCT_FIELD =
      new TField(RESPONSE_FIELD_NAME, TType.STRUCT, RESPONSE_FIELD_ID);
  public static final TField MAP_FIELD =
      new TField(RESPONSE_FIELD_NAME, TType.MAP, RESPONSE_FIELD_ID);
  public static final TField SET_FIELD =
      new TField(RESPONSE_FIELD_NAME, TType.SET, RESPONSE_FIELD_ID);
  public static final TField LIST_FIELD =
      new TField(RESPONSE_FIELD_NAME, TType.LIST, RESPONSE_FIELD_ID);
  public static final TField ENUM_FIELD =
      new TField(RESPONSE_FIELD_NAME, TType.ENUM, RESPONSE_FIELD_ID);
  public static final TField FLOAT_FIELD =
      new TField(RESPONSE_FIELD_NAME, TType.FLOAT, RESPONSE_FIELD_ID);

  private RpcPayloadUtil() {}

  /**
   * Maps an unhandled {@link Throwable} to a {@link TApplicationException} with code {@link
   * TApplicationException#INTERNAL_ERROR} and encodes it as a Thrift error response. Fires the
   * chain's exception write hooks when {@code chain} is non-null. Mirrors the C++ server's {@code
   * TApplicationException::INTERNAL_ERROR} wrapping pattern.
   *
   * @param description short description of what failed, e.g. {@code "during preprocess"} or {@code
   *     "processing ping"}; included in the encoded message.
   */
  public static ServerResponsePayload internalErrorResponse(
      final Throwable t,
      final String description,
      final RequestRpcMetadata requestRpcMetadata,
      final ContextChain chain) {
    String message =
        String.format(
            "Internal error %s: %s",
            description, t.getMessage() == null ? "<null>" : t.getMessage());
    TApplicationException tae =
        new TApplicationException(TApplicationException.INTERNAL_ERROR, message);
    tae.initCause(t);
    return fromTApplicationException(tae, requestRpcMetadata, chain);
  }

  public static ServerResponsePayload fromTApplicationException(
      final TApplicationException applicationException,
      final RequestRpcMetadata requestRpcMetadata,
      final ContextChain chain) {
    Writer bodyWriter =
        protocol -> {
          try {
            if (chain != null) {
              chain.preWriteException(applicationException);
              chain.undeclaredUserException(applicationException);
              // Application exceptions are sent to client, and the connection can be reused
              applicationException.write(protocol);
              chain.postWriteException(applicationException);
            } else {
              applicationException.write(protocol);
            }
          } catch (Exception e) {
            throw new TException(e);
          }
        };
    Map<String, String> inbound =
        requestRpcMetadata == null ? null : requestRpcMetadata.getOtherMetadata();
    return ServerResponsePayload.createWithTApplicationException(
        bodyWriter,
        new ResponseRpcMetadata.Builder()
            .setPayloadMetadata(
                PayloadMetadata.fromExceptionMetadata(
                    new PayloadExceptionMetadataBase.Builder()
                        .setWhatUtf8(applicationException.getMessage())
                        .setMetadata(serverBlamedAppUnknownException())
                        .build()))
            .setOtherMetadata(inbound)
            .build(),
        null,
        false,
        applicationException);
  }

  public static ServerResponsePayload createServerResponsePayload(
      final ServerRequestPayload payload, final Writer writer) {
    return createServerResponsePayload(payload, writer, (String) null);
  }

  // Maps the IDL classification carried by a declared exception to the wire
  // ErrorClassification, mirroring the C++ fromExceptionBlame/Kind/Safety
  // converters. Non-Thrift throwables have no IDL classification.
  public static ErrorClassification errorClassificationFor(Throwable t) {
    ExceptionBlame blame = ExceptionBlame.UNSPECIFIED;
    ExceptionKind kind = ExceptionKind.UNSPECIFIED;
    ExceptionSafety safety = ExceptionSafety.UNSPECIFIED;
    if (t instanceof TBaseException) {
      TBaseException e = (TBaseException) t;
      blame = e.getExceptionBlame();
      kind = e.getExceptionKind();
      safety = e.getExceptionSafety();
    }
    return new ErrorClassification.Builder()
        .setBlame(toWireBlame(blame))
        .setKind(toWireKind(kind))
        .setSafety(toWireSafety(safety))
        .build();
  }

  private static ErrorBlame toWireBlame(ExceptionBlame blame) {
    switch (blame) {
      case SERVER:
        return ErrorBlame.SERVER;
      case CLIENT:
        return ErrorBlame.CLIENT;
      default:
        return ErrorBlame.UNSPECIFIED;
    }
  }

  private static ErrorKind toWireKind(ExceptionKind kind) {
    switch (kind) {
      case TRANSIENT:
        return ErrorKind.TRANSIENT;
      case STATEFUL:
        return ErrorKind.STATEFUL;
      case PERMANENT:
        return ErrorKind.PERMANENT;
      default:
        return ErrorKind.UNSPECIFIED;
    }
  }

  private static ErrorSafety toWireSafety(ExceptionSafety safety) {
    switch (safety) {
      case SAFE:
        return ErrorSafety.SAFE;
      default:
        return ErrorSafety.UNSPECIFIED;
    }
  }

  /**
   * Converts request {@code otherMetadata} (string values) to the byte-valued map that {@link
   * StreamPayloadMetadata} carries, encoding each value as UTF-8.
   *
   * @return an empty immutable map when {@code otherMetadata} is null or empty
   */
  public static Map<String, byte[]> convertOtherData(Map<String, String> otherMetadata) {
    if (otherMetadata == null || otherMetadata.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, byte[]> converted = new HashMap<>(otherMetadata.size());
    for (Map.Entry<String, String> entry : otherMetadata.entrySet()) {
      converted.put(entry.getKey(), entry.getValue().getBytes(StandardCharsets.UTF_8));
    }
    return converted;
  }

  /**
   * Exception metadata for a declared (IDL {@code throws}) exception: the exception class name, its
   * message, and the error classification from {@link #errorClassificationFor(Throwable)}.
   */
  public static PayloadExceptionMetadataBase declaredExceptionMetadata(Throwable t) {
    return new PayloadExceptionMetadataBase.Builder()
        .setNameUtf8(t.getClass().getName())
        .setWhatUtf8(t.getMessage())
        .setMetadata(
            PayloadExceptionMetadata.fromDeclaredException(
                new PayloadDeclaredExceptionMetadata.Builder()
                    .setErrorClassification(errorClassificationFor(t))
                    .build()))
        .build();
  }

  /**
   * Exception metadata for an undeclared exception: {@code appUnknown} with server blame, as the
   * C++ server sends for an exception that the IDL does not declare.
   *
   * @param name the class name of the original exception
   * @param what the message of the original exception; can be null
   */
  public static PayloadExceptionMetadataBase undeclaredExceptionMetadata(String name, String what) {
    return new PayloadExceptionMetadataBase.Builder()
        .setNameUtf8(name)
        .setWhatUtf8(what)
        .setMetadata(serverBlamedAppUnknownException())
        .build();
  }

  /**
   * First-response (RPC-level) metadata for a declared exception. It passes the request {@code
   * otherMetadata} through unchanged.
   */
  public static ResponseRpcMetadata declaredExceptionResponseMetadata(
      ServerRequestPayload requestPayload, Throwable t) {
    return exceptionResponseMetadata(requestPayload, declaredExceptionMetadata(t));
  }

  /**
   * First-response (RPC-level) metadata for an undeclared exception. It passes the request {@code
   * otherMetadata} through unchanged.
   */
  public static ResponseRpcMetadata undeclaredExceptionResponseMetadata(
      ServerRequestPayload requestPayload, String name, String what) {
    return exceptionResponseMetadata(requestPayload, undeclaredExceptionMetadata(name, what));
  }

  /**
   * Stream-scope metadata for a declared exception that ends a stream, sink, or bidi download. The
   * request {@code otherMetadata} goes through {@link #convertOtherData(Map)}.
   */
  public static StreamPayloadMetadata declaredExceptionStreamMetadata(
      ServerRequestPayload requestPayload, Throwable t) {
    return streamMetadata(
        requestPayload, PayloadMetadata.fromExceptionMetadata(declaredExceptionMetadata(t)));
  }

  /**
   * Stream-scope metadata for an undeclared exception that ends a stream, sink, or bidi download.
   * The request {@code otherMetadata} goes through {@link #convertOtherData(Map)}.
   */
  public static StreamPayloadMetadata undeclaredExceptionStreamMetadata(
      ServerRequestPayload requestPayload, String name, String what) {
    return streamMetadata(
        requestPayload,
        PayloadMetadata.fromExceptionMetadata(undeclaredExceptionMetadata(name, what)));
  }

  /**
   * Stream-scope metadata for a successful stream element or sink final response. The request
   * {@code otherMetadata} goes through {@link #convertOtherData(Map)}.
   */
  public static StreamPayloadMetadata streamPayloadMetadata(ServerRequestPayload requestPayload) {
    return streamMetadata(
        requestPayload,
        PayloadMetadata.fromResponseMetadata(PayloadResponseMetadata.defaultInstance()));
  }

  /**
   * Metadata for a successful first response of a stream or sink. It carries no {@code
   * otherMetadata}.
   */
  public static ResponseRpcMetadata streamFirstResponseMetadata() {
    return new ResponseRpcMetadata.Builder()
        .setPayloadMetadata(
            PayloadMetadata.fromResponseMetadata(new PayloadResponseMetadata.Builder().build()))
        .build();
  }

  /**
   * Writes a declared exception as the response struct, with the exception in field {@code
   * fieldId}. Fires {@code declaredUserException} before and {@code postWriteException} after the
   * write. The caller fires {@code preWriteException}.
   *
   * @param t the declared exception; must be {@link ThriftSerializable}
   * @param fieldId the IDL field id of the exception in the {@code throws} clause
   */
  public static Writer declaredExceptionWriter(Throwable t, ContextChain chain, int fieldId) {
    return protocol -> {
      try {
        chain.declaredUserException(t);
        protocol.writeStructBegin(TSTRUCT);
        protocol.writeFieldBegin(new TField(RESPONSE_FIELD_NAME, TType.STRUCT, (short) fieldId));
        ((ThriftSerializable) t).write0(protocol);
        protocol.writeFieldEnd();
        protocol.writeFieldStop();
        protocol.writeStructEnd();
        chain.postWriteException(t);
      } catch (Throwable e) {
        throw Exceptions.propagate(e);
      }
    };
  }

  /**
   * Writes a {@link TApplicationException} as the response body. Fires {@code
   * undeclaredUserException} before and {@code postWriteException} after the write. The caller
   * fires {@code preWriteException}.
   */
  public static Writer undeclaredExceptionWriter(TApplicationException t, ContextChain chain) {
    return protocol -> {
      try {
        chain.undeclaredUserException(t);
        t.write(protocol);
        chain.postWriteException(t);
      } catch (Throwable e) {
        throw Exceptions.propagate(e);
      }
    };
  }

  /**
   * Wraps {@code writer} so that it fires {@code preWriteException(t)} at write time, before {@code
   * writer} runs. For callers that fire the hook when the payload is written, not when it is built.
   */
  public static Writer withPreWriteException(Throwable t, ContextChain chain, Writer writer) {
    return protocol -> {
      try {
        chain.preWriteException(t);
      } catch (Throwable e) {
        throw Exceptions.propagate(e);
      }
      writer.write(protocol);
    };
  }

  /** Writes response metadata in the protocol that the caller supplies (compact on Rocket). */
  public static Writer metadataWriter(ResponseRpcMetadata metadata) {
    return protocol -> {
      try {
        metadata.write0(protocol);
      } catch (Throwable t) {
        throw Exceptions.propagate(t);
      }
    };
  }

  /**
   * Writes stream payload metadata in the protocol that the caller supplies (compact on Rocket).
   */
  public static Writer metadataWriter(StreamPayloadMetadata metadata) {
    return protocol -> {
      try {
        metadata.write0(protocol);
      } catch (Throwable t) {
        throw Exceptions.propagate(t);
      }
    };
  }

  private static PayloadExceptionMetadata serverBlamedAppUnknownException() {
    return PayloadExceptionMetadata.fromAppUnknownException(
        new PayloadAppUnknownExceptionMetdata.Builder()
            .setErrorClassification(
                new ErrorClassification.Builder().setBlame(ErrorBlame.SERVER).build())
            .build());
  }

  private static ResponseRpcMetadata exceptionResponseMetadata(
      ServerRequestPayload requestPayload, PayloadExceptionMetadataBase base) {
    return new ResponseRpcMetadata.Builder()
        .setOtherMetadata(requestPayload.getRequestRpcMetadata().getOtherMetadata())
        .setPayloadMetadata(PayloadMetadata.fromExceptionMetadata(base))
        .build();
  }

  private static StreamPayloadMetadata streamMetadata(
      ServerRequestPayload requestPayload, PayloadMetadata payloadMetadata) {
    return new StreamPayloadMetadata.Builder()
        .setOtherMetadata(
            convertOtherData(requestPayload.getRequestRpcMetadata().getOtherMetadata()))
        .setPayloadMetadata(payloadMetadata)
        .build();
  }

  public static PayloadExceptionMetadata createDeclaredPayloadException() {
    return createDeclaredPayloadException((Throwable) null);
  }

  public static PayloadExceptionMetadata createDeclaredPayloadException(Throwable t) {
    PayloadDeclaredExceptionMetadata metadata =
        new PayloadDeclaredExceptionMetadata.Builder()
            .setErrorClassification(errorClassificationFor(t))
            .build();

    return PayloadExceptionMetadata.fromDeclaredException(metadata);
  }

  public static ServerResponsePayload createServerResponsePayload(
      final ServerRequestPayload payload, final Writer writer, String what) {
    return createServerResponsePayload(payload, writer, null, what);
  }

  public static ServerResponsePayload createServerResponsePayload(
      final ServerRequestPayload payload, final Writer writer, String name, String what) {

    PayloadExceptionMetadataBase base = null;
    if (name != null || what != null) {
      base =
          new PayloadExceptionMetadataBase.Builder()
              .setNameUtf8(name)
              .setWhatUtf8(what)
              .setMetadata(createDeclaredPayloadException())
              .build();
    }

    return ServerResponsePayload.create(
        writer, createResponseRpcMetadata(payload.getRequestRpcMetadata(), base), null, false);
  }

  public static ServerResponsePayload createServerResponsePayload(
      final ServerRequestPayload payload, final Writer writer, final Throwable t) {
    PayloadExceptionMetadataBase base = t == null ? null : declaredExceptionMetadata(t);
    return ServerResponsePayload.create(
        writer, createResponseRpcMetadata(payload.getRequestRpcMetadata(), base), null, false);
  }

  private static ResponseRpcMetadata createResponseRpcMetadata(
      final RequestRpcMetadata requestRpcMetadata, PayloadExceptionMetadataBase base) {
    final PayloadMetadata payloadMetadata;
    if (base != null) {
      payloadMetadata = PayloadMetadata.fromExceptionMetadata(base);
    } else {
      payloadMetadata =
          PayloadMetadata.fromResponseMetadata(PayloadResponseMetadata.defaultInstance());
    }

    return new ResponseRpcMetadata.Builder()
        .setPayloadMetadata(payloadMetadata)
        .setOtherMetadata(requestRpcMetadata.getOtherMetadata())
        .build();
  }
}
