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

package com.facebook.thrift.payload;

import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.thrift.ResponseRpcMetadata;
import org.apache.thrift.StreamPayloadMetadata;
import org.apache.thrift.TApplicationException;

public interface ServerResponsePayload {
  /*
   * In thrift, every response is either a thrift union for result and user declared exception or a TApplicationException.
   * For the first case we return a serialized thrift union where,
   * (1) field 0 is for response type (either primitive type such as int or thrift struct);
   * (2) field 1,2,3… - are for user declare exceptions in thrift IDL;
   * (3) In case of a void return no fields in that union should be set (empty union).
   * For the second case, a serialized TApplicationException is returned.
   */
  Writer getDataWriter();

  boolean isTApplicationException();

  /**
   * The actual application exception this payload carries, or null for normal responses and
   * IDL-declared errors. Header transports use the actual object, never header or writer identity,
   * to decide overload classification. Every implementer forwards or stores it explicitly.
   */
  @Nullable
  TApplicationException getApplicationException();

  ResponseRpcMetadata getResponseRpcMetadata();

  StreamPayloadMetadata getStreamPayloadMetadata();

  boolean isStreamingResponse();

  static ServerResponsePayload create(
      Writer dataWriter,
      ResponseRpcMetadata responseRpcMetadata,
      StreamPayloadMetadata streamPayloadMetadata,
      boolean hasStreamingResponse) {
    return new DefaultServerResponsePayload(
        dataWriter, responseRpcMetadata, streamPayloadMetadata, hasStreamingResponse, null);
  }

  /**
   * Creates a response carrying a {@link TApplicationException} body. The actual exception is
   * required: IDL-declared error payloads use {@link #create}, which carries no exception.
   */
  static ServerResponsePayload createWithTApplicationException(
      Writer tApplicationExceptionWriter,
      ResponseRpcMetadata responseRpcMetadata,
      StreamPayloadMetadata streamPayloadMetadata,
      boolean hasStreamingResponse,
      TApplicationException applicationException) {
    return new DefaultServerResponsePayload(
        tApplicationExceptionWriter,
        responseRpcMetadata,
        streamPayloadMetadata,
        hasStreamingResponse,
        Objects.requireNonNull(applicationException, "applicationException is null"));
  }
}
