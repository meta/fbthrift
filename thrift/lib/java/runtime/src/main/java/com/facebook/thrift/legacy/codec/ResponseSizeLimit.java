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

package com.facebook.thrift.legacy.codec;

import static com.facebook.thrift.util.ThriftProtocolConstants.HEADER_EX;
import static com.facebook.thrift.util.ThriftProtocolConstants.RESPONSE_TOO_BIG_ERROR_CODE;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;

/**
 * The largest response frame a HEADER server can write. THeader has no valid 4-byte length above
 * 0x3FFFFFFF bytes. A larger response is answered the way the C++ server answers a response above
 * its {@code maxResponseSize} ({@code Cpp2Connection::Cpp2Request::sendReply}): {@code
 * TApplicationException(INTERNAL_ERROR, "Response size too big")} with the {@code ex} header set to
 * {@code kResponseTooBigErrorCode}.
 */
public final class ResponseSizeLimit {
  public static final String MESSAGE = "Response size too big";

  private ResponseSizeLimit() {}

  /**
   * Returns the exception frame that answers the same call as {@code response}. Reads only the
   * message envelope of {@code response} and does not take its reference.
   */
  public static ThriftFrame responseTooBig(ByteBufAllocator alloc, ThriftFrame response) {
    TMessage reply =
        response.getProtocol().apply(response.getMessage().duplicate()).readMessageBegin();

    ByteBuf message = alloc.buffer();
    TProtocol out = response.getProtocol().apply(message);
    out.writeMessageBegin(new TMessage(reply.name, TMessageType.EXCEPTION, reply.seqid));
    new TApplicationException(TApplicationException.INTERNAL_ERROR, MESSAGE).write(out);
    out.writeMessageEnd();

    Map<String, String> headers = new HashMap<>(response.getHeaders());
    headers.put(HEADER_EX, RESPONSE_TOO_BIG_ERROR_CODE);

    return new ThriftFrame(
        response.getSequenceId(),
        message,
        headers,
        Collections.emptyMap(),
        response.getTransport(),
        response.getProtocol(),
        response.isSupportOutOfOrderResponse());
  }
}
