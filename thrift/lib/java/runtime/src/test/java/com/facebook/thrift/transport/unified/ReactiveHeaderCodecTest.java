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

package com.facebook.thrift.transport.unified;

import static org.assertj.core.api.Assertions.assertThat;

import com.facebook.thrift.legacy.codec.LegacyTransportType;
import com.facebook.thrift.legacy.codec.ThriftFrame;
import com.facebook.thrift.protocol.TProtocolType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.junit.jupiter.api.Test;

class ReactiveHeaderCodecTest {
  private static final int FRAME_SEQUENCE_ID = 7;
  private static final int PROTOCOL_SEQUENCE_ID = 42;

  private final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;

  @Test
  void encodesResponseWithinLimitUnchanged() throws Exception {
    ByteBuf encoded = ReactiveHeaderCodec.encodeResponseFrame(allocator, reply(64), 256);
    ThriftFrame sent = ReactiveHeaderCodec.decodeFrame(allocator, encoded);

    TMessage message = TProtocolType.TBinary.apply(sent.getMessage()).readMessageBegin();
    assertThat(message.type).isEqualTo(TMessageType.REPLY);
    assertThat(sent.getHeaders()).doesNotContainKey("ex");
    sent.release();
  }

  @Test
  void replacesResponseOverLimitWithResponseTooBig() throws Exception {
    ThriftFrame response = reply(1024);

    ByteBuf encoded = ReactiveHeaderCodec.encodeResponseFrame(allocator, response, 256);
    ThriftFrame sent = ReactiveHeaderCodec.decodeFrame(allocator, encoded);

    TProtocol in = TProtocolType.TBinary.apply(sent.getMessage());
    TMessage message = in.readMessageBegin();
    TApplicationException exception = TApplicationException.read(in);
    assertThat(sent.getSequenceId()).isEqualTo(FRAME_SEQUENCE_ID);
    assertThat(message.name).isEqualTo("getPartitions");
    assertThat(message.type).isEqualTo(TMessageType.EXCEPTION);
    assertThat(message.seqid).isEqualTo(PROTOCOL_SEQUENCE_ID);
    assertThat(exception.getType()).isEqualTo(TApplicationException.INTERNAL_ERROR);
    assertThat(exception.getMessage()).isEqualTo("Response size too big");
    assertThat(sent.getHeaders()).containsEntry("ex", "17").containsEntry("uex", "MetaException");
    assertThat(response.refCnt()).isZero();
    sent.release();
  }

  /**
   * A response frame as the connection acceptor builds it: a reply envelope, a body and a header.
   */
  private ThriftFrame reply(int bodyBytes) {
    ByteBuf message = allocator.buffer();
    TProtocol out = TProtocolType.TBinary.apply(message);
    out.writeMessageBegin(new TMessage("getPartitions", TMessageType.REPLY, PROTOCOL_SEQUENCE_ID));
    message.writeZero(bodyBytes);

    Map<String, String> headers = new HashMap<>();
    headers.put("uex", "MetaException");
    return new ThriftFrame(
        FRAME_SEQUENCE_ID,
        message,
        headers,
        Collections.emptyMap(),
        LegacyTransportType.HEADER,
        TProtocolType.TBinary,
        true);
  }
}
