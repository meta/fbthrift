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

package com.facebook.thrift.legacy.client;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.facebook.thrift.legacy.codec.LegacyTransportType;
import com.facebook.thrift.legacy.codec.ThriftFrame;
import com.facebook.thrift.payload.ClientRequestPayload;
import com.facebook.thrift.protocol.ByteBufTProtocol;
import com.facebook.thrift.protocol.TProtocolType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.thrift.ProtocolId;
import org.apache.thrift.RequestRpcMetadata;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;

/** Builders shared by the Legacy client tests. */
final class LegacyTestFixtures {
  private LegacyTestFixtures() {}

  @SuppressWarnings("unchecked")
  static ClientRequestPayload<Object> mockProbingPayload(AtomicReference<Thread> readerThread) {
    ClientRequestPayload<Object> payload = mock(ClientRequestPayload.class);
    when(payload.getRequestRpcMetadata())
        .thenReturn(
            new RequestRpcMetadata.Builder()
                .setProtocol(ProtocolId.COMPACT)
                .setName("probe")
                .setOtherMetadata(new HashMap<>())
                .build());
    when(payload.getPersistentHeaders()).thenReturn(Collections.emptyMap());
    when(payload.getDataWriter()).thenReturn(out -> {});
    when(payload.getResponseReader())
        .thenReturn(
            in -> {
              readerThread.set(Thread.currentThread());
              return "ok";
            });
    when(payload.getExceptionReaders()).thenReturn(Collections.emptyMap());

    return payload;
  }

  static void writeReply(ByteBuf out, int sequenceId, String name) throws Exception {
    ByteBufTProtocol protocol = TProtocolType.TCompact.apply(out);
    protocol.writeMessageBegin(new TMessage(name, TMessageType.REPLY, sequenceId));
    protocol.writeStructBegin(new TStruct());
    protocol.writeFieldBegin(new TField("result", TType.STRING, (short) 0));
    protocol.writeFieldEnd();
    protocol.writeStructEnd();
    protocol.writeMessageEnd();
  }

  static ThriftFrame inboundReply(int sequenceId, String methodName) throws Exception {
    ByteBuf reply = Unpooled.buffer();

    writeReply(reply, sequenceId, methodName);

    return new ThriftFrame(
        sequenceId,
        reply,
        Collections.emptyMap(),
        Collections.emptyMap(),
        LegacyTransportType.HEADER,
        TProtocolType.TCompact,
        false);
  }
}
