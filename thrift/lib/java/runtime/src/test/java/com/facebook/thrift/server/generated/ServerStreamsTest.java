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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.facebook.nifty.core.RequestContext;
import com.facebook.swift.service.ContextChain;
import com.facebook.swift.service.ThriftEventHandler;
import com.facebook.thrift.payload.Writer;
import com.facebook.thrift.util.NettyNiftyRequestContext;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.junit.jupiter.api.Test;

/** Pins the plain-stream first-response hook after serialization, not at writer creation. */
final class ServerStreamsTest {

  private static final RequestContext REQ_CTX = new NettyNiftyRequestContext(new HashMap<>(), null);

  @Test
  void emptyFirstResponseWriterFiresPostWriteAfterSerialization() throws Exception {
    AtomicBoolean postWrite = new AtomicBoolean(false);
    Writer writer = writerWith(postWrite);

    assertFalse(postWrite.get(), "postWrite fired at writer creation, before serialization");
    writer.write(new TCompactProtocol(new TIOStreamTransport(new ByteArrayOutputStream())));
    assertTrue(postWrite.get(), "postWrite never fired");
  }

  @Test
  void emptyFirstResponseWriterSkipsPostWriteWhenSerializationFails() {
    AtomicBoolean postWrite = new AtomicBoolean(false);
    Writer writer = writerWith(postWrite);

    OutputStream broken =
        new OutputStream() {
          @Override
          public void write(int b) throws IOException {
            throw new IOException("boom");
          }
        };

    assertThrows(
        TException.class, () -> writer.write(new TCompactProtocol(new TIOStreamTransport(broken))));
    assertFalse(postWrite.get(), "postWrite fired although serialization failed");
  }

  private static Writer writerWith(AtomicBoolean postWrite) {
    ThriftEventHandler hooks =
        new ThriftEventHandler() {
          @Override
          public void postWrite(Object context, String methodName, Object result) {
            postWrite.set(true);
          }
        };

    ContextChain chain = new ContextChain(Collections.singletonList(hooks), "Svc.m", REQ_CTX);

    return ServerStreams.EMPTY_FIRST_RESPONSE_WRITER.createResponseWriter(
        ServerStreams.NO_FIRST_RESPONSE, chain);
  }
}
