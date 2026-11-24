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

import com.facebook.thrift.payload.ThriftSerializable;
import com.facebook.thrift.payload.Writer;
import com.facebook.thrift.test.EveryLayout;
import io.netty.buffer.PooledByteBufAllocator;
import java.util.ArrayList;
import java.util.List;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;

/** Shared writers with known size hints for response-buffer tests. */
public final class TestResponseWriters {

  private TestResponseWriters() {}

  /** Frames a payload the way a generated response writer does, hint included. */
  public static Writer sized(ThriftSerializable payload) {
    return new Writer() {
      @Override
      public void write(TProtocol out) throws TException {
        out.writeStructBegin(RpcPayloadUtil.TSTRUCT);
        out.writeFieldBegin(RpcPayloadUtil.STRUCT_FIELD);
        payload.write0(out);
        out.writeFieldEnd();
        out.writeFieldStop();
        out.writeStructEnd();
      }

      @Override
      public int sizeHint() {
        long payloadSize = payload.serializedSize();

        if (payloadSize < 0) {
          return -1;
        }

        return ThriftSizes.clampToInt(ThriftSizes.FIELD + payloadSize + ThriftSizes.FIELD_STOP);
      }
    };
  }

  public static EveryLayout smallPayload() {
    return new EveryLayout.Builder().setABool(true).setAInt(42).setAString("hello").build();
  }

  public static EveryLayout largePayload() {
    String chunk = new String(new char[16 * 1024]).replace('\0', 'x');
    List<String> lines = new ArrayList<>(4096);

    for (int i = 0; i < 4096; i++) {
      lines.add(chunk);
    }

    return new EveryLayout.Builder().setAList(lines).build();
  }

  /** The exact capacity `ResponseBuffers` allocates for a sized writer plus a message envelope. */
  public static int envelopeBound(Writer writer, String method) {
    return writer.sizeHint() + (int) ThriftSizes.messageBegin(method);
  }

  /** The exact capacity `ResponseBuffers` allocates for a sized writer with no envelope. */
  public static int bareBound(Writer writer) {
    return writer.sizeHint();
  }

  /** Counts buffer growth steps; a sized response must allocate without growing. */
  public static final class CountingAllocator extends PooledByteBufAllocator {
    private int growthSteps;

    public CountingAllocator() {
      super(true);
    }

    @Override
    public int calculateNewCapacity(int minNewCapacity, int maxCapacity) {
      growthSteps++;
      return super.calculateNewCapacity(minNewCapacity, maxCapacity);
    }

    public int growthSteps() {
      return growthSteps;
    }
  }
}
