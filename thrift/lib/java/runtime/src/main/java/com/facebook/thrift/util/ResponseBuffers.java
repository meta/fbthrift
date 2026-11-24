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

import com.facebook.thrift.payload.Writer;
import com.facebook.thrift.protocol.TProtocolType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

/** Buffers for server responses. */
public final class ResponseBuffers {

  private ResponseBuffers() {}

  /**
   * Allocates the response buffer for a writer plus a message envelope. On Binary or Compact with a
   * known bound under 2 GiB, one flat direct buffer of that size. A bound of 2 GiB or more gets a
   * composite that grows by segments. An unknown bound gets a normal direct buffer.
   */
  public static ByteBuf allocate(
      ByteBufAllocator alloc, Writer writer, TProtocolType type, String methodName) {
    return allocate(
        alloc,
        writer,
        type,
        (int) Math.min(ThriftSizes.messageBegin(methodName), Integer.MAX_VALUE));
  }

  /**
   * Allocates the response buffer for a bare writer with no message envelope. Behaves like {@link
   * #allocate(ByteBufAllocator, Writer, TProtocolType, String)} with a zero envelope.
   */
  public static ByteBuf allocate(ByteBufAllocator alloc, Writer writer, TProtocolType type) {
    return allocate(alloc, writer, type, 0);
  }

  /** Allocates with an explicit envelope size in bytes. */
  private static ByteBuf allocate(
      ByteBufAllocator alloc, Writer writer, TProtocolType type, int envelopeBytes) {
    long total = sizeOrUnknown(writer, type, envelopeBytes);

    // Direct, never heap: with Unsafe off (the Java 25 default) `alloc.buffer()` returns a heap
    // buffer, which the FFM compressors copy into an arena and the socket write copies again.
    if (total < 0) {
      return alloc.directBuffer();
    }

    if (total >= Integer.MAX_VALUE) {
      return alloc.compositeDirectBuffer(Integer.MAX_VALUE);
    }

    return alloc.directBuffer((int) total);
  }

  private static long sizeOrUnknown(Writer writer, TProtocolType type, int envelopeBytes) {
    if (type != TProtocolType.TBinary && type != TProtocolType.TCompact) {
      return -1;
    }

    try {
      int hint = writer.sizeHint();

      return hint < 0 ? -1 : (long) hint + envelopeBytes;
    } catch (RuntimeException e) {
      return -1;
    }
  }
}
