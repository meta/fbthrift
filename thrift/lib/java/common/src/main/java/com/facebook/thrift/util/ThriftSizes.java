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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.RandomAccess;

/**
 * Upper-bound serialized widths for the Binary and Compact protocols, used to pre-size response
 * buffers without measuring first.
 *
 * <p>Each constant is the max of what {@link com.facebook.thrift.protocol.ByteBufTBinaryProtocol}
 * and {@link com.facebook.thrift.protocol.ByteBufTCompactProtocol} emit: fixed widths on Binary,
 * worst-case varint lengths on Compact. A bound computed from these never under-counts valid input
 * on either protocol, so a buffer allocated from it only grows for malformed strings or when
 * handing it a protocol neither writer covers.
 */
public final class ThriftSizes {
  public static final int BOOL = 1;
  public static final int BYTE = 1;
  public static final int I16 = 3;
  public static final int I32 = 5;
  public static final int I64 = 10;
  public static final int FLOAT = 4;
  public static final int DOUBLE = 8;
  public static final int ENUM = 5;
  public static final int FIELD = 4;
  public static final int FIELD_STOP = 1;
  public static final int LIST = 6;
  public static final int SET = 6;
  public static final int MAP = 6;
  public static final int LENGTH_PREFIX = 5;

  private ThriftSizes() {}

  /**
   * Upper bound for a string field value: the worst-case length prefix plus the UTF-8 byte count.
   * The byte count is exact for valid UTF-16. For malformed UTF-16 (a lone high surrogate before a
   * non-ASCII char), the count is less than the bytes that {@code String.getBytes(UTF_8)} writes.
   * Then the buffer grows, and the data stays correct.
   */
  public static long string(String s) {
    return (long) LENGTH_PREFIX + ByteBufUtil.utf8Bytes(s);
  }

  /** Upper bound for a binary field value of the given readable length. */
  public static long binary(int readableBytes) {
    return (long) LENGTH_PREFIX + readableBytes;
  }

  /** Upper bound for a {@code byte[]} binary field: written with its full length. */
  public static long binary(byte[] bytes) {
    return binary(bytes.length);
  }

  /** Upper bound for a {@code ByteBuffer} binary field: written with its remaining bytes. */
  public static long binary(ByteBuffer bytes) {
    return binary(bytes.remaining());
  }

  /** Upper bound for a {@code ByteBuf} binary field: written with its readable bytes. */
  public static long binary(ByteBuf bytes) {
    return binary(bytes.readableBytes());
  }

  /**
   * Upper bound for a list of strings: one {@link #string(String)} per element. Lists with random
   * access iterate by index; the rest iterate in order.
   */
  public static long stringList(List<String> values) {
    long size = 0;

    if (values instanceof RandomAccess) {
      for (int i = 0, n = values.size(); i < n; i++) {
        size = addSize(size, string(values.get(i)));
      }
    } else {
      for (String value : values) {
        size = addSize(size, string(value));
      }
    }

    return size;
  }

  /** Upper bound for a set of strings: one {@link #string(String)} per element. */
  public static long strings(Collection<String> values) {
    long size = 0;

    for (String value : values) {
      size = addSize(size, string(value));
    }

    return size;
  }

  /**
   * Upper bound for a list of binaries: one {@link #binary(byte[])} per element. Lists with random
   * access iterate by index; the rest iterate in order.
   */
  public static long binaryList(List<byte[]> values) {
    long size = 0;

    if (values instanceof RandomAccess) {
      for (int i = 0, n = values.size(); i < n; i++) {
        size = addSize(size, binary(values.get(i)));
      }
    } else {
      for (byte[] value : values) {
        size = addSize(size, binary(value));
      }
    }

    return size;
  }

  /** Upper bound for a set of binaries: one {@link #binary(byte[])} per element. */
  public static long binaries(Collection<byte[]> values) {
    long size = 0;

    for (byte[] value : values) {
      size = addSize(size, binary(value));
    }

    return size;
  }

  /**
   * Upper bound for a collection of structs: one {@code serializedSize()} per element. Returns -1
   * when any element is -1, so the caller falls back to an unbounded buffer.
   */
  public static long structs(Collection<? extends ThriftSerializable> values) {
    long size = 0;

    for (ThriftSerializable value : values) {
      long element = value.serializedSize();

      if (element < 0) {
        return -1;
      }

      size = addSize(size, element);
    }

    return size;
  }

  /**
   * Upper bound for a message begin with the given method name: 12 fixed bytes (version, name
   * prefix, sequence id on Binary; protocol id, version/type, two worst-case varints on Compact)
   * plus the exact UTF-8 bytes of the name.
   */
  public static long messageBegin(String methodName) {
    return 12L + ByteBufUtil.utf8Bytes(methodName);
  }

  /** Clamps an accumulated size into an {@code int} hint, saturating past {@code MAX_VALUE}. */
  public static int clampToInt(long size) {
    return size > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) size;
  }

  /**
   * Saturated addition for accumulated sizes: -1 on either side (unknown) stays -1, and an overflow
   * of two non-negative sizes saturates at {@code Long.MAX_VALUE} instead of wrapping.
   */
  public static long addSize(long a, long b) {
    if (a < 0 || b < 0) {
      return -1;
    }

    long sum = a + b;

    return sum < 0 ? Long.MAX_VALUE : sum;
  }
}
