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

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.zip.Deflater;
import java.util.zip.DeflaterInputStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CompressionUtilTest {
  @Test
  public void testInflateByteArray() throws Exception {
    InputStream s = Thread.currentThread().getContextClassLoader().getResourceAsStream("lorem.txt");

    DeflaterInputStream dis = new DeflaterInputStream(s);
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    byte[] buf = new byte[1024];
    int len;

    while ((len = dis.read(buf)) > 0) bos.write(buf, 0, len);

    dis.close();
    bos.close();

    byte[] bytes = bos.toByteArray();

    s = Thread.currentThread().getContextClassLoader().getResourceAsStream("lorem.txt");
    bos = new ByteArrayOutputStream();
    assert s != null;

    while ((len = s.read(buf)) > 0) bos.write(buf, 0, len);

    String text = new String(bos.toByteArray(), StandardCharsets.UTF_8);

    ByteBufAllocator alloc = ByteBufAllocator.DEFAULT;
    ByteBuf in = Unpooled.wrappedBuffer(bytes);

    ByteBuf out = CompressionUtil.inflate(alloc, in);

    Assertions.assertEquals(text, out.toString(StandardCharsets.UTF_8));
  }

  @Test
  public void testInflateDirectMemory() throws Exception {
    InputStream s = Thread.currentThread().getContextClassLoader().getResourceAsStream("lorem.txt");

    DeflaterInputStream dis = new DeflaterInputStream(s);
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    byte[] buf = new byte[1024];
    int len;

    while ((len = dis.read(buf)) > 0) bos.write(buf, 0, len);

    dis.close();
    bos.close();

    byte[] bytes = bos.toByteArray();

    s = Thread.currentThread().getContextClassLoader().getResourceAsStream("lorem.txt");
    bos = new ByteArrayOutputStream();
    assert s != null;

    while ((len = s.read(buf)) > 0) bos.write(buf, 0, len);

    String text = new String(bos.toByteArray(), StandardCharsets.UTF_8);

    ByteBufAllocator alloc = ByteBufAllocator.DEFAULT;
    ByteBuf in = alloc.buffer();
    in.writeBytes(bytes);

    ByteBuf out = CompressionUtil.inflate(alloc, in);

    Assertions.assertEquals(text, out.toString(StandardCharsets.UTF_8));
    Assertions.assertEquals(0, in.refCnt());
  }

  @Test
  public void testInflateMultiSegmentInput() {
    ByteBufAllocator alloc = ByteBufAllocator.DEFAULT;
    byte[] original = new byte[64 * 1024];
    new Random(42).nextBytes(original);

    Deflater deflater = new Deflater();
    deflater.setInput(original);
    deflater.finish();

    ByteBuf deflated = alloc.directBuffer();
    byte[] chunk = new byte[4096];

    while (!deflater.finished()) {
      deflated.writeBytes(chunk, 0, deflater.deflate(chunk));
    }

    deflater.end();

    int half = deflated.readableBytes() / 2;
    ByteBuf first = alloc.directBuffer(half);
    deflated.readBytes(first);
    ByteBuf second = alloc.directBuffer(deflated.readableBytes());
    deflated.readBytes(second);
    deflated.release();

    ByteBuf in = spy(alloc.compositeDirectBuffer().addComponents(true, first, second));
    Assertions.assertTrue(((CompositeByteBuf) in).nioBufferCount() > 1);

    ByteBuf out = CompressionUtil.inflate(alloc, in);

    verify(in, never()).nioBuffer();

    byte[] result = new byte[out.readableBytes()];
    out.readBytes(result);
    out.release();

    Assertions.assertArrayEquals(original, result);
    Assertions.assertEquals(0, in.refCnt());
  }

  @Test
  public void testCoalesceKeepsSingleSegmentInput() {
    ByteBufAllocator alloc = ByteBufAllocator.DEFAULT;
    ByteBuf input = alloc.directBuffer(64).writeZero(64);

    ByteBuf result = CompressionUtil.coalesce(alloc, input);

    Assertions.assertSame(input, result);
    Assertions.assertEquals(1, input.refCnt());
    input.release();
  }

  @Test
  public void testCoalesceCopiesMultiSegmentInputOnce() {
    CountingAllocator counting = new CountingAllocator();
    byte[] bytes = new byte[4096];

    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte) i;
    }

    ByteBuf first = counting.directBuffer(1024).writeBytes(bytes, 0, 1024);
    ByteBuf second = counting.directBuffer(3072).writeBytes(bytes, 1024, 3072);
    ByteBuf input = counting.compositeDirectBuffer().addComponents(true, first, second);
    input.skipBytes(512);
    int readable = input.readableBytes();

    counting.capacities.clear();

    ByteBuf result = CompressionUtil.coalesce(counting, input);

    try {
      Assertions.assertEquals(0, input.refCnt());
      Assertions.assertEquals(1, counting.capacities.size());
      Assertions.assertEquals(readable, (int) counting.capacities.get(0));
      Assertions.assertEquals(1, result.nioBufferCount());
      Assertions.assertTrue(result.isDirect());
      Assertions.assertEquals(3584, result.readableBytes());

      byte[] actual = new byte[result.readableBytes()];
      result.getBytes(result.readerIndex(), actual);
      byte[] expected = new byte[3584];
      System.arraycopy(bytes, 512, expected, 0, 3584);
      Assertions.assertArrayEquals(expected, actual);
    } finally {
      result.release();
    }
  }

  @Test
  public void testCoalesceKeepsReadableSingleSegmentInput() {
    CountingAllocator counting = new CountingAllocator();
    byte[] bytes = new byte[4096];

    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte) i;
    }

    ByteBuf first = counting.directBuffer(1024).writeBytes(bytes, 0, 1024);
    ByteBuf second = counting.directBuffer(3072).writeBytes(bytes, 1024, 3072);
    ByteBuf input = counting.compositeDirectBuffer().addComponents(true, first, second);
    input.skipBytes(1024);

    counting.capacities.clear();

    ByteBuf result = CompressionUtil.coalesce(counting, input);

    Assertions.assertSame(input, result);
    Assertions.assertTrue(counting.capacities.isEmpty());
    Assertions.assertEquals(1, input.refCnt());
    input.release();
  }

  @Test
  public void testCoalesceKeepsHeapSingleSegmentInput() {
    CountingAllocator counting = new CountingAllocator();
    byte[] bytes = new byte[128];

    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte) i;
    }

    ByteBuf first = counting.heapBuffer(64).writeBytes(bytes, 0, 64);
    ByteBuf second = counting.heapBuffer(64).writeBytes(bytes, 64, 64);
    ByteBuf input = counting.compositeBuffer().addComponents(true, first, second);
    input.skipBytes(64);

    counting.capacities.clear();

    ByteBuf result = CompressionUtil.coalesce(counting, input);

    Assertions.assertSame(input, result);
    Assertions.assertTrue(counting.capacities.isEmpty());
    Assertions.assertEquals(1, input.refCnt());
    input.release();
  }

  @Test
  public void testCoalesceKeepsEmptyInput() {
    ByteBufAllocator alloc = ByteBufAllocator.DEFAULT;
    ByteBuf first = alloc.directBuffer(64).writeZero(64);
    ByteBuf second = alloc.directBuffer(64).writeZero(64);
    ByteBuf input = alloc.compositeDirectBuffer().addComponents(true, first, second);
    input.skipBytes(input.readableBytes());

    ByteBuf result = CompressionUtil.coalesce(alloc, input);

    Assertions.assertSame(input, result);
    input.release();
  }

  @Test
  public void testInflateMixedMultiSegmentInput() {
    ByteBufAllocator alloc = ByteBufAllocator.DEFAULT;
    byte[] original = new byte[64 * 1024];
    new Random(42).nextBytes(original);

    Deflater deflater = new Deflater();
    deflater.setInput(original);
    deflater.finish();

    ByteBuf deflated = alloc.directBuffer();
    byte[] chunk = new byte[4096];

    while (!deflater.finished()) {
      deflated.writeBytes(chunk, 0, deflater.deflate(chunk));
    }

    deflater.end();

    int total = deflated.readableBytes();
    int third = total / 3;

    ByteBuf first = alloc.heapBuffer(third);
    deflated.readBytes(first);

    ByteBuf second = alloc.directBuffer(third);
    deflated.readBytes(second);

    int remaining = deflated.readableBytes();
    ByteBuf rest = alloc.directBuffer(remaining + 8);
    rest.writeZero(8);
    deflated.readBytes(rest, remaining);
    deflated.release();
    rest.skipBytes(8);

    ByteBuf in = spy(alloc.compositeBuffer().addComponents(true, first, second, rest));
    Assertions.assertEquals(3, in.nioBuffers(in.readerIndex(), in.readableBytes()).length);

    ByteBuf out = CompressionUtil.inflate(alloc, in);

    verify(in, never()).nioBuffer();

    byte[] result = new byte[out.readableBytes()];
    out.readBytes(result);
    out.release();

    Assertions.assertArrayEquals(original, result);
    Assertions.assertEquals(0, in.refCnt());
  }

  static final class CountingAllocator extends PooledByteBufAllocator {
    final List<Integer> capacities = new ArrayList<>();

    CountingAllocator() {
      super(true);
    }

    @Override
    protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
      capacities.add(initialCapacity);

      return super.newDirectBuffer(initialCapacity, maxCapacity);
    }
  }
}
