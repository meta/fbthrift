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

package com.facebook.thrift.rsocket.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import io.rsocket.Payload;
import java.util.ArrayList;
import java.util.List;
import org.apache.thrift.CompressionAlgorithm;
import org.junit.jupiter.api.Test;

public class PayloadUtilTest {
  private static final ByteBufAllocator ALLOC = ByteBufAllocator.DEFAULT;

  @Test
  public void compressedThrowingFactoryReleasesAll() {
    ByteBuf data = Unpooled.wrappedBuffer(new byte[1024]);
    ByteBuf metadata = Unpooled.wrappedBuffer(new byte[] {1, 2, 3});
    List<ByteBuf> compressed = new ArrayList<>();

    assertThrows(
        IllegalStateException.class,
        () ->
            PayloadUtil.createPayload(
                ALLOC,
                CompressionAlgorithm.LZ4,
                data,
                metadata,
                (payloadData, meta) -> {
                  compressed.add(payloadData);
                  throw new IllegalStateException("boom");
                }));

    assertEquals(1, compressed.size());
    assertEquals(0, compressed.get(0).refCnt());
    assertEquals(0, data.refCnt());
    assertEquals(0, metadata.refCnt());
  }

  @Test
  public void uncompressedThrowingFactoryReleasesBoth() {
    ByteBuf data = Unpooled.wrappedBuffer(new byte[] {1, 2, 3});
    ByteBuf metadata = Unpooled.wrappedBuffer(new byte[] {4, 5});

    assertThrows(
        IllegalStateException.class,
        () ->
            PayloadUtil.createPayload(
                ALLOC,
                CompressionAlgorithm.NONE,
                data,
                metadata,
                (payloadData, meta) -> {
                  throw new IllegalStateException("boom");
                }));

    assertEquals(0, data.refCnt());
    assertEquals(0, metadata.refCnt());
  }

  @Test
  public void compressThrowingReleasesBothWithoutDoubling() {
    ByteBuf data = Unpooled.wrappedBuffer(new byte[] {1, 2, 3});
    ByteBuf metadata = Unpooled.wrappedBuffer(new byte[] {4, 5});

    assertThrows(
        RuntimeException.class,
        () -> PayloadUtil.createPayload(ALLOC, CompressionAlgorithm.CUSTOM, data, metadata));

    assertEquals(0, data.refCnt());
    assertEquals(0, metadata.refCnt());
  }

  @Test
  public void twoArgCreatePayloadTransfersOwnership() {
    ByteBuf data = Unpooled.wrappedBuffer(new byte[] {1});
    ByteBuf metadata = Unpooled.wrappedBuffer(new byte[] {2});

    Payload payload = PayloadUtil.createPayload(data, metadata);

    assertEquals(1, data.refCnt());
    assertEquals(1, metadata.refCnt());

    ReferenceCountUtil.release(payload);

    assertEquals(0, data.refCnt());
    assertEquals(0, metadata.refCnt());
  }
}
