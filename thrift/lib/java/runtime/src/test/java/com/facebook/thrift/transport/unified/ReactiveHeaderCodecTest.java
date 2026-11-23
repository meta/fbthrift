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
import com.google.common.base.Strings;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import org.apache.thrift.TTransform;
import org.junit.jupiter.api.Test;

class ReactiveHeaderCodecTest {
  private static final int BINARY_PROTOCOL_ID = 0;

  private final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;

  @Test
  void encodeFrameCompressesWithItsTransformsAndWritesTheirIds() {
    byte[] payload = Strings.repeat("compressible payload ", 200).getBytes(StandardCharsets.UTF_8);
    ThriftFrame frame =
        new ThriftFrame(
            1,
            Unpooled.wrappedBuffer(payload),
            new HashMap<>(),
            Collections.emptyMap(),
            LegacyTransportType.HEADER,
            TProtocolType.TBinary,
            false,
            Collections.singletonList(TTransform.ZLIB.getValue()));

    ByteBuf written = ReactiveHeaderCodec.encodeFrame(allocator, frame, false);

    // After the 10-byte frame header: protocol id, number of transforms, transform ids.
    assertThat(written.getByte(10)).isEqualTo((byte) BINARY_PROTOCOL_ID);
    assertThat(written.getByte(11)).isEqualTo((byte) 1);
    assertThat(written.getByte(12)).isEqualTo((byte) TTransform.ZLIB.getValue());
    assertThat(written.readableBytes()).isLessThan(payload.length);

    ThriftFrame read = ReactiveHeaderCodec.decodeFrame(allocator, written);
    assertThat(ByteBufUtil.getBytes(read.getMessage())).isEqualTo(payload);
    assertThat(read.getTransforms()).containsExactly(TTransform.ZLIB.getValue());
    read.release();
  }
}
