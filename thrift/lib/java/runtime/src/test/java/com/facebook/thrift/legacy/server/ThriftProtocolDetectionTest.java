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

package com.facebook.thrift.legacy.server;

import static com.facebook.thrift.util.ThriftProtocolConstants.BIG_FRAME_MAGIC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.airlift.units.DataSize;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

class ThriftProtocolDetectionTest {

  @Test
  void switchesToHeaderWhenFirstFrameIsBigFrame() {
    EmbeddedChannel channel =
        new EmbeddedChannel(
            new ThriftProtocolDetection(
                mock(ThriftServerHandler.class), new DataSize(2, DataSize.Unit.GIGABYTE), true));

    // Only the first bytes of a big frame of more than 0x3FFFFFFF bytes arrive.
    ByteBuf prefix =
        Unpooled.buffer().writeInt(BIG_FRAME_MAGIC).writeLong(0x40000100L).writeZero(16);
    channel.writeInbound(prefix);

    assertThat(channel.isActive()).isTrue();
    assertThat(channel.pipeline().get(ThriftHeaderFrameCodec.class)).isNotNull();
    assertThat(channel.pipeline().get(ThriftProtocolDetection.class)).isNull();
    channel.finishAndReleaseAll();
  }
}
