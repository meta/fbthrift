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

import static com.google.common.base.Strings.repeat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.TooLongFrameException;
import org.junit.jupiter.api.Test;

class ThriftHeaderFrameLengthPrependerTest {
  private static final int MAX_STANDARD_FRAME_LENGTH = 16;

  @Test
  void writesFourByteLengthForStandardFrame() {
    EmbeddedChannel channel =
        new EmbeddedChannel(new ThriftHeaderFrameLengthPrepender(MAX_STANDARD_FRAME_LENGTH));

    channel.writeOutbound(frame(16));

    assertThat(writtenHex(channel)).isEqualTo("00000010" + repeat("ab", 16));
    channel.finishAndReleaseAll();
  }

  @Test
  void failsOnlyTheWriteOfALongerFrame() {
    EmbeddedChannel channel =
        new EmbeddedChannel(new ThriftHeaderFrameLengthPrepender(MAX_STANDARD_FRAME_LENGTH));
    ByteBuf longer = frame(17);

    assertThatThrownBy(() -> channel.writeOutbound(longer))
        .hasCauseInstanceOf(TooLongFrameException.class);

    assertThat(longer.refCnt()).isZero();
    assertThat(writtenHex(channel)).isEmpty();
    assertThat(channel.isActive()).isTrue();
    channel.finishAndReleaseAll();
  }

  private static ByteBuf frame(int length) {
    ByteBuf frame = Unpooled.buffer(length);
    for (int i = 0; i < length; i++) {
      frame.writeByte(0xab);
    }
    return frame;
  }

  private static String writtenHex(EmbeddedChannel channel) {
    ByteBuf all = Unpooled.buffer();
    for (ByteBuf part = channel.readOutbound(); part != null; part = channel.readOutbound()) {
      all.writeBytes(part);
      part.release();
    }
    String hex = ByteBufUtil.hexDump(all);
    all.release();
    return hex;
  }
}
