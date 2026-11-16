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

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

class ThriftHeaderFrameCodecTest {

  @Test
  void readsFramesItWrites() {
    EmbeddedChannel writer = new EmbeddedChannel(new ThriftHeaderFrameCodec());
    EmbeddedChannel reader = new EmbeddedChannel(new ThriftHeaderFrameCodec());

    writer.writeOutbound(frame(16), frame(64), frame(8));
    for (ByteBuf part = writer.readOutbound(); part != null; part = writer.readOutbound()) {
      reader.writeInbound(part);
    }

    assertThat(readSize(reader)).isEqualTo(16);
    assertThat(readSize(reader)).isEqualTo(64);
    assertThat(readSize(reader)).isEqualTo(8);
    assertThat(reader.isActive()).isTrue();
    writer.finishAndReleaseAll();
    reader.finishAndReleaseAll();
  }

  private static ByteBuf frame(int length) {
    return Unpooled.buffer(length).writeZero(length);
  }

  private static int readSize(EmbeddedChannel channel) {
    ByteBuf frame = channel.readInbound();
    int size = frame.readableBytes();
    frame.release();
    return size;
  }
}
