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

package com.facebook.thrift.legacy.codec;

import static com.facebook.thrift.util.ThriftProtocolConstants.BIG_FRAME_MAGIC;
import static org.assertj.core.api.Assertions.assertThat;

import com.facebook.thrift.legacy.client.ThriftClientHandler;
import com.facebook.thrift.protocol.TProtocolType;
import io.airlift.units.DataSize;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class LegacyTransportTypeTest {
  private static final int HEADER_MAGIC = 0x0FFF;
  private static final int COMPACT_PROTOCOL_ID = 2;
  private static final int HEADER_FRAME_BYTES = 14;

  private final List<ThriftFrame> delivered = new ArrayList<>();
  private final List<Throwable> errors = new ArrayList<>();

  @Test
  void headerClientDeliversStandardFrame() {
    EmbeddedChannel channel = newHeaderClientChannel();

    channel.writeInbound(standardFrame(1, 64));

    assertThat(sequenceIds()).containsExactly(1);
    assertThat(errors).isEmpty();
    channel.finishAndReleaseAll();
  }

  @Test
  void headerClientDeliversBigFrameAndLaterFrames() {
    EmbeddedChannel channel = newHeaderClientChannel();

    channel.writeInbound(bigFrame(1, 64));
    channel.writeInbound(standardFrame(2, 64));

    assertThat(sequenceIds()).containsExactly(1, 2);
    assertThat(errors).isEmpty();
    channel.finishAndReleaseAll();
  }

  @Test
  void headerClientDeliversFramesThatArriveInPieces() {
    EmbeddedChannel channel = newHeaderClientChannel();
    ByteBuf stream = Unpooled.wrappedBuffer(bigFrame(1, 64), standardFrame(2, 64));

    // The first pieces split the 12-byte big frame prefix; the last split the bodies.
    for (int size : new int[] {3, 5, 7, 40, 30}) {
      channel.writeInbound(stream.readRetainedSlice(size));
    }
    channel.writeInbound(stream.readRetainedSlice(stream.readableBytes()));
    stream.release();

    assertThat(sequenceIds()).containsExactly(1, 2);
    assertThat(errors).isEmpty();
    channel.finishAndReleaseAll();
  }

  /** Java HEADER servers write a 4-byte length for every frame, also above 0x3FFFFFFF bytes. */
  @Test
  void headerClientDeliversStandardLengthOverOneGib() {
    EmbeddedChannel channel = newHeaderClientChannel();

    channel.writeInbound(standardFrame(1, 0x40000000));
    channel.writeInbound(standardFrame(2, 64));

    assertThat(sequenceIds()).containsExactly(1, 2);
    assertThat(errors).isEmpty();
    channel.finishAndReleaseAll();
  }

  @Test
  void headerClientClosesOnStandardLengthOfTwoGibOrMore() {
    EmbeddedChannel channel = newHeaderClientChannel();
    ByteBuf frame = Unpooled.buffer(Integer.BYTES + HEADER_FRAME_BYTES + 64);
    frame.writeInt(0x80000000);
    writeHeaderFrame(frame, 1, 64);

    channel.writeInbound(frame);

    assertThat(errors).isNotEmpty();
    assertThat(channel.isActive()).isFalse();
    channel.finishAndReleaseAll();
  }

  private EmbeddedChannel newHeaderClientChannel() {
    EmbeddedChannel channel = new EmbeddedChannel();
    LegacyTransportType.HEADER.addFrameHandlers(
        channel.pipeline(),
        Optional.of(TProtocolType.TCompact),
        new DataSize(16, DataSize.Unit.MEGABYTE),
        true);

    String clientHandler = channel.pipeline().context(ThriftClientHandler.class).name();
    channel.pipeline().addBefore(clientHandler, "recorder", new Recorder());
    return channel;
  }

  private List<Integer> sequenceIds() {
    List<Integer> ids = new ArrayList<>();
    for (ThriftFrame frame : delivered) {
      ids.add(frame.getSequenceId());
    }
    return ids;
  }

  /**
   * A THeader frame with a 4-byte length, in one contiguous buffer so the decoder does not copy it.
   */
  private static ByteBuf standardFrame(int sequenceId, int payloadBytes) {
    int length = HEADER_FRAME_BYTES + payloadBytes;
    ByteBuf frame = Unpooled.buffer(Integer.BYTES + length);
    frame.writeInt(length);
    writeHeaderFrame(frame, sequenceId, payloadBytes);
    return frame;
  }

  /** The framing the C++ `THeader` writes for a frame longer than 0x3FFFFFFF bytes. */
  private static ByteBuf bigFrame(int sequenceId, int payloadBytes) {
    int length = HEADER_FRAME_BYTES + payloadBytes;
    ByteBuf frame = Unpooled.buffer(Integer.BYTES + Long.BYTES + length);
    frame.writeInt(BIG_FRAME_MAGIC);
    frame.writeLong(length);
    writeHeaderFrame(frame, sequenceId, payloadBytes);
    return frame;
  }

  private static void writeHeaderFrame(ByteBuf out, int sequenceId, int payloadBytes) {
    out.writeShort(HEADER_MAGIC);
    out.writeShort(0);
    out.writeInt(sequenceId);

    // Header section of 4 bytes: protocol id, zero transforms, padding.
    out.writeShort(1);
    out.writeByte(COMPACT_PROTOCOL_ID);
    out.writeByte(0);
    out.writeZero(2);

    out.writeZero(payloadBytes);
  }

  private final class Recorder extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
      if (msg instanceof ThriftFrame) {
        delivered.add((ThriftFrame) msg);
        ReferenceCountUtil.release(((ThriftFrame) msg).getMessage());
        return;
      }

      ctx.fireChannelRead(msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      errors.add(cause);
      ctx.fireExceptionCaught(cause);
    }
  }
}
