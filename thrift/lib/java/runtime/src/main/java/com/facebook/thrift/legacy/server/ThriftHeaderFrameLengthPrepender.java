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

import static com.facebook.thrift.util.ThriftProtocolConstants.MAX_FRAME_SIZE;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.TooLongFrameException;
import java.util.List;

/**
 * Writes the 4-byte THeader length prefix that {@link ThriftHeaderFrameLengthBasedDecoder} reads.
 * THeader has no valid 4-byte length above 0x3FFFFFFF bytes, so a larger frame fails to write: only
 * its own call fails, and no invalid frame reaches the wire.
 */
final class ThriftHeaderFrameLengthPrepender extends MessageToMessageEncoder<ByteBuf> {
  private final int maxStandardFrameLength;

  ThriftHeaderFrameLengthPrepender() {
    this(MAX_FRAME_SIZE);
  }

  /** Tests lower the limit; a real frame above 0x3FFFFFFF bytes does not fit in a test heap. */
  ThriftHeaderFrameLengthPrepender(int maxStandardFrameLength) {
    this.maxStandardFrameLength = maxStandardFrameLength;
  }

  @Override
  protected void encode(ChannelHandlerContext ctx, ByteBuf frame, List<Object> out) {
    int length = frame.readableBytes();

    if (length > maxStandardFrameLength) {
      throw new TooLongFrameException(
          "THeader frame of " + length + " bytes exceeds " + maxStandardFrameLength + " bytes");
    }

    out.add(ctx.alloc().buffer(Integer.BYTES).writeInt(length));
    out.add(frame.retain());
  }
}
