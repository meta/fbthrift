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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.facebook.thrift.compression.CompressionManager;
import com.facebook.thrift.protocol.TProtocolType;
import com.google.common.base.Strings;
import com.google.common.primitives.Ints;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.TooLongFrameException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.thrift.CompressionAlgorithm;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TTransform;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.junit.jupiter.api.Test;

class HeaderTransportCodecTest {

  private static final int HEADER_MAGIC = 0x0FFF;
  private static final int BINARY_PROTOCOL_ID = 0;
  private static final int FRAME_SEQUENCE_ID = 7;
  private static final int PROTOCOL_SEQUENCE_ID = 42;
  private final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;

  @Test
  void decodeFrameNoTransforms() {
    byte[] payload = "no compression".getBytes(StandardCharsets.UTF_8);
    ByteBuf frame = buildFrame(new int[0], Unpooled.wrappedBuffer(payload));

    ThriftFrame result = HeaderTransportCodec.decodeFrame(allocator, frame);

    byte[] decoded = new byte[result.getMessage().readableBytes()];
    result.getMessage().readBytes(decoded);
    assertThat(decoded).isEqualTo(payload);
    result.release();
  }

  @Test
  void decodeFrameWithZlibTransform() {
    byte[] payload = "ZLIB compressed payload for THeader".getBytes(StandardCharsets.UTF_8);
    ByteBuf compressed =
        CompressionManager.compress(
            CompressionAlgorithm.ZLIB, allocator, Unpooled.wrappedBuffer(payload));

    ByteBuf frame = buildFrame(new int[] {TTransform.ZLIB.getValue()}, compressed);

    ThriftFrame result = HeaderTransportCodec.decodeFrame(allocator, frame);

    byte[] decoded = new byte[result.getMessage().readableBytes()];
    result.getMessage().readBytes(decoded);
    assertThat(decoded).isEqualTo(payload);
    result.release();
  }

  @Test
  void decodeFrameWithZstdTransform() {
    byte[] payload = "ZSTD compressed payload for THeader".getBytes(StandardCharsets.UTF_8);
    ByteBuf compressed =
        CompressionManager.compress(
            CompressionAlgorithm.ZSTD, allocator, Unpooled.wrappedBuffer(payload));

    ByteBuf frame = buildFrame(new int[] {TTransform.ZSTD.getValue()}, compressed);

    ThriftFrame result = HeaderTransportCodec.decodeFrame(allocator, frame);

    byte[] decoded = new byte[result.getMessage().readableBytes()];
    result.getMessage().readBytes(decoded);
    assertThat(decoded).isEqualTo(payload);
    result.release();
  }

  @Test
  void decodeFrameWithLz4Transform() {
    byte[] payload = "LZ4 compressed payload for THeader".getBytes(StandardCharsets.UTF_8);
    ByteBuf compressed =
        CompressionManager.compress(
            CompressionAlgorithm.LZ4, allocator, Unpooled.wrappedBuffer(payload));

    ByteBuf frame = buildFrame(new int[] {TTransform.LZ4.getValue()}, compressed);

    ThriftFrame result = HeaderTransportCodec.decodeFrame(allocator, frame);

    byte[] decoded = new byte[result.getMessage().readableBytes()];
    result.getMessage().readBytes(decoded);
    assertThat(decoded).isEqualTo(payload);
    result.release();
  }

  @Test
  void decodeFrameWithNoneTransform() {
    byte[] payload = "NONE transform passthrough".getBytes(StandardCharsets.UTF_8);
    ByteBuf frame =
        buildFrame(new int[] {TTransform.NONE.getValue()}, Unpooled.wrappedBuffer(payload));

    ThriftFrame result = HeaderTransportCodec.decodeFrame(allocator, frame);

    byte[] decoded = new byte[result.getMessage().readableBytes()];
    result.getMessage().readBytes(decoded);
    assertThat(decoded).isEqualTo(payload);
    result.release();
  }

  @Test
  void serverSendsResponseWithinLimitUnchanged() throws Exception {
    EmbeddedChannel channel = new EmbeddedChannel(new HeaderTransportCodec(true, 256));

    channel.writeOutbound(reply(64));
    ThriftFrame sent = HeaderTransportCodec.decodeFrame(allocator, channel.readOutbound());

    TMessage message = TProtocolType.TBinary.apply(sent.getMessage()).readMessageBegin();
    assertThat(message.type).isEqualTo(TMessageType.REPLY);
    assertThat(sent.getHeaders()).doesNotContainKey("ex");
    sent.release();
    channel.finishAndReleaseAll();
  }

  @Test
  void serverReplacesResponseOverLimitWithResponseTooBig() throws Exception {
    EmbeddedChannel channel = new EmbeddedChannel(new HeaderTransportCodec(true, 256));
    ThriftFrame response = reply(1024);

    channel.writeOutbound(response);
    ThriftFrame sent = HeaderTransportCodec.decodeFrame(allocator, channel.readOutbound());

    TProtocol in = TProtocolType.TBinary.apply(sent.getMessage());
    TMessage message = in.readMessageBegin();
    TApplicationException exception = TApplicationException.read(in);
    assertThat(sent.getSequenceId()).isEqualTo(FRAME_SEQUENCE_ID);
    assertThat(message.name).isEqualTo("getPartitions");
    assertThat(message.type).isEqualTo(TMessageType.EXCEPTION);
    assertThat(message.seqid).isEqualTo(PROTOCOL_SEQUENCE_ID);
    assertThat(exception.getType()).isEqualTo(TApplicationException.INTERNAL_ERROR);
    assertThat(exception.getMessage()).isEqualTo("Response size too big");
    assertThat(sent.getHeaders()).containsEntry("ex", "17").containsEntry("uex", "MetaException");
    assertThat(response.refCnt()).isZero();
    sent.release();
    channel.finishAndReleaseAll();
  }

  @Test
  void clientFailsRequestOverLimitWithoutSendingIt() {
    EmbeddedChannel channel = new EmbeddedChannel(new HeaderTransportCodec(false, 256));
    ThriftFrame request = reply(1024);

    assertThatThrownBy(() -> channel.writeOutbound(request))
        .isInstanceOf(TooLongFrameException.class);

    assertThat((Object) channel.readOutbound()).isNull();
    assertThat(request.refCnt()).isZero();
    assertThat(channel.isActive()).isTrue();
    channel.finishAndReleaseAll();
  }

  @Test
  void serverReplacesCompressedResponseOverLimitWithResponseTooBig() throws Exception {
    EmbeddedChannel channel = new EmbeddedChannel(new HeaderTransportCodec(true, 256));
    ThriftFrame response = reply(1024, TTransform.ZSTD.getValue());

    channel.writeOutbound(response);
    ThriftFrame sent = HeaderTransportCodec.decodeFrame(allocator, channel.readOutbound());

    TProtocol in = TProtocolType.TBinary.apply(sent.getMessage());
    TMessage message = in.readMessageBegin();
    assertThat(message.name).isEqualTo("getPartitions");
    assertThat(TApplicationException.read(in).getMessage()).isEqualTo("Response size too big");
    assertThat(response.refCnt()).isZero();
    sent.release();
    channel.finishAndReleaseAll();
  }

  /** A response frame as the server handler writes it: a reply envelope, a body and a header. */
  private ThriftFrame reply(int bodyBytes, int... transforms) {
    ByteBuf message = allocator.buffer();
    TProtocol out = TProtocolType.TBinary.apply(message);
    out.writeMessageBegin(new TMessage("getPartitions", TMessageType.REPLY, PROTOCOL_SEQUENCE_ID));
    byte[] body = new byte[bodyBytes];
    new Random(1).nextBytes(body);
    message.writeBytes(body);

    Map<String, String> headers = new HashMap<>();
    headers.put("uex", "MetaException");
    return new ThriftFrame(
        FRAME_SEQUENCE_ID,
        message,
        headers,
        Collections.emptyMap(),
        LegacyTransportType.HEADER,
        TProtocolType.TBinary,
        true,
        Ints.asList(transforms));
  }

  @Test
  void encodeFrameCompressesWithItsTransformsAndWritesTheirIds() {
    EmbeddedChannel channel = new EmbeddedChannel(new HeaderTransportCodec(true));
    byte[] payload = repeatedPayload();

    channel.writeOutbound(frameWithTransforms(payload, TTransform.ZSTD.getValue()));
    ByteBuf written = channel.readOutbound();

    // After the 10-byte frame header: protocol id, number of transforms, transform ids.
    assertThat(written.getByte(10)).isEqualTo((byte) BINARY_PROTOCOL_ID);
    assertThat(written.getByte(11)).isEqualTo((byte) 1);
    assertThat(written.getByte(12)).isEqualTo((byte) TTransform.ZSTD.getValue());
    assertThat(written.readableBytes()).isLessThan(payload.length);

    ThriftFrame read = HeaderTransportCodec.decodeFrame(allocator, written);
    assertThat(ByteBufUtil.getBytes(read.getMessage())).isEqualTo(payload);
    assertThat(read.getTransforms()).containsExactly(TTransform.ZSTD.getValue());
    read.release();
    channel.finishAndReleaseAll();
  }

  /**
   * Builds a minimal THeader frame suitable for decodeFrame(). The wire format is:
   *
   * <pre>
   *   short  magic (0x0FFF)
   *   short  flags
   *   int    sequenceId
   *   short  headerSize (in 4-byte words)
   *   [header section]:
   *     varint protocolId
   *     varint numTransforms
   *     varint transformId * numTransforms
   *     [padding to 4-byte boundary]
   *   [payload]
   * </pre>
   */
  private ByteBuf buildFrame(int[] transformIds, ByteBuf payload) {
    ByteBuf headerSection = allocator.buffer();
    writeVarInt(headerSection, BINARY_PROTOCOL_ID);
    writeVarInt(headerSection, transformIds.length);
    for (int id : transformIds) {
      writeVarInt(id, headerSection);
    }

    int headerBytes = headerSection.readableBytes();
    int paddingSize = 4 - (headerBytes & 0b11);
    headerSection.writeZero(paddingSize);
    int paddedHeaderSize = headerBytes + paddingSize;

    ByteBuf frame = allocator.buffer();
    frame.writeShort(HEADER_MAGIC);
    frame.writeShort(0); // flags: none
    frame.writeInt(1); // sequenceId
    frame.writeShort(paddedHeaderSize >> 2); // header size in 4-byte words
    frame.writeBytes(headerSection);
    frame.writeBytes(payload);

    headerSection.release();
    payload.release();
    return frame;
  }

  private static void writeVarInt(ByteBuf buffer, int value) {
    while (true) {
      if ((value & ~0x7F) == 0) {
        buffer.writeByte(value);
        return;
      }
      buffer.writeByte(value | 0x80);
      value >>>= 7;
    }
  }

  private static void writeVarInt(int value, ByteBuf buffer) {
    writeVarInt(buffer, value);
  }

  private static byte[] repeatedPayload() {
    return Strings.repeat("compressible payload ", 200).getBytes(StandardCharsets.UTF_8);
  }

  private static ThriftFrame frameWithTransforms(byte[] payload, int... transforms) {
    return new ThriftFrame(
        1,
        Unpooled.wrappedBuffer(payload),
        new HashMap<>(),
        Collections.emptyMap(),
        LegacyTransportType.HEADER,
        TProtocolType.TBinary,
        false,
        Ints.asList(transforms));
  }
}
