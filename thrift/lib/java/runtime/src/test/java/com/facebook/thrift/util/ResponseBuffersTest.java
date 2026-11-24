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

import static com.facebook.thrift.util.TestResponseWriters.sized;
import static com.facebook.thrift.util.TestResponseWriters.smallPayload;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.facebook.swift.service.ContextChain;
import com.facebook.thrift.example.ping.CustomException;
import com.facebook.thrift.payload.Writer;
import com.facebook.thrift.protocol.TProtocolType;
import com.facebook.thrift.test.EveryLayout;
import com.facebook.thrift.util.TestResponseWriters.CountingAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.junit.jupiter.api.Test;

class ResponseBuffersTest {

  private static final String METHOD = "ping";

  private static ByteBuf writeResponse(TProtocolType type, Writer writer, ByteBuf buf)
      throws TException {
    TProtocol out = type.apply(buf);
    out.writeMessageBegin(new TMessage(METHOD, TMessageType.REPLY, 21));
    writer.write(out);
    out.writeMessageEnd();
    return buf;
  }

  private static ByteBuf flatResponse(TProtocolType type, Writer writer) throws TException {
    return writeResponse(type, writer, ByteBufAllocator.DEFAULT.buffer());
  }

  @Test
  void sizedSmallResponseIsFlatAndNeverGrows() throws Exception {
    Writer writer = sized(smallPayload());

    for (TProtocolType type : new TProtocolType[] {TProtocolType.TBinary, TProtocolType.TCompact}) {
      CountingAllocator alloc = new CountingAllocator();
      ByteBuf buf = ResponseBuffers.allocate(alloc, writer, type, METHOD);

      try {
        writeResponse(type, writer, buf);

        assertThat(buf).isNotInstanceOf(CompositeByteBuf.class);
        assertThat(alloc.growthSteps()).isZero();
        assertThat(buf.capacity()).isGreaterThanOrEqualTo(buf.readableBytes());
      } finally {
        buf.release();
      }
    }
  }

  @Test
  void sizedLargeResponseNeverGrows() throws Exception {
    for (TProtocolType type : new TProtocolType[] {TProtocolType.TBinary, TProtocolType.TCompact}) {
      CountingAllocator alloc = new CountingAllocator();
      Writer writer = sized(TestResponseWriters.largePayload());

      ByteBuf buf = ResponseBuffers.allocate(alloc, writer, type, METHOD);

      try {
        writeResponse(type, writer, buf);

        assertThat(buf).isNotInstanceOf(CompositeByteBuf.class);
        assertThat(buf.isDirect()).isTrue();
        assertThat(alloc.growthSteps()).isZero();

        ByteBuf expected = flatResponse(type, writer);

        try {
          assertThat(ByteBufUtil.equals(buf, expected)).isTrue();
        } finally {
          expected.release();
        }
      } finally {
        buf.release();
      }
    }
  }

  @Test
  void unknownHintAllocatesFlatDirect() throws Exception {
    Writer writer = smallPayload()::write0;
    ByteBufAllocator alloc = ByteBufAllocator.DEFAULT;

    assertThat(writer.sizeHint()).isNegative();

    ByteBuf buf = ResponseBuffers.allocate(alloc, writer, TProtocolType.TCompact);

    try {
      assertThat(buf).isNotInstanceOf(CompositeByteBuf.class);
      assertThat(buf.isDirect()).isTrue();
    } finally {
      buf.release();
    }
  }

  /**
   * The declared-exception writer mirrors the generated frame, so declared errors allocate flat at
   * exactly the hinted bound.
   */
  @Test
  void declaredExceptionWriterIsFlat() throws Exception {
    CustomException exception = new CustomException.Builder().setMessage("declared boom").build();
    Writer writer = RpcPayloadUtil.declaredExceptionWriter(exception, mock(ContextChain.class), 1);

    for (TProtocolType type : new TProtocolType[] {TProtocolType.TBinary, TProtocolType.TCompact}) {
      ByteBuf buf = ResponseBuffers.allocate(UnpooledByteBufAllocator.DEFAULT, writer, type);

      try {
        assertThat(buf).isNotInstanceOf(CompositeByteBuf.class);

        writer.write(type.apply(buf));

        assertThat(buf.capacity()).isEqualTo(writer.sizeHint());
        assertThat(buf.capacity()).isGreaterThanOrEqualTo(buf.readableBytes());
      } finally {
        buf.release();
      }
    }
  }

  /**
   * A hint clamped to `Integer.MAX_VALUE` must not allocate a 2 GiB flat buffer: it takes the
   * composite path.
   */
  @Test
  void clampedHintReturnsComposite() {
    EveryLayout payload = TestResponseWriters.largePayload();

    Writer writer =
        new Writer() {
          @Override
          public void write(TProtocol protocol) throws TException {
            payload.write0(protocol);
          }

          @Override
          public int sizeHint() {
            return ThriftSizes.clampToInt(3_000_000_000L);
          }
        };

    ByteBufAllocator alloc = ByteBufAllocator.DEFAULT;

    ByteBuf buf = ResponseBuffers.allocate(alloc, writer, TProtocolType.TBinary);

    try {
      assertThat(buf).isInstanceOf(CompositeByteBuf.class);
      assertThat(buf.capacity()).isLessThan(Integer.MAX_VALUE);

      TProtocol out = TProtocolType.TBinary.apply(buf);
      writer.write(out);

      ByteBuf expected = ByteBufAllocator.DEFAULT.buffer();

      try {
        writer.write(TProtocolType.TBinary.apply(expected));

        assertThat(ByteBufUtil.equals(buf, expected)).isTrue();
      } finally {
        expected.release();
      }
    } finally {
      buf.release();
    }
  }

  @Test
  void throwingSizeHintFallsBackWithoutThrowing() {
    Writer writer =
        new Writer() {
          @Override
          public void write(TProtocol protocol) throws TException {
            throw new TException("write boom");
          }

          @Override
          public int sizeHint() {
            throw new RuntimeException("size boom");
          }
        };

    ByteBufAllocator alloc = ByteBufAllocator.DEFAULT;
    ByteBuf buf = ResponseBuffers.allocate(alloc, writer, TProtocolType.TBinary, METHOD);

    try {
      assertThat(buf).isNotInstanceOf(CompositeByteBuf.class);
      assertThat(buf.isDirect()).isTrue();
    } finally {
      buf.release();
    }
  }

  @Test
  void nullTypeAllocatesFlatDirect() {
    ByteBufAllocator alloc = ByteBufAllocator.DEFAULT;
    ByteBuf buf = ResponseBuffers.allocate(alloc, Writer.EMPTY_WRITER, null, METHOD);

    try {
      assertThat(buf).isNotInstanceOf(CompositeByteBuf.class);
      assertThat(buf.isDirect()).isTrue();
    } finally {
      buf.release();
    }
  }

  private static Writer fixedHintWriter(int hint) {
    return new Writer() {
      @Override
      public void write(TProtocol protocol) throws TException {}

      @Override
      public int sizeHint() {
        return hint;
      }
    };
  }

  @Test
  void knownHintAllocatesDirectFlat() {
    int hint = 1_000_000;
    int envelope = (int) ThriftSizes.messageBegin(METHOD);

    ByteBufAllocator alloc = ByteBufAllocator.DEFAULT;
    ByteBuf buf =
        ResponseBuffers.allocate(alloc, fixedHintWriter(hint), TProtocolType.TBinary, METHOD);

    try {
      assertThat(buf).isNotInstanceOf(CompositeByteBuf.class);
      assertThat(buf.nioBufferCount()).isEqualTo(1);
      assertThat(buf.isDirect()).isTrue();
      assertThat(buf.capacity()).isEqualTo(hint + envelope);
    } finally {
      buf.release();
    }
  }
}
