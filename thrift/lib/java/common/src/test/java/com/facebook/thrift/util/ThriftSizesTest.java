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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.facebook.thrift.adapter.TypeAdapter;
import com.facebook.thrift.payload.ThriftSerializable;
import com.facebook.thrift.protocol.ByteBufTBinaryProtocol;
import com.facebook.thrift.protocol.ByteBufTCompactProtocol;
import com.facebook.thrift.protocol.ByteBufTProtocol;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TType;
import org.junit.jupiter.api.Test;

/** Each {@link ThriftSizes} width bounds what the Binary and Compact writers emit. */
public class ThriftSizesTest {

  private interface WriteOp {
    void write(ByteBufTProtocol protocol) throws Exception;
  }

  private static int written(boolean compact, WriteOp op) throws Exception {
    ByteBuf buf = Unpooled.buffer();

    try {
      ByteBufTProtocol protocol =
          compact ? new ByteBufTCompactProtocol() : new ByteBufTBinaryProtocol();
      protocol.wrap(buf);
      op.write(protocol);

      return buf.readableBytes();
    } finally {
      buf.release();
    }
  }

  private static void assertBounds(long bound, WriteOp op) throws Exception {
    assertTrue(bound >= written(false, op), "binary wrote more than " + bound);
    assertTrue(bound >= written(true, op), "compact wrote more than " + bound);
  }

  @Test
  public void testPrimitiveWidths() throws Exception {
    for (boolean value : new boolean[] {true, false}) {
      assertBounds(ThriftSizes.BOOL, p -> p.writeBool(value));
    }

    for (byte value : new byte[] {0, 1, -1, Byte.MIN_VALUE, Byte.MAX_VALUE}) {
      assertBounds(ThriftSizes.BYTE, p -> p.writeByte(value));
    }

    for (short value : new short[] {0, 1, -1, 255, 256, Short.MIN_VALUE, Short.MAX_VALUE}) {
      assertBounds(ThriftSizes.I16, p -> p.writeI16(value));
    }

    for (int value :
        new int[] {
          0, 1, -1, (1 << 21) - 1, 1 << 21, Integer.MIN_VALUE, Integer.MAX_VALUE,
        }) {
      assertBounds(ThriftSizes.I32, p -> p.writeI32(value));
      assertBounds(ThriftSizes.ENUM, p -> p.writeI32(value));
    }

    for (long value :
        new long[] {
          0L, 1L, -1L, (1L << 35) - 1, 1L << 35, Long.MIN_VALUE, Long.MAX_VALUE,
        }) {
      assertBounds(ThriftSizes.I64, p -> p.writeI64(value));
    }

    for (float value : new float[] {0f, 1.5f, Float.NaN, Float.MAX_VALUE}) {
      assertBounds(ThriftSizes.FLOAT, p -> p.writeFloat(value));
    }

    for (double value : new double[] {0d, 1.5d, Double.NaN, Double.MAX_VALUE}) {
      assertBounds(ThriftSizes.DOUBLE, p -> p.writeDouble(value));
    }
  }

  @Test
  public void testFieldHeaderWidth() throws Exception {
    for (short id : new short[] {1, 15, 16, 100, Short.MAX_VALUE}) {
      TField field = new TField("f", TType.I32, id);
      assertBounds(ThriftSizes.FIELD, p -> p.writeFieldBegin(field));
    }

    // Negative and extreme ids take the full compact header: 1 type byte plus a zigzag varint
    // of up to 3 bytes, exactly the bound.
    for (short id : new short[] {-1, Short.MIN_VALUE}) {
      TField field = new TField("f", TType.I32, id);
      assertBounds(ThriftSizes.FIELD, p -> p.writeFieldBegin(field));
    }

    // Sequential ids take the compact delta path; far-apart ids take the full header.
    assertBounds(
        2L * ThriftSizes.FIELD,
        p -> {
          p.writeFieldBegin(new TField("a", TType.I32, (short) 1));
          p.writeFieldBegin(new TField("b", TType.I32, (short) 2));
        });
    assertBounds(
        2L * ThriftSizes.FIELD,
        p -> {
          p.writeFieldBegin(new TField("a", TType.I32, (short) 1));
          p.writeFieldBegin(new TField("b", TType.I32, (short) 1000));
        });

    // A boolean field folds its value into the header: header plus value still bounds it.
    assertBounds(
        (long) ThriftSizes.FIELD + ThriftSizes.BOOL,
        p -> {
          p.writeFieldBegin(new TField("b", TType.BOOL, (short) 3));
          p.writeBool(true);
        });

    assertBounds(ThriftSizes.FIELD_STOP, ByteBufTProtocol::writeFieldStop);
  }

  @Test
  public void testContainerHeaderWidths() throws Exception {
    for (int size : new int[] {0, 1, 14, 15, 100000, 1 << 28, Integer.MAX_VALUE}) {
      assertBounds(ThriftSizes.LIST, p -> p.writeListBegin(new TList(TType.I64, size)));
      assertBounds(ThriftSizes.SET, p -> p.writeSetBegin(new TSet(TType.STRING, size)));
      assertBounds(ThriftSizes.MAP, p -> p.writeMapBegin(new TMap(TType.STRING, TType.I32, size)));
    }

    // A worst-case length needs a 5-byte varint on Compact, exactly the prefix bound, while
    // Binary writes its 4-byte length. Only the header is written: no bytes are allocated.
    assertBounds(ThriftSizes.LENGTH_PREFIX, p -> p.getWritableBinaryAsByteBuf(Integer.MAX_VALUE));
  }

  @Test
  public void testStringWidth() throws Exception {
    String[] values = {
      "",
      "hello",
      new String(new char[4096]).replace('\0', 'x'),
      "héllo wörld",
      "日本語テスト",
      "emoji \uD83D\uDE00 pair",
    };

    for (String value : values) {
      long expected =
          (long) ThriftSizes.LENGTH_PREFIX + value.getBytes(StandardCharsets.UTF_8).length;

      assertEquals(expected, ThriftSizes.string(value));
      assertBounds(expected, p -> p.writeString(value));
    }
  }

  @Test
  public void testStringWidthMalformed() throws Exception {
    // Lone surrogates that Netty counts exactly: the bound matches the written bytes.
    String[] exact = {
      "lone lead \uD800 here",
      "lone trail \uDC00 here",
      "high at end abc\uD800",
      "high before ascii a\uD800b",
    };

    for (String value : exact) {
      long expected =
          (long) ThriftSizes.LENGTH_PREFIX + value.getBytes(StandardCharsets.UTF_8).length;

      assertEquals(expected, ThriftSizes.string(value));
      assertBounds(expected, p -> p.writeString(value));
    }

    // A lone high surrogate before a non-ASCII char counts below the written bytes.
    String under = "high before non-ascii a\uD800\u00e9b";
    long writtenLength = under.getBytes(StandardCharsets.UTF_8).length;
    long bound = ThriftSizes.string(under);
    String name = "one lone high surrogate";

    assertTrue(bound < (long) ThriftSizes.LENGTH_PREFIX + writtenLength, name);
    assertTrue(bound >= written(true, p -> p.writeString(under)), name);

    // Two such surrogates make a shortfall larger than Binary's prefix slack.
    String underTwice = "a\uD800\u00e9b\uD800\u00e9b";
    long twiceBound = ThriftSizes.string(underTwice);
    String twiceName = "two lone high surrogates";

    assertTrue(twiceBound < written(false, p -> p.writeString(underTwice)), twiceName);
    assertTrue(twiceBound >= written(true, p -> p.writeString(underTwice)), twiceName);
  }

  @Test
  public void testBinaryWidths() throws Exception {
    byte[] bytes = new byte[4096];

    assertBounds(ThriftSizes.binary(bytes), p -> p.writeBinary(ByteBuffer.wrap(bytes)));
    assertBounds(ThriftSizes.binary(bytes.length), p -> p.writeBinary(ByteBuffer.wrap(bytes)));

    assertBounds(ThriftSizes.binary(50), p -> p.writeBinary(allocateSlice()));

    ByteBuf buf = Unpooled.buffer(256, 256);
    buf.writerIndex(256);
    buf.readerIndex(100);

    try {
      assertEquals(156, buf.readableBytes());
      assertEquals((long) ThriftSizes.LENGTH_PREFIX + 156, ThriftSizes.binary(buf));
      assertBounds(ThriftSizes.binary(buf), p -> p.writeBinaryAsByteBuf(buf.duplicate()));
    } finally {
      buf.release();
    }
  }

  private static ByteBuffer allocateSlice() {
    ByteBuffer buf = ByteBuffer.allocate(100);
    buf.position(10);
    buf.limit(60);
    return buf;
  }

  @Test
  public void testMessageBeginWidth() throws Exception {
    String[] names = {"", "ping", "aMuchLongerMethodName", "méthodeÜnïcodé"};

    for (String name : names) {
      long bound = ThriftSizes.messageBegin(name);

      assertBounds(bound, p -> p.writeMessageBegin(new TMessage(name, TMessageType.REPLY, 1)));
      assertBounds(
          bound, p -> p.writeMessageBegin(new TMessage(name, TMessageType.REPLY, 1 << 28)));

      // A negative sequence id takes the worst-case 5-byte compact varint.
      assertBounds(bound, p -> p.writeMessageBegin(new TMessage(name, TMessageType.REPLY, -1)));
    }
  }

  @Test
  public void testClampToInt() {
    assertEquals(Integer.MAX_VALUE, ThriftSizes.clampToInt((long) Integer.MAX_VALUE + 1));
    assertEquals(Integer.MAX_VALUE, ThriftSizes.clampToInt(Long.MAX_VALUE));
    assertEquals(Integer.MAX_VALUE, ThriftSizes.clampToInt(Integer.MAX_VALUE));
    assertEquals(42, ThriftSizes.clampToInt(42));
    assertEquals(-1, ThriftSizes.clampToInt(-1));
    assertEquals(-1, ThriftSizes.clampToInt(Long.MIN_VALUE));
  }

  @Test
  public void testAddSizeSaturates() {
    assertEquals(Long.MAX_VALUE, ThriftSizes.addSize(Long.MAX_VALUE, 1));
    assertEquals(Long.MAX_VALUE, ThriftSizes.addSize(Long.MAX_VALUE, Long.MAX_VALUE));
    assertEquals(12, ThriftSizes.addSize(5, 7));
    assertEquals(0, ThriftSizes.addSize(0, 0));
    assertEquals(-1, ThriftSizes.addSize(-1, 5));
    assertEquals(-1, ThriftSizes.addSize(5, -1));
  }

  @Test
  public void testStringListWidth() throws Exception {
    List<String> indexed = Arrays.asList("a", "", "méthodeÜnïcodé");
    List<String> sequential = new LinkedList<>(indexed);

    long expected = 0;

    for (String value : indexed) {
      expected += ThriftSizes.string(value);
    }

    assertEquals(expected, ThriftSizes.stringList(indexed));
    assertEquals(expected, ThriftSizes.stringList(sequential));
    assertEquals(expected, ThriftSizes.strings(new HashSet<>(indexed)));

    for (String value : indexed) {
      final String one = value;
      assertBounds(ThriftSizes.stringList(Collections.singletonList(one)), p -> p.writeString(one));
    }
  }

  @Test
  public void testBinaryListWidth() {
    List<byte[]> indexed = Arrays.asList(new byte[0], new byte[] {1, 2, 3}, new byte[300]);
    List<byte[]> sequential = new LinkedList<>(indexed);

    long expected = 0;

    for (byte[] value : indexed) {
      expected += ThriftSizes.binary(value);
    }

    assertEquals(expected, ThriftSizes.binaryList(indexed));
    assertEquals(expected, ThriftSizes.binaryList(sequential));
    assertEquals(expected, ThriftSizes.binaries(new HashSet<>(indexed)));
  }

  @Test
  public void testStructsWidth() {
    List<ThriftSerializable> known = Arrays.asList(new FixedSize(10), new FixedSize(0));

    assertEquals(10, ThriftSizes.structs(known));

    List<ThriftSerializable> unknown =
        Arrays.asList(new FixedSize(10), new UnknownSize(), new FixedSize(3));

    assertEquals(-1, ThriftSizes.structs(unknown));
    assertEquals(-1, new UnknownSize().serializedSize());
  }

  @Test
  public void testAddAdaptedStringFieldHook() {
    HookStringAdapter adapter = new HookStringAdapter(ThriftSizes.string("hey"));

    long size = ThriftSizes.addAdaptedStringField(10, "hey", adapter);

    assertEquals(
        ThriftSizes.addSize(ThriftSizes.addSize(10, ThriftSizes.FIELD), ThriftSizes.string("hey")),
        size);
    assertEquals(1, adapter.hookCalls);
    assertEquals(0, adapter.convertCalls);
  }

  @Test
  public void testAddAdaptedStringFieldFallback() {
    HookStringAdapter adapter = new HookStringAdapter(-1);

    long size = ThriftSizes.addAdaptedStringField(10, "hey", adapter);

    assertEquals(
        ThriftSizes.addSize(ThriftSizes.addSize(10, ThriftSizes.FIELD), ThriftSizes.string("hey")),
        size);
    assertEquals(1, adapter.hookCalls);
    assertEquals(1, adapter.convertCalls);
  }

  @Test
  public void testAddAdaptedStringFieldNullToValue() {
    HookStringAdapter adapter = new HookStringAdapter(-1);
    adapter.nullMapping = "yo";

    long size = ThriftSizes.addAdaptedStringField(10, null, adapter);

    assertEquals(
        ThriftSizes.addSize(ThriftSizes.addSize(10, ThriftSizes.FIELD), ThriftSizes.string("yo")),
        size);
    assertEquals(0, adapter.hookCalls);
    assertEquals(1, adapter.convertCalls);
  }

  @Test
  public void testAddAdaptedStringFieldNullToNull() {
    HookStringAdapter adapter = new HookStringAdapter(-1);

    long size = ThriftSizes.addAdaptedStringField(10, null, adapter);

    assertEquals(10, size);
    assertEquals(0, adapter.hookCalls);
    assertEquals(1, adapter.convertCalls);
  }

  @Test
  public void testAddAdaptedStringFieldSaturates() {
    HookStringAdapter adapter = new HookStringAdapter(Long.MAX_VALUE);

    long size = ThriftSizes.addAdaptedStringField(10, "hey", adapter);

    assertEquals(Long.MAX_VALUE, size);
    assertEquals(0, adapter.convertCalls);
  }

  @Test
  public void testAddAdaptedBinaryFieldHook() {
    HookBinaryAdapter adapter = new HookBinaryAdapter(12);

    long size = ThriftSizes.addAdaptedBinaryField(10, "hey", adapter);

    assertEquals(ThriftSizes.addSize(ThriftSizes.addSize(10, ThriftSizes.FIELD), 12), size);
    assertEquals(1, adapter.hookCalls);
    assertEquals(0, adapter.convertCalls);
  }

  @Test
  public void testAddAdaptedBinaryFieldFallback() {
    HookBinaryAdapter adapter = new HookBinaryAdapter(-1);

    long size = ThriftSizes.addAdaptedBinaryField(10, "hey", adapter);

    assertEquals(
        ThriftSizes.addSize(
            ThriftSizes.addSize(10, ThriftSizes.FIELD), ThriftSizes.binary(new byte[] {1, 2, 3})),
        size);
    assertEquals(1, adapter.convertCalls);
  }

  @Test
  public void testAddAdaptedBinaryFieldSaturates() {
    HookBinaryAdapter adapter = new HookBinaryAdapter(Long.MAX_VALUE);

    long size = ThriftSizes.addAdaptedBinaryField(10, "hey", adapter);

    assertEquals(Long.MAX_VALUE, size);
    assertEquals(0, adapter.convertCalls);
  }

  @Test
  public void testAddAdaptedStructFieldHook() {
    HookStructAdapter adapter = new HookStructAdapter(7);

    long size = ThriftSizes.addAdaptedStructField(10, "hey", adapter);

    assertEquals(ThriftSizes.addSize(ThriftSizes.addSize(10, ThriftSizes.FIELD), 7), size);
    assertEquals(1, adapter.hookCalls);
    assertEquals(0, adapter.convertCalls);
  }

  @Test
  public void testAddAdaptedStructFieldUnknownSize() {
    HookStructAdapter adapter = new HookStructAdapter(7);

    long size = ThriftSizes.addAdaptedStructField(-1, "hey", adapter);

    assertEquals(-1, size);
    assertEquals(0, adapter.hookCalls);
    assertEquals(0, adapter.convertCalls);
  }

  @Test
  public void testAddAdaptedStructFieldConvertedUnknown() {
    HookStructAdapter adapter = new HookStructAdapter(-1);
    adapter.structResult = new UnknownSize();

    long size = ThriftSizes.addAdaptedStructField(10, "hey", adapter);

    assertEquals(-1, size);
    assertEquals(1, adapter.convertCalls);
  }

  @Test
  public void testAddAdaptedStructFieldConvertedKnown() {
    HookStructAdapter adapter = new HookStructAdapter(-1);
    adapter.structResult = new FixedSize(7);

    long size = ThriftSizes.addAdaptedStructField(10, "hey", adapter);

    assertEquals(ThriftSizes.addSize(ThriftSizes.addSize(10, ThriftSizes.FIELD), 7), size);
    assertEquals(1, adapter.convertCalls);
  }

  @Test
  public void testAddAdaptedStructFieldNullToNull() {
    HookStructAdapter adapter = new HookStructAdapter(-1);

    long size = ThriftSizes.addAdaptedStructField(10, null, adapter);

    assertEquals(10, size);
    assertEquals(0, adapter.hookCalls);
    assertEquals(1, adapter.convertCalls);
  }

  @Test
  public void testAdaptedStringListHooked() {
    HookStringAdapter adapter = new HookStringAdapter(5);
    List<String> indexed = Arrays.asList("a", "bb");
    List<String> sequential = new LinkedList<>(indexed);

    assertEquals(10, ThriftSizes.adaptedStringList(indexed, adapter));
    assertEquals(10, ThriftSizes.adaptedStringList(sequential, adapter));
    assertEquals(0, ThriftSizes.adaptedStringList(Collections.emptyList(), adapter));
    assertEquals(4, adapter.hookCalls);
    assertEquals(0, adapter.convertCalls);
  }

  @Test
  public void testAdaptedStringListConverted() {
    HookStringAdapter adapter = new HookStringAdapter(-1);

    long size = ThriftSizes.adaptedStringList(Arrays.asList("a", "bb"), adapter);

    assertEquals(ThriftSizes.addSize(ThriftSizes.string("a"), ThriftSizes.string("bb")), size);
    assertEquals(2, adapter.convertCalls);
  }

  @Test
  public void testAdaptedStringListSaturates() {
    HookStringAdapter adapter = new HookStringAdapter(Long.MAX_VALUE);

    long size = ThriftSizes.adaptedStringList(Arrays.asList("a", "bb"), adapter);

    assertEquals(Long.MAX_VALUE, size);
  }

  @Test
  public void testAdaptedStrings() {
    HookStringAdapter adapter = new HookStringAdapter(5);

    assertEquals(10, ThriftSizes.adaptedStrings(new HashSet<>(Arrays.asList("a", "bb")), adapter));
    assertEquals(0, ThriftSizes.adaptedStrings(Collections.emptySet(), adapter));
    assertEquals(0, adapter.convertCalls);
  }

  @Test
  public void testAdaptedBinaryList() {
    HookBinaryAdapter adapter = new HookBinaryAdapter(9);

    assertEquals(18, ThriftSizes.adaptedBinaryList(Arrays.asList("a", "bb"), adapter));
    assertEquals(0, ThriftSizes.adaptedBinaryList(Collections.emptyList(), adapter));
    assertEquals(0, adapter.convertCalls);
  }

  @Test
  public void testAdaptedBinaries() {
    HookBinaryAdapter adapter = new HookBinaryAdapter(-1);

    long size = ThriftSizes.adaptedBinaries(Collections.singletonList("hey"), adapter);

    assertEquals(ThriftSizes.binary(new byte[] {1, 2, 3}), size);
    assertEquals(1, adapter.convertCalls);
  }

  @Test
  public void testAdaptedStructList() {
    HookStructAdapter adapter = new HookStructAdapter(-1);
    adapter.structResult = new FixedSize(7);

    assertEquals(14, ThriftSizes.adaptedStructList(Arrays.asList("a", "bb"), adapter));
    assertEquals(
        14, ThriftSizes.adaptedStructList(new LinkedList<>(Arrays.asList("a", "bb")), adapter));
    assertEquals(0, ThriftSizes.adaptedStructList(Collections.emptyList(), adapter));
    assertEquals(4, adapter.convertCalls);
  }

  @Test
  public void testAdaptedStructListUnknown() {
    HookStructAdapter adapter = new HookStructAdapter(-1);
    adapter.structResult = new UnknownSize();

    long size = ThriftSizes.adaptedStructList(Collections.singletonList("a"), adapter);

    assertEquals(-1, size);
  }

  @Test
  public void testAdaptedStructs() {
    HookStructAdapter adapter = new HookStructAdapter(7);

    assertEquals(14, ThriftSizes.adaptedStructs(Arrays.asList("a", "bb"), adapter));
    assertEquals(0, adapter.convertCalls);

    HookStructAdapter fallback = new HookStructAdapter(-1);
    fallback.structResult = new UnknownSize();

    assertEquals(-1, ThriftSizes.adaptedStructs(Collections.singletonList("a"), fallback));
  }

  private static final class HookStringAdapter implements TypeAdapter<String, String> {
    private final long hookResult;
    private int hookCalls;
    private int convertCalls;
    private String nullMapping;

    HookStringAdapter(long hookResult) {
      this.hookResult = hookResult;
    }

    @Override
    public String fromThrift(String value) {
      return value;
    }

    @Override
    public String toThrift(String value) {
      convertCalls++;

      return value == null ? nullMapping : value;
    }

    @Override
    public long serializedSize(String value) {
      hookCalls++;

      return hookResult;
    }
  }

  private static final class HookBinaryAdapter implements TypeAdapter<ByteBuf, String> {
    private final long hookResult;
    private int hookCalls;
    private int convertCalls;

    HookBinaryAdapter(long hookResult) {
      this.hookResult = hookResult;
    }

    @Override
    public String fromThrift(ByteBuf value) {
      return value.toString(StandardCharsets.UTF_8);
    }

    @Override
    public ByteBuf toThrift(String value) {
      convertCalls++;

      return value == null ? null : Unpooled.wrappedBuffer(new byte[] {1, 2, 3});
    }

    @Override
    public long serializedSize(String value) {
      hookCalls++;

      return hookResult;
    }
  }

  private static final class HookStructAdapter implements TypeAdapter<ThriftSerializable, String> {
    private final long hookResult;
    private int hookCalls;
    private int convertCalls;
    private ThriftSerializable structResult;

    HookStructAdapter(long hookResult) {
      this.hookResult = hookResult;
    }

    @Override
    public String fromThrift(ThriftSerializable value) {
      return "";
    }

    @Override
    public ThriftSerializable toThrift(String value) {
      convertCalls++;

      return structResult;
    }

    @Override
    public long serializedSize(String value) {
      hookCalls++;

      return hookResult;
    }
  }

  private static final class FixedSize implements ThriftSerializable {
    private final long size;

    FixedSize(long size) {
      this.size = size;
    }

    @Override
    public void write0(TProtocol protocol) {}

    @Override
    public long serializedSize() {
      return size;
    }
  }

  private static final class UnknownSize implements ThriftSerializable {
    @Override
    public void write0(TProtocol protocol) {}
  }
}
