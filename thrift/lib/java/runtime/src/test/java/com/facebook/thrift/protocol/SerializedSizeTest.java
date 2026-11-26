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

package com.facebook.thrift.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.facebook.thrift.adapter.test.BooleanToStringTypeAdapter;
import com.facebook.thrift.adapter.test.CountingHookStringAdapter;
import com.facebook.thrift.adapter.test.CountingPooledByteBufTypeAdapter;
import com.facebook.thrift.adapter.test.StringToByteBufTypeAdapter;
import com.facebook.thrift.adapter.test.Wrapped;
import com.facebook.thrift.any.Any;
import com.facebook.thrift.example.ping.CustomException;
import com.facebook.thrift.payload.ThriftSerializable;
import com.facebook.thrift.test.EveryLayout;
import com.facebook.thrift.test.JustABinary;
import com.facebook.thrift.test.ManyFixedFields;
import com.facebook.thrift.test.MutualSizeA;
import com.facebook.thrift.test.MutualSizeB;
import com.facebook.thrift.test.Nested1;
import com.facebook.thrift.test.Nested2;
import com.facebook.thrift.test.NestedLeaf;
import com.facebook.thrift.test.NestedMid;
import com.facebook.thrift.test.NestedMidList;
import com.facebook.thrift.test.NestedMidMap;
import com.facebook.thrift.test.NullMappingAdaptedStruct;
import com.facebook.thrift.test.OptionalBoundHolder;
import com.facebook.thrift.test.OptionalBoundLeaf;
import com.facebook.thrift.test.Pet;
import com.facebook.thrift.test.RecursiveSizeStruct;
import com.facebook.thrift.test.TestUnion;
import com.facebook.thrift.test.adapter.AdaptedTestStruct;
import com.facebook.thrift.test.adapter.AdaptedTestUnion;
import com.facebook.thrift.test.adapter.AdapterHookTestStruct;
import com.facebook.thrift.test.adapter.DoubleAdaptedProbe;
import com.facebook.thrift.test.adapter.InnerException;
import com.facebook.thrift.test.adapter.InnerUnion;
import com.facebook.thrift.test.adapter.MaxBoundHolder;
import com.facebook.thrift.test.adapter.MaxBoundParent;
import com.facebook.thrift.test.adapter.NullToIntProbe;
import com.facebook.thrift.test.adapter.TestEnum;
import com.facebook.thrift.test.cross.CrossLeaf;
import com.facebook.thrift.test.terse.AdaptedTerseStruct;
import com.facebook.thrift.test.terse.EmptyStruct;
import com.facebook.thrift.test.terse.FieldLevelTerseStruct;
import com.facebook.thrift.test.terse.InnerTerseStruct;
import com.facebook.thrift.test.terse.MyEnum;
import com.facebook.thrift.test.terse.MyException;
import com.facebook.thrift.test.terse.MyStruct;
import com.facebook.thrift.test.terse.MyUnion;
import com.facebook.thrift.test.terse.SingleFieldStruct;
import com.facebook.thrift.test.terse.StructLevelTerseStruct;
import com.facebook.thrift.test.terse.Structv2;
import com.facebook.thrift.test.terse.TerseException;
import com.facebook.thrift.test.terse.TerseStructWithDateAdapter;
import com.facebook.thrift.test.thrift.any.SolidColor;
import com.facebook.thrift.test.wrapper.WrappedTestStruct;
import com.facebook.thrift.util.ThriftSizes;
import com.facebook.thrift.wrapper.test.PoliciedField;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TStruct;
import org.junit.jupiter.api.Test;

/**
 * Generated {@code serializedSize} is an upper bound on both protocols: {@code size >= written} for
 * every layout, including non-ASCII strings, terse rollbacks, adapters and wrappers.
 */
public class SerializedSizeTest {
  private static final byte[] binary;

  static {
    binary = new byte[1024];
    ThreadLocalRandom.current().nextBytes(binary);
  }

  private interface WriteOp {
    void write(ByteBufTProtocol protocol) throws Exception;
  }

  /**
   * Sums ThriftSizes widths per write call instead of writing bytes. Feeding the same value through
   * this and through the generated size must give the same total: any field the size walk skips but
   * write0 emits (or vice versa) shows up as a mismatch.
   */
  static final class SizingProtocol extends ByteBufTProtocol {
    private long total;
    private final java.util.ArrayDeque<Long> marks = new java.util.ArrayDeque<>();

    long total() {
      return total;
    }

    @Override
    public void writeMessageBegin(TMessage message) {}

    @Override
    public void writeMessageEnd() {}

    @Override
    public void writeStructBegin(TStruct struct) {}

    @Override
    public void writeStructEnd() {}

    @Override
    public void writeFieldBegin(TField field) {
      total += ThriftSizes.FIELD;
    }

    @Override
    public void writeFieldEnd() {}

    @Override
    public void writeFieldStop() {
      total += ThriftSizes.FIELD_STOP;
    }

    @Override
    public void writeMapBegin(TMap map) {
      total += ThriftSizes.MAP;
    }

    @Override
    public void writeMapEnd() {}

    @Override
    public void writeListBegin(TList list) {
      total += ThriftSizes.LIST;
    }

    @Override
    public void writeListEnd() {}

    @Override
    public void writeSetBegin(TSet set) {
      total += ThriftSizes.SET;
    }

    @Override
    public void writeSetEnd() {}

    @Override
    public void writeBool(boolean b) {
      total += ThriftSizes.BOOL;
    }

    @Override
    public void writeByte(byte b) {
      total += ThriftSizes.BYTE;
    }

    @Override
    public void writeI16(short i16) {
      total += ThriftSizes.I16;
    }

    @Override
    public void writeI32(int i32) {
      total += ThriftSizes.I32;
    }

    @Override
    public void writeI64(long i64) {
      total += ThriftSizes.I64;
    }

    @Override
    public void writeFloat(float value) {
      total += ThriftSizes.FLOAT;
    }

    @Override
    public void writeDouble(double value) {
      total += ThriftSizes.DOUBLE;
    }

    @Override
    public void writeString(String str) {
      total += ThriftSizes.string(str);
    }

    @Override
    public void writeBinary(ByteBuffer buf) {
      total += ThriftSizes.binary(buf);
    }

    @Override
    public void writeBinaryAsByteBuf(ByteBuf buf) {
      total += ThriftSizes.binary(buf);
    }

    @Override
    public ByteBuf getWritableBinaryAsByteBuf(int size) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int mark() {
      // Reports the running total as the position, mirroring `writerIndex`: `write0` subtracts
      // two marks to measure the bytes between them, so this must return a count, not an id.
      marks.push(total);
      return (int) Math.min(total, Integer.MAX_VALUE);
    }

    @Override
    public void rollback(int id) {
      // Drops every mark after the named one, then the named one itself: everything pushed
      // between holds a strictly larger total (even a field header advances the count).
      while (!marks.isEmpty() && marks.peek() > id) {
        marks.pop();
      }

      if (!marks.isEmpty()) {
        marks.pop();
      }

      total = id;
    }

    @Override
    public TMessage readMessageBegin() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void readMessageEnd() {
      throw new UnsupportedOperationException();
    }

    @Override
    public TStruct readStructBegin() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void readStructEnd() {
      throw new UnsupportedOperationException();
    }

    @Override
    public TField readFieldBegin() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void readFieldEnd() {
      throw new UnsupportedOperationException();
    }

    @Override
    public TMap readMapBegin() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void readMapEnd() {
      throw new UnsupportedOperationException();
    }

    @Override
    public TList readListBegin() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void readListEnd() {
      throw new UnsupportedOperationException();
    }

    @Override
    public TSet readSetBegin() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void readSetEnd() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean readBool() {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte readByte() {
      throw new UnsupportedOperationException();
    }

    @Override
    public short readI16() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int readI32() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long readI64() {
      throw new UnsupportedOperationException();
    }

    @Override
    public float readFloat() {
      throw new UnsupportedOperationException();
    }

    @Override
    public double readDouble() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String readString() {
      throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer readBinary() {
      throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf readBinaryAsSlice() {
      throw new UnsupportedOperationException();
    }
  }

  private static int writtenBytes(boolean compact, WriteOp op) throws Exception {
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

  private static void assertSize(String name, Supplier<ThriftSerializable> fresh) throws Exception {
    // Fresh instances per pass: `writeBinary` consumes `ByteBuf` fields, so a second `write0`
    // on the same value would under-write.
    long size = fresh.get().serializedSize();
    SizingProtocol sizing = new SizingProtocol();
    fresh.get().write0(sizing);

    assertEquals(sizing.total(), size, () -> name + " sizing mismatch: walk=" + sizing.total());

    int binaryWritten = writtenBytes(false, fresh.get()::write0);
    int compactWritten = writtenBytes(true, fresh.get()::write0);

    assertTrue(
        size >= binaryWritten,
        () -> name + " binary undercount: size=" + size + " written=" + binaryWritten);
    assertTrue(
        size >= compactWritten,
        () -> name + " compact undercount: size=" + size + " written=" + compactWritten);
  }

  /**
   * Sizes a value whose size may over-count: rolled-back terse fields, or terse adapted fields
   * whose stored pre-image is present while the converted value is default-skipped. The generated
   * size must still bound the sizing walk, which itself bounds both protocols.
   */
  private static void assertSizeAllowingOvercount(String name, Supplier<ThriftSerializable> fresh)
      throws Exception {
    long size = fresh.get().serializedSize();
    SizingProtocol sizing = new SizingProtocol();
    fresh.get().write0(sizing);

    assertTrue(
        size >= sizing.total(),
        () -> name + " rollback undercount: size=" + size + " walk=" + sizing.total());

    int binaryWritten = writtenBytes(false, fresh.get()::write0);
    int compactWritten = writtenBytes(true, fresh.get()::write0);

    assertTrue(
        sizing.total() >= binaryWritten,
        () -> name + " binary walk undercount: walk=" + sizing.total());
    assertTrue(
        sizing.total() >= compactWritten,
        () -> name + " compact walk undercount: walk=" + sizing.total());
  }

  private static EveryLayout fullLayout() {
    return new EveryLayout.Builder()
        .setABool(true)
        .setADouble(Double.MAX_VALUE)
        .setAFloat(Float.MAX_VALUE)
        .setAByte((byte) 1)
        .setAInt(Integer.MAX_VALUE)
        .setAShort(Short.MAX_VALUE)
        .setALong(Long.MAX_VALUE)
        .setAList(Lists.newArrayList("1", "2", "3"))
        .setAMap(ImmutableMap.of(1, "value"))
        .setASet(ImmutableSet.of("1", "2", "3"))
        .setAListOfLists(
            Lists.newArrayList(
                Lists.newArrayList("1", "2", "3"),
                Lists.newArrayList("1", "2", "3"),
                Lists.newArrayList("1", "2", "3")))
        .setASetOfSets(
            ImmutableSet.of(
                ImmutableSet.of("1", "2", "3"),
                ImmutableSet.of("4", "5", "6"),
                ImmutableSet.of("7", "8", "9")))
        .setAMapOfLists(ImmutableMap.of(Lists.newArrayList(1, 2, 3), Lists.newArrayList(1, 2, 3)))
        .setAMapOfSets(ImmutableMap.of(ImmutableSet.of(1, 2, 3), 42))
        .setABinary(binary)
        .build();
  }

  @Test
  public void testEveryLayout() throws Exception {
    assertSize("every-layout", () -> fullLayout());

    assertSize("every-layout-empty", () -> new EveryLayout.Builder().build());

    assertSize(
        "every-layout-unicode",
        () ->
            new EveryLayout.Builder()
                .setAString("héllo 日本語 emoji \uD83D\uDE00")
                .setAList(Lists.newArrayList("plain", "lone lead \uD800 here", "trail \uDC00"))
                .setAMap(ImmutableMap.of(1, "clé \uD800"))
                .build());

    assertSize("just-binary", () -> new JustABinary.Builder().setABinary(binary).build());

    assertSize(
        "just-binary-empty", () -> new JustABinary.Builder().setABinary(new byte[0]).build());

    assertSize(
        "map-of-sets",
        () ->
            new EveryLayout.Builder()
                .setAMapOfSets(ImmutableMap.of(ImmutableSet.of(), 1, ImmutableSet.of(1, 2), 2))
                .build());
  }

  @Test
  public void testNested() throws Exception {
    Pet pet = new Pet.Builder().setName("fido").setAge(3).build();

    assertSize("pet", () -> pet);
    assertSize(
        "nested1", () -> new Nested1.Builder().setPets(Lists.newArrayList(pet, pet)).build());
    assertSize(
        "nested2",
        () ->
            new Nested2.Builder()
                .setNests(
                    ImmutableMap.of(
                        1, new Nested1.Builder().setPets(Lists.newArrayList(pet)).build()))
                .build());
  }

  @Test
  public void testOptionalBoundStruct() throws Exception {
    OptionalBoundLeaf leaf = new OptionalBoundLeaf.Builder().setV(7).build();

    assertSize(
        "optional-bound-unset",
        () -> new OptionalBoundHolder.Builder().setA(1).setPlain(leaf).build());

    assertSize(
        "optional-bound-set",
        () -> new OptionalBoundHolder.Builder().setA(1).setLeaf(leaf).setPlain(leaf).build());

    assertSize(
        "optional-bound-alias-unset",
        () -> new OptionalBoundHolder.Builder().setA(1).setPlain(leaf).build());

    assertSize(
        "optional-bound-alias-set",
        () -> new OptionalBoundHolder.Builder().setA(1).setPlain(leaf).setAliasLeaf(leaf).build());

    CrossLeaf cross = new CrossLeaf.Builder().setV(9).build();

    assertSize(
        "optional-bound-cross-unset",
        () -> new OptionalBoundHolder.Builder().setA(1).setPlain(leaf).build());

    assertSize(
        "optional-bound-cross-set",
        () -> new OptionalBoundHolder.Builder().setA(1).setPlain(leaf).setCrossLeaf(cross).build());
  }

  @Test
  public void testUnions() throws Exception {
    Pet pet = new Pet.Builder().setName("fido").build();

    assertSize("union-pet", () -> TestUnion.fromThePet(pet));
    assertSize("union-other-pet", () -> TestUnion.fromTheOtherPet(pet));
    assertSize("union-layout", () -> TestUnion.fromAllThings(fullLayout()));
    assertSize(
        "union-binary",
        () -> TestUnion.fromABinary(new JustABinary.Builder().setABinary(binary).build()));
    assertSize("union-empty", () -> new TestUnion());
    assertSize("my-union-1", () -> MyUnion.fromIntField1(42));
    assertSize("my-union-2", () -> MyUnion.fromIntField2(-7));
  }

  @Test
  public void testTerseRollback() throws Exception {
    Structv2 empty =
        new Structv2.Builder()
            .setStringField("")
            .setInnerField(new SingleFieldStruct.Builder().build())
            .build();
    assertSizeAllowingOvercount("terse-rollback-empty", () -> empty);

    Structv2 value =
        new Structv2.Builder()
            .setStringField("x")
            .setInnerField(new SingleFieldStruct.Builder().build())
            .build();
    assertSizeAllowingOvercount("terse-rollback", () -> value);
  }

  @Test
  public void testTerseStructs() throws Exception {
    MyStruct myStruct =
        new MyStruct.Builder()
            .setIntField(5)
            .setBoolField(true)
            .setStructField(new SingleFieldStruct.Builder().setIntTerseField(9).build())
            .build();

    assertSize("terse-my-struct", () -> myStruct);

    FieldLevelTerseStruct fieldLevel =
        new FieldLevelTerseStruct.Builder()
            .setTerseBoolField(true)
            .setTerseByteField((byte) 2)
            .setTerseShortField((short) 3)
            .setTerseIntField(4)
            .setTerseLongField(5L)
            .setTerseFloatField(6.f)
            .setTerseDoubleField(7.)
            .setTerseStringField("hello")
            .setTerseBinaryField(new byte[] {1, 2, 3})
            .setTerseEnumField(MyEnum.ME1)
            .setTerseListField(Lists.newArrayList((short) 1, (short) 2))
            .setTerseSetField(ImmutableSet.of((short) 1))
            .setTerseMapField(ImmutableMap.of((short) 1, (short) 2))
            .setTerseStructField(myStruct)
            .setByteField((byte) 8)
            .setShortField((short) 9)
            .setIntField(10)
            .setFloatField(11.f)
            .setDoubleField(12.)
            .setStringField("world")
            .setEnumField(MyEnum.ME0)
            .setSetField(ImmutableSet.of((short) 3))
            .setMapField(ImmutableMap.of((short) 3, (short) 4))
            .setStructField(myStruct)
            .setEmptyField(new EmptyStruct.Builder().build())
            .setSingleField(new SingleFieldStruct.Builder().setIntTerseField(9).build())
            .setUnionField(MyUnion.fromIntField3(11))
            .build();

    assertSize("terse-field-level", () -> fieldLevel);

    StructLevelTerseStruct structLevel =
        new StructLevelTerseStruct.Builder()
            .setBoolField(true)
            .setByteField((byte) 1)
            .setShortField((short) 2)
            .setIntField(3)
            .setLongField(4L)
            .setFloatField(5.f)
            .setDoubleField(6.)
            .setStringField("s")
            .setBinaryField(new byte[] {9})
            .setEnumField(MyEnum.ME1)
            .setListField(Lists.newArrayList((short) 7))
            .setSetField(ImmutableSet.of((short) 8))
            .setMapField(ImmutableMap.of((short) 9, (short) 10))
            .setStructField(myStruct)
            .setInnerField(
                new InnerTerseStruct.Builder().setBoolField(true).setByteField((byte) 2).build())
            .setUnionField(MyUnion.fromIntField1(1))
            .setExceptionField(
                new TerseException.Builder().setMsg("m").setCode(2).setReason("r").build())
            .setAdaptedByteField("65")
            .build();

    assertSize("terse-struct-level", () -> structLevel);
  }

  @Test
  public void testAdaptedFixedNeverConverts() throws Exception {
    AdaptedTerseStruct value =
        new AdaptedTerseStruct.Builder().setAdaptedBooleanField("true").build();
    BooleanToStringTypeAdapter.toThriftCalls = 0;

    value.serializedSize();

    assertEquals(0, BooleanToStringTypeAdapter.toThriftCalls);
    assertSizeAllowingOvercount("adapted-terse-minimal", () -> value);

    AdaptedTerseStruct withOptional =
        new AdaptedTerseStruct.Builder()
            .setAdaptedBooleanField("true")
            .setOptionalAdaptedBooleanField("false")
            .build();
    BooleanToStringTypeAdapter.toThriftCalls = 0;

    withOptional.serializedSize();

    assertEquals(0, BooleanToStringTypeAdapter.toThriftCalls);
    assertSizeAllowingOvercount("adapted-terse-optional", () -> withOptional);
  }

  @Test
  public void testAdaptedVariableConvertsOncePerPass() throws Exception {
    Supplier<AdaptedTestStruct> fresh =
        () -> new AdaptedTestStruct.Builder().setAdaptedStringField(utf8Buffer("test")).build();

    StringToByteBufTypeAdapter.toThriftCalls = 0;

    long size = fresh.get().serializedSize();
    long perPass = StringToByteBufTypeAdapter.toThriftCalls;

    // AdaptedTestStruct carries four StringToByteBuf fields (the adapted string
    // field and its default, plus the size/hookSize collision fields): each
    // converts once per pass, set or not.
    assertEquals(4, perPass);

    int binaryWritten = writtenBytes(false, fresh.get()::write0);

    assertEquals(2 * perPass, StringToByteBufTypeAdapter.toThriftCalls);

    int compactWritten = writtenBytes(true, fresh.get()::write0);

    assertEquals(3 * perPass, StringToByteBufTypeAdapter.toThriftCalls);
    assertTrue(size >= binaryWritten);
    assertTrue(size >= compactWritten);
  }

  @Test
  public void testAdaptedUnionCollisionMembers() throws Exception {
    assertSize(
        "union-size-member",
        () -> AdaptedTestUnion.fromSize(Unpooled.wrappedBuffer(new byte[] {1, 2, 3})));

    assertSize(
        "union-hook-size-member",
        () -> AdaptedTestUnion.fromHookSize(Unpooled.wrappedBuffer(new byte[] {4, 5})));
  }

  @Test
  public void testDoubleAdaptedElementsUseHook() throws Exception {
    Supplier<ThriftSerializable> fresh =
        () ->
            new DoubleAdaptedProbe.Builder()
                .setValues(
                    Arrays.asList(
                        Unpooled.wrappedBuffer(new byte[] {1, 2, 3}),
                        Unpooled.wrappedBuffer(new byte[] {4, 5})))
                .build();

    CountingPooledByteBufTypeAdapter.toThriftCalls = 0;
    CountingPooledByteBufTypeAdapter.hookCalls = 0;

    long size = fresh.get().serializedSize();

    // The outer hook answers -1, so sizing falls back; the fallback must size
    // elements through the element hook instead of converting them.
    assertEquals(0, CountingPooledByteBufTypeAdapter.toThriftCalls);
    assertEquals(2, CountingPooledByteBufTypeAdapter.hookCalls);
    assertTrue(size > 0);

    assertSize("double-adapted-probe", fresh);
  }

  @Test
  public void testSaturatedSumsStayMax() throws Exception {
    MaxBoundHolder holder = new MaxBoundHolder.Builder().setFirst("a").setSecond("b").build();
    Supplier<ThriftSerializable> fresh =
        () ->
            new MaxBoundParent.Builder()
                .setLeft(holder)
                .setRight(holder)
                .setTrailing("tail")
                .build();

    long size = fresh.get().serializedSize();

    assertEquals(Long.MAX_VALUE, size);
    assertSizeAllowingOvercount("saturated-parent", fresh);
  }

  @Test
  public void testNullStoredAdapterCountsConvertedValue() throws Exception {
    // The full struct carries a pre-existing 123-byte hook overcount, so it
    // bounds but does not equal the walk. The probe struct proves the
    // null-stored path itself is exact.
    assertSizeAllowingOvercount("null-to-int-unset", () -> new AdaptedTestStruct.Builder().build());

    assertSizeAllowingOvercount(
        "null-to-int-set", () -> new AdaptedTestStruct.Builder().setNullToIntField(7).build());

    assertSize("null-to-int-probe-unset", () -> new NullToIntProbe.Builder().build());

    assertSize(
        "null-to-int-probe-set", () -> new NullToIntProbe.Builder().setNullToIntField(7).build());
  }

  @Test
  public void testAdapterHookSizesWithoutConverting() throws Exception {
    Supplier<ThriftSerializable> fresh =
        () ->
            new AdapterHookTestStruct.Builder()
                .setPlainField("plain")
                .setHookedField("hooked héllo")
                .setOptHookedField("opt")
                .build();

    CountingHookStringAdapter.hookCalls = 0;
    CountingHookStringAdapter.convertCalls = 0;

    long size = fresh.get().serializedSize();

    assertEquals(2, CountingHookStringAdapter.hookCalls);
    assertEquals(0, CountingHookStringAdapter.convertCalls);

    int binaryWritten = writtenBytes(false, fresh.get()::write0);

    assertEquals(2, CountingHookStringAdapter.convertCalls);

    int compactWritten = writtenBytes(true, fresh.get()::write0);

    assertEquals(4, CountingHookStringAdapter.convertCalls);
    assertEquals(2, CountingHookStringAdapter.hookCalls);
    assertTrue(size >= binaryWritten);
    assertTrue(size >= compactWritten);

    assertSize("adapter-hook", fresh);
  }

  @Test
  public void testAdapted() throws Exception {
    assertSize(
        "terse-date-adapter",
        () ->
            new TerseStructWithDateAdapter.Builder()
                .setStringField("s")
                .setDateField(new Date(1234567890L))
                .build());
    assertSizeAllowingOvercount(
        "adapted-terse-struct",
        () ->
            new AdaptedTerseStruct.Builder()
                .setAdaptedBooleanField("true")
                .setAdaptedShortField("7")
                .setAdaptedIntField("42")
                .setAdaptedLongField("8")
                .setDoubleAdaptedIntField(43L)
                .setDoubleTypedefAdaptedIntField("44")
                .build());
  }

  @Test
  public void testNullMappingAdapterCountsUnconditionally() throws Exception {
    Supplier<ThriftSerializable> fresh = () -> new NullMappingAdaptedStruct.Builder().build();

    assertEquals(
        ThriftSizes.FIELD_STOP + ThriftSizes.FIELD + ThriftSizes.I32, fresh.get().serializedSize());
    assertSize("null-mapping", fresh);
  }

  @Test
  public void testStructNamedThriftSizes() throws Exception {
    assertSize(
        "thrift-sizes-struct",
        () -> new com.facebook.thrift.test.ThriftSizes.Builder().setValue(7).build());
  }

  @Test
  public void testManyFixedFieldsFoldCompilesAndCounts() throws Exception {
    Supplier<ThriftSerializable> fresh = () -> new ManyFixedFields.Builder().build();

    assertEquals(
        ThriftSizes.FIELD_STOP + 900 * (ThriftSizes.FIELD + ThriftSizes.I32),
        fresh.get().serializedSize());
    assertSize("many-fixed", fresh);
  }

  private static NestedMid nestedMid(int seed) {
    return new NestedMid.Builder()
        .setX(seed)
        .setLeaf(new NestedLeaf.Builder().setX(seed).build())
        .build();
  }

  @Test
  public void testNestedBoundStructContainers() throws Exception {
    Supplier<ThriftSerializable> fresh = () -> nestedMid(7);

    assertEquals(
        ThriftSizes.FIELD_STOP
            + (ThriftSizes.FIELD + ThriftSizes.I64)
            + (ThriftSizes.FIELD + NestedLeaf.serializedSizeBound()),
        fresh.get().serializedSize());
    assertSize("nested-mid", fresh);
    assertSize(
        "nested-mid-list",
        () ->
            new NestedMidList.Builder().setItems(Collections.nCopies(1000, nestedMid(7))).build());

    ImmutableMap.Builder<Integer, NestedMid> map = ImmutableMap.builder();

    for (int i = 0; i < 1000; i++) {
      map.put(i, nestedMid(i));
    }

    ImmutableMap<Integer, NestedMid> items = map.build();
    assertSize("nested-mid-map", () -> new NestedMidMap.Builder().setItems(items).build());
  }

  @Test
  public void testRecursiveStructsHaveNoFixedBound() throws Exception {
    assertSize("recursive-leaf", () -> new RecursiveSizeStruct.Builder().setX(1).build());
    assertSize(
        "recursive-nested",
        () ->
            new RecursiveSizeStruct.Builder()
                .setX(1)
                .setNext(new RecursiveSizeStruct.Builder().setX(2).build())
                .build());
    assertSize(
        "mutual-pair",
        () ->
            new MutualSizeA.Builder()
                .setX(1)
                .setB(new MutualSizeB.Builder().setY(2).build())
                .build());
  }

  private static ByteBuf slicedBuffer() {
    ByteBuf buf = Unpooled.wrappedBuffer(new byte[] {0, 1, 2, 3, 4});
    buf.skipBytes(2);
    return buf;
  }

  private static ByteBuf utf8Buffer(String text) {
    return Unpooled.wrappedBuffer(text.getBytes());
  }

  /**
   * Triple-nested `GenericTypeAdapter` unwraps one `Wrapped` level per adapter, so at runtime each
   * level's value must be a real `List`. That alternating shape is statically inexpressible, hence
   * the raw construction.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static Wrapped quadAdaptedValue() {
    return new Wrapped(Arrays.asList(new Wrapped(Arrays.asList(new Wrapped(Arrays.asList("5"))))));
  }

  @Test
  public void testAdaptedTestStruct() throws Exception {
    assertSizeAllowingOvercount(
        "adapted-test-struct",
        () ->
            new AdaptedTestStruct.Builder()
                .setAdaptedBooleanField("true")
                .setAdaptedByteField("9")
                .setAdaptedShortField("101")
                .setAdaptedIntField("1024")
                .setAdaptedLongField("5000")
                .setAdaptedFloatField("2.3")
                .setAdaptedDoubleField("5.67")
                .setAdaptedStringField(utf8Buffer("test"))
                .setTestEnumField(TestEnum.ONE)
                .setB1(slicedBuffer())
                .setB2(utf8Buffer("b2"))
                .setB3(utf8Buffer("b3"))
                .setDateField(new Date(1234567890L))
                .setAdaptedIntListField("1,2")
                .setAdaptedBinaryListField("aa:bb")
                .setAdaptedListIntListField("1,2:3")
                .setAdaptedIntSetField("10,20")
                .setAdaptedBinarySetField("aa,bb")
                .setAdaptedIntMapField("1=7")
                .setAdaptedIntBinaryMapField("")
                .setAdaptedIntStringMapField("1=foo")
                .setAdaptedIntBinaryStringMapField("")
                .setAdaptedIntBinaryListMapField("")
                .setAdaptedBooleanField2("false")
                .setAdaptedBinaryListField2("")
                .setDoubleAdaptedIntField(3001L)
                .setDoubleTypedefAdaptedIntField("50")
                .setMultipleTypedefAdaptedIntField("51")
                .setGenericAdapterField(new Wrapped<>(2.5))
                .setListAdaptedIntField(Arrays.asList("5", "6"))
                .setListOfListAdaptedIntField(Arrays.asList(Arrays.asList("5")))
                .setNestedListAdaptedIntField(Arrays.asList(Arrays.asList(Arrays.asList("5"))))
                .setSetAdaptedIntField(ImmutableSet.of("7"))
                .setListOfSetAdaptedIntField(Arrays.asList(ImmutableSet.of("8")))
                .setMapOfIntToShortField(ImmutableMap.of("8", "9"))
                .setMapOfIntToMapIntToShortField(ImmutableMap.of("8", ImmutableMap.of("8", "9")))
                .setNestedMapIntToShortField(
                    ImmutableMap.of("8", ImmutableMap.of("8", ImmutableMap.of("8", "9"))))
                .setNestedAdaptedField(
                    ImmutableMap.of("8", ImmutableMap.of("9", Arrays.asList("10"))))
                .setAdaptedBooleanListField(Arrays.asList("true", "false"))
                .setAdaptedByteListField(Arrays.asList("9"))
                .setAdaptedShortListField(Arrays.asList("10"))
                .setAdaptedLongListField(Arrays.asList("11"))
                .setAdaptedFloatListField(Arrays.asList("2.5"))
                .setAdaptedDoubleListField(Arrays.asList("3.5"))
                .setAdaptedStringListField(Arrays.asList(utf8Buffer("a")))
                .setTestEnumListField(Arrays.asList(TestEnum.ONE))
                .setAdaptedBinListField(Arrays.asList(utf8Buffer("b")))
                .setAdaptedBinList2Field(Arrays.asList(Arrays.asList(utf8Buffer("c"))))
                .setAdaptedBinList3Field(
                    Arrays.asList(Arrays.asList(Arrays.asList(utf8Buffer("d")))))
                .setAdaptedBinSetField(ImmutableSet.of(utf8Buffer("e")))
                .setAdaptedBinMapField(ImmutableMap.of(1, utf8Buffer("f")))
                .setAdaptedBinMap2Field(ImmutableMap.of(1, ImmutableMap.of(2, utf8Buffer("g"))))
                .setAdaptedBinMap3Field(
                    ImmutableMap.of(
                        1, ImmutableMap.of(2, ImmutableMap.of(utf8Buffer("h"), utf8Buffer("i")))))
                .setAdaptedBinStringList2Field(Arrays.asList(Arrays.asList(utf8Buffer("j"))))
                .setAdaptedBinStringList3Field(
                    Arrays.asList(Arrays.asList(Arrays.asList(utf8Buffer("k")))))
                .setAdaptedBinNestedTypeDefListField(Arrays.asList(utf8Buffer("l")))
                .setAdaptedSetListField(Arrays.asList(""))
                .setAdaptedSetList2Field(Arrays.asList(Arrays.asList("")))
                .setAdaptedSetList3Field(Arrays.asList(Arrays.asList(Arrays.asList(""))))
                .setAdaptedMap7Field(ImmutableMap.of(1, ""))
                .setAdaptedMap8Field(ImmutableMap.of(1, ImmutableMap.of(2, "3,4")))
                .setAdaptedMap9Field(
                    ImmutableMap.of(1, ImmutableMap.of(2, ImmutableMap.of(3, "4=5"))))
                .setDoubleAdaptedListField(new Wrapped<>(Arrays.asList("5")))
                .setDoubleAdaptedList2Field(new Wrapped<>(Arrays.asList("")))
                .setDoubleAdaptedList3Field(new Wrapped<>(Arrays.asList(utf8Buffer("m"))))
                .setDoubleAdaptedMap1Field(new Wrapped<>(ImmutableMap.of("8", utf8Buffer("n"))))
                .setDoubleAdaptedIntList2Field(new Wrapped<>(new Wrapped<>(Arrays.asList("5"))))
                .setAnyListField(Collections.emptyList())
                .setAnyMapField(Collections.emptyMap())
                .setUnionListField(Arrays.asList(new Wrapped<>(InnerUnion.fromIntField(1))))
                .setExceptionListField(
                    Arrays.asList(
                        new Wrapped<>(new InnerException.Builder().setIntField(1).build())))
                .setTerseAdaptedLongListField(Arrays.asList("11"))
                .setTerseAdaptedIntSetListField(Arrays.asList(""))
                .setQuadAdaptedField(quadAdaptedValue())
                .setQuadAdaptedMapField(
                    ImmutableMap.of("8", ImmutableMap.of("9", quadAdaptedValue())))
                .build());
  }

  @Test
  public void testAdaptedTestUnion() throws Exception {
    assertSize("adapted-union-bool", () -> AdaptedTestUnion.fromAdaptedBooleanField("true"));
    assertSize("adapted-union-int", () -> AdaptedTestUnion.fromAdaptedIntField("42"));
    assertSize("adapted-union-list", () -> AdaptedTestUnion.fromAdaptedIntListField("1,2"));
    assertSize(
        "adapted-union-lists",
        () -> AdaptedTestUnion.fromListAdaptedIntField(Arrays.asList("5", "6")));
  }

  /** Fresh buffers per call: `write0` releases `ByteBuf` fields, so each struct needs its own. */
  private static WrappedTestStruct wrappedTestStruct(List<Any> anyList) {
    return new WrappedTestStruct.Builder()
        .setContext(1)
        .setWrappedBooleanField(new PoliciedField<>(true))
        .setWrappedIntField(new PoliciedField<>(20))
        .setWrappedByteBufField(new PoliciedField<>(slicedBuffer()))
        .setWrappedByteBufField2(new PoliciedField<>(utf8Buffer("w")))
        .setWrappedDoubleAdaptedIntField(new PoliciedField<>(42L))
        .setWrappedSingleAdaptedIntField(new PoliciedField<>("43"))
        .setListAdaptedIntField(new PoliciedField<>(Arrays.asList("5")))
        .setNestedAdaptedField(
            new PoliciedField<>(ImmutableMap.of("8", ImmutableMap.of("9", Arrays.asList("10")))))
        .setAnyListField(new PoliciedField<>(anyList))
        .setWrappedDoubleAdaptedIntListField(new PoliciedField<>(new Wrapped<>(Arrays.asList("5"))))
        .build();
  }

  /** Fresh values per call: an `Any` builds its `AnyStruct` once, and `write0` consumes it. */
  private static Any freshAny() {
    return new Any.Builder<>(new SolidColor.Builder().setColor(2).build()).build();
  }

  @Test
  public void testWrappedTestStruct() throws Exception {
    assertSize("wrapped-test-struct", () -> wrappedTestStruct(Collections.emptyList()));

    assertSize("wrapped-test-struct-any", () -> wrappedTestStruct(Arrays.asList(freshAny())));

    ThriftSerializable handWritten = protocol -> {};
    assertEquals(-1, handWritten.serializedSize());
  }

  @Test
  public void testExceptions() throws Exception {
    assertSize("custom-exception", () -> new CustomException.Builder().setMessage("boom").build());
    assertSize("my-exception", () -> new MyException.Builder().setMsg("m").build());
    assertSize(
        "terse-exception",
        () -> new TerseException.Builder().setMsg("m").setCode(2).setReason("r").build());
    assertSize("inner-exception", () -> new InnerException.Builder().setIntField(1).build());
  }
}
