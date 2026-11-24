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

package com.facebook.thrift.serialization;

import com.facebook.thrift.protocol.ByteBufTBinaryProtocol;
import com.facebook.thrift.protocol.ByteBufTCompactProtocol;
import com.facebook.thrift.test.JustABinary;
import com.facebook.thrift.test.TestUnion;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class UnionTest {
  /** The value of the field that follows the union. A reader that loses its place misses it. */
  private static final int VALUE_AFTER_UNION = 0x01020304;

  @Test
  public void testCreatingUnionUnTyped() {
    TestUnion.from(TestUnion._ABINARY, JustABinary.defaultInstance());
  }

  @Test
  public void testCreatingInvalidType() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> TestUnion.from(Integer.MIN_VALUE, JustABinary.defaultInstance()));
  }

  @Test
  public void testInvalidObject() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> TestUnion.from(TestUnion._ABINARY, new ArrayList<>()));
  }

  @Test
  public void testUnionType() {
    TestUnion anUnion = TestUnion.from(TestUnion._ABINARY, JustABinary.defaultInstance());
    Assertions.assertEquals(anUnion.getThriftUnionType(), TestUnion.TypeEnum.A_BINARY);
  }

  @Test
  public void testEmptyUnion() {
    TestUnion anUnion = new TestUnion();
    Assertions.assertEquals(
        anUnion.getThriftUnionType(), TestUnion.TypeEnum.__FBTHRIFT_EMPTY_UNION__);
  }

  /**
   * After the type of a union member changes, an old reader sees the member id with another wire
   * type. It must skip the value, as the reader of a struct does.
   */
  @Test
  public void testMemberWithAnotherWireType() throws TException {
    ByteBuf buf = Unpooled.buffer();
    ByteBufTBinaryProtocol out = new ByteBufTBinaryProtocol();
    out.wrap(buf);
    out.writeStructBegin(new TStruct(""));
    out.writeFieldBegin(new TField("", TType.I32, (short) TestUnion._ABINARY));
    // The first byte of the value must not be zero. An unskipped zero byte reads as the STOP byte,
    // and then the test passes whether the reader skips the value or not.
    out.writeI32(0x01020304);
    out.writeFieldEnd();
    out.writeFieldStop();
    out.writeStructEnd();

    ByteBufTBinaryProtocol in = new ByteBufTBinaryProtocol();
    in.wrap(buf);

    Assertions.assertEquals(
        TestUnion.TypeEnum.__FBTHRIFT_EMPTY_UNION__, TestUnion.read0(in).getThriftUnionType());
  }

  /**
   * A union inside a struct, with one more field after it. The value of the mismatched member
   * starts with a zero byte. A reader that does not skip the value takes that byte as the STOP byte
   * of the union. It then reads the rest of the value as the next field header. The field after the
   * union is lost, with no error.
   */
  @Test
  public void testMemberWithAnotherWireTypeInsideAStruct() throws TException {
    ByteBuf buf = Unpooled.buffer();
    ByteBufTBinaryProtocol out = new ByteBufTBinaryProtocol();
    out.wrap(buf);
    out.writeStructBegin(new TStruct(""));
    out.writeFieldBegin(new TField("", TType.STRUCT, (short) 1));
    out.writeStructBegin(new TStruct(""));
    out.writeFieldBegin(new TField("", TType.I32, (short) TestUnion._ABINARY));
    out.writeI32(5);
    out.writeFieldEnd();
    out.writeFieldStop();
    out.writeStructEnd();
    out.writeFieldEnd();
    out.writeFieldBegin(new TField("", TType.I32, (short) 2));
    out.writeI32(VALUE_AFTER_UNION);
    out.writeFieldEnd();
    out.writeFieldStop();
    out.writeStructEnd();

    ByteBufTBinaryProtocol in = new ByteBufTBinaryProtocol();
    in.wrap(buf);
    in.readStructBegin();
    in.readFieldBegin();

    Assertions.assertEquals(
        TestUnion.TypeEnum.__FBTHRIFT_EMPTY_UNION__, TestUnion.read0(in).getThriftUnionType());
    in.readFieldEnd();
    Assertions.assertEquals((short) 2, in.readFieldBegin().id);
    Assertions.assertEquals(VALUE_AFTER_UNION, in.readI32());
  }

  /**
   * The same case in the Compact protocol, for a member that the union declares as a bool. Compact
   * carries the value of a bool inside the field header. So a reader of a bool member reads no
   * value bytes of its own. It must still move over the bytes of another type. Compact writes i32 0
   * as one zero byte. A reader that does not skip it takes that byte as the STOP byte of the union.
   */
  @Test
  public void testBoolMemberWithAnotherWireTypeInCompact() throws TException {
    ByteBuf buf = Unpooled.buffer();
    ByteBufTCompactProtocol out = new ByteBufTCompactProtocol();
    out.wrap(buf);
    out.writeStructBegin(new TStruct(""));
    out.writeFieldBegin(new TField("", TType.STRUCT, (short) 1));
    out.writeStructBegin(new TStruct(""));
    out.writeFieldBegin(
        new TField(
            "", TType.I32, (short) com.facebook.thrift.test.thrift.any.TestUnion._BOOL_FIELD));
    out.writeI32(0);
    out.writeFieldEnd();
    out.writeFieldStop();
    out.writeStructEnd();
    out.writeFieldEnd();
    out.writeFieldBegin(new TField("", TType.I32, (short) 2));
    out.writeI32(VALUE_AFTER_UNION);
    out.writeFieldEnd();
    out.writeFieldStop();
    out.writeStructEnd();

    ByteBufTCompactProtocol in = new ByteBufTCompactProtocol();
    in.wrap(buf);
    in.readStructBegin();
    in.readFieldBegin();

    Assertions.assertEquals(
        com.facebook.thrift.test.thrift.any.TestUnion.TypeEnum.__FBTHRIFT_EMPTY_UNION__,
        com.facebook.thrift.test.thrift.any.TestUnion.read0(in).getThriftUnionType());
    in.readFieldEnd();
    Assertions.assertEquals((short) 2, in.readFieldBegin().id);
    Assertions.assertEquals(VALUE_AFTER_UNION, in.readI32());
  }
}
