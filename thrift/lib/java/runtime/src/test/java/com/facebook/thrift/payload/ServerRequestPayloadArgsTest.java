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

package com.facebook.thrift.payload;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.facebook.thrift.example.ping.PingRequest;
import com.facebook.thrift.protocol.ByteBufTBinaryProtocol;
import com.facebook.thrift.protocol.ByteBufTCompactProtocol;
import com.facebook.thrift.util.Readers;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.function.Consumer;
import org.apache.thrift.RequestRpcMetadata;
import org.apache.thrift.RpcKind;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.junit.jupiter.api.Test;

/** Verifies that server request arguments are decoded as struct fields, matched by Field ID. */
public class ServerRequestPayloadArgsTest {

  private static final PingRequest PING = new PingRequest.Builder().setRequest("hello").build();
  private static final PingRequest EMPTY_PING = new PingRequest.Builder().build();

  /** Mirrors a method like {@code foo(1: i32 count, 2: PingRequest request)}. */
  private static List<FieldReader<?>> fieldReaders() {
    return Arrays.asList(
        new FieldReader<>((short) 1, TType.I32, Readers.i32Reader(), () -> 0),
        new FieldReader<>(
            (short) 2,
            TType.STRUCT,
            Readers.wrap(PingRequest.asReader()),
            () -> new PingRequest.Builder().build()));
  }

  private static ServerRequestPayload payloadFor(ByteBuf buf, TProtocol in) {
    RequestRpcMetadata metadata =
        new RequestRpcMetadata.Builder()
            .setName("foo")
            .setKind(RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE)
            .build();
    return ServerRequestPayload.create(metadata, null, RequestData.of(buf, in));
  }

  private static List<Object> decode(
      List<FieldReader<?>> readers, Consumer<ByteBufTCompactProtocol> args) {
    ByteBuf buf = Unpooled.buffer();
    ByteBufTCompactProtocol out = new ByteBufTCompactProtocol();
    out.wrap(buf);
    out.writeStructBegin(new TStruct(""));
    args.accept(out);
    out.writeFieldStop();
    out.writeStructEnd();

    ByteBufTCompactProtocol in = new ByteBufTCompactProtocol();
    in.wrap(buf);
    List<Object> data = payloadFor(buf, in).getData(readers);
    assertThat(buf.refCnt()).isEqualTo(0);
    return data;
  }

  private static List<Object> decodeBinary(
      List<FieldReader<?>> readers, Consumer<ByteBufTBinaryProtocol> args) {
    ByteBuf buf = Unpooled.buffer();
    ByteBufTBinaryProtocol out = new ByteBufTBinaryProtocol();
    out.wrap(buf);
    out.writeStructBegin(new TStruct(""));
    args.accept(out);
    out.writeFieldStop();
    out.writeStructEnd();

    ByteBufTBinaryProtocol in = new ByteBufTBinaryProtocol();
    in.wrap(buf);
    List<Object> data = payloadFor(buf, in).getData(readers);
    assertThat(buf.refCnt()).isEqualTo(0);
    return data;
  }

  private static void writeCount(TProtocol out, short id, int value) {
    out.writeFieldBegin(new TField("", TType.I32, id));
    out.writeI32(value);
    out.writeFieldEnd();
  }

  private static void writePing(TProtocol out, short id) {
    out.writeFieldBegin(new TField("", TType.STRUCT, id));
    PING.write0(out);
    out.writeFieldEnd();
  }

  @Test
  public void allArgumentsPresent() {
    List<Object> data =
        decode(
            fieldReaders(),
            out -> {
              writeCount(out, (short) 1, 7);
              writePing(out, (short) 2);
            });
    assertThat(data).containsExactly(7, PING);
  }

  @Test
  public void omittedTrailingStructArgument_isDefaultConstructed() {
    // A client that skips null fields, e.g. Hack passing null for the struct parameter.
    List<Object> data = decode(fieldReaders(), out -> writeCount(out, (short) 1, 7));
    assertThat(data).containsExactly(7, EMPTY_PING);
  }

  @Test
  public void omittedPrimitiveArgument_isZero() {
    List<Object> data = decode(fieldReaders(), out -> writePing(out, (short) 2));
    assertThat(data).containsExactly(0, PING);
  }

  @Test
  public void argumentsOutOfOrder_matchById() {
    List<Object> data =
        decode(
            fieldReaders(),
            out -> {
              writePing(out, (short) 2);
              writeCount(out, (short) 1, 7);
            });
    assertThat(data).containsExactly(7, PING);
  }

  @Test
  public void unknownArgument_isSkipped() {
    List<Object> data =
        decode(
            fieldReaders(),
            out -> {
              writeCount(out, (short) 1, 7);
              out.writeFieldBegin(new TField("", TType.STRING, (short) 9));
              out.writeString("from a newer client");
              out.writeFieldEnd();
              writePing(out, (short) 2);
            });
    assertThat(data).containsExactly(7, PING);
  }

  @Test
  public void argumentWithWrongType_isSkipped() {
    List<Object> data =
        decode(
            fieldReaders(),
            out -> {
              writeCount(out, (short) 1, 7);
              writeCount(out, (short) 2, 8);
            });
    assertThat(data).containsExactly(7, EMPTY_PING);
  }

  @Test
  public void omittedArgument_defaultIsNotSharedAcrossRequests() {
    Object first = decode(fieldReaders(), out -> writeCount(out, (short) 1, 7)).get(1);
    Object second = decode(fieldReaders(), out -> writeCount(out, (short) 1, 7)).get(1);
    assertThat(first).isEqualTo(second).isNotSameAs(second);
  }

  @Test
  public void methodWithoutParameters_skipsEveryField() {
    List<Object> data = decode(Collections.emptyList(), out -> writeCount(out, (short) 1, 7));
    assertThat(data).isEmpty();
  }

  @Test
  public void duplicateFieldId_lastOneWins() {
    List<Object> data =
        decode(
            fieldReaders(),
            out -> {
              writeCount(out, (short) 1, 7);
              writeCount(out, (short) 1, 8);
              writePing(out, (short) 2);
            });
    assertThat(data).containsExactly(8, PING);
  }

  @Test
  public void wrongTypeThenCorrectType_readsCorrectOne() {
    List<Object> data =
        decode(
            fieldReaders(),
            out -> {
              writeCount(out, (short) 1, 7);
              writeCount(out, (short) 2, 8);
              writePing(out, (short) 2);
            });
    assertThat(data).containsExactly(7, PING);
  }

  @Test
  public void missingMiddleParam_isDefaulted() {
    List<FieldReader<?>> readers =
        Arrays.asList(
            new FieldReader<>((short) 1, TType.I32, Readers.i32Reader(), () -> 0),
            new FieldReader<>(
                (short) 2,
                TType.STRUCT,
                Readers.wrap(PingRequest.asReader()),
                () -> new PingRequest.Builder().build()),
            new FieldReader<>((short) 3, TType.I32, Readers.i32Reader(), () -> 0));
    List<Object> data =
        decode(
            readers,
            out -> {
              writeCount(out, (short) 1, 7);
              writeCount(out, (short) 3, 9);
            });
    assertThat(data).containsExactly(7, EMPTY_PING, 9);
  }

  @Test
  public void allParamsMissing_eachIsDefaulted() {
    List<Object> data = decode(fieldReaders(), out -> {});
    assertThat(data).containsExactly(0, EMPTY_PING);
  }

  @Test
  public void binaryProtocol_allArgumentsPresent() {
    List<Object> data =
        decodeBinary(
            fieldReaders(),
            out -> {
              writeCount(out, (short) 1, 7);
              writePing(out, (short) 2);
            });
    assertThat(data).containsExactly(7, PING);
  }

  @Test
  public void binaryProtocol_missingAndReorderedArgs() {
    List<Object> missing = decodeBinary(fieldReaders(), out -> writePing(out, (short) 2));
    assertThat(missing).containsExactly(0, PING);

    List<Object> reordered =
        decodeBinary(
            fieldReaders(),
            out -> {
              writePing(out, (short) 2);
              writeCount(out, (short) 1, 7);
            });
    assertThat(reordered).containsExactly(7, PING);
  }

  @Test
  public void truncatedArgs_throwProtocolExceptionAndReleaseBuffer() {
    ByteBuf buf = Unpooled.buffer();
    ByteBufTCompactProtocol out = new ByteBufTCompactProtocol();
    out.wrap(buf);
    out.writeStructBegin(new TStruct(""));
    writeCount(out, (short) 1, 7);
    // No STOP: the frame ends mid-struct.

    ByteBufTCompactProtocol in = new ByteBufTCompactProtocol();
    in.wrap(buf);
    ServerRequestPayload payload = payloadFor(buf, in);
    assertThatThrownBy(() -> payload.getData(fieldReaders()))
        .isInstanceOf(TProtocolException.class)
        .hasMessageContaining("Truncated args for foo");
    assertThat(buf.refCnt()).isEqualTo(0);
    assertThatThrownBy(() -> payload.getData(fieldReaders()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("already released");
  }

  @Test
  public void readerThrowingMidLoop_releasesBuffer() {
    List<FieldReader<?>> readers =
        Arrays.asList(
            new FieldReader<Integer>(
                (short) 1,
                TType.I32,
                iprot -> {
                  throw new TException("boom");
                },
                () -> 0),
            new FieldReader<>(
                (short) 2,
                TType.STRUCT,
                Readers.wrap(PingRequest.asReader()),
                () -> new PingRequest.Builder().build()));

    ByteBuf buf = Unpooled.buffer();
    ByteBufTCompactProtocol out = new ByteBufTCompactProtocol();
    out.wrap(buf);
    out.writeStructBegin(new TStruct(""));
    writeCount(out, (short) 1, 7);
    writePing(out, (short) 2);
    out.writeFieldStop();
    out.writeStructEnd();

    ByteBufTCompactProtocol in = new ByteBufTCompactProtocol();
    in.wrap(buf);
    ServerRequestPayload payload = payloadFor(buf, in);
    assertThatThrownBy(() -> payload.getData(readers)).isInstanceOf(TException.class);
    assertThat(buf.refCnt()).isEqualTo(0);
    assertThatThrownBy(() -> payload.getData(readers)).isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void allMissingScalarsAndContainers_takeCodegenDefaults() {
    // Supplier shapes mirror types.whisker param_default_value for each type.
    List<FieldReader<?>> readers =
        Arrays.asList(
            new FieldReader<>((short) 1, TType.BOOL, Readers.booleanReader(), () -> false),
            new FieldReader<>((short) 2, TType.BYTE, Readers.byteReader(), () -> (byte) 0),
            new FieldReader<>((short) 3, TType.I16, Readers.i16Reader(), () -> (short) 0),
            new FieldReader<>((short) 4, TType.I32, Readers.i32Reader(), () -> 0),
            new FieldReader<>((short) 5, TType.I64, Readers.i64Reader(), () -> 0L),
            new FieldReader<>((short) 6, TType.FLOAT, Readers.floatReader(), () -> 0.f),
            new FieldReader<>((short) 7, TType.DOUBLE, Readers.doubleReader(), () -> 0.),
            new FieldReader<>((short) 8, TType.STRING, Readers.stringReader(), () -> ""),
            new FieldReader<>((short) 9, TType.STRING, Readers.binaryReader(), () -> new byte[0]),
            new FieldReader<>(
                (short) 10,
                TType.LIST,
                Readers.listReader(Readers.i32Reader()),
                () -> new ArrayList<>()),
            new FieldReader<>(
                (short) 11,
                TType.SET,
                Readers.setReader(Readers.i32Reader()),
                () -> new HashSet<>()),
            new FieldReader<>(
                (short) 12,
                TType.MAP,
                Readers.mapReader(Readers.i32Reader(), Readers.i32Reader()),
                () -> new HashMap<>()));
    List<Object> data = decode(readers, out -> {});
    assertThat(data).hasSize(12);
    assertThat(data.subList(0, 8)).containsExactly(false, (byte) 0, (short) 0, 0, 0L, 0.f, 0., "");
    assertThat((byte[]) data.get(8)).isEmpty();
    assertThat(data.get(9)).isEqualTo(new ArrayList<>());
    assertThat(data.get(10)).isEqualTo(new HashSet<>());
    assertThat(data.get(11)).isEqualTo(new HashMap<>());
  }

  @Test
  public void unknownFieldWithGarbageType_throwsProtocolException() {
    ByteBuf buf = Unpooled.buffer();
    ByteBufTBinaryProtocol out = new ByteBufTBinaryProtocol();
    out.wrap(buf);
    out.writeStructBegin(new TStruct(""));
    out.writeFieldBegin(new TField("", (byte) 5, (short) 9));
    out.writeFieldEnd();
    out.writeFieldStop();
    out.writeStructEnd();

    ByteBufTBinaryProtocol in = new ByteBufTBinaryProtocol();
    in.wrap(buf);
    ServerRequestPayload payload = payloadFor(buf, in);
    assertThatThrownBy(() -> payload.getData(fieldReaders()))
        .isInstanceOf(TProtocolException.class)
        .hasMessageContaining("Invalid skip type");
    assertThat(buf.refCnt()).isEqualTo(0);
  }

  @Test
  public void compactFieldWithInvalidTypeNibble_throwsProtocolException() {
    ByteBuf buf = Unpooled.buffer();
    ByteBufTCompactProtocol out = new ByteBufTCompactProtocol();
    out.wrap(buf);
    out.writeStructBegin(new TStruct(""));
    // Raw field header: id delta 1, invalid type nibble 0xE.
    buf.writeByte(0x1E);
    buf.writeByte(0x00); // STOP ends the struct if the header were valid.

    ByteBufTCompactProtocol in = new ByteBufTCompactProtocol();
    in.wrap(buf);
    ServerRequestPayload payload = payloadFor(buf, in);
    assertThatThrownBy(() -> payload.getData(fieldReaders()))
        .isInstanceOf(TProtocolException.class)
        .hasMessageContaining("Invalid compact type");
    assertThat(buf.refCnt()).isEqualTo(0);
  }

  @Test
  public void deeplyNestedUnknownField_hitsSkipDepthLimit() {
    ByteBuf buf = Unpooled.buffer();
    ByteBufTBinaryProtocol out = new ByteBufTBinaryProtocol();
    out.wrap(buf);
    out.writeStructBegin(new TStruct(""));
    for (int i = 0; i < 70; i++) {
      out.writeFieldBegin(new TField("", TType.STRUCT, (short) 9));
      out.writeStructBegin(new TStruct(""));
    }
    for (int i = 0; i < 70; i++) {
      out.writeFieldStop();
      out.writeStructEnd();
      out.writeFieldEnd();
    }
    out.writeFieldStop();
    out.writeStructEnd();

    ByteBufTBinaryProtocol in = new ByteBufTBinaryProtocol();
    in.wrap(buf);
    ServerRequestPayload payload = payloadFor(buf, in);
    assertThatThrownBy(() -> payload.getData(Collections.emptyList()))
        .isInstanceOf(TException.class)
        .hasMessageContaining("Maximum skip depth exceeded");
    assertThat(buf.refCnt()).isEqualTo(0);
  }
}
