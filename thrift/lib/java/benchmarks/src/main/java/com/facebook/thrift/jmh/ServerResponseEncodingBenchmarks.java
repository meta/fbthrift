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

package com.facebook.thrift.jmh;

import com.facebook.thrift.payload.ThriftSerializable;
import com.facebook.thrift.payload.Writer;
import com.facebook.thrift.protocol.TProtocolType;
import com.facebook.thrift.test.EveryLayout;
import com.facebook.thrift.test.JustABinary;
import com.facebook.thrift.test.adapter.AdapterHookTestStruct;
import com.facebook.thrift.test.terse.AdaptedTerseStruct;
import com.facebook.thrift.util.ResponseBuffers;
import com.facebook.thrift.util.RpcPayloadUtil;
import com.facebook.thrift.util.ThriftSizes;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Measures the exact work a server does per response: allocate, {@code writeMessageBegin}, the
 * generated-struct response writer, {@code writeMessageEnd}, release.
 *
 * <p>The {@code hint} param selects the allocation: {@code none} is the old unhinted {@code
 * alloc.buffer()}, {@code unknown} runs the production unknown-hint path, which allocates a normal
 * direct buffer, {@code sized} runs the production helper, which returns one flat direct buffer for
 * every known hint.
 *
 * <p>The everything sweep ({@code encodeEverythingResponse} over {@code none}/{@code unknown}/
 * {@code sized}) encodes a framed response holding a list of fully-populated {@code EveryLayout}
 * structs at 21 written sizes (1 KiB to 1 GiB, scaled by one element's real written bytes per
 * protocol). The size-only benchmark ({@code encodeSerializedSizeOnly}) measures {@code
 * serializedSize} with no allocation and no write; run it with {@code -prof gc} to verify 0 B/op
 * for the plain and hooked payloads.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
public class ServerResponseEncodingBenchmarks {

  /** Envelope bound for the "benchmark" method name, shared by every case. */
  static final int ENVELOPE_BYTES = (int) ThriftSizes.messageBegin("benchmark");

  /** A writer with no hint, measuring the production unknown-hint path. */
  static Writer unknownWriter(Writer writer) {
    return writer::write;
  }

  /**
   * Wraps a payload the way a generated response writer does: struct begin, the field 0 header, the
   * payload, field stop, struct end.
   */
  static Writer responseWriter(ThriftSerializable payload) {
    return new Writer() {
      @Override
      public void write(TProtocol out) throws TException {
        out.writeStructBegin(RpcPayloadUtil.TSTRUCT);
        out.writeFieldBegin(RpcPayloadUtil.STRUCT_FIELD);
        payload.write0(out);
        out.writeFieldEnd();
        out.writeFieldStop();
        out.writeStructEnd();
      }

      @Override
      public int sizeHint() {
        long payloadSize = payload.serializedSize();

        if (payloadSize < 0) {
          return -1;
        }

        return ThriftSizes.clampToInt(ThriftSizes.FIELD + payloadSize + ThriftSizes.FIELD_STOP);
      }
    };
  }

  /** A numeric-heavy payload: a flat list of small i64 values, generated on the fly. */
  static final class I64ListPayload implements ThriftSerializable {
    private final int count;

    I64ListPayload(int count) {
      this.count = count;
    }

    @Override
    public void write0(TProtocol out) throws TException {
      out.writeStructBegin(RpcPayloadUtil.TSTRUCT);
      out.writeFieldBegin(new TField("values", TType.LIST, (short) 1));
      out.writeListBegin(new TList(TType.I64, count));

      for (int i = 0; i < count; i++) {
        out.writeI64(i % 1001);
      }

      out.writeListEnd();
      out.writeFieldEnd();
      out.writeFieldStop();
      out.writeStructEnd();
    }

    @Override
    public long serializedSize() {
      return ThriftSizes.FIELD
          + ThriftSizes.LIST
          + (long) count * ThriftSizes.I64
          + ThriftSizes.FIELD_STOP;
    }
  }

  /** Writes one framed response through a fresh buffer from the given allocation. */
  private static int serializedSize(Writer writer, TProtocolType type) throws Exception {
    ByteBuf buf = ByteBufAllocator.DEFAULT.buffer();

    try {
      TProtocol out = type.apply(buf);
      out.writeMessageBegin(new TMessage("benchmark", TMessageType.REPLY, 1));

      writer.write(out);
      out.writeMessageEnd();
      return buf.readableBytes();
    } finally {
      buf.release();
    }
  }

  private static long hintOf(Writer writer) {
    int hint = writer.sizeHint();

    return hint < 0 ? -1 : Math.min((long) hint + ENVELOPE_BYTES, Integer.MAX_VALUE);
  }

  @State(Scope.Benchmark)
  public static class SmallState {
    @Param({"TBinary", "TCompact"})
    public String protocolName;

    @Param({"none", "unknown", "sized"})
    public String hint;

    TProtocolType protocolType;
    ByteBufAllocator alloc;

    Writer smallWriter;
    Writer unknownSmallWriter;

    @Setup
    public void setup() throws Exception {
      alloc = ByteBufAllocator.DEFAULT;
      protocolType =
          "TBinary".equals(protocolName) ? TProtocolType.TBinary : TProtocolType.TCompact;

      EveryLayout small =
          new EveryLayout.Builder()
              .setABool(true)
              .setAInt(42)
              .setALong(123456789L)
              .setADouble(1.5)
              .setAString(new String(new char[768]).replace('\0', 'x'))
              .setABinary(new byte[128])
              .build();
      smallWriter = responseWriter(small);
      unknownSmallWriter = unknownWriter(smallWriter);

      ByteBuf capacityProbe =
          allocateSmall(alloc, smallWriter, unknownSmallWriter, protocolType, hint);

      try {
        System.out.printf(
            "small response: %d bytes hint=%d capacity=%d protocol=%s hintMode=%s%n",
            serializedSize(smallWriter, protocolType),
            hintOf(smallWriter),
            capacityProbe.capacity(),
            protocolName,
            hint);
      } finally {
        capacityProbe.release();
      }
    }
  }

  /** Counts buffer growth steps. Used once in setup, never in the timed path. */
  static final class GrowthCountingAllocator extends PooledByteBufAllocator {
    int growthSteps;

    GrowthCountingAllocator() {
      super(true);
    }

    @Override
    public int calculateNewCapacity(int minNewCapacity, int maxCapacity) {
      growthSteps++;
      return super.calculateNewCapacity(minNewCapacity, maxCapacity);
    }
  }

  @State(Scope.Benchmark)
  public static class LargeState {
    @Param({"TBinary", "TCompact"})
    public String protocolName;

    @Param({"65536", "16777216", "134217728"})
    int largeSize;

    @Param({"strings", "i64"})
    public String shape;

    @Param({"none", "unknown", "sized"})
    public String hint;

    TProtocolType protocolType;
    ByteBufAllocator alloc;

    Writer largeWriter;
    Writer unknownLargeWriter;

    @Setup
    public void setup() throws Exception {
      alloc = ByteBufAllocator.DEFAULT;
      protocolType =
          "TBinary".equals(protocolName) ? TProtocolType.TBinary : TProtocolType.TCompact;

      largeWriter = buildLargeWriter(shape, largeSize, protocolType);
      unknownLargeWriter = unknownWriter(largeWriter);

      ByteBuf capacityProbe = allocateLarge(alloc, largeWriter, protocolType);

      try {
        printLargeSetup("large", largeWriter, protocolType, shape, hint, capacityProbe.capacity());
      } finally {
        capacityProbe.release();
      }
    }

    ByteBuf allocateLarge(ByteBufAllocator alloc, Writer writer, TProtocolType type) {
      switch (hint) {
        case "none":
          return alloc.buffer();
        case "sized":
          return ResponseBuffers.allocate(alloc, writer, type, "benchmark");
        case "unknown":
        default:
          return ResponseBuffers.allocate(alloc, unknownLargeWriter, type, "benchmark");
      }
    }
  }

  /** A fully-populated everything struct: every field kind set to a small value. */
  static EveryLayout richElement() {
    return new EveryLayout.Builder()
        .setABool(true)
        .setAInt(42)
        .setALong(123456789L)
        .setAString("element")
        .setADouble(1.5)
        .setAFloat(2.5f)
        .setAShort((short) 7)
        .setAByte((byte) 3)
        .setAList(Lists.newArrayList("a", "bb", "ccc"))
        .setASet(ImmutableSet.of("x", "yy"))
        .setAMap(ImmutableMap.of(1, "one"))
        .setAListOfLists(Lists.newArrayList(Lists.newArrayList("n1"), Lists.newArrayList("n2")))
        .setASetOfSets(ImmutableSet.of(ImmutableSet.of("s1")))
        .setAMapOfLists(ImmutableMap.of(Lists.newArrayList(1, 2), Lists.newArrayList(3)))
        .setABinary(new byte[] {0, 1, 2, 3})
        .build();
  }

  /** A struct holding a list of everything structs, sharing one element instance. */
  static final class EverythingListPayload implements ThriftSerializable {
    private final ThriftSerializable element;
    private final int count;
    private final long elementSize;

    EverythingListPayload(ThriftSerializable element, int count) {
      this.element = element;
      this.count = count;
      this.elementSize = element.serializedSize();
    }

    @Override
    public void write0(TProtocol out) throws TException {
      out.writeStructBegin(RpcPayloadUtil.TSTRUCT);
      out.writeFieldBegin(new TField("elements", TType.LIST, (short) 1));
      out.writeListBegin(new TList(TType.STRUCT, count));

      for (int i = 0; i < count; i++) {
        element.write0(out);
      }

      out.writeListEnd();
      out.writeFieldEnd();
      out.writeFieldStop();
      out.writeStructEnd();
    }

    @Override
    public long serializedSize() {
      return ThriftSizes.FIELD
          + ThriftSizes.LIST
          + (long) count * elementSize
          + ThriftSizes.FIELD_STOP;
    }
  }

  /** Writes one element with a real protocol write; the sweep scales by written bytes. */
  static int writtenElementBytes(TProtocolType type, ThriftSerializable element) throws Exception {
    ByteBuf buf = ByteBufAllocator.DEFAULT.buffer();

    try {
      TProtocol out = type.apply(buf);

      element.write0(out);
      return buf.readableBytes();
    } finally {
      buf.release();
    }
  }

  @State(Scope.Benchmark)
  public static class EverythingState {
    @Param({"TBinary", "TCompact"})
    public String protocolName;

    @Param({
      "1024",
      "2048",
      "4096",
      "8192",
      "16384",
      "32768",
      "65536",
      "131072",
      "262144",
      "524288",
      "1048576",
      "2097152",
      "4194304",
      "8388608",
      "16777216",
      "33554432",
      "67108864",
      "134217728",
      "268435456",
      "536870912",
      "1073741824"
    })
    int targetBytes;

    @Param({"none", "unknown", "sized"})
    public String hint;

    TProtocolType protocolType;
    ByteBufAllocator alloc;

    Writer everythingWriter;
    Writer unknownEverythingWriter;

    @Setup
    public void setup() throws Exception {
      alloc = ByteBufAllocator.DEFAULT;
      protocolType =
          "TBinary".equals(protocolName) ? TProtocolType.TBinary : TProtocolType.TCompact;

      EveryLayout element = richElement();
      int count =
          (int) Math.max(1, (long) targetBytes / writtenElementBytes(protocolType, element));
      everythingWriter = responseWriter(new EverythingListPayload(element, count));
      unknownEverythingWriter = unknownWriter(everythingWriter);

      ByteBuf capacityProbe =
          allocateEverything(alloc, everythingWriter, unknownEverythingWriter, protocolType, hint);

      try {
        printLargeSetup(
            "everything",
            everythingWriter,
            protocolType,
            String.valueOf(targetBytes),
            hint,
            capacityProbe.capacity());
      } finally {
        capacityProbe.release();
      }
    }
  }

  static ByteBuf allocateEverything(
      ByteBufAllocator alloc,
      Writer writer,
      Writer unknownWriter,
      TProtocolType type,
      String hint) {
    switch (hint) {
      case "none":
        return alloc.buffer();
      case "sized":
        return ResponseBuffers.allocate(alloc, writer, type, "benchmark");
      case "unknown":
      default:
        return ResponseBuffers.allocate(alloc, unknownWriter, type, "benchmark");
    }
  }

  @State(Scope.Benchmark)
  public static class SizeOnlyState {
    @Param({"plain", "hooked", "converted"})
    public String payloadKind;

    ThriftSerializable payload;

    @Setup
    public void setup() {
      switch (payloadKind) {
        case "hooked":
          payload =
              new AdapterHookTestStruct.Builder()
                  .setPlainField("plain")
                  .setHookedField("hooked")
                  .setOptHookedField("opt")
                  .build();
          break;
        case "converted":
          payload =
              new AdaptedTerseStruct.Builder()
                  .setAdaptedBooleanField("true")
                  .setAdaptedShortField("7")
                  .setAdaptedIntField("42")
                  .setAdaptedLongField("8")
                  .setDoubleAdaptedIntField(43L)
                  .setDoubleTypedefAdaptedIntField("44")
                  .build();
          break;
        case "plain":
        default:
          payload = richElement();
          break;
      }

      System.out.printf("size-only: kind=%s bound=%d%n", payloadKind, payload.serializedSize());
    }
  }

  static Writer buildLargeWriter(String shape, int largeSize, TProtocolType protocolType) {
    if ("i64".equals(shape)) {
      int perElem = protocolType == TProtocolType.TBinary ? 8 : 2;
      return responseWriter(new I64ListPayload(largeSize / perElem));
    }

    if (largeSize == 65536) {
      return responseWriter(new JustABinary.Builder().setABinary(new byte[largeSize]).build());
    }

    // Many small writes: this is the shape that grows a flat buffer by realloc-copy.
    String element = new String(new char[4096]).replace('\0', 'x');
    List<String> elements = new ArrayList<>(largeSize / 4096);

    for (int i = 0; i < largeSize / 4096; i++) {
      elements.add(element);
    }

    return responseWriter(new EveryLayout.Builder().setAList(elements).build());
  }

  static void printLargeSetup(
      String label,
      Writer writer,
      TProtocolType protocolType,
      String shape,
      String hint,
      int capacity)
      throws Exception {
    GrowthCountingAllocator counting = new GrowthCountingAllocator();
    ByteBuf probe = counting.buffer();
    int written;

    try {
      TProtocol out = protocolType.apply(probe);
      out.writeMessageBegin(new TMessage("benchmark", TMessageType.REPLY, 1));
      writer.write(out);
      out.writeMessageEnd();
      written = probe.readableBytes();
    } finally {
      probe.release();
    }

    System.out.printf(
        "%s response: %d bytes in %d growth steps hint=%d capacity=%d"
            + " protocol=%s shape=%s hintMode=%s%n",
        label, written, counting.growthSteps, hintOf(writer), capacity, protocolType, shape, hint);
  }

  static ByteBuf allocateSmall(
      ByteBufAllocator alloc,
      Writer writer,
      Writer unknownWriter,
      TProtocolType type,
      String hint) {
    switch (hint) {
      case "none":
        return alloc.buffer();
      case "unknown":
        return ResponseBuffers.allocate(alloc, unknownWriter, type, "benchmark");
      case "sized":
      default:
        return ResponseBuffers.allocate(alloc, writer, type, "benchmark");
    }
  }

  @Benchmark
  @Fork(
      value = 3,
      jvmArgsAppend = {
        "-Dio.netty.leakDetectionLevel=disabled",
        "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
        "-Dio.netty.tryReflectionSetAccessible=true",
        "-Dio.netty.buffer.checkBounds=false",
        "-Dio.netty.buffer.checkAccessible=false",
        "-Dthrift.force-execution-off-eventloop=false"
      })
  public void encodeSmallResponse(SmallState state, Blackhole bh) throws Exception {
    ByteBuf buf =
        allocateSmall(
            state.alloc,
            state.smallWriter,
            state.unknownSmallWriter,
            state.protocolType,
            state.hint);

    writeFramed(state.protocolType, buf, state.smallWriter, bh);
  }

  @Benchmark
  @Fork(
      value = 1,
      jvmArgsAppend = {
        "-Dio.netty.leakDetectionLevel=disabled",
        "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
        "-Dio.netty.tryReflectionSetAccessible=true",
        "-Dio.netty.buffer.checkBounds=false",
        "-Dio.netty.buffer.checkAccessible=false",
        "-Dthrift.force-execution-off-eventloop=false",
        "-Xmx16g",
        "-XX:MaxDirectMemorySize=32g"
      })
  public void encodeLargeResponse(LargeState state, Blackhole bh) throws Exception {
    ByteBuf buf = state.allocateLarge(state.alloc, state.largeWriter, state.protocolType);

    writeFramed(state.protocolType, buf, state.largeWriter, bh);
  }

  @Benchmark
  @Warmup(iterations = 2, time = 1)
  @Measurement(iterations = 3, time = 1)
  @Fork(
      value = 1,
      jvmArgsAppend = {
        "-Dio.netty.leakDetectionLevel=disabled",
        "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
        "-Dio.netty.tryReflectionSetAccessible=true",
        "-Dio.netty.buffer.checkBounds=false",
        "-Dio.netty.buffer.checkAccessible=false",
        "-Dthrift.force-execution-off-eventloop=false",
        "-Xmx16g",
        "-XX:MaxDirectMemorySize=32g"
      })
  public void encodeEverythingResponse(EverythingState state, Blackhole bh) throws Exception {
    ByteBuf buf =
        allocateEverything(
            state.alloc,
            state.everythingWriter,
            state.unknownEverythingWriter,
            state.protocolType,
            state.hint);

    writeFramed(state.protocolType, buf, state.everythingWriter, bh);
  }

  @Benchmark
  @Warmup(iterations = 2, time = 1)
  @Measurement(iterations = 3, time = 1)
  @Fork(
      value = 1,
      jvmArgsAppend = {
        "-Dio.netty.leakDetectionLevel=disabled",
        "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
        "-Dio.netty.tryReflectionSetAccessible=true",
        "-Dio.netty.buffer.checkBounds=false",
        "-Dio.netty.buffer.checkAccessible=false",
        "-Dthrift.force-execution-off-eventloop=false"
      })
  public void encodeSerializedSizeOnly(SizeOnlyState state, Blackhole bh) {
    bh.consume(state.payload.serializedSize());
  }

  private static void writeFramed(
      TProtocolType protocolType, ByteBuf buf, Writer writer, Blackhole bh) throws Exception {
    try {
      TProtocol out = protocolType.apply(buf);
      out.writeMessageBegin(new TMessage("benchmark", TMessageType.REPLY, 1));

      writer.write(out);
      out.writeMessageEnd();
      bh.consume(buf);
    } finally {
      buf.release();
    }
  }
}
