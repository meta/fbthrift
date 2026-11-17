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

package com.facebook.thrift.rsocket.server;

import static io.rsocket.frame.FrameHeaderCodec.FLAGS_C;
import static io.rsocket.frame.FrameHeaderCodec.FLAGS_F;
import static io.rsocket.frame.FrameHeaderCodec.FLAGS_N;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.facebook.thrift.protocol.ProtocolUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.DuplexConnection;
import io.rsocket.RSocketErrorException;
import io.rsocket.frame.CancelFrameCodec;
import io.rsocket.frame.ErrorFrameCodec;
import io.rsocket.frame.FrameHeaderCodec;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.PayloadFrameCodec;
import io.rsocket.frame.RequestChannelFrameCodec;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.thrift.ProtocolId;
import org.apache.thrift.RequestRpcMetadata;
import org.apache.thrift.RpcKind;
import org.apache.thrift.StreamRpcError;
import org.apache.thrift.StreamRpcErrorCode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

public class RocketDuplexConnectionTest {
  private final ByteBufAllocator alloc = ByteBufAllocator.DEFAULT;
  private final Sinks.Many<ByteBuf> inbound = Sinks.many().unicast().onBackpressureBuffer();
  private final List<String> sent = new ArrayList<>();
  private final List<String> sentErrorData = new ArrayList<>();
  private final List<String> passed = new ArrayList<>();
  private final DuplexConnection connection = RocketDuplexConnection.wrap(new Recorder());

  @BeforeEach
  public void receive() {
    connection
        .receive()
        .subscribe(
            frame -> {
              try {
                passed.add(FrameHeaderCodec.frameType(frame).name());
              } catch (RuntimeException e) {
                passed.add("MALFORMED");
              }

              frame.release();
            });
  }

  @Test
  public void sinkFinalCarriesCompleteAndTheTrailingCompleteIsDropped() {
    int streamId = 1;
    receiveChannel(streamId, RpcKind.SINK, false);

    sendPayload(streamId, FLAGS_N);
    sendPayload(streamId, FLAGS_N);
    ByteBuf complete = PayloadFrameCodec.encodeComplete(alloc, streamId);
    connection.sendFrame(streamId, complete);

    assertEquals(Arrays.asList("NEXT", "NEXT_COMPLETE"), sent);
    assertEquals(0, complete.refCnt());
  }

  @Test
  public void fragmentsOfOnePayloadCountOnce() {
    int streamId = 1;
    receiveChannel(streamId, RpcKind.SINK, false);

    sendPayload(streamId, FLAGS_N | FLAGS_F);
    sendPayload(streamId, FLAGS_N);
    sendPayload(streamId, FLAGS_N | FLAGS_F);
    sendPayload(streamId, FLAGS_N);

    assertEquals(Arrays.asList("NEXT+F", "NEXT", "NEXT+F", "NEXT_COMPLETE"), sent);
  }

  @Test
  public void bidiFramesPassUnchanged() {
    int streamId = 1;
    receiveChannel(streamId, RpcKind.BIDIRECTIONAL_STREAM, false);
    sendPayload(streamId, FLAGS_N);
    sendPayload(streamId, FLAGS_N);
    sendPayload(streamId, FLAGS_C);

    assertEquals(Arrays.asList("NEXT", "NEXT", "COMPLETE"), sent);
  }

  @Test
  public void peerErrorOrCancelEndsTheSink() {
    int errorId = 1;
    receiveChannel(errorId, RpcKind.SINK, false);
    sendPayload(errorId, FLAGS_N);
    receive(ErrorFrameCodec.encode(alloc, errorId, new RuntimeException("sink")));
    sendPayload(errorId, FLAGS_N);

    int cancelId = 3;
    receiveChannel(cancelId, RpcKind.SINK, false);
    sendPayload(cancelId, FLAGS_N);
    receive(CancelFrameCodec.encode(alloc, cancelId));
    sendPayload(cancelId, FLAGS_N);

    assertEquals(Arrays.asList("NEXT", "NEXT", "NEXT", "NEXT"), sent);
  }

  @Test
  public void bidiPeerSinkErrorEndsTheStreamWithAnError() {
    int streamId = 1;
    receiveChannel(streamId, RpcKind.BIDIRECTIONAL_STREAM, false);
    sendPayload(streamId, FLAGS_N);
    receive(ErrorFrameCodec.encode(alloc, streamId, new RuntimeException("sink")));

    assertEquals(Arrays.asList("NEXT", "ERROR"), sent);
    assertEquals(Arrays.asList("REQUEST_CHANNEL", "ERROR"), passed);

    StreamRpcError error = decodeStreamError(sentErrorData.get(0));

    assertEquals(StreamRpcErrorCode.SERVER_CLOSING_CONNECTION, error.getCode());
  }

  @Test
  public void bidiPeerCancelEndsTheSinkWithACancel() {
    int streamId = 1;
    receiveChannel(streamId, RpcKind.BIDIRECTIONAL_STREAM, false);
    sendPayload(streamId, FLAGS_N);
    receive(CancelFrameCodec.encode(alloc, streamId));

    assertEquals(Arrays.asList("NEXT", "CANCEL"), sent);
  }

  @Test
  public void bidiServerErrorAfterTheFirstResponseAlsoEndsThePeerSink() {
    int untouchedId = 1;
    receiveChannel(untouchedId, RpcKind.BIDIRECTIONAL_STREAM, false);
    connection.sendFrame(
        untouchedId, ErrorFrameCodec.encode(alloc, untouchedId, new Exception("a")));

    int trackedId = 3;
    receiveChannel(trackedId, RpcKind.BIDIRECTIONAL_STREAM, false);
    sendPayload(trackedId, FLAGS_N);
    connection.sendFrame(trackedId, ErrorFrameCodec.encode(alloc, trackedId, new Exception("b")));

    assertEquals(Arrays.asList("ERROR", "NEXT", "ERROR", "CANCEL"), sent);
  }

  @Test
  public void bidiNeedsNoFrameBeforeTheFirstResponseOrAfterAHalfEnded() {
    int beforeId = 1;
    receiveChannel(beforeId, RpcKind.BIDIRECTIONAL_STREAM, false);
    receive(CancelFrameCodec.encode(alloc, beforeId));

    int completeId = 3;
    receiveChannel(completeId, RpcKind.BIDIRECTIONAL_STREAM, true);
    sendPayload(completeId, FLAGS_N);
    receive(CancelFrameCodec.encode(alloc, completeId));

    assertEquals(Collections.singletonList("NEXT"), sent);
  }

  @Test
  public void bidiPeerFramesAfterTheServerCompletePassUnchanged() {
    int streamId = 1;
    receiveChannel(streamId, RpcKind.BIDIRECTIONAL_STREAM, false);
    sendPayload(streamId, FLAGS_N);
    sendPayload(streamId, FLAGS_C);

    receive(
        PayloadFrameCodec.encode(
            alloc, streamId, false, false, true, null, alloc.buffer().writeByte(0)));
    receive(ErrorFrameCodec.encode(alloc, streamId, new RuntimeException("sink")));

    assertEquals(Arrays.asList("NEXT", "COMPLETE"), sent);
    assertEquals(Arrays.asList("REQUEST_CHANNEL", "NEXT", "ERROR"), passed);
  }

  @Test
  public void setupWhoseMetadataSpansFragmentsIsRejectedBeforeDispatch() {
    int streamId = 1;
    ByteBuf metadata = metadata(RpcKind.SINK);
    ByteBuf setup =
        RequestChannelFrameCodec.encode(
            alloc, streamId, true, false, 1, metadata.retainedSlice(0, 3), alloc.buffer());
    metadata.release();

    receive(setup);

    // The transport owns the release of a dropped setup, so the wrapper must not release it.
    assertEquals(1, setup.refCnt());
    setup.release();

    assertEquals(Collections.singletonList("ERROR"), sent);
    assertTrue(passed.isEmpty());
  }

  @Test
  public void fragmentedSetupWithWholeMetadataIsTracked() {
    int streamId = 1;
    receive(
        RequestChannelFrameCodec.encode(
            alloc, streamId, true, false, 1, metadata(RpcKind.SINK), alloc.buffer()));

    sendPayload(streamId, FLAGS_N);
    sendPayload(streamId, FLAGS_N);

    assertEquals(Arrays.asList("NEXT", "NEXT_COMPLETE"), sent);
  }

  @Test
  public void malformedFramePassesThroughAndWrapperKeepsWorking() {
    receive(alloc.buffer().writeInt(7));

    int streamId = 9;
    receiveChannel(streamId, RpcKind.SINK, false);
    sendPayload(streamId, FLAGS_N);
    sendPayload(streamId, FLAGS_N);

    assertTrue(passed.contains("MALFORMED"));
    assertEquals(Arrays.asList("NEXT", "NEXT_COMPLETE"), sent);
  }

  @Test
  public void requestResponseAndRequestStreamPassUntouched() {
    int responseId = 1;
    sendPayload(responseId, FLAGS_N);
    connection.sendFrame(responseId, PayloadFrameCodec.encodeComplete(alloc, responseId));

    int streamId = 3;
    sendPayload(streamId, FLAGS_N);
    sendPayload(streamId, FLAGS_N);
    connection.sendFrame(streamId, PayloadFrameCodec.encodeComplete(alloc, streamId));

    assertEquals(Arrays.asList("NEXT", "COMPLETE", "NEXT", "NEXT", "COMPLETE"), sent);
  }

  @Test
  public void interleavedIdsKeepTheirOwnRules() {
    int sinkId = 1;
    int bidiId = 3;
    int streamId = 5;

    receiveChannel(sinkId, RpcKind.SINK, false);
    receiveChannel(bidiId, RpcKind.BIDIRECTIONAL_STREAM, false);

    sendPayload(sinkId, FLAGS_N);
    sendPayload(bidiId, FLAGS_N);
    sendPayload(streamId, FLAGS_N);
    sendPayload(sinkId, FLAGS_N);
    sendPayload(bidiId, FLAGS_N);
    connection.sendFrame(streamId, PayloadFrameCodec.encodeComplete(alloc, streamId));

    assertEquals(Arrays.asList("NEXT", "NEXT", "NEXT", "NEXT_COMPLETE", "NEXT", "COMPLETE"), sent);
  }

  @Test
  public void bidiPeerCompleteStopsTracking() {
    int completeId = 1;
    receiveChannel(completeId, RpcKind.BIDIRECTIONAL_STREAM, false);
    sendPayload(completeId, FLAGS_N);
    receive(PayloadFrameCodec.encodeComplete(alloc, completeId));
    connection.sendFrame(completeId, ErrorFrameCodec.encode(alloc, completeId, new Exception("a")));

    int nextCompleteId = 3;
    receiveChannel(nextCompleteId, RpcKind.BIDIRECTIONAL_STREAM, false);
    sendPayload(nextCompleteId, FLAGS_N);
    receive(
        PayloadFrameCodec.encode(
            alloc, nextCompleteId, false, true, true, null, alloc.buffer().writeByte(0)));
    connection.sendFrame(
        nextCompleteId, ErrorFrameCodec.encode(alloc, nextCompleteId, new Exception("b")));

    assertEquals(Arrays.asList("NEXT", "ERROR", "NEXT", "ERROR"), sent);
  }

  @Test
  public void sinkServerErrorStopsTracking() {
    int beforeId = 1;
    receiveChannel(beforeId, RpcKind.SINK, false);
    connection.sendFrame(beforeId, ErrorFrameCodec.encode(alloc, beforeId, new Exception("a")));
    sendPayload(beforeId, FLAGS_N);
    sendPayload(beforeId, FLAGS_N);

    int afterId = 3;
    receiveChannel(afterId, RpcKind.SINK, false);
    sendPayload(afterId, FLAGS_N);
    connection.sendFrame(afterId, ErrorFrameCodec.encode(alloc, afterId, new Exception("b")));
    sendPayload(afterId, FLAGS_N);
    sendPayload(afterId, FLAGS_N);

    assertEquals(Arrays.asList("ERROR", "NEXT", "NEXT", "NEXT", "ERROR", "NEXT", "NEXT"), sent);
  }

  @Test
  public void bidiPeerErrorBeforeTheFirstResponseAddsNothing() {
    int streamId = 1;
    receiveChannel(streamId, RpcKind.BIDIRECTIONAL_STREAM, false);
    receive(ErrorFrameCodec.encode(alloc, streamId, new RuntimeException("sink")));
    sendPayload(streamId, FLAGS_N);

    assertEquals(Collections.singletonList("NEXT"), sent);
    assertEquals(Arrays.asList("REQUEST_CHANNEL", "ERROR"), passed);
  }

  @Test
  public void serverCancelPassesThrough() {
    int sinkId = 1;
    receiveChannel(sinkId, RpcKind.SINK, false);
    connection.sendFrame(sinkId, CancelFrameCodec.encode(alloc, sinkId));

    int bidiId = 3;
    receiveChannel(bidiId, RpcKind.BIDIRECTIONAL_STREAM, false);
    sendPayload(bidiId, FLAGS_N);
    connection.sendFrame(bidiId, CancelFrameCodec.encode(alloc, bidiId));

    assertEquals(Arrays.asList("CANCEL", "NEXT", "CANCEL"), sent);
  }

  @Test
  public void unknownAndNonChannelKindsPassUntracked() {
    int unknownId = 1;
    receive(
        RequestChannelFrameCodec.encode(
            alloc, unknownId, false, false, 1, metadata(null), alloc.buffer()));
    sendPayload(unknownId, FLAGS_N);
    sendPayload(unknownId, FLAGS_N);

    int responseId = 3;
    receiveChannel(responseId, RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE, false);
    sendPayload(responseId, FLAGS_N);
    sendPayload(responseId, FLAGS_N);

    int streamKindId = 5;
    receiveChannel(streamKindId, RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE, false);
    sendPayload(streamKindId, FLAGS_N);
    sendPayload(streamKindId, FLAGS_N);

    assertEquals(Arrays.asList("NEXT", "NEXT", "NEXT", "NEXT", "NEXT", "NEXT"), sent);
  }

  @Test
  public void corruptSingleFragmentSetupPassesUntracked() {
    int streamId = 1;
    ByteBuf corrupt = alloc.buffer().writeBytes(new byte[] {(byte) 0xFF, 0x00, 0x01});
    receive(
        RequestChannelFrameCodec.encode(alloc, streamId, false, false, 1, corrupt, alloc.buffer()));

    sendPayload(streamId, FLAGS_N);
    sendPayload(streamId, FLAGS_N);

    assertEquals(Arrays.asList("REQUEST_CHANNEL"), passed.subList(0, 1));
    assertEquals(Arrays.asList("NEXT", "NEXT"), sent);
  }

  @Test
  public void receiveEndStopsTracking() {
    int streamId = 1;
    receiveChannel(streamId, RpcKind.SINK, false);
    inbound.tryEmitComplete();

    sendPayload(streamId, FLAGS_N);
    sendPayload(streamId, FLAGS_N);
    connection.sendFrame(streamId, PayloadFrameCodec.encodeComplete(alloc, streamId));

    assertEquals(Arrays.asList("NEXT", "NEXT", "COMPLETE"), sent);
  }

  private void receiveChannel(int streamId, RpcKind kind, boolean complete) {
    receive(
        RequestChannelFrameCodec.encode(
            alloc, streamId, false, complete, 1, metadata(kind), alloc.buffer()));
  }

  private void receive(ByteBuf frame) {
    inbound.tryEmitNext(frame);
  }

  private ByteBuf metadata(RpcKind kind) {
    RequestRpcMetadata.Builder builder =
        new RequestRpcMetadata.Builder().setProtocol(ProtocolId.COMPACT).setName("channel");

    if (kind != null) {
      builder.setKind(kind);
    }

    ByteBuf bytes = alloc.buffer();
    ProtocolUtil.writeCompact(builder.build()::write0, bytes);
    return bytes;
  }

  private StreamRpcError decodeStreamError(String data) {
    ByteBuf buffer = alloc.buffer().writeBytes(data.getBytes(StandardCharsets.UTF_8));

    try {
      return ProtocolUtil.readCompact(StreamRpcError::read0, buffer);
    } finally {
      buffer.release();
    }
  }

  private void sendPayload(int streamId, int flags) {
    connection.sendFrame(
        streamId,
        PayloadFrameCodec.encode(
            alloc,
            streamId,
            (flags & FLAGS_F) != 0,
            (flags & FLAGS_C) != 0,
            (flags & FLAGS_N) != 0,
            null,
            alloc.buffer().writeByte(0)));
  }

  /** Records the type of each frame it sends, then frees the frame. */
  private final class Recorder implements DuplexConnection {
    @Override
    public void sendFrame(int streamId, ByteBuf frame) {
      sent.add(
          FrameHeaderCodec.frameType(frame).name()
              + (FrameHeaderCodec.hasFollows(frame) ? "+F" : ""));

      if (FrameHeaderCodec.frameType(frame) == FrameType.ERROR) {
        sentErrorData.add(ErrorFrameCodec.dataUtf8(frame));
      }

      frame.release();
    }

    @Override
    public Flux<ByteBuf> receive() {
      return inbound.asFlux();
    }

    @Override
    public void sendErrorAndClose(RSocketErrorException errorException) {}

    @Override
    public ByteBufAllocator alloc() {
      return alloc;
    }

    @Override
    public SocketAddress remoteAddress() {
      return null;
    }

    @Override
    public void dispose() {}

    @Override
    public Mono<Void> onClose() {
      return Mono.never();
    }

    @Override
    public boolean isDisposed() {
      return false;
    }
  }
}
