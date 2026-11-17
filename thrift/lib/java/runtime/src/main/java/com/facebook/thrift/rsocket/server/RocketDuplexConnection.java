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

import static com.facebook.thrift.rsocket.util.RocketErrorUtil.serverError;

import com.facebook.thrift.protocol.ProtocolUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.DuplexConnection;
import io.rsocket.RSocketErrorException;
import io.rsocket.exceptions.CanceledException;
import io.rsocket.frame.CancelFrameCodec;
import io.rsocket.frame.ErrorFrameCodec;
import io.rsocket.frame.FrameHeaderCodec;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.RequestChannelFrameCodec;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.thrift.RequestRpcMetadata;
import org.apache.thrift.ResponseRpcErrorCode;
import org.apache.thrift.RpcKind;
import org.apache.thrift.StreamRpcError;
import org.apache.thrift.StreamRpcErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Frames channel traffic where a Thrift peer differs from rsocket-java, in the one place that sees
 * every frame of a connection. It completes a sink's final response in one frame and ends one half
 * of a bidi call without ending the other.
 */
public final class RocketDuplexConnection implements DuplexConnection {
  private static final Logger logger = LoggerFactory.getLogger(RocketDuplexConnection.class);

  // A frame starts with the stream id, then the type and flags.
  private static final int TYPE_AND_FLAGS_OFFSET = Integer.BYTES;

  private final DuplexConnection delegate;

  /**
   * The payloads sent on the stream of each open sink: the second one is the final response, and it
   * gets COMPLETE. Frames carry only a stream id, and only the REQUEST_CHANNEL metadata names the
   * kind, so the count is kept by stream id.
   */
  private final Map<Integer, AtomicInteger> sinkPayloadCount = new ConcurrentHashMap<>();

  /**
   * Whether each open bidi call sent its first response. Only after it does the server end the
   * other half for the peer, because before it the peer ends the whole call itself. The flag is
   * kept by stream id for the same reason as the sink count.
   */
  private final Map<Integer, AtomicBoolean> bidiFirstResponseSent = new ConcurrentHashMap<>();

  private RocketDuplexConnection(DuplexConnection delegate) {
    this.delegate = delegate;
  }

  public static DuplexConnection wrap(DuplexConnection connection) {
    return new RocketDuplexConnection(connection);
  }

  @Override
  public void sendFrame(int streamId, ByteBuf frame) {
    AtomicInteger payloadCount = sinkPayloadCount.get(streamId);
    if (payloadCount != null) {
      sendSinkFrame(streamId, frame, payloadCount);
      return;
    }

    AtomicBoolean firstResponseSent = bidiFirstResponseSent.get(streamId);
    if (firstResponseSent != null) {
      sendBidiFrame(streamId, frame, firstResponseSent);
      return;
    }

    delegate.sendFrame(streamId, frame);
  }

  /** Puts COMPLETE on the final response, the second payload, and drops the COMPLETE after it. */
  private void sendSinkFrame(int streamId, ByteBuf frame, AtomicInteger payloadCount) {
    FrameType type = FrameHeaderCodec.frameType(frame);

    if (isLastFragment(type, frame)) {
      if (payloadCount.incrementAndGet() == 2) {
        int headerOffset = frame.readerIndex() + TYPE_AND_FLAGS_OFFSET;

        frame.setShort(headerOffset, frame.getShort(headerOffset) | FrameHeaderCodec.FLAGS_C);
      }
    } else if (ends(type)) {
      sinkPayloadCount.remove(streamId);

      if (type == FrameType.COMPLETE && payloadCount.get() == 2) {
        frame.release();
        return;
      }
    }

    delegate.sendFrame(streamId, frame);
  }

  /**
   * Marks the first response of an open bidi call. A server ERROR after it ends only the stream
   * half on the peer, so a CANCEL follows it to end the sink half. A server COMPLETE stops the
   * tracking, so the peer's later sink frames pass unchanged.
   */
  private void sendBidiFrame(int streamId, ByteBuf frame, AtomicBoolean firstResponseSent) {
    FrameType type = FrameHeaderCodec.frameType(frame);

    if (isLastFragment(type, frame)) {
      firstResponseSent.set(true);
    } else if (ends(type)) {
      bidiFirstResponseSent.remove(streamId);

      if (type == FrameType.ERROR && firstResponseSent.get()) {
        delegate.sendFrame(streamId, frame);
        delegate.sendFrame(streamId, CancelFrameCodec.encode(alloc(), streamId));
        return;
      }
    }

    delegate.sendFrame(streamId, frame);
  }

  @Override
  public Flux<ByteBuf> receive() {
    return delegate
        .receive()
        .<ByteBuf>handle(
            (frame, sink) -> {
              boolean pass;

              try {
                pass = admit(frame);
              } catch (Throwable t) {
                sink.next(frame);
                return;
              }

              if (pass) {
                sink.next(frame);
              }
            })
        .doFinally(
            signal -> {
              sinkPayloadCount.clear();
              bidiFirstResponseSent.clear();
            });
  }

  /** Tracks the channels the peer opens and passes every frame upstream except a rejected setup. */
  private boolean admit(ByteBuf frame) {
    int streamId = FrameHeaderCodec.streamId(frame);
    FrameType type = FrameHeaderCodec.frameType(frame);

    if (type == FrameType.REQUEST_CHANNEL) {
      return admitChannel(streamId, frame);
    }

    if (sinkPayloadCount.containsKey(streamId)) {
      admitSink(streamId, type);
    } else if (bidiFirstResponseSent.containsKey(streamId)) {
      admitBidi(streamId, type);
    }

    return true;
  }

  /**
   * Starts to track a sink or a bidi call. A setup whose metadata does not fit in its first
   * fragment is rejected: its kind is unknown until upstream reassembles the setup.
   */
  private boolean admitChannel(int streamId, ByteBuf frame) {
    RpcKind kind = kind(frame);

    if (kind == null && FrameHeaderCodec.hasFollows(frame)) {
      logger.warn(
          "Rejecting channel setup on stream {}: metadata spans fragments, kind unknown", streamId);

      sendError(streamId, ResponseRpcErrorCode.REQUEST_PARSING_FAILURE, "Metadata spans fragments");
      return false;
    }

    if (kind == RpcKind.SINK) {
      sinkPayloadCount.put(streamId, new AtomicInteger());
    } else if (kind == RpcKind.BIDIRECTIONAL_STREAM && !FrameHeaderCodec.hasComplete(frame)) {
      bidiFirstResponseSent.put(streamId, new AtomicBoolean());
    }

    return true;
  }

  /** A peer ERROR or CANCEL ends a sink; its own frames need no change after that. */
  private void admitSink(int streamId, FrameType type) {
    if (type == FrameType.ERROR || type == FrameType.CANCEL) {
      sinkPayloadCount.remove(streamId);
    }
  }

  /**
   * Ends the other half on the peer when it ends one half of a bidi call after the first response,
   * because upstream ends both halves.
   */
  private void admitBidi(int streamId, FrameType type) {
    if (type == FrameType.COMPLETE || type == FrameType.NEXT_COMPLETE) {
      bidiFirstResponseSent.remove(streamId);
    } else if (type == FrameType.ERROR || type == FrameType.CANCEL) {
      AtomicBoolean firstResponseSent = bidiFirstResponseSent.remove(streamId);

      if (firstResponseSent != null && firstResponseSent.get()) {
        endOtherHalf(streamId, type);
      }
    }
  }

  /** A peer sink ERROR ends the stream half with an error; a peer CANCEL ends the sink half. */
  private void endOtherHalf(int streamId, FrameType peerEnd) {
    if (peerEnd == FrameType.ERROR) {
      sendStreamError(
          streamId, StreamRpcErrorCode.SERVER_CLOSING_CONNECTION, "Peer sink error ends the call");
    } else {
      delegate.sendFrame(streamId, CancelFrameCodec.encode(alloc(), streamId));
    }
  }

  /** The frame that ends one payload: a NEXT without FOLLOWS. */
  private static boolean isLastFragment(FrameType type, ByteBuf frame) {
    return type == FrameType.NEXT && !FrameHeaderCodec.hasFollows(frame);
  }

  private static boolean ends(FrameType type) {
    return type == FrameType.COMPLETE || type == FrameType.ERROR || type == FrameType.CANCEL;
  }

  /**
   * Reads the kind of a channel setup. Unclassifiable setups pass through for the server's setup
   * decoding to reject.
   */
  private static RpcKind kind(ByteBuf frame) {
    try {
      return ProtocolUtil.readCompact(
              RequestRpcMetadata::read0, RequestChannelFrameCodec.metadata(frame))
          .getKind();
    } catch (RuntimeException e) {
      return null;
    }
  }

  private void sendError(int streamId, ResponseRpcErrorCode code, String message) {
    delegate.sendFrame(
        streamId, ErrorFrameCodec.encode(alloc(), streamId, serverError(code, message)));
  }

  private void sendStreamError(int streamId, StreamRpcErrorCode code, String message) {
    StreamRpcError error =
        new StreamRpcError.Builder()
            .setNameUtf8(code.name())
            .setWhatUtf8(message)
            .setCode(code)
            .build();

    ByteBuf buffer = alloc().buffer();

    try {
      ProtocolUtil.writeCompact(error::write0, buffer);
      String encoded = buffer.toString(StandardCharsets.UTF_8);

      delegate.sendFrame(
          streamId, ErrorFrameCodec.encode(alloc(), streamId, new CanceledException(encoded)));
    } finally {
      buffer.release();
    }
  }

  @Override
  public void sendErrorAndClose(RSocketErrorException errorException) {
    delegate.sendErrorAndClose(errorException);
  }

  @Override
  public ByteBufAllocator alloc() {
    return delegate.alloc();
  }

  @Override
  public SocketAddress remoteAddress() {
    return delegate.remoteAddress();
  }

  @Override
  public void dispose() {
    delegate.dispose();
  }

  @Override
  public Mono<Void> onClose() {
    return delegate.onClose();
  }

  @Override
  public boolean isDisposed() {
    return delegate.isDisposed();
  }
}
