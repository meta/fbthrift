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

package com.facebook.thrift.server.sink;

import com.facebook.thrift.rsocket.server.sink.SinkPayloadSubscriber;
import java.time.Duration;
import java.util.Objects;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.util.context.Context;

/**
 * Settings a sink or bidi handler applies to the sink it reads. The transport owns the settings:
 * the handler only hands them upstream, so a stalled peer ends the call even when the handler
 * blocks.
 */
public final class SinkOptions {
  /** Default credit window: at most this many unread sink payloads wait for the handler. */
  static final int DEFAULT_BUFFER_SIZE = 100;

  /** Largest credit window: the transport buffers at most this many unread sink payloads. */
  static final int MAX_BUFFER_SIZE = 1 << 20;

  private final Duration chunkTimeout;
  private final int bufferSize;

  private SinkOptions(Duration chunkTimeout, int bufferSize) {
    this.chunkTimeout = chunkTimeout;
    this.bufferSize = bufferSize;
  }

  /** Returns a builder with the defaults: no chunk timeout and a window of 100 payloads. */
  public static Builder builder() {
    return new Builder();
  }

  /** Returns the idle limit for the sink: no chunk may take longer once credit is outstanding. */
  public Duration getChunkTimeout() {
    return chunkTimeout;
  }

  /** Returns how many unread sink payloads the transport buffers before it stops asking. */
  public int getBufferSize() {
    return bufferSize;
  }

  /**
   * Applies these options to the sink a handler reads. The options travel in the Reactor Context,
   * so they cross only Reactor operators: assembling the sink with anything else drops them. When
   * several handlers wrap one sink, the application nearest the sink wins.
   *
   * @param <T> the sink element type
   * @param sink the sink the handler reads
   * @return the sink with these options attached
   */
  public <T> Flux<T> applyTo(Publisher<T> sink) {
    Objects.requireNonNull(sink, "sink");

    return Flux.from(sink).contextWrite(Context.of(SinkPayloadSubscriber.SINK_OPTIONS, this));
  }

  /** Builds {@link SinkOptions} with the defaults for every unset field. */
  public static final class Builder {
    private Duration chunkTimeout = Duration.ZERO;
    private int bufferSize = DEFAULT_BUFFER_SIZE;

    private Builder() {}

    /**
     * Bounds how long one chunk may stay away while the peer holds unused credit. Zero disables the
     * timeout; a sub-millisecond timeout rounds up to one millisecond, which is normalization
     * rather than precision. Time the server keeps a chunk queued counts too, so a stall is loud
     * and retryable.
     *
     * @param chunkTimeout the idle limit, never negative
     * @return this builder
     */
    public Builder chunkTimeout(Duration chunkTimeout) {
      Objects.requireNonNull(chunkTimeout, "chunkTimeout");

      if (chunkTimeout.isNegative()) {
        throw new IllegalArgumentException(
            "chunkTimeout must be non-negative, was: " + chunkTimeout);
      }

      if (chunkTimeout.isZero()) {
        this.chunkTimeout = Duration.ZERO;
      } else {
        this.chunkTimeout = Duration.ofMillis(Math.max(chunkTimeout.toMillis(), 1));
      }

      return this;
    }

    /**
     * Sets how many unread sink payloads the transport buffers before it stops asking the peer. The
     * window refills when 75% drained. The hand-off preallocates its queue per call, sized to the
     * next power of two, so a value near 2^20 costs megabytes even for an idle sink.
     *
     * @param bufferSize the credit window, from 1 to 2^20
     * @return this builder
     */
    public Builder bufferSize(int bufferSize) {
      if (bufferSize <= 0 || bufferSize > MAX_BUFFER_SIZE) {
        throw new IllegalArgumentException(
            "bufferSize must be positive and at most " + MAX_BUFFER_SIZE + ", was: " + bufferSize);
      }

      this.bufferSize = bufferSize;

      return this;
    }

    /** Returns the options. */
    public SinkOptions build() {
      return new SinkOptions(chunkTimeout, bufferSize);
    }
  }
}
