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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.facebook.thrift.rsocket.server.sink.SinkPayloadSubscriber;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import reactor.util.context.ContextView;

public class SinkOptionsTest {
  @Test
  public void defaultsAreNoTimeoutAndAWindowOfOneHundred() {
    SinkOptions options = SinkOptions.builder().build();

    assertEquals(Duration.ZERO, options.getChunkTimeout());
    assertEquals(100, options.getBufferSize());
  }

  @Test
  public void negativeChunkTimeoutIsRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> SinkOptions.builder().chunkTimeout(Duration.ofMillis(-1)));
  }

  @Test
  public void subMillisecondChunkTimeoutRoundsUpToOneMillisecond() {
    SinkOptions options = SinkOptions.builder().chunkTimeout(Duration.ofNanos(1)).build();

    assertEquals(Duration.ofMillis(1), options.getChunkTimeout());
  }

  @Test
  public void zeroChunkTimeoutDisables() {
    SinkOptions options =
        SinkOptions.builder()
            .chunkTimeout(Duration.ofSeconds(5))
            .chunkTimeout(Duration.ZERO)
            .build();

    assertTrue(options.getChunkTimeout().isZero());
  }

  @Test
  public void nonPositiveBufferSizeIsRejected() {
    assertThrows(IllegalArgumentException.class, () -> SinkOptions.builder().bufferSize(0));
    assertThrows(IllegalArgumentException.class, () -> SinkOptions.builder().bufferSize(-1));
  }

  @Test
  public void oversizedBufferSizeIsRejected() {
    assertThrows(
        IllegalArgumentException.class, () -> SinkOptions.builder().bufferSize((1 << 20) + 1));
  }

  @Test
  public void bufferSizeIsKept() {
    SinkOptions options = SinkOptions.builder().bufferSize(256).build();

    assertEquals(256, options.getBufferSize());
  }

  @Test
  public void bufferSizeAtTheCapIsAccepted() {
    SinkOptions options = SinkOptions.builder().bufferSize(1 << 20).build();

    assertEquals(1 << 20, options.getBufferSize());
  }

  @Test
  public void applyToWritesTheOptionsIntoTheContext() {
    SinkOptions options =
        SinkOptions.builder().chunkTimeout(Duration.ofSeconds(5)).bufferSize(256).build();

    AtomicReference<ContextView> seen = new AtomicReference<>();

    StepVerifier.create(
            Flux.deferContextual(
                    context -> {
                      seen.set(context);

                      return Flux.just("a");
                    })
                .transform(options::applyTo))
        .expectNext("a")
        .verifyComplete();

    assertSame(options, seen.get().get(SinkPayloadSubscriber.SINK_OPTIONS));
  }
}
