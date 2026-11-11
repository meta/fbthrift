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

package com.facebook.thrift.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

public class ResponseAndStreamTest {

  @Test
  public void mappedViewSharesDisposedFlag() {
    AtomicInteger teardowns = new AtomicInteger();
    ResponseAndStream<String, String> holder =
        ResponseAndStream.of("first", Flux.just("a"), () -> teardowns.incrementAndGet());
    ResponseAndStream<String, String> mapped =
        holder.mapStream(flux -> flux.map(String::toUpperCase));
    assertFalse(holder.isDisposed());
    assertFalse(mapped.isDisposed());
    // Disposing through the mapped view disposes the source too, and the teardown runs once.
    mapped.dispose();
    assertTrue(mapped.isDisposed());
    assertTrue(holder.isDisposed());
    holder.dispose();
    assertEquals(1, teardowns.get());
  }
}
