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

package com.facebook.thrift.client;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Allocates interaction ids from one process-wide counter.
 *
 * <p>An interaction id must be unique per connection. Id zero means no interaction. Ids stay
 * positive until 2^63 - 1 allocations; that bound is not reachable in practice.
 *
 * <p>This matches the C++, Rust, and Go runtimes. All allocate from one process-wide counter.
 */
public final class InteractionIds {
  private static final AtomicLong NEXT_ID = new AtomicLong(0);

  private InteractionIds() {}

  /** Returns the next interaction id. */
  public static long next() {
    return NEXT_ID.incrementAndGet();
  }
}
