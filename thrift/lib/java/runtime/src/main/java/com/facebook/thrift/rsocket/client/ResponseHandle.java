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

package com.facebook.thrift.rsocket.client;

import io.netty.util.ReferenceCountUtil;
import io.rsocket.Payload;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Holds one response buffer across the hop. Claim (decode) and close (cancel or release path) both
 * take the cell with getAndSet, so exactly one side frees the buffer. Once closed, a payload
 * arriving late frees itself: rsocket can deliver a response after the call ended.
 */
final class ResponseHandle {
  private final AtomicReference<Payload> payload = new AtomicReference<>();
  private volatile boolean closed;

  void offer(Payload response) {
    payload.set(response);

    // A close that already ran leaves closed set, so free a late arrival here.
    if (closed) {
      release();
    }
  }

  Payload claim() {
    return payload.getAndSet(null);
  }

  void close() {
    closed = true;

    release();
  }

  private void release() {
    Payload response = payload.getAndSet(null);

    if (response != null) {
      ReferenceCountUtil.safeRelease(response);
    }
  }
}
