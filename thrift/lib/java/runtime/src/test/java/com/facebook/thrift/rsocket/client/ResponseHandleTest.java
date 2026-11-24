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

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.buffer.Unpooled;
import io.rsocket.Payload;
import io.rsocket.util.ByteBufPayload;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Hooks;

class ResponseHandleTest {

  private final List<Object> dropped = Collections.synchronizedList(new ArrayList<>());

  @BeforeEach
  void trackDropped() {
    Hooks.onErrorDropped(dropped::add);
    Hooks.onNextDropped(dropped::add);
  }

  @AfterEach
  void assertNothingDropped() {
    Hooks.resetOnErrorDropped();
    Hooks.resetOnNextDropped();

    assertThat(dropped).isEmpty();
  }

  @Test
  void claimThenCloseIsNoOp() {
    Payload response = responsePayload();
    ResponseHandle handle = new ResponseHandle();

    handle.offer(response);

    Payload claimed = handle.claim();

    claimed.release();

    handle.close();

    assertThat(claimed).isSameAs(response);
    assertThat(response.refCnt()).isEqualTo(1);

    response.release();
  }

  @Test
  void closeThenClaimReturnsNull() {
    Payload response = responsePayload();
    ResponseHandle handle = new ResponseHandle();

    handle.offer(response);
    handle.close();

    assertThat(handle.claim()).isNull();
    assertThat(response.refCnt()).isEqualTo(1);

    response.release();
  }

  @Test
  void offerAfterCloseFrees() {
    Payload response = responsePayload();
    ResponseHandle handle = new ResponseHandle();

    handle.close();
    handle.offer(response);

    assertThat(response.refCnt()).isEqualTo(1);

    response.release();
  }

  @Test
  void doubleCloseIsNoOp() {
    Payload response = responsePayload();
    ResponseHandle handle = new ResponseHandle();

    handle.offer(response);
    handle.close();
    handle.close();

    assertThat(response.refCnt()).isEqualTo(1);

    response.release();
  }

  private static Payload responsePayload() {
    Payload response = ByteBufPayload.create(Unpooled.buffer(1), Unpooled.buffer(1));
    response.retain();
    return response;
  }
}
