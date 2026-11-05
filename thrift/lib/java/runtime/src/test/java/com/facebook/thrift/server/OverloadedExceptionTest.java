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

package com.facebook.thrift.server;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.thrift.TApplicationException;
import org.junit.jupiter.api.Test;

public class OverloadedExceptionTest {

  @Test
  public void overloadReasonsAndClassifierMatrix() {
    assertEquals(1, OverloadedException.HOST_OVERLOAD);
    assertEquals(5, OverloadedException.QUEUE_OVERLOAD);
    assertEquals(22, OverloadedException.APP_OVERLOAD);
    assertEquals(-1, OverloadedException.NO_RPC_ERROR_CODE);
    assertEquals(
        OverloadedException.HOST_OVERLOAD, OverloadedException.host("m").getRpcErrorCode());
    assertEquals(
        OverloadedException.QUEUE_OVERLOAD, OverloadedException.queue("m").getRpcErrorCode());
    assertEquals(
        OverloadedException.APP_OVERLOAD, OverloadedException.application("m").getRpcErrorCode());
    assertEquals(TApplicationException.LOADSHEDDING, OverloadedException.host("m").getType());
    assertEquals(1, OverloadedException.rpcErrorCodeOf(OverloadedException.host("m")));
    assertEquals(5, OverloadedException.rpcErrorCodeOf(OverloadedException.queue("m")));
    assertEquals(22, OverloadedException.rpcErrorCodeOf(OverloadedException.application("m")));
    assertEquals(
        22,
        OverloadedException.rpcErrorCodeOf(
            new TApplicationException(TApplicationException.LOADSHEDDING, "busy")));
    assertEquals(
        OverloadedException.NO_RPC_ERROR_CODE,
        OverloadedException.rpcErrorCodeOf(
            new TApplicationException(TApplicationException.UNKNOWN_METHOD, "nope")));
    assertEquals(OverloadedException.NO_RPC_ERROR_CODE, OverloadedException.rpcErrorCodeOf(null));
  }
}
