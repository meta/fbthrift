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

package com.facebook.thrift.payload;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import org.apache.thrift.ResponseRpcMetadata;
import org.apache.thrift.TApplicationException;
import org.junit.jupiter.api.Test;

public class ServerResponsePayloadTest {

  @Test
  public void createKeepsMetadataWithNullActual() {
    ServerResponsePayload payload =
        ServerResponsePayload.create(
            out -> {},
            new ResponseRpcMetadata.Builder()
                .setOtherMetadata(Collections.singletonMap("k", "v"))
                .build(),
            null,
            false);

    assertFalse(payload.isTApplicationException());
    assertNull(payload.getApplicationException());
    assertEquals("v", payload.getResponseRpcMetadata().getOtherMetadata().get("k"));
  }

  @Test
  public void createWithTApplicationExceptionKeepsActualAndRejectsNull() {
    TApplicationException actual =
        new TApplicationException(TApplicationException.LOADSHEDDING, "busy");
    ServerResponsePayload payload =
        ServerResponsePayload.createWithTApplicationException(
            out -> {}, new ResponseRpcMetadata.Builder().build(), null, false, actual);

    assertTrue(payload.isTApplicationException());
    assertSame(actual, payload.getApplicationException());
    assertThrows(
        NullPointerException.class,
        () ->
            ServerResponsePayload.createWithTApplicationException(
                out -> {}, new ResponseRpcMetadata.Builder().build(), null, false, null));
  }
}
