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

package com.facebook.thrift.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.facebook.thrift.server.OverloadedException;
import java.util.HashMap;
import java.util.Map;
import org.apache.thrift.TApplicationException;
import org.junit.jupiter.api.Test;

public class HeaderOverloadErrorTest {

  @Test
  public void bareLoadsheddingStampsAppCode() {
    Map<String, String> responseHeaders = new HashMap<>();
    responseHeaders.put("k", "v");
    responseHeaders.put("uex", "EvilException");

    HeaderOverloadError.stampResponseHeaders(
        responseHeaders, new TApplicationException(TApplicationException.LOADSHEDDING, "busy"));

    assertEquals("22", responseHeaders.get("ex"));
    assertEquals("v", responseHeaders.get("k"));
    assertFalse(responseHeaders.containsKey("uex"));
  }

  @Test
  public void typedHostOverloadStampsHostCode() {
    Map<String, String> responseHeaders = new HashMap<>();

    HeaderOverloadError.stampResponseHeaders(responseHeaders, OverloadedException.host("shed"));

    assertEquals("1", responseHeaders.get("ex"));
    assertEquals(1, responseHeaders.size());
  }

  @Test
  public void stripsOutrankingErrorHeaders() {
    Map<String, String> responseHeaders = errorFamilyHeaders();

    HeaderOverloadError.stampResponseHeaders(
        responseHeaders, new TApplicationException(TApplicationException.LOADSHEDDING, "busy"));

    assertEquals("22", responseHeaders.get("ex"));
    assertFalse(responseHeaders.containsKey("pex"));
    assertFalse(responseHeaders.containsKey("uex"));
    assertFalse(responseHeaders.containsKey("puex"));
    assertFalse(responseHeaders.containsKey("uexw"));
    assertFalse(responseHeaders.containsKey("puexw"));
    assertFalse(responseHeaders.containsKey("anyex"));
    assertFalse(responseHeaders.containsKey("anyext"));
    assertFalse(responseHeaders.containsKey("panyex"));
    assertFalse(responseHeaders.containsKey("panyext"));
  }

  @Test
  public void leavesNonOverloadUntouched() {
    Map<String, String> ordinaryHeaders = new HashMap<>();
    ordinaryHeaders.put("ex", "9");
    ordinaryHeaders.put("k", "v");

    HeaderOverloadError.stampResponseHeaders(
        ordinaryHeaders, new TApplicationException(TApplicationException.INTERNAL_ERROR, "boom"));
    assertEquals("9", ordinaryHeaders.get("ex"));
    assertEquals("v", ordinaryHeaders.get("k"));

    Map<String, String> nullHeaders = new HashMap<>();
    nullHeaders.put("ex", "9");
    HeaderOverloadError.stampResponseHeaders(nullHeaders, null);
    assertEquals("9", nullHeaders.get("ex"));
    assertEquals(1, nullHeaders.size());
  }

  private static Map<String, String> errorFamilyHeaders() {
    Map<String, String> headers = new HashMap<>();
    headers.put("uex", "HookException");
    headers.put("puex", "HookException");
    headers.put("uexw", "hook");
    headers.put("puexw", "hook");
    headers.put("anyex", "hook");
    headers.put("anyext", "hook");
    headers.put("panyex", "hook");
    headers.put("panyext", "hook");
    headers.put("pex", "1");
    return headers;
  }
}
