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

import com.facebook.thrift.server.OverloadedException;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.thrift.TApplicationException;

/**
 * Header-only overload error encoding for the THeader transports.
 *
 * <p>Both Header transports call {@link #stampResponseHeaders} after the body writer (and its write
 * hooks) ran and the existing response metadata merged, before the response frame is built. Only an
 * actual overload rejection stamps the {@code ex} code and clears the higher-priority error headers
 * that would otherwise override the classification; any other response, including one carrying an
 * arbitrary {@code ex} entry, is left untouched. The reason travels as an {@code int} until here:
 * {@link Integer#toString} at this wire boundary is the only int-to-wire rendering. Nothing here
 * inspects writers, payload identity, or body bytes.
 *
 * <p>Header names follow {@code thrift/lib/cpp2/transport/core/RpcMetadataUtil.h}. Service Router
 * reads the proxied anyex pair first, then anyex, then uex, then ex.
 */
public final class HeaderOverloadError {
  private HeaderOverloadError() {}

  /**
   * Stamps the THeader overload code for an actual overload rejection and clears the
   * higher-priority error headers a write hook may have set. Does nothing for a null or
   * non-overload exception.
   *
   * @param responseHeaders the per-request response headers the frame is built from
   * @param actual the actual application exception the payload carries, or null
   */
  public static void stampResponseHeaders(
      Map<String, String> responseHeaders, @Nullable TApplicationException actual) {
    int reason = OverloadedException.rpcErrorCodeOf(actual);
    if (reason == OverloadedException.NO_RPC_ERROR_CODE) {
      return;
    }
    stripOutrankingHeaders(responseHeaders);
    responseHeaders.put("ex", Integer.toString(reason));
  }

  // Higher-priority error headers cleared on an actual overload response so nothing overrides ex.
  // "ex" itself is kept (then overwritten with the proven reason) and "exm" is inert without uex.
  private static void stripOutrankingHeaders(Map<String, String> responseHeaders) {
    responseHeaders.remove("pex");
    responseHeaders.remove("uex");
    responseHeaders.remove("puex");
    responseHeaders.remove("uexw");
    responseHeaders.remove("puexw");
    responseHeaders.remove("anyex");
    responseHeaders.remove("anyext");
    responseHeaders.remove("panyex");
    responseHeaders.remove("panyext");
  }
}
