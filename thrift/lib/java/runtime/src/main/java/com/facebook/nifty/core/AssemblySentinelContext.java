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

package com.facebook.nifty.core;

import java.time.Duration;
import java.util.Iterator;
import java.util.Map;

/**
 * Stand-ins for a {@link RequestContext} where none exists. Every accessor throws rather than
 * answering with an absent or stale context.
 *
 * <p>A top-level class rather than a member of {@code RequestContextScope} because that class is
 * replaced wholesale by its Java 25 multi-release override; holding the instances here keeps a
 * single identity that both variants can publish and compare against.
 */
final class AssemblySentinelContext implements RequestContext {

  static final RequestContext INSTANCE =
      new AssemblySentinelContext(
          "No request context exists while a reactive handler assembles its pipeline. "
              + "Use RequestContexts.currentContext() to read the context at subscribe time.");

  /**
   * Distinguishes "explicitly no context" from "nothing recorded here", which a plain absent entry
   * cannot. Readers translate it back to null; it must never be handed to a caller.
   */
  static final RequestContext CLEARED =
      new AssemblySentinelContext(
          "No request context is bound. This marker should never be handed to a caller.");

  private final String reason;

  private AssemblySentinelContext(String reason) {
    this.reason = reason;
  }

  @Override
  public ConnectionContext getConnectionContext() {
    throw notARequest();
  }

  @Override
  public void setContextData(String key, Object val) {
    throw notARequest();
  }

  @Override
  public Object getContextData(String key) {
    throw notARequest();
  }

  @Override
  public void clearContextData(String key) {
    throw notARequest();
  }

  @Override
  public Iterator<Map.Entry<String, Object>> contextDataIterator() {
    throw notARequest();
  }

  @Override
  public Map<String, String> getRequestHeader() {
    throw notARequest();
  }

  @Override
  public void setResponseHeader(String key, String value) {
    throw notARequest();
  }

  @Override
  public Duration getClientTimeout() {
    throw notARequest();
  }

  private IllegalStateException notARequest() {
    return new IllegalStateException(reason);
  }
}
