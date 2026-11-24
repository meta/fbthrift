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

package com.facebook.thrift.util.resources;

import java.util.HashSet;
import java.util.Set;

/**
 * @deprecated The framework binds the whole {@link com.facebook.nifty.core.RequestContext} for each
 *     handler call and carries it on the Reactor context, so there are no keys to register and no
 *     hooks to install. Every method here does nothing; the class is kept so callers compiled
 *     against it still link. Hand work to another thread with {@link
 *     com.facebook.nifty.core.RequestContextScope#capture} or {@link
 *     com.facebook.nifty.core.RequestContextScope#wrap} instead.
 */
@Deprecated
public class ContextPropagationRegistry {
  private ContextPropagationRegistry() {}

  /** Does nothing. */
  public static void registerContextPropagationKey(String key) {}

  /** Always empty. Each call returns a new set, so a caller that changes it changes nothing. */
  public static Set<String> getContextPropagationKeys() {
    return new HashSet<>();
  }

  /** Always false. */
  public static boolean isContextPropEnabled() {
    return false;
  }
}
