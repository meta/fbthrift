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

/**
 * @deprecated Runs the task and propagates nothing. The framework binds the request context for
 *     each handler call; to hand it to another thread, wrap the task with {@link
 *     com.facebook.nifty.core.RequestContextScope#capture(Runnable)}. Kept so callers compiled
 *     against it still link.
 */
@Deprecated
public final class ContextPropRunnable implements Runnable {
  private final Runnable runnable;

  public ContextPropRunnable(Runnable runnable) {
    this.runnable = runnable;
  }

  @Override
  public void run() {
    runnable.run();
  }
}
