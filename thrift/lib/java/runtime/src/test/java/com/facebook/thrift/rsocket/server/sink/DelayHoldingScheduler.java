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

package com.facebook.thrift.rsocket.server.sink;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.scheduler.Scheduler;

/** A scheduler that runs immediate tasks inline and holds delayed ones until the test runs them. */
final class DelayHoldingScheduler implements Scheduler {
  final List<Runnable> delayed = new CopyOnWriteArrayList<>();

  @Override
  public Disposable schedule(Runnable task) {
    task.run();

    return Disposables.disposed();
  }

  @Override
  public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
    delayed.add(task);

    return Disposables.disposed();
  }

  void runDelayed() {
    delayed.forEach(Runnable::run);
  }

  @Override
  public Scheduler.Worker createWorker() {
    throw new UnsupportedOperationException();
  }
}
