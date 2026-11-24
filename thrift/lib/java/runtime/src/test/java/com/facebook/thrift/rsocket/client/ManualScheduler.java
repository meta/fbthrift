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

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;

/** A scheduler that queues tasks until the test runs them, dropping queued tasks on dispose. */
final class ManualScheduler implements Scheduler {
  final Queue<Runnable> tasks = new ConcurrentLinkedQueue<>();

  void runAll() {
    Runnable task;

    while ((task = tasks.poll()) != null) {
      task.run();
    }
  }

  @Override
  public Disposable schedule(Runnable task) {
    tasks.add(task);

    return () -> tasks.remove(task);
  }

  @Override
  public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
    return schedule(task);
  }

  @Override
  public Disposable schedulePeriodically(
      Runnable task, long initialDelay, long period, TimeUnit unit) {
    return schedule(task);
  }

  @Override
  public Worker createWorker() {
    return new Worker() {
      @Override
      public Disposable schedule(Runnable task) {
        return ManualScheduler.this.schedule(task);
      }

      @Override
      public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
        return schedule(task);
      }

      @Override
      public Disposable schedulePeriodically(
          Runnable task, long initialDelay, long period, TimeUnit unit) {
        return schedule(task);
      }

      @Override
      public void dispose() {
        tasks.clear();
      }

      @Override
      public boolean isDisposed() {
        return false;
      }
    };
  }

  @Override
  public void dispose() {
    tasks.clear();
  }

  @Override
  public boolean isDisposed() {
    return false;
  }
}
