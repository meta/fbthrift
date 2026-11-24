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

package com.facebook.thrift.client;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;

/** A scheduler that counts schedule calls and runs them on a delegate. */
public final class CountingScheduler implements Scheduler {
  private final Scheduler delegate;
  private final AtomicInteger schedules = new AtomicInteger();

  public CountingScheduler(Scheduler delegate) {
    this.delegate = delegate;
  }

  public int schedules() {
    return schedules.get();
  }

  @Override
  public Disposable schedule(Runnable task) {
    schedules.incrementAndGet();

    return delegate.schedule(task);
  }

  @Override
  public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
    schedules.incrementAndGet();

    return delegate.schedule(task, delay, unit);
  }

  @Override
  public Disposable schedulePeriodically(
      Runnable task, long initialDelay, long period, TimeUnit unit) {
    schedules.incrementAndGet();

    return delegate.schedulePeriodically(task, initialDelay, period, unit);
  }

  @Override
  public Worker createWorker() {
    Worker worker = delegate.createWorker();

    return new Worker() {
      @Override
      public Disposable schedule(Runnable task) {
        schedules.incrementAndGet();

        return worker.schedule(task);
      }

      @Override
      public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
        schedules.incrementAndGet();

        return worker.schedule(task, delay, unit);
      }

      @Override
      public Disposable schedulePeriodically(
          Runnable task, long initialDelay, long period, TimeUnit unit) {
        schedules.incrementAndGet();

        return worker.schedulePeriodically(task, initialDelay, period, unit);
      }

      @Override
      public void dispose() {
        worker.dispose();
      }

      @Override
      public boolean isDisposed() {
        return worker.isDisposed();
      }
    };
  }

  @Override
  public void dispose() {
    delegate.dispose();
  }

  @Override
  public boolean isDisposed() {
    return delegate.isDisposed();
  }
}
