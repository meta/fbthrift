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

import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** A timer that holds every timeout until the test expires it. */
final class ManualTimer implements Timer {
  volatile ManualTimeout timeout;
  final List<ManualTimeout> timeouts = new ArrayList<>();
  volatile int created;
  volatile long delay;
  volatile TimeUnit unit;

  @Override
  public Timeout newTimeout(TimerTask task, long delay, TimeUnit unit) {
    this.created++;
    this.delay = delay;
    this.unit = unit;
    this.timeout = new ManualTimeout(this, task);
    this.timeouts.add(timeout);

    return timeout;
  }

  void expire() {
    timeout.expire();
  }

  @Override
  public Set<Timeout> stop() {
    return Collections.emptySet();
  }

  static final class ManualTimeout implements Timeout {
    private final Timer timer;
    private final TimerTask task;
    private volatile boolean cancelled;

    ManualTimeout(Timer timer, TimerTask task) {
      this.timer = timer;
      this.task = task;
    }

    void expire() {
      if (cancelled) {
        return;
      }

      try {
        task.run(this);
      } catch (Exception e) {
        throw new AssertionError(e);
      }
    }

    @Override
    public Timer timer() {
      return timer;
    }

    @Override
    public TimerTask task() {
      return task;
    }

    @Override
    public boolean isExpired() {
      return false;
    }

    @Override
    public boolean isCancelled() {
      return cancelled;
    }

    @Override
    public boolean cancel() {
      cancelled = true;
      return true;
    }
  }
}
