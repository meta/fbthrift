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

package com.facebook.thrift.jmh;

import com.facebook.swift.service.ContextChain;
import com.facebook.swift.service.StreamEventHandler;
import com.facebook.swift.service.ThriftEventHandler;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

/**
 * Cost of the server-side stream hook fan-out per element: a chain with one no-op stream handler
 * against a chain with a plain handler (no stream section). Throughput here is elements/s; pair
 * with {@code -prof gc} for alloc/op.
 *
 * <p>Plus the per-request chain cost: construct + {@code done()} with two plain handlers and with
 * none. Chains without stream handlers must allocate nothing per request (shared empty index array,
 * no lock, updater CAS instead of an AtomicBoolean).
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class StreamEventHookBenchmarks {

  private ContextChain oneHook;
  private ContextChain noHook;

  @Setup
  public void setup() {
    oneHook = new ContextChain(ImmutableList.of(new StreamEventHandler() {}), "bench", null);
    noHook = new ContextChain(ImmutableList.of(new ThriftEventHandler() {}), "bench", null);
  }

  @Benchmark
  public void elementOneHook() {
    oneHook.onStreamNext();
  }

  @Benchmark
  public void elementNoHook() {
    noHook.onStreamNext();
  }

  /** Shared chain for the contended variant: two threads hammering one chain's monitor. */
  @State(Scope.Benchmark)
  public static class SharedChain {
    ContextChain chain;

    @Setup
    public void setup() {
      chain = new ContextChain(ImmutableList.of(new StreamEventHandler() {}), "bench", null);
    }
  }

  @Benchmark
  @Threads(2)
  public void elementOneHookContended(SharedChain shared) {
    shared.chain.onStreamNext();
  }

  private final List<ThriftEventHandler> twoPlain =
      ImmutableList.of(new ThriftEventHandler() {}, new ThriftEventHandler() {});
  private final List<ThriftEventHandler> none = ImmutableList.of();

  @Benchmark
  public void chainConstructDoneTwoPlain(Blackhole bh) {
    ContextChain chain = new ContextChain(twoPlain, "bench", null);
    chain.done();
    bh.consume(chain);
  }

  @Benchmark
  public void chainConstructDoneNone(Blackhole bh) {
    ContextChain chain = new ContextChain(none, "bench", null);
    chain.done();
    bh.consume(chain);
  }

  public static void main(String... args) throws Exception {
    Options opt =
        new OptionsBuilder()
            .parent(new org.openjdk.jmh.runner.options.CommandLineOptions(args))
            .include(StreamEventHookBenchmarks.class.getSimpleName())
            .mode(Mode.Throughput)
            .forks(1)
            .warmupIterations(5)
            .warmupTime(TimeValue.milliseconds(1000))
            .measurementIterations(5)
            .measurementTime(TimeValue.milliseconds(1000))
            .build();
    new Runner(opt).run();
  }
}
