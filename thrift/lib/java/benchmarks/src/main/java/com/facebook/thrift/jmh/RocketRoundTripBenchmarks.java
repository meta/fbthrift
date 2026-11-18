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

import com.facebook.swift.service.ThriftServerConfig;
import com.facebook.thrift.client.RpcClientFactory;
import com.facebook.thrift.client.ThriftClientConfig;
import com.facebook.thrift.example.ping.PingRequest;
import com.facebook.thrift.example.ping.PingResponse;
import com.facebook.thrift.example.ping.PingService;
import com.facebook.thrift.example.ping.PingServiceRpcServerHandler;
import com.facebook.thrift.rsocket.server.RSocketServerTransport;
import com.facebook.thrift.rsocket.server.RSocketServerTransportFactory;
import com.facebook.thrift.server.RpcServerHandler;
import io.airlift.units.Duration;
import io.netty.handler.ssl.ClientAuth;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.thrift.ProtocolId;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Round trips through a real Rocket server over loopback: one blocking request-response per op, and
 * one stream of 1000 small elements per op. Every frame passes through the server {@code
 * DuplexConnection}, so this measures what {@code RocketDuplexConnection} costs per request.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(3)
public class RocketRoundTripBenchmarks {
  private static final PingRequest REQUEST = new PingRequest.Builder().setRequest("ping").build();
  private static final int STREAM_ELEMENTS = 1000;

  private RSocketServerTransport transport;
  private PingService.Reactive pingService;

  @Setup(Level.Trial)
  public void setup() {
    RpcServerHandler serverHandler =
        new PingServiceRpcServerHandler(new PingServiceImpl(), Collections.emptyList());

    RSocketServerTransportFactory transportFactory =
        new RSocketServerTransportFactory(
            new ThriftServerConfig()
                .setClientAuth(ClientAuth.OPTIONAL)
                .setSslEnabled(false)
                .setEnableJdkSsl(false));
    transport = transportFactory.createServerTransport(serverHandler).block();
    InetSocketAddress address = (InetSocketAddress) transport.getAddress();

    RpcClientFactory clientFactory =
        RpcClientFactory.builder()
            .setDisableLoadBalancing(true)
            .setDisableRSocket(false)
            .setThriftClientConfig(
                new ThriftClientConfig()
                    .setDisableSSL(true)
                    .setRequestTimeout(Duration.succinctDuration(1, TimeUnit.DAYS)))
            .build();

    pingService =
        PingService.Reactive.clientBuilder()
            .setProtocolId(ProtocolId.BINARY)
            .build(clientFactory, address);
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    if (transport != null) {
      transport.dispose();
      transport = null;
    }
  }

  @Benchmark
  public void requestResponse(Blackhole bh) {
    PingResponse response = pingService.ping(REQUEST).block();
    bh.consume(response);
  }

  @Benchmark
  public void stream1000(Blackhole bh) {
    Object last = pingService.streamOfPings(REQUEST, STREAM_ELEMENTS).blockLast();
    bh.consume(last);
  }

  private static class PingServiceImpl implements PingService.Reactive {
    @Override
    public void dispose() {}

    @Override
    public Mono<PingResponse> ping(PingRequest pingRequest) {
      return Mono.just(new PingResponse.Builder().setResponse("pong").build());
    }

    @Override
    public Mono<byte[]> pingBinary(PingRequest pingRequest) {
      return Mono.just("hello!".getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Mono<PingResponse> pingException(PingRequest pingRequest) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Mono<Void> pingVoid(PingRequest pingRequest) {
      return Mono.empty();
    }

    @Override
    public Flux<PingResponse> streamOfPings(PingRequest request, int numberOfPings) {
      return Flux.range(0, numberOfPings)
          .map(i -> new PingResponse.Builder().setResponse("pong").build());
    }
  }

  public static void main(String... args) throws Exception {
    Options opt =
        new OptionsBuilder()
            .parent(new CommandLineOptions(args))
            .include(RocketRoundTripBenchmarks.class.getSimpleName())
            .mode(Mode.Throughput)
            .forks(3)
            .warmupIterations(5)
            .warmupTime(new TimeValue(1000, TimeUnit.MILLISECONDS))
            .measurementIterations(10)
            .measurementTime(new TimeValue(1000, TimeUnit.MILLISECONDS))
            .jvmArgsAppend(
                "-Dio.netty.leakDetectionLevel=disabled",
                "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
                "-Dio.netty.tryReflectionSetAccessible=true")
            .build();
    new Runner(opt).run();
  }
}
