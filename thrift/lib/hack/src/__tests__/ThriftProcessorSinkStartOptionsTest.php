<?hh
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

<<Oncalls('thrift')>>
final class ThriftProcessorSinkStartOptionsTest extends WWWTest {

  <<JKBoolDataProvider('thrift/hack:thrift_use_method_metadata_processor')>>
  public async function testASinkStartsWithItsHandlersLongChunkTimeout(
  ): Awaitable<void> {
    $start_sink = self::mockFunction(gen_start_thrift_sink<>)
      ->mockYield(self::serverSink());
    await self::genProcessSink(new ResponseAndSink(
      example_ResponseStruct::withDefaultValues(),
      self::genFinalResponse<>,
      100,
      900000,
    ));
    expect($start_sink)->wasCalledOnceWith('', 100, 900000);
  }

  <<JKBoolDataProvider('thrift/hack:thrift_use_method_metadata_processor')>>
  public async function testASinkStartsWithItsHandlersOptionsBelowTheDefaults(
  ): Awaitable<void> {
    $start_sink = self::mockFunction(gen_start_thrift_sink<>)
      ->mockYield(self::serverSink());
    await self::genProcessSink(new ResponseAndSink(
      example_ResponseStruct::withDefaultValues(),
      self::genFinalResponse<>,
      7,
      250,
    ));
    expect($start_sink)->wasCalledOnceWith('', 7, 250);
  }

  <<JKBoolDataProvider('thrift/hack:thrift_use_method_metadata_processor')>>
  public async function testASinkWithoutOptionsStartsWithTheDefaults(
  ): Awaitable<void> {
    $start_sink = self::mockFunction(gen_start_thrift_sink<>)
      ->mockYield(self::serverSink());
    await self::genProcessSink(new ResponseAndSink(
      example_ResponseStruct::withDefaultValues(),
      self::genFinalResponse<>,
    ));
    expect($start_sink)->wasCalledOnceWith('', 100, 1000);
  }

  private static function serverSink(): TServerSink {
    $server_sink = mock(nameof TServerSink);
    $server_sink->mockReturn('isClientException', false);
    $server_sink->mockReturn('sendFinalResponse', null);
    return $server_sink;
  }

  private static async function genFinalResponse(
    HH\AsyncGenerator<null, string, void> $_sink,
  ): Awaitable<example_ResponseStruct> {
    return example_ResponseStruct::withDefaultValues();
  }

  private static async function genProcessSink(
    ResponseAndSink<example_ResponseStruct, string, example_ResponseStruct>
      $response_and_sink,
  ): Awaitable<void> {
    self::mockFunction(PHP\hphp_set_error_page<>);
    self::mockFunction(php_root<>)->mockReturn('/var/www');
    self::mockFunction(request_stats_total_wall<>)->mockReturn(0);
    $perf_metadata = mock(nameof PerfMetadata);
    $perf_metadata->mockReturn('setThriftFlush0Duration', null);
    self::mockFunction(PerfMetadata::get<>)->mockReturn($perf_metadata);

    $input = new TMemoryBuffer();
    $args_protocol = new TBinaryProtocol($input);
    ExampleStreamingService_testSink_args::withDefaultValues()
      ->write($args_protocol);
    $args_protocol->writeMessageEnd();
    $output = mock(nameof TMemoryBuffer);
    $output->mockReturn('getBuffer', '');
    $output->mockReturn('resetBuffer', null);

    $handler = mock(nameof ExampleStreamingServiceAsyncIf);
    $handler->mockYield('testSink', $response_and_sink);
    await (new ExampleStreamingServiceAsyncProcessor($handler))->processAsync(
      new TBinaryProtocol($input),
      new TBinaryProtocol($output),
      'testSink',
      1,
    );
  }
}
