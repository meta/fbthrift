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
 *
 */

<<Oncalls('thrift')>>
final class ThriftProcessorStreamingEventHandlerTest extends WWWTest {

  const string TEST_STREAMING_SERVICE_NAME = 'TestStreamingService';
  const string TEST_STREAMING_SERVICE_METHOD_NAME = 'testStream';

  private function mockServerStream(): TServerStream {
    $mock_server_stream = mock(nameof TServerStream);
    $mock_server_stream->mockYield('genIsStreamReady', true);
    $mock_server_stream->mockYield('genSendStreamPayload', true);
    $mock_server_stream->mockReturn('sendStreamComplete', null);
    return $mock_server_stream;
  }

  private function mockProtocols(
    TServerStream $server_stream,
  ): shape('input' => TProtocol, 'output' => TProtocol) {
    self::mockFunction(PHP\gen_start_thrift_stream<>)
      ->mockYield($server_stream);
    return $this->mockProtocolsAroundStreamStart();
  }

  private function mockProtocolsAroundStreamStart(
    ?IThriftSyncStruct $args = null,
  ): shape('input' => TProtocol, 'output' => TProtocol) {
    self::mockFunction(PHP\hphp_set_error_page<>);
    self::mockFunction(php_root<>)->mockReturn('/var/www');
    self::mockFunction(request_stats_total_wall<>)->mockReturn(0);

    $mock_perf_metadata = mock(nameof PerfMetadata);
    $mock_perf_metadata->mockReturn('setThriftFlush0Duration', null);
    self::mockFunction(PerfMetadata::get<>)->mockReturn($mock_perf_metadata);

    // Create input for the test
    $args ??= ExampleStreamingService_testStream_args::withDefaultValues();
    $input_buffer = new TMemoryBuffer();
    $temp_protocol = new \TBinaryProtocol($input_buffer);
    $args->write($temp_protocol);
    $temp_protocol->writeMessageEnd();

    // mock output protocol
    $output_transport = mock(nameof TMemoryBuffer);
    $output_transport->mockReturn('getBuffer', '');
    $output_transport->mockReturn('resetBuffer', null);

    $input_protocol = new TBinaryProtocol($input_buffer);
    $output_protocol = new TBinaryProtocol($output_transport);

    return shape(
      'input' => $input_protocol,
      'output' => $output_protocol,
    );
  }

  /**
   * Test streaming event handler hooks with both JK enabled and disabled.
   * Uses JKBoolDataProvider to run with both old (process_) and new (getMethodMetadata) paths.
   */
  <<JKBoolDataProvider('thrift/hack:thrift_use_method_metadata_processor')>>
  public async function testStreamingEventHandlerHooks(): Awaitable<void> {
    $mock_server_stream = $this->mockServerStream();
    $protocols = $this->mockProtocols($mock_server_stream);
    $handler = new TestStreamingExampleThriftHandler();
    $processor = new TestStreamingApplicationProcessor($handler);
    $event_handler = new TestStreamingApplicationEventHandler();
    $processor->setEventHandler($event_handler);

    await $processor->processAsync(
      $protocols['input'],
      $protocols['output'],
      self::TEST_STREAMING_SERVICE_METHOD_NAME,
      1,
    );

    expect($event_handler->postStreamStartCalled)->toBeTrue();
    expect($event_handler->postStreamPayloadGenerateCalled)->toBeTrue();
    expect($event_handler->postStreamPayloadWriteCalled)->toBeTrue();
    expect($event_handler->requestName)->toBePHPEqual(
      self::TEST_STREAMING_SERVICE_METHOD_NAME,
    );
  }

  /**
   * Test streaming event handler hooks with undeclared exception.
   * Uses JKBoolDataProvider to run with both old and new code paths.
   */
  <<JKBoolDataProvider('thrift/hack:thrift_use_method_metadata_processor')>>
  public async function testStreamingEventHandlerHooksWithUndeclaredException(
  ): Awaitable<void> {
    $mock_server_stream = $this->mockServerStream();
    $protocols = $this->mockProtocols($mock_server_stream);
    $handler = new TestStreamingErrorThriftHandler();
    $processor = new TestStreamingApplicationProcessor($handler);
    $event_handler = new TestStreamingApplicationEventHandler();
    $processor->setEventHandler($event_handler);

    await $processor->processAsync(
      $protocols['input'],
      $protocols['output'],
      self::TEST_STREAMING_SERVICE_METHOD_NAME,
      1,
    );

    expect($event_handler->postStreamStartCalled)->toBeTrue();
    expect($event_handler->postStreamPayloadGenerateCalled)->toBeTrue();
    expect($event_handler->postStreamPayloadErrorCalled)->toBeTrue();
    expect($event_handler->postStreamPayloadExceptionCalled)->toBeFalse();
  }

  /**
   * Test streaming event handler hooks with defined exception.
   * Uses JKBoolDataProvider to run with both old and new code paths.
   */
  <<JKBoolDataProvider('thrift/hack:thrift_use_method_metadata_processor')>>
  public async function testStreamingEventHandlerHooksWithDefinedException(
  ): Awaitable<void> {
    $mock_server_stream = $this->mockServerStream();
    $protocols = $this->mockProtocols($mock_server_stream);
    $handler = new TestStreamingExceptionHandler();
    $processor = new TestStreamingApplicationProcessor($handler);
    $event_handler = new TestStreamingApplicationEventHandler();
    $processor->setEventHandler($event_handler);

    await $processor->processAsync(
      $protocols['input'],
      $protocols['output'],
      self::TEST_STREAMING_SERVICE_METHOD_NAME,
      1,
    );

    expect($event_handler->postStreamStartCalled)->toBeTrue();
    expect($event_handler->postStreamPayloadGenerateCalled)->toBeTrue();
    expect($event_handler->postStreamPayloadExceptionCalled)->toBeTrue();
    expect($event_handler->postStreamPayloadErrorCalled)->toBeFalse();
  }

  /**
   * The stream start, the wait for client credits and each payload send block
   * on the stream's I/O thread. Each fake below suspends the way the native
   * wait handle does, then records whether a WallTimeOperation is open while
   * the wait is pending. Without one, the wall time profiler logs
   * `UnattributedIOTimeException` for every streamed request.
   */
  <<JKBoolDataProvider('thrift/hack:thrift_use_method_metadata_processor')>>
  public async function testNativeStreamWaitsRunInsideWallTimeOperation(
  ): Awaitable<void> {
    $open_while_pending = new Ref(dict<string, bool>[]);
    $record_while_pending = async (string $wait): Awaitable<void> ==> {
      await stop_eager_execution();
      $root = WallTimeProfiler::getRoot();
      $open_while_pending->value[$wait] = $root->getNext() !== $root;
    };
    $server_stream = mock(TServerStream::class);
    $server_stream->mockImplementation(
      'genIsStreamReady',
      async () ==> {
        await $record_while_pending('genIsStreamReady');
        return true;
      },
    );
    $server_stream->mockImplementation(
      'genSendStreamPayload',
      async ($_payload) ==> {
        await $record_while_pending('genSendStreamPayload');
        return true;
      },
    );
    $server_stream->mockReturn('sendStreamComplete', null);
    self::mockFunction(PHP\gen_start_thrift_stream<>)->mockImplementation(
      async ($_first_response) ==> {
        await $record_while_pending('gen_start_thrift_stream');
        return $server_stream;
      },
    );
    $protocols = $this->mockProtocolsAroundStreamStart();
    $processor = new TestStreamingApplicationProcessor(
      new TestStreamingExampleThriftHandler(),
    );

    await $processor->processAsync(
      $protocols['input'],
      $protocols['output'],
      self::TEST_STREAMING_SERVICE_METHOD_NAME,
      1,
    );

    expect($open_while_pending->value)->toEqual(dict[
      'gen_start_thrift_stream' => true,
      'genIsStreamReady' => true,
      'genSendStreamPayload' => true,
    ]);
  }

  /**
   * The sink start and every wait for the client's sink payloads block on the
   * stream's I/O thread. Each must run inside a WallTimeOperation, or the wall
   * time profiler logs `UnattributedIOTimeException` for every sink request.
   */
  <<JKBoolDataProvider('thrift/hack:thrift_use_method_metadata_processor')>>
  public async function testNativeSinkWaitsRunInsideWallTimeOperation(
  ): Awaitable<void> {
    $open_while_pending = new Ref(dict<string, bool>[]);
    $record_while_pending = async (string $wait): Awaitable<void> ==> {
      await stop_eager_execution();
      $root = WallTimeProfiler::getRoot();
      $open_while_pending->value[$wait] = $root->getNext() !== $root;
    };
    $ready_calls = new Ref(0);
    $server_sink = mock(TServerSink::class);
    $server_sink->mockImplementation(
      'genIsSinkReady',
      async () ==> {
        await $record_while_pending('genIsSinkReady');
        $ready_calls->value++;
        // One empty batch, then the client closes the sink.
        return $ready_calls->value === 1;
      },
    );
    $server_sink->mockReturn('getPayloads', tuple(vec[], null));
    $server_sink->mockReturn('isClientException', false);
    $server_sink->mockReturn('sendFinalResponse', null);
    self::mockFunction(gen_start_thrift_sink<>)->mockImplementation(
      async (
        $_first_response,
        $_buffer_size = 100,
        $_chunk_timeout_ms = 1000,
      ) ==> {
        await $record_while_pending('gen_start_thrift_sink');
        return $server_sink;
      },
    );
    $protocols = $this->mockProtocolsAroundStreamStart(
      ExampleStreamingService_testSink_args::withDefaultValues(),
    );
    $processor = new TestStreamingApplicationProcessor(
      new TestStreamingSinkDrainingHandler(),
    );

    await $processor->processAsync(
      $protocols['input'],
      $protocols['output'],
      'testSink',
      1,
    );

    expect($ready_calls->value)->toEqual(2, 'the sink was drained to its end');
    expect($open_while_pending->value)->toEqual(dict[
      'gen_start_thrift_sink' => true,
      'genIsSinkReady' => true,
    ]);
  }

  /**
   * Once the client's sink is no longer active, the native
   * `TServerSink::genIsSinkReady()` returns null instead of an Awaitable. The
   * sink must end on it, as it does on false.
   */
  <<JKBoolDataProvider('thrift/hack:thrift_use_method_metadata_processor')>>
  public async function testASinkEndsWhenTheReadyWaitIsNull(): Awaitable<void> {
    $ready_calls = new Ref(0);
    $server_sink = mock(TServerSink::class);
    $server_sink->mockImplementation(
      'genIsSinkReady',
      () ==> {
        $ready_calls->value++;
        if ($ready_calls->value === 1) {
          return async {
            return true;
          };
        }
        return null;
      },
    );
    $server_sink->mockReturn('getPayloads', tuple(vec[], null));
    $server_sink->mockReturn('isClientException', false);
    $server_sink->mockReturn('sendFinalResponse', null);
    $server_exception = new Ref(null);
    $server_sink->mockImplementation(
      'sendServerException',
      (mixed ...$args) ==> {
        $server_exception->value = $args[1] ?? null;
      },
    );
    self::mockFunction(gen_start_thrift_sink<>)->mockYield($server_sink);
    $protocols = $this->mockProtocolsAroundStreamStart(
      ExampleStreamingService_testSink_args::withDefaultValues(),
    );
    $processor = new TestStreamingApplicationProcessor(
      new TestStreamingSinkDrainingHandler(),
    );

    await $processor->processAsync(
      $protocols['input'],
      $protocols['output'],
      'testSink',
      1,
    );

    expect($ready_calls->value)->toEqual(2, 'the sink ended on the null wait');
    expect($server_exception->value)->toBeNull();
  }

  /**
   * A bidi stream starts, reads the client's sink payloads and sends stream
   * payloads; every one of those native waits must run inside a
   * WallTimeOperation.
   */
  public async function testNativeBiDiStreamWaitsRunInsideWallTimeOperation(
  ): Awaitable<void> {
    $open_while_pending = new Ref(dict<string, bool>[]);
    $record_while_pending = async (string $wait): Awaitable<void> ==> {
      await stop_eager_execution();
      $root = WallTimeProfiler::getRoot();
      $open_while_pending->value[$wait] = $root->getNext() !== $root;
    };
    $sink_ready_calls = new Ref(0);
    $bidi_stream = mock(TServerBiDiStream::class);
    $bidi_stream->mockImplementation(
      'genIsSinkReady',
      async () ==> {
        await $record_while_pending('genIsSinkReady');
        $sink_ready_calls->value++;
        return $sink_ready_calls->value === 1;
      },
    );
    $bidi_stream->mockReturn('getPayloads', tuple(vec[], null));
    $bidi_stream->mockReturn('sinkClose', null);
    $bidi_stream->mockImplementation(
      'genIsStreamReady',
      async () ==> {
        await $record_while_pending('genIsStreamReady');
        return true;
      },
    );
    $bidi_stream->mockImplementation(
      'genSendStreamPayload',
      async ($_payload) ==> {
        await $record_while_pending('genSendStreamPayload');
        return true;
      },
    );
    $bidi_stream->mockReturn('sendStreamComplete', null);
    $bidi_stream->mockReturn('isClientException', false);
    self::mockFunction(gen_start_thrift_bidi_stream<>)->mockImplementation(
      async ($_first_response, $_buffer_size) ==> {
        await $record_while_pending('gen_start_thrift_bidi_stream');
        return $bidi_stream;
      },
    );
    $protocols = $this->mockProtocolsAroundStreamStart();
    $processor =
      new TestBiDiStreamingProcessor(new TestStreamingExampleThriftHandler());

    await $processor->genRunBiDiStream(
      $protocols['input'],
      $protocols['output'],
    );

    expect($sink_ready_calls->value)->toEqual(
      2,
      'the sink side was drained to its end',
    );
    expect($open_while_pending->value)->toEqual(dict[
      'gen_start_thrift_bidi_stream' => true,
      'genIsStreamReady' => true,
      'genIsSinkReady' => true,
      'genSendStreamPayload' => true,
    ]);
  }
}

final class TestStreamingApplicationEventHandler
  extends TProcessorEventHandler {
  public bool $postStreamStartCalled = false;
  public bool $postStreamPayloadGenerateCalled = false;
  public bool $postStreamPayloadWriteCalled = false;
  public bool $postStreamPayloadErrorCalled = false;
  public bool $postStreamPayloadExceptionCalled = false;
  public ?string $requestName = null;

  <<__Override>>
  public function postStreamStart(
    mixed $handler_context,
    string $fn_name,
  )[write_props]: void {
    $this->postStreamStartCalled = true;
    $this->requestName = $fn_name;
  }

  <<__Override>>
  public function postStreamPayloadGenerate(
    mixed $handler_context,
    string $fn_name,
    mixed $result,
  )[write_props]: void {
    $this->postStreamPayloadGenerateCalled = true;
    $this->requestName = $fn_name;
  }

  <<__Override>>
  public function postStreamPayloadWrite(
    mixed $handler_context,
    string $fn_name,
    mixed $payload,
  )[write_props]: void {
    $this->postStreamPayloadWriteCalled = true;
    $this->requestName = $fn_name;
  }

  <<__Override>>
  public function postStreamPayloadError(
    mixed $handler_context,
    string $fn_name,
    Exception $exception,
  )[write_props]: void {
    $this->postStreamPayloadErrorCalled = true;
    $this->requestName = $fn_name;
  }

  <<__Override>>
  public function postStreamPayloadException(
    mixed $handler_context,
    string $fn_name,
    Exception $exception,
  )[write_props]: void {
    $this->postStreamPayloadExceptionCalled = true;
    $this->requestName = $fn_name;
  }
}

class TestStreamingServiceAsyncIf implements ExampleStreamingServiceAsyncIf {

  public async function testStream(
    ?example_RequestStruct $request,
  ): Awaitable<ResponseAndStream<example_ResponseStruct, string>> {
    return new ResponseAndStream(
      example_ResponseStruct::fromShape(shape('text' => 'test')),
      $this->emptyStream(),
    );
  }

  public async function testSink(?example_RequestStruct $_request): Awaitable<
    ResponseAndSink<example_ResponseStruct, string, example_ResponseStruct>,
  > {
    return new ResponseAndSink(
      example_ResponseStruct::fromShape(shape('text' => 'test')),
      async (HH\AsyncGenerator<null, string, void> $_gen) ==> {
        return example_ResponseStruct::fromShape(shape('text' => 'done'));
      },
    );
  }

  private async function emptyStream(): HH\AsyncGenerator<null, string, void> {
    yield '';
  }
}

final class TestStreamingExampleThriftHandler
  extends TestStreamingServiceAsyncIf {

  <<__Override>>
  public async function testStream(
    ?example_RequestStruct $request,
  ): Awaitable<ResponseAndStream<example_ResponseStruct, string>> {
    return new ResponseAndStream(
      example_ResponseStruct::fromShape(shape('text' => 'test')),
      $this->genStream(),
    );
  }

  private async function genStream(): HH\AsyncGenerator<null, string, void> {
    yield 'item1';
    yield 'item2';
    yield 'item3';
  }
}

final class TestStreamingErrorThriftHandler
  extends TestStreamingServiceAsyncIf {

  <<__Override>>
  public async function testStream(
    ?example_RequestStruct $request,
  ): Awaitable<ResponseAndStream<example_ResponseStruct, string>> {
    return new ResponseAndStream(
      example_ResponseStruct::fromShape(shape('text' => 'test')),
      $this->genStreamWithError(),
    );
  }

  private async function genStreamWithError(
  ): HH\AsyncGenerator<null, string, void> {
    yield 'item1';
    throw new Exception('Undeclared stream error');
  }

}

final class TestStreamingExceptionHandler extends TestStreamingServiceAsyncIf {

  <<__Override>>
  public async function testStream(
    ?example_RequestStruct $request,
  ): Awaitable<ResponseAndStream<example_ResponseStruct, string>> {
    return new ResponseAndStream(
      example_ResponseStruct::fromShape(shape('text' => 'test')),
      $this->genStreamWithException(),
    );
  }

  private async function genStreamWithException(
  ): HH\AsyncGenerator<null, string, void> {
    yield 'item1';
    $exception = example_StreamException::fromShape(
      shape('message' => 'Declared stream exception'),
    );
    throw $exception;
  }
}

final class TestStreamingApplicationProcessor
  extends ExampleStreamingServiceAsyncProcessorBase {
  const type TThriftIf = TestStreamingServiceAsyncIf;
}

final class TestStreamingSinkDrainingHandler
  extends TestStreamingServiceAsyncIf {
  <<__Override>>
  public async function testSink(?example_RequestStruct $_request): Awaitable<
    ResponseAndSink<example_ResponseStruct, string, example_ResponseStruct>,
  > {
    return new ResponseAndSink(
      example_ResponseStruct::fromShape(shape('text' => 'test')),
      async (HH\AsyncGenerator<null, string, void> $sink) ==> {
        foreach ($sink await as $_payload) {
        }
        return example_ResponseStruct::fromShape(shape('text' => 'done'));
      },
    );
  }
}

/**
 * The example service declares no bidi method, so this runs the processor's
 * bidi path directly with the example sink payload and stream response types.
 */
final class TestBiDiStreamingProcessor
  extends ExampleStreamingServiceAsyncProcessorBase {
  const type TThriftIf = TestStreamingServiceAsyncIf;

  public async function genRunBiDiStream(
    TProtocol $input,
    TProtocol $output,
  ): Awaitable<void> {
    await $this->genExecuteBiDiStream(
      self::genDrainSinkThenRespond<>,
      ExampleStreamingService_testSink_SinkPayload::class,
      ExampleStreamingService_testStream_StreamResponse::class,
      $input,
      $output,
      'testBiDi',
    );
  }

  private static async function genDrainSinkThenRespond(
    HH\AsyncGenerator<null, string, void> $sink,
  ): HH\AsyncGenerator<null, string, void> {
    foreach ($sink await as $_payload) {
    }
    yield 'done';
  }
}
