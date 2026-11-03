<?hh
// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

/**
 * ResponseAndStream/ResponseAndSink allow a null first response. The server
 * must still send a REPLY with an unset `success`, so the client reports its
 * usual missing-result error.
 */
<<
  Oncalls('thrift'),
  JKBoolDataProvider('thrift/hack:thrift_use_method_metadata_processor'),
>>
final class ThriftAsyncProcessorNullFirstResponseTest extends WWWTest {

  public async function testStreamWithNullFirstResponseRepliesWithUnsetSuccess(
  ): Awaitable<void> {
    $first_response = new Ref('');
    $server_stream = mock(nameof TServerStream);
    $server_stream->mockYield('genIsStreamReady', true);
    $server_stream->mockYield('genSendStreamPayload', true);
    $server_stream->mockReturn('sendStreamComplete', null);
    self::mockFunction(PHP\gen_start_thrift_stream<>)->mockImplementation(
      async ($encoded) ==> {
        $first_response->value = $encoded;
        return $server_stream;
      },
    );
    $handler = mock(nameof ExampleStreamingServiceAsyncIf);
    $handler->mockYield(
      'testStream',
      new ResponseAndStream(null, self::genStream()),
    );

    await self::genProcess(
      $handler,
      'testStream',
      ExampleStreamingService_testStream_args::withDefaultValues(),
    );

    $result = self::readReply(
      $first_response->value,
      'testStream',
      ExampleStreamingService_testStream_FirstResponse::class,
    );
    expect($result->success)->toBeNull('success must be left unset');
  }

  public async function testSinkWithNullFirstResponseRepliesWithUnsetSuccess(
  ): Awaitable<void> {
    $first_response = new Ref('');
    $server_sink = mock(nameof TServerSink);
    $server_sink->mockReturn('isClientException', false);
    $server_sink->mockReturn('sendFinalResponse', null);
    self::mockFunction(gen_start_thrift_sink<>)->mockImplementation(
      async ($encoded, $_buffer_size = 100, $_chunk_timeout_ms = 1000) ==> {
        $first_response->value = $encoded;
        return $server_sink;
      },
    );
    $handler = mock(nameof ExampleStreamingServiceAsyncIf);
    $handler->mockYield(
      'testSink',
      new ResponseAndSink(null, self::genFinalResponse<>),
    );

    await self::genProcess(
      $handler,
      'testSink',
      ExampleStreamingService_testSink_args::withDefaultValues(),
    );

    $result = self::readReply(
      $first_response->value,
      'testSink',
      ExampleStreamingService_testSink_FirstResponse::class,
    );
    expect($result->success)->toBeNull('success must be left unset');
  }

  private static async function genProcess(
    ExampleStreamingServiceAsyncIf $handler,
    string $fname,
    IThriftSyncStruct $args,
  ): Awaitable<void> {
    self::mockFunction(PHP\hphp_set_error_page<>);
    self::mockFunction(php_root<>)->mockReturn('/var/www');
    self::mockFunction(request_stats_total_wall<>)->mockReturn(0);
    $perf_metadata = mock(nameof PerfMetadata);
    $perf_metadata->mockReturn('setThriftFlush0Duration', null);
    self::mockFunction(PerfMetadata::get<>)->mockReturn($perf_metadata);

    $input = new TMemoryBuffer();
    $args_protocol = new TBinaryProtocol($input);
    $args->write($args_protocol);
    $args_protocol->writeMessageEnd();

    await (new ExampleStreamingServiceAsyncProcessor($handler))->processAsync(
      new TBinaryProtocol($input),
      new TBinaryProtocol(new TMemoryBuffer()),
      $fname,
      1,
    );
  }

  private static function readReply<TResult as IThriftSyncStruct>(
    string $encoded,
    string $expected_name,
    class<TResult> $result_class,
  ): TResult {
    $protocol = new TBinaryProtocol(new TMemoryBuffer($encoded));
    $name = '';
    $type = 0;
    $seqid = 0;
    $protocol->readMessageBegin(inout $name, inout $type, inout $seqid);
    expect($name)->toEqual($expected_name);
    expect($type)->toEqual(TMessageType::REPLY);
    return $protocol->readRPCStruct($result_class);
  }

  private static async function genStream(
  ): HH\AsyncGenerator<null, string, void> {
    yield 'item';
  }

  private static async function genFinalResponse(
    HH\AsyncGenerator<null, string, void> $_sink,
  ): Awaitable<example_ResponseStruct> {
    return example_ResponseStruct::withDefaultValues();
  }
}
