<?hh
// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

/**
 * Exceptions thrown by the event handler hooks that run before the handler
 * (preRead, postRead, preExec) must be caught by the processor and reported
 * to the client, the same way handler exceptions are.
 */
<<Oncalls('thrift')>>
final class ThriftAsyncProcessorEventHandlerExceptionTest extends WWWTest {

  public static function provideThrowingHooks(
  ): dict<string, shape('hook' => string)> {
    return dict[
      'preRead' => shape('hook' => 'preRead'),
      'postRead' => shape('hook' => 'postRead'),
      'preExec' => shape('hook' => 'preExec'),
    ];
  }

  <<
    DataProvider('provideThrowingHooks'),
    JKBoolDataProvider('thrift/hack:thrift_use_method_metadata_processor'),
  >>
  public async function testUndeclaredHookExceptionIsSentAsApplicationException(
    string $hook,
  ): Awaitable<void> {
    $ex = new ThriftEventHookTestException('hook failed');
    $event_handler = new ThrowingHookEventHandlerForTest($hook, $ex);
    $proc = new ExampleRootServiceAsyncProcessor(
      mock(nameof ExampleRootServiceAsyncIf),
    );
    $proc->setEventHandler($event_handler);
    $input = new TCompactProtocol(new TMemoryBuffer());
    $input->writeRPCMessage(
      'doNothing',
      TMessageType::CALL,
      ExampleRootService_doNothing_args::withDefaultValues(),
      7,
    );
    $output = new TCompactProtocol(new TMemoryBuffer());

    expect(await $proc->processAsync($input, $output))->toBeTrue();

    expect($event_handler->handlerErrors)->toEqual(vec[$ex]);
    expect($event_handler->handlerExceptions)->toEqual(vec[]);
    $name = '';
    $type = 0;
    $seqid = 0;
    $output->readMessageBegin(inout $name, inout $type, inout $seqid);
    expect($name)->toEqual('doNothing');
    expect($type)->toEqual(TMessageType::EXCEPTION);
    expect($seqid)->toEqual(7);
    $app_ex = $output->readRPCStruct(TApplicationException::class);
    expect(Str\starts_with($app_ex->getMessage(), 'hook failed'))->toBeTrue();
  }

  <<JKBoolDataProvider('thrift/hack:thrift_use_method_metadata_processor')>>
  public async function testDeclaredPreExecExceptionIsSentInResult(
  ): Awaitable<void> {
    $ex = new example_WhisperException('declared');
    $event_handler = new ThrowingHookEventHandlerForTest('preExec', $ex);
    $proc = new ExampleRootServiceAsyncProcessor(
      mock(nameof ExampleRootServiceAsyncIf),
    );
    $proc->setEventHandler($event_handler);
    $input = new TCompactProtocol(new TMemoryBuffer());
    $input->writeRPCMessage(
      'sendRequest',
      TMessageType::CALL,
      ExampleRootService_sendRequest_args::withDefaultValues(),
      3,
    );
    $output = new TCompactProtocol(new TMemoryBuffer());

    expect(await $proc->processAsync($input, $output))->toBeTrue();

    expect($event_handler->handlerExceptions)->toEqual(vec[$ex]);
    expect($event_handler->handlerErrors)->toEqual(vec[]);
    $name = '';
    $type = 0;
    $seqid = 0;
    $output->readMessageBegin(inout $name, inout $type, inout $seqid);
    expect($type)->toEqual(TMessageType::REPLY);
    $result =
      $output->readRPCStruct(ExampleRootService_sendRequest_result::class);
    expect($result->ex?->message)->toEqual('declared');
  }

  <<DataProvider('provideThrowingHooks')>>
  public async function testOnewayHookExceptionIsReportedWithoutReply(
    string $hook,
  ): Awaitable<void> {
    $ex = new ThriftEventHookTestException('hook failed');
    $event_handler = new ThrowingHookEventHandlerForTest($hook, $ex);
    $proc = new GenProcessMethodExposingExampleRootServiceAsyncProcessor(
      mock(nameof ExampleRootServiceAsyncIf),
    );
    $proc->setEventHandler($event_handler);
    $input = new TCompactProtocol(new TMemoryBuffer());
    $input->writeRPCStruct(
      ExampleRootService_doNothing_args::withDefaultValues(),
    );
    $output_buffer = new TMemoryBuffer();

    await $proc->genProcessMethodForTest(
      new ThriftServiceOnewayMethod(
        'example_ExampleRootService',
        ExampleRootService_doNothing_args::class,
        async (
          ExampleRootServiceAsyncIf $handler,
          ExampleRootService_doNothing_args $_args,
        ): Awaitable<void> ==> {
          await $handler->doNothing();
        },
      ),
      'doNothing',
      0,
      $input,
      new TCompactProtocol($output_buffer),
    );

    expect($event_handler->handlerErrors)->toEqual(vec[$ex]);
    expect($output_buffer->getBuffer())->toEqual('');
  }
}

final class ThriftEventHookTestException extends Exception {}

final class ThrowingHookEventHandlerForTest extends TProcessorEventHandler {
  public vec<Exception> $handlerErrors = vec[];
  public vec<Exception> $handlerExceptions = vec[];

  public function __construct(
    private string $throwingHook,
    private Exception $exception,
  ) {}

  <<__Override>>
  public function preRead(
    mixed $_handler_context,
    string $_fn_name,
    mixed $_args,
  ): void {
    $this->throwIf('preRead');
  }

  <<__Override>>
  public function postRead(
    mixed $_handler_context,
    string $_fn_name,
    mixed $_args,
  ): void {
    $this->throwIf('postRead');
  }

  <<__Override>>
  public function preExec(
    mixed $_handler_context,
    string $_service_name,
    string $_fn_name,
    mixed $_args,
  ): void {
    $this->throwIf('preExec');
  }

  <<__Override>>
  public function handlerException(
    mixed $_handler_context,
    string $_fn_name,
    Exception $exception,
  ): void {
    $this->handlerExceptions[] = $exception;
  }

  <<__Override>>
  public function handlerError(
    mixed $_handler_context,
    string $_fn_name,
    Exception $exception,
  ): void {
    $this->handlerErrors[] = $exception;
  }

  private function throwIf(string $hook): void {
    if ($hook === $this->throwingHook) {
      throw $this->exception;
    }
  }
}

final class GenProcessMethodExposingExampleRootServiceAsyncProcessor
  extends ExampleRootServiceAsyncProcessor {

  public function __construct(ExampleRootServiceAsyncIf $handler) {
    parent::__construct($handler);
  }

  public async function genProcessMethodForTest(
    this::TThriftServiceMethodMetadata $method_metadata,
    string $fname,
    int $seqid,
    TProtocol $input,
    TProtocol $output,
  ): Awaitable<void> {
    await $this->genProcessMethod(
      $method_metadata,
      $fname,
      $seqid,
      $input,
      $output,
    );
  }
}
