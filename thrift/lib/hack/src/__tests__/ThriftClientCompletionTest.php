<?hh
// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

<<Oncalls('thrift_hack')>>
final class ThriftClientCompletionTest extends WWWTest {

  public static function paths(): dict<string, shape('use_channel' => bool)> {
    return dict[
      'channel' => shape('use_channel' => true),
      'genWait' => shape('use_channel' => false),
    ];
  }

  public static function failures(): dict<string, shape(
    'use_channel' => bool,
    'declared' => bool,
  )> {
    return dict[
      'channel declared' => shape('use_channel' => true, 'declared' => true),
      'channel unexpected' => shape('use_channel' => true, 'declared' => false),
      'genWait declared' => shape('use_channel' => false, 'declared' => true),
      'genWait unexpected' =>
        shape('use_channel' => false, 'declared' => false),
    ];
  }

  private static function fixture<T>(
    bool $use_channel,
    THandlerShortCircuitException<T> $completion,
    bool $short_send = false,
  ): shape(
    'client' => ThriftCompletionObservingClient,
    'calls' => Vector<string>,
    'received' => Vector<mixed>,
    'requests' => Vector<string>,
    'input' => TMemoryBuffer,
  ) {
    $calls = Vector<string> {};
    $received = Vector<mixed> {};
    $requests = Vector<string> {};
    $input = new TMemoryBuffer();
    $output = new TMemoryBuffer();
    $events = strict_mock(TClientEventHandler::class)
      ->mockImplementation('preSend', ($name, $_args, $id, $_service) ==> {
        $calls[] = 'preSend:'.$name.':'.$id;
        if ($short_send) {
          throw new THandlerShortCircuitException(
            null,
            THandlerShortCircuitException::R_SUCCESS,
          );
        }
      })
      ->mockImplementation('postSend', ($name, $_args, $id) ==> {
        $calls[] = 'postSend:'.$name.':'.$id;
      })
      ->mockImplementation('preRecv', ($name, $id) ==> {
        $calls[] = 'preRecv:'.$name.':'.(string)$id;
        throw $completion;
      });
    foreach (vec['postRecv', 'recvException', 'recvError'] as $hook) {
      $events->mockImplementation($hook, ($name, $id, $value) ==> {
        $calls[] = $hook.':'.$name.':'.(string)$id;
        $received[] = $value;
      });
    }
    $async = strict_mock(TClientAsyncHandler::class)
      ->mockImplementation('genBefore', async ($_service, $name, $_args) ==> {
        $calls[] = 'genBefore:'.$name;
      })
      ->mockImplementation('genWait', async ($id) ==> {
        $calls[] = 'genWait:'.$id;
        $requests[] = $output->getBuffer();
      })
      ->mockImplementation('genAfter', async ($name, $value) ==> {
        $calls[] = 'genAfter:'.$name;
        $received[] = $value;
      })
      ->mockImplementation('genOnError', async ($name, $ex) ==> {
        $calls[] = 'genOnError:'.$name;
        $received[] = $ex;
      });
    $channel = null;
    if ($use_channel) {
      $channel = strict_mock(IThriftMigrationAsyncChannel::class)
        ->mockImplementation('genSendRequestResponse', async (
          RpcOptions $_options,
          string $message,
        ) ==> {
          $calls[] = 'channel';
          $requests[] = $message;
          // preRecv throws before decoding: these are deliberately not RPC bytes.
          return
            tuple('ignored response bytes', dict['reply-header' => 'value']);
        });
    }
    $client = new ThriftCompletionObservingClient(
      new TCompactProtocolAccelerated($input),
      new TCompactProtocolAccelerated($output),
      $channel,
    );
    $client->setEventHandler($events)->setAsyncHandler($async);
    return shape(
      'client' => $client,
      'calls' => $calls,
      'received' => $received,
      'requests' => $requests,
      'input' => $input,
    );
  }

  private static function expectedCalls(
    bool $use_channel,
    string $recv_hook,
    string $async_hook,
  ): vec<string> {
    return vec[
      'genBefore:sendRequest',
      'preSend:sendRequest:0',
      'postSend:sendRequest:0',
      $use_channel ? 'channel' : 'genWait:0',
      'preRecv:sendRequest:0',
      $recv_hook.':sendRequest:0',
      $async_hook.':sendRequest',
    ];
  }

  <<DataProvider('paths')>>
  public async function testReceiveSuccessPreservesIdentityAndPathMetadata(
    bool $use_channel,
  ): Awaitable<void> {
    $response =
      example_ResponseStruct::fromShape(shape('text' => 'completion'));
    $fixture = self::fixture($use_channel, new THandlerShortCircuitException(
      $response,
      THandlerShortCircuitException::R_SUCCESS,
    ));

    $returned = await $fixture['client']->sendRequest(
      example_RequestStruct::fromShape(shape('text' => 'request')),
    );

    expect($returned)->toEqual($response);
    expect($fixture['calls']->toVArray())->toEqual(
      self::expectedCalls($use_channel, 'postRecv', 'genAfter'),
    );
    expect($fixture['received']->toVArray())->toEqual(
      vec[$response, $response],
    );
    expect($fixture['client']->readHeaders)->toEqual(
      $use_channel ? dict['reply-header' => 'value'] : null,
    );
    expect($fixture['input']->getBuffer())->toEqual(
      $use_channel ? 'ignored response bytes' : '',
    );
    expect(C\count($fixture['requests']))->toEqual(1);
  }

  <<DataProvider('failures')>>
  public async function testReceiveFailurePreservesExceptionAndCause(
    bool $use_channel,
    bool $declared,
  ): Awaitable<void> {
    $cause = new TestException('original cause');
    $failure = $declared
      ? example_WhisperException::fromShape(shape('message' => 'declared'))
      : new TestException('unexpected');
    $failure->setPrevious($cause);
    $fixture = self::fixture($use_channel, new THandlerShortCircuitException(
      $failure,
      $declared
        ? THandlerShortCircuitException::R_EXPECTED_EX
        : THandlerShortCircuitException::R_UNEXPECTED_EX,
    ));
    $caught = null;
    try {
      await $fixture['client']->sendRequest(
        example_RequestStruct::fromShape(shape('text' => 'request')),
      );
    } catch (Exception $ex) {
      Ope::markClownyControlFlowException(
        $ex,
        'because the test observes the rethrown completion',
      );
      $caught = $ex;
    }

    // Keep assertions outside core catches, including genOnError's catch.
    expect($caught)->toEqual($failure);
    expect($caught?->getPrevious())->toEqual($cause);
    expect($fixture['calls']->toVArray())->toEqual(self::expectedCalls(
      $use_channel,
      $declared ? 'recvException' : 'recvError',
      'genOnError',
    ));
    expect($fixture['received']->toVArray())->toEqual(vec[$failure, $failure]);
    expect(C\count($fixture['requests']))->toEqual(1);
  }

  <<DataProvider('paths')>>
  public async function testSendSuccessShortCircuitStillHandsOffEmptyBuffer(
    bool $use_channel,
  ): Awaitable<void> {
    $response =
      example_ResponseStruct::fromShape(shape('text' => 'completion'));
    $fixture = self::fixture(
      $use_channel,
      new THandlerShortCircuitException(
        $response,
        THandlerShortCircuitException::R_SUCCESS,
      ),
      true,
    );

    $returned = await $fixture['client']->sendRequest(
      example_RequestStruct::fromShape(shape('text' => 'request')),
    );

    expect($returned)->toEqual($response);
    expect($fixture['calls']->toVArray())->toEqual(
      self::expectedCalls($use_channel, 'postRecv', 'genAfter'),
    );
    expect($fixture['requests']->toVArray())->toEqual(vec['']);
  }
}

// Observe the real tuple while invoking the existing generated public method.
<<Oncalls('thrift_hack')>>
final class ThriftCompletionObservingClient
  extends ExampleRootServiceAsyncClient {
  public ?dict<string, string> $readHeaders = null;

  <<__Override>>
  protected async function genAwaitResponse<TRet>(
    class<IResultThriftStruct with { type TResult = TRet }> $result,
    string $name,
    bool $is_return_void,
    int $expectedsequenceid,
    RpcOptions $rpc_options,
    shape(?'read_options' => int) $options = shape(),
  ): Awaitable<(TRet, ?dict<string, string>)> {
    $completion = await parent::genAwaitResponse(
      $result,
      $name,
      $is_return_void,
      $expectedsequenceid,
      $rpc_options,
      $options,
    );
    $this->readHeaders = $completion[1];
    return $completion;
  }
}
