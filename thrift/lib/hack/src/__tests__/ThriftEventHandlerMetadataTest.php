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

<<
  Oncalls('thrift_hack'),
  JKBoolDataProvider('thrift/hack:thrift_use_method_metadata_processor'),
>>
final class ThriftEventHandlerMetadataTest extends WWWTest {
  public async function testMetadataBackwardCompatibility(): Awaitable<void> {
    $handler = mock(nameof ExampleRootServiceAsyncIf);
    $proc = new ExampleRootServiceAsyncProcessor($handler);

    // The event handler will assert that the received metadata matches
    // the expected metadata provided here
    $event_handler =
      new VerifyNameEventHandler('example_ExampleRootService', 'doNothing');
    $proc->setEventHandler($event_handler);

    // Make the RPC call, which will invoke the event handler
    $input = new TCompactProtocol(new TMemoryBuffer());
    $input->writeRPCMessage(
      'doNothing',
      TMessageType::CALL,
      ExampleRootService_doNothing_args::withDefaultValues(),
      0,
    );
    $_ = await $proc->processAsync(
      $input,
      new TCompactProtocol(new TMemoryBuffer()),
      'doNothing',
    );
  }

  public async function testMiddleServiceInheritMethodUsesDeclaringServiceName(
  ): Awaitable<void> {
    $handler = mock(nameof ExampleMiddleServiceAsyncIf);
    $handler->mockYield(
      'sendMiddleRequest',
      example_ResponseStruct::withDefaultValues(),
    );
    $proc = new ExampleMiddleServiceAsyncProcessor($handler);

    // Make the RPC call for doNothing (declared at Root) and verify the name
    $event_handler_do_nothing =
      new VerifyNameEventHandler('example_ExampleRootService', 'doNothing');
    $proc->setEventHandler($event_handler_do_nothing);

    $input = new TCompactProtocol(new TMemoryBuffer());
    $input->writeRPCMessage(
      'doNothing',
      TMessageType::CALL,
      ExampleRootService_doNothing_args::withDefaultValues(),
      0,
    );
    $_ = await $proc->processAsync(
      $input,
      new TCompactProtocol(new TMemoryBuffer()),
      'doNothing',
    );

    // Make the RPC call for sendMiddleRequest (declared at Middle) and verify the name
    $event_handler_send_middle_request = new VerifyNameEventHandler(
      'example_ExampleMiddleService',
      'sendMiddleRequest',
    );
    $proc = new ExampleMiddleServiceAsyncProcessor($handler);
    $proc->setEventHandler($event_handler_send_middle_request);

    $input = new TCompactProtocol(new TMemoryBuffer());
    $input->writeRPCMessage(
      'sendMiddleRequest',
      TMessageType::CALL,
      ExampleMiddleService_sendMiddleRequest_args::withDefaultValues(),
      0,
    );
    $_ = await $proc->processAsync(
      $input,
      new TCompactProtocol(new TMemoryBuffer()),
      'sendMiddleRequest',
    );
  }

  public async function testChildServiceInheritMethodUsesDeclaringServiceName(
  ): Awaitable<void> {
    $handler = mock(nameof ExampleChildServiceAsyncIf);
    $handler->mockYield(
      'sendMiddleRequest',
      example_ResponseStruct::withDefaultValues(),
    );
    $handler->mockYield(
      'sendChildRequest',
      example_ResponseStruct::withDefaultValues(),
    );
    $proc = new ExampleChildServiceAsyncProcessor($handler);

    // Make the RPC call for doNothing (declared at Root) and verify the name
    $event_handler_do_nothing =
      new VerifyNameEventHandler('example_ExampleRootService', 'doNothing');
    $proc->setEventHandler($event_handler_do_nothing);

    $input = new TCompactProtocol(new TMemoryBuffer());
    $input->writeRPCMessage(
      'doNothing',
      TMessageType::CALL,
      ExampleRootService_doNothing_args::withDefaultValues(),
      0,
    );
    $_ = await $proc->processAsync(
      $input,
      new TCompactProtocol(new TMemoryBuffer()),
      'doNothing',
    );

    // Make the RPC call for sendMiddleRequest (declared at Middle) and verify the name
    $event_handler_send_middle_request = new VerifyNameEventHandler(
      'example_ExampleMiddleService',
      'sendMiddleRequest',
    );
    $proc = new ExampleChildServiceAsyncProcessor($handler);
    $proc->setEventHandler($event_handler_send_middle_request);

    $input = new TCompactProtocol(new TMemoryBuffer());
    $input->writeRPCMessage(
      'sendMiddleRequest',
      TMessageType::CALL,
      ExampleMiddleService_sendMiddleRequest_args::withDefaultValues(),
      0,
    );
    $_ = await $proc->processAsync(
      $input,
      new TCompactProtocol(new TMemoryBuffer()),
      'sendMiddleRequest',
    );

    // Make the RPC call for sendChildRequest (declared at Child) and verify the name
    $event_handler_send_child_request = new VerifyNameEventHandler(
      'example_ExampleChildService',
      'sendChildRequest',
    );
    $proc = new ExampleChildServiceAsyncProcessor($handler);
    $proc->setEventHandler($event_handler_send_child_request);

    $input = new TCompactProtocol(new TMemoryBuffer());
    $input->writeRPCMessage(
      'sendChildRequest',
      TMessageType::CALL,
      ExampleChildService_sendChildRequest_args::withDefaultValues(),
      0,
    );
    $_ = await $proc->processAsync(
      $input,
      new TCompactProtocol(new TMemoryBuffer()),
      'sendChildRequest',
    );
  }
}

final class VerifyNameEventHandler extends TProcessorEventHandler {
  public function __construct(
    private string $expectedServiceName,
    private string $expectedMethodName,
  ) {
  }

  <<__Override>>
  public function preExec(
    mixed $handler_context,
    string $service_name,
    string $method_name,
    mixed $args,
  ): void {
    expect($service_name)->toEqual($this->expectedServiceName);
    expect($method_name)->toEqual($this->expectedMethodName);
  }
}
