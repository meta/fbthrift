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

<<Oncalls('flow_control')>>
final class TransportPriorityOverrideContextHandlerTest extends WWWTest {
  const string SERVICE = 'sample_service_name';

  private static function runHandler(
    ?RequestPriority $upstream_priority,
  ): ?RequestPriority {
    $mutable_tfm = ThriftFrameworkMetadata::withDefaultValues();
    $mutable_tfm->request_priority = $upstream_priority;
    (new TransportPriorityOverrideContextHandler())->onOutgoingDownstream(
      shape('service_name' => self::SERVICE),
      $mutable_tfm,
      new ImmutableThriftContextPropState(ThriftContextPropState::get()),
    );
    return $mutable_tfm->request_priority;
  }

  public static function dataProviderForOverrides(): dict<
    string,
    shape(
      'override' => RequestPriority,
      'upstream' => ?RequestPriority,
    ),
  > {
    return dict[
      'bronze over unset' => shape(
        'override' => RequestPriority::SHEDDABLE_PLUS,
        'upstream' => null,
      ),
      'critical_plus over sheddable' => shape(
        'override' => RequestPriority::CRITICAL_PLUS,
        'upstream' => RequestPriority::SHEDDABLE,
      ),
      'sheddable over critical_plus' => shape(
        'override' => RequestPriority::SHEDDABLE,
        'upstream' => RequestPriority::CRITICAL_PLUS,
      ),
    ];
  }

  <<DataProvider('dataProviderForOverrides')>>
  public async function testOverwritesInsideScope(
    RequestPriority $override,
    ?RequestPriority $upstream,
  ): Awaitable<void> {
    $sent = await RequestPriorityTransportScope::genRun(
      $override,
      async () ==> self::runHandler($upstream),
    );

    expect($sent)->toEqual($override);
  }

  public async function testLeavesUpstreamOutsideScope(): Awaitable<void> {
    expect(self::runHandler(RequestPriority::SHEDDABLE))->toEqual(
      RequestPriority::SHEDDABLE,
    );
    expect(self::runHandler(null))->toBeNull();
  }

  public async function testInstalledForAllClients(): Awaitable<void> {
    MockJustKnobs::setBool('servicerouter/thrift_context_prop:enabled', true);

    $sent = await RequestPriorityTransportScope::genRun(
      RequestPriority::SHEDDABLE_PLUS,
      async () ==> self::sendThroughInstalledHandlers(),
    );

    expect($sent)->toEqual(RequestPriority::SHEDDABLE_PLUS);
    expect(ThriftContextPropState::get()->getRequestPriority())->toBeNull();
  }

  private static function sendThroughInstalledHandlers(): ?RequestPriority {
    $transport = TServiceRouterTransport::create(self::SERVICE, dict[], dict[]);
    $multi_handler = new TClientMultiEventHandler();
    ThriftContextPropHandlers::install(
      $multi_handler,
      $transport,
      shape('service_name' => self::SERVICE),
    );
    $multi_handler->preSend('sample_method_name', null, 0, 'sample_interface');

    $tfm = ThriftFrameworkMetadata::withDefaultValues();
    $tfm->read(
      new TCompactProtocolAccelerated(
        new TMemoryBuffer(
          Base64::decode(
            $transport->getWriteHeaders()[
              ThriftFrameworkMetadata_CONSTANTS::ThriftFrameworkMetadataHeaderKey
            ],
          ),
        ),
      ),
    );
    return $tfm->request_priority;
  }
}
