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

<<Oncalls('perf_primary')>>
final class RequestPriorityContextHandlerTest extends WWWTest {
  const string KNOB = 'www/web_efficiency_infra:pl_set_priority_to_tcp';
  const string ALLOWLISTED_SERVICE =
    'multifeed.aggregator.aggregator_qe_canary';
  const string OTHER_SERVICE = 'some.other.service';
  const string KILLSWITCH =
    'upop/contextprop_priority:disable_request_priority_context_handler';

  <<__Override>>
  public async function beforeEach(): Awaitable<void> {
    MockJustKnobs::overrideKnob<bool>(
      self::KNOB,
      false,
      null,
      dict[self::ALLOWLISTED_SERVICE => true],
    );
  }

  private static function mockEffectivePriority(
    ?RequestPriority $priority,
  ): void {
    self::mockClassStaticMethodUNSAFE(MCPContext::class, 'getRequestPriority')
      ->mockReturn($priority);
  }

  private static function runHandler(
    string $service_name,
    ?RequestPriority $upstream_priority = null,
  ): ThriftFrameworkMetadata {
    $mutable_tfm = ThriftFrameworkMetadata::withDefaultValues();
    $mutable_tfm->request_priority = $upstream_priority;
    (new RequestPriorityContextHandler())->onOutgoingDownstream(
      shape('service_name' => $service_name),
      $mutable_tfm,
      new ImmutableThriftContextPropState(ThriftContextPropState::get()),
    );
    return $mutable_tfm;
  }

  public async function testPropagatesBronzeToAllowlistedService(
  ): Awaitable<void> {
    self::mockEffectivePriority(RequestPriority::SHEDDABLE_PLUS);

    expect(self::runHandler(self::ALLOWLISTED_SERVICE)->request_priority)
      ->toEqual(RequestPriority::SHEDDABLE_PLUS);
  }

  public async function testPropagatesSheddableToAllowlistedService(
  ): Awaitable<void> {
    self::mockEffectivePriority(RequestPriority::SHEDDABLE);

    expect(self::runHandler(self::ALLOWLISTED_SERVICE)->request_priority)
      ->toEqual(RequestPriority::SHEDDABLE);
  }

  public async function testSkipsServiceNotAllowlisted(): Awaitable<void> {
    self::mockEffectivePriority(RequestPriority::SHEDDABLE_PLUS);

    expect(self::runHandler(self::OTHER_SERVICE)->request_priority)->toBeNull();
  }

  public async function testSkipsNonSheddablePriorities(): Awaitable<void> {
    foreach (
      vec[null, RequestPriority::CRITICAL, RequestPriority::CRITICAL_PLUS] as
        $priority
    ) {
      self::mockEffectivePriority($priority);

      expect(self::runHandler(self::ALLOWLISTED_SERVICE)->request_priority)
        ->toBeNull();
    }
  }

  public async function testKeepsMoreSheddableUpstreamPriority(
  ): Awaitable<void> {
    self::mockEffectivePriority(RequestPriority::SHEDDABLE_PLUS);

    expect(
      self::runHandler(self::ALLOWLISTED_SERVICE, RequestPriority::SHEDDABLE)
        ->request_priority,
    )->toEqual(RequestPriority::SHEDDABLE);
  }

  public async function testDowngradesLessSheddableUpstreamPriority(
  ): Awaitable<void> {
    self::mockEffectivePriority(RequestPriority::SHEDDABLE);

    expect(
      self::runHandler(self::ALLOWLISTED_SERVICE, RequestPriority::CRITICAL)
        ->request_priority,
    )->toEqual(RequestPriority::SHEDDABLE);
  }

  public async function testDoesNotMutateGlobalContextPropState(
  ): Awaitable<void> {
    self::mockEffectivePriority(RequestPriority::SHEDDABLE_PLUS);

    self::runHandler(self::ALLOWLISTED_SERVICE);

    expect(ThriftContextPropState::get()->getRequestPriority())->toBeNull();
  }

  // Regression for S715712: a concurrent call that resumes while another
  // branch's Bronze-scoped call is suspended must not inherit Bronze.
  public async function testScopedPriorityDoesNotLeakIntoConcurrentCall(
  ): Awaitable<void> {
    $concurrent_call = async () ==> {
      await stop_eager_execution();
      return self::runHandler(self::ALLOWLISTED_SERVICE)->request_priority;
    };
    // Created outside the Bronze scope, so it keeps the outer context even
    // though it is awaited (and resumes) inside that scope.
    $concurrent_awaitable = $concurrent_call();

    list($concurrent_priority, $scoped_priority) =
      await TagManager::genTrySetOriginId(
        async () ==> {
          $concurrent_priority = await $concurrent_awaitable;
          return tuple(
            $concurrent_priority,
            self::runHandler(self::ALLOWLISTED_SERVICE)->request_priority,
          );
        },
        MCPContextState::getOrInheritFromCurrent(
          null,
          WwwHackTestAssetXID::get(nameof static),
          RequestPriority::SHEDDABLE_PLUS,
        ),
      );

    expect($scoped_priority)->toEqual(RequestPriority::SHEDDABLE_PLUS);
    expect($concurrent_priority)->toBeNull();
    expect(ThriftContextPropState::get()->getRequestPriority())->toBeNull();
  }

  public async function testIgnoresRequestGlobalPrivacyLibPriorityOutsideScope(
  ): Awaitable<void> {
    self::mockEffectivePriority(null);
    self::mockClassStaticMethodUNSAFE(
      PrivacyLibRequestPriorityState::class,
      'get',
    )->mockReturn(RequestPriority::SHEDDABLE_PLUS);

    expect(self::runHandler(self::ALLOWLISTED_SERVICE)->request_priority)
      ->toBeNull();
  }

  public async function testInstalledWhenKillswitchOff(): Awaitable<void> {
    MockJustKnobs::setBool(self::KILLSWITCH, false);
    self::mockEffectivePriority(RequestPriority::SHEDDABLE_PLUS);

    expect(self::sendThroughInstalledHandlers())->toEqual(
      RequestPriority::SHEDDABLE_PLUS,
    );
  }

  public async function testNotInstalledWhenKillswitchOn(): Awaitable<void> {
    MockJustKnobs::setBool(self::KILLSWITCH, true);
    self::mockEffectivePriority(RequestPriority::SHEDDABLE_PLUS);

    expect(self::sendThroughInstalledHandlers())->toBeNull();
  }

  private static function sendThroughInstalledHandlers(): ?RequestPriority {
    $transport = TServiceRouterTransport::create(
      self::ALLOWLISTED_SERVICE,
      dict[],
      dict[],
    );
    $multi_handler = new TClientMultiEventHandler();
    ThriftContextPropHandlers::install(
      $multi_handler,
      $transport,
      shape('service_name' => self::ALLOWLISTED_SERVICE),
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
