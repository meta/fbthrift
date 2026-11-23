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

/**
 * Propagates an explicitly tagged Bronze (SHEDDABLE_PLUS) or Sheddable
 * (SHEDDABLE) priority from implicit context onto the outgoing TFM for
 * allowlisted destinations. Other priorities are never written, so untagged
 * traffic keeps sending a null priority.
 */
final class RequestPriorityContextHandler implements IContextHandler {

  const keyset<RequestPriority> PROPAGATED_PRIORITIES = keyset[
    RequestPriority::SHEDDABLE_PLUS,
    RequestPriority::SHEDDABLE,
  ];

  public function onIncomingDownstream(
    ThriftContextPropState $mutable_ctx,
    ClientInstrumentationParams $params,
    ImmutableThriftFrameworkMetadataOnResponse $immutable_tfmr,
  ): void {}

  public function onOutgoingDownstream(
    ClientInstrumentationParams $params,
    ThriftFrameworkMetadata $mutable_tfm,
    ImmutableThriftContextPropState $immutable_ctx,
  ): void {
    // Deliberately no fallback to PrivacyLibRequestPriorityState: it is
    // request-global and last-writer-wins across concurrent branches, so code
    // outside any implicit-context scope could pick up another branch's Bronze.
    $priority = MCPContext::getRequestPriority();
    if (
      $priority is null ||
      !C\contains_key(self::PROPAGATED_PRIORITIES, $priority)
    ) {
      return;
    }
    $service_name = Shapes::idx($params, 'service_name');
    if ($service_name is null) {
      self::logSenderEvent(
        $params,
        null,
        $priority,
        $mutable_tfm->request_priority,
        $mutable_tfm->request_priority,
        'NO_SERVICE_NAME',
        $immutable_ctx->getRequestId(),
      );
      return;
    }
    $upstream = $mutable_tfm->request_priority;
    // A higher value is more sheddable; never upgrade an upstream priority.
    if (
      !JustKnobs::evalString(
        'www/web_efficiency_infra:pl_set_priority_to_tcp',
        $immutable_ctx->getRequestId(),
        $service_name,
      )
    ) {
      $decision = 'NOT_ALLOWLISTED';
    } else if ($upstream is null || ($priority as int) > ($upstream as int)) {
      $mutable_tfm->request_priority = $priority;
      $decision = 'PROPAGATED';
    } else {
      $decision = 'KEPT_UPSTREAM';
    }

    self::logSenderEvent(
      $params,
      $service_name,
      $priority,
      $upstream,
      $mutable_tfm->request_priority,
      $decision,
      $immutable_ctx->getRequestId(),
    );
  }

  private static function logSenderEvent(
    ClientInstrumentationParams $params,
    ?string $receiver_service,
    RequestPriority $effective_priority,
    ?RequestPriority $upstream_priority,
    ?RequestPriority $wire_priority,
    string $decision,
    string $request_id,
  ): void {
    // Best-effort: a throw here would abort the remaining ContextProp handlers
    // and the TFM header write for this outbound call.
    try {
      // Destinations without their own switch fall back to the knob default.
      $sample_denom = JustKnobs::getInt(
        'upop/contextprop_priority:log_sample_denom',
        $receiver_service is null
          ? 'WWW.SENDER.no_service'
          : 'WWW.SENDER.'.$receiver_service,
      );
      if ($sample_denom <= 0 || PseudoRandom\int(1, $sample_denom) !== 1) {
        return;
      }
      $names = RequestPriority::getNames();
      RoContextPropPriorityFunctionalLogger::logOnlyForNonAsyncMethods(shape(
        'event_count' => 1,
        'aggregation_window_sec' => 0,
        'sample_denom' => $sample_denom,
        'reporter_service' => 'www',
        'reporter_platform' => 'WWW',
        'reporter_side' => 'SENDER',
        'log_point' => 'www.contextprop_v2_handler',
        'sender_service' => 'www',
        'receiver_service' => $receiver_service,
        'method' => Shapes::idx($params, 'fn_name'),
        'transport' => 'THRIFT_CONTEXTPROP',
        'wire_priority' =>
          $wire_priority is null ? null : idx($names, $wire_priority),
        'effective_priority' => idx($names, $effective_priority),
        'upstream_priority' =>
          $upstream_priority is null ? null : idx($names, $upstream_priority),
        'decision' => $decision,
        'product_id' => TagManager::getLatestOriginID(),
        'root_product_id' => ThriftContextPropState::get()->getRootProductId(),
        'entrypoint' => RelativeScript::getPath(),
        'request_id' => PHP\bin2hex($request_id),
      ));
    } catch (Exception $e) {
      ope($e, causes_the('RO priority sender logging')->to('be skipped'));
    }
  }

  public function onIncomingUpstream(
    ThriftContextPropState $mutable_ctx,
    ServerInstrumentationParams $params,
    ImmutableThriftFrameworkMetadata $immutable_tfm,
  ): void {}

  public function onOutgoingUpstream(
    ServerInstrumentationParams $params,
    ThriftFrameworkMetadataOnResponse $mutable_tfmr,
    ImmutableThriftContextPropState $immutable_ctx,
  ): void {}
}
