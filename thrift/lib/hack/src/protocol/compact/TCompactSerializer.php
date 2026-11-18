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

use namespace FlibSL\{C, Math, Str, Vec}; // @oss-enable

/**
 * Utility class for serializing and deserializing
 * a thrift object using TCompactProtocol.
 */
// @oss-disable: <<Oncalls('thrift')>>
final class TCompactSerializer extends TProtocolWritePropsSerializer {

  <<__Override>>
  public static function serialize(
    IThriftStruct $object,
    ?int $override_version = null,
    bool $disable_hphp_extension = false,
  )[write_props]: string {
    $override_version ??= TCompactProtocolBase::VERSION;
    $use_hphp_extension = !$disable_hphp_extension &&
      !ThriftSerializationHelper::structContainsObjectKeyContainer($object);

    if ($use_hphp_extension) {
      return HH\Coeffects\fb\backdoor_from_write_props__DO_NOT_USE(
        ()[defaults] ==> thrift_protocol_write_compact_struct_to_string(
          $object,
          $override_version,
        ),
        'Compact with memory buffer would have write_props, but Hack doesn\'t '.
        'have a way to express this atm.',
      );
    }

    $transport = new TMemoryBuffer();
    $protocol = new TCompactProtocolAccelerated($transport);

    if ($override_version !== null) {
      $protocol->setWriteVersion($override_version);
    }

    HH\Coeffects\fb\backdoor_from_write_props__DO_NOT_USE(
      ()[defaults] ==> $object->write($protocol),
      'Compact with memory buffer would have write_props, but Hack doesn\'t '.
      'have a way to express this atm.',
    );
    return $transport->getBuffer();
  }

  public static function deserializeTyped<T as IThriftStruct>(
    string $str,
    T $object,
    ?int $override_version = null,
    bool $disable_hphp_extension = false,
  )[write_props]: T {
    return self::deserialize(
      $str,
      $object,
      $override_version,
      $disable_hphp_extension,
    );
  }

  <<__Override>>
  public static function deserialize<T as IThriftStruct>(
    string $str,
    T $object,
    ?int $override_version = null,
    bool $disable_hphp_extension = false,
    bool $should_leave_extra = false,
    int $options = 0,
  )[write_props]: T {
    $use_hphp_extension = !$disable_hphp_extension &&
      !ThriftSerializationHelper::structContainsObjectKeyContainer($object);

    if ($use_hphp_extension) {
      $override_version ??= TCompactProtocolBase::VERSION;
      return HH\Coeffects\fb\backdoor_from_write_props__DO_NOT_USE(
        ()[defaults] ==> thrift_protocol_read_compact_struct_from_string(
          $str,
          get_class($object),
          $options,
          $override_version,
        ),
        'Compact with memory buffer would have write_props, but Hack doesn\'t '.
        'have a way to express this atm.',
      );
    }

    $transport = new TMemoryBuffer();
    $protocol = (new TCompactProtocolAccelerated($transport))
      ->setOptions($options);

    if ($override_version !== null) {
      $protocol->setWriteVersion($override_version);
    }

    $transport->write($str);
    HH\Coeffects\fb\backdoor_from_write_props__DO_NOT_USE(
      ()[defaults] ==> $object->read($protocol),
      'Compact with memory buffer would have write_props, but Hack doesn\'t '.
      'have a way to express this atm.',
    );
    /* END_STRIP */
    return $object;
  }/* BEGIN_STRIP */

  public static function deserializeThrowing<T as IThriftStruct>(
    string $str,
    T $object,
    ?int $override_version = null,
    bool $disable_hphp_extension = false,
    int $options = 0,
  )[write_props]: T {
    $transport = new TMemoryBuffer();
    $protocol = (new TCompactProtocolAccelerated($transport))
      ->setOptions($options);

    if ($override_version !== null) {
      $protocol->setWriteVersion($override_version);
    }

    if (!$disable_hphp_extension) {
      HH\Coeffects\fb\backdoor_from_write_props__DO_NOT_USE(
        ()[defaults] ==>
          $protocol->writeMessageBegin('', TMessageType::REPLY, 0),
        'Compact with memory buffer would have write_props, but Hack doesn\'t '.
        'have a way to express this atm.',
      );
      $transport->write($str);
      $object = HH\Coeffects\fb\backdoor_from_write_props__DO_NOT_USE(
        ()[defaults] ==> thrift_protocol_read_compact(
          $protocol,
          get_class($object),
          $protocol->getOptions(),
        ),
        'Compact with memory buffer would have write_props, but Hack doesn\'t '.
        'have a way to express this atm.',
      );
    } else {
      $transport->write($str);
      HH\Coeffects\fb\backdoor_from_write_props__DO_NOT_USE(
        ()[defaults] ==> $object->read($protocol),
        'Compact with memory buffer would have write_props, but Hack doesn\'t '.
        'have a way to express this atm.',
      );
    }
    $remaining = $transport->available();
    invariant(
      $remaining === 0,
      "Deserialization didn't consume the whole input string (%d bytes left)".
      "Are you sure this was serialized as a '%s'?",
      $remaining,
      get_class($object),
    );
    return $object;
  }

  public static function deserialize_DEPRECATED(
    string $string_object,
    string $class_name,
    ?int $override_version = null,
    bool $disable_hphp_extension = false,
  )[write_props]: IThriftStruct {
    $class_name = ArgAssert::isClassname($class_name, nameof IThriftStruct);
    return static::deserialize(
      (string)$string_object,
      HH\classname_to_class($class_name) |> new $$(),
      $override_version,
      $disable_hphp_extension,
    );
  }

  <<__Override>>
  public static function serializeData(
    // If type spec contains "adapter" or "wrapper" fields,
    // then $object is expected to be the adapted/wrapped value.
    mixed $object,
    ThriftStructTypes::TGenericSpec $type_spec,
  )[write_props]: string {
    $transport = new TMemoryBuffer();
    $protocol = new TCompactProtocolAccelerated($transport);
    // A top-level bool can't go through writeStructHelper, so the adapter is
    // applied here.
    $bool = $type_spec['type'] === TType::BOOL
      ? ThriftSerializationHelper::unwrapApplyAdapter($object, $type_spec)
      : $object;
    if ($type_spec['type'] === TType::BOOL && $bool is bool) {
      HH\Coeffects\fb\backdoor_from_write_props__DO_NOT_USE(
        ()[defaults] ==> $protocol->writeByte(
          $bool
            ? TCompactProtocolBase::COMPACT_TRUE
            : TCompactProtocolBase::COMPACT_FALSE,
        ),
        '[T133628451] Compact with memory buffer would have write_props, but Hack doesn\'t '.
        'have a way to express this atm.',
      );
    } else {
      HH\Coeffects\fb\backdoor_from_write_props__DO_NOT_USE(
        ()[defaults] ==> ThriftSerializationHelper::writeStructHelper(
          $protocol,
          $type_spec['type'],
          $object,
          $type_spec,
        ),
        '[T133628451] Memory buffer transport would have write_props, but Hack doesn\'t '.
        'have a way to express this atm.',
      );
    }
    return $transport->getBuffer();
  }

  <<__Override>>
  public static function deserializeData(
    string $str,
    ThriftStructTypes::TGenericSpec $type_spec,
  )[write_props]: mixed {
    $transport = new TMemoryBuffer();
    $transport->write($str);
    $protocol = new TCompactProtocolAccelerated($transport);
    if ($type_spec['type'] === TType::BOOL) {
      return HH\Coeffects\fb\backdoor_from_write_props__DO_NOT_USE(
        ()[defaults] ==> {
          $result = 0;
          $protocol->readByte(inout $result);
          return ThriftSerializationHelper::applyAdapterFromThrift(
            $result === TCompactProtocolBase::COMPACT_TRUE,
            $type_spec,
          );
        },
        '[T133628451] Compact with memory buffer would have write_props, but Hack doesn\'t '.
        'have a way to express this atm.',
      );
    } else {
      return HH\Coeffects\fb\backdoor_from_write_props__DO_NOT_USE(
        ()[defaults] ==> {
          $result = null;
          $has_wrapper = false;
          ThriftSerializationHelper::readStructHelper(
            $protocol,
            $type_spec['type'],
            inout $result,
            $type_spec,
            inout $has_wrapper,
          );
          return $result;
        },
        '[T133628451] Compact with memory buffer would have write_props, but Hack doesn\'t '.
        'have a way to express this atm.',
      );
    }
  }

  /* END_STRIP */
}
