<?hh
// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.
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

use namespace FlibSL\{C, Dict, Math, Str, Vec}; // @oss-enable

/**
 * The Thrift object model's stable order of values (`isStableLessThan`):
 *
 * - bool: false first; integers and enums: by value
 * - float and double: IEEE 754 total order with all NaNs of one sign equal,
 *   -NaN < -Infinity < ... < -0.0 < +0.0 < ... < Infinity < NaN
 * - string and binary: bytewise, a prefix first
 * - list: element by element, a prefix first; set and map: the same over
 *   their sorted elements and entries
 * - struct: field by field in id order, an unset field first; union: the empty
 *   union first, then the larger field id first
 */
// @oss-disable: <<Oncalls('thrift')>>
abstract final class ThriftStableOrder {

  /** The set's elements, converted from their adapters, in stable order. */
  public static function sortedSetElements(
    ThriftStructTypes::TGenericSpec $set_spec,
    mixed $set,
  )[write_props]: vec<mixed> {
    $espec = Shapes::at($set_spec, 'elem');
    // Legacy arrays hold the elements as keys.
    $elements = Shapes::idx($set_spec, 'format') === 'array'
      ? Vec\keys($set as KeyedTraversable<_, _>)
      : vec($set as Traversable<_>);
    return Vec\sort(
      Vec\map(
        $elements,
        $e ==> ThriftSerializationHelper::unwrapApplyAdapter($e, $espec),
      ),
      ($a, $b) ==> self::compare($espec, $a, $b),
    );
  }

  /** The map's entries, converted from their adapters, in stable order. */
  public static function sortedMapEntries(
    ThriftStructTypes::TGenericSpec $map_spec,
    mixed $map,
  )[write_props]: vec<(mixed, mixed)> {
    $kspec = Shapes::at($map_spec, 'key');
    $vspec = Shapes::at($map_spec, 'val');
    return Vec\sort(
      Vec\map_with_key(
        $map as KeyedTraversable<_, _>,
        ($k, $v) ==> tuple(
          ThriftSerializationHelper::unwrapApplyAdapter($k, $kspec),
          ThriftSerializationHelper::unwrapApplyAdapter($v, $vspec),
        ),
      ),
      ($a, $b) ==> self::compareEntries($kspec, $vspec, $a, $b),
    );
  }

  /**
   * -1, 0 or 1 as `$a` is ordered before, equivalent to or after `$b`. Both are
   * values of `$spec`, already converted from their adapters.
   */
  public static function compare(
    ThriftStructTypes::TGenericSpec $spec,
    mixed $a,
    mixed $b,
  )[write_props]: int {
    switch ($spec['type']) {
      case TType::BOOL:
        return (HH\legacy_is_truthy($a) ? 1 : 0) <=>
          (HH\legacy_is_truthy($b) ? 1 : 0);
      case TType::BYTE:
      case TType::I16:
      case TType::I32:
      case TType::I64:
        return (int)$a <=> (int)$b;
      case TType::FLOAT:
        return self::compareFloats(
          ThriftFloatText::toBinary32((float)$a),
          ThriftFloatText::toBinary32((float)$b),
        );
      case TType::DOUBLE:
        return self::compareFloats((float)$a, (float)$b);
      case TType::STRING:
        // Never `<=>`: HHVM compares numeric strings as numbers.
        return Str\compare((string)$a, (string)$b) <=> 0;
      case TType::LST:
        $espec = Shapes::at($spec, 'elem');
        $elements = (mixed $list) ==> Vec\map(
          $list as Traversable<_>,
          $e ==> ThriftSerializationHelper::unwrapApplyAdapter($e, $espec),
        );
        return self::lexicographic(
          $elements($a),
          $elements($b),
          ($x, $y) ==> self::compare($espec, $x, $y),
        );
      case TType::SET:
        $espec = Shapes::at($spec, 'elem');
        return self::lexicographic(
          self::sortedSetElements($spec, $a),
          self::sortedSetElements($spec, $b),
          ($x, $y) ==> self::compare($espec, $x, $y),
        );
      case TType::MAP:
        $kspec = Shapes::at($spec, 'key');
        $vspec = Shapes::at($spec, 'val');
        return self::lexicographic(
          self::sortedMapEntries($spec, $a),
          self::sortedMapEntries($spec, $b),
          ($x, $y) ==> self::compareEntries($kspec, $vspec, $x, $y),
        );
      case TType::STRUCT:
        return self::compareStructs($a as IThriftStruct, $b as IThriftStruct);
      default:
        invariant_violation('Unexpected type %d', $spec['type']);
    }
  }

  private static function compareEntries(
    ThriftStructTypes::TGenericSpec $kspec,
    ThriftStructTypes::TGenericSpec $vspec,
    (mixed, mixed) $a,
    (mixed, mixed) $b,
  )[write_props]: int {
    $result = self::compare($kspec, $a[0], $b[0]);
    return $result !== 0 ? $result : self::compare($vspec, $a[1], $b[1]);
  }

  private static function compareFloats(float $a, float $b)[]: int {
    if (Math\is_nan($a) || Math\is_nan($b)) {
      return self::nanRank($a) <=> self::nanRank($b);
    }
    $result = $a <=> $b;
    // Only -0.0 and +0.0 are both equal and distinct.
    return
      $result !== 0 ? $result : (self::signRank($a) <=> self::signRank($b));
  }

  private static function nanRank(float $value)[]: int {
    return Math\is_nan($value) ? self::signRank($value) : 0;
  }

  private static function signRank(float $value)[]: int {
    return ThriftFloatText::bits($value) < 0 ? -1 : 1;
  }

  // A union holds at most one field, so this orders the empty union first and
  // then the larger field id first.
  private static function compareStructs(
    IThriftStruct $a,
    IThriftStruct $b,
  )[write_props]: int {
    foreach (Dict\sort_by_key($a::SPEC) as $fspec) {
      $a_value = self::fieldValue($a, $fspec);
      $b_value = self::fieldValue($b, $fspec);
      $result = $a_value is null || $b_value is null
        ? ($a_value is null ? 0 : 1) <=> ($b_value is null ? 0 : 1)
        : self::compare($fspec, $a_value, $b_value);
      if ($result !== 0) {
        return $result;
      }
    }
    return 0;
  }

  private static function fieldValue(
    IThriftStruct $object,
    ThriftStructTypes::TGenericSpec $fspec,
  )[write_props]: mixed {
    return ThriftSerializationHelper::unwrapApplyAdapter(
      ThriftSerializationHelper::readStructField($object, $fspec),
      $fspec,
    );
  }

  private static function lexicographic<T>(
    vec<T> $a,
    vec<T> $b,
    (function(T, T)[write_props]: int) $compare,
  )[write_props]: int {
    foreach (Vec\zip($a, $b) as list($x, $y)) {
      $result = $compare($x, $y);
      if ($result !== 0) {
        return $result;
      }
    }
    return C\count($a) <=> C\count($b);
  }
}
