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

use namespace FlibSL\{Math, Str}; // @oss-enable

// @oss-disable: <<Oncalls('thrift')>>
abstract final class ThriftFloatText {

  /**
   * A floating-point value as the shortest literal that reads back as the same
   * value: the same double for a `double`, and for a `float` the same binary32
   * value, which arrives widened to a double.
   *
   *   1234567.0            ->   1234567.0
   *   0.30000000000000004  ->   0.30000000000000004
   *   0.800000011920929    ->   0.8 as a `float`, 0.800000011920929 as a `double`
   *   1e20                 ->   1e+20
   *
   * The form is the one Python's `repr()` picks: positional from 1e-4 up to
   * 1e16, scientific outside it.
   */
  public static function renderFloat(float $value, bool $single)[]: string {
    if (Math\is_nan($value) || !PHP\is_finite($value)) {
      return Str\format('%g', $value);
    }
    // 40 correctly rounded digits, enough to round to the 17 or fewer that
    // read back as the same double without rounding twice.
    $body = Str\format('%.39e', Math\abs($value));
    // From the sign bit, which `%e` drops from -0.0.
    $sign = Str::ord(PHP\pack('E', $value)[0]) >= 0x80 ? '-' : '';
    $e = Str\search($body, 'e') as nonnull;
    $digits = Str\replace(Str\slice($body, 0, $e), '.', '');
    $exponent = (int)Str\slice($body, $e + 1);
    $target = $single ? self::toBinary32($value) : $value;
    $reads_back = (string $candidate)[] ==> {
      $parsed = (float)$candidate;
      return ($single ? self::toBinary32($parsed) : $parsed) === $target;
    };
    for ($length = 1; $length <= 17; $length++) {
      // The two `$length`-digit neighbours of the value. Either can be the
      // only one that reads back: just below a power of two, the values are
      // twice as dense on one side.
      $kept = Str\slice($digits, 0, $length);
      $dropped = Str\slice($digits, $length);
      $down = self::formatDecimal(
        $sign,
        Str\trim_right($kept, '0') |> $$ === '' ? '0' : $$,
        $exponent,
      );
      $up_exponent = $exponent;
      $up_digits = self::incrementDigits($kept, inout $up_exponent);
      $up = self::formatDecimal($sign, $up_digits, $up_exponent);
      $down_reads_back = $reads_back($down);
      $up_reads_back = $reads_back($up);
      if ($down_reads_back && $up_reads_back) {
        // Both are shortest; take the nearer, and on a tie the even one.
        $half = Str\pad_right('5', Str\length($dropped), '0');
        $order = Str\compare($dropped, $half);
        if ($order === 0) {
          return (int)$kept[$length - 1] % 2 === 0 ? $down : $up;
        }
        return $order < 0 ? $down : $up;
      }
      if ($down_reads_back) {
        return $down;
      }
      if ($up_reads_back) {
        return $up;
      }
    }
    return self::formatDecimal(
      $sign,
      Str\trim_right($digits, '0') |> $$ === '' ? '0' : $$,
      $exponent,
    );
  }

  /**
   * `$digits` plus one in the last place, without trailing zeros. A carry out
   * of the leading digit raises `$exponent`.
   */
  private static function incrementDigits(
    string $digits,
    inout int $exponent,
  )[]: string {
    $length = Str\length($digits);
    $carried = '';
    $carry = true;
    for ($i = $length - 1; $i >= 0; $i--) {
      $digit = (int)$digits[$i] + ($carry ? 1 : 0);
      $carry = $digit === 10;
      $carried = (string)($digit % 10).$carried;
    }
    if ($carry) {
      $carried = '1'.Str\slice($carried, 0, $length - 1);
      $exponent++;
    }
    $carried = Str\trim_right($carried, '0');
    return $carried === '' ? '0' : $carried;
  }

  /** Significant `$digits` scaled by `10 ** $exponent`, as a Hack literal. */
  private static function formatDecimal(
    string $sign,
    string $digits,
    int $exponent,
  )[]: string {
    if ($exponent >= -4 && $exponent < 16) {
      if ($exponent < 0) {
        return $sign.'0.'.Str\repeat('0', -$exponent - 1).$digits;
      }
      $whole =
        Str\pad_right(Str\slice($digits, 0, $exponent + 1), $exponent + 1, '0');
      $fraction =
        Str\slice($digits, Math\minva($exponent + 1, Str\length($digits)));
      return $sign.$whole.'.'.($fraction === '' ? '0' : $fraction);
    }
    $mantissa =
      Str\length($digits) > 1 ? $digits[0].'.'.Str\slice($digits, 1) : $digits;
    return $sign.
      $mantissa.
      'e'.
      ($exponent < 0 ? '-' : '+').
      Str\pad_left((string)Math\abs($exponent), 2, '0');
  }

  /** The binary32 value nearest `$value`, widened back to a double. */
  private static function toBinary32(float $value)[]: float {
    return PHP\unpack('G', PHP\pack('G', $value))[1] as float;
  }
}
