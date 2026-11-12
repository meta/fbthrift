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
   * 1e16, scientific outside it. `$scientific_from_exponent` moves the upper
   * bound.
   *
   * A `float` candidate is checked as HHVM reads it (to a double, then to
   * binary32), or with `$read_as_double = false` as C++ does (straight to
   * binary32). They differ for e.g. `7.038531e-26`.
   */
  public static function renderFloat(
    float $value,
    bool $single,
    bool $read_as_double = true,
    int $scientific_from_exponent = 16,
  )[]: string {
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
    $reads_back = (string $candidate, string $digits, int $exponent)[] ==> {
      $parsed = (float)$candidate;
      if (!$single) {
        return $parsed === $target;
      }
      if ($read_as_double) {
        return self::toBinary32($parsed) === $target;
      }
      return self::decimalToFloat32(
        $digits,
        $exponent - Str\length($digits) + 1,
        Math\abs($parsed),
      ) ===
        Math\abs($target);
    };
    for ($length = 1; $length <= 17; $length++) {
      // The two `$length`-digit neighbours of the value. Either can be the
      // only one that reads back: just below a power of two, the values are
      // twice as dense on one side.
      $kept = Str\slice($digits, 0, $length);
      $dropped = Str\slice($digits, $length);
      $down_digits = Str\trim_right($kept, '0') |> $$ === '' ? '0' : $$;
      $down = self::formatDecimal(
        $sign,
        $down_digits,
        $exponent,
        $scientific_from_exponent,
      );
      $up_exponent = $exponent;
      $up_digits = self::incrementDigits($kept, inout $up_exponent);
      $up = self::formatDecimal(
        $sign,
        $up_digits,
        $up_exponent,
        $scientific_from_exponent,
      );
      $down_reads_back = $reads_back($down, $down_digits, $exponent);
      $up_reads_back = $reads_back($up, $up_digits, $up_exponent);
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
      $scientific_from_exponent,
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
    int $scientific_from_exponent,
  )[]: string {
    if ($exponent >= -4 && $exponent < $scientific_from_exponent) {
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
  public static function toBinary32(float $value)[]: float {
    return PHP\unpack('G', PHP\pack('G', $value))[1] as float;
  }

  public static function bits(float $value)[]: int {
    return PHP\unpack('J', PHP\pack('E', $value))[1] as int;
  }

  // No HHVM arithmetic sets a NaN's sign bit: unary minus is `0 - x`, and
  // `NAN * -1.0` stays positive.
  public static function negativeNaN()[]: float {
    return PHP\unpack('E', "\xff\xf8\0\0\0\0\0\0")[1] as float;
  }

  /**
   * The positive decimal `$digits * 10 ** $scale` rounded to binary32, as C++
   * reads it, given `$back = (float)$text`. Rounding `$back` to binary32 rounds
   * twice, which is wrong when `$back` lies exactly halfway between two
   * binary32 values: C++ reads `7.038531e-26` as 0x15ae43fd, but `$back` is
   * halfway between 0x15ae43fd and 0x15ae43fe and rounds to the even one. Then
   * the digits decide, as in Clinger's "How to Read Floating Point Numbers
   * Accurately" (https://doi.org/10.1145/989393.989430).
   */
  public static function decimalToFloat32(
    string $digits,
    int $scale,
    float $back,
  )[]: float {
    // `$back` is `$mantissa * 2 ** $exponent`; binary32 values near it are
    // multiples of `2 ** ($exponent + $shift)`.
    $bits = self::bits($back);
    $mantissa = ($bits & 0xFFFFFFFFFFFFF) | (1 << 52);
    $exponent = ($bits >> 52) - 1075;
    $shift = Math\maxva(29, -149 - $exponent);
    if (
      $shift > 53 || ($mantissa & ((1 << $shift) - 1)) !== 1 << ($shift - 1)
    ) {
      return self::toBinary32($back);
    }
    $cmp = self::compareDecimal($digits, $scale, $mantissa, $exponent);
    $half = 2.0 ** ($exponent + $shift - 1);
    return self::toBinary32(
      $cmp === 0 ? $back : ($cmp < 0 ? $back - $half : $back + $half),
    );
  }

  /**
   * The sign of `$digits * 10 ** $scale - $mantissa * 2 ** $exponent`, for
   * `$digits` without leading zeros and a positive `$mantissa`.
   */
  public static function compareDecimal(
    string $digits,
    int $scale,
    int $mantissa,
    int $exponent,
  )[]: int {
    // 2 ** -n is 5 ** n * 10 ** -n.
    $value = (string)$mantissa;
    for ($n = Math\abs($exponent); $n > 0; $n--) {
      $product = '';
      $carry = 0;
      for ($i = Str\length($value) - 1; $i >= 0; $i--) {
        $carry += (int)$value[$i] * ($exponent >= 0 ? 2 : 5);
        $product = (string)($carry % 10).$product;
        $carry = Math\int_div($carry, 10);
      }
      $value = ($carry > 0 ? (string)$carry : '').$product;
    }
    $value_scale = Math\minva($exponent, 0);
    $unit = Math\minva($scale, $value_scale);
    $digits .= Str\repeat('0', $scale - $unit);
    $value .= Str\repeat('0', $value_scale - $unit);
    $order = Str\length($digits) <=> Str\length($value);
    return $order !== 0 ? $order : Str\compare($digits, $value) <=> 0;
  }
}
