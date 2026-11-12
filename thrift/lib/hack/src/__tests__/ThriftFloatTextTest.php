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

<<Oncalls('thrift')>>
final class ThriftFloatTextTest extends WWWTest {
  use ClassLevelTest;

  // The double nearest `7.038531e-26` lies exactly halfway between the
  // binary32 values 0x15ae43fd and 0x15ae43fe. C++ rounds the text straight to
  // 0x15ae43fd; HHVM rounds that double to the even 0x15ae43fe.
  public function testRenderFloatHalfway(): void {
    $value = PHP\unpack('g', PHP\pack('V', 0x15ae43fd))[1] as float;
    expect(ThriftFloatText::renderFloat($value, true))
      ->toEqual('7.0385307e-26');
    expect(ThriftFloatText::renderFloat(
      $value,
      true,
      /* read_as_double = */ false,
    ))->toEqual('7.038531e-26');
  }

  public function testRenderFloatScientificFromExponent(): void {
    expect(ThriftFloatText::renderFloat(16777216.0, true))
      ->toEqual('16777216.0');
    expect(ThriftFloatText::renderFloat(
      16777216.0,
      true,
      /* read_as_double = */ false,
      /* scientific_from_exponent = */ 7,
    ))->toEqual('1.6777216e+07');
    expect(ThriftFloatText::renderFloat(
      10000000.0,
      true,
      /* read_as_double = */ false,
      /* scientific_from_exponent = */ 7,
    ))->toEqual('1e+07');
    expect(ThriftFloatText::renderFloat(
      1234567.0,
      true,
      /* read_as_double = */ false,
      /* scientific_from_exponent = */ 7,
    ))->toEqual('1234567.0');
  }

  // `$digits * 10 ** $scale` and the binary32 bits C++ reads it as.
  public static function providerDecimalToFloat32(
  ): dict<string, shape('digits' => string, 'scale' => int, 'bits' => int)> {
    return dict[
      'not halfway' =>
        shape('digits' => '1', 'scale' => -1, 'bits' => 0x3dcccccd),
      // These parse to the double halfway between 0x15ae43fd and 0x15ae43fe.
      'below the halfway point' =>
        shape('digits' => '7038531', 'scale' => -32, 'bits' => 0x15ae43fd),
      'just above the halfway point' => shape(
        'digits' => '703853100000000022281692450609677778769437',
        'scale' => -67,
        'bits' => 0x15ae43fe,
      ),
      // 2^24 + 1 and 2^24 + 3 are halfway points; ties go to the even value.
      'exact tie, even below' =>
        shape('digits' => '16777217', 'scale' => 0, 'bits' => 0x4b800000),
      'exact tie, even above' =>
        shape('digits' => '16777219', 'scale' => 0, 'bits' => 0x4b800002),
      // These parse to 2^-150, halfway between 0 and the smallest subnormal.
      'above the subnormal halfway point' =>
        shape('digits' => '70064923216240854', 'scale' => -62, 'bits' => 1),
      'below the subnormal halfway point' =>
        shape('digits' => '70064923216240853', 'scale' => -62, 'bits' => 0),
      // These parse to 2^128 - 2^103, halfway between FLT_MAX and 2^128.
      'below the halfway point after FLT_MAX' => shape(
        'digits' => '340282356779733661637539395458142568447',
        'scale' => 0,
        'bits' => 0x7f7fffff,
      ),
      'above the halfway point after FLT_MAX' => shape(
        'digits' => '340282356779733661637539395458142568449',
        'scale' => 0,
        'bits' => 0x7f800000,
      ),
    ];
  }

  <<DataProvider('providerDecimalToFloat32')>>
  public function testDecimalToFloat32(
    string $digits,
    int $scale,
    int $bits,
  ): void {
    $value = ThriftFloatText::decimalToFloat32(
      $digits,
      $scale,
      (float)($digits.'e'.$scale),
    );
    expect(PHP\unpack('V', PHP\pack('g', $value))[1])->toEqual($bits);
  }

  public static function providerToBinary32(
  ): dict<string, shape('value' => float, 'expected' => float)> {
    return dict[
      'exact' => shape('value' => 0.5, 'expected' => 0.5),
      'nearest' =>
        shape('value' => 0.1, 'expected' => 0.100000001490116119384765625),
      // 1 + 2^-24 and 1 + 3 * 2^-24 lie halfway between binary32 values.
      'tie to even, down' =>
        shape('value' => 1.000000059604644775390625, 'expected' => 1.0),
      'tie to even, up' => shape(
        'value' => 1.000000178813934326171875,
        'expected' => 1.0000002384185791015625,
      ),
      'FLT_MAX' => shape(
        'value' => 3.4028234663852886e38,
        'expected' => 3.4028234663852886e38,
      ),
      'overflow' => shape('value' => 1e39, 'expected' => INF),
      'underflow' => shape('value' => 1e-46, 'expected' => 0.0),
      '-0.0' => shape('value' => (float)'-0', 'expected' => (float)'-0'),
      'NaN' => shape('value' => Math\NAN, 'expected' => Math\NAN),
      '-NaN' => shape(
        'value' => ThriftFloatText::negativeNaN(),
        'expected' => ThriftFloatText::negativeNaN(),
      ),
    ];
  }

  <<DataProvider('providerToBinary32')>>
  public function testToBinary32(float $value, float $expected): void {
    expect(ThriftFloatText::bits(ThriftFloatText::toBinary32($value)))
      ->toEqual(ThriftFloatText::bits($expected));
  }

  public static function providerBits(
  ): dict<string, shape('value' => float, 'expected' => int)> {
    return dict[
      '0.0' => shape('value' => 0.0, 'expected' => 0),
      '-0.0' => shape('value' => (float)'-0', 'expected' => Math\INT64_MIN),
      '1.0' => shape('value' => 1.0, 'expected' => 0x3FF0000000000000),
      '-2.0' => shape(
        'value' => -2.0,
        'expected' => Math\INT64_MIN | 0x4000000000000000,
      ),
      'Infinity' => shape('value' => INF, 'expected' => 0x7FF0000000000000),
      'NaN' => shape('value' => Math\NAN, 'expected' => 0x7FF8000000000000),
    ];
  }

  <<DataProvider('providerBits')>>
  public function testBits(float $value, int $expected): void {
    expect(ThriftFloatText::bits($value))->toEqual($expected);
  }

  public function testNegativeNaN(): void {
    expect(ThriftFloatText::bits(ThriftFloatText::negativeNaN()))
      ->toEqual(Math\INT64_MIN | 0x7FF8000000000000);
  }

  // The sign of `$digits * 10 ** $scale - $mantissa * 2 ** $exponent`.
  public static function providerCompareDecimal(): dict<string, shape(
    'digits' => string,
    'scale' => int,
    'mantissa' => int,
    'exponent' => int,
    'expected' => int,
  )> {
    return dict[
      '0.5 = 0.5' => shape(
        'digits' => '5',
        'scale' => -1,
        'mantissa' => 1,
        'exponent' => -1,
        'expected' => 0,
      ),
      '0.4 < 0.5' => shape(
        'digits' => '4',
        'scale' => -1,
        'mantissa' => 1,
        'exponent' => -1,
        'expected' => -1,
      ),
      '0.6 > 0.5' => shape(
        'digits' => '6',
        'scale' => -1,
        'mantissa' => 1,
        'exponent' => -1,
        'expected' => 1,
      ),
      'trailing zeros: 1.0 = 1' => shape(
        'digits' => '10',
        'scale' => -1,
        'mantissa' => 1,
        'exponent' => 0,
        'expected' => 0,
      ),
      'fewer digits: 1 < 2' => shape(
        'digits' => '1',
        'scale' => 0,
        'mantissa' => 1,
        'exponent' => 1,
        'expected' => -1,
      ),
      '1000 < 1024' => shape(
        'digits' => '1',
        'scale' => 3,
        'mantissa' => 1,
        'exponent' => 10,
        'expected' => -1,
      ),
      '2000 > 1024' => shape(
        'digits' => '2',
        'scale' => 3,
        'mantissa' => 1,
        'exponent' => 10,
        'expected' => 1,
      ),
      // `(2^53 - 1) * 2^-1075` is halfway between the largest subnormal double
      // and DBL_MIN.
      'just above the DBL_MIN halfway point' => shape(
        'digits' => '22250738585072012',
        'scale' => -324,
        'mantissa' => (1 << 53) - 1,
        'exponent' => -1075,
        'expected' => 1,
      ),
      'just below the DBL_MIN halfway point' => shape(
        'digits' => '22250738585072011',
        'scale' => -324,
        'mantissa' => (1 << 53) - 1,
        'exponent' => -1075,
        'expected' => -1,
      ),
    ];
  }

  <<DataProvider('providerCompareDecimal')>>
  public function testCompareDecimal(
    string $digits,
    int $scale,
    int $mantissa,
    int $exponent,
    int $expected,
  ): void {
    expect(
      ThriftFloatText::compareDecimal($digits, $scale, $mantissa, $exponent),
    )->toEqual($expected);
  }
}
