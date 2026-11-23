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

<<Oncalls('thrift')>>
final class ThriftStableOrderTest extends WWWTest {
  use ClassLevelTest;

  public static function providerCompare(): dict<string, shape(
    'spec' => ThriftStructTypes::TGenericSpec,
    'a' => mixed,
    'b' => mixed,
    'expected' => int,
  )> {
    $double = shape('type' => TType::DOUBLE);
    $string = shape('type' => TType::STRING);
    $i32 = shape('type' => TType::I32);
    $double_list = shape(
      'type' => TType::LST,
      'etype' => TType::DOUBLE,
      'elem' => $double,
      'format' => 'harray',
    );
    $i32_set = shape(
      'type' => TType::SET,
      'etype' => TType::I32,
      'elem' => $i32,
      'format' => 'harray',
    );
    $i32_string_map = shape(
      'type' => TType::MAP,
      'ktype' => TType::I32,
      'vtype' => TType::STRING,
      'key' => $i32,
      'val' => $string,
      'format' => 'harray',
    );
    $example = shape(
      'type' => TType::STRUCT,
      'class' => facebook\thrift\json5\Example::class,
    );
    $union = shape(
      'type' => TType::STRUCT,
      'class' => facebook\thrift\json5\ExampleUnion::class,
    );
    $negative_nan = ThriftFloatText::negativeNaN();
    return dict[
      'false before true' => shape(
        'spec' => shape('type' => TType::BOOL),
        'a' => false,
        'b' => true,
        'expected' => -1,
      ),
      'i64' => shape(
        'spec' => shape('type' => TType::I64),
        'a' => Math\INT64_MIN,
        'b' => Math\INT64_MAX,
        'expected' => -1,
      ),
      '-NaN before -Infinity' => shape(
        'spec' => $double,
        'a' => $negative_nan,
        'b' => -INF,
        'expected' => -1,
      ),
      '-0.0 before +0.0' => shape(
        'spec' => $double,
        'a' => (float)'-0',
        'b' => 0.0,
        'expected' => -1,
      ),
      'Infinity before NaN' => shape(
        'spec' => $double,
        'a' => INF,
        'b' => Math\NAN,
        'expected' => -1,
      ),
      'NaN payloads are equivalent' => shape(
        'spec' => $double,
        'a' => Math\NAN,
        'b' => PHP\unpack('E', "\x7F\xF8\0\0\0\0\0\x01")[1] as float,
        'expected' => 0,
      ),
      'float compares binary32 values' => shape(
        'spec' => shape('type' => TType::FLOAT),
        'a' => 0.1,
        'b' => 0.10000000000000002,
        'expected' => 0,
      ),
      'bytes, not numbers' => shape(
        'spec' => $string,
        'a' => '10',
        'b' => '9',
        'expected' => -1,
      ),
      'prefix first' => shape(
        'spec' => $string,
        'a' => 'a',
        'b' => 'ab',
        'expected' => -1,
      ),
      'unsigned bytes' => shape(
        'spec' => $string,
        'a' => 'z',
        'b' => "\xc3\xa9",
        'expected' => -1,
      ),
      'legacy int key as string' => shape(
        'spec' => $string,
        'a' => 1,
        'b' => '1',
        'expected' => 0,
      ),
      'list by first difference' => shape(
        'spec' => $double_list,
        'a' => vec[1.0, 2.0],
        'b' => vec[1.0, 1.0],
        'expected' => 1,
      ),
      'shorter list first' => shape(
        'spec' => $double_list,
        'a' => vec[1.0],
        'b' => vec[1.0, 1.0],
        'expected' => -1,
      ),
      'set as sorted list' => shape(
        'spec' => $i32_set,
        'a' => keyset[3, 1],
        'b' => keyset[2, 1],
        'expected' => 1,
      ),
      'map by sorted keys' => shape(
        'spec' => $i32_string_map,
        'a' => dict[2 => 'x', 1 => 'y'],
        'b' => dict[3 => 'x', 1 => 'y'],
        'expected' => -1,
      ),
      'map by value for equal keys' => shape(
        'spec' => $i32_string_map,
        'a' => dict[1 => 'b'],
        'b' => dict[1 => 'a'],
        'expected' => 1,
      ),
      'unset field first' => shape(
        'spec' => $example,
        'a' => facebook\thrift\json5\Example::withDefaultValues(),
        'b' =>
          facebook\thrift\json5\Example::fromShape(shape('stringValue' => '')),
        'expected' => -1,
      ),
      // Example declares `30: boolValue` first.
      'fields in id order' => shape(
        'spec' => $example,
        'a' => facebook\thrift\json5\Example::fromShape(
          shape('i64Value' => 2, 'boolValue' => false),
        ),
        'b' => facebook\thrift\json5\Example::fromShape(
          shape('i64Value' => 1, 'boolValue' => true),
        ),
        'expected' => 1,
      ),
      'equal structs' => shape(
        'spec' => $example,
        'a' => facebook\thrift\json5\Example::fromShape(
          shape('i64Value' => 1, 'stringValue' => 'a'),
        ),
        'b' => facebook\thrift\json5\Example::fromShape(
          shape('i64Value' => 1, 'stringValue' => 'a'),
        ),
        'expected' => 0,
      ),
      'empty union first' => shape(
        'spec' => $union,
        'a' => new facebook\thrift\json5\ExampleUnion(),
        'b' =>
          facebook\thrift\json5\ExampleUnion::fromShape(shape('intValue' => 1)),
        'expected' => -1,
      ),
      'union with the larger field id first' => shape(
        'spec' => $union,
        'a' => facebook\thrift\json5\ExampleUnion::fromShape(
          shape('stringValue' => 'a'),
        ),
        'b' =>
          facebook\thrift\json5\ExampleUnion::fromShape(shape('intValue' => 1)),
        'expected' => -1,
      ),
      'union members of the same field' => shape(
        'spec' => $union,
        'a' =>
          facebook\thrift\json5\ExampleUnion::fromShape(shape('intValue' => 2)),
        'b' =>
          facebook\thrift\json5\ExampleUnion::fromShape(shape('intValue' => 1)),
        'expected' => 1,
      ),
    ];
  }

  <<DataProvider('providerCompare')>>
  public function testCompare(
    ThriftStructTypes::TGenericSpec $spec,
    mixed $a,
    mixed $b,
    int $expected,
  ): void {
    expect(ThriftStableOrder::compare($spec, $a, $b))
      ->toEqual($expected);
    expect(ThriftStableOrder::compare($spec, $b, $a))
      ->toEqual(-$expected);
  }
}
