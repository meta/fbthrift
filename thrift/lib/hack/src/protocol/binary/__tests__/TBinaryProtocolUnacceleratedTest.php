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
final class TBinaryProtocolUnacceleratedTest extends WWWTest {
  use ClassLevelTest;

  public static function providerI16(
  ): dict<string, shape('value' => int, 'expected' => string)> {
    return dict[
      'zero' => shape('value' => 0, 'expected' => "\x00\x00"),
      'one' => shape('value' => 1, 'expected' => "\x00\x01"),
      'negative one' => shape('value' => -1, 'expected' => "\xff\xff"),
      'max' => shape('value' => Math\INT16_MAX, 'expected' => "\x7f\xff"),
      'min' => shape('value' => Math\INT16_MIN, 'expected' => "\x80\x00"),
    ];
  }

  <<DataProvider('providerI16')>>
  public function testI16WireFormat(int $value, string $expected): void {
    $buffer = new TMemoryBuffer();
    $prot = new TBinaryProtocolUnaccelerated($buffer);

    $prot->writeI16($value);
    expect($buffer->getBuffer())->toEqual($expected);

    $out = 0;
    $prot->readI16(inout $out);
    expect($out)->toEqual($value);
  }

  public static function providerI32(
  ): dict<string, shape('value' => int, 'expected' => string)> {
    return dict[
      'zero' => shape('value' => 0, 'expected' => "\x00\x00\x00\x00"),
      'byte order' =>
        shape('value' => 0x01020304, 'expected' => "\x01\x02\x03\x04"),
      'negative one' => shape('value' => -1, 'expected' => "\xff\xff\xff\xff"),
      'max' =>
        shape('value' => Math\INT32_MAX, 'expected' => "\x7f\xff\xff\xff"),
      'min' =>
        shape('value' => Math\INT32_MIN, 'expected' => "\x80\x00\x00\x00"),
    ];
  }

  <<DataProvider('providerI32')>>
  public function testI32WireFormat(int $value, string $expected): void {
    $buffer = new TMemoryBuffer();
    $prot = new TBinaryProtocolUnaccelerated($buffer);

    $prot->writeI32($value);
    expect($buffer->getBuffer())->toEqual($expected);

    $out = 0;
    $prot->readI32(inout $out);
    expect($out)->toEqual($value);
  }

  public static function providerI64(
  ): dict<string, shape('value' => int, 'expected' => string)> {
    return dict[
      'zero' =>
        shape('value' => 0, 'expected' => "\x00\x00\x00\x00\x00\x00\x00\x00"),
      'byte order' => shape(
        'value' => 0x0102030405060708,
        'expected' => "\x01\x02\x03\x04\x05\x06\x07\x08",
      ),
      'negative one' =>
        shape('value' => -1, 'expected' => "\xff\xff\xff\xff\xff\xff\xff\xff"),
      'max' => shape(
        'value' => Math\INT64_MAX,
        'expected' => "\x7f\xff\xff\xff\xff\xff\xff\xff",
      ),
      'min' => shape(
        'value' => Math\INT64_MIN,
        'expected' => "\x80\x00\x00\x00\x00\x00\x00\x00",
      ),
    ];
  }

  <<DataProvider('providerI64')>>
  public function testI64WireFormat(int $value, string $expected): void {
    $buffer = new TMemoryBuffer();
    $prot = new TBinaryProtocolUnaccelerated($buffer);

    $prot->writeI64($value);
    expect($buffer->getBuffer())->toEqual($expected);

    $out = 0;
    $prot->readI64(inout $out);
    expect($out)->toEqual($value);
  }

  public static function providerDouble(
  ): dict<string, shape('value' => float, 'expected' => string)> {
    return dict[
      'positive' =>
        shape('value' => 1.5, 'expected' => "\x3f\xf8\x00\x00\x00\x00\x00\x00"),
      'negative' => shape(
        'value' => -2.0,
        'expected' => "\xc0\x00\x00\x00\x00\x00\x00\x00",
      ),
    ];
  }

  <<DataProvider('providerDouble')>>
  public function testDoubleWireFormat(float $value, string $expected): void {
    $buffer = new TMemoryBuffer();
    $prot = new TBinaryProtocolUnaccelerated($buffer);

    $prot->writeDouble($value);
    expect($buffer->getBuffer())->toEqual($expected);

    $out = 0.0;
    $prot->readDouble(inout $out);
    expect($out)->toEqual($value);
  }

  public static function providerFloat(
  ): dict<string, shape('value' => float, 'expected' => string)> {
    return dict[
      'positive' => shape('value' => 1.5, 'expected' => "\x3f\xc0\x00\x00"),
      'negative' => shape('value' => -2.0, 'expected' => "\xc0\x00\x00\x00"),
    ];
  }

  <<DataProvider('providerFloat')>>
  public function testFloatWireFormat(float $value, string $expected): void {
    $buffer = new TMemoryBuffer();
    $prot = new TBinaryProtocolUnaccelerated($buffer);

    $prot->writeFloat($value);
    expect($buffer->getBuffer())->toEqual($expected);

    $out = 0.0;
    $prot->readFloat(inout $out);
    expect($out)->toEqual($value);
  }

  public function testHeaderWireFormat(): void {
    $buffer = new TMemoryBuffer();
    $prot = new TBinaryProtocolUnaccelerated($buffer);

    expect($prot->writeFieldBegin('f', TType::I32, -3))->toEqual(3);
    expect($prot->writeListBegin(TType::STRING, 70000))->toEqual(5);
    expect($prot->writeSetBegin(TType::I64, 2))->toEqual(5);
    expect($prot->writeMapBegin(TType::STRING, TType::I32, 1))->toEqual(6);
    expect($prot->writeFieldStop())->toEqual(1);
    expect($buffer->getBuffer())->toEqual(
      "\x08\xff\xfd".
      "\x0b\x00\x01\x11\x70".
      "\x0a\x00\x00\x00\x02".
      "\x0b\x08\x00\x00\x00\x01".
      "\x00",
    );

    $name = null;
    $type = null;
    $key_type = null;
    $id = null;
    $size = null;
    expect($prot->readFieldBegin(inout $name, inout $type, inout $id))
      ->toEqual(3);
    expect(tuple($type, $id))->toEqual(tuple(TType::I32, -3));
    expect($prot->readListBegin(inout $type, inout $size))->toEqual(5);
    expect(tuple($type, $size))->toEqual(tuple(TType::STRING, 70000));
    expect($prot->readSetBegin(inout $type, inout $size))->toEqual(5);
    expect(tuple($type, $size))->toEqual(tuple(TType::I64, 2));
    expect($prot->readMapBegin(inout $key_type, inout $type, inout $size))
      ->toEqual(6);
    expect(tuple($key_type, $type, $size))
      ->toEqual(tuple(TType::STRING, TType::I32, 1));
    expect($prot->readFieldBegin(inout $name, inout $type, inout $id))
      ->toEqual(1);
    expect($type)->toEqual(TType::STOP);
  }

  public static function providerStructs(
  ): dict<string, shape('struct' => IThriftSyncStruct)> {
    $kudo = thriftshim_Kudo::withDefaultValues();
    $kudo->is_awesome = true;
    $kudo->awesome_score = -7;
    $pokemon = thriftshim_Pokemon::withDefaultValues();
    $pokemon->charmander = -43;
    $pokemon->charmeleon = 107373182;
    $pokemon->charizard = Math\INT64_MIN;
    $foobar = thriftshim_FooBar::withDefaultValues();
    $foobar->foo = Vector {4.5, -2.3};
    $foobar->bar = 'Test';
    return dict[
      'bool and byte' => shape('struct' => $kudo),
      'integers' => shape('struct' => $pokemon),
      'double list and string' => shape('struct' => $foobar),
    ];
  }

  <<DataProvider('providerStructs')>>
  public function testStructMatchesExtension(IThriftSyncStruct $struct): void {
    $buffer = new TMemoryBuffer();
    $prot = new TBinaryProtocolUnaccelerated($buffer);

    $struct->write($prot);
    expect($buffer->getBuffer())
      ->toEqual(thrift_protocol_write_binary_struct_to_string($struct));

    $struct_in = $struct::withDefaultValues();
    $struct_in->read($prot);
    expect($struct_in)->toBePHPEqual($struct);
  }

  public static function providerCorrupted(): dict<
    string,
    shape('value' => string, 'expected' => classname<Exception>),
  > {
    return dict[
      'unknown field type' => shape(
        'value' => "\x7f\x00\x01",
        'expected' => nameof UnexpectedValueException,
      ),
      'negative field type' => shape(
        'value' => "\xff\x00\x01",
        'expected' => nameof UnexpectedValueException,
      ),
      'truncated field id' =>
        shape('value' => "\x06\x00", 'expected' => nameof TTransportException),
      'truncated value' => shape(
        'value' => "\x08\x00\x02\x00\x00",
        'expected' => nameof TTransportException,
      ),
      'string longer than input' => shape(
        'value' => "\x0b\x00\x05\x00\x00\x00\x10abc",
        'expected' => nameof TTransportException,
      ),
      'list with unknown element type' => shape(
        'value' => "\x0f\x00\x05\x7f\x00\x00\x00\x01",
        'expected' => nameof UnexpectedValueException,
      ),
      'truncated map header' => shape(
        'value' => "\x0d\x00\x05\x0b\x08\x00",
        'expected' => nameof TTransportException,
      ),
      'missing stop' => shape(
        'value' => "\x06\x00\x01\x00\x05",
        'expected' => nameof TTransportException,
      ),
    ];
  }

  <<DataProvider('providerCorrupted')>>
  public function testCorruptedInputFails(
    string $value,
    classname<Exception> $expected,
  ): void {
    expect(
      () ==> thriftshim_Pokemon::withDefaultValues()
        ->read(new TBinaryProtocolUnaccelerated(new TMemoryBuffer($value))),
    )->toThrow($expected);
  }
}
