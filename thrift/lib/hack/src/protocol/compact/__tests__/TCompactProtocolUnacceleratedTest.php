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
final class TCompactProtocolUnacceleratedTest extends WWWTest {
  use ClassLevelTest;

  public static function providerDouble(): dict<
    string,
    shape('version' => int, 'value' => float, 'expected' => string),
  > {
    return dict[
      'big endian positive' => shape(
        'version' => TCompactProtocolBase::VERSION_DOUBLE_BE,
        'value' => 1.5,
        'expected' => "\x3f\xf8\x00\x00\x00\x00\x00\x00",
      ),
      'big endian negative' => shape(
        'version' => TCompactProtocolBase::VERSION_DOUBLE_BE,
        'value' => -2.0,
        'expected' => "\xc0\x00\x00\x00\x00\x00\x00\x00",
      ),
      'little endian positive' => shape(
        'version' => TCompactProtocolBase::VERSION_LOW,
        'value' => 1.5,
        'expected' => "\x00\x00\x00\x00\x00\x00\xf8\x3f",
      ),
      'little endian negative' => shape(
        'version' => TCompactProtocolBase::VERSION_LOW,
        'value' => -2.0,
        'expected' => "\x00\x00\x00\x00\x00\x00\x00\xc0",
      ),
    ];
  }

  <<DataProvider('providerDouble')>>
  public function testDoubleWireFormat(
    int $version,
    float $value,
    string $expected,
  ): void {
    $buffer = new TMemoryBuffer();
    $prot = new TCompactProtocolUnaccelerated($buffer);
    $prot->setWriteVersion($version);

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
    $prot = new TCompactProtocolUnaccelerated($buffer);

    $prot->writeFloat($value);
    expect($buffer->getBuffer())->toEqual($expected);

    $out = 0.0;
    $prot->readFloat(inout $out);
    expect($out)->toEqual($value);
  }

  public static function providerVarint(
  ): dict<string, shape('value' => int, 'expected' => string)> {
    return dict[
      'zero' => shape('value' => 0, 'expected' => "\x00"),
      'one byte max' => shape('value' => 127, 'expected' => "\x7f"),
      'two bytes min' => shape('value' => 128, 'expected' => "\x80\x01"),
      'two bytes' => shape('value' => 300, 'expected' => "\xac\x02"),
      'three bytes min' =>
        shape('value' => 1 << 14, 'expected' => "\x80\x80\x01"),
      'four bytes max' =>
        shape('value' => (1 << 28) - 1, 'expected' => "\xff\xff\xff\x7f"),
      'i32 max' => shape(
        'value' => Math\INT32_MAX,
        'expected' => "\xff\xff\xff\xff\x07",
      ),
      'eight bytes min' => shape(
        'value' => 1 << 49,
        'expected' => "\x80\x80\x80\x80\x80\x80\x80\x01",
      ),
      'i64 max' => shape(
        'value' => Math\INT64_MAX,
        'expected' => "\xff\xff\xff\xff\xff\xff\xff\xff\x7f",
      ),
      'negative one' => shape(
        'value' => -1,
        'expected' => "\xff\xff\xff\xff\xff\xff\xff\xff\xff\x01",
      ),
      'i64 min' => shape(
        'value' => Math\INT64_MIN,
        'expected' => "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x01",
      ),
    ];
  }

  <<DataProvider('providerVarint')>>
  public function testVarintRoundTrip(int $value, string $expected): void {
    $buffer = new TMemoryBuffer();
    $prot = new TCompactProtocolUnaccelerated($buffer);

    expect($prot->getVarint($value))->toEqual($expected);
    $prot->writeVarint($value);
    expect($buffer->getBuffer())->toEqual($expected);

    $out = 0;
    expect($prot->readVarint(inout $out))->toEqual(Str\length($expected));
    expect($out)->toEqual($value);
  }

  public static function providerI64(): dict<string, shape('value' => int)> {
    return dict[
      'zero' => shape('value' => 0),
      'negative one' => shape('value' => -1),
      'i32 max' => shape('value' => Math\INT32_MAX),
      'just past u32' => shape('value' => 4294967297),
      'just below negative u32' => shape('value' => -4294967297),
      'large positive' => shape('value' => 0x0123456789abcdef),
      'large negative' => shape('value' => -0x0123456789abcdef),
      'i64 max' => shape('value' => Math\INT64_MAX),
      'i64 min' => shape('value' => Math\INT64_MIN),
    ];
  }

  <<DataProvider('providerI64')>>
  public function testI64MatchesExtension(int $value): void {
    $pokemon = thriftshim_Pokemon::withDefaultValues();
    $pokemon->charizard = $value;
    $buffer = new TMemoryBuffer();
    $prot = new TCompactProtocolUnaccelerated($buffer);

    $pokemon->write($prot);
    expect($buffer->getBuffer())->toEqual(
      thrift_protocol_write_compact_struct_to_string(
        $pokemon,
        TCompactProtocolBase::VERSION,
      ),
    );

    $pokemon_in = thriftshim_Pokemon::withDefaultValues();
    $pokemon_in->read($prot);
    expect($pokemon_in->charizard)->toEqual($value);
  }

  // A 10-byte i64 varint that sets bits beyond the 64th: the extra bits are
  // dropped, as the native extension does.
  public function testOverflowingI64VarintMatchesExtension(): void {
    $bytes = "\x36".Str\repeat("\xff", 9)."\x02\x00";
    $pokemon = thriftshim_Pokemon::withDefaultValues();
    $pokemon->read(
      new TCompactProtocolUnaccelerated(new TMemoryBuffer($bytes)),
    );
    $expected = thrift_protocol_read_compact_struct_from_string(
      $bytes,
      nameof thriftshim_Pokemon,
      0,
      TCompactProtocolBase::VERSION,
    ) as thriftshim_Pokemon;
    expect($pokemon->charizard)->toEqual($expected->charizard);
  }

  public function testNestedStructRestoresFieldDelta(): void {
    $buffer = new TMemoryBuffer();
    $prot = new TCompactProtocolUnaccelerated($buffer);

    $prot->writeStructBegin('outer');
    $prot->writeFieldBegin('a', TType::I32, 1);
    $prot->writeI32(7);
    $prot->writeFieldEnd();
    $prot->writeFieldBegin('nested', TType::STRUCT, -5);
    $prot->writeStructBegin('inner');
    $prot->writeFieldBegin('x', TType::I32, 2);
    $prot->writeI32(1);
    $prot->writeFieldEnd();
    $prot->writeFieldStop();
    $prot->writeStructEnd();
    $prot->writeFieldEnd();
    // Encodes as delta 1 only if the outer lastFid (-5) was restored.
    $prot->writeFieldBegin('b', TType::I32, -4);
    $prot->writeI32(3);
    $prot->writeFieldEnd();
    $prot->writeFieldStop();
    $prot->writeStructEnd();
    expect($buffer->getBuffer())
      ->toEqual("\x15\x0e\x0c\x09\x25\x02\x00\x15\x06\x00");

    $name = null;
    $type = null;
    $id = null;
    $value = 0;
    $prot->readStructBegin(inout $name);
    $prot->readFieldBegin(inout $name, inout $type, inout $id);
    $prot->readI32(inout $value);
    expect(tuple($type, $id, $value))->toEqual(tuple(TType::I32, 1, 7));
    $prot->readFieldEnd();
    $prot->readFieldBegin(inout $name, inout $type, inout $id);
    expect(tuple($type, $id))->toEqual(tuple(TType::STRUCT, -5));
    $prot->readStructBegin(inout $name);
    $prot->readFieldBegin(inout $name, inout $type, inout $id);
    $prot->readI32(inout $value);
    expect(tuple($type, $id, $value))->toEqual(tuple(TType::I32, 2, 1));
    $prot->readFieldEnd();
    $prot->readFieldBegin(inout $name, inout $type, inout $id);
    expect($type)->toEqual(TType::STOP);
    $prot->readStructEnd();
    $prot->readFieldEnd();
    $prot->readFieldBegin(inout $name, inout $type, inout $id);
    $prot->readI32(inout $value);
    expect(tuple($type, $id, $value))->toEqual(tuple(TType::I32, -4, 3));
    $prot->readFieldEnd();
    $prot->readFieldBegin(inout $name, inout $type, inout $id);
    expect($type)->toEqual(TType::STOP);
    $prot->readStructEnd();
  }

  public function testListOfStructsRestoresFieldDelta(): void {
    $buffer = new TMemoryBuffer();
    $prot = new TCompactProtocolUnaccelerated($buffer);

    $prot->writeStructBegin('outer');
    $prot->writeFieldBegin('items', TType::LST, 1);
    $prot->writeListBegin(TType::STRUCT, 2);
    foreach (vec[5, 6] as $item) {
      $prot->writeStructBegin('item');
      $prot->writeFieldBegin('v', TType::I32, 1);
      $prot->writeI32($item);
      $prot->writeFieldEnd();
      $prot->writeFieldStop();
      $prot->writeStructEnd();
    }
    $prot->writeListEnd();
    $prot->writeFieldEnd();
    // Encodes as delta 1 only if leaving the list restored lastFid to 1.
    $prot->writeFieldBegin('after', TType::I32, 2);
    $prot->writeI32(7);
    $prot->writeFieldEnd();
    $prot->writeFieldStop();
    $prot->writeStructEnd();
    expect($buffer->getBuffer())
      ->toEqual("\x19\x2c\x15\x0a\x00\x15\x0c\x00\x15\x0e\x00");

    $name = null;
    $type = null;
    $id = null;
    $size = null;
    $value = 0;
    $prot->readStructBegin(inout $name);
    $prot->readFieldBegin(inout $name, inout $type, inout $id);
    expect(tuple($type, $id))->toEqual(tuple(TType::LST, 1));
    $prot->readListBegin(inout $type, inout $size);
    expect(tuple($type, $size))->toEqual(tuple(TType::STRUCT, 2));
    $items = vec[];
    for ($i = 0; $i < 2; $i++) {
      $prot->readStructBegin(inout $name);
      $prot->readFieldBegin(inout $name, inout $type, inout $id);
      $prot->readI32(inout $value);
      $items[] = $value;
      $prot->readFieldEnd();
      $prot->readFieldBegin(inout $name, inout $type, inout $id);
      $prot->readStructEnd();
    }
    expect($items)->toEqual(vec[5, 6]);
    $prot->readListEnd();
    $prot->readFieldEnd();
    $prot->readFieldBegin(inout $name, inout $type, inout $id);
    $prot->readI32(inout $value);
    expect(tuple($type, $id, $value))->toEqual(tuple(TType::I32, 2, 7));
    $prot->readFieldEnd();
    $prot->readFieldBegin(inout $name, inout $type, inout $id);
    expect($type)->toEqual(TType::STOP);
    $prot->readStructEnd();
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
    $prot = new TCompactProtocolUnaccelerated($buffer);

    $struct->write($prot);
    expect($buffer->getBuffer())->toEqual(
      thrift_protocol_write_compact_struct_to_string(
        $struct,
        TCompactProtocolBase::VERSION,
      ),
    );

    $struct_in = $struct::withDefaultValues();
    $struct_in->read($prot);
    expect($struct_in)->toBePHPEqual($struct);
  }

  public static function providerCorrupted(): dict<
    string,
    shape('value' => string, 'expected' => classname<Exception>),
  > {
    return dict[
      'unknown compact type' =>
        shape('value' => "\x1e", 'expected' => nameof OutOfBoundsException),
      'list with unknown element type' => shape(
        'value' => "\x19\x1e",
        'expected' => nameof OutOfBoundsException,
      ),
      'truncated varint' =>
        shape('value' => "\x15\x80", 'expected' => nameof TTransportException),
      'truncated double' => shape(
        'value' => "\x17\x00\x00",
        'expected' => nameof TTransportException,
      ),
      'truncated collection size' => shape(
        'value' => "\x19\xfc\x80",
        'expected' => nameof TTransportException,
      ),
      'string longer than input' => shape(
        'value' => "\x18\x10abc",
        'expected' => nameof TTransportException,
      ),
      'missing stop' =>
        shape('value' => "\x14\x0e", 'expected' => nameof TTransportException),
    ];
  }

  <<DataProvider('providerCorrupted')>>
  public function testCorruptedInputFails(
    string $value,
    classname<Exception> $expected,
  ): void {
    expect(
      () ==> thriftshim_Pokemon::withDefaultValues()
        ->read(new TCompactProtocolUnaccelerated(new TMemoryBuffer($value))),
    )->toThrow($expected);
  }
}
