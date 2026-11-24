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
 * Compact implementation of the Thrift protocol.
 *
 */
// @oss-disable: <<Oncalls('thrift')>>
abstract class TCompactProtocolBase extends TProtocol {
  const ctx CReadWriteDefault = [zoned_shallow];

  const COMPACT_STOP = 0x00;
  const COMPACT_TRUE = 0x01;
  const COMPACT_FALSE = 0x02;
  const COMPACT_BYTE = 0x03;
  const COMPACT_I16 = 0x04;
  const COMPACT_I32 = 0x05;
  const COMPACT_I64 = 0x06;
  const COMPACT_DOUBLE = 0x07;
  const COMPACT_BINARY = 0x08;
  const COMPACT_LIST = 0x09;
  const COMPACT_SET = 0x0A;
  const COMPACT_MAP = 0x0B;
  const COMPACT_STRUCT = 0x0C;
  const COMPACT_FLOAT = 0x0D;

  const STATE_CLEAR = 0;
  const STATE_FIELD_WRITE = 1;
  const STATE_VALUE_WRITE = 2;
  const STATE_CONTAINER_WRITE = 3;
  const STATE_BOOL_WRITE = 4;
  const STATE_FIELD_READ = 5;
  const STATE_CONTAINER_READ = 6;
  const STATE_VALUE_READ = 7;
  const STATE_BOOL_READ = 8;

  const VERSION_MASK = 0x1f;
  const VERSION = 2;
  const VERSION_LOW = 1;
  const VERSION_DOUBLE_BE = 2;
  const PROTOCOL_ID = 0x82;
  const TYPE_MASK = 0xe0;
  const TYPE_SHIFT_AMOUNT = 5;

  const int I16_MIN = -(1 << 15);
  const int I16_MAX = (1 << 15) - 1;
  const int I32_MIN = -(1 << 31);
  const int I32_MAX = (1 << 31) - 1;

  const dict<TType, int> CTYPES = dict[
    TType::STOP => self::COMPACT_STOP,
    TType::BOOL => self::COMPACT_TRUE, // used for collection
    TType::BYTE => self::COMPACT_BYTE,
    TType::I16 => self::COMPACT_I16,
    TType::I32 => self::COMPACT_I32,
    TType::I64 => self::COMPACT_I64,
    TType::DOUBLE => self::COMPACT_DOUBLE,
    TType::FLOAT => self::COMPACT_FLOAT,
    TType::STRING => self::COMPACT_BINARY,
    TType::STRUCT => self::COMPACT_STRUCT,
    TType::LST => self::COMPACT_LIST,
    TType::SET => self::COMPACT_SET,
    TType::MAP => self::COMPACT_MAP,
  ];

  const dict<int, TType> TTYPES = dict[
    self::COMPACT_STOP => TType::STOP,
    self::COMPACT_TRUE => TType::BOOL, // used for collection
    self::COMPACT_FALSE => TType::BOOL,
    self::COMPACT_BYTE => TType::BYTE,
    self::COMPACT_I16 => TType::I16,
    self::COMPACT_I32 => TType::I32,
    self::COMPACT_I64 => TType::I64,
    self::COMPACT_DOUBLE => TType::DOUBLE,
    self::COMPACT_FLOAT => TType::FLOAT,
    self::COMPACT_BINARY => TType::STRING,
    self::COMPACT_STRUCT => TType::STRUCT,
    self::COMPACT_LIST => TType::LST,
    self::COMPACT_SET => TType::SET,
    self::COMPACT_MAP => TType::MAP,
  ];

  protected int $state = self::STATE_CLEAR;
  protected int $lastFid = 0;
  protected ?int $boolFid = null;
  protected ?bool $boolValue = null;
  protected int $version = self::VERSION;
  // (state, lastFid) pairs for open structs and containers. Slots are reused
  // rather than popped, so struct boundaries stop allocating once the stack
  // has grown. Containers never move lastFid, so one stack covers both.
  private vec<int> $frames = vec[];
  private int $sp = 0;

  protected function pushFrame()[write_props]: void {
    $sp = $this->sp;
    if ($sp === C\count($this->frames)) {
      $this->frames[] = $this->state;
      $this->frames[] = $this->lastFid;
    } else {
      $this->frames[$sp] = $this->state;
      $this->frames[$sp + 1] = $this->lastFid;
    }
    $this->sp = $sp + 2;
  }

  private function popFrame()[write_props]: void {
    $sp = $this->sp - 2;
    $this->sp = $sp;
    $this->state = $this->frames[$sp];
    $this->lastFid = $this->frames[$sp + 1];
  }

  public function setWriteVersion(int $ver)[write_props]: void {
    $this->version = $ver;
  }

  public function checkRange(int $value, int $min, int $max)[]: void {
    if ($value < $min || $value > $max) {
      throw new TProtocolException("Value is out of range");
    }
  }

  // Some varint / zigzag helper methods
  public function toZigZag(int $n, int $bits)[]: int {
    return ($n << 1) ^ ($n >> ($bits - 1));
  }

  public function fromZigZag(int $n)[]: int {
    return ($n >> 1) ^ -($n & 1);
  }

  // Folds the 7-bit groups and their continuation bits into one little-endian
  // integer and packs it once, instead of building the string a byte at a
  // time. Negative values encode as their 64-bit two's complement (10 bytes),
  // which requires the masked (logical) shift below.
  public function getVarint(int $data)[]: string {
    if (($data & ~0x7f) === 0) {
      return PHP\chr($data);
    }
    $word = 0;
    $shift = 0;
    while (($data & ~0x7f) !== 0 && $shift < 64) {
      $word |= (($data & 0x7f) | 0x80) << $shift;
      $data = ($data >> 7) & 0x01ffffffffffffff;
      $shift += 8;
    }
    if ($shift === 64) {
      // 9 or 10 bytes: the first 8 fill the word.
      return PHP\pack('P', $word).$this->getVarint($data);
    }
    $word |= $data << $shift;
    switch ($shift) {
      case 8:
        return PHP\pack('v', $word);
      case 24:
        return PHP\pack('V', $word);
      case 56:
        return PHP\pack('P', $word);
      default:
        return Str\slice(PHP\pack('P', $word), 0, ($shift >> 3) + 1);
    }
  }

  public function writeVarint(int $data)[zoned_shallow]: int {
    $out = $this->getVarint($data);
    $this->trans_->write($out);
    return Str\length($out);
  }

  // Most varints are one byte; return before setting up the loop.
  public function readVarint(inout int $result)[zoned_shallow]: int {
    $result = PHP\ord($this->trans_->readAll(1));
    if ($result < 0x80) {
      return 1;
    }
    $result &= 0x7f;
    $idx = 1;
    $shift = 7;
    while (true) {
      $byte = PHP\ord($this->trans_->readAll(1));
      $idx += 1;
      $result |= ($byte & 0x7f) << $shift;
      if (($byte >> 7) === 0) {
        return $idx;
      }
      $shift += 7;
    }
  }

  public function __construct(TTransport $trans)[] {
    parent::__construct($trans);
  }

  <<__Override>>
  public function writeMessageBegin(
    string $name,
    TMessageType $type,
    int $seqid,
  )[zoned_shallow]: int {
    $written = $this->writeUByte(self::PROTOCOL_ID) +
      $this->writeUByte($this->version | ($type << self::TYPE_SHIFT_AMOUNT)) +
      $this->writeVarint($seqid) +
      $this->writeString($name);
    $this->state = self::STATE_VALUE_WRITE;
    return $written;
  }

  <<__Override>>
  public function writeMessageEnd()[write_props]: int {
    $this->state = self::STATE_CLEAR;
    return 0;
  }

  <<__Override>>
  public function writeStructBegin(string $_name)[write_props]: int {
    $this->pushFrame();
    $this->state = self::STATE_FIELD_WRITE;
    $this->lastFid = 0;
    return 0;
  }

  <<__Override>>
  public function writeStructEnd()[write_props]: int {
    $this->popFrame();
    return 0;
  }

  <<__Override>>
  public function writeFieldStop()[zoned_shallow]: int {
    return $this->writeByte(0);
  }

  public function writeFieldHeader(int $type, int $fid)[zoned_shallow]: int {
    $delta = $fid - $this->lastFid;
    if (0 < $delta && $delta <= 15) {
      $written = $this->writeUByte(($delta << 4) | $type);
    } else {
      $written = $this->writeByte($type) + $this->writeI16($fid);
    }
    $this->lastFid = $fid;
    return $written;
  }

  <<__Override>>
  public function writeFieldBegin(
    string $_field_name,
    TType $field_type,
    int $field_id,
  )[zoned_shallow]: int {
    if ($field_type === TType::BOOL) {
      $this->state = self::STATE_BOOL_WRITE;
      $this->boolFid = $field_id;
      return 0;
    } else {
      $this->state = self::STATE_VALUE_WRITE;
      return $this->writeFieldHeader(self::CTYPES[$field_type], $field_id);
    }
  }

  <<__Override>>
  public function writeFieldEnd()[write_props]: int {
    $this->state = self::STATE_FIELD_WRITE;
    return 0;
  }

  public function writeCollectionBegin(
    TType $etype,
    int $size,
  )[zoned_shallow]: int {
    if ($size <= 14) {
      $header = PHP\chr($size << 4 | self::CTYPES[$etype]);
    } else {
      $header = PHP\chr(0xf0 | self::CTYPES[$etype]).$this->getVarint($size);
    }
    $this->trans_->write($header);
    $this->pushFrame();
    $this->state = self::STATE_CONTAINER_WRITE;
    return Str\length($header);
  }

  <<__Override>>
  public function writeMapBegin(
    TType $key_type,
    TType $val_type,
    int $size,
  )[zoned_shallow]: int {
    if ($size === 0) {
      $header = "\x00";
    } else {
      $header = $this->getVarint($size).
        PHP\chr(self::CTYPES[$key_type] << 4 | self::CTYPES[$val_type]);
    }
    $this->trans_->write($header);
    $this->pushFrame();
    $this->state = self::STATE_CONTAINER_WRITE;
    return Str\length($header);
  }

  public function writeCollectionEnd()[write_props]: int {
    $this->popFrame();
    return 0;
  }

  <<__Override>>
  public function writeMapEnd()[write_props]: int {
    return $this->writeCollectionEnd();
  }

  <<__Override>>
  public function writeListBegin(
    TType $elem_type,
    int $size,
  )[zoned_shallow]: int {
    return $this->writeCollectionBegin($elem_type, $size);
  }

  <<__Override>>
  public function writeListEnd()[write_props]: int {
    return $this->writeCollectionEnd();
  }

  <<__Override>>
  public function writeSetBegin(
    TType $elem_type,
    int $size,
  )[zoned_shallow]: int {
    return $this->writeCollectionBegin($elem_type, $size);
  }

  <<__Override>>
  public function writeSetEnd()[write_props]: int {
    return $this->writeCollectionEnd();
  }

  <<__Override>>
  public function writeBool(bool $value)[zoned_shallow]: int {
    if ($this->state === self::STATE_BOOL_WRITE) {
      $ctype = self::COMPACT_FALSE;
      if ($value) {
        $ctype = self::COMPACT_TRUE;
      }
      // This worked before with a strict type in writeFieldHeader
      // TODO (partisan): Rewrite to avoid null
      return $this->writeFieldHeader($ctype, (int)$this->boolFid);
    } else if ($this->state === self::STATE_CONTAINER_WRITE) {
      return
        $this->writeByte($value ? self::COMPACT_TRUE : self::COMPACT_FALSE);
    } else {
      throw new TProtocolException('Invalid state in compact protocol');
    }
  }

  <<__Override>>
  public function writeByte(int $value)[zoned_shallow]: int {
    $this->trans_->write(PHP\chr($value));
    return 1;
  }

  public function writeUByte(int $byte)[zoned_shallow]: int {
    $this->trans_->write(PHP\chr($byte));
    return 1;
  }

  <<__Override>>
  public function writeI16(int $value)[zoned_shallow]: int {
    $this->checkRange($value, self::I16_MIN, self::I16_MAX);
    $thing = $this->toZigZag($value, 16);
    return $this->writeVarint($thing);
  }

  <<__Override>>
  public function writeI32(int $value)[zoned_shallow]: int {
    $this->checkRange($value, self::I32_MIN, self::I32_MAX);
    $thing = $this->toZigZag($value, 32);
    return $this->writeVarint($thing);
  }

  <<__Override>>
  public function writeDouble(float $value)[zoned_shallow]: int {
    $format = $this->version >= self::VERSION_DOUBLE_BE ? 'E' : 'e';
    $this->trans_->write(PHP\pack($format, $value));
    return 8;
  }

  <<__Override>>
  public function writeFloat(float $value)[zoned_shallow]: int {
    $this->trans_->write(PHP\pack('G', $value));
    return 4;
  }

  <<__Override>>
  public function writeString(string $value)[zoned_shallow]: int {
    $len = Str\length($value);
    $result = $this->writeVarint($len);
    if ($len !== 0) {
      $this->trans_->write($value);
    }
    return $result + $len;
  }

  <<__Override>>
  public function readFieldBegin(
    inout ?string $name,
    inout ?TType $field_type,
    inout ?int $field_id,
  )[zoned_shallow]: int {
    $field_type_ubyte = 0;
    $result = $this->readUByte(inout $field_type_ubyte);
    $delta = $field_type_ubyte >> 4;
    $field_type = $field_type_ubyte & 0x0f;

    if ($field_type === self::COMPACT_STOP) {
      $field_id = 0;
      $field_type = $this->getTType($field_type_ubyte);
      return $result;
    }

    if ($delta === 0) {
      // TODO (partisan): Remove this after refactoring read methods to not use
      // inout.
      $field_id_nonnull = 0;
      $result += $this->readI16(inout $field_id_nonnull);
      $field_id = $field_id_nonnull;
    } else {
      $field_id = $this->lastFid + $delta;
    }
    $this->lastFid = $field_id;

    if ($field_type === self::COMPACT_TRUE) {
      $this->state = self::STATE_BOOL_READ;
      $this->boolValue = true;
    } else if ($field_type === self::COMPACT_FALSE) {
      $this->state = self::STATE_BOOL_READ;
      $this->boolValue = false;
    } else {
      $this->state = self::STATE_VALUE_READ;
    }

    $field_type = $this->getTType($field_type_ubyte);
    return $result;
  }

  <<__Override>>
  public function readFieldEnd()[write_props]: int {
    $this->state = self::STATE_FIELD_READ;
    return 0;
  }

  public function readUByte(inout int $value)[zoned_shallow]: int {
    $value = PHP\ord($this->trans_->readAll(1));
    return 1;
  }

  <<__Override>>
  public function readByte(inout int $value)[zoned_shallow]: int {
    $value = PHP\ord($this->trans_->readAll(1));
    $value = $value > 0x7f ? 0 - (($value - 1) ^ 0xff) : $value;
    return 1;
  }

  public function readZigZag(inout int $value)[zoned_shallow]: int {
    $result = $this->readVarint(inout $value);
    $value = $this->fromZigZag($value);
    return $result;
  }

  <<__Override>>
  public function readMessageBegin(
    inout string $name,
    inout int $type,
    inout int $seqid,
  )[zoned_shallow]: int {
    $protoId = 0;
    $result = $this->readUByte(inout $protoId);
    if ($protoId !== self::PROTOCOL_ID) {
      throw new TProtocolException('Bad protocol id in TCompact message');
    }
    $verType = 0;
    $result += $this->readUByte(inout $verType);
    $type = ($verType & self::TYPE_MASK) >> self::TYPE_SHIFT_AMOUNT;
    $this->version = $verType & self::VERSION_MASK;
    if (
      !($this->version <= self::VERSION && $this->version >= self::VERSION_LOW)
    ) {
      throw new TProtocolException('Bad version in TCompact message');
    }
    $result += $this->readVarint(inout $seqid);
    $result += $this->readString(inout $name);

    return $result;
  }

  <<__Override>>
  public function readMessageEnd()[]: int {
    return 0;
  }

  <<__Override>>
  public function readStructBegin(inout ?string $name)[write_props]: int {
    $name = '';
    $this->pushFrame();
    $this->state = self::STATE_FIELD_READ;
    $this->lastFid = 0;
    return 0;
  }

  <<__Override>>
  public function readStructEnd()[write_props]: int {
    $this->popFrame();
    return 0;
  }

  public function readCollectionBegin(
    inout ?TType $type,
    inout ?int $size,
  )[zoned_shallow]: int {
    $size_type = PHP\ord($this->trans_->readAll(1));
    $result = 1;
    $size = $size_type >> 4;
    $type = $this->getTType($size_type);
    if ($size === 15) {
      $result += $this->readVarint(inout $size);
    }
    $this->pushFrame();
    $this->state = self::STATE_CONTAINER_READ;
    return $result;
  }

  <<__Override>>
  public function readMapBegin(
    inout ?TType $key_type,
    inout ?TType $val_type,
    inout ?int $size,
  )[zoned_shallow]: int {
    $size_nonnull = -1;
    $result = $this->readVarint(inout $size_nonnull);
    $size = $size_nonnull;
    $types = 0;
    if ($size > 0) {
      $types = PHP\ord($this->trans_->readAll(1));
      $result += 1;
    }
    $val_type = $this->getTType($types);
    $key_type = $this->getTType($types >> 4);
    $this->pushFrame();
    $this->state = self::STATE_CONTAINER_READ;
    return $result;
  }

  public function readCollectionEnd()[write_props]: int {
    $this->popFrame();
    return 0;
  }

  <<__Override>>
  public function readMapEnd()[write_props]: int {
    return $this->readCollectionEnd();
  }

  <<__Override>>
  public function readListBegin(
    inout ?TType $elem_type,
    inout ?int $size,
  )[zoned_shallow]: int {
    // TODO (partisan): Remove after refactoring of read methods to not use
    // inout.
    $size_nonnull = -1;
    $result = $this->readCollectionBegin(inout $elem_type, inout $size_nonnull);
    $size = $size_nonnull;
    return $result;
  }

  <<__Override>>
  public function readListEnd()[write_props]: int {
    return $this->readCollectionEnd();
  }

  <<__Override>>
  public function readSetBegin(
    inout ?TType $elem_type,
    inout ?int $size,
  )[zoned_shallow]: int {
    // TODO (partisan): Remove after refactoring of read methods to not use
    // inout.
    $size_nonnull = -1;
    $result = $this->readCollectionBegin(inout $elem_type, inout $size_nonnull);
    $size = $size_nonnull;
    return $result;
  }

  <<__Override>>
  public function readSetEnd()[write_props]: int {
    return $this->readCollectionEnd();
  }

  <<__Override>>
  public function readBool(inout bool $value)[zoned_shallow]: int {
    if ($this->state === self::STATE_BOOL_READ) {
      // TODO (partisan): Refactor the code so this check is no longer
      // necessary.
      if ($this->boolValue is null) {
        throw new TProtocolException(
          '$this->boolValue shouldn\'t be null in the STATE_BOOL_READ.',
        );
      }
      $value = $this->boolValue;
      return 0;
    } else if ($this->state === self::STATE_CONTAINER_READ) {
      $value_byte = -1;
      $result = $this->readByte(inout $value_byte);
      $value = $value_byte === self::COMPACT_TRUE;
      return $result;
    } else {
      throw new TProtocolException('Invalid state in compact protocol');
    }
  }

  <<__Override>>
  public function readI16(inout int $value)[zoned_shallow]: int {
    return $this->readZigZag(inout $value);
  }

  <<__Override>>
  public function readI32(inout int $value)[zoned_shallow]: int {
    return $this->readZigZag(inout $value);
  }

  <<__Override>>
  public function readDouble(inout float $value)[zoned_shallow]: int {
    $format = $this->version >= self::VERSION_DOUBLE_BE ? 'E' : 'e';
    $value = PHP\unpack($format, $this->trans_->readAll(8))[1] as float;
    return 8;
  }

  <<__Override>>
  public function readFloat(inout float $value)[zoned_shallow]: int {
    $value = PHP\unpack('G', $this->trans_->readAll(4))[1] as float;
    return 4;
  }

  <<__Override>>
  public function readString(inout string $value)[zoned_shallow]: int {
    $len = 0;
    $result = $this->readVarint(inout $len);
    if ($len !== 0) {
      $value = $this->trans_->readAll($len);
    } else {
      $value = '';
    }
    return $result + $len;
  }

  public function getTType(int $byte)[]: TType {
    return self::TTYPES[$byte & 0x0f];
  }

  <<__Override>>
  public function readI64(inout int $value)[zoned_shallow]: int {
    $result = $this->readVarint(inout $value);
    // Logical shift: an i64's zig-zag form can have its top bit set, which an
    // arithmetic shift would copy down.
    $value = (($value >> 1) & 0x7fffffffffffffff) ^ -($value & 1);
    return $result;
  }

  <<__Override>>
  public function writeI64(int $value)[zoned_shallow]: int {
    return $this->writeVarint($this->toZigZag($value, 64));
  }
}
