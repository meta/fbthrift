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
 * Binary implementation of the Thrift protocol.
 *
 * Assumes 64-bit ints: there is no hi/lo split for i64, and decodeI32
 * sign-extends through a 64-bit int.
 */
// @oss-disable: <<Oncalls('thrift')>>
abstract class TBinaryProtocolBase extends TProtocol {
  const ctx CReadWriteDefault = [zoned_shallow];

  const VERSION_MASK = 0xffff0000;
  const VERSION_1 = 0x80010000;
  const DEFAULT_MEMORY_LIMIT = 128000000; //128M, the default

  protected int $memoryLimit = self::DEFAULT_MEMORY_LIMIT;
  protected ?int $sequenceID = null;

  public function __construct(
    TTransport $trans,
    protected bool $strictRead = false,
    protected bool $strictWrite = true,
  )[read_globals] {
    parent::__construct($trans);
    $this->memoryLimit = MemoryLimit::get() ?? self::DEFAULT_MEMORY_LIMIT;
  }

  <<__Override>>
  public function writeMessageBegin(
    string $name,
    TMessageType $type,
    int $seqid,
  )[zoned_shallow]: int {
    if ($this->strictWrite) {
      $version = self::VERSION_1 | $type;
      return $this->writeI32($version) +
        $this->writeString($name) +
        $this->writeI32($seqid);
    } else {
      return $this->writeString($name) +
        $this->writeByte($type) +
        $this->writeI32($seqid);
    }
  }

  <<__Override>>
  public function writeMessageEnd()[]: int {
    return 0;
  }

  <<__Override>>
  public function writeStructBegin(string $_name)[]: int {
    return 0;
  }

  <<__Override>>
  public function writeStructEnd()[]: int {
    return 0;
  }

  <<__Override>>
  public function writeFieldBegin(
    string $_field_name,
    TType $field_type,
    int $field_id,
  )[zoned_shallow]: int {
    $this->trans_->write(PHP\pack('cn', $field_type, $field_id));
    return 3;
  }

  <<__Override>>
  public function writeFieldEnd()[]: int {
    return 0;
  }

  <<__Override>>
  public function writeFieldStop()[zoned_shallow]: int {
    return $this->writeByte(TType::STOP);
  }

  <<__Override>>
  public function writeMapBegin(
    TType $key_type,
    TType $val_type,
    int $size,
  )[zoned_shallow]: int {
    $this->trans_->write(PHP\pack('ccN', $key_type, $val_type, $size));
    return 6;
  }

  <<__Override>>
  public function writeMapEnd()[]: int {
    return 0;
  }

  <<__Override>>
  public function writeListBegin(
    TType $elem_type,
    int $size,
  )[zoned_shallow]: int {
    $this->trans_->write(PHP\pack('cN', $elem_type, $size));
    return 5;
  }

  <<__Override>>
  public function writeListEnd()[]: int {
    return 0;
  }

  <<__Override>>
  public function writeSetBegin(
    TType $elem_type,
    int $size,
  )[zoned_shallow]: int {
    $this->trans_->write(PHP\pack('cN', $elem_type, $size));
    return 5;
  }

  <<__Override>>
  public function writeSetEnd()[]: int {
    return 0;
  }

  <<__Override>>
  public function writeBool(bool $value)[zoned_shallow]: int {
    $this->trans_->write($value ? "\x01" : "\x00");
    return 1;
  }

  <<__Override>>
  public function writeByte(int $value)[zoned_shallow]: int {
    $this->trans_->write(PHP\chr($value));
    return 1;
  }

  <<__Override>>
  public function writeI16(int $value)[zoned_shallow]: int {
    $this->trans_->write(PHP\pack('n', $value));
    return 2;
  }

  <<__Override>>
  public function writeI32(int $value)[zoned_shallow]: int {
    $this->trans_->write(PHP\pack('N', $value));
    return 4;
  }

  <<__Override>>
  public function writeI64(?int $value)[zoned_shallow]: int {
    $this->trans_->write(PHP\pack('J', $value ?? 0));
    return 8;
  }

  <<__Override>>
  public function writeDouble(float $value)[zoned_shallow]: int {
    $this->trans_->write(PHP\pack('E', $value));
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
    $this->trans_->write(PHP\pack('N', $len));
    if ($len !== 0) {
      $this->trans_->write($value);
    }
    return 4 + $len;
  }

  // ord() arithmetic instead of unpack() for 2 and 4 bytes: unpack's format
  // parsing and result dict made i16 decoding 4x slower.
  private static function decodeI16(string $data, int $offset)[]: int {
    $value = (PHP\ord($data[$offset]) << 8) | PHP\ord($data[$offset + 1]);
    return $value > 0x7fff ? $value - 0x10000 : $value;
  }

  private static function decodeI32(string $data, int $offset)[]: int {
    $value = (PHP\ord($data[$offset]) << 24) |
      (PHP\ord($data[$offset + 1]) << 16) |
      (PHP\ord($data[$offset + 2]) << 8) |
      PHP\ord($data[$offset + 3]);
    return $value > 0x7fffffff ? $value - 0x100000000 : $value;
  }

  /**
   * Returns the sequence ID of the next message; only valid when called
   * before readMessageBegin()
   */
  <<__Deprecated(
    'This function was found unused by CodemodRuleDeprecateUnusedClassMethod',
  )>>
  public function peekSequenceID()[zoned_shallow]: int {
    $trans = $this->trans_;
    if (!($trans is IThriftBufferedTransport)) {
      throw new TProtocolException(
        get_class($this->trans_).' does not support peek',
        TProtocolException::BAD_VERSION,
      );
    }
    if ($this->sequenceID !== null) {
      throw new TProtocolException(
        'peekSequenceID can only be called '.'before readMessageBegin',
        TProtocolException::INVALID_DATA,
      );
    }

    $data = $trans->peek(4);
    $sz = self::decodeI32($data, 0);
    $start = 4;

    if ($sz < 0) {
      $version = $sz & self::VERSION_MASK;
      if ($version !== self::VERSION_1) {
        throw new TProtocolException(
          'Bad version identifier: '.$sz,
          TProtocolException::BAD_VERSION,
        );
      }
      // skip name string
      $data = $trans->peek(4, $start);
      $name_len = self::decodeI32($data, 0);
      $start += 4 + $name_len;
      // peek seqId
      $data = $trans->peek(4, $start);
      $seqid = self::decodeI32($data, 0);
    } else {
      if ($this->strictRead) {
        throw new TProtocolException(
          'No version identifier, old protocol client?',
          TProtocolException::BAD_VERSION,
        );
      } else {
        // need to guard the length from mis-configured other type of TCP server
        // for example, if mis-configure sshd, will read 'SSH-' as the length
        // if memory limit is -1, means no limit.
        if ($this->memoryLimit > 0 && $sz > $this->memoryLimit) {
          throw new TProtocolException(
            'Length overflow: '.$sz,
            TProtocolException::SIZE_LIMIT,
          );
        }
        // Handle pre-versioned input
        $start += $sz;
        // skip type byte
        $start += 1;
        // peek seqId
        $data = $trans->peek(4, $start);
        $seqid = self::decodeI32($data, 0);
      }
    }
    return $seqid;
  }

  <<__Override>>
  public function readMessageBegin(
    inout string $name,
    inout int $type,
    inout int $seqid,
  )[zoned_shallow]: int {
    $sz = 0;
    $result = $this->readI32(inout $sz);
    if ($sz < 0) {
      $version = $sz & self::VERSION_MASK;
      if ($version !== self::VERSION_1) {
        throw new TProtocolException(
          'Bad version identifier: '.$sz,
          TProtocolException::BAD_VERSION,
        );
      }
      $type = $sz & 0x000000ff;
      // TODO (partisan): Refactor this as a part of inout deprecation in read
      // methods.
      $name_nonnull = '';
      $name_res = $this->readString(inout $name_nonnull);
      $name = $name_nonnull;
      // TODO (partisan): Refactor this as a part of inout deprecation in read
      // methods.
      $seqid_nonnull = -1;
      $seqid_res = $this->readI32(inout $seqid_nonnull);
      $seqid = $seqid_nonnull;
      $result += $name_res + $seqid_res;
    } else {
      if ($this->strictRead) {
        throw new TProtocolException(
          'No version identifier, old protocol client?',
          TProtocolException::BAD_VERSION,
        );
      } else {
        // need to guard the length from mis-configured other type of TCP server
        // for example, if mis-configure sshd, will read 'SSH-' as the length
        // if memory limit is -1, means no limit.
        if ($this->memoryLimit > 0 && $sz > $this->memoryLimit) {
          throw new TProtocolException(
            'Length overflow: '.$sz,
            TProtocolException::SIZE_LIMIT,
          );
        }
        // Handle pre-versioned input
        $name = $this->trans_->readAll($sz);
        $type_byte = -1;
        $type_res = $this->readByte(inout $type_byte);
        $type = TMessageType::assert($type_byte);
        // TODO (partisan): Refactor this as a part of inout deprecation in read
        // methods.
        $seqid_nonnull = -1;
        $seqid_res = $this->readI32(inout $seqid_nonnull);
        $seqid = $seqid_nonnull;
        $result += $sz + $type_res + $seqid_res;
      }
    }
    $this->sequenceID = $seqid;
    return $result;
  }

  <<__Override>>
  public function readMessageEnd()[write_props]: int {
    $this->sequenceID = null;
    return 0;
  }

  <<__Override>>
  public function readStructBegin(inout ?string $name)[]: int {
    $name = '';
    return 0;
  }

  <<__Override>>
  public function readStructEnd()[]: int {
    return 0;
  }

  <<__Override>>
  public function readFieldBegin(
    inout ?string $_name,
    inout ?TType $field_type,
    inout ?int $field_id,
  )[zoned_shallow]: int {
    $field_type = TType::assert(PHP\ord($this->trans_->readAll(1)));
    if ($field_type === TType::STOP) {
      $field_id = 0;
      return 1;
    }
    $field_id = self::decodeI16($this->trans_->readAll(2), 0);
    return 3;
  }

  <<__Override>>
  public function readFieldEnd()[]: int {
    return 0;
  }

  <<__Override>>
  public function readMapBegin(
    inout ?TType $key_type,
    inout ?TType $val_type,
    inout ?int $size,
  )[zoned_shallow]: int {
    $header = $this->trans_->readAll(6);
    $key_type = TType::assert(PHP\ord($header[0]));
    $val_type = TType::assert(PHP\ord($header[1]));
    $size = self::decodeI32($header, 2);
    return 6;
  }

  <<__Override>>
  public function readMapEnd()[]: int {
    return 0;
  }

  <<__Override>>
  public function readListBegin(
    inout ?TType $elem_type,
    inout ?int $size,
  )[zoned_shallow]: int {
    $header = $this->trans_->readAll(5);
    $elem_type = TType::assert(PHP\ord($header[0]));
    $size = self::decodeI32($header, 1);
    return 5;
  }

  <<__Override>>
  public function readListEnd()[]: int {
    return 0;
  }

  <<__Override>>
  public function readSetBegin(
    inout ?TType $elem_type,
    inout ?int $size,
  )[zoned_shallow]: int {
    $header = $this->trans_->readAll(5);
    $elem_type = TType::assert(PHP\ord($header[0]));
    $size = self::decodeI32($header, 1);
    return 5;
  }

  <<__Override>>
  public function readSetEnd()[]: int {
    return 0;
  }

  <<__Override>>
  public function readBool(inout bool $value)[zoned_shallow]: int {
    $value = PHP\ord($this->trans_->readAll(1)) === 1;
    return 1;
  }

  <<__Override>>
  public function readByte(inout int $value)[zoned_shallow]: int {
    $value = PHP\ord($this->trans_->readAll(1));
    if ($value > 0x7f) {
      $value = 0 - (($value - 1) ^ 0xff);
    }
    return 1;
  }

  <<__Override>>
  public function readI16(inout int $value)[zoned_shallow]: int {
    $value = self::decodeI16($this->trans_->readAll(2), 0);
    return 2;
  }

  <<__Override>>
  public function readI32(inout int $value)[zoned_shallow]: int {
    $value = self::decodeI32($this->trans_->readAll(4), 0);
    return 4;
  }

  <<__Override>>
  public function readI64(inout int $value)[zoned_shallow]: int {
    $value = PHP\unpack('J', $this->trans_->readAll(8))[1] as int;
    return 8;
  }

  <<__Override>>
  public function readDouble(inout float $value)[zoned_shallow]: int {
    $value = PHP\unpack('E', $this->trans_->readAll(8))[1] as float;
    return 8;
  }

  <<__Override>>
  public function readFloat(inout float $value)[zoned_shallow]: int {
    $value = PHP\unpack('G', $this->trans_->readAll(4))[1] as float;
    return 4;
  }

  <<__Override>>
  public function readString(inout string $value)[zoned_shallow]: int {
    $len = self::decodeI32($this->trans_->readAll(4), 0);
    if ($len !== 0) {
      $value = $this->trans_->readAll($len);
    } else {
      $value = '';
    }
    return 4 + $len;
  }
}
