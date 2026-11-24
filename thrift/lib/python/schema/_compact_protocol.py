# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""
The fbthrift Compact protocol (version 2) in pure Python, following
``thrift/lib/cpp2/protocol/CompactProtocol-inl.h``.

* A field header is ``(delta << 4) | ctype`` when the id is 1-15 above the
  previous field's, and otherwise ``[ctype][zigzag varint id]``. Each struct
  saves and restores the previous field id.
* A bool field has no value byte: its value is the header's ctype (1 or 2).
* A list or set header is ``(size << 4) | ctype`` for sizes up to 14, and
  otherwise ``[0xF0 | ctype][varint size]``. An empty map is ``0x00``;
  otherwise its header is ``[varint size][key ctype << 4 | value ctype]``.
* Integers are zigzag varints, except byte. Floats and doubles are
  big-endian, unlike Apache's compact protocol and ``CompactV1Protocol``.

Readers follow C++ on odd input: a varint may be at most as long as its type
allows (5 bytes for i16 and i32, 10 for i64), but bits beyond the type's
width are dropped; field ids wrap to i16; and a bool element byte other than
1 reads as false.
"""

from __future__ import annotations

import struct
from typing import ClassVar

from thrift.lib.python.schema._binary_protocol import (
    BufferReader,
    check_binary_size,
    STRING_TTYPES,
)
from thrift.lib.python.schema._errors import DecodeError
from thrift.lib.python.schema._ttypes import (
    Protocol,
    T_BOOL,
    T_BYTE,
    T_DOUBLE,
    T_FLOAT,
    T_I16,
    T_I32,
    T_I64,
    T_STOP,
    T_U64,
)

_CT_BOOLEAN_TRUE = 1
_CT_BOOLEAN_FALSE = 2

# Indexed by TType and by compact type, as in CompactProtocol-inl.h.
_TTYPE_TO_CTYPE = (0, 0, 1, 3, 7, 0, 4, 0, 5, 0, 6, 8, 12, 11, 10, 9, 0, 0, 0, 13)
_CTYPE_TO_TTYPE = (0, 2, 2, 3, 6, 8, 10, 4, 11, 15, 14, 13, 12, 19)

_BYTE = struct.Struct(">b")
_FLOAT = struct.Struct(">f")
_DOUBLE = struct.Struct(">d")

_MASK32 = 0xFFFFFFFF
_MASK64 = 0xFFFFFFFFFFFFFFFF


def _to_ttype(ctype: int) -> int:
    if ctype >= len(_CTYPE_TO_TTYPE):
        raise DecodeError(f"unknown compact type {ctype}")
    return _CTYPE_TO_TTYPE[ctype]


def _signed32(value: int) -> int:
    return value - (1 << 32) if value & 0x80000000 else value


def _wrap16(value: int) -> int:
    return ((value + 0x8000) & 0xFFFF) - 0x8000


def _check_size(size: int) -> int:
    if size < 0:
        raise DecodeError(f"negative size {size}")
    return size


def _zigzag(value: int) -> int:
    # Equal to C++'s 32-bit zigzag for every i32, so all widths share it.
    return (value << 1) ^ (value >> 63)


class CompactReader(BufferReader):
    __slots__ = ("_last_field_id", "_field_id_stack", "_bool_value")
    protocol = Protocol.COMPACT
    FIXED_SIZES_IN_CONTAINER = {T_BOOL: 1, T_BYTE: 1, T_FLOAT: 4, T_DOUBLE: 8}

    _last_field_id: int
    _field_id_stack: list[int]
    # The value of the bool field whose header was read last, until read.
    _bool_value: bool | None

    def __init__(self, data: bytes) -> None:
        super().__init__(data)
        self._last_field_id = 0
        self._field_id_stack = []
        self._bool_value = None

    def _read_varint(self, max_bytes: int) -> int:
        data = self._data
        pos = self._pos
        result = 0
        shift = 0
        for _ in range(max_bytes):
            if pos >= len(data):
                raise DecodeError(f"truncated varint at offset {self._pos}")
            byte = data[pos]
            pos += 1
            result |= (byte & 0x7F) << shift
            if byte < 0x80:
                self._pos = pos
                return result
            shift += 7
        raise DecodeError(f"varint longer than {max_bytes} bytes at offset {self._pos}")

    def _read_zigzag32(self) -> int:
        value = self._read_varint(5) & _MASK32
        return (value >> 1) ^ -(value & 1)

    def _read_size(self) -> int:
        return _check_size(_signed32(self._read_varint(5) & _MASK32))

    def read_struct_begin(self) -> None:
        self._descend()
        self._field_id_stack.append(self._last_field_id)
        self._last_field_id = 0

    def read_struct_end(self) -> None:
        self._last_field_id = self._field_id_stack.pop()
        self._ascend()

    def read_field_begin(self) -> tuple[int, int, str]:
        byte = self._data[self._take(1)]
        ctype = byte & 0x0F
        if ctype == T_STOP:
            return T_STOP, 0, ""
        delta = byte >> 4
        field_id = (
            self.read_i16() if delta == 0 else _wrap16(self._last_field_id + delta)
        )
        ttype = _to_ttype(ctype)
        if ctype == _CT_BOOLEAN_TRUE or ctype == _CT_BOOLEAN_FALSE:
            self._bool_value = ctype == _CT_BOOLEAN_TRUE
        self._last_field_id = field_id
        return ttype, field_id, ""

    def read_map_begin(self) -> tuple[int, int, int]:
        self._descend()
        size = _signed32(self._read_varint(5) & _MASK32)
        key_and_value = self._data[self._take(1)] if size != 0 else 0
        _check_size(size)
        return _to_ttype(key_and_value >> 4), _to_ttype(key_and_value & 0x0F), size

    def read_list_begin(self) -> tuple[int, int]:
        self._descend()
        byte = self._data[self._take(1)]
        size = byte >> 4
        if size == 15:
            size = self._read_size()
        return _to_ttype(byte & 0x0F), size

    def read_bool(self) -> bool:
        value = self._bool_value
        if value is not None:
            self._bool_value = None
            return value
        return self._data[self._take(1)] == _CT_BOOLEAN_TRUE

    def read_byte(self) -> int:
        return _BYTE.unpack_from(self._data, self._take(1))[0]

    def read_i16(self) -> int:
        return _wrap16(self._read_zigzag32())

    def read_i32(self) -> int:
        return self._read_zigzag32()

    def read_i64(self) -> int:
        value = self._read_varint(10) & _MASK64
        return (value >> 1) ^ -(value & 1)

    def read_float(self) -> float:
        return _FLOAT.unpack_from(self._data, self._take(4))[0]

    def read_double(self) -> float:
        return _DOUBLE.unpack_from(self._data, self._take(8))[0]

    def read_binary(self) -> bytes:
        return self._read_bytes(self._read_size())

    def _skip_scalar(self, ttype: int) -> bool:
        if ttype == T_BOOL:
            self.read_bool()
        elif ttype == T_BYTE:
            self._take(1)
        elif ttype == T_I16 or ttype == T_I32:
            self._read_varint(5)
        elif ttype == T_I64 or ttype == T_U64:
            self._read_varint(10)
        elif ttype == T_DOUBLE:
            self._take(8)
        elif ttype == T_FLOAT:
            self._take(4)
        elif ttype in STRING_TTYPES:
            self._take(self._read_size())
        else:
            return False
        return True


class CompactWriter:
    __slots__ = ("_out", "_last_field_id", "_field_id_stack", "_pending_bool_field")
    protocol: ClassVar[Protocol] = Protocol.COMPACT

    _out: bytearray
    _last_field_id: int
    _field_id_stack: list[int]
    # A bool field's header carries its value, so it waits for write_bool.
    _pending_bool_field: int | None

    def __init__(self) -> None:
        self._out = bytearray()
        self._last_field_id = 0
        self._field_id_stack = []
        self._pending_bool_field = None

    def getvalue(self) -> bytes:
        return bytes(self._out)

    def _write_varint(self, value: int) -> None:
        out = self._out
        while value > 0x7F:
            out.append((value & 0x7F) | 0x80)
            value >>= 7
        out.append(value)

    def _write_field_header(self, ctype: int, field_id: int) -> None:
        delta = field_id - self._last_field_id
        if 0 < delta <= 15:
            self._out.append((delta << 4) | ctype)
        else:
            self._out.append(ctype)
            self._write_varint(_zigzag(field_id))
        self._last_field_id = field_id

    def write_struct_begin(self, name: str) -> None:
        self._field_id_stack.append(self._last_field_id)
        self._last_field_id = 0

    def write_struct_end(self) -> None:
        self._last_field_id = self._field_id_stack.pop()

    def write_field_begin(self, name: str, ttype: int, field_id: int) -> None:
        if ttype == T_BOOL:
            self._pending_bool_field = field_id
        else:
            self._write_field_header(_TTYPE_TO_CTYPE[ttype], field_id)

    def write_field_end(self) -> None:
        pass

    def write_field_stop(self) -> None:
        self._out.append(T_STOP)

    def write_map_begin(self, key_ttype: int, value_ttype: int, size: int) -> None:
        if size == 0:
            self._out.append(0)
            return
        self._write_varint(size)
        self._out.append(
            (_TTYPE_TO_CTYPE[key_ttype] << 4) | _TTYPE_TO_CTYPE[value_ttype]
        )

    def write_map_end(self) -> None:
        pass

    def write_list_begin(self, element_ttype: int, size: int) -> None:
        ctype = _TTYPE_TO_CTYPE[element_ttype]
        if size <= 14:
            self._out.append((size << 4) | ctype)
        else:
            self._out.append(0xF0 | ctype)
            self._write_varint(size)

    def write_list_end(self) -> None:
        pass

    def write_set_begin(self, element_ttype: int, size: int) -> None:
        self.write_list_begin(element_ttype, size)

    def write_set_end(self) -> None:
        pass

    def write_bool(self, value: bool) -> None:
        ctype = _CT_BOOLEAN_TRUE if value else _CT_BOOLEAN_FALSE
        field_id = self._pending_bool_field
        if field_id is None:
            self._out.append(ctype)
        else:
            self._pending_bool_field = None
            self._write_field_header(ctype, field_id)

    def write_byte(self, value: int) -> None:
        self._out += _BYTE.pack(value)

    def write_i16(self, value: int) -> None:
        self._write_varint(_zigzag(value))

    def write_i32(self, value: int) -> None:
        self._write_varint(_zigzag(value))

    def write_i64(self, value: int) -> None:
        self._write_varint(_zigzag(value))

    def write_float(self, value: float) -> None:
        self._out += _FLOAT.pack(value)

    def write_double(self, value: float) -> None:
        self._out += _DOUBLE.pack(value)

    def write_string(self, value: str) -> None:
        self.write_binary(value.encode("utf-8"))

    def write_binary(self, value: bytes) -> None:
        self._write_varint(check_binary_size(len(value)))
        self._out += value

    def write_raw(self, data: bytes) -> None:
        self._out += data
