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
The Binary protocol in pure Python, following
``thrift/lib/cpp2/protocol/BinaryProtocol-inl.h``, plus ``BufferReader``, the
buffer handling and ``skip`` shared with the Compact reader.

Binary encodes every integer in fixed-width big-endian form. A field header is
``[ttype:i8][id:i16]``, a list or set header ``[ttype:i8][size:i32]``, a map
header ``[key ttype:i8][value ttype:i8][size:i32]``, and a string or binary
``[size:i32][bytes]``.
"""

from __future__ import annotations

import struct
from collections.abc import Iterator, Mapping
from typing import ClassVar

from thrift.lib.python.schema._errors import DecodeError, EncodeError
from thrift.lib.python.schema._protocol_common import decode_utf8, INT32_MAX
from thrift.lib.python.schema._ttypes import (
    Protocol,
    PROTOCOL_MAX_DEPTH,
    T_BOOL,
    T_BYTE,
    T_DOUBLE,
    T_FLOAT,
    T_I16,
    T_I32,
    T_I64,
    T_LIST,
    T_MAP,
    T_SET,
    T_STOP,
    T_STRING,
    T_STRUCT,
    T_U64,
    T_UTF16,
    T_UTF8,
)

# A pending ``skip``: a value of a wire type, at a ``skip`` depth.
_SkipRequest = tuple[int, int]

STRING_TTYPES: frozenset[int] = frozenset({T_STRING, T_UTF8, T_UTF16})


class BufferReader:
    """Reads from a complete ``bytes`` buffer. Tracks the nesting height of
    structs and containers, and skips values without recursion."""

    __slots__ = ("_data", "_pos", "_height")
    protocol: ClassVar[Protocol]
    uses_field_names: ClassVar[bool] = False
    omits_container_sizes: ClassVar[bool] = False
    FIXED_SIZES_IN_CONTAINER: ClassVar[Mapping[int, int]] = {}

    _data: bytes
    _pos: int
    _height: int

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._pos = 0
        self._height = PROTOCOL_MAX_DEPTH + 1

    def remaining(self) -> int:
        return len(self._data) - self._pos

    def _take(self, size: int) -> int:
        """Consume ``size`` bytes and return the offset of the first."""
        pos = self._pos
        end = pos + size
        if size < 0 or end > len(self._data):
            raise DecodeError(
                f"truncated input: {size} bytes needed at offset {pos}, "
                f"{len(self._data) - pos} available"
            )
        self._pos = end
        return pos

    def _read_bytes(self, size: int) -> bytes:
        pos = self._take(size)
        return self._data[pos : pos + size]

    def _descend(self) -> None:
        self._height -= 1
        if self._height == 0:
            self._height += 1
            raise DecodeError(f"nesting depth exceeds {PROTOCOL_MAX_DEPTH}")

    def _ascend(self) -> None:
        self._height += 1

    def read_struct_begin(self) -> None:
        self._descend()

    def read_struct_end(self) -> None:
        self._ascend()

    def read_field_begin(self) -> tuple[int, int, str]:
        raise NotImplementedError

    def read_field_end(self) -> None:
        pass

    def read_map_begin(self) -> tuple[int, int, int]:
        raise NotImplementedError

    def read_map_end(self) -> None:
        self._ascend()

    def read_list_begin(self) -> tuple[int, int]:
        raise NotImplementedError

    def read_list_end(self) -> None:
        self._ascend()

    def read_set_begin(self) -> tuple[int, int]:
        return self.read_list_begin()

    def read_set_end(self) -> None:
        self._ascend()

    def read_binary(self) -> bytes:
        raise NotImplementedError

    def read_string(self) -> str:
        return decode_utf8(self.read_binary())

    # -- skip -------------------------------------------------------------

    def _skip_scalar(self, ttype: int) -> bool:
        """Skip a scalar of wire type ``ttype``; ``False`` if it is not one."""
        raise NotImplementedError

    def skip(self, ttype: int, depth: int = 0) -> None:
        # Each compound value is a generator that yields its children as
        # pending requests, so nesting costs heap, not Python stack.
        stack: list[Iterator[_SkipRequest]] = []
        request: _SkipRequest | None = (ttype, depth)
        while True:
            if request is not None:
                children = self._skip_value(*request)
                if children is not None:
                    stack.append(children)
            if not stack:
                return
            request = next(stack[-1], None)
            if request is None:
                stack.pop()

    def read_raw(self, ttype: int) -> bytes:
        start = self._pos
        self.skip(ttype)
        return self._data[start : self._pos]

    def _skip_value(self, ttype: int, depth: int) -> Iterator[_SkipRequest] | None:
        if depth >= PROTOCOL_MAX_DEPTH:
            raise DecodeError(f"nesting depth exceeds {PROTOCOL_MAX_DEPTH}")
        if self._skip_scalar(ttype):
            return None
        if ttype == T_STRUCT:
            return self._skip_struct(depth)
        if ttype == T_LIST or ttype == T_SET:
            return self._skip_list(depth)
        if ttype == T_MAP:
            return self._skip_map(depth)
        raise DecodeError(f"cannot skip a value of wire type {ttype}")

    def _skip_struct(self, depth: int) -> Iterator[_SkipRequest]:
        self.read_struct_begin()
        while True:
            ttype, _, _ = self.read_field_begin()
            if ttype == T_STOP:
                break
            yield ttype, depth + 1
            self.read_field_end()
        self.read_struct_end()

    def _skip_list(self, depth: int) -> Iterator[_SkipRequest]:
        element_ttype, size = self.read_list_begin()
        yield from self._skip_n(size, (element_ttype,), depth + 1)
        self.read_list_end()

    def _skip_map(self, depth: int) -> Iterator[_SkipRequest]:
        key_ttype, value_ttype, size = self.read_map_begin()
        yield from self._skip_n(size, (key_ttype, value_ttype), depth + 1)
        self.read_map_end()

    def _skip_n(
        self, size: int, ttypes: tuple[int, ...], depth: int
    ) -> Iterator[_SkipRequest]:
        if depth >= PROTOCOL_MAX_DEPTH:
            raise DecodeError(f"nesting depth exceeds {PROTOCOL_MAX_DEPTH}")
        fixed_sizes = [self.FIXED_SIZES_IN_CONTAINER.get(t, 0) for t in ttypes]
        if all(fixed_sizes):
            self._take(size * sum(fixed_sizes))
            return
        # Every element consumes at least one byte, so truncated input ends
        # the loop long before a hostile ``size`` would.
        for _ in range(size):
            for ttype in ttypes:
                yield ttype, depth + 1


_BYTE = struct.Struct(">b")
_I16 = struct.Struct(">h")
_I32 = struct.Struct(">i")
_I64 = struct.Struct(">q")
_FLOAT = struct.Struct(">f")
_DOUBLE = struct.Struct(">d")
_FIELD_HEADER = struct.Struct(">Bh")
_LIST_HEADER = struct.Struct(">Bi")
_MAP_HEADER = struct.Struct(">BBi")

_FIXED_SIZES: dict[int, int] = {
    T_BOOL: 1,
    T_BYTE: 1,
    T_I16: 2,
    T_I32: 4,
    T_FLOAT: 4,
    T_I64: 8,
    T_DOUBLE: 8,
}
# ``BinaryProtocolReader::skip`` also skips T_U64, which no container skips.
_SKIP_SIZES: dict[int, int] = {**_FIXED_SIZES, T_U64: 8}


def _check_size(size: int) -> int:
    if size < 0:
        raise DecodeError(f"negative size {size}")
    return size


class BinaryReader(BufferReader):
    __slots__ = ()
    protocol = Protocol.BINARY
    FIXED_SIZES_IN_CONTAINER = _FIXED_SIZES

    def read_field_begin(self) -> tuple[int, int, str]:
        ttype = self._data[self._take(1)]
        if ttype == T_STOP:
            return T_STOP, 0, ""
        return ttype, _I16.unpack_from(self._data, self._take(2))[0], ""

    def read_map_begin(self) -> tuple[int, int, int]:
        self._descend()
        key_ttype, value_ttype, size = _MAP_HEADER.unpack_from(
            self._data, self._take(6)
        )
        return key_ttype, value_ttype, _check_size(size)

    def read_list_begin(self) -> tuple[int, int]:
        self._descend()
        element_ttype, size = _LIST_HEADER.unpack_from(self._data, self._take(5))
        return element_ttype, _check_size(size)

    def read_bool(self) -> bool:
        byte = self._data[self._take(1)]
        if byte >= 2:
            raise DecodeError(f"bool value out of range: {byte}")
        return byte == 1

    def read_byte(self) -> int:
        return _BYTE.unpack_from(self._data, self._take(1))[0]

    def read_i16(self) -> int:
        return _I16.unpack_from(self._data, self._take(2))[0]

    def read_i32(self) -> int:
        return _I32.unpack_from(self._data, self._take(4))[0]

    def read_i64(self) -> int:
        return _I64.unpack_from(self._data, self._take(8))[0]

    def read_float(self) -> float:
        return _FLOAT.unpack_from(self._data, self._take(4))[0]

    def read_double(self) -> float:
        return _DOUBLE.unpack_from(self._data, self._take(8))[0]

    def read_binary(self) -> bytes:
        return self._read_bytes(_check_size(self.read_i32()))

    def _skip_scalar(self, ttype: int) -> bool:
        size = _SKIP_SIZES.get(ttype)
        if size is not None:
            self._take(size)
            return True
        if ttype in STRING_TTYPES:
            self._take(self.read_i32())
            return True
        return False


def check_binary_size(size: int) -> int:
    if size > INT32_MAX:
        raise EncodeError(f"string or binary of {size} bytes exceeds {INT32_MAX}")
    return size


class BinaryWriter:
    __slots__ = ("_out",)
    protocol: ClassVar[Protocol] = Protocol.BINARY
    _out: bytearray

    def __init__(self) -> None:
        self._out = bytearray()

    def getvalue(self) -> bytes:
        return bytes(self._out)

    def write_struct_begin(self, name: str) -> None:
        pass

    def write_struct_end(self) -> None:
        pass

    def write_field_begin(self, name: str, ttype: int, field_id: int) -> None:
        self._out += _FIELD_HEADER.pack(ttype, field_id)

    def write_field_end(self) -> None:
        pass

    def write_field_stop(self) -> None:
        self._out.append(T_STOP)

    def write_map_begin(self, key_ttype: int, value_ttype: int, size: int) -> None:
        self._out += _MAP_HEADER.pack(key_ttype, value_ttype, size)

    def write_map_end(self) -> None:
        pass

    def write_list_begin(self, element_ttype: int, size: int) -> None:
        self._out += _LIST_HEADER.pack(element_ttype, size)

    def write_list_end(self) -> None:
        pass

    def write_set_begin(self, element_ttype: int, size: int) -> None:
        self._out += _LIST_HEADER.pack(element_ttype, size)

    def write_set_end(self) -> None:
        pass

    def write_bool(self, value: bool) -> None:
        self._out.append(1 if value else 0)

    def write_byte(self, value: int) -> None:
        self._out += _BYTE.pack(value)

    def write_i16(self, value: int) -> None:
        self._out += _I16.pack(value)

    def write_i32(self, value: int) -> None:
        self._out += _I32.pack(value)

    def write_i64(self, value: int) -> None:
        self._out += _I64.pack(value)

    def write_float(self, value: float) -> None:
        self._out += _FLOAT.pack(value)

    def write_double(self, value: float) -> None:
        self._out += _DOUBLE.pack(value)

    def write_string(self, value: str) -> None:
        self.write_binary(value.encode("utf-8"))

    def write_binary(self, value: bytes) -> None:
        self._out += _I32.pack(check_binary_size(len(value)))
        self._out += value

    def write_raw(self, data: bytes) -> None:
        self._out += data
