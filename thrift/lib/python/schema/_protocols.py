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
The boundary between the schema-driven walk (``serialization``) and the wire
format: the protocol reader and writer interface, and the one place that maps
a ``Protocol`` to reader and writer classes.
"""

from __future__ import annotations

import functools
from collections.abc import Mapping
from dataclasses import dataclass
from typing import ClassVar, Protocol as TypingProtocol

from thrift.lib.python.schema._binary_protocol import BinaryReader, BinaryWriter
from thrift.lib.python.schema._compact_protocol import CompactReader, CompactWriter
from thrift.lib.python.schema._simple_json_protocol import (
    SimpleJsonReader,
    SimpleJsonWriter,
)
from thrift.lib.python.schema._ttypes import Protocol


class ProtocolReader(TypingProtocol):
    """Reads one value from a complete buffer. Containers and structs count
    toward a nesting limit of ``_ttypes.PROTOCOL_MAX_DEPTH``. A reader whose
    protocol omits wire types returns ``T_VOID`` for them."""

    protocol: ClassVar[Protocol]
    uses_field_names: ClassVar[bool]
    omits_container_sizes: ClassVar[bool]

    def __init__(self, data: bytes) -> None: ...
    def remaining(self) -> int:
        """The number of unread bytes."""
        ...

    def read_struct_begin(self) -> None: ...
    def read_struct_end(self) -> None: ...
    def read_field_begin(self) -> tuple[int, int, str]:
        """``(ttype, field_id, name)``; ``ttype`` is ``T_STOP`` after the last
        field. ``name`` is empty unless the reader ``uses_field_names``, and
        ``field_id`` is ``_ttypes.NO_FIELD_ID`` if the input has only the name."""
        ...

    def read_field_end(self) -> None: ...
    def read_map_begin(self) -> tuple[int, int, int]:
        """``(key_ttype, value_ttype, size)``."""
        ...

    def read_map_end(self) -> None: ...
    def read_list_begin(self) -> tuple[int, int]:
        """``(element_ttype, size)``."""
        ...

    def read_list_end(self) -> None: ...
    def read_set_begin(self) -> tuple[int, int]: ...
    def read_set_end(self) -> None: ...
    def read_bool(self) -> bool: ...
    def read_byte(self) -> int: ...
    def read_i16(self) -> int: ...
    def read_i32(self) -> int: ...
    def read_i64(self) -> int: ...
    def read_float(self) -> float: ...
    def read_double(self) -> float: ...
    def read_string(self) -> str:
        """A string, which must be valid UTF-8."""
        ...

    def read_binary(self) -> bytes: ...
    def skip(self, ttype: int, depth: int = 0) -> None: ...

    def read_raw(self, ttype: int) -> bytes:
        """Skip a value and return its encoding (for ``any``)."""
        ...


class ProtocolWriter(TypingProtocol):
    """Writes one value. Values arrive already validated by the walk."""

    protocol: ClassVar[Protocol]

    def __init__(self) -> None: ...
    def getvalue(self) -> bytes: ...
    def write_struct_begin(self, name: str) -> None: ...
    def write_struct_end(self) -> None: ...
    def write_field_begin(self, name: str, ttype: int, field_id: int) -> None: ...
    def write_field_end(self) -> None: ...
    def write_field_stop(self) -> None: ...
    def write_map_begin(self, key_ttype: int, value_ttype: int, size: int) -> None: ...
    def write_map_end(self) -> None: ...
    def write_list_begin(self, element_ttype: int, size: int) -> None: ...
    def write_list_end(self) -> None: ...
    def write_set_begin(self, element_ttype: int, size: int) -> None: ...
    def write_set_end(self) -> None: ...
    def write_bool(self, value: bool) -> None: ...
    def write_byte(self, value: int) -> None: ...
    def write_i16(self, value: int) -> None: ...
    def write_i32(self, value: int) -> None: ...
    def write_i64(self, value: int) -> None: ...
    def write_float(self, value: float) -> None: ...
    def write_double(self, value: float) -> None: ...
    def write_string(self, value: str) -> None: ...
    def write_binary(self, value: bytes) -> None: ...
    def write_raw(self, data: bytes) -> None:
        """Write a value already encoded in this protocol (for ``any``)."""
        ...


@dataclass(frozen=True)
class Backend:
    """A set of reader and writer classes."""

    name: str
    readers: Mapping[Protocol, type[ProtocolReader]]
    writers: Mapping[Protocol, type[ProtocolWriter]]


@functools.cache
def available_backends() -> tuple[Backend, ...]:
    """Every backend that can run here, preferred first."""
    return (
        Backend(
            name="python",
            readers={
                Protocol.BINARY: BinaryReader,
                Protocol.COMPACT: CompactReader,
                Protocol.SIMPLE_JSON: SimpleJsonReader,
            },
            writers={
                Protocol.BINARY: BinaryWriter,
                Protocol.COMPACT: CompactWriter,
                Protocol.SIMPLE_JSON: SimpleJsonWriter,
            },
        ),
    )


def default_backend() -> Backend:
    return available_backends()[0]
