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
SimpleJSON: fully-compatible with C++ ``SimpleJSONProtocolWriter`` and
``SimpleJSONProtocolReader``.

A struct is ``{"name":value,...}``, a list or set ``[...]``, and a map
``{key:value,...}``, with no whitespace. Map keys of integer, enum, bool, and
floating-point types are quoted; struct and container keys, which C++ writes
as invalid JSON, raise ``EncodeError``.
"""

from __future__ import annotations

import functools
import itertools
import json
from typing import cast, ClassVar, NoReturn

from thrift.lib.python.schema._errors import DecodeError, EncodeError
from thrift.lib.python.schema._protocol_common import decode_utf8
from thrift.lib.python.schema._simple_json_scalars import (
    binary_from_json,
    bool_from_json_key,
    floating_from_json,
    floating_from_json_key,
    int_from_json,
    json_binary,
    json_double,
    json_float32,
    json_string,
    special_floating_from_json,
    utf8_text,
)
from thrift.lib.python.schema._ttypes import (
    NO_FIELD_ID,
    Protocol,
    PROTOCOL_MAX_DEPTH,
    T_STOP,
    T_VOID,
)


class _Context:
    """An open object or array. ``meta`` counts the items written in it;
    in an object, even items are keys."""

    __slots__ = ("is_object", "meta")
    is_object: bool
    meta: int

    def __init__(self, is_object: bool) -> None:
        self.is_object = is_object
        self.meta = 0


class SimpleJsonWriter:
    __slots__ = ("_out", "_contexts")
    protocol: ClassVar[Protocol] = Protocol.SIMPLE_JSON

    _out: list[str]
    _contexts: list[_Context]

    def __init__(self) -> None:
        self._out = []
        self._contexts = []

    def getvalue(self) -> bytes:
        return "".join(self._out).encode("utf-8")

    def _write_context(self) -> bool:
        """Write the separator before an item; ``True`` if it is a map key."""
        if not self._contexts:
            return False
        context = self._contexts[-1]
        meta = context.meta
        context.meta += 1
        if context.is_object:
            if meta % 2 == 0:
                if meta:
                    self._out.append(",")
                return True
            self._out.append(":")
        elif meta:
            self._out.append(",")
        return False

    def _begin(self, is_object: bool) -> None:
        if self._write_context():
            raise EncodeError("SimpleJSON cannot write a struct or container map key")
        self._contexts.append(_Context(is_object))
        self._out.append("{" if is_object else "[")

    def _end(self) -> None:
        self._out.append("}" if self._contexts.pop().is_object else "]")

    def _write_number(self, text: str) -> None:
        # Numbers are quoted in key position; "NaN" and the infinities
        # already are.
        if self._write_context() and not text.startswith('"'):
            text = f'"{text}"'
        self._out.append(text)

    def write_struct_begin(self, name: str) -> None:
        self._begin(True)

    def write_struct_end(self) -> None:
        self._end()

    def write_field_begin(self, name: str, ttype: int, field_id: int) -> None:
        self._write_context()
        self._out.append(json_string(name))

    def write_field_end(self) -> None:
        pass

    def write_field_stop(self) -> None:
        pass

    def write_map_begin(self, key_ttype: int, value_ttype: int, size: int) -> None:
        self._begin(True)

    def write_map_end(self) -> None:
        self._end()

    def write_list_begin(self, element_ttype: int, size: int) -> None:
        self._begin(False)

    def write_list_end(self) -> None:
        self._end()

    def write_set_begin(self, element_ttype: int, size: int) -> None:
        self._begin(False)

    def write_set_end(self) -> None:
        self._end()

    def write_bool(self, value: bool) -> None:
        self._write_number("true" if value else "false")

    def _write_int(self, value: int) -> None:
        # A generated enum member is an int whose str() is its name.
        self._write_number(str(int(value)))

    def write_byte(self, value: int) -> None:
        self._write_int(value)

    def write_i16(self, value: int) -> None:
        self._write_int(value)

    def write_i32(self, value: int) -> None:
        self._write_int(value)

    def write_i64(self, value: int) -> None:
        self._write_int(value)

    def write_float(self, value: float) -> None:
        self._write_number(json_float32(value))

    def write_double(self, value: float) -> None:
        self._write_number(json_double(value))

    def write_string(self, value: str) -> None:
        self._write_context()
        self._out.append(json_string(value))

    def write_binary(self, value: bytes) -> None:
        self._write_context()
        self._out.append(json_binary(value))

    def write_raw(self, data: bytes) -> None:
        if self._write_context():
            raise EncodeError("SimpleJSON cannot write a struct map key")
        self._out.append(decode_utf8(data))


# ---------------------------------------------------------------------------
# Reading: the ``json`` module (C-accelerated) builds a tree, which
# ``SimpleJsonReader`` reads.
# ---------------------------------------------------------------------------


class JsonObject(list[tuple[str, object]]):
    """A JSON object, as its ``(name, value)`` pairs in input order, so that
    repeated names stay visible."""

    __slots__ = ()


class JsonNumber:
    """A JSON number, kept as its literal so that ``-0`` and float32 rounding
    are exact."""

    __slots__ = ("literal",)
    literal: str

    def __init__(self, literal: str) -> None:
        self.literal = literal

    def __repr__(self) -> str:
        return f"JsonNumber({self.literal})"


def _reject_constant(name: str) -> NoReturn:
    raise ValueError(f"bare {name} is not valid; it must be quoted")


@functools.cache
def _decoder() -> json.JSONDecoder:
    # C++ accepts raw control characters inside strings, hence strict=False.
    return json.JSONDecoder(
        object_pairs_hook=JsonObject,
        parse_float=JsonNumber,
        parse_int=JsonNumber,
        parse_constant=_reject_constant,
        strict=False,
    )


def parse_json(data: bytes) -> object:
    """The tree of a JSON document: ``JsonObject``, ``list``, ``str``,
    ``JsonNumber``, ``bool``, and ``None``. Raises ``DecodeError`` if ``data``
    is not one JSON value (or ``RecursionError`` if it nests too deeply)."""
    try:
        return _decoder().decode(decode_utf8(data))
    except ValueError as e:  # includes json.JSONDecodeError
        raise DecodeError(f"invalid JSON: {e}") from e


def parse_json_prefix(text: str, start: int = 0) -> tuple[object, int]:
    """The tree of the JSON value at ``text[start]``, as ``parse_json``
    builds it, and the index just past the value; what follows is not read.
    Raises ``DecodeError`` if no JSON value starts at ``start`` (or
    ``RecursionError`` if it nests too deeply)."""
    try:
        return _decoder().raw_decode(text, start)
    except ValueError as e:  # includes json.JSONDecodeError
        raise DecodeError(f"invalid JSON: {e}") from e


def json_kind(node: object) -> str:
    """The JSON kind of a tree node, for error messages."""
    match node:
        case JsonObject():
            return "object"
        case list():
            return "array"
        case str():
            return "string"
        case JsonNumber():
            return "number"
        case bool():
            return "bool"
        case None:
            return "null"
        case _:
            raise AssertionError(f"unexpected JSON node {node!r}")


def dump_json(node: object) -> str:
    """A tree as JSON text again, with numbers as they were written."""
    match node:
        case JsonObject():
            items = ",".join(f"{json.dumps(k)}:{dump_json(v)}" for k, v in node)
            return f"{{{items}}}"
        case list():
            return f"[{','.join(dump_json(e) for e in node)}]"
        case JsonNumber():
            return node.literal
        case _:
            return json.dumps(node)


_READ = object()


def _mismatch(node: object, type_name: str) -> DecodeError:
    return DecodeError(f"JSON {json_kind(node)} cannot be read as type '{type_name}'")


class _ReadContext:
    """An open object or array: its items, keys and values alternating in an
    object, and how many have been read."""

    __slots__ = ("items", "is_object", "meta")
    items: list[object]
    is_object: bool
    meta: int

    def __init__(self, items: list[object], is_object: bool) -> None:
        self.items = items
        self.is_object = is_object
        self.meta = 0


class SimpleJsonReader:
    """Reads SimpleJSON.

    A field is identified by its name alone, and one whose value is ``null``
    is skipped. Map keys are quoted; scalar keys are decoded from their text. The
    input has no container sizes or wire types: the begin methods return the
    size of the parsed object or array, and ``T_VOID`` for every wire type.
    ``skip`` ignores the wire type."""

    __slots__ = ("_size", "_root", "_contexts", "_height")
    protocol: ClassVar[Protocol] = Protocol.SIMPLE_JSON
    uses_field_names: ClassVar[bool] = True
    omits_container_sizes: ClassVar[bool] = True

    _size: int
    _root: object
    _contexts: list[_ReadContext]
    _height: int

    def __init__(self, data: bytes) -> None:
        try:
            self._root = parse_json(data)
        except RecursionError:
            raise DecodeError("input is nested too deeply to parse") from None
        self._size = len(data)
        self._contexts = []
        self._height = PROTOCOL_MAX_DEPTH + 1

    def remaining(self) -> int:
        """``len(data)`` until the value has been read, then 0: the parser has
        already rejected anything but whitespace after it."""
        return 0 if self._root is _READ and not self._contexts else self._size

    def _next(self) -> tuple[object, str | None]:
        """The next item, and its text if it is an object key."""
        if not self._contexts:
            root = self._root
            if root is _READ:
                raise DecodeError("no value left to read")
            self._root = _READ
            return root, None
        context = self._contexts[-1]
        meta = context.meta
        if meta == len(context.items):
            kind = "object" if context.is_object else "array"
            raise DecodeError(f"no items left to read in a JSON {kind}")
        context.meta = meta + 1
        item = context.items[meta]
        if context.is_object and meta % 2 == 0:
            return item, cast(str, item)
        return item, None

    def _descend(self) -> None:
        self._height -= 1
        if self._height == 0:
            self._height += 1
            raise DecodeError(f"nesting depth exceeds {PROTOCOL_MAX_DEPTH}")

    def _next_container(self, type_name: str) -> object:
        self._descend()
        node, key = self._next()
        if key is not None:
            raise DecodeError(f"SimpleJSON cannot read a map key of type '{type_name}'")
        return node

    def _open_object(self, type_name: str) -> int:
        node = self._next_container(type_name)
        if not isinstance(node, JsonObject):
            raise _mismatch(node, type_name)
        self._contexts.append(
            _ReadContext(list(itertools.chain.from_iterable(node)), True)
        )
        return len(node)

    def _open_array(self, type_name: str) -> int:
        node = self._next_container(type_name)
        if not isinstance(node, list) or isinstance(node, JsonObject):
            raise _mismatch(node, type_name)
        self._contexts.append(_ReadContext(node, False))
        return len(node)

    def _close(self) -> None:
        context = self._contexts.pop()
        if context.meta != len(context.items):
            kind = "object" if context.is_object else "array"
            raise DecodeError(f"unread items left in a JSON {kind}")
        self._height += 1

    def read_struct_begin(self) -> None:
        self._open_object("struct")

    def read_struct_end(self) -> None:
        self._close()

    def read_field_begin(self) -> tuple[int, int, str]:
        context = self._contexts[-1]
        items = context.items
        while context.meta < len(items) and items[context.meta + 1] is None:
            context.meta += 2
        if context.meta == len(items):
            return T_STOP, 0, ""
        name, _ = self._next()
        return T_VOID, NO_FIELD_ID, cast(str, name)

    def read_field_end(self) -> None:
        pass

    def read_map_begin(self) -> tuple[int, int, int]:
        return T_VOID, T_VOID, self._open_object("map")

    def read_map_end(self) -> None:
        self._close()

    def read_list_begin(self) -> tuple[int, int]:
        return T_VOID, self._open_array("list")

    def read_list_end(self) -> None:
        self._close()

    def read_set_begin(self) -> tuple[int, int]:
        return T_VOID, self._open_array("set")

    def read_set_end(self) -> None:
        self._close()

    def read_bool(self) -> bool:
        node, key = self._next()
        if key is not None:
            return bool_from_json_key(key)
        if isinstance(node, bool):
            return node
        raise _mismatch(node, "bool")

    def _read_int(self, bits: int, type_name: str) -> int:
        node, key = self._next()
        if key is not None:
            return int_from_json(key, bits, type_name)
        if isinstance(node, JsonNumber):
            return int_from_json(node.literal, bits, type_name)
        raise _mismatch(node, type_name)

    def read_byte(self) -> int:
        return self._read_int(8, "byte")

    def read_i16(self) -> int:
        return self._read_int(16, "i16")

    def read_i32(self) -> int:
        return self._read_int(32, "i32")

    def read_i64(self) -> int:
        return self._read_int(64, "i64")

    def _read_floating(self, float32: bool, type_name: str) -> float:
        node, key = self._next()
        if key is not None:
            return floating_from_json_key(key, float32=float32)
        if isinstance(node, JsonNumber):
            return floating_from_json(node.literal, float32=float32)
        if isinstance(node, str):
            return special_floating_from_json(node)
        raise _mismatch(node, type_name)

    def read_float(self) -> float:
        return self._read_floating(True, "float")

    def read_double(self) -> float:
        return self._read_floating(False, "double")

    def _read_text(self, type_name: str) -> str:
        node, _ = self._next()
        if isinstance(node, str):
            return node
        raise _mismatch(node, type_name)

    def read_string(self) -> str:
        return utf8_text(self._read_text("string"))

    def read_binary(self) -> bytes:
        return binary_from_json(self._read_text("binary"))

    def skip(self, ttype: int, depth: int = 0) -> None:
        self._skip(depth)

    def read_raw(self, ttype: int) -> bytes:
        return dump_json(self._skip(0)).encode("utf-8")

    def _skip(self, depth: int) -> object:
        """Consume the next item: each nested value is one level deeper,
        and each object or array counts toward the nesting limit."""
        node, _ = self._next()
        pending = [(node, 0)]
        while pending:
            item, level = pending.pop()
            if depth + level >= PROTOCOL_MAX_DEPTH or (
                isinstance(item, list) and level + 1 >= self._height
            ):
                raise DecodeError(f"nesting depth exceeds {PROTOCOL_MAX_DEPTH}")
            if isinstance(item, JsonObject):
                pending.extend((value, level + 1) for _, value in item)
            elif isinstance(item, list):
                pending.extend((element, level + 1) for element in item)
        return node
