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
Serialization of ``DynamicValue`` in the Binary, Compact, and SimpleJSON
protocols, driven by the value's ``TypeRef``::

    data = serialize(value, Protocol.BINARY)
    assert deserialize(value.type_ref, data, Protocol.BINARY) == value

Struct fields follow C++ ``DynamicValue`` serialization. Every non-optional
field is written, terse fields included, in ``TypeSystem`` field order, and an
optional field only when it is set. On read, a missing field takes its custom
or intrinsic default (``UNSET`` if it is optional) and an unknown field is
skipped. A known field, or an element of a non-empty container, whose wire type
differs from its declared type raises ``DecodeError``, as does a union with
more than one field; a union whose only field is unknown reads as empty.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import cast

from apache.thrift.type.any_rep.thrift_types import AnyStruct
from thrift.lib.python.schema._datum import (
    field_default,
    freeze_key,
    resolved,
    true_type,
    type_name,
    UNSET,
)
from thrift.lib.python.schema._errors import DecodeError, EncodeError
from thrift.lib.python.schema._protocols import (
    default_backend,
    ProtocolReader,
    ProtocolWriter,
)
from thrift.lib.python.schema._ttypes import (
    Protocol,
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
    T_VOID,
)
from thrift.lib.python.schema.dynamic_value import (
    _handle,
    DynamicValue,
    MutableDynamicValue,
)
from thrift.lib.python.schema.type_system import (
    EnumTypeRef,
    FieldDefinition,
    InvalidTypeError,
    ListTypeRef,
    MapTypeRef,
    Primitive,
    PrimitiveTypeRef,
    SetTypeRef,
    StructNode,
    StructTypeRef,
    TypeRef,
    UnionNode,
    UnionTypeRef,
)
from thrift.python import serializer as thrift_python

__all__ = ["DecodeError", "EncodeError", "Protocol", "deserialize", "serialize"]

# ``any`` is an ``AnyStruct``, which thrift-python encodes.
_THRIFT_PYTHON_PROTOCOLS: dict[Protocol, thrift_python.Protocol] = {
    Protocol.BINARY: thrift_python.Protocol.BINARY,
    Protocol.COMPACT: thrift_python.Protocol.COMPACT,
    Protocol.SIMPLE_JSON: thrift_python.Protocol.JSON,
}


def serialize(value: DynamicValue, protocol: Protocol = Protocol.COMPACT) -> bytes:
    """Encode ``value`` as its ``TypeRef``.

    Raises ``EncodeError`` if ``value`` cannot be encoded, such as when it is
    nested too deeply or, in SimpleJSON, has a struct or container map key."""
    writer = default_backend().writers[protocol]()
    try:
        # The stored datum itself, owned or frozen; both encode alike.
        _write(writer, value.type_ref, value._datum)
    except RecursionError:
        raise EncodeError("value is nested too deeply to encode") from None
    return writer.getvalue()


def deserialize(
    type_ref: TypeRef,
    data: bytes | bytearray | memoryview,
    protocol: Protocol = Protocol.COMPACT,
) -> MutableDynamicValue:
    """Decode a value of ``type_ref`` that makes up all of ``data``.

    Raises ``DecodeError`` if ``data`` is malformed, nested too deeply, or does
    not match ``type_ref``."""
    reader = default_backend().readers[protocol](bytes(data))
    try:
        datum = _read(reader, type_ref)
    except RecursionError:
        raise DecodeError("input is nested too deeply to decode") from None
    if reader.remaining():
        raise DecodeError(f"{reader.remaining()} bytes left after the value")
    # The walk built owned storage that fits ``type_ref``; no validating copy.
    return _handle(MutableDynamicValue, type_ref, datum, False)


_PRIMITIVE_TTYPES: dict[Primitive, int] = {
    Primitive.BOOL: T_BOOL,
    Primitive.BYTE: T_BYTE,
    Primitive.I16: T_I16,
    Primitive.I32: T_I32,
    Primitive.I64: T_I64,
    Primitive.FLOAT: T_FLOAT,
    Primitive.DOUBLE: T_DOUBLE,
    Primitive.STRING: T_STRING,
    Primitive.BINARY: T_STRING,
    Primitive.ANY: T_STRUCT,
}


def _ttype(type_ref: TypeRef) -> int:
    """The wire type of ``type_ref``."""
    match true_type(type_ref):
        case PrimitiveTypeRef(primitive=primitive):
            return _PRIMITIVE_TTYPES[primitive]
        case EnumTypeRef():
            return T_I32
        case ListTypeRef():
            return T_LIST
        case SetTypeRef():
            return T_SET
        case MapTypeRef():
            return T_MAP
        case _:
            return T_STRUCT


# ---------------------------------------------------------------------------
# Writing. Values are valid for their types, as every ``DynamicValue`` is.
# ---------------------------------------------------------------------------


def _write(writer: ProtocolWriter, type_ref: TypeRef, datum: object) -> None:
    target = true_type(type_ref)
    match target:
        case PrimitiveTypeRef(primitive=primitive):
            _write_primitive(writer, primitive, datum)
        case EnumTypeRef():
            writer.write_i32(cast(int, datum))
        case ListTypeRef():
            element_type = resolved(target.element_type)
            elements = cast(Sequence[object], datum)
            writer.write_list_begin(_ttype(element_type), len(elements))
            for element in elements:
                _write(writer, element_type, element)
            writer.write_list_end()
        case SetTypeRef():
            element_type = resolved(target.element_type)
            elements = cast(Mapping[object, None], datum)
            writer.write_set_begin(_ttype(element_type), len(elements))
            for element in elements:
                _write(writer, element_type, element)
            writer.write_set_end()
        case MapTypeRef():
            key_type = resolved(target.key_type)
            value_type = resolved(target.value_type)
            entries = cast(Mapping[object, object], datum)
            writer.write_map_begin(_ttype(key_type), _ttype(value_type), len(entries))
            for key, item in entries.items():
                _write(writer, key_type, key)
                _write(writer, value_type, item)
            writer.write_map_end()
        case StructTypeRef():
            writer.write_struct_begin(target.node.uri)
            for field, item in zip(target.node.fields, cast(Sequence[object], datum)):
                if item is not UNSET:
                    _write_field(writer, field, item)
            writer.write_field_stop()
            writer.write_struct_end()
        case UnionTypeRef():
            writer.write_struct_begin(target.node.uri)
            if active := cast(Sequence[object], datum):
                field_id, item = active
                field = target.node.field_by_id(cast(int, field_id))
                assert field is not None
                _write_field(writer, field, item)
            writer.write_field_stop()
            writer.write_struct_end()
        case _:
            raise InvalidTypeError(f"unsupported type {target!r}")


def _write_field(writer: ProtocolWriter, field: FieldDefinition, item: object) -> None:
    identity = field.identity
    writer.write_field_begin(identity.name, _ttype(field.type), identity.id)
    _write(writer, field.type, item)
    writer.write_field_end()


def _write_primitive(
    writer: ProtocolWriter, primitive: Primitive, datum: object
) -> None:
    match primitive:
        case Primitive.BOOL:
            writer.write_bool(cast(bool, datum))
        case Primitive.BYTE:
            writer.write_byte(cast(int, datum))
        case Primitive.I16:
            writer.write_i16(cast(int, datum))
        case Primitive.I32:
            writer.write_i32(cast(int, datum))
        case Primitive.I64:
            writer.write_i64(cast(int, datum))
        case Primitive.FLOAT:
            writer.write_float(cast(float, datum))
        case Primitive.DOUBLE:
            writer.write_double(cast(float, datum))
        case Primitive.STRING:
            writer.write_string(cast(str, datum))
        case Primitive.BINARY:
            writer.write_binary(cast(bytes, datum))
        case Primitive.ANY:
            protocol = _THRIFT_PYTHON_PROTOCOLS[writer.protocol]
            writer.write_raw(thrift_python.serialize(cast(AnyStruct, datum), protocol))


# ---------------------------------------------------------------------------
# Reading. The walk builds owned storage: lists for lists and structs, dicts
# for maps and sets with frozen keys, and [] or [field_id, value] for unions.
# ---------------------------------------------------------------------------


def _read(reader: ProtocolReader, type_ref: TypeRef) -> object:
    target = true_type(type_ref)
    match target:
        case PrimitiveTypeRef(primitive=primitive):
            return _read_primitive(reader, primitive)
        case EnumTypeRef():
            return reader.read_i32()
        case ListTypeRef():
            element_type = resolved(target.element_type)
            ttype, size = reader.read_list_begin()
            _check_container(reader, size, ("list element", ttype, element_type))
            elements = [_read(reader, element_type) for _ in range(size)]
            reader.read_list_end()
            return elements
        case SetTypeRef():
            element_type = resolved(target.element_type)
            ttype, size = reader.read_set_begin()
            _check_container(reader, size, ("set element", ttype, element_type))
            elements = dict.fromkeys(
                freeze_key(_read(reader, element_type), element_type)
                for _ in range(size)
            )
            reader.read_set_end()
            return elements
        case MapTypeRef():
            key_type = resolved(target.key_type)
            value_type = resolved(target.value_type)
            key_ttype, value_ttype, size = reader.read_map_begin()
            _check_container(
                reader,
                size,
                ("map key", key_ttype, key_type),
                ("map value", value_ttype, value_type),
            )
            entries: dict[object, object] = {}
            for _ in range(size):
                key = freeze_key(_read(reader, key_type), key_type)
                entries[key] = _read(reader, value_type)
            reader.read_map_end()
            return entries
        case StructTypeRef():
            return _read_struct(reader, target.node)
        case UnionTypeRef():
            return _read_union(reader, target.node)
        case _:
            raise InvalidTypeError(f"unsupported type {target!r}")


def _check_container(
    reader: ProtocolReader, size: int, *elements: tuple[str, int, TypeRef]
) -> None:
    """Check a container header before reading any element:
    element wire types only if the container is non-empty (writers may leave
    them unset otherwise), then that the input could hold ``size`` elements,
    unless the sizes come from the parsed input."""
    if not size:
        return
    for what, ttype, type_ref in elements:
        _check_ttype(ttype, type_ref, what)
    if not reader.omits_container_sizes and size * len(elements) > reader.remaining():
        raise DecodeError(
            f"truncated input: {size} elements cannot fit in {reader.remaining()} bytes"
        )


def _check_ttype(ttype: int, type_ref: TypeRef, what: str) -> None:
    expected = _ttype(type_ref)
    if ttype != expected and ttype != T_VOID:
        raise DecodeError(
            f"{what} of type '{type_name(type_ref)}' has wire type {ttype}, "
            f"expected {expected}"
        )


_MISSING = object()


def _read_struct(reader: ProtocolReader, node: StructNode) -> list[object]:
    fields = node.fields
    slots = {field.identity.id: index for index, field in enumerate(fields)}
    values: list[object] = [_MISSING] * len(fields)
    reader.read_struct_begin()
    while True:
        ttype, field_id, name = reader.read_field_begin()
        if ttype == T_STOP:
            break
        field = _header_field(node, field_id, name)
        if field is None:
            reader.skip(ttype)
        else:
            values[slots[field.identity.id]] = _read_field(reader, ttype, field)
        reader.read_field_end()
    reader.read_struct_end()
    # Defaults are built per value, since decoded storage may be edited.
    return [
        field_default(field) if value is _MISSING else value
        for field, value in zip(fields, values)
    ]


def _read_union(reader: ProtocolReader, node: UnionNode) -> list[object]:
    value: list[object] = []
    read_a_field = False
    reader.read_struct_begin()
    while True:
        ttype, field_id, name = reader.read_field_begin()
        if ttype == T_STOP:
            break
        if read_a_field:
            raise DecodeError(f"union '{node.uri}' has more than one field")
        read_a_field = True
        field = _header_field(node, field_id, name)
        if field is None:
            reader.skip(ttype)
        else:
            value = [field.identity.id, _read_field(reader, ttype, field)]
        reader.read_field_end()
    reader.read_struct_end()
    return value


def _header_field(
    node: StructNode | UnionNode, field_id: int, name: str
) -> FieldDefinition | None:
    """The field a field header names: by name if it has one, as from a
    SimpleJSON reader, and otherwise by id."""
    return node.field_by_name(name) if name else node.field_by_id(field_id)


def _read_field(reader: ProtocolReader, ttype: int, field: FieldDefinition) -> object:
    _check_ttype(ttype, field.type, f"field '{field.identity.name}'")
    return _read(reader, field.type)


def _read_primitive(reader: ProtocolReader, primitive: Primitive) -> object:
    match primitive:
        case Primitive.BOOL:
            return reader.read_bool()
        case Primitive.BYTE:
            return reader.read_byte()
        case Primitive.I16:
            return reader.read_i16()
        case Primitive.I32:
            return reader.read_i32()
        case Primitive.I64:
            return reader.read_i64()
        case Primitive.FLOAT:
            return reader.read_float()
        case Primitive.DOUBLE:
            return reader.read_double()
        case Primitive.STRING:
            return reader.read_string()
        case Primitive.BINARY:
            return reader.read_binary()
        case Primitive.ANY:
            return _read_any(reader)


def _read_any(reader: ProtocolReader) -> AnyStruct:
    data = reader.read_raw(T_STRUCT)
    try:
        return thrift_python.deserialize(
            AnyStruct, data, _THRIFT_PYTHON_PROTOCOLS[reader.protocol]
        )
    except Exception as e:  # thrift-python raises several unrelated types
        raise DecodeError(f"invalid any: {e}") from e
