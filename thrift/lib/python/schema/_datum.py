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
Schema-driven owned values. Composite storage uses plain lists/dictionaries;
map keys and set elements use recursively frozen datum. A TypeRef supplies
meaning, including field layout and integer widths.

    list, struct, exception  list (struct slots follow schema field order)
    map                      dict from frozen keys to owned values
    set                      insertion-ordered dict from frozen elements to None
    union                    [] or [field_id, payload]

Frozen storage follows the same shape: list becomes tuple, dict becomes
FrozenMap, and scalar leaves are shared. This includes set dictionaries and
empty unions. Only runtime code accesses the plain storage directly.
"""

from __future__ import annotations

import enum
from collections.abc import Sequence
from typing import Final, Literal, NoReturn, TypeVar

from apache.thrift.type.any_rep.thrift_types import AnyStruct
from thrift.lib.python.schema._errors import EncodeError
from thrift.lib.python.schema._record import (
    BoolRecord,
    ByteArrayRecord,
    FieldSetRecord,
    Float32Record,
    Float64Record,
    Int16Record,
    Int32Record,
    Int64Record,
    Int8Record,
    ListRecord,
    MapRecord,
    SerializableRecord,
    SetRecord,
    TextRecord,
)
from thrift.lib.python.schema._value_checks import (
    check_binary,
    check_bool,
    check_double,
    check_float32,
    check_int,
    check_string,
    int_in_range,
)
from thrift.lib.python.schema.type_system import (
    EnumTypeRef,
    FieldDefinition,
    InvalidTypeError,
    ListTypeRef,
    MapTypeRef,
    OpaqueAliasTypeRef,
    PresenceQualifier,
    Primitive,
    PrimitiveTypeRef,
    SetTypeRef,
    StructTypeRef,
    TypeRef,
    TypeRefBase,
    UnionTypeRef,
)

_K = TypeVar("_K")
_V = TypeVar("_V")


class _Sentinel(enum.Enum):
    UNSET = "UNSET"

    def __repr__(self) -> str:
        return self.value


UNSET: Final = _Sentinel.UNSET
Unset = Literal[_Sentinel.UNSET]


class FrozenMap(dict[_K, _V]):
    """A hashable, insertion-ordered ``dict`` with mutation disabled. Equality
    and hashing ignore order. Native code can read it through the dict C-API
    (``PyDict_Next``, ``PyDict_GetItem``)."""

    __slots__ = ("_hash",)
    _hash: int

    def __hash__(self) -> int:
        try:
            return self._hash
        except AttributeError:
            self._hash = hash(frozenset(self.items()))
            return self._hash

    def _immutable(self) -> TypeError:
        return TypeError(f"'{type(self).__name__}' object is immutable")

    def __setitem__(self, key: _K, value: _V) -> NoReturn:
        raise self._immutable()

    def __delitem__(self, key: _K) -> NoReturn:
        raise self._immutable()

    def __ior__(self, other: object) -> NoReturn:
        raise self._immutable()

    def clear(self) -> NoReturn:
        raise self._immutable()

    def pop(self, *args: object) -> NoReturn:
        raise self._immutable()

    def popitem(self) -> NoReturn:
        raise self._immutable()

    def setdefault(self, *args: object) -> NoReturn:
        raise self._immutable()

    def update(self, *args: object, **kwargs: object) -> NoReturn:
        raise self._immutable()

    def copy(self) -> FrozenMap[_K, _V]:
        return self

    def __reduce__(self) -> tuple[type[FrozenMap[_K, _V]], tuple[dict[_K, _V]]]:
        # The default dict pickling would call the disabled __setitem__.
        return (type(self), (dict(self),))

    def __repr__(self) -> str:
        return f"{type(self).__name__}({dict.__repr__(self)})"


_PRIMITIVE_DEFAULTS: dict[Primitive, object] = {
    Primitive.BOOL: False,
    Primitive.BYTE: 0,
    Primitive.I16: 0,
    Primitive.I32: 0,
    Primitive.I64: 0,
    Primitive.FLOAT: 0.0,
    Primitive.DOUBLE: 0.0,
    Primitive.STRING: "",
    Primitive.BINARY: b"",
}

INT_BITS: dict[Primitive, int] = {
    Primitive.BYTE: 8,
    Primitive.I16: 16,
    Primitive.I32: 32,
    Primitive.I64: 64,
}


def resolved(type_ref: TypeRefBase) -> TypeRef:
    """``type_ref`` as a ``TypeRef``. Container element types are typed
    ``TypeRefBase`` because builder input may hold unresolved references; a
    built type system holds only ``TypeRef``s."""
    if not isinstance(type_ref, TypeRef):
        raise InvalidTypeError(f"unresolved type {type_ref!r}")
    return type_ref


def true_type(type_ref: TypeRef) -> TypeRef:
    """``type_ref`` with an opaque alias replaced by its target. Alias
    targets are never user-defined, so one step suffices."""
    if isinstance(type_ref, OpaqueAliasTypeRef):
        return type_ref.node.target_type
    return type_ref


def type_name(type_ref: TypeRef) -> str:
    match type_ref:
        case PrimitiveTypeRef():
            return type_ref.primitive.name.lower()
        case ListTypeRef():
            return f"list<{type_name(resolved(type_ref.element_type))}>"
        case SetTypeRef():
            return f"set<{type_name(resolved(type_ref.element_type))}>"
        case MapTypeRef():
            key = type_name(resolved(type_ref.key_type))
            return f"map<{key}, {type_name(resolved(type_ref.value_type))}>"
        case _:
            return type_ref.node.uri


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------


def default_value(type_ref: TypeRef) -> object:
    """The default value of ``type_ref``: zero or empty. A struct holds each
    field's default (``field_default``)."""
    match true_type(type_ref):
        case PrimitiveTypeRef(primitive=Primitive.ANY):
            return AnyStruct()
        case PrimitiveTypeRef(primitive=primitive):
            return _PRIMITIVE_DEFAULTS[primitive]
        case EnumTypeRef():
            return 0
        case ListTypeRef():
            return []
        case SetTypeRef():
            return {}
        case MapTypeRef():
            return {}
        case StructTypeRef(node=node):
            return [field_default(f) for f in node.fields]
        case UnionTypeRef():
            return []
        case target:
            raise InvalidTypeError(f"opaque alias of user-defined type {target!r}")


def field_default(field: FieldDefinition) -> object:
    """The value a struct field holds when it is absent: ``UNSET`` if it is
    optional, else its custom default, else its type's default."""
    if field.presence is PresenceQualifier.OPTIONAL:
        return UNSET
    if field.custom_default is not None:
        return value_from_record(field.custom_default, field.type)
    return default_value(field.type)


def _record_mismatch(record: SerializableRecord, type_ref: TypeRef) -> InvalidTypeError:
    return InvalidTypeError(
        f"custom default {record!r} does not fit type '{type_name(type_ref)}'"
    )


def _int_from_record(
    record: SerializableRecord,
    record_type: type[Int8Record | Int16Record | Int32Record | Int64Record],
    bits: int,
    type_ref: TypeRef,
) -> int:
    if not isinstance(record, record_type) or not int_in_range(record.value, bits):
        raise _record_mismatch(record, type_ref)
    return record.value


_INT_RECORDS: dict[
    Primitive, type[Int8Record | Int16Record | Int32Record | Int64Record]
] = {
    Primitive.BYTE: Int8Record,
    Primitive.I16: Int16Record,
    Primitive.I32: Int32Record,
    Primitive.I64: Int64Record,
}

_ScalarRecord = (
    BoolRecord | Float32Record | Float64Record | TextRecord | ByteArrayRecord
)

_SCALAR_RECORDS: dict[Primitive, type[_ScalarRecord]] = {
    Primitive.BOOL: BoolRecord,
    Primitive.FLOAT: Float32Record,
    Primitive.DOUBLE: Float64Record,
    Primitive.STRING: TextRecord,
    Primitive.BINARY: ByteArrayRecord,
}


def _primitive_from_record(
    record: SerializableRecord, primitive: Primitive, type_ref: TypeRef
) -> object:
    int_record = _INT_RECORDS.get(primitive)
    if int_record is not None:
        return _int_from_record(record, int_record, INT_BITS[primitive], type_ref)
    scalar_record = _SCALAR_RECORDS.get(primitive)
    if scalar_record is None or not isinstance(record, scalar_record):
        raise _record_mismatch(record, type_ref)
    return record.value


def value_from_record(record: SerializableRecord, type_ref: TypeRef) -> object:
    """The value a ``SerializableRecord`` denotes as ``type_ref``, for custom
    defaults. A struct record may omit fields, which take their defaults.
    Raises ``InvalidTypeError`` if the record does not fit."""
    target = true_type(type_ref)
    match target:
        case PrimitiveTypeRef(primitive=primitive):
            return _primitive_from_record(record, primitive, type_ref)
        case EnumTypeRef():
            return _int_from_record(record, Int32Record, 32, type_ref)
        case ListTypeRef() if isinstance(record, ListRecord):
            element_type = resolved(target.element_type)
            return [value_from_record(e, element_type) for e in record.elements]
        case SetTypeRef() if isinstance(record, SetRecord):
            element_type = resolved(target.element_type)
            return dict.fromkeys(
                freeze_key(value_from_record(e, element_type), element_type)
                for e in record.elements
            )
        case MapTypeRef() if isinstance(record, MapRecord):
            key_type = resolved(target.key_type)
            value_type = resolved(target.value_type)
            return {
                freeze_key(value_from_record(k, key_type), key_type): value_from_record(
                    v, value_type
                )
                for k, v in record.entries
            }
        case StructTypeRef() if isinstance(record, FieldSetRecord):
            return [
                value_from_record(record.fields[f.identity.id], f.type)
                if f.identity.id in record.fields
                else field_default(f)
                for f in target.node.fields
            ]
        case UnionTypeRef() if isinstance(record, FieldSetRecord):
            return _union_from_record(record, target)
        case _:
            raise _record_mismatch(record, type_ref)


def _union_from_record(record: FieldSetRecord, type_ref: UnionTypeRef) -> object:
    if not record.fields:
        return []
    if len(record.fields) > 1:
        raise InvalidTypeError(
            f"union record {record!r} has more than one active field"
        )
    ((field_id, field_record),) = record.fields.items()
    field = type_ref.node.field_by_id(field_id)
    if field is None:
        raise InvalidTypeError(
            f"unknown field id {field_id} in record for union '{type_ref.node.uri}'"
        )
    return [field_id, value_from_record(field_record, field.type)]


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def check_value(value: object, type_ref: TypeRef) -> None:
    """Validate canonical owned datum, including its frozen hashed positions."""
    _check_value(value, type_ref, False, set())


def check_frozen_key(value: object, type_ref: TypeRef) -> None:
    """Validate the uniformly frozen representation of a key or element."""
    _check_value(value, type_ref, True, set())


def copy_value(value: object, type_ref: TypeRef, *, frozen: bool = False) -> object:
    """Validate canonical datum, owned or (with ``frozen``) frozen, and copy it
    into owned storage with each mutable branch independent."""
    _check_value(value, type_ref, frozen, set())
    return _copy_tree(value)


def freeze_key(value: object, type_ref: TypeRef) -> object:
    """Validate canonical datum before freezing it for a hashed position."""
    check_value(value, type_ref)
    return _freeze_tree(value)


def _copy_tree(value: object) -> object:
    """Copy a validated tree; immutable scalars and hashed keys can be shared."""
    if isinstance(value, (list, tuple)):
        return [_copy_tree(child) for child in value]
    if isinstance(value, dict):
        return {key: _copy_tree(child) for key, child in value.items()}
    return value


def _freeze_tree(value: object) -> object:
    """Freeze a validated tree whose dictionary keys are already immutable."""
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_tree(child) for child in value)
    if isinstance(value, dict):
        return FrozenMap({key: _freeze_tree(child) for key, child in value.items()})
    return value


def values_equal(left: object, right: object) -> bool:
    """Whether two valid trees of one type are equal. Owned and frozen storage
    compare alike, and dictionaries ignore order. Scalars compare by identity
    first, so a stored NaN equals itself."""
    if left is right:
        return True
    if isinstance(left, (list, tuple)):
        return (
            isinstance(right, (list, tuple))
            and len(left) == len(right)
            and all(values_equal(a, b) for a, b in zip(left, right))
        )
    if isinstance(left, dict):
        # Keys are frozen on both sides, so they hash and compare directly.
        return (
            isinstance(right, dict)
            and len(left) == len(right)
            and all(
                key in right and values_equal(item, right[key])
                for key, item in left.items()
            )
        )
    return left == right


def _check_value(
    value: object,
    type_ref: TypeRef,
    frozen: bool,
    active: set[int],
) -> None:
    target = true_type(type_ref)
    if isinstance(target, PrimitiveTypeRef):
        _check_primitive(value, target.primitive, type_name(type_ref))
        return
    if isinstance(target, EnumTypeRef):
        check_int(value, 32, type_name(type_ref))
        return
    identity = id(value)
    if identity in active:
        raise EncodeError(f"cyclic datum for type '{type_name(type_ref)}'")
    active.add(identity)
    try:
        match target:
            case ListTypeRef():
                _check_list(value, target, frozen, active)
            case SetTypeRef():
                _check_set(value, target, frozen, active)
            case MapTypeRef():
                _check_map(value, target, frozen, active)
            case StructTypeRef():
                _check_struct(value, target, frozen, active)
            case UnionTypeRef():
                _check_union(value, target, frozen, active)
            case _:
                raise InvalidTypeError(f"unsupported type {target!r}")
    finally:
        active.remove(identity)


def _sequence(
    value: object, name: str, frozen: bool
) -> list[object] | tuple[object, ...]:
    if frozen:
        return expect_instance(value, tuple, name)
    return expect_instance(value, list, name, exact=True)


def _dictionary(value: object, name: str, frozen: bool) -> dict[object, object]:
    if frozen:
        return expect_instance(value, FrozenMap, name)
    return expect_instance(value, dict, name, exact=True)


def _check_list(
    value: object,
    target: ListTypeRef,
    frozen: bool,
    active: set[int],
) -> None:
    element_type = resolved(target.element_type)
    for element in _sequence(value, type_name(target), frozen):
        _check_value(element, element_type, frozen, active)


def _check_set(
    value: object,
    target: SetTypeRef,
    frozen: bool,
    active: set[int],
) -> None:
    entries = _dictionary(value, type_name(target), frozen)
    if any(item is not None for item in entries.values()):
        raise EncodeError("set entries must have None payloads")
    element_type = resolved(target.element_type)
    for element in entries:
        _check_value(element, element_type, True, active)


def _check_map(
    value: object,
    target: MapTypeRef,
    frozen: bool,
    active: set[int],
) -> None:
    entries = _dictionary(value, type_name(target), frozen)
    key_type = resolved(target.key_type)
    value_type = resolved(target.value_type)
    for key, item in entries.items():
        _check_value(key, key_type, True, active)
        _check_value(item, value_type, frozen, active)


def _check_struct(
    value: object,
    target: StructTypeRef,
    frozen: bool,
    active: set[int],
) -> None:
    values = check_struct_shape(value, target, frozen=frozen)
    for field, item in zip(target.node.fields, values):
        try:
            if item is UNSET:
                if field.presence is not PresenceQualifier.OPTIONAL:
                    raise EncodeError(
                        "a non-optional field must hold a value, not UNSET"
                    )
            else:
                _check_value(item, field.type, frozen, active)
        except EncodeError as error:
            raise field_error(target, field, error) from None


def _check_union(
    value: object,
    target: UnionTypeRef,
    frozen: bool,
    active: set[int],
) -> None:
    active_field = check_union_shape(value, target, frozen=frozen)
    if active_field is not None:
        field, item = active_field
        try:
            _check_value(item, field.type, frozen, active)
        except EncodeError as error:
            raise field_error(target, field, error) from None


def _check_primitive(value: object, primitive: Primitive, name: str) -> None:
    match primitive:
        case Primitive.BOOL:
            check_bool(value, name)
        case Primitive.BYTE | Primitive.I16 | Primitive.I32 | Primitive.I64:
            check_int(value, INT_BITS[primitive], name)
        case Primitive.FLOAT:
            check_float32(value, name)
        case Primitive.DOUBLE:
            check_double(value, name)
        case Primitive.STRING:
            check_string(value, name)
        case Primitive.BINARY:
            check_binary(value, name)
        case Primitive.ANY:
            check_any(value)


def check_any(value: object) -> AnyStruct:
    if not isinstance(value, AnyStruct):
        raise EncodeError(
            f"value {value!r} of Python type '{type(value).__name__}' does not fit "
            "type 'any' (an AnyStruct)"
        )
    return value


_C = TypeVar("_C")


def expect_instance(
    value: object, kind: type[_C], name: str, *, exact: bool = False
) -> _C:
    if not isinstance(value, kind) or (exact and type(value) is not kind):
        raise EncodeError(
            f"value {value!r} of Python type '{type(value).__name__}' does not fit "
            f"type '{name}' (a {kind.__name__})"
        )
    return value


def _check_field_count(values: Sequence[object], type_ref: StructTypeRef) -> None:
    if len(values) != len(type_ref.node.fields):
        raise EncodeError(
            f"value does not fit struct '{type_ref.node.uri}': expected "
            f"{len(type_ref.node.fields)} field values"
        )


def check_struct_shape(
    value: object,
    type_ref: StructTypeRef,
    *,
    frozen: bool = False,
) -> list[object] | tuple[object, ...]:
    values = _sequence(value, type_ref.node.uri, frozen)
    _check_field_count(values, type_ref)
    return values


def check_union_shape(
    value: object,
    type_ref: UnionTypeRef,
    *,
    frozen: bool = False,
) -> tuple[FieldDefinition, object] | None:
    values = _sequence(value, type_ref.node.uri, frozen)
    if not values:
        return None
    if len(values) == 2:
        field_id, field_value = values
        if isinstance(field_id, int) and not isinstance(field_id, bool):
            field = type_ref.node.field_by_id(field_id)
            if field is not None and field_value is not UNSET:
                return field, field_value
    raise EncodeError(
        f"value does not fit union '{type_ref.node.uri}': invalid active field"
    )


def field_error(
    type_ref: StructTypeRef | UnionTypeRef, field: FieldDefinition, error: EncodeError
) -> EncodeError:
    """``error`` prefixed with the field it occurred in."""
    path = f"{type_ref.node.uri.rpartition('/')[2]}.{field.identity.name}"
    return EncodeError(f"{path}: {error}")
