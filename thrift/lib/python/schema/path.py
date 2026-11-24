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
Typed paths through Thrift values.

A ``Path`` is a root ``TypeRef`` and the components that step from it into a
value. ``PathBuilder`` builds one, checking each component against the
current type, or parses one from a string; ``Path.traverse`` follows one
through a ``DynamicValue``::

    builder = PathBuilder(my_struct_type)
    with builder.enter_field("users"), builder.enter_map_value("alice"):
        path = builder.path()
    assert str(path) == 'MyStruct.users["alice"]'
    profile = path.traverse(value)  # None if "alice" is absent
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from typing import assert_never, NoReturn, overload, TypeVar

from thrift.lib.python.schema._datum import resolved, true_type, type_name
from thrift.lib.python.schema._errors import DecodeError, EncodeError
from thrift.lib.python.schema._simple_json_protocol import parse_json_prefix
from thrift.lib.python.schema.dynamic_value import DynamicValue, MutableDynamicValue
from thrift.lib.python.schema.serialization import deserialize, Protocol, serialize
from thrift.lib.python.schema.type_system import (
    FieldDefinition,
    ListTypeRef,
    MapTypeRef,
    Primitive,
    PrimitiveTypeRef,
    SetTypeRef,
    StructTypeRef,
    TypeRef,
    TypeRefBase,
    TypeSystem,
    UnionTypeRef,
)
from thrift.lib.python.schema.type_system_builder import _resolve, UnresolvedTypeRef

__all__ = [
    "AnyType",
    "Component",
    "FieldAccess",
    "InvalidPathAccessError",
    "ListElement",
    "MapKey",
    "MapValue",
    "Path",
    "PathBuilder",
    "SetElement",
    "type_display_name",
]

_K = TypeVar("_K", ListTypeRef, SetTypeRef, MapTypeRef)

# C++ list indices are ``std::size_t``.
_MAX_LIST_INDEX: int = 2**64 - 1


class InvalidPathAccessError(Exception):
    """Raised when a path component does not apply to the type it is applied
    to, or a path string does not parse against its root type."""


def type_display_name(type_ref: TypeRef) -> str:
    """The name of ``type_ref`` at the start of a path string, as C++
    ``typeDisplayName`` writes it: a user-defined type by the last segment of
    its URI."""
    match type_ref:
        case PrimitiveTypeRef():
            return type_ref.primitive.name.lower()
        case ListTypeRef():
            return f"list<{type_display_name(resolved(type_ref.element_type))}>"
        case SetTypeRef():
            return f"set<{type_display_name(resolved(type_ref.element_type))}>"
        case MapTypeRef():
            key = type_display_name(resolved(type_ref.key_type))
            return f"map<{key}, {type_display_name(resolved(type_ref.value_type))}>"
        case _:
            return type_ref.node.uri.rpartition("/")[2]


# ---------------------------------------------------------------------------
# Components. Each prints as its fragment of the path string, and compares and
# hashes on what that fragment encodes.
# ---------------------------------------------------------------------------


class _Component:
    __slots__ = ()

    def _key(self) -> tuple[object, ...]:
        raise NotImplementedError

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _Component):
            return NotImplemented
        return type(self) is type(other) and self._key() == other._key()

    def __hash__(self) -> int:
        return hash((type(self), self._key()))


class FieldAccess(_Component):
    """``.name``: a field of a struct, union, or exception. Compares by the
    structured type and the field id."""

    __slots__ = ("_structured_type", "_field")
    __match_args__ = ("structured_type", "field")
    _structured_type: StructTypeRef | UnionTypeRef
    _field: FieldDefinition

    def __init__(
        self, structured_type: StructTypeRef | UnionTypeRef, field: FieldDefinition
    ) -> None:
        self._structured_type = structured_type
        self._field = field

    @property
    def structured_type(self) -> StructTypeRef | UnionTypeRef:
        return self._structured_type

    @property
    def field(self) -> FieldDefinition:
        return self._field

    @property
    def field_id(self) -> int:
        return self._field.identity.id

    @property
    def field_name(self) -> str:
        return self._field.identity.name

    @property
    def field_type(self) -> TypeRef:
        return self._field.type

    def _key(self) -> tuple[object, ...]:
        return (self._structured_type, self.field_id)

    def __str__(self) -> str:
        return f".{self.field_name}"

    def __repr__(self) -> str:
        return f"FieldAccess({self._structured_type!r}, {self.field_name!r})"


class ListElement(_Component):
    """``[index]``: an element of a list."""

    __slots__ = ("_index",)
    __match_args__ = ("index",)
    _index: int

    def __init__(self, index: int) -> None:
        self._index = index

    @property
    def index(self) -> int:
        return self._index

    def _key(self) -> tuple[object, ...]:
        return (self._index,)

    def __str__(self) -> str:
        return f"[{self._index}]"

    def __repr__(self) -> str:
        return f"ListElement({self._index})"


class AnyType(_Component):
    """``[type name]``: the value an ``any`` holds, as a known type. The type
    is written in full, as C++ ``TypeId::name()`` writes it."""

    __slots__ = ("_type",)
    __match_args__ = ("type",)
    _type: TypeRef

    def __init__(self, type: TypeRef) -> None:
        self._type = type

    @property
    def type(self) -> TypeRef:
        return self._type

    def _key(self) -> tuple[object, ...]:
        return (self._type,)

    def __str__(self) -> str:
        return f"[{type_name(self._type)}]"

    def __repr__(self) -> str:
        return f"AnyType({self._type!r})"


class _Selector(_Component):
    """A set element or map key: a read-only copy of a ``DynamicValue`` of the
    container's element or key type, and its SimpleJSON text, as C++
    ``toSimpleJSON`` writes it. Compares by the type and the text, so a NaN
    selector equals itself, and ``-0.0`` does not equal ``0.0``.

    Raises ``InvalidPathAccessError`` if the value has no SimpleJSON form,
    such as when it holds a map with struct or container keys."""

    __slots__ = ("_value", "_text")
    _value: DynamicValue
    _text: str

    def __init__(self, value: DynamicValue) -> None:
        self._value = value.copy().read_only()
        try:
            self._text = serialize(self._value, Protocol.SIMPLE_JSON).decode()
        except EncodeError as e:
            raise InvalidPathAccessError(f"invalid selector: {e}") from e

    def _key(self) -> tuple[object, ...]:
        return (self._value.type_ref, self._text)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._text})"


class SetElement(_Selector):
    """``{value}``: an element of a set."""

    __slots__ = ()
    __match_args__ = ("value",)

    @property
    def value(self) -> DynamicValue:
        return self._value

    def __str__(self) -> str:
        return f"{{{self._text}}}"


class MapKey(_Selector):
    """``{key}``: a key of a map."""

    __slots__ = ()
    __match_args__ = ("key",)

    @property
    def key(self) -> DynamicValue:
        return self._value

    def __str__(self) -> str:
        return f"{{{self._text}}}"


class MapValue(_Selector):
    """``[key]``: the value of a map at a key."""

    __slots__ = ()
    __match_args__ = ("key",)

    @property
    def key(self) -> DynamicValue:
        return self._value

    def __str__(self) -> str:
        return f"[{self._text}]"


Component = FieldAccess | ListElement | SetElement | MapKey | MapValue | AnyType


# ---------------------------------------------------------------------------
# Selectors
# ---------------------------------------------------------------------------


def _selector(value: object, type_ref: TypeRef) -> DynamicValue:
    """``value``, raw datum or a ``DynamicValue``, as a selector of
    ``type_ref``. Raw datum must fit ``type_ref`` as a ``DynamicValue`` datum
    does; a ``DynamicValue`` must have that exact ``TypeRef``."""
    if isinstance(value, DynamicValue):
        if value.type_ref != type_ref:
            raise InvalidPathAccessError(
                f"selector of type '{type_display_name(value.type_ref)}' does not "
                f"match '{type_display_name(type_ref)}'"
            )
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        value = int(value)
    try:
        return MutableDynamicValue(type_ref, value)
    except EncodeError as e:
        raise InvalidPathAccessError(f"invalid selector: {e}") from e


# ---------------------------------------------------------------------------
# Path
# ---------------------------------------------------------------------------


class Path:
    """A root type and the components that step from it into a value, built
    by ``PathBuilder``. Its string form is byte-identical to C++
    ``Path::toString()``.

    Two paths are equal when their root types and components are, which is
    when their root types and strings are."""

    __slots__ = ("_root_type", "_components")
    _root_type: TypeRef
    _components: tuple[Component, ...]

    def __init__(self, root_type: TypeRef) -> None:
        """The empty path at ``root_type``; ``PathBuilder`` builds longer
        ones."""
        self._root_type = root_type
        self._components = ()

    @property
    def root_type(self) -> TypeRef:
        return self._root_type

    @property
    def components(self) -> tuple[Component, ...]:
        return self._components

    @property
    def target_type(self) -> TypeRef:
        """The type of the value the path leads to, computed on each call."""
        type_ref = self._root_type
        for component in self._components:
            type_ref = _next_type(type_ref, component)
        return type_ref

    @overload
    def traverse(self, value: MutableDynamicValue) -> MutableDynamicValue | None: ...

    @overload
    def traverse(self, value: DynamicValue) -> DynamicValue | None: ...

    def traverse(self, value: DynamicValue) -> DynamicValue | None:
        """The value this path leads to from ``value``, as C++
        ``DynamicConstRef::traverse`` and ``DynamicRef::traverse`` find it,
        or ``None`` if an optional field, union field, list element, set
        element, or map entry along the way is absent.

        A ``MutableDynamicValue`` yields a ``MutableDynamicValue`` that shares
        its storage, so it cannot reach a set element or map key, which are
        frozen; traverse ``value.read_only()`` to read one.

        Raises ``InvalidPathAccessError`` if a component does not apply to
        the value it reaches, and ``NotImplementedError`` for an ``any``
        component."""
        current = value
        for component in self._components:
            child = _step(current, component)
            if child is None:
                return None
            current = child
        return current

    def to_string(self) -> str:
        return type_display_name(self._root_type) + "".join(
            str(component) for component in self._components
        )

    def __str__(self) -> str:
        return self.to_string()

    def __repr__(self) -> str:
        return f"Path({self.to_string()!r})"

    def __len__(self) -> int:
        return len(self._components)

    def __iter__(self) -> Iterator[Component]:
        return iter(self._components)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Path):
            return NotImplemented
        return (self._root_type, self._components) == (
            other._root_type,
            other._components,
        )

    def __hash__(self) -> int:
        return hash((self._root_type, self._components))


_KIND_NAMES: dict[type[TypeRef], str] = {
    ListTypeRef: "list",
    SetTypeRef: "set",
    MapTypeRef: "map",
}


def _expect_kind(type_ref: TypeRef, kind: type[_K], access: str) -> _K:
    """``type_ref``, unwrapped, if it is a ``kind``. ``access`` names the
    component for the error, e.g. ``"list element"``."""
    target = true_type(type_ref)
    if not isinstance(target, kind):
        raise InvalidPathAccessError(
            f"cannot access {access} on non-{_KIND_NAMES[kind]} type "
            f"'{type_display_name(type_ref)}'"
        )
    return target


def _is_any(type_ref: TypeRef) -> bool:
    match true_type(type_ref):
        case PrimitiveTypeRef(primitive=Primitive.ANY):
            return True
        case _:
            return False


def _next_type(type_ref: TypeRef, component: Component) -> TypeRef:
    """The type that ``component`` steps to from ``type_ref``."""
    match component:
        case FieldAccess():
            return component.field_type
        case ListElement():
            target = _expect_kind(type_ref, ListTypeRef, "list element")
            return resolved(target.element_type)
        case SetElement():
            target = _expect_kind(type_ref, SetTypeRef, "set element")
            return resolved(target.element_type)
        case MapKey():
            return resolved(_expect_kind(type_ref, MapTypeRef, "map key").key_type)
        case MapValue():
            target = _expect_kind(type_ref, MapTypeRef, "map value")
            return resolved(target.value_type)
        case AnyType():
            return component.type
        case _:
            assert_never(component)


# ---------------------------------------------------------------------------
# Traversal
# ---------------------------------------------------------------------------


def _step(value: DynamicValue, component: Component) -> DynamicValue | None:
    """The child of ``value`` that ``component`` selects, or ``None`` if it is
    absent."""
    match component:
        case FieldAccess():
            return _field_of(value, component)
        case ListElement(index=index):
            _expect_kind(value.type_ref, ListTypeRef, "list element")
            elements = value.as_list()
            return elements[index] if index < len(elements) else None
        case SetElement(value=element):
            target = _expect_kind(value.type_ref, SetTypeRef, "set element")
            _check_read_only(value, "set element")
            _check_selector(element, target.element_type)
            return value.as_set().find(element)
        case MapKey(key=key):
            target = _expect_kind(value.type_ref, MapTypeRef, "map key")
            _check_read_only(value, "map key")
            _check_selector(key, target.key_type)
            return value.as_map().find_key(key)
        case MapValue(key=key):
            target = _expect_kind(value.type_ref, MapTypeRef, "map value")
            _check_selector(key, target.key_type)
            try:
                return value.as_map().lookup(key)
            except KeyError:
                return None
        case AnyType():
            raise NotImplementedError("AnyType traversal is not supported")
        case _:
            assert_never(component)


def _field_of(value: DynamicValue, access: FieldAccess) -> DynamicValue | None:
    target = true_type(value.type_ref)
    if not isinstance(target, (StructTypeRef, UnionTypeRef)):
        raise InvalidPathAccessError(
            "cannot access field on non-structured type "
            f"'{type_display_name(value.type_ref)}'"
        )
    # A field id means nothing in another struct; C++ trusts the handle.
    if target != access.structured_type:
        raise InvalidPathAccessError(
            f"field '{access.field_name}' of "
            f"'{type_display_name(access.structured_type)}' does not apply to type "
            f"'{type_display_name(value.type_ref)}'"
        )
    if isinstance(target, UnionTypeRef):
        return value.as_union().get(access.field_id)
    try:
        return value.as_struct().by_id(access.field_id)
    except KeyError:
        return None


def _check_read_only(value: DynamicValue, what: str) -> None:
    if isinstance(value, MutableDynamicValue):
        raise InvalidPathAccessError(f"cannot get mutable reference to {what}")


def _check_selector(selector: DynamicValue, expected: TypeRefBase) -> None:
    if selector.type_ref != expected:
        raise InvalidPathAccessError(
            f"selector type '{type_display_name(selector.type_ref)}' does not match "
            f"container type '{type_display_name(resolved(expected))}'"
        )


# ---------------------------------------------------------------------------
# PathBuilder
# ---------------------------------------------------------------------------


class PathBuilder:
    """Builds a ``Path``, checking each component against the current type.

    Each ``enter_*`` method pushes a component and returns a context manager
    that, on exit, removes it and everything entered after it::

        builder = PathBuilder(my_struct_type)
        with builder.enter_field("users"), builder.enter_map_value("alice"):
            assert builder.to_string() == 'MyStruct.users["alice"]'
        assert builder.to_string() == "MyStruct"

    A component entered outside a ``with`` statement stays until the
    enclosing one exits, or for good at the top level. ``enter_*`` raises
    ``InvalidPathAccessError``, leaving the builder unchanged, if the
    component does not apply to the current type. Opaque aliases are
    unwrapped."""

    __slots__ = ("_root_type", "_components", "_types")
    _root_type: TypeRef
    _components: list[Component]
    # The current type after each component, with the number of components up
    # to it; ``enter_type_context`` pushes a type with no component. A scope
    # restores this stack, and the components follow from its top.
    _types: list[tuple[TypeRef, int]]

    def __init__(self, root_type: TypeRef) -> None:
        self._root_type = root_type
        self._components = []
        self._types = [(root_type, 0)]

    @property
    def current_type(self) -> TypeRef:
        return self._types[-1][0]

    def path(self) -> Path:
        """A snapshot of the path built so far."""
        path = Path(self._root_type)
        path._components = tuple(self._components)
        return path

    def to_string(self) -> str:
        return self.path().to_string()

    @classmethod
    def from_string(
        cls, type_system: TypeSystem, root_type: TypeRef, path: str
    ) -> PathBuilder:
        """A builder holding the path that the string ``path`` denotes from
        ``root_type``, parsed as C++ ``PathBuilder::fromString`` parses it.
        The leading type name may be omitted, and is not checked.

        Raises ``InvalidPathAccessError`` if ``path`` does not parse against
        ``root_type``, or ``InvalidTypeError`` if the type in an ``any``
        component does not resolve in ``type_system``."""
        parser = _PathParser(path, type_system)
        builder = cls(root_type)
        try:
            parser.skip_type_name()
            current = root_type
            while not parser.at_end():
                component, current = parser.parse_next(current)
                builder._push(component, current)
        except RecursionError:
            # Type names are parsed recursively.
            raise InvalidPathAccessError(
                "Invalid type name: nested too deeply"
            ) from None
        return builder

    def enter_field(self, field: str | int) -> _Scope:
        """Enter the field named, or with the id, ``field``."""
        if isinstance(field, bool) or not isinstance(field, (str, int)):
            raise TypeError(f"field {field!r} is neither a name nor an id")
        current = self.current_type
        target = true_type(current)
        if not isinstance(target, (StructTypeRef, UnionTypeRef)):
            raise InvalidPathAccessError(
                f"cannot access field '{field}' on non-structured type "
                f"'{type_display_name(current)}'"
            )
        node = target.node
        if isinstance(field, str):
            definition = node.field_by_name(field)
        else:
            definition = node.field_by_id(field)
        if definition is None:
            raise InvalidPathAccessError(
                f"field '{field}' does not exist on type '{type_display_name(current)}'"
            )
        return self._enter(FieldAccess(target, definition), definition.type)

    def enter_list_element(self, index: int) -> _Scope:
        if isinstance(index, bool) or not isinstance(index, int):
            raise TypeError(f"list index {index!r} is not an int")
        target = _expect_kind(self.current_type, ListTypeRef, "list element")
        if not 0 <= index <= _MAX_LIST_INDEX:
            raise InvalidPathAccessError(
                f"list index {index} is not in the range [0, 2**64)"
            )
        return self._enter(ListElement(int(index)), resolved(target.element_type))

    def enter_set_element(self, value: object) -> _Scope:
        """Enter the element ``value``, raw datum or a ``DynamicValue``, of a
        set."""
        target = _expect_kind(self.current_type, SetTypeRef, "set element")
        element_type = resolved(target.element_type)
        return self._enter(SetElement(_selector(value, element_type)), element_type)

    def enter_map_key(self, key: object) -> _Scope:
        """Enter the key ``key``, raw datum or a ``DynamicValue``, of a map."""
        target = _expect_kind(self.current_type, MapTypeRef, "map key")
        key_type = resolved(target.key_type)
        return self._enter(MapKey(_selector(key, key_type)), key_type)

    def enter_map_value(self, key: object) -> _Scope:
        """Enter the value at ``key``, raw datum or a ``DynamicValue``, of a
        map."""
        target = _expect_kind(self.current_type, MapTypeRef, "map value")
        selector = _selector(key, resolved(target.key_type))
        return self._enter(MapValue(selector), resolved(target.value_type))

    def enter_any_type(self, known_type: TypeRef) -> _Scope:
        """Enter the value of an ``any``, as ``known_type``."""
        current = self.current_type
        if not _is_any(current):
            raise InvalidPathAccessError(
                f"cannot access any type on non-any type '{type_display_name(current)}'"
            )
        return self._enter(AnyType(known_type), known_type)

    def enter_type_context(self, type_ref: TypeRef) -> _Scope:
        """Make ``type_ref`` the current type without adding a component."""
        scope = _Scope(self)
        self._types.append((type_ref, len(self._components)))
        return scope

    def _enter(self, component: Component, type_ref: TypeRef) -> _Scope:
        scope = _Scope(self)
        self._push(component, type_ref)
        return scope

    def _push(self, component: Component, type_ref: TypeRef) -> None:
        self._components.append(component)
        self._types.append((type_ref, len(self._components)))


class _Scope:
    """What ``PathBuilder.enter_*`` returns: a context manager that restores
    the builder to its depth before that call. The component is pushed
    eagerly, so ``__enter__`` does nothing; ``__exit__`` never suppresses an
    exception."""

    __slots__ = ("_builder", "_depth")
    _builder: PathBuilder
    _depth: int

    def __init__(self, builder: PathBuilder) -> None:
        self._builder = builder
        self._depth = len(builder._types)

    def __enter__(self) -> None:
        pass

    def __exit__(self, *exc_info: object) -> None:
        types = self._builder._types
        del types[self._depth :]
        del self._builder._components[types[-1][1] :]


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

_PRIMITIVE_NAMES: dict[str, Primitive] = {p.name.lower(): p for p in Primitive}
_TYPE_NAME_END: re.Pattern[str] = re.compile(r"[,\[{}\]>]")
_SELECTOR_START: re.Pattern[str] = re.compile(r"[\[{]")
_URI_END: re.Pattern[str] = re.compile(r"[.\[\]{]")
_IDENTIFIER: re.Pattern[str] = re.compile(r"[A-Za-z0-9_]+")
_DIGITS: re.Pattern[str] = re.compile(r"[0-9]+")


class _PathParser:
    __slots__ = ("_text", "_pos", "_type_system")
    _text: str
    _pos: int
    _type_system: TypeSystem

    def __init__(self, text: str, type_system: TypeSystem) -> None:
        self._text = text
        self._pos = 0
        self._type_system = type_system

    def at_end(self) -> bool:
        return self._pos == len(self._text)

    def skip_type_name(self) -> None:
        if not self.at_end() and self._text[self._pos] not in ".[{":
            self._parse_type_name()

    def parse_next(self, type_ref: TypeRef) -> tuple[Component, TypeRef]:
        """The next component, which must apply to ``type_ref``, and the type
        it steps to."""
        target = true_type(type_ref)
        match target:
            case PrimitiveTypeRef(primitive=Primitive.ANY):
                return self._parse_any_type()
            case ListTypeRef():
                return self._parse_list_element(target)
            case SetTypeRef():
                self._expect("{", "set traversal")
                element_type = resolved(target.element_type)
                element = SetElement(self._parse_selector(element_type))
                self._expect("}", "set traversal")
                return element, element_type
            case MapTypeRef():
                return self._parse_map_entry(target)
            case StructTypeRef() | UnionTypeRef():
                return self._parse_field(target)
            case _:
                self._invalid("traversal target")

    def _parse_any_type(self) -> tuple[Component, TypeRef]:
        self._expect("[", "any traversal")
        end = self._text.find("]", self._pos)
        if end == -1:
            self._invalid("any traversal")
        name = self._nested(self._text[self._pos : end]).parse_whole_type_name(
            "any traversal type"
        )
        self._pos = end + 1
        known_type = _resolve(name, {}, self._type_system)
        return AnyType(known_type), known_type

    def _parse_list_element(self, target: ListTypeRef) -> tuple[Component, TypeRef]:
        self._expect("[", "list traversal")
        digits = _DIGITS.match(self._text, self._pos)
        if digits is None or (index := _list_index(digits.group())) is None:
            self._invalid("list traversal")
        self._pos = digits.end()
        self._expect("]", "list traversal")
        return ListElement(index), resolved(target.element_type)

    def _parse_map_entry(self, target: MapTypeRef) -> tuple[Component, TypeRef]:
        key_type = resolved(target.key_type)
        if self._try_consume("{"):
            key = MapKey(self._parse_selector(key_type))
            self._expect("}", "map key traversal")
            return key, key_type
        if self._try_consume("["):
            value = MapValue(self._parse_selector(key_type))
            self._expect("]", "map value traversal")
            return value, resolved(target.value_type)
        self._invalid("map traversal")

    def _parse_field(
        self, target: StructTypeRef | UnionTypeRef
    ) -> tuple[Component, TypeRef]:
        self._expect(".", "structured traversal")
        name = self._parse_identifier()
        field = target.node.field_by_name(name)
        if field is None:
            self._invalid(
                f"field name `{name}` in traversal of structured type "
                f"`{target.node.uri}`"
            )
        return FieldAccess(target, field), field.type

    def _parse_selector(self, type_ref: TypeRef) -> DynamicValue:
        # ``deserialize`` reads whole documents, so find where the JSON ends
        # first; C++ instead advances by the reader's cursor position.
        try:
            _, end = parse_json_prefix(self._text, self._pos)
        except (DecodeError, RecursionError) as e:
            self._invalid("serialized value literal", e)
        # "surrogatepass" leaves a lone surrogate for the decoder to reject.
        literal = self._text[self._pos : end].encode("utf-8", "surrogatepass")
        try:
            value = deserialize(type_ref, literal, Protocol.SIMPLE_JSON)
        except DecodeError as e:
            self._invalid("value literal for expected type", e)
        self._pos = end
        return value

    def _parse_identifier(self) -> str:
        identifier = _IDENTIFIER.match(self._text, self._pos)
        if identifier is None:
            self._invalid("identifier")
        self._pos = identifier.end()
        return identifier.group()

    def parse_whole_type_name(self, what: str) -> TypeRefBase:
        type_name = self._parse_type_name()
        if not self.at_end():
            self._invalid(what)
        return type_name

    def _parse_type_name(self) -> TypeRefBase:
        """A type name, with user-defined types left unresolved."""
        delimiter = _TYPE_NAME_END.search(self._text, self._pos)
        token = self._text[self._pos : delimiter.start() if delimiter else None]
        primitive = _PRIMITIVE_NAMES.get(token)
        if primitive is not None:
            self._pos += len(token)
            return PrimitiveTypeRef(primitive)
        if self._try_consume("list<"):
            element = self._type_name_to(
                ">", "container name", "container element type"
            )
            return ListTypeRef(element)
        if self._try_consume("set<"):
            element = self._type_name_to(
                ">", "container name", "container element type"
            )
            return SetTypeRef(element)
        if self._try_consume("map<"):
            key = self._type_name_to(",", "map name", "map key type")
            return MapTypeRef(
                key, self._type_name_to(">", "map name", "map value type")
            )
        return UnresolvedTypeRef(self._parse_uri())

    def _type_name_to(self, terminator: str, what: str, part: str) -> TypeRefBase:
        """The type name up to ``terminator``. ``what`` names the enclosing
        type name and ``part`` this one, for errors."""
        text = self._consume_to(terminator, what)
        return self._nested(text).parse_whole_type_name(part)

    def _parse_uri(self) -> str:
        """A URI, or the last segment of one, as a root type is written."""
        text, start = self._text, self._pos
        slash = text.find("/", start)
        selector = _SELECTOR_START.search(text, start)
        if slash == -1 or (selector is not None and selector.start() < slash):
            slash = start
        end = _URI_END.search(text, slash)
        self._pos = end.start() if end else len(text)
        if self._pos == start:
            self._invalid("type name")
        return text[start : self._pos]

    def _consume_to(self, terminator: str, what: str) -> str:
        """The text up to ``terminator`` outside angle brackets, consuming the
        terminator and, after a comma, one space."""
        text, start, depth = self._text, self._pos, 1
        for pos in range(start, len(text)):
            match text[pos]:
                case "<":
                    depth += 1
                case ">":
                    depth -= 1
                    if depth == 0:
                        if terminator != ">":
                            self._invalid(what)
                        self._pos = pos + 1
                        return text[start:pos]
                case "," if terminator == "," and depth == 1:
                    self._pos = pos + (2 if text.startswith(" ", pos + 1) else 1)
                    return text[start:pos]
                case _:
                    pass
        self._invalid(what)

    def _nested(self, text: str) -> _PathParser:
        return _PathParser(text, self._type_system)

    def _try_consume(self, prefix: str) -> bool:
        if not self._text.startswith(prefix, self._pos):
            return False
        self._pos += len(prefix)
        return True

    def _expect(self, prefix: str, what: str) -> None:
        if not self._try_consume(prefix):
            self._invalid(what)

    def _invalid(self, what: str, cause: Exception | None = None) -> NoReturn:
        detail = "" if cause is None else f" ({cause})"
        rest = self._text[self._pos :]
        raise InvalidPathAccessError(f"Invalid {what}{detail}: {rest}") from cause


def _list_index(digits: str) -> int | None:
    """``digits`` as a list index, or ``None`` if it exceeds ``size_t``. The
    length is checked first: ``int()`` refuses more than 4300 digits."""
    significant = digits.lstrip("0")
    if len(significant) > len(str(_MAX_LIST_INDEX)):
        return None
    index = int(significant or "0")
    return index if index <= _MAX_LIST_INDEX else None
