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


from __future__ import annotations

import contextlib
import math
import struct
import sys
import unittest
from typing import cast

import thrift.lib.python.schema.tests.codec_test.thrift_types as fixtures
from thrift.lib.python.schema.dynamic_value import (
    DynamicValue,
    MutableDynamicValue,
    UNSET,
)
from thrift.lib.python.schema.path import (
    AnyType,
    FieldAccess,
    InvalidPathAccessError,
    ListElement,
    MapKey,
    MapValue,
    Path,
    PathBuilder,
    SetElement,
    type_display_name,
)
from thrift.lib.python.schema.tests.datum_testing import (
    ANY,
    any_of_i32,
    BINARY,
    BOOL,
    BYTE,
    containers,
    DOUBLE,
    FLOAT,
    I16,
    I32,
    I64,
    registry,
    STRING,
    struct_type_of,
    to_value,
)
from thrift.lib.python.schema.type_system import (
    _type_ref_for_node,
    EnumerableTypeSystem,
    EnumValue,
    FieldIdentity,
    InvalidTypeError,
    ListTypeRef,
    MapTypeRef,
    PresenceQualifier,
    SetTypeRef,
    StructNode,
    StructTypeRef,
    TypeRef,
)
from thrift.lib.python.schema.type_system_builder import (
    FieldSpec,
    ref,
    TypeInput,
    TypeSystemBuilder,
)
from thrift.python import serializer as thrift_python

# The `PathTest` fixture, plus the enum, union and exception that traversal
# needs.
USER_PROFILE_URI = "meta.com/thrift/test/UserProfile"
MY_STRUCT_URI = "meta.com/thrift/test/MyStruct"
ROOT_URI = "meta.com/thrift/test/Root"
INNER1_URI = "meta.com/thrift/test/Inner1"
INNER2_URI = "meta.com/thrift/test/Inner2"
BOOL_PREFIX_URI = "bool.example/thrift/test/boolRecord"
LIST_ALIAS_URI = "meta.com/thrift/test/IntList"
SET_ALIAS_URI = "meta.com/thrift/test/IntSet"
MAP_ALIAS_URI = "meta.com/thrift/test/IntMap"
USER_ID_URI = "meta.com/thrift/test/UserId"
COLOR_URI = "meta.com/thrift/test/Color"
CHOICE_URI = "meta.com/thrift/test/Choice"
FAILURE_URI = "meta.com/thrift/test/Failure"
EXTRAS_URI = "meta.com/thrift/test/Extras"

OPTIONAL = PresenceQualifier.OPTIONAL


def _field(
    field_id: int,
    name: str,
    type_input: TypeInput,
    presence: PresenceQualifier = PresenceQualifier.UNQUALIFIED,
) -> FieldSpec:
    return FieldSpec(FieldIdentity(field_id, name), presence, type_input)


def _build_type_system() -> EnumerableTypeSystem:
    return (
        TypeSystemBuilder()
        .add_struct(
            USER_PROFILE_URI,
            [
                _field(1, "name", STRING),
                _field(2, "scores", ListTypeRef(I32)),
                _field(3, "tags", SetTypeRef(I32)),
                _field(4, "metadata", ANY, OPTIONAL),
            ],
        )
        .add_struct(
            MY_STRUCT_URI,
            [
                _field(1, "users", MapTypeRef(STRING, ref(USER_PROFILE_URI))),
                _field(2, "counts", MapTypeRef(I32, STRING)),
            ],
        )
        .add_struct(ROOT_URI, [_field(1, "a", INNER1_URI)])
        .add_struct(INNER1_URI, [_field(1, "b", INNER2_URI)])
        .add_struct(INNER2_URI, [_field(1, "c", I32)])
        .add_struct(BOOL_PREFIX_URI, [_field(1, "value", I32)])
        .add_opaque_alias(LIST_ALIAS_URI, ListTypeRef(I32))
        .add_opaque_alias(SET_ALIAS_URI, SetTypeRef(I32))
        .add_opaque_alias(MAP_ALIAS_URI, MapTypeRef(I32, STRING))
        .add_opaque_alias(USER_ID_URI, I64)
        .add_enum(
            COLOR_URI,
            [EnumValue("RED", 0), EnumValue("GREEN", 1), EnumValue("BLUE", 2)],
        )
        .add_union(
            CHOICE_URI,
            [
                _field(1, "number", I32),
                _field(2, "text", STRING),
                _field(3, "profile", USER_PROFILE_URI),
            ],
        )
        # An exception is a struct in the runtime type system.
        .add_struct(FAILURE_URI, [_field(1, "reason", STRING), _field(2, "code", I32)])
        .add_struct(
            EXTRAS_URI,
            [
                _field(1, "choice", CHOICE_URI),
                _field(2, "maybe", I32, OPTIONAL),
                _field(3, "labels", MapTypeRef(ref(COLOR_URI), STRING)),
                _field(4, "colors", SetTypeRef(ref(COLOR_URI))),
                _field(5, "failures", ListTypeRef(ref(FAILURE_URI))),
                _field(6, "failure", FAILURE_URI),
                _field(7, "aliased", LIST_ALIAS_URI),
            ],
        )
        .build()
    )


TYPE_SYSTEM: EnumerableTypeSystem = _build_type_system()


def _user_type(uri: str, type_system: EnumerableTypeSystem = TYPE_SYSTEM) -> TypeRef:
    return _type_ref_for_node(type_system.get_user_defined_type_or_throw(uri))


MY_STRUCT: TypeRef = _user_type(MY_STRUCT_URI)
USER_PROFILE: TypeRef = _user_type(USER_PROFILE_URI)
ROOT: TypeRef = _user_type(ROOT_URI)
INNER2: TypeRef = _user_type(INNER2_URI)
BOOL_PREFIX: TypeRef = _user_type(BOOL_PREFIX_URI)
LIST_ALIAS: TypeRef = _user_type(LIST_ALIAS_URI)
SET_ALIAS: TypeRef = _user_type(SET_ALIAS_URI)
MAP_ALIAS: TypeRef = _user_type(MAP_ALIAS_URI)
USER_ID: TypeRef = _user_type(USER_ID_URI)
COLOR: TypeRef = _user_type(COLOR_URI)
CHOICE: TypeRef = _user_type(CHOICE_URI)
FAILURE: TypeRef = _user_type(FAILURE_URI)
EXTRAS: TypeRef = _user_type(EXTRAS_URI)


def _field_type(struct_type: TypeRef, name: str) -> TypeRef:
    assert isinstance(struct_type, StructTypeRef), struct_type
    field = struct_type.node.field_by_name(name)
    assert field is not None, name
    return field.type


def _root_a_b_c(root: TypeRef = ROOT) -> Path:
    builder = PathBuilder(root)
    builder.enter_field("a")
    builder.enter_field("b")
    builder.enter_field("c")
    return builder.path()


class TypeDisplayNameTest(unittest.TestCase):
    def test_primitives(self) -> None:
        cases = [
            (BOOL, "bool"),
            (BYTE, "byte"),
            (I16, "i16"),
            (I32, "i32"),
            (I64, "i64"),
            (FLOAT, "float"),
            (DOUBLE, "double"),
            (STRING, "string"),
            (BINARY, "binary"),
            (ANY, "any"),
        ]
        for type_ref, name in cases:
            with self.subTest(name=name):
                self.assertEqual(type_display_name(type_ref), name)

    def test_nested_containers(self) -> None:
        nested = MapTypeRef(STRING, ListTypeRef(SetTypeRef(I64)))
        self.assertEqual(type_display_name(nested), "map<string, list<set<i64>>>")

    def test_user_types_use_the_last_uri_segment(self) -> None:
        cases = [
            (MY_STRUCT, "MyStruct"),
            (CHOICE, "Choice"),
            (COLOR, "Color"),
            (LIST_ALIAS, "IntList"),
            (BOOL_PREFIX, "boolRecord"),
            (ListTypeRef(MY_STRUCT), "list<MyStruct>"),
            (StructTypeRef(StructNode(uri="NoSlash")), "NoSlash"),
        ]
        for type_ref, name in cases:
            with self.subTest(name=name):
                self.assertEqual(type_display_name(type_ref), name)


class BuilderFieldListAnyTest(unittest.TestCase):
    def test_path_basics(self) -> None:
        builder = PathBuilder(USER_PROFILE)
        self.assertEqual(builder.path().to_string(), "UserProfile")
        with builder.enter_field("scores"):
            self.assertEqual(builder.path().to_string(), "UserProfile.scores")
            with builder.enter_list_element(0):
                self.assertEqual(builder.path().to_string(), "UserProfile.scores[0]")
            self.assertEqual(builder.path().to_string(), "UserProfile.scores")
        self.assertEqual(builder.path().to_string(), "UserProfile")

    def test_nested_scopes(self) -> None:
        builder = PathBuilder(ROOT)
        with builder.enter_field("a"):
            with builder.enter_field("b"):
                with builder.enter_field("c"):
                    self.assertEqual(builder.to_string(), "Root.a.b.c")
                self.assertEqual(builder.to_string(), "Root.a.b")
            self.assertEqual(builder.to_string(), "Root.a")
        self.assertEqual(builder.to_string(), "Root")

    def test_type_context_advances_the_type_without_a_component(self) -> None:
        builder = PathBuilder(MY_STRUCT)
        builder.enter_field("users")
        with builder.enter_type_context(USER_PROFILE), builder.enter_field("name"):
            self.assertEqual(builder.to_string(), "MyStruct.users.name")
            self.assertEqual(len(builder.path()), 2)
            self.assertEqual(builder.current_type, STRING)
        self.assertEqual(builder.to_string(), "MyStruct.users")
        self.assertEqual(builder.current_type, _field_type(MY_STRUCT, "users"))

    def test_export_path(self) -> None:
        self.assertEqual(_root_a_b_c().to_string(), "Root.a.b.c")

    def test_current_type(self) -> None:
        builder = PathBuilder(USER_PROFILE)
        self.assertEqual(builder.current_type, USER_PROFILE)
        with builder.enter_field("scores"):
            self.assertEqual(builder.current_type, ListTypeRef(I32))
            with builder.enter_list_element(0):
                self.assertEqual(builder.current_type, I32)
            self.assertEqual(builder.current_type, ListTypeRef(I32))
        self.assertEqual(builder.current_type, USER_PROFILE)

    def test_primitive_roots(self) -> None:
        for type_ref in (BOOL, BYTE, I16, I32, I64, FLOAT, DOUBLE, STRING, BINARY):
            with self.subTest(type_ref=type_ref):
                builder = PathBuilder(type_ref)
                self.assertEqual(builder.to_string(), type_display_name(type_ref))
                self.assertEqual(builder.current_type, type_ref)

    def test_list_root(self) -> None:
        builder = PathBuilder(ListTypeRef(I32))
        self.assertEqual(builder.to_string(), "list<i32>")
        with builder.enter_list_element(5):
            self.assertEqual(builder.to_string(), "list<i32>[5]")
            self.assertEqual(builder.current_type, I32)

    def test_enter_field_by_id(self) -> None:
        builder = PathBuilder(MY_STRUCT)
        with builder.enter_field(1):
            self.assertEqual(builder.to_string(), "MyStruct.users")
        with builder.enter_field(2):
            self.assertEqual(builder.to_string(), "MyStruct.counts")

    def test_rejects_field_selectors_that_are_not_names_or_ids(self) -> None:
        builder = PathBuilder(MY_STRUCT)
        for field in cast(list[str | int], [True, 1.0, None]):
            with self.subTest(field=field):
                with self.assertRaises(TypeError):
                    builder.enter_field(field)
        self.assertEqual(builder.to_string(), "MyStruct")

    def test_list_index_range(self) -> None:
        builder = PathBuilder(ListTypeRef(I32))
        with builder.enter_list_element(2**64 - 1):
            self.assertEqual(builder.to_string(), "list<i32>[18446744073709551615]")
        for index in (-1, 2**64):
            with self.subTest(index=index):
                with self.assertRaises(InvalidPathAccessError):
                    builder.enter_list_element(index)
        for index in cast(list[int], [True, 1.0, "0"]):
            with self.subTest(index=index):
                with self.assertRaises(TypeError):
                    builder.enter_list_element(index)
        self.assertEqual(builder.to_string(), "list<i32>")

    def test_any_type(self) -> None:
        builder = PathBuilder(USER_PROFILE)
        builder.enter_field("metadata")
        self.assertEqual(builder.current_type, ANY)
        with builder.enter_any_type(USER_PROFILE):
            self.assertEqual(
                builder.to_string(),
                "UserProfile.metadata[meta.com/thrift/test/UserProfile]",
            )
            self.assertEqual(builder.current_type, USER_PROFILE)
            with builder.enter_field("name"):
                self.assertEqual(
                    builder.to_string(),
                    "UserProfile.metadata[meta.com/thrift/test/UserProfile].name",
                )
                self.assertEqual(builder.current_type, STRING)
            with builder.enter_field("scores"), builder.enter_list_element(0):
                self.assertEqual(builder.current_type, I32)
            with self.assertRaises(InvalidPathAccessError):
                builder.enter_field("nonexistent")
        self.assertEqual(builder.to_string(), "UserProfile.metadata")
        self.assertEqual(builder.current_type, ANY)

    def test_any_type_names_use_full_uris(self) -> None:
        cases = [
            (MY_STRUCT, "any[meta.com/thrift/test/MyStruct]"),
            (I32, "any[i32]"),
            (
                MapTypeRef(STRING, ListTypeRef(MY_STRUCT)),
                "any[map<string, list<meta.com/thrift/test/MyStruct>>]",
            ),
        ]
        for known_type, expected in cases:
            with self.subTest(expected=expected):
                builder = PathBuilder(ANY)
                with builder.enter_any_type(known_type):
                    self.assertEqual(builder.to_string(), expected)

    def test_target_type_tracks_traversal(self) -> None:
        builder = PathBuilder(USER_PROFILE)
        self.assertEqual(builder.path().target_type, USER_PROFILE)
        with builder.enter_field("scores"):
            self.assertEqual(builder.path().target_type, ListTypeRef(I32))
            with builder.enter_list_element(0):
                self.assertEqual(builder.path().target_type, I32)
        with builder.enter_field("metadata"):
            self.assertEqual(builder.path().target_type, ANY)
            with builder.enter_any_type(USER_PROFILE):
                self.assertEqual(builder.path().target_type, USER_PROFILE)

    def test_target_type_rejects_components_a_type_context_misapplied(self) -> None:
        builder = PathBuilder(MY_STRUCT)
        builder.enter_field("users")
        builder.enter_type_context(ListTypeRef(I32))
        builder.enter_list_element(0)
        self.assertEqual(builder.current_type, I32)
        with self.assertRaises(InvalidPathAccessError):
            builder.path().target_type

    def test_opaque_aliases_are_unwrapped(self) -> None:
        builder = PathBuilder(LIST_ALIAS)
        with builder.enter_list_element(2):
            self.assertEqual(builder.to_string(), "IntList[2]")
            self.assertEqual(builder.current_type, I32)
        builder = PathBuilder(EXTRAS)
        with builder.enter_field("aliased"), builder.enter_list_element(0):
            self.assertEqual(builder.to_string(), "Extras.aliased[0]")
            self.assertEqual(builder.path().target_type, I32)


class BuilderErrorTest(unittest.TestCase):
    def test_invalid_field_access(self) -> None:
        builder = PathBuilder(MY_STRUCT)
        with self.assertRaisesRegex(
            InvalidPathAccessError,
            "field 'nonexistent' does not exist on type 'MyStruct'",
        ):
            builder.enter_field("nonexistent")
        with self.assertRaisesRegex(
            InvalidPathAccessError, "field '99' does not exist on type 'MyStruct'"
        ):
            builder.enter_field(99)
        builder = PathBuilder(USER_PROFILE)
        with builder.enter_field("scores"), builder.enter_list_element(0):
            with self.assertRaisesRegex(
                InvalidPathAccessError,
                "cannot access field 'foo' on non-structured type 'i32'",
            ):
                builder.enter_field("foo")

    def test_invalid_list_access(self) -> None:
        with self.assertRaisesRegex(
            InvalidPathAccessError,
            "cannot access list element on non-list type 'MyStruct'",
        ):
            PathBuilder(MY_STRUCT).enter_list_element(0)

    def test_invalid_any_access(self) -> None:
        with self.assertRaisesRegex(
            InvalidPathAccessError, "cannot access any type on non-any type 'MyStruct'"
        ):
            PathBuilder(MY_STRUCT).enter_any_type(USER_PROFILE)

    def test_a_failed_enter_leaves_the_builder_unchanged(self) -> None:
        builder = PathBuilder(USER_PROFILE)
        builder.enter_field("scores")
        for enter in (
            lambda: builder.enter_field("name"),
            lambda: builder.enter_any_type(I32),
            lambda: builder.enter_list_element(-1),
        ):
            with self.assertRaises(InvalidPathAccessError):
                enter()
            self.assertEqual(builder.to_string(), "UserProfile.scores")
            self.assertEqual(builder.current_type, ListTypeRef(I32))


class ScopeTest(unittest.TestCase):
    def test_an_enter_without_with_persists_at_top_level(self) -> None:
        builder = PathBuilder(ROOT)
        builder.enter_field("a")
        builder.enter_field("b")
        self.assertEqual(builder.to_string(), "Root.a.b")

    def test_a_bare_enter_inside_with_is_removed_when_the_block_exits(self) -> None:
        builder = PathBuilder(ROOT)
        with builder.enter_field("a"):
            builder.enter_field("b")
            self.assertEqual(builder.to_string(), "Root.a.b")
        self.assertEqual(builder.to_string(), "Root")
        self.assertEqual(builder.current_type, ROOT)

    def test_an_exception_propagates_and_restores_the_builder(self) -> None:
        builder = PathBuilder(ROOT)
        error = ValueError("boom")
        with self.assertRaises(ValueError) as raised:
            with builder.enter_field("a"), builder.enter_field("b"):
                raise error
        self.assertIs(raised.exception, error)
        self.assertEqual(builder.to_string(), "Root")
        self.assertEqual(builder.current_type, ROOT)

    def test_a_failed_enter_inside_scopes_restores_the_builder(self) -> None:
        builder = PathBuilder(ROOT)
        with self.assertRaises(InvalidPathAccessError):
            with builder.enter_field("a"):
                builder.enter_field("b")
                builder.enter_field("nonexistent")
        self.assertEqual(builder.to_string(), "Root")

    def test_closing_an_outer_scope_first_removes_the_inner_ones(self) -> None:
        builder = PathBuilder(ROOT)
        with contextlib.ExitStack() as outer, contextlib.ExitStack() as inner:
            outer.enter_context(builder.enter_field("a"))
            inner.enter_context(builder.enter_field("b"))
            outer.close()
            self.assertEqual(builder.to_string(), "Root")
            inner.close()
            self.assertEqual(builder.to_string(), "Root")
            self.assertEqual(builder.current_type, ROOT)

    def test_a_stale_scope_keeps_the_path_and_current_type_together(self) -> None:
        builder = PathBuilder(USER_PROFILE)
        with contextlib.ExitStack() as outer, contextlib.ExitStack() as inner:
            outer.enter_context(builder.enter_type_context(ListTypeRef(I32)))
            inner.enter_context(builder.enter_list_element(0))
            outer.close()
            builder.enter_field("scores")
            inner.close()
            self.assertEqual(builder.to_string(), "UserProfile.scores")
            self.assertEqual(builder.current_type, ListTypeRef(I32))

    def test_type_context_scopes_restore_the_type(self) -> None:
        builder = PathBuilder(ANY)
        with builder.enter_type_context(USER_PROFILE):
            self.assertEqual(builder.current_type, USER_PROFILE)
            self.assertEqual(builder.to_string(), "any")
        self.assertEqual(builder.current_type, ANY)

    def test_a_path_snapshot_does_not_change_with_the_builder(self) -> None:
        builder = PathBuilder(ROOT)
        builder.enter_field("a")
        path = builder.path()
        with builder.enter_field("b"):
            self.assertEqual(path.to_string(), "Root.a")
        self.assertEqual(len(path), 1)


class PathModelTest(unittest.TestCase):
    def test_inspect_components(self) -> None:
        builder = PathBuilder(USER_PROFILE)
        builder.enter_field("metadata")
        builder.enter_any_type(USER_PROFILE)
        builder.enter_field("scores")
        builder.enter_list_element(2)
        path = builder.path()

        self.assertEqual(path.root_type, USER_PROFILE)
        self.assertEqual(len(path), 4)
        metadata, any_type, _, score = path.components

        assert isinstance(metadata, FieldAccess)
        self.assertEqual(metadata.structured_type, USER_PROFILE)
        self.assertEqual(metadata.field_id, 4)
        self.assertEqual(metadata.field_name, "metadata")
        self.assertEqual(metadata.field_type, ANY)
        assert isinstance(USER_PROFILE, StructTypeRef)
        self.assertIs(metadata.field, USER_PROFILE.node.field_by_id(4))

        assert isinstance(any_type, AnyType)
        self.assertEqual(any_type.type, USER_PROFILE)
        assert isinstance(score, ListElement)
        self.assertEqual(score.index, 2)

    def test_components_support_match(self) -> None:
        path = _root_a_b_c()
        match path.components[0]:
            case FieldAccess(structured_type, field):
                self.assertEqual(structured_type, ROOT)
                self.assertEqual(field.identity.name, "a")
            case _:
                self.fail("expected a field access")
        builder = PathBuilder(ListTypeRef(ANY))
        builder.enter_list_element(7)
        builder.enter_any_type(I32)
        match builder.path().components:
            case (ListElement(7), AnyType(known_type)):
                self.assertEqual(known_type, I32)
            case _:
                self.fail("expected a list element and an any type")

    def test_sequence_protocol(self) -> None:
        empty = PathBuilder(ROOT).path()
        self.assertFalse(empty)
        self.assertEqual(len(empty), 0)
        path = _root_a_b_c()
        self.assertTrue(path)
        self.assertEqual(tuple(path), path.components)
        self.assertEqual(
            [
                component.field_name
                for component in path
                if isinstance(component, FieldAccess)
            ],
            ["a", "b", "c"],
        )

    def test_str_and_repr(self) -> None:
        path = _root_a_b_c()
        self.assertEqual(str(path), "Root.a.b.c")
        self.assertEqual(repr(path), "Path('Root.a.b.c')")
        self.assertEqual(str(path.components[0]), ".a")
        self.assertEqual(str(ListElement(3)), "[3]")
        self.assertEqual(repr(ListElement(3)), "ListElement(3)")
        self.assertEqual(str(AnyType(MY_STRUCT)), "[meta.com/thrift/test/MyStruct]")


class PathEqualityTest(unittest.TestCase):
    def _paths(self, type_system: EnumerableTypeSystem) -> list[Path]:
        builder = PathBuilder(_user_type(USER_PROFILE_URI, type_system))
        with builder.enter_field("scores"), builder.enter_list_element(3):
            scores = builder.path()
        with (
            builder.enter_field(4),
            builder.enter_any_type(_user_type(MY_STRUCT_URI, type_system)),
        ):
            metadata = builder.path()
        return [_root_a_b_c(_user_type(ROOT_URI, type_system)), scores, metadata]

    def test_equal_across_separately_built_type_systems(self) -> None:
        copy = _build_type_system()
        for path, other in zip(self._paths(TYPE_SYSTEM), self._paths(copy)):
            with self.subTest(path=str(path)):
                self.assertIsNot(path.root_type, other.root_type)
                self.assertEqual(path, other)
                self.assertEqual(hash(path), hash(other))

    def test_field_by_name_equals_field_by_id(self) -> None:
        by_name, by_id = PathBuilder(MY_STRUCT), PathBuilder(MY_STRUCT)
        by_name.enter_field("counts")
        by_id.enter_field(2)
        self.assertEqual(by_name.path(), by_id.path())

    def test_paths_that_differ_are_unequal(self) -> None:
        def path_of(root: TypeRef, *steps: str | int) -> Path:
            builder = PathBuilder(root)
            for step in steps:
                if isinstance(step, str):
                    builder.enter_field(step)
                else:
                    builder.enter_list_element(step)
            return builder.path()

        base = path_of(USER_PROFILE, "scores", 1)
        others = [
            path_of(USER_PROFILE, "scores", 2),
            path_of(USER_PROFILE, "scores"),
            path_of(USER_PROFILE, "name"),
            path_of(ROOT),
        ]
        for other in others:
            with self.subTest(other=str(other)):
                self.assertNotEqual(base, other)
        self.assertNotEqual(PathBuilder(ROOT).path(), PathBuilder(ANY).path())
        self.assertNotEqual(base, "UserProfile.scores[1]")

    def test_any_types_compare_by_type(self) -> None:
        self.assertEqual(
            AnyType(MY_STRUCT), AnyType(_user_type(MY_STRUCT_URI, _build_type_system()))
        )
        self.assertNotEqual(AnyType(MY_STRUCT), AnyType(ROOT))
        self.assertNotEqual(ListElement(1), AnyType(I32))


def _f32(value: float) -> float:
    return struct.unpack("<f", struct.pack("<f", value))[0]


def _selector_path(container: TypeRef, enter: str, selector: object) -> Path:
    builder = PathBuilder(container)
    getattr(builder, enter)(selector)
    return builder.path()


class BuilderSelectorTest(unittest.TestCase):
    def test_path_basics(self) -> None:
        builder = PathBuilder(MY_STRUCT)
        self.assertEqual(builder.path().to_string(), "MyStruct")
        with builder.enter_field("users"):
            self.assertEqual(builder.path().to_string(), "MyStruct.users")
            with builder.enter_map_value("alice"):
                self.assertEqual(builder.path().to_string(), 'MyStruct.users["alice"]')
                with builder.enter_field(2), builder.enter_list_element(0):
                    self.assertEqual(
                        builder.path().to_string(), 'MyStruct.users["alice"].scores[0]'
                    )
                self.assertEqual(builder.path().to_string(), 'MyStruct.users["alice"]')
        self.assertEqual(builder.path().to_string(), "MyStruct")

    def test_all_component_types(self) -> None:
        builder = PathBuilder(MY_STRUCT)
        with builder.enter_field("users"):
            self.assertEqual(builder.to_string(), "MyStruct.users")
        with (
            builder.enter_field("users"),
            builder.enter_map_value("alice"),
            builder.enter_field("scores"),
            builder.enter_list_element(42),
        ):
            self.assertEqual(builder.to_string(), 'MyStruct.users["alice"].scores[42]')
        with (
            builder.enter_field("users"),
            builder.enter_map_value("alice"),
            builder.enter_field("tags"),
            builder.enter_set_element(123),
        ):
            self.assertEqual(builder.to_string(), 'MyStruct.users["alice"].tags{123}')
        with builder.enter_field("users"), builder.enter_map_key("key"):
            self.assertEqual(builder.to_string(), 'MyStruct.users{"key"}')
        with (
            builder.enter_field("users"),
            builder.enter_map_value("alice"),
            builder.enter_field("metadata"),
            builder.enter_any_type(USER_PROFILE),
        ):
            self.assertEqual(
                builder.to_string(),
                'MyStruct.users["alice"].metadata[meta.com/thrift/test/UserProfile]',
            )

    def test_all_access_types(self) -> None:
        builder = PathBuilder(MY_STRUCT)
        with builder.enter_field("users"):
            with builder.enter_map_value("alice"):
                with builder.enter_field("tags"):
                    with builder.enter_set_element(42):
                        self.assertEqual(
                            builder.to_string(), 'MyStruct.users["alice"].tags{42}'
                        )
                    self.assertEqual(
                        builder.to_string(), 'MyStruct.users["alice"].tags'
                    )
                with (
                    builder.enter_field("metadata"),
                    builder.enter_any_type(USER_PROFILE),
                    builder.enter_field("name"),
                ):
                    self.assertEqual(
                        builder.to_string(),
                        'MyStruct.users["alice"].metadata'
                        "[meta.com/thrift/test/UserProfile].name",
                    )
            with builder.enter_map_key("bob"):
                self.assertEqual(builder.to_string(), 'MyStruct.users{"bob"}')
        with builder.enter_field("counts"), builder.enter_map_key(123):
            self.assertEqual(builder.to_string(), "MyStruct.counts{123}")
        self.assertEqual(builder.to_string(), "MyStruct")

    def test_formats_selector_types(self) -> None:
        builder = PathBuilder(MY_STRUCT)
        with builder.enter_field("users"), builder.enter_map_value("42"):
            self.assertEqual(builder.to_string(), 'MyStruct.users["42"]')
        with builder.enter_field("counts"), builder.enter_map_value(42):
            self.assertEqual(builder.to_string(), "MyStruct.counts[42]")

    def test_container_roots(self) -> None:
        set_builder = PathBuilder(SetTypeRef(STRING))
        self.assertEqual(set_builder.to_string(), "set<string>")
        with set_builder.enter_set_element("hello"):
            self.assertEqual(set_builder.to_string(), 'set<string>{"hello"}')
            self.assertEqual(set_builder.current_type, STRING)
        map_builder = PathBuilder(MapTypeRef(I32, STRING))
        self.assertEqual(map_builder.to_string(), "map<i32, string>")
        with map_builder.enter_map_value(42):
            self.assertEqual(map_builder.to_string(), "map<i32, string>[42]")
            self.assertEqual(map_builder.current_type, STRING)
        with map_builder.enter_map_key(42):
            self.assertEqual(map_builder.to_string(), "map<i32, string>{42}")
            self.assertEqual(map_builder.current_type, I32)

    def test_target_type_tracks_traversal(self) -> None:
        builder = PathBuilder(MY_STRUCT)
        builder.enter_field("users")
        self.assertEqual(builder.path().target_type, MapTypeRef(STRING, USER_PROFILE))
        with builder.enter_map_key("alice"):
            self.assertEqual(builder.path().target_type, STRING)
        builder.enter_map_value("alice")
        self.assertEqual(builder.path().target_type, USER_PROFILE)
        with builder.enter_field("tags"):
            self.assertEqual(builder.path().target_type, SetTypeRef(I32))
            builder.enter_set_element(42)
            self.assertEqual(builder.path().target_type, I32)

    def test_opaque_alias_containers_are_unwrapped(self) -> None:
        cases = [
            (SET_ALIAS, "enter_set_element", 3, "IntSet{3}", I32),
            (MAP_ALIAS, "enter_map_key", 4, "IntMap{4}", I32),
            (MAP_ALIAS, "enter_map_value", 4, "IntMap[4]", STRING),
        ]
        for root, enter, selector, expected, target_type in cases:
            with self.subTest(expected=expected):
                path = _selector_path(root, enter, selector)
                self.assertEqual(path.to_string(), expected)
                self.assertEqual(path.target_type, target_type)

    def test_inspect_components(self) -> None:
        builder = PathBuilder(MY_STRUCT)
        builder.enter_field("users")
        builder.enter_map_value("alice")
        user = builder.path().components[1]
        assert isinstance(user, MapValue)
        self.assertEqual(user.key, MutableDynamicValue(STRING, "alice"))

        counts_key = _selector_path(MapTypeRef(I32, STRING), "enter_map_key", 42)
        key = counts_key.components[-1]
        assert isinstance(key, MapKey)
        self.assertEqual(key.key, MutableDynamicValue(I32, 42))

        element = _selector_path(SetTypeRef(STRING), "enter_set_element", "value")
        set_element = element.components[0]
        assert isinstance(set_element, SetElement)
        self.assertEqual(set_element.value, MutableDynamicValue(STRING, "value"))
        self.assertIs(type(set_element.value), DynamicValue)

    def test_selectors_support_match_and_print(self) -> None:
        path = _selector_path(MapTypeRef(STRING, I32), "enter_map_value", "k")
        match path.components:
            case (MapValue(key),):
                self.assertEqual(key.as_str(), "k")
            case _:
                self.fail("expected a map value")
        self.assertEqual(str(path.components[0]), '["k"]')
        self.assertEqual(repr(path.components[0]), 'MapValue("k")')
        element = _selector_path(SetTypeRef(I32), "enter_set_element", 5)
        self.assertEqual(str(element.components[0]), "{5}")
        self.assertEqual(repr(element.components[0]), "SetElement(5)")

    def test_structured_map_key(self) -> None:
        key = any_of_i32(42)
        builder = PathBuilder(MapTypeRef(ANY, STRING))
        with builder.enter_map_key(key):
            encoded = thrift_python.serialize(key, thrift_python.Protocol.JSON)
            self.assertEqual(
                builder.to_string(), f"map<any, string>{{{encoded.decode()}}}"
            )
            parsed = PathBuilder.from_string(
                TYPE_SYSTEM, MapTypeRef(ANY, STRING), builder.to_string()
            )
            self.assertEqual(parsed.path(), builder.path())

    def test_dynamic_value_selectors_are_copied(self) -> None:
        selector = MutableDynamicValue(ListTypeRef(I32), [1])
        path = _selector_path(
            SetTypeRef(ListTypeRef(I32)), "enter_set_element", selector
        )
        element = SetElement(selector)
        selector.as_list().append(2)
        for component in (path.components[0], element):
            with self.subTest(component=component):
                assert isinstance(component, SetElement)
                self.assertEqual(
                    component.value, MutableDynamicValue(ListTypeRef(I32), [1])
                )
                self.assertEqual(str(component), "{[1]}")


class BuilderSelectorErrorTest(unittest.TestCase):
    def test_invalid_set_access(self) -> None:
        with self.assertRaisesRegex(
            InvalidPathAccessError,
            "cannot access set element on non-set type 'MyStruct'",
        ):
            PathBuilder(MY_STRUCT).enter_set_element(42)

    def test_invalid_map_access(self) -> None:
        builder = PathBuilder(MY_STRUCT)
        with self.assertRaisesRegex(
            InvalidPathAccessError, "cannot access map key on non-map type 'MyStruct'"
        ):
            builder.enter_map_key("key")
        with self.assertRaisesRegex(
            InvalidPathAccessError, "cannot access map value on non-map type 'MyStruct'"
        ):
            builder.enter_map_value("key")

    def test_rejects_mismatched_selectors(self) -> None:
        builder = PathBuilder(MapTypeRef(I32, STRING))
        with self.assertRaises(InvalidPathAccessError):
            builder.enter_map_value(MutableDynamicValue(STRING, "42"))
        with self.assertRaises(InvalidPathAccessError):
            builder.enter_map_value(MutableDynamicValue(I64, 42))
        with self.assertRaises(InvalidPathAccessError):
            PathBuilder(MapTypeRef(STRING, STRING)).enter_map_value(42)
        self.assertEqual(builder.to_string(), "map<i32, string>")

    def test_rejects_selectors_simple_json_cannot_write(self) -> None:
        # JSON object keys are strings, so a map with list keys has no
        # SimpleJSON form.
        builder = PathBuilder(SetTypeRef(MapTypeRef(ListTypeRef(I32), I32)))
        with self.assertRaisesRegex(InvalidPathAccessError, "map key"):
            builder.enter_set_element({(1,): 2})
        self.assertEqual(builder.to_string(), "set<map<list<i32>, i32>>")


class SelectorConversionTest(unittest.TestCase):
    def assert_accepts(
        self, element_type: TypeRef, selector: object, text: str
    ) -> None:
        path = _selector_path(SetTypeRef(element_type), "enter_set_element", selector)
        self.assertEqual(
            path.to_string(), f"set<{type_display_name(element_type)}>{text}"
        )

    def assert_rejects(self, element_type: TypeRef, selector: object) -> None:
        with self.assertRaises(InvalidPathAccessError):
            _selector_path(SetTypeRef(element_type), "enter_set_element", selector)

    def test_integer_ranges(self) -> None:
        for type_ref, bits in ((BYTE, 8), (I16, 16), (I32, 32), (I64, 64)):
            low, high = -(2 ** (bits - 1)), 2 ** (bits - 1) - 1
            with self.subTest(type_ref=type_ref):
                self.assert_accepts(type_ref, low, f"{{{low}}}")
                self.assert_accepts(type_ref, high, f"{{{high}}}")
                self.assert_rejects(type_ref, low - 1)
                self.assert_rejects(type_ref, high + 1)

    def test_bools_and_ints_do_not_mix(self) -> None:
        self.assert_accepts(BOOL, True, "{true}")
        self.assert_accepts(BOOL, False, "{false}")
        self.assert_rejects(BOOL, 1)
        self.assert_rejects(I32, True)
        self.assert_rejects(I32, 1.0)
        self.assert_rejects(I32, "1")

    def test_enums(self) -> None:
        self.assert_accepts(COLOR, 1, "{1}")
        self.assert_accepts(COLOR, -(2**31), "{-2147483648}")
        self.assert_rejects(COLOR, 2**31)
        self.assert_rejects(COLOR, "RED")

    def test_int_subclasses_are_normalized(self) -> None:
        # A generated enum member is an int whose str() is "Color.BLUE".
        self.assertIsInstance(fixtures.Color.BLUE, int)
        for type_ref in (COLOR, I32):
            with self.subTest(type_ref=type_ref):
                path = _selector_path(
                    SetTypeRef(type_ref), "enter_set_element", fixtures.Color.BLUE
                )
                self.assertEqual(str(path.components[0]), "{2}")
                element = path.components[0]
                assert isinstance(element, SetElement)
                self.assertIs(type(element.value.as_int()), int)
                typed = MutableDynamicValue(type_ref, fixtures.Color.BLUE)
                path = _selector_path(SetTypeRef(type_ref), "enter_set_element", typed)
                self.assertEqual(str(path.components[0]), "{2}")
                nested = _selector_path(
                    SetTypeRef(ListTypeRef(type_ref)),
                    "enter_set_element",
                    [fixtures.Color.BLUE],
                )
                self.assertEqual(str(nested.components[0]), "{[2]}")

    def test_floats(self) -> None:
        self.assert_accepts(FLOAT, 1.5, "{1.5}")
        self.assert_accepts(FLOAT, _f32(0.1), "{0.1}")
        self.assert_accepts(FLOAT, math.inf, '{"Infinity"}')
        self.assert_rejects(FLOAT, 0.1)
        self.assert_rejects(FLOAT, 1e39)
        self.assert_rejects(FLOAT, 1)
        self.assert_rejects(DOUBLE, 1)

    def test_strings_and_binary(self) -> None:
        self.assert_accepts(STRING, "", '{""}')
        self.assert_accepts(BINARY, b"", '{""}')
        self.assert_rejects(STRING, "\ud800")
        self.assert_rejects(STRING, b"bytes")
        self.assert_rejects(BINARY, bytearray(b"ab"))
        self.assert_rejects(BINARY, "text")

    def test_dynamic_values(self) -> None:
        self.assert_accepts(I16, MutableDynamicValue(I16, 7), "{7}")
        self.assert_accepts(
            STRING, MutableDynamicValue(STRING, "x").read_only(), '{"x"}'
        )
        self.assert_accepts(COLOR, MutableDynamicValue(COLOR, 2), "{2}")
        self.assert_rejects(I16, MutableDynamicValue(I32, 7))
        self.assert_rejects(USER_ID, MutableDynamicValue(I64, 7))
        self.assert_rejects(I64, MutableDynamicValue(USER_ID, 7))


# (container, builder method, selector, path string as C++ writes it)
SELECTOR_GOLDENS: list[tuple[TypeRef, str, object, str]] = [
    (
        MapTypeRef(STRING, I32),
        "enter_map_value",
        'q"b\\s\n\t\x01\x1f\x7f é☃😀',
        'map<string, i32>["q\\"b\\\\s\\n\\t\\u0001\\u001f\x7f é☃😀"]',
    ),
    (
        SetTypeRef(BINARY),
        "enter_set_element",
        b"\x00\xff\x80bin",
        'set<binary>{"AP+AYmlu"}',
    ),
    (SetTypeRef(BINARY), "enter_set_element", b"a", 'set<binary>{"YQ"}'),
    (
        MapTypeRef(FLOAT, STRING),
        "enter_map_key",
        _f32(3.1415927),
        "map<float, string>{3.1415927}",
    ),
    (
        MapTypeRef(FLOAT, STRING),
        "enter_map_value",
        _f32(16777216.0),
        "map<float, string>[16777216]",
    ),
    (
        MapTypeRef(DOUBLE, STRING),
        "enter_map_value",
        -0.0,
        "map<double, string>[-0]",
    ),
    (
        MapTypeRef(DOUBLE, STRING),
        "enter_map_value",
        0.1,
        "map<double, string>[0.1]",
    ),
    (
        MapTypeRef(DOUBLE, STRING),
        "enter_map_value",
        1e16,
        "map<double, string>[1e+16]",
    ),
    (
        MapTypeRef(DOUBLE, STRING),
        "enter_map_value",
        math.nan,
        'map<double, string>["NaN"]',
    ),
    (
        MapTypeRef(DOUBLE, STRING),
        "enter_map_value",
        math.inf,
        'map<double, string>["Infinity"]',
    ),
    (
        MapTypeRef(DOUBLE, STRING),
        "enter_map_value",
        -math.inf,
        'map<double, string>["-Infinity"]',
    ),
    (MapTypeRef(COLOR, STRING), "enter_map_value", 1, "map<Color, string>[1]"),
    (
        MapTypeRef(I64, STRING),
        "enter_map_key",
        -(2**63),
        "map<i64, string>{-9223372036854775808}",
    ),
    (MapTypeRef(USER_ID, STRING), "enter_map_key", 7, "map<UserId, string>{7}"),
    (
        SetTypeRef(ROOT),
        "enter_set_element",
        [[[0]]],
        'set<Root>{{"a":{"b":{"c":0}}}}',
    ),
    (
        SetTypeRef(USER_PROFILE),
        "enter_set_element",
        ["Ann", [1], {2: None}, UNSET],
        'set<UserProfile>{{"name":"Ann","scores":[1],"tags":[2]}}',
    ),
    (SetTypeRef(CHOICE), "enter_set_element", [], "set<Choice>{{}}"),
    (
        MapTypeRef(CHOICE, I32),
        "enter_map_value",
        [2, "t"],
        'map<Choice, i32>[{"text":"t"}]',
    ),
    (
        SetTypeRef(ListTypeRef(DOUBLE)),
        "enter_set_element",
        [math.nan, -0.0, 1e16],
        'set<list<double>>{["NaN",-0,1e+16]}',
    ),
    (
        MapTypeRef(SetTypeRef(I32), STRING),
        "enter_map_key",
        {3: None},
        "map<set<i32>, string>{[3]}",
    ),
    (
        MapTypeRef(MapTypeRef(I32, STRING), I32),
        "enter_map_value",
        {1: "one"},
        'map<map<i32, string>, i32>[{"1":"one"}]',
    ),
]


class SelectorEncodingTest(unittest.TestCase):
    """Selector text as C++ `SimpleJSONProtocolWriter` writes it in value
    position (`test_simple_json_protocol.py` pins the scalar formatting)."""

    def test_goldens(self) -> None:
        for container, enter, selector, expected in SELECTOR_GOLDENS:
            with self.subTest(expected=expected):
                self.assertEqual(
                    _selector_path(container, enter, selector).to_string(), expected
                )

    def test_opaque_alias_selectors_keep_the_alias_type(self) -> None:
        key = _selector_path(
            MapTypeRef(USER_ID, STRING), "enter_map_key", 7
        ).components[0]
        assert isinstance(key, MapKey)
        self.assertEqual(key.key.type_ref, USER_ID)
        self.assertEqual(key.key.as_int(), 7)


class SelectorEqualityTest(unittest.TestCase):
    def test_nan_selectors_are_equal(self) -> None:
        first, second = float("nan"), float("nan")
        self.assertIsNot(first, second)
        container = MapTypeRef(DOUBLE, STRING)
        path = _selector_path(container, "enter_map_value", first)
        other = _selector_path(container, "enter_map_value", second)
        self.assertEqual(path, other)
        self.assertEqual(hash(path), hash(other))

    def test_signed_zeros_are_unequal(self) -> None:
        container = SetTypeRef(DOUBLE)
        self.assertNotEqual(
            _selector_path(container, "enter_set_element", -0.0),
            _selector_path(container, "enter_set_element", 0.0),
        )

    def test_selectors_compare_by_kind_type_and_text(self) -> None:
        container = MapTypeRef(I32, STRING)
        value = _selector_path(container, "enter_map_value", 1)
        self.assertEqual(value, _selector_path(container, "enter_map_value", 1))
        self.assertNotEqual(
            value.components[0],
            _selector_path(container, "enter_map_key", 1).components[0],
        )
        self.assertNotEqual(value, _selector_path(container, "enter_map_value", 2))
        self.assertNotEqual(
            _selector_path(SetTypeRef(I32), "enter_set_element", 1).components[0],
            _selector_path(SetTypeRef(I64), "enter_set_element", 1).components[0],
        )

    def test_equal_across_separately_built_type_systems(self) -> None:
        copy = _build_type_system()

        def build(type_system: EnumerableTypeSystem) -> Path:
            builder = PathBuilder(_user_type(MY_STRUCT_URI, type_system))
            builder.enter_field("users")
            builder.enter_map_value("alice")
            builder.enter_field("tags")
            builder.enter_set_element(3)
            return builder.path()

        self.assertEqual(build(TYPE_SYSTEM), build(copy))
        self.assertEqual(hash(build(TYPE_SYSTEM)), hash(build(copy)))
        color_key = _selector_path(MapTypeRef(COLOR, STRING), "enter_map_key", 1)
        copied_color = _user_type(COLOR_URI, copy)
        self.assertEqual(
            color_key,
            _selector_path(MapTypeRef(copied_color, STRING), "enter_map_key", 1),
        )


class ParseTest(unittest.TestCase):
    def assert_round_trip(
        self, root: TypeRef, text: str, final_type: TypeRef, component_count: int
    ) -> None:
        with self.subTest(text=text):
            builder = PathBuilder.from_string(TYPE_SYSTEM, root, text)
            self.assertEqual(builder.to_string(), text)
            self.assertEqual(builder.current_type, final_type)
            self.assertEqual(len(builder.path()), component_count)

    def assert_invalid_path(self, root: TypeRef, text: str) -> None:
        with self.subTest(text=text), self.assertRaises(InvalidPathAccessError):
            PathBuilder.from_string(TYPE_SYSTEM, root, text)

    def assert_invalid_type(self, root: TypeRef, text: str) -> None:
        with self.subTest(text=text), self.assertRaises(InvalidTypeError):
            PathBuilder.from_string(TYPE_SYSTEM, root, text)

    def test_primitive_roots_round_trip(self) -> None:
        for type_ref in (BOOL, BYTE, I16, I32, I64, FLOAT, DOUBLE, STRING, BINARY, ANY):
            self.assert_round_trip(type_ref, type_display_name(type_ref), type_ref, 0)

    def test_every_component_type_round_trips(self) -> None:
        self.assert_round_trip(MY_STRUCT, "MyStruct", MY_STRUCT, 0)
        users_type = _field_type(MY_STRUCT, "users")
        self.assert_round_trip(MY_STRUCT, "MyStruct.users", users_type, 1)
        self.assert_round_trip(MY_STRUCT, 'MyStruct.users["alice"]', USER_PROFILE, 2)
        self.assert_round_trip(MY_STRUCT, 'MyStruct.users{"alice"}', STRING, 2)
        self.assert_round_trip(MY_STRUCT, 'MyStruct.users["alice"].scores[0]', I32, 4)
        self.assert_round_trip(MY_STRUCT, 'MyStruct.users["alice"].tags{-42}', I32, 4)
        self.assert_round_trip(
            MY_STRUCT,
            'MyStruct.users["alice"].metadata[meta.com/thrift/test/UserProfile].name',
            STRING,
            5,
        )
        self.assert_round_trip(CHOICE, "Choice.profile.tags{1}", I32, 3)

    def test_populates_components_with_an_optional_root_type_name(self) -> None:
        components = (
            '.users["alice"].metadata[meta.com/thrift/test/UserProfile].scores[2]'
        )
        for text in (f"MyStruct{components}", components):
            with self.subTest(text=text):
                builder = PathBuilder.from_string(TYPE_SYSTEM, MY_STRUCT, text)
                path = builder.path()
                self.assertEqual(path.root_type, MY_STRUCT)
                self.assertEqual(builder.current_type, I32)
                match path.components:
                    case (
                        FieldAccess(users_type, users),
                        MapValue(user),
                        FieldAccess(metadata_type, metadata),
                        AnyType(known_type),
                        FieldAccess(scores_type, scores),
                        ListElement(2),
                    ):
                        self.assertEqual(users_type, MY_STRUCT)
                        self.assertEqual(users.identity, FieldIdentity(1, "users"))
                        self.assertEqual(user, MutableDynamicValue(STRING, "alice"))
                        self.assertEqual(metadata_type, USER_PROFILE)
                        self.assertEqual(
                            metadata.identity, FieldIdentity(4, "metadata")
                        )
                        self.assertEqual(known_type, USER_PROFILE)
                        self.assertEqual(scores_type, USER_PROFILE)
                        self.assertEqual(scores.identity, FieldIdentity(2, "scores"))
                    case _:
                        self.fail(f"unexpected components {path.components!r}")

    def test_the_root_type_name_is_skipped_unchecked(self) -> None:
        builder = PathBuilder.from_string(TYPE_SYSTEM, MY_STRUCT, "Other.users")
        self.assertEqual(builder.to_string(), "MyStruct.users")

    def test_uri_starting_with_a_primitive_name_round_trips(self) -> None:
        self.assert_round_trip(BOOL_PREFIX, "boolRecord.value", I32, 1)
        self.assert_round_trip(
            ANY, "any[bool.example/thrift/test/boolRecord].value", I32, 2
        )

    def test_opaque_alias_container_traversal_round_trips(self) -> None:
        self.assert_round_trip(LIST_ALIAS, "IntList[2]", I32, 1)
        self.assert_round_trip(SET_ALIAS, "IntSet{3}", I32, 1)
        self.assert_round_trip(MAP_ALIAS, "IntMap[4]", STRING, 1)
        self.assert_round_trip(EXTRAS, "Extras.aliased[0]", I32, 2)

    def test_container_roots_round_trip(self) -> None:
        self.assert_round_trip(ListTypeRef(I32), "list<i32>[42]", I32, 1)
        self.assert_round_trip(SetTypeRef(STRING), 'set<string>{"hello"}', STRING, 1)
        self.assert_round_trip(MapTypeRef(I32, STRING), "map<i32, string>{-42}", I32, 1)
        self.assert_round_trip(
            MapTypeRef(I32, STRING), "map<i32, string>[42]", STRING, 1
        )

    def test_nested_container_type_names_round_trip(self) -> None:
        root = MapTypeRef(STRING, ListTypeRef(SetTypeRef(I64)))
        self.assert_round_trip(
            root, 'map<string, list<set<i64>>>["key"][3]{-1}', I64, 3
        )

    def test_selectors_with_path_delimiters_round_trip(self) -> None:
        self.assert_round_trip(
            MapTypeRef(STRING, I32), 'map<string, i32>["a.b[0]{\\"x\\"}"]', I32, 1
        )

    def test_any_type_names_round_trip(self) -> None:
        self.assert_round_trip(ANY, "any[i32]", I32, 1)
        self.assert_round_trip(
            ANY,
            "any[map<string, list<meta.com/thrift/test/MyStruct>>]",
            MapTypeRef(STRING, ListTypeRef(MY_STRUCT)),
            1,
        )
        self.assert_round_trip(ListTypeRef(ANY), "list<any>[1][set<i32>]{2}", I32, 3)

    def test_list_indices(self) -> None:
        self.assert_round_trip(
            ListTypeRef(I32), "list<i32>[18446744073709551615]", I32, 1
        )
        builder = PathBuilder.from_string(
            TYPE_SYSTEM, ListTypeRef(I32), "list<i32>[007]"
        )
        self.assertEqual(builder.to_string(), "list<i32>[7]")
        self.assert_invalid_path(ListTypeRef(I32), "list<i32>[18446744073709551616]")
        # Longer than int() parses by default.
        padded = f"list<i32>[{'0' * 5000}1]"
        builder = PathBuilder.from_string(TYPE_SYSTEM, ListTypeRef(I32), padded)
        self.assertEqual(builder.to_string(), "list<i32>[1]")
        self.assert_invalid_path(ListTypeRef(I32), f"list<i32>[{'1' * 5000}]")

    def test_rejects_invalid_structured_traversal(self) -> None:
        self.assert_invalid_path(MY_STRUCT, "MyStruct.")
        self.assert_invalid_path(MY_STRUCT, "MyStruct.unknown")
        self.assert_invalid_path(MY_STRUCT, "MyStruct.users.")
        self.assert_invalid_path(MY_STRUCT, 'MyStruct.users["alice"]name')
        self.assert_invalid_path(MY_STRUCT, "MyStruct.users-name")

    def test_rejects_invalid_list_traversal(self) -> None:
        root = ListTypeRef(I32)
        self.assert_invalid_path(root, "list<i32>[]")
        self.assert_invalid_path(root, "list<i32>[-1]")
        self.assert_invalid_path(root, "list<i32>[abc]")
        self.assert_invalid_path(root, "list<i32>[1")
        self.assert_invalid_path(root, "list<i32>[1]trailing")
        self.assert_invalid_path(root, "list<i32>[184467440737095516160]")

    def test_rejects_invalid_set_traversal(self) -> None:
        root = SetTypeRef(STRING)
        self.assert_invalid_path(root, 'set<string>["value"]')
        self.assert_invalid_path(root, 'set<string>{"value"]')
        self.assert_invalid_path(root, "set<string>{not-json}")
        self.assert_invalid_path(root, 'set<string>{"value')
        self.assert_invalid_path(root, 'set<string>{"value"}trailing')

    def test_rejects_invalid_map_traversal(self) -> None:
        root = MapTypeRef(I32, STRING)
        self.assert_invalid_path(root, "map<i32, string>(42)")
        self.assert_invalid_path(root, 'map<i32, string>{"wrong type"}')
        self.assert_invalid_path(root, "map<i32, string>[42")
        self.assert_invalid_path(root, "map<i32, string>{42]")
        self.assert_invalid_path(root, "map<i32, string>[42]trailing")

    def test_rejects_malformed_container_type_names(self) -> None:
        self.assert_invalid_path(ListTypeRef(I32), "list<i32[0]")
        self.assert_invalid_path(ListTypeRef(I32), "list<list<i32>[0]")
        self.assert_invalid_path(MapTypeRef(I32, STRING), "map<i32 string>[42]")
        self.assert_invalid_path(MapTypeRef(I32, STRING), "map<i32, string[42]")

    def test_rejects_invalid_any_traversal(self) -> None:
        self.assert_invalid_path(ANY, "any[]")
        self.assert_invalid_type(ANY, "any[unknown.example/Type]")
        self.assert_invalid_type(ANY, "any[list<unknown.example/Type>]")
        self.assert_invalid_type(ANY, "any[MyStruct]")
        self.assert_invalid_path(ANY, "any[meta.com/thrift/test/UserProfile")
        self.assert_invalid_path(ANY, "any[list<i32,junk>]")
        self.assert_invalid_path(ANY, "any[meta.com/thrift/test/UserProfile]trailing")

    def test_rejects_type_names_nested_too_deeply(self) -> None:
        # Each level of nesting takes at least one stack frame to parse.
        depth = sys.getrecursionlimit()
        nested = "list<" * depth + "i32" + ">" * depth
        self.assert_invalid_path(ANY, f"any[{nested}]")
        self.assert_invalid_path(ListTypeRef(I32), f"{nested}[0]")

    def test_rejects_traversal_from_primitive_types(self) -> None:
        self.assert_invalid_path(BOOL, "bool.value")
        self.assert_invalid_path(I32, "i32[0]")
        self.assert_invalid_path(STRING, 'string{"value"}')

    def test_errors_show_the_unparsed_rest(self) -> None:
        with self.assertRaisesRegex(
            InvalidPathAccessError, r"^Invalid list traversal: abc\]$"
        ):
            PathBuilder.from_string(TYPE_SYSTEM, ListTypeRef(I32), "list<i32>[abc]")


class ParseSelectorTest(unittest.TestCase):
    def parsed(self, root: TypeRef, text: str) -> Path:
        return PathBuilder.from_string(TYPE_SYSTEM, root, text).path()

    def selector_value(self, root: TypeRef, text: str) -> DynamicValue:
        match self.parsed(root, text).components:
            case (SetElement(value) | MapKey(value) | MapValue(value),):
                return value
            case components:
                raise AssertionError(f"unexpected components {components!r}")

    def test_selector_goldens_round_trip(self) -> None:
        for container, enter, selector, expected in SELECTOR_GOLDENS:
            with self.subTest(expected=expected):
                parsed = self.parsed(container, expected)
                self.assertEqual(parsed.to_string(), expected)
                built = _selector_path(container, enter, selector)
                self.assertEqual(parsed, built)
                self.assertEqual(hash(parsed), hash(built))

    def test_decodes_scalars(self) -> None:
        cases = [
            (MapTypeRef(STRING, I32), '["\\ud83d\\ude00"]', "😀"),
            (MapTypeRef(STRING, I32), '["\\u0041\\/"]', "A/"),
            (SetTypeRef(STRING), '{"a\tb"}', "a\tb"),
            (SetTypeRef(BINARY), '{"YQ=="}', b"a"),
            (SetTypeRef(BINARY), '{"+/8"}', b"\xfb\xff"),
            (SetTypeRef(BOOL), "{false}", False),
            (SetTypeRef(BYTE), "{-128}", -128),
            (SetTypeRef(FLOAT), "{0.1}", _f32(0.1)),
            (SetTypeRef(FLOAT), "{1e-45}", _f32(1.401298464324817e-45)),
            (SetTypeRef(FLOAT), '{"-Infinity"}', -math.inf),
            (SetTypeRef(DOUBLE), "{1.50e+3}", 1500.0),
            (SetTypeRef(COLOR), "{2}", 2),
            (MapTypeRef(USER_ID, STRING), "{7}", 7),
        ]
        for root, text, expected in cases:
            with self.subTest(text=text):
                value = self.selector_value(root, f"{type_display_name(root)}{text}")
                self.assertEqual(value.to_datum(), expected)

    def test_special_floats(self) -> None:
        value = self.selector_value(SetTypeRef(DOUBLE), 'set<double>{"NaN"}')
        self.assertTrue(math.isnan(value.as_float()))
        value = self.selector_value(SetTypeRef(DOUBLE), "set<double>{-0}")
        self.assertEqual(math.copysign(1.0, value.as_float()), -1.0)

    def test_canonicalizes_the_printed_form(self) -> None:
        cases = [
            (MapTypeRef(STRING, I32), '["\\u0041"]', '["A"]'),
            (SetTypeRef(BINARY), '{"YQ=="}', '{"YQ"}'),
            (SetTypeRef(DOUBLE), "{1.50}", "{1.5}"),
            (SetTypeRef(DOUBLE), '{"-NaN"}', '{"NaN"}'),
            (SetTypeRef(STRING), '{"a\tb"}', '{"a\\tb"}'),
            (SetTypeRef(ListTypeRef(DOUBLE)), "{[1.50]}", "{[1.5]}"),
            # Fields in declared order, defaults filled in, and unknown and
            # null fields skipped.
            (
                SetTypeRef(USER_PROFILE),
                '{{"tags":[2],"metadata":null,"name":"Ann"}}',
                '{{"name":"Ann","scores":[],"tags":[2]}}',
            ),
            (SetTypeRef(INNER2), '{{"z":{"y":[1]},"c":3}}', '{{"c":3}}'),
        ]
        for root, text, printed in cases:
            name = type_display_name(root)
            with self.subTest(text=text):
                self.assertEqual(
                    self.parsed(root, f"{name}{text}").to_string(), f"{name}{printed}"
                )

    def test_rejects_invalid_selectors(self) -> None:
        cases = [
            (MapTypeRef(STRING, I32), '["\\ud800"]'),
            (MapTypeRef(STRING, I32), '["\ud800"]'),
            (SetTypeRef(DOUBLE), "{NaN}"),
            (SetTypeRef(DOUBLE), '{"1.5"}'),
            (SetTypeRef(BYTE), "{128}"),
            (SetTypeRef(I32), "{1.0}"),
            (SetTypeRef(I32), '{"1"}'),
            (SetTypeRef(I32), "{true}"),
            (SetTypeRef(BOOL), "{1}"),
            (SetTypeRef(BINARY), '{"not base64!"}'),
            (SetTypeRef(STRING), "{null}"),
            (SetTypeRef(STRING), '{["a"]}'),
            (SetTypeRef(I32), "{ 1}"),
            (SetTypeRef(I32), "{1 }"),
            (SetTypeRef(I32), "{" + "[" * 100000 + "}"),
            (SetTypeRef(ROOT), '{{"a":{"b":{"c":"x"}}}}'),
            (SetTypeRef(ROOT), "{[1]}"),
            (SetTypeRef(CHOICE), '{{"number":1,"text":"t"}}'),
            (SetTypeRef(ListTypeRef(I32)), "{[1,]}"),
            (SetTypeRef(ListTypeRef(I32)), '{["1"]}'),
            (SetTypeRef(MapTypeRef(ListTypeRef(I32), I32)), '{{"[1]":2}}'),
        ]
        for root, text in cases:
            with self.subTest(text=text[:20]):
                with self.assertRaises(InvalidPathAccessError):
                    self.parsed(root, f"{type_display_name(root)}{text}")

    def test_a_parsed_nan_path_equals_the_built_one(self) -> None:
        built = _selector_path(
            MapTypeRef(DOUBLE, STRING), "enter_map_value", float("nan")
        )
        parsed = self.parsed(MapTypeRef(DOUBLE, STRING), built.to_string())
        self.assertEqual(parsed, built)
        self.assertEqual(hash(parsed), hash(built))

    def test_a_parsed_path_equals_the_built_one(self) -> None:
        builder = PathBuilder(MY_STRUCT)
        builder.enter_field("users")
        builder.enter_map_value("alice")
        builder.enter_field("metadata")
        builder.enter_any_type(MapTypeRef(COLOR, ListTypeRef(FAILURE)))
        builder.enter_map_value(2)
        builder.enter_list_element(1)
        builder.enter_field("reason")
        built = builder.path()
        text = built.to_string()
        self.assertEqual(
            text,
            'MyStruct.users["alice"].metadata'
            "[map<meta.com/thrift/test/Color, list<meta.com/thrift/test/Failure>>]"
            "[2][1].reason",
        )
        parsed = self.parsed(MY_STRUCT, text)
        self.assertEqual(parsed, built)
        self.assertEqual(hash(parsed), hash(built))
        copy = _build_type_system()
        self.assertEqual(
            PathBuilder.from_string(copy, _user_type(MY_STRUCT_URI, copy), text).path(),
            built,
        )


def _profile(name: str, scores: list[int], tags: list[int]) -> list[object]:
    return [name, scores, dict.fromkeys(tags), UNSET]


def _my_struct() -> MutableDynamicValue:
    users = {
        "alice": _profile("Alice", [10, 20, 30], [1, 2]),
        "bob": _profile("Bob", [], []),
    }
    return MutableDynamicValue(MY_STRUCT, [users, {1: "one", -2: "minus two"}])


def _extras(choice: list[object]) -> MutableDynamicValue:
    return MutableDynamicValue(
        EXTRAS,
        [
            choice,
            UNSET,
            {0: "red", 2: "blue"},
            {1: None},
            [["bad", 3]],
            ["why", 500],
            [7, 8],
        ],
    )


def _parse(root: TypeRef, text: str) -> Path:
    return PathBuilder.from_string(TYPE_SYSTEM, root, text).path()


class TraverseTest(unittest.TestCase):
    def assert_resolves(
        self, root: DynamicValue, text: str, expected: DynamicValue | None
    ) -> None:
        with self.subTest(text=text):
            self.assertEqual(_parse(root.type_ref, text).traverse(root), expected)

    def test_an_empty_path_returns_the_root(self) -> None:
        value = _my_struct()
        self.assertIs(PathBuilder(MY_STRUCT).path().traverse(value), value)

    def test_nested_struct_fields(self) -> None:
        root = MutableDynamicValue(ROOT, [[[42]]])
        self.assert_resolves(root, "Root.a.b.c", MutableDynamicValue(I32, 42))
        self.assert_resolves(root, "Root.a.b", MutableDynamicValue(INNER2, [42]))

    def test_map_values(self) -> None:
        root = _my_struct().read_only()
        self.assert_resolves(
            root, 'MyStruct.users["alice"].name', MutableDynamicValue(STRING, "Alice")
        )
        self.assert_resolves(
            root, "MyStruct.counts[-2]", MutableDynamicValue(STRING, "minus two")
        )
        self.assert_resolves(root, 'MyStruct.users["carol"]', None)
        self.assert_resolves(root, 'MyStruct.users["carol"].name', None)
        self.assert_resolves(root, "MyStruct.counts[3]", None)

    def test_list_elements(self) -> None:
        root = _my_struct().read_only()
        self.assert_resolves(
            root, 'MyStruct.users["alice"].scores[1]', MutableDynamicValue(I32, 20)
        )
        self.assert_resolves(root, 'MyStruct.users["alice"].scores[3]', None)
        self.assert_resolves(root, 'MyStruct.users["bob"].scores[0]', None)
        self.assert_resolves(
            root, 'MyStruct.users["alice"].scores[18446744073709551615]', None
        )

    def test_set_elements_and_map_keys(self) -> None:
        root = _my_struct().read_only()
        self.assert_resolves(
            root, 'MyStruct.users["alice"].tags{2}', MutableDynamicValue(I32, 2)
        )
        self.assert_resolves(root, 'MyStruct.users["alice"].tags{9}', None)
        self.assert_resolves(
            root, 'MyStruct.users{"alice"}', MutableDynamicValue(STRING, "alice")
        )
        self.assert_resolves(root, 'MyStruct.users{"carol"}', None)

    def test_structured_selectors(self) -> None:
        roots = MutableDynamicValue(SetTypeRef(ROOT), {(((1,),),): None}).read_only()
        self.assert_resolves(
            roots, 'set<Root>{{"a":{"b":{"c":1}}}}', MutableDynamicValue(ROOT, [[[1]]])
        )
        self.assert_resolves(roots, 'set<Root>{{"a":{"b":{"c":2}}}}', None)
        pairs = MutableDynamicValue(
            MapTypeRef(ListTypeRef(I32), STRING), {(1, 2): "pair"}
        )
        self.assert_resolves(
            pairs, "map<list<i32>, string>[[1,2]]", MutableDynamicValue(STRING, "pair")
        )
        self.assert_resolves(pairs, "map<list<i32>, string>[[2,1]]", None)
        self.assert_resolves(
            pairs.read_only(),
            "map<list<i32>, string>{[1,2]}",
            MutableDynamicValue(ListTypeRef(I32), [1, 2]),
        )

    def test_results_have_the_declared_child_types(self) -> None:
        root = _my_struct().read_only()
        user = _parse(MY_STRUCT, 'MyStruct.users["alice"]').traverse(root)
        assert user is not None
        self.assertEqual(user.type_ref, USER_PROFILE)
        self.assertEqual(user.as_struct()["name"].as_str(), "Alice")

    def test_optional_fields(self) -> None:
        root = _extras([]).read_only()
        self.assert_resolves(root, "Extras.maybe", None)
        mutable = _extras([])
        mutable.as_struct().set_field("maybe", 4)
        self.assert_resolves(mutable, "Extras.maybe", MutableDynamicValue(I32, 4))

    def test_unions(self) -> None:
        number = _extras([1, 5]).read_only()
        self.assert_resolves(
            number, "Extras.choice.number", MutableDynamicValue(I32, 5)
        )
        self.assert_resolves(number, "Extras.choice.text", None)
        self.assert_resolves(_extras([]), "Extras.choice.number", None)
        profile = MutableDynamicValue(CHOICE, [3, _profile("Carol", [7], [])])
        self.assert_resolves(
            profile, "Choice.profile.scores[0]", MutableDynamicValue(I32, 7)
        )
        self.assert_resolves(profile, "Choice.number", None)

    def test_exception_fields(self) -> None:
        root = _extras([]).read_only()
        self.assert_resolves(root, "Extras.failure.code", MutableDynamicValue(I32, 500))
        self.assert_resolves(
            root, "Extras.failures[0].reason", MutableDynamicValue(STRING, "bad")
        )

    def test_enum_keyed_maps_and_sets(self) -> None:
        root = _extras([]).read_only()
        self.assert_resolves(
            root, "Extras.labels[2]", MutableDynamicValue(STRING, "blue")
        )
        self.assert_resolves(root, "Extras.labels[1]", None)
        self.assert_resolves(root, "Extras.labels{0}", MutableDynamicValue(COLOR, 0))
        self.assert_resolves(root, "Extras.colors{1}", MutableDynamicValue(COLOR, 1))
        self.assert_resolves(root, "Extras.colors{2}", None)

    def test_opaque_alias_values_are_unwrapped(self) -> None:
        self.assert_resolves(
            MutableDynamicValue(LIST_ALIAS, [1, 2, 3]),
            "IntList[2]",
            MutableDynamicValue(I32, 3),
        )
        int_map = MutableDynamicValue(MAP_ALIAS, {4: "four"}).read_only()
        self.assert_resolves(int_map, "IntMap[4]", MutableDynamicValue(STRING, "four"))
        self.assert_resolves(int_map, "IntMap{4}", MutableDynamicValue(I32, 4))
        int_set = MutableDynamicValue(SET_ALIAS, {3: None}).read_only()
        self.assert_resolves(int_set, "IntSet{3}", MutableDynamicValue(I32, 3))
        self.assert_resolves(
            _extras([]), "Extras.aliased[1]", MutableDynamicValue(I32, 8)
        )
        by_user = MutableDynamicValue(MapTypeRef(USER_ID, STRING), {7: "seven"})
        self.assert_resolves(
            by_user, "map<UserId, string>[7]", MutableDynamicValue(STRING, "seven")
        )

    def test_through_the_schema_registry(self) -> None:
        containers_type = struct_type_of(fixtures.Containers)
        root = MutableDynamicValue(
            containers_type, to_value(containers(), containers_type)
        ).read_only()
        cases: list[tuple[str, object]] = [
            ('Containers.counts["b"]', -(2**63)),
            ('Containers.names{"mu"}', "mu"),
            ("Containers.lists_by_id[7][1]", "y"),
            ('Containers.deep[0]["k"]{-2}', -2),
            ("Containers.weights[1]", -2.0),
            ("Containers.choices[2].point.x", 9),
            ("Containers.choices[2].number", None),
            ("Containers.failures[0].code", 3),
            ("Containers.bytes_by_flag[true]", 1),
            ("Containers.names_by_weight[1.5]", "one and a half"),
            ('Containers.flags_by_blob["AQI"]', False),
            ("Containers.colors[0]", 1),
            ("Containers.ints[25]", None),
        ]
        for text, expected in cases:
            with self.subTest(text=text):
                path = PathBuilder.from_string(registry(), containers_type, text).path()
                result = path.traverse(root)
                self.assertEqual(
                    None if result is None else result.to_datum(), expected
                )

    def test_any_type_traversal_is_not_supported(self) -> None:
        root = MutableDynamicValue(
            USER_PROFILE, ["Dana", [], {}, any_of_i32(1)]
        ).read_only()
        path = _parse(USER_PROFILE, "UserProfile.metadata[i32]")
        with self.assertRaisesRegex(
            NotImplementedError, "AnyType traversal is not supported"
        ):
            path.traverse(root)
        unset = MutableDynamicValue(USER_PROFILE, ["Dana", [], {}, UNSET])
        self.assertIsNone(path.traverse(unset))


class TraverseErrorTest(unittest.TestCase):
    def assert_invalid(self, path: Path, value: DynamicValue, message: str) -> None:
        with self.subTest(path=str(path), value=value):
            with self.assertRaisesRegex(InvalidPathAccessError, message):
                path.traverse(value)

    def test_kind_mismatches(self) -> None:
        ints = MutableDynamicValue(ListTypeRef(I32), [1]).read_only()
        counts = MutableDynamicValue(MapTypeRef(I32, STRING), {1: "one"}).read_only()
        names = MutableDynamicValue(SetTypeRef(I32), {1: None}).read_only()
        self.assert_invalid(
            _parse(MY_STRUCT, "MyStruct.users"),
            ints,
            "cannot access field on non-structured type 'list<i32>'",
        )
        self.assert_invalid(
            _parse(ListTypeRef(I32), "list<i32>[0]"),
            counts,
            "cannot access list element on non-list type 'map<i32, string>'",
        )
        self.assert_invalid(
            _parse(SetTypeRef(I32), "set<i32>{1}"),
            ints,
            "cannot access set element on non-set type 'list<i32>'",
        )
        self.assert_invalid(
            _parse(MapTypeRef(I32, STRING), "map<i32, string>{1}"),
            names,
            "cannot access map key on non-map type 'set<i32>'",
        )
        self.assert_invalid(
            _parse(MapTypeRef(I32, STRING), "map<i32, string>[1]"),
            ints,
            "cannot access map value on non-map type 'list<i32>'",
        )

    def test_a_field_of_another_struct(self) -> None:
        # Both structs have an i32 field with id 1.
        value = MutableDynamicValue(BOOL_PREFIX, [1])
        self.assert_invalid(
            _parse(INNER2, "Inner2.c"), value, "field 'c' of 'Inner2' does not apply"
        )

    def test_selector_type_mismatches(self) -> None:
        counts = MutableDynamicValue(MapTypeRef(I32, STRING), {1: "one"}).read_only()
        message = "selector type 'i64' does not match container type 'i32'"
        self.assert_invalid(
            _parse(MapTypeRef(I64, STRING), "map<i64, string>[1]"), counts, message
        )
        self.assert_invalid(
            _parse(MapTypeRef(I64, STRING), "map<i64, string>{1}"), counts, message
        )
        names = MutableDynamicValue(SetTypeRef(I32), {1: None}).read_only()
        self.assert_invalid(_parse(SetTypeRef(I64), "set<i64>{1}"), names, message)
        by_user = MutableDynamicValue(MapTypeRef(USER_ID, STRING), {7: "seven"})
        self.assert_invalid(
            _parse(MapTypeRef(I64, STRING), "map<i64, string>[7]"),
            by_user,
            "selector type 'i64' does not match container type 'UserId'",
        )


class TraverseHandleTest(unittest.TestCase):
    def test_a_read_only_root_gives_read_only_results(self) -> None:
        root = _my_struct().read_only()
        for text in ('MyStruct.users["alice"]', 'MyStruct.users["alice"].scores[0]'):
            with self.subTest(text=text):
                self.assertIs(
                    type(_parse(MY_STRUCT, text).traverse(root)), DynamicValue
                )

    def test_a_mutable_root_gives_mutable_results_that_share_storage(self) -> None:
        root = _my_struct()
        scores = _parse(MY_STRUCT, 'MyStruct.users["alice"].scores').traverse(root)
        assert scores is not None
        scores.as_list().append(40)
        user = _parse(MY_STRUCT, 'MyStruct.users["alice"]').traverse(root)
        assert user is not None
        user.as_struct()["name"] = "Alicia"
        self.assertEqual(
            root.as_struct()["users"].as_map().lookup("alice"),
            MutableDynamicValue(
                USER_PROFILE, _profile("Alicia", [10, 20, 30, 40], [1, 2])
            ),
        )
        choice = _parse(EXTRAS, "Extras.choice").traverse(_extras([1, 5]))
        assert choice is not None
        self.assertIsInstance(choice, MutableDynamicValue)
        choice.as_union().set_field("text", "t")

    def test_a_mutable_root_rejects_set_elements_and_map_keys(self) -> None:
        root = _my_struct()
        cases = [
            ('MyStruct.users["alice"].tags{2}', "set element"),
            ('MyStruct.users["alice"].tags{9}', "set element"),
            ('MyStruct.users{"alice"}', "map key"),
            ('MyStruct.users{"carol"}', "map key"),
        ]
        for text, what in cases:
            with self.subTest(text=text):
                path = _parse(MY_STRUCT, text)
                with self.assertRaisesRegex(
                    InvalidPathAccessError, f"cannot get mutable reference to {what}"
                ):
                    path.traverse(root)
                result = path.traverse(root.read_only())
                self.assertIn(type(result), (DynamicValue, type(None)))

    def test_an_absent_component_ends_traversal_first(self) -> None:
        root = _my_struct()
        self.assertIsNone(
            _parse(MY_STRUCT, 'MyStruct.users["carol"].tags{2}').traverse(root)
        )

    def test_the_kind_check_comes_first(self) -> None:
        ints = MutableDynamicValue(ListTypeRef(I32), [1])
        with self.assertRaisesRegex(InvalidPathAccessError, "non-set type"):
            _parse(SetTypeRef(I32), "set<i32>{1}").traverse(ints)
