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

import sys
import types
import unittest

import thrift.python.test.python_service_schema_test.thrift_types as types_module
import thrift.python.test.python_service_schema_test_base.thrift_types as base_types_module
import thrift.python.test.python_service_schema_test_dep.thrift_types as dep_types_module
from thrift.python.server_impl.schema_bundle import collect_schema_bundles
from thrift.python.test.python_service_schema_test.thrift_mutable_services import (
    TestServiceInterface as MutableTestServiceInterface,
)
from thrift.python.test.python_service_schema_test.thrift_services import (
    ExtendsRemoteBaseInterface,
    TestServiceInterface,
)


def _own_bundle(module: object) -> bytes:
    """The ``_fbthrift_schema_*`` attribute of a generated module."""
    bundles = [
        value
        for name, value in vars(module).items()
        if name.startswith("_fbthrift_schema_")
    ]
    assert len(bundles) == 1, f"expected one bundle, got {len(bundles)}"
    return bundles[0]


# A handler is a user-written subclass, so its own module is the user's rather
# than the generated one. That is the shape the server passes in.
class TestServiceHandler(TestServiceInterface):
    async def describe(self, point: object, label: str) -> str:
        return label

    async def ping(self, seq: int) -> int:
        return seq


class ExtendsRemoteBaseHandler(ExtendsRemoteBaseInterface):
    async def noop(self) -> None:
        pass

    async def remotePing(self, seq: int) -> int:
        return seq


class MutableTestServiceHandler(MutableTestServiceInterface):
    async def describe(self, point: object, label: str) -> str:
        return label

    async def ping(self, seq: int) -> int:
        return seq


class SchemaBundleTest(unittest.TestCase):
    def test_collects_the_services_own_bundle_first(self) -> None:
        bundles = collect_schema_bundles(TestServiceHandler)

        self.assertTrue(bundles)
        self.assertEqual(bundles[0], _own_bundle(types_module))

    def test_collects_the_bundle_of_an_included_program(self) -> None:
        # `describe` takes a `Point` from the included program, so its bundle
        # has to come along or the schema cannot resolve the parameter.
        bundles = collect_schema_bundles(TestServiceHandler)

        self.assertIn(_own_bundle(dep_types_module), bundles)

    def test_collects_the_bundle_of_a_base_service_from_a_program_without_types(
        self,
    ) -> None:
        # The service's own `thrift_types` module does not import the base
        # service's program, which has no types to import.
        bundles = collect_schema_bundles(ExtendsRemoteBaseHandler)

        self.assertEqual(bundles[0], _own_bundle(types_module))
        self.assertIn(_own_bundle(base_types_module), bundles)
        self.assertEqual(len(bundles), len(set(bundles)))

    def test_collects_each_bundle_once(self) -> None:
        bundles = collect_schema_bundles(TestServiceHandler)

        self.assertEqual(len(bundles), len(set(bundles)))

    def test_works_through_a_deeper_subclass(self) -> None:
        class Subclass(TestServiceHandler):
            pass

        self.assertEqual(
            collect_schema_bundles(Subclass),
            collect_schema_bundles(TestServiceHandler),
        )

    def test_has_no_bundles_for_a_non_thrift_class(self) -> None:
        class NotAHandler:
            pass

        self.assertEqual(collect_schema_bundles(NotAHandler), [])

    def test_has_no_bundles_for_the_generated_interface_of_no_module(
        self,
    ) -> None:
        # A class whose module is not a generated `thrift_services`.
        self.assertEqual(collect_schema_bundles(dict), [])

    def test_collects_the_immutable_bundles_for_a_mutable_handler(self) -> None:
        # `thrift_mutable_types` carries no schema constant, so a mutable
        # handler's bundles come from the immutable `thrift_types` modules.
        self.assertEqual(
            collect_schema_bundles(MutableTestServiceHandler),
            collect_schema_bundles(TestServiceHandler),
        )

    def test_collects_a_bundle_carried_by_two_modules_once(self) -> None:
        # Two modules carrying the same bundle under different names.
        first = types.ModuleType("schema_bundle_test_first.thrift_types")
        second = types.ModuleType("schema_bundle_test_second.thrift_types")
        vars(first).update(_fbthrift_schema_0=b"the same bundle", dep=second)
        vars(second).update(_fbthrift_schema_0=b"the same bundle")
        sys.modules[first.__name__] = first
        self.addCleanup(sys.modules.pop, first.__name__)

        class Handler:
            pass

        Handler.__module__ = "schema_bundle_test_first.thrift_services"

        self.assertEqual(collect_schema_bundles(Handler), [b"the same bundle"])
