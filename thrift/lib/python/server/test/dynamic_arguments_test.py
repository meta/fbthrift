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

"""End to end: a C++ ServiceInterceptor reads a thrift-python service's
arguments off the schema its generated modules bundle."""

from __future__ import annotations

import asyncio
import unittest

from thrift.python.client import ClientType, get_client
from thrift.python.server import ServiceInterface, ThriftServer
from thrift.python.server_impl.interceptor.server_module import PythonServerModule
from thrift.python.test.dynamic_arguments_test import (
    DynamicArgumentsRecorder,
    set_populate_service_schema,
    unmock_populate_service_schema,
)
from thrift.python.test.python_service_schema_test.thrift_clients import TestService
from thrift.python.test.python_service_schema_test.thrift_mutable_services import (
    TestServiceInterface as MutableTestServiceInterface,
)
from thrift.python.test.python_service_schema_test.thrift_services import (
    TestServiceInterface,
)
from thrift.python.test.python_service_schema_test_dep.thrift_mutable_types import (
    Point as MutablePoint,
)
from thrift.python.test.python_service_schema_test_dep.thrift_types import Point


class Handler(TestServiceInterface):
    async def describe(self, point: Point, label: str) -> str:
        return f"{label}:{point.x},{point.y}"

    async def ping(self, seq: int) -> int:
        return seq


class MutableHandler(MutableTestServiceInterface):
    async def describe(self, point: MutablePoint, label: str) -> str:
        return f"{label}:{point.x},{point.y}"

    async def ping(self, seq: int) -> int:
        return seq


class DynamicArgumentsTest(unittest.IsolatedAsyncioTestCase):
    async def _serve(
        self,
        recorder: DynamicArgumentsRecorder,
        handler: ServiceInterface | None = None,
    ) -> None:
        module = PythonServerModule("DynamicArgumentsTestModule")
        recorder.install(module)

        server = ThriftServer(handler or Handler(), ip="::1")
        server.add_server_module(module)
        serve_task = asyncio.create_task(server.serve())
        try:
            address = await asyncio.wait_for(server.get_address(), timeout=30)
            assert address.ip is not None and address.port is not None
            async with get_client(
                TestService,
                host=str(address.ip),
                port=address.port,
                client_type=ClientType.THRIFT_ROCKET_CLIENT_TYPE,
            ) as client:
                self.assertEqual(
                    "here:3,4",
                    await client.describe(Point(x=3, y=4), "here"),
                )
                self.assertEqual(7, await client.ping(7))
        finally:
            server.stop()
            await asyncio.gather(serve_task, return_exceptions=True)

    async def test_interceptor_reads_the_arguments_of_each_method(self) -> None:
        recorder = DynamicArgumentsRecorder()

        await self._serve(recorder)

        # Before this was wired up, every record read `<no schema>`: a
        # thrift-python server never populated the schema a C++ interceptor
        # needs to deserialize arguments.
        self.assertEqual(
            ["describe point=(3,4) label=here", "ping seq=7"],
            recorder.records(),
        )

    async def test_interceptor_reads_the_arguments_of_a_mutable_handler(
        self,
    ) -> None:
        recorder = DynamicArgumentsRecorder()

        await self._serve(recorder, MutableHandler())

        self.assertEqual(
            ["describe point=(3,4) label=here", "ping seq=7"],
            recorder.records(),
        )

    async def test_the_kill_switch_restores_the_prior_behavior(self) -> None:
        recorder = DynamicArgumentsRecorder()
        set_populate_service_schema(False)
        self.addCleanup(unmock_populate_service_schema)

        await self._serve(recorder)

        # No schema is served, so an interceptor sees a null
        # `dynamicArguments` -- exactly what it saw before this change.
        self.assertEqual(
            ["describe:<no schema>", "ping:<no schema>"],
            recorder.records(),
        )
