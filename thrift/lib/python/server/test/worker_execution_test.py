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

import asyncio
import threading
from typing import Any, cast

from later.unittest import TestCase
from testing.base_service_only.thrift_clients import BaseService
from testing.base_service_only.thrift_services import BaseServiceInterface
from thrift.python.client import get_client
from thrift.python.server import ThriftServer


class ThreadRecordingAnswer(BaseServiceInterface):
    def __init__(self, request_threads: list[threading.Thread]) -> None:
        self._request_threads = request_threads

    async def theAnswer(self) -> int:
        self._request_threads.append(threading.current_thread())
        return 42


# A fixed timeout cannot prevent flakes. This bound only keeps a hung server
# startup from stalling the test; it does not check correctness. If the test
# still times out here, raise the bound.
SERVER_STARTUP_TIMEOUT = 30.0


async def serve_and_ask_once(server: ThriftServer) -> int:
    serve_task = asyncio.create_task(server.serve())
    try:
        address = await asyncio.wait_for(
            server.get_address(), timeout=SERVER_STARTUP_TIMEOUT
        )
        assert address.port is not None
        async with get_client(BaseService, host="::1", port=address.port) as client:
            return await asyncio.wait_for(client.theAnswer(), timeout=5.0)
    finally:
        server.stop()
        await asyncio.wait_for(serve_task, timeout=5.0)


class WorkerExecutionTest(TestCase):
    async def asyncSetUp(self) -> None:
        self.maxDiff = None

    async def test_enable_cpu_worker_threads_runs_factory_requests_on_workers(
        self,
    ) -> None:
        # GIVEN
        expected_excluded_thread = threading.current_thread()
        request_threads: list[threading.Thread] = []
        server = ThriftServer(lambda: ThreadRecordingAnswer(request_threads), ip="::1")
        server.enable_cpu_worker_threads(2)
        expected_response = 42
        expected_request_count = 1

        # WHEN
        actual_response = await serve_and_ask_once(server)

        # THEN
        self.assertEqual(expected_response, actual_response)
        self.assertEqual(expected_request_count, len(request_threads))
        self.assertNotIn(expected_excluded_thread, request_threads)

    async def test_enable_cpu_worker_threads_runs_shared_handler_requests_on_workers(
        self,
    ) -> None:
        # GIVEN
        control_thread = threading.current_thread()
        request_threads: list[threading.Thread] = []
        server = ThriftServer(ThreadRecordingAnswer(request_threads), ip="::1")
        server.enable_cpu_worker_threads(2)
        expected_response = 42
        expected_request_count = 1

        # WHEN
        actual_response = await serve_and_ask_once(server)

        # THEN
        self.assertEqual(expected_response, actual_response)
        self.assertEqual(expected_request_count, len(request_threads))
        self.assertIsNot(control_thread, request_threads[0])

    async def test_enable_cpu_worker_threads_back_to_one_stays_single_threaded(
        self,
    ) -> None:
        # GIVEN
        control_thread = threading.current_thread()
        request_threads: list[threading.Thread] = []
        server = ThriftServer(lambda: ThreadRecordingAnswer(request_threads), ip="::1")
        server.enable_cpu_worker_threads(2)
        server.enable_cpu_worker_threads(1)
        expected_response = 42
        expected_request_threads = [control_thread]

        # WHEN
        actual_response = await serve_and_ask_once(server)

        # THEN
        self.assertEqual(expected_response, actual_response)
        self.assertEqual(expected_request_threads, request_threads)

    async def test_handler_factory_without_threads_serves_on_control(self) -> None:
        # GIVEN
        control_thread = threading.current_thread()
        request_threads: list[threading.Thread] = []
        factory_calls: list[int] = []

        def create_handler() -> ThreadRecordingAnswer:
            factory_calls.append(1)
            return ThreadRecordingAnswer(request_threads)

        server = ThriftServer(create_handler, ip="::1")
        expected_response = 42
        expected_request_threads = [control_thread]
        expected_factory_calls = [1]

        # WHEN
        actual_response = await serve_and_ask_once(server)

        # THEN
        self.assertEqual(expected_response, actual_response)
        self.assertEqual(expected_request_threads, request_threads)
        self.assertEqual(expected_factory_calls, factory_calls)

    def test_handler_factory_that_returns_no_service_interface_raises(self) -> None:
        # GIVEN
        handler_factory = cast(Any, lambda: 42)
        expected_message = (
            "Handler factory returned int; expected a thrift-python ServiceInterface."
        )

        # WHEN
        with self.assertRaises(TypeError) as raised:
            ThriftServer(handler_factory, ip="::1")

        # THEN
        self.assertEqual(expected_message, str(raised.exception))

    def test_handler_that_is_neither_interface_nor_factory_raises(self) -> None:
        # GIVEN
        handler = cast(Any, 42)
        expected_message = (
            "handler must be a ServiceInterface, a callable that returns a "
            "ServiceInterface, or an AsyncProcessorFactory"
        )

        # WHEN
        with self.assertRaises(TypeError) as raised:
            ThriftServer(handler, ip="::1")

        # THEN
        self.assertEqual(expected_message, str(raised.exception))

    def test_enable_cpu_worker_threads_without_python_handler_raises(self) -> None:
        # GIVEN
        server = ThriftServer(None, ip="::1")
        expected_message = (
            "Threads need a thrift-python handler. Pass a thrift-python "
            "ServiceInterface instance or handler factory to the server."
        )

        # WHEN
        with self.assertRaises(TypeError) as raised:
            server.enable_cpu_worker_threads(2)

        # THEN
        self.assertEqual(expected_message, str(raised.exception))

    def test_enable_cpu_worker_threads_rejects_count_below_one(self) -> None:
        # GIVEN
        server = ThriftServer(ThreadRecordingAnswer([]), ip="::1")
        expected_message = "Thread count must be one or more; got 0."

        # WHEN
        with self.assertRaises(ValueError) as raised:
            server.enable_cpu_worker_threads(0)

        # THEN
        self.assertEqual(expected_message, str(raised.exception))

    def test_get_cpu_worker_threads_reports_the_enabled_count(self) -> None:
        # GIVEN
        server = ThriftServer(ThreadRecordingAnswer([]), ip="::1")
        server.enable_cpu_worker_threads(3)
        expected_count = 3

        # WHEN
        actual_count = server.get_cpu_worker_threads()

        # THEN
        self.assertEqual(expected_count, actual_count)

    def test_get_cpu_worker_threads_reports_one_without_a_call(self) -> None:
        # GIVEN
        server = ThriftServer(ThreadRecordingAnswer([]), ip="::1")
        expected_count = 1

        # WHEN
        actual_count = server.get_cpu_worker_threads()

        # THEN
        self.assertEqual(expected_count, actual_count)
