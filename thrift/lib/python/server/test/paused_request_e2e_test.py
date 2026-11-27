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

"""A native interceptor pauses each request that a CPU worker thread serves.
The request must resume on its worker, because Python runs there."""

from __future__ import annotations

import asyncio
import threading

from later.unittest import TestCase
from testing.base_service_only.thrift_clients import BaseService
from testing.base_service_only.thrift_services import BaseServiceInterface
from thrift.python.client import get_client
from thrift.python.server import ThriftServer
from thrift.python.server_impl.interceptor.server_module import PythonServerModule
from thrift.python.test.pause_interceptor_test import PauseInterceptor


class ThreadRecorder(BaseServiceInterface):
    def __init__(self, request_threads: list[threading.Thread]) -> None:
        self._request_threads = request_threads

    async def theAnswer(self) -> int:
        self._request_threads.append(threading.current_thread())
        return 42


# After this many seconds, a server that never starts fails the test.
SERVER_STARTUP_TIMEOUT = 30.0


async def serve_and_ask(server: ThriftServer, requests: int) -> list[int]:
    serve_task = asyncio.create_task(server.serve())
    try:
        address = await asyncio.wait_for(
            server.get_address(), timeout=SERVER_STARTUP_TIMEOUT
        )
        assert address.port is not None
        async with get_client(BaseService, host="::1", port=address.port) as client:
            return list(
                await asyncio.wait_for(
                    asyncio.gather(*(client.theAnswer() for _ in range(requests))),
                    timeout=10.0,
                )
            )
    finally:
        server.stop()
        await asyncio.wait_for(serve_task, timeout=10.0)


class PausedRequestTest(TestCase):
    async def test_worker_request_resumes_on_its_worker_after_native_pause(
        self,
    ) -> None:
        # GIVEN
        main_thread = threading.current_thread()
        request_threads: list[threading.Thread] = []
        interceptor = PauseInterceptor()
        module = PythonServerModule("PauseInterceptorTestModule")
        interceptor.install(module)
        server = ThriftServer(lambda: ThreadRecorder(request_threads), ip="::1")
        server.enable_cpu_worker_threads(2)
        server.add_server_module(module)
        expected_responses = [42] * 8

        # WHEN
        responses = await serve_and_ask(server, len(expected_responses))

        # THEN
        self.assertEqual(expected_responses, responses)
        self.assertEqual(len(expected_responses), interceptor.requests())
        self.assertEqual(len(expected_responses), len(request_threads))
        self.assertNotIn(main_thread, request_threads)
        self.assertFalse(
            [thread for thread in request_threads if thread.name.startswith("Dummy")]
        )
