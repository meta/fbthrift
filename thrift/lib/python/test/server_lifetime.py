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
import gc
import json
import sys
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from typing import ParamSpec, TypeVar

from later.unittest import TestCase
from thrift.python.server import ThriftServer


_P = ParamSpec("_P")
_T = TypeVar("_T")


class KeepCallsToHoldServerExecutor(ThreadPoolExecutor):
    # A pool thread can still hold a finished call's arguments when the caller
    # resumes. This executor holds them until the test clears kept_calls,
    # so the test meets that worst case on every run instead of by chance.
    def __init__(self) -> None:
        super().__init__(max_workers=1)
        self.kept_calls: list[tuple[object, ...]] = []

    def submit(
        self, fn: Callable[_P, _T], /, *args: _P.args, **kwargs: _P.kwargs
    ) -> Future[_T]:
        self.kept_calls.append((fn, args, kwargs))
        return super().submit(fn, *args, **kwargs)


def count_live_servers(server_id: int) -> int:
    return sum(
        id(obj) == server_id and type(obj) is ThriftServer for obj in gc.get_objects()
    )


async def serve_and_stop() -> int:
    server = ThriftServer(None, port=0, ip="::1")
    server.set_io_worker_threads(1)
    server_id = id(server)
    serve_task = asyncio.create_task(server.serve())
    try:
        await server.get_address()
    finally:
        server.stop()
        await serve_task
    return server_id


class TestServerLifetime(TestCase):
    async def test_server_releases_after_serve_without_gc(self) -> None:
        # GIVEN
        expected_retained_count = 0
        gc.collect()
        gc_was_enabled = gc.isenabled()
        gc.disable()
        try:
            unserved_server = ThriftServer(None, port=0, ip="::1")
            unserved_was_tracked = gc.is_tracked(unserved_server)
            unserved_id = id(unserved_server)

            # WHEN
            del unserved_server
            unserved_retained_count = count_live_servers(unserved_id)
            served_id = await serve_and_stop()
            retained_before_gc = count_live_servers(served_id)
            gc.collect()
            retained_after_gc = count_live_servers(served_id)
            print(
                json.dumps(
                    {
                        "python": sys.version,
                        "unserved_retained": unserved_retained_count,
                        "served_before_gc": retained_before_gc,
                        "served_after_gc": retained_after_gc,
                    }
                )
            )

            # THEN
            self.assertTrue(unserved_was_tracked)
            self.assertEqual(expected_retained_count, unserved_retained_count)
            self.assertEqual(expected_retained_count, retained_after_gc)
            self.assertEqual(expected_retained_count, retained_before_gc)
        finally:
            gc.collect()
            if gc_was_enabled:
                gc.enable()

    async def test_server_releases_after_serve_while_the_pool_keeps_its_call(
        self,
    ) -> None:
        # GIVEN
        expected_live_servers_after_serve = 0
        serve_thread_executor = KeepCallsToHoldServerExecutor()
        asyncio.get_running_loop().set_default_executor(serve_thread_executor)
        gc.collect()
        gc_was_enabled = gc.isenabled()
        gc.disable()
        try:
            # WHEN
            served_id = await serve_and_stop()
            actual_live_servers_after_serve = count_live_servers(served_id)
        finally:
            # Without the fix, the kept calls still hold the server. Free it before the
            # test ends.
            serve_thread_executor.kept_calls.clear()
            # Await the executor shutdown, which leaves the event loop free: a native
            # serve that has not stopped yet may need the loop to finish.
            await asyncio.get_running_loop().shutdown_default_executor()
            gc.collect()
            if gc_was_enabled:
                gc.enable()

        # THEN
        self.assertEqual(
            expected_live_servers_after_serve, actual_live_servers_after_serve
        )
