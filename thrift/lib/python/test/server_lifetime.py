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

from later.unittest import TestCase
from thrift.python.server import ThriftServer


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
            unserved_retained_count = sum(
                id(obj) == unserved_id and type(obj) is ThriftServer
                for obj in gc.get_objects()
            )
            served_id = await serve_and_stop()
            await asyncio.get_running_loop().shutdown_default_executor()
            retained_before_gc = sum(
                id(obj) == served_id and type(obj) is ThriftServer
                for obj in gc.get_objects()
            )
            gc.collect()
            retained_after_gc = sum(
                id(obj) == served_id and type(obj) is ThriftServer
                for obj in gc.get_objects()
            )
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
