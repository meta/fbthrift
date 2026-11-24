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
from unittest.mock import patch

from later.unittest import TestCase
from test_thrift.thrift_clients import TestingService
from thrift.python.test.channel_callback import complete_channel


class ChannelCallbackTest(TestCase):
    async def _closed_after_interrupted_startup(self) -> TestingService.Async:
        client = TestingService.Async()
        startup = asyncio.create_task(client.__aenter__())
        await asyncio.sleep(0)
        startup.cancel("scope interrupted")
        with self.assertRaises(asyncio.CancelledError):
            await startup
        await client.__aexit__(None, None, None)
        return client

    async def test_late_success_does_not_rebind_closed_client(self) -> None:
        client = await self._closed_after_interrupted_startup()
        self.assertFalse(complete_channel(client, False))
        with self.assertRaises(asyncio.InvalidStateError):
            await client.__aenter__()

    async def test_late_failure_does_not_complete_closed_future_twice(self) -> None:
        client = await self._closed_after_interrupted_startup()
        errors: list[object] = []
        with patch("sys.unraisablehook", side_effect=errors.append):
            self.assertFalse(complete_channel(client, True))
        self.assertEqual([], errors)
        with self.assertRaises(asyncio.InvalidStateError):
            await client.__aenter__()

    async def test_normal_success_still_binds_and_enters(self) -> None:
        client = TestingService.Async()
        self.assertTrue(complete_channel(client, False))
        self.assertIs(client, await client.__aenter__())
        await client.__aexit__(None, None, None)

    async def test_normal_failure_still_propagates(self) -> None:
        client = TestingService.Async()
        self.assertFalse(complete_channel(client, True))
        with self.assertRaisesRegex(RuntimeError, "late channel failure"):
            await client.__aenter__()
        await client.__aexit__(None, None, None)
