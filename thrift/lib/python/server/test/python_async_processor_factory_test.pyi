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

# pyre-strict

import unittest

from thrift.python.server import RpcKind

class PythonAsyncProcessorFactoryCTest:
    def __init__(self, unit_test: unittest.TestCase) -> None: ...
    def test_create_method_metadata(
        self,
        function_name: bytes,
        rpc_kind: RpcKind,
        expected: bytes,
    ) -> None: ...
    def test_make_handler_func_full_name(
        self,
        service_name: bytes,
        function_name: bytes,
        rpc_kind: RpcKind,
        expected: bytes,
    ) -> None: ...
    async def test_workers_serve_requests_from_a_shared_table(self) -> None: ...
    async def test_each_worker_serves_requests_with_its_own_handler(self) -> None: ...
    async def test_queued_worker_request_runs_under_its_own_request_context(
        self,
    ) -> None: ...
    async def test_failed_worker_start_propagates_and_the_worker_handler_exits(
        self, exit_result: bool | None
    ) -> None: ...
