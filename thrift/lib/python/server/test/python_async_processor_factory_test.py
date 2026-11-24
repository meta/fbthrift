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


import importlib.util
import sys

from later.unittest import TestCase
from parameterized import parameterized
from thrift.python.server import RpcKind
from thrift.python.test.python_async_processor_factory_test import (
    PythonAsyncProcessorFactoryCTest as CTests,
)


class PythonAsyncProcessorFactoryTest(TestCase):
    @parameterized.expand(
        [
            (
                b"with_resource_pool_single_request_single_response",
                RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE,
                (
                    b"CreateMethodMetadataResult(MethodMetadataMap(with_resource_pool_single_request_single_response=MethodMetadata("
                    b"executorType=ANY "
                    b"interactionType=NONE "
                    b"rpcKind=SINGLE_REQUEST_SINGLE_RESPONSE "
                    b"priority=NORMAL "
                    b"interactionName=NONE "
                    b"createsInteraction=false "
                    b"isWildcard=false"
                    b")))"
                ),
            ),
            (
                b"with_resource_pool_single_request_no_response",
                RpcKind.SINGLE_REQUEST_NO_RESPONSE,
                (
                    b"CreateMethodMetadataResult(MethodMetadataMap(with_resource_pool_single_request_no_response=MethodMetadata("
                    b"executorType=ANY "
                    b"interactionType=NONE "
                    b"rpcKind=SINGLE_REQUEST_NO_RESPONSE "
                    b"priority=NORMAL "
                    b"interactionName=NONE "
                    b"createsInteraction=false "
                    b"isWildcard=false"
                    b")))"
                ),
            ),
            (
                b"with_resource_pool_single_request_streaming_response",
                RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE,
                (
                    b"CreateMethodMetadataResult(MethodMetadataMap(with_resource_pool_single_request_streaming_response=MethodMetadata("
                    b"executorType=ANY "
                    b"interactionType=NONE "
                    b"rpcKind=SINGLE_REQUEST_STREAMING_RESPONSE "
                    b"priority=NORMAL "
                    b"interactionName=NONE "
                    b"createsInteraction=false "
                    b"isWildcard=false"
                    b")))"
                ),
            ),
        ],
    )
    def test_create_method_metadata(
        self,
        function_name: bytes,
        rpc_kind: RpcKind,
        expected: bytes,
    ) -> None:
        """
        Even though the service creator does not interact directly with
        CreateMethodMetadata, this test is in python because the cpp
        code is in a cython library and I wasn't able to successfully
        make the cpp test depend on a cython_library target.
        """
        CTests(self).test_create_method_metadata(
            function_name,
            rpc_kind,
            expected,
        )

    def test_make_handler_func_full_name(self) -> None:
        CTests(self).test_make_handler_func_full_name(
            b"PythonService",
            b"createResponsesStream",
            RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE,
            b"PythonService.createResponsesStream",
        )

    async def test_workers_serve_requests_from_a_shared_table(self) -> None:
        await CTests(self).test_workers_serve_requests_from_a_shared_table()

    async def test_each_worker_serves_requests_with_its_own_handler(self) -> None:
        await CTests(self).test_each_worker_serves_requests_with_its_own_handler()

    async def test_queued_worker_request_runs_under_its_own_request_context(
        self,
    ) -> None:
        if importlib.util.find_spec("folly.request_context") is None:
            self.skipTest("The OSS folly Python build has no folly.request_context.")
        # Before 3.14, CPython has no context watcher API. Meta's Linux builds
        # carry a backport; on other platforms folly cannot move a
        # RequestContext onto asyncio tasks (folly/python/request_context.h).
        if sys.version_info < (3, 14) and sys.platform != "linux":
            self.skipTest(
                "Before Python 3.14, folly carries a RequestContext into asyncio "
                "tasks only on Linux."
            )
        await CTests(
            self
        ).test_queued_worker_request_runs_under_its_own_request_context()

    async def test_failed_worker_start_propagates_when_the_worker_handler_does_not_suppress(
        self,
    ) -> None:
        await CTests(
            self
        ).test_failed_worker_start_propagates_and_the_worker_handler_exits(None)

    async def test_failed_worker_start_propagates_even_when_the_worker_handler_suppresses(
        self,
    ) -> None:
        await CTests(
            self
        ).test_failed_worker_start_propagates_and_the_worker_handler_exits(True)
