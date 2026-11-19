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
import io
import tempfile
import threading
from contextlib import redirect_stderr
from pathlib import Path
from typing import Sequence
from unittest import mock

import later.unittest
from test_thrift.thrift_clients import TestingService
from test_thrift.thrift_services import TestingServiceInterface
from test_thrift.thrift_types import Color, SimpleError
from thrift.lib.python.test.test_server import TestServer
from thrift.python.client import ClientType, get_client
from thrift.python.common import RpcOptions
from thrift.python.exceptions import (
    ApplicationError,
    ApplicationErrorType,
    ApplicationOverloadError,
)
from thrift.python.protocol import Protocol, RpcKind
from thrift.python.server import FunctionEntry, ThriftServer


class PrivacyHandler(TestingServiceInterface):
    def should_hide_unexpected_exceptions(self, method_name: bytes) -> bool:
        return method_name in {b"getName", b"invert", b"takes_a_list"}

    async def getName(self) -> str:
        raise KeyError("secret-operation-programmer")

    async def getMethodName(self) -> str:
        raise KeyError("secret-operation-programmer")

    async def invert(self, value: bool) -> bool:
        if value:
            raise ApplicationOverloadError("declared overload")
        raise ApplicationError(ApplicationErrorType.INTERNAL_ERROR, "declared error")

    async def takes_a_list(self, ints: Sequence[int]) -> None:
        raise SimpleError(color=Color.red)


class UnexpectedExceptionPrivacyTest(later.unittest.TestCase):
    def test_generated_default_and_older_function_entry_constructors(self) -> None:
        handler = TestingServiceInterface()
        for name in handler.getFunctionTable():
            self.assertIs(handler.should_hide_unexpected_exceptions(name), False)
        entry = FunctionEntry(RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE, handler.getName)
        older = FunctionEntry(
            RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE,
            handler.getName,
            b"",
            False,
            None,
            False,
        )
        for item in (entry, older):
            self.assertEqual(item.rpc_kind, RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE)
            self.assertEqual(item.interaction, b"")
            self.assertFalse(item.creates_interaction)
            self.assertIsNone(item.interaction_factory)
            self.assertFalse(item.returns_initial_response)

    async def test_configuration_requires_actual_bool(self) -> None:
        handler = PrivacyHandler()
        for disposition in (1, "true", None):
            with (
                self.subTest(disposition=disposition),
                mock.patch.object(
                    handler,
                    "should_hide_unexpected_exceptions",
                    return_value=disposition,
                ),
            ):
                with self.assertRaisesRegex(TypeError, "must return a bool"):
                    ThriftServer(handler)

    async def test_configuration_rejects_non_unary_and_interaction(self) -> None:
        handler = PrivacyHandler()
        entries = [
            FunctionEntry(kind, handler.getName)
            for kind in RpcKind
            if kind != RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE
        ]
        entries.append(
            FunctionEntry(
                RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE, handler.getName, b"Interaction"
            )
        )
        for entry in entries:
            with (
                self.subTest(entry=entry),
                mock.patch.object(
                    handler, "getFunctionTable", return_value={b"getName": entry}
                ),
            ):
                with self.assertRaisesRegex(ValueError, "ordinary unary RPC"):
                    ThriftServer(handler)

    async def test_disposition_is_snapshotted_once_before_serving(self) -> None:
        handler = PrivacyHandler()
        expected_calls = [mock.call(name) for name in handler.getFunctionTable()]
        with (
            tempfile.TemporaryDirectory() as directory,
            mock.patch.object(
                handler,
                "should_hide_unexpected_exceptions",
                wraps=handler.should_hide_unexpected_exceptions,
            ) as disposition,
        ):
            path = Path(directory) / "server.sock"
            test_server = TestServer(handler, path=path)
            self.assertCountEqual(disposition.call_args_list, expected_calls)
            disposition.side_effect = AssertionError("configuration called during RPC")
            test_server.server.enable_cpu_worker_threads(2)
            async with test_server:
                async with get_client(
                    TestingService,
                    path=path,
                    client_type=ClientType.THRIFT_ROCKET_CLIENT_TYPE,
                ) as client:
                    for _ in range(2):
                        with self.assertRaises(ApplicationError) as raised:
                            await client.getName()
                        self.assertEqual(
                            raised.exception.message, "Unexpected server error"
                        )
            self.assertEqual(disposition.call_count, len(expected_calls))

    async def test_per_loop_worker_uses_control_policy_and_its_own_handler(
        self,
    ) -> None:
        constructed: list[WorkerHandler] = []
        called: list[WorkerHandler] = []
        configured: list[WorkerHandler] = []
        control_thread = threading.get_ident()

        class WorkerHandler(PrivacyHandler):
            def should_hide_unexpected_exceptions(self, method_name: bytes) -> bool:
                configured.append(self)
                return self is constructed[0] and method_name == b"getName"

            async def getName(self) -> str:
                called.append(self)
                self.thread = threading.get_ident()
                raise KeyError("secret-operation-programmer")

        def factory() -> WorkerHandler:
            handler = WorkerHandler()
            constructed.append(handler)
            return handler

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "server.sock"
            server = ThriftServer(factory, path=path)
            server.enable_cpu_worker_threads(2)
            serving = asyncio.create_task(server.serve())
            try:
                await server.get_address()
                async with get_client(
                    TestingService,
                    path=path,
                    client_type=ClientType.THRIFT_ROCKET_CLIENT_TYPE,
                ) as client:
                    with self.assertRaises(ApplicationError) as raised:
                        await client.getName()
            finally:
                server.stop()
                await serving
        self.assertEqual(raised.exception.message, "Unexpected server error")
        self.assertEqual(len(constructed), 3)
        self.assertEqual(len(called), 1)
        self.assertIsNot(called[0], constructed[0])
        self.assertNotEqual(called[0].thread, control_thread)
        self.assertTrue(configured)
        self.assertTrue(all(handler is constructed[0] for handler in configured))

    async def test_opt_in_does_not_change_cancellation(self) -> None:
        class CancelHandler(PrivacyHandler):
            async def getName(self) -> str:
                raise asyncio.CancelledError("separate cancellation")

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "server.sock"
            async with TestServer(CancelHandler(), path=path):
                async with get_client(
                    TestingService,
                    path=path,
                    client_type=ClientType.THRIFT_ROCKET_CLIENT_TYPE,
                ) as client:
                    with self.assertRaises(ApplicationError) as raised:
                        await client.getName()
        self.assertEqual(raised.exception.type, ApplicationErrorType.UNKNOWN)
        self.assertEqual(
            raised.exception.message,
            "Application was cancelled on the server with message: separate cancellation",
        )

    async def test_opt_in_opaque_wire_retains_original_traceback(self) -> None:
        for protocol in (Protocol.BINARY, Protocol.COMPACT):
            diagnostics = io.StringIO()
            options = RpcOptions()
            with (
                self.subTest(protocol=protocol),
                tempfile.TemporaryDirectory() as directory,
                redirect_stderr(diagnostics),
            ):
                path = Path(directory) / "server.sock"
                async with TestServer(PrivacyHandler(), path=path):
                    async with get_client(
                        TestingService,
                        path=path,
                        client_type=ClientType.THRIFT_ROCKET_CLIENT_TYPE,
                        protocol=protocol,
                    ) as client:
                        with self.assertRaises(ApplicationError) as raised:
                            await client.getName(rpc_options=options)
                self.assertIn(
                    "Traceback (most recent call last)", diagnostics.getvalue()
                )
                self.assertIn(
                    "KeyError: 'secret-operation-programmer'", diagnostics.getvalue()
                )
                self.assertIn(
                    "Unexpected error in TestingService.getName:",
                    diagnostics.getvalue(),
                )
                self.assertEqual(raised.exception.type, ApplicationErrorType.UNKNOWN)
                self.assertEqual(raised.exception.message, "Unexpected server error")
                self.assertNotIn("secret-operation-programmer", str(raised.exception))
                self.assertNotIn(
                    "secret-operation-programmer", repr(options.read_headers)
                )

    async def test_non_opt_in_retains_repr_despite_request_header(self) -> None:
        options = RpcOptions()
        options.set_header("hide_unexpected_exceptions", "true")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "server.sock"
            async with TestServer(PrivacyHandler(), path=path):
                async with get_client(
                    TestingService,
                    path=path,
                    client_type=ClientType.THRIFT_ROCKET_CLIENT_TYPE,
                ) as client:
                    with self.assertRaises(ApplicationError) as raised:
                        await client.getMethodName(rpc_options=options)
        self.assertEqual(raised.exception.type, ApplicationErrorType.UNKNOWN)
        self.assertEqual(
            raised.exception.message, "KeyError('secret-operation-programmer')"
        )

    async def test_opt_in_preserves_declared_exception_mappings(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "server.sock"
            async with TestServer(PrivacyHandler(), path=path):
                async with get_client(
                    TestingService,
                    path=path,
                    client_type=ClientType.THRIFT_ROCKET_CLIENT_TYPE,
                ) as client:
                    with self.assertRaises(ApplicationError) as application:
                        await client.invert(False)
                    self.assertEqual(
                        application.exception.type, ApplicationErrorType.UNKNOWN
                    )
                    self.assertEqual(application.exception.message, "declared error")
                    with self.assertRaises(ApplicationError) as overload:
                        await client.invert(True)
                    self.assertEqual(
                        overload.exception.type, ApplicationErrorType.LOADSHEDDING
                    )
                    self.assertEqual(overload.exception.message, "declared overload")
                    with self.assertRaises(SimpleError) as declared:
                        await client.takes_a_list([])
                    self.assertEqual(declared.exception.color, Color.red)
