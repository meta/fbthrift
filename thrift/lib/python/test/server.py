#!/usr/bin/env python3
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
import concurrent.futures
import gc
import threading
import weakref
from collections.abc import Callable
from contextlib import contextmanager
from types import TracebackType
from typing import Iterator, Optional, Sequence
from unittest import mock

import later.unittest
from test_thrift.thrift_clients import TestingService
from test_thrift.thrift_services import TestingServiceInterface
from test_thrift.thrift_types import Color, easy
from thrift.py3 import server as server_module
from thrift.py3.server import SocketAddress
from thrift.python.client import get_client
from thrift.python.server import ThriftServer

# White-box: the runtime holds strong refs to in-flight onStopRequested lifecycle
# tasks here so asyncio's weak task bookkeeping can't GC them before they run.
# pyre-ignore[21]: Cython module has no type stub for pyre.
from thrift.python.server_impl import python_async_processor

# Typed alias for the same runtime set object (the Cython module has no stub).
_inflight_lifecycle_tasks: set[asyncio.Task[object]] = (
    python_async_processor._inflight_lifecycle_tasks
)


@contextmanager
def event_loop() -> Iterator[asyncio.AbstractEventLoop]:
    """
    If there is already running loop, yields it. Otherwise, creates a new loop,
    sets it as the current event loop, yields it, and then closes it on exit.
    """
    try:
        loop = asyncio.get_running_loop()
        yield loop
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            yield loop
        finally:
            loop.close()


class Handler(TestingServiceInterface):
    initialized = False

    def __init__(self) -> None:
        self.on_start_serving = False
        self.on_stop_requested = False

    async def __aenter__(self) -> "Handler":
        self.initialized = True
        return self

    async def onStartServing(self) -> None:
        self.on_start_serving = True

    async def onStopRequested(self) -> None:
        self.on_stop_requested = True

    async def invert(self, value: bool) -> bool:
        return not value

    async def getName(self) -> str:
        return "Testing"

    async def getMethodName(self) -> str:
        return "Testing"

    async def getRequestTimeout(self) -> float:
        return 100.0

    async def shutdown(self) -> None:
        pass

    async def complex_action(
        self, first: str, second: str, third: int, fourth: str
    ) -> int:
        return third

    async def takes_a_list(self, ints: Sequence[int]) -> None:
        pass

    async def take_it_easy(self, how: int, what: easy) -> None:
        pass

    async def pick_a_color(self, color: Color) -> None:
        pass

    async def int_sizes(self, one: int, two: int, three: int, four: int) -> None:
        pass

    async def hard_error(self, valid: bool) -> None:
        pass

    async def renamed_func(self, ret: bool) -> bool:
        return ret


class CancelOnStopRequestedHandler(Handler):
    # onStopRequested cancels its own lifecycle task, the way the asyncio loop
    # cancels pending tasks during server/loop teardown. The C++
    # ThriftServer::callOnStopRequested collector XLOG(FATAL)s (aborting the
    # process) if the lifecycle promise resolves to an exception, so cancellation
    # here must still complete the promise.
    async def onStopRequested(self) -> None:
        self.on_stop_requested = True
        current = asyncio.current_task()
        assert current is not None
        current.cancel()
        await asyncio.sleep(0)


class BlockingOnStopRequestedHandler(Handler):
    # onStopRequested suspends mid-flight, modelling a hook still running as the
    # server tears down, so a test can observe the in-flight lifecycle task.
    def __init__(self) -> None:
        super().__init__()
        self.hook_started: asyncio.Event = asyncio.Event()
        self.release_hook: asyncio.Event = asyncio.Event()
        self.lifecycle_task: asyncio.Task[object] | None = None

    async def onStopRequested(self) -> None:
        # onStopRequested is awaited inside the runtime's `lifecycle_coro`, not
        # wrapped in a new task, so the currently running task IS the exact
        # lifecycle task the runtime tracks. Capturing it here lets the test
        # assert on that specific task rather than on shared set state.
        self.lifecycle_task = asyncio.current_task()
        self.hook_started.set()
        await self.release_hook.wait()
        self.on_stop_requested = True


class DrainingHandler(Handler):
    def __init__(self) -> None:
        super().__init__()
        self.events: list[str] = []
        self.request_started: asyncio.Event = asyncio.Event()
        self.release_request: asyncio.Event = asyncio.Event()
        self.stop_requested: asyncio.Event = asyncio.Event()

    async def __aenter__(self) -> "DrainingHandler":
        self.events.append("context_enter")
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.events.append("context_exit")

    async def onStartServing(self) -> None:
        self.events.append("start_serving")

    async def onStopRequested(self) -> None:
        self.events.append("stop_requested")
        self.stop_requested.set()

    async def invert(self, value: bool) -> bool:
        self.events.append("request_started")
        self.request_started.set()
        await self.release_request.wait()
        self.events.append("request_finished")
        return not value


class BlockingEntryHandler(Handler):
    def __init__(self) -> None:
        super().__init__()
        self.events: list[str] = []
        self.enter_started: asyncio.Event = asyncio.Event()

    async def __aenter__(self) -> "BlockingEntryHandler":
        self.events.append("context_enter")
        self.enter_started.set()
        await asyncio.Event().wait()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.events.append("context_exit")


class EnterFailureHandler(Handler):
    def __init__(self, failure: RuntimeError) -> None:
        super().__init__()
        self.failure = failure
        self.enter_count = 0
        self.exit_count = 0

    async def __aenter__(self) -> "EnterFailureHandler":
        self.enter_count += 1
        raise self.failure

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        self.exit_count += 1


class ExitFailureHandler(Handler):
    def __init__(self, failure: RuntimeError) -> None:
        super().__init__()
        self.failure = failure
        self.enter_count = 0
        self.exit_count = 0

    async def __aenter__(self) -> "ExitFailureHandler":
        self.enter_count += 1
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        self.exit_count += 1
        raise self.failure


class ServicesTests(later.unittest.TestCase):
    def test_handler_acontext(self) -> None:
        async def inner() -> None:
            async with Handler() as h:
                self.assertTrue(h.initialized)

        asyncio.run(inner())

    def test_get_service_name(self) -> None:
        self.assertEqual(Handler.service_name(), b"TestingService")

    def test_get_address(self) -> None:
        addy = asyncio.run(self.get_address())
        self.assertIsInstance(addy, SocketAddress)

    async def get_address(self, handler: Optional[Handler] = None) -> SocketAddress:
        loop = asyncio.get_running_loop()
        # pyrefly: ignore [bad-specialization]
        server = ThriftServer(Handler() if handler is None else handler, port=0)
        serve_task = loop.create_task(server.serve())
        addy = await server.get_address()
        server.stop()
        await serve_task
        return addy

    def test_unittest_call(self) -> None:
        h = Handler()
        call = 5
        ret = asyncio.run(h.complex_action("", "", call, ""))
        self.assertEqual(call, ret)

    def test_unittest_call_renamed_func(self) -> None:
        h = Handler()
        ret = asyncio.run(h.renamed_func(True))
        self.assertTrue(ret)

    def test_server_manipulate_config(self) -> None:
        MAX_REQUESTS = 142
        MAX_CONNECTIONS = 132
        LISTEN_BACKLOG = 167
        NUM_IO_WORKERS = 10
        IDLE_TIMEOUT = 19.84
        QUEUE_TIMEOUT = 20.19
        SOCKET_QUEUE_TIMEOUT = 21.37

        with event_loop():
            # pyrefly: ignore [bad-specialization]
            server = ThriftServer(Handler(), port=0)
            server.set_max_requests(MAX_REQUESTS)
            server.set_max_connections(MAX_CONNECTIONS)
            server.set_listen_backlog(LISTEN_BACKLOG)
            server.set_io_worker_threads(NUM_IO_WORKERS)
            server.set_idle_timeout(IDLE_TIMEOUT)
            server.set_queue_timeout(QUEUE_TIMEOUT)
            server.set_socket_queue_timeout(SOCKET_QUEUE_TIMEOUT)
            self.assertEqual(server.get_max_requests(), MAX_REQUESTS)
            self.assertEqual(server.get_max_connections(), MAX_CONNECTIONS)
            self.assertEqual(server.get_listen_backlog(), LISTEN_BACKLOG)
            self.assertEqual(server.get_io_worker_threads(), NUM_IO_WORKERS)
            self.assertEqual(server.get_idle_timeout(), IDLE_TIMEOUT)
            self.assertEqual(server.get_queue_timeout(), QUEUE_TIMEOUT)
            self.assertEqual(server.get_socket_queue_timeout(), SOCKET_QUEUE_TIMEOUT)

            self.assertFalse(server.is_plaintext_allowed_on_loopback())
            server.set_allow_plaintext_on_loopback(True)
            self.assertTrue(server.is_plaintext_allowed_on_loopback())

    def test_server_get_stats(self) -> None:
        with event_loop():
            # pyrefly: ignore [bad-specialization]
            server = ThriftServer(Handler(), port=0)

            active_requests = server.get_active_requests()
            self.assertGreaterEqual(active_requests, 0)
            self.assertLess(active_requests, 10)

    def test_lifecycle_hooks(self) -> None:
        handler = Handler()
        asyncio.run(self.get_address(handler))
        self.assertTrue(handler.on_start_serving)
        self.assertTrue(handler.on_stop_requested)

    def test_on_stop_requested_cancellation_does_not_crash(self) -> None:
        handler = CancelOnStopRequestedHandler()
        asyncio.run(self.get_address(handler))
        self.assertTrue(handler.on_stop_requested)

    def test_on_stop_requested_task_is_strongly_referenced(self) -> None:
        # Regression for the shutdown BrokenPromise crash: while an
        # onStopRequested hook is in flight, the runtime must keep a strong
        # reference to its lifecycle task so asyncio's weak bookkeeping cannot
        # GC it before it completes (which would break the C++ promise and
        # FATAL-abort the worker). Mirrors _inflight_termination_tasks.
        handler: BlockingOnStopRequestedHandler = BlockingOnStopRequestedHandler()

        async def inner() -> None:
            loop = asyncio.get_running_loop()
            server = ThriftServer(handler, port=0)
            serve_task = loop.create_task(server.serve())
            await server.get_address()
            server.stop()
            # Let the stop path schedule onStopRequested and run it up to its
            # suspension point, then grab the exact task the runtime tracks.
            await asyncio.wait_for(handler.hook_started.wait(), timeout=5)
            task = handler.lifecycle_task
            assert task is not None
            # The runtime strongly references the in-flight lifecycle task...
            self.assertIn(task, _inflight_lifecycle_tasks)
            self.assertFalse(task.done())
            # ...so it survives a GC pass while suspended.
            gc.collect()
            self.assertIn(task, _inflight_lifecycle_tasks)
            handler.release_hook.set()
            await serve_task
            # Once settled, the done-callback discards exactly this task.
            await asyncio.sleep(0)
            self.assertNotIn(task, _inflight_lifecycle_tasks)

        asyncio.run(inner())
        self.assertTrue(handler.on_stop_requested)

    def test_cancellation_drains_request_before_context_exit(self) -> None:
        # GIVEN
        handler = DrainingHandler()
        expected_events = [
            "context_enter",
            "start_serving",
            "request_started",
            "stop_requested",
            "request_finished",
            "context_exit",
        ]

        async def inner() -> tuple[bool, bool, bool, list[str]]:
            server = ThriftServer(handler, ip="::1", port=0)
            serve_task = asyncio.create_task(server.serve())
            try:
                address = await server.get_address()
                assert address.ip and address.port

                async with get_client(
                    TestingService,
                    host=address.ip,
                    port=address.port,
                ) as client:
                    request_task = asyncio.create_task(client.invert(True))
                    await asyncio.wait_for(handler.request_started.wait(), timeout=5)
                    serve_task.cancel()
                    await asyncio.wait_for(handler.stop_requested.wait(), timeout=5)
                    serve_completed_before_request_release = serve_task.done()
                    handler.release_request.set()
                    response = await asyncio.wait_for(request_task, timeout=5)

                serve_result = await asyncio.wait_for(
                    asyncio.gather(serve_task, return_exceptions=True),
                    timeout=5,
                )
                return (
                    serve_completed_before_request_release,
                    response,
                    isinstance(serve_result[0], asyncio.CancelledError),
                    handler.events,
                )
            finally:
                handler.release_request.set()
                server.stop()
                await asyncio.gather(serve_task, return_exceptions=True)

        # WHEN
        (
            actual_serve_completed_before_request_release,
            actual_response,
            actual_serve_cancelled,
            actual_events,
        ) = asyncio.run(inner())

        # THEN
        self.assertFalse(actual_serve_completed_before_request_release)
        self.assertFalse(actual_response)
        self.assertTrue(actual_serve_cancelled)
        self.assertEqual(expected_events, actual_events)

    def test_cancellation_during_context_entry_does_not_start_serving(self) -> None:
        # GIVEN
        handler = BlockingEntryHandler()
        expected_events = ["context_enter"]

        async def inner() -> tuple[bool, bool, list[str]]:
            server = ThriftServer(handler, ip="::1", port=0)
            serve_task = asyncio.create_task(server.serve())
            try:
                await asyncio.wait_for(handler.enter_started.wait(), timeout=5)
                serve_task.cancel()
                serve_result = await asyncio.wait_for(
                    asyncio.gather(serve_task, return_exceptions=True),
                    timeout=5,
                )
                return (
                    isinstance(serve_result[0], asyncio.CancelledError),
                    handler.on_start_serving,
                    handler.events,
                )
            finally:
                server.stop()
                await asyncio.gather(serve_task, return_exceptions=True)

        # The cancelled serve task keeps the server alive through its traceback
        # cycle, so only a collection frees it. Collect while the loop still
        # runs: the C++ server holds the last reference to the address future,
        # and dropping it after the loop is otherwise unreferenced destroys the
        # loop's folly executor from inside ~ThriftServer, which then hangs
        # waiting on keep-alives that same destructor has yet to release.
        async def inner_and_release_server() -> tuple[bool, bool, list[str]]:
            result = await inner()
            gc.collect()
            return result

        # WHEN
        actual_serve_cancelled, actual_serving_started, actual_events = asyncio.run(
            inner_and_release_server()
        )

        # THEN
        self.assertTrue(actual_serve_cancelled)
        self.assertFalse(actual_serving_started)
        self.assertEqual(expected_events, actual_events)

    async def test_cancellation_after_native_submission_waits_for_startup(
        self,
    ) -> None:
        # Regression: cancellation after native submission but before native setup
        # must wait until stop can take effect.
        # GIVEN
        loop = asyncio.get_running_loop()
        executor_occupied = asyncio.Event()
        release_executor = threading.Event()
        native_serve_submitted = asyncio.Event()
        native_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        # Cleanups run last-in, first-out: release the blocked worker before
        # shutdown waits for the executor to drain.
        self.addCleanup(native_executor.shutdown)
        self.addCleanup(release_executor.set)
        # pyrefly: ignore [bad-specialization]
        server = ThriftServer(Handler(), ip="::1", port=0)

        def occupy_executor() -> None:
            loop.call_soon_threadsafe(executor_occupied.set)
            release_executor.wait()

        native_executor.submit(occupy_executor)
        await asyncio.wait_for(executor_occupied.wait(), timeout=5.0)
        original_run_in_executor = loop.run_in_executor

        # Preserve production's request for the loop default, but send every
        # intercepted submission to this one-worker pool. Its occupied worker
        # holds that work until after cancellation with no change to loop state.
        def observe_native_serve_submission(
            executor: concurrent.futures.Executor | None,
            native_serve: Callable[..., None],
            *args: object,
        ) -> asyncio.Future[None]:
            self.assertIsNone(executor)
            native_future = original_run_in_executor(
                native_executor, native_serve, *args
            )
            native_serve_submitted.set()
            return native_future

        # WHEN
        with mock.patch.object(
            loop,
            "run_in_executor",
            autospec=True,
            side_effect=observe_native_serve_submission,
        ):
            serve_task = asyncio.create_task(server.serve())
            try:
                await asyncio.wait_for(native_serve_submitted.wait(), timeout=5.0)
                serve_task.cancel()
                loop.call_soon(release_executor.set)
                serve_result = await asyncio.wait_for(
                    asyncio.shield(asyncio.gather(serve_task, return_exceptions=True)),
                    timeout=5.0,
                )
            finally:
                release_executor.set()
                if not serve_task.done():
                    await asyncio.wait_for(server.get_address(), timeout=5.0)
                    server.stop()
                    await asyncio.gather(serve_task, return_exceptions=True)
        actual_serve_cancelled = isinstance(serve_result[0], asyncio.CancelledError)

        # THEN
        self.assertTrue(actual_serve_cancelled)

    async def test_native_failure_during_cancellation_preserves_cancellation(
        self,
    ) -> None:
        # GIVEN
        native_failure = RuntimeError("native serve failed during cancellation")
        # pyrefly: ignore [bad-specialization]
        server = ThriftServer(Handler(), port=0)
        loop = asyncio.get_running_loop()
        native_serve: asyncio.Future[None] = loop.create_future()
        native_serve_started = asyncio.Event()
        real_run_in_executor = loop.run_in_executor
        expected_log_message = (
            "Native server execution for ThriftServer failed before shutdown completed"
        )

        def start_native_serve(
            _executor: object,
            operation: Callable[..., None],
            *args: object,
        ) -> asyncio.Future[None]:
            if operation is not server_module._run_native_serve_impl:
                return real_run_in_executor(None, operation, *args)
            native_serve_started.set()
            return native_serve

        # WHEN
        with (
            mock.patch.object(loop, "run_in_executor", side_effect=start_native_serve),
            self.assertLogs("thrift.py3.server", level="ERROR") as captured_logs,
        ):
            serve_task = asyncio.create_task(server.serve())
            await asyncio.wait_for(native_serve_started.wait(), timeout=5)
            serve_task.cancel()
            await asyncio.sleep(0)
            native_serve.set_exception(native_failure)
            serve_result = await asyncio.wait_for(
                asyncio.gather(serve_task, return_exceptions=True),
                timeout=5,
            )
        actual_serve_cancelled = isinstance(serve_result[0], asyncio.CancelledError)
        actual_log_messages = [record.getMessage() for record in captured_logs.records]

        # THEN
        self.assertTrue(actual_serve_cancelled)
        self.assertIn(expected_log_message, actual_log_messages)

    async def test_native_serve_raises_when_its_owner_died_before_it_started(
        self,
    ) -> None:
        # GIVEN
        loop = asyncio.get_running_loop()
        executor_occupied = asyncio.Event()
        release_executor = threading.Event()
        native_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        # Cleanups run last-in, first-out: release the blocked worker before
        # shutdown waits for the executor to drain.
        self.addCleanup(native_executor.shutdown)
        self.addCleanup(release_executor.set)
        loop.set_default_executor(native_executor)
        expected_served_servers: list[ThriftServer] = []
        actual_served_servers: list[ThriftServer] = []
        # pyrefly: ignore [bad-specialization]
        server = ThriftServer(Handler(), ip="::1", port=0)
        server_ref = weakref.ref(server)

        def occupy_executor() -> None:
            loop.call_soon_threadsafe(executor_occupied.set)
            release_executor.wait()

        native_executor.submit(occupy_executor)
        await asyncio.wait_for(executor_occupied.wait(), timeout=5.0)
        native_serve = server_module.run_native_serve(
            loop, actual_served_servers.append, server
        )

        # WHEN
        del server
        actual_server_died_before_start = server_ref() is None
        release_executor.set()
        with self.assertRaisesRegex(
            RuntimeError, "The serve owner died before its native serve started"
        ):
            await asyncio.wait_for(native_serve, timeout=5.0)

        # THEN
        self.assertTrue(actual_server_died_before_start)
        self.assertEqual(expected_served_servers, actual_served_servers)

    def test_context_entry_failure_releases_factory_resources(self) -> None:
        # GIVEN
        failure = RuntimeError("context entry failed")
        handler = EnterFailureHandler(failure)
        errors: list[Exception] = []
        retained_servers: list[object] = []
        expected_errors = [failure]
        expected_enter_count = 1
        expected_exit_count = 0

        async def inner() -> None:
            server = ThriftServer(handler, port=0)
            retained_servers.append(server)
            serve_task = asyncio.create_task(server.serve())
            try:
                await server.get_address()
            except RuntimeError:
                pass
            await serve_task

        def run() -> None:
            try:
                asyncio.run(inner())
            except Exception as error:
                errors.append(error)

        # WHEN
        server_runner = threading.Thread(target=run, daemon=True)
        server_runner.start()
        server_runner.join(timeout=10)
        actual_server_runner_alive = server_runner.is_alive()
        actual_errors = errors
        actual_enter_count = handler.enter_count
        actual_exit_count = handler.exit_count
        retained_servers.clear()

        # THEN
        self.assertFalse(actual_server_runner_alive)
        self.assertEqual(expected_errors, actual_errors)
        self.assertEqual(expected_enter_count, actual_enter_count)
        self.assertEqual(expected_exit_count, actual_exit_count)

    def test_context_exit_failure_releases_factory_resources(self) -> None:
        # GIVEN
        failure = RuntimeError("context exit failed")
        handler = ExitFailureHandler(failure)
        errors: list[Exception] = []
        retained_servers: list[object] = []
        expected_errors = [failure]
        expected_enter_count = 1
        expected_exit_count = 1

        async def inner() -> None:
            server = ThriftServer(handler, port=0)
            retained_servers.append(server)
            serve_task = asyncio.create_task(server.serve())
            await server.get_address()
            server.stop()
            await serve_task

        def run() -> None:
            try:
                asyncio.run(inner())
            except Exception as error:
                errors.append(error)

        # WHEN
        server_runner = threading.Thread(target=run, daemon=True)
        server_runner.start()
        server_runner.join(timeout=10)
        actual_server_runner_alive = server_runner.is_alive()
        actual_errors = errors
        actual_enter_count = handler.enter_count
        actual_exit_count = handler.exit_count
        retained_servers.clear()

        # THEN
        self.assertFalse(actual_server_runner_alive)
        self.assertEqual(expected_errors, actual_errors)
        self.assertEqual(expected_enter_count, actual_enter_count)
        self.assertEqual(expected_exit_count, actual_exit_count)

    def test_threaded_destruction(self) -> None:
        handler = Handler()

        async def inner(handler: Handler) -> None:
            await self.get_address(handler)

        server_runner = threading.Thread(
            target=lambda handler: asyncio.run(inner(handler)),
            args=(handler,),
        )
        server_runner.start()
        server_runner.join()
        self.assertTrue(handler.on_start_serving)
        self.assertTrue(handler.on_stop_requested)
        del server_runner
        gc.collect()
