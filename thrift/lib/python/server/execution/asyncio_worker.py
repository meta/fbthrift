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
import inspect
import threading
from collections.abc import Awaitable, Callable
from typing import Protocol, TypeVar


_T = TypeVar("_T")


class AsyncioWorkerCleanup(Protocol):
    async def aclose(self) -> None: ...


AsyncioWorkerInitializationResult = AsyncioWorkerCleanup | None
AsyncioWorkerInitializer = Callable[
    [],
    AsyncioWorkerInitializationResult | Awaitable[AsyncioWorkerInitializationResult],
]


async def _await_initialization(
    initialization: Awaitable[AsyncioWorkerInitializationResult],
) -> AsyncioWorkerInitializationResult:
    return await initialization


def launch_worker_task(
    release_lease: Callable[[], None],
    coroutine_factory: Callable[[], Awaitable[_T]],
) -> asyncio.Task[_T]:
    async def run() -> _T:
        # While the request's first step runs, the scheduler must not assign
        # this worker more work. Release the lease on the loop's next
        # iteration, once the request first suspends or finishes, so the worker
        # can take new work while this request waits.
        asyncio.get_running_loop().call_soon(release_lease)
        return await coroutine_factory()

    coroutine = run()
    task = asyncio.get_running_loop().create_task(coroutine)
    # The done callback covers a task that ends before its first step runs,
    # such as one cancelled early. A second release does nothing, because the
    # releaser only drops its lease reference.
    task.add_done_callback(lambda _completed_task: release_lease())
    return task


class AsyncioWorker:
    def __init__(self, initializer: AsyncioWorkerInitializer) -> None:
        self.__initializer = initializer
        self.__initialization_result: AsyncioWorkerInitializationResult = None
        self.__loop: asyncio.AbstractEventLoop | None = None
        self.__initialization_exception: BaseException | None = None
        self.__cleanup_exception: BaseException | None = None
        self.__ready = threading.Event()
        self.__close_requested = False
        self.__thread = threading.Thread(target=self.__run)

    def start(self) -> None:
        self.__thread.start()
        self.__ready.wait()
        initialization_exception = self.__initialization_exception
        if initialization_exception is not None:
            self.__thread.join()
            raise initialization_exception

    def close(self) -> None:
        loop = self.__loop
        assert loop is not None
        self.__close_requested = True
        loop.call_soon_threadsafe(loop.stop)
        self.__thread.join()
        cleanup_exception = self.__cleanup_exception
        self.__cleanup_exception = None
        if cleanup_exception is not None:
            raise cleanup_exception

    def is_alive(self) -> bool:
        return self.__thread.is_alive()

    def __run(self) -> None:
        try:
            loop = asyncio.new_event_loop()
        except BaseException as exception:  # noqa: B036 - publish startup failure
            self.__initialization_exception = exception
            self.__ready.set()
            return

        try:
            self.__run_event_loop(loop)
        finally:
            try:
                self.__close_initialization_result(loop)
            finally:
                self.__close_event_loop(loop)

    def __run_event_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self.__loop = loop
        asyncio.set_event_loop(loop)
        loop.call_soon(self.__initialize)
        loop.run_forever()

    def __close_initialization_result(self, loop: asyncio.AbstractEventLoop) -> None:
        try:
            initialization_result = self.__initialization_result
            self.__initialization_result = None
            if self.__close_requested and initialization_result is not None:
                loop.run_until_complete(initialization_result.aclose())
        except BaseException as exception:  # noqa: B036 - report in close
            self.__cleanup_exception = exception

    def __close_event_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        try:
            asyncio.set_event_loop(None)
        finally:
            loop.close()

    def __initialize(self) -> None:
        try:
            initialization_result = self.__initializer()
            if inspect.isawaitable(initialization_result):
                loop = self.__loop
                assert loop is not None
                # create_task() requires a coroutine, but an initializer may
                # return any awaitable.
                initialization_task = loop.create_task(
                    _await_initialization(initialization_result)
                )
                initialization_task.add_done_callback(
                    self.__finish_async_initialization
                )
                return
        except BaseException as exception:  # noqa: B036 - propagate across threads
            self.__fail_initialization(exception)
            return

        self.__initialization_result = initialization_result
        self.__ready.set()

    def __finish_async_initialization(
        self, task: asyncio.Future[AsyncioWorkerInitializationResult]
    ) -> None:
        try:
            self.__initialization_result = task.result()
        except BaseException as exception:  # noqa: B036 - propagate across threads
            self.__fail_initialization(exception)
            return
        self.__ready.set()

    def __fail_initialization(self, exception: BaseException) -> None:
        self.__initialization_exception = exception
        loop = self.__loop
        assert loop is not None
        loop.stop()
        self.__ready.set()
