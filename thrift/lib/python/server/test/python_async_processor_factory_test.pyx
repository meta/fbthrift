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

import asyncio
import functools
import threading

from cython.operator cimport dereference as deref
from libcpp.memory cimport shared_ptr
from libcpp.utility cimport move as cmove
from folly.executor cimport get_executor
from thrift.python.server_impl.python_async_processor cimport (
    cLaunchControlRequest,
    makeControlOnlyExecutionSystem,
    makeHandlerFunc,
    WorkerSystem,
)
from thrift.python.server_impl.python_async_processor import (
    PerLoopWorkerInitializer,
    SharedWorkerInitializer,
)
from thrift.python.std_libcpp cimport bytes_to_string_view
from thrift.python.types import FunctionEntry, ServiceInterface


cdef extern from "thrift/lib/python/server/test/WorkerInitializerTestHelper.h" namespace "apache::thrift::python::test":
    void startSelectedFunctionForTest(
        cExecutionSystem& executionSystem, string method
    ) except +


WORKER_METHOD = b"worker_method"


cdef int launchControlTask(object coroutine_factory) except -1:
    asyncio.get_running_loop().create_task(coroutine_factory())
    return 0


cdef cCreateMethodMetadataResult create(bytes function_name, RpcKind rpc_kind):
    cdef cmap[string_view, HandlerFunc] funcs
    cdef cmap[string, PyObjPtr] controlMethods
    cdef cvector[PyObjPtr] lifecycle
    cdef object server = None
    cdef string serviceName
    cdef string_view function_name_view = bytes_to_string_view(function_name)

    funcs[function_name_view] = makeHandlerFunc(
        rpc_kind,
        <PyObject*>None,
        serviceName,
        function_name_view,
    )
    controlMethods[function_name] = <PyObject*>None

    cdef cAsyncioExecutor* controlExecutor = get_executor()
    cdef shared_ptr[cExecutionSystem] executionSystem = makeControlOnlyExecutionSystem(
        controlExecutor, <cLaunchControlRequest>launchControlTask, cmove(controlMethods)
    )
    cdef shared_ptr[cPythonAsyncProcessorFactory] obj = cCreatePythonAsyncProcessorFactory(
        <PyObject*>server,
        cmove(funcs),
        cmove(lifecycle),
        executionSystem,
        controlExecutor,
        serviceName
    )
    return deref(obj).createMethodMetadata()


class RecordingHandler(ServiceInterface):
    def __init__(self, served):
        self.created_on = threading.get_ident()
        self.events = []
        self.threads = set()
        self.exit_error = None
        self._served = served

    def _record(self, event):
        self.events.append(event)
        self.threads.add(threading.get_ident())

    async def __aenter__(self):
        self._record("enter")
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        self.exit_error = exc
        self._record("exit")

    def getFunctionTable(self):
        return {
            WORKER_METHOD: FunctionEntry(
                RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE, self.serve
            )
        }

    async def serve(self):
        self._record("request")
        self._served.release()


class FailingTableHandler(RecordingHandler):
    def __init__(self, served, exit_result):
        super().__init__(served)
        self._exit_result = exit_result

    def getFunctionTable(self):
        raise RuntimeError("no function table")

    async def __aexit__(self, exc_type, exc, traceback):
        await super().__aexit__(exc_type, exc, traceback)
        return self._exit_result


async def wait_for_requests(served, count):
    return await asyncio.to_thread(
        lambda: all(served.acquire(timeout=10) for _ in range(count))
    )


cdef start_request(WorkerSystem worker_system, bytes method):
    startSelectedFunctionForTest(deref(worker_system.executionSystem.get()), method)


cdef class PythonAsyncProcessorFactoryCTest:
    def __cinit__(self, object unit_test):
        self.ut = unit_test

    def test_create_method_metadata(self, bytes function_name, RpcKind rpc_kind, string expected) -> None:
        create_method_metadata_result = create(function_name, rpc_kind)
        actual = cAsyncProcessorFactory.describe(create_method_metadata_result)
        self.ut.maxDiff = None
        self.ut.assertEqual(expected, actual)

    def test_make_handler_func_full_name(
        self,
        bytes service_name,
        bytes function_name,
        RpcKind rpc_kind,
        string expected,
    ) -> None:
        cdef string service_name_string = service_name
        cdef string_view function_name_view = bytes_to_string_view(function_name)

        handler_func = makeHandlerFunc(
            rpc_kind,
            <PyObject*>None,
            service_name_string,
            function_name_view,
        )
        self.ut.assertEqual(expected, handler_func.fullName)

    async def test_workers_serve_requests_from_a_shared_table(self):
        # GIVEN
        forbidden_worker_thread = threading.get_ident()
        served = threading.Semaphore(0)
        shared_handler = RecordingHandler(served)
        function_table = shared_handler.getFunctionTable()
        expected_events = ["request", "request"]
        expected_worker_thread_count = 2
        cdef WorkerSystem worker_system = WorkerSystem.create(
            get_executor(), function_table, {WORKER_METHOD}
        )

        # WHEN
        try:
            worker_system.start(
                2, functools.partial(SharedWorkerInitializer, funcMap=function_table)
            )
            # Each worker publishes one capacity slot, so the two requests go to
            # the two workers in turn.
            start_request(worker_system, WORKER_METHOD)
            start_request(worker_system, WORKER_METHOD)
            served_in_time = await wait_for_requests(served, 2)
        finally:
            worker_system.close()

        # THEN
        self.ut.assertTrue(served_in_time)
        self.ut.assertEqual(expected_events, shared_handler.events)
        self.ut.assertEqual(expected_worker_thread_count, len(shared_handler.threads))
        self.ut.assertNotIn(forbidden_worker_thread, shared_handler.threads)

    async def test_each_worker_serves_requests_with_its_own_handler(self):
        # GIVEN
        forbidden_worker_thread = threading.get_ident()
        served = threading.Semaphore(0)
        handlers = []
        control_table = RecordingHandler(served).getFunctionTable()
        expected_events = [["enter", "request", "exit"], ["enter", "request", "exit"]]
        expected_worker_thread_count = 2

        def make_handler():
            handler = RecordingHandler(served)
            handlers.append(handler)
            return handler

        cdef WorkerSystem worker_system = WorkerSystem.create(
            get_executor(), control_table, {WORKER_METHOD}
        )

        # WHEN
        try:
            worker_system.start(
                2, functools.partial(PerLoopWorkerInitializer, factory=make_handler)
            )
            # Each worker publishes one capacity slot, so the two requests go to
            # the two workers in turn.
            start_request(worker_system, WORKER_METHOD)
            start_request(worker_system, WORKER_METHOD)
            served_in_time = await wait_for_requests(served, 2)
        finally:
            worker_system.close()

        # THEN
        creation_threads = [handler.created_on for handler in handlers]
        self.ut.assertTrue(served_in_time)
        self.ut.assertEqual(expected_events, [handler.events for handler in handlers])
        # Each handler does everything on the worker thread that created it.
        self.ut.assertEqual(
            [{thread} for thread in creation_threads],
            [handler.threads for handler in handlers],
        )
        self.ut.assertEqual(expected_worker_thread_count, len(set(creation_threads)))
        self.ut.assertNotIn(forbidden_worker_thread, creation_threads)

    async def test_failed_worker_start_propagates_and_the_worker_handler_exits(
        self, exit_result
    ):
        # GIVEN
        served = threading.Semaphore(0)
        handler = FailingTableHandler(served, exit_result)
        control_table = RecordingHandler(served).getFunctionTable()
        expected_events = ["enter", "exit"]
        cdef WorkerSystem worker_system = WorkerSystem.create(
            get_executor(), control_table, {WORKER_METHOD}
        )

        # WHEN
        try:
            with self.ut.assertRaisesRegex(
                RuntimeError, "^no function table$"
            ) as failure:
                worker_system.start(
                    1,
                    functools.partial(PerLoopWorkerInitializer, factory=lambda: handler),
                )
        finally:
            worker_system.close()

        # THEN
        self.ut.assertEqual(expected_events, handler.events)
        self.ut.assertIs(failure.exception, handler.exit_error)
