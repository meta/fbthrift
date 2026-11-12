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
import threading

from cpython.ref cimport PyObject
from cython.operator cimport dereference as deref
from folly cimport cFollyExecutor
from folly.executor cimport get_executor
from libc.stddef cimport size_t
from libcpp.map cimport map as cmap
from libcpp.memory cimport make_shared, shared_ptr, static_pointer_cast
from libcpp.utility cimport move as cmove
from libcpp.vector cimport vector as cvector
from thrift.python.std_libcpp cimport bytes_to_string_view, string_view
from testing.base_service_only.thrift_clients import BaseService
from testing.base_service_only.thrift_services import BaseServiceInterface
from thrift.python.client import get_client
from thrift.python.server import ThriftServer
from thrift.py3.server cimport ThriftServer as Py3ThriftServer

from thrift.python.server_impl.async_processor cimport (
    cAsyncProcessorFactory,
    AsyncProcessorFactory,
)
from thrift.python.server_impl.python_async_processor cimport (
    cLaunchControlRequest,
    cPythonAsyncProcessorFactory,
    HandlerFunc,
    makeHandlerFunc,
    PyObjPtr,
    PythonAsyncProcessorFactory,
    WorkerSystem,
)
from thrift.python.types cimport (
    FunctionEntry,
    ServiceInterface as cServiceInterface,
)


cdef extern from "thrift/lib/python/server/test/PythonAsyncProcessorFactoryLifecycleTestHelper.h" namespace "apache::thrift::python::test":
    cdef cppclass cForwardingKeepAliveTrackingExecutor "apache::thrift::python::test::ForwardingKeepAliveTrackingExecutor":
        cForwardingKeepAliveTrackingExecutor(cFollyExecutor* delegate) except +
        size_t keepAliveCount() noexcept

    shared_ptr[cPythonAsyncProcessorFactory] createHostedTestFactory(
        PyObject* python_server,
        cmap[string_view, HandlerFunc] functions,
        cvector[PyObjPtr] lifecycle_functions,
        cForwardingKeepAliveTrackingExecutor& control_executor,
        cLaunchControlRequest launch_control_request,
    ) except +

    bint isFreeThreadedBuild() noexcept

    size_t createProcessorsWhileLegacyStopRuns(
        cForwardingKeepAliveTrackingExecutor& control_executor,
        size_t iteration_count,
    ) except +


cdef extern from "thrift/lib/cpp2/async/MultiplexAsyncProcessor.h" namespace "apache::thrift":
    cdef cppclass cMultiplexAsyncProcessorFactory "apache::thrift::MultiplexAsyncProcessorFactory"(cAsyncProcessorFactory):
        cMultiplexAsyncProcessorFactory(
            cvector[shared_ptr[cAsyncProcessorFactory]] processorFactories,
        ) except +


cdef int launchTestControlTask(PyObject* coroutine_factory) except -1:
    asyncio.get_running_loop().create_task((<object>coroutine_factory)())
    return 0


cdef class ExecutorKeepAliveProbe:
    cdef shared_ptr[cForwardingKeepAliveTrackingExecutor] executor

    def __cinit__(self):
        self.executor = make_shared[cForwardingKeepAliveTrackingExecutor](
            <cFollyExecutor*>get_executor()
        )

    def keep_alive_count(self):
        return self.executor.get().keepAliveCount()


class StopRequestedExecutorObserver:
    def __init__(self, callback, executor_probe):
        self.callback = callback
        self.executor_probe = executor_probe
        self.keep_alive_count_before_callback = None
        self.keep_alive_count_after_callback = None
        self.callback_completed = asyncio.Event()
        self.release_callback = asyncio.Event()

    async def __call__(self):
        self.keep_alive_count_before_callback = (
            self.executor_probe.keep_alive_count()
        )
        await self.callback()
        self.keep_alive_count_after_callback = (
            self.executor_probe.keep_alive_count()
        )
        self.callback_completed.set()
        await self.release_callback.wait()


class Handler(BaseServiceInterface):
    async def theAnswer(self) -> int:
        return 42


class ContextHandler(Handler):
    def __init__(self):
        self.events = []

    async def __aenter__(self):
        self.events.append("enter")
        return self

    async def __aexit__(self, *exc_info):
        self.events.append("exit")


class SuppressingContextHandler(ContextHandler):
    def __init__(self):
        super().__init__()
        self.exit_error = None

    async def __aexit__(self, exc_type, exc, traceback):
        self.events.append("exit")
        self.exit_error = exc
        return True


class RecordingContextHandler(ContextHandler):
    def __init__(self, exit_result):
        super().__init__()
        self.exit_error = None
        self._exit_result = exit_result

    async def __aexit__(self, exc_type, exc, traceback):
        self.events.append("exit")
        self.exit_error = exc
        return self._exit_result


class FailingTableHandler(Handler):
    def getFunctionTable(self):
        raise RuntimeError("no function table")


class MissingMethodHandler(Handler):
    def getFunctionTable(self):
        return {}


class ThreadRecordingHandler(Handler):
    def __init__(self):
        self.request_threads = []

    async def theAnswer(self) -> int:
        self.request_threads.append(threading.current_thread())
        return 42


class NoopContextPythonAsyncProcessorFactory(PythonAsyncProcessorFactory):
    def __init__(self):
        self.context_enter_count = 0
        self.context_exit_count = 0

    async def __aenter__(self):
        self.context_enter_count += 1
        return self

    async def __aexit__(self, *exc_info):
        self.context_exit_count += 1


class HostedLifecycleHandler(Handler):
    def __init__(self):
        self.context_events = []
        self.service_events = []
        self.stop_requested = asyncio.Event()
        self.context_exit_started = asyncio.Event()
        self.release_context_exit = asyncio.Event()

    async def __aenter__(self):
        self.context_events.append("enter")
        return self

    async def __aexit__(self, *exc_info):
        self.context_events.append("exit")
        self.context_exit_started.set()
        await self.release_context_exit.wait()

    async def onStartServing(self):
        self.service_events.append("start")

    async def onStopRequested(self):
        self.service_events.append("stop")
        self.stop_requested.set()


class FailingExitHandler(Handler):
    def __init__(self, failure):
        self.failure = failure

    async def __aexit__(self, *exc_info):
        raise self.failure


async def fail_worker_start():
    raise RuntimeError("worker start failed")


cdef PythonAsyncProcessorFactory create_noop_context_factory(
    PythonAsyncProcessorFactory factory,
):
    cdef PythonAsyncProcessorFactory noop_context_factory = (
        NoopContextPythonAsyncProcessorFactory()
    )
    noop_context_factory._cpp_obj = factory._cpp_obj
    return noop_context_factory


cdef PythonAsyncProcessorFactory create_hosted_factory(
    cServiceInterface handler,
    ExecutorKeepAliveProbe executor_probe,
    object on_start_serving,
    object on_stop_requested,
):
    cdef dict function_map = handler.getFunctionTable()
    cdef cmap[string_view, HandlerFunc] cpp_functions
    cdef FunctionEntry entry
    cdef string_view name_view
    for name, entry in function_map.items():
        name_view = bytes_to_string_view(name)
        cpp_functions[name_view] = makeHandlerFunc(
            entry.rpc_kind,
            <PyObject*>entry.handler,
            <bytes>handler.service_name(),
            name_view,
        )

    cdef list lifecycle_functions = [on_start_serving, on_stop_requested]
    cdef cvector[PyObjPtr] cpp_lifecycle_functions
    cdef object lifecycle_function
    for lifecycle_function in lifecycle_functions:
        cpp_lifecycle_functions.push_back(<PyObject*>lifecycle_function)

    cdef PythonAsyncProcessorFactory factory = (
        PythonAsyncProcessorFactory.__new__(PythonAsyncProcessorFactory)
    )
    factory.funcMap = function_map
    factory.lifecycleFuncs = lifecycle_functions
    factory.handler = handler
    factory._cpp_obj = static_pointer_cast[
        cAsyncProcessorFactory,
        cPythonAsyncProcessorFactory,
    ](
        createHostedTestFactory(
            <PyObject*>handler,
            cmove(cpp_functions),
            cmove(cpp_lifecycle_functions),
            deref(executor_probe.executor),
            launchTestControlTask,
        )
    )
    return factory


cdef AsyncProcessorFactory compose_processor_factory(
    PythonAsyncProcessorFactory factory,
):
    cdef shared_ptr[cAsyncProcessorFactory] inner_factory = factory._cpp_obj
    cdef cvector[shared_ptr[cAsyncProcessorFactory]] factories
    cdef AsyncProcessorFactory composed_factory = (
        AsyncProcessorFactory.__new__(AsyncProcessorFactory)
    )
    factories.push_back(inner_factory)
    composed_factory._cpp_obj = static_pointer_cast[
        cAsyncProcessorFactory,
        cMultiplexAsyncProcessorFactory,
    ](
        make_shared[cMultiplexAsyncProcessorFactory](cmove(factories))
    )
    return composed_factory


async def round_trip(AsyncProcessorFactory factory):
    server = ThriftServer(factory, ip="::1")
    serve_task = asyncio.create_task(server.serve())
    try:
        address = await asyncio.wait_for(server.get_address(), timeout=5.0)
        assert address.ip is not None
        assert address.port is not None
        async with get_client(
            BaseService,
            host=str(address.ip),
            port=address.port,
        ) as client:
            response = await asyncio.wait_for(client.theAnswer(), timeout=5.0)
        server.stop()
        await asyncio.wait_for(serve_task, timeout=5.0)
        return response
    except BaseException:
        server.stop()
        await asyncio.gather(serve_task, return_exceptions=True)
        raise


cdef class PythonAsyncProcessorFactoryCustomerApiCTest:
    """Exercises Cython APIs that customers call directly."""

    def __cinit__(self, object unit_test):
        self.ut = unit_test

    def test_processor_creation_and_legacy_stop_overlap(self):
        # GIVEN
        executor_probe = ExecutorKeepAliveProbe()
        expected_processor_count = 1000

        # WHEN
        actual_processor_count = createProcessorsWhileLegacyStopRuns(
            deref(executor_probe.executor),
            expected_processor_count,
        )

        # THEN
        self.ut.assertEqual(expected_processor_count, actual_processor_count)

    def test_set_cpu_worker_threads_rejects_count_below_one(self):
        # GIVEN
        cdef PythonAsyncProcessorFactory factory = PythonAsyncProcessorFactory.create(
            Handler()
        )
        expected_message = "Thread count must be one or more; got 0."

        # WHEN
        with self.ut.assertRaises(ValueError) as raised:
            factory.set_cpu_worker_threads(0)

        # THEN
        self.ut.assertEqual(expected_message, str(raised.exception))

    def test_get_cpu_worker_threads_reports_the_cpp_setting_without_a_handler(self):
        # GIVEN
        server = ThriftServer(None, ip="::1")
        (<Py3ThriftServer>server).server.get().setNumCPUWorkerThreads(42)
        expected_count = 42

        # WHEN
        actual_count = server.get_cpu_worker_threads()

        # THEN
        self.ut.assertEqual(expected_count, actual_count)

    async def test_threads_without_factory_context_warn_and_stay_single_threaded(
        self,
    ):
        # GIVEN
        handler = ThreadRecordingHandler()
        cdef PythonAsyncProcessorFactory factory = PythonAsyncProcessorFactory.create(
            handler
        )
        factory.set_cpu_worker_threads(2)
        server = ThriftServer(create_noop_context_factory(factory), ip="::1")
        expected_response = 42
        expected_request_threads = [threading.current_thread()]
        expected_log_output = [
            "WARNING:thrift.python.server_impl.python_async_processor:"
            "workers set and context not entered. "
            "Server will stay in single thread mode"
        ]

        # WHEN
        with self.ut.assertLogs(
            "thrift.python.server_impl.python_async_processor", level="WARNING"
        ) as captured:
            serve_task = asyncio.create_task(server.serve())
            address = await asyncio.wait_for(server.get_address(), timeout=5.0)
            async with get_client(
                BaseService,
                host=str(address.ip),
                port=address.port,
            ) as client:
                actual_response = await asyncio.wait_for(
                    client.theAnswer(),
                    timeout=5.0,
                )
            server.stop()
            await asyncio.wait_for(serve_task, timeout=5.0)

        # THEN
        self.ut.assertEqual(expected_response, actual_response)
        self.ut.assertEqual(expected_request_threads, handler.request_threads)
        self.ut.assertEqual(expected_log_output, captured.output)

    async def test_threads_with_factory_context_serve_requests_on_workers(self):
        # GIVEN
        handler = ThreadRecordingHandler()
        cdef PythonAsyncProcessorFactory factory = PythonAsyncProcessorFactory.create(
            handler
        )
        factory.set_cpu_worker_threads(2)
        server = ThriftServer(factory, ip="::1")
        forbidden_worker_thread = threading.current_thread()
        expected_responses = [42]
        expected_request_count = 1
        serve_task = asyncio.create_task(server.serve())
        address = await asyncio.wait_for(server.get_address(), timeout=5.0)
        responses = []
        request_done = threading.Event()

        async def ask_from_another_thread():
            async with get_client(
                BaseService,
                host=str(address.ip),
                port=address.port,
            ) as client:
                responses.append(
                    await asyncio.wait_for(client.theAnswer(), timeout=5.0)
                )

        def run_client_thread():
            try:
                asyncio.run(ask_from_another_thread())
            finally:
                request_done.set()

        client_thread = threading.Thread(target=run_client_thread)

        # WHEN
        client_thread.start()
        request_done.wait(timeout=10.0)
        client_thread.join()
        server.stop()
        await asyncio.wait_for(serve_task, timeout=5.0)

        # THEN
        self.ut.assertEqual(expected_responses, responses)
        self.ut.assertEqual(expected_request_count, len(handler.request_threads))
        self.ut.assertNotIn(forbidden_worker_thread, handler.request_threads)

    async def test_host_without_factory_context_uses_legacy_stop_fallback(self):
        # GIVEN
        handler = HostedLifecycleHandler()
        executor_probe = ExecutorKeepAliveProbe()
        stop_observer = StopRequestedExecutorObserver(
            handler.onStopRequested,
            executor_probe,
        )
        cdef PythonAsyncProcessorFactory factory = create_hosted_factory(
            <cServiceInterface>handler,
            executor_probe,
            handler.onStartServing,
            stop_observer,
        )
        noop_context_factory = create_noop_context_factory(factory)
        server = ThriftServer(noop_context_factory, ip="::1")
        serve_task = None
        expected_context_enter_count = 1
        expected_context_exit_count = 1
        expected_context_events = []
        expected_service_events = ["start", "stop"]
        expected_response = 42
        # Only a free-threaded build has the stop-time fallback, so only there is
        # the executor released before releaseOwnedResources().
        expected_executor_released_after_serve = isFreeThreadedBuild()

        # WHEN
        try:
            serve_task = asyncio.create_task(server.serve())
            address = await asyncio.wait_for(server.get_address(), timeout=5.0)
            self.ut.assertIsNotNone(address.ip)
            self.ut.assertIsNotNone(address.port)
            actual_keep_alive_count_before_stop = executor_probe.keep_alive_count()
            async with get_client(
                BaseService,
                host=str(address.ip),
                port=address.port,
            ) as client:
                actual_initial_response = await asyncio.wait_for(
                    client.theAnswer(),
                    timeout=5.0,
                )
                server.stop()
                await asyncio.wait_for(
                    stop_observer.callback_completed.wait(),
                    timeout=5.0,
                )
                actual_response_during_stop = await asyncio.wait_for(
                    client.theAnswer(),
                    timeout=5.0,
                )
                stop_observer.release_callback.set()
            await asyncio.wait_for(serve_task, timeout=5.0)
            actual_executor_released_after_serve = (
                executor_probe.keep_alive_count() == 0
            )
            actual_serve_completed = serve_task.done()
        finally:
            stop_observer.release_callback.set()
            server.stop()
            if serve_task is not None:
                await asyncio.gather(serve_task, return_exceptions=True)
            factory.releaseOwnedResources()
        actual_context_enter_count = noop_context_factory.context_enter_count
        actual_context_exit_count = noop_context_factory.context_exit_count
        actual_keep_alive_count_before_callback = (
            stop_observer.keep_alive_count_before_callback
        )
        actual_keep_alive_count_after_callback = (
            stop_observer.keep_alive_count_after_callback
        )
        actual_callback_count_stable = (
            actual_keep_alive_count_before_callback
            == actual_keep_alive_count_after_callback
        )
        actual_context_events = handler.context_events
        actual_service_events = handler.service_events

        # THEN
        self.ut.assertEqual(expected_context_enter_count, actual_context_enter_count)
        self.ut.assertEqual(expected_context_exit_count, actual_context_exit_count)
        self.ut.assertEqual(expected_response, actual_initial_response)
        self.ut.assertEqual(expected_response, actual_response_during_stop)
        # Internal token copies determine the exact positive count. The contract
        # distinguishes executor retention from complete release at zero.
        self.ut.assertGreater(actual_keep_alive_count_before_stop, 0)
        self.ut.assertIsNotNone(actual_keep_alive_count_before_callback)
        self.ut.assertIsNotNone(actual_keep_alive_count_after_callback)
        self.ut.assertTrue(actual_callback_count_stable)
        self.ut.assertEqual(
            expected_executor_released_after_serve,
            actual_executor_released_after_serve,
        )
        self.ut.assertTrue(actual_serve_completed)
        self.ut.assertEqual(expected_context_events, actual_context_events)
        self.ut.assertEqual(expected_service_events, actual_service_events)

    async def test_host_with_factory_context_defers_resources_to_exit(self):
        # GIVEN
        handler = HostedLifecycleHandler()
        executor_probe = ExecutorKeepAliveProbe()
        stop_observer = StopRequestedExecutorObserver(
            handler.onStopRequested,
            executor_probe,
        )
        cdef PythonAsyncProcessorFactory factory = create_hosted_factory(
            <cServiceInterface>handler,
            executor_probe,
            handler.onStartServing,
            stop_observer,
        )
        server = ThriftServer(factory, ip="::1")
        serve_task = None
        expected_context_events = ["enter", "exit"]
        expected_service_events = ["start", "stop"]
        expected_response = 42
        expected_released_keep_alive_count = 0

        # WHEN
        try:
            serve_task = asyncio.create_task(server.serve())
            address = await asyncio.wait_for(server.get_address(), timeout=5.0)
            self.ut.assertIsNotNone(address.ip)
            self.ut.assertIsNotNone(address.port)
            actual_keep_alive_count_after_entry = executor_probe.keep_alive_count()
            async with get_client(
                BaseService,
                host=str(address.ip),
                port=address.port,
            ) as client:
                actual_initial_response = await asyncio.wait_for(
                    client.theAnswer(),
                    timeout=5.0,
                )
                server.stop()
                await asyncio.wait_for(
                    stop_observer.callback_completed.wait(),
                    timeout=5.0,
                )
                actual_response_during_stop = await asyncio.wait_for(
                    client.theAnswer(),
                    timeout=5.0,
                )
                stop_observer.release_callback.set()
            await asyncio.wait_for(handler.context_exit_started.wait(), timeout=5.0)
            actual_keep_alive_count_during_exit = executor_probe.keep_alive_count()
            handler.release_context_exit.set()
            await asyncio.wait_for(serve_task, timeout=5.0)
            actual_keep_alive_count_after_exit = executor_probe.keep_alive_count()
            actual_serve_completed = serve_task.done()
        finally:
            stop_observer.release_callback.set()
            handler.release_context_exit.set()
            server.stop()
            if serve_task is not None:
                await asyncio.gather(serve_task, return_exceptions=True)
            factory.releaseOwnedResources()
        actual_keep_alive_count_before_callback = (
            stop_observer.keep_alive_count_before_callback
        )
        actual_keep_alive_count_after_callback = (
            stop_observer.keep_alive_count_after_callback
        )
        actual_context_events = handler.context_events
        actual_service_events = handler.service_events

        # THEN
        self.ut.assertEqual(expected_response, actual_initial_response)
        self.ut.assertEqual(expected_response, actual_response_during_stop)
        self.ut.assertGreater(actual_keep_alive_count_after_entry, 0)
        self.ut.assertIsNotNone(actual_keep_alive_count_before_callback)
        self.ut.assertGreater(actual_keep_alive_count_before_callback, 0)
        self.ut.assertIsNotNone(actual_keep_alive_count_after_callback)
        self.ut.assertGreater(actual_keep_alive_count_after_callback, 0)
        self.ut.assertGreater(actual_keep_alive_count_during_exit, 0)
        self.ut.assertEqual(
            expected_released_keep_alive_count,
            actual_keep_alive_count_after_exit,
        )
        self.ut.assertTrue(actual_serve_completed)
        self.ut.assertEqual(expected_context_events, actual_context_events)
        self.ut.assertEqual(expected_service_events, actual_service_events)

    async def test_factory_context_composes_handler_context(self):
        # GIVEN
        handler = ContextHandler()
        cdef PythonAsyncProcessorFactory factory = (
            PythonAsyncProcessorFactory.create(<cServiceInterface>handler)
        )
        expected = ["enter", "inside", "exit"]

        # WHEN
        async with factory:
            handler.events.append("inside")

        # THEN
        self.ut.assertEqual(expected, handler.events)

    async def test_handler_that_returns_true_still_suppresses_the_callers_exception(
        self,
    ):
        # GIVEN
        handler = SuppressingContextHandler()
        cdef PythonAsyncProcessorFactory factory = (
            PythonAsyncProcessorFactory.create(<cServiceInterface>handler)
        )
        caller_failure = ValueError("caller failed")
        expected_events = ["enter", "exit"]

        # WHEN
        async with factory:
            raise caller_failure

        # THEN
        self.ut.assertEqual(expected_events, handler.events)
        self.ut.assertIs(caller_failure, handler.exit_error)

    async def test_unary_rpc_round_trips(self):
        # GIVEN
        cdef cServiceInterface handler = Handler()
        cdef PythonAsyncProcessorFactory factory = (
            PythonAsyncProcessorFactory.create(handler)
        )
        expected = 42

        # WHEN
        actual = await round_trip(factory)

        # THEN
        self.ut.assertEqual(expected, actual)

    async def test_unary_rpc_round_trips_through_composed_factory(self):
        # GIVEN
        cdef cServiceInterface handler = Handler()
        cdef PythonAsyncProcessorFactory factory = (
            PythonAsyncProcessorFactory.create(handler)
        )
        composed_factory = compose_processor_factory(factory)
        expected = 42

        # WHEN
        async with factory:
            actual = await round_trip(composed_factory)

        # THEN
        self.ut.assertEqual(expected, actual)

    async def test_failed_worker_start_propagates_even_when_the_control_handler_suppresses(self):
        # GIVEN
        control_handler = SuppressingContextHandler()
        products = [control_handler]

        def make_handler():
            return products.pop() if products else FailingTableHandler()

        cdef PythonAsyncProcessorFactory factory = (
            PythonAsyncProcessorFactory.create_from_handler_factory(make_handler)
        )
        factory.set_cpu_worker_threads(2)
        expected_events = ["enter", "exit"]

        # WHEN
        with self.ut.assertRaisesRegex(
            RuntimeError, "^no function table$"
        ) as raised:
            async with factory:
                pass

        # THEN
        self.ut.assertEqual(expected_events, control_handler.events)
        self.ut.assertIs(raised.exception, control_handler.exit_error)

    async def test_worker_handler_that_lacks_a_routed_method_fails_the_start(self):
        # GIVEN
        control_handler = ContextHandler()
        products = [control_handler]

        def make_handler():
            return products.pop() if products else MissingMethodHandler()

        cdef PythonAsyncProcessorFactory factory = (
            PythonAsyncProcessorFactory.create_from_handler_factory(make_handler)
        )
        factory.set_cpu_worker_threads(2)
        expected_events = ["enter", "exit"]

        # WHEN
        with self.ut.assertRaisesRegex(
            TypeError,
            "^Handler factory returned MissingMethodHandler for a worker; it "
            "lacks routed methods: theAnswer\\.$",
        ):
            async with factory:
                pass

        # THEN
        self.ut.assertEqual(expected_events, control_handler.events)

    async def test_worker_product_that_is_not_a_service_interface_fails_the_start(self):
        # GIVEN
        control_handler = ContextHandler()
        products = [control_handler]

        def make_handler():
            return products.pop() if products else object()

        cdef PythonAsyncProcessorFactory factory = (
            PythonAsyncProcessorFactory.create_from_handler_factory(make_handler)
        )
        factory.set_cpu_worker_threads(2)
        expected_events = ["enter", "exit"]

        # WHEN
        with self.ut.assertRaisesRegex(
            TypeError,
            "^Handler factory returned object; expected a thrift-python "
            "ServiceInterface\\.$",
        ):
            async with factory:
                pass

        # THEN
        self.ut.assertEqual(expected_events, control_handler.events)

    async def test_worker_shutdown_failure_propagates_and_the_control_handler_exits(
        self, exit_result
    ):
        # GIVEN
        control_handler = RecordingContextHandler(exit_result)
        worker_exit_failure = RuntimeError("worker handler exit failed")
        products = [FailingExitHandler(worker_exit_failure), control_handler]

        def make_handler():
            return products.pop() if products else Handler()

        cdef PythonAsyncProcessorFactory factory = (
            PythonAsyncProcessorFactory.create_from_handler_factory(make_handler)
        )
        factory.set_cpu_worker_threads(2)
        expected_events = ["enter", "exit"]

        # WHEN
        with self.ut.assertRaises(RuntimeError) as raised:
            async with factory:
                pass

        # THEN
        self.ut.assertIs(worker_exit_failure, raised.exception)
        self.ut.assertEqual(expected_events, control_handler.events)
        self.ut.assertIs(worker_exit_failure, control_handler.exit_error)

    async def test_failed_worker_start_releases_resources_and_propagates_the_handler_exit_failure(
        self,
    ):
        # GIVEN
        handler_exit_failure = RuntimeError("handler exit failed")
        handler = FailingExitHandler(handler_exit_failure)
        executor_probe = ExecutorKeepAliveProbe()
        cdef PythonAsyncProcessorFactory factory = create_hosted_factory(
            <cServiceInterface>handler,
            executor_probe,
            handler.onStartServing,
            handler.onStopRequested,
        )
        factory.workerSystem = WorkerSystem.create(
            get_executor(), factory.funcMap, {b"theAnswer"}
        )
        factory.initializerFactory = lambda worker_system: fail_worker_start
        factory.set_cpu_worker_threads(2)
        expected_released_keep_alive_count = 0

        # WHEN
        try:
            with self.ut.assertRaises(RuntimeError) as raised:
                async with factory:
                    pass
            actual_keep_alive_count = executor_probe.keep_alive_count()
        finally:
            factory.releaseOwnedResources()

        # THEN
        self.ut.assertIs(handler_exit_failure, raised.exception)
        self.ut.assertEqual(
            expected_released_keep_alive_count,
            actual_keep_alive_count,
        )
