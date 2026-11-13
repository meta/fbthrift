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
import contextlib
import functools
import logging
import sys
import traceback

from thrift.python.serializer import serialize_iobuf
from thrift.python.types import ServiceInterface

cimport cython
from cpython.ref cimport PyObject
from cython.operator cimport dereference
from libcpp.map cimport map as cmap
from libcpp.memory cimport make_unique, make_shared, static_pointer_cast
from libcpp.optional cimport optional
from libcpp.pair cimport pair
from libcpp.unordered_set cimport unordered_set
from libcpp.utility cimport move as cmove
from libcpp.vector cimport vector as cvector

from folly cimport cFollyPromise, cFollyUnit, c_unit, cFollyExceptionWrapper, cFollyExecutor
from folly.executor cimport get_executor
from folly.iobuf cimport IOBuf, from_unique_ptr
from libcpp.string cimport string
from thrift.python.exceptions cimport (
    ApplicationError,
    ApplicationOverloadError,
    cTApplicationException,
    cTApplicationExceptionType__UNKNOWN,
)

cdef extern from "thrift/lib/python/server/TrustedServerExceptionUtil.h" namespace "apache::thrift::python":
    cdef cFollyExceptionWrapper makeAppOverloadExceptionWrapper(
        const string& message
    ) except +
from thrift.python.server_impl.request_context cimport (
    Cpp2RequestContext,
    handleAddressCallback,
    RequestContext,
    THRIFT_REQUEST_CONTEXT,
)
from thrift.python.server_impl.request_context import (
    RequestContext,
    SocketAddress,
)
from thrift.python.server_impl.execution.asyncio_worker import (
    launch_worker_task,
)
from thrift.python.server_impl.execution.asyncio_worker_pool import (
    AsyncioWorkerPool,
)

cdef extern from "thrift/lib/python/server/PythonAsyncProcessor.h" namespace "apache::thrift::python":
    void installInteractionTileFromHandler(
        Cpp2RequestContext* context,
        PyObject* instance,
        cFollyExecutor* executor,
    ) except +
from thrift.py3.stream cimport (
    cServerStream,
    cResponseAndServerStream,
    createResponseAndServerStream,
    createAsyncIteratorFromPyIterator,
    ServerStream
)
from thrift.python.types cimport ServiceInterface as cServiceInterface, FunctionEntry
from thrift.python.protocol cimport Protocol
from thrift.python.std_libcpp cimport bytes_to_string_view
from thrift.python.streaming.py_promise cimport (
    Promise_IOBuf,
    Promise_Py,
    genNextStreamValue,
)
from thrift.python.streaming.python_user_exception cimport (
    cPythonUserException,
    PythonUserException,
)
from thrift.python.streaming.sink cimport (
    createResponseAndSinkConsumer,
    cResponseAndSinkConsumer,
    cSinkConsumer,
    makeIOBufSinkConsumer,
)
from thrift.python.streaming.bidistream cimport (
    cStreamTransformation,
    cResponseAndStreamTransformation,
    createIOBufStreamTransformation,
    createResponseAndStreamTransformation,
)
from cpython.contextvars cimport PyContextVar_Set, PyContextVar_Reset

logger: logging.Logger = logging.getLogger(__name__)


ctypedef unique_ptr[cIOBuf] UniqueIOBuf
ctypedef cResponseAndServerStream[UniqueIOBuf, UniqueIOBuf] StreamResponse
ctypedef cResponseAndSinkConsumer[UniqueIOBuf, UniqueIOBuf, UniqueIOBuf] SinkResponse
ctypedef cResponseAndStreamTransformation[UniqueIOBuf, UniqueIOBuf, UniqueIOBuf] BiDiResponse

@cython.final
cdef class ServerSink_IOBuf:
    cdef unique_ptr[cSinkConsumer[UniqueIOBuf, UniqueIOBuf]] _cSink

    @staticmethod
    cdef _fbthrift_create(object sink_callback):
        cdef ServerSink_IOBuf inst = ServerSink_IOBuf.__new__(ServerSink_IOBuf)
        # currently uses a dummy no-op callback that accumulates IOBuf sink elements
        # then returns them as a single IOBuf
        inst._cSink = makeIOBufSinkConsumer(sink_callback, get_executor())
        return inst

cdef class ResponseAndSinkConsumer:
    cdef unique_ptr[SinkResponse] _cResponseSink

    @staticmethod
    cdef _fbthrift_create(object val, object sink):
        cdef ResponseAndSinkConsumer inst = ResponseAndSinkConsumer.__new__(ResponseAndSinkConsumer)
        inst._cResponseSink = make_unique[SinkResponse](
            createResponseAndSinkConsumer[UniqueIOBuf, UniqueIOBuf, UniqueIOBuf](
                cmove((<IOBuf>val)._ours),
                cmove(dereference((<ServerSink_IOBuf>sink)._cSink))
            )
        )
        return inst


cdef class Promise_Sink(Promise_Py):
    cdef cFollyPromise[SinkResponse]* _cPromise

    def __cinit__(self):
        self._cPromise = new cFollyPromise[SinkResponse](cFollyPromise[SinkResponse].makeEmpty())

    def __dealloc__(self):
        del self._cPromise

    cdef error_ta(Promise_Sink self, cTApplicationException err):
        self._cPromise.setException(err)

    cdef error_tse(Promise_Sink self, cFollyExceptionWrapper err):
        self._cPromise.setException(cmove(err))

    cdef error_py(Promise_Sink self, cPythonUserException err):
        self._cPromise.setException(cmove(err))

    cdef complete(Promise_Sink self, object pyobj):
        self._cPromise.setValue(
            cmove(dereference(cmove((<ResponseAndSinkConsumer>pyobj)._cResponseSink)))
        )

    @staticmethod
    cdef create(cFollyPromise[SinkResponse] promise):
        cdef Promise_Sink inst = Promise_Sink.__new__(Promise_Sink)
        inst._cPromise[0] = cmove(promise)
        return inst

cdef class Promise_Stream(Promise_Py):
    cdef cFollyPromise[StreamResponse]* cPromise

    def __cinit__(self):
        self.cPromise = new cFollyPromise[StreamResponse](cFollyPromise[StreamResponse].makeEmpty())

    def __dealloc__(self):
        del self.cPromise

    cdef error_ta(Promise_Stream self, cTApplicationException err):
        self.cPromise.setException(err)

    cdef error_tse(Promise_Stream self, cFollyExceptionWrapper err):
        self.cPromise.setException(cmove(err))

    cdef error_py(Promise_Stream self, cPythonUserException err):
        self.cPromise.setException(cmove(err))

    cdef complete(Promise_Stream self, object pyobj):
        self.cPromise.setValue(cmove(dereference(cmove((<ResponseAndServerStream>pyobj).cResponseStream))))

    @staticmethod
    cdef create(cFollyPromise[StreamResponse] cPromise):
        cdef Promise_Stream inst = Promise_Stream.__new__(Promise_Stream)
        inst.cPromise[0] = cmove(cPromise)
        return inst


cdef class Promise_cFollyUnit(Promise_Py):
    cdef cFollyPromise[cFollyUnit]* cPromise

    def __cinit__(self):
        self.cPromise = new cFollyPromise[cFollyUnit](cFollyPromise[cFollyUnit].makeEmpty())

    def __dealloc__(self):
        del self.cPromise

    cdef error_ta(Promise_cFollyUnit self, cTApplicationException err):
        self.cPromise.setException(err)

    cdef error_tse(Promise_cFollyUnit self, cFollyExceptionWrapper err):
        self.cPromise.setException(cmove(err))

    cdef error_py(Promise_cFollyUnit self, cPythonUserException err):
        self.cPromise.setException(cmove(err))

    cdef complete(Promise_cFollyUnit self, object _):
        self.cPromise.setValue(c_unit)

    @staticmethod
    cdef create(cFollyPromise[cFollyUnit] cPromise):
        cdef Promise_cFollyUnit inst = Promise_cFollyUnit.__new__(Promise_cFollyUnit)
        inst.cPromise[0] = cmove(cPromise)
        return inst

cdef class ServerStream_IOBuf(ServerStream):
    cdef unique_ptr[cServerStream[UniqueIOBuf]] cStream

    @staticmethod
    cdef _fbthrift_create(object stream):
        cdef ServerStream_IOBuf inst = ServerStream_IOBuf.__new__(ServerStream_IOBuf)
        inst.cStream = make_unique[cServerStream[UniqueIOBuf]](
            createAsyncIteratorFromPyIterator[unique_ptr[cIOBuf]](
                stream,
                get_executor(),
                genNextStreamValue
            )
        )
        return inst

cdef class ResponseAndServerStream:
    cdef unique_ptr[StreamResponse] cResponseStream

    @staticmethod
    cdef _fbthrift_create(object val, object stream):
        cdef ResponseAndServerStream inst = ResponseAndServerStream.__new__(ResponseAndServerStream)
        inst.cResponseStream = make_unique[StreamResponse](
            createResponseAndServerStream[UniqueIOBuf, UniqueIOBuf](
                cmove((<IOBuf>val)._ours),
                cmove(dereference((<ServerStream_IOBuf>stream).cStream))
            )
        )
        return inst

cdef class StreamTransformation_IOBuf:
    cdef unique_ptr[cStreamTransformation[UniqueIOBuf, UniqueIOBuf]] _cBidi

    @staticmethod
    cdef _fbthrift_create(object bidi):
        cdef StreamTransformation_IOBuf inst = StreamTransformation_IOBuf.__new__(StreamTransformation_IOBuf)
        inst._cBidi = createIOBufStreamTransformation(bidi, get_executor())
        return inst

cdef class ResponseAndStreamTransformation:
    cdef unique_ptr[BiDiResponse] _cResponseBidi

    @staticmethod
    cdef _fbthrift_create(object val, object bidi):
        cdef ResponseAndStreamTransformation inst = ResponseAndStreamTransformation.__new__(ResponseAndStreamTransformation)
        inst._cResponseBidi = make_unique[BiDiResponse](
            createResponseAndStreamTransformation[UniqueIOBuf, UniqueIOBuf, UniqueIOBuf](
                cmove((<IOBuf>val)._ours),
                cmove(dereference((<StreamTransformation_IOBuf>bidi)._cBidi))
            )
        )
        return inst

cdef class Promise_BiDi(Promise_Py):
    cdef cFollyPromise[BiDiResponse]* _cPromise

    def __cinit__(self):
        self._cPromise = new cFollyPromise[BiDiResponse](cFollyPromise[BiDiResponse].makeEmpty())

    def __dealloc__(self):
        del self._cPromise

    cdef error_ta(Promise_BiDi self, cTApplicationException err):
        self._cPromise.setException(err)

    cdef error_tse(Promise_BiDi self, cFollyExceptionWrapper err):
        self._cPromise.setException(cmove(err))

    cdef error_py(Promise_BiDi self, cPythonUserException err):
        self._cPromise.setException(cmove(err))

    cdef complete(Promise_BiDi self, object pyobj):
        self._cPromise.setValue(
            cmove(dereference(cmove((<ResponseAndStreamTransformation>pyobj)._cResponseBidi)))
        )

    @staticmethod
    cdef create(cFollyPromise[BiDiResponse] promise):
        cdef Promise_BiDi inst = Promise_BiDi.__new__(Promise_BiDi)
        inst._cPromise[0] = cmove(promise)
        return inst

cdef void _fail_with_app_overload(Promise_Py promise, ApplicationOverloadError ex):
    # Build the C++ TrustedServerException (wrapped in an exception_wrapper that
    # retains its concrete type) so the overload error code -- and therefore the
    # ApplicationErrorType (LOADSHEDDING) -- is preserved to the client, unlike a
    # plain TApplicationException whose type is discarded.
    cdef string message = <string>ex.message.encode('UTF-8')
    promise.error_tse(makeAppOverloadExceptionWrapper(message))


cdef void _move_python_user_exception(Promise_Py promise, PythonUserException pyex):
    cdef unique_ptr[cPythonUserException] owned = cmove(pyex._cpp_obj)
    promise.error_py(cmove(dereference(owned.get())))

async def serverCallback_coro(object callFunc, str funcName, Promise_Py promise, IOBuf buf, Protocol prot, RpcKind kind):
    try:
        if kind is RpcKind.SINGLE_REQUEST_STREAMING_RESPONSE:
            val, stream = await callFunc(buf, prot)
            stream = ServerStream_IOBuf._fbthrift_create(stream)
            val = ResponseAndServerStream._fbthrift_create(val, stream)
        elif kind is RpcKind.SINK:
            val, sink = await callFunc(buf, prot)
            sink = ServerSink_IOBuf._fbthrift_create(sink)
            val = ResponseAndSinkConsumer._fbthrift_create(val, sink)
        elif kind is RpcKind.BIDIRECTIONAL_STREAM:
            val, bidi = await callFunc(buf, prot)
            bidi = StreamTransformation_IOBuf._fbthrift_create(bidi)
            val = ResponseAndStreamTransformation._fbthrift_create(val, bidi)
        else:
            val = await callFunc(buf, prot)
    except PythonUserException as pyex:
        _move_python_user_exception(promise, <PythonUserException>pyex)
    except ApplicationOverloadError as ex:
        # Preserves ApplicationErrorType.LOADSHEDDING to the client.
        _fail_with_app_overload(promise, ex)
    except ApplicationError as ex:
        # If the handler raised an ApplicationError convert it to a C++ one
        promise.error_ta(cTApplicationException(
            ex.type.value, ex.message.encode('UTF-8')
        ))
    except Exception as ex:
        print(
            f"Unexpected error in {funcName}:",
            file=sys.stderr)
        traceback.print_exc()
        promise.error_ta(cTApplicationException(
            cTApplicationExceptionType__UNKNOWN,
            repr(ex).encode('UTF-8'),
        ))
    except asyncio.CancelledError as ex:
        print(f"Coroutine was cancelled in service handler {funcName}:", file=sys.stderr)
        traceback.print_exc()
        promise.error_ta(cTApplicationException(
            cTApplicationExceptionType__UNKNOWN,
            (f'Application was cancelled on the server with message: {str(ex)}').encode('UTF-8'),
        ))
    else:
        promise.complete(val)

# Strong refs to in-flight lifecycle tasks so they aren't GC'd before running
# during shutdown (which would break the promise). See _inflight_termination_tasks.
_inflight_lifecycle_tasks = set()

async def lifecycle_coro(object func, str funcName, Promise_Py promise):
    try:
        if func is None:
            promise.complete(c_unit)
            return
        await func()
    except PythonUserException as pyex:
        _move_python_user_exception(promise, <PythonUserException>pyex)
    except ApplicationOverloadError as ex:
        # Preserves ApplicationErrorType.LOADSHEDDING to the client.
        _fail_with_app_overload(promise, ex)
    except ApplicationError as ex:
        # If the handler raised an ApplicationError convert it to a C++ one
        promise.error_ta(cTApplicationException(
            ex.type.value, ex.message.encode('UTF-8')
        ))
    except Exception as ex:
        print(
            f"Unexpected error in {funcName}:",
            file=sys.stderr)
        traceback.print_exc()
        promise.error_ta(cTApplicationException(
            cTApplicationExceptionType__UNKNOWN, repr(ex).encode('UTF-8')
        ))
    except asyncio.CancelledError:
        # A lifecycle hook is cancelled only when the server/event loop is tearing
        # down; complete (not error) the promise so the C++ onStopRequested
        # collector does not XLOG(FATAL) on shutdown. Mirrors termination_coro.
        promise.complete(c_unit)
    else:
        promise.complete(c_unit)

cdef api int launchControlTask(object coroutine_factory) except -1:
    asyncio.get_running_loop().create_task(coroutine_factory())
    return 0


cdef int launchWorkerTask(
    object coroutine_factory,
    shared_ptr[cWorkerCapacityLease] lease,
) except -1:
    def release_lease():
        lease.reset()

    launch_worker_task(release_lease, coroutine_factory)
    return 0

cdef int combinedHandler(
    object func,
    object interaction_self,
    string funcName,
    Promise_Py promise,
    RequestDispatchParameters requestDispatchParameters,
) except -1:
    cdef Cpp2RequestContext* ctx = requestDispatchParameters.requestContext
    cdef Protocol prot = requestDispatchParameters.protocol
    cdef RpcKind kind = requestDispatchParameters.rpcKind
    # For an inside-interaction method, `func` is the *unbound* dispatch
    # function and `interaction_self` is the per-session handler instance; bind
    # the two here. For ordinary service methods `interaction_self` is None and
    # `func` is already a bound method.
    cdef object call_func = func if interaction_self is None else func.__get__(interaction_self)
    cdef object rpc_coroutine_factory
    reset_token = PyContextVar_Set(THRIFT_REQUEST_CONTEXT, RequestContext._fbthrift_create(ctx))

    try:
        rpc_coroutine_factory = functools.partial(
            serverCallback_coro,
            call_func,
            funcName.decode('UTF-8'),
            promise,
            from_unique_ptr(cmove(requestDispatchParameters.serializedRequest.buffer)),
            prot,
            kind,
        )
        return cmove(requestDispatchParameters.launchRequest)(
            <PyObject*>rpc_coroutine_factory
        )
    finally:
        PyContextVar_Reset(THRIFT_REQUEST_CONTEXT, reset_token)

cdef api void warnThreadsWithoutFactoryContext() noexcept with gil:
    logger.warning(
        "workers set and context not entered. "
        "Server will stay in single thread mode"
    )


cdef api void handleLifecycleCallback(object func, string funcName, cFollyPromise[cFollyUnit] cPromise):
    cdef Promise_cFollyUnit __promise = Promise_cFollyUnit.create(cmove(cPromise))
    cdef object task
    cdef object loop
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # No running loop: a task scheduled here would never execute, so
        # lifecycle_coro would never complete the promise and its destructor
        # would raise BrokenPromise into the C++ collector.
        __promise.complete(c_unit)
        return
    try:
        task = loop.create_task(
            lifecycle_coro(func, funcName.decode('UTF-8'), __promise)
        )
    except Exception:
        # Loop already gone (typically shutdown). Complete the promise so the C++
        # onStopRequested collector does not see a BrokenPromise and XLOG(FATAL).
        # Mirrors _schedule_termination.
        __promise.complete(c_unit)
        return
    _inflight_lifecycle_tasks.add(task)
    task.add_done_callback(_inflight_lifecycle_tasks.discard)

cdef api int handleServerCallback(
    object func,
    object interaction_self,
    string funcName,
    cFollyPromise[unique_ptr[cIOBuf]] cPromise,
    RequestDispatchParameters requestDispatchParameters,
) except -1:
    cdef Promise_IOBuf __promise = Promise_IOBuf.create(cmove(cPromise))
    return combinedHandler(
        func,
        interaction_self,
        funcName,
        __promise,
        cmove(requestDispatchParameters),
    )

def install_interaction_tile(object tile):
    """Install ``tile`` as the per-session Tile for the in-flight interaction.

    Called by the generated wrapper of a factory-with-initial-response method
    (returns ``(interaction_handler, response)``) on the loop thread during
    dispatch; the request context comes from the contextvar set by
    ``combinedHandler``. The C++ side wraps ``tile`` in a PythonTile and fulfills
    the parked TilePromise so inside-interaction calls dispatch against it."""
    cdef RequestContext rctx = <RequestContext>THRIFT_REQUEST_CONTEXT.get()
    installInteractionTileFromHandler(rctx._get_ctx(), <PyObject*>tile, get_executor())

cdef api int handleServerStreamCallback(
    object func,
    object interaction_self,
    string funcName,
    cFollyPromise[cResponseAndServerStream[unique_ptr[cIOBuf], unique_ptr[cIOBuf]]] cPromise,
    RequestDispatchParameters requestDispatchParameters,
) except -1:
    cdef Promise_Stream __promise = Promise_Stream.create(cmove(cPromise))
    return combinedHandler(
        func,
        interaction_self,
        funcName,
        __promise,
        cmove(requestDispatchParameters),
    )

cdef api int handleServerSinkCallback(
    object func,
    object interaction_self,
    string funcName,
    cFollyPromise[cResponseAndSinkConsumer[unique_ptr[cIOBuf], unique_ptr[cIOBuf], unique_ptr[cIOBuf]]] cPromise,
    RequestDispatchParameters requestDispatchParameters,
) except -1:
    cdef Promise_Sink __promise = Promise_Sink.create(cmove(cPromise))
    return combinedHandler(
        func,
        interaction_self,
        funcName,
        __promise,
        cmove(requestDispatchParameters),
    )

cdef api int handleServerBidiCallback(
    object func,
    object interaction_self,
    string funcName,
    cFollyPromise[cResponseAndStreamTransformation[unique_ptr[cIOBuf], unique_ptr[cIOBuf], unique_ptr[cIOBuf]]] cPromise,
    RequestDispatchParameters requestDispatchParameters,
) except -1:
    cdef Promise_BiDi __promise = Promise_BiDi.create(cmove(cPromise))
    return combinedHandler(
        func,
        interaction_self,
        funcName,
        __promise,
        cmove(requestDispatchParameters),
    )

cdef api int handleServerCallbackOneway(
    object func,
    object interaction_self,
    string funcName,
    cFollyPromise[cFollyUnit] cPromise,
    RequestDispatchParameters requestDispatchParameters,
) except -1:
    cdef Promise_cFollyUnit __promise = Promise_cFollyUnit.create(cmove(cPromise))
    return combinedHandler(
        func,
        interaction_self,
        funcName,
        __promise,
        cmove(requestDispatchParameters),
    )

cdef api unique_ptr[cIOBuf] getSerializedPythonMetadata(object server):
    metadata = server.__get_metadata_service_response__()
    iobuf = serialize_iobuf(metadata, protocol=Protocol.BINARY)
    return cmove((<IOBuf>iobuf)._ours)

cdef api object callInteractionFactory(object factory):
    """Invoke a zero-arg interaction factory (``self.create<Name>``) and return
    the new per-session handler instance. Called from C++
    (``createInteractionImpl`` / ``maybeFulfillTilePromise``) to build a
    ``PythonTile``. A factory exception propagates per the C-API convention
    (``nullptr`` + error set); the C++ caller surfaces it via
    ``handlePythonError``."""
    return factory()

# Strong references to in-flight ``termination_coro`` tasks: asyncio holds only a
# *weak* reference, so without this a task could be GC'd mid-flight -- dropping
# the hook (fire-and-forget path) or breaking its promise (awaited path). Each
# task discards itself via a done-callback once it settles, so the set stays
# bounded; loop shutdown cancels pending tasks, which ``termination_coro``'s
# ``finally`` turns into a completed promise -- no leak.
_inflight_termination_tasks = set()

async def termination_coro(object handler, Promise_cFollyUnit promise):
    """Run an interaction handler's async ``onInteractionTermination`` hook.

    Exceptions are logged and swallowed: there is no client to report to. The
    ``promise`` (``None`` on the fire-and-forget connection-close path) is always
    completed in ``finally`` so an awaiting C++ ``co_onTermination`` resumes
    regardless of outcome -- including cancellation, which (a ``BaseException``)
    escapes ``except Exception`` but still runs ``finally`` first."""
    try:
        await handler.onInteractionTermination()
    except Exception:
        print("Unexpected error in interaction onInteractionTermination:", file=sys.stderr)
        traceback.print_exc()
    finally:
        if promise is not None:
            promise.complete(c_unit)

cdef _schedule_termination(object handler, Promise_cFollyUnit promise):
    """Schedule ``termination_coro`` on the asyncio loop. ``promise`` is set for
    the awaited explicit-terminate path and ``None`` for fire-and-forget
    connection-close. Held in ``_inflight_termination_tasks`` until it settles so
    it isn't GC'd mid-flight. Must be called on the loop thread (GIL held)."""
    cdef object task
    try:
        task = asyncio.get_running_loop().create_task(termination_coro(handler, promise))
    except Exception:
        # Loop already gone (typically shutdown). Complete the promise (when
        # present) so an awaiting co_onTermination resumes; else drop best-effort.
        print("Could not schedule interaction onInteractionTermination:", file=sys.stderr)
        traceback.print_exc()
        if promise is not None:
            promise.complete(c_unit)
        return
    _inflight_termination_tasks.add(task)
    task.add_done_callback(_inflight_termination_tasks.discard)

cdef api void handleInteractionTermination(object handler, cFollyPromise[cFollyUnit] cPromise):
    """Schedule ``handler.onInteractionTermination()`` on the asyncio loop and fulfill the
    promise when it finishes. Called from ``PythonTile::co_onTermination`` (on
    the Python executor / loop thread) on an explicit client terminate."""
    _schedule_termination(handler, Promise_cFollyUnit.create(cmove(cPromise)))

cdef api void scheduleInteractionTermination(object handler):
    """Fire-and-forget ``onInteractionTermination`` used by ``PythonTile``'s destructor when
    the connection closed before an explicit terminate (nothing awaits it). Must
    be called on the loop thread (GIL held)."""
    _schedule_termination(handler, None)

cdef cmap[string, PyObjPtr] buildHandlerMethods(dict funcMap):
    cdef cmap[string, PyObjPtr] methods
    cdef FunctionEntry entry
    for name, entry in funcMap.items():
        methods[<bytes>name] = <PyObject*>entry.handler
    return methods


cdef class WorkerSystem:
    """Carries what the worker subsystem shares from factory creation to
    shutdown: the control executor and the function table it starts from, and,
    once started, the work scheduler, the worker-capable execution system, and
    the worker pool. Callers skip worker setup entirely when no method is
    worker-capable."""

    @staticmethod
    cdef WorkerSystem create(
        cAsyncioExecutor* controlExecutor,
        dict funcMap,
        set workerCapableMethodNames,
    ):
        """Workers must send control methods to the executor that control
        dispatch already uses, so the factory passes that executor in when it
        is created."""
        cdef WorkerSystem workerSystem = WorkerSystem.__new__(WorkerSystem)
        workerSystem.controlExecutor = controlExecutor
        workerSystem.funcMap = funcMap
        workerSystem.workerCapableMethodNames = workerCapableMethodNames
        return workerSystem

    cdef start(self, int workerCount, object makeInitializer):
        """Builds the work scheduler and the worker-capable execution system,
        then starts workerCount workers, each with the initializer that
        makeInitializer builds from this worker system. If the workers fail to
        start, closes what it built and re-raises."""
        cdef cmap[string, PyObjPtr] controlMethods = buildHandlerMethods(
            self.funcMap
        )
        cdef cset[string] workerCapable
        for name in self.workerCapableMethodNames:
            workerCapable.insert(<bytes>name)
        self.scheduler = make_shared[cWorkScheduler]()
        self.executionSystem = makeWorkerCapableExecutionSystem(
            self.controlExecutor,
            <cLaunchControlRequest>launchControlTask,
            cmove(controlMethods),
            self.scheduler,
            <cLaunchWorkerRequest>launchWorkerTask,
            cmove(workerCapable),
        )
        try:
            workerPool = AsyncioWorkerPool(workerCount, makeInitializer(self))
            # On failure the pool rolls back the workers it started.
            workerPool.start()
        except BaseException:
            self.close()
            raise
        self.workerPool = workerPool

    cdef close(self):
        """Closes the workers, so each cleans up on its own loop, then the work
        scheduler, the order that work_scheduler.md requires, and then releases
        the execution system, which holds the control executor alive. Does
        nothing if start() never ran."""
        # The pool's initializers hold this worker system; drop the pool to
        # break that cycle.
        workerPool, self.workerPool = self.workerPool, None
        try:
            if workerPool is not None:
                workerPool.close()
        finally:
            if self.scheduler.get() != NULL:
                self.scheduler.get().close()
            self.scheduler.reset()
            self.executionSystem.reset()


# Each worker unregisters on its own loop, before that loop closes: the
# registration keeps the loop's executor alive, and an executor's destructor
# never returns while something still keeps it alive.
cdef object makeWorkerUnregisterer(
    shared_ptr[cWorkScheduler] scheduler, shared_ptr[cWorkerHandle] worker
):
    def unregister_worker():
        cdef cWorkScheduler* workScheduler = scheduler.get()
        cdef cWorkerHandle* workerHandle = worker.get()
        with nogil:
            workScheduler.unregisterWorker(dereference(workerHandle))

    return unregister_worker


# The stack ignores the value a handler's exit returns, so a handler that
# suppresses exceptions cannot hide a failed worker start.
async def exitThatPropagatesErrors(handler, exc_type, exc, traceback):
    await handler.__aexit__(exc_type, exc, traceback)
    return False


cdef class PerLoopWorkerInitializer:
    """Starts a per-loop worker: the worker calls factory on its own loop and
    serves requests with that handler."""

    cdef WorkerSystem workerSystem
    cdef object factory

    def __cinit__(self, WorkerSystem workerSystem, object factory):
        self.workerSystem = workerSystem
        self.factory = factory

    async def __call__(self):
        cdef shared_ptr[cWorkerHandle] worker
        cdef cmap[string, PyObjPtr] workerMethods
        # A failed start unwinds the stack; a successful one hands it to the
        # pool, which closes it when the worker leaves. Until then the stack
        # also keeps this worker's handler and function table alive.
        async with contextlib.AsyncExitStack() as cleanup:
            handler = self.factory()
            if not isinstance(handler, ServiceInterface):
                raise TypeError(
                    f"Handler factory returned {type(handler).__name__}; "
                    "expected a thrift-python ServiceInterface."
                )
            await handler.__aenter__()
            cleanup.push_async_exit(
                functools.partial(exitThatPropagatesErrors, handler)
            )
            # getFunctionTable builds fresh entries per call, and the bindings
            # below borrow them.
            workerFuncMap = handler.getFunctionTable()
            cleanup.callback(workerFuncMap.clear)
            missingMethodNames = (
                self.workerSystem.workerCapableMethodNames.difference(workerFuncMap)
            )
            if missingMethodNames:
                missing = ", ".join(
                    sorted(name.decode() for name in missingMethodNames)
                )
                raise TypeError(
                    f"Handler factory returned {type(handler).__name__} for a "
                    f"worker; it lacks routed methods: {missing}."
                )
            worker = registerWorkerExecutor(
                dereference(self.workerSystem.scheduler), get_executor()
            )
            cleanup.callback(
                makeWorkerUnregisterer(self.workerSystem.scheduler, worker)
            )
            workerMethods = buildHandlerMethods(workerFuncMap)
            self.workerSystem.executionSystem.get().bindWorkerMethods(
                dereference(worker), cmove(workerMethods)
            )
            self.workerSystem.scheduler.get().makeWorkerCapacityAvailable(
                dereference(worker)
            )
            return cleanup.pop_all()


cdef class SharedWorkerInitializer:
    """Starts a shared-mode worker: the worker binds funcMap's handler
    functions, and control owns the one handler."""

    cdef WorkerSystem workerSystem
    cdef cmap[string, PyObjPtr] workerMethods

    def __cinit__(self, WorkerSystem workerSystem, dict funcMap):
        self.workerSystem = workerSystem
        # Shared table only; control owns the handler lifecycle. The caller
        # keeps funcMap alive for the borrowed bindings.
        self.workerMethods = buildHandlerMethods(funcMap)

    async def __call__(self):
        cdef shared_ptr[cWorkerHandle] worker
        # A failed start unwinds the stack; a successful one hands it to the
        # pool, which closes it when the worker leaves.
        async with contextlib.AsyncExitStack() as cleanup:
            worker = registerWorkerExecutor(
                dereference(self.workerSystem.scheduler), get_executor()
            )
            cleanup.callback(
                makeWorkerUnregisterer(self.workerSystem.scheduler, worker)
            )
            self.workerSystem.executionSystem.get().bindWorkerMethods(
                dereference(worker), self.workerMethods
            )
            self.workerSystem.scheduler.get().makeWorkerCapacityAvailable(
                dereference(worker)
            )
            return cleanup.pop_all()


class FactoryExit:
    """Exits an entered factory. The control handler may suppress only the
    exception that the caller raised inside the factory context; a failure from
    the factory's own entry or cleanup always propagates, whatever the
    handler's exit returns."""

    def __init__(self, handler):
        self.handler = handler
        self.entered = None
        self.caller_exception = None

    async def exit_control_handler(self, exc_type, exc, traceback):
        suppress = await self.handler.__aexit__(exc_type, exc, traceback)
        return bool(suppress) and exc is not None and exc is self.caller_exception

    async def __call__(self, *exc_info):
        self.caller_exception = exc_info[1]
        return await self.entered.__aexit__(*exc_info)


cdef class PythonAsyncProcessorFactory(AsyncProcessorFactory):
    cdef void releaseOwnedResources(self) noexcept:
        (<cPythonAsyncProcessorFactory*>self._cpp_obj.get()).releaseOwnedResources()

    cdef void _release_executor_facade(self) noexcept:
        self.executorFacade.releaseCreatorReference()
        self.executorFacade = NULL

    async def __aenter__(self):
        factory_exit = FactoryExit(self.handler)
        async with contextlib.AsyncExitStack() as stack:
            stack.callback(lambda: self.releaseOwnedResources())
            await self.handler.__aenter__()
            stack.push_async_exit(factory_exit.exit_control_handler)
            (<cPythonAsyncProcessorFactory*>self._cpp_obj.get()).markContextEntered()
            if (
                self.workerSystem is not None
                and (<cPythonAsyncProcessorFactory*>self._cpp_obj.get()).requestedThreadCount() > 1
            ):
                stack.enter_context(self._activate_workers())
            factory_exit.entered = stack.pop_all()
            # The exit is assigned only after every step succeeds. A failed step
            # unwinds this stack, so a failed entry leaves no exit to call.
            self.factoryExit = factory_exit
        return self

    async def __aexit__(self, *exc_info):
        # Python calls __aexit__ once for each __aenter__ that returns, so
        # factoryExit is set here.
        #
        # The exit records the caller's exception. That exception's traceback
        # holds this frame, and this frame holds the factory. If the factory
        # kept the exit, the cycle would keep the handler and everything it
        # reaches alive until the garbage collector runs; a cycle like this
        # once blocked event-loop shutdown. So the factory detaches the exit
        # first and then calls it.
        factory_exit, self.factoryExit = self.factoryExit, None
        return await factory_exit(*exc_info)

    def set_cpu_worker_threads(self, number_of_threads):
        """Sets the number of CPU worker threads for this factory's requests.
        Call it before the caller enters the factory context, on the thread
        that enters it; the last value set before entry wins. One, the default,
        serves every request on the control thread; a larger number starts that
        many worker threads beside the control thread when the factory context
        is entered. The factory applies the count on any build. A count below
        one raises ValueError.

        Worker threads run only request-response methods outside interactions.
        A service without such methods serves every request on the control
        thread, whatever count it requests.

        A factory from create() shares one handler across every thread. Objects
        that the handler creates on the control thread's event loop, such as
        async clients or asyncio locks, may fail when a worker thread uses them.
        create_from_handler_factory() gives each thread its own handler."""
        if number_of_threads < 1:
            raise ValueError(
                f"Thread count must be one or more; got {number_of_threads}."
            )
        (<cPythonAsyncProcessorFactory*>self._cpp_obj.get()).setRequestedThreadCount(
            number_of_threads
        )

    cdef _activate_workers(self):
        with contextlib.ExitStack() as stack:
            self.workerSystem.start(
                (<cPythonAsyncProcessorFactory*>self._cpp_obj.get()).requestedThreadCount(),
                self.initializerFactory,
            )
            # start() rolls back what it started when it fails, so close() is
            # registered only after a successful start.
            stack.callback(lambda: self.workerSystem.close())
            (<cPythonAsyncProcessorFactory*>self._cpp_obj.get()).setExecutionSystem(
                self.workerSystem.executionSystem
            )
            self.executorFacade = cWorkerExecutorFacade.create(
                self.workerSystem.scheduler, <cFollyExecutor*>self.controlExecutor
            )
            stack.callback(lambda: self._release_executor_facade())
            return stack.pop_all()

    @staticmethod
    cdef PythonAsyncProcessorFactory create(cServiceInterface server):
        # One handler serves every thread. For a handler with loop-affine resources,
        # such as async clients or asyncio locks, use create_from_handler_factory().
        cdef cServiceInterface controlHandler = server
        cdef cmap[string_view, HandlerFunc] funcs
        cdef cmap[string, PyObjPtr] controlMethods
        cdef cvector[PyObject*] lifecycle
        cdef string_view name_view

        cdef dict funcMap = controlHandler.getFunctionTable()
        cdef list lifecycleFuncs = [
            controlHandler.onStartServing,
            controlHandler.onStopRequested,
        ]
        cdef FunctionEntry entry
        cdef PyObject* factory_obj
        cdef set workerCapableMethodNames = set()

        for name, entry in funcMap.items():
            name_view = bytes_to_string_view(name)
            controlMethods[<bytes>name] = <PyObject*>entry.handler
            if (
                entry.rpc_kind == RpcKind.SINGLE_REQUEST_SINGLE_RESPONSE
                and not entry.interaction
            ):
                workerCapableMethodNames.add(name)
            if entry.interaction:
                # Factory or inside-interaction method. interactionName is a
                # string_view into the Python bytes held by `entry` (kept alive
                # by funcMap), matching the funcMap keys.
                factory_obj = NULL
                if entry.interaction_factory is not None:
                    factory_obj = <PyObject*>entry.interaction_factory
                funcs[name_view] = makeInteractionHandlerFunc(
                    entry.rpc_kind,
                    <PyObject*>entry.handler,
                    <bytes>controlHandler.service_name(),
                    name_view,
                    bytes_to_string_view(entry.interaction),
                    entry.creates_interaction,
                    factory_obj,
                    entry.returns_initial_response,
                )
            else:
                funcs[name_view] = makeHandlerFunc(
                    entry.rpc_kind,
                    <PyObject*>entry.handler,
                    <bytes>controlHandler.service_name(),
                    name_view,
                )

        for lifecycle_func in lifecycleFuncs:
            lifecycle.push_back(<PyObject*>lifecycle_func)

        cdef cAsyncioExecutor* controlExecutor = get_executor()
        cdef shared_ptr[cExecutionSystem] executionSystem = makeControlOnlyExecutionSystem(
            controlExecutor, <cLaunchControlRequest>launchControlTask, cmove(controlMethods)
        )
        cdef PythonAsyncProcessorFactory inst = PythonAsyncProcessorFactory.__new__(PythonAsyncProcessorFactory)
        inst.funcMap = funcMap
        inst.initializerFactory = functools.partial(
            SharedWorkerInitializer, funcMap=funcMap
        )
        inst.lifecycleFuncs = lifecycleFuncs
        inst.handler = controlHandler
        inst.controlExecutor = controlExecutor
        if workerCapableMethodNames:
            inst.workerSystem = WorkerSystem.create(
                controlExecutor, funcMap, workerCapableMethodNames
            )
        inst._cpp_obj = static_pointer_cast[cAsyncProcessorFactory, cPythonAsyncProcessorFactory](
            cCreatePythonAsyncProcessorFactory(
                <PyObject*>controlHandler,
                cmove(funcs),
                cmove(lifecycle),
                executionSystem,
                controlExecutor,
                <bytes>controlHandler.service_name()))
        return inst

    @staticmethod
    cdef PythonAsyncProcessorFactory create_from_handler_factory(
        object handlerFactory
    ):
        # The factory's first product is the control handler. Each worker
        # calls the factory again on its own loop once workers activate.
        controlHandler = handlerFactory()
        if not isinstance(controlHandler, ServiceInterface):
            raise TypeError(
                f"Handler factory returned {type(controlHandler).__name__}; "
                "expected a thrift-python ServiceInterface."
            )
        cdef PythonAsyncProcessorFactory inst = PythonAsyncProcessorFactory.create(
            controlHandler
        )
        inst.initializerFactory = functools.partial(
            PerLoopWorkerInitializer, factory=handlerFactory
        )
        return inst
