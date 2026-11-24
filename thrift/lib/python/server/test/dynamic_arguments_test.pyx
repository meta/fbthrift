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

"""Exposes a C++ ServiceInterceptor to the thrift-python server tests, so a
test can assert on the `dynamicArguments` a C++ interceptor is handed."""

from cython.operator cimport dereference as deref
from libcpp.memory cimport make_shared, shared_ptr, static_pointer_cast
from libcpp.string cimport string
from libcpp.utility cimport move as cmove
from libcpp.vector cimport vector

from thrift.python.server_impl.interceptor.server_module cimport (
    PythonServerModule,
    ServiceInterceptorBase,
)
from thrift.python.server_impl.python_async_processor cimport (
    PythonAsyncProcessorFactory,
)


cdef class DynamicArgumentsRecorder:
    """Records what a C++ interceptor sees for each request."""

    def __cinit__(self):
        self._cpp_recorder = make_shared[cDynamicArgumentsRecorder]()

    def install(self, PythonServerModule module):
        cdef shared_ptr[ServiceInterceptorBase] ptr = static_pointer_cast[
            ServiceInterceptorBase, cDynamicArgumentsRecorder
        ](self._cpp_recorder)
        module._cpp_module.get().addServiceInterceptor(cmove(ptr))

    def records(self):
        cdef vector[string] records = self._cpp_recorder.get().records()
        return [record.decode("utf-8") for record in records]

    def clear(self):
        self._cpp_recorder.get().clear()


def set_populate_service_schema(bint enabled):
    """Mocks the kill switch over serving a service's schema."""
    setPopulateServiceSchemaForTest(enabled)


def unmock_populate_service_schema():
    unmockPopulateServiceSchemaForTest()


def clear_service_schemas():
    """Forgets the service schemas earlier tests built and shared."""
    clearServiceSchemasForTest()


def service_schema_is_built(bytes service_definition_key):
    """Whether the shared schema of the service was built."""
    return isServiceSchemaBuiltForTest(service_definition_key)


def create_factory_and_metadata(handler):
    """Does what a host that dispatches requests itself does per request:
    creates a processor factory for `handler` and builds its method metadata."""
    cdef PythonAsyncProcessorFactory factory = PythonAsyncProcessorFactory.create(handler)
    createMethodMetadataForTest(deref(factory._cpp_obj))
