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

"""A thrift-python server test needs a native interceptor that pauses each
request, to check where the paused request resumes. This module gives Python a
handle to one."""

from libcpp.memory cimport make_shared, shared_ptr, static_pointer_cast
from libcpp.utility cimport move as cmove

from thrift.python.server_impl.interceptor.server_module cimport (
    PythonServerModule,
    ServiceInterceptorBase,
)


cdef class PauseInterceptor:
    def __cinit__(self):
        self._cpp_interceptor = make_shared[cPauseInterceptor]()

    def install(self, PythonServerModule module):
        cdef shared_ptr[ServiceInterceptorBase] ptr = static_pointer_cast[
            ServiceInterceptorBase, cPauseInterceptor
        ](self._cpp_interceptor)
        module._cpp_module.get().addServiceInterceptor(cmove(ptr))

    def requests(self):
        return self._cpp_interceptor.get().requests()
