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

from cpython.ref cimport PyObject
from folly cimport cFollyTry
from libcpp.utility cimport move as cmove
from thrift.python.client.async_client cimport AsyncClient
from thrift.python.client.async_client_factory cimport requestchannel_callback
from thrift.python.client.request_channel cimport cRequestChannel_ptr

cdef extern from "thrift/lib/python/test/channel_callback.h" namespace "apache::thrift::python::test":
    cFollyTry[cRequestChannel_ptr] lateChannelResult(bint fail) except +


def complete_channel(AsyncClient client, bint fail):
    cdef cFollyTry[cRequestChannel_ptr] result = lateChannelResult(fail)
    requestchannel_callback(cmove(result), <PyObject *>client)
    return client._omni_client.get() != NULL
