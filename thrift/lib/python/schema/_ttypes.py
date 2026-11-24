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


"""Protocol ids, wire type ids, and the C++ protocol nesting limit, shared by
the protocol backends and the walk."""

import enum


class Protocol(enum.Enum):
    """A wire format. Values match ``StandardProtocol``
    (``thrift/lib/thrift/standard.thrift``)."""

    BINARY = 1
    COMPACT = 2
    SIMPLE_JSON = 4


# Wire types (thrift/lib/cpp/protocol/TType.h).
T_STOP = 0
T_VOID = 1
T_BOOL = 2
T_BYTE = 3
T_DOUBLE = 4
T_I16 = 6
T_I32 = 8
T_U64 = 9
T_I64 = 10
T_STRING = 11
T_STRUCT = 12
T_MAP = 13
T_SET = 14
T_LIST = 15
T_UTF8 = 16
T_UTF16 = 17
T_FLOAT = 19

# The field id from a reader that identifies a field by name alone, as C++
# ``readFieldBegin`` gives it.
NO_FIELD_ID = -(2**15)

# C++ ``FLAGS_thrift_protocol_max_depth``: the nesting limit of the readers
# and of ``skip``.
PROTOCOL_MAX_DEPTH = 12000
