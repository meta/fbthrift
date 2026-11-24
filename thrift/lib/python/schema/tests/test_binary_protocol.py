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

# Wire-level tests of the protocol readers and writers. Expected bytes are
# derived by hand from the C++ writers in thrift/lib/cpp2/protocol/.

from __future__ import annotations

from thrift.lib.python.schema._protocols import Protocol
from thrift.lib.python.schema._ttypes import (
    T_BOOL,
    T_BYTE,
    T_DOUBLE,
    T_FLOAT,
    T_I16,
    T_I32,
    T_I64,
    T_LIST,
    T_MAP,
    T_SET,
    T_STOP,
    T_STRING,
    T_STRUCT,
)
from thrift.lib.python.schema.tests.protocol_testing import WireTestCase


class BinaryProtocolTest(WireTestCase):
    PROTOCOL = Protocol.BINARY

    def nested_structs(self, levels: int) -> bytes:
        return b"\x0c\x00\x01" * (levels - 1) + b"\x00" * levels

    def nested_lists(self, levels: int) -> bytes:
        return b"\x0f\x00\x00\x00\x01" * (levels - 1) + b"\x08\x00\x00\x00\x00"

    def test_write_headers(self) -> None:
        self.assert_writes(
            [
                (lambda w: w.write_struct_begin("S"), b""),
                (lambda w: w.write_struct_end(), b""),
                (lambda w: w.write_field_begin("f", T_I32, 1), b"\x08\x00\x01"),
                (lambda w: w.write_field_begin("f", T_BOOL, -5), b"\x02\xff\xfb"),
                (lambda w: w.write_field_begin("f", T_LIST, 32767), b"\x0f\x7f\xff"),
                (lambda w: w.write_field_end(), b""),
                (lambda w: w.write_field_stop(), b"\x00"),
                (lambda w: w.write_list_begin(T_I32, 3), b"\x08\x00\x00\x00\x03"),
                (lambda w: w.write_list_begin(T_I64, 0), b"\x0a\x00\x00\x00\x00"),
                (lambda w: w.write_set_begin(T_STRING, 2), b"\x0b\x00\x00\x00\x02"),
                (
                    lambda w: w.write_map_begin(T_STRING, T_STRUCT, 258),
                    b"\x0b\x0c\x00\x00\x01\x02",
                ),
                (
                    lambda w: w.write_map_begin(T_I32, T_I32, 0),
                    b"\x08\x08\x00\x00\x00\x00",
                ),
                (lambda w: w.write_list_end(), b""),
                (lambda w: w.write_set_end(), b""),
                (lambda w: w.write_map_end(), b""),
            ]
        )

    def test_write_scalars(self) -> None:
        self.assert_writes(
            [
                (lambda w: w.write_bool(True), b"\x01"),
                (lambda w: w.write_bool(False), b"\x00"),
                (lambda w: w.write_byte(-1), b"\xff"),
                (lambda w: w.write_byte(127), b"\x7f"),
                (lambda w: w.write_i16(-2), b"\xff\xfe"),
                (lambda w: w.write_i16(0x1234), b"\x12\x34"),
                (lambda w: w.write_i32(1), b"\x00\x00\x00\x01"),
                (lambda w: w.write_i32(-(2**31)), b"\x80\x00\x00\x00"),
                (lambda w: w.write_i64(-1), b"\xff" * 8),
                (lambda w: w.write_i64(2**63 - 1), b"\x7f" + b"\xff" * 7),
                (lambda w: w.write_double(1.0), b"\x3f\xf0" + b"\x00" * 6),
                (lambda w: w.write_double(-0.0), b"\x80" + b"\x00" * 7),
                (lambda w: w.write_float(1.0), b"\x3f\x80\x00\x00"),
                (lambda w: w.write_float(-2.5), b"\xc0\x20\x00\x00"),
                (lambda w: w.write_string(""), b"\x00\x00\x00\x00"),
                (lambda w: w.write_string("é"), b"\x00\x00\x00\x02\xc3\xa9"),
                (lambda w: w.write_binary(b"\x00\xff"), b"\x00\x00\x00\x02\x00\xff"),
                (lambda w: w.write_raw(b"raw"), b"raw"),
            ]
        )

    def test_read_headers(self) -> None:
        self.assert_reads(
            [
                (b"\x08\x00\x01", lambda r: r.read_field_begin(), (T_I32, 1, "")),
                (b"\x02\xff\xfb", lambda r: r.read_field_begin(), (T_BOOL, -5, "")),
                (b"\x00", lambda r: r.read_field_begin(), (T_STOP, 0, "")),
                (b"\x08\x00\x00\x00\x03", lambda r: r.read_list_begin(), (T_I32, 3)),
                (b"\x0b\x00\x00\x00\x00", lambda r: r.read_set_begin(), (T_STRING, 0)),
                (
                    b"\x0b\x0c\x00\x00\x01\x02",
                    lambda r: r.read_map_begin(),
                    (T_STRING, T_STRUCT, 258),
                ),
            ]
        )

    def test_read_scalars(self) -> None:
        self.assert_reads(
            [
                (b"\x01", lambda r: r.read_bool(), True),
                (b"\x00", lambda r: r.read_bool(), False),
                (b"\xff", lambda r: r.read_byte(), -1),
                (b"\xff\xfe", lambda r: r.read_i16(), -2),
                (b"\x80\x00\x00\x00", lambda r: r.read_i32(), -(2**31)),
                (b"\x7f" + b"\xff" * 7, lambda r: r.read_i64(), 2**63 - 1),
                (b"\x3f\xf0" + b"\x00" * 6, lambda r: r.read_double(), 1.0),
                (b"\xc0\x20\x00\x00", lambda r: r.read_float(), -2.5),
                (b"\x00\x00\x00\x02\xc3\xa9", lambda r: r.read_string(), "é"),
                (b"\x00\x00\x00\x00", lambda r: r.read_string(), ""),
                (b"\x00\x00\x00\x02\x00\xff", lambda r: r.read_binary(), b"\x00\xff"),
            ]
        )

    def test_rejects_malformed_values(self) -> None:
        self.assert_read_fails(
            [
                (b"\x02", lambda r: r.read_bool()),  # C++ throwBoolValueOutOfRange
                (b"\xff", lambda r: r.read_bool()),
                (b"\x00\x00\x00\x01\xff", lambda r: r.read_string()),  # not UTF-8
                (b"\xff\xff\xff\xff", lambda r: r.read_string()),  # negative size
                (b"\xff\xff\xff\xff", lambda r: r.read_binary()),
                (b"\x08\xff\xff\xff\xff", lambda r: r.read_list_begin()),
                (b"\x08\x80\x00\x00\x00", lambda r: r.read_set_begin()),
                (b"\x08\x08\xff\xff\xff\xfe", lambda r: r.read_map_begin()),
                (b"\x7f\xff\xff\xff" + b"x" * 8, lambda r: r.read_binary()),
            ]
        )

    def test_skip_every_type(self) -> None:
        self.assert_skips(
            [
                (T_BOOL, b"\x01"),
                (T_BYTE, b"\x80"),
                (T_I16, b"\x00\x01"),
                (T_I32, b"\x00\x00\x00\x01"),
                (T_I64, b"\x00" * 8),
                (T_FLOAT, b"\x00" * 4),
                (T_DOUBLE, b"\x00" * 8),
                (T_STRING, b"\x00\x00\x00\x03abc"),
                (T_STRUCT, b"\x00"),
                (
                    T_STRUCT,
                    b"\x08\x00\x01\x00\x00\x00\x07"  # 1: i32
                    b"\x0c\x00\x02\x0b\x00\x01\x00\x00\x00\x01x\x00"  # 2: struct
                    b"\x0f\x00\x03\x08\x00\x00\x00\x02\x00\x00\x00\x01\x00\x00\x00\x02"
                    b"\x00",
                ),
                (T_LIST, b"\x08\x00\x00\x00\x02\x00\x00\x00\x01\x00\x00\x00\x02"),
                (T_LIST, b"\x0b\x00\x00\x00\x02\x00\x00\x00\x01a\x00\x00\x00\x00"),
                (T_LIST, b"\x01\x00\x00\x00\x00"),  # C++ writes T_VOID when empty
                (T_SET, b"\x0c\x00\x00\x00\x02\x00\x00"),
                (
                    T_MAP,
                    b"\x0b\x0f\x00\x00\x00\x01\x00\x00\x00\x01k"
                    b"\x02\x00\x00\x00\x02\x01\x00",
                ),
                (T_MAP, b"\x08\x08\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x02"),
            ]
        )

    def test_skip_checks_sizes_before_reading(self) -> None:
        self.assert_read_fails(
            [
                (b"\x08\x7f\xff\xff\xff", lambda r: r.skip(T_LIST)),
                (b"\x0c\x7f\xff\xff\xff\x00", lambda r: r.skip(T_LIST)),
                (b"\x08\xff\xff\xff\xff", lambda r: r.skip(T_SET)),
                (b"\x0b\x0b\x7f\xff\xff\xff", lambda r: r.skip(T_MAP)),
                (b"\xff\xff\xff\xff", lambda r: r.skip(T_STRING)),
                (b"\x01\x00\x00\x00\x01", lambda r: r.skip(T_LIST)),  # T_VOID
            ]
        )
