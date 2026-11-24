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

from thrift.lib.python.schema._protocols import Protocol, ProtocolReader, ProtocolWriter
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
from thrift.lib.python.schema.tests.protocol_testing import WireTestCase, WriteCall


def _sequence(*calls: WriteCall) -> WriteCall:
    def write(writer: ProtocolWriter) -> None:
        for call in calls:
            call(writer)

    return write


class CompactProtocolTest(WireTestCase):
    PROTOCOL = Protocol.COMPACT

    def nested_structs(self, levels: int) -> bytes:
        return b"\x1c" * (levels - 1) + b"\x00" * levels

    def nested_lists(self, levels: int) -> bytes:
        return b"\x19" * (levels - 1) + b"\x05"

    def test_write_field_headers(self) -> None:
        begin = lambda w: w.write_struct_begin("S")  # noqa: E731
        self.assert_writes(
            [
                # A delta of 1-15 from the previous id shares the type byte.
                (
                    _sequence(begin, lambda w: w.write_field_begin("f", T_I32, 1)),
                    b"\x15",
                ),
                (
                    _sequence(begin, lambda w: w.write_field_begin("f", T_LIST, 15)),
                    b"\xf9",
                ),
                # Otherwise the id follows as a zigzag varint: a delta of 16,
                # a decreasing id, a negative id, and the largest id.
                (
                    _sequence(begin, lambda w: w.write_field_begin("f", T_I32, 16)),
                    b"\x05\x20",
                ),
                (
                    _sequence(
                        begin,
                        lambda w: w.write_field_begin("f", T_I32, 17),
                        lambda w: w.write_i32(0),
                        lambda w: w.write_field_begin("f", T_I32, 2),
                    ),
                    b"\x05\x22\x00\x05\x04",
                ),
                (
                    _sequence(begin, lambda w: w.write_field_begin("f", T_I32, -5)),
                    b"\x05\x09",
                ),
                (
                    _sequence(begin, lambda w: w.write_field_begin("f", T_I64, 32767)),
                    b"\x06\xfe\xff\x03",
                ),
                (_sequence(begin, lambda w: w.write_field_stop()), b"\x00"),
            ]
        )

    def test_write_bool_fields_in_the_header(self) -> None:
        begin = lambda w: w.write_struct_begin("S")  # noqa: E731
        self.assert_writes(
            [
                (
                    _sequence(
                        begin,
                        lambda w: w.write_field_begin("f", T_BOOL, 1),
                        lambda w: w.write_bool(True),
                    ),
                    b"\x11",
                ),
                (
                    _sequence(
                        begin,
                        lambda w: w.write_field_begin("f", T_BOOL, 3),
                        lambda w: w.write_bool(False),
                    ),
                    b"\x32",
                ),
                (
                    _sequence(
                        begin,
                        lambda w: w.write_field_begin("f", T_BOOL, 300),
                        lambda w: w.write_bool(True),
                    ),
                    b"\x01\xd8\x04",
                ),
                # A bool outside a field header is a byte.
                (lambda w: w.write_bool(True), b"\x01"),
                (lambda w: w.write_bool(False), b"\x02"),
            ]
        )

    def test_nested_structs_save_the_last_field_id(self) -> None:
        self.assert_writes(
            [
                (
                    _sequence(
                        lambda w: w.write_struct_begin("Outer"),
                        lambda w: w.write_field_begin("inner", T_STRUCT, 5),
                        lambda w: w.write_struct_begin("Inner"),
                        lambda w: w.write_field_begin("x", T_I32, 1),
                        lambda w: w.write_i32(1),
                        lambda w: w.write_field_stop(),
                        lambda w: w.write_struct_end(),
                        lambda w: w.write_field_begin("y", T_I32, 6),
                        lambda w: w.write_i32(2),
                        lambda w: w.write_field_stop(),
                        lambda w: w.write_struct_end(),
                    ),
                    b"\x5c\x15\x02\x00\x15\x04\x00",
                ),
            ]
        )

    def test_write_collection_headers(self) -> None:
        self.assert_writes(
            [
                (lambda w: w.write_list_begin(T_I32, 3), b"\x35"),
                (lambda w: w.write_list_begin(T_I64, 0), b"\x06"),
                (lambda w: w.write_list_begin(T_BOOL, 14), b"\xe1"),
                (lambda w: w.write_list_begin(T_STRING, 15), b"\xf8\x0f"),
                (lambda w: w.write_set_begin(T_STRUCT, 200), b"\xfc\xc8\x01"),
                (lambda w: w.write_set_begin(T_FLOAT, 1), b"\x1d"),
                (lambda w: w.write_map_begin(T_STRING, T_I64, 0), b"\x00"),
                (lambda w: w.write_map_begin(T_STRING, T_I64, 2), b"\x02\x86"),
                (lambda w: w.write_map_begin(T_BOOL, T_DOUBLE, 200), b"\xc8\x01\x17"),
                (lambda w: w.write_list_end(), b""),
                (lambda w: w.write_set_end(), b""),
                (lambda w: w.write_map_end(), b""),
            ]
        )

    def test_write_scalars(self) -> None:
        self.assert_writes(
            [
                (lambda w: w.write_byte(-1), b"\xff"),
                (lambda w: w.write_i16(0), b"\x00"),
                (lambda w: w.write_i16(-1), b"\x01"),
                (lambda w: w.write_i16(1), b"\x02"),
                (lambda w: w.write_i16(-(2**15)), b"\xff\xff\x03"),
                (lambda w: w.write_i16(2**15 - 1), b"\xfe\xff\x03"),
                (lambda w: w.write_i32(-(2**31)), b"\xff\xff\xff\xff\x0f"),
                (lambda w: w.write_i32(2**31 - 1), b"\xfe\xff\xff\xff\x0f"),
                (lambda w: w.write_i64(-(2**63)), b"\xff" * 9 + b"\x01"),
                (lambda w: w.write_i64(2**63 - 1), b"\xfe" + b"\xff" * 8 + b"\x01"),
                (lambda w: w.write_i64(64), b"\x80\x01"),
                # Big-endian, unlike Apache's compact protocol.
                (lambda w: w.write_double(1.0), b"\x3f\xf0" + b"\x00" * 6),
                (lambda w: w.write_float(-2.5), b"\xc0\x20\x00\x00"),
                (lambda w: w.write_string("é"), b"\x02\xc3\xa9"),
                (lambda w: w.write_binary(b"x" * 200), b"\xc8\x01" + b"x" * 200),
                (lambda w: w.write_raw(b"raw"), b"raw"),
            ]
        )

    def test_read_field_headers(self) -> None:
        def fields(reader: ProtocolReader) -> list[tuple[int, int, str]]:
            reader.read_struct_begin()
            result = []
            while (field := reader.read_field_begin())[0] != T_STOP:
                result.append(field)
                reader.skip(field[0])
            return result

        self.assert_reads(
            [
                (b"\x15\x00\x00", fields, [(T_I32, 1, "")]),
                (
                    b"\x05\x22\x00\x05\x04\x00\x00",
                    fields,
                    [(T_I32, 17, ""), (T_I32, 2, "")],
                ),
                (b"\x05\x09\x00\x00", fields, [(T_I32, -5, "")]),
                (b"\x11\x32\x00", fields, [(T_BOOL, 1, ""), (T_BOOL, 4, "")]),
                (b"\x01\xd8\x04\x00", fields, [(T_BOOL, 300, "")]),
                (b"\x06\xfe\xff\x03\x00\x00", fields, [(T_I64, 32767, "")]),
                (b"\x05\x80\x80\x04\x00\x00", fields, [(T_I32, -(2**15), "")]),
                (
                    b"\x05\xfe\xff\x03\x00\x15\x00\x00",
                    fields,
                    [(T_I32, 2**15 - 1, ""), (T_I32, -(2**15), "")],
                ),
            ]
        )

    def test_read_bool_fields_from_the_header(self) -> None:
        def bools(reader: ProtocolReader) -> list[bool]:
            reader.read_struct_begin()
            result = []
            while reader.read_field_begin()[0] != T_STOP:
                result.append(reader.read_bool())
            return result

        self.assert_reads([(b"\x11\x12\x00", bools, [True, False])])

    def test_nested_structs_restore_the_last_field_id(self) -> None:
        def read(reader: ProtocolReader) -> list[tuple[int, int, str]]:
            reader.read_struct_begin()
            outer = reader.read_field_begin()
            reader.read_struct_begin()
            inner = reader.read_field_begin()
            reader.read_i32()
            reader.read_field_begin()
            reader.read_struct_end()
            after = reader.read_field_begin()
            reader.read_i32()
            return [outer, inner, after, reader.read_field_begin()]

        self.assert_reads(
            [
                (
                    b"\x5c\x15\x02\x00\x15\x04\x00",
                    read,
                    [
                        (T_STRUCT, 5, ""),
                        (T_I32, 1, ""),
                        (T_I32, 6, ""),
                        (T_STOP, 0, ""),
                    ],
                )
            ]
        )

    def test_read_collection_headers(self) -> None:
        self.assert_reads(
            [
                (b"\x35", lambda r: r.read_list_begin(), (T_I32, 3)),
                (b"\xe1", lambda r: r.read_list_begin(), (T_BOOL, 14)),
                (b"\xe2", lambda r: r.read_list_begin(), (T_BOOL, 14)),
                (b"\xf8\x0f", lambda r: r.read_list_begin(), (T_STRING, 15)),
                (b"\xfc\xc8\x01", lambda r: r.read_set_begin(), (T_STRUCT, 200)),
                (b"\x00", lambda r: r.read_map_begin(), (T_STOP, T_STOP, 0)),
                (b"\x02\x86", lambda r: r.read_map_begin(), (T_STRING, T_I64, 2)),
                (
                    b"\xc8\x01\x17",
                    lambda r: r.read_map_begin(),
                    (T_BOOL, T_DOUBLE, 200),
                ),
            ]
        )

    def test_read_scalars(self) -> None:
        self.assert_reads(
            [
                (b"\xff", lambda r: r.read_byte(), -1),
                (b"\x01", lambda r: r.read_i16(), -1),
                (b"\xff\xff\x03", lambda r: r.read_i16(), -(2**15)),
                (b"\xfe\xff\xff\xff\x0f", lambda r: r.read_i32(), 2**31 - 1),
                (b"\xff" * 9 + b"\x01", lambda r: r.read_i64(), -(2**63)),
                (b"\x80\x01", lambda r: r.read_i64(), 64),
                (b"\x3f\xf0" + b"\x00" * 6, lambda r: r.read_double(), 1.0),
                (b"\xc0\x20\x00\x00", lambda r: r.read_float(), -2.5),
                (b"\x02\xc3\xa9", lambda r: r.read_string(), "é"),
                (b"\x00", lambda r: r.read_binary(), b""),
                (b"\x01", lambda r: r.read_bool(), True),
                (b"\x02", lambda r: r.read_bool(), False),
                (b"\x00", lambda r: r.read_bool(), False),
                (b"\x03", lambda r: r.read_bool(), False),
                (b"\xff", lambda r: r.read_bool(), False),
            ]
        )

    def test_varints_keep_only_the_bits_of_their_width(self) -> None:
        self.assert_reads(
            [
                (b"\xff\xff\xff\xff\x7f", lambda r: r.read_i32(), -(2**31)),
                (b"\xfe\xff\xff\xff\x7f", lambda r: r.read_i32(), 2**31 - 1),
                (b"\x80\x80\x04", lambda r: r.read_i16(), -(2**15)),
                (b"\xff" * 9 + b"\x7f", lambda r: r.read_i64(), -(2**63)),
            ]
        )

    def test_rejects_malformed_values(self) -> None:
        self.assert_read_fails(
            [
                # Overlong varints.
                (b"\xff\xff\xff\xff\xff\x01", lambda r: r.read_i32()),
                (b"\x80\x80\x80\x80\x80\x00", lambda r: r.read_i16()),
                (b"\xff" * 10 + b"\x01", lambda r: r.read_i64()),
                (b"\xf5\xff\xff\xff\xff\xff\x01", lambda r: r.read_list_begin()),
                (b"\xff\xff\xff\xff\xff\x01", lambda r: r.read_binary()),
                (b"\x05\xff\xff\xff\xff\xff\x01", lambda r: r.read_field_begin()),
                # Negative sizes.
                (b"\xf5\xff\xff\xff\xff\x0f", lambda r: r.read_list_begin()),
                (b"\xf5\x80\x80\x80\x80\x08", lambda r: r.read_set_begin()),
                (b"\xff\xff\xff\xff\x0f\x88", lambda r: r.read_map_begin()),
                (b"\xff\xff\xff\xff\x0f", lambda r: r.read_map_begin()),
                (b"\xff\xff\xff\xff\x0f", lambda r: r.read_binary()),
                # Unknown compact types.
                (b"\x1e", lambda r: r.read_field_begin()),
                (b"\x0f\x02", lambda r: r.read_field_begin()),
                (b"\x3e", lambda r: r.read_list_begin()),
                (b"\x01\xe5", lambda r: r.read_map_begin()),
                (b"\x01\x5f", lambda r: r.read_map_begin()),
                # Sizes beyond the input, and invalid UTF-8.
                (b"\x05abc", lambda r: r.read_binary()),
                (b"\x01\xff", lambda r: r.read_string()),
            ]
        )

    def test_stop_ignores_the_high_nibble(self) -> None:
        self.assert_reads([(b"\x50", lambda r: r.read_field_begin(), (T_STOP, 0, ""))])

    def test_skip_every_type(self) -> None:
        self.assert_skips(
            [
                (T_BOOL, b"\x01"),
                (T_BYTE, b"\x80"),
                (T_I16, b"\xff\xff\x03"),
                (T_I32, b"\x02"),
                (T_I64, b"\xff" * 9 + b"\x01"),
                (T_FLOAT, b"\x00" * 4),
                (T_DOUBLE, b"\x00" * 8),
                (T_STRING, b"\x03abc"),
                (T_STRUCT, b"\x00"),
                (
                    T_STRUCT,
                    b"\x15\x0e"  # 1: i32
                    b"\x1c\x18\x01x\x00"  # 2: struct
                    b"\x19\x25\x02\x04"  # 3: list<i32>
                    b"\x11"  # 4: bool in the header
                    b"\x00",
                ),
                (T_LIST, b"\x25\x02\x04"),
                (T_LIST, b"\x28\x01a\x00"),
                (T_LIST, b"\x31\x01\x02\x01"),
                (T_LIST, b"\x05"),
                (T_SET, b"\x2c\x00\x00"),
                (T_MAP, b"\x01\x89\x01k\x21\x01\x02"),
                (T_MAP, b"\x00"),
                (T_MAP, b"\x01\x57\x04" + b"\x00" * 8),
            ]
        )

    def test_skip_checks_sizes_before_reading(self) -> None:
        self.assert_read_fails(
            [
                (b"\xf7\xff\xff\xff\xff\x07", lambda r: r.skip(T_LIST)),
                (b"\xfc\xff\xff\xff\xff\x07\x00", lambda r: r.skip(T_LIST)),
                (b"\xff\xff\xff\xff\x07\x77", lambda r: r.skip(T_MAP)),
                (b"\xff\xff\xff\xff\x0f", lambda r: r.skip(T_STRING)),
            ]
        )
