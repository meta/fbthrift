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

import functools
import math
import struct
import unittest

from thrift.lib.python.schema._errors import DecodeError, EncodeError
from thrift.lib.python.schema._protocols import (
    Backend,
    Protocol,
    ProtocolReader,
    ProtocolWriter,
)
from thrift.lib.python.schema._simple_json_protocol import (
    dump_json,
    JsonNumber,
    parse_json_prefix,
)
from thrift.lib.python.schema._ttypes import (
    NO_FIELD_ID,
    PROTOCOL_MAX_DEPTH,
    T_BOOL,
    T_BYTE,
    T_DOUBLE,
    T_FLOAT,
    T_I16,
    T_I32,
    T_I64,
    T_LIST,
    T_STOP,
    T_STRING,
    T_STRUCT,
    T_VOID,
)
from thrift.lib.python.schema.tests.protocol_testing import (
    ProtocolTestCase,
    ReadCall,
    WriteCall,
)


def _f32(value: float) -> float:
    return struct.unpack("<f", struct.pack("<f", value))[0]


def _struct_of(*fields: tuple[str, int, WriteCall]) -> WriteCall:
    def write(writer: ProtocolWriter) -> None:
        writer.write_struct_begin("S")
        for field_id, (name, ttype, value) in enumerate(fields, start=1):
            writer.write_field_begin(name, ttype, field_id)
            value(writer)
            writer.write_field_end()
        writer.write_field_stop()
        writer.write_struct_end()

    return write


def _map_of(key_ttype: int, *entries: tuple[WriteCall, WriteCall]) -> WriteCall:
    def write(writer: ProtocolWriter) -> None:
        writer.write_map_begin(key_ttype, T_I32, len(entries))
        for key, value in entries:
            key(writer)
            value(writer)
        writer.write_map_end()

    return write


def _list_of(*elements: WriteCall) -> WriteCall:
    def write(writer: ProtocolWriter) -> None:
        writer.write_list_begin(T_I32, len(elements))
        for element in elements:
            element(writer)
        writer.write_list_end()

    return write


def _one(writer: ProtocolWriter) -> None:
    writer.write_i32(1)


def _empty_set(writer: ProtocolWriter) -> None:
    writer.write_set_begin(T_I32, 0)
    writer.write_set_end()


def _raw_struct(writer: ProtocolWriter) -> None:
    writer.write_raw(b"{}")


class SimpleJsonWriterTest(ProtocolTestCase):
    """Byte-exact SimpleJSON, as `SimpleJSONProtocolWriter` writes it."""

    def written(self, backend: Backend, call: WriteCall) -> bytes:
        writer = backend.writers[Protocol.SIMPLE_JSON]()
        call(writer)
        return writer.getvalue()

    def assert_writes(self, cases: list[tuple[WriteCall, str]]) -> None:
        for backend in self.backends():
            for call, expected in cases:
                with self.subTest(expected=expected):
                    self.assertEqual(
                        self.written(backend, call), expected.encode("utf-8")
                    )

    def test_structs_use_field_names_and_no_whitespace(self) -> None:
        self.assert_writes(
            [
                (_struct_of(), "{}"),
                (
                    _struct_of(
                        ("a", T_I32, _one),
                        ("b", T_STRING, lambda w: w.write_string("x")),
                        ('q"uote', T_BOOL, lambda w: w.write_bool(False)),
                    ),
                    '{"a":1,"b":"x","q\\"uote":false}',
                ),
                (
                    _struct_of(("inner", T_STRUCT, _struct_of(("x", T_I32, _one)))),
                    '{"inner":{"x":1}}',
                ),
            ]
        )

    def test_lists_sets_and_maps(self) -> None:
        self.assert_writes(
            [
                (_list_of(), "[]"),
                (_list_of(_one, _one), "[1,1]"),
                (_list_of(_list_of(_one), _list_of()), "[[1],[]]"),
                (_map_of(T_STRING), "{}"),
                (
                    _map_of(
                        T_STRING,
                        (lambda w: w.write_string("k"), _list_of(_one)),
                        (lambda w: w.write_string("j"), _struct_of(("x", T_I32, _one))),
                    ),
                    '{"k":[1],"j":{"x":1}}',
                ),
                (_list_of(_map_of(T_I32, (_one, _one))), '[{"1":1}]'),
            ]
        )

    def test_map_keys_are_quoted_by_key_type(self) -> None:
        self.assert_writes(
            [
                (_map_of(T_I32, (_one, _one)), '{"1":1}'),
                (_map_of(T_BYTE, (lambda w: w.write_byte(-1), _one)), '{"-1":1}'),
                (_map_of(T_I16, (lambda w: w.write_i16(7), _one)), '{"7":1}'),
                (
                    _map_of(T_I64, (lambda w: w.write_i64(2**40), _one)),
                    '{"1099511627776":1}',
                ),
                (_map_of(T_BOOL, (lambda w: w.write_bool(True), _one)), '{"true":1}'),
                (_map_of(T_DOUBLE, (lambda w: w.write_double(1.5), _one)), '{"1.5":1}'),
                (_map_of(T_DOUBLE, (lambda w: w.write_double(-0.0), _one)), '{"-0":1}'),
                (
                    _map_of(T_DOUBLE, (lambda w: w.write_double(math.nan), _one)),
                    '{"NaN":1}',
                ),
                (
                    _map_of(T_FLOAT, (lambda w: w.write_float(_f32(0.1)), _one)),
                    '{"0.1":1}',
                ),
                (_map_of(T_STRING, (lambda w: w.write_string("s"), _one)), '{"s":1}'),
                (
                    _map_of(T_STRING, (lambda w: w.write_binary(b"\x00\x01"), _one)),
                    '{"AAE":1}',
                ),
            ]
        )

    def test_struct_and_container_map_keys_are_rejected(self) -> None:
        # C++ writes these unquoted, which is not JSON.
        keys: list[WriteCall] = [
            _struct_of(),
            _list_of(),
            _map_of(T_I32),
            _empty_set,
            _raw_struct,
        ]
        for backend in self.backends():
            for key in keys:
                with self.subTest(key=key):
                    with self.assertRaises(EncodeError):
                        self.written(backend, _map_of(T_STRUCT, (key, _one)))

    def test_scalars(self) -> None:
        self.assert_writes(
            [
                (lambda w: w.write_bool(True), "true"),
                (lambda w: w.write_bool(False), "false"),
                (lambda w: w.write_byte(-128), "-128"),
                (lambda w: w.write_i16(32767), "32767"),
                (lambda w: w.write_i32(-(2**31)), "-2147483648"),
                (lambda w: w.write_i64(2**63 - 1), "9223372036854775807"),
                (lambda w: w.write_binary(b""), '""'),
                (lambda w: w.write_binary(b"a"), '"YQ"'),
                (lambda w: w.write_binary(b"ab"), '"YWI"'),
                (lambda w: w.write_binary(b"abc"), '"YWJj"'),
                (lambda w: w.write_binary(b"\xfb\xff"), '"+/8"'),
                (lambda w: w.write_raw(b'{"x":1}'), '{"x":1}'),
            ]
        )

    def test_strings_escape_quotes_backslashes_and_controls(self) -> None:
        self.assert_writes(
            [
                (lambda w: w.write_string(""), '""'),
                (lambda w: w.write_string('a"b\\c'), '"a\\"b\\\\c"'),
                (
                    lambda w: w.write_string("\b\t\n\f\r\x00\x01\x1f\x7f"),
                    '"\\b\\t\\n\\f\\r\\u0000\\u0001\\u001f\x7f"',
                ),
                (lambda w: w.write_string("é ☃ 😀 /"), '"é ☃ 😀 /"'),
            ]
        )

    def test_doubles_follow_fmt(self) -> None:
        cases = [
            (1.0, "1"),
            (0.1, "0.1"),
            (1e16, "1e+16"),
            (1e15, "1000000000000000"),
            (1e-5, "1e-05"),
            (1e-4, "0.0001"),
            (-0.0, "-0"),
            (0.0, "0"),
            (123.456, "123.456"),
            (1.5e300, "1.5e+300"),
            (5e-324, "5e-324"),
            (1.7976931348623157e308, "1.7976931348623157e+308"),
            (1234567890123456.8, "1234567890123456.8"),
            (0.30000000000000004, "0.30000000000000004"),
            (1e22, "1e+22"),
            (2.2250738585072014e-308, "2.2250738585072014e-308"),
            (123456789012345680.0, "1.2345678901234568e+17"),
            (math.inf, '"Infinity"'),
            (-math.inf, '"-Infinity"'),
            (math.nan, '"NaN"'),
            (-math.nan, '"NaN"'),
        ]
        self.assert_writes(
            [
                (functools.partial(_write_double, value=value), text)
                for value, text in cases
            ]
        )

    def test_floats_follow_fmt_9(self) -> None:
        cases = [
            (0.1, "0.1"),
            (1.0, "1"),
            (3.4028234663852886e38, "3.4028235e+38"),
            (1.401298464324817e-45, "1e-45"),
            (1.1754943508222875e-38, "1.1754944e-38"),
            (1234567.0, "1234567"),
            (0.3, "0.3"),
            (-2.5, "-2.5"),
            (1e-5, "1e-05"),
            (-0.0, "-0"),
            (3.1415927, "3.1415927"),
            (9.999999e-5, "9.999999e-05"),
            (8388609.0, "8388609"),
            (5e-39, "5e-39"),
            # fmt 12 switches float32 to exponent notation from 1e7 up.
            (16777216.0, "16777216"),
            (123456789.0, "123456790"),
            (1e15, "1000000000000000"),
            (1e16, "1e+16"),
            (math.inf, '"Infinity"'),
            (math.nan, '"NaN"'),
        ]
        self.assert_writes(
            [
                (functools.partial(_write_float, value=_f32(value)), text)
                for value, text in cases
            ]
        )


def _write_double(writer: ProtocolWriter, value: float) -> None:
    writer.write_double(value)


def _write_float(writer: ProtocolWriter, value: float) -> None:
    writer.write_float(value)


def _bits(value: object) -> object:
    """A float as its bits, so that -0.0 and NaN compare exactly."""
    return struct.pack(">d", value) if isinstance(value, float) else value


def _read_bool(reader: ProtocolReader) -> object:
    return reader.read_bool()


def _read_byte(reader: ProtocolReader) -> object:
    return reader.read_byte()


def _read_i16(reader: ProtocolReader) -> object:
    return reader.read_i16()


def _read_i32(reader: ProtocolReader) -> object:
    return reader.read_i32()


def _read_i64(reader: ProtocolReader) -> object:
    return reader.read_i64()


def _read_float(reader: ProtocolReader) -> object:
    return reader.read_float()


def _read_double(reader: ProtocolReader) -> object:
    return reader.read_double()


def _read_string(reader: ProtocolReader) -> object:
    return reader.read_string()


def _read_binary(reader: ProtocolReader) -> object:
    return reader.read_binary()


def _read_raw(reader: ProtocolReader) -> object:
    return reader.read_raw(T_STRUCT)


def _fields(reader: ProtocolReader) -> object:
    """A struct's fields, as names and raw values."""
    fields = []
    reader.read_struct_begin()
    while True:
        ttype, _, name = reader.read_field_begin()
        if ttype == T_STOP:
            break
        fields.append((name, reader.read_raw(T_VOID)))
        reader.read_field_end()
    reader.read_struct_end()
    return fields


def _read_list_of(read_element: ReadCall) -> ReadCall:
    def read(reader: ProtocolReader) -> object:
        _, size = reader.read_list_begin()
        elements = [read_element(reader) for _ in range(size)]
        reader.read_list_end()
        return elements

    return read


def _read_set_of(read_element: ReadCall) -> ReadCall:
    def read(reader: ProtocolReader) -> object:
        _, size = reader.read_set_begin()
        elements = [read_element(reader) for _ in range(size)]
        reader.read_set_end()
        return elements

    return read


def _read_map_of(read_key: ReadCall, read_value: ReadCall) -> ReadCall:
    def read(reader: ProtocolReader) -> object:
        _, _, size = reader.read_map_begin()
        entries = [(read_key(reader), read_value(reader)) for _ in range(size)]
        reader.read_map_end()
        return entries

    return read


def _key(read_key: ReadCall) -> ReadCall:
    """The key of a one-entry map to i32."""

    def read(reader: ProtocolReader) -> object:
        reader.read_map_begin()
        key = read_key(reader)
        reader.read_i32()
        reader.read_map_end()
        return key

    return read


def _elements(count: int) -> ReadCall:
    """A list read as if it had ``count`` i32 elements."""

    def read(reader: ProtocolReader) -> object:
        reader.read_list_begin()
        for _ in range(count):
            reader.read_i32()
        reader.read_list_end()
        return None

    return read


def _nested_lists(levels: int) -> str:
    return "[" * levels + "]" * levels


def _open_lists(levels: int) -> ReadCall:
    def read(reader: ProtocolReader) -> object:
        for _ in range(levels):
            reader.read_list_begin()
        return None

    return read


class SimpleJsonReaderTest(ProtocolTestCase):
    """SimpleJSON as `SimpleJSONProtocolReader` reads it."""

    def reader(self, backend: Backend, text: str | bytes) -> ProtocolReader:
        data = text.encode("utf-8") if isinstance(text, str) else text
        return backend.readers[Protocol.SIMPLE_JSON](data)

    def assert_reads(self, cases: list[tuple[str | bytes, ReadCall, object]]) -> None:
        """Each read consumes the whole document."""
        for backend in self.backends():
            for text, call, expected in cases:
                with self.subTest(text=text):
                    reader = self.reader(backend, text)
                    self.assertEqual(_bits(call(reader)), _bits(expected))
                    self.assertEqual(reader.remaining(), 0)

    def assert_read_fails(self, cases: list[tuple[str | bytes, ReadCall]]) -> None:
        for backend in self.backends():
            for text, call in cases:
                with self.subTest(text=text):
                    with self.assertRaises(DecodeError):
                        call(self.reader(backend, text))

    def test_read_scalars(self) -> None:
        self.assert_reads(
            [
                ("true", _read_bool, True),
                ("false", _read_bool, False),
                ("-128", _read_byte, -128),
                ("32767", _read_i16, 32767),
                ("-2147483648", _read_i32, -(2**31)),
                ("9223372036854775807", _read_i64, 2**63 - 1),
                ("-0", _read_i32, 0),
                (" \t\n7\r\n ", _read_i32, 7),
                ("1.5", _read_double, 1.5),
                ("3", _read_double, 3.0),
                ("-0", _read_double, -0.0),
                ("1E2", _read_double, 100.0),
                ("1e400", _read_double, math.inf),
                ("-1e-400", _read_double, -0.0),
                ('"NaN"', _read_double, math.nan),
                ('"-NaN"', _read_double, -math.nan),
                ('"Infinity"', _read_double, math.inf),
                ('"-Infinity"', _read_double, -math.inf),
                ("0.1", _read_float, _f32(0.1)),
                ("3.4028235e+38", _read_float, _f32(3.4028234663852886e38)),
                ("3.5e38", _read_float, math.inf),
                ('"NaN"', _read_float, math.nan),
                ('"h\\u00e9 \\ud83d\\ude00 \\/"', _read_string, "hé 😀 /"),
                # C++ accepts raw control characters in strings.
                ('"a\tb\x01"', _read_string, "a\tb\x01"),
                ('""', _read_binary, b""),
                ('"YWJj"', _read_binary, b"abc"),
                ('"YQ"', _read_binary, b"a"),
                ('"YQ=="', _read_binary, b"a"),
                ('"+/8"', _read_binary, b"\xfb\xff"),
            ]
        )

    def test_rejects_malformed_scalars(self) -> None:
        self.assert_read_fails(
            [
                # Only map keys are quoted.
                ('"1"', _read_i32),
                ('"true"', _read_bool),
                ('"1.5"', _read_double),
                ("1.0", _read_i32),
                ("1e2", _read_i64),
                ("2147483648", _read_i32),
                ("128", _read_byte),
                ("-32769", _read_i16),
                ("1" * 30, _read_i64),
                ("true", _read_i32),
                ("null", _read_i32),
                ("[]", _read_i64),
                ("1", _read_bool),
                ("null", _read_bool),
                ('"nan"', _read_double),
                ("true", _read_float),
                ("1", _read_string),
                ("null", _read_string),
                ('"\\ud800"', _read_string),
                ("1", _read_binary),
                ('"Y"', _read_binary),
                ('"Y!=="', _read_binary),
            ]
        )

    def test_rejects_malformed_json(self) -> None:
        self.assert_read_fails(
            [
                ("", _read_i32),
                ("  ", _read_i32),
                ("tru", _read_bool),
                ("01", _read_i32),
                ("+1", _read_i32),
                ("NaN", _read_double),
                ("Infinity", _read_double),
                ("-Infinity", _read_double),
                ("1 2", _read_i32),
                ("[1] x", _read_list_of(_read_i32)),
                ("[1,]", _read_list_of(_read_i32)),
                ('{"a":1,}', _fields),
                ("{'a':1}", _fields),
                ('{"a" 1}', _fields),
                ('"abc', _read_string),
                (b'"\xff"', _read_string),
                ("\ufeff1", _read_i32),
            ]
        )

    def test_fields_are_named_and_untyped(self) -> None:
        for backend in self.backends():
            data = '{"a":1}'
            reader = self.reader(backend, data)
            self.assertEqual(reader.remaining(), len(data))
            reader.read_struct_begin()
            self.assertEqual(reader.read_field_begin(), (T_VOID, NO_FIELD_ID, "a"))
            self.assertEqual(reader.read_i32(), 1)
            reader.read_field_end()
            self.assertEqual(reader.read_field_begin(), (T_STOP, 0, ""))
            reader.read_struct_end()
            self.assertEqual(reader.remaining(), 0)

    def test_read_fields(self) -> None:
        self.assert_reads(
            [
                ("{}", _fields, []),
                (' { "a" : 1 , "b":"x" } ', _fields, [("a", b"1"), ("b", b'"x"')]),
                ('{"a":1,"a":2}', _fields, [("a", b"1"), ("a", b"2")]),
                ('{"q\\"uote":true}', _fields, [('q"uote', b"true")]),
                ('{"inner":{"x":[1, 2]}}', _fields, [("inner", b'{"x":[1,2]}')]),
                # C++ skips a field whose value is null.
                ('{"a":null,"b":1,"c":null}', _fields, [("b", b"1")]),
                ('{"a":null}', _fields, []),
            ]
        )

    def test_container_headers_have_sizes_but_no_wire_types(self) -> None:
        for backend in self.backends():
            reader = self.reader(backend, '[[1,2],[3],{"k":1,"j":2,"i":3}]')
            self.assertEqual(reader.read_list_begin(), (T_VOID, 3))
            self.assertEqual(reader.read_set_begin(), (T_VOID, 2))
            for _ in range(2):
                reader.skip(T_I32)
            reader.read_set_end()
            self.assertEqual(reader.read_list_begin(), (T_VOID, 1))
            reader.skip(T_I32)
            reader.read_list_end()
            self.assertEqual(reader.read_map_begin(), (T_VOID, T_VOID, 3))
            for _ in range(6):
                reader.skip(T_VOID)
            reader.read_map_end()
            reader.read_list_end()
            self.assertEqual(reader.remaining(), 0)

    def test_read_containers(self) -> None:
        self.assert_reads(
            [
                ("[]", _read_list_of(_read_i32), []),
                (" [ 1 , 2,3 ] ", _read_list_of(_read_i32), [1, 2, 3]),
                ("[[1],[]]", _read_list_of(_read_list_of(_read_i32)), [[1], []]),
                ('["a","b"]', _read_set_of(_read_string), ["a", "b"]),
                ("{}", _read_map_of(_read_i32, _read_i32), []),
                (
                    '{"1":2,"-3":4}',
                    _read_map_of(_read_i32, _read_i32),
                    [(1, 2), (-3, 4)],
                ),
                (
                    '{"k":[1],"k":[]}',
                    _read_map_of(_read_string, _read_list_of(_read_i32)),
                    [("k", [1]), ("k", [])],
                ),
                ('[{"x":1}]', _read_list_of(_fields), [[("x", b"1")]]),
            ]
        )

    def test_read_map_keys(self) -> None:
        self.assert_reads(
            [
                ('{"-128":0}', _key(_read_byte), -128),
                ('{"32767":0}', _key(_read_i16), 32767),
                ('{"-0":0}', _key(_read_i32), 0),
                ('{"9223372036854775807":0}', _key(_read_i64), 2**63 - 1),
                ('{"true":0}', _key(_read_bool), True),
                ('{"false":0}', _key(_read_bool), False),
                ('{"1.5":0}', _key(_read_double), 1.5),
                ('{"-0":0}', _key(_read_double), -0.0),
                ('{"1e+16":0}', _key(_read_double), 1e16),
                ('{"NaN":0}', _key(_read_double), math.nan),
                ('{"-Infinity":0}', _key(_read_double), -math.inf),
                ('{"0.1":0}', _key(_read_float), _f32(0.1)),
                ('{"s":0}', _key(_read_string), "s"),
                ('{"\\u00e9":0}', _key(_read_string), "é"),
                ('{"AAE":0}', _key(_read_binary), b"\x00\x01"),
            ]
        )

    def test_rejects_malformed_map_keys(self) -> None:
        self.assert_read_fails(
            [
                ('{"x":0}', _key(_read_i32)),
                ('{"1.0":0}', _key(_read_i32)),
                ('{"01":0}', _key(_read_i32)),
                ('{"+1":0}', _key(_read_i32)),
                ('{"128":0}', _key(_read_byte)),
                ('{"True":0}', _key(_read_bool)),
                ('{"1":0}', _key(_read_bool)),
                ('{"":0}', _key(_read_double)),
                ('{"nan":0}', _key(_read_double)),
                ('{"Y":0}', _key(_read_binary)),
                ('{"\\ud800":0}', _key(_read_string)),
                # Keys are strings, never structs or containers.
                ('{"{}":0}', _key(_fields)),
                ('{"[]":0}', _key(_read_list_of(_read_i32))),
                ('{"{}":0}', _key(_read_map_of(_read_i32, _read_i32))),
            ]
        )

    def test_rejects_mismatched_and_partly_read_containers(self) -> None:
        self.assert_read_fails(
            [
                ("[]", _fields),
                ("null", _fields),
                ('"{}"', _fields),
                ("{}", _read_list_of(_read_i32)),
                ("{}", _read_set_of(_read_i32)),
                ("[]", _read_map_of(_read_i32, _read_i32)),
                ("[null]", _read_list_of(_read_i32)),
                ('{"k":null}', _read_map_of(_read_string, _read_i32)),
                ("[1,2]", _elements(1)),
                ("[1]", _elements(2)),
                ('{"a":1}', lambda r: (r.read_struct_begin(), r.read_struct_end())),
                ("1", lambda r: (r.read_i32(), r.read_i32())),
            ]
        )

    def test_skip_consumes_any_value_whatever_the_wire_type(self) -> None:
        values = ['{"a":[1,{"b":null}],"c":{}}', "[]", '"s"', "-1.5e3", "true", "null"]
        for backend in self.backends():
            for ttype in (T_VOID, T_I32, T_STRUCT):
                with self.subTest(ttype=ttype):
                    reader = self.reader(backend, f"[{','.join(values)}]")
                    reader.read_list_begin()
                    for _ in values:
                        reader.skip(ttype)
                    reader.read_list_end()
                    self.assertEqual(reader.remaining(), 0)

            reader = self.reader(backend, '{"unknown":{"x":[1]},"a":1}')
            reader.read_struct_begin()
            self.assertEqual(
                reader.read_field_begin(), (T_VOID, NO_FIELD_ID, "unknown")
            )
            reader.skip(T_STRUCT)
            reader.read_field_end()
            self.assertEqual(reader.read_field_begin(), (T_VOID, NO_FIELD_ID, "a"))
            self.assertEqual(reader.read_i32(), 1)
            reader.read_field_end()
            self.assertEqual(reader.read_field_begin(), (T_STOP, 0, ""))
            reader.read_struct_end()
            self.assertEqual(reader.remaining(), 0)

    def test_read_raw_keeps_numbers_as_written(self) -> None:
        self.assert_reads(
            [
                (
                    ' { "a" : [ 1 , 1.50 , -0 , 1E5 ] } ',
                    _read_raw,
                    b'{"a":[1,1.50,-0,1E5]}',
                ),
                ('"\\u00e9\\n"', _read_raw, b'"\\u00e9\\n"'),
                ("[true,false,null]", _read_raw, b"[true,false,null]"),
                (_nested_lists(100), _read_raw, _nested_lists(100).encode()),
            ]
        )

    def test_skip_depth_limit_matches_cpp(self) -> None:
        # C++ `skip` fails when its depth reaches FLAGS_thrift_protocol_max_depth,
        # and skips each nested value one level deeper.
        for backend in self.backends():
            for text in ("1", "[]", "{}"):
                reader = self.reader(backend, text)
                reader.skip(T_VOID, PROTOCOL_MAX_DEPTH - 1)
                self.assertEqual(reader.remaining(), 0)
            cases = [
                ("1", PROTOCOL_MAX_DEPTH),
                ("[1]", PROTOCOL_MAX_DEPTH - 1),
                ('{"a":1}', PROTOCOL_MAX_DEPTH - 1),
            ]
            for text, depth in cases:
                with self.subTest(text=text):
                    with self.assertRaises(DecodeError):
                        self.reader(backend, text).skip(T_VOID, depth)

    def test_nesting(self) -> None:
        levels = 500
        too_deep = PROTOCOL_MAX_DEPTH + 1
        for backend in self.backends():
            reader = self.reader(backend, _nested_lists(levels))
            _open_lists(levels)(reader)
            for _ in range(levels):
                reader.read_list_end()
            self.assertEqual(reader.remaining(), 0)

            reader = self.reader(backend, _nested_lists(levels))
            reader.skip(T_LIST)
            self.assertEqual(reader.remaining(), 0)

            # Python's parser may stop short of the C++ limit; either way the
            # input is a DecodeError.
            with self.assertRaises(DecodeError):
                _open_lists(too_deep)(self.reader(backend, _nested_lists(too_deep)))
            with self.assertRaises(DecodeError):
                self.reader(backend, _nested_lists(too_deep)).skip(T_LIST)
            with self.assertRaises(DecodeError):
                self.reader(backend, _nested_lists(1_000_000))

    def test_reads_what_the_writer_writes(self) -> None:
        doubles = [
            0.0,
            -0.0,
            0.1,
            1e16,
            1e-5,
            5e-324,
            2.2250738585072014e-308,
            1.7976931348623157e308,
            123456789012345680.0,
            math.inf,
            -math.inf,
            math.nan,
        ]
        floats = [
            _f32(value)
            for value in (
                0.1,
                -0.0,
                3.4028234663852886e38,
                1.401298464324817e-45,
                1.1754943508222875e-38,
                9.999999e-5,
                123456789.0,
                16777216.0,
                math.inf,
            )
        ]
        string = '\b\t\n\f\r\x00\x01\x1f\x7f"\\ é ☃ 😀 /'
        cases: list[tuple[WriteCall, ReadCall, object]] = [
            *[
                (functools.partial(_write_double, value=value), _read_double, value)
                for value in doubles
            ],
            *[
                (functools.partial(_write_float, value=value), _read_float, value)
                for value in floats
            ],
            *[(_double_key(value), _key(_read_double), value) for value in doubles],
            (lambda w: w.write_i64(-(2**63)), _read_i64, -(2**63)),
            (lambda w: w.write_string(string), _read_string, string),
            (
                lambda w: w.write_binary(bytes(range(256))),
                _read_binary,
                bytes(range(256)),
            ),
        ]
        for backend in self.backends():
            for write, read, expected in cases:
                writer = backend.writers[Protocol.SIMPLE_JSON]()
                write(writer)
                data = writer.getvalue()
                with self.subTest(data=data):
                    reader = self.reader(backend, data)
                    self.assertEqual(_bits(read(reader)), _bits(expected))
                    self.assertEqual(reader.remaining(), 0)


def _double_key(value: float) -> WriteCall:
    def write(writer: ProtocolWriter) -> None:
        writer.write_map_begin(T_DOUBLE, T_I32, 1)
        writer.write_double(value)
        writer.write_i32(0)
        writer.write_map_end()

    return write


class ParseJsonPrefixTest(unittest.TestCase):
    """The JSON value that starts a string, as a path selector is read."""

    def test_returns_the_value_and_the_index_past_it(self) -> None:
        cases = [
            ('"a.b[0]{}"]rest', '"a.b[0]{}"', 10),
            ("-42}", "-42", 3),
            ("[1,[2]]]", "[1,[2]]", 7),
            ('{"k":true}}', '{"k":true}', 10),
            ("null", "null", 4),
        ]
        for text, value, end in cases:
            with self.subTest(text=text):
                node, stop = parse_json_prefix(text)
                self.assertEqual(dump_json(node), value)
                self.assertEqual(stop, end)

    def test_starts_at_the_given_index(self) -> None:
        node, end = parse_json_prefix('set<string>{"x"}', 12)
        self.assertEqual(node, "x")
        self.assertEqual(end, 15)

    def test_keeps_number_literals(self) -> None:
        node, _ = parse_json_prefix("-0}")
        self.assertIsInstance(node, JsonNumber)
        assert isinstance(node, JsonNumber)
        self.assertEqual(node.literal, "-0")
        node, _ = parse_json_prefix("1.50e+3]")
        assert isinstance(node, JsonNumber)
        self.assertEqual(node.literal, "1.50e+3")

    def test_accepts_raw_control_characters_in_strings(self) -> None:
        node, end = parse_json_prefix('"a\tb"\n')
        self.assertEqual(node, "a\tb")
        self.assertEqual(end, 5)

    def test_rejects_what_is_not_json(self) -> None:
        for text, start in [
            ("NaN", 0),
            ("Infinity", 0),
            ("not-json", 0),
            ('"open', 0),
            ("", 0),
            ("[1]", 3),
            (" 1", 0),
        ]:
            with self.subTest(text=text, start=start):
                with self.assertRaises(DecodeError):
                    parse_json_prefix(text, start)
