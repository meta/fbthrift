#!/usr/bin/env python
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


# This starts up a bunch of servers, one for each of the server type
# and socket typecombinations we have. It then runs through the tests
# for each server, which entails connecting to calling a method on the
# server, asserting something about that method, and then closing the
# connection

from __future__ import absolute_import, division, print_function, unicode_literals

import importlib.resources
import os
import string
import sys
import tempfile
import time
import unittest
from subprocess import Popen

from thrift.protocol import (
    TBinaryProtocol,
    TCompactProtocol,
    THeaderProtocol,
    TMultiplexedProtocol,
)
from thrift.transport import TSocket, TSSLSocket, TTransport
from thrift.transport.THeaderTransport import CLIENT_TYPE, THeaderTransport, TRANSFORM
from ThriftTest import SecondService, ThriftTest
from ThriftTest.ttypes import Xception, Xtruct


_servers = []
_ports = {}

try:
    from thrift.protocol import fastproto
except ImportError:
    fastproto = None


def start_server(server_type, ssl, server_header, server_context, multiple, port_file):
    server_bin = str(
        importlib.resources.files(__package__).joinpath("python_test_server")
    )

    args = [server_bin, "--port", "0", "--port-file", port_file]
    if ssl:
        args.append("--ssl")
    if server_header:
        args.append("--header")
    if server_context:
        args.append("--context")
    if multiple:
        args.append("--multiple")
    args.append(server_type)
    stdout = None
    stderr = None
    if sys.stdout.isatty():
        stdout = sys.stdout
        stderr = sys.stderr
    return Popen(args, stdout=stdout, stderr=stderr)


def wait_for_port(server, port_file, timeout):
    """Returns the port the server bound to, or None if it exited or timed out."""
    end = time.time() + timeout
    while time.time() < end:
        if server.poll() is not None:
            return None
        try:
            with open(port_file) as f:
                return int(f.read())
        except FileNotFoundError:
            pass
        time.sleep(0.05)
    return None


class AbstractTest:
    @classmethod
    def setUpClass(cls):
        cls._tmpdir = tempfile.TemporaryDirectory()
        port_file = os.path.join(cls._tmpdir.name, "port")
        server = start_server(
            cls.server_type,
            cls.ssl,
            cls.server_header,
            cls.server_context,
            cls.multiple,
            port_file,
        )

        port = wait_for_port(server, port_file, 15.0)
        if port is None:
            server.kill()
            returncode = server.wait()
            cls._tmpdir.cleanup()
            msg = "Failed to start " + cls.server_type
            if cls.ssl:
                msg += " using ssl"
            if cls.server_header:
                msg += " using header protocol"
            if cls.server_context:
                msg += " using context"
            msg += " (exit code {})".format(returncode)
            raise Exception(msg)

        cls._port = port
        cls._server = server

    @classmethod
    def tearDownClass(cls):
        cls._server.kill()
        cls._server.wait()
        cls._tmpdir.cleanup()

    def bytes_comp(self, seq1, seq2):
        if not isinstance(seq1, bytes):
            seq1 = seq1.encode("utf-8")
        if not isinstance(seq2, bytes):
            seq2 = seq2.encode("utf-8")
        self.assertEqual(seq1, seq2)

    def setUp(self):
        if self.ssl:
            self.socket = TSSLSocket.TSSLSocket("localhost", self._port)
        else:
            self.socket = TSocket.TSocket("localhost", self._port)

        self.transport = TTransport.TBufferedTransport(self.socket)
        self.protocol = self.protocol_factory.getProtocol(self.transport)
        if isinstance(self, HeaderAcceleratedCompactTest):
            self.protocol.trans.set_protocol_id(
                THeaderProtocol.THeaderProtocol.T_COMPACT_PROTOCOL
            )
            self.protocol.reset_protocol()
        self.transport.open()
        self.client = ThriftTest.Client(self.protocol)
        if self.multiple:
            p = TMultiplexedProtocol.TMultiplexedProtocol(self.protocol, "ThriftTest")
            self.client = ThriftTest.Client(p)
            p = TMultiplexedProtocol.TMultiplexedProtocol(
                self.protocol, "SecondService"
            )
            self.client2 = SecondService.Client(p)
        else:
            self.client = ThriftTest.Client(self.protocol)
            self.client2 = None

    def tearDown(self):
        self.transport.close()

    def testVoid(self):
        self.client.testVoid()

    def testString(self):
        self.bytes_comp(self.client.testString("Python"), "Python")

    def testByte(self):
        self.assertEqual(self.client.testByte(63), 63)

    def testI32(self):
        self.assertEqual(self.client.testI32(-1), -1)
        self.assertEqual(self.client.testI32(0), 0)

    def testI64(self):
        self.assertEqual(self.client.testI64(-34359738368), -34359738368)

    def testDouble(self):
        self.assertEqual(self.client.testDouble(-5.235098235), -5.235098235)

    def testFloat(self):
        self.assertEqual(self.client.testFloat(-5.25), -5.25)

    def testStruct(self):
        x = Xtruct()
        x.string_thing = "Zero"
        x.byte_thing = 1
        x.i32_thing = -3
        x.i64_thing = -5
        y = self.client.testStruct(x)

        self.bytes_comp(y.string_thing, "Zero")
        self.assertEqual(y.byte_thing, 1)
        self.assertEqual(y.i32_thing, -3)
        self.assertEqual(y.i64_thing, -5)

    def testException(self):
        self.client.testException("Safe")
        try:
            self.client.testException("Xception")
            self.fail("should have gotten exception")
        except Xception as x:
            self.assertEqual(x.errorCode, 1001)
            self.bytes_comp(x.message, "Xception")  # noqa

        try:
            self.client.testException("throw_undeclared")
            self.fail("should have thrown exception")
        except Exception:  # type is undefined
            pass

    def testOneway(self):
        start = time.time()
        self.client.testOneway(1)
        end = time.time()
        self.assertTrue(end - start < 0.2, "oneway sleep took %f sec" % (end - start))

    def testblahBlah(self):
        if self.client2:
            self.assertEqual(self.client2.blahBlah(), None)


class NormalBinaryTest(AbstractTest):
    protocol_factory = TBinaryProtocol.TBinaryProtocolFactory()


class AcceleratedBinaryTest(AbstractTest):
    protocol_factory = TBinaryProtocol.TBinaryProtocolAcceleratedFactory()


class HeaderBase(AbstractTest):
    protocol_factory = THeaderProtocol.THeaderProtocolFactory(
        True,
        [
            CLIENT_TYPE.HEADER,
            CLIENT_TYPE.FRAMED_DEPRECATED,
            CLIENT_TYPE.UNFRAMED_DEPRECATED,
            CLIENT_TYPE.HTTP_SERVER,
        ],
    )


class HeaderTest(HeaderBase):
    def testZlibCompression(self):
        htrans = self.protocol.trans
        if isinstance(htrans, THeaderTransport):
            htrans.add_transform(TRANSFORM.ZLIB)
            self.testStruct()

    def testSnappyCompression(self):
        htrans = self.protocol.trans
        if isinstance(htrans, THeaderTransport):
            htrans.add_transform(TRANSFORM.SNAPPY)
            self.testStruct()

    def testMultipleCompression(self):
        htrans = self.protocol.trans
        if isinstance(htrans, THeaderTransport):
            htrans.add_transform(TRANSFORM.ZLIB)
            htrans.add_transform(TRANSFORM.SNAPPY)
            self.testStruct()

    def testKeyValueHeader(self):
        htrans = self.protocol.trans
        if isinstance(htrans, THeaderTransport):
            # Try just persistent header
            htrans.set_persistent_header("permanent", "true")
            self.client.testString("test")
            headers = htrans.get_headers()
            self.assertTrue("permanent" in headers)
            self.assertEqual(headers["permanent"], "true")

            # Try with two transient headers
            htrans.set_header("transient1", "true")
            htrans.set_header("transient2", "true")
            self.client.testString("test")
            headers = htrans.get_headers()
            self.assertTrue("permanent" in headers)
            self.assertEqual(headers["permanent"], "true")
            self.assertTrue("transient1" in headers)
            self.assertEqual(headers["transient1"], "true")
            self.assertTrue("transient2" in headers)
            self.assertEqual(headers["transient2"], "true")

            # Add one, update one and delete one transient header
            htrans.set_header("transient2", "false")
            htrans.set_header("transient3", "true")
            self.client.testString("test")
            headers = htrans.get_headers()
            self.assertTrue("permanent" in headers)
            self.assertEqual(headers["permanent"], "true")
            self.assertTrue("transient1" not in headers)
            self.assertTrue("transient2" in headers)
            self.assertEqual(headers["transient2"], "false")
            self.assertTrue("transient3" in headers)
            self.assertEqual(headers["transient3"], "true")


class HeaderAcceleratedCompactTest(HeaderBase):
    pass


class HeaderFramedCompactTest(HeaderBase):
    def setUp(self):
        self.socket = TSocket.TSocket("localhost", self._port)
        self.transport = TTransport.TFramedTransport(self.socket)
        self.protocol = TCompactProtocol.TCompactProtocol(self.transport)
        self.transport.open()
        self.client = ThriftTest.Client(self.protocol)
        self.client2 = None


class HeaderFramedBinaryTest(HeaderBase):
    def setUp(self):
        self.socket = TSocket.TSocket("localhost", self._port)
        self.transport = TTransport.TFramedTransport(self.socket)
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.transport.open()
        self.client = ThriftTest.Client(self.protocol)
        self.client2 = None


def camelcase(s):
    if not s[0].isupper():
        if sys.version_info[0] >= 3:
            s = "".join([x.capitalize() for x in s.split("_")])
        else:
            s = "".join(map(string.capitalize, s.split("_")))
    return s


def class_name_mixin(k, v):
    mixin = camelcase(k)
    if isinstance(v, bool):
        mixin += "On" if v else "Off"
    else:
        mixin += camelcase(v)
    return mixin


def new_test_class(cls, vars):
    template = ""
    name = cls.__name__
    for k, v in sorted(vars.items()):
        name += class_name_mixin(k, v)
        template += "  {} = {!r}\n".format(k, v)
    template = "class {}(cls, unittest.TestCase):\n".format(name) + template
    namespace = {"cls": cls}
    exec(template, globals(), namespace)
    return namespace[name]


def add_test_classes(module):
    classes = []
    for server_context in (True, False):
        for multiple in (True, False):
            config1 = {
                "server_type": "TCppServer",
                "ssl": False,
                "server_header": False,
                "server_context": server_context,
                "multiple": multiple,
            }
            classes.append(new_test_class(NormalBinaryTest, config1))
            classes.append(new_test_class(AcceleratedBinaryTest, config1))

    config2 = {
        "server_type": "TCppServer",
        "ssl": False,
        "server_header": False,
        "server_context": False,
        "multiple": False,
    }

    if fastproto is not None:
        classes.append(new_test_class(HeaderAcceleratedCompactTest, config2))

    for header in (
        HeaderFramedCompactTest,
        HeaderFramedBinaryTest,
    ):
        classes.append(new_test_class(header, config2))

    for cls in classes:
        setattr(module, cls.__name__, cls)


add_test_classes(sys.modules[__name__])
