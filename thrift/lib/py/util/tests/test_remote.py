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

import argparse
import unittest
from unittest import mock

from thrift.util import remote as remote_module
from thrift.util.remote import Remote, RemoteHostClient


def _client(host="localhost:1234", ssl=True, framed=True, certfile=None, keyfile=None):
    return RemoteHostClient(
        {},
        [],
        object,
        None,
        lambda *args, **kwargs: None,
        9090,
    ), argparse.Namespace(
        host=host,
        ssl=ssl,
        framed=framed,
        unframed=False,
        json=False,
        compact=False,
        headers=None,
        certfile=certfile,
        keyfile=keyfile,
    )


class TestRemoteHostClientCerts(unittest.TestCase):
    def _socket_kwargs(self, options):
        client, _ = _client()
        with (
            mock.patch.object(
                remote_module.TSSLSocket, "TSSLSocket", return_value=mock.Mock()
            ) as socket_cls,
            mock.patch.object(
                RemoteHostClient, "_get_client_by_transport", return_value=mock.Mock()
            ),
        ):
            client._get_client(options)
        return socket_cls.call_args

    def test_explicit_certfile_and_keyfile_reach_socket(self):
        client, opts = _client(
            certfile="/certs/client.pem", keyfile="/certs/client.key"
        )
        with (
            mock.patch.object(
                remote_module.TSSLSocket, "TSSLSocket", return_value=mock.Mock()
            ) as socket_cls,
            mock.patch.object(
                RemoteHostClient, "_get_client_by_transport", return_value=mock.Mock()
            ),
        ):
            client._get_client(opts)
        _, kwargs = socket_cls.call_args
        self.assertEqual(kwargs["certfile"], "/certs/client.pem")
        self.assertEqual(kwargs["keyfile"], "/certs/client.key")

    def test_env_pair_reaches_socket_when_readable(self):
        with (
            mock.patch.dict(
                "os.environ",
                {
                    "THRIFT_TLS_CL_CERT_PATH": "/env/client.pem",
                    "THRIFT_TLS_CL_KEY_PATH": "/env/client.key",
                },
            ),
            mock.patch("os.path.exists", return_value=True),
            mock.patch("os.access", return_value=True),
        ):
            _, kwargs = self._socket_kwargs(_client()[1])
        self.assertEqual(kwargs["certfile"], "/env/client.pem")
        self.assertEqual(kwargs["keyfile"], "/env/client.key")

    def test_no_env_pair_means_no_cert_even_if_identity_files_exist(self):
        with (
            mock.patch.dict("os.environ", {}, clear=True),
            mock.patch("os.path.exists", return_value=True),
            mock.patch("os.access", return_value=True),
        ):
            _, kwargs = self._socket_kwargs(_client()[1])
        self.assertIsNone(kwargs["certfile"])
        self.assertIsNone(kwargs["keyfile"])

    def test_partial_env_pair_means_no_cert(self):
        with (
            mock.patch.dict(
                "os.environ",
                {"THRIFT_TLS_CL_CERT_PATH": "/env/client.pem"},
                clear=True,
            ),
            mock.patch("os.path.exists", return_value=True),
            mock.patch("os.access", return_value=True),
        ):
            _, kwargs = self._socket_kwargs(_client()[1])
        self.assertIsNone(kwargs["certfile"])
        self.assertIsNone(kwargs["keyfile"])

    def test_keyfile_without_certfile_is_usage_error(self):
        client, opts = _client(certfile=None, keyfile="/certs/client.key")
        with self.assertRaises(SystemExit):
            client._validate_options(opts)

    def test_certfile_alone_reaches_socket_with_no_keyfile(self):
        client, opts = _client(certfile="/certs/combined.pem", keyfile=None)
        with (
            mock.patch.object(
                remote_module.TSSLSocket, "TSSLSocket", return_value=mock.Mock()
            ) as socket_cls,
            mock.patch.object(
                RemoteHostClient, "_get_client_by_transport", return_value=mock.Mock()
            ),
        ):
            client._get_client(opts)
        _, kwargs = socket_cls.call_args
        self.assertEqual(kwargs["certfile"], "/certs/combined.pem")
        self.assertIsNone(kwargs["keyfile"])

    def test_non_ssl_ignores_cert_options(self):
        client, opts = _client(ssl=False, certfile="/certs/client.pem", keyfile="/k")
        with (
            mock.patch.object(
                remote_module.TSocket, "TSocket", return_value=mock.Mock()
            ) as socket_cls,
            mock.patch.object(remote_module.TSSLSocket, "TSSLSocket") as ssl_socket_cls,
            mock.patch.object(
                RemoteHostClient, "_get_client_by_transport", return_value=mock.Mock()
            ),
        ):
            client._get_client(opts)
        socket_cls.assert_called_once()
        ssl_socket_cls.assert_not_called()

    def test_cmdline_parses_cert_options(self):
        args = Remote._parse_cmdline_options(
            [
                "pyremote",
                "--host",
                "localhost:1234",
                "--certfile",
                "/certs/client.pem",
                "--keyfile",
                "/certs/client.key",
                "shutdown",
            ]
        )
        self.assertEqual(args.certfile, "/certs/client.pem")
        self.assertEqual(args.keyfile, "/certs/client.key")
