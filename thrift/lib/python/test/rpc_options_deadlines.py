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

import unittest

from thrift.python.common import RpcOptions


class RpcOptionsDeadlineTest(unittest.TestCase):
    def test_sr_deadlines_are_independent_and_can_restore_defaults(self) -> None:
        """SR deadline overrides must preserve generic Thrift timeout state."""
        options = RpcOptions()
        self.assertEqual(options.processing_timeout, 0)
        self.assertEqual(options.overall_timeout, 0)
        options.timeout = 5
        options.processing_timeout = 0.125
        options.overall_timeout = 0.75
        self.assertEqual(options.timeout, 5)
        self.assertEqual(options.processing_timeout, 0.125)
        self.assertEqual(options.overall_timeout, 0.75)
        options.processing_timeout = 0
        options.overall_timeout = 0
        self.assertEqual(options.timeout, 5)
        self.assertEqual(options.processing_timeout, 0)
        self.assertEqual(options.overall_timeout, 0)
