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

# thrift-python's packages, such as thrift.python, live in another "thrift"
# directory on sys.path.
__path__ = __import__("pkgutil").extend_path(__path__, __name__)

# pyrefly: ignore [bad-dunder-all]
__all__ = ["Thrift", "TSCons"]
