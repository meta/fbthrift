/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// A base service in a program that defines no types. A generated
// `thrift_types` module imports only the included programs that have types, so
// this program's bundle is not reachable from a derived service's own module.

package "thrift.com/python/server/test/python_service_schema_test_base"

namespace cpp2 apache.thrift.python.test
namespace py3 thrift.python.test

service RemoteBaseService {
  i32 remotePing(1: i32 seq);
}
