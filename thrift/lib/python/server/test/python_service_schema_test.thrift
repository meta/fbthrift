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

include "thrift/lib/python/server/test/python_service_schema_test_base.thrift"
include "thrift/lib/python/server/test/python_service_schema_test_dep.thrift"
include "thrift/lib/python/server/test/python_service_schema_test_same_name.thrift"

package "thrift.com/python/server/test/python_service_schema_test"

namespace cpp2 apache.thrift.python.test
namespace py3 thrift.python.test

// Gives the test a definition in this program to reach the program's schema
// bundles through, via `TSchemaAssociation`.
struct Anchor {
  1: python_service_schema_test_dep.Point point;
  2: python_service_schema_test_same_name.Other other;
}

interaction Session {
  string echo(1: string text);
}

service BaseService {
  i32 ping(1: i32 seq);
}

service TestService extends BaseService {
  string describe(
    1: python_service_schema_test_dep.Point point,
    2: string label,
  );
  Session startSession();
}

service ExtendsRemoteBase extends python_service_schema_test_base.RemoteBaseService {
  void noop();
}
