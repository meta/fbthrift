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

// A parameter type defined outside the service's own program, so the test
// covers a schema that only resolves once the transitive bundles are merged.

package "thrift.com/python/server/test/python_service_schema_test_dep"

namespace cpp2 apache.thrift.python.test
namespace py3 thrift.python.test

struct Point {
  1: i32 x;
  2: i32 y;
}
