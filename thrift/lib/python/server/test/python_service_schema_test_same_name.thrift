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

// Another program defining a service named `TestService`, with a method named
// `describe` that takes other parameters. Both end up in the same merged
// schema, so a lookup by name alone could pick either.

package "thrift.com/python/server/test/python_service_schema_test_same_name"

namespace cpp2 apache.thrift.python.test.same_name
namespace py3 thrift.python.test

struct Other {
  1: i32 value;
}

service TestService {
  string describe(1: Other other);
}
