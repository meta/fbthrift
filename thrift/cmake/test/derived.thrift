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

package "meta.com/thrift/cmake/test/derived"

namespace cpp2 thrift.cmake.test

include "thrift/cmake/test/base.thrift"
include "thrift/cmake/test/service.thrift"

struct Wrapper {
  1: base.Base base;
}

service Derived extends service.Echo {
  Wrapper wrap(1: base.Base base);
}
