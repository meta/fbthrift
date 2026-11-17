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

include "thrift/annotation/cpp.thrift"
include "thrift/annotation/thrift.thrift"

package "facebook.com/thrift/test/fixtures/cpp_parameter_type"

namespace cpp2 test.fixtures.cpp_parameter_type

@cpp.GenerateServiceMethodDecorator
service ParameterTypes {
  void process(
    @cpp.Type{template = "std::deque"}
    1: list<string> names,
    @cpp.Type{name = "uint32_t"}
    2: i32 count,
  );
  @thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.stack_arguments": "1"}}
  void processOnStack(
    @cpp.Type{template = "std::deque"}
    1: list<string> names,
  );
}

@cpp.FastServer
@cpp.FastClient
service FastParameterTypes {
  void process(
    @cpp.Type{template = "std::deque"}
    1: list<string> names,
    @cpp.Type{name = "uint32_t"}
    2: i32 count,
  );
}
