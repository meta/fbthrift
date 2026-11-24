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

#pragma once
#include <thrift/conformance/if/gen-cpp2/test_suite_types.h>

namespace apache::thrift::conformance::data {

TestSuite createRPCServerTestSuite();
TestSuite createBasicRPCServerTestSuite();
TestSuite createRPCClientTestSuite();
// Client cases beyond createRPCClientTestSuite, for behaviors of a C++ client
// that not every language's client implements yet. A client opts in by listing
// gen-rpc-client-extended among its suite generators.
TestSuite createRPCClientExtendedTestSuite();
// Every request-response client case, common and extended, for a transport
// like HTTP/2.
TestSuite createRPCClientRequestResponseTestSuite();
TestSuite createRPCClientHTTP2TestSuite();
TestSuite createRPCClientChecksumTestSuite();

} // namespace apache::thrift::conformance::data
