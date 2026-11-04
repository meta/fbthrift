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

# The generated code the tests share, compiled once. An object library rather
# than a static one, so that every test links all of the generated objects.
add_library(thrift_protocol_test_fixtures OBJECT)
target_compile_options(thrift_protocol_test_fixtures
                       PRIVATE -Wno-shadow-compatible-local)
target_link_libraries(thrift_protocol_test_fixtures
                      PUBLIC thrift_annotation thrift_frozen thrift_metadata)
# cursor.thrift's adapters are in thrift/test/AdapterTest.h, which uses GTest.
target_link_libraries(thrift_protocol_test_fixtures PRIVATE GTest::gtest)
foreach (thrift_file Module CompactProtocolTestStructs cursor)
  # cursor.thrift's adapted types have no frozen layouts, and its Buck target
  # doesn't generate them either.
  if ("${thrift_file}" STREQUAL "cursor")
    set(thrift_options "")
  else ()
    set(thrift_options frozen2)
  endif ()
  thrift_generate(thrift_protocol_test_fixtures ${thrift_file}.thrift
                  LANGUAGE cpp2 OPTIONS ${thrift_options})
endforeach ()

# Test files to compile.
set(PROTOCOL_TESTS
    BinaryProtocolTest
    CompactProtocolTest
    CompactV1ProtocolTest
    ContainerSkippingTest
    DebugProtocolTest
    F14RoundTripTest
    InterfaceTest
    JSONProtocolTest
    ProtocolReaderStructReadStateTest
    ProtocolTest
    SimpleJSONProtocolTest
    TraitsTest)

# Build each test independently.
foreach (test_name ${PROTOCOL_TESTS})
  add_executable(thrift_${test_name} ${test_name}.cpp)
  target_compile_options(thrift_${test_name}
                         PUBLIC -Wno-shadow-compatible-local)
  target_link_libraries(thrift_${test_name} thrift_protocol_test_fixtures
                        GTest::gtest_main Folly::folly fmt::fmt)
  gtest_discover_tests(thrift_${test_name})
endforeach ()
