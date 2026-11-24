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

if (THRIFT_BENCHMARKS)
  add_executable(thrift_VarintUtilsBench VarintUtilsBench.cpp)
  target_link_libraries(thrift_VarintUtilsBench thrift_protocol Folly::folly
                        Folly::follybenchmark)

  thrift_install_targets(thrift_VarintUtilsBench)
endif ()

if (THRIFT_TESTS)
  set(PROTOCOL_TESTS VarintUtilsTest)

  # Build each test independently.
  foreach (test_name ${PROTOCOL_TESTS})
    add_executable(thrift_${test_name} ${test_name}.cpp)
    target_compile_options(thrift_${test_name}
                           PUBLIC -Wno-shadow-compatible-local)
    target_link_libraries(thrift_${test_name} GTest::gtest_main thrift_protocol
                          Folly::folly)
    gtest_discover_tests(thrift_${test_name})
  endforeach ()
endif ()
