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

# Checks with `ninja -t query` that the command generating OUTPUT depends on
# each file in REQUIRED and on no library. Order-only inputs, which order the
# command but don't rerun it, are ignored.
#
#   cmake -DNINJA=<ninja> -DBUILD_DIR=<dir> -DOUTPUT=<file> -DREQUIRED=<names>
#     -P <this file>

cmake_minimum_required(VERSION 3.24)

execute_process(COMMAND "${NINJA}" -C "${BUILD_DIR}" -t query "${OUTPUT}"
                OUTPUT_VARIABLE query RESULT_VARIABLE result)
if (NOT result EQUAL 0)
  message(FATAL_ERROR "ninja -t query failed: ${result}")
endif ()
# The inputs are the lines between "input:" and "outputs:".
string(REGEX REPLACE ".*input:[^\n]*\n" "" inputs "${query}")
string(REGEX REPLACE "\n *outputs:.*" "" inputs "${inputs}")
string(REPLACE "\n" ";" inputs "${inputs}")
set(names)
foreach (input IN LISTS inputs)
  string(STRIP "${input}" input)
  if (input MATCHES "^\\|\\|")
    continue()
  endif ()
  string(REGEX REPLACE "^\\| " "" input "${input}")
  if (input MATCHES "\\.(a|so|dylib|lib)(\\.[0-9.]+)?$")
    message(FATAL_ERROR "The generation of ${OUTPUT} depends on ${input}")
  endif ()
  get_filename_component(name "${input}" NAME)
  list(APPEND names "${name}")
endforeach ()
foreach (name IN LISTS REQUIRED)
  if (NOT name IN_LIST names)
    message(
      FATAL_ERROR
        "The generation of ${OUTPUT} doesn't depend on ${name}:\n${query}")
  endif ()
endforeach ()
message(STATUS "Inputs: ${names}")
