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

# Runs the Thrift compiler with --record-genfiles and checks that it generates
# exactly the files EXPECTED lists, other than the processmap files, which
# define nothing.
#
#   cmake -DCOMPILER=<thrift> -DGENERATOR=<gen> -DINCLUDE_DIR=<dir>
#     -DTHRIFT_FILE=<file> -DOUTPUT_DIR=<dir> -DEXPECTED=<names> -P <this file>

cmake_minimum_required(VERSION 3.24)

file(REMOVE_RECURSE "${OUTPUT_DIR}")
file(MAKE_DIRECTORY "${OUTPUT_DIR}")
execute_process(
  COMMAND "${COMPILER}" --record-genfiles "${OUTPUT_DIR}/genfiles.txt" --gen
          "${GENERATOR}" -I "${INCLUDE_DIR}" -o "${OUTPUT_DIR}" "${THRIFT_FILE}"
  RESULT_VARIABLE result)
if (NOT result EQUAL 0)
  message(FATAL_ERROR "The Thrift compiler failed: ${result}")
endif ()
file(STRINGS "${OUTPUT_DIR}/genfiles.txt" recorded)
set(actual)
foreach (file IN LISTS recorded)
  get_filename_component(name "${file}" NAME)
  if (NOT name MATCHES "_processmap_(binary|compact)\\.cpp$"
      AND name MATCHES "\\.(cpp|h|tcc)$")
    list(APPEND actual "${name}")
  endif ()
endforeach ()
set(listed)
foreach (name IN LISTS EXPECTED)
  list(APPEND listed "${name}")
endforeach ()
list(SORT actual)
list(SORT listed)
if (NOT actual STREQUAL listed)
  set(missing ${actual})
  list(REMOVE_ITEM missing ${listed})
  set(extra ${listed})
  list(REMOVE_ITEM extra ${actual})
  message(
    FATAL_ERROR
      "Generated but not listed: ${missing}\nListed but not generated: ${extra}"
  )
endif ()
list(LENGTH actual count)
message(STATUS "${count} generated files, as listed")
