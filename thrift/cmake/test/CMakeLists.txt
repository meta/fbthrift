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

# Tests thrift_add_library() and the files thrift_generate() lists.

thrift_add_library(thrift_cmake_test_base base.thrift LANGUAGE cpp2)
# Created before the library it depends on, which DEPENDS allows.
thrift_add_library(
  thrift_cmake_test_derived derived.thrift
  LANGUAGE cpp2
  SERVICES Derived
  DEPENDS thrift_cmake_test_base thrift_cmake_test_service)
thrift_add_library(
  thrift_cmake_test_service service.thrift
  LANGUAGE cpp2
  SERVICES Echo
  DEPENDS thrift_cmake_test_base)
# A hand-written source, as some projects add. Rebuilding the library must not
# regenerate the code of the libraries that depend on it.
target_sources(thrift_cmake_test_service PRIVATE ServiceExtra.cpp)

add_executable(thrift_cmake_test ThriftAddLibraryTest.cpp)
target_link_libraries(thrift_cmake_test PRIVATE thrift_cmake_test_derived
                                                GTest::gtest)
gtest_discover_tests(thrift_cmake_test)

# The same file with the options that change which files are generated. Each
# library only compiles, since they all define the same symbols.
set(variants
    plain
    "no_metadata"
    "frozen2"
    "types_cpp_splits=2"
    "client_cpp_splits={Variant:2}"
    "single_file_service"
    "json"
    "py3cpp")
set(libraries thrift_cmake_test_base thrift_cmake_test_service
              thrift_cmake_test_derived)
set(i 0)
foreach (options IN LISTS variants)
  set(name thrift_cmake_test_options_${i})
  if (options STREQUAL "plain")
    set(options)
  endif ()
  thrift_add_library(
    ${name} options.thrift
    LANGUAGE cpp2
    SERVICES Variant
    OPTIONS ${options}
    OUTPUT_DIRECTORY variant${i}/thrift/cmake/test
    INCLUDE_PREFIX thrift/cmake/test)
  list(APPEND libraries ${name})
  math(EXPR i "${i} + 1")
endforeach ()

# An empty INCLUDE_PREFIX, as for a file at the top of the include directory.
thrift_add_library(
  thrift_cmake_test_options_root options.thrift
  LANGUAGE cpp2
  SERVICES Variant
  OUTPUT_DIRECTORY root
  INCLUDE_PREFIX "")
list(APPEND libraries thrift_cmake_test_options_root)

# Include directories from a generator expression rather than DEPENDS.
set(base_includes thrift_cmake_test_base.thrift_includes)
thrift_add_library(
  thrift_cmake_test_service_genex service.thrift
  LANGUAGE cpp2
  SERVICES Echo
  INCLUDE_DIRECTORIES
    "$<TARGET_PROPERTY:${base_includes},INTERFACE_INCLUDE_DIRECTORIES>"
  OUTPUT_DIRECTORY genex/thrift/cmake/test
  INCLUDE_PREFIX thrift/cmake/test)
target_link_libraries(thrift_cmake_test_service_genex
                      PUBLIC thrift_cmake_test_base)
list(APPEND libraries thrift_cmake_test_service_genex)

# Compares each library's generated files with the ones the compiler records.
set(compiler "${THRIFT_COMPILER}")
if (TARGET ${THRIFT_COMPILER})
  set(compiler "$<TARGET_FILE:${THRIFT_COMPILER}>")
endif ()
foreach (library IN LISTS libraries)
  get_target_property(sources ${library} SOURCES)
  get_target_property(generator ${library} _THRIFT_GENERATOR)
  get_target_property(thrift_file ${library}.thrift_includes INTERFACE_SOURCES)
  string(REGEX REPLACE "^\\$<BUILD_INTERFACE:(.*)>$" "\\1" thrift_file
                       "${thrift_file}")
  set(expected)
  foreach (source IN LISTS sources)
    if (source MATCHES "/gen-(cpp2|py3cpp)/")
      get_filename_component(source "${source}" NAME)
      list(APPEND expected "${source}")
    endif ()
  endforeach ()
  add_test(
    NAME ${library}_generated_files
    COMMAND
      ${CMAKE_COMMAND} -DCOMPILER=${compiler} "-DGENERATOR=${generator}"
      -DINCLUDE_DIR=${PROJECT_SOURCE_DIR} -DTHRIFT_FILE=${thrift_file}
      -DOUTPUT_DIR=${CMAKE_CURRENT_BINARY_DIR}/genfiles/${library}
      "-DEXPECTED=${expected}" -P
      ${CMAKE_CURRENT_SOURCE_DIR}/check_generated_files.cmake)
endforeach ()

if (CMAKE_GENERATOR MATCHES "Ninja")
  # thrift_cmake_test_derived's generation depends on the .thrift files it
  # includes, directly and through service.thrift, and on no library.
  get_target_property(headers thrift_cmake_test_derived PUBLIC_HEADER)
  list(GET headers 0 header)
  file(RELATIVE_PATH header "${CMAKE_BINARY_DIR}" "${header}")
  add_test(
    NAME thrift_cmake_test_generation_depends
    COMMAND
      ${CMAKE_COMMAND} -DNINJA=${CMAKE_MAKE_PROGRAM}
      -DBUILD_DIR=${CMAKE_BINARY_DIR} -DOUTPUT=${header}
      "-DREQUIRED=base.thrift;service.thrift;derived.thrift" -P
      ${CMAKE_CURRENT_SOURCE_DIR}/check_generation_depends.cmake)
endif ()
