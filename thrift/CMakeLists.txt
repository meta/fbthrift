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

include(ThriftGenerate)

# The library file names, which are also the names an older Thrift installed,
# of the targets whose names differ from them, as <target>:<name>.
set(thrift_output_names
    thrift_core:thrift-core
    thrift_concurrency:concurrency
    thrift_runtime:runtime
    thrift_transport:transport
    thrift_async:async
    thrift_protocol:thriftprotocol
    thrift_rpc:thriftcpp2
    thrift_metadata:thriftmetadata
    thrift_rpc_metadata:rpcmetadata
    thrift_server_debug_info:serverdbginfo
    thrift_type:thrifttype
    thrift_type_rep:thrifttyperep
    thrift_any_rep:thriftanyrep
    thrift_frozen:thriftfrozen2
    thrift_VarintUtilsBench:VarintUtilsBench
    thrift_ProtocolBench:ProtocolBench)

# Sets <out> to the target's thrift:: name: its name without `thrift_`, or
# `compiler` for the compiler.
function (thrift_name target out)
  if (target STREQUAL "thrift")
    set(${out} compiler PARENT_SCOPE)
  else ()
    string(REGEX REPLACE "^thrift_" "" name ${target})
    set(${out} ${name} PARENT_SCOPE)
  endif ()
endfunction ()

# Adds the alias thrift::<name> of a target, for projects that build Thrift from
# source.
function (thrift_add_alias target)
  thrift_name(${target} name)
  get_target_property(type ${target} TYPE)
  if (type STREQUAL "EXECUTABLE")
    add_executable(thrift::${name} ALIAS ${target})
  else ()
    add_library(thrift::${name} ALIAS ${target})
  endif ()
endfunction ()

# Adds the alias FBThrift::<legacy> of a target, the name an older Thrift
# installed, and records it for FBThriftConfig.cmake, which adds it for an
# installed Thrift.
function (thrift_add_legacy_name legacy target)
  get_target_property(type ${target} TYPE)
  if (type STREQUAL "EXECUTABLE")
    add_executable(FBThrift::${legacy} ALIAS ${target})
  else ()
    add_library(FBThrift::${legacy} ALIAS ${target})
  endif ()
  thrift_name(${target} name)
  set_property(GLOBAL APPEND PROPERTY THRIFT_LEGACY_NAMES "${legacy}:${name}")
endfunction ()

# Exports an installed target as thrift::<name>, keeping its legacy library file
# name, and adds its build-tree names.
function (thrift_export target)
  thrift_name(${target} name)
  set_target_properties(${target} PROPERTIES EXPORT_NAME ${name})
  set(legacy ${target})
  foreach (entry IN LISTS thrift_output_names)
    if (entry MATCHES "^${target}:(.+)$")
      set(legacy ${CMAKE_MATCH_1})
      set_target_properties(${target} PROPERTIES OUTPUT_NAME ${legacy})
    endif ()
  endforeach ()
  thrift_add_alias(${target})
  thrift_add_legacy_name(${legacy} ${target})
endfunction ()

# Installs the targets into fbthrift-exports and exports them.
function (thrift_install_targets)
  install(
    TARGETS ${ARGN}
    EXPORT fbthrift-exports
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})
  foreach (target IN LISTS ARGN)
    thrift_export(${target})
  endforeach ()
endfunction ()

add_subdirectory(common)
add_subdirectory(compiler)
add_subdirectory(annotation)
add_subdirectory(lib)

# Install CMake package configuration files for Thrift.
include(CMakePackageConfigHelpers)
configure_package_config_file(
  cmake/thrift-config.cmake.in thrift-config.cmake
  INSTALL_DESTINATION ${THRIFT_INSTALL_CMAKEDIR}
  PATH_VARS CMAKE_INSTALL_INCLUDEDIR)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/thrift-config.cmake
        DESTINATION ${THRIFT_INSTALL_CMAKEDIR})
# The package config for find_package(FBThrift), which finds the thrift package
# next to it and adds the names an older Thrift installed.
set(thrift_legacy_cmakedir "${CMAKE_INSTALL_LIBDIR}/cmake/fbthrift")
if (IS_ABSOLUTE "${CMAKE_INSTALL_LIBDIR}" OR IS_ABSOLUTE
                                             "${THRIFT_INSTALL_CMAKEDIR}")
  # An absolute directory already makes the installation not relocatable.
  get_filename_component(thrift_config_dir "${THRIFT_INSTALL_CMAKEDIR}"
                         ABSOLUTE BASE_DIR "${CMAKE_INSTALL_PREFIX}")
else ()
  file(RELATIVE_PATH thrift_config_dir "/${thrift_legacy_cmakedir}"
       "/${THRIFT_INSTALL_CMAKEDIR}")
  set(thrift_config_dir "\${CMAKE_CURRENT_LIST_DIR}/${thrift_config_dir}")
endif ()
get_property(thrift_legacy_names GLOBAL PROPERTY THRIFT_LEGACY_NAMES)
configure_file(cmake/FBThriftConfig.cmake.in FBThriftConfig.cmake @ONLY)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/FBThriftConfig.cmake
        DESTINATION ${thrift_legacy_cmakedir})
# xxhash, gflags, glog and zstd ship no CMake package config, so
# thrift-config.cmake resolves them in MODULE mode. Install their Find modules
# next to the config (which prepends its own directory to CMAKE_MODULE_PATH) so
# downstream find_package(thrift) can define the imported targets that
# thrift-targets.cmake refers to. FindLibUnwind is here because FindGlog uses
# it, and FindSodium because fizz's package config looks up Sodium without
# installing the module.
install(
  FILES ${PROJECT_SOURCE_DIR}/build/fbcode_builder/CMake/FindXxhash.cmake
        ${PROJECT_SOURCE_DIR}/build/fbcode_builder/CMake/FindZstd.cmake
        ${PROJECT_SOURCE_DIR}/build/fbcode_builder/CMake/FindGflags.cmake
        ${PROJECT_SOURCE_DIR}/build/fbcode_builder/CMake/FindGlog.cmake
        ${PROJECT_SOURCE_DIR}/build/fbcode_builder/CMake/FindLibUnwind.cmake
        ${PROJECT_SOURCE_DIR}/build/fbcode_builder/CMake/FindSodium.cmake
  DESTINATION ${THRIFT_INSTALL_CMAKEDIR})
install(
  EXPORT fbthrift-exports
  FILE thrift-targets.cmake
  NAMESPACE thrift::
  DESTINATION ${THRIFT_INSTALL_CMAKEDIR})

# For projects that build Thrift from source: the THRIFT_INCLUDE_DIR that
# fbcode_builder's add_fbthrift_*_library() read, and the FBTHRIFT_COMPILER and
# FBTHRIFT_INCLUDE_DIR that older copies of them read.
set(THRIFT_INCLUDE_DIR "${PROJECT_SOURCE_DIR}" CACHE INTERNAL
                                                     "Thrift include directory")
set(FBTHRIFT_COMPILER FBThrift::thrift CACHE INTERNAL "thrift compiler target")
set(FBTHRIFT_INCLUDE_DIR "${PROJECT_SOURCE_DIR}"
    CACHE INTERNAL "fbthrift include directory")

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/cmake/ThriftGenerate.cmake
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/thrift)

install(
  DIRECTORY annotation ${CMAKE_CURRENT_SOURCE_DIR}/annotation
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/thrift
  FILES_MATCHING
  PATTERN *.thrift)

install(
  DIRECTORY annotation ${CMAKE_CURRENT_BINARY_DIR}/annotation
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/thrift
  FILES_MATCHING
  PATTERN *.h
  PATTERN *.tcc
  PATTERN CMakeFiles EXCLUDE)
