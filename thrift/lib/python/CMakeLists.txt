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

find_package(Python3 COMPONENTS Interpreter Development REQUIRED)

# Set FOLLY_PYTHON_SITE_PACKAGES for Cython .pxd files
# (folly.pxd, iobuf.pxd, etc.)
# These are separate from C++ headers and live in Python site-packages
set(thrift_stage_folly_python "")
if (TARGET folly_python_bindings)
  # folly is built in this build, so its Python package is a wheel in folly's
  # build tree. Unpack it where an install would put it, before anything below
  # reads it.
  set(FOLLY_PYTHON_SITE_PACKAGES "${CMAKE_CURRENT_BINARY_DIR}/folly-site-packages")
  get_target_property(_folly_wheel_dir folly_python_bindings
                      FOLLY_PYTHON_WHEEL_DIR)
  add_custom_target(
    thrift_stage_folly_python
    COMMAND ${CMAKE_COMMAND} -E rm -rf "${FOLLY_PYTHON_SITE_PACKAGES}"
    COMMAND
      ${Python3_EXECUTABLE} -c
      "import glob, sys, zipfile; zipfile.ZipFile(glob.glob(sys.argv[1] + '/folly-*.whl')[0]).extractall(sys.argv[2])"
      "${_folly_wheel_dir}" "${FOLLY_PYTHON_SITE_PACKAGES}"
    COMMENT "Unpacking folly's Python wheel for thrift-python"
    VERBATIM)
  add_dependencies(thrift_stage_folly_python folly_python_bindings)
  set(thrift_stage_folly_python thrift_stage_folly_python)
else ()
  # find_file locates a known file, then extract the parent directory.
  # HINTS cover both install locations:
  #   - lib/pythonX.Y/site-packages/ (setup.py install)
  #   - local/lib/pythonX.Y/dist-packages/ (pip --prefix on Debian/Ubuntu)
  set(_folly_py_version "python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}")
  unset(_folly_pxd_file CACHE)
  find_file(
    _folly_pxd_file
    NAMES folly/__init__.pxd
    HINTS
      "${FOLLY_PREFIX_DIR}/lib/${_folly_py_version}/site-packages"
      "${FOLLY_PREFIX_DIR}/lib64/${_folly_py_version}/site-packages"
      "${FOLLY_PREFIX_DIR}/local/lib/${_folly_py_version}/dist-packages"
      "${FOLLY_PREFIX_DIR}/local/lib64/${_folly_py_version}/dist-packages"
      "${FOLLY_PREFIX_DIR}/lib/x86_64-linux-gnu/${_folly_py_version}/site-packages"
      "${FOLLY_PREFIX_DIR}/lib64/x86_64-linux-gnu/${_folly_py_version}/site-packages"
      "${FOLLY_PREFIX_DIR}/local/lib/x86_64-linux-gnu/${_folly_py_version}/dist-packages"
      "${FOLLY_PREFIX_DIR}/local/lib64/x86_64-linux-gnu/${_folly_py_version}/dist-packages"
    NO_DEFAULT_PATH REQUIRED)

  # Extract site-packages directory (two levels up from folly/__init__.pxd)
  get_filename_component(_folly_pkg_dir "${_folly_pxd_file}" DIRECTORY)
  get_filename_component(FOLLY_PYTHON_SITE_PACKAGES "${_folly_pkg_dir}" DIRECTORY)
endif ()
message(STATUS "FOLLY_PYTHON_SITE_PACKAGES: ${FOLLY_PYTHON_SITE_PACKAGES}")

find_package(Cython 0.28 REQUIRED)

# thrift-python's server and streaming modules depend architecturally on py3's
# infrastructure. The py3 code provides:
#   - Streaming support (stream)
#   - Core server infrastructure (server)
# thrift-python builds on top of these py3 components rather than duplicating them.
# They cannot be separated into independent add_subdirectory() calls.

# Reference to parent lib directory for sibling directories (py3/) and setup.py
get_filename_component(_lib_dir "${CMAKE_CURRENT_SOURCE_DIR}/.." ABSOLUTE)
# Debug option for build troubleshooting
option(DEBUG_BUILD "Enable debug messages" OFF)

# =============================================================================
# Explicit Dependencies for Self-Contained Wheel
# =============================================================================
#
# Goal: Build a self-contained Python wheel that installs with just
# `pip install thrift-*.whl` - no additional dependencies required.
#
# Challenge: A standard shared library build would require wheel consumers to:
#   - Install 20+ shared libraries (folly, fizz, wangle, etc.) separately
#   - Manage LD_LIBRARY_PATH or install to system paths
#   - Handle version compatibility across all dependencies
#
# Approach: Create a consolidated library (libthrift_python_cpp.so) that embeds
# all C++ dependencies into one shared object. auditwheel bundles this single
# .so into the wheel.
#
# Why --whole-archive?
#   Without it, each Python extension (.so) would embed the full static library:
#     types.so (~600MB) + client.so (~600MB) + server.so (~600MB) + ...
#     = 27+ extensions × 600MB = 16-32GB of duplicated code
#
#   With --whole-archive, one shared library holds everything:
#     libthrift_python_cpp.so (~600MB) + tiny extensions (~50KB each)
#     = ~600MB total
#
# Trade-off: --whole-archive is a linker flag, not a CMake concept. CMake
# propagates transitive dependencies via INTERFACE_LINK_LIBRARIES on targets.
# But linker flags bypass this mechanism. When CMake sees:
#
#   target_link_libraries(thrift_python_cpp "-Wl,--whole-archive" Folly::folly ...)
#
# it passes the flag to the linker, but the linker does not understand CMake's
# target properties. Transitive deps like libiberty (from Folly::folly) do not
# propagate. Without --whole-archive, CMake would automatically add libiberty
# to the link line. With --whole-archive, the find_package calls below must add
# them explicitly.
# =============================================================================

# Libevent: Force static link to embed symbols
if (TARGET event_core_static)
  # folly fetched libevent, whose own build has no libevent:: targets.
  set(thrift_python_libevent event_core_static event_extra_static)
else ()
  # Like folly, also find a libevent that installed no CMake package config,
  # such as Homebrew's.
  set(LIBEVENT_STATIC_LINK TRUE)
  find_package(LibEvent MODULE REQUIRED)
  set(thrift_python_libevent ${LIBEVENT_LIB})
endif ()

# Libiberty: Provides cplus_demangle_v3_callback for folly stack traces, when
# folly found it too
find_package(Libiberty)

# gflags warning:
# This section adds explicit dependencies that --whole-archive bypasses.
# A natural assumption: if libevent and libiberty are here, gflags should be too.
# gflags is different. thrift_concurrency and thrift_frozen already link it,
# and so do folly and glog, which setup.py links as shared libraries. All of
# them must use the same shared gflags: a static copy in thrift_python_cpp.so
# would register gflags' flags a second time, and gflags exits. Do not add
# gflags.

function (log_debug msg)
  if (DEBUG_BUILD)
    message(STATUS "[DEBUG] ${msg}")
  endif ()
endfunction ()

# Use parent binary dir so _cybld stays at <build>/thrift/lib/cybld
# (consistent with include paths like thrift/lib/python/server/*.h)
#
# Why symlinks?
# Buck has a "base_module" concept that changes import paths. Buck handles this
# automatically. This build achieves the same explicitly via symlinks from source
# files to the build directory with the desired import path structure.
# Example: thrift/lib/python with base_module="thrift.python" becomes import
# thrift.python. Symlinks map thrift/lib/python -> thrift/python under _cybld.
get_filename_component(_cybld "${CMAKE_CURRENT_BINARY_DIR}/../cybld" ABSOLUTE)

# Create build directory structure
set(_cybld_dirs
    "thrift/python"
    "thrift/python/client"
    "thrift/python/client/py_bridge"
    "thrift/python/server"
    "thrift/python/server/flagged"
    "thrift/python/server/interceptor"
    "thrift/python/any"
    "thrift/python/conformance"
    "thrift/python/streaming"
    "thrift/py3")
foreach (_dir ${_cybld_dirs})
  file(MAKE_DIRECTORY "${_cybld}/${_dir}")
endforeach ()

# So that cython includes work correctly
file(TOUCH "${_cybld}/thrift/__init__.pxd")
file(TOUCH "${_cybld}/thrift/python/__init__.pxd")
file(TOUCH "${_cybld}/thrift/py3/__init__.pxd")

# Helper function: symlink all source files from a directory to build dir
# Non-recursive - only files directly in the directory
# (subdirs handled separately)
function (symlink_dir_contents src_dir dest_dir)
  file(MAKE_DIRECTORY "${dest_dir}")
  file(GLOB _all_items "${src_dir}/*")

  foreach (_src ${_all_items})
    if (IS_DIRECTORY "${_src}")
      continue()
    endif ()

    get_filename_component(_filename "${_src}" NAME)

    # Skip http2_helper files — they depend on proxygen which is no longer a dependency
    if ("${_filename}" MATCHES "^http2_helper\\.")
      continue()
    endif ()

    set(_dest "${dest_dir}/${_filename}")

    message(STATUS "Symlinking ${_src} -> ${_dest}")
    file(CREATE_LINK "${_src}" "${_dest}" SYMBOLIC)
  endforeach ()
endfunction ()

# Helper function: recursively symlink all files from a source tree
# to build directory. Calls symlink_dir_contents for each directory.
function (symlink_dir_recursive src_dir dest_base_dir)
  # Handle the root directory
  symlink_dir_contents("${src_dir}" "${dest_base_dir}")

  # Find all subdirectories and call symlink_dir_contents for each
  file(GLOB_RECURSE _all_subdirs LIST_DIRECTORIES true "${src_dir}/*")

  foreach (_src_dir ${_all_subdirs})
    if (IS_DIRECTORY "${_src_dir}")
      file(RELATIVE_PATH _rel "${src_dir}" "${_src_dir}")
      symlink_dir_contents("${src_dir}/${_rel}" "${dest_base_dir}/${_rel}")
    endif ()
  endforeach ()
endfunction ()

# Symlink all python/ and py3/ source files recursively
symlink_dir_recursive("${CMAKE_CURRENT_SOURCE_DIR}" "${_cybld}/thrift/python")
symlink_dir_recursive("${_lib_dir}/py3" "${_cybld}/thrift/py3")

# Placeholder targets for build dependency tracking
# Symlinks are created at configure time by symlink_dir_recursive above,
# but other parts of the build depend on these targets for ordering.
add_custom_target(create_binding_symlink_python ALL)
add_custom_target(create_binding_symlink_py3 ALL)

#####################################################################
# Include Path Setup (must be done early, before API header generation)
#####################################################################
# fmt, folly and Boost can each be fetched rather than found. A fetched one
# sets no FindBoost-style *_INCLUDE_DIRS variable, and its target's include
# dirs are generator expressions, so read all three through $<TARGET_PROPERTY>
# to get them evaluated rather than pasted in. wangle, glog and mvfst always
# come from find_package, so get_target_property is enough for them.
set(fmt_include_dirs
    "$<JOIN:$<TARGET_PROPERTY:fmt::fmt,INTERFACE_INCLUDE_DIRECTORIES>,:>")
set(folly_incs
    "$<JOIN:$<TARGET_PROPERTY:Folly::folly,INTERFACE_INCLUDE_DIRECTORIES>,:>")
set(boost_incs
    "$<JOIN:$<TARGET_PROPERTY:Boost::headers,INTERFACE_INCLUDE_DIRECTORIES>,:>")
# Unlike get_target_property(), these include the directories of the targets
# that each one links, such as the one with the <wangle/wangle-config.h> that a
# wangle built in this build generates.
set(wangle_incs
    "$<JOIN:$<TARGET_PROPERTY:wangle::wangle,INTERFACE_INCLUDE_DIRECTORIES>,:>")
set(glog_include_dirs
    "$<JOIN:$<TARGET_PROPERTY:glog::glog,INTERFACE_INCLUDE_DIRECTORIES>,:>")

get_filename_component(python_libname ${Python3_LIBRARIES} NAME)

# Create colon-separated include path for CYTHON_INCLUDE_PATH env var
# mvfst include directories
# Derive from cmake targets (found via find_package above) - not hardcoded paths
set(mvfst_incs
    "$<JOIN:$<TARGET_PROPERTY:mvfst::mvfst_transport,INTERFACE_INCLUDE_DIRECTORIES>,:>"
)

set(_all_cython_includes
    # Folly's headers come first: the Python packages staged in the build
    # directories below have a folly/executor.h, which is <folly/Executor.h>
    # on a case-insensitive file system.
    ${folly_incs}
    # Thrift includes. setup.py is outside CMake's target graph, so the source
    # and build roots have to be listed explicitly.
    ${PROJECT_SOURCE_DIR}
    ${PROJECT_BINARY_DIR}
    ${CMAKE_BINARY_DIR}/thrift/lib
    ${CMAKE_BINARY_DIR}/thrift/lib/cybld
    # Folly's .pxd files
    ${FOLLY_PYTHON_SITE_PACKAGES}
    # Boost
    ${boost_incs}
    # Other dependencies
    ${fmt_include_dirs}
    ${wangle_incs}
    ${glog_include_dirs}
    ${mvfst_incs}
    # Reached transitively through folly and glog headers. setup.py sees only
    # this list: neither the directory-wide include_directories() in the
    # top-level CMakeLists nor an imported target's usage requirements (how the
    # CMake targets pick these up) reach it.
    ${LIBGFLAGS_INCLUDE_DIR}
    ${OPENSSL_INCLUDE_DIR}
    ${ZSTD_INCLUDE_DIR}
    ${Xxhash_INCLUDE_DIR})
list(JOIN _all_cython_includes ":" cython_include_path)
log_debug("cython_include_path='${cython_include_path}'")

#####################################################################
# Library Path Setup
#####################################################################
# Library directories for setup.py's link step and for auditwheel: the
# prefixes of this build's dependencies, which getdeps passes through the
# CMAKE_PREFIX_PATH environment variable. getdeps' install root also holds
# other projects' builds of the same libraries, such as a static glog beside
# this build's shared one, and the linker takes the first directory that has
# the library.
set(dep_prefixes ${CMAKE_PREFIX_PATH})
file(TO_CMAKE_PATH "$ENV{CMAKE_PREFIX_PATH}" env_prefixes)
list(APPEND dep_prefixes ${env_prefixes})

# All thrift libraries share this directory. thrift_python_cpp is defined below;
# generator expressions resolve at generate time, so the forward reference is
# fine.
set(all_lib_dirs "$<TARGET_FILE_DIR:thrift_python_cpp>")
# Where the shared libraries that setup.py links by name are, which a build
# that fetched folly has in its own tree rather than under a prefix.
list(APPEND all_lib_dirs "$<TARGET_FILE_DIR:Folly::folly>"
     "$<TARGET_FILE_DIR:Folly::folly_python_cpp>"
     "$<TARGET_FILE_DIR:fmt::fmt>" "$<TARGET_FILE_DIR:glog::glog>")
foreach (pkg ${dep_prefixes})
  if (EXISTS "${pkg}/lib")
    list(APPEND all_lib_dirs "${pkg}/lib")
  endif ()
  if (EXISTS "${pkg}/lib64")
    list(APPEND all_lib_dirs "${pkg}/lib64")
  endif ()
endforeach ()
list(APPEND all_lib_dirs "/usr/lib64")
get_filename_component(python_lib ${Python3_LIBRARIES} DIRECTORY)
list(APPEND all_lib_dirs "${python_lib}")

# Join all library directories with colons for -L flags and LIBRARY_DIRS
list(JOIN all_lib_dirs ":" library_dirs_str)
set(libs "-L${library_dirs_str}")

#####################################################################
# Install Directory
#####################################################################
set(py_install_dir ${CMAKE_INSTALL_PREFIX})
if (NOT ${PYTHON_PACKAGE_INSTALL_DIR} STREQUAL "")
  set(py_install_dir ${PYTHON_PACKAGE_INSTALL_DIR})
endif ()

#####################################################################
# Linker Flags
#####################################################################
# Set default LDFLAGS for setup.py to fix undefined symbol issues
# --no-as-needed ensures libfolly.so is in NEEDED list even if linker
# thinks it's not needed. macOS's linker records every library it is given.
if (APPLE)
  set(DEFAULT_SETUP_LDFLAGS "")
else ()
  set(DEFAULT_SETUP_LDFLAGS "-Wl,--no-as-needed")
endif ()

# Allow users to override via environment variable (e.g., for custom RPATH)
if (DEFINED ENV{LDFLAGS})
  set(SETUP_LDFLAGS "$ENV{LDFLAGS}")
  message(STATUS "Using user-provided LDFLAGS: ${SETUP_LDFLAGS}")
else ()
  set(SETUP_LDFLAGS "${DEFAULT_SETUP_LDFLAGS}")
  message(STATUS "Using default LDFLAGS: ${SETUP_LDFLAGS}")
endif ()

###
# First, run Thrift compiler on metadata.thrift. thrift-python runtime
# depends on apache.thrift.metadata package, which is created and
# installed later in setup.py.
###
add_custom_target(
  generate_apache_thrift_metadata_python
  COMMAND
    ${THRIFT_COMPILER} -I ${CMAKE_SOURCE_DIR} --out . --gen
    "mstch_python:include_prefix=thrift" ${_lib_dir}/thrift/metadata.thrift
  COMMAND ${CMAKE_COMMAND} -E make_directory ${_cybld}/apache/thrift/metadata
  WORKING_DIRECTORY ${_cybld}
  BYPRODUCTS ${_cybld}/apache/thrift/metadata/thrift_abstract_types.py
             ${_cybld}/apache/thrift/metadata/thrift_clients.py
             ${_cybld}/apache/thrift/metadata/thrift_enums.py
             ${_cybld}/apache/thrift/metadata/thrift_metadata.py
             ${_cybld}/apache/thrift/metadata/thrift_mutable_clients.py
             ${_cybld}/apache/thrift/metadata/thrift_mutable_services.py
             ${_cybld}/apache/thrift/metadata/thrift_mutable_types.py
             ${_cybld}/apache/thrift/metadata/thrift_mutable_types.pyi
             ${_cybld}/apache/thrift/metadata/thrift_services.py
             ${_cybld}/apache/thrift/metadata/thrift_types.py
             ${_cybld}/apache/thrift/metadata/thrift_types.pyi)

###
# Also generate py3 version for apache.thrift.metadata
# (required by py3/metadata.pyx)
###
add_custom_target(
  generate_apache_thrift_metadata_py3
  COMMAND ${THRIFT_COMPILER} -I ${CMAKE_SOURCE_DIR} --out . --gen
          "mstch_py3:include_prefix=thrift" ${_lib_dir}/thrift/metadata.thrift
  WORKING_DIRECTORY ${_cybld}
  BYPRODUCTS ${_cybld}/apache/thrift/metadata/cbindings.pxd
             ${_cybld}/apache/thrift/metadata/cbindings.pyx
             ${_cybld}/apache/thrift/metadata/metadata.pxd
             ${_cybld}/apache/thrift/metadata/metadata.pyx)

add_dependencies(generate_apache_thrift_metadata_python ${THRIFT_COMPILER})
add_dependencies(generate_apache_thrift_metadata_py3 ${THRIFT_COMPILER})

####
# Next, prepare for building cython extensions by creating the necessary
# symlinks in the cybld directory
####
add_custom_target(create_binding_symlink_python_types ALL)
# Python types files (local to this directory)
foreach (_file "types.pxd" "types.pyx")
  set(_src "${CMAKE_CURRENT_SOURCE_DIR}/${_file}")
  get_filename_component(_target_file "${_src}" NAME)
  message(STATUS "Linking ${_src} to ${_cybld}/thrift/python/_${_target_file}")
  add_custom_command(
    TARGET create_binding_symlink_python_types PRE_BUILD
    COMMAND ${CMAKE_COMMAND} -E create_symlink "${_src}"
            "${_cybld}/thrift/python/_${_target_file}")
endforeach ()
# py3 stream files (in sibling py3 directory)
foreach (_file "stream.pxd" "stream.pyx")
  set(_src "${_lib_dir}/py3/${_file}")
  get_filename_component(_target_file "${_src}" NAME)
  message(STATUS "Linking ${_src} to ${_cybld}/thrift/py3/_${_target_file}")
  add_custom_command(
    TARGET create_binding_symlink_python_types PRE_BUILD
    COMMAND ${CMAKE_COMMAND} -E create_symlink "${_src}"
            "${_cybld}/thrift/py3/_${_target_file}")
endforeach ()
# Below symlinks simplify cythonize logic in setup.py
add_custom_command(
  TARGET create_binding_symlink_python_types
  PRE_BUILD
  COMMAND
    ${CMAKE_COMMAND} -E create_symlink
    "${_cybld}/thrift/python/server/server.pxd"
    "${_cybld}/thrift/python/server.pxd")
add_custom_command(
  TARGET create_binding_symlink_python_types
  PRE_BUILD
  COMMAND
    ${CMAKE_COMMAND} -E create_symlink
    "${_cybld}/thrift/python/server/server.pyx"
    "${_cybld}/thrift/python/server.pyx")

#####
# Compile a few *_api.h headers first.
#####
# List all headers generated by setup.py --api-only
set(CYTHON_GENERATED_API_HEADERS
    ${_cybld}/thrift/python/_types_api.h
    ${_cybld}/thrift/python/server/python_async_processor_api.h
    ${_cybld}/thrift/python/server/request_context_api.h
    ${_cybld}/thrift/python/server/interceptor/service_interceptor_api.h
    ${_cybld}/thrift/python/server_impl/python_async_processor_api.h
    ${_cybld}/thrift/python/server_impl/request_context_api.h
    ${_cybld}/thrift/python/server_impl/interceptor/service_interceptor_api.h
    ${_cybld}/thrift/py3/_stream_api.h
    ${_cybld}/thrift/python/streaming/sink_api.h
    ${_cybld}/thrift/python/streaming/bidistream_api.h
    ${_cybld}/thrift/python/streaming/py_promise_api.h
    ${_cybld}/thrift/python/client/py_bridge/py_bridge_channel_api.h)

log_debug(
  "At API header generation: cython_include_path='${cython_include_path}'")

if (CMAKE_C_COMPILER)
  set(_setup_py_c_compiler "${CMAKE_C_COMPILER}")
  set(_setup_py_c_launcher "${CMAKE_C_COMPILER_LAUNCHER}")
else ()
  set(_setup_py_c_compiler "${CMAKE_CXX_COMPILER}")
  set(_setup_py_c_launcher "${CMAKE_CXX_COMPILER_LAUNCHER}")
endif ()
if (_setup_py_c_launcher)
  set(_setup_py_cc "${_setup_py_c_launcher} ${_setup_py_c_compiler}")
else ()
  set(_setup_py_cc "${_setup_py_c_compiler}")
endif ()
if (CMAKE_CXX_COMPILER_LAUNCHER)
  set(_setup_py_cxx "${CMAKE_CXX_COMPILER_LAUNCHER} ${CMAKE_CXX_COMPILER}")
else ()
  set(_setup_py_cxx "${CMAKE_CXX_COMPILER}")
endif ()
add_custom_command(
  OUTPUT ${CYTHON_GENERATED_API_HEADERS}
  COMMAND
    ${CMAKE_COMMAND} -E env "CFLAGS=${CMAKE_C_FLAGS}"
    "CXXFLAGS=${CMAKE_CXX_FLAGS}" "CC=${_setup_py_cc}" "CXX=${_setup_py_cxx}"
    "CYTHON_INCLUDE_PATH=${cython_include_path}" ${Python3_EXECUTABLE}
    ${_lib_dir}/setup.py -v --api-only
  DEPENDS create_binding_symlink_python_types create_binding_symlink_python
          create_binding_symlink_py3 create_binding_symlink_server_impl
          thrift_rpc
  WORKING_DIRECTORY ${_cybld}
  COMMENT "Generating Cython API headers")

add_custom_target(thrift_python_types_bindings ALL
                  DEPENDS ${CYTHON_GENERATED_API_HEADERS})

#####################################################################
# API Header Symlinks
#####################################################################
# WHY: C++ code includes headers as "thrift/lib/python/types_api.h"
#      but setup.py generates them at "thrift/python/_types_api.h"
#      Symlinks bridge this path difference.
#
# NOTE: server_impl in source paths (not server) because:
#       - C++ code expects
#         import_thrift__python__server_impl__python_async_processor
#       - Buck uses package = "thrift.python.server_impl"
#       - Must match the import function names in generated API headers

# Define all API header symlink mappings (source|destination)
set(CYTHON_API_SYMLINK_MAPPINGS
    "${_cybld}/thrift/python/_types_api.h|\
${_cybld}/thrift/lib/python/types_api.h"
    "${_cybld}/thrift/python/_types.h|${_cybld}/thrift/lib/python/_types.h"
    "${_cybld}/thrift/python/server_impl/python_async_processor_api.h|\
${_cybld}/thrift/lib/python/server/python_async_processor_api.h"
    "${_cybld}/thrift/python/server_impl/request_context_api.h|\
${_cybld}/thrift/lib/python/server/request_context_api.h"
    "${_cybld}/thrift/python/server_impl/interceptor/service_interceptor_api.h|\
${_cybld}/thrift/lib/python/server/interceptor/service_interceptor_api.h"
    "${_cybld}/thrift/py3/_stream_api.h|${_cybld}/thrift/lib/py3/stream_api.h"
    "${_cybld}/thrift/py3/_stream.h|${_cybld}/thrift/lib/py3/_stream.h"
    "${_cybld}/thrift/python/streaming/sink_api.h|\
${_cybld}/thrift/lib/python/streaming/sink_api.h"
    "${_cybld}/thrift/python/streaming/bidistream_api.h|\
${_cybld}/thrift/lib/python/streaming/bidistream_api.h"
    "${_cybld}/thrift/python/streaming/py_promise_api.h|\
${_cybld}/thrift/lib/python/streaming/py_promise_api.h"
    "${_cybld}/thrift/python/client/py_bridge/py_bridge_channel_api.h|\
${_cybld}/thrift/lib/python/client/py_bridge/py_bridge_channel_api.h")

# Generate symlink commands from mappings
set(CYTHON_API_SYMLINKS "")
set(SYMLINK_COMMANDS "")
foreach (mapping IN LISTS CYTHON_API_SYMLINK_MAPPINGS)
  string(REPLACE "|" ";" pair ${mapping})
  list(GET pair 0 source)
  list(GET pair 1 dest)
  list(APPEND CYTHON_API_SYMLINKS ${dest})
  list(
    APPEND
    SYMLINK_COMMANDS
    COMMAND
    ${CMAKE_COMMAND}
    -E
    create_symlink
    ${source}
    ${dest})
endforeach ()

file(MAKE_DIRECTORY "${_cybld}/thrift/lib/python/")
file(MAKE_DIRECTORY "${_cybld}/thrift/lib/python/server")
file(MAKE_DIRECTORY "${_cybld}/thrift/lib/python/server/interceptor")
file(MAKE_DIRECTORY "${_cybld}/thrift/lib/python/streaming")
file(MAKE_DIRECTORY "${_cybld}/thrift/lib/python/client/py_bridge")
file(MAKE_DIRECTORY "${_cybld}/thrift/lib/py3/")

# Create all symlinks
# (OUTPUT ensures files exist before dependent targets build)
add_custom_command(
  OUTPUT ${CYTHON_API_SYMLINKS} ${SYMLINK_COMMANDS}
  DEPENDS ${CYTHON_GENERATED_API_HEADERS}
  COMMENT "Creating symlinks to Cython API headers")

# Target for dependency tracking
add_custom_target(create_cython_types_api_symlink ALL
                  DEPENDS ${CYTHON_API_SYMLINKS})
# Explicit target-level dependency: symlinks wait for API headers
add_dependencies(create_cython_types_api_symlink thrift_python_types_bindings)

# SHARED to reduce build time, disk space, and runtime memory
add_library(
  thrift_python_cpp SHARED
  client/OmniClient.cpp
  client/RequestChannel.cpp
  client/ssl.cpp
  Serializer.cpp
  server/event_handler.cpp
  server/flagged/RcAwareTaskPatch.cpp
  server/request_context_holder.cpp
  server/interceptor/PythonServiceInterceptor.cpp
  server/PythonAsyncProcessor.cpp
  server/execution/ExecutionSystem.cpp
  server/execution/WorkScheduler.cpp
  server/PythonAsyncProcessorFactory.cpp
  server/RequestContextExtractor.cpp
  std_libcpp.cpp
  streaming/bidi_stream.cpp
  streaming/PythonUserException.cpp
  streaming/Sink.cpp
  streaming/StreamElementEncoder.cpp
  types.cpp
  ${_lib_dir}/py3/stream.cpp)
add_dependencies(thrift_python_cpp create_cython_types_api_symlink)
set_property(TARGET thrift_python_cpp PROPERTY VERSION ${PROJECT_VERSION})
target_compile_definitions(thrift_python_cpp PRIVATE BOOST_NO_AUTO_PTR)
target_compile_definitions(thrift_python_cpp
                           PRIVATE THRIFT_NO_HTTP_CLIENT_CHANNEL)
# Workaround for https://github.com/cython/cython/issues/5973
target_compile_definitions(thrift_python_cpp PRIVATE __PYX_ENUM_CLASS_DECL=)
# As for setup.py, folly's headers come before the staged folly/executor.h.
# An installed folly's include directories would otherwise be system ones,
# which compilers search after "${_cybld}" whatever their order here.
set_property(TARGET thrift_python_cpp PROPERTY NO_SYSTEM_FROM_IMPORTED ON)
target_include_directories(
  thrift_python_cpp BEFORE
  PRIVATE "$<TARGET_PROPERTY:Folly::folly,INTERFACE_INCLUDE_DIRECTORIES>")
target_include_directories(thrift_python_cpp PRIVATE "${_cybld}"
                                                     ${Python3_INCLUDE_DIRS})
# Link strategy:
# - STATIC libs (thrift + deps): embed with --whole-archive to consolidate
#   into single .so
# - SHARED libs (folly, fmt, glog): link normally, become runtime deps
# --allow-multiple-definition handles duplicate symbols across static libs
# (e.g., librpcmetadata.a and libthriftprotocol.a both compile
# TJSONProtocol.cpp)
# libevent/libiberty: needed for undefined symbols,
# --allow-multiple-definition handles duplicates
set(thrift_python_static_libs
    thrift_rpc
    thrift_rpc_metadata
    thrift_annotation
    thrift_metadata
    thrift_frozen
    thrift_protocol
    thrift_type_rep
    thrift_type
    thrift_any_rep
    thrift_server_debug_info)
if (APPLE)
  # ld64 has neither --whole-archive nor --allow-multiple-definition, but
  # loads each archive given to -force_load whole. thrift_metadata and
  # thrift_protocol compile some of thrift_rpc's sources too, so they are left
  # to supply only the objects that something references.
  set(whole_archive_begin "")
  set(whole_archive_end "")
  set(force_loaded_libs ${thrift_python_static_libs})
  list(REMOVE_ITEM force_loaded_libs thrift_metadata thrift_protocol)
  foreach (lib IN LISTS force_loaded_libs)
    target_link_options(thrift_python_cpp PRIVATE
                        "LINKER:-force_load,$<TARGET_FILE:${lib}>")
  endforeach ()
else ()
  set(whole_archive_begin "-Wl,--whole-archive"
                          "-Wl,--allow-multiple-definition")
  set(whole_archive_end "-Wl,--no-whole-archive")
endif ()
target_link_libraries(
  thrift_python_cpp
  PUBLIC # Static libs - embed all symbols
         ${whole_archive_begin}
         ${thrift_python_static_libs}
         ${thrift_python_libevent}
         ${LIBIBERTY_LIBRARIES}
         ${whole_archive_end}
         # Shared libs - runtime deps (see manifests/folly, manifests/fmt)
         Folly::folly
         Folly::folly_python_cpp
         fmt::fmt
         glog::glog)
# The interpreter that loads the extensions provides Python's symbols, which
# macOS's linker otherwise requires at link time.
target_link_libraries(thrift_python_cpp PRIVATE Python3::Module)
install(TARGETS thrift_python_cpp EXPORT thrift)
install(TARGETS thrift_python_cpp DESTINATION ${py_install_dir})

#####
# Create server_impl alias for OSS builds
# Buck internal builds use package = "thrift.python.server_impl" remapping.
# OSS CMake builds need this alias created manually.
#####
add_custom_target(create_binding_symlink_server_impl ALL)

file(MAKE_DIRECTORY "${_cybld}/thrift/python/server_impl")
file(MAKE_DIRECTORY "${_cybld}/thrift/python/server_impl/execution")
file(MAKE_DIRECTORY "${_cybld}/thrift/python/server_impl/interceptor")

# For cimport during Cython compilation, and the pure Python modules that
# server.pyx imports from thrift.python.server_impl
file(GLOB ServerImplFiles "${CMAKE_CURRENT_SOURCE_DIR}/server/*.pxd"
     "${CMAKE_CURRENT_SOURCE_DIR}/server/*.pyx"
     "${CMAKE_CURRENT_SOURCE_DIR}/server/*.py")

foreach (_src ${ServerImplFiles})
  get_filename_component(_target_file "${_src}" NAME)
  message(STATUS "Creating server_impl symlink: \
${_cybld}/thrift/python/server_impl/${_target_file} -> ${_src}")
  add_custom_command(
    TARGET create_binding_symlink_server_impl PRE_BUILD
    COMMAND ${CMAKE_COMMAND} -E create_symlink "${_src}"
            "${_cybld}/thrift/python/server_impl/${_target_file}")
endforeach ()

set(ServerImplExecutionFiles
    "${CMAKE_CURRENT_SOURCE_DIR}/server/execution/asyncio_worker.py"
    "${CMAKE_CURRENT_SOURCE_DIR}/server/execution/asyncio_worker_pool.py")

foreach (_src ${ServerImplExecutionFiles})
  get_filename_component(_target_file "${_src}" NAME)
  message(STATUS "Creating server_impl/execution symlink: \
${_cybld}/thrift/python/server_impl/execution/${_target_file} -> ${_src}")
  add_custom_command(
    TARGET create_binding_symlink_server_impl PRE_BUILD
    COMMAND ${CMAKE_COMMAND} -E create_symlink "${_src}"
            "${_cybld}/thrift/python/server_impl/execution/${_target_file}")
endforeach ()

# Create symlinks for server_impl/interceptor/*.pxd
file(GLOB ServerImplInterceptorFiles
     "${CMAKE_CURRENT_SOURCE_DIR}/server/interceptor/*.pxd"
     "${CMAKE_CURRENT_SOURCE_DIR}/server/interceptor/*.pyx")

foreach (_src ${ServerImplInterceptorFiles})
  get_filename_component(_target_file "${_src}" NAME)
  message(STATUS "Creating server_impl/interceptor symlink: \
${_cybld}/thrift/python/server_impl/interceptor/${_target_file} -> ${_src}")
  add_custom_command(
    TARGET create_binding_symlink_server_impl PRE_BUILD
    COMMAND ${CMAKE_COMMAND} -E create_symlink "${_src}"
            "${_cybld}/thrift/python/server_impl/interceptor/${_target_file}")
endforeach ()

#####
# Tests follow THRIFT_TESTS. setup.py builds the test extensions, which compile
# from the test/ symlinks, only when THRIFT_BUILD_TESTS is 1.
#####
set(_python_test_bindings)
if (THRIFT_TESTS)
  set(_setup_py_build_tests 1)
  add_subdirectory(test)
  set(_python_test_bindings create_binding_symlink_test
                            create_binding_symlink_lib_test)
else ()
  set(_setup_py_build_tests 0)
endif ()

#####
# Now build everything else in lib/python, including all remaining Cython
# modules that depend on types_api.h and thrift_python_cpp.
#####
add_custom_target(
  thrift_python_and_py3_bindings ALL
  DEPENDS generate_apache_thrift_metadata_python
          generate_apache_thrift_metadata_py3
          create_binding_symlink_python
          create_binding_symlink_py3
          create_binding_symlink_server_impl
          ${_python_test_bindings}
          thrift_rpc
          thrift_python_cpp
          thrift_python_types_bindings
  WORKING_DIRECTORY ${_cybld})
if (thrift_stage_folly_python)
  add_dependencies(thrift_python_types_bindings thrift_stage_folly_python)
  add_dependencies(thrift_python_and_py3_bindings thrift_stage_folly_python)
endif ()

# Copy folly Python bindings into the build directory before wheel creation.
# Folly doesn't build a wheel yet, so we bundle folly.iobuf, folly.executor,
# etc. into the thrift wheel so users don't need to install folly separately.
add_custom_command(
  TARGET thrift_python_and_py3_bindings
  COMMAND ${CMAKE_COMMAND} -E copy_directory
          "${FOLLY_PYTHON_SITE_PACKAGES}/folly" "${_cybld}/folly"
  COMMENT "Copying folly Python bindings into wheel build directory")
set(_setup_py_parallel_jobs "")
if (DEFINED CMAKE_JOB_POOLS)
  string(REGEX MATCH "compile=([0-9]+)" _m "${CMAKE_JOB_POOLS}")
  if (CMAKE_MATCH_1)
    set(_setup_py_parallel_jobs "${CMAKE_MATCH_1}")
  endif ()
endif ()
if (NOT _setup_py_parallel_jobs AND DEFINED ENV{CMAKE_BUILD_PARALLEL_LEVEL}
    AND "$ENV{CMAKE_BUILD_PARALLEL_LEVEL}" MATCHES "^[0-9]+$")
  set(_setup_py_parallel_jobs "$ENV{CMAKE_BUILD_PARALLEL_LEVEL}")
endif ()
if (NOT _setup_py_parallel_jobs)
  cmake_host_system_information(RESULT _setup_py_parallel_jobs
                                QUERY NUMBER_OF_LOGICAL_CORES)
endif ()
add_custom_command(
  TARGET thrift_python_and_py3_bindings
  COMMAND
    ${CMAKE_COMMAND} -E env "CFLAGS=${CMAKE_C_FLAGS}"
    "CXXFLAGS=${CMAKE_CXX_FLAGS}" "CC=${_setup_py_cc}" "CXX=${_setup_py_cxx}"
    "LDFLAGS=${SETUP_LDFLAGS}" "CYTHON_INCLUDE_PATH=${cython_include_path}"
    "LIBRARY_DIRS=${library_dirs_str}"
    "THRIFT_BUILD_TESTS=${_setup_py_build_tests}" ${Python3_EXECUTABLE}
    ${_lib_dir}/setup.py -v build_ext --parallel ${_setup_py_parallel_jobs}
    bdist_wheel --libpython ${python_libname}
  WORKING_DIRECTORY ${_cybld})

# Run auditwheel repair to make the wheel self-contained
# This bundles .so dependencies and rewrites RPATH to $ORIGIN
# LD_LIBRARY_PATH tells auditwheel where to find the libraries to bundle.
# delocate does the same for macOS, where DYLD_LIBRARY_PATH tells it.
if (APPLE)
  set(repair_wheel "DYLD_LIBRARY_PATH=${library_dirs_str}"
      ${Python3_EXECUTABLE} -m delocate.cmd.delocate_wheel -w)
else ()
  set(repair_wheel "LD_LIBRARY_PATH=${library_dirs_str}"
      ${Python3_EXECUTABLE} -m auditwheel repair --wheel-dir)
endif ()
add_custom_command(
  TARGET thrift_python_and_py3_bindings
  COMMAND ${CMAKE_COMMAND} -E make_directory "${_cybld}/dist_self_contained"
  COMMAND ${CMAKE_COMMAND} -E env ${repair_wheel}
          "${_cybld}/dist_self_contained" "${_cybld}/dist/*.whl"
  WORKING_DIRECTORY ${_cybld})

# Install the self-contained wheel to the install prefix
# for users to pip install
# The wheel is processed by auditwheel to ${_cybld}/dist_self_contained/
# Destination stays literal (not CMAKE_INSTALL_DATADIR): out-of-tree consumers
# glob this path directly and cannot observe that variable.
install(
  DIRECTORY "${_cybld}/dist_self_contained/"
  DESTINATION share/thrift/wheels
  FILES_MATCHING
  PATTERN "*.whl")
