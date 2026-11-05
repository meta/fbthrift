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

cmake_minimum_required(VERSION 3.24)

project(THRIFT VERSION 1.0.0 LANGUAGES CXX)

# Build rules should still use $<TARGET_FILE...> instead of these paths:
# multi-config generators add a per-config subdirectory they do not carry.
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/bin)

include(GNUInstallDirs)

# Not cached, so the default follows CMAKE_INSTALL_LIBDIR on a reconfigure.
if (NOT DEFINED THRIFT_INSTALL_CMAKEDIR)
  set(THRIFT_INSTALL_CMAKEDIR "${CMAKE_INSTALL_LIBDIR}/cmake/thrift")
endif ()

# Relative RPATHs keep the installation relocatable. Append, so a value from
# the command line or from fbcode_builder (which sets one on macOS) survives.
# Both entries go on everything: libraries resolve via the first, executables
# in bindir via the second, and the unused one is an empty search path.
if (NOT WIN32)
  if (APPLE)
    set(_rpath_origin "@loader_path")
  else ()
    set(_rpath_origin "$ORIGIN")
  endif ()
  file(RELATIVE_PATH _libdir_from_bindir "/${CMAKE_INSTALL_BINDIR}"
       "/${CMAKE_INSTALL_LIBDIR}")
  list(APPEND CMAKE_INSTALL_RPATH "${_rpath_origin}"
       "${_rpath_origin}/${_libdir_from_bindir}")
endif ()

# Thrift's libraries default to STATIC, and without PIC they cannot link into
# a consumer built with BUILD_SHARED_LIBS=ON, nor into the thrift-python
# extension .so files. PIC is cheap here, and keeping the archives static
# avoids the cross-dylib RTTI and undefined-symbol problems that building
# Thrift itself shared exposes.
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# The compile settings and the include directories for `#include <thrift/...>`
# and generated headers. PUBLIC targets also export the relocatable install
# path; PRIVATE ones do not.
function (thrift_configure_target target visibility)
  target_compile_features(${target} ${visibility} cxx_std_20)
  # GCC 10 needs this even under -std=c++20, and folly keys FOLLY_HAS_COROUTINES
  # off it. Inert from GCC 11 on.
  target_compile_options(${target} ${visibility}
                         $<$<COMPILE_LANG_AND_ID:CXX,GNU>:-fcoroutines>)
  target_include_directories(
    ${target} ${visibility} $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${PROJECT_BINARY_DIR}>)
  if (NOT visibility STREQUAL "PRIVATE")
    target_include_directories(${target} ${visibility}
                               $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
  endif ()
endfunction ()

# Prepend, so entries from a parent project or the command line survive.
list(PREPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/thrift/cmake"
     "${CMAKE_CURRENT_SOURCE_DIR}/build/fbcode_builder/CMake")

option(
  THRIFT_RPC
  "Enable the RPC transport stack and its dependencies (fizz, wangle, mvfst)."
  ON)
option(THRIFT_TESTS "Enable tests (requires GTest)." OFF)
option(
  THRIFT_BENCHMARKS
  "Enable benchmarks (requires google/benchmark). Defaults to THRIFT_TESTS."
  ${THRIFT_TESTS})
option(THRIFT_PY_DEPRECATED
       "Enable the deprecated thrift-py library; use thrift-python instead" ON)
option(THRIFT_PYTHON
       "Enable the thrift-python library and extensions (requires Cython)" OFF)

# Records a dependency that exists only in this build tree. As in folly, an
# installed thrift would point at files that are not there, so install rules
# are turned off unless CMAKE_SKIP_INSTALL_RULES is set explicitly. A macro, so
# the setting reaches the subdirectories added after it.
set(thrift_build_local_deps "")
macro (thrift_build_local name)
  list(APPEND thrift_build_local_deps ${name})
  if (NOT DEFINED CMAKE_SKIP_INSTALL_RULES)
    set(CMAKE_SKIP_INSTALL_RULES ON)
    set(thrift_skipped_install ON)
  endif ()
endmacro ()

# Sets `out` to `dep` kept out of thrift's export, like folly_localize_deps.
# CMake before 3.26 has no $<BUILD_LOCAL_INTERFACE:>.
function (thrift_localize out dep)
  if (CMAKE_VERSION VERSION_LESS 3.26)
    set(${out} "${dep}" PARENT_SCOPE)
  else ()
    set(${out} "$<BUILD_LOCAL_INTERFACE:${dep}>" PARENT_SCOPE)
  endif ()
endfunction ()

include(FBFetchFromManifest)

# Homebrew's OpenSSL is keg-only, so CMake's FindOpenSSL doesn't search it.
# Set before folly, which also looks for OpenSSL.
if (APPLE AND NOT DEFINED OPENSSL_ROOT_DIR)
  execute_process(COMMAND brew --prefix openssl OUTPUT_VARIABLE OPENSSL_ROOT_DIR
                  OUTPUT_STRIP_TRAILING_WHITESPACE ERROR_QUIET)
endif ()

# Fetch gflags if it is not installed. folly fetches it only for its own tests,
# but thrift_concurrency and thrift_frozen link it.
function (thrift_fetch_gflags)
  # As a subproject gflags builds only its single-threaded library.
  set(GFLAGS_BUILD_gflags_LIB ON)
  set(GFLAGS_BUILD_gflags_nothreads_LIB OFF)
  # gflags builds a static library even for a shared build, and its `gflags`
  # alias prefers it. Every shared library linking the alias then embeds its
  # own copy, and the second copy to register its flags aborts the process.
  # thrift-python links folly and glog as shared libraries even though thrift's
  # own are static, and folly turns BUILD_SHARED_LIBS on only after this runs.
  if (BUILD_SHARED_LIBS OR THRIFT_PYTHON)
    set(GFLAGS_BUILD_SHARED_LIBS ON)
    set(GFLAGS_BUILD_STATIC_LIBS OFF)
  endif ()
  fb_fetch_from_manifest(gflags gflags OVERRIDE_FIND_PACKAGE)
  # Every find_package(gflags) or find_package(Gflags) now loads this file
  # instead of gflags' package config or FindGflags.cmake, so it sets what
  # those do; folly, glog, fizz, wangle and mvfst read different subsets. glog
  # would otherwise determine the namespace by linking a test program, which
  # can't use a library that isn't built yet.
  FetchContent_GetProperties(gflags BINARY_DIR binary_dir)
  set(include_dir "${binary_dir}/include")
  file(
    WRITE "${CMAKE_FIND_PACKAGE_REDIRECTS_DIR}/gflags-extra.cmake"
    "set(gflags_LIBRARIES gflags)
set(GFLAGS_LIBRARIES gflags)
set(LIBGFLAGS_LIBRARY gflags)
set(gflags_INCLUDE_DIR \"${include_dir}\")
set(GFLAGS_INCLUDE_DIR \"${include_dir}\")
set(LIBGFLAGS_INCLUDE_DIR \"${include_dir}\")
set(LIBGFLAGS_FOUND ON)
set(gflags_NAMESPACE gflags)
")
endfunction ()

# Before folly, so that a fetched folly, and the glog it fetches, use the same
# gflags.
find_package(Gflags)
set(thrift_gflags gflags)
if (NOT LIBGFLAGS_FOUND)
  thrift_build_local(gflags)
  thrift_fetch_gflags()
  thrift_localize(thrift_gflags gflags)
endif ()

# Fetch zlib if it is not installed.
function (thrift_fetch_zlib)
  set(ZLIB_BUILD_EXAMPLES OFF)
  fb_fetch_from_manifest(zlib zlib OVERRIDE_FIND_PACKAGE)
  # The static library, like the other fetched dependencies. zlib also declares
  # a shared one, which nothing builds.
  add_library(ZLIB::ZLIB ALIAS zlibstatic)
  # Every find_package(ZLIB), folly's and fizz's included, now loads this file
  # instead of FindZLIB, so it sets the variables they read. zlib generates
  # zconf.h in its binary directory.
  FetchContent_GetProperties(zlib SOURCE_DIR source_dir BINARY_DIR binary_dir)
  file(WRITE "${CMAKE_FIND_PACKAGE_REDIRECTS_DIR}/zlib-extra.cmake"
       "set(ZLIB_INCLUDE_DIRS \"${source_dir};${binary_dir}\")
set(ZLIB_LIBRARIES ZLIB::ZLIB)
set(ZLIB_LIBRARY ZLIB::ZLIB)
")
endfunction ()

# Fetch zstd if it is not installed.
function (thrift_fetch_zstd)
  set(ZSTD_BUILD_PROGRAMS OFF)
  set(ZSTD_BUILD_TESTS OFF)
  set(ZSTD_BUILD_SHARED OFF)
  # zstd FORCEs CMAKE_BUILD_TYPE to Release when it is unset.
  set(saved_build_type "${CMAKE_BUILD_TYPE}")
  fb_fetch_from_manifest(zstd zstd OVERRIDE_FIND_PACKAGE)
  if (NOT saved_build_type)
    set(CMAKE_BUILD_TYPE "" CACHE STRING "Choose the type of build." FORCE)
  endif ()
  add_library(zstd::zstd ALIAS libzstd_static)
  # As for zlib, every find_package(Zstd) now loads this file instead of
  # FindZstd.cmake. thrift-python reads ZSTD_INCLUDE_DIR too.
  FetchContent_GetProperties(zstd SOURCE_DIR source_dir)
  set(include_dir "${source_dir}/lib")
  file(WRITE "${CMAKE_FIND_PACKAGE_REDIRECTS_DIR}/zstd-extra.cmake"
       "set(ZSTD_FOUND ON)
set(ZSTD_INCLUDE_DIR \"${include_dir}\")
set(ZSTD_LIBRARY zstd::zstd)
")
  set(ZSTD_INCLUDE_DIR "${include_dir}" PARENT_SCOPE)
endfunction ()

# Only the RPC transport links zlib and zstd. Before folly, so that a fetched
# folly, and the fizz fetched below, use the same copies.
set(thrift_zlib ZLIB::ZLIB)
set(thrift_zstd zstd::zstd)
if (THRIFT_RPC)
  find_package(ZLIB)
  if (NOT ZLIB_FOUND)
    thrift_build_local(zlib)
    thrift_fetch_zlib()
    thrift_localize(thrift_zlib ZLIB::ZLIB)
  endif ()
  find_package(Zstd)
  if (NOT Zstd_FOUND)
    thrift_build_local(zstd)
    thrift_fetch_zstd()
    thrift_localize(thrift_zstd zstd::zstd)
  endif ()
endif ()

find_package(folly CONFIG QUIET)
if (NOT folly_FOUND)
  thrift_build_local(folly)
  if (THRIFT_PYTHON)
    # thrift-python bundles folly's Python package and links folly's
    # libraries, which folly builds only as shared libraries for Python.
    set(PYTHON_EXTENSIONS ON)
  endif ()
  fb_fetch_from_manifest(folly folly)
  if (THRIFT_PYTHON)
    # PYTHON_EXTENSIONS forced BUILD_SHARED_LIBS on in the cache. Thrift's own
    # libraries, and the projects it fetches, stay static, as above.
    set(BUILD_SHARED_LIBS OFF)
  endif ()
endif ()

# Older folly installs export Threads::Threads without declaring it.
# Newer folly finds it itself, making this redundant.
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads)

# Find required dependencies for thrift/lib
find_package(Glog REQUIRED)
# Thrift links fmt::fmt directly. A folly that this build fetched found fmt in
# its own directory, where the imported target is invisible here, unless it
# fetched fmt too.
if (NOT TARGET fmt::fmt)
  find_package(fmt CONFIG REQUIRED)
endif ()
find_package(OpenSSL REQUIRED)

# folly lists here whatever it had to build into its own build tree. Such a
# dependency is absent from an installed folly, so it must not reach thrift's
# install interface either. The property is global, so it is readable here even
# though folly set it in the scope FetchContent added it in.
get_property(thrift_folly_build_local GLOBAL PROPERTY FOLLY_BUILD_LOCAL_TARGETS)
set(thrift_openssl_crypto OpenSSL::Crypto)
if ("OpenSSL::Crypto" IN_LIST thrift_folly_build_local)
  thrift_build_local(OpenSSL)
  thrift_localize(thrift_openssl_crypto OpenSSL::Crypto)
endif ()

# Fetch xxhash if it is not installed.
function (thrift_fetch_xxhash)
  set(XXHASH_BUILD_XXHSUM OFF)
  # Otherwise xxhash's option() caches BUILD_SHARED_LIBS=ON for all of thrift.
  if (NOT DEFINED BUILD_SHARED_LIBS)
    set(BUILD_SHARED_LIBS OFF)
  endif ()
  # xxhash FORCEs CMAKE_BUILD_TYPE to Release when it is unset.
  set(saved_build_type "${CMAKE_BUILD_TYPE}")
  fb_fetch_from_manifest(xxhash xxhash)
  if (NOT saved_build_type)
    set(CMAKE_BUILD_TYPE "" CACHE STRING "Choose the type of build." FORCE)
  endif ()
  set_target_properties(xxhash PROPERTIES POSITION_INDEPENDENT_CODE ON)
  # The name an installed xxhash provides.
  add_library(Xxhash::xxhash INTERFACE IMPORTED GLOBAL)
  target_link_libraries(Xxhash::xxhash INTERFACE xxhash)
  FetchContent_GetProperties(xxhash SOURCE_DIR source_dir)
  # Replace find_package(Xxhash)'s NOTFOUND result in the cache, which is where
  # FBThriftCppLibrary.cmake finds it in projects built alongside fbthrift.
  set(Xxhash_INCLUDE_DIR "${source_dir}" CACHE PATH "xxhash's include directory" FORCE)
  set(Xxhash_LIBRARY xxhash PARENT_SCOPE)
  set(Xxhash_FOUND ON PARENT_SCOPE)
endfunction ()

find_package(Xxhash)
set(thrift_xxhash Xxhash::xxhash)
if (NOT Xxhash_FOUND)
  # Bundled mode, which a fetched xxhash builds in, disables its install rules.
  thrift_build_local(xxhash)
  thrift_fetch_xxhash()
  thrift_localize(thrift_xxhash Xxhash::xxhash)
endif ()

# libsodium ships no CMakeLists.txt, so ExternalProject runs its configure
# script at build time, as folly does for OpenSSL.
function (thrift_build_sodium)
  fb_manifest_value(libsodium download url url)
  fb_manifest_value(libsodium download sha256 sha)
  if (WIN32)
    message(
      FATAL_ERROR
        "libsodium is not installed, and building it is not supported on "
        "Windows. Install libsodium and set sodium_DIR.")
  endif ()
  find_program(THRIFT_SODIUM_MAKE NAMES make)
  if (NOT THRIFT_SODIUM_MAKE)
    message(FATAL_ERROR "libsodium is not installed, and building it needs "
                        "make. Install make or libsodium.")
  endif ()
  message(STATUS "libsodium not found, building ${url}")
  set(prefix "${CMAKE_CURRENT_BINARY_DIR}/libsodium")
  set(lib "${prefix}/lib/libsodium.a")
  include(ExternalProject)
  ExternalProject_Add(
    libsodium
    URL "${url}"
    URL_HASH SHA256=${sha}
    DOWNLOAD_EXTRACT_TIMESTAMP FALSE
    BUILD_IN_SOURCE ON
    CONFIGURE_COMMAND <SOURCE_DIR>/configure --prefix=${prefix}
                      --libdir=${prefix}/lib --disable-shared --with-pic
    BUILD_COMMAND ${THRIFT_SODIUM_MAKE} -j
    INSTALL_COMMAND ${THRIFT_SODIUM_MAKE} install
    # Without this Ninja has no rule to produce the library.
    BUILD_BYPRODUCTS "${lib}")
  file(MAKE_DIRECTORY "${prefix}/include")
  # The target fizz links. add_dependencies() orders compiling against the
  # headers; BUILD_BYPRODUCTS only orders linking.
  add_library(sodium STATIC IMPORTED GLOBAL)
  set_target_properties(
    sodium PROPERTIES IMPORTED_LOCATION "${lib}" INTERFACE_INCLUDE_DIRECTORIES
                                                 "${prefix}/include")
  add_dependencies(sodium libsodium)
  # Makes fizz's find_package(Sodium) resolve to this instead of FindSodium.
  file(WRITE "${CMAKE_FIND_PACKAGE_REDIRECTS_DIR}/sodium-config.cmake"
       "set(sodium_INCLUDE_DIR \"${prefix}/include\")\n")
endfunction ()

set(thrift_wangle wangle::wangle)
set(thrift_mvfst_server mvfst::mvfst_server_async_tran)
if (THRIFT_RPC)
  find_package(fizz CONFIG QUIET)
  find_package(wangle CONFIG QUIET)
  find_package(mvfst CONFIG QUIET)
  if (NOT fizz_FOUND)
    # Not find_package(Sodium): FindSodium defines the target fizz links even
    # when it finds nothing.
    find_library(THRIFT_SODIUM_LIBRARY sodium)
    if (NOT THRIFT_SODIUM_LIBRARY)
      thrift_build_sodium()
    endif ()
    thrift_build_local(fizz)
    fb_fetch_from_manifest(fizz fizz)
  endif ()
  if (NOT wangle_FOUND)
    # When folly fetched libevent, FindLibEvent's NOTFOUND results stay cached
    # and wangle would use them. folly's link interface carries libevent.
    foreach (var LIBEVENT_INCLUDE_DIR LIBEVENT_LIB)
      if (NOT ${var})
        set(${var} "")
      endif ()
    endforeach ()
    thrift_build_local(wangle)
    fb_fetch_from_manifest(wangle wangle)
    thrift_localize(thrift_wangle wangle::wangle)
  endif ()
  if (NOT mvfst_FOUND)
    thrift_build_local(mvfst)
    fb_fetch_from_manifest(mvfst mvfst)
    thrift_localize(thrift_mvfst_server mvfst::mvfst_server_async_tran)
  endif ()
endif ()

# Add the test dependencies.
if (THRIFT_TESTS)
  find_package(GTest REQUIRED)
  include(GoogleTest)
  enable_testing()
endif ()

if (thrift_skipped_install)
  list(JOIN thrift_build_local_deps ", " text)
  message(STATUS "Not installable, so install rules are off. Install these and "
                 "reconfigure for an installable thrift: ${text}")
endif ()

add_subdirectory(thrift)

# include(CPack) snapshots the CPACK_* variables where it is called, so keep it
# after every target, install rule and CPACK_* setting. A parent project that
# adds Thrift with add_subdirectory() or FetchContent does its own packaging.
if (PROJECT_IS_TOP_LEVEL)
  if (NOT DEFINED CPACK_GENERATOR)
    set(CPACK_GENERATOR RPM)
  endif ()
  set(CPACK_RPM_PACKAGE_PROVIDES thrift)
  include(CPack)
endif ()
