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

# Requires CMake 3.14. Functions record the policies in effect where they are
# defined, and projects with any cmake_minimum_required() include this file.
cmake_policy(VERSION 3.14...3.31)

# ${THRIFT_COMPILER} goes to COMMAND as-is rather than through
# $<TARGET_FILE:...>. CMake substitutes an imported target's location and adds
# the dependency, and when cross-compiling an in-tree compiler it falls back to
# a host thrift on PATH -- which a baked-in path to the unrunnable
# target-architecture binary would defeat.

# The in-tree compiler target. A project building against an installed Thrift
# overrides it before including this file: its exported targets carry a
# namespace prefix, and some such projects point it at a thrift binary found on
# PATH instead.
if (NOT DEFINED THRIFT_COMPILER)
  set(THRIFT_COMPILER thrift)
endif ()

# Sets <headers_var> and <sources_var> to the files the mstch_cpp2 generator
# writes to <dir> for <stem>.thrift with these services and options.
# `fast_client_stubs` is assumed, as thrift_generate() always passes it.
function (
  _thrift_cpp2_files
  dir
  stem
  services
  options
  headers_var
  sources_var)
  # The compiler takes comma-separated options and tests an option's key, so
  # `no_metadata=0` counts as no_metadata.
  string(REPLACE "," ";" options "${options}")
  string(REGEX REPLACE "=[^;]*" "" keys "${options}")
  foreach (option ${options})
    if (option MATCHES
        "^(client_cpp_splits|single_file_service|include_prefix)(=|$)"
        OR (option MATCHES "^types_cpp_splits" AND NOT option MATCHES
                                                   "^types_cpp_splits=0+$"))
      message(FATAL_ERROR "${option} is not supported by thrift_generate()")
    endif ()
  endforeach ()
  set(gen_dir "${dir}/gen-cpp2")
  if ("py3cpp" IN_LIST keys)
    set(gen_dir "${dir}/gen-py3cpp")
  endif ()
  set(p "${gen_dir}/${stem}")
  set(headers
      ${p}_clients.h
      ${p}_clients_fwd.h
      ${p}_constants.h
      ${p}_data.h
      ${p}_fast_clients.h
      ${p}_handlers.h
      ${p}_metadata.h
      ${p}_types.h
      ${p}_types.tcc
      ${p}_types_custom_protocol.h
      ${p}_types_fwd.h)
  # _sinit.cpp is empty unless the `any` option is given.
  set(sources
      ${p}_constants.cpp
      ${p}_data.cpp
      ${p}_sinit.cpp
      ${p}_types.cpp
      ${p}_types_binary.cpp
      ${p}_types_compact.cpp
      ${p}_types_serialization.cpp)
  if (NOT "no_metadata" IN_LIST keys)
    list(APPEND sources ${p}_metadata.cpp)
  endif ()
  if ("frozen2" IN_LIST keys)
    list(APPEND headers ${p}_layouts.h)
    list(APPEND sources ${p}_layouts.cpp)
  endif ()
  # The compiler also writes <service>_processmap_{binary,compact}.cpp, which
  # only include <service>.h and <service>.tcc and define nothing.
  foreach (service ${services})
    set(s "${gen_dir}/${service}")
    list(
      APPEND
      headers
      ${s}.h
      ${s}.tcc
      ${s}AsyncClient.h
      ${s}FastClient.h
      ${s}_custom_protocol.h)
    list(APPEND sources ${s}.cpp ${s}AsyncClient.cpp ${s}FastClient.cpp)
  endforeach ()
  set(${headers_var} "${headers}" PARENT_SCOPE)
  set(${sources_var} "${sources}" PARENT_SCOPE)
endfunction ()

# Sets <out_var> to the keywords among <keywords> that have an empty string as
# their value in <args>, the "${ARGV}" of the caller. cmake_parse_arguments()
# drops such a keyword before CMP0174 (CMake 3.31).
function (_thrift_empty_keywords out_var keywords args)
  set(empty)
  list(LENGTH args length)
  math(EXPR last "${length} - 2")
  if (last GREATER_EQUAL 0)
    foreach (i RANGE ${last})
      math(EXPR next "${i} + 1")
      list(GET args ${i} arg)
      list(GET args ${next} value)
      if (arg IN_LIST keywords AND "${value}" STREQUAL "")
        list(APPEND empty "${arg}")
      endif ()
    endforeach ()
  endif ()
  set(${out_var} "${empty}" PARENT_SCOPE)
endfunction ()

# Generates code from a .thrift file with the Thrift compiler and adds it to a
# target.
#
#   thrift_generate(<target> <file>.thrift
#     LANGUAGE <cpp2|py3|python>
#     [SERVICES <service>...]
#     [OPTIONS <option>...]
#     [INCLUDE_DIRECTORIES <dir>...]
#     [OUTPUT_DIRECTORY <dir>]
#     [INCLUDE_PREFIX <prefix>]
#     [NAMESPACE <py.namespace>]
#     [FILE_DEPENDS <file>...]
#     [LEGACY_STRICT])
#
#   <target> - The target to add the generated files to as sources. For cpp2,
#       it also links thrift::protocol publicly, which provides the include
#       directories and runtime the generated code needs.
#   <file>.thrift - The file to compile, relative to CMAKE_CURRENT_SOURCE_DIR.
#   LANGUAGE - The generator to use.
#   SERVICES - The services declared in the file.
#   OPTIONS - Generator options, e.g. `OPTIONS json no_metadata`. For cpp2, the
#       options that change which files are generated (no_metadata, frozen2 and
#       py3cpp) are accounted for, and types_cpp_splits, client_cpp_splits and
#       single_file_service are not supported.
#   INCLUDE_DIRECTORIES - Where the compiler looks for included .thrift files,
#       relative to CMAKE_CURRENT_SOURCE_DIR. Defaults to PROJECT_SOURCE_DIR. An
#       entry can be a generator expression that evaluates to a list of
#       directories, such as $<TARGET_PROPERTY:...>. The compiler also looks in
#       THRIFT_INCLUDE_DIR, after these, so files can include Thrift's own
#       .thrift files, such as thrift/annotation/thrift.thrift.
#   OUTPUT_DIRECTORY - Where gen-<language> is created, relative to
#       CMAKE_CURRENT_BINARY_DIR. Defaults to the file's directory under
#       PROJECT_BINARY_DIR.
#   INCLUDE_PREFIX - (cpp2, py3) The prefix generated code uses to include
#       generated headers; empty for none. Defaults to the file's directory
#       relative to PROJECT_SOURCE_DIR.
#   NAMESPACE - (python) Must match the "namespace py3" directive in the file.
#       Dot-separated (e.g. "example.sum") maps to output directory structure
#       (example/sum/<file>/).
#   FILE_DEPENDS - Other files that the generation depends on, such as the
#       .thrift files that <file> includes. Can contain generator expressions.
#   LEGACY_STRICT - Passes --legacy-strict to the compiler.
#
# For a file outside PROJECT_SOURCE_DIR, OUTPUT_DIRECTORY is required, and so
# is INCLUDE_PREFIX for cpp2 and py3.
#
# Creates no targets of its own and adds no install rules. Call it in the
# directory that creates <target>: CMake gives the rule that generates the files
# only to targets in that directory.
function (thrift_generate)
  # Earlier forms took the .thrift file, or a file name without the extension,
  # first. Checking the second argument stops them before they are misparsed.
  if (ARGC LESS 2 OR NOT "${ARGV1}" MATCHES "\\.thrift$")
    list(JOIN ARGV " " args)
    message(
      FATAL_ERROR
        "thrift_generate: expected thrift_generate(<target> <file>.thrift "
        "LANGUAGE ...), got thrift_generate(${args})")
  endif ()
  set(target "${ARGV0}")
  set(file "${ARGV1}")
  if (NOT TARGET "${target}")
    message(
      FATAL_ERROR
        "thrift_generate: ${target} is not a target; create it before "
        "calling thrift_generate(${target} ${file} ...)")
  endif ()
  if (NOT THRIFT_COMPILER)
    message(
      FATAL_ERROR "thrift_generate: no Thrift compiler; set THRIFT_COMPILER")
  endif ()
  set(one_value_keywords LANGUAGE OUTPUT_DIRECTORY INCLUDE_PREFIX NAMESPACE)
  # Parsed only to be rejected; unparsed, they would join SERVICES or OPTIONS.
  set(removed_keywords TARGET OUT_SOURCES)
  _thrift_empty_keywords(empty_keywords "${one_value_keywords}" "${ARGV}")
  cmake_policy(PUSH)
  if (POLICY CMP0174)
    cmake_policy(SET CMP0174 NEW)
  endif ()
  cmake_parse_arguments(
    PARSE_ARGV 2 THRIFT_GENERATE "LEGACY_STRICT"
    "${one_value_keywords};${removed_keywords}"
    "SERVICES;OPTIONS;INCLUDE_DIRECTORIES;FILE_DEPENDS")
  cmake_policy(POP)
  foreach (keyword ${removed_keywords})
    if (DEFINED THRIFT_GENERATE_${keyword}
        OR keyword IN_LIST THRIFT_GENERATE_KEYWORDS_MISSING_VALUES)
      message(
        FATAL_ERROR
          "thrift_generate: ${keyword} was removed; the generated files are "
          "added to the target, the first argument: "
          "thrift_generate(<target> <file>.thrift LANGUAGE ...)")
    endif ()
  endforeach ()
  if (DEFINED THRIFT_GENERATE_UNPARSED_ARGUMENTS)
    message(FATAL_ERROR "thrift_generate: unexpected arguments: "
                        "${THRIFT_GENERATE_UNPARSED_ARGUMENTS}")
  endif ()
  foreach (keyword ${THRIFT_GENERATE_KEYWORDS_MISSING_VALUES} ${empty_keywords})
    # Only an explicit "" means empty. A keyword with no argument at all is
    # also in KEYWORDS_MISSING_VALUES, and under CMP0174 it is DEFINED too.
    if (keyword MATCHES "^(INCLUDE_PREFIX|NAMESPACE)$" AND keyword IN_LIST
                                                           empty_keywords)
      continue()
    endif ()
    if (keyword IN_LIST one_value_keywords)
      message(FATAL_ERROR "thrift_generate: ${keyword} needs a value")
    endif ()
  endforeach ()
  set(language "${THRIFT_GENERATE_LANGUAGE}")
  if (NOT language MATCHES "^(cpp2|py3|python)$")
    message(FATAL_ERROR "thrift_generate: LANGUAGE must be cpp2, py3 or python")
  endif ()
  set(namespace "${THRIFT_GENERATE_NAMESPACE}")
  if (NOT namespace STREQUAL "" AND NOT language STREQUAL "python")
    message(FATAL_ERROR "thrift_generate: NAMESPACE requires LANGUAGE python")
  endif ()
  if (NOT "${THRIFT_GENERATE_INCLUDE_PREFIX}" STREQUAL "" AND language STREQUAL
                                                              "python")
    message(
      FATAL_ERROR "thrift_generate: INCLUDE_PREFIX does not apply to python")
  endif ()

  get_filename_component(thrift_file "${file}" ABSOLUTE BASE_DIR
                         "${CMAKE_CURRENT_SOURCE_DIR}")
  get_filename_component(stem "${thrift_file}" NAME_WLE)
  get_filename_component(thrift_dir "${thrift_file}" DIRECTORY)
  file(RELATIVE_PATH rel "${PROJECT_SOURCE_DIR}" "${thrift_dir}")
  set(outside FALSE)
  if (rel MATCHES "^\\.\\.(/|$)" OR IS_ABSOLUTE "${rel}")
    set(outside TRUE)
  endif ()

  if (DEFINED THRIFT_GENERATE_OUTPUT_DIRECTORY)
    get_filename_component(output_dir "${THRIFT_GENERATE_OUTPUT_DIRECTORY}"
                           ABSOLUTE BASE_DIR "${CMAKE_CURRENT_BINARY_DIR}")
  elseif (outside)
    message(
      FATAL_ERROR "thrift_generate: ${file} is outside PROJECT_SOURCE_DIR, so "
                  "OUTPUT_DIRECTORY is required")
  elseif (rel STREQUAL "")
    set(output_dir "${PROJECT_BINARY_DIR}")
  else ()
    set(output_dir "${PROJECT_BINARY_DIR}/${rel}")
  endif ()
  if (DEFINED THRIFT_GENERATE_INCLUDE_PREFIX OR "INCLUDE_PREFIX" IN_LIST
                                                empty_keywords)
    set(include_prefix "${THRIFT_GENERATE_INCLUDE_PREFIX}")
  elseif (language STREQUAL "python")
    set(include_prefix "")
  elseif (outside)
    message(
      FATAL_ERROR "thrift_generate: ${file} is outside PROJECT_SOURCE_DIR, so "
                  "INCLUDE_PREFIX is required")
  else ()
    set(include_prefix "${rel}")
  endif ()
  if (include_prefix MATCHES "(^|/)\\.\\.(/|$)" OR IS_ABSOLUTE
                                                   "${include_prefix}")
    message(
      FATAL_ERROR "thrift_generate: INCLUDE_PREFIX ${include_prefix} must "
                  "be a relative path without ..")
  endif ()
  # COMMAND_EXPAND_LISTS below splits a generator expression's list into
  # separate arguments, so each directory gets its own -I.
  set(include_args)
  foreach (dir ${THRIFT_GENERATE_INCLUDE_DIRECTORIES})
    if (dir MATCHES "\\$<")
      set(sep "$<SEMICOLON>-I$<SEMICOLON>")
      list(APPEND include_args
           "$<$<BOOL:$<JOIN:${dir},>>:-I$<SEMICOLON>$<JOIN:${dir},${sep}>>")
    else ()
      get_filename_component(dir "${dir}" ABSOLUTE BASE_DIR
                             "${CMAKE_CURRENT_SOURCE_DIR}")
      list(APPEND include_args "-I" "${dir}")
    endif ()
  endforeach ()
  if (NOT include_args)
    set(include_args "-I" "${PROJECT_SOURCE_DIR}")
  endif ()
  if (THRIFT_INCLUDE_DIR)
    get_filename_component(dir "${THRIFT_INCLUDE_DIR}" ABSOLUTE)
    if (NOT dir IN_LIST include_args)
      list(APPEND include_args "-I" "${dir}")
    endif ()
  endif ()
  set(compiler_flags)
  if (THRIFT_GENERATE_LEGACY_STRICT)
    set(compiler_flags --legacy-strict)
  endif ()
  set(services "${THRIFT_GENERATE_SERVICES}")
  set(options "${THRIFT_GENERATE_OPTIONS}")
  set(generator "mstch_${language}")
  set(gen_dir "${output_dir}/gen-${language}")
  if ("${language}" STREQUAL "cpp2")
    _thrift_cpp2_files("${output_dir}" "${stem}" "${services}" "${options}"
                       headers sources)
    # The FastClient files of an annotated service are generated either way, so
    # list them for every service.
    list(APPEND options fast_client_stubs)
  elseif ("${language}" STREQUAL "py3")
    set(headers
        ${gen_dir}/${stem}_constants.h ${gen_dir}/${stem}_data.h
        ${gen_dir}/${stem}_metadata.h ${gen_dir}/${stem}_types.h
        ${gen_dir}/${stem}_types.tcc ${gen_dir}/${stem}_types_custom_protocol.h)
    set(sources
        ${gen_dir}/${stem}_constants.cpp
        ${gen_dir}/${stem}_data.cpp
        ${gen_dir}/${stem}_types.cpp
        ${gen_dir}/${stem}_types_binary.cpp
        ${gen_dir}/${stem}_types_compact.cpp
        ${gen_dir}/${stem}_types_serialization.cpp)
    if (NOT "no_metadata" IN_LIST options)
      list(APPEND sources ${gen_dir}/${stem}_metadata.cpp)
    endif ()
    foreach (service ${services})
      list(APPEND headers ${gen_dir}/${service}.h ${gen_dir}/${service}.tcc
           ${gen_dir}/${service}AsyncClient.h
           ${gen_dir}/${service}_custom_protocol.h)
      list(APPEND sources ${gen_dir}/${service}.cpp
           ${gen_dir}/${service}AsyncClient.cpp)
    endforeach ()
  endif ()
  string(REPLACE ";" "," joined_options "${options}")
  # The compiler requires -o to exist. Ninja creates the directories of a
  # command's outputs, but Makefiles don't, and only directories added with
  # add_subdirectory() exist otherwise.
  file(MAKE_DIRECTORY "${output_dir}")

  if ("${language}" STREQUAL "python")
    # thrift-python uses PEP 420 namespace packages — no __init__.py.
    if (NOT "${namespace}" STREQUAL "")
      string(REPLACE "." "/" namespace_dir "${namespace}")
      set(python_dir "${output_dir}/gen-python/${namespace_dir}/${stem}")
    else ()
      set(python_dir "${output_dir}/gen-python/${stem}")
    endif ()
    set(headers "")
    set(sources
        ${python_dir}/thrift_types.py
        ${python_dir}/thrift_types.pyi
        ${python_dir}/thrift_metadata.py
        ${python_dir}/thrift_enums.py
        ${python_dir}/thrift_abstract_types.py
        ${python_dir}/thrift_mutable_types.py
        ${python_dir}/thrift_mutable_types.pyi)
    if (NOT "${services}" STREQUAL "")
      list(
        APPEND
        sources
        ${python_dir}/thrift_services.py
        ${python_dir}/thrift_clients.py
        ${python_dir}/thrift_mutable_services.py
        ${python_dir}/thrift_mutable_clients.py)
    endif ()
    set(gen "${generator}")
    if (NOT "${joined_options}" STREQUAL "")
      set(gen "${gen}:${joined_options}")
    endif ()
    add_custom_command(
      OUTPUT ${sources}
      COMMAND ${THRIFT_COMPILER} ${compiler_flags} --gen "${gen}" -o
              ${output_dir} ${include_args} "${thrift_file}"
      DEPENDS ${THRIFT_COMPILER} "${thrift_file}"
              ${THRIFT_GENERATE_FILE_DEPENDS}
      COMMENT "Generating ${stem} thrift-python files. Output: ${output_dir}"
      COMMAND_EXPAND_LISTS)
  else ()
    if ("${language}" STREQUAL "py3")
      file(WRITE "${gen_dir}/${stem}/__init__.py" )
    endif ()
    set(gen "${generator}:${joined_options}")
    if (NOT "${include_prefix}" STREQUAL "")
      if (NOT "${joined_options}" STREQUAL "")
        set(gen "${gen},")
      endif ()
      set(gen "${gen}include_prefix=${include_prefix}")
    endif ()
    # Without include_prefix, the compiler takes the directory of the file it is
    # given as the prefix, so give it the file name alone.
    set(input "${thrift_file}")
    set(working_dir "${CMAKE_CURRENT_BINARY_DIR}")
    if ("${include_prefix}" STREQUAL "")
      get_filename_component(input "${thrift_file}" NAME)
      set(working_dir "${thrift_dir}")
    endif ()
    add_custom_command(
      OUTPUT ${headers} ${sources}
      COMMAND ${THRIFT_COMPILER} ${compiler_flags} --gen "${gen}" -o
              ${output_dir} ${include_args} "${input}"
      DEPENDS ${THRIFT_COMPILER} "${thrift_file}"
              ${THRIFT_GENERATE_FILE_DEPENDS}
      WORKING_DIRECTORY "${working_dir}"
      COMMENT "Generating ${stem} files. Output: ${output_dir}"
      COMMAND_EXPAND_LISTS)
  endif ()

  target_sources(${target} PRIVATE ${headers} ${sources})
  # For thrift_add_library().
  set_property(TARGET ${target} APPEND PROPERTY _THRIFT_HEADERS ${headers})
  if (language STREQUAL "cpp2")
    # Several calls can share a target.
    get_target_property(links ${target} LINK_LIBRARIES)
    if (NOT "thrift::protocol" IN_LIST links)
      target_link_libraries(${target} PUBLIC thrift::protocol)
    endif ()
  endif ()
endfunction ()

# Creates a library from a .thrift file, using thrift_generate() for the code.
#
#   thrift_add_library(<name> <file>.thrift
#     LANGUAGE cpp2
#     [SERVICES <service>...]
#     [DEPENDS <library>...]
#     [OPTIONS <option>...]
#     [INCLUDE_DIRECTORIES <dir>...]
#     [OUTPUT_DIRECTORY <dir>]
#     [INCLUDE_PREFIX <prefix>]
#     [LEGACY_STRICT])
#
#   <name> - The library to create, static or shared as BUILD_SHARED_LIBS says.
#   LANGUAGE - The generator to use. Only cpp2 is supported.
#   DEPENDS - The libraries of the .thrift files that <file> includes, created
#       by thrift_add_library() in this project or imported from an installed
#       one. <name> links them, and the compiler looks for included files in the
#       include directories of their .thrift_includes. A library of this
#       project can be created after <name>. The code is generated again when a
#       .thrift file of this project's DEPENDS libraries changes, or when an
#       imported library does, but not when a library of this project is
#       rebuilt.
#   INCLUDE_DIRECTORIES - Where the compiler looks for the .thrift files that
#       <file> includes, other than in the DEPENDS libraries' directories and
#       THRIFT_INCLUDE_DIR. Can contain generator expressions. Defaults to
#       PROJECT_SOURCE_DIR.
#   OUTPUT_DIRECTORY - The include directory of the generated code, relative to
#       CMAKE_CURRENT_BINARY_DIR. The code goes to
#       <OUTPUT_DIRECTORY>/<INCLUDE_PREFIX>/gen-cpp2. Defaults to
#       PROJECT_BINARY_DIR.
#   INCLUDE_PREFIX - As for thrift_generate(). The generated code of a library
#       with an empty INCLUDE_PREFIX can't be included by a library with a
#       prefix: the compiler gives the included file that prefix.
#   SERVICES, OPTIONS, LEGACY_STRICT - As for thrift_generate().
#
# <name> links thrift::protocol, and thrift::rpc if SERVICES is given, publicly.
# Its include directory, OUTPUT_DIRECTORY, lets code include the generated
# headers as <include_prefix>/gen-cpp2/<header> (gen-py3cpp with the py3cpp
# option).
#
# Also creates the INTERFACE library <name>.thrift_includes, whose include
# directories are where the compiler looks for files that <file> includes. It
# links the DEPENDS libraries' .thrift_includes.
#
# Adds no install rules. A project that installs <name> adds
# $<INSTALL_INTERFACE:...> include directories to <name> and, if it installs
# the .thrift file, to <name>.thrift_includes. These properties of <name> are
# for install(TARGETS <name> PUBLIC_HEADER DESTINATION
# <dir>/$<TARGET_PROPERTY:<name>,THRIFT_HEADER_DIR>):
#   PUBLIC_HEADER - The generated headers, which are all in one directory.
#   THRIFT_HEADER_DIR - The directory of the generated headers relative to the
#       include directory, such as <include_prefix>/gen-cpp2.
function (thrift_add_library)
  if (ARGC LESS 2 OR NOT "${ARGV1}" MATCHES "\\.thrift$")
    list(JOIN ARGV " " args)
    message(
      FATAL_ERROR
        "thrift_add_library: expected thrift_add_library(<name> <file>.thrift "
        "LANGUAGE ...), got thrift_add_library(${args})")
  endif ()
  set(name "${ARGV0}")
  set(file "${ARGV1}")
  _thrift_empty_keywords(empty_keywords "INCLUDE_PREFIX;OUTPUT_DIRECTORY"
                         "${ARGV}")
  cmake_policy(PUSH)
  if (POLICY CMP0174)
    cmake_policy(SET CMP0174 NEW)
  endif ()
  cmake_parse_arguments(
    PARSE_ARGV 2 ARG "LEGACY_STRICT" "LANGUAGE;OUTPUT_DIRECTORY;INCLUDE_PREFIX"
    "SERVICES;DEPENDS;OPTIONS;INCLUDE_DIRECTORIES")
  cmake_policy(POP)
  if (DEFINED ARG_UNPARSED_ARGUMENTS)
    message(FATAL_ERROR "thrift_add_library: unexpected arguments: "
                        "${ARG_UNPARSED_ARGUMENTS}")
  endif ()
  # An empty list of SERVICES, DEPENDS, OPTIONS or INCLUDE_DIRECTORIES is fine.
  foreach (keyword LANGUAGE OUTPUT_DIRECTORY INCLUDE_PREFIX)
    if (keyword IN_LIST ARG_KEYWORDS_MISSING_VALUES AND NOT keyword IN_LIST
                                                        empty_keywords)
      message(FATAL_ERROR "thrift_add_library: ${keyword} requires a value")
    endif ()
  endforeach ()
  if ("OUTPUT_DIRECTORY" IN_LIST empty_keywords)
    message(FATAL_ERROR "thrift_add_library: OUTPUT_DIRECTORY requires a value")
  endif ()
  if (NOT ARG_LANGUAGE STREQUAL "cpp2")
    message(
      FATAL_ERROR
        "thrift_add_library: LANGUAGE must be cpp2; for other languages, use "
        "thrift_generate()")
  endif ()
  # Not if (ARG_SERVICES): a service can be named Off.
  set(generate_args)
  if (NOT "${ARG_SERVICES}" STREQUAL "")
    list(APPEND generate_args SERVICES ${ARG_SERVICES})
  endif ()
  if (NOT "${ARG_OPTIONS}" STREQUAL "")
    list(APPEND generate_args OPTIONS ${ARG_OPTIONS})
  endif ()
  if (ARG_LEGACY_STRICT)
    list(APPEND generate_args LEGACY_STRICT)
  endif ()
  set(include_dirs)
  foreach (dir ${ARG_INCLUDE_DIRECTORIES})
    if (NOT dir MATCHES "\\$<")
      get_filename_component(dir "${dir}" ABSOLUTE BASE_DIR
                             "${CMAKE_CURRENT_SOURCE_DIR}")
    endif ()
    list(APPEND include_dirs "${dir}")
  endforeach ()
  if ("${include_dirs}" STREQUAL "")
    set(include_dirs "${PROJECT_SOURCE_DIR}")
  endif ()
  get_filename_component(thrift_file "${file}" ABSOLUTE BASE_DIR
                         "${CMAKE_CURRENT_SOURCE_DIR}")
  if (DEFINED ARG_INCLUDE_PREFIX OR "INCLUDE_PREFIX" IN_LIST empty_keywords)
    set(include_prefix "${ARG_INCLUDE_PREFIX}")
  else ()
    get_filename_component(thrift_dir "${thrift_file}" DIRECTORY)
    file(RELATIVE_PATH include_prefix "${PROJECT_SOURCE_DIR}" "${thrift_dir}")
    if (include_prefix MATCHES "^\\.\\.(/|$)" OR IS_ABSOLUTE
                                                 "${include_prefix}")
      message(
        FATAL_ERROR
          "thrift_add_library: ${file} is outside PROJECT_SOURCE_DIR, "
          "so INCLUDE_PREFIX is required")
    endif ()
  endif ()
  set(include_root "${PROJECT_BINARY_DIR}")
  if (DEFINED ARG_OUTPUT_DIRECTORY)
    get_filename_component(include_root "${ARG_OUTPUT_DIRECTORY}" ABSOLUTE
                           BASE_DIR "${CMAKE_CURRENT_BINARY_DIR}")
  endif ()
  set(output_dir "${include_root}")
  if (NOT include_prefix STREQUAL "")
    string(APPEND output_dir "/${include_prefix}")
  endif ()

  set(includes "${name}.thrift_includes")
  add_library(${includes} INTERFACE)
  foreach (dir ${include_dirs})
    target_include_directories(${includes} INTERFACE $<BUILD_INTERFACE:${dir}>)
  endforeach ()
  # INTERFACE_SOURCES is transitive, so the generation can depend on the
  # .thrift files of all the DEPENDS libraries.
  target_sources(${includes} INTERFACE $<BUILD_INTERFACE:${thrift_file}>)
  foreach (dep ${ARG_DEPENDS})
    target_link_libraries(${includes} INTERFACE ${dep}.thrift_includes)
  endforeach ()

  add_library(${name})
  # CMP0155 is NEW where this function is defined, so CMake would otherwise
  # scan the generated C++20 sources for modules, which they don't use.
  set_target_properties(${name} PROPERTIES CXX_SCAN_FOR_MODULES OFF)
  set(thrift_include_dirs
      "$<TARGET_PROPERTY:${includes},INTERFACE_INCLUDE_DIRECTORIES>")
  # Not the DEPENDS libraries of this project: the generated code changes only
  # with the .thrift files, and a dependency on a library would rerun the
  # compiler whenever the library is rebuilt. An imported library carries no
  # .thrift files, so it stands in for them, for the libraries that include
  # this one too.
  foreach (dep ${ARG_DEPENDS})
    if (TARGET ${dep})
      get_target_property(imported ${dep} IMPORTED)
      get_target_property(type ${dep} TYPE)
      if (imported AND type MATCHES "^(STATIC|SHARED|UNKNOWN)_LIBRARY$")
        target_sources(${includes}
                       INTERFACE $<BUILD_INTERFACE:$<TARGET_FILE:${dep}>>)
      endif ()
    endif ()
  endforeach ()
  set(file_depends "$<TARGET_PROPERTY:${includes},INTERFACE_SOURCES>")
  thrift_generate(
    ${name} "${thrift_file}"
    LANGUAGE cpp2
    INCLUDE_DIRECTORIES ${thrift_include_dirs}
    FILE_DEPENDS ${file_depends} ${generate_args}
    OUTPUT_DIRECTORY "${output_dir}"
    INCLUDE_PREFIX "${include_prefix}")

  get_target_property(headers ${name} _THRIFT_HEADERS)
  target_include_directories(${name} PUBLIC $<BUILD_INTERFACE:${include_root}>)
  target_link_libraries(${name} PUBLIC ${ARG_DEPENDS})
  if (NOT "${ARG_SERVICES}" STREQUAL "")
    target_link_libraries(${name} PUBLIC thrift::rpc)
  endif ()
  list(GET headers 0 header)
  get_filename_component(header_dir "${header}" DIRECTORY)
  file(RELATIVE_PATH header_dir "${include_root}" "${header_dir}")
  set_target_properties(${name} PROPERTIES PUBLIC_HEADER "${headers}"
                                           THRIFT_HEADER_DIR "${header_dir}")
endfunction ()
