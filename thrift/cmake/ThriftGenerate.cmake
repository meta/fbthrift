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
# writes to <gen_dir> for <stem>.thrift with these services and options.
function (
  _thrift_cpp2_files
  gen_dir
  stem
  services
  options
  headers_var
  sources_var)
  # The compiler tests an option's key, so `no_metadata=0` counts as
  # no_metadata, and uses the first value of a repeated key.
  set(keys)
  set(types_cpp_splits 0)
  set(client_cpp_splits)
  foreach (option ${options})
    string(REGEX REPLACE "=.*" "" key "${option}")
    if (key IN_LIST keys)
      continue()
    endif ()
    list(APPEND keys "${key}")
    if (option MATCHES "^types_cpp_splits=([0-9]+)$")
      math(EXPR types_cpp_splits "${CMAKE_MATCH_1}")
    elseif (option MATCHES "^types_cpp_splits=")
      message(FATAL_ERROR "thrift_generate: invalid ${option}")
    elseif (option MATCHES "^client_cpp_splits=")
      if (NOT option MATCHES "^client_cpp_splits=\\{(.*)\\}$")
        message(FATAL_ERROR "thrift_generate: invalid ${option}")
      endif ()
      string(REPLACE "," ";" client_cpp_splits "${CMAKE_MATCH_1}")
      foreach (entry ${client_cpp_splits})
        if (NOT entry MATCHES "^[^:]+:0*[1-9][0-9]*$")
          message(FATAL_ERROR "thrift_generate: invalid ${option}: each "
                              "count must be a positive integer")
        endif ()
      endforeach ()
    endif ()
  endforeach ()
  set(p "${gen_dir}/${stem}")
  set(headers
      ${p}_constants.h
      ${p}_data.h
      ${p}_metadata.h
      ${p}_types.h
      ${p}_types_fwd.h
      ${p}_types.tcc
      ${p}_types_custom_protocol.h
      ${p}_clients.h
      ${p}_clients_fwd.h
      ${p}_handlers.h)
  # _sinit.cpp is empty unless the `any` option is given.
  set(sources ${p}_constants.cpp ${p}_data.cpp ${p}_sinit.cpp)
  set(types_kinds types types_binary types_compact types_serialization)
  if (types_cpp_splits GREATER 0)
    math(EXPR last "${types_cpp_splits} - 1")
    string(LENGTH "${last}" width)
    foreach (i RANGE ${last})
      _thrift_pad("${i}" ${width} id)
      foreach (kind ${types_kinds})
        list(APPEND sources ${p}_${kind}.${id}.split.cpp)
      endforeach ()
    endforeach ()
  else ()
    foreach (kind ${types_kinds})
      list(APPEND sources ${p}_${kind}.cpp)
    endforeach ()
  endif ()
  if (NOT "no_metadata" IN_LIST keys)
    list(APPEND sources ${p}_metadata.cpp)
  endif ()
  if ("frozen2" IN_LIST keys)
    list(APPEND headers ${p}_layouts.h)
    list(APPEND sources ${p}_layouts.cpp)
  endif ()
  # fast_client_stubs, which thrift_generate() always passes, generates the
  # FastClient files.
  list(APPEND headers ${p}_fast_clients.h)
  if ("single_file_service" IN_LIST keys)
    list(APPEND headers ${p}_handlers-inl.h)
    list(APPEND sources ${p}_clients.cpp ${p}_handlers.cpp
         ${p}_fast_clients.cpp)
  else ()
    foreach (service ${services})
      set(s "${gen_dir}/${service}")
      list(
        APPEND
        headers
        ${s}.h
        ${s}.tcc
        ${s}AsyncClient.h
        ${s}_custom_protocol.h
        ${s}FastClient.h)
      list(APPEND sources ${s}.cpp ${s}FastClient.cpp)
      set(splits 0)
      foreach (entry ${client_cpp_splits})
        if (entry MATCHES "^${service}:([0-9]+)$")
          math(EXPR splits "${CMAKE_MATCH_1}")
        endif ()
      endforeach ()
      if (splits GREATER 0)
        math(EXPR last "${splits} - 1")
        string(LENGTH "${last}" width)
        foreach (i RANGE ${last})
          _thrift_pad("${i}" ${width} id)
          list(APPEND sources ${s}.${id}.async_client_split.cpp)
        endforeach ()
      else ()
        list(APPEND sources ${s}AsyncClient.cpp)
      endif ()
      # The compiler also writes <service>_processmap_{binary,compact}.cpp,
      # which only include <service>.h and <service>.tcc and define nothing.
    endforeach ()
  endif ()
  set(${headers_var} "${headers}" PARENT_SCOPE)
  set(${sources_var} "${sources}" PARENT_SCOPE)
endfunction ()

# Sets <out_var> to <number> padded with leading zeros to <width> digits.
function (_thrift_pad number width out_var)
  string(LENGTH "${number}" length)
  while (length LESS width)
    string(PREPEND number 0)
    math(EXPR length "${length} + 1")
  endwhile ()
  set(${out_var} "${number}" PARENT_SCOPE)
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
#       options that change which files are generated (no_metadata, frozen2,
#       types_cpp_splits, client_cpp_splits, single_file_service and py3cpp) are
#       accounted for.
#   INCLUDE_DIRECTORIES - Where the compiler looks for included .thrift files,
#       relative to CMAKE_CURRENT_SOURCE_DIR. Defaults to PROJECT_SOURCE_DIR. An
#       entry can be a generator expression that evaluates to a list of
#       directories, such as $<TARGET_PROPERTY:...>.
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
  # Before CMP0174 (CMake 3.31), cmake_parse_arguments() drops a keyword whose
  # value is an empty string, so find those here.
  set(empty_keywords)
  math(EXPR last "${ARGC} - 2")
  if (last GREATER_EQUAL 2)
    foreach (i RANGE 2 ${last})
      math(EXPR next "${i} + 1")
      if ("${ARGV${i}}" IN_LIST one_value_keywords AND "${ARGV${next}}"
                                                       STREQUAL "")
        list(APPEND empty_keywords "${ARGV${i}}")
      endif ()
    endforeach ()
  endif ()
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
  set(compiler_flags)
  if (THRIFT_GENERATE_LEGACY_STRICT)
    set(compiler_flags --legacy-strict)
  endif ()
  set(services "${THRIFT_GENERATE_SERVICES}")
  set(options "${THRIFT_GENERATE_OPTIONS}")
  set(generator "mstch_${language}")
  set(gen_dir "${output_dir}/gen-${language}")
  if ("${language}" STREQUAL "cpp2")
    # The FastClient files of an annotated service are generated either way, so
    # list them for every service.
    list(APPEND options fast_client_stubs)
    foreach (option ${options})
      if (option MATCHES "^py3cpp(=|$)")
        set(gen_dir "${output_dir}/gen-py3cpp")
      endif ()
    endforeach ()
    _thrift_cpp2_files("${gen_dir}" "${stem}" "${services}" "${options}"
                       headers sources)
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
    if ("layouts" IN_LIST options)
      list(APPEND sources ${gen_dir}/${stem}_layouts.cpp)
    endif ()
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
  set_property(TARGET ${target} PROPERTY _THRIFT_OUTPUT_DIRECTORY
                                         "${output_dir}")
  set_property(TARGET ${target} PROPERTY _THRIFT_INCLUDE_PREFIX
                                         "${include_prefix}")
  set_property(TARGET ${target} PROPERTY _THRIFT_GENERATOR "${gen}")
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
#   OUTPUT_DIRECTORY, INCLUDE_PREFIX - As for thrift_generate(), but
#       OUTPUT_DIRECTORY must end with INCLUDE_PREFIX. The generated code of a
#       library with an empty INCLUDE_PREFIX can't be included by a library
#       with a prefix: the compiler gives the included file that prefix.
#   SERVICES, OPTIONS, LEGACY_STRICT - As for thrift_generate().
#
# <name> links thrift::protocol, and thrift::rpc if SERVICES is given, publicly.
# Its include directory, OUTPUT_DIRECTORY without INCLUDE_PREFIX, lets code
# include the generated headers as <include_prefix>/gen-cpp2/<header>
# (gen-py3cpp with the py3cpp option).
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
  # An empty INCLUDE_PREFIX is meaningful, and cmake_parse_arguments() drops it
  # before CMP0174 (CMake 3.31).
  set(empty_prefix FALSE)
  math(EXPR last "${ARGC} - 2")
  if (last GREATER_EQUAL 2)
    foreach (i RANGE 2 ${last})
      math(EXPR next "${i} + 1")
      if ("${ARGV${i}}" STREQUAL "INCLUDE_PREFIX" AND "${ARGV${next}}" STREQUAL
                                                      "")
        set(empty_prefix TRUE)
      endif ()
    endforeach ()
  endif ()
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
    if (keyword IN_LIST ARG_KEYWORDS_MISSING_VALUES
        AND NOT (keyword STREQUAL "INCLUDE_PREFIX" AND empty_prefix))
      message(FATAL_ERROR "thrift_add_library: ${keyword} requires a value")
    endif ()
  endforeach ()
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
  if (DEFINED ARG_OUTPUT_DIRECTORY)
    list(APPEND generate_args OUTPUT_DIRECTORY "${ARG_OUTPUT_DIRECTORY}")
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
  set(thrift_include_dirs
      "$<TARGET_PROPERTY:${includes},INTERFACE_INCLUDE_DIRECTORIES>")
  if (THRIFT_INCLUDE_DIR)
    list(APPEND thrift_include_dirs "${THRIFT_INCLUDE_DIR}")
  endif ()
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
  if (empty_prefix)
    thrift_generate(
      ${name} "${thrift_file}"
      LANGUAGE cpp2
      INCLUDE_DIRECTORIES ${thrift_include_dirs}
      FILE_DEPENDS ${file_depends} ${generate_args}
      INCLUDE_PREFIX "")
  elseif (DEFINED ARG_INCLUDE_PREFIX)
    thrift_generate(
      ${name} "${thrift_file}"
      LANGUAGE cpp2
      INCLUDE_DIRECTORIES ${thrift_include_dirs}
      FILE_DEPENDS ${file_depends} ${generate_args}
      INCLUDE_PREFIX "${ARG_INCLUDE_PREFIX}")
  else ()
    thrift_generate(
      ${name} "${thrift_file}"
      LANGUAGE cpp2
      INCLUDE_DIRECTORIES ${thrift_include_dirs}
      FILE_DEPENDS ${file_depends} ${generate_args})
  endif ()

  get_target_property(output_dir ${name} _THRIFT_OUTPUT_DIRECTORY)
  get_target_property(include_prefix ${name} _THRIFT_INCLUDE_PREFIX)
  get_target_property(headers ${name} _THRIFT_HEADERS)
  # The generated code includes <include_prefix>/gen-cpp2/<header>, so the
  # include directory is OUTPUT_DIRECTORY without INCLUDE_PREFIX.
  set(include_root "${output_dir}")
  if (NOT include_prefix STREQUAL "")
    string(LENGTH "/${include_prefix}" suffix_length)
    string(LENGTH "${output_dir}" length)
    math(EXPR start "${length} - ${suffix_length}")
    set(suffix)
    if (start GREATER_EQUAL 0)
      string(SUBSTRING "${output_dir}" ${start} -1 suffix)
    endif ()
    if (NOT suffix STREQUAL "/${include_prefix}")
      message(
        FATAL_ERROR
          "thrift_add_library: OUTPUT_DIRECTORY ${output_dir} must end with "
          "INCLUDE_PREFIX ${include_prefix}")
    endif ()
    string(SUBSTRING "${output_dir}" 0 ${start} include_root)
  endif ()
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
