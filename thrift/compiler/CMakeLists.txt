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

# The Thrift AST library.
add_library(
  thrift_ast STATIC
  ast/t_primitive_type.cc
  ast/t_const.cc
  ast/t_const_value.cc
  ast/t_container.cc
  ast/t_enum.cc
  ast/t_exception.cc
  ast/t_field.cc
  ast/t_function.cc
  ast/t_interaction.cc
  ast/t_interface.cc
  ast/t_list.cc
  ast/t_named.cc
  ast/t_node.cc
  ast/t_map.cc
  ast/t_package.cc
  ast/t_program.cc
  ast/t_global_scope.cc
  ast/t_set.cc
  ast/t_structured.cc
  ast/t_type.cc
  ast/t_typedef.cc
  ast/t_union.cc
  ast/scope_identifier.cc)
thrift_configure_target(thrift_ast PUBLIC)
target_link_libraries(thrift_ast fmt::fmt Folly::folly)

# The base compiler infrastructure library
add_library(
  thrift_compiler_base STATIC detail/system.cc detail/pluggable_functions.cc
                              diagnostic.cc source_location.cc)
target_link_libraries(thrift_compiler_base fmt::fmt Folly::folly thrift_common)

# The base Thrift compiler library.
add_library(
  thrift_frontend STATIC
  parse/lexer.cc
  parse/lexer.h
  parse/parser.cc
  parse/parser.h
  parse/parse_ast.cc
  parse/parse_ast.h
  parse/token.cc
  parse/token.h
  sema/ast_uri_utils.cc
  sema/check_initializer.cc
  sema/check_map_keys.cc
  sema/explicit_include_validator.cc
  sema/scope_validator.cc
  sema/sema.cc
  sema/sema_context.cc
  sema/schematizer.cc
  sema/standard_validator.cc
  sema/resolution_mismatch.cc)
target_compile_definitions(thrift_frontend PRIVATE THRIFT_OSS)
target_link_libraries(thrift_frontend thrift_compiler_base thrift_ast fmt::fmt
                      Folly::folly OpenSSL::Crypto)

# The Thrift compiler library.
add_library(
  thrift_generator_util STATIC
  generate/cpp/allocator.cc
  generate/cpp/reference_type.cc
  generate/cpp/name_resolver.cc
  generate/cpp/util.cc
  generate/csharp/util.cc
  generate/go/util.cc
  generate/java/util.cc
  generate/python/util.cc
  generate/rust/util.cc
  generate/swift/util.cc)
target_link_libraries(thrift_generator_util thrift_ast thrift_compiler_base
                      fmt::fmt Folly::folly OpenSSL::Crypto)

add_library(
  thrift_whisker STATIC
  whisker/ast.cc
  whisker/dsl.cc
  whisker/detail/string.cc
  whisker/eval_context.cc
  whisker/lexer.cc
  whisker/object.cc
  whisker/parser.cc
  whisker/print_ast.cc
  whisker/render.cc
  whisker/standard_library.cc
  whisker/token.cc)
target_link_libraries(thrift_whisker fmt::fmt Folly::folly thrift_compiler_base)

add_subdirectory(generate)

# Force thrift_generators linking (compiler will optimize it out otherwise).
if (MSVC)
  set(GENERATE_LINKED thrift_generators) # MSVC WHOLEARCHIVE is set after exe.
elseif (APPLE)
  set(GENERATE_LINKED -Wl,-force_load thrift_generators)
else ()
  set(GENERATE_LINKED -Wl,--whole-archive thrift_generators
                      -Wl,--no-whole-archive)
endif ()

# Create the Thrift compiler binary.
add_executable(thrift main.cc compiler.cc)
target_link_libraries(thrift thrift_ast thrift_frontend ${GENERATE_LINKED})
# Force thrift_generators linking (compiler will optimize it out otherwise).
if (MSVC)
  set_target_properties(thrift PROPERTIES LINK_FLAGS
                                          "/WHOLEARCHIVE:thrift_generators")
endif ()

thrift_install_targets(thrift thrift_common)

# Deprecated alias. The link is relative, so the install stays relocatable.
# Windows has no dependable symlink support for an unprivileged install, so
# copy there.
if (WIN32)
  install(
    CODE "
set(_dir \"\$ENV{DESTDIR}\${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_BINDIR}\")
execute_process(
  COMMAND \"\${CMAKE_COMMAND}\" -E copy_if_different
          \"\${_dir}/thrift${CMAKE_EXECUTABLE_SUFFIX}\"
          \"\${_dir}/thrift1${CMAKE_EXECUTABLE_SUFFIX}\")")
else ()
  install(
    CODE "
set(_dir \"\$ENV{DESTDIR}\${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_BINDIR}\")
execute_process(
  COMMAND \"\${CMAKE_COMMAND}\" -E create_symlink thrift \"\${_dir}/thrift1\")")
endif ()
