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

add_executable(thrift_build_templates build_templates.cc)
thrift_configure_target(thrift_build_templates PRIVATE)

file(GLOB_RECURSE TEMPLATE_FILES CONFIGURE_DEPENDS
     RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
     ${CMAKE_CURRENT_SOURCE_DIR}/templates/**/*.whisker)
add_custom_command(
  OUTPUT templates.cc
  COMMAND thrift_build_templates ${CMAKE_CURRENT_SOURCE_DIR}/templates >
          templates.cc
  DEPENDS thrift_build_templates ${TEMPLATE_FILES}
  VERBATIM)

file(GLOB GENERATOR_FILES RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} *.cc)
list(REMOVE_ITEM GENERATOR_FILES build_templates.cc)
list(REMOVE_ITEM GENERATOR_FILES t_ast_generator.cc)
list(REMOVE_ITEM GENERATOR_FILES schema_populator.cc)
list(APPEND GENERATOR_FILES
     ${CMAKE_CURRENT_SOURCE_DIR}/cpp/orderable_type_utils.cc)

add_library(thrift_generators STATIC ${GENERATOR_FILES}
                                     ${CMAKE_CURRENT_BINARY_DIR}/templates.cc)
# Always emit PIC so that thrift_generators can be linked into a
# downstream shared library; the upstream rule only enables PIC when
# building the whole tree shared, which forced consumers that want
# static archives in a shared world to either turn on BUILD_SHARED_LIBS
# (and inherit fbthrift's cross-dylib RTTI problems) or maintain a
# separate patch for this one line.
set_target_properties(thrift_generators PROPERTIES POSITION_INDEPENDENT_CODE ON)
if (MSVC)
  set_target_properties(thrift_generators PROPERTIES COMPILE_FLAGS "/bigobj")
endif ()
target_link_libraries(
  thrift_generators
  thrift_ast
  thrift_compiler_base
  thrift_frontend
  thrift_generator_util
  thrift_whisker
  OpenSSL::Crypto)
