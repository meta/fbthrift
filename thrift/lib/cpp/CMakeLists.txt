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

if (THRIFT_TESTS OR THRIFT_BENCHMARKS)
  add_subdirectory(util/test)
endif ()

add_library(thrift_core Thrift.cpp ../cpp2/FieldRef.cpp)
thrift_configure_target(thrift_core PUBLIC)
target_link_libraries(thrift_core PUBLIC fmt::fmt Folly::folly)

add_library(
  thrift_concurrency
  concurrency/InitThreadFactory.cpp concurrency/PosixThreadFactory.cpp
  concurrency/ThreadManager.cpp)
thrift_configure_target(thrift_concurrency PUBLIC)
target_link_libraries(thrift_concurrency PUBLIC Folly::folly ${thrift_gflags}
                                                glog::glog)

add_library(thrift_runtime ../cpp2/runtime/Init.cpp)
thrift_configure_target(thrift_runtime PUBLIC)
target_link_libraries(thrift_runtime PUBLIC Folly::folly)

# thrift_transport pulls in rocket/CompressionManager via THeader.cpp, which
# lives in thrift_rpc, and thrift_async and thrift-cpp build on thrift_transport.
# Build them only when the RPC stack is enabled.
if (THRIFT_RPC)
  add_library(
    thrift_transport
    transport/TTransportException.cpp
    transport/TFDTransport.cpp
    transport/THttpTransport.cpp
    transport/THttpClient.cpp
    transport/THttpServer.cpp
    transport/TSocket.cpp
    transport/TBufferTransports.cpp
    transport/THeader.cpp
    transport/TZlibTransport.cpp
    ../cpp2/transport/rocket/framing/Util.cpp
    ../cpp2/Flags.cpp
    ../cpp2/transport/rocket/compression/CompressionAlgorithmSelector.cpp
    ../cpp2/transport/rocket/compression/CompressionManager.cpp
    util/PausableTimer.cpp
    util/THttpParser.cpp
    util/VarintUtils.cpp)
  add_dependencies(thrift_transport thrift_rpc_metadata)
  target_link_libraries(
    thrift_transport
    PUBLIC thrift_common
           thrift_concurrency
           thrift_core
           thrift_rpc_metadata
           Folly::folly
           ZLIB::ZLIB
           ${thrift_openssl_crypto}
           zstd::zstd)

  add_library(
    thrift_async
    ContextStack.cpp
    EventHandlerBase.cpp
    server/TServerObserver.cpp
    ../cpp2/detail/EventHandlerRuntime.cpp
    ../cpp2/async/ClientInterceptorBase.cpp
    ../cpp2/async/InterceptorFlags.cpp
    ../cpp2/async/InterceptorFrameworkMetadata.cpp)
  if (CMAKE_CXX_COMPILER_ID MATCHES "Clang" OR CMAKE_CXX_COMPILER_ID STREQUAL
                                               "GNU")
    target_compile_options(
      thrift_async PUBLIC $<$<COMPILE_LANGUAGE:CXX>:-fsized-deallocation>)
  endif ()
  target_link_libraries(
    thrift_async
    PUBLIC thrift_concurrency
           thrift_runtime
           thrift_transport
           Boost::headers
           Folly::folly
           ${thrift_wangle}
           glog::glog
           ${thrift_openssl_crypto})

  add_library(thrift-cpp INTERFACE)
  target_link_libraries(
    thrift-cpp INTERFACE thrift_async thrift_concurrency thrift_protocol
                         thrift_transport Folly::folly glog::glog)
  add_library(FBThrift::thrift-cpp ALIAS thrift-cpp)
endif () # THRIFT_RPC

if (BUILD_SHARED_LIBS)
  # all but thrift-cpp since it's an interface
  set_target_properties(thrift_core thrift_concurrency
                        PROPERTIES VERSION ${PROJECT_VERSION})
  if (THRIFT_RPC)
    set_target_properties(thrift_async thrift_transport
                          PROPERTIES VERSION ${PROJECT_VERSION})
  endif ()
endif ()

thrift_install_targets(thrift_core thrift_concurrency thrift_runtime)

if (THRIFT_RPC)
  thrift_install_targets(thrift_transport thrift_async)
endif ()
