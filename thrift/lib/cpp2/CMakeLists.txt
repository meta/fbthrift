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

# Set the cpp2 directory
set(LIB_CPP2_HOME ${CMAKE_CURRENT_SOURCE_DIR})

if (THRIFT_TESTS)
  add_subdirectory(protocol/test)
endif ()
if (THRIFT_BENCHMARKS)
  add_subdirectory(test)
endif ()

add_library(
  thrift_metadata
  gen/module_types_cpp.cpp
  detail/Enum.cpp
  dynamic/SerializableRecord.cpp
  dynamic/SerializableTypeSystemBuilder.cpp
  dynamic/TypeId.cpp
  dynamic/TypeSystem.cpp
  dynamic/TypeSystemBuilder.cpp
  dynamic/TypeSystemDigest.cpp
  dynamic/TypeSystemTraits.cpp
  runtime/BaseSchemaRegistry.cpp
  runtime/Init.cpp
  schema/detail/Merge.cpp
  ../thrift/detail/protocol.cpp
  ../thrift/detail/TypeIdAdapter.cpp
  ../thrift/detail/TypeSystemAdapter.cpp)
thrift_generate(thrift_metadata ../thrift/metadata.thrift LANGUAGE cpp2)
thrift_generate(thrift_metadata ../thrift/protocol.thrift LANGUAGE cpp2)
thrift_generate(thrift_metadata ../thrift/protocol_detail.thrift LANGUAGE cpp2)
thrift_generate(thrift_metadata ../thrift/type_id.thrift LANGUAGE cpp2
                OPTIONS no_metadata)
thrift_generate(thrift_metadata ../thrift/type_system.thrift LANGUAGE cpp2
                OPTIONS no_metadata)
thrift_generate(thrift_metadata ../thrift/schema.thrift LANGUAGE cpp2
                OPTIONS no_metadata)
thrift_generate(thrift_metadata ../thrift/record.thrift LANGUAGE cpp2
                OPTIONS no_metadata)
thrift_generate(thrift_metadata schema/syntax_graph.thrift LANGUAGE cpp2
                OPTIONS no_metadata)
thrift_generate(thrift_metadata ../thrift/service_catalog.thrift LANGUAGE cpp2
                OPTIONS no_metadata)
# For the headers generated for thrift_type and thrift_type_rep.
add_dependencies(thrift_metadata thrift_type)
target_link_libraries(
  thrift_metadata PUBLIC Folly::folly thrift_protocol thrift_type_rep
                         thrift_type thrift_common)
target_link_libraries(thrift_metadata PUBLIC ${thrift_xxhash})
# SerializableRecord.cpp includes F14Map and instantiates PrimitiveDatum for
# every primitive type; on MSVC this exceeds the default COFF section limit.
if (MSVC)
  target_compile_options(thrift_metadata PRIVATE /bigobj)
endif ()

add_library(thrift_path dynamic/Path.cpp)
add_dependencies(thrift_path thrift_metadata thrift_protocol)
target_link_libraries(thrift_path PUBLIC fmt::fmt Folly::folly thrift_metadata
                                         thrift_protocol)

add_library(
  thrift_dynamic_value
  dynamic/Any.cpp
  dynamic/Binary.cpp
  dynamic/DynamicValue.cpp
  dynamic/List.cpp
  dynamic/Map.cpp
  dynamic/Set.cpp
  dynamic/StableCompare.cpp
  dynamic/String.cpp
  dynamic/Struct.cpp
  dynamic/Union.cpp
  dynamic/ServiceCatalogDigest.cpp
  dynamic/ServiceDescriptorSerialization.cpp
  dynamic/detail/ConcreteMapAny.cpp
  dynamic/detail/ConcreteMapBinary.cpp
  dynamic/detail/ConcreteMapBool.cpp
  dynamic/detail/ConcreteMapDouble.cpp
  dynamic/detail/ConcreteMapFloat.cpp
  dynamic/detail/ConcreteMapI16.cpp
  dynamic/detail/ConcreteMapI32.cpp
  dynamic/detail/ConcreteMapI64.cpp
  dynamic/detail/ConcreteMapI8.cpp
  dynamic/detail/ConcreteMapList.cpp
  dynamic/detail/ConcreteMapMap.cpp
  dynamic/detail/ConcreteMapSet.cpp
  dynamic/detail/ConcreteMapString.cpp
  dynamic/detail/ConcreteMapStruct.cpp
  dynamic/detail/ConcreteMapUnion.cpp
  dynamic/detail/ConcreteSet.cpp
  dynamic/detail/Datum.cpp
  dynamic/detail/DatumHash.cpp
  # ServiceDescriptor and the SyntaxGraph-backed catalog that builds it use
  # DynamicValue for annotations, so they layer above thrift_metadata here.
  # Putting them in thrift_metadata would cycle with thrift_dynamic_value.
  dynamic/AnnotationValue.cpp
  dynamic/ServiceDescriptor.cpp
  schema/detail/SchemaBackedResolver.cpp
  schema/SyntaxGraph.cpp
  runtime/SchemaRegistry.cpp)
add_dependencies(thrift_dynamic_value thrift_metadata thrift_protocol
                 thrift_path)
target_link_libraries(thrift_dynamic_value PUBLIC Folly::folly thrift_metadata
                                                  thrift_protocol thrift_path)
# dynamic/Map.cpp and ConcreteMap*.cpp instantiate F14Map for every value type;
# on MSVC this exceeds the default COFF section limit.
if (MSVC)
  target_compile_options(thrift_dynamic_value PRIVATE /bigobj)
endif ()

add_library(thrift_frozen frozen/Frozen.cpp frozen/FrozenUtil.cpp
                          frozen/schema/MemorySchema.cpp)
thrift_generate(thrift_frozen ../thrift/frozen.thrift LANGUAGE cpp2)
add_dependencies(thrift_frozen thrift_metadata)
target_link_libraries(
  thrift_frozen PUBLIC Folly::folly thrift_metadata thrift_protocol glog::glog
                       ${thrift_gflags})

add_library(thrift_type_rep)
thrift_generate(thrift_type_rep ../thrift/id.thrift LANGUAGE cpp2
                OPTIONS no_metadata)
thrift_generate(thrift_type_rep ../thrift/standard.thrift LANGUAGE cpp2
                OPTIONS json no_metadata)
thrift_generate(thrift_type_rep ../thrift/type_rep.thrift LANGUAGE cpp2
                OPTIONS json no_metadata)
add_dependencies(thrift_type_rep thrift_annotation)
target_link_libraries(thrift_type_rep PUBLIC Folly::folly thrift_annotation)

add_library(
  thrift_type
  ../thrift/TypeToMaskAdapter.cpp
  op/Serializer.cpp
  type/Any.cpp
  type/BaseType.cpp
  type/Protocol.cpp
  type/Type.cpp
  type/TypeRegistry.cpp
  type/UniversalName.cpp)
thrift_generate(thrift_type ../thrift/any_rep.thrift LANGUAGE cpp2
                OPTIONS json no_metadata)
thrift_generate(thrift_type ../thrift/field_mask.thrift LANGUAGE cpp2
                OPTIONS no_metadata)
thrift_generate(thrift_type ../thrift/type.thrift LANGUAGE cpp2
                OPTIONS no_metadata)
add_dependencies(thrift_type thrift_annotation thrift_type_rep thrift_protocol)
target_link_libraries(
  thrift_type PUBLIC thrift_common Folly::folly thrift_annotation
                     thrift_type_rep thrift_protocol ${thrift_openssl_crypto})

# thrift_any_rep sources are now compiled into thrift_type to avoid a circular
# shared library dependency. Keep thrift_any_rep as a thin wrapper so that
# existing consumers (e.g. Haskell FFI) that link -lthriftanyrep keep working.
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/thriftanyrep_stub.cpp" "")
add_library(thrift_any_rep "${CMAKE_CURRENT_BINARY_DIR}/thriftanyrep_stub.cpp")
target_link_libraries(thrift_any_rep PUBLIC thrift_type)

add_library(thrift_rpc_metadata)
thrift_generate(thrift_rpc_metadata ../thrift/RpcMetadata.thrift LANGUAGE cpp2
                OPTIONS json no_metadata)
target_link_libraries(thrift_rpc_metadata PUBLIC Folly::folly thrift_protocol)

add_library(thrift_server_debug_info)
thrift_generate(thrift_server_debug_info ../thrift/serverdbginfo.thrift
                LANGUAGE cpp2 OPTIONS json no_metadata)
thrift_configure_target(thrift_server_debug_info PUBLIC)
target_link_libraries(thrift_server_debug_info PUBLIC Folly::folly)

add_library(
  thrift_protocol
  protocol/Protocol.cpp
  protocol/BinaryProtocol.cpp
  protocol/CompactProtocol.cpp
  protocol/CompactV1Protocol.cpp
  protocol/DebugProtocol.cpp
  protocol/JSONProtocol.cpp
  protocol/JSONProtocolCommon.cpp
  protocol/Serializer.cpp
  protocol/TableBasedSerializer.cpp
  protocol/TableBasedSerializerImpl.cpp
  protocol/TableBasedSerializerJson5.cpp
  protocol/VirtualProtocol.cpp
  # Required by TableBasedSerializerJson5.cpp, which instantiates the JSON5
  # reader/writer.
  protocol/detail/Json5ProtocolReader.cpp
  protocol/detail/Json5ProtocolWriter.cpp
  protocol/detail/JsonReader.cpp
  protocol/detail/JsonWriter.cpp
  protocol/detail/JsonUtils.cpp
  protocol/detail/CompoundTypeTracker.cpp
  ../cpp/protocol/TBase64Utils.cpp
  ../cpp/protocol/TProtocolException.cpp
  ../cpp/protocol/TType.cpp
  ../cpp/util/VarintUtils.cpp
  patch/detail/PatchLogging.cpp
  # Required by protocol/BinaryProtocol.cpp, which instantiates
  # BinaryProtocolReaderBase<io::IOBufChainCursor>.
  IOBufChain.cpp
  IOBufChainCursor.cpp
  # Required by protocol/detail/Json5ProtocolReader.cpp, which calls
  # enum_find<Int>::find_name; the explicit instantiation for `int` only
  # exists in this translation unit.
  detail/Enum.cpp
  # Required by protocol/TableBasedSerializerImpl.h's field-name lookup
  # path (used by the JSON5 protocol), which calls checkFieldIdConflict.
  gen/module_types_cpp.cpp
  pmr/ArenaResource.cpp)
target_link_libraries(thrift_protocol PUBLIC fmt::fmt Folly::folly glog::glog
                                             thrift_core)
# The name an older Thrift installed for thrift::protocol, which
# thrift_generate() links.
thrift_add_legacy_name(protocol thrift_protocol)

if (THRIFT_RPC)
  add_library(
    thrift_rpc
    FieldRef.cpp
    Flags.cpp
    GeneratedCodeHelper.cpp
    TrustedServerException.cpp
    async/AsyncClient.cpp
    async/AsyncProcessor.cpp
    async/AsyncProcessorFactory.cpp
    async/AsyncProcessorHelper.cpp
    async/ClientChannel.cpp
    async/ClientCompressionFlags.cpp
    async/ClientSinkBridge.cpp
    async/ClientStreamBridge.cpp
    async/ClientStreamInterceptorContext.cpp
    async/Cpp2Channel.cpp
    async/DuplexChannel.cpp
    async/FramingHandler.cpp
    async/HeaderChannel.cpp
    async/HeaderChannelTrait.cpp
    async/HeaderClientChannel.cpp
    async/HeaderServerChannel.cpp
    async/HibernatingRequestChannel.cpp
    async/Interaction.cpp
    async/InteractionOverloadPolicy.cpp
    async/InterceptorFlags.cpp
    async/MultiplexAsyncProcessor.cpp
    async/PooledRequestChannel.cpp
    async/ReconnectingRequestChannel.cpp
    async/RequestCallback.cpp
    async/RequestChannel.cpp
    async/ResponseChannel.cpp
    async/RetryingRequestChannel.cpp
    async/RocketClientChannel.cpp
    async/RocketClientChannelBase.cpp
    async/RpcOptions.cpp
    async/RpcTypes.cpp
    async/SecureRocketClientChannel.cpp
    async/ServerBiDiStreamFactory.cpp
    async/ServerGeneratorStreamBridge.cpp
    async/ServerRequestData.cpp
    async/ServerSinkBridge.cpp
    async/ServerSinkFactory.cpp
    async/ServerStreamDetail.cpp
    async/StreamPayload.cpp
    async/processor/AsyncProcessor.cpp
    async/processor/AsyncProcessorFunc.cpp
    async/processor/EventTask.cpp
    async/processor/GeneratedAsyncProcessorBase.cpp
    async/processor/HandlerCallback.cpp
    async/processor/HandlerCallbackBase.cpp
    async/processor/HandlerCallbackOneWay.cpp
    async/processor/RequestParams.cpp
    async/processor/RequestTask.cpp
    async/processor/ServerInterface.cpp
    async/processor/ServerRequest.cpp
    async/processor/ServerRequestHelper.cpp
    async/processor/ServerRequestTask.cpp
    async/processor/ServiceHandlerBase.cpp
    gen/module_constants_cpp.cpp
    gen/module_types_cpp.cpp
    detail/Enum.cpp
    fast_thrift/common/allocator/EvbAllocator.cpp
    fast_thrift/channel_pipeline/PipelineImpl.cpp
    fast_thrift/channel_pipeline/detail/ContextImpl.cpp
    fast_thrift/frame/write/FifoStreamQueue.cpp
    fast_thrift/frame/write/FrameWriter.cpp
    fast_thrift/frame/write/handler/FifoFrameFragmentationHandlerState.cpp
    fast_thrift/frame/write/handler/YieldingSrptFrameFragmentationHandlerState.cpp
    fast_thrift/thrift/FastThriftAdapterBase.cpp
    fast_thrift/thrift/client/ThriftClientChannel.cpp
    fast_thrift/thrift/client/util/RocketFrameDecoder.cpp
    fast_thrift/thrift/server/ThriftServerChannel.cpp
    fast_thrift/transport/TransportHandler.cpp
    logging/ThriftBiDiLog.cpp
    logging/ThriftConnectionLog.cpp
    logging/ThriftSinkLog.cpp
    logging/ThriftStreamLog.cpp
    security/AsyncStopTLS.cpp
    security/FizzPeeker.cpp
    security/SSLUtil.cpp
    security/extensions/ThriftParametersClientExtension.cpp
    security/extensions/ThriftParametersServerExtension.cpp
    security/extensions/Types.cpp
    server/AdaptiveConcurrency.cpp
    server/ConcurrencyControllerBase.cpp
    server/Cpp2ConnContext.cpp
    server/Cpp2Connection.cpp
    server/Cpp2Worker.cpp
    server/CPUConcurrencyController.cpp
    server/DecoratorData.cpp
    server/DecoratorDataRuntime.cpp
    server/DiscriminantLoadSheddingOSS.cpp
    server/LazyDynamicArguments.cpp
    server/LegacyHeaderRoutingHandler.cpp
    server/LoggingEvent.cpp
    server/LoggingEventHelper.cpp
    server/LoggingEventTransportMetadata.cpp
    server/MonitoringMethodNames.cpp
    server/ParallelConcurrencyController.cpp
    server/TokenBucketConcurrencyController.cpp
    server/ServiceInterceptorQualifiedName.cpp
    server/StandardConcurrencyController.cpp
    server/ThreadManagerLoggingWrapper.cpp
    server/RequestDebugLog.cpp
    server/RequestEncryptionState.cpp
    server/RequestPileBase.cpp
    server/ReactiveToggle.cpp
    server/RequestsRegistry.cpp
    server/ResourcePool.cpp
    server/ResourcePoolSet.cpp
    server/ResourcePoolHandle.cpp
    server/RoundRobinRequestPile.cpp
    server/WeightedRequestPileQueue.cpp
    server/ServerAttribute.cpp
    server/ServerFlags.cpp
    server/ServerInstrumentation.cpp
    server/ServiceHealthPoller.cpp
    server/ServiceInterceptorBase.cpp
    server/ServiceInterceptorControl.cpp
    server/ServiceInterceptorOnDrop.cpp
    server/ServiceInterceptorOnReceived.cpp
    server/ThriftCatalogServerInterface.cpp
    server/ThriftProcessor.cpp
    server/ThriftServer.cpp
    server/ThriftServerConfig.cpp
    server/TMConcurrencyController.cpp
    server/peeking/TLSHelper.cpp
    server/metrics/PendingConnectionsMetrics.cpp
    transport/core/RequestStateMachine.cpp
    transport/core/RpcMetadataUtil.cpp
    transport/core/RpcMetadataPlugins.cpp
    transport/core/SendCallbacks.cpp
    transport/core/ThriftClient.cpp
    transport/core/ThriftClientCallback.cpp
    transport/core/ThriftRequest.cpp
    transport/rocket/FdSocket.cpp
    transport/rocket/ChecksumGenerator.cpp
    transport/rocket/Types.cpp
    transport/rocket/compression/CustomCompressorRegistry.cpp
    transport/rocket/client/RequestContext.cpp
    transport/rocket/client/RequestContextQueue.cpp
    transport/rocket/client/RocketClient.cpp
    transport/rocket/client/SecureRocketClient.cpp
    transport/rocket/client/KeepAliveWatcher.cpp
    transport/rocket/client/RocketStreamServerCallback.cpp
    transport/rocket/client/RocketStreamServerCallbackWithChunkTimeout.cpp
    transport/rocket/client/RocketSinkServerCallback.cpp
    transport/rocket/client/RocketBiDiServerCallback.cpp
    transport/rocket/flush/FlushManager.cpp
    transport/rocket/framing/parser/AllocatingParserStrategy.cpp
    transport/rocket/framing/ErrorCode.cpp
    transport/rocket/framing/FrameDataFirstFieldAligner.cpp
    transport/rocket/framing/Frames.cpp
    transport/rocket/framing/Parser.cpp
    transport/rocket/framing/Serializer.cpp
    transport/rocket/framing/Util.cpp
    transport/rocket/payload/DefaultPayloadSerializerStrategy.cpp
    transport/rocket/payload/PayloadSerializer.cpp
    transport/rocket/server/BidirectionalStreamState.cpp
    transport/rocket/server/InteractionOverload.cpp
    transport/rocket/server/RocketRoutingHandler.cpp
    transport/rocket/server/RocketServerConnection.cpp
    transport/rocket/server/RefactoredRocketServerConnection.cpp
    transport/rocket/server/RefactoredThriftRocketServerHandler.cpp
    transport/rocket/server/RocketServerConnectionPlugins.cpp
    transport/rocket/server/RocketServerFrameContext.cpp
    transport/rocket/server/RocketSinkClientCallback.cpp
    transport/rocket/server/RocketServerUtil.cpp
    transport/rocket/server/RocketStreamClientCallback.cpp
    transport/rocket/server/StreamFirstResponseLoggingCallback.cpp
    transport/rocket/server/RocketBiDiClientCallback.cpp
    transport/rocket/server/RocketThriftRequests.cpp
    transport/rocket/server/RocketServerConnectionFactory.cpp
    transport/rocket/server/ThriftRocketServerHandler.cpp
    transport/rocket/server/detail/RequestEncryptionStateDispatch.cpp
    transport/rocket/server/detail/RocketContextUtils.cpp
    transport/rocket/server/detail/RocketErrorHandler.cpp
    transport/rocket/server/detail/RocketRequestHandler.cpp
    transport/rocket/server/detail/RocketRequestOrchestrator.cpp
    transport/rocket/server/detail/RocketRequestProcessor.cpp
    transport/rocket/server/detail/RocketSetupProcessor.cpp
    transport/rocket/server/detail/WriteBatcher.cpp
    util/Checksum.cpp
    util/DebugString.cpp
    util/EmptyAsyncProcessor.cpp
    util/SchemaToMetadata.cpp
    util/ScopedServerInterfaceThread.cpp
    util/ScopedServerThread.cpp)
  thrift_generate(
    thrift_rpc ../thrift/RocketUpgrade.thrift
    LANGUAGE cpp2
    SERVICES RocketUpgrade
    OPTIONS no_metadata)
  thrift_generate(
    thrift_rpc ../thrift/thrift_catalog_service.thrift
    LANGUAGE cpp2
    SERVICES ThriftCatalogService
    OPTIONS no_metadata)
  add_dependencies(
    thrift_rpc
    thrift_rpc_metadata
    thrift_annotation
    thrift_metadata
    thrift_path
    thrift_dynamic_value
    thrift_type_rep
    thrift_type
    thrift_any_rep
    thrift_server_debug_info)

  target_link_libraries(
    thrift_rpc
    PUBLIC fmt::fmt
           thrift_async
           thrift_concurrency
           thrift_transport
           thrift_rpc_metadata
           thrift_annotation
           thrift_metadata
           thrift_path
           thrift_dynamic_value
           thrift_frozen
           thrift_protocol
           thrift_type_rep
           thrift_type
           thrift_any_rep
           thrift_server_debug_info
           ${thrift_xxhash})

  add_library(thrift_rpc_quic server/ThriftQuicServer.cpp)
  add_dependencies(thrift_rpc_quic thrift_rpc mvfst::mvfst_server_async_tran)
  target_link_libraries(thrift_rpc_quic PUBLIC thrift_rpc
                                               ${thrift_mvfst_server})
  thrift_add_alias(thrift_rpc_quic)
endif () # THRIFT_RPC

if (BUILD_SHARED_LIBS)
  set_target_properties(
    thrift_rpc_metadata
    thrift_annotation
    thrift_metadata
    thrift_path
    thrift_dynamic_value
    thrift_frozen
    thrift_protocol
    thrift_type_rep
    thrift_type
    thrift_any_rep
    thrift_server_debug_info
    PROPERTIES VERSION ${PROJECT_VERSION})
  if (THRIFT_RPC)
    set_target_properties(thrift_rpc PROPERTIES VERSION ${PROJECT_VERSION})
  endif ()
endif ()

thrift_install_targets(
  thrift_rpc_metadata
  thrift_metadata
  thrift_path
  thrift_dynamic_value
  thrift_frozen
  thrift_type_rep
  thrift_type
  thrift_any_rep
  thrift_protocol
  thrift_server_debug_info)
if (THRIFT_RPC)
  thrift_install_targets(thrift_rpc)
endif ()
