add_library(tf_cnet_adapter SHARED
  src/turbo_flow_cnet_datagram_sink.c
  src/turbo_flow_cnet_stream_sink.c
  src/turbo_flow_cnet_listener_source.c
  src/turbo_flow_cnet_packet_sink.c
  src/turbo_flow_cnet_packet_source.c
  src/turbo_flow_cnet_stream_source.c
  include/turbo_flow_cnet.h)
cmake_config_target(
  tf_cnet_adapter
  ALIAS TurboFlow::CNetAdapter
  FOLDER "io/cnet"
  EXPORT_NAME CNetAdapter)
target_compile_definitions(tf_cnet_adapter PRIVATE TURBO_FLOW_BUILD)
target_include_directories(
  tf_cnet_adapter
  PRIVATE $<TARGET_PROPERTY:xxHash::xxhash,INTERFACE_INCLUDE_DIRECTORIES>
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:include>)
target_link_libraries(tf_cnet_adapter PUBLIC TurboFlow::Graph Salts::CNet Salts::CFlow Salts::CSTL)

salts_idl_target(
  TARGET turbo_flow_cnet_provider_config
  IDL "${CMAKE_CURRENT_SOURCE_DIR}/src/turbo_flow_cnet_provider_config.schema"
  ARTIFACT_NAME "cnet_provider_config"
  ARTIFACTS MESSAGE)
set_target_properties(${turbo_flow_cnet_provider_config_NATIVE_TARGET} PROPERTIES
  C_VISIBILITY_PRESET hidden
  VISIBILITY_INLINES_HIDDEN YES)

install(FILES
  include/turbo_flow_cnet.h
  include/turbo_flow_cnet_resource.h
  DESTINATION include)

add_library(tf_cnet_provider_support STATIC
  src/turbo_flow_cnet_typed_config.c
  src/turbo_flow_cnet_typed_config_internal.h)
set_target_properties(tf_cnet_provider_support PROPERTIES
  FOLDER "io/cnet"
  POSITION_INDEPENDENT_CODE ON
  C_VISIBILITY_PRESET hidden
  VISIBILITY_INLINES_HIDDEN YES)
target_include_directories(tf_cnet_provider_support PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  ${CMAKE_CURRENT_SOURCE_DIR}/src
  "${turbo_flow_cnet_provider_config_GENERATED_DIR}")
target_link_libraries(tf_cnet_provider_support PRIVATE
  tf_cnet_adapter
  TurboFlow::Product
  Salts::UriParser
  ${turbo_flow_cnet_provider_config_NATIVE_TARGET})
add_dependencies(tf_cnet_provider_support
  turbo_flow_cnet_provider_config_idl_codegen)

add_library(tf_cnet_plugin SHARED
  src/turbo_flow_cnet_plugin.c
  src/turbo_flow_cnet_provider_adapter_internal.h)
cmake_config_target(tf_cnet_plugin FOLDER "io/cnet")
target_compile_definitions(tf_cnet_plugin PRIVATE TURBO_FLOW_PLUGIN_BUILD)
target_include_directories(tf_cnet_plugin PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  ${CMAKE_CURRENT_SOURCE_DIR}/src
  "${turbo_flow_cnet_provider_config_GENERATED_DIR}")
target_link_libraries(tf_cnet_plugin PRIVATE
  tf_cnet_adapter
  tf_cnet_provider_support
  TurboFlow::Product
  Salts::Plugin
  ${turbo_flow_cnet_provider_config_NATIVE_TARGET})
add_dependencies(tf_cnet_plugin
  turbo_flow_cnet_provider_config_idl_codegen)
if(WIN32)
  # TLS is supplied by the linked Salts::CNet runtime, not by a second
  # OpenSSL/BoringSSL ABI loaded directly by this protocol provider.
  target_link_libraries(tf_cnet_plugin PRIVATE ws2_32)
endif()
set_target_properties(tf_cnet_plugin PROPERTIES C_VISIBILITY_PRESET hidden
                                               VISIBILITY_INLINES_HIDDEN YES)
install(TARGETS tf_cnet_plugin RUNTIME DESTINATION bin LIBRARY DESTINATION lib)
if(WIN32)
  # Bundle the runtime DLLs that CMake resolves from canonical imported SDK
  # targets. Never copy standalone ssl.dll/crypto.dll or reconstruct an old ABI.
  install(FILES $<TARGET_RUNTIME_DLLS:tf_cnet_plugin> DESTINATION bin)
endif()

if(BUILD_TESTING)
  add_subdirectory(tests)
endif()

if(BUILD_TESTING AND BUILD_BENCHMARKS)
  add_subdirectory(benchmarks)
endif()
