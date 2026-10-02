add_library(tf_chttp_adapter SHARED
  src/turbo_flow_chttp.c
  src/turbo_flow_chttp_server.c
  src/turbo_flow_chttp_websocket_server.c
  src/turbo_flow_chttp_provider_adapter_internal.h
  include/turbo_flow_chttp.h)
cmake_config_target(
  tf_chttp_adapter
  ALIAS TurboFlow::CHTTPAdapter
  FOLDER "io/chttp"
  EXPORT_NAME CHTTPAdapter)
target_compile_definitions(tf_chttp_adapter PRIVATE TURBO_FLOW_BUILD)
target_include_directories(
  tf_chttp_adapter
  PRIVATE $<TARGET_PROPERTY:xxHash::xxhash,INTERFACE_INCLUDE_DIRECTORIES>
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:include>)
target_link_libraries(tf_chttp_adapter PUBLIC
  TurboFlow::Graph CHttp::Client CHttp::Server)

salts_idl_target(
  TARGET turbo_flow_chttp_provider_config
  IDL "${CMAKE_CURRENT_SOURCE_DIR}/src/turbo_flow_chttp_provider_config.schema"
  ARTIFACT_NAME "chttp_provider_config"
  ARTIFACTS MESSAGE)

add_library(tf_chttp_provider_support STATIC
  src/turbo_flow_chttp_typed_config.c
  src/turbo_flow_chttp_typed_config_internal.h)
set_target_properties(tf_chttp_provider_support PROPERTIES
  FOLDER "io/chttp"
  POSITION_INDEPENDENT_CODE ON
  C_VISIBILITY_PRESET hidden
  VISIBILITY_INLINES_HIDDEN YES)
set_target_properties(${turbo_flow_chttp_provider_config_NATIVE_TARGET} PROPERTIES
  C_VISIBILITY_PRESET hidden
  VISIBILITY_INLINES_HIDDEN YES)
target_include_directories(tf_chttp_provider_support PRIVATE
  include
  src
  "${turbo_flow_chttp_provider_config_GENERATED_DIR}")
target_link_libraries(tf_chttp_provider_support PRIVATE
  tf_chttp_adapter
  TurboFlow::Product
  Salts::UriParser
  ${turbo_flow_chttp_provider_config_NATIVE_TARGET})
add_dependencies(tf_chttp_provider_support
  turbo_flow_chttp_provider_config_idl_codegen)
if(WIN32)
  target_link_libraries(tf_chttp_provider_support PRIVATE ws2_32)
endif()

install(FILES
  include/turbo_flow_chttp.h
  include/turbo_flow_chttp_resource.h
  DESTINATION include)

add_library(tf_chttp_plugin SHARED
  src/turbo_flow_chttp_plugin.c)
cmake_config_target(tf_chttp_plugin FOLDER "io/chttp")
target_compile_definitions(tf_chttp_plugin PRIVATE TURBO_FLOW_PLUGIN_BUILD)
target_include_directories(tf_chttp_plugin PRIVATE
  include
  src
  "${turbo_flow_chttp_provider_config_GENERATED_DIR}")
target_link_libraries(tf_chttp_plugin PRIVATE
  tf_chttp_adapter
  tf_chttp_provider_support
  TurboFlow::Product
  Salts::Plugin
  ${turbo_flow_chttp_provider_config_NATIVE_TARGET})
add_dependencies(tf_chttp_plugin
  turbo_flow_chttp_provider_config_idl_codegen)
set_target_properties(tf_chttp_plugin PROPERTIES C_VISIBILITY_PRESET hidden
                                               VISIBILITY_INLINES_HIDDEN YES)
install(TARGETS tf_chttp_plugin RUNTIME DESTINATION bin LIBRARY DESTINATION lib)
if(WIN32)
  target_link_libraries(tf_chttp_plugin PRIVATE ws2_32)
  install(FILES $<TARGET_RUNTIME_DLLS:tf_chttp_plugin> DESTINATION bin)
endif()

if(BUILD_TESTING)
  add_subdirectory(tests)
endif()
