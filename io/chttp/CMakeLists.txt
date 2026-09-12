add_library(tf_chttp_adapter SHARED
  src/turbo_flow_chttp.c
  src/turbo_flow_chttp_server.c
  src/turbo_flow_chttp_websocket_server.c
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

install(FILES include/turbo_flow_chttp.h DESTINATION include)

add_library(tf_chttp_plugin SHARED src/turbo_flow_chttp_plugin.c
  src/turbo_flow_chttp_plugin_config.c src/turbo_flow_chttp_plugin_internal.h)
cmake_config_target(tf_chttp_plugin FOLDER "io/chttp")
target_compile_definitions(tf_chttp_plugin PRIVATE TURBO_FLOW_PLUGIN_BUILD)
target_include_directories(tf_chttp_plugin PRIVATE include src)
target_link_libraries(tf_chttp_plugin PRIVATE tf_chttp_adapter TurboFlow::Product Salts::CSTL
                                             Salts::UriParser)
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
