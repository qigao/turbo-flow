add_library(tf_chttp_adapter SHARED
  src/turbo_flow_chttp.c
  include/turbo_flow_chttp.h)
cmake_config_target(
  tf_chttp_adapter
  ALIAS TurboFlow::CHTTPAdapter
  FOLDER "io/chttp"
  EXPORT_NAME CHTTPAdapter)
target_compile_definitions(tf_chttp_adapter PRIVATE TURBO_FLOW_BUILD)
target_include_directories(
  tf_chttp_adapter
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:include>)
target_link_libraries(tf_chttp_adapter PUBLIC TurboFlow::Graph Salts::CHTTP)

install(FILES include/turbo_flow_chttp.h DESTINATION include)

if(BUILD_TESTING)
  add_subdirectory(tests)
endif()
