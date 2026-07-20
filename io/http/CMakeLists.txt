set(TURBO_FLOW_HTTP_HEADERS
    ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_http.h
    ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_http_acl.h
    ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_http_auth.h
    ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_http_client.h
    ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_http_server.h
    ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_http_types.h)

set(TURBO_FLOW_HTTP_TARGETS)

add_library(tf_http_content STATIC ${CMAKE_CURRENT_SOURCE_DIR}/src/flow_http_content.c)
cmake_config_target(tf_http_content FOLDER "io/http/internal")
set_target_properties(tf_http_content PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(tf_http_content PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(tf_http_content PRIVATE TurboFlow::Flow TurboUtils::Core)

if(TURBO_FLOW_BUILD_HTTP_CLIENT)
  add_library(tf_http_client SHARED
              ${CMAKE_CURRENT_SOURCE_DIR}/src/flow_http_acl.c
              ${CMAKE_CURRENT_SOURCE_DIR}/src/flow_http_acl_internal.h
              ${CMAKE_CURRENT_SOURCE_DIR}/src/flow_http_auth.c
              ${CMAKE_CURRENT_SOURCE_DIR}/src/flow_http_tls_client.c
              ${CMAKE_CURRENT_SOURCE_DIR}/src/flow_http_tls_client.h
              ${CMAKE_CURRENT_SOURCE_DIR}/src/flow_http_client.c
              ${TURBO_FLOW_HTTP_HEADERS})
  cmake_config_target(
    tf_http_client
    ALIAS TurboFlow::HttpClient
    FOLDER "io/http/client"
    EXPORT_NAME HttpClient)
  target_compile_definitions(tf_http_client PRIVATE SHARED_CXX)
  target_include_directories(
    tf_http_client PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                          $<INSTALL_INTERFACE:include>)
  target_link_libraries(
    tf_http_client
    PUBLIC TurboFlow::Flow
    PRIVATE tf_http_content tf_executor_common TurboHttp::HttpClient TurboNet::CoroNet
            TurboUtils::Core TurboUtils::Parser monocypher)
  list(APPEND TURBO_FLOW_HTTP_TARGETS tf_http_client)
endif()

if(TURBO_FLOW_BUILD_HTTP_SERVER)
  add_library(tf_http_server SHARED
              ${CMAKE_CURRENT_SOURCE_DIR}/src/flow_http_server.c
              ${TURBO_FLOW_HTTP_HEADERS})
  cmake_config_target(
    tf_http_server
    ALIAS TurboFlow::HttpServer
    FOLDER "io/http/server"
    EXPORT_NAME HttpServer)
  target_compile_definitions(tf_http_server PRIVATE SHARED_CXX)
  target_include_directories(
    tf_http_server PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                          $<INSTALL_INTERFACE:include>)
  target_link_libraries(
    tf_http_server
    PUBLIC TurboFlow::Flow
    PRIVATE tf_http_content tf_executor_common TurboHttp::Iris TurboNet::CoroNet TurboUtils::Core)
  list(APPEND TURBO_FLOW_HTTP_TARGETS tf_http_server)
endif()

add_library(tf_http INTERFACE)
cmake_config_target(
  tf_http
  ALIAS TurboFlow::Http
  FOLDER "io/http"
  EXPORT_NAME Http)
target_link_libraries(tf_http INTERFACE ${TURBO_FLOW_HTTP_TARGETS})

install(FILES ${TURBO_FLOW_HTTP_HEADERS} DESTINATION include)

if(BUILD_TESTS AND TURBO_FLOW_BUILD_HTTP_CLIENT AND TURBO_FLOW_BUILD_HTTP_SERVER)
  add_subdirectory(tests)
endif()

if(BUILD_BENCHMARKS AND TURBO_FLOW_BUILD_HTTP_CLIENT AND TURBO_FLOW_BUILD_HTTP_SERVER)
  add_subdirectory(benchmarks)
endif()

