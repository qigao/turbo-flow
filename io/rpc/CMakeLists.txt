set(TARGET_NAME tf_rpc)

file(GLOB TURBO_FLOW_RPC_SOURCES CONFIGURE_DEPENDS
     ${CMAKE_CURRENT_SOURCE_DIR}/src/*.c)
set(TURBO_FLOW_RPC_HEADERS
    ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_rpc.h)

add_library(${TARGET_NAME} SHARED ${TURBO_FLOW_RPC_SOURCES}
                                  ${TURBO_FLOW_RPC_HEADERS})
cmake_config_target(
  ${TARGET_NAME}
  ALIAS TurboFlow::RPC
  FOLDER "io/rpc"
  EXPORT_NAME RPC)
target_compile_definitions(${TARGET_NAME} PRIVATE SHARED_CXX)
target_include_directories(
  ${TARGET_NAME} PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                        $<INSTALL_INTERFACE:include>)
target_link_libraries(
  ${TARGET_NAME}
  PUBLIC TurboFlow::Flow
  PRIVATE TurboHttp::TurboHttp TurboHttp::HttpClient TurboHttp::Iris
          tf_executor_common TurboNet::CoroNet TurboUtils::Core)
install(FILES ${TURBO_FLOW_RPC_HEADERS} DESTINATION include)

if(BUILD_TESTS)
  add_subdirectory(tests)
endif()

