set(TARGET_NAME tf_socket)

file(GLOB TURBO_FLOW_SOCKET_SOURCES CONFIGURE_DEPENDS
     ${CMAKE_CURRENT_SOURCE_DIR}/src/*.c)
set(TURBO_FLOW_SOCKET_HEADERS
    ${CMAKE_CURRENT_SOURCE_DIR}/include/socket.h)

add_library(${TARGET_NAME} SHARED ${TURBO_FLOW_SOCKET_SOURCES}
                                  ${TURBO_FLOW_SOCKET_HEADERS})

cmake_config_target(
  ${TARGET_NAME}
  ALIAS
  TurboFlow::Socket
  FOLDER
  "io/socket"
  EXPORT_NAME
  Socket)

target_compile_definitions(${TARGET_NAME} PRIVATE SHARED_CXX)

target_include_directories(
  ${TARGET_NAME} PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/io/common/include>
                        $<INSTALL_INTERFACE:include>)

target_link_libraries(
  ${TARGET_NAME} PUBLIC TurboFlow::Flow TurboNet::CoroNet TurboUtils::Core
                        TurboUtils::DataBind
                 PRIVATE tf_executor_common)

install(FILES ${TURBO_FLOW_SOCKET_HEADERS} DESTINATION include)

if(BUILD_TESTS)
  add_subdirectory(tests)
endif()

