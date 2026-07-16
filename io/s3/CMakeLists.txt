set(TARGET_NAME tf_s3)

file(GLOB TURBO_FLOW_S3_SOURCES CONFIGURE_DEPENDS
     ${CMAKE_CURRENT_SOURCE_DIR}/src/*.c)
set(TURBO_FLOW_S3_HEADERS
    ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_s3.h)

add_library(${TARGET_NAME} SHARED ${TURBO_FLOW_S3_SOURCES}
                                ${TURBO_FLOW_S3_HEADERS})
cmake_config_target(
  ${TARGET_NAME}
  ALIAS TurboFlow::S3
  FOLDER "io/s3"
  EXPORT_NAME S3)
target_compile_definitions(${TARGET_NAME} PRIVATE SHARED_CXX)
target_include_directories(
  ${TARGET_NAME} PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                        $<INSTALL_INTERFACE:include>)
target_link_libraries(
  ${TARGET_NAME}
  PUBLIC TurboFlow::Flow
  PRIVATE tf_executor_common TurboHttp::S3 TurboNet::CoroNet TurboUtils::Core)
install(FILES ${TURBO_FLOW_S3_HEADERS} DESTINATION include)

if(BUILD_TESTS)
  add_subdirectory(tests)
endif()

