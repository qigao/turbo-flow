set(TARGET_NAME tf_email)

file(GLOB TURBO_FLOW_EMAIL_SOURCES CONFIGURE_DEPENDS
     ${CMAKE_CURRENT_SOURCE_DIR}/src/*.c)
set(TURBO_FLOW_EMAIL_HEADERS
    ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_email.h)

add_library(${TARGET_NAME} SHARED ${TURBO_FLOW_EMAIL_SOURCES}
                                  ${TURBO_FLOW_EMAIL_HEADERS})

cmake_config_target(
  ${TARGET_NAME}
  ALIAS
  TurboFlow::Email
  FOLDER
  "io/email"
  EXPORT_NAME
  Email)

target_compile_definitions(${TARGET_NAME} PRIVATE SHARED_CXX)

target_include_directories(
  ${TARGET_NAME} PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                        $<INSTALL_INTERFACE:include>)

target_link_libraries(
  ${TARGET_NAME}
  PUBLIC TurboFlow::Flow TurboNet::CoroNet
  PRIVATE tf_executor_common TurboNet::Email TurboNet::MimeParser)

install(FILES ${TURBO_FLOW_EMAIL_HEADERS} DESTINATION include)

if(BUILD_TESTS)
  add_subdirectory(tests)
endif()

if(BUILD_EXAMPLES)
  add_executable(turbo_flow_smtp4dev_example
                 ${CMAKE_CURRENT_SOURCE_DIR}/examples/smtp4dev_flow.c)
  cmake_config_target(turbo_flow_smtp4dev_example FOLDER "io/email/examples")
  target_link_libraries(turbo_flow_smtp4dev_example PRIVATE ${TARGET_NAME})

  add_executable(turbo_flow_smtp4dev_pop3_example
                 ${CMAKE_CURRENT_SOURCE_DIR}/examples/smtp4dev_pop3_flow.c)
  cmake_config_target(turbo_flow_smtp4dev_pop3_example FOLDER "io/email/examples")
  target_link_libraries(turbo_flow_smtp4dev_pop3_example PRIVATE ${TARGET_NAME})
endif()

