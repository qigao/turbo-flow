cmake_add_test(
  test_turbo_flow
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_turbo_flow.c
  LIBS TurboFlow::Flow TurboUtils::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../src
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_resource_document
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_resource_document.c
  LIBS TurboFlow::Flow TurboUtils::DataBind TurboUtils::TinyTest
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_expr
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_expr.c
  LIBS TurboFlow::Flow TurboUtils::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../src
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_control
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_control.c
  LIBS TurboFlow::Flow TurboUtils::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../src
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_policy
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_policy.c
  LIBS TurboFlow::Flow TurboUtils::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../src
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_security
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_security.c
  LIBS TurboFlow::Flow TurboUtils::TinyTest
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_domain
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_domain.c
  LIBS TurboFlow::Flow TurboUtils::TinyTest
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_keyed_state
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_keyed_state.c
  LIBS TurboFlow::Flow TurboUtils::TinyTest
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_protocol
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_protocol.c
  LIBS TurboFlow::Flow TurboUtils::TinyTest
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_discovery
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_discovery.c
  LIBS TurboFlow::Flow TurboUtils::TinyTest
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_config
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_config.c
  LIBS TurboFlow::Flow TurboUtils::TinyTest
  FOLDER "turbo_flow/tests")

set_tests_properties(
  test_turbo_flow test_flow_resource_document test_flow_config
  PROPERTIES LABELS "fmq-release;fmq-chaos")

if(WIN32)
  set_tests_properties(
    test_flow_config
    PROPERTIES ENVIRONMENT_MODIFICATION
               "PATH=path_list_prepend:$<TARGET_FILE_DIR:TurboUtils::Core>;PATH=path_list_prepend:$<TARGET_FILE_DIR:TurboUtils::Parser>")
endif()
