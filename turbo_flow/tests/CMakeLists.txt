cmake_add_test(
  test_turbo_flow
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_turbo_flow.c
  LIBS TurboFlow::Flow Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../src
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_resource_document
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_resource_document.c
  LIBS TurboFlow::Flow Salts::DataBind Salts::TinyTest
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_expr
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_expr.c
  LIBS TurboFlow::Flow Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../src
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_control
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_control.c
  LIBS TurboFlow::Flow Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../src
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_policy
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_policy.c
  LIBS TurboFlow::Flow Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../src
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_rulesforge
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_rulesforge.c
  LIBS TurboFlow::Flow Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../src
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_security
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_security.c
  LIBS TurboFlow::Flow Salts::TinyTest
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_domain
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_domain.c
  LIBS TurboFlow::Flow Salts::TinyTest
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_keyed_state
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_keyed_state.c
  LIBS TurboFlow::Flow Salts::TinyTest
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_discovery
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_discovery.c
  LIBS TurboFlow::Flow Salts::TinyTest
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_config
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_config.c
  LIBS TurboFlow::Product Salts::TinyTest
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_compat_boundary
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_compat_boundary.c
  LIBS TurboFlow::Flow Salts::TinyTest
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_resolved_config_boundary
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_resolved_config_boundary.c
  LIBS TurboFlow::Config Salts::TinyTest
  FOLDER "turbo_flow/tests")

set_tests_properties(
  test_turbo_flow test_flow_resource_document test_flow_config test_flow_compat_boundary
  test_resolved_config_boundary
  PROPERTIES LABELS "graph;core")

if(WIN32)
  set_tests_properties(
    test_flow_config test_flow_compat_boundary test_resolved_config_boundary
    PROPERTIES ENVIRONMENT_MODIFICATION
               "PATH=path_list_prepend:$<TARGET_FILE_DIR:Salts::Core>;PATH=path_list_prepend:$<TARGET_FILE_DIR:Salts::DataBind>")
endif()
