cmake_add_test(
  test_turbodb_adapter
  SOURCES test_turbodb_adapter.c turbodb_adapter_header_cpp.cpp
  LIBS TurboFlow::TurboDbAdapter Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../include
  FOLDER "io/turbodb/tests")

set_tests_properties(test_turbodb_adapter
                     PROPERTIES LABELS "turbodb-adapter;graph")

cmake_add_test(
  test_turbodb_outbox_source
  SOURCES test_turbodb_outbox_source.c
  LIBS TurboFlow::TurboDbAdapter Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../include
  FOLDER "io/turbodb/tests")

set_tests_properties(test_turbodb_outbox_source
                     PROPERTIES LABELS "turbodb-adapter;graph;outbox")

add_executable(test_turbodb_inbox_crash_helper turbodb_inbox_crash_helper.c)
target_link_libraries(test_turbodb_inbox_crash_helper PRIVATE TurboFlow::TurboDbAdapter)
target_include_directories(test_turbodb_inbox_crash_helper
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
set_target_properties(test_turbodb_inbox_crash_helper
                      PROPERTIES FOLDER "io/turbodb/tests/helpers")

cmake_add_test(
  test_turbodb_inbox
  SOURCES test_turbodb_inbox.c
  LIBS TurboFlow::TurboDbAdapter TurboFlow::ProtocolIngress
       tf_protocol_inbox_schema Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../include
           ${PROJECT_SOURCE_DIR}/ingress/protocol/inbox/include
  FOLDER "io/turbodb/tests")
add_dependencies(test_turbodb_inbox test_turbodb_inbox_crash_helper)
target_compile_definitions(
  test_turbodb_inbox
  PRIVATE TURBO_FLOW_TURBODB_INBOX_CRASH_HELPER="$<TARGET_FILE:test_turbodb_inbox_crash_helper>")

set_tests_properties(test_turbodb_inbox
                     PROPERTIES LABELS "turbodb-adapter;graph;inbox")

cmake_add_test(
  test_turbodb_inbox_source
  SOURCES test_turbodb_inbox_source.c
  LIBS TurboFlow::TurboDbAdapter Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../include
  FOLDER "io/turbodb/tests")

set_tests_properties(test_turbodb_inbox_source
                     PROPERTIES LABELS "turbodb-adapter;graph;inbox")

set(TURBO_FLOW_TURBODB_RESOURCE_FIXTURE_DB
  "${CMAKE_CURRENT_BINARY_DIR}/turbodb-provider-resource.sqlite3")

add_library(test_turbodb_database_resource_fixture SHARED
  turbodb_database_resource_fixture.c)
target_link_libraries(test_turbodb_database_resource_fixture
  PRIVATE TurboFlow::TurboDbAdapter Salts::Plugin)
target_include_directories(test_turbodb_database_resource_fixture
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_compile_definitions(test_turbodb_database_resource_fixture PRIVATE
  FLOW_TURBODB_RESOURCE_FIXTURE_DB="${TURBO_FLOW_TURBODB_RESOURCE_FIXTURE_DB}")
set_target_properties(test_turbodb_database_resource_fixture PROPERTIES
  FOLDER "io/turbodb/tests/fixtures"
  C_VISIBILITY_PRESET hidden
  VISIBILITY_INLINES_HIDDEN YES)

cmake_add_test(test_turbodb_durable_buffer_plugin
  SOURCES test_turbodb_provider_interface.c
  LIBS TurboFlow::Product TurboFlow::TurboDbAdapter
       Salts::Plugin Salts::TinyTest
  INCLUDES
    ${CMAKE_CURRENT_SOURCE_DIR}/../include
    ${PROJECT_SOURCE_DIR}/turbo_flow/src
  FOLDER "io/turbodb/tests")
add_dependencies(test_turbodb_durable_buffer_plugin
  tf_turbodb_plugin
  test_turbodb_database_resource_fixture)
target_compile_definitions(test_turbodb_durable_buffer_plugin PRIVATE
  FLOW_TURBODB_PROVIDER_FIXTURE="$<TARGET_FILE:tf_turbodb_plugin>"
  FLOW_TURBODB_RESOURCE_FIXTURE="$<TARGET_FILE:test_turbodb_database_resource_fixture>"
  FLOW_TURBODB_RESOURCE_FIXTURE_DB="${TURBO_FLOW_TURBODB_RESOURCE_FIXTURE_DB}")
set_tests_properties(test_turbodb_durable_buffer_plugin PROPERTIES
  LABELS "turbodb-adapter;graph;durable;plugin;provider;resource;config")

cmake_add_test(
  test_turbodb_typed_config
  SOURCES
    test_turbodb_typed_config.c
    turbodb_adapter_header_cpp.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/turbo_flow_turbodb_durable_config.c
  LIBS
    TurboFlow::Product
    TurboFlow::TurboDbAdapter
    Salts::TinyTest
    ${turbo_flow_turbodb_provider_config_NATIVE_TARGET}
  INCLUDES
    ${CMAKE_CURRENT_SOURCE_DIR}/../src
    ${turbo_flow_turbodb_provider_config_GENERATED_DIR}
  FOLDER "io/turbodb/tests")
add_dependencies(
  test_turbodb_typed_config
  turbo_flow_turbodb_provider_config_idl_codegen)
set_target_properties(test_turbodb_typed_config PROPERTIES
  C_STANDARD 11 C_STANDARD_REQUIRED ON C_EXTENSIONS OFF
  CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON CXX_EXTENSIONS OFF)
set_tests_properties(test_turbodb_typed_config PROPERTIES
  LABELS "turbodb-adapter;graph;config;databind" TIMEOUT 30)

