add_executable(test_flow_plugin_operation test_flow_plugin_operation.c operation_schema_fixture.c)
target_link_libraries(test_flow_plugin_operation PRIVATE TurboFlow::PluginHost Salts::TinyTest)
set_target_properties(test_flow_plugin_operation PROPERTIES FOLDER "turbo_flow/tests")
add_test(NAME test_flow_plugin_operation COMMAND test_flow_plugin_operation)
add_executable(test_flow_plugin_result_domain test_flow_plugin_result_domain.c)
target_link_libraries(test_flow_plugin_result_domain PRIVATE TurboFlow::PluginHost Salts::TinyTest)
set_target_properties(test_flow_plugin_result_domain PROPERTIES FOLDER "turbo_flow/tests")
add_test(NAME test_flow_plugin_result_domain COMMAND test_flow_plugin_result_domain)
foreach(_operation_mode IN ITEMS OK DUPLICATE DUPLICATE_CHANGED TWO_VERSIONS NO_SCHEMA SWALLOW
    EXECUTE_ALIAS EXECUTE_FAIL_ALIAS BAD_SHAPE BAD_ABI SHORT LONG BAD_VTABLE NO_GUARANTEE
    BAD_EFFECT BAD_PERMISSION DUP_PERMISSION PERMISSION BAD_LIMIT MISSING_OUTPUT MEMORY_OVERFLOW OWNER_FAIL TWO_NAMES)
  add_library(test_flow_operation_fixture_${_operation_mode} SHARED plugin_operation_fixture.c)
  target_link_libraries(test_flow_operation_fixture_${_operation_mode} PRIVATE TurboFlow::PluginHost)
  target_compile_definitions(test_flow_operation_fixture_${_operation_mode} PRIVATE
    TURBO_FLOW_PLUGIN_BUILD FLOW_OPERATION_MODE=OP_FIXTURE_${_operation_mode})
  set_target_properties(test_flow_operation_fixture_${_operation_mode} PROPERTIES
    FOLDER "turbo_flow/tests/plugin-fixtures")
  target_compile_definitions(test_flow_plugin_operation PRIVATE
    FLOW_OPERATION_${_operation_mode}="$<TARGET_FILE:test_flow_operation_fixture_${_operation_mode}>")
  add_dependencies(test_flow_plugin_operation test_flow_operation_fixture_${_operation_mode})
endforeach()
target_compile_definitions(test_flow_operation_fixture_NO_SCHEMA PRIVATE FLOW_OPERATION_ID="fixture.other")
add_executable(test_flow_plugin_operation_runtime test_flow_plugin_operation_runtime.c)
target_link_libraries(test_flow_plugin_operation_runtime PRIVATE TurboFlow::PluginHost Salts::TinyTest)
target_compile_definitions(test_flow_plugin_operation_runtime PRIVATE
  FLOW_OPERATION_OK="$<TARGET_FILE:test_flow_operation_fixture_OK>")
add_dependencies(test_flow_plugin_operation_runtime test_flow_operation_fixture_OK)
foreach(_operation_mode IN ITEMS EXECUTE_ALIAS EXECUTE_FAIL_ALIAS MEMORY_OVERFLOW OWNER_FAIL TWO_NAMES)
  target_compile_definitions(test_flow_plugin_operation_runtime PRIVATE
    FLOW_OPERATION_${_operation_mode}="$<TARGET_FILE:test_flow_operation_fixture_${_operation_mode}>")
  add_dependencies(test_flow_plugin_operation_runtime test_flow_operation_fixture_${_operation_mode})
endforeach()
set_target_properties(test_flow_plugin_operation_runtime PROPERTIES FOLDER "turbo_flow/tests")
add_test(NAME test_flow_plugin_operation_runtime COMMAND test_flow_plugin_operation_runtime)
set_tests_properties(test_flow_plugin_operation test_flow_plugin_result_domain test_flow_plugin_operation_runtime
  PROPERTIES LABELS "graph;plugin;abi3" TIMEOUT 30)
add_library(test_flow_plugin_projection_fixture SHARED plugin_projection_fixture.c)
add_executable(test_flow_data_schema test_flow_data_schema.c operation_schema_fixture.c)
target_link_libraries(test_flow_data_schema PRIVATE TurboFlow::Graph Salts::TinyTest)
set_target_properties(test_flow_data_schema PROPERTIES FOLDER "turbo_flow/tests")
add_test(NAME test_flow_data_schema COMMAND test_flow_data_schema)
add_executable(test_flow_operation_result test_flow_operation_result.c)
target_link_libraries(test_flow_operation_result PRIVATE TurboFlow::Graph Salts::TinyTest)
set_target_properties(test_flow_operation_result PROPERTIES FOLDER "turbo_flow/tests")
add_test(NAME test_flow_operation_result COMMAND test_flow_operation_result)
set_tests_properties(test_flow_data_schema test_flow_operation_result
  PROPERTIES LABELS "graph;core" TIMEOUT 30)
target_link_libraries(test_flow_plugin_projection_fixture PRIVATE TurboFlow::Product)
target_compile_definitions(test_flow_plugin_projection_fixture PRIVATE TURBO_FLOW_PLUGIN_BUILD)
set_target_properties(test_flow_plugin_projection_fixture PROPERTIES
  C_VISIBILITY_PRESET hidden VISIBILITY_INLINES_HIDDEN YES FOLDER "turbo_flow/tests/plugin-fixtures")
cmake_add_test(test_flow_plugin_projection
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_plugin_projection.c
          ${CMAKE_CURRENT_SOURCE_DIR}/plugin_operation_header_cpp.cpp
  LIBS TurboFlow::PluginHost Salts::TinyTest FOLDER "turbo_flow/tests")
target_compile_definitions(test_flow_plugin_projection PRIVATE
  FLOW_PROJECTION_FIXTURE="$<TARGET_FILE:test_flow_plugin_projection_fixture>"
  FLOW_PROJECTION_GENERATION_FIXTURE="$<TARGET_FILE:test_flow_plugin_generation_success>")
add_dependencies(test_flow_plugin_projection test_flow_plugin_projection_fixture test_flow_plugin_generation_success)
set_tests_properties(test_flow_plugin_projection PROPERTIES LABELS "plugin;graph" TIMEOUT 30)

cmake_add_test(
  test_flow_projection_owner
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_projection_owner.c
          ${CMAKE_CURRENT_SOURCE_DIR}/projection_header_cpp.cpp
  LIBS TurboFlow::Graph Salts::TinyTest
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_projection_owner_fault
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_projection_owner_fault.c
  LIBS TurboFlow::Graph Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../src
  FOLDER "turbo_flow/tests")
cmake_add_test(
  test_flow_resource_command_fault
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_resource_command_fault.c
  LIBS TurboFlow::Graph Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../src
  FOLDER "turbo_flow/tests")
set_tests_properties(test_flow_projection_owner test_flow_projection_owner_fault
  PROPERTIES LABELS "graph;core" TIMEOUT 30)

cmake_add_test(
  test_turbo_flow
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_turbo_flow.c
          ${CMAKE_CURRENT_SOURCE_DIR}/flow_plan_type_probe.c
  LIBS TurboFlow::Graph Salts::CFlow Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../src
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_durable_buffer
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_durable_buffer.c
  LIBS TurboFlow::Graph Salts::TinyTest
  FOLDER "turbo_flow/tests")
set_tests_properties(test_flow_durable_buffer PROPERTIES LABELS "graph;core" TIMEOUT 30)
cmake_add_test(
  test_flow_durable_admission
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_durable_admission.c
  LIBS TurboFlow::Graph Salts::TinyTest
  FOLDER "turbo_flow/tests")
set_tests_properties(test_flow_durable_admission PROPERTIES LABELS "graph;core" TIMEOUT 30)


cmake_add_test(
  test_flow_run
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_run.c
          ${CMAKE_CURRENT_SOURCE_DIR}/flow_run_header_cpp.cpp
  LIBS TurboFlow::Graph Salts::TinyTest
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_async_terminal
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_async_terminal.c
  LIBS TurboFlow::Graph Salts::TinyTest
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_async_emit
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_async_emit.c
  LIBS TurboFlow::Graph Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../src
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_resource_document
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_resource_document.c
  LIBS TurboFlow::Graph Salts::DataBind Salts::TinyTest
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_managed_boundary
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_managed_boundary.c
  LIBS TurboFlow::Graph Salts::TinyTest
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_managed_async_terminal
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_managed_async_terminal.c
  LIBS TurboFlow::Graph Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../src
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_managed_source
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_managed_source.c
  LIBS TurboFlow::Graph Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../src
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_inbox
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_inbox.c
  LIBS TurboFlow::Graph Salts::TinyTest
  FOLDER "turbo_flow/tests")
set_tests_properties(test_flow_inbox PROPERTIES LABELS "graph;core;inbox" TIMEOUT 30)

cmake_add_test(
  test_flow_inbox_source
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_inbox_source.c
          ${CMAKE_CURRENT_SOURCE_DIR}/inbox_source_header_cpp.cpp
  LIBS TurboFlow::Graph Salts::TinyTest
  FOLDER "turbo_flow/tests")
target_include_directories(test_flow_inbox_source PRIVATE ${PROJECT_SOURCE_DIR}/tests)
set_tests_properties(test_flow_inbox_source PROPERTIES LABELS "graph;core;inbox" TIMEOUT 30)

cmake_add_test(
  test_flow_inbox_driver
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_inbox_driver.c
  LIBS TurboFlow::Graph Salts::TinyTest
  FOLDER "turbo_flow/tests")
set_tests_properties(test_flow_inbox_driver PROPERTIES LABELS "graph;core;inbox" TIMEOUT 30)

cmake_add_test(
  test_flow_expr
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_expr.c
  LIBS TurboFlow::Graph Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../src
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_control
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_control.c
  LIBS TurboFlow::Graph Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../src
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_policy
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_policy.c
  LIBS TurboFlow::Graph Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../src
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_rulesforge
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_rulesforge.c
  LIBS TurboFlow::Graph Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../src
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_security
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_security.c
  LIBS TurboFlow::Graph Salts::TinyTest
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_domain
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_domain.c
  LIBS TurboFlow::Graph Salts::TinyTest
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_keyed_state
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_keyed_state.c
  LIBS TurboFlow::Graph Salts::TinyTest
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_discovery
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_discovery.c
  LIBS TurboFlow::Graph Salts::TinyTest
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_config
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_config.c
  LIBS TurboFlow::Product Salts::TinyTest
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_operation_binding_config
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_operation_binding_config.c
  LIBS TurboFlow::Config Salts::TinyTest
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_resolved_config_boundary
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_resolved_config_boundary.c
  LIBS TurboFlow::Config Salts::TinyTest
  FOLDER "turbo_flow/tests")

function(turbo_flow_add_plugin_fixture target_name)
  add_library(${target_name} SHARED plugin_fixture.c)
  target_link_libraries(${target_name} PRIVATE TurboFlow::Product)
  target_compile_definitions(${target_name} PRIVATE TURBO_FLOW_PLUGIN_BUILD)
  set_target_properties(
    ${target_name}
    PROPERTIES C_VISIBILITY_PRESET hidden
               VISIBILITY_INLINES_HIDDEN YES
               FOLDER "turbo_flow/tests/plugin-fixtures")
  if(ARGN)
    target_compile_definitions(${target_name} PRIVATE ${ARGN})
  endif()
endfunction()

function(turbo_flow_add_schema_fixture target_name)
  add_library(${target_name} SHARED plugin_schema_fixture.c)
  target_link_libraries(${target_name} PRIVATE TurboFlow::PluginHost Salts::CMeta)
  target_compile_definitions(${target_name} PRIVATE TURBO_FLOW_PLUGIN_BUILD ${ARGN})
  set_target_properties(${target_name} PROPERTIES C_VISIBILITY_PRESET hidden VISIBILITY_INLINES_HIDDEN YES FOLDER "turbo_flow/tests/plugin-fixtures")
endfunction()

turbo_flow_add_schema_fixture(test_flow_plugin_schema_good)
turbo_flow_add_schema_fixture(test_flow_plugin_schema_second FLOW_SCHEMA_FIXTURE_ID="fixture.schema.second" FLOW_SCHEMA_STABLE_ID="fixture.schema.long")
turbo_flow_add_schema_fixture(test_flow_plugin_schema_duplicate FLOW_SCHEMA_FIXTURE_ID="fixture.schema.duplicate" FLOW_SCHEMA_MODE=1)
turbo_flow_add_schema_fixture(test_flow_plugin_schema_bad_version FLOW_SCHEMA_FIXTURE_ID="fixture.schema.bad-version" FLOW_SCHEMA_MODE=2)
turbo_flow_add_schema_fixture(test_flow_plugin_schema_bad_descriptor FLOW_SCHEMA_FIXTURE_ID="fixture.schema.bad-descriptor" FLOW_SCHEMA_MODE=3)
turbo_flow_add_schema_fixture(test_flow_plugin_schema_swallow FLOW_SCHEMA_FIXTURE_ID="fixture.schema.swallow" FLOW_SCHEMA_MODE=4)
turbo_flow_add_schema_fixture(test_flow_plugin_schema_old_abi FLOW_SCHEMA_FIXTURE_ID="fixture.schema.old-abi" FLOW_SCHEMA_ABI_MAJOR=1 FLOW_SCHEMA_ABI_MINOR=4)
turbo_flow_add_schema_fixture(test_flow_plugin_schema_bad_abi FLOW_SCHEMA_FIXTURE_ID="fixture.schema.bad-abi" FLOW_SCHEMA_DESCRIPTOR_MINOR=99)
turbo_flow_add_schema_fixture(test_flow_plugin_schema_old_wrapper FLOW_SCHEMA_FIXTURE_ID="fixture.schema.old-wrapper" FLOW_SCHEMA_DESCRIPTOR_MAJOR=1 FLOW_SCHEMA_DESCRIPTOR_MINOR=4)
turbo_flow_add_schema_fixture(test_flow_plugin_schema_bad_id FLOW_SCHEMA_FIXTURE_ID="fixture.schema.bad-id" FLOW_SCHEMA_MODE=5)
turbo_flow_add_schema_fixture(test_flow_plugin_schema_long_id FLOW_SCHEMA_FIXTURE_ID="fixture.schema.long-id" FLOW_SCHEMA_MODE=6)
turbo_flow_add_schema_fixture(test_flow_plugin_schema_version_two FLOW_SCHEMA_FIXTURE_ID="fixture.schema.version-two" FLOW_SCHEMA_VERSION=2)

cmake_add_test(test_flow_plugin_schema SOURCES test_flow_plugin_schema.c plugin_operation_header_cpp.cpp LIBS TurboFlow::PluginHost Salts::CMeta Salts::TinyTest FOLDER "turbo_flow/tests")
target_compile_definitions(test_flow_plugin_schema PRIVATE
  FLOW_SCHEMA_GOOD="$<TARGET_FILE:test_flow_plugin_schema_good>"
  FLOW_SCHEMA_SECOND="$<TARGET_FILE:test_flow_plugin_schema_second>"
  FLOW_SCHEMA_DUPLICATE="$<TARGET_FILE:test_flow_plugin_schema_duplicate>"
  FLOW_SCHEMA_BAD_VERSION="$<TARGET_FILE:test_flow_plugin_schema_bad_version>"
  FLOW_SCHEMA_BAD_DESCRIPTOR="$<TARGET_FILE:test_flow_plugin_schema_bad_descriptor>"
  FLOW_SCHEMA_SWALLOW="$<TARGET_FILE:test_flow_plugin_schema_swallow>"
  FLOW_SCHEMA_OLD_ABI="$<TARGET_FILE:test_flow_plugin_schema_old_abi>"
  FLOW_SCHEMA_BAD_ABI="$<TARGET_FILE:test_flow_plugin_schema_bad_abi>"
  FLOW_SCHEMA_OLD_WRAPPER="$<TARGET_FILE:test_flow_plugin_schema_old_wrapper>"
  FLOW_SCHEMA_BAD_ID="$<TARGET_FILE:test_flow_plugin_schema_bad_id>"
  FLOW_SCHEMA_LONG_ID="$<TARGET_FILE:test_flow_plugin_schema_long_id>"
  FLOW_SCHEMA_VERSION_TWO="$<TARGET_FILE:test_flow_plugin_schema_version_two>")
add_dependencies(test_flow_plugin_schema test_flow_plugin_schema_good test_flow_plugin_schema_second test_flow_plugin_schema_duplicate test_flow_plugin_schema_bad_version test_flow_plugin_schema_bad_descriptor test_flow_plugin_schema_swallow test_flow_plugin_schema_old_abi test_flow_plugin_schema_bad_abi test_flow_plugin_schema_old_wrapper test_flow_plugin_schema_bad_id test_flow_plugin_schema_long_id test_flow_plugin_schema_version_two)

turbo_flow_add_plugin_fixture(test_flow_plugin_good_one)
turbo_flow_add_plugin_fixture(
  test_flow_plugin_good_two
  FLOW_PLUGIN_FIXTURE_ID="fixture.two"
  FLOW_PLUGIN_FIXTURE_ADAPTER_KIND="fixture.adapter.two"
  FLOW_PLUGIN_FIXTURE_RESOURCE_KIND="fixture.resource.two")
turbo_flow_add_plugin_fixture(
  test_flow_plugin_duplicate_id
  FLOW_PLUGIN_FIXTURE_ID="fixture.one"
  FLOW_PLUGIN_FIXTURE_ADAPTER_KIND="fixture.adapter.duplicate-id"
  FLOW_PLUGIN_FIXTURE_RESOURCE_KIND="fixture.resource.duplicate-id")
turbo_flow_add_plugin_fixture(
  test_flow_plugin_duplicate_kind
  FLOW_PLUGIN_FIXTURE_ID="fixture.duplicate-kind"
  FLOW_PLUGIN_FIXTURE_ADAPTER_KIND="fixture.adapter.one"
  FLOW_PLUGIN_FIXTURE_RESOURCE_KIND="fixture.resource.duplicate-kind")
turbo_flow_add_plugin_fixture(
  test_flow_plugin_duplicate_resource_kind
  FLOW_PLUGIN_FIXTURE_ID="fixture.duplicate-resource-kind"
  FLOW_PLUGIN_FIXTURE_ADAPTER_KIND="fixture.adapter.duplicate-resource-kind"
  FLOW_PLUGIN_FIXTURE_RESOURCE_KIND="fixture.resource.one")
turbo_flow_add_plugin_fixture(
  test_flow_plugin_register_fail
  FLOW_PLUGIN_FIXTURE_ID="fixture.register-fail"
  FLOW_PLUGIN_FIXTURE_ADAPTER_KIND="fixture.adapter.register-fail"
  FLOW_PLUGIN_FIXTURE_RESOURCE_KIND="fixture.resource.register-fail"
  FLOW_PLUGIN_FIXTURE_MODE=1)
turbo_flow_add_plugin_fixture(
  test_flow_plugin_duplicate_staged
  FLOW_PLUGIN_FIXTURE_ID="fixture.duplicate-staged"
  FLOW_PLUGIN_FIXTURE_ADAPTER_KIND="fixture.adapter.duplicate-staged"
  FLOW_PLUGIN_FIXTURE_RESOURCE_KIND="fixture.resource.duplicate-staged"
  FLOW_PLUGIN_FIXTURE_MODE=2)
turbo_flow_add_plugin_fixture(
  test_flow_plugin_load_fail
  FLOW_PLUGIN_FIXTURE_ID="fixture.load-fail"
  FLOW_PLUGIN_FIXTURE_ADAPTER_KIND="fixture.adapter.load-fail"
  FLOW_PLUGIN_FIXTURE_RESOURCE_KIND="fixture.resource.load-fail"
  FLOW_PLUGIN_FIXTURE_MODE=3)
turbo_flow_add_plugin_fixture(
  test_flow_plugin_bad_abi
  FLOW_PLUGIN_FIXTURE_ID="fixture.bad-abi"
  FLOW_PLUGIN_FIXTURE_ADAPTER_KIND="fixture.adapter.bad-abi"
  FLOW_PLUGIN_FIXTURE_RESOURCE_KIND="fixture.resource.bad-abi"
  FLOW_PLUGIN_FIXTURE_ABI_MAJOR=99)
turbo_flow_add_plugin_fixture(
  test_flow_plugin_bad_size
  FLOW_PLUGIN_FIXTURE_ID="fixture.bad-size"
  FLOW_PLUGIN_FIXTURE_ADAPTER_KIND="fixture.adapter.bad-size"
  FLOW_PLUGIN_FIXTURE_RESOURCE_KIND="fixture.resource.bad-size"
  FLOW_PLUGIN_FIXTURE_API_SIZE=0u)
turbo_flow_add_plugin_fixture(
  test_flow_plugin_bad_id
  FLOW_PLUGIN_FIXTURE_ID="fixture bad id"
  FLOW_PLUGIN_FIXTURE_ADAPTER_KIND="fixture.adapter.bad-id"
  FLOW_PLUGIN_FIXTURE_RESOURCE_KIND="fixture.resource.bad-id")
turbo_flow_add_plugin_fixture(
  test_flow_plugin_non_ascii_id
  FLOW_PLUGIN_FIXTURE_NON_ASCII_ID=1
  FLOW_PLUGIN_FIXTURE_ADAPTER_KIND="fixture.adapter.non-ascii-id"
  FLOW_PLUGIN_FIXTURE_RESOURCE_KIND="fixture.resource.non-ascii-id")
turbo_flow_add_plugin_fixture(
  test_flow_plugin_bad_version
  FLOW_PLUGIN_FIXTURE_ID="fixture.bad-version"
  FLOW_PLUGIN_FIXTURE_VERSION="1 0"
  FLOW_PLUGIN_FIXTURE_ADAPTER_KIND="fixture.adapter.bad-version"
  FLOW_PLUGIN_FIXTURE_RESOURCE_KIND="fixture.resource.bad-version")
turbo_flow_add_plugin_fixture(
  test_flow_plugin_missing_callback
  FLOW_PLUGIN_FIXTURE_ID="fixture.missing-callback"
  FLOW_PLUGIN_FIXTURE_ADAPTER_KIND="fixture.adapter.missing-callback"
  FLOW_PLUGIN_FIXTURE_RESOURCE_KIND="fixture.resource.missing-callback"
  FLOW_PLUGIN_FIXTURE_MISSING_DESTROY=1)
turbo_flow_add_plugin_fixture(
  test_flow_plugin_quiesce_once
  FLOW_PLUGIN_FIXTURE_ID="fixture.quiesce-once"
  FLOW_PLUGIN_FIXTURE_ADAPTER_KIND="fixture.adapter.quiesce-once"
  FLOW_PLUGIN_FIXTURE_RESOURCE_KIND="fixture.resource.quiesce-once"
  FLOW_PLUGIN_FIXTURE_MODE=4)
turbo_flow_add_plugin_fixture(
  test_flow_plugin_shutdown_once
  FLOW_PLUGIN_FIXTURE_ID="fixture.shutdown-once"
  FLOW_PLUGIN_FIXTURE_ADAPTER_KIND="fixture.adapter.shutdown-once"
  FLOW_PLUGIN_FIXTURE_RESOURCE_KIND="fixture.resource.shutdown-once"
  FLOW_PLUGIN_FIXTURE_MODE=5)
turbo_flow_add_plugin_fixture(
  test_flow_plugin_capability_mismatch
  FLOW_PLUGIN_FIXTURE_ID="fixture.capability-mismatch"
  FLOW_PLUGIN_FIXTURE_ADAPTER_KIND="fixture.adapter.capability-mismatch"
  FLOW_PLUGIN_FIXTURE_RESOURCE_KIND="fixture.resource.capability-mismatch"
  FLOW_PLUGIN_FIXTURE_MODE=6)
turbo_flow_add_plugin_fixture(
  test_flow_plugin_external_poll_without_transactional
  FLOW_PLUGIN_FIXTURE_ID="fixture.external-poll-without-transactional"
  FLOW_PLUGIN_FIXTURE_ADAPTER_KIND="fixture.adapter.external-poll-without-transactional"
  FLOW_PLUGIN_FIXTURE_RESOURCE_KIND="fixture.resource.external-poll-without-transactional"
  FLOW_PLUGIN_FIXTURE_CAPABILITIES=TURBO_FLOW_PLUGIN_CAP_EXTERNAL_POLL)
turbo_flow_add_plugin_fixture(
  test_flow_plugin_invalid_provider
  FLOW_PLUGIN_FIXTURE_ID="fixture.invalid-provider"
  FLOW_PLUGIN_FIXTURE_ADAPTER_KIND="fixture.adapter.invalid-provider"
  FLOW_PLUGIN_FIXTURE_RESOURCE_KIND="fixture.resource.invalid-provider"
  FLOW_PLUGIN_FIXTURE_MODE=7)
turbo_flow_add_plugin_fixture(
  test_flow_plugin_invalid_provider_abi
  FLOW_PLUGIN_FIXTURE_ID="fixture.invalid-provider-abi"
  FLOW_PLUGIN_FIXTURE_ADAPTER_KIND="fixture.adapter.invalid-provider-abi"
  FLOW_PLUGIN_FIXTURE_RESOURCE_KIND="fixture.resource.invalid-provider-abi"
  FLOW_PLUGIN_FIXTURE_MODE=8)

add_library(test_flow_plugin_missing_symbol SHARED plugin_fixture_missing_symbol.c)
set_target_properties(test_flow_plugin_missing_symbol PROPERTIES FOLDER "turbo_flow/tests/plugin-fixtures")

add_library(test_flow_plugin_transactional SHARED plugin_generation_fixture.c)
target_link_libraries(test_flow_plugin_transactional PRIVATE TurboFlow::Product)
target_compile_definitions(test_flow_plugin_transactional PRIVATE TURBO_FLOW_PLUGIN_BUILD)
set_target_properties(
  test_flow_plugin_transactional
  PROPERTIES C_VISIBILITY_PRESET hidden
             VISIBILITY_INLINES_HIDDEN YES
             FOLDER "turbo_flow/tests/plugin-fixtures")

add_library(test_flow_plugin_transactional_duplicate_kind SHARED plugin_generation_fixture.c)
target_link_libraries(test_flow_plugin_transactional_duplicate_kind PRIVATE TurboFlow::Product)
target_compile_definitions(
  test_flow_plugin_transactional_duplicate_kind
  PRIVATE TURBO_FLOW_PLUGIN_BUILD
          FLOW_PLUGIN_GENERATION_FIXTURE_ID="fixture.transactional-duplicate-kind"
          FLOW_PLUGIN_GENERATION_ADAPTER_KIND="fixture.adapter.one")
set_target_properties(
  test_flow_plugin_transactional_duplicate_kind
  PROPERTIES C_VISIBILITY_PRESET hidden
             VISIBILITY_INLINES_HIDDEN YES
             FOLDER "turbo_flow/tests/plugin-fixtures")

function(turbo_flow_add_generation_fixture target_name)
  add_library(${target_name} SHARED plugin_generation_fixture.c)
  target_link_libraries(${target_name} PRIVATE TurboFlow::Product)
  target_compile_definitions(${target_name} PRIVATE TURBO_FLOW_PLUGIN_BUILD ${ARGN})
  set_target_properties(
    ${target_name}
    PROPERTIES C_VISIBILITY_PRESET hidden
               VISIBILITY_INLINES_HIDDEN YES
               FOLDER "turbo_flow/tests/plugin-fixtures")
endfunction()

turbo_flow_add_generation_fixture(
  test_flow_plugin_generation_capacity
  FLOW_PLUGIN_GENERATION_FIXTURE_ID="fixture.generation-capacity"
  FLOW_PLUGIN_GENERATION_EXPECT_PREFLIGHT_CALLS=0
  FLOW_PLUGIN_GENERATION_EXPECT_MATERIALIZE_CALLS=0
  FLOW_PLUGIN_GENERATION_EXPECT_OWNER_DESTROYS=0)
turbo_flow_add_generation_fixture(
  test_flow_plugin_generation_preflight_fail
  FLOW_PLUGIN_GENERATION_FIXTURE_ID="fixture.generation-preflight-fail"
  FLOW_PLUGIN_GENERATION_FAIL_PREFLIGHT_CALL=1
  FLOW_PLUGIN_GENERATION_EXPECT_PREFLIGHT_CALLS=1
  FLOW_PLUGIN_GENERATION_EXPECT_MATERIALIZE_CALLS=0
  FLOW_PLUGIN_GENERATION_EXPECT_OWNER_DESTROYS=0)
turbo_flow_add_generation_fixture(
  test_flow_plugin_generation_success
  FLOW_PLUGIN_GENERATION_FIXTURE_ID="fixture.generation-success"
  FLOW_PLUGIN_GENERATION_EXPECT_PREFLIGHT_CALLS=2
  FLOW_PLUGIN_GENERATION_EXPECT_MATERIALIZE_CALLS=2
  FLOW_PLUGIN_GENERATION_EXPECT_OWNER_DESTROYS=2)

add_executable(test_flow_plugin_projection_fixture_abi plugin_fixture_abi_test.c)
target_link_libraries(test_flow_plugin_projection_fixture_abi
  PRIVATE TurboFlow::PluginHost Salts::TinyTest test_flow_plugin_projection_fixture)
add_test(NAME test_flow_plugin_projection_fixture_abi
  COMMAND test_flow_plugin_projection_fixture_abi)
add_executable(test_flow_plugin_generation_fixture_abi plugin_fixture_abi_test.c)
# ABI rejection never preflights or materializes providers; the capacity fixture asserts zero.
target_link_libraries(test_flow_plugin_generation_fixture_abi
  PRIVATE TurboFlow::PluginHost Salts::TinyTest test_flow_plugin_generation_capacity)
add_test(NAME test_flow_plugin_generation_fixture_abi
  COMMAND test_flow_plugin_generation_fixture_abi)
set_target_properties(test_flow_plugin_projection_fixture_abi
  test_flow_plugin_generation_fixture_abi PROPERTIES FOLDER "turbo_flow/tests")
set_tests_properties(test_flow_plugin_projection_fixture_abi
                     test_flow_plugin_generation_fixture_abi PROPERTIES LABELS "plugin")
turbo_flow_add_generation_fixture(
  test_flow_plugin_generation_duplicate_reference
  FLOW_PLUGIN_GENERATION_FIXTURE_ID="fixture.generation-duplicate-reference"
  FLOW_PLUGIN_GENERATION_EXPECT_PREFLIGHT_CALLS=1
  FLOW_PLUGIN_GENERATION_EXPECT_MATERIALIZE_CALLS=1
  FLOW_PLUGIN_GENERATION_EXPECT_OWNER_DESTROYS=1)
turbo_flow_add_generation_fixture(
  test_flow_plugin_generation_materialize_first_fail
  FLOW_PLUGIN_GENERATION_FIXTURE_ID="fixture.generation-materialize-first-fail"
  FLOW_PLUGIN_GENERATION_FAIL_MATERIALIZE_CALL=1
  FLOW_PLUGIN_GENERATION_EXPECT_PREFLIGHT_CALLS=2
  FLOW_PLUGIN_GENERATION_EXPECT_MATERIALIZE_CALLS=1
  FLOW_PLUGIN_GENERATION_EXPECT_OWNER_DESTROYS=0
  FLOW_PLUGIN_GENERATION_EXPECT_LIFECYCLE="")
turbo_flow_add_generation_fixture(
  test_flow_plugin_generation_materialize_second_fail
  FLOW_PLUGIN_GENERATION_FIXTURE_ID="fixture.generation-materialize-second-fail"
  FLOW_PLUGIN_GENERATION_FAIL_MATERIALIZE_CALL=2
  FLOW_PLUGIN_GENERATION_EXPECT_PREFLIGHT_CALLS=2
  FLOW_PLUGIN_GENERATION_EXPECT_MATERIALIZE_CALLS=2
  FLOW_PLUGIN_GENERATION_EXPECT_OWNER_DESTROYS=1
  FLOW_PLUGIN_GENERATION_EXPECT_GRAPH_SHUTDOWN_BEFORE_OWNER_DESTROY=1
  FLOW_PLUGIN_GENERATION_EXPECT_LIFECYCLE="q1d1s1x1")
turbo_flow_add_generation_fixture(
  test_flow_plugin_generation_compile_fail
  FLOW_PLUGIN_GENERATION_FIXTURE_ID="fixture.generation-compile-fail"
  FLOW_PLUGIN_GENERATION_FORCE_COMPILE_ROLE_MISMATCH=1
  FLOW_PLUGIN_GENERATION_EXPECT_PREFLIGHT_CALLS=2
  FLOW_PLUGIN_GENERATION_EXPECT_MATERIALIZE_CALLS=2
  FLOW_PLUGIN_GENERATION_EXPECT_OWNER_DESTROYS=2
  FLOW_PLUGIN_GENERATION_EXPECT_GRAPH_SHUTDOWN_BEFORE_OWNER_DESTROY=1
  FLOW_PLUGIN_GENERATION_EXPECT_LIFECYCLE="q2q1d2d1s2s1x2x1")
turbo_flow_add_generation_fixture(
  test_flow_plugin_generation_lifecycle
  FLOW_PLUGIN_GENERATION_FIXTURE_ID="fixture.generation-lifecycle"
  FLOW_PLUGIN_GENERATION_EXPECT_PREFLIGHT_CALLS=2
  FLOW_PLUGIN_GENERATION_EXPECT_MATERIALIZE_CALLS=2
  FLOW_PLUGIN_GENERATION_EXPECT_OWNER_DESTROYS=2
  FLOW_PLUGIN_GENERATION_EXPECT_LIFECYCLE="q2q1d2d1s2s1x2x1"
  FLOW_PLUGIN_GENERATION_EXPECT_STOPPED_ON_DRAIN=1)
turbo_flow_add_generation_fixture(
  test_flow_plugin_generation_quiesce_once
  FLOW_PLUGIN_GENERATION_FIXTURE_ID="fixture.generation-quiesce-once"
  FLOW_PLUGIN_GENERATION_FAIL_QUIESCE_CALL=1
  FLOW_PLUGIN_GENERATION_EXPECT_PREFLIGHT_CALLS=2
  FLOW_PLUGIN_GENERATION_EXPECT_MATERIALIZE_CALLS=2
  FLOW_PLUGIN_GENERATION_EXPECT_OWNER_DESTROYS=2
  FLOW_PLUGIN_GENERATION_EXPECT_LIFECYCLE="q2q2q1d2d1s2s1x2x1"
  FLOW_PLUGIN_GENERATION_EXPECT_STOPPED_ON_DRAIN=1)
turbo_flow_add_generation_fixture(
  test_flow_plugin_generation_drain_once
  FLOW_PLUGIN_GENERATION_FIXTURE_ID="fixture.generation-drain-once"
  FLOW_PLUGIN_GENERATION_FAIL_DRAIN_CALL=1
  FLOW_PLUGIN_GENERATION_EXPECT_PREFLIGHT_CALLS=2
  FLOW_PLUGIN_GENERATION_EXPECT_MATERIALIZE_CALLS=2
  FLOW_PLUGIN_GENERATION_EXPECT_OWNER_DESTROYS=2
  FLOW_PLUGIN_GENERATION_EXPECT_LIFECYCLE="q2q1d2d2d1s2s1x2x1"
  FLOW_PLUGIN_GENERATION_EXPECT_STOPPED_ON_DRAIN=1)
turbo_flow_add_generation_fixture(
  test_flow_plugin_generation_shutdown_once
  FLOW_PLUGIN_GENERATION_FIXTURE_ID="fixture.generation-shutdown-once"
  FLOW_PLUGIN_GENERATION_FAIL_SHUTDOWN_CALL=1
  FLOW_PLUGIN_GENERATION_EXPECT_PREFLIGHT_CALLS=2
  FLOW_PLUGIN_GENERATION_EXPECT_MATERIALIZE_CALLS=2
  FLOW_PLUGIN_GENERATION_EXPECT_OWNER_DESTROYS=2
  FLOW_PLUGIN_GENERATION_EXPECT_LIFECYCLE="q2q1d2d1s2s2s1x2x1"
  FLOW_PLUGIN_GENERATION_EXPECT_STOPPED_ON_DRAIN=1)
turbo_flow_add_generation_fixture(
  test_flow_plugin_generation_quiesce_timeout
  FLOW_PLUGIN_GENERATION_FIXTURE_ID="fixture.generation-quiesce-timeout"
  FLOW_PLUGIN_GENERATION_FAIL_QUIESCE_CALL=1
  FLOW_PLUGIN_GENERATION_LIFECYCLE_FAIL_STATUS=SALTS_ETIMEDOUT
  FLOW_PLUGIN_GENERATION_EXPECT_PREFLIGHT_CALLS=2
  FLOW_PLUGIN_GENERATION_EXPECT_MATERIALIZE_CALLS=2
  FLOW_PLUGIN_GENERATION_EXPECT_OWNER_DESTROYS=2
  FLOW_PLUGIN_GENERATION_EXPECT_LIFECYCLE="q2q2q1d2d1s2s1x2x1"
  FLOW_PLUGIN_GENERATION_EXPECT_STOPPED_ON_DRAIN=1)
turbo_flow_add_generation_fixture(
  test_flow_plugin_generation_drain_timeout
  FLOW_PLUGIN_GENERATION_FIXTURE_ID="fixture.generation-drain-timeout"
  FLOW_PLUGIN_GENERATION_FAIL_DRAIN_CALL=1
  FLOW_PLUGIN_GENERATION_LIFECYCLE_FAIL_STATUS=SALTS_ETIMEDOUT
  FLOW_PLUGIN_GENERATION_EXPECT_PREFLIGHT_CALLS=2
  FLOW_PLUGIN_GENERATION_EXPECT_MATERIALIZE_CALLS=2
  FLOW_PLUGIN_GENERATION_EXPECT_OWNER_DESTROYS=2
  FLOW_PLUGIN_GENERATION_EXPECT_LIFECYCLE="q2q1d2d2d1s2s1x2x1"
  FLOW_PLUGIN_GENERATION_EXPECT_STOPPED_ON_DRAIN=1)
turbo_flow_add_generation_fixture(
  test_flow_plugin_generation_resource_order
  FLOW_PLUGIN_GENERATION_FIXTURE_ID="fixture.generation-resource-order"
  FLOW_PLUGIN_GENERATION_SKIP_ADAPTER_REGISTRATION_CALL=2
  FLOW_PLUGIN_GENERATION_EXPECT_PREFLIGHT_CALLS=2
  FLOW_PLUGIN_GENERATION_EXPECT_MATERIALIZE_CALLS=2
  FLOW_PLUGIN_GENERATION_EXPECT_OWNER_DESTROYS=2
  FLOW_PLUGIN_GENERATION_EXPECT_ASSEMBLY="prpamrma"
  FLOW_PLUGIN_GENERATION_EXPECT_LIFECYCLE="q2q1d2d1s2s1x2x1")
turbo_flow_add_generation_fixture(
  test_flow_plugin_generation_external_poll
  FLOW_PLUGIN_GENERATION_FIXTURE_ID="fixture.generation-external-poll"
  FLOW_PLUGIN_GENERATION_ENABLE_POLL=1
  FLOW_PLUGIN_GENERATION_EXPECT_PREFLIGHT_CALLS=3
  FLOW_PLUGIN_GENERATION_EXPECT_MATERIALIZE_CALLS=3
  FLOW_PLUGIN_GENERATION_EXPECT_POLL_CALLS=6
  FLOW_PLUGIN_GENERATION_EXPECT_BLOCKING_POLL_CALLS=2
  FLOW_PLUGIN_GENERATION_EXPECT_POLL_TIMEOUT_1=7
  FLOW_PLUGIN_GENERATION_EXPECT_POLL_TIMEOUT_2=0
  FLOW_PLUGIN_GENERATION_EXPECT_POLL_TIMEOUT_3=0
  FLOW_PLUGIN_GENERATION_EXPECT_POLL_TIMEOUT_4=11
  FLOW_PLUGIN_GENERATION_EXPECT_POLL_TIMEOUT_5=0
  FLOW_PLUGIN_GENERATION_EXPECT_POLL_TIMEOUT_6=0
  FLOW_PLUGIN_GENERATION_EXPECT_OWNER_DESTROYS=3
  FLOW_PLUGIN_GENERATION_EXPECT_LIFECYCLE="p1p2p3p2p3p1q3q2q1d3d2d1s3s2s1x3x2x1")
turbo_flow_add_generation_fixture(
  test_flow_plugin_generation_mixed_poll
  FLOW_PLUGIN_GENERATION_FIXTURE_ID="fixture.generation-mixed-poll"
  FLOW_PLUGIN_GENERATION_ENABLE_POLL=1
  FLOW_PLUGIN_GENERATION_POLL_OWNER_ORDINAL=2
  FLOW_PLUGIN_GENERATION_EXPECT_PREFLIGHT_CALLS=2
  FLOW_PLUGIN_GENERATION_EXPECT_MATERIALIZE_CALLS=2
  FLOW_PLUGIN_GENERATION_EXPECT_POLL_CALLS=2
  FLOW_PLUGIN_GENERATION_EXPECT_BLOCKING_POLL_CALLS=1
  FLOW_PLUGIN_GENERATION_EXPECT_POLL_TIMEOUT_1=13
  FLOW_PLUGIN_GENERATION_EXPECT_POLL_TIMEOUT_2=0
  FLOW_PLUGIN_GENERATION_EXPECT_OWNER_DESTROYS=2
  FLOW_PLUGIN_GENERATION_EXPECT_LIFECYCLE="p2p2q2q1d2d1s2s1x2x1")
turbo_flow_add_generation_fixture(
  test_flow_plugin_generation_legacy_owner_prefix
  FLOW_PLUGIN_GENERATION_FIXTURE_ID="fixture.generation-legacy-owner-prefix"
  FLOW_PLUGIN_GENERATION_LEGACY_OWNER_PREFIX=1
  FLOW_PLUGIN_GENERATION_EXPECT_PREFLIGHT_CALLS=2
  FLOW_PLUGIN_GENERATION_EXPECT_MATERIALIZE_CALLS=1
  FLOW_PLUGIN_GENERATION_EXPECT_POLL_CALLS=0
  FLOW_PLUGIN_GENERATION_EXPECT_OWNER_DESTROYS=1)
turbo_flow_add_generation_fixture(
  test_flow_plugin_generation_poll_error
  FLOW_PLUGIN_GENERATION_FIXTURE_ID="fixture.generation-poll-error"
  FLOW_PLUGIN_GENERATION_ENABLE_POLL=1
  FLOW_PLUGIN_GENERATION_FAIL_POLL_CALL=2
  FLOW_PLUGIN_GENERATION_EXPECT_PREFLIGHT_CALLS=2
  FLOW_PLUGIN_GENERATION_EXPECT_MATERIALIZE_CALLS=2
  FLOW_PLUGIN_GENERATION_EXPECT_POLL_CALLS=2
  FLOW_PLUGIN_GENERATION_EXPECT_BLOCKING_POLL_CALLS=1
  FLOW_PLUGIN_GENERATION_EXPECT_OWNER_DESTROYS=2
  FLOW_PLUGIN_GENERATION_EXPECT_LIFECYCLE="p1p2q2q1d2d1s2s1x2x1")
turbo_flow_add_generation_fixture(
  test_flow_plugin_generation_poll_closes
  FLOW_PLUGIN_GENERATION_FIXTURE_ID="fixture.generation-poll-closes"
  FLOW_PLUGIN_GENERATION_ENABLE_POLL=1
  FLOW_PLUGIN_GENERATION_FAIL_QUIESCE_CALL=1
  FLOW_PLUGIN_GENERATION_LIFECYCLE_FAIL_STATUS=SALTS_ETIMEDOUT
  FLOW_PLUGIN_GENERATION_EXPECT_PREFLIGHT_CALLS=2
  FLOW_PLUGIN_GENERATION_EXPECT_MATERIALIZE_CALLS=2
  FLOW_PLUGIN_GENERATION_EXPECT_POLL_CALLS=0
  FLOW_PLUGIN_GENERATION_EXPECT_OWNER_DESTROYS=2
  FLOW_PLUGIN_GENERATION_EXPECT_LIFECYCLE="q2q2q1d2d1s2s1x2x1"
  FLOW_PLUGIN_GENERATION_EXPECT_STOPPED_ON_DRAIN=1)
foreach(mode IN ITEMS flag_without_callback callback_without_flag small_descriptor)
  if(mode STREQUAL "flag_without_callback")
    set(mode_value 1)
  elseif(mode STREQUAL "callback_without_flag")
    set(mode_value 2)
  else()
    set(mode_value 3)
  endif()
  turbo_flow_add_generation_fixture(
    test_flow_plugin_generation_poll_${mode}
    FLOW_PLUGIN_GENERATION_FIXTURE_ID="fixture.generation-poll-${mode}"
    FLOW_PLUGIN_GENERATION_POLL_DESCRIPTOR_MODE=${mode_value}
    FLOW_PLUGIN_GENERATION_EXPECT_GRAPH_SHUTDOWN_BEFORE_OWNER_DESTROY=1
    FLOW_PLUGIN_GENERATION_EXPECT_PREFLIGHT_CALLS=2
    FLOW_PLUGIN_GENERATION_EXPECT_MATERIALIZE_CALLS=1
    FLOW_PLUGIN_GENERATION_EXPECT_OWNER_DESTROYS=1
    FLOW_PLUGIN_GENERATION_EXPECT_LIFECYCLE="x1")
endforeach()

cmake_add_test(
  test_flow_plugin_host
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_plugin_host.c
          ${CMAKE_CURRENT_SOURCE_DIR}/plugin_generation_header_cpp.cpp
  LIBS TurboFlow::PluginHost Salts::TinyTest
  FOLDER "turbo_flow/tests")
target_compile_definitions(
  test_flow_plugin_host
  PRIVATE FLOW_PLUGIN_FIXTURE_GOOD_ONE="$<TARGET_FILE:test_flow_plugin_good_one>"
          FLOW_PLUGIN_FIXTURE_GOOD_TWO="$<TARGET_FILE:test_flow_plugin_good_two>"
          FLOW_PLUGIN_FIXTURE_DUPLICATE_ID="$<TARGET_FILE:test_flow_plugin_duplicate_id>"
          FLOW_PLUGIN_FIXTURE_DUPLICATE_KIND="$<TARGET_FILE:test_flow_plugin_duplicate_kind>"
          FLOW_PLUGIN_FIXTURE_DUPLICATE_RESOURCE_KIND="$<TARGET_FILE:test_flow_plugin_duplicate_resource_kind>"
          FLOW_PLUGIN_FIXTURE_REGISTER_FAIL="$<TARGET_FILE:test_flow_plugin_register_fail>"
          FLOW_PLUGIN_FIXTURE_DUPLICATE_STAGED="$<TARGET_FILE:test_flow_plugin_duplicate_staged>"
          FLOW_PLUGIN_FIXTURE_LOAD_FAIL="$<TARGET_FILE:test_flow_plugin_load_fail>"
          FLOW_PLUGIN_FIXTURE_BAD_ABI="$<TARGET_FILE:test_flow_plugin_bad_abi>"
          FLOW_PLUGIN_FIXTURE_BAD_SIZE="$<TARGET_FILE:test_flow_plugin_bad_size>"
          FLOW_PLUGIN_FIXTURE_BAD_ID="$<TARGET_FILE:test_flow_plugin_bad_id>"
          FLOW_PLUGIN_FIXTURE_NON_ASCII_ID="$<TARGET_FILE:test_flow_plugin_non_ascii_id>"
          FLOW_PLUGIN_FIXTURE_BAD_VERSION="$<TARGET_FILE:test_flow_plugin_bad_version>"
          FLOW_PLUGIN_FIXTURE_MISSING_CALLBACK="$<TARGET_FILE:test_flow_plugin_missing_callback>"
          FLOW_PLUGIN_FIXTURE_QUIESCE_ONCE="$<TARGET_FILE:test_flow_plugin_quiesce_once>"
          FLOW_PLUGIN_FIXTURE_SHUTDOWN_ONCE="$<TARGET_FILE:test_flow_plugin_shutdown_once>"
          FLOW_PLUGIN_FIXTURE_CAPABILITY_MISMATCH="$<TARGET_FILE:test_flow_plugin_capability_mismatch>"
          FLOW_PLUGIN_FIXTURE_EXTERNAL_POLL_WITHOUT_TRANSACTIONAL="$<TARGET_FILE:test_flow_plugin_external_poll_without_transactional>"
          FLOW_PLUGIN_FIXTURE_INVALID_PROVIDER="$<TARGET_FILE:test_flow_plugin_invalid_provider>"
          FLOW_PLUGIN_FIXTURE_INVALID_PROVIDER_ABI="$<TARGET_FILE:test_flow_plugin_invalid_provider_abi>"
          FLOW_PLUGIN_FIXTURE_TRANSACTIONAL="$<TARGET_FILE:test_flow_plugin_transactional>"
          FLOW_PLUGIN_FIXTURE_TRANSACTIONAL_DUPLICATE_KIND="$<TARGET_FILE:test_flow_plugin_transactional_duplicate_kind>"
          FLOW_PLUGIN_FIXTURE_MISSING_SYMBOL="$<TARGET_FILE:test_flow_plugin_missing_symbol>")
add_dependencies(
  test_flow_plugin_host
  test_flow_plugin_good_one
  test_flow_plugin_good_two
  test_flow_plugin_duplicate_id
  test_flow_plugin_duplicate_kind
  test_flow_plugin_duplicate_resource_kind
  test_flow_plugin_register_fail
  test_flow_plugin_duplicate_staged
  test_flow_plugin_load_fail
  test_flow_plugin_bad_abi
  test_flow_plugin_bad_size
  test_flow_plugin_bad_id
  test_flow_plugin_non_ascii_id
  test_flow_plugin_bad_version
  test_flow_plugin_missing_callback
  test_flow_plugin_quiesce_once
  test_flow_plugin_shutdown_once
  test_flow_plugin_capability_mismatch
  test_flow_plugin_external_poll_without_transactional
  test_flow_plugin_invalid_provider
  test_flow_plugin_invalid_provider_abi
  test_flow_plugin_transactional
  test_flow_plugin_transactional_duplicate_kind
  test_flow_plugin_missing_symbol)

cmake_add_test(
  test_flow_plugin_generation
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_plugin_generation.c
  LIBS TurboFlow::PluginHost Salts::TinyTest
  FOLDER "turbo_flow/tests")
target_compile_definitions(
  test_flow_plugin_generation
  PRIVATE FLOW_PLUGIN_GENERATION_FIXTURE_LEGACY="$<TARGET_FILE:test_flow_plugin_good_one>"
          FLOW_PLUGIN_GENERATION_FIXTURE_CAPACITY="$<TARGET_FILE:test_flow_plugin_generation_capacity>"
          FLOW_PLUGIN_GENERATION_FIXTURE_PREFLIGHT_FAIL="$<TARGET_FILE:test_flow_plugin_generation_preflight_fail>"
          FLOW_PLUGIN_GENERATION_FIXTURE_SUCCESS="$<TARGET_FILE:test_flow_plugin_generation_success>"
          FLOW_PLUGIN_GENERATION_FIXTURE_DUPLICATE_REFERENCE="$<TARGET_FILE:test_flow_plugin_generation_duplicate_reference>"
          FLOW_PLUGIN_GENERATION_FIXTURE_MATERIALIZE_FIRST_FAIL="$<TARGET_FILE:test_flow_plugin_generation_materialize_first_fail>"
          FLOW_PLUGIN_GENERATION_FIXTURE_MATERIALIZE_SECOND_FAIL="$<TARGET_FILE:test_flow_plugin_generation_materialize_second_fail>"
          FLOW_PLUGIN_GENERATION_FIXTURE_COMPILE_FAIL="$<TARGET_FILE:test_flow_plugin_generation_compile_fail>"
          FLOW_PLUGIN_GENERATION_FIXTURE_LIFECYCLE="$<TARGET_FILE:test_flow_plugin_generation_lifecycle>"
          FLOW_PLUGIN_GENERATION_FIXTURE_QUIESCE_ONCE="$<TARGET_FILE:test_flow_plugin_generation_quiesce_once>"
          FLOW_PLUGIN_GENERATION_FIXTURE_DRAIN_ONCE="$<TARGET_FILE:test_flow_plugin_generation_drain_once>"
          FLOW_PLUGIN_GENERATION_FIXTURE_SHUTDOWN_ONCE="$<TARGET_FILE:test_flow_plugin_generation_shutdown_once>"
          FLOW_PLUGIN_GENERATION_FIXTURE_QUIESCE_TIMEOUT="$<TARGET_FILE:test_flow_plugin_generation_quiesce_timeout>"
          FLOW_PLUGIN_GENERATION_FIXTURE_DRAIN_TIMEOUT="$<TARGET_FILE:test_flow_plugin_generation_drain_timeout>"
          FLOW_PLUGIN_GENERATION_FIXTURE_RESOURCE_ORDER="$<TARGET_FILE:test_flow_plugin_generation_resource_order>"
          FLOW_PLUGIN_GENERATION_FIXTURE_EXTERNAL_POLL="$<TARGET_FILE:test_flow_plugin_generation_external_poll>"
          FLOW_PLUGIN_GENERATION_FIXTURE_MIXED_POLL="$<TARGET_FILE:test_flow_plugin_generation_mixed_poll>"
          FLOW_PLUGIN_GENERATION_FIXTURE_LEGACY_OWNER_PREFIX="$<TARGET_FILE:test_flow_plugin_generation_legacy_owner_prefix>"
          FLOW_PLUGIN_GENERATION_FIXTURE_POLL_ERROR="$<TARGET_FILE:test_flow_plugin_generation_poll_error>"
          FLOW_PLUGIN_GENERATION_FIXTURE_POLL_CLOSES="$<TARGET_FILE:test_flow_plugin_generation_poll_closes>"
          FLOW_PLUGIN_GENERATION_FIXTURE_POLL_FLAG_WITHOUT_CALLBACK="$<TARGET_FILE:test_flow_plugin_generation_poll_flag_without_callback>"
          FLOW_PLUGIN_GENERATION_FIXTURE_POLL_CALLBACK_WITHOUT_FLAG="$<TARGET_FILE:test_flow_plugin_generation_poll_callback_without_flag>"
          FLOW_PLUGIN_GENERATION_FIXTURE_POLL_SMALL_DESCRIPTOR="$<TARGET_FILE:test_flow_plugin_generation_poll_small_descriptor>")
add_dependencies(
  test_flow_plugin_generation
  test_flow_plugin_good_one
  test_flow_plugin_generation_capacity
  test_flow_plugin_generation_preflight_fail
  test_flow_plugin_generation_success
  test_flow_plugin_generation_duplicate_reference
  test_flow_plugin_generation_materialize_first_fail
  test_flow_plugin_generation_materialize_second_fail
  test_flow_plugin_generation_compile_fail
  test_flow_plugin_generation_lifecycle
  test_flow_plugin_generation_quiesce_once
  test_flow_plugin_generation_drain_once
  test_flow_plugin_generation_shutdown_once
  test_flow_plugin_generation_quiesce_timeout
  test_flow_plugin_generation_drain_timeout
  test_flow_plugin_generation_resource_order
  test_flow_plugin_generation_external_poll
  test_flow_plugin_generation_mixed_poll
  test_flow_plugin_generation_legacy_owner_prefix
  test_flow_plugin_generation_poll_error
  test_flow_plugin_generation_poll_closes
  test_flow_plugin_generation_poll_flag_without_callback
  test_flow_plugin_generation_poll_callback_without_flag
  test_flow_plugin_generation_poll_small_descriptor)
add_library(test_flow_plugin_generation_graph_stop SHARED plugin_generation_fixture.c)
target_link_libraries(test_flow_plugin_generation_graph_stop PRIVATE TurboFlow::PluginHost)
target_compile_definitions(test_flow_plugin_generation_graph_stop PRIVATE TURBO_FLOW_PLUGIN_BUILD
  FLOW_PLUGIN_GENERATION_FIXTURE_ID="fixture.generation-graph-stop"
  FLOW_PLUGIN_GENERATION_FAIL_GRAPH_STOP=1
  FLOW_PLUGIN_GENERATION_EXPECT_PREFLIGHT_CALLS=2
  FLOW_PLUGIN_GENERATION_EXPECT_MATERIALIZE_CALLS=2
  FLOW_PLUGIN_GENERATION_EXPECT_OWNER_DESTROYS=2
  FLOW_PLUGIN_GENERATION_EXPECT_LIFECYCLE="q2q1g2g1g2d2d1s2s1x2x1")
set_target_properties(test_flow_plugin_generation_graph_stop PROPERTIES FOLDER "turbo_flow/tests/plugin-fixtures")
target_compile_definitions(test_flow_plugin_generation PRIVATE
  FLOW_PLUGIN_GENERATION_FIXTURE_GRAPH_STOP="$<TARGET_FILE:test_flow_plugin_generation_graph_stop>")
add_dependencies(test_flow_plugin_generation test_flow_plugin_generation_graph_stop)

add_library(test_flow_plugin_invalid_owner_fixture SHARED plugin_generation_fixture.c)
target_link_libraries(test_flow_plugin_invalid_owner_fixture PRIVATE TurboFlow::PluginHost)
target_compile_definitions(test_flow_plugin_invalid_owner_fixture PRIVATE TURBO_FLOW_PLUGIN_BUILD
  FLOW_PLUGIN_GENERATION_FIXTURE_ID="fixture.invalid-owner"
  FLOW_PLUGIN_GENERATION_EXPECT_GRAPH_SHUTDOWN_BEFORE_OWNER_DESTROY=1)
set_target_properties(test_flow_plugin_invalid_owner_fixture PROPERTIES FOLDER "turbo_flow/tests/plugin-fixtures")
add_executable(test_flow_plugin_invalid_owner test_flow_plugin_invalid_owner.c)
target_link_libraries(test_flow_plugin_invalid_owner PRIVATE TurboFlow::PluginHost Salts::TinyTest)
target_compile_definitions(test_flow_plugin_invalid_owner PRIVATE
  FLOW_INVALID_OWNER_FIXTURE="$<TARGET_FILE:test_flow_plugin_invalid_owner_fixture>"
  FLOW_INVALID_OWNER_LEGACY="$<TARGET_FILE:test_flow_plugin_generation_legacy_owner_prefix>"
  FLOW_INVALID_OWNER_SMALL_POLL="$<TARGET_FILE:test_flow_plugin_generation_poll_small_descriptor>")
add_dependencies(test_flow_plugin_invalid_owner test_flow_plugin_invalid_owner_fixture
  test_flow_plugin_generation_legacy_owner_prefix test_flow_plugin_generation_poll_small_descriptor)
set_target_properties(test_flow_plugin_invalid_owner PROPERTIES FOLDER "turbo_flow/tests")
add_test(NAME test_flow_plugin_invalid_owner COMMAND test_flow_plugin_invalid_owner --filter "destroys exact ABI")
add_test(NAME test_flow_plugin_invalid_owner_abi COMMAND test_flow_plugin_invalid_owner --filter "pins unknown ABI")
set_tests_properties(test_flow_plugin_invalid_owner test_flow_plugin_invalid_owner_abi PROPERTIES LABELS "graph;core" TIMEOUT 30)

set_tests_properties(
  test_turbo_flow test_flow_async_terminal test_flow_async_emit test_flow_resource_document test_flow_managed_boundary test_flow_config
  test_flow_managed_source test_resolved_config_boundary test_flow_plugin_host
  test_flow_plugin_generation
  PROPERTIES LABELS "graph;core")

if(WIN32)
  set_tests_properties(
    test_flow_config test_resolved_config_boundary
    PROPERTIES ENVIRONMENT_MODIFICATION
               "PATH=path_list_prepend:$<TARGET_FILE_DIR:Salts::Core>;PATH=path_list_prepend:$<TARGET_FILE_DIR:Salts::DataBind>")
endif()
