cmake_add_test(
  test_turbo_flow
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_turbo_flow.c
          ${CMAKE_CURRENT_SOURCE_DIR}/flow_plan_type_probe.c
  LIBS TurboFlow::Flow Salts::CFlow Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../src
  FOLDER "turbo_flow/tests")

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
  LIBS TurboFlow::Flow Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../src
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_resource_document
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_resource_document.c
  LIBS TurboFlow::Flow Salts::DataBind Salts::TinyTest
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_managed_boundary
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_managed_boundary.c
  LIBS TurboFlow::Flow Salts::TinyTest
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_managed_async_terminal
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_managed_async_terminal.c
  LIBS TurboFlow::Flow Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../src
  FOLDER "turbo_flow/tests")

cmake_add_test(
  test_flow_managed_source
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_managed_source.c
  LIBS TurboFlow::Flow Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../src
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
  FLOW_PLUGIN_GENERATION_EXPECT_LIFECYCLE="x1")
turbo_flow_add_generation_fixture(
  test_flow_plugin_generation_compile_fail
  FLOW_PLUGIN_GENERATION_FIXTURE_ID="fixture.generation-compile-fail"
  FLOW_PLUGIN_GENERATION_SKIP_ADAPTER_REGISTRATION_CALL=2
  FLOW_PLUGIN_GENERATION_EXPECT_PREFLIGHT_CALLS=2
  FLOW_PLUGIN_GENERATION_EXPECT_MATERIALIZE_CALLS=2
  FLOW_PLUGIN_GENERATION_EXPECT_OWNER_DESTROYS=2
  FLOW_PLUGIN_GENERATION_EXPECT_LIFECYCLE="x2x1")
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
  FLOW_PLUGIN_GENERATION_EXPECT_LIFECYCLE="x2x1")

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
          FLOW_PLUGIN_GENERATION_FIXTURE_RESOURCE_ORDER="$<TARGET_FILE:test_flow_plugin_generation_resource_order>")
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
  test_flow_plugin_generation_resource_order)

set_tests_properties(
  test_turbo_flow test_flow_async_terminal test_flow_async_emit test_flow_resource_document test_flow_managed_boundary test_flow_config test_flow_compat_boundary
  test_flow_managed_source test_resolved_config_boundary test_flow_plugin_host
  test_flow_plugin_generation
  PROPERTIES LABELS "graph;core")

if(WIN32)
  set_tests_properties(
    test_flow_config test_flow_compat_boundary test_resolved_config_boundary
    PROPERTIES ENVIRONMENT_MODIFICATION
               "PATH=path_list_prepend:$<TARGET_FILE_DIR:Salts::Core>;PATH=path_list_prepend:$<TARGET_FILE_DIR:Salts::DataBind>")
endif()
