salts_idl_target(
  TARGET turbo_flow_databind_channel_fixture
  IDL "${CMAKE_CURRENT_SOURCE_DIR}/databind_channel.schema"
  ARTIFACT_NAME "tf_channel"
  TRANSPORTS SOCKET FLOWMQ
  PROJECTION_CONFIG
    "${CMAKE_CURRENT_SOURCE_DIR}/databind_channel_projection.json")

cmake_add_test(
  test_flow_databind_channel_plan
  SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_databind_channel_plan.c
    ${CMAKE_CURRENT_SOURCE_DIR}/databind_channel_header_cpp.cpp
  LIBS
    TurboFlow::Product
    Salts::TinyTest
    ${turbo_flow_databind_channel_fixture_NATIVE_TARGET}
  INCLUDES
    ${CMAKE_CURRENT_SOURCE_DIR}/../src
    ${turbo_flow_databind_channel_fixture_GENERATED_DIR}
  FOLDER "turbo_flow/tests")
add_dependencies(
  test_flow_databind_channel_plan
  turbo_flow_databind_channel_fixture_idl_codegen)
set_target_properties(test_flow_databind_channel_plan PROPERTIES
  C_STANDARD 11
  C_STANDARD_REQUIRED ON
  C_EXTENSIONS OFF
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED ON
  CXX_EXTENSIONS OFF)
set_tests_properties(test_flow_databind_channel_plan PROPERTIES
  LABELS "graph;databind;plan;transport" TIMEOUT 30)

salts_idl_target(
  TARGET turbo_flow_provider_config_fixture
  IDL "${CMAKE_CURRENT_SOURCE_DIR}/provider_config.schema"
  ARTIFACT_NAME "provider_config"
  ARTIFACTS MESSAGE)

cmake_add_test(
  test_flow_provider_typed_config
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_provider_typed_config.c
  LIBS
    TurboFlow::Product
    Salts::TinyTest
    ${turbo_flow_provider_config_fixture_NATIVE_TARGET}
  INCLUDES
    ${CMAKE_CURRENT_SOURCE_DIR}/../src
    ${turbo_flow_provider_config_fixture_GENERATED_DIR}
  FOLDER "turbo_flow/tests")
add_dependencies(
  test_flow_provider_typed_config
  turbo_flow_provider_config_fixture_idl_codegen)
set_target_properties(test_flow_provider_typed_config PROPERTIES
  C_STANDARD 11
  C_STANDARD_REQUIRED ON
  C_EXTENSIONS OFF)
set_tests_properties(test_flow_provider_typed_config PROPERTIES
  LABELS "graph;config;databind;provider" TIMEOUT 30)

salts_idl_target(
  TARGET turbo_flow_databind_service_fixture
  IDL "${CMAKE_CURRENT_SOURCE_DIR}/databind_service.schema"
  ARTIFACT_NAME "tf_service"
  ARTIFACTS NATIVE
  TRANSPORTS HTTP RPC)

cmake_add_test(
  test_flow_databind_service_plan
  SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_databind_service_plan.c
    ${CMAKE_CURRENT_SOURCE_DIR}/databind_service_header_cpp.cpp
  LIBS
    TurboFlow::Product
    Salts::TinyTest
    ${turbo_flow_databind_service_fixture_NATIVE_TARGET}
  INCLUDES
    ${CMAKE_CURRENT_SOURCE_DIR}/../src
    ${turbo_flow_databind_service_fixture_GENERATED_DIR}
  FOLDER "turbo_flow/tests")
add_dependencies(
  test_flow_databind_service_plan
  turbo_flow_databind_service_fixture_idl_codegen)
set_target_properties(test_flow_databind_service_plan PROPERTIES
  C_STANDARD 11
  C_STANDARD_REQUIRED ON
  C_EXTENSIONS OFF
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED ON
  CXX_EXTENSIONS OFF)
set_tests_properties(test_flow_databind_service_plan PROPERTIES
  LABELS "graph;databind;plan;service" TIMEOUT 30)

add_library(test_flow_plugin_cflow_function_fixture SHARED
  plugin_cflow_function_fixture.c)
target_link_libraries(test_flow_plugin_cflow_function_fixture
  PRIVATE Salts::PluginABI)
set_target_properties(test_flow_plugin_cflow_function_fixture PROPERTIES
  FOLDER "turbo_flow/tests/plugin-fixtures")

add_executable(test_flow_plugin_cflow_plan test_flow_plugin_cflow_plan.c)
target_link_libraries(test_flow_plugin_cflow_plan
  PRIVATE TurboFlow::PluginHost Salts::Plugin Salts::TinyTest)
target_include_directories(test_flow_plugin_cflow_plan
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../src)
target_compile_definitions(test_flow_plugin_cflow_plan PRIVATE
  FLOW_PLUGIN_CFLOW_FIXTURE="$<TARGET_FILE:test_flow_plugin_cflow_function_fixture>")
add_dependencies(test_flow_plugin_cflow_plan test_flow_plugin_cflow_function_fixture)
set_target_properties(test_flow_plugin_cflow_plan PROPERTIES
  FOLDER "turbo_flow/tests")
add_test(NAME test_flow_plugin_cflow_plan COMMAND test_flow_plugin_cflow_plan)
set_tests_properties(test_flow_plugin_cflow_plan PROPERTIES
  LABELS "graph;plugin;cflow;plan" TIMEOUT 30)

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
add_executable(test_flow_plugin_materializer_generation
  test_flow_plugin_materializer_generation.c)
target_link_libraries(test_flow_plugin_materializer_generation
  PRIVATE TurboFlow::PluginHost Salts::TinyTest)
target_compile_definitions(test_flow_plugin_materializer_generation PRIVATE
  FLOW_OPERATION_OK="$<TARGET_FILE:test_flow_operation_fixture_OK>"
  FLOW_OPERATION_TWO_NAMES="$<TARGET_FILE:test_flow_operation_fixture_TWO_NAMES>"
  FLOW_MATERIALIZER_OPERATION="$<TARGET_FILE:test_flow_plugin_materializer_operation>"
  FLOW_MATERIALIZER_MISMATCH="$<TARGET_FILE:test_flow_plugin_materializer_mismatch>"
  FLOW_MATERIALIZER_UNUSED="$<TARGET_FILE:test_flow_plugin_materializer_unused>")
add_dependencies(test_flow_plugin_materializer_generation
  test_flow_operation_fixture_OK
  test_flow_operation_fixture_TWO_NAMES
  test_flow_plugin_materializer_operation
  test_flow_plugin_materializer_mismatch
  test_flow_plugin_materializer_unused)
set_target_properties(test_flow_plugin_materializer_generation
  PROPERTIES FOLDER "turbo_flow/tests")
add_test(NAME test_flow_plugin_materializer_generation
  COMMAND test_flow_plugin_materializer_generation)
set_tests_properties(test_flow_plugin_materializer_generation
  PROPERTIES LABELS "graph;plugin;abi3;materializer" TIMEOUT 30)

add_library(test_flow_plugin_projection_fixture SHARED plugin_projection_fixture.c)
cmake_add_test(
  test_flow_provider_interface
  SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_provider_interface.c
    ${CMAKE_CURRENT_SOURCE_DIR}/provider_interface_header_cpp.cpp
  LIBS TurboFlow::Product Salts::Plugin Salts::TinyTest
  FOLDER "turbo_flow/tests")
set_tests_properties(test_flow_provider_interface
  PROPERTIES LABELS "graph;provider;plugin;interface" TIMEOUT 30)

cmake_add_test(
  test_flow_provider_adapter_binding
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_provider_adapter_binding.c
  LIBS TurboFlow::Product Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../src
  FOLDER "turbo_flow/tests")
set_target_properties(test_flow_provider_adapter_binding PROPERTIES
  C_STANDARD 11 C_STANDARD_REQUIRED ON C_EXTENSIONS OFF)
set_tests_properties(test_flow_provider_adapter_binding PROPERTIES
  LABELS "graph;provider;adapter;instance" TIMEOUT 30)

add_library(test_flow_salts_provider_fixture SHARED salts_provider_fixture.c)
target_link_libraries(test_flow_salts_provider_fixture
  PRIVATE TurboFlow::Product Salts::Plugin
          ${turbo_flow_provider_config_fixture_NATIVE_TARGET})
target_include_directories(test_flow_salts_provider_fixture PRIVATE
  ${turbo_flow_provider_config_fixture_GENERATED_DIR})
add_dependencies(test_flow_salts_provider_fixture
  turbo_flow_provider_config_fixture_idl_codegen)
set_target_properties(test_flow_salts_provider_fixture PROPERTIES
  FOLDER "turbo_flow/tests/fixtures"
  C_VISIBILITY_PRESET hidden
  VISIBILITY_INLINES_HIDDEN YES)

add_library(test_flow_salts_resource_fixture SHARED
  salts_resource_fixture.c
  salts_resource_fixture.h)
target_link_libraries(test_flow_salts_resource_fixture
  PRIVATE TurboFlow::Product Salts::Plugin)
set_target_properties(test_flow_salts_resource_fixture PROPERTIES
  FOLDER "turbo_flow/tests/fixtures"
  C_VISIBILITY_PRESET hidden
  VISIBILITY_INLINES_HIDDEN YES)

cmake_add_test(
  test_flow_salts_provider_binding
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_salts_provider_binding.c
  LIBS TurboFlow::Product Salts::Plugin Salts::TinyTest
       ${turbo_flow_provider_config_fixture_NATIVE_TARGET}
  INCLUDES ${turbo_flow_provider_config_fixture_GENERATED_DIR}
  FOLDER "turbo_flow/tests")
add_dependencies(test_flow_salts_provider_binding
  test_flow_salts_provider_fixture
  test_flow_salts_resource_fixture
  turbo_flow_provider_config_fixture_idl_codegen)
target_compile_definitions(test_flow_salts_provider_binding PRIVATE
  FLOW_SALTS_PROVIDER_FIXTURE="$<TARGET_FILE:test_flow_salts_provider_fixture>"
  FLOW_SALTS_RESOURCE_FIXTURE="$<TARGET_FILE:test_flow_salts_resource_fixture>")
set_tests_properties(test_flow_salts_provider_binding PROPERTIES
  LABELS "graph;provider;plugin;interface;lease"
  TIMEOUT 30)

cmake_add_test(
  test_flow_provider_binding
  SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_provider_binding.c
    ${CMAKE_CURRENT_SOURCE_DIR}/provider_binding_header_cpp.cpp
  LIBS TurboFlow::Product Salts::Plugin Salts::TinyTest
       ${turbo_flow_provider_config_fixture_NATIVE_TARGET}
  INCLUDES ${turbo_flow_provider_config_fixture_GENERATED_DIR}
  FOLDER "turbo_flow/tests")
add_dependencies(test_flow_provider_binding
  test_flow_salts_provider_fixture
  test_flow_salts_resource_fixture
  turbo_flow_provider_config_fixture_idl_codegen)
target_compile_definitions(test_flow_provider_binding PRIVATE
  FLOW_SALTS_PROVIDER_FIXTURE="$<TARGET_FILE:test_flow_salts_provider_fixture>"
  FLOW_SALTS_RESOURCE_FIXTURE="$<TARGET_FILE:test_flow_salts_resource_fixture>")
set_target_properties(test_flow_provider_binding PROPERTIES
  C_STANDARD 11 C_STANDARD_REQUIRED ON C_EXTENSIONS OFF
  CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON CXX_EXTENSIONS OFF)
set_tests_properties(test_flow_provider_binding PROPERTIES
  LABELS "graph;provider;plugin;interface;lease"
  TIMEOUT 30)

cmake_add_test(
  test_flow_compiled_provider_instance
  SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_compiled_provider_instance.c
  LIBS TurboFlow::Product Salts::Plugin Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../src
  FOLDER "turbo_flow/tests")
add_dependencies(test_flow_compiled_provider_instance
  test_flow_salts_provider_fixture
  test_flow_salts_resource_fixture)
target_compile_definitions(test_flow_compiled_provider_instance PRIVATE
  FLOW_SALTS_PROVIDER_FIXTURE="$<TARGET_FILE:test_flow_salts_provider_fixture>"
  FLOW_SALTS_RESOURCE_FIXTURE="$<TARGET_FILE:test_flow_salts_resource_fixture>")
set_target_properties(test_flow_compiled_provider_instance PROPERTIES
  C_STANDARD 11 C_STANDARD_REQUIRED ON C_EXTENSIONS OFF)
set_tests_properties(test_flow_compiled_provider_instance PROPERTIES
  LABELS "graph;provider;plugin;interface;lease;config;resource"
  TIMEOUT 30)

cmake_add_test(
  test_flow_provider_generation
  SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_provider_generation.c
  LIBS TurboFlow::Product Salts::Plugin Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../src
  FOLDER "turbo_flow/tests")
add_dependencies(test_flow_provider_generation
  test_flow_salts_provider_fixture
  test_flow_salts_resource_fixture)
target_compile_definitions(test_flow_provider_generation PRIVATE
  FLOW_SALTS_PROVIDER_FIXTURE="$<TARGET_FILE:test_flow_salts_provider_fixture>"
  FLOW_SALTS_RESOURCE_FIXTURE="$<TARGET_FILE:test_flow_salts_resource_fixture>")
set_target_properties(test_flow_provider_generation PROPERTIES
  C_STANDARD 11 C_STANDARD_REQUIRED ON C_EXTENSIONS OFF)
set_tests_properties(test_flow_provider_generation PROPERTIES
  LABELS "graph;provider;plugin;interface;lease;generation"
  TIMEOUT 30)

cmake_add_test(
  test_flow_resource_binding
  SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_resource_binding.c
    ${CMAKE_CURRENT_SOURCE_DIR}/resource_header_cpp.cpp
  LIBS TurboFlow::Product Salts::Plugin Salts::TinyTest
  FOLDER "turbo_flow/tests")
add_dependencies(test_flow_resource_binding
  test_flow_salts_resource_fixture)
target_compile_definitions(test_flow_resource_binding PRIVATE
  FLOW_SALTS_RESOURCE_FIXTURE="$<TARGET_FILE:test_flow_salts_resource_fixture>")
set_target_properties(test_flow_resource_binding PROPERTIES
  C_STANDARD 11
  C_STANDARD_REQUIRED ON
  C_EXTENSIONS OFF
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED ON
  CXX_EXTENSIONS OFF)
set_tests_properties(test_flow_resource_binding PROPERTIES
  LABELS "graph;resource;plugin;interface;lease"
  TIMEOUT 30)


cmake_add_test(
  test_turbo_flow_diagnostic
  SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/test_turbo_flow_diagnostic.c
    ${CMAKE_CURRENT_SOURCE_DIR}/diagnostic_header_cpp.cpp
  LIBS TurboFlow::Graph Salts::TinyTest
  FOLDER "turbo_flow/tests")
set_tests_properties(test_turbo_flow_diagnostic
  PROPERTIES LABELS "graph;api;diagnostic" TIMEOUT 30)

cmake_add_test(
  test_flow_provider_config
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_provider_config.c
  LIBS TurboFlow::Product Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../src
  FOLDER "turbo_flow/tests")
set_tests_properties(test_flow_provider_config
  PROPERTIES LABELS "graph;config;databind" TIMEOUT 30)

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
  test_flow_reflected_operation
  SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_reflected_operation.c
    ${CMAKE_CURRENT_SOURCE_DIR}/reflected_operation_header_cpp.cpp
  LIBS TurboFlow::Graph Salts::CFlow Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../src
  FOLDER "turbo_flow/tests")
set_target_properties(test_flow_reflected_operation PROPERTIES
  C_STANDARD 11
  C_STANDARD_REQUIRED ON
  C_EXTENSIONS OFF
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED ON)
set_tests_properties(test_flow_reflected_operation
  PROPERTIES LABELS "graph;core" TIMEOUT 30)

cmake_add_test(
  test_flow_plan_introspection
  SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_plan_introspection.c
    ${CMAKE_CURRENT_SOURCE_DIR}/plan_introspection_header_cpp.cpp
  LIBS TurboFlow::Graph Salts::CFlow Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../src
  FOLDER "turbo_flow/tests")
set_target_properties(test_flow_plan_introspection PROPERTIES
  C_STANDARD 11
  C_STANDARD_REQUIRED ON
  C_EXTENSIONS OFF
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED ON)
set_tests_properties(test_flow_plan_introspection
  PROPERTIES LABELS "graph;core;plan" TIMEOUT 30)

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
  test_flow_durable_lifecycle
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_durable_lifecycle.c
  LIBS TurboFlow::Graph Salts::TinyTest
  FOLDER "turbo_flow/tests")
set_tests_properties(test_flow_durable_lifecycle PROPERTIES LABELS "graph;core" TIMEOUT 30)


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
  test_flow_transport_reply
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_transport_reply.c
          ${CMAKE_CURRENT_SOURCE_DIR}/transport_reply_header_cpp.cpp
  LIBS TurboFlow::Graph Salts::TinyTest
  FOLDER "turbo_flow/tests")
set_tests_properties(test_flow_transport_reply
  PROPERTIES LABELS "graph;core;transport-reply" TIMEOUT 30)

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

if(TARGET tf_rulesforge_provider)
  cmake_add_test(
    test_flow_rulesforge_plugin
    SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_rulesforge_plugin.c
    LIBS TurboFlow::PluginHost Salts::TinyTest
    INCLUDES ${PROJECT_SOURCE_DIR}/plugins/rulesforge/include
    FOLDER "turbo_flow/tests")
  target_compile_definitions(test_flow_rulesforge_plugin PRIVATE
    FLOW_RULESFORGE_PLUGIN="$<TARGET_FILE:tf_rulesforge_provider>")
  add_dependencies(test_flow_rulesforge_plugin tf_rulesforge_provider)

  if(TARGET tf_cnet_plugin AND TARGET tf_durable_memory_plugin)
    cmake_add_test(
      test_flow_rulesforge_durable_network
      SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_rulesforge_durable_network.c
      LIBS TurboFlow::PluginHost Salts::TinyTest Salts::CNet
      INCLUDES ${PROJECT_SOURCE_DIR}/plugins/rulesforge/include
      FOLDER "turbo_flow/tests")
    target_compile_definitions(test_flow_rulesforge_durable_network PRIVATE
      FLOW_RULESFORGE_PLUGIN="$<TARGET_FILE:tf_rulesforge_provider>"
      TURBO_FLOW_CNET_PLUGIN="$<TARGET_FILE:tf_cnet_plugin>"
      TURBO_FLOW_DURABLE_MEMORY_PLUGIN="$<TARGET_FILE:tf_durable_memory_plugin>")
    add_dependencies(test_flow_rulesforge_durable_network
      tf_rulesforge_provider tf_cnet_plugin tf_durable_memory_plugin)
    set_tests_properties(test_flow_rulesforge_durable_network PROPERTIES
      LABELS "graph;plugin;abi3;materializer;durable;cnet" TIMEOUT 30)
  endif()
endif()

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
  test_materializer_binding_config
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_materializer_binding_config.c
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

function(turbo_flow_add_materializer_fixture target_name)
  add_library(${target_name} SHARED plugin_materializer_fixture.c)
  target_link_libraries(${target_name} PRIVATE TurboFlow::PluginHost Salts::CMeta)
  target_compile_definitions(${target_name} PRIVATE TURBO_FLOW_PLUGIN_BUILD ${ARGN})
  set_target_properties(
    ${target_name}
    PROPERTIES C_VISIBILITY_PRESET hidden
               VISIBILITY_INLINES_HIDDEN YES
               FOLDER "turbo_flow/tests/plugin-fixtures")
endfunction()

turbo_flow_add_materializer_fixture(test_flow_plugin_materializer_good)
turbo_flow_add_materializer_fixture(
  test_flow_plugin_materializer_second
  FLOW_MATERIALIZER_FIXTURE_ID="fixture.materializer.second")
turbo_flow_add_materializer_fixture(
  test_flow_plugin_materializer_duplicate
  FLOW_MATERIALIZER_FIXTURE_ID="fixture.materializer.duplicate"
  FLOW_MATERIALIZER_MODE=1)
turbo_flow_add_materializer_fixture(
  test_flow_plugin_materializer_bad_schema_version
  FLOW_MATERIALIZER_FIXTURE_ID="fixture.materializer.bad-schema-version"
  FLOW_MATERIALIZER_MODE=2)
turbo_flow_add_materializer_fixture(
  test_flow_plugin_materializer_bad_native_size
  FLOW_MATERIALIZER_FIXTURE_ID="fixture.materializer.bad-native-size"
  FLOW_MATERIALIZER_MODE=3)
turbo_flow_add_materializer_fixture(
  test_flow_plugin_materializer_bad_max_encoded
  FLOW_MATERIALIZER_FIXTURE_ID="fixture.materializer.bad-max-encoded"
  FLOW_MATERIALIZER_MODE=4)
turbo_flow_add_materializer_fixture(
  test_flow_plugin_materializer_missing_callback
  FLOW_MATERIALIZER_FIXTURE_ID="fixture.materializer.missing-callback"
  FLOW_MATERIALIZER_MODE=5)
turbo_flow_add_materializer_fixture(
  test_flow_plugin_materializer_swallow
  FLOW_MATERIALIZER_FIXTURE_ID="fixture.materializer.swallow"
  FLOW_MATERIALIZER_MODE=6)
turbo_flow_add_materializer_fixture(
  test_flow_plugin_materializer_overaligned
  FLOW_MATERIALIZER_FIXTURE_ID="fixture.materializer.overaligned"
  FLOW_MATERIALIZER_MODE=7)
turbo_flow_add_materializer_fixture(
  test_flow_plugin_materializer_bad_abi
  FLOW_MATERIALIZER_FIXTURE_ID="fixture.materializer.bad-abi"
  FLOW_MATERIALIZER_DESCRIPTOR_MINOR=99)
turbo_flow_add_materializer_fixture(
  test_flow_plugin_materializer_old_abi
  FLOW_MATERIALIZER_FIXTURE_ID="fixture.materializer.old-abi"
  FLOW_MATERIALIZER_ABI_MINOR=0)
turbo_flow_add_materializer_fixture(
  test_flow_plugin_materializer_version_two
  FLOW_MATERIALIZER_FIXTURE_ID="fixture.materializer.version-two"
  FLOW_MATERIALIZER_SCHEMA_VERSION=2)

turbo_flow_add_materializer_fixture(
  test_flow_plugin_materializer_operation
  FLOW_MATERIALIZER_FIXTURE_ID="fixture.materializer.operation"
  FLOW_MATERIALIZER_SCHEMA_ID="cmeta.int.data"
  FLOW_MATERIALIZER_TYPE_NAME="Integer"
  FLOW_MATERIALIZER_SCHEMA_NUMERIC_ID=7)
turbo_flow_add_materializer_fixture(
  test_flow_plugin_materializer_mismatch
  FLOW_MATERIALIZER_FIXTURE_ID="fixture.materializer.operation"
  FLOW_MATERIALIZER_SCHEMA_ID="cmeta.int.data"
  FLOW_MATERIALIZER_TYPE_NAME="Integer"
  FLOW_MATERIALIZER_SCHEMA_NUMERIC_ID=99)
turbo_flow_add_materializer_fixture(
  test_flow_plugin_materializer_unused
  FLOW_MATERIALIZER_FIXTURE_ID="fixture.materializer.unused"
  FLOW_MATERIALIZER_SCHEMA_ID="fixture.unused.data"
  FLOW_MATERIALIZER_TYPE_NAME="Integer"
  FLOW_MATERIALIZER_SCHEMA_NUMERIC_ID=15999)

cmake_add_test(
  test_flow_plugin_materializer
  SOURCES test_flow_plugin_materializer.c plugin_materializer_header_cpp.cpp
  LIBS TurboFlow::PluginHost Salts::CMeta Salts::TinyTest
  FOLDER "turbo_flow/tests")
target_compile_definitions(test_flow_plugin_materializer PRIVATE
  FLOW_MATERIALIZER_GOOD="$<TARGET_FILE:test_flow_plugin_materializer_good>"
  FLOW_MATERIALIZER_SECOND="$<TARGET_FILE:test_flow_plugin_materializer_second>"
  FLOW_MATERIALIZER_DUPLICATE="$<TARGET_FILE:test_flow_plugin_materializer_duplicate>"
  FLOW_MATERIALIZER_BAD_SCHEMA_VERSION="$<TARGET_FILE:test_flow_plugin_materializer_bad_schema_version>"
  FLOW_MATERIALIZER_BAD_NATIVE_SIZE="$<TARGET_FILE:test_flow_plugin_materializer_bad_native_size>"
  FLOW_MATERIALIZER_BAD_MAX_ENCODED="$<TARGET_FILE:test_flow_plugin_materializer_bad_max_encoded>"
  FLOW_MATERIALIZER_MISSING_CALLBACK="$<TARGET_FILE:test_flow_plugin_materializer_missing_callback>"
  FLOW_MATERIALIZER_SWALLOW="$<TARGET_FILE:test_flow_plugin_materializer_swallow>"
  FLOW_MATERIALIZER_OVERALIGNED="$<TARGET_FILE:test_flow_plugin_materializer_overaligned>"
  FLOW_MATERIALIZER_BAD_ABI="$<TARGET_FILE:test_flow_plugin_materializer_bad_abi>"
  FLOW_MATERIALIZER_OLD_ABI="$<TARGET_FILE:test_flow_plugin_materializer_old_abi>"
  FLOW_MATERIALIZER_VERSION_TWO="$<TARGET_FILE:test_flow_plugin_materializer_version_two>")
add_dependencies(
  test_flow_plugin_materializer
  test_flow_plugin_materializer_good
  test_flow_plugin_materializer_second
  test_flow_plugin_materializer_duplicate
  test_flow_plugin_materializer_bad_schema_version
  test_flow_plugin_materializer_bad_native_size
  test_flow_plugin_materializer_bad_max_encoded
  test_flow_plugin_materializer_missing_callback
  test_flow_plugin_materializer_swallow
  test_flow_plugin_materializer_overaligned
  test_flow_plugin_materializer_bad_abi
  test_flow_plugin_materializer_old_abi
  test_flow_plugin_materializer_version_two)

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

add_executable(test_flow_plugin_projection_fixture_abi plugin_fixture_abi_test.c)
target_link_libraries(test_flow_plugin_projection_fixture_abi
  PRIVATE TurboFlow::PluginHost Salts::TinyTest test_flow_plugin_projection_fixture)
add_test(NAME test_flow_plugin_projection_fixture_abi
  COMMAND test_flow_plugin_projection_fixture_abi)
set_target_properties(test_flow_plugin_projection_fixture_abi
  PROPERTIES FOLDER "turbo_flow/tests")
set_tests_properties(test_flow_plugin_projection_fixture_abi
  PROPERTIES LABELS "plugin")

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
  test_flow_plugin_missing_symbol)

cmake_add_test(
  test_flow_plugin_generation
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flow_plugin_generation.c
  LIBS TurboFlow::PluginHost Salts::Plugin Salts::TinyTest
  FOLDER "turbo_flow/tests")
target_compile_definitions(
  test_flow_plugin_generation
  PRIVATE FLOW_SALTS_PROVIDER_FIXTURE="$<TARGET_FILE:test_flow_salts_provider_fixture>"
          FLOW_SALTS_RESOURCE_FIXTURE="$<TARGET_FILE:test_flow_salts_resource_fixture>")
add_dependencies(
  test_flow_plugin_generation
  test_flow_salts_provider_fixture
  test_flow_salts_resource_fixture)

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


# Protocol mapper PluginHost catalog (ABI 3.2).
foreach(_mapper_case IN ITEMS GOOD SECOND DUPLICATE MISSING_MAP)
  string(TOLOWER "${_mapper_case}" _mapper_case_lower)
  add_library(test_flow_protocol_mapper_fixture_${_mapper_case_lower} SHARED
    plugin_protocol_mapper_fixture.c)
  target_link_libraries(test_flow_protocol_mapper_fixture_${_mapper_case_lower}
    PRIVATE TurboFlow::PluginHost)
  target_compile_definitions(test_flow_protocol_mapper_fixture_${_mapper_case_lower}
    PRIVATE TURBO_FLOW_PLUGIN_BUILD)
  set_target_properties(test_flow_protocol_mapper_fixture_${_mapper_case_lower}
    PROPERTIES FOLDER "turbo_flow/tests/plugin-fixtures")
endforeach()

target_compile_definitions(test_flow_protocol_mapper_fixture_second PRIVATE
  FLOW_PROTOCOL_MAPPER_FIXTURE_ID="fixture.protocol-mapper.second"
  FLOW_PROTOCOL_MAPPER_NAME="fixture.mapper.second")
target_compile_definitions(test_flow_protocol_mapper_fixture_duplicate PRIVATE
  FLOW_PROTOCOL_MAPPER_FIXTURE_ID="fixture.protocol-mapper.duplicate"
  FLOW_PROTOCOL_MAPPER_MODE=1)
target_compile_definitions(test_flow_protocol_mapper_fixture_missing_map PRIVATE
  FLOW_PROTOCOL_MAPPER_FIXTURE_ID="fixture.protocol-mapper.missing-map"
  FLOW_PROTOCOL_MAPPER_MODE=2)

add_executable(test_flow_plugin_protocol_mapper
  test_flow_plugin_protocol_mapper.c
  plugin_protocol_mapper_header_cpp.cpp)
target_link_libraries(test_flow_plugin_protocol_mapper
  PRIVATE TurboFlow::PluginHost Salts::TinyTest)
target_compile_definitions(test_flow_plugin_protocol_mapper PRIVATE
  FLOW_PROTOCOL_MAPPER_GOOD="$<TARGET_FILE:test_flow_protocol_mapper_fixture_good>"
  FLOW_PROTOCOL_MAPPER_SECOND="$<TARGET_FILE:test_flow_protocol_mapper_fixture_second>"
  FLOW_PROTOCOL_MAPPER_DUPLICATE="$<TARGET_FILE:test_flow_protocol_mapper_fixture_duplicate>"
  FLOW_PROTOCOL_MAPPER_MISSING_MAP="$<TARGET_FILE:test_flow_protocol_mapper_fixture_missing_map>")
add_dependencies(test_flow_plugin_protocol_mapper
  test_flow_protocol_mapper_fixture_good
  test_flow_protocol_mapper_fixture_second
  test_flow_protocol_mapper_fixture_duplicate
  test_flow_protocol_mapper_fixture_missing_map)
set_target_properties(test_flow_plugin_protocol_mapper
  PROPERTIES FOLDER "turbo_flow/tests")
add_test(NAME test_flow_plugin_protocol_mapper
  COMMAND test_flow_plugin_protocol_mapper)
set_tests_properties(test_flow_plugin_protocol_mapper
  PROPERTIES LABELS "graph;plugin;abi3;protocol-mapper" TIMEOUT 30)
