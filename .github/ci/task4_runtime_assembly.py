from pathlib import Path
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--turbodb", action="store_true")
parser.add_argument("--rulesforge", action="store_true")
parser.add_argument("--rulesforge-e2e", action="store_true")
parser.add_argument("--materializer", action="store_true")
parser.add_argument("--protocol-intake", action="store_true")
parser.add_argument("--benchmark", action="store_true")
args = parser.parse_args()
if sum((args.turbodb, args.rulesforge, args.rulesforge_e2e, args.materializer,
        args.protocol_intake, args.benchmark)) > 1:
    parser.error("--turbodb, --rulesforge, --rulesforge-e2e, --materializer, --protocol-intake and --benchmark are mutually exclusive")

root = Path("CMakeLists.txt")
text = root.read_text()
assert "set(_turbo_flow_required_dependency_roots\n    SALTS_ROOT SALTS_UTILS_ROOT)" in text
assert "DATABIND_ROOT" not in text
assert "include(TurboFlowRequireCHTTP)" in text
text = text.replace("include(TurboFlowRequireCHTTP)\n", "", 1)
start = text.index("add_subdirectory(turbo_flow)\n")
end = text.index("set(TURBO_FLOW_EXPORT_TARGETS)", start)
if args.benchmark:
    children = (
        "add_subdirectory(ingress/protocol/common)\n"
        "add_subdirectory(turbo_flow)\n"
    )
elif args.rulesforge:
    children = (
        "add_subdirectory(ingress/protocol/common)\n"
        "add_subdirectory(turbo_flow)\n"
        "add_subdirectory(plugins/rulesforge)\n"
        "\n"
        "cmake_add_test(\n"
        "  test_flow_rulesforge_plugin\n"
        "  SOURCES ${CMAKE_SOURCE_DIR}/turbo_flow/tests/test_flow_rulesforge_plugin.c\n"
        "  LIBS TurboFlow::PluginHost Salts::TinyTest\n"
        "  INCLUDES ${CMAKE_SOURCE_DIR}/plugins/rulesforge/include\n"
        '  FOLDER "turbo_flow/tests")\n'
        "target_compile_definitions(test_flow_rulesforge_plugin PRIVATE\n"
        '  FLOW_RULESFORGE_PLUGIN="$<TARGET_FILE:tf_rulesforge_provider>")\n'
        "add_dependencies(test_flow_rulesforge_plugin tf_rulesforge_provider)\n"
    )
elif args.rulesforge_e2e:
    children = (
        "add_subdirectory(ingress/protocol/common)\n"
        "add_subdirectory(turbo_flow)\n"
        "add_subdirectory(io/cnet)\n"
        "add_subdirectory(io/durable)\n"
        "add_subdirectory(ingress/protocol/inbox)\n"
        "add_subdirectory(ingress/protocol/network)\n"
        "add_subdirectory(plugins/rulesforge)\n"
        "add_subdirectory(plugins/applicant_mapper)\n"
        "\n"
        "add_library(tf_protocol_plugin_support STATIC\n"
        "  ${CMAKE_SOURCE_DIR}/ingress/protocol/src/flow_protocol_plugin_support.c\n"
        "  ${CMAKE_SOURCE_DIR}/ingress/protocol/src/flow_protocol_plugin_support.h)\n"
        "set_target_properties(tf_protocol_plugin_support PROPERTIES POSITION_INDEPENDENT_CODE ON)\n"
        "target_include_directories(tf_protocol_plugin_support PUBLIC ${CMAKE_SOURCE_DIR}/ingress/protocol/src)\n"
        "target_link_libraries(tf_protocol_plugin_support PUBLIC TurboFlow::ProtocolIngress)\n"
        "function(turbo_flow_ci_add_protocol target_name source_file)\n"
        "  add_library(${target_name} SHARED ${source_file})\n"
        "  target_compile_definitions(${target_name} PRIVATE TURBO_FLOW_BUILD TURBO_FLOW_PLUGIN_BUILD)\n"
        "  target_include_directories(${target_name} PRIVATE ${CMAKE_SOURCE_DIR}/ingress/protocol/src)\n"
        "  target_link_libraries(${target_name} PRIVATE tf_protocol_plugin_support TurboFlow::ProtocolIngress)\n"
        "  set_target_properties(${target_name} PROPERTIES C_VISIBILITY_PRESET hidden VISIBILITY_INLINES_HIDDEN YES)\n"
        "endfunction()\n"
        "turbo_flow_ci_add_protocol(tf_protocol_jtt808 ${CMAKE_SOURCE_DIR}/ingress/protocol/jtt808/flow_protocol_jtt808.c)\n"
        "turbo_flow_ci_add_protocol(tf_protocol_coap ${CMAKE_SOURCE_DIR}/ingress/protocol/coap/flow_protocol_coap.c)\n"
        "install(TARGETS tf_protocol_jtt808 tf_protocol_coap RUNTIME DESTINATION bin LIBRARY DESTINATION lib ARCHIVE DESTINATION lib)\n"
        "\n"
        "cmake_add_test(\n"
        "  test_flow_rulesforge_plugin\n"
        "  SOURCES ${CMAKE_SOURCE_DIR}/turbo_flow/tests/test_flow_rulesforge_plugin.c\n"
        "  LIBS TurboFlow::PluginHost Salts::TinyTest\n"
        "  INCLUDES ${CMAKE_SOURCE_DIR}/plugins/rulesforge/include\n"
        '  FOLDER "turbo_flow/tests")\n'
        "target_compile_definitions(test_flow_rulesforge_plugin PRIVATE\n"
        '  FLOW_RULESFORGE_PLUGIN="$<TARGET_FILE:tf_rulesforge_provider>")\n'
        "add_dependencies(test_flow_rulesforge_plugin tf_rulesforge_provider)\n"
        "\n"
        "cmake_add_test(\n"
        "  test_flow_rulesforge_durable_network\n"
        "  SOURCES ${CMAKE_SOURCE_DIR}/turbo_flow/tests/test_flow_rulesforge_durable_network.c\n"
        "  LIBS TurboFlow::PluginHost TurboFlow::ProtocolNetworkIntake Salts::TinyTest Salts::CNet\n"
        "  INCLUDES ${CMAKE_SOURCE_DIR}/plugins/rulesforge/include\n"
        "           ${CMAKE_SOURCE_DIR}/plugins/applicant_mapper/include\n"
        '  FOLDER "turbo_flow/tests")\n'
        "target_compile_definitions(test_flow_rulesforge_durable_network PRIVATE\n"
        '  FLOW_RULESFORGE_PLUGIN="$<TARGET_FILE:tf_rulesforge_provider>"\n'
        '  TURBO_FLOW_CNET_PLUGIN="$<TARGET_FILE:tf_cnet_plugin>"\n'
        '  TURBO_FLOW_DURABLE_MEMORY_PLUGIN="$<TARGET_FILE:tf_durable_memory_plugin>"\n'
        '  FLOW_PROTOCOL_JTT808_PLUGIN="$<TARGET_FILE:tf_protocol_jtt808>"\n'
        '  FLOW_PROTOCOL_COAP_PLUGIN="$<TARGET_FILE:tf_protocol_coap>"\n'
        '  FLOW_APPLICANT_MAPPER_PLUGIN="$<TARGET_FILE:tf_applicant_mapper_provider>")\n'
        "add_dependencies(test_flow_rulesforge_durable_network\n"
        "  tf_rulesforge_provider tf_cnet_plugin tf_durable_memory_plugin\n"
        "  tf_protocol_jtt808 tf_protocol_coap tf_applicant_mapper_provider)\n"
    )
elif args.protocol_intake:
    children = (
        "add_subdirectory(ingress/protocol/common)\n"
        "add_subdirectory(turbo_flow)\n"
        "add_subdirectory(io/cnet)\n"
        "add_subdirectory(io/durable)\n"
        "add_subdirectory(ingress/protocol/inbox)\n"
        "add_subdirectory(ingress/protocol/network)\n"
        "add_subdirectory(plugins/applicant_mapper)\n"
        "\n"
        "add_library(tf_protocol_plugin_support STATIC\n"
        "  ${CMAKE_SOURCE_DIR}/ingress/protocol/src/flow_protocol_plugin_support.c\n"
        "  ${CMAKE_SOURCE_DIR}/ingress/protocol/src/flow_protocol_plugin_support.h)\n"
        "set_target_properties(tf_protocol_plugin_support PROPERTIES POSITION_INDEPENDENT_CODE ON)\n"
        "target_include_directories(tf_protocol_plugin_support PUBLIC ${CMAKE_SOURCE_DIR}/ingress/protocol/src)\n"
        "target_link_libraries(tf_protocol_plugin_support PUBLIC TurboFlow::ProtocolIngress)\n"
        "add_library(tf_protocol_jtt808 SHARED ${CMAKE_SOURCE_DIR}/ingress/protocol/jtt808/flow_protocol_jtt808.c)\n"
        "target_compile_definitions(tf_protocol_jtt808 PRIVATE TURBO_FLOW_BUILD TURBO_FLOW_PLUGIN_BUILD)\n"
        "target_include_directories(tf_protocol_jtt808 PRIVATE ${CMAKE_SOURCE_DIR}/ingress/protocol/src)\n"
        "target_link_libraries(tf_protocol_jtt808 PRIVATE tf_protocol_plugin_support TurboFlow::ProtocolIngress)\n"
        "set_target_properties(tf_protocol_jtt808 PROPERTIES C_VISIBILITY_PRESET hidden VISIBILITY_INLINES_HIDDEN YES)\n"
        "\n"
        "cmake_add_test(\n"
        "  test_protocol_network_intake_config\n"
        "  SOURCES ${CMAKE_SOURCE_DIR}/ingress/protocol/tests/test_protocol_network_intake_config.c\n"
        "  LIBS tf_protocol_network_intake_core Salts::TinyTest\n"
        "  INCLUDES ${CMAKE_SOURCE_DIR}/ingress/protocol/network/src\n"
        '  FOLDER "ingress/protocol/tests")\n'
        "cmake_add_test(\n"
        "  test_protocol_network_intake_core\n"
        "  SOURCES ${CMAKE_SOURCE_DIR}/ingress/protocol/tests/test_protocol_network_intake_core.c\n"
        "  LIBS tf_protocol_network_intake_core TurboFlow::CNetAdapter Salts::TinyTest\n"
        "  INCLUDES ${CMAKE_SOURCE_DIR}/ingress/protocol/network/src\n"
        '  FOLDER "ingress/protocol/tests")\n'
        "add_executable(test_turbo_flow_focus\n"
        "  ${CMAKE_SOURCE_DIR}/turbo_flow/tests/test_turbo_flow.c\n"
        "  ${CMAKE_SOURCE_DIR}/turbo_flow/tests/flow_plan_type_probe.c)\n"
        "target_link_libraries(test_turbo_flow_focus PRIVATE TurboFlow::Graph Salts::CFlow Salts::TinyTest)\n"
        "target_include_directories(test_turbo_flow_focus PRIVATE ${CMAKE_SOURCE_DIR}/turbo_flow/src)\n"
        "set_target_properties(test_turbo_flow_focus PROPERTIES FOLDER \"turbo_flow/tests\")\n"
        "add_test(NAME test_turbo_flow_transport_vectors COMMAND test_turbo_flow_focus --filter \"transport vectors\")\n"
        "add_test(NAME test_turbo_flow_use_aliases COMMAND test_turbo_flow_focus --filter \"use aliases\")\n"
        "add_test(NAME test_turbo_flow_legacy_cflow_boundary COMMAND test_turbo_flow_focus --filter \"legacy typed operations\")\n"
        "add_test(NAME test_turbo_flow_source_only_cflow COMMAND test_turbo_flow_focus --filter \"source-only plan\")\n"
        "cmake_add_test(\n"
        "  test_flow_transport_reply\n"
        "  SOURCES ${CMAKE_SOURCE_DIR}/turbo_flow/tests/test_flow_transport_reply.c\n"
        "          ${CMAKE_SOURCE_DIR}/turbo_flow/tests/transport_reply_header_cpp.cpp\n"
        "  LIBS TurboFlow::Graph Salts::TinyTest\n"
        '  FOLDER "turbo_flow/tests")\n'
        "cmake_add_test(\n"
        "  test_protocol_network_jtt808\n"
        "  SOURCES ${CMAKE_SOURCE_DIR}/ingress/protocol/tests/test_protocol_network_jtt808.c\n"
        "          ${CMAKE_SOURCE_DIR}/ingress/protocol/tests/protocol_network_e2e_fixture.c\n"
        "  LIBS TurboFlow::ProtocolNetworkIntake TurboFlow::PluginHost Salts::CNet Salts::TinyTest\n"
        "  DEFS FLOW_CNET_PLUGIN_MODULE=\"$<TARGET_FILE:tf_cnet_plugin>\"\n"
        "       FLOW_PROTOCOL_JTT808_MODULE=\"$<TARGET_FILE:tf_protocol_jtt808>\"\n"
        "       FLOW_DURABLE_MEMORY_PLUGIN_MODULE=\"$<TARGET_FILE:tf_durable_memory_plugin>\"\n"
        '  FOLDER "ingress/protocol/tests")\n'
        "add_dependencies(test_protocol_network_jtt808 tf_cnet_plugin tf_protocol_jtt808 tf_durable_memory_plugin)\n"
    )
elif args.materializer:
    children = (
        "add_subdirectory(ingress/protocol/common)\n"
        "add_subdirectory(turbo_flow)\n"
        "\n"
        "salts_idl_target(\n"
        "  TARGET turbo_flow_databind_service_fixture\n"
        "  IDL ${CMAKE_SOURCE_DIR}/turbo_flow/tests/databind_service.schema\n"
        "  ARTIFACT_NAME tf_service\n"
        "  ARTIFACTS NATIVE\n"
        "  TRANSPORTS HTTP RPC)\n"
        "cmake_add_test(\n"
        "  test_flow_databind_service_plan\n"
        "  SOURCES ${CMAKE_SOURCE_DIR}/turbo_flow/tests/test_flow_databind_service_plan.c\n"
        "          ${CMAKE_SOURCE_DIR}/turbo_flow/tests/databind_service_header_cpp.cpp\n"
        "  LIBS TurboFlow::Product Salts::TinyTest ${turbo_flow_databind_service_fixture_NATIVE_TARGET}\n"
        "  INCLUDES ${CMAKE_SOURCE_DIR}/turbo_flow/src ${turbo_flow_databind_service_fixture_GENERATED_DIR}\n"
        "  FOLDER \"turbo_flow/tests\")\n"
        "add_dependencies(test_flow_databind_service_plan turbo_flow_databind_service_fixture_idl_codegen)\n"
        "set_target_properties(test_flow_databind_service_plan PROPERTIES\n"
        "  C_STANDARD 11 C_STANDARD_REQUIRED ON C_EXTENSIONS OFF\n"
        "  CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON CXX_EXTENSIONS OFF)\n"
        "set_tests_properties(test_flow_databind_service_plan PROPERTIES\n"
        "  LABELS \"graph;databind;plan;service\" TIMEOUT 30\n"
        "  FIXTURES_SETUP generated_databind_service_plan)\n"
        "\n"
        "function(turbo_flow_ci_add_materializer_fixture target_name)\n"
        "  add_library(${target_name} SHARED ${CMAKE_SOURCE_DIR}/turbo_flow/tests/plugin_materializer_fixture.c)\n"
        "  target_link_libraries(${target_name} PRIVATE TurboFlow::PluginHost Salts::CMeta)\n"
        "  target_compile_definitions(${target_name} PRIVATE TURBO_FLOW_PLUGIN_BUILD ${ARGN})\n"
        "  set_target_properties(${target_name} PROPERTIES C_VISIBILITY_PRESET hidden VISIBILITY_INLINES_HIDDEN YES)\n"
        "endfunction()\n"
        "turbo_flow_ci_add_materializer_fixture(test_flow_plugin_materializer_good)\n"
        "turbo_flow_ci_add_materializer_fixture(test_flow_plugin_materializer_second FLOW_MATERIALIZER_FIXTURE_ID=\\\"fixture.materializer.second\\\")\n"
        "turbo_flow_ci_add_materializer_fixture(test_flow_plugin_materializer_duplicate FLOW_MATERIALIZER_FIXTURE_ID=\\\"fixture.materializer.duplicate\\\" FLOW_MATERIALIZER_MODE=1)\n"
        "turbo_flow_ci_add_materializer_fixture(test_flow_plugin_materializer_bad_schema_version FLOW_MATERIALIZER_FIXTURE_ID=\\\"fixture.materializer.bad-schema-version\\\" FLOW_MATERIALIZER_MODE=2)\n"
        "turbo_flow_ci_add_materializer_fixture(test_flow_plugin_materializer_bad_native_size FLOW_MATERIALIZER_FIXTURE_ID=\\\"fixture.materializer.bad-native-size\\\" FLOW_MATERIALIZER_MODE=3)\n"
        "turbo_flow_ci_add_materializer_fixture(test_flow_plugin_materializer_bad_max_encoded FLOW_MATERIALIZER_FIXTURE_ID=\\\"fixture.materializer.bad-max-encoded\\\" FLOW_MATERIALIZER_MODE=4)\n"
        "turbo_flow_ci_add_materializer_fixture(test_flow_plugin_materializer_missing_callback FLOW_MATERIALIZER_FIXTURE_ID=\\\"fixture.materializer.missing-callback\\\" FLOW_MATERIALIZER_MODE=5)\n"
        "turbo_flow_ci_add_materializer_fixture(test_flow_plugin_materializer_swallow FLOW_MATERIALIZER_FIXTURE_ID=\\\"fixture.materializer.swallow\\\" FLOW_MATERIALIZER_MODE=6)\n"
        "turbo_flow_ci_add_materializer_fixture(test_flow_plugin_materializer_overaligned FLOW_MATERIALIZER_FIXTURE_ID=\\\"fixture.materializer.overaligned\\\" FLOW_MATERIALIZER_MODE=7)\n"
        "turbo_flow_ci_add_materializer_fixture(test_flow_plugin_materializer_bad_abi FLOW_MATERIALIZER_FIXTURE_ID=\\\"fixture.materializer.bad-abi\\\" FLOW_MATERIALIZER_DESCRIPTOR_MINOR=99)\n"
        "turbo_flow_ci_add_materializer_fixture(test_flow_plugin_materializer_old_abi FLOW_MATERIALIZER_FIXTURE_ID=\\\"fixture.materializer.old-abi\\\" FLOW_MATERIALIZER_ABI_MINOR=0)\n"
        "turbo_flow_ci_add_materializer_fixture(test_flow_plugin_materializer_version_two FLOW_MATERIALIZER_FIXTURE_ID=\\\"fixture.materializer.version-two\\\" FLOW_MATERIALIZER_SCHEMA_VERSION=2)\n"
        "cmake_add_test(\n"
        "  test_flow_plugin_materializer\n"
        "  SOURCES ${CMAKE_SOURCE_DIR}/turbo_flow/tests/test_flow_plugin_materializer.c ${CMAKE_SOURCE_DIR}/turbo_flow/tests/plugin_materializer_header_cpp.cpp\n"
        "  LIBS TurboFlow::PluginHost Salts::CMeta Salts::TinyTest\n"
        '  FOLDER "turbo_flow/tests")\n'
        "target_compile_definitions(test_flow_plugin_materializer PRIVATE\n"
        '  FLOW_MATERIALIZER_GOOD="$<TARGET_FILE:test_flow_plugin_materializer_good>"\n'
        '  FLOW_MATERIALIZER_SECOND="$<TARGET_FILE:test_flow_plugin_materializer_second>"\n'
        '  FLOW_MATERIALIZER_DUPLICATE="$<TARGET_FILE:test_flow_plugin_materializer_duplicate>"\n'
        '  FLOW_MATERIALIZER_BAD_SCHEMA_VERSION="$<TARGET_FILE:test_flow_plugin_materializer_bad_schema_version>"\n'
        '  FLOW_MATERIALIZER_BAD_NATIVE_SIZE="$<TARGET_FILE:test_flow_plugin_materializer_bad_native_size>"\n'
        '  FLOW_MATERIALIZER_BAD_MAX_ENCODED="$<TARGET_FILE:test_flow_plugin_materializer_bad_max_encoded>"\n'
        '  FLOW_MATERIALIZER_MISSING_CALLBACK="$<TARGET_FILE:test_flow_plugin_materializer_missing_callback>"\n'
        '  FLOW_MATERIALIZER_SWALLOW="$<TARGET_FILE:test_flow_plugin_materializer_swallow>"\n'
        '  FLOW_MATERIALIZER_OVERALIGNED="$<TARGET_FILE:test_flow_plugin_materializer_overaligned>"\n'
        '  FLOW_MATERIALIZER_BAD_ABI="$<TARGET_FILE:test_flow_plugin_materializer_bad_abi>"\n'
        '  FLOW_MATERIALIZER_OLD_ABI="$<TARGET_FILE:test_flow_plugin_materializer_old_abi>"\n'
        '  FLOW_MATERIALIZER_VERSION_TWO="$<TARGET_FILE:test_flow_plugin_materializer_version_two>")\n'
        "add_dependencies(test_flow_plugin_materializer\n"
        "  test_flow_plugin_materializer_good test_flow_plugin_materializer_second\n"
        "  test_flow_plugin_materializer_duplicate test_flow_plugin_materializer_bad_schema_version\n"
        "  test_flow_plugin_materializer_bad_native_size test_flow_plugin_materializer_bad_max_encoded\n"
        "  test_flow_plugin_materializer_missing_callback test_flow_plugin_materializer_swallow\n"
        "  test_flow_plugin_materializer_overaligned\n"
        "  test_flow_plugin_materializer_bad_abi test_flow_plugin_materializer_old_abi\n"
        "  test_flow_plugin_materializer_version_two)\n"
        "add_dependencies(test_flow_plugin_materializer test_flow_databind_service_plan)\n"
        "set_tests_properties(test_flow_plugin_materializer PROPERTIES FIXTURES_REQUIRED generated_databind_service_plan)\n"
        "turbo_flow_ci_add_materializer_fixture(test_flow_plugin_materializer_operation FLOW_MATERIALIZER_FIXTURE_ID=\\\"fixture.materializer.operation\\\" FLOW_MATERIALIZER_SCHEMA_ID=\\\"cmeta.int.data\\\" FLOW_MATERIALIZER_TYPE_NAME=\\\"Integer\\\" FLOW_MATERIALIZER_SCHEMA_NUMERIC_ID=7)\n"
        "turbo_flow_ci_add_materializer_fixture(test_flow_plugin_materializer_mismatch FLOW_MATERIALIZER_FIXTURE_ID=\\\"fixture.materializer.operation\\\" FLOW_MATERIALIZER_SCHEMA_ID=\\\"cmeta.int.data\\\" FLOW_MATERIALIZER_TYPE_NAME=\\\"Integer\\\" FLOW_MATERIALIZER_SCHEMA_NUMERIC_ID=99)\n"
        "turbo_flow_ci_add_materializer_fixture(test_flow_plugin_materializer_unused FLOW_MATERIALIZER_FIXTURE_ID=\\\"fixture.materializer.unused\\\" FLOW_MATERIALIZER_SCHEMA_ID=\\\"fixture.unused.data\\\" FLOW_MATERIALIZER_TYPE_NAME=\\\"Integer\\\" FLOW_MATERIALIZER_SCHEMA_NUMERIC_ID=15999)\n"
        "function(turbo_flow_ci_add_operation_fixture target_name mode)\n"
        "  add_library(${target_name} SHARED ${CMAKE_SOURCE_DIR}/turbo_flow/tests/plugin_operation_fixture.c)\n"
        "  target_link_libraries(${target_name} PRIVATE TurboFlow::PluginHost)\n"
        "  target_compile_definitions(${target_name} PRIVATE TURBO_FLOW_PLUGIN_BUILD FLOW_OPERATION_MODE=${mode})\n"
        "  set_target_properties(${target_name} PROPERTIES C_VISIBILITY_PRESET hidden VISIBILITY_INLINES_HIDDEN YES)\n"
        "endfunction()\n"
        "turbo_flow_ci_add_operation_fixture(test_flow_operation_fixture_OK OP_FIXTURE_OK)\n"
        "turbo_flow_ci_add_operation_fixture(test_flow_operation_fixture_TWO_NAMES OP_FIXTURE_TWO_NAMES)\n"
        "turbo_flow_ci_add_operation_fixture(test_flow_operation_fixture_EXECUTE_ALIAS OP_FIXTURE_EXECUTE_ALIAS)\n"
        "turbo_flow_ci_add_operation_fixture(test_flow_operation_fixture_EXECUTE_FAIL_ALIAS OP_FIXTURE_EXECUTE_FAIL_ALIAS)\n"
        "turbo_flow_ci_add_operation_fixture(test_flow_operation_fixture_MEMORY_OVERFLOW OP_FIXTURE_MEMORY_OVERFLOW)\n"
        "turbo_flow_ci_add_operation_fixture(test_flow_operation_fixture_OWNER_FAIL OP_FIXTURE_OWNER_FAIL)\n"
        'cmake_add_test(test_materializer_binding_config SOURCES ${CMAKE_SOURCE_DIR}/turbo_flow/tests/test_materializer_binding_config.c LIBS TurboFlow::Config Salts::TinyTest FOLDER "turbo_flow/tests")\n'
        'cmake_add_test(test_flow_plugin_materializer_generation SOURCES ${CMAKE_SOURCE_DIR}/turbo_flow/tests/test_flow_plugin_materializer_generation.c LIBS TurboFlow::PluginHost Salts::TinyTest FOLDER "turbo_flow/tests")\n'
        "cmake_add_test(\n"
        "  test_flow_reflected_operation\n"
        "  SOURCES ${CMAKE_SOURCE_DIR}/turbo_flow/tests/test_flow_reflected_operation.c\n"
        "          ${CMAKE_SOURCE_DIR}/turbo_flow/tests/reflected_operation_header_cpp.cpp\n"
        "  LIBS TurboFlow::Graph Salts::CFlow Salts::TinyTest\n"
        "  INCLUDES ${CMAKE_SOURCE_DIR}/turbo_flow/src\n"
        '  FOLDER "turbo_flow/tests")\n'
        "set_target_properties(test_flow_reflected_operation PROPERTIES\n"
        "  C_STANDARD 11 C_STANDARD_REQUIRED ON C_EXTENSIONS OFF\n"
        "  CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON)\n"
        "target_compile_definitions(test_flow_plugin_materializer_generation PRIVATE\n"
        "  FLOW_OPERATION_OK=\\\"$<TARGET_FILE:test_flow_operation_fixture_OK>\\\"\n"
        "  FLOW_OPERATION_TWO_NAMES=\\\"$<TARGET_FILE:test_flow_operation_fixture_TWO_NAMES>\\\"\n"
        "  FLOW_MATERIALIZER_OPERATION=\\\"$<TARGET_FILE:test_flow_plugin_materializer_operation>\\\"\n"
        "  FLOW_MATERIALIZER_MISMATCH=\\\"$<TARGET_FILE:test_flow_plugin_materializer_mismatch>\\\"\n"
        "  FLOW_MATERIALIZER_UNUSED=\\\"$<TARGET_FILE:test_flow_plugin_materializer_unused>\\\")\n"
        "add_dependencies(test_flow_plugin_materializer_generation test_flow_operation_fixture_OK test_flow_operation_fixture_TWO_NAMES test_flow_plugin_materializer_operation test_flow_plugin_materializer_mismatch test_flow_plugin_materializer_unused)\n"
        "cmake_add_test(test_flow_plugin_operation_runtime SOURCES ${CMAKE_SOURCE_DIR}/turbo_flow/tests/test_flow_plugin_operation_runtime.c LIBS TurboFlow::PluginHost Salts::TinyTest FOLDER \"turbo_flow/tests\")\n"
        "target_compile_definitions(test_flow_plugin_operation_runtime PRIVATE\n"
        "  FLOW_OPERATION_OK=\\\"$<TARGET_FILE:test_flow_operation_fixture_OK>\\\"\n"
        "  FLOW_OPERATION_EXECUTE_ALIAS=\\\"$<TARGET_FILE:test_flow_operation_fixture_EXECUTE_ALIAS>\\\"\n"
        "  FLOW_OPERATION_EXECUTE_FAIL_ALIAS=\\\"$<TARGET_FILE:test_flow_operation_fixture_EXECUTE_FAIL_ALIAS>\\\"\n"
        "  FLOW_OPERATION_MEMORY_OVERFLOW=\\\"$<TARGET_FILE:test_flow_operation_fixture_MEMORY_OVERFLOW>\\\"\n"
        "  FLOW_OPERATION_OWNER_FAIL=\\\"$<TARGET_FILE:test_flow_operation_fixture_OWNER_FAIL>\\\"\n"
        "  FLOW_OPERATION_TWO_NAMES=\\\"$<TARGET_FILE:test_flow_operation_fixture_TWO_NAMES>\\\")\n"
        "add_dependencies(test_flow_plugin_operation_runtime\n"
        "  test_flow_operation_fixture_OK test_flow_operation_fixture_EXECUTE_ALIAS\n"
        "  test_flow_operation_fixture_EXECUTE_FAIL_ALIAS test_flow_operation_fixture_MEMORY_OVERFLOW\n"
        "  test_flow_operation_fixture_OWNER_FAIL test_flow_operation_fixture_TWO_NAMES)\n"
    )
else:
    children = "add_subdirectory(ingress/protocol/common)\nadd_subdirectory(turbo_flow)\nadd_subdirectory(io/durable)\n"
    if args.turbodb:
        # The real Inbox tests use the protocol envelope and its generated schema.
        children += "add_subdirectory(ingress/protocol/inbox)\n"
        children += "add_subdirectory(io/turbodb)\n"
text = text[:start] + children + "\n" + text[end:]
root.write_text(text)

graph = Path("turbo_flow/CMakeLists.txt")
text = graph.read_text()
assert "RulesForge::RulesForge" not in text
assert "find_package(RulesForge" not in text

if args.benchmark:
    graph.write_text(text)
    raise SystemExit(0)

if args.rulesforge or args.rulesforge_e2e or args.materializer or args.protocol_intake:
    full_tests = """if(BUILD_TESTING)
  add_subdirectory(tests)
  add_subdirectory(benchmarks)
endif()
"""
    assert full_tests in text
    text = text.replace(full_tests, "", 1)
    graph.write_text(text)
    raise SystemExit(0)


insertion = "\n".join(
    (
        "",
        "cmake_add_test(",
        "  test_flow_durable_buffer",
        "  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_flow_durable_buffer.c",
        "  LIBS TurboFlow::Graph Salts::TinyTest",
        '  FOLDER "turbo_flow/tests")',
        "",
        "foreach(_test IN ITEMS test_flow_durable_admission test_flow_durable_lifecycle test_flow_inbox test_flow_inbox_source test_flow_inbox_driver test_flow_async_terminal test_flow_run test_flow_operation_result test_flow_projection_owner test_flow_projection_owner_fault)",
        "  cmake_add_test(${_test}",
        "    SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/tests/${_test}.c",
        "    LIBS TurboFlow::Graph Salts::TinyTest",
        '    FOLDER "turbo_flow/tests")',
        "  target_include_directories(${_test} PRIVATE ${PROJECT_SOURCE_DIR}/tests ${CMAKE_CURRENT_SOURCE_DIR}/src)",
        "endforeach()",
        "target_sources(test_flow_projection_owner PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests/projection_header_cpp.cpp)",
        "target_sources(test_flow_run PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests/flow_run_header_cpp.cpp)",
        "target_sources(test_flow_inbox_source PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests/inbox_source_header_cpp.cpp)",
        "",
        "cmake_add_test(",
        "  test_flow_provider_interface",
        "  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_flow_provider_interface.c",
        "          ${CMAKE_CURRENT_SOURCE_DIR}/tests/provider_interface_header_cpp.cpp",
        "  LIBS TurboFlow::Product Salts::Plugin Salts::TinyTest",
        '  FOLDER "turbo_flow/tests")',
        "set_tests_properties(test_flow_provider_interface",
        '  PROPERTIES LABELS "graph;provider;plugin;interface" TIMEOUT 30)',
        "",
        "add_library(test_flow_salts_provider_fixture SHARED",
        "  ${CMAKE_CURRENT_SOURCE_DIR}/tests/salts_provider_fixture.c)",
        "target_link_libraries(test_flow_salts_provider_fixture",
        "  PRIVATE TurboFlow::Product Salts::Plugin)",
        "set_target_properties(test_flow_salts_provider_fixture PROPERTIES",
        '  FOLDER "turbo_flow/tests/fixtures"',
        "  C_VISIBILITY_PRESET hidden",
        "  VISIBILITY_INLINES_HIDDEN YES)",
        "",
        "add_library(test_flow_salts_resource_fixture SHARED",
        "  ${CMAKE_CURRENT_SOURCE_DIR}/tests/salts_resource_fixture.c",
        "  ${CMAKE_CURRENT_SOURCE_DIR}/tests/salts_resource_fixture.h)",
        "target_link_libraries(test_flow_salts_resource_fixture",
        "  PRIVATE TurboFlow::Product Salts::Plugin)",
        "set_target_properties(test_flow_salts_resource_fixture PROPERTIES",
        '  FOLDER "turbo_flow/tests/fixtures"',
        "  C_VISIBILITY_PRESET hidden",
        "  VISIBILITY_INLINES_HIDDEN YES)",
        "",
        "cmake_add_test(",
        "  test_flow_salts_provider_binding",
        "  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_flow_salts_provider_binding.c",
        "  LIBS TurboFlow::Product Salts::Plugin Salts::TinyTest",
        '  FOLDER "turbo_flow/tests")',
        "add_dependencies(test_flow_salts_provider_binding",
        "  test_flow_salts_provider_fixture",
        "  test_flow_salts_resource_fixture)",
        "target_compile_definitions(test_flow_salts_provider_binding PRIVATE",
        '  FLOW_SALTS_PROVIDER_FIXTURE="$<TARGET_FILE:test_flow_salts_provider_fixture>"',
        '  FLOW_SALTS_RESOURCE_FIXTURE="$<TARGET_FILE:test_flow_salts_resource_fixture>")',
        "set_tests_properties(test_flow_salts_provider_binding PROPERTIES",
        '  LABELS "graph;provider;plugin;interface;lease" TIMEOUT 30)',
        "",
        "cmake_add_test(",
        "  test_flow_provider_binding",
        "  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_flow_provider_binding.c",
        "          ${CMAKE_CURRENT_SOURCE_DIR}/tests/provider_binding_header_cpp.cpp",
        "  LIBS TurboFlow::Product Salts::Plugin Salts::TinyTest",
        '  FOLDER "turbo_flow/tests")',
        "add_dependencies(test_flow_provider_binding",
        "  test_flow_salts_provider_fixture",
        "  test_flow_salts_resource_fixture)",
        "target_compile_definitions(test_flow_provider_binding PRIVATE",
        '  FLOW_SALTS_PROVIDER_FIXTURE="$<TARGET_FILE:test_flow_salts_provider_fixture>"',
        '  FLOW_SALTS_RESOURCE_FIXTURE="$<TARGET_FILE:test_flow_salts_resource_fixture>")',
        "set_target_properties(test_flow_provider_binding PROPERTIES",
        "  C_STANDARD 11 C_STANDARD_REQUIRED ON C_EXTENSIONS OFF",
        "  CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON CXX_EXTENSIONS OFF)",
        "set_tests_properties(test_flow_provider_binding PROPERTIES",
        '  LABELS "graph;provider;plugin;interface;lease" TIMEOUT 30)',

        "",
    )
)
# Reuse the committed generation fixtures and target registrations. Only CMake
# assembly/path anchoring changes; production code and assertions stay untouched.
tests = Path("turbo_flow/tests/CMakeLists.txt").read_text()
plugin_start = tests.index("function(turbo_flow_add_plugin_fixture target_name)")
plugin_end = tests.index("endfunction()", plugin_start) + len("endfunction()")
fixtures_start = tests.index("function(turbo_flow_add_generation_fixture target_name)")
fixtures_end = tests.index("cmake_add_test(\n  test_flow_plugin_host", fixtures_start)
fixtures = tests[fixtures_start:fixtures_end]
abi_start = fixtures.index("add_executable(test_flow_plugin_projection_fixture_abi")
abi_end = fixtures.index("turbo_flow_add_generation_fixture(\n  test_flow_plugin_generation_duplicate_reference", abi_start)
fixtures = fixtures[:abi_start] + fixtures[abi_end:]
generation_start = tests.index("cmake_add_test(\n  test_flow_plugin_generation\n")
generation_end = tests.index("add_library(test_flow_plugin_invalid_owner_fixture", generation_start)
registrations = (tests[plugin_start:plugin_end] + "\nturbo_flow_add_plugin_fixture(test_flow_plugin_good_one)\n"
                 + fixtures + tests[generation_start:generation_end])
registrations = registrations.replace("${CMAKE_CURRENT_SOURCE_DIR}/", "${CMAKE_CURRENT_SOURCE_DIR}/tests/")
for filename in ("plugin_fixture.c", "plugin_generation_fixture.c"):
    registrations = registrations.replace("SHARED " + filename, 'SHARED "${CMAKE_CURRENT_SOURCE_DIR}/tests/' + filename + '"')
insertion += "\n" + registrations + "\nreturn()\n"
anchor = "install(FILES ${TURBO_FLOW_HEADERS} DESTINATION include)"
assert anchor in text
text = text.replace(anchor, insertion + anchor, 1)
graph.write_text(text)

if args.turbodb:
    # SQLite Inbox tests exercise the canonical C codec, not RulesForge bindings.
    # Keep the schema and enum values intact; the full declarations gate remains
    # outside this focused assembly, which already excludes RulesForge.
    inbox = Path("ingress/protocol/inbox/CMakeLists.txt")
    text = inbox.read_text()
    for line in (
        '         "${_protocol_inbox_generated_rfl}"\n',
        '    --dsl-output "${_protocol_inbox_generated_rfl}"\n',
        '        "${_protocol_inbox_generated_rfl}"\n',
    ):
        assert line in text
        text = text.replace(line, "", 1)
    text = text.replace(
        '  DEPENDS "${_protocol_inbox_generated_rfl}"\n'
        '          "${_protocol_inbox_generated_ts}")',
        '  DEPENDS "${_protocol_inbox_generated_ts}")',
        1,
    )
    text = text.replace(
        'COMMENT "Generating canonical protocol Inbox TBE and RulesForge bindings"',
        'COMMENT "Generating canonical protocol Inbox TBE C bindings"',
        1,
    )
    inbox.write_text(text)
