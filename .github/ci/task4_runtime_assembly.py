from pathlib import Path
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--turbodb", action="store_true")
args = parser.parse_args()

root = Path("CMakeLists.txt")
text = root.read_text()
assert "set(_turbo_flow_required_dependency_roots\n    SALTS_ROOT SALTS_UTILS_ROOT DATABIND_ROOT)" in text
assert "include(TurboFlowRequireCHTTP)" in text
text = text.replace("include(TurboFlowRequireCHTTP)\n", "", 1)
start = text.index("add_subdirectory(turbo_flow)\n")
end = text.index("set(TURBO_FLOW_EXPORT_TARGETS)", start)
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
