from pathlib import Path

root = Path("target/CMakeLists.txt")
text = root.read_text()
old = "set(_turbo_flow_required_dependency_roots\n    SALTS_ROOT SALTS_UTILS_ROOT RULES_FORGE_ROOT)"
new = "set(_turbo_flow_required_dependency_roots\n    SALTS_ROOT SALTS_UTILS_ROOT)"
assert old in text
text = text.replace(old, new, 1)
assert "include(TurboFlowRequireCHTTP)" in text
text = text.replace("include(TurboFlowRequireCHTTP)\n", "", 1)
start = text.index("add_subdirectory(turbo_flow)\n")
end = text.index("set(TURBO_FLOW_EXPORT_TARGETS)", start)
text = text[:start] + "add_subdirectory(turbo_flow)\n\n" + text[end:]
root.write_text(text)

graph = Path("target/turbo_flow/CMakeLists.txt")
text = graph.read_text()
rules = (
    'find_package(RulesForge 0.9 CONFIG REQUIRED\n'
    '             PATHS "$ENV{RULES_FORGE_ROOT}" NO_DEFAULT_PATH)\n'
)
assert rules in text
text = text.replace(rules, "", 1)
marker = "file(GLOB TURBO_FLOW_SOURCES CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/*.c)\n"
assert marker in text
text = text.replace(
    marker,
    marker
    + "list(REMOVE_ITEM TURBO_FLOW_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/src/flow_rulesforge.c)\n",
    1,
)
assert "Salts::CMeta RulesForge::RulesForge" in text
text = text.replace("Salts::CMeta RulesForge::RulesForge", "Salts::CMeta", 1)

insertion = "\n".join(
    (
        "",
        "cmake_add_test(",
        "  test_flow_durable_buffer",
        "  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_flow_durable_buffer.c",
        "          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_flow_durable_identity.c",
        "          ${CMAKE_CURRENT_SOURCE_DIR}/tests/durable_identity_header_cpp.cpp",
        "  LIBS TurboFlow::Graph Salts::TinyTest",
        '  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/src',
        '  FOLDER "turbo_flow/tests")',
        "",
        "cmake_add_test(",
        "  test_flow_data_schema",
        "  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_flow_data_schema.c",
        "          ${CMAKE_CURRENT_SOURCE_DIR}/tests/operation_schema_fixture.c",
        "  LIBS TurboFlow::Graph Salts::TinyTest",
        '  FOLDER "turbo_flow/tests")',
        "",
        "cmake_add_test(",
        "  test_flow_projection_owner",
        "  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_flow_projection_owner.c",
        "          ${CMAKE_CURRENT_SOURCE_DIR}/tests/projection_header_cpp.cpp",
        "  LIBS TurboFlow::Graph Salts::TinyTest",
        '  FOLDER "turbo_flow/tests")',
        "",
        "return()",
        "",
        "",
    )
)
anchor = "add_library(turbo_flow_product SHARED\n"
assert anchor in text
text = text.replace(anchor, insertion + anchor, 1)
graph.write_text(text)
