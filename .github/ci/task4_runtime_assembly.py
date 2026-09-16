from pathlib import Path

root = Path("CMakeLists.txt")
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

graph = Path("turbo_flow/CMakeLists.txt")
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
        "  LIBS TurboFlow::Graph Salts::TinyTest",
        '  FOLDER "turbo_flow/tests")',
        "",
        "foreach(_test IN ITEMS test_flow_inbox test_flow_inbox_source test_flow_run)",
        "  cmake_add_test(${_test}",
        "    SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/tests/${_test}.c",
        "    LIBS TurboFlow::Graph Salts::TinyTest",
        '    FOLDER "turbo_flow/tests")',
        "  target_include_directories(${_test} PRIVATE ${PROJECT_SOURCE_DIR}/tests)",
        "endforeach()",
        "target_sources(test_flow_run PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests/flow_run_header_cpp.cpp)",
        "target_sources(test_flow_inbox_source PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests/inbox_source_header_cpp.cpp)",
        "return()",
        "",
        "",
    )
)
anchor = "add_library(turbo_flow_product SHARED\n"
assert anchor in text
text = text.replace(anchor, insertion + anchor, 1)
graph.write_text(text)
