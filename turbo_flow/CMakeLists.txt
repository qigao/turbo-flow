set(TARGET_NAME turbo_flow)

cmake_add_grammar(TURBO_FLOW
  LEXER_RE ${CMAKE_CURRENT_SOURCE_DIR}/parser/flow_lexer.re
  GRAMMAR_Y ${CMAKE_CURRENT_SOURCE_DIR}/parser/flow_grammar.y
  FOLDER "turbo_flow/flow")

cmake_add_grammar(TURBO_FLOW_EXPR
  LEXER_RE ${CMAKE_CURRENT_SOURCE_DIR}/parser/expr_lexer.re
  GRAMMAR_Y ${CMAKE_CURRENT_SOURCE_DIR}/parser/expr_grammar.y
  FOLDER "turbo_flow/expr")

cmake_add_grammar(TURBO_FLOW_CONTROL
  LEXER_RE ${CMAKE_CURRENT_SOURCE_DIR}/parser/control_lexer.re
  GRAMMAR_Y ${CMAKE_CURRENT_SOURCE_DIR}/parser/control_grammar.y
  FOLDER "turbo_flow/control")

cmake_add_grammar(TURBO_FLOW_SECURITY_RULE
  LEXER_RE ${CMAKE_CURRENT_SOURCE_DIR}/parser/security_rule_lexer.re
  FOLDER "turbo_flow/security")

if(MSVC)
  set_source_files_properties(${TURBO_FLOW_GRAMMAR_GEN} PROPERTIES COMPILE_OPTIONS "/wd4189")
  set_source_files_properties(${TURBO_FLOW_EXPR_GRAMMAR_GEN} PROPERTIES COMPILE_OPTIONS "/wd4189")
  set_source_files_properties(${TURBO_FLOW_CONTROL_GRAMMAR_GEN} PROPERTIES COMPILE_OPTIONS "/wd4189")
endif()

file(GLOB TURBO_FLOW_SOURCES CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/*.c)
set(TURBO_FLOW_HEADERS
  ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow.h
  ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_domain.h
  ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_control.h
  ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_expr.h)
list(APPEND TURBO_FLOW_HEADERS ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_policy.h)
list(APPEND TURBO_FLOW_HEADERS ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_protocol.h)
list(APPEND TURBO_FLOW_HEADERS ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_discovery.h)
list(APPEND TURBO_FLOW_HEADERS ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_config.h)
list(APPEND TURBO_FLOW_HEADERS ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_security.h)

add_library(${TARGET_NAME} SHARED
  ${TURBO_FLOW_SOURCES}
  ${TURBO_FLOW_LEXER_GEN}
  ${TURBO_FLOW_GRAMMAR_GEN}
  ${TURBO_FLOW_EXPR_LEXER_GEN}
  ${TURBO_FLOW_EXPR_GRAMMAR_GEN}
  ${TURBO_FLOW_CONTROL_LEXER_GEN}
  ${TURBO_FLOW_CONTROL_GRAMMAR_GEN}
  ${TURBO_FLOW_SECURITY_RULE_LEXER_GEN}
  ${TURBO_FLOW_HEADERS})

add_dependencies(${TARGET_NAME}
  ${TURBO_FLOW_LEXER_TARGET}
  ${TURBO_FLOW_GRAMMAR_TARGET}
  ${TURBO_FLOW_EXPR_LEXER_TARGET}
  ${TURBO_FLOW_EXPR_GRAMMAR_TARGET})
add_dependencies(${TARGET_NAME}
  ${TURBO_FLOW_CONTROL_LEXER_TARGET}
  ${TURBO_FLOW_CONTROL_GRAMMAR_TARGET})
add_dependencies(${TARGET_NAME} ${TURBO_FLOW_SECURITY_RULE_LEXER_TARGET})

cmake_config_target(
  ${TARGET_NAME}
  ALIAS TurboFlow::Flow
  FOLDER "turbo_flow"
  EXPORT_NAME Flow)

target_compile_definitions(${TARGET_NAME} PRIVATE SHARED_CXX)
if(TURBO_FLOW_EXPR_ENABLE_JIT)
  target_compile_definitions(${TARGET_NAME} PRIVATE TURBO_FLOW_EXPR_ENABLE_JIT=1)
else()
  target_compile_definitions(${TARGET_NAME} PRIVATE TURBO_FLOW_EXPR_ENABLE_JIT=0)
endif()

target_include_directories(
  ${TARGET_NAME}
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:include>
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
          ${CMAKE_CURRENT_SOURCE_DIR}/parser
          ${CMAKE_CURRENT_BINARY_DIR}
          ${PROJECT_SOURCE_DIR}/vendor/mir)

target_link_libraries(${TARGET_NAME} PUBLIC TurboUtils::Core PRIVATE TurboUtils::Parser mir_static)

install(FILES ${TURBO_FLOW_HEADERS} DESTINATION include)

if(BUILD_TESTING)
  add_subdirectory(tests)
  add_subdirectory(benchmarks)
endif()
