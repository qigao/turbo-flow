set(TARGET_NAME turbo_flow_graph)
find_package(RulesForge 0.9 CONFIG REQUIRED
             PATHS "$ENV{RULES_FORGE_ROOT}" NO_DEFAULT_PATH)
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
list(REMOVE_ITEM TURBO_FLOW_SOURCES
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flow_config.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flow_product.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flow_plugin.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flow_plugin_generation.c)
set(TURBO_FLOW_HEADERS
  ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_export.h
  ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow.h
  ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_domain.h
  ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_control.h
  ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_expr.h
  ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_rulesforge.h)
list(APPEND TURBO_FLOW_HEADERS ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_policy.h)
list(APPEND TURBO_FLOW_HEADERS ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_discovery.h)
list(APPEND TURBO_FLOW_HEADERS ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_config.h)
list(APPEND TURBO_FLOW_HEADERS ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_config_limits.h)
list(APPEND TURBO_FLOW_HEADERS ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_product.h)
list(APPEND TURBO_FLOW_HEADERS ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_plugin.h)
list(APPEND TURBO_FLOW_HEADERS ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_plugin_generation.h)
list(APPEND TURBO_FLOW_HEADERS ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_plugin_protocol.h)
list(APPEND TURBO_FLOW_HEADERS ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_resolved_config.h)
list(APPEND TURBO_FLOW_HEADERS ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_security.h)

add_library(turbo_flow_config SHARED
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flow_config.c
  ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_resolved_config.h)
cmake_config_target(
  turbo_flow_config
  ALIAS TurboFlow::Config
  FOLDER "turbo_flow/config"
  EXPORT_NAME Config)
target_compile_definitions(turbo_flow_config PRIVATE TURBO_FLOW_BUILD)
target_include_directories(
  turbo_flow_config
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:include>
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(turbo_flow_config PUBLIC Salts::Core
                                        PRIVATE Salts::CYamlJsonAdapter)

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
  ALIAS TurboFlow::Graph
  FOLDER "turbo_flow"
  EXPORT_NAME Graph)

target_compile_definitions(${TARGET_NAME} PRIVATE TURBO_FLOW_BUILD)
target_compile_definitions(${TARGET_NAME} PRIVATE TURBO_FLOW_EXPR_ENABLE_JIT=1)

target_include_directories(
  ${TARGET_NAME}
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:include>
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
          ${CMAKE_CURRENT_SOURCE_DIR}/parser
          ${CMAKE_CURRENT_BINARY_DIR}
          ${PROJECT_SOURCE_DIR}/vendor/mir)

target_link_libraries(${TARGET_NAME} PUBLIC 
  TurboFlow::Config Salts::Core Salts::CSTL Salts::CFlow RulesForge::RulesForge
  PRIVATE Salts::DataBind Salts::JsonParser mir_static)

add_library(turbo_flow_product SHARED
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flow_product.c
  ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_product.h
  ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_config.h)
cmake_config_target(
  turbo_flow_product
  ALIAS TurboFlow::Product
  FOLDER "turbo_flow/product"
  EXPORT_NAME Product)
target_compile_definitions(turbo_flow_product PRIVATE TURBO_FLOW_BUILD)
target_include_directories(
  turbo_flow_product
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:include>
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(turbo_flow_product PUBLIC TurboFlow::Graph TurboFlow::Config
                                        PRIVATE Salts::JsonParser)

add_library(turbo_flow_plugin_host SHARED
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flow_plugin.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flow_plugin_generation.c
  ${PROJECT_SOURCE_DIR}/ingress/protocol/common/src/flow_protocol_registry.c
  ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_plugin.h
  ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_plugin_generation.h
  ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_plugin_protocol.h)
cmake_config_target(
  turbo_flow_plugin_host
  ALIAS TurboFlow::PluginHost
  FOLDER "turbo_flow/plugin"
  EXPORT_NAME PluginHost)
target_compile_definitions(turbo_flow_plugin_host PRIVATE TURBO_FLOW_BUILD)
target_include_directories(
  turbo_flow_plugin_host
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/ingress/protocol/common/include>
         $<INSTALL_INTERFACE:include>
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(turbo_flow_plugin_host
                      PUBLIC TurboFlow::Product TurboFlow::ProtocolIngress
                      PRIVATE Salts::CSTL ${CMAKE_DL_LIBS})

# Binary compatibility surface: keep the historical turbo_flow library and
# its exported symbols while new consumers move to the smaller targets above.
add_library(turbo_flow SHARED
  ${TURBO_FLOW_SOURCES}
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flow_config.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flow_product.c
  ${TURBO_FLOW_LEXER_GEN}
  ${TURBO_FLOW_GRAMMAR_GEN}
  ${TURBO_FLOW_EXPR_LEXER_GEN}
  ${TURBO_FLOW_EXPR_GRAMMAR_GEN}
  ${TURBO_FLOW_CONTROL_LEXER_GEN}
  ${TURBO_FLOW_CONTROL_GRAMMAR_GEN}
  ${TURBO_FLOW_SECURITY_RULE_LEXER_GEN}
  ${TURBO_FLOW_HEADERS})
add_dependencies(turbo_flow
  ${TURBO_FLOW_LEXER_TARGET}
  ${TURBO_FLOW_GRAMMAR_TARGET}
  ${TURBO_FLOW_EXPR_LEXER_TARGET}
  ${TURBO_FLOW_EXPR_GRAMMAR_TARGET}
  ${TURBO_FLOW_CONTROL_LEXER_TARGET}
  ${TURBO_FLOW_CONTROL_GRAMMAR_TARGET}
  ${TURBO_FLOW_SECURITY_RULE_LEXER_TARGET})
cmake_config_target(
  turbo_flow
  ALIAS TurboFlow::Flow
  FOLDER "turbo_flow/compat"
  EXPORT_NAME Flow)
target_compile_definitions(turbo_flow PRIVATE TURBO_FLOW_BUILD TURBO_FLOW_EXPR_ENABLE_JIT=1)
target_include_directories(
  turbo_flow
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:include>
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
          ${CMAKE_CURRENT_SOURCE_DIR}/parser
          ${CMAKE_CURRENT_BINARY_DIR}
          ${PROJECT_SOURCE_DIR}/vendor/mir)
target_link_libraries(turbo_flow PUBLIC Salts::Core Salts::CSTL Salts::CFlow RulesForge::RulesForge
                               PRIVATE Salts::CYamlJsonAdapter Salts::DataBind
                                       Salts::JsonParser mir_static)

install(FILES ${TURBO_FLOW_HEADERS} DESTINATION include)

if(BUILD_TESTING)
  add_subdirectory(tests)
  add_subdirectory(benchmarks)
endif()
