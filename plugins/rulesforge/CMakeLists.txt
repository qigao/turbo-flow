file(TO_CMAKE_PATH "$ENV{RULES_FORGE_ROOT}" _turbo_flow_rulesforge_root)
find_package(RulesForge CONFIG REQUIRED
             PATHS "${_turbo_flow_rulesforge_root}" NO_DEFAULT_PATH)

add_library(tf_rulesforge_provider SHARED
  src/rulesforge_plugin.c)

target_compile_definitions(tf_rulesforge_provider PRIVATE TURBO_FLOW_PLUGIN_BUILD)

target_include_directories(tf_rulesforge_provider
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)

target_link_libraries(tf_rulesforge_provider
  PRIVATE
    TurboFlow::PluginHost
    RulesForge::RulesForge
    Salts::DataBind)

set_target_properties(tf_rulesforge_provider PROPERTIES
  C_VISIBILITY_PRESET hidden
  VISIBILITY_INLINES_HIDDEN YES
  FOLDER "plugins/rulesforge")

install(TARGETS tf_rulesforge_provider
  RUNTIME DESTINATION bin
  LIBRARY DESTINATION lib
  ARCHIVE DESTINATION lib)

install(FILES include/turbo_flow_rulesforge_plugin.h include/turbo_flow_rulesforge_resource.h
        DESTINATION include/turbo_flow/plugins)

unset(_turbo_flow_rulesforge_root)

if(BUILD_TESTING)
  add_library(test_rulesforge_resource_fixture SHARED
    ${PROJECT_SOURCE_DIR}/turbo_flow/tests/rulesforge_resource_fixture.c)
  get_target_property(_rulesforge_resource_includes RulesForge::RulesForge INTERFACE_INCLUDE_DIRECTORIES)
  target_include_directories(test_rulesforge_resource_fixture PUBLIC ${_rulesforge_resource_includes}
    PRIVATE
    ${PROJECT_SOURCE_DIR}/turbo_flow/tests ${CMAKE_CURRENT_SOURCE_DIR}/include)
  target_link_libraries(test_rulesforge_resource_fixture PRIVATE
    TurboFlow::PluginHost RulesForge::RulesForge Salts::Core)
  set_target_properties(test_rulesforge_resource_fixture PROPERTIES
    FOLDER "turbo_flow/tests/plugin-fixtures")
  cmake_add_test(
    test_flow_rulesforge_plugin
    SOURCES ${PROJECT_SOURCE_DIR}/turbo_flow/tests/test_flow_rulesforge_plugin.c
            ${PROJECT_SOURCE_DIR}/turbo_flow/tests/rulesforge_resource_deployment.c
            ${PROJECT_SOURCE_DIR}/turbo_flow/tests/rulesforge_resource_header_cpp.cpp
    LIBS TurboFlow::PluginHost Salts::TinyTest
    INCLUDES ${PROJECT_SOURCE_DIR}/plugins/rulesforge/include
             ${_rulesforge_resource_includes}
    FOLDER "turbo_flow/tests")
  target_compile_definitions(test_flow_rulesforge_plugin PRIVATE
    FLOW_RULESFORGE_PLUGIN="$<TARGET_FILE:tf_rulesforge_provider>"
    FLOW_RULESFORGE_RESOURCE_FIXTURE="$<TARGET_FILE:test_rulesforge_resource_fixture>")
  add_dependencies(test_flow_rulesforge_plugin tf_rulesforge_provider test_rulesforge_resource_fixture)

  unset(_rulesforge_resource_includes)
endif()
