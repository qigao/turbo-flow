find_package(RulesForge 0.9 CONFIG REQUIRED
             PATHS "$ENV{RULES_FORGE_ROOT}" NO_DEFAULT_PATH)

add_library(tf_rulesforge_provider SHARED
  src/rulesforge_plugin.c)

target_compile_definitions(tf_rulesforge_provider PRIVATE TURBO_FLOW_PLUGIN_BUILD)

target_include_directories(tf_rulesforge_provider
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)

target_link_libraries(tf_rulesforge_provider
  PRIVATE TurboFlow::PluginHost RulesForge::RulesForge)

set_target_properties(tf_rulesforge_provider PROPERTIES
  C_VISIBILITY_PRESET hidden
  VISIBILITY_INLINES_HIDDEN YES
  FOLDER "plugins/rulesforge")

install(TARGETS tf_rulesforge_provider
  RUNTIME DESTINATION bin
  LIBRARY DESTINATION lib
  ARCHIVE DESTINATION lib)

install(FILES include/turbo_flow_rulesforge_plugin.h
        DESTINATION include/turbo_flow/plugins)
