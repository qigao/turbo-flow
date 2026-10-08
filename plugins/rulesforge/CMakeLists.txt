file(TO_CMAKE_PATH "$ENV{RULES_FORGE_ROOT}" _turbo_flow_rulesforge_root)
find_package(RulesForge CONFIG REQUIRED
             PATHS "${_turbo_flow_rulesforge_root}" NO_DEFAULT_PATH)

# SaltsUtils 4.2+ publishes one canonical installed DataBind target.
# Internal Core/adapter archives are not consumer package contracts.
if(NOT TARGET Salts::DataBind)
  message(FATAL_ERROR "resolved SaltsUtils package is missing Salts::DataBind")
endif()

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

install(FILES include/turbo_flow_rulesforge_plugin.h
        DESTINATION include/turbo_flow/plugins)

unset(_turbo_flow_rulesforge_root)
