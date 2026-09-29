file(TO_CMAKE_PATH "$ENV{RULES_FORGE_ROOT}" _turbo_flow_rulesforge_root)
find_package(RulesForge 0.9.0 EXACT CONFIG REQUIRED
             PATHS "${_turbo_flow_rulesforge_root}" NO_DEFAULT_PATH)

foreach(_turbo_flow_rulesforge_databind_target IN ITEMS
        Salts::DataBindCore
        Salts::DataBindJsonAdapter)
  if(NOT TARGET ${_turbo_flow_rulesforge_databind_target})
    message(FATAL_ERROR
            "SaltsUtils 4.1.3 package is missing required target: "
            "${_turbo_flow_rulesforge_databind_target}")
  endif()
endforeach()

add_library(tf_rulesforge_provider SHARED
  src/rulesforge_plugin.c)

target_compile_definitions(tf_rulesforge_provider PRIVATE TURBO_FLOW_PLUGIN_BUILD)

target_include_directories(tf_rulesforge_provider
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)

target_link_libraries(tf_rulesforge_provider
  PRIVATE
    TurboFlow::PluginHost
    RulesForge::RulesForge
    Salts::DataBindCore
    Salts::DataBindJsonAdapter)

if(UNIX AND NOT APPLE)
  # DataBindCore/JSON adapter are private static implementation archives.
  # Keep their global symbols out of the provider's dynamic export surface.
  target_link_options(tf_rulesforge_provider PRIVATE "LINKER:--exclude-libs,ALL")
endif()

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

unset(_turbo_flow_rulesforge_databind_target)
unset(_turbo_flow_rulesforge_root)
