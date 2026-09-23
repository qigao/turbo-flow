add_library(tf_applicant_mapper_provider SHARED
  src/applicant_mapper_plugin.c)

target_compile_definitions(tf_applicant_mapper_provider PRIVATE TURBO_FLOW_PLUGIN_BUILD)

target_include_directories(tf_applicant_mapper_provider
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)

target_link_libraries(tf_applicant_mapper_provider
  PRIVATE TurboFlow::PluginHost Salts::JsonParser)

if(UNIX AND NOT APPLE)
  target_link_options(tf_applicant_mapper_provider PRIVATE "LINKER:--exclude-libs,ALL")
endif()

set_target_properties(tf_applicant_mapper_provider PROPERTIES
  C_VISIBILITY_PRESET hidden
  VISIBILITY_INLINES_HIDDEN YES
  FOLDER "plugins/applicant_mapper")

install(TARGETS tf_applicant_mapper_provider
  RUNTIME DESTINATION bin
  LIBRARY DESTINATION lib
  ARCHIVE DESTINATION lib)

install(FILES include/turbo_flow_applicant_mapper_plugin.h
        DESTINATION include/turbo_flow/plugins)

if(BUILD_TESTING)
  add_subdirectory(tests)
endif()
