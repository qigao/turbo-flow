cmake_add_test(
  test_flow_applicant_mapper_plugin
  SOURCES test_flow_applicant_mapper_plugin.c
  LIBS TurboFlow::PluginHost Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../include
  FOLDER "plugins/applicant_mapper/tests")
target_compile_definitions(test_flow_applicant_mapper_plugin PRIVATE
  FLOW_APPLICANT_MAPPER_PLUGIN="$<TARGET_FILE:tf_applicant_mapper_provider>")
add_dependencies(test_flow_applicant_mapper_plugin tf_applicant_mapper_provider)
set_tests_properties(test_flow_applicant_mapper_plugin
  PROPERTIES LABELS "plugin;protocol-mapper;applicant")
