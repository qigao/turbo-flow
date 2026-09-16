cmake_add_test(test_flow_durable_memory_plugin
  SOURCES test_flow_durable_memory_plugin.c
  LIBS TurboFlow::PluginHost Salts::TinyTest
  FOLDER "io/durable/tests")
add_dependencies(test_flow_durable_memory_plugin tf_durable_memory_plugin)
set_tests_properties(test_flow_durable_memory_plugin PROPERTIES
  LABELS "graph;core;durable;plugin"
  ENVIRONMENT "TURBO_FLOW_DURABLE_MEMORY_PLUGIN_PATH=$<TARGET_FILE:tf_durable_memory_plugin>")
