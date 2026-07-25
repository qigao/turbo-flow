add_executable(flowmq_yaml_endpoint yaml_endpoint.c)
cmake_config_target(flowmq_yaml_endpoint FOLDER "flowmq/examples")
target_link_libraries(flowmq_yaml_endpoint PRIVATE tf_fmq)

set(FLOWMQ_ZMQ_STYLE_EXAMPLES pub_sub req_rep router_dealer)

foreach(example_name IN LISTS FLOWMQ_ZMQ_STYLE_EXAMPLES)
  set(example_target flowmq_zmq_style_${example_name})
  add_executable(${example_target} zmq_style_${example_name}.c)
  cmake_config_target(${example_target} FOLDER "flowmq/examples")
  target_link_libraries(${example_target} PRIVATE tf_fmq)
endforeach()
