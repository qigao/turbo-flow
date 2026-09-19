set(_turbo_flow_databind_host_root "$ENV{DATABIND_ROOT}")
if(DEFINED ENV{DATABIND_HOST_ROOT} AND
   NOT "$ENV{DATABIND_HOST_ROOT}" STREQUAL "")
  set(_turbo_flow_databind_host_root "$ENV{DATABIND_HOST_ROOT}")
endif()
if(NOT IS_DIRECTORY "${_turbo_flow_databind_host_root}")
  message(FATAL_ERROR
          "DataBind host root is not a directory: ${_turbo_flow_databind_host_root}")
endif()

find_program(
  TURBO_FLOW_TBE_COMPILER_HOST_EXECUTABLE
  NAMES tbe_compiler tbe_compiler.exe
  PATHS "${_turbo_flow_databind_host_root}/bin"
  NO_DEFAULT_PATH
  REQUIRED)

file(REAL_PATH "${_turbo_flow_databind_host_root}"
     _turbo_flow_databind_host_real_root)
file(REAL_PATH "${TURBO_FLOW_TBE_COMPILER_HOST_EXECUTABLE}"
     _turbo_flow_tbe_compiler_real_path)
cmake_path(IS_PREFIX _turbo_flow_databind_host_real_root
           "${_turbo_flow_tbe_compiler_real_path}" NORMALIZE
           _turbo_flow_tbe_compiler_is_within_databind_root)
if(NOT _turbo_flow_tbe_compiler_is_within_databind_root)
  message(FATAL_ERROR
          "tbe_compiler resolved outside the DataBind host root: "
          "${TURBO_FLOW_TBE_COMPILER_HOST_EXECUTABLE}")
endif()

add_subdirectory(common)
add_subdirectory(inbox)
add_subdirectory(network)

add_library(
  tf_protocol_plugin_support STATIC
  src/flow_protocol_plugin_support.c
  src/flow_protocol_plugin_support.h)
set_target_properties(tf_protocol_plugin_support PROPERTIES
                      POSITION_INDEPENDENT_CODE ON
                      FOLDER "ingress/protocol/internal")
target_include_directories(
  tf_protocol_plugin_support
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(tf_protocol_plugin_support PUBLIC TurboFlow::ProtocolIngress)

function(turbo_flow_add_protocol target_name export_name source_file)
  add_library(${target_name} SHARED ${source_file})
  cmake_config_target(
    ${target_name}
    FOLDER "ingress/protocol/plugins"
    EXPORT_NAME ${export_name})
  target_compile_definitions(${target_name} PRIVATE TURBO_FLOW_BUILD TURBO_FLOW_PLUGIN_BUILD)
  target_include_directories(${target_name}
                             PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
  target_link_libraries(${target_name}
                        PRIVATE tf_protocol_plugin_support TurboFlow::ProtocolIngress)
endfunction()

turbo_flow_add_protocol(
  tf_protocol_mqtt_sn ProtocolMqttSn mqtt_sn/flow_protocol_mqtt_sn.c)
turbo_flow_add_protocol(
  tf_protocol_coap ProtocolCoap coap/flow_protocol_coap.c)
turbo_flow_add_protocol(
  tf_protocol_lwm2m ProtocolLwm2m lwm2m/flow_protocol_lwm2m.c)
turbo_flow_add_protocol(
  tf_protocol_ocpp ProtocolOcpp ocpp/flow_protocol_ocpp.c)
target_link_libraries(tf_protocol_ocpp PRIVATE Salts::JsonParser)
if(NOT WIN32)
  target_link_libraries(tf_protocol_ocpp PRIVATE m)
endif()
turbo_flow_add_protocol(
  tf_protocol_gbt32960 ProtocolGbt32960 gbt32960/flow_protocol_gbt32960.c)
turbo_flow_add_protocol(
  tf_protocol_jtt808 ProtocolJtt808 jtt808/flow_protocol_jtt808.c)

add_subdirectory(business)

if(BUILD_TESTS)
  add_subdirectory(tests)
endif()

unset(_turbo_flow_databind_host_real_root)
unset(_turbo_flow_databind_host_root)
unset(_turbo_flow_tbe_compiler_is_within_databind_root)
unset(_turbo_flow_tbe_compiler_real_path)
